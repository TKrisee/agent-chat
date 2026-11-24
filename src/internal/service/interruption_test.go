package service

import (
	"context"
	"database/sql"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"agentchat/src/internal/runtimeapi"
)

type stopFixture struct {
	started   chan string
	cancelled chan string
}

func (f stopFixture) Execute(ctx context.Context, r runtimeapi.Request) (runtimeapi.Result, error) {
	if err := runtimeapi.StartThroughGate(ctx, func() error { f.started <- r.AgentID; return nil }); err != nil {
		return runtimeapi.Result{}, err
	}
	runtimeapi.NotifyStart(ctx)
	<-ctx.Done()
	f.cancelled <- r.AgentID
	return runtimeapi.Result{}, ctx.Err()
}
func (f stopFixture) Recover(context.Context, runtimeapi.Request) (runtimeapi.Result, runtimeapi.State, error) {
	return runtimeapi.Result{}, runtimeapi.Unknown, nil
}
func TestProjectStopCancelsOwnedRunAndFencesReplay(t *testing.T) {
	s := openTest(t, t.TempDir())
	defer s.Close()
	po, worker, source, task := newID(), newID(), newID(), newID()
	if err := s.write(func(tx *sql.Tx) error {
		if _, err := tx.Exec("INSERT INTO projects VALUES('alpha','Alpha',?,'/tmp',0)", po); err != nil {
			return err
		}
		for _, a := range []struct{ id, role string }{{po, "po"}, {worker, "worker"}} {
			if _, err := tx.Exec("INSERT INTO agents VALUES(?,?,?,'alpha','model','high')", a.id, a.role, a.role); err != nil {
				return err
			}
		}
		if _, err := tx.Exec("INSERT INTO tasks VALUES(?,'alpha','task','queued',?)", task, worker); err != nil {
			return err
		}
		if err := addMessage(tx, source, po, worker, "po", "work", "alpha", task); err != nil {
			return err
		}
		return s.enqueue(tx, agent{ID: worker, Role: "worker", Project: "alpha", Model: "model", Effort: "high"}, "alpha", task, source, "work", "work")
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	f := stopFixture{make(chan string, 1), make(chan string, 1)}
	go func() { done <- s.Run(ctx, f) }()
	defer func() {
		cancel()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()
	select {
	case <-f.started:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not start")
	}
	w := httptest.NewRecorder()
	s.setStop(w, "alpha", true)
	if w.Code != 200 {
		t.Fatalf("stop status %d", w.Code)
	}
	select {
	case <-f.cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("owned worker was not cancelled")
	}
	launched := false
	if err := s.startGate(context.Background(), job{Project: "alpha"}, func() error { launched = true; return nil }); !errors.Is(err, runtimeapi.ErrStartWithheld) || launched {
		t.Fatalf("stop/start gate allowed launch: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		var status string
		if err := s.db.QueryRow("SELECT status FROM jobs WHERE agent_id=?", worker).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status == "uncertain" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("interrupted job disposition %s", status)
		}
		time.Sleep(10 * time.Millisecond)
	}
	var blocked string
	if err := s.db.QueryRow("SELECT status FROM tasks WHERE id=?", task).Scan(&blocked); err != nil || blocked != "blocked" {
		t.Fatalf("task %s: %v", blocked, err)
	}
	if err := s.write(func(tx *sql.Tx) error { _, err := updateStop(tx, "alpha", false); return err }); err != nil {
		t.Fatal(err)
	}
	if j, err := s.claim(nil); err != nil || j != nil {
		t.Fatalf("uncertain job replayed: %+v %v", j, err)
	}
}
