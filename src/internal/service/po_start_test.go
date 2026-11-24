package service

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"agentchat/src/internal/runtimeapi"
)

type poStartFixture struct{ entered, launch, fired, release chan struct{} }

func (r poStartFixture) Execute(ctx context.Context, req runtimeapi.Request) (runtimeapi.Result, error) {
	close(r.entered)
	select {
	case <-r.launch:
	case <-ctx.Done():
		return runtimeapi.Result{}, ctx.Err()
	}
	runtimeapi.NotifyStart(ctx)
	runtimeapi.NotifyStart(ctx)
	close(r.fired)
	select {
	case <-r.release:
	case <-ctx.Done():
	}
	return runtimeapi.Result{}, errors.New("fixture ended")
}
func (r poStartFixture) Recover(context.Context, runtimeapi.Request) (runtimeapi.Result, runtimeapi.State, error) {
	return runtimeapi.Result{}, runtimeapi.Unknown, nil
}
func TestProjectOwnerAssignmentLaunchIsConfirmedOnce(t *testing.T) {
	s := openTest(t, t.TempDir())
	defer s.Close()
	po, task, source := newID(), newID(), newID()
	if err := s.write(func(tx *sql.Tx) error {
		if _, err := tx.Exec("INSERT INTO projects VALUES('alpha','Alpha',?,'/tmp',0)", po); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT INTO agents VALUES(?,?,'po','alpha','model','high')", po, "Alpha PO"); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT INTO tasks VALUES(?,'alpha','task','assigned',?)", task, po); err != nil {
			return err
		}
		if err := addMessage(tx, source, root, po, "root", "private assignment", "alpha", task); err != nil {
			return err
		}
		return s.enqueue(tx, agent{ID: po, Role: "po", Project: "alpha", Model: "model", Effort: "high"}, "alpha", task, source, "assignment", "private assignment")
	}); err != nil {
		t.Fatal(err)
	}
	j, err := s.claim(nil)
	if err != nil || j == nil {
		t.Fatalf("claim: %+v %v", j, err)
	}
	fixture := poStartFixture{make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- s.runJob(context.Background(), fixture, *j) }()
	defer func() { close(fixture.release); <-done }()
	select {
	case <-fixture.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("assignment did not enter runtime")
	}
	var count int
	var status string
	if err = s.db.QueryRow("SELECT count(*) FROM workflow_notices WHERE event_key=?", "start:"+j.ID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("notice before start: %d %v", count, err)
	}
	if err = s.db.QueryRow("SELECT status FROM jobs WHERE id=?", j.ID).Scan(&status); err != nil || status != "submitted" {
		t.Fatalf("prelaunch status %s %v", status, err)
	}
	close(fixture.launch)
	select {
	case <-fixture.fired:
	case <-time.After(5 * time.Second):
		t.Fatal("start callback missing")
	}
	var text, room string
	if err = s.db.QueryRow(`SELECT m.text,m.room_id FROM workflow_notices n JOIN messages m ON m.id=n.message_id WHERE n.event_key=?`, "start:"+j.ID).Scan(&text, &room); err != nil {
		t.Fatal(err)
	}
	if room != "group:alpha" || text != "Project owner run launched locally; agent acknowledgment is pending." {
		t.Fatalf("wrong notice: %q %q", room, text)
	}
	if err = s.db.QueryRow("SELECT status FROM jobs WHERE id=?", j.ID).Scan(&status); err != nil || status != "running" {
		t.Fatalf("running status %s %v", status, err)
	}
	if err = s.db.QueryRow("SELECT count(*) FROM workflow_notices WHERE event_key=?", "start:"+j.ID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("duplicate start notices: %d %v", count, err)
	}
	if _, err = s.db.Exec("UPDATE jobs SET status='uncertain' WHERE id=?", j.ID); err != nil {
		t.Fatal(err)
	}
	if err = s.ownerAssignmentStarted(*j); err != nil {
		t.Fatal(err)
	}
	if err = s.db.QueryRow("SELECT status FROM jobs WHERE id=?", j.ID).Scan(&status); err != nil || status != "uncertain" {
		t.Fatalf("late callback changed outcome: %s %v", status, err)
	}
}
