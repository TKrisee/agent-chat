package service

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"agentchat/src/internal/runtimeapi"
)

func TestUpdateDrainPreservesRunningAndPendingWork(t *testing.T) {
	s := openTest(t, t.TempDir())
	defer s.Close()
	s.opts.Concurrency = 1
	po, worker, task, source := newID(), newID(), newID(), newID()
	if err := s.write(func(tx *sql.Tx) error {
		if _, err := tx.Exec(`INSERT INTO projects VALUES('alpha','Alpha',?,'/tmp',0)`, po); err != nil {
			return err
		}
		for _, a := range []struct{ id, role string }{{po, "po"}, {worker, "worker"}} {
			if _, err := tx.Exec(`INSERT INTO agents VALUES(?,?,?,'alpha','model','high')`, a.id, a.role, a.role); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(`INSERT INTO tasks VALUES(?,'alpha','task','queued',?)`, task, worker); err != nil {
			return err
		}
		if err := addMessage(tx, source, po, worker, "po", "work", "alpha", task); err != nil {
			return err
		}
		return s.enqueue(tx, agent{ID: worker, Role: "worker", Project: "alpha", Model: "model", Effort: "high"}, "alpha", task, source, "work", "work")
	}); err != nil {
		t.Fatal(err)
	}
	started := make(chan context.Context, 1)
	finish := make(chan struct{})
	runner := runnerFuncs{execute: func(ctx context.Context, req runtimeapi.Request) (runtimeapi.Result, error) {
		started <- ctx
		<-finish
		return runtimeapi.Result{Text: "finished"}, nil
	}, recover: func(context.Context, runtimeapi.Request) (runtimeapi.Result, runtimeapi.State, error) {
		return runtimeapi.Result{}, runtimeapi.Unknown, nil
	}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, runner) }()
	defer func() { cancel(); <-done }()
	var active context.Context
	select {
	case active = <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("work did not start")
	}
	ready, release, err := s.BeginUpdateDrain()
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-ready:
		t.Fatal("drain returned while work active")
	default:
	}
	select {
	case <-active.Done():
		t.Fatal("drain cancelled active work")
	default:
	}
	if _, _, err := s.BeginUpdateDrain(); err == nil {
		t.Fatal("duplicate drain accepted")
	}
	close(finish)
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("drain did not finish")
	}
	var pending int
	if err := s.db.QueryRow(`SELECT count(*) FROM jobs WHERE kind='review' AND status='pending'`).Scan(&pending); err != nil || pending != 1 {
		t.Fatalf("pending review=%d: %v", pending, err)
	}
	if j, err := s.claim(nil); err != nil || j != nil {
		t.Fatalf("claimed during drain: %v", err)
	}
	release()
	s.startMu.Lock()
	draining := s.updateDrain != nil
	s.startMu.Unlock()
	if draining {
		t.Fatal("admission remained blocked after release")
	}
}
