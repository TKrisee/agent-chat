//go:build !windows

package service

import (
	"context"
	"database/sql"
	"errors"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"agentchat/src/internal/codexruntime"
	"agentchat/src/internal/runtimeapi"
)

// Expose only the execution contract, avoiding unrelated account telemetry.
type nativeStopRunner struct{ runner *codexruntime.Runner }

func (r nativeStopRunner) Execute(ctx context.Context, req runtimeapi.Request) (runtimeapi.Result, error) {
	return r.runner.Execute(ctx, req)
}
func (r nativeStopRunner) Recover(ctx context.Context, req runtimeapi.Request) (runtimeapi.Result, runtimeapi.State, error) {
	return r.runner.Recover(ctx, req)
}
func TestProjectStopReapsOwnedNativeProcess(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, dir)
	defer s.Close()
	pidPath := filepath.Join(dir, "child.pid")
	countPath := filepath.Join(dir, "starts")
	script := filepath.Join(dir, "fake-codex")
	if err := os.WriteFile(script, []byte("#!/bin/sh\necho $$ > \"$AGENTCHAT_FAKE_PID\"\necho start >> \"$AGENTCHAT_FAKE_STARTS\"\nexec sleep 60\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGENTCHAT_FAKE_PID", pidPath)
	t.Setenv("AGENTCHAT_FAKE_STARTS", countPath)
	jq, err := exec.LookPath("jq")
	if err != nil {
		t.Fatal(err)
	}
	runner, err := codexruntime.New(codexruntime.Config{StateDir: filepath.Join(dir, "native"), CodexPath: script, JQPath: jq})
	if err != nil {
		t.Fatal(err)
	}
	po, worker, source, task := newID(), newID(), newID(), newID()
	if err = os.MkdirAll(s.workspace("alpha"), 0700); err != nil {
		t.Fatal(err)
	}
	if err = s.write(func(tx *sql.Tx) error {
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
	go func() { done <- s.Run(ctx, nativeStopRunner{runner}) }()
	defer func() {
		cancel()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()
	deadline := time.Now().Add(5 * time.Second)
	var pid int
	for time.Now().Before(deadline) {
		if b, e := os.ReadFile(pidPath); e == nil {
			pid, e = strconv.Atoi(strings.TrimSpace(string(b)))
			if e == nil && pid > 0 {
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if pid == 0 {
		t.Fatal("owned fake process never started")
	}
	started := time.Now()
	w := httptest.NewRecorder()
	s.setStop(w, "alpha", true)
	if w.Code != 200 {
		t.Fatalf("stop status %d", w.Code)
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		var status string
		if err = s.db.QueryRow("SELECT status FROM jobs WHERE agent_id=?", worker).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status == "uncertain" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stop unresolved after %v: %s", time.Since(started), status)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err = syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("owned process still exists or cannot verify: %v", err)
	}
	if b, err := os.ReadFile(countPath); err != nil || strings.Count(string(b), "start") != 1 {
		t.Fatalf("unexpected launches %q: %v", b, err)
	}
}
