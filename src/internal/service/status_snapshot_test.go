package service

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
)

func TestRootStatusSnapshotUsesClaimTimeAndPreservesPrivacy(t *testing.T) {
	s := openTest(t, t.TempDir())
	defer s.Close()
	po, worker, task, source := newID(), newID(), newID(), newID()
	if err := s.write(func(tx *sql.Tx) error {
		if _, err := tx.Exec("INSERT INTO projects VALUES('alpha','Alpha',?,'/tmp',0)", po); err != nil {
			return err
		}
		for _, a := range []struct{ id, role string }{{po, "po"}, {worker, "worker"}} {
			if _, err := tx.Exec("INSERT INTO agents VALUES(?,?,?,'alpha','model','high')", a.id, a.role, a.role); err != nil {
				return err
			}
		}
		if _, err := tx.Exec("INSERT INTO project_goals VALUES('alpha','secret goal objective','active')"); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT INTO tasks VALUES(?,'alpha','secret task title','assigned',?)", task, po); err != nil {
			return err
		}
		return addMessage(tx, source, po, worker, "po", "secret internal DM", "alpha", task)
	}); err != nil {
		t.Fatal(err)
	}
	id := newID()
	if _, err := s.userMessage(context.Background(), id, root, "", "What is alpha's status?", "", nil); err != nil {
		t.Fatal(err)
	}
	// State changes after enqueue must be included in the snapshot captured at claim.
	if err := s.write(func(tx *sql.Tx) error {
		if _, err := tx.Exec("UPDATE tasks SET status='blocked' WHERE id=?", task); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO jobs(id,agent_id,project_id,task_id,source_id,kind,status,request,diagnostic,next_at) VALUES(?,?,'alpha',?,?,'assignment','completed',?,'invalid runtime result: task owner or status changed',9999999999999)`, newID(), po, task, source, []byte("{}")); err != nil {
			return err
		}
		for i := 0; i < 9; i++ {
			if _, err := tx.Exec(`INSERT INTO jobs(id,agent_id,project_id,task_id,source_id,kind,status,request,diagnostic,next_at) VALUES(?,?,'alpha',?,?,?,'running',?,'sensitive raw diagnostic',9999999999999)`, newID(), worker, task, source, fmt.Sprintf("work-%d", i), []byte("{}")); err != nil {
				return fmt.Errorf("active job fixture %d: %w", i, err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	j, err := s.claim(nil)
	if err != nil || j == nil || j.Source != id {
		t.Fatalf("claim: %+v %v", j, err)
	}
	snapshot := j.Request.Instructions
	for _, want := range []string{"App-owned status snapshot v1", `project="alpha"`, `task="` + task + `"`, "status=\"blocked\"", "kind=\"assignment\"", "rejected_task_owner_or_status_changed", "jobs_truncated=true", "po_goal_status=\"active\""} {
		if !strings.Contains(snapshot, want) {
			t.Errorf("snapshot missing %q", want)
		}
	}
	for _, secret := range []string{"secret goal objective", "secret task title", "secret internal DM", "sensitive raw diagnostic"} {
		if strings.Contains(snapshot, secret) {
			t.Fatalf("private content leaked: %q", secret)
		}
	}
	for _, m := range j.Request.Context {
		if m.ID == source {
			t.Fatal("other agents' DM leaked into Root context")
		}
	}
	if err = s.disposition(j.ID, "pending", "verified pre-start retry", 0); err != nil {
		t.Fatal(err)
	}
	again, err := s.claim(nil)
	if err != nil || again == nil {
		t.Fatalf("retry claim: %v", err)
	}
	if again.Request.Instructions != snapshot {
		t.Fatal("retry rewrote snapshot and would change durable request identity")
	}
}
