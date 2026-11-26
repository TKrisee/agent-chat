package service

import (
	"database/sql"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func TestOneShotWorkerBindingAndRetirement(t *testing.T) {
	s, parent, poID, _ := continuationFixture(t)
	defer s.Close()
	j := job{Task: parent, Project: "alpha", Agent: poID, Kind: "assignment"}
	var id, child string
	if err := s.write(func(tx *sql.Tx) error {
		po, err := getAgent(tx, poID)
		if err != nil {
			return err
		}
		if err = s.createProjectWorker(tx, j, po, runtimeapi.Action{Type: "create_one_shot_worker", Title: "Temporary"}); err != nil {
			return err
		}
		if err = s.queuePOWorkItem(tx, j, po, runtimeapi.Action{Title: "Inspect", Body: "Inspect evidence", Recipient: "Temporary"}); err != nil {
			return err
		}
		return tx.QueryRow(`SELECT agent_id,child_task_id FROM one_shot_workers`).Scan(&id, &child)
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.write(func(tx *sql.Tx) error {
		po, err := getAgent(tx, poID)
		if err != nil {
			return err
		}
		return s.queuePOWorkItem(tx, j, po, runtimeapi.Action{Title: "Second", Body: "Other work", Recipient: id})
	}); err == nil {
		t.Fatal("one-shot worker accepted a second task")
	}
	check := func(want bool) {
		t.Helper()
		state, err := s.state()
		if err != nil {
			t.Fatal(err)
		}
		for _, a := range state.Agents {
			if a.ID == id {
				if !a.OneShot || a.OneShotRetired != want {
					t.Fatalf("agent=%+v", a)
				}
				return
			}
		}
		t.Fatal("agent missing")
	}
	check(false)
	if _, err := s.db.Exec(`UPDATE tasks SET status='completed',owner_id=? WHERE id=?`, poID, child); err != nil {
		t.Fatal(err)
	}
	check(false) // Pending work must still fence retirement.
	if _, err := s.db.Exec(`UPDATE jobs SET status='completed' WHERE task_id=?`, child); err != nil {
		t.Fatal(err)
	}
	check(true)
	if _, err := s.db.Exec(`INSERT INTO approvals VALUES(?,'alpha',?,?,'prior decision','settled','denied')`, newID(), child, poID); err != nil {
		t.Fatal(err)
	}
	check(true) // A settled denial alone does not make a completed worker active.
	if _, err := s.db.Exec(`UPDATE jobs SET status='uncertain' WHERE task_id=?`, child); err != nil {
		t.Fatal(err)
	}
	check(false)
}
