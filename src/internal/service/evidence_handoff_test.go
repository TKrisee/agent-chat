package service

import (
	"context"
	"database/sql"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func TestEvidenceHandoffPreservesTaskAndPrivateSource(t *testing.T) {
	s, task, po, _ := continuationFixture(t)
	defer s.Close()
	if _, err := s.db.Exec(`UPDATE tasks SET status='blocked' WHERE id=?`, task); err != nil {
		t.Fatal(err)
	}
	source := newID()
	if _, err := s.userMessage(context.Background(), source, root, "", "Please pass these evidence locations to the project owner.", "", nil); err != nil {
		t.Fatal(err)
	}
	j, err := s.claim(nil)
	if err != nil || j == nil || j.Source != source {
		t.Fatalf("root turn=%+v err=%v", j, err)
	}
	if j.Request.AllowOperatorResolveHold {
		t.Fatal("evidence-only message offered hold resolution")
	}
	result := runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "handoff_evidence", Project: "alpha", Body: "Evidence is available at the referenced location."}}}
	if err = s.complete(*j, result, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err = s.complete(*j, result, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	var count int
	var taskStatus string
	if err = s.db.QueryRow(`SELECT count(*) FROM messages WHERE sender='root' AND recipient=? AND project_id='alpha' AND task_id IS NULL AND text=?`, po, result.Actions[0].Body).Scan(&count); err != nil || count != 1 {
		t.Fatalf("handoffs=%d err=%v", count, err)
	}
	if err = s.db.QueryRow(`SELECT status FROM tasks WHERE id=?`, task).Scan(&taskStatus); err != nil || taskStatus != "blocked" {
		t.Fatalf("task=%s err=%v", taskStatus, err)
	}
	if err = s.write(func(tx *sql.Tx) error {
		a, err := getAgent(tx, po)
		if err != nil {
			return err
		}
		messages, err := s.runtimeContext(tx, a, "alpha", task, "", "assignment", source)
		if err != nil {
			return err
		}
		found := false
		for _, m := range messages {
			if m.Text == result.Actions[0].Body {
				found = true
			}
			if m.ID == source {
				t.Fatal("operator private source exposed to PO")
			}
		}
		if !found {
			t.Fatal("summary unavailable in PO task context")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
