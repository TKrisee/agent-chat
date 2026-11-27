package service

import (
	"context"
	"database/sql"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func TestWorkerDMCanShareAssignedFindingWithAppointedPO(t *testing.T) {
	s, task, po, worker := continuationFixture(t)
	defer s.Close()
	if err := s.write(func(tx *sql.Tx) error {
		source := newID()
		if err := addMessage(tx, source, po, worker, "po", "assigned work", "alpha", task); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT INTO jobs(id,agent_id,project_id,task_id,source_id,kind,status,request,next_at) VALUES(?,?,'alpha',?,?,'work','completed',?,0)`, newID(), worker, task, source, []byte("{}"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	source := newID()
	const private = "Please tell the project owner what you observed; private operator detail"
	if _, err := s.userMessage(context.Background(), source, worker, "", private, "", nil); err != nil {
		t.Fatal(err)
	}
	j, err := s.claim(nil)
	if err != nil || j == nil || j.Agent != worker || j.Kind != "operator_message" {
		t.Fatalf("worker DM claim: %v", err)
	}
	result := runtimeapi.Result{Text: "I will share the observed startup state.", Actions: []runtimeapi.Action{{Type: "share_with_po", Body: "The assigned startup produced no confirmed readiness."}}}
	if err := s.complete(*j, result, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.complete(*j, result, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.db.QueryRow(`SELECT count(*) FROM worker_po_handoffs h JOIN messages m ON m.id=h.message_id WHERE h.source_id=? AND h.job_id=? AND m.sender=? AND m.recipient=? AND m.text=?`, source, j.ID, worker, po, "The assigned startup produced no confirmed readiness.").Scan(&count); err != nil || count != 1 {
		t.Fatalf("handoffs=%d: %v", count, err)
	}
	if err := s.db.QueryRow(`SELECT count(*) FROM messages WHERE sender=? AND recipient=? AND text=?`, worker, operator, result.Text).Scan(&count); err != nil || count != 1 {
		t.Fatalf("operator reply=%d: %v", count, err)
	}
	if err := s.write(func(tx *sql.Tx) error {
		a, err := getAgent(tx, po)
		if err != nil {
			return err
		}
		context, err := s.runtimeContext(tx, a, "alpha", task, "", "assignment")
		if err != nil {
			return err
		}
		found := false
		for _, m := range context {
			if m.Text == "The assigned startup produced no confirmed readiness." {
				found = true
			}
			if m.Text == private {
				t.Fatal("operator-worker private source leaked into PO context")
			}
		}
		if !found {
			t.Fatal("PO task context missing handoff")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	view, err := s.state()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range view.Messages {
		if m.Text == "The assigned startup produced no confirmed readiness." {
			t.Fatal("private PO handoff appeared in operator-visible state")
		}
	}
}

func TestWorkerShareRejectsForeignSourceAndWrongScope(t *testing.T) {
	for _, scenario := range []string{"foreign source", "wrong recipient"} {
		t.Run(scenario, func(t *testing.T) {
			s, _, _, worker := continuationFixture(t)
			defer s.Close()
			source := newID()
			if _, err := s.userMessage(context.Background(), source, worker, "", "Tell the PO what you observed.", "", nil); err != nil {
				t.Fatal(err)
			}
			j, err := s.claim(nil)
			if err != nil || j == nil {
				t.Fatalf("claim: %v", err)
			}
			if scenario == "foreign source" {
				j.Source = newID()
			}
			action := runtimeapi.Action{Type: "share_with_po", Body: "Observation"}
			if scenario == "wrong recipient" {
				action.Recipient = root
			}
			if err := s.complete(*j, runtimeapi.Result{Text: "Done.", Actions: []runtimeapi.Action{action}}, nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			var count int
			if err := s.db.QueryRow(`SELECT count(*) FROM worker_po_handoffs`).Scan(&count); err != nil || count != 0 {
				t.Fatalf("unauthorized handoff=%d: %v", count, err)
			}
		})
	}
}
