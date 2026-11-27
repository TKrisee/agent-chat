package service

import (
	"agentchat/src/internal/runtimeapi"
	"database/sql"
	"testing"
)

func TestLocalHoldInvestigationPreservesHoldAndPinsAssessment(t *testing.T) {
	s, task, po, _ := continuationFixture(t)
	defer s.Close()
	origin := newID()
	if err := s.write(func(tx *sql.Tx) error {
		if err := addMessage(tx, origin, operator, root, "operator", "Authorized scope", "", ""); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT INTO task_origins(task_id,message_id) VALUES(?,?)`, task, origin)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	j := activeAssignment(t, s, task, po)
	if err := s.complete(j, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "hold_task", Body: "Technical prerequisite unavailable."}}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	supervisor, err := s.claim(nil)
	if err != nil || supervisor == nil || !supervisor.Request.LocalHoldSupervisor {
		t.Fatalf("supervisor: %v", err)
	}
	if err = s.complete(*supervisor, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "investigate_hold", Project: "alpha", Task: task, Body: "Assess the existing evidence."}}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	investigation, err := s.claim(nil)
	if err != nil || investigation == nil || investigation.Kind != "po_hold_investigation" {
		t.Fatalf("investigation: %v", err)
	}
	if err = s.complete(*investigation, runtimeapi.Result{Text: "Private assessment of the held prerequisite."}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	var status, hold string
	if err = s.db.QueryRow(`SELECT t.status,h.status FROM tasks t JOIN task_holds h ON h.task_id=t.id WHERE t.id=?`, task).Scan(&status, &hold); err != nil || status != "blocked" || hold != "active" {
		t.Fatalf("hold %s/%s: %v", status, hold, err)
	}
	err = s.write(func(tx *sql.Tx) error {
		a, e := getAgent(tx, po)
		if e != nil {
			return e
		}
		messages, e := s.runtimeContext(tx, a, "alpha", task, "", "assignment")
		if e != nil {
			return e
		}
		for _, m := range messages {
			if m.Text == "Private assessment of the held prerequisite." {
				return nil
			}
		}
		t.Error("assessment absent from later same-task PO context")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
