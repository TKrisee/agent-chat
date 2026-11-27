package service

import (
	"database/sql"
	"net/http/httptest"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func TestDeferredParentWakeAfterPlanningCompletes(t *testing.T) {
	s, parent, po, worker := continuationFixture(t)
	defer s.Close()
	if err := s.complete(activeAssignment(t, s, parent, po), runtimeapi.Result{Actions: []runtimeapi.Action{
		{Type: "queue_work_item", Title: "First", Body: "Work", Recipient: worker},
		{Type: "queue_work_item", Title: "Dependent", Body: "Follow", Recipient: worker, DependsOn: []string{"First"}},
	}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	planning, err := s.claim(map[string]bool{worker: true})
	if err != nil || planning == nil || planning.Kind != "po_followup" {
		t.Fatalf("planning: %+v %v", planning, err)
	}
	work, err := s.claim(map[string]bool{po: true})
	if err != nil || work == nil || work.Kind != "work" {
		t.Fatalf("work: %+v %v", work, err)
	}
	if err := s.disposition(work.ID, "uncertain", "unknown outcome", 0); err != nil {
		t.Fatal(err)
	}
	var pending int
	if err := s.db.QueryRow(`SELECT count(*) FROM po_parent_outcome_events WHERE parent_task_id=? AND delivered_source_id IS NULL`, parent).Scan(&pending); err != nil || pending != 1 {
		t.Fatalf("pending=%d %v", pending, err)
	}
	if err := s.complete(*planning, runtimeapi.Result{Text: "Waiting"}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	wake, err := s.claim(map[string]bool{worker: true})
	if err != nil || wake == nil || wake.Kind != "po_followup" || wake.Task != parent {
		t.Fatalf("wake: %+v %v", wake, err)
	}
	if err := s.write(func(tx *sql.Tx) error { return s.reconcileProjectWakes(tx, "alpha") }); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM po_parent_outcome_events WHERE parent_task_id=? AND delivered_source_id IS NULL`, parent).Scan(&n); err != nil || n != 0 {
		t.Fatalf("undelivered=%d %v", n, err)
	}
	if err := s.complete(*wake, runtimeapi.Result{Text: "I will continue independent planning."}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	next, err := s.claim(map[string]bool{worker: true})
	if err != nil || next == nil || next.Kind != "po_followup" || next.Task != parent {
		t.Fatalf("follow-through stopped on dependency-waiting child: %+v %v", next, err)
	}
}

func TestStoppedProjectReconcilesParentOutcomeOnResume(t *testing.T) {
	s, parent, po, worker := continuationFixture(t)
	defer s.Close()
	if err := s.complete(activeAssignment(t, s, parent, po), runtimeapi.Result{Actions: []runtimeapi.Action{
		{Type: "queue_work_item", Title: "First", Body: "Work", Recipient: worker},
		{Type: "queue_work_item", Title: "Dependent", Body: "Follow", Recipient: worker, DependsOn: []string{"First"}},
	}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	planning, err := s.claim(map[string]bool{worker: true})
	if err != nil || planning == nil {
		t.Fatalf("planning: %v", err)
	}
	if err := s.complete(*planning, runtimeapi.Result{Text: "Waiting"}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	work, err := s.claim(map[string]bool{po: true})
	if err != nil || work == nil || work.Kind != "work" {
		t.Fatalf("work: %+v %v", work, err)
	}
	if _, err := s.db.Exec(`UPDATE projects SET stopped=1 WHERE id='alpha'`); err != nil {
		t.Fatal(err)
	}
	if err := s.disposition(work.ID, "uncertain", "unknown", 0); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM po_parent_outcome_events WHERE parent_task_id=? AND delivered_source_id IS NULL`, parent).Scan(&n); err != nil || n != 1 {
		t.Fatalf("pending=%d %v", n, err)
	}
	w := httptest.NewRecorder()
	s.setStop(w, "alpha", false)
	if w.Code != 200 {
		t.Fatalf("resume status=%d", w.Code)
	}
	wake, err := s.claim(map[string]bool{worker: true})
	if err != nil || wake == nil || wake.Kind != "po_followup" || wake.Task != parent {
		t.Fatalf("wake: %+v %v", wake, err)
	}
}
