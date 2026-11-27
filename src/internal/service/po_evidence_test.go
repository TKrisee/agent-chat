package service

import (
	"context"
	"database/sql"
	"net/http/httptest"
	"strings"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func TestPOEvidenceCoalescesAndRecordsConsumption(t *testing.T) {
	s, task, po, worker := continuationFixture(t)
	defer s.Close()
	if _, err := s.db.Exec(`UPDATE tasks SET status='blocked' WHERE id=?`, task); err != nil {
		t.Fatal(err)
	}
	if err := s.write(func(tx *sql.Tx) error {
		return addMessage(tx, newID(), operator, po, "operator", "unrelated private PO message", "alpha", "")
	}); err != nil {
		t.Fatal(err)
	}
	rootSource := newID()
	if _, err := s.userMessage(context.Background(), rootSource, root, "", "private root request", "", nil); err != nil {
		t.Fatal(err)
	}
	rj, err := s.claim(nil)
	if err != nil || rj == nil || rj.Source != rootSource {
		t.Fatalf("root: %+v %v", rj, err)
	}
	if err := s.complete(*rj, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "handoff_evidence", Project: "alpha", Body: "Root evidence summary"}}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	workerSource := newID()
	if _, err := s.userMessage(context.Background(), workerSource, worker, "", "private worker request", "", nil); err != nil {
		t.Fatal(err)
	}
	wj, err := s.claim(map[string]bool{po: true})
	if err != nil || wj == nil || wj.Agent != worker {
		t.Fatalf("worker: %+v %v", wj, err)
	}
	if err := s.complete(*wj, runtimeapi.Result{Text: "Shared.", Actions: []runtimeapi.Action{{Type: "share_with_po", Body: "Worker observation"}}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	first, err := s.claim(nil)
	if err != nil || first == nil || first.Kind != "po_evidence" || first.Agent != po {
		t.Fatalf("first: %+v %v", first, err)
	}
	if !strings.Contains(first.Request.Instructions, "Root evidence summary") || strings.Contains(first.Request.Instructions, "private root request") || strings.Contains(first.Request.Instructions, "Worker observation") {
		t.Fatal("first wake has incorrect snapshot")
	}
	for _, m := range first.Request.Context {
		if strings.Contains(m.Text, "unrelated private PO message") || strings.Contains(m.Text, "private worker request") {
			t.Fatal("unrelated private message in evidence context")
		}
	}
	var consumed int
	if err := s.db.QueryRow(`SELECT count(*) FROM po_evidence_wake_items WHERE consumed_job_id IS NOT NULL`).Scan(&consumed); err != nil || consumed != 0 {
		t.Fatalf("premature consumed=%d %v", consumed, err)
	}
	if err := s.complete(*first, runtimeapi.Result{Text: "Reviewed the root evidence."}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.write(func(tx *sql.Tx) error {
		a, err := getAgent(tx, po)
		if err != nil {
			return err
		}
		messages, err := s.runtimeContext(tx, a, "alpha", task, "", "assignment")
		if err != nil {
			return err
		}
		for _, m := range messages {
			if m.Text == "Reviewed the root evidence." {
				return nil
			}
		}
		t.Fatal("assessment absent from later task context")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	second, err := s.claim(nil)
	if err != nil || second == nil || second.Kind != "po_evidence" || !strings.Contains(second.Request.Instructions, "Worker observation") || strings.Contains(second.Request.Instructions, "private worker request") {
		t.Fatalf("second: %+v %v", second, err)
	}
	if err := s.disposition(second.ID, "uncertain", "unknown", 0); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT count(*) FROM po_evidence_wake_items WHERE consumed_job_id IS NOT NULL`).Scan(&consumed); err != nil || consumed != 1 {
		t.Fatalf("consumed=%d %v", consumed, err)
	}
	if duplicate, err := s.claim(nil); err != nil || duplicate != nil {
		t.Fatalf("duplicate: %+v %v", duplicate, err)
	}
	var status string
	if err := s.db.QueryRow(`SELECT status FROM tasks WHERE id=?`, task).Scan(&status); err != nil || status != "blocked" {
		t.Fatalf("task=%s %v", status, err)
	}
}

func TestRejectedPOEvidenceReleasedWithoutAutomaticReplay(t *testing.T) {
	s, task, po, _ := continuationFixture(t)
	defer s.Close()
	post := func(body string) {
		source := newID()
		if _, err := s.userMessage(context.Background(), source, root, "", "Share this evidence", "", nil); err != nil {
			t.Fatal(err)
		}
		j, err := s.claim(nil)
		if err != nil || j == nil || j.Source != source {
			t.Fatalf("root: %+v %v", j, err)
		}
		if err := s.complete(*j, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "handoff_evidence", Project: "alpha", Body: body}}}, nil, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	post("First evidence")
	first, err := s.claim(nil)
	if err != nil || first == nil || first.Kind != "po_evidence" {
		t.Fatalf("wake: %+v %v", first, err)
	}
	if err := s.complete(*first, runtimeapi.Result{}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	var reservations, rejections int
	if err := s.db.QueryRow(`SELECT (SELECT count(*) FROM po_evidence_wake_items WHERE source_id=?),(SELECT count(*) FROM po_evidence_rejections WHERE job_id=?)`, first.Source, first.ID).Scan(&reservations, &rejections); err != nil || reservations != 0 || rejections != 1 {
		t.Fatalf("reservation=%d rejection=%d %v", reservations, rejections, err)
	}
	if j, err := s.claim(nil); err != nil || j != nil {
		t.Fatalf("automatic replay: %+v %v", j, err)
	}
	if err := s.write(func(tx *sql.Tx) error {
		a, e := getAgent(tx, po)
		if e != nil {
			return e
		}
		messages, e := s.runtimeContext(tx, a, "alpha", task, "", "assignment")
		if e != nil {
			return e
		}
		for _, m := range messages {
			if m.Text == "First evidence" {
				return nil
			}
		}
		t.Fatal("released evidence missing from authorized task context")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	post("New evidence")
	next, err := s.claim(nil)
	if err != nil || next == nil || next.Kind != "po_evidence" || strings.Contains(next.Request.Instructions, "First evidence") || !strings.Contains(next.Request.Instructions, "New evidence") {
		t.Fatalf("next wake: %+v %v", next, err)
	}
}

func TestPOEvidenceWaitsForProjectResume(t *testing.T) {
	s, task, po, _ := continuationFixture(t)
	defer s.Close()
	if _, err := s.db.Exec(`UPDATE projects SET stopped=1 WHERE id='alpha'`); err != nil {
		t.Fatal(err)
	}
	source := newID()
	if _, err := s.userMessage(context.Background(), source, root, "", "Pass the evidence", "", nil); err != nil {
		t.Fatal(err)
	}
	j, err := s.claim(nil)
	if err != nil || j == nil || j.Source != source {
		t.Fatalf("root: %+v %v", j, err)
	}
	if err := s.complete(*j, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "handoff_evidence", Project: "alpha", Body: "Evidence summary"}}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if wake, err := s.claim(nil); err != nil || wake != nil {
		t.Fatalf("stopped wake: %+v %v", wake, err)
	}
	w := httptest.NewRecorder()
	s.setStop(w, "alpha", false)
	if w.Code != 200 {
		t.Fatalf("resume=%d", w.Code)
	}
	wake, err := s.claim(nil)
	if err != nil || wake == nil || wake.Kind != "po_evidence" || wake.Agent != po {
		t.Fatalf("wake: %+v %v", wake, err)
	}
	var status string
	if err := s.db.QueryRow(`SELECT status FROM tasks WHERE id=?`, task).Scan(&status); err != nil || status != "assigned" {
		t.Fatalf("task=%s %v", status, err)
	}
}

func TestPOEvidenceReassignmentKeepsPriorHandoffPrivate(t *testing.T) {
	s, _, oldPO, _ := continuationFixture(t)
	defer s.Close()
	post := func(body string) {
		source := newID()
		if _, err := s.userMessage(context.Background(), source, root, "", "Share evidence", "", nil); err != nil {
			t.Fatal(err)
		}
		j, err := s.claim(map[string]bool{oldPO: true})
		if err != nil || j == nil || j.Source != source {
			t.Fatalf("root: %+v %v", j, err)
		}
		if err := s.complete(*j, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "handoff_evidence", Project: "alpha", Body: body}}}, nil, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	post("Old private summary")
	newPO := newID()
	if _, err := s.db.Exec(`INSERT INTO agents VALUES(?,'New PO','po','alpha','model','medium')`, newPO); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE projects SET po_id=? WHERE id='alpha'`, newPO); err != nil {
		t.Fatal(err)
	}
	if j, err := s.claim(nil); err != nil || j != nil {
		t.Fatalf("former PO wake: %+v %v", j, err)
	}
	post("New private summary")
	j, err := s.claim(nil)
	if err != nil || j == nil || j.Agent != newPO || j.Kind != "po_evidence" {
		t.Fatalf("new PO wake: %+v %v", j, err)
	}
	if strings.Contains(j.Request.Instructions, "Old private summary") || !strings.Contains(j.Request.Instructions, "New private summary") {
		t.Fatal("cross-owner evidence leaked or new summary missing")
	}
}
