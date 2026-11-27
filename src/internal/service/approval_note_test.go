package service

import (
	"agentchat/src/internal/runtimeapi"
	"database/sql"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestApprovalDecisionNoteIsDurableAndPrivate(t *testing.T) {
	for _, decision := range []string{"approved", "denied"} {
		t.Run(decision, func(t *testing.T) {
			s, task, po, _ := continuationFixture(t)
			defer s.Close()
			id := newID()
			if err := s.write(func(tx *sql.Tx) error {
				_, err := tx.Exec(`INSERT INTO approvals VALUES(?,'alpha',?,?,'Decision','Choose.','pending')`, id, task, po)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			post := func(payload string) int {
				r := httptest.NewRequest("POST", "/api/approvals/"+id, strings.NewReader(payload))
				r.Header.Set("Content-Type", "application/json")
				r.SetPathValue("id", id)
				w := httptest.NewRecorder()
				s.decideApproval(w, r)
				return w.Code
			}
			if got := post(`{"decision":"` + decision + `","note":"  scoped note  "}`); got != 200 {
				t.Fatalf("decision status %d", got)
			}
			if got := post(`{"decision":"` + decision + `","note":"different retry"}`); got != 409 {
				t.Fatalf("retry %d", got)
			}
			if got := post(`{"decision":"` + decision + `"}`); got != 200 {
				t.Fatalf("legacy retry %d", got)
			}
			state, err := s.state()
			if err != nil {
				t.Fatal(err)
			}
			if len(state.Approvals) != 1 || state.Approvals[0].DecisionNote != "scoped note" {
				t.Fatalf("notes %+v", state.Approvals)
			}
			j, err := s.claim(nil)
			if err != nil || j == nil || j.Kind != "approval_"+decision {
				t.Fatalf("job %+v %v", j, err)
			}
			found := strings.Contains(j.Request.Instructions, "scoped note")
			for _, m := range j.Request.Context {
				if strings.Contains(m.Text, "scoped note") {
					found = true
				}
			}
			if !found {
				t.Fatal("decision note missing from PO context")
			}
			var n int
			if err := s.db.QueryRow(`SELECT count(*) FROM messages WHERE room_id='group:alpha' AND instr(text,'scoped note')>0`).Scan(&n); err != nil || n != 0 {
				t.Fatalf("group leak %d %v", n, err)
			}
		})
	}
}

func TestDeniedDecisionSchedulesIndependentPlanning(t *testing.T) {
	s, task, po, _ := continuationFixture(t)
	defer s.Close()
	id := newID()
	if _, err := s.db.Exec(`INSERT INTO approvals VALUES(?,'alpha',?,?,'Decision','Do not deploy.','pending')`, id, task, po); err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("POST", "/api/approvals/"+id, strings.NewReader(`{"decision":"denied","note":"Keep review independent."}`))
	r.Header.Set("Content-Type", "application/json")
	r.SetPathValue("id", id)
	w := httptest.NewRecorder()
	s.decideApproval(w, r)
	if w.Code != 200 {
		t.Fatalf("decision: %d", w.Code)
	}
	j, err := s.claim(nil)
	if err != nil || j == nil || j.Kind != "approval_denied" {
		t.Fatalf("claim: %+v %v", j, err)
	}
	if err := s.complete(*j, runtimeapi.Result{Text: "Understood."}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	follow := followupClaim(t, s)
	found := false
	for _, m := range follow.Request.Context {
		if strings.Contains(m.Text, "Keep review independent.") && strings.Contains(m.Text, "denied") {
			found = true
		}
	}
	if !found {
		t.Fatal("exact denial missing from follow-up context")
	}
	if err := s.complete(follow, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "queue_work_item", Project: "alpha", Recipient: "Worker", Title: "Independent review", Body: "Review independently."}}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM po_work_items WHERE parent_task_id=?`, task).Scan(&n); err != nil || n != 1 {
		t.Fatalf("independent work=%d: %v", n, err)
	}
}

func TestPendingApprovalStopsTaskReport(t *testing.T) {
	s, task, po, _ := continuationFixture(t)
	defer s.Close()
	if _, err := s.db.Exec(`INSERT INTO approvals VALUES(?,'alpha',?,?,'Decision','Choose.','pending')`, newID(), task, po); err != nil {
		t.Fatal(err)
	}
	err := s.write(func(tx *sql.Tx) error {
		a, err := getAgent(tx, po)
		if err != nil {
			return err
		}
		return s.applyAction(tx, a, job{Task: task, Project: "alpha", Agent: po}, runtimeapi.Action{Type: "report", Body: "Done"})
	})
	if err == nil || !strings.Contains(err.Error(), "pending operator approval") {
		t.Fatalf("report error: %v", err)
	}
}

func TestDeniedTurnCannotDispatch(t *testing.T) {
	s, task, po, _ := continuationFixture(t)
	defer s.Close()
	err := s.write(func(tx *sql.Tx) error {
		return s.applyResult(tx, job{Task: task, Project: "alpha", Agent: po, Kind: "approval_denied", Request: runtimeapi.Request{Role: "po"}}, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "queue_work_item", Project: "alpha", Recipient: "Worker", Title: "Denied", Body: "Work"}}}, nil)
	})
	if err == nil || !strings.Contains(err.Error(), "denial cannot dispatch") {
		t.Fatalf("denied dispatch error: %v", err)
	}
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM po_work_items WHERE parent_task_id=?`, task).Scan(&n); err != nil || n != 0 {
		t.Fatalf("work=%d: %v", n, err)
	}
}

func TestPendingApprovalStopsNewWorkOnSameTask(t *testing.T) {
	s, task, po, _ := continuationFixture(t)
	defer s.Close()
	if _, err := s.db.Exec(`INSERT INTO approvals VALUES(?,'alpha',?,?,'Decision','Choose.','pending')`, newID(), task, po); err != nil {
		t.Fatal(err)
	}
	err := s.write(func(tx *sql.Tx) error {
		return s.applyResult(tx, job{Task: task, Project: "alpha", Agent: po, Kind: "po_followup", Request: runtimeapi.Request{Role: "po"}}, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "queue_work_item", Project: "alpha", Recipient: "Worker", Title: "New", Body: "Work"}}}, nil)
	})
	if err == nil || !strings.Contains(err.Error(), "pending operator approval") {
		t.Fatalf("dispatch error: %v", err)
	}
}

func TestApprovalDecisionKeepsExactReviewReceipt(t *testing.T) {
	for _, phase := range []string{"review", "accepted"} {
		t.Run(phase, func(t *testing.T) {
			s, review, _, worker, _ := childReviewFixture(t)
			defer s.Close()
			requester := review
			if phase == "accepted" {
				if err := s.complete(review, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "accept_result", Recipient: worker}}}, nil, nil, nil); err != nil {
					t.Fatal(err)
				}
				requester = followupClaim(t, s)
			}
			if err := s.complete(requester, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "request_approval", Project: "alpha", Title: "Choose", Body: "Decision required."}}}, nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			var approval string
			if err := s.db.QueryRow(`SELECT id FROM approvals WHERE task_id=? AND status='pending'`, review.Task).Scan(&approval); err != nil {
				t.Fatal(err)
			}
			r := httptest.NewRequest("POST", "/api/approvals/"+approval, strings.NewReader(`{"decision":"approved","note":"Use the selected option."}`))
			r.Header.Set("Content-Type", "application/json")
			r.SetPathValue("id", approval)
			w := httptest.NewRecorder()
			s.decideApproval(w, r)
			if w.Code != 200 {
				t.Fatalf("decision=%d", w.Code)
			}
			j, err := s.claim(nil)
			if err != nil || j == nil || j.Kind != "approval_approved" {
				t.Fatalf("approval job: %+v %v", j, err)
			}
			if err := s.complete(*j, runtimeapi.Result{Text: "Acknowledged."}, nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			follow := followupClaim(t, s)
			var note, receipt bool
			for _, m := range follow.Request.Context {
				if strings.Contains(m.Text, "Use the selected option.") {
					note = true
				}
				if m.ID == review.Source {
					receipt = true
				}
			}
			if !note || !receipt {
				t.Fatalf("note=%v receipt=%v", note, receipt)
			}
		})
	}
}
