package service

import (
	"database/sql"
	"net/http/httptest"
	"strings"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func TestRootHoldApprovalOutcome(t *testing.T) {
	for _, decision := range []string{"approved", "denied"} {
		t.Run(decision, func(t *testing.T) {
			s, task, po, _ := continuationFixture(t)
			defer s.Close()
			j := activeAssignment(t, s, task, po)
			if err := s.complete(j, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "hold_task", Project: "alpha", Recipient: root, Body: "Choose the product option."}}}, nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			rootJob, err := s.claim(nil)
			if err != nil || rootJob == nil {
				t.Fatalf("Root wake %v %v", rootJob, err)
			}
			if err := s.complete(*rootJob, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "request_approval", Project: "alpha", Title: "Choose option", Body: "Approve option A?"}}}, nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			var approval string
			var notified int
			if err := s.db.QueryRow(`SELECT id FROM approvals WHERE task_id=? AND requested_by=?`, task, root).Scan(&approval); err != nil {
				t.Fatal(err)
			}
			if err := s.db.QueryRow(`SELECT count(*) FROM approval_deliveries WHERE approval_id=?`, approval).Scan(&notified); err != nil || notified != 1 {
				t.Fatalf("notification=%d %v", notified, err)
			}
			req := httptest.NewRequest("POST", "/", strings.NewReader(`{"decision":"`+decision+`"}`))
			req.Header.Set("Content-Type", "application/json")
			req.SetPathValue("id", approval)
			w := httptest.NewRecorder()
			s.decideApproval(w, req)
			if w.Code != 200 {
				t.Fatalf("decision %d %s", w.Code, w.Body.String())
			}
			retry := httptest.NewRequest("POST", "/", strings.NewReader(`{"decision":"`+decision+`"}`))
			retry.Header.Set("Content-Type", "application/json")
			retry.SetPathValue("id", approval)
			retryResult := httptest.NewRecorder()
			s.decideApproval(retryResult, retry)
			if retryResult.Code != 200 {
				t.Fatalf("idempotent retry %d", retryResult.Code)
			}
			var status string
			var n int
			if err := s.db.QueryRow(`SELECT status FROM task_holds WHERE job_id=?`, j.ID).Scan(&status); err != nil {
				t.Fatal(err)
			}
			if err := s.db.QueryRow(`SELECT count(*) FROM jobs WHERE task_id=? AND kind='po_followup'`, task).Scan(&n); err != nil {
				t.Fatal(err)
			}
			if decision == "approved" && (status != "resolved" || n != 1) {
				t.Fatalf("approved hold=%s jobs=%d", status, n)
			}
			if decision == "denied" && (status != "active" || n != 0) {
				t.Fatalf("denied hold=%s jobs=%d", status, n)
			}
			if decision == "denied" {
				if err := s.write(func(tx *sql.Tx) error {
					h, found, err := s.activeTaskHold(tx, task)
					if err != nil || !found {
						t.Fatalf("active hold missing: %v", err)
					}
					ok, err := s.holdCanResolve(tx, h)
					if err != nil {
						return err
					}
					if ok {
						t.Fatal("denied hold was resolvable")
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestRootHoldApprovalRejectsForeignSource(t *testing.T) {
	s, task, po, _ := continuationFixture(t)
	defer s.Close()
	j := activeAssignment(t, s, task, po)
	if err := s.complete(j, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "hold_task", Project: "alpha", Recipient: root, Body: "Choose a product option."}}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	wake, err := s.claim(nil)
	if err != nil || wake == nil {
		t.Fatalf("wake %v %v", wake, err)
	}
	wake.Source = newID()
	err = s.write(func(tx *sql.Tx) error {
		return s.requestRootHoldApproval(tx, *wake, runtimeapi.Action{Type: "request_approval", Project: "alpha", Title: "Choose", Body: "Approve A?"})
	})
	if err == nil {
		t.Fatal("foreign source accepted")
	}
	var count int
	if err := s.db.QueryRow(`SELECT count(*) FROM root_hold_approvals`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("links %d %v", count, err)
	}
}

func TestSecondRootOwnedHoldCanAskOperatorWithoutAnotherAutoResolution(t *testing.T) {
	s, task, po, _ := continuationFixture(t)
	defer s.Close()
	first := activeAssignment(t, s, task, po)
	if err := s.complete(first, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "hold_task", Project: "alpha", Recipient: root, Body: "Choose a scoped technical step."}}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	wake, err := s.claim(nil)
	if err != nil || wake == nil {
		t.Fatalf("first wake %v %v", wake, err)
	}
	if err = s.complete(*wake, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "resolve_hold", Project: "alpha", Task: task, Body: "Use the permitted diagnostic."}}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	follow, err := s.claim(nil)
	if err != nil || follow == nil {
		t.Fatalf("followup %v %v", follow, err)
	}
	if err = s.complete(*follow, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "hold_task", Project: "alpha", Recipient: root, Body: "The operator must choose the art direction."}}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	second, err := s.claim(nil)
	if err != nil || second == nil {
		t.Fatalf("second wake %v %v", second, err)
	}
	if err = s.complete(*second, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "request_approval", Project: "alpha", Title: "Art direction", Body: "Approve option A?"}}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = s.db.QueryRow(`SELECT count(*) FROM root_hold_approvals x JOIN task_holds h ON h.job_id=x.hold_job_id WHERE h.job_id=? AND h.status='active'`, follow.ID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("linked approval=%d err=%v", count, err)
	}
}
