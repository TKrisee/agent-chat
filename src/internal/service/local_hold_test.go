package service

import (
	"database/sql"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func TestLocalHoldWakesRootPrivatelyWithoutPromptingOperator(t *testing.T) {
	for _, protected := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "private task input"}[protected], func(t *testing.T) {
			s, task, po, _ := continuationFixture(t)
			defer s.Close()
			var j job
			if protected {
				if code := postContinuation(s, task, "private operator sentinel"); code != 202 {
					t.Fatalf("continuation %d", code)
				}
				claimed, err := s.claim(nil)
				if err != nil || claimed == nil || claimed.Kind != "po_continue" {
					t.Fatalf("claim %v", err)
				}
				j = *claimed
			} else {
				j = activeAssignment(t, s, task, po)
			}
			const reason = "local technical blocker and private operator sentinel"
			if err := s.complete(j, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "hold_task", Body: reason}}}, nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			var status string
			var marker, rootJobs, operatorMessages, rootReason, ownerReason int
			if err := s.db.QueryRow(`SELECT status FROM tasks WHERE id=?`, task).Scan(&status); err != nil {
				t.Fatal(err)
			}
			if err := s.db.QueryRow(`SELECT count(*) FROM task_local_holds l JOIN task_holds h ON h.job_id=l.job_id WHERE h.task_id=? AND h.status='active'`, task).Scan(&marker); err != nil {
				t.Fatal(err)
			}
			if err := s.db.QueryRow(`SELECT count(*) FROM jobs WHERE task_id=? AND kind='task_blocked'`, task).Scan(&rootJobs); err != nil {
				t.Fatal(err)
			}
			if err := s.db.QueryRow(`SELECT count(*) FROM messages WHERE task_id=? AND recipient=? AND text=?`, task, operator, reason).Scan(&operatorMessages); err != nil {
				t.Fatal(err)
			}
			if err := s.db.QueryRow(`SELECT count(*) FROM messages WHERE task_id=? AND recipient=? AND text=?`, task, root, reason).Scan(&rootReason); err != nil {
				t.Fatal(err)
			}
			if err := s.db.QueryRow(`SELECT count(*) FROM messages WHERE task_id=? AND recipient=? AND text=?`, task, po, reason).Scan(&ownerReason); err != nil {
				t.Fatal(err)
			}
			if status != "blocked" || marker != 1 || rootJobs != 1 || operatorMessages != 0 || (!protected && rootReason != 1) || (protected && (rootReason != 0 || ownerReason != 1)) {
				t.Fatalf("status=%s marker=%d root jobs=%d operator=%d root reason=%d owner reason=%d", status, marker, rootJobs, operatorMessages, rootReason, ownerReason)
			}
			wake, err := s.claim(nil)
			if err != nil || wake == nil || !wake.Request.LocalHoldSupervisor {
				t.Fatalf("private Root wake: %v", err)
			}
			if protected {
				for _, m := range wake.Request.Context {
					if m.Text == reason {
						t.Fatal("protected hold reason reached Root")
					}
				}
			}
			if err := s.complete(*wake, runtimeapi.Result{Text: "No independent route established."}, nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			id := newID()
			if code := postOperatorHold(s, task, "alpha", "Continue within scope.", id); code != 202 {
				t.Fatalf("explicit resume %d", code)
			}
			if code := postOperatorHold(s, task, "alpha", "Continue within scope.", id); code != 202 {
				t.Fatalf("idempotent resume %d", code)
			}
		})
	}
}

func TestLocalChildHoldDoesNotStopIndependentWork(t *testing.T) {
	s, review, parent, _, other := childReviewFixture(t)
	defer s.Close()
	if err := s.write(func(tx *sql.Tx) error {
		po, err := getAgent(tx, review.Agent)
		if err != nil {
			return err
		}
		return s.queuePOWorkItem(tx, job{Task: parent, Project: review.Project}, po, runtimeapi.Action{Title: "Independent", Body: "Proceed within scope", Recipient: other})
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.complete(review, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "hold_task", Body: "Child requires local investigation."}}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	claimed, err := s.claim(map[string]bool{review.Agent: true})
	if err != nil || claimed == nil || claimed.Kind != "work" || claimed.Agent != other {
		t.Fatalf("independent work unavailable: %v", err)
	}
}

func TestInternalRootWakeRejectsUnsupportedApproval(t *testing.T) {
	s, task, po, _ := continuationFixture(t)
	defer s.Close()
	j := activeAssignment(t, s, task, po)
	if err := s.complete(j, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "hold_task", Recipient: root, Body: "Root should review this prerequisite."}}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	wake, err := s.claim(nil)
	if err != nil || wake == nil || wake.Kind != "task_blocked" {
		t.Fatalf("Root wake: %v", err)
	}
	if err := s.complete(*wake, runtimeapi.Result{Text: "Internal note", Actions: []runtimeapi.Action{{Type: "request_approval", Title: "Choose", Body: "Please decide."}}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	var approvals, outward int
	if err := s.db.QueryRow(`SELECT count(*) FROM approvals WHERE task_id=?`, task).Scan(&approvals); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT count(*) FROM messages WHERE task_id=? AND sender=? AND recipient=?`, task, root, operator).Scan(&outward); err != nil {
		t.Fatal(err)
	}
	if approvals != 0 || outward != 0 {
		t.Fatalf("approvals=%d outward=%d", approvals, outward)
	}
}
