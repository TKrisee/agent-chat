package service

import (
	"strings"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func TestMixedApprovalCorrectsWithoutApplyingBatch(t *testing.T) {
	s, task, po, _ := continuationFixture(t)
	defer s.Close()
	j := activeAssignment(t, s, task, po)
	bad := runtimeapi.Result{Actions: []runtimeapi.Action{
		{Type: "queue_worker", Project: "alpha", Body: "Independent work."},
		{Type: "request_approval", Project: "alpha", Title: "Decision", Body: "Choose scope."},
	}}
	if err := s.complete(j, bad, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	var work, approvals int
	if err := s.db.QueryRow("SELECT count(*) FROM jobs WHERE task_id=? AND kind='work'", task).Scan(&work); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow("SELECT count(*) FROM approvals WHERE task_id=?", task).Scan(&approvals); err != nil {
		t.Fatal(err)
	}
	if work != 0 || approvals != 0 {
		t.Fatalf("rejected batch applied work=%d approvals=%d", work, approvals)
	}
	follow := followupClaim(t, s)
	if follow.Task != task {
		t.Fatalf("wrong task %s", follow.Task)
	}
	brief := false
	for _, m := range follow.Request.Context {
		if m.Text == "authoritative final assignment" {
			brief = true
		}
	}
	if !brief {
		t.Fatal("original brief missing")
	}
	if !strings.Contains(strings.ToLower(follow.Request.Instructions), "approval") {
		t.Fatal("correction missing approval feedback")
	}
	if err := s.complete(follow, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "queue_worker", Project: "alpha", Body: "Independent work."}}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow("SELECT count(*) FROM jobs WHERE task_id=? AND kind='work'", task).Scan(&work); err != nil || work != 1 {
		t.Fatalf("corrected work=%d: %v", work, err)
	}
}

func TestInitialPlanningCorrectionDispatchesWorkItem(t *testing.T) {
	s, task, po, _ := continuationFixture(t)
	defer s.Close()
	j := activeAssignment(t, s, task, po)
	bad := runtimeapi.Result{Actions: []runtimeapi.Action{
		{Type: "queue_work_item", Project: "alpha", Title: "Bad", Body: "Work", Recipient: "missing"},
	}}
	if err := s.complete(j, bad, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	follow := followupClaim(t, s)
	good := runtimeapi.Result{Actions: []runtimeapi.Action{
		{Type: "create_worker", Project: "alpha", Title: "CorrectedWorker"},
		{Type: "queue_work_item", Project: "alpha", Title: "Corrected", Body: "Work", Recipient: "CorrectedWorker"},
	}}
	if err := s.complete(follow, good, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM po_work_items WHERE parent_task_id=?`, task).Scan(&n); err != nil || n != 1 {
		t.Fatalf("corrected work items=%d: %v", n, err)
	}
}

func TestSecondPlanningCorrectionDispatchesWorkItem(t *testing.T) {
	s, task, po, _ := continuationFixture(t)
	defer s.Close()
	bad := runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "queue_work_item", Project: "alpha", Title: "Bad", Body: "Work", Recipient: "missing"}}}
	if err := s.complete(activeAssignment(t, s, task, po), bad, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.complete(followupClaim(t, s), bad, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	good := runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "queue_work_item", Project: "alpha", Title: "Corrected", Body: "Work", Recipient: "Worker"}}}
	if err := s.complete(followupClaim(t, s), good, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM po_work_items WHERE parent_task_id=?`, task).Scan(&n); err != nil || n != 1 {
		t.Fatalf("corrected work items=%d: %v", n, err)
	}
}

func TestMixedApprovalCorrectionIsBounded(t *testing.T) {
	s, task, po, _ := continuationFixture(t)
	defer s.Close()
	j := activeAssignment(t, s, task, po)
	bad := runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "queue_worker", Project: "alpha", Body: "Work."}, {Type: "request_approval", Project: "alpha", Title: "Decision", Body: "Choose."}}}
	for i := 0; i < 3; i++ {
		if err := s.complete(j, bad, nil, nil, nil); err != nil {
			t.Fatal(err)
		}
		if i < 2 {
			j = followupClaim(t, s)
		}
	}
	var status string
	var n int
	if err := s.db.QueryRow("SELECT status FROM tasks WHERE id=?", task).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow("SELECT count(*) FROM po_followups f JOIN messages m ON m.id=f.source_id WHERE m.task_id=?", task).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if status != "blocked" || n != 2 {
		t.Fatalf("status=%s corrections=%d", status, n)
	}
}
