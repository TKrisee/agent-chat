package service

import (
	"fmt"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func TestReceiptOnlyReviewPreservesParentApproval(t *testing.T) {
	for _, forbidden := range []bool{false, true} {
		t.Run(map[bool]string{false: "review receipt", true: "reject new work"}[forbidden], func(t *testing.T) {
			s, j, parent, worker, _ := childReviewFixture(t)
			defer s.Close()
			installReviewInspector(t, s)
			payload := fmt.Sprintf(`{"text":"","actions":[{"type":"accept_result","project":"alpha","title":"","body":"","recipient":%q},{"type":"report","project":"alpha","title":"","body":"Reviewed","recipient":""},{"type":"report_progress","project":"alpha","title":"","body":"Review complete","recipient":"root"}],"attachments":[]}`, worker)
			if _, err := s.db.Exec("UPDATE jobs SET status='completed',diagnostic=?,result=? WHERE id=?", legacyChildProgressDiagnostic, []byte(payload), j.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := s.db.Exec("UPDATE tasks SET status='blocked' WHERE id IN (?,?)", parent, j.Task); err != nil {
				t.Fatal(err)
			}
			approval := newID()
			if _, err := s.db.Exec("INSERT INTO approvals VALUES(?,'alpha',?,?,'Decision','Body','pending')", approval, parent, j.Agent); err != nil {
				t.Fatal(err)
			}
			if code := postContinuation(s, j.Task, "Review existing receipt only."); code != 202 {
				t.Fatalf("continuation %d", code)
			}
			follow := followupClaim(t, s)
			found := false
			for _, m := range follow.Request.Context {
				if m.ID == j.Source {
					found = true
				}
			}
			if !found {
				t.Fatal("worker receipt missing")
			}
			actions := []runtimeapi.Action{{Type: "accept_result", Project: "alpha", Recipient: worker}, {Type: "report", Project: "alpha", Recipient: root, Body: "Reviewed."}}
			if forbidden {
				actions = append(actions, runtimeapi.Action{Type: "queue_work_item", Project: "alpha", Title: "New", Body: "New work", Recipient: worker})
			}
			if err := s.complete(follow, runtimeapi.Result{Actions: actions}, nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			var parentStatus, approvalStatus, childStatus string
			var works int
			if err := s.db.QueryRow("SELECT status FROM tasks WHERE id=?", parent).Scan(&parentStatus); err != nil {
				t.Fatal(err)
			}
			if err := s.db.QueryRow("SELECT status FROM approvals WHERE id=?", approval).Scan(&approvalStatus); err != nil {
				t.Fatal(err)
			}
			if err := s.db.QueryRow("SELECT status FROM tasks WHERE id=?", j.Task).Scan(&childStatus); err != nil {
				t.Fatal(err)
			}
			if err := s.db.QueryRow("SELECT count(*) FROM jobs WHERE task_id=? AND kind='work'", j.Task).Scan(&works); err != nil {
				t.Fatal(err)
			}
			if parentStatus != "blocked" || approvalStatus != "pending" || works != 1 {
				t.Fatalf("parent=%s approval=%s work=%d", parentStatus, approvalStatus, works)
			}
			if !forbidden && childStatus != "completed" {
				t.Fatalf("child=%s", childStatus)
			}
			if forbidden && childStatus == "completed" {
				t.Fatal("forbidden batch applied")
			}
		})
	}
}
