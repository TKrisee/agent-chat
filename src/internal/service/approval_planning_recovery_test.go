package service

import (
	"database/sql"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func TestApprovalPlanningRecovery(t *testing.T) {
	for _, fence := range []string{"valid", "child uncertain", "stop", "parent uncertain"} {
		t.Run(fence, func(t *testing.T) {
			s, parent, child, _ := parentReportFixture(t)
			defer s.Close()
			installReviewInspector(t, s)
			worker := newID()
			if _, err := s.db.Exec("INSERT INTO agents VALUES(?,'Independent','worker','alpha','model','medium')", worker); err != nil {
				t.Fatal(err)
			}
			var po, failed string
			if err := s.db.QueryRow("SELECT owner_id FROM tasks WHERE id=?", parent).Scan(&po); err != nil {
				t.Fatal(err)
			}
			if err := s.db.QueryRow("SELECT id FROM jobs WHERE task_id=? AND diagnostic='invalid runtime result: active worker result unavailable'", parent).Scan(&failed); err != nil {
				t.Fatal(err)
			}
			prior := `{"text":"","actions":[{"type":"accept_result","project":"alpha","title":"Existing child","body":"review","recipient":""}],"attachments":[]}`
			approved := `{"text":"","actions":[{"type":"queue_work_item","project":"alpha","title":"Next","body":"Independent","recipient":"worker"}],"attachments":[]}`
			decision := newID()
			if err := s.write(func(tx *sql.Tx) error {
				if _, err := tx.Exec("UPDATE jobs SET result=? WHERE id=?", []byte(prior), failed); err != nil {
					return err
				}
				if _, err := tx.Exec("INSERT INTO approvals VALUES(?,'alpha',?,?,'Roster','Approved scope','approved')", newID(), parent, po); err != nil {
					return err
				}
				if err := addMessage(tx, decision, operator, po, "operator", "Operator approved “Roster”.\nApproved scope", "alpha", parent); err != nil {
					return err
				}
				_, err := tx.Exec(`INSERT INTO jobs(id,agent_id,project_id,task_id,source_id,kind,status,request,result,diagnostic,next_at) VALUES(?,?,'alpha',?,?,'approval_approved','completed',?,?,'invalid runtime result: parent task unavailable',0)`, newID(), po, parent, decision, []byte("{}"), []byte(approved))
				return err
			}); err != nil {
				t.Fatal(err)
			}
			switch fence {
			case "stop":
				_, _ = s.db.Exec("UPDATE projects SET stopped=1 WHERE id='alpha'")
			case "parent uncertain":
				_, _ = s.db.Exec("UPDATE jobs SET status='uncertain' WHERE id=?", failed)
			case "child uncertain":
				_, _ = s.db.Exec("UPDATE jobs SET status='uncertain' WHERE task_id=? AND kind='work'", child)
			}
			id := newID()
			want := 202
			if fence == "stop" || fence == "parent uncertain" {
				want = 409
			}
			if got := postContinuationID(s, parent, "Continue independent work without replay.", id); got != want {
				t.Fatalf("status=%d want=%d", got, want)
			}
			if want != 202 {
				return
			}
			if got := postContinuationID(s, parent, "Continue independent work without replay.", id); got != 202 {
				t.Fatalf("retry=%d", got)
			}
			follow := followupClaim(t, s)
			found := false
			for _, m := range follow.Request.Context {
				if m.ID == decision {
					found = true
				}
			}
			if !found {
				t.Fatal("approved decision not pinned")
			}
			if err := s.complete(follow, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "queue_work_item", Project: "alpha", Title: "New independent work", Body: "Inspect", Recipient: worker}}}, nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			var n int
			if err := s.db.QueryRow("SELECT count(*) FROM po_work_items WHERE parent_task_id=?", parent).Scan(&n); err != nil || n != 2 {
				t.Fatalf("items=%d %v", n, err)
			}
			var status string
			if err := s.db.QueryRow("SELECT status FROM approvals WHERE task_id=?", parent).Scan(&status); err != nil || status != "approved" {
				t.Fatalf("approval=%s %v", status, err)
			}
		})
	}
}
