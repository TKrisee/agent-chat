package service

import (
	"strings"
	"testing"
)

const mixedApprovalResult = `{"text":"","actions":[{"type":"create_worker","project":"alpha","title":"WorkerTwo","body":"","recipient":""},{"type":"queue_work_item","project":"alpha","title":"Inspect","body":"Inspect safely","recipient":"WorkerTwo"},{"type":"request_approval","project":"alpha","title":"Review","body":"Approval needed","recipient":""},{"type":"report_progress","project":"alpha","title":"","body":"Planning","recipient":"root"}],"attachments":[]}`

func TestLegacyMixedApprovalRecovery(t *testing.T) {
	for _, tc := range []struct {
		name, mode string
		want       int
	}{{"valid", "", 202}, {"stopped", "stop", 409}, {"prior approval", "approval", 409}, {"uncertain work", "uncertain", 409}, {"unsafe action", "unsafe", 409}} {
		t.Run(tc.name, func(t *testing.T) {
			s, task, po, _ := continuationFixture(t)
			defer s.Close()
			installReviewInspector(t, s)
			payload := mixedApprovalResult
			if tc.mode == "unsafe" {
				payload = strings.Replace(payload, `"create_worker"`, `"stop_project"`, 1)
			}
			if _, err := s.db.Exec(`UPDATE jobs SET diagnostic=?,result=? WHERE task_id=? AND kind='assignment'`, legacyApprovalDiagnostic, []byte(payload), task); err != nil {
				t.Fatal(err)
			}
			if _, err := s.db.Exec(`UPDATE tasks SET status='blocked' WHERE id=?`, task); err != nil {
				t.Fatal(err)
			}
			switch tc.mode {
			case "stop":
				_, _ = s.db.Exec(`UPDATE projects SET stopped=1 WHERE id='alpha'`)
			case "approval":
				_, _ = s.db.Exec(`INSERT INTO approvals VALUES(?,?,?,?,?,?,'pending')`, newID(), "alpha", task, po, "decision", "detail")
			case "uncertain":
				_, _ = s.db.Exec(`UPDATE jobs SET status='uncertain' WHERE task_id=? AND kind='assignment'`, task)
			}
			id := newID()
			if got := postContinuationID(s, task, "Continue safely.", id); got != tc.want {
				t.Fatalf("status %d want %d", got, tc.want)
			}
			if tc.want != 202 {
				return
			}
			if got := postContinuationID(s, task, "Continue safely.", id); got != 202 {
				t.Fatalf("idempotency %d", got)
			}
			j, err := s.claim(nil)
			if err != nil || j == nil || j.Kind != "po_followup" {
				t.Fatalf("claim %v %v", j, err)
			}
			found := false
			for _, m := range j.Request.Context {
				if m.Text == "authoritative final assignment" {
					found = true
				}
			}
			if !strings.Contains(j.Request.Instructions, "only action") || !found {
				t.Fatal("missing correction or original assignment")
			}
			var n int
			_ = s.db.QueryRow(`SELECT count(*) FROM approvals WHERE task_id=?`, task).Scan(&n)
			if n != 0 {
				t.Fatalf("approval replayed: %d", n)
			}
		})
	}
}
