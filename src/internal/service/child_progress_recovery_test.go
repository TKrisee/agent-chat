package service

import (
	"fmt"
	"strings"
	"testing"
)

func TestLegacyChildProgressRecovery(t *testing.T) {
	for _, tc := range []struct {
		name, fence string
		want        int
	}{{"eligible", "", 202}, {"parent uncertain", "uncertain", 409}, {"active parent hold", "hold", 409}, {"stop", "stop", 409}, {"wrong worker", "worker", 409}, {"unsafe action", "unsafe", 409}} {
		t.Run(tc.name, func(t *testing.T) {
			s, j, parent, worker, other := childReviewFixture(t)
			defer s.Close()
			installReviewInspector(t, s)
			recipient := worker
			if tc.fence == "worker" {
				recipient = other
			}
			payload := fmt.Sprintf(`{"text":"","actions":[{"type":"accept_result","project":"alpha","title":"","body":"","recipient":%q},{"type":"report","project":"alpha","title":"","body":"Reviewed","recipient":""},{"type":"queue_work_item","project":"alpha","title":"Sibling","body":"Inspect","recipient":%q},{"type":"report_progress","project":"alpha","title":"","body":"Review complete","recipient":"root"}],"attachments":[]}`, recipient, other)
			if tc.fence == "unsafe" {
				payload = strings.Replace(payload, `"queue_work_item"`, `"stop_project"`, 1)
			}
			if _, err := s.db.Exec(`UPDATE jobs SET status='completed',diagnostic=?,result=? WHERE id=?`, legacyChildProgressDiagnostic, []byte(payload), j.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := s.db.Exec(`UPDATE tasks SET status='blocked' WHERE id=?`, j.Task); err != nil {
				t.Fatal(err)
			}
			if tc.fence == "stop" {
				_, _ = s.db.Exec(`UPDATE projects SET stopped=1 WHERE id='alpha'`)
			}
			if tc.fence == "uncertain" {
				_, _ = s.db.Exec(`UPDATE jobs SET status='uncertain' WHERE task_id=? AND kind='assignment'`, parent)
			}
			if tc.fence == "hold" {
				_, err := s.db.Exec(`UPDATE tasks SET status='blocked' WHERE id=?`, parent)
				if err != nil {
					t.Fatal(err)
				}
				_, err = s.db.Exec(`INSERT INTO approvals VALUES(?,'alpha',?,?,'decision','body','pending')`, newID(), parent, j.Agent)
				if err != nil {
					t.Fatal(err)
				}
				_, err = s.db.Exec(`INSERT INTO task_holds(job_id,task_id,project_id,po_id,phase,assignment_id,root_source_id,status) SELECT id,task_id,project_id,agent_id,'assigned',source_id,source_id,'active' FROM jobs WHERE task_id=? AND kind='assignment'`, parent)
				if err != nil {
					t.Fatal(err)
				}
			}
			id := newID()
			if got := postContinuationID(s, j.Task, "Review the existing receipt.", id); got != tc.want {
				t.Fatalf("status=%d", got)
			}
			if tc.want != 202 {
				return
			}
			if got := postContinuationID(s, j.Task, "Review the existing receipt.", id); got != 202 {
				t.Fatalf("retry=%d", got)
			}
			follow, err := s.claim(nil)
			if err != nil || follow == nil {
				t.Fatalf("claim %v %v", follow, err)
			}
			if !strings.Contains(strings.ToLower(follow.Request.Instructions), "do not replay the saved batch") {
				t.Fatal("missing no-replay guidance")
			}
			found := false
			for _, m := range follow.Request.Context {
				if m.ID == j.Source {
					found = true
				}
			}
			if !found {
				t.Fatal("missing worker receipt")
			}
			var count int
			_ = s.db.QueryRow(`SELECT count(*) FROM jobs WHERE task_id=? AND kind='work'`, j.Task).Scan(&count)
			if count != 1 {
				t.Fatalf("worker replay count=%d", count)
			}
		})
	}
}
