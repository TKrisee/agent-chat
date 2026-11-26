package service

import (
	"fmt"
	"path/filepath"
	"testing"
)

func TestLegacyReceiptAttachmentRecovery(t *testing.T) {
	for _, tc := range []struct {
		name  string
		busy  bool
		wrong bool
		want  int
	}{{"eligible", false, false, 202}, {"busy parent", true, false, 409}, {"wrong attachment", false, true, 409}, {"active child hold", false, false, 409}} {
		t.Run(tc.name, func(t *testing.T) {
			s, j, parent, worker, _ := childReviewFixture(t)
			defer s.Close()
			installReviewInspector(t, s)
			id := newID()
			name := "receipt"
			media := "text/markdown"
			if _, err := s.db.Exec(`INSERT INTO attachments(id,message_id,name,media_type,content) VALUES(?,?,?,?,?)`, id, j.Source, name, media, []byte("receipt")); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(s.dir, "attachments", id)
			if tc.wrong {
				path = filepath.Join(s.dir, "attachments", newID())
			}
			payload := fmt.Sprintf(`{"text":"review","actions":[{"type":"accept_result","project":"alpha","title":"item","body":"done","recipient":%q},{"type":"report_progress","project":"alpha","title":"","body":"update","recipient":"root"}],"attachments":[{"path":%q,"name":"receipt","media_type":"text/markdown"}]}`, worker, path)
			if _, err := s.db.Exec(`UPDATE jobs SET status='completed',diagnostic=?,result=? WHERE id=?`, fmt.Sprintf("invalid runtime result: attachment %q: path is outside the workspace", name), []byte(payload), j.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := s.db.Exec(`UPDATE tasks SET status='blocked' WHERE id=?`, j.Task); err != nil {
				t.Fatal(err)
			}
			if tc.busy {
				if _, err := s.db.Exec(`UPDATE jobs SET status='submitted' WHERE task_id=? AND kind='assignment'`, parent); err != nil {
					t.Fatal(err)
				}
			}
			if tc.name == "active child hold" {
				if _, err := s.db.Exec(`INSERT INTO task_holds(job_id,task_id,project_id,po_id,phase,assignment_id,root_source_id,status) SELECT id,task_id,project_id,agent_id,'review',source_id,source_id,'active' FROM jobs WHERE id=?`, j.ID); err != nil {
					t.Fatal(err)
				}
			}
			if got := postContinuationID(s, j.Task, "Review the existing receipt.", newID()); got != tc.want {
				t.Fatalf("status=%d want=%d", got, tc.want)
			}
			if tc.want == 202 {
				follow, err := s.claim(nil)
				if err != nil || follow == nil || !follow.Request.ChildReceiptOnly {
					t.Fatalf("receipt-only claim: %+v %v", follow, err)
				}
			}
		})
	}
}
