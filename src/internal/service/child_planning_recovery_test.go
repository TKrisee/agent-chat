package service

import (
	"fmt"
	"path/filepath"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func TestChildPlanningConflictQueuesReceiptOnlyCorrection(t *testing.T) {
	s, j, _, worker, other := childReviewFixture(t)
	defer s.Close()
	result := runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "accept_result", Recipient: worker}, {Type: "queue_work_item", Title: "Next", Body: "Work", Recipient: other}}}
	if err := s.complete(j, result, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	var status string
	if err := s.db.QueryRow(`SELECT status FROM tasks WHERE id=?`, j.Task).Scan(&status); err != nil || status != "review" {
		t.Fatalf("child status=%s error=%v", status, err)
	}
	follow, err := s.claim(nil)
	if err != nil || follow == nil || !follow.Request.ChildReceiptOnly {
		t.Fatalf("correction=%+v error=%v", follow, err)
	}
}

func TestChildPlanningRecoveryAcceptsOnlyPinnedReceiptAttachment(t *testing.T) {
	for _, matching := range []bool{true, false} {
		s, j, _, worker, other := childReviewFixture(t)
		installReviewInspector(t, s)
		id := newID()
		if _, err := s.db.Exec(`INSERT INTO attachments(id,message_id,name,media_type,content) VALUES(?,?,?,'text/plain',?)`, id, j.Source, "receipt", []byte("evidence")); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(s.dir, "attachments", id)
		if !matching {
			path = filepath.Join(s.dir, "attachments", newID())
		}
		payload := fmt.Sprintf(`{"text":"","actions":[{"type":"accept_result","project":"alpha","title":"review","body":"accepted","recipient":%q},{"type":"report","project":"alpha","title":"review","body":"Reviewed","recipient":%q},{"type":"queue_work_item","project":"alpha","title":"next","body":"work","recipient":%q}],"attachments":[{"path":%q,"name":"alias","media_type":"text/plain"}]}`, worker, worker, other, path)
		if _, err := s.db.Exec(`UPDATE jobs SET status='completed',diagnostic=?,result=? WHERE id=?`, legacyChildParentUnavailableDiagnostic, []byte(payload), j.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.Exec(`UPDATE tasks SET status='blocked' WHERE id=?`, j.Task); err != nil {
			t.Fatal(err)
		}
		want := 409
		if matching {
			want = 202
		}
		if code := postContinuationID(s, j.Task, "Review the existing receipt.", newID()); code != want {
			t.Fatalf("matching=%v code=%d", matching, code)
		}
		_ = s.Close()
	}
}

func TestChildPlanningHistoricalReceiptRecovery(t *testing.T) {
	for _, tc := range []struct {
		diagnostic, middle string
		attachment         bool
		want               int
	}{
		{legacyChildPlanningOrderDiagnostic, `{"type":"report_progress","project":"alpha","title":"","body":"Progress","recipient":"root"}`, false, 202},
		{legacyChildParentUnavailableDiagnostic, `{"type":"report","project":"alpha","title":"review","body":"Reviewed","recipient":%q}`, false, 202},
		{legacyChildParentUnavailableDiagnostic, `{"type":"report","project":"alpha","title":"review","body":"Reviewed","recipient":%q}`, true, 409},
	} {
		s, j, _, worker, other := childReviewFixture(t)
		installReviewInspector(t, s)
		middle := tc.middle
		if tc.diagnostic == legacyChildParentUnavailableDiagnostic {
			middle = fmt.Sprintf(middle, worker)
		}
		attachments := "[]"
		if tc.attachment {
			attachments = `[{"path":"x","name":"x","media_type":"text/plain"}]`
		}
		payload := fmt.Sprintf(`{"text":"","actions":[{"type":"accept_result","project":"alpha","title":"review","body":"accepted","recipient":%q},%s,{"type":"queue_work_item","project":"alpha","title":"next","body":"work","recipient":%q}],"attachments":%s}`, worker, middle, other, attachments)
		if _, err := s.db.Exec(`UPDATE jobs SET status='completed',diagnostic=?,result=? WHERE id=?`, tc.diagnostic, []byte(payload), j.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.Exec(`UPDATE tasks SET status='blocked' WHERE id=?`, j.Task); err != nil {
			t.Fatal(err)
		}
		id := newID()
		if code := postContinuationID(s, j.Task, "Review the exact receipt.", id); code != tc.want {
			t.Fatalf("%s attachment=%v: %d", tc.diagnostic, tc.attachment, code)
		}
		if tc.want == 202 {
			follow, err := s.claim(nil)
			if err != nil || follow == nil || !follow.Request.ChildReceiptOnly {
				t.Fatalf("receipt-only followup: %+v %v", follow, err)
			}
		}
		_ = s.Close()
	}
}
