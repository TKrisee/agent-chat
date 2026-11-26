package codexruntime

import (
	"context"
	"strings"
	"testing"
)

func TestLegacyReceiptCorrectionProof(t *testing.T) {
	r, _, _ := setup(t)
	first := `{"text":"","actions":[{"type":"accept_result","project":"alpha","title":"Child","body":"","recipient":"worker"},{"type":"report_progress","project":"alpha","title":"","body":"Status","recipient":"root"},{"type":"queue_work_item","project":"alpha","title":"Next","body":"Inspect","recipient":"worker"}],"attachments":[]}`
	second := `{"text":"","actions":[{"type":"accept_result","project":"alpha","title":"Child","body":"","recipient":"worker"},{"type":"report","project":"alpha","title":"","body":"Done","recipient":"worker"},{"type":"queue_work_item","project":"alpha","title":"Next","body":"Inspect","recipient":"other"}],"attachments":[]}`
	for _, tc := range []struct {
		name, payload, project, worker, diagnostic string
		want                                       bool
	}{
		{"missing report", first, "alpha", "worker", "invalid runtime result: review must be accepted and reported before planning", true},
		{"parent unavailable", second, "alpha", "worker", "invalid runtime result: parent task unavailable", true},
		{"wrong diagnosis", first, "alpha", "worker", "invalid runtime result: parent task unavailable", false},
		{"wrong worker", first, "alpha", "other", "invalid runtime result: review must be accepted and reported before planning", false},
		{"wrong project", first, "other", "worker", "invalid runtime result: review must be accepted and reported before planning", false},
		{"unsafe", strings.Replace(second, `"queue_work_item"`, `"stop_project"`, 1), "alpha", "worker", "invalid runtime result: parent task unavailable", false},
		{"attachments", strings.Replace(second, `"attachments":[]`, `"attachments":[{}]`, 1), "alpha", "worker", "invalid runtime result: parent task unavailable", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := r.IsLegacyReceiptCorrection(context.Background(), []byte(tc.payload), tc.project, tc.worker, tc.diagnostic, ""); got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
	const path = "/managed/attachments/approved-id"
	withAttachment := strings.Replace(second, `"attachments":[]`, `"attachments":[{"path":"/managed/attachments/approved-id","name":"alias","media_type":"text/markdown"}]`, 1)
	if !r.IsLegacyReceiptCorrection(context.Background(), []byte(withAttachment), "alpha", "worker", "invalid runtime result: parent task unavailable", path) {
		t.Fatal("exact pinned attachment rejected")
	}
	if r.IsLegacyReceiptCorrection(context.Background(), []byte(withAttachment), "alpha", "worker", "invalid runtime result: parent task unavailable", "/managed/attachments/other") {
		t.Fatal("unrelated attachment accepted")
	}
	if r.IsLegacyReceiptCorrection(context.Background(), []byte(withAttachment), "alpha", "worker", "invalid runtime result: review must be accepted and reported before planning", path) {
		t.Fatal("attachment accepted for other diagnostic")
	}
}
