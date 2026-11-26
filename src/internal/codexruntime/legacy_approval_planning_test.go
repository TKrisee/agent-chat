package codexruntime

import (
	"context"
	"strings"
	"testing"
)

func TestLegacyApprovalPlanningProof(t *testing.T) {
	r, _, _ := setup(t)
	prior := `{"text":"","actions":[{"type":"accept_result","project":"alpha","title":"Child","body":"","recipient":""},{"type":"report_progress","project":"alpha","title":"","body":"Status","recipient":"root"}],"attachments":[]}`
	approved := `{"text":"","actions":[{"type":"queue_work_item","project":"alpha","title":"Next","body":"Inspect","recipient":"worker"},{"type":"report_progress","project":"alpha","title":"","body":"Status","recipient":""}],"attachments":[]}`
	for _, tc := range []struct {
		name, prior, approved, project string
		want                           bool
	}{
		{"exact", prior, approved, "alpha", true},
		{"wrong project", prior, approved, "other", false},
		{"accept recipient", strings.Replace(prior, `"recipient":""`, `"recipient":"worker"`, 1), approved, "alpha", false},
		{"unsafe action", prior, strings.Replace(approved, `"queue_work_item"`, `"stop_project"`, 1), "alpha", false},
		{"missing title", strings.Replace(prior, `"Child"`, `""`, 1), approved, "alpha", false},
		{"attachments", prior, strings.Replace(approved, `"attachments":[]`, `"attachments":[{}]`, 1), "alpha", false},
		{"malformed", `{`, approved, "alpha", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := r.IsLegacyApprovalPlanning(context.Background(), []byte(tc.prior), []byte(tc.approved), tc.project); got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
}
