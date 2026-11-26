package codexruntime

import (
	"os/exec"
	"strings"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func TestNewJobActionSchema(t *testing.T) {
	for _, tc := range []struct{ role, kind, filter string }{
		{"po", "assignment", `(.properties.actions.items.properties.type.enum | index("resolve_hold")) == null and (.properties.actions.items.properties.type.enum | index("queue_work_item")) != null and (.properties.actions.items.properties.type.enum | index("create_one_shot_worker")) != null`},
		{"po", "po_continue", `(.properties.actions.items.properties.type.enum | index("resolve_hold")) == null and (.properties.actions.items.properties.type.enum | index("report_progress")) != null`},
		{"po", "scope_review", `.properties.actions.items.properties.type.enum == ["post_group"]`},
		{"po", "operator_message", `.properties.actions.items.properties.type.enum == ["request_approval"]`},
		{"po", "project_message", `.properties.actions.maxItems == 0`},
		{"po", "approval_denied", `.properties.actions.maxItems == 0`},
		{"root", "approval_denied", `.properties.actions.maxItems == 0`},
		{"worker", "project_message", `.properties.actions.maxItems == 0`},
		{"root", "task_blocked", `.properties.actions.items.properties.type.enum == ["request_approval","resolve_hold"]`},
		{"root", "operator_message", `(.properties.actions.items.properties.type.enum | index("resolve_hold")) != null and (.properties.actions.items.properties.type.enum | index("delegate")) != null`},
	} {
		t.Run(tc.role+"/"+tc.kind, func(t *testing.T) {
			schema := newJobSchema(runtimeapi.Request{Role: tc.role, Kind: tc.kind})
			cmd := exec.Command("jq", "-e", `(`+tc.filter+`) and ((.properties.actions.items.required|sort)==(.properties.actions.items.properties|keys|sort))`)
			cmd.Stdin = strings.NewReader(schema)
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("schema check: %v %s", err, out)
			}
		})
	}
}

func TestReportRecoverySchemaAndFingerprint(t *testing.T) {
	request := runtimeapi.Request{Role: "po", Kind: "po_followup"}
	ordinary := newJobSchema(request)
	oldFingerprint := fingerprint(request)
	request.ReportOnly = true
	if fingerprint(request) == oldFingerprint {
		t.Fatal("report-only intent must bind the receipt")
	}
	for _, tc := range []struct{ schema, filter string }{
		{newJobSchema(request), `.properties.actions.items.properties.type.enum == ["report"]`},
		{ordinary, `(.properties.actions.items.properties.type.enum | index("queue_work_item")) != null`},
	} {
		cmd := exec.Command("jq", "-e", tc.filter)
		cmd.Stdin = strings.NewReader(tc.schema)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("schema: %v %s", err, out)
		}
	}
}
