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

func TestChildReceiptRecoverySchema(t *testing.T) {
	req := runtimeapi.Request{Role: "po", Kind: "po_followup"}
	ordinary := fingerprint(req)
	req.ChildReceiptOnly = true
	if fingerprint(req) == ordinary {
		t.Fatal("receipt intent not bound")
	}
	cmd := exec.Command("jq", "-e", `.properties.actions.items.properties.type.enum == ["accept_result","report","report_progress"]`)
	cmd.Stdin = strings.NewReader(newJobSchema(req))
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("schema: %v %s", err, out)
	}
}

func TestParentPlanningSchema(t *testing.T) {
	req := runtimeapi.Request{Role: "po", Kind: "po_followup", TaskID: "parent"}
	original := fingerprint(req)
	req.ParentPlanning = true
	if fingerprint(req) == original {
		t.Fatal("parent planning intent not bound")
	}
	cmd := exec.Command("jq", "-e", `(.properties.actions.items.properties.type.enum|index("accept_result"))==null and (.properties.actions.items.properties.type.enum|index("report"))!=null and (.properties.actions.items.properties.type.enum|index("queue_work_item"))!=null`)
	cmd.Stdin = strings.NewReader(newJobSchema(req))
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("schema: %v %s", err, output)
	}
	if !strings.Contains(prompt(req), "do not accept them again") {
		t.Fatal("missing parent guidance")
	}
	req.ParentPlanning = false
	req.ChildReceiptOnly = true
	cmd = exec.Command("jq", "-e", `(.properties.actions.items.properties.type.enum|index("accept_result"))!=null`)
	cmd.Stdin = strings.NewReader(newJobSchema(req))
	if err := cmd.Run(); err != nil {
		t.Fatal("child review lost accept_result")
	}
}
