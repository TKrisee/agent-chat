package codexruntime

import (
	"context"
	"os/exec"
	"strings"
	"testing"
)

func TestWorkerRelaySchema(t *testing.T) {
	r, req, _ := setup(t)
	req.Role, req.Kind = "worker", "project_message"
	cmd := exec.Command("jq", "-e", `.properties.actions.maxItems == 0`)
	cmd.Stdin = strings.NewReader(newJobSchema(req))
	if err := cmd.Run(); err != nil {
		t.Fatal("group turn must have no actions")
	}
	req.Kind = "operator_message"
	cmd = exec.Command("jq", "-e", `.properties.actions.maxItems == 1 and .properties.actions.items.properties.type.enum == ["share_with_po"]`)
	cmd.Stdin = strings.NewReader(newJobSchema(req))
	if err := cmd.Run(); err != nil {
		t.Fatal("worker DM schema")
	}
	if strings.Contains(prompt(req), "do not propose actions") || !strings.Contains(prompt(req), "Never copy private DM text") || !strings.Contains(prompt(req), "current operator message clearly asks") {
		t.Fatal("relay guidance invalid")
	}
	t.Setenv("FAKE_RESULT", `{"text":"done","actions":[{"type":"share_with_po","project":"","title":"","body":"Sanitized observed status.","recipient":"","reply_to":"","depends_on":[]}],"attachments":[]}`)
	result, err := r.Execute(context.Background(), req)
	if err != nil || len(result.Actions) != 1 || result.Actions[0].Type != "share_with_po" {
		t.Fatalf("relay decode: %+v %v", result.Actions, err)
	}
}
