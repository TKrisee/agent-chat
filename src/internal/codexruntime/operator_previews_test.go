package codexruntime

import (
	"context"
	"os/exec"
	"strings"
	"testing"
)

func TestOperatorPreviewsNativeContract(t *testing.T) {
	r, req, _ := setup(t)
	req.Role = "worker"
	req.Kind = "work"
	old := fingerprint(req)
	cmd := exec.Command(r.cfg.JQPath, "-e", `(.properties.actions.items.properties.type.enum|index("send_operator_previews"))==null`)
	cmd.Stdin = strings.NewReader(newJobSchema(req))
	if err := cmd.Run(); err != nil {
		t.Fatal("preview action offered without consent")
	}
	req.OperatorPreviews = true
	if fingerprint(req) == old {
		t.Fatal("preview consent not bound to journal")
	}
	cmd = exec.Command(r.cfg.JQPath, "-e", `(.properties.actions.items.properties.type.enum|index("send_operator_previews"))!=null`)
	cmd.Stdin = strings.NewReader(newJobSchema(req))
	if err := cmd.Run(); err != nil {
		t.Fatal("preview action missing")
	}
	t.Setenv("FAKE_RESULT", `{"text":"done","actions":[{"type":"send_operator_previews","project":"","title":"","body":"","recipient":""}],"attachments":[]}`)
	result, err := r.Execute(context.Background(), req)
	if err != nil || len(result.Actions) != 1 || result.Actions[0].Type != "send_operator_previews" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if !strings.Contains(prompt(req), "PO still receives") {
		t.Fatal("missing delivery guidance")
	}
}
