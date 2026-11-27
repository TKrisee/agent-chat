package codexruntime

import (
	"agentchat/src/internal/runtimeapi"
	"os/exec"
	"strings"
	"testing"
)

func TestModelPreferenceActionSchemaAndLegacyDecode(t *testing.T) {
	schema := newJobSchema(runtimeapi.Request{Role: "po", Kind: "assignment", ModelSelection: true, Instructions: "Preferred models for new workers: reasoning: gpt-terra"})
	cmd := exec.Command("jq", "-e", `.properties.actions.items | (.required | index("model") != null and index("category") != null and index("model_reason") != null) and .properties.model.type == "string" and .properties.category.enum == ["","reasoning","implementation","precise_edits","research","routine_checks"]`)
	cmd.Stdin = strings.NewReader(schema)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("schema: %s %v", out, err)
	}
	for _, raw := range []string{`{"text":"","actions":[{"type":"create_worker","project":"","title":"A","body":"","recipient":"","task":"","reply_to":"","depends_on":[],"model":"gpt-astra","category":"","model_reason":""}],"attachments":[]}`, `{"text":"","actions":[{"type":"create_worker","project":"","title":"A","body":"","recipient":""}],"attachments":[]}`} {
		cmd = exec.Command("jq", "-r", "-e", "--arg", "worker", "no", extractFilter)
		cmd.Stdin = strings.NewReader(raw)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("extract %s: %s %v", raw, out, err)
		}
	}
}
