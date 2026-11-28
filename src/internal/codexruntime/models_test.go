package codexruntime

import (
	"os/exec"
	"strings"
	"testing"
)

func TestModelCatalogFilter(t *testing.T) {
	for _, tc := range []struct{ input, want string }{
		{`{"id":1,"result":{}}`, "ready"},
		{`{"id":2,"result":{"data":[{"model":"gpt-6.1-terra","hidden":false,"defaultReasoningEffort":"low","supportedReasoningEfforts":[{"reasoningEffort":"low"},{"reasoningEffort":"ultra"}]},{"model":"hidden-model","hidden":true}],"nextCursor":"next"}}`, "model\tgpt-6.1-terra\tlow\tlow,ultra\ncursor\tbmV4dA=="},
		{`{"id":2,"result":{"data":[],"nextCursor":null}}`, "done"},
		{`{"id":2,"result":{"data":[{"model":"bad slug"}],"nextCursor":null}}`, "error"},
		{`{"id":2,"error":{"message":"denied"}}`, "error"},
	} {
		cmd := exec.Command("jq", "-r", modelListFilter)
		cmd.Stdin = strings.NewReader(tc.input)
		out, err := cmd.Output()
		if err != nil || normalizeModelListLines(out) != tc.want {
			t.Errorf("%s: got %q err %v want %q", tc.input, out, err, tc.want)
		}
	}
}

func TestNormalizeModelListLinesWindows(t *testing.T) {
	if got := normalizeModelListLines([]byte("model\tgpt-6.1-terra\r\ncursor\tbmV4dA==\r\n")); got != "model\tgpt-6.1-terra\ncursor\tbmV4dA==" {
		t.Fatalf("unexpected normalized lines %q", got)
	}
}
