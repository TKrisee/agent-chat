package codexruntime

import (
	"os/exec"
	"strings"
	"testing"
)

func TestLocalModelsFilter(t *testing.T) {
	for _, tc := range []struct {
		input, want string
		valid       bool
	}{
		{`{"data":[{"id":"mlx-community/Qwen3.5"}]}`, "mlx-community/Qwen3.5", true},
		{`{"data":[]}`, "", true},
		{`{"data":[{"id":"../../secret"}]}`, "", false},
		{`{"data":[{"id":"bad\nname"}]}`, "", false},
		{`{"data":{}}`, "", false},
	} {
		cmd := exec.Command("jq", "-r", localModelsFilter)
		cmd.Stdin = strings.NewReader(tc.input)
		out, err := cmd.Output()
		if (err == nil) != tc.valid || tc.valid && normalizeModelListLines(out) != tc.want {
			t.Errorf("input %s: got %q err %v", tc.input, out, err)
		}
	}
}
