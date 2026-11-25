package codexruntime

import (
	"strings"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func TestTaskWorkspacePolicyAppliesToPOAndWorker(t *testing.T) {
	for _, role := range []string{"po", "worker"} {
		t.Run(role, func(t *testing.T) {
			text := prompt(runtimeapi.Request{Role: role, Kind: "work", ProjectID: "project", TaskID: "task"})
			for _, required := range []string{"not an instruction to copy the project", "same owner, repository and work lane", "keep concurrent work isolated", "Preserve unmerged, dirty, uncertain and evidence-bearing work", "Clean only scratch explicitly designated disposable"} {
				if !strings.Contains(text, required) {
					t.Errorf("missing workspace policy %q", required)
				}
			}
		})
	}
}
