package codexruntime

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"agentchat/rebuild/internal/runtimeapi"
)

// Opt-in only: invokes a new ephemeral configured Codex CLI, never the bridge.
func TestGenuineProviderSmoke(t *testing.T) {
	if os.Getenv("AGENT_CHAT_RUNTIME_SMOKE") != "1" {
		t.Skip("explicit isolated provider smoke not requested")
	}
	base := t.TempDir()
	workspace := filepath.Join(base, "empty-workspace")
	if err := os.Mkdir(workspace, 0700); err != nil {
		t.Fatal(err)
	}
	runner, err := New(Config{StateDir: filepath.Join(base, "journal"), MaxDuration: 75 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	req := runtimeapi.Request{ID: "disposable-smoke", AgentID: "disposable-worker", Role: "worker",
		ProjectID: "disposable-project", TaskID: "disposable-task", Workspace: workspace,
		Model: "gpt-6.1-terra", Effort: "medium",
		Instructions: "This is an isolated smoke test. Do not use any tools, shell, network, or files. Return exactly SMOKE_OK as text with empty actions and attachments."}
	result, err := runner.Execute(ctx, req)
	if err != nil {
		t.Fatalf("isolated native request did not complete: %v", err)
	}
	if result.Text != "SMOKE_OK" || len(result.Actions) != 0 || len(result.Attachments) != 0 {
		t.Fatal("unexpected result; response contents withheld")
	}
	t.Log("requested model gpt-6.1-terra; result SMOKE_OK; actual provider/model metadata is not exposed by this CLI adapter")
}
