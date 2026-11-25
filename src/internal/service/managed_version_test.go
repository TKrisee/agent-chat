package service

import (
	"context"
	"net/http/httptest"
	"os"
	"runtime"
	"testing"

	"agentchat/src/internal/codexruntime"
)

func TestManagedVersionHeaderWithOverride(t *testing.T) {
	if !(runtime.GOOS == "darwin" && runtime.GOARCH == "arm64" || runtime.GOOS == "linux" && runtime.GOARCH == "amd64" || runtime.GOOS == "windows" && runtime.GOARCH == "amd64") {
		t.Skip("no managed package on this platform")
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	stateDir := t.TempDir()
	if err := os.Chmod(stateDir, 0700); err != nil {
		t.Fatal(err)
	}
	runner, err := codexruntime.New(codexruntime.Config{StateDir: stateDir, CodexPath: exe, JQPath: "jq"})
	if err != nil {
		t.Fatal(err)
	}
	s := openTest(t, t.TempDir())
	defer s.Close()
	before := httptest.NewRecorder()
	s.codexSetupStatus(before, httptest.NewRequest("GET", "/api/codex-setup", nil))
	if before.Header().Get("X-AgentChat-Codex-Managed-Version") != "" {
		t.Fatal("fixture unexpectedly advertises a managed version")
	}
	s.ConfigureRuntime(context.Background(), runner)
	w := httptest.NewRecorder()
	s.codexSetupStatus(w, httptest.NewRequest("GET", "/api/codex-setup", nil))
	if w.Header().Get("X-AgentChat-Codex-Source") != "override" || w.Header().Get("X-AgentChat-Codex-Managed-Version") == "" || w.Header().Get("X-AgentChat-Codex-Version") != "" {
		t.Fatalf("unexpected setup headers: %v", w.Header())
	}
}
