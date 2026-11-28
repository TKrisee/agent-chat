package codexruntime

import (
	"agentchat/src/internal/runtimeapi"
	"strings"
	"testing"
)

func TestLocalProviderBinding(t *testing.T) {
	req := runtimeapi.Request{ID: "job", Instructions: "work", AgentID: "worker", Role: "worker", ProjectID: "p", TaskID: "t", Workspace: t.TempDir(), Model: "mlx-community/Qwen", Provider: "agentchat-local", ProviderID: "local:01234567-89ab-4cde-8fab-0123456789ab", ProviderPort: 8000}
	if err := validate(req); err != nil {
		t.Fatal(err)
	}
	first := fingerprint(req)
	req.ProviderPort = 8001
	if fingerprint(req) == first {
		t.Fatal("port not pinned")
	}
	req.ProviderPort = 8000
	req.ProviderID = "local:11234567-89ab-4cde-8fab-0123456789ab"
	if fingerprint(req) == first {
		t.Fatal("identity not pinned")
	}
	req.ProviderID = "local:bad"
	if validate(req) == nil {
		t.Fatal("bad identity accepted")
	}
	req.ProviderID = "local:01234567-89ab-4cde-8fab-0123456789ab"
	req.Provider = "cloud"
	if validate(req) == nil {
		t.Fatal("unverified named provider accepted")
	}
	a, e := localProviderArgs(8000)
	if e != nil {
		t.Fatal(e)
	}
	b, e := localProviderArgs(8000)
	if e != nil {
		t.Fatal(e)
	}
	if a[1] == b[1] {
		t.Fatal("alias reused")
	}
	if !strings.Contains(a[3], "http://127.0.0.1:8000/v1") || !strings.Contains(a[3], `wire_api="responses"`) || strings.Contains(a[3], "env_key") {
		t.Fatalf("unsafe route: %v", a)
	}
}
