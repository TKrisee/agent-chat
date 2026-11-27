package codexruntime

import (
	"os/exec"
	"strings"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func TestLocalHoldSupervisorContract(t *testing.T) {
	req := runtimeapi.Request{Role: "root", Kind: "task_blocked", ProjectID: "alpha", TaskID: "task", LocalHoldSupervisor: true}
	cmd := exec.Command("jq", "-e", `.properties.actions.maxItems == 1 and .properties.actions.items.properties.type.enum == ["delegate"]`)
	cmd.Stdin = strings.NewReader(newJobSchema(req))
	if err := cmd.Run(); err != nil {
		t.Fatal("supervisor schema")
	}
	if !strings.Contains(prompt(req), "original local hold remains active") || !strings.Contains(prompt(req), "Do not repeat effects") {
		t.Fatal("supervisor guidance missing")
	}
	old := req
	old.LocalHoldSupervisor = false
	if fingerprint(old) == fingerprint(req) {
		t.Fatal("supervisor flag omitted from fingerprint")
	}
	cmd = exec.Command("jq", "-e", `(.properties.actions.items.properties.type.enum | index("resolve_hold")) != null`)
	cmd.Stdin = strings.NewReader(newJobSchema(old))
	if err := cmd.Run(); err != nil {
		t.Fatal("ordinary hold schema changed")
	}
}
