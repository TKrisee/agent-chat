package service

import (
	"strings"
	"testing"
)

func TestWorkflowDefaultsReachEveryAgentDelivery(t *testing.T) {
	cases := []struct {
		role, kind, roleGuide string
		shared                bool
	}{
		{"root", "operator_message", rootGuide, false},
		{"po", "scope_review", poGuide, true},
		{"po", "review", poGuide, true},
		{"worker", "assignment", workerGuide, true},
		{"worker", "operator_message", workerChatGuide, true},
		{"root", groupMessageKind, groupChatGuide, false},
		{"po", groupMessageKind, groupChatGuide, true},
		{"worker", groupMessageKind, groupChatGuide, true},
	}
	for _, tc := range cases {
		guide := agentGuide(tc.role, tc.kind, tc.shared)
		if !strings.HasPrefix(guide, tc.roleGuide) || !strings.Contains(guide, workflowDefaults) {
			t.Errorf("role %s kind %s lost role or workflow guidance", tc.role, tc.kind)
		}
	}
}
