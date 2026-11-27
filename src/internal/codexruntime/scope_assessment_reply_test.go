package codexruntime

import (
	"context"
	"fmt"
	"testing"
)

func TestDelegateReplyPreserved(t *testing.T) {
	for _, tc := range []struct {
		name, role, kind, action, reply string
		ok                              bool
	}{
		{"empty", "root", "scope_decision", "delegate", "", true},
		{"explicit", "root", "scope_decision", "delegate", "assessment", true},
		{"not delegate", "root", "scope_decision", "request_approval", "assessment", false},
		{"not root", "po", "assignment", "report", "assessment", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, req, _ := setup(t)
			req.Role, req.Kind, req.ProjectID, req.TaskID = tc.role, tc.kind, "alpha", "task"
			t.Setenv("FAKE_RESULT", fmt.Sprintf(`{"text":"","actions":[{"type":%q,"project":"alpha","title":"scope","body":"brief","recipient":"po","reply_to":%q}],"attachments":[]}`, tc.action, tc.reply))
			got, err := r.Execute(context.Background(), req)
			if (err == nil) != tc.ok {
				t.Fatalf("err=%v", err)
			}
			if tc.ok && (len(got.Actions) != 1 || got.Actions[0].ReplyTo != tc.reply) {
				t.Fatal("delegate reply changed")
			}
		})
	}
}
