package codexruntime

import (
	"context"
	"testing"
)

func TestResolveHoldActionTaskField(t *testing.T) {
	r, req, _ := setup(t)
	t.Setenv("FAKE_RESULT", `{"text":"Proceeding.","actions":[{"type":"resolve_hold","task":"11111111-1111-4111-8111-111111111111","project":"alpha","title":"","body":"go ahead","recipient":""}],"attachments":[]}`)
	result, err := r.Execute(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Actions) != 1 || result.Actions[0].Type != "resolve_hold" || result.Actions[0].Task != "11111111-1111-4111-8111-111111111111" {
		t.Fatalf("action %+v", result.Actions)
	}
}
