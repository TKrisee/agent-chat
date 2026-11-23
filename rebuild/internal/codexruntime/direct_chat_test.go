package codexruntime

import (
	"context"
	"strings"
	"testing"

	"agentchat/rebuild/internal/runtimeapi"
)

func TestTasklessWorkerConversationRequiresExplicitKind(t *testing.T) {
	for _, kind := range []string{"operator_message", "project_message"} {
		t.Run(kind, func(t *testing.T) {
			r, req, _ := setup(t)
			req.Role, req.TaskID, req.Kind = "worker", "", kind
			t.Setenv("FAKE_RESULT", `{"text":"Hello","attachments":[]}`)
			result, err := r.Execute(context.Background(), req)
			if err != nil || result.Text != "Hello" || launchCount(t) != 1 {
				t.Fatalf("worker conversation: %+v %v", result, err)
			}
			_, state, err := r.Recover(context.Background(), req)
			if err != nil || state != runtimeapi.Completed {
				t.Fatalf("exact recovery: %v %v", state, err)
			}
			req.Kind = "review"
			if _, state, err := r.Recover(context.Background(), req); err == nil || state != runtimeapi.Unknown {
				t.Fatalf("changed kind recovered: %v %v", state, err)
			}
			if launchCount(t) != 1 {
				t.Fatal("unexpected replay")
			}
		})
	}
}

func TestTasklessWorkerRejectsOtherKinds(t *testing.T) {
	for _, kind := range []string{"", "work", "review", " operator_message", " project_message"} {
		t.Run(kind, func(t *testing.T) {
			r, req, _ := setup(t)
			req.Role, req.TaskID, req.Kind = "worker", "", kind
			if _, err := r.Execute(context.Background(), req); err == nil || launchCount(t) != 0 {
				t.Fatalf("taskless worker accepted kind %q: %v", kind, err)
			}
		})
	}
}

func TestGroupContextAttributionBindsRuntimeIdentity(t *testing.T) {
	r, req, _ := setup(t)
	req.Context = []runtimeapi.Message{{ID: "message", Sender: "operator", Role: "operator", Text: "example"}}
	if _, err := r.Execute(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	withoutGroup := fingerprint(req)
	req.Context[0].GroupRecipient = "group:" + req.ProjectID
	if withoutGroup == fingerprint(req) ||
		!strings.Contains(prompt(req), `group_recipient="group:`+req.ProjectID+`"`) {
		t.Fatal("shared recipient was missing from the fingerprint or runtime prompt")
	}
	if _, state, err := r.Recover(context.Background(), req); err == nil || state != runtimeapi.Unknown || launchCount(t) != 1 {
		t.Fatalf("changed shared recipient recovered or replayed: %v %v", state, err)
	}
}

func TestLegacyFingerprintAndKindBinding(t *testing.T) {
	req := runtimeapi.Request{ID: "id", AgentID: "agent", Role: "worker", ProjectID: "p", TaskID: "t", Workspace: "/tmp/w", Instructions: "work"}
	const legacy = "da278de70fa49a5a641c703449a63f60d929372180b40b711413398a2e6294d6"
	if got := fingerprint(req); got != legacy {
		t.Fatalf("legacy fingerprint changed: %s", got)
	}
	r, saved, _ := setup(t)
	if _, err := r.Execute(context.Background(), saved); err != nil {
		t.Fatal(err)
	}
	withKind := saved
	withKind.Kind = "operator_message"
	if fingerprint(withKind) == fingerprint(saved) {
		t.Fatal("kind not bound")
	}
	if _, state, err := r.Recover(context.Background(), withKind); err == nil || state != runtimeapi.Unknown {
		t.Fatalf("kind change recovered: %v %v", state, err)
	}
	if _, state, err := r.Recover(context.Background(), saved); err != nil || state != runtimeapi.Completed {
		t.Fatalf("legacy recovery: %v %v", state, err)
	}
}
