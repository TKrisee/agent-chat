package service

import (
	"strings"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func TestPrivatePOContinuationRequiresExplicitSharing(t *testing.T) {
	for _, kind := range []string{"text", "progress", "hold"} {
		t.Run(kind, func(t *testing.T) {
			s, task, _, _ := continuationFixture(t)
			defer s.Close()
			const sentinel = "PRIVATE-CONTINUATION-SENTINEL"
			if code := postContinuation(s, task, sentinel); code != 202 {
				t.Fatalf("continuation=%d", code)
			}
			j, err := s.claim(nil)
			if err != nil || j == nil || j.Kind != "po_continue" {
				t.Fatalf("claim %+v: %v", j, err)
			}
			result := runtimeapi.Result{Text: sentinel}
			switch kind {
			case "progress":
				result.Actions = []runtimeapi.Action{{Type: "report_progress", Project: "alpha", Recipient: root, Body: sentinel}}
			case "hold":
				result.Actions = []runtimeapi.Action{{Type: "hold_task", Project: "alpha", Recipient: root, Body: sentinel}}
			}
			if err := s.complete(*j, result, nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			var leaks int
			if err := s.db.QueryRow("SELECT count(*) FROM messages WHERE recipient='root' AND instr(text,?)>0", sentinel).Scan(&leaks); err != nil {
				t.Fatal(err)
			}
			if kind == "text" && leaks != 0 || kind != "text" && leaks != 1 {
				t.Fatalf("Root shared count=%d for %s", leaks, kind)
			}
		})
	}
}

func TestRootSharedHoldReasonRemainsActionable(t *testing.T) {
	s, task, po, _ := continuationFixture(t)
	defer s.Close()
	j := activeAssignment(t, s, task, po)
	const question = "Can Root choose the bounded diagnostic pass?"
	if err := s.complete(j, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "hold_task", Project: "alpha", Recipient: root, Body: question}}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	r, err := s.claim(nil)
	if err != nil || r == nil || r.Kind != "task_blocked" {
		t.Fatalf("Root job %+v: %v", r, err)
	}
	found := false
	for _, m := range r.Request.Context {
		if strings.Contains(m.Text, question) {
			found = true
		}
	}
	if !found {
		t.Fatal("Root-shared question unavailable")
	}
}
