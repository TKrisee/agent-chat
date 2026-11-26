package service

import (
	"strings"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func TestExhaustionSharesOnlyExplicitPOProgress(t *testing.T) {
	for _, mode := range []string{"marked", "legacy", "legacy without inspector", "none"} {
		t.Run(mode, func(t *testing.T) {
			shared := mode != "none"
			wantShared := mode == "marked" || mode == "legacy"
			s, task, _, _ := continuationFixture(t)
			defer s.Close()
			if mode == "legacy" {
				installReviewInspector(t, s)
			}
			if code := postContinuation(s, task, "operator-private sentinel"); code != 202 {
				t.Fatalf("continue %d", code)
			}
			j, err := s.claim(nil)
			if err != nil || j == nil || j.Kind != "po_continue" {
				t.Fatalf("claim %+v %v", j, err)
			}
			for i := 0; i < 3; i++ {
				result := runtimeapi.Result{Text: "unaddressed PO sentinel"}
				if shared && i == 0 {
					result.Actions = []runtimeapi.Action{{Type: "report_progress", Project: "alpha", Recipient: root, Body: "Explicit task progress: obtain the source record."}}
				}
				if err := s.complete(*j, result, nil, nil, nil); err != nil {
					t.Fatal(err)
				}
				if i == 0 && strings.HasPrefix(mode, "legacy") {
					if _, err := s.db.Exec("DELETE FROM po_explicit_progress"); err != nil {
						t.Fatal(err)
					}
				}
				if i < 2 {
					next := followupClaim(t, s)
					j = &next
				}
			}
			rootJob, err := s.claim(nil)
			if !wantShared {
				if err != nil || rootJob != nil {
					t.Fatalf("unexpected vague escalation %+v %v", rootJob, err)
				}
				return
			}
			if err != nil || rootJob == nil || rootJob.Kind != "task_blocked" {
				t.Fatalf("escalation %+v %v", rootJob, err)
			}
			all := rootJob.Request.Instructions
			for _, m := range rootJob.Request.Context {
				all += "\n" + m.Text
			}
			if strings.Contains(all, "operator-private sentinel") || strings.Contains(all, "unaddressed PO sentinel") {
				t.Fatal("private source was copied to Root")
			}
			if strings.Contains(all, "operator has the exact details") || strings.Contains(all, "operator has the private details") {
				t.Fatal("false private-details claim")
			}
			if strings.Contains(all, "Explicit task progress: obtain the source record.") != wantShared {
				t.Fatalf("shared progress missing or invented: %q", all)
			}
			if !shared && strings.Contains(all, "Ask only whether an operator decision is needed") {
				t.Fatal("vague decision prompt")
			}
		})
	}
}
