package service

import (
	"strings"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func TestRootOwnedHoldDecisionIsScopedAndBounded(t *testing.T) {
	for _, tc := range []struct {
		name, fence string
		valid       bool
	}{{"root owned", "", true}, {"operator owned", "default", false}, {"stopped", "stop", false}, {"wrong owner", "owner", false}, {"wrong source", "source", false}, {"already decided", "repeat", false}} {
		t.Run(tc.name, func(t *testing.T) {
			s, task, po, worker := continuationFixture(t)
			defer s.Close()
			j := activeAssignment(t, s, task, po)
			recipient := root
			if tc.fence == "default" {
				recipient = ""
			}
			if err := s.complete(j, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "hold_task", Project: "alpha", Title: "Decision needed", Recipient: recipient, Body: "Choose a bounded diagnostic pass."}}}, nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			rootJob, err := s.claim(nil)
			if err != nil || rootJob == nil || rootJob.Kind != "task_blocked" {
				t.Fatalf("root job %+v: %v", rootJob, err)
			}
			switch tc.fence {
			case "stop":
				_, err = s.db.Exec("UPDATE projects SET stopped=1 WHERE id='alpha'")
			case "owner":
				_, err = s.db.Exec("UPDATE tasks SET owner_id=? WHERE id=?", worker, task)
			case "source":
				rootJob.Source = newID()
			case "repeat":
				_, err = s.db.Exec("INSERT INTO root_hold_decisions(task_id,hold_job_id,note_id) VALUES(?,?,?)", task, j.ID, j.Source)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := s.complete(*rootJob, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "resolve_hold", Project: "alpha", Task: task, Body: "Run the bounded diagnostics; preserve the stop."}}}, nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			var decisions, followups, operatorMessages int
			if err := s.db.QueryRow("SELECT count(*) FROM root_hold_decisions WHERE task_id=?", task).Scan(&decisions); err != nil {
				t.Fatal(err)
			}
			if err := s.db.QueryRow("SELECT count(*) FROM jobs WHERE task_id=? AND kind='po_followup' AND status='pending'", task).Scan(&followups); err != nil {
				t.Fatal(err)
			}
			if err := s.db.QueryRow("SELECT count(*) FROM messages WHERE task_id=? AND sender='root' AND recipient='operator'", task).Scan(&operatorMessages); err != nil {
				t.Fatal(err)
			}
			if tc.valid {
				if decisions != 1 || followups != 1 || operatorMessages != 0 {
					t.Fatalf("decision=%d followups=%d operator=%d", decisions, followups, operatorMessages)
				}
				follow, err := s.claim(nil)
				if err != nil || follow == nil || follow.Kind != "po_followup" {
					t.Fatalf("resumed PO %+v: %v", follow, err)
				}
				for _, fragment := range []string{task, "already been resolved", "phase is assigned", po, "do not emit resolve_hold", "Root-only"} {
					if !strings.Contains(follow.Request.Instructions, fragment) {
						t.Fatalf("missing handoff instruction %q", fragment)
					}
				}
			} else if followups != 0 {
				t.Fatalf("fence %s queued followup", tc.fence)
			}
		})
	}
}
