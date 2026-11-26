package service

import (
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func TestParentReportAcceptsOnlyProvenCompletedChild(t *testing.T) {
	for _, tc := range []struct {
		name, change string
		valid        bool
	}{
		{"child report to root", "", true},
		{"child report to self", "self", true},
		{"wrong worker", "worker", false},
		{"missing review", "review", false},
		{"missing report", "report", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, review, parent, worker, other := childReviewFixture(t)
			defer s.Close()
			if err := s.complete(review, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "accept_result", Project: "alpha", Recipient: worker}, {Type: "report", Project: "alpha", Recipient: root, Body: "Reviewed."}}}, nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			var err error
			switch tc.change {
			case "self":
				_, err = s.db.Exec("UPDATE messages SET recipient=? WHERE task_id=? AND role='po' AND recipient=?", review.Agent, review.Task, root)
			case "review":
				_, err = s.db.Exec("UPDATE jobs SET diagnostic='failed' WHERE id=?", review.ID)
			case "report":
				_, err = s.db.Exec("UPDATE messages SET recipient='operator' WHERE task_id=? AND role='po' AND recipient IN (?,?)", review.Task, root, review.Agent)
			}
			if err != nil {
				t.Fatal(err)
			}
			follow := followupClaim(t, s)
			if follow.Task != parent {
				t.Fatalf("followup task %s", follow.Task)
			}
			if tc.change == "worker" {
				worker = other
			}
			if err := s.complete(follow, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "accept_result", Project: "alpha", Recipient: worker}, {Type: "report", Project: "alpha", Recipient: root, Body: "Final report."}}}, nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			var status string
			if err := s.db.QueryRow("SELECT status FROM tasks WHERE id=?", parent).Scan(&status); err != nil {
				t.Fatal(err)
			}
			if (status == "completed") != tc.valid {
				t.Fatalf("parent status %s, expected valid=%v", status, tc.valid)
			}
		})
	}
}
