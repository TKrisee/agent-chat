package service

import (
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func TestChildReviewProgressAfterReportIsAtomic(t *testing.T) {
	for _, tc := range []struct {
		name, fence string
		valid       bool
	}{
		{"accepted child", "", true}, {"foreign project", "project", false}, {"stopped", "stop", false}, {"already completed", "completed", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, j, _, worker, _ := childReviewFixture(t)
			defer s.Close()
			switch tc.fence {
			case "stop":
				if _, err := s.db.Exec("UPDATE projects SET stopped=1 WHERE id='alpha'"); err != nil {
					t.Fatal(err)
				}
			case "completed":
				if _, err := s.db.Exec("UPDATE tasks SET status='completed' WHERE id=?", j.Task); err != nil {
					t.Fatal(err)
				}
			}
			project := "alpha"
			if tc.fence == "project" {
				project = "other"
			}
			result := runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "accept_result", Project: "alpha", Recipient: worker}, {Type: "report", Project: "alpha", Recipient: root, Body: "Reviewed receipt."}, {Type: "report_progress", Project: project, Recipient: root, Body: "Explicit progress."}}}
			if err := s.complete(j, result, nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			var progress, reports int
			if err := s.db.QueryRow("SELECT count(*) FROM messages WHERE task_id=? AND sender=? AND recipient=? AND text='Explicit progress.'", j.Task, j.Agent, root).Scan(&progress); err != nil {
				t.Fatal(err)
			}
			if err := s.db.QueryRow("SELECT count(*) FROM messages WHERE task_id=? AND sender=? AND text='Reviewed receipt.'", j.Task, j.Agent).Scan(&reports); err != nil {
				t.Fatal(err)
			}
			if tc.valid && (progress != 1 || reports != 1) {
				t.Fatalf("progress=%d report=%d", progress, reports)
			}
			if !tc.valid && (progress != 0 || reports != 0) {
				t.Fatalf("rejected batch applied progress=%d report=%d", progress, reports)
			}
		})
	}
}
