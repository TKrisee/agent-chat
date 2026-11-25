package service

import (
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func TestTitledPOPostAndProvisionAtomic(t *testing.T) {
	for _, tc := range []struct {
		name, project, recipient string
		valid                    bool
	}{
		{"titled", "alpha", "", true}, {"wrong project", "other", "", false}, {"recipient", "alpha", "someone", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, task, po, _ := continuationFixture(t)
			defer s.Close()
			j := activeAssignment(t, s, task, po)
			result := runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "post_group", Project: tc.project, Recipient: tc.recipient, Title: "Handoff acknowledged", Body: "Beginning work."}, {Type: "create_worker", Title: "Character-Art"}}}
			if err := s.complete(j, result, nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			var workers, posts int
			if err := s.db.QueryRow("SELECT count(*) FROM agents WHERE project_id='alpha' AND name='Character-Art'").Scan(&workers); err != nil {
				t.Fatal(err)
			}
			if err := s.db.QueryRow("SELECT count(*) FROM messages WHERE recipient='group:alpha' AND text='Beginning work.'").Scan(&posts); err != nil {
				t.Fatal(err)
			}
			want := 0
			if tc.valid {
				want = 1
			}
			if workers != want || posts != want {
				t.Fatalf("worker=%d posts=%d want=%d", workers, posts, want)
			}
		})
	}
}

const titledRejectedResult = `{"text":"","actions":[{"type":"post_group","project":"alpha","title":"Acknowledged","body":"Beginning work.","recipient":""},{"type":"create_worker","project":"alpha","title":"Character-Art","body":"","recipient":""}],"attachments":[]}`

func TestTitledPostLegacyRecoveryFences(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, *Service, string, string)
		want   int
	}{
		{"eligible", nil, 202},
		{"stopped", func(t *testing.T, s *Service, task, po string) {
			if _, e := s.db.Exec("UPDATE projects SET stopped=1 WHERE id='alpha'"); e != nil {
				t.Fatal(e)
			}
		}, 409},
		{"changed owner", func(t *testing.T, s *Service, task, po string) {
			if _, e := s.db.Exec("UPDATE tasks SET owner_id=(SELECT id FROM agents WHERE project_id='alpha' AND role='worker' LIMIT 1) WHERE id=?", task); e != nil {
				t.Fatal(e)
			}
		}, 409},
		{"other failure", func(t *testing.T, s *Service, task, po string) {
			if _, e := s.db.Exec("UPDATE jobs SET diagnostic='invalid runtime result: another reason' WHERE task_id=? AND kind='assignment'", task); e != nil {
				t.Fatal(e)
			}
		}, 409},
		{"uncertain", func(t *testing.T, s *Service, task, po string) {
			if _, e := s.db.Exec("UPDATE jobs SET status='uncertain' WHERE task_id=? AND kind='assignment'", task); e != nil {
				t.Fatal(e)
			}
		}, 409},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, task, po, _ := continuationFixture(t)
			defer s.Close()
			installReviewInspector(t, s)
			_, err := s.db.Exec("UPDATE jobs SET status='completed',diagnostic='invalid runtime result: group post outside project scope',result=? WHERE task_id=? AND kind='assignment'", []byte(titledRejectedResult), task)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.db.Exec("UPDATE tasks SET status='blocked' WHERE id=?", task); err != nil {
				t.Fatal(err)
			}
			if tc.mutate != nil {
				tc.mutate(t, s, task, po)
			}
			id := newID()
			if code := postContinuationID(s, task, "Continue the existing assignment.", id); code != tc.want {
				t.Fatalf("status=%d want=%d", code, tc.want)
			}
			if tc.want != 202 {
				return
			}
			if code := postContinuationID(s, task, "Continue the existing assignment.", id); code != 202 {
				t.Fatalf("idempotent retry=%d", code)
			}
			var n int
			if err := s.db.QueryRow("SELECT count(*) FROM jobs WHERE task_id=? AND source_id=?", task, id).Scan(&n); err != nil || n != 1 {
				t.Fatalf("queued=%d err=%v", n, err)
			}
			j, err := s.claim(nil)
			if err != nil || j == nil || j.Task != task {
				t.Fatalf("claim %+v: %v", j, err)
			}
			found := false
			for _, m := range j.Request.Context {
				if m.Text == "authoritative final assignment" && m.Sender == root {
					found = true
				}
			}
			if !found {
				t.Fatal("original assignment not pinned")
			}
			if err := s.db.QueryRow("SELECT count(*) FROM agents WHERE project_id='alpha' AND name='Character-Art'").Scan(&n); err != nil || n != 0 {
				t.Fatalf("rejected action applied=%d err=%v", n, err)
			}
		})
	}
}

func TestRecoveredTitledAssignmentCanDispatchTeam(t *testing.T) {
	s, task, _, _ := continuationFixture(t)
	defer s.Close()
	installReviewInspector(t, s)
	if _, err := s.db.Exec("UPDATE jobs SET status='completed',diagnostic='invalid runtime result: group post outside project scope',result=? WHERE task_id=? AND kind='assignment'", []byte(titledRejectedResult), task); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("UPDATE tasks SET status='blocked' WHERE id=?", task); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("INSERT INTO project_workspace_roots(project_id,path) VALUES('alpha',?)", t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if code := postContinuation(s, task, "Continue the existing assignment."); code != 202 {
		t.Fatalf("continuation=%d", code)
	}
	j, err := s.claim(nil)
	if err != nil || j == nil || j.Kind != "po_followup" {
		t.Fatalf("claim %+v: %v", j, err)
	}
	actions := []runtimeapi.Action{
		{Type: "create_worker", Title: "Character-Art"}, {Type: "create_worker", Title: "Technical-Art"},
		{Type: "queue_work_item", Title: "Character sources", Body: "Trace character provenance.", Recipient: "Character-Art"},
		{Type: "queue_work_item", Title: "Technical sources", Body: "Trace technical provenance.", Recipient: "Technical-Art"},
	}
	if err := s.complete(*j, runtimeapi.Result{Actions: actions}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	var status, diag string
	if err := s.db.QueryRow("SELECT status,COALESCE(diagnostic,'') FROM jobs WHERE id=?", j.ID).Scan(&status, &diag); err != nil || status != "completed" || diag != "" {
		t.Fatalf("recovered result %s %q: %v", status, diag, err)
	}
	var children, owners int
	if err := s.db.QueryRow("SELECT count(*),count(DISTINCT t.owner_id) FROM po_work_items i JOIN tasks t ON t.id=i.child_task_id WHERE i.parent_task_id=? AND t.status='queued'", task).Scan(&children, &owners); err != nil || children != 2 || owners != 2 {
		t.Fatalf("children=%d owners=%d: %v", children, owners, err)
	}
	var planningSource string
	if err := s.db.QueryRow("SELECT source_id FROM jobs WHERE task_id=? AND kind='po_followup' AND status='pending'", task).Scan(&planningSource); err != nil {
		t.Fatal(err)
	}
	var original string
	if err := s.db.QueryRow("SELECT assignment_id FROM po_followups WHERE source_id=?", planningSource).Scan(&original); err != nil {
		t.Fatal(err)
	}
	var text string
	if err := s.db.QueryRow("SELECT text FROM messages WHERE id=?", original).Scan(&text); err != nil || text != "authoritative final assignment" {
		t.Fatalf("planning assignment=%q: %v", text, err)
	}
}

func TestSecondLegacyLineageRejectionCanResumeOnce(t *testing.T) {
	for _, tc := range []struct {
		name, fence string
		want        int
	}{{"eligible", "", 202}, {"stop", "stop", 409}, {"owner", "owner", 409}, {"uncertain", "uncertain", 409}} {
		t.Run(tc.name, func(t *testing.T) {
			s, task, _, _ := continuationFixture(t)
			defer s.Close()
			installReviewInspector(t, s)
			if _, err := s.db.Exec("UPDATE jobs SET status='completed',diagnostic='invalid runtime result: group post outside project scope',result=? WHERE task_id=? AND kind='assignment'", []byte(titledRejectedResult), task); err != nil {
				t.Fatal(err)
			}
			if _, err := s.db.Exec("UPDATE tasks SET status='blocked' WHERE id=?", task); err != nil {
				t.Fatal(err)
			}
			if code := postContinuation(s, task, "Continue the existing assignment."); code != 202 {
				t.Fatalf("first continuation=%d", code)
			}
			j, err := s.claim(nil)
			if err != nil || j == nil || j.Kind != "po_followup" {
				t.Fatalf("claim %+v: %v", j, err)
			}
			payload := `{"text":"","actions":[{"type":"create_worker","project":"alpha","title":"Character-Art","body":"","recipient":""},{"type":"queue_work_item","project":"alpha","title":"Trace","body":"Trace approved source.","recipient":"Character-Art"}],"attachments":[]}`
			if _, err = s.db.Exec("UPDATE jobs SET status='completed',diagnostic='invalid runtime result: original PO assignment is unavailable',result=? WHERE id=?", []byte(payload), j.ID); err != nil {
				t.Fatal(err)
			}
			if _, err = s.db.Exec("UPDATE tasks SET status='blocked' WHERE id=?", task); err != nil {
				t.Fatal(err)
			}
			switch tc.fence {
			case "stop":
				_, err = s.db.Exec("UPDATE projects SET stopped=1 WHERE id='alpha'")
			case "owner":
				_, err = s.db.Exec("UPDATE tasks SET owner_id=(SELECT id FROM agents WHERE project_id='alpha' AND role='worker' LIMIT 1) WHERE id=?", task)
			case "uncertain":
				_, err = s.db.Exec("UPDATE jobs SET status='uncertain' WHERE id=?", j.ID)
			}
			if err != nil {
				t.Fatal(err)
			}
			id := newID()
			if code := postContinuationID(s, task, "Continue the same task.", id); code != tc.want {
				t.Fatalf("second continuation=%d want=%d", code, tc.want)
			}
			if tc.want != 202 {
				return
			}
			if code := postContinuationID(s, task, "Continue the same task.", id); code != 202 {
				t.Fatalf("idempotent=%d", code)
			}
			var n int
			if err := s.db.QueryRow("SELECT count(*) FROM jobs WHERE task_id=? AND source_id=?", task, id).Scan(&n); err != nil || n != 1 {
				t.Fatalf("jobs=%d: %v", n, err)
			}
		})
	}
}
