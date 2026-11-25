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
