package service

import (
	"database/sql"
	"strings"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func activeAssignment(t *testing.T, s *Service, task, po string) job {
	t.Helper()
	var id, source string
	if err := s.db.QueryRow("SELECT id,source_id FROM jobs WHERE task_id=? AND kind='assignment'", task).Scan(&id, &source); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("UPDATE jobs SET status='submitted' WHERE id=?", id); err != nil {
		t.Fatal(err)
	}
	return job{ID: id, Agent: po, Project: "alpha", Task: task, Source: source, Kind: "assignment", Request: runtimeapi.Request{Role: "po"}}
}
func followupClaim(t *testing.T, s *Service) job {
	t.Helper()
	j, err := s.claim(nil)
	if err != nil || j == nil || j.Kind != "po_followup" {
		t.Fatalf("followup: %+v %v", j, err)
	}
	return *j
}
func TestPOFollowThroughAssignmentAndBound(t *testing.T) {
	t.Run("explicit operator instruction remains pinned", func(t *testing.T) {
		s, task, _, _ := continuationFixture(t)
		defer s.Close()
		if code := postContinuation(s, task, "Review only; do not deploy."); code != 202 {
			t.Fatalf("continuation %d", code)
		}
		j, err := s.claim(nil)
		if err != nil || j == nil || j.Kind != "po_continue" {
			t.Fatalf("claim %+v %v", j, err)
		}
		if err := s.complete(*j, runtimeapi.Result{Text: "Understood."}, nil, nil, nil); err != nil {
			t.Fatal(err)
		}
		follow := followupClaim(t, s)
		var note, brief bool
		for _, m := range follow.Request.Context {
			if m.Text == "Review only; do not deploy." && m.Sender == operator {
				note = true
			}
			if m.Text == "authoritative final assignment" && m.Sender == root {
				brief = true
			}
		}
		if !note || !brief {
			t.Fatalf("pinned instruction=%v brief=%v", note, brief)
		}
		if err := s.complete(follow, runtimeapi.Result{Text: "Review only; do not deploy.", Actions: []runtimeapi.Action{{Type: "hold_task", Project: "alpha", Body: "Review only; do not deploy."}}}, nil, nil, nil); err != nil {
			t.Fatal(err)
		}
		var n int
		if err := s.db.QueryRow("SELECT count(*) FROM jobs WHERE task_id=? AND kind='work'", task).Scan(&n); err != nil || n != 0 {
			t.Fatalf("unexpected work %d %v", n, err)
		}
		escalation, err := s.claim(nil)
		if err != nil || escalation == nil || escalation.Kind != "task_blocked" {
			t.Fatalf("escalation %+v %v", escalation, err)
		}
		if strings.Contains(escalation.Request.Instructions, "Review only; do not deploy.") {
			t.Fatal("private note leaked to Root instructions")
		}
		for _, m := range escalation.Request.Context {
			if strings.Contains(m.Text, "Review only; do not deploy.") {
				t.Fatal("private note leaked to Root context")
			}
		}
	})
	t.Run("update then worker", func(t *testing.T) {
		s, task, po, _ := continuationFixture(t)
		defer s.Close()
		j := activeAssignment(t, s, task, po)
		result := runtimeapi.Result{Text: "I will coordinate.", Actions: []runtimeapi.Action{{Type: "post_group", Project: "alpha", Body: "Starting coordination."}}}
		if err := s.complete(j, result, nil, nil, nil); err != nil {
			t.Fatal(err)
		}
		follow := followupClaim(t, s)
		hasBrief := false
		for _, m := range follow.Request.Context {
			if m.Text == "authoritative final assignment" {
				hasBrief = true
			}
		}
		if !hasBrief {
			t.Fatal("missing original brief")
		}
		if err := s.complete(follow, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "queue_worker", Project: "alpha", Body: "Proceed safely."}}}, nil, nil, nil); err != nil {
			t.Fatal(err)
		}
		var n int
		if err := s.db.QueryRow("SELECT count(*) FROM jobs WHERE task_id=? AND kind='work'", task).Scan(&n); err != nil || n != 1 {
			t.Fatalf("worker jobs %d %v", n, err)
		}
		if err := s.db.QueryRow("SELECT count(*) FROM messages WHERE project_id='alpha' AND recipient='group:alpha' AND text='Starting coordination.'").Scan(&n); err != nil || n != 1 {
			t.Fatalf("group post %d %v", n, err)
		}
	})
	t.Run("two followups then escalation", func(t *testing.T) {
		s, task, po, _ := continuationFixture(t)
		defer s.Close()
		j := activeAssignment(t, s, task, po)
		for i := 0; i < 3; i++ {
			if err := s.complete(j, runtimeapi.Result{Text: "Still considering."}, nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			if i < 2 {
				j = followupClaim(t, s)
			}
		}
		var phase string
		var n int
		if err := s.db.QueryRow("SELECT status FROM tasks WHERE id=?", task).Scan(&phase); err != nil {
			t.Fatal(err)
		}
		if err := s.db.QueryRow("SELECT count(*) FROM po_followups").Scan(&n); err != nil || phase != "blocked" || n != 2 {
			t.Fatalf("phase %s followups %d %v", phase, n, err)
		}
		if err := s.db.QueryRow("SELECT count(*) FROM jobs WHERE task_id=? AND kind='task_blocked'", task).Scan(&n); err != nil || n != 1 {
			t.Fatalf("missing Root escalation %d %v", n, err)
		}
	})
}
func TestPOFollowThroughReviewAcceptedAndReport(t *testing.T) {
	s, task, po, worker := continuationFixture(t)
	defer s.Close()
	source, id := newID(), newID()
	if err := s.write(func(tx *sql.Tx) error {
		if _, err := tx.Exec("UPDATE tasks SET status='review' WHERE id=?", task); err != nil {
			return err
		}
		if err := addMessage(tx, source, worker, po, "worker", "worker result", "alpha", task); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT INTO jobs(id,agent_id,project_id,task_id,source_id,kind,status,request,next_at) VALUES(?,?,'alpha',?,?,'review','submitted',?,0)`, id, po, task, source, []byte("{}"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	j := job{ID: id, Agent: po, Project: "alpha", Task: task, Source: source, Kind: "review", Request: runtimeapi.Request{Role: "po"}}
	if err := s.complete(j, runtimeapi.Result{Text: "Checking result."}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	follow := followupClaim(t, s)
	if err := s.complete(follow, runtimeapi.Result{Text: "Checking further."}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	follow = followupClaim(t, s)
	if err := s.complete(follow, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "accept_result", Project: "alpha"}}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	final := followupClaim(t, s)
	if !strings.Contains(final.Request.Instructions, "already accepted") {
		t.Fatal("accepted phase not supplied")
	}
	if err := s.complete(final, runtimeapi.Result{Text: "Preparing report."}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	final = followupClaim(t, s)
	if err := s.complete(final, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "report", Project: "alpha", Body: "Result accepted."}}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	var state string
	if err := s.db.QueryRow("SELECT status FROM tasks WHERE id=?", task).Scan(&state); err != nil || state != "completed" {
		t.Fatalf("task %s %v", state, err)
	}
	var receipt int
	if err := s.db.QueryRow(`SELECT count(*) FROM jobs j JOIN messages m ON m.id=j.source_id WHERE j.task_id=? AND j.kind='project_report' AND j.agent_id=? AND m.sender=? AND m.recipient=? AND m.text='Result accepted.'`, task, root, po, root).Scan(&receipt); err != nil || receipt != 1 {
		t.Fatalf("Root report receipt %d: %v", receipt, err)
	}
}
func TestPOFollowThroughHoldAndFences(t *testing.T) {
	for _, kind := range []string{"hold", "approval", "stopped", "uncertain"} {
		t.Run(kind, func(t *testing.T) {
			s, task, po, worker := continuationFixture(t)
			defer s.Close()
			j := activeAssignment(t, s, task, po)
			result := runtimeapi.Result{Text: "status"}
			switch kind {
			case "hold":
				result.Actions = []runtimeapi.Action{{Type: "hold_task", Project: "alpha", Body: "Waiting for a required decision."}}
			case "approval":
				result.Actions = []runtimeapi.Action{{Type: "request_approval", Project: "alpha", Title: "Decision", Body: "Choose an option."}}
			case "stopped":
				if _, err := s.db.Exec("UPDATE projects SET stopped=1 WHERE id='alpha'"); err != nil {
					t.Fatal(err)
				}
			case "uncertain":
				source := newID()
				if err := s.write(func(tx *sql.Tx) error {
					if err := addMessage(tx, source, po, worker, "po", "old work", "alpha", task); err != nil {
						return err
					}
					_, err := tx.Exec(`INSERT INTO jobs(id,agent_id,project_id,task_id,source_id,kind,status,request,next_at) VALUES(?,?,'alpha',?,?,'work','uncertain',?,0)`, newID(), worker, task, source, []byte("{}"))
					return err
				}); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.complete(j, result, nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			var n int
			if err := s.db.QueryRow("SELECT count(*) FROM po_followups").Scan(&n); err != nil || n != 0 {
				t.Fatalf("%s followups %d %v", kind, n, err)
			}
			if kind == "hold" {
				var state string
				if err := s.db.QueryRow("SELECT status FROM tasks WHERE id=?", task).Scan(&state); err != nil || state != "blocked" {
					t.Fatalf("hold %s %v", state, err)
				}
			}
		})
	}
}
