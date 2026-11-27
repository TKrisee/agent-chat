package service

import (
	"context"
	"database/sql"
	"net/http/httptest"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func TestLocalHoldSupervisionWaitsForIdleAndPreservesHold(t *testing.T) {
	for _, otherWork := range []bool{false, true} {
		t.Run(map[bool]string{false: "idle", true: "after independent work"}[otherWork], func(t *testing.T) {
			s, task, po, worker := continuationFixture(t)
			defer s.Close()
			origin := newID()
			if err := s.write(func(tx *sql.Tx) error {
				if err := addMessage(tx, origin, operator, root, "operator", "Original authorized scope", "", ""); err != nil {
					return err
				}
				_, err := tx.Exec(`INSERT INTO task_origins(task_id,message_id) VALUES(?,?)`, task, origin)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			j := activeAssignment(t, s, task, po)
			if otherWork {
				if _, err := s.userMessage(context.Background(), newID(), worker, "", "Independent question", "", nil); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.complete(j, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "hold_task", Body: "Technical input unavailable."}}}, nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			if otherWork {
				var count int
				if err := s.db.QueryRow(`SELECT count(*) FROM task_local_supervisions`).Scan(&count); err != nil || count != 0 {
					t.Fatalf("premature wake=%d %v", count, err)
				}
				work, err := s.claim(nil)
				if err != nil || work == nil || work.Agent != worker {
					t.Fatalf("independent claim %v", err)
				}
				if err := s.complete(*work, runtimeapi.Result{Text: "Independent response."}, nil, nil, nil); err != nil {
					t.Fatal(err)
				}
			}
			supervisor, err := s.claim(nil)
			if err != nil || supervisor == nil || supervisor.Kind != "task_blocked" || !supervisor.Request.LocalHoldSupervisor {
				t.Fatalf("supervisor %v", err)
			}
			if err := s.complete(*supervisor, runtimeapi.Result{Text: "Internal assessment.", Actions: []runtimeapi.Action{{Type: "delegate", Project: "alpha", Title: "Independent evidence review", Body: "Review already authorized independent evidence without changing the held work.", ReplyTo: supervisor.Source}}}, nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			var status string
			var wake, links, operatorOut int
			if err := s.db.QueryRow(`SELECT status FROM tasks WHERE id=?`, task).Scan(&status); err != nil {
				t.Fatal(err)
			}
			if err := s.db.QueryRow(`SELECT count(*),count(recovery_task_id) FROM task_local_supervisions`).Scan(&wake, &links); err != nil {
				t.Fatal(err)
			}
			if err := s.db.QueryRow(`SELECT count(*) FROM messages WHERE sender=? AND recipient=? AND text='Internal assessment.'`, root, operator).Scan(&operatorOut); err != nil {
				t.Fatal(err)
			}
			if status != "blocked" || wake != 1 || links != 1 || operatorOut != 0 {
				t.Fatalf("hold=%s wake=%d recovery=%d operator=%d", status, wake, links, operatorOut)
			}
			var newOrigin string
			if err := s.db.QueryRow(`SELECT o.message_id FROM task_local_supervisions x JOIN task_origins o ON o.task_id=x.recovery_task_id`).Scan(&newOrigin); err != nil || newOrigin != origin {
				t.Fatalf("origin %q: %v", newOrigin, err)
			}
			scope, err := s.claim(nil)
			if err != nil || scope == nil || scope.Kind != "scope_review" {
				t.Fatalf("scope review: %v", err)
			}
			var quoted string
			if err := s.db.QueryRow(`SELECT parent_id FROM message_replies WHERE message_id=?`, scope.Source).Scan(&quoted); err != nil || quoted != supervisor.Source {
				t.Fatalf("supervision quote: %v", err)
			}
			if err := s.complete(*scope, runtimeapi.Result{Text: "Assess independent scope."}, nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			decision, err := s.claim(nil)
			if err != nil || decision == nil || decision.Kind != "scope_decision" {
				t.Fatalf("scope decision: %v", err)
			}
			if err := s.complete(*decision, runtimeapi.Result{Text: "No safe independent scope identified."}, nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			if err := s.db.QueryRow(`SELECT count(*) FROM messages WHERE sender=? AND recipient=? AND text='No safe independent scope identified.'`, root, operator).Scan(&operatorOut); err != nil || operatorOut != 0 {
				t.Fatalf("scope decision leaked: %v", err)
			}
			if err := s.db.QueryRow(`SELECT count(*) FROM task_local_supervisions`).Scan(&wake); err != nil || wake != 1 {
				t.Fatalf("recursive wake=%d: %v", wake, err)
			}
		})
	}
}

func TestLocalHoldSupervisionAfterLastJobUncertain(t *testing.T) {
	s, task, po, worker := continuationFixture(t)
	defer s.Close()
	origin := newID()
	if err := s.write(func(tx *sql.Tx) error {
		if err := addMessage(tx, origin, operator, root, "operator", "Original scope", "", ""); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT INTO task_origins(task_id,message_id) VALUES(?,?)`, task, origin)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.userMessage(context.Background(), newID(), worker, "", "Independent question", "", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.complete(activeAssignment(t, s, task, po), runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "hold_task", Body: "Technical input unavailable."}}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	work, err := s.claim(nil)
	if err != nil || work == nil || work.Agent != worker {
		t.Fatalf("work: %+v %v", work, err)
	}
	if err := s.write(func(tx *sql.Tx) error {
		if _, err := tx.Exec(`UPDATE jobs SET kind='work' WHERE id=?`, work.ID); err != nil {
			return err
		}
		stale := newID()
		if err := addMessage(tx, stale, "service", root, "system", "stale internal wake", "alpha", task); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO jobs(id,agent_id,project_id,task_id,source_id,kind,status,request,next_at) VALUES(?,?,'alpha',?,?,'root_backlog','pending',?,0)`, newID(), root, task, stale, []byte("{}")); err != nil {
			return err
		}
		pending, otherTask := newID(), newID()
		if _, err := tx.Exec(`INSERT INTO tasks VALUES(?,'alpha','Other work','queued',?)`, otherTask, worker); err != nil {
			return err
		}
		if err := addMessage(tx, pending, po, worker, "po", "dependent work", "alpha", otherTask); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT INTO jobs(id,agent_id,project_id,task_id,source_id,kind,status,request,next_at) VALUES(?,?,'alpha',?,?,'work','pending',?,0)`, newID(), worker, otherTask, pending, []byte("{}"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.disposition(work.ID, "uncertain", "unknown", 0); err != nil {
		t.Fatal(err)
	}
	wake, err := s.claim(nil)
	if err != nil || wake == nil || wake.Kind != "task_blocked" || !wake.Request.LocalHoldSupervisor {
		t.Fatalf("supervisor: %+v %v", wake, err)
	}
	var status string
	if err := s.db.QueryRow(`SELECT status FROM jobs WHERE id=?`, work.ID).Scan(&status); err != nil || status != "uncertain" {
		t.Fatalf("job=%s %v", status, err)
	}
	if err := s.complete(*wake, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "delegate", Project: "alpha", Title: "Independent investigation", Body: "Inspect an independent authorized route."}}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	var linked int
	if err := s.db.QueryRow(`SELECT count(*) FROM task_local_supervisions WHERE source_id=? AND recovery_task_id IS NOT NULL`, wake.Source).Scan(&linked); err != nil || linked != 1 {
		t.Fatalf("supervisor action blocked by fenced pending jobs: %d %v", linked, err)
	}
}

func TestLocalHoldSupervisionAfterProjectResume(t *testing.T) {
	s, task, po, worker := continuationFixture(t)
	defer s.Close()
	origin := newID()
	if err := s.write(func(tx *sql.Tx) error {
		if err := addMessage(tx, origin, operator, root, "operator", "Original scope", "", ""); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT INTO task_origins(task_id,message_id) VALUES(?,?)`, task, origin)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.userMessage(context.Background(), newID(), worker, "", "Independent question", "", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.complete(activeAssignment(t, s, task, po), runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "hold_task", Body: "Technical input unavailable."}}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	work, err := s.claim(nil)
	if err != nil || work == nil || work.Agent != worker {
		t.Fatalf("work: %+v %v", work, err)
	}
	if _, err := s.db.Exec(`UPDATE projects SET stopped=1 WHERE id='alpha'`); err != nil {
		t.Fatal(err)
	}
	if err := s.disposition(work.ID, "uncertain", "unknown", 0); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM task_local_supervisions`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("stopped wake=%d %v", n, err)
	}
	w := httptest.NewRecorder()
	s.setStop(w, "alpha", false)
	if w.Code != 200 {
		t.Fatalf("resume status=%d", w.Code)
	}
	wake, err := s.claim(nil)
	if err != nil || wake == nil || wake.Kind != "task_blocked" || !wake.Request.LocalHoldSupervisor {
		t.Fatalf("supervisor: %+v %v", wake, err)
	}
}
