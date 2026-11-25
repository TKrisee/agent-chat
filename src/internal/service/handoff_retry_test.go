package service

import (
	"database/sql"
	"net/http/httptest"
	"strings"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func scopeFixture(t *testing.T) (*Service, string, string) {
	t.Helper()
	s := openTest(t, t.TempDir())
	po, task, origin, assessment, draft := newID(), newID(), newID(), newID(), newID()
	err := s.write(func(tx *sql.Tx) error {
		if _, err := tx.Exec("INSERT INTO projects VALUES('alpha','Alpha',?,'/tmp',0)", po); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT INTO agents VALUES(?,?,'po','alpha','model','medium')", po, "PO"); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT INTO tasks VALUES(?,'alpha','draft','assigned',?)", task, po); err != nil {
			return err
		}
		if err := addMessage(tx, origin, operator, root, "user", "original", "alpha", task); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT INTO task_origins VALUES(?,?)", task, origin); err != nil {
			return err
		}
		if err := addMessage(tx, assessment, po, root, "po", "assessment", "alpha", task); err != nil {
			return err
		}
		if err := addMessage(tx, draft, root, po, "root", "draft", "alpha", task); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO jobs(id,agent_id,project_id,task_id,source_id,kind,status,request,next_at) VALUES(?,?,'alpha',?,?,'scope_review','completed',?,0)`, newID(), po, task, draft, []byte("{}")); err != nil {
			return err
		}
		a, err := getAgent(tx, root)
		if err != nil {
			return err
		}
		return s.enqueue(tx, a, "alpha", task, assessment, "scope_decision", "finalize")
	})
	if err != nil {
		t.Fatal(err)
	}
	return s, task, po
}
func claimScope(t *testing.T, s *Service) job {
	t.Helper()
	j, err := s.claim(nil)
	if err != nil || j == nil {
		t.Fatalf("claim: %+v %v", j, err)
	}
	if err := s.disposition(j.ID, "submitted", "", 0); err != nil {
		t.Fatal(err)
	}
	return *j
}
func TestScopeCorrectionBoundAndRecipient(t *testing.T) {
	t.Run("corrected appointed recipient", func(t *testing.T) {
		s, task, po := scopeFixture(t)
		defer s.Close()
		if err := s.write(func(tx *sql.Tx) error {
			for i := 0; i < 30; i++ {
				if err := addMessage(tx, newID(), "service", root, "system", "later", "alpha", task); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		j := claimScope(t, s)
		invalid := runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "delegate", Project: "alpha", Title: "final", Body: "body", Recipient: "wrong"}}}
		if err := s.complete(j, invalid, nil, nil, nil); err != nil {
			t.Fatal(err)
		}
		next := claimScope(t, s)
		if next.Task != task || !strings.Contains(next.Request.Instructions, "recipient must be empty or the appointed PO ID") {
			t.Fatalf("correction context: %+v", next)
		}
		var assessment, original bool
		for _, m := range next.Request.Context {
			if m.Text == "assessment" && m.Sender == po {
				assessment = true
			}
			if m.Text == "original" && m.Sender == operator {
				original = true
			}
		}
		if !assessment || !original {
			t.Fatalf("scope packet not preserved: assessment=%v original=%v", assessment, original)
		}
		if err := s.complete(next, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "delegate", Project: "alpha", Title: "final", Body: "body", Recipient: po}}}, nil, nil, nil); err != nil {
			t.Fatal(err)
		}
		var n int
		if err := s.db.QueryRow("SELECT count(*) FROM jobs WHERE task_id=? AND kind='assignment'", task).Scan(&n); err != nil || n != 1 {
			t.Fatalf("assignment count %d: %v", n, err)
		}
	})
	t.Run("two failed corrections block", func(t *testing.T) {
		s, task, _ := scopeFixture(t)
		defer s.Close()
		for i := 0; i < 3; i++ {
			j := claimScope(t, s)
			if err := s.complete(j, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "delegate", Project: "alpha", Body: "body"}}}, nil, nil, nil); err != nil {
				t.Fatal(err)
			}
		}
		var status string
		var n int
		if err := s.db.QueryRow("SELECT status FROM tasks WHERE id=?", task).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if err := s.db.QueryRow("SELECT count(*) FROM jobs WHERE task_id=? AND kind='scope_decision'", task).Scan(&n); err != nil || status != "blocked" || n != 3 {
			t.Fatalf("status %s decisions %d: %v", status, n, err)
		}
	})
}
func TestLegacyHandoffRetryIsExplicitAndIdempotent(t *testing.T) {
	s, task, _ := scopeFixture(t)
	defer s.Close()
	j := claimScope(t, s)
	if _, err := s.db.Exec("UPDATE jobs SET status='completed',diagnostic='invalid runtime result: final assignment requires the same project, a title and a body' WHERE id=?", j.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("UPDATE tasks SET status='blocked' WHERE id=?", task); err != nil {
		t.Fatal(err)
	}
	view, err := s.state()
	if err != nil || len(view.Tasks) != 1 || !view.Tasks[0].RetryHandoffEligible {
		t.Fatalf("eligible: %+v %v", view.Tasks, err)
	}
	request := func() int {
		r := httptest.NewRequest("POST", "/api/tasks/"+task+"/retry-handoff", nil)
		r.SetPathValue("id", task)
		w := httptest.NewRecorder()
		s.retryHandoff(w, r)
		return w.Code
	}
	if code := request(); code != 202 {
		t.Fatalf("retry %d", code)
	}
	if code := request(); code != 409 {
		t.Fatalf("duplicate %d", code)
	}
	next := claimScope(t, s)
	if next.Task != task || next.Kind != "scope_decision" {
		t.Fatalf("wrong retry: %+v", next)
	}
}

func TestScopeCorrectionRespectsStoppedAndUncertain(t *testing.T) {
	for _, fence := range []string{"stopped", "uncertain"} {
		t.Run(fence, func(t *testing.T) {
			s, task, po := scopeFixture(t)
			defer s.Close()
			j := claimScope(t, s)
			if fence == "stopped" {
				if _, err := s.db.Exec("UPDATE projects SET stopped=1 WHERE id='alpha'"); err != nil {
					t.Fatal(err)
				}
			} else {
				_, err := s.db.Exec(`INSERT INTO jobs(id,agent_id,project_id,task_id,source_id,kind,status,request,next_at) VALUES(?,?,'alpha',?,?,'work','uncertain',?,0)`, newID(), po, task, j.Source, []byte("{}"))
				if err != nil {
					t.Fatal(err)
				}
			}
			invalid := runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "delegate", Project: "alpha", Body: "body"}}}
			if err := s.complete(j, invalid, nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			var count int
			if err := s.db.QueryRow("SELECT count(*) FROM jobs WHERE task_id=? AND kind='scope_decision'", task).Scan(&count); err != nil || count != 1 {
				t.Fatalf("unexpected correction count %d: %v", count, err)
			}
		})
	}
}

func TestHandoffRecoveryRejectsUnlinkedAssessment(t *testing.T) {
	s, task, _ := scopeFixture(t)
	defer s.Close()
	j := claimScope(t, s)
	if _, err := s.db.Exec("UPDATE jobs SET status='completed',diagnostic='invalid runtime result: final assignment requires the same project, a title and a body' WHERE id=?", j.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("UPDATE tasks SET status='blocked' WHERE id=?", task); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("UPDATE messages SET role='system' WHERE id=?", j.Source); err != nil {
		t.Fatal(err)
	}
	view, err := s.state()
	if err != nil {
		t.Fatal(err)
	}
	if view.Tasks[0].RetryHandoffEligible {
		t.Fatal("unlinked assessment was accepted")
	}
}
