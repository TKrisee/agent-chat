package service

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

const testBacklogGoal = "Finish the authorized work"
const testBacklogIntake = "/backlog alpha\nInspect the remaining documents. Never publish or deploy."

func setBacklogGoal(t *testing.T, s *Service) {
	t.Helper()
	if err := s.write(func(tx *sql.Tx) error {
		return setProjectGoal(tx, runtimeapi.Action{Type: "set_project_goal", Project: "alpha", Body: testBacklogGoal})
	}); err != nil {
		t.Fatal(err)
	}
}

func postBacklogIntake(s *Service, id, text string) int {
	request := httptest.NewRequest(http.MethodPost, "/api/messages",
		strings.NewReader(fmt.Sprintf(`{"id":%q,"recipient":"root","text":%q}`, id, text)))
	request.Header.Set("Content-Type", "application/json")
	writer := httptest.NewRecorder()
	s.Handler(nil).ServeHTTP(writer, request)
	return writer.Code
}

func claimBacklogStep(t *testing.T, s *Service, kind string) job {
	t.Helper()
	next, err := s.claim(nil)
	if err != nil || next == nil || next.Kind != kind {
		t.Fatalf("want %s, got %+v: %v", kind, next, err)
	}
	return *next
}

func completeBacklogStep(t *testing.T, s *Service, j job, result runtimeapi.Result) {
	t.Helper()
	if err := s.complete(j, result, nil, nil, nil); err != nil {
		t.Fatalf("complete %s: %v", j.Kind, err)
	}
}

func finishBacklogParent(t *testing.T, s *Service, worker string, beforeReport func()) string {
	t.Helper()
	delegate := runtimeapi.Result{Actions: []runtimeapi.Action{
		{Type: "delegate", Project: "alpha", Title: "Review first document", Body: "Inspect the first document."},
	}}
	intake := claimBacklogStep(t, s, "operator_message")
	completeBacklogStep(t, s, intake, delegate)
	scope := claimBacklogStep(t, s, "scope_review")
	completeBacklogStep(t, s, scope, runtimeapi.Result{Text: "The first document is ready for inspection."})
	decision := claimBacklogStep(t, s, "scope_decision")
	completeBacklogStep(t, s, decision, delegate)
	assignment := claimBacklogStep(t, s, "assignment")
	completeBacklogStep(t, s, assignment, runtimeapi.Result{Actions: []runtimeapi.Action{
		{Type: "queue_worker", Recipient: worker, Body: "Inspect the document."},
	}})
	work := claimBacklogStep(t, s, "work")
	completeBacklogStep(t, s, work, runtimeapi.Result{Text: "The document was inspected."})
	review := claimBacklogStep(t, s, "review")
	if beforeReport != nil {
		beforeReport()
	}
	completeBacklogStep(t, s, review, runtimeapi.Result{Actions: []runtimeapi.Action{
		{Type: "accept_result", Recipient: worker}, {Type: "report", Body: "Inspection accepted."},
	}})
	report := claimBacklogStep(t, s, "project_report")
	completeBacklogStep(t, s, report, runtimeapi.Result{Text: "The first inspection is complete."})
	return scope.Task
}

func TestExplicitBacklogIntakeAdvancesCompletedParent(t *testing.T) {
	s, _, po, worker := continuationFixture(t)
	defer s.Close()
	setBacklogGoal(t, s)
	origin := newID()
	if status := postBacklogIntake(s, origin, testBacklogIntake); status != http.StatusCreated {
		t.Fatalf("intake=%d", status)
	}
	if status := postBacklogIntake(s, origin, testBacklogIntake); status != http.StatusOK {
		t.Fatalf("repeated intake=%d", status)
	}
	if status := postBacklogIntake(s, newID(), testBacklogIntake); status != http.StatusConflict {
		t.Fatalf("second grant for same revision=%d", status)
	}

	task := finishBacklogParent(t, s, worker, nil)
	stop := httptest.NewRecorder()
	s.setStop(stop, "alpha", true)
	if stop.Code != http.StatusOK {
		t.Fatalf("stop=%d", stop.Code)
	}
	if wake, err := s.claim(nil); err != nil || wake != nil {
		t.Fatalf("claimed backlog while stopped: %+v %v", wake, err)
	}
	resume := httptest.NewRecorder()
	s.setStop(resume, "alpha", false)
	if resume.Code != http.StatusOK {
		t.Fatalf("resume=%d", resume.Code)
	}
	wake := claimBacklogStep(t, s, "root_backlog")
	if wake.Task != task || !strings.Contains(wake.Request.Instructions, "Never publish or deploy.") {
		t.Fatalf("wrong wake provenance: %s", wake.Task)
	}
	found := false
	for _, m := range wake.Request.Context {
		if m.ID == origin && m.Text == testBacklogIntake {
			found = true
		}
	}
	if !found {
		t.Fatal("authenticated intake absent from the wake")
	}
	delegate := runtimeapi.Result{Text: "Private backlog assessment.", Actions: []runtimeapi.Action{
		{Type: "delegate", Project: "alpha", Title: "Review next document", Body: "Inspect the next document."},
	}}
	completeBacklogStep(t, s, wake, delegate)
	completeBacklogStep(t, s, wake, delegate)
	next := claimBacklogStep(t, s, "scope_review")
	if next.Task == task || next.Project != "alpha" || next.Agent != po {
		t.Fatalf("wrong next assignment: %+v", next)
	}
	var count int
	if err := s.db.QueryRow(`SELECT count(*) FROM root_backlog_wakes w JOIN task_origins o ON o.task_id=w.next_task_id
		WHERE w.parent_task_id=? AND w.next_task_id=? AND o.message_id=?`, task, next.Task, origin).Scan(&count); err != nil || count != 1 {
		t.Fatalf("delegation provenance=%d: %v", count, err)
	}
	if err := s.db.QueryRow(`SELECT count(*) FROM messages WHERE recipient='operator' AND text='Private backlog assessment.'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("private assessment exposed=%d: %v", count, err)
	}
}

func TestOneShotAndRevisedGoalsCannotCreateBacklogCandidates(t *testing.T) {
	for _, test := range []struct {
		name, intake string
		changed      bool
	}{
		{"ordinary request with active goal", "Inspect the first document once, then stop.", false},
		{"goal activated after ordinary assignment", "Inspect the first document once, then stop.", true},
		{"same goal text replaced after explicit intake", testBacklogIntake, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, _, _, worker := continuationFixture(t)
			defer s.Close()
			if test.name != "goal activated after ordinary assignment" {
				setBacklogGoal(t, s)
			}
			if status := postBacklogIntake(s, newID(), test.intake); status != http.StatusCreated {
				t.Fatalf("intake=%d", status)
			}
			var beforeReport func()
			if test.changed {
				beforeReport = func() { setBacklogGoal(t, s) }
			}
			task := finishBacklogParent(t, s, worker, beforeReport)
			var count int
			if err := s.db.QueryRow(`SELECT count(*) FROM root_backlog_candidates WHERE parent_task_id=?`, task).Scan(&count); err != nil || count != 0 {
				t.Fatalf("unauthorized candidates=%d: %v", count, err)
			}
			if wake, err := s.claim(nil); err != nil || wake != nil {
				t.Fatalf("unauthorized wake: %+v %v", wake, err)
			}
		})
	}
}

func TestBacklogRejectsStaleResultAndCrossProjectIntake(t *testing.T) {
	t.Run("goal changed after wake claimed", func(t *testing.T) {
		s, _, _, worker := continuationFixture(t)
		defer s.Close()
		setBacklogGoal(t, s)
		if status := postBacklogIntake(s, newID(), testBacklogIntake); status != http.StatusCreated {
			t.Fatalf("intake=%d", status)
		}
		finishBacklogParent(t, s, worker, nil)
		wake := claimBacklogStep(t, s, "root_backlog")
		setBacklogGoal(t, s)
		completeBacklogStep(t, s, wake, runtimeapi.Result{Actions: []runtimeapi.Action{
			{Type: "delegate", Project: "alpha", Title: "Not authorized", Body: "Do work"},
		}})
		var count int
		if err := s.db.QueryRow(`SELECT count(*) FROM tasks WHERE title='Not authorized'`).Scan(&count); err != nil || count != 0 {
			t.Fatalf("stale delegation=%d: %v", count, err)
		}
	})
	t.Run("intake requires ready goal and exact project", func(t *testing.T) {
		s, _, _, _ := continuationFixture(t)
		defer s.Close()
		if status := postBacklogIntake(s, newID(), testBacklogIntake); status != http.StatusConflict {
			t.Fatalf("intake without goal=%d", status)
		}
		setBacklogGoal(t, s)
		if status := postBacklogIntake(s, newID(), testBacklogIntake); status != http.StatusCreated {
			t.Fatalf("intake=%d", status)
		}
		j := claimBacklogStep(t, s, "operator_message")
		completeBacklogStep(t, s, j, runtimeapi.Result{Actions: []runtimeapi.Action{
			{Type: "create_project", Project: "outside", Title: "Not authorized", Body: "Do work"},
		}})
		var count int
		if err := s.db.QueryRow(`SELECT count(*) FROM projects WHERE id='outside'`).Scan(&count); err != nil || count != 0 {
			t.Fatalf("cross-project intake applied=%d: %v", count, err)
		}
	})
	t.Run("goal changed before final scope decision", func(t *testing.T) {
		s, _, _, _ := continuationFixture(t)
		defer s.Close()
		setBacklogGoal(t, s)
		if status := postBacklogIntake(s, newID(), testBacklogIntake); status != http.StatusCreated {
			t.Fatalf("intake=%d", status)
		}
		delegate := runtimeapi.Result{Actions: []runtimeapi.Action{
			{Type: "delegate", Project: "alpha", Title: "Review a document", Body: "Inspect it."},
		}}
		completeBacklogStep(t, s, claimBacklogStep(t, s, "operator_message"), delegate)
		scope := claimBacklogStep(t, s, "scope_review")
		completeBacklogStep(t, s, scope, runtimeapi.Result{Text: "Ready for inspection."})
		decision := claimBacklogStep(t, s, "scope_decision")
		setBacklogGoal(t, s)
		completeBacklogStep(t, s, decision, delegate)
		var count int
		if err := s.db.QueryRow(`SELECT count(*) FROM jobs WHERE task_id=? AND kind='assignment'`, scope.Task).Scan(&count); err != nil || count != 0 {
			t.Fatalf("stale final scope applied=%d: %v", count, err)
		}
	})
}
