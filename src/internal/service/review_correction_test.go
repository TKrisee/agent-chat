package service

import (
	"context"
	"database/sql"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"agentchat/src/internal/codexruntime"
	"agentchat/src/internal/runtimeapi"
)

const legacyConflictFixture = `{"text":"","actions":[{"type":"accept_result"},{"type":"hold_task"},{"type":"report"},{"type":"post_group"}],"attachments":[]}`

func reviewConflictFixture(t *testing.T) (*Service, job, string) {
	t.Helper()
	s, task, po, worker := continuationFixture(t)
	workSource, receipt, reviewID := newID(), newID(), newID()
	err := s.write(func(tx *sql.Tx) error {
		if _, err := tx.Exec("UPDATE tasks SET status='review' WHERE id=?", task); err != nil {
			return err
		}
		for _, text := range []string{"Earlier task context.", "Preserve earlier work.", "Use a fresh isolated workspace."} {
			note := newID()
			if err := addMessage(tx, note, operator, po, "operator", text, "alpha", task); err != nil {
				return err
			}
			if _, err := tx.Exec(`INSERT INTO jobs(id,agent_id,project_id,task_id,source_id,kind,status,request,next_at) VALUES(?,?,'alpha',?,?,'po_continue','completed',?,0)`, newID(), po, task, note, []byte("{}")); err != nil {
				return err
			}
		}
		if err := addMessage(tx, workSource, po, worker, "po", "work", "alpha", task); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO jobs(id,agent_id,project_id,task_id,source_id,kind,status,request,next_at) VALUES(?,?,'alpha',?,?,'work','completed',?,0)`, newID(), worker, task, workSource, []byte("{}")); err != nil {
			return err
		}
		if err := addMessage(tx, receipt, worker, po, "worker", "worker receipt", "alpha", task); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT INTO jobs(id,agent_id,project_id,task_id,source_id,kind,status,request,next_at) VALUES(?,?,'alpha',?,?,'review','submitted',?,0)`, reviewID, po, task, receipt, []byte("{}"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return s, job{ID: reviewID, Agent: po, Project: "alpha", Task: task, Source: receipt, Kind: "review", Request: runtimeapi.Request{Role: "po"}}, worker
}
func conflictingResult() runtimeapi.Result {
	return runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "accept_result", Project: "alpha"}, {Type: "hold_task", Project: "alpha", Body: "Needs decision."}, {Type: "report", Project: "alpha", Body: "Done."}, {Type: "post_group", Project: "alpha", Body: "Update."}}}
}
func TestReviewConflictAutoCorrectionAndLimit(t *testing.T) {
	s, j, _ := reviewConflictFixture(t)
	defer s.Close()
	for i := 0; i < 3; i++ {
		if err := s.complete(j, conflictingResult(), nil, nil, nil); err != nil {
			t.Fatal(err)
		}
		if i < 2 {
			j = followupClaim(t, s)
			if !strings.Contains(j.Request.Instructions, "hold_task cannot be combined") {
				t.Fatal("missing actionable reason")
			}
		}
	}
	var state string
	var n int
	if err := s.db.QueryRow("SELECT status FROM tasks WHERE id=?", j.Task).Scan(&state); err != nil || state != "blocked" {
		t.Fatalf("phase %s %v", state, err)
	}
	if err := s.db.QueryRow("SELECT count(*) FROM po_followups WHERE phase='review'").Scan(&n); err != nil || n != 2 {
		t.Fatalf("followups %d %v", n, err)
	}
	if err := s.db.QueryRow("SELECT count(*) FROM jobs WHERE task_id=? AND kind='work'", j.Task).Scan(&n); err != nil || n != 1 {
		t.Fatalf("worker replay %d %v", n, err)
	}
	if err := s.db.QueryRow("SELECT count(*) FROM messages WHERE recipient='group:alpha' AND text='Update.'").Scan(&n); err != nil || n != 0 {
		t.Fatalf("rejected group effect %d %v", n, err)
	}
}
func installReviewInspector(t *testing.T, s *Service) {
	t.Helper()
	jq, err := exec.LookPath("jq")
	if err != nil {
		t.Fatal("jq is required for legacy result inspection")
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "codex")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	r, err := codexruntime.New(codexruntime.Config{StateDir: dir, CodexPath: exe, JQPath: jq})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ConfigureRuntime(context.Background(), r); err != nil {
		t.Fatal(err)
	}
}
func markLegacyReview(t *testing.T, s *Service, j job, payload string) {
	t.Helper()
	if _, err := s.db.Exec("UPDATE jobs SET status='completed',diagnostic=?,result=? WHERE id=?", legacyReviewDiagnostic, []byte(payload), j.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("UPDATE tasks SET status='blocked' WHERE id=?", j.Task); err != nil {
		t.Fatal(err)
	}
}
func submitReviewContinue(s *Service, task, id, note string) int {
	form := url.Values{"note": {note}, "id": {id}}
	r := httptest.NewRequest("POST", "/api/tasks/"+task+"/continue-po", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.SetPathValue("id", task)
	w := httptest.NewRecorder()
	s.continuePO(w, r)
	return w.Code
}
func TestLegacyReviewConflictExplicitRecovery(t *testing.T) {
	s, j, _ := reviewConflictFixture(t)
	defer s.Close()
	installReviewInspector(t, s)
	markLegacyReview(t, s, j, legacyConflictFixture)
	if err := s.write(func(tx *sql.Tx) error {
		for i := 0; i < 30; i++ {
			if err := addMessage(tx, newID(), "service", j.Agent, "system", "later", "alpha", j.Task); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	view, err := s.state()
	if err != nil || !view.Tasks[0].ContinuePOEligible {
		t.Fatalf("legacy eligibility %+v %v", view.Tasks, err)
	}
	id := newID()
	if code := submitReviewContinue(s, j.Task, id, "Review the receipt; do not replay work."); code != 202 {
		t.Fatalf("continue %d", code)
	}
	if code := submitReviewContinue(s, j.Task, id, "Review the receipt; do not replay work."); code != 202 {
		t.Fatalf("idempotent %d", code)
	}
	if code := submitReviewContinue(s, j.Task, newID(), "another"); code != 409 {
		t.Fatalf("duplicate %d", code)
	}
	next := followupClaim(t, s)
	var note, receipt, earlier, isolation bool
	for _, m := range next.Request.Context {
		if m.Text == "Review the receipt; do not replay work." {
			note = true
		}
		if m.ID == j.Source && m.Text == "worker receipt" {
			receipt = true
		}
		if m.Text == "Preserve earlier work." && m.Sender == operator {
			earlier = true
		}
		if m.Text == "Use a fresh isolated workspace." && m.Sender == operator {
			isolation = true
		}
	}
	if !note || !receipt || !earlier || !isolation {
		t.Fatalf("missing pinned new note %v receipt %v earlier %v isolation %v", note, receipt, earlier, isolation)
	}
	if !strings.Contains(next.Request.Instructions, "follow the latest one") {
		t.Fatal("instruction precedence missing")
	}
	if err := s.write(func(tx *sql.Tx) error {
		a, err := getAgent(tx, root)
		if err != nil {
			return err
		}
		messages, err := s.runtimeContext(tx, a, j.Project, j.Task, "", "task_blocked")
		if err != nil {
			return err
		}
		for _, m := range messages {
			if m.Text == "Preserve earlier work." || m.Text == "Use a fresh isolated workspace." || m.Text == "Review the receipt; do not replay work." {
				t.Fatal("operator-to-PO note leaked into Root context")
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := s.db.QueryRow("SELECT count(*) FROM jobs WHERE task_id=? AND kind='work'", j.Task).Scan(&n); err != nil || n != 1 {
		t.Fatalf("worker replay %d %v", n, err)
	}
}
func TestLegacyReviewConflictFences(t *testing.T) {
	for _, fence := range []string{"wrong_shape", "stopped", "uncertain", "approval", "changed_receipt", "ambiguous_note"} {
		t.Run(fence, func(t *testing.T) {
			s, j, worker := reviewConflictFixture(t)
			defer s.Close()
			installReviewInspector(t, s)
			payload := legacyConflictFixture
			if fence == "wrong_shape" {
				payload = `{"actions":[{"type":"report"}]}`
			}
			markLegacyReview(t, s, j, payload)
			switch fence {
			case "stopped":
				_, _ = s.db.Exec("UPDATE projects SET stopped=1 WHERE id='alpha'")
			case "approval":
				_, _ = s.db.Exec(`INSERT INTO approvals VALUES(?,'alpha',?,?,'decision','body','pending')`, newID(), j.Task, j.Agent)
			case "uncertain":
				_, _ = s.db.Exec("UPDATE jobs SET status='uncertain' WHERE task_id=? AND kind='work'", j.Task)
			case "changed_receipt":
				if err := s.write(func(tx *sql.Tx) error {
					return addMessage(tx, newID(), worker, j.Agent, "worker", "later result", "alpha", j.Task)
				}); err != nil {
					t.Fatal(err)
				}
			case "ambiguous_note":
				if _, err := s.db.Exec(`UPDATE jobs SET source_id=(SELECT source_id FROM jobs WHERE task_id=? AND kind='assignment' LIMIT 1) WHERE id=(SELECT id FROM jobs WHERE task_id=? AND kind='po_continue' ORDER BY rowid DESC LIMIT 1)`, j.Task, j.Task); err != nil {
					t.Fatal(err)
				}
			}
			if code := submitReviewContinue(s, j.Task, newID(), "continue"); code != 409 {
				t.Fatalf("%s accepted: %d", fence, code)
			}
		})
	}
}

func TestReviewConflictAutoFencesAndRealHold(t *testing.T) {
	for _, fence := range []string{"stopped", "uncertain", "approval", "hold"} {
		t.Run(fence, func(t *testing.T) {
			s, j, _ := reviewConflictFixture(t)
			defer s.Close()
			result := conflictingResult()
			switch fence {
			case "stopped":
				if _, err := s.db.Exec("UPDATE projects SET stopped=1 WHERE id='alpha'"); err != nil {
					t.Fatal(err)
				}
			case "approval":
				if _, err := s.db.Exec(`INSERT INTO approvals VALUES(?,'alpha',?,?,'decision','body','pending')`, newID(), j.Task, j.Agent); err != nil {
					t.Fatal(err)
				}
			case "uncertain":
				if _, err := s.db.Exec("UPDATE jobs SET status='uncertain' WHERE task_id=? AND kind='work'", j.Task); err != nil {
					t.Fatal(err)
				}
			case "hold":
				result = runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "hold_task", Project: "alpha", Body: "A real prerequisite is unavailable."}}}
			}
			if err := s.complete(j, result, nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			var n int
			if err := s.db.QueryRow("SELECT count(*) FROM po_followups").Scan(&n); err != nil || n != 0 {
				t.Fatalf("%s auto correction: %d %v", fence, n, err)
			}
			if fence == "hold" {
				var state string
				if err := s.db.QueryRow("SELECT status FROM tasks WHERE id=?", j.Task).Scan(&state); err != nil || state != "blocked" {
					t.Fatalf("hold %s %v", state, err)
				}
			}
		})
	}
}
