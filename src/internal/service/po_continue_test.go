package service

import (
	"agentchat/src/internal/runtimeapi"
	"database/sql"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func continuationFixture(t *testing.T) (*Service, string, string, string) {
	t.Helper()
	s := openTest(t, t.TempDir())
	po, worker, task, original := newID(), newID(), newID(), newID()
	err := s.write(func(tx *sql.Tx) error {
		if _, err := tx.Exec("INSERT INTO projects VALUES('alpha','Alpha',?,'/tmp',0)", po); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT INTO agents VALUES(?,?,'po','alpha','model','medium')", po, "PO"); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT INTO agents VALUES(?,?,'worker','alpha','model','medium')", worker, "Worker"); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT INTO tasks VALUES(?,'alpha','task','assigned',?)", task, po); err != nil {
			return err
		}
		if err := addMessage(tx, original, root, po, "root", "authoritative final assignment", "alpha", task); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO jobs(id,agent_id,project_id,task_id,source_id,kind,status,request,next_at) VALUES(?,?,'alpha',?,?,'assignment','completed',?,0)`, newID(), po, task, original, []byte("{}")); err != nil {
			return err
		}
		for i := 0; i < 30; i++ {
			if err := addMessage(tx, newID(), root, po, "root", "later", "alpha", task); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return s, task, po, worker
}
func postContinuationID(s *Service, task, note, id string) int {
	form := url.Values{"note": {note}, "id": {id}}
	r := httptest.NewRequest("POST", "/api/tasks/"+task+"/continue-po", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.SetPathValue("id", task)
	w := httptest.NewRecorder()
	s.continuePO(w, r)
	return w.Code
}
func postContinuation(s *Service, task, note string) int {
	return postContinuationID(s, task, note, newID())
}
func TestContinuePOSameTaskAndFences(t *testing.T) {
	t.Run("unrelated uncertain task remains untouched", func(t *testing.T) {
		s, task, po, worker := continuationFixture(t)
		defer s.Close()
		other, source := newID(), newID()
		if err := s.write(func(tx *sql.Tx) error {
			if _, err := tx.Exec("INSERT INTO tasks VALUES(?,'alpha','other','queued',?)", other, worker); err != nil {
				return err
			}
			if err := addMessage(tx, source, po, worker, "po", "earlier work", "alpha", other); err != nil {
				return err
			}
			_, err := tx.Exec(`INSERT INTO jobs(id,agent_id,project_id,task_id,source_id,kind,status,request,next_at) VALUES(?,?,'alpha',?,?,'work','uncertain',?,0)`, newID(), worker, other, source, []byte("{}"))
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if code := postContinuation(s, task, "continue"); code != 202 {
			t.Fatalf("unrelated uncertain blocked continuation: %d", code)
		}
		var status string
		if err := s.db.QueryRow("SELECT status FROM jobs WHERE task_id=?", other).Scan(&status); err != nil || status != "uncertain" {
			t.Fatalf("unrelated work changed: %q %v", status, err)
		}
	})
	t.Run("eligible and private context", func(t *testing.T) {
		s, task, po, _ := continuationFixture(t)
		defer s.Close()
		view, err := s.state()
		if err != nil || len(view.Tasks) != 1 || !view.Tasks[0].ContinuePOEligible {
			t.Fatalf("eligible %+v %v", view.Tasks, err)
		}
		requestID := newID()
		if status := postContinuationID(s, task, "Please hand this off now.", requestID); status != 202 {
			t.Fatalf("continue %d", status)
		}
		if status := postContinuation(s, task, "Please hand this off now."); status != 409 {
			t.Fatalf("duplicate %d", status)
		}
		j, err := s.claim(nil)
		if err != nil || j == nil || j.Kind != "po_continue" || j.Task != task || j.Agent != po {
			t.Fatalf("job %+v %v", j, err)
		}
		original, note := false, false
		for _, m := range j.Request.Context {
			if m.Text == "authoritative final assignment" && m.Sender == root {
				original = true
			}
			if m.Text == "Please hand this off now." && m.Sender == operator {
				note = true
			}
		}
		if !original || !note {
			t.Fatalf("missing pinned context original=%v note=%v", original, note)
		}
		if err := s.complete(*j, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "queue_worker", Project: "alpha", Body: "Begin the agreed work."}}}, nil, nil, nil); err != nil {
			t.Fatal(err)
		}
		if status := postContinuationID(s, task, "Please hand this off now.", requestID); status != 202 {
			t.Fatalf("replayed request %d", status)
		}
		var count int
		if err := s.db.QueryRow("SELECT count(*) FROM jobs WHERE task_id=? AND kind='po_continue'", task).Scan(&count); err != nil || count != 1 {
			t.Fatalf("duplicate job count %d: %v", count, err)
		}
		if err := s.db.QueryRow("SELECT count(*) FROM jobs WHERE task_id=? AND kind='work'", task).Scan(&count); err != nil || count != 1 {
			t.Fatalf("worker handoff count %d: %v", count, err)
		}
	})
	for _, fence := range []string{"stopped", "approval", "worker", "uncertain"} {
		t.Run(fence, func(t *testing.T) {
			s, task, po, worker := continuationFixture(t)
			defer s.Close()
			err := s.write(func(tx *sql.Tx) error {
				switch fence {
				case "stopped":
					_, err := tx.Exec("UPDATE projects SET stopped=1 WHERE id='alpha'")
					return err
				case "approval":
					_, err := tx.Exec(`INSERT INTO approvals VALUES(?,'alpha',?,?,'approval','body','pending')`, newID(), task, po)
					return err
				case "worker":
					id := newID()
					if err := addMessage(tx, id, po, worker, "po", "prior work", "alpha", task); err != nil {
						return err
					}
					_, err := tx.Exec(`INSERT INTO jobs(id,agent_id,project_id,task_id,source_id,kind,status,request,next_at) VALUES(?,?,'alpha',?,?,'work','completed',?,0)`, newID(), worker, task, id, []byte("{}"))
					return err
				default:
					id := newID()
					if err := addMessage(tx, id, po, root, "po", "unresolved", "alpha", task); err != nil {
						return err
					}
					_, err := tx.Exec(`INSERT INTO jobs(id,agent_id,project_id,task_id,source_id,kind,status,request,next_at) VALUES(?,?,'alpha',?,?,'po_continue','uncertain',?,0)`, newID(), po, task, id, []byte("{}"))
					return err
				}
			})
			if err != nil {
				t.Fatal(err)
			}
			if code := postContinuation(s, task, "continue"); code != 409 {
				t.Fatalf("%s accepted: %d", fence, code)
			}
		})
	}
}
