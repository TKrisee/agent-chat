package service

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"unicode/utf8"
)

var continuationID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// A continuation is allowed only where the original Root-to-PO assignment
// committed successfully and no worker effect or unresolved task job exists.
func poContinuation(tx *sql.Tx, task string) (project, po, assignment string, eligible bool, err error) {
	var stopped int
	err = tx.QueryRow(`SELECT p.id,p.po_id,p.stopped FROM tasks t JOIN projects p ON p.id=t.project_id JOIN agents a ON a.id=p.po_id AND a.project_id=p.id AND a.role='po' WHERE t.id=? AND t.status='assigned' AND t.owner_id=p.po_id`, task).Scan(&project, &po, &stopped)
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
		return
	}
	if err != nil || stopped != 0 {
		return
	}
	var assignments, active, approvals, workers, messages int
	err = tx.QueryRow(`SELECT
 (SELECT count(*) FROM jobs j JOIN messages m ON m.id=j.source_id WHERE j.task_id=? AND j.project_id=? AND j.agent_id=? AND j.kind='assignment' AND j.status='completed' AND COALESCE(j.diagnostic,'')='' AND m.task_id=j.task_id AND m.project_id=j.project_id AND m.sender=? AND m.recipient=? AND m.role='root' AND m.room_id=? AND length(trim(m.text))>0),
 (SELECT count(*) FROM jobs WHERE task_id=? AND status IN ('pending','submitted','running','uncertain')),
 (SELECT count(*) FROM approvals WHERE task_id=?),
 (SELECT count(*) FROM jobs WHERE task_id=? AND kind='work'),
 (SELECT count(*) FROM messages m JOIN agents a ON a.id=m.recipient AND a.role='worker' WHERE m.task_id=? AND m.sender=? AND m.project_id=?)`, task, project, po, root, po, directRoom(root, po), task, task, task, task, po, project).Scan(&assignments, &active, &approvals, &workers, &messages)
	if err != nil || assignments != 1 || active != 0 || approvals != 0 || workers != 0 || messages != 0 {
		return
	}
	err = tx.QueryRow(`SELECT m.id FROM jobs j JOIN messages m ON m.id=j.source_id WHERE j.task_id=? AND j.project_id=? AND j.agent_id=? AND j.kind='assignment' AND j.status='completed' AND COALESCE(j.diagnostic,'')='' AND m.task_id=j.task_id AND m.project_id=j.project_id AND m.sender=? AND m.recipient=? AND m.role='root' AND m.room_id=? AND length(trim(m.text))>0`, task, project, po, root, po, directRoom(root, po)).Scan(&assignment)
	if err != nil {
		return
	}
	eligible = true
	return
}
func (s *Service) continuePO(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	if err := r.ParseForm(); err != nil || len(r.PostForm) != 2 || len(r.PostForm["note"]) != 1 || len(r.PostForm["id"]) != 1 {
		http.Error(w, "A continuation note is required.", http.StatusBadRequest)
		return
	}
	note := strings.TrimSpace(r.PostForm.Get("note"))
	id := r.PostForm.Get("id")
	if note == "" || len(note) > 4096 || !utf8.ValidString(note) || !continuationID.MatchString(id) {
		http.Error(w, "Continuation note must be 1–4096 UTF-8 bytes.", http.StatusBadRequest)
		return
	}
	err := s.write(func(tx *sql.Tx) error {
		var sender, priorTask, priorText string
		e := tx.QueryRow(`SELECT sender,COALESCE(task_id,''),text FROM messages WHERE id=?`, id).Scan(&sender, &priorTask, &priorText)
		if e == nil {
			var linked int
			if err := tx.QueryRow(`SELECT count(*) FROM jobs WHERE source_id=? AND task_id=? AND kind='po_continue'`, id, priorTask).Scan(&linked); err != nil {
				return err
			}
			if sender == operator && priorTask == r.PathValue("id") && priorText == note && linked == 1 {
				return nil
			}
			return errConflict
		}
		if !errors.Is(e, sql.ErrNoRows) {
			return e
		}
		project, po, _, eligible, err := poContinuation(tx, r.PathValue("id"))
		if err != nil {
			return err
		}
		if !eligible {
			return fmt.Errorf("%w: PO continuation unavailable", errConflict)
		}
		a, err := getAgent(tx, po)
		if err != nil {
			return err
		}
		if err := addMessage(tx, id, operator, po, "operator", note, project, r.PathValue("id")); err != nil {
			return err
		}
		return s.enqueue(tx, a, project, r.PathValue("id"), id, "po_continue", "The operator asks you to continue the same existing assignment. Read the original final assignment and the operator's task-linked instruction. Promptly hand off actionable work or report a concrete blocker. Do not start a new scope review or redispatch uncertain work.")
	})
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if err != nil {
		if errors.Is(err, errConflict) {
			http.Error(w, "PO continuation is unavailable.", http.StatusConflict)
		} else {
			http.Error(w, "PO continuation failed.", http.StatusInternalServerError)
		}
		return
	}
	s.signal()
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte("PO continuation queued."))
}
