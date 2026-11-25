package service

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

type scopeFieldError struct{ field string }

func (e *scopeFieldError) Error() string {
	return errWorkflow.Error() + ": final assignment " + e.field
}
func (e *scopeFieldError) Unwrap() error { return errWorkflow }

// The first decision was enqueued in the same transaction that committed the
// PO assessment. Its source is the durable link; later correction sources are
// service messages and must never replace that link.
func scopeAssessmentID(tx *sql.Tx, task, project, po string) (string, error) {
	var id string
	err := tx.QueryRow(`SELECT m.id FROM jobs d
		JOIN messages m ON m.id=d.source_id AND m.task_id=d.task_id AND m.project_id=d.project_id
		JOIN jobs r ON r.task_id=d.task_id AND r.project_id=d.project_id AND r.agent_id=? AND r.kind='scope_review' AND r.status='completed' AND r.rowid<d.rowid
		JOIN messages draft ON draft.id=r.source_id AND draft.task_id=d.task_id AND draft.project_id=d.project_id
		WHERE d.id=(SELECT id FROM jobs WHERE task_id=? AND kind='scope_decision' ORDER BY rowid LIMIT 1)
		AND d.project_id=? AND d.agent_id=? AND m.sender=? AND m.recipient=? AND m.role='po' AND m.room_id=? AND length(trim(m.text))>0
		AND draft.sender=? AND draft.recipient=? AND draft.room_id=?`, po, task, project, root, po, root, directRoom(po, root), root, po, directRoom(root, po)).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return id, err
}

// Eligibility is rechecked in the same write transaction as a retry. Only the
// narrowly recognized validation rejection is repairable; no provider result is replayed.
func handoffEligible(tx *sql.Tx, task, phase, current string, legacy bool) (bool, error) {
	var project, po string
	var stopped int
	err := tx.QueryRow(`SELECT p.id,p.po_id,p.stopped FROM tasks t JOIN projects p ON p.id=t.project_id JOIN agents a ON a.id=p.po_id AND a.role='po' AND a.project_id=p.id WHERE t.id=? AND t.status=? AND t.owner_id=p.po_id`, task, phase).Scan(&project, &po, &stopped)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if stopped != 0 {
		return false, nil
	}
	assessment, err := scopeAssessmentID(tx, task, project, po)
	if err != nil {
		return false, err
	}
	if assessment == "" {
		return false, nil
	}
	var latest, status, diagnostic string
	err = tx.QueryRow(`SELECT id,status,COALESCE(diagnostic,'') FROM jobs WHERE task_id=? AND project_id=? AND agent_id=? AND kind='scope_decision' ORDER BY rowid DESC LIMIT 1`, task, project, root).Scan(&latest, &status, &diagnostic)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if current != "" {
		if latest != current || (status != "running" && status != "submitted") {
			return false, nil
		}
	} else {
		if status != "completed" || (diagnostic != "invalid runtime result: final assignment requires the same project, a title and a body" && !strings.HasPrefix(diagnostic, "invalid runtime result: final assignment ")) {
			return false, nil
		}
		if legacy && diagnostic != "invalid runtime result: final assignment requires the same project, a title and a body" {
			return false, nil
		}
	}
	var decisions, review, other, approvals, origins int
	err = tx.QueryRow(`SELECT
 (SELECT count(*) FROM jobs WHERE task_id=? AND kind='scope_decision'),
 (SELECT count(*) FROM jobs WHERE task_id=? AND project_id=? AND agent_id=? AND kind='scope_review' AND status='completed'),
 (SELECT count(*) FROM jobs WHERE task_id=? AND ((kind NOT IN ('scope_review','scope_decision')) OR (id<>? AND status IN ('pending','submitted','running','uncertain')))),
 (SELECT count(*) FROM approvals WHERE task_id=?),
 (SELECT count(*) FROM task_origins o JOIN messages m ON m.id=o.message_id WHERE o.task_id=? AND m.sender=? AND m.recipient=? AND m.room_id=?)`, task, task, project, po, task, latest, task, task, operator, root, directRoom(operator, root)).Scan(&decisions, &review, &other, &approvals, &origins)
	if err != nil {
		return false, err
	}
	return decisions < 3 && review == 1 && other == 0 && approvals == 0 && origins == 1, nil
}
func (s *Service) queueScopeCorrection(tx *sql.Tx, task, project, reason string) error {
	id := newID()
	if err := addMessage(tx, id, "service", root, "system", reason, project, task); err != nil {
		return err
	}
	a, err := getAgent(tx, root)
	if err != nil {
		return err
	}
	return s.enqueue(tx, a, project, task, id, "scope_decision", "The previous final assignment was rejected without applying it: "+reason+". Correct the stated field using the existing original request and PO assessment; do not start another scope round.")
}
func (s *Service) retryHandoff(w http.ResponseWriter, r *http.Request) {
	err := s.write(func(tx *sql.Tx) error {
		task := r.PathValue("id")
		eligible, err := handoffEligible(tx, task, "blocked", "", true)
		if err != nil {
			return err
		}
		if !eligible {
			return fmt.Errorf("%w: handoff retry is unavailable", errConflict)
		}
		var project, reason string
		if err := tx.QueryRow(`SELECT t.project_id,j.diagnostic FROM tasks t JOIN jobs j ON j.task_id=t.id AND j.kind='scope_decision' WHERE t.id=? ORDER BY j.rowid DESC LIMIT 1`, task).Scan(&project, &reason); err != nil {
			return err
		}
		result, err := tx.Exec(`UPDATE tasks SET status='assigned' WHERE id=? AND status='blocked'`, task)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if n != 1 {
			return errConflict
		}
		return s.queueScopeCorrection(tx, task, project, reason)
	})
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if err != nil {
		if errors.Is(err, errConflict) {
			http.Error(w, "Handoff retry is unavailable.", http.StatusConflict)
		} else {
			http.Error(w, "Handoff retry failed.", http.StatusInternalServerError)
		}
		return
	}
	s.signal()
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte("Handoff correction queued."))
}
