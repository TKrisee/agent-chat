package service

import (
	"database/sql"
	"errors"
	"net/http"
	"strings"
)

const scopeReviewRoleDiagnostic = "Native output contained an action outside the recorded role; inspect the result before any new run."

// confirmScopeDraft is an explicit operator choice of the existing Root draft;
// it never reuses the rejected output or changes the uncertain review receipt.
func (s *Service) confirmScopeDraft(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	if err := r.ParseForm(); err != nil || len(r.PostForm) != 4 || len(r.PostForm["id"]) != 1 || len(r.PostForm["review_job_id"]) != 1 || len(r.PostForm["draft_message_id"]) != 1 || len(r.PostForm["confirm"]) != 1 {
		http.Error(w, "Explicit draft confirmation is required.", http.StatusBadRequest)
		return
	}
	id, review, draft, confirm := r.PostForm.Get("id"), r.PostForm.Get("review_job_id"), r.PostForm.Get("draft_message_id"), r.PostForm.Get("confirm")
	if !continuationID.MatchString(id) || !clientID.MatchString(review) || !clientID.MatchString(draft) || confirm != "effects-unknown" {
		http.Error(w, "Explicit draft confirmation is required.", http.StatusBadRequest)
		return
	}
	task := r.PathValue("id")
	err := s.write(func(tx *sql.Tx) error {
		var oldTask, oldReview, oldDraft string
		e := tx.QueryRow(`SELECT task_id,review_job_id,draft_message_id FROM scope_draft_confirmations WHERE request_id=?`, id).Scan(&oldTask, &oldReview, &oldDraft)
		if e == nil {
			if oldTask == task && oldReview == review && oldDraft == draft {
				return nil
			}
			return errConflict
		}
		if !errors.Is(e, sql.ErrNoRows) {
			return e
		}
		var project, po, draftText string
		e = tx.QueryRow(`SELECT p.id,p.po_id,m.text FROM tasks t JOIN projects p ON p.id=t.project_id AND p.po_id=t.owner_id AND p.stopped=0 JOIN agents a ON a.id=p.po_id AND a.role='po' AND a.project_id=p.id JOIN jobs j ON j.id=? AND j.task_id=t.id AND j.project_id=p.id AND j.agent_id=p.po_id AND j.kind='scope_review' AND j.status='uncertain' AND j.diagnostic=? AND j.source_id=? JOIN messages m ON m.id=j.source_id AND m.task_id=t.id AND m.project_id=p.id AND m.sender=? AND m.recipient=p.po_id AND m.role='root' AND m.room_id='dm:'||CASE WHEN m.sender<m.recipient THEN m.sender||':'||m.recipient ELSE m.recipient||':'||m.sender END JOIN task_origins o ON o.task_id=t.id JOIN messages origin ON origin.id=o.message_id AND origin.sender=? AND origin.recipient=? AND origin.room_id=? WHERE t.id=? AND t.status='blocked'`, review, scopeReviewRoleDiagnostic, draft, root, operator, root, directRoom(operator, root), task).Scan(&project, &po, &draftText)
		if errors.Is(e, sql.ErrNoRows) {
			return errConflict
		}
		if e != nil {
			return e
		}
		if strings.TrimSpace(draftText) == "" {
			return errConflict
		}
		var conflicts int
		e = tx.QueryRow(`SELECT (SELECT count(*) FROM jobs WHERE task_id=? AND id<>? AND (status IN ('pending','submitted','running','uncertain') OR kind IN ('scope_review','scope_decision','assignment','work')))+(SELECT count(*) FROM approvals WHERE task_id=?)+(SELECT count(*) FROM po_work_items WHERE parent_task_id=? OR child_task_id=?)+(SELECT count(*) FROM task_holds WHERE task_id=?)+(SELECT count(*) FROM scope_draft_confirmations WHERE task_id=? OR review_job_id=?)+(SELECT count(*) FROM messages m JOIN agents a ON a.id=m.recipient AND a.role='worker' WHERE m.task_id=? AND m.sender=?)`, task, review, task, task, task, task, task, review, task, po).Scan(&conflicts)
		if e != nil {
			return e
		}
		if conflicts != 0 {
			return errConflict
		}
		res, e := tx.Exec(`UPDATE tasks SET status='assigned' WHERE id=? AND project_id=? AND owner_id=? AND status='blocked'`, task, project, po)
		if e != nil {
			return e
		}
		n, e := res.RowsAffected()
		if e != nil {
			return e
		}
		if n != 1 {
			return errConflict
		}
		a, e := getAgent(tx, po)
		if e != nil {
			return e
		}
		instruction := "This is the final operator-confirmed handoff of the original Root draft referenced by the task. The operator explicitly acknowledges that effects of the previous scope review remain unknown. Its preliminary-assessment and after-handoff wording is superseded: coordinate the authorized work now. Preserve the original project scope, production stops, signed inputs, and all safety constraints. Do not use rejected output, replay the previous review, or assume its external effects were absent."
		if e = s.enqueue(tx, a, project, task, draft, "assignment", instruction); e != nil {
			return e
		}
		var assignment string
		if e = tx.QueryRow(`SELECT id FROM jobs WHERE task_id=? AND agent_id=? AND source_id=? AND kind='assignment'`, task, po, draft).Scan(&assignment); e != nil {
			return e
		}
		_, e = tx.Exec(`INSERT INTO scope_draft_confirmations(request_id,task_id,review_job_id,draft_message_id,assignment_job_id,confirmed_by,effects_unknown_ack,created_at) VALUES(?,?,?,?,?,'operator',1,?)`, id, task, review, draft, assignment, now())
		return e
	})
	w.Header().Set("Cache-Control", "no-store")
	if err != nil {
		if errors.Is(err, errConflict) {
			http.Error(w, "Original draft handoff is unavailable.", http.StatusConflict)
		} else {
			http.Error(w, "Original draft handoff failed.", http.StatusInternalServerError)
		}
		return
	}
	s.signal()
	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte("Original draft handoff queued; previous effects remain unknown."))
}
