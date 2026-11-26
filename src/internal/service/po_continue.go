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
 (SELECT count(*) FROM jobs WHERE task_id=? AND status IN ('pending','submitted','running','uncertain') AND NOT EXISTS(SELECT 1 FROM scope_draft_confirmations c WHERE c.task_id=jobs.task_id AND c.review_job_id=jobs.id AND jobs.status='uncertain')),
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
			if err := tx.QueryRow(`SELECT count(*) FROM jobs j WHERE j.source_id=? AND j.task_id=? AND (j.kind='po_continue' OR (j.kind='po_followup' AND EXISTS (SELECT 1 FROM po_followups f WHERE f.source_id=j.source_id AND f.note_id=j.source_id)))`, id, priorTask).Scan(&linked); err != nil {
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
			approval, approvalOK, err := s.legacyMixedApprovalRecovery(tx, r.PathValue("id"))
			if err != nil {
				return err
			}
			if approvalOK {
				if err := addMessage(tx, id, operator, approval.job.Agent, "operator", note, approval.job.Project, approval.job.Task); err != nil {
					return err
				}
				res, err := tx.Exec(`UPDATE tasks SET status='assigned' WHERE id=? AND project_id=? AND owner_id=? AND status='blocked'`, approval.job.Task, approval.job.Project, approval.job.Agent)
				if err != nil {
					return err
				}
				n, err := res.RowsAffected()
				if err != nil {
					return err
				}
				if n != 1 {
					return errConflict
				}
				if _, err := tx.Exec(`INSERT INTO po_followups(source_id,trigger_id,ordinal,phase,assignment_id,note_id) VALUES(?,?,1,'assigned',?,?)`, id, approval.job.ID, approval.assignment, id); err != nil {
					return err
				}
				a, err := getAgent(tx, approval.job.Agent)
				if err != nil {
					return err
				}
				return s.enqueue(tx, a, approval.job.Project, approval.job.Task, id, "po_followup", "The prior proposal was rejected before any app actions applied because request_approval must be the only action in its response. Do not replay the rejected batch or assume any proposal applied. Reconsider the original assignment and operator note: dispatch independent authorized work in a separate turn, or request only the needed approval as a standalone action. Never dispatch denied work.")
			}
			report, reportOK, err := s.parentReportRecoveryEligible(tx, r.PathValue("id"))
			if err != nil {
				return err
			}
			if reportOK {
				if err := addMessage(tx, id, operator, report.po, "operator", note, report.project, r.PathValue("id")); err != nil {
					return err
				}
				res, err := tx.Exec(`UPDATE tasks SET status='assigned' WHERE id=? AND project_id=? AND owner_id=? AND status='blocked'`, r.PathValue("id"), report.project, report.po)
				if err != nil {
					return err
				}
				n, err := res.RowsAffected()
				if err != nil {
					return err
				}
				if n != 1 {
					return errConflict
				}
				if _, err := tx.Exec(`INSERT INTO po_report_recoveries(source_id,task_id,child_id,failed_job_id) VALUES(?,?,?,?)`, id, r.PathValue("id"), report.child, report.failed); err != nil {
					return err
				}
				if _, err := tx.Exec(`INSERT INTO po_followups(source_id,trigger_id,ordinal,phase,assignment_id,note_id) VALUES(?,?,1,'assigned',?,?)`, id, report.failed, report.assignment, id); err != nil {
					return err
				}
				a, err := getAgent(tx, report.po)
				if err != nil {
					return err
				}
				return s.enqueue(tx, a, report.project, r.PathValue("id"), id, "po_followup", "Return exactly one report action addressed to Root with the final report in its body; leave response text empty and include no attachments or other actions. Use only the original assignment, operator note and validated completed child report below. Do not dispatch or repeat work.\nValidated child report:\n"+report.report)
			}
			legacy, ok, err := s.legacyReviewRecovery(tx, r.PathValue("id"))
			if err != nil {
				return err
			}
			if !ok {
				planning, found, err := s.legacyTitledPlanning(tx, r.PathValue("id"))
				if err != nil {
					return err
				}
				if !found {
					return fmt.Errorf("%w: PO continuation unavailable", errConflict)
				}
				if err := addMessage(tx, id, operator, planning.job.Agent, "operator", note, planning.job.Project, planning.job.Task); err != nil {
					return err
				}
				res, err := tx.Exec(`UPDATE tasks SET status='assigned' WHERE id=? AND project_id=? AND owner_id=? AND status='blocked'`, planning.job.Task, planning.job.Project, planning.job.Agent)
				if err != nil {
					return err
				}
				n, err := res.RowsAffected()
				if err != nil {
					return err
				}
				if n != 1 {
					return errConflict
				}
				if _, err = tx.Exec(`INSERT INTO po_followups(source_id,trigger_id,ordinal,phase,assignment_id,note_id) VALUES(?,?,1,'assigned',?,?)`, id, planning.job.ID, planning.assignment, id); err != nil {
					return err
				}
				a, err := getAgent(tx, planning.job.Agent)
				if err != nil {
					return err
				}
				return s.enqueue(tx, a, planning.job.Project, planning.job.Task, id, "po_followup", "Continue the same assignment using the pinned original brief and operator instruction. The prior planning proposal was rejected before its actions were applied; plan the next safe steps without assuming they committed or replaying effects. Preserve all stops and scope constraints.")
			}
			a, err := getAgent(tx, legacy.job.Agent)
			if err != nil {
				return err
			}
			if err := addMessage(tx, id, operator, a.ID, "operator", note, legacy.job.Project, legacy.job.Task); err != nil {
				return err
			}
			res, err := tx.Exec(`UPDATE tasks SET status='review' WHERE id=? AND status='blocked' AND project_id=? AND owner_id=?`, legacy.job.Task, legacy.job.Project, a.ID)
			if err != nil {
				return err
			}
			n, err := res.RowsAffected()
			if err != nil {
				return err
			}
			if n != 1 {
				return errConflict
			}
			if _, err := tx.Exec(`INSERT INTO po_followups(source_id,trigger_id,ordinal,phase,assignment_id,note_id,review_id) VALUES(?,?,1,'review',?,?,?)`, id, legacy.job.ID, legacy.assignment, id, legacy.job.Source); err != nil {
				return err
			}
			if legacy.blockedParent != "" {
				if _, err := tx.Exec(`INSERT INTO po_child_receipt_recoveries(source_id,task_id,review_job_id,parent_task_id) VALUES(?,?,?,?)`, id, legacy.job.Task, legacy.job.ID, legacy.blockedParent); err != nil {
					return err
				}
			}
			instruction := "The operator explicitly asks you to continue the same review. The prior proposal was rejected: hold_task cannot be combined with accept_result, report, queue_worker, or request_approval. Read the pinned original assignment, worker receipt, and task-scoped operator instructions; when instructions differ, follow the latest one within existing safety and authorization boundaries. Choose either a hold or a valid review action; do not replay or assume completion of prior work."
			if legacy.progressConflict {
				instruction = "The operator explicitly asks you to continue the same review. The prior completion and progress proposal was rejected before app actions applied. Review the pinned original assignment, worker receipt and operator instructions; do not replay the saved batch, rerun the worker, or assume any proposal applied. Complete only the authorized review using the existing safeguards."
			}
			if legacy.childReport && !legacy.progressConflict {
				instruction = "The operator explicitly asks you to continue the same child review. The prior review proposal was rejected before any action applied. Review the pinned original assignment and exact worker receipt, then make a fresh decision; do not replay worker work or assume earlier actions committed. Preserve production stops and all task-scoped constraints."
			}
			if legacy.blockedParent != "" {
				instruction = "Review only the pinned existing worker receipt against the original assignment. Return accept_result followed by report, optionally report_progress; no other actions or attachments. Do not dispatch, rerun work, replay prior actions or change the parent approval. Respect production stops and all task-scoped constraints."
			}
			return s.enqueue(tx, a, legacy.job.Project, legacy.job.Task, id, "po_followup", instruction)
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
