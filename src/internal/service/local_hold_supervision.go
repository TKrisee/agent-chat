package service

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"agentchat/src/internal/runtimeapi"
)

// One private supervisory turn per local hold, only when no other project work
// is runnable or active. A recovery task cannot recursively create a chain.
func (s *Service) maybeSuperviseLocalHold(tx *sql.Tx, project string) error {
	if project == "" {
		return nil
	}
	var busy int
	var enabled bool
	var reserve int
	if err := tx.QueryRow(`SELECT enabled,reserve FROM usage_settings WHERE id=1`).Scan(&enabled, &reserve); err != nil {
		return err
	}
	if enabled && s.usageBlocked(reserve) {
		return nil
	}
	err := tx.QueryRow(`SELECT count(*) FROM jobs j WHERE j.project_id=? AND (j.status IN ('submitted','running') OR (j.next_at<=? AND (`+pendingClaimPredicate+`)))`, project, now(), false).Scan(&busy)
	if err != nil || busy != 0 {
		return err
	}
	var hold, source, task, po string
	err = tx.QueryRow(`SELECT h.job_id,h.root_source_id,h.task_id,h.po_id FROM task_holds h JOIN task_local_holds l ON l.job_id=h.job_id JOIN task_local_supervision_candidates c ON c.hold_job_id=h.job_id JOIN tasks t ON t.id=h.task_id JOIN projects p ON p.id=h.project_id AND p.po_id=h.po_id JOIN agents a ON a.id=h.po_id AND a.role='po' AND a.project_id=p.id WHERE h.project_id=? AND h.status='active' AND t.status='blocked' AND t.owner_id=h.po_id AND p.stopped=0 AND NOT EXISTS(SELECT 1 FROM task_local_supervisions x WHERE x.hold_job_id=h.job_id OR x.recovery_task_id=h.task_id OR x.recovery_task_id=(SELECT parent_task_id FROM po_work_items WHERE child_task_id=h.task_id)) AND NOT EXISTS(SELECT 1 FROM jobs j WHERE j.task_id=h.task_id AND (j.status IN ('submitted','running','uncertain') OR (j.next_at<=? AND (`+pendingClaimPredicate+`)))) AND NOT EXISTS(SELECT 1 FROM approvals ap WHERE ap.task_id=h.task_id AND ap.status='pending') ORDER BY h.rowid LIMIT 1`, project, now(), false).Scan(&hold, &source, &task, &po)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if _, err = tx.Exec(`INSERT INTO task_local_supervisions(hold_job_id,source_id) VALUES(?,?)`, hold, source); err != nil {
		return err
	}
	if _, err = tx.Exec(`INSERT INTO task_internal_wakes(source_id) VALUES(?)`, source); err != nil {
		return err
	}
	manager, err := getAgent(tx, root)
	if err != nil {
		return err
	}
	return s.enqueue(tx, manager, project, task, source, "task_blocked", "The project has no runnable work and the PO recorded a local hold. Keep this hold and every production, qualification, authorization and uncertainty boundary intact. Review already-authorized scope and receipts. You may delegate at most one distinct independent task to the appointed PO for scope assessment, or take no action; never repeat prior work or invent an operator decision.")
}

func (s *Service) delegateLocalSupervision(tx *sql.Tx, a agent, j job, action runtimeapi.Action) error {
	if action.Type != "delegate" || action.Project != j.Project || action.Task != "" || strings.TrimSpace(action.Title) == "" || strings.TrimSpace(action.Body) == "" {
		return fmt.Errorf("%w: supervisor requires a distinct same-project delegation", errWorkflow)
	}
	var origin, po string
	err := tx.QueryRow(`SELECT origin.id,h.po_id FROM task_local_supervisions x JOIN task_holds h ON h.job_id=x.hold_job_id JOIN task_local_holds l ON l.job_id=h.job_id JOIN tasks t ON t.id=h.task_id AND t.status='blocked' AND t.owner_id=h.po_id JOIN projects p ON p.id=h.project_id AND p.po_id=h.po_id AND p.stopped=0 JOIN task_origins o ON o.task_id=COALESCE((SELECT parent_task_id FROM po_work_items WHERE child_task_id=h.task_id),h.task_id) JOIN messages origin ON origin.id=o.message_id AND origin.sender=? AND origin.recipient=? AND origin.role='operator' AND origin.room_id=? WHERE x.source_id=? AND x.recovery_task_id IS NULL AND h.status='active' AND h.task_id=? AND h.project_id=? AND NOT EXISTS(SELECT 1 FROM jobs j WHERE j.task_id=h.task_id AND j.id<>? AND (j.status IN ('submitted','running','uncertain') OR (j.next_at<=? AND (`+pendingClaimPredicate+`)))) AND NOT EXISTS(SELECT 1 FROM approvals ap WHERE ap.task_id=h.task_id AND ap.status='pending')`, operator, root, directRoom(operator, root), j.Source, j.Task, j.Project, j.ID, now(), false).Scan(&origin, &po)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: local hold supervision unavailable", errWorkflow)
	}
	if err != nil {
		return err
	}
	if action.Recipient != "" && action.Recipient != po {
		return fmt.Errorf("%w: delegation requires the appointed PO", errWorkflow)
	}
	var duplicate int
	if err = tx.QueryRow(`SELECT count(*) FROM tasks WHERE project_id=? AND lower(trim(title))=lower(trim(?))`, j.Project, action.Title).Scan(&duplicate); err != nil {
		return err
	}
	if duplicate != 0 {
		return fmt.Errorf("%w: task title already exists", errWorkflow)
	}
	delegation := j
	delegation.Source = origin
	if err = s.rootAssignment(tx, a, delegation, action); err != nil {
		return err
	}
	var task string
	if err = tx.QueryRow(`SELECT id FROM tasks WHERE project_id=? AND title=?`, j.Project, action.Title).Scan(&task); err != nil {
		return err
	}
	_, err = tx.Exec(`UPDATE task_local_supervisions SET recovery_task_id=? WHERE source_id=? AND recovery_task_id IS NULL`, task, j.Source)
	return err
}
