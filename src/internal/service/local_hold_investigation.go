package service

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"agentchat/src/internal/runtimeapi"
)

// investigateLocalHold schedules one assessment of the existing held task;
// it never resolves the hold or creates another assignment.
func (s *Service) investigateLocalHold(tx *sql.Tx, a agent, j job, action runtimeapi.Action) error {
	if a.ID != root || !j.Request.LocalHoldSupervisor || j.Kind != "task_blocked" || action.Type != "investigate_hold" || action.Project != j.Project || action.Task != j.Task || action.Title != "" || action.Recipient != "" || action.ReplyTo != "" || len(action.DependsOn) != 0 || strings.TrimSpace(action.Body) == "" || len(action.Body) > 4096 {
		return fmt.Errorf("%w: invalid local hold investigation", errWorkflow)
	}
	var hold, po string
	err := tx.QueryRow(`SELECT h.job_id,h.po_id FROM task_local_supervisions x JOIN task_holds h ON h.job_id=x.hold_job_id JOIN task_local_holds l ON l.job_id=h.job_id JOIN tasks t ON t.id=h.task_id AND t.status='blocked' AND t.owner_id=h.po_id JOIN projects p ON p.id=h.project_id AND p.po_id=h.po_id AND p.stopped=0 JOIN agents a ON a.id=h.po_id AND a.role='po' AND a.project_id=h.project_id WHERE x.source_id=? AND x.recovery_task_id IS NULL AND h.status='active' AND h.task_id=? AND h.project_id=? AND NOT EXISTS(SELECT 1 FROM local_hold_investigations i WHERE i.hold_job_id=h.job_id) AND NOT EXISTS(SELECT 1 FROM jobs other WHERE other.task_id=h.task_id AND other.id<>? AND other.status IN ('pending','submitted','running','uncertain')) AND NOT EXISTS(SELECT 1 FROM approvals ap WHERE ap.task_id=h.task_id AND ap.status='pending')`, j.Source, j.Task, j.Project, j.ID).Scan(&hold, &po)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: local hold investigation unavailable", errWorkflow)
	}
	if err != nil {
		return err
	}
	source := newID()
	if err = addMessage(tx, source, root, po, "root", action.Body, j.Project, j.Task); err != nil {
		return err
	}
	if _, err = tx.Exec(`INSERT INTO local_hold_investigations(source_id,hold_job_id,supervisor_source_id) VALUES(?,?,?)`, source, hold, j.Source); err != nil {
		return err
	}
	owner, err := getAgent(tx, po)
	if err != nil {
		return err
	}
	return s.enqueue(tx, owner, j.Project, j.Task, source, "po_hold_investigation", "Assess only the existing held task and pinned evidence; report a substantive private assessment. No task actions, hold changes, replay or new authority. Preserve every stop, approval and uncertain-effect boundary.")
}

func (s *Service) consumeLocalHoldInvestigation(tx *sql.Tx, a agent, j job, result runtimeapi.Result, files []attachmentData) error {
	if a.Role != "po" || len(result.Actions) != 0 || len(result.Attachments) != 0 || len(files) != 0 || strings.TrimSpace(result.Text) == "" {
		return fmt.Errorf("%w: investigation requires text only", errWorkflow)
	}
	var n int
	if err := tx.QueryRow(`SELECT count(*) FROM local_hold_investigations i JOIN task_holds h ON h.job_id=i.hold_job_id JOIN tasks t ON t.id=h.task_id AND t.status='blocked' AND t.owner_id=h.po_id JOIN projects p ON p.id=h.project_id AND p.po_id=h.po_id WHERE i.source_id=? AND i.assessment_id IS NULL AND h.status='active' AND h.task_id=? AND h.project_id=? AND h.po_id=?`, j.Source, j.Task, j.Project, a.ID).Scan(&n); err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("%w: held investigation unavailable", errWorkflow)
	}
	id := newID()
	if err := addMessage(tx, id, a.ID, a.ID, "po", result.Text, j.Project, j.Task); err != nil {
		return err
	}
	_, err := tx.Exec(`UPDATE local_hold_investigations SET assessment_id=? WHERE source_id=? AND assessment_id IS NULL`, id, j.Source)
	return err
}
