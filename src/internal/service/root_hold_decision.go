package service

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"agentchat/src/internal/runtimeapi"
)

// requestRootHoldApproval binds one formal operator decision to the exact
// active Root-owned hold whose internal wake caused this Root turn.
func (s *Service) requestRootHoldApproval(tx *sql.Tx, j job, action runtimeapi.Action) error {
	if j.Agent != root || j.Kind != "task_blocked" || j.Request.LocalHoldSupervisor || j.Task == "" || j.Project == "" || (action.Project != "" && action.Project != j.Project) || (action.Recipient != "" && action.Recipient != operator) || strings.TrimSpace(action.Title) == "" || strings.TrimSpace(action.Body) == "" || action.Task != "" || action.ReplyTo != "" {
		return fmt.Errorf("%w: invalid Root-owned approval scope", errWorkflow)
	}
	h, found, err := s.activeTaskHold(tx, j.Task)
	if err != nil {
		return err
	}
	if !found || h.legacy || h.project != j.Project || h.rootSource != j.Source {
		return fmt.Errorf("%w: Root-owned hold unavailable", errWorkflow)
	}
	var marked, used int
	if err = tx.QueryRow(`SELECT (SELECT count(*) FROM root_owned_holds WHERE job_id=?),(SELECT count(*) FROM root_hold_approvals WHERE hold_job_id=?)`, h.job, h.job).Scan(&marked, &used); err != nil {
		return err
	}
	if marked != 1 || used != 0 {
		return fmt.Errorf("%w: Root-owned hold approval unavailable", errWorkflow)
	}
	allowed, err := s.holdCanResolveExcluding(tx, h, j.ID, false)
	if err != nil {
		return err
	}
	if !allowed {
		return fmt.Errorf("%w: held task is not safe for approval", errWorkflow)
	}
	id := newID()
	if _, err = tx.Exec(`INSERT INTO approvals VALUES(?,?,?,?,?,?,'pending')`, id, j.Project, j.Task, root, action.Title, action.Body); err != nil {
		return err
	}
	if _, err = tx.Exec(`INSERT INTO root_hold_approvals(approval_id,hold_job_id) VALUES(?,?)`, id, h.job); err != nil {
		return err
	}
	return deliverApproval(tx, id, j.Project, j.Task, action.Title, action.Body)
}

// A denial never changes the hold; approval consumes only the exact eligible hold.
func (s *Service) resolveRootHoldApproval(tx *sql.Tx, approvalID, decision, note string) (bool, error) {
	var hold, project, task, title, body string
	err := tx.QueryRow(`SELECT x.hold_job_id,a.project_id,a.task_id,a.title,a.body FROM root_hold_approvals x JOIN approvals a ON a.id=x.approval_id WHERE x.approval_id=? AND a.requested_by=?`, approvalID, root).Scan(&hold, &project, &task, &title, &body)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	if decision == "denied" {
		return true, nil
	}
	h, found, err := s.activeTaskHold(tx, task)
	if err != nil {
		return true, err
	}
	if !found || h.job != hold || h.project != project || h.legacy {
		return true, fmt.Errorf("%w: held task changed", errConflict)
	}
	allowed, err := s.holdCanResolveExcluding(tx, h, "", false)
	if err != nil {
		return true, err
	}
	if !allowed {
		return true, fmt.Errorf("%w: held task is not safe to resume", errConflict)
	}
	text := "Operator approved the following specific decision: " + title + "\n" + body
	if note != "" {
		text += "\nOperator decision note: " + note
	}
	return true, s.applyHoldResolution(tx, h, text, newID(), "")
}
