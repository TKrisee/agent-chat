package service

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"agentchat/src/internal/runtimeapi"
)

// setProjectGoal changes durable PO context only; it never creates a job.
func setProjectGoal(tx *sql.Tx, action runtimeapi.Action) error {
	if !projectID.MatchString(action.Project) || action.Title != "" || action.Recipient != "" {
		return fmt.Errorf("%w: goal requires an exact project and empty title and recipient", errWorkflow)
	}
	var po string
	if err := tx.QueryRow(`SELECT p.po_id FROM projects p JOIN agents a ON a.id=p.po_id AND a.project_id=p.id AND a.role='po' WHERE p.id=?`, action.Project).Scan(&po); errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: project has no appointed PO", errWorkflow)
	} else if err != nil {
		return err
	}
	switch action.Type {
	case "set_project_goal":
		if strings.TrimSpace(action.Body) == "" || len(action.Body) > 4096 {
			return fmt.Errorf("%w: goal objective must be 1 to 4096 bytes", errWorkflow)
		}
		_, err := tx.Exec(`INSERT INTO project_goals(project_id,objective,status) VALUES (?,?,'active') ON CONFLICT(project_id) DO UPDATE SET objective=excluded.objective,status='active'`, action.Project, action.Body)
		return err
	case "set_project_goal_status":
		if action.Body != "active" && action.Body != "paused" && action.Body != "complete" {
			return fmt.Errorf("%w: goal status must be active, paused, or complete", errWorkflow)
		}
		result, err := tx.Exec(`UPDATE project_goals SET status=? WHERE project_id=?`, action.Body, action.Project)
		if err != nil {
			return err
		}
		count, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if count != 1 {
			return fmt.Errorf("%w: project has no goal", errWorkflow)
		}
		return nil
	}
	return fmt.Errorf("%w: invalid goal action", errWorkflow)
}
