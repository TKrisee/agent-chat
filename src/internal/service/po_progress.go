package service

import (
	"database/sql"
	"fmt"
	"strings"

	"agentchat/src/internal/runtimeapi"
)

// reportPOProgress records a nonterminal note in the PO-to-Root room.
func (s *Service) reportPOProgress(tx *sql.Tx, j job, po agent, action runtimeapi.Action) error {
	if po.Role != "po" || j.Task == "" || (j.Kind != "assignment" && j.Kind != "review" && j.Kind != "po_continue" && j.Kind != "po_followup") || (action.Project != "" && action.Project != j.Project) || (action.Recipient != "" && action.Recipient != root) || action.Title != "" || action.Task != "" || action.ReplyTo != "" || strings.TrimSpace(action.Body) == "" {
		return fmt.Errorf("%w: progress note outside PO task scope", errWorkflow)
	}
	var valid int
	if err := tx.QueryRow(`SELECT count(*) FROM tasks t JOIN projects p ON p.id=t.project_id AND p.po_id=? JOIN agents a ON a.id=p.po_id AND a.role='po' AND a.project_id=p.id WHERE t.id=? AND t.project_id=? AND t.owner_id=? AND t.status IN ('assigned','review','accepted') AND p.stopped=0`, po.ID, j.Task, j.Project, po.ID).Scan(&valid); err != nil {
		return err
	}
	if valid != 1 {
		return fmt.Errorf("%w: progress task unavailable", errWorkflow)
	}
	body := action.Body
	id, err := outputMessage(tx, po, root, j, body, nil)
	if err != nil {
		return err
	}
	_, err = tx.Exec(`INSERT INTO po_progress_notes(message_id) VALUES(?)`, id)
	return err
}
