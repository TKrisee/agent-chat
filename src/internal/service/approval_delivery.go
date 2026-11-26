package service

import (
	"database/sql"
	"fmt"
)

// deliverApproval uses the explicitly operator-addressed approval fields only.
// The receipt and message are committed together with the approval.
func deliverApproval(tx *sql.Tx, id, project, task, title, body string) error {
	var existing string
	err := tx.QueryRow(`SELECT message_id FROM approval_deliveries WHERE approval_id=?`, id).Scan(&existing)
	if err == nil {
		return nil
	}
	if err != sql.ErrNoRows {
		return err
	}
	// No unlinked historic message proves which approval it represents.
	existing = newID()
	text := fmt.Sprintf("The project owner requests your decision: %s\n\n%s", title, body)
	if err := addMessage(tx, existing, root, operator, "root", text, project, task); err != nil {
		return err
	}
	_, err = tx.Exec(`INSERT INTO approval_deliveries(approval_id,message_id) VALUES(?,?)`, id, existing)
	return err
}

func deliverPendingApprovals(tx *sql.Tx) error {
	rows, err := querySlice(tx, `SELECT a.id,COALESCE(a.project_id,''),COALESCE(a.task_id,''),a.title,a.body FROM approvals a JOIN agents p ON p.id=a.requested_by AND p.role='po' WHERE a.status='pending' AND NOT EXISTS(SELECT 1 FROM approval_deliveries d WHERE d.approval_id=a.id)`, func(r *sql.Rows) ([]string, error) {
		var id, project, task, title, body string
		err := r.Scan(&id, &project, &task, &title, &body)
		return []string{id, project, task, title, body}, err
	})
	if err != nil {
		return err
	}
	for _, a := range rows {
		if err := deliverApproval(tx, a[0], a[1], a[2], a[3], a[4]); err != nil {
			return err
		}
	}
	return nil
}
