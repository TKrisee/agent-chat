package service

import (
	"database/sql"
)

// poHasPrivateOperatorInput fences all later turns on a task (including child
// work) once the operator has supplied a private task-scoped note to its PO.
func poHasPrivateOperatorInput(tx *sql.Tx, j job, po agent) (bool, error) {
	var count int
	err := tx.QueryRow(`SELECT count(*) FROM messages m WHERE m.project_id=? AND m.sender=? AND m.recipient=? AND m.role='operator' AND m.room_id=? AND (m.task_id=? OR m.task_id=(SELECT parent_task_id FROM po_work_items WHERE child_task_id=?))`, j.Project, operator, po.ID, directRoom(operator, po.ID), j.Task, j.Task).Scan(&count)
	if err != nil {
		return false, err
	}
	if count > 0 {
		return true, nil
	}
	// Also fence any private operator message explicitly included in the durable
	// provider request, even if it is not attached to this task.
	ids := make([]string, 0, len(j.Request.Context)+1)
	ids = append(ids, j.Source)
	for _, m := range j.Request.Context {
		ids = append(ids, m.ID)
	}
	seen := map[string]bool{}
	for _, id := range ids {
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		if err := tx.QueryRow(`SELECT count(*) FROM messages WHERE id=? AND sender=? AND recipient=? AND role='operator' AND room_id=?`, id, operator, po.ID, directRoom(operator, po.ID)).Scan(&count); err != nil {
			return false, err
		}
		if count > 0 {
			return true, nil
		}
	}
	return false, nil
}
