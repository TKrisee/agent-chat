package service

import "database/sql"

// Revisit durable child outcomes after a project becomes runnable or a job settles.
func (s *Service) reconcileProjectWakes(tx *sql.Tx, project string) error {
	if project == "" {
		return nil
	}
	type pending struct{ job, child string }
	items, err := querySlice(tx, `SELECT e.job_id,e.child_task_id FROM po_parent_outcome_events e JOIN tasks t ON t.id=e.parent_task_id WHERE t.project_id=? AND e.delivered_source_id IS NULL ORDER BY e.rowid`, func(rows *sql.Rows) (pending, error) {
		var p pending
		err := rows.Scan(&p.job, &p.child)
		return p, err
	}, project)
	if err != nil {
		return err
	}
	for _, item := range items {
		if err := s.maybeFollowParent(tx, job{ID: item.job, Task: item.child, Project: project}); err != nil {
			return err
		}
	}
	if err := s.queuePOEvidence(tx, project); err != nil {
		return err
	}
	if err := s.queueRootBacklog(tx, project); err != nil {
		return err
	}
	return s.maybeSuperviseLocalHold(tx, project)
}
