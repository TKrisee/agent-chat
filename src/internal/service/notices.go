package service

import (
	"database/sql"
	"fmt"
)

// workflowNotice commits only service-owned facts and never includes private task content.
func workflowNotice(tx *sql.Tx, project, key, text string) error {
	if project == "" {
		return nil
	}
	var exists int
	if err := tx.QueryRow("SELECT count(*) FROM workflow_notices WHERE event_key=?", key).Scan(&exists); err != nil {
		return err
	}
	if exists != 0 {
		return nil
	}
	id := newID()
	if err := addMessage(tx, id, "service", groupPrefix+project, "system", text, project, ""); err != nil {
		return err
	}
	_, err := tx.Exec("INSERT INTO workflow_notices(event_key,message_id,project_id) VALUES(?,?,?)", key, id, project)
	return err
}

const groupNoticeSender = `(m.sender='service' AND m.role='system' AND EXISTS (SELECT 1 FROM workflow_notices n WHERE n.message_id=m.id AND n.project_id=m.project_id))`

func (s *Service) workerStarted(j job) error {
	return s.write(func(tx *sql.Tx) error {
		var status string
		if err := tx.QueryRow("SELECT status FROM jobs WHERE id=?", j.ID).Scan(&status); err != nil {
			return err
		}
		if status != "submitted" && status != "running" {
			return nil
		}
		if _, err := tx.Exec("UPDATE jobs SET status='running' WHERE id=? AND status='submitted'", j.ID); err != nil {
			return err
		}
		return workflowNotice(tx, j.Project, "start:"+j.ID, "Worker run launched.")
	})
}

// ownerAssignmentStarted records only the exact appointed PO's live assignment launch.
func (s *Service) ownerAssignmentStarted(j job) error {
	return s.write(func(tx *sql.Tx) error {
		var status string
		err := tx.QueryRow(`SELECT j.status FROM jobs j JOIN projects p ON p.id=j.project_id JOIN agents a ON a.id=j.agent_id
   WHERE j.id=? AND j.kind='assignment' AND j.project_id=? AND j.agent_id=? AND j.task_id=? AND j.task_id IS NOT NULL AND p.po_id=j.agent_id AND a.role='po' AND a.project_id=p.id`, j.ID, j.Project, j.Agent, j.Task).Scan(&status)
		if err == sql.ErrNoRows {
			return nil
		}
		if err != nil {
			return err
		}
		if status != "submitted" && status != "running" {
			return nil
		}
		if _, err = tx.Exec("UPDATE jobs SET status='running' WHERE id=? AND status='submitted'", j.ID); err != nil {
			return err
		}
		return workflowNotice(tx, j.Project, "start:"+j.ID, "Project owner run launched locally; agent acknowledgment is pending.")
	})
}
func stopNotice(tx *sql.Tx, project string, unsettled int) error {
	text := "New dispatch is paused."
	if unsettled > 0 {
		if unsettled == 1 {
			text = "New dispatch is paused; 1 job is active or unresolved."
		} else {
			text = fmt.Sprintf("New dispatch is paused; %d jobs are active or unresolved.", unsettled)
		}
	}
	return workflowNotice(tx, project, "stop:"+newID(), text)
}
