package service

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"

	"agentchat/src/internal/runtimeapi"
)

const legacyTitledPlanningDiagnostic = "invalid runtime result: group post outside project scope"

type legacyTitledPlan struct {
	job        job
	assignment string
}

// legacyTitledPlanning permits only the known pre-apply titled-post rejection.
func (s *Service) legacyTitledPlanning(tx *sql.Tx, task string) (out legacyTitledPlan, eligible bool, err error) {
	var po, project string
	var stopped int
	err = tx.QueryRow(`SELECT p.id,p.po_id,p.stopped FROM tasks t JOIN projects p ON p.id=t.project_id JOIN agents a ON a.id=p.po_id AND a.role='po' AND a.project_id=p.id WHERE t.id=? AND t.status='blocked' AND t.owner_id=p.po_id`, task).Scan(&project, &po, &stopped)
	if errors.Is(err, sql.ErrNoRows) {
		return out, false, nil
	}
	if err != nil || stopped != 0 {
		return out, false, err
	}
	var status, diagnostic string
	var payload []byte
	err = tx.QueryRow(`SELECT id,agent_id,source_id,kind,status,COALESCE(diagnostic,''),result FROM jobs WHERE task_id=? ORDER BY rowid DESC LIMIT 1`, task).Scan(&out.job.ID, &out.job.Agent, &out.job.Source, &out.job.Kind, &status, &diagnostic, &payload)
	if errors.Is(err, sql.ErrNoRows) {
		return out, false, nil
	}
	if err != nil {
		return out, false, err
	}
	if out.job.Agent != po || (out.job.Kind != "assignment" && out.job.Kind != "po_followup") || status != "completed" || diagnostic != legacyTitledPlanningDiagnostic || len(payload) == 0 || len(payload) > 1<<20 {
		return out, false, nil
	}
	out.job.Project = project
	out.job.Task = task
	if out.job.Kind == "po_followup" {
		f, e := readPOFollowup(tx, out.job.Source)
		if e != nil {
			if errors.Is(e, sql.ErrNoRows) {
				return out, false, nil
			}
			return out, false, e
		}
		if f.phase != "assigned" {
			return out, false, nil
		}
	}
	var other int
	err = tx.QueryRow(`SELECT (SELECT count(*) FROM jobs WHERE task_id=? AND status IN ('pending','submitted','running','uncertain'))+(SELECT count(*) FROM approvals WHERE task_id=?)+(SELECT count(*) FROM po_work_items WHERE parent_task_id=?)+(SELECT count(*) FROM po_followups WHERE trigger_id=?)`, task, task, task, out.job.ID).Scan(&other)
	if err != nil || other != 0 {
		return out, false, err
	}
	if out.job.Kind == "assignment" {
		var valid int
		err = tx.QueryRow(`SELECT count(*) FROM messages WHERE id=? AND task_id=? AND project_id=? AND sender=? AND recipient=? AND role='root' AND room_id=? AND length(trim(text))>0`, out.job.Source, task, project, root, po, directRoom(root, po)).Scan(&valid)
		if err != nil {
			return out, false, err
		}
		if valid != 1 {
			return out, false, nil
		}
		out.assignment = out.job.Source
	} else {
		out.assignment, err = poAssignmentSource(tx, out.job, po)
		if errors.Is(err, errWorkflow) {
			return out, false, nil
		}
		if err != nil {
			return out, false, err
		}
	}
	inspector, ok := s.runtimeSetup().(runtimeapi.LegacyTitledPlanningInspector)
	if !ok {
		return out, false, nil
	}
	key := fmt.Sprintf("titled:%s:%x", out.job.ID, sha256.Sum256(payload))
	s.reviewProofMu.Lock()
	proven := s.reviewProof[key]
	s.reviewProofMu.Unlock()
	if !proven {
		proven = inspector.IsLegacyTitledPlanning(context.Background(), payload, project)
		if proven {
			s.reviewProofMu.Lock()
			if s.reviewProof == nil {
				s.reviewProof = make(map[string]bool)
			}
			s.reviewProof[key] = true
			s.reviewProofMu.Unlock()
		}
	}
	return out, proven, nil
}
