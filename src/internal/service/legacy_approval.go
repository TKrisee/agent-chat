package service

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"

	"agentchat/src/internal/runtimeapi"
)

const legacyApprovalDiagnostic = "invalid runtime result: operator approval must be requested separately and a denial cannot dispatch work"

// legacyApprovalAssignment proves the exact rejected Root-to-PO assignment;
// it never applies or interprets the rejected actions as completed work.
func (s *Service) legacyApprovalAssignment(tx *sql.Tx, task, project, po string) (assignment string, ok bool, err error) {
	var count int
	var payload []byte
	err = tx.QueryRow(`SELECT count(*),COALESCE(min(m.id),''),min(j.result) FROM jobs j JOIN messages m ON m.id=j.source_id WHERE j.task_id=? AND j.project_id=? AND j.agent_id=? AND j.kind='assignment' AND j.status='completed' AND j.diagnostic=? AND m.task_id=j.task_id AND m.project_id=j.project_id AND m.sender=? AND m.recipient=? AND m.role='root' AND m.room_id=? AND length(trim(m.text))>0`, task, project, po, legacyApprovalDiagnostic, root, po, directRoom(root, po)).Scan(&count, &assignment, &payload)
	if err != nil || count != 1 {
		return "", false, err
	}
	var all int
	if err = tx.QueryRow(`SELECT count(*) FROM jobs WHERE task_id=? AND kind='assignment'`, task).Scan(&all); err != nil || all != 1 {
		return "", false, err
	}
	inspector, ok := s.runtimeSetup().(runtimeapi.LegacyMixedApprovalInspector)
	if !ok {
		return "", false, nil
	}
	key := fmt.Sprintf("mixed-approval:%s:%x", assignment, sha256.Sum256(payload))
	s.reviewProofMu.Lock()
	proven := s.reviewProof[key]
	s.reviewProofMu.Unlock()
	if !proven {
		proven = inspector.IsLegacyMixedApproval(context.Background(), payload, project)
		if proven {
			s.reviewProofMu.Lock()
			if s.reviewProof == nil {
				s.reviewProof = make(map[string]bool)
			}
			s.reviewProof[key] = true
			s.reviewProofMu.Unlock()
		}
	}
	if !proven {
		return "", false, nil
	}
	return assignment, true, nil
}

type legacyApprovalPlan struct {
	job        job
	assignment string
}

func (s *Service) legacyMixedApprovalRecovery(tx *sql.Tx, task string) (out legacyApprovalPlan, ok bool, err error) {
	var stopped int
	err = tx.QueryRow(`SELECT p.id,p.po_id,p.stopped FROM tasks t JOIN projects p ON p.id=t.project_id JOIN agents a ON a.id=p.po_id AND a.role='po' AND a.project_id=p.id WHERE t.id=? AND t.status='blocked' AND t.owner_id=p.po_id`, task).Scan(&out.job.Project, &out.job.Agent, &stopped)
	if errors.Is(err, sql.ErrNoRows) {
		return out, false, nil
	}
	if err != nil || stopped != 0 {
		return out, false, err
	}
	out.job.Task = task
	var kind, status, diagnostic string
	err = tx.QueryRow(`SELECT id,source_id,kind,status,COALESCE(diagnostic,'') FROM jobs WHERE task_id=? ORDER BY rowid DESC LIMIT 1`, task).Scan(&out.job.ID, &out.job.Source, &kind, &status, &diagnostic)
	if errors.Is(err, sql.ErrNoRows) {
		return out, false, nil
	}
	if err != nil {
		return out, false, err
	}
	if kind != "assignment" || status != "completed" || diagnostic != legacyApprovalDiagnostic {
		return out, false, nil
	}
	out.assignment, ok, err = s.legacyApprovalAssignment(tx, task, out.job.Project, out.job.Agent)
	if err != nil || !ok || out.assignment != out.job.Source {
		return out, false, err
	}
	var n int
	err = tx.QueryRow(`SELECT (SELECT count(*) FROM jobs WHERE task_id=? AND status IN ('pending','submitted','running','uncertain'))+(SELECT count(*) FROM jobs WHERE task_id=? AND kind='work')+(SELECT count(*) FROM approvals WHERE task_id=?)+(SELECT count(*) FROM po_work_items WHERE parent_task_id=? OR child_task_id=?)+(SELECT count(*) FROM po_followups WHERE trigger_id=?)+(SELECT count(*) FROM messages m JOIN agents a ON a.id=m.recipient AND a.role='worker' WHERE m.task_id=? AND m.sender=?)`, task, task, task, task, task, out.job.ID, task, out.job.Agent).Scan(&n)
	return out, n == 0, err
}
