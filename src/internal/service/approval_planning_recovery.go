package service

import (
	"context"
	"database/sql"
	"errors"

	"agentchat/src/internal/runtimeapi"
)

type approvalPlanningRecovery struct{ project, po, assignment, failed, decision string }

// approvalPlanningRecoveryEligible recognizes only the recorded two-failure
// chain. Neither rejected result is applied or treated as work completed.
func (s *Service) approvalPlanningRecoveryEligible(tx *sql.Tx, task string) (out approvalPlanningRecovery, ok bool, err error) {
	var stopped int
	err = tx.QueryRow(`SELECT p.id,p.po_id,p.stopped FROM tasks t JOIN projects p ON p.id=t.project_id JOIN agents a ON a.id=p.po_id AND a.project_id=p.id AND a.role='po' WHERE t.id=? AND t.status='blocked' AND t.owner_id=p.po_id`, task).Scan(&out.project, &out.po, &stopped)
	if errors.Is(err, sql.ErrNoRows) {
		return out, false, nil
	}
	if err != nil || stopped != 0 {
		return out, false, err
	}
	var source, status, kind, diagnostic string
	var approvedPayload []byte
	err = tx.QueryRow(`SELECT id,source_id,kind,status,COALESCE(diagnostic,''),result FROM jobs WHERE task_id=? ORDER BY rowid DESC LIMIT 1`, task).Scan(&out.failed, &source, &kind, &status, &diagnostic, &approvedPayload)
	if errors.Is(err, sql.ErrNoRows) {
		return out, false, nil
	}
	if err != nil {
		return out, false, err
	}
	if kind != "approval_approved" || status != "completed" || diagnostic != "invalid runtime result: parent task unavailable" {
		return out, false, nil
	}
	var prior, priorSource, priorKind, priorStatus, priorDiagnostic string
	var priorPayload []byte
	err = tx.QueryRow(`SELECT id,source_id,kind,status,COALESCE(diagnostic,''),result FROM jobs WHERE task_id=? AND rowid<(SELECT rowid FROM jobs WHERE id=?) AND agent_id=? ORDER BY rowid DESC LIMIT 1`, task, out.failed, out.po).Scan(&prior, &priorSource, &priorKind, &priorStatus, &priorDiagnostic, &priorPayload)
	if errors.Is(err, sql.ErrNoRows) {
		return out, false, nil
	}
	if err != nil {
		return out, false, err
	}
	if priorKind != "po_followup" || priorStatus != "completed" || priorDiagnostic != "invalid runtime result: active worker result unavailable" {
		return out, false, nil
	}
	inspector, yes := s.runtimeSetup().(runtimeapi.LegacyApprovalPlanningInspector)
	if !yes || !inspector.IsLegacyApprovalPlanning(context.Background(), priorPayload, approvedPayload, out.project) {
		return out, false, nil
	}
	j := job{ID: prior, Task: task, Project: out.project}
	out.assignment, err = s.poAssignmentSource(tx, j, out.po)
	if errors.Is(err, errWorkflow) {
		return out, false, nil
	}
	if err != nil {
		return out, false, err
	}
	var valid, conflicts int
	err = tx.QueryRow(`SELECT (SELECT count(*) FROM po_followups f JOIN messages m ON m.id=f.source_id JOIN jobs trigger ON trigger.id=f.trigger_id WHERE f.source_id=? AND f.phase='assigned' AND f.assignment_id=? AND m.task_id=? AND m.project_id=? AND m.sender='service' AND m.role='system' AND m.recipient=? AND trigger.task_id=m.task_id AND trigger.project_id=m.project_id AND trigger.agent_id=? AND trigger.status='completed'), (SELECT count(*) FROM jobs x WHERE x.task_id=? AND x.status IN ('pending','submitted','running','uncertain') AND NOT EXISTS(SELECT 1 FROM scope_draft_confirmations c WHERE c.review_job_id=x.id AND c.task_id=x.task_id AND x.status='uncertain'))+(SELECT count(*) FROM task_holds WHERE task_id=? AND status='active')+(SELECT count(*) FROM approvals WHERE task_id=? AND status='pending')`, priorSource, out.assignment, task, out.project, out.po, out.po, task, task, task).Scan(&valid, &conflicts)
	if err != nil || valid != 1 || conflicts != 0 {
		return out, false, err
	}
	// The generated decision message is exact and must correspond to one
	// approved same-task request; matching is intentionally fail-closed.
	err = tx.QueryRow(`SELECT count(*),COALESCE(min(m.id),'') FROM jobs j JOIN messages m ON m.id=j.source_id JOIN approvals ap ON ap.task_id=j.task_id AND ap.project_id=j.project_id AND ap.requested_by=j.agent_id AND ap.status='approved' LEFT JOIN approval_decision_notes n ON n.approval_id=ap.id WHERE j.id=? AND j.agent_id=? AND j.project_id=? AND m.sender='operator' AND m.recipient=? AND m.role='operator' AND m.room_id=? AND m.task_id=j.task_id AND m.project_id=j.project_id AND m.text=('Operator approved “'||ap.title||'”.'||char(10)||ap.body||CASE WHEN COALESCE(n.note,'')<>'' THEN char(10)||char(10)||'Operator decision note: '||n.note ELSE '' END)`, out.failed, out.po, out.project, out.po, directRoom(operator, out.po)).Scan(&valid, &out.decision)
	return out, err == nil && valid == 1, err
}
