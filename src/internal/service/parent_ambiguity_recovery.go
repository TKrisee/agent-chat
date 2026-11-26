package service

import (
	"context"
	"database/sql"
	"errors"

	"agentchat/src/internal/runtimeapi"
)

type parentAmbiguityRecovery struct{ project, po, assignment, failed string }

func (s *Service) parentAmbiguityRecoveryEligible(tx *sql.Tx, task string) (out parentAmbiguityRecovery, ok bool, err error) {
	var stopped int
	err = tx.QueryRow(`SELECT p.id,p.po_id,p.stopped FROM tasks t JOIN projects p ON p.id=t.project_id JOIN agents a ON a.id=p.po_id AND a.project_id=p.id AND a.role='po' WHERE t.id=? AND t.status='blocked' AND t.owner_id=p.po_id`, task).Scan(&out.project, &out.po, &stopped)
	if errors.Is(err, sql.ErrNoRows) {
		return out, false, nil
	}
	if err != nil || stopped != 0 {
		return out, false, err
	}
	var source, kind, status, diagnostic, agent string
	var payload []byte
	err = tx.QueryRow(`SELECT id,source_id,kind,status,COALESCE(diagnostic,''),agent_id,result FROM jobs WHERE task_id=? ORDER BY rowid DESC LIMIT 1`, task).Scan(&out.failed, &source, &kind, &status, &diagnostic, &agent, &payload)
	if errors.Is(err, sql.ErrNoRows) {
		return out, false, nil
	}
	if err != nil {
		return out, false, err
	}
	if agent != out.po || kind != "po_followup" || status != "completed" || diagnostic != "invalid runtime result: completed child recipient is ambiguous" {
		return out, false, nil
	}
	inspector, yes := s.runtimeSetup().(runtimeapi.LegacyParentPairInspector)
	if !yes {
		return out, false, nil
	}
	title1, worker1, title2, worker2, valid := inspector.LegacyParentPairs(context.Background(), payload, out.project)
	if !valid || title1 == title2 {
		return out, false, nil
	}
	j := job{ID: out.failed, Task: task, Project: out.project}
	out.assignment, err = s.poAssignmentSource(tx, j, out.po)
	if errors.Is(err, errWorkflow) {
		return out, false, nil
	}
	if err != nil {
		return out, false, err
	}
	var provenance, conflicts int
	err = tx.QueryRow(`SELECT (SELECT count(*) FROM po_followups f JOIN messages m ON m.id=f.source_id JOIN jobs prior ON prior.id=f.trigger_id WHERE f.source_id=? AND f.phase='assigned' AND f.assignment_id=? AND m.task_id=? AND m.project_id=? AND m.recipient=? AND prior.task_id=m.task_id AND prior.project_id=m.project_id AND prior.agent_id=? AND prior.status='completed'),(SELECT count(*) FROM jobs x WHERE x.task_id=? AND x.status IN ('pending','submitted','running','uncertain') AND NOT EXISTS(SELECT 1 FROM scope_draft_confirmations c WHERE c.task_id=x.task_id AND c.review_job_id=x.id AND x.status='uncertain'))+(SELECT count(*) FROM task_holds WHERE task_id=? AND status='active')+(SELECT count(*) FROM po_followups WHERE trigger_id=?)`, source, out.assignment, task, out.project, out.po, out.po, task, task, out.failed).Scan(&provenance, &conflicts)
	if err != nil || provenance != 1 || conflicts != 0 {
		return out, false, err
	}
	for _, pair := range [][2]string{{title1, worker1}, {title2, worker2}} {
		var count int
		err = tx.QueryRow(`SELECT count(DISTINCT c.id) FROM po_work_items i JOIN tasks c ON c.id=i.child_task_id AND c.project_id=? AND c.owner_id=? AND c.status='completed' JOIN agents w ON w.id=? AND w.project_id=c.project_id AND w.role='worker' JOIN messages m ON m.task_id=c.id AND m.project_id=c.project_id AND m.sender=w.id AND m.recipient=? AND m.role='worker' JOIN jobs r ON r.task_id=c.id AND r.project_id=c.project_id AND r.agent_id=? AND r.status='completed' AND COALESCE(r.diagnostic,'')='' AND ((r.kind='review' AND r.source_id=m.id) OR (r.kind='po_followup' AND EXISTS(SELECT 1 FROM po_followups f WHERE f.source_id=r.source_id AND f.review_id=m.id))) JOIN messages report ON report.task_id=c.id AND report.project_id=c.project_id AND report.sender=? AND report.recipient IN (?,?) AND report.role='po' AND report.seq>m.seq WHERE i.parent_task_id=? AND c.title=? AND NOT EXISTS(SELECT 1 FROM jobs x WHERE x.task_id=c.id AND x.status IN ('pending','submitted','running','uncertain')) AND NOT EXISTS(SELECT 1 FROM task_holds h WHERE h.task_id=c.id AND h.status='active')`, out.project, out.po, pair[1], out.po, out.po, out.po, out.po, root, task, pair[0]).Scan(&count)
		if err != nil || count != 1 {
			return out, false, err
		}
	}
	return out, true, nil
}
