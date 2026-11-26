package service

import (
	"database/sql"
	"errors"
)

type parentReportRecovery struct{ project, po, assignment, child, failed, report string }

// parentReportRecoveryEligible admits only the exact pre-apply parent review
// failure with one fully reported child; it never replays the rejected result.
func (s *Service) parentReportRecoveryEligible(tx *sql.Tx, task string) (out parentReportRecovery, ok bool, err error) {
	err = tx.QueryRow(`SELECT p.id,p.po_id FROM tasks t JOIN projects p ON p.id=t.project_id JOIN agents a ON a.id=p.po_id AND a.project_id=p.id AND a.role='po' WHERE t.id=? AND t.status='blocked' AND t.owner_id=p.po_id AND p.stopped=0`, task).Scan(&out.project, &out.po)
	if errors.Is(err, sql.ErrNoRows) {
		return out, false, nil
	}
	if err != nil {
		return
	}
	var source, kind, status, diagnostic string
	err = tx.QueryRow(`SELECT id,source_id,kind,status,COALESCE(diagnostic,'') FROM jobs WHERE task_id=? ORDER BY rowid DESC LIMIT 1`, task).Scan(&out.failed, &source, &kind, &status, &diagnostic)
	if errors.Is(err, sql.ErrNoRows) {
		return out, false, nil
	}
	if err != nil {
		return
	}
	if kind != "po_followup" || status != "completed" || diagnostic != "invalid runtime result: active worker result unavailable" {
		return out, false, nil
	}
	j := job{ID: out.failed, Task: task, Project: out.project}
	out.assignment, err = s.poAssignmentSource(tx, j, out.po)
	if errors.Is(err, errWorkflow) {
		return out, false, nil
	}
	if err != nil {
		return
	}
	var provenance, conflicts int
	err = tx.QueryRow(`SELECT (SELECT count(*) FROM jobs j JOIN po_followups f ON f.source_id=j.source_id JOIN messages m ON m.id=j.source_id JOIN jobs prior ON prior.id=f.trigger_id WHERE j.id=? AND j.agent_id=? AND j.project_id=? AND f.phase='assigned' AND f.assignment_id=? AND m.sender='service' AND m.recipient=? AND m.role='system' AND m.task_id=j.task_id AND m.project_id=j.project_id AND prior.task_id=j.task_id AND prior.project_id=j.project_id AND prior.agent_id=j.agent_id AND prior.status='completed'), (SELECT count(*) FROM jobs x WHERE (x.task_id=? OR x.task_id IN (SELECT child_task_id FROM po_work_items WHERE parent_task_id=?)) AND x.status IN ('pending','submitted','running','uncertain') AND NOT EXISTS(SELECT 1 FROM scope_draft_confirmations c WHERE c.task_id=x.task_id AND c.review_job_id=x.id AND x.status='uncertain'))+(SELECT count(*) FROM approvals WHERE task_id=? OR task_id IN (SELECT child_task_id FROM po_work_items WHERE parent_task_id=?))+(SELECT count(*) FROM task_holds WHERE task_id=? AND status='active')+(SELECT count(*) FROM po_report_recoveries WHERE task_id=?)`, out.failed, out.po, out.project, out.assignment, out.po, task, task, task, task, task, task).Scan(&provenance, &conflicts)
	if err != nil || provenance != 1 || conflicts != 0 {
		return out, false, err
	}
	var count int
	err = tx.QueryRow(`SELECT count(*),COALESCE(min(c.id),''),COALESCE(min(r.text),'') FROM po_work_items i JOIN tasks c ON c.id=i.child_task_id AND c.status='completed' AND c.project_id=? AND c.owner_id=? JOIN jobs wj ON wj.task_id=c.id AND wj.project_id=c.project_id AND wj.kind='work' AND wj.status='completed' AND COALESCE(wj.diagnostic,'')='' JOIN agents w ON w.id=wj.agent_id AND w.role='worker' AND w.project_id=c.project_id JOIN messages wm ON wm.task_id=c.id AND wm.project_id=c.project_id AND wm.sender=w.id AND wm.recipient=? AND wm.role='worker' JOIN jobs review ON review.task_id=c.id AND review.project_id=c.project_id AND review.agent_id=? AND review.kind='review' AND review.source_id=wm.id AND review.status='completed' AND COALESCE(review.diagnostic,'')='' JOIN messages r ON r.task_id=c.id AND r.project_id=c.project_id AND r.sender=? AND r.recipient=? AND r.role='po' AND r.seq>wm.seq AND length(trim(r.text))>0 WHERE i.parent_task_id=?`, out.project, out.po, out.po, out.po, out.po, out.po, task).Scan(&count, &out.child, &out.report)
	if err != nil || count != 1 {
		return out, false, err
	}
	var children int
	err = tx.QueryRow(`SELECT count(*) FROM po_work_items WHERE parent_task_id=?`, task).Scan(&children)
	return out, children == 1, err
}
