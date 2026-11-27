package service

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"agentchat/src/internal/runtimeapi"
)

const reportDeliverySchema = `
CREATE TABLE IF NOT EXISTS accepted_task_reports (
 task_id TEXT PRIMARY KEY REFERENCES tasks(id),
 message_id TEXT NOT NULL UNIQUE REFERENCES messages(id),
 job_id TEXT NOT NULL REFERENCES jobs(id)
);
CREATE TABLE IF NOT EXISTS project_report_deliveries (
 relay_job_id TEXT PRIMARY KEY REFERENCES jobs(id),
 message_id TEXT NOT NULL UNIQUE REFERENCES messages(id)
);
CREATE TABLE IF NOT EXISTS project_report_delivery_issues (
 relay_job_id TEXT PRIMARY KEY REFERENCES jobs(id),
 notice_id TEXT NOT NULL UNIQUE REFERENCES messages(id)
);
CREATE TABLE IF NOT EXISTS project_report_review_attempts (
 request_message_id TEXT PRIMARY KEY REFERENCES messages(id),
 relay_job_id TEXT NOT NULL REFERENCES project_report_delivery_issues(relay_job_id)
);
CREATE INDEX IF NOT EXISTS project_report_review_attempts_relay ON project_report_review_attempts(relay_job_id);
CREATE INDEX IF NOT EXISTS project_report_failed_relays ON jobs(agent_id,kind,status)
 WHERE kind='project_report' AND (status='uncertain' OR (status='completed' AND COALESCE(diagnostic,'')<>''));
`

func recordAcceptedTaskReport(tx *sql.Tx, po agent, j job, recipient, text string, files []attachmentData) (string, error) {
	id, err := outputMessage(tx, po, recipient, j, text, files)
	if err != nil {
		return "", err
	}
	_, err = tx.Exec(`INSERT INTO accepted_task_reports(task_id,message_id,job_id) VALUES(?,?,?)`, j.Task, id, j.ID)
	return id, err
}

func shareAcceptedChildReportAttachments(tx *sql.Tx, j job, po, report string) error {
	_, err := tx.Exec(`INSERT INTO attachment_shares(message_id,attachment_id)
	 SELECT ?,links.attachment_id FROM po_work_items i JOIN tasks c ON c.id=i.child_task_id AND c.status='completed'
	 JOIN accepted_task_reports r ON r.task_id=c.id
	 JOIN jobs review ON review.id=r.job_id AND review.task_id=c.id AND review.project_id=c.project_id AND review.agent_id=? AND review.status='completed' AND COALESCE(review.diagnostic,'')=''
	 JOIN messages m ON m.id=r.message_id AND m.task_id=c.id AND m.project_id=c.project_id AND m.sender=review.agent_id AND m.recipient=review.agent_id AND m.role='po' AND m.room_id=?
	 JOIN message_attachments links ON links.message_id=m.id
	 WHERE i.parent_task_id=? AND c.project_id=? AND c.owner_id=review.agent_id
	 ON CONFLICT(message_id,attachment_id) DO NOTHING`, report, po, directRoom(po, po), j.Task, j.Project)
	return err
}

func (s *Service) relayProjectReport(tx *sql.Tx, a agent, j job, text string, files []attachmentData) error {
	if err := verifyProjectReportSource(tx, j); err != nil {
		return err
	}
	if strings.TrimSpace(text) == "" {
		return fmt.Errorf("%w: report relay requires an operator-facing summary", errWorkflow)
	}
	id, err := outputMessage(tx, a, operator, j, text, files)
	if err != nil {
		return err
	}
	if err := shareTaskAttachments(tx, j.Source, id); err != nil {
		return err
	}
	return recordProjectReportDelivery(tx, j.ID, id)
}

func verifyProjectReportSource(tx *sql.Tx, j job) error {
	var id string
	err := tx.QueryRow(`SELECT m.id FROM messages m JOIN projects p ON p.id=m.project_id
		JOIN tasks t ON t.id=m.task_id AND t.project_id=p.id AND t.status='completed' AND t.owner_id=p.po_id
		JOIN agents owner ON owner.id=p.po_id AND owner.project_id=p.id AND owner.role='po'
		WHERE m.id=? AND m.task_id=? AND p.id=? AND m.sender=owner.id AND m.recipient=? AND m.role='po'
		AND m.room_id='dm:'||CASE WHEN m.sender<m.recipient THEN m.sender||':'||m.recipient ELSE m.recipient||':'||m.sender END
		AND length(trim(m.text))>0
		AND NOT EXISTS(SELECT 1 FROM accepted_task_reports r WHERE r.task_id=t.id AND r.message_id<>m.id)`, j.Source, j.Task, j.Project, root).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: exact accepted PO report unavailable", errWorkflow)
	}
	return err
}

func recordProjectReportDelivery(tx *sql.Tx, relay, message string) error {
	_, err := tx.Exec(`INSERT INTO project_report_deliveries(relay_job_id,message_id) VALUES(?,?)`, relay, message)
	return err
}

const projectReportRecovery = "project_report_recovery"

const reportRecoveryGuide = "Produce only an operator-facing summary of the exact accepted report supplied in context. Preserve the saved outcome and omit unrelated private material. Do not perform work, use tools, or repeat any prior effects. Return no orchestration actions and no new files. Nominate only exact linked report attachments that you have reviewed and explicitly authorize for this operator delivery."

func reportRelay(tx *sql.Tx, relayID string) (j job, err error) {
	err = tx.QueryRow(`SELECT id,project_id,task_id,source_id FROM jobs WHERE id=? AND agent_id=? AND kind='project_report' AND
	 (status='uncertain' OR (status='completed' AND COALESCE(diagnostic,'')<>'')) AND project_id IS NOT NULL AND task_id IS NOT NULL`, relayID, root).
		Scan(&j.ID, &j.Project, &j.Task, &j.Source)
	return j, err
}

func recordProjectReportIssue(tx *sql.Tx, relayID string) error {
	j, err := reportRelay(tx, relayID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := verifyProjectReportSource(tx, j); err != nil {
		if errors.Is(err, errWorkflow) {
			return nil
		}
		return err
	}
	var known int
	if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM project_report_delivery_issues WHERE relay_job_id=? UNION ALL
		SELECT 1 FROM project_report_deliveries WHERE relay_job_id=?)`, relayID, relayID).Scan(&known); err != nil || known != 0 {
		return err
	}
	id := newID()
	const notice = "The project owner’s accepted report is saved, but Root’s relay did not complete. Reply to this message with /recover-report to request a fresh delivery review. The original attempt remains recorded."
	if err := addMessage(tx, id, root, operator, "system", notice, j.Project, j.Task); err != nil {
		return err
	}
	_, err = tx.Exec(`INSERT INTO project_report_delivery_issues(relay_job_id,notice_id) VALUES(?,?)`, relayID, id)
	return err
}

func backfillProjectReportIssues(tx *sql.Tx) error {
	ids, err := querySlice(tx, `SELECT id FROM jobs j WHERE j.agent_id=? AND j.kind='project_report'
		AND (j.status='uncertain' OR (j.status='completed' AND COALESCE(j.diagnostic,'')<>''))
		AND NOT EXISTS(SELECT 1 FROM project_report_delivery_issues x WHERE x.relay_job_id=j.id)
		AND NOT EXISTS(SELECT 1 FROM project_report_deliveries d WHERE d.relay_job_id=j.id)`, func(rows *sql.Rows) (string, error) {
		var id string
		err := rows.Scan(&id)
		return id, err
	}, root)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := recordProjectReportIssue(tx, id); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) requestProjectReportRecovery(tx *sql.Tx, request, notice string) error {
	var relay string
	err := tx.QueryRow(`SELECT i.relay_job_id FROM project_report_delivery_issues i
		JOIN messages n ON n.id=i.notice_id AND n.sender=? AND n.recipient=? AND n.role='system' AND n.room_id=?
		WHERE i.notice_id=?`, root, operator, directRoom(root, operator), notice).Scan(&relay)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: reply to the exact report delivery notice", errInvalid)
	}
	if err != nil {
		return err
	}
	j, err := reportRelay(tx, relay)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: pending report delivery is unavailable", errConflict)
	}
	if err != nil {
		return err
	}
	if err := verifyProjectReportSource(tx, j); err != nil {
		return err
	}
	var busy int
	err = tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM project_report_deliveries d WHERE d.relay_job_id=? UNION ALL
		SELECT 1 FROM project_report_review_attempts r JOIN jobs j ON j.source_id=r.request_message_id
		WHERE r.relay_job_id=? AND j.kind=? AND j.status IN ('pending','submitted','running','uncertain'))`, relay, relay, projectReportRecovery).Scan(&busy)
	if err != nil {
		return err
	}
	if busy != 0 {
		return fmt.Errorf("%w: report recovery is queued, unresolved, or already delivered", errConflict)
	}
	if _, err := tx.Exec(`INSERT INTO project_report_review_attempts(request_message_id,relay_job_id) VALUES(?,?)`, request, relay); err != nil {
		return err
	}
	manager, err := getAgent(tx, root)
	if err != nil {
		return err
	}
	return s.enqueue(tx, manager, j.Project, j.Task, request, projectReportRecovery, reportRecoveryGuide)
}

func (s *Service) relayRecoveredProjectReport(tx *sql.Tx, a agent, j job, text string, refs []runtimeapi.Attachment) error {
	var relay, notice string
	err := tx.QueryRow(`SELECT r.relay_job_id,i.notice_id FROM project_report_review_attempts r
		JOIN project_report_delivery_issues i ON i.relay_job_id=r.relay_job_id
		JOIN message_replies reply ON reply.message_id=r.request_message_id AND reply.parent_id=i.notice_id
		JOIN messages m ON m.id=r.request_message_id AND m.sender=? AND m.recipient=? AND m.role='operator' AND m.room_id=? AND m.text='/recover-report'
		WHERE r.request_message_id=? AND NOT EXISTS(SELECT 1 FROM project_report_deliveries d WHERE d.relay_job_id=r.relay_job_id)`, operator, root, directRoom(root, operator), j.Source).Scan(&relay, &notice)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: exact report recovery unavailable", errWorkflow)
	}
	if err != nil {
		return err
	}
	original, err := reportRelay(tx, relay)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && (original.Task != j.Task || original.Project != j.Project)) {
		return fmt.Errorf("%w: report recovery scope changed", errWorkflow)
	}
	if err != nil {
		return err
	}
	if err := verifyProjectReportSource(tx, original); err != nil {
		return err
	}
	if strings.TrimSpace(text) == "" {
		return fmt.Errorf("%w: report recovery requires an operator-facing summary", errWorkflow)
	}
	id, err := outputMessage(tx, a, operator, j, text, nil)
	if err != nil {
		return err
	}
	if err := s.shareNominatedReportAttachments(tx, original.Source, id, refs); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO message_replies(message_id,parent_id) VALUES(?,?)`, id, notice); err != nil {
		return err
	}
	return recordProjectReportDelivery(tx, relay, id)
}
