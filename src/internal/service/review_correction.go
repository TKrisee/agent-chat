package service

import (
	"agentchat/src/internal/runtimeapi"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
)

type reviewConflictError struct{}

func (*reviewConflictError) Error() string {
	return errWorkflow.Error() + ": hold_task cannot be combined with accept_result, report, queue_worker, or request_approval; choose a hold or a review action"
}
func (*reviewConflictError) Unwrap() error { return errWorkflow }

func reviewCorrectionEligible(tx *sql.Tx, j job) (bool, error) {
	if j.Kind != "review" && j.Kind != "po_followup" {
		return false, nil
	}
	var valid, other, approvals int
	err := tx.QueryRow(`SELECT
 (SELECT count(*) FROM tasks t JOIN projects p ON p.id=t.project_id JOIN agents a ON a.id=p.po_id AND a.role='po' AND a.project_id=p.id JOIN jobs q ON q.id=? AND q.agent_id=a.id AND q.project_id=p.id AND q.task_id=t.id WHERE t.id=? AND p.id=? AND t.status='review' AND t.owner_id=a.id AND p.stopped=0 AND q.status IN ('submitted','running')),
 (SELECT count(*) FROM jobs WHERE task_id=? AND id<>? AND status IN ('pending','submitted','running','uncertain')),
 (SELECT count(*) FROM approvals WHERE task_id=?)`, j.ID, j.Task, j.Project, j.Task, j.ID, j.Task).Scan(&valid, &other, &approvals)
	if err != nil {
		return false, err
	}
	if valid != 1 || other != 0 || approvals != 0 {
		return false, nil
	}
	if j.Kind == "po_followup" {
		f, err := readPOFollowup(tx, j.Source)
		if err != nil {
			return false, err
		}
		if f.phase != "review" {
			return false, nil
		}
	}
	_, err = activeReviewWorker(tx, j, j.Agent)
	if errors.Is(err, errWorkflow) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

const legacyReviewDiagnostic = "invalid runtime result: task owner or status changed"
const legacyChildReportDiagnostic = "invalid runtime result: report requires Root and a body"

type legacyReview struct {
	job         job
	assignment  string
	childReport bool
}

// legacyReviewRecovery only recognizes the exact historical jq-proven conflict.
func (s *Service) legacyReviewRecovery(tx *sql.Tx, task string) (out legacyReview, eligible bool, err error) {
	var po, project string
	var stopped int
	err = tx.QueryRow(`SELECT p.id,p.po_id,p.stopped FROM tasks t JOIN projects p ON p.id=t.project_id JOIN agents a ON a.id=p.po_id AND a.role='po' AND a.project_id=p.id WHERE t.id=? AND t.status='blocked' AND t.owner_id=p.po_id`, task).Scan(&project, &po, &stopped)
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
		return
	}
	if err != nil || stopped != 0 {
		return
	}
	var payload []byte
	var diagnostic, status string
	err = tx.QueryRow(`SELECT id,agent_id,source_id,kind,status,COALESCE(diagnostic,''),result FROM jobs WHERE task_id=? ORDER BY rowid DESC LIMIT 1`, task).Scan(&out.job.ID, &out.job.Agent, &out.job.Source, &out.job.Kind, &status, &diagnostic, &payload)
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
		return
	}
	if err != nil {
		return
	}
	if out.job.Agent != po || out.job.Kind != "review" || status != "completed" || (diagnostic != legacyReviewDiagnostic && diagnostic != legacyChildReportDiagnostic) {
		return
	}
	out.job.Task = task
	out.job.Project = project
	out.childReport = diagnostic == legacyChildReportDiagnostic
	if out.childReport {
		var parent string
		err = tx.QueryRow(`SELECT i.parent_task_id FROM po_work_items i JOIN tasks p ON p.id=i.parent_task_id JOIN projects pr ON pr.id=p.project_id AND pr.po_id=p.owner_id WHERE i.child_task_id=? AND p.project_id=? AND p.owner_id=? AND p.status='assigned' AND pr.stopped=0`, task, project, po).Scan(&parent)
		if errors.Is(err, sql.ErrNoRows) {
			return out, false, nil
		}
		if err != nil {
			return out, false, err
		}
		var parentUnsettled int
		if err = tx.QueryRow(`SELECT (SELECT count(*) FROM jobs WHERE task_id=? AND status='uncertain')+(SELECT count(*) FROM approvals WHERE task_id=?)`, parent, parent).Scan(&parentUnsettled); err != nil {
			return out, false, err
		}
		if parentUnsettled != 0 {
			return out, false, nil
		}
	}
	var other, approvals int
	err = tx.QueryRow(`SELECT (SELECT count(*) FROM jobs WHERE task_id=? AND status IN ('pending','submitted','running','uncertain')),(SELECT count(*) FROM approvals WHERE task_id=?)`, task, task).Scan(&other, &approvals)
	if err != nil || other != 0 || approvals != 0 {
		return
	}
	worker, e := activeReviewWorker(tx, out.job, po)
	if errors.Is(e, errWorkflow) {
		return out, false, nil
	}
	if e != nil {
		return out, false, e
	}
	var latestWorker, workerStatus string
	e = tx.QueryRow(`SELECT agent_id,status FROM jobs WHERE task_id=? AND project_id=? AND kind='work' ORDER BY rowid DESC LIMIT 1`, task, project).Scan(&latestWorker, &workerStatus)
	if errors.Is(e, sql.ErrNoRows) {
		return out, false, nil
	}
	if e != nil {
		return out, false, e
	}
	if latestWorker != worker || workerStatus != "completed" {
		return
	}
	var previous int
	if err = tx.QueryRow(`SELECT count(*) FROM po_followups WHERE trigger_id=?`, out.job.ID).Scan(&previous); err != nil || previous != 0 {
		return
	}
	out.assignment, e = s.poAssignmentSource(tx, out.job, po)
	if errors.Is(e, errWorkflow) {
		return out, false, nil
	}
	if e != nil {
		return out, false, e
	}
	_, notesOK, e := legacyReviewNotes(tx, out.job.ID, task, project, po)
	if e != nil {
		return out, false, e
	}
	if !notesOK {
		return out, false, nil
	}
	if out.childReport {
		inspector, ok := s.runtimeSetup().(runtimeapi.LegacyChildReportInspector)
		if !ok || len(payload) == 0 || len(payload) > 1<<20 {
			return out, false, nil
		}
		return out, inspector.IsLegacyChildReport(context.Background(), payload, project, worker), nil
	}
	inspector, ok := s.runtimeSetup().(runtimeapi.LegacyReviewConflictInspector)
	if !ok || len(payload) == 0 || len(payload) > 1<<20 {
		return
	}
	key := fmt.Sprintf("%s:%x", out.job.ID, sha256.Sum256(payload))
	s.reviewProofMu.Lock()
	proven := s.reviewProof[key]
	s.reviewProofMu.Unlock()
	if !proven {
		proven = inspector.IsLegacyReviewConflict(context.Background(), payload)
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

// legacyReviewNotes pins only authenticated task-scoped continuation notes that
// preceded this worker dispatch. The final PO decision must carry the latest
// note, including through an existing follow-up provenance record.
func legacyReviewNotes(tx *sql.Tx, trigger, task, project, po string) ([]string, bool, error) {
	var kind, diag string
	err := tx.QueryRow(`SELECT kind,COALESCE(diagnostic,'') FROM jobs WHERE id=? AND task_id=? AND project_id=? AND agent_id=?`, trigger, task, project, po).Scan(&kind, &diag)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if kind != "review" || diag != legacyReviewDiagnostic {
		return nil, true, nil
	}
	var workRow, triggerRow int64
	err = tx.QueryRow(`SELECT w.rowid,r.rowid FROM jobs w JOIN jobs r ON r.id=? WHERE w.task_id=? AND w.project_id=? AND w.kind='work' AND w.status='completed' ORDER BY w.rowid DESC LIMIT 1`, trigger, task, project).Scan(&workRow, &triggerRow)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if workRow >= triggerRow {
		return nil, false, nil
	}
	type oldNote struct {
		id    string
		valid int
	}
	notes, err := querySlice(tx, `SELECT c.source_id,CASE WHEN c.agent_id=? AND c.project_id=? AND c.status='completed' AND m.sender=? AND m.recipient=? AND m.role='operator' AND m.task_id=c.task_id AND m.project_id=c.project_id AND m.room_id=? THEN 1 ELSE 0 END FROM jobs c LEFT JOIN messages m ON m.id=c.source_id WHERE c.task_id=? AND c.kind='po_continue' AND c.rowid<? ORDER BY c.rowid`, func(rows *sql.Rows) (oldNote, error) { var n oldNote; err := rows.Scan(&n.id, &n.valid); return n, err }, po, project, operator, po, directRoom(operator, po), task, workRow)
	if err != nil {
		return nil, false, err
	}
	if len(notes) > 12 {
		return nil, false, nil
	}
	ids := make([]string, 0, len(notes))
	for _, n := range notes {
		if n.valid != 1 {
			return nil, false, nil
		}
		ids = append(ids, n.id)
	}
	if len(ids) == 0 {
		return ids, true, nil
	}
	var decisionKind, decisionSource, decisionStatus string
	err = tx.QueryRow(`SELECT kind,source_id,status FROM jobs WHERE task_id=? AND project_id=? AND agent_id=? AND rowid<? AND kind IN ('assignment','po_continue','po_followup','review','approval_approved') ORDER BY rowid DESC LIMIT 1`, task, project, po, workRow).Scan(&decisionKind, &decisionSource, &decisionStatus)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if decisionStatus != "completed" {
		return nil, false, nil
	}
	if decisionKind == "po_followup" {
		f, err := readPOFollowup(tx, decisionSource)
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
		decisionSource = f.note
	} else if decisionKind != "po_continue" {
		return nil, false, nil
	}
	return ids, decisionSource == ids[len(ids)-1], nil
}
