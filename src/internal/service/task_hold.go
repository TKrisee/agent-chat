package service

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"agentchat/src/internal/runtimeapi"
)

type activeHold struct {
	job, task, project, po, phase, assignment, note, review, rootSource, rootMessage string
	legacy                                                                           bool
}

func (s *Service) activeTaskHold(tx *sql.Tx, task string) (h activeHold, ok bool, err error) {
	err = tx.QueryRow(`SELECT h.job_id,h.task_id,h.project_id,h.po_id,h.phase,h.assignment_id,COALESCE(h.note_id,''),COALESCE(h.review_id,''),h.root_source_id,COALESCE(h.root_message_id,'') FROM task_holds h JOIN tasks t ON t.id=h.task_id JOIN projects p ON p.id=t.project_id AND p.po_id=h.po_id WHERE h.task_id=? AND h.status='active' AND t.status='blocked' AND t.owner_id=h.po_id ORDER BY h.rowid DESC LIMIT 1`, task).Scan(&h.job, &h.task, &h.project, &h.po, &h.phase, &h.assignment, &h.note, &h.review, &h.rootSource, &h.rootMessage)
	if err == nil {
		return h, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return h, false, err
	}
	// Historical reconstruction is read-only until an explicit operator action.
	var payload []byte
	var kind, diagnostic string
	var noticeSeq int64
	err = tx.QueryRow(`SELECT j.id,j.task_id,j.project_id,j.agent_id,j.kind,COALESCE(j.diagnostic,''),j.result,nm.seq FROM tasks t JOIN projects p ON p.id=t.project_id AND p.po_id=t.owner_id JOIN agents a ON a.id=p.po_id AND a.role='po' AND a.project_id=p.id JOIN jobs j ON j.task_id=t.id AND j.project_id=p.id AND j.agent_id=a.id JOIN workflow_notices n ON n.event_key='blocked:'||j.id AND n.project_id=p.id JOIN messages nm ON nm.id=n.message_id WHERE t.id=? AND t.status='blocked' AND j.status='completed' AND NOT EXISTS(SELECT 1 FROM task_holds h WHERE h.task_id=t.id) ORDER BY j.rowid DESC LIMIT 1`, task).Scan(&h.job, &h.task, &h.project, &h.po, &kind, &diagnostic, &payload, &noticeSeq)
	if errors.Is(err, sql.ErrNoRows) {
		return h, false, nil
	}
	if err != nil {
		return h, false, err
	}
	if diagnostic != "" || len(payload) == 0 || len(payload) > 1<<20 {
		return h, false, nil
	}
	inspector, has := s.runtimeSetup().(runtimeapi.LegacyStructuredHoldInspector)
	if !has {
		return h, false, nil
	}
	key := fmt.Sprintf("hold:%s:%x", h.job, sha256.Sum256(payload))
	s.reviewProofMu.Lock()
	proven := s.reviewProof[key]
	s.reviewProofMu.Unlock()
	if !proven {
		proven = inspector.IsLegacyStructuredHold(context.Background(), payload)
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
		return h, false, nil
	}
	j := job{ID: h.job, Task: h.task, Project: h.project, Agent: h.po, Kind: kind}
	var source string
	if err = tx.QueryRow("SELECT source_id FROM jobs WHERE id=?", h.job).Scan(&source); err != nil {
		return h, false, err
	}
	j.Source = source
	h.assignment, err = s.poAssignmentSource(tx, j, h.po)
	if errors.Is(err, errWorkflow) {
		return h, false, nil
	}
	if err != nil {
		return h, false, err
	}
	switch kind {
	case "assignment":
		h.phase = "assigned"
	case "po_continue":
		h.phase = "assigned"
		h.note = source
	case "review":
		h.phase = "review"
		h.review = source
	case "po_followup":
		f, e := readPOFollowup(tx, source)
		if errors.Is(e, sql.ErrNoRows) {
			return h, false, nil
		}
		if e != nil {
			return h, false, e
		}
		h.phase, h.assignment, h.note, h.review = f.phase, f.assignment, f.note, f.review
	default:
		return h, false, nil
	}
	if (h.phase == "review" || h.phase == "accepted") && h.review == "" {
		return h, false, nil
	}
	// The hold transaction records the operator reason then the PO→Root source
	// immediately after its unique notice; this links the legacy escalation.
	var rootSource string
	err = tx.QueryRow(`SELECT m.id FROM messages m JOIN jobs j ON j.source_id=m.id AND j.kind='task_blocked' AND j.agent_id=? AND j.task_id=? AND j.project_id=? AND j.status='completed' WHERE m.seq=? AND m.sender=? AND m.recipient=? AND m.role='po' AND m.task_id=? AND m.project_id=? AND m.room_id=?`, root, task, h.project, noticeSeq+2, h.po, root, task, h.project, directRoom(h.po, root)).Scan(&rootSource)
	if errors.Is(err, sql.ErrNoRows) {
		return h, false, nil
	}
	if err != nil {
		return h, false, err
	}
	h.rootSource = rootSource
	var count int
	err = tx.QueryRow(`SELECT count(*),COALESCE(min(m.id),'') FROM messages m WHERE m.sender=? AND m.recipient=? AND m.role='root' AND m.task_id=? AND m.project_id=? AND m.room_id=? AND m.seq>(SELECT seq FROM messages WHERE id=?)`, root, operator, task, h.project, directRoom(root, operator), rootSource).Scan(&count, &h.rootMessage)
	if err != nil {
		return h, false, err
	}
	if count != 1 {
		return h, false, nil
	}
	h.legacy = true
	return h, true, nil
}
func (s *Service) holdCanResolve(tx *sql.Tx, h activeHold) (bool, error) {
	return s.holdCanResolveExcluding(tx, h, "", true)
}
func (s *Service) holdCanResolveExcluding(tx *sql.Tx, h activeHold, excludeJob string, requireRootMessage bool) (bool, error) {
	var valid, other, approvals int
	err := tx.QueryRow(`SELECT (SELECT count(*) FROM tasks t JOIN projects p ON p.id=t.project_id JOIN agents a ON a.id=p.po_id AND a.project_id=p.id AND a.role='po' WHERE t.id=? AND p.id=? AND p.po_id=? AND p.stopped=0 AND t.status='blocked' AND t.owner_id=p.po_id), (SELECT count(*) FROM jobs WHERE task_id=? AND id<>? AND status IN ('pending','submitted','running','uncertain')), (SELECT count(*) FROM approvals WHERE task_id=?)`, h.task, h.project, h.po, h.task, excludeJob, h.task).Scan(&valid, &other, &approvals)
	if err != nil {
		return false, err
	}
	if valid != 1 || other != 0 || approvals != 0 || (requireRootMessage && h.rootMessage == "") {
		return false, nil
	}
	if h.review != "" {
		worker, e := activeReviewWorker(tx, job{Task: h.task, Project: h.project, Kind: "review", Source: h.review}, h.po)
		err = e
		if errors.Is(err, errWorkflow) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		var owner, status string
		err = tx.QueryRow(`SELECT agent_id,status FROM jobs WHERE task_id=? AND project_id=? AND kind='work' ORDER BY rowid DESC LIMIT 1`, h.task, h.project).Scan(&owner, &status)
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if owner != worker || status != "completed" {
			return false, nil
		}
	} else if h.phase == "assigned" {
		var work int
		if err := tx.QueryRow(`SELECT count(*) FROM jobs WHERE task_id=? AND kind='work'`, h.task).Scan(&work); err != nil {
			return false, err
		}
		if work != 0 {
			return false, nil
		}
	}
	return true, nil
}
func (s *Service) resolveHold(tx *sql.Tx, j job, action runtimeapi.Action) error {
	if j.Kind == "task_blocked" && j.Agent == root {
		return s.resolveRootOwnedHold(tx, j, action)
	}
	if j.Kind != "operator_message" || j.Agent != root || action.Task == "" || action.Project == "" || action.Recipient != "" || action.Title != "" {
		return fmt.Errorf("%w: resolve_hold requires an exact task and project", errWorkflow)
	}
	h, found, err := s.activeTaskHold(tx, action.Task)
	if err != nil {
		return err
	}
	if !found || h.project != action.Project {
		return fmt.Errorf("%w: active held task unavailable", errWorkflow)
	}
	allowed, err := s.holdCanResolve(tx, h)
	if err != nil {
		return err
	}
	if !allowed {
		return fmt.Errorf("%w: held task is not safe to resume", errWorkflow)
	}
	var text string
	var userSeq, rootSeq int64
	var reply string
	err = tx.QueryRow(`SELECT m.text,m.seq,COALESCE(r.parent_id,'') FROM messages m LEFT JOIN message_replies r ON r.message_id=m.id WHERE m.id=? AND m.sender=? AND m.recipient=? AND m.role='operator' AND m.room_id=?`, j.Source, operator, root, directRoom(operator, root)).Scan(&text, &userSeq, &reply)
	if err != nil {
		return fmt.Errorf("%w: operator instruction unavailable", errWorkflow)
	}
	if strings.TrimSpace(text) == "" || strings.TrimSpace(action.Body) != strings.TrimSpace(text) {
		return fmt.Errorf("%w: resolve_hold body must match the operator instruction", errWorkflow)
	}
	if !clearHoldAuthorization(text, h.task) {
		return fmt.Errorf("%w: operator authorization must be an unambiguous affirmative", errWorkflow)
	}
	if err = tx.QueryRow("SELECT seq FROM messages WHERE id=? AND sender=? AND recipient=? AND task_id=?", h.rootMessage, root, operator, h.task).Scan(&rootSeq); err != nil {
		return fmt.Errorf("%w: linked Root escalation unavailable", errWorkflow)
	}
	if userSeq <= rootSeq {
		return fmt.Errorf("%w: operator instruction predates hold", errWorkflow)
	}
	if !strings.Contains(text, h.task) && reply != h.rootMessage {
		var latest string
		err = tx.QueryRow(`SELECT id FROM messages WHERE sender=? AND recipient=? AND room_id=? AND seq<? ORDER BY seq DESC LIMIT 1`, root, operator, directRoom(root, operator), userSeq).Scan(&latest)
		if err != nil || latest != h.rootMessage {
			return fmt.Errorf("%w: specify the exact held task or reply to its escalation", errWorkflow)
		}
		holds, truncated, err := s.activeHolds(tx)
		if err != nil {
			return err
		}
		if truncated || len(holds) != 1 || holds[0].job != h.job {
			return fmt.Errorf("%w: specify the exact held task", errWorkflow)
		}
	}
	return s.applyHoldResolution(tx, h, text, newID(), "")
}

// applyHoldResolution executes the shared held-task transition after authorization.
func (s *Service) applyHoldResolution(tx *sql.Tx, h activeHold, text, note, rootJob string) error {
	var err error
	if h.legacy {
		_, err = tx.Exec(`INSERT INTO task_holds(job_id,task_id,project_id,po_id,phase,assignment_id,note_id,review_id,root_source_id,root_message_id,status) VALUES(?,?,?,?,?,?,?,?,?,?,'active')`, h.job, h.task, h.project, h.po, h.phase, h.assignment, nullIfEmpty(h.note), nullIfEmpty(h.review), h.rootSource, h.rootMessage)
		if err != nil {
			return err
		}
	}
	result, err := tx.Exec(`UPDATE task_holds SET status='resolved' WHERE job_id=? AND status='active'`, h.job)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil || n != 1 {
		return fmt.Errorf("%w: held task already resolved", errWorkflow)
	}
	result, err = tx.Exec(`UPDATE tasks SET status=? WHERE id=? AND project_id=? AND status='blocked' AND owner_id=?`, h.phase, h.task, h.project, h.po)
	if err != nil {
		return err
	}
	n, err = result.RowsAffected()
	if err != nil || n != 1 {
		return fmt.Errorf("%w: held task changed", errWorkflow)
	}
	sender, role, noteID := operator, "operator", note
	if rootJob != "" {
		sender, role, noteID = root, "root", ""
	}
	if err := addMessage(tx, note, sender, h.po, role, text, h.project, h.task); err != nil {
		return err
	}
	if rootJob != "" {
		if _, err = tx.Exec(`INSERT INTO root_hold_decisions(task_id,hold_job_id,note_id) VALUES(?,?,?)`, h.task, h.job, note); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(`INSERT INTO po_followups(source_id,trigger_id,ordinal,phase,assignment_id,note_id,review_id) VALUES(?,?,1,?,?,?,?)`, note, h.job, h.phase, h.assignment, nullIfEmpty(noteID), nullIfEmpty(h.review)); err != nil {
		return err
	}
	a, err := getAgent(tx, h.po)
	if err != nil {
		return err
	}
	instruction := "The operator has explicitly responded to the held task. Read the pinned original assignment, any current worker receipt and the new operator instruction. Respect all earlier task-scoped safety constraints, stops and authorization boundaries. Continue only this held phase; do not replay earlier effects or create a new task."
	if rootJob != "" {
		instruction = "Root has made an explicit within-scope decision on this held task. Read the pinned original assignment, any current worker receipt and Root's task-linked decision. Respect all earlier safety constraints, production stops and authorization boundaries. Continue only this held phase; do not replay earlier effects or create a new task."
	}
	instruction += fmt.Sprintf(" The hold on task %s has already been resolved; the current task phase is %s and its owning PO is %s. You are the PO: do not emit resolve_hold, which is Root-only. Make the next permitted decision or delegate approved scoped work through normal actions, preserving the recorded receipt and all constraints.", h.task, h.phase, h.po)
	return s.enqueue(tx, a, h.project, h.task, note, "po_followup", instruction)
}

// Outstanding hold references are bounded; ambiguity fails closed.
func (s *Service) activeHolds(tx *sql.Tx) ([]activeHold, bool, error) {
	tasks, err := querySlice(tx, `SELECT id FROM tasks WHERE status='blocked' ORDER BY rowid DESC LIMIT 21`, func(rows *sql.Rows) (string, error) { var id string; err := rows.Scan(&id); return id, err })
	if err != nil {
		return nil, false, err
	}
	truncated := len(tasks) > 20
	if truncated {
		tasks = tasks[:20]
	}
	var holds []activeHold
	for _, task := range tasks {
		h, ok, err := s.activeTaskHold(tx, task)
		if err != nil {
			return nil, false, err
		}
		if ok && h.rootMessage != "" {
			holds = append(holds, h)
		}
	}
	return holds, truncated, nil
}

var negativeHoldAuthorization = regexp.MustCompile(`(?:^|[^a-z])(?:do\s+not|don't|dont|never|not|no)\s*[,]?\s*(?:go\s+ahead|proceed|continu(?:e|ing)|approv(?:e|al|ed)|authoriz(?:e|ation)|resum(?:e|ing)|accept|agree|now|yet)(?:[^a-z]|$)`)

func clearHoldAuthorization(text, task string) bool {
	lower := strings.ToLower(strings.TrimSpace(strings.NewReplacer("’", "'", "‘", "'").Replace(text)))
	if negativeHoldAuthorization.MatchString(lower) {
		return false
	}
	if strings.Contains(lower, "unless ") || strings.Contains(lower, "if ") || strings.Contains(lower, "when ") || strings.Contains(lower, "after ") || strings.Contains(lower, "once ") || strings.Contains(lower, "until ") || strings.HasPrefix(lower, "no,") || strings.Contains(lower, "don't go ahead") || strings.Contains(lower, "do not go ahead") || strings.Contains(lower, "never go ahead") || strings.Contains(lower, "not go ahead") || strings.Contains(lower, "don't proceed") || strings.Contains(lower, "do not proceed") {
		return false
	}
	scoped := strings.Contains(lower, strings.ToLower(task))
	answer := strings.Trim(strings.TrimSpace(strings.ReplaceAll(lower, strings.ToLower(task), "")), " .!")
	if scoped && strings.HasPrefix(answer, "continue ") && len(strings.TrimSpace(strings.TrimPrefix(answer, "continue "))) > 0 {
		return true
	}
	if i := strings.LastIndex(answer, " but "); i >= 0 {
		answer = strings.TrimSpace(answer[i+5:])
	}
	switch answer {
	case "yes", "go ahead", "proceed", "approved", "continue", "sure go ahead", "sure, go ahead":
		return true
	}
	return false
}

// resolveRootOwnedHold handles one explicit Root-owned escalation without
// converting a legacy/operator-owned hold into Root authority.
func (s *Service) resolveRootOwnedHold(tx *sql.Tx, j job, action runtimeapi.Action) error {
	if action.Task == "" || action.Task != j.Task || action.Project == "" || action.Project != j.Project || action.Recipient != "" || action.Title != "" || action.ReplyTo != "" || strings.TrimSpace(action.Body) == "" || len(action.Body) > 4096 {
		return fmt.Errorf("%w: Root decision requires the exact held task and project", errWorkflow)
	}
	h, found, err := s.activeTaskHold(tx, action.Task)
	if err != nil {
		return err
	}
	if !found || h.project != j.Project || h.rootSource != j.Source || h.legacy {
		return fmt.Errorf("%w: Root-owned hold unavailable", errWorkflow)
	}
	var marked, used int
	if err = tx.QueryRow(`SELECT (SELECT count(*) FROM root_owned_holds WHERE job_id=?),(SELECT count(*) FROM root_hold_decisions WHERE task_id=?)`, h.job, h.task).Scan(&marked, &used); err != nil {
		return err
	}
	if marked != 1 || used != 0 {
		return fmt.Errorf("%w: Root-owned hold unavailable or already resolved", errWorkflow)
	}
	allowed, err := s.holdCanResolveExcluding(tx, h, j.ID, false)
	if err != nil {
		return err
	}
	if !allowed {
		return fmt.Errorf("%w: held task is not safe to resume", errWorkflow)
	}
	return s.applyHoldResolution(tx, h, strings.TrimSpace(action.Body), newID(), j.ID)
}
