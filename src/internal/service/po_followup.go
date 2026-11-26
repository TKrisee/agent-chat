package service

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"agentchat/src/internal/runtimeapi"
)

type poFollowup struct {
	trigger, phase, assignment, note, review string
	ordinal                                  int
}

func readPOFollowup(tx *sql.Tx, source string) (f poFollowup, err error) {
	err = tx.QueryRow(`SELECT trigger_id,phase,assignment_id,COALESCE(note_id,''),COALESCE(review_id,''),ordinal FROM po_followups WHERE source_id=?`, source).Scan(&f.trigger, &f.phase, &f.assignment, &f.note, &f.review, &f.ordinal)
	return
}
func (s *Service) poAssignmentSource(tx *sql.Tx, j job, po string) (string, error) {
	var id string
	var count int
	assignmentTask := j.Task
	var parent string
	if err := tx.QueryRow("SELECT parent_task_id FROM po_work_items WHERE child_task_id=?", j.Task).Scan(&parent); err == nil {
		assignmentTask = parent
	} else if !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	err := tx.QueryRow(`SELECT count(*),COALESCE(min(m.id),'') FROM jobs a JOIN messages m ON m.id=a.source_id WHERE a.task_id=? AND a.project_id=? AND a.agent_id=? AND a.kind='assignment' AND (a.status='completed' OR a.id=?) AND COALESCE(a.diagnostic,'')='' AND m.task_id=a.task_id AND m.project_id=a.project_id AND m.sender=? AND m.recipient=? AND m.role='root' AND m.room_id=? AND length(trim(m.text))>0`, assignmentTask, j.Project, po, j.ID, root, po, directRoom(root, po)).Scan(&count, &id)
	if err != nil {
		return "", err
	}
	if count == 0 {
		_, recovered, ok, err := s.recoveredTitledAssignment(tx, assignmentTask, j.Project, po)
		if err != nil {
			return "", err
		}
		if ok {
			return recovered, nil
		}
		recovered, ok, err = s.legacyApprovalAssignment(tx, assignmentTask, j.Project, po)
		if err != nil {
			return "", err
		}
		if ok {
			return recovered, nil
		}
	}
	if count != 1 {
		return "", fmt.Errorf("%w: original PO assignment is unavailable", errWorkflow)
	}
	return id, nil
}
func (s *Service) poFollowThrough(tx *sql.Tx, j job, a agent, result runtimeapi.Result, correction ...string) error {
	if a.Role != "po" || j.Task == "" || (j.Kind != "assignment" && j.Kind != "po_continue" && j.Kind != "review" && j.Kind != "po_followup") {
		return nil
	}
	for _, action := range result.Actions {
		if action.Type != "post_group" && action.Type != "accept_result" && action.Type != "create_worker" && action.Type != "create_one_shot_worker" && action.Type != "report_progress" {
			return nil
		}
	}
	var phase, owner string
	var stopped int
	err := tx.QueryRow(`SELECT t.status,t.owner_id,p.stopped FROM tasks t JOIN projects p ON p.id=t.project_id AND p.po_id=? WHERE t.id=? AND p.id=?`, a.ID, j.Task, j.Project).Scan(&phase, &owner, &stopped)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if owner != a.ID || stopped != 0 || (phase != "assigned" && phase != "review" && phase != "accepted") {
		return nil
	}
	var other, approvals int
	if err = tx.QueryRow(`SELECT (SELECT count(*) FROM jobs WHERE task_id=? AND id<>? AND status IN ('pending','submitted','running','uncertain') AND NOT EXISTS(SELECT 1 FROM scope_draft_confirmations c WHERE c.task_id=jobs.task_id AND c.review_job_id=jobs.id AND jobs.status='uncertain')),(SELECT count(*) FROM approvals WHERE task_id=? AND status IN ('pending','denied'))`, j.Task, j.ID, j.Task).Scan(&other, &approvals); err != nil {
		return err
	}
	if other != 0 || approvals != 0 {
		return nil
	}
	var children int
	if err := tx.QueryRow(`SELECT count(*) FROM po_work_items i JOIN tasks c ON c.id=i.child_task_id WHERE i.parent_task_id=? AND c.status IN ('queued','review','accepted')`, j.Task).Scan(&children); err != nil {
		return err
	}
	if children != 0 && len(correction) == 0 {
		return nil
	}
	f := poFollowup{trigger: j.ID, phase: phase}
	if j.Kind == "po_followup" {
		f, err = readPOFollowup(tx, j.Source)
		if err != nil {
			return err
		}
		var planning int
		if err = tx.QueryRow(`SELECT count(*) FROM po_planning_wakes WHERE source_id=?`, j.Source).Scan(&planning); err != nil {
			return err
		}
		if planning != 0 {
			f.trigger = j.ID
			f.ordinal = 0
		} else if f.phase != phase {
			f.ordinal = 0
		}
		f.phase = phase
	} else {
		f.assignment, err = s.poAssignmentSource(tx, j, a.ID)
		if err != nil {
			return err
		}
		if j.Kind == "po_continue" {
			f.note = j.Source
		}
		if j.Kind == "review" {
			err = tx.QueryRow(`SELECT COALESCE((SELECT m.id FROM jobs c JOIN messages m ON m.id=c.source_id WHERE c.task_id=? AND c.project_id=? AND c.agent_id=? AND c.kind='po_continue' AND c.status='completed' AND m.sender=? AND m.recipient=? AND m.role='operator' AND m.room_id=? ORDER BY c.rowid DESC LIMIT 1),'')`, j.Task, j.Project, a.ID, operator, a.ID, directRoom(operator, a.ID)).Scan(&f.note)
			if err != nil {
				return err
			}
		}
		if j.Kind == "review" {
			f.review = j.Source
		}
	}
	if phase == "review" || phase == "accepted" {
		if f.review == "" {
			return fmt.Errorf("%w: original worker review unavailable", errWorkflow)
		}
		if _, err := activeReviewWorker(tx, job{Task: j.Task, Project: j.Project, Kind: "review", Source: f.review}, a.ID); err != nil {
			return err
		}
	}
	if f.ordinal >= 2 {
		return s.escalatePOHold(tx, j, a, phase, "The project owner did not choose a next task action after two follow-up turns. The task needs a decision; no further work was dispatched by the service.", false, false)
	}
	f.ordinal++
	id := newID()
	if err := addMessage(tx, id, "service", a.ID, "system", "Continue the existing task without repeating prior effects.", j.Project, j.Task); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO po_followups(source_id,trigger_id,ordinal,phase,assignment_id,note_id,review_id) VALUES(?,?,?,?,?,?,?)`, id, f.trigger, f.ordinal, f.phase, f.assignment, nullIfEmpty(f.note), nullIfEmpty(f.review)); err != nil {
		return err
	}
	instruction := "Make the next safe decision for this same task using the pinned original assignment and task-scoped operator instructions. When instructions differ, follow the latest one within existing safety and authorization boundaries. Preserve scope, explicit holds, read-only and production restrictions; do not repeat prior effects or assume authorization. If a genuine blocker or deliberate hold prevents progress, use hold_task with its exact reason."
	if len(correction) > 0 {
		instruction += " The prior proposal was rejected before applying it: " + correction[0] + " Correct the stated issue without repeating effects; choose a valid next action or an explicit hold."
		if correction[0] == fmt.Sprintf("%v: operator approval must be requested separately and a denial cannot dispatch work", errWorkflow) {
			instruction += " No actions from that proposal were applied. Put independent dispatch in a separate response, and submit each operator approval request alone. Use current receipts and preserve the authorized scope."
		}
	}
	switch phase {
	case "assigned":
		instruction += " If authorized and actionable, create the required team and use queue_work_item for independent work; use queue_worker only for legacy tasks. Otherwise use hold_task or request_approval."
	case "review":
		instruction += " Review the pinned worker result. Use accept_result then report if accepted, queue_worker for a justified revision, or hold_task/request_approval."
	case "accepted":
		instruction += " The worker result is already accepted: report to Root; do not call accept_result again or dispatch more work."
	}
	return s.enqueue(tx, a, j.Project, j.Task, id, "po_followup", instruction)
}
func (s *Service) escalatePOHold(tx *sql.Tx, j job, a agent, phase, reason string, explicit, rootOwned bool) error {
	if err := moveTask(tx, j, phase, "blocked", a.ID, a.ID); err != nil {
		return err
	}
	if err := workflowNotice(tx, j.Project, "blocked:"+j.ID, "A task could not continue and needs attention."); err != nil {
		return err
	}
	if !explicit {
		var shared string
		err := tx.QueryRow(`SELECT m.text FROM po_explicit_progress p JOIN messages m ON m.id=p.message_id WHERE m.project_id=? AND m.task_id=? AND m.sender=? AND m.recipient=? AND m.role='po' AND m.room_id=? AND length(trim(m.text))>0 ORDER BY m.seq DESC LIMIT 1`, j.Project, j.Task, a.ID, root, directRoom(a.ID, root)).Scan(&shared)
		if errors.Is(err, sql.ErrNoRows) {
			shared, err = s.legacyExhaustionProgress(tx, j, a)
			if err != nil {
				return err
			}
			if shared == "" {
				return nil
			}
		}
		if err != nil {
			return err
		}
		source := newID()
		if err := addMessage(tx, source, a.ID, root, a.Role, shared, j.Project, j.Task); err != nil {
			return err
		}
		manager, err := getAgent(tx, root)
		if err != nil {
			return err
		}
		return s.enqueue(tx, manager, j.Project, j.Task, source, "task_blocked", "The PO follow-through exhausted its bounded attempts. The source is the PO's explicitly Root-addressed task progress. Summarize the recorded blocker and ask only a specific decision actually requested in that source; do not invent missing details or ask a generic question.")
	}
	protected, err := poHasPrivateOperatorInput(tx, j, a)
	if err != nil {
		return err
	}
	rootReason := reason
	if protected && !rootOwned {
		if err := addMessage(tx, newID(), "service", operator, "system", reason, j.Project, j.Task); err != nil {
			return err
		}
		rootReason = "A task is blocked and needs a decision. The operator has the private details; do not infer or disclose them."
	}
	source := newID()
	if err := addMessage(tx, source, a.ID, root, a.Role, rootReason, j.Project, j.Task); err != nil {
		return err
	}
	if explicit {
		assignment, err := s.poAssignmentSource(tx, j, a.ID)
		if err != nil {
			return err
		}
		var note, review string
		switch j.Kind {
		case "po_continue":
			note = j.Source
		case "review":
			review = j.Source
		case "po_followup":
			f, err := readPOFollowup(tx, j.Source)
			if err != nil {
				return err
			}
			note, review = f.note, f.review
		}
		if (phase == "review" || phase == "accepted") && review == "" {
			return fmt.Errorf("%w: held review receipt unavailable", errWorkflow)
		}
		if _, err := tx.Exec(`INSERT INTO task_holds(job_id,task_id,project_id,po_id,phase,assignment_id,note_id,review_id,root_source_id,status) VALUES(?,?,?,?,?,?,?,?,?,'active')`, j.ID, j.Task, j.Project, a.ID, phase, assignment, nullIfEmpty(note), nullIfEmpty(review), source); err != nil {
			return err
		}
	}
	if rootOwned && explicit {
		var used int
		if err := tx.QueryRow(`SELECT count(*) FROM root_hold_decisions WHERE task_id=?`, j.Task).Scan(&used); err != nil {
			return err
		}
		if used == 0 {
			if _, err := tx.Exec(`INSERT INTO root_owned_holds(job_id) VALUES(?)`, j.ID); err != nil {
				return err
			}
		} else {
			rootOwned = false
		}
	}
	manager, err := getAgent(tx, root)
	if err != nil {
		return err
	}
	instruction := "The project owner recorded a blocker. Determine whether an operator decision is actually required; if so, state the exact question and any supplied options, risks and recommendation. Do not invent missing choices or create a new task or assignment."
	if protected && !rootOwned {
		instruction = "The project owner recorded a task blocker with operator-private details. The operator has the exact details. Ask only whether an operator decision is needed; do not infer, reconstruct, or disclose private content."
	}
	if rootOwned {
		instruction = "The project owner explicitly requests a Root-owned decision. If within existing scope and all stops and safeguards, use resolve_hold with this exact task and project and your decision as body; work resumes only through the appointed PO. Otherwise ask the operator a specific question with any supplied options and risks. Do not invent authorization."
	}
	return s.enqueue(tx, manager, j.Project, j.Task, source, "task_blocked", instruction)
}
func (s *Service) poHoldAction(tx *sql.Tx, j job, a agent, action runtimeapi.Action) error {
	if j.Kind != "assignment" && j.Kind != "po_continue" && j.Kind != "review" && j.Kind != "po_followup" {
		return fmt.Errorf("%w: hold requires an active task decision", errWorkflow)
	}
	if action.Project != "" && action.Project != j.Project || (action.Recipient != "" && action.Recipient != root) || strings.TrimSpace(action.Body) == "" || len(action.Body) > 4096 {
		return fmt.Errorf("%w: task hold requires a reason and the current project", errWorkflow)
	}
	var phase, owner string
	var stopped int
	err := tx.QueryRow(`SELECT t.status,t.owner_id,p.stopped FROM tasks t JOIN projects p ON p.id=t.project_id AND p.po_id=? WHERE t.id=? AND p.id=?`, a.ID, j.Task, j.Project).Scan(&phase, &owner, &stopped)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: task hold scope changed", errWorkflow)
	}
	if err != nil {
		return err
	}
	if owner != a.ID || stopped != 0 || (phase != "assigned" && phase != "review" && phase != "accepted") {
		return fmt.Errorf("%w: task hold scope changed", errWorkflow)
	}
	return s.escalatePOHold(tx, j, a, phase, action.Body, true, action.Recipient == root)
}
