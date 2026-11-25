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
func poAssignmentSource(tx *sql.Tx, j job, po string) (string, error) {
	var id string
	var count int
	err := tx.QueryRow(`SELECT count(*),COALESCE(min(m.id),'') FROM jobs a JOIN messages m ON m.id=a.source_id WHERE a.task_id=? AND a.project_id=? AND a.agent_id=? AND a.kind='assignment' AND (a.status='completed' OR a.id=?) AND COALESCE(a.diagnostic,'')='' AND m.task_id=a.task_id AND m.project_id=a.project_id AND m.sender=? AND m.recipient=? AND m.role='root' AND m.room_id=? AND length(trim(m.text))>0`, j.Task, j.Project, po, j.ID, root, po, directRoom(root, po)).Scan(&count, &id)
	if err != nil {
		return "", err
	}
	if count != 1 {
		return "", fmt.Errorf("%w: original PO assignment is unavailable", errWorkflow)
	}
	return id, nil
}
func (s *Service) poFollowThrough(tx *sql.Tx, j job, a agent, result runtimeapi.Result) error {
	if a.Role != "po" || j.Task == "" || (j.Kind != "assignment" && j.Kind != "po_continue" && j.Kind != "review" && j.Kind != "po_followup") {
		return nil
	}
	for _, action := range result.Actions {
		if action.Type != "post_group" && action.Type != "accept_result" {
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
	if err = tx.QueryRow(`SELECT (SELECT count(*) FROM jobs WHERE task_id=? AND id<>? AND status IN ('pending','submitted','running','uncertain')),(SELECT count(*) FROM approvals WHERE task_id=? AND status IN ('pending','denied'))`, j.Task, j.ID, j.Task).Scan(&other, &approvals); err != nil {
		return err
	}
	if other != 0 || approvals != 0 {
		return nil
	}
	f := poFollowup{trigger: j.ID, phase: phase}
	if j.Kind == "po_followup" {
		f, err = readPOFollowup(tx, j.Source)
		if err != nil {
			return err
		}
		if f.phase != phase {
			f.ordinal = 0
		}
		f.phase = phase
	} else {
		f.assignment, err = poAssignmentSource(tx, j, a.ID)
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
		return s.escalatePOHold(tx, j, a, phase, "The project owner did not choose a next task action after two follow-up turns. The task needs a decision; no further work was dispatched by the service.")
	}
	f.ordinal++
	id := newID()
	if err := addMessage(tx, id, "service", a.ID, "system", "Continue the existing task without repeating prior effects.", j.Project, j.Task); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO po_followups(source_id,trigger_id,ordinal,phase,assignment_id,note_id,review_id) VALUES(?,?,?,?,?,?,?)`, id, f.trigger, f.ordinal, f.phase, f.assignment, nullIfEmpty(f.note), nullIfEmpty(f.review)); err != nil {
		return err
	}
	instruction := "Make the next safe decision for this same task using the pinned original assignment and any operator instruction. Preserve scope, explicit holds, read-only and production restrictions; do not repeat prior effects or assume authorization. If a genuine blocker or deliberate hold prevents progress, use hold_task with its exact reason."
	switch phase {
	case "assigned":
		instruction += " If authorized and actionable, use queue_worker; otherwise use hold_task or request_approval."
	case "review":
		instruction += " Review the pinned worker result. Use accept_result then report if accepted, queue_worker for a justified revision, or hold_task/request_approval."
	case "accepted":
		instruction += " The worker result is already accepted: report to Root; do not call accept_result again or dispatch more work."
	}
	return s.enqueue(tx, a, j.Project, j.Task, id, "po_followup", instruction)
}
func (s *Service) escalatePOHold(tx *sql.Tx, j job, a agent, phase, reason string) error {
	if err := moveTask(tx, j, phase, "blocked", a.ID, a.ID); err != nil {
		return err
	}
	if err := workflowNotice(tx, j.Project, "blocked:"+j.ID, "A task could not continue and needs attention."); err != nil {
		return err
	}
	if err := addMessage(tx, newID(), "service", operator, "system", reason, j.Project, j.Task); err != nil {
		return err
	}
	source := newID()
	if err := addMessage(tx, source, a.ID, root, a.Role, "This task is blocked and needs operator attention. The operator has the detailed reason.", j.Project, j.Task); err != nil {
		return err
	}
	manager, err := getAgent(tx, root)
	if err != nil {
		return err
	}
	return s.enqueue(tx, manager, j.Project, j.Task, source, "task_blocked", "The project owner stopped this task pending a decision. The operator has the detailed reason. Report the recorded blocked state without inventing a reason; do not create a new task or assignment.")
}
func (s *Service) poHoldAction(tx *sql.Tx, j job, a agent, action runtimeapi.Action) error {
	if j.Kind != "assignment" && j.Kind != "po_continue" && j.Kind != "review" && j.Kind != "po_followup" {
		return fmt.Errorf("%w: hold requires an active task decision", errWorkflow)
	}
	if action.Project != "" && action.Project != j.Project || action.Recipient != "" || strings.TrimSpace(action.Body) == "" || len(action.Body) > 4096 {
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
	return s.escalatePOHold(tx, j, a, phase, action.Body)
}
