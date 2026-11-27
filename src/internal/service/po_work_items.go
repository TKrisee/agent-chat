package service

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"agentchat/src/internal/runtimeapi"
)

type workItemPlanningError struct{ message string }

func (e *workItemPlanningError) Error() string { return e.message }
func (e *workItemPlanningError) Unwrap() error { return errWorkflow }
func workItemPlanning(message string) error    { return &workItemPlanningError{message: message} }

func parentTaskOwned(tx *sql.Tx, j job, po agent) error {
	var n int
	err := tx.QueryRow(`SELECT count(*) FROM tasks t JOIN projects p ON p.id=t.project_id WHERE t.id=? AND t.project_id=? AND t.owner_id=? AND t.status='assigned' AND p.po_id=? AND p.stopped=0 AND NOT EXISTS(SELECT 1 FROM po_work_items WHERE child_task_id=t.id)`, j.Task, j.Project, po.ID, po.ID).Scan(&n)
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("%w: parent task unavailable", errWorkflow)
	}
	return nil
}
func (s *Service) createProjectWorker(tx *sql.Tx, j job, po agent, action runtimeapi.Action) error {
	if err := parentTaskOwned(tx, j, po); err != nil {
		return err
	}
	if err := validateDisplayName(action.Title); err != nil || len(action.Title) > 100 || action.Body != "" || action.Recipient != "" {
		return workItemPlanning("worker requires an exact name only")
	}
	var count int
	if err := tx.QueryRow("SELECT count(*) FROM agents WHERE project_id=? AND name=?", j.Project, action.Title).Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return workItemPlanning("worker name already exists; use the registered roster")
	}
	id := newID()
	model, err := s.preferredWorkerModel(tx, action.Model)
	if err != nil {
		return err
	}
	if _, err := tx.Exec("INSERT INTO agents VALUES(?,?,'worker',?,?,?)", id, action.Title, j.Project, model, s.opts.Effort); err != nil {
		return err
	}
	if action.Type == "create_one_shot_worker" {
		_, err := tx.Exec("INSERT INTO one_shot_workers(agent_id) VALUES(?)", id)
		return err
	}
	return nil
}
func (s *Service) queuePOWorkItem(tx *sql.Tx, j job, po agent, action runtimeapi.Action) error {
	if err := parentTaskOwned(tx, j, po); err != nil {
		return err
	}
	if strings.TrimSpace(action.Title) == "" || strings.TrimSpace(action.Body) == "" || action.Recipient == "" {
		return workItemPlanning("work item requires title, instructions, and worker")
	}
	var duplicate int
	if err := tx.QueryRow(`SELECT count(*) FROM po_work_items i JOIN tasks c ON c.id=i.child_task_id WHERE i.parent_task_id=? AND lower(trim(c.title))=lower(trim(?))`, j.Task, action.Title).Scan(&duplicate); err != nil {
		return err
	}
	if duplicate != 0 {
		return workItemPlanning("work item title already exists; use the recorded item and do not duplicate work")
	}
	var prerequisites []string
	seen := make(map[string]bool)
	for _, title := range action.DependsOn {
		var id string
		err := tx.QueryRow(`SELECT c.id FROM po_work_items i JOIN tasks c ON c.id=i.child_task_id WHERE i.parent_task_id=? AND c.title=?`, j.Task, title).Scan(&id)
		if errors.Is(err, sql.ErrNoRows) || title == "" {
			return workItemPlanning("dependency must name an existing earlier work item in this parent")
		}
		if err != nil {
			return err
		}
		if seen[id] {
			return workItemPlanning("duplicate work item dependency")
		}
		seen[id] = true
		prerequisites = append(prerequisites, id)
	}
	var worker agent
	var id string
	rows, err := querySlice(tx, `SELECT id FROM agents WHERE project_id=? AND role='worker' AND (id=? OR name=?)`, func(rows *sql.Rows) (string, error) { var id string; e := rows.Scan(&id); return id, e }, j.Project, action.Recipient, action.Recipient)
	if err != nil {
		return err
	}
	if len(rows) != 1 {
		return workItemPlanning("worker must identify one same-project worker")
	}
	id = rows[0]
	var alreadyBound int
	if err := tx.QueryRow(`SELECT count(*) FROM one_shot_workers WHERE agent_id=? AND child_task_id IS NOT NULL`, id).Scan(&alreadyBound); err != nil {
		return err
	}
	if alreadyBound != 0 {
		return workItemPlanning("one-shot worker already has its work item")
	}
	var busy int
	if err := tx.QueryRow(`SELECT count(*) FROM jobs WHERE agent_id=? AND kind='work' AND status='uncertain'`, id).Scan(&busy); err != nil {
		return err
	}
	if busy != 0 {
		return fmt.Errorf("%w: worker has unsettled work", errWorkflow)
	}
	worker, err = getAgent(tx, id)
	if err != nil {
		return err
	}
	child, source := newID(), newID()
	if _, err = tx.Exec(`INSERT INTO tasks(id,project_id,title,status,owner_id) VALUES(?,?,?,'queued',?)`, child, j.Project, action.Title, id); err != nil {
		return err
	}
	if _, err = tx.Exec(`INSERT INTO po_work_items(parent_task_id,child_task_id) VALUES(?,?)`, j.Task, child); err != nil {
		return err
	}
	for _, id := range prerequisites {
		if _, err = tx.Exec(`INSERT INTO po_work_dependencies(child_task_id,prerequisite_task_id) VALUES(?,?)`, child, id); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(`UPDATE one_shot_workers SET child_task_id=? WHERE agent_id=? AND child_task_id IS NULL`, child, id); err != nil {
		return err
	}
	if err = addMessage(tx, source, po.ID, id, "po", action.Body, j.Project, child); err != nil {
		return err
	}
	if err = workflowNotice(tx, j.Project, "work-item:"+child, "The project owner queued a work item for a worker."); err != nil {
		return err
	}
	if err = s.notifyModelDeparture(tx, j.Project, child, action.Category, worker.Model, action.ModelReason); err != nil {
		return err
	}
	return s.enqueue(tx, worker, j.Project, child, source, "work", action.Body)
}
func (s *Service) maybeFollowParent(tx *sql.Tx, j job) error {
	var parent string
	err := tx.QueryRow(`SELECT parent_task_id FROM po_work_items WHERE child_task_id=?`, j.Task).Scan(&parent)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	var childStatus string
	if err = tx.QueryRow(`SELECT status FROM tasks WHERE id=? AND project_id=?`, j.Task, j.Project).Scan(&childStatus); err != nil {
		return err
	}
	if childStatus != "completed" && childStatus != "blocked" {
		return nil
	}
	if _, err = tx.Exec(`INSERT INTO po_parent_outcome_events(job_id,parent_task_id,child_task_id) VALUES(?,?,?) ON CONFLICT(job_id) DO NOTHING`, j.ID, parent, j.Task); err != nil {
		return err
	}
	var pending int
	if err = tx.QueryRow(`SELECT count(*) FROM po_parent_outcome_events WHERE parent_task_id=? AND delivered_source_id IS NULL`, parent).Scan(&pending); err != nil || pending == 0 {
		return err
	}
	var parentStatus, poID string
	var stopped, open, busy int
	err = tx.QueryRow(`SELECT t.status,t.owner_id,p.stopped,(SELECT count(*) FROM po_work_items i JOIN tasks c ON c.id=i.child_task_id WHERE i.parent_task_id=t.id AND c.status IN ('queued','review','accepted') AND NOT (c.status='queued' AND EXISTS(SELECT 1 FROM po_work_dependencies d JOIN tasks prerequisite ON prerequisite.id=d.prerequisite_task_id WHERE d.child_task_id=c.id AND prerequisite.status!='completed'))),(SELECT count(*) FROM jobs q WHERE q.task_id=t.id AND q.status IN ('pending','submitted','running','uncertain'))+(SELECT count(*) FROM approvals ap WHERE ap.task_id=t.id AND ap.status='pending')+(SELECT count(*) FROM task_holds h WHERE h.task_id=t.id AND h.status='active') FROM tasks t JOIN projects p ON p.id=t.project_id AND p.po_id=t.owner_id WHERE t.id=? AND t.project_id=?`, parent, j.Project).Scan(&parentStatus, &poID, &stopped, &open, &busy)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if parentStatus != "assigned" || stopped != 0 || open != 0 || busy != 0 {
		return nil
	}
	var trigger string
	assignment, err := s.poAssignmentSource(tx, job{Task: parent, Project: j.Project}, poID)
	if err != nil {
		return err
	}
	// Each completed parent turn starts a distinct child-work round.
	if err = tx.QueryRow(`SELECT id FROM jobs WHERE task_id=? AND agent_id=? AND kind IN ('assignment','po_continue','po_followup') AND status='completed' ORDER BY rowid DESC LIMIT 1`, parent, poID).Scan(&trigger); err != nil {
		return err
	}
	type outcome struct{ id, line string }
	rows, err := querySlice(tx, `SELECT c.id,c.title,c.status,COALESCE((SELECT m.text FROM messages m WHERE m.task_id=c.id AND m.sender=? AND m.recipient=? ORDER BY m.seq DESC LIMIT 1),'') FROM po_work_items i JOIN tasks c ON c.id=i.child_task_id WHERE i.parent_task_id=? ORDER BY c.rowid`, func(rows *sql.Rows) (outcome, error) {
		var id, title, status, text string
		e := rows.Scan(&id, &title, &status, &text)
		text = strings.ToValidUTF8(text, "�")
		if len(text) > 2000 {
			end := 1997
			for end > 0 && !utf8.RuneStart(text[end]) {
				end--
			}
			text = text[:end] + "…"
		}
		return outcome{id, strings.ToValidUTF8(title, "�") + " [" + status + "]: " + text}, e
	}, poID, poID, parent)
	if err != nil {
		return err
	}
	// Include the triggering child, then the newest other outcomes; retain their
	// original order and leave room for an explicit omission notice.
	selected := make([]bool, len(rows))
	used := 0
	for i, row := range rows {
		if row.id == j.Task && len(row.line)+1 <= 64000-256 {
			selected[i] = true
			used += len(row.line) + 1
			break
		}
	}
	for i := len(rows) - 1; i >= 0; i-- {
		if !selected[i] && used+len(rows[i].line)+1 <= 64000-256 {
			selected[i] = true
			used += len(rows[i].line) + 1
		}
	}
	lines := make([]string, 0, len(rows))
	for i, row := range rows {
		if selected[i] {
			lines = append(lines, row.line)
		}
	}
	summary := "Work item outcomes:\n"
	if omitted := len(rows) - len(lines); omitted > 0 {
		summary += fmt.Sprintf("%d other outcomes omitted from this summary; records remain available.\n", omitted)
	}
	summary += strings.Join(lines, "\n")
	source := newID()
	if err = addMessage(tx, source, "service", poID, "system", summary, j.Project, parent); err != nil {
		return err
	}
	if _, err = tx.Exec(`INSERT INTO po_followups(source_id,trigger_id,ordinal,phase,assignment_id) VALUES(?,?,1,'assigned',?)`, source, trigger, assignment); err != nil {
		return err
	}
	po, err := getAgent(tx, poID)
	if err != nil {
		return err
	}
	if err := s.enqueue(tx, po, j.Project, parent, source, "po_followup", "Review the recorded child work item outcomes. Continue independent authorized work via new work items, or report the completed full scope only after all required child work is complete. Preserve stops and uncertain effects; do not repeat prior work."); err != nil {
		return err
	}
	_, err = tx.Exec(`UPDATE po_parent_outcome_events SET delivered_source_id=? WHERE parent_task_id=? AND delivered_source_id IS NULL`, source, parent)
	return err
}

// A successful dispatch is progress and warrants one coalesced planning turn.
// If the PO has nothing else to dispatch while children run, it may wait.
func (s *Service) followWorkPlanning(tx *sql.Tx, j job, po agent) error {
	if err := parentTaskOwned(tx, j, po); err != nil {
		return err
	}
	var busy int
	if err := tx.QueryRow(`SELECT count(*) FROM jobs WHERE task_id=? AND id<>? AND kind='po_followup' AND status IN ('pending','submitted','running')`, j.Task, j.ID).Scan(&busy); err != nil {
		return err
	}
	if busy != 0 {
		return nil
	}
	assignment, err := s.poAssignmentSource(tx, j, po.ID)
	if err != nil {
		return err
	}
	source := newID()
	if err = addMessage(tx, source, "service", po.ID, "system", "A work item was queued. Plan other independent authorized work now, or wait for current outcomes.", j.Project, j.Task); err != nil {
		return err
	}
	if _, err = tx.Exec(`INSERT INTO po_followups(source_id,trigger_id,ordinal,phase,assignment_id) VALUES(?,?,1,'assigned',?)`, source, j.ID, assignment); err != nil {
		return err
	}
	if _, err = tx.Exec(`INSERT INTO po_planning_wakes(source_id) VALUES(?)`, source); err != nil {
		return err
	}
	return s.enqueue(tx, po, j.Project, j.Task, source, "po_followup", "Continue planning the original parent assignment. Queue other independent work items if ready; otherwise wait for current child outcomes. Never repeat uncertain effects or bypass stops.")
}
