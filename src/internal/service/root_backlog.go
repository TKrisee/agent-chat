package service

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"agentchat/src/internal/runtimeapi"
)

func isRootBacklogIntake(text string) bool {
	header, _, _ := strings.Cut(text, "\n")
	fields := strings.Fields(header)
	return len(fields) != 0 && fields[0] == "/backlog"
}

func recordRootBacklogGrant(tx *sql.Tx, source, text string) error {
	header, scope, hasScope := strings.Cut(text, "\n")
	fields := strings.Fields(header)
	scope = strings.TrimSpace(scope)
	if len(fields) != 2 || !projectID.MatchString(fields[1]) || !hasScope || scope == "" {
		return fmt.Errorf("%w: write /backlog <project-id> followed by the continuing scope on a new line", errInvalid)
	}
	project := fields[1]
	var po string
	err := tx.QueryRow(`SELECT p.po_id FROM projects p JOIN project_goals g ON g.project_id=p.id AND g.status='active'
		JOIN agents a ON a.id=p.po_id AND a.project_id=p.id AND a.role='po'
		WHERE p.id=? AND p.stopped=0`, project).Scan(&po)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: backlog requires an open project with an active goal and appointed PO", errConflict)
	}
	if err != nil {
		return err
	}
	// An explicit intake can bind a legacy goal. A goal alone never creates a grant.
	if _, err := tx.Exec(`INSERT INTO project_goal_revisions(project_id,revision) VALUES(?,?) ON CONFLICT(project_id) DO NOTHING`, project, newID()); err != nil {
		return err
	}
	grant, err := tx.Exec(`INSERT INTO root_backlog_grants(origin_id,project_id,po_id,goal_revision,scope)
		SELECT ?,project_id,?,revision,? FROM project_goal_revisions WHERE project_id=?
		ON CONFLICT(project_id,goal_revision) DO NOTHING`, source, po, scope, project)
	if err != nil {
		return err
	}
	count, err := grant.RowsAffected()
	if err == nil && count != 1 {
		return fmt.Errorf("%w: this goal revision already has an authorized backlog intake", errConflict)
	}
	return err
}

func validateBacklogIntakeResult(tx *sql.Tx, j job, actions []runtimeapi.Action) error {
	if j.Agent != root || j.Kind != "operator_message" || len(actions) == 0 {
		return nil
	}
	var project string
	var current bool
	err := tx.QueryRow(`SELECT project_id,EXISTS(SELECT 1 FROM authorized_root_backlog_intakes a JOIN projects p ON p.id=a.project_id AND p.stopped=0 WHERE a.origin_id=g.origin_id)
		FROM root_backlog_grants g WHERE g.origin_id=?`, j.Source).Scan(&project, &current)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if !current || len(actions) != 1 || actions[0].Type != "delegate" || actions[0].Project != project {
		return fmt.Errorf("%w: backlog intake permits one delegation under its current authorized project goal", errWorkflow)
	}
	return validateBacklogTitle(tx, project, actions[0].Title, "")
}

func validateBacklogScopeDecision(tx *sql.Tx, j job, title string) error {
	var current bool
	err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM authorized_root_backlog_intakes a
		WHERE a.origin_id=o.message_id AND a.project_id=t.project_id AND a.po_id=t.owner_id)
		FROM task_origins o JOIN root_backlog_grants g ON g.origin_id=o.message_id
		JOIN tasks t ON t.id=o.task_id WHERE t.id=? AND t.project_id=?`, j.Task, j.Project).Scan(&current)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if !current {
		return fmt.Errorf("%w: backlog scope authorization is no longer current", errWorkflow)
	}
	return validateBacklogTitle(tx, j.Project, title, j.Task)
}

func validateBacklogTitle(tx *sql.Tx, project, title, task string) error {
	var duplicate bool
	if err := tx.QueryRow(`SELECT EXISTS(SELECT 1 FROM tasks WHERE project_id=? AND id<>?
		AND lower(trim(title))=lower(trim(?)))`, project, task, title).Scan(&duplicate); err != nil {
		return err
	}
	if duplicate {
		return fmt.Errorf("%w: task title already exists", errWorkflow)
	}
	return nil
}

func recordRootBacklogCandidate(tx *sql.Tx, j job, report string) error {
	_, err := tx.Exec(`INSERT INTO root_backlog_candidates(parent_task_id,report_message_id,objective)
		SELECT t.id,?,a.objective FROM tasks t JOIN task_origins o ON o.task_id=t.id
		JOIN authorized_root_backlog_intakes a ON a.origin_id=o.message_id AND a.project_id=t.project_id AND a.po_id=t.owner_id
		WHERE t.id=? AND t.project_id=? AND t.status='completed' AND t.owner_id=?
		ON CONFLICT(parent_task_id) DO NOTHING`, report, j.Task, j.Project, j.Agent)
	return err
}

// queueRootBacklog accepts only completed parent tasks from an explicit operator
// intake under the same still-active goal revision and appointed PO.
func (s *Service) queueRootBacklog(tx *sql.Tx, project string) error {
	if project == "" || !s.runtimeReady() {
		return nil
	}
	type candidate struct{ task, po, origin, objective, scope, report string }
	items, err := querySlice(tx, `SELECT t.id,a.po_id,a.origin_id,a.objective,a.scope,m.text
		FROM tasks t JOIN task_origins o ON o.task_id=t.id
		JOIN authorized_root_backlog_intakes a ON a.origin_id=o.message_id AND a.project_id=t.project_id AND a.po_id=t.owner_id
		JOIN projects p ON p.id=a.project_id AND p.stopped=0
		JOIN root_backlog_candidates c ON c.parent_task_id=t.id AND c.objective=a.objective
		JOIN jobs r ON r.source_id=c.report_message_id AND r.task_id=t.id AND r.project_id=a.project_id AND r.agent_id='root' AND r.kind='project_report'
		JOIN messages m ON m.id=r.source_id AND m.task_id=t.id AND m.project_id=a.project_id AND m.sender=a.po_id AND m.recipient='root' AND m.role='po'
		WHERE a.project_id=? AND t.status='completed'
		  AND NOT EXISTS(SELECT 1 FROM root_backlog_wakes w WHERE w.parent_task_id=t.id)
		  AND NOT EXISTS(SELECT 1 FROM approvals ap WHERE ap.task_id=t.id AND ap.status='pending')
		  AND NOT EXISTS(SELECT 1 FROM task_holds h WHERE h.task_id=t.id AND h.status='active')
		  AND NOT EXISTS(SELECT 1 FROM jobs x WHERE x.task_id=t.id AND x.status='uncertain')
		ORDER BY t.rowid`, func(rows *sql.Rows) (candidate, error) {
		var c candidate
		e := rows.Scan(&c.task, &c.po, &c.origin, &c.objective, &c.scope, &c.report)
		return c, e
	}, project)
	if err != nil {
		return err
	}
	for _, c := range items {
		source := newID()
		if err := addMessage(tx, source, "service", root, "system", "Review the completed project assignment and authorized remaining goal.", project, c.task); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO root_backlog_wakes(parent_task_id,source_id,origin_id,project_id,po_id,objective) VALUES(?,?,?,?,?,?)`, c.task, source, c.origin, project, c.po, c.objective); err != nil {
			return err
		}
		a, err := getAgent(tx, root)
		if err != nil {
			return err
		}
		instructions := fmt.Sprintf("The operator explicitly authorized ongoing work through the pinned intake. The active goal is context: %q. The exact authorized continuing scope is %q. The completed PO report is %q. Return no action unless one distinct next assignment fits that scope and is ready. Do not broaden authority, repeat work, or bypass stops, approvals, holds, or uncertain effects.", c.objective, c.scope, c.report)
		if err := s.enqueue(tx, a, project, c.task, source, "root_backlog", instructions); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) applyRootBacklog(tx *sql.Tx, a agent, j job, result runtimeapi.Result, files []attachmentData) error {
	if a.ID != root || j.Task == "" || len(result.Actions) > 1 || len(files) != 0 || len(result.Attachments) != 0 {
		return fmt.Errorf("%w: invalid backlog assessment", errWorkflow)
	}
	var origin, po string
	err := tx.QueryRow(`SELECT origin_id,po_id FROM eligible_root_backlog_wakes
		WHERE source_id=? AND parent_task_id=? AND project_id=?`, j.Source, j.Task, j.Project).Scan(&origin, &po)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: backlog authorization unavailable", errWorkflow)
	}
	if err != nil {
		return err
	}
	if len(result.Actions) == 0 {
		return nil
	}
	action := result.Actions[0]
	if action.Type != "delegate" || action.Project != j.Project || (action.Recipient != "" && action.Recipient != po) || action.Task != "" || strings.TrimSpace(action.Title) == "" || strings.TrimSpace(action.Body) == "" {
		return fmt.Errorf("%w: backlog requires one same-project delegation", errWorkflow)
	}
	if err := validateBacklogTitle(tx, j.Project, action.Title, ""); err != nil {
		return err
	}
	delegation := j
	delegation.Source = origin
	if err := s.rootAssignment(tx, a, delegation, action); err != nil {
		return err
	}
	var task string
	if err := tx.QueryRow(`SELECT id FROM tasks WHERE project_id=? AND title=?`, j.Project, action.Title).Scan(&task); err != nil {
		return err
	}
	_, err = tx.Exec(`UPDATE root_backlog_wakes SET next_task_id=? WHERE source_id=? AND next_task_id IS NULL`, task, j.Source)
	return err
}
