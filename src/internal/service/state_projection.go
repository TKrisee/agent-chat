package service

import (
	"database/sql"
	"strings"
	"time"
)

// A nil selection is the complete compatibility snapshot. Delta selections
// use primary keys and skip queries for entity types with no changes.
type stateSelection map[string][]string

func queryStateRows[T any](tx *sql.Tx, selected stateSelection, kind, predicate, order, query string, scan func(*sql.Rows) (T, error), args ...any) ([]T, error) {
	if selected != nil {
		ids := selected[kind]
		if len(ids) == 0 {
			return []T{}, nil
		}
		query += predicate + " IN (?" + strings.Repeat(",?", len(ids)-1) + ")"
		for _, id := range ids {
			args = append(args, id)
		}
	}
	return querySlice(tx, query+" ORDER BY "+order, scan, args...)
}

func (s *Service) projectState(tx *sql.Tx, selected stateSelection, view *State) error {
	var err error
	view.Messages, err = queryStateRows(tx, selected, "message", " AND m.id", "m.seq", `SELECT m.seq,m.id,m.sender,m.recipient,m.role,m.text,m.at,
		COALESCE(m.task_id,''),COALESCE(m.project_id,''),COALESCE(reply.parent_id,''),receipt.read_at,m.room_id,COALESCE(delivery.approval_id,''),
		CASE WHEN backlog.origin_id IS NOT NULL THEN 'backlog' WHEN recovery.request_message_id IS NOT NULL THEN 'recover_report' ELSE '' END,
		COALESCE(backlog.project_id,relay.project_id,''),COALESCE(relay.task_id,'')
		FROM messages m
		LEFT JOIN approval_deliveries delivery ON delivery.message_id=m.id
		LEFT JOIN message_replies reply ON reply.message_id=m.id
		LEFT JOIN operator_message_reads receipt ON receipt.message_id=m.id
		LEFT JOIN root_backlog_grants backlog ON backlog.origin_id=m.id
			AND m.sender='operator' AND m.recipient='root' AND m.role='operator' AND m.room_id='dm:operator:root'
		LEFT JOIN project_report_review_attempts recovery ON recovery.request_message_id=m.id
			AND m.sender='operator' AND m.recipient='root' AND m.role='operator' AND m.room_id='dm:operator:root'
		LEFT JOIN jobs relay ON relay.id=recovery.relay_job_id
            WHERE m.room_id IS NOT NULL AND ((m.room_id LIKE 'dm:%' AND (m.sender='operator' OR m.recipient='operator'))
              OR (m.room_id='group:'||m.project_id AND EXISTS (SELECT 1 FROM projects p WHERE p.id=m.project_id)))`, func(rows *sql.Rows) (Message, error) {
		var m Message
		var command MessageCommand
		var millis int64
		var readMillis sql.NullInt64
		err := rows.Scan(&m.Position, &m.ID, &m.Sender, &m.Recipient, &m.Role, &m.Text, &millis,
			&m.Task, &m.Project, &m.ReplyTo, &readMillis, &m.RoomID, &m.ApprovalID, &command.Kind, &command.Project, &command.Task)
		if command.Kind != "" {
			m.Command = &command
		}
		m.At = time.UnixMilli(millis).UTC()
		if readMillis.Valid {
			at := time.UnixMilli(readMillis.Int64).UTC()
			m.OperatorReadAt = &at
		}
		m.Attachments = []AttachmentRef{}
		return m, err
	})
	if err != nil {
		return err
	}
	type mention struct{ message, agent string }
	mentions, err := queryStateRows(tx, selected, "message", " WHERE message_id", "message_id,agent_id", "SELECT message_id,agent_id FROM message_mentions", func(rows *sql.Rows) (mention, error) {
		var m mention
		err := rows.Scan(&m.message, &m.agent)
		return m, err
	})
	if err != nil {
		return err
	}
	scopeDetails, err := queryStateRows(tx, selected, "message", " WHERE message_id", "message_id", "SELECT message_id FROM ("+scopeDetailsSQL+")", func(rows *sql.Rows) (string, error) { var id string; err := rows.Scan(&id); return id, err })
	if err != nil {
		return err
	}
	scopeEligible := make(map[string]bool, len(scopeDetails))
	for _, d := range scopeDetails {
		scopeEligible[d] = true
	}
	byMentionedMessage := make(map[string][]string)
	for _, m := range mentions {
		byMentionedMessage[m.message] = append(byMentionedMessage[m.message], m.agent)
	}
	type ref struct {
		AttachmentRef
		Message string
	}
	refs, err := queryStateRows(tx, selected, "message", " WHERE m.message_id", "a.id", `SELECT a.id,m.message_id,a.name,a.media_type
		FROM message_attachments m JOIN attachments a ON a.id=m.attachment_id`, func(rows *sql.Rows) (ref, error) {
		var a ref
		err := rows.Scan(&a.ID, &a.Message, &a.Name, &a.MediaType)
		a.URL = "/api/attachments/" + a.ID
		return a, err
	})
	if err != nil {
		return err
	}
	byMessage := make(map[string][]AttachmentRef)
	for _, a := range refs {
		byMessage[a.Message] = append(byMessage[a.Message], a.AttachmentRef)
	}
	for i := range view.Messages {
		if scopeEligible[view.Messages[i].ID] {
			view.Messages[i].NoticeDetail = "scope_request"
		}
		view.Messages[i].MentionIDs = byMentionedMessage[view.Messages[i].ID]
		if refs := byMessage[view.Messages[i].ID]; len(refs) > 0 {
			view.Messages[i].Attachments = refs
		}
	}
	view.Projects, err = queryStateRows(tx, selected, "project", " WHERE p.id", "p.id", `SELECT p.id,p.name,p.po_id,p.stopped,COALESCE(g.objective,''),COALESCE(g.status,''),COALESCE((SELECT path FROM project_workspace_roots w WHERE w.project_id=p.id),''),(SELECT COUNT(*) FROM jobs j
		WHERE j.project_id=p.id AND j.status IN ('submitted','running','uncertain'))
		FROM projects p LEFT JOIN project_goals g ON g.project_id=p.id`, func(rows *sql.Rows) (Project, error) {
		var p Project
		var stopped bool
		err := rows.Scan(&p.ID, &p.Name, &p.PO, &stopped, &p.GoalObjective, &p.GoalStatus, &p.WorkspaceRoot, &p.UnsettledEffects)
		p.Status = "running"
		if stopped {
			p.Status = "stopped"
			if p.UnsettledEffects != 0 {
				p.Status = "stopping"
			}
		}
		return p, err
	})
	if err != nil {
		return err
	}
	view.Tasks, err = queryStateRows(tx, selected, "task", " WHERE t.id", "t.rowid", `SELECT t.rowid,t.id,t.project_id,t.title,t.status,t.owner_id,EXISTS(SELECT 1 FROM po_work_dependencies d JOIN tasks p ON p.id=d.prerequisite_task_id WHERE d.child_task_id=t.id AND p.status!='completed') AND t.status='queued' FROM tasks t`, func(rows *sql.Rows) (Task, error) {
		var t Task
		err := rows.Scan(&t.Position, &t.ID, &t.Project, &t.Title, &t.Status, &t.Owner, &t.WaitingOnDependencies)
		return t, err
	})
	if err != nil {
		return err
	}
	for i := range view.Tasks {
		if view.Tasks[i].Status == "blocked" {
			h, found, holdErr := s.activeTaskHold(tx, view.Tasks[i].ID)
			if holdErr != nil {
				return holdErr
			}
			if found {
				view.Tasks[i].ResolveHoldEligible, holdErr = s.holdCanResolve(tx, h)
				if holdErr != nil {
					return holdErr
				}
			}
			view.Tasks[i].RetryHandoffEligible, err = handoffEligible(tx, view.Tasks[i].ID, "blocked", "", true)
			if err != nil {
				return err
			}
		}
		view.Tasks[i].ContinuePOEligible, err = s.continuationEligible(tx, view.Tasks[i].ID, view.Tasks[i].Status)
		if err != nil {
			return err
		}
	}
	view.Agents, err = queryStateRows(tx, selected, "agent", " WHERE a.id", "a.rowid", `SELECT a.rowid,a.id,a.name,a.role,COALESCE(a.project_id,''),a.model,
		CASE WHEN p.stopped=1 THEN 'stopped' WHEN EXISTS(SELECT 1 FROM jobs j
		WHERE j.agent_id=a.id AND j.status IN ('submitted','running')) THEN 'busy' ELSE 'idle' END,
		CASE WHEN a.role='po' AND a.id=po.id THEN ? WHEN a.role='worker' THEN COALESCE(po.id,'') ELSE '' END,
		EXISTS(SELECT 1 FROM one_shot_workers o WHERE o.agent_id=a.id),
		EXISTS(SELECT 1 FROM one_shot_workers o JOIN tasks t ON t.id=o.child_task_id AND t.status='completed' WHERE o.agent_id=a.id
		 AND NOT EXISTS(SELECT 1 FROM jobs j WHERE (j.agent_id=a.id OR j.task_id=t.id) AND j.status IN ('pending','submitted','running','uncertain'))
		 AND NOT EXISTS(SELECT 1 FROM approvals x WHERE x.task_id=t.id AND x.status='pending')
		 AND NOT EXISTS(SELECT 1 FROM task_holds h WHERE h.task_id=t.id AND h.status='active'))
		FROM agents a LEFT JOIN projects p ON p.id=a.project_id
		LEFT JOIN agents po ON po.id=p.po_id AND po.role='po' AND po.project_id=p.id`, func(rows *sql.Rows) (Agent, error) {
		var a Agent
		err := rows.Scan(&a.Position, &a.ID, &a.Name, &a.Role, &a.Project, &a.Model, &a.Status, &a.Parent, &a.OneShot, &a.OneShotRetired)
		return a, err
	}, root)
	if err != nil {
		return err
	}
	view.Approvals, err = queryStateRows(tx, selected, "approval", " WHERE a.id", "a.rowid", `SELECT a.rowid,a.id,COALESCE(a.project_id,''),COALESCE(a.task_id,''),
		a.requested_by,a.title,a.body,a.status,COALESCE(n.note,'') FROM approvals a LEFT JOIN approval_decision_notes n ON n.approval_id=a.id`, func(rows *sql.Rows) (Approval, error) {
		var a Approval
		err := rows.Scan(&a.Position, &a.ID, &a.Project, &a.Task, &a.RequestedBy, &a.Title, &a.Body, &a.Status, &a.DecisionNote)
		return a, err
	})
	if err != nil {
		return err
	}
	view.Jobs, err = queryStateRows(tx, selected, "job", " WHERE j.id", "j.rowid", `SELECT j.rowid,j.id,j.agent_id,COALESCE(j.project_id,''),COALESCE(j.task_id,''),
		j.kind,j.source_id,j.status,COALESCE(j.diagnostic,''),
		EXISTS(SELECT 1 FROM scope_draft_confirmations c WHERE c.review_job_id=j.id AND c.task_id=j.task_id AND c.confirmed_by='operator' AND c.effects_unknown_ack=1 AND j.kind='scope_review' AND j.status='uncertain') FROM jobs j`, func(rows *sql.Rows) (Job, error) {
		var j Job
		err := rows.Scan(&j.Position, &j.ID, &j.Agent, &j.Project, &j.Task, &j.Kind, &j.Source, &j.Status, &j.Diagnostic, &j.Superseded)
		return j, err
	})
	return err
}
