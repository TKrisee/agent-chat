package service

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"

	"agentchat/src/internal/runtimeapi"
)

const rootGuide = "You are Root. Turn the operator's development request into a project outcome in your own words. Preserve scope and constraints, specify evidence of acceptance, and ask the PO which relevant project context, dependencies and current work to check. For an existing project emit delegate with the project ID and draft title/body. For a new project emit create_project with a new lowercase project ID and draft title/body. Leave recipient empty; the service appoints the PO. The PO first returns one scope assessment. On scope_decision, use that assessment and the original operator request to emit one delegate for the same project with the final title/body. Leave recipient empty or set it to the exact appointed PO ID. Include the agreed outcome, constraints, verified context or remaining assumptions, and acceptance criteria the PO can use to assign work. If a necessary decision or prerequisite still blocks execution, explain it in operator-facing text with no action. Do not start another scope round. Status or visibility questions about an existing project are read-only: prefer the timestamped app-owned status snapshot over older chat, report only recorded facts or uncertainty, and do not create a project or assignment. When the snapshot says po_continuation_available=true, the operator can send /continue <instruction> in that project’s group chat to continue the existing assignment without a new scope round; if several tasks are eligible, use /continue <full-task-UUID> <instruction>. Reading a snapshot does not contact a project owner; no supported action asks a PO for status, so say so rather than creating an assignment as a status ping. When an active hold is shown and the operator clearly authorizes continuing that exact task, emit only resolve_hold with the exact task and project IDs, body copied exactly from the current operator message, and empty title and recipient. Use the linked Root escalation as context; if task or authorization is ambiguous, ask which task instead. The action resumes a PO decision, never directly dispatches work. Other replies can be text. An external decision can use request_approval. Use stop_project or resume_project with the exact project ID when the operator directs it. A project_report is a PO's completed report to relay to the operator. Its reviewed input attachments are forwarded automatically; do not re-upload input attachment paths."
const poGuide = "You are the project owner. On scope_review assess the original request and context for Root; do not dispatch. After the finalized assignment, plan the full authorized scope, create a requirement-specific team using create_worker, and delegate distinct work items to registered same-project workers with queue_work_item. Specify each worker by exact name or ID, with a clear title and brief. You coordinate and review; do not perform leaf implementation yourself. The parent task stays open through multiple rounds. Review each child worker receipt with accept_result then report on that child; the service returns child outcomes for your next parent turn. Continue independent work after a blocker without repeating uncertain effects. Only report the parent to Root once every required work item is completed and the full scope is delivered. Preserve production stops, read-only restrictions, explicit holds and authorization boundaries. For a genuine blocker use hold_task with the exact reason, or request_approval only when operator approval is actually required. Existing queue_worker remains available for legacy tasks. Use post_group for team working chatter and coordination; do not mention the operator for routine progress. Direct questions or decisions needing operator input through the existing hold or approval workflow, and send the final report to Root. Reply privately when the operator directly messages you. Never publish private handoff content."

const workerGuide = "You are a worker. Complete the assigned task in your project workspace and return a factual text result. Use post_group for ordinary project updates, questions and decisions: set project to your exact project ID, body to the shared message, and leave title and recipient empty. Posts commit at the end of your turn and do not wake recipients; keep private handoffs private. Attach regular workspace files by their absolute paths. Role and scope come from the service."
const poChatGuide = "You are a project owner in a direct conversation with the operator. Answer in text; this is private to you and the operator. Do not post it to the project group or dispatch work. You may attach regular files from your workspace."
const workerChatGuide = "You are a project worker in a direct conversation with the operator. Answer the operator in text. Use relevant project work history for context. This message is independent of task assignments, so do not start or change a task or emit orchestration actions. You may attach regular files from your workspace."
const groupChatGuide = "The operator addressed you in the shared project group conversation by mention, direct reply, or an ordinary PO-directed group message. Your text and any attached regular workspace files will be shared with the project group. Use the shared history for context. Answer without orchestration actions or relays."

// workflowDefaults are app-owned guidance, subordinate to the task, project
// instructions, and the runtime's permission and safety requirements.
const workflowDefaults = `When applicable to the assigned work, follow these defaults:
- Agree on the smallest useful acceptance criteria and prove the normal user workflow early when practical.
- At the first genuine blocker, report the exact failed prerequisite or error, its impact, the responsible owner, and the next viable permitted alternative. Continue independent backlog work.
- For the same blocked item, use meaningfully changed, permitted approaches across at most two further failed task turns. If both fail, stop only that item and escalate through your immediate parent (Root to the operator), with the attempts and decision or input needed. If no permitted alternative exists, escalate promptly instead of running empty turns.
- Respect authorization boundaries, denials, stops, and uncertain-effect fences; never replay an effect without proof and authorization.
- Stop repeating checks once the relevant workflow passes and required checks are complete. Preserve working patches and receipts rather than discarding or restarting them.`

func agentGuide(role, kind string, shared bool) string {
	guide := workerGuide
	switch role {
	case "root":
		guide = rootGuide
		guide += "\nFor project reports and escalations, filter and summarize the facts for the operator; forward concrete questions or choices that require the operator's decision. Do not relay routine team chatter or private PO conversations."
		guide += "\nOnly when the operator explicitly asks to set a PO goal, emit set_project_goal with the exact existing project ID, objective in body, and empty title and recipient. To change an existing goal only on explicit operator instruction, emit set_project_goal_status with the project ID and body exactly active, paused, or complete. These actions do not dispatch work."
	case "po":
		guide = poGuide + " Submit each operator approval request alone, with no other actions in that response. Send independent assignments in a separate valid response so unrelated work can continue; never dispatch work on an approval denial."
		if kind == "operator_message" {
			guide = poChatGuide
		}
	case "worker":
		if kind == "operator_message" {
			guide = workerChatGuide
		}
	}
	if kind == groupMessageKind {
		guide = groupChatGuide
	} else if shared {
		guide += "\nMessages marked with a project group recipient are shared context. Follow the instructions for the current task or conversation."
	}
	return guide + "\n\n" + workflowDefaults
}

func (s *Service) enqueue(tx *sql.Tx, a agent, project, task, source, kind, instructions string) error {
	if !s.runtimeReady() {
		return fmt.Errorf("%w: Codex setup is not ready", errConflict)
	}
	guide := agentGuide(a.Role, kind, project != "" && project == a.Project)
	if a.Role == "po" && kind != "operator_message" && kind != groupMessageKind {
		guide += "\nUse explicit task-scoped Root escalation or report_progress to share task decisions and progress. You may quote task-sourced private wording when it is needed for a decision, or summarize it; omit unrelated material. Ordinary operator direct messages remain private unless intentionally shared for the task."
	}
	if a.Role == "po" && project != "" && task != "" && kind != "operator_message" && kind != groupMessageKind {
		var objective string
		err := tx.QueryRow(`SELECT g.objective FROM project_goals g JOIN projects p ON p.id=g.project_id WHERE g.project_id=? AND p.po_id=? AND g.status='active'`, project, a.ID).Scan(&objective)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil {
			guide += "\nCurrent active PO goal (project context, subject to the current assignment): " + objective
		}
		roster, err := querySlice(tx, `SELECT id,name FROM agents a WHERE project_id=? AND role='worker' AND NOT EXISTS(SELECT 1 FROM one_shot_workers o WHERE o.agent_id=a.id AND o.child_task_id IS NOT NULL) ORDER BY name,id`, func(rows *sql.Rows) (string, error) {
			var id, name string
			err := rows.Scan(&id, &name)
			return name + " (" + id + ")", err
		}, project)
		if err != nil {
			return err
		}
		guide += "\nRegistered project workers: " + strings.Join(roster, ", ")
		outcomes, err := querySlice(tx, `SELECT c.title,c.status,COALESCE((SELECT m.text FROM messages m WHERE m.task_id=c.id AND m.sender=? AND m.recipient=? AND m.role='po' ORDER BY m.seq DESC LIMIT 1),'') FROM po_work_items i JOIN tasks c ON c.id=i.child_task_id WHERE i.parent_task_id=? ORDER BY c.rowid`, func(rows *sql.Rows) (string, error) {
			var title, status, detail string
			err := rows.Scan(&title, &status, &detail)
			runes := []rune(detail)
			if len(runes) > 1500 {
				detail = string(runes[:1500]) + "…"
			}
			return title + " [" + status + "]: " + detail, err
		}, a.ID, a.ID, task)
		if err != nil {
			return err
		}
		if len(outcomes) > 0 {
			guide += "\nRecorded child work items:\n" + strings.Join(outcomes, "\n")
		}
	}
	if a.Role == "root" {
		guide += "\nRoot-bound task updates and escalations may contain private operator-to-PO material intentionally shared for a decision. Before any operator-facing response, include only the information needed for that response; do not relay unrelated private conversation."
	}
	if a.Role == "root" && (kind == "operator_message" || kind == "approval_approved") {
		projects, err := querySlice(tx, "SELECT id FROM projects ORDER BY id", func(rows *sql.Rows) (string, error) {
			var id string
			err := rows.Scan(&id)
			return id, err
		})
		if err != nil {
			return err
		}
		guide += "\nExisting project IDs: " + strings.Join(projects, ", ")
	}

	workspace := s.workspace("_root")
	if project != "" {
		workspace = s.workspace(project)
	}
	var parent string
	if kind == "operator_message" || kind == groupMessageKind {
		err := tx.QueryRow("SELECT parent_id FROM message_replies WHERE message_id=?", source).Scan(&parent)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	if parent != "" {
		guide += "\nThe operator replied to message " + parent + ". Its exact text and attachments are included in the context."
	}
	history, err := s.runtimeContext(tx, a, project, task, parent, kind, source)
	if err != nil {
		return err
	}
	id := newID()
	if kind == "work" && project != "" {
		var root string
		err := tx.QueryRow("SELECT path FROM project_workspace_roots WHERE project_id=?", project).Scan(&root)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err == nil {
			workspace = filepath.Join(root, id)
			if _, err := tx.Exec("INSERT INTO project_workspace_jobs(job_id,path) VALUES(?,?)", id, workspace); err != nil {
				return err
			}
			guide += "\nOperator-authorized fresh job workspace: " + workspace + ". This is the default isolated workspace, not a request to copy a repository. Reuse an existing worktree only when your PO explicitly assigns its exact authorized path for this same worker, repository and work lane, the sandbox permits it, and no concurrent owner or unresolved work would be disturbed. Preserve dirty, unmerged and uncertain work. All other job workspaces remain out of scope."
		}
	}
	req := runtimeapi.Request{ID: id, AgentID: a.ID, Role: a.Role, ProjectID: project, TaskID: task, Kind: kind,
		Workspace: workspace, Model: a.Model, Effort: a.Effort, Context: history,
		Instructions: guide + "\n\nEvent: " + kind + "\n" + instructions}
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	_, err = tx.Exec("INSERT INTO jobs (id,agent_id,project_id,task_id,source_id,kind,status,request,next_at) VALUES (?,?,?,?,?,?,'pending',?,?)",
		req.ID, a.ID, nullIfEmpty(project), nullIfEmpty(task), source, kind, body, now())
	if err == nil && kind == "work" {
		_, err = tx.Exec("INSERT INTO job_scratch(job_id) VALUES(?)", id)
	}
	return err
}

func (s *Service) runtimeContext(tx *sql.Tx, a agent, project, task, parent, kind string, source ...string) ([]runtimeapi.Message, error) {
	const privateColumns = "id,sender,role,text,seq,'' AS group_recipient,recipient,room_id"
	const shared = `SELECT m.id,m.sender,m.role,m.text,m.seq,m.recipient AS group_recipient,m.recipient,m.room_id FROM messages m
		WHERE m.project_id=? AND m.recipient=? AND m.room_id=m.recipient AND m.task_id IS NULL
		AND ((m.sender=? AND m.role='operator') OR ` + groupMemberSender + `) ORDER BY m.seq DESC LIMIT `
	const notice = `SELECT m.id,m.sender,m.role,m.text,m.seq,m.recipient AS group_recipient,m.recipient,m.room_id FROM messages m
        WHERE m.project_id=? AND m.recipient=? AND m.room_id=m.recipient AND m.task_id IS NULL AND ` + groupNoticeSender + ` ORDER BY m.seq DESC LIMIT 12`
	groupRecipient := groupPrefix + project
	sharedArgs := []any{project, groupRecipient, operator}
	query := "SELECT " + privateColumns + " FROM messages WHERE task_id=? ORDER BY seq DESC LIMIT 24"
	args := []any{task}
	if kind == groupMessageKind {
		query = shared + "24"
		args = sharedArgs
	} else if task == "" {
		query = "SELECT " + privateColumns + " FROM messages WHERE (sender=? OR recipient=?) AND project_id IS NULL ORDER BY seq DESC LIMIT 24"
		args = []any{a.ID, a.ID}
		if project != "" {
			query = "SELECT " + privateColumns + " FROM messages WHERE project_id=? AND recipient<>? AND (sender=? OR recipient=?) ORDER BY seq DESC LIMIT 24"
			args = []any{project, groupRecipient, a.ID, a.ID}
		}
	}
	query = "SELECT * FROM (" + query + ")"
	if a.ID == root && kind == "operator_message" && project == "" {
		// PO progress is private to Root; preserve project/task identity in the runtime view.
		query += ` UNION SELECT * FROM (SELECT id,sender,role,('[Project '||project_id||'; task '||task_id||'] '||text) AS text,seq,'' AS group_recipient,recipient,room_id FROM messages WHERE id IN (SELECT message_id FROM po_progress_notes) AND recipient=? AND role='po' AND room_id='dm:'||CASE WHEN sender<recipient THEN sender||':'||recipient ELSE recipient||':'||sender END ORDER BY seq DESC LIMIT 24)`
		args = append(args, root)
	}

	if kind == groupMessageKind {
		query += " UNION SELECT * FROM (" + notice + ")"
		args = append(args, project, groupRecipient)
	}
	if kind != groupMessageKind && kind != "operator_message" && project != "" && (project == a.Project || a.Role == "root") {
		query += " UNION SELECT * FROM (" + shared + "12)"
		args = append(args, sharedArgs...)
		query += " UNION SELECT * FROM (" + notice + ")"
		args = append(args, project, groupRecipient)
	}
	if kind == "scope_review" {
		query += ` UNION SELECT * FROM (SELECT ` + privateColumns + ` FROM messages
			WHERE project_id=? AND task_id<>? AND (sender=? OR recipient=?) ORDER BY seq DESC LIMIT 12)`
		args = append(args, project, task, a.ID, a.ID)
	}
	if kind == "scope_decision" {
		query += ` UNION SELECT ` + privateColumns + ` FROM messages m
			WHERE m.id=(SELECT message_id FROM task_origins WHERE task_id=?)`
		args = append(args, task)
		var po string
		if err := tx.QueryRow("SELECT po_id FROM projects WHERE id=?", project).Scan(&po); err != nil {
			return nil, err
		}
		assessment, err := scopeAssessmentID(tx, task, project, po)
		if err != nil {
			return nil, err
		}
		if assessment != "" {
			query += ` UNION SELECT ` + privateColumns + ` FROM messages WHERE id=?`
			args = append(args, assessment)
		}
	}
	if kind == "operator_message" && a.ID == root {
		holds, _, err := s.activeHolds(tx)
		if err != nil {
			return nil, err
		}
		for _, h := range holds {
			query += ` UNION SELECT ` + privateColumns + ` FROM messages WHERE id=?`
			args = append(args, h.rootMessage)
		}
		if len(source) == 1 {
			query += ` UNION SELECT ` + privateColumns + ` FROM messages WHERE id=? AND sender=? AND recipient=? AND room_id=?`
			args = append(args, source[0], operator, root, directRoom(operator, root))
		}
	}
	if kind == "po_continue" {
		p, po, assignment, eligible, err := poContinuation(tx, task)
		if err != nil {
			return nil, err
		}
		if !eligible || p != project || po != a.ID || len(source) != 1 {
			return nil, fmt.Errorf("%w: PO continuation scope changed", errConflict)
		}
		var note string
		err = tx.QueryRow(`SELECT id FROM messages WHERE id=? AND task_id=? AND project_id=? AND sender=? AND recipient=? AND role='operator' AND room_id=?`, source[0], task, project, operator, po, directRoom(operator, po)).Scan(&note)
		if err != nil {
			return nil, err
		}
		query += ` UNION SELECT ` + privateColumns + ` FROM messages WHERE id IN (?,?)`
		args = append(args, assignment, note)
	}
	if kind == "po_followup" {
		if len(source) != 1 {
			return nil, fmt.Errorf("%w: follow-up source unavailable", errWorkflow)
		}
		f, err := readPOFollowup(tx, source[0])
		if err != nil {
			return nil, err
		}
		var valid int
		err = tx.QueryRow(`SELECT count(*) FROM jobs j JOIN messages m ON m.id=? WHERE j.id=? AND j.task_id=? AND j.project_id=? AND j.agent_id=? AND (m.task_id=j.task_id OR EXISTS(SELECT 1 FROM po_work_items i WHERE i.child_task_id=j.task_id AND i.parent_task_id=m.task_id)) AND m.project_id=j.project_id AND m.sender=? AND m.recipient=? AND m.role='root' AND m.room_id=?`, f.assignment, f.trigger, task, project, a.ID, root, a.ID, directRoom(root, a.ID)).Scan(&valid)
		if err != nil {
			return nil, err
		}
		if valid != 1 {
			return nil, fmt.Errorf("%w: follow-up assignment unavailable", errWorkflow)
		}
		if f.note != "" {
			err = tx.QueryRow(`SELECT count(*) FROM messages WHERE id=? AND task_id=? AND project_id=? AND sender=? AND recipient=? AND role='operator' AND room_id=?`, f.note, task, project, operator, a.ID, directRoom(operator, a.ID)).Scan(&valid)
			if err != nil {
				return nil, err
			}
			if valid != 1 {
				return nil, fmt.Errorf("%w: follow-up operator instruction unavailable", errWorkflow)
			}
		}
		var heldNote string
		err = tx.QueryRow(`SELECT COALESCE(note_id,'') FROM task_holds WHERE job_id=? AND task_id=? AND po_id=? AND status='resolved'`, f.trigger, task, a.ID).Scan(&heldNote)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		if heldNote != "" {
			err = tx.QueryRow(`SELECT count(*) FROM messages WHERE id=? AND task_id=? AND project_id=? AND sender=? AND recipient=? AND role='operator' AND room_id=?`, heldNote, task, project, operator, a.ID, directRoom(operator, a.ID)).Scan(&valid)
			if err != nil {
				return nil, err
			}
			if valid != 1 {
				return nil, fmt.Errorf("%w: held instruction unavailable", errWorkflow)
			}
			query += ` UNION SELECT ` + privateColumns + ` FROM messages WHERE id=?`
			args = append(args, heldNote)
		}
		if f.review != "" {
			if _, err := activeReviewWorker(tx, job{Task: task, Project: project, Kind: "review", Source: f.review}, a.ID); err != nil {
				return nil, err
			}
		}
		oldNotes, notesOK, err := legacyReviewNotes(tx, f.trigger, task, project, a.ID)
		if err != nil {
			return nil, err
		}
		if !notesOK {
			return nil, fmt.Errorf("%w: earlier continuation instructions unavailable", errWorkflow)
		}
		if len(oldNotes) > 0 {
			query += ` UNION SELECT ` + privateColumns + ` FROM messages WHERE id IN (` + strings.TrimRight(strings.Repeat("?,", len(oldNotes)), ",") + `)`
			for _, id := range oldNotes {
				args = append(args, id)
			}
		}
		// Carry authenticated parent-task decisions into a child review without widening rooms.
		query += ` UNION SELECT * FROM (SELECT ` + privateColumns + ` FROM messages WHERE id IN (SELECT f.note_id FROM po_followups f JOIN jobs j ON j.source_id=f.source_id JOIN po_work_items i ON i.parent_task_id=j.task_id WHERE i.child_task_id=? AND j.project_id=? AND j.agent_id=? AND f.note_id IS NOT NULL) AND sender=? AND recipient=? AND role='operator' AND room_id=? ORDER BY seq DESC LIMIT 12)`
		args = append(args, task, project, a.ID, operator, a.ID, directRoom(operator, a.ID))
		query += ` UNION SELECT ` + privateColumns + ` FROM messages WHERE id IN (SELECT note_id FROM root_hold_decisions WHERE task_id=?) AND sender=? AND recipient=? AND role='root' AND room_id=?`
		args = append(args, task, root, a.ID, directRoom(root, a.ID))
		query += ` UNION SELECT ` + privateColumns + ` FROM messages WHERE id IN (?,?,?)`
		args = append(args, f.assignment, f.note, f.review)
	}
	if parent != "" {
		query += ` UNION SELECT id,sender,role,text,seq,CASE WHEN recipient=? AND project_id=? THEN recipient ELSE '' END,recipient,room_id
			FROM messages WHERE id=?`
		args = append(args, groupRecipient, project, parent)
	}
	query = "SELECT id,sender,role,text,group_recipient FROM (" + query + ") WHERE ((room_id LIKE 'dm:%' AND (sender=? OR recipient=?)) OR (room_id=? AND ?=1))"
	groupAllowed := 0
	if project != "" && (a.Role == "root" || a.Project == project) {
		groupAllowed = 1
	}
	args = append(args, a.ID, a.ID, groupRecipient, groupAllowed)
	if kind == "operator_message" {
		if a.ID == root && project == "" {
			query += " AND (room_id=? OR (id IN (SELECT message_id FROM po_progress_notes) AND recipient=?))"
			args = append(args, directRoom(a.ID, operator), root)
		} else {
			query += " AND room_id=?"
			args = append(args, directRoom(a.ID, operator))
		}
	} else if kind == groupMessageKind {
		query += " AND room_id=?"
		args = append(args, groupRecipient)
	}
	query += " ORDER BY seq"
	messages, err := querySlice(tx, query, func(rows *sql.Rows) (runtimeapi.Message, error) {
		var m runtimeapi.Message
		err := rows.Scan(&m.ID, &m.Sender, &m.Role, &m.Text, &m.GroupRecipient)
		return m, err
	}, args...)
	if err != nil {
		return nil, err
	}
	for i := range messages {
		messages[i].Attachments, err = querySlice(tx,
			`SELECT a.id,a.name,a.media_type FROM message_attachments m
			JOIN attachments a ON a.id=m.attachment_id WHERE m.message_id=? ORDER BY a.id`,
			func(rows *sql.Rows) (runtimeapi.AttachmentRef, error) {
				var ref runtimeapi.AttachmentRef
				err := rows.Scan(&ref.ID, &ref.Name, &ref.MediaType)
				ref.Path = filepath.Join(s.dir, "attachments", ref.ID)
				return ref, err
			}, messages[i].ID)
		if err != nil {
			return nil, err
		}
	}
	return messages, nil
}

func (s *Service) complete(j job, result runtimeapi.Result, uploads []runtimeapi.Attachment, files []attachmentData, invalid error) error {
	s.startMu.Lock()
	defer s.startMu.Unlock()
	payload, err := json.Marshal(result)
	if err != nil {
		return err
	}
	err = s.write(func(tx *sql.Tx) error {
		var state string
		if err := tx.QueryRow("SELECT status FROM jobs WHERE id=?", j.ID).Scan(&state); err != nil {
			return err
		}
		if state == "completed" {
			return nil
		}
		if state != "submitted" && state != "running" {
			return fmt.Errorf("%w: result for %s job", errConflict, state)
		}
		if invalid == nil {
			current, err := s.uploadAttachments(tx, j, result)
			if err != nil {
				return err
			}
			if !slices.Equal(uploads, current) {
				invalid = fmt.Errorf("%w: attachment source changed before result commit", errWorkflow)
			}
		}
		diagnostic := ""
		if invalid == nil {
			if _, err := tx.Exec("SAVEPOINT apply_result"); err != nil {
				return err
			}
			invalid = s.applyResult(tx, j, result, files)
			if invalid != nil && !errors.Is(invalid, errWorkflow) {
				return invalid
			}
			if invalid != nil {
				if _, err := tx.Exec("ROLLBACK TO apply_result"); err != nil {
					return err
				}
			}
			if _, err := tx.Exec("RELEASE apply_result"); err != nil {
				return err
			}
		}
		if invalid != nil {
			var reviewErr *reviewConflictError
			if errors.As(invalid, &reviewErr) && len(files) == 0 && len(uploads) == 0 {
				ok, err := reviewCorrectionEligible(tx, j)
				if err != nil {
					return err
				}
				if ok {
					a, err := getAgent(tx, j.Agent)
					if err != nil {
						return err
					}
					if err := s.poFollowThrough(tx, j, a, runtimeapi.Result{}, reviewErr.Error()); err != nil {
						return err
					}
					_, err = tx.Exec("UPDATE jobs SET status='completed',result=?,diagnostic=? WHERE id=?", payload, invalid.Error(), j.ID)
					return err
				}
			}
			var planning *workItemPlanningError
			if errors.As(invalid, &planning) && len(files) == 0 && len(uploads) == 0 {
				a, err := getAgent(tx, j.Agent)
				if err != nil {
					return err
				}
				if a.Role == "po" && (j.Kind == "assignment" || j.Kind == "po_continue" || j.Kind == "po_followup") && parentTaskOwned(tx, j, a) == nil {
					var other int
					if err = tx.QueryRow(`SELECT (SELECT count(*) FROM jobs WHERE task_id=? AND id<>? AND status IN ('pending','submitted','running','uncertain') AND NOT EXISTS(SELECT 1 FROM scope_draft_confirmations c WHERE c.task_id=jobs.task_id AND c.review_job_id=jobs.id AND jobs.status='uncertain'))+(SELECT count(*) FROM approvals WHERE task_id=? AND status IN ('pending','denied'))`, j.Task, j.ID, j.Task).Scan(&other); err != nil {
						return err
					}
					if other == 0 {
						if err = s.poFollowThrough(tx, j, a, runtimeapi.Result{}, planning.Error()); err != nil {
							return err
						}
						_, err = tx.Exec("UPDATE jobs SET status='completed',result=?,diagnostic=? WHERE id=?", payload, invalid.Error(), j.ID)
						return err
					}
				}
			}
			var fieldErr *scopeFieldError
			if j.Kind == "scope_decision" && errors.As(invalid, &fieldErr) && len(files) == 0 && len(uploads) == 0 {
				eligible, err := handoffEligible(tx, j.Task, "assigned", j.ID, false)
				if err != nil {
					return err
				}
				if eligible {
					if err := s.queueScopeCorrection(tx, j.Task, j.Project, fieldErr.Error()); err != nil {
						return err
					}
					_, err := tx.Exec("UPDATE jobs SET status='completed',result=?,diagnostic=? WHERE id=?", payload, invalid.Error(), j.ID)
					return err
				}
			}
			if j.Task != "" {
				if err := workflowNotice(tx, j.Project, "blocked:"+j.ID, "A task could not continue and needs attention."); err != nil {
					return err
				}
			}
			diagnostic = invalid.Error()
			if j.Task != "" {
				if _, err := tx.Exec("UPDATE tasks SET status='blocked' WHERE id=? AND status!='completed'", j.Task); err != nil {
					return err
				}
				if err := s.maybeFollowParent(tx, j); err != nil {
					return err
				}
			}
			if j.Kind != groupMessageKind {
				if err := addMessage(tx, newID(), "service", operator, "system", diagnostic, j.Project, j.Task); err != nil {
					return err
				}
			}
		}
		_, err := tx.Exec("UPDATE jobs SET status='completed',result=?,diagnostic=? WHERE id=?", payload, nullIfEmpty(diagnostic), j.ID)
		return err
	})
	if err == nil {
		s.cancelStoppedRuns()
		s.signal()
	}
	return err
}

func (s *Service) applyResult(tx *sql.Tx, j job, result runtimeapi.Result, files []attachmentData) error {
	a, err := getAgent(tx, j.Agent)
	if err != nil {
		return err
	}
	if a.Role != j.Request.Role || (a.Role != "root" && a.Project != j.Project) {
		return fmt.Errorf("%w: runtime owner scope changed", errWorkflow)
	}
	if len(result.Text) > 128*1024 || len(result.Actions) > 12 {
		return fmt.Errorf("%w: result exceeds limits", errWorkflow)
	}
	if j.Kind == "po_followup" {
		var childReceiptOnly int
		if err := tx.QueryRow(`SELECT count(*) FROM po_child_receipt_recoveries WHERE source_id=? AND task_id=?`, j.Source, j.Task).Scan(&childReceiptOnly); err != nil {
			return err
		}
		if childReceiptOnly != 0 {
			if a.Role != "po" || len(result.Actions) < 2 || len(result.Actions) > 3 || result.Actions[0].Type != "accept_result" || result.Actions[1].Type != "report" || (len(result.Actions) == 3 && result.Actions[2].Type != "report_progress") || len(files) != 0 || len(result.Attachments) != 0 {
				return fmt.Errorf("%w: recovery permits only the child receipt review", errWorkflow)
			}
			result.Text = ""
		}
		var reportOnly int
		if err := tx.QueryRow(`SELECT count(*) FROM po_report_recoveries WHERE source_id=? AND task_id=?`, j.Source, j.Task).Scan(&reportOnly); err != nil {
			return err
		}
		if reportOnly != 0 {
			if a.Role != "po" || len(result.Actions) != 1 || result.Actions[0].Type != "report" || (result.Actions[0].Recipient != "" && result.Actions[0].Recipient != root) || len(files) != 0 || len(result.Attachments) != 0 {
				return fmt.Errorf("%w: recovery permits only the parent report", errWorkflow)
			}
			// The action is the sole authorized output; retain raw result in the job receipt.
			result.Text = ""
		}
	}
	for _, action := range result.Actions {
		if action.Task != "" && action.Type != "resolve_hold" {
			return fmt.Errorf("%w: task field is reserved for resolve_hold", errWorkflow)
		}
		if action.ReplyTo != "" && action.Type != "post_group" {
			return fmt.Errorf("%w: reply_to is reserved for project group posts", errWorkflow)
		}
		if action.Type == "resolve_hold" && len(result.Actions) != 1 {
			return fmt.Errorf("%w: resolve_hold must be a separate action", errWorkflow)
		}
	}
	if a.Role == "po" {
		hold, advance := false, false
		for _, action := range result.Actions {
			switch action.Type {
			case "hold_task":
				hold = true
			case "accept_result", "report", "queue_worker", "queue_work_item", "create_worker", "create_one_shot_worker", "request_approval":
				advance = true
			}
		}
		if hold && advance {
			return &reviewConflictError{}
		}
	}
	if j.Kind == groupMessageKind {
		return groupMessageResult(tx, a, j, result, files)
	}
	if a.Role == "worker" {
		return s.workerResult(tx, j, a, result, files)
	}
	if j.Kind == "scope_review" {
		if strings.TrimSpace(result.Text) == "" {
			return fmt.Errorf("%w: PO scope review requires a text assessment before any assignment", errWorkflow)
		}
		for _, action := range result.Actions {
			if action.Type != "post_group" {
				return fmt.Errorf("%w: scope review cannot dispatch work", errWorkflow)
			}
			if err := postGroupAction(tx, a, j, action); err != nil {
				return err
			}
		}
		id, err := outputMessage(tx, a, root, j, result.Text, files)
		if err != nil {
			return err
		}
		manager, err := getAgent(tx, root)
		if err != nil {
			return err
		}
		return s.enqueue(tx, manager, j.Project, j.Task, id, "scope_decision", "Use the original request, draft and PO assessment in the task history to finalize this assignment or explain the blocker to the operator.")
	}
	if j.Kind == "scope_decision" {
		return s.finishScope(tx, a, j, result, files)
	}

	recipient := operator
	if a.Role == "po" && j.Kind != "operator_message" {
		recipient = root
	}
	if a.Role == "root" && j.Kind == "project_report" {
		if err := relayProjectReport(tx, a, j, result.Text, files); err != nil {
			return err
		}
	} else if a.ID == root && j.Kind == "task_blocked" && len(result.Actions) == 1 && result.Actions[0].Type == "resolve_hold" {
		// A successful Root-owned decision is delivered to the PO by the action, not the operator.

	} else if strings.TrimSpace(result.Text) != "" || len(files) > 0 {
		text, outputFiles := result.Text, files
		if a.Role == "po" && j.Kind != "operator_message" && recipient == root {
			protected, err := poHasPrivateOperatorInput(tx, j, a)
			if err != nil {
				return err
			}
			if protected {
				if _, err := outputMessage(tx, a, a.ID, j, result.Text, files); err != nil {
					return err
				}
				text = "A task turn completed; details remain private to the operator and project owner."
				outputFiles = nil
			}
		}
		id, err := outputMessage(tx, a, recipient, j, text, outputFiles)
		if err != nil {
			return err
		}
		if a.ID == root && j.Kind == "task_blocked" {
			if _, err := tx.Exec(`UPDATE task_holds SET root_message_id=? WHERE root_source_id=? AND status='active'`, id, j.Source); err != nil {
				return err
			}
		}
		if a.Role == "po" && j.Kind != "operator_message" && recipient == root {
			if _, err := tx.Exec(`INSERT INTO po_progress_notes(message_id) VALUES(?)`, id); err != nil {
				return err
			}
		}
	}
	planningJob := j
	var childParent string
	if a.Role == "po" {
		err := tx.QueryRow(`SELECT parent_task_id FROM po_work_items WHERE child_task_id=?`, j.Task).Scan(&childParent)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	hasPlanning := false
	for _, action := range result.Actions {
		if action.Type == "queue_work_item" || action.Type == "create_worker" || action.Type == "create_one_shot_worker" {
			hasPlanning = true
		}
	}
	if childParent != "" && hasPlanning {
		if _, err := activeReviewWorker(tx, j, a.ID); err != nil {
			return err
		}
		planningJob.Task = childParent
		if err := parentTaskOwned(tx, planningJob, a); err != nil {
			return err
		}
		if _, err := s.poAssignmentSource(tx, planningJob, a.ID); err != nil {
			return err
		}
	}
	completedChildInResult := false
	for pass := 0; pass < 2; pass++ {
		for _, action := range result.Actions {
			planning := childParent != "" && hasPlanning && (action.Type == "queue_work_item" || action.Type == "create_worker" || action.Type == "create_one_shot_worker")
			if (pass == 0 && planning) || (pass == 1 && !planning) {
				continue
			}
			if j.Kind == "approval_denied" {
				return fmt.Errorf("%w: operator approval must be requested separately and a denial cannot dispatch work", errWorkflow)
			}
			if action.Type == "request_approval" && len(result.Actions) != 1 {
				if a.Role == "po" && (j.Kind == "assignment" || j.Kind == "po_continue" || j.Kind == "po_followup") {
					return workItemPlanning(fmt.Sprintf("%v: operator approval must be requested separately and a denial cannot dispatch work", errWorkflow))
				}
				return fmt.Errorf("%w: operator approval must be requested separately and a denial cannot dispatch work", errWorkflow)
			}
			if planning {
				var status string
				if err := tx.QueryRow(`SELECT status FROM tasks WHERE id=? AND project_id=? AND owner_id=?`, j.Task, j.Project, a.ID).Scan(&status); err != nil {
					return err
				}
				if status != "completed" {
					return fmt.Errorf("%w: review must be accepted and reported before planning", errWorkflow)
				}
				if err := s.applyAction(tx, a, planningJob, action); err != nil {
					return err
				}
			} else {
				if action.Type == "report_progress" && completedChildInResult {
					if err := s.reportPOProgress(tx, j, a, action, true); err != nil {
						return err
					}
				} else if err := s.applyAction(tx, a, j, action); err != nil {
					return err
				}
				if childParent != "" && action.Type == "report" {
					completedChildInResult = true
				}
			}
		}
	}
	if a.Role == "po" {
		if err := s.maybeFollowParent(tx, j); err != nil {
			return err
		}
		if childParent == "" {
			for _, action := range result.Actions {
				if action.Type == "queue_work_item" {
					return s.followWorkPlanning(tx, j, a)
				}
			}
		}
	}
	return s.poFollowThrough(tx, j, a, result)
}

func groupMessageResult(tx *sql.Tx, a agent, j job, result runtimeapi.Result, files []attachmentData) error {
	if (a.Role != "po" && a.Role != "worker") || j.Task != "" || len(result.Actions) != 0 ||
		(strings.TrimSpace(result.Text) == "" && len(files) == 0) {
		return fmt.Errorf("%w: project group response requires an addressed agent and text or attachments without orchestration actions", errWorkflow)
	}
	recipient := groupPrefix + j.Project
	var source string
	err := tx.QueryRow(`SELECT m.id FROM messages m JOIN projects p ON p.id=m.project_id
		WHERE m.id=? AND m.project_id=? AND m.recipient=? AND m.sender=? AND m.role='operator' AND m.task_id IS NULL
		AND (EXISTS(SELECT 1 FROM message_mentions target WHERE target.message_id=m.id AND target.agent_id=?) OR EXISTS(SELECT 1 FROM message_implicit_dispatch target WHERE target.message_id=m.id AND target.agent_id=?))
		AND (?='worker' OR p.po_id=?)`, j.Source, j.Project, recipient, operator, a.ID, a.ID, a.Role, a.ID).Scan(&source)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: exact project group source unavailable", errWorkflow)
	}
	if err != nil {
		return err
	}
	id, err := outputMessage(tx, a, recipient, j, result.Text, files)
	if err != nil {
		return err
	}
	return attachGroupReply(tx, id, source, j.Project)
}

// attachGroupReply only links an extant message in the same project group.
// A missing or inaccessible target leaves the new message standalone.
func attachGroupReply(tx *sql.Tx, messageID, parentID, project string) error {
	if parentID == "" {
		return nil
	}
	_, err := tx.Exec(`INSERT INTO message_replies(message_id,parent_id) SELECT child.id,parent.id FROM messages child JOIN messages parent ON parent.id=? AND parent.room_id=? AND parent.project_id=? WHERE child.id=? AND child.room_id=? AND child.project_id=? AND child.id<>parent.id`, parentID, groupPrefix+project, project, messageID, groupPrefix+project, project)
	return err
}

func relayProjectReport(tx *sql.Tx, a agent, j job, text string, files []attachmentData) error {
	var report string
	err := tx.QueryRow(`SELECT m.text FROM messages m JOIN projects p ON p.id=m.project_id
		JOIN tasks t ON t.id=m.task_id AND t.project_id=p.id AND t.status='completed'
		WHERE m.id=? AND m.task_id=? AND p.id=? AND m.sender=p.po_id AND m.recipient=? AND m.role='po'`,
		j.Source, j.Task, j.Project, root).Scan(&report)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: exact PO report unavailable", errWorkflow)
	}
	if err != nil {
		return err
	}
	if strings.TrimSpace(text) == "" {
		text = report
	}
	id, err := outputMessage(tx, a, operator, j, text, files)
	if err != nil {
		return err
	}
	return shareTaskAttachments(tx, j.Source, id)
}

func shareAcceptedWorkerAttachments(tx *sql.Tx, j job, po, reportID string) error {
	var workerResultID string
	err := tx.QueryRow(`SELECT m.id FROM messages m JOIN agents w ON w.id=m.sender AND w.project_id=m.project_id
		JOIN messages report ON report.id=? AND report.task_id=m.task_id
		  AND report.project_id=m.project_id AND report.sender=m.recipient AND report.seq>m.seq
		WHERE m.task_id=? AND m.project_id=? AND m.recipient=? AND m.role='worker' AND w.role='worker'
		ORDER BY m.seq DESC LIMIT 1`, reportID, j.Task, j.Project, po).Scan(&workerResultID)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: exact accepted worker result unavailable", errWorkflow)
	}
	if err != nil {
		return err
	}
	return shareTaskAttachments(tx, workerResultID, reportID)
}

func (s *Service) workerResult(tx *sql.Tx, j job, a agent, result runtimeapi.Result, files []attachmentData) error {
	if (j.Kind != "work" && len(result.Actions) != 0) || (strings.TrimSpace(result.Text) == "" && len(files) == 0) {
		return fmt.Errorf("%w: worker must submit text or attachments without orchestration actions", errWorkflow)
	}
	if j.Kind == "operator_message" && j.Task == "" {
		var source string
		err := tx.QueryRow(`SELECT id FROM messages WHERE id=? AND sender=? AND recipient=?
			AND role='operator' AND project_id=? AND task_id IS NULL`, j.Source, operator, a.ID, j.Project).Scan(&source)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: exact operator conversation source unavailable", errWorkflow)
		}
		if err != nil {
			return err
		}
		_, err = outputMessage(tx, a, operator, j, result.Text, files)
		return err
	}
	if j.Kind != "work" {
		return fmt.Errorf("%w: unsupported worker event", errWorkflow)
	}
	for _, action := range result.Actions {
		if action.Type != "post_group" {
			return fmt.Errorf("%w: worker must submit text or attachments without orchestration actions", errWorkflow)
		}
		if err := postGroupAction(tx, a, j, action); err != nil {
			return err
		}
	}
	var poID string
	if err := tx.QueryRow("SELECT po_id FROM projects WHERE id=?", j.Project).Scan(&poID); err != nil {
		return err
	}
	po, err := getAgent(tx, poID)
	if err != nil {
		return err
	}
	if err := moveTask(tx, j, "queued", "review", a.ID, po.ID); err != nil {
		return err
	}
	id, err := outputMessage(tx, a, po.ID, j, result.Text, files)
	if err != nil {
		return err
	}
	return s.enqueue(tx, po, j.Project, j.Task, id, "review", "Check the worker result and its attachments before reporting.")
}

func outputMessage(tx *sql.Tx, a agent, recipient string, j job, text string, files []attachmentData) (string, error) {
	id := newID()
	if err := addMessage(tx, id, a.ID, recipient, a.Role, text, j.Project, j.Task); err != nil {
		return "", err
	}
	for _, file := range files {
		if _, err := tx.Exec("INSERT INTO attachments VALUES (?,?,?,?,?)", file.ID, id, file.Name, file.MediaType, file.Content); err != nil {
			return "", err
		}
	}
	return id, nil
}

func (s *Service) applyAction(tx *sql.Tx, a agent, j job, action runtimeapi.Action) error {
	if len(action.Title) > 250 || len(action.Body) > 64*1024 {
		return fmt.Errorf("%w: action title or body exceeds limits", errWorkflow)
	}
	if action.Type == "request_approval" && (a.Role == "root" || a.Role == "po") {
		if action.Project != "" && action.Project != j.Project || action.Recipient != "" && action.Recipient != operator ||
			strings.TrimSpace(action.Title) == "" || strings.TrimSpace(action.Body) == "" {
			return fmt.Errorf("%w: invalid operator approval scope or reason", errWorkflow)
		}
		id := newID()
		if _, err := tx.Exec("INSERT INTO approvals VALUES (?,?,?,?,?,?,'pending')",
			id, nullIfEmpty(j.Project), nullIfEmpty(j.Task), a.ID, action.Title, action.Body); err != nil {
			return err
		}
		if a.Role == "po" {
			return deliverApproval(tx, id, j.Project, j.Task, action.Title, action.Body)
		}
		return nil
	}
	if a.Role == "root" && j.Kind == "task_blocked" && action.Type == "resolve_hold" {
		return s.resolveHold(tx, j, action)
	}
	if a.Role == "root" && (j.Kind == "operator_message" || j.Kind == "approval_approved") {
		switch action.Type {
		case "resolve_hold":
			return s.resolveHold(tx, j, action)
		case "set_project_goal", "set_project_goal_status":
			return setProjectGoal(tx, action)
		case "delegate", "create_project":
			return s.rootAssignment(tx, a, j, action)
		case "stop_project", "resume_project":
			if !projectID.MatchString(action.Project) || action.Recipient != "" {
				return fmt.Errorf("%w: stop or resume requires an exact project ID", errWorkflow)
			}
			_, err := updateStop(tx, action.Project, action.Type == "stop_project")
			if errors.Is(err, errAbsent) {
				err = fmt.Errorf("%w: project does not exist", errWorkflow)
			}
			return err
		}
	}
	if a.Role == "po" && action.Type == "post_group" && j.Kind != "operator_message" {
		return postGroupAction(tx, a, j, action)
	}
	if a.Role != "po" || j.Task == "" || (action.Project != "" && action.Project != j.Project) {
		return fmt.Errorf("%w: action %q outside role or task scope", errWorkflow, action.Type)
	}
	switch action.Type {
	case "hold_task":
		return s.poHoldAction(tx, j, a, action)
	case "report_progress":
		return s.reportPOProgress(tx, j, a, action, false)
	case "queue_worker":
		return s.queueWorker(tx, j, a, action)
	case "create_worker", "create_one_shot_worker":
		return s.createProjectWorker(tx, j, a, action)
	case "queue_work_item":
		return s.queuePOWorkItem(tx, j, a, action)
	case "accept_result":
		if j.Kind == "po_followup" && action.Recipient != "" {
			var matched int
			err := tx.QueryRow(`SELECT count(DISTINCT c.id) FROM po_work_items i
				JOIN tasks parent ON parent.id=i.parent_task_id AND parent.project_id=? AND parent.owner_id=? AND parent.status='assigned'
				JOIN tasks c ON c.id=i.child_task_id AND c.project_id=? AND c.status='completed' AND c.owner_id=?
				JOIN projects p ON p.id=c.project_id AND p.po_id=?
				JOIN agents w ON w.id=? AND w.project_id=c.project_id AND w.role='worker'
				JOIN messages m ON m.task_id=c.id AND m.project_id=c.project_id AND m.sender=w.id AND m.recipient=? AND m.role='worker'
				JOIN jobs r ON r.source_id=m.id AND r.task_id=c.id AND r.project_id=c.project_id AND r.agent_id=? AND r.kind='review' AND r.status='completed' AND COALESCE(r.diagnostic,'')=''
				JOIN messages report ON report.task_id=c.id AND report.project_id=c.project_id AND report.sender=? AND report.recipient IN (?,?) AND report.role='po' AND report.seq>m.seq
				WHERE i.parent_task_id=?`, j.Project, a.ID, j.Project, a.ID, a.ID, action.Recipient, a.ID, a.ID, a.ID, a.ID, root, j.Task).Scan(&matched)
			if err != nil {
				return err
			}
			if matched == 1 {
				return nil
			}
			if matched > 1 {
				return fmt.Errorf("%w: completed child recipient is ambiguous", errWorkflow)
			}
		}
		worker, err := activeReviewWorker(tx, j, a.ID)
		if err != nil {
			return err
		}
		if action.Recipient != "" && action.Recipient != worker {
			return fmt.Errorf("%w: accept_result recipient differs from the active review worker", errWorkflow)
		}
		return moveTask(tx, j, "review", "accepted", a.ID, a.ID)
	case "report":
		if strings.TrimSpace(action.Body) == "" {
			return fmt.Errorf("%w: report requires Root and a body", errWorkflow)
		}
		var parent string
		err := tx.QueryRow("SELECT parent_task_id FROM po_work_items WHERE child_task_id=?", j.Task).Scan(&parent)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if parent != "" {
			worker, err := activeReviewWorker(tx, j, a.ID)
			if err != nil {
				return err
			}
			if action.Recipient != "" && action.Recipient != root && action.Recipient != worker {
				return fmt.Errorf("%w: child report recipient differs from the reviewed worker", errWorkflow)
			}
			if err := moveTask(tx, j, "accepted", "completed", a.ID, a.ID); err != nil {
				return err
			}
			_, err = outputMessage(tx, a, a.ID, j, action.Body, nil)
			return err
		}
		if action.Recipient != "" && action.Recipient != root {
			return fmt.Errorf("%w: report requires Root and a body", errWorkflow)
		}
		var total, incomplete int
		if err := tx.QueryRow(`SELECT count(*),COALESCE(sum(CASE WHEN c.status!='completed' THEN 1 ELSE 0 END),0) FROM po_work_items i JOIN tasks c ON c.id=i.child_task_id WHERE i.parent_task_id=?`, j.Task).Scan(&total, &incomplete); err != nil {
			return err
		}
		if total > 0 {
			if incomplete != 0 {
				return fmt.Errorf("%w: required work items are incomplete", errWorkflow)
			}
			if err := moveTask(tx, j, "assigned", "completed", a.ID, a.ID); err != nil {
				return err
			}
		} else if err := moveTask(tx, j, "accepted", "completed", a.ID, a.ID); err != nil {
			return err
		}
		if err := workflowNotice(tx, j.Project, "complete:"+j.ID, "The project owner completed the task and sent the report to Root."); err != nil {
			return err
		}
		id, err := outputMessage(tx, a, root, j, action.Body, nil)
		if err != nil {
			return err
		}
		if total > 0 {
			// Forward only the latest worker receipt behind each completed child report.
			_, err = tx.Exec(`INSERT INTO attachment_shares(message_id,attachment_id)
			 SELECT ?,links.attachment_id FROM po_work_items i JOIN tasks c ON c.id=i.child_task_id AND c.status='completed'
			 JOIN messages m ON m.id=(SELECT w.id FROM messages w JOIN agents wa ON wa.id=w.sender AND wa.project_id=w.project_id AND wa.role='worker' WHERE w.task_id=c.id AND w.project_id=c.project_id AND w.recipient=? AND w.role='worker' ORDER BY w.seq DESC LIMIT 1)
			 JOIN message_attachments links ON links.message_id=m.id
			 WHERE i.parent_task_id=? AND EXISTS(SELECT 1 FROM messages r WHERE r.task_id=c.id AND r.project_id=c.project_id AND r.sender=? AND r.recipient=? AND r.role='po' AND r.seq>m.seq)
			 ON CONFLICT(message_id,attachment_id) DO NOTHING`, id, a.ID, j.Task, a.ID, a.ID)
			if err != nil {
				return err
			}
		} else if err := shareAcceptedWorkerAttachments(tx, j, a.ID, id); err != nil {
			return err
		}
		manager, err := getAgent(tx, root)
		if err != nil {
			return err
		}
		return s.enqueue(tx, manager, j.Project, j.Task, id, "project_report", action.Body)
	default:
		return fmt.Errorf("%w: unsupported action %q", errWorkflow, action.Type)
	}
}

func activeReviewWorker(tx *sql.Tx, j job, po string) (string, error) {
	if j.Kind != "review" && j.Kind != "approval_approved" && j.Kind != "po_followup" {
		return "", fmt.Errorf("%w: accept_result requires the active review", errWorkflow)
	}
	var message, worker string
	var seq int64
	err := tx.QueryRow(`SELECT m.id,m.sender,m.seq FROM messages m
		JOIN agents w ON w.id=m.sender AND w.project_id=m.project_id AND w.role='worker'
		JOIN projects p ON p.id=m.project_id AND p.po_id=m.recipient
		WHERE m.task_id=? AND m.project_id=? AND m.recipient=? AND m.role='worker'
		ORDER BY m.seq DESC LIMIT 1`, j.Task, j.Project, po).Scan(&message, &worker, &seq)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("%w: active worker result unavailable", errWorkflow)
	}
	if err != nil {
		return "", err
	}
	if j.Kind == "review" || j.Kind == "po_followup" {
		source := j.Source
		if j.Kind == "po_followup" {
			f, err := readPOFollowup(tx, j.Source)
			if err != nil {
				return "", err
			}
			source = f.review
		}
		if source != message {
			return "", fmt.Errorf("%w: review source differs from the current worker result", errWorkflow)
		}
	} else {
		var approved int
		err = tx.QueryRow(`SELECT 1 FROM messages WHERE id=? AND task_id=? AND project_id=?
			AND sender=? AND recipient=? AND role='operator' AND seq>?`,
			j.Source, j.Task, j.Project, operator, po, seq).Scan(&approved)
		if errors.Is(err, sql.ErrNoRows) {
			return "", fmt.Errorf("%w: approved review source differs from the current worker result", errWorkflow)
		}
		if err != nil {
			return "", err
		}
	}
	return worker, nil
}

func (s *Service) rootAssignment(tx *sql.Tx, a agent, j job, action runtimeapi.Action) error {
	if err := validateDisplayName(action.Project); err != nil {
		return fmt.Errorf("%w: project/channel name %v", errWorkflow, err)
	}
	if !projectID.MatchString(action.Project) || strings.TrimSpace(action.Title) == "" ||
		strings.TrimSpace(action.Body) == "" {
		return fmt.Errorf("%w: assignment needs project ID, title and body", errWorkflow)
	}
	var poID string
	err := tx.QueryRow("SELECT po_id FROM projects WHERE id=?", action.Project).Scan(&poID)
	if action.Type == "create_project" {
		if err == nil {
			return fmt.Errorf("%w: a new project requires an unused ID", errWorkflow)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		// Creation always provisions a fresh PO. Only delegation uses a proposed recipient.
		poID = newID()
		if _, err := tx.Exec("INSERT INTO projects(id,name,po_id,workspace) VALUES (?,?,?,?)",
			action.Project, action.Project, poID, s.workspace(action.Project)); err != nil {
			return err
		}
		for _, member := range []agent{{ID: poID, Name: action.Project + "-PO", Role: "po", Model: s.opts.POModel}} {
			if err := validateDisplayName(member.Name); err != nil {
				return fmt.Errorf("%w: agent name %v", errWorkflow, err)
			}
			if _, err := tx.Exec("INSERT INTO agents VALUES (?,?,?,?,?,?)",
				member.ID, member.Name, member.Role, action.Project, member.Model, s.opts.Effort); err != nil {
				return err
			}
		}
	} else {
		if errors.Is(err, sql.ErrNoRows) || (err == nil && action.Recipient != "" && action.Recipient != poID) {
			return fmt.Errorf("%w: delegation requires the existing project and its PO", errWorkflow)
		}
		if err != nil {
			return err
		}
	}
	po, err := getAgent(tx, poID)
	if err != nil {
		return err
	}
	task, id := newID(), newID()
	if _, err := tx.Exec("INSERT INTO tasks VALUES (?,?,?,'assigned',?)", task, action.Project, action.Title, po.ID); err != nil {
		return err
	}
	if _, err := tx.Exec("INSERT INTO task_origins VALUES (?,?)", task, j.Source); err != nil {
		return err
	}
	if err := addMessage(tx, id, a.ID, po.ID, a.Role, action.Body, action.Project, task); err != nil {
		return err
	}
	if err := workflowNotice(tx, action.Project, "scope:"+task, "Root sent the project owner a request for scope assessment."); err != nil {
		return err
	}
	return s.enqueue(tx, po, action.Project, task, id, "scope_review", action.Body)
}

func (s *Service) finishScope(tx *sql.Tx, a agent, j job, result runtimeapi.Result, files []attachmentData) error {
	var poID string
	if err := tx.QueryRow("SELECT po_id FROM projects WHERE id=?", j.Project).Scan(&poID); err != nil {
		return err
	}
	var valid int
	if err := tx.QueryRow(`SELECT count(*) FROM tasks t JOIN projects p ON p.id=t.project_id JOIN agents po ON po.id=p.po_id AND po.project_id=p.id AND po.role='po' WHERE t.id=? AND p.id=? AND p.stopped=0 AND t.status='assigned' AND t.owner_id=p.po_id`, j.Task, j.Project).Scan(&valid); err != nil {
		return err
	}
	if valid != 1 || a.ID != root {
		return fmt.Errorf("%w: task owner or status changed", errWorkflow)
	}
	if len(result.Actions) == 0 && strings.TrimSpace(result.Text) != "" {
		if err := moveTask(tx, j, "assigned", "blocked", poID, poID); err != nil {
			return err
		}
		if err := workflowNotice(tx, j.Project, "blocked:"+j.ID, "A task could not continue and needs attention."); err != nil {
			return err
		}
		_, err := outputMessage(tx, a, operator, j, result.Text, files)
		return err
	}
	if len(result.Actions) != 1 {
		return fmt.Errorf("%w: Root must finalize the assignment once or explain the blocker", errWorkflow)
	}
	action := result.Actions[0]
	if action.Type != "delegate" {
		return fmt.Errorf("%w: final assignment action must be delegate", errWorkflow)
	}
	if action.Project != j.Project {
		return &scopeFieldError{"project must equal the current project ID"}
	}
	if action.Recipient != "" && action.Recipient != poID {
		return &scopeFieldError{"recipient must be empty or the appointed PO ID"}
	}
	if strings.TrimSpace(action.Title) == "" || len(action.Title) > 250 {
		return &scopeFieldError{"title must be nonempty and at most 250 bytes"}
	}
	if strings.TrimSpace(action.Body) == "" || len(action.Body) > 64*1024 {
		return &scopeFieldError{"body must be nonempty and at most 65536 bytes"}
	}
	po, err := getAgent(tx, poID)
	if err != nil {
		return err
	}
	if _, err := tx.Exec("UPDATE tasks SET title=? WHERE id=?", action.Title, j.Task); err != nil {
		return err
	}
	if strings.TrimSpace(result.Text) != "" || len(files) > 0 {
		if _, err := outputMessage(tx, a, operator, j, result.Text, files); err != nil {
			return err
		}
	}
	id, err := outputMessage(tx, a, po.ID, j, action.Body, nil)
	if err != nil {
		return err
	}
	if err := workflowNotice(tx, j.Project, "assignment:"+j.ID, "Root sent the finalized assignment to the project owner."); err != nil {
		return err
	}
	return s.enqueue(tx, po, j.Project, j.Task, id, "assignment", action.Body)
}

func (s *Service) queueWorker(tx *sql.Tx, j job, po agent, action runtimeapi.Action) error {
	var workerID, prior string
	if strings.TrimSpace(action.Body) == "" {
		return fmt.Errorf("%w: worker assignment needs instructions", errWorkflow)
	}
	query := "SELECT id FROM agents WHERE project_id=? AND role='worker' AND NOT EXISTS (SELECT 1 FROM one_shot_workers o WHERE o.agent_id=agents.id AND (o.child_task_id IS NULL OR o.child_task_id<>?))"
	args := []any{j.Project, j.Task}
	if action.Recipient != "" {
		query += " AND id=?"
		args = append(args, action.Recipient)
	}
	if err := tx.QueryRow(query+" ORDER BY id LIMIT 1", args...).Scan(&workerID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: worker outside project", errWorkflow)
		}
		return err
	}
	if err := tx.QueryRow("SELECT status FROM tasks WHERE id=? AND project_id=? AND owner_id=?", j.Task, j.Project, po.ID).
		Scan(&prior); err != nil {
		return err
	}
	if prior != "assigned" && prior != "review" {
		return fmt.Errorf("%w: worker assignment outside assigned or review phase", errWorkflow)
	}
	if err := moveTask(tx, j, prior, "queued", po.ID, workerID); err != nil {
		return err
	}
	worker, err := getAgent(tx, workerID)
	if err != nil {
		return err
	}
	id := newID()
	if err := addMessage(tx, id, po.ID, worker.ID, po.Role, action.Body, j.Project, j.Task); err != nil {
		return err
	}
	if err := workflowNotice(tx, j.Project, "queue:"+j.ID, "The project owner queued work for a worker."); err != nil {
		return err
	}
	return s.enqueue(tx, worker, j.Project, j.Task, id, "work", action.Body)
}

func moveTask(tx *sql.Tx, j job, from, to, oldOwner, newOwner string) error {
	result, err := tx.Exec("UPDATE tasks SET status=?,owner_id=? WHERE id=? AND project_id=? AND status=? AND owner_id=?",
		to, newOwner, j.Task, j.Project, from, oldOwner)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("%w: task owner or status changed", errWorkflow)
	}
	return nil
}

// postGroupAction records an authored project-room message without dispatching new work.
func postGroupAction(tx *sql.Tx, a agent, j job, action runtimeapi.Action) error {
	if (a.Role != "po" && a.Role != "worker") || j.Project == "" || a.Project != j.Project || action.Project != j.Project || (action.Recipient != "" && action.Recipient != groupPrefix+j.Project) || strings.TrimSpace(action.Body) == "" || len(action.Body) > 64*1024 {
		return fmt.Errorf("%w: group post outside project scope", errWorkflow)
	}
	var valid int
	if err := tx.QueryRow(`SELECT count(*) FROM agents a JOIN projects p ON p.id=a.project_id WHERE a.id=? AND a.project_id=? AND a.role=? AND (a.role='worker' OR (a.role='po' AND p.po_id=a.id))`, a.ID, j.Project, a.Role).Scan(&valid); err != nil {
		return err
	}
	if valid != 1 {
		return fmt.Errorf("%w: group author not a project member", errWorkflow)
	}
	id := newID()
	if err := addMessage(tx, id, a.ID, groupPrefix+j.Project, a.Role, action.Body, j.Project, ""); err != nil {
		return err
	}
	return attachGroupReply(tx, id, action.ReplyTo, j.Project)
}
