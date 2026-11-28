CREATE TABLE IF NOT EXISTS projects (
  id TEXT PRIMARY KEY, name TEXT NOT NULL, po_id TEXT NOT NULL UNIQUE,
  workspace TEXT NOT NULL, stopped INTEGER NOT NULL DEFAULT 0 CHECK(stopped IN (0,1))
);
CREATE TABLE IF NOT EXISTS agents (
  id TEXT PRIMARY KEY, name TEXT NOT NULL, role TEXT NOT NULL CHECK(role IN ('root','po','worker')),
  project_id TEXT REFERENCES projects(id), model TEXT NOT NULL, effort TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS agent_model_providers (
 agent_id TEXT PRIMARY KEY REFERENCES agents(id), provider TEXT NOT NULL CHECK(provider!=''), port INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS local_model_providers (id TEXT PRIMARY KEY, name TEXT NOT NULL, enabled INTEGER NOT NULL CHECK(enabled IN (0,1)), port INTEGER NOT NULL CHECK(port BETWEEN 1 AND 65535));
CREATE TABLE IF NOT EXISTS one_shot_workers (
 agent_id TEXT PRIMARY KEY REFERENCES agents(id),
 child_task_id TEXT UNIQUE REFERENCES tasks(id)
);
CREATE TABLE IF NOT EXISTS operator_preview_workers (
 agent_id TEXT PRIMARY KEY REFERENCES agents(id),
 project_id TEXT NOT NULL REFERENCES projects(id)
);
CREATE TABLE IF NOT EXISTS operator_preview_deliveries (
 job_id TEXT PRIMARY KEY REFERENCES jobs(id),
 message_id TEXT NOT NULL UNIQUE REFERENCES messages(id)
);
CREATE TABLE IF NOT EXISTS tasks (
  id TEXT PRIMARY KEY, project_id TEXT NOT NULL REFERENCES projects(id), title TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('assigned','queued','review','accepted','completed','blocked')),
  owner_id TEXT NOT NULL REFERENCES agents(id)
);
CREATE TABLE IF NOT EXISTS messages (
  seq INTEGER PRIMARY KEY, id TEXT NOT NULL UNIQUE, sender TEXT NOT NULL, recipient TEXT NOT NULL,
  role TEXT NOT NULL, text TEXT NOT NULL, at INTEGER NOT NULL,
  task_id TEXT REFERENCES tasks(id), project_id TEXT REFERENCES projects(id)
);
CREATE TABLE IF NOT EXISTS task_origins (
  task_id TEXT PRIMARY KEY REFERENCES tasks(id),
  message_id TEXT NOT NULL REFERENCES messages(id)
);
CREATE TABLE IF NOT EXISTS message_replies (
  message_id TEXT PRIMARY KEY REFERENCES messages(id),
  parent_id TEXT NOT NULL REFERENCES messages(id),
  CHECK(message_id<>parent_id)
);
CREATE TABLE IF NOT EXISTS worker_po_handoffs (
 message_id TEXT PRIMARY KEY REFERENCES messages(id),
 source_id TEXT NOT NULL UNIQUE REFERENCES messages(id),
 job_id TEXT NOT NULL UNIQUE REFERENCES jobs(id),
 project_id TEXT NOT NULL REFERENCES projects(id)
);
CREATE TABLE IF NOT EXISTS message_mentions (
  message_id TEXT NOT NULL REFERENCES messages(id),
  agent_id TEXT NOT NULL REFERENCES agents(id),
  PRIMARY KEY(message_id,agent_id)
);
CREATE TABLE IF NOT EXISTS message_implicit_dispatch (
 message_id TEXT PRIMARY KEY REFERENCES messages(id),
 agent_id TEXT NOT NULL REFERENCES agents(id)
);
CREATE TABLE IF NOT EXISTS operator_message_reads (
  message_id TEXT PRIMARY KEY REFERENCES messages(id),
  read_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS attachments (
  id TEXT PRIMARY KEY, message_id TEXT NOT NULL REFERENCES messages(id),
  name TEXT NOT NULL, media_type TEXT NOT NULL, content BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS attachments_message ON attachments(message_id);
CREATE TABLE IF NOT EXISTS attachment_shares (
  message_id TEXT NOT NULL REFERENCES messages(id),
  attachment_id TEXT NOT NULL REFERENCES attachments(id),
  PRIMARY KEY(message_id,attachment_id)
);
CREATE VIEW IF NOT EXISTS message_attachments AS
  SELECT id AS attachment_id,message_id FROM attachments
  UNION ALL
  SELECT attachment_id,message_id FROM attachment_shares;
CREATE TABLE IF NOT EXISTS approvals (
  id TEXT PRIMARY KEY, project_id TEXT REFERENCES projects(id), task_id TEXT REFERENCES tasks(id),
  requested_by TEXT NOT NULL REFERENCES agents(id), title TEXT NOT NULL, body TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('pending','approved','denied'))
);
CREATE TABLE IF NOT EXISTS approval_deliveries (
 approval_id TEXT PRIMARY KEY REFERENCES approvals(id),
 message_id TEXT NOT NULL REFERENCES messages(id)
);
CREATE TABLE IF NOT EXISTS approval_decision_notes (
 approval_id TEXT PRIMARY KEY REFERENCES approvals(id), note TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY, agent_id TEXT NOT NULL REFERENCES agents(id),
  project_id TEXT REFERENCES projects(id), task_id TEXT REFERENCES tasks(id),
  source_id TEXT NOT NULL REFERENCES messages(id), kind TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('pending','submitted','running','completed','uncertain')),
  request BLOB NOT NULL, result BLOB, diagnostic TEXT, next_at INTEGER NOT NULL,
  UNIQUE(agent_id,source_id,kind)
);
CREATE TABLE IF NOT EXISTS attachment_alias_first_claim (
 job_id TEXT PRIMARY KEY REFERENCES jobs(id)
);
CREATE INDEX IF NOT EXISTS jobs_ready ON jobs(status,next_at);

CREATE TABLE IF NOT EXISTS project_workspace_roots (
 project_id TEXT PRIMARY KEY REFERENCES projects(id), path TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS project_workspace_jobs (
 job_id TEXT PRIMARY KEY, path TEXT NOT NULL, created INTEGER NOT NULL DEFAULT 0 CHECK(created IN (0,1))
);
CREATE TABLE IF NOT EXISTS job_scratch (
 job_id TEXT PRIMARY KEY REFERENCES jobs(id), created INTEGER NOT NULL DEFAULT 0,
 cleaned INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS po_followups (
 source_id TEXT PRIMARY KEY REFERENCES messages(id),
 trigger_id TEXT NOT NULL REFERENCES jobs(id), ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 1 AND 2),
 phase TEXT NOT NULL CHECK(phase IN ('assigned','review','accepted')),
 assignment_id TEXT NOT NULL REFERENCES messages(id), note_id TEXT REFERENCES messages(id), review_id TEXT REFERENCES messages(id),
 UNIQUE(trigger_id,phase,ordinal)
);

CREATE TABLE IF NOT EXISTS po_assignment_corrections (
 source_id TEXT PRIMARY KEY REFERENCES po_followups(source_id)
);

CREATE TABLE IF NOT EXISTS po_parent_outcome_events (
 job_id TEXT PRIMARY KEY REFERENCES jobs(id),
 parent_task_id TEXT NOT NULL REFERENCES tasks(id),
 child_task_id TEXT NOT NULL REFERENCES tasks(id),
 delivered_source_id TEXT REFERENCES messages(id)
);

CREATE TABLE IF NOT EXISTS task_holds (
 job_id TEXT PRIMARY KEY REFERENCES jobs(id), task_id TEXT NOT NULL REFERENCES tasks(id),
 project_id TEXT NOT NULL REFERENCES projects(id), po_id TEXT NOT NULL REFERENCES agents(id),
 phase TEXT NOT NULL CHECK(phase IN ('assigned','review','accepted')),
 assignment_id TEXT NOT NULL REFERENCES messages(id), note_id TEXT REFERENCES messages(id), review_id TEXT REFERENCES messages(id),
 root_source_id TEXT NOT NULL REFERENCES messages(id), root_message_id TEXT REFERENCES messages(id),
 status TEXT NOT NULL CHECK(status IN ('active','resolved'))
);
CREATE TABLE IF NOT EXISTS task_local_holds (
 job_id TEXT PRIMARY KEY REFERENCES task_holds(job_id)
);
CREATE TABLE IF NOT EXISTS task_local_supervision_candidates (
 hold_job_id TEXT PRIMARY KEY REFERENCES task_local_holds(job_id)
);
CREATE TABLE IF NOT EXISTS task_internal_wakes (
 source_id TEXT PRIMARY KEY REFERENCES messages(id)
);
CREATE TABLE IF NOT EXISTS task_local_supervisions (
 hold_job_id TEXT PRIMARY KEY REFERENCES task_holds(job_id),
 source_id TEXT NOT NULL UNIQUE REFERENCES messages(id),
 recovery_task_id TEXT UNIQUE REFERENCES tasks(id)
);
CREATE TABLE IF NOT EXISTS local_hold_investigations (
 source_id TEXT PRIMARY KEY REFERENCES messages(id),
 hold_job_id TEXT NOT NULL UNIQUE REFERENCES task_holds(job_id),
 supervisor_source_id TEXT NOT NULL UNIQUE REFERENCES task_local_supervisions(source_id),
 assessment_id TEXT UNIQUE REFERENCES messages(id)
);

CREATE TABLE IF NOT EXISTS project_goals (
 project_id TEXT PRIMARY KEY REFERENCES projects(id),
 objective TEXT NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('active','paused','complete'))
);
CREATE TABLE IF NOT EXISTS project_goal_revisions (
 project_id TEXT PRIMARY KEY REFERENCES project_goals(project_id), revision TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS root_backlog_grants (
 origin_id TEXT PRIMARY KEY REFERENCES messages(id), project_id TEXT NOT NULL REFERENCES projects(id),
 po_id TEXT NOT NULL REFERENCES agents(id), goal_revision TEXT NOT NULL, scope TEXT NOT NULL,
 UNIQUE(project_id,goal_revision)
);
CREATE TABLE IF NOT EXISTS root_backlog_wakes (
 parent_task_id TEXT PRIMARY KEY REFERENCES tasks(id), source_id TEXT NOT NULL UNIQUE REFERENCES messages(id),
 origin_id TEXT NOT NULL REFERENCES messages(id), project_id TEXT NOT NULL REFERENCES projects(id),
 po_id TEXT NOT NULL REFERENCES agents(id), objective TEXT NOT NULL, next_task_id TEXT UNIQUE REFERENCES tasks(id)
);
CREATE TABLE IF NOT EXISTS root_backlog_candidates (
 parent_task_id TEXT PRIMARY KEY REFERENCES tasks(id), report_message_id TEXT NOT NULL REFERENCES messages(id),
 objective TEXT NOT NULL
);
CREATE VIEW IF NOT EXISTS authorized_root_backlog_intakes AS
 SELECT grant.origin_id,grant.project_id,grant.po_id,grant.scope,g.objective
 FROM root_backlog_grants grant
 JOIN project_goal_revisions rev ON rev.project_id=grant.project_id AND rev.revision=grant.goal_revision
 JOIN project_goals g ON g.project_id=rev.project_id AND g.status='active'
 JOIN projects p ON p.id=grant.project_id AND p.po_id=grant.po_id
 JOIN agents po ON po.id=p.po_id AND po.project_id=p.id AND po.role='po'
 JOIN messages m ON m.id=grant.origin_id AND m.sender='operator' AND m.recipient='root'
  AND m.role='operator' AND m.room_id='dm:operator:root' AND m.project_id IS NULL AND m.task_id IS NULL;
CREATE VIEW IF NOT EXISTS eligible_root_backlog_wakes AS
 SELECT w.source_id,w.parent_task_id,w.project_id,w.po_id,w.origin_id
 FROM root_backlog_wakes w
 JOIN authorized_root_backlog_intakes a ON a.origin_id=w.origin_id AND a.project_id=w.project_id
  AND a.po_id=w.po_id AND a.objective=w.objective
 JOIN projects p ON p.id=w.project_id AND p.stopped=0
 JOIN tasks t ON t.id=w.parent_task_id AND t.project_id=w.project_id AND t.owner_id=w.po_id AND t.status='completed'
 JOIN task_origins o ON o.task_id=t.id AND o.message_id=a.origin_id
 WHERE w.next_task_id IS NULL
  AND NOT EXISTS(SELECT 1 FROM approvals ap WHERE ap.task_id=t.id AND ap.status='pending')
  AND NOT EXISTS(SELECT 1 FROM task_holds h WHERE h.task_id=t.id AND h.status='active')
  AND NOT EXISTS(SELECT 1 FROM jobs x WHERE x.task_id=t.id AND x.status='uncertain');

CREATE TABLE IF NOT EXISTS usage_settings (
 id INTEGER PRIMARY KEY CHECK(id=1), enabled INTEGER NOT NULL CHECK(enabled IN (0,1)),
 warn INTEGER NOT NULL CHECK(warn BETWEEN 0 AND 100), reserve INTEGER NOT NULL CHECK(reserve BETWEEN 0 AND warn)
);
CREATE TABLE IF NOT EXISTS model_preferences (
 id INTEGER PRIMARY KEY CHECK(id=1), reasoning TEXT NOT NULL DEFAULT '', implementation TEXT NOT NULL DEFAULT '',
 precise_edits TEXT NOT NULL DEFAULT '', research TEXT NOT NULL DEFAULT '', routine_checks TEXT NOT NULL DEFAULT ''
);
INSERT OR IGNORE INTO model_preferences VALUES (1,'','','','','');
CREATE TABLE IF NOT EXISTS codex_source_settings (
 id INTEGER PRIMARY KEY CHECK(id=1), source TEXT NOT NULL CHECK(source IN ('managed','override'))
);
INSERT OR IGNORE INTO usage_settings VALUES (1,0,40,30);

CREATE TABLE IF NOT EXISTS workflow_notices (
 event_key TEXT PRIMARY KEY, message_id TEXT NOT NULL UNIQUE REFERENCES messages(id),
 project_id TEXT NOT NULL REFERENCES projects(id)
);

CREATE TABLE IF NOT EXISTS po_work_items (
 parent_task_id TEXT NOT NULL REFERENCES tasks(id),
 child_task_id TEXT PRIMARY KEY REFERENCES tasks(id)
);
CREATE INDEX IF NOT EXISTS po_work_items_parent ON po_work_items(parent_task_id);
CREATE TABLE IF NOT EXISTS po_work_dependencies (
 child_task_id TEXT NOT NULL REFERENCES po_work_items(child_task_id),
 prerequisite_task_id TEXT NOT NULL REFERENCES po_work_items(child_task_id),
 PRIMARY KEY(child_task_id, prerequisite_task_id)
);
CREATE TABLE IF NOT EXISTS po_planning_wakes (
 source_id TEXT PRIMARY KEY REFERENCES messages(id)
);
CREATE TABLE IF NOT EXISTS po_report_recoveries (
 source_id TEXT PRIMARY KEY REFERENCES messages(id),
 task_id TEXT NOT NULL UNIQUE REFERENCES tasks(id),
 child_id TEXT NOT NULL REFERENCES tasks(id),
 failed_job_id TEXT NOT NULL UNIQUE REFERENCES jobs(id)
);
CREATE TABLE IF NOT EXISTS approval_planning_recoveries (
 source_id TEXT PRIMARY KEY REFERENCES messages(id),
 task_id TEXT NOT NULL UNIQUE REFERENCES tasks(id),
 failed_job_id TEXT NOT NULL UNIQUE REFERENCES jobs(id),
 decision_id TEXT NOT NULL REFERENCES messages(id)
);
CREATE TABLE IF NOT EXISTS po_child_receipt_recoveries (
 source_id TEXT PRIMARY KEY REFERENCES messages(id),
 task_id TEXT NOT NULL UNIQUE REFERENCES tasks(id),
 review_job_id TEXT NOT NULL UNIQUE REFERENCES jobs(id),
 parent_task_id TEXT NOT NULL REFERENCES tasks(id)
);
CREATE TABLE IF NOT EXISTS po_child_receipt_corrections (
 source_id TEXT PRIMARY KEY REFERENCES messages(id),
 task_id TEXT NOT NULL REFERENCES tasks(id),
 review_job_id TEXT NOT NULL UNIQUE REFERENCES jobs(id),
 parent_task_id TEXT NOT NULL REFERENCES tasks(id)
);

CREATE TABLE IF NOT EXISTS po_progress_notes (
 message_id TEXT PRIMARY KEY REFERENCES messages(id)
);
CREATE TABLE IF NOT EXISTS root_evidence_handoffs (
 message_id TEXT PRIMARY KEY REFERENCES messages(id),
 source_id TEXT NOT NULL UNIQUE REFERENCES messages(id),
 project_id TEXT NOT NULL REFERENCES projects(id)
);
CREATE TABLE IF NOT EXISTS po_evidence_wakes (
 source_id TEXT PRIMARY KEY REFERENCES messages(id), project_id TEXT NOT NULL REFERENCES projects(id),
 po_id TEXT NOT NULL REFERENCES agents(id)
);
CREATE TABLE IF NOT EXISTS po_evidence_wake_items (
 source_id TEXT NOT NULL REFERENCES po_evidence_wakes(source_id),
 message_id TEXT NOT NULL UNIQUE REFERENCES messages(id),
 consumed_job_id TEXT REFERENCES jobs(id), PRIMARY KEY(source_id,message_id)
);
CREATE TABLE IF NOT EXISTS po_evidence_assessments (
 job_id TEXT PRIMARY KEY REFERENCES jobs(id), message_id TEXT NOT NULL UNIQUE REFERENCES messages(id)
);
CREATE TABLE IF NOT EXISTS po_evidence_rejections (
 message_id TEXT PRIMARY KEY REFERENCES messages(id), job_id TEXT NOT NULL REFERENCES jobs(id)
);
CREATE TABLE IF NOT EXISTS po_explicit_progress (
 message_id TEXT PRIMARY KEY REFERENCES po_progress_notes(message_id)
);

CREATE TABLE IF NOT EXISTS root_owned_holds (
 job_id TEXT PRIMARY KEY REFERENCES task_holds(job_id)
);
CREATE TABLE IF NOT EXISTS root_hold_approvals (
 approval_id TEXT PRIMARY KEY REFERENCES approvals(id),
 hold_job_id TEXT NOT NULL UNIQUE REFERENCES root_owned_holds(job_id)
);
CREATE TABLE IF NOT EXISTS root_hold_decisions (
 task_id TEXT PRIMARY KEY REFERENCES tasks(id),
 hold_job_id TEXT NOT NULL REFERENCES task_holds(job_id),
 note_id TEXT NOT NULL REFERENCES messages(id)
);

-- Operator explicitly chose the already-recorded Root draft while retaining
-- the uncertain scope-review receipt and its unknown effects.
CREATE TABLE IF NOT EXISTS scope_draft_confirmations (
 request_id TEXT PRIMARY KEY,
 task_id TEXT NOT NULL UNIQUE REFERENCES tasks(id),
 review_job_id TEXT NOT NULL UNIQUE REFERENCES jobs(id),
 draft_message_id TEXT NOT NULL REFERENCES messages(id),
 assignment_job_id TEXT NOT NULL UNIQUE REFERENCES jobs(id),
 confirmed_by TEXT NOT NULL CHECK(confirmed_by='operator'),
 effects_unknown_ack INTEGER NOT NULL CHECK(effects_unknown_ack=1),
 created_at INTEGER NOT NULL
);
