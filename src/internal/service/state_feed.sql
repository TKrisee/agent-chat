-- Keep the newest revision for each changed entity. Revisions advance in the
-- same transaction as source mutations, and state is projected from those rows.
CREATE TABLE IF NOT EXISTS state_feed_generation (
 id INTEGER PRIMARY KEY CHECK(id=1), generation TEXT NOT NULL
);
INSERT OR IGNORE INTO state_feed_generation(id,generation) VALUES(1,lower(hex(randomblob(16))));
CREATE TABLE IF NOT EXISTS state_feed (
 version INTEGER PRIMARY KEY AUTOINCREMENT,
 kind TEXT NOT NULL, entity TEXT NOT NULL, UNIQUE(kind,entity)
);

CREATE INDEX IF NOT EXISTS state_jobs_agent_status ON jobs(agent_id,status);
CREATE INDEX IF NOT EXISTS state_jobs_project_status ON jobs(project_id,status);
CREATE INDEX IF NOT EXISTS state_jobs_task ON jobs(task_id);
CREATE INDEX IF NOT EXISTS state_tasks_project ON tasks(project_id);
CREATE INDEX IF NOT EXISTS state_agents_project ON agents(project_id);
CREATE INDEX IF NOT EXISTS state_approvals_task ON approvals(task_id);
CREATE INDEX IF NOT EXISTS state_approval_delivery_message ON approval_deliveries(message_id);
CREATE INDEX IF NOT EXISTS state_dependencies_prerequisite ON po_work_dependencies(prerequisite_task_id);

CREATE TRIGGER IF NOT EXISTS state_feed_agents_insert AFTER INSERT ON agents
BEGIN
 INSERT INTO state_feed(kind,entity)
  SELECT 'agent',NEW.id WHERE NEW.id IS NOT NULL
  ON CONFLICT(kind,entity) DO UPDATE SET version=excluded.version;
END;

CREATE TRIGGER IF NOT EXISTS state_feed_projects_insert AFTER INSERT ON projects
BEGIN
 INSERT INTO state_feed(kind,entity)
  SELECT 'project',NEW.id WHERE NEW.id IS NOT NULL
  ON CONFLICT(kind,entity) DO UPDATE SET version=excluded.version;
END;

CREATE TRIGGER IF NOT EXISTS state_feed_projects_update AFTER UPDATE ON projects
BEGIN
 INSERT INTO state_feed(kind,entity)
  SELECT 'project',NEW.id WHERE NEW.id IS NOT NULL
  UNION ALL SELECT 'task',id FROM tasks WHERE project_id=NEW.id AND OLD.stopped<>NEW.stopped AND status IN ('assigned','blocked')
  UNION ALL SELECT 'agent',id FROM agents WHERE project_id=NEW.id AND OLD.stopped<>NEW.stopped
  ON CONFLICT(kind,entity) DO UPDATE SET version=excluded.version;
END;

CREATE TRIGGER IF NOT EXISTS state_feed_tasks_insert AFTER INSERT ON tasks
BEGIN
 INSERT INTO state_feed(kind,entity)
  SELECT 'task',NEW.id WHERE NEW.id IS NOT NULL
  UNION ALL SELECT 'task',parent_task_id FROM po_work_items WHERE child_task_id=NEW.id
  UNION ALL SELECT 'task',child_task_id FROM po_work_items WHERE parent_task_id=NEW.id
  UNION ALL SELECT 'task',child_task_id FROM po_work_dependencies WHERE prerequisite_task_id=NEW.id
  UNION ALL SELECT 'agent',agent_id FROM one_shot_workers WHERE child_task_id=NEW.id
  ON CONFLICT(kind,entity) DO UPDATE SET version=excluded.version;
END;

CREATE TRIGGER IF NOT EXISTS state_feed_tasks_update AFTER UPDATE ON tasks
BEGIN
 INSERT INTO state_feed(kind,entity)
  SELECT 'task',NEW.id WHERE NEW.id IS NOT NULL
  UNION ALL SELECT 'task',parent_task_id FROM po_work_items WHERE child_task_id=NEW.id
  UNION ALL SELECT 'task',child_task_id FROM po_work_items WHERE parent_task_id=NEW.id
  UNION ALL SELECT 'task',child_task_id FROM po_work_dependencies WHERE prerequisite_task_id=NEW.id
  UNION ALL SELECT 'agent',agent_id FROM one_shot_workers WHERE child_task_id=NEW.id
  ON CONFLICT(kind,entity) DO UPDATE SET version=excluded.version;
END;

CREATE TRIGGER IF NOT EXISTS state_feed_approvals_insert AFTER INSERT ON approvals
BEGIN
 INSERT INTO state_feed(kind,entity)
  SELECT 'approval',NEW.id WHERE NEW.id IS NOT NULL
  UNION ALL SELECT 'task',NEW.task_id WHERE NEW.task_id IS NOT NULL
  UNION ALL SELECT 'task',parent_task_id FROM po_work_items WHERE child_task_id=NEW.task_id
  UNION ALL SELECT 'agent',agent_id FROM one_shot_workers WHERE child_task_id=NEW.task_id
  ON CONFLICT(kind,entity) DO UPDATE SET version=excluded.version;
END;

CREATE TRIGGER IF NOT EXISTS state_feed_approvals_update AFTER UPDATE ON approvals
BEGIN
 INSERT INTO state_feed(kind,entity)
  SELECT 'approval',NEW.id WHERE NEW.id IS NOT NULL
  UNION ALL SELECT 'task',NEW.task_id WHERE NEW.task_id IS NOT NULL
  UNION ALL SELECT 'task',parent_task_id FROM po_work_items WHERE child_task_id=NEW.task_id
  UNION ALL SELECT 'agent',agent_id FROM one_shot_workers WHERE child_task_id=NEW.task_id
  ON CONFLICT(kind,entity) DO UPDATE SET version=excluded.version;
END;

CREATE TRIGGER IF NOT EXISTS state_feed_jobs_insert AFTER INSERT ON jobs
BEGIN
 INSERT INTO state_feed(kind,entity)
  SELECT 'job',NEW.id WHERE NEW.id IS NOT NULL
  UNION ALL SELECT 'project',NEW.project_id WHERE NEW.project_id IS NOT NULL AND NEW.status IN ('submitted','running','uncertain')
  UNION ALL SELECT 'agent',NEW.agent_id WHERE NEW.status IN ('submitted','running','uncertain')
  UNION ALL SELECT 'task',NEW.task_id WHERE NEW.task_id IS NOT NULL
  UNION ALL SELECT 'task',parent_task_id FROM po_work_items WHERE child_task_id=NEW.task_id
  UNION ALL SELECT 'agent',agent_id FROM one_shot_workers WHERE child_task_id=NEW.task_id
  UNION ALL SELECT 'message',n.message_id FROM workflow_notices n WHERE NEW.kind='scope_review' AND n.project_id=NEW.project_id AND n.event_key='scope:'||NEW.task_id
  ON CONFLICT(kind,entity) DO UPDATE SET version=excluded.version;
END;

CREATE TRIGGER IF NOT EXISTS state_feed_jobs_update AFTER UPDATE OF status,diagnostic ON jobs WHEN OLD.status<>NEW.status OR OLD.diagnostic IS NOT NEW.diagnostic
BEGIN
 INSERT INTO state_feed(kind,entity)
  SELECT 'job',NEW.id WHERE NEW.id IS NOT NULL
  UNION ALL SELECT 'project',NEW.project_id WHERE NEW.project_id IS NOT NULL AND (OLD.status IN ('submitted','running','uncertain'))<>(NEW.status IN ('submitted','running','uncertain'))
  UNION ALL SELECT 'agent',NEW.agent_id WHERE OLD.status<>NEW.status
  UNION ALL SELECT 'task',NEW.task_id WHERE NEW.task_id IS NOT NULL
  UNION ALL SELECT 'task',parent_task_id FROM po_work_items WHERE child_task_id=NEW.task_id
  UNION ALL SELECT 'agent',agent_id FROM one_shot_workers WHERE child_task_id=NEW.task_id
  ON CONFLICT(kind,entity) DO UPDATE SET version=excluded.version;
END;

CREATE TRIGGER IF NOT EXISTS state_feed_messages_insert AFTER INSERT ON messages
BEGIN
 INSERT INTO state_feed(kind,entity)
  SELECT 'message',NEW.id WHERE ((NEW.room_id LIKE 'dm:%' AND (NEW.sender='operator' OR NEW.recipient='operator')) OR NEW.room_id='group:'||NEW.project_id)
  ON CONFLICT(kind,entity) DO UPDATE SET version=excluded.version;
END;

CREATE TRIGGER IF NOT EXISTS state_feed_messages_update AFTER UPDATE ON messages
BEGIN
 INSERT INTO state_feed(kind,entity)
  SELECT 'message',NEW.id WHERE (((NEW.room_id LIKE 'dm:%' AND (NEW.sender='operator' OR NEW.recipient='operator')) OR NEW.room_id='group:'||NEW.project_id) OR ((OLD.room_id LIKE 'dm:%' AND (OLD.sender='operator' OR OLD.recipient='operator')) OR OLD.room_id='group:'||OLD.project_id))
  ON CONFLICT(kind,entity) DO UPDATE SET version=excluded.version;
END;

CREATE TRIGGER IF NOT EXISTS state_feed_message_replies_insert AFTER INSERT ON message_replies
BEGIN
 INSERT INTO state_feed(kind,entity)
  SELECT 'message',m.id FROM messages m WHERE m.id=NEW.message_id AND ((m.room_id LIKE 'dm:%' AND (m.sender='operator' OR m.recipient='operator')) OR m.room_id='group:'||m.project_id)
  ON CONFLICT(kind,entity) DO UPDATE SET version=excluded.version;
END;

CREATE TRIGGER IF NOT EXISTS state_feed_message_mentions_insert AFTER INSERT ON message_mentions
BEGIN
 INSERT INTO state_feed(kind,entity)
  SELECT 'message',m.id FROM messages m WHERE m.id=NEW.message_id AND ((m.room_id LIKE 'dm:%' AND (m.sender='operator' OR m.recipient='operator')) OR m.room_id='group:'||m.project_id)
  ON CONFLICT(kind,entity) DO UPDATE SET version=excluded.version;
END;

CREATE TRIGGER IF NOT EXISTS state_feed_operator_message_reads_insert AFTER INSERT ON operator_message_reads
BEGIN
 INSERT INTO state_feed(kind,entity)
  SELECT 'message',m.id FROM messages m WHERE m.id=NEW.message_id AND ((m.room_id LIKE 'dm:%' AND (m.sender='operator' OR m.recipient='operator')) OR m.room_id='group:'||m.project_id)
  ON CONFLICT(kind,entity) DO UPDATE SET version=excluded.version;
END;

CREATE TRIGGER IF NOT EXISTS state_feed_attachments_insert AFTER INSERT ON attachments
BEGIN
 INSERT INTO state_feed(kind,entity)
  SELECT 'message',m.id FROM messages m WHERE m.id=NEW.message_id AND ((m.room_id LIKE 'dm:%' AND (m.sender='operator' OR m.recipient='operator')) OR m.room_id='group:'||m.project_id)
  ON CONFLICT(kind,entity) DO UPDATE SET version=excluded.version;
END;

CREATE TRIGGER IF NOT EXISTS state_feed_attachment_shares_insert AFTER INSERT ON attachment_shares
BEGIN
 INSERT INTO state_feed(kind,entity)
  SELECT 'message',m.id FROM messages m WHERE m.id=NEW.message_id AND ((m.room_id LIKE 'dm:%' AND (m.sender='operator' OR m.recipient='operator')) OR m.room_id='group:'||m.project_id)
  ON CONFLICT(kind,entity) DO UPDATE SET version=excluded.version;
END;

CREATE TRIGGER IF NOT EXISTS state_feed_approval_deliveries_insert AFTER INSERT ON approval_deliveries
BEGIN
 INSERT INTO state_feed(kind,entity)
  SELECT 'message',m.id FROM messages m WHERE m.id=NEW.message_id AND ((m.room_id LIKE 'dm:%' AND (m.sender='operator' OR m.recipient='operator')) OR m.room_id='group:'||m.project_id)
  ON CONFLICT(kind,entity) DO UPDATE SET version=excluded.version;
END;

CREATE TRIGGER IF NOT EXISTS state_feed_workflow_notices_insert AFTER INSERT ON workflow_notices
BEGIN
 INSERT INTO state_feed(kind,entity)
  SELECT 'message',m.id FROM messages m WHERE m.id=NEW.message_id AND ((m.room_id LIKE 'dm:%' AND (m.sender='operator' OR m.recipient='operator')) OR m.room_id='group:'||m.project_id)
  ON CONFLICT(kind,entity) DO UPDATE SET version=excluded.version;
END;

CREATE TRIGGER IF NOT EXISTS state_feed_project_workspace_roots_insert AFTER INSERT ON project_workspace_roots
BEGIN
 INSERT INTO state_feed(kind,entity)
  SELECT 'project',NEW.project_id WHERE NEW.project_id IS NOT NULL
  ON CONFLICT(kind,entity) DO UPDATE SET version=excluded.version;
END;

CREATE TRIGGER IF NOT EXISTS state_feed_project_workspace_roots_update AFTER UPDATE ON project_workspace_roots
BEGIN
 INSERT INTO state_feed(kind,entity)
  SELECT 'project',NEW.project_id WHERE NEW.project_id IS NOT NULL
  ON CONFLICT(kind,entity) DO UPDATE SET version=excluded.version;
END;

CREATE TRIGGER IF NOT EXISTS state_feed_project_goals_insert AFTER INSERT ON project_goals
BEGIN
 INSERT INTO state_feed(kind,entity)
  SELECT 'project',NEW.project_id WHERE NEW.project_id IS NOT NULL
  ON CONFLICT(kind,entity) DO UPDATE SET version=excluded.version;
END;

CREATE TRIGGER IF NOT EXISTS state_feed_project_goals_update AFTER UPDATE ON project_goals
BEGIN
 INSERT INTO state_feed(kind,entity)
  SELECT 'project',NEW.project_id WHERE NEW.project_id IS NOT NULL
  ON CONFLICT(kind,entity) DO UPDATE SET version=excluded.version;
END;

CREATE TRIGGER IF NOT EXISTS state_feed_approval_decision_notes_insert AFTER INSERT ON approval_decision_notes
BEGIN
 INSERT INTO state_feed(kind,entity)
  SELECT 'approval',NEW.approval_id WHERE NEW.approval_id IS NOT NULL
  ON CONFLICT(kind,entity) DO UPDATE SET version=excluded.version;
END;

CREATE TRIGGER IF NOT EXISTS state_feed_po_work_dependencies_insert AFTER INSERT ON po_work_dependencies
BEGIN
 INSERT INTO state_feed(kind,entity)
  SELECT 'task',NEW.child_task_id WHERE NEW.child_task_id IS NOT NULL
  ON CONFLICT(kind,entity) DO UPDATE SET version=excluded.version;
END;

CREATE TRIGGER IF NOT EXISTS state_feed_one_shot_workers_insert AFTER INSERT ON one_shot_workers
BEGIN
 INSERT INTO state_feed(kind,entity)
  SELECT 'agent',NEW.agent_id WHERE NEW.agent_id IS NOT NULL
  ON CONFLICT(kind,entity) DO UPDATE SET version=excluded.version;
END;

CREATE TRIGGER IF NOT EXISTS state_feed_one_shot_workers_update AFTER UPDATE ON one_shot_workers
BEGIN
 INSERT INTO state_feed(kind,entity)
  SELECT 'agent',NEW.agent_id WHERE NEW.agent_id IS NOT NULL
  ON CONFLICT(kind,entity) DO UPDATE SET version=excluded.version;
END;

CREATE TRIGGER IF NOT EXISTS state_feed_task_holds_insert AFTER INSERT ON task_holds
BEGIN
 INSERT INTO state_feed(kind,entity)
  SELECT 'task',NEW.task_id WHERE NEW.task_id IS NOT NULL
  UNION ALL SELECT 'task',parent_task_id FROM po_work_items WHERE child_task_id=NEW.task_id
  UNION ALL SELECT 'agent',agent_id FROM one_shot_workers WHERE child_task_id=NEW.task_id
  ON CONFLICT(kind,entity) DO UPDATE SET version=excluded.version;
END;

CREATE TRIGGER IF NOT EXISTS state_feed_task_holds_update AFTER UPDATE ON task_holds
BEGIN
 INSERT INTO state_feed(kind,entity)
  SELECT 'task',NEW.task_id WHERE NEW.task_id IS NOT NULL
  UNION ALL SELECT 'task',parent_task_id FROM po_work_items WHERE child_task_id=NEW.task_id
  UNION ALL SELECT 'agent',agent_id FROM one_shot_workers WHERE child_task_id=NEW.task_id
  ON CONFLICT(kind,entity) DO UPDATE SET version=excluded.version;
END;

CREATE TRIGGER IF NOT EXISTS state_feed_scope_draft_confirmations_insert AFTER INSERT ON scope_draft_confirmations
BEGIN
 INSERT INTO state_feed(kind,entity)
  SELECT 'job',NEW.review_job_id WHERE NEW.review_job_id IS NOT NULL
  UNION ALL SELECT 'task',NEW.task_id WHERE NEW.task_id IS NOT NULL
  UNION ALL SELECT 'task',parent_task_id FROM po_work_items WHERE child_task_id=NEW.task_id
  ON CONFLICT(kind,entity) DO UPDATE SET version=excluded.version;
END;
