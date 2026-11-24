"""Durable assigned project work; native turn activity is deliberately unrelated.

This backend is called with an authenticated project's Coordinator. Routing/UI
adapters are separate: callers must not accept a session ID from an untrusted body.
"""
import time
import uuid
import sqlite3
import hashlib
from contextlib import closing, nullcontext
from pathlib import Path

from agent_chat.core import CoordError
from agent_chat.management import manager

RUNNABLE = ('queued', 'in_progress')
UNFINISHED = frozenset((*RUNNABLE, 'blocked'))
FINISHED = frozenset(('completed', 'cancelled'))
STATES = UNFINISHED | FINISHED
# Match the scope of direct Root worker creation.
_DIRECT_ROOT_PROJECT = 'project_5fb8d3d13ec348e0993b25a3f3169289'


def read_objectives(path, project):
    """Read an operator-visible projection without creating a project store."""
    try:
        with closing(sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True)) as db:
            db.row_factory = sqlite3.Row
            identity = db.execute("SELECT value FROM meta WHERE key='project_id'").fetchone()
            if (identity['value'] if identity else 'default') != project:
                return None
            tables = {row['name'] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if 'project_work' not in tables:
                return []
            rows = db.execute("SELECT id,source_request_id,owner_session,state,title FROM project_work "
                              "WHERE project_id=? AND kind='goal' ORDER BY created_at,id", (project,)).fetchall()
            result = []
            for row in rows:
                item = dict(row)
                metadata = (db.execute('SELECT next_action FROM project_work_metadata WHERE work_id=?', (row['id'],)).fetchone()
                            if 'project_work_metadata' in tables else None)
                item['next_action'] = metadata['next_action'] if metadata else None
                item['dependencies'] = ([entry['dependency_id'] for entry in db.execute(
                    'SELECT dependency_id FROM project_work_dependencies WHERE work_id=? ORDER BY dependency_id', (row['id'],))]
                    if metadata and 'project_work_dependencies' in tables else None)
                result.append(item)
            return result
    except (OSError, sqlite3.Error):
        return None


def non_runnable_task_sources(db):
    """Withhold task execution without consuming its original assignment.

    Ordinary messages and goals do not participate in task admission. A task's
    own state governs it even when its parent goal has finished.
    """
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='project_work'").fetchone():
        return set()
    return {row['source_request_id'] for row in db.execute(
        "SELECT source_request_id FROM project_work WHERE kind='task' AND state NOT IN (?,?)", RUNNABLE)}


class ProjectActivity:
    """Unfinished states may move between queued/in_progress/blocked or finish.

    Finished work is immutable except for an identical state/replay request.
    Goals and tasks finish independently: any remaining unfinished task keeps
    the project active even after its parent goal has finished.
    """
    def __init__(self, projects, project, coord):
        if coord.path.resolve() != projects.db_path(project).resolve():
            raise CoordError('activity coordinator belongs to a different project')
        recorded = coord.db.execute("SELECT value FROM meta WHERE key='project_id'").fetchone()
        if (recorded['value'] if recorded else 'default') != project:
            raise CoordError('activity database project identity mismatch')
        self.projects, self.coord, self.project = projects, coord, project
        coord.require_session()
        coord.db.executescript('''
          CREATE TABLE IF NOT EXISTS project_work (
            id TEXT PRIMARY KEY, project_id TEXT NOT NULL,
            kind TEXT NOT NULL CHECK(kind IN ('goal','task')),
            goal_id TEXT, source_request_id TEXT NOT NULL UNIQUE,
            owner_session TEXT NOT NULL, owner_role TEXT NOT NULL,
            assigned_by TEXT NOT NULL, title TEXT NOT NULL,
            state TEXT NOT NULL CHECK(state IN ('queued','in_progress','blocked','completed','cancelled')),
            created_at REAL NOT NULL, updated_at REAL NOT NULL,
            CHECK((kind='goal' AND goal_id IS NULL AND owner_role='project_orchestrator')
               OR (kind='task' AND goal_id IS NOT NULL AND owner_role='worker')));
          CREATE INDEX IF NOT EXISTS project_work_goal ON project_work(goal_id);
          CREATE TABLE IF NOT EXISTS project_work_metadata (
            work_id TEXT PRIMARY KEY, next_action TEXT);
          CREATE TABLE IF NOT EXISTS project_work_dependencies (
            work_id TEXT NOT NULL, dependency_id TEXT NOT NULL,
            PRIMARY KEY(work_id, dependency_id));
          CREATE TABLE IF NOT EXISTS project_work_events (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT, work_id TEXT NOT NULL,
            actor_session TEXT NOT NULL, previous_state TEXT, state TEXT NOT NULL,
            recorded_at REAL NOT NULL);
          CREATE TABLE IF NOT EXISTS project_work_escalations (
            id TEXT PRIMARY KEY, origin_work_id TEXT NOT NULL,
            diagnostic_work_id TEXT NOT NULL UNIQUE, denial_message_id TEXT NOT NULL,
            next_action TEXT NOT NULL, requested_by TEXT NOT NULL, created_at REAL NOT NULL,
            purpose TEXT NOT NULL CHECK(purpose='diagnostic_repair'),
            denial_provenance TEXT NOT NULL CHECK(denial_provenance='owner_report'),
            CHECK(origin_work_id<>diagnostic_work_id));
          CREATE TRIGGER IF NOT EXISTS project_work_escalations_no_update
            BEFORE UPDATE ON project_work_escalations
            BEGIN SELECT RAISE(ABORT,'escalation association is immutable'); END;
          CREATE TRIGGER IF NOT EXISTS project_work_escalations_no_delete
            BEFORE DELETE ON project_work_escalations
            BEGIN SELECT RAISE(ABORT,'escalation association is immutable'); END;
        ''')
        from agent_chat.activity_evidence import install
        install(coord.db)

    def _po(self, db):
        if not self._table(db, 'project_orchestrators'):
            raise CoordError('project has no designated orchestrator')
        rows = db.execute('SELECT session_id,root_session FROM project_orchestrators').fetchall()
        if len(rows) != 1:
            raise CoordError('project must have one designated orchestrator')
        return rows[0]

    @staticmethod
    def _table(db, table):
        return db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()

    def _worker(self, db, worker, po):
        if worker == po or not db.execute('SELECT 1 FROM sessions WHERE id=?', (worker,)).fetchone():
            return False
        current, seen = worker, set()
        while current and current not in seen:
            seen.add(current)
            binding = (db.execute('SELECT parent_session FROM bridge_bindings WHERE session_id=?', (current,)).fetchone()
                       if self._table(db, 'bridge_bindings') else None)
            current = (binding['parent_session'] if binding else None) or manager(db, current)
            if current == po:
                return True
        return False

    def _authorize(self, db, po_only=False):
        actor = self.coord.require_session()
        po = self._po(db)
        if actor == po['session_id']:
            return actor, po
        if not po_only and (actor == po['root_session'] or self._worker(db, actor, po['session_id'])):
            return actor, po
        raise CoordError('activity requires the designated PO or its assigned team')

    def _snapshot_actor(self, db):
        if (self.project != _DIRECT_ROOT_PROJECT or not self._table(db, 'project_orchestrators')
                or db.execute('SELECT 1 FROM project_orchestrators LIMIT 1').fetchone()):
            return self._authorize(db)[0]

        actor = self.coord.require_session()
        with self.projects.connection() as directory:
            root = directory.execute('''
                SELECT p.session_id, c.home_project, c.home_session, c.thread_id, c.host_id
                FROM root_projects p
                JOIN root_config c ON c.singleton=1
                JOIN root_directory_owner o ON o.singleton=1
                    AND o.home_project=c.home_project AND o.home_session=c.home_session
                WHERE p.project_id=? AND p.joined=1 AND p.orchestrator_session IS NULL
                    AND c.home_project='default' AND c.mode='running'
            ''', (self.project,)).fetchone()
        if not root or not self._table(db, 'root_projections') or not db.execute('''
                SELECT 1 FROM root_projections p
                JOIN sessions s ON s.id=p.session_id
                JOIN bridge_bindings b ON b.session_id=p.session_id
                    AND b.thread_id=p.thread_id AND b.parent_session IS NULL
                JOIN remote_session_hosts h ON h.session_id=p.session_id
                WHERE p.session_id=? AND p.home_project=? AND p.home_session=?
                    AND p.thread_id=? AND h.host_id=? AND p.paused=0
            ''', tuple(root)).fetchone():
            raise CoordError('activity snapshot requires the current direct Root project projection')
        if actor != root['session_id']:
            from agent_chat.root_orchestrator import assert_management_target
            assert_management_target(db, root['session_id'], actor)
        return actor

    @staticmethod
    def _title(title):
        if not isinstance(title, str) or not title.strip() or len(title) > 500:
            raise CoordError('work title must contain 1–500 characters')
        return title.strip()

    @staticmethod
    def _source(db, source, sender, recipient):
        row = db.execute('SELECT sender_session,recipient_session FROM messages WHERE id=?', (source,)).fetchone()
        if not row or (row['sender_session'], row['recipient_session']) != (sender, recipient):
            raise CoordError('source request must be the exact routed assignment in this project')

    def _insert(self, db, kind, goal, source, owner, actor, title):
        old = db.execute('SELECT * FROM project_work WHERE source_request_id=?', (source,)).fetchone()
        values = (self.project, kind, goal, owner, actor, title)
        if old:
            if tuple(old[key] for key in ('project_id','kind','goal_id','owner_session','assigned_by','title')) != values:
                raise CoordError('source request is already adopted with different work')
            return dict(old)
        work, now = 'work_' + uuid.uuid4().hex, time.time()
        role = 'project_orchestrator' if kind == 'goal' else 'worker'
        db.execute('INSERT INTO project_work VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',
                   (work, self.project, kind, goal, source, owner, role, actor, title, 'queued', now, now))
        db.execute('INSERT INTO project_work_events(work_id,actor_session,previous_state,state,recorded_at) VALUES(?,?,NULL,?,?)',
                   (work, actor, 'queued', now))
        return dict(db.execute('SELECT * FROM project_work WHERE id=?', (work,)).fetchone())

    def adopt(self, source_request_id, title):
        title = self._title(title)
        with self.coord.tx() as db:
            actor, po = self._authorize(db, po_only=True)
            self._source(db, source_request_id, po['root_session'], actor)
            return self._insert(db, 'goal', None, source_request_id, actor, actor, title)

    def assign(self, goal_id, worker, source_request_id, title):
        title = self._title(title)
        with self.coord.tx() as db:
            actor, po = self._authorize(db, po_only=True)
            goal = self._work(db, goal_id)
            if goal['kind'] != 'goal' or goal['owner_session'] != actor:
                raise CoordError('task requires an owned PO goal')
            if not self._worker(db, worker, actor):
                raise CoordError('worker has no explicit delegation to this PO')
            self._source(db, source_request_id, actor, worker)
            old = db.execute('SELECT id FROM project_work WHERE source_request_id=?', (source_request_id,)).fetchone()
            if not old and goal['state'] in FINISHED:
                raise CoordError('cannot assign new work to a finished goal')
            return self._insert(db, 'task', goal_id, source_request_id, worker, actor, title)

    def _work(self, db, work_id):
        row = db.execute('SELECT * FROM project_work WHERE id=? AND project_id=?', (work_id, self.project)).fetchone()
        if not row:
            raise CoordError('unknown work in this project')
        return row

    @staticmethod
    def _with_metadata(db, row):
        result = dict(row)
        metadata = db.execute('SELECT next_action FROM project_work_metadata WHERE work_id=?', (row['id'],)).fetchone()
        result['next_action'] = metadata['next_action'] if metadata else None
        result['dependencies'] = ([r['dependency_id'] for r in db.execute(
            'SELECT dependency_id FROM project_work_dependencies WHERE work_id=? ORDER BY dependency_id', (row['id'],))]
            if metadata else None)
        return result

    def metadata(self, work_id, dependencies, next_action):
        if (not isinstance(dependencies, list) or len(dependencies) > 100 or
                any(not isinstance(value, str) or not value for value in dependencies) or
                len(set(dependencies)) != len(dependencies)):
            raise CoordError('dependencies must be distinct project work IDs')
        if next_action is not None and (not isinstance(next_action, str) or not next_action.strip() or len(next_action) > 2000):
            raise CoordError('next action must be null or nonempty bounded text')
        with self.coord.tx() as db:
            actor, po = self._authorize(db, po_only=True)
            work = self._work(db, work_id)
            if work['kind'] != 'goal' or work['owner_session'] != actor:
                raise CoordError('only the assigned PO may update an objective')
            for dependency in dependencies:
                if dependency == work_id:
                    raise CoordError('an objective cannot depend on itself')
                self._work(db, dependency)
            db.execute('INSERT INTO project_work_metadata(work_id,next_action) VALUES(?,?) '
                       'ON CONFLICT(work_id) DO UPDATE SET next_action=excluded.next_action', (work_id, next_action))
            db.execute('DELETE FROM project_work_dependencies WHERE work_id=?', (work_id,))
            db.executemany('INSERT INTO project_work_dependencies(work_id,dependency_id) VALUES(?,?)',
                           [(work_id, dependency) for dependency in dependencies])
            return self._with_metadata(db, work)

    def transition(self, work_id, state):
        if not isinstance(state, str) or state not in STATES:
            raise CoordError('unknown work state')
        with self.coord.tx() as db:
            actor, po = self._authorize(db)
            work = self._work(db, work_id)
            if actor not in (work['owner_session'], po['session_id']):
                raise CoordError('only the assigned owner or current PO may update work')
            if work['state'] == state:
                return dict(work)
            if work['state'] in FINISHED:
                raise CoordError('finished work cannot be reopened; use a new source request')
            now = time.time()
            db.execute('UPDATE project_work SET state=?,updated_at=? WHERE id=?', (state, now, work_id))
            db.execute('INSERT INTO project_work_events(work_id,actor_session,previous_state,state,recorded_at) VALUES(?,?,?,?,?)',
                       (work_id, actor, work['state'], state, now))
            return dict(self._work(db, work_id))

    def link_escalation(self, origin_work_id, diagnostic_work_id, denial_message_id, next_action):
        """Link a separately assigned repair task; never grant or transfer authority."""
        if not isinstance(next_action, str) or not next_action.strip() or len(next_action) > 2000:
            raise CoordError('escalation next action must contain 1–2000 characters')
        with self.coord.tx() as db:
            actor, _ = self._authorize(db, po_only=True)
            origin = self._work(db, origin_work_id)
            diagnostic = self._work(db, diagnostic_work_id)
            values = (origin_work_id, diagnostic_work_id, denial_message_id, next_action, actor)
            old = db.execute('SELECT * FROM project_work_escalations WHERE diagnostic_work_id=?',
                             (diagnostic_work_id,)).fetchone()
            if old:
                if tuple(old[key] for key in ('origin_work_id','diagnostic_work_id','denial_message_id',
                                               'next_action','requested_by')) != values:
                    raise CoordError('diagnostic task is already linked to a different escalation')
                return dict(old)
            if (origin['kind'] != 'task' or diagnostic['kind'] != 'task'
                    or origin_work_id == diagnostic_work_id or origin['state'] != 'blocked'
                    or diagnostic['state'] not in RUNNABLE
                    or origin['goal_id'] != diagnostic['goal_id']
                    or origin['assigned_by'] != actor or diagnostic['assigned_by'] != actor
                    or origin['owner_session'] == diagnostic['owner_session']):
                raise CoordError('escalation requires blocked work and a distinct specialist task under the same PO goal')
            if not all(self._worker(db, work['owner_session'], actor) for work in (origin, diagnostic)):
                raise CoordError('escalation owners require current delegation to this PO')
            for work in (origin, diagnostic):
                self._source(db, work['source_request_id'], actor, work['owner_session'])
            denial = db.execute('''SELECT m.sender_session,m.recipient_session,r.reply_to
                FROM messages m LEFT JOIN message_replies r ON r.message_id=m.id WHERE m.id=?''',
                (denial_message_id,)).fetchone()
            if (not denial or tuple(denial) != (origin['owner_session'], actor, origin['source_request_id'])):
                raise CoordError('denial report must be the original owner reply to its exact assignment')
            if not db.execute('''SELECT 1 FROM message_reads WHERE message_id=? AND reader_session=?
                AND provenance IN ('recipient_full_message','recipient_context_full')''',
                (denial_message_id, actor)).fetchone():
                raise CoordError('consume the complete denial report before linking diagnostic work')
            escalation = 'escalation_' + uuid.uuid4().hex
            db.execute('INSERT INTO project_work_escalations VALUES(?,?,?,?,?,?,?,?,?)',
                       (escalation, *values, time.time(), 'diagnostic_repair', 'owner_report'))
            return dict(db.execute('SELECT * FROM project_work_escalations WHERE id=?', (escalation,)).fetchone())

    def snapshot(self):
        directory_lock = self.projects.root_lock if self.project == _DIRECT_ROOT_PROJECT else nullcontext()
        with directory_lock, self.coord.tx() as db:
            actor = self._snapshot_actor(db)
            work = [self._with_metadata(db, row) for row in db.execute('SELECT * FROM project_work WHERE project_id=? ORDER BY created_at,id', (self.project,))]
            escalations = [dict(row) for row in db.execute('SELECT * FROM project_work_escalations ORDER BY created_at,id')]
            result = dict(project=self.project, active=any(row['state'] in UNFINISHED for row in work),
                          blocked=[row['id'] for row in work if row['state']=='blocked'], work=work,
                          escalations=escalations)
            if any(row['kind'] == 'task' and row['owner_session'] == actor and row['state'] in RUNNABLE for row in work):
                result['assignment_provenance_guidance'] = (
                    'After you fully read the source of your exact current owned task, you may send to one same-team peer '
                    'with --assignment-id TASK_ID to include task provenance. For an identical first-ACK retry, '
                    'preserve the same recipient, body, attachments, and same ID or absence of the ID. '
                    'Task provenance is metadata and does not grant authority.')
            return result

    def export(self):
        """Export maintained own-PO work, without consuming or accepting it.

        Message bodies are stored Unicode encoded as UTF-8 for hashing; these
        are not the original transport bytes. The ordinary authenticated remote
        dispatcher still performs its normal host-currentness bookkeeping.
        """
        with self.coord.tx() as db:
            actor, _ = self._authorize(db, po_only=True)
            rows = db.execute(
                "SELECT * FROM project_work WHERE project_id=? AND "
                "((kind='goal' AND owner_session=?) OR (kind='task' AND assigned_by=?)) "
                "ORDER BY created_at,id", (self.project, actor, actor)).fetchall()
            work, sources, events, gaps = [], [], [], []
            work_ids = {row['id'] for row in rows}
            states = {state: 0 for state in sorted(STATES)}
            kinds = {'goal': 0, 'task': 0}
            untracked = ('original_customer_request', 'change_evidence', 'validation',
                         'artifact_delivery', 'customer_acceptance')
            for row in rows:
                item = self._with_metadata(db, row)
                states[row['state']] += 1
                kinds[row['kind']] += 1
                source = db.execute(
                    'SELECT id,sender_session,recipient_session,body,created_at,acked_at '
                    'FROM messages WHERE id=?', (row['source_request_id'],)).fetchone()
                # Even a malformed retained work/source association cannot grant
                # this PO access to another actor's message body or metadata.
                reason = None
                if source is None:
                    reason = 'source_missing'
                elif actor not in (source['sender_session'], source['recipient_session']):
                    reason = 'source_inaccessible'
                elif ((row['kind'] == 'goal' and source['recipient_session'] != actor) or
                      (row['kind'] == 'task' and
                       (source['sender_session'], source['recipient_session']) != (actor, row['owner_session']))):
                    reason = 'source_route_mismatch'
                if reason:
                    gaps.append(dict(work_id=row['id'], source_id=row['source_request_id'], reason=reason))
                else:
                    exported = dict(source)
                    body_bytes = source['body'].encode('utf-8')
                    exported.update(body_encoding='utf-8', body_bytes=len(body_bytes),
                                    body_sha256=hashlib.sha256(body_bytes).hexdigest(),
                                    byte_scope='stored_body_utf8')
                    origin = db.execute('SELECT channel FROM operator_message_origins '
                                        'WHERE message_id=? AND sender_session=?',
                                        (source['id'], source['sender_session'])).fetchone()
                    exported['instruction_origin'] = 'authenticated_operator_ui' if origin else 'agent_message'
                    exported['source_channel'] = origin['channel'] if origin else None
                    exported['attachments'] = [dict(attachment) for attachment in db.execute(
                        'SELECT id,name,mime,size FROM attachments WHERE message_id=? ORDER BY rowid', (source['id'],))]
                    # Preserve the actual association only; never traverse an
                    # unrelated conversation or expand a batch's other recipients.
                    reply = db.execute('SELECT reply_to FROM message_replies WHERE message_id=?', (source['id'],)).fetchone()
                    exported['reply_to'] = reply['reply_to'] if reply else None
                    sources.append(exported)
                from agent_chat.activity_evidence import project as evidence_project
                item['request_evidence'] = evidence_project(db, row['id'])
                item['acceptance'] = {stage: dict(state='unknown', reason='not_tracked_by_activity_ledger')
                                      for stage in untracked}
                if item['request_evidence']:
                    item['acceptance']['original_customer_request'] = dict(state='recorded', count=len(item['request_evidence']), reason='server_stamped_origin_not_independent_customer_identity')
                    for kind, stage in (('change','change_evidence'),('validation','validation'),('delivery','artifact_delivery')):
                        count = sum(1 for request in item['request_evidence'] for event in request['events'] if event['kind']==kind)
                        if count: item['acceptance'][stage] = dict(state='reported', count=count, reason='scoped_record_not_independent_acceptance')
                    count = sum(len(request['customer_acceptance'].get('decisions', [])) for request in item['request_evidence'])
                    if count: item['acceptance']['customer_acceptance'] = dict(state='decisions_recorded', count=count, unknown_request_count=sum(1 for request in item['request_evidence'] if request['customer_acceptance']['state']=='unknown'), undecided_delivery_count=sum(request['customer_acceptance']['undecided_delivery_count'] for request in item['request_evidence']), reason='specific_delivery_decisions_only_not_task_completion')
                item['acceptance']['assigned_objective'] = dict(
                    state='source_unavailable' if reason else 'source_available',
                    source_id=row['source_request_id'], goal_id=row['goal_id'])
                if item['dependencies'] is None:
                    gaps.append(dict(work_id=row['id'], reason='dependencies_unrecorded'))
                else:
                    for dependency in item['dependencies']:
                        if dependency not in work_ids:
                            gaps.append(dict(work_id=row['id'], dependency_id=dependency,
                                             reason='dependency_outside_owned_export'))
                if row['goal_id'] and row['goal_id'] not in work_ids:
                    gaps.append(dict(work_id=row['id'], reason='parent_goal_outside_owned_export'))
                work.append(item)
                events.extend(dict(event) for event in db.execute(
                    'SELECT * FROM project_work_events WHERE work_id=? ORDER BY sequence', (row['id'],)))
            escalations = [dict(row) for row in db.execute('SELECT * FROM project_work_escalations ORDER BY created_at,id')
                           if row['origin_work_id'] in work_ids and row['diagnostic_work_id'] in work_ids]
            for stage in untracked:
                gaps.append(dict(stage=stage, reason='per_request_evidence_may_be_incomplete'))
            return dict(schema_version=1, project=self.project, generated_at=time.time(),
                        label='interim_incomplete',
                        scope=dict(kind='current_po_owned_backlog', owner_session=actor,
                                   includes_all_recorded_states=True, includes_other_po_work=False),
                        counts=dict(work=len(work), by_kind=kinds, by_state=states,
                                    source_associations=len(work), available_sources=len(sources),
                                    unavailable_sources=len(work)-len(sources), events=len(events),
                                    escalations=len(escalations), gaps=len(gaps)),
                        completeness=dict(owned_ledger='complete',
                            sources='complete' if len(work)==len(sources) else 'incomplete',
                            result_evidence=('partial' if any(request['events'] for item in work for request in item['request_evidence']) else 'untracked'), customer_acceptance=('decisions_recorded' if any(request['customer_acceptance']['state']=='decisions_recorded' for item in work for request in item['request_evidence']) else 'unknown')),
                        limits=['Recorded work states are actor reports, not validation or customer acceptance.',
                                'Root-routed agent instructions are not authenticated customer provenance.',
                                'Attachment metadata is not proof of delivery, retrieval or usable content.',
                                'Only current dependency metadata is stored; no dependency revision history is inferred.',
                                'Stored body UTF-8 bytes and hashes are not original transport-wire bytes.',
                                'This export does not read/ACK messages, change work or authorize execution.'],
                        work=work, sources=sources, events=events, escalations=escalations, gaps=gaps)

    def history(self, work_id):
        with self.coord.tx() as db:
            self._authorize(db)
            self._work(db, work_id)
            return [dict(row) for row in db.execute('SELECT * FROM project_work_events WHERE work_id=? ORDER BY sequence', (work_id,))]
