"""Durable, opt-in wake routes. Inbox and reservation state remain untouched."""
from __future__ import annotations

import time
import uuid
import json
import pathlib
import sqlite3

from .core import CoordError, Coordinator, assert_agent_running

TERMINAL = ('dispatched', 'cancelled')

def cold_load_pending(db, thread):
    return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='cold_message_loads'").fetchone()
                and db.execute("SELECT 1 FROM cold_message_loads WHERE thread_id=? AND status IN ('claimed','attempting')", (thread,)).fetchone())



def _first_full_read_at(db, message_id, reader_session):
    """Return the first typed full-read receipt for this exact message and reader."""
    columns = {row['name'] for row in db.execute('PRAGMA table_info(message_reads)')}
    if not {'message_id', 'reader_session', 'read_at', 'provenance'}.issubset(columns):
        return None
    rows = db.execute('''SELECT read_at FROM message_reads
        WHERE message_id=? AND reader_session=?
          AND provenance IN ('recipient_full_message','recipient_context_full')''',
        (message_id, reader_session)).fetchmany(2)
    return rows[0]['read_at'] if len(rows) == 1 else None


def _source_reply_trace(db, source):
    """Existing opposite-direction records, never native content or delivery proof.

    Sequence chooses the first server-recorded qualifying reply even if the wall
    clock steps. Full-read producers retain their first typed receipt; legacy
    markers and ACKs are not evidence of complete recipient consumption.
    """
    result = {'first_reply_message_id': None, 'first_reply_accepted_at': None,
              'first_reply_read_at': None}
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='message_replies'").fetchone():
        return result
    reply = db.execute('''SELECT m.id,m.created_at FROM message_replies r
        JOIN messages m ON m.id=r.message_id
        WHERE r.reply_to=? AND m.sender_session=? AND m.recipient_session=?
          AND NOT EXISTS (SELECT 1 FROM message_batches b WHERE b.message_id=m.id)
        ORDER BY m.seq LIMIT 1''',
        (source['id'], source['recipient_session'], source['sender_session'])).fetchone()
    if reply is None:
        return result
    result.update(first_reply_message_id=reply['id'], first_reply_accepted_at=reply['created_at'])
    result['first_reply_read_at'] = _first_full_read_at(db, reply['id'], source['sender_session'])
    return result


def _attempt_marker(db, job):
    """First recorded transition, converted from Julian days to Unix seconds.

    The trigger fires before a status update whose new status is neither
    prepared nor cancelled. This does not prove a queue call or acceptance.
    """
    if not job or not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='bridge_job_native_attempts'").fetchone():
        return None
    rows = db.execute('SELECT (recorded_at - 2440587.5) * 86400.0 AS recorded_at FROM bridge_job_native_attempts WHERE job_id=?', (job['id'],)).fetchmany(2)
    return rows[0]['recorded_at'] if len(rows) == 1 else None


def _native_stage_trace(db, job):
    unknown = {'status':'unknown', 'reason':'no_observation', 'attribution':'job_turn_only', 'coverage':'unverified'}
    if not job or not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='bridge_native_stage_observations'").fetchone():
        return unknown
    row = db.execute('SELECT * FROM bridge_native_stage_observations WHERE job_id=?', (job['id'],)).fetchone()
    if row is None:
        return unknown
    if row['censored']:
        return dict(unknown, reason=row['censor_reason'])
    lease = db.execute('SELECT owner,secret_hash,acquired_at FROM bridge_dispatcher WHERE host_id=?',(row['host_id'],)).fetchone()
    if not lease or (lease['owner'],lease['secret_hash'],lease['acquired_at']) != (row['lease_owner'],row['lease_hash'],row['lease_acquired']):
        return dict(unknown, reason='dispatcher_changed')
    return {'status':'observed', 'attribution':'job_turn_only', 'coverage':'unverified',
            'job_id':row['job_id'], 'turn_id':row['turn_id'], 'item_id':row['item_id'],
            'observed_at':row['observed_at'], 'observed_monotonic_ns':row['monotonic_ns'],
            'connection_instance':row['connection_instance'], 'connection_epoch':row['connection_epoch'],
            'sequence':row['sequence'], 'recorded_at':row['recorded_at']}


def _eligibility_trace(db, job):
    unknown = {'status': 'unknown', 'reason': 'no_observation'}
    if not job or not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='bridge_eligibility_observations'").fetchone():
        return unknown
    row = db.execute('SELECT * FROM bridge_eligibility_observations WHERE job_id=?', (job['id'],)).fetchone()
    if not row:
        return unknown
    lease = db.execute('SELECT owner,secret_hash,acquired_at FROM bridge_dispatcher WHERE host_id=?', (row['host_id'],)).fetchone()
    if not lease or tuple(lease) != (row['lease_owner'], row['lease_hash'], row['lease_acquired']):
        return {'status': 'unknown', 'reason': 'dispatcher_changed'}
    return {'status': 'observed', 'attribution': 'job_included_sources',
            'coverage': 'two_samples_not_continuous', 'clock_domain': 'host_monotonic_ns',
            'job_id': row['job_id'], 'thread_id': row['thread_id'], 'host_id': row['host_id'],
            'connection_instance': row['connection_instance'], 'connection_epoch': row['connection_epoch'],
            'start_monotonic_ns': row['start_monotonic_ns'], 'end_monotonic_ns': row['end_monotonic_ns']}


def source_trace(db_path, *, project_id, session_id, host_id, source_id):
    """Read only an exact direct source owned by this authenticated session."""
    if any(not isinstance(v, str) or not v or len(v) > 512
           for v in (project_id, session_id, host_id, source_id)):
        return {'status': 'unauthorized'}
    path = pathlib.Path(db_path)
    if not path.is_file():
        return {'status': 'unavailable'}
    conn = None
    try:
        conn = sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=3)
        conn.row_factory = sqlite3.Row
        conn.execute('BEGIN')
        schema = conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
        project = conn.execute("SELECT value FROM meta WHERE key='project_id'").fetchone()
        if not schema or schema['value'] != '1':
            return {'status': 'unavailable'}
        if (project and project['value'] != project_id) or (not project and project_id != 'default'):
            return {'status': 'unauthorized'}
        if (not conn.execute('SELECT 1 FROM sessions WHERE id=?', (session_id,)).fetchone()
                or conn.execute('SELECT 1 FROM retired_sessions WHERE id=?', (session_id,)).fetchone()):
            return {'status': 'unauthorized'}
        hosts = conn.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (session_id,)).fetchmany(2)
        if len(hosts) != 1 or hosts[0]['host_id'] != host_id:
            return {'status': 'unauthorized'}
        if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_team_reset_targets'").fetchone():
            if conn.execute('SELECT 1 FROM root_team_reset_targets WHERE session_id=?', (session_id,)).fetchone():
                return {'status': 'unauthorized'}
        rows = conn.execute('''SELECT id,sender_session,recipient_session,created_at FROM messages WHERE id=?''', (source_id,)).fetchmany(2)
        if len(rows) != 1:
            return {'status': 'unauthorized'}
        source = rows[0]
        if (not source['sender_session'] or not source['recipient_session']
                or source['sender_session'] == source['recipient_session']
                or session_id not in (source['sender_session'], source['recipient_session'])):
            return {'status': 'unauthorized'}
        if conn.execute('SELECT 1 FROM message_batches WHERE message_id=?', (source_id,)).fetchone():
            return {'status': 'unauthorized'}
        rows = conn.execute('''SELECT j.id,j.status,o.prepared_at FROM bridge_deliveries d
            JOIN bridge_jobs j ON j.id=d.job_id LEFT JOIN bridge_job_origins o ON o.job_id=j.id
            WHERE d.message_id=?''', (source_id,)).fetchmany(2)
        if len(rows) > 1:
            return {'status': 'unavailable'}
        job = rows[0] if rows else None
        return {'status':'ok', 'source_id':source_id, 'accepted_at':source['created_at'],
                'clock':'unix_wall_seconds', 'job_id':job['id'] if job else None,
                'job_status':job['status'] if job else None,
                'prepared_at':job['prepared_at'] if job else None,
                'eligibility_at':None, 'native_first_response_at':None, 'recipient_delivery_at':None,
                'source_first_full_read_at':_first_full_read_at(conn, source['id'], source['recipient_session']),
                'job_transition_recorded_at':_attempt_marker(conn, job),
                'job_eligibility_observation':_eligibility_trace(conn, job),
                'job_native_content':_native_stage_trace(conn, job),
                **_source_reply_trace(conn, source)}
    except sqlite3.Error:
        return {'status': 'unavailable'}
    finally:
        if conn is not None:
            conn.close()


def administrative_attachment_fenced(db, *, session=None, thread=None):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_admin_attachment_fences'").fetchone():
        return False
    return bool(db.execute('SELECT 1 FROM root_admin_attachment_fences WHERE (active=1 AND session_id=?) OR (active IN (1,2) AND old_thread_id=?) LIMIT 1',
                           (session, thread)).fetchone())


def administrative_attachment_job_fenced(db, job_id, thread):
    if administrative_attachment_fenced(db, thread=thread):
        return True
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_admin_attachment_fences'").fetchone():
        return False
    return bool(db.execute('''SELECT 1 FROM bridge_deliveries d JOIN messages m ON m.id=d.message_id
        JOIN root_admin_attachment_fences f ON f.session_id=m.recipient_session AND f.active=1
        WHERE d.job_id=? LIMIT 1''', (job_id,)).fetchone())


def runtime_snapshot(db, now=None):
    """Read bridge health without creating tables (also used by the web UI)."""
    now = time.time() if now is None else now
    def recent(row):
        item = dict(row)
        item['recent'] = item['pid'] != 0 and now - item['heartbeat'] < 30
        return item
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='bridge_client_runtime'").fetchone():
        clients = [recent(row) for row in db.execute('SELECT * FROM bridge_client_runtime ORDER BY host_id')]
        if clients:
            healthy = [item for item in clients if item['recent'] and not item['error']]
            latest = max(healthy or clients, key=lambda item: item['heartbeat'])
            return dict(latest, recent=any(item['recent'] for item in clients), clients=clients)
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='bridge_runtime'").fetchone():
        row = db.execute('SELECT * FROM bridge_runtime WHERE singleton=1').fetchone()
        if row:
            return recent(row)
    return None


def resolve_binding_route(db, session_id):
    """Resolve the maintained explicit parent chain without changing state."""
    route, seen = [], set()
    while session_id:
        if session_id in seen:
            raise CoordError('cyclic bridge parent route')
        seen.add(session_id)
        row = db.execute('SELECT * FROM bridge_bindings WHERE session_id=?', (session_id,)).fetchone()
        if not row:
            return None
        route.append(dict(row))
        if row['thread_id']:
            return {'thread_id': row['thread_id'], 'route': list(reversed(route))}
        session_id = row['parent_session']
    return None


class BridgeState:
    def __init__(self, coord: Coordinator):
        self.coord = coord
        self.db = coord.db
        self.policy_store = None
        self.root_restart = None
        import socket
        self.policy_host = socket.gethostname()
        self.policy_project = str(coord.path.resolve())
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS bridge_bindings (
                session_id TEXT PRIMARY KEY, thread_id TEXT UNIQUE,
                parent_session TEXT, agent_path TEXT, updated_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS bridge_jobs (
                id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, payload TEXT NOT NULL,
                status TEXT NOT NULL, queue_id TEXT, error TEXT,
                created_at REAL NOT NULL, updated_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS bridge_deliveries (
                message_id TEXT PRIMARY KEY, job_id TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS bridge_runtime (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                server TEXT NOT NULL, pid INTEGER NOT NULL,
                heartbeat REAL NOT NULL, error TEXT);
            CREATE TABLE IF NOT EXISTS bridge_client_runtime (
                host_id TEXT PRIMARY KEY, server TEXT NOT NULL, pid INTEGER NOT NULL,
                heartbeat REAL NOT NULL, error TEXT);
            CREATE TABLE IF NOT EXISTS bridge_observations (
                thread_id TEXT PRIMARY KEY, state TEXT NOT NULL,
                error TEXT, checked_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS bridge_job_origins (
                job_id TEXT PRIMARY KEY,prepared_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS bridge_job_native_attempts (
                job_id TEXT PRIMARY KEY,recorded_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS bridge_native_stage_observations (
                job_id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, host_id TEXT NOT NULL,
                turn_id TEXT NOT NULL, item_id TEXT NOT NULL,
                observed_at REAL NOT NULL, monotonic_ns INTEGER NOT NULL,
                connection_instance TEXT NOT NULL, connection_epoch INTEGER NOT NULL,
                sequence INTEGER NOT NULL, recorded_at REAL NOT NULL,
                censored INTEGER NOT NULL DEFAULT 0, censor_reason TEXT,
                lease_owner TEXT NOT NULL, lease_hash TEXT NOT NULL, lease_acquired REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS bridge_eligibility_observations (
                job_id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, host_id TEXT NOT NULL,
                connection_instance TEXT NOT NULL, connection_epoch INTEGER NOT NULL,
                start_monotonic_ns INTEGER NOT NULL, end_monotonic_ns INTEGER NOT NULL,
                lease_owner TEXT NOT NULL, lease_hash TEXT NOT NULL, lease_acquired REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS bridge_root_wake_batches (
                job_id TEXT PRIMARY KEY,request_id TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS manager_update_claims (
                message_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, original_job_id TEXT,
                turn_id TEXT NOT NULL, job_id TEXT NOT NULL UNIQUE, claimed_at REAL NOT NULL);
            CREATE TRIGGER IF NOT EXISTS bridge_job_native_attempt BEFORE UPDATE OF status ON bridge_jobs
                WHEN NEW.status NOT IN ('prepared','cancelled')
                BEGIN INSERT OR IGNORE INTO bridge_job_native_attempts VALUES(NEW.id,julianday('now')); END;
            CREATE TRIGGER IF NOT EXISTS immutable_bridge_job_origin_update BEFORE UPDATE ON bridge_job_origins
                BEGIN SELECT RAISE(ABORT,'wake preparation provenance is immutable'); END;
            CREATE TRIGGER IF NOT EXISTS immutable_bridge_job_origin_delete BEFORE DELETE ON bridge_job_origins
                BEGIN SELECT RAISE(ABORT,'wake preparation provenance is immutable'); END;
            CREATE TRIGGER IF NOT EXISTS immutable_bridge_job_attempt_update BEFORE UPDATE ON bridge_job_native_attempts
                BEGIN SELECT RAISE(ABORT,'wake submission evidence is immutable'); END;
            CREATE TRIGGER IF NOT EXISTS immutable_bridge_job_attempt_delete BEFORE DELETE ON bridge_job_native_attempts
                BEGIN SELECT RAISE(ABORT,'wake submission evidence is immutable'); END;
            CREATE TRIGGER IF NOT EXISTS immutable_manager_update_claim_update BEFORE UPDATE ON manager_update_claims
                BEGIN SELECT RAISE(ABORT,'manager update claim is immutable'); END;
            CREATE TRIGGER IF NOT EXISTS immutable_manager_update_claim_delete BEFORE DELETE ON manager_update_claims
                BEGIN SELECT RAISE(ABORT,'manager update claim is immutable'); END;
        ''')
        if 'project_scope_worker' not in {row['name'] for row in self.db.execute('PRAGMA table_info(bridge_client_runtime)')}:
            self.db.execute('ALTER TABLE bridge_client_runtime ADD COLUMN project_scope_worker INTEGER NOT NULL DEFAULT 0')
        if 'project_workspaces_worker' not in {row['name'] for row in self.db.execute('PRAGMA table_info(bridge_client_runtime)')}:
            self.db.execute('ALTER TABLE bridge_client_runtime ADD COLUMN project_workspaces_worker INTEGER NOT NULL DEFAULT 0')
        for name in ('running_version', 'installed_version'):
            if name not in {row['name'] for row in self.db.execute('PRAGMA table_info(bridge_client_runtime)')}:
                self.db.execute('ALTER TABLE bridge_client_runtime ADD COLUMN ' + name + ' TEXT')
        from .session_reset import SessionResetState
        SessionResetState(coord)
        from .agent_control import AgentControlState
        self.controls = AgentControlState(coord)
        from .capacity_recovery import CapacityState
        self.capacity = CapacityState(self)
        from .goal_selection_generation import install as install_goal_generation, PROJECT as GOAL_PROJECT
        install_goal_generation(self.db, GOAL_PROJECT)

    def capacity_targets(self): return self.capacity.targets()
    def capacity_jobs(self): return self.capacity.rows()
    def capacity_gate(self, session_id, thread_id):
        if administrative_attachment_fenced(self.db, session=session_id, thread=thread_id):
            return 'administrative attachment has unresolved old-thread work'
        return self.capacity.gate(session_id, thread_id)
    def capacity_schedule(self, session_id, thread_id, turn_id, now, settings_digest, model, reasoning_effort, owner):
        return self.capacity.schedule(session_id, thread_id, turn_id, now, settings_digest, model, reasoning_effort, owner)
    def capacity_update(self, job_id, expected, status, now, **values):
        return self.capacity.update(job_id, expected, status, now, **values)

    def resolve(self, session_id):
        """Return the root thread and explicit descendant route, or no binding."""
        return resolve_binding_route(self.db, session_id)

    def _assert_editable(self):
        # Parent rebinding could redirect descendants with queued input. Freeze
        # routes while a job needs reconciliation instead of silently rerouting it.
        caller = self.coord.require_session()
        if administrative_attachment_fenced(self.db, session=caller):
            raise CoordError('administrative attachment cutover is in progress')
        from .root_orchestrator import assert_local_lifecycle
        assert_local_lifecycle(self.db, caller)
        from .session_reset import assert_reset_unlocked
        assert_reset_unlocked(self.db, caller)
        assert_agent_running(self.db, caller)
        for row in self.db.execute("SELECT id,thread_id FROM bridge_jobs WHERE status NOT IN ('dispatched','cancelled')"):
            route = self.resolve(caller)
            if route and row['thread_id'] == route['thread_id']:
                raise CoordError('resolve outstanding bridge jobs before changing this route')

    def bind(self, thread_id=None, parent_session=None, agent_path=None):
        caller = self.coord.require_session()
        if bool(thread_id) == bool(parent_session):
            raise CoordError('choose --thread or --parent-session')
        if thread_id:
            try:
                thread_id = str(uuid.UUID(thread_id))
            except (ValueError, TypeError, AttributeError):
                raise CoordError('--thread must be the actual Codex conversation UUID')
            if agent_path:
                raise CoordError('--agent-path is only for a parent route')
        else:
            if not agent_path or not agent_path.startswith('/') or any(c.isspace() for c in agent_path):
                raise CoordError('parent routes require an exact canonical --agent-path, e.g. /root/reviewer')
            if not self.db.execute('SELECT 1 FROM sessions WHERE id=?', (parent_session,)).fetchone():
                raise CoordError('parent must be an exact registered coordination session ID')
        with self.coord.tx():
            if parent_session:
                from .root_orchestrator import assert_local_lifecycle
                assert_local_lifecycle(self.db, parent_session)
                from .session_reset import assert_reset_unlocked
                assert_reset_unlocked(self.db, parent_session)
                assert_agent_running(self.db, parent_session)
            old = self.db.execute('SELECT * FROM bridge_bindings WHERE session_id=?', (caller,)).fetchone()
            if old and (old['thread_id'], old['parent_session'], old['agent_path']) == (thread_id, parent_session, agent_path):
                return dict(old)
            self._assert_editable()
            duplicate = self.db.execute('SELECT session_id FROM bridge_bindings WHERE thread_id=? AND session_id<>?',
                                        (thread_id, caller)).fetchone() if thread_id else None
            if duplicate:
                raise CoordError('thread is already bound to another session; subagents must use their own thread or a parent route')
            self.db.execute('INSERT OR REPLACE INTO bridge_bindings VALUES(?,?,?,?,?)',
                            (caller, thread_id, parent_session, agent_path, time.time()))
            self.resolve(caller)  # Cycle detection rolls the transaction back.
        return dict(self.db.execute('SELECT * FROM bridge_bindings WHERE session_id=?', (caller,)).fetchone())

    def unbind(self):
        with self.coord.tx():
            self._assert_editable()
            self.db.execute('DELETE FROM bridge_bindings WHERE session_id=?', (self.coord.require_session(),))
        return {'unbound': True}

    def pending(self, recipient_session=None, before_seq=None):
        # Do not replay ACKed history from before the provenance migration boundary.
        if recipient_session is not None and (not isinstance(recipient_session, str) or not recipient_session):
            raise CoordError('invalid pending recipient filter')
        if before_seq is not None and (not isinstance(before_seq, int) or isinstance(before_seq, bool) or before_seq < 1):
            raise CoordError('invalid pending sequence filter')
        has_updates = bool(self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='manager_message_updates'").fetchone())
        marker = ('CASE WHEN u.message_id IS NULL THEN 0 ELSE 1 END AS manager_update_candidate'
                  if has_updates else '0 AS manager_update_candidate')
        update_join = 'LEFT JOIN manager_message_updates u ON u.message_id=m.id' if has_updates else ''
        # Bound the FIFO check in SQLite before resolving lifecycle state.
        scope = ''
        bindings = []
        if recipient_session is not None:
            scope += ' AND m.recipient_session=?'
            bindings.append(recipient_session)
        if before_seq is not None:
            scope += ' AND m.seq<?'
            bindings.append(before_seq)
        messages = [dict(r) for r in self.db.execute('''
            SELECT m.id,m.recipient_session,'''+marker+''' FROM messages m
            LEFT JOIN bridge_deliveries d ON d.message_id=m.id
            '''+update_join+'''
            WHERE m.sender_session<>m.recipient_session
              AND NOT EXISTS (SELECT 1 FROM message_reads r WHERE r.message_id=m.id
                  AND r.reader_session=m.recipient_session
                  AND r.provenance IN ('recipient_full_message','recipient_context_full')) AND NOT (m.acked_at IS NOT NULL AND m.acked_at <= COALESCE(CAST((SELECT value FROM meta WHERE key='message_delivery_archive_before') AS REAL),-1))
              AND d.message_id IS NULL
              AND NOT EXISTS (SELECT 1 FROM session_resets r WHERE r.target_session=m.recipient_session
                  AND r.status NOT IN ('completed','cancelled','failed','parked'))
              AND NOT EXISTS (SELECT 1 FROM agent_controls c WHERE c.target_session=m.recipient_session AND c.mode<>'running')
              AND (NOT EXISTS (SELECT 1 FROM message_batches b WHERE b.message_id=m.id)
                   OR EXISTS (SELECT 1 FROM message_attention a WHERE a.message_id=m.id))
            ''' + scope + ' ORDER BY m.seq', bindings)]
        from .root_orchestrator import projection
        from .root_lifecycle import stopped
        from .core import reset_locked
        from .root_restart import held
        from .project_activity import non_runnable_task_sources
        withheld = non_runnable_task_sources(self.db)
        result = [message for message in messages
                  if message['id'] not in withheld
                  and not administrative_attachment_fenced(self.db, session=message['recipient_session'])
                  and not held(self.db,message['recipient_session']) and not stopped(self.db,message['recipient_session'])
                  and not reset_locked(self.db,message['recipient_session'])
                  and not (projection(self.db, message['recipient_session']) or {}).get('paused')]
        for message in result:
            if message['manager_update_candidate']:
                message['manager_update_candidate'] = True
            else:
                del message['manager_update_candidate']
        return result

    def _manager_update_candidate_locked(self, thread_id, message_id):
        """Strictly prove an opt-in active-turn update without reading its body."""
        if not self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='manager_message_updates'").fetchone():
            return None
        row = self.db.execute('''SELECT m.id,m.sender_session,m.recipient_session,u.task_id,
                w.owner_session,w.assigned_by,w.source_request_id,w.state,
                d.job_id AS original_job_id,j.status AS original_job_status,b.parent_session,m.seq
            FROM manager_message_updates u JOIN messages m ON m.id=u.message_id
            JOIN project_work w ON w.id=u.task_id AND w.kind='task'
            JOIN bridge_bindings b ON b.session_id=m.recipient_session AND b.thread_id=?
            JOIN bridge_deliveries d ON d.message_id=w.source_request_id
            JOIN bridge_jobs j ON j.id=d.job_id AND j.thread_id=? AND j.status='dispatched'
            JOIN messages source ON source.id=w.source_request_id
            WHERE u.message_id=? AND m.recipient_session=w.owner_session
              AND source.sender_session=w.assigned_by AND source.recipient_session=w.owner_session
              AND NOT EXISTS (SELECT 1 FROM message_batches x WHERE x.message_id=source.id)
              AND (SELECT COUNT(*) FROM project_orchestrators)=1
              AND EXISTS (SELECT 1 FROM project_orchestrators p WHERE p.session_id=m.sender_session)
              AND NOT EXISTS (SELECT 1 FROM message_batches x WHERE x.message_id=m.id)
              AND NOT EXISTS (SELECT 1 FROM message_reads r WHERE r.message_id=m.id
                  AND r.reader_session=m.recipient_session
                  AND r.provenance IN ('recipient_full_message','recipient_context_full'))
              AND NOT (m.acked_at IS NOT NULL AND m.acked_at <= COALESCE(CAST((SELECT value FROM meta WHERE key='message_delivery_archive_before') AS REAL),-1))
              AND NOT EXISTS (SELECT 1 FROM bridge_deliveries x WHERE x.message_id=m.id)
              AND NOT EXISTS (SELECT 1 FROM manager_update_claims x WHERE x.message_id=m.id)
              AND NOT EXISTS (SELECT 1 FROM bridge_deliveries x WHERE x.job_id=d.job_id AND x.message_id<>source.id)''',
            (thread_id, thread_id, message_id)).fetchone()
        if not row or row['parent_session'] is not None or row['state'] != 'in_progress':
            return None
        from .management import manager
        if manager(self.db, row['recipient_session']) != row['sender_session'] or row['assigned_by'] != row['sender_session']:
            return None
        route = self.resolve(row['recipient_session'])
        if (not route or route['thread_id'] != thread_id or len(route['route']) != 1
                or route['route'][0]['session_id'] != row['recipient_session']):
            return None
        if cold_load_pending(self.db, thread_id) or administrative_attachment_fenced(self.db, session=row['recipient_session'], thread=thread_id):
            return None
        if self.db.execute("SELECT 1 FROM bridge_jobs WHERE thread_id=? AND status NOT IN ('dispatched','cancelled')", (thread_id,)).fetchone():
            return None
        if self.pending(recipient_session=row['recipient_session'], before_seq=row['seq']):
            return None
        from .root_lifecycle import stopped
        from .root_orchestrator import projection
        from .root_restart import held
        if stopped(self.db,row['recipient_session']) or held(self.db,row['recipient_session']) or (projection(self.db,row['recipient_session']) or {}).get('paused'):
            return None
        try:
            from .session_reset import assert_reset_unlocked
            assert_reset_unlocked(self.db,row['recipient_session'])
            assert_agent_running(self.db,row['recipient_session'])
        except CoordError:
            return None
        return {'task_id': row['task_id'], 'recipient_session': row['recipient_session'],
                'original_job_id': row['original_job_id']}

    def manager_update_candidate(self, thread_id, message_id):
        if not isinstance(thread_id,str) or not isinstance(message_id,str) or not thread_id or not message_id:
            raise CoordError('invalid manager update candidate')
        with self.coord.tx():
            return self._manager_update_candidate_locked(thread_id,message_id)

    def manager_update_claim(self, thread_id, message_id, task_id, original_job_id, turn_id, payload):
        if (not all(isinstance(v,str) and v for v in (thread_id,message_id,task_id,turn_id,payload))
                or original_job_id is not None and (not isinstance(original_job_id,str) or not original_job_id)
                or len(payload)>262144):
            raise CoordError('invalid manager update claim')
        with self.coord.tx():
            old = self.db.execute('SELECT * FROM manager_update_claims WHERE message_id=?',(message_id,)).fetchone()
            if old:
                job=self.db.execute('SELECT thread_id,payload FROM bridge_jobs WHERE id=?',(old['job_id'],)).fetchone()
                if (not job or tuple(old[k] for k in ('task_id','original_job_id','turn_id')) != (task_id,original_job_id,turn_id)
                        or job['thread_id'] != thread_id or job['payload'] != payload):
                    raise CoordError('manager update claim differs from original')
                return None
            candidate=self._manager_update_candidate_locked(thread_id,message_id)
            if (not candidate or candidate['task_id'] != task_id
                    or candidate['original_job_id'] != original_job_id):
                return None
            job_id='wake_'+uuid.uuid4().hex
            now=time.time()
            self.db.execute('INSERT INTO bridge_jobs VALUES(?,?,?,?,?,?,?,?)',(job_id,thread_id,payload,'prepared',None,None,now,now))
            self.db.execute('INSERT INTO bridge_job_origins VALUES(?,?)',(job_id,now))
            self.db.execute('INSERT INTO bridge_deliveries VALUES(?,?)',(message_id,job_id))
            self.db.execute('INSERT INTO manager_update_claims VALUES(?,?,?,?,?,?)',(message_id,task_id,original_job_id,turn_id,job_id,now))
            return self.job(job_id)

    def manager_update_prior(self, thread_id, message_id, task_id, original_job_id, turn_id):
        """Return only settled steers proven to share this still-live exact update tuple."""
        with self.coord.tx():
            candidate = self._manager_update_candidate_locked(thread_id, message_id)
            if (not candidate or candidate['task_id'] != task_id
                    or candidate['original_job_id'] != original_job_id):
                return []
            sender = self.db.execute('SELECT sender_session FROM messages WHERE id=?', (message_id,)).fetchone()['sender_session']
            return [row['job_id'] for row in self.db.execute('''SELECT c.job_id
                FROM manager_update_claims c JOIN bridge_jobs j ON j.id=c.job_id
                JOIN messages m ON m.id=c.message_id
                WHERE c.task_id=? AND c.original_job_id=? AND c.turn_id=?
                  AND m.recipient_session=? AND m.sender_session=? AND j.status='dispatched'
                ORDER BY c.claimed_at,c.job_id''',
                (task_id, original_job_id, turn_id, candidate['recipient_session'], sender))]

    def manager_update_epoch_check(self, thread_id, job_id, actor, method=None):
        """Recheck the exact claimed update immediately before turn/steer submission."""
        base = self.db.execute('SELECT 1 FROM manager_update_claims WHERE job_id=?', (job_id,)).fetchone()
        if not base:
            return
        claim = self.db.execute('''SELECT c.*,m.sender_session,m.recipient_session,w.owner_session,w.assigned_by,w.state
            FROM manager_update_claims c JOIN messages m ON m.id=c.message_id
            JOIN project_work w ON w.id=c.task_id WHERE c.job_id=?''',(job_id,)).fetchone()
        if not claim:
            raise CoordError('manager update claim lost its source association')
        if method != 'turn/steer':
            raise CoordError('claimed manager update requires turn/steer')
        if (actor != claim['recipient_session'] or actor != claim['owner_session']
                or claim['state'] != 'in_progress'):
            raise CoordError('manager update task changed before native epoch')
        route=self.resolve(actor)
        if not route or route['thread_id'] != thread_id or len(route['route']) != 1:
            raise CoordError('manager update route changed before native epoch')
        from .management import manager
        if manager(self.db,actor) != claim['sender_session'] or claim['assigned_by'] != claim['sender_session']:
            raise CoordError('manager update ownership changed before native epoch')
        original = self.db.execute('''SELECT 1 FROM project_work w
            JOIN bridge_deliveries d ON d.message_id=w.source_request_id
            JOIN bridge_jobs j ON j.id=d.job_id AND j.status='dispatched' AND j.thread_id=?
            JOIN messages m ON m.id=w.source_request_id
            WHERE w.id=? AND d.job_id=? AND m.sender_session=? AND m.recipient_session=?''',
            (thread_id,claim['task_id'],claim['original_job_id'],claim['sender_session'],actor)).fetchone()
        if not original:
            raise CoordError('manager update original assignment changed before native epoch')
        if (self.db.execute('SELECT 1 FROM message_batches WHERE message_id=?', (claim['message_id'],)).fetchone()
                or self.db.execute('SELECT 1 FROM bridge_deliveries WHERE job_id=? AND message_id<>?',
                                   (claim['original_job_id'], self.db.execute(
                                       'SELECT source_request_id FROM project_work WHERE id=?',
                                       (claim['task_id'],)).fetchone()['source_request_id'])).fetchone()):
            raise CoordError('manager update original delivery is no longer sole direct source')
        po = self.db.execute('SELECT session_id FROM project_orchestrators').fetchall()
        if len(po) != 1 or po[0]['session_id'] != claim['sender_session']:
            raise CoordError('manager update PO changed before native epoch')
        if self.db.execute('''SELECT 1 FROM message_reads WHERE message_id=? AND reader_session=?
            AND provenance IN ('recipient_full_message','recipient_context_full')''',(claim['message_id'],actor)).fetchone():
            raise CoordError('manager update was consumed before native epoch')
        if self.db.execute('SELECT 1 FROM message_batches WHERE message_id=?',(claim['message_id'],)).fetchone():
            raise CoordError('manager update no longer direct')
        if self.db.execute("SELECT 1 FROM bridge_jobs WHERE thread_id=? AND id<>? AND status NOT IN ('dispatched','cancelled')",(thread_id,job_id)).fetchone():
            raise CoordError('competing wake appeared before manager update epoch')
        if administrative_attachment_fenced(self.db, session=actor, thread=thread_id):
            raise CoordError('administrative attachment fences manager update epoch')
        from .root_lifecycle import stopped
        if stopped(self.db,actor) or cold_load_pending(self.db,thread_id):
            raise CoordError('manager update admission changed before native epoch')
        from .session_reset import assert_reset_unlocked
        assert_reset_unlocked(self.db,actor)
        assert_agent_running(self.db,actor)

    def investigation_tasks(self, host_id=None):
        """Return only exact, unread, single-message task admissions."""
        from .root_lifecycle import task_execution_admission
        if not self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_team_stop_task_admissions'").fetchone():
            return []
        rows = self.db.execute('''SELECT message_id FROM root_team_stop_task_admissions
            ORDER BY created_at,message_id''').fetchall()
        result = []
        for row in rows:
            admission = task_execution_admission(self.db, row['message_id'])
            if not admission:
                continue
            if host_id is not None and admission['host_id'] != host_id:
                continue
            delivery = self.db.execute('SELECT job_id FROM bridge_deliveries WHERE message_id=?',
                                       (row['message_id'],)).fetchone()
            if delivery:
                continue
            message = self.db.execute('SELECT id,recipient_session FROM messages WHERE id=?',
                                      (row['message_id'],)).fetchone()
            if message:
                result.append({'admission': admission, 'message': dict(message)})
        return result

    def still_unread(self, job_id):
        if self.is_root_batch(job_id):
            return self.check_root_batch(job_id)
        return bool(self.db.execute('''SELECT 1 FROM bridge_deliveries d
            JOIN messages m ON m.id=d.message_id
            WHERE d.job_id=? AND NOT EXISTS (SELECT 1 FROM message_reads r WHERE r.message_id=m.id
                AND r.reader_session=m.recipient_session
                AND r.provenance IN ('recipient_full_message','recipient_context_full')) AND NOT (m.acked_at IS NOT NULL AND m.acked_at <= COALESCE(CAST((SELECT value FROM meta WHERE key='message_delivery_archive_before') AS REAL),-1))
              AND NOT EXISTS (SELECT 1 FROM agent_controls c WHERE c.target_session=m.recipient_session AND c.mode<>'running')
              AND (NOT EXISTS (SELECT 1 FROM message_batches b WHERE b.message_id=m.id)
                   OR EXISTS (SELECT 1 FROM message_attention a WHERE a.message_id=m.id)) LIMIT 1''', (job_id,)).fetchone())

    def check_root_batch(self, job_id, admit=False):
        if not self.root_restart:
            raise CoordError('root wake batch requires its authenticated root registry')
        return self.root_restart.check_batch(job_id,self.root_project,self.root_host,admit=admit,local_state=self)

    def is_root_batch(self,job_id):
        return bool(self.db.execute('SELECT 1 FROM bridge_root_wake_batches WHERE job_id=?',(job_id,)).fetchone()
            or (self.root_restart and self.root_restart.is_batch(job_id)))

    def pristine(self, job_id):
        return bool(self.db.execute('''SELECT 1 FROM bridge_jobs j JOIN bridge_job_origins o ON o.job_id=j.id
            WHERE j.id=? AND j.status='prepared' AND j.queue_id IS NULL AND j.error IS NULL
              AND NOT EXISTS (SELECT 1 FROM bridge_job_native_attempts a WHERE a.job_id=j.id)
              AND NOT EXISTS (SELECT 1 FROM agent_control_requests r WHERE r.native_input_job_id=j.id)
              AND NOT EXISTS (SELECT 1 FROM bridge_root_wake_batches b WHERE b.job_id=j.id)''',(job_id,)).fetchone())

    def wake_messages(self, thread_id, message_ids):
        """Bound direct-recipient content without reading or acknowledging inboxes.

        Child messages belong to the child, even when a parent dispatches its wake.
        Large bodies/metadata stay retrievable through the recipient's message API.
        """
        result = []
        for message_id in message_ids:
            row = self.db.execute('''SELECT * FROM messages WHERE id=?
                AND NOT EXISTS (SELECT 1 FROM message_reads r WHERE r.message_id=messages.id
                    AND r.reader_session=messages.recipient_session
                    AND r.provenance IN ('recipient_full_message','recipient_context_full')) AND NOT (messages.acked_at IS NOT NULL AND messages.acked_at <= COALESCE(CAST((SELECT value FROM meta WHERE key='message_delivery_archive_before') AS REAL),-1))
                AND (NOT EXISTS (SELECT 1 FROM message_batches b WHERE b.message_id=messages.id)
                     OR EXISTS (SELECT 1 FROM message_attention a WHERE a.message_id=messages.id))''',
                                  (message_id,)).fetchone()
            route = self.resolve(row['recipient_session']) if row else None
            if not route or route['thread_id'] != thread_id or len(route['route']) != 1:
                continue
            item = {'id': message_id, 'complete': False}
            if len(row['body']) <= 4096:
                full = self.coord._message_dicts(self.db, [row])[0]
                candidate = dict(full, complete=True)
                if len(json.dumps(candidate, ensure_ascii=False).encode('utf-8')) <= 4096:
                    item = candidate
            result.append(item)
        return result

    def connection_metadata(self):
        return {'database': str(self.coord.path.resolve())}

    def policies(self):
        if self.policy_store is None:
            from .policy_store import PolicyStore
            self.policy_store = PolicyStore(self.coord.path)
        return self.policy_store

    def policy_consumed(self, thread_id):
        return self.policies().consumed(self.policy_host,thread_id,project=self.policy_project)

    def policy_offer(self, kind, client_id, thread_id, payload):
        return self.policies().offer(self.policy_host,self.policy_project,kind,client_id,thread_id,payload)

    def policy_pending(self):
        bound={row['thread_id'] for row in self.db.execute('SELECT thread_id FROM bridge_bindings WHERE parent_session IS NULL')}
        return [offer for offer in self.policies().pending(self.policy_host,project=self.policy_project) if offer['thread'] in bound]

    def policy_receipt(self, offer, turn_id):
        return self.policies().receipt(self.policy_host,offer['thread'],offer['client_id'],offer['version'],turn_id,project=self.policy_project)

    def prepare(self, thread_id, messages, payload, *, investigation_request_id=None,
                eligibility_observation=None, eligibility_lease=None):
        if eligibility_observation is not None:
            o = eligibility_observation
            keys = {'thread_id', 'connection_instance', 'connection_epoch', 'start_monotonic_ns', 'end_monotonic_ns'}
            if (investigation_request_id is not None or not isinstance(o, dict) or set(o) != keys
                    or o['thread_id'] != thread_id or not isinstance(o['connection_instance'], str)
                    or not 1 <= len(o['connection_instance']) <= 256
                    or any(type(o[k]) is not int or not 0 < o[k] < 2**63 for k in ('connection_epoch','start_monotonic_ns','end_monotonic_ns'))
                    or o['start_monotonic_ns'] > o['end_monotonic_ns']):
                raise CoordError('invalid eligibility observation')
            if eligibility_lease is None:
                # Local dispatch lacks authenticated host/lease provenance.
                eligibility_observation = None
        if investigation_request_id is not None:
            if len(messages) != 1:
                raise CoordError('investigation admission permits exactly one original message per wake')
            from .root_lifecycle import task_execution_admission
            admission = task_execution_admission(self.db, messages[0]['id'])
            if (not admission or admission['request_id'] != investigation_request_id
                    or admission['thread_id'] != thread_id
                    or admission['target_session'] != messages[0]['recipient_session']):
                raise CoordError('investigation task scope changed before wake preparation')
        job_id = 'wake_' + uuid.uuid4().hex
        with self.coord.tx():
            if cold_load_pending(self.db, thread_id):
                return None
            if administrative_attachment_fenced(self.db, thread=thread_id) or any(
                    administrative_attachment_fenced(self.db, session=message['recipient_session']) for message in messages):
                return None
            from .project_activity import non_runnable_task_sources
            withheld = non_runnable_task_sources(self.db)
            if any(message['id'] in withheld for message in messages):
                # The payload already contains every selected assignment. Do
                # not retain blocked text by merely dropping its delivery ID.
                # A later tick selects runnable work and rebuilds the payload.
                return None
            live = []
            for message in messages:
                row = self.db.execute('''SELECT recipient_session,sender_session FROM messages WHERE id=?
                    AND NOT EXISTS (SELECT 1 FROM message_reads r WHERE r.message_id=messages.id
                        AND r.reader_session=messages.recipient_session
                        AND r.provenance IN ('recipient_full_message','recipient_context_full')) AND NOT (messages.acked_at IS NOT NULL AND messages.acked_at <= COALESCE(CAST((SELECT value FROM meta WHERE key='message_delivery_archive_before') AS REAL),-1))''', (message['id'],)).fetchone()
                from .session_reset import assert_reset_unlocked
                assert_reset_unlocked(self.db, message['recipient_session'])
                assert_agent_running(self.db, message['recipient_session'],
                    admitted_task_message_id=(message['id'] if investigation_request_id is not None else None))
                route = self.resolve(message['recipient_session'])
                if (row and row['recipient_session'] == message['recipient_session']
                        and row['sender_session'] != row['recipient_session'] and route and route['thread_id'] == thread_id
                        and not self.db.execute('SELECT 1 FROM bridge_deliveries WHERE message_id=?', (message['id'],)).fetchone()
                        and (not self.db.execute('SELECT 1 FROM message_batches WHERE message_id=?', (message['id'],)).fetchone()
                             or self.db.execute('SELECT 1 FROM message_attention WHERE message_id=?', (message['id'],)).fetchone())
                        and message not in live):
                    live.append(message)
            if not live:
                return None
            self.db.execute('INSERT INTO bridge_jobs VALUES(?,?,?,?,?,?,?,?)',
                            (job_id, thread_id, payload, 'prepared', None, None, time.time(), time.time()))
            self.db.execute('INSERT INTO bridge_job_origins VALUES(?,?)',(job_id,time.time()))
            self.db.executemany('INSERT INTO bridge_deliveries VALUES(?,?)', [(m['id'], job_id) for m in live])
            if eligibility_observation is not None:
                o, lease = eligibility_observation, eligibility_lease
                current = self.db.execute('SELECT owner,secret_hash,acquired_at FROM bridge_dispatcher WHERE host_id=?',
                                          (lease['host_id'],)).fetchone()
                if not current or tuple(current) != (lease['owner'], lease['secret_hash'], lease['acquired_at']):
                    raise CoordError('dispatcher ownership changed before eligibility association')
                self.db.execute('INSERT INTO bridge_eligibility_observations VALUES(?,?,?,?,?,?,?,?,?,?)',
                    (job_id, thread_id, lease['host_id'], o['connection_instance'], o['connection_epoch'],
                     o['start_monotonic_ns'], o['end_monotonic_ns'], lease['owner'], lease['secret_hash'], lease['acquired_at']))
            # Claim selection only with genuine managed input; never manufacture
            # a turn to change defaults. One request remains tied to one wake UUID.
            selection = self.db.execute('''SELECT r.id FROM agent_control_requests r
                JOIN bridge_bindings b ON b.session_id=r.target_session
                WHERE r.model_route='ordinary_input' AND r.status IN ('pending','waiting')
                  AND r.native_input_job_id IS NULL AND r.thread_id=? AND b.thread_id=r.thread_id
                ORDER BY r.created_at LIMIT 1''', (thread_id,)).fetchone()
            if selection:
                self.db.execute('UPDATE agent_control_requests SET native_input_job_id=? WHERE id=?',
                                (job_id, selection['id']))
        return self.job(job_id)

    def job(self, job_id):
        row = self.db.execute('SELECT * FROM bridge_jobs WHERE id=?', (job_id,)).fetchone()
        if not row:
            raise CoordError('unknown bridge job')
        result = dict(row)
        result['pristine'] = self.pristine(job_id)
        from .root_lifecycle import task_admission_for_job
        task_admission = task_admission_for_job(self.db, job_id)
        if task_admission:
            result['investigation_admission_request_id'] = task_admission['request_id']
        update = self.db.execute('SELECT message_id,task_id,original_job_id,turn_id FROM manager_update_claims WHERE job_id=?',
                                 (job_id,)).fetchone()
        if update:
            result['target_session'] = self.db.execute('SELECT recipient_session FROM messages WHERE id=?',
                                                       (update['message_id'],)).fetchone()['recipient_session']
            result['manager_update'] = {key: update[key] for key in ('task_id','original_job_id','turn_id')}
            result['manager_update']['recipient_session'] = result['target_session']
        selection = self.db.execute('SELECT id FROM agent_control_requests WHERE native_input_job_id=?', (job_id,)).fetchone()
        if selection:
            control = self.controls.job(selection['id'])
            result['model_request_id'] = control['id']
            if control['status'] != 'failed' or result['status'] == 'failed':
                result['model_control'] = control
        return result

    def record_native_stage(self, job, host, observation, lease):
        """Record one host-reported receipt; no source or coverage inference."""
        import math
        fields = {'status','attribution','coverage','job_id','thread_id','turn_id','item_id',
                  'observed_at','observed_monotonic_ns','connection_instance','connection_epoch','sequence'}
        if not isinstance(observation, dict) or set(observation) != fields:
            raise CoordError('invalid native stage fields')
        if (observation['status'] != 'observed' or observation['attribution'] != 'job_turn_only'
                or observation['coverage'] != 'unverified' or observation['job_id'] != job['id']
                or observation['thread_id'] != job['thread_id']
                or job['status'] not in ('starting','uncertain','dispatched')):
            raise CoordError('invalid native stage scope')
        for key in ('turn_id','item_id','connection_instance'):
            if not isinstance(observation[key], str) or not 1 <= len(observation[key]) <= 256:
                raise CoordError('invalid native stage identity')
        for key in ('observed_monotonic_ns','connection_epoch','sequence'):
            if type(observation[key]) is not int or not 0 < observation[key] < 2**63:
                raise CoordError('invalid native stage counter')
        stamp = observation['observed_at']
        if type(stamp) not in (int,float) or not math.isfinite(stamp) or stamp <= 0:
            raise CoordError('invalid native stage clock')
        values = (job['id'],job['thread_id'],host,observation['turn_id'],observation['item_id'],stamp,
                  observation['observed_monotonic_ns'],observation['connection_instance'],
                  observation['connection_epoch'],observation['sequence'])
        old = self.db.execute('SELECT job_id,thread_id,host_id,turn_id,item_id,observed_at,monotonic_ns,connection_instance,connection_epoch,sequence FROM bridge_native_stage_observations WHERE job_id=?',(job['id'],)).fetchone()
        if old:
            if tuple(old) != values:
                raise CoordError('conflicting native stage observation')
            saved = self.db.execute('SELECT censored,lease_owner,lease_hash,lease_acquired FROM bridge_native_stage_observations WHERE job_id=?',(job['id'],)).fetchone()
            if saved['censored'] or (saved['lease_owner'],saved['lease_hash'],saved['lease_acquired']) != (lease['owner'],lease['secret_hash'],lease['acquired_at']):
                raise CoordError('native stage observation is censored')
            return {'recorded':True, 'replayed':True}
        self.db.execute('INSERT INTO bridge_native_stage_observations (job_id,thread_id,host_id,turn_id,item_id,observed_at,monotonic_ns,connection_instance,connection_epoch,sequence,recorded_at,lease_owner,lease_hash,lease_acquired) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)', values + (time.time(),lease['owner'],lease['secret_hash'],lease['acquired_at']))
        return {'recorded':True, 'replayed':False}

    def censor_native_stage(self, job, host, reason, lease):
        allowed = {'window_unavailable','ambiguous_input','turn_mismatch','observed_lifecycle_gap','invalid_record','invalid_reader_clock','invalid_start_response'}
        if reason not in allowed:
            raise CoordError('invalid native stage gap')
        row = self.db.execute('SELECT host_id,censored,censor_reason,lease_owner,lease_hash,lease_acquired FROM bridge_native_stage_observations WHERE job_id=?',(job['id'],)).fetchone()
        if not row:
            return {'censored':False, 'reason':'no_record'}
        if (row['host_id'] != host or (row['lease_owner'],row['lease_hash'],row['lease_acquired']) !=
                (lease['owner'],lease['secret_hash'],lease['acquired_at'])):
            raise CoordError('native stage belongs to another host')
        if row['censored']:
            if row['censor_reason'] != reason:
                raise CoordError('conflicting native stage gap')
            return {'censored':True, 'replayed':True}
        self.db.execute('UPDATE bridge_native_stage_observations SET censored=1,censor_reason=? WHERE job_id=?',(reason,job['id']))
        return {'censored':True, 'replayed':False}

    def jobs(self):
        jobs = [dict(r) for r in self.db.execute("""SELECT j.* FROM bridge_jobs j WHERE j.status NOT IN ('dispatched','cancelled')
            AND NOT (j.status='prepared' AND EXISTS (SELECT 1 FROM bridge_bindings b JOIN agent_controls c ON c.target_session=b.session_id WHERE b.thread_id=j.thread_id AND c.mode<>'running')) ORDER BY j.created_at""")]
        from .root_orchestrator import projection
        from .root_restart import held
        def paused(job):
            row = self.db.execute('SELECT session_id FROM bridge_bindings WHERE thread_id=?', (job['thread_id'],)).fetchone()
            return row and (held(self.db,row['session_id']) or (projection(self.db, row['session_id']) or {}).get('paused'))
        from .root_lifecycle import stopped, task_admission_for_job
        result = []
        for job in jobs:
            if cold_load_pending(self.db, job['thread_id']):
                continue
            if administrative_attachment_fenced(self.db, thread=job['thread_id']):
                continue
            if paused(job):
                continue
            recipients = [row['recipient_session'] for row in self.db.execute(
                'SELECT m.recipient_session FROM bridge_deliveries d JOIN messages m ON m.id=d.message_id WHERE d.job_id=?',
                (job['id'],))]
            if any(administrative_attachment_fenced(self.db, session=recipient) for recipient in recipients):
                continue
            if any(stopped(self.db, recipient) for recipient in recipients):
                if not task_admission_for_job(self.db, job['id']):
                    continue
            result.append(self.job(job['id']))
        return result

    def update(self, job_id, status, queue_id=None, error=None):
        def write():
            if status in ('adding','starting'):
                existing = self.job(job_id)
                if cold_load_pending(self.db, existing['thread_id']):
                    raise CoordError('uncertain cold message load fences wake submission')
                if administrative_attachment_job_fenced(self.db, job_id, existing['thread_id']):
                    raise CoordError('administrative attachment fences native wake submission')
                if self.is_root_batch(job_id):
                    self.check_root_batch(job_id,admit=status)
                from .root_lifecycle import stopped, task_admission_for_job
                stopped_recipients = [row['recipient_session'] for row in self.db.execute(
                    'SELECT m.recipient_session FROM bridge_deliveries d JOIN messages m ON m.id=d.message_id WHERE d.job_id=?', (job_id,))]
                task_admission = task_admission_for_job(self.db, job_id)
                if any(stopped(self.db, recipient) for recipient in stopped_recipients) and not task_admission:
                    raise CoordError('root stop-all barrier withheld native wake admission')
                row = self.db.execute('SELECT b.session_id FROM bridge_jobs j JOIN bridge_bindings b ON b.thread_id=j.thread_id WHERE j.id=?', (job_id,)).fetchone()
                if row: assert_agent_running(self.db, row['session_id'],
                    admitted_task_message_id=(task_admission['message_id'] if task_admission else None))
            self.db.execute('UPDATE bridge_jobs SET status=?,queue_id=COALESCE(?,queue_id),error=?,updated_at=? WHERE id=?',
                            (status, queue_id, error, time.time(), job_id))
            job = self.job(job_id)
            selection = job.get('model_control')
            if selection and status == 'starting':
                if not self.still_unread(job_id):
                    raise CoordError('ordinary model input was consumed before submission')
                self.controls.update(selection['id'], selection['status'], 'submitting',
                    input_job_id=job_id, direct_input=True)
            elif selection and status in ('uncertain','failed') and selection['status'] == 'submitting':
                self.controls.update(selection['id'], selection['status'], status,
                    input_job_id=job_id, error=error)
            elif selection and status == 'cancelled' and selection['status'] in ('pending','waiting'):
                self.controls.update(selection['id'], selection['status'], 'failed', input_job_id=job_id,
                    error='Original associated input consumed before native submission; no replacement input or model replay.')
        if self.db.in_transaction: write()
        else:
            with self.coord.tx(): write()

    def retry(self, job_id, confirmed=False):
        if not confirmed:
            raise CoordError('verify no wake started and remove any queued copy, then pass --confirm-not-started')
        with self.coord.tx():
            job = self.job(job_id)
            if administrative_attachment_fenced(self.db, thread=job['thread_id']):
                raise CoordError('administrative attachment fences wake retry')
            route = self.resolve(self.coord.require_session())
            if not route or route['thread_id'] != job['thread_id']:
                raise CoordError('only a session bound to this route can retry its job')
            if job['status'] not in ('uncertain', 'failed'):
                raise CoordError('only uncertain or failed jobs can be explicitly retried')
            selection = job.get('model_control')
            if selection and not (job['status'] == 'failed' and selection['status'] == 'failed'):
                raise CoordError('ordinary model input cannot be replayed; inspect original request and native input evidence')
            self.db.execute("UPDATE bridge_jobs SET status='prepared',queue_id=NULL,error=NULL,updated_at=? WHERE id=?", (time.time(), job_id))
        return self.job(job_id)

    def heartbeat(self, server, pid, error=None, *, host_id=None, project_scope_worker=False, project_workspaces_worker=False, versions=None):
        if host_id is None:
            self.db.execute('INSERT OR REPLACE INTO bridge_runtime VALUES(1,?,?,?,?)', (server, pid, time.time(), error))
        else:
            self.db.execute('INSERT INTO bridge_client_runtime(host_id,server,pid,heartbeat,error,project_scope_worker,project_workspaces_worker,running_version,installed_version) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(host_id) DO UPDATE SET server=excluded.server,pid=excluded.pid,heartbeat=excluded.heartbeat,error=excluded.error,project_scope_worker=excluded.project_scope_worker,project_workspaces_worker=excluded.project_workspaces_worker,running_version=excluded.running_version,installed_version=excluded.installed_version',
                            (host_id, server, pid, time.time(), error, int(project_scope_worker), int(project_workspaces_worker), *(versions or (None, None))))

    def observe(self, thread_id, state, error=None):
        self.db.execute('INSERT OR REPLACE INTO bridge_observations VALUES(?,?,?,?)',
                        (thread_id, state, error, time.time()))

    def supervisor_jobs(self):
        from .supervisor import SupervisorState
        if not self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='remote_session_hosts'").fetchone():
            return []  # Legacy local dispatch has no authenticated host supervisor.
        host = self.db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (self.coord.require_session(),)).fetchone()
        return SupervisorState(self.coord).jobs(host['host_id']) if host else []

    def supervisor_observe(self, job, report):
        from .supervisor import SupervisorState
        if not self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='remote_session_hosts'").fetchone():
            raise CoordError('supervisor observation requires an authenticated host')
        host = self.db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (self.coord.require_session(),)).fetchone()
        if not host: raise CoordError('supervisor target has no authenticated host')
        return SupervisorState(self.coord).observe(job, report, host['host_id'])

    def resolve_delivered(self, job_id, confirmed=False):
        if not confirmed:
            raise CoordError('inspect the existing Codex turn first, then pass --confirm-delivered')
        with self.coord.tx():
            job = self.job(job_id)
            route = self.resolve(self.coord.require_session())
            if not route or route['thread_id'] != job['thread_id']:
                raise CoordError('only a session bound to this route can resolve its job')
            if job['status'] != 'uncertain':
                raise CoordError('only uncertain jobs need delivered confirmation')
            self.update(job_id, 'dispatched', error='Delivery explicitly confirmed by ' + self.coord.require_session())
        return self.job(job_id)

    def status(self, mine=False):
        """Return either the legacy diagnostic or the caller's bridge route."""
        if not mine:
            return {'runtime': runtime_snapshot(self.db),
                    'bindings': [dict(r) for r in self.db.execute('SELECT * FROM bridge_bindings ORDER BY updated_at')],
                    'threads': [dict(r) for r in self.db.execute('SELECT * FROM bridge_observations ORDER BY checked_at DESC')],
                    'jobs': [dict(r) for r in self.db.execute('SELECT id,thread_id,status,queue_id,error,created_at,updated_at FROM bridge_jobs ORDER BY created_at DESC LIMIT 100')],
                    'capacity_recoveries': [dict(r) for r in self.db.execute('SELECT * FROM bridge_capacity_recoveries ORDER BY updated_at DESC LIMIT 100')]}

        caller = self.coord.require_session()
        binding = self.db.execute('SELECT * FROM bridge_bindings WHERE session_id=?', (caller,)).fetchone()
        if not binding:
            return {'runtime': runtime_snapshot(self.db), 'bindings': [], 'threads': [],
                    'jobs': [], 'jobs_has_more': False, 'capacity_recoveries': []}
        route = self.resolve(caller)
        if not route:
            return {'runtime': runtime_snapshot(self.db), 'bindings': [dict(binding)], 'threads': [],
                    'jobs': [], 'jobs_has_more': False, 'capacity_recoveries': []}
        thread_id = route['thread_id']
        jobs = [dict(r) for r in self.db.execute('''
            SELECT id,thread_id,status,queue_id,error,created_at,updated_at
            FROM bridge_jobs WHERE thread_id=? AND status NOT IN ('dispatched','cancelled')
            ORDER BY created_at DESC LIMIT 11''', (thread_id,))]
        return {'runtime': runtime_snapshot(self.db), 'bindings': [dict(binding)],
                'threads': [dict(r) for r in self.db.execute(
                    'SELECT * FROM bridge_observations WHERE thread_id=?', (thread_id,))],
                'jobs': jobs[:10], 'jobs_has_more': len(jobs) > 10,
                'capacity_recoveries': [dict(r) for r in self.db.execute('SELECT * FROM bridge_capacity_recoveries WHERE thread_id=? ORDER BY updated_at DESC LIMIT 10', (thread_id,))]}
