"""Durable, opt-in wake routes. Inbox and reservation state remain untouched."""
from __future__ import annotations

import time
import uuid
import json

from .core import CoordError, Coordinator, assert_agent_running

TERMINAL = ('dispatched', 'cancelled')


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


class BridgeState:
    def __init__(self, coord: Coordinator):
        self.coord = coord
        self.db = coord.db
        self.policy_store = None
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
        ''')
        if 'project_scope_worker' not in {row['name'] for row in self.db.execute('PRAGMA table_info(bridge_client_runtime)')}:
            self.db.execute('ALTER TABLE bridge_client_runtime ADD COLUMN project_scope_worker INTEGER NOT NULL DEFAULT 0')
        if 'project_workspaces_worker' not in {row['name'] for row in self.db.execute('PRAGMA table_info(bridge_client_runtime)')}:
            self.db.execute('ALTER TABLE bridge_client_runtime ADD COLUMN project_workspaces_worker INTEGER NOT NULL DEFAULT 0')
        from .session_reset import SessionResetState
        SessionResetState(coord)
        from .agent_control import AgentControlState
        self.controls = AgentControlState(coord)
        from .capacity_recovery import CapacityState
        self.capacity = CapacityState(self)

    def capacity_targets(self): return self.capacity.targets()
    def capacity_jobs(self): return self.capacity.rows()
    def capacity_gate(self, session_id, thread_id): return self.capacity.gate(session_id, thread_id)
    def capacity_schedule(self, session_id, thread_id, turn_id, now, settings_digest, model, reasoning_effort, owner):
        return self.capacity.schedule(session_id, thread_id, turn_id, now, settings_digest, model, reasoning_effort, owner)
    def capacity_update(self, job_id, expected, status, now, **values):
        return self.capacity.update(job_id, expected, status, now, **values)

    def resolve(self, session_id):
        """Return the root thread and explicit descendant route, or no binding."""
        route, seen = [], set()
        while session_id:
            if session_id in seen:
                raise CoordError('cyclic bridge parent route')
            seen.add(session_id)
            row = self.db.execute('SELECT * FROM bridge_bindings WHERE session_id=?',
                                  (session_id,)).fetchone()
            if not row:
                return None
            route.append(dict(row))
            if row['thread_id']:
                return {'thread_id': row['thread_id'], 'route': list(reversed(route))}
            session_id = row['parent_session']
        return None

    def _assert_editable(self):
        # Parent rebinding could redirect descendants with queued input. Freeze
        # routes while a job needs reconciliation instead of silently rerouting it.
        caller = self.coord.require_session()
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

    def pending(self):
        # Do not replay ACKed history from before the provenance migration boundary.
        messages = [dict(r) for r in self.db.execute('''
            SELECT m.id,m.recipient_session FROM messages m
            LEFT JOIN bridge_deliveries d ON d.message_id=m.id
            WHERE m.sender_session<>m.recipient_session
              AND NOT EXISTS (SELECT 1 FROM message_reads r WHERE r.message_id=m.id
                  AND r.reader_session=m.recipient_session
                  AND r.provenance IN ('recipient_full_message','recipient_context_full')) AND NOT (m.acked_at IS NOT NULL AND m.acked_at <= COALESCE(CAST((SELECT value FROM meta WHERE key='message_delivery_archive_before') AS REAL),-1))
              AND d.message_id IS NULL
              AND NOT EXISTS (SELECT 1 FROM session_resets r WHERE r.target_session=m.recipient_session
                  AND r.status NOT IN ('completed','cancelled','failed'))
              AND NOT EXISTS (SELECT 1 FROM agent_controls c WHERE c.target_session=m.recipient_session AND c.mode<>'running')
              AND (NOT EXISTS (SELECT 1 FROM message_batches b WHERE b.message_id=m.id)
                   OR EXISTS (SELECT 1 FROM message_attention a WHERE a.message_id=m.id))
            ORDER BY m.seq''')]
        from .root_orchestrator import projection
        from .root_lifecycle import stopped
        from .root_restart import held
        return [message for message in messages if not held(self.db,message['recipient_session']) and not stopped(self.db,message['recipient_session']) and not (projection(self.db, message['recipient_session']) or {}).get('paused')]

    def still_unread(self, job_id):
        return bool(self.db.execute('''SELECT 1 FROM bridge_deliveries d
            JOIN messages m ON m.id=d.message_id
            WHERE d.job_id=? AND NOT EXISTS (SELECT 1 FROM message_reads r WHERE r.message_id=m.id
                AND r.reader_session=m.recipient_session
                AND r.provenance IN ('recipient_full_message','recipient_context_full')) AND NOT (m.acked_at IS NOT NULL AND m.acked_at <= COALESCE(CAST((SELECT value FROM meta WHERE key='message_delivery_archive_before') AS REAL),-1))
              AND NOT EXISTS (SELECT 1 FROM agent_controls c WHERE c.target_session=m.recipient_session AND c.mode<>'running')
              AND (NOT EXISTS (SELECT 1 FROM message_batches b WHERE b.message_id=m.id)
                   OR EXISTS (SELECT 1 FROM message_attention a WHERE a.message_id=m.id)) LIMIT 1''', (job_id,)).fetchone())

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
        return self.policies().consumed(self.policy_host,thread_id)

    def policy_offer(self, kind, client_id, thread_id, payload):
        return self.policies().offer(self.policy_host,self.policy_project,kind,client_id,thread_id,payload)

    def policy_pending(self):
        bound={row['thread_id'] for row in self.db.execute('SELECT thread_id FROM bridge_bindings WHERE parent_session IS NULL')}
        return [offer for offer in self.policies().pending(self.policy_host) if offer['thread'] in bound]

    def policy_receipt(self, offer, turn_id):
        return self.policies().receipt(self.policy_host,offer['thread'],offer['client_id'],offer['version'],turn_id)

    def prepare(self, thread_id, messages, payload):
        job_id = 'wake_' + uuid.uuid4().hex
        with self.coord.tx():
            live = []
            for message in messages:
                row = self.db.execute('''SELECT recipient_session,sender_session FROM messages WHERE id=?
                    AND NOT EXISTS (SELECT 1 FROM message_reads r WHERE r.message_id=messages.id
                        AND r.reader_session=messages.recipient_session
                        AND r.provenance IN ('recipient_full_message','recipient_context_full')) AND NOT (messages.acked_at IS NOT NULL AND messages.acked_at <= COALESCE(CAST((SELECT value FROM meta WHERE key='message_delivery_archive_before') AS REAL),-1))''', (message['id'],)).fetchone()
                from .session_reset import assert_reset_unlocked
                assert_reset_unlocked(self.db, message['recipient_session'])
                assert_agent_running(self.db, message['recipient_session'])
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
            self.db.executemany('INSERT INTO bridge_deliveries VALUES(?,?)', [(m['id'], job_id) for m in live])
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
        selection = self.db.execute('SELECT id FROM agent_control_requests WHERE native_input_job_id=?', (job_id,)).fetchone()
        if selection:
            control = self.controls.job(selection['id'])
            result['model_request_id'] = control['id']
            if control['status'] != 'failed' or result['status'] == 'failed':
                result['model_control'] = control
        return result

    def jobs(self):
        jobs = [dict(r) for r in self.db.execute("""SELECT j.* FROM bridge_jobs j WHERE j.status NOT IN ('dispatched','cancelled')
            AND NOT (j.status='prepared' AND EXISTS (SELECT 1 FROM bridge_bindings b JOIN agent_controls c ON c.target_session=b.session_id WHERE b.thread_id=j.thread_id AND c.mode<>'running')) ORDER BY j.created_at""")]
        from .root_orchestrator import projection
        from .root_restart import held
        def paused(job):
            row = self.db.execute('SELECT session_id FROM bridge_bindings WHERE thread_id=?', (job['thread_id'],)).fetchone()
            return row and (held(self.db,row['session_id']) or (projection(self.db, row['session_id']) or {}).get('paused'))
        from .root_lifecycle import stopped
        return [self.job(job['id']) for job in jobs if not paused(job)
                and not any(stopped(self.db,row['recipient_session']) for row in self.db.execute(
                    'SELECT m.recipient_session FROM bridge_deliveries d JOIN messages m ON m.id=d.message_id WHERE d.job_id=?', (job['id'],)))]

    def update(self, job_id, status, queue_id=None, error=None):
        def write():
            if status in ('adding','starting'):
                from .root_lifecycle import stopped
                if any(stopped(self.db,row['recipient_session']) for row in self.db.execute(
                    'SELECT m.recipient_session FROM bridge_deliveries d JOIN messages m ON m.id=d.message_id WHERE d.job_id=?', (job_id,))):
                    raise CoordError('root stop-all barrier withheld native wake admission')
                row = self.db.execute('SELECT b.session_id FROM bridge_jobs j JOIN bridge_bindings b ON b.thread_id=j.thread_id WHERE j.id=?', (job_id,)).fetchone()
                if row: assert_agent_running(self.db, row['session_id'])
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

    def heartbeat(self, server, pid, error=None, *, host_id=None, project_scope_worker=False, project_workspaces_worker=False):
        if host_id is None:
            self.db.execute('INSERT OR REPLACE INTO bridge_runtime VALUES(1,?,?,?,?)', (server, pid, time.time(), error))
        else:
            self.db.execute('INSERT OR REPLACE INTO bridge_client_runtime(host_id,server,pid,heartbeat,error,project_scope_worker,project_workspaces_worker) VALUES(?,?,?,?,?,?,?)',
                            (host_id, server, pid, time.time(), error, int(project_scope_worker), int(project_workspaces_worker)))

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
