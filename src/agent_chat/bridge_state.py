"""Durable, opt-in wake routes. Inbox and reservation state remain untouched."""
from __future__ import annotations

import json
import time
import uuid
from pathlib import Path

from .core import CoordError, Coordinator

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

    def operator(self):
        sidecar = Path(str(self.coord.path) + '.web-session.json')
        try:
            value = json.loads(sidecar.read_text())['id']
        except (OSError, ValueError, KeyError, TypeError):
            raise CoordError('start agent-chat-server for this database first (operator identity is missing)')
        if not isinstance(value, str) or not self.db.execute('SELECT 1 FROM sessions WHERE id=?', (value,)).fetchone():
            raise CoordError('web operator identity is not registered in this database')
        return value

    def pending(self):
        return [dict(r) for r in self.db.execute('''
            SELECT m.id,m.recipient_session FROM messages m
            LEFT JOIN bridge_deliveries d ON d.message_id=m.id
            WHERE m.sender_session=? AND m.acked_at IS NULL AND d.message_id IS NULL
            ORDER BY m.seq''', (self.operator(),))]

    def still_unread(self, job_id):
        return bool(self.db.execute('''SELECT 1 FROM bridge_deliveries d
            JOIN messages m ON m.id=d.message_id
            WHERE d.job_id=? AND m.acked_at IS NULL LIMIT 1''', (job_id,)).fetchone())

    def connection_metadata(self):
        return {'database': str(self.coord.path.resolve())}

    def prepare(self, thread_id, messages, payload):
        job_id = 'wake_' + uuid.uuid4().hex
        with self.coord.tx():
            live = []
            for message in messages:
                row = self.db.execute('SELECT acked_at,recipient_session,sender_session FROM messages WHERE id=?', (message['id'],)).fetchone()
                route = self.resolve(message['recipient_session'])
                if (row and row['acked_at'] is None and row['recipient_session'] == message['recipient_session']
                        and row['sender_session'] == self.operator() and route and route['thread_id'] == thread_id
                        and not self.db.execute('SELECT 1 FROM bridge_deliveries WHERE message_id=?', (message['id'],)).fetchone()
                        and message not in live):
                    live.append(message)
            if not live:
                return None
            self.db.execute('INSERT INTO bridge_jobs VALUES(?,?,?,?,?,?,?,?)',
                            (job_id, thread_id, payload, 'prepared', None, None, time.time(), time.time()))
            self.db.executemany('INSERT INTO bridge_deliveries VALUES(?,?)', [(m['id'], job_id) for m in live])
        return self.job(job_id)

    def job(self, job_id):
        row = self.db.execute('SELECT * FROM bridge_jobs WHERE id=?', (job_id,)).fetchone()
        if not row:
            raise CoordError('unknown bridge job')
        return dict(row)

    def jobs(self):
        return [dict(r) for r in self.db.execute("SELECT * FROM bridge_jobs WHERE status NOT IN ('dispatched','cancelled') ORDER BY created_at")]

    def update(self, job_id, status, queue_id=None, error=None):
        self.db.execute('UPDATE bridge_jobs SET status=?,queue_id=COALESCE(?,queue_id),error=?,updated_at=? WHERE id=?',
                        (status, queue_id, error, time.time(), job_id))

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
            self.db.execute("UPDATE bridge_jobs SET status='prepared',queue_id=NULL,error=NULL,updated_at=? WHERE id=?", (time.time(), job_id))
        return self.job(job_id)

    def heartbeat(self, server, pid, error=None, *, host_id=None):
        if host_id is None:
            self.db.execute('INSERT OR REPLACE INTO bridge_runtime VALUES(1,?,?,?,?)', (server, pid, time.time(), error))
        else:
            self.db.execute('INSERT OR REPLACE INTO bridge_client_runtime VALUES(?,?,?,?,?)',
                            (host_id, server, pid, time.time(), error))

    def observe(self, thread_id, state, error=None):
        self.db.execute('INSERT OR REPLACE INTO bridge_observations VALUES(?,?,?,?)',
                        (thread_id, state, error, time.time()))

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

    def status(self):
        return {'runtime': runtime_snapshot(self.db),
                'bindings': [dict(r) for r in self.db.execute('SELECT * FROM bridge_bindings ORDER BY updated_at')],
                'threads': [dict(r) for r in self.db.execute('SELECT * FROM bridge_observations ORDER BY checked_at DESC')],
                'jobs': [dict(r) for r in self.db.execute('SELECT id,thread_id,status,queue_id,error,created_at,updated_at FROM bridge_jobs ORDER BY created_at DESC LIMIT 100')]}
