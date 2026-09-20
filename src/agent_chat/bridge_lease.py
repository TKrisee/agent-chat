"""One durable dispatcher per client host and project."""
import contextlib
import hashlib
import secrets
import threading
import time

from .core import Coordinator, CoordError
from .bridge_state import BridgeState


class BridgeManager:
    def __init__(self, db_path, usage_store=None):
        self.db_path = db_path
        self.usage_store = usage_store
        self.mutex = threading.RLock()
        self.lock = None
        c = Coordinator(db_path)
        try:
            BridgeState(c)
            with c.tx():
                legacy = any(row['name'] == 'singleton' for row in c.db.execute('PRAGMA table_info(bridge_dispatcher)'))
                if legacy:
                    c.db.execute('ALTER TABLE bridge_dispatcher RENAME TO bridge_dispatcher_legacy')
                c.db.execute('''CREATE TABLE IF NOT EXISTS bridge_dispatcher (
                    host_id TEXT PRIMARY KEY, owner TEXT NOT NULL,
                    secret_hash TEXT NOT NULL, acquired_at REAL NOT NULL)''')
                if legacy:
                    c.db.execute('''INSERT INTO bridge_dispatcher
                        SELECT host_id,owner,secret_hash,acquired_at FROM bridge_dispatcher_legacy''')
                    c.db.execute('DROP TABLE bridge_dispatcher_legacy')
            if c.db.execute('SELECT 1 FROM bridge_dispatcher').fetchone():
                self._hold_lock()
        finally:
            c.close()

    def _hold_lock(self):
        if self.lock is None:
            from .bridge import exclusive_bridge
            context = exclusive_bridge(self.db_path, allow_remote=True)
            context.__enter__()
            self.lock = context

    def close(self):
        with self.mutex:
            if self.lock:
                self.lock.__exit__(None, None, None)
                self.lock = None

    def dispatch(self, body):
        if not isinstance(body, dict) or set(body) - {'op', 'owner', 'secret', 'host_id', 'params'}:
            raise CoordError('invalid bridge request')
        op, params = body.get('op'), body.get('params', {})
        if not isinstance(op, str) or not isinstance(params, dict):
            raise CoordError('invalid bridge operation or parameters')
        with self.mutex:
            c = Coordinator(self.db_path)
            try:
                return self._dispatch(c, op, body, params)
            finally:
                c.close()

    @contextlib.contextmanager
    def recovery(self, route):
        """Pause acquisition while recovering one stopped host's wake job."""
        if not route:
            raise CoordError('bind this session before recovering a wake job')
        with self.mutex:
            c = Coordinator(self.db_path)
            try:
                row = c.db.execute('''SELECT d.host_id FROM bridge_dispatcher d
                    JOIN remote_session_hosts h ON h.host_id=d.host_id
                    WHERE h.session_id=?''', (route['route'][0]['session_id'],)).fetchone()
                if row:
                    raise CoordError('stop the bridge on this route\'s client machine before recovering its wake job')
                # Other client hosts may keep dispatching. When none owns the
                # database, also exclude a legacy direct-database dispatcher.
                from .bridge import exclusive_bridge
                with contextlib.nullcontext() if self.lock else exclusive_bridge(self.db_path):
                    yield
            finally:
                c.close()

    def _dispatch(self, c, op, body, params):
        owner, secret, host = body.get('owner'), body.get('secret'), body.get('host_id')
        if not all(isinstance(x, str) and x.isascii() and 1 <= len(x) <= 256 for x in (owner, secret, host)) or len(secret) < 32:
            raise CoordError('invalid dispatcher identity')
        existing = c.db.execute('SELECT * FROM bridge_dispatcher WHERE host_id=?', (host,)).fetchone()
        if op == 'reset':
            if params != {'confirm_stopped': True}:
                raise CoordError('verify the previous bridge client is stopped before confirming recovery')
            c.db.execute('DELETE FROM bridge_dispatcher WHERE host_id=?', (host,))
            c.db.execute('DELETE FROM bridge_client_runtime WHERE host_id=?', (host,))
            if not c.db.execute('SELECT 1 FROM bridge_dispatcher').fetchone():
                self.close()
            return {'released': True}
        digest = hashlib.sha256(secret.encode()).hexdigest()
        same = existing and existing['owner'] == owner and existing['host_id'] == host and secrets.compare_digest(existing['secret_hash'], digest)
        if op == 'acquire':
            if existing and not same:
                raise CoordError('another bridge owns this host in this project; restart it with its saved identity')
            self._hold_lock()
            c.db.execute('INSERT OR IGNORE INTO bridge_dispatcher VALUES(?,?,?,?)',
                         (host, owner, digest, time.time()))
            return {'owner': owner, 'host_id': host, 'acquired': True}
        if not same:
            raise CoordError('dispatcher ownership is absent or has changed; reacquire before dispatch')
        if op == 'release':
            c.db.execute('DELETE FROM bridge_dispatcher WHERE host_id=?', (host,))
            c.db.execute('UPDATE bridge_client_runtime SET pid=0,error=? WHERE host_id=?', ('bridge stopped', host))
            if not c.db.execute('SELECT 1 FROM bridge_dispatcher').fetchone():
                self.close()
            return {'released': True}
        usage = self.usage_store.status(host) if self.usage_store else None
        state = BridgeState(c)

        def route_for(session_id):
            route = state.resolve(session_id)
            if not route:
                return None
            if not c.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='remote_session_hosts'").fetchone():
                return None
            row = c.db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (route['route'][0]['session_id'],)).fetchone()
            return route if row and row['host_id'] == host else None

        def thread_allowed(thread_id):
            if not isinstance(thread_id, str):
                raise CoordError('invalid thread id')
            row = c.db.execute('SELECT session_id FROM bridge_bindings WHERE thread_id=?', (thread_id,)).fetchone()
            if not row or not route_for(row['session_id']):
                raise CoordError('thread belongs to another host or is not remotely bound')

        def job_allowed(job_id):
            if not isinstance(job_id, str):
                raise CoordError('invalid job id')
            job = state.job(job_id)
            thread_allowed(job['thread_id'])
            return job

        if op == 'pending':
            if usage and usage['blocked']:
                return []
            return [m for m in state.pending() if route_for(m['recipient_session'])]
        if op == 'resolve':
            sid = params.get('session_id')
            if not isinstance(sid, str):
                raise CoordError('invalid session id')
            return route_for(sid)
        if op == 'jobs':
            jobs = []
            for job in state.jobs():
                try:
                    thread_allowed(job['thread_id'])
                except CoordError:
                    continue
                jobs.append(job)
            return jobs
        if op in ('job', 'still_unread', 'update'):
            job = job_allowed(params.get('job_id'))
            if op == 'job':
                return job
            if op == 'still_unread':
                return state.still_unread(job['id'])
            status = params.get('status')
            if status not in ('prepared', 'adding', 'queued', 'starting', 'uncertain', 'dispatched', 'cancelled', 'failed'):
                raise CoordError('invalid job status')
            if usage and usage['blocked'] and status in ('adding', 'starting'):
                raise CoordError(usage['reason'] or 'weekly usage reserve is active')
            queue_id, error = params.get('queue_id'), params.get('error')
            if any(v is not None and (not isinstance(v, str) or len(v) > 8192) for v in (queue_id, error)):
                raise CoordError('invalid queue/error metadata')
            state.update(job['id'], status, queue_id, error)
            return {'updated': True}
        if op == 'prepare':
            if usage and usage['blocked']:
                raise CoordError(usage['reason'] or 'weekly usage reserve is active')
            thread_id, messages, payload = params.get('thread_id'), params.get('messages'), params.get('payload')
            thread_allowed(thread_id)
            if not isinstance(messages, list) or not 1 <= len(messages) <= 100 or not isinstance(payload, str) or len(payload) > 262144:
                raise CoordError('invalid dispatch payload')
            pending = {m['id']: m for m in state.pending() if route_for(m['recipient_session'])}
            valid = []
            for message in messages:
                if not isinstance(message, dict) or message.get('id') not in pending or message != pending[message['id']]:
                    raise CoordError('dispatch message is no longer pending or does not match its recipient')
                if message not in valid:
                    valid.append(message)
            return state.prepare(thread_id, valid, payload)
        if op == 'observe':
            thread_allowed(params.get('thread_id'))
            if not isinstance(params.get('state'), str) or not 1 <= len(params['state']) <= 256:
                raise CoordError('invalid thread state')
            error = params.get('error')
            if error is not None and (not isinstance(error, str) or len(error) > 8192):
                raise CoordError('invalid observation error')
            state.observe(params['thread_id'], params['state'], error)
            return {'observed': True}
        if op == 'heartbeat':
            if not isinstance(params.get('server'), str) or not isinstance(params.get('pid'), int) or isinstance(params['pid'], bool):
                raise CoordError('invalid heartbeat')
            error = params.get('error')
            if error is not None and not isinstance(error, str):
                raise CoordError('invalid heartbeat error')
            state.heartbeat(params['server'], params['pid'], error, host_id=host)
            return {'heartbeat': True}
        raise CoordError('unsupported bridge operation')
