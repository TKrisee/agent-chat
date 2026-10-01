"""Durable main-agent admission controls; reservations are never transferred."""
import json
import time
import uuid

from .core import CoordError
from .session_reset import SessionResetState


class AgentControlState:
    def __init__(self, coord):
        self.coord, self.db = coord, coord.db
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS agent_controls (
                target_session TEXT PRIMARY KEY, mode TEXT NOT NULL, updated_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS agent_control_requests (
                id TEXT PRIMARY KEY, requester_session TEXT NOT NULL, target_session TEXT NOT NULL,
                host_id TEXT NOT NULL, thread_id TEXT NOT NULL, action TEXT NOT NULL,
                interrupt INTEGER NOT NULL, status TEXT NOT NULL, turn_id TEXT, error TEXT,
                created_at REAL NOT NULL, updated_at REAL NOT NULL);
            CREATE UNIQUE INDEX IF NOT EXISTS one_agent_control ON agent_control_requests(target_session)
                WHERE action<>'inspect' AND status NOT IN ('completed','failed','cancelled');
            CREATE TABLE IF NOT EXISTS agent_control_observations (
                target_session TEXT PRIMARY KEY, thread_id TEXT NOT NULL, state TEXT NOT NULL,
                turn_id TEXT, queue_count INTEGER NOT NULL, model TEXT, reasoning_effort TEXT,
                cwd TEXT, permissions_json TEXT, checked_at REAL NOT NULL);
        ''')

    def job(self, request_id):
        row = self.db.execute('SELECT * FROM agent_control_requests WHERE id=?', (request_id,)).fetchone()
        if not row: raise CoordError('unknown agent control request')
        return dict(row)

    def request(self, name, thread, action, request_id, confirmed, interrupt=False):
        if action not in ('stop', 'resume', 'inspect') or not isinstance(interrupt, bool):
            raise CoordError('invalid agent control action')
        if action != 'stop' and interrupt: raise CoordError('interrupt applies only to agent-stop')
        try: request_id = str(uuid.UUID(request_id))
        except (ValueError, TypeError, AttributeError): raise CoordError('request_id must be a UUID')
        if action != 'inspect' and confirmed is not True:
            raise CoordError('agent stop/resume requires explicit confirmation')
        caller = self.coord.require_session()
        target = SessionResetState(self.coord).target(name)['id']
        with self.coord.tx():
            existing = self.db.execute('SELECT * FROM agent_control_requests WHERE id=?', (request_id,)).fetchone()
            if existing:
                if (existing['requester_session'], existing['target_session'], existing['thread_id'], existing['action'], bool(existing['interrupt'])) != (caller, target, thread, action, interrupt):
                    raise CoordError('agent control retry payload differs from the original')
                return dict(existing)
            self.coord._fresh(self.db, caller)
            binding = self.db.execute('SELECT * FROM bridge_bindings WHERE session_id=?', (target,)).fetchone()
            if not binding or not binding['thread_id'] or binding['thread_id'] != thread:
                raise CoordError('agent control requires the exact current main thread; child routes are not independent agents')
            if action != 'inspect' and self.db.execute('SELECT 1 FROM bridge_bindings WHERE parent_session=?', (target,)).fetchone():
                raise CoordError('retire bound children before stopping their parent')
            host = self.db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (target,)).fetchone()
            if not host: raise CoordError('target has no authenticated bridge host')
            active = self.db.execute("SELECT * FROM agent_control_requests WHERE target_session=? AND action<>'inspect' AND status NOT IN ('completed','failed','cancelled')", (target,)).fetchone()
            if active:
                # A graceful wait has not issued an interrupt. Resume can safely
                # cancel that wait without changing the owner's resources.
                if action == 'resume' and active['action'] == 'stop' and active['status'] in ('pending','waiting') and not active['turn_id']:
                    self.db.execute("UPDATE agent_control_requests SET status='cancelled',updated_at=? WHERE id=?", (time.time(), active['id']))
                else: raise CoordError('another agent control is unresolved; inspect agent-status')
            now = time.time()
            self.db.execute('INSERT INTO agent_control_requests VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',
                (request_id, caller, target, host['host_id'], thread, action, int(interrupt), 'pending', None, None, now, now))
            if action == 'stop':
                self.db.execute("INSERT INTO agent_controls VALUES(?,'stopping',?) ON CONFLICT(target_session) DO UPDATE SET mode='stopping',updated_at=excluded.updated_at", (target, now))
        return self.job(request_id)

    def ownership(self, target):
        held = [dict(row) for row in self.db.execute('SELECT name AS resource,reservation_id,stale,deadline FROM resources WHERE owner_session=? ORDER BY name', (target,))]
        guards = []
        for table, column, identifier in (('guarded_runs','session','run_id'), ('remote_guarded_runs','session_id','run_id')):
            if self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone():
                guards.extend(dict(row) for row in self.db.execute(f'SELECT {identifier} AS run_id,resource,reservation_id FROM {table} WHERE {column}=? AND closed_at IS NULL', (target,)))
        return dict(reservations=held, open_guards=guards)

    def status(self, name):
        target = SessionResetState(self.coord).target(name)
        binding = self.db.execute('SELECT * FROM bridge_bindings WHERE session_id=?', (target['id'],)).fetchone()
        mode = self.db.execute('SELECT mode FROM agent_controls WHERE target_session=?', (target['id'],)).fetchone()
        observation = self.db.execute('SELECT * FROM agent_control_observations WHERE target_session=?', (target['id'],)).fetchone()
        thread = binding['thread_id'] if binding else None
        requests = [dict(row) for row in self.db.execute('SELECT * FROM agent_control_requests WHERE target_session=? ORDER BY created_at DESC LIMIT 10', (target['id'],))]
        wakes = [dict(row) for row in self.db.execute("SELECT id,status,queue_id,error FROM bridge_jobs WHERE thread_id=? AND status NOT IN ('dispatched','cancelled') ORDER BY created_at", (thread,))]
        return dict(target, thread_id=thread, parent_session=binding['parent_session'] if binding else None,
                    admission=mode['mode'] if mode else 'running', observation=dict(observation) if observation else None,
                    requests=requests, wakes=wakes, ownership=self.ownership(target['id']))

    def roster(self):
        return [dict(id=row['id'], agent=row['agent'], thread_id=row['thread_id'], parent_session=row['parent_session'],
                     admission=row['mode'] or 'running', state=row['state'], checked_at=row['checked_at'])
                for row in self.db.execute('''SELECT s.id,s.agent,b.thread_id,b.parent_session,c.mode,o.state,o.checked_at
                    FROM sessions s LEFT JOIN bridge_bindings b ON b.session_id=s.id
                    LEFT JOIN agent_controls c ON c.target_session=s.id
                    LEFT JOIN agent_control_observations o ON o.target_session=s.id ORDER BY s.agent''')]

    def jobs(self, host):
        return [dict(row, ownership=self.ownership(row['target_session'])) for row in self.db.execute(
            "SELECT * FROM agent_control_requests WHERE host_id=? AND status NOT IN ('completed','failed','cancelled') ORDER BY created_at", (host,))]

    def update(self, request_id, expected, status, *, turn_id=None, error=None, observation=None):
        if status not in ('waiting','interrupting','completed','failed'): raise CoordError('invalid agent control status')
        with self.coord.tx():
            job = self.job(request_id)
            if job['status'] != expected: raise CoordError('agent control changed; inspect its current request')
            binding = self.db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?', (job['target_session'],)).fetchone()
            if not binding or binding['thread_id'] != job['thread_id']:
                raise CoordError('agent thread changed; control withheld')
            if job['status'] == 'interrupting' and status == 'interrupting' and turn_id != job['turn_id']:
                raise CoordError('cannot replace an unresolved interrupt turn')
            if status == 'interrupting' and (job['action'] != 'stop' or not job['interrupt'] or not turn_id):
                raise CoordError('interrupt requires an explicit stop and exact turn')
            if observation:
                self.db.execute('INSERT OR REPLACE INTO agent_control_observations VALUES(?,?,?,?,?,?,?,?,?,?)',
                    (job['target_session'], job['thread_id'], observation['state'], observation.get('turn_id'), observation['queue_count'],
                     observation.get('model'), observation.get('reasoning_effort'), observation.get('cwd'),
                     json.dumps(observation.get('permissions'), sort_keys=True), time.time()))
            if status == 'completed' and job['action'] == 'stop':
                if not observation or observation['state'] != 'idle' or observation['queue_count']:
                    raise CoordError('stop completion requires observed idle and an empty app-server queue')
                own = self.ownership(job['target_session'])
                if not job['interrupt'] and (own['reservations'] or own['open_guards']):
                    raise CoordError('graceful stop waits for actual owner cleanup')
                self.db.execute("UPDATE agent_controls SET mode='stopped',updated_at=? WHERE target_session=?", (time.time(), job['target_session']))
            if status == 'completed' and job['action'] == 'resume':
                self.db.execute("INSERT INTO agent_controls VALUES(?,'running',?) ON CONFLICT(target_session) DO UPDATE SET mode='running',updated_at=excluded.updated_at", (job['target_session'], time.time()))
            self.db.execute('UPDATE agent_control_requests SET status=?,turn_id=COALESCE(?,turn_id),error=?,updated_at=? WHERE id=?',
                (status, turn_id, error, time.time(), request_id))
        return self.job(request_id)
