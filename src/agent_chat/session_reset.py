"""Durable fresh-conversation requests; never transfer resource ownership."""
import time
import uuid

from .core import CoordError, assert_reset_unlocked

TERMINAL = ('completed', 'cancelled', 'failed')


class SessionResetState:
    def __init__(self, coord):
        self.coord, self.db = coord, coord.db
        self.db.execute('''CREATE TABLE IF NOT EXISTS session_resets (
            id TEXT PRIMARY KEY, requester_session TEXT NOT NULL, target_session TEXT NOT NULL,
            host_id TEXT NOT NULL, old_thread_id TEXT NOT NULL, new_thread_id TEXT,
            prompt TEXT NOT NULL, status TEXT NOT NULL, queue_id TEXT, settings_digest TEXT,
            error TEXT, created_at REAL NOT NULL, updated_at REAL NOT NULL)''')
        self.db.execute('''CREATE UNIQUE INDEX IF NOT EXISTS one_pending_session_reset
            ON session_resets(target_session) WHERE status NOT IN ('completed','cancelled','failed')''')
        if 'kind' not in {row['name'] for row in self.db.execute('PRAGMA table_info(session_resets)')}:
            self.db.execute("ALTER TABLE session_resets ADD COLUMN kind TEXT NOT NULL DEFAULT 'reset'")
        if 'new_settings_digest' not in {row['name'] for row in self.db.execute('PRAGMA table_info(session_resets)')}:
            self.db.execute('ALTER TABLE session_resets ADD COLUMN new_settings_digest TEXT')
        for column in ('requested_model', 'requested_effort', 'requested_cwd'):
            if column not in {row['name'] for row in self.db.execute('PRAGMA table_info(session_resets)')}:
                self.db.execute(f'ALTER TABLE session_resets ADD COLUMN {column} TEXT')

    def target(self, name):
        row = self.db.execute('SELECT id,agent FROM sessions WHERE id=? OR agent=? ORDER BY (id=?) DESC', (name, name, name)).fetchone()
        if not row:
            raise CoordError('reset target must be an exact registered session or agent name')
        return dict(row)

    def job(self, job_id):
        row = self.db.execute('SELECT * FROM session_resets WHERE id=?', (job_id,)).fetchone()
        if not row:
            raise CoordError('unknown fresh-session reset request')
        return dict(row, target_agent=self.target(row['target_session'])['agent'])

    @staticmethod
    def public(job):
        return {key: value for key, value in job.items() if key not in ('prompt', 'settings_digest', 'new_settings_digest')}

    def status(self, name):
        target = self.target(name)
        binding = self.db.execute('SELECT * FROM bridge_bindings WHERE session_id=?', (target['id'],)).fetchone()
        jobs = [self.public(dict(row)) for row in self.db.execute(
            'SELECT * FROM session_resets WHERE target_session=? ORDER BY created_at DESC LIMIT 10', (target['id'],))]
        return dict(target, thread_id=binding['thread_id'] if binding else None, resets=jobs)

    def clean(self, target, old_thread):
        binding = self.db.execute('SELECT * FROM bridge_bindings WHERE session_id=?', (target,)).fetchone()
        if not binding or not binding['thread_id'] or binding['thread_id'] != old_thread:
            raise CoordError('reset requires the exact current main-conversation binding; child routes cannot be reset')
        if self.db.execute('SELECT 1 FROM bridge_bindings WHERE parent_session=?', (target,)).fetchone():
            raise CoordError('retire bound children before resetting their parent')
        if self.db.execute('SELECT 1 FROM resources WHERE owner_session=?', (target,)).fetchone():
            raise CoordError('close and receipt-release or recover every held reservation before resetting')
        for table, column in (('guarded_runs', 'session'), ('remote_guarded_runs', 'session_id')):
            if self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone():
                if self.db.execute(f'SELECT 1 FROM {table} WHERE {column}=? AND closed_at IS NULL', (target,)).fetchone():
                    raise CoordError('close open guarded runs with truthful receipts before resetting')
        if self.db.execute("SELECT 1 FROM bridge_jobs WHERE thread_id=? AND status NOT IN ('dispatched','cancelled')", (old_thread,)).fetchone():
            raise CoordError('resolve pending or uncertain wakes before resetting')

    def request(self, name, old_thread, prompt, request_id, confirmed, *, human=False):
        caller = self.coord.require_session()
        target = self.target(name)['id']
        try:
            request_id = str(uuid.UUID(request_id))
        except (ValueError, TypeError, AttributeError):
            raise CoordError('reset request_id must be a UUID')
        if confirmed is not True:
            raise CoordError('fresh-session reset requires explicit confirmation')
        if not isinstance(prompt, str) or not prompt.strip() or len(prompt.encode('utf-8')) > 65536:
            raise CoordError('reset prompt must be nonempty and at most 64 KiB')
        with self.coord.tx():
            existing = self.db.execute('SELECT * FROM session_resets WHERE id=?', (request_id,)).fetchone()
            if existing:
                if existing['kind'] != 'reset' or (existing['requester_session'], existing['target_session'], existing['old_thread_id'], existing['prompt']) != (caller, target, old_thread, prompt):
                    raise CoordError('reset retry payload differs from the original')
                return self.public(dict(existing))
            if not human:
                self.coord._fresh(self.db, caller)
            assert_reset_unlocked(self.db, target)
            self.clean(target, old_thread)
            host = self.db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (target,)).fetchone()
            if not host:
                raise CoordError('reset target has no authenticated bridge host')
            now = time.time()
            self.db.execute('INSERT INTO session_resets(id,requester_session,target_session,host_id,old_thread_id,prompt,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)',
                (request_id, caller, target, host['host_id'], old_thread, prompt, 'pending', now, now))
        return self.public(self.job(request_id))

    def create(self, agent, template_thread, prompt, request_id, confirmed, *, template_session=None, human=False, model=None, reasoning_effort=None, cwd=None):
        caller = self.coord.require_session()
        template = self.target(template_session or caller)['id']
        if not human and template != caller:
            raise CoordError('agent creation uses the requester\'s own conversation settings')
        if not isinstance(agent, str) or not agent.strip() or len(agent) > 64:
            raise CoordError('new agent name must be nonempty and at most 64 characters')
        for value, label, limit in ((model, 'model', 128), (reasoning_effort, 'reasoning effort', 32), (cwd, 'cwd', 4096)):
            if value is not None and (not isinstance(value, str) or not value.strip() or len(value) > limit):
                raise CoordError(f'creation {label} must be a nonempty bounded string')
        if cwd is not None and not cwd.startswith('/'):
            raise CoordError('creation cwd must be an absolute existing directory on the agent host')
        try:
            request_id = str(uuid.UUID(request_id))
        except (ValueError, TypeError, AttributeError):
            raise CoordError('creation request_id must be a UUID')
        if confirmed is not True or not isinstance(prompt, str) or not prompt.strip() or len(prompt.encode('utf-8')) > 65536:
            raise CoordError('agent creation requires confirmation and a nonempty prompt of at most 64 KiB')
        with self.coord.tx():
            existing = self.db.execute('SELECT * FROM session_resets WHERE id=?', (request_id,)).fetchone()
            if existing:
                if (existing['kind'], existing['requester_session'], existing['old_thread_id'], existing['prompt'], self.target(existing['target_session'])['agent'], existing['requested_model'], existing['requested_effort'], existing['requested_cwd']) != ('create', caller, template_thread, prompt, agent, model, reasoning_effort, cwd):
                    raise CoordError('creation retry payload differs from the original')
                return self.public(dict(existing))
            if not human:
                self.coord._fresh(self.db, caller)
            assert_reset_unlocked(self.db, template)
            binding = self.db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?', (template,)).fetchone()
            if not binding or binding['thread_id'] != template_thread or not template_thread:
                raise CoordError('agent creation requires the requester\'s exact main-conversation binding')
            if self.db.execute('SELECT 1 FROM sessions WHERE agent=?', (agent,)).fetchone():
                raise CoordError('new agent name is already registered')
            host = self.db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (template,)).fetchone()
            if not host:
                raise CoordError('requester has no authenticated bridge host')
            now, target = time.time(), 'session_'+uuid.uuid4().hex
            self.db.execute('INSERT INTO sessions(id,agent,registered_at) VALUES(?,?,?)', (target, agent, now))
            self.db.execute('INSERT INTO remote_session_hosts VALUES(?,?,?,?)', (target, host['host_id'], now, now))
            self.db.execute('INSERT INTO session_resets(id,requester_session,target_session,host_id,old_thread_id,prompt,status,created_at,updated_at,kind,requested_model,requested_effort,requested_cwd) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
                (request_id, caller, target, host['host_id'], template_thread, prompt, 'pending', now, now, 'create', model, reasoning_effort, cwd))
        return self.public(self.job(request_id))

    def retry_prompt(self, job_id, confirmed):
        with self.coord.tx():
            job = self.job(job_id)
            if self.coord.require_session() not in (job['requester_session'], job['target_session']):
                raise CoordError('only the requester or target can retry a reset prompt')
            if confirmed is not True or job['status'] != 'uncertain' or not job['new_thread_id']:
                raise CoordError('retry requires a known new thread and explicit confirmation that no prompt started')
            self.db.execute("UPDATE session_resets SET status='checking',error=NULL,updated_at=? WHERE id=?", (time.time(), job_id))
        return self.public(self.job(job_id))

    def cancel(self, job_id):
        with self.coord.tx():
            job = self.job(job_id)
            if self.coord.require_session() not in (job['requester_session'], job['target_session']):
                raise CoordError('only the requester or target can cancel a reset')
            if job['status'] not in ('pending', 'created', 'failed', 'cancelled'):
                raise CoordError('reset may have crossed the app-server boundary; inspect it instead of cancelling')
            self.db.execute("UPDATE session_resets SET status='cancelled',updated_at=? WHERE id=?", (time.time(), job_id))
        return self.public(self.job(job_id))

    def jobs(self, host):
        return [self.job(row['id']) for row in self.db.execute(
            "SELECT r.* FROM session_resets r WHERE r.host_id=? AND r.status NOT IN ('completed','cancelled','failed') AND NOT EXISTS (SELECT 1 FROM agent_controls c WHERE c.target_session=r.target_session AND c.mode<>'running') ORDER BY r.created_at", (host,))]

    def update(self, job_id, expected, status, **values):
        transitions = {
            'pending': {'pending', 'creating'}, 'creating': {'created', 'uncertain', 'failed'},
            'created': {'created', 'ready', 'failed'}, 'ready': {'ready', 'adding'},
            'adding': {'queued', 'uncertain', 'ready'}, 'queued': {'queued', 'starting', 'uncertain', 'completed'},
            'starting': {'queued', 'uncertain', 'completed'}, 'uncertain': {'uncertain', 'queued', 'completed', 'created'},
            'checking': {'checking', 'ready', 'queued', 'completed', 'uncertain'},
        }
        if status not in transitions.get(expected, set()) or set(values) - {'new_thread_id', 'queue_id', 'settings_digest', 'new_settings_digest', 'error'}:
            raise CoordError('invalid fresh-session reset transition')
        with self.coord.tx():
            job = self.job(job_id)
            if job['status'] != expected:
                raise CoordError('reset state changed; inspect current request before continuing')
            if status == 'creating' or (status == 'ready' and expected == 'created'):
                if job['kind'] == 'reset':
                    self.clean(job['target_session'], job['old_thread_id'])
                elif self.db.execute('SELECT 1 FROM bridge_bindings WHERE session_id=?', (job['target_session'],)).fetchone():
                    raise CoordError('new agent acquired an unexpected binding; creation withheld')
            if status == 'ready' and expected == 'created':
                new_thread = job['new_thread_id']
                try:
                    if str(uuid.UUID(new_thread)) != new_thread or new_thread == job['old_thread_id']:
                        raise ValueError()
                except (ValueError, TypeError, AttributeError):
                    raise CoordError('reset must bind a distinct actual Codex conversation UUID')
                if self.db.execute('SELECT 1 FROM bridge_bindings WHERE thread_id=?', (new_thread,)).fetchone():
                    raise CoordError('new reset conversation is already bound')
                if job['kind'] == 'create':
                    self.db.execute('INSERT INTO bridge_bindings VALUES(?,?,NULL,NULL,?)', (job['target_session'], new_thread, time.time()))
                else:
                    self.db.execute('UPDATE bridge_bindings SET thread_id=?,updated_at=? WHERE session_id=?',
                                    (new_thread, time.time(), job['target_session']))
            changes = dict(values, status=status, updated_at=time.time())
            self.db.execute('UPDATE session_resets SET ' + ','.join(key+'=?' for key in changes) + ' WHERE id=?', (*changes.values(), job_id))
        return self.job(job_id)

    def resolve_creation(self, job_id, thread_id, confirmed):
        with self.coord.tx():
            job = self.job(job_id)
            if self.coord.require_session() not in (job['requester_session'], job['target_session']):
                raise CoordError('only the requester or target can resolve a reset')
            if confirmed is not True or job['status'] != 'uncertain' or job['new_thread_id']:
                raise CoordError('inspect the lost creation and confirm its exact new thread before resolving')
            try:
                thread_id = str(uuid.UUID(thread_id))
            except (ValueError, TypeError, AttributeError):
                raise CoordError('resolved reset thread must be an actual Codex conversation UUID')
            if thread_id == job['old_thread_id']:
                raise CoordError('resolved reset thread must be fresh')
            self.db.execute("UPDATE session_resets SET status='created',new_thread_id=?,error=NULL,updated_at=? WHERE id=?", (thread_id, time.time(), job_id))
        return self.public(self.job(job_id))
