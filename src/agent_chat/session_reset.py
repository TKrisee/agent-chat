"""Durable fresh-conversation requests; never transfer resource ownership."""
import time
import json
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

        self.db.execute('CREATE TABLE IF NOT EXISTS session_reset_workspaces (request_id TEXT NOT NULL,position INTEGER NOT NULL,path TEXT NOT NULL,PRIMARY KEY(request_id,position))')
        self.db.execute('CREATE TABLE IF NOT EXISTS parked_native_rejections (request_id TEXT PRIMARY KEY, code INTEGER NOT NULL, observed_at REAL NOT NULL)')
        self.db.execute('CREATE TABLE IF NOT EXISTS parked_native_rpc_errors (request_id TEXT PRIMARY KEY, code INTEGER NOT NULL, message TEXT NOT NULL, observed_at REAL NOT NULL)')
        self.db.execute("CREATE TRIGGER IF NOT EXISTS parked_native_rpc_errors_no_update BEFORE UPDATE ON parked_native_rpc_errors BEGIN SELECT RAISE(ABORT,'parked RPC error receipt immutable'); END")
        self.db.execute("CREATE TRIGGER IF NOT EXISTS parked_native_rpc_errors_no_delete BEFORE DELETE ON parked_native_rpc_errors BEGIN SELECT RAISE(ABORT,'parked RPC error receipt immutable'); END")
        self.db.execute("CREATE TRIGGER IF NOT EXISTS parked_native_rejections_no_update BEFORE UPDATE ON parked_native_rejections BEGIN SELECT RAISE(ABORT,'parked rejection receipt immutable'); END")
        self.db.execute("CREATE TRIGGER IF NOT EXISTS parked_native_rejections_no_delete BEFORE DELETE ON parked_native_rejections BEGIN SELECT RAISE(ABORT,'parked rejection receipt immutable'); END")

        if 'workspace_revalidated' not in {row['name'] for row in self.db.execute('PRAGMA table_info(session_resets)')}:
            self.db.execute('ALTER TABLE session_resets ADD COLUMN workspace_revalidated INTEGER NOT NULL DEFAULT 0')
        if 'project_scope' not in {row['name'] for row in self.db.execute('PRAGMA table_info(session_resets)')}:
            self.db.execute('ALTER TABLE session_resets ADD COLUMN project_scope INTEGER NOT NULL DEFAULT 0')

    def target(self, name):
        row = self.db.execute('SELECT id,agent FROM sessions WHERE id=? OR agent=? ORDER BY (id=?) DESC', (name, name, name)).fetchone()
        if not row:
            raise CoordError('reset target must be an exact registered session or agent name')
        return dict(row)

    def job(self, job_id):
        row = self.db.execute('SELECT * FROM session_resets WHERE id=?', (job_id,)).fetchone()
        if not row:
            raise CoordError('unknown fresh-session reset request')
        members = [item['path'] for item in self.db.execute('SELECT path FROM session_reset_workspaces WHERE request_id=? ORDER BY position', (job_id,))]
        from .management import parked_creation
        admitted = bool(self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_turnover_bootstraps'").fetchone() and self.db.execute('SELECT 1 FROM root_turnover_bootstraps WHERE request_id=?', (job_id,)).fetchone())
        rpc_error = self.db.execute('SELECT code,message,observed_at FROM parked_native_rpc_errors WHERE request_id=?', (job_id,)).fetchone()
        return dict(row, parked=bool(parked_creation(self.db, job_id)), turnover_bootstrap=admitted, native_rpc_error=dict(rpc_error) if rpc_error else None, target_agent=self.target(row['target_session'])['agent'],
                    role_context=self.role_context(row['target_session']),
                    requested_workspaces=members or ([row['requested_cwd']] if row['requested_cwd'] else []))

    def role_context(self, session):
        # Read the current designation, including on retries and worker updates.
        # A name or a previously saved task prompt is not role authority.
        if self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='project_orchestrators'").fetchone():
            role = self.db.execute('SELECT root_session FROM project_orchestrators WHERE session_id=?', (session,)).fetchone()
            if role:
                return dict(session=session, role='project_orchestrator',
                            authority='project_orchestrators', root_session=role['root_session'])
        return dict(session=session, role='worker', authority='registered_session')

    def save_workspaces(self, request_id, members):
        self.db.executemany('INSERT INTO session_reset_workspaces VALUES(?,?,?)',
                           [(request_id, index, member) for index, member in enumerate(members)])

    def requires_workspace_worker(self, job):
        if job.get('project_scope') and (job.get('kind') != 'create' or len(job.get('requested_workspaces') or []) > 1):
            return True
        if job.get('kind') != 'reset':
            return False
        # Ordinary resets preserve their existing native roots. Legacy workers
        # must also be withheld when those roots were observed or provisioned.
        observed = self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='agent_control_workspace_roots'").fetchone()
        if observed and self.db.execute('SELECT COUNT(*) FROM agent_control_workspace_roots w '
                'JOIN agent_control_observations o ON o.target_session=w.target_session '
                'WHERE w.target_session=? AND o.thread_id=?',
                (job['target_session'], job['old_thread_id'])).fetchone()[0] > 1:
            return True
        return bool(self.db.execute('SELECT 1 FROM session_resets r '
                'JOIN session_reset_workspaces w ON w.request_id=r.id '
                'WHERE r.target_session=? AND r.new_thread_id=? AND r.project_scope=1 '
                'GROUP BY r.id HAVING COUNT(*)>1',
                (job['target_session'], job['old_thread_id'])).fetchone())

    @staticmethod
    def public(job):
        return {key: value for key, value in job.items() if key not in ('prompt', 'settings_digest', 'new_settings_digest')}

    def status(self, name):
        target = self.target(name)
        binding = self.db.execute('SELECT * FROM bridge_bindings WHERE session_id=?', (target['id'],)).fetchone()
        jobs = [self.public(self.job(row['id'])) for row in self.db.execute(
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

    def request(self, name, old_thread, prompt, request_id, confirmed, *, human=False, project_scope=False, workspaces=None, model=None, reasoning_effort=None):
        caller = self.coord.require_session()
        target = self.target(name)['id']
        from .root_orchestrator import assert_local_lifecycle, assert_management_target
        assert_local_lifecycle(self.db, target)
        assert_management_target(self.db, caller, target)
        members = []
        if project_scope:
            from .projects import Projects
            from .root_orchestrator import projection
            if not projection(self.db, caller):
                raise CoordError('project workspace reconfiguration requires the authorized root projection')
            members = Projects.workspace_list(workspaces)
        elif workspaces is not None:
            raise CoordError('explicit workspace reset requires supported root reconfiguration')
        if (model is None) != (reasoning_effort is None):
            raise CoordError('root workspace reconfiguration requires model and reasoning effort together')
        if model is not None:
            if not project_scope:
                raise CoordError('model override requires supported root workspace reconfiguration')
            for value, label, limit in ((model, 'model', 128), (reasoning_effort, 'reasoning effort', 32)):
                if not isinstance(value, str) or not value.strip() or len(value) > limit:
                    raise CoordError(f'reconfiguration {label} must be a nonempty bounded string')
        try:
            request_id = str(uuid.UUID(request_id))
        except (ValueError, TypeError, AttributeError):
            raise CoordError('reset request_id must be a UUID')
        if confirmed is not True:
            raise CoordError('fresh-session reset requires explicit confirmation')
        if not isinstance(prompt, str) or not prompt.strip() or len(prompt.encode('utf-8')) > 65536:
            raise CoordError('reset prompt must be nonempty and at most 64 KiB')
        with self.coord.tx():
            assert_local_lifecycle(self.db, target)
            assert_management_target(self.db, caller, target)
            existing = self.db.execute('SELECT * FROM session_resets WHERE id=?', (request_id,)).fetchone()
            if existing:
                from .parked_retirement import assert_not_retired
                assert_not_retired(self.db, request_id)
                if existing['kind'] != 'reset' or (existing['requester_session'], existing['target_session'], existing['old_thread_id'], existing['prompt'], existing['project_scope'], existing['requested_model'], existing['requested_effort'], self.job(request_id)['requested_workspaces']) != (caller, target, old_thread, prompt, int(project_scope), model, reasoning_effort, members):
                    raise CoordError('reset retry payload differs from the original')
                return self.public(self.job(request_id))
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
            if project_scope:
                self.db.execute('UPDATE session_resets SET project_scope=1,requested_cwd=?,requested_model=?,requested_effort=? WHERE id=?', (members[0], model, reasoning_effort, request_id))
                self.save_workspaces(request_id, members)
        return self.public(self.job(request_id))

    def create(self, agent, template_thread, prompt, request_id, confirmed, *, template_session=None, human=False, model=None, reasoning_effort=None, cwd=None, project_scope=False, project_orchestrator=False, workspaces=None, _direct_scope=None, _park_stop=None, _sub_bootstrap=None, _sub_registry=None):
        from .root_team_reset import assert_ready
        assert_ready(self.db)
        if not _park_stop and self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_team_stops'").fetchone() and self.db.execute('SELECT 1 FROM root_team_stops WHERE active=1').fetchone():
            raise CoordError('root stop-all barrier is active; agent creation is withheld until explicit resume')
        caller = self.coord.require_session()
        template = self.target(template_session or caller)['id']
        from .root_orchestrator import projection
        members = []
        if workspaces is not None:
            from .projects import Projects
            if not project_scope:
                raise CoordError('explicit project member grants require supported root configuration')
            members = Projects.workspace_list(workspaces)
            if cwd is not None and cwd != members[0]:
                raise CoordError('cwd must equal the first project workspace')
            cwd = members[0]
        elif cwd:
            members = [cwd]
        root = projection(self.db, template)
        if root:
            if not project_orchestrator and not _direct_scope:
                raise CoordError('root creates project orchestrators through root configure; project orchestrators create workers')
            from .core import assert_agent_running
            assert_agent_running(self.db, template)
            if not root['cwd'] or cwd not in (None, root['cwd']):
                raise CoordError('configure the exact project workspace before using root as a creation template')
            project_scope, cwd = True, root['cwd']
            model, reasoning_effort = model or root['model'], reasoning_effort or root['effort']
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
            if not _park_stop and self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_team_stops'").fetchone() and self.db.execute('SELECT 1 FROM root_team_stops WHERE active=1').fetchone():
                raise CoordError('root stop-all barrier is active; agent creation is withheld until explicit resume')
            if _park_stop:
                if _sub_bootstrap:
                    from .turnover_bootstrap import authorize_subordinate
                    proof = authorize_subordinate(self.db, _sub_registry, _sub_bootstrap, caller, template_thread, self.db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (caller,)).fetchone()['host_id'], model, reasoning_effort)
                    if project_orchestrator or _direct_scope or proof['stop_id'] != _park_stop:
                        raise CoordError('parked subordinate scope differs')
                else:
                    if not _direct_scope or not project_orchestrator or model != 'gpt-6.2-sol':
                        raise CoordError('parked creation requires the exact direct PO scope and model')
                    stops = self.db.execute('SELECT id,root_session FROM root_team_stops WHERE active=1').fetchall()
                    if len(stops) != 1 or tuple(stops[0]) != (_park_stop, caller):
                        raise CoordError('parked creation requires the exact active project stop')
            if _direct_scope:
                from .management import assert_governing_reset
                if not root:
                    raise CoordError('direct Root creation requires the current Root projection')
                assert_governing_reset(self.db,_direct_scope[1],root['home_session'],_direct_scope[0])
            existing = self.db.execute('SELECT * FROM session_resets WHERE id=?', (request_id,)).fetchone()
            if existing:
                from .management import parked_creation, assert_parked_creation
                parked = parked_creation(self.db, request_id)
                if bool(parked) != bool(_park_stop) or (parked and parked['stop_id'] != _park_stop):
                    raise CoordError('creation retry parked scope differs')
                if parked and (('bootstrap_id' in parked.keys()) != bool(_sub_bootstrap) or (_sub_bootstrap and parked['bootstrap_id'] != _sub_bootstrap)):
                    raise CoordError('creation retry bootstrap scope differs')
                if parked:
                    assert_parked_creation(self.db, request_id)
                if _direct_scope:
                    if not self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_direct_creations'").fetchone():
                        raise CoordError('direct Root worker creation retry scope differs')
                    saved = self.db.execute('SELECT project_id,reset_id FROM root_direct_creations WHERE request_id=?', (request_id,)).fetchone()
                    if not saved or tuple(saved) != _direct_scope:
                        raise CoordError('direct Root worker creation retry scope differs')
                if (existing['kind'], existing['requester_session'], existing['old_thread_id'], existing['prompt'], self.target(existing['target_session'])['agent'], existing['requested_model'], existing['requested_effort'], existing['requested_cwd'], existing['project_scope']) != ('create', caller, template_thread, prompt, agent, model, reasoning_effort, cwd, int(project_scope)) or self.job(request_id)['requested_workspaces'] != members:
                    raise CoordError('creation retry payload differs from the original')
                return self.public(self.job(request_id))
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
            self.save_workspaces(request_id, members)
            if project_scope:
                self.db.execute('UPDATE session_resets SET project_scope=1 WHERE id=?', (request_id,))
            if project_orchestrator:
                self.db.execute('INSERT OR REPLACE INTO project_orchestrators VALUES(?,?)', (target, caller))
            if _direct_scope:
                from .management import record_root_creation
                record_root_creation(self.coord, request_id, *_direct_scope, parked=bool(_park_stop))
            elif not project_orchestrator:
                from .management import record_creation
                record_creation(self.coord, request_id)
            if _park_stop:
                if _sub_bootstrap:
                    self.db.execute('''CREATE TABLE IF NOT EXISTS root_turnover_subordinates (
                        request_id TEXT PRIMARY KEY, bootstrap_id TEXT NOT NULL, project_id TEXT NOT NULL,
                        reset_id TEXT NOT NULL, stop_id TEXT NOT NULL, root_session TEXT NOT NULL,
                        target_session TEXT NOT NULL UNIQUE, thread_id TEXT NOT NULL, host_id TEXT NOT NULL)''')
                    self.db.execute("CREATE TRIGGER IF NOT EXISTS turnover_subordinate_no_update BEFORE UPDATE ON root_turnover_subordinates BEGIN SELECT RAISE(ABORT,'turnover subordinate immutable'); END")
                    self.db.execute("CREATE TRIGGER IF NOT EXISTS turnover_subordinate_no_delete BEFORE DELETE ON root_turnover_subordinates BEGIN SELECT RAISE(ABORT,'turnover subordinate immutable'); END")
                    self.db.execute('INSERT INTO root_turnover_subordinates VALUES(?,?,?,?,?,?,?,?,?)', (request_id,_sub_bootstrap,proof['project_id'],proof['reset_id'],proof['stop_id'],proof['root_session'],target,template_thread,host['host_id']))
                    return self.public(self.job(request_id))
                self.db.execute('''CREATE TABLE IF NOT EXISTS root_parked_creations (
                    request_id TEXT PRIMARY KEY, project_id TEXT NOT NULL, reset_id TEXT NOT NULL,
                    stop_id TEXT NOT NULL, root_session TEXT NOT NULL, target_session TEXT NOT NULL UNIQUE,
                    thread_id TEXT NOT NULL, host_id TEXT NOT NULL)''')
                self.db.execute("CREATE TRIGGER IF NOT EXISTS root_parked_no_update BEFORE UPDATE ON root_parked_creations BEGIN SELECT RAISE(ABORT,'parked creation audit immutable'); END")
                self.db.execute("CREATE TRIGGER IF NOT EXISTS root_parked_no_delete BEFORE DELETE ON root_parked_creations BEGIN SELECT RAISE(ABORT,'parked creation audit immutable'); END")
                self.db.execute('CREATE TABLE IF NOT EXISTS root_parked_activations (request_id TEXT PRIMARY KEY)')
                self.db.execute("CREATE TRIGGER IF NOT EXISTS root_parked_activation_no_update BEFORE UPDATE ON root_parked_activations BEGIN SELECT RAISE(ABORT,'parked activation immutable'); END")
                self.db.execute("CREATE TRIGGER IF NOT EXISTS root_parked_activation_no_delete BEFORE DELETE ON root_parked_activations BEGIN SELECT RAISE(ABORT,'parked activation immutable'); END")
                self.db.execute('INSERT INTO root_parked_creations VALUES(?,?,?,?,?,?,?,?)',
                    (request_id,*_direct_scope,_park_stop,caller,target,template_thread,host['host_id']))
                from .management import assert_parked_creation
                assert_parked_creation(self.db,request_id)
        return self.public(self.job(request_id))

    def revalidate_info(self, job_id):
        job = self.job(job_id)
        caller = self.coord.require_session()
        from .root_orchestrator import assert_management_target
        host = self.db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (caller,)).fetchone()
        if caller != job['requester_session'] or not host or host['host_id'] != job['host_id']:
            raise CoordError('only the original requester on the creation host may revalidate settings')
        target_host = self.db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (job['target_session'],)).fetchone()
        if not target_host or target_host['host_id'] != job['host_id']:
            raise CoordError('creation candidate host differs from the original requester')
        bound = self.db.execute('SELECT 1 FROM bridge_bindings WHERE session_id=? OR thread_id=?',
                                (job['target_session'], job['new_thread_id'])).fetchone()
        if bound:
            # Preserve the ordinary lifecycle route after a candidate acquires a
            # binding; the unbound exception below never becomes a generic
            # parent-null management bypass.
            assert_management_target(self.db, caller, job['target_session'])
        elif job['kind'] != 'create' or job.get('project_scope'):
            raise CoordError('unbound revalidation is limited to the original non-project-scope creation candidate')
        elif (self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='project_orchestrators'").fetchone()
              and not self.db.execute('SELECT 1 FROM project_orchestrators WHERE session_id=?', (caller,)).fetchone()):
            raise CoordError('unbound revalidation requires the original project orchestrator')
        if job.get('project_scope'):
            raise CoordError('explicit project workspace settings require exact native readback; named-profile reclassification is withheld')
        if job['kind'] != 'create' or not job['requested_cwd']:
            raise CoordError('workspace revalidation requires an explicit-workspace agent creation')
        if not job['workspace_revalidated'] and (job['status'] != 'created' or not job['new_thread_id'] or job['queue_id'] or not job['new_settings_digest']):
            raise CoordError('workspace revalidation requires the exact created candidate before prompt submission')
        return self.public(job)

    def revalidate_workspace(self, job_id, proof, confirmed):
        from .creation_settings import creation_settings, resolve_workspace_settings, settings_data
        with self.coord.tx():
            from .parked_retirement import assert_not_retired
            assert_not_retired(self.db, job_id)
            self.revalidate_info(job_id)
            job = self.job(job_id)
            if confirmed is not True:
                raise CoordError('workspace revalidation requires explicit confirmation')
            if job['workspace_revalidated']:
                return self.public(job)
            self.coord._fresh(self.db, self.coord.require_session())
            if self.db.execute('SELECT 1 FROM bridge_bindings WHERE session_id=? OR thread_id=?', (job['target_session'], job['new_thread_id'])).fetchone():
                raise CoordError('candidate acquired an unexpected binding; revalidation withheld')
            if not isinstance(proof, dict) or set(proof) != {'source', 'source_profiles', 'target_profiles', 'thread_id'} or proof['thread_id'] != job['new_thread_id']:
                raise CoordError('workspace revalidation proof must name the exact candidate')
            try:
                source, _ = settings_data(proof['source'])
                if not isinstance(source.get('modelProvider'), str) or not source['modelProvider']:
                    raise CoordError('source provider identity is unavailable; fresh inspection required')
                desired = creation_settings(source, job, job['requested_cwd'])
                if settings_data(desired)[1] != job['settings_digest']:
                    raise CoordError('original creation settings changed; revalidation withheld')
                # The existing authenticated bridge, not the requester-supplied
                # proof, anchors permissions that were implicit in the old cwd.
                observed = self.db.execute('SELECT * FROM agent_control_observations WHERE target_session=? AND thread_id=?', (job['requester_session'], job['old_thread_id'])).fetchone()
                binding = self.db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?', (job['requester_session'],)).fetchone()
                if not observed or not binding or binding['thread_id'] != job['old_thread_id'] or observed['checked_at'] < job['created_at'] or time.time() - observed['checked_at'] > 120:
                    raise CoordError('refresh the original requester agent-status through the existing bridge before revalidation')
                permissions = {key: source.get(key) for key in ('modelProvider', 'sandbox', 'approvalPolicy', 'approvalsReviewer', 'activePermissionProfile', 'runtimeWorkspaceRoots')}
                if observed['permissions_json'] is None:
                    raise CoordError('authenticated source provider is unavailable; refresh the original requester inspection')
                observed_permissions = json.loads(observed['permissions_json'])
                if (not isinstance(observed_permissions, dict) or
                        not isinstance(observed_permissions.get('modelProvider'), str) or
                        not observed_permissions['modelProvider']):
                    raise CoordError('authenticated source provider is unavailable; refresh the original requester inspection')
                if (observed['cwd'], observed['model'], observed['reasoning_effort'], observed_permissions) != (source['cwd'], source['model'], source['reasoningEffort'], permissions):
                    raise CoordError('source settings do not match the authenticated bridge observation')
                corrected = resolve_workspace_settings(source, desired, proof['source_profiles'], proof['target_profiles'])
                digest = settings_data(corrected)[1]
            except (KeyError, TypeError, AttributeError, ValueError) as error:
                raise CoordError('invalid workspace revalidation proof') from error
            if digest == job['settings_digest'] or digest != job['new_settings_digest']:
                raise CoordError('captured candidate settings do not match resolved original permissions; revalidation withheld')
            self.db.execute('UPDATE session_resets SET settings_digest=?,workspace_revalidated=1,error=NULL,updated_at=? WHERE id=?', (digest, time.time(), job_id))
        return self.public(self.job(job_id))

    def retry_prompt(self, job_id, confirmed):
        with self.coord.tx():
            from .parked_retirement import assert_not_retired
            assert_not_retired(self.db, job_id)
            job = self.job(job_id)
            from .root_orchestrator import assert_management_target
            assert_management_target(self.db, self.coord.require_session(), job['target_session'])
            if self.coord.require_session() not in (job['requester_session'], job['target_session']):
                raise CoordError('only the requester or target can retry a reset prompt')
            if job['parked']:
                raise CoordError('parked creation has no prompt retry admission')
            if confirmed is not True or job['status'] != 'uncertain' or not job['new_thread_id']:
                raise CoordError('retry requires a known new thread and explicit confirmation that no prompt started')
            self.db.execute("UPDATE session_resets SET status='checking',error=NULL,updated_at=? WHERE id=?", (time.time(), job_id))
        return self.public(self.job(job_id))

    def cancel(self, job_id):
        with self.coord.tx():
            from .parked_retirement import assert_not_retired
            assert_not_retired(self.db, job_id)
            job = self.job(job_id)
            from .root_orchestrator import assert_management_target
            assert_management_target(self.db, self.coord.require_session(), job['target_session'])
            if self.coord.require_session() not in (job['requester_session'], job['target_session']):
                raise CoordError('only the requester or target can cancel a reset')
            if job['status'] not in ('pending', 'created', 'failed', 'cancelled'):
                raise CoordError('reset may have crossed the app-server boundary; inspect it instead of cancelling')
            self.db.execute("UPDATE session_resets SET status='cancelled',updated_at=? WHERE id=?", (time.time(), job_id))
        return self.public(self.job(job_id))

    def jobs(self, host, project_scope=False, project_workspaces=False, parked_worker=False):
        from .root_team_reset import quarantined
        jobs = [self.job(row['id']) for row in self.db.execute(
            "SELECT r.* FROM session_resets r WHERE r.host_id=? AND (r.project_scope=0 OR ?) AND r.status NOT IN ('completed','cancelled','failed') AND NOT EXISTS (SELECT 1 FROM agent_controls c WHERE c.target_session=r.target_session AND c.mode<>'running') ORDER BY r.created_at", (host, int(project_scope)))
            if not quarantined(self.db,session=row['target_session'],job=row['id'])]
        from .root_lifecycle import stopped
        selected = []
        for job in jobs:
            if job['parked']:
                if not parked_worker:
                    continue
                if job['status'] == 'parked' and not job['turnover_bootstrap']:
                    continue
                from .management import assert_parked_active
                try:
                    assert_parked_active(self.db, job['id'])
                except CoordError:
                    continue
            elif stopped(self.db, job['target_session']):
                continue
            if project_workspaces or not self.requires_workspace_worker(job):
                selected.append(job)
        return selected

    def update(self, job_id, expected, status, *, _turnover=False, **values):
        transitions = {
            'pending': {'pending', 'creating'}, 'creating': {'created', 'uncertain', 'failed'},
            'created': {'created', 'ready', 'materializing', 'failed'}, 'materializing': {'materializing', 'parked'}, 'parked': {'ready'}, 'ready': {'ready', 'adding'},
            'adding': {'queued', 'uncertain', 'ready'}, 'queued': {'queued', 'starting', 'uncertain', 'completed'},
            'starting': {'queued', 'uncertain', 'completed'}, 'uncertain': {'uncertain', 'queued', 'completed', 'created'},
            'checking': {'checking', 'ready', 'queued', 'completed', 'uncertain'},
        }
        if status not in transitions.get(expected, set()) or set(values) - {'new_thread_id', 'queue_id', 'settings_digest', 'new_settings_digest', 'error', 'native_error_code', 'native_rpc_code', 'native_rpc_message'}:
            raise CoordError('invalid fresh-session reset transition')
        with self.coord.tx():
            job = self.job(job_id)
            from .parked_retirement import assert_not_retired
            assert_not_retired(self.db, job_id)
            if job['status'] != expected:
                raise CoordError('reset state changed; inspect current request before continuing')
            if (status == 'materializing' or expected == 'materializing') and not job['parked']:
                raise CoordError('materialization is restricted to parked creation')
            native_error_code = values.pop('native_error_code',None)
            native_rpc_code = values.pop('native_rpc_code', None)
            native_rpc_message = values.pop('native_rpc_message', None)
            if native_rpc_code is not None or native_rpc_message is not None:
                if (not job['parked'] or expected != 'creating' or status not in ('uncertain','failed')
                        or type(native_rpc_code) is not int or native_rpc_code >= 0
                        or not isinstance(native_rpc_message, str)
                        or (status == 'failed') != (native_rpc_code == -32602)):
                    raise CoordError('invalid parked native RPC error receipt')
            if native_error_code is not None and (not job['parked'] or (expected,status,native_error_code) != ('creating','failed',-32602)):
                raise CoordError('invalid parked native rejection receipt')
            if job['parked']:
                from .management import assert_parked_active
                if expected != 'creating':
                    assert_parked_active(self.db,job_id)
                bootstrap_transitions = {('parked','ready'),('ready','ready'),('ready','adding'),('adding','queued'),('adding','uncertain'),('queued','queued'),('queued','starting'),('queued','uncertain'),('queued','completed'),('starting','completed'),('starting','uncertain'),('uncertain','queued'),('uncertain','completed')}
                if (expected,status) not in {('pending','pending'),('pending','creating'),('creating','created'),('creating','uncertain'),('creating','failed'),('created','created'),('created','materializing'),('materializing','materializing'),('materializing','parked'),('uncertain','uncertain')} and not (_turnover and job['turnover_bootstrap'] and (expected,status) in bootstrap_transitions):
                    raise CoordError('parked creation cannot submit model input')
                verified_digest = values.get('new_settings_digest', job['new_settings_digest'])
                if status == 'parked' and (not job['settings_digest'] or job['settings_digest'] != verified_digest
                                           or (job['new_settings_digest'] and job['new_settings_digest'] != verified_digest)):
                    raise CoordError('parked creation requires matching verified settings')
            if (expected,status) in (('pending','creating'),('created','ready'),('created','materializing'),('materializing','parked'),('ready','adding'),('queued','starting')):
                from .core import assert_agent_running
                if job['parked']:
                    assert_agent_running(self.db, job['target_session'], cleanup=True)
                    from .core import assert_original_owner_admission
                    assert_original_owner_admission(self.db,job['target_session'])
                    control = self.db.execute('SELECT mode FROM agent_controls WHERE target_session=?', (job['target_session'],)).fetchone()
                    if control and control['mode'] != 'running':
                        raise CoordError('parked creation target has a separate control stop')
                else:
                    assert_agent_running(self.db, job['target_session'])
            if status == 'creating' or (status == 'ready' and expected == 'created') or (status == 'parked' and expected == 'materializing'):
                if job['kind'] == 'reset':
                    self.clean(job['target_session'], job['old_thread_id'])
                elif self.db.execute('SELECT 1 FROM bridge_bindings WHERE session_id=?', (job['target_session'],)).fetchone():
                    raise CoordError('new agent acquired an unexpected binding; creation withheld')
            if (status == 'ready' and expected == 'created') or (status == 'parked' and expected == 'materializing'):
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
            if native_error_code is not None:
                self.db.execute('INSERT INTO parked_native_rejections VALUES(?,?,?)',(job_id,native_error_code,time.time()))
            if native_rpc_code is not None:
                self.db.execute('INSERT INTO parked_native_rpc_errors VALUES(?,?,?,?)', (job_id,native_rpc_code,native_rpc_message,time.time()))
        return self.job(job_id)

    def resolve_creation(self, job_id, thread_id, confirmed):
        with self.coord.tx():
            from .parked_retirement import assert_not_retired
            assert_not_retired(self.db, job_id)
            job = self.job(job_id)
            from .root_orchestrator import assert_management_target
            assert_management_target(self.db, self.coord.require_session(), job['target_session'])
            if self.coord.require_session() not in (job['requester_session'], job['target_session']):
                raise CoordError('only the requester or target can resolve a reset')
            if job['parked']:
                from .management import assert_parked_active
                assert_parked_active(self.db,job_id)
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
