"""Durable main-agent admission controls; reservations are never transferred."""
import json
import hashlib
import time
import uuid
import contextlib

from .core import CoordError
from .session_reset import SessionResetState


class AgentControlState:
    def __init__(self, coord, sessions_root=None):
        self.coord, self.db = coord, coord.db
        from .compaction import counter
        self.compactions = counter(sessions_root)
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS agent_controls (
                target_session TEXT PRIMARY KEY, mode TEXT NOT NULL, updated_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS agent_control_requests (
                id TEXT PRIMARY KEY, requester_session TEXT NOT NULL, target_session TEXT NOT NULL,
                host_id TEXT NOT NULL, thread_id TEXT NOT NULL, action TEXT NOT NULL,
                interrupt INTEGER NOT NULL, status TEXT NOT NULL, turn_id TEXT, error TEXT,
                created_at REAL NOT NULL, updated_at REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS agent_control_observations (
                target_session TEXT PRIMARY KEY, thread_id TEXT NOT NULL, state TEXT NOT NULL,
                turn_id TEXT, queue_count INTEGER NOT NULL, model TEXT, reasoning_effort TEXT,
                cwd TEXT, permissions_json TEXT, checked_at REAL NOT NULL);
        ''')
        self.db.execute('CREATE TABLE IF NOT EXISTS agent_control_workspace_roots (target_session TEXT NOT NULL,position INTEGER NOT NULL,path TEXT NOT NULL,PRIMARY KEY(target_session,position))')
        columns = {row['name'] for row in self.db.execute('PRAGMA table_info(agent_control_requests)')}
        for column, definition in (
            ('requested_model', 'TEXT'), ('requested_effort', 'TEXT'),
            ('baseline_settings_json', 'TEXT'), ('observed_settings_json', 'TEXT'),
            ('previous_admission_mode', 'TEXT'),
            ('allow_cold_resume', 'INTEGER NOT NULL DEFAULT 0'), ('native_response_json', 'TEXT'),
            ('resolution_id', 'TEXT'), ('resolution_actor', 'TEXT'), ('resolved_at', 'REAL'),
            ('resolution_evidence_json', 'TEXT'), ('uncertainty_provenance', 'TEXT'),
            ('model_route', 'TEXT'), ('native_input_job_id', 'TEXT'), ('native_input_evidence', 'TEXT'),
        ):
            if column not in columns:
                self.db.execute(f'ALTER TABLE agent_control_requests ADD COLUMN {column} {definition}')
        # Build the widened terminal-state invariant first.  SQLite DDL may
        # commit around initialization, so never create a no-index interval.
        self.db.execute("""CREATE UNIQUE INDEX IF NOT EXISTS one_agent_control_v2 ON agent_control_requests(target_session)
            WHERE action<>'inspect' AND status NOT IN ('completed','failed','cancelled','resolved_unchanged')""")
        self.db.execute('DROP INDEX IF EXISTS one_agent_control')
        self.db.execute('CREATE UNIQUE INDEX IF NOT EXISTS one_model_resolution_id ON agent_control_requests(resolution_id) WHERE resolution_id IS NOT NULL')

    def job(self, request_id):
        from .root_lifecycle import job as team_job
        bulk = team_job(self.db, request_id)
        if bulk:
            return bulk
        row = self.db.execute('SELECT * FROM agent_control_requests WHERE id=?', (request_id,)).fetchone()
        if not row: raise CoordError('unknown agent control request')
        from .root_lifecycle import stopped
        return self.public_request(dict(row, root_team_barrier_active=stopped(self.db,row['target_session'])))

    @staticmethod
    def public_request(row):
        result = dict(row)
        if result.get('model_route') == 'ordinary_input':
            result['native_input_state'] = 'accepted' if result['status'] == 'completed' else result['status']
            result['observed_defaults_verified'] = False
        return result

    @staticmethod
    def _requested_setting(value, name):
        if (not isinstance(value, str) or not value or value != value.strip() or len(value) > 128
                or any(ord(char) < 32 for char in value)):
            raise CoordError(f'{name} must be a nonempty bounded string')
        return value

    @staticmethod
    def _baseline(value):
        if not isinstance(value, dict):
            raise CoordError('model transition requires complete baseline settings')
        required = ('model', 'modelProvider', 'cwd', 'sandbox', 'reasoningEffort',
                    'approvalPolicy', 'approvalsReviewer', 'activePermissionProfile',
                    'runtimeWorkspaceRoots')
        if any(key not in value for key in required):
            raise CoordError('model transition baseline settings are incomplete')
        try:
            return json.dumps(value, sort_keys=True, separators=(',', ':'))
        except (TypeError, ValueError):
            raise CoordError('model transition baseline settings are invalid')

    @staticmethod
    def _configuration(value, thread):
        if not isinstance(value, dict):
            raise CoordError('model transition settings are invalid')
        runtime = value.get('thread')
        if not isinstance(runtime, dict) or runtime.get('id') != thread:
            raise CoordError('model transition readback is for a different thread')
        return {key:item for key, item in value.items() if key != 'thread'}

    def request(self, name, thread, action, request_id, confirmed, interrupt=False, *, model=None, reasoning_effort=None, allow_cold_resume=False):
        if action not in ('stop', 'resume', 'inspect', 'model') or not isinstance(interrupt, bool):
            raise CoordError('invalid agent control action')
        if action != 'stop' and interrupt: raise CoordError('interrupt applies only to agent-stop')
        if action == 'model':
            model = self._requested_setting(model, 'model')
            reasoning_effort = self._requested_setting(reasoning_effort, 'reasoning_effort')
            if not isinstance(allow_cold_resume, bool): raise CoordError('allow_cold_resume must be boolean')
        elif model is not None or reasoning_effort is not None or allow_cold_resume:
            raise CoordError('model settings apply only to agent-model')
        try: request_id = str(uuid.UUID(request_id))
        except (ValueError, TypeError, AttributeError): raise CoordError('request_id must be a UUID')
        if action != 'inspect' and confirmed is not True:
            raise CoordError('agent lifecycle/model changes require explicit confirmation')
        caller = self.coord.require_session()
        target = SessionResetState(self.coord).target(name)['id']
        from .root_lifecycle import stopped
        if action != 'inspect' and stopped(self.db,target):
            raise CoordError('root stop-all barrier requires explicit matching root resume-all')
        from .root_orchestrator import assert_management_target
        if action == 'inspect':
            assert_management_target(self.db, caller, target, inspect=True)
        if action != 'inspect':
            from .root_orchestrator import assert_local_lifecycle
            assert_local_lifecycle(self.db, target)
            assert_management_target(self.db, caller, target)
        with self.coord.tx():
            existing = self.db.execute('SELECT * FROM agent_control_requests WHERE id=?', (request_id,)).fetchone()
            if existing:
                existing = dict(existing)
                if (existing['requester_session'], existing['target_session'], existing['thread_id'], existing['action'],
                        bool(existing['interrupt']), existing.get('requested_model'), existing.get('requested_effort'), bool(existing.get('allow_cold_resume'))) != (
                            caller, target, thread, action, interrupt, model, reasoning_effort, allow_cold_resume):
                    raise CoordError('agent control retry payload differs from the original')
                return self.public_request(existing)
            self.coord._fresh(self.db, caller)
            binding = self.db.execute('SELECT * FROM bridge_bindings WHERE session_id=?', (target,)).fetchone()
            if not binding or not binding['thread_id'] or binding['thread_id'] != thread:
                raise CoordError('agent control requires the exact current main thread; child routes are not independent agents')
            if action == 'stop' and self.db.execute('SELECT 1 FROM bridge_bindings WHERE parent_session=?', (target,)).fetchone():
                raise CoordError('retire bound children before stopping their parent')
            host = self.db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (target,)).fetchone()
            if not host: raise CoordError('target has no authenticated bridge host')
            active = self.db.execute("SELECT * FROM agent_control_requests WHERE target_session=? AND action<>'inspect' AND status NOT IN ('completed','failed','cancelled','resolved_unchanged')", (target,)).fetchone()
            if active:
                # A graceful wait has not issued an interrupt. Resume can safely
                # cancel that wait without changing the owner's resources.
                if action == 'resume' and active['action'] == 'stop' and active['status'] in ('pending','waiting') and not active['turn_id']:
                    self.db.execute("UPDATE agent_control_requests SET status='cancelled',updated_at=? WHERE id=?", (time.time(), active['id']))
                else: raise CoordError('another agent control is unresolved; inspect agent-status')
            if action == 'model' and self.db.execute("SELECT 1 FROM session_resets WHERE target_session=? AND status NOT IN ('completed','cancelled','failed')", (target,)).fetchone():
                raise CoordError('fresh-session lifecycle operation is unresolved; inspect session-reset-status')
            now = time.time()
            prior_mode = (self.db.execute('SELECT mode FROM agent_controls WHERE target_session=?', (target,)).fetchone() or {'mode':'running'})['mode']
            if action == 'model' and prior_mode not in ('running', 'stopped'):
                raise CoordError('model transition requires a stable running or stopped admission mode')
            ordinary = action == 'model' and not allow_cold_resume
            if ordinary and prior_mode != 'running':
                raise CoordError('ordinary model input requires running admission; explicitly resume first')
            self.db.execute('''INSERT INTO agent_control_requests(
                    id,requester_session,target_session,host_id,thread_id,action,interrupt,status,turn_id,error,
                    created_at,updated_at,requested_model,requested_effort,previous_admission_mode,allow_cold_resume)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                (request_id, caller, target, host['host_id'], thread, action, int(interrupt), 'pending', None, None,
                 now, now, model, reasoning_effort, prior_mode, int(allow_cold_resume)))
            if ordinary:
                self.db.execute("UPDATE agent_control_requests SET model_route='ordinary_input' WHERE id=?", (request_id,))
            if action == 'stop' or (action == 'model' and not ordinary):
                self.db.execute("INSERT INTO agent_controls VALUES(?,'stopping',?) ON CONFLICT(target_session) DO UPDATE SET mode='stopping',updated_at=excluded.updated_at", (target, now))
        return self.job(request_id)


    def ownership(self, target):
        held = [dict(row) for row in self.db.execute('SELECT name AS resource,reservation_id,stale,deadline FROM resources WHERE owner_session=? ORDER BY name', (target,))]
        guards = []
        for table, column, identifier in (('guarded_runs','session','run_id'), ('remote_guarded_runs','session_id','run_id')):
            if self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone():
                guards.extend(dict(row) for row in self.db.execute(f'SELECT {identifier} AS run_id,resource,reservation_id FROM {table} WHERE {column}=? AND closed_at IS NULL', (target,)))
        return dict(reservations=held, open_guards=guards)

    @staticmethod
    def observed_workspaces(db, session, cwd):
        exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='agent_control_workspace_roots'").fetchone()
        members = [row['path'] for row in db.execute('SELECT path FROM agent_control_workspace_roots WHERE target_session=? ORDER BY position', (session,))] if exists else []
        return members or ([cwd] if cwd else [])

    def status(self, name):
        target = SessionResetState(self.coord).target(name)
        binding = self.db.execute('SELECT * FROM bridge_bindings WHERE session_id=?', (target['id'],)).fetchone()
        mode = self.db.execute('SELECT mode FROM agent_controls WHERE target_session=?', (target['id'],)).fetchone()
        observation = self.db.execute('SELECT * FROM agent_control_observations WHERE target_session=?', (target['id'],)).fetchone()
        # Recorded host registration is not evidence of a live native binding.
        host = (self.db.execute('SELECT host_id,registered_at,last_seen_at FROM remote_session_hosts WHERE session_id=?',
                               (target['id'],)).fetchone()
                if self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='remote_session_hosts'").fetchone() else None)
        thread = binding['thread_id'] if binding else None
        requests = [self.public_request(row) for row in self.db.execute('SELECT * FROM agent_control_requests WHERE target_session=? ORDER BY created_at DESC LIMIT 10', (target['id'],))]
        wakes = [dict(row) for row in self.db.execute("SELECT id,status,queue_id,error FROM bridge_jobs WHERE thread_id=? AND status NOT IN ('dispatched','cancelled') ORDER BY created_at", (thread,))]
        from .management import manager
        return dict(target, host_id=host['host_id'] if host else None, host_record=dict(host) if host else None,
                    manager_session=manager(self.db,target['id']), thread_id=thread, compaction_count=self.compactions.count(thread), parent_session=binding['parent_session'] if binding else None,
                    admission=mode['mode'] if mode else 'running', observation=dict(observation, workspaces=self.observed_workspaces(self.db, target['id'], observation['cwd'])) if observation else None,
                    requests=requests, wakes=wakes, ownership=self.ownership(target['id']))

    def roster(self):
        from .management import manager
        return [dict(manager_session=manager(self.db,row['id']), id=row['id'], agent=row['agent'], thread_id=row['thread_id'], compaction_count=self.compactions.count(row['thread_id']), parent_session=row['parent_session'],
                     admission=row['mode'] or 'running', state=row['state'], checked_at=row['checked_at'])
                for row in self.db.execute('''SELECT s.id,s.agent,b.thread_id,b.parent_session,c.mode,o.state,o.checked_at
                    FROM sessions s LEFT JOIN bridge_bindings b ON b.session_id=s.id
                    LEFT JOIN agent_controls c ON c.target_session=s.id
                    LEFT JOIN agent_control_observations o ON o.target_session=s.id ORDER BY s.agent''')]

    def jobs(self, host):
        from .root_lifecycle import jobs as team_jobs, stopped
        priority = [dict(row, ownership=self.ownership(row['target_session'])) for row in team_jobs(self.db, host)]
        return priority + [dict(row, ownership=self.ownership(row['target_session'])) for row in self.db.execute(
            "SELECT * FROM agent_control_requests WHERE host_id=? AND status NOT IN ('completed','failed','cancelled','resolved_unchanged') ORDER BY created_at", (host,))
            if not stopped(self.db, row['target_session'])]

    def update(self, request_id, expected, status, *, turn_id=None, error=None, observation=None,
               baseline_settings=None, observed_settings=None, direct_input=None, native_response=None, resolution_evidence=None,
               input_job_id=None, input_evidence=None):
        from .root_lifecycle import job as team_job, update as team_update
        if team_job(self.db, request_id):
            return team_update(self.coord, request_id, expected, status, turn_id=turn_id, error=error, observation=observation)
        if status not in ('waiting','interrupting','changing','submitting','uncertain','completed','failed','resolved_unchanged'):
            raise CoordError('invalid agent control status')
        with contextlib.nullcontext() if self.db.in_transaction else self.coord.tx():
            job = self.job(request_id)
            if job['status'] != expected: raise CoordError('agent control changed; inspect its current request')
            if job.get('root_team_barrier_active') and ((job['action']=='model' and expected=='pending' and status=='changing') or job['action']=='resume'):
                raise CoordError('root stop-all barrier withheld pending native lifecycle action')
            from .root_orchestrator import assert_local_lifecycle, assert_management_target
            if job['action'] == 'inspect':
                assert_management_target(self.db, job['requester_session'], job['target_session'], inspect=True)
            else:
                assert_local_lifecycle(self.db, job['target_session'])
                assert_management_target(self.db, job['requester_session'], job['target_session'])
            binding = self.db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?', (job['target_session'],)).fetchone()
            if not binding or binding['thread_id'] != job['thread_id']:
                raise CoordError('agent thread changed; control withheld')
            host = self.db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (job['target_session'],)).fetchone()
            if not host or host['host_id'] != job['host_id']:
                raise CoordError('agent host changed; control withheld')
            if job['status'] == 'interrupting' and status == 'interrupting' and turn_id != job['turn_id']:
                raise CoordError('cannot replace an unresolved interrupt turn')
            if status == 'interrupting' and (job['action'] != 'stop' or not job['interrupt'] or not turn_id):
                raise CoordError('interrupt requires an explicit stop and exact turn')
            ordinary = job['action'] == 'model' and job.get('model_route') == 'ordinary_input'
            if ordinary:
                transitions = {'pending': {'waiting', 'submitting', 'failed'},
                               'waiting': {'waiting', 'submitting', 'failed'},
                               'submitting': {'completed', 'uncertain', 'failed'},
                               'uncertain': {'uncertain', 'completed'}}
                if status not in transitions.get(expected, set()):
                    raise CoordError('invalid ordinary model-input transition')
                if any(value is not None for value in (baseline_settings, observed_settings, resolution_evidence)):
                    raise CoordError('ordinary turn acceptance is not exact settings readback')
                if input_job_id is not None and input_job_id != job.get('native_input_job_id'):
                    raise CoordError('ordinary model input differs from the immutable associated job')
                if status in ('submitting', 'completed') and (input_job_id is None or not job.get('native_input_job_id')):
                    raise CoordError('ordinary model outcome requires its original genuine input UUID')
                if status == 'submitting':
                    if job.get('root_team_barrier_active'):
                        raise CoordError('root stop-all barrier withheld ordinary model input')
                    wake = self.db.execute('SELECT * FROM bridge_jobs WHERE id=?', (job.get('native_input_job_id'),)).fetchone()
                    mode = (self.db.execute('SELECT mode FROM agent_controls WHERE target_session=?', (job['target_session'],)).fetchone() or {'mode':'running'})['mode']
                    if (not wake or wake['thread_id'] != job['thread_id'] or wake['status'] != 'starting'
                            or mode != 'running' or direct_input is not True):
                        raise CoordError('ordinary model submission requires the exact admitted genuine input')
                    if self.ownership(job['target_session'])['open_guards']:
                        raise CoordError('ordinary model input withheld while guarded work is open')
                if status == 'completed':
                    if input_evidence == 'rpc_ack':
                        turn = native_response.get('turn') if isinstance(native_response, dict) else None
                        if (not isinstance(turn, dict) or not isinstance(turn.get('id'), str) or not turn['id']
                                or turn_id != turn['id'] or not isinstance(turn.get('status'), str)
                                or native_response.get('threadId', job['thread_id']) != job['thread_id']):
                            raise CoordError('ordinary model acceptance requires an actual matching turn response')
                    elif input_evidence != 'history_client_id' or native_response is not None:
                        raise CoordError('ordinary model acceptance requires positive original input evidence')
                elif native_response is not None or input_evidence is not None:
                    raise CoordError('ordinary model response evidence is only accepted at its outcome')
            elif job['action'] == 'model':
                transitions = {
                    'pending': {'changing', 'failed'}, 'changing': {'changing', 'uncertain', 'completed', 'failed'},
                    'uncertain': {'uncertain', 'completed', 'failed'},
                }
                if status == 'resolved_unchanged':
                    if expected not in ('changing','uncertain') or not job.get('resolution_id'):
                        raise CoordError('model abandonment requires a recorded unknown-outcome resolution')
                    if observed_settings is None or not isinstance(observation, dict) or observation.get('state') != 'idle' or observation.get('queue_count') != 0 or observation.get('turn_id') is not None or direct_input is not True:
                        raise CoordError('model abandonment requires fresh idle direct-input baseline evidence')
                    if self.ownership(job['target_session'])['open_guards']:
                        raise CoordError('model abandonment requires closed guarded runs')
                    if self._configuration(observed_settings, job['thread_id']) != self._configuration(json.loads(job['baseline_settings_json']), job['thread_id']):
                        raise CoordError('model abandonment settings do not match the captured baseline')
                elif status not in transitions.get(expected, set()):
                    raise CoordError('invalid model transition state')
                if status == 'changing' and expected == 'pending':
                    if baseline_settings is None or job.get('baseline_settings_json'):
                        raise CoordError('model mutation boundary requires one baseline capture')
                    if (not isinstance(observation, dict) or observation.get('state') != 'idle'
                            or observation.get('queue_count') != 0 or observation.get('turn_id') is not None
                            or direct_input is not True):
                        raise CoordError('model mutation requires fresh idle, direct-input, empty-queue observation')
                    own = self.ownership(job['target_session'])
                    if own['open_guards']:
                        raise CoordError('model mutation requires no open guarded runs')
                    self._configuration(baseline_settings, job['thread_id'])
                    baseline_settings = self._baseline(baseline_settings)
                if native_response is not None:
                    if status != 'changing' or expected != 'changing' or job.get('native_response_json'):
                        raise CoordError('native model response may be captured exactly once at the changing boundary')
                    self._configuration(native_response, job['thread_id'])
                if baseline_settings is not None and not (status == 'changing' and expected == 'pending'):
                    raise CoordError('model baseline is only accepted at the mutation boundary')
                if observed_settings is not None:
                    if status not in ('uncertain', 'completed', 'failed', 'resolved_unchanged'):
                        raise CoordError('model observed settings require a readback outcome')
                    observed_settings = self._baseline(observed_settings)
                if status == 'failed' and expected in ('changing', 'uncertain'):
                    if observed_settings is None or not job.get('baseline_settings_json'):
                        raise CoordError('post-boundary model failure requires an exact original-settings readback')
                    if expected == 'uncertain' and not job.get('native_response_json'):
                        raise CoordError('unknown model outcomes cannot be failed without an acknowledged native response')
                    original_settings = self._configuration(json.loads(job['baseline_settings_json']), job['thread_id'])
                    if self._configuration(json.loads(observed_settings), job['thread_id']) != original_settings:
                        raise CoordError('post-boundary model failure settings do not match the captured baseline')
                if status == 'completed':
                    if observed_settings is None or not job.get('baseline_settings_json'):
                        raise CoordError('model completion requires an exact observed settings readback')
                    expected_settings = json.loads(job['baseline_settings_json'])
                    expected_settings = self._configuration(expected_settings, job['thread_id'])
                    expected_settings['model'] = job['requested_model']
                    expected_settings['reasoningEffort'] = job['requested_effort']
                    if not isinstance(expected_settings.get('collaborationMode'), dict) or not isinstance(expected_settings['collaborationMode'].get('settings'), dict):
                        raise CoordError('model completion baseline lacks collaboration mode settings')
                    expected_settings['collaborationMode']['settings']['model'] = job['requested_model']
                    expected_settings['collaborationMode']['settings']['reasoning_effort'] = job['requested_effort']
                    if self._configuration(json.loads(observed_settings), job['thread_id']) != expected_settings:
                        raise CoordError('model completion settings do not match the requested target and preserved baseline')
            elif status == 'submitting' or input_job_id is not None or input_evidence is not None:
                raise CoordError('ordinary input state does not apply to this action')
            elif baseline_settings is not None:
                raise CoordError('model baseline does not apply to this action')
            elif observed_settings is not None:
                raise CoordError('model observed settings do not apply to this action')
            if observation:
                self.db.execute('INSERT OR REPLACE INTO agent_control_observations VALUES(?,?,?,?,?,?,?,?,?,?)',
                    (job['target_session'], job['thread_id'], observation['state'], observation.get('turn_id'), observation['queue_count'],
                     observation.get('model'), observation.get('reasoning_effort'), observation.get('cwd'),
                     json.dumps(observation.get('permissions'), sort_keys=True), time.time()))
                members = (observation.get('permissions') or {}).get('runtimeWorkspaceRoots')
                self.db.execute('DELETE FROM agent_control_workspace_roots WHERE target_session=?', (job['target_session'],))
                if isinstance(members, list) and all(isinstance(member, str) for member in members):
                    self.db.executemany('INSERT INTO agent_control_workspace_roots VALUES(?,?,?)', [(job['target_session'], index, member) for index, member in enumerate(members)])
            if status == 'completed' and job['action'] == 'stop':
                if not observation or observation['state'] != 'idle' or observation['queue_count']:
                    raise CoordError('stop completion requires observed idle and an empty app-server queue')
                own = self.ownership(job['target_session'])
                if not job['interrupt'] and (own['reservations'] or own['open_guards']):
                    raise CoordError('graceful stop waits for actual owner cleanup')
                self.db.execute("UPDATE agent_controls SET mode='stopped',updated_at=? WHERE target_session=?", (time.time(), job['target_session']))
            if status == 'completed' and job['action'] == 'resume':
                self.db.execute("INSERT INTO agent_controls VALUES(?,'running',?) ON CONFLICT(target_session) DO UPDATE SET mode='running',updated_at=excluded.updated_at", (job['target_session'], time.time()))
            if job['action'] == 'model' and not ordinary and status in ('completed', 'failed', 'resolved_unchanged'):
                prior = job.get('previous_admission_mode')
                if prior not in ('running', 'stopped'):
                    raise CoordError('model transition has no stable prior admission mode')
                self.db.execute("INSERT INTO agent_controls VALUES(?,?,?) ON CONFLICT(target_session) DO UPDATE SET mode=excluded.mode,updated_at=excluded.updated_at", (job['target_session'], prior, time.time()))
            self.db.execute('''UPDATE agent_control_requests
                SET status=?,turn_id=COALESCE(?,turn_id),error=?,baseline_settings_json=COALESCE(?,baseline_settings_json),
                    observed_settings_json=COALESCE(?,observed_settings_json),native_response_json=COALESCE(?,native_response_json),resolution_evidence_json=COALESCE(?,resolution_evidence_json),native_input_evidence=COALESCE(?,native_input_evidence),resolved_at=CASE WHEN ?='resolved_unchanged' THEN ? ELSE resolved_at END,updated_at=? WHERE id=?''',
                (status, turn_id, error, baseline_settings, observed_settings,
                 json.dumps(native_response, sort_keys=True) if native_response is not None else None,
                 json.dumps(resolution_evidence, sort_keys=True) if resolution_evidence is not None else None,
                 input_evidence,
                 status, time.time(), time.time(), request_id))
        return self.job(request_id)

    def resolve_unchanged(self, request_id, expected_thread, resolution_id, confirmed):
        if confirmed is not True: raise CoordError('model resolution requires explicit confirmation')
        try: resolution_id = str(uuid.UUID(resolution_id))
        except (TypeError, ValueError, AttributeError): raise CoordError('resolution_id must be a UUID')
        caller = self.coord.require_session()
        with self.coord.tx():
            job = self.job(request_id)
            if job['action'] != 'model' or job['thread_id'] != expected_thread: raise CoordError('resolution requires the exact model request and thread')
            from .root_orchestrator import assert_local_lifecycle, assert_management_target
            assert_local_lifecycle(self.db, job['target_session'])
            assert_management_target(self.db, caller, job['target_session'])
            binding = self.db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?', (job['target_session'],)).fetchone()
            host = self.db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (job['target_session'],)).fetchone()
            if not binding or binding['thread_id'] != job['thread_id'] or not host or host['host_id'] != job['host_id']:
                raise CoordError('model resolution target route or host changed')
            other = self.db.execute('SELECT id FROM agent_control_requests WHERE resolution_id=?', (resolution_id,)).fetchone()
            if other and other['id'] != request_id: raise CoordError('resolution_id is already bound to another request')
            if job['requester_session'] != caller: raise CoordError('only the original requester can abandon an unknown model outcome')
            if job.get('resolution_id'):
                if job['resolution_id'] != resolution_id: raise CoordError('resolution id differs from original')
                return job
            if job['status'] != 'uncertain': raise CoordError('model abandonment requires a stable uncertain outcome')
            if not job.get('baseline_settings_json') or job.get('previous_admission_mode') not in ('running','stopped'):
                raise CoordError('model resolution requires a captured baseline and prior admission')
            self.coord._fresh(self.db, caller)
            # The host worker provides fresh native proof through update-like guards; this coordinator method records intent only.
            self.db.execute('''UPDATE agent_control_requests SET resolution_id=?,resolution_actor=?,uncertainty_provenance=COALESCE(uncertainty_provenance,error),updated_at=? WHERE id=?''',
                            (resolution_id, caller, time.time(), request_id))
        return self.job(request_id)


class AdminRecoveryState:
    """Audited, exact-reservation recovery after native work is proven closed."""

    def __init__(self, coord):
        self.coord, self.db = coord, coord.db
        self.db.execute('''CREATE TABLE IF NOT EXISTS admin_recovery_attempts (
            request_id TEXT PRIMARY KEY, project TEXT NOT NULL, resource TEXT NOT NULL,
            reservation_id TEXT NOT NULL, owner_session TEXT NOT NULL,
            requester_session TEXT NOT NULL, requester_host TEXT NOT NULL,
            operator TEXT NOT NULL, reason TEXT NOT NULL, evidence_json TEXT NOT NULL,
            outcome TEXT NOT NULL CHECK(outcome IN ('cleared','qualified_released','retained','blocked')),
            checks_json TEXT NOT NULL, recorded_at REAL NOT NULL)''')
        self.db.execute('''CREATE TABLE IF NOT EXISTS admin_qualified_dispositions (
            request_id TEXT PRIMARY KEY, resource TEXT NOT NULL, reservation_id TEXT NOT NULL,
            owner_session TEXT NOT NULL, original_run_id TEXT NOT NULL, original_refusal TEXT NOT NULL,
            evidence_json TEXT NOT NULL, historical_uncertainty_json TEXT NOT NULL, recorded_at REAL NOT NULL)''')
        self.db.execute('''CREATE TABLE IF NOT EXISTS admin_qualified_evidence (
            request_id TEXT NOT NULL, role TEXT NOT NULL CHECK(role IN ('current_closure','reviewer_closure')),
            message_id TEXT NOT NULL, attachment_id TEXT NOT NULL, sender_session TEXT NOT NULL,
            sha256 TEXT NOT NULL, PRIMARY KEY(request_id,role))''')

    def _authorize(self, db, caller, owner):
        from .root_orchestrator import projection
        if projection(db, caller):
            required_root = caller
        else:
            if not db.execute('SELECT 1 FROM project_orchestrators WHERE session_id=?', (caller,)).fetchone():
                raise CoordError('admin recovery requires the authenticated global root or project orchestrator')
            required_root = None
        current, seen = owner, set()
        while True:
            if current in seen:
                raise CoordError('admin recovery parent chain contains a cycle')
            seen.add(current)
            recorded = db.execute('SELECT root_session FROM project_orchestrators WHERE session_id=?', (current,)).fetchone()
            if required_root is not None and recorded and recorded['root_session'] == required_root:
                return
            if required_root is None and current == caller:
                if len(seen) == 1:
                    raise CoordError('admin recovery cannot target the caller own reservation')
                return
            binding = db.execute('SELECT parent_session FROM bridge_bindings WHERE session_id=?', (current,)).fetchone()
            if not binding or not binding['parent_session']:
                if required_root is not None:
                    raise CoordError('admin recovery target is outside a recorded project orchestrator chain')
                raise CoordError('admin recovery target is outside the project orchestrator parent chain')
            current = binding['parent_session']

    def recover(self, *, project, canonical_project, resource, reservation_id, owner_session,
                request_id, operator, reason, evidence, host_id):
        if not isinstance(canonical_project, str) or not canonical_project or project != canonical_project:
            raise CoordError('admin recovery requires the exact server-authenticated project')
        for label, value in (('reservation_id', reservation_id), ('owner_session', owner_session),
                             ('operator', operator), ('reason', reason)):
            if not isinstance(value, str) or not value.strip() or value != value.strip() or len(value) > 2048:
                raise CoordError(label + ' must be a nonempty bounded string')
        try: request_id = str(uuid.UUID(request_id))
        except (ValueError, TypeError, AttributeError): raise CoordError('request_id must be a UUID')
        if not isinstance(evidence, dict) or not evidence:
            raise CoordError('admin recovery requires structured operator evidence')
        try: evidence_json = json.dumps(evidence, sort_keys=True, separators=(',', ':'), allow_nan=False)
        except (TypeError, ValueError): raise CoordError('admin recovery evidence is invalid')
        if len(evidence_json) > 32768:
            raise CoordError('admin recovery evidence is too large')
        resource = self.coord.resource_name(resource)
        caller = self.coord.require_session()
        with self.coord.tx() as db:
            self._authorize(db, caller, owner_session)
            self.coord._fresh(db, caller)
            exact = (project, resource, reservation_id, owner_session, caller, host_id,
                     operator, reason, evidence_json)
            previous = db.execute('SELECT * FROM admin_recovery_attempts WHERE request_id=?', (request_id,)).fetchone()
            if previous:
                if tuple(previous[key] for key in ('project','resource','reservation_id','owner_session',
                        'requester_session','requester_host','operator','reason','evidence_json')) != exact:
                    raise CoordError('admin recovery retry payload differs from the original')
                return {'request_id': request_id, 'project': project, 'resource': resource,
                        'reservation_id': reservation_id, 'owner_session': owner_session,
                        'outcome': previous['outcome'], 'checks': json.loads(previous['checks_json'])}
            checks = self._checks(db, resource, reservation_id, owner_session, evidence)
            outcome = ('blocked' if checks['blockers'] else 'retained' if checks['retained']
                       else 'qualified_released' if checks['qualified_release'] else 'cleared')
            now = time.time()
            if outcome in ('cleared', 'qualified_released'):
                if outcome == 'qualified_released':
                    qualified = checks['qualified_release']
                    db.execute('INSERT INTO admin_qualified_dispositions VALUES(?,?,?,?,?,?,?,?,?)',
                               (request_id, resource, reservation_id, owner_session,
                                qualified['original_run_id'], qualified['original_refusal'], evidence_json,
                                json.dumps(qualified['historical_uncertainty'], sort_keys=True), now))
                    for role in ('current_closure', 'reviewer_closure'):
                        pin = qualified[role]
                        db.execute('INSERT INTO admin_qualified_evidence VALUES(?,?,?,?,?,?)',
                                   (request_id, role, pin['message_id'], pin['attachment_id'],
                                    pin['sender_session'], pin['sha256']))
                else:
                    receipt = {'version': 1, 'action': 'admin-recover', 'project': project,
                               'resource': resource, 'reservation_id': reservation_id,
                               'owner_session': owner_session, 'operator': operator,
                               'reason': reason, 'evidence': evidence, 'closed_at': now}
                    canonical = json.dumps(receipt, sort_keys=True, separators=(',', ':'))
                    digest = hashlib.sha256(canonical.encode()).hexdigest()
                    evidence_digest = hashlib.sha256(evidence_json.encode()).hexdigest()
                    db.execute('INSERT INTO receipts VALUES(?,?,?,?,?,?)',
                               (reservation_id, digest, evidence_digest, canonical, 'recover', now))
                    prior = db.execute('SELECT state,resource,owner_session FROM remote_reservation_outcomes WHERE reservation_id=?',
                                       (reservation_id,)).fetchone()
                    if prior:
                        if (prior['state'], prior['resource'], prior['owner_session']) != ('pending', resource, owner_session):
                            raise CoordError('remote reservation outcome is inconsistent')
                        db.execute('''UPDATE remote_reservation_outcomes SET state='cleared',action='recover',
                            receipt_sha256=?,evidence_sha256=?,cleared_at=? WHERE reservation_id=?''',
                                   (digest, evidence_digest, now, reservation_id))
                db.execute('''UPDATE resources SET owner_session=NULL,reservation_id=NULL,token=NULL,
                    granted_at=NULL,deadline=NULL,stale=0,reason=NULL
                    WHERE name=? AND reservation_id=? AND owner_session=?''',
                           (resource, reservation_id, owner_session))
            db.execute('INSERT INTO admin_recovery_attempts VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
                       (request_id, *exact, outcome, json.dumps(checks, sort_keys=True), now))
        return {'request_id': request_id, 'project': project, 'resource': resource,
                'reservation_id': reservation_id, 'owner_session': owner_session,
                'outcome': outcome, 'checks': checks}

    def _qualified_release(self, db, caller, evidence, row, reservation_id, owner, local, remote):
        """Validate an explicitly supplied current-instance closure proof.

        This exception records a qualified current disposition, never a normal
        CLOSED receipt. It is bound to the exact stale reservation/run/refusal
        and requires two independently pinned current-closure evidence objects.
        """
        from .root_orchestrator import projection
        candidate = evidence.get('qualified_current_disposition')
        if candidate is None:
            return None
        if not isinstance(candidate, dict):
            raise CoordError('qualified current disposition evidence is invalid')
        required = ('original_reservation_id', 'original_run_id', 'original_refusal',
                    'current_closure', 'reviewer_closure', 'historical_uncertainty')
        if set(candidate) != set(required):
            raise CoordError('qualified current disposition evidence is incomplete')
        if candidate['original_reservation_id'] != reservation_id:
            raise CoordError('qualified current disposition reservation mismatch')
        if not isinstance(candidate['original_refusal'], str) or candidate['original_refusal'] != row['reason'] or not candidate['original_refusal'].startswith('OPEN'):
            raise CoordError('qualified current disposition must preserve the original OPEN refusal')
        def bounded(value, label):
            if not isinstance(value, str) or not value or value != value.strip() or len(value) > 512:
                raise CoordError('qualified current disposition ' + label + ' is invalid')
            return value

        def packet(content, label):
            def no_duplicates(pairs):
                result = {}
                for key, value in pairs:
                    if key in result: raise ValueError('duplicate key')
                    result[key] = value
                return result
            try:
                text = bytes(content).decode('utf-8')
                value = json.loads(text, object_pairs_hook=no_duplicates)
            except (UnicodeDecodeError, ValueError, TypeError):
                raise CoordError('qualified current disposition ' + label + ' packet is invalid')
            if json.dumps(value, sort_keys=True, separators=(',', ':')) != text:
                raise CoordError('qualified current disposition ' + label + ' packet is noncanonical')
            return value

        pins = {}
        for label in ('current_closure', 'reviewer_closure'):
            pin = dict(candidate[label]) if isinstance(candidate[label], dict) else candidate[label]
            if (not isinstance(pin, dict) or set(pin) != {'attachment_id', 'sha256'}
                    or not isinstance(pin['attachment_id'], str) or not pin['attachment_id']
                    or not isinstance(pin['sha256'], str) or len(pin['sha256']) != 64
                    or any(char not in '0123456789abcdef' for char in pin['sha256'])):
                raise CoordError('qualified current disposition evidence pin is invalid')
            evidence_row = db.execute('''SELECT a.message_id,a.content,m.sender_session,m.recipient_session
                FROM attachments a JOIN messages m ON m.id=a.message_id WHERE a.id=?''',
                                      (pin['attachment_id'],)).fetchone()
            if not evidence_row or hashlib.sha256(bytes(evidence_row['content'])).hexdigest() != pin['sha256']:
                raise CoordError('qualified current disposition evidence record mismatch')
            if evidence_row['recipient_session'] != caller:
                raise CoordError('qualified current disposition evidence must be delivered to the authorizing caller')
            pin.update(message_id=evidence_row['message_id'], sender_session=evidence_row['sender_session'],
                       packet=packet(evidence_row['content'], label))
            pins[label] = pin
        if pins['current_closure']['sender_session'] != caller:
            raise CoordError('qualified current disposition current closure is not caller-authenticated')
        reviewer = pins['reviewer_closure']['sender_session']
        if reviewer == caller or not (projection(db, reviewer) or db.execute(
                'SELECT 1 FROM project_orchestrators WHERE session_id=?', (reviewer,)).fetchone()):
            raise CoordError('qualified current disposition reviewer closure is not independently authenticated')
        def root_for(session):
            if projection(db, session):
                return session
            membership = db.execute('SELECT root_session FROM project_orchestrators WHERE session_id=?', (session,)).fetchone()
            return membership['root_session'] if membership else None
        if not root_for(caller) or root_for(reviewer) != root_for(caller) or reviewer == owner:
            raise CoordError('qualified current disposition reviewer must belong to the same root')
        self._authorize(db, reviewer, owner)
        if not isinstance(candidate['original_run_id'], str) or not candidate['original_run_id']:
            raise CoordError('qualified current disposition run id is invalid')
        current = next((run for run in remote if run['run_id'] == candidate['original_run_id']), None)
        if (not current or current['resource'] != row['name'] or current['session_id'] != owner
                or current['closed_at'] is not None or current['local_pid'] <= 0):
            raise CoordError('qualified current disposition original run mismatch')
        original = {'resource': row['name'], 'reservation_id': reservation_id, 'owner_session': owner,
                    'remote_run_id': candidate['original_run_id'], 'refusal': candidate['original_refusal']}
        closure = pins['current_closure']['packet']; review = pins['reviewer_closure']['packet']
        if set(closure) != {'version', 'kind', 'original', 'current', 'known_instances'} or closure.get('version') is not 1 or closure.get('kind') != 'qualified-current-closure' or closure.get('original') != original:
            raise CoordError('qualified current disposition closure packet mismatch')
        current = closure.get('current')
        if not isinstance(current, dict) or set(current) != {'instance_id', 'closure_fact_id', 'host_id', 'closure_authority', 'closure_digest'}:
            raise CoordError('qualified current disposition current packet mismatch')
        instance_id = bounded(current.get('instance_id'), 'instance')
        host_id = bounded(current.get('host_id'), 'host')
        authority = bounded(current.get('closure_authority'), 'authority')
        digest = current.get('closure_digest')
        instances = closure.get('known_instances')
        from .lifecycle_producer import schema as producer_schema
        producer_schema(db)
        fact = db.execute('''SELECT i.*,c.id AS closure_fact_id,c.evidence_sha256
            FROM execution_instances i JOIN current_instance_closures c ON c.instance_id=i.id
            WHERE i.id=? AND c.id=?''', (instance_id, current['closure_fact_id'])).fetchone()
        if (not isinstance(digest, str) or len(digest) != 64 or any(c not in '0123456789abcdef' for c in digest)
                or not isinstance(instances, list) or not instances or len(instances) != len(set(instances))
                or any(not isinstance(item, str) or not item or len(item) > 512 for item in instances)
                or instances.count(instance_id) != 1 or instance_id == candidate['original_run_id']
                or not fact or fact['run_id'] != candidate['original_run_id']
                or fact['resource'] != row['name'] or fact['reservation_id'] != reservation_id
                or fact['owner_session'] != owner or fact['host_id'] != host_id
                or fact['producer_id'] != authority or fact['evidence_sha256'] != digest
                or db.execute('SELECT count(*) FROM remote_reservation_outcomes WHERE reservation_id=? AND resource=? AND owner_session=? AND state=?',
                              (reservation_id, row['name'], owner, 'pending')).fetchone()[0] != 1):
            raise CoordError('qualified current disposition current binding mismatch')
        if (set(review) != {'version', 'kind', 'closure_attachment_id', 'closure_packet_sha256', 'original'}
                or review.get('version') is not 1 or review.get('kind') != 'qualified-current-review'
                or review.get('closure_attachment_id') != pins['current_closure']['attachment_id']
                or review.get('closure_packet_sha256') != pins['current_closure']['sha256']
                or review.get('original') != original):
            raise CoordError('qualified current disposition reviewer packet mismatch')
        if not isinstance(candidate['historical_uncertainty'], list) or not candidate['historical_uncertainty'] or not all(isinstance(item, str) and item for item in candidate['historical_uncertainty']):
            raise CoordError('qualified current disposition historical uncertainty is invalid')
        historical = [run['pid'] for run in local if run['pid'] > 0]
        historical.extend(run['local_pid'] for run in remote if run['local_pid'] > 0)
        return {'kind': 'qualified-current-disposition', 'original_run_id': candidate['original_run_id'],
                'original_refusal': candidate['original_refusal'],
                'current_closure': pins['current_closure'],
                'reviewer_closure': pins['reviewer_closure'],
                'historical_uncertainty': candidate['historical_uncertainty'],
                'historical_pids': historical}

    def _checks(self, db, resource, reservation_id, owner, evidence):
        blockers, retained = [], []
        row = self.coord._row(db, resource)
        exact = bool(row and row['reservation_id'] == reservation_id and row['owner_session'] == owner)
        if not exact:
            historical = db.execute('SELECT 1 FROM receipts WHERE reservation_id=?', (reservation_id,)).fetchone()
            return {'retained': True, 'retained_reasons': [
                'reservation was already cleared' if historical else 'exact reservation is absent or changed'],
                    'blockers': [], 'open_guards': [], 'historical_pids': []}
        if self.coord._state(row) != 'stale': blockers.append('reservation is active')
        if db.execute('SELECT 1 FROM receipts WHERE reservation_id=?', (reservation_id,)).fetchone():
            retained.append('reservation already has a receipt')
        local = [dict(r) for r in db.execute('SELECT run_id,resource,session,pid,pgid,started_at,closed_at FROM guarded_runs WHERE reservation_id=?', (reservation_id,))]
        remote = [dict(r) for r in db.execute('SELECT run_id,resource,session_id,local_pid,started_at,closed_at,close_evidence_sha256 FROM remote_guarded_runs WHERE reservation_id=?', (reservation_id,))]
        qualified = self._qualified_release(db, self.coord.require_session(), evidence, row, reservation_id, owner, local, remote)
        exempt = qualified['original_run_id'] if qualified else None
        if any(r['closed_at'] is None and r['run_id'] != exempt for r in local + remote): blockers.append('guarded run remains open')
        if any(r['pid'] > 0 or r['pgid'] for r in local):
            retained.append('local process closure is not independently proven')
        if any(r['local_pid'] <= 0 for r in remote):
            retained.append('native launch outcome is unresolved')
        if any(r['run_id'] != exempt and r['local_pid'] > 0 and (not r['close_evidence_sha256'] or
                r['closed_at'] is None or r['closed_at'] < r['started_at']) for r in remote):
            retained.append('historical remote PID closure proof is missing')
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='remote_reservation_outcomes'").fetchone() and db.execute(
                "SELECT 1 FROM remote_reservation_outcomes WHERE owner_session=? AND reservation_id<>? AND state='pending'",
                (owner, reservation_id)).fetchone():
            retained.append('unrelated remote reservation outcome remains pending')
        tables = {r['name'] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if 'agent_control_observations' in tables:
            active = db.execute("SELECT 1 FROM agent_control_observations WHERE target_session=? AND state<>'idle' AND checked_at>?",
                                (owner, time.time()-120)).fetchone()
            if active: blockers.append('target has a recent active observation')
        if 'agent_control_requests' in tables and db.execute("SELECT 1 FROM agent_control_requests WHERE target_session=? AND status NOT IN ('completed','failed','cancelled','resolved_unchanged')", (owner,)).fetchone():
            retained.append('agent control request is pending or uncertain')
        if 'session_resets' in tables and db.execute("SELECT 1 FROM session_resets WHERE target_session=? AND status NOT IN ('completed','failed','cancelled')", (owner,)).fetchone():
            retained.append('fresh-session request is pending or uncertain')
        if 'bridge_bindings' in tables and 'bridge_jobs' in tables:
            binding = db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?', (owner,)).fetchone()
            if binding and db.execute("SELECT 1 FROM bridge_jobs WHERE thread_id=? AND status NOT IN ('dispatched','cancelled')", (binding['thread_id'],)).fetchone():
                retained.append('native input request is pending or uncertain')
        return {'retained': bool(retained), 'retained_reasons': retained, 'blockers': blockers, 'open_guards':
                [r['run_id'] for r in local + remote if r['closed_at'] is None],
                'historical_pids': [r['local_pid'] for r in remote if r['local_pid'] > 0],
                'qualified_release': qualified}
