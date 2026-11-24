"""One durable dispatcher per client host and project."""
import contextlib
import hashlib
import math
import secrets
import threading
import time

from .core import Coordinator, CoordError
from .bridge_state import BridgeState
from .approvals import ApprovalInbox


class BridgeManager:
    def __init__(self, db_path, usage_store=None):
        self.db_path = db_path
        self.usage_store = usage_store
        self.measurements = None
        self.roots = None
        self.project_id = 'default'
        self.mutex = threading.RLock()
        self.lock = None
        self.approvals = ApprovalInbox()
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
            from .goal_selection_generation import install as install_goal_generation, PROJECT as GOAL_PROJECT
            install_goal_generation(c.db, GOAL_PROJECT)
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
            for host in list(self.approvals.epochs):
                self.approvals.invalidate(host)
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
            self.approvals.invalidate(host)
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
            self.approvals.invalidate(host)
            c.db.execute('DELETE FROM bridge_dispatcher WHERE host_id=?', (host,))
            c.db.execute('UPDATE bridge_client_runtime SET pid=0,error=? WHERE host_id=?', ('bridge stopped', host))
            if not c.db.execute('SELECT 1 FROM bridge_dispatcher').fetchone():
                self.close()
            return {'released': True}
        if op.startswith('approval-'):
            fields = {'approval-sync': {'epoch'}, 'approval-route': {'thread'},
                      'approval-publish': {'epoch','key','method','context','preview','choices','remaining'},
                      'approval-poll': {'epoch','handles'}, 'approval-finish': {'epoch','handle','outcome'}}
            valid_fields = op in fields and (set(params) == fields[op] or
                (op == 'approval-finish' and set(params) == fields[op] | {'item_outcome'}) or
                (op == 'approval-publish' and set(params) == fields[op] | {'requested_permissions'}))
            if not valid_fields:
                raise CoordError('invalid approval operation or parameters')
            if op == 'approval-sync': return self.approvals.sync(host, owner, params['epoch'])
            if op == 'approval-route': return {'route': bool(self.approvals.route(c, host, params['thread']))}
            if op == 'approval-publish': return self.approvals.publish(c, host, owner, params)
            if op == 'approval-poll': return self.approvals.poll(host, owner, **params)
            return self.approvals.finish(host, owner, **params)
        from .root_team_reset import quarantined,handoff_begin,handoff_settling,handoff_settle,handoff_pending,handoff_retry_verified
        def reset_fence(*,session=None,thread=None,job=None):
            if quarantined(c.db,session=session,thread=thread,job=job):
                raise CoordError('team reset quarantined this former actor or journal; native calls withheld')
        def native_update(kind,job,status,values,write):
            phase = ('interrupting:'+str(values.get('turn_id'))) if kind=='control' and status=='interrupting' else status
            native = status in (('changing','interrupting') if kind=='control' else ('creating','materializing','adding','starting'))
            begin = native and (job['status']!=status or (status=='interrupting' and job.get('turn_id')!=values.get('turn_id')))
            if begin: handoff_begin(c.db,kind,job,host,phase)
            try:
                result = write()
            except BaseException:
                if begin: handoff_settle(c.db,kind,job['id'],host,'rejected-before-dispatch')
                raise
            if not begin: handoff_settle(c.db,kind,job['id'],host,status)
            if kind=='reset' and job['status']=='checking' and status=='ready':
                handoff_retry_verified(c.db,job['id'],host)
            return result
        def settlement_allowed(kind,job,status):
            if handoff_settling(c.db,kind,job['id'],host,status): return True
            if kind=='control' and status=='interrupting' and job['status']=='interrupting' and params.get('turn_id')==job.get('turn_id') and any(row['kind']==kind and row['job_id']==job['id'] and row['host_id']==host for row in handoff_pending(c.db)): return True
            return kind=='control' and status=='changing' and params.get('native_response') is not None and any(
                row['kind']==kind and row['job_id']==job['id'] and row['host_id']==host for row in handoff_pending(c.db))
        if op == 'control-update':
            from .agent_control import AgentControlState, parse_po_status_action
            table = c.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='agent_control_requests'").fetchone()
            columns = {row['name'] for row in c.db.execute('PRAGMA table_info(agent_control_requests)')} if table else set()
            candidate = (c.db.execute('SELECT * FROM agent_control_requests WHERE id=?',
                                      (params.get('job_id'),)).fetchone()
                         if {'id', 'action'} <= columns else None)
            if candidate and parse_po_status_action(candidate['action']):
                controls = AgentControlState.existing_schema(c)
                job = controls.job(candidate['id'])
                if job['host_id'] != host:
                    raise CoordError('PO status inspection belongs to another host')
                if set(params) - {'job_id','expected','status','observation'}:
                    raise CoordError('invalid PO status inspection completion fields')
                return controls.update(job['id'], params.get('expected'), params.get('status'),
                                       observation=params.get('observation'))
        # The scoped status request is carried by the existing control route,
        # but must not reach BridgeState's schema initializer on readback.
        if op in ('control-jobs', 'control-job'):
            from .agent_control import AgentControlState, parse_po_status_action
            table = c.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='agent_control_requests'").fetchone()
            columns = {row['name'] for row in c.db.execute('PRAGMA table_info(agent_control_requests)')} if table else set()
            candidate = None
            if {'id', 'action'} <= columns and op == 'control-job':
                candidate = c.db.execute('SELECT * FROM agent_control_requests WHERE id=?',
                                         (params.get('job_id'),)).fetchone()
            if {'host_id', 'action'} <= columns and op == 'control-jobs':
                candidate = next((row for row in c.db.execute(
                    'SELECT * FROM agent_control_requests WHERE host_id=?', (host,))
                    if parse_po_status_action(row['action'])), None)
            if candidate and parse_po_status_action(candidate['action']):
                controls = AgentControlState.existing_schema(c)
                if op == 'control-job':
                    job = controls.job(candidate['id'])
                    if job['host_id'] != host:
                        raise CoordError('agent control belongs to another host')
                    reset_fence(session=job['target_session'])
                    return job
                jobs = controls.jobs(host)
                for pending in handoff_pending(c.db):
                    if pending['kind'] == 'control' and pending['host_id'] == host and not any(
                            item['id'] == pending['job_id'] for item in jobs):
                        retained = controls.job(pending['job_id'])
                        if retained['status'] not in ('completed', 'failed', 'cancelled', 'resolved_unchanged'):
                            item = dict(retained)
                            if not parse_po_status_action(item.get('action')):
                                item['ownership'] = controls.ownership(retained['target_session'])
                            jobs.append(item)
                return jobs
        usage = self.usage_store.status(host) if self.usage_store else None
        state = BridgeState(c)
        if self.roots:
            from .root_restart import RootRestart
            state.root_restart=RootRestart(self.roots)
            state.root_project,state.root_host=self.project_id,host
        if op in ('root-fresh-jobs' ,'root-fresh-job','root-fresh-update'):
            from .root_restart import RootRestart
            restart = RootRestart(self.roots)
            if op == 'root-fresh-jobs': return restart.jobs(self.project_id,host)
            job = restart.job(params.get('job_id'))
            if (job['home_project'],job['host_id']) != (self.project_id,host):
                raise CoordError('root fresh-start belongs to another owning project or host')
            if op == 'root-fresh-job': return job
            if self.measurements and params.get('status') in ('creating','adding','starting') and self.measurements.status(self.project_id)['active']:
                raise CoordError('finish the active measurement before fresh root native mutation')
            if usage and usage['blocked']: raise CoordError(usage['reason'] or 'weekly usage reserve is active')
            values = {key:value for key,value in params.items() if key not in ('job_id','expected','status')}
            return restart.update(self.project_id,host,job['id'],params.get('expected'),params.get('status'),**values)
        if op=='root-fresh-recovery-event':
            from .root_restart import RootRestart
            attempt_id=params.get('attempt_id')
            with self.roots.projects.connection() as db:
                row=db.execute('SELECT home_project,host_id FROM root_fresh_recovery_attempts WHERE attempt_id=?',(attempt_id,)).fetchone()
            if not row or (row['home_project'],row['host_id'])!=(self.project_id,host):
                raise CoordError('candidate recovery attempt belongs to another owning host or project')
            return RootRestart(self.roots).recovery_event(attempt_id,params.get('event'),params.get('detail'))
        if op=='root-fresh-replacement-event':
            request_id=params.get('request_id')
            with self.roots.projects.connection() as db:
                row=db.execute('SELECT home_project,host_id FROM root_fresh_replacements WHERE request_id=?',(request_id,)).fetchone()
            if not row or (row['home_project'],row['host_id'])!=(self.project_id,host):
                raise CoordError('candidate replacement belongs to another owning host or project')
            values={key:value for key,value in params.items() if key not in ('request_id','reconciliation_id','event')}
            return RootRestart(self.roots).replacement_event(request_id,params.get('reconciliation_id'),params.get('event'),**values)
        if op in ('supervisor-jobs','supervisor-observe'):
            from .supervisor import SupervisorState
            supervisor = SupervisorState(c)
            if op == 'supervisor-jobs': return supervisor.jobs(host)
            job, report = params.get('job'), params.get('report')
            if not isinstance(job, dict): raise CoordError('invalid supervisor job')
            return supervisor.observe(job, report, host)
        if op in ('control-jobs','control-job','control-update'):
            from .agent_control import AgentControlState, parse_po_status_action
            has_po_status = any(parse_po_status_action(row['action']) for row in c.db.execute(
                'SELECT action FROM agent_control_requests WHERE host_id=?', (host,)))
            controls = (AgentControlState.existing_schema(c) if has_po_status
                        else AgentControlState(c))
            if op == 'control-jobs':
                jobs=controls.jobs(host)
                for pending in handoff_pending(c.db):
                    if pending['kind']=='control' and pending['host_id']==host and not any(j['id']==pending['job_id'] for j in jobs):
                        retained=controls.job(pending['job_id'])
                        if retained['status'] not in ('completed','failed','cancelled','resolved_unchanged'):
                            item = dict(retained)
                            if not parse_po_status_action(item.get('action')):
                                item['ownership'] = controls.ownership(retained['target_session'])
                            jobs.append(item)
                return jobs
            job = controls.job(params.get('job_id'))
            if op!='control-job' or not any(row['kind']=='control' and row['job_id']==job['id'] and row['host_id']==host for row in handoff_pending(c.db)):
                if not (op=='control-update' and settlement_allowed('control',job,params.get('status'))): reset_fence(session=job['target_session'])
            if job['host_id'] != host: raise CoordError('agent control belongs to another host')
            if op == 'control-job': return job
            if job['action'] == 'model':
                allowed = {'job_id', 'expected', 'status', 'error', 'observation',
                           'baseline_settings', 'observed_settings', 'direct_input',
                           'native_response', 'resolution_evidence', 'turn_id', 'input_job_id', 'input_evidence'}
                if set(params) - allowed:
                    raise CoordError('invalid model-control host update')
            if job['action'] == 'resume' and params.get('status') == 'completed' and usage and usage['blocked']:
                raise CoordError(usage['reason'] or 'weekly usage reserve is active')
            values = {key:value for key,value in params.items() if key not in ('job_id','expected','status')}
            with self.measurements.lock if self.measurements else contextlib.nullcontext():
                if self.measurements and job['action'] != 'inspect' and not job.get('root_team_stop') and self.measurements.status(self.project_id)['active']:
                    raise CoordError('finish the active project measurement before changing agent admission')
                return native_update('control',job,params.get('status'),values,lambda: controls.update(job['id'], params.get('expected'), params.get('status'), **values))
        if op in ('reset-jobs', 'reset-job', 'reset-update'):
            from .session_reset import SessionResetState
            resets = SessionResetState(c)
            capabilities = params.get('capabilities')
            durable_worker = capabilities == ['project-scope-v1', 'project-workspaces-v1', 'parked-provision-v1', 'turnover-bootstrap-v1', 'parked-durability-v1']
            turnover_worker = durable_worker or capabilities == ['project-scope-v1', 'project-workspaces-v1', 'parked-provision-v1', 'turnover-bootstrap-v1']
            parked_worker = turnover_worker or capabilities == ['project-scope-v1', 'project-workspaces-v1', 'parked-provision-v1']
            scoped_worker = parked_worker or capabilities in (['project-scope-v1'], ['project-scope-v1', 'project-workspaces-v1'])
            workspace_worker = parked_worker or capabilities == ['project-scope-v1', 'project-workspaces-v1']
            def parked_pending_epoch(job):
                if not c.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='execution_epoch_handoffs'").fetchone():
                    return None
                rows = c.db.execute('''SELECT id,dispatcher FROM execution_epoch_handoffs WHERE actor=? AND thread=?
                    AND host=? AND method='thread/start' AND settled_at IS NULL AND started_at>=?''',
                    (job['target_session'],job['old_thread_id'],host,job['created_at'])).fetchall()
                if len(rows) > 1 or (rows and rows[0]['dispatcher'] != owner):
                    raise CoordError('parked native creation epoch is ambiguous or belongs to another dispatcher')
                return rows[0]['id'] if rows else None
            def materialization_pending_epoch(job):
                if not c.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='execution_epoch_handoffs'").fetchone():
                    return None
                rows = c.db.execute('''SELECT id,dispatcher FROM execution_epoch_handoffs WHERE actor=? AND thread=?
                    AND host=? AND method='thread/name/set' AND settled_at IS NULL AND started_at>=?''',
                    (job['target_session'],job['old_thread_id'],host,job['created_at'])).fetchall()
                if len(rows) > 1 or (rows and rows[0]['dispatcher'] != owner):
                    raise CoordError('parked materialization epoch is ambiguous or belongs to another dispatcher')
                return rows[0]['id'] if rows else None
            def parked_registry_valid(job, registry):
                from .management import parked_creation
                proof = parked_creation(c.db,job['id'])
                if not proof or not self.roots or self.project_id != proof['project_id']:
                    return False
                if 'bootstrap_id' in proof.keys():
                    from .turnover_bootstrap import authorize_subordinate
                    try:
                        authorize_subordinate(c.db, registry, proof['bootstrap_id'], job['requester_session'],
                                              job['old_thread_id'], job['host_id'], job['requested_model'], job['requested_effort'])
                        return True
                    except CoordError:
                        return False
                row = registry.execute('SELECT session_id,orchestrator_session,joined,request_id FROM root_projects WHERE project_id=?', (self.project_id,)).fetchone()
                stops = registry.execute('SELECT id,root_session FROM root_team_operations WHERE resumed_at IS NULL').fetchall()
                original = registry.execute('SELECT root_session FROM root_team_operation_projects WHERE request_id=? AND project_id=?', (proof['stop_id'],self.project_id)).fetchone()
                root = c.db.execute('SELECT home_session FROM root_projections WHERE session_id=?', (proof['root_session'],)).fetchone()
                return bool(row and original and root and row['joined'] and row['session_id'] == proof['root_session']
                    and row['orchestrator_session'] == proof['target_session'] and row['request_id'] == proof['request_id']
                    and len(stops) == 1 and tuple(stops[0]) == (proof['stop_id'],root['home_session'])
                    and original['root_session'] == proof['root_session'])
            if op == 'reset-jobs':
                if parked_worker:
                    from .management import parked_creation
                    for pending in handoff_pending(c.db):
                        if pending['kind'] != 'reset' or pending['host_id'] != host:
                            continue
                        if not parked_creation(c.db, pending['job_id']):
                            continue
                        outcome = c.db.execute('''SELECT r.status,r.target_session,r.new_thread_id,r.settings_digest,r.new_settings_digest,
                            x.code,b.thread_id AS bound_thread FROM session_resets r
                            LEFT JOIN parked_native_rejections x ON x.request_id=r.id
                            LEFT JOIN bridge_bindings b ON b.session_id=r.target_session WHERE r.id=? AND r.host_id=?''',
                            (pending['job_id'],host)).fetchone()
                        if outcome and outcome['status'] == 'failed' and outcome['code'] == -32602:
                            handoff_settle(c.db,'reset',pending['job_id'],host,'provider-rejected')
                        elif (outcome and outcome['status'] == 'parked' and outcome['new_thread_id']
                              and outcome['bound_thread'] == outcome['new_thread_id'] and outcome['settings_digest']
                              and outcome['settings_digest'] == outcome['new_settings_digest']):
                            epoch_id = parked_pending_epoch(resets.job(pending['job_id']))
                            if epoch_id:
                                from .core import execution_epoch_settle
                                with c.tx() as db:
                                    execution_epoch_settle(db,epoch_id,host,'submitted',owner)
                            handoff_settle(c.db,'reset',pending['job_id'],host,'created')
                            metadata_epoch = materialization_pending_epoch(resets.job(pending['job_id']))
                            if metadata_epoch:
                                from .core import execution_epoch_settle
                                with c.tx() as db:
                                    execution_epoch_settle(db,metadata_epoch,host,'submitted',owner)
                            handoff_settle(c.db,'reset',pending['job_id'],host,'parked')
                    # A crash may happen after the parked transaction settled
                    # its root handoff but before the native epoch settlement.
                    # Recover only a positively committed exact binding.
                    if c.db.execute("SELECT 1 FROM sqlite_master WHERE name='execution_epoch_handoffs'").fetchone():
                        committed = c.db.execute('''SELECT DISTINCT r.id FROM session_resets r
                            JOIN bridge_bindings b ON b.session_id=r.target_session AND b.thread_id=r.new_thread_id
                            JOIN execution_epoch_handoffs e ON e.actor=r.target_session AND e.thread=r.old_thread_id
                                AND e.host=r.host_id AND e.method='thread/name/set' AND e.settled_at IS NULL AND e.started_at>=r.created_at
                            WHERE r.host_id=? AND r.status='parked' AND r.settings_digest IS NOT NULL
                                AND r.settings_digest=r.new_settings_digest''', (host,)).fetchall()
                        for row in committed:
                            if not parked_creation(c.db, row['id']):
                                continue
                            metadata_epoch = materialization_pending_epoch(resets.job(row['id']))
                            if metadata_epoch:
                                from .core import execution_epoch_settle
                                with c.tx() as db:
                                    execution_epoch_settle(db,metadata_epoch,host,'submitted',owner)
                jobs = [] if usage and usage['blocked'] else resets.jobs(host, project_scope=scoped_worker, project_workspaces=workspace_worker, parked_worker=parked_worker)
                if any(job.get('parked') for job in jobs):
                    if not self.roots:
                        return [job for job in jobs if not job.get('parked')]
                    with self.roots.projects.connection() as registry:
                        def allowed(job):
                            if not job.get('parked'): return True
                            if not durable_worker and job['status'] in ('pending','creating','created','materializing'): return False
                            if not parked_registry_valid(job,registry): return False
                            if job.get('turnover_bootstrap'):
                                if not turnover_worker: return False
                                from .turnover_bootstrap import authorized
                                try: authorized(c.db,registry,job['id'])
                                except CoordError: return False
                            return True
                        jobs = [job for job in jobs if allowed(job)]
                return jobs
            job = resets.job(params.get('job_id'))
            if job.get('parked') and not parked_worker:
                raise CoordError('upgrade the owning bridge for parked provisioning')
            if job.get('parked') and op == 'reset-update' and params.get('status') in ('materializing','parked') and not durable_worker:
                raise CoordError('upgrade the owning bridge for durable parked provisioning')
            if not (op=='reset-update' and settlement_allowed('reset',job,params.get('status'))):
                reset_fence(session=job['target_session'])
                reset_fence(session=job['requester_session'])
            if job.get('project_scope') and not scoped_worker:
                raise CoordError('upgrade the existing bridge for project-scoped root creation; no input admitted')
            if resets.requires_workspace_worker(job) and not workspace_worker:
                raise CoordError('upgrade the existing bridge for explicit project workspaces; no input admitted')
            if job['host_id'] != host:
                raise CoordError('fresh-session reset belongs to another host')
            if op == 'reset-job':
                return job
            status = params.get('status')
            if status in ('creating','ready','adding','starting') and not job.get('parked'):
                from .core import assert_agent_running
                assert_agent_running(c.db, job['target_session'])
            if usage and usage['blocked'] and status in ('creating', 'ready', 'parked', 'adding', 'starting'):
                raise CoordError(usage['reason'] or 'weekly usage reserve is active')
            with self.measurements.lock if self.measurements else contextlib.nullcontext():
                if self.measurements and status in ('creating', 'ready', 'parked', 'adding', 'starting') and self.measurements.status(self.project_id)['active']:
                    raise CoordError('finish the active project measurement before resetting a conversation')
                values = {key: value for key, value in params.items() if key not in ('job_id', 'expected', 'status', 'capabilities')}
                if '_turnover' in values:
                    raise CoordError('turnover authority is server-derived')
                if job.get('parked'):
                    if not self.roots:
                        raise CoordError('parked provisioning registry unavailable')
                    with self.roots.projects.connection() as registry:
                        registry.execute('BEGIN IMMEDIATE')
                        if job['status'] != 'creating' and not parked_registry_valid(job,registry):
                            registry.rollback()
                            raise CoordError('parked provisioning registry scope changed')
                        turnover = bool(job.get('turnover_bootstrap'))
                        if turnover:
                            if not turnover_worker: raise CoordError('upgrade bridge for turnover bootstrap')
                            from .turnover_bootstrap import authorized
                            authorized(c.db,registry,job['id'])
                        epoch_id = parked_pending_epoch(job) if job['status'] == 'created' and status == 'materializing' else None
                        result = native_update('reset',job,status,values,lambda: resets.update(job['id'], params.get('expected'), status, _turnover=turnover, **values))
                        if job['status'] == 'creating' and status == 'failed' and values.get('native_error_code') == -32602:
                            handoff_settle(c.db,'reset',job['id'],host,'provider-rejected')
                        if job['status'] == 'created' and status == 'materializing':
                            if epoch_id:
                                from .core import execution_epoch_settle
                                with c.tx() as db:
                                    execution_epoch_settle(db,epoch_id,host,'submitted',owner)
                            handoff_settle(c.db,'reset',job['id'],host,'created')
                        if job['status'] == 'materializing' and status == 'parked':
                            metadata_epoch = materialization_pending_epoch(job)
                            if metadata_epoch:
                                from .core import execution_epoch_settle
                                with c.tx() as db:
                                    execution_epoch_settle(db,metadata_epoch,host,'submitted',owner)
                        registry.commit()
                        return result
                return native_update('reset',job,status,values,lambda: resets.update(job['id'], params.get('expected'), status, **values))

        def route_for(session_id):
            if quarantined(c.db,session=session_id): return None
            route = state.resolve(session_id)
            if not route:
                return None
            if not c.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='remote_session_hosts'").fetchone():
                return None
            row = c.db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (route['route'][0]['session_id'],)).fetchone()
            return route if row and row['host_id'] == host else None

        def thread_allowed(thread_id):
            reset_fence(thread=thread_id)
            if not isinstance(thread_id, str):
                raise CoordError('invalid thread id')
            row = c.db.execute('SELECT session_id FROM bridge_bindings WHERE thread_id=?', (thread_id,)).fetchone()
            if not row or not route_for(row['session_id']):
                raise CoordError('thread belongs to another host or is not remotely bound')

        def job_allowed(job_id, settlement=False):
            if not isinstance(job_id, str):
                raise CoordError('invalid job id')
            if not settlement: reset_fence(job=job_id)
            job = state.job(job_id)
            if not settlement: thread_allowed(job['thread_id'])
            return job

        if op.startswith('capacity-'):
            def capacity_owned(job):
                assigned = c.db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (job['session_id'],)).fetchone()
                if not assigned or assigned['host_id'] != host:
                    raise CoordError('capacity recovery belongs to another host')
                reset_fence(session=job['session_id'], thread=job['thread_id'])
                return job
            if op == 'capacity-targets':
                return [target for target in state.capacity_targets() if route_for(target['session_id'])]
            if op == 'capacity-jobs':
                result = []
                for job in state.capacity_jobs():
                    try: result.append(capacity_owned(job))
                    except CoordError: continue
                return result
            if op in ('capacity-gate', 'capacity-schedule'):
                session, thread = params.get('session_id'), params.get('thread_id')
                if not isinstance(session, str) or not isinstance(thread, str):
                    raise CoordError('invalid capacity recovery target')
                capacity_owned(dict(session_id=session, thread_id=thread))
                if usage and usage['blocked']:
                    if op == 'capacity-gate': return usage['reason'] or 'weekly usage reserve is active'
                    return None
                if op == 'capacity-gate': return state.capacity_gate(session, thread)
                thread_allowed(thread)
                turn, now = params.get('turn_id'), params.get('now')
                if not isinstance(turn, str) or not turn or len(turn) > 128 or not isinstance(now, (int, float)) or isinstance(now, bool) or not math.isfinite(now):
                    raise CoordError('invalid capacity failure evidence')
                digest, model, effort = params.get('settings_digest'), params.get('model'), params.get('reasoning_effort')
                if not isinstance(digest, str) or len(digest) != 64 or not isinstance(model, str) or not model or len(model) > 128 or (effort is not None and (not isinstance(effort, str) or len(effort) > 128)):
                    raise CoordError('invalid capacity configuration evidence')
                return state.capacity_schedule(session, thread, turn, now, digest, model, effort, params.get('owner'))
            if op == 'capacity-update':
                job = capacity_owned(state.capacity.get(params.get('job_id')))
                expected, status, now = params.get('expected'), params.get('status'), params.get('now')
                values = {key:value for key,value in params.items() if key not in ('job_id','expected','status','now')}
                if usage and usage['blocked'] and status in ('adding','starting'):
                    raise CoordError(usage['reason'] or 'weekly usage reserve is active')
                interrupt = status == 'cancelling' and str(values.get('native_proof', '')).startswith('interrupt_requested:')
                native = status in ('adding','starting') or interrupt
                phase = 'interrupting:' + str(values.get('retry_turn_id')) if interrupt else status + ':' + str(values.get('attempts', job['attempts']))
                if native: handoff_begin(c.db, 'capacity', dict(job, target_session=job['session_id']), host, phase)
                try: result = state.capacity_update(job['id'], expected, status, now, **values)
                except BaseException:
                    if native: handoff_settle(c.db, 'capacity', job['id'], host, 'rejected-before-dispatch')
                    raise
                if not native:
                    proof = values.get('native_proof', '')
                    settlement = ('cancelled_verified' if status == 'cancelled' and proof.startswith(('queue_deleted:', 'turn_observed:', 'turn_stopped:'))
                                  else 'queued_verified' if status == 'queued' and proof.startswith('retained_queue:')
                                  else 'turn_observed_verified' if status in ('retrying','pending','completed') and proof.startswith('turn_observed:') else status)
                    handoff_settle(c.db, 'capacity', job['id'], host, settlement)
                return result
            raise CoordError('unknown capacity recovery operation')

        if op in ('policy-consumed','policy-offer','policy-pending','policy-receipt'):
            from .policy_store import PolicyStore
            state.policy_store=PolicyStore(self.roots.projects.path if self.roots else str(self.db_path)+'.policies.sqlite3')
            state.policy_host,state.policy_project=host,self.project_id
            def policy_scope(thread):
                root=self.roots.config() if self.roots else None
                if not root or (root['thread_id'],root['host_id'])!=(thread,host):
                    return self.project_id
                exists=c.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_projections'").fetchone()
                if not exists: return self.project_id
                columns={row['name'] for row in c.db.execute('PRAGMA table_info(root_projections)')}
                if not {'session_id','home_project','home_session','thread_id'} <= columns:
                    return self.project_id
                projection=c.db.execute('''SELECT 1 FROM root_projections p
                    JOIN bridge_bindings b ON b.session_id=p.session_id
                    JOIN remote_session_hosts h ON h.session_id=p.session_id
                    WHERE p.home_project=? AND p.home_session=? AND p.thread_id=?
                      AND b.thread_id=? AND b.parent_session IS NULL AND h.host_id=?''',
                    (root['home_project'],root['home_session'],thread,thread,host)).fetchone()
                return root['home_project'] if projection else self.project_id
            if op=='policy-pending':
                bound=c.db.execute('SELECT DISTINCT b.thread_id FROM bridge_bindings b JOIN remote_session_hosts h ON h.session_id=b.session_id WHERE h.host_id=?',(host,)).fetchall()
                result=[]
                for row in bound:
                    thread=row['thread_id'];state.policy_project=policy_scope(thread)
                    result.extend(offer for offer in state.policy_pending() if offer['thread']==thread)
                return result
            if op=='policy-receipt':
                offer=params.get('offer')
                if not isinstance(offer,dict) or any(not isinstance(offer.get(key),str) or not offer[key] for key in ('thread','client_id','version')): raise CoordError('invalid policy offer proof')
                thread_allowed(offer.get('thread'))
                state.policy_project=policy_scope(offer['thread'])
                return state.policy_receipt(offer,params.get('turn_id'))
            thread=params.get('thread_id');thread_allowed(thread)
            state.policy_project=policy_scope(thread)
            if op=='policy-consumed':
                from .communication import policy_version
                return {'consumed':params.get('version')==policy_version() and state.policy_consumed(thread)}
            kind,client_id=params.get('kind'),params.get('client_id')
            if kind=='wake':
                job=state.job(client_id)
                if job['thread_id']!=thread or job['payload']!=params.get('payload'): raise CoordError('policy offer differs from exact wake')
            elif kind=='reset':
                from .session_reset import SessionResetState
                job=SessionResetState(c).job(client_id)
                if job['host_id']!=host or job['new_thread_id']!=thread or job['status'] not in ('ready','adding'): raise CoordError('policy offer differs from owned bootstrap')
            elif kind=='root':
                from .root_restart import RootRestart
                job=RootRestart(self.roots).job(client_id)
                if (job['home_project'],job['host_id'],job['new_thread_id'])!=(self.project_id,host,thread) or job['status'] not in ('switched','adding'): raise CoordError('policy offer differs from owning root bootstrap')
            else: raise CoordError('invalid policy offer source')
            return state.policy_offer(kind,client_id,thread,params.get('payload'))
        if op == 'pending':
            if usage and usage['blocked']:
                return []
            return [m for m in state.pending() if route_for(m['recipient_session'])]
        if op == 'investigation-tasks':
            if usage and usage['blocked']:
                return []
            tasks = []
            for item in state.investigation_tasks(host_id=host):
                admission, message = item['admission'], item['message']
                route = route_for(message['recipient_session'])
                if route and route['thread_id'] == admission['thread_id']:
                    thread_allowed(admission['thread_id'])
                    tasks.append(item)
            return tasks
        if op == 'resolve':
            sid = params.get('session_id')
            if not isinstance(sid, str):
                raise CoordError('invalid session id')
            return route_for(sid)
        if op == 'wake-messages':
            thread_id, message_ids = params.get('thread_id'), params.get('message_ids')
            thread_allowed(thread_id)
            if (not isinstance(message_ids, list) or not 1 <= len(message_ids) <= 20
                    or not all(isinstance(mid, str) for mid in message_ids)):
                raise CoordError('wake messages require 1 to 20 message IDs')
            return state.wake_messages(thread_id, message_ids)
        if op == 'jobs':
            jobs = []
            for job in state.jobs():
                try:
                    reset_fence(job=job['id'])
                    thread_allowed(job['thread_id'])
                except CoordError:
                    continue
                jobs.append(job)
            return jobs
        if op in ('native-stage-record', 'native-stage-censor'):
            if set(params) != ({'job_id','observation'} if op == 'native-stage-record' else {'job_id','reason'}):
                raise CoordError('invalid native stage request')
            with c.tx():
                current = c.db.execute('SELECT owner,secret_hash,acquired_at FROM bridge_dispatcher WHERE host_id=?', (host,)).fetchone()
                if not current or current['owner'] != owner or not secrets.compare_digest(current['secret_hash'], digest):
                    raise CoordError('dispatcher ownership changed')
                job = job_allowed(params['job_id'])
                if op == 'native-stage-record':
                    return state.record_native_stage(job, host, params['observation'], current)
                return state.censor_native_stage(job, host, params['reason'], current)
        if op in ('job', 'still_unread', 'update'):
            settlement = op=='update' and params.get('status') in ('queued','dispatched','uncertain','failed','cancelled') and handoff_settling(c.db,'wake',params.get('job_id'),host,params.get('status'))
            job = job_allowed(params.get('job_id'), settlement=settlement)
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
            if status in ('adding','starting'):
                handoff_begin(c.db,'wake',job,host,status)
            try:
                state.update(job['id'], status, queue_id, error)
            except BaseException:
                if status in ('adding','starting'):
                    handoff_settle(c.db,'wake',job['id'],host,'rejected-before-dispatch')
                raise
            if status not in ('adding','starting'):
                handoff_settle(c.db,'wake',job['id'],host,status)
            return {'updated': True}
        if op == 'manager-update-candidate':
            if set(params) != {'thread_id','message_id'}:
                raise CoordError('invalid manager update candidate request')
            thread_allowed(params['thread_id'])
            return state.manager_update_candidate(params['thread_id'], params['message_id'])
        if op == 'manager-update-prior':
            if set(params) != {'thread_id','message_id','task_id','original_job_id','turn_id'}:
                raise CoordError('invalid manager update prior request')
            thread_allowed(params['thread_id'])
            return state.manager_update_prior(params['thread_id'], params['message_id'], params['task_id'],
                                              params['original_job_id'], params['turn_id'])
        if op == 'manager-update-claim':
            if set(params) != {'thread_id','message_id','task_id','original_job_id','turn_id','payload'}:
                raise CoordError('invalid manager update claim request')
            if usage and usage['blocked']:
                raise CoordError(usage['reason'] or 'weekly usage reserve is active')
            thread_allowed(params['thread_id'])
            return state.manager_update_claim(params['thread_id'], params['message_id'], params['task_id'],
                                              params['original_job_id'], params['turn_id'], params['payload'])
        if op == 'prepare':
            if usage and usage['blocked']:
                raise CoordError(usage['reason'] or 'weekly usage reserve is active')
            thread_id, messages, payload = params.get('thread_id'), params.get('messages'), params.get('payload')
            thread_allowed(thread_id)
            investigation_request_id = params.get('investigation_request_id')
            if investigation_request_id is not None and (not isinstance(investigation_request_id, str)
                    or len(investigation_request_id) > 128):
                raise CoordError('invalid investigation admission request ID')
            if not isinstance(messages, list) or not 1 <= len(messages) <= 100 or not isinstance(payload, str) or len(payload) > 262144:
                raise CoordError('invalid dispatch payload')
            if investigation_request_id is not None:
                if len(messages) != 1:
                    raise CoordError('investigation admission permits exactly one original message')
                authorized = {item['message']['id']: item for item in state.investigation_tasks(host_id=host)
                              if item['admission']['request_id'] == investigation_request_id
                              and item['admission']['thread_id'] == thread_id}
                pending = {message_id: item['message'] for message_id, item in authorized.items()}
            else:
                pending = {m['id']: m for m in state.pending() if route_for(m['recipient_session'])}
            valid = []
            for message in messages:
                actual = c.db.execute('SELECT recipient_session FROM messages WHERE id=?',
                    (message.get('id'),)).fetchone() if isinstance(message,dict) else None
                if actual: reset_fence(session=actual['recipient_session'])
                if not isinstance(message, dict) or message.get('id') not in pending or message != pending[message['id']]:
                    raise CoordError('dispatch message is no longer pending or does not match its recipient')
                if message not in valid:
                    valid.append(message)
            if 'eligibility_observation' in params and params['eligibility_observation'] is None:
                raise CoordError('invalid eligibility observation')
            return state.prepare(thread_id, valid, payload,
                                 investigation_request_id=investigation_request_id,
                                 eligibility_observation=params.get('eligibility_observation'),
                                 eligibility_lease=existing)
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
            versions = params.get('versions')
            if versions is not None and (not isinstance(versions, list) or len(versions) != 2 or any(v is not None and (not isinstance(v, str) or len(v) > 64) for v in versions)):
                raise CoordError('invalid version observation')
            if error is not None and not isinstance(error, str):
                raise CoordError('invalid heartbeat error')
            state.heartbeat(params['server'], params['pid'], error, host_id=host, project_scope_worker=params.get('capabilities') in (['project-scope-v1'], ['project-scope-v1', 'project-workspaces-v1']),
                            project_workspaces_worker=params.get('capabilities') == ['project-scope-v1', 'project-workspaces-v1'], versions=versions)
            return {'heartbeat': True}
        if op == 'execution-epoch-begin':
            from .core import execution_epoch_handoff
            from .bridge import _ExecutionEpochRpc
            if set(params)!={'method','thread','actor','job_id'} or params['method'] not in _ExecutionEpochRpc.MUTATING:
                raise CoordError('invalid native epoch admission')
            thread,actor=params['thread'],params['actor']
            try:
                grant = c.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_turnover_bootstraps'").fetchone()
                grant = grant and c.db.execute('SELECT 1 FROM root_turnover_bootstraps WHERE request_id=?', (params['job_id'],)).fetchone()
                metadata = params['method'] == 'thread/name/set'
                if grant and not metadata:
                    if not self.roots or params['method'] not in ('thread/queue/add','thread/queue/start'):
                        raise CoordError('invalid turnover bootstrap native epoch')
                if thread:
                    try: thread_allowed(thread)
                    except CoordError:
                        reset=c.db.execute("SELECT target_session,host_id,old_thread_id FROM session_resets WHERE new_thread_id=? AND status NOT IN ('completed','cancelled','failed')",(thread,)).fetchone() if c.db.execute("SELECT 1 FROM sqlite_master WHERE name='session_resets'").fetchone() else None
                        pinned=c.db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?',(reset['target_session'],)).fetchone() if reset else None
                        if not reset or reset['host_id']!=host or not pinned or pinned['host_id']!=host:
                            raise
                        thread_allowed(reset['old_thread_id'])
                        actor=reset['target_session']
                else:
                    if not actor or not route_for(actor): raise CoordError('native epoch new-thread owner host unavailable')
                    row=c.db.execute('SELECT target_session FROM session_resets WHERE id=?',(params['job_id'],)).fetchone() if c.db.execute("SELECT 1 FROM sqlite_master WHERE name='session_resets'").fetchone() else None
                    if not row or row['target_session']!=actor: raise CoordError('native epoch new-thread job association unavailable')
                def issue_epoch(registry=None):
                    with c.tx() as db:
                        from .parked_retirement import retired
                        if retired(db, session=actor, request_id=params['job_id']):
                            raise CoordError('retired or quarantined native actor or job; new native input withheld')
                        if metadata:
                            from .management import assert_parked_active, parked_creation
                            scope = db.execute('SELECT * FROM session_resets WHERE id=?', (params['job_id'],)).fetchone()
                            proof = parked_creation(db, params['job_id'])
                            if not self.roots or not registry or not scope or not proof or self.project_id != proof['project_id'] or (scope['status'],scope['target_session'],scope['new_thread_id'],scope['host_id']) != ('materializing',actor,thread,host):
                                raise CoordError('parked materialization native epoch scope changed')
                            assert_parked_active(db, params['job_id'])
                            if 'bootstrap_id' in proof.keys():
                                from .turnover_bootstrap import authorize_subordinate
                                authorize_subordinate(db, registry, proof['bootstrap_id'], scope['requester_session'], scope['old_thread_id'], host, scope['requested_model'], scope['requested_effort'])
                            else:
                                row = registry.execute('SELECT session_id,orchestrator_session,joined,request_id FROM root_projects WHERE project_id=?', (self.project_id,)).fetchone()
                                stops = registry.execute('SELECT id,root_session FROM root_team_operations WHERE resumed_at IS NULL').fetchall()
                                original = registry.execute('SELECT root_session FROM root_team_operation_projects WHERE request_id=? AND project_id=?', (proof['stop_id'],self.project_id)).fetchone()
                                root = db.execute('SELECT home_session FROM root_projections WHERE session_id=?', (proof['root_session'],)).fetchone()
                                if not (row and original and root and row['joined'] and row['session_id'] == proof['root_session'] and row['orchestrator_session'] == proof['target_session'] and row['request_id'] == proof['request_id'] and len(stops) == 1 and tuple(stops[0]) == (proof['stop_id'],root['home_session']) and original['root_session'] == proof['root_session']):
                                    raise CoordError('parked materialization registry scope changed')
                        if grant and not metadata:
                            from .turnover_bootstrap import authorized
                            scope = authorized(db, registry, params['job_id'])
                            expected = 'adding' if params['method'] == 'thread/queue/add' else 'starting'
                            if (scope['status'],scope['target_session'],scope['new_thread_id'],scope['host_id']) != (expected,actor,thread,host):
                                raise CoordError('turnover bootstrap native epoch scope changed')
                        from .supervisor import cold_epoch_check
                        cold_epoch_check(c, db, method=params['method'], thread=thread, actor=actor,
                                         host=host, wake_id=params['job_id'])
                        state.manager_update_epoch_check(thread, params['job_id'], actor, params['method'])
                        return execution_epoch_handoff(db,params['method'],thread,actor,host,owner)
                if grant or metadata:
                    with self.roots.projects.connection() as registry:
                        registry.execute('BEGIN IMMEDIATE')
                        identifier = issue_epoch(registry)
                        registry.commit()
                else:
                    identifier = issue_epoch()
            except CoordError as error:
                from .supervisor import reject_cold_epoch
                with c.tx() as db:
                    rejected=reject_cold_epoch(db, method=params['method'],thread=params['thread'],actor=params['actor'],
                                               host=host,wake_id=params['job_id'],reason=error)
                    claimed = bool(c.db.execute('SELECT 1 FROM manager_update_claims WHERE job_id=?',
                                                 (params['job_id'],)).fetchone())
                if rejected or claimed:
                    return {'rejected':str(error)}
                raise
            return {'id':identifier}
        if op == 'execution-epoch-settle':
            from .core import execution_epoch_settle
            if set(params)!={'id','outcome'}: raise CoordError('invalid native epoch settlement')
            with c.tx() as db: execution_epoch_settle(db,params['id'],host,params['outcome'],owner)
            return {'settled':True}
        raise CoordError('unsupported bridge operation')
