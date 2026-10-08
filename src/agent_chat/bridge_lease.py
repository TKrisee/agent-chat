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
        self.measurements = None
        self.roots = None
        self.project_id = 'default'
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
        from .root_team_reset import quarantined,handoff_begin,handoff_settling,handoff_settle,handoff_pending,handoff_retry_verified
        def reset_fence(*,session=None,thread=None,job=None):
            if quarantined(c.db,session=session,thread=thread,job=job):
                raise CoordError('team reset quarantined this former actor or journal; native calls withheld')
        def native_update(kind,job,status,values,write):
            phase = ('interrupting:'+str(values.get('turn_id'))) if kind=='control' and status=='interrupting' else status
            native = status in (('changing','interrupting') if kind=='control' else ('creating','adding','starting'))
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
        usage = self.usage_store.status(host) if self.usage_store else None
        state = BridgeState(c)
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
        if op in ('supervisor-jobs','supervisor-observe'):
            from .supervisor import SupervisorState
            supervisor = SupervisorState(c)
            if op == 'supervisor-jobs': return supervisor.jobs(host)
            job, report = params.get('job'), params.get('report')
            if not isinstance(job, dict): raise CoordError('invalid supervisor job')
            return supervisor.observe(job, report, host)
        if op in ('control-jobs','control-job','control-update'):
            from .agent_control import AgentControlState
            controls = AgentControlState(c)
            if op == 'control-jobs':
                jobs=controls.jobs(host)
                for pending in handoff_pending(c.db):
                    if pending['kind']=='control' and pending['host_id']==host and not any(j['id']==pending['job_id'] for j in jobs):
                        retained=controls.job(pending['job_id'])
                        if retained['status'] not in ('completed','failed','cancelled','resolved_unchanged'):
                            jobs.append(dict(retained,ownership=controls.ownership(retained['target_session'])))
                return jobs
            job = controls.job(params.get('job_id'))
            if op!='control-job' or not any(row['kind']=='control' and row['job_id']==job['id'] and row['host_id']==host for row in handoff_pending(c.db)):
                if not (op=='control-update' and settlement_allowed('control',job,params.get('status'))): reset_fence(session=job['target_session'])
            if job['host_id'] != host: raise CoordError('agent control belongs to another host')
            if op == 'control-job': return job
            if job['action'] == 'model':
                allowed = {'job_id', 'expected', 'status', 'error', 'observation',
                           'baseline_settings', 'observed_settings', 'direct_input',
                           'native_response', 'resolution_evidence'}
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
            scoped_worker = capabilities in (['project-scope-v1'], ['project-scope-v1', 'project-workspaces-v1'])
            workspace_worker = capabilities == ['project-scope-v1', 'project-workspaces-v1']
            if op == 'reset-jobs':
                return [] if usage and usage['blocked'] else resets.jobs(host, project_scope=scoped_worker, project_workspaces=workspace_worker)
            job = resets.job(params.get('job_id'))
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
            if status in ('creating','ready','adding','starting'):
                from .core import assert_agent_running
                assert_agent_running(c.db, job['target_session'])
            if usage and usage['blocked'] and status in ('creating', 'ready', 'adding', 'starting'):
                raise CoordError(usage['reason'] or 'weekly usage reserve is active')
            with self.measurements.lock if self.measurements else contextlib.nullcontext():
                if self.measurements and status in ('creating', 'ready', 'adding', 'starting') and self.measurements.status(self.project_id)['active']:
                    raise CoordError('finish the active project measurement before resetting a conversation')
                values = {key: value for key, value in params.items() if key not in ('job_id', 'expected', 'status', 'capabilities')}
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

        if op in ('policy-consumed','policy-offer','policy-pending','policy-receipt'):
            from .policy_store import PolicyStore
            state.policy_store=PolicyStore(self.roots.projects.path if self.roots else str(self.db_path)+'.policies.sqlite3')
            state.policy_host,state.policy_project=host,self.project_id
            if op=='policy-pending':
                return [offer for offer in state.policy_pending() if c.db.execute('SELECT 1 FROM bridge_bindings b JOIN remote_session_hosts h ON h.session_id=b.session_id WHERE b.thread_id=? AND h.host_id=?',(offer['thread'],host)).fetchone()]
            if op=='policy-receipt':
                offer=params.get('offer')
                if not isinstance(offer,dict) or any(not isinstance(offer.get(key),str) or not offer[key] for key in ('thread','client_id','version')): raise CoordError('invalid policy offer proof')
                thread_allowed(offer.get('thread'))
                return state.policy_receipt(offer,params.get('turn_id'))
            thread=params.get('thread_id');thread_allowed(thread)
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
                actual = c.db.execute('SELECT recipient_session FROM messages WHERE id=?',
                    (message.get('id'),)).fetchone() if isinstance(message,dict) else None
                if actual: reset_fence(session=actual['recipient_session'])
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
            state.heartbeat(params['server'], params['pid'], error, host_id=host, project_scope_worker=params.get('capabilities') in (['project-scope-v1'], ['project-scope-v1', 'project-workspaces-v1']),
                            project_workspaces_worker=params.get('capabilities') == ['project-scope-v1', 'project-workspaces-v1'])
            return {'heartbeat': True}
        raise CoordError('unsupported bridge operation')
