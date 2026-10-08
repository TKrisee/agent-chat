import base64
import contextlib
import hashlib
import json
import unittest
import copy
from pathlib import Path
import tempfile
import time
import uuid

from agent_chat.bridge import Bridge
from agent_chat.bridge_client import RemoteBridgeState
from agent_chat.core import CoordError
from agent_chat.remote import RemoteCoordError
from agent_chat.rpc import TransportError
from test_remote_web import RemoteWebFixture
from test_session_reset import ResetRpc


class ControlRpc(ResetRpc):
    def __init__(self, old):
        super().__init__(old)
        self.turn = {'id':'owned-turn', 'status':'inProgress'}
        self.drop_interrupt = False
        self.started_settings = {}

    def request(self, method, params):
        thread = params.get('threadId')
        if method == 'thread/turns/list' and thread == self.old:
            self.calls.append((method, copy.deepcopy(params)))
            return {'data':[dict(self.turn)], 'nextCursor':None}
        if method == 'turn/interrupt':
            self.calls.append((method, copy.deepcopy(params)))
            assert params['turnId'] == self.turn['id']
            self.status[thread] = 'idle'
            self.turn['status'] = 'interrupted'
            if self.drop_interrupt:
                self.drop_interrupt = False
                raise TransportError('lost exact interrupt result')
            return {}
        if method == 'thread/resume' and thread in self.started_settings:
            self.calls.append((method, copy.deepcopy(params)))
            return copy.deepcopy(self.started_settings[thread])
        result = super().request(method, params)
        if method == 'thread/start':
            for key in ('model','cwd','runtimeWorkspaceRoots'):
                if key in params: result[key] = params[key]
            result['reasoningEffort'] = params.get('config', {}).get('model_reasoning_effort', self.original['reasoningEffort'])
            workspace = params.get('config', {}).get('sandbox_workspace_write')
            if workspace is not None and result['sandbox'].get('type') == 'workspaceWrite':
                result['sandbox']['writableRoots'] = workspace['writable_roots']
            elif (params.get('permissions') == ':workspace' and params.get('cwd') != self.original['cwd']
                  and result['sandbox'].get('type') == 'workspaceWrite'):
                result['sandbox']['writableRoots'] = params['runtimeWorkspaceRoots'][1:]
            self.started_settings[self.new] = copy.deepcopy(result)
        return result


class AgentControlTests(RemoteWebFixture):
    def setUp(self):
        super().setUp()
        self.call('register', agent='worker')
        self.call('register', session='b', agent='orchestrator')
        self.old = str(uuid.uuid4())
        self.call('bind', thread=self.old)
        self.identity = {'owner':'control-test-worker','secret':'x'*40,'host_id':'mac'}
        self.state = RemoteBridgeState(self.client, self.url, self.identity)
        self.state.call('acquire')
        self.rpc = ControlRpc(self.old)
        self.bridge = Bridge(self.state, self.rpc)

    def control(self, action='stop', **values):
        params = dict(to='worker', expected_thread=self.old, request_id=str(uuid.uuid4()), confirm=True)
        params.update(values)
        return self.call('agent-'+action, session='b', **params)

    def status(self): return self.call('agent-status', session='b', to='worker')

    def test_idle_stop_preserves_unread_then_resume_dispatches_once(self):
        msg = self.call('send', session='b', to='worker', body='Bounded handoff')
        stop = self.control()
        self.assertEqual(self.status()['admission'], 'stopping')
        self.bridge.tick()
        result = self.status()
        self.assertEqual(result['admission'], 'stopped')
        self.assertEqual(result['requests'][0]['id'], stop['id'])
        self.assertEqual(result['observation']['model'], 'test-model')
        self.assertIsNone(self.call('inbox')['messages'][0]['acked_at'])
        self.assertEqual(self.rpc.queues[self.old], [])
        self.control('resume')
        self.bridge.tick()
        self.assertEqual(self.status()['admission'], 'running')
        self.assertEqual(len([m for m,p in self.rpc.calls if m == 'thread/queue/add']), 1)
        self.assertEqual(self.call('inbox')['messages'][0]['id'], msg['id'])

    def test_graceful_stop_allows_owned_cleanup_and_preserves_queue(self):
        claim = self.call('request', resource='held', minutes=5)
        self.call('request', session='b', resource='held', minutes=5)
        self.control()
        self.bridge.tick()
        self.assertEqual(self.status()['admission'], 'stopping')
        self.assertEqual(self.call('request', resource='held', minutes=5)['reservation_id'], claim['reservation_id'])
        with self.assertRaisesRegex(RemoteCoordError, 'admission is stopping'):
            self.call('request', resource='new-work', minutes=5)
        run = self.call('begin-guard', resource='held', token=claim['token'], pid=0)
        self.call('close-guard', run_id=run['run_id'], token=claim['token'], evidence_sha256='0'*64)
        receipt = dict(version=1,resource='held',reservation_id=claim['reservation_id'],restored=True,
                       processes_closed=True,closed_at=time.time(),evidence='CLOSED.txt',pids=[])
        self.call('release', resource='held', token=claim['token'], receipt=receipt,
                  evidence_base64=base64.b64encode(b'Owner cleanup complete').decode())
        self.bridge.tick()
        self.assertEqual(self.status()['admission'], 'stopped')
        self.assertEqual(self.call('status', session='b', resources=['held'])['resources'][0]['queue'][0]['session'], 'b')

    def test_interrupt_does_not_close_guards_or_transfer_hold_and_no_replay(self):
        claim = self.call('request', resource='held', minutes=5)
        run = self.call('begin-guard', resource='held', token=claim['token'], pid=0)
        self.rpc.status[self.old] = 'active'
        self.rpc.drop_interrupt = True
        self.control(interrupt=True)
        with self.assertRaises(TransportError): self.bridge.tick()
        self.bridge.tick()
        status = self.status()
        self.assertEqual(status['admission'], 'stopped')
        self.assertEqual(status['ownership']['reservations'][0]['reservation_id'], claim['reservation_id'])
        self.assertEqual(status['ownership']['open_guards'][0]['run_id'], run['run_id'])
        self.assertEqual(len([m for m,p in self.rpc.calls if m == 'turn/interrupt']), 1)
        with self.assertRaisesRegex(RemoteCoordError, 'admission is stopped'):
            self.call('begin-guard', resource='held', token=claim['token'], pid=0)
        self.control('resume'); self.bridge.tick()
        self.assertEqual(self.status()['admission'], 'running')
        self.assertEqual(len(self.status()['ownership']['open_guards']), 1)

    def test_existing_app_queue_blocks_interrupt_without_deleting_input(self):
        self.rpc.status[self.old] = 'active'
        self.rpc.queues[self.old] = [{'id':'existing-input','clientUserMessageId':'operator-input'}]
        self.control(interrupt=True); self.bridge.tick()
        self.assertEqual(self.status()['admission'], 'stopping')
        self.assertEqual(self.status()['observation']['queue_count'], 1)
        self.assertEqual(self.rpc.queues[self.old][0]['id'], 'existing-input')
        self.assertFalse(any(m == 'turn/interrupt' for m,p in self.rpc.calls))

    def test_prepared_wake_waits_and_admission_gate_rejects_crossed_stop(self):
        message = self.call('send',session='b',to='worker',body='Prepared handoff')
        messages = self.state.pending()
        job = self.state.prepare(self.old,messages,self.bridge.payload(self.old,messages))
        self.control()
        with self.assertRaisesRegex(RemoteCoordError,'admission is stopping'):
            self.state.update(job['id'],'adding')
        self.bridge.tick()
        self.assertEqual(self.state.job(job['id'])['status'],'prepared')
        self.assertEqual(self.call('inbox')['messages'][0]['id'],message['id'])
        self.control('resume');self.bridge.tick()
        self.assertEqual(self.state.job(job['id'])['status'],'dispatched')

    def test_lost_interrupt_never_interrupts_a_later_manual_turn(self):
        self.rpc.status[self.old]='active';self.rpc.drop_interrupt=True
        self.control(interrupt=True)
        with self.assertRaises(TransportError):self.bridge.tick()
        self.rpc.turn={'id':'later-manual-turn','status':'inProgress'}
        self.rpc.status[self.old]='active';self.bridge.tick()
        self.assertEqual(self.status()['admission'],'stopping')
        self.assertEqual(len([m for m,p in self.rpc.calls if m=='turn/interrupt']),1)
        # Authoritative idle is enough to prove no turn remains active even if
        # the original turn has fallen outside the bounded history window.
        self.rpc.turn['status']='completed';self.rpc.status[self.old]='idle';self.bridge.tick()
        self.assertEqual(self.status()['admission'],'stopped')

    def test_resume_cancels_graceful_wait_without_interrupt(self):
        self.rpc.status[self.old] = 'active'
        stop = self.control(); self.bridge.tick()
        self.control('resume'); self.bridge.tick()
        self.assertEqual(self.status()['admission'], 'running')
        self.assertEqual(next(r for r in self.status()['requests'] if r['id']==stop['id'])['status'], 'cancelled')
        self.assertFalse(any(m == 'turn/interrupt' for m,p in self.rpc.calls))

    def test_confirmed_exact_thread_idempotency_and_host_scope(self):
        request_id = str(uuid.uuid4())
        first = self.control(request_id=request_id)
        self.assertEqual(self.control(request_id=request_id)['id'], first['id'])
        with self.assertRaisesRegex(RemoteCoordError, 'differs'):
            self.control(request_id=request_id, interrupt=True)
        with self.assertRaisesRegex(RemoteCoordError, 'exact current main'):
            self.control(expected_thread=str(uuid.uuid4()))
        with self.assertRaisesRegex(RemoteCoordError, 'confirmation'):
            self.control(confirm=False)
        other = RemoteBridgeState(self.client,self.url,dict(self.identity,host_id='other'))
        other.call('acquire')
        self.assertEqual(other.control_jobs(), [])
        with self.assertRaisesRegex(RemoteCoordError,'another host'):
            other.control_job(first['id'])

    def test_refresh_is_durable_read_only_and_roster_compact(self):
        result = self.call('agent-status', session='b', to='worker', refresh=True,
                           expected_thread=self.old, request_id=str(uuid.uuid4()))
        self.bridge.tick()
        status = self.status()
        self.assertEqual(status['admission'], 'running')
        self.assertEqual(status['requests'][0]['id'], result['refresh']['id'])
        self.assertEqual(status['observation']['reasoning_effort'], 'high')
        self.assertTrue({'worker','orchestrator'}.issubset({a['agent'] for a in self.call('agents')['agents']}))
        self.assertFalse(any(m in ('turn/interrupt','thread/start') for m,p in self.rpc.calls))

    def test_creation_selects_model_reasoning_existing_workspace_and_preserves_policy(self):
        result = self.call('agent-create', agent='coder', expected_thread=self.old,
            prompt='Use your own identity. Work here.', request_id=str(uuid.uuid4()), confirm=True,
            model='gpt-6.1-sol', reasoning_effort='high', cwd=self.tmp.name)
        self.bridge.tick()
        job = self.call('session-reset-status',to='coder')['resets'][0]
        self.assertEqual(job['status'], 'completed', job.get('error'))
        params = next(p for m,p in self.rpc.calls if m == 'thread/start')
        self.assertEqual(params['model'],'gpt-6.1-sol')
        self.assertEqual(params['cwd'],str(Path(self.tmp.name).resolve()))
        self.assertEqual(params['permissions'],':workspace')
        self.assertEqual(params['config']['model_reasoning_effort'],'high')
        self.assertNotEqual(result['target_session'],'a')

    def test_missing_workspace_and_setting_mismatch_withhold_prompt(self):
        self.call('agent-create',agent='coder',expected_thread=self.old,prompt='Work',request_id=str(uuid.uuid4()),
                  confirm=True,cwd=str(Path(self.tmp.name)/'absent'))
        self.bridge.tick()
        job=self.call('session-reset-status',to='coder')['resets'][0]
        self.assertIn('already exist',job['error'])
        self.assertFalse(any(m=='thread/start' for m,p in self.rpc.calls))

    def test_stop_during_pending_reset_holds_fresh_bootstrap_until_resume(self):
        self.call('session-reset',session='b',to='worker',expected_thread=self.old,prompt='Fresh task',request_id=str(uuid.uuid4()),confirm=True)
        self.control(); self.bridge.tick()
        self.assertFalse(any(m=='thread/start' for m,p in self.rpc.calls))
        self.control('resume');self.bridge.tick()
        self.assertEqual(self.call('session-reset-status',to='worker')['resets'][0]['status'],'completed')


class WorkspaceControlTests(unittest.TestCase):
    def test_multi_workspace_cold_resume_remains_refused(self):
        from agent_chat.control_worker import ControlWorker
        thread = str(uuid.uuid4())
        settings = dict(thread={'id':thread}, model='test-model', modelProvider='openai', reasoningEffort='high',
            cwd='/synthetic/primary', runtimeWorkspaceRoots=['/synthetic/primary','/synthetic/secondary'],
            activePermissionProfile={'id':':workspace','extends':None},
            sandbox=dict(type='workspaceWrite',writableRoots=[],networkAccess=False,excludeTmpdirEnvVar=False,excludeSlashTmp=False),
            approvalPolicy='on-request',approvalsReviewer='auto_review',serviceTier=None,multiAgentMode='explicitRequestOnly',
            collaborationMode={'mode':'default','settings':{'model':'test-model','reasoning_effort':'high','developer_instructions':None}})
        self.assertFalse(ControlWorker.cold_supported(settings, thread))

    def test_observed_members_are_durable_and_ordered_without_permission_mutation(self):
        import contextlib, tempfile
        from agent_chat.core import Coordinator
        from agent_chat.bridge_state import BridgeState
        from agent_chat.remote_service import _schema
        from agent_chat.agent_control import AgentControlState
        with tempfile.TemporaryDirectory(prefix='agent-chat-workspace-observation-') as directory:
            path = Path(directory)/'state.sqlite3'
            with contextlib.closing(Coordinator(path,'owner')) as coord:
                coord.register('owner'); coord.inbox(); state = BridgeState(coord); _schema(coord)
                members = ['/synthetic/primary','/synthetic/secondary']
                thread = str(uuid.uuid4()); state.bind(thread)
                coord.db.execute('INSERT INTO remote_session_hosts VALUES(?,?,?,?)',('owner','synthetic-host',time.time(),time.time()))
                controls = AgentControlState(coord)
                job = controls.request('owner',thread,'inspect',str(uuid.uuid4()),True)
                controls.update(job['id'],job['status'],'completed',observation=dict(state='idle',queue_count=0,
                    model='test-model',reasoning_effort='high',cwd=members[0],permissions={'runtimeWorkspaceRoots':members}))
            with contextlib.closing(Coordinator(path,'owner')) as coord:
                self.assertEqual(AgentControlState(coord).status('owner')['observation']['workspaces'], members)
                self.assertEqual(AgentControlState.observed_workspaces(coord.db,'legacy','/synthetic/legacy'), ['/synthetic/legacy'])


class AdminRecoveryStateTests(unittest.TestCase):
    """Exact stale recovery must never turn missing native evidence into a clear."""
    def setUp(self):
        from agent_chat.agent_control import AgentControlState
        from agent_chat.bridge_state import BridgeState
        from agent_chat.core import Coordinator
        from agent_chat.remote_service import _schema
        self.tmp = tempfile.TemporaryDirectory(prefix='agent-chat-admin-recovery-')
        self.addCleanup(self.tmp.cleanup)
        self.coord = Coordinator(Path(self.tmp.name) / 'state.sqlite3', 'root')
        self.addCleanup(self.coord.close)
        BridgeState(self.coord); _schema(self.coord); AgentControlState(self.coord)
        for session, agent in (('root', 'root'), ('po', 'project-orchestrator'), ('child', 'worker'), ('orphan', 'orphan')):
            self.coord.session = session; self.coord.register(agent); self.coord.inbox()
        self.coord.db.executescript('''
            CREATE TABLE IF NOT EXISTS root_projections (
                session_id TEXT PRIMARY KEY, home_project TEXT NOT NULL, home_session TEXT NOT NULL,
                thread_id TEXT NOT NULL, cwd TEXT, paused INTEGER NOT NULL DEFAULT 0, model TEXT, effort TEXT);
            CREATE TABLE IF NOT EXISTS project_orchestrators (session_id TEXT PRIMARY KEY, root_session TEXT NOT NULL);
        ''')
        self.coord.db.execute('INSERT INTO root_projections VALUES(?,?,?,?,?,?,?,?)',
                              ('root', 'home', 'root', 'root-thread', '/synthetic', 0, None, None))
        self.coord.db.execute('INSERT INTO project_orchestrators VALUES(?,?)', ('po', 'root'))
        self.coord.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?,?,?)', ('po', 'po-thread', 'root', '/po', time.time()))
        self.coord.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?,?,?)', ('child', 'child-thread', 'po', '/po/child', time.time()))

    def stale(self, owner='child', resource='file:synthetic.py'):
        self.coord.session = owner; self.coord.inbox()
        claim = self.coord.request(resource, 5)
        self.coord.db.execute('UPDATE resources SET deadline=? WHERE name=?', (time.time() - 1, resource))
        self.coord.session = 'root'; self.coord.inbox()
        return claim

    def recover(self, claim, *, request_id=None, owner='child', project='project-x', evidence=None):
        from agent_chat.agent_control import AdminRecoveryState
        return AdminRecoveryState(self.coord).recover(
            project=project, canonical_project='project-x', resource=claim['resource'],
            reservation_id=claim['reservation_id'], owner_session=owner,
            request_id=request_id or str(uuid.uuid4()), operator='operator', reason='approved recovery',
            evidence=evidence or {'ticket': 'operator-approved'}, host_id='host')

    def qualified_pin(self, sender, label, packet):
        message_id, attachment_id = 'message-' + label, 'attachment-' + label
        content = json.dumps(packet, sort_keys=True, separators=(',', ':')).encode()
        self.coord.db.execute('INSERT INTO messages(id,sender_session,recipient_session,body,created_at) VALUES(?,?,?,?,?)',
                              (message_id, sender, 'root', label, time.time()))
        self.coord.db.execute('INSERT INTO attachments(id,message_id,name,mime,size,content) VALUES(?,?,?,?,?,?)',
                              (attachment_id, message_id, label + '.txt', 'text/plain', len(content), content))
        return {'attachment_id': attachment_id, 'sha256': hashlib.sha256(content).hexdigest()}

    def test_exact_stale_clear_is_audited_and_idempotent(self):
        claim = self.stale(); request_id = str(uuid.uuid4())
        first = self.recover(claim, request_id=request_id)
        second = self.recover(claim, request_id=request_id)
        self.assertEqual(first['outcome'], 'cleared')
        self.assertEqual(second, first)
        self.assertIsNone(self.coord.db.execute('SELECT owner_session FROM resources WHERE name=?',
                                                (claim['resource'],)).fetchone()['owner_session'])
        self.assertTrue(self.coord.db.execute('SELECT 1 FROM receipts WHERE reservation_id=?',
                                              (claim['reservation_id'],)).fetchone())

    def test_pending_control_and_missing_pid_are_retained(self):
        claim = self.stale(resource='file:pending.py')
        self.coord.db.execute('''INSERT INTO agent_control_requests
            (id,requester_session,target_session,host_id,thread_id,action,interrupt,status,turn_id,error,created_at,updated_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''',
            (str(uuid.uuid4()), 'root', 'child', 'host', 'child-thread', 'inspect', 0, 'pending', None, None, time.time(), time.time()))
        self.assertEqual(self.recover(claim)['outcome'], 'retained')
        self.coord.db.execute('DELETE FROM agent_control_requests')
        second = self.stale(resource='file:missing-pid.py')
        now = time.time()
        self.coord.db.execute('INSERT INTO remote_guarded_runs VALUES(?,?,?,?,?,?,?,?,?)',
                              ('run-missing-pid', second['resource'], second['reservation_id'], 'child', 'host', 0, now, now, 'e' * 64))
        self.assertEqual(self.recover(second)['outcome'], 'retained')

    def test_qualified_current_disposition_preserves_original_open_refusal_and_history(self):
        claim = self.stale(resource='file:qualified-current.py')
        self.coord.db.execute('UPDATE resources SET reason=? WHERE name=?',
                              ('OPEN: original refusal retained', claim['resource']))
        now = time.time()
        from agent_chat.lifecycle_producer import enroll, credential, dispatch
        self.coord.db.execute('INSERT INTO remote_session_hosts VALUES(?,?,?,?)', ('child', 'unity-host', now, now))
        private_capability = Path(self.tmp.name) / 'producer-capability'
        producer = enroll(self.coord, 'child', 'unity-host', private_capability)
        self.coord.db.execute('INSERT INTO remote_guarded_runs VALUES(?,?,?,?,?,?,?,?,?)',
                              ('original-default-run', claim['resource'], claim['reservation_id'], 'child', 'unity-host', 0,
                               time.time(), None, None))
        auth = {'capability': credential(private_capability), 'run_id': 'original-default-run'}
        instance = dispatch(self.coord, 'child', 'unity-host', 'producer-establish', auth)
        self.coord.db.execute('UPDATE remote_guarded_runs SET local_pid=404 WHERE run_id=?', ('original-default-run',))
        fact = dispatch(self.coord, 'child', 'unity-host', 'producer-close', dict(auth, instance_id=instance['id'], pid=404, returncode=0, group_closed=True))
        self.coord.db.execute('INSERT INTO remote_reservation_outcomes(reservation_id,resource,owner_session,host_id,state,created_at) VALUES(?,?,?,?,?,?)',
                              (claim['reservation_id'], claim['resource'], 'child', 'unity-host', 'pending', now))
        original = {'resource': claim['resource'], 'reservation_id': claim['reservation_id'],
                    'owner_session': 'child', 'remote_run_id': 'original-default-run',
                    'refusal': 'OPEN: original refusal retained'}
        closure = {'version': 1, 'kind': 'qualified-current-closure', 'original': original,
                   'current': {'instance_id': instance['id'], 'closure_fact_id': fact['closure_fact_id'], 'host_id': 'unity-host',
                               'closure_authority': producer, 'closure_digest': fact['sha256']},
                   'known_instances': [instance['id']]}
        current_pin = self.qualified_pin('root', 'current', closure)
        reviewer = {'version': 1, 'kind': 'qualified-current-review', 'original': original,
                    'closure_attachment_id': current_pin['attachment_id'],
                    'closure_packet_sha256': current_pin['sha256']}
        evidence = {'ticket': 'operator-approved', 'qualified_current_disposition': {
            'original_reservation_id': claim['reservation_id'], 'original_run_id': 'original-default-run',
            'original_refusal': 'OPEN: original refusal retained',
            'current_closure': current_pin,
            'reviewer_closure': self.qualified_pin('po', 'reviewer', reviewer),
            'historical_uncertainty': ['historical descendant frontier was never captured']}}
        self.coord.session = 'root'; self.coord.inbox()
        request_id = str(uuid.uuid4())
        self.coord.db.execute('UPDATE messages SET recipient_session=? WHERE id=?', ('po', 'message-current'))
        with self.assertRaisesRegex(CoordError, 'delivered'):
            self.recover(claim, evidence=evidence)
        self.coord.db.execute('UPDATE messages SET recipient_session=? WHERE id=?', ('root', 'message-current'))
        self.coord.db.execute('UPDATE project_orchestrators SET root_session=? WHERE session_id=?', ('unrelated-root', 'po'))
        with self.assertRaisesRegex(CoordError, 'root|chain'):
            self.recover(claim, evidence=evidence)
        self.coord.db.execute('UPDATE project_orchestrators SET root_session=? WHERE session_id=?', ('root', 'po'))
        self.coord.db.execute('DELETE FROM remote_reservation_outcomes WHERE reservation_id=?', (claim['reservation_id'],))
        with self.assertRaisesRegex(CoordError, 'binding mismatch'):
            self.recover(claim, evidence=evidence)
        self.coord.db.execute('INSERT INTO remote_reservation_outcomes(reservation_id,resource,owner_session,host_id,state,created_at) VALUES(?,?,?,?,?,?)',
                              (claim['reservation_id'], claim['resource'], 'child', 'unity-host', 'pending', now))
        first = self.recover(claim, request_id=request_id, evidence=evidence)
        second = self.recover(claim, request_id=request_id, evidence=evidence)
        self.assertEqual(first['outcome'], 'qualified_released')
        self.assertEqual(second, first)
        audit = self.coord.db.execute('SELECT * FROM admin_qualified_dispositions WHERE request_id=?',
                                      (request_id,)).fetchone()
        self.assertEqual(audit['original_refusal'], evidence['qualified_current_disposition']['original_refusal'])
        self.assertEqual(json.loads(audit['historical_uncertainty_json']), evidence['qualified_current_disposition']['historical_uncertainty'])
        self.assertIsNone(self.coord.db.execute('SELECT 1 FROM receipts WHERE reservation_id=?',
                                                (claim['reservation_id'],)).fetchone())
        self.assertIsNone(self.coord.db.execute('SELECT owner_session FROM resources WHERE name=?',
                                                (claim['resource'],)).fetchone()['owner_session'])
        self.assertIsNone(self.coord.db.execute('SELECT closed_at FROM remote_guarded_runs WHERE run_id=?',
                                                ('original-default-run',)).fetchone()['closed_at'])
        self.assertEqual(self.coord.db.execute('SELECT count(*) FROM admin_qualified_evidence WHERE request_id=?',
                                               (request_id,)).fetchone()[0], 2)

    def test_malformed_qualified_current_disposition_is_rejected_without_release(self):
        claim = self.stale(resource='file:malformed-qualified.py')
        self.coord.db.execute('UPDATE resources SET reason=? WHERE name=?',
                              ('OPEN: original refusal retained', claim['resource']))
        with self.assertRaisesRegex(CoordError, 'qualified current disposition evidence is incomplete'):
            self.recover(claim, evidence={'qualified_current_disposition': {'original_reservation_id': claim['reservation_id']}})
        self.assertEqual(self.coord.db.execute('SELECT owner_session FROM resources WHERE name=?',
                                               (claim['resource'],)).fetchone()['owner_session'], 'child')

    def test_active_observation_withholds_and_failed_parent_chain_rejects(self):
        claim = self.stale(resource='file:active.py')
        self.coord.db.execute('INSERT INTO agent_control_observations VALUES(?,?,?,?,?,?,?,?,?,?)',
                              ('child', 'child-thread', 'active', None, 0, None, None, None, '{}', time.time()))
        self.assertEqual(self.recover(claim)['outcome'], 'blocked')
        orphan = self.stale(owner='orphan', resource='file:orphan.py')
        with self.assertRaisesRegex(Exception, 'outside a recorded project orchestrator chain'):
            self.recover(orphan, owner='orphan')

    def test_exact_server_project_is_required(self):
        claim = self.stale(resource='file:project.py')
        with self.assertRaisesRegex(Exception, 'exact server-authenticated project'):
            self.recover(claim, project='other-project')

    def test_remote_dispatch_requires_client_project_to_match_server_selection(self):
        from agent_chat.core import CoordError
        from agent_chat.remote_service import dispatch
        claim = self.stale(resource='file:dispatch-project.py')
        self.coord.db.execute('INSERT INTO remote_session_hosts VALUES(?,?,?,?)',
                              ('root', 'host', time.time(), time.time()))
        params = dict(resource=claim['resource'], reservation_id=claim['reservation_id'],
                      owner_session='child', request_id=str(uuid.uuid4()), operator='operator',
                      reason='approved recovery', evidence={'ticket': 'operator-approved'})
        body = dict(op='admin-recover', session='root', host_id='host', params=params)
        with self.assertRaisesRegex(CoordError, 'exact project, target, audit, and evidence'):
            dispatch(self.coord, body, canonical_project='project-x')
        body['params'] = dict(params, project='other-project')
        with self.assertRaisesRegex(CoordError, 'exact server-authenticated project'):
            dispatch(self.coord, body, canonical_project='project-x')
        body['params'] = dict(params, project='project-x')
        self.assertEqual(dispatch(self.coord, body, canonical_project='project-x')['outcome'], 'cleared')


class AdminRecoveryHttpTests(RemoteWebFixture):
    def test_server_selected_project_is_required_for_admin_recover(self):
        from agent_chat.core import Coordinator
        for session, agent in (('root', 'root'), ('po', 'project-orchestrator'), ('child', 'worker')):
            self.call('register', session=session, agent=agent)
        claim = self.call('request', session='child', resource='file:server-canonical.py', minutes=5)
        with contextlib.closing(Coordinator(self.db)) as coord:
            coord.db.executescript('''
                CREATE TABLE IF NOT EXISTS root_projections (
                    session_id TEXT PRIMARY KEY, home_project TEXT NOT NULL, home_session TEXT NOT NULL,
                    thread_id TEXT NOT NULL, cwd TEXT, paused INTEGER NOT NULL DEFAULT 0, model TEXT, effort TEXT);
                CREATE TABLE IF NOT EXISTS project_orchestrators (session_id TEXT PRIMARY KEY, root_session TEXT NOT NULL);
            ''')
            coord.db.execute('INSERT INTO root_projections VALUES(?,?,?,?,?,?,?,?)',
                             ('root', 'home', 'root', 'root-thread', '/synthetic', 0, None, None))
            coord.db.execute('INSERT INTO project_orchestrators VALUES(?,?)', ('po', 'root'))
            coord.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?,?,?)', ('po', 'po-thread', 'root', '/po', time.time()))
            coord.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?,?,?)', ('child', 'child-thread', 'po', '/po/child', time.time()))
            coord.db.execute('UPDATE resources SET deadline=? WHERE name=?', (time.time() - 1, claim['resource']))
        params = dict(resource=claim['resource'], reservation_id=claim['reservation_id'], owner_session='child',
                      request_id=str(uuid.uuid4()), operator='operator', reason='approved recovery',
                      evidence={'ticket': 'operator-approved'})
        with self.assertRaisesRegex(RemoteCoordError, 'exact project, target, audit, and evidence'):
            self.call('admin-recover', session='root', **params)
        with self.assertRaisesRegex(RemoteCoordError, 'exact server-authenticated project'):
            self.call('admin-recover', session='root', project='other-project', **params)
        self.assertEqual(self.call('admin-recover', session='root', project='default', **params)['outcome'], 'cleared')
