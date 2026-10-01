import base64
import copy
from pathlib import Path
import time
import uuid

from agent_chat.bridge import Bridge
from agent_chat.bridge_client import RemoteBridgeState
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
        self.assertEqual(job['status'], 'completed')
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
