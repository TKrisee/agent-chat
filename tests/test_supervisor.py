"""Owner-scoped supervision through HTTP/CLI and a native metadata fixture."""
import contextlib
import copy
import io
import os
from pathlib import Path
import time
import uuid
from unittest import mock

from agent_chat import cli
from agent_chat.bridge import Bridge
from agent_chat.bridge_client import RemoteBridgeState
from agent_chat.core import Coordinator, CoordError
from agent_chat.remote import RemoteCoordError
from agent_chat.rpc import RpcError, TransportError
from agent_chat.supervisor_worker import SupervisorWorker
from agent_chat.supervisor import SupervisorState
from agent_chat.web import operator_session
from test_agent_control import ControlRpc
from test_remote_web import RemoteWebFixture, TOKEN
import test_root_orchestrator as root_fixture


class MetadataRpc(ControlRpc):
    def __init__(self, target, owner):
        super().__init__(target)
        self.status[owner] = 'idle'
        self.queues[owner], self.history[owner] = [], []
        self.conflicting = False
        self.read_count = 0
        self.wrong_identity = False
        self.lose_resume_result = False
        self.history_unavailable = False

    def request(self, method, params):
        if method == 'thread/turns/list':
            self.calls.append((method, copy.deepcopy(params)))
            if self.history_unavailable:
                raise RpcError(-32601, 'list_turns is not supported yet')
            active = self.status[params['threadId']] == 'active'
            turns = [dict(id='active-turn', status='inProgress')] if active else []
            if self.history[params['threadId']]:
                turns.append(dict(id='consumed-turn', status='completed',
                                  items=copy.deepcopy(self.history[params['threadId']])))
            return dict(data=turns, nextCursor=None)
        result = super().request(method, params)
        if method == 'thread/resume':
            self.status[params['threadId']] = 'idle'
            if self.lose_resume_result:
                self.lose_resume_result = False
                raise TransportError('lost exact resume result')
        if method == 'thread/read':
            self.read_count += 1
            if self.conflicting and self.read_count % 2 == 0:
                result['thread']['status'] = dict(type='active')
            if self.wrong_identity:
                result['thread']['id'] = 'another-thread'
        return result


class SupervisorWorkflowTests(RemoteWebFixture):
    def setUp(self):
        super().setUp()
        self.operator_session = operator_session(self.db)
        self.call('register', agent='worker')
        self.call('register', session='b', agent='orchestrator')
        self.target_thread, self.owner_thread = str(uuid.uuid4()), str(uuid.uuid4())
        self.call('bind', thread=self.target_thread)
        self.call('bind', session='b', thread=self.owner_thread)
        # Isolated authority fixture; live project routing is exercised separately.
        with contextlib.closing(Coordinator(self.db)) as coord, coord.tx() as db:
            db.execute('CREATE TABLE IF NOT EXISTS project_orchestrators (session_id TEXT PRIMARY KEY, root_session TEXT NOT NULL)')
            db.execute('INSERT INTO project_orchestrators VALUES(?,?)', ('b', 'root-session'))
        self.identity = dict(owner='supervisor-fixture', secret='x' * 40, host_id='mac')
        self.state = RemoteBridgeState(self.client, self.url, self.identity)
        self.state.call('acquire')
        self.rpc = MetadataRpc(self.target_thread, self.owner_thread)
        self.bridge = Bridge(self.state, self.rpc)
        self.now = time.time()
        self.clock = mock.patch('agent_chat.supervisor.time.time', side_effect=lambda: self.now)
        self.clock.start()
        self.addCleanup(self.clock.stop)
        self.heartbeat()

    def heartbeat(self):
        self.state.heartbeat(self.url, os.getpid())

    def enable(self, enabled=True):
        return self.call('supervisor-configure', session='b', enabled=enabled)

    def recovery_consent(self, enabled=True):
        self.call('context', session='b')
        return self.call('supervisor-configure', session='b', recovery_enabled=enabled)

    def recovery_policy(self):
        with contextlib.closing(Coordinator(self.db)) as coord:
            return dict(coord.db.execute('SELECT * FROM supervisor_owner_policies WHERE owner_session=?', ('b',)).fetchone())

    def prepared_target(self, message):
        messages = [dict(id=message['id'], recipient_session='a')]
        return self.state.prepare(self.target_thread, messages, self.bridge.payload(self.target_thread, messages))

    def attended_target(self, body):
        result = self.call('send', session='b', to=['worker'], body=body)
        message = result['deliveries'][0]
        with contextlib.closing(Coordinator(self.db)) as coord:
            self.assertIsNotNone(coord.db.execute('SELECT 1 FROM message_batches WHERE message_id=?', (message['id'],)).fetchone())
            self.assertIsNotNone(coord.db.execute('SELECT 1 FROM message_attention WHERE message_id=?', (message['id'],)).fetchone())
        return message

    def status(self, **params):
        return self.call('supervisor-status', session='b', **params)

    def sample(self, advance=0):
        self.now += advance
        self.heartbeat()
        for job in self.state.supervisor_jobs():
            SupervisorWorker(self.bridge).advance(job)
        return self.status()

    def target(self):
        return next(item for item in self.status()['targets'] if item['target_session'] == 'a')

    def notices(self):
        return [item for item in self.call('inbox', session='b', all=True)['messages']
                if item['sender_session'] == 'system:supervisor']

    def assert_passive(self):
        self.assertTrue(self.rpc.calls)
        self.assertTrue(all(method in ('thread/read', 'thread/turns/list', 'thread/queue/list')
                            for method, params in self.rpc.calls), self.rpc.calls)


    def recovery_rows(self):
        with contextlib.closing(Coordinator(self.db)) as coord:
            return [dict(row) for row in coord.db.execute(
                'SELECT * FROM supervisor_recoveries ORDER BY created_at')]

    def resume_calls(self):
        return [params for method, params in self.rpc.calls if method == 'thread/resume']

    def test_recovery_status_authorized_pending_and_consumed_proof(self):
        private_body = 'PRIVATE task payload must not appear in recovery evidence'
        message = self.attended_target(private_body)
        self.enable()
        self.recovery_consent()
        self.assertIsNone(self.target()['recovery'])
        self.rpc.status[self.target_thread] = 'notLoaded'
        self.sample()
        self.sample(61)
        evidence = self.target()['recovery']
        row = self.recovery_rows()[0]
        self.assertEqual(evidence['owner_session'], 'b')
        self.assertEqual(evidence['target_session'], 'a')
        self.assertEqual(evidence['thread_id'], self.target_thread)
        self.assertEqual(evidence['host_id'], 'mac')
        self.assertEqual(evidence['route_generation'], self.target()['route_generation'])
        self.assertEqual(evidence['binding_state'], 'current')
        self.assertEqual(evidence['message_ids'], [message['id']])
        self.assertEqual(evidence['wake_id'], row['wake_id'])
        self.assertEqual(evidence['attempt_state'], 'awaiting_consumption')
        self.assertEqual(evidence['native_consumption'], 'pending')
        self.assertIsNone(evidence['native_consumption_proof'])
        self.assertEqual(evidence['provenance'], 'coordinator-journal')
        self.assertEqual(evidence['created_at'], row['created_at'])
        self.assertEqual(evidence['updated_at'], row['updated_at'])
        self.assertEqual(evidence['incident']['episode'], row['episode'])
        self.assertNotIn(private_body, str(evidence))
        self.assertNotIn(self.identity['secret'], str(evidence))
        self.bridge.tick()
        self.assertEqual(self.state.job(row['wake_id'])['status'], 'dispatched')
        self.assertEqual(self.target()['recovery']['native_consumption'], 'pending')
        self.sample(61)
        evidence = self.target()['recovery']
        self.assertEqual(evidence['native_consumption'], 'consumed')
        self.assertEqual(evidence['native_consumption_proof'], {
            'source': 'native-history', 'recorded_at': self.recovery_rows()[0]['updated_at'],
            'thread_id': self.target_thread, 'host_id': 'mac', 'client_id': row['wake_id']})
        old_thread = self.target_thread
        with contextlib.closing(Coordinator(self.db)) as coord, coord.tx() as db:
            db.execute('UPDATE bridge_bindings SET thread_id=? WHERE session_id=?',
                       (str(uuid.uuid4()), 'a'))
        evidence = self.target()['recovery']
        self.assertEqual(evidence['binding_state'], 'stale_route')
        self.assertEqual(evidence['native_consumption'], 'consumed')
        self.assertEqual(evidence['native_consumption_proof']['thread_id'], old_thread)
        self.assertNotEqual(self.target()['thread_id'], old_thread)

    def test_recovery_status_foreign_owner_is_rejected_and_hidden(self):
        self.attended_target('Only the authorized owner sees recovery identifiers')
        self.enable()
        self.recovery_consent()
        self.rpc.status[self.target_thread] = 'notLoaded'
        self.sample()
        self.sample(61)
        evidence = self.target()['recovery']
        with self.assertRaises(RemoteCoordError):
            self.call('supervisor-status', session='a', to='worker')
        with contextlib.closing(Coordinator(self.db)) as coord, coord.tx() as db:
            db.execute('''INSERT INTO supervisor_recoveries
                (id,owner_session,target_session,route_generation,episode,thread_id,host_id,status,
                 message_ids_json,wake_id,native_consumed,created_at,updated_at)
                SELECT ?,?,target_session,route_generation,episode,thread_id,host_id,status,
                 message_ids_json,wake_id,native_consumed,created_at+1,updated_at
                FROM supervisor_recoveries WHERE id=?''', ('foreign-attempt', 'a', evidence['id']))
        visible = self.target()['recovery']
        self.assertEqual(visible['id'], evidence['id'])
        self.assertNotIn('foreign-attempt', str(self.status()))

    def test_recovery_status_uncertain_is_unresolved_without_native_proof(self):
        self.attended_target('Lost native resume result remains unresolved')
        self.enable()
        self.recovery_consent()
        self.rpc.status[self.target_thread] = 'notLoaded'
        self.rpc.lose_resume_result = True
        self.sample()
        self.sample(61)
        evidence = self.target()['recovery']
        self.assertEqual(evidence['attempt_state'], 'uncertain')
        self.assertEqual(evidence['native_consumption'], 'unresolved')
        self.assertIsNone(evidence['native_consumption_proof'])

    def test_recovery_status_stale_pending_route_is_unresolved(self):
        self.attended_target('Pending proof belongs only to the original thread')
        self.enable()
        self.recovery_consent()
        self.rpc.status[self.target_thread] = 'notLoaded'
        self.sample()
        self.sample(61)
        evidence = self.target()['recovery']
        self.assertEqual(evidence['native_consumption'], 'pending')
        old_thread, old_generation = evidence['thread_id'], evidence['route_generation']
        with contextlib.closing(Coordinator(self.db)) as coord, coord.tx() as db:
            db.execute('UPDATE bridge_bindings SET thread_id=? WHERE session_id=?',
                       (str(uuid.uuid4()), 'a'))
        evidence = self.target()['recovery']
        self.assertEqual(evidence['thread_id'], old_thread)
        self.assertEqual(evidence['route_generation'], old_generation)
        self.assertEqual(evidence['binding_state'], 'stale_route')
        self.assertEqual(evidence['incident']['state'], 'suppressed')
        self.assertEqual(evidence['native_consumption'], 'unresolved')
        self.assertIsNone(evidence['native_consumption_proof'])


    def test_internal_recovery_requires_native_consumption_and_deduplicates(self):
        older_source = self.call('send', session='b', to='a',
                                 body='Independent source message')
        message = self.attended_target('One existing task')
        other = self.attended_target('A second existing task')
        self.enable()
        self.recovery_consent()
        deliveries = [dict(id=item['id'], recipient_session='a') for item in (message, other)]
        prepared = self.state.prepare(self.target_thread, deliveries,
                                      self.bridge.payload(self.target_thread, deliveries))
        self.rpc.status[self.target_thread] = 'notLoaded'
        self.sample()
        self.assertEqual(self.resume_calls(), [])
        self.sample(61)
        self.assertEqual(self.resume_calls(), [{'threadId': self.target_thread, 'excludeTurns': True}])
        row = self.recovery_rows()[0]
        self.assertEqual(row['wake_id'], prepared['id'])
        self.assertEqual(row['status'], 'awaiting_consumption')
        self.assertEqual(row['native_consumed'], 0)
        self.assertEqual(self.rpc.history[self.target_thread], [])
        self.assertEqual(self.call('message', id=message['id'])['message']['acked_at'], None)
        self.state = RemoteBridgeState(self.client, self.url, self.identity)
        self.state.call('acquire')
        self.bridge = Bridge(self.state, self.rpc)
        self.sample(61)
        self.assertEqual(len(self.resume_calls()), 1)
        self.assertEqual(self.recovery_rows()[0]['native_consumed'], 0)
        self.bridge.tick()  # Existing ordinary prepared job; no recovery queue/start path.
        self.sample(61)
        self.assertEqual(self.recovery_rows()[0]['status'], 'completed')
        self.assertEqual(self.recovery_rows()[0]['native_consumed'], 1)
        evidence = self.target()['recovery']
        self.assertEqual(evidence['source_reporting'], {
            'state': 'awaiting_acknowledged_reply', 'delivered_count': 2})
        self.assertEqual(evidence['source_reporting_obligation'], {
            'state': 'active', 'notice_sent': False})
        self.call('acknowledge', session='a', id=older_source['id'])
        self.call('send', session='a', to='b', body='Reply to independent source.',
                  reply_to=older_source['id'], ack_reply=True)
        self.assertEqual(self.target()['recovery']['source_reporting']['state'],
                         'awaiting_acknowledged_reply')
        self.call('acknowledge', session='a', id=message['id'])
        self.call('register', session='c', agent='third-party')
        with self.assertRaisesRegex(RemoteCoordError, 'reply'):
            self.call('send', session='a', to='c', body='Reply delivered elsewhere.',
                      reply_to=message['id'], ack_reply=True)
        self.assertEqual(self.target()['recovery']['source_reporting']['state'],
                         'awaiting_acknowledged_reply')
        self.call('acknowledge', session='a', id=message['id'])
        self.call('send', session='a', to='b', body='Linked, but not acknowledged.', reply_to=other['id'])
        self.assertEqual(self.target()['recovery']['source_reporting']['state'],
                         'awaiting_acknowledged_reply')
        self.sample(61)
        self.assertEqual(len([item for item in self.notices()
                              if 'source_report_pending' in item['body']]), 1)
        first_reply = self.call('send', session='a', to='b', body='First completed.', reply_to=message['id'], ack_reply=True)
        self.assertEqual(self.target()['recovery']['source_reporting']['state'],
                         'awaiting_acknowledged_reply')
        retry_reply = self.call('send', session='a', to='b', body='First completed.',
                                reply_to=message['id'], ack_reply=True)
        self.assertEqual(retry_reply['id'], first_reply['id'])
        self.assertEqual(self.target()['recovery']['source_reporting']['state'],
                         'awaiting_acknowledged_reply')
        self.call('send', session='a', to='b', body='Second completed.', reply_to=other['id'], ack_reply=True)
        self.assertEqual(self.target()['recovery']['source_reporting']['state'], 'reported')
        self.assertEqual(self.target()['recovery']['source_reporting_obligation'], {
            'state': 'active', 'notice_sent': True})
        self.sample(61)
        self.assertEqual(self.target()['recovery']['source_reporting_obligation'], {
            'state': 'resolved', 'notice_sent': True})
        consumed = [item['clientId'] for item in self.rpc.history[self.target_thread]]
        self.assertEqual(consumed, [row['wake_id']])
        self.bridge.tick()
        self.sample(61)
        self.assertEqual(len(self.resume_calls()), 1)
        self.assertEqual([item['clientId'] for item in self.rpc.history[self.target_thread]], consumed)
        self.assertFalse(any(method in ('thread/start', 'turn/start', 'turn/steer') for method, _ in self.rpc.calls))

    def test_internal_recovery_changed_binding_withholds_resume(self):
        self.call('send', session='b', to='worker', body='Only the original exact thread')
        self.enable()
        self.recovery_consent()
        self.rpc.status[self.target_thread] = 'notLoaded'
        self.sample()
        original_observe = self.state.supervisor_observe
        changed = str(uuid.uuid4())
        def observe(job, report):
            if report['state'] == 'recovery_attempt':
                with contextlib.closing(Coordinator(self.db)) as coord, coord.tx() as db:
                    db.execute('UPDATE bridge_bindings SET thread_id=? WHERE session_id=?', (changed, 'a'))
            return original_observe(job, report)
        with mock.patch.object(self.state, 'supervisor_observe', side_effect=observe):
            with self.assertRaises(RemoteCoordError):
                self.sample(61)
        self.assertEqual(self.resume_calls(), [])
        self.assertEqual(self.recovery_rows()[0]['native_consumed'], 0)

    def test_internal_recovery_lost_resume_result_never_repeats_attempt(self):
        self.call('send', session='b', to='worker', body='Existing delivery only')
        self.enable()
        self.recovery_consent()
        self.rpc.status[self.target_thread] = 'notLoaded'
        self.rpc.lose_resume_result = True
        self.sample()
        self.sample(61)
        self.assertEqual(self.recovery_rows()[0]['status'], 'uncertain')
        self.assertEqual(len(self.resume_calls()), 1)
        self.bridge.tick()
        self.sample(61)
        self.assertEqual(self.recovery_rows()[0]['status'], 'completed')
        self.assertEqual(len(self.resume_calls()), 1)

    def test_internal_recovery_dispatched_delivery_and_paused_owner_are_withheld(self):
        message = self.call('send', session='b', to='worker', body='Already consumed once')
        self.bridge.tick()
        self.assertTrue(self.rpc.history[self.target_thread])
        self.enable()
        self.recovery_consent()
        self.rpc.status[self.target_thread] = 'notLoaded'
        self.sample()
        self.sample(61)
        self.assertEqual(self.resume_calls(), [])
        self.assertEqual(self.recovery_rows(), [])
        self.assertEqual(self.call('message', id=message['id'])['message']['acked_at'], None)
        self.call('send', session='b', to='worker', body='Paused owner pending input')
        with contextlib.closing(Coordinator(self.db)) as coord, coord.tx() as db:
            db.execute('CREATE TABLE IF NOT EXISTS root_projections(session_id TEXT PRIMARY KEY,paused INTEGER)')
            db.execute('INSERT INTO root_projections VALUES(?,1)', ('b',))
            db.execute('INSERT INTO project_orchestrators VALUES(?,?)', ('a', 'b'))
        self.sample(61)
        self.sample(61)
        self.assertEqual(self.resume_calls(), [])
        self.assertEqual(self.recovery_rows(), [])

    def test_internal_recovery_disabled_and_loaded_idle_keep_single_task_behavior(self):
        self.call('send', session='b', to='worker', body='Normal single task')
        self.rpc.status[self.target_thread] = 'notLoaded'
        self.sample()
        self.sample(61)
        self.assertEqual(self.resume_calls(), [])
        self.assertEqual(self.recovery_rows(), [])
        self.rpc.status[self.target_thread] = 'idle'
        self.bridge.tick()
        self.bridge.tick()
        self.assertEqual(len(self.rpc.history[self.target_thread]), 1)
        self.assertEqual(self.resume_calls(), [])

    def test_recovery_consent_is_independent_durable_and_revocable(self):
        self.call('send', session='b', to='worker', body='Explicit consent is required')
        self.enable()
        self.assertEqual(self.recovery_policy()['recovery_enabled'], 0)
        self.rpc.status[self.target_thread] = 'notLoaded'
        self.sample()
        self.sample(61)
        self.assertEqual(self.resume_calls(), [])
        self.assertEqual(self.recovery_rows(), [])
        self.recovery_consent()
        self.assertEqual(self.recovery_policy()['recovery_enabled'], 1)
        self.enable(False)
        self.assertEqual(self.recovery_policy()['enabled'], 0)
        self.assertEqual(self.recovery_policy()['recovery_enabled'], 1)
        self.sample(61)
        self.assertEqual(self.resume_calls(), [])
        self.enable()
        self.recovery_consent(False)
        self.sample(61)
        self.assertEqual(self.resume_calls(), [])
        self.assertEqual(self.recovery_rows(), [])
        self.recovery_consent()
        self.sample(61)
        self.assertEqual(len(self.resume_calls()), 1)
        self.assertEqual(self.recovery_rows()[0]['native_consumed'], 0)

    def test_recovery_consent_migrates_off_and_checks_owner_authority(self):
        self.enable()
        with contextlib.closing(Coordinator(self.db)) as coord, coord.tx() as db:
            db.execute('DROP TABLE supervisor_owner_policies')
            db.execute('CREATE TABLE supervisor_owner_policies(owner_session TEXT PRIMARY KEY,enabled INTEGER NOT NULL,updated_at REAL NOT NULL)')
            db.execute('INSERT INTO supervisor_owner_policies VALUES(?,1,?)', ('b', self.now))
        with contextlib.closing(Coordinator(self.db, session='b')) as coord:
            SupervisorState(coord)
        self.assertEqual(self.recovery_policy()['enabled'], 1)
        self.assertEqual(self.recovery_policy()['recovery_enabled'], 0)
        self.recovery_consent()
        with contextlib.closing(Coordinator(self.db, session='b')) as coord:
            state = SupervisorState(coord)
            self.assertEqual(self.recovery_policy()['recovery_enabled'], 1)
            with self.assertRaises(CoordError):
                state._configure_recovery(1)
        with contextlib.closing(Coordinator(self.db, session='a')) as coord:
            with self.assertRaises(CoordError):
                SupervisorState(coord)._configure_recovery(True)
        self.assertEqual(self.recovery_policy()['recovery_enabled'], 1)

    def test_recovery_consent_withdrawn_between_claim_and_attempt_withholds_resume(self):
        self.call('send', session='b', to='worker', body='Do not resume after withdrawal')
        self.enable()
        self.recovery_consent()
        self.rpc.status[self.target_thread] = 'notLoaded'
        self.sample()
        original_observe = self.state.supervisor_observe
        def observe(job, report):
            if report['state'] == 'recovery_attempt':
                self.recovery_consent(False)
            return original_observe(job, report)
        with mock.patch.object(self.state, 'supervisor_observe', side_effect=observe):
            self.sample(61)
        self.assertEqual(self.resume_calls(), [])
        self.assertEqual(self.recovery_rows()[0]['native_consumed'], 0)
        self.recovery_consent()
        self.sample(121)
        self.assertEqual(self.resume_calls(), [])  # The incomplete claim is not replayed.

    def test_prepared_recovery_withdrawn_attention_is_ineligible_at_claim(self):
        message = self.attended_target('Withdraw before recovery claim')
        self.enable()
        self.recovery_consent()
        self.prepared_target(message)
        with contextlib.closing(Coordinator(self.db)) as coord, coord.tx() as db:
            db.execute('DELETE FROM message_attention WHERE message_id=?', (message['id'],))
        self.rpc.status[self.target_thread] = 'notLoaded'
        self.sample()
        self.sample(61)
        self.assertEqual(self.resume_calls(), [])
        self.assertEqual(self.recovery_rows(), [])
        self.assertEqual(self.rpc.history[self.target_thread], [])

    def test_prepared_recovery_attention_withdrawn_before_attempt_is_not_resumed(self):
        message = self.attended_target('Withdraw after recovery claim')
        self.enable()
        self.recovery_consent()
        self.prepared_target(message)
        self.rpc.status[self.target_thread] = 'notLoaded'
        self.sample()
        original_observe = self.state.supervisor_observe
        def observe(job, report):
            if report['state'] == 'recovery_attempt':
                with contextlib.closing(Coordinator(self.db)) as coord, coord.tx() as db:
                    db.execute('DELETE FROM message_attention WHERE message_id=?', (message['id'],))
            return original_observe(job, report)
        with mock.patch.object(self.state, 'supervisor_observe', side_effect=observe):
            self.sample(61)
        self.assertEqual(self.resume_calls(), [])
        self.assertEqual(self.recovery_rows()[0]['native_consumed'], 0)
        self.sample(121)
        self.assertEqual(self.resume_calls(), [])
        self.assertEqual(self.rpc.history[self.target_thread], [])


    def test_supported_configure_updates_only_supplied_bits_and_validates_atomically(self):
        self.assertFalse(self.status()['enabled'])
        self.assertFalse(self.status()['recovery_enabled'])
        result = self.call('supervisor-configure', session='b', recovery_enabled=True)
        self.assertFalse(result['enabled'])
        self.assertTrue(result['recovery_enabled'])
        result = self.enable()
        self.assertTrue(result['enabled'])
        self.assertTrue(result['recovery_enabled'])
        result = self.recovery_consent(False)
        self.assertTrue(result['enabled'])
        self.assertFalse(result['recovery_enabled'])
        result = self.enable(False)
        self.assertFalse(result['enabled'])
        self.assertFalse(result['recovery_enabled'])
        result = self.recovery_consent(True)
        self.assertFalse(result['enabled'])
        self.assertTrue(result['recovery_enabled'])
        with contextlib.closing(Coordinator(self.db, session='b')) as coord:
            state = SupervisorState(coord)
            with self.assertRaises(CoordError):
                state.configure(enabled=True, recovery_enabled=None)
            with self.assertRaises(CoordError):
                state.configure()
            persisted = state.status()
        self.assertFalse(persisted['enabled'])
        self.assertTrue(persisted['recovery_enabled'])

    def test_cli_opt_in_and_healthy_status_do_not_start_inference(self):
        self.assertFalse(self.status()['enabled'])
        self.assertEqual(self.state.supervisor_jobs(), [])
        args = ['--server', self.url, '--session', 'b', 'supervisor-configure', '--enable']
        with mock.patch.dict(os.environ, dict(AGENT_CHAT_API_TOKEN=TOKEN), clear=True), mock.patch('agent_chat.remote.client_host_id', return_value='mac'):
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(cli.main(args), 0)
        self.sample()
        self.assertTrue(self.status()['enabled'])
        self.assertEqual(self.target()['thread_id'], self.target_thread)
        self.assertEqual(self.target()['observation']['state'], 'idle')
        self.assertTrue(self.target()['observation']['summary']['direct_input'])
        self.assertEqual(self.notices(), [])
        self.assert_passive()
        with self.assertRaises(RemoteCoordError):
            self.call('supervisor-configure', enabled=True)
        with self.assertRaises(RemoteCoordError):
            self.call('supervisor-status', to='orchestrator')
        self.assertNotIn(self.operator_session, [row['target_session'] for row in self.status()['targets']])

    def test_active_queued_and_child_routes_are_not_failure_triggers(self):
        self.call('register', session='child', agent='audit')
        self.call('bind', session='child', parent_session='a', agent_path='/root/audit')
        self.enable()
        self.recovery_consent()
        self.rpc.status[self.target_thread] = 'active'
        self.sample()
        self.sample(61)
        self.assertEqual(self.notices(), [])
        self.assertEqual(self.resume_calls(), [])
        self.assertEqual(self.recovery_rows(), [])
        self.rpc.status[self.target_thread] = 'idle'
        entry = dict(id='existing-input', status='queued')
        self.rpc.queues[self.target_thread].append(entry)
        self.sample(61)
        self.sample(61)
        self.assertEqual(self.rpc.queues[self.target_thread], [entry])
        self.assertEqual(self.notices(), [])
        child = next(item for item in self.status()['targets'] if item['target_session'] == 'child')
        self.assertEqual(child['observation']['state'], 'route_only_inconclusive')
        self.assert_passive()

    def test_history_unavailable_is_actionable_and_withholds_recovery(self):
        self.attended_target('Do not infer an empty history from unsupported RPC')
        self.enable()
        self.recovery_consent()
        self.rpc.status[self.target_thread] = 'notLoaded'
        self.rpc.history_unavailable = True
        self.sample()
        self.sample(61)
        observation = self.target()['observation']
        self.assertEqual(observation['state'], 'history_unavailable')
        self.assertEqual(observation['summary']['history_state'], 'unavailable')
        self.assertEqual(observation['summary']['history_error']['code'], -32601)
        self.assertEqual(self.resume_calls(), [])
        self.assertEqual(self.recovery_rows(), [])
        self.assertEqual(self.notices(), [])

    def test_unloaded_pending_alert_deduplicates_and_preserves_work_and_ownership(self):
        hold = self.call('request', resource='owned-work', minutes=10)
        queued = self.call('request', session='b', resource='owned-work', minutes=10)
        message = self.call('send', session='b', to='worker', body='Existing bounded task')
        self.enable()
        self.rpc.status[self.target_thread] = 'notLoaded'
        self.sample()
        self.assertEqual(self.notices(), [])
        self.sample(61)
        self.assertEqual(len(self.notices()), 1)
        notice = self.notices()[0]
        self.assertEqual(self.call('message', session='b', id=notice['id'])['message']['id'], notice['id'])
        with self.assertRaises(RemoteCoordError):
            self.call('message', id=notice['id'])
        self.assertIn(self.target_thread, notice['body'])
        self.assertEqual(self.call('message', id=message['id'])['message']['acked_at'], None)
        self.assertEqual(self.call('status', mine=True)['resources'][0]['reservation_id'], hold['reservation_id'])
        self.sample(61)
        self.state = RemoteBridgeState(self.client, self.url, self.identity)
        self.state.call('acquire')
        self.bridge = Bridge(self.state, self.rpc)
        self.sample(61)
        self.assertEqual([item['id'] for item in self.notices()], [notice['id']])
        self.assertEqual(self.rpc.queues[self.target_thread], [])
        self.assertEqual(self.resume_calls(), [])
        self.assert_passive()
        # Passive monitoring dispatches only its notice, not the unloaded task.
        self.bridge.tick()
        self.bridge.tick()
        additions = [params for method, params in self.rpc.calls if method == 'thread/queue/add']
        self.assertEqual(len(additions), 1)
        self.assertEqual(additions[0]['threadId'], self.owner_thread)

    def test_disable_and_route_change_refuse_stale_host_reports(self):
        self.enable()
        job = next(item for item in self.state.supervisor_jobs() if item['target_session'] == 'a')
        self.enable(False)
        with self.assertRaises(RemoteCoordError):
            self.state.supervisor_observe(job, dict(state='idle', source='native', summary={}))
        self.enable()
        job = next(item for item in self.state.supervisor_jobs() if item['target_session'] == 'a')
        self.call('unbind')
        self.call('bind', thread=str(uuid.uuid4()))
        with self.assertRaises(RemoteCoordError):
            self.state.supervisor_observe(job, dict(state='idle', source='native', summary={}))
        self.assertEqual(self.notices(), [])

    def test_recovery_resolves_episode_and_later_failure_gets_one_new_notice(self):
        self.call('send', session='b', to='worker', body='Retained pending input')
        self.enable()
        self.rpc.status[self.target_thread] = 'notLoaded'
        self.sample()
        self.sample(61)
        first = self.notices()[0]['id']
        self.rpc.status[self.target_thread] = 'idle'
        self.sample(61)
        self.assertEqual(len(self.notices()), 1)
        self.assertTrue(any(item['state'] == 'resolved' for item in self.target()['incidents']))
        self.rpc.status[self.target_thread] = 'notLoaded'
        self.sample(61)
        self.assertEqual(len(self.notices()), 1)
        self.sample(61)
        self.assertEqual(len(self.notices()), 2)
        self.assertNotEqual(first, self.notices()[1]['id'])
        self.sample(61)
        self.assertEqual(len(self.notices()), 2)
        self.assertEqual(self.resume_calls(), [])
        self.assert_passive()

    def test_enabled_owner_scope_discovers_new_worker_and_disable_covers_it(self):
        self.enable()
        self.call('register', session='new-worker', agent='new-worker')
        thread = str(uuid.uuid4())
        self.call('bind', session='new-worker', thread=thread)
        self.rpc.status[thread] = 'idle'
        self.rpc.queues[thread], self.rpc.history[thread] = [], []
        self.sample()
        status = self.status(to='new-worker')
        self.assertTrue(status['enabled'])
        self.assertEqual(status['targets'][0]['thread_id'], thread)
        self.assertEqual(status['targets'][0]['observation']['state'], 'idle')
        self.enable(False)
        self.assertEqual(self.state.supervisor_jobs(), [])
        self.assertFalse(self.status()['enabled'])
        self.assertEqual(self.notices(), [])
        with self.assertRaises(RemoteCoordError):
            self.call('register', session='system:supervisor', agent='impersonator')

    def test_gated_target_and_revoked_owner_do_not_generate_alerts(self):
        self.call('send', session='b', to='worker', body='Pending input before explicit owner stop')
        self.enable()
        self.recovery_consent()
        with contextlib.closing(Coordinator(self.db)) as coord, coord.tx() as db:
            db.execute('UPDATE bridge_bindings SET parent_session=?,agent_path=? WHERE session_id=?',
                       ('b', '/fixture/worker', 'a'))
        self.call('agent-stop', session='b', to='worker', expected_thread=self.target_thread,
                  request_id=str(uuid.uuid4()), confirm=True)
        self.rpc.status[self.target_thread] = 'notLoaded'
        self.sample()
        self.sample(61)
        self.assertEqual(self.notices(), [])
        self.assertEqual(self.resume_calls(), [])
        self.assertEqual(self.recovery_rows(), [])
        with contextlib.closing(Coordinator(self.db)) as coord, coord.tx() as db:
            db.execute('DELETE FROM project_orchestrators WHERE session_id=?', ('b',))
        self.now += 61
        self.heartbeat()
        self.assertEqual(self.state.supervisor_jobs(), [])
        with self.assertRaises(RemoteCoordError):
            self.status()
        self.assertEqual(self.notices(), [])

    def test_incoherent_native_evidence_remains_inconclusive(self):
        self.enable()
        self.rpc.conflicting = True
        self.sample()
        self.sample(61)
        self.assertEqual(self.target()['observation']['state'], 'inconclusive')
        self.assertEqual(self.notices(), [])
        self.rpc.conflicting = False
        self.rpc.wrong_identity = True
        self.sample(61)
        self.assertEqual(self.target()['observation']['state'], 'inconclusive')
        self.assertEqual(self.call('agent-status')['thread_id'], self.target_thread)
        self.assert_passive()

    def test_uncertain_wake_alert_does_not_replay_work(self):
        message = self.call('send', session='b', to='worker', body='Existing task with ambiguous dispatch')
        payload = self.bridge.payload(self.target_thread, [dict(id=message['id'], recipient_session='a')])
        wake = self.state.prepare(self.target_thread, [dict(id=message['id'], recipient_session='a')], payload)
        self.state.update(wake['id'], 'uncertain', error='lost native result')
        self.enable()
        self.recovery_consent()
        self.sample()
        self.sample(61)
        self.assertEqual(len(self.notices()), 1)
        self.assertEqual(self.state.jobs()[0]['id'], wake['id'])
        self.assertEqual(self.state.jobs()[0]['status'], 'uncertain')
        self.assertEqual(self.rpc.queues[self.target_thread], [])
        self.assert_passive()

    def test_unloaded_self_note_is_not_an_undelivered_task(self):
        self.call('send', to='worker', body='Private self note, no model wake requested')
        self.enable()
        self.rpc.status[self.target_thread] = 'notLoaded'
        self.sample()
        self.sample(61)
        self.assertEqual(self.notices(), [])
        self.assertEqual(self.target()['observation']['state'], 'notLoaded')
        self.assert_passive()

    def revoke_owner(self):
        with contextlib.closing(Coordinator(self.db)) as coord, coord.tx() as db:
            db.execute('DELETE FROM project_orchestrators WHERE session_id=?', ('b',))

    def test_revoked_owner_withdraws_only_unsent_system_notice(self):
        self.call('send', session='b', to='worker', body='Existing work stays untouched')
        self.enable()
        self.rpc.status[self.target_thread] = 'notLoaded'
        self.sample()
        self.sample(61)
        self.assertEqual(len(self.notices()), 1)
        self.revoke_owner()
        self.assertEqual(self.state.supervisor_jobs(), [])
        self.assertEqual(self.notices(), [])
        self.assertEqual(len(self.call('inbox', all=True)['messages']), 1)

    def test_revoked_owner_preserves_uncertain_notice_delivery(self):
        self.call('send', session='b', to='worker', body='Existing input')
        self.enable()
        self.rpc.status[self.target_thread] = 'notLoaded'
        self.sample()
        self.sample(61)
        notice = self.notices()[0]
        messages = [dict(id=notice['id'], recipient_session='b')]
        wake = self.state.prepare(self.owner_thread, messages, self.bridge.payload(self.owner_thread, messages))
        self.state.update(wake['id'], 'uncertain', error='unknown native result')
        self.revoke_owner()
        self.assertEqual(self.state.supervisor_jobs(), [])
        self.assertEqual([row['id'] for row in self.notices()], [notice['id']])
        retained = next(row for row in self.state.jobs() if row['id'] == wake['id'])
        self.assertEqual(retained['status'], 'uncertain')

    def test_rebound_target_marks_old_sample_stale_and_samples_new_route_immediately(self):
        self.enable()
        self.sample()
        self.assertEqual(self.target()['observation']['state'], 'idle')
        new_thread = str(uuid.uuid4())
        self.call('unbind')
        self.call('bind', thread=new_thread)
        self.rpc.status[new_thread] = 'idle'
        self.rpc.queues[new_thread], self.rpc.history[new_thread] = [], []
        self.assertEqual(self.target()['observation']['state'], 'stale_route')
        self.sample()
        target = self.target()
        self.assertEqual(target['thread_id'], new_thread)
        self.assertEqual(target['observation']['state'], 'idle')
        self.assertEqual(target['observation']['summary']['thread_id'], new_thread)
        self.assertEqual(self.notices(), [])

    def test_offline_target_host_alert_is_owner_scoped_without_fake_native_reads(self):
        self.call('register', session='remote', agent='remote-worker', host='other')
        self.call('bind', session='remote', thread=str(uuid.uuid4()), host='other')
        self.enable()
        self.sample()
        self.sample(61)
        remote = next(item for item in self.status()['targets'] if item['target_session'] == 'remote')
        self.assertEqual(remote['host_id'], 'other')
        self.assertEqual(len(self.notices()), 1)
        self.assertTrue(all(params.get('threadId') != remote['thread_id'] for method, params in self.rpc.calls))
        self.assert_passive()


class RootSupervisorRoutingTests(RemoteWebFixture):
    root = root_fixture.RootOrchestratorTests.root
    project = root_fixture.RootOrchestratorTests.project
    routed = root_fixture.RootOrchestratorTests.routed
    configure = root_fixture.RootOrchestratorTests.configure
    project_bridge = root_fixture.RootOrchestratorTests.project_bridge

    def setUp(self):
        super().setUp()
        self.call('register', agent='root')
        self.thread_id = str(uuid.uuid4())
        self.call('bind', thread=self.thread_id)
        self.neutral = Path(self.tmp.name).resolve() / 'neutral'
        self.neutral.mkdir()
        self.workspace = Path(self.tmp.name).resolve() / 'project'
        self.workspace.mkdir()
        self.rpc = root_fixture.ScopedRpc(self.thread_id, str(self.neutral))
        self.identity = dict(owner='root-supervisor-fixture', secret='x' * 40, host_id='mac')
        self.state = RemoteBridgeState(self.client, self.url, self.identity)
        self.state.call('acquire')
        self.bridge = Bridge(self.state, self.rpc)
        self.call('agent-status', refresh=True, expected_thread=self.thread_id, request_id=str(uuid.uuid4()))
        self.bridge.tick()
        self.root('enroll', expected_thread=self.thread_id, confirm=True)

    def test_root_cli_monitors_only_designated_orchestrator(self):
        project = self.project()
        self.configure(project)
        state, bridge = self.project_bridge(project)
        bridge.tick()
        target = self.routed(project, 'session-reset-status', to='orchestrator')['thread_id']
        self.rpc.status[target] = 'idle'
        baseline = copy.deepcopy(self.rpc.started[target])
        args = ['--server', self.url, '--session', 'a', 'root', 'in', project, 'supervisor-configure', '--enable']
        with mock.patch.dict(os.environ, dict(AGENT_CHAT_API_TOKEN=TOKEN), clear=True), mock.patch('agent_chat.remote.client_host_id', return_value='mac'):
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(cli.main(args), 0)
        status = self.routed(project, 'supervisor-status')
        self.assertTrue(status['enabled'])
        self.assertEqual([row['thread_id'] for row in status['targets']], [target])
        with self.assertRaises(RemoteCoordError):
            self.routed(project, 'supervisor-status', to='root')
        self.assertEqual(self.rpc.started[target], baseline)
