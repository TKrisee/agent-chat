import json
import uuid
from unittest import mock

from agent_chat.bridge import Bridge, exclusive_bridge
from agent_chat.bridge_client import RemoteBridgeState
from agent_chat.bridge_lease import BridgeManager
from agent_chat.core import CoordError, Coordinator
from agent_chat.remote import RemoteCoordError
from agent_chat.rpc import TransportError
from test_bridge import FakeRpc
from test_remote_web import RemoteWebFixture


class RemoteBridgeTests(RemoteWebFixture):
    def setUp(self):
        super().setUp()
        self.call('register', agent='alpha')
        self.tid = str(uuid.uuid4())
        self.call('bind', thread=self.tid)
        self.identity = {'owner': 'test-worker', 'secret': 'x'*40, 'host_id': 'mac'}
        self.state = RemoteBridgeState(self.client, self.url, self.identity)
        self.state.call('acquire')
        self.rpc = FakeRpc()
        self.bridge = Bridge(self.state, self.rpc)

    def send_operator(self, to='a'):
        c = Coordinator(self.db, self.server.sender_session)
        try: return c.send(to, 'Please work on this')['id']
        finally: c.close()

    def test_operator_wake_roundtrip_reuses_real_bridge_engine(self):
        message = self.send_operator()
        self.bridge.tick()
        self.assertEqual([m for m,p in self.rpc.calls].count('thread/queue/start'), 1)
        result = self.call('bridge-status')
        self.assertEqual(result['jobs'][0]['status'], 'dispatched')
        self.assertIsNone(self.call('inbox')['messages'][0]['acked_at'])
        payload = next(p for m,p in self.rpc.calls if m == 'thread/queue/add')['input'][0]['text']
        self.assertIn(self.url, payload)
        self.assertIn(message, payload)
        self.assertNotIn('database', json.loads(payload.split('\n')[-1]))

    def test_two_workers_local_dispatch_and_recovery_cannot_overlap(self):
        other = RemoteBridgeState(self.client, self.url, dict(self.identity, owner='another', secret='z'*40))
        with self.assertRaises(RemoteCoordError): other.call('acquire')
        with self.assertRaises(CoordError):
            with exclusive_bridge(self.db): pass
        self.state.call('release')
        other.call('acquire')
        with self.assertRaises(RemoteCoordError): self.state.pending()
        other.call('release')

    def test_lost_codex_start_response_reconciles_without_second_start(self):
        self.send_operator()
        self.rpc.drop_start = True
        with self.assertRaises(TransportError): self.bridge.tick()
        self.rpc.drop_start = False
        self.bridge.tick()
        self.assertEqual([m for m,p in self.rpc.calls].count('thread/queue/start'), 1)
        self.assertEqual(self.call('bridge-status')['jobs'][0]['status'], 'dispatched')

    def test_lost_http_write_response_reconciles_durable_intent(self):
        self.send_operator()
        original = self.state.update
        def lose_response(job_id, status, queue_id=None, error=None):
            original(job_id, status, queue_id, error)
            if status == 'starting': raise RemoteCoordError('response lost')
        with mock.patch.object(self.state, 'update', side_effect=lose_response):
            with self.assertRaises(RemoteCoordError): self.bridge.tick()
        self.bridge.tick()
        self.assertEqual([m for m,p in self.rpc.calls].count('thread/queue/add'), 1)
        self.assertEqual([m for m,p in self.rpc.calls].count('thread/queue/start'), 1)

    def test_server_restart_retains_dispatcher_until_same_identity_returns(self):
        self.server.bridge_manager.close()
        replacement = BridgeManager(self.db)
        self.server.bridge_manager = replacement
        with self.assertRaises(RemoteCoordError):
            RemoteBridgeState(self.client, self.url, dict(self.identity, owner='other')).call('acquire')
        self.state.call('acquire')
        self.assertEqual(self.state.jobs(), [])

    def test_child_routes_keep_parent_chain_and_other_host_cannot_dispatch(self):
        self.call('register', session='child', agent='alpha/child')
        self.call('bind', session='child', parent_session='a', agent_path='/root/child')
        self.send_operator('child')
        self.bridge.tick()
        payload = next(p for m,p in self.rpc.calls if m == 'thread/queue/add')['input'][0]['text']
        self.assertIn('/root/child', payload)
        self.state.call('release')
        wrong = RemoteBridgeState(self.client, self.url, dict(self.identity, owner='other', host_id='other-mac'))
        wrong.call('acquire')
        self.assertEqual(wrong.pending(), [])
        with self.assertRaises(RemoteCoordError): wrong.call('job', job_id=self.call('bridge-status')['jobs'][0]['id'])

    def test_malformed_bridge_requests_cannot_reassign_deliveries(self):
        self.send_operator()
        with self.assertRaises(RemoteCoordError):
            self.state.prepare(self.tid, [{'id': 'fake', 'recipient_session': 'a'}], 'fake')
        self.assertEqual(self.state.jobs(), [])
        with self.assertRaises(RemoteCoordError):
            self.state.call('update', job_id='fake', status='dispatched')
