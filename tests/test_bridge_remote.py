import json
import hashlib
from pathlib import Path
import tempfile
import unittest
import uuid
from unittest import mock

from agent_chat.bridge import Bridge, exclusive_bridge
from agent_chat.bridge_client import RemoteBridgeState
from agent_chat.bridge_lease import BridgeManager
from agent_chat.bridge_state import BridgeState
from agent_chat.core import CoordError, Coordinator
from agent_chat.remote import RemoteCoordError
from agent_chat.rpc import TransportError
from agent_chat.web import snapshot
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

    def second_host(self):
        self.call('register', session='b', host='linux', agent='beta')
        thread_id = str(uuid.uuid4())
        self.call('bind', session='b', host='linux', thread=thread_id)
        state = RemoteBridgeState(self.client, self.url,
                                  {'owner': 'linux-worker', 'secret': 'z' * 40, 'host_id': 'linux'})
        state.call('acquire')
        return state, thread_id

    def test_two_client_machines_wake_only_their_own_threads(self):
        other, thread_id = self.second_host()
        self.send_operator('a'); self.send_operator('b')
        other_rpc = FakeRpc()
        self.bridge.tick()
        Bridge(other, other_rpc).tick()
        for rpc, expected in ((self.rpc, self.tid), (other_rpc, thread_id)):
            calls = [params for method, params in rpc.calls if method == 'thread/queue/start']
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0]['threadId'], expected)
            self.assertTrue(all(params.get('threadId') == expected for _, params in rpc.calls))
        with self.assertRaises(RemoteCoordError):
            other.call('observe', thread_id=self.tid, state='idle')
        self.assertEqual(len(self.call('bridge-status')['jobs']), 2)

    def test_reset_and_release_do_not_steal_another_hosts_dispatcher(self):
        other, _ = self.second_host()
        self.state.call('reset', confirm_stopped=True)
        with self.assertRaises(RemoteCoordError): self.state.pending()
        other.call('acquire')
        self.assertEqual(other.pending(), [])
        with self.assertRaises(CoordError):
            with exclusive_bridge(self.db): pass
        other.call('release')
        with exclusive_bridge(self.db): pass

    def test_bridge_health_tracks_each_host_independently(self):
        other, _ = self.second_host()
        self.state.heartbeat('ws://127.0.0.1:4500', 11)
        other.heartbeat('ws://127.0.0.1:4500', 22)
        self.state.call('release')
        runtime = self.call('bridge-status')['runtime']
        self.assertEqual(runtime, snapshot(self.db)['bridge'])
        self.assertTrue(runtime['recent'])
        self.assertIsNone(runtime['error'])
        clients = {item['host_id']: item for item in runtime['clients']}
        self.assertFalse(clients['mac']['recent'])
        self.assertTrue(clients['linux']['recent'])

    def test_job_recovery_requires_only_its_own_host_to_stop(self):
        other, _ = self.second_host()
        self.send_operator()
        coord = Coordinator(self.db, 'a')
        try:
            state = BridgeState(coord)
            job = state.prepare(self.tid, state.pending(), 'wake')
            state.update(job['id'], 'failed')
        finally:
            coord.close()
        with self.assertRaises(RemoteCoordError):
            self.call('bridge-retry', job_id=job['id'], confirm_not_started=True)
        self.state.call('release')
        recovered = self.call('bridge-retry', job_id=job['id'], confirm_not_started=True)
        self.assertEqual(recovered['status'], 'prepared')
        self.assertEqual(other.pending(), [])


class DispatcherMigrationTests(unittest.TestCase):
    def test_legacy_singleton_lease_keeps_its_owner_when_upgraded(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'state.sqlite3'
            c = Coordinator(path)
            c.db.execute('''CREATE TABLE bridge_dispatcher (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1), owner TEXT NOT NULL,
                secret_hash TEXT NOT NULL, host_id TEXT NOT NULL, acquired_at REAL NOT NULL)''')
            c.db.execute('INSERT INTO bridge_dispatcher VALUES(1,?,?,?,1)',
                         ('saved', hashlib.sha256(b's' * 40).hexdigest(), 'mac'))
            c.close()
            manager = BridgeManager(path)
            try:
                identity = {'owner': 'saved', 'secret': 's' * 40, 'host_id': 'mac'}
                self.assertTrue(manager.dispatch(dict(identity, op='acquire'))['acquired'])
                with self.assertRaises(CoordError):
                    manager.dispatch(dict(identity, owner='intruder', op='acquire'))
                self.assertTrue(manager.dispatch(dict(identity, host_id='linux', op='acquire'))['acquired'])
            finally:
                manager.close()
