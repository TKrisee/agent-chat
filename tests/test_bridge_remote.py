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
        instructions, metadata = payload.split('\n')
        self.assertIn('Keep established communication style.', instructions)
        self.assertNotIn('caveman', instructions.lower())
        self.assertNotIn('child', instructions)
        self.assertEqual({k: v for k, v in json.loads(metadata).items() if k != 'messages'}, {
            'server': self.url, 'project': 'default', 'thread_id': self.tid,
            'deliveries': [{'message_id': message, 'recipient_session': 'a',
                            'route': [{'session': 'a', 'agent_path': None}]}],
        })

    def test_unread_peer_request_wakes_idle_recipient_once(self):
        self.call('register', session='b', agent='beta')
        message = self.call('send', session='b', to='a', body='Please review this.')['id']

        self.bridge.tick()

        self.assertEqual([m for m, _ in self.rpc.calls].count('thread/queue/start'), 1)
        payload = next(p for m, p in self.rpc.calls if m == 'thread/queue/add')['input'][0]['text']
        self.assertIn(message, payload)
        self.assertEqual(self.call('bridge-status')['jobs'][0]['status'], 'dispatched')

        self.rpc.status = 'idle'
        self.bridge.tick()
        self.assertEqual([m for m, _ in self.rpc.calls].count('thread/queue/start'), 1)

    def test_unread_peer_request_waits_for_busy_recipient(self):
        self.call('register', session='b', agent='beta')
        self.call('send', session='b', to='a', body='Please review this.')
        self.rpc.status = 'active'

        self.bridge.tick()

        self.assertFalse(any(m == 'thread/queue/add' for m, _ in self.rpc.calls))
        self.assertEqual(self.call('bridge-status')['jobs'], [])

        self.rpc.status = 'idle'
        self.bridge.tick()
        self.assertEqual([m for m, _ in self.rpc.calls].count('thread/queue/start'), 1)

    def test_unread_peer_reply_wakes_but_acknowledged_and_self_messages_do_not(self):
        self.call('register', session='b', agent='beta')
        question = self.call('send', to='b', body='Can you check this?')['id']
        reply = self.call('send', session='b', to='a', body='Done.', reply_to=question)['id']
        acknowledged = self.call('send', session='b', to='a', body='Already handled.')['id']
        self.call('acknowledge', id=acknowledged)
        self.call('deregister', session='b')
        own_message = self.call('send', to='a', body='Do not wake myself.')['id']

        self.assertNotIn(acknowledged, [m['id'] for m in self.state.pending()])
        self.assertNotIn(own_message, [m['id'] for m in self.state.pending()])
        with self.assertRaises(RemoteCoordError):
            self.state.prepare(self.tid, [{'id': own_message, 'recipient_session': 'a'}], 'self wake')

        self.bridge.tick()

        payload = next(p for m, p in self.rpc.calls if m == 'thread/queue/add')['input'][0]['text']
        self.assertIn(reply, payload)
        self.assertNotIn(acknowledged, payload)
        self.assertNotIn(own_message, payload)

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
        self.call('register', session='peer', agent='beta')
        message = self.call('send', session='peer', to='child', body='Please help.')['id']
        self.bridge.tick()
        payload = next(p for m,p in self.rpc.calls if m == 'thread/queue/add')['input'][0]['text']
        self.assertIn('/root/child', payload)
        instructions, metadata = payload.split('\n')
        self.assertIn('For child routes', instructions)
        self.assertIn('Keep established communication style.', instructions)
        self.assertNotIn('caveman', instructions.lower())
        self.assertEqual(json.loads(metadata)['deliveries'], [
            {'message_id': message, 'recipient_session': 'child', 'route': [
                {'session': 'a', 'agent_path': None},
                {'session': 'child', 'agent_path': '/root/child'},
            ]},
        ])
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
        self.call('send', session='b', host='linux', to='a', body='Please review this.')
        self.call('send', to='b', body='Please review this.')
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
