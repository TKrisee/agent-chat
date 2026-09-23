import subprocess
import uuid

from agent_chat.bridge import Bridge
from agent_chat.bridge_state import BridgeState
from agent_chat.core import Coordinator
from agent_chat.bridge_client import RemoteBridgeState
from test_bridge import FakeRpc
from test_remote_web import RemoteWebFixture


class GroupDeliveryTests(RemoteWebFixture):
    def setUp(self):
        super().setUp()
        self.call('register', agent='alpha')
        self.call('register', session='b', agent='beta')
        self.thread_id = str(uuid.uuid4())
        self.call('bind', thread=self.thread_id)
        self.operator = Coordinator(self.db, self.server.sender_session)
        self.addCleanup(self.operator.close)
        self.state = BridgeState(self.operator)
        self.rpc = FakeRpc()
        self.bridge = Bridge(self.state, self.rpc)

    def assert_json(self, raw, expression):
        checked = subprocess.run(['jq', '-e', expression], input=raw.decode(),
                                 text=True, capture_output=True, timeout=10)
        self.assertEqual(checked.returncode, 0, checked.stderr or checked.stdout)

    def test_group_read_and_ack_are_independent_and_only_direct_wakes(self):
        copies = self.operator.send_group_prepared(None, 'Shared progress', [])
        self.bridge.tick()
        self.assertEqual(self.rpc.calls, [])
        self.assertEqual(self.state.pending(), [])
        self.assertEqual(self.state.wake_messages(self.thread_id, [c['id'] for c in copies]), [])
        first = self.call('context')['messages'][0]
        second = self.call('context', session='b')['messages'][0]
        self.assertEqual(first['batch_id'], second['batch_id'])
        self.assertNotEqual(first['id'], second['id'])
        self.call('acknowledge', id=first['id'])
        self.assertEqual(self.call('context')['messages'], [])
        self.assertEqual(self.call('context', session='b')['messages'][0]['id'], second['id'])
        direct = self.operator.send('a', 'Please take action')
        self.bridge.tick()
        self.assertEqual(sum(name == 'thread/queue/start' for name, _ in self.rpc.calls), 1)
        payload = next(params['input'][0]['text'] for name, params in self.rpc.calls if name == 'thread/queue/add')
        self.assertIn(direct['id'], payload)
        self.assertNotIn(second['id'], payload)

    def test_single_recipient_group_is_quiet_and_cannot_be_prepared(self):
        self.call('deregister', session='b')
        copy = self.operator.send_group_prepared(None, 'Group intent with one member', [])[0]
        self.assertIsNone(self.state.prepare(self.thread_id,
                                            [{'id': copy['id'], 'recipient_session': 'a'}], 'must not dispatch'))
        self.bridge.tick()
        self.assertEqual(self.rpc.calls, [])
        self.assertEqual(self.call('context')['messages'][0]['id'], copy['id'])

    def test_preexisting_prepared_or_queued_group_only_job_is_cancelled(self):
        for status in ('prepared', 'queued'):
            with self.subTest(status=status):
                copy = self.operator.send_many(['a'], 'Older group delivery')[0]
                self.operator.db.execute('DELETE FROM message_attention WHERE message_id=?', (copy['id'],))
                job_id = 'old-group-' + status
                queue_id = 'queue-' + status if status == 'queued' else None
                with self.operator.tx() as db:
                    db.execute('INSERT INTO bridge_jobs VALUES(?,?,?,?,?,?,?,?)',
                               (job_id, self.thread_id, 'old payload', status, queue_id, None, 1, 1))
                    db.execute('INSERT INTO bridge_deliveries VALUES(?,?)', (copy['id'], job_id))
                if queue_id:
                    self.rpc.entries.append({'id': queue_id, 'clientUserMessageId': job_id, 'input': []})
                self.bridge.tick()
                self.assertEqual(self.state.job(job_id)['status'], 'cancelled')
                self.assertFalse(any(name == 'thread/queue/start' for name, _ in self.rpc.calls))
                self.assertIsNone(self.operator.db.execute('SELECT acked_at FROM messages WHERE id=?', (copy['id'],)).fetchone()[0])

    def test_remote_bridge_obeys_group_silence(self):
        remote = RemoteBridgeState(self.client, self.url,
                                   {'owner': 'group-test', 'secret': 'x' * 40, 'host_id': 'mac'})
        remote.call('acquire')
        bridge = Bridge(remote, self.rpc)
        self.operator.send_group_prepared(None, 'Quiet remote group', [])
        bridge.tick()
        self.assertFalse(any(name == 'thread/queue/start' for name, _ in self.rpc.calls))
        self.operator.send_many(['a', 'b'], 'Addressed remote request')
        bridge.tick()
        self.assertEqual(sum(name == 'thread/queue/start' for name, _ in self.rpc.calls), 1)

    def test_ui_endpoint_without_recipient_creates_one_group(self):
        status, raw, _ = self.request('POST', '/api/messages', {'body': 'UI group'},
                                      headers={'X-Agent-Chat-CSRF': self.server.csrf_token})
        self.assertEqual(status, 200, raw)
        self.assert_json(raw, '(.messages|length==2) and ([.messages[].batch_id]|unique|length==1) and all(.messages[];.batch_id!=null)')
        status, raw, _ = self.request('GET', '/api/messages')
        self.assertEqual(status, 200)
        self.assert_json(raw, '([.messages[].batch_id]|unique|length==1) and all(.messages[];(.deliveries|length)==2)')
        self.bridge.tick()
        self.assertEqual(self.rpc.calls, [])

    def test_explicit_tags_wake_each_addressed_agent_once(self):
        self.call('bind', session='b', thread=str(uuid.uuid4()))
        self.call('register', session='c', agent='untagged')
        self.call('bind', session='c', thread=str(uuid.uuid4()))
        status, raw, _ = self.request('POST', '/api/messages',
                                      {'to': ['a', 'b', 'a'], 'body': 'Please review'},
                                      headers={'X-Agent-Chat-CSRF': self.server.csrf_token})
        self.assertEqual(status, 200, raw)
        self.assert_json(raw, '(.messages|length==2) and ([.messages[].batch_id]|unique|length==1)')
        pending = self.state.pending()
        self.assertEqual({m['recipient_session'] for m in pending}, {'a', 'b'})
        self.assertEqual(len(self.state.wake_messages(self.thread_id, [m['id'] for m in pending])), 1)
        self.bridge.tick()
        # FakeRpc models one global active flag, so make the next thread idle.
        self.rpc.status = 'idle'
        self.bridge.tick()
        self.assertEqual(sum(name == 'thread/queue/start' for name, _ in self.rpc.calls), 2)
        self.assertEqual(len({params['threadId'] for name, params in self.rpc.calls
                              if name == 'thread/queue/start'}), 2)
        self.rpc.status = 'idle'
        self.bridge.tick()
        self.assertEqual(sum(name == 'thread/queue/start' for name, _ in self.rpc.calls), 2)
        status, raw, _ = self.request('GET', '/api/messages')
        self.assertEqual(status, 200)
        self.assert_json(raw, 'all(.messages[].deliveries[]; .wake_requested==true)')

    def test_explicit_single_member_batch_wakes(self):
        copies = self.operator.send_many(['a'], 'Explicit attention')
        self.assertEqual(self.state.pending()[0]['id'], copies[0]['id'])
        self.bridge.tick()
        self.assertEqual(sum(name == 'thread/queue/start' for name, _ in self.rpc.calls), 1)

    def test_ui_invalid_recipient_and_implicit_reply_do_not_broadcast(self):
        original = self.operator.send('a', 'Original direct')
        before = self.operator.db.execute('SELECT COUNT(*) FROM messages').fetchone()[0]
        for payload in ({'to': [], 'body': 'No one'}, {'to': '', 'body': 'Bad target'},
                        {'body': 'Do not leak reply', 'reply_to': original['id']}):
            status, raw, _ = self.request('POST', '/api/messages', payload,
                                          headers={'X-Agent-Chat-CSRF': self.server.csrf_token})
            self.assertEqual(status, 400, raw)
        self.assertEqual(self.operator.db.execute('SELECT COUNT(*) FROM messages').fetchone()[0], before)
