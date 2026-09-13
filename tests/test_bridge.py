import json
import os
import subprocess
import sys
import tempfile
import unittest
import uuid
from pathlib import Path

from agent_chat.bridge import Bridge, exclusive_bridge
from agent_chat.bridge_state import BridgeState
from agent_chat.core import CoordError, Coordinator
from agent_chat.rpc import RpcError, TransportError, RpcClient


class FakeRpc:
    def __init__(self):
        self.status = 'idle'
        self.direct = True
        self.entries = []
        self.history = []
        self.calls = []
        self.drop_add = False
        self.drop_start = False
        self.busy_start = False

    def request(self, method, params):
        self.calls.append((method, params))
        if method == 'thread/read':
            return {'thread': {'status': {'type': self.status}, 'canAcceptDirectInput': self.direct}}
        if method == 'thread/queue/list':
            return {'data': self.entries.copy(), 'nextCursor': None}
        if method == 'thread/turns/list':
            return {'data': [{'items': self.history.copy()}], 'nextCursor': None}
        if method == 'thread/queue/add':
            entry = {'id': 'q_' + str(len(self.entries)), 'clientUserMessageId': params['clientUserMessageId'], 'input': params['input']}
            self.entries.append(entry)
            if self.drop_add:
                raise TransportError('lost add response')
            return {'queuedSubmission': entry}
        if method == 'thread/queue/start':
            if self.busy_start:
                raise RpcError(-32600, 'thread already has an active or pending turn')
            entry = next(e for e in self.entries if e['id'] == params['queuedSubmissionId'])
            self.entries.remove(entry)
            self.history.append({'type': 'userMessage', 'clientId': entry['clientUserMessageId']})
            if self.drop_start:
                raise TransportError('lost start response')
            self.status = 'active'
            return {'turn': {'id': 'turn'}}
        if method == 'thread/queue/delete':
            found = next((e for e in self.entries if e['id'] == params['queuedSubmissionId']), None)
            if found:
                self.entries.remove(found)
            return {'deleted': bool(found)}
        raise AssertionError(method)


class BridgeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='agent-chat-bridge-')
        self.addCleanup(self.tmp.cleanup)
        self.db = Path(self.tmp.name) / 'state.sqlite3'
        self.operator = self.coord('human', 'operator')
        Path(str(self.db) + '.web-session.json').write_text(json.dumps({'id': 'human'}))
        self.agent = self.coord('worker', 'worker')
        self.state = BridgeState(self.agent)
        self.thread = str(uuid.uuid4())
        self.state.bind(thread_id=self.thread)
        self.rpc = FakeRpc()
        self.bridge = Bridge(self.state, self.rpc)

    def coord(self, session, label):
        coord = Coordinator(self.db, session)
        coord.register(label)
        self.addCleanup(coord.close)
        return coord

    def send(self, recipient='worker', sender=None):
        return (sender or self.operator).send(recipient, 'Please check this.')['id']

    def count(self, method):
        return sum(m == method for m, _ in self.rpc.calls)

    def job(self):
        return dict(self.agent.db.execute('SELECT * FROM bridge_jobs ORDER BY created_at DESC LIMIT 1').fetchone())

    def test_coalesces_and_does_not_read_ack_or_repeat(self):
        first, second = self.send(), self.send()
        before = dict(self.agent.db.execute("SELECT * FROM sessions WHERE id='worker'").fetchone())
        self.bridge.tick()
        self.assertEqual(self.count('thread/queue/add'), 1)
        self.assertEqual(self.count('thread/queue/start'), 1)
        self.assertIn(first, self.job()['payload'])
        self.assertIn(second, self.job()['payload'])
        self.assertEqual(self.job()['status'], 'dispatched')
        self.assertEqual(before, dict(self.agent.db.execute("SELECT * FROM sessions WHERE id='worker'").fetchone()))
        self.assertEqual(self.agent.db.execute('SELECT COUNT(*) FROM messages WHERE acked_at IS NOT NULL').fetchone()[0], 0)
        self.rpc.status = 'idle'
        self.bridge.tick()
        self.assertEqual(self.count('thread/queue/start'), 1)

    def test_acknowledged_and_agent_messages_do_not_wake(self):
        self.agent.acknowledge(self.send())
        other = self.coord('other', 'other')
        self.send(sender=other)
        self.bridge.tick()
        self.assertEqual(self.count('thread/queue/add'), 0)

    def test_busy_unloaded_and_noninput_threads_wait_then_wake(self):
        self.send()
        for status, direct in [('active', True), ('notLoaded', None), ('systemError', True), ('idle', False), ('idle', None)]:
            self.rpc.status, self.rpc.direct = status, direct
            self.bridge.tick()
            self.assertEqual(self.count('thread/queue/add'), 0)
        self.rpc.status, self.rpc.direct = 'idle', True
        self.bridge.tick()
        self.assertEqual(self.count('thread/queue/start'), 1)
        self.assertFalse(any(m in ('turn/start', 'turn/steer', 'thread/resume') for m, _ in self.rpc.calls))

    def test_unknown_thread_does_not_starve_other_recipients(self):
        self.send()
        original = self.rpc.request
        self.rpc.request = lambda m, p: (_ for _ in ()).throw(RpcError(-1, 'not found')) if m == 'thread/read' else original(m, p)
        self.bridge.tick()
        self.assertEqual(self.state.jobs(), [])
        self.assertIn('not found', self.state.status()['threads'][0]['error'])

    def test_respects_existing_codex_queue(self):
        self.send()
        self.rpc.entries.append({'id': 'user-q', 'clientUserMessageId': 'someone-else'})
        self.bridge.tick()
        self.assertEqual(self.count('thread/queue/add'), 0)
        self.rpc.entries.clear()
        self.bridge.tick()
        self.assertEqual(self.count('thread/queue/start'), 1)

    def test_idle_race_uses_atomic_queue_start_and_retries_existing_item(self):
        self.send()
        self.rpc.busy_start = True
        self.bridge.tick()
        self.assertEqual(self.job()['status'], 'queued')
        self.assertEqual(len(self.rpc.entries), 1)
        self.rpc.busy_start = False
        self.bridge.tick()
        self.assertEqual(self.count('thread/queue/add'), 1)
        self.assertEqual(self.job()['status'], 'dispatched')

    def test_lost_add_response_reconciles_existing_queue_after_restart(self):
        self.send()
        self.rpc.drop_add = True
        with self.assertRaises(TransportError):
            self.bridge.tick()
        self.assertEqual(self.job()['status'], 'uncertain')
        Bridge(BridgeState(self.agent), self.rpc).tick()
        self.assertEqual(self.count('thread/queue/add'), 1)
        self.assertEqual(self.job()['status'], 'dispatched')

    def test_lost_start_response_reconciles_history_without_duplicate_turn(self):
        self.send()
        self.rpc.drop_start = True
        with self.assertRaises(TransportError):
            self.bridge.tick()
        self.rpc.drop_start = False
        self.bridge.tick()
        self.assertEqual(self.job()['status'], 'dispatched')
        self.assertEqual(self.count('thread/queue/start'), 1)

    def test_no_evidence_is_uncertain_never_automatic_retry(self):
        self.send()
        self.rpc.drop_start = True
        with self.assertRaises(TransportError):
            self.bridge.tick()
        self.rpc.history.clear()
        for _ in range(2):
            self.bridge.tick()
        self.assertEqual(self.job()['status'], 'uncertain')
        self.assertEqual(self.count('thread/queue/start'), 1)
        with self.assertRaises(CoordError):
            self.state.retry(self.job()['id'])
        other = self.coord('other', 'other')
        with self.assertRaises(CoordError):
            BridgeState(other).retry(self.job()['id'], True)
        self.state.retry(self.job()['id'], True)
        self.assertEqual(self.job()['status'], 'prepared')

    def test_pending_survives_unbound_session_and_process_restart(self):
        self.state.unbind()
        self.send()
        self.bridge.tick()
        self.assertEqual(self.count('thread/queue/add'), 0)
        self.state.bind(thread_id=self.thread)
        Bridge(BridgeState(self.agent), self.rpc).tick()
        self.assertEqual(self.count('thread/queue/start'), 1)

    def test_uncertain_delivery_can_be_explicitly_confirmed_without_new_turn(self):
        self.send()
        self.rpc.drop_start = True
        with self.assertRaises(TransportError):
            self.bridge.tick()
        job_id = self.job()['id']
        with self.assertRaises(CoordError):
            self.state.resolve_delivered(job_id)
        self.state.resolve_delivered(job_id, True)
        self.bridge.tick()
        self.assertEqual(self.job()['status'], 'dispatched')
        self.assertEqual(self.count('thread/queue/start'), 1)

    def test_binding_identity_duplicates_cycles_and_parent_chain(self):
        child = self.coord('child', 'worker/reviewer')
        child_state = BridgeState(child)
        with self.assertRaises(CoordError):
            child_state.bind(thread_id=self.thread)
        child_state.bind(parent_session='worker', agent_path='/root/reviewer')
        with self.assertRaises(CoordError):
            self.state.bind(parent_session='child', agent_path='/root')
        self.assertEqual(self.state.resolve('child')['thread_id'], self.thread)
        grandchild = self.coord('grandchild', 'worker/reviewer/check')
        BridgeState(grandchild).bind(parent_session='child', agent_path='/root/reviewer/check')
        child_message = self.send('child')
        grand_message = self.send('grandchild')
        self.send()
        self.bridge.tick()
        self.assertEqual(self.count('thread/queue/start'), 1)
        self.assertIn(child_message, self.job()['payload'])
        self.assertIn(grand_message, self.job()['payload'])
        self.assertIn('/root/reviewer/check', self.job()['payload'])
        self.assertEqual(child.db.execute("SELECT inbox_read_seq FROM sessions WHERE id='child'").fetchone()[0], 0)

    def test_binding_rejects_inherited_non_uuid_and_missing_parent_path(self):
        for kwargs in ({'thread_id': 'worker'}, {'parent_session': 'worker'}, {'parent_session': 'missing', 'agent_path': '/root/child'}):
            with self.assertRaises(CoordError):
                self.state.bind(**kwargs)

    def test_no_reroute_of_outstanding_job(self):
        self.send()
        self.rpc.busy_start = True
        self.bridge.tick()
        with self.assertRaises(CoordError):
            self.state.bind(thread_id=str(uuid.uuid4()))
        with self.assertRaises(CoordError):
            self.state.unbind()
        self.assertEqual(self.state.bind(thread_id=self.thread)['thread_id'], self.thread)

    def test_ack_during_wait_cancels_own_queue_without_model_turn(self):
        message = self.send()
        self.rpc.busy_start = True
        self.bridge.tick()
        self.agent.acknowledge(message)
        self.rpc.busy_start = False
        self.bridge.tick()
        self.assertEqual(self.job()['status'], 'cancelled')
        self.assertEqual(self.rpc.entries, [])
        self.assertEqual(self.count('thread/queue/start'), 1)  # first attempt was rejected busy

    def test_operator_identity_is_exact_not_label_or_prefix(self):
        impostor = self.coord('web_operator_impostor', 'operator')
        self.send(sender=impostor)
        self.bridge.tick()
        self.assertEqual(self.count('thread/queue/add'), 0)
        Path(str(self.db) + '.web-session.json').unlink()
        with self.assertRaises(CoordError):
            self.state.pending()

    def test_single_bridge_lock_is_released_on_exception(self):
        with exclusive_bridge(self.db):
            with self.assertRaises(CoordError):
                with exclusive_bridge(self.db):
                    self.fail('second bridge admitted')
        with exclusive_bridge(self.db):
            pass

    def test_cli_binding_and_status_do_not_read_inbox(self):
        self.send()
        root = Path(__file__).resolve().parents[1]
        result = subprocess.run([sys.executable, str(root / 'bin/agent-chat'), '--db', str(self.db), '--session', 'worker', 'bind', '--thread', self.thread], text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)['session_id'], 'worker')
        self.assertEqual(self.agent.db.execute("SELECT inbox_read_seq FROM sessions WHERE id='worker'").fetchone()[0], 0)

    def test_end_to_end_websocket_dispatch(self):
        from test_rpc import FakeServer, _initialize, _frame, _send
        self.send()
        def handler(sock):
            _initialize(sock)
            while True:
                opcode, _, body = _frame(sock)
                if opcode == 8:
                    return
                request = json.loads(body)
                result = self.rpc.request(request['method'], request['params'])
                _send(sock, 1, json.dumps({'id': request['id'], 'result': result}).encode())
        server = FakeServer(handler)
        try:
            with RpcClient(server.endpoint) as client:
                Bridge(self.state, client).tick()
        finally:
            server.join()
        self.assertEqual(self.job()['status'], 'dispatched')
        self.assertEqual(self.count('thread/queue/start'), 1)


if __name__ == '__main__':
    unittest.main()
