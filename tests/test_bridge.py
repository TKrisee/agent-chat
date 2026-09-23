import json
import os
import struct
import subprocess
import sys
import tempfile
import unittest
import uuid
from unittest import mock
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

    def connect(self):
        return self

    def close(self):
        pass

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
        payload = next(p for method, p in self.rpc.calls if method == 'thread/queue/add')['input'][0]['text']
        instructions, metadata = payload.split('\n')
        self.assertIn('Keep established communication style.', instructions)
        self.assertNotIn('caveman', instructions.lower())
        self.assertIn('Chat only via agent-chat-client', instructions)
        self.assertIn('no duplicate terminal commentary or final replies', instructions)
        self.assertIn('--reply-to INBOX_MESSAGE_ID', instructions)
        self.assertIn('do not send ACK-only messages', instructions)
        self.assertNotIn('child', instructions)
        self.assertLessEqual(len(instructions.split()), 90)
        self.assertEqual({k: v for k, v in json.loads(metadata).items() if k != 'messages'}, {
            'database': str(self.db.resolve()), 'thread_id': self.thread,
            'deliveries': [
                {'message_id': message, 'recipient_session': 'worker',
                 'route': [{'session': 'worker', 'agent_path': None}]}
                for message in (first, second)
            ],
        })
        self.assertEqual(self.job()['status'], 'dispatched')
        self.assertEqual(before, dict(self.agent.db.execute("SELECT * FROM sessions WHERE id='worker'").fetchone()))
        self.assertEqual(self.agent.db.execute('SELECT COUNT(*) FROM messages WHERE acked_at IS NOT NULL').fetchone()[0], 0)
        self.rpc.status = 'idle'
        self.bridge.tick()
        self.assertEqual(self.count('thread/queue/start'), 1)

    def test_acknowledged_and_self_messages_do_not_wake(self):
        self.agent.acknowledge(self.send())
        other = self.coord('other', 'other')
        self.agent.acknowledge(self.send(sender=other))
        self_message = self.send(sender=self.agent)
        self.assertEqual(self.state.pending(), [])
        self.assertIsNone(self.state.prepare(
            self.thread, [{'id': self_message, 'recipient_session': 'worker'}], 'ignored'))
        self.bridge.tick()
        self.assertEqual(self.count('thread/queue/add'), 0)
        self.assertIsNone(self.agent.db.execute(
            'SELECT acked_at FROM messages WHERE id=?', (self_message,)).fetchone()[0])

    def test_peer_and_operator_messages_coalesce_without_automatic_ack_or_repeats(self):
        other = self.coord('other', 'other')
        first = self.send(sender=other)
        second = self.send()
        self.bridge.tick()
        self.assertEqual(self.count('thread/queue/start'), 1)
        metadata = json.loads(self.job()['payload'].split('\n')[-1])
        self.assertEqual([item['message_id'] for item in metadata['deliveries']], [first, second])
        self.assertEqual(self.agent.db.execute(
            'SELECT COUNT(*) FROM messages WHERE acked_at IS NOT NULL').fetchone()[0], 0)
        self.rpc.status = 'idle'
        self.bridge.tick()
        self.assertEqual(self.count('thread/queue/start'), 1)
        self.agent.acknowledge(first)
        self.agent.acknowledge(second)
        self.bridge.tick()
        self.assertEqual(self.count('thread/queue/start'), 1)

    def test_prepare_rechecks_peer_acknowledgement_and_recipient(self):
        other = self.coord('other', 'other')
        message = self.send(sender=other)
        pending = self.state.pending()
        self.assertEqual(pending, [{'id': message, 'recipient_session': 'worker'}])
        self.assertIsNone(self.state.prepare(
            self.thread, [{'id': message, 'recipient_session': 'other'}], 'wrong recipient'))
        self.agent.acknowledge(message)
        self.assertIsNone(self.state.prepare(self.thread, pending, 'already acknowledged'))
        self.assertEqual(self.state.jobs(), [])

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

    def test_absent_experimental_capability_waits_without_starting(self):
        self.send()
        original = self.rpc.request
        def request(method, params):
            result = original(method, params)
            if method == 'thread/read': result['thread'].pop('canAcceptDirectInput')
            return result
        with mock.patch.object(self.rpc, 'request', side_effect=request): self.bridge.tick()
        self.assertEqual(self.count('thread/queue/add'), 0)

    def test_bad_route_does_not_starve_unrelated_thread(self):
        self.send()
        other = self.coord('other', 'other')
        tid = str(uuid.uuid4()); BridgeState(other).bind(thread_id=tid)
        self.send('other')
        resolve = self.state.resolve
        def route(session):
            if session == 'worker': raise CoordError('broken parent route')
            return resolve(session)
        with mock.patch.object(self.state, 'resolve', side_effect=route):
            with self.assertRaisesRegex(CoordError, 'broken parent'): self.bridge.tick()
        self.assertEqual(self.job()['thread_id'], tid)
        self.assertEqual(self.job()['status'], 'dispatched')

    def test_bad_persisted_job_does_not_starve_unrelated_thread(self):
        message = self.send()
        broken = self.state.prepare(self.thread, [{'id':message,'recipient_session':'worker'}], 'payload')
        other = self.coord('other', 'other')
        tid = str(uuid.uuid4()); BridgeState(other).bind(thread_id=tid); self.send('other')
        unread = self.state.still_unread
        def check(job):
            if job == broken['id']: raise CoordError('job state unavailable')
            return unread(job)
        with mock.patch.object(self.state, 'still_unread', side_effect=check):
            with self.assertRaisesRegex(CoordError, 'job state unavailable'): self.bridge.tick()
        self.assertEqual(self.state.job(broken['id'])['status'], 'prepared')
        self.assertEqual(self.job()['thread_id'], tid)
        self.assertEqual(self.job()['status'], 'dispatched')

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

    def test_reconciles_paginated_summaries_without_loading_tool_outputs(self):
        self.send()
        self.rpc.drop_start = True
        with self.assertRaises(TransportError):
            self.bridge.tick()
        job = self.job()
        original = self.rpc.request
        history_pages = []
        def request(method, params):
            if method != 'thread/turns/list':
                return original(method, params)
            # Full history for these turns contains oversized tool output.
            if params.get('itemsView') != 'summary':
                raise TransportError('WebSocket frame exceeds size limit')
            history_pages.append(params)
            page = int(params.get('cursor', '0'))
            item = {'type': 'userMessage', 'clientId': job['id'] if page == 3 else 'unrelated'}
            return {'data': [{'items': [item]}], 'nextCursor': str(page + 1)}
        with mock.patch.object(self.rpc, 'request', side_effect=request):
            self.bridge.tick()
        self.assertEqual(self.state.job(job['id'])['status'], 'dispatched')
        self.assertEqual([page.get('cursor') for page in history_pages], [None, '1', '2', '3'])
        self.assertEqual(self.count('thread/queue/add'), 1)
        self.assertEqual(self.count('thread/queue/start'), 1)

    def test_missing_summary_marker_remains_uncertain_with_bounded_history(self):
        self.send()
        self.rpc.drop_start = True
        with self.assertRaises(TransportError):
            self.bridge.tick()
        original = self.rpc.request
        history_pages = []
        def request(method, params):
            if method != 'thread/turns/list':
                return original(method, params)
            history_pages.append(params)
            return {'data': [{'items': [{'type': 'userMessage', 'clientId': None}]}],
                    'nextCursor': str(len(history_pages))}
        with mock.patch.object(self.rpc, 'request', side_effect=request):
            self.bridge.tick()
        self.assertEqual(self.job()['status'], 'uncertain')
        self.assertEqual(len(history_pages), 4)
        self.assertEqual(self.count('thread/queue/add'), 1)
        self.assertEqual(self.count('thread/queue/start'), 1)

    def assert_oversized_history_does_not_starve_other_thread(self, prepare_other):
        from test_rpc import FakeServer, _initialize, _frame, _send
        from agent_chat.rpc import _MAX_MESSAGE
        message = self.send()
        broken = self.state.prepare(self.thread, [{'id': message, 'recipient_session': 'worker'}], 'payload')
        self.state.update(broken['id'], 'queued', queue_id='gone')
        other = self.coord('other', 'other')
        tid = str(uuid.uuid4())
        BridgeState(other).bind(thread_id=tid)
        other_message = self.send('other')
        if prepare_other:
            self.state.prepare(tid, [{'id': other_message, 'recipient_session': 'other'}], 'payload')
        connections = []
        def handler(sock):
            connections.append(True)
            _initialize(sock)
            while True:
                opcode, _, body = _frame(sock)
                if opcode == 8:
                    return
                request = json.loads(body)
                if request['method'] == 'thread/turns/list':
                    # Leave unread body bytes on the rejected connection.
                    sock.sendall(b'\x81\x7f' + struct.pack('!Q', _MAX_MESSAGE + 1) + b'{"id":')
                    self.assertEqual(_frame(sock)[0], 8)
                    return
                result = self.rpc.request(request['method'], request['params'])
                _send(sock, 1, json.dumps({'id': request['id'], 'result': result}).encode())
        server = FakeServer(handler, connections=2)
        try:
            with RpcClient(server.endpoint) as client:
                with self.assertRaisesRegex(TransportError, 'frame exceeds size limit'):
                    Bridge(self.state, client).tick()
        finally:
            server.join()
        self.assertEqual(len(connections), 2)
        self.assertEqual(self.state.job(broken['id'])['status'], 'queued')
        other_job = self.agent.db.execute('SELECT status FROM bridge_jobs WHERE thread_id=?', (tid,)).fetchone()
        self.assertEqual(other_job['status'], 'dispatched')
        for method in ('thread/queue/add', 'thread/queue/start'):
            self.assertEqual([p['threadId'] for m, p in self.rpc.calls if m == method], [tid])

    def test_oversized_history_reconnects_before_other_persisted_job(self):
        self.assert_oversized_history_does_not_starve_other_thread(prepare_other=True)

    def test_oversized_history_reconnects_before_new_wake(self):
        self.assert_oversized_history_does_not_starve_other_thread(prepare_other=False)

    def test_lost_new_enqueue_does_not_starve_or_repeat(self):
        self.send()
        other = self.coord('other', 'other')
        tid = str(uuid.uuid4())
        BridgeState(other).bind(thread_id=tid)
        self.send('other')
        original = self.rpc.request
        def request(method, params):
            result = original(method, params)
            if method == 'thread/queue/add' and params['threadId'] == self.thread:
                # Isolate the other thread's queue in this single-thread fake.
                self.rpc.entries.clear()
                raise TransportError('lost add response')
            return result
        with mock.patch.object(self.rpc, 'request', side_effect=request), \
                mock.patch.object(self.rpc, 'close') as close, \
                mock.patch.object(self.rpc, 'connect') as connect:
            with self.assertRaisesRegex(TransportError, 'lost add response'):
                self.bridge.tick()
        close.assert_called_once_with()
        connect.assert_called_once_with()
        jobs = dict(self.agent.db.execute('SELECT thread_id, status FROM bridge_jobs'))
        self.assertEqual(jobs, {self.thread: 'uncertain', tid: 'dispatched'})
        self.assertEqual([p['threadId'] for m, p in self.rpc.calls if m == 'thread/queue/add'], [self.thread, tid])
        self.assertEqual([p['threadId'] for m, p in self.rpc.calls if m == 'thread/queue/start'], [tid])

    def test_failed_reconnect_stops_pass_for_normal_backoff(self):
        self.send()
        self.rpc.drop_add = True
        with mock.patch.object(self.rpc, 'connect', side_effect=TransportError('connection refused')) as connect:
            with self.assertRaisesRegex(TransportError, 'connection refused'):
                self.bridge.tick()
        connect.assert_called_once_with()
        self.assertEqual(self.job()['status'], 'uncertain')
        self.assertEqual(self.count('thread/queue/add'), 1)
        self.assertEqual(self.count('thread/queue/start'), 0)

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
        root_message = self.send()
        self.bridge.tick()
        self.assertEqual(self.count('thread/queue/start'), 1)
        self.assertIn(child_message, self.job()['payload'])
        self.assertIn(grand_message, self.job()['payload'])
        self.assertIn('/root/reviewer/check', self.job()['payload'])
        instructions, metadata = self.job()['payload'].split('\n')
        self.assertIn('Keep established communication style.', instructions)
        self.assertNotIn('caveman', instructions.lower())
        self.assertEqual(instructions.count('For child routes'), 1)
        self.assertIn('native follow-up', instructions)
        self.assertIn('Never impersonate a child, read/ack its inbox', instructions)
        self.assertIn('routing metadata and these instructions', instructions)
        self.assertIn('Report unavailable children in chat', instructions)
        root_route = [{'session': 'worker', 'agent_path': None}]
        child_route = root_route + [{'session': 'child', 'agent_path': '/root/reviewer'}]
        self.assertEqual(json.loads(metadata)['deliveries'], [
            {'message_id': child_message, 'recipient_session': 'child', 'route': child_route},
            {'message_id': grand_message, 'recipient_session': 'grandchild',
             'route': child_route + [{'session': 'grandchild', 'agent_path': '/root/reviewer/check'}]},
            {'message_id': root_message, 'recipient_session': 'worker', 'route': root_route},
        ])
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

    def test_peer_wake_does_not_depend_on_sender_label_or_web_identity(self):
        peer = self.coord('web_operator_peer', 'operator')
        self.send(sender=peer)
        Path(str(self.db) + '.web-session.json').unlink()
        self.bridge.tick()
        self.assertEqual(self.count('thread/queue/start'), 1)
        self.assertEqual(self.state.pending(), [])

    def test_single_bridge_lock_is_released_on_exception(self):
        with exclusive_bridge(self.db):
            with self.assertRaises(CoordError):
                with exclusive_bridge(self.db):
                    self.fail('second bridge admitted')
        with exclusive_bridge(self.db):
            pass

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
