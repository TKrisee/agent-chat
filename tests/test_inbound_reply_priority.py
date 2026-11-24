"""The delivered wake prioritizes truthful first reply before task setup.

Native calls below are an in-memory provider fixture, not model obedience proof.
"""
import subprocess
import tempfile
import unittest
import uuid
from pathlib import Path

from agent_chat import communication
from agent_chat.bridge import Bridge
from agent_chat.bridge_state import BridgeState
from agent_chat.core import Coordinator
from agent_chat.reset_worker import ResetWorker


class NativeFixture:
    def __init__(self):
        self.status = 'idle'
        self.queued = []
        self.turns = []
        self.inputs = []

    def request(self, method, params):
        if method == 'thread/read':
            return {'thread': {'status': {'type': self.status}, 'canAcceptDirectInput': True}}
        if method == 'thread/queue/list':
            return {'data': list(self.queued), 'nextCursor': None}
        if method == 'thread/turns/list':
            return {'data': list(self.turns), 'nextCursor': None}
        if method == 'thread/queue/add':
            self.inputs.append(params)
            item = {'id': 'queue-' + str(len(self.inputs)),
                    'clientUserMessageId': params['clientUserMessageId'], 'input': params['input']}
            self.queued.append(item)
            return {'queuedSubmission': item}
        if method == 'thread/queue/start':
            item = next(x for x in self.queued if x['id'] == params['queuedSubmissionId'])
            self.queued.remove(item)
            turn = {'id': 'turn-' + str(len(self.inputs)), 'status': 'completed',
                    'items': [{'type': 'userMessage', 'clientId': item['clientUserMessageId']}]}
            self.turns.append(turn)
            self.status = 'active'
            return {'turn': {'id': turn['id']}}
        raise AssertionError(method)


class InboundReplyPriorityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='reply-priority-')
        self.addCleanup(self.temp.cleanup)
        database = Path(self.temp.name) / 'state.sqlite3'
        self.sender = Coordinator(database, 'source-owner')
        self.addCleanup(self.sender.close)
        self.sender.register('source-owner')
        self.recipient = Coordinator(database, 'recipient')
        self.addCleanup(self.recipient.close)
        self.recipient.register('recipient')
        self.state = BridgeState(self.recipient)
        self.thread = str(uuid.uuid4())
        self.state.bind(thread_id=self.thread)
        self.native = NativeFixture()
        self.bridge = Bridge(self.state, self.native)
        self.bridge._policy_clock = lambda: 1000

    def dispatch(self, body='Investigate this assigned issue and report the next action.'):
        source = self.sender.send('recipient', body)['id']
        self.native.status = 'idle'
        self.bridge.tick()
        return source, self.native.inputs[-1]['input'][0]['text']

    def metadata_check(self, payload, expression):
        subprocess.run(['jq', '-e', expression], input=payload.split('\n', 1)[1],
                       text=True, stdout=subprocess.DEVNULL, check=True)

    def assert_priority(self, payload):
        policy = payload.split('\n', 1)[0]
        for requirement in ('After fully consuming each new substantive request',
                            'before deep checks, status/help lookups, goal adoption, task setup or delegation',
                            'truthful substantive initial reply',
                            'do not claim adoption, execution or results before they occur',
                            '--reply-to INBOX_MESSAGE_ID --body-file BODY_FILE',
                            'Then continue authorized orchestration or assigned work'):
            self.assertIn(requirement, policy)

    def test_cold_and_compact_wakes_show_exact_first_reply_invocation(self):
        for wake in (communication.STANDING_POLICY, communication.COMPACT_WAKE):
            with self.subTest(wake=wake[:20]):
                self.assertIn('agent-chat-client send --to CANONICAL_SOURCE_SENDER_ID', wake)
                self.assertIn('--reply-to INBOX_MESSAGE_ID --body-file BODY_FILE', wake)
                self.assertIn('Use --ack-reply only when explicitly acknowledging', wake)
                self.assertIn('simple request fully answerable', wake)
                self.assertIn('same first reply', wake)

    def test_supervisor_notice_is_not_a_source_linked_first_reply(self):
        self.assertIn('system:supervisor', communication.STANDING_POLICY)
        self.assertIn('not source-linked task requests', communication.STANDING_POLICY)

    def assert_no_automatic_reply_or_read(self, source):
        self.assertIsNone(self.recipient.db.execute('SELECT acked_at FROM messages WHERE id=?', (source,)).fetchone()[0])
        self.assertEqual(self.recipient.db.execute('SELECT COUNT(*) FROM message_reads WHERE message_id=?', (source,)).fetchone()[0], 0)
        self.assertEqual(self.recipient.db.execute('SELECT COUNT(*) FROM messages WHERE sender_session=?', ('recipient',)).fetchone()[0], 0)

    def test_cold_delivery_prioritizes_truthful_reply_without_forging_it(self):
        source, payload = self.dispatch()
        self.metadata_check(payload, '.policy_included == true and (.messages | length) == 1 and .messages[0].complete == true')
        self.assert_no_automatic_reply_or_read(source)
        self.assert_priority(payload)
        self.recipient.message(source)
        reply = self.recipient.send('source-owner', 'Received this issue. I am starting coordination; investigation and results are pending.',
                                    reply_to=source, ack_reply=True)
        self.assertTrue(reply['id'])
        self.assertIsNotNone(self.recipient.db.execute('SELECT acked_at FROM messages WHERE id=?', (source,)).fetchone()[0])

    def test_consumed_policy_warm_delivery_keeps_priority_and_source(self):
        first, cold = self.dispatch()
        self.bridge.observe_policies()
        self.assertTrue(self.state.policy_consumed(self.thread))
        source, warm = self.dispatch('New bounded assignment after prior policy consumption.')
        self.assertNotEqual(first, source)
        self.metadata_check(warm, '.policy_included == false and (.messages | length) == 1 and .messages[0].complete == true')
        self.assertNotIn(communication.STANDING_POLICY, warm)
        self.assertLess(len(warm), len(cold))
        self.assert_no_automatic_reply_or_read(source)
        self.assert_priority(warm)
        self.bridge.tick()
        self.assertEqual(len(self.native.inputs), 2)

    def test_incomplete_source_requires_fetch_before_initial_reply(self):
        source, payload = self.dispatch('Large task detail. ' * 400)
        self.metadata_check(payload, '.messages[0].complete == false and (.messages[0] | has("body") | not)')
        self.assert_no_automatic_reply_or_read(source)
        self.assert_priority(payload)
        self.assertIn('Fetch incomplete: message MESSAGE_ID.', payload.split('\n', 1)[0])

    def test_current_po_reset_bootstrap_keeps_role_and_reply_priority(self):
        prompt = 'Retain this bounded task and coordinate a worker; no task has been adopted yet.'
        payload = ResetWorker(self.bridge).bootstrap({
            'target_session': 'recipient', 'target_agent': 'ordinary-name', 'new_thread_id': self.thread,
            'role_context': {'session': 'recipient', 'role': 'project_orchestrator'}, 'prompt': prompt,
        })
        self.assertIn('Your current server-derived role is project orchestrator.', payload)
        self.assertIn('Ordinary role-scoped administration and decision routing remain authorized.', payload)
        self.assertTrue(payload.endswith(prompt))
        self.assert_priority(payload)


if __name__ == '__main__':
    unittest.main()
