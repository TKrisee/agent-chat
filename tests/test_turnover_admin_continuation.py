"""Disposable actual Root dispatch; no live provider or project state."""
import contextlib
import uuid
from unittest import mock

from agent_chat.core import CoordError, Coordinator
from agent_chat.rpc import TransportError
from test_second_turnover_continuation import SecondTurnoverContinuationTests


class AdminContinuationTests(SecondTurnoverContinuationTests):
    def setUp(self):
        super().setUp()
        self.retire_both()
        self.second_receipt = self.second()
        self.prior_id = self.second_receipt['native_turn_id']
        self.admin_message = self.reply('Exact later stopped administration')
        self.args = dict(project=self.project_id, request_id=self.bootstrap['request_id'],
                         previous_id=self.arguments['continuation_id'], continuation_id=str(uuid.uuid4()),
                         message_id=self.admin_message, retirement_ids=[str(uuid.uuid4()) for _ in range(7)], confirm=True)
        self.native_calls.clear()
        self.admin_error = None

    def native(self, method, params):
        if method == 'turn/start' and params['clientUserMessageId'] == getattr(self, 'admin_message', None):
            self.native_calls.append((method, dict(params)))
            if self.admin_error:
                raise self.admin_error
            return {'turn': {'id': getattr(self, 'next_turn_id', 'admin-native-turn')}}
        return super().native(method, params)

    def admin(self, **changes):
        return self.root('turnover-admin-continue', **dict(self.args, **changes))

    def test_reusable_exact_chain_and_unknown_fence(self):
        with mock.patch('agent_chat.turnover_admin_continuation._retirement_proof', return_value=('fixture',)):
            receipt = self.admin()
            self.assertEqual(receipt['status'], 'started')
            self.assertEqual(receipt['native_turn_id'], 'admin-native-turn')
            self.assertEqual(self.admin(), receipt)
            self.assertEqual(self.start_count(), 1)
            with self.assertRaisesRegex(CoordError, 'already used'):
                self.admin(continuation_id=str(uuid.uuid4()))
            self.prior_id = receipt['native_turn_id']
            self.admin_message = self.reply('Another separately authorized administration reply')
            self.next_turn_id = 'later-native-turn'
            later = self.admin(previous_id=receipt['continuation_id'], continuation_id=str(uuid.uuid4()),
                               message_id=self.admin_message, retirement_ids=[str(uuid.uuid4())])
            self.assertEqual(later['native_turn_id'], self.next_turn_id)
            self.assertEqual(self.start_count(), 2)
            self.assert_stop_active()

    def test_unknown_result_is_retained_and_cannot_be_replayed(self):
        self.admin_error = TransportError('unknown admin submission')
        with mock.patch('agent_chat.turnover_admin_continuation._retirement_proof', return_value=('fixture',)):
            with self.assertRaises(TransportError):
                self.admin()
            self.assertEqual(self.admin()['status'], 'submitted_unknown')
            with self.assertRaisesRegex(CoordError, 'already used'):
                self.admin(continuation_id=str(uuid.uuid4()))
            self.assertEqual(self.start_count(), 1)
            with contextlib.closing(Coordinator(self.path, self.project_root)) as coord:
                row = coord.db.execute('''SELECT e.settled_at,e.outcome FROM root_turnover_admin_continuations c
                    JOIN execution_epoch_handoffs e ON e.id=c.epoch_id WHERE c.continuation_id=?''', (self.args['continuation_id'],)).fetchone()
                self.assertEqual(tuple(row), (None, None))

    def test_requires_exact_terminal_previous_turn_idle_queue_and_root_reply(self):
        with mock.patch('agent_chat.turnover_admin_continuation._retirement_proof', return_value=('fixture',)):
            for attr, value, error in (
                    ('thread_state', 'active', 'idle exact PO thread'),
                    ('prior_status', 'inProgress', 'exact terminal prior turn'),
                    ('prior_id', 'unrelated-turn', 'exact terminal prior turn'),
                    ('queue', [{'id': 'other'}], 'empty native queue')):
                with self.subTest(attr=attr), mock.patch.object(self, attr, value):
                    with self.assertRaisesRegex(CoordError, error):
                        self.admin()
            with self.assertRaisesRegex(CoordError, 'exact Root reply'):
                self.admin(message_id=self.source)
            with self.assertRaisesRegex(CoordError, 'explicit confirmation'):
                self.admin(confirm=False)
            self.assertEqual(self.start_count(), 0)
            self.assert_stop_active()
