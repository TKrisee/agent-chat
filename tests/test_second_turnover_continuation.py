"""Exercise the exact second continuation with disposable stores and mocked native RPC."""
import contextlib
import uuid
from unittest import mock

from agent_chat.core import CoordError, Coordinator
from agent_chat.rpc import RpcError, TransportError
import test_parked_provision_http as provision_fixture
import test_parked_retirement as retirement_fixture


class SecondTurnoverContinuationTests(provision_fixture.ParkedProvisionHttpTests):
    setup_attempts = retirement_fixture.ParkedRetirementTests.setup_attempts
    retire = retirement_fixture.ParkedRetirementTests.retire

    def setUp(self):
        super().setUp()
        (self.project_id, self.route, self.path, self.bootstrap,
         _, _, self.po, self.po_client, self.attempts) = self.setup_attempts()
        self.project_root = self.route['session']
        self.po_thread = self.po['new_thread_id']
        self.source = self.po_client.call('/api/coord', dict(op='send', session=self.po['target_session'],
            host_id='mac', params=dict(to=self.project_root, body='Retained parked administration report')))['id']
        self.routed(self.project_id, 'context')
        self.older_message = self.reply('Earlier unused administrative reply')
        self.first_message = self.reply('First exact administrative reply')
        self.second_message = self.reply('Second exact administrative reply for two replacements')
        self.native_calls = []
        self.thread_state = 'idle'
        self.loaded_state = 'idle'
        self.direct_input = True
        self.prior_id = 'first-native-turn'
        self.prior_status = 'failed'
        self.queue = []
        self.provider_error = None

        stack = contextlib.ExitStack()
        self.addCleanup(stack.close)
        stack.enter_context(mock.patch('agent_chat.provider_service.configured',
            return_value=(b'disposable-wrapper', ['--codex-server', 'ws://127.0.0.1:1'])))
        stack.enter_context(mock.patch('agent_chat.provider_service.selected_listener', return_value='fixture-listener'))
        rpc = stack.enter_context(mock.patch('agent_chat.rpc.RpcClient'))
        rpc.return_value.__enter__.return_value.request.side_effect = self.native
        self.first_receipt = self.root('turnover-bootstrap-continue', project=self.project_id,
            request_id=self.bootstrap['request_id'], message_id=self.first_message, confirm=True)
        self.assertEqual(self.first_receipt['native_turn_id'], self.prior_id)
        self.native_calls.clear()
        self.arguments = dict(project=self.project_id, request_id=self.bootstrap['request_id'],
            continuation_id=str(uuid.uuid4()), message_id=self.second_message, confirm=True,
            first_retirement_id=self.attempts[0][0], second_retirement_id=self.attempts[1][0])

    def reply(self, body):
        return self.routed(self.project_id, 'send', to=self.po['target_session'],
                           body=body, reply_to=self.source)['id']

    def native(self, method, params):
        self.native_calls.append((method, dict(params)))
        self.assertEqual(params['threadId'], self.po_thread)
        if method == 'thread/read':
            return {'thread': dict(id=self.po_thread, status={'type': self.thread_state},
                                   canAcceptDirectInput=None if self.thread_state == 'notLoaded' else self.direct_input)}
        if method == 'thread/resume':
            self.assertEqual(params, {'threadId': self.po_thread, 'excludeTurns': True})
            self.thread_state = self.loaded_state
            return {'thread': {'id': self.po_thread}}
        if method == 'thread/turns/list':
            self.assertEqual(params['limit'], 1)
            return {'data': [dict(id=self.prior_id, status=self.prior_status)]}
        if method == 'thread/queue/list':
            return {'data': self.queue}
        if method == 'turn/start':
            self.assertEqual(set(params), {'threadId', 'clientUserMessageId', 'input'})
            first = params['clientUserMessageId'] == self.first_message
            if not first:
                self.assertEqual(params['clientUserMessageId'], self.second_message)
                self.assertIn(self.second_message, params['input'][0]['text'])
                if self.provider_error is not None:
                    raise self.provider_error
            return {'turn': {'id': 'first-native-turn' if first else 'second-native-turn'}}
        self.fail('unexpected native method ' + method)

    def retire_both(self):
        for request, _, _ in self.attempts:
            receipt = self.retire(self.project_id, self.bootstrap, self.po, request)
            self.assertTrue(receipt['retired'])
            self.assertEqual(receipt['native_outcome'], 'unknown')

    def second(self, **changes):
        return self.root('turnover-bootstrap-continue-second', **dict(self.arguments, **changes))

    def start_count(self):
        return sum(method == 'turn/start' for method, _ in self.native_calls)

    def earlier_records(self):
        with contextlib.closing(Coordinator(self.path, self.project_root)) as coord:
            earlier = (
                ('root_turnover_continuations', 'request_id=?', (self.bootstrap['request_id'],)),
                ('root_parked_retirements', 'request_id IN (?,?)', tuple(x[0] for x in self.attempts)),
                ('root_native_handoffs', 'job_id IN (?,?)', tuple(x[0] for x in self.attempts)),
                ('execution_epoch_handoffs', 'id IN (?,?)', tuple('epoch-'+x[0] for x in self.attempts)),
            )
            return [[tuple(row) for row in coord.db.execute(f'SELECT * FROM {table} WHERE {where} ORDER BY rowid', ids)]
                    for table, where, ids in earlier]

    def assert_stop_active(self):
        with contextlib.closing(Coordinator(self.path, self.project_root)) as coord:
            self.assertEqual(coord.db.execute('SELECT active FROM root_team_stops WHERE id=?',
                                              (self.bootstrap['parked_stop'],)).fetchone()[0], 1)

    def test_distinct_second_starts_once_and_preserves_existing_records(self):
        self.retire_both()
        earlier = self.earlier_records()
        receipt = self.second()
        self.assertEqual(receipt['native_turn_id'], 'second-native-turn')
        self.assertEqual(receipt['continuation_id'], self.arguments['continuation_id'])
        self.assertEqual([method for method, _ in self.native_calls],
                         ['thread/read', 'thread/turns/list', 'thread/queue/list', 'thread/read', 'turn/start'])
        self.native_calls.clear()
        self.assertEqual(self.second(), receipt)
        with self.assertRaisesRegex(CoordError, 'different exact request or reply'):
            self.second(message_id=self.reply('A third administrative reply'), continuation_id=str(uuid.uuid4()))
        with self.assertRaisesRegex(CoordError, 'different exact request or reply'):
            self.second(first_retirement_id=self.arguments['second_retirement_id'],
                        second_retirement_id=self.arguments['first_retirement_id'])
        legacy = self.root('turnover-bootstrap-continue', project=self.project_id,
            request_id=self.bootstrap['request_id'], message_id=self.first_message, confirm=True)
        self.assertEqual(legacy['native_turn_id'], self.first_receipt['native_turn_id'])
        self.assertEqual(self.native_calls, [])
        self.assertEqual(self.earlier_records(), earlier)
        self.assert_stop_active()

    def test_other_agent_settings_loads_do_not_block_the_po_continuation(self):
        self.retire_both()
        from agent_chat.core import execution_epoch_handoff
        with contextlib.closing(Coordinator(self.path, self.project_root)) as coord:
            with coord.tx() as db:
                worker = self.attempts[0][1]
                execution_epoch_handoff(db, 'thread/resume', self.po_thread, worker, 'mac')
                execution_epoch_handoff(db, 'thread/resume', str(uuid.uuid4()), self.po['target_session'], 'mac')
        self.assertEqual(self.second()['native_turn_id'], 'second-native-turn')
        self.assertEqual(self.start_count(), 1)

    def test_unloaded_po_can_start_only_after_one_exact_metadata_load_and_fresh_checks(self):
        self.retire_both()
        self.thread_state = 'notLoaded'
        receipt = self.second()
        self.assertEqual(receipt['native_turn_id'], 'second-native-turn')
        self.assertEqual([method for method, _ in self.native_calls],
                         ['thread/read', 'thread/resume', 'thread/read', 'thread/turns/list',
                          'thread/queue/list', 'thread/read', 'turn/start'])
        calls = list(self.native_calls)
        self.assertEqual(self.second(), receipt)
        self.assertEqual(self.native_calls, calls)
        self.assert_stop_active()

    def test_unloaded_po_with_unavailable_direct_input_cannot_start(self):
        self.retire_both()
        self.thread_state = 'notLoaded'
        self.direct_input = None
        with self.assertRaisesRegex(CoordError, 'idle exact PO thread'):
            self.second()
        self.assertEqual([method for method, _ in self.native_calls],
                         ['thread/read', 'thread/resume', 'thread/read'])
        self.assert_stop_active()

    def test_requires_two_exact_retirements_and_a_distinct_source_linked_reply(self):
        args = self.arguments
        with self.assertRaisesRegex(CoordError, 'both exact uncertain parked retirements'):
            self.second()
        self.retire(self.project_id, self.bootstrap, self.po, self.attempts[0][0])
        with self.assertRaisesRegex(CoordError, 'both exact uncertain parked retirements'):
            self.second()
        self.retire(self.project_id, self.bootstrap, self.po, self.attempts[1][0])
        for changes, error in (
                (dict(second_retirement_id=args['first_retirement_id']), 'distinct canonical UUIDs'),
                (dict(second_retirement_id=str(uuid.uuid4())), 'both exact uncertain parked retirements'),
                (dict(message_id=self.source), 'exact Root reply'),
                (dict(message_id=self.first_message), 'distinct Root reply'),
                (dict(message_id=self.older_message), 'distinct Root reply'),
                (dict(confirm=False), 'explicit confirmation')):
            with self.subTest(changes=changes), self.assertRaisesRegex(CoordError, error):
                self.second(**changes)
        self.assertEqual(self.native_calls, [])

    def test_requires_exact_terminal_prior_turn_idle_queue_and_active_stop(self):
        self.retire_both()
        for attr, value, error in (
                ('thread_state', 'active', 'idle exact PO thread'),
                ('prior_status', 'inProgress', 'exact terminal prior turn'),
                ('prior_id', 'later-turn', 'exact terminal prior turn'),
                ('queue', [{'id': 'another-input'}], 'empty native queue')):
            with self.subTest(attr=attr), mock.patch.object(self, attr, value):
                with self.assertRaisesRegex(CoordError, error):
                    self.second()
        self.assertEqual(self.start_count(), 0)
        with contextlib.closing(Coordinator(self.path, self.project_root)) as coord:
            from agent_chat.core import execution_epoch_handoff
            with coord.tx() as db:
                execution_epoch_handoff(db, 'turn/steer', self.po_thread, self.po['target_session'], 'mac')
            with self.assertRaisesRegex(CoordError, 'unsettled native input or resume'):
                self.second()
            coord.db.execute('UPDATE root_team_stops SET active=0 WHERE id=?', (self.bootstrap['parked_stop'],))
        self.assertEqual(self.start_count(), 0)
        self.native_calls.clear()
        with self.assertRaisesRegex(CoordError, 'stop, designation, verified job or binding changed'):
            self.second()
        self.assertEqual(self.native_calls, [])

    def assert_failed_submission_cannot_replay(self, error, status):
        self.retire_both()
        self.provider_error = error
        with self.assertRaises(type(error)):
            self.second()
        receipt = self.second()
        self.assertEqual(receipt['status'], status)
        self.assertIsNone(receipt['native_turn_id'])
        self.assertEqual(receipt['native_outcome'], 'unknown')
        if status == 'refused':
            self.assertEqual(receipt['provider_error'], str(error))
        with self.assertRaisesRegex(CoordError, 'different exact request or reply'):
            self.second(message_id=self.reply('Replacement for an uncertain continuation'),
                        continuation_id=str(uuid.uuid4()))
        self.assertEqual(self.start_count(), 1)
        with contextlib.closing(Coordinator(self.path, self.project_root)) as coord:
            epoch = coord.db.execute("""SELECT e.settled_at,e.outcome
                FROM root_turnover_second_continuations c JOIN execution_epoch_handoffs e ON e.id=c.epoch_id
                WHERE c.continuation_id=?""", (self.arguments['continuation_id'],)).fetchone()
            self.assertEqual(tuple(epoch), (None, None))
        self.assert_stop_active()

    def test_provider_refusal_is_retained_and_cannot_replay(self):
        self.assert_failed_submission_cannot_replay(RpcError(-32600, 'fixture native refusal'), 'refused')

    def test_unknown_native_response_is_retained_and_cannot_replay(self):
        self.assert_failed_submission_cannot_replay(TransportError('fixture lost response'), 'submitted_unknown')


def load_tests(loader, _standard_tests, _pattern):
    # The parent supplies disposable Root/bridge setup. Its tests run in its own module.
    test_case = SecondTurnoverContinuationTests
    return loader.suiteClass(test_case(name) for name in vars(test_case) if name.startswith('test_'))
