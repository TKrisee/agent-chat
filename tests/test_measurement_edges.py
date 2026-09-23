import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest import mock
import uuid

from agent_chat.bridge_state import BridgeState
from agent_chat.core import Coordinator
from agent_chat.measurement import MeasurementStore


class MeasurementEdgesTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='agent-chat-measurement-edges-')
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.db = self.base / 'state.sqlite3'
        self.thread_id = str(uuid.uuid4())
        coord = Coordinator(self.db, 'agent')
        try:
            coord.register('Agent')
            BridgeState(coord).bind(thread_id=self.thread_id)
        finally:
            coord.close()
        self.logs = self.base / 'sessions'
        self.day = self.logs / time.strftime('%Y/%m/%d')
        self.day.mkdir(parents=True)
        self.log = self.day / ('rollout-' + self.thread_id + '.jsonl')
        self.meta = {'type': 'session_meta', 'payload': {'id': self.thread_id,
                     'session_id': self.thread_id, 'thread_source': 'user'}}
        self.append(self.log, self.meta)
        self.store = MeasurementStore(self.db, sessions_root=self.logs)
        self.addCleanup(self.store.close)

    @staticmethod
    def append(log, record):
        with log.open('a') as stream:
            stream.write(json.dumps(record) + '\n')

    def record(self, identity='response', thread=None):
        return {'type': 'token_usage_record', 'payload': {
            'thread_id': thread or self.thread_id, 'session_id': self.thread_id,
            'response_id': identity, 'usage': {'input_tokens': 100,
            'cached_input_tokens': 80, 'output_tokens': 10, 'reasoning_output_tokens': 4}}}

    def begin(self, pause=False):
        self.store.start('default', self.db, 60, pause_at_end=pause)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            active = self.store.status('default')['active']
            if active and active['state'] == 'running':
                return active['id']
            time.sleep(.01)
        self.fail('measurement did not start')

    def test_baseline_is_excluded_and_guardian_is_separate(self):
        self.append(self.log, self.record('before-window'))
        guardian = str(uuid.uuid4())
        guardian_log = self.day / ('rollout-' + guardian + '.jsonl')
        self.append(guardian_log, {'type': 'session_meta', 'payload': {
            'id': guardian, 'session_id': self.thread_id, 'thread_source': 'guardian_review'}})
        self.begin()
        self.append(self.log, self.record('in-window'))
        self.append(guardian_log, self.record('review-response', guardian))
        report = self.store.stop('default')['latest']
        self.assertEqual(report['totals']['response_count'], 1)
        self.assertEqual(report['totals']['fresh_input_tokens'], 20)
        self.assertEqual(report['guardian']['response_count'], 1)
        self.assertEqual(report['guardian']['fresh_input_tokens'], 20)

    def test_restart_interrupts_without_sending_pause(self):
        requested = []
        self.store.pause_callback = lambda *args: requested.append(args)
        self.begin(pause=True)
        self.store.close()
        replacement = MeasurementStore(self.db, sessions_root=self.logs)
        self.addCleanup(replacement.close)
        report = replacement.status('default')
        self.assertIsNone(report['active'])
        self.assertEqual(report['latest']['state'], 'interrupted')
        self.assertEqual(requested, [])

    def test_deadline_finishes_without_polling_and_requests_opt_in_pause(self):
        requested = []
        self.store.pause_callback = lambda *args: requested.append(args)
        self.store.interval_seconds = .01
        measurement_id = self.begin(pause=True)
        self.append(self.log, self.record('timed-response'))
        with self.store._db() as db:
            db.execute('UPDATE usage_measurements SET deadline=? WHERE id=?', (time.time() + .05, measurement_id))
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            report = self.store.status('default')['latest']
            if report['state'] == 'complete' and report['pause_requested_at'] is not None:
                break
            time.sleep(.02)
        self.assertEqual(report['state'], 'complete')
        self.assertEqual(report['totals']['response_count'], 1)
        self.assertEqual(len(requested), 1)

    def test_initial_partial_line_does_not_block_later_complete_records(self):
        partial = json.dumps({'type': 'ignored_fixture_record', 'payload': 'partial'})
        with self.log.open('a') as out:
            out.write(partial[:20])
        self.begin()
        with self.log.open('a') as out:
            out.write(partial[20:] + '\n')
        self.append(self.log, self.record())
        report = self.store.stop('default')['latest']
        self.assertEqual(report['totals']['response_count'], 1)
        self.assertEqual(report['totals']['fresh_input_tokens'], 20)

    def test_truncation_does_not_replay_old_usage(self):
        self.append(self.log, {'type': 'ignored_fixture_record', 'payload': 'x' * 10000})
        self.begin()
        self.log.write_text(json.dumps(self.meta) + '\n')
        self.append(self.log, self.record('old-replayed-response'))
        report = self.store.stop('default')['latest']
        self.assertEqual(report['totals']['response_count'], 0)
        self.assertTrue(any('truncat' in error.lower() for error in report['errors']))
        self.assertIsNone(report['totals']['fresh_input_tokens'])

    def test_no_jq_is_visible_as_unavailable(self):
        with mock.patch('agent_chat.measurement.shutil.which', return_value=None):
            report = self.store.status('default')
            self.assertFalse(report['available'])
            self.assertIn('jq', report['error'])

    def test_final_large_tail_is_counted_or_explicitly_incomplete(self):
        self.begin()
        for _ in range(20):
            self.append(self.log, {'type': 'response_item', 'payload': {
                'type': 'function_call_output', 'output': 'x' * 100000}})
        self.append(self.log, self.record('after-large-tail'))
        report = self.store.stop('default')['latest']
        if not report['errors']:
            self.assertEqual(report['totals']['response_count'], 1)
            self.assertEqual(report['totals']['tool_result_text_characters'], 2000000)
        else:
            self.assertIsNone(report['totals']['fresh_input_tokens'])
