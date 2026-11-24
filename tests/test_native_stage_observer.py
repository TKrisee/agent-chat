"""Synthetic same-connection observer lifecycle; no provider input."""
import unittest
from unittest import mock
import agent_chat.native_stage_observer as observer
from agent_chat.native_stage_observer import StageWindows


class Rpc:
    timeout = 1

    def __init__(self):
        self.events = []
        self.epoch = 1
        self.snapshots = 0

    def connection_identity(self):
        return {'client_instance': 'c', 'connection_epoch': self.epoch}

    def notification_boundary(self, **_):
        return len(self.events)

    def notification_snapshot_since(self, cursor):
        self.snapshots += 1
        rows = [(i, e) for i, e in enumerate(self.events, 1) if i > cursor]
        stamps = [dict(sequence=i, connection_instance='c', connection_epoch=self.epoch,
                       observed_at=100.0+i, observed_monotonic_ns=1000+i) for i, _ in rows]
        return rows, stamps

    def emit(self, method, **params):
        self.events.append({'method': method, 'params': params})


class State:
    def __init__(self):
        self.records = []
        self.censors = []
        self.fail = False

    def native_stage_record(self, job, result):
        if self.fail:
            raise OSError('lost publish')
        self.records.append((job, result))

    def native_stage_censor(self, job, reason):
        if self.fail:
            raise OSError('lost censor')
        self.censors.append((job, reason))


class WindowLifecycle(unittest.TestCase):
    def setUp(self):
        self.rpc = Rpc()
        self.state = State()
        self.windows = StageWindows(self.rpc, self.state)

    def arm(self, job):
        self.windows.arm({'id': job, 'thread_id': 'thread'})

    def input(self, job, turn):
        self.rpc.emit('item/started', threadId='thread', turnId=turn,
                      item={'type': 'userMessage', 'clientId': job})

    def delta(self, turn):
        self.rpc.emit('item/agentMessage/delta', threadId='thread', turnId=turn,
                      itemId='item', delta='text')

    def terminal(self, turn):
        self.rpc.emit('turn/completed', threadId='thread', turn={'id': turn, 'status': 'completed'})

    def test_129_sequential_completed_jobs_reuse_capacity(self):
        for i in range(129):
            job, turn = f'job-{i}', f'turn-{i}'
            self.arm(job)
            self.assertIn(job, self.windows.windows)
            self.input(job, turn)
            self.delta(turn)
            self.terminal(turn)
            self.windows.poll()
        self.assertEqual(len(self.state.records), 129)
        self.assertEqual(len(self.windows.windows), 128)
        self.assertNotIn('job-0', self.windows.windows)
        self.assertTrue(self.windows.windows['job-128']['settled'])

    def test_inflight_and_failed_publish_are_retained_until_retry(self):
        self.arm('job')
        self.input('job', 'turn')
        self.delta('turn')
        self.state.fail = True
        self.windows.poll()
        self.terminal('turn')
        self.windows.poll()
        self.assertFalse(self.windows.windows['job']['settled'])
        self.state.fail = False
        self.windows.poll()
        self.assertTrue(self.windows.windows['job']['settled'])
        self.assertEqual(len(self.state.records), 1)

    def test_late_other_input_censors_before_settlement(self):
        self.arm('job')
        self.input('job', 'turn')
        self.delta('turn')
        self.windows.poll()
        self.rpc.emit('item/started', threadId='thread', turnId='turn',
                      item={'type': 'userMessage', 'clientId': 'other'})
        self.terminal('turn')
        self.windows.poll()
        self.assertEqual(self.state.censors, [('job', 'ambiguous_input')])
        self.assertTrue(self.windows.windows['job']['settled'])

    def test_failed_withdrawal_is_not_reclaimed(self):
        self.arm('job')
        self.input('job', 'turn')
        self.delta('turn')
        self.windows.poll()
        self.state.fail = True
        self.rpc.epoch = 2
        self.windows.poll()
        for i in range(127):
            self.arm(f'active-{i}')
        self.arm('extra')
        self.assertNotIn('extra', self.windows.windows)
        self.assertIn('job', self.windows.windows)
        self.state.fail = False
        self.windows.poll()
        self.arm('extra')
        self.assertIn('extra', self.windows.windows)
        self.assertEqual(self.state.censors, [('job', 'window_unavailable')])

    def test_gap_without_publication_reclaims_only_at_capacity(self):
        self.arm('job')
        self.rpc.epoch = 2
        self.windows.poll()
        self.assertIn('job', self.windows.windows)
        for i in range(127):
            self.arm(f'active-{i}')
        self.arm('extra')
        self.assertIn('extra', self.windows.windows)
        self.assertNotIn('job', self.windows.windows)

    def test_no_publisher_can_settle_and_reclaim(self):
        self.windows = StageWindows(self.rpc, object())
        self.arm('job')
        self.input('job', 'turn')
        self.delta('turn')
        self.terminal('turn')
        self.windows.poll()
        self.assertTrue(self.windows.windows['job']['settled'])
        for i in range(127):
            self.arm(f'active-{i}')
        self.arm('extra')
        self.assertIn('extra', self.windows.windows)
        self.assertNotIn('job', self.windows.windows)

    def test_malformed_terminal_params_do_not_interrupt_poll(self):
        self.arm('job')
        self.input('job', 'turn')
        self.delta('turn')
        self.rpc.events.append({'method': 'turn/completed', 'params': None})
        self.terminal('turn')
        self.windows.poll()
        self.assertEqual(self.windows.windows['job']['result']['status'], 'observed')
        self.assertTrue(self.windows.windows['job']['settled'])

    def test_terminal_scan_only_runs_on_new_valid_terminal(self):
        self.arm('job')
        self.input('job', 'turn')
        self.delta('turn')
        with mock.patch.object(observer, '_exact_terminal', wraps=observer._exact_terminal) as scan:
            self.windows.poll()
            self.windows.poll()
            self.assertEqual(scan.call_count, 0)
            self.terminal('turn')
            self.windows.poll()
            self.windows.poll()
            self.assertEqual(scan.call_count, 1)

    def test_closed_window_does_not_rescan_foreign_work(self):
        self.arm('job')
        self.input('job', 'turn')
        self.delta('turn')
        self.terminal('turn')
        self.windows.poll()
        snapshots = self.rpc.snapshots
        self.rpc.emit('item/started', threadId='other', turnId='other-turn',
                      item={'type': 'userMessage', 'clientId': 'other'})
        self.windows.poll()
        self.windows.poll()
        self.assertEqual(self.rpc.snapshots, snapshots)
        self.assertEqual(self.windows.windows['job']['result']['status'], 'observed')

    def test_late_conflicting_start_withdraws_closed_result(self):
        self.arm('job')
        self.input('job', 'turn')
        self.delta('turn')
        self.terminal('turn')
        self.windows.poll()
        self.windows.started('job', {'turn': {'id': 'different-turn'}})
        self.assertEqual(self.state.censors, [('job', 'turn_mismatch')])
        self.assertFalse(self.windows.windows['job']['settled'])

    def test_late_conflicting_start_retries_failed_withdrawal(self):
        self.arm('job')
        self.input('job', 'turn')
        self.delta('turn')
        self.terminal('turn')
        self.windows.poll()
        self.state.fail = True
        self.windows.started('job', {'turn': {'id': 'different-turn'}})
        self.assertFalse(self.windows.windows['job']['settled'])
        self.assertEqual(self.state.censors, [])
        self.state.fail = False
        self.windows.poll()
        self.assertEqual(self.state.censors, [('job', 'turn_mismatch')])
