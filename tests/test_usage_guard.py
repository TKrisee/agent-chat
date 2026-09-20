import tempfile
import unittest
from pathlib import Path

from agent_chat.core import CoordError
from agent_chat.rpc import RpcError, TransportError
from agent_chat.usage import UsageStore
from agent_chat.usage_guard import UsageGuard, weekly_allowance


def quota(used=47, key='primary'):
    return {'rateLimits': {key: {'usedPercent': used, 'windowDurationMins': 10080, 'resetsAt': 500}}}


class UsageRpc:
    def __init__(self):
        self.calls = []
        self.quota = quota()
        self.threads = {'root': 'active', 'child': 'active', 'idle': 'idle'}
        self.fail = set()
        self.closed = 0

    def connect(self): return self
    def close(self): self.closed += 1

    def request(self, method, params):
        self.calls.append((method, params))
        thread = params.get('threadId')
        if (method, thread) in self.fail:
            raise RpcError(-1, 'test failure')
        if method == 'account/rateLimits/read':
            if isinstance(self.quota, Exception): raise self.quota
            return self.quota
        if method == 'thread/loaded/list':
            return ({'data': ['idle'], 'nextCursor': None} if params.get('cursor')
                    else {'data': ['root', 'child'], 'nextCursor': 'page2'})
        if method == 'thread/read':
            assert params['includeTurns'] is False
            return {'thread': {'status': {'type': self.threads[thread]}}}
        if method == 'thread/turns/list':
            assert params == {'threadId': thread, 'limit': 1, 'itemsView': 'summary', 'sortDirection': 'desc'}
            return {'data': [{'id': thread + '-turn', 'status': 'inProgress'}]}
        if method == 'turn/interrupt':
            self.threads[thread] = 'idle'
            return {}
        if method == 'thread/backgroundTerminals/clean': return {}
        raise AssertionError('Unexpected Codex operation: ' + method)


class QuotaParsingTests(unittest.TestCase):
    def test_weekly_primary_secondary_and_most_restrictive_bucket(self):
        self.assertEqual(weekly_allowance(quota()), (53, 500))
        self.assertEqual(weekly_allowance(quota(70, 'secondary')), (30, 500))
        result = quota(1)
        result['rateLimitsByLimitId'] = {'codex': quota(40)['rateLimits'], 'other': quota(80)['rateLimits']}
        self.assertEqual(weekly_allowance(result), (20, 500))
        self.assertEqual(weekly_allowance(quota(101)), (0, 500))

    def test_absent_or_malformed_weekly_data_is_unknown(self):
        wrong_window = quota()
        wrong_window['rateLimits']['primary']['windowDurationMins'] = 300
        for data in (None, {}, wrong_window, quota(True), quota(float('nan')), quota(-1)):
            with self.subTest(data=data), self.assertRaises(CoordError):
                weekly_allowance(data)


class UsageGuardTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.store = UsageStore(Path(directory.name) / 'usage.sqlite3')
        self.now = 100
        self.http_calls, self.http_fail = [], set()
        self.guard = UsageGuard(self, 'host', clock=lambda: self.now)
        self.rpc = UsageRpc()

    def call(self, path, payload):
        self.assertEqual(path, '/api/usage/rpc')
        self.http_calls.append(payload)
        payload = dict(payload)
        op = payload.pop('op')
        if op in self.http_fail: raise CoordError('server unavailable')
        return getattr(self.store, op)(**payload, now=self.now)

    def enable(self): self.store.configure(True, 30, now=self.now)

    def methods(self, method): return [p for m, p in self.rpc.calls if m == method]

    def test_cutoff_stops_root_child_and_background_tools_without_resuming_history(self):
        self.enable()
        self.rpc.quota = quota(70)
        result = self.guard.check(self.rpc)
        self.assertTrue(result['blocked'])
        self.assertEqual(result['stopped_threads'], 2)
        self.assertEqual({p['threadId'] for p in self.methods('turn/interrupt')}, {'root', 'child'})
        self.assertEqual({p['threadId'] for p in self.methods('thread/backgroundTerminals/clean')}, {'root', 'child', 'idle'})
        self.assertIsNone(result['enforcement_error'])
        self.guard.check(self.rpc)
        self.assertEqual(len(self.methods('turn/interrupt')), 2)
        self.assertEqual(len(self.methods('thread/backgroundTerminals/clean')), 3)

    def test_reset_and_restarting_guard_do_not_resume_until_manual_clear(self):
        self.enable()
        self.rpc.quota = quota(70)
        self.guard.check(self.rpc)
        self.rpc.quota = quota(0)
        self.guard = UsageGuard(self, 'host', clock=lambda: self.now)
        self.assertTrue(self.guard.check(self.rpc)['blocked'])
        self.store.resume(now=self.now)
        self.assertFalse(self.guard.check(self.rpc)['blocked'])

    def test_new_manual_turn_is_interrupted_while_paused(self):
        self.enable()
        self.rpc.quota = quota(80)
        self.guard.check(self.rpc)
        self.rpc.threads['root'] = 'active'
        self.guard.check(self.rpc)
        self.assertEqual(len(self.methods('turn/interrupt')), 3)
        self.assertEqual(self.rpc.threads['root'], 'idle')

    def test_stop_failure_does_not_prevent_other_threads_and_retries(self):
        self.enable()
        self.rpc.quota = quota(80)
        self.rpc.fail.add(('turn/interrupt', 'root'))
        result = self.guard.check(self.rpc)
        self.assertIn('test failure', result['enforcement_error'])
        self.assertEqual(self.rpc.threads['child'], 'idle')
        self.assertEqual(self.rpc.threads['root'], 'active')
        self.rpc.fail.clear()
        self.assertIsNone(self.guard.check(self.rpc)['enforcement_error'])
        self.assertEqual(self.rpc.threads['root'], 'idle')

    def test_unknown_quota_blocks_enabled_guard_and_recovers_without_reserve_latch(self):
        self.enable()
        self.rpc.quota = TransportError('quota unavailable')
        result = self.guard.check(self.rpc)
        self.assertTrue(result['blocked'])
        self.assertFalse(result['paused'])
        self.assertEqual(self.rpc.closed, 1)
        self.rpc.quota = quota(20)
        self.now += 10
        self.assertFalse(self.guard.check(self.rpc)['blocked'])

    def test_disabled_unknown_quota_does_not_interrupt(self):
        self.rpc.quota = {}
        self.assertFalse(self.guard.check(self.rpc)['blocked'])
        self.assertEqual(self.methods('turn/interrupt'), [])

    def test_lost_policy_stops_only_previously_enabled_guard(self):
        self.http_fail.add('status')
        self.assertTrue(self.guard.check(self.rpc)['blocked'])
        self.assertEqual(self.methods('turn/interrupt'), [])
        self.http_fail.clear()
        self.enable()
        self.assertFalse(self.guard.check(self.rpc)['blocked'])
        self.http_fail.add('status')
        self.assertTrue(self.guard.check(self.rpc)['blocked'])
        self.assertEqual(len(self.methods('turn/interrupt')), 2)

    def test_lost_low_quota_report_still_interrupts_locally(self):
        self.enable()
        self.rpc.quota = quota(75)
        self.http_fail.add('report')
        self.assertTrue(self.guard.check(self.rpc)['blocked'])
        self.assertEqual(len(self.methods('turn/interrupt')), 2)

    def test_quota_poll_is_throttled_but_global_policy_checked_every_pass(self):
        self.enable()
        self.guard.check(self.rpc)
        self.now += 1
        self.store.report('other', 25, 500, now=self.now)
        self.assertTrue(self.guard.check(self.rpc)['blocked'])
        self.assertEqual(len(self.methods('account/rateLimits/read')), 1)
        self.assertEqual(len([p for p in self.http_calls if p['op'] == 'status']), 2)
        self.now += 10
        self.guard.check(self.rpc)
        self.assertEqual(len(self.methods('account/rateLimits/read')), 2)


if __name__ == '__main__':
    unittest.main()
