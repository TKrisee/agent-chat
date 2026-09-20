import pathlib
import sys
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from agent_chat.usage import UsageStore
from agent_chat.core import CoordError


class UsageStoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = UsageStore(pathlib.Path(self.tmp.name) / 'state.sqlite3')

    def tearDown(self):
        self.tmp.cleanup()

    def test_latch_and_manual_resume_require_fresh_healthy_reports(self):
        self.store.configure(True, 30, now=100)
        self.store.report('one', 30, 500, now=101)
        self.assertTrue(self.store.status(now=102)['paused'])
        with self.assertRaises(CoordError):
            self.store.resume(now=102)
        self.store.report('one', 31, 500, now=103)
        self.assertFalse(self.store.resume(now=104)['paused'])

    def test_missing_and_error_reports_block_without_latching(self):
        self.store.status('one', now=100)
        self.store.configure(True, 30, now=100)
        status = self.store.status(now=101)
        self.assertTrue(status['blocked'])
        self.assertFalse(status['paused'])
        status = self.store.report('one', None, None, error='quota unavailable', now=102)
        self.assertTrue(status['blocked'])
        self.assertFalse(status['paused'])
        self.assertIsNone(status['hosts'][0]['remaining_percent'])

    def test_stale_host_does_not_block_a_fresh_host(self):
        self.store.report('old', 10, 500, now=1)
        self.store.report('new', 80, 500, now=100)
        status = self.store.configure(True, 30, now=100)
        self.assertFalse(status['paused'])
        self.assertFalse(status['blocked'])

    def test_enforcement_does_not_refresh_quota(self):
        self.store.report('one', 80, 500, now=1)
        self.store.enforcement('one', 2, 'stop failed', now=100)
        status = self.store.configure(True, 30, now=100)
        self.assertTrue(status['blocked'])
        self.assertEqual(status['hosts'][0]['stopped_threads'], 2)

    def test_raising_enabled_threshold_latches_against_fresh_report(self):
        self.store.report('one', 40, 500, now=100)
        self.assertFalse(self.store.configure(True, 30, now=101)['paused'])
        status = self.store.configure(True, 50, now=102)
        self.assertTrue(status['paused'])
        self.assertTrue(status['blocked'])

    def test_latch_persists_across_restart_and_healthy_reports_until_resume(self):
        self.store.configure(True, 30, now=100)
        self.store.report('one', 20, 500, now=101)
        restarted = UsageStore(self.store.path)
        status = restarted.report('one', 80, 600, now=102)
        self.assertTrue(status['paused'])
        self.assertTrue(status['blocked'])
        self.assertFalse(restarted.resume(now=103)['paused'])

    def test_invalid_numeric_values_are_rejected(self):
        for threshold in (True, -1, 101, float('nan'), float('inf')):
            with self.assertRaises(CoordError):
                self.store.configure(True, threshold, now=100)
        for remaining in (True, -1, 101, float('nan'), float('-inf')):
            with self.assertRaises(CoordError):
                self.store.report('one', remaining, 100, now=100)
        for reset in (True, -1, float('nan'), float('inf')):
            with self.assertRaises(CoordError):
                self.store.report('one', 50, reset, now=100)

    def test_stale_online_quota_blocks_but_offline_host_does_not(self):
        self.store.report('offline', 80, 500, now=1)
        self.store.report('online', 80, 500, now=1)
        self.store.enforcement('online', 0, now=100)
        status = self.store.configure(True, 30, now=100)
        self.assertTrue(status['blocked'])
        hosts = {item['host_id']: item for item in status['hosts']}
        self.assertFalse(hosts['offline']['recent'])
        self.assertTrue(hosts['online']['recent'])
