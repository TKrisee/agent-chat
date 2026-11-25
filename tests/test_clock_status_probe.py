from pathlib import Path
from unittest import TestSuite, mock
from test_admission_journal_probe import JournalFaults
from agent_chat import worker_admission_intent as journal
from agent_chat.core import CoordError

class ClockStatus(JournalFaults):
    def test_latest_attempt_uses_durable_order_after_clock_rollback(self):
        registry=Path(str(self.master)+'.projects.sqlite3')
        with mock.patch.object(journal.time, 'time', return_value=2000):
            first=journal._journal_begin(registry,self.root_sid,self.host,self.params)
            journal._journal_refused(registry,self.root_sid,self.params['request_id'],first,CoordError('worker intent exact worker/main/host/manager differs'))
        with mock.patch.object(journal.time, 'time', return_value=1000):
            second=journal._journal_begin(registry,self.root_sid,self.host,self.params)
        self.assertNotEqual(first,second)
        result=self.status()
        self.assertEqual(result['state'],'unknown')
        self.assertFalse(result['rollback_observed'])
        self.assertEqual(result['next_action'],'retain_unknown_no_replay')
        with self.assertRaisesRegex(CoordError,'outcome is unknown'):
            journal._journal_begin(registry,self.root_sid,self.host,self.params)

def load_tests(loader,tests,pattern):
    return TestSuite([ClockStatus('test_latest_attempt_uses_durable_order_after_clock_rollback')])
