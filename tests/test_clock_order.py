import tempfile, unittest, uuid
from pathlib import Path
from unittest.mock import patch
from agent_chat.core import CoordError
from agent_chat.worker_admission_intent import _journal_begin, _journal_refused

class ClockOrder(unittest.TestCase):
    def test_clock_rollback_does_not_replay_unresolved_newer_attempt(self):
        with tempfile.TemporaryDirectory(dir='/private/tmp') as td:
            db=Path(td)/'registry.db'
            params={'project':'private','request_id':str(uuid.uuid4())}
            with patch('agent_chat.worker_admission_intent.time.time',return_value=2000):
                _journal_begin(db,'root','host',params,enroll=True,enrollment_key=str(uuid.uuid4()))
                first=_journal_begin(db,'root','host',params)
                _journal_refused(db,'root',params['request_id'],first,CoordError('worker intent exact worker/main/host/manager differs'))
            with patch('agent_chat.worker_admission_intent.time.time',return_value=1000):
                second=_journal_begin(db,'root','host',params)
            self.assertNotEqual(first,second)
            with patch('agent_chat.worker_admission_intent.time.time',return_value=1001):
                with self.assertRaisesRegex(CoordError,'outcome is unknown'):
                    _journal_begin(db,'root','host',params)
if __name__=='__main__': unittest.main()
