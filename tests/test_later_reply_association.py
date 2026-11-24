"""Synthetic separately authorized progress preserves both conversation sources."""
import contextlib
import sqlite3
import tempfile
import unittest
from pathlib import Path
from agent_chat.core import Coordinator

class LaterReplyAssociation(unittest.TestCase):
    def test_later_request_has_its_own_ack_and_progress_association(self):
        with tempfile.TemporaryDirectory() as directory:
            db=Path(directory)/'coord.sqlite3'
            with contextlib.closing(Coordinator(db,'manager')) as manager:
                manager.register('manager')
            with contextlib.closing(Coordinator(db,'worker')) as worker:
                worker.register('worker')
            with contextlib.closing(Coordinator(db,'manager')) as manager:
                original=manager.send('worker','Initial assignment')
            with contextlib.closing(Coordinator(db,'worker')) as worker:
                worker.message(original['id'])
                first=worker.send('manager','Accepted and next action',reply_to=original['id'],ack_reply=True)
            with contextlib.closing(Coordinator(db,'manager')) as manager:
                manager.message(first['id'])
                followup=manager.send('worker','New separately authorized follow-up',reply_to=first['id'],ack_reply=True)
            with contextlib.closing(Coordinator(db,'worker')) as worker:
                worker.message(followup['id'])
                second=worker.send('manager','Follow-up accepted and next action',reply_to=followup['id'],ack_reply=True)
                later=worker.send('manager','Later progress for follow-up',reply_to=followup['id'])
                legacy=worker.send('manager','Synthetic legacy compatibility',reply_to=original['id'])
                repeated=worker.send('manager','Follow-up accepted and next action',reply_to=followup['id'],ack_reply=True)
            self.assertNotEqual(original['id'],followup['id'])
            self.assertEqual(first['reply_to'],original['id'])
            self.assertEqual(second['reply_to'],followup['id'])
            self.assertEqual(later['reply_to'],followup['id'])
            self.assertNotEqual(later['reply_to'],original['id'])
            self.assertEqual(legacy['reply_to'],original['id'])  # Compatibility, not current guidance.
            self.assertNotEqual(later['id'],second['id'])
            self.assertEqual(repeated['id'],second['id'])
            with sqlite3.connect(db) as connection:
                associations=connection.execute('SELECT sender_session,reply_to,message_id FROM reply_ack_idempotency WHERE sender_session=? ORDER BY reply_to',('worker',)).fetchall()
            self.assertEqual(set(associations),{('worker',original['id'],first['id']),('worker',followup['id'],second['id'])})
