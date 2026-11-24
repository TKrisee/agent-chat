"""Ordinary replies and explicit ACK retries remain distinct effects."""
import contextlib
import subprocess
import time
import uuid
import unittest
from agent_chat.core import Coordinator
from agent_chat.remote import RemoteCoordError
from test_large_attachment_transfer import LargeAttachmentRequirementTests


class FirstReplyOmissionProbe(LargeAttachmentRequirementTests):
    def test_fully_read_ordinary_reply_and_explicit_ack_retry_are_distinct(self):
        source = self.call('send',session='large-recipient',to='large-sender',body='New private blocker')['id']
        self.call('message',session='large-sender',id=source)
        with contextlib.closing(Coordinator(self.db)) as coord:
            self.assertTrue(coord.db.execute("SELECT 1 FROM message_reads WHERE message_id=? AND reader_session='large-sender' AND provenance='recipient_full_message'",(source,)).fetchone())
            before = coord.db.execute('SELECT COUNT(*) FROM messages').fetchone()[0]
        self.body.write_text('Dependency repaired; continue original task.')
        args = ['send','--to','large-recipient','--body-file',str(self.body),'--reply-to',source]
        self.command('large-sender',*args)
        with contextlib.closing(Coordinator(self.db)) as coord:
            after = coord.db.execute('SELECT COUNT(*) FROM messages').fetchone()[0]
            ack = coord.db.execute('SELECT acked_at FROM messages WHERE id=?',(source,)).fetchone()[0]
            association = coord.db.execute('SELECT COUNT(*) FROM reply_ack_idempotency WHERE sender_session=? AND reply_to=?',('large-sender',source)).fetchone()[0]
            self.assertEqual(after,before+1)
            self.assertIsNone(ack); self.assertEqual(association,0)
        with self.lose_response(lambda value: value.get('acknowledged_reply') is True):
            with self.assertRaises(RemoteCoordError): self.command('large-sender',*args,'--ack-reply')
        lookup = self.call('first-ack-lookup',session='large-sender',source_id=source,expected_recipient='large-recipient')
        self.assertEqual(lookup['status'],'recorded')
        output = self.command('large-sender',*args,'--ack-reply')
        reply = subprocess.run(['jq','-er','.id'],input=output,text=True,capture_output=True,check=True).stdout.strip()
        self.assertEqual(lookup['reply_id'],reply)
        with contextlib.closing(Coordinator(self.db)) as coord:
            self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM messages').fetchone()[0],before+2)
            self.assertIsNotNone(coord.db.execute('SELECT acked_at FROM messages WHERE id=?',(source,)).fetchone()[0])
        self.body.write_text('Changed first ACK payload.')
        with self.assertRaises(RemoteCoordError): self.command('large-sender',*args,'--ack-reply')
        self.body.write_text('Later authorized progress.')
        self.command('large-sender',*args)
        with contextlib.closing(Coordinator(self.db)) as coord:
            self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM messages').fetchone()[0],before+3)

    def test_legacy_ordinary_reply_history_is_not_rewritten(self):
        source = self.call('send',session='large-recipient',to='large-sender',body='Historical private source')['id']
        self.body.write_text('Existing unacknowledged reply.')
        args = ['send','--to','large-recipient','--body-file',str(self.body),'--reply-to',source]
        # Retained pre-existing ordinary message; never create an ACK/read receipt.
        legacy_id = 'message_' + uuid.uuid4().hex
        with contextlib.closing(Coordinator(self.db)) as coord, coord.tx() as db:
            db.execute('INSERT INTO messages(id,sender_session,recipient_session,body,created_at) VALUES(?,?,?,?,?)',
                       (legacy_id,'large-sender','large-recipient','Existing unacknowledged reply.',time.time()))
            db.execute('INSERT INTO message_replies(message_id,reply_to) VALUES(?,?)',(legacy_id,source))
        self.call('message',session='large-sender',id=source)
        self.body.write_text('Later legacy progress.')
        self.command('large-sender',*args)
        with contextlib.closing(Coordinator(self.db)) as coord:
            self.assertIsNone(coord.db.execute('SELECT acked_at FROM messages WHERE id=?',(source,)).fetchone()[0])
            self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM reply_ack_idempotency WHERE reply_to=?',(source,)).fetchone()[0],0)

    def test_unread_ordinary_reply_does_not_fabricate_read_or_ack(self):
        source = self.call('send',session='large-recipient',to='large-sender',body='Unread new source')['id']
        self.body.write_text('Premature ordinary reply.')
        args = ['send','--to','large-recipient','--body-file',str(self.body),'--reply-to',source]
        self.command('large-sender',*args)
        with contextlib.closing(Coordinator(self.db)) as coord:
            self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM message_replies WHERE reply_to=?',(source,)).fetchone()[0],1)
            self.assertIsNone(coord.db.execute('SELECT acked_at FROM messages WHERE id=?',(source,)).fetchone()[0])
        self.call('inbox',session='large-sender')
        with contextlib.closing(Coordinator(self.db)) as coord:
            self.assertFalse(coord.db.execute("SELECT 1 FROM message_reads WHERE message_id=? AND reader_session='large-sender' AND provenance IN ('recipient_full_message','recipient_context_full')",(source,)).fetchone())

    def test_operator_self_and_batch_semantics_are_preserved(self):
        from agent_chat.web import operator_session
        operator = operator_session(self.db)
        with contextlib.closing(Coordinator(self.db,'large-recipient')) as coord:
            incoming = coord.send(operator,'Operator source')['id']
            batch = coord.send_many(['large-sender','large-unrelated'],'Batch source')
            batch_source = next(item['id'] for item in batch if item['recipient_session']=='large-sender')
        with contextlib.closing(Coordinator(self.db,operator)) as coord:
            coord.message(incoming)
            self.assertEqual(coord.send('large-recipient','Operator reply',reply_to=incoming)['reply_to'],incoming)
        with contextlib.closing(Coordinator(self.db,'large-sender')) as coord:
            own = coord.send('large-sender','Own source')['id']
            self.assertEqual(coord.send('large-sender','Own reply',reply_to=own)['reply_to'],own)
            coord.message(batch_source)
            self.assertEqual(coord.send('large-recipient','Batch reply',reply_to=batch_source)['reply_to'],batch_source)

    def test_existing_explicit_ack_does_not_fabricate_a_read(self):
        source = self.call('send',session='large-recipient',to='large-sender',body='Unread source')['id']
        self.body.write_text('Explicit reply.')
        self.command('large-sender','send','--to','large-recipient','--body-file',str(self.body),'--reply-to',source,'--ack-reply')
        with contextlib.closing(Coordinator(self.db)) as coord:
            self.assertIsNotNone(coord.db.execute('SELECT acked_at FROM messages WHERE id=?',(source,)).fetchone()[0])
            self.assertFalse(coord.db.execute("SELECT 1 FROM message_reads WHERE message_id=? AND reader_session='large-sender' AND provenance IN ('recipient_full_message','recipient_context_full')",(source,)).fetchone())

    def test_agent_reply_to_server_operator_can_be_ordinary(self):
        from agent_chat.web import operator_session
        operator = operator_session(self.db)
        with contextlib.closing(Coordinator(self.db,operator)) as coord:
            source=coord.send('large-sender','Operator request')['id']
        self.call('message',session='large-sender',id=source)
        self.body.write_text('Substantive response.')
        args=['send','--to',operator,'--body-file',str(self.body),'--reply-to',source]
        result=self.command('large-sender',*args)
        subprocess.run(['jq','-e','.acknowledged_reply == null'],input=result,text=True,capture_output=True,check=True)


def load_tests(loader, tests, pattern):
    return unittest.TestSuite([FirstReplyOmissionProbe('test_fully_read_ordinary_reply_and_explicit_ack_retry_are_distinct'),FirstReplyOmissionProbe('test_legacy_ordinary_reply_history_is_not_rewritten'),FirstReplyOmissionProbe('test_unread_ordinary_reply_does_not_fabricate_read_or_ack'),FirstReplyOmissionProbe('test_operator_self_and_batch_semantics_are_preserved'),FirstReplyOmissionProbe('test_existing_explicit_ack_does_not_fabricate_a_read'),FirstReplyOmissionProbe('test_agent_reply_to_server_operator_can_be_ordinary')])
