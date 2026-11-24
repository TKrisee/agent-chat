import contextlib
import io
import os
from pathlib import Path
import tempfile
import threading
import unittest

from agent_chat import cli
from agent_chat.core import Coordinator
from agent_chat.remote import RemoteCoordError
from agent_chat.server import create_server


class OperatorReplyRoutingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='operator-reply-fixture-')
        self.db = Path(self.tmp.name) / 'state.sqlite3'
        self.body = Path(self.tmp.name) / 'body.txt'
        self.body.write_text('synthetic first acknowledgement')
        self.attachment = Path(self.tmp.name) / 'fixture-report.txt'
        self.attachment.write_text('synthetic report')
        self.token = 'fixture-only-token-123456789012345'
        self.old_host = os.environ.get('AGENT_CHAT_HOST_ID')
        os.environ['AGENT_CHAT_HOST_ID'] = 'fixture_host_routing_only'
        Coordinator(self.db).close()
        self.server = create_server(self.db, port=0, api_token=self.token, sessions_root=str(Path(self.tmp.name)/'sessions'))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.po = 'session_fixture_po'
        self.other = 'session_fixture_other'
        for sid, label in ((self.po, 'fixture-po'), (self.other, 'fixture-other')):
            with contextlib.closing(Coordinator(self.db, sid)) as coord:
                coord.register(label)
        self.operator = self.server.sender_session
        with contextlib.closing(Coordinator(self.db, self.operator)) as coord:
            coord._operator_message_origin = True
            self.source = coord.send(self.po, 'synthetic question')['id']
            coord.db.execute('DELETE FROM sessions WHERE id=?', (self.operator,))

    def tearDown(self):
        self.server.shutdown()
        self.thread.join(5)
        self.server.server_close()
        if self.old_host is None: os.environ.pop('AGENT_CHAT_HOST_ID', None)
        else: os.environ['AGENT_CHAT_HOST_ID'] = self.old_host
        self.tmp.cleanup()

    def call(self, *, sender=None, target=None, source=None, project='default'):
        args = ['--api-token', self.token, '--server', self.server.origin,
                '--session', sender or self.po, '--project', project, 'send',
                '--to', target or self.operator, '--reply-to', source or self.source,
                '--ack-reply', '--body-file', str(self.body), '--attach', str(self.attachment)]
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            result = cli.main(args)
        return result, out.getvalue()

    def query(self, sql, params=()):
        with contextlib.closing(Coordinator(self.db)) as coord:
            return [tuple(row) for row in coord.db.execute(sql, params)]

    def test_first_ack_idempotency_and_changed_payload(self):
        first = self.call(); second = self.call()
        self.assertEqual(first, second)
        rows = self.query('SELECT m.sender_session,m.recipient_session,r.reply_to FROM messages m JOIN message_replies r ON r.message_id=m.id WHERE r.reply_to=?', (self.source,))
        self.assertEqual(rows, [(self.po, self.operator, self.source)])
        self.assertEqual(len(self.query('SELECT a.id FROM attachments a JOIN message_replies r ON r.message_id=a.message_id WHERE r.reply_to=?', (self.source,))), 1)
        self.assertIsNotNone(self.query('SELECT acked_at FROM messages WHERE id=?', (self.source,))[0][0])
        self.assertEqual(len(self.query('SELECT message_id FROM reply_ack_idempotency WHERE sender_session=? AND reply_to=?', (self.po,self.source))), 1)
        self.body.write_text('synthetic first acknowledgement')
        self.attachment.write_text('synthetic changed report')
        with self.assertRaises(RemoteCoordError): self.call()
        self.assertEqual(len(self.query('SELECT message_id FROM reply_ack_idempotency WHERE sender_session=? AND reply_to=?', (self.po,self.source))), 1)
        self.attachment.write_text('synthetic report')
        self.body.write_text('synthetic changed acknowledgement')
        with self.assertRaises(RemoteCoordError): self.call()
        self.assertEqual(len(self.query('SELECT message_id FROM reply_ack_idempotency WHERE sender_session=? AND reply_to=?', (self.po,self.source))), 1)

    def test_wrong_target_sender_and_project_are_rejected(self):
        for args in ({'target':self.other}, {'sender':self.other}, {'project':'synthetic-wrong-project'}):
            with self.subTest(args=args), self.assertRaises(RemoteCoordError): self.call(**args)
        self.assertEqual(self.query('SELECT message_id FROM message_replies WHERE reply_to=?', (self.source,)), [])

    def test_unstamped_origin_is_rejected(self):
        with contextlib.closing(Coordinator(self.db)) as coord:
            coord.db.execute('INSERT INTO messages(id,sender_session,recipient_session,body,created_at) VALUES(?,?,?,?,?)', ('message_fixture_unstamped',self.operator,self.po,'untrusted',1))
        with self.assertRaises(RemoteCoordError): self.call(source='message_fixture_unstamped')
        self.assertEqual(self.query('SELECT message_id FROM message_replies WHERE reply_to=?', ('message_fixture_unstamped',)), [])


if __name__ == '__main__': unittest.main(verbosity=2)
