"""Real HTTP/CLI group-send workflows; JSON assertions use jq."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from agent_chat.core import CoordError, Coordinator
from test_remote_web import RemoteWebFixture, TOKEN


class GroupSendTests(RemoteWebFixture):
    def command(self, *args, ok=True):
        env = {key: value for key, value in os.environ.items() if not key.startswith('AGENT_CHAT_')}
        env.update(AGENT_CHAT_SERVER=self.url, AGENT_CHAT_API_TOKEN=TOKEN,
                   AGENT_CHAT_HOST_ID='mac', AGENT_CHAT_STATE_DIR=self.tmp.name)
        client = Path(__file__).resolve().parents[1] / 'bin' / 'agent-chat-client'
        result = subprocess.run([sys.executable, str(client), *args], env=env,
                                capture_output=True, text=True, timeout=10)
        if ok:
            self.assertEqual(result.returncode, 0, result.stderr)
            return result.stdout
        self.assertNotEqual(result.returncode, 0)
        return result.stderr

    def assert_json(self, raw, expression, *args):
        result = subprocess.run(['jq', '-e', *args, expression], input=raw, text=True,
                                capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr or result.stdout)

    def jq_value(self, raw, expression):
        result = subprocess.run(['jq', '-er', expression], input=raw, text=True,
                                capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr or result.stdout)
        return result.stdout.strip()

    def setUp(self):
        super().setUp()
        for session, agent in (('a', 'alpha'), ('b', 'beta'), ('c', 'gamma')):
            self.command('--session', session, 'register', '--agent', agent)
        self.body = Path(self.tmp.name) / 'body.txt'
        self.body.write_text('Group update', encoding='utf-8')

    def message_count(self):
        coord = Coordinator(self.db)
        try:
            return coord.db.execute('SELECT COUNT(*) FROM messages').fetchone()[0]
        finally:
            coord.close()

    def test_explicit_and_implicit_groups_are_atomic_and_deduplicated(self):
        explicit = self.command('--session', 'a', 'send', '--to', 'b', '--to', 'b',
                                '--body-file', str(self.body))
        self.assert_json(explicit,
                         '(has("batch_id") and (.deliveries | length == 1) and .deliveries[0].recipient_session == "b" and (has("body") | not))')

        implicit = self.command('--session', 'a', 'send', '--body-file', str(self.body))
        self.assert_json(implicit,
                         '(.deliveries | length == 3) and all(.deliveries[]; .recipient_session != "a") and any(.deliveries[]; .recipient_session == $operator)',
                         '--arg', 'operator', self.server.sender_session)

        before = self.message_count()
        self.command('--session', 'a', 'send', '--to', 'b', '--to', 'missing',
                     '--body-file', str(self.body), ok=False)
        self.command('--session', 'a', 'send', '--reply-to', 'message-private',
                     '--body-file', str(self.body), ok=False)
        self.assertEqual(self.message_count(), before)

    def test_group_attachments_and_direct_reply_ack_remain_supported(self):
        image = Path(self.tmp.name) / 'proof.png'
        image.write_bytes(b'\x89PNG\r\n\x1a\nproof')
        grouped = self.command('--session', 'a', 'send', '--to', 'b', '--to', 'c',
                               '--body-file', str(self.body), '--attach', str(image))
        self.assert_json(grouped, '.deliveries | length == 2')
        self.assert_json(self.command('--session', 'b', 'inbox'),
                         '(.messages | length == 1) and (.messages[0].attachments | length == 1)')

        question = self.command('--session', 'b', 'send', '--to', 'a', '--body-file', str(self.body))
        question_id = self.jq_value(question, '.id')
        reply = self.command('--session', 'a', 'send', '--to', 'b', '--body-file', str(self.body),
                             '--reply-to', question_id, '--ack-reply')
        self.assert_json(reply, '.acknowledged_reply == true and .reply_to == $parent',
                         '--arg', 'parent', question_id)
        reply_id = self.jq_value(reply, '.id')
        self.assert_json(self.command('--session', 'a', 'send', '--to', 'b', '--body-file', str(self.body),
                                      '--reply-to', question_id, '--ack-reply'),
                         '.acknowledged_reply == true and .id == $reply', '--arg', 'reply', reply_id)
        self.command('--session', 'a', 'send', '--to', 'b', '--to', 'c', '--body-file', str(self.body),
                     '--ack-reply', ok=False)


class EmptyImplicitGroupTests(unittest.TestCase):
    def test_implicit_group_without_any_other_session_is_rejected_without_insertion(self):
        with tempfile.TemporaryDirectory(prefix='agent-chat-empty-group-') as directory:
            db = Path(directory) / 'state.sqlite3'
            sender = Coordinator(db, 'sender')
            sender.register('sender')
            try:
                with self.assertRaisesRegex(CoordError, 'no other registered sessions'):
                    sender.send_group_prepared(None, 'No recipients', [])
                self.assertEqual(sender.db.execute('SELECT COUNT(*) FROM messages').fetchone()[0], 0)
            finally:
                sender.close()
