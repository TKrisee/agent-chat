import contextlib
import json
import pathlib
import sqlite3
import subprocess
import sys
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
CLI = ROOT / 'bin' / 'agent-chat'
sys.path.insert(0, str(ROOT / 'src'))
from agent_chat.core import CoordError, Coordinator


class ReplyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='agent-chat-reply-')
        self.db = pathlib.Path(self.tmp.name) / 'state.sqlite3'
        self.a, self.b, self.c = (self.register(name) for name in ('a', 'b', 'c'))

    def tearDown(self): self.tmp.cleanup()

    def register(self, name):
        coord = Coordinator(self.db, name)
        try: return coord.register(name)['session']
        finally: coord.close()

    def send(self, sender, target, body, reply_to=None):
        coord = Coordinator(self.db, sender)
        try: return coord.send(target, body, reply_to=reply_to)
        finally: coord.close()

    def test_cli_send_and_inbox_include_reply_metadata(self):
        parent = self.send(self.a, self.b, 'parent')
        body = pathlib.Path(self.tmp.name) / 'body.txt'; body.write_text('child')
        result = subprocess.run([str(CLI), '--db', str(self.db), '--session', self.b,
                                 'send', '--to', self.a, '--body-file', str(body), '--reply-to', parent['id']],
                                text=True, capture_output=True, check=True)
        child = json.loads(result.stdout)
        self.assertEqual(child['reply_to'], parent['id'])
        result = subprocess.run([str(CLI), '--db', str(self.db), '--session', self.a,
                                 'inbox', '--all'], text=True, capture_output=True, check=True)
        message = json.loads(result.stdout)['messages'][-1]
        self.assertEqual((message['reply_to'], message['reply_preview']['body']), (parent['id'], 'parent'))

    def test_reply_links_persist_and_reject_unknown_or_other_pair_atomically(self):
        parent = self.send(self.a, self.b, 'parent')
        child = self.send(self.b, self.a, 'child', parent['id'])
        coord = Coordinator(self.db, self.a)
        try: inbox = coord.inbox(True)['messages']
        finally: coord.close()
        self.assertEqual(inbox[-1]['reply_to'], parent['id'])
        unrelated = self.send(self.a, self.c, 'other')
        for bad in ('message_missing', unrelated['id']):
            with contextlib.closing(sqlite3.connect(self.db)) as db: before = db.execute('SELECT COUNT(*) FROM messages').fetchone()[0]
            with self.assertRaises(CoordError): self.send(self.b, self.a, 'nope', bad)
            with contextlib.closing(sqlite3.connect(self.db)) as db: self.assertEqual(db.execute('SELECT COUNT(*) FROM messages').fetchone()[0], before)
        self.assertEqual(child['reply_preview']['id'], parent['id'])

    def test_link_reply_requires_sender_and_is_idempotent(self):
        parent = self.send(self.a, self.b, 'first parent')
        alternate = self.send(self.a, self.b, 'second parent')
        child = self.send(self.a, self.b, 'child')
        coord = Coordinator(self.db, self.b)
        try: coord.acknowledge(child['id'])
        finally: coord.close()
        result = subprocess.run([str(CLI), '--db', str(self.db), '--session', self.a,
                                 'link-reply', child['id'], '--reply-to', parent['id']],
                                text=True, capture_output=True, check=True)
        self.assertTrue(json.loads(result.stdout)['linked'])
        with contextlib.closing(sqlite3.connect(self.db)) as db:
            before = db.execute('SELECT id,body,acked_at FROM messages WHERE id=?', (child['id'],)).fetchone()
        coord = Coordinator(self.db, self.a)
        try:
            self.assertTrue(coord.link_reply(child['id'], parent['id'])['linked'])
            with self.assertRaises(CoordError): coord.link_reply(child['id'], alternate['id'])
            with self.assertRaises(CoordError): coord.link_reply(child['id'], 'message_missing')
            wrong_pair = self.send(self.a, self.c, 'other conversation')
            with self.assertRaises(CoordError): coord.link_reply(child['id'], wrong_pair['id'])
            unlinked = self.send(self.a, self.b, 'unlinked')
            newer = self.send(self.a, self.b, 'newer')
            with self.assertRaises(CoordError): coord.link_reply(unlinked['id'], newer['id'])
        finally: coord.close()
        with contextlib.closing(sqlite3.connect(self.db)) as db:
            after = db.execute('SELECT id,body,acked_at FROM messages WHERE id=?', (child['id'],)).fetchone()
            linked = db.execute('SELECT reply_to FROM message_replies WHERE message_id=?', (child['id'],)).fetchone()
        self.assertEqual(after, before)
        self.assertEqual(linked[0], parent['id'])
        coord = Coordinator(self.db, self.b)
        try:
            with self.assertRaises(CoordError): coord.link_reply(child['id'], parent['id'])
        finally: coord.close()
        coord = Coordinator(self.db, self.a)
        try:
            with self.assertRaises(CoordError): coord.link_reply(parent['id'], child['id'])
        finally: coord.close()


if __name__ == '__main__': unittest.main()
