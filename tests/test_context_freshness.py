import tempfile
import unittest
from pathlib import Path

from agent_chat.core import CoordError, Coordinator


class ContextFreshnessTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="agent-chat-context-freshness-")
        self.db = Path(self.tmp.name) / "state.sqlite3"
        self.a = self._register("a")
        self.b = self._register("b")

    def tearDown(self):
        self.tmp.cleanup()

    def _register(self, name):
        coord = Coordinator(self.db, name)
        try:
            return coord.register(name)["session"]
        finally:
            coord.close()

    def _send(self, body):
        coord = Coordinator(self.db, self.a)
        try:
            return coord.send(self.b, body)
        finally:
            coord.close()

    def test_acknowledged_message_remains_unread_until_full_context_consumption(self):
        message = self._send("please proceed")
        coord = Coordinator(self.db, self.b)
        try:
            coord.acknowledge(message["id"])
            with self.assertRaisesRegex(CoordError, "read your inbox"):
                coord.request("blocked-after-ack", 5)
            self.assertEqual([item['id'] for item in coord.context()["messages"]], [message['id']])
            self.assertEqual(coord.request("fresh-after-ack", 5)["state"], "owned")
        finally:
            coord.close()

    def test_acknowledgement_and_legacy_unknown_proof_never_make_fresh(self):
        historical = self._send("already acknowledged before read receipts")
        coord = Coordinator(self.db, self.b)
        try:
            coord.acknowledge(historical["id"])
        finally:
            coord.close()
        inspector = Coordinator(self.db)
        try:
            inspector.db.execute("DELETE FROM message_reads WHERE message_id=?", (historical["id"],))
            self.assertIsNone(inspector.db.execute("SELECT 1 FROM message_reads WHERE message_id=?",
                                                   (historical["id"],)).fetchone())
        finally:
            inspector.close()
        newer = self._send("new unread message")
        coord = Coordinator(self.db, self.b)
        try:
            with self.assertRaisesRegex(CoordError, "read your inbox"):
                coord.request("blocked-by-newer", 5)
            coord.acknowledge(newer["id"])
            with self.assertRaisesRegex(CoordError, "read your inbox"):
                coord.request("ack-is-not-fresh", 5)
            self.assertEqual(coord.message(historical['id'])['message']['id'], historical['id'])
            self.assertEqual(coord.message(newer['id'])['message']['id'], newer['id'])
            self.assertEqual(coord.request("genuine-recipient-read-is-fresh", 5)["state"], "owned")
        finally:
            coord.close()

    def test_filtered_or_truncated_context_does_not_bypass_unread_proof(self):
        self._send("x" * 8000)
        coord = Coordinator(self.db, self.b)
        try:
            self.assertEqual(coord.context(message_ids=[])["messages"], [])
            with self.assertRaisesRegex(CoordError, "read your inbox"):
                coord.request("filtered-context", 5)
            page = coord.context(max_bytes=512)
            self.assertTrue(page["messages"][0]["body_truncated"])
            with self.assertRaisesRegex(CoordError, "read your inbox"):
                coord.request("truncated-context", 5)
        finally:
            coord.close()

    def test_listing_and_ack_reply_do_not_create_recipient_read_evidence(self):
        message = self._send("reply is not a read receipt")
        coord = Coordinator(self.db, self.b)
        try:
            self.assertEqual([item['id'] for item in coord.inbox()['messages']], [message['id']])
            coord.send(self.a, 'substantive reply', reply_to=message['id'], ack_reply=True)
            self.assertIsNotNone(coord.db.execute('SELECT acked_at FROM messages WHERE id=?',
                                                  (message['id'],)).fetchone()['acked_at'])
            self.assertIsNone(coord.db.execute('SELECT 1 FROM message_reads WHERE message_id=? AND reader_session=?',
                                               (message['id'], self.b)).fetchone())
            with self.assertRaisesRegex(CoordError, "read your inbox"):
                coord.request('reply-does-not-consume', 5)
            coord.message(message['id'])
            self.assertEqual(coord.request('direct-read-does-consume', 5)['state'], 'owned')
        finally:
            coord.close()

    def test_legacy_read_marker_is_unknown_until_fresh_recipient_read(self):
        message = self._send('legacy provenance is not recoverable')
        legacy = Coordinator(self.db)
        try:
            legacy.db.execute('DROP TABLE message_reads')
            legacy.db.execute('''CREATE TABLE message_reads (
                message_id TEXT PRIMARY KEY, reader_session TEXT NOT NULL, read_at REAL NOT NULL)''')
            legacy.db.execute('INSERT INTO message_reads VALUES(?,?,?)', (message['id'], self.b, 1))
        finally:
            legacy.close()
        coord = Coordinator(self.db, self.b)
        try:
            row = coord.db.execute('SELECT reader_session,provenance FROM message_reads WHERE message_id=?',
                                   (message['id'],)).fetchone()
            self.assertEqual(tuple(row), (self.b, 'legacy_unknown'))
            with self.assertRaisesRegex(CoordError, 'read your inbox'):
                coord.request('legacy-is-not-proof', 5)
            coord.message(message['id'])
            self.assertEqual(coord.db.execute('SELECT provenance FROM message_reads WHERE message_id=? AND reader_session=?',
                                              (message['id'], self.b)).fetchone()['provenance'],
                             'recipient_full_message')
        finally:
            coord.close()

    def test_wrong_reader_and_legacy_marker_do_not_block_later_exact_read(self):
        message = self._send("recipient-only evidence")
        inspector = Coordinator(self.db)
        try:
            inspector.db.execute("INSERT INTO message_reads(message_id,reader_session,read_at,provenance) VALUES(?,?,?,?)",
                                 (message['id'], self.a, 1, 'recipient_full_message'))
            inspector.db.execute("INSERT INTO message_reads(message_id,reader_session,read_at,provenance) VALUES(?,?,?,?)",
                                 (message['id'], self.b, 2, 'legacy_unknown'))
        finally:
            inspector.close()
        coord = Coordinator(self.db, self.b)
        try:
            with self.assertRaisesRegex(CoordError, "read your inbox"):
                coord.request("wrong-reader-is-not-proof", 5)
            coord.message(message['id'])
            provenance = coord.db.execute("SELECT provenance FROM message_reads WHERE message_id=? AND reader_session=?",
                                          (message['id'], self.b)).fetchone()['provenance']
            self.assertEqual(provenance, 'recipient_full_message')
            self.assertEqual(coord.request("fresh-read-replaces-legacy-unknown", 5)["state"], "owned")
        finally:
            coord.close()


if __name__ == "__main__":
    unittest.main()
