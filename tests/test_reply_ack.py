import base64
import sqlite3
import tempfile
import unittest
from pathlib import Path

from agent_chat.core import CoordError, Coordinator
from agent_chat.remote_service import dispatch


class ReplyAcknowledgementTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="agent-chat-reply-ack-")
        self.db = Path(self.tmp.name) / "state.sqlite3"
        self.a = self.register("a")
        self.b = self.register("b")
        self.c = self.register("c")

    def tearDown(self):
        self.tmp.cleanup()

    def register(self, name):
        coord = Coordinator(self.db, name)
        try:
            return coord.register(name)["session"]
        finally:
            coord.close()

    def call(self, op, session, **params):
        coord = Coordinator(self.db)
        try:
            return dispatch(coord, {"op": op, "session": session, "host_id": "test-host", "params": params})
        finally:
            coord.close()

    def test_remote_reply_ack_is_atomic_and_retry_idempotent(self):
        parent = self.call("send", self.a, to=self.b, body="question")
        attachment = [{"name": "proof.png", "content_base64": base64.b64encode(b"\x89PNG\r\n\x1a\nproof").decode()}]
        reply = self.call("send", self.b, to=self.a, body="answer", attachments=attachment,
                          reply_to=parent["id"], ack_reply=True)
        retry = self.call("send", self.b, to=self.a, body="answer", attachments=attachment,
                          reply_to=parent["id"], ack_reply=True)
        self.assertEqual(reply["id"], retry["id"])
        self.assertTrue(retry["acknowledged_reply"])
        self.assertEqual(len(retry["attachments"]), 1)
        inbox = self.call("inbox", self.b)
        self.assertEqual(inbox["messages"], [])
        with self.assertRaisesRegex(CoordError, "differs"):
            self.call("send", self.b, to=self.a, body="changed", attachments=attachment,
                      reply_to=parent["id"], ack_reply=True)

    def test_invalid_atomic_reply_does_not_ack_original(self):
        parent = self.call("send", self.a, to=self.b, body="question")
        unrelated = self.call("send", self.a, to=self.c, body="other")
        with self.assertRaises(CoordError):
            self.call("send", self.b, to=self.a, body="bad", reply_to=unrelated["id"], ack_reply=True)
        inbox = self.call("inbox", self.b)
        self.assertEqual([message["id"] for message in inbox["messages"]], [parent["id"]])
        with self.assertRaisesRegex(CoordError, "requires reply_to"):
            self.call("send", self.b, to=self.a, body="bad", ack_reply=True)

    def test_ack_reply_requires_an_incoming_parent_and_attachment_failure_rolls_back(self):
        parent = self.call("send", self.a, to=self.b, body="question")
        with self.assertRaisesRegex(CoordError, "caller inbox"):
            self.call("send", self.a, to=self.b, body="invalid", reply_to=parent["id"], ack_reply=True)
        with sqlite3.connect(self.db) as db:
            db.execute("CREATE TRIGGER reject_attachment BEFORE INSERT ON attachments "
                       "BEGIN SELECT RAISE(ABORT, 'attachment failure'); END")
        attachment = [{"name": "proof.png", "content_base64": base64.b64encode(b"\x89PNG\r\n\x1a\nproof").decode()}]
        with self.assertRaisesRegex(sqlite3.Error, "attachment failure"):
            self.call("send", self.b, to=self.a, body="answer", attachments=attachment,
                      reply_to=parent["id"], ack_reply=True)
        with sqlite3.connect(self.db) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM messages").fetchone()[0], 1)
            self.assertIsNone(db.execute("SELECT acked_at FROM messages WHERE id=?", (parent["id"],)).fetchone()[0])


if __name__ == "__main__":
    unittest.main()
