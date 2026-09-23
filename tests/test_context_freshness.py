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

    def test_acknowledged_message_then_empty_context_allows_resource_request(self):
        message = self._send("please proceed")
        coord = Coordinator(self.db, self.b)
        try:
            coord.acknowledge(message["id"])
            self.assertEqual(coord.context()["messages"], [])
            self.assertEqual(coord.request("fresh-after-ack", 5)["state"], "owned")
        finally:
            coord.close()

    def test_historical_ack_without_read_proof_is_consumed_but_new_unread_still_blocks(self):
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
            self.assertEqual(coord.request("historical-ack-is-fresh", 5)["state"], "owned")
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


if __name__ == "__main__":
    unittest.main()
