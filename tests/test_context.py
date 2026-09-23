import tempfile
import unittest
from pathlib import Path

from agent_chat.core import CoordError, Coordinator


class ContextTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="agent-chat-context-")
        self.db = Path(self.tmp.name) / "state.sqlite3"
        self.a = self.register("a")
        self.b = self.register("b")

    def tearDown(self):
        self.tmp.cleanup()

    def register(self, name):
        coord = Coordinator(self.db, name)
        try:
            return coord.register(name)["session"]
        finally:
            coord.close()

    def send(self, body):
        coord = Coordinator(self.db, self.a)
        try:
            return coord.send(self.b, body)
        finally:
            coord.close()

    def test_unicode_budget_pagination_and_full_message_are_session_scoped(self):
        first = self.send("first \U0001f642")
        second = self.send("\U0001f642" * 5000)
        outsider = Coordinator(self.db, self.a)
        try:
            with self.assertRaises(CoordError):
                outsider.message(first["id"])
        finally:
            outsider.close()
        coord = Coordinator(self.db, self.b)
        try:
            page = coord.context(limit=1, max_bytes=800)
            self.assertEqual([message["id"] for message in page["messages"]], [first["id"]])
            self.assertTrue(page["has_more"])
            self.assertLessEqual(len(coord._canonical_bytes(page)), 800)
            page_two = coord.context(limit=1, max_bytes=800, cursor=page["cursor"])
            self.assertEqual([message["id"] for message in page_two["messages"]], [second["id"]])
            self.assertTrue(page_two["messages"][0]["body_truncated"])
            self.assertTrue(page_two["truncated"])
            self.assertLessEqual(len(coord._canonical_bytes(page_two)), 800)
            full = coord.message(second["id"])
            self.assertEqual(full["message"]["body"], "\U0001f642" * 5000)
        finally:
            coord.close()

    def test_context_read_proof_and_owned_or_named_resources(self):
        message = self.send("read before requesting")
        owner = Coordinator(self.db, self.b)
        try:
            owner.context(message_ids=[message["id"]])
            claim = owner.request("mine", 5)
            self.assertEqual(claim["state"], "owned")
        finally:
            owner.close()
        other = Coordinator(self.db, self.a)
        try:
            other.request("named", 5)
        finally:
            other.close()
        coord = Coordinator(self.db, self.b)
        try:
            context = coord.context(resources=["named"])
            self.assertEqual([item["resource"] for item in context["resources"]], ["mine", "named"])
            self.assertEqual(coord.status(mine=True), {"resources": [context["resources"][0]]})
            self.assertEqual([item["resource"] for item in coord.status(resources=["named"])["resources"]], ["named"])
            self.assertEqual(coord.status(resources=[]), {"resources": []})
            self.assertEqual(coord.status(mine=True, resources=["named"]), {"resources": []})
        finally:
            coord.close()

    def test_resource_and_metadata_stubs_advance_their_independent_cursors(self):
        owner = Coordinator(self.db, self.b)
        try:
            for name in ("a-resource", "b-resource", "c-resource"):
                owner.request(name, 5)
        finally:
            owner.close()
        coord = Coordinator(self.db, self.a)
        try:
            attachment = [("x" * 180, "image/png", b"\x89PNG\r\n\x1a\n") for _ in range(4)]
            incoming = coord._send_prepared(self.b, "small", attachment)
        finally:
            coord.close()
        owner = Coordinator(self.db, self.b)
        try:
            page = owner.context(max_bytes=768)
            self.assertTrue(page["resources"])
            self.assertTrue(page["resources_has_more"])
            self.assertEqual(page["resources_cursor"], page["resources"][-1]["resource"])
            self.assertLessEqual(owner.context_size(page), 768)
            next_page = owner.context(max_bytes=768, resource_cursor=page["resources_cursor"])
            self.assertNotEqual(next_page["resources_cursor"], page["resources_cursor"])
            message_page = owner.context(max_bytes=512)
            stub = next(item for item in message_page["messages"] if item["id"] == incoming["id"])
            self.assertTrue(stub["metadata_truncated"])
            self.assertIsInstance(message_page["cursor"], int)
            self.assertEqual(owner.message(incoming["id"])["message"]["body"], "small")
        finally:
            owner.close()


if __name__ == "__main__":
    unittest.main()
