import base64
import hashlib
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

from agent_chat.core import Coordinator, CoordError
from agent_chat.remote_service import _prepare_attachments, _send


class EmptyAttachmentTest(unittest.TestCase):
    def test_send_store_and_recipient_download(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            db = root / "chat.db"
            sender = Coordinator(db, "sender")
            recipient = Coordinator(db, "recipient")
            outsider = Coordinator(db, "outsider")
            try:
                for coord in (sender, recipient, outsider):
                    coord.register(coord.session)
                for payload in (b"", b"hello\n"):
                    path = root / "report.txt"
                    path.write_bytes(payload)
                    sent = _send(sender, "recipient", "report", [{"name": path.name,
                                 "content_base64": base64.b64encode(path.read_bytes()).decode("ascii")}], None)
                    attachment = sent["attachments"][0]
                    self.assertEqual(attachment["size"], len(payload))
                    item = recipient.private_inbox_attachment(attachment["id"], message_id=sent["id"])
                    self.assertEqual(item["content"], payload)
                    self.assertEqual(item["sha256"], hashlib.sha256(payload).hexdigest())
                    with self.assertRaises(CoordError):
                        outsider.private_inbox_attachment(attachment["id"], message_id=sent["id"])
                    with self.assertRaises(CoordError):
                        recipient.private_inbox_attachment(attachment["id"], message_id="wrong")
                with self.assertRaises(CoordError):
                    sender.send("recipient", "missing", attachments=[root / "missing.txt"])
                for invalid in ([{"name": "report.txt"}], [{"content_base64": ""}],
                                [{"name": "report.txt", "content_base64": "!"}],
                                [{"name": "image.png", "content_base64": ""}],
                                [{"name": "bad.exe", "content_base64": ""}]):
                    with self.subTest(invalid=invalid), self.assertRaises(CoordError):
                        _prepare_attachments(sender, invalid)
            finally:
                sender.close()
                recipient.close()
                outsider.close()


class AttachmentDiagnosticTest(unittest.TestCase):
    def test_mixed_batch_reports_index_and_stores_nothing(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / "chat.db"
            sender = Coordinator(db, "sender")
            recipient = Coordinator(db, "recipient")
            try:
                sender.register("sender")
                recipient.register("recipient")
                sensitive = b"private-content-marker-2058"
                encoded = base64.b64encode(sensitive).decode("ascii")
                batch = [{"name": "ok.txt", "content_base64": ""},
                         {"name": "private-secret.xyz", "content_base64": encoded}]
                with self.assertRaises(CoordError) as failure:
                    _send(sender, "recipient", "report", batch, None)
                diagnostic = str(failure.exception)
                self.assertIn("attachment #2: unsupported attachment type .xyz", diagnostic)
                self.assertIn("filename='private-secret.xyz'", diagnostic)
                self.assertIn("size=27 bytes", diagnostic)
                self.assertNotIn(sensitive.decode("ascii"), diagnostic)
                self.assertNotIn(encoded, diagnostic)
                # Local diagnostics expose only the requested basename, never
                # the caller's containing directory or attachment bytes.
                path = Path(directory) / "private-secret.xyz"
                path.write_bytes(sensitive)
                with self.assertRaises(CoordError) as local_failure:
                    sender.send("recipient", "report", [path])
                self.assertIn("filename='private-secret.xyz'", str(local_failure.exception))
                self.assertNotIn(directory, str(local_failure.exception))
                self.assertNotIn(sensitive.decode("ascii"), str(local_failure.exception))
                self.assertEqual(sender.db.execute("SELECT count(*) FROM messages").fetchone()[0], 0)
                self.assertEqual(sender.db.execute("SELECT count(*) FROM attachments").fetchone()[0], 0)
            finally:
                sender.close()
                recipient.close()


class JsonlAttachmentTest(unittest.TestCase):
    def test_jsonl_rpc_send_and_recipient_download(self):
        with tempfile.TemporaryDirectory() as directory:
            db = Path(directory) / "chat.db"
            sender = Coordinator(db, "sender")
            recipient = Coordinator(db, "recipient")
            try:
                sender.register("sender")
                recipient.register("recipient")
                payload = b'{"event":"ok"}\n[1,2]\n'
                sent = _send(sender, "recipient", "report", [{"name": "events.jsonl",
                    "content_base64": base64.b64encode(payload).decode("ascii")}], None)
                attachment = sent["attachments"][0]
                item = recipient.private_inbox_attachment(attachment["id"], message_id=sent["id"])
                self.assertEqual(item["content"], payload)
                self.assertEqual(item["sha256"], hashlib.sha256(payload).hexdigest())
            finally:
                sender.close()
                recipient.close()

    def test_jsonl_values_and_failures(self):
        for content in (b"", b"{}\n[1,2]\nnull\n", b"true"):
            with self.subTest(content=content):
                name, mime, actual = Coordinator._prepare_attachment("events.jsonl", content)
                self.assertEqual((name, mime, actual), ("events.jsonl", "application/x-ndjson", content))
        for content in (b"bad\n{}\n", b"{} {}\n", b"{}\n\n", b"{\n"):
            with self.subTest(content=content), self.assertRaises(CoordError):
                Coordinator._prepare_attachment("events.jsonl", content)
        with patch.dict("os.environ", {"PATH": ""}), self.assertRaises(CoordError):
            Coordinator._prepare_attachment("events.jsonl", b"{}\n")


if __name__ == "__main__":
    unittest.main()
