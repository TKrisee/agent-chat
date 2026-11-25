from __future__ import annotations

import base64
import math
import contextlib
import hashlib
from pathlib import Path
import struct
import tempfile
import unittest

from agent_chat import cli
from agent_chat.core import Coordinator, CoordError, MAX_ATTACHMENT_SIZE
from agent_chat.remote_service import dispatch
from test_remote_web import RemoteWebFixture


def make_pcm_wav(data: bytes) -> bytes:
    rate = 16000
    fmt = struct.pack("<HHIIHH", 1, 1, rate, rate * 2, 2, 16)
    chunks = b"fmt " + struct.pack("<I", len(fmt)) + fmt
    chunks += b"data" + struct.pack("<I", len(data)) + data
    return b"RIFF" + struct.pack("<I", 4 + len(chunks)) + b"WAVE" + chunks


WAV = make_pcm_wav(b"".join(struct.pack("<h", int(12000 * math.sin(2 * math.pi * 440 * i / 16000))) for i in range(16000)))


class PcmWavValidationTests(unittest.TestCase):
    def test_valid_pcm_wav_keeps_name_mime_and_original_bytes(self):
        name, mime, content = Coordinator._prepare_attachment("tone.WAV", WAV)
        self.assertEqual((name, mime, content), ("tone.WAV", "audio/wav", WAV))
        self.assertEqual(hashlib.sha256(content).hexdigest(),
                         "7c1d53149d24cb54a92e2f22ccbefe62dc27ada4a41b33296ae641bd9f2eb8b7")

    def test_non_pcm_or_malformed_wav_is_rejected(self):
        invalid = []
        unsupported = bytearray(WAV)
        unsupported[20:22] = struct.pack("<H", 3)
        invalid.append(bytes(unsupported))
        truncated = bytearray(WAV)
        truncated[40:44] = struct.pack("<I", len(WAV))
        invalid.append(bytes(truncated))
        bad_alignment = bytearray(WAV)
        bad_alignment[32:34] = struct.pack("<H", 1)
        invalid.append(bytes(bad_alignment))
        invalid.append(b"not a wave file")
        invalid.append(WAV + b"trailing data")
        for content in invalid:
            with self.subTest(length=len(content)), self.assertRaises(CoordError):
                Coordinator._prepare_attachment("bad.wav", content)
        with self.assertRaisesRegex(CoordError, "nonempty"):
            Coordinator._prepare_attachment("empty.wav", b"")

    def test_pcm_wav_limits_and_renamed_binary_remain_enforced(self):
        at_limit = make_pcm_wav(bytes(MAX_ATTACHMENT_SIZE - 44))
        self.assertEqual(len(at_limit), MAX_ATTACHMENT_SIZE)
        self.assertEqual(Coordinator._prepare_attachment("limit.wav", at_limit)[2], at_limit)
        with self.assertRaisesRegex(CoordError, "10 MiB"):
            Coordinator._prepare_attachment("too-large.wav", make_pcm_wav(bytes(MAX_ATTACHMENT_SIZE - 42)))
        with self.assertRaisesRegex(CoordError, "UTF-8"):
            Coordinator._prepare_attachment("renamed.txt", WAV)


class PcmWavPrivateDownloadTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="agent-chat-wav-private-")
        self.addCleanup(temporary.cleanup)
        self.db = Path(temporary.name) / "state.sqlite3"
        for session in ("operator", "recipient", "other"):
            with contextlib.closing(Coordinator(self.db, session)) as coord:
                coord.register(session)
        with contextlib.closing(Coordinator(self.db)) as coord:
            coord.db.execute("INSERT INTO operator_identities(session_id) VALUES('operator')")
            coord.session = "operator"
            self.sent = coord._send_prepared(
                "recipient", "original PCM source", [coord._prepare_attachment("source.wav", WAV)])

    def call(self, *, session="recipient", project="default", **params):
        with contextlib.closing(Coordinator(self.db)) as coord:
            return dispatch(coord, {"op": "attachment-download", "session": session,
                                    "host_id": "recipient-host", "params": params}, canonical_project=project)

    def test_same_project_recipient_gets_exact_original_wav(self):
        attachment = self.sent["attachments"][0]
        result = self.call(source_project="default", message_id=self.sent["id"],
                           attachment_id=attachment["id"])
        content = base64.b64decode(result["content_base64"], validate=True)
        self.assertEqual(content, WAV)
        self.assertEqual(result["sha256"], hashlib.sha256(WAV).hexdigest())
        self.assertEqual((result["mime"], result["size"], result["recipient_session"]),
                         ("audio/wav", len(WAV), "recipient"))

    def test_wrong_recipient_host_project_or_message_still_rejects(self):
        attachment_id = self.sent["attachments"][0]["id"]
        bad_calls = (
            dict(session="other", project="default", source_project="default",
                 message_id=self.sent["id"], attachment_id=attachment_id),
            dict(session="recipient", project="elsewhere", source_project="default",
                 message_id=self.sent["id"], attachment_id=attachment_id),
            dict(session="recipient", project="default", source_project="default",
                 message_id="message_wrong", attachment_id=attachment_id),
        )
        for call in bad_calls:
            with self.subTest(call=call), self.assertRaises(CoordError):
                self.call(**call)

    def test_verified_local_download_publishes_same_wav_bytes(self):
        attachment = self.sent["attachments"][0]
        response = self.call(source_project="default", message_id=self.sent["id"],
                             attachment_id=attachment["id"])
        output_directory = tempfile.TemporaryDirectory(prefix="agent-chat-wav-output-")
        self.addCleanup(output_directory.cleanup)
        destination = Path(output_directory.name).resolve() / "source.wav"
        output = cli._publish_attachment_download(response, destination, "default", attachment["id"],
                                                  "recipient", "recipient-host",
                                                  project="default", message_id=self.sent["id"])
        self.assertEqual(destination.read_bytes(), WAV)
        self.assertEqual(output["sha256"], hashlib.sha256(WAV).hexdigest())


class PcmWavPrivateGalleryTests(RemoteWebFixture):
    def setUp(self):
        super().setUp()
        self.call("register", session="recipient", agent="recipient")
        with contextlib.closing(Coordinator(self.db)) as coord:
            operator = coord.db.execute("SELECT session_id FROM operator_identities").fetchone()[0]
            coord.session = operator
            self.sent = coord._send_prepared(
                "recipient", "listen to the original", [coord._prepare_attachment("source.wav", WAV)])
        self.attachment_id = self.sent["attachments"][0]["id"]

    @staticmethod
    def header(headers, name):
        return next(value for key, value in headers.items() if key.lower() == name.lower())

    def test_authenticated_gallery_serves_original_inline_wav_and_byte_ranges(self):
        path = f"/api/gallery-attachments/{self.attachment_id}?project=default"
        status, body, headers = self.request("GET", path, auth="basic")
        self.assertEqual(status, 200)
        self.assertEqual(body, WAV)
        self.assertEqual(self.header(headers, "Content-Type").split(";", 1)[0], "audio/wav")
        self.assertFalse(any(key.lower() == "content-disposition" for key in headers))
        self.assertEqual(self.header(headers, "Accept-Ranges"), "bytes")

        status, body, headers = self.request("GET", path, auth="basic", headers={"Range": "bytes=0-11"})
        self.assertEqual(status, 206)
        self.assertEqual(body, WAV[:12])
        self.assertEqual(self.header(headers, "Content-Range"), f"bytes 0-11/{len(WAV)}")

        self.assertEqual(self.request("GET", path, auth=None)[0], 401)


