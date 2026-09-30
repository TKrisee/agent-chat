#!/usr/bin/env python3
"""Attachment persistence and CLI coverage for agent-chat."""
from __future__ import annotations

import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
# Keep coordinator regression coverage independent of the public HTTP client.
CLI = [sys.executable, str(ROOT / 'src' / 'agent_chat' / 'core.py')]


class AttachmentTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='agent-chat-attachments-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.db = self.root / 'state.sqlite3'
        self.env = dict(os.environ, AGENT_CHAT_DB=str(self.db))
        self.cli('register', '--agent', 'alpha', session='alpha')
        self.cli('register', '--agent', 'beta', session='beta')
        self.body = self.root / 'caption.md'
        self.body.write_text('Screenshots attached.\n')

    def cli(self, *args, session='alpha', check=True):
        result = subprocess.run([*CLI, *args], cwd=ROOT,
                                env=dict(self.env, AGENT_CHAT_SESSION=session), text=True,
                                capture_output=True, timeout=20)
        if check:
            self.assertEqual(result.returncode, 0, result.stderr or result.stdout)
        return result, json.loads(result.stdout) if result.stdout else None

    def image(self, name, payload):
        path = self.root / name
        path.write_bytes(payload)
        return path

    def test_repeated_attach_persists_exact_bytes_and_inbox_metadata(self):
        png = self.image('one.png', b'\x89PNG\r\n\x1a\nfirst')
        jpeg = self.image('two.jpeg', b'\xff\xd8\xffsecond')
        _, sent = self.cli('send', '--to', 'beta', '--body-file', str(self.body),
                           '--attach', str(png), '--attach', str(jpeg))
        self.assertEqual([item['mime'] for item in sent['attachments']], ['image/png', 'image/jpeg'])
        self.assertEqual([item['name'] for item in sent['attachments']], ['one.png', 'two.jpeg'])
        png.unlink(); jpeg.unlink()
        _, inbox = self.cli('inbox', session='beta')
        self.assertEqual(inbox['messages'][0]['attachments'], sent['attachments'])
        with sqlite3.connect(self.db) as db:
            stored = db.execute('SELECT content FROM attachments ORDER BY rowid').fetchall()
        self.assertEqual([row[0] for row in stored], [b'\x89PNG\r\n\x1a\nfirst', b'\xff\xd8\xffsecond'])

    def test_gif_and_webp_signatures_match_their_extensions(self):
        gif = self.image('animation.gif', b'GIF89aimage-data')
        webp = self.image('image.webp', b'RIFF\x00\x00\x00\x00WEBPimage-data')
        _, sent = self.cli('send', '--to', 'beta', '--body-file', str(self.body),
                           '--attach', str(gif), '--attach', str(webp))
        self.assertEqual([item['mime'] for item in sent['attachments']], ['image/gif', 'image/webp'])

    def test_attachment_limits_nonregular_files_and_later_invalid_attachment_are_atomic(self):
        good = self.image('good.png', b'\x89PNG\r\n\x1a\ngood')
        bad = self.image('bad.dat', b'not image')
        directory = self.root / 'directory'
        directory.mkdir()
        before_messages = self.count_messages()
        before_attachments = self.count_attachments()
        failed, _ = self.cli('send', '--to', 'beta', '--body-file', str(self.body),
                             '--attach', str(good), '--attach', str(bad), check=False)
        self.assertNotEqual(failed.returncode, 0)
        self.assertEqual((self.count_messages(), self.count_attachments()), (before_messages, before_attachments))
        failed, _ = self.cli('send', '--to', 'beta', '--body-file', str(self.body),
                             '--attach', str(directory), check=False)
        self.assertNotEqual(failed.returncode, 0)
        failed, _ = self.cli('send', '--to', 'beta', '--body-file', str(self.body),
                             '--attach', str(good), '--attach', str(good), '--attach', str(good),
                             '--attach', str(good), '--attach', str(good), check=False)
        self.assertNotEqual(failed.returncode, 0)
        self.assertEqual((self.count_messages(), self.count_attachments()), (before_messages, before_attachments))

    def test_invalid_and_oversize_attachments_are_atomic_and_plain_messages_stay_compatible(self):
        bad = self.image('unsupported.dat', b'plain text')
        before = self.count_messages()
        failed, _ = self.cli('send', '--to', 'beta', '--body-file', str(self.body), '--attach', str(bad), check=False)
        self.assertNotEqual(failed.returncode, 0)
        self.assertEqual(self.count_messages(), before)
        oversized = self.image('large.png', b'\x89PNG\r\n\x1a\n' + b'x' * (10 * 1024 * 1024))
        failed, _ = self.cli('send', '--to', 'beta', '--body-file', str(self.body), '--attach', str(oversized), check=False)
        self.assertNotEqual(failed.returncode, 0)
        self.assertEqual(self.count_messages(), before)
        _, plain = self.cli('send', '--to', 'beta', '--body-file', str(self.body))
        self.assertEqual(plain['attachments'], [])
        _, inbox = self.cli('inbox', session='beta')
        self.assertEqual(inbox['messages'][0]['attachments'], [])

    def count_messages(self):
        with sqlite3.connect(self.db) as db:
            return db.execute('SELECT COUNT(*) FROM messages').fetchone()[0]

    def count_attachments(self):
        with sqlite3.connect(self.db) as db:
            return db.execute('SELECT COUNT(*) FROM attachments').fetchone()[0]


if __name__ == '__main__':
    unittest.main()
