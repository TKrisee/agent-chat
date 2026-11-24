"""Bounded Python source is transported as original UTF-8 bytes, never imported."""
import base64
import hashlib
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import unittest
from unittest import mock

from agent_chat import core, root_local, service
from agent_chat.remote import HttpClient, RemoteCoordError
from test_large_attachment_transfer import LargeAttachmentRequirementTests


class PythonSourceTransport(LargeAttachmentRequirementTests):
    def setUp(self):
        private = tempfile.TemporaryDirectory(prefix='agent-chat-python-source-home-')
        self.addCleanup(private.cleanup)
        home = Path(private.name).resolve()
        codex = home / 'codex'; codex.mkdir()
        env = {key: value for key, value in os.environ.items()
               if not key.startswith('AGENT_CHAT_') and key not in ('HOME', 'CODEX_HOME')}
        env.update(HOME=str(home), CODEX_HOME=str(codex))
        patch = mock.patch.dict(os.environ, env, clear=True)
        patch.start(); self.addCleanup(patch.stop)
        for module in (service, root_local):
            for name in ('_installed_server_config', 'configured_server_database'):
                guard = mock.patch.object(module, name, side_effect=AssertionError(
                    'installed/default service discovery is outside this private fixture'))
                guard.start(); self.addCleanup(guard.stop)
        super().setUp()

    def counts(self):
        with sqlite3.connect(self.db) as db:
            return tuple(db.execute('SELECT COUNT(*) FROM ' + table).fetchone()[0]
                         for table in ('messages', 'attachments'))

    @staticmethod
    def packet(name, content):
        return {'name': name, 'content_base64': base64.b64encode(content).decode('ascii')}

    def test_python_source_cli_http_exact_recipient_roundtrip(self):
        sentinel = self.root / 'must-not-be-executed'
        content = ('\ufeff# Original source: café; preserve BOM and CRLF.\r\n'
                   'from pathlib import Path\r\n'
                   f'Path({str(sentinel)!r}).write_text("unexpected execution")\r\n'
                   'raise RuntimeError("transport must not execute source")\r\n').encode('utf-8')
        source = self.root / 'original.py'; source.write_bytes(content)
        sent = self.command('large-sender', 'send', '--to', 'large-recipient',
                            '--body-file', str(self.body), '--attach', str(source))
        fields = subprocess.run(['jq', '-er',
            '[.id,.attachments[0].id,.attachments[0].name,.attachments[0].mime,.attachments[0].size] | @tsv'],
            input=sent, text=True, capture_output=True, check=True).stdout.strip().split('\t')
        message_id, attachment_id, name, mime, size = fields
        self.assertEqual((name, mime, int(size)), ('original.py', 'text/plain', len(content)))
        output = self.root / 'received.py'
        receipt = self.command('large-recipient', 'attachment-download', '--source-project', 'default',
            '--message-id', message_id, '--attachment-id', attachment_id, '--output', str(output))
        digest = subprocess.run(['jq', '-er', '.sha256'], input=receipt,
            text=True, capture_output=True, check=True).stdout.strip()
        self.assertEqual(output.read_bytes(), content)
        self.assertEqual(digest, hashlib.sha256(content).hexdigest())
        self.assertFalse(sentinel.exists(), 'transport executed uploaded source')
        self.assertEqual(self.counts(), (1, 1))
        params = dict(source_project='default', message_id=message_id, attachment_id=attachment_id)
        for actor, host in [('large-unrelated', self.HOST), ('large-recipient', 'wrong-host')]:
            with self.subTest(actor=actor, host=host), self.assertRaises(RemoteCoordError):
                self.call('attachment-download', session=actor, host=host, **params)
        with self.assertRaises(RemoteCoordError):
            self.call('attachment-download', session='large-recipient',
                      **(params | {'source_project': 'wrong-project'}))
        # The shared server key is admin transport authority, not a separate
        # cryptographic agent principal. A wrong key must still fail HTTP auth.
        with self.assertRaises(RemoteCoordError):
            HttpClient(self.url, 'wrong-fixture-token', project='default').call('/api/coord',
                {'op': 'attachment-download', 'session': 'large-recipient',
                 'host_id': self.HOST, 'params': params})

    def test_python_source_text_guards_and_packet_atomicity(self):
        for content, reason in [(b'\xff', 'UTF-8'), (b'x\x00y', 'control bytes'),
                                (b'x\xc2\x80y', 'control bytes'),
                                (b'#!/usr/bin/env python3\npass\n', 'executable scripts')]:
            with self.subTest(reason=reason), self.assertRaisesRegex(RemoteCoordError, reason):
                self.call('send', to='large-recipient', body='invalid source fixture',
                          attachments=[self.packet('invalid.py', content)])
            self.assertEqual(self.counts(), (0, 0))
        with self.assertRaisesRegex(RemoteCoordError, r'attachment #2: unsupported attachment type \.pyc'):
            self.call('send', to='large-recipient', body='atomic source packet fixture',
                attachments=[self.packet('valid.py', b'value = 1\n'), self.packet('invalid.pyc', b'compiled')])
        self.assertEqual(self.counts(), (0, 0))

    def test_python_source_keeps_default_size_boundary(self):
        content = b'#' + b' ' * (core.MAX_ATTACHMENT_SIZE - 2) + b'\n'
        name, mime, prepared = core.Coordinator._prepare_attachment('boundary.py', content)
        self.assertEqual((name, mime, prepared), ('boundary.py', 'text/plain', content))
        with self.assertRaisesRegex(RemoteCoordError, 'attachment exceeds 10 MiB'):
            self.call('send', to='large-recipient', body='oversized source fixture',
                attachments=[self.packet('oversized.py', content + b' ')])
        self.assertEqual(self.counts(), (0, 0))


def load_tests(loader, tests, pattern):
    # Do not rerun the inherited large-transfer campaign when selecting this file.
    return unittest.TestSuite(PythonSourceTransport(name) for name in (
        'test_python_source_cli_http_exact_recipient_roundtrip',
        'test_python_source_text_guards_and_packet_atomicity',
        'test_python_source_keeps_default_size_boundary'))
