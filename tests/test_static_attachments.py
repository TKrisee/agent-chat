"""Static attachment-type coverage without HTTP or JSON parsing."""
from __future__ import annotations

import base64
import sqlite3
import tempfile
import unittest
from pathlib import Path

from agent_chat.core import CoordError, Coordinator
from agent_chat.remote_service import dispatch


FILES = {
    'image.png': (b'\x89PNG\r\n\x1a\npng', 'image/png'),
    'image.jpg': (b'\xff\xd8\xffjpeg', 'image/jpeg'),
    'image.jpeg': (b'\xff\xd8\xffjpeg', 'image/jpeg'),
    'image.gif': (b'GIF89agif', 'image/gif'),
    'image.webp': (b'RIFF\x00\x00\x00\x00WEBPwebp', 'image/webp'),
    'notes.txt': (b'plain text\n', 'text/plain'),
    'notes.md': (b'# Notes\n', 'text/markdown'),
    'notes.markdown': (b'# Notes\n', 'text/markdown'),
    'data.json': (b'{"safe": true}\n', 'application/json'),
    'data.xml': (b'<note>safe</note>\n', 'application/xml'),
    'data.csv': (b'name,value\na,1\n', 'text/csv'),
    'data.tsv': (b'name\tvalue\na\t1\n', 'text/tab-separated-values'),
    'run.log': (b'finished\n', 'text/plain'),
    'config.yaml': (b'key: value\n', 'application/yaml'),
    'config.yml': (b'key: value\n', 'application/yaml'),
    'config.toml': (b'key = "value"\n', 'application/toml'),
}


class StaticAttachmentTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='agent-chat-static-attachments-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.local_db = self.root / 'local.sqlite3'
        self.remote_db = self.root / 'remote.sqlite3'

    def local(self, session):
        coord = Coordinator(self.local_db, session)
        return coord

    def remote(self, op, session, **params):
        coord = Coordinator(self.remote_db)
        try:
            return dispatch(coord, {'op': op, 'session': session, 'host_id': 'test-host', 'params': params})
        finally:
            coord.close()

    def register_local(self):
        for session, agent in (('alpha', 'alpha'), ('beta', 'beta')):
            coord = self.local(session)
            try:
                coord.register(agent)
            finally:
                coord.close()

    def test_all_allowed_static_formats_preserve_exact_bytes_and_mime_locally_and_remotely(self):
        self.register_local()
        self.remote('register', 'alpha', agent='alpha')
        self.remote('register', 'beta', agent='beta')
        expected = [(name, mime, content) for name, (content, mime) in FILES.items()]
        local_results, remote_results = [], []
        for offset in range(0, len(expected), 4):
            group = expected[offset:offset + 4]
            paths = []
            for name, _, content in group:
                path = self.root / name
                path.write_bytes(content)
                paths.append(path)
            coord = self.local('alpha')
            try:
                local_results.extend(coord.send('beta', 'attachments', paths)['attachments'])
            finally:
                coord.close()
            remote_results.extend(self.remote(
                'send', 'alpha', to='beta', body='attachments',
                attachments=[{'name': name, 'content_base64': base64.b64encode(content).decode('ascii')}
                             for name, _, content in group],
            )['attachments'])
        expected_metadata = [(name, mime, len(content)) for name, mime, content in expected]
        self.assertEqual([(item['name'], item['mime'], item['size']) for item in local_results], expected_metadata)
        self.assertEqual([(item['name'], item['mime'], item['size']) for item in remote_results], expected_metadata)
        for db_path in (self.local_db, self.remote_db):
            with sqlite3.connect(db_path) as db:
                stored = db.execute('SELECT name,mime,size,content FROM attachments ORDER BY rowid').fetchall()
            self.assertEqual(stored, [(name, mime, len(content), content) for name, mime, content in expected])

    def test_local_and_remote_normalize_names_identically(self):
        self.register_local()
        self.remote('register', 'alpha', agent='alpha')
        self.remote('register', 'beta', agent='beta')
        name = '../unsafe\\\x00 report.txt'
        content = b'report\n'
        coord = self.local('alpha')
        try:
            local = coord._prepare_attachment(name, content)
        finally:
            coord.close()
        remote = self.remote('send', 'alpha', to='beta', body='attachment', attachments=[{
            'name': name, 'content_base64': base64.b64encode(content).decode('ascii'),
        }])
        self.assertEqual(local[0], '_ report.txt')
        self.assertEqual(remote['attachments'][0]['name'], local[0])

    def test_invalid_content_and_invalid_batch_rollback_match_local_and_remote(self):
        self.register_local()
        self.remote('register', 'alpha', agent='alpha')
        self.remote('register', 'beta', agent='beta')
        invalid = {
            'unknown.dat': b'\x89PNG\r\n\x1a\npng',
            'script.py': b'\x89PNG\r\n\x1a\npng',
            'program.txt': b'#!/bin/sh\necho nope\n',
            'binary.txt': b'hello\x00world',
            'invalid-utf8.txt': b'\xff',
            'disguised.txt': b'\x89PNG\r\n\x1a\npng',
            'wrong.png': b'plain text',
            'empty.txt': b'',
            'control.log': b'hello\x1b[31mworld',
        }
        for extension in ('exe', 'js', 'sh', 'ps1', 'html', 'svg', 'zip', 'pdf', 'docm', 'bin'):
            invalid['blocked.' + extension] = b'\x89PNG\r\n\x1a\npng'
        for name, content in invalid.items():
            path = self.root / name
            path.write_bytes(content)
            coord = self.local('alpha')
            try:
                with self.assertRaises(CoordError):
                    coord.send('beta', 'bad', [path])
            finally:
                coord.close()
            with self.assertRaises(CoordError):
                self.remote('send', 'alpha', to='beta', body='bad', attachments=[{
                    'name': name, 'content_base64': base64.b64encode(content).decode('ascii'),
                }])
        good = self.root / 'good.txt'
        good.write_bytes(b'good\n')
        bad = self.root / 'bad.dat'
        bad.write_bytes(b'bad\n')
        for db_path, sender in ((self.local_db, 'local'), (self.remote_db, 'remote')):
            with sqlite3.connect(db_path) as db:
                before = db.execute('SELECT COUNT(*) FROM messages').fetchone()[0]
            if sender == 'local':
                coord = self.local('alpha')
                try:
                    with self.assertRaises(CoordError):
                        coord.send('beta', 'atomic', [good, bad])
                finally:
                    coord.close()
            else:
                with self.assertRaises(CoordError):
                    self.remote('send', 'alpha', to='beta', body='atomic', attachments=[
                        {'name': 'good.txt', 'content_base64': base64.b64encode(b'good\n').decode('ascii')},
                        {'name': 'bad.dat', 'content_base64': base64.b64encode(b'bad\n').decode('ascii')},
                    ])
            with sqlite3.connect(db_path) as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM messages').fetchone()[0], before)


if __name__ == '__main__':
    unittest.main()
