"""Static attachment-type coverage without HTTP or JSON parsing."""
from __future__ import annotations
from unittest import mock

import base64
import sqlite3
import struct
import tempfile
import unittest
from pathlib import Path

from agent_chat.core import CoordError, Coordinator, MAX_ATTACHMENTS, MAX_ATTACHMENT_SIZE, MAX_MODEL_ATTACHMENT_SIZE
from agent_chat.remote import MAX_COORD_REQUEST_BYTES
from agent_chat.web import MAX_MULTIPART_BYTES
from agent_chat.remote_service import dispatch


FILES = {
    'report.PDF': ((Path(__file__).parent / 'fixtures' / 'attachment.pdf').read_bytes(), 'application/pdf'),
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

GLB = b'glTF' + struct.pack('<II', 2, 48) + struct.pack('<II', 4, 0x4E4F534A) + b'xxxx' + struct.pack('<II', 4, 0x004E4942) + b'BIN!' + struct.pack('<II', 4, 0x12345678) + b'EXT!'
FBX = b'Kaydara FBX Binary  \x00\x1a\x00' + struct.pack('<I', 7400) + b'node'
FILES.update({'model.glb': (GLB, 'model/gltf-binary'), 'model.fbx': (FBX, 'application/octet-stream'),
              'empty.txt': (b'', 'text/plain')})


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

    def test_only_supported_model_files_receive_the_20_mib_per_file_limit(self):
        self.register_local()
        self.remote('register', 'alpha', agent='alpha')
        self.remote('register', 'beta', agent='beta')
        size = MAX_ATTACHMENT_SIZE + 4
        chunk_size = size - 20
        large_glb = b'glTF' + struct.pack('<II', 2, size) + struct.pack('<II', chunk_size, 0x4E4F534A) + b' ' * chunk_size
        large_fbx = FBX[:27] + b'F' * (size - 27)
        expected = [('large.glb', 'model/gltf-binary', large_glb),
                    ('large.fbx', 'application/octet-stream', large_fbx)]

        paths = []
        for name, _, content in expected:
            path = self.root / name
            path.write_bytes(content)
            paths.append(path)
        coord = self.local('alpha')
        try:
            local = coord.send('beta', 'large models', paths)['attachments']
        finally:
            coord.close()
        remote = self.remote('send', 'alpha', to='beta', body='large models', attachments=[
            {'name': name, 'content_base64': base64.b64encode(content).decode('ascii')}
            for name, _, content in expected])['attachments']
        self.assertEqual([(item['mime'], item['size']) for item in local],
                         [(mime, len(content)) for _, mime, content in expected])
        self.assertEqual([(item['mime'], item['size']) for item in remote],
                         [(mime, len(content)) for _, mime, content in expected])
        for db_path in (self.local_db, self.remote_db):
            with sqlite3.connect(db_path) as db:
                stored = db.execute('SELECT name,mime,size,content FROM attachments ORDER BY rowid').fetchall()
            self.assertEqual(stored, [(name, mime, len(content), content) for name, mime, content in expected])

        with self.assertRaisesRegex(CoordError, '64 MiB'):
            Coordinator._prepare_attachment('too-large.glb', b'glTF' + bytes(MAX_MODEL_ATTACHMENT_SIZE - 3))
        with self.assertRaisesRegex(CoordError, '10 MiB'):
            Coordinator._prepare_attachment('ordinary.txt', b'x' * (MAX_ATTACHMENT_SIZE + 1))

    def test_existing_request_and_file_count_ceilings_are_unchanged(self):
        self.assertEqual(MAX_ATTACHMENTS, 50)
        self.assertEqual(MAX_ATTACHMENT_SIZE, 10 * 1024 * 1024)
        self.assertEqual(MAX_MULTIPART_BYTES, MAX_ATTACHMENTS * MAX_ATTACHMENT_SIZE + 64 * 1024)
        self.assertEqual(MAX_COORD_REQUEST_BYTES,
                         MAX_ATTACHMENTS * (4 * ((MAX_ATTACHMENT_SIZE + 2) // 3)) + 1024 * 1024)

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
            'fake.pdf': b'plain text',
            'empty.pdf': b'',
            'huge.pdf': b'%PDF-1.4\n' + b'x' * (10 * 1024 * 1024),
            'control.log': b'hello\x1b[31mworld',
        }
        invalid.update({
            'bad-magic.glb': b'nope' + GLB[4:],
            'wrong-version.glb': b'glTF' + struct.pack('<II', 1, len(GLB)) + GLB[12:],
            'wrong-size.glb': GLB[:4] + struct.pack('<II', 2, len(GLB) - 4) + GLB[12:],
            'truncated-chunk.glb': b'glTF' + struct.pack('<II', 2, 20) + struct.pack('<II', 4, 0x4E4F534A),
            'wrong-first-chunk.glb': GLB[:12] + struct.pack('<II', 4, 0x004E4942) + b'BIN!' + GLB[24:],
            'misplaced-bin.glb': GLB[:24] + struct.pack('<II', 4, 0x12345678) + b'EXT!' + struct.pack('<II', 4, 0x004E4942) + b'BIN!',
            'misaligned-chunk.glb': b'glTF' + struct.pack('<II', 2, 25) + struct.pack('<II', 5, 0x4E4F534A) + b'xxxxx',
            'wrong-version.fbx': FBX[:23] + struct.pack('<I', 7400 + 1) + FBX[27:],
            'ascii.fbx': b'; FBX 7.4.0 project file\n',
            'other-model.blend': b'BLENDER',
        })
        for extension in ('exe', 'js', 'sh', 'ps1', 'html', 'svg', 'zip', 'pdf', 'docm', 'bin'):
            invalid['blocked.' + extension] = b'\x89PNG\r\n\x1a\npng'
        for name, content in invalid.items():
            path = self.root / name
            path.write_bytes(content)
            coord = self.local('alpha')
            try:
                with self.assertRaises(CoordError, msg=name):
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


class ModelAggregateBoundaryTests(unittest.TestCase):
    def test_explicit_raw_ceiling_rejects_before_reading_large_local_batch(self):
        from agent_chat.core import MAX_ATTACHMENT_TOTAL_SIZE
        self.assertEqual(MAX_ATTACHMENT_TOTAL_SIZE, 500 * 1024 * 1024)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'synthetic.glb'
            with path.open('wb') as stream: stream.truncate(64 * 1024 * 1024)
            coord = Coordinator(Path(directory) / 'state.sqlite3', 'fixture')
            try:
                with mock.patch('agent_chat.core.os.open', side_effect=AssertionError('must reject before opening contents')):
                    with self.assertRaisesRegex(CoordError, '500 MiB total'):
                        coord._prepare_attachments([path] * 8)
            finally: coord.close()

    def test_exact_model_cap_and_ordinary_cap_remain_format_specific(self):
        maximum = 64 * 1024 * 1024
        for extension in ('glb', 'fbx'):
            if extension == 'glb':
                data = b'glTF' + struct.pack('<II', 2, maximum) + struct.pack('<II', maximum-20, 0x4E4F534A) + b'{}' + b' '*(maximum-22)
            else:
                data = b'Kaydara FBX Binary  \x00\x1a\x00' + struct.pack('<I', 7400) + bytes(maximum-27)
            name, mime, actual = Coordinator._prepare_attachment('boundary.'+extension, data)
            self.assertEqual(len(actual), maximum)
            self.assertEqual(mime, 'model/gltf-binary' if extension=='glb' else 'application/octet-stream')
            with self.assertRaisesRegex(CoordError, '64 MiB'):
                Coordinator._prepare_attachment('boundary.'+extension, data+b'x')
        with self.assertRaisesRegex(CoordError, '10 MiB'):
            Coordinator._prepare_attachment('ordinary.txt', b'x'*(10*1024*1024+1))
