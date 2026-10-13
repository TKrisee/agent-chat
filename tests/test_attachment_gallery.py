"""Sent-file gallery preserves room boundaries and works beyond loaded messages."""
import contextlib
import hashlib
import sqlite3
import subprocess
from urllib.parse import quote

from agent_chat.core import Coordinator
from test_remote_web import RemoteWebFixture


class AttachmentGalleryTests(RemoteWebFixture):
    def setUp(self):
        super().setUp()
        status, raw, _ = self.request('POST', '/api/projects/rpc', dict(op='create', name='Second project'))
        self.assertEqual(status, 200, raw)
        self.second = self.jq(raw, '.project.id', True)
        self.paths = [self.db, self.server.projects.db_path(self.second)]
        self.files = []
        for number, path in enumerate(self.paths):
            with contextlib.closing(Coordinator(path, 'sender')) as coord:
                coord.register('Sender'); coord.session = 'receiver'; coord.register('Receiver'); coord.session = 'sender'
                for index in range(4):
                    result = coord._send_prepared('receiver', 'message body must not appear',
                        [(f'file-{number}-{index}.txt', 'text/plain', b'fixture file')])
                    self.files.extend(result['attachments'])
        self.operator = self.server.sender_session
        with contextlib.closing(Coordinator(self.db, 'root')) as coord:
            coord.register('Root')
            coord.db.execute('CREATE TABLE IF NOT EXISTS quiet_root_sessions (session_id TEXT PRIMARY KEY)')
            coord.db.execute('INSERT INTO quiet_root_sessions VALUES(?)', ('root',))
            coord.session = self.operator
            private = coord._send_prepared('root', 'private message body', [('private-root.txt', 'text/plain', b'private')])
            self.private = private['attachments'][0]['id']
            shared = coord._send_many_prepared(['root', 'receiver'], 'shared message', [('shared.txt', 'text/plain', b'shared')])
            self.shared = shared[0]['attachments'][0]['id']
        with self.server.projects.connection() as db:
            db.execute('INSERT INTO root_config(singleton,home_project,home_session,thread_id,host_id,cwd,agent,created_at) '
                       "VALUES(1,'default','root','fixture-thread','fixture-host','/fixture','root',1)")

    def jq(self, raw, expression, output=False):
        result = subprocess.run(['jq', '-r' if output else '-e', expression], input=raw, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        return result.stdout.decode().strip()

    def snapshot(self):
        return {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in self.paths}

    def test_gallery_pages_all_sent_files_without_bodies_or_read_mutation(self):
        before = self.snapshot()
        contexts = set(self.server.project_contexts)
        seen = []
        cursor = None
        while True:
            path = '/api/attachment-gallery?limit=3' + ('&before=' + quote(cursor) if cursor else '')
            status, raw, _ = self.request('GET', path)
            self.assertEqual(status, 200, raw)
            self.jq(raw, 'all(.attachments[]; (has("body")|not) and (has("content")|not) and (.url|startswith("/api/gallery-attachments/")))')
            seen.extend(self.jq(raw, '.attachments[].id', True).splitlines())
            cursor = self.jq(raw, '.next_cursor', True)
            if cursor == 'null': break
        self.assertEqual(len(seen), len(set(seen)))
        self.assertEqual(len(seen), 9)  # Eight ordinary plus one logical shared upload.
        self.assertNotIn(self.private, seen)
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(set(self.server.project_contexts), contexts)

    def test_project_filename_filter_and_gallery_download_uses_source_project(self):
        status, raw, _ = self.request('GET', '/api/attachment-gallery?project=' + self.second + '&q=FILE-1-2')
        self.assertEqual(status, 200, raw)
        self.jq(raw, '(.attachments|length)==1 and .attachments[0].name=="file-1-2.txt"')
        url = self.jq(raw, '.attachments[0].url', True)
        self.assertEqual(self.request('GET', url)[1], b'fixture file')
        self.assertEqual(self.request('GET', url.replace(self.second, 'default'))[0], 404)
        status, raw, _ = self.request('GET', '/api/attachment-gallery?q=absent')
        self.assertEqual(status, 200, raw); self.jq(raw, '.attachments==[] and (.has_more|not)')

    def test_root_gallery_and_scoped_downloads_enforce_parent_visibility(self):
        status, raw, _ = self.request('GET', '/api/root/attachment-gallery')
        self.assertEqual(status, 200, raw)
        self.jq(raw, '(.attachments|length)==1 and .attachments[0].name=="private-root.txt"')
        self.assertEqual(self.request('GET', self.jq(raw, '.attachments[0].url', True))[1], b'private')
        self.assertEqual(self.request('GET', '/api/gallery-attachments/' + self.private)[0], 404)
        self.assertEqual(self.request('GET', '/api/root/gallery-attachments/' + self.shared)[0], 404)
        self.assertEqual(self.request('GET', '/api/root/gallery-attachments/' + self.files[0]['id'])[0], 404)
        self.assertEqual(self.request('GET', '/api/gallery-attachments/' + self.shared)[1], b'shared')

    def test_unlinked_file_not_listed_or_gallery_downloaded_and_legacy_preview_retained(self):
        with contextlib.closing(sqlite3.connect(self.db)) as db:
            db.execute("INSERT INTO attachments VALUES('attachment_unlinked','message_absent','draft.txt','text/plain',5,?)", (b'draft',))
            db.commit()
        status, raw, _ = self.request('GET', '/api/attachment-gallery?q=draft')
        self.assertEqual(status, 200, raw); self.jq(raw, '.attachments==[]')
        self.assertEqual(self.request('GET', '/api/gallery-attachments/attachment_unlinked')[0], 404)
        self.assertEqual(self.request('GET', '/api/attachments/attachment_unlinked')[1], b'draft')

    def test_auth_query_validation_and_partial_project_failure(self):
        self.assertEqual(self.request('GET', '/api/attachment-gallery', auth=None)[0], 401)
        for query in ('limit=0', 'limit=101', 'limit=no', 'limit=1&limit=2', 'project=unknown', 'before=bad', 'session=root'):
            with self.subTest(query=query):
                self.assertEqual(self.request('GET', '/api/attachment-gallery?' + query)[0], 400)
        self.paths[1].unlink()
        status, raw, _ = self.request('GET', '/api/attachment-gallery')
        self.assertEqual(status, 200, raw)
        self.jq(raw, '.partial and (.errors|length)==1 and (.attachments|length)>0')
        self.assertFalse(self.paths[1].exists())

    def test_group_uploads_collapse_deliveries_but_preserve_distinct_files_and_sends(self):
        with contextlib.closing(Coordinator(self.db, 'sender')) as coord:
            coord._send_many_prepared(['root', 'receiver'], 'group one', [('group-one.txt','text/plain',b'one')])
            coord._send_many_prepared(['root', 'receiver'], 'group pair',
                [('group-pair.txt','text/plain',b'first'), ('group-pair.txt','text/plain',b'second')])
            coord._send_prepared('receiver', 'separate first', [('separate.txt','text/plain',b'same')])
            coord._send_prepared('receiver', 'separate second', [('separate.txt','text/plain',b'same')])
        for filename, count in (('group-one',1), ('group-pair',2), ('separate',2)):
            status, raw, _ = self.request('GET', '/api/attachment-gallery?q=' + filename)
            self.assertEqual(status, 200, raw)
            self.jq(raw, '(.attachments|length)==' + str(count))
        status, raw, _ = self.request('GET', '/api/attachment-gallery?q=group-pair')
        urls = self.jq(raw, '.attachments[].url', True).splitlines()
        self.assertEqual({self.request('GET', url)[1] for url in urls}, {b'first', b'second'})
