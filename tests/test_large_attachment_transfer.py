"""Owned HTTP/CLI resumable large-artifact workflow; no production paths."""
import base64
import contextlib
import hashlib
import http.client
import io
import os
import platform
from pathlib import Path
import resource
import socket
import subprocess
import tempfile
import threading
import unittest
import uuid
from unittest import mock

from agent_chat import cli, core, root_local, service
from agent_chat.remote import HttpClient, RemoteCoordError
from agent_chat.web import Handler, create_server


class LargeAttachmentRequirementTests(unittest.TestCase):
    SIZE = 320_392_827
    CHUNK = 1024 * 1024
    TOKEN = 'isolated-large-transfer-fixture-token'
    HOST = 'isolated-large-transfer-host'

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='agent-chat-large-transfer-')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.db = self.root / 'state.sqlite3'
        self.sessions = self.root / 'native-sessions'
        self.sessions.mkdir()
        self.state = self.root / 'client-state'
        self.state.mkdir()
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)
        # These are test-boundary assertions, not a claimed OS sandbox. All
        # application stores are explicit; installed/default paths must not run.
        for module in (service, root_local):
            self.stack.enter_context(mock.patch.object(module, '_installed_server_config',
                side_effect=AssertionError('installed provider resolution is outside this fixture')))
        self.stack.enter_context(mock.patch.object(core, 'default_db',
            side_effect=AssertionError('default database resolution is outside this fixture')))
        self.stack.enter_context(mock.patch.dict(os.environ, {
            'AGENT_CHAT_DB': str(self.db), 'AGENT_CHAT_STATE_DIR': str(self.state),
            'AGENT_CHAT_HOST_ID': self.HOST, 'AGENT_CHAT_PROJECT': 'default',
            'AGENT_CHAT_SESSION': 'large-sender', 'AGENT_CHAT_API_TOKEN': self.TOKEN,
        }))
        core.Coordinator(self.db).close()
        self.server = None
        self.addCleanup(self.stop)
        self.start()
        for session in ('large-sender', 'large-recipient', 'large-unrelated'):
            self.call('register', session=session, agent=session)
        self.body = self.root / 'body.txt'
        self.body.write_text('Generated inert large artifact requirement fixture.\n')

    def start(self):
        self.server = create_server(self.db, host='127.0.0.1', port=0,
            api_token=self.TOKEN, sessions_root=self.sessions)
        self.assertEqual(self.server.projects.path, Path(str(self.db) + '.projects.sqlite3'))
        self.assertEqual(self.server.measurements.sessions_root, self.sessions)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = 'http://127.0.0.1:' + str(self.server.server_port)
        self.client = HttpClient(self.url, self.TOKEN, project='default')

    def stop(self):
        if self.server is not None:
            server, self.server = self.server, None
            server.stop_event.set()
            server.shutdown()
            server.server_close()
            self.thread.join(3)
            self.assertFalse(self.thread.is_alive(), 'owned fixture server did not stop')

    def call(self, operation, session='large-sender', host=None, **params):
        return self.client.call('/api/coord', {'op': operation, 'session': session,
            'host_id': host or self.HOST, 'params': params})

    def command(self, session, *arguments):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.assertEqual(cli.main(['--server', self.url, '--api-token', self.TOKEN,
                '--project', 'default', '--session', session, *arguments]), 0)
        return output.getvalue()

    @contextlib.contextmanager
    def lose_response(self, predicate):
        """Actually close the owned HTTP socket after commit, before its reply."""
        original = Handler.json
        dropped = []
        def response(handler, code, value):
            if not dropped and code == 200 and predicate(value):
                dropped.append(True)
                handler.close_connection = True
                handler.connection.shutdown(socket.SHUT_RDWR)
                handler.connection.close()
                handler._finish_request(error=ConnectionResetError('owned fixture dropped response'))
                return
            return original(handler, code, value)
        with mock.patch.object(Handler, 'json', response):
            yield
        self.assertEqual(dropped, [True], 'the intended response was not interrupted')

    def test_sender_can_deliver_generated_large_text_to_exact_recipient(self):
        initial_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        source = self.root / 'inert-large.txt'
        block = (b'Generated inert fixture bytes.\n' * 4096)[:65536]
        digest = hashlib.sha256()
        with source.open('xb') as output:
            remaining = self.SIZE
            while remaining:
                part = block[:min(remaining, len(block))]
                output.write(part)
                digest.update(part)
                remaining -= len(part)
        self.assertGreater(source.stat().st_size, 320_392_826)
        print('fixture_bytes=' + str(source.stat().st_size) + ' sha256=' + digest.hexdigest())
        transfer_id = str(uuid.uuid4())
        send = ['send', '--to', 'large-recipient', '--body-file', str(self.body),
                '--large-attach', str(source), '--transfer-id', transfer_id]
        attempts = []
        original_call = HttpClient.call
        def counted(client, path, payload, **kwargs):
            if payload.get('op') in ('attachment-upload-chunk', 'attachment-download-chunk'):
                attempts.append((payload['op'], payload['params']['offset']))
            return original_call(client, path, payload, **kwargs)
        with mock.patch.object(HttpClient, 'call', counted):
            with self.lose_response(lambda value: value.get('transfer_id') == transfer_id and value.get('received') == 3 * self.CHUNK):
                with self.assertRaises(RemoteCoordError):
                    self.command('large-sender', *send)
            progress = self.call('attachment-upload-status', transfer_id=transfer_id)
            self.assertEqual(progress['received'], 3 * self.CHUNK)
            with contextlib.closing(core.Coordinator(self.db)) as coord:
                self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM attachments').fetchone()[0], 0)
            # Reconcile the exact lost chunk receipt without advancing twice.
            with source.open('rb') as stream:
                stream.seek(2 * self.CHUNK); repeated = stream.read(self.CHUNK)
            duplicate = self.call('attachment-upload-chunk', transfer_id=transfer_id, offset=2*self.CHUNK,
                sha256=hashlib.sha256(repeated).hexdigest(), content_base64=base64.b64encode(repeated).decode('ascii'))
            self.assertEqual(duplicate['received'], 3 * self.CHUNK)
            self.stop(); self.start()
            resumed_index = len(attempts)
            with self.lose_response(lambda value: bool(value.get('attachments')) and value.get('sender_session') == 'large-sender'):
                with self.assertRaises(RemoteCoordError):
                    self.command('large-sender', *send)
            resumed = [offset for operation, offset in attempts[resumed_index:] if operation == 'attachment-upload-chunk']
            self.assertTrue(resumed)
            self.assertEqual(min(resumed), 3 * self.CHUNK)
            self.assertEqual(len(resumed), len(set(resumed)))
            self.stop(); self.start()
            published_index = len(attempts)
            output = self.command('large-sender', *send)
            self.assertFalse(any(op == 'attachment-upload-chunk' for op, _ in attempts[published_index:]))
        # All emitted CLI JSON is parsed exclusively by jq.
        receipt = subprocess.run(['jq', '-er', '[.id, .attachments[0].id] | @tsv'],
            input=output, text=True, capture_output=True, check=True)
        message_id, attachment_id = receipt.stdout.strip().split('\t')
        with contextlib.closing(core.Coordinator(self.db)) as coord:
            self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM messages WHERE sender_session=? AND recipient_session=?',
                ('large-sender','large-recipient')).fetchone()[0], 1)
            self.assertEqual(coord.db.execute('SELECT COUNT(*),MAX(length(content)) FROM attachments').fetchone()[:], (1,0))
            self.assertLessEqual(coord.db.execute('SELECT MAX(length(content)) FROM attachment_transfer_chunks').fetchone()[0], self.CHUNK)
        params = {'source_project': 'default', 'message_id': message_id,
                  'attachment_id': attachment_id}
        destination = self.root / 'received.txt'
        download = ['attachment-download', '--resume', '--output', str(destination),
                    '--source-project', 'default', '--message-id', message_id, '--attachment-id', attachment_id]
        attempts.clear()
        with mock.patch.object(HttpClient, 'call', counted):
            with self.lose_response(lambda value: value.get('attachment_id') == attachment_id and value.get('offset') == 2*self.CHUNK):
                with self.assertRaises(RemoteCoordError):
                    self.command('large-recipient', *download)
            self.assertFalse(destination.exists())
            partials = list(self.root.glob('.agent-chat-resume-*.part'))
            self.assertEqual(len(partials), 1)
            self.assertEqual(partials[0].stat().st_size, 2*self.CHUNK)
            self.stop(); self.start()
            resumed_index = len(attempts)
            self.command('large-recipient', *download)
            resumed = [offset for operation, offset in attempts[resumed_index:] if operation == 'attachment-download-chunk']
            self.assertEqual(min(resumed), 2*self.CHUNK)
            self.assertEqual(len(resumed), len(set(resumed)))
        actual = hashlib.sha256()
        with destination.open('rb') as stream:
            for block in iter(lambda: stream.read(self.CHUNK), b''):
                actual.update(block)
        self.assertEqual(destination.stat().st_size, self.SIZE)
        self.assertEqual(actual.hexdigest(), digest.hexdigest())
        self.assertFalse(list(self.root.glob('.agent-chat-resume-*.part')))
        with self.assertRaisesRegex(core.CoordError, 'already exists'):
            self.command('large-recipient', *download)
        with self.assertRaises(RemoteCoordError):
            self.call('attachment-download-chunk', session='large-unrelated', offset=0, **params)
        peak_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        multiplier = 1 if platform.system() == 'Darwin' else 1024
        growth = (peak_rss - initial_rss) * multiplier
        self.assertLess(growth, 128 * 1024 * 1024, 'transfer allocated a substantial fraction of the whole file')
        retained_disk = sum(path.stat().st_size for path in self.root.rglob('*') if path.is_file())
        print('upload_resume_offset=' + str(3*self.CHUNK) + ' download_resume_offset=' + str(2*self.CHUNK)
              + ' peak_rss_growth_bytes=' + str(growth) + ' retained_fixture_disk_bytes=' + str(retained_disk))

    def test_transfer_rejects_changed_identity_payload_hash_and_default_cap(self):
        data = b'ordinary inert fixture\n'
        transfer_id = str(uuid.uuid4())
        begin = dict(transfer_id=transfer_id, to='large-recipient', name='small.txt',
                     size=len(data), sha256=hashlib.sha256(data).hexdigest(), body='Exact body', reply_to=None)
        self.call('attachment-upload-begin', **begin)
        for changed in (dict(begin, body='Changed'), dict(begin, to='large-unrelated'),
                        dict(begin, sha256='0'*64), dict(begin, name='changed.diff')):
            with self.assertRaises(RemoteCoordError):
                self.call('attachment-upload-begin', **changed)
        for identity in (dict(session='large-unrelated'), dict(host='wrong-host')):
            with self.assertRaises(RemoteCoordError):
                self.call('attachment-upload-status', transfer_id=transfer_id, **identity)
        with self.assertRaises(RemoteCoordError):
            self.call('attachment-upload-begin', **dict(begin, transfer_id=str(uuid.uuid4())))
        chunk = dict(transfer_id=transfer_id, offset=0, sha256=hashlib.sha256(data).hexdigest(),
                     content_base64=base64.b64encode(data).decode('ascii'))
        with self.assertRaises(RemoteCoordError):
            self.call('attachment-upload-chunk', **dict(chunk, sha256='0'*64))
        self.assertEqual(self.call('attachment-upload-chunk', **chunk)['received'], len(data))
        altered = b'x' * len(data)
        with self.assertRaises(RemoteCoordError):
            self.call('attachment-upload-chunk', **dict(chunk, sha256=hashlib.sha256(altered).hexdigest(),
                content_base64=base64.b64encode(altered).decode('ascii')))
        with self.assertRaises(RemoteCoordError):
            self.call('attachment-upload-send', transfer_id=transfer_id, body='changed')
        sent = self.call('attachment-upload-send', transfer_id=transfer_id)
        self.assertEqual(self.call('attachment-upload-send', transfer_id=transfer_id)['id'], sent['id'])
        params = dict(source_project='default', message_id=sent['id'], attachment_id=sent['attachments'][0]['id'], offset=0)
        for changed in (dict(params, message_id='message_wrong'), dict(params, source_project='wrong-project'),
                        dict(params, prefix_sha256='0'*64), dict(params, offset=1)):
            with self.assertRaises(RemoteCoordError):
                self.call('attachment-download-chunk', session='large-recipient', **changed)
        with self.assertRaises(RemoteCoordError):
            self.call('attachment-download-chunk', session='large-recipient', host='wrong-host', **params)
        # Default attachment path still enforces its original 10 MiB bound.
        ordinary = self.root / 'ordinary.txt'
        with ordinary.open('wb') as stream:
            for _ in range(10): stream.write(b'x' * self.CHUNK)
            stream.write(b'x')
        with self.assertRaisesRegex(core.CoordError, 'format-specific size limit'):
            self.command('large-sender', 'send', '--to', 'large-recipient', '--body-file', str(self.body), '--attach', str(ordinary))
        for extra in (['--attach', str(ordinary)], ['--ack-reply', '--reply-to', sent['id']]):
            with self.assertRaisesRegex(core.CoordError, 'cannot mix'):
                self.command('large-sender', 'send', '--to', 'large-recipient', '--body-file', str(self.body),
                    '--large-attach', str(ordinary), '--transfer-id', str(uuid.uuid4()), *extra)
        # Invalid text never becomes an attachment; explicit own cancellation
        # releases the one active staging slot without altering published bytes.
        invalid = b'#!/bin/sh\necho inert\n'
        bad = dict(begin, transfer_id=str(uuid.uuid4()), size=len(invalid), sha256=hashlib.sha256(invalid).hexdigest())
        self.call('attachment-upload-begin', **bad)
        self.call('attachment-upload-chunk', transfer_id=bad['transfer_id'], offset=0,
            sha256=bad['sha256'], content_base64=base64.b64encode(invalid).decode('ascii'))
        with self.assertRaisesRegex(RemoteCoordError, 'executable scripts'):
            self.call('attachment-upload-send', transfer_id=bad['transfer_id'])
        self.assertEqual(self.call('attachment-upload-cancel', transfer_id=bad['transfer_id'])['state'], 'cancelled')
        with self.assertRaises(RemoteCoordError):
            self.call('attachment-upload-cancel', transfer_id=transfer_id)

    def publish_browser_fixture(self):
        data = b'Private intended-recipient fixture bytes.\n'
        transfer_id = str(uuid.uuid4())
        digest = hashlib.sha256(data).hexdigest()
        self.call('attachment-upload-begin', transfer_id=transfer_id, to='large-recipient',
            name='fixture.txt', size=len(data), sha256=digest, body='Inert fixture', reply_to=None)
        self.call('attachment-upload-chunk', transfer_id=transfer_id, offset=0,
            sha256=digest, content_base64=base64.b64encode(data).decode('ascii'))
        result = self.call('attachment-upload-send', transfer_id=transfer_id)
        return result, data

    def browser_attachment(self, attachment_id, authorization=None, project=None):
        connection = http.client.HTTPConnection('127.0.0.1', self.server.server_port, timeout=5)
        try:
            headers = {}
            if authorization is not None:
                headers['Authorization'] = authorization
            if project is not None:
                headers['X-Agent-Chat-Project'] = project
            connection.request('GET', '/api/attachments/' + attachment_id, headers=headers)
            response = connection.getresponse()
            return response.status, response.read()
        finally:
            connection.close()

    def test_shared_operator_capability_and_recipient_api_contract(self):
        result, data = self.publish_browser_fixture()
        attachment = result['attachments'][0]['id']
        # The documented shared server key carries operator authority in either
        # HTTP scheme. Re-encoding it is not a new human credential or grant.
        secret = self.client.token
        basic = 'Basic ' + base64.b64encode(('operator:' + secret).encode()).decode('ascii')
        for authorization in ('Bearer ' + secret, basic):
            self.assertEqual(self.browser_attachment(attachment, authorization), (200, data))
        self.assertEqual(self.browser_attachment(attachment)[0], 401)
        self.assertEqual(self.browser_attachment(attachment, 'Bearer incorrect-fixture-secret')[0], 401)
        other = self.server.projects.create('Other isolated project')['id']
        # This is project-qualified lookup, not a scoped-credential claim.
        self.assertEqual(self.browser_attachment(attachment, basic, other)[0], 404)
        params = dict(source_project='default', message_id=result['id'], attachment_id=attachment, offset=0)
        downloaded = self.call('attachment-download-chunk', session='large-recipient', **params)
        self.assertEqual(base64.b64decode(downloaded['content_base64']), data)
        with self.assertRaises(RemoteCoordError):
            self.call('attachment-download-chunk', session='large-unrelated', **params)
        with self.assertRaises(RemoteCoordError):
            self.call('attachment-download-chunk', session='large-recipient', host='wrong-fixture-host', **params)

    def test_intentional_tokenless_loopback_browser_contract(self):
        result, data = self.publish_browser_fixture()
        self.stop()
        self.server = create_server(self.db, host='127.0.0.1', port=0,
            api_token=None, sessions_root=self.sessions)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.assertEqual(self.browser_attachment(result['attachments'][0]['id']), (200, data))

    def test_staging_cap_cancel_cleanup_and_reusable_sender_slot(self):
        import sqlite3
        published, data = self.publish_browser_fixture()
        transfer_id = str(uuid.uuid4())
        begin = dict(transfer_id=transfer_id, to='large-recipient', name='staged.txt',
            size=500 * 1024 * 1024, sha256='0' * 64, body='Inert fixture', reply_to=None)
        self.call('attachment-upload-begin', **begin)
        with self.assertRaises(RemoteCoordError):
            self.call('attachment-upload-begin', **dict(begin, transfer_id=str(uuid.uuid4())))
        chunk = b'a' * self.CHUNK
        self.call('attachment-upload-chunk', transfer_id=transfer_id, offset=0,
            sha256=hashlib.sha256(chunk).hexdigest(), content_base64=base64.b64encode(chunk).decode('ascii'))
        with self.assertRaises(RemoteCoordError):
            self.call('attachment-upload-cancel', host='wrong-fixture-host', transfer_id=transfer_id)
        with sqlite3.connect(self.db) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM attachment_transfer_chunks WHERE transfer_id=?',
                (transfer_id,)).fetchone()[0], 1)
        self.assertEqual(self.call('attachment-upload-cancel', transfer_id=transfer_id)['state'], 'cancelled')
        with sqlite3.connect(self.db) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM attachment_transfer_chunks WHERE transfer_id=?',
                (transfer_id,)).fetchone()[0], 0)
        with self.assertRaises(RemoteCoordError):
            self.call('attachment-upload-begin', **dict(begin, transfer_id=str(uuid.uuid4()), size=500 * 1024 * 1024 + 1))
        replacement = str(uuid.uuid4())
        self.call('attachment-upload-begin', **dict(begin, transfer_id=replacement, size=1))
        self.assertEqual(self.call('attachment-upload-cancel', transfer_id=replacement)['state'], 'cancelled')
        self.assertEqual(self.browser_attachment(published['attachments'][0]['id'],
            'Bearer ' + self.client.token), (200, data))


if __name__ == '__main__':
    unittest.main()
