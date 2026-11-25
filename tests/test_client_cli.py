import contextlib
import io
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
from unittest import mock
import uuid

from agent_chat import cli
from test_remote_web import RemoteWebFixture, TOKEN


class ClientCliTests(RemoteWebFixture):
    def command(self, *args, host_id='mac'):
        env = {key: value for key, value in os.environ.items()
               if not key.startswith('AGENT_CHAT_')}
        env.update(AGENT_CHAT_SERVER=self.url, AGENT_CHAT_API_TOKEN=TOKEN,
                   AGENT_CHAT_HOST_ID=host_id, AGENT_CHAT_STATE_DIR=self.tmp.name)
        path = Path(__file__).resolve().parents[1] / 'bin' / 'agent-chat-client'
        result = subprocess.run([sys.executable, str(path), *args],
                                text=True, capture_output=True, env=env, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)

    def _body_file(self, name, body):
        path = Path(self.tmp.name) / name
        path.write_text(body, encoding='utf-8')
        return str(path)

    def test_first_ack_lookup_uses_real_handler_readonly_path(self):
        from agent_chat.core import Coordinator
        from agent_chat.remote_service import dispatch

        self.command('--session', 'caller', 'register', '--agent', 'caller')
        self.command('--session', 'operator', 'register', '--agent', 'operator')
        source = self.command('--session', 'operator', 'send', '--to', 'caller',
                              '--body-file', self._body_file('source.txt', 'private source'))
        with sqlite3.connect(self.db) as db:
            db.execute('INSERT INTO resources(name,owner_session,reservation_id,token,granted_at,deadline,stale,reason) '
                       'VALUES(?,?,?,?,?,?,?,?)',
                       ('expired-held', 'caller', 'expired-rid', 'private-token', 1, 1, 0, None))

        before = self._ack_state(source['id'])
        with mock.patch('agent_chat.web.Coordinator', wraps=Coordinator) as constructor:
            absent = self.command('--session', 'caller', 'first-ack-lookup', source['id'],
                                  '--expected-recipient', 'operator')
            self.assertEqual(absent['status'], 'not_recorded_at_read')
            constructor.assert_not_called()
            after_absent = self._ack_state(source['id'])
            self.assertEqual(after_absent, before)
        with mock.patch('agent_chat.remote_service.dispatch', wraps=dispatch) as generic_dispatch:
            reply = self.command('--session', 'caller', 'send', '--to', 'operator',
                                 '--body-file', self._body_file('reply.txt', 'private reply'),
                                 '--reply-to', source['id'], '--ack-reply')
            generic_dispatch.assert_called_once()
            self.assertEqual(generic_dispatch.call_args.args[1]['op'], 'send')
        with mock.patch('agent_chat.web.Coordinator', wraps=Coordinator) as constructor:
            before_recorded = self._ack_state(source['id'])
            recorded = self.command('--session', 'caller', 'first-ack-lookup', source['id'],
                                    '--expected-recipient', 'operator')
            self.assertEqual(recorded['status'], 'recorded')
            self.assertEqual(recorded['reply_id'], reply['id'])
            self.assertNotIn('body', recorded)
            self.assertNotIn('fingerprint', recorded)
            constructor.assert_not_called()
            after_recorded = self._ack_state(source['id'])
            # The lookup adds no inbox/read marker, host binding update, schema
            # change, or expiry sweep. The ACK send may update those normally.
            self.assertEqual(after_recorded, before_recorded)

    def _ack_state(self, source_id):
        with sqlite3.connect(self.db) as db:
            resource = db.execute('SELECT owner_session,reservation_id,token,deadline,stale FROM resources '
                                  'WHERE name=?', ('expired-held',)).fetchone()
            host = db.execute('SELECT host_id,last_seen_at FROM remote_session_hosts WHERE session_id=?',
                              ('caller',)).fetchone()
            reads = db.execute('SELECT COUNT(*) FROM message_reads WHERE message_id=?', (source_id,)).fetchone()[0]
            schema = tuple(db.execute("SELECT name,sql FROM sqlite_master WHERE type IN ('table','trigger') ORDER BY type,name"))
            inbox = db.execute('SELECT inbox_read_seq FROM sessions WHERE id=?', ('caller',)).fetchone()[0]
        return {'resource': resource, 'host': host, 'reads': reads, 'schema': schema, 'inbox': inbox}

    def _lookup_route_snapshot(self, path):
        with sqlite3.connect(path) as db:
            schema = tuple(db.execute("SELECT name,sql FROM sqlite_master WHERE type IN ('table','trigger') ORDER BY type,name"))
            tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            sessions = (tuple(db.execute('SELECT id,inbox_read_seq FROM sessions ORDER BY id'))
                        if 'sessions' in tables else None)
            hosts = (tuple(db.execute('SELECT session_id,host_id,last_seen_at FROM remote_session_hosts ORDER BY session_id,host_id'))
                     if 'remote_session_hosts' in tables else None)
            counts = {}
            for table in ('messages', 'message_replies', 'reply_ack_idempotency', 'message_reads', 'resources'):
                counts[table] = (db.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]
                                 if table in tables else None)
        return schema, sessions, hosts, counts

    def test_first_ack_lookup_route_denials_are_nonmutating(self):
        from agent_chat.core import Coordinator

        self.command('--session', 'caller', 'register', '--agent', 'caller')
        self.command('--session', 'other', 'register', '--agent', 'other')
        self.command('--session', 'operator', 'register', '--agent', 'operator')
        incoming = self.command('--session', 'operator', 'send', '--to', 'caller',
                                '--body-file', self._body_file('incoming.txt', 'private source'))
        outgoing = self.command('--session', 'caller', 'send', '--to', 'operator',
                                '--body-file', self._body_file('outgoing.txt', 'private reverse source'))
        before = self._lookup_route_snapshot(self.db)

        with mock.patch('agent_chat.web.Coordinator', wraps=Coordinator) as constructor:
            wrong_recipient = self.command('--session', 'caller', 'first-ack-lookup', incoming['id'],
                                           '--expected-recipient', 'other')
            wrong_caller = self.command('--session', 'other', 'first-ack-lookup', incoming['id'],
                                        '--expected-recipient', 'operator')
            wrong_direction = self.command('--session', 'caller', 'first-ack-lookup', outgoing['id'],
                                           '--expected-recipient', 'operator')
            self.assertEqual(wrong_recipient, {'status': 'unauthorized'})
            self.assertEqual(wrong_caller, {'status': 'unauthorized'})
            self.assertEqual(wrong_direction, {'status': 'unauthorized'})
            constructor.assert_not_called()
        self.assertEqual(self._lookup_route_snapshot(self.db), before)

        project = self.command('project', 'create', '--name', 'Wrong ACK lookup scope')['project']['id']
        self.command('--project', project, '--session', 'caller', 'register', '--agent', 'caller')
        project_db = self.server.projects.db_path(project)
        project_before = self._lookup_route_snapshot(project_db)
        with mock.patch('agent_chat.web.Coordinator', wraps=Coordinator) as constructor:
            wrong_project = self.command('--project', project, '--session', 'caller',
                                         'first-ack-lookup', incoming['id'],
                                         '--expected-recipient', 'operator')
            self.assertEqual(wrong_project, {'status': 'unauthorized'})
            constructor.assert_not_called()
        self.assertEqual(self._lookup_route_snapshot(project_db), project_before)

    def test_first_ack_lookup_does_not_recreate_unlinked_registry(self):
        from agent_chat.core import Coordinator

        self.command('--session', 'caller', 'register', '--agent', 'caller')
        self.command('--session', 'operator', 'register', '--agent', 'operator')
        source = self.command('--session', 'operator', 'send', '--to', 'caller',
                              '--body-file', self._body_file('source.txt', 'private source'))
        registry = Path(self.server.projects.path)
        self.assertTrue(registry.is_file())
        readiness_db = self.server.roots._readiness_db

        @contextlib.contextmanager
        def unlink_registry_after_read(path):
            with readiness_db(path) as db:
                yield db
            if Path(path) == registry:
                registry.unlink()

        with mock.patch.object(self.server.roots, '_readiness_db', side_effect=unlink_registry_after_read), \
             mock.patch('agent_chat.web.Coordinator', wraps=Coordinator) as constructor:
            result = self.command('--session', 'caller', 'first-ack-lookup', source['id'],
                                  '--expected-recipient', 'operator')
            self.assertEqual(result['status'], 'not_recorded_at_read')
            constructor.assert_not_called()
        self.assertFalse(registry.exists(), 'lookup must not recreate a registry unlinked after selection')

    def test_first_ack_lookup_denies_wrong_or_missing_host_without_binding(self):
        self.command('--session', 'caller', 'register', '--agent', 'caller')
        self.command('--session', 'operator', 'register', '--agent', 'operator')
        source = self.command('--session', 'operator', 'send', '--to', 'caller',
                              '--body-file', self._body_file('source.txt', 'private source'))
        wrong = self.command('--session', 'caller', 'first-ack-lookup', source['id'],
                             '--expected-recipient', 'operator', host_id='other-host')
        self.assertEqual(wrong['status'], 'unauthorized')
        with sqlite3.connect(self.db) as db:
            db.execute('DELETE FROM remote_session_hosts WHERE session_id=?', ('caller',))
        unbound = self.command('--session', 'caller', 'first-ack-lookup', source['id'],
                               '--expected-recipient', 'operator')
        self.assertEqual(unbound['status'], 'unauthorized')
        with sqlite3.connect(self.db) as db:
            self.assertIsNone(db.execute('SELECT 1 FROM remote_session_hosts WHERE session_id=?',
                                         ('caller',)).fetchone())

    def test_client_registers_and_binds_without_reading_inbox(self):
        self.command('--session', 'a', 'register', '--agent', 'alpha')
        thread_id = str(uuid.uuid4())
        self.command('--session', 'a', 'bind', '--thread', thread_id)
        self.assertEqual(self.command('--session', 'a', 'bridge-status')['bindings'][0]['thread_id'], thread_id)
        from agent_chat.core import Coordinator
        coord = Coordinator(self.db)
        try:
            self.assertEqual(coord.db.execute("SELECT inbox_read_seq FROM sessions WHERE id='a'").fetchone()[0], 0)
        finally:
            coord.close()

    def test_project_commands_and_deregister_use_selected_server_project(self):
        project = self.command('project', 'create', '--name', 'CLI')['project']['id']
        self.command('--project', project, '--session', 'a', 'register', '--agent', 'alpha')
        sessions = self.command('--project', project, '--session', 'a', 'status')['sessions']
        self.assertIn('a', [item['session'] for item in sessions])
        self.command('--project', project, '--session', 'a', 'deregister')
        status, raw, _ = self.request('GET', '/api/snapshot')
        self.assertEqual(status, 200)
        self.assertNotIn('a', [item['id'] for item in json.loads(raw)['sessions']])

    def test_default_and_explicit_bridge_forward_globals(self):
        with mock.patch('agent_chat.bridge_client.main', return_value=0) as bridge:
            for suffix in ([], ['bridge', '--connect-only']):
                self.assertEqual(cli.main(['--server', self.url, '--api-token', TOKEN, '--project', 'default', *suffix]), 0)
                expected = ['--server', self.url, '--api-token', TOKEN, '--project', 'default']
                self.assertEqual(bridge.call_args.args[0], expected + suffix[1:])

    def test_agent_investigation_preload_cli_emits_only_exact_no_input_scope(self):
        continuation_id, request_id, thread_id = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())
        output = io.StringIO()
        with mock.patch('agent_chat.cli._logged_call', return_value={'status':'pending'}) as call, \
                mock.patch('agent_chat.remote.client_host_id', return_value='host'), \
                contextlib.redirect_stdout(output):
            self.assertEqual(cli.main(['--server','http://127.0.0.1:8765','--project','project',
                '--session','current-po','agent-investigation-preload','--to','worker',
                '--expected-thread',thread_id,'--continuation-id',continuation_id,
                '--request-id',request_id,'--confirm']),0)
        payload = call.call_args.args[3]
        self.assertEqual(payload['op'],'agent-investigation-preload')
        self.assertEqual(payload['params'], {'to':'worker','expected_thread':thread_id,
            'continuation_id':continuation_id,'request_id':request_id,'confirm':True})
        self.assertEqual(output.getvalue().strip(), '{\"status\": \"pending\"}')

    def test_resource_token_flag_remains_separate_from_api_token(self):
        self.command('--session', 'a', 'register', '--agent', 'alpha')
        from agent_chat.private_artifacts import read_capability
        private = Path(self.tmp.name) / 'capabilities'
        private.mkdir(mode=0o700)
        path = private / 'shared.json'
        metadata = self.command('--session', 'a', 'request', 'shared', '--minutes', '1',
                                '--capability-file', str(path))
        self.assertNotIn('token', metadata)
        claim = read_capability(path)
        result = self.command('--session', 'a', '--api-token', TOKEN,
                              'check', 'shared', '--token', claim['token'])
        self.assertEqual(result['state'], 'owned')
        self.assertEqual(result['reservation_id'], claim['reservation_id'])

    def test_public_client_has_no_database_fallback(self):
        with mock.patch('agent_chat.cli.core.Coordinator', side_effect=AssertionError('local database used')):
            with self.assertRaises(SystemExit):
                cli.main(['--db', str(Path(self.tmp.name) / 'never.sqlite3'), 'status'])
        self.assertFalse((Path(self.tmp.name) / 'never.sqlite3').exists())
