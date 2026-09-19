import json
import os
from pathlib import Path
import subprocess
import sys
from unittest import mock
import uuid

from agent_chat import cli
from test_remote_web import RemoteWebFixture, TOKEN


class ClientCliTests(RemoteWebFixture):
    def command(self, *args):
        env = {key: value for key, value in os.environ.items()
               if not key.startswith('AGENT_CHAT_')}
        env.update(AGENT_CHAT_SERVER=self.url, AGENT_CHAT_API_TOKEN=TOKEN,
                   AGENT_CHAT_HOST_ID='mac', AGENT_CHAT_STATE_DIR=self.tmp.name)
        path = Path(__file__).resolve().parents[1] / 'bin' / 'agent-chat-client'
        result = subprocess.run([sys.executable, str(path), *args],
                                text=True, capture_output=True, env=env, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)

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

    def test_resource_token_flag_remains_separate_from_api_token(self):
        self.command('--session', 'a', 'register', '--agent', 'alpha')
        claim = self.command('--session', 'a', 'request', 'shared', '--minutes', '1')
        result = self.command('--session', 'a', '--api-token', TOKEN,
                              'check', 'shared', '--token', claim['token'])
        self.assertEqual(result['state'], 'owned')
        self.assertEqual(result['reservation_id'], claim['reservation_id'])

    def test_public_client_has_no_database_fallback(self):
        with mock.patch('agent_chat.cli.core.Coordinator', side_effect=AssertionError('local database used')):
            with self.assertRaises(SystemExit):
                cli.main(['--db', str(Path(self.tmp.name) / 'never.sqlite3'), 'status'])
        self.assertFalse((Path(self.tmp.name) / 'never.sqlite3').exists())
