import json
import os
from pathlib import Path
import selectors
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock
import urllib.request

from agent_chat import server


class ServerTests(unittest.TestCase):
    def test_token_file_is_persistent_private_and_bootstraps_parent(self):
        with tempfile.TemporaryDirectory(prefix='agent-chat-server-') as directory:
            db = Path(directory) / 'missing' / 'state.sqlite3'
            first, path = server.api_token_file(db)
            self.assertEqual(path, Path(str(db) + '.api-token').resolve())
            self.assertTrue(path.parent.is_dir())
            self.assertGreaterEqual(len(first), 24)
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            path.chmod(0o644)
            second, second_path = server.api_token_file(db)
            self.assertEqual(second, first)
            self.assertEqual(second_path, path)
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)

    def test_empty_token_file_is_rejected(self):
        with tempfile.TemporaryDirectory(prefix='agent-chat-server-') as directory:
            db = Path(directory) / 'state.sqlite3'
            token_path = Path(str(db) + '.api-token')
            token_path.write_text('')
            with self.assertRaisesRegex(ValueError, 'empty'):
                server.api_token_file(db)

    def test_database_symlink_uses_the_canonical_token(self):
        with tempfile.TemporaryDirectory(prefix='agent-chat-server-') as directory:
            root = Path(directory)
            database = root / 'app' / 'state.sqlite3'
            database.parent.mkdir()
            database.touch()
            alias = root / 'project' / 'state.sqlite3'
            alias.parent.mkdir()
            alias.symlink_to(database)
            token, path = server.api_token_file(database)
            alias_token, alias_path = server.api_token_file(alias)
            self.assertEqual(alias_token, token)
            self.assertEqual(alias_path, path)
            self.assertFalse(Path(str(alias) + '.api-token').exists())

    def test_server_default_ignores_client_project_and_database_environment(self):
        with tempfile.TemporaryDirectory(prefix='agent-chat-server-') as directory:
            root = Path(directory)
            env = {'AGENT_CHAT_ROOT': str(root / 'project'),
                   'AGENT_CHAT_DB': str(root / 'project' / 'old-state.sqlite3'),
                   'AGENT_CHAT_API_TOKEN': ''}
            with mock.patch.dict(os.environ, env), mock.patch.object(server.Path, 'cwd', return_value=root), \
                    mock.patch.object(server, 'api_token_file', side_effect=ValueError('stop before serving')) as token_file, \
                    mock.patch('sys.stderr'):
                self.assertEqual(server.main([]), 2)
            token_file.assert_called_once_with(str((root / '.agent-chat' / 'state.sqlite3').resolve()))
            self.assertFalse((root / 'project').exists())

    def test_bad_port_is_rejected(self):
        with self.assertRaises(SystemExit) as result:
            server.main(['--port', '-1'])
        self.assertEqual(result.exception.code, 2)

    def test_checkout_launcher_ignores_inherited_db_and_cwd_but_accepts_explicit_db(self):
        source = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(prefix='agent-chat-launcher-') as directory:
            root = Path(directory)
            checkout, project = root / 'app', root / 'project'
            (checkout / 'bin').mkdir(parents=True)
            project.mkdir()
            launcher = checkout / 'bin' / 'agent-chat-server'
            shutil.copy2(source / 'bin' / 'agent-chat-server', launcher)
            (checkout / 'src').symlink_to(source / 'src', target_is_directory=True)
            env = dict(os.environ, AGENT_CHAT_DB=str(project / 'old-state.sqlite3'),
                       AGENT_CHAT_ROOT=str(project), AGENT_CHAT_API_TOKEN='')
            for explicit in (False, True):
                with self.subTest(explicit_db=explicit):
                    database = (root / 'custom' if explicit else checkout / '.agent-chat') / 'state.sqlite3'
                    args = [sys.executable, str(launcher), '--port', '0']
                    if explicit:
                        args += ['--db', str(database)]
                    proc = subprocess.Popen(args, cwd=project, env=env, text=True,
                                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                    try:
                        with selectors.DefaultSelector() as selector:
                            selector.register(proc.stdout, selectors.EVENT_READ)
                            self.assertTrue(selector.select(8), 'launcher did not print its URL')
                        self.assertTrue(proc.stdout.readline().strip().startswith('http://127.0.0.1:'))
                        self.assertEqual(proc.stderr.readline().strip(),
                                         f'API token file: {database.resolve()}.api-token')
                        self.assertTrue(database.is_file())
                        self.assertEqual(list(project.iterdir()), [])
                        proc.send_signal(signal.SIGTERM)
                        self.assertEqual(proc.wait(timeout=10), 0)
                    finally:
                        if proc.poll() is None:
                            proc.kill()
                            proc.wait(5)
                        proc.stdout.close()
                        proc.stderr.close()

    def test_default_server_authenticates_machine_registration_and_stops(self):
        with tempfile.TemporaryDirectory(prefix='agent-chat-server-') as directory:
            db = str(Path(directory) / 'state.sqlite3')
            env = dict(os.environ)
            env.pop('AGENT_CHAT_API_TOKEN', None)
            source = str(Path(__file__).resolve().parents[1] / 'src')
            env['PYTHONPATH'] = source + (os.pathsep + env['PYTHONPATH'] if env.get('PYTHONPATH') else '')
            proc = subprocess.Popen(
                [sys.executable, '-m', 'agent_chat.server', '--db', db, '--port', '0'],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env,
            )
            try:
                with selectors.DefaultSelector() as selector:
                    selector.register(proc.stdout, selectors.EVENT_READ)
                    self.assertTrue(selector.select(8), 'server did not print its URL')
                url = proc.stdout.readline().strip()
                self.assertTrue(url.startswith('http://127.0.0.1:'), url)
                token_path = Path(db + '.api-token')
                for _ in range(80):
                    if token_path.exists():
                        break
                    time.sleep(.05)
                token = token_path.read_text().strip()
                request = urllib.request.Request(
                    url + 'api/coord',
                    data=json.dumps({
                        'op': 'register', 'session': 'client_session', 'host_id': 'client_host',
                        'params': {'agent': 'client'},
                    }).encode(),
                    headers={'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'},
                    method='POST',
                )
                with urllib.request.urlopen(request, timeout=4) as response:
                    registered = json.load(response)
                self.assertEqual(registered['session'], 'client_session')
                self.assertEqual(stat.S_IMODE(token_path.stat().st_mode), 0o600)
                proc.send_signal(signal.SIGTERM)
                self.assertEqual(proc.wait(timeout=15), 0)
            finally:
                if proc.poll() is None:
                    proc.kill()
                    proc.wait(5)
                proc.stdout.close()
                proc.stderr.close()
