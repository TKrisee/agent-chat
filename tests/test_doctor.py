import contextlib
import io
import os
from pathlib import Path
import subprocess
import sqlite3
from unittest import mock

from agent_chat import cli, doctor
from test_remote_web import RemoteWebFixture, TOKEN


class DoctorTests(RemoteWebFixture):
    def setUp(self):
        super().setUp()
        self.state = Path(self.tmp.name) / 'unused-host-state'
        self.environment = mock.patch.dict(os.environ, {
            'AGENT_CHAT_ROOT': self.tmp.name, 'AGENT_CHAT_STATE_DIR': str(self.state),
            'AGENT_CHAT_SERVER': self.url, 'AGENT_CHAT_API_TOKEN': TOKEN,
            'AGENT_CHAT_PROJECT': 'default',
        })
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.tools = mock.patch.object(doctor.shutil, 'which', side_effect=lambda name: '/usr/bin/' + name)
        self.tools.start()
        self.addCleanup(self.tools.stop)

    def diagnose(self, **kwargs):
        return doctor.diagnose(server=kwargs.pop('server', self.url), api_token=kwargs.pop('api_token', TOKEN), **kwargs)

    def test_ready_reads_server_without_registering_or_creating_identity(self):
        before = self.client.call('/api/projects/rpc', {'op': 'list'})
        with sqlite3.connect(self.db) as database:
            sessions = database.execute('SELECT id FROM sessions ORDER BY id').fetchall()
        report = self.diagnose()
        self.assertTrue(report['ready'])
        self.assertFalse(self.state.exists())
        self.assertEqual(self.client.call('/api/projects/rpc', {'op': 'list'}), before)
        with sqlite3.connect(self.db) as database:
            self.assertEqual(database.execute('SELECT id FROM sessions ORDER BY id').fetchall(), sessions)

    def test_missing_jq_and_invalid_project_fail(self):
        self.tools.stop()
        with mock.patch.object(doctor.shutil, 'which', side_effect=lambda name: None if name == 'jq' else '/usr/bin/' + name):
            report = self.diagnose(project='missing-project')
        self.assertFalse(report['ready'])
        failed = {item['name'] for item in report['checks'] if not item['ok']}
        self.assertEqual(failed, {'jq', 'project'})

    def test_unreachable_server_and_invalid_token_fail_without_exposing_secrets(self):
        for options in ({'server': 'http://127.0.0.1:0'}, {'api_token': 'private-wrong-token'}):
            report = self.diagnose(**options)
            self.assertFalse(report['ready'])
            self.assertNotIn(TOKEN, str(report))
            self.assertNotIn('private-wrong-token', str(report))
        with mock.patch.object(doctor.HttpClient, 'call', side_effect=ValueError(TOKEN)):
            self.assertNotIn(TOKEN, str(self.diagnose()))

    def test_missing_token_skips_http_and_invalid_root_fails(self):
        with mock.patch.object(doctor.HttpClient, 'call') as call, mock.patch.dict(os.environ, {'AGENT_CHAT_ROOT': str(self.state)}):
            report = self.diagnose(api_token=None)
        call.assert_not_called()
        self.assertFalse(report['ready'])
        self.assertFalse(self.state.exists())

    def test_bridge_queries_only_version_and_existing_login_and_hides_outputs(self):
        with mock.patch.object(doctor.subprocess, 'run', return_value=subprocess.CompletedProcess([], 0, TOKEN, TOKEN)) as run:
            report = self.diagnose(bridge=True, codex_bin='custom-codex')
        self.assertTrue(report['ready'])
        self.assertNotIn(TOKEN, str(report))
        self.assertEqual([call.args[0] for call in run.call_args_list],
                         [['/usr/bin/custom-codex', '--version'], ['/usr/bin/custom-codex', 'login', 'status']])
        for call in run.call_args_list:
            self.assertEqual(call.kwargs['stdin'], subprocess.DEVNULL)
            self.assertNotIn('AGENT_CHAT_API_TOKEN', call.kwargs['env'])

    def test_bridge_missing_executable_and_failed_login_fail(self):
        self.tools.stop()
        with mock.patch.object(doctor.shutil, 'which', side_effect=lambda name: None if name == 'codex' else '/usr/bin/' + name):
            self.assertFalse(self.diagnose(bridge=True)['ready'])
        with mock.patch.object(doctor.shutil, 'which', return_value='/usr/bin/codex'), \
             mock.patch.object(doctor.subprocess, 'run', return_value=subprocess.CompletedProcess([], 1, TOKEN, TOKEN)):
            report = self.diagnose(bridge=True)
        self.assertFalse(report['ready'])
        self.assertNotIn(TOKEN, str(report))

    def test_cli_exit_status_and_dispatch(self):
        with contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(cli.main(['doctor']), 0)
        self.assertNotIn(TOKEN, output.getvalue())
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(cli.main(['doctor', '--project', 'missing-project']), 2)
