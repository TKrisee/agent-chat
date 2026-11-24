import subprocess
import unittest
import tempfile
from pathlib import Path
from unittest import mock
from agent_chat.bridge_client import version_observation
from agent_chat.bridge_state import BridgeState
from agent_chat.core import Coordinator
from agent_chat.web import snapshot


class VersionObservationTests(unittest.TestCase):
    def test_separately_reports_running_and_installed(self):
        with mock.patch('agent_chat.bridge_client.subprocess.run', return_value=mock.Mock(stdout='codex-cli 0.170.0\n')) as run:
            self.assertEqual(version_observation({'userAgent': 'codex_app_server/0.169.0 (macOS)'}, '/fixture/codex'), ('0.169.0', '0.170.0'))
        self.assertEqual(run.call_args.args[0], ['/fixture/codex', '--version'])
        self.assertEqual(run.call_args.kwargs['timeout'], 3)

    def test_unknowns_do_not_claim_versions(self):
        with mock.patch('agent_chat.bridge_client.subprocess.run', side_effect=subprocess.TimeoutExpired('codex', 3)):
            self.assertEqual(version_observation({}, '/fixture/codex'), (None, None))
        with mock.patch('agent_chat.bridge_client.subprocess.run', side_effect=UnicodeDecodeError('utf-8', b'\xff', 0, 1, 'bad')):
            self.assertEqual(version_observation({'userAgent': 'codex/1.2.3 ('}, '/fixture/codex'), ('1.2.3', None))
        long_version = '1.2.3-' + 'a' * 64
        with mock.patch('agent_chat.bridge_client.subprocess.run', return_value=mock.Mock(stdout='codex-cli ' + long_version)):
            self.assertEqual(version_observation({'userAgent': 'codex/' + long_version + ' ('}, '/fixture/codex'), (None, None))

    def test_sqlite_snapshot_keeps_and_clears_versions(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'state.sqlite3'
            coord = Coordinator(path, 'root')
            self.addCleanup(coord.close)
            coord.register('root')
            state = BridgeState(coord)
            state.heartbeat('ws://127.0.0.1:1', 123, host_id='host', versions=('0.1.0', '0.2.0'))
            bridge = snapshot(path)['bridge']
            self.assertEqual((bridge['running_version'], bridge['installed_version']), ('0.1.0', '0.2.0'))
            state.heartbeat('ws://127.0.0.1:1', 123, host_id='host')
            self.assertIsNone(snapshot(path)['bridge']['running_version'])
