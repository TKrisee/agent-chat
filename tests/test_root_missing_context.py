import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SOURCE = Path(__file__).resolve().parents[1] / 'src'

class MissingContext(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.env = {'PATH': '/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin',
                    'HOME': str(root), 'CODEX_HOME': str(root/'.codex'), 'TMPDIR': str(root),
                    'PYTHONPATH': str(SOURCE), 'PYTHONDONTWRITEBYTECODE': '1'}

    def invoke(self, *args):
        p = subprocess.run([sys.executable, '-m', 'agent_chat.cli', '--server',
                            'http://127.0.0.1:1', *args], env=self.env, text=True,
                           capture_output=True, timeout=10)
        q = subprocess.run(['jq', '-er', '.error | strings'], input=p.stderr,
                           text=True, capture_output=True, check=True)
        return p.returncode, q.stdout.strip()

    def test_missing_context_explains_supported_discovery_without_assuming_deletion(self):
        code, message = self.invoke('policy')
        self.assertEqual(code, 2)
        self.assertIn('AGENT_CHAT_SESSION or --session', message)
        self.assertIn('agent-chat-client root-local discover', message)
        self.assertIn('does not establish that the registration was deleted', message)
        self.assertNotIn('attached', message.lower())

    def test_explicit_session_proceeds_to_transport_without_discovery_claim(self):
        code, message = self.invoke('--session', 'session_fixture', 'policy')
        self.assertEqual(code, 2)
        self.assertNotIn('root-local discover', message)
        self.assertNotIn('AGENT_CHAT_SESSION or --session is required', message)

    def test_root_local_help_remains_available_without_session(self):
        p = subprocess.run([sys.executable, '-m', 'agent_chat.cli', 'root-local', '--help'],
                           env=self.env, text=True, capture_output=True, timeout=10)
        self.assertEqual(p.returncode, 0)
        self.assertIn('discover', p.stdout)
        self.assertNotIn('registration was deleted', p.stderr)
