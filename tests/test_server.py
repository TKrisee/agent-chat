import json
import os
from pathlib import Path
import selectors
import signal
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest import mock
import urllib.request

from agent_chat.core import Coordinator
from agent_chat import server
from agent_chat.rpc import TransportError


class ServerTests(unittest.TestCase):
    def test_combined_server_serves_http_and_shuts_down_when_codex_offline(self):
        with tempfile.TemporaryDirectory(prefix='agent-chat-server-') as directory:
            db = str(Path(directory) / 'state.sqlite3')
            env = dict(os.environ)
            env.pop('AGENT_CHAT_API_TOKEN', None)
            proc = subprocess.Popen([sys.executable, '-m', 'agent_chat.server', '--db', db, '--port', '0', '--codex-server', 'ws://127.0.0.1:1'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
            try:
                with selectors.DefaultSelector() as selector:
                    selector.register(proc.stdout, selectors.EVENT_READ)
                    self.assertTrue(selector.select(8), 'server did not print its URL')
                url = proc.stdout.readline().strip()
                self.assertTrue(url.startswith('http://127.0.0.1:'), url)
                with urllib.request.urlopen(url + 'api/config', timeout=4) as response:
                    self.assertIn('sender', json.load(response))
                proc.send_signal(signal.SIGTERM)
                self.assertEqual(proc.wait(timeout=15), 0)
            finally:
                if proc.poll() is None: proc.kill(); proc.wait(5)
                proc.stdout.close(); proc.stderr.close()

    def test_bridge_reconnects_same_client_after_initial_connection_failure(self):
        with tempfile.TemporaryDirectory(prefix='agent-chat-reconnect-') as directory:
            db = Path(directory) / 'state.sqlite3'
            c = Coordinator(db, 'human'); c.register('operator'); c.close()
            Path(str(db) + '.web-session.json').write_text(json.dumps({'id':'human'}))
            stop = threading.Event()
            instances = []
            class FakeRpc:
                def __init__(self, endpoint): self.attempts = 0; instances.append(self)
                def connect(self):
                    self.attempts += 1
                    if self.attempts == 1: raise TransportError('offline first')
                    stop.set()
                def close(self): pass
            with mock.patch.object(server, 'RpcClient', FakeRpc):
                server.run_bridge(db, 'ws://127.0.0.1:1', .01, stop)
            self.assertEqual(len(instances), 1)
            self.assertEqual(instances[0].attempts, 2)
