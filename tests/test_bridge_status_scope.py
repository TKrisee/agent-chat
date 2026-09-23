import os
import subprocess
import sys
import tempfile
import unittest
import uuid
from pathlib import Path

from agent_chat.bridge_state import BridgeState
from agent_chat.core import Coordinator
from test_remote_web import RemoteWebFixture, TOKEN


class BridgeStatusScopeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='agent-chat-bridge-status-')
        self.addCleanup(self.tmp.cleanup)
        self.db = Path(self.tmp.name) / 'state.sqlite3'
        self.root = self.coord('root')
        self.child = self.coord('child')
        self.peer = self.coord('peer')
        self.unbound = self.coord('unbound')
        self.root_state = BridgeState(self.root)
        self.child_state = BridgeState(self.child)
        self.peer_state = BridgeState(self.peer)
        self.root_thread = str(uuid.uuid4())
        self.peer_thread = str(uuid.uuid4())
        self.root_state.bind(thread_id=self.root_thread)
        self.child_state.bind(parent_session='root', agent_path='/root/child')
        self.peer_state.bind(thread_id=self.peer_thread)
        self.root_state.observe(self.root_thread, 'idle')
        self.peer_state.observe(self.peer_thread, 'active')

    def coord(self, session):
        coord = Coordinator(self.db, session)
        coord.register(session)
        self.addCleanup(coord.close)
        return coord

    def add_job(self, state, thread_id, index, status='prepared'):
        state.db.execute('INSERT INTO bridge_jobs VALUES(?,?,?,?,?,?,?,?)',
                         ('job-' + str(index), thread_id, '{}', status, None, None, index, index))

    def test_root_and_child_status_expose_only_their_route(self):
        self.add_job(self.root_state, self.root_thread, 1)
        self.add_job(self.root_state, self.root_thread, 3, status='dispatched')
        self.add_job(self.root_state, self.root_thread, 4, status='cancelled')
        self.add_job(self.peer_state, self.peer_thread, 2)

        root = self.root_state.status(mine=True)
        child = self.child_state.status(mine=True)

        self.assertEqual([row['session_id'] for row in root['bindings']], ['root'])
        self.assertEqual([row['session_id'] for row in child['bindings']], ['child'])
        self.assertEqual([row['thread_id'] for row in root['threads']], [self.root_thread])
        self.assertEqual([row['thread_id'] for row in child['threads']], [self.root_thread])
        self.assertEqual([row['id'] for row in root['jobs']], ['job-1'])
        self.assertEqual([row['id'] for row in child['jobs']], ['job-1'])
        self.assertFalse(root['jobs_has_more'])
        self.assertFalse(child['jobs_has_more'])

    def test_unbound_own_status_is_empty_and_global_status_is_unchanged(self):
        self.add_job(self.root_state, self.root_thread, 1)
        self.add_job(self.peer_state, self.peer_thread, 2, status='dispatched')

        own = BridgeState(self.unbound).status(mine=True)
        legacy = self.root_state.status()

        self.assertEqual(own['bindings'], [])
        self.assertEqual(own['threads'], [])
        self.assertEqual(own['jobs'], [])
        self.assertFalse(own['jobs_has_more'])
        self.assertEqual({row['id'] for row in legacy['jobs']}, {'job-1', 'job-2'})
        self.assertNotIn('jobs_has_more', legacy)

    def test_own_status_limits_current_route_jobs_and_marks_truncation(self):
        for index in range(12):
            self.add_job(self.root_state, self.root_thread, index)
        self.add_job(self.peer_state, self.peer_thread, 99)

        result = self.root_state.status(mine=True)

        self.assertEqual(len(result['jobs']), 10)
        self.assertTrue(result['jobs_has_more'])
        self.assertEqual({row['thread_id'] for row in result['jobs']}, {self.root_thread})
        self.assertEqual([row['id'] for row in result['jobs']],
                         ['job-11', 'job-10', 'job-9', 'job-8', 'job-7', 'job-6', 'job-5', 'job-4', 'job-3', 'job-2'])


class BridgeStatusRemoteValidationTests(RemoteWebFixture):
    def test_invalid_mine_selector_is_rejected_and_absent_selector_is_legacy_global(self):
        self.call('register', agent='alpha')
        self.call('register', session='b', agent='beta')
        self.call('bind', thread=str(uuid.uuid4()))
        self.call('bind', session='b', thread=str(uuid.uuid4()))

        legacy = self.call('bridge-status')
        self.assertEqual({row['session_id'] for row in legacy['bindings']}, {'a', 'b'})
        status, raw, _ = self.request('POST', '/api/coord', {
            'op': 'bridge-status', 'session': 'a', 'host_id': 'mac',
            'params': {'mine': 'true'},
        })
        self.assertEqual(status, 400)
        self.assert_json(raw.decode(), '.error == "mine must be a boolean"')

    def assert_json(self, raw, expression):
        result = subprocess.run(['jq', '-e', expression], input=raw, text=True,
                                capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr or result.stdout)


class BridgeStatusCliTests(RemoteWebFixture):
    def command(self, *args, ok=True):
        env = {key: value for key, value in os.environ.items() if not key.startswith('AGENT_CHAT_')}
        env.update(AGENT_CHAT_SERVER=self.url, AGENT_CHAT_API_TOKEN=TOKEN,
                   AGENT_CHAT_HOST_ID='mac', AGENT_CHAT_STATE_DIR=self.tmp.name)
        client = Path(__file__).resolve().parents[1] / 'bin' / 'agent-chat-client'
        result = subprocess.run([sys.executable, str(client), *args], env=env,
                                capture_output=True, text=True, timeout=10)
        if ok:
            self.assertEqual(result.returncode, 0, result.stderr)
            return result.stdout
        self.assertNotEqual(result.returncode, 0)
        return result.stderr

    def assert_json(self, raw, expression):
        result = subprocess.run(['jq', '-e', expression], input=raw, text=True,
                                capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr or result.stdout)

    def add_job(self, thread_id, job_id, status):
        coord = Coordinator(self.db, 'a')
        self.addCleanup(coord.close)
        with coord.tx():
            coord.db.execute('INSERT INTO bridge_jobs VALUES(?,?,?,?,?,?,?,?)',
                             (job_id, thread_id, '{}', status, None, None, 1, 1))

    def test_cli_defaults_to_own_route_and_all_includes_global_terminal_history(self):
        root_thread, peer_thread = str(uuid.uuid4()), str(uuid.uuid4())
        self.command('--session', 'a', 'register', '--agent', 'alpha')
        self.command('--session', 'b', 'register', '--agent', 'beta')
        self.command('--session', 'a', 'bind', '--thread', root_thread)
        self.command('--session', 'b', 'bind', '--thread', peer_thread)
        self.add_job(root_thread, 'job-a', 'prepared')
        self.add_job(root_thread, 'job-terminal', 'dispatched')
        self.add_job(peer_thread, 'job-b', 'prepared')

        self.assert_json(self.command('--session', 'a', 'bridge-status'),
                         '([.bindings[].session_id] == ["a"]) and ([.jobs[].id] == ["job-a"]) and .jobs_has_more == false')
        self.assert_json(self.command('--session', 'a', 'bridge-status', '--mine'),
                         '([.bindings[].session_id] == ["a"]) and ([.jobs[].id] == ["job-a"])')
        self.assert_json(self.command('--session', 'a', 'bridge-status', '--all'),
                         '([.bindings[].session_id] | sort == ["a", "b"]) and ([.jobs[].id] | sort == ["job-a", "job-b", "job-terminal"]) and (has("jobs_has_more") | not)')
        self.assertIn('not allowed with argument', self.command(
            '--session', 'a', 'bridge-status', '--all', '--mine', ok=False))
