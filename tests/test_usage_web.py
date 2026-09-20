import contextlib
import pathlib
import json
import sqlite3
import sys
import tempfile
import types
import unittest
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from agent_chat.core import CoordError, Coordinator
from agent_chat.bridge_client import RemoteBridgeState
from agent_chat.remote import RemoteCoordError
from agent_chat.usage import UsageStore
from agent_chat.web import Handler
from test_remote_web import RemoteWebFixture


class UsageWebActionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.handler = object.__new__(Handler)
        self.handler.server = types.SimpleNamespace(usage=UsageStore(pathlib.Path(self.tmp.name) / 'state.sqlite3'))

    def tearDown(self):
        self.tmp.cleanup()

    def test_browser_cannot_report_and_machine_cannot_configure(self):
        with self.assertRaises(CoordError):
            self.handler.usage_action({'op': 'report', 'host_id': 'h', 'remaining_percent': 50, 'resets_at': None}, False)
        with self.assertRaises(CoordError):
            self.handler.usage_action({'op': 'configure', 'enabled': True, 'threshold_percent': 30}, True)

    def test_machine_report_and_enforcement_return_bare_status(self):
        result = self.handler.usage_action({'op': 'report', 'host_id': 'h', 'remaining_percent': 50, 'resets_at': None}, True)
        self.assertIn('hosts', result)
        result = self.handler.usage_action({'op': 'enforcement', 'host_id': 'h', 'stopped_threads': 1, 'enforcement_error': None}, True)
        self.assertEqual(result['hosts'][0]['stopped_threads'], 1)


class UsageWebIntegrationTests(RemoteWebFixture):
    def usage(self, method='GET', path='/api/usage', payload=None, auth='basic', headers=None):
        status, body, response_headers = self.request(method, path, payload, auth=auth, headers=headers)
        return status, (json.loads(body) if body else None), response_headers

    def csrf(self):
        return json.loads(self.request('GET', '/api/config', auth='basic')[1])['csrf_token']

    def configure(self, enabled=True, threshold=30):
        status, body, _ = self.usage('POST', payload={'op': 'configure', 'enabled': enabled,
                                                       'threshold_percent': threshold},
                                     headers={'X-Agent-Chat-CSRF': self.csrf()})
        self.assertEqual(status, 200)
        return body

    def report(self, host, remaining):
        return self.client.call('/api/usage/rpc', {'op': 'report', 'host_id': host,
                                                  'remaining_percent': remaining, 'resets_at': 500})

    def send_operator(self):
        coord = Coordinator(self.db, self.server.sender_session)
        try:
            return coord.send('a', 'Please work on this')['id']
        finally:
            coord.close()

    def test_get_auth_and_browser_csrf_machine_bearer_boundaries(self):
        self.assertEqual(self.usage(auth=None)[0], 401)
        self.assertEqual(self.usage(auth='basic')[0], 200)
        self.assertEqual(self.usage(auth='bearer')[0], 200)
        self.assertEqual(self.usage('POST', payload={'op': 'configure', 'enabled': True,
                                                      'threshold_percent': 30})[0], 403)
        self.assertEqual(self.usage('POST', '/api/usage/rpc', {'op': 'status', 'host_id': 'mac'}, auth='basic')[0], 401)
        self.assertEqual(self.usage('POST', '/api/usage/rpc', {'op': 'status', 'host_id': 'mac'}, auth='bearer')[0], 200)
        status, result, _ = self.usage('POST', payload={'op': 'configure', 'enabled': True,
                                                         'threshold_percent': 30},
                                      headers={'X-Agent-Chat-CSRF': self.csrf()})
        self.assertEqual(status, 200)
        self.assertTrue(result['enabled'])
        self.assertEqual(result['threshold_percent'], 30)

    def test_usage_is_global_across_project_snapshots(self):
        project = self.client.call('/api/projects/rpc', {'op': 'create', 'name': 'Other'})['project']['id']
        self.report('mac', 80)
        self.configure()
        default = json.loads(self.request('GET', '/api/snapshot', auth='basic')[1])['usage']
        other = json.loads(self.request('GET', '/api/snapshot?project=' + project, auth='basic')[1])['usage']
        self.assertEqual(default, other)
        self.assertTrue(default['enabled'])
        self.report('linux', 30)
        default = json.loads(self.request('GET', '/api/snapshot', auth='basic')[1])['usage']
        other = json.loads(self.request('GET', '/api/snapshot?project=' + project, auth='basic')[1])['usage']
        self.assertTrue(default['paused'])
        self.assertTrue(default['blocked'])
        self.assertEqual(default, other)

    def test_threshold_latch_blocks_bridge_without_acknowledging_messages_until_resume(self):
        self.call('register', agent='alpha')
        thread_id = str(uuid.uuid4())
        self.call('bind', thread=thread_id)
        identity = {'owner': 'test-worker', 'secret': 'x' * 40, 'host_id': 'mac'}
        state = RemoteBridgeState(self.client, self.url, identity)
        state.call('acquire')
        self.report('mac', 80)
        self.configure()
        message_id = self.send_operator()
        pending = state.pending()
        job = state.prepare(thread_id, pending, 'wake')
        state.update(job['id'], 'queued', queue_id='already-queued')
        low = self.report('mac', 30)
        later_message = self.send_operator()
        self.assertTrue(low['paused'])
        self.assertEqual(state.pending(), [])
        with self.assertRaises(RemoteCoordError):
            state.prepare(thread_id, pending, 'another wake')
        with self.assertRaises(RemoteCoordError):
            state.update(job['id'], 'adding')
        with self.assertRaises(RemoteCoordError):
            state.update(job['id'], 'starting')
        with contextlib.closing(sqlite3.connect(self.db)) as db:
            self.assertIsNone(db.execute('SELECT acked_at FROM messages WHERE id=?', (message_id,)).fetchone()[0])
        self.report('mac', 80)
        self.assertTrue(self.usage()[1]['paused'])
        status, resumed, _ = self.usage('POST', payload={'op': 'resume'}, headers={'X-Agent-Chat-CSRF': self.csrf()})
        self.assertEqual(status, 200)
        self.assertFalse(resumed['paused'])
        self.assertEqual([item['id'] for item in state.pending()], [later_message])
        self.assertEqual(state.job(job['id'])['status'], 'queued')
        state.update(job['id'], 'starting')
        self.assertEqual(state.job(job['id'])['status'], 'starting')
