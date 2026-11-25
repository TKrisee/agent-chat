import http.client
import pathlib
import tempfile
import threading
import unittest
import base64
from unittest import mock
from email.message import Message

from agent_chat.auth import RequestOrigins
from agent_chat.core import Coordinator
from agent_chat.web import create_server


class OriginBoundaryTests(unittest.TestCase):
    def test_direct_local_requires_exact_host_origin_and_no_forwarding(self):
        policy = RequestOrigins('127.0.0.1', 8765, None)
        headers = Message()
        headers['Host'] = '127.0.0.1:8765'
        headers['Origin'] = 'http://127.0.0.1:8765'
        self.assertTrue(policy.direct_local(headers, '127.0.0.1'))
        self.assertFalse(policy.direct_local(headers, '192.0.2.1'))
        headers['X-Forwarded-For'] = ''
        self.assertFalse(policy.direct_local(headers, '127.0.0.1'))
        del headers['X-Forwarded-For']
        del headers['Origin']
        self.assertFalse(policy.direct_local(headers, '127.0.0.1'))
        self.assertTrue(policy.direct_local(headers, '127.0.0.1', require_origin=False))
        headers['Origin'] = 'http://evil.example'
        self.assertFalse(policy.direct_local(headers, '127.0.0.1'))
        del headers['Origin']
        headers['Sec-Fetch-Site'] = 'cross-site'
        self.assertFalse(policy.direct_local(headers, '127.0.0.1', require_origin=False))
        del headers['Host']
        headers['Host'] = 'evil.example'
        self.assertFalse(policy.direct_local(headers, '127.0.0.1'))


class DirectLocalAuthenticationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        db = pathlib.Path(self.tmp.name) / 'state.sqlite3'
        Coordinator(db).close()
        self.server = create_server(db, port=0, api_token='x' * 32)
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={'poll_interval': .01}, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.thread.join(3)
        self.server.stop_event.set()
        self.server.server_close()
        self.tmp.cleanup()

    def request(self, method, path, headers=None, body=None):
        conn = http.client.HTTPConnection('127.0.0.1', self.server.server_port, timeout=5)
        try:
            conn.request(method, path, body=body, headers=headers or {})
            response = conn.getresponse()
            data = response.read()
            return response.status, data
        finally:
            conn.close()

    def test_browser_and_api_boundaries(self):
        base = 'http://127.0.0.1:' + str(self.server.server_port)
        self.assertEqual(self.request('GET', '/')[0], 200)
        self.assertEqual(self.request('GET', '/api/config', {'Sec-Fetch-Site': 'same-origin'})[0], 200)
        self.assertEqual(self.request('GET', '/api/config')[0], 200)
        self.assertEqual(self.request('GET', '/api/config', {'Sec-Fetch-Site': 'cross-site'})[0], 401)
        self.assertEqual(self.request('GET', '/api/config', {'Origin': base})[0], 200)
        self.assertEqual(self.request('GET', '/api/config', {'Origin': 'http://evil.example'})[0], 403)
        self.assertEqual(self.request('GET', '/api/config', {'Origin': base, 'X-Forwarded-For': '127.0.0.1'})[0], 401)
        self.assertEqual(self.request('GET', '/api/config', {'Host': 'evil.example', 'Origin': base})[0], 403)
        self.assertEqual(self.request('GET', '/api/approvals', {'Origin': base})[0], 403)
        self.assertEqual(self.request('POST', '/api/root/fresh-start', {
            'Content-Type': 'application/json', 'Origin': base,
            'X-Agent-Chat-CSRF': self.server.csrf_token}, '{}')[0], 403)

    def test_public_proxy_configuration_with_stripped_forwarding_fails_closed(self):
        self.server.shutdown()
        self.thread.join(3)
        self.server.stop_event.set()
        self.server.server_close()
        db = pathlib.Path(self.tmp.name) / 'proxy.sqlite3'
        Coordinator(db).close()
        self.server = create_server(db, port=0, api_token='x' * 32,
                                    public_url='https://chat.example', trusted_proxies=['127.0.0.1'])
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={'poll_interval': .01}, daemon=True)
        self.thread.start()
        self.assertFalse(self.server.direct_local_auth)
        self.assertEqual(self.request('GET', '/api/config')[0], 401)

    def test_root_admin_routes_require_operator_channel_and_check_owner(self):
        base = 'http://127.0.0.1:' + str(self.server.server_port)
        headers = {'Content-Type': 'application/json', 'Origin': base,
                   'X-Agent-Chat-CSRF': self.server.csrf_token}
        payload = '{"project":"default","agent":"test","request_id":"r","confirm":true,"model":null,"effort":null}'
        self.server.roots = mock.Mock()
        self.server.roots.config.return_value = {'home_project': 'default', 'home_session': 'root', 'host_id': 'host'}
        self.server.measurements.status = mock.Mock(return_value={'active': False})
        self.assertEqual(self.request('POST', '/api/root/provision', headers, payload)[0], 403)
        self.server.roots.authorize.assert_not_called()
        self.assertEqual(self.request('POST', '/api/root/workspaces', headers, '{}')[0], 403)
        self.assertEqual(self.request('POST', '/api/sessions/reset', headers, '{}')[0], 403)
        self.assertEqual(self.request('POST', '/api/sessions/remove', headers, '{}')[0], 403)
        headers['Authorization'] = 'Basic ' + base64.b64encode(('operator:' + 'x' * 32).encode()).decode()
        self.server.roots.authorize.side_effect = ValueError('ownership denied')
        self.assertEqual(self.request('POST', '/api/root/provision', headers, payload)[0], 400)
        self.server.roots.authorize.assert_called_once_with('default', 'root', 'host')
        self.server.roots.provision.assert_not_called()

    def test_bridge_reset_requires_original_bearer(self):
        headers = {'Content-Type': 'application/json'}
        self.server.bridge_manager.dispatch = mock.Mock(return_value={'released': True})
        payload = '{"op":"reset","owner":"test","secret":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx","host_id":"host","params":{"confirm_stopped":true}}'
        self.assertEqual(self.request('POST', '/api/bridge/rpc', headers, payload)[0], 403)
        self.server.bridge_manager.dispatch.assert_not_called()
        headers['Authorization'] = 'Bearer ' + 'x' * 32
        self.assertEqual(self.request('POST', '/api/bridge/rpc', headers, payload)[0], 200)
        self.server.bridge_manager.dispatch.assert_called_once()

    def test_tokenless_machine_cannot_bind_or_ack_operator(self):
        session = self.server.sender_session
        headers = {'Content-Type': 'application/json'}
        payload = ('{"op":"ack","session":"' + session + '","host_id":"untrusted-host",'
                   '"params":{"id":"missing"}}')
        with mock.patch('agent_chat.remote_service.dispatch') as dispatch:
            self.assertEqual(self.request('POST', '/api/coord', headers, payload)[0], 403)
            dispatch.assert_not_called()
        coord = Coordinator(self.server.db_path)
        try:
            if coord.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='remote_session_hosts'").fetchone():
                self.assertIsNone(coord.db.execute(
                    'SELECT 1 FROM remote_session_hosts WHERE session_id=?', (session,)).fetchone())
        finally:
            coord.close()

    def test_metadata_only_coordinator_reads_use_selected_project(self):
        project = self.server.projects.create('Metadata test')['id']
        _, operator, _ = self.server.project_context(project)
        headers = {'Content-Type': 'application/json', 'X-Agent-Chat-Project': project}
        cases = (
            ('attachment-download', 'managed_operator_attachment', '{"source_project":"other"}'),
            ('first-ack-lookup', 'sender_owned_first_ack_lookup', '{}'),
            ('source-trace', 'source_trace_read', '{}'),
        )
        for operation, function, params in cases:
            with self.subTest(operation=operation), mock.patch('agent_chat.remote_service.' + function, return_value={}):
                def payload(session):
                    return ('{"op":"' + operation + '","session":"' + session +
                            '","host_id":"host","params":' + params + '}')
                self.assertEqual(self.request('POST', '/api/coord', headers, payload(operator))[0], 403)
                self.assertEqual(self.request('POST', '/api/coord', headers, payload('ordinary-session'))[0], 200)


if __name__ == '__main__':
    unittest.main()
