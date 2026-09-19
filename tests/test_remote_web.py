import base64
import http.client
import json
import tempfile
import threading
import unittest
from pathlib import Path

from agent_chat.auth import origin
from agent_chat.core import Coordinator
from agent_chat.remote import HttpClient, RemoteCoordError
from agent_chat.web import create_server

TOKEN = 'test-only-token-for-remote-hosting-123456789'


class RemoteWebFixture(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='agent-chat-http-')
        self.db = Path(self.tmp.name) / 'state.sqlite3'
        Coordinator(self.db).close()
        self.start()

    def start(self, **kwargs):
        self.server = create_server(self.db, port=0, api_token=TOKEN, **kwargs)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = 'http://127.0.0.1:' + str(self.server.server_port)
        self.client = HttpClient(self.url, TOKEN)

    def stop(self):
        self.server.stop_event.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(3)

    def tearDown(self):
        self.stop()
        self.tmp.cleanup()

    def request(self, method, path, payload=None, auth='bearer', headers=None):
        h = dict(headers or {})
        if auth == 'bearer': h['Authorization'] = 'Bearer ' + TOKEN
        if auth == 'basic': h['Authorization'] = 'Basic ' + base64.b64encode(('operator:' + TOKEN).encode()).decode()
        data = json.dumps(payload).encode() if payload is not None else None
        if data is not None: h['Content-Type'] = 'application/json'
        conn = http.client.HTTPConnection('127.0.0.1', self.server.server_port, timeout=3)
        try:
            conn.request(method, path, data, h)
            response = conn.getresponse()
            return response.status, response.read(), dict(response.getheaders())
        finally: conn.close()

    def call(self, op, session='a', host='mac', **params):
        return self.client.call('/api/coord', {'op': op, 'session': session, 'host_id': host, 'params': params})


class RemoteWebTests(RemoteWebFixture):
    def test_all_ui_and_data_routes_require_auth(self):
        for path in ('/', '/app.js', '/api/config', '/api/snapshot', '/api/events', '/api/attachments/fake'):
            status, body, headers = self.request('GET', path, auth=None)
            self.assertEqual(status, 401, path)
            self.assertIn('Basic', headers['WWW-Authenticate'])
        self.assertEqual(self.request('GET', '/', auth='basic')[0], 200)
        status, _, headers = self.request('POST', '/api/coord', {}, auth='basic')
        self.assertEqual(status, 401)
        self.assertIn('Bearer', headers['WWW-Authenticate'])

    def test_proxy_host_and_origin_are_exact_and_default_ports_normalized(self):
        self.stop()
        self.start(host='0.0.0.0', public_url='https://chat.example.test:443')
        allowed = {'Host': 'chat.example.test', 'Origin': 'https://chat.example.test'}
        self.assertEqual(self.request('GET', '/api/config', headers=allowed)[0], 200)
        self.assertEqual(self.request('GET', '/api/config', headers={**allowed, 'Host': 'evil.test'})[0], 403)
        self.assertEqual(self.request('GET', '/api/config', headers={**allowed, 'Origin': 'https://evil.test'})[0], 403)
        self.assertEqual(origin('http://EXAMPLE.test:80/'), 'http://example.test')

    def test_nonlocal_bind_requires_token_and_public_origin(self):
        for options in ({'host': '0.0.0.0'}, {'host': '0.0.0.0', 'api_token': TOKEN}):
            with self.assertRaises(ValueError):
                create_server(self.db, port=0, **options)

    def test_remote_cli_data_reply_image_and_explicit_ack(self):
        self.call('register', agent='alpha')
        self.call('register', session='b', agent='beta')
        msg = self.call('send', to='b', body='Question')
        image = b'\x89PNG\r\n\x1a\n' + b'fixture'
        reply = self.call('send', session='b', to='a', body='Answer', reply_to=msg['id'], attachments=[{'name': 'answer.png', 'content_base64': base64.b64encode(image).decode()}])
        inbox = self.call('inbox')['messages']
        self.assertEqual(inbox[0]['reply_to'], msg['id'])
        self.assertIsNone(inbox[0]['acked_at'])
        attachment = reply['attachments'][0]
        self.assertEqual(self.request('GET', attachment['url'])[1], image)
        self.assertTrue(self.call('acknowledge', id=reply['id'])['acknowledged'])
        self.assertEqual(self.call('inbox')['messages'], [])
        with self.assertRaises(RemoteCoordError):
            self.call('status', host='another-mac')

    def test_csrf_required_for_removal_and_operator_is_preserved(self):
        self.call('register', agent='alpha')
        self.assertEqual(self.request('POST', '/api/sessions/remove', {'id': 'a'})[0], 403)
        config = json.loads(self.request('GET', '/api/config')[1])
        headers = {'X-Agent-Chat-CSRF': config['csrf_token']}
        self.assertEqual(self.request('POST', '/api/sessions/remove', {'id': config['sender']['id']}, headers=headers)[0], 400)
        self.assertEqual(self.request('POST', '/api/sessions/remove', {'id': 'a'}, headers=headers)[0], 200)
        self.assertEqual(self.request('POST', '/api/sessions/remove', [], headers=headers)[0], 400)

    def test_large_image_without_caption_and_reply_preview(self):
        self.call('register', agent='alpha')
        self.call('register', session='b', agent='beta')
        question = self.call('send', to='b', body='Screenshot please')
        image = b'\x89PNG\r\n\x1a\n' + b'fixture' * 450000
        reply = self.call('send', session='b', to='a', body='', reply_to=question['id'], attachments=[{'name':'large.png','content_base64':base64.b64encode(image).decode()}])
        self.assertEqual(reply['reply_preview']['body'], 'Screenshot please')
        self.assertEqual(self.request('GET', reply['attachments'][0]['url'])[1], image)

    def test_invalid_machine_request_does_not_break_server_or_expose_files(self):
        for payload in ({}, [], {'op':'execute', 'session':'a', 'host_id':'mac','params':{}}):
            self.assertEqual(self.request('POST', '/api/coord', payload)[0], 400)
        self.assertEqual(self.request('GET', '/api/config')[0], 200)
        for endpoint in ('http://remote.example:8765', 'https://user:pass@example.test', 'https://example.test?q=token'):
            with self.assertRaises(RemoteCoordError): HttpClient(endpoint, TOKEN)
