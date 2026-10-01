import http.client
import json
import os
import pathlib
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
import uuid


ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from agent_chat.core import Coordinator
from agent_chat.web import create_server, operator_session, page, snapshot


class WebApiTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="agent-chat-web-test-")
        self.db = str(pathlib.Path(self.tmp.name) / "state.sqlite3")
        self.alpha = self.register("alpha")
        self.beta = self.register("beta")
        self.gamma = self.register("gamma")
        self.send(self.alpha, self.beta, "first")
        self.send(self.alpha, self.beta, "second")
        coordinator = self.coordinator(self.alpha)
        try:
            claim = coordinator.request("web-test-resource", 5)
        finally:
            coordinator.close()
        self.assertEqual(claim["state"], "owned")
        coordinator = self.coordinator(self.beta)
        try:
            coordinator.inbox()
            queued = coordinator.request("web-test-resource", 5)
        finally:
            coordinator.close()
        self.assertEqual(queued["state"], "queued")
        self.old_session = os.environ.get("AGENT_CHAT_SESSION")
        os.environ["AGENT_CHAT_SESSION"] = "parent-session-must-not-be-used"
        try:
            self.server = create_server(self.db, port=0)
        except OSError as error:
            self.tmp.cleanup()
            self.restore_environment()
            self.fail("loopback socket binding is required: " + str(error))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.port = self.server.server_port

    def tearDown(self):
        if hasattr(self, "server"):
            self.server.stop_event.set()
            self.server.shutdown()
            self.server.server_close()
            self.thread.join(3)
        self.restore_environment()
        self.tmp.cleanup()

    def restore_environment(self):
        if self.old_session is None:
            os.environ.pop("AGENT_CHAT_SESSION", None)
        else:
            os.environ["AGENT_CHAT_SESSION"] = self.old_session

    def coordinator(self, session):
        return Coordinator(self.db, session)

    def register(self, agent):
        coordinator = Coordinator(self.db, "web-test-" + agent + "-" + uuid.uuid4().hex)
        try:
            return coordinator.register(agent)["session"]
        finally:
            coordinator.close()

    def send(self, sender, recipient, body):
        coordinator = self.coordinator(sender)
        try:
            return coordinator.send(recipient, body)
        finally:
            coordinator.close()

    def value(self, sql, params=()):
        connection = sqlite3.connect(self.db)
        try:
            return connection.execute(sql, params).fetchone()[0]
        finally:
            connection.close()

    def request(self, method, path, body=None, headers=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=6)
        request_headers = {"Host": "127.0.0.1:" + str(self.port)}
        request_headers.update(headers or {})
        connection.request(method, path, body=body, headers=request_headers)
        response = connection.getresponse()
        result = response.status, dict(response.getheaders()), response.read()
        connection.close()
        return result

    def config(self):
        status, _, raw = self.request("GET", "/api/config")
        self.assertEqual(status, 200)
        return json.loads(raw)

    def post(self, data, headers=None):
        config = self.config()
        post_headers = {
            "Content-Type": "application/json",
            "Origin": "http://127.0.0.1:" + str(self.port),
            "X-Agent-Chat-CSRF": config["csrf_token"],
        }
        post_headers.update(headers or {})
        body = data if isinstance(data, (bytes, str)) else json.dumps(data)
        return self.request("POST", "/api/messages", body, post_headers)

    def post_multipart(self, message, images=(), headers=None, extra_fields=()):
        boundary = 'agent-chat-' + uuid.uuid4().hex
        chunks = []
        def field(headers, value):
            chunks.extend((b'--' + boundary.encode(), headers, b'', value))
        field(b'Content-Disposition: form-data; name="message"', json.dumps(message).encode())
        for name, content in images:
            field(('Content-Disposition: form-data; name="images"; filename="%s"' % name).encode(), content)
        for name, content in extra_fields:
            field(('Content-Disposition: form-data; name="%s"' % name).encode(), content)
        chunks.extend((b'--' + boundary.encode() + b'--', b''))
        config = self.config()
        post_headers = {
            'Content-Type': 'multipart/form-data; boundary=' + boundary,
            'Origin': 'http://127.0.0.1:' + str(self.port),
            'X-Agent-Chat-CSRF': config['csrf_token'],
        }
        post_headers.update(headers or {})
        return self.request('POST', '/api/messages', b'\r\n'.join(chunks), post_headers)

    def read_event(self, response):
        while True:
            line = response.fp.readline().decode().strip()
            if line.startswith(":") or not line:
                continue
            self.assertEqual(line, "event: snapshot")
            data = response.fp.readline().decode().strip()
            self.assertEqual(response.fp.readline().decode().strip(), "")
            return json.loads(data.removeprefix("data: "))

    def test_snapshot_resource_queue_hides_tokens_and_get_is_read_only(self):
        before_read = self.value("SELECT inbox_read_seq FROM sessions WHERE id=?", (self.beta,))
        before_stale = self.value("SELECT stale FROM resources WHERE name='web-test-resource'")
        status, _, raw = self.request("GET", "/api/snapshot")
        snapshot = json.loads(raw)
        self.assertEqual(status, 200)
        self.assertEqual([item["body"] for item in snapshot["messages"]], ["first", "second"])
        resource = next(item for item in snapshot["resources"] if item["resource"] == "web-test-resource")
        self.assertEqual(resource["owner_session"], self.alpha)
        self.assertEqual(resource["queue"][0]["session"], self.beta)
        self.assertNotIn("token", raw.decode())
        self.assertEqual(self.value("SELECT inbox_read_seq FROM sessions WHERE id=?", (self.beta,)), before_read)
        self.assertEqual(self.value("SELECT stale FROM resources WHERE name='web-test-resource'"), before_stale)

    def test_pagination_boundaries_and_expired_resource_read(self):
        self.send(self.alpha, self.beta, "third")
        self.send(self.alpha, self.beta, "fourth")
        status, _, raw = self.request("GET", "/api/messages?limit=2")
        page = json.loads(raw)
        self.assertEqual(status, 200)
        self.assertEqual([item["body"] for item in page["messages"]], ["third", "fourth"])
        self.assertTrue(page["has_more"])
        before = page["messages"][0]["seq"]
        status, _, raw = self.request("GET", "/api/messages?before=%s&limit=2" % before)
        self.assertEqual(status, 200)
        self.assertEqual([item["body"] for item in json.loads(raw)["messages"]], ["first", "second"])
        self.assertFalse(json.loads(raw)["has_more"])
        first = json.loads(raw)['messages'][0]['seq']
        status, _, raw = self.request("GET", "/api/messages?after=%s&limit=2&project=default" % first)
        forward = json.loads(raw)
        self.assertEqual(status, 200)
        self.assertEqual([item['body'] for item in forward['messages']], ['second', 'third'])
        self.assertEqual([item['seq'] for item in forward['messages']], sorted(item['seq'] for item in forward['messages']))
        self.assertTrue(forward['has_more'])
        status, _, raw = self.request("GET", "/api/messages?after=%s&limit=2&project=default" % forward['messages'][-1]['seq'])
        next_page = json.loads(raw)
        self.assertEqual(status, 200)
        self.assertEqual([item['body'] for item in next_page['messages']], ['fourth'])
        self.assertFalse(next_page['has_more'])
        self.assertFalse({item['id'] for item in forward['messages']} & {item['id'] for item in next_page['messages']})
        self.assertEqual(self.request("GET", "/api/messages?limit=0")[0], 400)
        self.assertEqual(self.request("GET", "/api/messages?limit=201")[0], 400)
        connection = sqlite3.connect(self.db)
        connection.execute("UPDATE resources SET deadline=?, stale=0 WHERE name='web-test-resource'", (time.time() - 1,))
        connection.commit()
        connection.close()
        self.assertEqual(self.request("GET", "/api/snapshot")[0], 200)

    def test_pagination_rejects_invalid_or_ambiguous_cursors_without_side_effects(self):
        first = self.value('SELECT MIN(seq) FROM messages')
        before_read = self.value('SELECT inbox_read_seq FROM sessions WHERE id=?', (self.beta,))
        messages = self.value('SELECT COUNT(*) FROM messages')
        resources = self.value('SELECT COUNT(*) FROM resources')
        for query in (
            'before=%s&after=%s' % (first, first),
            'before=%s&before=%s' % (first, first),
            'after=%s&after=%s' % (first, first),
            'after=0',
            'after=',
            'after=not-a-sequence',
            'after=9223372036854775808',
            'before=-1',
        ):
            self.assertEqual(self.request('GET', '/api/messages?' + query)[0], 400)
        self.assertEqual(self.value('SELECT inbox_read_seq FROM sessions WHERE id=?', (self.beta,)), before_read)
        self.assertEqual(self.value('SELECT COUNT(*) FROM messages'), messages)
        self.assertEqual(self.value('SELECT COUNT(*) FROM resources'), resources)

    def test_direct_conversation_pages_skip_groups_and_unrelated_history(self):
        operator = operator_session(self.db)
        self.send(self.alpha, operator, 'old direct to operator')
        self.send(operator, self.alpha, 'old direct to alpha')
        coordinator = self.coordinator(self.alpha)
        try:
            coordinator.send_group_prepared(None, 'quiet group', [])
            coordinator.send_many([operator, self.beta], 'addressed group')
        finally:
            coordinator.close()
        for index in range(60):
            self.send(self.beta, self.gamma, 'unrelated %s' % index)
        self.assertFalse(any(item['body'] == 'old direct to operator' for item in snapshot(self.db)['messages']))
        incoming = page(self.db, limit=1, operator=operator, to_me=True)
        self.assertEqual([item['body'] for item in incoming['messages']], ['old direct to operator'])
        self.assertFalse(incoming['has_more'])
        conversation = page(self.db, limit=1, operator=operator, agent=self.alpha)
        self.assertEqual([item['body'] for item in conversation['messages']], ['old direct to alpha'])
        self.assertTrue(conversation['has_more'])
        older = page(self.db, before=conversation['messages'][0]['seq'], limit=1,
                     operator=operator, agent=self.alpha)
        self.assertEqual([item['body'] for item in older['messages']], ['old direct to operator'])
        self.assertFalse(older['has_more'])
        only_incoming = page(self.db, operator=operator, agent=self.alpha, to_me=True)
        self.assertEqual([item['body'] for item in only_incoming['messages']], ['old direct to operator'])

    def test_default_snapshot_events_and_history_load_only_fifty_messages(self):
        coordinator = self.coordinator(self.alpha)
        try:
            for index in range(80):
                coordinator.send(self.beta, 'history %s' % index)
        finally:
            coordinator.close()
        status, _, raw = self.request('GET', '/api/snapshot')
        latest = json.loads(raw)
        self.assertEqual(status, 200)
        self.assertEqual(len(latest['messages']), 50)
        self.assertEqual(latest['total_messages'], 82)
        self.assertTrue(latest['history_truncated'])
        self.assertEqual(latest['messages'][0]['body'], 'history 30')
        connection = http.client.HTTPConnection('127.0.0.1', self.port, timeout=6)
        try:
            connection.request('GET', '/api/events')
            response = connection.getresponse()
            self.assertEqual(len(self.read_event(response)['messages']), 50)
            response.close()
        finally:
            connection.close()
        status, _, raw = self.request('GET', '/api/messages')
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(raw)['messages'], latest['messages'])
        before = latest['messages'][0]['seq']
        status, _, raw = self.request('GET', '/api/messages?before=%s' % before)
        older = json.loads(raw)
        self.assertEqual(status, 200)
        self.assertEqual(len(older['messages']), 32)
        self.assertFalse(older['has_more'])
        self.assertLess(older['messages'][-1]['seq'], before)
        self.assertEqual(self.value('SELECT COUNT(*) FROM messages'), 82)
        self.assertEqual(self.value("SELECT stale FROM resources WHERE name='web-test-resource'"), 0)

    def test_sse_initial_message_and_ack_updates(self):
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=8)
        connection.request("GET", "/api/events", headers={"Host": "127.0.0.1:" + str(self.port)})
        response = connection.getresponse()
        self.assertEqual(response.status, 200)
        self.read_event(response)
        message = self.send(self.alpha, self.beta, "streamed")
        event = self.read_event(response)
        self.assertIsNone(next(item for item in event["messages"] if item["id"] == message["id"])["acked_at"])
        coordinator = self.coordinator(self.beta)
        try:
            coordinator.acknowledge(message["id"])
        finally:
            coordinator.close()
        event = self.read_event(response)
        self.assertIsNotNone(next(item for item in event["messages"] if item["id"] == message["id"])["acked_at"])
        connection.close()

    def test_static_allowlist_mime_csp_and_host_origin_rules(self):
        for path, mime in (("/", "text/html; charset=utf-8"), ("/app.js", "application/javascript; charset=utf-8"), ("/markdown.js", "application/javascript; charset=utf-8"), ("/style.css", "text/css; charset=utf-8")):
            status, headers, payload = self.request("GET", path)
            self.assertEqual(status, 200)
            self.assertEqual(headers["Content-Type"], mime)
            self.assertIn("default-src 'self'", headers["Content-Security-Policy"])
            self.assertTrue(payload)
        self.assertEqual(self.request("GET", "/../agent_chat.core.py")[0], 404)
        self.assertEqual(self.request("GET", "/web_assets/index.html")[0], 404)
        self.assertEqual(self.request("GET", "/api/snapshot", headers={"Host": "localhost:%s" % self.port})[0], 403)
        self.assertEqual(self.request("GET", "/api/snapshot", headers={"Origin": "http://evil.example"})[0], 403)
        self.assertEqual(self.request("GET", "/api/snapshot")[0], 200)

    def test_attachment_metadata_and_bytes_are_available_without_blob_leaks(self):
        image = pathlib.Path(self.tmp.name) / 'proof.png'
        payload = b'\x89PNG\r\n\x1a\nweb-proof'
        image.write_bytes(payload)
        coordinator = self.coordinator(self.alpha)
        try:
            sent = coordinator.send(self.beta, 'with image', [image])
        finally:
            coordinator.close()
        status, _, raw = self.request('GET', '/api/snapshot')
        self.assertEqual(status, 200)
        listed = next(item for item in json.loads(raw)['messages'] if item['id'] == sent['id'])
        self.assertEqual(listed['attachments'], sent['attachments'])
        self.assertNotIn('web-proof', raw.decode())
        attachment = sent['attachments'][0]
        status, headers, body = self.request('GET', attachment['url'])
        self.assertEqual((status, headers['Content-Type'], body), (200, 'image/png', payload))
        self.assertEqual(self.request('GET', '/api/attachments/not-an-id')[0], 404)

    def test_multipart_image_only_send_preserves_sanitized_name(self):
        image = b'\x89PNG\r\n\x1a\nweb-upload'
        status, _, raw = self.post_multipart({'to': self.beta, 'body': ''}, [('../proof.png', image)])
        self.assertEqual(status, 200)
        sent = json.loads(raw)
        self.assertEqual((sent['recipient_session'], sent['attachments'][0]['name']), (self.beta, 'proof.png'))
        self.assertEqual(self.value('SELECT body FROM messages WHERE id=?', (sent['id'],)), '')
        status, _, stored = self.request('GET', sent['attachments'][0]['url'])
        self.assertEqual((status, stored), (200, image))

    def test_multipart_group_reply_attaches_each_delivery_atomically(self):
        status, _, raw = self.post({'to': [self.beta, self.gamma], 'body': 'group parent'})
        self.assertEqual(status, 200)
        parent = json.loads(raw)['messages']
        status, _, raw = self.post_multipart(
            {'to': [self.gamma, self.beta], 'body': '', 'reply_to': parent[0]['id']},
            [('group.gif', b'GIF89agroup-upload')],
        )
        self.assertEqual(status, 200)
        sent = json.loads(raw)['messages']
        parent_for = {item['recipient_session']: item['id'] for item in parent}
        self.assertEqual({item['recipient_session']: item['reply_to'] for item in sent}, parent_for)
        self.assertEqual({item['recipient_session'] for item in sent}, {self.beta, self.gamma})
        self.assertTrue(all(item['attachments'][0]['mime'] == 'image/gif' for item in sent))
        self.assertEqual(self.value('SELECT COUNT(*) FROM attachments'), 2)

    def test_multipart_documents_download_without_rendering_and_preserve_exact_bytes(self):
        uploads = [
            ('notes.txt', 'Notes: árvíz\r\n'.encode()),
            ('guide.md', b'# Guide\n<script>alert(1)</script>'),
            ('data.json', b'{"value": true}\n'),
            ('document.xml', b'<?xml version="1.0"?><!DOCTYPE x [<!ENTITY file SYSTEM "file:///etc/passwd">]><x>&file;</x>'),
        ]
        status, _, raw = self.post_multipart({'to': self.beta, 'body': ''}, uploads)
        self.assertEqual(status, 200, raw)
        with sqlite3.connect(self.db) as db:
            rows = db.execute('SELECT id,name,mime,content FROM attachments ORDER BY rowid').fetchall()
        self.assertEqual([(row[1], row[3]) for row in rows], uploads)
        self.assertEqual([row[2] for row in rows], ['text/plain', 'text/markdown', 'application/json', 'application/xml'])
        for attachment_id, name, mime, content in rows:
            status, headers, downloaded = self.request('GET', '/api/attachments/' + attachment_id)
            self.assertEqual(status, 200)
            self.assertEqual(downloaded, content)
            self.assertEqual(headers['Content-Type'], mime)
            self.assertEqual(headers['Content-Disposition'], "attachment; filename*=UTF-8''" + name)
            self.assertEqual(headers['X-Content-Type-Options'], 'nosniff')
            self.assertEqual(headers['Content-Security-Policy'], "default-src 'none'; sandbox")

    def test_fifty_multipart_files_and_batch_above_old_body_limit(self):
        files = [(f'file-{i}.txt', b'valid text') for i in range(50)]
        self.assertEqual(self.post_multipart({'to': self.beta, 'body': ''}, files)[0], 200)
        self.assertEqual(self.value('SELECT COUNT(*) FROM attachments'), 50)
        image = b'\x89PNG\r\n\x1a\n' + b'x' * (9 * 1024 * 1024 - 8)
        self.assertEqual(self.post_multipart({'to': self.beta, 'body': ''},
                         [(f'large-{i}.png', image) for i in range(5)])[0], 200)
        self.assertEqual(self.value('SELECT COUNT(*) FROM attachments'), 55)

    def test_video_upload_download_and_byte_ranges(self):
        content = (ROOT / 'tests/fixtures/attachment.mp4').read_bytes()
        self.assertEqual(self.post_multipart({'to': self.beta, 'body': ''}, [('clip.mp4', content)])[0], 200)
        url = '/api/attachments/' + self.value('SELECT id FROM attachments')
        status, headers, raw = self.request('GET', url)
        self.assertEqual((status, raw), (200, content))
        self.assertEqual(headers['Content-Type'], 'video/mp4')
        self.assertEqual(headers['Accept-Ranges'], 'bytes')
        self.assertNotIn('Content-Disposition', headers)
        for requested, start, end in [('bytes=0-1', 0, 1), ('bytes=10-', 10, len(content)-1),
                                       ('bytes=-20', len(content)-20, len(content)-1),
                                       ('bytes=0-999999', 0, len(content)-1)]:
            status, headers, raw = self.request('GET', url, headers={'Range': requested})
            self.assertEqual((status, raw), (206, content[start:end+1]))
            self.assertEqual(headers['Content-Range'], f'bytes {start}-{end}/{len(content)}')
            self.assertEqual(int(headers['Content-Length']), len(raw))
        for requested in ('bytes=999999-', 'bytes=4-2', 'bytes=-0', 'bytes=',
                          'bytes=0-1,5-6', 'bytes=bad-9'):
            status, headers, raw = self.request('GET', url, headers={'Range': requested})
            self.assertEqual((status, raw), (416, b''))
            self.assertEqual(headers['Content-Range'], f'bytes */{len(content)}')

    def test_rejected_documents_preserve_message_and_attachment_counts(self):
        before = self.value('SELECT COUNT(*) FROM messages')
        for name, content in [('unsafe.svg', b'<svg/>'), ('script.txt', b'#!/bin/sh\necho unsafe'),
                              ('binary.json', b'hello\x00world'), ('data.xml', b'\xff')]:
            status, _, _ = self.post_multipart({'to': self.beta, 'body': ''}, [('good.txt', b'good'), (name, content)])
            self.assertEqual(status, 400, name)
        self.assertEqual(self.value('SELECT COUNT(*) FROM messages'), before)
        self.assertEqual(self.value('SELECT COUNT(*) FROM attachments'), 0)

    def test_invalid_multipart_uploads_do_not_insert_messages(self):
        before = self.value('SELECT COUNT(*) FROM messages')
        image = b'\x89PNG\r\n\x1a\nimage'
        self.assertEqual(self.post_multipart({'to': self.beta, 'body': ''}, [('a.png', image)] * 51)[0], 400)
        self.assertEqual(self.post_multipart({'to': self.beta, 'body': ''}, [('bad.exe', b'not a supported file')])[0], 400)
        self.assertEqual(self.post_multipart({'to': self.beta, 'body': ''}, [('large.png', b'\x89PNG\r\n\x1a\n' + b'x' * (10 * 1024 * 1024))])[0], 400)
        self.assertEqual(self.post_multipart({'to': self.beta, 'body': ''}, [('a.png', image)], extra_fields=[('message', b'{}')])[0], 400)
        self.assertEqual(self.post_multipart(None, [('a.png', image)],
                         extra_fields=[('message', json.dumps({'to': self.beta, 'body': ''}).encode())])[0], 400)
        self.assertEqual(self.post_multipart({'to': self.beta, 'body': ''}, [('a.png', image)], extra_fields=[('unexpected', b'x')])[0], 400)
        self.assertEqual(self.post_multipart({'to': self.beta, 'body': 'x' * (64 * 1024 + 1)}, [('a.png', image)])[0], 400)
        self.assertEqual(self.post_multipart({'to': self.beta, 'body': ''}, [('a.png', image)],
                                             {'Content-Type': 'multipart/form-data; boundary=wrong'})[0], 400)
        self.assertEqual(self.post_multipart({'to': self.beta, 'body': ''}, [('a.png', image)], {'X-Agent-Chat-CSRF': 'wrong'})[0], 403)
        self.assertEqual(self.value('SELECT COUNT(*) FROM messages'), before)

    def test_operator_persistence_parent_identity_isolation_and_valid_post(self):
        first = self.config()
        self.assertEqual(first["sender"]["agent"], "operator")
        self.assertNotEqual(first["sender"]["id"], "parent-session-must-not-be-used")
        resource_count = self.value("SELECT COUNT(*) FROM resources")
        status, _, _ = self.post({"to": self.beta, "body": "from browser"})
        self.assertEqual(status, 200)
        self.assertEqual(self.value("SELECT COUNT(*) FROM resources"), resource_count)
        coordinator = self.coordinator(self.beta)
        try:
            inbox = coordinator.inbox()["messages"]
        finally:
            coordinator.close()
        browser_message = next(item for item in inbox if item["body"] == "from browser")
        self.assertEqual(browser_message["sender_session"], first["sender"]["id"])
        self.server.stop_event.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(3)
        self.server = create_server(self.db, port=0)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.port = self.server.server_port
        self.assertEqual(self.config()["sender"]["id"], first["sender"]["id"])

    def test_array_recipient_post_returns_batch_messages_and_legacy_string_is_unchanged(self):
        status, _, raw = self.post({'to': [self.beta, self.gamma], 'body': 'group from browser'})
        self.assertEqual(status, 200)
        sent = json.loads(raw)
        self.assertEqual(len(sent['messages']), 2)
        self.assertEqual({item['recipient_session'] for item in sent['messages']}, {self.beta, self.gamma})
        self.assertEqual(len({item['batch_id'] for item in sent['messages']}), 1)
        status, _, raw = self.post({'to': self.beta, 'body': 'legacy response'})
        self.assertEqual(status, 200)
        self.assertIn('id', json.loads(raw))
        self.assertNotIn('messages', json.loads(raw))

    def test_array_recipient_post_rejects_partial_invalid_target_without_inserting(self):
        before = self.value('SELECT COUNT(*) FROM messages')
        status, _, _ = self.post({'to': [self.beta, 'session_missing'], 'body': 'must fail together'})
        self.assertEqual(status, 400)
        self.assertEqual(self.value('SELECT COUNT(*) FROM messages'), before)

    def test_batch_metadata_retains_all_deliveries_outside_history_page(self):
        status, _, raw = self.post({'to': [self.beta, self.gamma], 'body': 'batch page'})
        self.assertEqual(status, 200)
        sent = json.loads(raw)['messages']
        status, _, raw = self.request('GET', '/api/messages?limit=1')
        self.assertEqual(status, 200)
        item = json.loads(raw)['messages'][0]
        self.assertEqual(item['batch_id'], sent[0]['batch_id'])
        self.assertEqual({delivery['id'] for delivery in item['deliveries']}, {message['id'] for message in sent})

    def test_invalid_posts_have_no_insert_or_resource_side_effect(self):
        messages = self.value("SELECT COUNT(*) FROM messages")
        resources = self.value("SELECT COUNT(*) FROM resources")
        invalid = (
            ({"to": self.beta, "body": "x"}, {"X-Agent-Chat-CSRF": "wrong"}, 403),
            ({"to": self.beta, "body": "x"}, {"Origin": "http://evil.example"}, 403),
            ({"to": "beta", "body": "x"}, {}, 400),
            ({"to": self.beta, "body": "   "}, {}, 400),
            ({"to": self.beta}, {}, 400),
            ({"to": self.beta, "body": "x", "extra": True}, {}, 400),
            ([self.beta, "x"], {}, 400),
            ("not-json", {}, 400),
        )
        for data, headers, expected in invalid:
            self.assertEqual(self.post(data, headers)[0], expected)
        self.assertEqual(
            self.request(
                "POST",
                "/api/messages",
                json.dumps({"to": self.beta, "body": "x"}),
                {
                    "Content-Type": "application/json",
                    "Origin": "http://127.0.0.1:" + str(self.port),
                },
            )[0],
            403,
        )
        self.assertEqual(self.post({"to": self.beta, "body": "x"}, {"Content-Type": "text/plain"})[0], 415)
        self.assertEqual(self.post(b"{" + b"x" * 65537)[0], 413)
        self.assertEqual(self.value("SELECT COUNT(*) FROM messages"), messages)
        self.assertEqual(self.value("SELECT COUNT(*) FROM resources"), resources)

    def test_reply_preview_outside_page_direct_lookup_and_post_pair_validation(self):
        operator = self.config()['sender']['id']
        parent = self.send(self.beta, operator, 'quoted original')
        for number in range(3):
            self.send(self.alpha, self.beta, 'filler %s' % number)
        status, _, raw = self.post({'to': self.beta, 'body': 'reply', 'reply_to': parent['id']})
        self.assertEqual(status, 200)
        sent = json.loads(raw)
        self.assertEqual(sent['reply_preview']['body'], 'quoted original')
        status, _, raw = self.request('GET', '/api/messages?limit=1')
        self.assertEqual(status, 200)
        page = json.loads(raw)['messages']
        self.assertEqual(page[0]['reply_to'], parent['id'])
        self.assertEqual(page[0]['reply_preview']['id'], parent['id'])
        status, _, raw = self.request('GET', '/api/messages/' + parent['id'])
        self.assertEqual((status, json.loads(raw)['body']), (200, 'quoted original'))
        self.assertEqual(self.request('GET', '/api/messages/not-a-message')[0], 404)
        before = self.value('SELECT COUNT(*) FROM messages')
        self.assertEqual(self.post({'to': self.alpha, 'body': 'bad pair', 'reply_to': parent['id']})[0], 400)
        self.assertEqual(self.post({'to': self.beta, 'body': 'bad parent', 'reply_to': 'message_missing'})[0], 400)
        self.assertEqual(self.value('SELECT COUNT(*) FROM messages'), before)


class WebApiStartupTests(unittest.TestCase):
    def test_snapshot_tolerates_legacy_database_without_attachment_table(self):
        with tempfile.TemporaryDirectory(prefix="agent-chat-web-legacy-") as directory:
            database = pathlib.Path(directory) / 'state.sqlite3'
            coordinator = Coordinator(database, 'legacy')
            try:
                coordinator.register('legacy')
                coordinator.send('legacy', 'plain legacy message')
            finally:
                coordinator.close()
            connection = sqlite3.connect(database)
            connection.execute('DROP INDEX attachments_message_id')
            connection.execute('DROP TABLE attachments')
            connection.commit()
            connection.close()
            messages = snapshot(database)['messages']
            self.assertEqual([(message['body'], message['attachments']) for message in messages],
                             [('plain legacy message', [])])

    def test_snapshot_tolerates_legacy_database_without_reply_table(self):
        with tempfile.TemporaryDirectory(prefix='agent-chat-web-legacy-') as directory:
            database = pathlib.Path(directory) / 'state.sqlite3'
            coordinator = Coordinator(database, 'legacy')
            try:
                coordinator.register('legacy')
                coordinator.send('legacy', 'plain legacy message')
            finally:
                coordinator.close()
            connection = sqlite3.connect(database)
            connection.execute('DROP TABLE message_replies')
            connection.commit()
            connection.close()
            message = snapshot(database)['messages'][0]
            self.assertEqual((message['reply_to'], message['reply_preview']), (None, None))

    def test_snapshot_tolerates_legacy_database_without_batch_table(self):
        with tempfile.TemporaryDirectory(prefix='agent-chat-web-legacy-') as directory:
            database = pathlib.Path(directory) / 'state.sqlite3'
            coordinator = Coordinator(database, 'legacy')
            try:
                coordinator.register('legacy')
                coordinator.send('legacy', 'plain legacy message')
            finally:
                coordinator.close()
            connection = sqlite3.connect(database)
            connection.execute('DROP TABLE message_batches')
            connection.commit()
            connection.close()
            message = snapshot(database)['messages'][0]
            self.assertEqual((message['batch_id'], message['deliveries']), (None, []))

    def test_missing_and_wrong_schema_refuse_before_registration(self):
        with tempfile.TemporaryDirectory(prefix="agent-chat-web-test-") as directory:
            missing = pathlib.Path(directory) / "missing.sqlite3"
            with self.assertRaises(FileNotFoundError):
                create_server(missing, port=0)
            wrong = pathlib.Path(directory) / "wrong.sqlite3"
            connection = sqlite3.connect(wrong)
            connection.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
            connection.execute("INSERT INTO meta VALUES ('schema_version', '999')")
            connection.commit()
            connection.close()
            with self.assertRaises(RuntimeError):
                create_server(wrong, port=0)
            connection = sqlite3.connect(wrong)
            self.assertIsNone(connection.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sessions'").fetchone())
            connection.close()


if __name__ == "__main__":
    unittest.main()
