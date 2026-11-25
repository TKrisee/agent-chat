import os
from pathlib import Path
import subprocess
import sys
import sqlite3
import unittest
from test_remote_web import RemoteWebFixture, TOKEN


def jq(raw, expression):
    return subprocess.run(['jq', '-er', expression], input=raw, stdout=subprocess.PIPE, check=True).stdout.decode().strip()

class FirstReplyProbe(RemoteWebFixture):
    def test_first_reply_actual_cli(self):
        status, raw, _ = self.request('POST', '/api/coord', {'op':'register','session':'po','host_id':'mac','params':{'agent':'project-po'}})
        self.assertEqual(status, 200, raw)
        status, config, _ = self.request('GET', '/api/config', auth='basic')
        self.assertEqual(status, 200, config)
        operator = jq(config, '.sender.id')
        status, raw, _ = self.request('POST', '/api/messages', {'to':'po','body':'Customer request'}, auth='basic', headers={'X-Agent-Chat-CSRF':self.server.csrf_token})
        self.assertEqual(status, 200, raw)
        source = jq(raw, '.id')
        body = Path(self.tmp.name) / 'reply.txt'; body.write_text('First substantive reply')
        env = dict(os.environ, AGENT_CHAT_HOST_ID='mac', PYTHONPATH='src')
        cmd = [sys.executable,'-m','agent_chat.cli','--server',self.url,'--api-token',TOKEN,'--project','default','--session','po','send','--to',operator,'--body-file',str(body),'--reply-to',source,'--ack-reply']
        first = subprocess.run(cmd, capture_output=True, env=env, timeout=8)
        self.assertEqual(first.returncode, 0, first.stderr.decode())
        reply = jq(first.stdout, '.id')
        lookup_cmd = [sys.executable,'-m','agent_chat.cli','--server',self.url,'--api-token',TOKEN,'--project','default','--session','po','first-ack-lookup',source,'--expected-recipient',operator]
        lookup = subprocess.run(lookup_cmd, capture_output=True, env=env, timeout=8)
        self.assertEqual(lookup.returncode, 0, lookup.stderr.decode())
        self.assertEqual(jq(lookup.stdout, '.status'), 'recorded')
        self.assertEqual(jq(lookup.stdout, '.reply_id'), reply)
        wrong_lookup = subprocess.run(lookup_cmd[:-1] + ['not-the-operator'], capture_output=True, env=env, timeout=8)
        self.assertEqual(jq(wrong_lookup.stdout, '.status'), 'unauthorized')
        retry = subprocess.run(cmd, capture_output=True, env=env, timeout=8)
        self.assertEqual(retry.returncode, 0, retry.stderr.decode())
        self.assertEqual(jq(retry.stdout, '.id'), reply)
        self.assertEqual(jq(retry.stdout, '.recipient_session'), operator)

    def test_committed_reply_lost_response_reconciles_without_duplicate(self):
        import http.client
        import http.server
        import socket
        import threading
        status, raw, _ = self.request('POST', '/api/coord', {'op':'register','session':'po','host_id':'mac','params':{'agent':'project-po'}})
        self.assertEqual(status, 200, raw)
        status, config, _ = self.request('GET', '/api/config', auth='basic')
        self.assertEqual(status, 200, config)
        operator = jq(config, '.sender.id')
        status, raw, _ = self.request('POST', '/api/messages', {'to':'po','body':'Customer request'}, auth='basic', headers={'X-Agent-Chat-CSRF':self.server.csrf_token})
        self.assertEqual(status, 200, raw)
        source = jq(raw, '.id')
        upstream_port = self.server.server_port
        observed = []
        class DropCommittedResponse(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                payload = self.rfile.read(int(self.headers['Content-Length']))
                conn = http.client.HTTPConnection('127.0.0.1', upstream_port, timeout=5)
                try:
                    headers = dict(self.headers); headers['Host'] = '127.0.0.1:' + str(upstream_port)
                    conn.request('POST', self.path, payload, headers)
                    response = conn.getresponse()
                    response.read()
                    observed.append(response.status)
                finally:
                    conn.close()
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR)
            def log_message(self, *args):
                pass
        proxy = http.server.HTTPServer(('127.0.0.1', 0), DropCommittedResponse)
        thread = threading.Thread(target=proxy.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 3)
        self.addCleanup(proxy.server_close)
        self.addCleanup(proxy.shutdown)
        body = Path(self.tmp.name) / 'reply-lost.txt'; body.write_text('First substantive reply')
        env = dict(os.environ, AGENT_CHAT_HOST_ID='mac')
        base = [sys.executable,'-m','agent_chat.cli','--server',self.url,'--api-token',TOKEN,'--project','default','--session','po']
        args = ['send','--to',operator,'--body-file',str(body),'--reply-to',source,'--ack-reply']
        via_proxy = base.copy(); via_proxy[via_proxy.index(self.url)] = 'http://127.0.0.1:' + str(proxy.server_port)
        uncertain = subprocess.run(via_proxy+args, capture_output=True, env=env, timeout=8)
        self.assertNotEqual(uncertain.returncode, 0)
        self.assertEqual(observed, [200], uncertain.stderr.decode())
        lookup = subprocess.run(base+['first-ack-lookup',source,'--expected-recipient',operator], capture_output=True, env=env, timeout=8)
        self.assertEqual(lookup.returncode, 0, lookup.stderr.decode())
        self.assertEqual(jq(lookup.stdout, '.status'), 'recorded')
        reply = jq(lookup.stdout, '.reply_id')
        retry = subprocess.run(base+args, capture_output=True, env=env, timeout=8)
        self.assertEqual(retry.returncode, 0, retry.stderr.decode())
        self.assertEqual(jq(retry.stdout, '.id'), reply)
        self.assertEqual(jq(retry.stdout, '.recipient_session'), operator)
        with sqlite3.connect(self.db) as db:
            rows = db.execute('SELECT message_id FROM message_replies WHERE reply_to=?', (source,)).fetchall()
        self.assertEqual(rows, [(reply,)])

    def test_removed_operator_session_fallback_and_wrong_recipient(self):
        status, raw, _ = self.request('POST', '/api/coord', {'op':'register','session':'po','host_id':'mac','params':{'agent':'project-po'}})
        self.assertEqual(status, 200, raw)
        status, config, _ = self.request('GET', '/api/config', auth='basic')
        operator = jq(config, '.sender.id')
        status, raw, _ = self.request('POST', '/api/messages', {'to':'po','body':'Customer request'}, auth='basic', headers={'X-Agent-Chat-CSRF':self.server.csrf_token})
        self.assertEqual(status, 200, raw)
        source = jq(raw, '.id')
        with sqlite3.connect(self.db) as db:
            db.execute('DELETE FROM sessions WHERE id=?', (operator,))
        body = Path(self.tmp.name) / 'reply.txt'; body.write_text('First substantive reply')
        env = dict(os.environ, AGENT_CHAT_HOST_ID='mac', PYTHONPATH='src')
        base = [sys.executable,'-m','agent_chat.cli','--server',self.url,'--api-token',TOKEN,'--project','default','--session','po','send']
        suffix = ['--body-file',str(body),'--reply-to',source,'--ack-reply']
        wrong = subprocess.run(base+['--to','missing-operator']+suffix, capture_output=True, env=env, timeout=8)
        self.assertNotEqual(wrong.returncode, 0)
        first = subprocess.run(base+['--to',operator]+suffix, capture_output=True, env=env, timeout=8)
        self.assertEqual(first.returncode, 0, first.stderr.decode())
        self.assertEqual(jq(first.stdout, '.recipient_session'), operator)

    def test_ambiguous_label_and_untrusted_missing_actor_rejected(self):
        for sid, agent in [('po','project-po'),('spoof','claimed-operator'),('twin-a','same-label'),('twin-b','same-label')]:
            status, raw, _ = self.request('POST', '/api/coord', {'op':'register','session':sid,'host_id':'mac','params':{'agent':agent}})
            self.assertEqual(status, 200, raw)
        status, raw, _ = self.request('POST', '/api/coord', {'op':'send','session':'spoof','host_id':'mac','params':{'to':'po','body':'I am the customer'}})
        self.assertEqual(status, 200, raw)
        source = jq(raw, '.id')
        with sqlite3.connect(self.db) as db:
            db.execute('DELETE FROM sessions WHERE id=?', ('spoof',))
        body = Path(self.tmp.name) / 'reply.txt'; body.write_text('First substantive reply')
        env = dict(os.environ, AGENT_CHAT_HOST_ID='mac', PYTHONPATH='src')
        base = [sys.executable,'-m','agent_chat.cli','--server',self.url,'--api-token',TOKEN,'--project','default','--session','po','send']
        suffix = ['--body-file',str(body),'--reply-to',source,'--ack-reply']
        for target in ('spoof','same-label','twin-a'):
            attempt = subprocess.run(base+['--to',target]+suffix, capture_output=True, env=env, timeout=8)
            self.assertNotEqual(attempt.returncode, 0, target)

import test_project_activity as activity_fixtures

class ActualPOReplyProbe(activity_fixtures.ActivityWorkflow):
    def test_real_designated_po_first_reply(self):
        project, po, _, _, _ = self.projects['Alpha']
        status, config, _ = self.request('GET', '/api/config?project='+project, auth='basic')
        self.assertEqual(status, 200, config)
        operator = jq(config, '.sender.id')
        status, raw, _ = self.request('POST', '/api/messages?project='+project, {'to':po,'body':'Customer request'}, auth='basic', headers={'X-Agent-Chat-CSRF':self.server.csrf_token})
        self.assertEqual(status, 200, raw)
        source = jq(raw, '.id')
        body = Path(self.tmp.name) / 'po-reply.txt'; body.write_text('PO substantive reply')
        env = dict(os.environ, AGENT_CHAT_HOST_ID='mac', PYTHONPATH='src')
        base = [sys.executable,'-m','agent_chat.cli','--server',self.url,'--api-token',TOKEN,'--project',project,'--session',po]
        sent = subprocess.run(base+['send','--to',operator,'--body-file',str(body),'--reply-to',source,'--ack-reply'], capture_output=True, env=env, timeout=8)
        self.assertEqual(sent.returncode, 0, sent.stderr.decode())
        reply = jq(sent.stdout, '.id')
        lookup = subprocess.run(base+['first-ack-lookup',source,'--expected-recipient',operator], capture_output=True, env=env, timeout=8)
        self.assertEqual(jq(lookup.stdout, '.reply_id'), reply)
        self.assertEqual(jq(sent.stdout, '.recipient_session'), operator)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    suite.addTests(loader.loadTestsFromTestCase(FirstReplyProbe))
    suite.addTest(ActualPOReplyProbe("test_real_designated_po_first_reply"))
    return suite
