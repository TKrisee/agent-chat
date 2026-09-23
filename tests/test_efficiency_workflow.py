"""Real HTTP/CLI workflows; JSON assertions use jq rather than another parser."""
import json
import os
from pathlib import Path
import subprocess
import sys

from agent_chat.bridge import Bridge
from agent_chat.bridge_state import BridgeState
from agent_chat.core import Coordinator
from test_bridge import FakeRpc
from test_remote_web import RemoteWebFixture, TOKEN


class EfficiencyWorkflowTests(RemoteWebFixture):
    def assert_json(self, raw, expression):
        result = subprocess.run(['jq', '-e', expression], input=raw,
                                text=True, capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr or result.stdout)

    def command(self, *args, ok=True):
        env = {k: v for k, v in os.environ.items() if not k.startswith('AGENT_CHAT_')}
        env.update(AGENT_CHAT_SERVER=self.url, AGENT_CHAT_API_TOKEN=TOKEN,
                   AGENT_CHAT_HOST_ID='mac', AGENT_CHAT_STATE_DIR=self.tmp.name)
        result = subprocess.run([sys.executable, str(Path(__file__).resolve().parents[1] /
                                'bin/agent-chat-client'), *args], env=env,
                                capture_output=True, text=True, timeout=10)
        if ok:
            self.assertEqual(result.returncode, 0, result.stderr)
        else:
            self.assertNotEqual(result.returncode, 0)
        return result.stdout if ok else result.stderr

    def setup_people(self):
        self.call('register', session='a', agent='worker')
        self.call('register', session='b', agent='sender')

    def test_cli_consumes_context_replies_and_retries_without_duplicate(self):
        self.setup_people()
        original = self.call('send', session='b', to='a', body='Review the result')
        self.assert_json(self.command('--session', 'a', 'context'),
                         '.messages | length == 1')
        self.assert_json(self.command('--session', 'a', 'message', original['id']),
                         '.message.body == "Review the result"')
        body = Path(self.tmp.name) / 'reply.md'
        body.write_text('Reviewed; checks passed.')
        args = ('--session', 'a', 'send', '--to', 'b', '--body-file', str(body),
                '--reply-to', original['id'], '--ack-reply')
        self.assert_json(self.command(*args), '.acknowledged_reply == true')
        self.assert_json(self.command(*args), '.acknowledged_reply == true')
        self.assertEqual(len(self.call('inbox', session='b')['messages']), 1)
        self.assertEqual(self.call('context')['messages'], [])
        self.command('--session', 'b', 'message', original['id'], ok=False)
        self.command('--session', 'a', 'send', '--to', 'b', '--body-file', str(body),
                     '--ack-reply', ok=False)

    def test_wire_budget_unicode_and_project_attachment_urls(self):
        project = self.client.call('/api/projects/rpc', {'op': 'create', 'name': 'Unicode'})['project']['id']
        from agent_chat.remote import HttpClient
        scoped = HttpClient(self.url, TOKEN, project=project)
        def call(op, session='a', **params):
            return scoped.call('/api/coord', {'op': op, 'session': session,
                               'host_id': 'mac', 'params': params})
        call('register', agent='worker')
        call('register', session='b', agent='sender')
        import base64
        call('send', session='b', to='a', body='🙂' * 5000,
             attachments=[{'name': '🙂.png', 'content_base64':
                           base64.b64encode(b'\x89PNG\r\n\x1a\nproof').decode()}])
        status, raw, _ = self.request('POST', '/api/coord',
            {'op': 'context', 'session': 'a', 'host_id': 'mac', 'params': {'max_bytes': 900}},
            headers={'X-Agent-Chat-Project': project})
        self.assertEqual(status, 200, raw)
        self.assertLessEqual(len(raw), 900)
        self.assert_json(raw.decode(), '.truncated and .messages[0].body_truncated')
        self.assert_json(self.command('--project', project, '--session', 'a', 'context',
                         '--max-bytes', '900'), '.messages | length == 1')

    def test_routine_workflow_halves_coordination_steps_and_reduces_output(self):
        self.setup_people()
        coord = Coordinator(self.db, 'a')
        try:
            coord.db.executemany('INSERT INTO resources(name) VALUES(?)',
                                [(f'file:old/{i}.cs',) for i in range(409)])
        finally:
            coord.close()
        old_message = self.call('send', session='b', to='a', body='Verify readiness')
        old_calls = [('inbox', {}), ('status', {}),
                     ('send', {'to': 'b', 'body': 'Ready', 'reply_to': old_message['id']}),
                     ('acknowledge', {'id': old_message['id']})]
        old_results = [self.call(op, **params) for op, params in old_calls]
        fresh = self.call('send', session='b', to='a', body='Verify readiness again')
        new_calls = [('context', {}), ('send', {'to': 'b', 'body': 'Ready',
                     'reply_to': fresh['id'], 'ack_reply': True})]
        new_results = [self.call(op, **params) for op, params in new_calls]
        self.assertLessEqual(len(new_calls), len(old_calls) / 2)
        old_bytes = sum(len(json.dumps(r).encode()) for r in old_results[:2])
        new_bytes = len(json.dumps(new_results[0]).encode())
        self.assertLessEqual(new_bytes, old_bytes * .3)
        self.assertEqual(self.call('inbox')['messages'], [])
        print(f'efficiency fixture: coordination steps {len(old_calls)} -> {len(new_calls)}; '
              f'snapshot bytes {old_bytes} -> {new_bytes}')

    def test_direct_wake_inlines_complete_content_and_hides_child_body(self):
        self.setup_people()
        import uuid
        coord = Coordinator(self.db, 'a')
        try:
            state = BridgeState(coord)
            thread = str(uuid.uuid4())
            state.bind(thread_id=thread)
            child = Coordinator(self.db, 'child')
            try:
                child.register('child')
                BridgeState(child).bind(parent_session='a', agent_path='/root/child')
            finally:
                child.close()
            self.call('send', session='b', to='a', body='Inline complete content')
            self.call('send', session='b', to='child', body='Child confidential content')
            self.call('send', session='b', to='a', body='🙂' * 9000)
            rpc = FakeRpc()
            Bridge(state, rpc).tick()
            payload = next(p for m, p in rpc.calls if m == 'thread/queue/add')['input'][0]['text']
            self.assertLessEqual(len(payload.encode()), 12288)
            self.assertNotIn('Child confidential content', payload)
            self.assert_json(payload.split('\n', 1)[1],
                '(.messages | any(.complete and .body == "Inline complete content")) and '
                '(.messages | any(.complete == false)) and (.deliveries | length == 3)')
            self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM message_reads').fetchone()[0], 0)
            self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM messages WHERE acked_at IS NOT NULL').fetchone()[0], 0)
        finally:
            coord.close()

    def test_twenty_message_wake_reserves_space_for_every_retrieval_id(self):
        self.setup_people()
        import uuid
        coord = Coordinator(self.db, 'a')
        try:
            state = BridgeState(coord)
            thread = str(uuid.uuid4())
            state.bind(thread_id=thread)
            for size in [2800, 3100, 400, 3000, 90] * 4:
                self.call('send', session='b', to='a', body='x' * size)
            rpc = FakeRpc()
            Bridge(state, rpc).tick()
            payload = next(p for m, p in rpc.calls if m == 'thread/queue/add')['input'][0]['text']
            self.assertLessEqual(len(payload.encode()), 12288)
            self.assert_json(payload.split('\n', 1)[1],
                '(.messages | length == 20) and (.deliveries | length == 20) and '
                '(.messages | any(.complete == false))')
        finally:
            coord.close()
