import json
from pathlib import Path
import subprocess
import time
import uuid

from agent_chat.bridge_state import BridgeState
from agent_chat.core import Coordinator
from test_remote_web import RemoteWebFixture


class MeasurementWebTests(RemoteWebFixture):
    def start(self, **kwargs):
        self.rollouts = Path(self.tmp.name) / 'rollouts'
        self.rollouts.mkdir(exist_ok=True)
        super().start(sessions_root=self.rollouts, **kwargs)

    def setUp(self):
        super().setUp()
        self.root_thread = str(uuid.uuid4())
        coord = Coordinator(self.db, 'measurement-agent')
        try:
            coord.register('Measured agent')
            BridgeState(coord).bind(thread_id=self.root_thread)
        finally:
            coord.close()
        self.log = self.rollouts / ('rollout-' + self.root_thread + '.jsonl')
        self.log.write_text(json.dumps({'type': 'session_meta', 'payload': {
            'id': self.root_thread, 'session_id': self.root_thread,
            'source': 'cli', 'thread_source': 'user'}}) + '\n')

    def check_json(self, raw, expression):
        result = subprocess.run(['jq', '-e', expression], input=raw,
                                capture_output=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr or result.stdout)

    def measure(self, payload, project=None, csrf=True):
        url = '/api/measurements' + ('?project=' + project if project else '')
        headers = {'X-Agent-Chat-CSRF': self.server.csrf_token} if csrf else {}
        return self.request('POST', url, payload, auth='basic', headers=headers)

    def wait_running(self):
        until = time.monotonic() + 5
        while time.monotonic() < until:
            active = self.server.measurements.status('default')['active']
            if active and active['state'] == 'running':
                return
            time.sleep(.02)
        self.fail('measurement did not reach running state')

    def test_auth_csrf_duration_and_project_isolation(self):
        self.assertEqual(self.request('GET', '/api/measurements', auth=None)[0], 401)
        self.assertEqual(self.measure({'op': 'start', 'duration_seconds': 300}, csrf=False)[0], 403)
        for duration in (0, 59, 3601, True, '300'):
            self.assertEqual(self.measure({'op': 'start', 'duration_seconds': duration})[0], 400)
        self.assertEqual(self.measure({'op': 'start', 'duration_seconds': 300, 'extra': True})[0], 400)
        self.assertEqual(self.measure({'op': 'start', 'duration_seconds': 300, 'pause_at_end': 'yes'})[0], 400)
        status, raw, _ = self.measure({'op': 'start', 'duration_seconds': 300})
        self.assertEqual(status, 200, raw)
        self.wait_running()
        self.assertEqual(self.measure({'op': 'start', 'duration_seconds': 300})[0], 400)
        other = self.server.projects.create('Measurement isolation')['id']
        status, raw, _ = self.request('GET', '/api/measurements?project=' + other)
        self.assertEqual(status, 200, raw)
        self.check_json(raw, '.active==null and .latest==null')
        self.measure({'op': 'stop'}, project=other)
        self.assertIsNotNone(self.server.measurements.status('default')['active'])

    def test_stop_reports_only_new_tokens_and_survives_restart(self):
        self.assertEqual(self.measure({'op': 'start', 'duration_seconds': 300})[0], 200)
        self.wait_running()
        with self.log.open('a') as stream:
            stream.write(json.dumps({'type': 'token_usage_record', 'payload': {
                'thread_id': self.root_thread, 'session_id': self.root_thread,
                'response_id': 'new-http-response', 'usage': {
                    'input_tokens': 100, 'cached_input_tokens': 80,
                    'output_tokens': 10, 'reasoning_output_tokens': 4}}}) + '\n')
        status, raw, _ = self.measure({'op': 'stop'})
        self.assertEqual(status, 200, raw)
        self.check_json(raw, '.active==null and .latest.totals.response_count==1 and .latest.totals.fresh_input_tokens==20 and .latest.totals.cached_input_tokens==80 and .latest.totals.output_tokens==10')
        with self.log.open('a') as stream:
            stream.write(json.dumps({'type': 'ignored_fixture_record', 'payload': 'after completed window'}) + '\n')
        self.stop()
        self.start()
        status, raw, _ = self.request('GET', '/api/measurements')
        self.assertEqual(status, 200, raw)
        self.check_json(raw, '.active==null and .latest.totals.response_count==1 and (.latest.errors|length)==0')

    def test_opt_in_pause_is_addressed_and_idempotent(self):
        from agent_chat.measurement_pause import request_pauses
        other = Coordinator(self.db, 'second-measured-agent')
        try:
            other.register('Second measured agent')
            BridgeState(other).bind(thread_id=str(uuid.uuid4()))
        finally:
            other.close()
        self.assertEqual(self.measure({'op': 'start', 'duration_seconds': 60, 'pause_at_end': True})[0], 200)
        self.wait_running()
        status, raw, _ = self.measure({'op': 'stop'})
        self.assertEqual(status, 200, raw)
        self.check_json(raw, '.latest.pause_at_end==true and .latest.pause_requested_at!=null and .latest.pause_error==null')
        measurement = self.server.measurements.status('default')['latest']
        request_pauses(self.db, self.server.sender_session, measurement['id'], ['measurement-agent'])
        coord = Coordinator(self.db, 'measurement-agent')
        try:
            messages = coord.inbox()['messages']
            self.assertEqual(len(messages), 1)
            self.assertIn('Pause at your next safe checkpoint', messages[0]['body'])
            self.assertIsNotNone(messages[0]['batch_id'])
            self.assertEqual(len(messages[0]['deliveries']), 2)
            self.assertEqual(BridgeState(coord).pending()[0]['id'], messages[0]['id'])
        finally:
            coord.close()

    def test_restart_retries_pause_without_duplicate_message(self):
        real_callback = self.server.measurements.pause_callback
        def interrupted_callback(*args):
            real_callback(*args)
            raise RuntimeError('simulated interruption after message commit')
        self.server.measurements.pause_callback = interrupted_callback
        self.assertEqual(self.measure({'op': 'start', 'duration_seconds': 60, 'pause_at_end': True})[0], 200)
        self.wait_running()
        status, raw, _ = self.measure({'op': 'stop'})
        self.assertEqual(status, 200, raw)
        self.check_json(raw, '.latest.pause_requested_at==null and .latest.pause_error!=null')
        self.stop()
        self.start()
        status, raw, _ = self.request('GET', '/api/measurements')
        self.assertEqual(status, 200, raw)
        self.check_json(raw, '.latest.pause_requested_at!=null and .latest.pause_error==null')
        coord = Coordinator(self.db, 'measurement-agent')
        try:
            self.assertEqual(len(coord.inbox()['messages']), 1)
        finally:
            coord.close()
