"""Synthetic end-to-end capability and prospective closeout contracts."""
import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import signal
import time
from unittest import mock, TestCase
import tempfile

from agent_chat import cli
from agent_chat.core import CoordError, Coordinator
from agent_chat.private_artifacts import atomic_capture, read_capability
from agent_chat.remote import HttpClient, RemoteCoordError
from test_remote_web import RemoteWebFixture, TOKEN


class PrivateArtifactTests(TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.path = self.root / 'capability.json'
        self.item = dict(resource='shared', session_id='a', host_id='mac',
                         reservation_id='rid', token='synthetic-secret', server='server', project='default')

    def test_atomic_replacement_and_exact_mode(self):
        atomic_capture(self.path, self.item)
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(read_capability(self.path), self.item)
        old = self.path.read_bytes()
        with mock.patch('agent_chat.private_artifacts.os.replace', side_effect=OSError):
            with self.assertRaises(CoordError): atomic_capture(self.path, dict(self.item, token='other'))
        self.assertEqual(self.path.read_bytes(), old)
        self.assertEqual(list(self.root.iterdir()), [self.path])

    def test_symlink_modes_hardlinks_fifo_rejected(self):
        target = self.root / 'target'
        target.write_text('untouched')
        self.path.symlink_to(target)
        for operation in (lambda: atomic_capture(self.path, self.item), lambda: read_capability(self.path)):
            with self.assertRaises(CoordError): operation()
        self.assertEqual(target.read_text(), 'untouched')
        self.path.unlink()
        atomic_capture(self.path, self.item)
        self.path.chmod(0o644)
        with self.assertRaises(CoordError): read_capability(self.path)
        with self.assertRaises(CoordError): atomic_capture(self.path, self.item)
        self.path.chmod(0o600)
        os.link(self.path, target.with_name('link'))
        with self.assertRaises(CoordError): read_capability(self.path)
        self.path.unlink()
        os.mkfifo(self.path, 0o600)
        with self.assertRaises(CoordError): read_capability(self.path)


class PreventionWorkflows(RemoteWebFixture):
    def setUp(self):
        super().setUp()
        self.call('register', agent='alpha')
        self.root = Path(self.tmp.name) / 'private'
        self.root.mkdir(mode=0o700)
        self.capability = self.root / 'capability.json'
        self.journal = self.root / 'process.jsonl'
        self.env = dict(os.environ, AGENT_CHAT_SERVER=self.url, AGENT_CHAT_API_TOKEN=TOKEN,
                        AGENT_CHAT_SESSION='a', AGENT_CHAT_HOST_ID='mac',
                        AGENT_CHAT_STATE_DIR=str(self.root))
        self.env.pop('AGENT_CHAT_TOKEN', None)
        self.env.pop('AGENT_CHAT_PRODUCER_CAPABILITY_FILE', None)

    def invoke(self, *args):
        output = io.StringIO()
        with mock.patch.dict(os.environ, self.env, clear=True), contextlib.redirect_stdout(output):
            result = cli.main(list(args))
        return result, output.getvalue()

    def claim(self):
        _, output = self.invoke('request', 'shared', '--minutes', '5',
                                '--capability-file', str(self.capability))
        self.assertNotIn('"token"', output)
        return read_capability(self.capability)

    def run_args(self, item, code='pass'):
        return ['run', '--capability-file', str(self.capability),
                '--reservation-id', item['reservation_id'], '--process-evidence-file', str(self.journal),
                'shared', '--', sys.executable, '-c', code]

    def query(self, expression):
        result = subprocess.run(['jq', '-ser', expression, str(self.journal)],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout.strip()

    def rows(self):
        with contextlib.closing(Coordinator(self.db)) as coord:
            return [dict(row) for row in coord.db.execute('SELECT * FROM remote_guarded_runs')]

    def test_lost_stdout_same_owner_and_queued_no_capability(self):
        first = self.claim()
        class LostStdout(io.StringIO):
            def write(self, value): raise BrokenPipeError()
        with mock.patch.dict(os.environ, self.env, clear=True), contextlib.redirect_stdout(LostStdout()):
            with self.assertRaises(BrokenPipeError):
                cli.main(['request', 'shared', '--minutes', '5', '--capability-file', str(self.capability)])
        self.assertEqual(read_capability(self.capability), first)
        self.assertEqual(self.claim(), first)
        self.call('register', session='b', agent='beta')
        queued = self.root / 'queued.json'
        queued.write_text('untouched')
        queued.chmod(0o600)
        with mock.patch.dict(os.environ, dict(self.env, AGENT_CHAT_SESSION='b'), clear=True):
            with contextlib.redirect_stdout(io.StringIO()) as output:
                cli.main(['request', 'shared', '--minutes', '5', '--capability-file', str(queued)])
        self.assertEqual(queued.read_text(), 'untouched')
        self.assertNotIn('"token"', output.getvalue())

    def test_tuple_mismatch_before_any_admission_or_child(self):
        item = self.claim()
        atomic_capture(self.capability, dict(item, reservation_id='wrong'))
        with mock.patch.object(HttpClient, 'call') as api:
            with self.assertRaises(CoordError): self.invoke(*self.run_args(item))
        api.assert_not_called()
        self.assertFalse(self.journal.exists())
        self.assertEqual(self.rows(), [])

    def test_request_without_private_destination_has_no_server_effect(self):
        with mock.patch.object(HttpClient, 'call') as api:
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                self.invoke('request', 'shared', '--minutes', '5')
        api.assert_not_called()

    def test_server_rejects_valid_token_with_different_exact_rid(self):
        item = self.claim()
        with self.assertRaises(RemoteCoordError):
            self.call('begin-guard', resource='shared', token=item['token'], reservation_id='wrong')
        self.assertEqual(self.rows(), [])

    def test_normal_execution_retains_actual_reap_attach_and_close(self):
        item = self.claim()
        marker = self.root / 'effect'
        result, _ = self.invoke(*self.run_args(item, f'from pathlib import Path; Path({str(marker)!r}).touch()'))
        self.assertEqual(result, 0)
        self.assertTrue(marker.exists())
        self.assertEqual(self.query('[.[] | select(.event=="leader_reaped") | .returncode] == [0]'), 'true')
        self.assertEqual(self.query('any(.[]; .event=="group_absent")'), 'true')
        self.assertEqual(self.query('([.[]|select(.operation=="attach-guard-pid" and .event=="guard_call_succeeded")|.sequence][0]) < ([.[]|select(.event=="gate_released")|.sequence][0])'), 'true')
        self.assertTrue(all(row['closed_at'] for row in self.rows()))
        self.assertNotIn(item['token'], self.journal.read_text())

    def test_disconnect_after_attach_preserves_open_guard_and_no_effect(self):
        item = self.claim()
        marker = self.root / 'effect'
        original = HttpClient.call
        def disconnect(client, path, payload):
            result = original(client, path, payload)
            if payload['op'] == 'attach-guard-pid': raise RemoteCoordError('synthetic disconnect')
            return result
        with mock.patch.object(HttpClient, 'call', disconnect):
            with self.assertRaises(RemoteCoordError):
                self.invoke(*self.run_args(item, f'from pathlib import Path; Path({str(marker)!r}).touch()'))
        self.assertFalse(marker.exists())
        self.assertTrue(all(row['closed_at'] is None for row in self.rows()))
        self.assertEqual(self.query('any(.[]; .event=="leader_reaped") and any(.[]; .event=="group_absent") and (any(.[]; .event=="gate_released")|not)'), 'true')

    def assert_invalid_attach_response(self, change):
        item = self.claim()
        marker = self.root / 'effect'
        original, calls = HttpClient.call, []
        def invalid_attach(client, path, payload):
            calls.append(payload['op'])
            result = original(client, path, payload)
            if payload['op'] == 'attach-guard-pid':
                change(result)
            return result
        with mock.patch.object(HttpClient, 'call', invalid_attach):
            with self.assertRaisesRegex(CoordError, 'attach response unresolved'):
                self.invoke(*self.run_args(item, f'from pathlib import Path; Path({str(marker)!r}).touch()'))
        self.assertFalse(marker.exists())
        rows = self.rows()
        self.assertTrue(rows and all(row['closed_at'] is None for row in rows))
        self.assertNotIn('close-guard', calls)
        self.assertEqual(read_capability(self.capability), item)
        for row in rows:
            self.assertGreater(row['local_pid'], 0)
            with self.assertRaises(ProcessLookupError): os.killpg(row['local_pid'], 0)
        self.assertEqual(self.query(
            'any(.[]; .event=="leader_reaped" and .returncode != null) '
            'and any(.[]; .event=="group_absent") '
            'and any(.[]; .event=="guard_call_failed" and .operation=="attach-guard-pid") '
            'and (any(.[]; .event=="gate_released" or '
            '(.event=="guard_call_succeeded" and .operation=="attach-guard-pid"))|not)'), 'true')

    def test_successful_attach_false_keeps_gate_closed_and_guard_open(self):
        self.assert_invalid_attach_response(lambda result: result.update(attached=False))

    def test_successful_attach_missing_confirmation_keeps_gate_closed(self):
        self.assert_invalid_attach_response(lambda result: result.pop('attached'))

    def test_successful_attach_wrong_confirmation_type_keeps_gate_closed(self):
        for index, value in enumerate((1, 'true', None)):
            with self.subTest(attached=value):
                self.journal = self.root / f'wrong-type-{index}.jsonl'
                self.assert_invalid_attach_response(lambda result: result.update(attached=value))

    def test_successful_attach_mismatched_run_id_keeps_gate_closed(self):
        self.assert_invalid_attach_response(lambda result: result.update(run_id='another-run'))

    def test_successful_attach_missing_run_id_keeps_gate_closed(self):
        self.assert_invalid_attach_response(lambda result: result.pop('run_id'))

    def test_evidence_failure_closes_actual_group_but_retains_guard(self):
        item = self.claim()
        from agent_chat.private_artifacts import ProcessEvidence
        original = ProcessEvidence.record
        def fail(journal, event, **facts):
            if event == 'gate_released':
                journal.failed = True
                raise CoordError('synthetic retention failure')
            return original(journal, event, **facts)
        with mock.patch.object(ProcessEvidence, 'record', fail):
            with self.assertRaises(CoordError):
                self.invoke(*self.run_args(item, 'import time; time.sleep(60)'))
        rows = self.rows()
        self.assertTrue(rows and all(row['closed_at'] is None for row in rows))
        for row in rows:
            with self.assertRaises(ProcessLookupError): os.killpg(row['local_pid'], 0)

    def test_runner_crash_keeps_partial_facts_without_inferred_close(self):
        item = self.claim()
        marker = self.root / 'effect'
        code = '''import os, signal, sys
from agent_chat import cli
from agent_chat.remote import HttpClient
original = HttpClient.call
def crash(client, path, payload):
    result = original(client, path, payload)
    if payload['op'] == 'attach-guard-pid':
        os.kill(os.getpid(), signal.SIGKILL)
    return result
HttpClient.call = crash
cli.main(sys.argv[1:])
'''
        env = dict(self.env, PYTHONPATH=str(Path(__file__).resolve().parents[1] / 'src'))
        child_code = f'from pathlib import Path; Path({str(marker)!r}).touch()'
        runner = subprocess.run([sys.executable, '-c', code, *self.run_args(item, child_code)],
                                env=env, capture_output=True, timeout=10)
        self.assertEqual(runner.returncode, -signal.SIGKILL)
        self.assertFalse(marker.exists())
        rows = self.rows()
        self.assertTrue(rows and all(row['closed_at'] is None for row in rows))
        self.assertEqual(self.query('any(.[]; .event=="launcher_created") and (any(.[]; .event=="leader_reaped" or .event=="group_absent" or .event=="gate_released")|not)'), 'true')
        # The client died before releasing the pipe gate. EOF exits only its
        # inert launcher; no journal entry is invented for the unobserved reap.
        for row in rows:
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                try: os.killpg(row['local_pid'], 0)
                except ProcessLookupError: break
                time.sleep(.05)
            else:
                os.killpg(row['local_pid'], signal.SIGKILL)
                self.fail('crashed runner launcher group did not disappear')
