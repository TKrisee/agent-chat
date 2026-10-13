import contextlib
import builtins
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import urllib.error
import uuid
from unittest import mock, TestCase, skipUnless

from agent_chat import cli
from agent_chat.app_logging import LoggingConfig, configure
from agent_chat.core import Coordinator, CoordError
from agent_chat.remote import HttpClient, INBOX_TRACE_HEADER, MAX_RESPONSE_BYTES, RemoteCoordError
from test_remote_web import RemoteWebFixture, TOKEN


class BatchManifestPathTests(TestCase):
    """Service-free descriptor admission regressions, without a server fixture."""

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='agent-chat-manifest-test-')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.env = {key: value for key, value in os.environ.items() if not key.startswith('AGENT_CHAT_')}
        self.env['PYTHONPATH'] = str(Path(__file__).resolve().parents[1] / 'src')
        self.env['AGENT_CHAT_STATE_DIR'] = str(self.root)

    @skipUnless(hasattr(os, 'mkfifo'), 'FIFO paths require POSIX')
    def test_fifo_and_directory_reject_before_guard_activity(self):
        fifo = self.root / 'private-fifo'
        os.mkfifo(fifo, 0o600)  # Deliberately no writer: old blocking open cannot finish.
        directory = self.root / 'private-directory'
        directory.mkdir(mode=0o700)
        code = """
import contextlib
import sys
from unittest import mock
from agent_chat import cli
from agent_chat.core import CoordError
from agent_chat.remote import HttpClient
with contextlib.ExitStack() as stack:
    stack.enter_context(mock.patch('agent_chat.remote.client_host_id', return_value='manifest-host'))
    api = stack.enter_context(mock.patch.object(HttpClient, 'call', side_effect=AssertionError('admission reached')))
    launcher = stack.enter_context(mock.patch('agent_chat.cli.subprocess.Popen', side_effect=AssertionError('launcher reached')))
    watchdog = stack.enter_context(mock.patch('agent_chat.cli.threading.Thread', side_effect=AssertionError('watchdog reached')))
    lifecycle = stack.enter_context(mock.patch('agent_chat.cli._RunLifecycleEvents', side_effect=AssertionError('lifecycle reached')))
    signals = stack.enter_context(mock.patch('agent_chat.cli.signal.signal', side_effect=AssertionError('signal reached')))
    try:
        cli.main(['--server', 'http://127.0.0.1:1', '--session', 'manifest-owner',
                  'run', '--batch-manifest', sys.argv[1], '--lifecycle-events',
                  '--admission-level', '0', 'batch', '--', sys.executable, '-c', 'pass'])
    except CoordError as error:
        assert str(error) == 'batch manifest must be a bounded private owner file', str(error)
    else:
        raise AssertionError('non-regular manifest accepted')
    for guard in (api, launcher, watchdog, lifecycle, signals):
        guard.assert_not_called()
print('rejected-before-guard-activity')
"""
        for path in (fifo, directory):
            with self.subTest(kind=path.name):
                result = subprocess.run([sys.executable, '-c', code, str(path)], env=self.env,
                                        capture_output=True, text=True, timeout=5)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout, 'rejected-before-guard-activity\n')

    def test_regular_private_manifest_reaches_only_mocked_admission(self):
        items = [dict(resource='shared', session_id='manifest-owner', host_id='manifest-host',
                      reservation_id='fixture-reservation', token='fixture-token')]
        manifest = self.root / 'private-regular.json'
        manifest.write_text(json.dumps(items), encoding='utf-8')
        manifest.chmod(0o600)
        class AdmissionReached(Exception):
            pass
        original_popen = subprocess.Popen
        def parser_only(command, *args, **kwargs):
            if command[0] != 'jq':
                self.fail('guard launcher reached')
            return original_popen(command, *args, **kwargs)
        with mock.patch.dict(os.environ, self.env, clear=True), \
                mock.patch('agent_chat.remote.client_host_id', return_value='manifest-host'), \
                mock.patch.object(HttpClient, 'call', side_effect=AdmissionReached) as api, \
                mock.patch('agent_chat.cli.subprocess.Popen', side_effect=parser_only), \
                mock.patch('agent_chat.cli.threading.Thread') as watchdog:
            with self.assertRaises(AdmissionReached):
                cli.main(['--server', 'http://127.0.0.1:1', '--session', 'manifest-owner',
                          'run', '--batch-manifest', str(manifest), 'batch', '--',
                          sys.executable, '-c', 'pass'])
        api.assert_called_once()
        self.assertEqual(api.call_args.args[1]['op'], 'begin-guard-batch')
        self.assertEqual(api.call_args.args[1]['params']['tuples'], items)
        watchdog.assert_not_called()


class RemoteLeaseRenewalTests(TestCase):
    """Real CLI/dispatcher behavior with a service-free synthetic transport."""

    def setUp(self):
        from agent_chat.private_artifacts import atomic_capture
        from agent_chat.remote_service import dispatch
        temporary = tempfile.TemporaryDirectory(prefix='remote-lease-0061-')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.coord = Coordinator(self.root / 'state.sqlite3', 'owner')
        self.addCleanup(self.coord.close)
        self.dispatch = dispatch
        self.calls = []
        self.server = 'http://127.0.0.1:1'
        self.request('register', agent='owner')
        self.coord.context()
        self.claim = self.request('request', resource='shared', minutes=5)
        self.capability = self.root / 'existing-hold.json'
        self.tuple = dict(resource='shared', session_id='owner', host_id='fixture-host',
                          reservation_id=self.claim['reservation_id'], token=self.claim['token'],
                          server=self.server, project='default')
        atomic_capture(self.capability, self.tuple)

    def request(self, operation, **params):
        return self.dispatch(self.coord, dict(op=operation, session='owner', host_id='fixture-host', params=params))

    def invoke(self, capability=None, reservation=None):
        def transport(client, path, payload, **kwargs):
            self.assertEqual(path, '/api/coord')
            self.calls.append(dict(payload))
            return self.dispatch(self.coord, payload)
        output = io.StringIO()
        with mock.patch('agent_chat.remote.client_host_id', return_value='fixture-host'), \
                mock.patch.object(HttpClient, 'call', transport), contextlib.redirect_stdout(output):
            result = cli.main(['--server', self.server, '--project', 'default', '--session', 'owner',
                '--api-token', 'synthetic-test-token', 'renew', 'shared', '--minutes', '2',
                '--reservation-id', reservation or self.claim['reservation_id'],
                '--capability-file', str(capability or self.capability)])
        self.assertEqual(result, 0)
        return output.getvalue()

    def test_cli_explicit_extension_uses_private_tuple_without_rewriting_capture(self):
        original = self.capability.read_bytes()
        before = tuple(self.coord.db.execute("SELECT * FROM resources WHERE name='shared'").fetchone())
        output = self.invoke()
        checked = subprocess.run(['jq', '-e', '.renewal_count == 1 and .renewal_minutes == 2 and (has("token") | not)'],
                                 input=output, capture_output=True, text=True, check=False)
        self.assertEqual(checked.returncode, 0, checked.stderr)
        after = tuple(self.coord.db.execute("SELECT * FROM resources WHERE name='shared'").fetchone())
        self.assertEqual(after[:5], before[:5])
        self.assertEqual(after[5], before[5] + 120)
        self.assertEqual(after[6:], before[6:])
        self.assertEqual(self.capability.read_bytes(), original)
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.calls[0]['params']['token'], self.claim['token'])

    def test_changed_private_binding_refuses_before_transport_and_budget(self):
        from agent_chat.private_artifacts import atomic_capture
        for field in ('resource', 'session_id', 'host_id', 'reservation_id', 'server', 'project'):
            altered = dict(self.tuple, **{field: 'different'})
            capability = self.root / (field + '.json')
            atomic_capture(capability, altered)
            before = tuple(self.coord.db.execute("SELECT * FROM resources WHERE name='shared'").fetchone())
            with self.subTest(field=field), self.assertRaisesRegex(CoordError, 'private capability tuple'):
                self.invoke(capability=capability)
            self.assertEqual(self.calls, [])
            self.assertEqual(tuple(self.coord.db.execute("SELECT * FROM resources WHERE name='shared'").fetchone()), before)
            self.assertEqual(self.coord.db.execute('SELECT COUNT(*) FROM reservation_renewals').fetchone()[0], 0)

    def test_server_host_and_closed_field_contract_refuse_unchanged(self):
        params = dict(resource='shared', minutes=2, reservation_id=self.claim['reservation_id'], token=self.claim['token'])
        before = tuple(self.coord.db.execute("SELECT * FROM resources WHERE name='shared'").fetchone())
        for payload in (dict(op='renew', session='owner', host_id='other-host', params=params),
                        dict(op='renew', session='owner', host_id='fixture-host', params=dict(params, automatic=True))):
            with self.assertRaises(CoordError):
                self.dispatch(self.coord, payload)
            self.assertEqual(tuple(self.coord.db.execute("SELECT * FROM resources WHERE name='shared'").fetchone()), before)
            self.assertEqual(self.coord.db.execute('SELECT COUNT(*) FROM reservation_renewals').fetchone()[0], 0)

    def test_expired_preliminary_host_refusal_preserves_irreversible_latch(self):
        params = dict(resource='shared', minutes=2, reservation_id=self.claim['reservation_id'], token=self.claim['token'])
        before = tuple(self.coord.db.execute("SELECT * FROM resources WHERE name='shared'").fetchone())
        with mock.patch('agent_chat.core._now', return_value=self.claim['deadline'] + 1):
            with self.assertRaisesRegex(CoordError, 'different remote host'):
                self.dispatch(self.coord, dict(op='renew', session='owner', host_id='other-host', params=params))
        after = tuple(self.coord.db.execute("SELECT * FROM resources WHERE name='shared'").fetchone())
        self.assertEqual(after[:6], before[:6])
        self.assertEqual(after[6], 1)
        self.assertEqual(after[7:], before[7:])
        with mock.patch('agent_chat.core._now', return_value=self.claim['deadline'] - 1):
            with self.assertRaisesRegex(CoordError, 'stale'):
                self.request('renew', **params)
        self.assertEqual(tuple(self.coord.db.execute("SELECT * FROM resources WHERE name='shared'").fetchone()), after)
        self.assertEqual(self.coord.db.execute('SELECT COUNT(*) FROM reservation_renewals').fetchone()[0], 0)


class RemoteRunTests(RemoteWebFixture):
    def setUp(self):
        super().setUp()
        self.call('register', agent='alpha')
        self.claim = self.call('request', resource='shared', minutes=5)
        self.env = dict(os.environ, AGENT_CHAT_SERVER=self.url,
                        AGENT_CHAT_API_TOKEN=TOKEN, AGENT_CHAT_SESSION='a',
                        AGENT_CHAT_HOST_ID='mac', AGENT_CHAT_TOKEN=self.claim['token'],
                        AGENT_CHAT_STATE_DIR=str(Path(self.tmp.name)/'client-state'),
                        AGENT_CHAT_DB=str(Path(self.tmp.name)/'must-not-create.sqlite3'))
        from agent_chat.private_artifacts import atomic_capture
        private = Path(self.tmp.name) / 'capabilities'
        private.mkdir(mode=0o700)
        self.capability_path = private / 'shared.json'
        atomic_capture(self.capability_path, dict(resource='shared', session_id='a', host_id='mac',
                       reservation_id=self.claim['reservation_id'], token=self.claim['token'],
                       server=self.url, project='default'))

    def capability_args(self, args):
        args = list(args)
        head = args[:args.index('--')] if '--' in args else args
        if 'run' in head and '--batch-manifest' not in head:
            run_index = head.index('run')
            args[run_index + 1:run_index + 1] = ['--capability-file', str(self.capability_path)]
            if '--reservation-id' not in head:
                args[run_index + 1:run_index + 1] = ['--reservation-id', self.claim['reservation_id']]
        return args

    def batch_manifest(self):
        other = self.call('request', resource='second', minutes=5)
        items = [dict(resource=resource, session_id='a', host_id='mac',
                      reservation_id=claim['reservation_id'], token=claim['token'])
                 for resource, claim in (('shared', self.claim), ('second', other))]
        path = Path(self.tmp.name) / 'batch-private.json'
        path.write_text(json.dumps(items))
        path.chmod(0o600)
        return path

    def test_batch_one_launcher_all_attached_and_closed(self):
        manifest = self.batch_manifest()
        original, calls = HttpClient.call, []
        def capture(client, path, payload):
            calls.append(payload['op'])
            return original(client, path, payload)
        with mock.patch.object(HttpClient, 'call', capture):
            result, output = self.invoke('run', '--batch-manifest', str(manifest), 'batch', '--',
                                         sys.executable, '-c', 'pass')
        self.assertEqual(result, 0)
        runs = self.runs()
        self.assertEqual(len(runs), 2)
        self.assertEqual(len({run['local_pid'] for run in runs}), 1)
        self.assertTrue(all(run['closed_at'] is not None for run in runs))
        self.assertEqual(calls.count('begin-guard-batch'), 1)
        self.assertEqual(calls.count('attach-guard-pid'), 2)
        self.assertEqual(calls.count('close-guard'), 2)
        self.assertEqual(self.lifecycle_query(output,
            '.[0].restored == false and .[0].released == false and .[0].processes_closed == true'),
            'true')
        self.assert_group_dead(runs[0]['local_pid'])

    def test_batch_partial_attach_keeps_gate_closed_and_truthful_rows(self):
        manifest = self.batch_manifest()
        marker = Path(self.tmp.name) / 'must-not-run'
        original, count = HttpClient.call, 0
        def fail_second(client, path, payload):
            nonlocal count
            if payload['op'] == 'attach-guard-pid':
                count += 1
                if count == 2: raise RemoteCoordError('second attach unresolved')
            return original(client, path, payload)
        with mock.patch.object(HttpClient, 'call', fail_second), self.assertRaises(RemoteCoordError):
            self.invoke('run', '--batch-manifest', str(manifest), 'batch', '--', sys.executable, '-c',
                        'from pathlib import Path; Path(' + repr(str(marker)) + ').touch()')
        self.assertFalse(marker.exists())
        runs = self.runs()
        self.assertEqual(sorted(run['local_pid'] == 0 for run in runs), [False, True])
        self.assertTrue(all(run['closed_at'] is None for run in runs))
        self.assert_group_dead(next(run['local_pid'] for run in runs if run['local_pid']))

    def test_batch_invalid_successful_second_attach_keeps_gate_closed(self):
        manifest = self.batch_manifest()
        marker = Path(self.tmp.name) / 'must-not-run'
        original, count, calls = HttpClient.call, 0, []
        def invalid_second(client, path, payload):
            nonlocal count
            calls.append(payload['op'])
            result = original(client, path, payload)
            if payload['op'] == 'attach-guard-pid':
                count += 1
                if count == 2: result = dict(result, run_id='another-run')
            return result
        with mock.patch.object(HttpClient, 'call', invalid_second), self.assertRaisesRegex(CoordError, 'attach response unresolved'):
            self.invoke('run', '--batch-manifest', str(manifest), 'batch', '--', sys.executable, '-c',
                        'from pathlib import Path; Path(' + repr(str(marker)) + ').touch()')
        self.assertFalse(marker.exists())
        runs = self.runs()
        self.assertEqual(len(runs), 2)
        self.assertEqual(len({run['local_pid'] for run in runs}), 1)
        self.assertTrue(all(run['local_pid'] > 0 and run['closed_at'] is None for run in runs))
        self.assertNotIn('close-guard', calls)
        self.assert_group_dead(runs[0]['local_pid'])

    def test_batch_launcher_failure_retains_pid_zero_admissions(self):
        manifest = self.batch_manifest()
        original = subprocess.Popen
        def fail_launcher(command, *args, **kwargs):
            if command[0] == 'jq': return original(command, *args, **kwargs)
            raise OSError('launcher failed')
        with mock.patch('agent_chat.cli.subprocess.Popen', side_effect=fail_launcher):
            with self.assertRaises(OSError):
                self.invoke('run', '--batch-manifest', str(manifest), 'batch', '--', sys.executable, '-c', 'pass')
        self.assertEqual(len(self.runs()), 2)
        self.assertTrue(all(run['local_pid'] == 0 and run['closed_at'] is None for run in self.runs()))

    def test_batch_partial_close_does_not_claim_all_closed(self):
        manifest = self.batch_manifest()
        original, count = HttpClient.call, 0
        def fail_second_close(client, path, payload):
            nonlocal count
            if payload['op'] == 'close-guard':
                count += 1
                if count == 2: raise RemoteCoordError('second close unresolved')
            return original(client, path, payload)
        with mock.patch.object(HttpClient, 'call', fail_second_close), self.assertRaises(RemoteCoordError):
            self.invoke('run', '--batch-manifest', str(manifest), 'batch', '--', sys.executable, '-c', 'pass')
        runs = self.runs()
        self.assertEqual(sum(run['closed_at'] is None for run in runs), 1)
        self.assert_group_dead(runs[0]['local_pid'])

    def invoke(self, *args):
        output = io.StringIO()
        with mock.patch.dict(os.environ, self.env, clear=True), contextlib.redirect_stdout(output):
            result = cli.main(self.capability_args(args))
        return result, output.getvalue()

    def invoke_with_stderr(self, *args):
        output, errors = io.StringIO(), io.StringIO()
        with (mock.patch.dict(os.environ, self.env, clear=True),
              contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors)):
            result = cli.main(self.capability_args(args))
        return result, output.getvalue(), errors.getvalue()

    def lifecycle_query(self, output, expression, *args):
        result = subprocess.run(['jq', '-ser', *args, expression], input=output,
                                text=True, capture_output=True, check=False)
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout.strip()

    def runs(self):
        c = Coordinator(self.db)
        try: return [dict(r) for r in c.db.execute('SELECT * FROM remote_guarded_runs')]
        finally: c.close()

    def assert_group_dead(self, pid):
        self.assertFalse(Coordinator._alive(pid))
        self.assertFalse(Coordinator._alive(pid, True))

    def receipt(self, pids):
        directory = Path(self.tmp.name)
        (directory/'evidence.txt').write_text('Owned process group closed; fixture baseline restored.')
        value = dict(version=1, resource='shared', reservation_id=self.claim['reservation_id'],
                     restored=True, processes_closed=True, closed_at=time.time(),
                     evidence='evidence.txt', pids=pids)
        path=directory/'CLOSED'; path.write_text(json.dumps(value))
        return str(path)

    def expire(self):
        c=Coordinator(self.db)
        try: c.db.execute("UPDATE resources SET deadline=? WHERE name='shared'", (time.time()-1,))
        finally: c.close()

    def guard_status(self):
        result, output = self.invoke('guard-status', 'shared', '--reservation-id', self.claim['reservation_id'])
        self.assertEqual(result, 0)
        return output

    def query_guard_status(self, output, expression, *args):
        result = subprocess.run(['jq', '-er', *args, expression], input=output,
                                text=True, capture_output=True, check=True)
        return result.stdout.strip()

    def test_guard_status_supplies_interrupted_roots_for_truthful_stale_recovery(self):
        self.assertEqual(self.invoke('run', 'shared', '--', sys.executable, '-c', 'pass')[0], 0)
        original = HttpClient.call
        def outage(client, path, payload):
            if payload.get('op') == 'guard-pulse':
                raise RemoteCoordError('injected connection loss')
            return original(client, path, payload)
        with mock.patch.object(HttpClient, 'call', outage), self.assertRaisesRegex(RemoteCoordError, 'connection loss'):
            self.invoke('run', 'shared', '--', sys.executable, '-c', 'import time; time.sleep(60)')
        unattached = self.call('begin-guard', resource='shared', token=self.claim['token'])
        before = self.runs()
        held = self.call('status', resources=['shared'])
        output = self.guard_status()
        self.query_guard_status(output,
            '.resource == "shared" and .reservation_id == $reservation and (.runs | length) == 3'
            ' and (.runs | map(keys) | all(. == ["closed_at", "local_pid", "run_id", "started_at"]))'
            ' and (.runs | any(.closed_at != null))'
            ' and (.runs | any(.run_id == $unattached and .local_pid == 0 and .closed_at == null))',
            '--arg', 'reservation', self.claim['reservation_id'], '--arg', 'unattached', unattached['run_id'])
        root = int(self.query_guard_status(output, '.runs[] | select(.closed_at == null and .local_pid > 0) | .local_pid'))
        self.assert_group_dead(root)
        self.assertNotIn(self.claim['token'], output)
        self.assertNotIn(TOKEN, output)
        self.assertEqual(self.runs(), before)
        self.assertEqual(self.call('status', resources=['shared']), held)
        self.expire()
        self.assertEqual(self.guard_status(), output)
        pids = [int(pid) for pid in self.query_guard_status(output, '.runs[] | select(.local_pid > 0) | .local_pid').splitlines()]
        self.assertEqual(self.invoke('recover', 'shared', '--receipt', self.receipt(pids))[0], 0)
        self.assertEqual(self.call('status', resources=['shared'])['resources'][0]['state'], 'free')

    def test_reservation_result_is_receipt_backed_across_a_real_cli_release(self):
        self.assertEqual(self.call('reservation-result', resource='shared',
                                   reservation_id=self.claim['reservation_id']),
                         {'state': 'pending', 'resource': 'shared',
                          'reservation_id': self.claim['reservation_id'], 'receipt_backed_clear': False})
        self.assertEqual(self.invoke('release', 'shared', '--receipt', self.receipt([]))[0], 0)
        outcome = self.call('reservation-result', resource='shared', reservation_id=self.claim['reservation_id'])
        self.assertEqual(outcome['state'], 'cleared')
        self.assertEqual(outcome['action'], 'release')
        self.assertTrue(outcome['receipt_backed_clear'])
        self.assertNotIn('receipt_json', outcome)

    def test_guard_status_rejects_other_session_and_host_without_changing_hold(self):
        self.call('register', session='b', agent='beta')
        before = self.call('status', resources=['shared'])
        self.env['AGENT_CHAT_SESSION'] = 'b'
        with self.assertRaisesRegex(CoordError, 'owning session'):
            self.guard_status()
        self.env['AGENT_CHAT_SESSION'] = 'a'
        self.env['AGENT_CHAT_HOST_ID'] = 'other-host'
        with self.assertRaisesRegex(RemoteCoordError, 'different remote host'):
            self.guard_status()
        self.assertEqual(self.call('status', resources=['shared']), before)
        self.assertEqual(self.runs(), [])

    def test_guard_status_requires_exact_existing_reservation(self):
        with self.assertRaisesRegex(CoordError, 'does not match'):
            self.invoke('guard-status', 'shared', '--reservation-id', 'old-reservation')
        with self.assertRaisesRegex(CoordError, 'owning session'):
            self.invoke('guard-status', 'missing', '--reservation-id', self.claim['reservation_id'])
        self.query_guard_status(self.guard_status(), '.runs == []')
        self.assertEqual(self.call('status', resources=['shared'])['resources'][0]['reservation_id'], self.claim['reservation_id'])

    def test_guard_status_rejects_reservation_changed_during_readback(self):
        original = HttpClient.call
        def replacement(client, path, payload):
            result = original(client, path, payload)
            if payload.get('op') == 'guard-context':
                result['reservation_id'] = 'replacement-reservation'
            return result
        with mock.patch.object(HttpClient, 'call', replacement), self.assertRaisesRegex(CoordError, 'reservation changed'):
            self.guard_status()
        self.assertEqual(self.runs(), [])
        self.assertEqual(self.call('status', resources=['shared'])['resources'][0]['reservation_id'], self.claim['reservation_id'])

    def test_invalid_token_never_executes_command(self):
        marker=Path(self.tmp.name)/'sentinel'
        self.env['AGENT_CHAT_TOKEN']='invalid'
        from agent_chat.private_artifacts import atomic_capture, read_capability
        atomic_capture(self.capability_path, dict(read_capability(self.capability_path), token='invalid'))
        with self.assertRaises(RemoteCoordError):
            self.invoke('run', 'shared', '--', sys.executable, '-c', 'from pathlib import Path; Path('+repr(str(marker))+').touch()')
        self.assertFalse(marker.exists())
        self.assertEqual(self.runs(), [])
        self.assertFalse(Path(self.env['AGENT_CHAT_DB']).exists())

    def restore_args(self, seconds=10):
        return ['run', '--restore', '--reservation-id', self.claim['reservation_id'],
                '--max-seconds', str(seconds), 'shared', '--']

    def test_restoration_runs_to_closure_then_recovers_with_real_receipt(self):
        self.expire()
        marker = Path(self.tmp.name)/'restored'
        code = 'import time; from pathlib import Path; time.sleep(.6); Path('+repr(str(marker))+').touch()'
        self.assertEqual(self.invoke(*self.restore_args(), sys.executable, '-c', code)[0], 0)
        self.assertTrue(marker.exists())
        run = self.runs()[0]
        self.assertIsNotNone(run['closed_at'])
        self.assert_group_dead(run['local_pid'])
        status = self.call('status', resources=['shared'])['resources'][0]
        self.assertEqual(status['state'], 'stale')
        self.assertEqual(status['reservation_id'], self.claim['reservation_id'])
        self.assertEqual(self.invoke('recover', 'shared', '--receipt', self.receipt([run['local_pid']]))[0], 0)
        self.assertEqual(self.call('status', resources=['shared'])['resources'][0]['state'], 'free')

    def test_restoration_expiry_kills_group_without_releasing_hold(self):
        self.expire()
        with self.assertRaisesRegex(CoordError, 'restoration deadline has expired'):
            self.invoke(*self.restore_args(1), sys.executable, '-c', 'import time; time.sleep(60)')
        run = self.runs()[0]
        self.assert_group_dead(run['local_pid'])
        self.assertIsNone(run['closed_at'])
        self.assertEqual(self.call('status', resources=['shared'])['resources'][0]['state'], 'stale')
        self.assertEqual(self.invoke('recover', 'shared', '--receipt', self.receipt([run['local_pid']]))[0], 0)

    def test_wrong_restoration_id_does_not_execute_command(self):
        self.expire()
        marker = Path(self.tmp.name)/'must-not-execute'
        args = self.restore_args()
        args[3] = 'wrong-reservation'
        from agent_chat.private_artifacts import atomic_capture, read_capability
        atomic_capture(self.capability_path, dict(read_capability(self.capability_path), reservation_id=args[3]))
        with self.assertRaisesRegex(RemoteCoordError, 'does not match'):
            self.invoke(*args, sys.executable, '-c', 'from pathlib import Path; Path('+repr(str(marker))+').touch()')
        self.assertFalse(marker.exists())
        self.assertEqual(self.runs(), [])

    def test_restoration_connection_loss_closes_group_and_preserves_hold(self):
        self.expire()
        original = HttpClient.call
        def outage(client, path, payload):
            if payload.get('op') == 'guard-pulse': raise RemoteCoordError('injected connection loss')
            return original(client, path, payload)
        with mock.patch.object(HttpClient, 'call', outage), self.assertRaisesRegex(RemoteCoordError, 'connection loss'):
            self.invoke(*self.restore_args(), sys.executable, '-c', 'import time; time.sleep(60)')
        run = self.runs()[0]
        self.assert_group_dead(run['local_pid'])
        self.assertIsNone(run['closed_at'])
        self.assertEqual(self.call('status')['resources'][0]['reservation_id'], self.claim['reservation_id'])

    def test_restoration_lost_attach_response_never_executes_command(self):
        self.expire()
        marker = Path(self.tmp.name)/'must-not-execute'
        original = HttpClient.call
        def lose_attach(client, path, payload):
            result = original(client, path, payload)
            if payload.get('op') == 'attach-guard-pid': raise RemoteCoordError('lost attach response')
            return result
        with mock.patch.object(HttpClient, 'call', lose_attach), self.assertRaisesRegex(RemoteCoordError, 'lost attach'):
            self.invoke(*self.restore_args(), sys.executable, '-c', 'from pathlib import Path; Path('+repr(str(marker))+').touch()')
        self.assertFalse(marker.exists())
        run = self.runs()[0]
        self.assert_group_dead(run['local_pid'])
        self.assertIsNone(run['closed_at'])

    def test_restoration_deadline_stops_group_during_blocked_inbox(self):
        self.expire()
        marker = Path(self.tmp.name)/'after-budget'
        original = HttpClient.call
        def delayed_inbox(client, path, payload):
            if payload.get('op') == 'inbox': time.sleep(2)
            return original(client, path, payload)
        code = 'import time; from pathlib import Path; time.sleep(1.4); Path('+repr(str(marker))+').touch(); time.sleep(60)'
        with mock.patch.object(HttpClient, 'call', delayed_inbox), self.assertRaisesRegex(CoordError, 'deadline has expired'):
            self.invoke(*self.restore_args(1), sys.executable, '-c', code)
        self.assertFalse(marker.exists())
        run = self.runs()[0]
        self.assert_group_dead(run['local_pid'])
        self.assertIsNone(run['closed_at'])

    def test_restoration_delayed_successful_attach_never_opens_command_gate(self):
        self.expire()
        marker = Path(self.tmp.name)/'after-attach-budget'
        original = HttpClient.call
        def delayed_attach(client, path, payload):
            result = original(client, path, payload)
            if payload.get('op') == 'attach-guard-pid': time.sleep(1.5)
            return result
        with mock.patch.object(HttpClient, 'call', delayed_attach), self.assertRaisesRegex(CoordError, 'deadline has expired'):
            self.invoke(*self.restore_args(1), sys.executable, '-c', 'from pathlib import Path; Path('+repr(str(marker))+').touch()')
        self.assertFalse(marker.exists())
        run = self.runs()[0]
        self.assert_group_dead(run['local_pid'])
        self.assertIsNone(run['closed_at'])

    def test_restoration_watchdog_does_not_signal_reused_group_after_closure(self):
        self.expire()
        signals = []
        replaced = False
        original = os.killpg
        class DeadlineAtCancel:
            def __init__(self, interval, callback): self.callback = callback; self.fired = False
            def start(self): pass
            def cancel(self):
                if not self.fired:
                    self.fired = True
                    self.callback()  # expiry races immediately after group closure
            def join(self): pass
        def reused_group(pid, sig):
            nonlocal replaced
            if replaced:
                if sig: signals.append((pid, sig))
                # Replacement is alive to signal0 and accepts TERM/KILL.
                return
            try: return original(pid, sig)
            except ProcessLookupError:
                replaced = True
                raise
        with mock.patch('agent_chat.cli.threading.Timer', DeadlineAtCancel), mock.patch('agent_chat.cli.os.killpg', reused_group):
            self.assertEqual(self.invoke(*self.restore_args(), sys.executable, '-c', 'pass')[0], 0)
        self.assertEqual(signals, [])
        self.assert_group_dead(self.runs()[0]['local_pid'])

    def test_run_propagates_remote_identity_preserves_child_flags_and_releases(self):
        marker=Path(self.tmp.name)/'env.json'
        code='import json,os,sys; open('+repr(str(marker))+',"w").write(json.dumps({"env":dict(os.environ),"args":sys.argv[1:]}))'
        result, output=self.invoke('run', 'shared', '--', sys.executable, '-c', code, '--server', 'child-server', '--session', 'child-session')
        self.assertEqual(result, 0)
        captured=json.loads(marker.read_text())
        self.assertEqual(captured['args'], ['--server','child-server','--session','child-session'])
        for key in ('AGENT_CHAT_SERVER','AGENT_CHAT_SESSION','AGENT_CHAT_API_TOKEN','AGENT_CHAT_TOKEN','AGENT_CHAT_HOST_ID'):
            self.assertEqual(captured['env'][key], self.env[key])
        self.assertNotIn('AGENT_CHAT_DB', captured['env'])
        run=self.runs()[0]; self.assertIsNotNone(run['closed_at']); self.assert_group_dead(run['local_pid'])
        self.assertTrue(json.loads(self.invoke('release','shared','--receipt',self.receipt([run['local_pid']]))[1])['released'])

    def test_lifecycle_events_are_opt_in_sanitized_and_ordered(self):
        result, output, errors = self.invoke_with_stderr(
            'run', '--lifecycle-events', '--admission-level', '0', 'shared', '--',
            sys.executable, '-c', 'pass')
        self.assertEqual(result, 0)
        self.assertIn('remote_run_', output)
        self.lifecycle_query(errors,
            'length as $count | $count >= 10'
            ' and map(.sequence) == [range(0; $count)]'
            ' and all(.[]; .pid == $pid and .admission_level == 0'
            ' and .timeout_seconds == 10 and .elapsed_seconds >= 0'
            ' and ((keys - $allowed) | length == 0))'
            ' and [.[0:7][] | .phase] == ["begin","begin","launcher","launcher","attach","attach","child_exit"]'
            ' and [.[0:7][] | .edge] == ["start","end","start","end","start","end","start"]'
            ' and [.[0:7][] | .outcome] == ["started","succeeded","started","launcher_created","started","succeeded","started"]'
            ' and ([.[] | select(.phase == "inbox")] | length % 2 == 0)'
            ' and ([.[] | select(.phase == "pulse")] | length % 2 == 0)'
            ' and [.[-3:][] | .phase] == ["child_exit","close","close"]'
            ' and [.[-3:][] | .edge] == ["end","start","end"]'
            ' and [.[-3:][] | .outcome] == ["child_exit","started","succeeded"]'
            ' and .[0].run_id == null and .[2].launcher_pid == null'
            ' and all(.[1:][]; .run_id | startswith("remote_run_"))'
            ' and all(.[3:][]; .launcher_pid > 0)',
            '--argjson', 'pid', str(os.getpid()), '--argjson', 'allowed',
            '["pid","admission_level","sequence","phase","edge","timeout_seconds","elapsed_seconds","outcome","run_id","launcher_pid","inbox_trace_id"]')
        self.lifecycle_query(errors,
            '([.[] | select(.phase == "inbox") | .inbox_trace_id'
            ' | test("^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")] | all)'
            ' and ([.[] | select(.phase != "inbox" and has("inbox_trace_id"))] | length) == 0')
        for private in (TOKEN, self.claim['token'], self.url, 'pass'):
            self.assertNotIn(private, errors)

        result, _, disabled_errors = self.invoke_with_stderr(
            'run', 'shared', '--', sys.executable, '-c', 'pass')
        self.assertEqual(result, 0)
        self.assertEqual(disabled_errors, '')

    def test_lifecycle_inbox_trace_is_opt_in_and_inbox_only(self):
        original, calls = HttpClient.call, []
        def capture(client, path, payload, *, inbox_trace_id=None):
            calls.append((payload.get('op'), inbox_trace_id))
            if inbox_trace_id is None:
                return original(client, path, payload)
            return original(client, path, payload, inbox_trace_id=inbox_trace_id)
        with mock.patch.object(HttpClient, 'call', capture):
            result, _, errors = self.invoke_with_stderr(
                'run', '--lifecycle-events', '--admission-level', '0', 'shared', '--',
                sys.executable, '-c', 'import time; time.sleep(2)')
        self.assertEqual(result, 0)
        inbox_traces = [trace for op, trace in calls if op == 'inbox']
        self.assertTrue(inbox_traces)
        self.assertTrue(all(trace is not None and uuid.UUID(trace).version == 4 for trace in inbox_traces))
        self.assertGreater(len(inbox_traces), 1)
        self.assertEqual(len(inbox_traces), len(set(inbox_traces)))
        self.assertTrue(all(trace is None for op, trace in calls if op != 'inbox'))
        for trace in inbox_traces:
            self.lifecycle_query(errors,
                '([.[] | select(.phase == "inbox" and .inbox_trace_id == $trace)] | length) == 2'
                ' and ([.[] | select(.phase == "inbox" and .inbox_trace_id == $trace)] | map(.edge)) == ["start", "end"]',
                '--arg', 'trace', trace)

        calls.clear()
        with mock.patch.object(HttpClient, 'call', capture):
            result, _, errors = self.invoke_with_stderr(
                'run', 'shared', '--', sys.executable, '-c', 'import time; time.sleep(1.2)')
        self.assertEqual(result, 0)
        self.assertEqual(errors, '')
        self.assertTrue(calls)
        self.assertTrue(all(trace is None for _, trace in calls))

    def test_http_client_inbox_trace_header_is_v4_and_inbox_only(self):
        class Response:
            def __enter__(self): return self
            def __exit__(self, *unused): return False
            def read(self, *unused): return b'{}'
        client = HttpClient('http://127.0.0.1:8765', 'private-token', project='test-project')
        trace = str(uuid.uuid4())
        with mock.patch.object(client._opener, 'open', return_value=Response()) as open_request:
            self.assertEqual(client.call('/api/coord', {'op': 'inbox'}, inbox_trace_id=trace), {})
            request = open_request.call_args.args[0]
            self.assertEqual(dict((key.lower(), value) for key, value in request.header_items())[INBOX_TRACE_HEADER.lower()], trace)
            self.assertEqual(client.call('/api/coord', {'op': 'inbox'}), {})
            self.assertNotIn(INBOX_TRACE_HEADER.lower(),
                             dict((key.lower(), value) for key, value in open_request.call_args.args[0].header_items()))
            with self.assertRaisesRegex(RemoteCoordError, 'UUIDv4'):
                client.call('/api/coord', {'op': 'inbox'}, inbox_trace_id='not-a-trace')
            with self.assertRaisesRegex(RemoteCoordError, 'only for an inbox'):
                client.call('/api/coord', {'op': 'guard-pulse'}, inbox_trace_id=trace)

    def test_client_file_logging_preserves_machine_stdout_and_privacy(self):
        log_path = Path(self.tmp.name) / 'client.jsonl'
        result, output, errors = self.invoke_with_stderr(
            '--log-level', 'INFO', '--log-file', str(log_path), 'run', '--lifecycle-events',
            '--admission-level', '0', 'shared', '--',
            sys.executable, '-c', 'import time; time.sleep(1.2)')
        self.assertEqual(result, 0)
        self.lifecycle_query(errors, 'length > 0 and all(.[]; .phase != null and .sequence >= 0)')
        self.lifecycle_query(output, 'length == 1 and .[0].exit_code == 0 and (.[0].run_id | startswith("remote_run_"))')
        records = log_path.read_text(encoding='utf-8')
        self.lifecycle_query(records,
            'length > 0 and all(.[]; ((keys - $allowed) | length) == 0'
            ' and (.component == "agent_chat.cli" or .component == "agent_chat.remote")'
            ' and (.event == "operation_start" or .event == "operation_complete")'
            ' and (.severity == "INFO"))',
            '--argjson', 'allowed',
            '["timestamp","monotonic_ns","sequence","component","channel","severity","event","correlation_id","error_category","run_id","inbox_trace_id"]')
        self.lifecycle_query(records,
            '([.[] | select(.component == "agent_chat.cli" and .run_id != null) | .run_id'
            ' | startswith("remote_run_")] | all) and ([.[] | select(.inbox_trace_id != null)] | length) > 0')
        for private in (TOKEN, self.claim['token'], self.url, 'pass'):
            self.assertNotIn(private, records)

    def test_client_logging_error_output_and_transport_privacy(self):
        stdout, stderr = io.StringIO(), io.StringIO()
        with (mock.patch.object(sys, 'argv', ['agent-chat-client', '--log-level', 'INFO', 'status']),
              contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr)):
            self.assertEqual(cli.entrypoint(), 2)
        self.assertEqual(stdout.getvalue(), '')
        self.lifecycle_query(stderr.getvalue(), 'length == 1 and .[0].error == "configuration_invalid"')

        log_path = Path(self.tmp.name) / 'transport.jsonl'
        log_path.touch(mode=0o600)
        runtime = configure(LoggingConfig(level='INFO', path=str(log_path), max_bytes=1048576, backups=1))
        client = HttpClient('http://127.0.0.1:8765', 'private-token', project='test-project',
                            logger=runtime.logger('agent_chat.remote'))
        try:
            with mock.patch.object(client._opener, 'open', side_effect=urllib.error.URLError(TimeoutError('private transport detail'))):
                with self.assertRaises(RemoteCoordError):
                    client.call('/api/coord', {'op': 'inbox'}, inbox_trace_id=str(uuid.uuid4()))
        finally:
            runtime.close()
        records = log_path.read_text(encoding='utf-8')
        self.lifecycle_query(records,
            '([.[] | select(.component == "agent_chat.remote" and .event == "transport_failed")]'
            ' | length) == 1 and ([.[] | select(.component == "agent_chat.remote"'
            ' and .event == "transport_failed")] | .[0].error_category) == "timeout"')
        for private in ('private-token', 'private transport detail', 'test-project'):
            self.assertNotIn(private, records)

    def test_selected_log_sink_failure_does_not_block_dispatch(self):
        """The selected sink is deferred: command dispatch stays authoritative."""
        parent = Path(self.tmp.name) / 'regular-file-parent'
        parent.write_text('preserve me', encoding='utf-8')
        log_path = parent / 'selected.jsonl'
        runtimes = []
        def dispatch(*args):
            runtime = args[-1]
            runtime.logger('agent_chat.cli').event('operation_start')
            runtimes.append(runtime)
            return 17
        output, errors = io.StringIO(), io.StringIO()
        with (mock.patch.object(cli, '_dispatch_main', side_effect=dispatch) as mocked_dispatch,
              contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors)):
            result = cli.main(['--log-level', 'INFO', '--log-file', str(log_path), 'status'])
        self.assertEqual(result, 17)
        self.assertEqual(output.getvalue(), '')
        self.assertEqual(errors.getvalue(), '')
        mocked_dispatch.assert_called_once()
        self.assertEqual(len(runtimes), 1)
        self.assertEqual(runtimes[0].sink_failures, 1)
        self.assertEqual(parent.read_text(encoding='utf-8'), 'preserve me')

    def test_project_success_and_root_failure_log_terminal_outcomes_without_stdout_noise(self):
        log_path = Path(self.tmp.name) / 'project-root.jsonl'
        result, output, errors = self.invoke_with_stderr(
            '--log-level', 'INFO', '--log-file', str(log_path), 'project', 'list')
        self.assertEqual(result, 0)
        self.assertEqual(errors, '')
        self.lifecycle_query(output, 'length == 1 and (.[0].projects | type) == "array"')

        stdout, stderr = io.StringIO(), io.StringIO()
        with (mock.patch.dict(os.environ, self.env, clear=True),
              mock.patch.object(sys, 'argv', ['agent-chat-client', '--log-level', 'INFO',
                                              '--log-file', str(log_path), 'root', 'status']),
              contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr)):
            self.assertEqual(cli.entrypoint(), 2)
        self.assertEqual(stdout.getvalue(), '')
        self.lifecycle_query(stderr.getvalue(), 'length == 1 and (.[0].error | type) == "string"')
        records = log_path.read_text(encoding='utf-8')
        self.lifecycle_query(records,
            '([.[] | select(.component == "agent_chat.cli" and .event == "operation_complete")] | length) >= 1'
            ' and ([.[] | select(.component == "agent_chat.remote" and .event == "operation_failed"'
            ' and .error_category == "validation")] | length) == 1')
        for private in (TOKEN, self.claim['token'], self.url, 'global root own canonical home'):
            self.assertNotIn(private, records)

    def test_remote_terminal_failures_use_fixed_categories_without_response_text(self):
        class Response:
            def __init__(self, data): self.data = data
            def __enter__(self): return self
            def __exit__(self, *unused): return False
            def read(self, *unused): return self.data
        log_path = Path(self.tmp.name) / 'terminal.jsonl'
        log_path.touch(mode=0o600)
        runtime = configure(LoggingConfig(level='ERROR', path=str(log_path), max_bytes=1048576, backups=1))
        client = HttpClient('http://127.0.0.1:8765', 'private-token', project='test-project',
                            logger=runtime.logger('agent_chat.remote'))
        payload = {'op': 'inbox', 'params': {'run_id': 'remote_run_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'}}
        try:
            for data in (b'x' * (MAX_RESPONSE_BYTES + 1), b'private malformed response', b'[]'):
                with mock.patch.object(client._opener, 'open', return_value=Response(data)), self.assertRaises(RemoteCoordError):
                    client.call('/api/coord', payload, inbox_trace_id=str(uuid.uuid4()))
            for code in (400, 401, 403):
                error = urllib.error.HTTPError('http://private.invalid', code, 'private reason', {},
                                               io.BytesIO(b'{"error":"private response text"}'))
                with mock.patch.object(client._opener, 'open', side_effect=error), self.assertRaises(RemoteCoordError):
                    client.call('/api/coord', payload, inbox_trace_id=str(uuid.uuid4()))
        finally:
            runtime.close()
        records = log_path.read_text(encoding='utf-8')
        self.lifecycle_query(records,
            '[.[] | select(.event == "operation_failed") | .error_category] | sort'
            ' == ["authentication","authorization","validation","validation","validation","validation"]')
        self.lifecycle_query(records,
            '[.[] | select(.event == "operation_failed")] | all(.[]; .severity == "ERROR")')
        self.lifecycle_query(records,
            '[.[] | select(.event == "operation_failed")]'
            ' | all(.[]; (.run_id | startswith("remote_run_")) and (.inbox_trace_id | type) == "string")')
        for private in ('private-token', 'private malformed response', 'private response text', 'private.invalid'):
            self.assertNotIn(private, records)

    def test_lifecycle_events_attribute_timeout_without_exception_text(self):
        original = HttpClient.call
        errors = io.StringIO()
        def timeout_on_attach(client, path, payload):
            if payload.get('op') == 'attach-guard-pid':
                try:
                    raise urllib.error.URLError(TimeoutError('private timeout detail'))
                except urllib.error.URLError as error:
                    raise RemoteCoordError('private transport wrapper') from error
            return original(client, path, payload)
        with (mock.patch.object(HttpClient, 'call', timeout_on_attach),
              mock.patch.dict(os.environ, self.env, clear=True),
              contextlib.redirect_stderr(errors), self.assertRaises(RemoteCoordError)):
            cli.main(self.capability_args(['run', '--lifecycle-events', '--admission-level', '2', 'shared', '--',
                                          sys.executable, '-c', 'pass']))
        self.lifecycle_query(errors.getvalue(),
            '[.[] | select(.phase == "attach")] | length == 2'
            ' and [.[].edge] == ["start", "end"]'
            ' and [.[].outcome] == ["started", "timeout"]'
            ' and all(.[]; .admission_level == 2 and (.run_id | startswith("remote_run_")))'
            ' and ([.[] | select(.phase == "child_exit")] | length == 0)')
        self.assertNotIn('private timeout detail', errors.getvalue())
        self.assertNotIn('private transport wrapper', errors.getvalue())
        self.assertNotIn(TOKEN, errors.getvalue())
        self.assertNotIn(self.claim['token'], errors.getvalue())

    def test_lifecycle_events_attribute_wrapped_inbox_timeout_without_exception_text(self):
        original = HttpClient.call
        errors = io.StringIO()
        observed_traces = []
        def timeout_on_inbox(client, path, payload, *, inbox_trace_id=None):
            if payload.get('op') == 'inbox':
                observed_traces.append(inbox_trace_id)
                try:
                    raise urllib.error.URLError(TimeoutError('private inbox timeout detail'))
                except urllib.error.URLError as error:
                    raise RemoteCoordError('private inbox transport wrapper') from error
            return original(client, path, payload)
        with (mock.patch.object(HttpClient, 'call', timeout_on_inbox),
              mock.patch.dict(os.environ, self.env, clear=True),
              contextlib.redirect_stderr(errors), self.assertRaises(RemoteCoordError)):
            cli.main(self.capability_args(['run', '--lifecycle-events', '--admission-level', '3', 'shared', '--',
                                          sys.executable, '-c', 'import time; time.sleep(60)']))
        self.lifecycle_query(errors.getvalue(),
            '([.[] | select(.phase == "inbox")] | length) == 2'
            ' and ([.[] | select(.phase == "inbox")] | map(.edge)) == ["start", "end"]'
            ' and ([.[] | select(.phase == "inbox")] | map(.outcome)) == ["started", "timeout"]'
            ' and ([.[] | select(.phase == "inbox")] | map(.inbox_trace_id) | unique | length) == 1'
            ' and ([.[] | select(.phase == "pulse")] | length) == 0'
            ' and all(.[]; .admission_level == 3)'
            ' and all(.[] | select(.run_id != null); .run_id | startswith("remote_run_"))')
        self.assertNotIn('private inbox timeout detail', errors.getvalue())
        self.assertNotIn('private inbox transport wrapper', errors.getvalue())
        self.assertNotIn(TOKEN, errors.getvalue())
        self.assertNotIn(self.claim['token'], errors.getvalue())
        self.assertEqual(len(observed_traces), 1)
        self.assertEqual(uuid.UUID(observed_traces[0]).version, 4)
        self.lifecycle_query(errors.getvalue(),
            '([.[] | select(.phase == "inbox" and .inbox_trace_id == $trace)] | length) == 2'
            ' and ([.[] | select(.phase == "inbox" and .inbox_trace_id == $trace)] | map(.outcome)) == ["started", "timeout"]',
            '--arg', 'trace', observed_traces[0])

    def test_lifecycle_events_distinguish_received_interruption_and_child_exit(self):
        original_signal = signal.signal
        original_print = builtins.print
        installed, fired, completed_pulses = {}, False, 0
        def record_handler(sig, handler):
            previous = original_signal(sig, handler)
            if getattr(handler, '__name__', None) == 'forward':
                installed[sig] = handler
            return previous
        def interrupt_during_post_pulse_inbox_start(*args, **kwargs):
            nonlocal fired, completed_pulses
            line = args[0] if args else ''
            if isinstance(line, str) and '"phase":"pulse"' in line and '"edge":"end"' in line:
                completed_pulses += 1
            if (isinstance(line, str) and '"phase":"inbox"' in line
                    and '"edge":"start"' in line and completed_pulses and not fired):
                fired = True
                installed[signal.SIGINT](signal.SIGINT)
            return original_print(*args, **kwargs)
        with (mock.patch('agent_chat.cli.signal.signal', record_handler),
              mock.patch('builtins.print', interrupt_during_post_pulse_inbox_start)):
            result, _, errors = self.invoke_with_stderr(
                'run', '--lifecycle-events', '--admission-level', '1', 'shared', '--',
                sys.executable, '-c', 'import time; time.sleep(60)')
        self.assertEqual(result, -signal.SIGTERM)
        self.assertTrue(fired)
        self.lifecycle_query(errors,
            '([.[] | select(.phase == "interruption")] | length) == 2'
            ' and ([.[] | select(.phase == "interruption")] | map(.outcome)) == ["received_interruption", "handled_interruption"]'
            ' and ([.[] | select(.phase == "inbox")] | map(.outcome)) == ["started", "succeeded", "started", "succeeded"]'
            ' and ([.[] | select(.phase == "child_exit" and .edge == "end" and .outcome == "child_exit")] | length == 1)'
            ' and ([.[] | select(.phase == "pulse" and .edge == "end")] | map(.sequence)) as $pulses'
            ' | ([.[] | select(.phase == "inbox" and .edge == "start")] | map(.sequence)) as $inboxes'
            ' | ([.[] | select(.phase == "interruption" and .outcome == "received_interruption")] | map(.sequence)) as $received'
            ' | $pulses[0] < $inboxes[1] and $pulses[-1] < $received[0]')
        self.lifecycle_query(errors,
            '([.[] | select(.phase == "inbox" and has("inbox_trace_id"))] | length) == 4'
            ' and ([.[] | select(.phase != "inbox" and has("inbox_trace_id"))] | length) == 0')

    def test_descendant_is_stopped_even_after_leader_exits(self):
        marker=Path(self.tmp.name)/'child.pid'
        child='import time; time.sleep(60)'
        code='import subprocess,sys; p=subprocess.Popen([sys.executable,"-c",'+repr(child)+']); open('+repr(str(marker))+',"w").write(str(p.pid))'
        try:
            self.assertEqual(self.invoke('run','shared','--',sys.executable,'-c',code)[0], 0)
            run=self.runs()[0]; self.assert_group_dead(run['local_pid'])
            self.assertFalse(Coordinator._alive(int(marker.read_text())))
            self.assertIsNotNone(run['closed_at'])
        finally:
            if marker.exists():
                try: os.kill(int(marker.read_text()), signal.SIGKILL)
                except ProcessLookupError: pass

    def test_connection_loss_kills_group_but_leaves_reservation_for_proven_recovery(self):
        original=HttpClient.call
        def outage(client,path,payload):
            if payload.get('op')=='guard-pulse': raise RemoteCoordError('injected connection loss')
            return original(client,path,payload)
        with mock.patch.object(HttpClient,'call',outage), self.assertRaisesRegex(RemoteCoordError,'connection loss'):
            self.invoke('run','shared','--',sys.executable,'-c','import time; time.sleep(60)')
        run=self.runs()[0]; self.assert_group_dead(run['local_pid'])
        self.assertIsNone(run['closed_at'])
        self.assertEqual(self.call('status')['resources'][0]['owner_session'], 'a')
        self.expire()
        result=self.invoke('recover','shared','--receipt',self.receipt([run['local_pid']]))
        self.assertTrue(json.loads(result[1])['recovered'])

    def test_run_survives_message_delivered_between_inbox_and_guard_pulse(self):
        self.call('register', session='b', agent='beta')
        marker = Path(self.tmp.name) / 'completed'
        proceed = Path(self.tmp.name) / 'pulses-completed'
        original = HttpClient.call
        delivered = False
        message_id = None
        pulses = 0

        def deliver_after_inbox(client, path, payload):
            nonlocal delivered, message_id, pulses
            result = original(client, path, payload)
            if payload.get('op') == 'inbox' and not delivered:
                delivered = True
                message_id = self.call('send', session='b', to='a',
                                       body='Delivered after the run inbox response.')['id']
            if payload.get('op') == 'guard-pulse':
                pulses += 1
                if pulses == 2:
                    proceed.touch()
            return result

        code = ('import pathlib,time\n'
                'proceed = pathlib.Path(' + repr(str(proceed)) + ')\n'
                'deadline = time.monotonic() + 10\n'
                'while not proceed.exists() and time.monotonic() < deadline: time.sleep(.02)\n'
                'assert proceed.exists(), "guard pulses did not complete"\n'
                'pathlib.Path(' + repr(str(marker)) + ').touch()\n')
        stderr = io.StringIO()
        with mock.patch.object(HttpClient, 'call', deliver_after_inbox), contextlib.redirect_stderr(stderr):
            result, _ = self.invoke('run', 'shared', '--', sys.executable, '-c', code)
        self.assertEqual(result, 0)
        self.assertTrue(delivered)
        self.assertGreaterEqual(pulses, 2)
        self.assertTrue(marker.exists())
        self.assertIn(message_id, stderr.getvalue())
        coord = Coordinator(self.db)
        try:
            message = coord.db.execute('SELECT acked_at FROM messages WHERE id=?', (message_id,)).fetchone()
        finally:
            coord.close()
        self.assertIsNone(message['acked_at'])
        self.assertIsNotNone(self.runs()[0]['closed_at'])

    def test_lost_attach_response_never_executes_unconfirmed_command(self):
        marker=Path(self.tmp.name)/'sentinel'
        original=HttpClient.call
        def lost(client,path,payload):
            result=original(client,path,payload)
            if payload.get('op')=='attach-guard-pid': raise RemoteCoordError('lost attach response')
            return result
        with mock.patch.object(HttpClient,'call',lost), self.assertRaises(RemoteCoordError):
            self.invoke('run','shared','--',sys.executable,'-c','from pathlib import Path; Path('+repr(str(marker))+').touch()')
        self.assertFalse(marker.exists())
        run=self.runs()[0]; self.assert_group_dead(run['local_pid'])
        self.expire()
        self.assertEqual(self.invoke('recover','shared','--receipt',self.receipt([run['local_pid']]))[0],0)

    def test_attach_failure_before_server_records_pid_can_recover_unstarted_run(self):
        original=HttpClient.call
        def lost(client,path,payload):
            if payload.get('op')=='attach-guard-pid': raise RemoteCoordError('offline before attach')
            return original(client,path,payload)
        with mock.patch.object(HttpClient,'call',lost), self.assertRaises(RemoteCoordError):
            self.invoke('run','shared','--',sys.executable,'-c','raise Exception("must not run")')
        self.assertEqual(self.runs()[0]['local_pid'],0)
        self.expire()
        self.assertEqual(self.invoke('recover','shared','--receipt',self.receipt([]))[0],0)

    def test_live_descendant_prevents_receipt_even_when_leader_is_dead(self):
        run=self.call('begin-guard', resource='shared',token=self.claim['token'])
        self.call('attach-guard-pid',run_id=run['run_id'],token=self.claim['token'],pid=123456789)
        self.expire()
        receipt=self.receipt([123456789])
        with mock.patch('agent_chat.cli.os.kill',side_effect=ProcessLookupError), mock.patch('agent_chat.cli.os.killpg',return_value=None):
            with self.assertRaisesRegex(CoordError,'process group'):
                self.invoke('recover','shared','--receipt',receipt)
        self.assertIsNone(self.runs()[0]['closed_at'])

    def test_local_caller_cannot_recover_remote_reservation(self):
        c=Coordinator(self.db,'local'); c.register('local')
        self.expire()
        try:
            with self.assertRaisesRegex(CoordError,'remote'):
                c.recover('shared',self.receipt([]))
        finally: c.close()
