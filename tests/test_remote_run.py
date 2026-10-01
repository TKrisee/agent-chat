import contextlib
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from unittest import mock

from agent_chat import cli
from agent_chat.core import Coordinator, CoordError
from agent_chat.remote import HttpClient, RemoteCoordError
from test_remote_web import RemoteWebFixture, TOKEN


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

    def invoke(self, *args):
        output = io.StringIO()
        with mock.patch.dict(os.environ, self.env, clear=True), contextlib.redirect_stdout(output):
            result = cli.main(list(args))
        return result, output.getvalue()

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
