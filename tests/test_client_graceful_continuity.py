import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
import contextlib
import io
import plistlib
import shlex
import stat
import uuid
from unittest import mock

from agent_chat import service


class OldHandlerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='client-continuity-', dir=os.environ['TMPDIR'])
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / 'fixture-owned').touch()
        self.output = (self.root / 'child-output').open('wb')
        self.addCleanup(self.output.close)
        self.child = subprocess.Popen([sys.executable, '-I', '-S', '-B',
            str(Path(__file__).with_name('client_shutdown_fixture.py')), str(self.root), '--connect-only'],
            stdout=self.output, stderr=subprocess.STDOUT,
            env={**os.environ, 'HOME': str(self.root), 'CODEX_HOME': str(self.root / '.codex')})
        self.addCleanup(self.finish_child)
        deadline = time.monotonic()+5
        while not (self.root / ('ready-'+str(self.child.pid))).exists():
            if self.child.poll() is not None or time.monotonic()>deadline:
                self.fail((self.root / 'child-output').read_text())
            time.sleep(.01)

    def finish_child(self):
        (self.root / 'release-cleanup').touch()
        (self.root / 'release-acquire').touch()
        (self.root / 'release-connect').touch()
        if self.child.poll() is None:
            self.child.send_signal(signal.SIGTERM)
        self.child.wait(timeout=5)

    def test_installed_old_connect_only_handler_performs_normal_cleanup(self):
        durable = self.root / 'durable-unknown-and-queue'
        durable.write_bytes(b'unknown job=fixture-job client=fixture-client queue=fixture-queue\n')
        before = durable.read_bytes()
        self.child.send_signal(signal.SIGTERM)
        self.assertEqual(self.child.wait(timeout=5), 0)
        events = (self.root / 'events').read_text().splitlines()
        names = [line.split('\t',1)[1] for line in events]
        for name in ('approval-finish:disconnected', 'approval-finish:response_uncertain',
                     'rpc-closed', 'heartbeat:0', 'release', 'identity-closed',
                     'external-provider-not-stopped', 'main-exit:0'):
            self.assertIn(name, names)
        self.assertLess(names.index('approval-finish:disconnected'), names.index('rpc-closed'))
        self.assertLess(names.index('heartbeat:0'), names.index('release'))
        self.assertEqual(before, durable.read_bytes())

    def test_supported_graceful_command_exists_instead_of_force_restart(self):
        calls=[]
        manager=service.ServiceManager(system='Darwin', home=self.root,
            server_bin='/bin/true', run=lambda command, **kw: calls.append(command))
        manager.restart('client')
        self.assertEqual(calls, [['launchctl','kickstart','-k',
                                'gui/'+str(os.getuid())+'/com.agent-chat.client']])
        # The existing command is deliberately not renamed graceful. No mocked
        # force command is executed against this or another process.
        args=service.parser().parse_args(['graceful-restart','--component','client',
            '--request-id','00000000-0000-4000-8000-000000000001',
            '--expected-pid',str(self.child.pid), '--expected-start','fixture',
            '--expected-config-sha','0'*64])
        self.assertEqual(args.action, 'graceful-restart')

    def test_stop_received_during_acquire_does_not_start_another_dispatch(self):
        self.stop_during('acquire')

    def test_stop_received_during_connect_does_not_start_another_dispatch(self):
        self.stop_during('connect')

    def stop_during(self, boundary):
        (self.root/('pause-'+boundary)).touch()
        deadline=time.monotonic()+3
        while not (self.root/('inside-'+boundary)).exists():
            self.assertLess(time.monotonic(), deadline)
            time.sleep(.01)
        self.child.send_signal(signal.SIGTERM)
        while not (self.root/'signal-observed').exists():
            self.assertLess(time.monotonic(), deadline)
            time.sleep(.01)
        (self.root/('release-'+boundary)).touch()
        self.assertEqual(self.child.wait(timeout=5),0)
        names=[line.split('\t',1)[1] for line in (self.root/'events').read_text().splitlines()]
        following=names[names.index('signal-stop-observed')+1:]
        self.assertNotIn('tick-existing-records-no-input', following)
        self.assertIn('release',following)


class PrivateLaunchd:
    """Service-manager stand-in; only explicitly owned real fixture PIDs."""
    def __init__(self, case):
        self.case = case
        self.current = case.child
        self.children = [self.current]
        self.runs = 7
        self.signal_calls = []
        self.signal_targets = []
        self.lose_response = False
        self.crash_after_signal = False
        self.fail_signal = False
        self.swap_before_signal = False
        self.auto_restart = True
    def spawn(self):
        child = subprocess.Popen([sys.executable, '-I', '-S', '-B',
            str(Path(__file__).with_name('client_shutdown_fixture.py')), str(self.case.root), '--connect-only'],
            stdout=self.case.output, stderr=subprocess.STDOUT,
            env={**os.environ, 'HOME': str(self.case.root), 'CODEX_HOME': str(self.case.root / '.codex')})
        self.current = child
        self.children.append(child)
        self.runs += 1
        return child
    def run(self, command, **kwargs):
        if command[:2] == ['launchctl','print']:
            if self.current.poll() is not None and self.auto_restart:
                self.spawn()
            pid = '' if self.current.poll() is not None else '\tpid = '+str(self.current.pid)+'\n'
            return subprocess.CompletedProcess(command, 0, stdout=pid+'\truns = '+str(self.runs)+'\n', stderr='')
        if command[0] == 'ps':
            self.case.assertIn(int(command[2]), [child.pid for child in self.children])
            return subprocess.run(command, **kwargs)
        if command[:2] == ['/usr/bin/otool', '-L']:
            self.case.assertEqual(Path(command[2]), Path(sys.executable).resolve())
            return subprocess.run(command, **kwargs)
        if command[:3] == ['launchctl','kill','SIGTERM'] or command[:2] == ['/bin/kill','-TERM']:
            self.signal_calls.append(command)
            if self.swap_before_signal:
                # External clean exit after the controller's final observation.
                # Rebinding the launchd label must not redirect its old request.
                self.current.send_signal(signal.SIGTERM)
                self.current.wait(timeout=5)
                self.spawn()
                self.swap_before_signal = False
            target = self.current.pid if command[0]=='launchctl' else int(command[2])
            self.signal_targets.append(target)
            if self.fail_signal:
                return subprocess.CompletedProcess(command, 1, stdout='', stderr='private signal refusal')
            try:
                os.kill(target, signal.SIGTERM)
                result = subprocess.CompletedProcess(command, 0, stdout='', stderr='')
            except ProcessLookupError:
                result = subprocess.CompletedProcess(command, 1, stdout='', stderr='old private generation exited')
            if self.crash_after_signal:
                raise SystemExit('private controller interruption after signal')
            if self.lose_response:
                raise subprocess.TimeoutExpired(command, 5)
            return result
        raise AssertionError('unexpected service operation: '+repr(command))
    def close(self):
        self.auto_restart = False
        (self.case.root/'release-cleanup').touch()
        for child in self.children:
            if child.poll() is None: child.send_signal(signal.SIGTERM)
            child.wait(timeout=5)


class GracefulControllerTests(OldHandlerTests):
    def setUp(self):
        super().setUp()
        from agent_chat.client_continuity import ClientContinuity
        self.launchd = PrivateLaunchd(self)
        self.addCleanup(self.launchd.close)
        self.manager = service.ServiceManager(system='Darwin', home=self.root,
            server_bin='/bin/true', run=self.launchd.run)
        paths = self.manager.paths('client')
        for path in (paths['config'], paths['wrapper']):
            path.parent.mkdir(parents=True, exist_ok=True)
        paths['wrapper'].write_text('exec ' + shlex.join([
            sys.executable, '-I', '-S', '-B',
            str(Path(__file__).with_name('client_shutdown_fixture.py')), str(self.root), '--connect-only'])+'\n')
        paths['wrapper'].chmod(0o700)
        with paths['config'].open('wb') as stream:
            plistlib.dump(dict(Label='com.agent-chat.client', KeepAlive=True,
                ProgramArguments=[str(paths['wrapper'])]), stream)
        paths['config'].chmod(0o600)
        self.controller = ClientContinuity(self.manager)
        self.pin = self.controller.inspect()
        self.request = str(uuid.uuid4())
        self.params = dict(expected_pid=self.pin['process']['pid'], expected_start=self.pin['process']['start'],
                           expected_config_sha=self.pin['config_sha256'])

    def test_cli_normal_cleanup_same_identity_and_no_repeated_signal(self):
        # Independent inert provider-process stand-in; no native RPC is implied.
        provider = subprocess.Popen([sys.executable,'-I','-S','-B','-c','import sys; sys.stdin.buffer.read()'], stdin=subprocess.PIPE)
        def close_provider():
            provider.stdin.close()
            provider.wait(timeout=5)
        self.addCleanup(close_provider)
        provider_pid = provider.pid
        durable = self.root/'durable-unknown-and-queue'
        durable.write_bytes(b'unknown job=fixture-job client=fixture-client queue=fixture-queue\n')
        before = durable.read_bytes()
        identity_before = (self.root/'identity.json').read_bytes()
        output = io.StringIO()
        with mock.patch.object(service, 'ServiceManager', return_value=self.manager), contextlib.redirect_stdout(output):
            rc = service._main(['graceful-restart','--component','client','--request-id',self.request,
                '--expected-pid',str(self.params['expected_pid']), '--expected-start',self.params['expected_start'],
                '--expected-config-sha',self.params['expected_config_sha'], '--graceful-timeout','3'])
        self.assertEqual(rc, 0)
        phase = subprocess.run(['jq','-er','.phase'],input=output.getvalue(), text=True, capture_output=True,check=True).stdout.strip()
        self.assertEqual(phase, 'new_generation_observed')
        result = self.controller.restart(self.request, **self.params, timeout=0)
        self.assertTrue(result['old_client_exited'])
        self.assertNotEqual(result['new_process']['pid'], self.child.pid)
        self.assertIsNone(result['reconnect_observed'])
        self.assertIsNone(result['runtime_ready'])
        self.assertEqual(result['approval_state'], 'not_inventoried')
        self.assertEqual(len(self.launchd.signal_calls), 1)
        self.assertEqual(self.launchd.signal_calls[0], ['/bin/kill','-TERM',str(self.child.pid)])
        self.assertIsNone(provider.poll())
        self.assertEqual(provider.pid,provider_pid)
        self.assertEqual(self.controller.status(self.request)['phase'], 'new_generation_observed')
        self.assertEqual(len(self.launchd.signal_calls), 1)
        self.assertEqual(before, durable.read_bytes())
        deadline=time.monotonic()+3
        while not (self.root/('ready-'+str(result['new_process']['pid']))).exists():
            self.assertLess(time.monotonic(),deadline)
            time.sleep(.01)
        self.assertEqual((self.root/'identity.json').read_bytes(),identity_before)
        opened=[line.split('\t',1)[1] for line in (self.root/'events').read_text().splitlines()
                if '\tidentity-open:' in line]
        self.assertEqual(len(opened),2)
        self.assertEqual(opened[0],opened[1])
        old_events=[line.split('\t',1)[1] for line in (self.root/'events').read_text().splitlines()
                    if line.split('\t')[0] == str(self.child.pid)]
        self.assertIn('approval-finish:disconnected',old_events)
        self.assertIn('approval-finish:response_uncertain',old_events)
        self.assertIn('identity-closed',old_events)
        self.assertIn('external-provider-not-stopped',old_events)
        with self.assertRaisesRegex(service.ServiceError, 'replay conflicts'):
            self.controller.restart(self.request, **{**self.params,'expected_start':'different'}, timeout=0)

    def wait_for_private_replacement(self):
        # A spawned Darwin Python may still be transitioning launcher -> image.
        # Reconcile the same request only after this owned child's actual marker.
        deadline = time.monotonic()+5
        while not (self.root/('ready-'+str(self.launchd.current.pid))).exists():
            self.assertIsNone(self.launchd.current.poll())
            self.assertLess(time.monotonic(), deadline)
            time.sleep(.01)

    def test_lost_signal_response_timeout_and_same_request_reconcile_never_force(self):
        (self.root/'block-cleanup').touch()
        self.launchd.lose_response = True
        result=self.controller.restart(self.request, **self.params, timeout=.1)
        self.assertTrue(result['deadline_reached'])
        self.assertFalse(result['old_client_exited'])
        self.assertEqual(result['signal_state'], 'uncertain')
        self.assertEqual(self.controller.restart(self.request, **self.params,timeout=0)['signal_state'], 'uncertain')
        self.assertEqual(len(self.launchd.signal_calls), 1)
        with self.assertRaisesRegex(service.ServiceError, 'already has a request'):
            self.controller.restart(str(uuid.uuid4()), **self.params,timeout=0)
        (self.root/'release-cleanup').touch()
        self.child.wait(timeout=5)
        status = self.controller.status(self.request)
        if status['phase'] == 'outcome_unknown':
            self.assertTrue(status['old_client_exited'])
            self.assertIn('observation_error', status)
        self.wait_for_private_replacement()
        status = self.controller.status(self.request)
        self.assertEqual(status['phase'], 'new_generation_observed', status)
        self.assertEqual(len(self.launchd.signal_calls), 1)

    def test_wrong_pins_or_failed_signal_have_no_force_fallback(self):
        with self.assertRaisesRegex(service.ServiceError, 'differs from supplied pins'):
            self.controller.restart(self.request, **{**self.params,'expected_config_sha':'0'*64},timeout=0)
        self.assertEqual(self.launchd.signal_calls, [])
        self.launchd.fail_signal = True
        result = self.controller.restart(self.request, **self.params,timeout=0)
        self.assertEqual(result['signal_state'], 'uncertain')
        self.assertFalse(result['old_client_exited'])
        self.assertEqual(len(self.launchd.signal_calls), 1)
        self.controller.restart(self.request, **self.params,timeout=0)
        self.assertEqual(len(self.launchd.signal_calls), 1)

    def test_label_generation_race_never_signals_replacement(self):
        self.launchd.swap_before_signal = True
        result = self.controller.restart(self.request, **self.params,timeout=.1)
        self.assertEqual(self.launchd.signal_targets, [self.child.pid])
        self.assertIsNone(result['runtime_ready'])
        self.assertIsNone(self.launchd.current.poll())

    def test_interrupted_controller_retains_intent_without_resending(self):
        self.launchd.crash_after_signal = True
        with self.assertRaisesRegex(SystemExit, 'controller interruption'):
            self.controller.restart(self.request, **self.params,timeout=0)
        self.child.wait(timeout=5)
        from agent_chat.client_continuity import ClientContinuity
        resumed = ClientContinuity(self.manager)
        result=resumed.restart(self.request, **self.params, timeout=0)
        self.assertEqual(result['signal_state'],'intent')
        if result['phase'] == 'outcome_unknown':
            self.assertTrue(result['old_client_exited'])
            self.assertIn('observation_error', result)
        self.wait_for_private_replacement()
        result = resumed.status(self.request)
        self.assertEqual(result['signal_state'], 'intent')
        self.assertEqual(result['phase'],'new_generation_observed',result)
        self.assertEqual(len(self.launchd.signal_calls),1)

    def test_disk_connect_only_is_not_proof_of_running_arguments(self):
        wrapper = self.manager.paths('client')['wrapper']
        wrapper.write_text(wrapper.read_text().rstrip()+' --codex-server ws://changed.invalid\n')
        with self.assertRaisesRegex(service.ServiceError, 'running client arguments'):
            self.controller.inspect()
        self.assertEqual(self.launchd.signal_calls, [])


class OutcomeSemanticsTests(unittest.TestCase):
    def test_ps_error_is_unknown_not_missing_process(self):
        from agent_chat.client_continuity import ClientContinuity
        controller=object.__new__(ClientContinuity)
        controller.target='private-service'
        controller._run=mock.Mock(return_value=subprocess.CompletedProcess([],1,'','ps: observation unavailable'))
        controller.inspect=mock.Mock()
        request=dict(request_id='fixture-request',old_pid=123,old_start='same-birth',
                     signal_state='uncertain',error='private lost response')
        result=controller._observe(request)
        self.assertIsNone(result['old_client_exited'])
        self.assertEqual(result['phase'],'outcome_unknown')
        self.assertIn('observation unavailable',result['observation_error'])
        controller.inspect.assert_not_called()

    def test_true_missing_process_remains_distinct(self):
        from agent_chat.client_continuity import ClientContinuity
        controller=object.__new__(ClientContinuity)
        controller._run=mock.Mock(return_value=subprocess.CompletedProcess([],1,'',''))
        self.assertIsNone(controller._process(123))

    def test_ownership_mismatch_is_not_process_exit(self):
        from agent_chat.client_continuity import ClientContinuity
        controller=object.__new__(ClientContinuity)
        controller.target='private-service'
        controller._process=mock.Mock(return_value=dict(pid=123,uid=os.getuid()+1,start='same-birth'))
        controller.inspect=mock.Mock(side_effect=service.ServiceError('ownership observation unavailable'))
        result=controller._observe(dict(request_id='fixture-request',old_pid=123,old_start='same-birth',
                                        signal_state='uncertain',error='private lost response'))
        self.assertFalse(result['old_client_exited'])
        self.assertEqual(result['phase'],'outcome_unknown')
        self.assertIsNone(result['runtime_ready'])


class JournalDurabilityTests(unittest.TestCase):
    def setUp(self):
        from agent_chat.client_continuity import ClientContinuity
        self.temp=tempfile.TemporaryDirectory(prefix='continuity-durability-',dir=os.environ['TMPDIR'])
        self.addCleanup(self.temp.cleanup)
        self.home=Path(self.temp.name)
        self.controller=ClientContinuity(service.ServiceManager(system='Darwin',home=self.home,server_bin='/bin/true'))
        self.process=dict(pid=12345,uid=os.getuid(),start='private-birth')
        self.controller.inspect=mock.Mock(return_value=dict(process=self.process,service_runs=4,config_sha256='a'*64))
        self.controller._process=mock.Mock(return_value=self.process)
        self.controller.status=mock.Mock(return_value=dict(phase='new_generation_observed'))
        self.request=str(uuid.uuid4())
        self.params=dict(expected_pid=12345,expected_start='private-birth',expected_config_sha='a'*64,timeout=0)
        self.synced=[]
        self.real_fsync=os.fsync

    def sync(self,fd):
        info=os.fstat(fd)
        if stat.S_ISDIR(info.st_mode): self.synced.append((info.st_dev,info.st_ino))
        return self.real_fsync(fd)

    def effect(self,command):
        self.assertEqual(command,['/bin/kill','-TERM','12345'])
        # No signal is sent. This is the controller's exact effect boundary,
        # after real private directory/file creation and SQLite intent commit.
        expected=[]
        parent=self.home
        expected.append(parent)
        for part in self.controller.journal.parent.relative_to(self.home).parts:
            parent=parent/part
            expected.append(parent)
        missing=[str(path) for path in expected if (path.stat().st_dev,path.stat().st_ino) not in self.synced]
        self.assertEqual(missing,[], 'directory entry not durable before signal')
        return subprocess.CompletedProcess(command,0,'','')

    def test_new_ancestor_and_journal_entries_synced_before_effect(self):
        self.controller._run=mock.Mock(side_effect=self.effect)
        with mock.patch.object(os,'fsync',side_effect=self.sync):
            self.controller.restart(self.request,**self.params)
        self.controller._run.assert_called_once()

    def test_failed_parent_sync_has_no_effect_and_retry_syncs_existing_entries(self):
        self.controller._run=mock.Mock(side_effect=self.effect)
        def refused(fd):
            if stat.S_ISDIR(os.fstat(fd).st_mode): raise OSError('private fsync failure')
            return self.real_fsync(fd)
        with mock.patch.object(os,'fsync',side_effect=refused), self.assertRaisesRegex(OSError,'fsync failure'):
            self.controller.restart(self.request,**self.params)
        self.controller._run.assert_not_called()
        with mock.patch.object(os,'fsync',side_effect=self.sync):
            self.controller.restart(self.request,**self.params)
        self.controller._run.assert_called_once()


def load_tests(loader, tests, pattern):
    result=unittest.TestSuite()
    for cls in (OldHandlerTests, GracefulControllerTests, OutcomeSemanticsTests, JournalDurabilityTests):
        for name in cls.__dict__:
            if name.startswith('test_'):result.addTest(cls(name))
    return result
