"""Owned Darwin child: kernel argv, framework launcher, no service mutation."""
import os
from pathlib import Path
import plistlib
import shlex
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from agent_chat import service
from agent_chat.client_continuity import ClientContinuity


class KernelArgvTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='continuity-argv-', dir=os.environ['TMPDIR'])
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.script = self.root / 'owned client.py'
        self.script.write_text("import select, sys\nprint('ready', flush=True)\nselect.select([sys.stdin], [], [], 15)\n")
        self.launcher = self.root / 'configured python'
        self.launcher.symlink_to(Path(sys._base_executable))
        self.command = [str(self.launcher), '-I', '-S', str(self.script), 'bridge',
                        '--codex-bin', str(self.root / "Application Support/quoted ' codex"),
                        '--codex-server', 'ws://127.0.0.1:1', '--connect-only']
        self.child = subprocess.Popen(self.command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE, text=True)
        self.addCleanup(self.finish)
        self.assertEqual(self.child.stdout.readline().strip(), 'ready')
        self.operations = []
        self.manager = service.ServiceManager(system='Darwin', home=self.root,
            server_bin='/bin/true', run=self.run_private)
        paths = self.manager.paths('client')
        for key in ('config', 'wrapper'):
            paths[key].parent.mkdir(parents=True, exist_ok=True)
        paths['config'].write_bytes(plistlib.dumps(dict(Label='com.agent-chat.client',
            ProgramArguments=[str(paths['wrapper'])], KeepAlive=True)))
        self.wrapper = paths['wrapper']
        self.write_command(self.command)
        self.controller = ClientContinuity(self.manager)

    def finish(self):
        self.child.stdin.close()
        self.child.wait(timeout=5)
        self.child.stdout.close()
        self.child.stderr.close()

    def write_command(self, command):
        self.wrapper.write_text('exec '+shlex.join(command)+'\n')
        self.wrapper.chmod(0o700)

    def run_private(self, command, **kwargs):
        self.operations.append(command)
        if command[:2] == ['launchctl', 'print']:
            return subprocess.CompletedProcess(command, 0, 'pid = '+str(self.child.pid)+'\nruns = 1\n', '')
        if command[0] == 'ps':
            self.assertEqual(command[2], str(self.child.pid))
            return subprocess.run(command, **kwargs)
        if command[:2] == ['/usr/bin/otool', '-L']:
            self.assertEqual(Path(command[2]), self.launcher.resolve())
            return subprocess.run(command, **kwargs)
        raise AssertionError('unexpected service effect: '+repr(command))

    def assert_no_effect(self):
        self.assertFalse(self.controller.journal.exists())
        self.assertFalse(any(cmd[0] == '/bin/kill' for cmd in self.operations))

    def test_framework_launcher_and_space_argument_are_exactly_observed(self):
        result = self.controller.inspect()
        self.assertTrue(result['process_command_matches_config'])
        self.assertEqual(result['process']['pid'], self.child.pid)
        self.assertIsNone(result['runtime_ready'])
        self.assertEqual(result['mode'], 'connect-only')
        self.assert_no_effect()

    def test_actual_option_or_executable_drift_is_rejected_without_effect(self):
        for replacement, index in [('ws://changed.invalid', -2), ('different path', -4), ('/bin/sh', 0)]:
            with self.subTest(index=index):
                command = list(self.command)
                command[index] = replacement
                self.write_command(command)
                with self.assertRaises(service.ServiceError):
                    self.controller.inspect()
                self.assert_no_effect()
        self.write_command(self.command[:-1])
        with self.assertRaisesRegex(service.ServiceError, 'connect-only'):
            self.controller.inspect()
        self.assert_no_effect()

    def test_unavailable_or_changed_kernel_observation_fails_without_effect(self):
        with mock.patch.object(self.controller, '_kernel_command', side_effect=service.ServiceError('kernel observation unavailable')):
            with self.assertRaisesRegex(service.ServiceError, 'kernel observation unavailable'):
                self.controller.inspect()
        self.assert_no_effect()
        observed = self.controller._kernel_command(self.child.pid)
        changed = (observed[0], list(observed[1]))
        changed[1][-2] = 'ws://changed.invalid'
        with mock.patch.object(self.controller, '_kernel_command', side_effect=[observed, changed]):
            with self.assertRaisesRegex(service.ServiceError, 'changed during observation'):
                self.controller.inspect()
        self.assert_no_effect()

    def test_process_generation_change_does_not_become_argument_mismatch(self):
        original = self.controller._process(self.child.pid)
        changed = {**original, 'start': 'different birth'}
        with mock.patch.object(self.controller, '_process', side_effect=[original, changed]):
            with self.assertRaisesRegex(service.ServiceError, 'generation changed'):
                self.controller.inspect()
        self.assert_no_effect()

    def test_unrelated_image_and_unqualified_framework_linkage_are_rejected(self):
        actual = self.controller._kernel_command(self.child.pid)
        with mock.patch.object(self.controller, '_kernel_command', return_value=('/bin/sh', actual[1])):
            with self.assertRaisesRegex(service.ServiceError, 'executable differs'):
                self.controller.inspect()
        self.assert_no_effect()
        original_run = self.controller._run
        def wrong_link(command):
            if command[:2] == ['/usr/bin/otool', '-L']:
                return subprocess.CompletedProcess(command, 0, 'launcher:\n\t/foreign/Python (compatibility version 3.14.0)\n', '')
            return original_run(command)
        with mock.patch.object(self.controller, '_run', side_effect=wrong_link):
            with self.assertRaisesRegex(service.ServiceError, 'does not link'):
                self.controller.inspect()
        self.assert_no_effect()
