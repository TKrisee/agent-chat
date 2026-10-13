import os
from pathlib import Path
import shutil
import subprocess
import tempfile
from types import SimpleNamespace
import unittest

from agent_chat.root_owner import RootOwnerMonitor, endpoint_identity


class OwnerMonitorTests(unittest.TestCase):
    def setUp(self):
        self.target = dict(session_id='root-session', thread_id='root-thread', generation=10.5)
        self.records = {42: dict(uid=os.getuid(), start='Mon Oct 12 09:13:14 2026', program='/tmp/codex',
                                args='/tmp/codex --remote ws://127.0.0.1:4500/ resume root-thread')}
        self.monitor = RootOwnerMonitor('ws://127.0.0.1:4500', self.run_utility)

    def run_utility(self, argv, **kwargs):
        if argv[0] == 'pgrep': return SimpleNamespace(returncode=0, stdout='\n'.join(map(str, self.records)))
        pid = int(argv[argv.index('-p') + 1])
        record = self.records.get(pid)
        if not record: return SimpleNamespace(returncode=1, stdout='')
        column = argv[-1]
        output = str(record['uid']) + ' ' + record['start'] if column == 'uid=,lstart=' else record['program'] if column == 'comm=' else record['args']
        return SimpleNamespace(returncode=0, stdout=output)

    def test_exact_resume_endpoint_and_uid_required(self):
        original = self.records[42].copy()
        self.assertEqual(self.monitor.observe(self.target)['state'], 'open')
        for update in (dict(args='/tmp/codex --remote ws://127.0.0.1:4500 root-thread'),
                       dict(args='/tmp/codex --remote ws://127.0.0.1:4500 resume --last'),
                       dict(args='/tmp/codex --remote ws://127.0.0.1:4600 resume root-thread'),
                       dict(args='/tmp/codex --remote ws://127.0.0.1:4500 resume other-thread'),
                       dict(args='/tmp/codex --remote ws://127.0.0.1:4500 resume other --prompt root-thread'),
                       dict(uid=os.getuid() + 1), dict(program='/tmp/other')):
            with self.subTest(update=update):
                self.records[42] = dict(original, **update)
                self.assertEqual(self.monitor.observe(self.target)['state'], 'unknown')

    def test_ambiguous_discovery_does_not_transfer_previously_bound_owner(self):
        bound = self.monitor.observe(self.target)
        self.records[43] = self.records[42].copy()
        self.assertEqual(self.monitor.observe(self.target)['state'], 'unknown')
        self.assertEqual(self.monitor.observe(self.target, bound)['state'], 'open')
        del self.records[42]
        self.assertEqual(self.monitor.observe(self.target, bound)['state'], 'closed')

    def test_pid_reuse_and_root_generation_change_close_old_receipt(self):
        bound = self.monitor.observe(self.target)
        self.records[42]['start'] = 'Mon Oct 12 09:15:14 2026'
        self.assertIn('reused', self.monitor.observe(self.target, bound)['reason'])
        replacement = self.monitor.observe(self.target)
        self.target['generation'] += 1
        self.assertEqual(self.monitor.observe(self.target, replacement)['state'], 'closed')

    def test_process_switching_root_or_endpoint_invalidates_receipt(self):
        bound = self.monitor.observe(self.target)
        self.records[42]['args'] = '/tmp/codex --remote ws://127.0.0.1:4600 resume root-thread'
        self.assertEqual(self.monitor.observe(self.target, bound)['state'], 'closed')

    def test_pid_swap_between_identity_and_command_reads_cannot_grant_owner(self):
        original = self.run_utility
        identity_reads = 0
        def swap(argv, **kwargs):
            nonlocal identity_reads
            if argv[-1] == 'uid=,lstart=':
                identity_reads += 1
                if identity_reads == 2: self.records[42]['start'] = 'Mon Oct 12 09:14:14 2026'
            return original(argv, **kwargs)
        self.monitor.run = swap
        self.assertEqual(self.monitor.observe(self.target)['state'], 'unknown')

    def test_unknown_tool_failure_exposes_no_command_or_environment(self):
        def unavailable(*args, **kwargs): raise PermissionError('fixture private command or token')
        self.monitor.run = unavailable
        result = self.monitor.observe(self.target)
        self.assertEqual(result['state'], 'unknown')
        self.assertNotIn('private', result['reason'])
        self.assertNotIn('token', result['reason'])

    def test_endpoint_normalization_preserves_server_address_and_excludes_credentials(self):
        self.assertEqual(endpoint_identity('ws://127.0.0.1:4500/'), endpoint_identity('ws://127.0.0.1:4500'))
        self.assertNotEqual(endpoint_identity('ws://[::1]:4500'), endpoint_identity('ws://127.0.0.1:4500'))
        for endpoint in ('ws://user:secret@127.0.0.1:4500', 'ws://127.0.0.1:4500/?token=secret', 'wss://127.0.0.1:4500', 'ws://remote.example:4500'):
            self.assertIsNone(endpoint_identity(endpoint))

    def test_controlled_native_cli_process_exit_is_observed_by_real_ps(self):
        compiler = shutil.which('cc')
        if not compiler: self.skipTest('C compiler unavailable for isolated native CLI fixture')
        with tempfile.TemporaryDirectory(prefix='root-owner-fixture-') as directory:
            source = Path(directory) / 'owner.c'
            binary = Path(directory) / 'codex'
            source.write_text('#include <stdio.h>\nint main(void) { while (getchar() != EOF) {} return 0; }\n')
            subprocess.run([compiler, str(source), '-o', str(binary)], check=True, capture_output=True)
            child = subprocess.Popen([str(binary), '--remote', 'ws://127.0.0.1:4500/', 'resume', 'root-thread'], stdin=subprocess.PIPE)
            try:
                def only_fixture(argv, **kwargs):
                    if argv[0] == 'pgrep': return SimpleNamespace(returncode=0, stdout=str(child.pid))
                    return subprocess.run(argv, **kwargs)
                monitor = RootOwnerMonitor('ws://127.0.0.1:4500', only_fixture)
                bound = monitor.observe(self.target)
                self.assertEqual(bound['state'], 'open')
                child.stdin.close()
                child.wait(timeout=3)
                self.assertEqual(monitor.observe(self.target, bound)['state'], 'closed')
            finally:
                if child.poll() is None:
                    child.terminate()
                    child.wait(timeout=3)
