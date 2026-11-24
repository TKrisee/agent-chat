"""Standalone canonical binding tests; never opens an installed store."""
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


class OriginalOwnerPreflightEntryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.launcher = self.root / 'checkout/bin/agent-chat-original-owner-preflight'
        self.launcher.parent.mkdir(parents=True)
        shutil.copy2(Path(__file__).resolve().parents[1] / 'bin/agent-chat-original-owner-preflight', self.launcher)
        package = self.root / 'checkout/src/agent_chat'
        package.mkdir(parents=True)
        (package / '__init__.py').write_text('')
        (package / 'original_owner_disposition.py').write_text(
            "def main(argv):\n    assert argv == ['preflight']\n    print('canonical preflight only')\n    return 7\n")
        decoy = self.root / 'decoy/agent_chat'
        decoy.mkdir(parents=True)
        (decoy / '__init__.py').write_text("raise RuntimeError('untrusted ambient package loaded')")
        fakebin = self.root / 'fakebin'
        fakebin.mkdir()
        fakepython = fakebin / 'python3'
        fakepython.write_text('#!/bin/sh\nexit 99\n')
        fakepython.chmod(0o755)
        self.env = dict(os.environ, PYTHONPATH=str(decoy.parent), PATH=str(fakebin))

    def invoke(self, *args):
        return subprocess.run(['/opt/homebrew/opt/python@3.14/bin/python3.14', str(self.launcher), *args], cwd=self.root,
                              env=self.env, capture_output=True, text=True)

    def test_exact_canonical_preflight_dispatch_and_exit_are_preserved(self):
        result = self.invoke()
        self.assertEqual(result.returncode, 7)
        self.assertEqual(result.stdout, 'canonical preflight only\n')
        self.assertEqual(result.stderr, '')

    def test_help_does_not_import_or_execute_a_custodian(self):
        result = self.invoke('--help')
        self.assertEqual(result.returncode, 0)
        self.assertIn('Read-only exact original-owner preflight', result.stdout)
        self.assertNotIn('canonical preflight only', result.stdout)

    def test_mutation_and_routing_arguments_are_rejected_before_import(self):
        for args in [('quarantine-failed',), ('--db', '/untrusted/store'), ('--request-id', 'unused')]:
            with self.subTest(args=args):
                result = self.invoke(*args)
                self.assertEqual(result.returncode, 2)
                self.assertEqual(result.stdout, '')
                self.assertIn('unrecognized arguments', result.stderr)


    def test_no_executable_path_fallback_and_absolute_interpreter_ignores_path(self):
        self.assertEqual(self.launcher.stat().st_mode & 0o111, 0)
        with self.assertRaises(PermissionError):
            subprocess.run([str(self.launcher)], env=self.env, check=True)
        self.assertEqual(self.invoke().returncode, 7)
