import contextlib
import io
import subprocess
import sqlite3
import unittest
from unittest.mock import patch
from agent_chat import root_local
from agent_chat.core import CoordError
from test_root_local import RootLocalTests

class RootDiscoveryTests(RootLocalTests):
    def test_retained_root_can_be_discovered_without_session_or_claiming_attachment(self):
        self.enroll()
        before = dict(self.roots.config())
        output = io.StringIO()
        with patch.dict('os.environ', {'CODEX_THREAD_ID': 'different-thread'}, clear=False), contextlib.redirect_stdout(output):
            self.assertEqual(root_local.main(['discover']), 0)
        check = subprocess.run(['jq', '-e', '--arg', 'thread', self.thread,
            '.home_session == "root" and .binding_thread == $thread and .conversation_relation == "different" and .attached_proven == false and .conversation_hint_is_authoritative == false'], input=output.getvalue(), text=True, capture_output=True)
        self.assertEqual(check.returncode, 0, check.stderr)
        self.assertEqual(dict(self.roots.config()), before)

    def test_missing_root_and_untrusted_uid_are_distinct_and_safe(self):
        self.assertEqual(root_local.discover()['state'], 'not_enrolled')
        self.enroll()
        with patch('agent_chat.root_local.os.geteuid', return_value=-1):
            with self.assertRaisesRegex(CoordError, 'real and effective UID'):
                root_local.discover()

    def test_missing_or_matching_hint_does_not_prove_attachment(self):
        self.enroll()
        with patch.dict('os.environ', {}, clear=True):
            self.assertEqual(root_local.discover()['conversation_relation'], 'unknown')
        with patch.dict('os.environ', {'CODEX_THREAD_ID': self.thread}, clear=True):
            result = root_local.discover()
        self.assertEqual(result['conversation_relation'], 'same_hint')
        self.assertFalse(result['attached_proven'])

    def test_binding_mismatch_is_not_reported_as_an_attachment(self):
        self.enroll()
        with sqlite3.connect(self.master) as db:
            db.execute("UPDATE bridge_bindings SET thread_id='mismatched' WHERE session_id='root'")
        with self.assertRaisesRegex(CoordError, 'current thread differs'):
            root_local.discover()

def load_tests(loader, tests, pattern):
    return loader.loadTestsFromNames([f'test_root_discovery_private.RootDiscoveryTests.{name}' for name in ('test_retained_root_can_be_discovered_without_session_or_claiming_attachment','test_missing_root_and_untrusted_uid_are_distinct_and_safe','test_missing_or_matching_hint_does_not_prove_attachment','test_binding_mismatch_is_not_reported_as_an_attachment')])
