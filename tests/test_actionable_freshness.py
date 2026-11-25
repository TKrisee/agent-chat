"""Own PO currentness through actual private CLI/HTTP; provider is fixture-only."""
import os
import contextlib
import unittest
from pathlib import Path
import sqlite3
import subprocess
import sys
import uuid

from test_creator_management import CreatorManagementTests
from agent_chat.core import Coordinator, InboxCurrentnessRequired


class ActionableFreshness(CreatorManagementTests):
    def setUp(self):
        super().setUp()
        created, self.worker_thread, _ = self.create_worker('currentness-worker')
        self.worker = created['target_session']
        initial = self.po_call('context', limit=100, max_bytes=1000000)
        self.assertFalse(initial['has_more'])
        self.assertFalse(any(m['body_truncated'] for m in initial['messages']))
        evidence_root = os.environ.get('ACTIONABLE_FRESHNESS_EVIDENCE')
        self.log_root = (Path(evidence_root) if evidence_root else self.private / 'evidence') / (self._testMethodName + '-' + str(uuid.uuid4()))
        self.log_root.mkdir(parents=True)
        if not evidence_root:
            self.assertTrue(self.log_root.is_relative_to(self.private))
        self.counter = 0
        self.request_id = str(uuid.uuid4())
        self.initial_native_calls = len(self.rpc.calls)

    def cli(self, *args, expect=0, host='host'):
        self.counter += 1
        env = dict(os.environ, AGENT_CHAT_HOST_ID=host)
        command = [sys.executable, '-m', 'agent_chat.cli', '--server', self.url,
                   '--project', self.project_id, '--session', self.po, *args]
        result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=15)
        prefix = self.log_root / f'{self.counter:03d}'
        prefix.with_suffix('.stdout').write_text(result.stdout)
        prefix.with_suffix('.stderr').write_text(result.stderr)
        prefix.with_suffix('.argv.txt').write_text('\n'.join(command)+'\n')
        self.assertEqual(result.returncode, expect, result.stderr[:1000])
        return result.stdout, result.stderr

    def jq(self, raw, expression):
        result = subprocess.run(['jq', '-r', expression], input=raw, text=True,
                                capture_output=True, check=True, timeout=5)
        return result.stdout.rstrip('\n')

    def send(self, body):
        return self.po_call('send', session=self.worker, to=self.po, body=body)['id']

    def request_count(self):
        with sqlite3.connect(self.projects.db_path(self.project_id)) as db:
            return db.execute('SELECT count(*) FROM agent_control_requests WHERE id=?',
                              (self.request_id,)).fetchone()[0]

    def refresh(self, expect=0):
        return self.cli('agent-status', '--refresh', '--to', self.worker,
                        '--expected-thread', self.worker_thread,
                        '--request-id', self.request_id, expect=expect)

    def pages(self, max_bytes=4000):
        cursor = None; ids = []; previews = []
        for _ in range(12):
            argv = ['context', '--limit', '2', '--max-bytes', str(max_bytes)]
            if cursor is not None: argv += ['--cursor', str(cursor)]
            raw, _ = self.cli(*argv)
            ids += self.jq(raw, '.messages[].id').splitlines()
            previews += self.jq(raw, '.messages[] | select(.body_truncated == true or .metadata_truncated == true) | .id').splitlines()
            cursor_text = self.jq(raw, '.cursor')
            cursor = None if cursor_text == 'null' else int(cursor_text)
            if self.jq(raw, '.has_more') == 'false': return ids, previews, cursor
        self.fail('private page traversal did not terminate')

    def assert_one_queued_no_native_effect(self):
        self.assertEqual(self.request_count(), 1)
        self.assertEqual(len(self.rpc.calls), self.initial_native_calls)


    def test_later_arrival_returns_only_own_actionable_ids_then_recovers_once(self):
        prior = self.send('Earlier completely consumed source')
        ids, previews, _ = self.pages()
        self.assertEqual(ids, [prior]); self.assertEqual(previews, [])
        self.po_call('register', session='unrelated-fixture', agent='unrelated-fixture')
        foreign = self.po_call('send', session=self.worker, to='unrelated-fixture',
                               body='Private other-recipient body')['id']
        incoming = self.send('Actual later own source; do not disclose this body in an error')
        self.cli('acknowledge', incoming)
        _, error = self.refresh(expect=2)
        self.assertIn('read your inbox after the newest message before changing ownership', error)
        self.assertIn(incoming, error)
        self.assertIn('more_unread=false', error)
        self.assertIn('context without --cursor', error)
        self.assertIn('message ID', error)
        self.assertNotIn(prior, error); self.assertNotIn(foreign, error)
        self.assertNotIn('Actual later own source', error)
        self.assertNotIn('Private other-recipient body', error)
        self.assertEqual(self.request_count(), 0)
        with sqlite3.connect(self.projects.db_path(self.project_id)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM message_reads WHERE message_id=? AND reader_session=?',
                                        (incoming, self.po)).fetchone()[0], 0)
        self.cli('message', incoming)
        result, _ = self.refresh()
        self.assertEqual(self.jq(result, '.refresh.id'), self.request_id)
        self.refresh(); self.assert_one_queued_no_native_effect()
        print('own actionable ID; foreign/body/history excluded; no automatic read; full-read same UUID accepted once')

    def test_bounded_list_reports_more_and_foreign_checked_session_is_redacted(self):
        self.po_call('register', session='unrelated-fixture', agent='unrelated-fixture')
        foreign = self.po_call('send', session=self.worker, to='unrelated-fixture',
                               body='Foreign unread private source')['id']
        own = [self.send('Unread own bounded source '+str(i)) for i in range(11)]
        _, error = self.refresh(expect=2)
        for mid in own[:10]: self.assertIn(mid, error)
        self.assertNotIn(own[10], error); self.assertNotIn(foreign, error)
        self.assertIn('more_unread=true', error)
        self.assertEqual(self.request_count(), 0)
        with contextlib.closing(Coordinator(self.projects.db_path(self.project_id), self.po)) as coord:
            with self.assertRaises(InboxCurrentnessRequired) as caught:
                with coord.tx() as db: coord._fresh(db, 'unrelated-fixture')
            self.assertNotIn(foreign, str(caught.exception))
            self.assertNotIn('own messages missing', str(caught.exception))
        self.assertEqual(len(self.rpc.calls), self.initial_native_calls)
        print('bounded first10 own IDs, explicit more=true; foreign-session guard remains generic and no native effects')


def load_tests(loader, tests, pattern):
    # The inherited fixture's own regressions remain in their owning modules.
    # This module contributes only these two new behavioral contracts.
    return unittest.TestSuite(ActionableFreshness(name) for name in (
        'test_later_arrival_returns_only_own_actionable_ids_then_recovers_once',
        'test_bounded_list_reports_more_and_foreign_checked_session_is_redacted',
    ))
