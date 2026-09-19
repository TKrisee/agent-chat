#!/usr/bin/env python3
"""Exercise the real CLI with isolated databases and harmless child processes."""
from __future__ import annotations

import concurrent.futures
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
CLI = ROOT / 'bin' / 'agent-chat'
sys.path.insert(0, str(ROOT / 'src'))
import agent_chat.core


class CoordTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='agent-chat-test-')
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.db = self.root / 'state.sqlite3'
        self.env = os.environ.copy()
        self.env.pop('AGENT_CHAT_TOKEN', None)
        self.env.update(AGENT_CHAT_DB=str(self.db), AGENT_CHAT_SESSION='a')
        self.cli('register', '--agent', 'alpha')
        self.cli('register', '--agent', 'beta', session='b')

    def cli(self, *args, session='a', token=None, check=True):
        env = dict(self.env, AGENT_CHAT_SESSION=session)
        if token is not None:
            env['AGENT_CHAT_TOKEN'] = token
        result = subprocess.run(
            [str(CLI), *args], cwd=ROOT, env=env, text=True,
            capture_output=True, timeout=20,
        )
        if check:
            self.assertEqual(result.returncode, 0, result.stderr or result.stdout)
        try:
            value = json.loads(result.stdout)
        except json.JSONDecodeError:
            value = None
        return result, value

    def request(self, name='shared', session='a'):
        return self.cli('request', name, '--minutes', '5', session=session)[1]

    def status(self, name='shared'):
        return next(row for row in self.cli('status')[1]['resources']
                    if row['resource'] == name)

    def send(self, recipient='a', sender='b'):
        body = self.root / 'message.md'
        body.write_text('Please coordinate this change.\n')
        return self.cli('send', '--to', recipient, '--body-file', str(body),
                        session=sender)[1]['id']

    def receipt(self, claim, **changes):
        evidence = self.root / 'restoration.txt'
        evidence.write_text('Verified baseline restoration and owned process closure.\n')
        data = {
            'version': 1, 'resource': claim['resource'],
            'reservation_id': claim['reservation_id'], 'restored': True,
            'processes_closed': True, 'closed_at': time.time(),
            'evidence': str(evidence), 'pids': [],
        }
        data.update(changes)
        path = self.root / ('receipt-' + str(time.time_ns()) + '.json')
        path.write_text(json.dumps(data))
        return path

    def release(self, claim, session='a', receipt=None, check=True):
        return self.cli('release', claim['resource'], '--receipt',
                        str(receipt or self.receipt(claim)), session=session,
                        token=claim['token'], check=check)

    def expire(self, name='shared'):
        with sqlite3.connect(self.db) as db:
            db.execute('UPDATE resources SET deadline=? WHERE name=?',
                       (time.time() - 1, name))

    def launch(self, claim, code):
        env = dict(self.env, AGENT_CHAT_TOKEN=claim['token'])
        process = subprocess.Popen(
            [str(CLI), 'run', claim['resource'], '--', sys.executable, '-c', code],
            env=env, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True,
        )
        def cleanup():
            if process.poll() is None:
                process.terminate()
            try:
                process.communicate(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate(timeout=5)
        self.addCleanup(cleanup)
        return process

    def await_file(self, path, process):
        deadline = time.monotonic() + 5
        while not path.exists() and time.monotonic() < deadline:
            if process.poll() is not None:
                self.fail('guard exited before child started: ' + str(process.communicate()))
            time.sleep(.025)
        self.assertTrue(path.exists(), 'child did not start')

    def test_registration_and_recipient_only_explicit_acknowledgements(self):
        self.cli('register', '--agent', 'alpha')
        failed, _ = self.cli('register', '--agent', 'renamed', check=False)
        self.assertNotEqual(failed.returncode, 0)
        message = self.send()
        self.assertEqual(self.cli('status')[1]['resources'], [])
        for _ in range(2):
            inbox = self.cli('inbox', '--agent', 'alpha')[1]['messages']
            self.assertEqual([item['id'] for item in inbox], [message])
            self.assertIsNone(inbox[0]['acked_at'])
        self.assertEqual(self.cli('inbox', session='b')[1]['messages'], [])
        failed, _ = self.cli('acknowledge', message, session='b', check=False)
        self.assertNotEqual(failed.returncode, 0)
        for _ in range(2):
            self.cli('acknowledge', message)
        self.assertEqual(self.cli('inbox')[1]['messages'], [])
        self.assertIsNotNone(self.cli('inbox', '--all')[1]['messages'][0]['acked_at'])
        failed, _ = self.cli('inbox', '--agent', 'beta', check=False)
        self.assertNotEqual(failed.returncode, 0)

    def test_duplicate_labels_require_exact_session_recipient(self):
        self.cli('register', '--agent', 'alpha', session='c')
        body = self.root / 'body'
        body.write_text('Request only')
        failed, _ = self.cli('send', '--to', 'alpha', '--body-file', str(body), check=False)
        self.assertNotEqual(failed.returncode, 0)
        self.send(recipient='c')
        self.assertEqual(len(self.cli('inbox', session='c')[1]['messages']), 1)
        self.assertEqual(len(self.cli('status')[1]['sessions']), 3)

    def test_fifo_idempotence_cancel_and_explicit_handover(self):
        first = self.request()
        self.assertEqual(self.request(), first)
        self.cli('register', '--agent', 'gamma', session='c')
        self.assertEqual(self.request(session='b')['queue_position'], 1)
        self.assertEqual(self.request(session='b')['queue_position'], 1)
        self.assertEqual(self.request(session='c')['queue_position'], 2)
        self.release(first)
        self.assertEqual(self.status()['state'], 'free')
        self.assertEqual(self.request(session='c')['state'], 'queued')
        second = self.request(session='b')
        self.assertEqual(second['state'], 'owned')
        self.assertNotEqual(second['token'], first['token'])
        self.assertEqual(self.cli('cancel', 'shared', session='c')[1]['cancelled'], True)
        self.assertEqual(self.cli('cancel', 'shared', session='c')[1]['cancelled'], False)

    def test_inbox_fence_rechecks_before_ownership_mutation(self):
        self.send()
        failed, _ = self.cli('request', 'shared', '--minutes', '5', check=False)
        self.assertNotEqual(failed.returncode, 0)
        self.cli('inbox')
        claim = self.request()
        self.send()
        failed, _ = self.cli('check', 'shared', token=claim['token'], check=False)
        self.assertNotEqual(failed.returncode, 0)
        self.cli('inbox')
        self.cli('check', 'shared', token=claim['token'])

    def test_eight_simultaneous_processes_create_one_owner_and_seven_waiters(self):
        sessions = ['worker-' + str(i) for i in range(8)]
        for session in sessions:
            self.cli('register', '--agent', session, session=session)
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(lambda sid: self.request(session=sid), sessions))
        self.assertEqual(sum(row['state'] == 'owned' for row in results), 1)
        self.assertEqual(sum(row['state'] == 'queued' for row in results), 7)
        self.assertEqual(len(self.status()['queue']), 7)
        self.assertEqual(sorted(row['queue_position'] for row in results), list(range(8)))

    def test_stale_owner_never_transfers_and_old_token_cannot_run(self):
        claim = self.request()
        self.expire()
        queued = self.request(session='b')
        self.assertEqual(queued['state'], 'queued')
        state = self.status()
        self.assertEqual((state['state'], state['owner_session']), ('stale', 'a'))
        self.assertNotIn('token', state)
        failed, _ = self.cli('check', 'shared', token=claim['token'], check=False)
        self.assertNotEqual(failed.returncode, 0)
        self.cli('recover', 'shared', '--receipt', str(self.receipt(claim)), session='b')
        self.assertEqual(self.status()['state'], 'free')
        self.assertEqual(self.request(session='b')['state'], 'owned')

    def test_stale_state_survives_clock_rollback(self):
        self.request()
        self.expire()
        self.assertEqual(self.status()['state'], 'stale')
        coord = agent_chat.core.Coordinator(self.db, 'a')
        self.addCleanup(coord.close)
        with mock.patch.object(agent_chat.core, '_now', return_value=time.time() - 3600):
            self.assertEqual(coord.status()['resources'][0]['state'], 'stale')

    def test_invalid_receipts_cannot_clear_owner_or_replay(self):
        claim = self.request()
        variants = [
            {'restored': False}, {'processes_closed': False}, {'restored': 1},
            {'reservation_id': 'old-reservation'}, {'pids': [os.getpid()]},
            {'evidence': str(self.root / 'missing')}, {'closed_at': 0},
            {'closed_at': time.time() + 600}, {'closed_at': float('nan')},
        ]
        for changes in variants:
            with self.subTest(changes=changes):
                failed, _ = self.release(claim, receipt=self.receipt(claim, **changes), check=False)
                self.assertNotEqual(failed.returncode, 0)
                self.assertEqual(self.status()['owner_session'], 'a')
        missing = self.root / 'missing-receipt'
        failed, _ = self.release(claim, receipt=missing, check=False)
        self.assertNotEqual(failed.returncode, 0)
        malformed = self.root / 'malformed'
        malformed.write_text('{')
        failed, _ = self.release(claim, receipt=malformed, check=False)
        self.assertNotEqual(failed.returncode, 0)
        receipt = self.receipt(claim)
        failed, _ = self.cli('recover', 'shared', '--receipt', str(receipt), session='b', check=False)
        self.assertNotEqual(failed.returncode, 0)
        self.release(claim, receipt=receipt)
        second = self.request()
        failed, _ = self.release(second, receipt=receipt, check=False)
        self.assertNotEqual(failed.returncode, 0)
        with sqlite3.connect(self.db) as db:
            row = db.execute('SELECT evidence_sha256,receipt_json FROM receipts').fetchone()
        self.assertEqual(len(row[0]), 64)
        self.assertEqual(json.loads(row[1])['reservation_id'], claim['reservation_id'])

    def test_wrong_session_token_and_named_file_aliases(self):
        name = 'file:project/../project/src/example.py'
        claim = self.request(name)
        normalized = 'file:project/src/example.py'
        self.assertEqual(claim['resource'], normalized)
        self.assertEqual(self.request(normalized, session='b')['state'], 'queued')
        failed, _ = self.cli('check', normalized, token=claim['token'], session='b', check=False)
        self.assertNotEqual(failed.returncode, 0)
        failed, _ = self.cli('request', 'file:../outside', '--minutes', '5', check=False)
        self.assertNotEqual(failed.returncode, 0)
        for minutes in ['0', '-1', 'nan', 'inf', '1e308']:
            failed, _ = self.cli('request', 'invalid', '--minutes', minutes, check=False)
            self.assertNotEqual(failed.returncode, 0)

    def test_run_preserves_stdout_exit_status_and_effective_environment(self):
        claim = self.request()
        self.env.update(AGENT_CHAT_ROOT=str(ROOT), ITR_COORD_DB='stale-db', ITR_COORD_SESSION='stale-session',
                        ITR_COORD_TOKEN='stale-token', ITR_COORD_ROOT='stale-root')
        code = ('import json,os; print(json.dumps({name: os.environ[name] for name in '
                '("AGENT_CHAT_DB", "AGENT_CHAT_SESSION", "AGENT_CHAT_TOKEN", "AGENT_CHAT_ROOT", '
                '"ITR_COORD_DB", "ITR_COORD_SESSION", "ITR_COORD_TOKEN", "ITR_COORD_ROOT")})); '
                'raise SystemExit(7)')
        result, _ = self.cli('run', 'shared', '--token', claim['token'], '--',
                             sys.executable, '-c', code, check=False)
        self.assertEqual(result.returncode, 7, result.stderr)
        environment = json.loads(result.stdout)
        self.assertEqual(environment['AGENT_CHAT_SESSION'], 'a')
        self.assertEqual(environment['AGENT_CHAT_TOKEN'], claim['token'])
        self.assertEqual(environment['AGENT_CHAT_DB'], str(self.db.resolve()))
        self.assertEqual(environment['AGENT_CHAT_ROOT'], str(ROOT.resolve()))
        self.assertEqual(environment['ITR_COORD_DB'], environment['AGENT_CHAT_DB'])
        self.assertEqual(environment['ITR_COORD_SESSION'], environment['AGENT_CHAT_SESSION'])
        self.assertEqual(environment['ITR_COORD_TOKEN'], environment['AGENT_CHAT_TOKEN'])
        self.assertEqual(environment['ITR_COORD_ROOT'], environment['AGENT_CHAT_ROOT'])
        self.release(claim)

    def test_live_run_prevents_release_and_signal_closes_child(self):
        claim = self.request()
        marker = self.root / 'pid'
        code = ('import os,pathlib,time; pathlib.Path(' + repr(str(marker)) +
                ').write_text(str(os.getpid())); time.sleep(60)')
        process = self.launch(claim, code)
        self.await_file(marker, process)
        pid = int(marker.read_text())
        failed, _ = self.release(claim, check=False)
        self.assertNotEqual(failed.returncode, 0)
        process.terminate()
        process.communicate(timeout=15)
        self.assertNotEqual(process.returncode, 0)
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)
        self.release(claim)

    def test_guard_polls_messages_without_acknowledging_them(self):
        claim = self.request()
        marker = self.root / 'ready'
        process = self.launch(claim, 'import pathlib,time; pathlib.Path(' +
                              repr(str(marker)) + ').touch(); time.sleep(6)')
        self.await_file(marker, process)
        message = self.send()
        _, stderr = process.communicate(timeout=15)
        self.assertEqual(process.returncode, 0, stderr)
        self.assertIn(message, stderr)
        inbox = self.cli('inbox')[1]['messages']
        self.assertEqual(inbox[0]['id'], message)
        self.assertIsNone(inbox[0]['acked_at'])
        self.release(claim)

    def test_recovery_of_crashed_guard_requires_dead_processes(self):
        claim = self.request()
        completed = subprocess.Popen([sys.executable, '-c', 'pass'])
        completed.wait(timeout=5)
        coord = agent_chat.core.Coordinator(self.db, 'a')
        run, _ = coord.begin_guard('shared', claim['token'], completed.pid)
        coord.close()
        self.expire()
        self.cli('recover', 'shared', '--receipt', str(self.receipt(claim)), session='b')
        self.assertEqual(self.status()['state'], 'free')
        with sqlite3.connect(self.db) as db:
            self.assertIsNotNone(db.execute('SELECT closed_at FROM guarded_runs WHERE run_id=?', (run,)).fetchone()[0])

    def test_expired_running_command_is_closed_before_recovery(self):
        claim = self.request()
        marker = self.root / 'expiring-pid'
        process = self.launch(claim, 'import os,pathlib,time; pathlib.Path(' +
                              repr(str(marker)) + ').write_text(str(os.getpid())); time.sleep(60)')
        self.await_file(marker, process)
        pid = int(marker.read_text())
        self.expire()
        failed, _ = self.cli('recover', 'shared', '--receipt', str(self.receipt(claim)),
                             session='b', check=False)
        self.assertNotEqual(failed.returncode, 0)
        process.communicate(timeout=15)
        self.assertNotEqual(process.returncode, 0)
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)
        self.assertEqual(self.status()['state'], 'stale')
        self.cli('recover', 'shared', '--receipt', str(self.receipt(claim)), session='b')

    def test_receipt_cannot_predate_later_guarded_work(self):
        claim = self.request()
        old_receipt = self.receipt(claim)
        self.cli('run', 'shared', '--', sys.executable, '-c', 'pass', token=claim['token'])
        failed, _ = self.release(claim, receipt=old_receipt, check=False)
        self.assertNotEqual(failed.returncode, 0)
        self.release(claim)

    def test_block_is_explicit_stale_hold(self):
        self.cli('block', 'shared', '--reason', 'Legacy owner must close first')
        self.assertEqual(self.status()['state'], 'stale')
        self.assertEqual(self.request(session='b')['state'], 'queued')
        failed, _ = self.cli('block', 'shared', '--reason', 'overwrite', check=False)
        self.assertNotEqual(failed.returncode, 0)

    def test_default_database_uses_the_callers_project_root(self):
        prior = {name: os.environ.pop(name, None) for name in (
            'AGENT_CHAT_DB', 'ITR_COORD_DB', 'AGENT_CHAT_ROOT', 'ITR_COORD_ROOT',
        )}
        cwd = Path.cwd()
        try:
            os.chdir(self.root)
            coordinator = agent_chat.core.Coordinator(session='default-root')
            try:
                self.assertEqual(coordinator.path, (self.root / '.agent-chat' / 'state.sqlite3').resolve())
            finally:
                coordinator.close()
        finally:
            os.chdir(cwd)
            for name, value in prior.items():
                if value is not None:
                    os.environ[name] = value

    def test_remove_session_basic(self):
        """A registered session can be removed."""
        _, result = self.cli('remove-session', 'b')
        self.assertEqual(result['removed'], 'b')
        self.assertEqual(result['agent'], 'beta')
        # Session should no longer exist
        failed, _ = self.cli('inbox', session='b', check=False)
        self.assertNotEqual(failed.returncode, 0)

    def test_remove_session_unknown(self):
        """Removing an unknown session fails."""
        failed, _ = self.cli('remove-session', 'nonexistent', check=False)
        self.assertNotEqual(failed.returncode, 0)

    def test_remove_session_blocks_active_reservation(self):
        """Cannot remove a session that holds an active resource."""
        claim = self.request()  # alpha owns "shared"
        self.assertEqual(claim['state'], 'owned')
        failed, _ = self.cli('remove-session', 'a', check=False)
        self.assertNotEqual(failed.returncode, 0)
        self.assertIn('active resource', (failed.stderr + failed.stdout).lower())
        # Release it first, then removal should succeed
        self.release(claim)
        _, result = self.cli('remove-session', 'a')
        self.assertEqual(result['removed'], 'a')

    def test_remove_session_preserves_messages(self):
        """Messages involving a removed session are kept."""
        msg_id = self.send()  # b -> a
        self.cli('acknowledge', msg_id, session='a')
        self.cli('remove-session', 'b')
        # Messages involving the removed session are preserved in the DB
        conn = sqlite3.connect(str(self.db))
        conn.row_factory = sqlite3.Row
        try:
            ids = [r['id'] for r in conn.execute(
                "SELECT id FROM messages WHERE sender_session=? OR recipient_session=?",
                ('b', 'b')).fetchall()]
        finally:
            conn.close()
        self.assertIn(msg_id, ids)

    def test_remove_session_clears_queue(self):
        """Removing a session clears its resource queue entries."""
        # alpha requests "shared", beta queues behind
        self.request()
        self.cli('request', 'shared', '--minutes', '5', session='b')
        self.cli('remove-session', 'b')
        status = self.cli('status')[1]
        shared = next(r for r in status['resources'] if r['resource'] == 'shared')
        self.assertEqual(shared['queue'], [])


if __name__ == '__main__':
    unittest.main()
