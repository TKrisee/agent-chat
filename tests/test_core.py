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
# Keep coordinator regression coverage independent of the public HTTP client.
CLI = [sys.executable, str(ROOT / 'src' / 'agent_chat' / 'core.py')]
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
        self.env.pop('AGENT_CHAT_SERVER', None)
        self.env.update(AGENT_CHAT_DB=str(self.db), AGENT_CHAT_SESSION='a',
                        AGENT_CHAT_STATE_DIR=str(self.root / 'client-state'))
        self.cli('register', '--agent', 'alpha')
        self.cli('register', '--agent', 'beta', session='b')

    def cli(self, *args, session='a', token=None, check=True):
        env = dict(self.env, AGENT_CHAT_SESSION=session)
        if token is not None:
            env['AGENT_CHAT_TOKEN'] = token
        result = subprocess.run(
            [*CLI, *args], cwd=ROOT, env=env, text=True,
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
            [*CLI, 'run', claim['resource'], '--', sys.executable, '-c', code],
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
        # ACK is routing/reporting bookkeeping, not recipient-body evidence.
        self.assertEqual([item['id'] for item in self.cli('inbox')[1]['messages']], [message])
        direct = agent_chat.core.Coordinator(self.db, 'a')
        try:
            direct.message(message)
        finally:
            direct.close()
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
        first_message = self.send()
        failed, _ = self.cli('request', 'shared', '--minutes', '5', check=False)
        self.assertNotEqual(failed.returncode, 0)
        self.cli('inbox')
        reader = agent_chat.core.Coordinator(self.db, 'a')
        try:
            reader.message(first_message)
        finally:
            reader.close()
        claim = self.request()
        second_message = self.send()
        failed, _ = self.cli('check', 'shared', token=claim['token'], check=False)
        self.assertNotEqual(failed.returncode, 0)
        self.cli('inbox')
        reader = agent_chat.core.Coordinator(self.db, 'a')
        try:
            reader.message(second_message)
        finally:
            reader.close()
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
        self.env.update(AGENT_CHAT_ROOT=str(ROOT))
        code = ('import json,os; print(json.dumps({name: os.environ[name] for name in '
                '("AGENT_CHAT_DB", "AGENT_CHAT_SESSION", "AGENT_CHAT_TOKEN", "AGENT_CHAT_ROOT")})); '
                'raise SystemExit(7)')
        result, _ = self.cli('run', 'shared', '--token', claim['token'], '--',
                             sys.executable, '-c', code, check=False)
        self.assertEqual(result.returncode, 7, result.stderr)
        environment = json.loads(result.stdout)
        self.assertEqual(environment['AGENT_CHAT_SESSION'], 'a')
        self.assertEqual(environment['AGENT_CHAT_TOKEN'], claim['token'])
        self.assertEqual(environment['AGENT_CHAT_DB'], str(self.db.resolve()))
        self.assertEqual(environment['AGENT_CHAT_ROOT'], str(ROOT.resolve()))
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
        _, stderr = process.communicate(timeout=15)
        self.assertNotEqual(process.returncode, 0)
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)
        try:
            self.release(claim)
        except AssertionError as error:
            self.fail(f'{error}\nGuarded process output: {stderr}')

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
        checker = agent_chat.core.Coordinator(self.db, 'a')
        try:
            with self.assertRaisesRegex(agent_chat.core.CoordError, 'read your inbox'):
                checker.check(claim['resource'], claim['token'])
            checker.message(message)
        finally:
            checker.close()
        self.release(claim)

    def test_guard_pulse_survives_message_delivered_after_its_inbox_read(self):
        claim = self.request()
        guard = agent_chat.core.ValidationGuard('shared', claim['token'], str(self.db), 'a')
        sender = agent_chat.core.Coordinator(self.db, 'b')
        try:
            with guard:
                emit = guard._emit_inbox

                def emit_then_deliver():
                    emit()
                    return sender.send('a', 'Delivered after the guard inbox read.')

                guard._emit_inbox = emit_then_deliver
                guard.pulse()
                with sqlite3.connect(self.db) as db:
                    message = db.execute("SELECT id,acked_at FROM messages WHERE recipient_session='a'").fetchone()
                    open_run = db.execute('SELECT 1 FROM guarded_runs WHERE run_id=? AND closed_at IS NULL',
                                          (guard.run_id,)).fetchone()
                self.assertIsNotNone(open_run)
                self.assertIsNone(message[1])
                checker = agent_chat.core.Coordinator(self.db, 'a')
                try:
                    with self.assertRaisesRegex(agent_chat.core.CoordError, 'read your inbox'):
                        checker.check('shared', claim['token'])
                    checker.inbox()
                    with self.assertRaisesRegex(agent_chat.core.CoordError, 'read your inbox'):
                        checker.check('shared', claim['token'])
                    checker.message(message[0])
                    self.assertEqual(checker.check('shared', claim['token'])['state'], 'owned')
                finally:
                    checker.close()
        finally:
            sender.close()
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
            'AGENT_CHAT_DB', 'AGENT_CHAT_ROOT',
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


class LeaseRenewalTests(unittest.TestCase):
    """Explicit owner renewal through isolated production coordinator APIs."""

    def setUp(self):
        from agent_chat.core import Coordinator
        from agent_chat.remote_service import dispatch
        self.dispatch = dispatch
        self.temporary = tempfile.TemporaryDirectory(prefix='lease-renewal-0061-')
        self.addCleanup(self.temporary.cleanup)
        self.database = Path(self.temporary.name) / 'state.sqlite3'
        self.now = time.time()
        clock = mock.patch('agent_chat.core._now', side_effect=lambda: self.now)
        clock.start(); self.addCleanup(clock.stop)
        self.coord = Coordinator(self.database, 'owner')
        self.addCleanup(lambda: self.coord.close())
        self.call('register', agent='owner')
        self.coord.context()
        self.claim = self.call('request', resource='lease', minutes=5)

    def call(self, operation, **params):
        return self.dispatch(self.coord, dict(op=operation, session='owner', host_id='fixture-host', params=params))

    def renew(self, minutes=1, **changes):
        params = dict(resource='lease', minutes=minutes, reservation_id=self.claim['reservation_id'],
                      token=self.claim['token'])
        params.update(changes)
        return self.call('renew', **params)

    def snapshot(self):
        return (tuple(self.coord.db.execute('SELECT * FROM resources WHERE name=?', ('lease',)).fetchone()),
                [tuple(row) for row in self.coord.db.execute('SELECT * FROM reservation_renewals')],
                [tuple(row) for row in self.coord.db.execute('SELECT * FROM resource_queue ORDER BY seq')],
                [tuple(row) for row in self.coord.db.execute('SELECT * FROM remote_guarded_runs')])

    def assert_expiry_only(self, before):
        after = self.snapshot()
        self.assertEqual(after[0][:6], before[0][:6])
        self.assertEqual(after[0][6], 1)
        self.assertEqual(after[0][7:], before[0][7:])
        self.assertEqual(after[1:], before[1:])

    def test_extension_preserves_original_grant_and_pulse_does_not_renew(self):
        before = self.snapshot()[0]
        guard = self.call('begin-guard', resource='lease', token=self.claim['token'])
        pulse = self.call('guard-pulse', run_id=guard['run_id'], token=self.claim['token'])
        self.assertEqual(pulse['deadline'], self.claim['deadline'])
        self.assertEqual(self.coord.db.execute('SELECT COUNT(*) FROM reservation_renewals').fetchone()[0], 0)
        renewed = self.renew(2)
        after = self.snapshot()[0]
        self.assertEqual(renewed['deadline'], self.claim['deadline'] + 120)
        self.assertEqual(renewed['reservation_id'], self.claim['reservation_id'])
        self.assertEqual(renewed['token'], self.claim['token'])
        self.assertEqual(after[:5], before[:5])
        self.assertEqual(after[6:], before[6:])
        self.assertEqual(renewed['renewal_count'], 1)
        self.assertIsNone(self.coord.db.execute('SELECT closed_at FROM remote_guarded_runs').fetchone()[0])

    def test_invalid_amount_early_expired_and_wrong_tuple_leave_lease_unchanged(self):
        for amount in (0, .5, 31, True, float('nan'), float('inf'), None):
            before = self.snapshot()
            with self.subTest(amount=amount), self.assertRaises(agent_chat.core.CoordError):
                self.renew(amount)
            self.assertEqual(self.snapshot(), before)
        for changes in (dict(reservation_id='different'), dict(token='different')):
            before = self.snapshot()
            with self.assertRaises(agent_chat.core.CoordError):
                self.renew(**changes)
            self.assertEqual(self.snapshot(), before)
        self.now = self.claim['deadline'] - 601
        before = self.snapshot()
        with self.assertRaisesRegex(agent_chat.core.CoordError, '10 minutes'):
            self.renew()
        self.assertEqual(self.snapshot(), before)
        self.now = self.claim['deadline']
        before = self.snapshot()
        with self.assertRaises(agent_chat.core.CoordError):
            self.renew()
        self.assertEqual(self.snapshot(), before)
        self.now += 1
        with self.assertRaises(agent_chat.core.CoordError):
            self.renew()
        self.assert_expiry_only(before)

    def test_fifo_refusal_preserves_all_claimants_and_budget(self):
        from agent_chat.core import Coordinator
        for session in ('second', 'third'):
            other = Coordinator(self.database, session)
            try:
                self.dispatch(other, dict(op='register', session=session, host_id='fixture-host', params=dict(agent=session)))
                other.context()
                result = self.dispatch(other, dict(op='request', session=session, host_id='fixture-host',
                                                   params=dict(resource='lease', minutes=5)))
                self.assertEqual(result['state'], 'queued')
            finally:
                other.close()
        before = self.snapshot()
        with self.assertRaisesRegex(agent_chat.core.CoordError, 'FIFO'):
            self.renew()
        self.assertEqual(self.snapshot(), before)
        self.assertEqual([row[2] for row in before[2]], ['second', 'third'])

    def test_ordinary_request_still_latches_expiry_after_renewal_refusal(self):
        self.now = self.claim['deadline'] + 1
        before = self.snapshot()
        with self.assertRaises(agent_chat.core.CoordError):
            self.renew()
        self.assert_expiry_only(before)
        with self.assertRaisesRegex(agent_chat.core.CoordError, 'stale'):
            self.call('request', resource='lease', minutes=5)
        self.assertEqual(self.coord.db.execute("SELECT stale FROM resources WHERE name='lease'").fetchone()[0], 1)
        self.now = self.claim['deadline'] - 1
        with self.assertRaisesRegex(agent_chat.core.CoordError, 'stale'):
            self.renew()
        self.assertEqual(self.coord.db.execute('SELECT COUNT(*) FROM reservation_renewals').fetchone()[0], 0)

    def test_expired_renewal_refusal_cannot_revive_after_clock_rollback(self):
        before = self.snapshot()
        self.now = self.claim['deadline'] + 1
        with self.assertRaises(agent_chat.core.CoordError):
            self.renew()
        self.now = self.claim['deadline'] - 1
        with self.assertRaisesRegex(agent_chat.core.CoordError, 'stale'):
            self.renew()
        after = self.snapshot()
        self.assertEqual(after[0][:6], before[0][:6])
        self.assertEqual(after[0][6], 1)
        self.assertEqual(after[0][7:], before[0][7:])
        self.assertEqual(after[1:], before[1:])

    def test_mid_transaction_expiry_refusal_cannot_revive_after_clock_rollback(self):
        before = self.snapshot()
        self.now = self.claim['deadline'] - 1
        original_row = self.coord._row

        def crossing_row(db, resource):
            self.now = self.claim['deadline'] + 1
            return original_row(db, resource)

        with mock.patch.object(self.coord, '_row', side_effect=crossing_row):
            with self.assertRaisesRegex(agent_chat.core.CoordError, 'stale'):
                self.renew()
        self.now = self.claim['deadline'] - 1
        with self.assertRaisesRegex(agent_chat.core.CoordError, 'stale'):
            self.renew()
        self.assert_expiry_only(before)

    def test_expiry_after_owner_check_is_latched_before_refusal(self):
        before = self.snapshot()
        self.now = self.claim['deadline'] - 1
        original_verify = self.coord._verify_owner

        def crossing_verify(*args, **kwargs):
            row = original_verify(*args, **kwargs)
            self.now = self.claim['deadline'] + 1
            return row

        with mock.patch.object(self.coord, '_verify_owner', side_effect=crossing_verify):
            with self.assertRaisesRegex(agent_chat.core.CoordError, 'unexpired'):
                self.renew()
        self.now = self.claim['deadline'] - 1
        with self.assertRaisesRegex(agent_chat.core.CoordError, 'stale'):
            self.renew()
        self.assert_expiry_only(before)

    def test_state_first_expiry_refusal_cannot_revive_after_clock_rollback(self):
        before = self.snapshot()
        self.now = self.claim['deadline'] - 1
        original_state = self.coord._state

        def crossing_state(row):
            self.now = self.claim['deadline'] + 1
            return original_state(row)

        with mock.patch.object(self.coord, '_state', side_effect=crossing_state):
            with self.assertRaisesRegex(agent_chat.core.CoordError, 'stale'):
                self.renew()
        self.now = self.claim['deadline'] - 1
        with self.assertRaisesRegex(agent_chat.core.CoordError, 'stale'):
            self.renew()
        self.assert_expiry_only(before)

    def test_request_state_first_expiry_cannot_revive_after_clock_rollback(self):
        before = self.snapshot()
        self.now = self.claim['deadline'] - 1
        original_state = self.coord._state

        def crossing_state(row):
            self.now = self.claim['deadline'] + 1
            return original_state(row)

        with mock.patch.object(self.coord, '_state', side_effect=crossing_state):
            with self.assertRaisesRegex(agent_chat.core.CoordError, 'stale'):
                self.coord.request('lease', 5)
        self.now = self.claim['deadline'] - 1
        with self.assertRaisesRegex(agent_chat.core.CoordError, 'stale'):
            self.coord.request('lease', 5)
        self.assert_expiry_only(before)

    def test_status_state_first_expiry_cannot_revive_after_clock_rollback(self):
        self.assert_status_first_expiry_latched(direct=False)

    def test_resource_status_first_expiry_cannot_revive_after_clock_rollback(self):
        self.assert_status_first_expiry_latched(direct=True)

    def assert_status_first_expiry_latched(self, *, direct):
        before = self.snapshot()
        self.now = self.claim['deadline'] - 1
        original_state = self.coord._state

        def crossing_state(row):
            self.now = self.claim['deadline'] + 1
            state = original_state(row)
            # The legitimate observation must survive even an immediate
            # rollback before the reporting transaction commits.
            self.now = self.claim['deadline'] - 1
            return state

        with mock.patch.object(self.coord, '_state', side_effect=crossing_state):
            if direct:
                with self.coord.tx() as db:
                    row = db.execute("SELECT * FROM resources WHERE name='lease'").fetchone()
                    observed = self.coord._resource_status(db, row)
            else:
                observed = self.coord.status(resources=['lease'])['resources'][0]
        self.assertEqual(observed['state'], 'stale')
        self.assertEqual(self.coord.status(resources=['lease'])['resources'][0]['state'], 'stale')
        with self.assertRaisesRegex(agent_chat.core.CoordError, 'stale'):
            self.coord.request('lease', 5)
        self.assert_expiry_only(before)

    def test_full_budget_survives_restart_and_same_rid_request(self):
        from agent_chat.core import Coordinator
        for count in range(1, 5):
            self.now = self.claim['deadline'] - 300
            result = self.renew(30)
            self.assertEqual((result['renewal_count'], result['renewal_minutes']), (count, count * 30))
            self.claim = result
            self.coord.close()
            self.coord = Coordinator(self.database, 'owner')
            self.assertEqual(self.call('request', resource='lease', minutes=5)['reservation_id'], self.claim['reservation_id'])
        self.now = self.claim['deadline'] - 300
        before = self.snapshot()
        with self.assertRaisesRegex(agent_chat.core.CoordError, 'budget'):
            self.renew()
        self.assertEqual(self.snapshot(), before)

    def test_stopped_retired_quarantined_and_global_root_are_ineligible(self):
        from agent_chat.bridge_state import BridgeState
        import uuid
        thread = str(uuid.uuid4())
        BridgeState(self.coord).bind(thread)
        self.coord.context(limit=100, max_bytes=100000)
        self.call('agent-stop', expected_thread=thread, request_id=str(uuid.uuid4()), confirm=True)
        # Fixture admission after its worker reaches stopped; no native turn
        # or control worker is run by this renewal harness.
        self.coord.db.execute("UPDATE agent_controls SET mode='stopped' WHERE target_session='owner'")
        before = self.snapshot()
        with self.assertRaisesRegex(agent_chat.core.CoordError, 'admission is stopped'):
            self.renew()
        self.assertEqual(self.snapshot(), before)
        self.coord.db.execute("UPDATE agent_controls SET mode='running' WHERE target_session='owner'")
        with mock.patch('agent_chat.root_team_reset.quarantined', return_value=True):
            before = self.snapshot()
            with self.assertRaisesRegex(agent_chat.core.CoordError, 'quarantined'):
                self.renew()
            self.assertEqual(self.snapshot(), before)
        with mock.patch('agent_chat.root_orchestrator.projection', return_value={'paused': False}):
            before = self.snapshot()
            with self.assertRaisesRegex(agent_chat.core.CoordError, 'global root is control-only'):
                self.renew()
            self.assertEqual(self.snapshot(), before)
        self.coord.db.execute("INSERT INTO retired_sessions SELECT id,agent,registered_at,? FROM sessions WHERE id='owner'",
                              (self.now,))
        before = self.snapshot()
        with self.assertRaisesRegex(agent_chat.core.CoordError, 'deregistered'):
            self.renew()
        self.assertEqual(self.snapshot(), before)

    def test_latched_stale_and_unregistered_host_cannot_extend(self):
        self.coord.db.execute("UPDATE resources SET stale=1 WHERE name='lease'")
        before = self.snapshot()
        with self.assertRaisesRegex(agent_chat.core.CoordError, 'stale'):
            self.renew()
        self.assertEqual(self.snapshot(), before)
        self.coord.db.execute("UPDATE resources SET stale=0 WHERE name='lease'")
        self.coord.db.execute("DELETE FROM remote_session_hosts WHERE session_id='owner'")
        before = self.snapshot()
        with self.assertRaisesRegex(agent_chat.core.CoordError, 'legacy session'):
            self.renew()
        self.assertEqual(self.snapshot(), before)

    def test_unread_changed_host_and_unbound_or_conflicting_guards_refuse(self):
        from agent_chat.core import Coordinator
        other = Coordinator(self.database, 'sender')
        try:
            self.dispatch(other, dict(op='register', session='sender', host_id='sender-host', params=dict(agent='sender')))
            other.context()
            other.send('owner', 'Read the changed synthetic instruction')
        finally:
            other.close()
        before = self.snapshot()
        with self.assertRaisesRegex(agent_chat.core.CoordError, 'read your inbox'):
            self.renew()
        self.assertEqual(self.snapshot(), before)
        self.coord.context(limit=100, max_bytes=100000)
        self.coord.db.execute("UPDATE remote_session_hosts SET host_id='changed-host' WHERE session_id='owner'")
        before = self.snapshot()
        with self.assertRaisesRegex(agent_chat.core.CoordError, 'different remote host'):
            self.renew()
        self.assertEqual(self.snapshot(), before)
        self.coord.db.execute("UPDATE remote_session_hosts SET host_id='fixture-host' WHERE session_id='owner'")
        self.coord.db.execute('INSERT INTO guarded_runs VALUES(?,?,?,?,?,?,?,?)',
            ('synthetic-unbound', 'lease', self.claim['reservation_id'], 'owner', 0, None, self.now, None))
        before = self.snapshot()
        with self.assertRaisesRegex(agent_chat.core.CoordError, 'unbound local'):
            self.renew()
        self.assertEqual(self.snapshot(), before)
        self.coord.db.execute('DELETE FROM guarded_runs WHERE run_id=?', ('synthetic-unbound',))
        guard = self.call('begin-guard', resource='lease', token=self.claim['token'])
        original = dict(self.coord.db.execute('SELECT * FROM remote_guarded_runs WHERE run_id=?', (guard['run_id'],)).fetchone())
        for field in ('resource', 'reservation_id', 'session_id', 'host_id'):
            self.coord.db.execute('UPDATE remote_guarded_runs SET ' + field + '=? WHERE run_id=?', ('conflicting', guard['run_id']))
            before = self.snapshot()
            with self.subTest(field=field), self.assertRaisesRegex(agent_chat.core.CoordError, 'conflicting'):
                self.renew()
            self.assertEqual(self.snapshot(), before)
            self.coord.db.execute('UPDATE remote_guarded_runs SET ' + field + '=? WHERE run_id=?', (original[field], guard['run_id']))


if __name__ == '__main__':
    unittest.main()
