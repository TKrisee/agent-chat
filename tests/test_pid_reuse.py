"""PID reuse must unblock old receipts without stopping replacement processes."""
from datetime import datetime, timezone
import contextlib
import io
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import unittest
from unittest import mock

from agent_chat import cli
from agent_chat.core import Coordinator, CoordError
from agent_chat.processes import receipt_pid_alive
from agent_chat.receipts import process_closure_proofs
from agent_chat.remote import RemoteCoordError
from test_remote_web import RemoteWebFixture, TOKEN


class ProcessReceiptTests(unittest.TestCase):
    def test_start_time_must_be_strictly_after_closure(self):
        stamp = datetime(2025, 1, 2, 3, 4, 5, tzinfo=timezone.utc).timestamp()
        result = subprocess.CompletedProcess([], 0, 'Thu Jan  2 03:04:05 2025\n', '')
        with mock.patch('agent_chat.processes.os.kill'), mock.patch('agent_chat.processes.subprocess.run', return_value=result):
            self.assertFalse(receipt_pid_alive(123, stamp - 1))
            self.assertTrue(receipt_pid_alive(123, stamp))
            self.assertTrue(receipt_pid_alive(123, stamp + .5))

    def test_missing_or_unreadable_start_time_does_not_prove_reuse(self):
        for failure in (OSError(), subprocess.TimeoutExpired('ps', 5), subprocess.CalledProcessError(1, 'ps')):
            with self.subTest(failure=failure), mock.patch('agent_chat.processes.os.kill'), mock.patch('agent_chat.processes.subprocess.run', side_effect=failure):
                self.assertTrue(receipt_pid_alive(123, time.time()))
        for output in ('', 'unparseable'):
            with self.subTest(output=output), mock.patch('agent_chat.processes.os.kill'), mock.patch('agent_chat.processes.subprocess.run', return_value=subprocess.CompletedProcess([], 0, output, '')):
                self.assertTrue(receipt_pid_alive(123, time.time()))

    def test_dead_and_inaccessible_processes(self):
        with mock.patch('agent_chat.processes.os.kill', side_effect=ProcessLookupError):
            self.assertFalse(receipt_pid_alive(123, time.time()))
        with mock.patch('agent_chat.processes.os.kill', side_effect=PermissionError):
            self.assertTrue(receipt_pid_alive(123, time.time()))

    def test_invalid_closure_timestamps_cannot_authorize_recovery(self):
        for stamp in (True, None, 'yesterday', 0, -1, 10 ** 1000, float('nan'), float('inf'), time.time() + 100):
            with self.subTest(stamp=stamp), self.assertRaises(ValueError):
                receipt_pid_alive(os.getpid(), stamp)


class ReceiptFixture:
    def receipt(self, claim, pids, closed_at, process_closed_at=None):
        directory = Path(self.tmp.name)
        (directory / 'evidence.txt').write_text('Fixture restored; original owned processes closed.')
        path = directory / 'CLOSED.json'
        value = dict(version=1, resource='shared', reservation_id=claim['reservation_id'],
                     restored=True, processes_closed=True, closed_at=closed_at, evidence='evidence.txt', pids=pids)
        if process_closed_at is not None:
            report = directory / 'process.txt'
            report.write_text('Original helper closed at the attested earlier timestamp.')
            value.update(version=2, process_closures=[dict(pid=pids[0], closed_at=process_closed_at,
                         evidence=report.name, evidence_sha256=hashlib.sha256(report.read_bytes()).hexdigest())])
        path.write_text(json.dumps(value))
        return str(path)

    def replacement_process(self):
        # Linux ps floors both boot time and elapsed start ticks separately.
        # Its displayed lower bound can lag the true start by almost two seconds.
        time.sleep(2.1)
        proc = subprocess.Popen(['sleep', '30'], start_new_session=True)
        def cleanup():
            proc.terminate()
            proc.wait(timeout=5)
        self.addCleanup(cleanup)
        return proc


class LocalReceiptReuseTests(ReceiptFixture, unittest.TestCase):
    def test_stale_recovery_preserves_replacement_and_rejects_original(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='agent-chat-pid-')
        self.addCleanup(self.tmp.cleanup)
        coord = Coordinator(Path(self.tmp.name) / 'state.sqlite3', 'owner')
        self.addCleanup(coord.close)
        coord.register('owner')
        claim = coord.request('shared', 5)
        closed_at = time.time()
        proc = self.replacement_process()
        with self.assertRaisesRegex(CoordError, 'still-live PID'):
            coord.release('shared', claim['token'], self.receipt(claim, [proc.pid], time.time()))
        coord.db.execute("UPDATE resources SET deadline=0 WHERE name='shared'")
        self.assertTrue(coord.recover('shared', self.receipt(claim, [proc.pid], closed_at))['recovered'])
        self.assertIsNone(proc.poll())
        self.assertIsNone(coord.db.execute("SELECT owner_session FROM resources WHERE name='shared'").fetchone()[0])

    def test_process_proof_may_predate_grant_without_backdating_restoration(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='agent-chat-pid-')
        self.addCleanup(self.tmp.cleanup)
        coord = Coordinator(Path(self.tmp.name) / 'state.sqlite3', 'owner')
        self.addCleanup(coord.close)
        coord.register('owner')
        claim = coord.request('shared', 5)
        proc = self.replacement_process()
        restored_at = time.time()
        receipt = self.receipt(claim, [proc.pid], restored_at, restored_at - 100)
        coord.db.execute("UPDATE resources SET deadline=0 WHERE name='shared'")
        self.assertTrue(coord.recover('shared', receipt)['recovered'])
        self.assertIsNone(proc.poll())
        self.assertIn(str(restored_at), coord.db.execute('SELECT receipt_json FROM receipts').fetchone()[0])


class RemoteReceiptReuseTests(ReceiptFixture, RemoteWebFixture):
    def setUp(self):
        super().setUp()
        self.call('register', agent='alpha')
        self.claim = self.call('request', resource='shared', minutes=5)
        self.env = dict(os.environ, AGENT_CHAT_SERVER=self.url, AGENT_CHAT_API_TOKEN=TOKEN,
                        AGENT_CHAT_SESSION='a', AGENT_CHAT_HOST_ID='mac', AGENT_CHAT_TOKEN=self.claim['token'])

    def invoke(self, action, receipt):
        with mock.patch.dict(os.environ, self.env, clear=True), contextlib.redirect_stdout(io.StringIO()):
            return cli.main([action, 'shared', '--receipt', receipt])

    def expire(self):
        coord = Coordinator(self.db)
        try:
            coord.db.execute("UPDATE resources SET deadline=0 WHERE name='shared'")
        finally:
            coord.close()

    def test_closed_guard_with_reused_pid_allows_recovery(self):
        run = self.call('begin-guard', resource='shared', token=self.claim['token'])
        self.call('attach-guard-pid', run_id=run['run_id'], token=self.claim['token'], pid=123456789)
        self.call('close-guard', run_id=run['run_id'], token=self.claim['token'], evidence_sha256='a' * 64)
        closed_at = time.time()
        proc = self.replacement_process()
        # Represent PID reuse without exhausting the operating system's PID space.
        coord = Coordinator(self.db)
        try:
            coord.db.execute('UPDATE remote_guarded_runs SET local_pid=? WHERE run_id=?', (proc.pid, run['run_id']))
        finally:
            coord.close()
        self.expire()
        self.assertEqual(self.invoke('recover', self.receipt(self.claim, [proc.pid], closed_at)), 0)
        self.assertIsNone(proc.poll())

    def test_live_original_remains_held(self):
        self.expire()
        with self.assertRaisesRegex(CoordError, 'still-live PID'):
            self.invoke('recover', self.receipt(self.claim, [os.getpid()], time.time()))
        self.assertEqual(self.call('status')['resources'][0]['reservation_id'], self.claim['reservation_id'])

    def test_reused_pid_cannot_override_an_open_live_group(self):
        run = self.call('begin-guard', resource='shared', token=self.claim['token'])
        closed_at = time.time()
        proc = self.replacement_process()
        self.call('attach-guard-pid', run_id=run['run_id'], token=self.claim['token'], pid=proc.pid)
        self.expire()
        with self.assertRaisesRegex(CoordError, 'process group'):
            self.invoke('recover', self.receipt(self.claim, [proc.pid], closed_at))
        self.assertIsNone(self.call('guard-context', resource='shared')['runs'][0]['closed_at'])
        self.assertIsNone(proc.poll())

    def test_future_or_nonfinite_receipt_cannot_bypass_process_check(self):
        self.expire()
        for stamp in (time.time() + 1000, float('nan'), float('inf')):
            with self.subTest(stamp=stamp), self.assertRaisesRegex(CoordError, 'closed_at'):
                self.invoke('recover', self.receipt(self.claim, [os.getpid()], stamp))

    def test_historical_proof_handles_replacement_born_before_restoration(self):
        proc = self.replacement_process()
        restored_at = time.time()
        self.expire()
        self.assertEqual(self.invoke('recover', self.receipt(self.claim, [proc.pid], restored_at, restored_at - 100)), 0)
        self.assertIsNone(proc.poll())

    def test_changed_or_missing_historical_report_retains_reservation(self):
        receipt = self.receipt(self.claim, [123456789], time.time(), time.time() - 100)
        report = Path(self.tmp.name) / 'process.txt'
        report.write_text('altered evidence')
        self.expire()
        with self.assertRaisesRegex(CoordError, 'hash'):
            self.invoke('recover', receipt)
        report.unlink()
        with self.assertRaises(CoordError):
            self.invoke('recover', receipt)
        self.assertEqual(self.call('status')['resources'][0]['reservation_id'], self.claim['reservation_id'])

    def test_server_rejects_missing_or_mismatched_proof_evidence(self):
        stamp = time.time()
        proof = dict(pid=123456789, closed_at=stamp - 100, evidence='old.txt', evidence_sha256=hashlib.sha256(b'old proof').hexdigest())
        receipt = dict(version=2, resource='shared', reservation_id=self.claim['reservation_id'], restored=True,
                       processes_closed=True, closed_at=stamp, evidence='restored.txt', pids=[123456789], process_closures=[proof])
        self.expire()
        for reports in ({}, {'123456789': 'bm90IHRoZSBwcm9vZg=='}, {'123456789': '!!!'}):
            with self.subTest(reports=reports), self.assertRaises(RemoteCoordError):
                self.call('recover', resource='shared', receipt=receipt, evidence_base64='cmVzdG9yZWQ=', process_evidence_base64=reports)
        self.assertEqual(self.call('status')['resources'][0]['reservation_id'], self.claim['reservation_id'])

    def test_historical_proof_cannot_close_a_later_guard(self):
        run = self.call('begin-guard', resource='shared', token=self.claim['token'])
        self.call('attach-guard-pid', run_id=run['run_id'], token=self.claim['token'], pid=123456789)
        self.expire()
        stamp = time.time()
        receipt = self.receipt(self.claim, [123456789], stamp, stamp - 100)
        with self.assertRaisesRegex(CoordError, 'predates'):
            self.invoke('recover', receipt)
        value = dict(version=2, resource='shared', reservation_id=self.claim['reservation_id'], restored=True, processes_closed=True,
                     closed_at=stamp, evidence='restored.txt', pids=[123456789], process_closures=[dict(
                         pid=123456789, closed_at=stamp-100, evidence='old.txt', evidence_sha256=hashlib.sha256(b'old proof').hexdigest())])
        with self.assertRaisesRegex(RemoteCoordError, 'not covered'):
            self.call('recover', resource='shared', receipt=value, evidence_base64='cmVzdG9yZWQ=', process_evidence_base64={'123456789': 'b2xkIHByb29m'})
        self.assertIsNone(self.call('guard-context', resource='shared')['runs'][0]['closed_at'])


class ProcessProofSchemaTests(unittest.TestCase):
    def test_bad_proof_bindings_are_rejected(self):
        proof = dict(pid=123, closed_at=1, evidence='old.txt', evidence_sha256='a' * 64)
        receipt = dict(version=2, resource='shared', reservation_id='reservation', restored=True,
                       processes_closed=True, closed_at=2, evidence='restored.txt', pids=[123], process_closures=[proof])
        for entries in ([proof, proof], [dict(proof, pid=456)], [dict(proof, closed_at=3)],
                        [dict(proof, closed_at=True)], [dict(proof, closed_at=float('nan'))],
                        [dict(proof, evidence='')], [dict(proof, evidence_sha256='bad')], 'invalid'):
            with self.subTest(entries=entries), self.assertRaises(ValueError):
                process_closure_proofs(dict(receipt, process_closures=entries))
