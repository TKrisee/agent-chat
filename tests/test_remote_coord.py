import base64
import contextlib
import io
import json
import tempfile
import unittest
from unittest import mock
from concurrent.futures import ThreadPoolExecutor
import os
import time
from pathlib import Path

from agent_chat.core import Coordinator, CoordError
from agent_chat.remote_service import dispatch
from agent_chat import cli
from agent_chat.remote import HttpClient


class RemoteCoordTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "state.sqlite3"

    def tearDown(self): self.tmp.cleanup()

    def call(self, op, session="a", host="host-a", **params):
        coord = Coordinator(self.db)
        try: return dispatch(coord, {"op": op, "session": session, "host_id": host, "params": params})
        finally: coord.close()

    def supervision_owner(self):
        self.call('register', agent='worker')
        self.call('register', session='b', agent='orchestrator')
        with contextlib.closing(Coordinator(self.db)) as coord, coord.tx() as db:
            db.execute('CREATE TABLE IF NOT EXISTS project_orchestrators (session_id TEXT PRIMARY KEY, root_session TEXT NOT NULL)')
            db.execute('INSERT INTO project_orchestrators VALUES(?,?)', ('b', 'root-session'))

    def supervision_bits(self):
        with contextlib.closing(Coordinator(self.db)) as coord:
            row = coord.db.execute('SELECT enabled,recovery_enabled FROM supervisor_owner_policies WHERE owner_session=?', ('b',)).fetchone()
            return tuple(row)

    def test_supervisor_recovery_api_preserves_independent_bits_and_withdraws(self):
        self.supervision_owner()
        self.call('supervisor-configure', session='b', enabled=True)
        self.assertEqual(self.supervision_bits(), (1, 0))
        self.call('supervisor-configure', session='b', recovery_enabled=True)
        self.assertEqual(self.supervision_bits(), (1, 1))
        self.call('supervisor-configure', session='b', enabled=False)
        self.assertEqual(self.supervision_bits(), (0, 1))
        self.call('supervisor-configure', session='b', recovery_enabled=False)
        self.assertEqual(self.supervision_bits(), (0, 0))
        self.call('supervisor-configure', session='b', enabled=True)
        self.assertEqual(self.supervision_bits(), (1, 0))
        self.call('supervisor-configure', session='b', recovery_enabled=True)
        self.call('supervisor-configure', session='b', recovery_enabled=False)
        self.assertEqual(self.supervision_bits(), (1, 0))

    def test_supervisor_recovery_api_rejects_invalid_unauthorized_and_unread_changes(self):
        self.supervision_owner()
        self.call('supervisor-configure', session='b', enabled=True)
        for params in ({}, {'enabled': True, 'recovery_enabled': True}, {'unknown': True},
                       {'recovery_enabled': None}, {'recovery_enabled': 1}, {'enabled': 0}):
            with self.subTest(params=params), self.assertRaises(CoordError):
                self.call('supervisor-configure', session='b', **params)
            self.assertEqual(self.supervision_bits(), (1, 0))
        with self.assertRaisesRegex(CoordError, 'not authorized'):
            self.call('supervisor-configure', recovery_enabled=True)
        with self.assertRaisesRegex(CoordError, 'different remote host'):
            self.call('supervisor-configure', session='b', host='another-host', recovery_enabled=True)
        self.call('send', to='b', body='Consume this coordination update first.')
        with self.assertRaisesRegex(CoordError, 'read your inbox'):
            self.call('supervisor-configure', session='b', recovery_enabled=True)
        self.assertEqual(self.supervision_bits(), (1, 0))
        self.call('inbox', session='b', all=True)
        self.call('supervisor-configure', session='b', recovery_enabled=True)
        self.assertEqual(self.supervision_bits(), (1, 1))

    def test_supervisor_recovery_cli_sends_only_selected_bit_and_rejects_mixed_flags(self):
        sent = []
        def capture(client, path, payload):
            sent.append((path, payload['op'], payload['params']))
            return {'configured': True}
        for flag, params in (('--enable', {'enabled': True}), ('--disable', {'enabled': False}),
                             ('--recovery-enable', {'recovery_enabled': True}),
                             ('--recovery-disable', {'recovery_enabled': False})):
            output, errors = io.StringIO(), io.StringIO()
            with (mock.patch.dict(os.environ, {'AGENT_CHAT_SESSION': 'b'}, clear=True),
                  mock.patch.object(HttpClient, 'call', capture),
                  mock.patch('agent_chat.remote.client_host_id', return_value='host-a'),
                  contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors)):
                self.assertEqual(cli.main(['supervisor-configure', flag]), 0)
            self.assertEqual(sent[-1], ('/api/coord', 'supervisor-configure', params))
            self.assertEqual(output.getvalue(), '{"configured": true}\n')
            self.assertEqual(errors.getvalue(), '')
        before = len(sent)
        for flags in ([], ['--enable', '--recovery-enable'], ['--recovery-enable', '--recovery-disable']):
            with (mock.patch.dict(os.environ, {'AGENT_CHAT_SESSION': 'b'}, clear=True),
                  mock.patch.object(HttpClient, 'call', capture),
                  contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit)):
                cli.main(['supervisor-configure', *flags])
        self.assertEqual(len(sent), before)

    def batch_claims(self, count=42):
        self.call('register', agent='alpha')
        return [dict(resource='batch-' + str(index), session_id='a', host_id='host-a',
                     reservation_id=claim['reservation_id'], token=claim['token'])
                for index in range(count)
                for claim in [self.call('request', resource='batch-' + str(index), minutes=5)]]

    def test_batch_admission_is_all_or_none_and_reuses_guard_operations(self):
        tuples = self.batch_claims()
        result = self.call('begin-guard-batch', tuples=tuples)
        self.assertEqual(len(result['admissions']), 42)
        self.assertNotIn('token', repr(result))
        with __import__('contextlib').closing(Coordinator(self.db)) as coord:
            self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM remote_guard_batches').fetchone()[0], 42)
        for item, admission in zip(tuples, result['admissions']):
            params = dict(run_id=admission['run_id'], token=item['token'])
            self.assertEqual(self.call('guard-pulse', **params)['reservation_id'], item['reservation_id'])
            self.call('attach-guard-pid', pid=123, **params)
            self.call('close-guard', evidence_sha256='0' * 64, **params)
        with __import__('contextlib').closing(Coordinator(self.db)) as coord:
            self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM remote_guarded_runs WHERE closed_at IS NULL').fetchone()[0], 0)

    def test_batch_final_invalid_tuple_creates_no_admission(self):
        tuples = self.batch_claims()
        tuples[-1]['reservation_id'] = 'different-reservation'
        with self.assertRaises(CoordError):
            self.call('begin-guard-batch', tuples=tuples)
        with __import__('contextlib').closing(Coordinator(self.db)) as coord:
            self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM remote_guarded_runs').fetchone()[0], 0)
            self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM remote_guard_batches').fetchone()[0], 0)
            self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM resources WHERE owner_session=?', ('a',)).fetchone()[0], 42)

    def test_batch_rejects_foreign_identity_duplicate_and_invalid_token(self):
        tuples = self.batch_claims(2)
        for key, value in (('session_id', 'other'), ('host_id', 'other'), ('token', 'invalid')):
            items = [dict(item) for item in tuples]
            items[-1][key] = value
            with self.assertRaises(CoordError): self.call('begin-guard-batch', tuples=items)
        with self.assertRaises(CoordError): self.call('begin-guard-batch', tuples=[tuples[0], tuples[0]])
        with self.assertRaises(CoordError): self.call('begin-guard-batch', tuples=tuples * 22)
        with __import__('contextlib').closing(Coordinator(self.db)) as coord:
            self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM remote_guarded_runs').fetchone()[0], 0)

    def test_batch_restore_requires_each_stale_exact_reservation(self):
        tuples = self.batch_claims(2)
        with __import__('contextlib').closing(Coordinator(self.db)) as coord:
            coord.db.execute('UPDATE resources SET deadline=? WHERE name=?', (time.time()-1, tuples[0]['resource']))
        with self.assertRaises(CoordError): self.call('begin-guard-batch', tuples=tuples, restore=True)
        with __import__('contextlib').closing(Coordinator(self.db)) as coord:
            self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM remote_guarded_runs').fetchone()[0], 0)
            coord.db.execute('UPDATE resources SET deadline=? WHERE name=?', (time.time()-1, tuples[1]['resource']))
        result = self.call('begin-guard-batch', tuples=tuples, restore=True, max_seconds=30)
        self.assertEqual(len(result['admissions']), 2)
        for item, admission in zip(tuples, result['admissions']):
            state = self.call('guard-pulse', run_id=admission['run_id'], token=item['token'])
            self.assertTrue(state['restore'])
            self.assertEqual(state['state'], 'stale')

    def test_session_is_host_pinned_and_no_local_database_fallback(self):
        self.call("register", agent="alpha")
        with self.assertRaisesRegex(CoordError, "different remote host"):
            self.call("status", host="host-b")

    def test_remote_guard_and_receipt_are_host_bound(self):
        self.call("register", agent="alpha")
        claim = self.call("request", resource="shared", minutes=5)
        run = self.call("begin-guard", resource="shared", token=claim["token"], pid=123)
        self.call("close-guard", run_id=run["run_id"], token=claim["token"], evidence_sha256="0" * 64)
        receipt = {"version": 1, "resource": "shared", "reservation_id": claim["reservation_id"],
                   "restored": True, "processes_closed": True, "closed_at": __import__("time").time(),
                   "evidence": "local-evidence", "pids": [123]}
        with self.assertRaisesRegex(CoordError, "different remote host"):
            self.call("recover", session="a", host="host-b", receipt=receipt,
                      evidence_base64=base64.b64encode(b"proof").decode())
        result = self.call("release", resource="shared", token=claim["token"], receipt=receipt,
                           evidence_base64=base64.b64encode(b"proof").decode())
        self.assertTrue(result["released"])

    def test_remote_recover_and_guard_context_require_owner_session_on_same_host(self):
        claim = self.stale_claim()
        receipt = {"version": 1, "resource": "shared", "reservation_id": claim["reservation_id"],
                   "restored": True, "processes_closed": True, "closed_at": time.time(),
                   "evidence": "local-evidence", "pids": []}
        with self.assertRaisesRegex(CoordError, "owning session"):
            self.call("guard-context", session="b", resource="shared")
        with self.assertRaisesRegex(CoordError, "owning session"):
            self.call("recover", session="b", resource="shared", receipt=receipt,
                      evidence_base64=base64.b64encode(b"proof").decode())
        held = self.call("status", resources=["shared"])["resources"][0]
        self.assertEqual(held["reservation_id"], claim["reservation_id"])
        self.assertEqual(held["state"], "stale")

    def test_reservation_result_is_owner_bound_pending_then_receipt_backed_clear(self):
        self.call("register", agent="alpha")
        claim = self.call("request", resource="shared", minutes=5)
        expected = {"state": "pending", "resource": "shared",
                    "reservation_id": claim["reservation_id"], "receipt_backed_clear": False}
        self.assertEqual(self.call("reservation-result", resource="shared",
                                   reservation_id=claim["reservation_id"]), expected)
        self.call("register", session="b", host="host-a", agent="beta")
        self.assertEqual(self.call("reservation-result", session="b", host="host-a", resource="shared",
                                   reservation_id=claim["reservation_id"]), {"state": "unknown"})
        with self.assertRaisesRegex(CoordError, "different remote host"):
            self.call("reservation-result", host="other-host", resource="shared",
                      reservation_id=claim["reservation_id"])
        self.assertEqual(self.call("reservation-result", resource="shared",
                                   reservation_id="different-reservation"), {"state": "unknown"})
        receipt = {"version": 1, "resource": "shared", "reservation_id": claim["reservation_id"],
                   "restored": True, "processes_closed": True, "closed_at": time.time(),
                   "evidence": "local-evidence", "pids": []}
        self.assertTrue(self.call("release", resource="shared", token=claim["token"], receipt=receipt,
                                  evidence_base64=base64.b64encode(b"proof").decode())["released"])
        cleared = self.call("reservation-result", resource="shared", reservation_id=claim["reservation_id"])
        self.assertEqual(cleared["state"], "cleared")
        self.assertEqual(cleared["action"], "release")
        self.assertTrue(cleared["receipt_backed_clear"])
        self.assertNotIn("receipt_json", cleared)
        self.assertEqual(self.call("reservation-result", session="b", host="host-a", resource="shared", reservation_id=claim["reservation_id"]), {"state": "unknown"})

    def test_reservation_result_hides_expired_pending_outcome(self):
        self.call("register", agent="alpha")
        claim = self.call("request", resource="shared", minutes=5)
        with contextlib.closing(Coordinator(self.db)) as coord, coord.tx() as db:
            db.execute("UPDATE resources SET deadline=? WHERE name=?", (time.time() - 1, "shared"))
        self.assertEqual(self.call("reservation-result", resource="shared", reservation_id=claim["reservation_id"]), {"state": "unknown"})

    def test_unread_messages_do_not_bypass_guard_ownership_or_lifecycle_checks(self):
        self.call('register', agent='alpha')
        self.call('register', session='b', agent='beta')
        claim = self.call('request', resource='shared', minutes=5)
        run = self.call('begin-guard', resource='shared', token=claim['token'])
        self.call('send', session='b', to='a', body='New coordination message.')
        params = dict(run_id=run['run_id'], token=claim['token'])
        self.assertEqual(self.call('guard-pulse', **params)['state'], 'owned')
        with self.assertRaisesRegex(CoordError, 'reservation token'):
            self.call('guard-pulse', run_id=run['run_id'], token='invalid')
        with self.assertRaisesRegex(CoordError, 'not open on this host'):
            self.call('guard-pulse', session='b', **params)
        with self.assertRaisesRegex(CoordError, 'read your inbox'):
            self.call('begin-guard', resource='shared', token=claim['token'])
        with self.assertRaisesRegex(CoordError, 'read your inbox'):
            self.call('request', resource='another', minutes=5)
        coord = Coordinator(self.db)
        try:
            coord.db.execute("UPDATE resources SET deadline=? WHERE name='shared'", (time.time()-1,))
        finally:
            coord.close()
        with self.assertRaisesRegex(CoordError, 'reservation is stale'):
            self.call('guard-pulse', **params)
        self.call('close-guard', evidence_sha256='0' * 64, **params)
        with self.assertRaisesRegex(CoordError, 'not open on this host'):
            self.call('guard-pulse', **params)

    def test_legacy_hold_cannot_be_claimed_by_remote_host(self):
        coord = Coordinator(self.db, "legacy")
        try:
            coord.register("legacy")
            coord.request("held", 5)
        finally: coord.close()
        with self.assertRaisesRegex(CoordError, "legacy session"):
            self.call("status", session="legacy", host="remote-host")

    def stale_claim(self):
        self.call('register', agent='alpha')
        claim = self.call('request', resource='shared', minutes=5)
        self.call('register', session='b', agent='beta')
        self.call('request', session='b', resource='shared', minutes=5)
        coord = Coordinator(self.db)
        try: coord.db.execute("UPDATE resources SET deadline=? WHERE name='shared'", (time.time()-1,))
        finally: coord.close()
        self.call('status', resources=['shared'])
        return claim

    def restoration(self, claim, **overrides):
        params = dict(resource='shared', token=claim['token'], restore=True,
                      reservation_id=claim['reservation_id'], max_seconds=30)
        params.update(overrides)
        return self.call('begin-guard', **params)

    def test_restoration_preserves_hold_queue_and_requires_receipt(self):
        claim = self.stale_claim()
        coord = Coordinator(self.db)
        try:
            before = dict(coord.db.execute("SELECT * FROM resources WHERE name='shared'").fetchone())
            queue = [tuple(r) for r in coord.db.execute('SELECT * FROM resource_queue')]
        finally: coord.close()
        run = self.restoration(claim)
        params = dict(run_id=run['run_id'], token=claim['token'])
        self.call('attach-guard-pid', pid=123, **params)
        self.call('send', session='b', to='a', body='New message during restoration.')
        pulse = self.call('guard-pulse', **params)
        self.assertEqual(pulse['state'], 'stale')
        self.assertTrue(pulse['restore'])
        self.call('close-guard', evidence_sha256='0'*64, **params)
        coord = Coordinator(self.db)
        try:
            self.assertEqual(dict(coord.db.execute("SELECT * FROM resources WHERE name='shared'").fetchone()), before)
            self.assertEqual([tuple(r) for r in coord.db.execute('SELECT * FROM resource_queue')], queue)
            self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM receipts').fetchone()[0], 0)
        finally: coord.close()
        with self.assertRaisesRegex(CoordError, 'not open'):
            self.call('guard-pulse', **params)
        self.call('context')
        with self.assertRaisesRegex(CoordError, 'stale'):
            self.call('check', resource='shared', token=claim['token'])

    def test_restoration_authorization_and_budget_boundaries(self):
        claim = self.stale_claim()
        for params, error in [
            ({'token': 'invalid'}, 'reservation token'),
            ({'session': 'b'}, 'reservation token'),
            ({'host': 'other'}, 'different remote host'),
            ({'reservation_id': 'other'}, 'does not match'),
            ({'resource': 'free'}, 'reservation token'),
            ({'max_seconds': 0}, 'integer from'),
            ({'max_seconds': 901}, 'integer from'),
            ({'max_seconds': True}, 'integer from'),
            ({'restore': 'true'}, 'boolean'),
        ]:
            with self.subTest(params=params), self.assertRaisesRegex(CoordError, error):
                self.restoration(claim, **params)
        with self.assertRaisesRegex(CoordError, 'stale'):
            self.call('begin-guard', resource='shared', token=claim['token'])
        self.call('send', session='b', to='a', body='Read before starting restoration.')
        with self.assertRaisesRegex(CoordError, 'read your inbox'):
            self.restoration(claim)
        self.call('context')
        run = self.restoration(claim)
        with self.assertRaisesRegex(CoordError, 'close existing'):
            self.restoration(claim)
        coord = Coordinator(self.db)
        try: coord.db.execute('UPDATE remote_restoration_runs SET deadline=? WHERE run_id=?', (time.time()-1, run['run_id']))
        finally: coord.close()
        params = dict(run_id=run['run_id'], token=claim['token'])
        with self.assertRaisesRegex(CoordError, 'deadline has expired'):
            self.call('attach-guard-pid', pid=123, **params)
        with self.assertRaisesRegex(CoordError, 'deadline has expired'):
            self.call('guard-pulse', **params)
        self.call('close-guard', evidence_sha256='0'*64, **params)

    def test_ordinary_run_cannot_be_upgraded_and_replacement_reservation_rejected(self):
        self.call('register', agent='alpha')
        claim = self.call('request', resource='shared', minutes=5)
        with self.assertRaisesRegex(CoordError, 'requires a stale'):
            self.restoration(claim)
        run = self.call('begin-guard', resource='shared', token=claim['token'])
        coord = Coordinator(self.db)
        try: coord.db.execute("UPDATE resources SET deadline=? WHERE name='shared'", (time.time()-1,))
        finally: coord.close()
        params = dict(run_id=run['run_id'], token=claim['token'], restore=True)
        with self.assertRaisesRegex(CoordError, 'stale'):
            self.call('attach-guard-pid', pid=123, **params)
        with self.assertRaisesRegex(CoordError, 'stale'):
            self.call('guard-pulse', **params)
        self.call('close-guard', evidence_sha256='0'*64, **params)
        restoration = self.restoration(claim)
        coord = Coordinator(self.db)
        try: coord.db.execute("UPDATE resources SET reservation_id='replacement' WHERE name='shared'")
        finally: coord.close()
        params['run_id'] = restoration['run_id']
        with self.assertRaisesRegex(CoordError, 'reservation has changed'):
            self.call('attach-guard-pid', pid=123, **params)
        with self.assertRaisesRegex(CoordError, 'reservation has changed'):
            self.call('guard-pulse', **params)

    def test_file_resource_keys_do_not_depend_on_host_filesystem(self):
        self.call('register', agent='alpha')
        with mock.patch('pathlib.Path.resolve', side_effect=AssertionError('must not inspect project filesystem')):
            from agent_chat.remote_service import resource_name
            self.assertEqual(resource_name('file:src/sub/../test.py'), 'file:src/test.py')
            for key in ('file:/absolute', 'file:../escape', 'file:a/../../escape'):
                with self.assertRaises(CoordError): resource_name(key)

    def test_concurrent_host_identity_is_stable_private_and_corruption_refused(self):
        from agent_chat.remote import client_host_id, RemoteCoordError
        with mock.patch.dict(os.environ, {'AGENT_CHAT_STATE_DIR':self.tmp.name}, clear=True):
            with ThreadPoolExecutor(max_workers=8) as pool:
                values=list(pool.map(lambda _: client_host_id(), range(16)))
            self.assertEqual(len(set(values)),1)
            path=Path(self.tmp.name)/'client-host.json'
            self.assertEqual(path.stat().st_mode & 0o777,0o600)
            path.write_text('{invalid')
            with self.assertRaises(RemoteCoordError): client_host_id()

    def test_remote_validation_guard_refuses_before_creating_database(self):
        prior = os.environ.get("AGENT_CHAT_SERVER")
        os.environ["AGENT_CHAT_SERVER"] = "https://coord.example"
        path = Path(self.tmp.name) / "must-not-exist.sqlite3"
        try:
            with self.assertRaisesRegex(CoordError, "--server"):
                __import__("agent_chat.core", fromlist=["ValidationGuard"]).ValidationGuard(db_path=str(path))
            self.assertFalse(path.exists())
        finally:
            if prior is None: os.environ.pop("AGENT_CHAT_SERVER", None)
            else: os.environ["AGENT_CHAT_SERVER"] = prior


if __name__ == "__main__": unittest.main()
