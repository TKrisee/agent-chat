import base64
import json
import tempfile
import unittest
from unittest import mock
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path

from agent_chat.core import Coordinator, CoordError
from agent_chat.remote_service import dispatch


class RemoteCoordTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "state.sqlite3"

    def tearDown(self): self.tmp.cleanup()

    def call(self, op, session="a", host="host-a", **params):
        coord = Coordinator(self.db)
        try: return dispatch(coord, {"op": op, "session": session, "host_id": host, "params": params})
        finally: coord.close()

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

    def test_legacy_hold_cannot_be_claimed_by_remote_host(self):
        coord = Coordinator(self.db, "legacy")
        try:
            coord.register("legacy")
            coord.request("held", 5)
        finally: coord.close()
        with self.assertRaisesRegex(CoordError, "legacy session"):
            self.call("status", session="legacy", host="remote-host")

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
