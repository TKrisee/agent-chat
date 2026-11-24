"""Isolated real Coordinator/SQLite/dispatch regressions for the fixed E2D unit.

Only native/kernel observations and the installed-provider locator are fixtures.
No live configuration, native input, process launch or ticket is used.
"""
import base64
import contextlib
import io
import sqlite3
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from synthetic_successor_allowance import allowance
from test_capacity_admission import make_root_fixture, SOURCE_ID
from agent_chat import capacity_admission as old, capacity_observer as observer
from agent_chat import root_local, remote_service, service, successor_admission as successor
from agent_chat.core import CoordError


class E2DAdmissionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="e2d-admission-")
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name).resolve()
        self.coord, self.master, self.registry = make_root_fixture(self.root)
        self.addCleanup(self.coord.close)
        self.db = self.coord.db
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("INSERT INTO sessions(id,agent,registered_at,inbox_read_seq) VALUES(?,?,?,0)",
                        (successor.CONSUMER, "consumer", time.time()))
        self.db.execute("INSERT INTO bridge_bindings VALUES(?,?,?)", (successor.CONSUMER, successor.THREAD, successor.ROOT))
        self.db.execute("INSERT INTO remote_session_hosts(session_id,host_id) VALUES(?,?)", (successor.CONSUMER, successor.HOST))
        self.db.execute("UPDATE resources SET owner_session=NULL,reservation_id=NULL,token=NULL,granted_at=NULL,deadline=NULL,stale=0,reason=NULL WHERE name=?", (successor.RESOURCE,))
        self.db.commit()
        self.config = SimpleNamespace(master=self.master, plist=self.root / "service.plist",
                                      wrapper=self.root / "wrapper", executable=self.root / "server", uid=self.master.stat().st_uid)
        self.roots = SimpleNamespace(projects=SimpleNamespace(database=self.master, path=self.registry), path=lambda project: self.master)
        self.native_live = False
        self.native_checks = 0
        self.state = "idle"
        self.direct = True
        self.queue = 0
        self.shift = 0
        self.peer_error = False
        self.after_native = None
        self.patches = contextlib.ExitStack()
        self.addCleanup(self.patches.close)
        self.patches.enter_context(patch.object(root_local, "_installed_server_config", return_value=self.config))
        self.patches.enter_context(patch.object(root_local, "configured_server_database", return_value=self.master))
        self.patches.enter_context(patch.object(service, "_installed_server_config", return_value=self.config))
        self.patches.enter_context(patch.object(observer, "_native_observations", side_effect=self.native))
        self.patches.enter_context(patch.object(observer, "_consumer_native_observations", side_effect=self.native))
        self.patches.enter_context(allowance(self.root, self.master, self.registry))
        self.issue(successor.REQUEST)
        claim = self.request()
        self.release(claim)
        self.history = self.evidence()

    @contextlib.contextmanager
    def native(self, bindings, registration):
        self.assertFalse(self.native_live)
        self.native_live = True
        now = time.time() + self.shift
        values = {sid: {"thread_id": row["thread"], "host": row["host"],
                       "native_state": self.state, "direct_input": self.direct,
                       "queue_count": self.queue, "turn_id_known": False}
                  for sid, row in bindings.items()}
        def revalidate(current):
            self.assertTrue(self.native_live)
            self.native_checks += 1
            if self.peer_error:
                raise observer.Blocked("fixture peer generation changed")
        try:
            if self.after_native:
                self.after_native()
            yield values, now, now, revalidate
        finally:
            self.native_live = False

    def issue(self, request=successor.E2D_REQUEST, **changes):
        params = dict(master=self.master, registry=self.registry, home_project="default",
                      home_session=successor.ROOT, host_id=successor.HOST, target_project="default",
                      request_id=request, authorization_source_id=SOURCE_ID, confirm=True)
        params.update(changes)
        return successor.issue(self.master, **params)

    def request(self, session=successor.CONSUMER, roots=True, resource=successor.RESOURCE):
        return remote_service.dispatch(self.coord, {"op": "request", "session": session,
            "host_id": successor.HOST, "params": {"resource": resource, "minutes": 5}},
            canonical_project="default", roots=self.roots if roots else None)

    def release(self, claim):
        receipt = dict(version=1, resource=successor.RESOURCE, reservation_id=claim["reservation_id"],
                       restored=True, processes_closed=True, closed_at=time.time(), evidence="private-fixture.txt", pids=[])
        return remote_service.dispatch(self.coord, {"op": "release", "session": successor.CONSUMER,
            "host_id": successor.HOST, "params": {"resource": successor.RESOURCE,
            "token": claim["token"], "receipt": receipt, "evidence_base64": base64.b64encode(b"No process started in this isolated admission fixture.").decode()}})

    def evidence(self):
        tables = ("successor_admissions", "successor_admission_observations", "successor_admission_uses", "remote_guarded_runs")
        return {table: [tuple(row) for row in self.db.execute("SELECT * FROM " + table)] for table in tables}

    def test_actual_issue_dispatch_one_use_and_ordinary_return_keep_history(self):
        result = self.issue()
        self.assertEqual(result["unit"], successor.E2D_UNIT)
        self.state = "active"  # actual intake/essential ACK is a separate orchestration prerequisite
        claim = self.request()
        self.assertEqual(claim["state"], "owned")
        self.assertFalse(self.native_live)
        self.assertGreaterEqual(self.native_checks, 4)
        use = self.db.execute("SELECT resource,consumer,reservation_id FROM successor_admission_uses WHERE request_id=?", (successor.E2D_REQUEST,)).fetchone()
        self.assertEqual(tuple(use), (successor.RESOURCE, successor.CONSUMER, claim["reservation_id"]))
        self.assertEqual(self.request()["reservation_id"], claim["reservation_id"])
        self.assertTrue(self.release(claim)["released"])
        with self.assertRaisesRegex(CoordError, "unresolved open"):
            self.request()
        self.state = "idle"
        with self.assertRaises(successor.DiagnosticRefused) as recorded:
            self.issue()
        self.assertEqual(recorded.exception.predicate, "attempt_already_recorded")
        for table, rows in self.history.items():
            self.assertEqual([tuple(row) for row in self.db.execute("SELECT * FROM " + table)][:len(rows)], rows)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM successor_admission_uses").fetchone()[0], 2)

    def test_producer_refuses_wrong_packet_or_nonidle_native_without_write(self):
        for changes in ({"request_id": "Measurement02"}, {"home_session": "wrong"},
                        {"host_id": "wrong"}, {"target_project": "other"},
                        {"authorization_source_id": "message_wrong"}, {"confirm": False}):
            with self.subTest(changes=changes), self.assertRaises(successor.Refused):
                self.issue(**changes)
        self.assertEqual(self.evidence(), self.history)

    def test_synthetic_native_active_refused(self):
        before = self.evidence()
        self.state, self.direct, self.queue = "active", True, 0
        with self.assertRaises(successor.DiagnosticRefused) as caught: self.issue()
        self.assertEqual(caught.exception.predicate, "native_tuple_differs")
        self.assertEqual(self.evidence(), before)

    def test_synthetic_native_direct_false_refused(self):
        before = self.evidence()
        self.state, self.direct, self.queue = "idle", False, 0
        with self.assertRaises(successor.DiagnosticRefused) as caught: self.issue()
        self.assertEqual(caught.exception.predicate, "native_direct_input_false")
        self.assertEqual(self.evidence(), before)

    def test_synthetic_native_queued_refused(self):
        before = self.evidence()
        self.state, self.direct, self.queue = "idle", True, 1
        with self.assertRaises(successor.DiagnosticRefused) as caught: self.issue()
        self.assertEqual(caught.exception.predicate, "native_tuple_differs")
        self.assertEqual(self.evidence(), before)


    def test_e2d_requires_matching_spent_predecessor(self):
        # A second fresh isolated DB models missing/mismatched historical custody.
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute("UPDATE successor_admission_uses SET consumer='wrong'")
        with self.assertRaises(sqlite3.IntegrityError):
            self.db.execute("DELETE FROM successor_admissions")
        self.assertEqual(self.evidence(), self.history)
        with tempfile.TemporaryDirectory() as tmp:
            other, master, registry = make_root_fixture(Path(tmp))
            try:
                with self.assertRaisesRegex(successor.Refused, "matching spent"):
                    successor._require_e2d_predecessor(other.db)
                ticket = self.history["successor_admissions"][0]
                other.db.execute("INSERT INTO successor_admissions VALUES(" + ",".join("?" for _ in ticket) + ")", ticket)
                other.db.execute("INSERT INTO successor_admission_uses VALUES(?,?,?,?,?)",
                                 (successor.REQUEST, successor.RESOURCE, "wrong-consumer", "different-use", time.time()))
                with self.assertRaisesRegex(successor.Refused, "matching spent"):
                    successor._require_e2d_predecessor(other.db)
            finally: other.close()

    def test_consumer_refuses_wrong_owner_and_native_inputs_without_use(self):
        self.issue()
        before = self.evidence()
        for state, direct, queue in (("notLoaded", True, 0), ("active", False, 0), ("idle", True, 1)):
            self.state, self.direct, self.queue = state, direct, queue
            with self.assertRaises(successor.Refused): self.request()
        self.state, self.direct, self.queue = "idle", True, 0
        with self.assertRaisesRegex(CoordError, "requester differs"):
            self.request(session="new-worker")
        with self.assertRaisesRegex(CoordError, "requester differs"):
            self.request(roots=False)
        self.assertEqual(self.evidence(), before)

    def test_peer_failure_after_acquire_rolls_back_claim_outcome_and_use(self):
        self.issue()
        before = self.evidence()
        original = self.coord._acquire
        def acquire(*args, **kwargs):
            value = original(*args, **kwargs)
            self.peer_error = True
            return value
        with patch.object(self.coord, "_acquire", side_effect=acquire):
            with self.assertRaisesRegex(successor.Refused, "successor admission changed"):
                self.request()
        self.assertEqual(self.evidence(), before)
        self.assertIsNone(self.db.execute("SELECT owner_session FROM resources WHERE name=?", (successor.RESOURCE,)).fetchone()[0])
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM remote_reservation_outcomes WHERE state='pending'").fetchone()[0], 0)

    def test_producer_peer_failure_is_zero_requested_write(self):
        self.peer_error = True
        with self.assertRaises(successor.DiagnosticRefused) as caught: self.issue()
        self.assertIsInstance(caught.exception.__cause__, observer.Blocked)
        self.assertEqual(self.evidence(), self.history)

    def test_observation_expiry_is_zero_requested_write(self):
        self.shift = -6
        with self.assertRaises(successor.DiagnosticRefused) as caught: self.issue()
        self.assertIsInstance(caught.exception.__cause__, old.AdmissionRefused)
        self.assertEqual(self.evidence(), self.history)

    def test_producer_registry_change_after_native_refuses(self):
        def change():
            with contextlib.closing(sqlite3.connect(self.registry)) as db:
                with db: db.execute("UPDATE root_config SET mode='detached'")
        self.after_native = change
        with self.assertRaises(successor.DiagnosticRefused) as caught: self.issue()
        self.assertIsInstance(caught.exception.__cause__, old.AdmissionRefused)
        self.assertEqual(self.evidence(), self.history)

    def test_registry_first_lock_and_native_context_hold_through_target_commit(self):
        self.issue()
        events = []
        original = old.registry_authority_gate
        @contextlib.contextmanager
        def gate(*args):
            self.assertTrue(self.native_live)
            with original(*args) as value:
                events.append("registry")
                yield value
                events.append("registry-release")
        def trace(sql):
            if sql == "BEGIN IMMEDIATE": events.append("target")
            if sql == "COMMIT":
                if "registry" in events:
                    self.assertTrue(self.native_live)
                    events.append("target-commit")
        self.db.set_trace_callback(trace)
        with patch.object(old, "registry_authority_gate", side_effect=gate):
            self.assertEqual(self.request()["state"], "owned")
        self.db.set_trace_callback(None)
        self.assertLess(events.index("registry"), events.index("target", events.index("registry")))
        self.assertLess(events.index("target-commit"), events.index("registry-release"))

    def test_managed_ticket_appearing_after_no_ticket_observation_fails_closed(self):
        # Explicitly inject at the request boundary without replacing producer semantics.
        original_observe = successor.observe_for_request
        @contextlib.contextmanager
        def observation(*args):
            with original_observe(*args) as native:
                self.assertIsNone(native)
                self.issue()
                yield native
        with patch.object(successor, "observe_for_request", side_effect=observation):
            with self.assertRaisesRegex(CoordError, "requester differs"): self.request()
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM successor_admission_uses WHERE request_id=?", (successor.E2D_REQUEST,)).fetchone()[0], 0)

    def test_ordinary_no_ticket_and_alternate_installation_keep_ordinary_path(self):
        with patch.object(observer, "_consumer_native_observations", side_effect=AssertionError("no native read expected")):
            self.assertEqual(self.request(resource="private-ordinary")["state"], "owned")
            config = SimpleNamespace(master=self.root / "different-master")
            with patch.object(service, "_installed_server_config", return_value=config):
                with self.assertRaisesRegex(CoordError, "unresolved open"): self.request()
        self.issue()
        with patch.object(service, "_installed_server_config", return_value=SimpleNamespace(master=self.root / "other")):
            with self.assertRaisesRegex(CoordError, "requester differs"): self.request()

    def test_root_local_fixed_entry_dispatches_real_e2d_producer(self):
        options = dict(master_db=self.master, service_plist=self.config.plist, home_project="default",
                       session=successor.ROOT, host_id=successor.HOST)
        result = root_local.execute("capacity-successor-e2d", {"confirm": True}, **options)
        self.assertEqual(result["request_id"], successor.E2D_REQUEST)
        with self.assertRaises(CoordError):
            root_local.execute("capacity-successor-e2d", {"confirm": True, "request_id": successor.REQUEST}, **options)

    def test_minimum_maintained_cli_parses_and_issues_only_fixed_packet(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            result = root_local.main(["--home-project", "default", "--home-session", successor.ROOT,
                                      "--host-id", successor.HOST, "capacity-successor-e2d", "--confirm"])
        self.assertEqual(result, 0)
        parsed = subprocess.run(["jq", "-er", ".request_id + \"|\" + .unit"],
                                input=output.getvalue(), text=True, capture_output=True, check=True)
        self.assertEqual(parsed.stdout.strip(), successor.E2D_REQUEST + "|" + successor.E2D_UNIT)

    def managed(self):
        self.db.executescript("""CREATE TABLE project_orchestrators(session_id TEXT PRIMARY KEY,root_session TEXT);
          CREATE TABLE agent_management(target_session TEXT PRIMARY KEY,manager_session TEXT,request_id TEXT);
          CREATE TABLE agent_management_requests(id TEXT PRIMARY KEY,root_session TEXT,manager_session TEXT,
            target_session TEXT,expected_thread TEXT,expected_host TEXT,created_at REAL);""")
        self.db.execute("INSERT INTO project_orchestrators VALUES(?,?)", ("target-po-session", successor.ROOT))
        self.db.execute("UPDATE bridge_bindings SET parent_session=NULL WHERE session_id=?", (successor.CONSUMER,))
        self.db.execute("INSERT INTO agent_management VALUES(?,?,?)", (successor.CONSUMER, "target-po-session", "management-fixture"))
        self.db.execute("INSERT INTO agent_management_requests VALUES(?,?,?,?,?,?,?)",
                        ("management-fixture", successor.ROOT, "target-po-session", successor.CONSUMER,
                         successor.THREAD, successor.HOST, time.time()))
        self.db.commit()

    def test_managed_consumer_exact_authority_succeeds(self):
        self.managed()
        self.issue()
        self.assertEqual(self.request()["state"], "owned")
        authority = self.db.execute("SELECT authority_kind,management_request FROM successor_admissions WHERE request_id=?", (successor.E2D_REQUEST,)).fetchone()
        self.assertEqual(tuple(authority), ("managed", "management-fixture"))

    def test_managed_authority_change_after_observation_or_issue_refuses(self):
        self.managed()
        def change():
            self.db.execute("UPDATE agent_management SET manager_session='different-manager'")
            self.db.commit()
        self.after_native = change
        with self.assertRaises(successor.DiagnosticRefused) as caught: self.issue()
        self.assertIsInstance(caught.exception.__cause__, observer.Blocked)
        self.assertEqual(self.evidence(), self.history)

    def test_managed_authority_change_after_issue_refuses(self):
        self.managed()
        def change():
            self.db.execute("UPDATE agent_management SET manager_session='different-manager'")
            self.db.commit()
        self.issue()
        before = self.evidence()
        change()
        with self.assertRaisesRegex(CoordError, "requester differs"): self.request()
        self.assertEqual(self.evidence(), before)

    def test_fifo_before_issue_refuses_and_after_issue_preserves_waiter(self):
        self.db.execute("INSERT INTO resource_queue(resource,session,queued_at) VALUES(?,?,?)",
                        (successor.RESOURCE, "new-worker", time.time()))
        self.db.commit()
        with self.assertRaises(successor.DiagnosticRefused) as caught: self.issue()
        self.assertIn("FIFO", str(caught.exception.__cause__))
        self.assertEqual(self.evidence(), self.history)

    def test_fifo_after_issue_preserves_waiter(self):
        self.issue()
        self.db.execute("INSERT INTO resource_queue(resource,session,queued_at) VALUES(?,?,?)",
                        (successor.RESOURCE, "new-worker", time.time()))
        self.db.commit()
        self.assertEqual(self.request()["state"], "queued")
        first = self.db.execute("SELECT session FROM resource_queue WHERE resource=? ORDER BY seq LIMIT 1", (successor.RESOURCE,)).fetchone()[0]
        self.assertEqual(first, "new-worker")
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM successor_admission_uses WHERE request_id=?", (successor.E2D_REQUEST,)).fetchone()[0], 0)

    def test_generation_change_during_acquire_rolls_back_use(self):
        self.issue()
        before = self.evidence()
        original = self.coord._acquire
        def acquire(*args, **kwargs):
            value = original(*args, **kwargs)
            self.db.execute("UPDATE root_guard_generation SET generation=generation+1")
            return value
        with patch.object(self.coord, "_acquire", side_effect=acquire):
            with self.assertRaises(successor.Refused): self.request()
        self.assertEqual(self.evidence(), before)

    def test_wal_reader_cannot_see_uncommitted_ticket_and_consumer_cannot_replay(self):
        with contextlib.closing(sqlite3.connect(self.master)) as reader:
            reader.execute("BEGIN")
            self.assertEqual(reader.execute("SELECT COUNT(*) FROM successor_admissions").fetchone()[0], 1)
            self.issue()
            self.assertEqual(reader.execute("SELECT COUNT(*) FROM successor_admissions").fetchone()[0], 1)
            reader.rollback()
            self.assertEqual(reader.execute("SELECT COUNT(*) FROM successor_admissions").fetchone()[0], 2)
        claim = self.request()
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM successor_admission_uses WHERE request_id=?", (successor.E2D_REQUEST,)).fetchone()[0], 1)
        self.release(claim)
        with self.assertRaises(CoordError): self.request()

    def test_configured_provider_and_canonical_target_cannot_be_supplied_by_caller(self):
        with self.assertRaises(old.AdmissionRefused):
            self.issue(master=self.root / "other.sqlite3")
        with self.assertRaises(old.AdmissionRefused):
            self.issue(registry=self.master)
        with patch.object(root_local, "configured_server_database", return_value=self.root / "other.sqlite3"):
            with self.assertRaises(old.AdmissionRefused): self.issue()
        self.assertEqual(self.evidence(), self.history)


if __name__ == "__main__":
    unittest.main()
