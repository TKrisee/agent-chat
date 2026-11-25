from __future__ import annotations

import sqlite3
import os
import contextlib
import io
import inspect
import shutil
import sys
import tempfile
import threading
import time
import uuid
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from agent_chat import root_lifecycle, root_restart
from agent_chat.core import Coordinator
from agent_chat import capacity_admission, cli, root_local
from agent_chat.capacity_admission import AdmissionRefused, admit_current, effective_open_guards
from agent_chat.guard_generation import install_guard_generation

ROOT_PROJECT = "default"
ROOT_SESSION = "session_tcnSV4Qo3EH4YU3gg2swqjO_GS1QQ9Fv"
ROOT_THREAD = "01a1448a-c377-73e0-9025-02c4f2f6f2d9"
HOST = "host_o6KZLd0kkjchCaH8OjoJZ9aZXHDlFiE2"
TARGET_PROJECT = "default"
PROJECT_ROOT = ROOT_SESSION
PO_SESSION = "target-po-session"
OWNER_A = "session_d96238daad0345b699ec1dd1972ab11b"
OWNER_B = "session_92cd8bfd4ee045798585e1f873c94d55"
THREAD_A = "01a11d1f-bb15-7750-affe-3029d7b4057d"
THREAD_B = "01a11d1f-cb9d-7d53-8aef-8d16a71502ef"
NOW = time.time()
BOOT = NOW - 300
SOURCE_ID = "message_KJqomMYywtU6GYfczB_fxftks4D_gz6y"
OPERATOR = "web_operator_qYmkTK9qVoubfWPPTLr8iSzaIKqKk3Si"
REQUEST_ID = "C34EBA6F-6562-4FF1-8984-D47A0685E2B5"
OPERATOR_BODY = ("set the unity projects playable build as a goal for the unity PO and yourself. "
                 "should any blockers arise that are from either codex or the agent-chat app "
                 "I authorize you to take ANY action that allows progression!")


def make_root_fixture(root: Path):
    now = time.time()
    master_path = root / "master.sqlite3"
    coord = Coordinator(master_path, session=OWNER_A)
    db = coord.db
    db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('project_id',?)", (TARGET_PROJECT,))
    db.execute("CREATE TABLE IF NOT EXISTS bridge_bindings(session_id TEXT PRIMARY KEY,thread_id TEXT NOT NULL,parent_session TEXT)")
    db.execute("CREATE TABLE IF NOT EXISTS remote_session_hosts(session_id TEXT PRIMARY KEY,host_id TEXT NOT NULL,registered_at REAL NOT NULL DEFAULT 0,last_seen_at REAL NOT NULL DEFAULT 0)")
    db.execute("CREATE TABLE IF NOT EXISTS remote_guarded_runs(run_id TEXT PRIMARY KEY,resource TEXT NOT NULL,reservation_id TEXT NOT NULL,session_id TEXT NOT NULL,host_id TEXT NOT NULL,local_pid INTEGER NOT NULL,started_at REAL NOT NULL,closed_at REAL,close_evidence_sha256 TEXT)")
    install_guard_generation(db)
    db.execute("CREATE TABLE IF NOT EXISTS bridge_dispatcher(host_id TEXT PRIMARY KEY,owner TEXT,acquired_at REAL)")
    db.execute("CREATE TABLE IF NOT EXISTS bridge_client_runtime(host_id TEXT PRIMARY KEY,server TEXT,pid INTEGER,heartbeat REAL,error TEXT)")
    db.execute("INSERT INTO bridge_dispatcher VALUES(?,?,?)", (HOST, "fixture-bridge", now))
    db.execute("INSERT INTO bridge_client_runtime VALUES(?,?,?,?,NULL)", (HOST, "ws://127.0.0.1:34971", 42, now))
    db.execute("INSERT OR IGNORE INTO sessions(id,agent,registered_at,inbox_read_seq) VALUES(?,?,?,0)",
               (ROOT_SESSION, "root", now - 1000))
    db.execute("INSERT OR REPLACE INTO bridge_bindings VALUES(?,?,?)", (ROOT_SESSION, ROOT_THREAD, None))
    db.execute("INSERT OR REPLACE INTO remote_session_hosts(session_id,host_id) VALUES(?,?)", (ROOT_SESSION, HOST))
    for owner, thread in ((OWNER_A, THREAD_A), (OWNER_B, THREAD_B)):
        db.execute("INSERT OR IGNORE INTO sessions(id,agent,registered_at,inbox_read_seq) VALUES(?,?,?,0)",
                   (owner, owner, now - 100))
        db.execute("INSERT OR REPLACE INTO bridge_bindings VALUES(?,?,?)", (owner, thread, PROJECT_ROOT))
        db.execute("INSERT OR REPLACE INTO remote_session_hosts(session_id,host_id) VALUES(?,?)", (owner, HOST))
    db.execute("INSERT OR IGNORE INTO sessions(id,agent,registered_at,inbox_read_seq) VALUES(?,?,?,0)",
               ("new-worker", "new-worker", now - 1))
    db.execute("INSERT INTO operator_identities VALUES(?)", (OPERATOR,))
    db.execute("INSERT INTO messages(id,sender_session,recipient_session,body,created_at,acked_at) VALUES(?,?,?,?,?,NULL)",
               (SOURCE_ID, OPERATOR, ROOT_SESSION, OPERATOR_BODY, 1793402570.231309))
    db.execute("INSERT INTO operator_message_origins VALUES(?,?,?)",
               (SOURCE_ID, OPERATOR, "authenticated_operator_ui"))
    claim_rows = [
        ("validation-clone", OWNER_A, "reservation_ozTM0ysovUwnzjB95HWDce4hRIw4bP__",
         1791670463.859751939, 1791669263.859751939, "old-token-a"),
        ("heavy-unity:2", OWNER_B, "reservation_2_fDGXtQwqWdiIVAtWRKC0QXJquAH2wK",
         1793322078.301824092, 1793318478.301824092, "old-token-b"),
    ]
    for resource, owner, rid, deadline, granted, token in claim_rows:
        db.execute("INSERT INTO resources(name,owner_session,reservation_id,token,granted_at,deadline,stale,reason) "
                   "VALUES(?,?,?,?,?,?,1,NULL)", (resource, owner, rid, token, granted, deadline))
    db.execute("INSERT INTO remote_guarded_runs VALUES(?,?,?,?,?,?,?,NULL,NULL)",
               ("remote_run_d6b4b581f256e9f9c2b0ed9954221b3f", "validation-clone",
                "reservation_ozTM0ysovUwnzjB95HWDce4hRIw4bP__", OWNER_A,
                HOST, 27633, 1791669860.716338873))
    db.execute("INSERT INTO resource_queue(resource,seq,session,queued_at) VALUES(?,?,?,?)",
               ("heavy-unity:1", 254, OWNER_B, 1791979035.772598028))
    db.commit()
    registry_path = root / "master.sqlite3.projects.sqlite3"
    registry = sqlite3.connect(registry_path)
    registry.executescript("""
      CREATE TABLE root_config(singleton INTEGER PRIMARY KEY,home_project TEXT,home_session TEXT,
        thread_id TEXT,host_id TEXT,mode TEXT);
      CREATE TABLE root_directory_owner(singleton INTEGER PRIMARY KEY,home_project TEXT,home_session TEXT);
      CREATE TABLE root_projects(project_id TEXT PRIMARY KEY,session_id TEXT,orchestrator_session TEXT,joined INTEGER);
      CREATE TABLE projects(id TEXT PRIMARY KEY);
      CREATE TABLE root_fresh_requests(id TEXT PRIMARY KEY,status TEXT);
      CREATE TABLE root_team_operations(id TEXT PRIMARY KEY,status TEXT);
    """)
    registry.execute("INSERT INTO root_config VALUES(1,?,?,?,?, 'running')",
                     (ROOT_PROJECT, ROOT_SESSION, ROOT_THREAD, HOST))
    registry.execute("INSERT INTO root_directory_owner VALUES(1,?,?)", (ROOT_PROJECT, ROOT_SESSION))
    registry.execute("INSERT INTO root_projects VALUES(?,?,?,1)", (TARGET_PROJECT, ROOT_SESSION, PO_SESSION))
    registry.execute("INSERT INTO projects VALUES(?)", (TARGET_PROJECT,))
    registry.commit(); registry.close()
    for name in ("service.plist", "wrapper", "server"):
        (root / name).write_text("private fixture", encoding="utf-8")
    return coord, master_path.resolve(), registry_path.resolve()


def make_fixture(root: Path):
    coord, master_path, registry_path = make_root_fixture(root)
    return coord, master_path, registry_path, master_path


def invoke(coord, master_path, registry_path, target_path, *, active=False, observation_shift=0,
           after_native_observation=None, action_peer_check=None, native_factory=None):
    from agent_chat import capacity_observer
    root = Path(master_path).parent
    config = SimpleNamespace(master=Path(master_path), plist=root / "service.plist",
                             wrapper=root / "wrapper", executable=root / "server")
    @contextlib.contextmanager
    def fake_native(bindings, registration):
        checked_at = time.time() + observation_shift
        observed = {owner: {"thread_id": item["thread"], "host": item["host"],
            "native_state": "active" if active else "idle", "direct_input": True,
            "queue_count": 0, "turn_id_known": False, "checked_at": checked_at}
            for owner, item in bindings.items()}
        if after_native_observation is not None:
            after_native_observation()
        yield observed, checked_at, checked_at, (action_peer_check or (lambda registration: None))
    def fake_guards(guards, _claims, _host, _boot):
        return {resource: [{"run_id": row["run_id"], "state": "OPEN",
            "original_process": "UNKNOWN", "current_pid_observation": "UNKNOWN",
            "process_group": "UNKNOWN"} for row in rows]
            for resource, rows in guards.items()}
    with (patch.object(root_local, "_installed_server_config", return_value=config),
          patch.object(root_local, "configured_server_database", return_value=Path(master_path)),
          patch.object(capacity_observer, "_boot_time", return_value=BOOT),
          patch.object(capacity_observer, "_native_observations", side_effect=native_factory or fake_native),
          patch.object(capacity_observer, "_guard_current_facts", side_effect=fake_guards)):
        return admit_current(target_path, master=master_path, registry=registry_path,
            home_project=ROOT_PROJECT, home_session=ROOT_SESSION, host_id=HOST,
            target_project=TARGET_PROJECT, request_id=REQUEST_ID,
            authorization_source_id=SOURCE_ID, confirm=True)


class CapacityAdmissionTests(unittest.TestCase):
    def setUp(self):
        # These fixture-only substitutions must not escape into later modules.
        for obj, name, replacement in (
            (root_lifecycle, 'schema', lambda _db: None),
            (root_lifecycle, 'stopped', lambda _db, _session: False),
            (root_lifecycle, 'task_execution_admission', lambda _db, _message_id: None),
            (root_restart, 'held', lambda _db, _session: False),
        ):
            guard = patch.object(obj, name, replacement)
            guard.start()
            self.addCleanup(guard.stop)

    def test_root_local_cli_dispatch_reaches_capacity_admission_handler(self):
        with tempfile.TemporaryDirectory(prefix="capacity-admission-cli-") as temporary:
            root = Path(temporary).resolve()
            coord, master, registry = make_root_fixture(root)
            plist, wrapper, executable = (root / name for name in ("service.plist", "wrapper", "server"))
            config = SimpleNamespace(master=master, plist=plist, wrapper=wrapper, executable=executable)
            from agent_chat import capacity_observer
            @contextlib.contextmanager
            def fake_native(bindings, registration):
                checked_at = time.time()
                observed = {owner: {"thread_id": item["thread"], "host": item["host"],
                    "native_state": "idle", "direct_input": True, "queue_count": 0,
                    "turn_id_known": False, "checked_at": checked_at} for owner, item in bindings.items()}
                yield observed, checked_at, checked_at, (lambda registration: None)
            def fake_guards(guards, _claims, _host, _boot):
                return {resource: [{"run_id": row["run_id"], "state": "OPEN",
                    "original_process": "UNKNOWN", "current_pid_observation": "UNKNOWN",
                    "process_group": "UNKNOWN"} for row in rows]
                    for resource, rows in guards.items()}
            argv = ["root-local", "--home-project", ROOT_PROJECT, "--home-session", ROOT_SESSION,
                    "--host-id", HOST, "capacity-admit", "--project", TARGET_PROJECT,
                    "--request-id", REQUEST_ID,
                    "--authorization-source-id", SOURCE_ID, "--confirm"]
            stdout = io.StringIO()
            with (patch.object(root_local, "_installed_server_config", return_value=config),
                  patch.object(root_local, "configured_server_database", return_value=master),
                  patch.object(capacity_observer, "_boot_time", return_value=BOOT),
                  patch.object(capacity_observer, "_native_observations", side_effect=fake_native),
                  patch.object(capacity_observer, "_guard_current_facts", side_effect=fake_guards),
                  contextlib.redirect_stdout(stdout)):
                rc = cli._dispatch_main(argv, SimpleNamespace(), None, None, None)
            self.assertEqual(rc, 0)
            self.assertIn('"operation": "capacity-admit"', stdout.getvalue())
            self.assertEqual(tuple(coord.db.execute(
                "SELECT request_id,authorization_source_id FROM capacity_admissions").fetchone()),
                (REQUEST_ID, SOURCE_ID))
            self.assertEqual(coord.db.execute("SELECT COUNT(*) FROM capacity_admission_uses").fetchone()[0], 0)
            self.assertEqual(coord.db.execute("SELECT COUNT(*) FROM remote_guarded_runs WHERE closed_at IS NULL").fetchone()[0], 1)
            self.assertEqual([tuple(row) for row in coord.db.execute(
                "SELECT resource,seq,session,queued_at FROM resource_queue")],
                [("heavy-unity:1", 254, OWNER_B, 1791979035.772598028)])
            coord.close()

    def test_exact_two_row_admission_preserves_open_history_and_allows_one_fresh_request(self):
        with tempfile.TemporaryDirectory(prefix="capacity-admission-v4-") as temporary:
            base = Path(temporary)
            coord, master, registry, target = make_fixture(base)
            db = coord.db
            before_guard = tuple(db.execute("SELECT * FROM remote_guarded_runs WHERE run_id='remote_run_d6b4b581f256e9f9c2b0ed9954221b3f'").fetchone())
            result = invoke(coord, master, registry, target)
            self.assertEqual(result["resources"], ["validation-clone", "heavy-unity:2"])
            self.assertEqual(result["cleared_claims"], 2)
            self.assertEqual(result["preserved_open_guards"], 1)
            self.assertFalse(result["ordinary_receipt"])
            self.assertEqual(result["process_state"], "UNKNOWN")
            self.assertEqual(result["restoration_state"], "UNKNOWN")
            self.assertEqual(db.execute("SELECT COUNT(*) FROM remote_guarded_runs WHERE closed_at IS NULL").fetchone()[0], 1)
            self.assertEqual(tuple(db.execute("SELECT * FROM remote_guarded_runs WHERE run_id='remote_run_d6b4b581f256e9f9c2b0ed9954221b3f'").fetchone()), before_guard)
            self.assertEqual([tuple(row) for row in db.execute(
                "SELECT resource,seq,session,queued_at FROM resource_queue ORDER BY resource,seq")],
                [("heavy-unity:1", 254, OWNER_B, 1791979035.772598028)])
            audit = db.execute("SELECT outcome,ordinary_receipt,process_state,restoration_state,fresh_ownership_required FROM capacity_admissions").fetchone()
            self.assertEqual(tuple(audit), ("ROOT_ADMIN_NONORDINARY_CAPACITY_ADMISSION", 0, "UNKNOWN", "UNKNOWN", 1))
            self.assertEqual(db.execute("SELECT COUNT(*) FROM capacity_admission_claims").fetchone()[0], 2)

            coord.session = "new-worker"
            for resource in ("validation-clone", "heavy-unity:2"):
                fresh = coord.request(resource, 5)
                self.assertEqual(fresh["state"], "owned")
                self.assertNotIn(fresh["reservation_id"], {"old-validation-rid", "old-heavy2-rid"})
            uses = db.execute("SELECT resource,session FROM capacity_admission_uses ORDER BY resource").fetchall()
            self.assertEqual([tuple(row) for row in uses], [("heavy-unity:2", "new-worker"), ("validation-clone", "new-worker")])
            self.assertEqual(effective_open_guards(db, "validation-clone"),
                             [("remote_guarded_runs", "remote_run_d6b4b581f256e9f9c2b0ed9954221b3f")])
            self.assertEqual(db.execute("SELECT COUNT(*) FROM resource_queue").fetchone()[0], 1)
            coord.close()

    def test_busy_native_owner_refuses_without_any_audit_or_claim_clear(self):
        with tempfile.TemporaryDirectory(prefix="capacity-admission-busy-") as temporary:
            base = Path(temporary)
            coord, master, registry, target = make_fixture(base)
            before = [tuple(row) for row in coord.db.execute("SELECT name,owner_session,reservation_id,deadline,stale FROM resources ORDER BY name")]
            with self.assertRaisesRegex(Exception, "not positively idle"):
                invoke(coord, master, registry, target, active=True)
            after = [tuple(row) for row in coord.db.execute("SELECT name,owner_session,reservation_id,deadline,stale FROM resources ORDER BY name")]
            self.assertEqual(after, before)
            self.assertEqual(coord.db.execute("SELECT COUNT(*) FROM capacity_admissions").fetchone()[0], 0)
            coord.close()

    def test_raw_open_guard_without_a_fresh_unused_admission_stays_blocked(self):
        with tempfile.TemporaryDirectory(prefix="capacity-admission-guard-") as temporary:
            coord, _, _, _ = make_fixture(Path(temporary))
            self.assertEqual(effective_open_guards(coord.db, "validation-clone"),
                             [("remote_guarded_runs", "remote_run_d6b4b581f256e9f9c2b0ed9954221b3f")])
            coord.close()

    def test_guard_generation_change_invalidates_admission_before_fresh_acquire(self):
        with tempfile.TemporaryDirectory(prefix="capacity-admission-generation-") as temporary:
            coord, master, registry, target = make_fixture(Path(temporary))
            invoke(coord, master, registry, target)
            before = tuple(coord.db.execute(
                "SELECT name,owner_session,reservation_id,deadline FROM resources "
                "WHERE name='validation-clone'").fetchone())
            coord.db.execute("UPDATE root_guard_generation SET generation=generation+1 WHERE singleton=1")
            coord.session = "new-worker"
            with self.assertRaisesRegex(Exception, "pending Root admission binding/generation"):
                coord.request("validation-clone", 5)
            after = tuple(coord.db.execute(
                "SELECT name,owner_session,reservation_id,deadline FROM resources "
                "WHERE name='validation-clone'").fetchone())
            self.assertEqual(after, before)
            self.assertEqual(coord.db.execute("SELECT COUNT(*) FROM capacity_admission_uses").fetchone()[0], 0)
            coord.close()

    def test_guard_free_heavy2_refuses_after_generation_change_before_claim_or_use(self):
        with tempfile.TemporaryDirectory(prefix="capacity-admission-heavy2-generation-") as temporary:
            coord, master, registry, target = make_fixture(Path(temporary))
            invoke(coord, master, registry, target)
            before = tuple(coord.db.execute(
                "SELECT name,owner_session,reservation_id,deadline FROM resources "
                "WHERE name='heavy-unity:2'").fetchone())
            coord.db.execute("UPDATE root_guard_generation SET generation=generation+1 WHERE singleton=1")
            coord.session = "new-worker"
            with self.assertRaisesRegex(Exception, "pending Root admission binding/generation"):
                coord.request("heavy-unity:2", 5)
            after = tuple(coord.db.execute(
                "SELECT name,owner_session,reservation_id,deadline FROM resources "
                "WHERE name='heavy-unity:2'").fetchone())
            self.assertEqual(after, before)
            self.assertEqual(coord.db.execute("SELECT COUNT(*) FROM capacity_admission_uses").fetchone()[0], 0)
            coord.close()

    def test_guard_free_heavy2_refuses_after_owner_thread_binding_change(self):
        with tempfile.TemporaryDirectory(prefix="capacity-admission-heavy2-binding-") as temporary:
            coord, master, registry, target = make_fixture(Path(temporary))
            invoke(coord, master, registry, target)
            before = tuple(coord.db.execute(
                "SELECT name,owner_session,reservation_id,deadline FROM resources "
                "WHERE name='heavy-unity:2'").fetchone())
            coord.db.execute("UPDATE bridge_bindings SET thread_id='rebound-thread' WHERE session_id=?",
                             (OWNER_B,))
            coord.session = "new-worker"
            with self.assertRaisesRegex(Exception, "pending Root admission binding/generation"):
                coord.request("heavy-unity:2", 5)
            after = tuple(coord.db.execute(
                "SELECT name,owner_session,reservation_id,deadline FROM resources "
                "WHERE name='heavy-unity:2'").fetchone())
            self.assertEqual(after, before)
            self.assertEqual(coord.db.execute("SELECT COUNT(*) FROM capacity_admission_uses").fetchone()[0], 0)
            coord.close()

    def test_untrusted_operator_source_variants_refuse(self):
        cases = (
            ("non-operator", "UPDATE messages SET sender_session='not-an-operator' WHERE id=?",
             "UPDATE operator_message_origins SET sender_session='not-an-operator' WHERE message_id=?",
             None),
            ("wrong-root", "UPDATE messages SET recipient_session='other-root' WHERE id=?", None, None),
            ("forged-origin", None,
             "UPDATE operator_message_origins SET sender_session='different-origin' WHERE message_id=?",
             None),
            ("batched", None, None, "INSERT INTO message_batches VALUES(?, 'fixture-batch')"),
        )
        for label, message_sql, origin_sql, batch_sql in cases:
            with self.subTest(label=label), tempfile.TemporaryDirectory(prefix="capacity-admission-auth-") as temporary:
                coord, master_path, registry, target = make_fixture(Path(temporary))
                master = sqlite3.connect(master_path)
                master.execute("DROP TRIGGER IF EXISTS immutable_operator_origin_update")
                if message_sql:
                    master.execute(message_sql, (SOURCE_ID,))
                if origin_sql:
                    master.execute(origin_sql, (SOURCE_ID,))
                if batch_sql:
                    master.execute(batch_sql, (SOURCE_ID,))
                master.commit(); master.close()
                with self.assertRaises(AdmissionRefused):
                    invoke(coord, master_path, registry, target)
                self.assertEqual(coord.db.execute("SELECT COUNT(*) FROM capacity_admissions").fetchone()[0], 0)
                coord.close()

    def test_unrelated_direct_operator_message_does_not_authorize_two_resource_scope(self):
        with tempfile.TemporaryDirectory(prefix="capacity-admission-auth-scope-") as temporary:
            coord, master_path, registry, target = make_fixture(Path(temporary))
            master = sqlite3.connect(master_path)
            master.execute("UPDATE messages SET body='Unrelated direct operator note' WHERE id=?", (SOURCE_ID,))
            master.commit(); master.close()
            with self.assertRaises(AdmissionRefused):
                invoke(coord, master_path, registry, target)
            coord.close()

    def test_mismatched_request_or_target_scope_refuses_before_observation(self):
        with tempfile.TemporaryDirectory(prefix="capacity-admission-scope-") as temporary:
            coord, master, registry, target = make_fixture(Path(temporary))
            from agent_chat import capacity_observer
            for changes in (
                {"request_id": str(uuid.uuid4())},
                {"target_project": "other-project"},
                {"home_session": "other-root"},
                {"host_id": "other-host"},
                {"authorization_source_id": "message_unrelatedOperatorMessage000001"},
            ):
                args = dict(master=master, registry=registry, home_project=ROOT_PROJECT,
                    home_session=ROOT_SESSION, host_id=HOST, target_project=TARGET_PROJECT,
                    request_id=REQUEST_ID, authorization_source_id=SOURCE_ID, confirm=True)
                args.update(changes)
                with (self.subTest(changes=changes), self.assertRaises(AdmissionRefused),
                      patch.object(capacity_observer, "_native_observations",
                                   side_effect=AssertionError("observer must not run"))):
                    admit_current(target, **args)
            self.assertEqual(coord.db.execute("SELECT COUNT(*) FROM capacity_admissions").fetchone()[0], 0)
            coord.close()

    def test_direct_writer_requires_configured_root_preflight_and_has_no_injection_api(self):
        with tempfile.TemporaryDirectory(prefix="capacity-admission-direct-bypass-") as temporary:
            coord, master, registry, target = make_fixture(Path(temporary))
            self.assertFalse({"_rpc_factory", "_boot_provider", "_kernel_factory", "_now"} &
                             set(inspect.signature(admit_current).parameters))
            root = Path(master).parent
            mismatched_config = SimpleNamespace(master=root / "other-master.sqlite3",
                plist=root / "service.plist", wrapper=root / "wrapper", executable=root / "server")
            from agent_chat import capacity_observer
            with (patch.object(root_local, "_installed_server_config", return_value=mismatched_config),
                  patch.object(root_local, "configured_server_database", return_value=master),
                  patch.object(capacity_observer, "_native_observations",
                               side_effect=AssertionError("writer crossed Root preflight")),
                  self.assertRaises(AdmissionRefused)):
                admit_current(target, master=master, registry=registry, home_project=ROOT_PROJECT,
                    home_session=ROOT_SESSION, host_id=HOST, target_project=TARGET_PROJECT,
                    request_id=REQUEST_ID, authorization_source_id=SOURCE_ID, confirm=True)
            self.assertEqual(coord.db.execute("SELECT COUNT(*) FROM capacity_admissions").fetchone()[0], 0)
            self.assertEqual(coord.db.execute("SELECT owner_session,reservation_id FROM resources WHERE name='validation-clone'").fetchone()[0:2],
                             (OWNER_A, "reservation_ozTM0ysovUwnzjB95HWDce4hRIw4bP__"))
            coord.close()

    def test_direct_writer_refuses_wrong_registry_even_when_master_is_configured(self):
        with tempfile.TemporaryDirectory(prefix="capacity-admission-wrong-registry-") as temporary:
            coord, master, registry, target = make_fixture(Path(temporary))
            wrong_registry = Path(str(registry) + ".copy")
            shutil.copy2(registry, wrong_registry)
            root = Path(master).parent
            config = SimpleNamespace(master=Path(master), plist=root / "service.plist",
                                     wrapper=root / "wrapper", executable=root / "server")
            from agent_chat import capacity_observer
            with (patch.object(root_local, "_installed_server_config", return_value=config),
                  patch.object(root_local, "configured_server_database", return_value=Path(master)),
                  patch.object(capacity_observer, "_native_observations",
                               side_effect=AssertionError("wrong registry reached observation")),
                  self.assertRaisesRegex(AdmissionRefused, "differs from the configured Root registry")):
                admit_current(target, master=master, registry=wrong_registry,
                    home_project=ROOT_PROJECT, home_session=ROOT_SESSION, host_id=HOST,
                    target_project=TARGET_PROJECT, request_id=REQUEST_ID,
                    authorization_source_id=SOURCE_ID, confirm=True)
            self.assertEqual(coord.db.execute("SELECT COUNT(*) FROM capacity_admissions").fetchone()[0], 0)
            self.assertEqual(coord.db.execute("SELECT COUNT(*) FROM capacity_admission_uses").fetchone()[0], 0)
            self.assertEqual(tuple(coord.db.execute(
                "SELECT owner_session,reservation_id,stale FROM resources WHERE name='validation-clone'").fetchone()),
                (OWNER_A, "reservation_ozTM0ysovUwnzjB95HWDce4hRIw4bP__", 1))
            coord.close()

    def test_lock_wait_past_observation_window_refuses_without_mutation(self):
        with tempfile.TemporaryDirectory(prefix="capacity-admission-lock-freshness-") as temporary:
            coord, master, registry, target = make_fixture(Path(temporary))
            locker = sqlite3.connect(str(master), timeout=10, isolation_level=None, check_same_thread=False)
            locker.execute("BEGIN IMMEDIATE")
            observed_at = []
            observed = threading.Event()
            def release_lock():
                observed.wait(timeout=10)
                time.sleep(5.5)
                locker.commit()
                locker.close()
            release = threading.Thread(target=release_lock)
            release.start()
            try:
                def mark_observed():
                    observed_at.append(time.monotonic())
                    observed.set()
                with self.assertRaisesRegex(AdmissionRefused, "five-second action window"):
                    invoke(coord, master, registry, target, after_native_observation=mark_observed)
                self.assertGreater(time.monotonic() - observed_at[0], 5.0)
            finally:
                release.join(timeout=2)
                if release.is_alive():
                    self.fail("test lock holder did not release")
            self.assertEqual(coord.db.execute("SELECT COUNT(*) FROM capacity_admissions").fetchone()[0], 0)
            self.assertEqual(coord.db.execute("SELECT COUNT(*) FROM capacity_admission_uses").fetchone()[0], 0)
            self.assertEqual(tuple(coord.db.execute(
                "SELECT owner_session,reservation_id,stale FROM resources WHERE name='validation-clone'").fetchone()),
                (OWNER_A, "reservation_ozTM0ysovUwnzjB95HWDce4hRIw4bP__", 1))
            coord.close()

    def test_future_observation_refuses_without_mutation(self):
        with tempfile.TemporaryDirectory(prefix="capacity-admission-future-observation-") as temporary:
            coord, master, registry, target = make_fixture(Path(temporary))
            with self.assertRaisesRegex(AdmissionRefused, "five-second action window"):
                invoke(coord, master, registry, target, observation_shift=1)
            self.assertEqual(coord.db.execute("SELECT COUNT(*) FROM capacity_admissions").fetchone()[0], 0)
            self.assertEqual(coord.db.execute("SELECT COUNT(*) FROM capacity_admission_uses").fetchone()[0], 0)
            self.assertEqual(tuple(coord.db.execute(
                "SELECT owner_session,reservation_id,stale FROM resources WHERE name='validation-clone'").fetchone()),
                (OWNER_A, "reservation_ozTM0ysovUwnzjB95HWDce4hRIw4bP__", 1))
            coord.close()

    def test_root_scope_change_after_observation_refuses_inside_action_transaction(self):
        with tempfile.TemporaryDirectory(prefix="capacity-admission-root-race-") as temporary:
            coord, master, registry, target = make_fixture(Path(temporary))
            from agent_chat import capacity_observer
            original = capacity_observer._root_scope
            calls = 0
            def change_after_final_observation(*args):
                nonlocal calls
                result = original(*args)
                calls += 1
                if calls == 2:
                    state = sqlite3.connect(registry)
                    state.execute("UPDATE root_config SET thread_id='changed-after-observation' WHERE singleton=1")
                    state.commit(); state.close()
                return result
            with patch.object(capacity_observer, "_root_scope", side_effect=change_after_final_observation):
                with self.assertRaisesRegex(AdmissionRefused, "transactional action time"):
                    invoke(coord, master, registry, target)
            self.assertEqual(coord.db.execute("SELECT COUNT(*) FROM capacity_admissions").fetchone()[0], 0)
            self.assertEqual(coord.db.execute("SELECT COUNT(*) FROM capacity_admission_uses").fetchone()[0], 0)
            coord.close()

    def test_changed_claim_tuple_refuses_without_audit_or_claim_mutation(self):
        with tempfile.TemporaryDirectory(prefix="capacity-admission-claim-race-") as temporary:
            coord, master, registry, target = make_fixture(Path(temporary))
            coord.db.execute("UPDATE resources SET reservation_id='different-rid' WHERE name='heavy-unity:2'")
            before = [tuple(row) for row in coord.db.execute(
                "SELECT name,owner_session,reservation_id,granted_at,deadline,stale,reason FROM resources ORDER BY name")]
            with self.assertRaises(AdmissionRefused):
                invoke(coord, master, registry, target)
            after = [tuple(row) for row in coord.db.execute(
                "SELECT name,owner_session,reservation_id,granted_at,deadline,stale,reason FROM resources ORDER BY name")]
            self.assertEqual(after, before)
            self.assertEqual(coord.db.execute("SELECT COUNT(*) FROM capacity_admissions").fetchone()[0], 0)
            coord.close()


class PeerBindingTests(unittest.TestCase):
    def test_wrong_registered_endpoint_refuses_without_admission(self):
        with tempfile.TemporaryDirectory(prefix="capacity-wrong-endpoint-") as temporary:
            coord, master, registry, target = make_fixture(Path(temporary))
            coord.db.execute("UPDATE bridge_client_runtime SET server=? WHERE host_id=?", ("ws://example.com:34971", HOST))
            before = [tuple(row) for row in coord.db.execute("SELECT name,owner_session,reservation_id FROM resources ORDER BY name")]
            from agent_chat.capacity_observer import Blocked
            with self.assertRaises(Blocked):
                invoke(coord, master, registry, target)
            self.assertEqual(coord.db.execute("SELECT COUNT(*) FROM capacity_admissions").fetchone()[0], 0)
            self.assertEqual(before, [tuple(row) for row in coord.db.execute("SELECT name,owner_session,reservation_id FROM resources ORDER BY name")])
            coord.close()

    def test_bridge_replacement_after_observation_refuses_without_admission(self):
        with tempfile.TemporaryDirectory(prefix="capacity-replaced-bridge-") as temporary:
            coord, master, registry, target = make_fixture(Path(temporary))
            before = [tuple(row) for row in coord.db.execute("SELECT name,owner_session,reservation_id FROM resources ORDER BY name")]
            def replace_bridge():
                coord.db.execute("UPDATE bridge_client_runtime SET pid=pid+1 WHERE host_id=?", (HOST,))
                coord.db.commit()
            with self.assertRaises(AdmissionRefused):
                invoke(coord, master, registry, target, after_native_observation=replace_bridge)
            self.assertEqual(coord.db.execute("SELECT COUNT(*) FROM capacity_admissions").fetchone()[0], 0)
            self.assertEqual(before, [tuple(row) for row in coord.db.execute("SELECT name,owner_session,reservation_id FROM resources ORDER BY name")])
            coord.close()

    def test_connected_peer_replacement_is_rejected(self):
        from agent_chat import capacity_observer, native_owner_evidence
        registration = {"endpoint":"ws://127.0.0.1:34971", "pid":42, "heartbeat":time.time()}
        bindings = {"owner":{"thread":"thread-a", "host":HOST}}
        class FakeKernel:
            calls=0
            def sample(self, reg, rpc=None):
                self.calls += 1
                return SimpleNamespace(endpoint=("127.0.0.1",34971), listener_pid=8 if self.calls>=3 else 7, chain=("generation",), image_sha="image", code_hash="code", peer=None)
        class FakeRpc:
            def connection_identity(self):
                return {"client_instance":"a"*32,"connection_epoch":1,"pid":os.getpid(),"fd":8,"local":("127.0.0.1",31002),"peer":("127.0.0.1",34971)}
            def __enter__(self): return self
            def __exit__(self, *args): return False
            def request(self, method, params):
                if method=="thread/read": return {"thread":{"id":"thread-a", "status":{"type":"idle"}, "canAcceptDirectInput":True}}
                return {"data":[], "nextCursor":None}
        with (patch.object(native_owner_evidence, "DarwinEvidence", FakeKernel), patch.object(capacity_observer, "_selected_client", side_effect=lambda reg,k: ("client", "link", k.sample(reg), "plist", "files")), patch.object(capacity_observer, "_observing_peer", return_value="synthetic-pair")):
            with self.assertRaises(capacity_observer.Blocked):
                with capacity_observer._native_observations(bindings, registration, lambda endpoint: FakeRpc()):
                    pass

    def test_matching_registered_peer_is_accepted(self):
        from agent_chat import capacity_observer, native_owner_evidence
        registration = {"endpoint":"ws://127.0.0.1:34971", "pid":42, "heartbeat":time.time()}
        bindings = {"owner":{"thread":"thread-a", "host":HOST}}
        class FakeKernel:
            def sample(self, reg, rpc=None):
                return SimpleNamespace(endpoint=("127.0.0.1",34971), listener_pid=7, chain=("generation",), image_sha="image", code_hash="code", peer=None if rpc is None else "same-peer")
        class FakeRpc:
            def connection_identity(self):
                return {"client_instance":"a"*32,"connection_epoch":1,"pid":os.getpid(),"fd":8,"local":("127.0.0.1",31002),"peer":("127.0.0.1",34971)}
            def __enter__(self): return self
            def __exit__(self, *args): return False
            def request(self, method, params):
                if method=="thread/read": return {"thread":{"id":"thread-a", "status":{"type":"idle"}, "canAcceptDirectInput":True}}
                return {"data":[], "nextCursor":None}
        with (patch.object(native_owner_evidence, "DarwinEvidence", FakeKernel), patch.object(capacity_observer, "_selected_client", side_effect=lambda reg,k: ("client", "link", k.sample(reg), "plist", "files")), patch.object(capacity_observer, "_observing_peer", return_value="synthetic-pair")):
            with capacity_observer._native_observations(bindings, registration, lambda endpoint: FakeRpc()) as (result, _, _, revalidate):
                revalidate(registration)
        self.assertEqual(result["owner"]["queue_count"], 0)

    def test_ambiguous_listener_rejected_before_connection(self):
        from agent_chat import capacity_observer, native_owner_evidence
        registration = {"endpoint":"ws://127.0.0.1:34971", "pid":42, "heartbeat":time.time()}
        class AmbiguousKernel:
            def sample(self, reg, rpc=None): raise native_owner_evidence.EvidenceError("listener is absent or ambiguous")
        calls=[]
        with (patch.object(native_owner_evidence, "DarwinEvidence", AmbiguousKernel), patch.object(capacity_observer, "_selected_client", side_effect=lambda reg,k: ("client", "link", k.sample(reg), "plist", "files"))):
            with self.assertRaises(capacity_observer.Blocked):
                with capacity_observer._native_observations({}, registration, lambda endpoint: calls.append(endpoint)):
                    pass
        self.assertEqual(calls, [])


    def test_post_begin_native_generation_change_has_zero_effect(self):
        from agent_chat import capacity_observer, native_owner_evidence
        original = capacity_observer._native_observations
        class FakeKernel:
            calls = 0
            def sample(self, registration, rpc=None):
                self.calls += 1
                return SimpleNamespace(endpoint=("127.0.0.1",34971), listener_pid=8 if self.calls>=4 else 7, chain=("generation",), image_sha="image", code_hash="code", peer=None)
        class FakeRpc:
            def connection_identity(self):
                return {"client_instance":"a"*32,"connection_epoch":1,"pid":os.getpid(),"fd":8,"local":("127.0.0.1",31002),"peer":("127.0.0.1",34971)}
            def __enter__(self): return self
            def __exit__(self, *args): return False
            def request(self, method, params):
                if method == "thread/read": return {"thread":{"id":params["threadId"], "status":{"type":"idle"}, "canAcceptDirectInput":True}}
                return {"data":[], "nextCursor":None}
        kernel = FakeKernel()
        def actual_native(bindings, registration):
            return original(bindings, registration, lambda endpoint: FakeRpc())
        with tempfile.TemporaryDirectory(prefix="capacity-post-begin-peer-") as temporary:
            coord, master, registry, target = make_fixture(Path(temporary))
            tables = ("resources", "resource_queue", "remote_guarded_runs", "guarded_runs", "capacity_admissions", "capacity_admission_claims", "capacity_admission_guards", "capacity_admission_observations", "capacity_admission_fifo", "capacity_admission_uses")
            before = {name: [tuple(row) for row in coord.db.execute("SELECT * FROM " + name)] for name in tables}
            runtime_before = tuple(coord.db.execute("SELECT * FROM bridge_client_runtime WHERE host_id=?", (HOST,)).fetchone())
            with (patch.object(native_owner_evidence, "DarwinEvidence", return_value=kernel), patch.object(capacity_observer, "_selected_client", side_effect=lambda reg,k: ("client", "link", k.sample(reg), "plist", "files")), patch.object(capacity_observer, "_observing_peer", return_value="synthetic-pair")):
                with self.assertRaises(capacity_observer.Blocked):
                    invoke(coord, master, registry, target, native_factory=actual_native)
            self.assertEqual(kernel.calls, 4)
            self.assertEqual(runtime_before, tuple(coord.db.execute("SELECT * FROM bridge_client_runtime WHERE host_id=?", (HOST,)).fetchone()))
            after = {name: [tuple(row) for row in coord.db.execute("SELECT * FROM " + name)] for name in tables}
            self.assertEqual(before, after)
            coord.close()



class ManagedClientSelectorTests(unittest.TestCase):
    def setUp(self):
        import os, plistlib
        from agent_chat import capacity_observer, native_owner_evidence, service
        self.obs=capacity_observer; self.evidence=native_owner_evidence; self.service=service
        self.tmp=tempfile.TemporaryDirectory(prefix="managed-client-selector-"); self.addCleanup(self.tmp.cleanup)
        root=Path(self.tmp.name); self.plist=root/"client.plist"; self.wrapper=root/"client"; self.wrapper.write_text("fixture")
        self.plist.write_bytes(plistlib.dumps({"Label":"com.agent-chat.client", "ProgramArguments":[str(self.wrapper)]}))
        self.uid=os.getuid(); self.pid=52; self.endpoint="ws://127.0.0.1:34971"; self.reg={"pid":self.pid,"endpoint":self.endpoint,"heartbeat":time.time()}
        self.client=SimpleNamespace(pid=self.pid,uid=self.uid,ruid=self.uid,birth_sec=int(time.time())-1,birth_usec=0)
        self.child=SimpleNamespace(pid=53,parent=self.pid)
        self.native=SimpleNamespace(endpoint=("127.0.0.1",34971),listener_pid=53,chain=(self.child,self.client),image_sha="image",code_hash="code",peer=None)
        self.links=[(self.pid,"3","127.0.0.1:31001->127.0.0.1:34971")]
        self.output=f"path = {self.plist}\nprogram = {self.wrapper}\npid = {self.pid}\n"
        self.stack=contextlib.ExitStack(); self.addCleanup(self.stack.close)
        self.stack.enter_context(patch.object(service,"LABEL","com.agent-chat",create=True))
        self.stack.enter_context(patch.object(service,"_installed_server_config",return_value=SimpleNamespace(uid=self.uid,executable=root/"server")))
        self.stack.enter_context(patch.object(service,"ServiceManager",return_value=SimpleNamespace(paths=lambda component:{"config":self.plist,"wrapper":self.wrapper}),create=True))
        self.stack.enter_context(patch.object(root_local,"_file",side_effect=lambda path,uid:Path(path)))
        self.stack.enter_context(patch.object(capacity_observer.pwd,"getpwuid",return_value=SimpleNamespace(pw_dir=str(root))))
        self.stack.enter_context(patch.object(capacity_observer.subprocess,"run",side_effect=lambda *a,**k:SimpleNamespace(returncode=0,stderr="",stdout=self.output)))
        outer=self
        class Kernel:
            def process(self,pid): return outer.client
            def sample(self,registration): return outer.native
            def lsof(self,*args): return outer.links
        self.kernel=Kernel()

    def test_selected_client_accepts_matching_synthetic_generation(self):
        selected=self.obs._selected_client(self.reg,self.kernel)
        self.assertEqual(selected[0],self.client)

    def test_wrong_client_pid_uid_birth_endpoint_and_ambiguous_connection_refuse(self):
        from agent_chat.capacity_observer import Blocked
        original_client=self.client; original_output=self.output; original_links=self.links
        cases=["launch_pid","runtime_pid","uid","birth","endpoint","ambiguous","loaded_program"]
        for case in cases:
            with self.subTest(case=case):
                self.client=original_client; self.output=original_output; self.links=original_links; reg=dict(self.reg)
                if case=="launch_pid": self.output=self.output.replace("pid = 52","pid = 54")
                if case=="runtime_pid": reg["pid"]=54
                if case=="uid": self.client=SimpleNamespace(pid=52,uid=self.uid+1,ruid=self.uid,birth_sec=int(time.time())-1,birth_usec=0)
                if case=="birth": reg["heartbeat"]=time.time()-20
                if case=="endpoint": reg["endpoint"]="ws://127.0.0.1:34972"
                if case=="ambiguous": self.links=original_links+[(52,"4",original_links[0][2])]
                if case=="loaded_program": self.output=self.output.replace(str(self.wrapper),str(self.wrapper)+"-other")
                with self.assertRaises(Blocked): self.obs._selected_client(reg,self.kernel)

    def test_observer_connection_pair_is_unique_and_stable(self):
        from agent_chat.capacity_observer import Blocked
        ident={"client_instance":"a"*32,"connection_epoch":1,"pid":os.getpid(),"fd":8,"local":("127.0.0.1",31002),"peer":("127.0.0.1",34971)}
        caller=(os.getpid(),"8","127.0.0.1:31002->127.0.0.1:34971")
        reverse=(53,"9","127.0.0.1:34971->127.0.0.1:31002")
        def lookup(*args): return [caller] if args[1]==str(os.getpid()) else self.links
        self.links=[reverse]
        with patch.object(self.kernel,"lsof",side_effect=lookup):
            pair=self.obs._observing_peer(self.kernel,self.reg,self.native,ident)
            self.assertEqual(pair,(caller,reverse))
            self.links=[reverse,(53,"10",reverse[2])]
            with self.assertRaises(Blocked): self.obs._observing_peer(self.kernel,self.reg,self.native,ident)
            self.links=[reverse]
            with self.assertRaises(Blocked): self.obs._observing_peer(self.kernel,self.reg,self.native,ident,("different",))
            wrong=dict(ident,fd=9)
            with self.assertRaises(Blocked): self.obs._observing_peer(self.kernel,self.reg,self.native,wrong)

    def test_post_begin_connection_epoch_change_has_zero_effect(self):
        from agent_chat import capacity_observer, native_owner_evidence
        original=capacity_observer._native_observations
        class FakeKernel:
            def sample(self,registration):
                return SimpleNamespace(endpoint=("127.0.0.1",34971),listener_pid=7,chain=("generation",),image_sha="image",code_hash="code",peer=None)
        class FakeRpc:
            calls=0
            def connection_identity(self):
                self.calls += 1
                return {"client_instance":"a"*32,"connection_epoch":2 if self.calls>=3 else 1,"pid":os.getpid(),"fd":8,"local":("127.0.0.1",31002),"peer":("127.0.0.1",34971)}
            def __enter__(self): return self
            def __exit__(self,*args): return False
            def request(self,method,params):
                if method=="thread/read": return {"thread":{"id":params["threadId"],"status":{"type":"idle"},"canAcceptDirectInput":True}}
                return {"data":[],"nextCursor":None}
        rpc=FakeRpc();kernel=FakeKernel()
        def actual_native(bindings,registration): return original(bindings,registration,lambda endpoint:rpc)
        with tempfile.TemporaryDirectory(prefix="capacity-post-begin-epoch-") as temporary:
            coord,master,registry,target=make_fixture(Path(temporary))
            tables=("resources","resource_queue","remote_guarded_runs","guarded_runs","capacity_admissions","capacity_admission_claims","capacity_admission_guards","capacity_admission_observations","capacity_admission_fifo","capacity_admission_uses")
            before={name:[tuple(row) for row in coord.db.execute("SELECT * FROM "+name)] for name in tables}
            runtime_before=tuple(coord.db.execute("SELECT * FROM bridge_client_runtime WHERE host_id=?",(HOST,)).fetchone())
            with (patch.object(native_owner_evidence,"DarwinEvidence",return_value=kernel), patch.object(capacity_observer,"_selected_client",side_effect=lambda reg,k:("client","link",k.sample(reg),"plist","files")),patch.object(capacity_observer,"_observing_peer",return_value="synthetic-pair")):
                with self.assertRaises(capacity_observer.Blocked): invoke(coord,master,registry,target,native_factory=actual_native)
            self.assertEqual(rpc.calls,3)
            self.assertEqual(runtime_before,tuple(coord.db.execute("SELECT * FROM bridge_client_runtime WHERE host_id=?",(HOST,)).fetchone()))
            self.assertEqual(before,{name:[tuple(row) for row in coord.db.execute("SELECT * FROM "+name)] for name in tables})
            coord.close()

    def test_post_begin_replaced_peer_has_zero_effect(self):
        from agent_chat import capacity_observer, native_owner_evidence
        original=capacity_observer._native_observations
        class FakeKernel:
            def sample(self,registration):
                return SimpleNamespace(endpoint=("127.0.0.1",34971),listener_pid=7,chain=("generation",),image_sha="image",code_hash="code",peer=None)
        class FakeRpc:
            def connection_identity(self):
                return {"client_instance":"a"*32,"connection_epoch":1,"pid":os.getpid(),"fd":8,"local":("127.0.0.1",31002),"peer":("127.0.0.1",34971)}
            def __enter__(self): return self
            def __exit__(self,*args): return False
            def request(self,method,params):
                if method=="thread/read": return {"thread":{"id":params["threadId"],"status":{"type":"idle"},"canAcceptDirectInput":True}}
                return {"data":[],"nextCursor":None}
        rpc=FakeRpc();kernel=FakeKernel()
        def actual_native(bindings,registration): return original(bindings,registration,lambda endpoint:rpc)
        with tempfile.TemporaryDirectory(prefix="capacity-post-begin-peer-") as temporary:
            coord,master,registry,target=make_fixture(Path(temporary))
            tables=("resources","resource_queue","remote_guarded_runs","guarded_runs","capacity_admissions","capacity_admission_claims","capacity_admission_guards","capacity_admission_observations","capacity_admission_fifo","capacity_admission_uses")
            before={name:[tuple(row) for row in coord.db.execute("SELECT * FROM "+name)] for name in tables}
            runtime_before=tuple(coord.db.execute("SELECT * FROM bridge_client_runtime WHERE host_id=?",(HOST,)).fetchone())
            with (patch.object(native_owner_evidence,"DarwinEvidence",return_value=kernel), patch.object(capacity_observer,"_selected_client",side_effect=lambda reg,k:("client","link",k.sample(reg),"plist","files")),patch.object(capacity_observer,"_observing_peer",side_effect=["synthetic-pair","synthetic-pair",capacity_observer.Blocked("peer replaced")]) as peer):
                with self.assertRaises(capacity_observer.Blocked): invoke(coord,master,registry,target,native_factory=actual_native)
            self.assertEqual(peer.call_count,3)
            self.assertEqual(runtime_before,tuple(coord.db.execute("SELECT * FROM bridge_client_runtime WHERE host_id=?",(HOST,)).fetchone()))
            self.assertEqual(before,{name:[tuple(row) for row in coord.db.execute("SELECT * FROM "+name)] for name in tables})
            coord.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
