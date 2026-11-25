"""Private prototype for one-shot Root-owned capacity admission.

Only the configured Root-local entry point may call :func:`admit_current`.
The durable record is explicitly nonordinary: it preserves old OPEN rows and
does not claim process closure or restoration.
"""
from __future__ import annotations

from contextlib import closing, contextmanager
import hashlib
from pathlib import Path
import sqlite3
import time
import uuid

RESOURCES = ("validation-clone", "heavy-unity:2")
OUTCOME = "ROOT_ADMIN_NONORDINARY_CAPACITY_ADMISSION"
MAX_OBSERVATION_AGE = 5.0
ROOT_OPERATION_ID = "C34EBA6F-6562-4FF1-8984-D47A0685E2B5"
ROOT_OPERATION_SOURCE_ID = "message_KJqomMYywtU6GYfczB_fxftks4D_gz6y"
ROOT_OPERATION_SOURCE_BODY = (
    "set the unity projects playable build as a goal for the unity PO and yourself. "
    "should any blockers arise that are from either codex or the agent-chat app "
    "I authorize you to take ANY action that allows progression!"
)
ROOT_OPERATION_SOURCE_SENDER = "web_operator_qYmkTK9qVoubfWPPTLr8iSzaIKqKk3Si"
ROOT_OPERATION_HOME_PROJECT = "default"
ROOT_OPERATION_HOME_SESSION = "session_tcnSV4Qo3EH4YU3gg2swqjO_GS1QQ9Fv"
ROOT_OPERATION_HOME_THREAD = "01a1448a-c377-73e0-9025-02c4f2f6f2d9"
ROOT_OPERATION_HOST = "host_o6KZLd0kkjchCaH8OjoJZ9aZXHDlFiE2"
ROOT_OPERATION_TARGET_PROJECT = "default"
ROOT_OPERATION_OWNERS = {
    "validation-clone": {
        "owner_session": "session_d96238daad0345b699ec1dd1972ab11b",
        "reservation_id": "reservation_ozTM0ysovUwnzjB95HWDce4hRIw4bP__",
        "granted_at": 1791669263.859751939,
        "deadline": 1791670463.859751939,
        "stale": 1,
        "reason": None,
        "thread": "01a11d1f-bb15-7750-affe-3029d7b4057d",
    },
    "heavy-unity:2": {
        "owner_session": "session_92cd8bfd4ee045798585e1f873c94d55",
        "reservation_id": "reservation_2_fDGXtQwqWdiIVAtWRKC0QXJquAH2wK",
        "granted_at": 1793318478.301824092,
        "deadline": 1793322078.301824092,
        "stale": 1,
        "reason": None,
        "thread": "01a11d1f-cb9d-7d53-8aef-8d16a71502ef",
    },
}
ROOT_OPERATION_GUARDS = {
    ("remote_guarded_runs", "remote_run_d6b4b581f256e9f9c2b0ed9954221b3f", "validation-clone",
     "reservation_ozTM0ysovUwnzjB95HWDce4hRIw4bP__",
     "session_d96238daad0345b699ec1dd1972ab11b", ROOT_OPERATION_HOST, 27633,
     1791669860.716338873, None),
}


class AdmissionRefused(RuntimeError):
    pass


def install_schema(db: sqlite3.Connection) -> None:
    statements = (
        """CREATE TABLE IF NOT EXISTS capacity_admissions (
          request_id TEXT PRIMARY KEY, payload_sha256 TEXT NOT NULL,
          observation_sha256 TEXT NOT NULL, authorization_source_id TEXT NOT NULL,
          root_project TEXT NOT NULL, root_session TEXT NOT NULL, root_thread TEXT NOT NULL,
          root_host TEXT NOT NULL, target_project TEXT NOT NULL,
          project_root_session TEXT NOT NULL, guard_generation INTEGER NOT NULL,
          observation_started_at REAL NOT NULL, observation_finished_at REAL NOT NULL,
          created_at REAL NOT NULL,
          outcome TEXT NOT NULL CHECK(outcome='ROOT_ADMIN_NONORDINARY_CAPACITY_ADMISSION'),
          process_state TEXT NOT NULL CHECK(process_state='UNKNOWN'),
          restoration_state TEXT NOT NULL CHECK(restoration_state='UNKNOWN'),
          ordinary_receipt INTEGER NOT NULL CHECK(ordinary_receipt=0),
          fresh_ownership_required INTEGER NOT NULL CHECK(fresh_ownership_required=1))""",
        """CREATE TABLE IF NOT EXISTS capacity_admission_claims (
          request_id TEXT NOT NULL REFERENCES capacity_admissions(request_id),
          resource TEXT NOT NULL, reservation_id TEXT NOT NULL, owner_session TEXT NOT NULL,
          granted_at REAL NOT NULL, deadline REAL NOT NULL, stale INTEGER NOT NULL,
          reason TEXT, token_sha256 TEXT, owner_host TEXT NOT NULL, owner_thread TEXT NOT NULL,
          PRIMARY KEY(request_id, resource))""",
        """CREATE TABLE IF NOT EXISTS capacity_admission_guards (
          request_id TEXT NOT NULL REFERENCES capacity_admissions(request_id),
          table_name TEXT NOT NULL, run_id TEXT NOT NULL, resource TEXT NOT NULL,
          reservation_id TEXT NOT NULL, owner_session TEXT NOT NULL, host_id TEXT NOT NULL,
          owner_thread TEXT NOT NULL, pid INTEGER NOT NULL, pgid INTEGER,
          started_at REAL NOT NULL, closed_at REAL,
          original_process TEXT NOT NULL, current_pid_observation TEXT NOT NULL,
          process_group_observation TEXT NOT NULL,
          PRIMARY KEY(request_id, table_name, run_id))""",
        """CREATE TABLE IF NOT EXISTS capacity_admission_observations (
          request_id TEXT NOT NULL REFERENCES capacity_admissions(request_id),
          owner_session TEXT NOT NULL, thread_id TEXT NOT NULL, host_id TEXT NOT NULL,
          native_state TEXT NOT NULL, direct_input INTEGER NOT NULL CHECK(direct_input=1),
          queue_count INTEGER NOT NULL CHECK(queue_count=0), checked_at REAL NOT NULL,
          turn_id_known INTEGER NOT NULL CHECK(turn_id_known=0),
          PRIMARY KEY(request_id, owner_session))""",
        """CREATE TABLE IF NOT EXISTS capacity_admission_fifo (
          request_id TEXT NOT NULL REFERENCES capacity_admissions(request_id),
          resource TEXT NOT NULL, seq INTEGER NOT NULL, session TEXT NOT NULL,
          queued_at REAL NOT NULL, PRIMARY KEY(request_id, resource, seq))""",
        """CREATE TABLE IF NOT EXISTS capacity_admission_authorities (
          request_id TEXT NOT NULL REFERENCES capacity_admissions(request_id),
          owner_session TEXT NOT NULL, kind TEXT NOT NULL CHECK(kind IN ('native-child','managed')),
          manager_session TEXT, management_request_id TEXT,
          PRIMARY KEY(request_id, owner_session))""",
        """CREATE TABLE IF NOT EXISTS capacity_admission_uses (
          request_id TEXT NOT NULL REFERENCES capacity_admissions(request_id),
          resource TEXT NOT NULL, reservation_id TEXT NOT NULL UNIQUE,
          session TEXT NOT NULL, used_at REAL NOT NULL,
          PRIMARY KEY(request_id, resource))""",
    )
    for sql in statements:
        db.execute(sql)
    for table in ("capacity_admissions", "capacity_admission_claims", "capacity_admission_guards",
                  "capacity_admission_observations", "capacity_admission_fifo", "capacity_admission_authorities", "capacity_admission_uses"):
        for op in ("UPDATE", "DELETE"):
            name = f"{table}_immutable_{op.lower()}"
            db.execute(f"CREATE TRIGGER IF NOT EXISTS {name} BEFORE {op} ON {table} "
                       f"BEGIN SELECT RAISE(ABORT,'capacity admission evidence is immutable'); END")

    from .successor_admission import install_schema as install_successor_schema
    install_successor_schema(db)


def _digest(parts) -> str:
    return hashlib.sha256(repr(parts).encode("utf-8")).hexdigest()


def _require_message_id(value: str) -> str:
    if not isinstance(value, str) or not value.startswith("message_") or len(value) < 20:
        raise AdmissionRefused("an exact operator authorization source message ID is required")
    return value


def _verify_operator_source_db(db, source_id, root_session):
    """Require the exact operator record selected by this Root operation."""
    if source_id != ROOT_OPERATION_SOURCE_ID or root_session != ROOT_OPERATION_HOME_SESSION:
        raise AdmissionRefused("operator source is not bound to the selected Root operation")
    tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    required = {"messages", "operator_identities", "operator_message_origins"}
    if not required <= tables:
        raise AdmissionRefused("configured Root operator-origin tables are unavailable")
    rows = db.execute("""SELECT m.id,m.sender_session,m.recipient_session,m.body,
               o.sender_session AS origin_sender,o.channel
        FROM messages m JOIN operator_message_origins o ON o.message_id=m.id
        JOIN operator_identities i ON i.session_id=m.sender_session
        WHERE m.id=?""", (source_id,)).fetchmany(2)
    if (len(rows) != 1 or rows[0]["recipient_session"] != root_session
            or rows[0]["sender_session"] != rows[0]["origin_sender"]
            or rows[0]["sender_session"] != ROOT_OPERATION_SOURCE_SENDER
            or rows[0]["sender_session"] == root_session
            or rows[0]["body"] != ROOT_OPERATION_SOURCE_BODY
            or rows[0]["channel"] != "authenticated_operator_ui"):
        raise AdmissionRefused("authorization source is not an exact direct operator message to this Root")
    if "message_batches" in tables and db.execute(
            "SELECT 1 FROM message_batches WHERE message_id=?", (source_id,)).fetchone():
        raise AdmissionRefused("batched message cannot authorize capacity admission")


def _verify_operator_source(master_path, source_id, root_session):
    db = sqlite3.connect(Path(master_path).resolve().as_uri() + "?mode=ro", uri=True, timeout=2)
    db.row_factory = sqlite3.Row
    try:
        _verify_operator_source_db(db, source_id, root_session)
    finally:
        db.close()


def _verify_registry_scope(db, root_scope):
    root = db.execute("SELECT home_project,home_session,thread_id,host_id,mode "
                      "FROM root_config WHERE singleton=1").fetchone()
    owner = db.execute("SELECT home_project,home_session FROM root_directory_owner "
                       "WHERE singleton=1").fetchone()
    enrollment = db.execute("SELECT session_id,orchestrator_session,joined FROM root_projects WHERE project_id=?",
                            (root_scope["target_project"],)).fetchone()
    if (root is None or owner is None or enrollment is None or
            (root["home_project"], root["home_session"], root["thread_id"], root["host_id"]) !=
            (root_scope["home_project"], root_scope["home_session"], root_scope["root_thread"], root_scope["host"]) or
            root["mode"] not in ("running", "maintenance_paused") or
            (owner["home_project"], owner["home_session"]) !=
            (root_scope["home_project"], root_scope["home_session"]) or
            enrollment["joined"] != 1 or enrollment["session_id"] != root_scope["project_root_session"] or
            enrollment["orchestrator_session"] != root_scope["project_orchestrator_session"]):
        raise AdmissionRefused("configured Root/project binding changed at transactional action time")


def _require_root_operation(home_project, home_session, host_id, target_project,
                            request_id, authorization_source_id):
    if (home_project, home_session, host_id, target_project, request_id, authorization_source_id) != (
            ROOT_OPERATION_HOME_PROJECT, ROOT_OPERATION_HOME_SESSION, ROOT_OPERATION_HOST,
            ROOT_OPERATION_TARGET_PROJECT, ROOT_OPERATION_ID, ROOT_OPERATION_SOURCE_ID):
        raise AdmissionRefused("request does not match the exact Root-selected operation packet")


def _require_packet_snapshot(snapshot):
    claims = {row["name"]: row for row in snapshot["claims"]}
    if set(claims) != set(ROOT_OPERATION_OWNERS):
        raise AdmissionRefused("current claims differ from the exact Root-selected operation packet")
    for resource, expected in ROOT_OPERATION_OWNERS.items():
        claim = claims[resource]
        if any(claim.get(field) != expected[field] for field in (
                "owner_session", "reservation_id", "granted_at", "deadline", "stale", "reason")):
            raise AdmissionRefused("current claim tuple differs from the exact Root-selected operation packet")
        binding = snapshot["owner_bindings"].get(expected["owner_session"])
        if binding != {"session": expected["owner_session"], "thread": expected["thread"],
                       "host": ROOT_OPERATION_HOST}:
            raise AdmissionRefused("current owner binding differs from the exact Root-selected operation packet")
    guards = set()
    for resource_rows in snapshot["guards"].values():
        for row in resource_rows:
            guards.add((row["table"], row["run_id"], row["resource"], row["reservation_id"],
                        row["owner_session"], row.get("host_id", ROOT_OPERATION_HOST), row["pid"],
                        row["started_at"], row["closed_at"]))
    if guards != ROOT_OPERATION_GUARDS:
        raise AdmissionRefused("current OPEN guard set differs from the exact Root-selected operation packet")


def _raw_guards(db, resource):
    rows = []
    tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if "guarded_runs" in tables:
        rows.extend(("guarded_runs", dict(row)) for row in db.execute(
            "SELECT run_id,resource,reservation_id,session AS owner_session,pid,pgid,started_at,closed_at "
            "FROM guarded_runs WHERE resource=? AND closed_at IS NULL ORDER BY run_id", (resource,)))
    if "remote_guarded_runs" in tables:
        rows.extend(("remote_guarded_runs", dict(row)) for row in db.execute(
            "SELECT run_id,resource,reservation_id,session_id AS owner_session,host_id AS host_id,"
            "local_pid AS pid,NULL AS pgid,started_at,closed_at "
            "FROM remote_guarded_runs WHERE resource=? AND closed_at IS NULL ORDER BY run_id", (resource,)))
    if "root_d6b4_guard_retirements" in tables:
        from .d6b4_retirement import is_retired_guard
        rows = [(table, row) for table, row in rows
                if not is_retired_guard(db, table, row)]
    return rows


def _admission_candidate(db, resource):
    rows = db.execute("""SELECT a.request_id FROM capacity_admissions a
        JOIN capacity_admission_claims c ON c.request_id=a.request_id
        WHERE c.resource=? AND a.outcome=? AND a.ordinary_receipt=0
          AND a.process_state='UNKNOWN' AND a.restoration_state='UNKNOWN'
          AND a.fresh_ownership_required=1
          AND NOT EXISTS (SELECT 1 FROM capacity_admission_uses u
                          WHERE u.request_id=a.request_id AND u.resource=c.resource)
        ORDER BY a.created_at DESC LIMIT 2""", (resource, OUTCOME)).fetchall()
    if not rows:
        return "none", None
    if len(rows) != 1:
        return "ambiguous", None
    return "one", rows[0]["request_id"]


def _admission_binding_is_current(db, request_id, registry_db=None):
    audit = db.execute("""SELECT target_project,project_root_session,root_project,root_session,root_thread,root_host,guard_generation,
        outcome,ordinary_receipt,process_state,restoration_state,fresh_ownership_required
        FROM capacity_admissions WHERE request_id=?""", (request_id,)).fetchone()
    if (audit is None or audit["outcome"] != OUTCOME or audit["ordinary_receipt"] != 0 or
            audit["process_state"] != "UNKNOWN" or audit["restoration_state"] != "UNKNOWN" or
            audit["fresh_ownership_required"] != 1):
        return False
    tables = {item[0] for item in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if not {"meta", "bridge_bindings", "remote_session_hosts", "root_guard_generation"} <= tables:
        return False
    project = db.execute("SELECT value FROM meta WHERE key='project_id'").fetchone()
    generation = db.execute("SELECT generation FROM root_guard_generation WHERE singleton=1").fetchone()
    claims = db.execute("""SELECT resource,owner_session,stale,owner_host,owner_thread
        FROM capacity_admission_claims WHERE request_id=? ORDER BY resource""", (request_id,)).fetchall()
    if ({claim["resource"] for claim in claims} != set(RESOURCES) or project is None or
            project["value"] != audit["target_project"] or generation is None or
            generation["generation"] != audit["guard_generation"] or
            any(claim["stale"] != 1 for claim in claims)):
        return False
    for claim in claims:
        owner_host = db.execute("SELECT host_id FROM remote_session_hosts WHERE session_id=?",
                                 (claim["owner_session"],)).fetchone()
        owner_binding = db.execute("SELECT thread_id,parent_session FROM bridge_bindings WHERE session_id=?",
                                   (claim["owner_session"],)).fetchone()
        if (owner_host is None or owner_host["host_id"] != claim["owner_host"] or
                owner_host["host_id"] != audit["root_host"] or owner_binding is None or
                owner_binding["thread_id"] != claim["owner_thread"]):
            return False
        saved = None
        if "capacity_admission_authorities" in tables:
            saved = db.execute("SELECT kind,manager_session,management_request_id FROM capacity_admission_authorities "
                               "WHERE request_id=? AND owner_session=?", (request_id, claim["owner_session"])).fetchone()
        if saved is None:
            # Previously recorded native-child tickets retain their original exact-parent behavior.
            if owner_binding["parent_session"] != audit["project_root_session"]:
                return False
            continue
        try:
            from . import capacity_observer as observer
            scope = {"host": audit["root_host"], "home_session": audit["root_session"],
                     "project_root_session": audit["project_root_session"],
                     "project_orchestrator_session": saved["manager_session"]}
            authority = observer._owner_authority(db, scope, claim["owner_session"], owner_binding, owner_host)
        except (observer.Blocked, sqlite3.Error, KeyError, TypeError):
            return False
        if authority["kind"] != saved["kind"]:
            return False
        if authority["kind"] == "managed":
            if (authority["link"]["manager_session"] != saved["manager_session"] or
                    authority["request"]["id"] != saved["management_request_id"] or not isinstance(registry_db, _RegistryGate)):
                return False
            try:
                registry_db.check()
                _verify_registry_scope(registry_db.db, {"home_project": audit["root_project"],
                    "home_session": audit["root_session"], "root_thread": audit["root_thread"],
                    "host": audit["root_host"], "target_project": audit["target_project"],
                    "project_root_session": audit["project_root_session"],
                    "project_orchestrator_session": saved["manager_session"]})
            except (AdmissionRefused, sqlite3.Error, KeyError, TypeError):
                return False
    return True


def request_admission_is_current(db, resource, registry_db=None):
    """Return None without a pending ticket, else whether its full scope is current."""
    state, request_id = _admission_candidate(db, resource)
    if state == "none":
        return None
    return state == "one" and _admission_binding_is_current(db, request_id, registry_db)


def _admission_for(db, resource):
    state, request_id = _admission_candidate(db, resource)
    return request_id if state == "one" else None


def _guard_is_audited(db, request_id, table, row, registry_db=None):
    if not _admission_binding_is_current(db, request_id, registry_db):
        return False
    owner = row["owner_session"]
    host = row.get("host_id")
    claim = db.execute("""SELECT c.owner_session,c.reservation_id,c.owner_host
        FROM capacity_admission_claims c WHERE c.request_id=? AND c.resource=?""",
        (request_id, row["resource"])).fetchone()
    if not claim or (claim["owner_session"], claim["reservation_id"]) != (owner, row["reservation_id"]):
        return False
    if host is not None and host != claim["owner_host"]:
        return False
    audit = db.execute("""SELECT a.target_project,a.project_root_session,a.guard_generation
        FROM capacity_admissions a JOIN capacity_admission_guards g ON g.request_id=a.request_id
        WHERE a.request_id=? AND g.table_name=? AND g.run_id=? AND g.resource=?
          AND g.reservation_id=? AND g.owner_session=? AND g.host_id=?
          AND g.pid=? AND g.pgid IS ? AND g.started_at=? AND g.closed_at IS NULL
          AND a.outcome=? AND a.ordinary_receipt=0 AND a.process_state='UNKNOWN'
          AND a.restoration_state='UNKNOWN' AND a.fresh_ownership_required=1""",
        (request_id, table, row["run_id"], row["resource"], row["reservation_id"], owner,
         claim["owner_host"], row["pid"], row["pgid"], row["started_at"], OUTCOME)).fetchone()
    if audit is None:
        return False
    tables = {item[0] for item in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if not {"meta", "bridge_bindings", "remote_session_hosts", "root_guard_generation"} <= tables:
        return False
    project = db.execute("SELECT value FROM meta WHERE key='project_id'").fetchone()
    owner_host = db.execute("SELECT host_id FROM remote_session_hosts WHERE session_id=?", (owner,)).fetchone()
    owner_binding = db.execute("SELECT thread_id,parent_session FROM bridge_bindings WHERE session_id=?", (owner,)).fetchone()
    generation = db.execute("SELECT generation FROM root_guard_generation WHERE singleton=1").fetchone()
    claim_binding = db.execute("SELECT owner_thread FROM capacity_admission_claims WHERE request_id=? AND resource=?",
                               (request_id, row["resource"])).fetchone()
    if (project is None or project["value"] != audit["target_project"] or owner_host is None or
            owner_host["host_id"] != claim["owner_host"] or owner_binding is None or claim_binding is None or
            owner_binding["thread_id"] != claim_binding["owner_thread"] or generation is None or
            generation["generation"] != audit["guard_generation"]):
        return False
    return True


def effective_open_guards(db, resource, registry_db=None):
    """Return raw OPEN guards except an exact, unconsumed one-shot admission."""
    raw = _raw_guards(db, resource)
    if not raw:
        return []
    request_id = _admission_for(db, resource)
    if request_id is None or any(not _guard_is_audited(db, request_id, table, row, registry_db) for table, row in raw):
        return [(table, row["run_id"]) for table, row in raw]
    recorded = db.execute("SELECT table_name,run_id FROM capacity_admission_guards WHERE request_id=? AND resource=?",
                          (request_id, resource)).fetchall()
    if {(row["table_name"], row["run_id"]) for row in recorded} != {(table, row["run_id"]) for table, row in raw}:
        return [(table, row["run_id"]) for table, row in raw]
    return []


def consume_for_request(db, resource, session, new_reservation_id, registry_db=None):
    """Bind each exact admission to at most one new claim per selected resource."""
    state, request_id = _admission_candidate(db, resource)
    if state == "none":
        return False
    if state != "one" or not _admission_binding_is_current(db, request_id, registry_db):
        raise AdmissionRefused("pending Root admission binding/generation is no longer current")
    old = db.execute("SELECT 1 FROM capacity_admission_claims WHERE request_id=? AND resource=?",
                     (request_id, resource)).fetchone()
    if old is None:
        return False
    # Existing OPEN guards must still be exactly the rows authorized by this ticket.
    blockers = effective_open_guards(db, resource, registry_db)
    if blockers:
        raise AdmissionRefused("current OPEN guard set no longer matches the one-shot admission")
    db.execute("INSERT INTO capacity_admission_uses VALUES(?,?,?,?,?)",
               (request_id, resource, new_reservation_id, session, time.time()))
    return True


def _table_names(db):
    return {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _same_claim(db, claim):
    row = db.execute("SELECT * FROM resources WHERE name=?", (claim["name"],)).fetchone()
    if row is None:
        return False
    actual = dict(row)
    token_hash = hashlib.sha256((actual["token"] or "").encode()).hexdigest() if actual["token"] else None
    expected = (claim["owner_session"], claim["reservation_id"], claim["granted_at"],
                claim["deadline"], claim["stale"], claim["reason"], claim["token_sha256"])
    got = (actual["owner_session"], actual["reservation_id"], actual["granted_at"],
           actual["deadline"], actual["stale"], actual["reason"], token_hash)
    return got == expected


def _require_configured_root_caller(master, home_project, home_session, host_id,
                                    target_project, request_id, authorization_source_id, confirm,
                                    registry):
    """Re-run the maintained Root-local identity/store preflight at the write boundary."""
    from . import root_local

    config = root_local._installed_server_config()
    params = {"project": target_project, "request_id": request_id,
              "authorization_source_id": authorization_source_id, "confirm": confirm}
    try:
        configured_master, enrolled = root_local._preflight(
            master, config.plist, home_project, home_session, host_id, "capacity-admit", params)
    except root_local.CoordError as error:
        raise AdmissionRefused("configured Root-local preflight did not authorize this writer") from error
    if not enrolled or Path(configured_master).resolve() != Path(master).resolve():
        raise AdmissionRefused("capacity admission requires the enrolled configured Root-local store")
    owner = config.plist.lstat().st_uid
    expected_registry = Path(str(Path(configured_master).resolve()) + ".projects.sqlite3")
    try:
        trusted_registry = root_local._file(expected_registry, owner)
        supplied_registry = root_local._file(Path(registry), owner)
    except root_local.CoordError as error:
        raise AdmissionRefused("configured Root registry is unavailable or has an untrusted path") from error
    if supplied_registry != trusted_registry:
        raise AdmissionRefused("caller registry differs from the configured Root registry")
    info = trusted_registry.lstat()
    identity = (info.st_dev, info.st_ino, info.st_uid, info.st_mode)
    return trusted_registry, owner, identity


def _recheck_root_registry(root_local, registry, owner, identity):
    try:
        checked = root_local._file(registry, owner)
    except root_local.CoordError as error:
        raise AdmissionRefused("configured Root registry changed identity or ownership") from error
    info = checked.lstat()
    if (info.st_dev, info.st_ino, info.st_uid, info.st_mode) != identity:
        raise AdmissionRefused("configured Root registry file identity changed")


class _RegistryGate:
    def __init__(self, db, check_file):
        self.db = db
        self._check_file = check_file
        self.active = True

    def check(self):
        if not self.active or not self.db.in_transaction:
            raise AdmissionRefused("registry authority gate is not active")
        self._check_file()


@contextmanager
def registry_authority_gate(registry, owner, identity):
    """Hold the trusted registry writer lock before any target transaction."""
    from . import root_local
    _recheck_root_registry(root_local, registry, owner, identity)
    db = sqlite3.connect(Path(registry).as_uri() + "?mode=rw", uri=True, timeout=3, isolation_level=None)
    db.row_factory = sqlite3.Row
    try:
        attached = db.execute("PRAGMA database_list").fetchone()
        if attached is None or Path(attached["file"]).resolve() != Path(registry).resolve():
            raise AdmissionRefused("registry gate opened a different database")
        db.execute("BEGIN IMMEDIATE")
        _recheck_root_registry(root_local, registry, owner, identity)
        gate = _RegistryGate(db, lambda: _recheck_root_registry(root_local, registry, owner, identity))
        try:
            yield gate, gate.check
        finally:
            gate.active = False
    finally:
        if db.in_transaction:
            db.rollback()
        db.close()


def _require_fresh_observation(started_at, finished_at, action_time):
    age = action_time - finished_at
    if (finished_at < started_at or finished_at - started_at > MAX_OBSERVATION_AGE or
            age < 0 or age > MAX_OBSERVATION_AGE):
        raise AdmissionRefused("configured Root observation exceeded its five-second action window")


def admit_current(target_path, *, master, registry, home_project, home_session, host_id,
                  target_project, request_id, authorization_source_id, confirm):
    """Observe configured Root state and admit two exact stale rows through Root-local preflight."""
    from . import capacity_observer as observer

    if confirm is not True:
        raise AdmissionRefused("explicit --confirm is required")
    authorization_source_id = _require_message_id(authorization_source_id)
    try:
        uuid.UUID(request_id)
    except (ValueError, TypeError, AttributeError) as error:
        raise AdmissionRefused("request_id must be a UUID") from error
    _require_root_operation(home_project, home_session, host_id, target_project,
                            request_id, authorization_source_id)
    from . import root_local
    registry, registry_owner, registry_identity = _require_configured_root_caller(
        master, home_project, home_session, host_id, target_project, request_id,
        authorization_source_id, confirm, registry)
    if Path(target_path).resolve() != Path(master).resolve():
        raise AdmissionRefused("the selected default-project operation must use the configured Root master store")
    _verify_operator_source(master, authorization_source_id, home_session)

    root_scope = observer._root_scope(registry, home_project, home_session, host_id, target_project)
    if root_scope["root_thread"] != ROOT_OPERATION_HOME_THREAD:
        raise AdmissionRefused("configured Root thread differs from the exact Root-selected operation packet")
    boot = observer._boot_time()
    snapshot = observer._target_snapshot(target_path, root_scope, boot)
    with observer._native_observations(snapshot["owner_bindings"], snapshot["bridge_registration"]) as (native, started_at, finished_at, revalidate_peer):
        guard_facts = observer._guard_current_facts(snapshot["guards"], snapshot["claims"], host_id, boot)
        after = observer._target_snapshot(target_path, root_scope, boot)
        if observer._root_scope(registry, home_project, home_session, host_id, target_project) != root_scope:
            raise AdmissionRefused("configured Root/project binding changed during observation")
        if (snapshot["claims"], snapshot["fifo"], snapshot["guards"], snapshot["generation"], snapshot["owner_bindings"], snapshot["owner_authorities"]) != \
           (after["claims"], after["fifo"], after["guards"], after["generation"], after["owner_bindings"], after["owner_authorities"]):
            raise AdmissionRefused("selected target tuples changed during native observation")
        if observer._bridge_identity(snapshot["bridge_registration"]) != observer._bridge_identity(after["bridge_registration"]):
            raise AdmissionRefused("registered bridge changed during native observation")
        _require_packet_snapshot(snapshot)
        if set(snapshot["claims"][i]["name"] for i in range(len(snapshot["claims"]))) != set(RESOURCES):
            raise AdmissionRefused("only the exact two selected resources may be admitted")
        _require_fresh_observation(started_at, finished_at, time.time())

        owner_observations = {}
        for owner, binding in snapshot["owner_bindings"].items():
            seen = native.get(owner)
            if not isinstance(seen, dict) or seen.get("thread_id") != binding["thread"] or seen.get("host") != host_id:
                raise AdmissionRefused("exact configured Root native observation is missing")
            if (seen.get("native_state"), seen.get("direct_input"), seen.get("queue_count"), seen.get("turn_id_known")) != ("idle", True, 0, False):
                raise AdmissionRefused("owner is not positively idle with an empty queue")
            owner_observations[owner] = seen

        guard_rows = []
        for resource in RESOURCES:
            selected = snapshot["guards"][resource]
            if resource == "validation-clone" and len(selected) != 1:
                raise AdmissionRefused("validation-clone must have its one exact selected OPEN guard")
            if resource == "heavy-unity:2" and selected:
                raise AdmissionRefused("heavy-unity:2 has an unexpected OPEN guard")
            facts_by_run = {item["run_id"]: item for item in guard_facts[resource]}
            for table_row in selected:
                facts = facts_by_run.get(table_row["run_id"])
                if facts is None or facts.get("state") != "OPEN":
                    raise AdmissionRefused("selected OPEN guard lacks current host process observation")
                binding = snapshot["owner_bindings"].get(table_row["owner_session"])
                if not binding:
                    raise AdmissionRefused("selected guard owner binding is unavailable")
                guard_rows.append((table_row["table"], table_row, binding, facts))

        with registry_authority_gate(registry, registry_owner, registry_identity) as (registry_db, check_registry):
            target = sqlite3.connect(str(target_path), timeout=10, isolation_level=None)
            target.row_factory = sqlite3.Row
            try:
                check_registry()
                names = _table_names(target)
                required = {"capacity_admissions", "capacity_admission_claims", "capacity_admission_guards",
                            "capacity_admission_observations", "capacity_admission_fifo", "capacity_admission_uses",
                            "resources", "resource_queue", "guarded_runs", "remote_guarded_runs", "sessions",
                            "bridge_bindings", "remote_session_hosts", "root_guard_generation"}
                if not required <= names:
                    raise AdmissionRefused("maintained capacity admission schema is unavailable")
                target.execute("BEGIN IMMEDIATE")
                now = time.time()
                _require_fresh_observation(started_at, finished_at, now)
                _recheck_root_registry(root_local, registry, registry_owner, registry_identity)
                _verify_registry_scope(registry_db.db, root_scope)
                current_registration = observer._registered_bridge(target, host_id)
                if observer._bridge_identity(current_registration) != observer._bridge_identity(snapshot["bridge_registration"]):
                    raise AdmissionRefused("registered bridge changed after native observation")
                revalidate_peer(current_registration)
                _require_fresh_observation(started_at, finished_at, time.time())
                _verify_operator_source_db(target, authorization_source_id, home_session)
                generation_row = target.execute("SELECT generation FROM root_guard_generation WHERE singleton=1").fetchone()
                if generation_row is None or generation_row["generation"] != snapshot["generation"]:
                    raise AdmissionRefused("target guard generation changed after current observation")
                claims = []
                bindings = snapshot["owner_bindings"]
                for expected in snapshot["claims"]:
                    resource = expected["name"]
                    owner = expected["owner_session"]
                    binding = bindings.get(owner)
                    if resource not in RESOURCES or not binding or owner == home_session:
                        raise AdmissionRefused("selected resource/owner scope differs")
                    host_row = target.execute("SELECT host_id FROM remote_session_hosts WHERE session_id=?", (owner,)).fetchone()
                    thread_row = target.execute("SELECT thread_id,parent_session FROM bridge_bindings WHERE session_id=?", (owner,)).fetchone()
                    if (host_row is None or host_row["host_id"] != host_id or thread_row is None or
                            thread_row["thread_id"] != binding["thread"]):
                        raise AdmissionRefused("owner host/thread/project binding changed")
                    try:
                        authority = observer._owner_authority(target, root_scope, owner, thread_row, host_row)
                    except (observer.Blocked, sqlite3.Error) as exc:
                        raise AdmissionRefused("owner management authority changed") from exc
                    if authority != snapshot["owner_authorities"].get(owner):
                        raise AdmissionRefused("owner management authority changed after observation")
                    if not _same_claim(target, expected) or expected["deadline"] >= now or expected["stale"] != 1:
                        raise AdmissionRefused("selected stale claim tuple changed or is no longer expired")
                    claims.append((expected, binding))
                if {item[0]["name"] for item in claims} != set(RESOURCES):
                    raise AdmissionRefused("the exact two stale claim rows are required")

                fifo = {}
                for resource in RESOURCES:
                    rows = [dict(row) for row in target.execute(
                        "SELECT seq,session,queued_at FROM resource_queue WHERE resource=? ORDER BY seq", (resource,))]
                    if rows != snapshot["fifo"][resource] or rows:
                        raise AdmissionRefused("selected FIFO changed or is nonempty")
                    fifo[resource] = rows

                current_guard_ids = set()
                for resource in RESOURCES:
                    for table, row in _raw_guards(target, resource):
                        current_guard_ids.add((table, row["run_id"]))
                if current_guard_ids != {(table, row["run_id"]) for table, row, _, _ in guard_rows}:
                    raise AdmissionRefused("selected OPEN guard set changed during observation")
                # A selected old reservation may not hide an OPEN guard recorded on another resource.
                selected_rids = {claim[0]["reservation_id"] for claim in claims}
                for table in ("guarded_runs", "remote_guarded_runs"):
                    for rid in selected_rids:
                        if table == "guarded_runs":
                            rows = target.execute("SELECT run_id,resource FROM guarded_runs WHERE reservation_id=? AND closed_at IS NULL", (rid,)).fetchall()
                        else:
                            rows = target.execute("SELECT run_id,resource FROM remote_guarded_runs WHERE reservation_id=? AND closed_at IS NULL", (rid,)).fetchall()
                        if any(row["resource"] not in RESOURCES for row in rows):
                            raise AdmissionRefused("selected reservation has an OPEN guard outside the exact admission set")

                payload = _digest((request_id, authorization_source_id, root_scope, snapshot["claims"],
                                   snapshot["fifo"], snapshot["guards"], snapshot["generation"],
                                   native, guard_facts, boot, started_at, finished_at))
                if target.execute("SELECT 1 FROM capacity_admissions WHERE request_id=?", (request_id,)).fetchone():
                    raise AdmissionRefused("request_id is already recorded; no replay")
                now = time.time()
                _require_fresh_observation(started_at, finished_at, now)
                if any(expected["deadline"] >= now for expected, _ in claims):
                    raise AdmissionRefused("selected stale claim expired status changed before action")
                target.execute("""INSERT INTO capacity_admissions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (request_id, payload, snapshot["snapshot_sha256"] if "snapshot_sha256" in snapshot else observer._digest(snapshot, native, root_scope),
                     authorization_source_id, root_scope["home_project"], root_scope["home_session"], root_scope["root_thread"],
                     host_id, target_project, root_scope["project_root_session"], snapshot["generation"], started_at, finished_at,
                     now, OUTCOME, "UNKNOWN", "UNKNOWN", 0, 1))
                for expected, binding in claims:
                    target.execute("INSERT INTO capacity_admission_claims VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                        (request_id, expected["name"], expected["reservation_id"], expected["owner_session"],
                         expected["granted_at"], expected["deadline"], expected["stale"], expected["reason"],
                         expected["token_sha256"], binding["host"], binding["thread"]))
                for table, row, binding, facts in guard_rows:
                    host = row.get("host_id", host_id)
                    target.execute("INSERT INTO capacity_admission_guards VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (request_id, table, row["run_id"], row["resource"], row["reservation_id"], row["owner_session"],
                         host, binding["thread"], row["pid"], row.get("pgid"), row["started_at"], row["closed_at"],
                         facts.get("original_process", "not_established"),
                         facts.get("current_pid_observation", "not_applicable"),
                         facts.get("process_group", "not_applicable")))
                for owner, authority in snapshot["owner_authorities"].items():
                    manager = authority["link"]["manager_session"] if authority["kind"] == "managed" else None
                    management_request = authority["request"]["id"] if authority["kind"] == "managed" else None
                    target.execute("INSERT INTO capacity_admission_authorities VALUES(?,?,?,?,?)",
                                   (request_id, owner, authority["kind"], manager, management_request))
                for owner, seen in owner_observations.items():
                    target.execute("INSERT INTO capacity_admission_observations VALUES(?,?,?,?,?,?,?,?,?)",
                        (request_id, owner, seen["thread_id"], seen["host"], seen["native_state"],
                         int(seen["direct_input"]), seen["queue_count"], seen["checked_at"], int(seen["turn_id_known"])))
                for resource, rows in fifo.items():
                    for row in rows:
                        target.execute("INSERT INTO capacity_admission_fifo VALUES(?,?,?,?,?)",
                                       (request_id, resource, row["seq"], row["session"], row["queued_at"]))

                for expected, _ in claims:
                    changed = target.execute("""UPDATE resources SET owner_session=NULL,reservation_id=NULL,token=NULL,
                        granted_at=NULL,deadline=NULL,stale=0,reason=NULL
                        WHERE name=? AND owner_session=? AND reservation_id=? AND granted_at=?
                          AND deadline=? AND stale=1 AND reason IS ?""",
                        (expected["name"], expected["owner_session"], expected["reservation_id"], expected["granted_at"],
                         expected["deadline"], expected["reason"])).rowcount
                    if changed != 1:
                        raise AdmissionRefused("exact conditional claim clear did not affect one row")
                if target.execute("SELECT generation FROM root_guard_generation WHERE singleton=1").fetchone()["generation"] != snapshot["generation"]:
                    raise AdmissionRefused("guard generation changed during exact CAS")
                if {(table, row["run_id"]) for resource in RESOURCES for table, row in _raw_guards(target, resource)} != current_guard_ids:
                    raise AdmissionRefused("OPEN guard history changed during admission")
                if any(target.execute("SELECT 1 FROM resource_queue WHERE resource=?", (resource,)).fetchone() for resource in RESOURCES):
                    raise AdmissionRefused("selected FIFO changed during exact CAS")
                check_registry()
                target.commit()
            except BaseException:
                if target.in_transaction:
                    target.rollback()
                raise
            finally:
                target.close()
        return {"operation": "capacity-admit", "request_id": request_id, "outcome": OUTCOME,
                "resources": list(RESOURCES), "cleared_claims": 2,
                "preserved_open_guards": len(guard_rows), "ordinary_receipt": False,
                "process_state": "UNKNOWN", "restoration_state": "UNKNOWN",
                "fresh_ownership_required": True, "fifo_preserved": True,
                "authorization_source_id": authorization_source_id,
                "payload_sha256": payload, "guard_generation": snapshot["generation"],
                "observation_started_at": started_at, "observation_finished_at": finished_at}
