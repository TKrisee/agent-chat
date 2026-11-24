"""Private, two-resource Root-local observer. It never changes coordinator state."""
from __future__ import annotations

from contextlib import closing, contextmanager
import hashlib
import os
import plistlib
import pwd
import re
from pathlib import Path
import sqlite3
import subprocess
import time

from .guard_generation import guard_generation


RESOURCES = ("validation-clone", "heavy-unity:2")
MAX_OBSERVATION_AGE = 5.0


class Blocked(RuntimeError):
    pass


def _open_ro(path):
    path = Path(path).resolve(strict=True)
    st = path.stat()
    if not path.is_file() or st.st_uid != os.getuid() or st.st_mode & 0o022:
        raise Blocked("configured store ownership/mode is not trusted")
    db = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=1)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA query_only=ON")
    db.execute("BEGIN")
    return db


def _tables(db):
    return {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _required(db, names):
    missing = sorted(set(names) - _tables(db))
    if missing:
        raise Blocked("required current-state tables unavailable: " + ",".join(missing))


def _one(db, sql, args, label):
    rows = db.execute(sql, args).fetchall()
    if len(rows) != 1:
        raise Blocked(label + " is missing or ambiguous")
    return dict(rows[0])


def _boot_time(boot_provider=None):
    if boot_provider is not None:
        value = boot_provider()
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
            raise Blocked("current kernel boot time unavailable")
        return float(value)
    result = subprocess.run(["/usr/sbin/sysctl", "-n", "kern.boottime"],
                            capture_output=True, text=True, timeout=2)
    if result.returncode or result.stderr:
        raise Blocked("current kernel boot time unavailable")
    pattern = (r"\s*\{\s*sec\s*=\s*([0-9]+)\s*,\s*usec\s*=\s*([0-9]+)\s*\}"
               r"(?:[ \t]+(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)[ \t]+"
               r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[ \t]+"
               r"[0-9]{1,2}[ \t]+[0-9]{2}:[0-9]{2}:[0-9]{2}[ \t]+[0-9]{4})?\s*")
    match = re.fullmatch(pattern, result.stdout)
    if not match:
        raise Blocked("current kernel boot time malformed")
    try:
        sec, usec = (int(value) for value in match.groups())
    except ValueError as error:
        raise Blocked("current kernel boot time malformed") from error
    if sec <= 0 or not 0 <= usec < 1_000_000:
        raise Blocked("current kernel boot time malformed")
    value = sec + usec / 1_000_000
    if value > time.time():
        raise Blocked("current kernel boot time malformed")
    return value


def _root_scope(registry_path, home_project, home_session, host, target_project):
    with closing(_open_ro(registry_path)) as db:
        _required(db, ("root_config", "root_directory_owner", "root_projects"))
        root = _one(db, "SELECT home_project,home_session,thread_id,host_id,mode FROM root_config WHERE singleton=1",
                    (), "configured Root row")
        owner = _one(db, "SELECT home_project,home_session FROM root_directory_owner WHERE singleton=1",
                     (), "Root directory owner")
        enrollment = _one(db, "SELECT session_id,orchestrator_session,joined FROM root_projects WHERE project_id=?",
                          (target_project,), "target project enrollment")
    if (root["home_project"], root["home_session"], root["host_id"]) != (home_project, home_session, host):
        raise Blocked("configured Root identity/host changed")
    if root["mode"] not in ("running", "maintenance_paused"):
        raise Blocked("configured Root is detaching or detached")
    if (owner["home_project"], owner["home_session"]) != (home_project, home_session):
        raise Blocked("configured Root directory owner changed")
    if enrollment["joined"] != 1 or not enrollment["session_id"]:
        raise Blocked("target project is not currently joined")
    return {"home_project": home_project, "home_session": home_session,
            "root_thread": root["thread_id"], "host": host,
            "target_project": target_project,
            "project_root_session": enrollment["session_id"],
            "project_orchestrator_session": enrollment["orchestrator_session"]}


def _registered_bridge(db, host):
    _required(db, ("bridge_dispatcher", "bridge_client_runtime"))
    lease = _one(db, "SELECT owner,acquired_at FROM bridge_dispatcher WHERE host_id=?", (host,), "registered bridge dispatcher")
    runtime = _one(db, "SELECT server,pid,heartbeat,error FROM bridge_client_runtime WHERE host_id=?", (host,), "registered bridge runtime")
    if (not isinstance(lease["owner"], str) or not lease["owner"] or
            not isinstance(lease["acquired_at"], (int, float)) or isinstance(lease["acquired_at"], bool) or
            not isinstance(runtime["pid"], int) or isinstance(runtime["pid"], bool) or runtime["pid"] <= 1 or
            runtime["error"] is not None or not isinstance(runtime["heartbeat"], (int, float)) or
            isinstance(runtime["heartbeat"], bool) or not 0 <= time.time() - runtime["heartbeat"] <= 15):
        raise Blocked("registered same-host bridge is unavailable or stale")
    from .native_owner_evidence import endpoint, EvidenceError
    try:
        endpoint(runtime["server"])
    except (EvidenceError, ValueError, TypeError, AttributeError) as error:
        raise Blocked("registered native endpoint is unavailable or ambiguous") from error
    return {"host": host, "owner": lease["owner"], "acquired_at": lease["acquired_at"],
            "pid": runtime["pid"], "endpoint": runtime["server"], "heartbeat": runtime["heartbeat"]}


def _bridge_identity(registration):
    return tuple(registration[key] for key in ("host", "owner", "acquired_at", "pid", "endpoint"))


def _owner_authority(db, scope, session_id, binding, host_row):
    if not binding["thread_id"] or host_row["host_id"] != scope["host"]:
        raise Blocked("owner thread is not bound to the configured Root host/project")
    parent = binding["parent_session"]
    if parent == scope["project_root_session"]:
        return {"kind": "native-child", "parent_session": parent}
    if parent is not None:
        raise Blocked("owner native parent differs from configured project Root")
    manager = scope["project_orchestrator_session"]
    if not manager or scope["project_root_session"] != scope["home_session"]:
        raise Blocked("current designated project management authority unavailable")
    _required(db, ("project_orchestrators", "agent_management", "agent_management_requests"))
    designation = _one(db, "SELECT session_id,root_session FROM project_orchestrators WHERE session_id=?",
                       (manager,), "current project orchestrator")
    link = _one(db, "SELECT target_session,manager_session,request_id FROM agent_management WHERE target_session=?",
                (session_id,), "current owner management link")
    request = _one(db, "SELECT id,root_session,manager_session,target_session,expected_thread,expected_host "
                   "FROM agent_management_requests WHERE id=?", (link["request_id"],), "immutable management request")
    if (designation["root_session"] != scope["home_session"] or link["manager_session"] != manager or
            (request["root_session"], request["manager_session"], request["target_session"],
             request["expected_thread"], request["expected_host"]) !=
            (scope["home_session"], manager, session_id, binding["thread_id"], scope["host"])):
        raise Blocked("current owner management authority differs from Root designation")
    return {"kind": "managed", "designation": designation, "link": link, "request": request}


def _target_snapshot(project_path, scope, boot):
    with closing(_open_ro(project_path)) as db:
        _required(db, ("resources", "resource_queue", "guarded_runs", "remote_guarded_runs",
                       "sessions", "bridge_bindings", "remote_session_hosts", "root_guard_generation"))
        registration = _registered_bridge(db, scope["host"])
        before_generation = guard_generation(db)
        claims = []
        owner_bindings = {}
        owner_authorities = {}
        fifo = {}
        guards = {}
        now = time.time()
        for resource in RESOURCES:
            claim = _one(db, "SELECT name,owner_session,reservation_id,token,granted_at,deadline,stale,reason FROM resources WHERE name=?",
                         (resource,), "exact resource claim")
            if (claim["name"] != resource or not claim["owner_session"] or not claim["reservation_id"]
                    or claim["stale"] != 1 or claim["deadline"] is None or claim["deadline"] > now):
                raise Blocked(resource + " claim is not the same exact expired owned row")
            token = claim.pop("token")
            claim["token_sha256"] = hashlib.sha256(token.encode()).hexdigest() if token else None
            claims.append(claim)
            queued = [dict(row) for row in db.execute(
                "SELECT seq,session,queued_at FROM resource_queue WHERE resource=? ORDER BY seq", (resource,))]
            fifo[resource] = queued
            local = [dict(row) for row in db.execute(
                "SELECT run_id,resource,reservation_id,session AS owner_session,pid,pgid,started_at,closed_at "
                "FROM guarded_runs WHERE resource=? AND closed_at IS NULL ORDER BY run_id", (resource,))]
            remote = [dict(row) for row in db.execute(
                "SELECT run_id,resource,reservation_id,session_id AS owner_session,host_id,local_pid AS pid,"
                "started_at,closed_at FROM remote_guarded_runs WHERE resource=? AND closed_at IS NULL ORDER BY run_id",
                (resource,))]
            guards[resource] = [{"table": "guarded_runs", **row} for row in local] + \
                               [{"table": "remote_guarded_runs", **row} for row in remote]
            if fifo[resource]:
                raise Blocked(resource + " FIFO is not empty")
            for session_id in {claim["owner_session"]}:
                if session_id in owner_bindings:
                    continue
                session = _one(db, "SELECT id FROM sessions WHERE id=?", (session_id,), "owner registration")
                binding = _one(db, "SELECT thread_id,parent_session FROM bridge_bindings WHERE session_id=?",
                               (session_id,), "owner thread binding")
                host_row = _one(db, "SELECT host_id FROM remote_session_hosts WHERE session_id=?",
                                (session_id,), "owner host binding")
                owner_authorities[session_id] = _owner_authority(db, scope, session_id, binding, host_row)
                owner_bindings[session_id] = {"session": session["id"], "thread": binding["thread_id"],
                                              "host": host_row["host_id"]}
        after_generation = guard_generation(db)
        if before_generation != after_generation:
            raise Blocked("guard generation changed during local snapshot")
    return {"claims": claims, "fifo": fifo, "guards": guards,
            "owner_bindings": owner_bindings, "owner_authorities": owner_authorities, "generation": before_generation,
            "boot_time": boot, "snapshot_time": now, "bridge_registration": registration}


def _selected_client(registration, kernel):
    """Bind the selected configured Root client to its current launchd and native generation."""
    from . import root_local, service
    from .native_owner_evidence import EvidenceError, endpoint
    config = service._installed_server_config()
    if os.getuid() != config.uid or os.geteuid() != config.uid:
        raise Blocked("managed client UID differs from configured Root")
    home = Path(pwd.getpwuid(config.uid).pw_dir)
    manager = service.ServiceManager(system="Darwin", home=home, server_bin=str(config.executable))
    paths = manager.paths("client")
    plist = root_local._file(paths["config"], config.uid)
    wrapper = root_local._file(paths["wrapper"], config.uid)
    before = (plist.stat(), wrapper.stat())
    plist_bytes = plist.read_bytes()
    obj = plistlib.loads(plist_bytes)
    label = service.LABEL + ".client"
    if obj.get("Label") != label or obj.get("ProgramArguments") != [str(wrapper)]:
        raise Blocked("managed client plist identity differs")
    proc = subprocess.run(["/bin/launchctl", "print", "gui/" + str(config.uid) + "/" + label], capture_output=True, text=True, timeout=3)
    if proc.returncode != 0 or proc.stderr:
        raise Blocked("managed client launchd state unavailable")
    def unique(field):
        matches = re.findall(r"^\s*" + field + r" = (.+?)\s*$", proc.stdout, re.MULTILINE)
        if len(matches) != 1: raise Blocked("managed client launchd " + field + " unavailable or ambiguous")
        return matches[0]
    pid_text = unique("pid")
    if not pid_text.isdigit() or int(pid_text) <= 1 or int(pid_text) != registration["pid"]:
        raise Blocked("managed client launchd/runtime PID differs")
    if unique("path") != str(plist) or unique("program") != str(wrapper):
        raise Blocked("loaded managed client configuration differs")
    client = kernel.process(int(pid_text))
    if client.uid != config.uid or client.ruid != config.uid or client.birth_sec <= 0:
        raise Blocked("managed client UID or birth unavailable")
    if registration["heartbeat"] < client.birth_sec + client.birth_usec / 1e6:
        raise Blocked("managed client replaced after registration")
    ep = endpoint(registration["endpoint"])
    suffix = "->%s:%s" % ep
    client_links = [(pid, fd, name) for pid, fd, name in kernel.lsof("-p",str(client.pid),"-iTCP","-sTCP:ESTABLISHED") if pid == client.pid and name.endswith(suffix) and name.startswith("127.0.0.1:")]
    if len(client_links) != 1:
        raise Blocked("managed client native connection missing or ambiguous")
    native = kernel.sample(registration)
    if len(native.chain) != 2 or native.chain[0].parent != client.pid or native.chain[-1] != client:
        raise Blocked("native listener is not the direct managed-client child")
    if root_local._file(paths["config"], config.uid) != plist or root_local._file(paths["wrapper"], config.uid) != wrapper:
        raise Blocked("managed client files changed")
    after = (plist.stat(), wrapper.stat())
    fields = lambda st: (st.st_dev, st.st_ino, st.st_uid, st.st_mode, st.st_size, st.st_mtime_ns)
    if tuple(map(fields,before)) != tuple(map(fields,after)):
        raise Blocked("managed client file identities changed")
    return (client, tuple(client_links[0]), native, hashlib.sha256(plist_bytes).hexdigest(), tuple(map(fields,after)))


def _observing_peer(kernel, registered, native, identity, expected=None):
    from .native_owner_evidence import endpoint
    ep = endpoint(registered["endpoint"])
    if (not isinstance(identity, dict) or set(identity) != {"client_instance", "connection_epoch", "pid", "fd", "local", "peer"}
            or not isinstance(identity["client_instance"], str) or not re.fullmatch(r"[0-9a-f]{32}", identity["client_instance"])
            or type(identity["connection_epoch"]) is not int or identity["connection_epoch"] <= 0
            or identity["pid"] != os.getpid() or type(identity["fd"]) is not int or identity["fd"] < 0
            or not isinstance(identity["local"], tuple) or len(identity["local"]) != 2
            or identity["local"][0] != "127.0.0.1" or type(identity["local"][1]) is not int
            or not 0 < identity["local"][1] < 65536 or identity["peer"] != ep):
        raise Blocked("calling RPC connection identity is invalid or not selected")
    local = "%s:%s" % identity["local"]
    remote = "%s:%s" % ep
    caller_name = local + "->" + remote
    caller = [row for row in kernel.lsof("-p", str(identity["pid"]), "-iTCP", "-sTCP:ESTABLISHED")
              if row == (identity["pid"], str(identity["fd"]), caller_name)]
    if len(caller) != 1:
        raise Blocked("calling RPC descriptor is not uniquely connected to the selected listener")
    reverse = remote + "->" + local
    peers = [row for row in kernel.lsof("-p", str(native.listener_pid), "-iTCP", "-sTCP:ESTABLISHED")
             if row[0] == native.listener_pid and row[2] == reverse]
    if len(peers) != 1:
        raise Blocked("calling RPC peer is not uniquely owned by the selected listener")
    pair = (caller[0], peers[0])
    if expected is not None and pair != expected:
        raise Blocked("calling RPC peer generation changed")
    return pair


@contextmanager
def _native_observations(owner_bindings, registration, rpc_factory=None):
    from .rpc import RpcClient, RpcError, TransportError
    from .native_owner_evidence import DarwinEvidence, EvidenceError, same_owner

    if rpc_factory is None:
        rpc_factory = lambda url: RpcClient(url, timeout=3)
    observations = {}
    started = time.time()
    try:
        kernel = DarwinEvidence()
        selected = _selected_client(registration, kernel)
        with rpc_factory(registration["endpoint"]) as rpc:
            identity = rpc.connection_identity()
            connected = _selected_client(registration, kernel)
            same_owner(selected[2], connected[2])
            if selected[:2] != connected[:2] or selected[3:] != connected[3:]:
                raise Blocked("managed client generation changed before native reads")
            pair = _observing_peer(kernel, registration, connected[2], identity)
            for session_id, binding in sorted(owner_bindings.items()):
                thread_id = binding["thread"]
                response = rpc.request("thread/read", {"threadId": thread_id, "includeTurns": False})
                thread = response.get("thread") if isinstance(response, dict) else None
                if not isinstance(thread, dict) or thread.get("id") != thread_id:
                    raise Blocked("exact current thread response is missing or mismatched")
                status = thread.get("status")
                state = status.get("type") if isinstance(status, dict) else None
                if state not in ("idle", "active", "notLoaded"):
                    raise Blocked("current native thread state is unknown")
                direct = thread.get("canAcceptDirectInput")
                if type(direct) is not bool:
                    raise Blocked("current native direct-input status is unknown")
                if state != "idle":
                    raise Blocked("current native owner is not positively idle")
                if not direct:
                    raise Blocked("current native owner is not positively idle: direct-input false")
                count, cursor, seen = 0, None, set()
                for _ in range(10):
                    params = {"threadId": thread_id, "limit": 100}
                    if cursor is not None:
                        params["cursor"] = cursor
                    page = rpc.request("thread/queue/list", params)
                    if not isinstance(page, dict) or not isinstance(page.get("data"), list):
                        raise Blocked("current native queue response is unknown")
                    count += len(page["data"])
                    if count > 1000:
                        raise Blocked("current native queue exceeds the bounded limit")
                    cursor = page.get("nextCursor")
                    if cursor is None:
                        break
                    if not isinstance(cursor, str) or not cursor or cursor in seen:
                        raise Blocked("current native queue pagination is ambiguous")
                    seen.add(cursor)
                else:
                    raise Blocked("current native queue pagination exceeded its bound")
                if count != 0:
                    raise Blocked("current native owner queue is not empty")
                observations[session_id] = {"thread_id": thread_id, "host": binding["host"],
                    "native_state": state, "direct_input": direct, "queue_count": count,
                    "checked_at": time.time(), "turn_id_known": False}
            observed = _selected_client(registration, kernel)
            same_owner(connected[2], observed[2])
            if connected[:2] != observed[:2] or connected[3:] != observed[3:]:
                raise Blocked("managed client generation changed during native reads")
            if rpc.connection_identity() != identity:
                raise Blocked("calling RPC connection changed during native reads")
            _observing_peer(kernel, registration, observed[2], identity, pair)
            finished = time.time()
            if finished - started > MAX_OBSERVATION_AGE:
                raise Blocked("native observation window exceeded five seconds")
            def revalidate(current):
                try:
                    final = _selected_client(current, kernel)
                    same_owner(connected[2], final[2])
                    if connected[:2] != final[:2] or connected[3:] != final[3:]:
                        raise Blocked("managed client generation changed at action boundary")
                    if rpc.connection_identity() != identity:
                        raise Blocked("calling RPC connection changed at action boundary")
                    _observing_peer(kernel, current, final[2], identity, pair)
                except (EvidenceError, OSError, ValueError, TypeError, AttributeError) as error:
                    raise Blocked("native owner/connected peer changed at action boundary") from error
            yield observations, started, finished, revalidate
    except (RpcError, TransportError, EvidenceError, OSError, ValueError, TypeError, AttributeError) as error:
        raise Blocked("configured local native read failed: " + type(error).__name__) from error


def _guard_current_facts(guards, claims, host, boot, kernel_factory=None):
    from .native_owner_evidence import DarwinEvidence, EvidenceError

    facts = {}
    try:
        kernel = (kernel_factory or DarwinEvidence)()
    except EvidenceError as error:
        raise Blocked("current host kernel process identity is unavailable") from error
    claim_by_resource = {claim["name"]: claim for claim in claims}
    for resource in RESOURCES:
        selected = guards[resource]
        if resource == "validation-clone":
            if len(selected) != 1:
                raise Blocked("validation-clone no longer has its one selected OPEN guard")
        elif selected:
            raise Blocked("heavy-unity:2 has an unexpected OPEN guard")
        rows = []
        for guard in selected:
            claim = claim_by_resource[resource]
            if (guard.get("resource") != resource or guard.get("reservation_id") != claim["reservation_id"]
                    or guard.get("owner_session") != claim["owner_session"]):
                raise Blocked("OPEN guard does not bind the exact selected claim tuple")
            if guard.get("table") == "remote_guarded_runs" and guard.get("host_id") is None:
                raise Blocked("remote OPEN guard host is unknown")
            if guard.get("table") == "remote_guarded_runs" and guard.get("host_id") != host:
                raise Blocked("remote OPEN guard is on a different host")
            start = guard.get("started_at")
            pid = guard.get("pid")
            if isinstance(start, bool) or not isinstance(start, (int, float)) or type(pid) is not int or pid <= 0:
                raise Blocked("OPEN guard process tuple is incomplete")
            if resource == "validation-clone" and start >= boot:
                raise Blocked("validation-clone selected guard did not start before the current host boot")
            if start >= boot:
                try:
                    process = kernel.process(pid)
                except EvidenceError:
                    if guard.get("table") == "remote_guarded_runs" or not guard.get("pgid"):
                        raise Blocked("post-boot OPEN guard lacks verifiable process-group closure")
                    try:
                        os.killpg(guard["pgid"], 0)
                    except ProcessLookupError:
                        pass
                    except PermissionError as error:
                        raise Blocked("OPEN guard process group is not inspectable") from error
                    else:
                        raise Blocked("OPEN guard process group remains present")
                    rows.append({"run_id": guard["run_id"], "state": "OPEN",
                                 "process_group": "absent_exact_pgid"})
                    continue
                if process.uid != os.getuid() or process.ruid != os.getuid():
                    raise Blocked("OPEN guard PID now belongs to a different UID")
                raise Blocked("OPEN guard PID is present after its recorded start")
            # A kernel boot strictly after the recorded exact run start proves
            # that original process incarnation cannot survive. It does not
            # close the durable OPEN row or prove restoration/helper history.
            try:
                process = kernel.process(pid)
                pid_state = "pid_reused_after_boot" if process.birth_sec + process.birth_usec / 1e6 > boot else "ambiguous_pid"
                if pid_state == "ambiguous_pid":
                    raise Blocked("recorded OPEN guard PID predates current boot or is ambiguous")
            except EvidenceError:
                pid_state = "no_current_pid_identity"
            rows.append({"run_id": guard["run_id"], "state": "OPEN",
                         "original_process": "predates_current_boot", "recorded_pid": pid,
                         "current_pid_observation": pid_state})
        facts[resource] = rows
    return facts


def _digest(snapshot, observations, root_scope):
    material = repr((root_scope, snapshot["claims"], snapshot["fifo"], snapshot["guards"],
                     snapshot["generation"], snapshot["owner_authorities"], snapshot["boot_time"], _bridge_identity(snapshot["bridge_registration"]), observations)).encode("utf-8")
    return hashlib.sha256(material).hexdigest()


def observe(master, registry, target_path, home_project, home_session, host, target_project,
            *, _rpc_factory=None, _boot_provider=None, _kernel_factory=None):
    """Configured Root CLI entry point. No caller-supplied booleans/PIDs/authority."""
    try:
        root_scope = _root_scope(registry, home_project, home_session, host, target_project)
        boot = _boot_time(_boot_provider)
        snapshot = _target_snapshot(target_path, root_scope, boot)
        with _native_observations(snapshot["owner_bindings"], snapshot["bridge_registration"], _rpc_factory) as (observations, observed_at, finished_at, _revalidate):
            guard_facts = _guard_current_facts(snapshot["guards"], snapshot["claims"], host, boot, _kernel_factory)
            # Re-read exact current tuples and generation after native observation.
            after = _target_snapshot(target_path, root_scope, boot)
            if _root_scope(registry, home_project, home_session, host, target_project) != root_scope:
                raise Blocked("configured Root/project enrollment changed during observation")
            if (snapshot["claims"] != after["claims"] or snapshot["fifo"] != after["fifo"]
                    or snapshot["guards"] != after["guards"] or snapshot["generation"] != after["generation"]
                    or snapshot["owner_bindings"] != after["owner_bindings"]
                    or snapshot["owner_authorities"] != after["owner_authorities"]
                    or _bridge_identity(snapshot["bridge_registration"]) != _bridge_identity(after["bridge_registration"])):
                raise Blocked("selected local capacity tuple changed during observation")
            if time.time() - finished_at > MAX_OBSERVATION_AGE:
                raise Blocked("native observation is stale at final tuple readback")
            return {"operation": "capacity-observe", "mode": "read_only_no_settlement",
                "verdict": "eligible_for_separate_nonordinary_review", "eligible": True,
                "resources": list(RESOURCES), "root": root_scope,
                "claim_rows": [{k: v for k, v in row.items() if k != "token_sha256"} for row in snapshot["claims"]],
                "capacity_scope": "exact resource-name single-owner rows; independent FIFO; no aggregate heavy-unity group cap",
                "guard_rows": guard_facts, "fifo": snapshot["fifo"],
                "owner_bindings": snapshot["owner_bindings"], "native_observations": observations,
                "boot_time": boot, "guard_generation": snapshot["generation"],
                "local_snapshot_time": snapshot["snapshot_time"],
                "observation_started_at": observed_at, "observation_finished_at": finished_at,
                "snapshot_sha256": _digest(snapshot, observations, root_scope),
                "historical_guard_state": "preserved_open_unknown",
                "process_restoration": "not_established", "mutation_performed": False,
                "ordinary_receipt": False, "next_step": "Root must revalidate exact snapshot before any separately reviewed operation"}
    except (Blocked, OSError, sqlite3.Error, KeyError, TypeError, ValueError) as error:
        return {"operation": "capacity-observe", "mode": "read_only_no_settlement",
                "verdict": "blocked_or_unknown", "eligible": False,
                "reason": str(error), "resources": list(RESOURCES),
                "mutation_performed": False, "ordinary_receipt": False}


@contextmanager
def _consumer_native_observations(owner_bindings, registration, rpc_factory=None):
    from .rpc import RpcClient, RpcError, TransportError
    from .native_owner_evidence import DarwinEvidence, EvidenceError, same_owner

    if rpc_factory is None:
        rpc_factory = lambda url: RpcClient(url, timeout=3)
    observations = {}
    started = time.time()
    try:
        kernel = DarwinEvidence()
        selected = _selected_client(registration, kernel)
        with rpc_factory(registration["endpoint"]) as rpc:
            identity = rpc.connection_identity()
            connected = _selected_client(registration, kernel)
            same_owner(selected[2], connected[2])
            if selected[:2] != connected[:2] or selected[3:] != connected[3:]:
                raise Blocked("managed client generation changed before native reads")
            pair = _observing_peer(kernel, registration, connected[2], identity)
            for session_id, binding in sorted(owner_bindings.items()):
                thread_id = binding["thread"]
                response = rpc.request("thread/read", {"threadId": thread_id, "includeTurns": False})
                thread = response.get("thread") if isinstance(response, dict) else None
                if not isinstance(thread, dict) or thread.get("id") != thread_id:
                    raise Blocked("exact current thread response is missing or mismatched")
                status = thread.get("status")
                state = status.get("type") if isinstance(status, dict) else None
                if state not in ("idle", "active", "notLoaded"):
                    raise Blocked("current native thread state is unknown")
                direct = thread.get("canAcceptDirectInput")
                if type(direct) is not bool:
                    raise Blocked("current native direct-input status is unknown")
                if state not in ("idle", "active") or not direct:
                    raise Blocked("current native requester state is not eligible")
                count, cursor, seen = 0, None, set()
                for _ in range(10):
                    params = {"threadId": thread_id, "limit": 100}
                    if cursor is not None:
                        params["cursor"] = cursor
                    page = rpc.request("thread/queue/list", params)
                    if not isinstance(page, dict) or not isinstance(page.get("data"), list):
                        raise Blocked("current native queue response is unknown")
                    count += len(page["data"])
                    if count > 1000:
                        raise Blocked("current native queue exceeds the bounded limit")
                    cursor = page.get("nextCursor")
                    if cursor is None:
                        break
                    if not isinstance(cursor, str) or not cursor or cursor in seen:
                        raise Blocked("current native queue pagination is ambiguous")
                    seen.add(cursor)
                else:
                    raise Blocked("current native queue pagination exceeded its bound")
                if count != 0:
                    raise Blocked("current native owner queue is not empty")
                observations[session_id] = {"thread_id": thread_id, "host": binding["host"],
                    "native_state": state, "direct_input": direct, "queue_count": count,
                    "checked_at": time.time(), "turn_id_known": False}
            observed = _selected_client(registration, kernel)
            same_owner(connected[2], observed[2])
            if connected[:2] != observed[:2] or connected[3:] != observed[3:]:
                raise Blocked("managed client generation changed during native reads")
            if rpc.connection_identity() != identity:
                raise Blocked("calling RPC connection changed during native reads")
            _observing_peer(kernel, registration, observed[2], identity, pair)
            finished = time.time()
            if finished - started > MAX_OBSERVATION_AGE:
                raise Blocked("native observation window exceeded five seconds")
            def revalidate(current):
                try:
                    final = _selected_client(current, kernel)
                    same_owner(connected[2], final[2])
                    if connected[:2] != final[:2] or connected[3:] != final[3:]:
                        raise Blocked("managed client generation changed at action boundary")
                    if rpc.connection_identity() != identity:
                        raise Blocked("calling RPC connection changed at action boundary")
                    _observing_peer(kernel, current, final[2], identity, pair)
                except (EvidenceError, OSError, ValueError, TypeError, AttributeError) as error:
                    raise Blocked("native owner/connected peer changed at action boundary") from error
            yield observations, started, finished, revalidate
    except (RpcError, TransportError, EvidenceError, OSError, ValueError, TypeError, AttributeError) as error:
        raise Blocked("configured local native read failed: " + type(error).__name__) from error
