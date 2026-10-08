"""Whitelisted server-side dispatcher for :mod:`agent_chat.remote`."""
from __future__ import annotations

import base64
import hashlib
import json
import posixpath
import secrets
import time
from typing import Any

from .core import CoordError, Coordinator, MAX_ATTACHMENTS
from .receipts import process_closure_proofs, validate_process_evidence


def _string(value: Any, name: str, required: bool = True) -> str | None:
    if value is None and not required:
        return None
    if not isinstance(value, str) or not value:
        raise CoordError(f"{name} must be a nonempty string")
    return value


def dispatch(coord: Coordinator, body: dict, *, bridge_manager=None, inbox_diagnostics=None,
             canonical_project=None) -> dict:
    """Execute one explicitly supported coordinator action.

    The request session is assigned to the Coordinator directly; server process
    environment can never accidentally impersonate a remote caller.
    """
    if not isinstance(body, dict) or set(body) - {"op", "session", "host_id", "params"}:
        raise CoordError("invalid coordinator request")
    op = _string(body.get("op"), "op")
    session = _string(body.get("session"), "session")
    host_id = _string(body.get("host_id"), "host_id")
    params = body.get("params", {})
    if not isinstance(params, dict):
        raise CoordError("params must be an object")
    if op == "register":
        _string(params.get("agent"), "agent")
    if coord.db.execute("SELECT 1 FROM retired_sessions WHERE id=?", (session,)).fetchone():
        raise CoordError("session is deregistered; register a new session")
    coord.session = session
    from .root_orchestrator import projection
    if projection(coord.db, session) and op in ('block','cancel','begin-guard','attach-guard','pulse-guard','rename'):
        raise CoordError('global root is control-only; use project agents for resource work and detach before a local rename')
    # Hosted file locks are logical keys, independent of server filesystem layout.
    coord.resource_name = resource_name
    if op == "rename":
        # Host authentication for a label edit must also leave expiry untouched.
        coord.db.execute("BEGIN IMMEDIATE")
        try:
            _schema(coord)
            _bind_host(coord, session, host_id, op)
        except BaseException:
            coord.db.rollback()
            raise
        else:
            coord.db.commit()
    else:
        # Remote inbox host binding is a real BEGIN IMMEDIATE acquisition before
        # the inbox query's own transaction.  Trace it separately so a lock wait
        # is not misattributed to later held inbox work.
        with coord.tx(observer=inbox_diagnostics if op == 'inbox' else None):
            _schema(coord)
            _bind_host(coord, session, host_id, op)
    # Keep this list auditable.  Do not replace it with getattr.
    if op == "register": return coord.register(_string(params.get("agent"), "agent"))
    if op == "rename": return coord.rename(_string(params.get("agent"), "agent"))
    if op == "status": return coord.status(params.get("mine", False), params.get("resources"))
    if op == "inbox": return coord.inbox(bool(params.get("all", False)), observer=inbox_diagnostics)
    if op == "policy":
        from .communication import current_policy
        return current_policy()
    if op == "context": return coord.context(params.get("limit", 20), params.get("max_bytes", 12288),
                                               params.get("cursor"), params.get("message_ids"), params.get("resources"),
                                               params.get("resource_cursor"))
    if op == "message": return coord.message(_string(params.get("id"), "id"))
    if op == "acknowledge": return coord.acknowledge(_string(params.get("id"), "id"))
    if op == "request":
        def record_pending(db, acquired):
            db.execute("INSERT INTO remote_reservation_outcomes (reservation_id,resource,owner_session,host_id,state,created_at) VALUES(?,?,?,?,?,?)", (acquired["reservation_id"], acquired["name"], session, host_id, "pending", time.time()))
        return coord.request(_string(params.get("resource"), "resource"), params.get("minutes"), record_pending)
    if op == "cancel": return coord.cancel(_string(params.get("resource"), "resource"))
    if op == "check": return coord.check(_string(params.get("resource"), "resource"), _string(params.get("token"), "token"))
    if op == "block": return coord.block(_string(params.get("resource"), "resource"), _string(params.get("reason"), "reason"))
    if op == "link-reply": return coord.link_reply(_string(params.get("id"), "id"), _string(params.get("reply_to"), "reply_to"))
    if op == "send":
        attachments = params.get("attachments", [])
        body = params.get("body", "")
        if not isinstance(body, str): raise CoordError("body must be a string")
        ack_reply = params.get("ack_reply", False)
        if not isinstance(ack_reply, bool): raise CoordError("ack_reply must be a boolean")
        targets = params.get("to")
        if isinstance(targets, str):
            return _send(coord, _string(targets, "to"), body, attachments, params.get("reply_to"), ack_reply)
        if targets is not None and (not isinstance(targets, list) or not all(isinstance(item, str) for item in targets)):
            raise CoordError("to must be a recipient string or a list of recipient strings")
        if ack_reply:
            raise CoordError("ack_reply is only supported for a direct recipient")
        if targets is None and params.get("reply_to") is not None:
            raise CoordError("reply_to requires an explicit recipient group")
        results = coord.send_group_prepared(targets, body, _prepare_attachments(coord, attachments), params.get("reply_to"))
        return _group_result(results)
    if op == "send-many":
        targets = params.get("to")
        if not isinstance(targets, list) or not all(isinstance(x, str) for x in targets):
            raise CoordError("to must be a list of recipient strings")
        return {"messages": coord.send_many(targets, _string(params.get("body"), "body", False) or "", params.get("reply_to"))}
    if op == "deregister": return coord.remove_session(coord.require_session())
    if op == 'admin-recover':
        from .agent_control import AdminRecoveryState
        allowed = {'project', 'resource', 'reservation_id', 'owner_session', 'request_id',
                   'operator', 'reason', 'evidence'}
        if set(params) != allowed:
            raise CoordError('admin-recover requires the exact project, target, audit, and evidence fields')
        return AdminRecoveryState(coord).recover(project=params.get('project'), canonical_project=canonical_project,
            resource=params.get('resource'), reservation_id=params.get('reservation_id'),
            owner_session=params.get('owner_session'), request_id=params.get('request_id'),
            operator=params.get('operator'), reason=params.get('reason'), evidence=params.get('evidence'),
            host_id=host_id)
    if op == "remove-session": return coord.remove_session(_string(params.get("id"), "id"))
    if op in ('supervisor-configure','supervisor-status'):
        from .bridge_state import BridgeState
        from .supervisor import SupervisorState
        BridgeState(coord); supervisor=SupervisorState(coord)
        if op == 'supervisor-configure':
            if set(params) not in ({'enabled'}, {'recovery_enabled'}):
                raise CoordError('supervisor-configure accepts exactly one of enabled or recovery_enabled')
            if not isinstance(next(iter(params.values())), bool):
                raise CoordError('supervisor-configure setting must be boolean')
            if 'recovery_enabled' in params:
                return supervisor._configure_recovery(params['recovery_enabled'])
            return supervisor.configure(params['enabled'])
        if set(params) - {'to'}: raise CoordError('supervisor-status accepts only optional to')
        return supervisor.status(params.get('to'))
    if op in ('agent-stop','agent-resume','agent-model','agent-model-resolve','agent-status','agents'):
        from .bridge_state import BridgeState
        from .agent_control import AgentControlState
        BridgeState(coord)
        controls = AgentControlState(coord)
        if op == 'agent-model-resolve':
            return controls.resolve_unchanged(params.get('request_id'), params.get('expected_thread'),
                                              params.get('resolution_id'), params.get('confirm'))
        if op == 'agents':
            rows = controls.roster()
            from .root_orchestrator import projection
            if projection(coord.db, session):
                allowed = {row['session_id'] for row in coord.db.execute('SELECT session_id FROM project_orchestrators WHERE root_session=?',(session,))} | {session}
                rows = [row for row in rows if row['id'] in allowed]
            return {'agents':rows}
        target = _string(params.get('to') or session, 'to')
        if op == 'agent-status':
            refresh = None
            if params.get('refresh'):
                refresh = controls.request(target, params.get('expected_thread'), 'inspect', params.get('request_id'), True)
            return dict(controls.status(target), refresh=refresh)
        return controls.request(target, params.get('expected_thread'), op.removeprefix('agent-'), params.get('request_id'),
                                params.get('confirm'), params.get('interrupt',False), model=params.get('model'),
                                reasoning_effort=params.get('reasoning_effort'),
                                allow_cold_resume=params.get('allow_cold_resume', False))
    if op.startswith('session-reset') or op == 'agent-create':
        from .bridge_state import BridgeState
        from .session_reset import SessionResetState
        BridgeState(coord)
        resets = SessionResetState(coord)
        if op == 'session-reset-revalidate-info':
            return resets.revalidate_info(_string(params.get('id'), 'id'))
        if op == 'session-reset-revalidate':
            return resets.revalidate_workspace(_string(params.get('id'), 'id'), params.get('proof'), params.get('confirm'))
        if op == 'session-reset-status':
            return resets.status(_string(params.get('to') or session, 'to'))
        if op == 'session-reset-cancel':
            return resets.cancel(_string(params.get('id'), 'id'))
        if op == 'session-reset-resolve':
            return resets.resolve_creation(_string(params.get('id'), 'id'), params.get('thread'), params.get('confirm_created'))
        if op == 'session-reset-retry':
            return resets.retry_prompt(_string(params.get('id'), 'id'), params.get('confirm_not_started'))
        if op == 'agent-create':
            return resets.create(params.get('agent'), params.get('expected_thread'), params.get('prompt'),
                params.get('request_id'), params.get('confirm'), model=params.get('model'),
                reasoning_effort=params.get('reasoning_effort'), cwd=params.get('cwd'))
        if op == 'session-reset':
            return resets.request(_string(params.get('to') or session, 'to'), params.get('expected_thread'),
                                  params.get('prompt'), params.get('request_id'), params.get('confirm'),
                                  model=params.get('model'), reasoning_effort=params.get('reasoning_effort'))
    if op in ("bind", "unbind", "bridge-status", "bridge-retry", "bridge-resolve"):
        from .bridge_state import BridgeState
        state = BridgeState(coord)
        if op == "bind": return state.bind(params.get("thread"), params.get("parent_session"), params.get("agent_path"))
        if op == "unbind": return state.unbind()
        if op == "bridge-status":
            mine = params.get("mine", False)
            if not isinstance(mine, bool):
                raise CoordError("mine must be a boolean")
            return state.status(mine)
        if op in ("bridge-retry", "bridge-resolve"):
            from .bridge import exclusive_bridge
            recovery = bridge_manager.recovery(state.resolve(session)) if bridge_manager else exclusive_bridge(coord.path)
            with recovery:
                if op == "bridge-retry": return state.retry(_string(params.get("job_id"), "job_id"), bool(params.get("confirm_not_started", False)))
                return state.resolve_delivered(_string(params.get("job_id"), "job_id"), bool(params.get("confirm_delivered", False)))
    if op == "guard-context":
        resource = resource_name(_string(params.get("resource"), "resource"))
        with coord.tx() as db:
            r = coord._row(db, resource)
            if not r or r["owner_session"] != session:
                raise CoordError("reservation proof requires its owning session")
            owner = db.execute("SELECT host_id FROM remote_session_hosts WHERE session_id=?", (r["owner_session"],)).fetchone() if r else None
            if not owner or owner["host_id"] != host_id:
                raise CoordError("reservation proof requires its owning host")
            runs = [dict(row) for row in db.execute("SELECT run_id,local_pid,started_at,closed_at FROM remote_guarded_runs WHERE reservation_id=?", (r["reservation_id"],))]
            return {"reservation_id": r["reservation_id"], "runs": runs}
    if op == "reservation-result":
        resource = resource_name(_string(params.get("resource"), "resource"))
        reservation_id = _string(params.get("reservation_id"), "reservation_id")
        with coord.tx() as db:
            outcome = db.execute("SELECT state,action,receipt_sha256,evidence_sha256,cleared_at FROM remote_reservation_outcomes WHERE reservation_id=? AND resource=? AND owner_session=? AND host_id=?", (reservation_id, resource, session, host_id)).fetchone()
            if not outcome:
                return {"state": "unknown"}
            if outcome["state"] == "pending":
                retained = coord.retained_reservation_result(db, resource, reservation_id)
                if retained["state"] == "unknown": return retained
                return {"state": "pending", "resource": resource, "reservation_id": reservation_id, "receipt_backed_clear": False}
            if outcome["state"] == "cleared":
                return {"state": "cleared", "resource": resource, "reservation_id": reservation_id, "action": outcome["action"], "receipt_sha256": outcome["receipt_sha256"], "evidence_sha256": outcome["evidence_sha256"], "recorded_at": outcome["cleared_at"], "receipt_backed_clear": True}
            return {"state": "unknown"}
    if op in ('producer-establish', 'producer-close', 'producer-status'):
        from .lifecycle_producer import dispatch as producer_dispatch
        return producer_dispatch(coord, session, host_id, op, params)
    if op == "begin-guard": return _begin_guard(coord, session, host_id, params)
    if op == "begin-guard-batch": return _begin_guard_batch(coord, session, host_id, params)
    if op == "attach-guard-pid": return _attach_guard_pid(coord, session, host_id, params)
    if op == "guard-pulse": return _guard_pulse(coord, session, host_id, params)
    if op == "close-guard": return _close_guard(coord, session, host_id, params)
    if op in ("release", "recover"): return _remote_clear(coord, session, host_id, params, op)
    raise CoordError("unsupported remote coordinator operation: " + op)


def resource_name(raw):
    if not isinstance(raw, str) or not raw or raw != raw.strip():
        raise CoordError("resource name is invalid")
    if raw.startswith('file:'):
        part = raw[5:]
        normalized = posixpath.normpath(part)
        if not part or part.startswith('/') or normalized in ('.', '..') or normalized.startswith('../') or '\x00' in part or '\\' in part:
            raise CoordError('file resource must be a repo-relative path within the project')
        return 'file:' + normalized
    if any(c.isspace() for c in raw) or '..' in raw.split('/'):
        raise CoordError('resource name is invalid')
    return raw


def _prepare_attachments(coord: Coordinator, attachments: Any) -> list[tuple[str, str, bytes]]:
    if not isinstance(attachments, list) or len(attachments) > MAX_ATTACHMENTS:
        raise CoordError(f"attachments must contain at most {MAX_ATTACHMENTS} files")
    prepared = []
    for item in attachments:
        if not isinstance(item, dict) or not isinstance(item.get("name"), str) or not isinstance(item.get("content_base64"), str):
            raise CoordError("remote attachment is invalid")
        try: content = base64.b64decode(item["content_base64"], validate=True)
        except ValueError as error: raise CoordError("remote attachment is invalid base64") from error
        prepared.append(coord._prepare_attachment(item["name"], content))
    return prepared


def _send(coord: Coordinator, target: str, body: str, attachments: Any, reply_to: Any,
          ack_reply: bool = False) -> dict:
    return coord._send_prepared(target, body, _prepare_attachments(coord, attachments), reply_to, ack_reply)


def _group_result(results: list[dict]) -> dict:
    return {"batch_id": results[0]["batch_id"], "deliveries": [
        {"id": delivery["id"], "recipient_session": delivery["recipient_session"],
         "recipient_agent": delivery["recipient_agent"]}
        for delivery in results[0]["deliveries"]
    ]}


def _schema(coord: Coordinator) -> None:
    # executescript implicitly commits SQLite transactions.  These individual
    # statements therefore retain the BEGIN IMMEDIATE lock established by tx().
    coord.db.execute("CREATE TABLE IF NOT EXISTS remote_session_hosts (session_id TEXT PRIMARY KEY, host_id TEXT NOT NULL, registered_at REAL NOT NULL, last_seen_at REAL NOT NULL)")
    coord.db.execute("CREATE TABLE IF NOT EXISTS remote_guarded_runs (run_id TEXT PRIMARY KEY, resource TEXT NOT NULL, reservation_id TEXT NOT NULL, session_id TEXT NOT NULL, host_id TEXT NOT NULL, local_pid INTEGER NOT NULL, started_at REAL NOT NULL, closed_at REAL, close_evidence_sha256 TEXT)")
    coord.db.execute("CREATE TABLE IF NOT EXISTS remote_restoration_runs (run_id TEXT PRIMARY KEY, deadline REAL NOT NULL)")
    coord.db.execute("CREATE TABLE IF NOT EXISTS remote_guard_batches (run_id TEXT PRIMARY KEY, batch_id TEXT NOT NULL)")
    coord.db.execute("CREATE TABLE IF NOT EXISTS remote_reservation_outcomes (reservation_id TEXT PRIMARY KEY, resource TEXT NOT NULL, owner_session TEXT NOT NULL, host_id TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('pending','cleared')), action TEXT, receipt_sha256 TEXT, evidence_sha256 TEXT, created_at REAL NOT NULL, cleared_at REAL, CHECK((state='pending' AND action IS NULL AND receipt_sha256 IS NULL AND evidence_sha256 IS NULL AND cleared_at IS NULL) OR (state='cleared' AND action IN ('release','recover') AND receipt_sha256 IS NOT NULL AND evidence_sha256 IS NOT NULL AND cleared_at IS NOT NULL)))")


def _bind_host(coord: Coordinator, session: str, host: str, op: str) -> None:
    from .root_team_reset import quarantined
    if quarantined(coord.db,session=session):
        raise CoordError('session is quarantined by team reset; old credentials cannot control current work')
    if coord.db.execute('SELECT 1 FROM retired_sessions WHERE id=?',(session,)).fetchone():
        raise CoordError('session is retired; old credentials cannot control the current project')
    """Pin a remote identity before an operation can mention local PIDs/proofs."""
    row = coord.db.execute("SELECT host_id FROM remote_session_hosts WHERE session_id=?", (session,)).fetchone()
    if row and row["host_id"] != host:
        raise CoordError("session is bound to a different remote host")
    if row:
        coord.db.execute("UPDATE remote_session_hosts SET last_seen_at=? WHERE session_id=?", (time.time(), session))
        return
    # Legacy reservations/runs have PIDs meaningful only on their original host.
    held = coord.db.execute("SELECT 1 FROM resources WHERE owner_session=? LIMIT 1", (session,)).fetchone()
    local_runs = coord.db.execute("SELECT 1 FROM guarded_runs WHERE session=? AND closed_at IS NULL LIMIT 1", (session,)).fetchone()
    if held or local_runs:
        raise CoordError("cannot bind a legacy session with held resources or open local guarded runs to a remote host")
    # A remote caller must still be registered except register itself.
    if op != "register" and not coord.db.execute("SELECT 1 FROM sessions WHERE id=?", (session,)).fetchone():
        raise CoordError("unknown session; register first")
    coord.db.execute("INSERT INTO remote_session_hosts VALUES(?,?,?,?)", (session, host, time.time(), time.time()))


def _begin_guard_batch(coord: Coordinator, session: str, host: str, p: dict) -> dict:
    from .root_lifecycle import stopped
    if stopped(coord.db,session):
        raise CoordError('root stop-all barrier withheld new guarded work')
    """Admit a bounded same-caller set before any launcher can be activated."""
    if set(p) - {'tuples', 'restore', 'max_seconds'}:
        raise CoordError('invalid batch admission parameters')
    tuples = p.get('tuples')
    restore = p.get('restore', False)
    if not isinstance(tuples, list) or not 1 <= len(tuples) <= 42 or type(restore) is not bool:
        raise CoordError('invalid batch admission')
    budget = p.get('max_seconds', 120)
    if restore:
        if type(budget) is not int or not 1 <= budget <= 900:
            raise CoordError('invalid batch restoration budget')
    elif 'max_seconds' in p:
        raise CoordError('batch restoration budget requires restoration mode')
    prepared, seen = [], set()
    for item in tuples:
        if not isinstance(item, dict) or set(item) != {'resource', 'session_id', 'host_id', 'reservation_id', 'token'}:
            raise CoordError('invalid batch admission tuple')
        if any(not isinstance(item[key], str) or not item[key] for key in item):
            raise CoordError('invalid batch admission tuple')
        if item['session_id'] != session or item['host_id'] != host:
            raise CoordError('batch admission requires the authenticated caller host session')
        resource = coord.resource_name(item['resource'])
        if resource != item['resource'] or resource in seen:
            raise CoordError('batch admission requires distinct exact resources')
        seen.add(resource)
        prepared.append(item)
    from .core import assert_agent_running
    from .root_orchestrator import projection
    with coord.tx() as db:
        coord._fresh(db, session)
        if projection(db, session):
            raise CoordError('global root cannot admit resource work')
        assert_agent_running(db, session, cleanup=True)
        pinned = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (session,)).fetchone()
        if not pinned or pinned['host_id'] != host:
            raise CoordError('batch admission requires the authenticated caller host session')
        rows = []
        for item in prepared:
            row = coord._verify_owner(db, item['resource'], session, item['token'], restore)
            if row['reservation_id'] != item['reservation_id']:
                raise CoordError('batch admission reservation changed')
            if restore:
                if coord._state(row) != 'stale':
                    raise CoordError('batch restoration requires stale reservations')
                if db.execute('SELECT 1 FROM remote_guarded_runs WHERE reservation_id=? AND closed_at IS NULL',
                              (row['reservation_id'],)).fetchone():
                    raise CoordError('close existing guarded runs before batch restoration')
            rows.append(row)
        # No row or association is created until the final tuple is validated.
        batch_id = 'remote_batch_' + secrets.token_hex(16)
        admissions, started = [], time.time()
        for row in rows:
            run_id = 'remote_run_' + secrets.token_hex(16)
            db.execute('INSERT INTO remote_guarded_runs VALUES(?,?,?,?,?,?,?,?,?)',
                       (run_id, row['name'], row['reservation_id'], session, host, 0, started, None, None))
            db.execute('INSERT INTO remote_guard_batches VALUES(?,?)', (run_id, batch_id))
            if restore:
                db.execute('INSERT INTO remote_restoration_runs VALUES(?,?)', (run_id, started + budget))
            admissions.append({'resource': row['name'], 'reservation_id': row['reservation_id'],
                               'run_id': run_id, 'host_id': host})
    return {'batch_id': batch_id, 'admissions': admissions}


def _begin_guard(coord: Coordinator, session: str, host: str, p: dict) -> dict:
    from .root_lifecycle import stopped
    if stopped(coord.db,session):
        raise CoordError('root stop-all barrier withheld new guarded work')
    resource, token = _string(p.get("resource"), "resource"), _string(p.get("token"), "token")
    restore = p.get("restore", False)
    if not isinstance(restore, bool): raise CoordError("restore must be a boolean")
    budget = p.get("max_seconds")
    if restore:
        reservation_id = _string(p.get("reservation_id"), "reservation_id")
        budget = 120 if budget is None else budget
        if not isinstance(budget, int) or isinstance(budget, bool) or not 1 <= budget <= 900:
            raise CoordError("restoration max_seconds must be an integer from 1 to 900")
    elif p.get("reservation_id") is not None or budget is not None:
        raise CoordError("reservation_id and max_seconds require restoration mode")
    with coord.tx() as db:
        coord._fresh(db, session)
        r = coord._verify_owner(db, coord.resource_name(resource), session, token, restore)
        from .core import assert_agent_running
        assert_agent_running(db, session, cleanup=True)
        if restore:
            if coord._state(r) != "stale": raise CoordError("restoration requires a stale reservation")
            if r["reservation_id"] != reservation_id: raise CoordError("restoration reservation_id does not match")
            if db.execute("SELECT 1 FROM remote_guarded_runs WHERE reservation_id=? AND closed_at IS NULL", (reservation_id,)).fetchone():
                raise CoordError("close existing guarded runs before starting restoration")
        run_id = "remote_run_" + hashlib.sha256((host + session + str(time.time())).encode()).hexdigest()[:32]
        db.execute("INSERT INTO remote_guarded_runs VALUES(?,?,?,?,?,?,?,?,?)",
                   (run_id, r["name"], r["reservation_id"], session, host, 0, time.time(), None, None))
        if restore:
            db.execute("INSERT INTO remote_restoration_runs VALUES(?,?)", (run_id, time.time() + budget))
    return {"run_id": run_id, "reservation_id": r["reservation_id"], "host_id": host}


def _check_guard(coord: Coordinator, db, row, session: str, token: str) -> dict:
    restoration = db.execute("SELECT deadline FROM remote_restoration_runs WHERE run_id=?", (row["run_id"],)).fetchone()
    r = coord._verify_owner(db, row["resource"], session, token, restoration is not None)
    if r["reservation_id"] != row["reservation_id"]: raise CoordError("guarded run reservation has changed")
    if restoration:
        if coord._state(r) != "stale": raise CoordError("restoration requires a stale reservation")
        if time.time() >= restoration["deadline"]: raise CoordError("restoration deadline has expired")
    result = {"state": coord._state(r), "resource": r["name"], "reservation_id": r["reservation_id"], "deadline": r["deadline"]}
    if restoration: result.update(restore=True, restoration_deadline=restoration["deadline"])
    return result


def _attach_guard_pid(coord: Coordinator, session: str, host: str, p: dict) -> dict:
    run_id, token, pid = _string(p.get("run_id"), "run_id"), _string(p.get("token"), "token"), p.get("pid")
    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0: raise CoordError("pid must be a positive integer")
    with coord.tx() as db:
        row = db.execute("SELECT * FROM remote_guarded_runs WHERE run_id=?", (run_id,)).fetchone()
        if not row or row["session_id"] != session or row["host_id"] != host or row["closed_at"] is not None or row["local_pid"] != 0: raise CoordError("remote guarded run cannot be attached")
        _check_guard(coord, db, row, session, token)
        db.execute("UPDATE remote_guarded_runs SET local_pid=? WHERE run_id=?", (pid, run_id))
    return {"run_id": run_id, "attached": True}


def _guard_pulse(coord: Coordinator, session: str, host: str, p: dict) -> dict:
    run_id, token = _string(p.get("run_id"), "run_id"), _string(p.get("token"), "token")
    with coord.tx() as db:
        row = db.execute("SELECT * FROM remote_guarded_runs WHERE run_id=?", (run_id,)).fetchone()
        if not row or row["session_id"] != session or row["host_id"] != host or row["closed_at"] is not None:
            raise CoordError("remote guarded run is not open on this host")
        return _check_guard(coord, db, row, session, token)


def _close_guard(coord: Coordinator, session: str, host: str, p: dict) -> dict:
    run_id, token = _string(p.get("run_id"), "run_id"), _string(p.get("token"), "token")
    evidence = _string(p.get("evidence_sha256"), "evidence_sha256")
    if len(evidence) != 64 or any(c not in "0123456789abcdef" for c in evidence): raise CoordError("closure evidence hash must be lowercase SHA-256")
    with coord.tx() as db:
        row = db.execute("SELECT * FROM remote_guarded_runs WHERE run_id=?", (run_id,)).fetchone()
        if not row or row["session_id"] != session or row["host_id"] != host:
            raise CoordError("remote guarded run does not belong to this host session")
        coord._verify_owner(db, row["resource"], session, token, True)
        db.execute("UPDATE remote_guarded_runs SET closed_at=COALESCE(closed_at,?),close_evidence_sha256=? WHERE run_id=?",
                   (time.time(), evidence, run_id))
    return {"run_id": run_id, "closed": True}


def _remote_clear(coord: Coordinator, session: str, host: str, p: dict, action: str) -> dict:
    resource = coord.resource_name(_string(p.get("resource"), "resource"))
    receipt = p.get("receipt")
    evidence = p.get("evidence_base64")
    if not isinstance(receipt, dict) or not isinstance(evidence, str):
        raise CoordError("remote release/recovery requires receipt object and base64 evidence")
    try: evidence_bytes = base64.b64decode(evidence, validate=True)
    except ValueError as error: raise CoordError("remote receipt evidence is invalid base64") from error
    if not evidence_bytes or len(evidence_bytes) > 2 * 1024 * 1024:
        raise CoordError("remote receipt evidence is missing or too large")
    try:
        proofs = process_closure_proofs(receipt)
        reports = p.get('process_evidence_base64', {})
        if not isinstance(reports, dict) or set(reports) != {str(pid) for pid in proofs}:
            raise ValueError('process closure evidence must cover exactly the receipt proofs')
        for pid, proof in proofs.items():
            if not isinstance(reports[str(pid)], str):
                raise ValueError('process closure evidence is invalid base64')
            validate_process_evidence(proof, base64.b64decode(reports[str(pid)], validate=True))
    except ValueError as error: raise CoordError(str(error)) from error
    if receipt.get("resource") != resource or receipt.get("restored") is not True or receipt.get("processes_closed") is not True:
        raise CoordError("remote receipt does not bind a restored reservation")
    if not isinstance(receipt["closed_at"], (int, float)) or isinstance(receipt["closed_at"], bool) or not isinstance(receipt["pids"], list) or any(not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0 for pid in receipt["pids"]):
        raise CoordError("remote receipt has invalid closure data")
    with coord.tx() as db:
        coord._fresh(db, session)
        r = coord._row(db, resource)
        if not r or not r["owner_session"]:
            raise CoordError("resource is not held")
        if r["owner_session"] != session:
            raise CoordError("reservation proof requires its owning session")
        owner_host = db.execute("SELECT host_id FROM remote_session_hosts WHERE session_id=?", (r["owner_session"],)).fetchone()
        if not owner_host or owner_host["host_id"] != host:
            raise CoordError("cannot release or recover a reservation without the owning host's proof")
        if action == "release": coord._verify_owner(db, resource, session, _string(p.get("token"), "token"), True)
        if action == "recover" and coord._state(r) != "stale": raise CoordError("only a stale reservation can be recovered")
        if receipt["reservation_id"] != r["reservation_id"] or not r["granted_at"] <= receipt["closed_at"] <= time.time():
            raise CoordError("remote receipt does not bind this reservation")
        open_rows = db.execute("SELECT run_id,local_pid,host_id,started_at FROM remote_guarded_runs WHERE reservation_id=? AND closed_at IS NULL", (r["reservation_id"],)).fetchall()
        if action == "recover" and open_rows:
            # The server cannot inspect another host's PIDs.  It accepts only
            # the owning host's already client-validated receipt, and only when
            # each activated process is explicitly covered by that proof.
            # PID 0 is safe to clear only because the client gate never allows
            # execution until attach-guard-pid commits the actual process group.
            if any(row["host_id"] != host or (row["local_pid"] != 0 and row["local_pid"] not in receipt["pids"]) or proofs.get(row["local_pid"], receipt)["closed_at"] < row["started_at"] for row in open_rows):
                raise CoordError("remote open guarded run is not covered by this host receipt")
            db.executemany("UPDATE remote_guarded_runs SET closed_at=?,close_evidence_sha256=? WHERE run_id=?",
                           [(proofs.get(row["local_pid"], receipt)["closed_at"], proofs.get(row["local_pid"], {}).get('evidence_sha256', hashlib.sha256(evidence_bytes).hexdigest()), row["run_id"]) for row in open_rows])
        open_runs = db.execute("SELECT 1 FROM remote_guarded_runs WHERE reservation_id=? AND closed_at IS NULL", (r["reservation_id"],)).fetchone()
        if open_runs: raise CoordError("remote guarded run remains open")
        latest = db.execute("SELECT MAX(closed_at) value FROM remote_guarded_runs WHERE reservation_id=?", (r["reservation_id"],)).fetchone()["value"]
        if latest is not None and receipt["closed_at"] < latest: raise CoordError("remote receipt predates guarded run closure")
        canonical = json.dumps(receipt, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(canonical.encode()).hexdigest()
        if db.execute("SELECT 1 FROM receipts WHERE reservation_id=? OR receipt_sha256=?", (r["reservation_id"], digest)).fetchone():
            raise CoordError("receipt has already been used")
        evidence_digest = hashlib.sha256(evidence_bytes).hexdigest()
        recorded_at = time.time()
        db.execute("INSERT INTO receipts VALUES(?,?,?,?,?,?)", (r["reservation_id"], digest, evidence_digest, canonical, action, recorded_at))
        outcome = db.execute("SELECT resource,owner_session,host_id,state FROM remote_reservation_outcomes WHERE reservation_id=?", (r["reservation_id"],)).fetchone()
        if outcome:
            if (outcome["resource"], outcome["owner_session"], outcome["host_id"], outcome["state"]) != (resource, session, host, "pending"):
                raise CoordError("remote reservation outcome is inconsistent")
            if db.execute("UPDATE remote_reservation_outcomes SET state='cleared',action=?,receipt_sha256=?,evidence_sha256=?,cleared_at=? WHERE reservation_id=? AND state='pending'", (action, digest, evidence_digest, recorded_at, r["reservation_id"])).rowcount != 1:
                raise CoordError("remote reservation outcome could not be committed")
        db.execute("UPDATE resources SET owner_session=NULL,reservation_id=NULL,token=NULL,granted_at=NULL,deadline=NULL,stale=0,reason=NULL WHERE name=?", (resource,))
    return {"resource": resource, "released" if action == "release" else "recovered": True}
