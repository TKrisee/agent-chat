"""Whitelisted server-side dispatcher for :mod:`agent_chat.remote`."""
from __future__ import annotations

import base64
import hashlib
import json
import posixpath
import secrets
import time
from typing import Any

from .core import CoordError, Coordinator


def _string(value: Any, name: str, required: bool = True) -> str | None:
    if value is None and not required:
        return None
    if not isinstance(value, str) or not value:
        raise CoordError(f"{name} must be a nonempty string")
    return value


def dispatch(coord: Coordinator, body: dict) -> dict:
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
    coord.session = session
    # Hosted file locks are logical keys, independent of server filesystem layout.
    coord.resource_name = resource_name
    with coord.tx():
        _schema(coord)
        _bind_host(coord, session, host_id, op)
    # Keep this list auditable.  Do not replace it with getattr.
    if op == "register": return coord.register(_string(params.get("agent"), "agent"))
    if op == "status": return coord.status()
    if op == "inbox": return coord.inbox(bool(params.get("all", False)))
    if op == "acknowledge": return coord.acknowledge(_string(params.get("id"), "id"))
    if op == "request": return coord.request(_string(params.get("resource"), "resource"), params.get("minutes"))
    if op == "cancel": return coord.cancel(_string(params.get("resource"), "resource"))
    if op == "check": return coord.check(_string(params.get("resource"), "resource"), _string(params.get("token"), "token"))
    if op == "block": return coord.block(_string(params.get("resource"), "resource"), _string(params.get("reason"), "reason"))
    if op == "link-reply": return coord.link_reply(_string(params.get("id"), "id"), _string(params.get("reply_to"), "reply_to"))
    if op == "send":
        attachments = params.get("attachments", [])
        body = params.get("body", "")
        if not isinstance(body, str): raise CoordError("body must be a string")
        return _send(coord, _string(params.get("to"), "to"), body, attachments, params.get("reply_to"))
    if op == "send-many":
        targets = params.get("to")
        if not isinstance(targets, list) or not all(isinstance(x, str) for x in targets):
            raise CoordError("to must be a list of recipient strings")
        return {"messages": coord.send_many(targets, _string(params.get("body"), "body", False) or "", params.get("reply_to"))}
    if op == "remove-session": return coord.remove_session(_string(params.get("id"), "id"))
    if op in ("bind", "unbind", "bridge-status", "bridge-retry", "bridge-resolve"):
        from .bridge_state import BridgeState
        state = BridgeState(coord)
        if op == "bind": return state.bind(params.get("thread"), params.get("parent_session"), params.get("agent_path"))
        if op == "unbind": return state.unbind()
        if op == "bridge-status": return state.status()
        if op in ("bridge-retry", "bridge-resolve"):
            from .bridge import exclusive_bridge
            with exclusive_bridge(coord.path):
                if op == "bridge-retry": return state.retry(_string(params.get("job_id"), "job_id"), bool(params.get("confirm_not_started", False)))
                return state.resolve_delivered(_string(params.get("job_id"), "job_id"), bool(params.get("confirm_delivered", False)))
    if op == "guard-context":
        resource = resource_name(_string(params.get("resource"), "resource"))
        with coord.tx() as db:
            r = coord._row(db, resource)
            owner = db.execute("SELECT host_id FROM remote_session_hosts WHERE session_id=?", (r["owner_session"],)).fetchone() if r else None
            if not owner or owner["host_id"] != host_id:
                raise CoordError("reservation proof requires its owning host")
            runs = [dict(row) for row in db.execute("SELECT run_id,local_pid,started_at,closed_at FROM remote_guarded_runs WHERE reservation_id=?", (r["reservation_id"],))]
            return {"reservation_id": r["reservation_id"], "runs": runs}
    if op == "begin-guard": return _begin_guard(coord, session, host_id, params)
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


def _send(coord: Coordinator, target: str, body: str, attachments: Any, reply_to: Any) -> dict:
    if not isinstance(attachments, list) or len(attachments) > 4:
        raise CoordError("attachments must contain at most four images")
    prepared = []
    for item in attachments:
        if not isinstance(item, dict) or not isinstance(item.get("name"), str) or not isinstance(item.get("content_base64"), str):
            raise CoordError("remote attachment is invalid")
        try: content = base64.b64decode(item["content_base64"], validate=True)
        except ValueError as error: raise CoordError("remote attachment is invalid base64") from error
        if not content or len(content) > 10 * 1024 * 1024 or coord._image_mime(content) is None:
            raise CoordError("remote attachment must be a supported image no larger than 10 MiB")
        prepared.append((item["name"], coord._image_mime(content), content))
    sid = coord.require_session()
    with coord.tx() as db:
        recipients = coord._resolve_targets(db, [target])
        parent = coord._reply_parent(db, reply_to, sid, recipients[0]) if reply_to is not None else None
        mid = "message_" + secrets.token_urlsafe(24)
        db.execute("INSERT INTO messages(id,sender_session,recipient_session,body,created_at) VALUES(?,?,?,?,?)", (mid, sid, recipients[0], body, time.time()))
        if parent: db.execute("INSERT INTO message_replies(message_id,reply_to) VALUES(?,?)", (mid, reply_to))
        metadata = []
        for name, mime, content in prepared:
            aid = "attachment_" + hashlib.sha256((mid + name + str(len(metadata))).encode()).hexdigest()[:32]
            db.execute("INSERT INTO attachments(id,message_id,name,mime,size,content) VALUES(?,?,?,?,?,?)", (aid, mid, name, mime, len(content), content))
            metadata.append({"id": aid, "name": name, "mime": mime, "size": len(content), "url": "/api/attachments/" + aid})
    return {"id": mid, "sender_session": sid, "recipient_session": recipients[0], "attachments": metadata,
            "reply_to": reply_to, "reply_preview": (None if parent is None else dict(parent, sender_agent=None))}


def _schema(coord: Coordinator) -> None:
    # executescript implicitly commits SQLite transactions.  These individual
    # statements therefore retain the BEGIN IMMEDIATE lock established by tx().
    coord.db.execute("CREATE TABLE IF NOT EXISTS remote_session_hosts (session_id TEXT PRIMARY KEY, host_id TEXT NOT NULL, registered_at REAL NOT NULL, last_seen_at REAL NOT NULL)")
    coord.db.execute("CREATE TABLE IF NOT EXISTS remote_guarded_runs (run_id TEXT PRIMARY KEY, resource TEXT NOT NULL, reservation_id TEXT NOT NULL, session_id TEXT NOT NULL, host_id TEXT NOT NULL, local_pid INTEGER NOT NULL, started_at REAL NOT NULL, closed_at REAL, close_evidence_sha256 TEXT)")


def _bind_host(coord: Coordinator, session: str, host: str, op: str) -> None:
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


def _begin_guard(coord: Coordinator, session: str, host: str, p: dict) -> dict:
    resource, token = _string(p.get("resource"), "resource"), _string(p.get("token"), "token")
    with coord.tx() as db:
        coord._fresh(db, session)
        r = coord._verify_owner(db, coord.resource_name(resource), session, token)
        run_id = "remote_run_" + hashlib.sha256((host + session + str(time.time())).encode()).hexdigest()[:32]
        db.execute("INSERT INTO remote_guarded_runs VALUES(?,?,?,?,?,?,?,?,?)",
                   (run_id, r["name"], r["reservation_id"], session, host, 0, time.time(), None, None))
    return {"run_id": run_id, "reservation_id": r["reservation_id"], "host_id": host}


def _attach_guard_pid(coord: Coordinator, session: str, host: str, p: dict) -> dict:
    run_id, token, pid = _string(p.get("run_id"), "run_id"), _string(p.get("token"), "token"), p.get("pid")
    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0: raise CoordError("pid must be a positive integer")
    with coord.tx() as db:
        row = db.execute("SELECT * FROM remote_guarded_runs WHERE run_id=?", (run_id,)).fetchone()
        if not row or row["session_id"] != session or row["host_id"] != host or row["closed_at"] is not None or row["local_pid"] != 0: raise CoordError("remote guarded run cannot be attached")
        coord._verify_owner(db, row["resource"], session, token)
        db.execute("UPDATE remote_guarded_runs SET local_pid=? WHERE run_id=?", (pid, run_id))
    return {"run_id": run_id, "attached": True}


def _guard_pulse(coord: Coordinator, session: str, host: str, p: dict) -> dict:
    run_id, token = _string(p.get("run_id"), "run_id"), _string(p.get("token"), "token")
    row = coord.db.execute("SELECT * FROM remote_guarded_runs WHERE run_id=?", (run_id,)).fetchone()
    if not row or row["session_id"] != session or row["host_id"] != host or row["closed_at"] is not None:
        raise CoordError("remote guarded run is not open on this host")
    return coord.check(row["resource"], token)


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
    required = {"version", "resource", "reservation_id", "restored", "processes_closed", "closed_at", "evidence", "pids"}
    if set(receipt) != required or receipt.get("version") != 1 or receipt.get("resource") != resource or receipt.get("restored") is not True or receipt.get("processes_closed") is not True:
        raise CoordError("remote receipt does not bind a restored reservation")
    if not isinstance(receipt["closed_at"], (int, float)) or isinstance(receipt["closed_at"], bool) or not isinstance(receipt["pids"], list) or any(not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0 for pid in receipt["pids"]):
        raise CoordError("remote receipt has invalid closure data")
    with coord.tx() as db:
        coord._fresh(db, session)
        r = coord._row(db, resource)
        if not r or not r["owner_session"]:
            raise CoordError("resource is not held")
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
            if any(row["host_id"] != host or (row["local_pid"] != 0 and row["local_pid"] not in receipt["pids"]) or receipt["closed_at"] < row["started_at"] for row in open_rows):
                raise CoordError("remote open guarded run is not covered by this host receipt")
            db.executemany("UPDATE remote_guarded_runs SET closed_at=?,close_evidence_sha256=? WHERE run_id=?",
                           [(receipt["closed_at"], hashlib.sha256(evidence_bytes).hexdigest(), row["run_id"]) for row in open_rows])
        open_runs = db.execute("SELECT 1 FROM remote_guarded_runs WHERE reservation_id=? AND closed_at IS NULL", (r["reservation_id"],)).fetchone()
        if open_runs: raise CoordError("remote guarded run remains open")
        latest = db.execute("SELECT MAX(closed_at) value FROM remote_guarded_runs WHERE reservation_id=?", (r["reservation_id"],)).fetchone()["value"]
        if latest is not None and receipt["closed_at"] < latest: raise CoordError("remote receipt predates guarded run closure")
        canonical = json.dumps(receipt, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(canonical.encode()).hexdigest()
        if db.execute("SELECT 1 FROM receipts WHERE reservation_id=? OR receipt_sha256=?", (r["reservation_id"], digest)).fetchone():
            raise CoordError("receipt has already been used")
        db.execute("INSERT INTO receipts VALUES(?,?,?,?,?,?)", (r["reservation_id"], digest, hashlib.sha256(evidence_bytes).hexdigest(), canonical, action, time.time()))
        db.execute("UPDATE resources SET owner_session=NULL,reservation_id=NULL,token=NULL,granted_at=NULL,deadline=NULL,stale=0,reason=NULL WHERE name=?", (resource,))
    return {"resource": resource, "released" if action == "release" else "recovered": True}
