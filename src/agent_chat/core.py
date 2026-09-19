#!/usr/bin/env python3
"""Local, session-bound coordinator for serialized validation work.

All mutable state is in one SQLite database.  This module deliberately uses
only the Python standard library so tooling may use it before dependencies are
installed.
"""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import math
import os
import pathlib
import secrets
import signal
import sqlite3
import stat
import subprocess
import sys
import time
from typing import Any, Iterator

def _project_root() -> pathlib.Path:
    """Resolve the caller's project root, never this installed package."""
    return pathlib.Path(
        os.environ.get("AGENT_CHAT_ROOT")
        or os.environ.get("ITR_COORD_ROOT")
        or os.getcwd()
    ).expanduser().resolve()


ROOT = _project_root()
DEFAULT_DB = ROOT / ".agent-chat" / "state.sqlite3"


def default_db() -> pathlib.Path:
    """Return the current project's default database location."""
    return _project_root() / ".agent-chat" / "state.sqlite3"
MAX_ATTACHMENTS = 4
MAX_ATTACHMENT_SIZE = 10 * 1024 * 1024
IMAGE_SIGNATURES = (
    (b"\x89PNG\r\n\x1a\n", "image/png"),
    (b"GIF87a", "image/gif"),
    (b"GIF89a", "image/gif"),
)


class CoordError(RuntimeError):
    """A requested coordination operation cannot be performed."""


def _now() -> float: return time.time()
def _id(prefix: str) -> str: return prefix + "_" + secrets.token_urlsafe(24)


def agent_labels(db):
    labels = {}
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='retired_sessions'").fetchone():
        labels.update({row['id']: row['agent'] for row in db.execute('SELECT id,agent FROM retired_sessions')})
    labels.update({row['id']: row['agent'] for row in db.execute('SELECT id,agent FROM sessions')})
    return labels


class Coordinator:
    def __init__(self, db_path: str | os.PathLike[str] | None = None,
                 session: str | None = None):
        self.path = pathlib.Path(db_path or os.environ.get("AGENT_CHAT_DB") or os.environ.get("ITR_COORD_DB") or default_db())
        self.session = session or os.environ.get("AGENT_CHAT_SESSION") or os.environ.get("ITR_COORD_SESSION")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(self.path), timeout=10, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA busy_timeout=10000")
        self._schema()

    def close(self) -> None: self.db.close()

    def _schema(self) -> None:
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS retired_sessions (
          id TEXT PRIMARY KEY, agent TEXT NOT NULL, registered_at REAL NOT NULL,
          removed_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS sessions (
          id TEXT PRIMARY KEY, agent TEXT NOT NULL, registered_at REAL NOT NULL,
          inbox_read_seq INTEGER NOT NULL DEFAULT 0);
        CREATE TRIGGER IF NOT EXISTS reject_retired_session BEFORE INSERT ON sessions
          WHEN EXISTS(SELECT 1 FROM retired_sessions WHERE id=NEW.id)
          BEGIN SELECT RAISE(ABORT, 'session is deregistered; use a new identity'); END;
        CREATE TABLE IF NOT EXISTS messages (
          seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT UNIQUE NOT NULL,
          sender_session TEXT NOT NULL, recipient_session TEXT NOT NULL,
          body TEXT NOT NULL, created_at REAL NOT NULL, acked_at REAL,
          UNIQUE(id));
        CREATE TABLE IF NOT EXISTS attachments (
          id TEXT PRIMARY KEY, message_id TEXT NOT NULL, name TEXT NOT NULL,
          mime TEXT NOT NULL, size INTEGER NOT NULL, content BLOB NOT NULL,
          FOREIGN KEY(message_id) REFERENCES messages(id));
        CREATE TABLE IF NOT EXISTS message_replies (
          message_id TEXT PRIMARY KEY, reply_to TEXT NOT NULL,
          FOREIGN KEY(message_id) REFERENCES messages(id),
          FOREIGN KEY(reply_to) REFERENCES messages(id));
        CREATE TABLE IF NOT EXISTS message_batches (
          message_id TEXT PRIMARY KEY, batch_id TEXT NOT NULL,
          FOREIGN KEY(message_id) REFERENCES messages(id));
        CREATE INDEX IF NOT EXISTS message_batches_batch_id ON message_batches(batch_id);
        CREATE INDEX IF NOT EXISTS attachments_message_id ON attachments(message_id);
        CREATE TABLE IF NOT EXISTS resources (
          name TEXT PRIMARY KEY, owner_session TEXT, reservation_id TEXT UNIQUE,
          token TEXT, granted_at REAL, deadline REAL, stale INTEGER NOT NULL DEFAULT 0, reason TEXT);
        CREATE TABLE IF NOT EXISTS resource_queue (
          seq INTEGER PRIMARY KEY AUTOINCREMENT, resource TEXT NOT NULL,
          session TEXT NOT NULL, queued_at REAL NOT NULL, UNIQUE(resource, session));
        CREATE TABLE IF NOT EXISTS receipts (
          reservation_id TEXT PRIMARY KEY, receipt_sha256 TEXT UNIQUE NOT NULL,
          evidence_sha256 TEXT NOT NULL, receipt_json TEXT NOT NULL, action TEXT NOT NULL, recorded_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS guarded_runs (
          run_id TEXT PRIMARY KEY, resource TEXT NOT NULL, reservation_id TEXT NOT NULL,
          session TEXT NOT NULL, pid INTEGER NOT NULL, pgid INTEGER, started_at REAL NOT NULL,
          closed_at REAL);
        INSERT OR IGNORE INTO meta(key,value) VALUES('schema_version','1');
        """)
        version = self.db.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()[0]
        if version != "1": raise CoordError("unsupported coordinator schema version")

    @contextlib.contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        self.db.execute("BEGIN IMMEDIATE")
        observed_at = _now()
        try:
            self.db.execute(
                "UPDATE resources SET stale=1 WHERE owner_session IS NOT NULL AND deadline<?",
                (observed_at,),
            )
            yield self.db
        except BaseException:
            self.db.rollback()
            # A refused operation must not undo an already-observed expiry.
            self.db.execute(
                "UPDATE resources SET stale=1 WHERE owner_session IS NOT NULL AND deadline<?",
                (observed_at,),
            )
            raise
        else: self.db.commit()

    def require_session(self) -> str:
        if not self.session: raise CoordError("AGENT_CHAT_SESSION or --session is required")
        if not self.db.execute("SELECT 1 FROM sessions WHERE id=?", (self.session,)).fetchone():
            raise CoordError("unknown session; register first")
        return self.session

    def register(self, agent: str) -> dict[str, Any]:
        if not agent or not agent.strip(): raise CoordError("agent must be nonempty")
        sid = self.session or _id("session")
        with self.tx() as db:
            if db.execute("SELECT 1 FROM retired_sessions WHERE id=?", (sid,)).fetchone():
                raise CoordError("session is deregistered; register a new session instead of reusing its ID")
            old = db.execute("SELECT agent FROM sessions WHERE id=?", (sid,)).fetchone()
            if old and old["agent"] != agent: raise CoordError("session is already registered to another agent")
            if not old: db.execute("INSERT INTO sessions(id,agent,registered_at) VALUES(?,?,?)", (sid, agent, _now()))
        self.session = sid
        return {"session": sid, "agent": agent}

    def remove_session(self, session_id: str) -> dict[str, Any]:
        """Remove a coordination session (e.g. a finished subagent).

        Removes only a fully closed session. Messages and an identity tombstone
        remain, preserving attribution without keeping an active recipient.
        """
        caller = self.require_session()
        if not session_id:
            raise CoordError("session_id must be nonempty")
        with self.tx() as db:
            info = db.execute(
                "SELECT id,agent FROM sessions WHERE id=?", (session_id,)).fetchone()
            if info is None:
                raise CoordError(f"unknown session: {session_id}")
            agent_name = info["agent"]

            # This is deliberately an authenticated administrative operation,
            # never a way for an anonymous client to discard a dead identity.
            # The browser's durable operator identity is part of the audit trail.
            sidecar = pathlib.Path(str(self.path) + ".web-session.json")
            try:
                operator_id = json.loads(sidecar.read_text(encoding="utf-8")).get("id")
            except (OSError, ValueError, AttributeError):
                operator_id = None
            if session_id == operator_id:
                raise CoordError("the web operator session cannot be removed")

            # Stale ownership remains ownership until a receipt-backed release or
            # recovery.  Clearing it here used to let a later request jump an
            # unresolved hold and lose the owner label/audit token.
            held = db.execute(
                "SELECT name FROM resources "
                "WHERE owner_session=? AND owner_session IS NOT NULL", (session_id,)).fetchall()
            if held:
                names = ", ".join(r["name"] for r in held)
                raise CoordError(
                    f"session {session_id} ({agent_name}) holds active resource(s): "
                    f"{names}; release or recover them with a receipt first")

            runs = db.execute("SELECT resource FROM guarded_runs WHERE session=? AND closed_at IS NULL", (session_id,)).fetchall()
            if runs:
                raise CoordError("session has open guarded run(s): " + ", ".join(r["resource"] for r in runs))
            if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='remote_guarded_runs'").fetchone():
                remote_runs = db.execute("SELECT resource FROM remote_guarded_runs WHERE session_id=? AND closed_at IS NULL", (session_id,)).fetchall()
                if remote_runs: raise CoordError("session has open remote guarded run(s): " + ", ".join(r["resource"] for r in remote_runs))

            # Clear resource queue entries.
            db.execute("DELETE FROM resource_queue WHERE session=?", (session_id,))

            # Remove bridge bindings (may not exist if bridge never started).
            if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='bridge_bindings'").fetchone():
                descendants = db.execute("SELECT session_id FROM bridge_bindings WHERE parent_session=?", (session_id,)).fetchall()
                if descendants:
                    raise CoordError("session has bound bridge descendant(s): " + ", ".join(r["session_id"] for r in descendants))
                binding = db.execute("SELECT * FROM bridge_bindings WHERE session_id=?", (session_id,)).fetchone()
                route = binding
                seen = set()
                while route and not route['thread_id'] and route['parent_session'] not in seen:
                    seen.add(route['parent_session'])
                    route = db.execute('SELECT * FROM bridge_bindings WHERE session_id=?', (route['parent_session'],)).fetchone()
                thread = route['thread_id'] if route else None
                pending = False
                if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='bridge_jobs'").fetchone() and db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='bridge_deliveries'").fetchone():
                    pending = db.execute(
                        """SELECT 1 FROM bridge_jobs j JOIN bridge_deliveries d ON d.job_id=j.id
                           JOIN messages m ON m.id=d.message_id
                           WHERE j.status NOT IN ('dispatched','cancelled')
                           AND (m.sender_session=? OR m.recipient_session=?) LIMIT 1""",
                        (session_id, session_id)).fetchone()
                route_pending = thread and db.execute("SELECT 1 FROM bridge_jobs WHERE thread_id=? AND status NOT IN ('dispatched','cancelled') LIMIT 1", (thread,)).fetchone()
                if pending or route_pending:
                    raise CoordError("session has an unresolved bridge job; resolve it before deregistering")
                db.execute("DELETE FROM bridge_bindings WHERE session_id=?", (session_id,))

            db.execute("INSERT INTO retired_sessions(id,agent,registered_at,removed_at) SELECT id,agent,registered_at,? FROM sessions WHERE id=?", (_now(), session_id))
            # Active recipients disappear; names remain available for history.
            db.execute("DELETE FROM sessions WHERE id=?", (session_id,))

        return {"removed": session_id, "agent": agent_name}

    def _fresh(self, db: sqlite3.Connection, session: str) -> None:
        row = db.execute("SELECT inbox_read_seq FROM sessions WHERE id=?", (session,)).fetchone()
        latest = db.execute("SELECT COALESCE(MAX(seq),0) n FROM messages WHERE recipient_session=?", (session,)).fetchone()["n"]
        if row is None: raise CoordError("unknown session")
        if row["inbox_read_seq"] < latest: raise CoordError("read your inbox after the newest message before changing ownership")

    def _has_attachments(self, db: sqlite3.Connection) -> bool:
        return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='attachments'").fetchone())

    def _has_replies(self, db: sqlite3.Connection) -> bool:
        return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='message_replies'").fetchone())

    def _has_batches(self, db: sqlite3.Connection) -> bool:
        return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='message_batches'").fetchone())

    def _attachment_metadata(self, db: sqlite3.Connection, message_ids: list[str]) -> dict[str, list[dict[str, Any]]]:
        result = {message_id: [] for message_id in message_ids}
        if not message_ids or not self._has_attachments(db):
            return result
        marks = ",".join("?" for _ in message_ids)
        for row in db.execute(f"SELECT id,message_id,name,mime,size FROM attachments WHERE message_id IN ({marks}) ORDER BY rowid", message_ids):
            result[row["message_id"]].append({"id": row["id"], "name": row["name"], "mime": row["mime"], "size": row["size"], "url": "/api/attachments/" + row["id"]})
        return result

    def _message_dicts(self, db: sqlite3.Connection, rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
        labels = agent_labels(db)
        attachments = self._attachment_metadata(db, [row["id"] for row in rows])
        replies = self._reply_metadata(db, [row["id"] for row in rows])
        batches = self._batch_metadata(db, [row["id"] for row in rows])
        return [{**{key: row[key] for key in ("id", "sender_session", "body", "created_at", "acked_at")},
                 "sender_agent": labels.get(row["sender_session"]),
                 "attachments": attachments[row["id"]], **replies[row["id"]], **batches[row["id"]]} for row in rows]

    def _batch_metadata(self, db: sqlite3.Connection, message_ids: list[str]) -> dict[str, dict[str, Any]]:
        result = {message_id: {"batch_id": None, "deliveries": []} for message_id in message_ids}
        if not message_ids or not self._has_batches(db):
            return result
        marks = ",".join("?" for _ in message_ids)
        memberships = db.execute(
            f"SELECT message_id,batch_id FROM message_batches WHERE message_id IN ({marks})", message_ids).fetchall()
        batch_ids = sorted({row["batch_id"] for row in memberships})
        if not batch_ids:
            return result
        batch_marks = ",".join("?" for _ in batch_ids)
        deliveries: dict[str, list[dict[str, Any]]] = {batch_id: [] for batch_id in batch_ids}
        labels = agent_labels(db)
        for row in db.execute(
            f"""SELECT b.batch_id,m.id,m.recipient_session,m.acked_at
                 FROM message_batches b JOIN messages m ON m.id=b.message_id
                 WHERE b.batch_id IN ({batch_marks}) ORDER BY m.seq""", batch_ids):
            deliveries[row["batch_id"]].append({"id": row["id"], "recipient_session": row["recipient_session"],
                                                 "recipient_agent": labels.get(row["recipient_session"]), "acked_at": row["acked_at"]})
        for row in memberships:
            result[row["message_id"]] = {"batch_id": row["batch_id"], "deliveries": deliveries[row["batch_id"]]}
        return result

    def _reply_metadata(self, db: sqlite3.Connection, message_ids: list[str]) -> dict[str, dict[str, Any]]:
        result = {message_id: {"reply_to": None, "reply_preview": None} for message_id in message_ids}
        if not message_ids or not self._has_replies(db):
            return result
        marks = ",".join("?" for _ in message_ids)
        query = f"""SELECT r.message_id, r.reply_to, p.seq, p.sender_session, p.recipient_session, p.body
                     FROM message_replies r JOIN messages p ON p.id=r.reply_to
                     WHERE r.message_id IN ({marks})"""
        labels = agent_labels(db)
        for row in db.execute(query, message_ids):
            result[row["message_id"]] = {
                "reply_to": row["reply_to"],
                "reply_preview": {"id": row["reply_to"], "seq": row["seq"],
                                  "sender_session": row["sender_session"],
                                  "recipient_session": row["recipient_session"],
                                  "sender_agent": labels.get(row["sender_session"]), "body": row["body"][:240]},
            }
        return result

    def inbox(self, all_messages: bool = False) -> dict[str, Any]:
        sid = self.require_session()
        with self.tx() as db:
            rows = db.execute("SELECT id,sender_session,body,created_at,acked_at,seq FROM messages WHERE recipient_session=? " + ("ORDER BY seq" if all_messages else "AND acked_at IS NULL ORDER BY seq"), (sid,)).fetchall()
            latest = db.execute("SELECT COALESCE(MAX(seq),0) n FROM messages WHERE recipient_session=?", (sid,)).fetchone()["n"]
            db.execute("UPDATE sessions SET inbox_read_seq=? WHERE id=?", (latest, sid))
            messages = self._message_dicts(db, rows)
        return {"messages": messages}

    @staticmethod
    def _image_mime(content: bytes) -> str | None:
        for signature, mime in IMAGE_SIGNATURES:
            if content.startswith(signature):
                return mime
        if content.startswith(b"\xff\xd8\xff"):
            return "image/jpeg"
        if content.startswith(b"RIFF") and len(content) >= 12 and content[8:12] == b"WEBP":
            return "image/webp"
        return None

    def _prepare_attachments(self, paths: list[str | os.PathLike[str]] | None) -> list[tuple[str, str, bytes]]:
        paths = paths or []
        if len(paths) > MAX_ATTACHMENTS:
            raise CoordError(f"at most {MAX_ATTACHMENTS} image attachments are allowed")
        prepared = []
        for raw in paths:
            path = pathlib.Path(raw)
            try:
                fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
            except OSError as error:
                raise CoordError("attachment is unreadable: " + str(error)) from error
            try:
                with os.fdopen(fd, "rb") as stream:
                    if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                        raise CoordError("attachment must be a regular file")
                    content = stream.read(MAX_ATTACHMENT_SIZE + 1)
            except OSError as error:
                raise CoordError("attachment is unreadable: " + str(error)) from error
            if len(content) > MAX_ATTACHMENT_SIZE:
                raise CoordError("attachment exceeds 10 MiB")
            mime = self._image_mime(content)
            if mime is None:
                raise CoordError("attachment is not a supported PNG, JPEG, GIF, or WebP image")
            prepared.append((path.name, mime, content))
        return prepared

    @staticmethod
    def _reply_parent(db: sqlite3.Connection, reply_to: str, sender: str, recipient: str) -> sqlite3.Row:
        parent = db.execute("SELECT id,seq,sender_session,recipient_session,body FROM messages WHERE id=?", (reply_to,)).fetchone()
        if not parent or {parent["sender_session"], parent["recipient_session"]} != {sender, recipient}:
            raise CoordError("reply parent must belong to the same conversation")
        return parent

    def send(self, target: str, body: str, attachments: list[str | os.PathLike[str]] | None = None,
             reply_to: str | None = None) -> dict[str, Any]:
        sid = self.require_session()
        if not target: raise CoordError("recipient is required")
        prepared = self._prepare_attachments(attachments)
        with self.tx() as db:
            exact = db.execute("SELECT id FROM sessions WHERE id=?", (target,)).fetchone()
            if exact: recipients = [exact["id"]]
            else:
                recipients = [r["id"] for r in db.execute("SELECT id FROM sessions WHERE agent=?", (target,))]
                if len(recipients) != 1:
                    raise CoordError("recipient label is unknown or ambiguous; use an exact session id")
            mid = _id("message")
            parent = self._reply_parent(db, reply_to, sid, recipients[0]) if reply_to is not None else None
            db.execute("INSERT INTO messages(id,sender_session,recipient_session,body,created_at) VALUES(?,?,?,?,?)", (mid,sid,recipients[0],body,_now()))
            if parent is not None:
                db.execute("INSERT INTO message_replies(message_id,reply_to) VALUES(?,?)", (mid,reply_to))
            metadata = []
            for name, mime, content in prepared:
                attachment_id = _id("attachment")
                db.execute("INSERT INTO attachments(id,message_id,name,mime,size,content) VALUES(?,?,?,?,?,?)", (attachment_id, mid, name, mime, len(content), content))
                metadata.append({"id": attachment_id, "name": name, "mime": mime, "size": len(content), "url": "/api/attachments/" + attachment_id})
        preview = None if parent is None else {"id": parent["id"], "seq": parent["seq"],
                                               "sender_session": parent["sender_session"],
                                               "recipient_session": parent["recipient_session"],
                                               "sender_agent": None, "body": parent["body"][:240]}
        return {"id":mid,"sender_session":sid,"recipient_session":recipients[0],"attachments":metadata,
                "reply_to": reply_to, "reply_preview": preview}

    def _resolve_targets(self, db: sqlite3.Connection, targets: list[str]) -> list[str]:
        if not targets:
            raise CoordError("at least one recipient is required")
        recipients = []
        for target in targets:
            if not isinstance(target, str) or not target:
                raise CoordError("recipient is required")
            exact = db.execute("SELECT id FROM sessions WHERE id=?", (target,)).fetchone()
            if exact:
                recipient = exact["id"]
            else:
                matches = db.execute("SELECT id FROM sessions WHERE agent=?", (target,)).fetchall()
                if len(matches) != 1:
                    raise CoordError("recipient label is unknown or ambiguous; use an exact session id")
                recipient = matches[0]["id"]
            if recipient not in recipients:
                recipients.append(recipient)
        return recipients

    def send_many(self, targets: list[str], body: str, reply_to: str | None = None) -> list[dict[str, Any]]:
        """Atomically create one delivery per distinct resolved recipient."""
        sid = self.require_session()
        if not isinstance(targets, list):
            raise CoordError("recipients must be a list")
        with self.tx() as db:
            recipients = self._resolve_targets(db, targets)
            parents: dict[str, sqlite3.Row | None] = {recipient: None for recipient in recipients}
            if reply_to is not None:
                parent = db.execute("SELECT id,sender_session FROM messages WHERE id=?", (reply_to,)).fetchone()
                membership = (db.execute("SELECT batch_id FROM message_batches WHERE message_id=?", (reply_to,)).fetchone()
                              if self._has_batches(db) else None)
                if membership and parent and parent["sender_session"] == sid:
                    copies = db.execute(
                        """SELECT m.id,m.seq,m.sender_session,m.recipient_session,m.body
                             FROM message_batches b JOIN messages m ON m.id=b.message_id
                             WHERE b.batch_id=?""", (membership["batch_id"],)).fetchall()
                    by_recipient = {copy["recipient_session"]: copy for copy in copies}
                    if any(recipient not in by_recipient for recipient in recipients):
                        raise CoordError("reply recipients must be recipients of the parent batch")
                    parents = {recipient: by_recipient[recipient] for recipient in recipients}
                else:
                    parents = {recipient: self._reply_parent(db, reply_to, sid, recipient) for recipient in recipients}
            batch_id = _id("batch")
            created_at = _now()
            results = []
            for recipient in recipients:
                message_id = _id("message")
                db.execute("INSERT INTO messages(id,sender_session,recipient_session,body,created_at) VALUES(?,?,?,?,?)",
                           (message_id, sid, recipient, body, created_at))
                db.execute("INSERT INTO message_batches(message_id,batch_id) VALUES(?,?)", (message_id, batch_id))
                parent = parents[recipient]
                if parent is not None:
                    db.execute("INSERT INTO message_replies(message_id,reply_to) VALUES(?,?)", (message_id, parent["id"]))
                results.append({"id": message_id, "sender_session": sid, "recipient_session": recipient,
                                "attachments": [], "reply_to": None if parent is None else parent["id"],
                                "reply_preview": None if parent is None else {"id": parent["id"], "seq": parent["seq"],
                                    "sender_session": parent["sender_session"], "recipient_session": parent["recipient_session"],
                                    "sender_agent": None, "body": parent["body"][:240]}})
            deliveries = [{"id": result["id"], "recipient_session": result["recipient_session"],
                           "recipient_agent": db.execute("SELECT agent FROM sessions WHERE id=?", (result["recipient_session"],)).fetchone()["agent"],
                           "acked_at": None} for result in results]
            for result in results:
                result.update(batch_id=batch_id, deliveries=deliveries)
        return results

    def link_reply(self, message_id: str, reply_to: str) -> dict[str, Any]:
        sid = self.require_session()
        with self.tx() as db:
            message = db.execute("SELECT id,seq,sender_session,recipient_session FROM messages WHERE id=?", (message_id,)).fetchone()
            if not message or message["sender_session"] != sid:
                raise CoordError("message is not sent by caller")
            parent = self._reply_parent(db, reply_to, sid, message["recipient_session"])
            if parent["seq"] >= message["seq"]:
                raise CoordError("reply parent must be older than the message")
            old = db.execute("SELECT reply_to FROM message_replies WHERE message_id=?", (message_id,)).fetchone()
            if old and old["reply_to"] != reply_to:
                raise CoordError("message already has a different reply parent")
            if not old:
                db.execute("INSERT INTO message_replies(message_id,reply_to) VALUES(?,?)", (message_id, reply_to))
        return {"id": message_id, "reply_to": reply_to, "linked": True}

    def acknowledge(self, message_id: str) -> dict[str, Any]:
        sid = self.require_session()
        with self.tx() as db:
            r = db.execute("SELECT recipient_session FROM messages WHERE id=?", (message_id,)).fetchone()
            if not r or r["recipient_session"] != sid: raise CoordError("message is not in caller inbox")
            db.execute("UPDATE messages SET acked_at=COALESCE(acked_at,?) WHERE id=?", (_now(),message_id))
        return {"id":message_id,"acknowledged":True}

    def resource_name(self, raw: str) -> str:
        if not raw or raw != raw.strip(): raise CoordError("resource name is invalid")
        if raw.startswith("file:"):
            part = raw[5:]
            if not part or pathlib.Path(part).is_absolute(): raise CoordError("file resource must be repo-relative")
            path = (ROOT / part).resolve()
            try: rel = path.relative_to(ROOT)
            except ValueError: raise CoordError("file resource escapes repository")
            return "file:" + rel.as_posix()
        if any(c.isspace() for c in raw) or ".." in raw.split("/"): raise CoordError("resource name is invalid")
        return raw

    def _row(self, db: sqlite3.Connection, resource: str) -> sqlite3.Row | None:
        r = db.execute("SELECT * FROM resources WHERE name=?", (resource,)).fetchone()
        if r and r["owner_session"] and not r["stale"] and r["deadline"] < _now():
            db.execute("UPDATE resources SET stale=1 WHERE name=?",(resource,))
            r=db.execute("SELECT * FROM resources WHERE name=?",(resource,)).fetchone()
        return r

    @staticmethod
    def _state(r: sqlite3.Row | None) -> str:
        if not r or not r["owner_session"]: return "free"
        return "stale" if r["stale"] or r["deadline"] < _now() else "owned"

    def _acquire(self, db: sqlite3.Connection, resource: str, sid: str, minutes: float, reason: str | None = None) -> sqlite3.Row:
        rid, token, now = _id("reservation"), _id("token"), _now()
        db.execute("INSERT INTO resources(name,owner_session,reservation_id,token,granted_at,deadline,stale,reason) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(name) DO UPDATE SET owner_session=excluded.owner_session,reservation_id=excluded.reservation_id,token=excluded.token,granted_at=excluded.granted_at,deadline=excluded.deadline,stale=0,reason=excluded.reason", (resource,sid,rid,token,now,now+minutes*60,0,reason))
        return db.execute("SELECT * FROM resources WHERE name=?", (resource,)).fetchone()

    def request(self, raw: str, minutes: float) -> dict[str, Any]:
        if not math.isfinite(minutes) or minutes <= 0 or minutes > float.fromhex('0x1.fffffffffffffp+1023') / 60: raise CoordError("minutes must be positive and finite")
        sid, resource = self.require_session(), self.resource_name(raw)
        with self.tx() as db:
            self._fresh(db,sid); r = self._row(db,resource)
            if r and r["owner_session"] == sid:
                st=self._state(r)
                if st == "stale": raise CoordError("your reservation is stale; release or recover it with a receipt")
                return self._reservation_result(r, "owned", 0)
            first = db.execute("SELECT session FROM resource_queue WHERE resource=? ORDER BY seq LIMIT 1", (resource,)).fetchone()
            if (not r or not r["owner_session"]) and (not first or first["session"] == sid):
                db.execute("DELETE FROM resource_queue WHERE resource=? AND session=?", (resource,sid))
                return self._reservation_result(self._acquire(db,resource,sid,minutes),"owned",0)
            db.execute("INSERT OR IGNORE INTO resource_queue(resource,session,queued_at) VALUES(?,?,?)", (resource,sid,_now()))
            pos=db.execute("SELECT COUNT(*) n FROM resource_queue WHERE resource=? AND seq <= (SELECT seq FROM resource_queue WHERE resource=? AND session=?)", (resource,resource,sid)).fetchone()["n"]
            return {"state":"queued","resource":resource,"reservation_id":None,"token":None,"deadline":None,"queue_position":pos}

    def _reservation_result(self,r: sqlite3.Row,state:str,pos:int)->dict[str,Any]:
        return {"state":state,"resource":r["name"],"reservation_id":r["reservation_id"],"token":r["token"],"deadline":r["deadline"],"queue_position":pos}

    def status(self) -> dict[str, Any]:
        self.require_session()
        with self.tx() as db:
          rows=db.execute("SELECT * FROM resources ORDER BY name").fetchall(); out=[]
          for r in rows:
            owner=None
            if r["owner_session"]:
                s=db.execute("SELECT agent FROM sessions WHERE id=?",(r["owner_session"],)).fetchone()
                owner=s["agent"] if s else None
            q=[dict(session=x["session"],agent=x["agent"],position=i+1) for i,x in enumerate(db.execute("SELECT q.session,s.agent FROM resource_queue q JOIN sessions s ON s.id=q.session WHERE q.resource=? ORDER BY q.seq",(r["name"],)).fetchall())]
            out.append({"resource":r["name"],"owner_session":r["owner_session"],"owner_agent":owner,"state":self._state(r),"reservation_id":r["reservation_id"],"deadline":r["deadline"],"reason":r["reason"],"queue":q})
          sessions=[dict(session=x["id"],agent=x["agent"],registered_at=x["registered_at"]) for x in db.execute("SELECT id,agent,registered_at FROM sessions ORDER BY registered_at,id")]
        return {"resources":out,"sessions":sessions}

    def cancel(self, raw:str)->dict[str,Any]:
        sid,resource=self.require_session(),self.resource_name(raw)
        with self.tx() as db:
            self._fresh(db,sid); n=db.execute("DELETE FROM resource_queue WHERE resource=? AND session=?",(resource,sid)).rowcount
        return {"resource":resource,"cancelled":bool(n)}

    def _verify_owner(self,db:sqlite3.Connection,resource:str,sid:str,token:str, allow_stale:bool=False)->sqlite3.Row:
        r=self._row(db,resource)
        if not r or r["owner_session"]!=sid or not secrets.compare_digest(r["token"] or "",token or ""): raise CoordError("caller does not hold this reservation token")
        if not allow_stale and self._state(r)!="owned": raise CoordError("reservation is stale")
        return r

    def check(self,raw:str,token:str)->dict[str,Any]:
        sid,resource=self.require_session(),self.resource_name(raw)
        with self.tx() as db:
            self._fresh(db,sid);r=self._verify_owner(db,resource,sid,token)
        return {"state":"owned","resource":resource,"reservation_id":r["reservation_id"],"deadline":r["deadline"]}

    def _validate_receipt(self,path:str,r:sqlite3.Row)->tuple[str,str,str]:
        try: data=json.loads(pathlib.Path(path).read_text(encoding="utf-8"))
        except Exception as e: raise CoordError("receipt is unreadable JSON: "+str(e))
        needed={"version","resource","reservation_id","restored","processes_closed","closed_at","evidence","pids"}
        if not isinstance(data,dict) or set(data)!=needed: raise CoordError("receipt must contain exactly the required fields")
        if data["version"]!=1 or data["resource"]!=r["name"] or data["reservation_id"]!=r["reservation_id"] or data["restored"] is not True or data["processes_closed"] is not True: raise CoordError("receipt does not bind this restored reservation")
        if not isinstance(data["closed_at"],(int,float)) or isinstance(data["closed_at"],bool) or not r["granted_at"]<=data["closed_at"]<=_now(): raise CoordError("receipt closed_at is invalid")
        if not isinstance(data["evidence"],str) or not data["evidence"].strip() or not isinstance(data["pids"],list) or any(not isinstance(p,int) or isinstance(p,bool) or p<=0 for p in data["pids"]): raise CoordError("receipt evidence or pids is invalid")
        evidence = pathlib.Path(data["evidence"])
        if not evidence.is_absolute(): evidence = pathlib.Path(path).resolve().parent / evidence
        try:
            evidence_bytes = evidence.read_bytes()
        except OSError as e: raise CoordError("receipt evidence report is unavailable: " + str(e))
        if not evidence_bytes: raise CoordError("receipt evidence report is empty")
        for pid in data["pids"]:
            try: os.kill(pid,0)
            except ProcessLookupError: continue
            except PermissionError: raise CoordError("receipt lists a still-live PID")
            raise CoordError("receipt lists a still-live PID")
        canonical=json.dumps(data,sort_keys=True,separators=(",",":")); return canonical,hashlib.sha256(canonical.encode()).hexdigest(),hashlib.sha256(evidence_bytes).hexdigest()

    @staticmethod
    def _alive(pid:int, group:bool=False)->bool:
        try:
            (os.killpg if group else os.kill)(pid,0); return True
        except ProcessLookupError: return False
        except PermissionError: return True

    def _runs_closed(self,db:sqlite3.Connection,resource:str,allow_dead:bool,attested_closed_at:float)->None:
        runs=db.execute("SELECT run_id,pid,pgid,started_at FROM guarded_runs WHERE resource=? AND closed_at IS NULL",(resource,)).fetchall()
        live=[r for r in runs if self._alive(r["pid"]) or (r["pgid"] is not None and self._alive(r["pgid"],True))]
        if live or (runs and not allow_dead): raise CoordError("guarded run remains open; it must close before release or recovery")
        if any(attested_closed_at < r["started_at"] for r in runs): raise CoordError("receipt predates an unclosed guarded run")
        if runs: db.executemany("UPDATE guarded_runs SET closed_at=? WHERE run_id=?",[(attested_closed_at,r["run_id"]) for r in runs])

    def _clear_with_receipt(self,db:sqlite3.Connection,r:sqlite3.Row,path:str,action:str)->None:
        if self._remote_bound(db, r["owner_session"]):
            raise CoordError("remote reservation requires receipt proof from its owning host via --server")
        canonical,digest,evidence_digest=self._validate_receipt(path,r)
        attested=json.loads(canonical)["closed_at"]
        latest=db.execute("SELECT MAX(closed_at) x FROM guarded_runs WHERE reservation_id=?",(r["reservation_id"],)).fetchone()["x"]
        if latest is not None and attested < latest: raise CoordError("receipt predates the most recent guarded run closure")
        self._runs_closed(db,r["name"],self._state(r)=="stale",attested)
        if db.execute("SELECT 1 FROM receipts WHERE reservation_id=? OR receipt_sha256=?",(r["reservation_id"],digest)).fetchone(): raise CoordError("receipt has already been used")
        db.execute("INSERT INTO receipts(reservation_id,receipt_sha256,evidence_sha256,receipt_json,action,recorded_at) VALUES(?,?,?,?,?,?)",(r["reservation_id"],digest,evidence_digest,canonical,action,_now()))
        db.execute("UPDATE resources SET owner_session=NULL,reservation_id=NULL,token=NULL,granted_at=NULL,deadline=NULL,stale=0,reason=NULL WHERE name=?",(r["name"],))

    def release(self,raw:str,token:str,path:str)->dict[str,Any]:
        sid,resource=self.require_session(),self.resource_name(raw)
        with self.tx() as db:
            if self._remote_bound(db, sid): raise CoordError("remote-bound reservation must be released with agent-chat --server")
            self._fresh(db,sid);r=self._verify_owner(db,resource,sid,token,True);self._clear_with_receipt(db,r,path,"release")
        return {"resource":resource,"released":True}

    def recover(self,raw:str,path:str)->dict[str,Any]:
        sid,resource=self.require_session(),self.resource_name(raw)
        with self.tx() as db:
            if self._remote_bound(db, sid): raise CoordError("remote-bound reservation must be recovered with agent-chat --server")
            self._fresh(db,sid);r=self._row(db,resource)
            if not r or self._state(r)!="stale": raise CoordError("only a stale reservation can be recovered")
            self._clear_with_receipt(db,r,path,"recover")
        return {"resource":resource,"recovered":True}

    def block(self,raw:str,reason:str)->dict[str,Any]:
        sid,resource=self.require_session(),self.resource_name(raw)
        if not reason.strip(): raise CoordError("reason must be nonempty")
        with self.tx() as db:
            self._fresh(db,sid);r=self._row(db,resource)
            if r and r["owner_session"]: raise CoordError("only an unowned resource can be blocked")
            r=self._acquire(db,resource,sid,0.000001,reason)
            db.execute("UPDATE resources SET deadline=?,stale=1 WHERE name=?",(_now()-1,resource))
            r=db.execute("SELECT * FROM resources WHERE name=?",(resource,)).fetchone()
        return self._reservation_result(r,"stale",0)

    def begin_guard(self,resource:str,token:str,pid:int)->tuple[str,sqlite3.Row]:
        sid=self.require_session(); resource=self.resource_name(resource)
        with self.tx() as db:
            if self._remote_bound(db, sid): raise CoordError("remote-bound reservation must use agent-chat --server run")
            # Reading inbox here is deliberate and satisfies the ownership mutation guard.
            self._fresh(db,sid);r=self._verify_owner(db,resource,sid,token)
            run=_id("run"); db.execute("INSERT INTO guarded_runs(run_id,resource,reservation_id,session,pid,started_at) VALUES(?,?,?,?,?,?)",(run,resource,r["reservation_id"],sid,pid,_now()))
        return run,r

    @staticmethod
    def _remote_bound(db: sqlite3.Connection, session: str) -> bool:
        return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='remote_session_hosts'").fetchone() and db.execute("SELECT 1 FROM remote_session_hosts WHERE session_id=?", (session,)).fetchone())

    def attach_group(self,run:str,pgid:int)->None:
        with self.tx() as db: db.execute("UPDATE guarded_runs SET pgid=? WHERE run_id=? AND closed_at IS NULL",(pgid,run))

    def close_guard(self,run:str)->None:
        with self.tx() as db: db.execute("UPDATE guarded_runs SET closed_at=? WHERE run_id=? AND closed_at IS NULL",(_now(),run))


class ValidationGuard:
    """Guard an active reservation and make release wait for normal cleanup."""
    def __init__(self,resource:str="validation-clone", token:str|None=None, db_path: str|None=None, session:str|None=None):
        if os.environ.get("AGENT_CHAT_SERVER") or os.environ.get("ITR_COORD_SERVER"):
            raise CoordError("ValidationGuard cannot use remote coordination; use agent-chat --server run")
        if db_path is None and os.environ.get('AGENT_CHAT_PROJECT'):
            from .projects import resolve_database
            db_path = str(resolve_database(os.environ.get('AGENT_CHAT_DB') or os.environ.get('ITR_COORD_DB') or default_db(), os.environ['AGENT_CHAT_PROJECT']))
        self.coord=Coordinator(db_path,session);self.resource=resource;self.token=token or os.environ.get("AGENT_CHAT_TOKEN") or os.environ.get("ITR_COORD_TOKEN","");self.run_id: str|None=None;self.seen:set[str]=set();self.safe_to_close=True
    def _emit_inbox(self)->None:
        messages=self.coord.inbox()["messages"]; new=[m for m in messages if m["id"] not in self.seen]
        self.seen.update(m["id"] for m in messages)
        if new: print(json.dumps({"messages":new}),file=sys.stderr,flush=True)
    def __enter__(self)->"ValidationGuard":
        try:
            self._emit_inbox()
            self.run_id, _ = self.coord.begin_guard(self.resource,self.token,os.getpid())
            return self
        except BaseException:
            self.coord.close()
            raise
    def pulse(self)->None:
        self._emit_inbox();self.coord.check(self.resource,self.token)
    def __exit__(self,typ:Any,val:Any,tb:Any)->bool:
        if self.run_id and self.safe_to_close: self.coord.close_guard(self.run_id)
        self.coord.close();return False


def validation_guard(resource:str="validation-clone") -> ValidationGuard:
    """Return a session/token-bound guard for use with ``with`` blocks."""
    return ValidationGuard(resource)


def _json_ok(value:dict[str,Any])->int: print(json.dumps(value,sort_keys=True));return 0
def _token(a:argparse.Namespace)->str:return a.token or os.environ.get("AGENT_CHAT_TOKEN") or os.environ.get("ITR_COORD_TOKEN","")

def run_command(c: Coordinator, a: argparse.Namespace) -> dict[str, Any]:
    command = list(a.command)
    if not command:
        raise CoordError("run requires a command after --")
    guard = ValidationGuard(a.resource, _token(a), str(c.path), c.session)
    with guard:
        resolved_db = str(c.path.resolve())
        resolved_session = c.session or ""
        resolved_token = _token(a)
        resolved_root = str(ROOT)
        env = dict(
            os.environ,
            AGENT_CHAT_DB=resolved_db,
            AGENT_CHAT_PROJECT=(c.db.execute("SELECT value FROM meta WHERE key='project_id'").fetchone() or ['default'])[0],
            AGENT_CHAT_SESSION=resolved_session,
            AGENT_CHAT_TOKEN=resolved_token,
            AGENT_CHAT_ROOT=resolved_root,
            ITR_COORD_DB=resolved_db,
            ITR_COORD_SESSION=resolved_session,
            ITR_COORD_TOKEN=resolved_token,
            ITR_COORD_ROOT=resolved_root,
        )
        proc = None
        interrupted = False
        previous_handlers = {}

        def forward(sig: int, frame: Any = None) -> None:
            nonlocal interrupted
            interrupted = True
            if proc is not None:
                try:
                    os.killpg(proc.pid, sig)
                except (ProcessLookupError, PermissionError):
                    # Darwin can report EPERM while a signalled group is
                    # disappearing. Cleanup below must still prove its absence;
                    # an inaccessible live group keeps the reservation open.
                    pass

        try:
            for sig in (signal.SIGINT, signal.SIGTERM):
                previous_handlers[sig] = signal.signal(sig, forward)
            if interrupted:
                raise CoordError("command interrupted before launch")
            proc = subprocess.Popen(command, start_new_session=True, env=env)
            guard.safe_to_close = False
            c.attach_group(guard.run_id or "", proc.pid)
            last_poll = 0.0
            while proc.poll() is None:
                if interrupted:
                    raise CoordError("command interrupted; closing owned process group")
                if time.monotonic() - last_poll >= 5:
                    guard.pulse()
                    last_poll = time.monotonic()
                time.sleep(.1)
            code = proc.wait()
            if interrupted:
                raise CoordError("command interrupted; owned process group closed")
        finally:
            try:
                if proc is not None:
                    # Always reap the leader while checking descendants. A zombie
                    # leader otherwise makes a closed group look permanently live.
                    for sig in (signal.SIGTERM, signal.SIGKILL):
                        proc.poll()
                        if not Coordinator._alive(proc.pid, True):
                            break
                        forward(sig)
                        deadline = time.monotonic() + 5
                        while time.monotonic() < deadline:
                            proc.poll()
                            if not Coordinator._alive(proc.pid, True):
                                break
                            time.sleep(.05)
                    proc.poll()
                    if Coordinator._alive(proc.pid, True):
                        raise CoordError("could not prove command group was closed; reservation retained")
                    proc.wait(timeout=1)
                guard.safe_to_close = True
            finally:
                for sig, handler in previous_handlers.items():
                    signal.signal(sig, handler)
        return {"resource": c.resource_name(a.resource),
                "exit_code": code if code >= 0 else 128 - code}


def main(argv:list[str]|None=None)->int:
    raw=list(sys.argv[1:] if argv is None else argv)
    run_tail: list[str] | None = None
    if "run" in raw:
        run_at=raw.index("run")
        if "--" in raw[run_at+1:]:
            divider=raw.index("--",run_at+1); run_tail=raw[divider+1:]; raw=raw[:divider]
    p=argparse.ArgumentParser(prog="agent-chat");p.add_argument("--db");p.add_argument("--session")
    sub=p.add_subparsers(dest="op",required=True)
    x=sub.add_parser("register");x.add_argument("--agent",required=True)
    x=sub.add_parser("send");x.add_argument("--to",required=True);x.add_argument("--body-file",required=True);x.add_argument("--attach",action="append",default=[]);x.add_argument("--reply-to")
    x=sub.add_parser("link-reply");x.add_argument("id");x.add_argument("--reply-to",required=True)
    x=sub.add_parser("inbox");x.add_argument("--agent");x.add_argument("--all",action="store_true")
    x=sub.add_parser("acknowledge");x.add_argument("id")
    x=sub.add_parser("request");x.add_argument("resource");x.add_argument("--minutes",type=float,required=True)
    x=sub.add_parser("status")
    x=sub.add_parser("cancel");x.add_argument("resource")
    x=sub.add_parser("check");x.add_argument("resource");x.add_argument("--token")
    x=sub.add_parser("release");x.add_argument("resource");x.add_argument("--receipt",required=True);x.add_argument("--token")
    x=sub.add_parser("recover");x.add_argument("resource");x.add_argument("--receipt",required=True)
    x=sub.add_parser("block");x.add_argument("resource");x.add_argument("--reason",required=True)
    x=sub.add_parser("remove-session");x.add_argument("id")
    sub.add_parser("deregister", help="retire your own session after closing work")
    x=sub.add_parser("run");x.add_argument("resource");x.add_argument("--token");x.add_argument("command",nargs="*")
    a=p.parse_args(raw)
    if a.op=="run": a.command=run_tail or []
    c: Coordinator | None = None
    try:
        c=Coordinator(a.db,a.session)
        if a.op=="register": out=c.register(a.agent)
        elif a.op=="send": out=c.send(a.to,pathlib.Path(a.body_file).read_text(encoding="utf-8"),a.attach,a.reply_to)
        elif a.op=="link-reply": out=c.link_reply(a.id,a.reply_to)
        elif a.op=="inbox":
            if a.agent:
                caller=c.db.execute("SELECT agent FROM sessions WHERE id=?",(c.require_session(),)).fetchone()["agent"]
                if a.agent != caller: raise CoordError("--agent cannot read another agent's inbox")
            out=c.inbox(a.all)
        elif a.op=="acknowledge":out=c.acknowledge(a.id)
        elif a.op=="request":out=c.request(a.resource,a.minutes)
        elif a.op=="status":out=c.status()
        elif a.op=="cancel":out=c.cancel(a.resource)
        elif a.op=="check":out=c.check(a.resource,_token(a))
        elif a.op=="release":out=c.release(a.resource,_token(a),a.receipt)
        elif a.op=="recover":out=c.recover(a.resource,a.receipt)
        elif a.op=="block":out=c.block(a.resource,a.reason)
        elif a.op=="remove-session":out=c.remove_session(a.id)
        elif a.op=="deregister":out=c.remove_session(c.require_session())
        else:
            out=run_command(c,a)
            return int(out["exit_code"])
        return _json_ok(out)
    finally:
        if c is not None: c.close()

if __name__=="__main__":
    try: raise SystemExit(main())
    except (CoordError,sqlite3.Error,OSError,ValueError) as e:
        print(json.dumps({"error":str(e)}),file=sys.stderr);raise SystemExit(2)
