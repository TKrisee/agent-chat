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
import struct
import subprocess
import sys
import time
from typing import Any, Iterator

if __package__:
    from .processes import receipt_pid_alive
    from .receipts import process_closure_proofs, read_process_evidence
    from .guard_generation import install_guard_generation
else:  # The legacy coordinator is also exercised as a standalone script.
    from processes import receipt_pid_alive
    from receipts import process_closure_proofs, read_process_evidence
    from guard_generation import install_guard_generation

def _project_root() -> pathlib.Path:
    """Resolve the caller's project root, never this installed package."""
    return pathlib.Path(
        os.environ.get("AGENT_CHAT_ROOT")
        or os.getcwd()
    ).expanduser().resolve()


ROOT = _project_root()
DEFAULT_DB = ROOT / ".agent-chat" / "state.sqlite3"


def default_db() -> pathlib.Path:
    """Return the current project's default database location."""
    return _project_root() / ".agent-chat" / "state.sqlite3"
MAX_ATTACHMENTS = 50
MAX_ATTACHMENT_SIZE = 10 * 1024 * 1024
MAX_MODEL_ATTACHMENT_SIZE = 64 * 1024 * 1024
MAX_ATTACHMENT_TOTAL_SIZE = MAX_ATTACHMENTS * MAX_ATTACHMENT_SIZE
IMAGE_SIGNATURES = (
    (b"\x89PNG\r\n\x1a\n", "image/png"),
    (b"GIF87a", "image/gif"),
    (b"GIF89a", "image/gif"),
)
IMAGE_EXTENSIONS = {
    ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
    ".gif": "image/gif", ".webp": "image/webp",
}
VIDEO_EXTENSIONS = {
    ".mp4": "video/mp4", ".m4v": "video/mp4",
    ".mov": "video/quicktime", ".webm": "video/webm",
}
AUDIO_EXTENSIONS = {".wav": "audio/wav"}
MODEL_EXTENSIONS = {".glb": "model/gltf-binary", ".fbx": "application/octet-stream"}
EMAIL_EXTENSIONS = {".eml": "message/rfc822"}
PACKAGE_EXTENSIONS = {".unitypackage": "application/octet-stream"}
PROJECT_EXTENSIONS = {".zip": "application/zip", ".itrdraft": "application/octet-stream", ".exr": "image/x-exr"}
TEXT_EXTENSIONS = {
    ".txt": "text/plain", ".md": "text/markdown", ".markdown": "text/markdown",
    ".json": "application/json", ".jsonl": "application/x-ndjson", ".xml": "application/xml",
    ".csv": "text/csv", ".tsv": "text/tab-separated-values", ".log": "text/plain",
    ".yaml": "application/yaml", ".yml": "application/yaml", ".toml": "application/toml",
    ".cs": "text/plain", ".py": "text/plain", ".meta": "application/yaml",
    ".patch": "text/plain", ".diff": "text/plain", ".csproj": "application/xml",
}


class CoordError(RuntimeError):
    """A requested coordination operation cannot be performed."""


class InboxCurrentnessRequired(CoordError):
    """The named session has not fully consumed its own inbox."""


class CurrentnessNoEffect(CoordError):
    """Operation-specific proof that currentness failed before acquisition."""
    def __init__(self, recovery):
        super().__init__('read your inbox after the newest message before changing ownership')
        self.recovery = recovery


def first_ack_lookup(db_path, *, project_id: str, session_id: str, host_id: str,
                     source_id: str, expected_recipient: str) -> dict[str, Any]:
    """Read one sender-owned ACK association without constructing Coordinator."""
    values = (project_id, session_id, host_id, source_id, expected_recipient)
    if any(not isinstance(value, str) or not value or len(value) > 512 for value in values):
        return {"status": "unauthorized"}
    path = pathlib.Path(db_path)
    if not path.is_file():
        return {"status": "unavailable"}
    conn = None
    try:
        conn = sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=3)
        conn.row_factory = sqlite3.Row
        conn.execute('BEGIN')
        required = {
            'meta': {'key', 'value'}, 'sessions': {'id'}, 'retired_sessions': {'id'},
            'remote_session_hosts': {'session_id', 'host_id'},
            'messages': {'id', 'sender_session', 'recipient_session', 'acked_at'},
            'message_replies': {'message_id', 'reply_to'},
            'reply_ack_idempotency': {'sender_session', 'reply_to', 'message_id'},
        }
        present = {row['name'] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        if not set(required) <= present:
            return {"status": "unavailable"}
        for table, columns in required.items():
            actual = {row['name'] for row in conn.execute(f'PRAGMA table_info({table})')}
            if not columns <= actual:
                return {"status": "unavailable"}
        schema = conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
        if not schema or schema['value'] != '1':
            return {"status": "unavailable"}
        project = conn.execute("SELECT value FROM meta WHERE key='project_id'").fetchone()
        if project and project['value'] != project_id:
            return {"status": "unauthorized"}
        if (not conn.execute('SELECT 1 FROM sessions WHERE id=?', (session_id,)).fetchone()
                or conn.execute('SELECT 1 FROM retired_sessions WHERE id=?', (session_id,)).fetchone()):
            return {"status": "unauthorized"}
        hosts = conn.execute(
            'SELECT host_id FROM remote_session_hosts WHERE session_id=?', (session_id,)).fetchmany(2)
        if len(hosts) != 1 or hosts[0]['host_id'] != host_id:
            return {"status": "unauthorized"}
        if conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_team_reset_targets'").fetchone():
            columns = {row['name'] for row in conn.execute('PRAGMA table_info(root_team_reset_targets)')}
            if 'session_id' not in columns:
                return {"status": "unavailable"}
            if conn.execute('SELECT 1 FROM root_team_reset_targets WHERE session_id=?', (session_id,)).fetchone():
                return {"status": "unauthorized"}

        source_rows = conn.execute(
            'SELECT id,sender_session,recipient_session,acked_at FROM messages WHERE id=?',
            (source_id,)).fetchmany(2)
        if len(source_rows) != 1:
            return {"status": "unauthorized"}
        source = source_rows[0]
        # The question/source is expected-recipient -> caller; the committed
        # ACK is caller -> expected-recipient.
        if (source['sender_session'], source['recipient_session']) != (expected_recipient, session_id):
            return {"status": "unauthorized"}

        associations = conn.execute(
            'SELECT message_id FROM reply_ack_idempotency WHERE sender_session=? AND reply_to=?',
            (session_id, source_id)).fetchmany(2)
        read_at = time.time()
        if not associations:
            if source['acked_at'] is not None:
                # Only a provenance row written in the same transaction as the
                # first standalone acknowledgement can disambiguate this marker.
                standalone = None
                if 'standalone_message_acknowledgements' in present:
                    columns = {row['name'] for row in conn.execute('PRAGMA table_info(standalone_message_acknowledgements)')}
                    if {'message_id', 'recipient_session', 'acknowledged_at'} <= columns:
                        standalone = conn.execute(
                            'SELECT 1 FROM standalone_message_acknowledgements WHERE message_id=? AND recipient_session=? AND acknowledged_at=?',
                            (source_id, session_id, source['acked_at'])).fetchone()
                if standalone is None:
                    return {"status": "inconsistent", "source_id": source_id, "read_at": read_at}
            return {"status": "not_recorded_at_read", "source_id": source_id,
                    "conversation_id": source_id, "read_at": read_at}
        if len(associations) != 1 or source['acked_at'] is None:
            return {"status": "inconsistent", "source_id": source_id, "read_at": read_at}
        reply_id = associations[0]['message_id']
        replies = conn.execute(
            '''SELECT m.id,m.sender_session,m.recipient_session,r.reply_to
               FROM messages m LEFT JOIN message_replies r ON r.message_id=m.id
               WHERE m.id=?''', (reply_id,)).fetchmany(2)
        if (len(replies) != 1 or replies[0]['sender_session'] != session_id
                or replies[0]['recipient_session'] != expected_recipient
                or replies[0]['reply_to'] != source_id):
            return {"status": "inconsistent", "source_id": source_id, "read_at": read_at}
        return {"status": "recorded", "source_id": source_id, "reply_id": reply_id,
                "conversation_id": source_id, "read_at": read_at}
    except (OSError, sqlite3.Error, TypeError, ValueError):
        return {"status": "unavailable"}
    finally:
        if conn is not None:
            conn.close()


def reset_locked(db, session):
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='session_resets'").fetchone():
        row = db.execute("SELECT status FROM session_resets WHERE target_session=? AND status NOT IN ('completed','cancelled','failed')", (session,)).fetchone()
        if row:
            from .root_lifecycle import released_parked
            return not (row['status'] == 'parked' and released_parked(db, session))
    return False


def assert_reset_unlocked(db, session):
    if reset_locked(db, session):
        raise CoordError('fresh-session reset pending; inspect session-reset-status before changing ownership or routing')


def assert_agent_running(db, session, *, cleanup=False, admitted_task_message_id=None):
    if __package__:
        from .root_restart import held
        if held(db, session) and not cleanup:
            raise CoordError('root fresh-start is pending; inspect root fresh-status before changing routing or ownership')
        from .root_lifecycle import stopped, parked, task_execution_admission
        if parked(db,session) and not cleanup:
            raise CoordError('parked PO has no input admission')
        task_admission_valid = (isinstance(admitted_task_message_id, str)
                                and (admission := task_execution_admission(db, admitted_task_message_id)) is not None
                                and admission['target_session'] == session)
        if stopped(db, session) and not cleanup and not task_admission_valid:
            raise CoordError('root stop-all barrier is active; explicit root resume-all is required')
        from .root_orchestrator import projection
        root = projection(db, session)
        if root and root['paused']:
            raise CoordError('root routing is paused for clean detach; inspect root status')
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='agent_controls'").fetchone():
        row = db.execute('SELECT mode FROM agent_controls WHERE target_session=?', (session,)).fetchone()
        if row and row['mode'] != 'running' and not (cleanup and row['mode'] == 'stopping'):
            raise CoordError('agent admission is ' + row['mode'] + '; inspect agent-status or resume the same identity')

    if not cleanup:
        assert_original_owner_admission(db, session)


def assert_original_owner_admission(db, session):
    """Deny new execution after custody quarantine; no user-selectable bypass."""
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='original_owner_dispositions'").fetchone():
        if db.execute('SELECT 1 FROM original_owner_dispositions WHERE owner_session=? LIMIT 1', (session,)).fetchone():
            raise CoordError('original owner disposition quarantine is active; future admission withheld')


def _now() -> float: return time.time()
def _id(prefix: str) -> str: return prefix + "_" + secrets.token_urlsafe(24)


def agent_labels(db):
    labels = {}
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='retired_sessions'").fetchone():
        labels.update({row['id']: row['agent'] for row in db.execute('SELECT id,agent FROM retired_sessions')})
    labels.update({row['id']: row['agent'] for row in db.execute('SELECT id,agent FROM sessions')})
    labels['system:supervisor'] = 'supervisor'
    return labels


# Decision paths are bounded independently of conversation/store size.
DECISION_MAX_DEPTH = 32
DECISION_PROJECTION_NODES = 128
DECISION_TERMINAL = frozenset({'accepted', 'declined', 'conditional'})


def _decision_request(db, message_id):
    request = db.execute('''SELECT r.*,m.seq FROM operator_decision_requests r
        JOIN messages m ON m.id=r.message_id AND m.sender_session=r.author_session
          AND m.recipient_session=r.recipient_session WHERE r.message_id=?''', (message_id,)).fetchone()
    project = db.execute("SELECT value FROM meta WHERE key='project_id'").fetchone()
    if not request or request['project_id'] != (project['value'] if project else 'default'):
        raise CoordError('decision request belongs to a different project context or is unavailable')
    if (request['author_session'] == request['recipient_session']
            or not db.execute('SELECT 1 FROM operator_identities WHERE session_id=?', (request['recipient_session'],)).fetchone()
            or db.execute('SELECT 1 FROM message_batches WHERE message_id=?', (message_id,)).fetchone()):
        raise CoordError('decision participants, role, origin or direct-message context is invalid')
    return dict(request)


def _decision_node(db, message_id):
    row = db.execute('''SELECT m.id,m.seq,m.created_at,m.sender_session,m.recipient_session,
        r.reply_to,o.sender_session origin_sender,
        EXISTS(SELECT 1 FROM message_batches b WHERE b.message_id=m.id) batched
        FROM messages m LEFT JOIN message_replies r ON r.message_id=m.id
        LEFT JOIN operator_message_origins o ON o.message_id=m.id WHERE m.id=?''', (message_id,)).fetchone()
    return dict(row) if row else None


def _decision_path(db, request, reply_id, *, recorded=False):
    """Validate exact alternating rooted ancestry; direct legacy origin stays weaker."""
    path = []; seen = set(); current = reply_id; child_seq = None
    for depth in range(DECISION_MAX_DEPTH):
        if current == request['message_id']:
            if not path or len(path) % 2 != 1 or (child_seq is not None and request['seq'] >= child_seq):
                break
            nested = len(path) > 1
            if nested and (db.execute('SELECT 1 FROM operator_identities WHERE session_id=?', (request['author_session'],)).fetchone()
                           or db.execute('SELECT 1 FROM operator_message_origins WHERE message_id=?', (request['message_id'],)).fetchone()):
                raise CoordError('nested decision author role/origin is invalid')
            if nested and any(node['origin_sender'] != request['recipient_session']
                              for node in path if node['sender_session'] == request['recipient_session']):
                raise CoordError('nested decision replies require authenticated operator UI provenance on every operator edge')
            return path
        if current in seen:
            raise CoordError('decision reply ancestry is cyclic')
        seen.add(current)
        node = _decision_node(db, current)
        operator_edge = depth % 2 == 0
        sender = request['recipient_session'] if operator_edge else request['author_session']
        recipient = request['author_session'] if operator_edge else request['recipient_session']
        if (not node or node['sender_session'] != sender or node['recipient_session'] != recipient
                or node['batched'] or not node['reply_to']
                or (child_seq is not None and node['seq'] >= child_seq)
                or (node['origin_sender'] is not None and (not operator_edge or node['origin_sender'] != sender))
                or (not recorded and db.execute('SELECT 1 FROM operator_decision_requests WHERE message_id=?', (current,)).fetchone())):
            raise CoordError('answer must be the exact direct reply or bounded rooted dialogue from the decision recipient')
        path.append(node); current = node['reply_to']; child_seq = node['seq']
    # The final permitted edge may land on the question after the last loop.
    if current == request['message_id'] and path and len(path) % 2 == 1 and request['seq'] < child_seq:
        if len(path) > 1 and (db.execute('SELECT 1 FROM operator_identities WHERE session_id=?', (request['author_session'],)).fetchone()
                              or db.execute('SELECT 1 FROM operator_message_origins WHERE message_id=?', (request['message_id'],)).fetchone()):
            raise CoordError('nested decision author role/origin is invalid')
        if len(path) > 1 and any(node['origin_sender'] != request['recipient_session']
                               for node in path if node['sender_session'] == request['recipient_session']):
            raise CoordError('nested decision replies require authenticated operator UI provenance on every operator edge')
        return path
    raise CoordError('decision reply ancestry is broken, unrelated, cyclic or exceeds 32 edges')


def _decision_progresses(previous, path):
    """A newer direct answer or descendant progresses a nonterminal dialogue."""
    return (previous['outcome'] == 'clarification'
            and path[0]['seq'] > previous['reply_seq']
            and (len(path) == 1 or previous['reply_message_id'] in {node['id'] for node in path}))


def _decision_legacy_classification(previous, event, path):
    """One explicit terminal classification may follow its immutable legacy reply."""
    return (previous['legacy'] and previous['outcome'] == 'clarification'
            and not event['legacy'] and event['outcome'] in DECISION_TERMINAL
            and event['reply_message_id'] == previous['reply_message_id']
            and path[0]['seq'] == previous['reply_seq'])


def _decision_history(db, request):
    legacy = db.execute('SELECT reply_message_id,recorded_by,outcome,recorded_at FROM operator_decision_answers WHERE message_id=?', (request['message_id'],)).fetchone()
    events = []
    if legacy:
        events.append(dict(legacy, legacy=True))
    if db.execute("SELECT 1 FROM sqlite_master WHERE name='operator_decision_events'").fetchone():
        events.extend(dict(row, legacy=False) for row in db.execute('''SELECT reply_message_id,recorded_by,outcome,recorded_at
            FROM operator_decision_events WHERE message_id=? ORDER BY event_seq LIMIT 33''', (request['message_id'],)))
    if len(events) > DECISION_MAX_DEPTH:
        raise CoordError('decision event history exceeds its bounded dialogue')
    previous = None; terminal = False
    for event in events:
        # An immutable admitted event is not retroactively reparented by a
        # later decision mark on an intermediate author message.
        path = _decision_path(db, request, event['reply_message_id'], recorded=True)
        if (event['recorded_by'] != request['author_session']
                or event['outcome'] not in DECISION_TERMINAL | {'clarification'}
                or terminal or (event['legacy'] and len(path) != 1)
                or (previous and not (_decision_progresses(previous, path)
                                   or _decision_legacy_classification(previous, event, path)))):
            raise CoordError('decision outcome history is inconsistent')
        event['reply_seq'] = path[0]['seq']
        event['reply_created_at'] = path[0]['created_at']
        event['origin'] = 'authenticated_operator_ui' if path[0]['origin_sender'] else 'registered_operator_direct'
        terminal = event['outcome'] in DECISION_TERMINAL
        previous = event
    return events


def _decision_public_history(history):
    fields = ('reply_message_id', 'recorded_by', 'outcome', 'recorded_at', 'legacy', 'origin')
    return [{field: event[field] for field in fields} for event in history]


def decision_projection(db, message_id, *, include_latest=False):
    """Private decision metadata shared by message, notification and author views."""
    request = _decision_request(db, message_id)
    history = _decision_history(db, request)
    last = history[-1] if history else None
    terminal = bool(last and last['outcome'] in DECISION_TERMINAL)
    if terminal and not include_latest:
        return dict(status='answer_recorded', requested_at=request['created_at'],
                    reply_message_id=last['reply_message_id'], reply_received_at=last['reply_created_at'],
                    answer_outcome=last['outcome'], answer_recorded_at=last['recorded_at'],
                    answer_origin=last['origin'], answer_history=_decision_public_history(history), ancestry_limited=False)
    latest = None; observed_latest = None; limited = False; visited = 1
    nodes = {request['message_id']: request['seq']}; frontier = [request['message_id']]
    # Forward traversal stays inside this exact two-party root, with a hard node
    # budget. Every selected operator candidate then receives full path validation.
    for depth in range(1, DECISION_MAX_DEPTH + 1):
        if not frontier:
            break
        marks = ','.join('?' for _ in frontier)
        children = db.execute(f'''SELECT m.id,m.seq FROM message_replies r
            JOIN messages m ON m.id=r.message_id WHERE r.reply_to IN ({marks})
            AND m.sender_session=? AND m.recipient_session=? ORDER BY m.seq LIMIT ?''',
            (*frontier, request['recipient_session'] if depth % 2 else request['author_session'],
             request['author_session'] if depth % 2 else request['recipient_session'],
             DECISION_PROJECTION_NODES - visited + 1)).fetchall()
        if len(children) > DECISION_PROJECTION_NODES - visited:
            limited = True; break
        visited += len(children); next_frontier = []
        for child in children:
            node = _decision_node(db, child['id'])
            if (node['batched'] or node['seq'] <= nodes[node['reply_to']]
                    or node['origin_sender'] not in ((request['recipient_session'], None) if depth % 2 else (None,))
                    or db.execute('SELECT 1 FROM operator_decision_requests WHERE message_id=?', (node['id'],)).fetchone()):
                continue
            if depth % 2:
                try:
                    path = _decision_path(db, request, node['id'])
                except CoordError:
                    continue
                if observed_latest is None or node['seq'] > observed_latest['seq']:
                    observed_latest = node
                if (not last or _decision_progresses(last, path)):
                    if latest is None or node['seq'] > latest['seq']:
                        latest = node
                if not node['origin_sender']:
                    continue  # Direct-only weaker origin cannot seed nested admission.
            nodes[node['id']] = node['seq']; next_frontier.append(node['id'])
        frontier = next_frontier
    if frontier and depth == DECISION_MAX_DEPTH:
        marks = ','.join('?' for _ in frontier)
        if db.execute(f'SELECT 1 FROM message_replies WHERE reply_to IN ({marks}) LIMIT 1', frontier).fetchone():
            limited = True
    if limited:
        latest = None; observed_latest = None  # No claim about a latest reply outside the explicit budget.
    return dict(status='answer_recorded' if terminal else ('reply_received' if latest else 'pending'), requested_at=request['created_at'],
                reply_message_id=last['reply_message_id'] if terminal else (latest['id'] if latest else (last['reply_message_id'] if last else None)),
                reply_received_at=last['reply_created_at'] if terminal else (latest['created_at'] if latest else None),
                latest_reply_message_id=observed_latest['id'] if observed_latest else None,
                latest_reply_received_at=observed_latest['created_at'] if observed_latest else None,
                answer_outcome=last['outcome'] if last else None,
                answer_recorded_at=last['recorded_at'] if last else None,
                answer_origin=last['origin'] if last else None,
                answer_history=_decision_public_history(history), ancestry_limited=limited)


class Coordinator:
    def __init__(self, db_path: str | os.PathLike[str] | None = None,
                 session: str | None = None):
        self.path = pathlib.Path(db_path or os.environ.get("AGENT_CHAT_DB") or default_db())
        self.session = session or os.environ.get("AGENT_CHAT_SESSION")
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
        CREATE TABLE IF NOT EXISTS operator_identities (session_id TEXT PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS operator_message_origins (
          message_id TEXT PRIMARY KEY, sender_session TEXT NOT NULL, channel TEXT NOT NULL);
        CREATE TRIGGER IF NOT EXISTS immutable_operator_origin_update BEFORE UPDATE ON operator_message_origins
          BEGIN SELECT RAISE(ABORT, 'operator message origin is immutable'); END;
        CREATE TRIGGER IF NOT EXISTS immutable_operator_origin_delete BEFORE DELETE ON operator_message_origins
          BEGIN SELECT RAISE(ABORT, 'operator message origin is immutable'); END;
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
        CREATE TABLE IF NOT EXISTS message_assignment_context (
          message_id TEXT PRIMARY KEY, task_id TEXT NOT NULL,
          FOREIGN KEY(message_id) REFERENCES messages(id));
        CREATE TRIGGER IF NOT EXISTS message_assignment_context_no_update
          BEFORE UPDATE ON message_assignment_context BEGIN SELECT RAISE(ABORT,'assignment link is immutable'); END;
        CREATE TRIGGER IF NOT EXISTS message_assignment_context_no_delete
          BEFORE DELETE ON message_assignment_context BEGIN SELECT RAISE(ABORT,'assignment link is immutable'); END;
        CREATE TABLE IF NOT EXISTS manager_message_updates (
          message_id TEXT PRIMARY KEY, task_id TEXT NOT NULL,
          FOREIGN KEY(message_id) REFERENCES messages(id));
        CREATE INDEX IF NOT EXISTS manager_message_updates_task_id ON manager_message_updates(task_id);
        CREATE TABLE IF NOT EXISTS message_attention (
          message_id TEXT PRIMARY KEY,
          FOREIGN KEY(message_id) REFERENCES messages(id));
        CREATE TABLE IF NOT EXISTS operator_decision_requests (
          message_id TEXT PRIMARY KEY, author_session TEXT NOT NULL,
          recipient_session TEXT NOT NULL, project_id TEXT NOT NULL,
          created_at REAL NOT NULL,
          FOREIGN KEY(message_id) REFERENCES messages(id));
        CREATE TABLE IF NOT EXISTS operator_decision_answers (
          message_id TEXT PRIMARY KEY, reply_message_id TEXT UNIQUE NOT NULL,
          recorded_by TEXT NOT NULL, outcome TEXT NOT NULL
            CHECK(outcome IN ('accepted','declined','conditional','clarification')),
          recorded_at REAL NOT NULL,
          FOREIGN KEY(message_id) REFERENCES operator_decision_requests(message_id),
          FOREIGN KEY(reply_message_id) REFERENCES messages(id));
        CREATE TRIGGER IF NOT EXISTS immutable_operator_decision_request_update
          BEFORE UPDATE ON operator_decision_requests
          BEGIN SELECT RAISE(ABORT, 'operator decision request is immutable'); END;
        CREATE TRIGGER IF NOT EXISTS immutable_operator_decision_request_delete
          BEFORE DELETE ON operator_decision_requests
          BEGIN SELECT RAISE(ABORT, 'operator decision request is immutable'); END;
        CREATE TRIGGER IF NOT EXISTS immutable_operator_decision_answer_update
          BEFORE UPDATE ON operator_decision_answers
          BEGIN SELECT RAISE(ABORT, 'operator decision answer is immutable'); END;
        CREATE TRIGGER IF NOT EXISTS immutable_operator_decision_answer_delete
          BEFORE DELETE ON operator_decision_answers
          BEGIN SELECT RAISE(ABORT, 'operator decision answer is immutable'); END;
        CREATE TABLE IF NOT EXISTS operator_decision_events (
          event_seq INTEGER PRIMARY KEY AUTOINCREMENT,message_id TEXT NOT NULL,
          reply_message_id TEXT UNIQUE NOT NULL,recorded_by TEXT NOT NULL,
          outcome TEXT NOT NULL CHECK(outcome IN ('accepted','declined','conditional','clarification')),
          recorded_at REAL NOT NULL,
          FOREIGN KEY(message_id) REFERENCES operator_decision_requests(message_id),
          FOREIGN KEY(reply_message_id) REFERENCES messages(id));
        CREATE UNIQUE INDEX IF NOT EXISTS operator_decision_event_terminal
          ON operator_decision_events(message_id) WHERE outcome != 'clarification';
        CREATE INDEX IF NOT EXISTS operator_decision_event_question ON operator_decision_events(message_id,event_seq);
        CREATE INDEX IF NOT EXISTS operator_decision_reply_parent ON message_replies(reply_to);
        CREATE TRIGGER IF NOT EXISTS immutable_operator_decision_event_update BEFORE UPDATE ON operator_decision_events
          BEGIN SELECT RAISE(ABORT, 'operator decision event is immutable'); END;
        CREATE TRIGGER IF NOT EXISTS immutable_operator_decision_event_delete BEFORE DELETE ON operator_decision_events
          BEGIN SELECT RAISE(ABORT, 'operator decision event is immutable'); END;
        CREATE TABLE IF NOT EXISTS operator_decision_dismissals (
          message_id TEXT PRIMARY KEY, author_session TEXT NOT NULL,
          recipient_session TEXT NOT NULL, project_id TEXT NOT NULL,
          dismissed_at REAL NOT NULL, reason TEXT NOT NULL,
          FOREIGN KEY(message_id) REFERENCES operator_decision_requests(message_id));
        CREATE TRIGGER IF NOT EXISTS immutable_operator_decision_dismissal_update BEFORE UPDATE ON operator_decision_dismissals
          BEGIN SELECT RAISE(ABORT, 'operator decision dismissal is immutable'); END;
        CREATE TRIGGER IF NOT EXISTS immutable_operator_decision_dismissal_delete BEFORE DELETE ON operator_decision_dismissals
          BEGIN SELECT RAISE(ABORT, 'operator decision dismissal is immutable'); END;
        CREATE INDEX IF NOT EXISTS attachments_message_id ON attachments(message_id);
        CREATE TABLE IF NOT EXISTS reply_ack_idempotency (
          sender_session TEXT NOT NULL, reply_to TEXT NOT NULL,
          fingerprint TEXT NOT NULL, message_id TEXT NOT NULL,
          PRIMARY KEY(sender_session, reply_to),
          FOREIGN KEY(message_id) REFERENCES messages(id));
        CREATE TABLE IF NOT EXISTS standalone_message_acknowledgements (
          message_id TEXT PRIMARY KEY, recipient_session TEXT NOT NULL,
          acknowledged_at REAL NOT NULL,
          FOREIGN KEY(message_id) REFERENCES messages(id));
        CREATE TABLE IF NOT EXISTS message_reads (
          message_id TEXT NOT NULL, reader_session TEXT NOT NULL, read_at REAL NOT NULL,
          provenance TEXT NOT NULL,
          PRIMARY KEY(message_id,reader_session),
          FOREIGN KEY(message_id) REFERENCES messages(id));
        CREATE TABLE IF NOT EXISTS resources (
          name TEXT PRIMARY KEY, owner_session TEXT, reservation_id TEXT UNIQUE,
          token TEXT, granted_at REAL, deadline REAL, stale INTEGER NOT NULL DEFAULT 0, reason TEXT);
        CREATE TABLE IF NOT EXISTS resource_queue (
          seq INTEGER PRIMARY KEY AUTOINCREMENT, resource TEXT NOT NULL,
          session TEXT NOT NULL, queued_at REAL NOT NULL, UNIQUE(resource, session));
        CREATE TABLE IF NOT EXISTS reservation_renewals (
          reservation_id TEXT PRIMARY KEY,
          successes INTEGER NOT NULL CHECK(successes BETWEEN 1 AND 4),
          extra_minutes REAL NOT NULL CHECK(extra_minutes > 0 AND extra_minutes <= 120));
        CREATE TABLE IF NOT EXISTS receipts (
          reservation_id TEXT PRIMARY KEY, receipt_sha256 TEXT UNIQUE NOT NULL,
          evidence_sha256 TEXT NOT NULL, receipt_json TEXT NOT NULL, action TEXT NOT NULL, recorded_at REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS resource_uncertainty_dispositions (
          request_id TEXT PRIMARY KEY, resource TEXT NOT NULL, reservation_id TEXT NOT NULL,
          owner_session TEXT NOT NULL, host_id TEXT NOT NULL, reason TEXT NOT NULL, recorded_at REAL NOT NULL,
          observed_resource_state TEXT, local_runs_total INTEGER, local_runs_open INTEGER,
          remote_runs_total INTEGER, remote_runs_open INTEGER);
        CREATE TRIGGER IF NOT EXISTS resource_uncertainty_immutable_update BEFORE UPDATE ON resource_uncertainty_dispositions
          BEGIN SELECT RAISE(ABORT,'resource uncertainty record is immutable'); END;
        CREATE TRIGGER IF NOT EXISTS resource_uncertainty_immutable_delete BEFORE DELETE ON resource_uncertainty_dispositions
          BEGIN SELECT RAISE(ABORT,'resource uncertainty record is immutable'); END;
        CREATE TABLE IF NOT EXISTS guarded_runs (
          run_id TEXT PRIMARY KEY, resource TEXT NOT NULL, reservation_id TEXT NOT NULL,
          session TEXT NOT NULL, pid INTEGER NOT NULL, pgid INTEGER, started_at REAL NOT NULL,
          closed_at REAL);
        INSERT OR IGNORE INTO meta(key,value) VALUES('schema_version','1');
        """)
        uncertainty_columns={row['name'] for row in self.db.execute('PRAGMA table_info(resource_uncertainty_dispositions)')}
        for name,kind in (('observed_resource_state','TEXT'),('local_runs_total','INTEGER'),
                          ('local_runs_open','INTEGER'),('remote_runs_total','INTEGER'),('remote_runs_open','INTEGER')):
            if name not in uncertainty_columns:
                self.db.execute(f'ALTER TABLE resource_uncertainty_dispositions ADD COLUMN {name} {kind}')
        self._migrate_message_reads()
        version = self.db.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()[0]
        if version != "1": raise CoordError("unsupported coordinator schema version")
        install_guard_generation(self.db)
        from .goal_selection_generation import install as install_goal_generation, PROJECT as GOAL_PROJECT
        install_goal_generation(self.db, GOAL_PROJECT)
        execution_epoch_schema(self.db)
        # Install lifecycle-owned tables on the same supported database
        # initialization path used by the bridge before its task readers run.
        # Older stores may have task admissions but predate their immutable
        # baseline/continuation tables; those readers must fail closed without
        # a missing-table error or an ad-hoc live schema repair.
        from .root_lifecycle import schema as root_lifecycle_schema
        root_lifecycle_schema(self.db)
        from .capacity_admission import install_schema as capacity_admission_schema
        capacity_admission_schema(self.db)
        from .guard_witness import schema as guard_witness_schema
        guard_witness_schema(self.db)

    def _migrate_message_reads(self) -> None:
        """Preserve historic receipt rows without mistaking them for full reads.

        Earlier versions could write the same row for acknowledgement/reply
        bookkeeping.  The table held no origin field, so those records are
        deliberately retained as ``legacy_unknown`` rather than promoted to
        recipient-body or full-context evidence.
        """
        columns = self.db.execute("PRAGMA table_info(message_reads)").fetchall()
        names = {row['name'] for row in columns}
        composite = {row['name']: row['pk'] for row in columns}.get('message_id') == 1 and \
            {row['name']: row['pk'] for row in columns}.get('reader_session') == 2
        if names == {'message_id', 'reader_session', 'read_at', 'provenance'} and composite:
            return
        self.db.execute("BEGIN IMMEDIATE")
        try:
            # Record routing history once, in the same transaction as provenance migration.
            # This archives prior ACKs; it never invents recipient consumption evidence.
            self.db.execute("INSERT OR IGNORE INTO meta(key,value) VALUES('message_delivery_archive_before',?)",
                            (str(_now()),))
            self.db.execute('''CREATE TABLE message_reads_migrating (
                message_id TEXT NOT NULL, reader_session TEXT NOT NULL, read_at REAL NOT NULL,
                provenance TEXT NOT NULL,
                PRIMARY KEY(message_id,reader_session),
                FOREIGN KEY(message_id) REFERENCES messages(id))''')
            if {'message_id', 'reader_session', 'read_at'}.issubset(names):
                # Every legacy marker is intentionally unknown: its original
                # producer may have been an ACK/reply route rather than a body read.
                self.db.execute('''INSERT INTO message_reads_migrating
                    (message_id,reader_session,read_at,provenance)
                    SELECT message_id,reader_session,read_at,'legacy_unknown'
                    FROM message_reads''')
            self.db.execute('DROP TABLE message_reads')
            self.db.execute('ALTER TABLE message_reads_migrating RENAME TO message_reads')
        except BaseException:
            self.db.rollback()
            raise
        else:
            self.db.commit()

    @contextlib.contextmanager
    def tx(self, observer=None) -> Iterator[sqlite3.Connection]:
        if observer: observer.mark('transaction_acquire', 'start')
        try:
            self.db.execute("BEGIN IMMEDIATE")
        except BaseException:
            if observer: observer.mark('transaction_acquire', 'end', 'error')
            raise
        if observer: observer.mark('transaction_acquire', 'end')
        observed_at = _now()
        observed_expiries = []
        self._tx_observed_expiries = observed_expiries
        if observer: observer.mark('transaction_held', 'start')
        try:
            self.db.execute(
                "UPDATE resources SET stale=1 WHERE owner_session IS NOT NULL AND deadline<?",
                (observed_at,),
            )
            yield self.db
        except BaseException:
            self._tx_observed_expiries = None
            self.db.rollback()
            # A refused operation must not undo an already-observed expiry.
            self.db.execute(
                "UPDATE resources SET stale=1 WHERE owner_session IS NOT NULL AND deadline<?",
                (observed_at,),
            )
            # _row can first observe expiry after transaction entry. Retain
            # only that exact pre-existing grant, not rolled-back new writes.
            self.db.executemany(
                "UPDATE resources SET stale=1 WHERE name=? AND owner_session=? "
                "AND reservation_id=? AND token=? AND deadline=?",
                observed_expiries,
            )
            if observer: observer.mark('transaction_held', 'end', 'error')
            raise
        else:
            self._tx_observed_expiries = None
            self.db.commit()
            if observer: observer.mark('transaction_held', 'end')

    def require_session(self) -> str:
        if not self.session: raise CoordError("AGENT_CHAT_SESSION or --session is required")
        if not self.db.execute("SELECT 1 FROM sessions WHERE id=?", (self.session,)).fetchone():
            raise CoordError("unknown session; register first")
        return self.session

    def register(self, agent: str) -> dict[str, Any]:
        if not agent or not agent.strip(): raise CoordError("agent must be nonempty")
        sid = self.session or _id("session")
        if sid == 'system:supervisor':
            raise CoordError('system:supervisor is reserved for durable system notices')
        with self.tx() as db:
            if db.execute("SELECT 1 FROM retired_sessions WHERE id=?", (sid,)).fetchone():
                raise CoordError("session is deregistered; register a new session instead of reusing its ID")
            old = db.execute("SELECT agent FROM sessions WHERE id=?", (sid,)).fetchone()
            if old and old["agent"] != agent: raise CoordError("session is already registered to another agent")
            if not old: db.execute("INSERT INTO sessions(id,agent,registered_at) VALUES(?,?,?)", (sid, agent, _now()))
        self.session = sid
        return {"session": sid, "agent": agent}

    def rename(self, agent: str) -> dict[str, Any]:
        """Change the caller's display name without replacing its identity."""
        if not isinstance(agent, str) or not agent.strip():
            raise CoordError("agent must be nonempty")
        sid = self.require_session()
        # A label edit must not observe expiry or otherwise mutate reservations.
        self.db.execute("BEGIN IMMEDIATE")
        try:
            if self.db.execute("SELECT 1 FROM sessions WHERE agent=? AND id<>?", (agent, sid)).fetchone():
                raise CoordError("agent name is already registered to another session")
            self.db.execute("UPDATE sessions SET agent=? WHERE id=?", (agent, sid))
        except BaseException:
            self.db.rollback()
            raise
        else:
            self.db.commit()
        return {"session": sid, "agent": agent}

    def remove_session(self, session_id: str) -> dict[str, Any]:
        """Remove a coordination session (e.g. a finished subagent).

        Removes only a fully closed session. Messages and an identity tombstone
        remain, preserving attribution without keeping an active recipient.
        """
        caller = self.require_session()
        if __package__:
            from .root_orchestrator import assert_local_lifecycle, assert_management_target
            assert_local_lifecycle(self.db, session_id)
            assert_management_target(self.db, caller, session_id)
        if not session_id:
            raise CoordError("session_id must be nonempty")
        with self.tx() as db:
            info = db.execute(
                "SELECT id,agent FROM sessions WHERE id=?", (session_id,)).fetchone()
            if info is None:
                raise CoordError(f"unknown session: {session_id}")
            agent_name = info["agent"]
            assert_reset_unlocked(db, session_id)
            if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='agent_control_requests'").fetchone():
                if db.execute("SELECT 1 FROM agent_control_requests WHERE target_session=? AND status NOT IN ('completed','failed','cancelled')", (session_id,)).fetchone():
                    raise CoordError('resolve pending agent controls before removing its identity')

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
        # Prior ACKs are archival scheduling state; new ACKs never prove a read.
        # Privileged consumption remains separate and requires genuine provenance.
        assert_reset_unlocked(db, session)
        row = db.execute("SELECT inbox_read_seq FROM sessions WHERE id=?", (session,)).fetchone()
        latest = db.execute("SELECT COALESCE(MAX(seq),0) n FROM messages WHERE recipient_session=?", (session,)).fetchone()["n"]
        if row is None: raise CoordError("unknown session")
        unseen = db.execute(
            "SELECT m.id FROM messages m WHERE m.recipient_session=? "
            "AND NOT EXISTS (SELECT 1 FROM message_reads r WHERE r.message_id=m.id "
            "AND r.reader_session=m.recipient_session "
            "AND r.provenance IN ('recipient_full_message','recipient_context_full')) AND NOT (m.acked_at IS NOT NULL AND m.acked_at <= COALESCE(CAST((SELECT value FROM meta WHERE key='message_delivery_archive_before') AS REAL),-1)) ORDER BY m.seq LIMIT 11",
            (session,)).fetchall()
        if unseen:
            message = "read your inbox after the newest message before changing ownership"
            # Some internal callers check another session. Do not expose that
            # recipient's inbox identifiers through the caller's error.
            if session == self.session:
                ids = json.dumps([item["id"] for item in unseen[:10]], separators=(",", ":"))
                more = "true" if len(unseen) > 10 else "false"
                message += (
                    f"; own messages missing full-read proof: {ids}; more_unread={more}. "
                    "Run own context without --cursor to return unread messages, then read "
                    "each truncated source completely with message ID. ACKs and previews "
                    "are not full reads. Reconcile prior attempts before retrying.")
            raise InboxCurrentnessRequired(message)
        if row["inbox_read_seq"] < latest:
            db.execute("UPDATE sessions SET inbox_read_seq=? WHERE id=?", (latest, session))

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

    def _stamp_operator_origin(self, db, message_id, sender):
        if getattr(self, '_operator_message_origin', False):
            if not db.execute('SELECT 1 FROM operator_identities WHERE session_id=?', (sender,)).fetchone():
                raise CoordError('operator message origin requires the server selected UI identity')
            db.execute('INSERT INTO operator_message_origins VALUES(?,?,?)',
                       (message_id, sender, 'authenticated_operator_ui'))

    @staticmethod
    def _assignment_provenance(db, task_id, sender, recipient):
        """Only already team-visible metadata for one explicit active task."""
        required=('project_work','project_orchestrators','message_reads','bridge_bindings')
        if any(not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(table,)).fetchone() for table in required):
            return None
        roles=db.execute('SELECT session_id,root_session FROM project_orchestrators').fetchall()
        if len(roles)!=1: return None
        po,root=roles[0]['session_id'],roles[0]['root_session']
        if sender in (po,root) or recipient in (po,root) or sender==recipient: return None
        if db.execute('SELECT 1 FROM operator_identities WHERE session_id IN (?,?)',(sender,recipient)).fetchone(): return None
        from .management import manager
        def worker(session):
            if not db.execute('SELECT 1 FROM sessions WHERE id=?',(session,)).fetchone(): return False
            if db.execute('SELECT 1 FROM retired_sessions WHERE id=?',(session,)).fetchone(): return False
            current,seen=session,set()
            while current and current not in seen:
                seen.add(current)
                binding=db.execute('SELECT parent_session FROM bridge_bindings WHERE session_id=?',(current,)).fetchone()
                current=(binding['parent_session'] if binding else None) or manager(db,current)
                if current==po:return True
            return False
        if not worker(sender) or not worker(recipient): return None
        project=db.execute("SELECT value FROM meta WHERE key='project_id'").fetchone()
        row=db.execute("SELECT id,project_id,source_request_id,owner_session,assigned_by,state FROM project_work WHERE id=? AND kind='task'",(task_id,)).fetchone()
        if not row or row['project_id'] != (project['value'] if project else 'default') or row['owner_session']!=sender or row['assigned_by']!=po or row['state'] not in ('queued','in_progress'): return None
        source=db.execute('SELECT sender_session,recipient_session FROM messages WHERE id=?',(row['source_request_id'],)).fetchone()
        if not source or (source['sender_session'],source['recipient_session'])!=(po,sender): return None
        if db.execute('SELECT 1 FROM operator_message_origins WHERE message_id=?',(row['source_request_id'],)).fetchone(): return None
        if not db.execute("SELECT 1 FROM message_reads WHERE message_id=? AND reader_session=? AND provenance IN ('recipient_full_message','recipient_context_full')",(row['source_request_id'],sender)).fetchone(): return None
        return {'task_id':row['id'],'source_request_id':row['source_request_id'],
                'owner_session':sender,'manager_session':po,'project_id':row['project_id'],
                'state':row['state'],'instruction_origin':'agent_message'}

    def _message_assignment(self, db, row):
        link=db.execute('SELECT task_id FROM message_assignment_context WHERE message_id=?',(row['id'],)).fetchone()
        if not link:return {}
        value=self._assignment_provenance(db,link['task_id'],row['sender_session'],row['recipient_session'])
        return {'assignment_provenance':value} if value is not None else {}

    def _message_dicts(self, db: sqlite3.Connection, rows: list[sqlite3.Row]) -> list[dict[str, Any]]:
        labels = agent_labels(db)
        operators = {row['session_id'] for row in db.execute('SELECT session_id FROM operator_identities')}
        attachments = self._attachment_metadata(db, [row["id"] for row in rows])
        replies = self._reply_metadata(db, [row["id"] for row in rows])
        batches = self._batch_metadata(db, [row["id"] for row in rows])
        return [{**{key: row[key] for key in ("id", "sender_session", "body", "created_at", "acked_at")},
                 "sender_agent": labels.get(row["sender_session"]),
                 "sender_role": "human_operator" if row["sender_session"] in operators else "agent",
                 "recipient_role": "human_operator" if row["recipient_session"] in operators else "agent",
                 "instruction_origin": ('authenticated_operator_ui' if db.execute(
                     'SELECT 1 FROM operator_message_origins WHERE message_id=? AND sender_session=?',
                     (row['id'], row['sender_session'])).fetchone() else 'agent_message'),
                 "attachments": attachments[row["id"]], **replies[row["id"]], **batches[row["id"]], **self._message_assignment(db,row)} for row in rows]

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

    def inbox(self, all_messages: bool = False, observer=None) -> dict[str, Any]:
        sid = self.require_session()
        with self.tx(observer=observer) as db:
            if observer: observer.mark('inbox_query', 'start')
            try:
                rows = db.execute("SELECT id,sender_session,recipient_session,body,created_at,acked_at,seq FROM messages WHERE recipient_session=? " + ("ORDER BY seq" if all_messages else "AND NOT EXISTS (SELECT 1 FROM message_reads r WHERE r.message_id=messages.id AND r.reader_session=messages.recipient_session AND r.provenance IN ('recipient_full_message','recipient_context_full')) AND NOT (messages.acked_at IS NOT NULL AND messages.acked_at <= COALESCE(CAST((SELECT value FROM meta WHERE key='message_delivery_archive_before') AS REAL),-1)) ORDER BY seq"), (sid,)).fetchall()
                latest = db.execute("SELECT COALESCE(MAX(seq),0) n FROM messages WHERE recipient_session=?", (sid,)).fetchone()["n"]
                db.execute("UPDATE sessions SET inbox_read_seq=? WHERE id=?", (latest, sid))
                messages = self._message_dicts(db, rows)
            except BaseException:
                if observer: observer.mark('inbox_query', 'end', 'error')
                raise
            else:
                if observer: observer.mark('inbox_query', 'end')
        return {"messages": messages}

    @staticmethod
    def _canonical_bytes(value: Any) -> bytes:
        return json.dumps(value, sort_keys=True, separators=(",", ":"),
                          ensure_ascii=False).encode("utf-8")

    def context_size(self, value: Any) -> int:
        return len(self._canonical_bytes(value))

    @staticmethod
    def _context_message(message: dict[str, Any], body: str) -> dict[str, Any]:
        return {"id": message["id"], "sender_session": message["sender_session"],
                "instruction_origin": message.get('instruction_origin','agent_message'),
                "sender_role": message["sender_role"], "recipient_role": message["recipient_role"],
                "sender_agent": message["sender_agent"], "created_at": message["created_at"],
                "body": body, "body_truncated": body != message["body"],
                "attachments": message["attachments"], "reply_to": message["reply_to"],
                **({"batch_id": message["batch_id"]} if message.get("batch_id") else {}),
                **({"assignment_provenance": message["assignment_provenance"]} if message.get("assignment_provenance") else {})}

    def context(self, limit: int = 20, max_bytes: int = 12288, cursor: int | None = None,
                message_ids: list[str] | None = None, resources: list[str] | None = None,
                resource_cursor: str | None = None) -> dict[str, Any]:
        """Return a bounded, session-scoped context snapshot.

        Message bodies are previews; :meth:`message` is the only full-body read.
        A cursor is the last returned message sequence and advances in sequence order.
        """
        sid = self.require_session()
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
            raise CoordError("limit must be an integer from 1 through 100")
        if not isinstance(max_bytes, int) or isinstance(max_bytes, bool) or max_bytes < 256:
            raise CoordError("max_bytes must be an integer of at least 256")
        if cursor is not None and (not isinstance(cursor, int) or isinstance(cursor, bool) or cursor < 0):
            raise CoordError("cursor must be a nonnegative integer")
        if resource_cursor is not None and (not isinstance(resource_cursor, str) or not resource_cursor):
            raise CoordError("resource_cursor must be a nonempty string")
        if message_ids is not None and (not isinstance(message_ids, list) or
                                        any(not isinstance(value, str) or not value for value in message_ids)):
            raise CoordError("message_ids must be a list of nonempty strings")
        if resources is not None and (not isinstance(resources, list) or
                                      any(not isinstance(value, str) or not value for value in resources)):
            raise CoordError("resources must be a list of nonempty strings")
        named_resources = [] if resources is None else list(dict.fromkeys(self.resource_name(value) for value in resources))
        with self.tx() as db:
            clauses = ["recipient_session=?", "NOT EXISTS (SELECT 1 FROM message_reads r WHERE r.message_id=messages.id AND r.reader_session=messages.recipient_session AND r.provenance IN ('recipient_full_message','recipient_context_full')) AND NOT (messages.acked_at IS NOT NULL AND messages.acked_at <= COALESCE(CAST((SELECT value FROM meta WHERE key='message_delivery_archive_before') AS REAL),-1))"]
            args: list[Any] = [sid]
            if cursor is not None:
                clauses.append("seq>?")
                args.append(cursor)
            if message_ids is not None:
                if not message_ids:
                    clauses.append("0")
                else:
                    clauses.append("id IN (" + ",".join("?" for _ in message_ids) + ")")
                    args.extend(message_ids)
            rows = db.execute("SELECT id,sender_session,recipient_session,body,created_at,acked_at,seq FROM messages WHERE " +
                              " AND ".join(clauses) + " ORDER BY seq LIMIT ?", (*args, limit + 1)).fetchall()
            has_more = len(rows) > limit
            rows = rows[:limit]
            resource_terms = ["owner_session=?", "name IN (SELECT resource FROM resource_queue WHERE session=?)"]
            resource_args: list[Any] = [sid, sid]
            if named_resources:
                resource_terms.append("name IN (" + ",".join("?" for _ in named_resources) + ")")
                resource_args.extend(named_resources)
            resource_where = "(" + " OR ".join(resource_terms) + ")"
            if resource_cursor is not None:
                resource_where += " AND name>?"
                resource_args.append(resource_cursor)
            resource_rows = db.execute("SELECT * FROM resources WHERE " + resource_where + " ORDER BY name LIMIT 101",
                                       resource_args).fetchall()
            resources_has_more = len(resource_rows) > 100
            resource_rows = resource_rows[:100]
            result: dict[str, Any] = {"messages": [], "resources": [], "cursor": cursor,
                                      "has_more": has_more, "truncated": False,
                                      "resources_cursor": resource_cursor,
                                      "resources_has_more": resources_has_more}
            from .management import manager
            explicit_manager = manager(db, sid)
            if explicit_manager is not None:
                result["manager_session"] = explicit_manager
            if self.context_size(result) > max_bytes:
                raise CoordError("max_bytes is too small for context metadata")
            resource_budget = max_bytes // 3
            for row in resource_rows:
                full_resource = self._resource_status(db, row)
                candidates = [full_resource, {"resource": row["name"], "state": self._state(row),
                                               "detail_truncated": True}]
                selected = None
                for candidate_resource in candidates:
                    trial = {**result, "resources": [*result["resources"], candidate_resource],
                             "resources_cursor": row["name"], "resources_has_more": False}
                    if (self.context_size(trial) <= max_bytes and
                            self.context_size(trial["resources"]) <= resource_budget):
                        selected = candidate_resource
                        break
                if selected is None:
                    if not result["resources"]:
                        raise CoordError("max_bytes is too small for a resource context stub")
                    result["resources_has_more"] = True
                    break
                result["resources"].append(selected)
                result["resources_cursor"] = row["name"]
            if len(result["resources"]) < len(resource_rows):
                result["resources_has_more"] = True
            for row, full in zip(rows, self._message_dicts(db, rows)):
                candidate = self._context_message(full, full["body"])
                trial = {**result, "messages": [*result["messages"], candidate], "cursor": row["seq"],
                         "has_more": False, "truncated": result["truncated"] or candidate["body_truncated"]}
                if self.context_size(trial) > max_bytes:
                    # Keep the message metadata and shrink its Unicode body until its
                    # canonical UTF-8 representation fits the requested budget.
                    low, high, best = 0, len(full["body"]), None
                    while low <= high:
                        middle = (low + high) // 2
                        shortened = self._context_message(full, full["body"][:middle])
                        shortened_trial = {**result, "messages": [*result["messages"], shortened], "cursor": row["seq"],
                                           "has_more": False,
                                           "truncated": result["truncated"] or shortened["body_truncated"]}
                        if self.context_size(shortened_trial) <= max_bytes:
                            best = shortened
                            low = middle + 1
                        else:
                            high = middle - 1
                    if best is None:
                        stub = {"id": row["id"], "body": "", "body_truncated": True,
                                "metadata_truncated": True}
                        stub_trial = {**result, "messages": [*result["messages"], stub], "cursor": row["seq"],
                                      "has_more": False, "truncated": True}
                        if self.context_size(stub_trial) > max_bytes:
                            if not result["messages"]:
                                raise CoordError("max_bytes is too small for a message context stub")
                            result["has_more"] = True
                            result["truncated"] = True
                            break
                        candidate = stub
                    else:
                        candidate = best
                result["messages"].append(candidate)
                result["cursor"] = row["seq"]
                result["truncated"] = result["truncated"] or candidate["body_truncated"]
                if not candidate["body_truncated"]:
                    db.execute("""INSERT INTO message_reads(message_id,reader_session,read_at,provenance) VALUES(?,?,?,?)
                        ON CONFLICT(message_id,reader_session) DO UPDATE SET read_at=excluded.read_at,
                          provenance=excluded.provenance WHERE message_reads.provenance='legacy_unknown'""",
                               (row["id"], sid, _now(), 'recipient_context_full'))
            if len(result["messages"]) < len(rows):
                result["has_more"] = True
            return result

    def sent_message(self, message_id: str) -> dict[str, Any]:
        """Inspect an exact own sent message without recording an inbox read."""
        sid = self.require_session()
        if not isinstance(message_id, str) or not message_id:
            raise CoordError('id must be a nonempty string')
        with self.tx() as db:
            row = db.execute('SELECT id,sender_session,recipient_session,body,created_at,acked_at FROM messages WHERE id=? AND sender_session=?', (message_id, sid)).fetchone()
            if row is None:
                raise CoordError('message is not sent by caller')
            message = dict(row)
            message['attachments'] = self._attachment_metadata(db, [message_id])[message_id]
            reply = db.execute('SELECT reply_to FROM message_replies WHERE message_id=?', (message_id,)).fetchone()
            message['reply_to'] = reply['reply_to'] if reply else None
            read = db.execute('SELECT read_at,provenance FROM message_reads WHERE message_id=? AND reader_session=?', (message_id, row['recipient_session'])).fetchone()
            message['recipient_read'] = dict(read) if read else None
            message['bridge_delivery'] = {'observation': 'unavailable'}
            if all(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone() for name in ('bridge_deliveries','bridge_jobs')):
                delivery = db.execute('SELECT j.id,j.status,j.created_at,j.updated_at FROM bridge_deliveries d JOIN bridge_jobs j ON j.id=d.job_id WHERE d.message_id=?', (message_id,)).fetchone()
                message['bridge_delivery'] = {'observation': 'recorded', **dict(delivery)} if delivery else {'observation': 'not_recorded'}
            return {'message': message}

    def message(self, message_id: str) -> dict[str, Any]:
        """Read one complete message only when it belongs to this session's inbox."""
        sid = self.require_session()
        if not isinstance(message_id, str) or not message_id:
            raise CoordError("id must be a nonempty string")
        with self.tx() as db:
            row = db.execute("SELECT id,sender_session,recipient_session,body,created_at,acked_at,seq FROM messages "
                             "WHERE id=? AND recipient_session=?", (message_id, sid)).fetchone()
            if row is None:
                raise CoordError("message is not in caller inbox")
            db.execute("""INSERT INTO message_reads(message_id,reader_session,read_at,provenance) VALUES(?,?,?,?)
                ON CONFLICT(message_id,reader_session) DO UPDATE SET read_at=excluded.read_at,
                  provenance=excluded.provenance WHERE message_reads.provenance='legacy_unknown'""",
                       (message_id, sid, _now(), 'recipient_full_message'))
            return {"message": self._message_dicts(db, [row])[0]}

    def private_inbox_attachment(self, attachment_id: str, *, message_id: str | None = None,
                                 operator_sender: bool = False, _chunk_offset: int | None = None,
                                 _prefix_sha256: str | None = None) -> dict[str, Any]:
        """Return one validated direct-message attachment owned by this inbox."""
        sid = self.require_session()
        valid_id = (isinstance(attachment_id, str) and attachment_id.startswith("attachment_")
                    and len(attachment_id) > len("attachment_")
                    and all(char.isalnum() or char in "_-" for char in attachment_id))
        if not valid_id:
            raise CoordError("attachment is not in caller private inbox")
        with self.tx() as db:
            if not self._has_attachments(db):
                raise CoordError("attachment is not in caller private inbox")
            row = db.execute(
                """SELECT a.id,a.name,a.mime,a.size,a.content,m.id AS message_id,m.sender_session
                     FROM attachments a JOIN messages m ON m.id=a.message_id
                     WHERE a.id=? AND m.recipient_session=?
                       AND NOT EXISTS (SELECT 1 FROM message_batches b WHERE b.message_id=m.id)
                       AND (? IS NULL OR m.id=?)
                       AND (?=0 OR EXISTS (
                           SELECT 1 FROM operator_identities o WHERE o.session_id=m.sender_session))""",
                (attachment_id, sid, message_id, message_id, int(operator_sender)),
            ).fetchone()
            if row is None:
                raise CoordError("attachment is not in caller private inbox")
            from .attachment_transfer import published, read_chunk
            if _chunk_offset is not None:
                return read_chunk(db, row, _chunk_offset, _prefix_sha256)
            if published(db, attachment_id):
                raise CoordError('large attachment requires attachment-download --resume')
            content = bytes(row["content"])
            if (isinstance(row["size"], bool) or not isinstance(row["size"], int)
                    or row["size"] != len(content)
                    or not 0 <= len(content) <= self.attachment_size_limit(row["name"])):
                raise CoordError("stored attachment size is invalid")
            name, mime, _ = self._prepare_attachment(row["name"], content)
            if name != row["name"] or mime != row["mime"]:
                raise CoordError("stored attachment metadata is invalid")
            return {"attachment_id": row["id"], "message_id": row["message_id"],
                    "name": name, "mime": mime, "size": len(content),
                    "sha256": hashlib.sha256(content).hexdigest(), "content": content}

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

    @staticmethod
    def _attachment_name(name: str) -> str:
        name = pathlib.Path(name.replace("\\", "/")).name
        name = "".join(char if char.isprintable() and char not in "/\\" else "_" for char in name).strip(" .")
        return name[:255] or "attachment"

    @staticmethod
    def _video_mime(content: bytes) -> str | None:
        if len(content) >= 16 and content[4:8] == b'ftyp':
            size = int.from_bytes(content[:4], 'big')
            if 16 <= size <= len(content) and size % 4 == 0:
                brands = [content[8:12]] + [content[i:i+4] for i in range(16, size, 4)]
                if b'qt  ' in brands:
                    return 'video/quicktime'
                if any(brand in {b'isom', b'iso2', b'iso3', b'iso4', b'iso5', b'iso6', b'iso7',
                                 b'iso8', b'iso9', b'mp41', b'mp42', b'avc1', b'M4V ', b'M4VH', b'dash'}
                       for brand in brands):
                    return 'video/mp4'
        if content.startswith(b'\x1a\x45\xdf\xa3') and len(content) > 5:
            # Bound the DocType check to the declared EBML header, not media data.
            first = content[4]
            width = next((n for n in range(1, 9) if first & (1 << (8 - n))), 0)
            if width and len(content) >= 4 + width:
                size = int.from_bytes(content[4:4+width], 'big') & ((1 << (7 * width)) - 1)
                end = 4 + width + size
                if end <= len(content) and b'\x42\x82\x84webm' in content[4+width:end]:
                    return 'video/webm'
        return None

    @staticmethod
    def _audio_mime(content: bytes) -> str | None:
        if len(content) < 12 or content[:4] != b"RIFF" or content[8:12] != b"WAVE":
            return None
        riff_end = 8 + int.from_bytes(content[4:8], "little")
        if riff_end < 12 or riff_end != len(content):
            return None
        offset = 12
        fmt = None
        data_size = None
        while offset + 8 <= riff_end:
            chunk_id = content[offset:offset + 4]
            chunk_size = int.from_bytes(content[offset + 4:offset + 8], "little")
            start = offset + 8
            end = start + chunk_size
            if end > riff_end:
                return None
            if chunk_id == b"fmt ":
                if fmt is not None or chunk_size < 16:
                    return None
                audio_format = int.from_bytes(content[start:start + 2], "little")
                channels = int.from_bytes(content[start + 2:start + 4], "little")
                sample_rate = int.from_bytes(content[start + 4:start + 8], "little")
                byte_rate = int.from_bytes(content[start + 8:start + 12], "little")
                block_align = int.from_bytes(content[start + 12:start + 14], "little")
                bits_per_sample = int.from_bytes(content[start + 14:start + 16], "little")
                if (audio_format != 1 or channels < 1 or sample_rate < 1
                        or bits_per_sample not in (8, 16, 24, 32)):
                    return None
                expected_align = channels * (bits_per_sample // 8)
                if block_align != expected_align or byte_rate != sample_rate * expected_align:
                    return None
                fmt = (block_align,)
            elif chunk_id == b"data":
                if data_size is not None:
                    return None
                data_size = chunk_size
            offset = end + (chunk_size & 1)
            if offset > riff_end:
                return None
        if offset != riff_end or fmt is None or not data_size or data_size % fmt[0]:
            return None
        return "audio/wav"

    @staticmethod
    def attachment_size_limit(name: str) -> int:
        """Return the per-file cap for an attachment name."""
        return MAX_MODEL_ATTACHMENT_SIZE if pathlib.Path(name).suffix.lower() in MODEL_EXTENSIONS else MAX_ATTACHMENT_SIZE

    @staticmethod
    def _model_mime(extension: str, content: bytes) -> str | None:
        if extension == ".glb":
            # Validate only GLB v2 container framing. The JSON chunk remains
            # opaque; this path transfers the original file and is not an importer.
            if len(content) < 20 or content[:4] != b"glTF":
                return None
            version, declared_size = struct.unpack_from("<II", content, 4)
            if version != 2 or declared_size != len(content):
                return None
            offset = 12
            chunk_index = 0
            while offset < declared_size:
                if declared_size - offset < 8:
                    return None
                chunk_size, chunk_type = struct.unpack_from("<II", content, offset)
                start = offset + 8
                end = start + chunk_size
                if chunk_size % 4 or end > declared_size:
                    return None
                if chunk_index == 0:
                    if chunk_type != 0x4E4F534A or chunk_size == 0:  # Required JSON
                        return None
                elif chunk_type == 0x4E4F534A:
                    return None
                elif chunk_type == 0x004E4942 and chunk_index != 1:  # BIN is optional, but must be second
                    return None
                offset = end
                chunk_index += 1
            return "model/gltf-binary" if offset == declared_size and chunk_index else None
        if extension == ".fbx":
            # Support only the observed binary FBX 7400 header. Do not parse
            # node records or claim semantic/model safety.
            header = b"Kaydara FBX Binary  \x00\x1a\x00"
            if (len(content) < len(header) + 5 or not content.startswith(header)
                    or struct.unpack_from("<I", content, len(header))[0] != 7400):
                return None
            return "application/octet-stream"
        return None

    @staticmethod
    def _validate_project_header(extension: str, prefix: bytes, size: int) -> None:
        # Header identification only. Never extract, decode pixels, import,
        # execute, or claim semantic safety for these opaque original bytes.
        valid = False
        if extension == ".unitypackage":
            valid = size >= 18 and prefix[:3] == b"\x1f\x8b\x08" and len(prefix) >= 4 and not prefix[3] & 0xe0
        elif extension == ".zip":
            valid = size >= 22 and prefix[:4] in (b"PK\x03\x04", b"PK\x05\x06")
        elif extension == ".exr":
            valid = size >= 9 and prefix[:4] == b"\x76\x2f\x31\x01" and len(prefix) >= 8 and prefix[4] == 2
        elif extension == ".itrdraft":
            # No native draft schema is asserted by the transport contract.
            valid = size > 0
        if not valid:
            raise CoordError(f"attachment content does not match its {extension} header")

    @classmethod
    def _prepare_attachment(cls, name: str, content: bytes) -> tuple[str, str, bytes]:
        name = cls._attachment_name(name)
        extension = pathlib.Path(name).suffix.lower()
        mime = (IMAGE_EXTENSIONS.get(extension) or VIDEO_EXTENSIONS.get(extension)
                or AUDIO_EXTENSIONS.get(extension) or EMAIL_EXTENSIONS.get(extension)
                or MODEL_EXTENSIONS.get(extension) or PACKAGE_EXTENSIONS.get(extension) or PROJECT_EXTENSIONS.get(extension)
                or TEXT_EXTENSIONS.get(extension)
                or ("application/pdf" if extension == ".pdf" else None))
        if mime is None:
            suffix = extension if len(extension) <= 12 and extension[1:].isascii() and extension[1:].isalnum() else "[unrecognized]"
            raise CoordError(f"unsupported attachment type {suffix}; choose PNG, JPEG, GIF, WebP, PCM WAV, MP4, M4V, MOV, WebM, binary GLB v2, binary FBX 7400, Unity package, ZIP, ITR draft, EXR v2, EML, PDF, C# source, Unity metadata, patch text, C# project XML, TXT, Markdown, JSON, XML, CSV, TSV, LOG, YAML, or TOML")
        if not content and extension not in TEXT_EXTENSIONS:
            raise CoordError("attachment must be nonempty")
        size_limit = cls.attachment_size_limit(name)
        if len(content) > size_limit:
            if size_limit == MAX_MODEL_ATTACHMENT_SIZE:
                raise CoordError("GLB v2 and binary FBX 7400 model attachments may not exceed 64 MiB")
            raise CoordError("attachment exceeds 10 MiB")
        if extension in IMAGE_EXTENSIONS:
            if cls._image_mime(content) != mime:
                raise CoordError("attachment content does not match its image extension")
        elif extension in VIDEO_EXTENSIONS:
            if cls._video_mime(content) != mime:
                raise CoordError("attachment content does not match its video extension")
        elif extension in AUDIO_EXTENSIONS:
            if cls._audio_mime(content) != mime:
                raise CoordError("attachment content does not match its PCM WAV extension")
        elif extension in MODEL_EXTENSIONS:
            if cls._model_mime(extension, content) != mime:
                raise CoordError("attachment content does not match a supported binary GLB v2 or FBX 7400 header")
        elif extension in PACKAGE_EXTENSIONS or extension in PROJECT_EXTENSIONS:
            cls._validate_project_header(extension, content[:32], len(content))
        elif extension == ".pdf":
            if not content.startswith(b"%PDF-"):
                raise CoordError("attachment content does not match its PDF extension")
        elif extension == ".eml":
            # Treat exported mail as an opaque bounded file. Do not parse,
            # render, extract, or reinterpret its headers/body/embedded parts.
            pass
        else:
            # Documents are opaque UTF-8 data: never parse XML, render markup, or
            # execute configuration. Reject binary payloads and renamed scripts.
            try:
                text = content.decode("utf-8-sig")
            except UnicodeDecodeError as error:
                raise CoordError("document attachments must contain UTF-8 text") from error
            if any((ord(char) < 32 and char not in "\t\r\n") or 127 <= ord(char) < 160 for char in text):
                raise CoordError("document attachments must not contain binary or control bytes")
            if text.lstrip().startswith("#!"):
                raise CoordError("executable scripts are not allowed as attachments")
        if extension == ".jsonl" and content:
            # jq is the sole JSON parser. Discard output and errors so file
            # contents cannot appear in diagnostics; fail closed if unavailable.
            program = r'split("\n") | if .[-1] == "" then .[:-1] else . end | .[] | try (fromjson | empty) catch halt_error(1)'
            try:
                result = subprocess.run(["jq", "-R", "-s", program], input=content,
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30, check=False)
            except (OSError, subprocess.TimeoutExpired) as error:
                raise CoordError("JSONL validation requires an available jq executable") from error
            if result.returncode != 0:
                raise CoordError("JSONL attachment must contain one valid JSON value per line")
        return name, mime, content

    def _prepare_attachments(self, paths: list[str | os.PathLike[str]] | None) -> list[tuple[str, str, bytes]]:
        paths = paths or []
        if len(paths) > MAX_ATTACHMENTS:
            raise CoordError(f"at most {MAX_ATTACHMENTS} attachments are allowed")
        # Preflight before accumulating large model contents; recheck actual
        # bytes below because files may change after their size observation.
        try:
            if sum(pathlib.Path(raw).stat().st_size for raw in paths) > MAX_ATTACHMENT_TOTAL_SIZE:
                raise CoordError('attachments exceed 500 MiB total raw size')
        except OSError as error:
            raise CoordError('attachment is unreadable: ' + str(error)) from error
        prepared = []
        total = 0
        for index, raw in enumerate(paths, 1):
            path = pathlib.Path(raw)
            try:
                fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
            except OSError as error:
                raise CoordError("attachment is unreadable: " + str(error)) from error
            try:
                with os.fdopen(fd, "rb") as stream:
                    if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                        raise CoordError("attachment must be a regular file")
                    content = stream.read(self.attachment_size_limit(path.name) + 1)
            except OSError as error:
                raise CoordError("attachment is unreadable: " + str(error)) from error
            total += len(content)
            if total > MAX_ATTACHMENT_TOTAL_SIZE:
                raise CoordError('attachments exceed 500 MiB total raw size')
            try:
                prepared.append(self._prepare_attachment(path.name, content))
            except CoordError as error:
                raise CoordError(f"attachment #{index}: {error}; filename={path.name!r}; size={len(content)} bytes") from error
        return prepared

    @staticmethod
    def _reply_parent(db: sqlite3.Connection, reply_to: str, sender: str, recipient: str) -> sqlite3.Row:
        parent = db.execute("SELECT id,seq,sender_session,recipient_session,body FROM messages WHERE id=?", (reply_to,)).fetchone()
        if not parent or {parent["sender_session"], parent["recipient_session"]} != {sender, recipient}:
            raise CoordError("reply parent must belong to the same conversation")
        return parent

    def send(self, target: str, body: str, attachments: list[str | os.PathLike[str]] | None = None,
             reply_to: str | None = None, ack_reply: bool = False,
             decision_needed: bool = False, update_for: str | None = None) -> dict[str, Any]:
        return self._send_prepared(target, body, self._prepare_attachments(attachments), reply_to,
                                   ack_reply, decision_needed, update_for=update_for)

    @staticmethod
    def _manager_update_task(db, task_id, sender, recipient):
        """Validate one current PO progress update to its exact working owner."""
        required = ('project_work', 'project_orchestrators', 'agent_management')
        if any(not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone()
               for table in required):
            raise CoordError('update marker requires current project task and management records')
        roles = db.execute('SELECT session_id FROM project_orchestrators').fetchall()
        if len(roles) != 1 or roles[0]['session_id'] != sender:
            raise CoordError('update marker requires the current designated project PO sender')
        from .management import manager
        if manager(db, recipient) != sender:
            raise CoordError('update marker recipient is not currently managed by the sender')
        project = db.execute("SELECT value FROM meta WHERE key='project_id'").fetchone()
        row = db.execute('''SELECT id FROM project_work
            WHERE id=? AND kind='task' AND project_id=? AND owner_session=?
              AND assigned_by=? AND state='in_progress' ''',
            (task_id, project['value'] if project else 'default', recipient, sender)).fetchone()
        if row is None:
            raise CoordError('update marker requires the recipient\'s exact current in_progress task assigned by the sender')

    def _automatic_manager_update_task(self, db, parent, sender, recipient):
        """Return a task only for an exact PO/worker assignment conversation.

        A missing or stale association is deliberately ordinary-message fallback,
        not a new refusal or an inferred update marker.
        """
        if parent is None or {parent['sender_session'], parent['recipient_session']} != {sender, recipient}:
            return None
        activity_tables = {row['name'] for row in db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('project_orchestrators','project_work')")}
        if activity_tables != {'project_orchestrators', 'project_work'}:
            return None
        if not db.execute('SELECT 1 FROM project_orchestrators WHERE session_id=? LIMIT 1', (sender,)).fetchone():
            return None
        if self._has_batches(db) and db.execute('SELECT 1 FROM message_batches WHERE message_id=?',
                                                (parent['id'],)).fetchone():
            return None
        row = []
        if (self._has_replies(db) and parent['sender_session'] == recipient
                and parent['recipient_session'] == sender):
            row = db.execute('''SELECT w.id FROM message_replies r
                JOIN messages original ON original.id=r.reply_to
                JOIN project_work w ON w.source_request_id=r.reply_to
                WHERE r.message_id=? AND original.sender_session=? AND original.recipient_session=?
                  AND w.kind='task' AND w.owner_session=? AND w.assigned_by=?
                LIMIT 2''', (parent['id'], sender, recipient, recipient, sender)).fetchall()
        if len(row) != 1:
            row = db.execute('''SELECT u.task_id AS id FROM manager_message_updates u
                JOIN messages marked ON marked.id=u.message_id
                WHERE u.message_id=? AND marked.sender_session=? AND marked.recipient_session=?
                LIMIT 2''', (parent['id'], sender, recipient)).fetchall()
        if len(row) != 1:
            return None
        try:
            self._manager_update_task(db, row[0]['id'], sender, recipient)
        except CoordError:
            return None
        return row[0]['id']

    def _send_result(self, db: sqlite3.Connection, message_id: str, acknowledged_reply: bool = False) -> dict[str, Any]:
        row = db.execute("SELECT id,sender_session,recipient_session,body,created_at,acked_at,seq FROM messages WHERE id=?",
                         (message_id,)).fetchone()
        if row is None:
            raise CoordError("idempotent reply message is unavailable")
        attachments = self._attachment_metadata(db, [message_id])[message_id]
        reply = self._reply_metadata(db, [message_id])[message_id]
        result = {"id": row["id"], "sender_session": row["sender_session"],
                  "recipient_session": row["recipient_session"], "attachments": attachments, **reply}
        if acknowledged_reply:
            result["acknowledged_reply"] = True
        return result

    def _send_prepared(self, target: str, body: str, prepared: list[tuple[str, str, bytes]],
                       reply_to: str | None = None, ack_reply: bool = False,
                       decision_needed: bool = False, *, _large_transfer=None, assignment_id=None,
                       update_for=None) -> dict[str, Any]:
        sid = self.require_session()
        if not target: raise CoordError("recipient is required")
        if not isinstance(body, str): raise CoordError("body must be a string")
        if not isinstance(ack_reply, bool): raise CoordError("ack_reply must be a boolean")
        if not isinstance(decision_needed, bool): raise CoordError("decision_needed must be a boolean")
        if decision_needed and (reply_to is not None or ack_reply):
            raise CoordError("decision_needed requires a new direct message, not a reply")
        if ack_reply and not isinstance(reply_to, str):
            raise CoordError("ack_reply requires reply_to")
        if assignment_id is not None and (not isinstance(assignment_id,str) or not assignment_id):
            raise CoordError('assignment link requires one explicit task')
        if update_for is not None and (not isinstance(update_for, str) or not update_for):
            raise CoordError('update marker requires one explicit task')
        if update_for is not None and ack_reply:
            raise CoordError('update marker cannot be a first-ACK reply')
        if _large_transfer is not None and (prepared or ack_reply or decision_needed):
            raise CoordError('large transfer cannot mix attachments, ACK replies or decisions')
        with self.tx() as db:
            exact = db.execute("SELECT id FROM sessions WHERE id=?", (target,)).fetchone()
            # An authenticated operator can have an inbox message after its
            # sessions row is gone. Permit only the original recipient's reply
            # to that exact server-stamped operator source and identity.
            operator_parent = None
            if reply_to is not None and not exact:
                operator_parent = db.execute(
                    "SELECT m.sender_session FROM messages m "
                    "JOIN operator_message_origins o ON o.message_id=m.id "
                    "JOIN operator_identities i ON i.session_id=o.sender_session "
                    "WHERE m.id=? AND m.recipient_session=? AND m.sender_session=? "
                    "AND o.sender_session=m.sender_session AND o.channel='authenticated_operator_ui'",
                    (reply_to, sid, target)).fetchone()
            if exact: recipients = [exact["id"]]
            elif operator_parent: recipients = [operator_parent["sender_session"]]
            else:
                recipients = [r["id"] for r in db.execute("SELECT id FROM sessions WHERE agent=?", (target,))]
                if len(recipients) != 1:
                    raise CoordError("recipient label is unknown or ambiguous; use an exact session id")
            if (db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_parked_retirements'").fetchone()
                    and db.execute('SELECT 1 FROM root_parked_retirements WHERE target_session=?', (recipients[0],)).fetchone()):
                raise CoordError('parked registration was retired; new work is withheld')
            if decision_needed and recipients[0] == sid:
                raise CoordError("decision request recipient must differ from the author")
            if decision_needed and not db.execute(
                    "SELECT 1 FROM operator_identities WHERE session_id=?", (recipients[0],)).fetchone():
                raise CoordError("decision requests must target a registered operator identity")
            if update_for is not None:
                self._manager_update_task(db, update_for, sid, recipients[0])
            large = None
            if _large_transfer is not None:
                from .attachment_transfer import ready_for_send, mime_for
                transfer_id, project, host = _large_transfer
                large = ready_for_send(db, self, transfer_id, project, host,
                                       recipients[0], body, reply_to, assignment_id)
                if large['state'] == 'published':
                    return self._send_result(db, large['message_id'])
            fingerprint = None
            if ack_reply:
                fingerprint = hashlib.sha256(self._canonical_bytes({
                    "recipient_session": recipients[0], "body": body,
                    **({'assignment_id':assignment_id} if assignment_id is not None else {}),
                    "attachments": [{"name": name, "mime": mime,
                                     "sha256": hashlib.sha256(content).hexdigest()}
                                    for name, mime, content in prepared],
                })).hexdigest()
                prior = db.execute("SELECT fingerprint,message_id FROM reply_ack_idempotency WHERE sender_session=? AND reply_to=?",
                                   (sid, reply_to)).fetchone()
                if prior is not None:
                    if not secrets.compare_digest(prior["fingerprint"], fingerprint):
                        raise CoordError("reply retry payload differs from the original")
                    return self._send_result(db, prior["message_id"], True)
            if assignment_id is not None and self._assignment_provenance(db,assignment_id,sid,recipients[0]) is None:
                raise CoordError('assignment link requires current owned task and same-manager peer')
            mid = _id("message")
            parent = self._reply_parent(db, reply_to, sid, recipients[0]) if reply_to is not None else None
            if ack_reply and parent is not None and parent["recipient_session"] != sid:
                raise CoordError("ack_reply parent must be in caller inbox")
            automatic_update_for = None
            if update_for is None and not ack_reply and parent is not None:
                automatic_update_for = self._automatic_manager_update_task(db, parent, sid, recipients[0])
            db.execute("INSERT INTO messages(id,sender_session,recipient_session,body,created_at) VALUES(?,?,?,?,?)", (mid,sid,recipients[0],body,_now()))
            self._stamp_operator_origin(db, mid, sid)
            if decision_needed:
                project = db.execute("SELECT value FROM meta WHERE key='project_id'").fetchone()
                db.execute("INSERT INTO operator_decision_requests VALUES(?,?,?,?,?)",
                           (mid, sid, recipients[0], project['value'] if project else 'default', _now()))
            if parent is not None:
                db.execute("INSERT INTO message_replies(message_id,reply_to) VALUES(?,?)", (mid,reply_to))
            metadata = []
            if large is not None:
                attachment_id = _id('attachment')
                # The immutable transfer chunks are the storage. No whole-file
                # BLOB copy or Python allocation occurs at publication.
                db.execute('INSERT INTO attachments(id,message_id,name,mime,size,content) VALUES(?,?,?,?,?,?)',
                           (attachment_id, mid, large['name'], mime_for(large['name']), large['size'], b''))
                db.execute("UPDATE attachment_transfers SET state='published',message_id=?,attachment_id=? WHERE id=?",
                           (mid, attachment_id, large['id']))
                metadata.append({'id': attachment_id, 'name': large['name'], 'mime': mime_for(large['name']),
                                 'size': large['size'], 'url': '/api/attachments/' + attachment_id})
            for name, mime, content in prepared:
                attachment_id = _id("attachment")
                db.execute("INSERT INTO attachments(id,message_id,name,mime,size,content) VALUES(?,?,?,?,?,?)", (attachment_id, mid, name, mime, len(content), content))
                metadata.append({"id": attachment_id, "name": name, "mime": mime, "size": len(content), "url": "/api/attachments/" + attachment_id})
            if assignment_id is not None:
                db.execute('INSERT INTO message_assignment_context(message_id,task_id) VALUES(?,?)',(mid,assignment_id))
            marked_task = update_for if update_for is not None else automatic_update_for
            if marked_task is not None:
                db.execute('INSERT INTO manager_message_updates(message_id,task_id) VALUES(?,?)', (mid, marked_task))
            if ack_reply:
                db.execute("UPDATE messages SET acked_at=COALESCE(acked_at,?) WHERE id=?", (_now(), reply_to))
                db.execute("INSERT INTO reply_ack_idempotency(sender_session,reply_to,fingerprint,message_id) VALUES(?,?,?,?)",
                           (sid, reply_to, fingerprint, mid))
        preview = None if parent is None else {"id": parent["id"], "seq": parent["seq"],
                                               "sender_session": parent["sender_session"],
                                               "recipient_session": parent["recipient_session"],
                                               "sender_agent": None, "body": parent["body"][:240]}
        result = {"id":mid,"sender_session":sid,"recipient_session":recipients[0],"attachments":metadata,
                  "reply_to": reply_to, "reply_preview": preview}
        if ack_reply:
            result["acknowledged_reply"] = True
        return result

    def mark_decision_needed(self, message_id: str) -> dict[str, Any]:
        """Mark one exact existing message authored by this caller as a decision request."""
        sid = self.require_session()
        if not isinstance(message_id, str) or not message_id.startswith('message_'):
            raise CoordError('message id is required')
        with self.tx() as db:
            row = db.execute('SELECT id,sender_session,recipient_session FROM messages WHERE id=?',
                             (message_id,)).fetchone()
            if row is None or row['sender_session'] != sid:
                raise CoordError('decision request must be authored by the caller')
            if not db.execute('SELECT 1 FROM operator_identities WHERE session_id=?',
                              (row['recipient_session'],)).fetchone():
                raise CoordError('decision request must target a registered operator identity')
            if not db.execute('SELECT 1 FROM sessions WHERE id=?', (row['recipient_session'],)).fetchone():
                raise CoordError('decision recipient is not currently registered')
            if self._has_batches(db) and db.execute(
                    'SELECT 1 FROM message_batches WHERE message_id=?', (message_id,)).fetchone():
                raise CoordError('decision request must be a direct message, not a batch delivery')
            project = db.execute("SELECT value FROM meta WHERE key='project_id'").fetchone()
            project_id = project['value'] if project else 'default'
            prior = db.execute('SELECT author_session,recipient_session,project_id FROM operator_decision_requests WHERE message_id=?',
                               (message_id,)).fetchone()
            if prior:
                if (prior['author_session'], prior['recipient_session'], prior['project_id']) != (sid, row['recipient_session'], project_id):
                    raise CoordError('existing decision annotation does not match this message context')
                return {'message_id': message_id, 'decision_needed': True, 'created': False}
            db.execute('INSERT INTO operator_decision_requests VALUES(?,?,?,?,?)',
                       (message_id, sid, row['recipient_session'], project_id, _now()))
        return {'message_id': message_id, 'decision_needed': True, 'created': True}

    def dismiss_decision(self, message_id: str, reason: str) -> dict[str, Any]:
        """Hide an authored reminder while preserving its decision history."""
        sid = self.require_session()
        if not isinstance(message_id, str) or not isinstance(reason, str) or not reason.strip() or len(reason) > 1000:
            raise CoordError('message id and a non-empty reason are required')
        with self.tx() as db:
            original = db.execute('SELECT author_session FROM operator_decision_requests WHERE message_id=?', (message_id,)).fetchone()
            if not original or original['author_session'] != sid:
                raise CoordError('only the original message author can dismiss its reminder')
            request = _decision_request(db, message_id)
            prior = db.execute('SELECT 1 FROM operator_decision_dismissals WHERE message_id=?', (message_id,)).fetchone()
            if prior:
                return {'message_id': message_id, 'dismissed': False}
            db.execute('INSERT INTO operator_decision_dismissals VALUES(?,?,?,?,?,?)',
                       (message_id, sid, request['recipient_session'], request['project_id'], _now(), reason.strip()))
        return {'message_id': message_id, 'dismissed': True}

    def record_decision_answer(self, message_id: str, reply_message_id: str,
                               outcome: str) -> dict[str, Any]:
        """Author-record an exact recipient reply without asserting task completion."""
        sid = self.require_session()
        outcomes = {'accepted', 'declined', 'conditional', 'clarification'}
        if not isinstance(message_id, str) or not isinstance(reply_message_id, str):
            raise CoordError('message and reply ids must be strings')
        if not isinstance(outcome, str) or outcome not in outcomes:
            raise CoordError('outcome must be accepted, declined, conditional, or clarification')
        with self.tx() as db:
            original = db.execute('SELECT author_session FROM operator_decision_requests WHERE message_id=?', (message_id,)).fetchone()
            if not original or original['author_session'] != sid:
                raise CoordError('only the original message author can record its decision answer')
            request = _decision_request(db, message_id)
            history = _decision_history(db, request)
            for prior in history:
                if (prior['reply_message_id'], prior['recorded_by'], prior['outcome']) == (reply_message_id, sid, outcome):
                    return {'message_id': message_id, 'reply_message_id': reply_message_id,
                            'outcome': outcome, 'recorded': False}
            path = _decision_path(db, request, reply_message_id)
            classification = bool(len(history) == 1 and _decision_legacy_classification(
                history[0], dict(legacy=False, outcome=outcome, reply_message_id=reply_message_id), path))
            if history and not (classification or _decision_progresses(history[-1], path)):
                raise CoordError('decision answer was already recorded differently or is out of order')
            if db.execute('''SELECT 1 FROM operator_decision_answers WHERE reply_message_id=? AND message_id!=?
                UNION ALL SELECT 1 FROM operator_decision_events WHERE reply_message_id=? AND message_id!=? LIMIT 1''',
                (reply_message_id,message_id,reply_message_id,message_id)).fetchone():
                raise CoordError('reply was already recorded for another decision')
            current = decision_projection(db, message_id)
            if (current['ancestry_limited'] or current['reply_message_id'] != reply_message_id
                    or (classification and current['latest_reply_message_id'] != reply_message_id)):
                raise CoordError('decision answer is not the latest supported rooted reply')
            if len(history) >= DECISION_MAX_DEPTH:
                raise CoordError('decision event history exceeds its bounded dialogue')
            db.execute('INSERT INTO operator_decision_events(message_id,reply_message_id,recorded_by,outcome,recorded_at) VALUES(?,?,?,?,?)',
                       (message_id, reply_message_id, sid, outcome, _now()))
        return {'message_id': message_id, 'reply_message_id': reply_message_id,
                'outcome': outcome, 'recorded': True}

    def publication_plan_create(self, target, fields):
        from .publication_plan import from_fields, decision_body
        try:
            body = decision_body(from_fields(fields))
        except (ValueError, TypeError) as error:
            raise CoordError(str(error)) from error
        return self.send(target, body, decision_needed=True)

    def publication_plan_status(self, message_id, fields):
        from .publication_plan import from_fields, decision_state
        try:
            plan = from_fields(fields)
        except (ValueError, TypeError) as error:
            raise CoordError(str(error)) from error
        state = decision_state(self, message_id, plan)
        return {'message_id': message_id, 'state': state,
                'classification_provenance': 'author_recorded' if state in ('accepted','declined') else 'unknown',
                'verified_consent': 'unknown'}

    def decision_diagnostic(self, message_id: str, dependency: str) -> dict[str, Any]:
        """Describe one supported integration boundary without granting an action."""
        if dependency != 'retained-child-review-event':
            raise CoordError('unsupported diagnostic dependency')
        status = self.decision_status(message_id)
        original = self.sent_message(message_id)['message']
        outcome = status.get('answer_outcome')
        origin = status.get('answer_origin')
        return {
            'scope': {'decision_id': message_id, 'author_session': self.require_session(),
                      'recipient_session': status['recipient_session'], 'request_body': original['body']},
            'author_classification': {'outcome': outcome if outcome else 'unknown'},
            'operator_reply': {'provenance': origin if origin else 'unknown',
                               'reply_id': status.get('reply_message_id') if outcome else None},
            'verified_consent': {'state': 'unknown', 'expiration': 'unknown'},
            'platform_dependency': {'capability': dependency, 'state': 'unavailable',
                                    'reason': 'this integration has no supported lookup for a retained parent-owned child review event'},
            'review_outcome': {'state': 'unknown', 'request_id': None, 'parent_id': None,
                               'child_id': None, 'event_id': None},
            'next_action': 'Use the supported parent review workflow when the exact retained event is available; preserve any existing refusal. No action is authorized or retried by this diagnostic.',
        }

    def decision_status(self, message_id: str) -> dict[str, Any]:
        """Read one original-author decision, without inbox or outcome mutation."""
        sid = self.require_session()
        if not isinstance(message_id, str):
            raise CoordError('message id must be a string')
        self.db.execute('BEGIN')
        try:
            row = self.db.execute('SELECT author_session,recipient_session FROM operator_decision_requests WHERE message_id=?', (message_id,)).fetchone()
            if not row or row['author_session'] != sid:
                raise CoordError('decision is unavailable to this author')
            decision = decision_projection(self.db, message_id, include_latest=True)
            return dict(message_id=message_id, recipient_session=row['recipient_session'],
                        recorded_by=sid if decision['answer_outcome'] else None,
                        terminal=decision['status'] == 'answer_recorded', **decision)
        finally:
            self.db.rollback()

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

    def send_many(self, targets: list[str], body: str, reply_to: str | None = None,
                  attachments: list[str | os.PathLike[str]] | None = None) -> list[dict[str, Any]]:
        """Atomically create one delivery per distinct resolved recipient."""
        if not isinstance(targets, list):
            raise CoordError("recipients must be a list")
        return self._send_many_prepared(targets, body, self._prepare_attachments(attachments), reply_to)

    def send_group_prepared(self, targets: list[str] | None, body: str,
                            prepared: list[tuple[str, str, bytes]],
                            reply_to: str | None = None) -> list[dict[str, Any]]:
        """Create an explicit group, or snapshot every other registered session."""
        if targets is not None and not isinstance(targets, list):
            raise CoordError("recipients must be a list")
        if targets is None and reply_to is not None:
            raise CoordError("reply_to requires an explicit recipient group")
        return self._send_many_prepared(targets, body, prepared, reply_to)

    def _send_many_prepared(self, targets: list[str] | None, body: str,
                            prepared: list[tuple[str, str, bytes]],
                            reply_to: str | None = None) -> list[dict[str, Any]]:
        sid = self.require_session()
        if not isinstance(body, str):
            raise CoordError("body must be a string")
        with self.tx() as db:
            if targets is None:
                recipients = [row["id"] for row in db.execute(
                    "SELECT id FROM sessions WHERE id<>? ORDER BY registered_at,id", (sid,))]
                if __package__:
                    from .root_orchestrator import quiet
                    recipients = [recipient for recipient in recipients if not quiet(db, recipient)]
                if not recipients:
                    raise CoordError("no other registered sessions are available")
            else:
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
                self._stamp_operator_origin(db, message_id, sid)
                db.execute("INSERT INTO message_batches(message_id,batch_id) VALUES(?,?)", (message_id, batch_id))
                # Explicit recipients request attention; an unaddressed broadcast
                # remains available in context without starting a model turn.
                if targets is not None:
                    db.execute("INSERT INTO message_attention(message_id) VALUES(?)", (message_id,))
                parent = parents[recipient]
                if parent is not None:
                    db.execute("INSERT INTO message_replies(message_id,reply_to) VALUES(?,?)", (message_id, parent["id"]))
                metadata = []
                for name, mime, content in prepared:
                    attachment_id = _id("attachment")
                    db.execute("INSERT INTO attachments(id,message_id,name,mime,size,content) VALUES(?,?,?,?,?,?)",
                               (attachment_id, message_id, name, mime, len(content), content))
                    metadata.append({"id": attachment_id, "name": name, "mime": mime,
                                     "size": len(content), "url": "/api/attachments/" + attachment_id})
                results.append({"id": message_id, "sender_session": sid, "recipient_session": recipient,
                                "attachments": metadata, "reply_to": None if parent is None else parent["id"],
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
            r = db.execute("SELECT recipient_session,acked_at FROM messages WHERE id=?", (message_id,)).fetchone()
            if not r or r["recipient_session"] != sid: raise CoordError("message is not in caller inbox")
            if r["acked_at"] is None:
                acknowledged_at = _now()
                db.execute("UPDATE messages SET acked_at=? WHERE id=?", (acknowledged_at,message_id))
                db.execute("INSERT INTO standalone_message_acknowledgements(message_id,recipient_session,acknowledged_at) VALUES(?,?,?)", (message_id,sid,acknowledged_at))
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
            self._latch_expiry(db, r)
            r=db.execute("SELECT * FROM resources WHERE name=?",(resource,)).fetchone()
        return r

    def _latch_expiry(self, db: sqlite3.Connection, row: sqlite3.Row) -> None:
        observed = getattr(self, '_tx_observed_expiries', None)
        if observed is not None:
            observed.append(tuple(row[key] for key in
                                  ('name', 'owner_session', 'reservation_id', 'token', 'deadline')))
        db.execute("UPDATE resources SET stale=1 WHERE name=?", (row['name'],))

    @staticmethod
    def _state(r: sqlite3.Row | None) -> str:
        if not r or not r["owner_session"]: return "free"
        return "stale" if r["stale"] or r["deadline"] < _now() else "owned"

    def _acquire(self, db: sqlite3.Connection, resource: str, sid: str, minutes: float, reason: str | None = None) -> sqlite3.Row:
        rid, token, now = _id("reservation"), _id("token"), _now()
        db.execute("INSERT INTO resources(name,owner_session,reservation_id,token,granted_at,deadline,stale,reason) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(name) DO UPDATE SET owner_session=excluded.owner_session,reservation_id=excluded.reservation_id,token=excluded.token,granted_at=excluded.granted_at,deadline=excluded.deadline,stale=0,reason=excluded.reason", (resource,sid,rid,token,now,now+minutes*60,0,reason))
        if _epoch_exists(db):
            epoch=db.execute('SELECT request_id FROM execution_epochs WHERE resource=? ORDER BY rowid DESC LIMIT 1',(resource,)).fetchone()
            if not epoch:
                epoch=db.execute('SELECT e.request_id FROM execution_epochs e JOIN execution_epoch_prepares p ON p.id=e.request_id WHERE p.workspace=? ORDER BY e.rowid DESC LIMIT 1',(resource,)).fetchone()
            if epoch: db.execute('INSERT INTO execution_epoch_grants VALUES(?,?)',(rid,epoch['request_id']))
        return db.execute("SELECT * FROM resources WHERE name=?", (resource,)).fetchone()

    def request(self, raw: str, minutes: float, on_acquire: Any = None, *, registry_db=None, registry_check=None, successor_native=None, new_only=False, on_currentness=None) -> dict[str, Any]:
        if __package__:
            from .root_orchestrator import projection
            if projection(self.db, self.require_session()):
                raise CoordError('global root is control-only; delegate exact resource work to the project orchestrator')
        if not math.isfinite(minutes) or minutes <= 0 or minutes > float.fromhex('0x1.fffffffffffffp+1023') / 60: raise CoordError("minutes must be positive and finite")
        sid, resource = self.require_session(), self.resource_name(raw)
        with self.tx() as db:
            assert_execution_leaf_acquisition(db,resource)
            try:
                self._fresh(db,sid)
            except InboxCurrentnessRequired:
                if on_currentness is not None: on_currentness(db, resource, sid)
                raise
            r = self._row(db,resource)
            from .hospital_vendor_scope import CONTROL as vendor_control
            if resource == vendor_control:
                from .hospital_vendor_binding import authorize_control_in_transaction, Refused as VendorRefused
                from .hospital_vendor_scope import Refused as VendorScopeRefused
                from .capacity_admission import AdmissionRefused as VendorAuthorityRefused
                owner_host=db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?',(sid,)).fetchone()
                if r and r['owner_session']:
                    raise CoordError('vendor control credential already issued; reconcile before retry')
                try:
                    authorize_control_in_transaction(db,sid,owner_host['host_id'] if owner_host else None,registry_db)
                except (VendorRefused,VendorScopeRefused,VendorAuthorityRefused) as error:
                    raise CoordError('vendor control refused') from error
            if r and r["owner_session"] == sid:
                if new_only: raise CoordError('deferred request requires a new reservation')
                st=self._state(r)
                if st == "stale":
                    if not r['stale']:
                        self._latch_expiry(db, r)
                    raise CoordError("your reservation is stale; release or recover it with a receipt")
                return self._reservation_result(r, "owned", 0)
            assert_agent_running(db, sid)
            first = db.execute("SELECT session FROM resource_queue WHERE resource=? ORDER BY seq LIMIT 1", (resource,)).fetchone()
            if (not r or not r["owner_session"]) and (not first or first["session"] == sid):
                from .capacity_admission import (effective_open_guards, consume_for_request,
                                                 request_admission_is_current)
                from .successor_admission import current as successor_current, consume as consume_successor
                successor = successor_current(db, resource, sid, registry_db, successor_native)
                if successor is False:
                    raise CoordError("pending successor admission is not current or requester differs")
                if request_admission_is_current(db, resource, registry_db) is False:
                    raise CoordError("pending Root admission binding/generation is no longer current")
                blockers = [] if successor is True else effective_open_guards(db, resource, registry_db)
                if blockers:
                    raise CoordError("fresh reservation withheld by unresolved open guarded run(s)")
                db.execute("DELETE FROM resource_queue WHERE resource=? AND session=?", (resource,sid))
                acquired = self._acquire(db,resource,sid,minutes)
                if resource == vendor_control:
                    from .hospital_vendor_binding import activate_control_in_transaction
                    try:
                        activate_control_in_transaction(db,sid,owner_host['host_id'],acquired,registry_db)
                    except (VendorRefused,VendorScopeRefused,VendorAuthorityRefused) as error:
                        raise CoordError('vendor control refused') from error
                if successor is True:
                    consume_successor(db, resource, sid, acquired["reservation_id"], registry_db, successor_native)
                else:
                    consume_for_request(db, resource, sid, acquired["reservation_id"], registry_db)
                if on_acquire is not None:
                    on_acquire(db, acquired)
                if registry_check is not None:
                    registry_check()
                return self._reservation_result(acquired,"owned",0)
            if new_only: raise CoordError('deferred reservation unavailable; no queue entry created')
            db.execute("INSERT OR IGNORE INTO resource_queue(resource,session,queued_at) VALUES(?,?,?)", (resource,sid,_now()))
            pos=db.execute("SELECT COUNT(*) n FROM resource_queue WHERE resource=? AND seq <= (SELECT seq FROM resource_queue WHERE resource=? AND session=?)", (resource,resource,sid)).fetchone()["n"]
            return {"state":"queued","resource":resource,"reservation_id":None,"token":None,"deadline":None,"queue_position":pos}

    def renew(self, raw: str, minutes: float, reservation_id: str, token: str,
              *, host_id: str) -> dict[str, Any]:
        """Explicitly extend one live owner lease without changing its grant."""
        if (isinstance(minutes, bool) or not isinstance(minutes, (int, float))
                or not 1 <= minutes <= 30 or not math.isfinite(minutes)):
            raise CoordError('renewal minutes must be finite and between 1 and 30')
        if any(not isinstance(value, str) or not value for value in (reservation_id, token, host_id)):
            raise CoordError('renewal requires an exact reservation, token and registered host')
        sid, resource = self.require_session(), self.resource_name(raw)
        if __package__:
            from .root_orchestrator import projection
            if projection(self.db, sid):
                raise CoordError('global root is control-only; delegate exact resource work to the project orchestrator')
        # Preserve the ordinary irreversible observation of actual expiry.
        with self.tx() as db:
            self._fresh(db, sid)
            assert_agent_running(db, sid)
            if __package__:
                from .root_team_reset import quarantined
                if quarantined(db, session=sid):
                    raise CoordError('session is quarantined by team reset')
            if db.execute('SELECT 1 FROM retired_sessions WHERE id=?', (sid,)).fetchone():
                raise CoordError('session is retired')
            if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='remote_session_hosts'").fetchone():
                raise CoordError('renewal requires the existing registered host binding')
            host = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (sid,)).fetchone()
            if not host or host['host_id'] != host_id:
                raise CoordError('renewal registered host binding differs')
            row = self._verify_owner(db, resource, sid, token)
            if row['reservation_id'] != reservation_id:
                raise CoordError('renewal reservation_id does not match')
            remaining = row['deadline'] - _now()
            if not 0 < remaining <= 600:
                if remaining < 0:
                    self._latch_expiry(db, row)
                raise CoordError('renewal requires an unexpired lease with at most 10 minutes remaining')
            if db.execute('SELECT 1 FROM resource_queue WHERE resource=? LIMIT 1', (resource,)).fetchone():
                raise CoordError('renewal refused while a FIFO claimant is queued')
            # Local guards have no registered host binding. Do not infer one
            # from their PIDs or turn an extension into closure evidence.
            if db.execute('SELECT 1 FROM guarded_runs WHERE closed_at IS NULL AND (resource=? OR reservation_id=?)',
                          (resource, reservation_id)).fetchone():
                raise CoordError('renewal refused for an unbound local guarded run')
            if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='remote_guarded_runs'").fetchone():
                guards = db.execute('SELECT * FROM remote_guarded_runs WHERE closed_at IS NULL AND (resource=? OR reservation_id=?)',
                                    (resource, reservation_id)).fetchall()
                if any((guard['resource'], guard['reservation_id'], guard['session_id'], guard['host_id']) !=
                       (resource, reservation_id, sid, host_id) for guard in guards):
                    raise CoordError('renewal refused for a conflicting guarded-run binding')
            used = db.execute('SELECT successes,extra_minutes FROM reservation_renewals WHERE reservation_id=?',
                              (reservation_id,)).fetchone()
            count, extra = (used['successes'], used['extra_minutes']) if used else (0, 0)
            if count >= 4 or extra + minutes > 120:
                raise CoordError('reservation renewal budget is exhausted')
            db.execute('UPDATE resources SET deadline=deadline+? WHERE name=?', (minutes * 60, resource))
            db.execute('INSERT INTO reservation_renewals VALUES(?,?,?) ON CONFLICT(reservation_id) DO UPDATE '
                       'SET successes=excluded.successes,extra_minutes=excluded.extra_minutes',
                       (reservation_id, count + 1, extra + minutes))
            renewed = db.execute('SELECT * FROM resources WHERE name=?', (resource,)).fetchone()
            return dict(self._reservation_result(renewed, 'owned', 0),
                        renewal_count=count + 1, renewal_minutes=extra + minutes)

    def _reservation_result(self,r: sqlite3.Row,state:str,pos:int)->dict[str,Any]:
        result={"state":state,"resource":r["name"],"reservation_id":r["reservation_id"],"token":r["token"],"deadline":r["deadline"],"queue_position":pos}
        if self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='deferred_reservations'").fetchone():
            deferred=self.db.execute("SELECT state FROM deferred_reservations WHERE reservation_id=?",(r['reservation_id'],)).fetchone()
            if deferred and deferred['state']=='deferred': result['token']=None
        if _epoch_exists(self.db):
            epoch=self.db.execute('SELECT e.request_id,e.backing,p.workspace FROM execution_epochs e JOIN execution_epoch_prepares p ON p.id=e.request_id WHERE e.resource=? OR p.workspace=? ORDER BY e.rowid DESC LIMIT 1',(r['name'],r['name'])).fetchone()
            if epoch: result['current_execution_epoch']=dict(id=epoch['request_id'],backing_identity=epoch['backing'],workspace_resource=epoch['workspace'],old_physical_state='UNVERIFIED',coverage='App-managed scope; external/shared domains excluded')
        return result

    def _resource_status(self, db: sqlite3.Connection, r: sqlite3.Row) -> dict[str, Any]:
        state = self._state(r)
        if state == 'stale' and not r['stale']:
            self._latch_expiry(db, r)
        owner = None
        if r["owner_session"]:
            s = db.execute("SELECT agent FROM sessions WHERE id=?", (r["owner_session"],)).fetchone()
            owner = s["agent"] if s else None
        q = [dict(session=x["session"], agent=x["agent"], position=i + 1) for i, x in enumerate(
            db.execute("SELECT q.session,s.agent FROM resource_queue q JOIN sessions s ON s.id=q.session "
                       "WHERE q.resource=? ORDER BY q.seq", (r["name"],)).fetchall())]
        return {"resource": r["name"], "owner_session": r["owner_session"], "owner_agent": owner,
                "state": state, "reservation_id": r["reservation_id"], "deadline": r["deadline"],
                "reason": r["reason"], "queue": q}

    def retained_reservation_result(self, db: sqlite3.Connection, resource: str,
                                    reservation_id: str) -> dict[str, Any]:
        """Report only an exact reservation that the caller still owns.

        Receipt insertion and resource clearing share one transaction, so a
        retained matching row with no receipt proves that this reservation has
        not committed a receipt-backed clear.  Historical receipt data is never
        returned: once the hold is cleared its owner binding is gone.
        """
        sid = self.require_session()
        r = self._row(db, resource)
        if (not r or r["owner_session"] != sid or r["reservation_id"] != reservation_id
                or self._state(r) != "owned"
                or db.execute("SELECT 1 FROM receipts WHERE reservation_id=?", (reservation_id,)).fetchone()):
            return {"state": "unknown"}
        return {"state": "retained", "resource": resource, "reservation_id": reservation_id,
                "receipt_backed_clear": False}

    def status(self, mine: bool = False, resources: list[str] | None = None) -> dict[str, Any]:
        self.require_session()
        if not isinstance(mine, bool): raise CoordError("mine must be a boolean")
        if resources is not None and (not isinstance(resources, list) or
                                      any(not isinstance(value, str) or not value for value in resources)):
            raise CoordError("resources must be a list of nonempty strings")
        named = [] if resources is None else list(dict.fromkeys(self.resource_name(value) for value in resources))
        with self.tx() as db:
            where, args = "", []
            if mine:
                where = " WHERE (owner_session=? OR name IN (SELECT resource FROM resource_queue WHERE session=?))"
                args.extend([self.session, self.session])
            if resources is not None and not named:
                where += (" AND " if where else " WHERE ") + "0"
            elif named:
                where += (" AND " if where else " WHERE ") + "name IN (" + ",".join("?" for _ in named) + ")"
                args.extend(named)
            rows = db.execute("SELECT * FROM resources" + where + " ORDER BY name", args).fetchall()
            out = [self._resource_status(db, r) for r in rows]
            if mine or resources is not None:
                return {"resources": out}
            sessions = [dict(session=x["id"], agent=x["agent"], registered_at=x["registered_at"])
                        for x in db.execute("SELECT id,agent,registered_at FROM sessions ORDER BY registered_at,id")]
        return {"resources": out, "sessions": sessions}

    def cancel(self, raw:str)->dict[str,Any]:
        sid,resource=self.require_session(),self.resource_name(raw)
        with self.tx() as db:
            self._fresh(db,sid); n=db.execute("DELETE FROM resource_queue WHERE resource=? AND session=?",(resource,sid)).rowcount
        return {"resource":resource,"cancelled":bool(n)}

    def _verify_owner(self,db:sqlite3.Connection,resource:str,sid:str,token:str, allow_stale:bool=False)->sqlite3.Row:
        r=self._row(db,resource)
        if not r or r["owner_session"]!=sid or not secrets.compare_digest(r["token"] or "",token or ""): raise CoordError("caller does not hold this reservation token")
        if not allow_stale and self._state(r)!="owned":
            if not r['stale']:
                # _state's clock read may be the first actual expiry observation.
                self._latch_expiry(db, r)
            raise CoordError("reservation is stale")
        execution_epoch_guard_check(db,resource,r['reservation_id'])
        return r

    def check(self,raw:str,token:str,*,require_fresh:bool=True)->dict[str,Any]:
        sid,resource=self.require_session(),self.resource_name(raw)
        with self.tx() as db:
            # Active pulses retain ownership; messages arriving after an inbox
            # poll must not invalidate an otherwise live reservation.
            if require_fresh: self._fresh(db,sid)
            r=self._verify_owner(db,resource,sid,token)
        return {"state":"owned","resource":resource,"reservation_id":r["reservation_id"],"deadline":r["deadline"]}

    def _validate_receipt(self,path:str,r:sqlite3.Row)->tuple[str,str,str,dict]:
        try: data=json.loads(pathlib.Path(path).read_text(encoding="utf-8"))
        except Exception as e: raise CoordError("receipt is unreadable JSON: "+str(e))
        try:
            proofs = process_closure_proofs(data)
            read_process_evidence(proofs, path)
        except (ValueError, OSError) as error: raise CoordError(str(error)) from error
        if data["resource"]!=r["name"] or data["reservation_id"]!=r["reservation_id"] or data["restored"] is not True or data["processes_closed"] is not True: raise CoordError("receipt does not bind this restored reservation")
        if not isinstance(data["closed_at"],(int,float)) or isinstance(data["closed_at"],bool) or not r["granted_at"]<=data["closed_at"]<=_now(): raise CoordError("receipt closed_at is invalid")
        if not isinstance(data["evidence"],str) or not data["evidence"].strip() or not isinstance(data["pids"],list) or any(not isinstance(p,int) or isinstance(p,bool) or p<=0 for p in data["pids"]): raise CoordError("receipt evidence or pids is invalid")
        evidence = pathlib.Path(data["evidence"])
        if not evidence.is_absolute(): evidence = pathlib.Path(path).resolve().parent / evidence
        try:
            evidence_bytes = evidence.read_bytes()
        except OSError as e: raise CoordError("receipt evidence report is unavailable: " + str(e))
        if not evidence_bytes: raise CoordError("receipt evidence report is empty")
        for pid in data["pids"]:
            if receipt_pid_alive(pid, proofs.get(pid, data)["closed_at"]):
                raise CoordError("receipt lists a still-live PID (or its start time cannot prove PID reuse)")
        canonical=json.dumps(data,sort_keys=True,separators=(",",":")); return canonical,hashlib.sha256(canonical.encode()).hexdigest(),hashlib.sha256(evidence_bytes).hexdigest(),proofs

    @staticmethod
    def _alive(pid:int, group:bool=False)->bool:
        try:
            (os.killpg if group else os.kill)(pid,0); return True
        except ProcessLookupError: return False
        except PermissionError: return True

    def _runs_closed(self,db:sqlite3.Connection,resource:str,allow_dead:bool,attested_closed_at:float,proofs:dict)->None:
        runs=db.execute("SELECT run_id,pid,pgid,started_at FROM guarded_runs WHERE resource=? AND closed_at IS NULL",(resource,)).fetchall()
        closure = lambda r: proofs.get(r["pid"], {"closed_at": attested_closed_at})["closed_at"]
        if any(closure(r) < r["started_at"] for r in runs): raise CoordError("process closure proof predates an unclosed guarded run")
        live=[r for r in runs if receipt_pid_alive(r["pid"], closure(r)) or (r["pgid"] is not None and self._alive(r["pgid"],True))]
        if live or (runs and not allow_dead): raise CoordError("guarded run remains open; it must close before release or recovery")
        if any(attested_closed_at < r["started_at"] for r in runs): raise CoordError("receipt predates an unclosed guarded run")
        if runs: db.executemany("UPDATE guarded_runs SET closed_at=? WHERE run_id=?",[(closure(r),r["run_id"]) for r in runs])

    def _clear_with_receipt(self,db:sqlite3.Connection,r:sqlite3.Row,path:str,action:str)->None:
        if self._remote_bound(db, r["owner_session"]):
            raise CoordError("remote reservation requires receipt proof from its owning host via --server")
        assert_execution_domain_return(db,r['name'])
        canonical,digest,evidence_digest,proofs=self._validate_receipt(path,r)
        attested=json.loads(canonical)["closed_at"]
        assert_execution_domain_return(db,r['name'],attested)
        latest=db.execute("SELECT MAX(closed_at) x FROM guarded_runs WHERE reservation_id=?",(r["reservation_id"],)).fetchone()["x"]
        if latest is not None and attested < latest: raise CoordError("receipt predates the most recent guarded run closure")
        self._runs_closed(db,r["name"],self._state(r)=="stale",attested,proofs)
        if db.execute("SELECT 1 FROM receipts WHERE reservation_id=? OR receipt_sha256=?",(r["reservation_id"],digest)).fetchone(): raise CoordError("receipt has already been used")
        db.execute("INSERT INTO receipts(reservation_id,receipt_sha256,evidence_sha256,receipt_json,action,recorded_at) VALUES(?,?,?,?,?,?)",(r["reservation_id"],digest,evidence_digest,canonical,action,_now()))
        db.execute("UPDATE resources SET owner_session=NULL,reservation_id=NULL,token=NULL,granted_at=NULL,deadline=NULL,stale=0,reason=NULL WHERE name=?",(r["name"],))

    def resource_uncertainty(self, raw, reservation_id, token, host_id, request_id, *, reason=None, record=False):
        """Record or read uncertainty without releasing the reservation."""
        import uuid
        if any(not isinstance(v,str) or not v for v in (reservation_id,token,host_id,request_id)):
            raise CoordError('uncertainty requires exact reservation, token, host and request ID')
        try:
            if str(uuid.UUID(request_id)) != request_id: raise ValueError
        except (ValueError,AttributeError):
            raise CoordError('uncertainty request ID must be a canonical UUID') from None
        if record and (not isinstance(reason,str) or not reason.strip() or len(reason.encode('utf-8')) > 512):
            raise CoordError('uncertainty reason must be nonempty and at most 512 bytes')
        if record and token in reason:
            raise CoordError('uncertainty reason must not contain the capability token')
        sid,resource=self.require_session(),self.resource_name(raw)
        with self.tx() as db:
            if record: self._fresh(db,sid)
            held=self._verify_owner(db,resource,sid,token,True)
            if held['reservation_id'] != reservation_id:
                raise CoordError('uncertainty reservation does not match current hold')
            bound=db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?',(sid,)).fetchone()
            if not bound or bound['host_id'] != host_id:
                raise CoordError('uncertainty requires the owning host')
            def counts(table):
                if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(table,)).fetchone():
                    return None
                row=db.execute(f'SELECT COUNT(*) total, SUM(CASE WHEN closed_at IS NULL THEN 1 ELSE 0 END) open FROM {table} WHERE reservation_id=?',(reservation_id,)).fetchone()
                return {'total':row['total'],'open':row['open'] or 0}
            prior=db.execute('SELECT * FROM resource_uncertainty_dispositions WHERE request_id=?',(request_id,)).fetchone()
            if prior:
                if (prior['resource'],prior['reservation_id'],prior['owner_session'],prior['host_id']) != (resource,reservation_id,sid,host_id):
                    raise CoordError('uncertainty request identity conflicts')
                if record and prior['reason'] != reason:
                    raise CoordError('uncertainty request reason conflicts')
            elif record:
                local,remote=counts('guarded_runs'),counts('remote_guarded_runs')
                db.execute('''INSERT INTO resource_uncertainty_dispositions
                    (request_id,resource,reservation_id,owner_session,host_id,reason,recorded_at,
                     observed_resource_state,local_runs_total,local_runs_open,remote_runs_total,remote_runs_open)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''',
                    (request_id,resource,reservation_id,sid,host_id,reason,_now(),self._state(held),
                     local['total'] if local is not None else None,local['open'] if local is not None else None,
                     remote['total'] if remote is not None else None,remote['open'] if remote is not None else None))
                prior=db.execute('SELECT * FROM resource_uncertainty_dispositions WHERE request_id=?',(request_id,)).fetchone()
            else:
                raise CoordError('uncertainty record not found')
            def observed_counts(prefix):
                total,opened=prior[prefix+'_runs_total'],prior[prefix+'_runs_open']
                return {'total':total,'open':opened} if total is not None and opened is not None else None
            return {'request_id':request_id,'resource':resource,'reservation_id':reservation_id,
                'state':'uncertain','administrative_state':'evidence_recorded',
                'resource_state':prior['observed_resource_state'] or 'unknown','physical_state':'unknown',
                'process_state':'unknown','reuse_authorized':False,'reason':prior['reason'],
                'recorded_at':prior['recorded_at'],
                'observed_at':prior['recorded_at'] if prior['observed_resource_state'] is not None else None,
                'local_guarded_runs':observed_counts('local'), 'remote_guarded_runs':observed_counts('remote')}

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
        from .hospital_vendor_scope import CONTROL as vendor_control, RUN_RESOURCE as vendor_run
        if resource in (vendor_control,vendor_run): raise CoordError('fixed vendor scopes cannot authorize arbitrary execution')
        with self.tx() as db:
            if self._remote_bound(db, sid): raise CoordError("remote-bound reservation must use agent-chat --server run")
            # Reading inbox here is deliberate and satisfies the ownership mutation guard.
            self._fresh(db,sid);r=self._verify_owner(db,resource,sid,token)
            assert_agent_running(db, sid, cleanup=True)
            assert_original_owner_admission(db, sid)
            assert_execution_domain_admission(db,resource,sid,r['reservation_id'])
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
        if os.environ.get("AGENT_CHAT_SERVER"):
            raise CoordError("ValidationGuard cannot use remote coordination; use agent-chat --server run")
        if db_path is None and os.environ.get('AGENT_CHAT_PROJECT'):
            from .projects import resolve_database
            db_path = str(resolve_database(os.environ.get('AGENT_CHAT_DB') or default_db(), os.environ['AGENT_CHAT_PROJECT']))
        self.coord=Coordinator(db_path,session);self.resource=resource;self.token=token or os.environ.get("AGENT_CHAT_TOKEN","");self.run_id: str|None=None;self.seen:set[str]=set();self.safe_to_close=True
    def _emit_inbox(self)->None:
        messages=self.coord.inbox()["messages"]; new=[m for m in messages if m["id"] not in self.seen]
        self.seen.update(m["id"] for m in messages)
        if new: print(json.dumps({"messages":new}),file=sys.stderr,flush=True)
    def __enter__(self)->"ValidationGuard":
        try:
            self._emit_inbox()
            self.run_id, reservation = self.coord.begin_guard(self.resource,self.token,os.getpid())
            self.reservation_id = reservation['reservation_id']
            return self
        except BaseException:
            self.coord.close()
            raise
    def pulse(self)->None:
        self._emit_inbox();self.coord.check(self.resource,self.token,require_fresh=False)
    def __exit__(self,typ:Any,val:Any,tb:Any)->bool:
        try:
            if self.run_id and self.safe_to_close:
                evidence = getattr(self, 'evidence', None)
                if evidence: evidence.record('guard_close_started', run_id=self.run_id)
                self.coord.close_guard(self.run_id)
                if evidence: evidence.record('guard_close_succeeded', run_id=self.run_id)
        finally:
            evidence = getattr(self, 'evidence', None)
            if evidence: evidence.close()
            self.coord.close()
        return False


def validation_guard(resource:str="validation-clone") -> ValidationGuard:
    """Return a session/token-bound guard for use with ``with`` blocks."""
    return ValidationGuard(resource)


def _json_ok(value:dict[str,Any])->int: print(json.dumps(value,sort_keys=True));return 0
def _token(a:argparse.Namespace)->str:return a.token or os.environ.get("AGENT_CHAT_TOKEN","")

def run_command(c: Coordinator, a: argparse.Namespace) -> dict[str, Any]:
    command = list(a.command)
    if not command:
        raise CoordError("run requires a command after --")
    guard = ValidationGuard(a.resource, _token(a), str(c.path), c.session)
    with guard:
        if __package__:
            from .private_artifacts import artifact_path, ProcessEvidence
        else:
            from private_artifacts import artifact_path, ProcessEvidence
        guard.safe_to_close = False
        evidence = ProcessEvidence(artifact_path('process-evidence', guard.run_id),
                                  runner_pid=os.getpid(), runner_pgid=os.getpgrp(),
                                  session_id=c.session, resource=c.resource_name(a.resource),
                                  reservation_id=guard.reservation_id, run_id=guard.run_id,
                                  host_id=__import__('socket').gethostname(),
                                  coverage='instrumented_process_group_only', transport='local')
        guard.evidence = evidence
        evidence.record('admitted')
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
            evidence.record('child_launch_started')
            proc = subprocess.Popen(command, start_new_session=True, env=env)
            evidence.record('child_created', child_pid=proc.pid, child_pgid=proc.pid)
            guard.safe_to_close = False
            c.attach_group(guard.run_id or "", proc.pid)
            evidence.record('group_attached', child_pgid=proc.pid)
            last_poll = 0.0
            while proc.poll() is None:
                if interrupted:
                    raise CoordError("command interrupted; closing owned process group")
                if time.monotonic() - last_poll >= 5:
                    guard.pulse()
                    last_poll = time.monotonic()
                time.sleep(.1)
            code = proc.wait()
            evidence.record('leader_reaped', child_pid=proc.pid, returncode=code)
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
                    evidence.record('leader_reaped', child_pid=proc.pid, returncode=proc.returncode)
                    evidence.record('group_absent', child_pgid=proc.pid)
                guard.safe_to_close = not evidence.failed
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
    sub.add_parser("policy", help="read standing coordination policy")
    x=sub.add_parser("register");x.add_argument("--agent",required=True)
    x=sub.add_parser("rename", help="change your name while preserving your session");x.add_argument("--agent",required=True)
    x=sub.add_parser("send");x.add_argument("--to",required=True);x.add_argument("--body-file",required=True);x.add_argument("--attach",action="append",default=[]);x.add_argument("--reply-to");x.add_argument("--ack-reply",action="store_true")
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
        if a.op=="policy":
            if __package__:
                from .communication import current_policy
            else:
                from communication import current_policy
            out=current_policy()
        elif a.op=="register": out=c.register(a.agent)
        elif a.op=="rename": out=c.rename(a.agent)
        elif a.op=="send": out=c.send(a.to,pathlib.Path(a.body_file).read_text(encoding="utf-8"),a.attach,a.reply_to,ack_reply=a.ack_reply)
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


# Managed execution epochs are logical admission fences, never old-run closure.
def execution_epoch_schema(db):
    db.executescript("""
      CREATE TABLE IF NOT EXISTS execution_epochs (
        request_id TEXT PRIMARY KEY, resource TEXT NOT NULL, project TEXT NOT NULL,
        old_owner TEXT NOT NULL, old_reservation TEXT NOT NULL, old_deadline REAL NOT NULL,
        old_thread TEXT NOT NULL, host TEXT NOT NULL, root_generation TEXT NOT NULL,
        attestation_id TEXT UNIQUE NOT NULL, backing TEXT NOT NULL, execution_id TEXT NOT NULL,
        payload_digest TEXT NOT NULL, recorded_at REAL NOT NULL);
      CREATE TABLE IF NOT EXISTS execution_epoch_prepares (
        id TEXT PRIMARY KEY, resource TEXT NOT NULL, workspace TEXT NOT NULL,
        leaf_prefix TEXT NOT NULL, gitdir TEXT NOT NULL, index_resource TEXT NOT NULL, scope_digest TEXT NOT NULL, old_owner TEXT NOT NULL, old_reservation TEXT NOT NULL,
        old_deadline REAL NOT NULL, old_thread TEXT NOT NULL, host TEXT NOT NULL,
        root_generation TEXT NOT NULL, instance_id TEXT NOT NULL,
        created_at REAL NOT NULL, expires_at REAL NOT NULL,state TEXT NOT NULL,
        payload_digest TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS execution_epoch_prepare_events (
        request_id TEXT NOT NULL,event TEXT NOT NULL,at REAL NOT NULL,
        authority_digest TEXT NOT NULL,PRIMARY KEY(request_id,event));
      CREATE TABLE IF NOT EXISTS execution_epoch_samples (
        attestation_id TEXT PRIMARY KEY, prepare_id TEXT NOT NULL, gitdir TEXT NOT NULL,
        writer_count INTEGER NOT NULL, coverage TEXT NOT NULL, sample_digest TEXT NOT NULL, scope_digest TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS execution_epoch_grants (
        reservation_id TEXT PRIMARY KEY, epoch_id TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS execution_epoch_handoffs (
        id TEXT PRIMARY KEY, thread TEXT, actor TEXT, host TEXT NOT NULL, dispatcher TEXT NOT NULL,
        method TEXT NOT NULL, started_at REAL NOT NULL, settled_at REAL, outcome TEXT);
    """)
    for table in ('execution_epochs', 'execution_epoch_grants','execution_epoch_samples','execution_epoch_prepare_events'):
        for operation in ('UPDATE', 'DELETE'):
            db.execute(f"CREATE TRIGGER IF NOT EXISTS {table}_no_{operation.lower()} BEFORE {operation} ON {table} BEGIN SELECT RAISE(ABORT,'execution epoch audit is immutable'); END")


    db.execute("CREATE TRIGGER IF NOT EXISTS epoch_prepare_no_delete BEFORE DELETE ON execution_epoch_prepares BEGIN SELECT RAISE(ABORT,'prepared scope audit is immutable'); END")
    fields=('id','resource','workspace','leaf_prefix','gitdir','index_resource','scope_digest','old_owner','old_reservation','old_deadline','old_thread','host','root_generation','instance_id','created_at','expires_at','payload_digest')
    changed=' OR '.join('NEW.'+name+' IS NOT OLD.'+name for name in fields)
    db.execute("CREATE TRIGGER IF NOT EXISTS epoch_prepare_typed_phase BEFORE UPDATE ON execution_epoch_prepares WHEN "+changed+" OR OLD.state<>'prepared' OR NEW.state NOT IN ('committed','cancelled') BEGIN SELECT RAISE(ABORT,'prepared scope identity/phase is immutable'); END")


def _epoch_exists(db):
    return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='execution_epochs'").fetchone())


def execution_epoch_native_check(db, *, thread=None, actor=None):
    if not _epoch_exists(db): return
    if not thread and not actor and db.execute('SELECT 1 FROM execution_epochs LIMIT 1').fetchone():
        raise CoordError('native epoch actor/thread association unavailable')
    if thread and db.execute("SELECT 1 FROM execution_epoch_prepares WHERE old_thread=? AND state='prepared'",(thread,)).fetchone():
        raise CoordError('current-domain prepared gate withheld old managed native admission')
    # Old thread identity stays quarantined even after a new owner obtains FIFO rights.
    if thread and db.execute('SELECT 1 FROM execution_epochs WHERE old_thread=?', (thread,)).fetchone():
        raise CoordError('execution epoch quarantined old managed thread; old physical state remains UNVERIFIED')
    if not thread and actor and db.execute('SELECT 1 FROM execution_epochs WHERE old_owner=?', (actor,)).fetchone():
        raise CoordError('execution epoch requires an independently bound new thread for this actor')


def execution_epoch_guard_check(db, resource, reservation):
    if not _epoch_exists(db): return
    epoch=db.execute('SELECT request_id FROM execution_epochs WHERE resource=? ORDER BY rowid DESC LIMIT 1',(resource,)).fetchone()
    if epoch and not db.execute('SELECT 1 FROM execution_epoch_grants WHERE reservation_id=? AND epoch_id=?',(reservation,epoch['request_id'])).fetchone():
        raise CoordError('reservation predates current managed execution epoch')


def execution_epoch_handoff(db, method, thread, actor, host, dispatcher="local"):
    # Caller owns the same-store BEGIN IMMEDIATE transaction used by transition.
    if thread and db.execute("SELECT 1 FROM sqlite_master WHERE name='session_resets'").fetchone():
        resets=db.execute("SELECT old_thread_id,target_session FROM session_resets WHERE new_thread_id=? AND status NOT IN ('completed','cancelled','failed')",(thread,)).fetchmany(2)
        if len(resets)>1: raise CoordError('ambiguous current reset thread scope')
        if resets:
            from .root_lifecycle import released_parked
            if not released_parked(db, resets[0]['target_session']):
                thread,actor=resets[0]['old_thread_id'],resets[0]['target_session']
    execution_epoch_native_check(db,thread=thread,actor=actor)
    identifier=_id('epoch_handoff')
    db.execute('INSERT INTO execution_epoch_handoffs VALUES(?,?,?,?,?,?,?,NULL,NULL)',
               (identifier,thread,actor,host,dispatcher,method,_now()))
    return identifier


def execution_epoch_settle(db, identifier, host, outcome, dispatcher="local"):
    if outcome not in ('submitted','rejected'): raise CoordError('uncertain native effect cannot be settled as closed')
    row=db.execute('SELECT * FROM execution_epoch_handoffs WHERE id=?',(identifier,)).fetchone()
    if not row or row['host']!=host or row['dispatcher']!=dispatcher:
        raise CoordError('execution handoff belongs to another host or dispatcher owner')
    if row['settled_at'] is not None:
        if row['outcome']!=outcome: raise CoordError('execution handoff settlement conflicts')
        return
    db.execute('UPDATE execution_epoch_handoffs SET settled_at=?,outcome=? WHERE id=?',(_now(),outcome,identifier))



def execution_prepared_scope(db,resource):
    if not _epoch_exists(db): return None
    rows=db.execute("SELECT * FROM execution_epoch_prepares WHERE state='prepared' LIMIT 17").fetchall()
    if len(rows)>16: raise CoordError('current-domain prepared scope exceeds bound')
    for row in rows:
        if resource in (row['resource'],row['workspace'],row['index_resource']) or resource.startswith(row['leaf_prefix']): return row
    return None


def assert_execution_leaf_acquisition(db,resource):
    scope=execution_prepared_scope(db,resource)
    if scope and (resource == scope['index_resource'] or resource.startswith(scope['leaf_prefix'])):
        raise CoordError('current-domain prepared gate withheld new mutable-leaf allocation')


def assert_execution_domain_admission(db,resource,session,reservation):
    if execution_prepared_scope(db,resource):
        raise CoordError('current-domain prepared gate withheld new guard/attach/restoration')
    if not _epoch_exists(db): return
    row=db.execute("SELECT e.request_id,e.resource,p.workspace FROM execution_epochs e JOIN execution_epoch_prepares p ON p.id=e.request_id WHERE p.workspace=? OR e.resource=? ORDER BY e.rowid DESC LIMIT 1",(resource,resource)).fetchone()
    if row:
        # Both entry paths admit native execution. Neither role alone is sufficient.
        canonical=db.execute('SELECT * FROM resources WHERE name=?',(row['resource'],)).fetchone()
        workspace=db.execute('SELECT * FROM resources WHERE name=?',(row['workspace'],)).fetchone()
        for label,held in (('canonical FIFO',canonical),('workspace',workspace)):
            if (not held or held['owner_session']!=session or held['deadline']<=_now() or
                not db.execute('SELECT 1 FROM execution_epoch_grants WHERE reservation_id=? AND epoch_id=?',(held['reservation_id'],row['request_id'])).fetchone()):
                raise CoordError('native epoch admission requires fresh same-owner '+label+' rights')
        if reservation not in (canonical['reservation_id'],workspace['reservation_id']):
            raise CoordError('native epoch admission reservation binding mismatch')


def assert_execution_domain_return(db,resource,closed_at=None):
    if not _epoch_exists(db): return
    row=db.execute('SELECT p.workspace FROM execution_epochs e JOIN execution_epoch_prepares p ON p.id=e.request_id WHERE e.resource=? ORDER BY e.rowid DESC LIMIT 1',(resource,)).fetchone()
    if row:
        for table in ('guarded_runs','remote_guarded_runs'):
            if db.execute("SELECT 1 FROM sqlite_master WHERE name=?",(table,)).fetchone() and db.execute('SELECT 1 FROM '+table+' WHERE resource=? AND closed_at IS NULL',(row['workspace'],)).fetchone():
                raise CoordError('canonical return requires current native backing guards closed')
            if closed_at is not None and db.execute("SELECT 1 FROM sqlite_master WHERE name=?",(table,)).fetchone():
                latest=db.execute('SELECT MAX(closed_at) FROM '+table+' WHERE resource=?',(row['workspace'],)).fetchone()[0]
                if latest is not None and closed_at<latest: raise CoordError('canonical receipt predates current backing closure')

if __name__=="__main__":
    try: raise SystemExit(main())
    except (CoordError,sqlite3.Error,OSError,ValueError) as e:
        print(json.dumps({"error":str(e)}),file=sys.stderr);raise SystemExit(2)
