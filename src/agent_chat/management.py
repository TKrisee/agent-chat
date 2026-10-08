"""Explicit management authority, independent of native conversation routing."""
import time
import uuid
from .core import CoordError


def manager(db, session):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='agent_management'").fetchone():
        return None
    row = db.execute('SELECT manager_session FROM agent_management WHERE target_session=?', (session,)).fetchone()
    return row['manager_session'] if row else None


def delegate(coord, root_session, po, target, expected_thread, expected_host, request_id, confirmed):
    try: request_id = str(uuid.UUID(request_id))
    except (ValueError, TypeError, AttributeError): raise CoordError('management request_id must be a UUID')
    if confirmed is not True: raise CoordError('management delegation requires --confirm intent confirmation for authorized orchestration')
    if any(not isinstance(value,str) or not value for value in (root_session,po,target)):
        raise CoordError('management identities must be exact session IDs')
    if expected_host is not None and (not isinstance(expected_host,str) or not expected_host):
        raise CoordError('expected host must be exact authenticated host or absent')
    db = coord.db
    db.executescript('''CREATE TABLE IF NOT EXISTS agent_management (
        target_session TEXT PRIMARY KEY, manager_session TEXT NOT NULL, request_id TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS agent_management_requests (
        id TEXT PRIMARY KEY, root_session TEXT NOT NULL, manager_session TEXT NOT NULL,
        target_session TEXT NOT NULL, expected_thread TEXT, expected_host TEXT,
        created_at REAL NOT NULL);
        CREATE TRIGGER IF NOT EXISTS agent_management_audit_update BEFORE UPDATE ON agent_management_requests
        BEGIN SELECT RAISE(ABORT,'management audit is immutable'); END;
        CREATE TRIGGER IF NOT EXISTS agent_management_audit_delete BEFORE DELETE ON agent_management_requests
        BEGIN SELECT RAISE(ABORT,'management audit is immutable'); END;''')
    with coord.tx():
        values = (root_session, po, target, expected_thread, expected_host)
        old = db.execute('SELECT * FROM agent_management_requests WHERE id=?', (request_id,)).fetchone()
        if old:
            if tuple(old[key] for key in ('root_session','manager_session','target_session','expected_thread','expected_host')) != values:
                raise CoordError('management delegation retry payload differs from original')
            return dict(old)
        if not db.execute('SELECT 1 FROM project_orchestrators WHERE session_id=? AND root_session=?', (po, root_session)).fetchone():
            raise CoordError('management requires the current designated project orchestrator')
        if not db.execute('SELECT 1 FROM sessions WHERE id=?', (target,)).fetchone():
            raise CoordError('management target must be an exact retained session ID')
        if (db.execute('SELECT 1 FROM root_projections WHERE session_id=?', (target,)).fetchone()
                or db.execute('SELECT 1 FROM project_orchestrators WHERE session_id=?', (target,)).fetchone()
                or db.execute('SELECT 1 FROM operator_identities WHERE session_id=?', (target,)).fetchone()):
            raise CoordError('root, operator and designated orchestrator identities cannot be delegated')
        binding = db.execute('SELECT * FROM bridge_bindings WHERE session_id=?', (target,)).fetchone()
        if binding and binding['parent_session']:
            raise CoordError('native child routes already have management ancestry; delegation refused')
        actual_thread = binding['thread_id'] if binding else None
        if expected_thread is not None:
            try: expected_thread = str(uuid.UUID(expected_thread))
            except (ValueError, TypeError, AttributeError): raise CoordError('expected main thread must be an exact native UUID')
            if not isinstance(expected_host,str) or not expected_host:
                raise CoordError('bound main delegation requires its exact authenticated host')
        host = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (target,)).fetchone()
        if actual_thread != expected_thread or (host['host_id'] if host else None) != expected_host:
            raise CoordError('management expected native binding/host changed; inspect exact target')
        prior = manager(db, target)
        if prior and prior != po: raise CoordError('retained target has a conflicting explicit manager')
        db.execute('INSERT INTO agent_management_requests VALUES(?,?,?,?,?,?,?)', (request_id, *values, time.time()))
        db.execute('INSERT OR IGNORE INTO agent_management VALUES(?,?,?)', (target, po, request_id))
        return dict(db.execute('SELECT * FROM agent_management_requests WHERE id=?', (request_id,)).fetchone())
