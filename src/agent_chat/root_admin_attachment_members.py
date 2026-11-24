"""Durable member identity and local fences for Root attachment."""

import contextlib
import sqlite3
import time
from pathlib import Path

from .core import CoordError


def joined_members(db, home_project, home_session):
    members = tuple(tuple(row) for row in db.execute(
        'SELECT project_id,session_id FROM root_projects WHERE joined=1 ORDER BY project_id'
    ))
    if (home_project, home_session) not in members or any(not project or not session for project, session in members):
        raise CoordError('administrative attachment joined Root scope changed')
    return members


def table(db, name):
    return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone())


def release_completed(db, request_id):
    return table(db, 'root_admin_attachment_releases') and bool(db.execute(
        'SELECT 1 FROM root_admin_attachment_releases WHERE request_id=?', (request_id,)
    ).fetchone())


def record_release(db, request_id):
    db.execute('''CREATE TABLE IF NOT EXISTS root_admin_attachment_releases (
        request_id TEXT PRIMARY KEY NOT NULL)''')
    for operation in ('UPDATE', 'DELETE'):
        db.execute(f'''CREATE TRIGGER IF NOT EXISTS root_admin_attachment_releases_no_{operation.lower()}
            BEFORE {operation} ON root_admin_attachment_releases
            BEGIN SELECT RAISE(ABORT,'administrative attachment release is immutable'); END''')
    db.execute('INSERT OR IGNORE INTO root_admin_attachment_releases VALUES(?)', (request_id,))


def cutover_pending(db):
    if not table(db, 'root_admin_attachment_members'):
        return False
    incomplete = '1'
    if table(db, 'root_admin_attachment_releases'):
        incomplete = '''a.state='prepared' OR NOT EXISTS (
            SELECT 1 FROM root_admin_attachment_releases r WHERE r.request_id=a.request_id)'''
    return bool(db.execute(f'''SELECT 1 FROM root_admin_attachment_requests a
        JOIN root_admin_attachment_members m ON m.request_id=a.request_id
        WHERE {incomplete} LIMIT 1''').fetchone())


def retained_members(db, request_id):
    if not table(db, 'root_admin_attachment_members'):
        return ()
    return tuple(tuple(row) for row in db.execute('''SELECT project_id,session_id,path,device,inode
        FROM root_admin_attachment_members WHERE request_id=? ORDER BY project_id''', (request_id,)))


def pin_members(members, resolve_project):
    if resolve_project is None:
        raise CoordError('administrative attachment requires validated project stores')
    pins = []
    for project, session in members:
        path = Path(resolve_project(project))
        stat = path.lstat()
        pins.append((project, session, str(path), stat.st_dev, stat.st_ino))
    if len({(pin[3], pin[4]) for pin in pins}) != len(pins):
        raise CoordError('administrative attachment project stores overlap')
    return tuple(pins)


def retain_members(db, request_id, pins, *, existing):
    db.execute('''CREATE TABLE IF NOT EXISTS root_admin_attachment_members (
        request_id TEXT NOT NULL, project_id TEXT NOT NULL, session_id TEXT NOT NULL,
        path TEXT NOT NULL, device INTEGER NOT NULL, inode INTEGER NOT NULL,
        PRIMARY KEY(request_id, project_id))''')
    for operation in ('UPDATE', 'DELETE'):
        db.execute(f'''CREATE TRIGGER IF NOT EXISTS root_admin_attachment_members_no_{operation.lower()}
            BEFORE {operation} ON root_admin_attachment_members
            BEGIN SELECT RAISE(ABORT,'administrative attachment member evidence is immutable'); END''')
    if not existing:
        db.executemany('INSERT INTO root_admin_attachment_members VALUES(?,?,?,?,?,?)',
                       ((request_id, *pin) for pin in pins))
    if retained_members(db, request_id) != pins:
        raise CoordError('administrative attachment retained project scope differs')


@contextlib.contextmanager
def open_member(pin, master, *, write):
    path = Path(pin[2])
    stat = path.lstat()
    if path.is_symlink() or (stat.st_dev, stat.st_ino) != pin[3:]:
        raise CoordError('administrative attachment retained project store changed')
    mode = 'rw' if write else 'ro'
    with contextlib.closing(sqlite3.connect(path.as_uri() + '?mode=' + mode, uri=True,
                                           timeout=10, isolation_level=None)) as db:
        db.execute('BEGIN IMMEDIATE' if write else 'BEGIN')
        stat = path.lstat()
        if (stat.st_dev, stat.st_ino) != pin[3:]:
            raise CoordError('administrative attachment retained project store changed')
        if not table(db, 'meta'):
            raise CoordError('administrative attachment member metadata is unavailable')
        data = dict(db.execute("SELECT key,value FROM meta WHERE key IN ('project_id','project_registry','schema_version')"))
        project = pin[0]
        master = Path(master).resolve()
        default = project == 'default'
        registry = data.get('project_registry')
        if (data.get('schema_version') != '1'
                or data.get('project_id', 'default' if default else None) != project
                or (default and path.resolve() != master)
                or (not default and not registry)
                or (registry and Path(registry).resolve() != master)):
            raise CoordError('administrative attachment opened member lineage differs')
        yield db


def exact_binding(db, session, home_project, home_session, host, permitted):
    projection = db.execute('SELECT home_project,home_session,thread_id FROM root_projections WHERE session_id=?',
                            (session,)).fetchone()
    binding = db.execute('SELECT thread_id,parent_session,agent_path FROM bridge_bindings WHERE session_id=?',
                         (session,)).fetchone()
    owner = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (session,)).fetchone()
    if (not binding or (permitted is not None and binding[0] not in permitted)
            or tuple(binding[1:]) != (None, None)
            or not projection or tuple(projection) != (home_project, home_session, binding[0])
            or not owner or owner[0] != host):
        raise CoordError('administrative attachment member binding or projection differs')
    return binding[0]


def has_execution_handoff_conflict(db, session, old_thread, host, listener_started_at):
    if not table(db, 'execution_epoch_handoffs'):
        return False
    # Old-thread reloads that predate the verified listener are historical for
    # attachment. Retain the original row and its unknown outcome.
    return db.execute('''SELECT 1 FROM execution_epoch_handoffs
        WHERE settled_at IS NULL AND (actor=? OR thread=?)
          AND NOT (method='thread/resume' AND thread IS ? AND host=? AND started_at<?)
        LIMIT 1''', (session, old_thread, old_thread, host, listener_started_at)).fetchone() is not None


def ensure_no_member_conflicts(db, session, old_thread, new_thread, *, host, listener_started_at):
    if db.execute('SELECT 1 FROM bridge_bindings WHERE thread_id=? AND session_id<>?',
                  (new_thread, session)).fetchone():
        raise CoordError('administrative attachment target thread belongs to another session')
    conflicts = (
        ('root_fresh_gates', 'root_session=? AND active=1', (session,)),
        ('root_native_handoffs', 'active=1 AND (session_id=? OR thread_id=?)', (session, old_thread)),
        ('bridge_jobs', "thread_id=? AND status IN ('adding','starting')", (old_thread,)),
        ('session_resets', "target_session=? AND status NOT IN ('completed','failed','cancelled')", (session,)),
        ('agent_control_requests', "target_session=? AND status NOT IN ('completed','failed','cancelled','resolved_unchanged')", (session,)),
    )
    for name, predicate, args in conflicts:
        if table(db, name) and db.execute(f'SELECT 1 FROM {name} WHERE {predicate}', args).fetchone():
            raise CoordError(f'administrative attachment requires member {name} reconciliation')
    if has_execution_handoff_conflict(db, session, old_thread, host, listener_started_at):
        raise CoordError('administrative attachment requires member execution_epoch_handoffs reconciliation')


def fence_state(db, request_id, session, old_thread, new_thread):
    if not table(db, 'root_admin_attachment_fences'):
        return None
    row = db.execute('''SELECT session_id,old_thread_id,new_thread_id,active
        FROM root_admin_attachment_fences WHERE request_id=?''', (request_id,)).fetchone()
    if row and (tuple(row[:3]) != (session, old_thread, new_thread) or row[3] not in (0, 1, 2)):
        raise CoordError('administrative attachment retained member fence differs')
    return row[3] if row else None


def prepare_member(db, request_id, session, home_project, home_session, host, old_thread, new_thread, *, listener_started_at):
    fence = fence_state(db, request_id, session, old_thread, new_thread)
    permitted = (old_thread, new_thread) if fence == 1 else (old_thread,)
    exact_binding(db, session, home_project, home_session, host, permitted)
    ensure_no_member_conflicts(db, session, old_thread, new_thread, host=host, listener_started_at=listener_started_at)
    db.execute('''CREATE TABLE IF NOT EXISTS root_admin_attachment_fences (
        request_id TEXT PRIMARY KEY,session_id TEXT NOT NULL,old_thread_id TEXT NOT NULL,
        new_thread_id TEXT NOT NULL,active INTEGER NOT NULL)''')
    other = db.execute('SELECT 1 FROM root_admin_attachment_fences WHERE request_id<>? AND active<>0',
                       (request_id,)).fetchone()
    if other or fence not in (None, 1):
        raise CoordError('another administrative attachment fence is active')
    if fence is None:
        db.execute('INSERT INTO root_admin_attachment_fences VALUES(?,?,?,?,1)',
                   (request_id, session, old_thread, new_thread))


def switch_member(db, request_id, session, home_project, home_session, host, old_thread, new_thread, *, listener_started_at):
    if fence_state(db, request_id, session, old_thread, new_thread) != 1:
        raise CoordError('administrative attachment member fence is missing')
    bound_thread = exact_binding(db, session, home_project, home_session, host, (old_thread, new_thread))
    ensure_no_member_conflicts(db, session, old_thread, new_thread, host=host, listener_started_at=listener_started_at)
    if bound_thread == old_thread:
        db.execute('UPDATE bridge_bindings SET thread_id=?,updated_at=? WHERE session_id=?',
                   (new_thread, time.time(), session))
        db.execute('UPDATE root_projections SET thread_id=? WHERE session_id=?', (new_thread, session))


def release_member(db, request_id, session, old_thread, new_thread):
    fence = fence_state(db, request_id, session, old_thread, new_thread)
    if fence is None:
        raise CoordError('administrative attachment member fence is missing')
    if fence == 1:
        outstanding = db.execute("SELECT 1 FROM bridge_jobs WHERE thread_id=? AND status NOT IN ('dispatched','cancelled') LIMIT 1",
                                 (old_thread,)).fetchone()
        fence = 2 if outstanding else 0
        db.execute('UPDATE root_admin_attachment_fences SET active=? WHERE request_id=?', (fence, request_id))
    return bool(fence)
