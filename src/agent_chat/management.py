"""Explicit management authority, independent of native conversation routing."""
import time
import uuid
from .core import CoordError


def manager(db, session):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='agent_management'").fetchone():
        return None
    row = db.execute('SELECT manager_session FROM agent_management WHERE target_session=?', (session,)).fetchone()
    return row['manager_session'] if row else None


def assert_governing_reset(db, reset_id, canonical_root, project_id):
    """Append-only reset journal: the last request for this project governs."""
    for trigger in ('root_team_reset_request_update','root_team_reset_request_delete'):
        if not db.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' AND name=? AND tbl_name='root_team_reset_requests'", (trigger,)).fetchone():
            raise CoordError('direct Root governing reset journal is not append-only')
    latest = db.execute('''SELECT id,root_session,status FROM root_team_reset_requests
                           WHERE project_id=? ORDER BY rowid DESC LIMIT 1''', (project_id,)).fetchone()
    if not latest or (latest['id'],latest['root_session'],latest['status']) != (reset_id,canonical_root,'completed'):
        raise CoordError('direct Root creation reset is superseded or not governing')


def parked_creation(db, job_id):
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_parked_creations'").fetchone():
        row = db.execute('SELECT * FROM root_parked_creations WHERE request_id=?', (job_id,)).fetchone()
        if row: return row
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_turnover_subordinates'").fetchone():
        return db.execute('SELECT * FROM root_turnover_subordinates WHERE request_id=?', (job_id,)).fetchone()
    return None


def assert_parked_creation(db, job_id):
    row = parked_creation(db, job_id)
    if not row:
        raise CoordError('parked creation provenance unavailable')
    if 'bootstrap_id' in row.keys():
        parent = assert_parked_active(db, row['bootstrap_id'])
        grant = db.execute('SELECT * FROM root_turnover_bootstraps WHERE request_id=?', (row['bootstrap_id'],)).fetchone()
        job = db.execute('SELECT * FROM session_resets WHERE id=?', (job_id,)).fetchone()
        parent_job = db.execute('SELECT status,new_thread_id FROM session_resets WHERE id=?', (row['bootstrap_id'],)).fetchone()
        manager = db.execute('SELECT manager_session,request_id FROM agent_management WHERE target_session=?', (row['target_session'],)).fetchone()
        target_host = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (row['target_session'],)).fetchone()
        if (not grant or not job or not parent_job or parent_job['status'] != 'completed' or not manager
                or (row['project_id'],row['reset_id'],row['stop_id'],row['root_session']) != (parent['project_id'],parent['reset_id'],parent['stop_id'],parent['root_session'])
                or (job['kind'],job['requester_session'],job['target_session'],job['old_thread_id'],job['host_id']) != ('create',parent['target_session'],row['target_session'],parent_job['new_thread_id'],row['host_id'])
                or row['thread_id'] != parent_job['new_thread_id'] or row['host_id'] != parent['host_id']
                or not target_host or target_host['host_id'] != row['host_id']
                or tuple(manager) != (parent['target_session'],job_id)):
            raise CoordError('parked subordinate provenance differs')
        return row
    job = db.execute('SELECT * FROM session_resets WHERE id=?', (job_id,)).fetchone()
    root = db.execute('SELECT * FROM root_projections WHERE session_id=? AND paused=0', (row['root_session'],)).fetchone()
    binding = db.execute('SELECT thread_id,parent_session FROM bridge_bindings WHERE session_id=?', (row['root_session'],)).fetchone()
    host = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (row['root_session'],)).fetchone()
    target_host = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (row['target_session'],)).fetchone()
    stops = db.execute('SELECT id,root_session FROM root_team_stops WHERE active=1').fetchall()
    if (not job or not root or not binding or not host or not target_host or len(stops) != 1
            or tuple(stops[0]) != (row['stop_id'], row['root_session'])
            or (job['kind'],job['requester_session'],job['target_session'],job['old_thread_id'],job['host_id']) !=
               ('create',row['root_session'],row['target_session'],row['thread_id'],row['host_id'])
            or root['thread_id'] != row['thread_id'] or binding['thread_id'] != row['thread_id']
            or binding['parent_session'] is not None or host['host_id'] != row['host_id']
            or target_host['host_id'] != row['host_id']):
        raise CoordError('parked creation stop, actor, binding, or host changed')
    assert_governing_reset(db,row['reset_id'],root['home_session'],row['project_id'])
    return row


def assert_parked_active(db, job_id):
    row = assert_parked_creation(db,job_id)
    if 'bootstrap_id' in row.keys():
        return row
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_parked_activations'").fetchone() or not db.execute('SELECT 1 FROM root_parked_activations WHERE request_id=?', (job_id,)).fetchone():
        raise CoordError('parked PO designation is not committed')
    role = db.execute('SELECT root_session FROM project_orchestrators WHERE session_id=?', (row['target_session'],)).fetchone()
    management = db.execute('SELECT manager_session,request_id FROM agent_management WHERE target_session=?', (row['target_session'],)).fetchone()
    if (not role or role['root_session'] != row['root_session'] or not management
            or (management['manager_session'],management['request_id']) != (row['root_session'],job_id)):
        raise CoordError('parked PO role or management changed')
    return row


def record_root_creation(coord, request_id, project_id, reset_id, *, parked=False):
    """Private prototype: retain exact Root creation provenance in the creation transaction."""
    db, caller = coord.db, coord.require_session()
    if not db.in_transaction or (not parked and project_id != 'project_5fb8d3d13ec348e0993b25a3f3169289'):
        raise CoordError('direct Root creation requires exact project transaction')
    job = db.execute('SELECT * FROM session_resets WHERE id=?', (request_id,)).fetchone()
    root = db.execute('SELECT * FROM root_projections WHERE session_id=? AND paused=0 AND home_project=?', (caller,'default')).fetchone()
    reset = (db.execute("SELECT 1 FROM root_team_reset_requests WHERE id=? AND root_session=? AND project_id=? AND status='completed'", (reset_id,root['home_session'],project_id)).fetchone() if root else None)
    binding = db.execute('SELECT thread_id,parent_session FROM bridge_bindings WHERE session_id=?', (caller,)).fetchone()
    host = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (caller,)).fetchone()
    if (not job or job['kind'] != 'create' or job['status'] != 'pending' or job['requester_session'] != caller
            or not root or not reset or not binding or binding['parent_session'] is not None
            or binding['thread_id'] != job['old_thread_id'] or binding['thread_id'] != root['thread_id']
            or not host or host['host_id'] != job['host_id']):
        raise CoordError('direct Root creation provenance differs')
    assert_governing_reset(db, reset_id, root['home_session'], project_id)
    target_agent = db.execute('SELECT agent FROM sessions WHERE id=?', (job['target_session'],)).fetchone()
    if not target_agent:
        raise CoordError('direct Root creation target registration disappeared')
    db.execute('CREATE TABLE IF NOT EXISTS agent_management (target_session TEXT PRIMARY KEY, manager_session TEXT NOT NULL, request_id TEXT NOT NULL)')
    db.execute('''CREATE TABLE IF NOT EXISTS root_direct_creations (
      request_id TEXT PRIMARY KEY, project_id TEXT NOT NULL, reset_id TEXT NOT NULL,
      root_session TEXT NOT NULL, target_session TEXT NOT NULL UNIQUE, thread_id TEXT NOT NULL, host_id TEXT NOT NULL,
      target_agent TEXT NOT NULL)''')
    db.execute("CREATE TRIGGER IF NOT EXISTS root_direct_no_update BEFORE UPDATE ON root_direct_creations BEGIN SELECT RAISE(ABORT,'direct creation audit immutable'); END")
    db.execute("CREATE TRIGGER IF NOT EXISTS root_direct_no_delete BEFORE DELETE ON root_direct_creations BEGIN SELECT RAISE(ABORT,'direct creation audit immutable'); END")
    db.execute('INSERT INTO root_direct_creations VALUES(?,?,?,?,?,?,?,?)',
               (request_id,project_id,reset_id,caller,job['target_session'],job['old_thread_id'],job['host_id'],target_agent['agent']))
    db.execute('INSERT INTO agent_management VALUES(?,?,?)', (job['target_session'],caller,request_id))


def record_creation(coord, request_id):
    """Assign a new worker to its current PO creator, within the creation commit.

    This is creation provenance, not a Root delegation or native parent route.
    Existing creations are never backfilled by retrying their request.
    """
    db = coord.db
    caller = coord.require_session()
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='project_orchestrators'").fetchone():
        return
    role = db.execute('SELECT root_session FROM project_orchestrators WHERE session_id=?', (caller,)).fetchone()
    if not role:
        return
    if not db.in_transaction:
        raise CoordError('creation management requires the creation transaction')
    request = db.execute('SELECT * FROM session_resets WHERE id=?', (request_id,)).fetchone()
    if (not request or request['kind'] != 'create' or request['status'] != 'pending'
            or request['requester_session'] != caller):
        raise CoordError('creation management requires the exact new creator request')
    target = request['target_session']
    binding = db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?', (caller,)).fetchone()
    creator_host = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (caller,)).fetchone()
    target_host = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (target,)).fetchone()
    if (not binding or binding['thread_id'] != request['old_thread_id']
            or not creator_host or creator_host['host_id'] != request['host_id']
            or not target_host or target_host['host_id'] != request['host_id']
            or db.execute('SELECT 1 FROM bridge_bindings WHERE session_id=?', (target,)).fetchone()
            or db.execute('SELECT 1 FROM project_orchestrators WHERE session_id=?', (target,)).fetchone()
            or db.execute('SELECT 1 FROM root_projections WHERE session_id=?', (target,)).fetchone()
            or db.execute('SELECT 1 FROM operator_identities WHERE session_id=?', (target,)).fetchone()):
        raise CoordError('creation management target or creator scope changed')
    db.execute('''CREATE TABLE IF NOT EXISTS agent_management (
        target_session TEXT PRIMARY KEY, manager_session TEXT NOT NULL, request_id TEXT NOT NULL)''')
    db.execute('''CREATE TABLE IF NOT EXISTS agent_management_creations (
        request_id TEXT PRIMARY KEY, creator_session TEXT NOT NULL, root_session TEXT NOT NULL,
        target_session TEXT NOT NULL UNIQUE, creator_thread TEXT NOT NULL, host_id TEXT NOT NULL,
        created_at REAL NOT NULL)''')
    db.execute('''CREATE TRIGGER IF NOT EXISTS agent_management_creation_update
        BEFORE UPDATE ON agent_management_creations
        BEGIN SELECT RAISE(ABORT,'creation management audit is immutable'); END''')
    db.execute('''CREATE TRIGGER IF NOT EXISTS agent_management_creation_delete
        BEFORE DELETE ON agent_management_creations
        BEGIN SELECT RAISE(ABORT,'creation management audit is immutable'); END''')
    if db.execute('SELECT 1 FROM agent_management WHERE target_session=?', (target,)).fetchone():
        raise CoordError('new creation already has a management owner')
    db.execute('INSERT INTO agent_management_creations VALUES(?,?,?,?,?,?,?)',
               (request_id, caller, role['root_session'], target, request['old_thread_id'],
                request['host_id'], request['created_at']))
    db.execute('INSERT INTO agent_management VALUES(?,?,?)', (target, caller, request_id))


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
