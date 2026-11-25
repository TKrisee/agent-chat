"""Private selection primitive; caller must authenticate and revalidate on publish."""
import hashlib
import math
import time
from .goal_peer_read import SelectedGoalRead, SELECTED_HOST
from . import goal_selection_generation as tracking

class GoalSelectionUnavailable(Exception):
    pass

def _one(db, sql, args=()):
    rows = db.execute(sql, args).fetchall()
    if len(rows) != 1:
        raise GoalSelectionUnavailable('missing or ambiguous binding')
    return rows[0]

def select_designated_goal(registry, project, project_id, *, now=None):
    """Read coherent snapshots; publication still requires a fresh revalidation."""
    if registry.in_transaction or project.in_transaction:
        raise GoalSelectionUnavailable('selection requires independent fresh read transactions')
    begun=[]
    try:
        for db in (registry,project):
            db.execute('BEGIN')
            begun.append(db)
        return _select_snapshot(registry,project,project_id,now=now)
    finally:
        for db in reversed(begun):
            db.rollback()


def _select_snapshot(registry, project, project_id, *, now=None):
    """All IDs and registration facts come from server-selected stores, never a client."""
    if not isinstance(project_id, str) or not project_id:
        raise GoalSelectionUnavailable('invalid project')
    now = time.time() if now is None else now
    if type(now) not in (int, float) or not math.isfinite(now):
        raise GoalSelectionUnavailable('invalid time')
    try:
        registry_version = registry.execute('PRAGMA data_version').fetchone()[0]
        project_version = project.execute('PRAGMA data_version').fetchone()[0]
        meta = project.execute("SELECT value FROM meta WHERE key='project_id'").fetchone()
        if (meta[0] if meta else 'default') != project_id:
            raise GoalSelectionUnavailable('project store identity changed')
        root = _one(registry, 'SELECT home_project,home_session,thread_id,host_id,mode FROM root_config WHERE singleton=1')
        owner = _one(registry, 'SELECT home_project,home_session FROM root_directory_owner WHERE singleton=1')
        entry = _one(registry, 'SELECT session_id,orchestrator_session,joined FROM root_projects WHERE project_id=?', (project_id,))
        project_root, po, joined = entry
        if tuple(owner) != tuple(root[:2]) or root[3] != SELECTED_HOST or root[4] not in ('running', 'maintenance_paused') or joined != 1 or not isinstance(po, str) or not po:
            raise GoalSelectionUnavailable('unsupported or undesignated project')
        _one(project, 'SELECT id FROM sessions WHERE id=?', (po,))
        if project.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='session_resets'").fetchone():
            pending = project.execute("SELECT 1 FROM session_resets WHERE target_session=? AND (status IS NULL OR status NOT IN ('completed','failed','cancelled')) LIMIT 1", (po,)).fetchone()
            if pending:
                raise GoalSelectionUnavailable('PO reset is pending')
        role = _one(project, 'SELECT root_session FROM project_orchestrators WHERE session_id=?', (po,))
        projection = _one(project, 'SELECT home_project,home_session,thread_id FROM root_projections WHERE session_id=?', (project_root,))
        binding = _one(project, 'SELECT thread_id,parent_session,updated_at FROM bridge_bindings WHERE session_id=?', (po,))
        host = _one(project, 'SELECT host_id FROM remote_session_hosts WHERE session_id=?', (po,))
        dispatcher = _one(project, 'SELECT owner,acquired_at FROM bridge_dispatcher WHERE host_id=?', (SELECTED_HOST,))
        runtime = _one(project, 'SELECT server,pid,heartbeat,error FROM bridge_client_runtime WHERE host_id=?', (SELECTED_HOST,))
        if tuple(projection) != tuple(root[:3]) or role[0] != project_root or binding[1] is not None or not isinstance(binding[0], str) or not binding[0] or host[0] != SELECTED_HOST:
            raise GoalSelectionUnavailable('role, thread or host changed')
        if not isinstance(dispatcher[0], str) or not dispatcher[0] or type(dispatcher[1]) not in (int,float):
            raise GoalSelectionUnavailable('dispatcher unavailable')
        if not isinstance(runtime[0], str) or type(runtime[1]) is not int or runtime[1] <= 1 or type(runtime[2]) not in (int,float) or not 0 <= now-runtime[2] <= 15 or runtime[3] is not None:
            raise GoalSelectionUnavailable('provider unavailable or stale')
        # Only trust scoped generations when every relevant schema hook is
        # present and verified. Legacy/incomplete stores retain the strict
        # whole-database fence; the observation itself never migrates stores.
        reg_tracked=tracking.validate(registry,tracking.REGISTRY)
        project_tracked=tracking.validate(project,tracking.PROJECT)
        reg_generation=((registry.execute('PRAGMA schema_version').fetchone()[0],)+tuple(tracking.value(registry,t,k) for t,k in
            (('root_config',1),('root_directory_owner',1),('root_projects',project_id)))
            if reg_tracked else ('legacy',registry_version))
        project_generation=((project.execute('PRAGMA schema_version').fetchone()[0],)+tuple(tracking.value(project,t,k) for t,k in
            (('meta','project_id'),('sessions',po),('project_orchestrators',po),
             ('root_projections',project_root),('bridge_bindings',po),('remote_session_hosts',po),
             ('bridge_dispatcher',SELECTED_HOST),('bridge_client_runtime',SELECTED_HOST),('session_resets',po)))
            if project_tracked else ('legacy',project_version))
        values = (reg_generation, project_generation, project_id, tuple(root), project_root, po, tuple(binding), tuple(host), tuple(dispatcher), runtime[0], runtime[1])
        generation = hashlib.sha256(repr(values).encode('utf-8')).hexdigest()
        registration = {'endpoint':runtime[0], 'pid':runtime[1], 'heartbeat':runtime[2], 'owner':dispatcher[0], 'acquired_at':dispatcher[1]}
        return SelectedGoalRead(project_id,po,binding[0],host[0],generation,now,registration)
    except GoalSelectionUnavailable:
        raise
    except Exception as exc:
        raise GoalSelectionUnavailable('binding unavailable') from exc

def require_current(original, registry, project, *, now=None):
    latest = select_designated_goal(registry, project, original.project_id, now=now)
    if (latest.project_id, latest.po_session, latest.thread_id, latest.host_id, latest.designation_generation) != (original.project_id, original.po_session, original.thread_id, original.host_id, original.designation_generation):
        raise GoalSelectionUnavailable('designation or provider generation changed')
    return latest
