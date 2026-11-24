"""Replace only one unavailable native binding of an admitted parked subordinate."""
from functools import partial
import time
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

from .core import CoordError, execution_epoch_handoff, execution_epoch_settle
from .creation_settings import creation_settings, settings_data
from .rpc import RpcError


def _receipt(db, identity):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_parked_binding_repairs'").fetchone():
        return None
    request, original, *_ = identity
    row = db.execute('SELECT * FROM root_parked_binding_repairs WHERE request_id=? OR original_request=?',
                     (request, original)).fetchone()
    if row is None:
        return None
    keys = ('request_id', 'original_request', 'project_id', 'stop_id', 'root_session',
            'target_session', 'old_thread', 'host_id')
    if tuple(row[key] for key in keys) != identity:
        raise CoordError('parked binding repair already has a different exact request or scope')
    receipt = {key: row[key] for key in (*keys, 'new_thread', 'status', 'rollout_path', 'settings_digest')}
    if row['status'] != 'published':
        receipt['native_outcome'] = 'candidate_unverified' if row['new_thread'] else 'unknown'
    if row['status'] == 'materializing':
        receipt['metadata_outcome'] = 'readback_required'
    return receipt


def effective_thread(db, original_request, stop, target, old_thread, host, digest):
    """Read the binding proof used by the existing park and release predicate."""
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_parked_binding_repairs'").fetchone():
        return old_thread
    row = db.execute('''SELECT stop_id,target_session,old_thread,host_id,settings_digest,status,new_thread,rollout_path
        FROM root_parked_binding_repairs WHERE original_request=?''', (original_request,)).fetchone()
    if row is None:
        return old_thread
    if tuple(row)[:6] != (stop, target, old_thread, host, digest, 'published') or not row['rollout_path']:
        return None
    return row['new_thread']


def _unsettled_worker_epoch(db, target, old_thread, template, host):
    # Provisioning resumes the PO settings template on behalf of this worker.
    # Only that detached metadata resume falls outside the worker's effect fence.
    return db.execute('''SELECT 1 FROM execution_epoch_handoffs
        WHERE settled_at IS NULL AND (actor=? OR thread=?)
          AND NOT (method='thread/resume' AND actor IS ? AND thread IS ? AND host=?)''',
                      (target, old_thread, target, template, host)).fetchone() is not None


def _scope(coord, registry, resets, identity):
    from .management import assert_parked_active
    from .root_team_reset import handoff_pending, quarantined
    from .turnover_bootstrap import authorized

    request, original, project, stop, root, target, old_thread, host = identity
    db = coord.db
    proof = assert_parked_active(db, original)
    if 'bootstrap_id' not in proof.keys():
        raise CoordError('binding repair requires original parked subordinate provenance')
    bootstrap = authorized(db, registry, proof['bootstrap_id'])
    job = resets.job(original)
    registration = db.execute('SELECT agent FROM sessions WHERE id=?', (target,)).fetchone()
    control = db.execute('SELECT mode FROM agent_controls WHERE target_session=?', (target,)).fetchone()
    if (not registration or registration['agent'] != job['target_agent']
            or db.execute('SELECT 1 FROM retired_sessions WHERE id=?', (target,)).fetchone()
            or target == root or resets.role_context(target)['role'] != 'worker'
            or db.execute('SELECT 1 FROM root_projections WHERE session_id=?', (target,)).fetchone()
            or db.execute('SELECT 1 FROM operator_identities WHERE session_id=?', (target,)).fetchone()):
        raise CoordError('parked binding repair requires an ordinary registered worker')
    if ((proof['project_id'], proof['stop_id'], proof['root_session'], proof['target_session'],
         proof['host_id']) != (project, stop, root, target, host)
            or bootstrap['status'] != 'completed' or bootstrap['target_session'] != job['requester_session']
            or job['status'] != 'parked' or job['kind'] != 'create'
            or job['new_thread_id'] != old_thread or job['queue_id'] is not None
            or not job['settings_digest'] or job['settings_digest'] != job['new_settings_digest']
            or quarantined(db, session=target, thread=old_thread, job=original)
            or (control and control['mode'] != 'running')):
        raise CoordError('parked binding repair stop, registration, provenance or settings changed')
    if db.execute('SELECT 1 FROM session_resets WHERE id=?', (request,)).fetchone():
        raise CoordError('parked binding repair request ID is already a session reset')
    if db.execute("""SELECT 1 FROM session_resets WHERE target_session=? AND id<>?
        AND status NOT IN ('completed','cancelled','failed')""", (target, original)).fetchone():
        raise CoordError('parked binding repair has a competing reset')
    resets.clean(target, old_thread)
    binding = db.execute('SELECT thread_id,parent_session,agent_path,updated_at FROM bridge_bindings WHERE session_id=?',
                         (target,)).fetchone()
    if not binding or tuple(binding)[:3] != (old_thread, None, None):
        raise CoordError('parked binding repair requires the exact original main binding')
    dispatcher = db.execute('SELECT owner,acquired_at FROM bridge_dispatcher WHERE host_id=?', (host,)).fetchone()
    if not dispatcher:
        raise CoordError('parked binding repair requires the current owning bridge')
    template = bootstrap['new_thread_id']
    if (_unsettled_worker_epoch(db, target, old_thread, template, host)
            or any(row['session_id'] == target or row['thread_id'] == old_thread or row['job_id'] == original
                   for row in handoff_pending(db))):
        raise CoordError('parked binding repair has an unsettled native effect for the old binding')
    return job, template, binding['updated_at'], tuple(dispatcher)


def _require_missing(rpc, thread):
    try:
        response = rpc.request('thread/read', {'threadId': thread})
    except RpcError as error:
        if (error.code, error.message) != (-32600, f'thread not loaded: {thread}'):
            raise
    else:
        state = response.get('thread', {})
        if state.get('id') != thread or state.get('status', {}).get('type') != 'notLoaded':
            raise CoordError('parked binding repair requires the exact unloaded old thread')
    try:
        rpc.request('thread/resume', {'threadId': thread, 'excludeTurns': True})
    except RpcError as error:
        if (error.code, error.message) == (-32600, f'no rollout found for thread id {thread}'):
            return
        raise
    raise CoordError('old parked thread can reopen; replacement creation withheld')


def _settings(rpc, job, template_thread):
    response = rpc.request('thread/resume', {'threadId': template_thread, 'excludeTurns': True})
    if response.get('thread', {}).get('id') != template_thread:
        raise CoordError('parked binding repair settings template identity changed')
    source, _ = settings_data(response)
    cwd = job['requested_cwd']
    if (job['project_scope'] or not cwd or job['requested_workspaces'] != [cwd]
            or not job['requested_model'] or not job['requested_effort']
            or not Path(cwd).is_dir() or str(Path(cwd).resolve()) != cwd):
        raise CoordError('cannot reproduce the original parked workspace and model settings')
    profile = (source.get('activePermissionProfile') or {}).get('id')
    if profile and not profile.startswith(':') and source['cwd'] != cwd:
        raise CoordError('cannot reproduce the original named workspace permission profile')
    wanted, digest = settings_data(creation_settings(source, job, cwd))
    if digest != job['settings_digest']:
        raise CoordError('cannot reproduce the original parked settings digest')
    return wanted, digest


def _record_candidate(coord, request, epoch, host, new_thread):
    with coord.tx() as db:
        if db.execute('SELECT 1 FROM bridge_bindings WHERE thread_id=?', (new_thread,)).fetchone():
            raise CoordError('parked repair candidate is already bound')
        execution_epoch_settle(db, epoch, host, 'submitted')
        changed = db.execute("""UPDATE root_parked_binding_repairs SET new_thread=?,status='created'
            WHERE request_id=? AND epoch_id=? AND status='creating'""", (new_thread, request, epoch))
        if changed.rowcount != 1:
            raise CoordError('parked binding repair creation receipt changed')


def repair(coord, registry, *, request, original, project, stop, target, old_thread, host, confirmed):
    from .bridge import Bridge
    from .provider_service import configured, option, selected_listener
    from .reset_worker import ResetWorker
    from .rpc import RpcClient
    from .service import ServiceManager
    from .session_reset import SessionResetState

    try:
        ids = tuple(str(UUID(value)) for value in (request, original, stop, old_thread))
    except (ValueError, TypeError, AttributeError) as error:
        raise CoordError('parked binding repair requires exact UUIDs') from error
    if confirmed is not True or ids != (request, original, stop, old_thread) or len(set(ids)) != 4:
        raise CoordError('parked binding repair requires distinct canonical UUIDs and explicit confirmation')
    root = coord.require_session()
    identity = (request, original, project, stop, root, target, old_thread, host)
    prior = _receipt(coord.db, identity)
    if prior:
        return prior
    resets = SessionResetState(coord)
    scope = _scope(coord, registry, resets, identity)
    job, template, binding_generation, _ = scope
    # Resolve the separately reviewed durability dependency before creating a native thread.
    try:
        from .parked_durability import verify_materialized
    except ImportError as error:
        raise CoordError('parked binding repair requires the reviewed durability provider') from error

    manager = ServiceManager()
    wrapper, command = configured(manager)
    endpoint = option(command, '--codex-server')
    listener = selected_listener(endpoint)
    with RpcClient(endpoint, timeout=5) as rpc:
        _require_missing(rpc, old_thread)
        wanted, digest = _settings(rpc, job, template)
        start = dict(ResetWorker.start_params(wanted), ephemeral=False)
        if configured(manager) != (wrapper, command) or selected_listener(endpoint) != listener:
            raise CoordError('configured native listener changed before parked binding repair')
        with coord.tx() as db:
            prior = _receipt(db, identity)
            if prior:
                return prior
            if _scope(coord, registry, resets, identity) != scope:
                raise CoordError('parked binding repair scope changed before native creation')
            db.execute("""CREATE TABLE IF NOT EXISTS root_parked_binding_repairs (
                request_id TEXT PRIMARY KEY, original_request TEXT NOT NULL UNIQUE,
                project_id TEXT NOT NULL, stop_id TEXT NOT NULL, root_session TEXT NOT NULL,
                target_session TEXT NOT NULL, old_thread TEXT NOT NULL, host_id TEXT NOT NULL,
                settings_digest TEXT NOT NULL, epoch_id TEXT NOT NULL, status TEXT NOT NULL,
                new_thread TEXT UNIQUE, metadata_epoch_id TEXT, rollout_path TEXT, published_at REAL)""")
            epoch = execution_epoch_handoff(db, 'thread/start', None, target, host)
            db.execute("""INSERT INTO root_parked_binding_repairs VALUES(?,?,?,?,?,?,?,?,?,?,'creating',NULL,NULL,NULL,NULL)""",
                       (*identity, digest, epoch))
        returned = rpc.request('thread/start', start)
        native_thread = returned.get('thread') if isinstance(returned, dict) else None
        new_thread = native_thread.get('id') if isinstance(native_thread, dict) else None
        try:
            if str(UUID(new_thread)) != new_thread or new_thread in (old_thread, template):
                raise ValueError()
        except (ValueError, TypeError, AttributeError) as error:
            raise CoordError('parked binding repair has no verified distinct native UUID; native outcome unknown') from error
        _record_candidate(coord, request, epoch, host, new_thread)
        if settings_data(returned)[1] != digest:
            raise CoordError('new parked binding settings differ; candidate remains unpublished')
        with coord.tx() as db:
            if _scope(coord, registry, resets, identity) != scope:
                raise CoordError('parked binding repair scope changed before materialization')
            metadata_epoch = execution_epoch_handoff(db, 'thread/name/set', new_thread, target, host)
            intent = db.execute("""UPDATE root_parked_binding_repairs SET status='materializing',metadata_epoch_id=?
                WHERE request_id=? AND new_thread=? AND status='created'""",
                                (metadata_epoch, request, new_thread))
            if intent.rowcount != 1:
                raise CoordError('parked binding repair materialization receipt changed')
        rpc.request('thread/name/set', {'threadId': new_thread, 'name': job['target_agent']})
        bridge = SimpleNamespace(state=None, rpc=rpc)
        bridge.queue = partial(Bridge.queue, bridge)
        proof = verify_materialized(ResetWorker(bridge), dict(job, new_thread_id=new_thread))
        if proof['thread_id'] != new_thread or proof['settings_digest'] != digest or not proof['path']:
            raise CoordError('parked binding repair durability proof differs from the candidate')
        _require_missing(rpc, old_thread)
        if configured(manager) != (wrapper, command) or selected_listener(endpoint) != listener:
            raise CoordError('configured native listener changed before parked binding publication')
        with coord.tx() as db:
            execution_epoch_settle(db, metadata_epoch, host, 'submitted')
            if _scope(coord, registry, resets, identity) != scope:
                raise CoordError('parked binding repair scope changed before publication')
            if db.execute('SELECT 1 FROM bridge_bindings WHERE thread_id=?', (new_thread,)).fetchone():
                raise CoordError('parked repair candidate is already bound')
            changed = db.execute("""UPDATE bridge_bindings SET thread_id=?,updated_at=?
                WHERE session_id=? AND thread_id=? AND updated_at=? AND parent_session IS NULL AND agent_path IS NULL""",
                                 (new_thread, time.time(), target, old_thread, binding_generation))
            receipt = db.execute("""UPDATE root_parked_binding_repairs SET status='published',rollout_path=?,published_at=?
                WHERE request_id=? AND epoch_id=? AND metadata_epoch_id=? AND new_thread=? AND status='materializing'""",
                                 (proof['path'], time.time(), request, epoch, metadata_epoch, new_thread))
            if changed.rowcount != 1 or receipt.rowcount != 1:
                raise CoordError('parked binding repair compare-and-swap changed; original binding retained')
    return _receipt(coord.db, identity)
