"""Root-authorized retirement of exact parked workers.

Retirement is deny-only: it does not settle native handoffs or assert closure.
"""
from pathlib import Path
import time
from uuid import UUID
from .core import CoordError


def retired(db, *, request_id=None, session=None, thread=None):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_parked_retirements'").fetchone():
        return False
    return bool((request_id and db.execute('SELECT 1 FROM root_parked_retirements WHERE request_id=?', (request_id,)).fetchone())
                or (session and db.execute('SELECT 1 FROM root_parked_retirements WHERE target_session=?', (session,)).fetchone())
                or (thread and _bound_schema_exists(db)
                    and db.execute('SELECT 1 FROM root_parked_bound_retirements WHERE old_thread=? OR candidate=?',
                                   (thread, thread)).fetchone()))


def assert_not_retired(db, request_id):
    if retired(db, request_id=request_id):
        raise CoordError('parked creation was retired; original native outcome remains unknown')


def schema(db):
    db.execute('''CREATE TABLE IF NOT EXISTS root_parked_retirements (
        request_id TEXT PRIMARY KEY, target_session TEXT NOT NULL UNIQUE,
        project_id TEXT NOT NULL, stop_id TEXT NOT NULL, bootstrap_id TEXT NOT NULL,
        root_session TEXT NOT NULL, po_session TEXT NOT NULL, po_thread TEXT NOT NULL,
        host_id TEXT NOT NULL, created_at REAL NOT NULL)''')
    db.execute("CREATE TRIGGER IF NOT EXISTS parked_retirement_no_update BEFORE UPDATE ON root_parked_retirements BEGIN SELECT RAISE(ABORT,'parked retirement immutable'); END")
    db.execute("CREATE TRIGGER IF NOT EXISTS parked_retirement_no_delete BEFORE DELETE ON root_parked_retirements BEGIN SELECT RAISE(ABORT,'parked retirement immutable'); END")
    db.execute("CREATE TRIGGER IF NOT EXISTS parked_retired_job_no_update BEFORE UPDATE ON session_resets WHEN EXISTS(SELECT 1 FROM root_parked_retirements WHERE request_id=OLD.id) BEGIN SELECT RAISE(ABORT,'retired parked job immutable'); END")
    db.execute("CREATE TRIGGER IF NOT EXISTS parked_retired_job_no_delete BEFORE DELETE ON session_resets WHEN EXISTS(SELECT 1 FROM root_parked_retirements WHERE request_id=OLD.id) BEGIN SELECT RAISE(ABORT,'retired parked job immutable'); END")


def retire(coord, registry, *, project, bootstrap_id, stop_id, po, thread, host, request_id, confirmed):
    if confirmed is not True:
        raise CoordError('parked retirement requires explicit Root confirmation')
    from .turnover_bootstrap import authorized
    from .management import assert_parked_active
    with coord.tx() as db:
        scope = authorized(db, registry, bootstrap_id)
        if (coord.require_session() != scope['root_session'] or scope['status'] != 'completed'
                or (project,stop_id,po,thread,host) != (scope['project_id'],scope['stop_id'],
                    scope['target_session'],scope['new_thread_id'],scope['host_id'])):
            raise CoordError('parked retirement requires the exact current Root, stop and successor PO')
        proof = db.execute('SELECT * FROM root_turnover_subordinates WHERE request_id=?', (request_id,)).fetchone()
        job = db.execute('SELECT * FROM session_resets WHERE id=?', (request_id,)).fetchone()
        if (not proof or not job or (proof['project_id'],proof['stop_id'],proof['bootstrap_id'],proof['root_session']) !=
                (project,stop_id,bootstrap_id,scope['root_session'])
                or (job['kind'],job['status'],job['requester_session'],job['target_session'],job['old_thread_id'],job['host_id']) !=
                   ('create','uncertain',po,proof['target_session'],thread,host)
                or job['new_thread_id'] is not None or job['queue_id'] is not None
                or db.execute('SELECT 1 FROM bridge_bindings WHERE session_id=?', (proof['target_session'],)).fetchone()
                or db.execute('SELECT 1 FROM root_projections WHERE session_id=?', (proof['target_session'],)).fetchone()
                or db.execute('SELECT 1 FROM project_orchestrators WHERE session_id=?', (proof['target_session'],)).fetchone()
                or db.execute('SELECT 1 FROM operator_identities WHERE session_id=?', (proof['target_session'],)).fetchone()):
            raise CoordError('parked retirement requires the exact unbound uncertain subordinate creation')
        assert_parked_active(db, request_id)
        schema(db)
        values = (request_id, proof['target_session'], project, stop_id, bootstrap_id,
                  scope['root_session'], po, thread, host)
        saved = db.execute('SELECT * FROM root_parked_retirements WHERE request_id=?', (request_id,)).fetchone()
        if saved:
            if tuple(saved)[:9] != values:
                raise CoordError('parked retirement retry scope differs')
        else:
            db.execute('INSERT INTO root_parked_retirements VALUES(?,?,?,?,?,?,?,?,?,?)', (*values,time.time()))
        return dict(request_id=request_id, target_session=proof['target_session'], project=project,
                    stop_id=stop_id, bootstrap_id=bootstrap_id, native_outcome='unknown',
                    native_closure_proven=False, retired=True)



def _bound_schema_exists(db):
    return db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_parked_bound_retirements'").fetchone()


def _bound_schema(db):
    schema(db)
    db.execute('''CREATE TABLE IF NOT EXISTS root_parked_bound_retirements (
        request_id TEXT PRIMARY KEY REFERENCES root_parked_retirements(request_id),
        agent TEXT NOT NULL, logical_owner TEXT NOT NULL, old_thread TEXT NOT NULL UNIQUE,
        old_rollout TEXT NOT NULL, repair_request_id TEXT NOT NULL UNIQUE, candidate TEXT UNIQUE,
        binding_updated_at REAL NOT NULL, repair_status TEXT, creation_epoch TEXT, metadata_epoch TEXT)''')
    for action in ('UPDATE', 'DELETE'):
        db.execute(f"CREATE TRIGGER IF NOT EXISTS parked_bound_retirement_no_{action.lower()} BEFORE {action} ON root_parked_bound_retirements BEGIN SELECT RAISE(ABORT,'parked bound retirement immutable'); END")


def _retained_repair(db, identity, old_thread, repair_request, candidate):
    original, target, project, stop, _, root, _, _, host = identity
    row = None
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_parked_binding_repairs'").fetchone():
        row = db.execute('SELECT * FROM root_parked_binding_repairs WHERE request_id=? OR original_request=?',
                         (repair_request, original)).fetchone()
    if row is None:
        if candidate:
            raise CoordError('expected unpublished repair candidate is missing')
        return None, None, None
    keys = ('request_id', 'original_request', 'project_id', 'stop_id', 'root_session', 'target_session', 'host_id')
    if (tuple(row[key] for key in keys) != (repair_request, original, project, stop, root, target, host)
            or row['old_thread'] != old_thread or row['status'] not in ('creating', 'created', 'materializing') or row['new_thread'] != candidate
            or row['rollout_path'] is not None or row['published_at'] is not None):
        raise CoordError('retained repair is published or differs from the exact retirement evidence')
    if candidate and db.execute('SELECT 1 FROM bridge_bindings WHERE thread_id=?', (candidate,)).fetchone():
        raise CoordError('unpublished repair candidate is bound elsewhere')
    return row['status'], row['epoch_id'], row['metadata_epoch_id']


def retire_bound(coord, registry, *, project, bootstrap_id, stop_id, po, thread, host, request_id,
                 target, agent, logical_owner, old_thread, old_rollout, repair_request, candidate, confirmed):
    """Archive one exact broken parked registration while leaving its native evidence unsettled."""
    from .management import assert_parked_active
    from .session_reset import SessionResetState
    from .turnover_bootstrap import authorized

    candidate = None if candidate == 'none' else candidate
    ids = (request_id, bootstrap_id, stop_id, thread, old_thread, repair_request)
    ids += (candidate,) if candidate is not None else ()
    try:
        canonical = tuple(str(UUID(value)) for value in ids)
    except (ValueError, TypeError, AttributeError) as error:
        raise CoordError('bound parked retirement requires exact UUIDs') from error
    if (confirmed is not True or canonical != ids or len(set(ids)) != len(ids)
            or not isinstance(logical_owner, str) or not logical_owner.strip() or not isinstance(old_rollout, str)):
        raise CoordError('bound parked retirement requires confirmation, owner and exact original rollout path')
    path = Path(old_rollout)
    if not path.is_absolute() or not path.name.endswith(f'-{old_thread}.jsonl'):
        raise CoordError('bound parked retirement requires the saved original rollout path')
    root = coord.require_session()
    scope_values = (request_id, target, project, stop_id, bootstrap_id, root, po, thread, host)
    details = (request_id, agent, logical_owner, old_thread, old_rollout, repair_request, candidate)
    resets = SessionResetState(coord)
    with coord.tx() as db:
        scope = authorized(db, registry, bootstrap_id)
        if (root != scope['root_session'] or scope['status'] != 'completed'
                or (project, stop_id, po, thread, host) != (scope['project_id'], scope['stop_id'],
                    scope['target_session'], scope['new_thread_id'], scope['host_id'])):
            raise CoordError('bound parked retirement requires the exact current Root, stop and successor PO')
        if retired(db, request_id=request_id, session=target):
            prior = db.execute('SELECT * FROM root_parked_retirements WHERE request_id=?', (request_id,)).fetchone()
            bound = (db.execute('SELECT * FROM root_parked_bound_retirements WHERE request_id=?',
                                (request_id,)).fetchone() if _bound_schema_exists(db) else None)
            if not prior or tuple(prior)[:9] != scope_values or not bound or tuple(bound)[:7] != details:
                raise CoordError('bound parked retirement retry scope differs')
            native = tuple(bound)[8:]
        else:
            proof = assert_parked_active(db, request_id)
            job = resets.job(request_id)
            binding = db.execute('SELECT thread_id,parent_session,agent_path,updated_at FROM bridge_bindings WHERE session_id=?',
                                 (target,)).fetchone()
            registration = db.execute('SELECT agent FROM sessions WHERE id=?', (target,)).fetchone()
            if ('bootstrap_id' not in proof.keys()
                    or (proof['project_id'], proof['stop_id'], proof['bootstrap_id'], proof['root_session'], proof['target_session'])
                    != (project, stop_id, bootstrap_id, root, target)
                    or (job['kind'], job['status'], job['target_session'], job['target_agent'], job['new_thread_id'], job['queue_id'])
                    != ('create', 'parked', target, agent, old_thread, None)
                    or not registration or registration['agent'] != agent
                    or not binding or tuple(binding)[:3] != (old_thread, None, None)
                    or target in (root, po)
                    or resets.role_context(target)['role'] != 'worker'
                    or db.execute('SELECT 1 FROM root_projections WHERE session_id=?', (target,)).fetchone()
                    or db.execute('SELECT 1 FROM operator_identities WHERE session_id=?', (target,)).fetchone()):
                raise CoordError('bound parked retirement requires the exact current original worker binding')
            resets.clean(target, old_thread)
            native = _retained_repair(db, scope_values, old_thread, repair_request, candidate)
            try:
                path.lstat()
            except FileNotFoundError:
                pass
            else:
                raise CoordError('original parked rollout exists; broken binding retirement withheld')
            _bound_schema(db)
            now = time.time()
            db.execute('INSERT INTO root_parked_retirements VALUES(?,?,?,?,?,?,?,?,?,?)', (*scope_values, now))
            db.execute('INSERT INTO root_parked_bound_retirements VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                       (*details, binding['updated_at'], *native))
            db.execute('INSERT INTO retired_sessions(id,agent,registered_at,removed_at) '
                       'SELECT id,agent,registered_at,? FROM sessions WHERE id=?', (now, target))
            db.execute('DELETE FROM bridge_bindings WHERE session_id=? AND thread_id=?', (target, old_thread))
            db.execute('DELETE FROM sessions WHERE id=?', (target,))
        return dict(request_id=request_id, target_session=target, project=project, stop_id=stop_id,
                    bootstrap_id=bootstrap_id, agent=agent, logical_owner=logical_owner, old_thread=old_thread,
                    old_rollout=old_rollout, repair_request_id=repair_request, retained_candidate=candidate,
                    repair_status=native[0], creation_epoch=native[1], metadata_epoch=native[2],
                    native_outcome='unknown', native_closure_proven=False, retired=True)
