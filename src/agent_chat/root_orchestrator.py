"""One control conversation, with durable, quiet identities in isolated projects.

This registry never dispatches model input itself. Project mains use the existing
fresh-conversation state machine and authenticated host worker.
"""
import contextlib
import hashlib
import json
import math
import os
import sqlite3
from pathlib import PurePosixPath
import time
import uuid

from .core import Coordinator, CoordError


_MANAGED_NATIVE_READS = 'root_managed_native_reads'
_MANAGED_NATIVE_SCOPE_FIELDS = (
    'request_id', 'requester_session', 'home_project', 'home_session', 'home_thread', 'home_host',
    'target_session', 'host_id', 'thread_id', 'project_id', 'project_root', 'po_session',
    'stop_id', 'admission_request_id', 'message_id', 'job_id',
    'inspection_request_id')
_MANAGED_NATIVE_RESULT_FIELDS = (
    'native_state', 'direct_input', 'turn_id', 'turn_id_known', 'queue_count', 'queue_state',
    'checked_at', 'error_code')


def _managed_native_base_schema(db):
    required = {
        'meta': {'key', 'value'},
        'bridge_jobs': {'id', 'thread_id', 'status', 'queue_id'},
        'bridge_deliveries': {'job_id', 'message_id'},
        'message_batches': {'message_id'},
        'root_team_stop_task_admissions': {'request_id', 'stop_id', 'root_session', 'po_session',
            'target_session', 'thread_id', 'host_id', 'message_id', 'inspection_request_id', 'purpose'},
        'bridge_bindings': {'session_id', 'thread_id', 'parent_session'},
        'sessions': {'id', 'agent'},
        'root_projections': {'session_id', 'home_project', 'home_session', 'thread_id'},
        'root_team_stops': {'id', 'root_session', 'active'},
        'root_team_stop_targets': {'stop_id', 'target_session', 'requester_session', 'thread_id', 'host_id', 'status'},
        'root_team_stop_po_admissions': {'stop_id', 'root_session', 'target_session', 'thread_id', 'host_id'},
        'project_orchestrators': {'session_id', 'root_session'},
        'messages': {'id', 'sender_session', 'recipient_session'},
        'agent_control_requests': {'id', 'requester_session', 'target_session', 'host_id', 'thread_id',
            'action', 'interrupt', 'status'},
        'session_resets': {'target_session', 'status'},
        'remote_session_hosts': {'session_id', 'host_id'},
    }
    tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    for table, columns in required.items():
        if table not in tables:
            raise CoordError('managed native read requires the existing scoped project schema')
        actual = {row[1] for row in db.execute(f'PRAGMA table_info({table})')}
        if not columns <= actual:
            raise CoordError('managed native read requires the existing scoped project schema')
    meta = dict(db.execute("SELECT key,value FROM meta WHERE key IN ('project_id','schema_version')"))
    if meta.get('schema_version') != '1':
        raise CoordError('managed native read project schema version is unsupported')
    return meta


def _managed_native_target_is_worker(target_session, target_record, *, project_root, home_root,
                                    po_session, task_scope_matches, binding_matches, host_matches):
    """Use the exact admitted task/binding, not the free-form ``sessions.agent`` label."""
    protected = {project_root, home_root, po_session}
    return bool(target_record and target_session not in protected and task_scope_matches
                and binding_matches and host_matches)


def _managed_native_scope(db, row, *, actor_session, diagnostic=False):
    """Evaluate the exact foreground scope once; diagnosis only exposes booleans."""
    unknown = 'unknown'
    names = (
        'project_schema_matches', 'actor_matches_request', 'task_association_present',
        'project_root_is_current', 'project_root_binding_matches', 'project_root_host_matches',
        'actor_is_authorized_root_or_admitted_po', 'po_role_matches_root',
        'po_binding_matches_admission', 'po_host_matches_admission', 'worker_role_is_worker',
        'worker_binding_matches_request', 'worker_host_matches_request',
        'task_scope_matches_request', 'only_expected_active_stop', 'stop_matches_project_root',
        'stop_is_active', 'target_admission_matches_request', 'target_admission_requester_matches_root',
        'target_admission_status_admissible', 'po_admission_matches_current_binding',
        'message_sender_recipient_matches', 'inspection_request_matches_target_thread_host',
        'inspection_requester_is_root_or_admitted_po', 'inspection_request_status_admissible',
        'original_wake_is_unique_exact_delivery', 'original_wake_not_cancelled',
        'message_not_batched', 'no_other_pending_control', 'no_pending_reset', 'worker_not_quarantined',
    )
    def result(values, scope=None):
        if diagnostic:
            return {'checks': {name: values.get(name, unknown) for name in names}, 'scope': scope}
        return values, scope
    try:
        meta = _managed_native_base_schema(db)
    except CoordError:
        if diagnostic:
            return result({})
        raise
    values = {name: unknown for name in names}
    values['project_schema_matches'] = (unknown if diagnostic and 'project_id' not in meta
                                        else meta.get('project_id') == row['project_id'])
    values['actor_matches_request'] = actor_session == row['requester_session']
    if not values['project_schema_matches'] or not values['actor_matches_request']:
        if diagnostic:
            return result(values)
        raise CoordError('managed native read actor or project differs from its immutable request')
    target, thread, host = row['target_session'], row['thread_id'], row['host_id']
    task = db.execute('''SELECT request_id,root_session,po_session,target_session,thread_id,host_id,
        message_id,inspection_request_id,purpose FROM root_team_stop_task_admissions
        WHERE request_id=? AND stop_id=? AND message_id=?''',
        (row['admission_request_id'], row['stop_id'], row['message_id'])).fetchone()
    values['task_association_present'] = (unknown if diagnostic and not task else bool(task))
    if not task:
        if diagnostic:
            return result(values)
        raise CoordError('managed native read task association is missing')
    project_root = task['root_session']
    root = db.execute('''SELECT home_project,home_session,thread_id FROM root_projections
        WHERE session_id=?''', (project_root,)).fetchone()
    project_root_binding = db.execute('SELECT thread_id,parent_session FROM bridge_bindings WHERE session_id=?',
                                      (project_root,)).fetchone()
    project_root_session = db.execute('SELECT agent FROM sessions WHERE id=?', (project_root,)).fetchone()
    project_root_host = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?',
                                   (project_root,)).fetchone()
    role = db.execute('SELECT 1 FROM project_orchestrators WHERE session_id=? AND root_session=?',
                      (row['po_session'], project_root)).fetchone()
    po_binding = db.execute('SELECT thread_id,parent_session FROM bridge_bindings WHERE session_id=?',
                             (row['po_session'],)).fetchone()
    po_host = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?',
                         (row['po_session'],)).fetchone()
    binding = db.execute('SELECT thread_id,parent_session FROM bridge_bindings WHERE session_id=?',
                         (target,)).fetchone()
    worker_session = db.execute('SELECT agent FROM sessions WHERE id=?', (target,)).fetchone()
    host_row = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (target,)).fetchone()
    stop = db.execute('SELECT root_session,active FROM root_team_stops WHERE id=?', (row['stop_id'],)).fetchone()
    target_admission = db.execute('''SELECT requester_session,thread_id,host_id,status FROM root_team_stop_targets
        WHERE stop_id=? AND target_session=?''', (row['stop_id'], target)).fetchone()
    po_admission = db.execute('''SELECT root_session,target_session,thread_id,host_id
        FROM root_team_stop_po_admissions WHERE stop_id=?''', (row['stop_id'],)).fetchone()
    message = db.execute('SELECT sender_session,recipient_session FROM messages WHERE id=?',
                         (row['message_id'],)).fetchone()
    inspection = db.execute('''SELECT requester_session,target_session,host_id,thread_id,action,interrupt,status
        FROM agent_control_requests WHERE id=?''', (row['inspection_request_id'],)).fetchone()
    wakes = db.execute('''SELECT j.id,j.thread_id,j.status,j.queue_id,m.id AS message_id,m.sender_session,
        m.recipient_session FROM bridge_jobs j JOIN bridge_deliveries d ON d.job_id=j.id
        JOIN messages m ON m.id=d.message_id WHERE j.id=? AND m.id=?''',
        (row['job_id'], row['message_id'])).fetchall()
    wake_delivery_count = db.execute('''SELECT count(*) FROM bridge_deliveries d
        JOIN messages m ON m.id=d.message_id WHERE d.job_id=?''', (row['job_id'],)).fetchone()[0]
    from .root_lifecycle import po_admitted
    active_stops = db.execute('SELECT count(*) FROM root_team_stops WHERE active=1').fetchone()[0]
    actor_is_root = bool(root and row['requester_session'] == root['home_session'])
    actor_is_po = (row['requester_session'] == row['po_session']
                   and po_admitted(db, row['stop_id'], row['po_session']))
    task_scope_matches = tuple(task[k] for k in ('root_session','po_session','target_session','thread_id','host_id','message_id','inspection_request_id','purpose')) == (
        project_root,row['po_session'],target,thread,host,row['message_id'],row['inspection_request_id'],'investigation')
    worker_binding_matches = bool(binding and binding['parent_session'] is None and binding['thread_id'] == thread)
    worker_host_matches = bool(host_row and host_row['host_id'] == host)
    values.update({
        'project_root_is_current': bool(root and (root['home_project'], root['home_session']) ==
                                        (row['home_project'], row['home_session']) and
                                        project_root_session and project_root_session['agent'] == 'root'),
        'project_root_binding_matches': bool(root and project_root_binding and
            project_root_binding['parent_session'] is None and project_root_binding['thread_id'] == root['thread_id']),
        'project_root_host_matches': bool(project_root_host and project_root_host['host_id'] == host),
        'actor_is_authorized_root_or_admitted_po': bool(actor_is_root or actor_is_po),
        'po_role_matches_root': bool(role),
        'po_binding_matches_admission': bool(po_admission and po_binding and
            po_binding['parent_session'] is None and po_binding['thread_id'] == po_admission['thread_id']),
        'po_host_matches_admission': bool(po_admission and po_host and po_host['host_id'] == po_admission['host_id']),
        # The immutable investigation admission establishes the worker/task
        # relationship.  Registration labels are descriptive, not an enum.
        # Keep the management identities explicitly excluded.
        'worker_role_is_worker': (
            unknown if diagnostic and (not worker_session or not binding or not host_row) else
            _managed_native_target_is_worker(
                target, worker_session, project_root=project_root,
                home_root=row['home_session'], po_session=row['po_session'],
                task_scope_matches=task_scope_matches, binding_matches=worker_binding_matches,
                host_matches=worker_host_matches)),
        'worker_binding_matches_request': worker_binding_matches,
        'worker_host_matches_request': worker_host_matches,
        'task_scope_matches_request': task_scope_matches,
        'only_expected_active_stop': active_stops == 1,
        'stop_matches_project_root': bool(stop and stop['root_session'] == project_root),
        'stop_is_active': bool(stop and stop['active'] == 1),
        'target_admission_matches_request': bool(target_admission and tuple(target_admission[:3]) == (project_root,thread,host)),
        'target_admission_requester_matches_root': bool(target_admission and target_admission['requester_session'] == project_root),
        'target_admission_status_admissible': bool(target_admission and target_admission['status'] in ('pending','waiting','completed')),
        'po_admission_matches_current_binding': bool(po_admission and po_binding and po_host and
            tuple(po_admission) == (project_root,row['po_session'],po_binding['thread_id'],po_host['host_id'])),
        'message_sender_recipient_matches': bool(message and tuple(message) == (row['po_session'],target)),
        'inspection_request_matches_target_thread_host': bool(inspection and tuple(inspection[k] for k in
            ('target_session','host_id','thread_id','action','interrupt')) == (target,host,thread,'inspect',0)),
        'inspection_requester_is_root_or_admitted_po': bool(inspection and
            (inspection['requester_session'] == project_root or
             (inspection['requester_session'] == row['po_session'] and po_admitted(db,row['stop_id'],row['po_session'])))),
        'inspection_request_status_admissible': bool(inspection and inspection['status'] in ('pending','waiting','completed')),
        'original_wake_is_unique_exact_delivery': len(wakes) == 1 and wake_delivery_count == 1 and
            tuple(wakes[0][k] for k in ('id','thread_id','message_id','sender_session','recipient_session')) ==
            (row['job_id'],thread,row['message_id'],row['po_session'],target),
        'original_wake_not_cancelled': len(wakes) == 1 and wakes[0]['status'] != 'cancelled',
        'message_not_batched': not db.execute('SELECT 1 FROM message_batches WHERE message_id=?',(row['message_id'],)).fetchone(),
    })
    first_group = names[:-3]
    if not diagnostic and not all(values[name] is True for name in first_group):
        raise CoordError('managed native read ownership, stop, task or original wake changed')
    pending = db.execute('''SELECT id FROM agent_control_requests WHERE target_session=? AND id<>?
        AND status NOT IN ('completed','failed','cancelled','resolved_unchanged') LIMIT 1''',
        (target, row['inspection_request_id'])).fetchone()
    reset = db.execute('''SELECT id FROM session_resets WHERE target_session=?
        AND status NOT IN ('completed','cancelled','failed') LIMIT 1''', (target,)).fetchone()
    from .root_team_reset import quarantined
    values['no_other_pending_control'] = not pending
    values['no_pending_reset'] = not reset
    values['worker_not_quarantined'] = not quarantined(db, session=target)
    if diagnostic:
        # A successfully read store with a missing required record is not
        # evidence that the relationship mismatches. Preserve that distinction
        # in the diagnostic projection while the foreground path below keeps
        # its existing fail-closed boolean guards.
        required_rows = {
            'project_root_is_current': (root, project_root_session),
            'project_root_binding_matches': (root, project_root_binding),
            'project_root_host_matches': (project_root_host,),
            'actor_is_authorized_root_or_admitted_po': (root, po_admission),
            'po_role_matches_root': (role,),
            'po_binding_matches_admission': (po_admission, po_binding),
            'po_host_matches_admission': (po_admission, po_host),
            'worker_role_is_worker': (worker_session,),
            'worker_binding_matches_request': (binding,),
            'worker_host_matches_request': (host_row,),
            'stop_matches_project_root': (stop,),
            'stop_is_active': (stop,),
            'target_admission_matches_request': (target_admission,),
            'target_admission_requester_matches_root': (target_admission,),
            'target_admission_status_admissible': (target_admission,),
            'po_admission_matches_current_binding': (po_admission, po_binding, po_host),
            'message_sender_recipient_matches': (message,),
            'inspection_request_matches_target_thread_host': (inspection,),
            'inspection_requester_is_root_or_admitted_po': (inspection, po_admission),
            'inspection_request_status_admissible': (inspection,),
            'original_wake_is_unique_exact_delivery': (wakes[0] if wakes else None,),
            'original_wake_not_cancelled': (wakes[0] if wakes else None,),
        }
        for name, dependencies in required_rows.items():
            if any(dependency is None for dependency in dependencies):
                values[name] = unknown
        return result(values, dict(project_root=project_root, original_job=dict(id=wakes[0]['id'], status=wakes[0]['status'], queue_id=wakes[0]['queue_id'])) if len(wakes)==1 else None)
    if not all(values[name] is True for name in names[-3:]):
        raise CoordError('managed native read withheld while target control, reset or quarantine is unresolved')
    wake = wakes[0]
    return dict(project_root=project_root, original_job=dict(id=wake['id'], status=wake['status'], queue_id=wake['queue_id']))

def _managed_native_result_schema(db):
    table = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (_MANAGED_NATIVE_READS,)).fetchone()
    if not table:
        return False
    expected = set(_MANAGED_NATIVE_SCOPE_FIELDS) | {'status','native_state','direct_input','turn_id',
        'turn_id_known','queue_count','queue_state','checked_at','error_code','created_at','updated_at'}
    actual = {row[1] for row in db.execute(f'PRAGMA table_info({_MANAGED_NATIVE_READS})')}
    if not expected <= actual:
        raise CoordError('managed native read result schema is unavailable')
    return True


def _ensure_managed_native_result_schema(db):
    if not _managed_native_result_schema(db):
        db.execute('''CREATE TABLE root_managed_native_reads (
            request_id TEXT PRIMARY KEY, requester_session TEXT NOT NULL, home_project TEXT NOT NULL,
            home_session TEXT NOT NULL, home_thread TEXT NOT NULL, home_host TEXT NOT NULL,
            target_session TEXT NOT NULL,
            host_id TEXT NOT NULL, thread_id TEXT NOT NULL, project_id TEXT NOT NULL, project_root TEXT NOT NULL,
            po_session TEXT NOT NULL, stop_id TEXT NOT NULL, admission_request_id TEXT NOT NULL,
            message_id TEXT NOT NULL, job_id TEXT NOT NULL, inspection_request_id TEXT NOT NULL,
            status TEXT NOT NULL, native_state TEXT, direct_input INTEGER, turn_id TEXT, turn_id_known INTEGER NOT NULL,
            queue_count INTEGER, queue_state TEXT, checked_at REAL, error_code TEXT,
            created_at REAL NOT NULL, updated_at REAL NOT NULL)''')
    immutable = ' OR '.join(f'NEW.{field} IS NOT OLD.{field}' for field in _MANAGED_NATIVE_SCOPE_FIELDS)
    trigger = db.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' AND name='root_managed_native_reads_cas'").fetchone()
    if not trigger:
        db.execute(f'''CREATE TRIGGER root_managed_native_reads_cas BEFORE UPDATE ON {_MANAGED_NATIVE_READS}
            WHEN OLD.status <> 'reading' OR NEW.status NOT IN ('observed','unavailable','failed','unknown') OR {immutable}
            BEGIN SELECT RAISE(ABORT,'managed native read scope is immutable'); END''')
    delete_trigger = db.execute("SELECT 1 FROM sqlite_master WHERE type='trigger' AND name='root_managed_native_reads_no_delete'").fetchone()
    if not delete_trigger:
        db.execute(f'''CREATE TRIGGER root_managed_native_reads_no_delete BEFORE DELETE ON {_MANAGED_NATIVE_READS}
            BEGIN SELECT RAISE(ABORT,'managed native read history is immutable'); END''')


def _managed_native_public(row, scope=None):
    result = None
    if row['status'] in ('observed','unavailable','failed','unknown'):
        result = {field: row[field] for field in _MANAGED_NATIVE_RESULT_FIELDS}
        if result['direct_input'] is not None:
            result['direct_input'] = bool(result['direct_input'])
        result['turn_id_known'] = bool(result['turn_id_known'])
    return dict(request_id=row['request_id'], status=row['status'], target_project=row['project_id'],
        project_root=row['project_root'], po_session=row['po_session'], stop_id=row['stop_id'],
        admission_request_id=row['admission_request_id'], target_session=row['target_session'],
        thread_id=row['thread_id'], host_id=row['host_id'], message_id=row['message_id'],
        job_id=row['job_id'], inspection_request_id=row['inspection_request_id'],
        original_job=(scope or {}).get('original_job'), result=result, checked_at=row['updated_at'])


def _managed_native_request(db, *, actor_session, home_project, home_session, home_thread, home_host,
                            target_session, expected_thread, expected_host, project_id, project_root,
                            po_session, stop_id, admission_request_id,
                            message_id, job_id, inspection_request_id, request_id):
    if (not isinstance(job_id, str) or len(job_id) != 37 or not job_id.startswith('wake_')
            or any(char not in '0123456789abcdef' for char in job_id[5:])):
        raise CoordError('managed native read requires the exact bounded bridge wake ID')
    try:
        normalized = {key: str(uuid.UUID(value)) for key, value in {
            'request_id': request_id, 'stop_id': stop_id, 'admission_request_id': admission_request_id,
            'inspection_request_id': inspection_request_id}.items()}
    except (TypeError, ValueError, AttributeError) as error:
        raise CoordError('managed native read requires exact request, stop, admission and inspection UUIDs') from error
    request_id = normalized['request_id']
    row = dict(request_id=request_id, requester_session=actor_session, home_project=home_project,
        home_session=home_session, home_thread=home_thread, home_host=home_host, target_session=target_session,
        host_id=expected_host, thread_id=expected_thread, project_id=project_id, project_root=project_root,
        po_session=po_session, stop_id=normalized['stop_id'], admission_request_id=normalized['admission_request_id'],
        message_id=message_id, job_id=job_id, inspection_request_id=normalized['inspection_request_id'])
    meta = _managed_native_base_schema(db)
    if meta.get('project_id') != project_id:
        raise CoordError('managed native read project metadata changed')
    db.execute('BEGIN IMMEDIATE')
    try:
        has_store = _managed_native_result_schema(db)
        existing = db.execute(f'SELECT * FROM {_MANAGED_NATIVE_READS} WHERE request_id=?', (request_id,)).fetchone() if has_store else None
        if existing:
            saved = {key: existing[key] for key in _MANAGED_NATIVE_SCOPE_FIELDS}
            if saved != row:
                raise CoordError('managed native read retry differs from its immutable scope')
            scope = _managed_native_scope(db, dict(existing), actor_session=actor_session)
            db.commit()
            return _managed_native_public(existing, scope), False
        scope = _managed_native_scope(db, row, actor_session=actor_session)
        _ensure_managed_native_result_schema(db)
        now = time.time()
        db.execute(f'''INSERT INTO {_MANAGED_NATIVE_READS}(
            request_id,requester_session,home_project,home_session,home_thread,home_host,target_session,
            host_id,thread_id,project_id,project_root,po_session,
            stop_id,admission_request_id,message_id,job_id,inspection_request_id,status,native_state,direct_input,
            turn_id,turn_id_known,queue_count,queue_state,checked_at,error_code,created_at,updated_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'reading',NULL,NULL,NULL,0,NULL,NULL,NULL,NULL,?,?)''',
            tuple(row[key] for key in _MANAGED_NATIVE_SCOPE_FIELDS) + (now, now))
        saved = db.execute(f'SELECT * FROM {_MANAGED_NATIVE_READS} WHERE request_id=?', (request_id,)).fetchone()
        db.commit()
        return _managed_native_public(saved, scope), True
    except BaseException:
        db.rollback()
        raise


@contextlib.contextmanager
def _managed_native_write_db(path):
    """Open an existing target store read/write without schema migrations."""
    with contextlib.closing(sqlite3.connect(path.as_uri() + '?mode=rw', uri=True, timeout=10)) as db:
        db.row_factory = sqlite3.Row
        yield db


def _managed_native_complete(db, request_id, status, observation, *, actor_session):
    if status not in ('observed','unavailable','failed','unknown') or not isinstance(observation, dict) or set(observation) != set(_MANAGED_NATIVE_RESULT_FIELDS):
        raise CoordError('managed native read result is invalid')
    if (observation['native_state'] not in ('idle','active','notLoaded','unknown')
            or (observation['direct_input'] is not None and type(observation['direct_input']) is not bool)
            or type(observation['turn_id_known']) is not bool or observation['turn_id'] is not None
            or observation['turn_id_known'] or observation['queue_state'] not in ('known','unknown','not_loaded')
            or (observation['queue_count'] is not None and
                (type(observation['queue_count']) is not int or not 0 <= observation['queue_count'] <= 1000))
            or not isinstance(observation['checked_at'], (int,float)) or isinstance(observation['checked_at'],bool)
            or not math.isfinite(observation['checked_at'])
            or observation['error_code'] not in (None,'scope_changed','native_read_failed','queue_read_failed',
                'queue_limit_exceeded','invalid_native_response','worker_interrupted')
            or (status in ('observed','unavailable') and observation['error_code'] is not None)
            or (status == 'observed' and (observation['native_state'] == 'notLoaded'
                or observation['queue_state'] != 'known'))
            or (status == 'unavailable' and observation['native_state'] != 'notLoaded')
            or (status in ('failed','unknown') and (observation['native_state'] != 'unknown'
                or observation['direct_input'] is not None or observation['queue_count'] is not None))):
        raise CoordError('managed native read result is outside its narrow metadata schema')
    db.execute('BEGIN IMMEDIATE')
    try:
        row = db.execute(f'SELECT * FROM {_MANAGED_NATIVE_READS} WHERE request_id=?', (request_id,)).fetchone()
        if not row or row['status'] != 'reading' or row['requester_session'] != actor_session:
            raise CoordError('managed native read is no longer the exact in-flight request')
        scope = _managed_native_scope(db, dict(row), actor_session=actor_session)
        if scope is None:
            raise CoordError('managed native read scope is unavailable')
        db.execute(f'''UPDATE {_MANAGED_NATIVE_READS} SET status=?,native_state=?,direct_input=?,turn_id=?,
            turn_id_known=?,queue_count=?,queue_state=?,checked_at=?,error_code=?,updated_at=?
            WHERE request_id=? AND status='reading' ''',
            (status, observation['native_state'], None if observation['direct_input'] is None else
             int(observation['direct_input']), observation['turn_id'], int(observation['turn_id_known']),
             observation['queue_count'], observation['queue_state'], observation['checked_at'],
             observation['error_code'], time.time(), request_id))
        saved = db.execute(f'SELECT * FROM {_MANAGED_NATIVE_READS} WHERE request_id=?', (request_id,)).fetchone()
        db.commit()
        return _managed_native_public(saved, scope)
    except CoordError:
        db.rollback()
        # If scope drifted while the call was in flight, persist only a withheld terminal result.
        db.execute('BEGIN IMMEDIATE')
        try:
            current = db.execute(f'SELECT * FROM {_MANAGED_NATIVE_READS} WHERE request_id=?', (request_id,)).fetchone()
            if not current or current['status'] != 'reading' or current['requester_session'] != actor_session:
                raise CoordError('managed native read changed before withheld result persistence')
            observation = {'native_state':'unknown','direct_input':None,'turn_id':None,'turn_id_known':False,
                'queue_count':None,'queue_state':'unknown','checked_at':time.time(),'error_code':'scope_changed'}
            _managed_native_base_schema(db)
            db.execute(f'''UPDATE {_MANAGED_NATIVE_READS} SET status='unknown',native_state='unknown',direct_input=NULL,
                turn_id=NULL,turn_id_known=0,queue_count=NULL,queue_state='unknown',checked_at=?,error_code='scope_changed',
                updated_at=? WHERE request_id=? AND status='reading' ''',
                (observation['checked_at'],time.time(),request_id))
            saved = db.execute(f'SELECT * FROM {_MANAGED_NATIVE_READS} WHERE request_id=?', (request_id,)).fetchone()
            db.commit()
            return _managed_native_public(saved, None)
        except BaseException:
            db.rollback()
            raise


def _read_managed_native_observation(rpc, thread_id):
    """Call only bounded status/queue read methods; never retain payload contents."""
    from .rpc import RpcError, TransportError

    def unknown(code):
        return {'native_state':'unknown','direct_input':None,'turn_id':None,'turn_id_known':False,
            'queue_count':None,'queue_state':'unknown','checked_at':time.time(),'error_code':code}

    try:
        response = rpc.request('thread/read', {'threadId':thread_id,'includeTurns':False})
    except (RpcError, TransportError, KeyError, TypeError):
        return unknown('native_read_failed')
    thread = response.get('thread') if isinstance(response, dict) else None
    if not isinstance(thread, dict) or thread.get('id') != thread_id:
        return unknown('invalid_native_response')
    raw_status = thread.get('status')
    state = raw_status.get('type') if isinstance(raw_status, dict) else None
    if state not in ('idle','active','notLoaded'):
        return unknown('invalid_native_response')
    direct = thread.get('canAcceptDirectInput')
    direct = direct if type(direct) is bool else None
    if state == 'notLoaded':
        return {'native_state':state,'direct_input':None,'turn_id':None,'turn_id_known':False,
            'queue_count':None,'queue_state':'not_loaded','checked_at':time.time(),'error_code':None}
    count, cursor, seen = 0, None, set()
    try:
        for _ in range(10):
            params = {'threadId':thread_id,'limit':100}
            if cursor is not None:
                params['cursor'] = cursor
            page = rpc.request('thread/queue/list', params)
            if not isinstance(page, dict) or not isinstance(page.get('data'), list):
                return unknown('queue_read_failed')
            count += len(page['data'])
            if count > 1000:
                return unknown('queue_limit_exceeded')
            cursor = page.get('nextCursor')
            if cursor is None:
                return {'native_state':state,'direct_input':direct,'turn_id':None,'turn_id_known':False,
                    'queue_count':count,'queue_state':'known','checked_at':time.time(),'error_code':None}
            if not isinstance(cursor, str) or not cursor or cursor in seen:
                return unknown('queue_read_failed')
            seen.add(cursor)
        return unknown('queue_limit_exceeded')
    except (RpcError, TransportError, KeyError, TypeError):
        return unknown('queue_read_failed')


def projection(db, session):
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_projections'").fetchone():
        row = db.execute('SELECT * FROM root_projections WHERE session_id=?', (session,)).fetchone()
        return dict(row) if row else None
    return None


def quiet(db, session):
    return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='quiet_root_sessions'").fetchone()
                and db.execute('SELECT 1 FROM quiet_root_sessions WHERE session_id=?', (session,)).fetchone())


def assert_local_lifecycle(db, session):
    if projection(db, session):
        raise CoordError('global root binding is shared across projects; use root detach before local reset, stop, removal or rebinding')


def assert_management_target(db, caller, target, *, inspect=False):
    # Read-only inspection also supports adopting an existing project main.
    if inspect:
        return
    if projection(db, caller):
        if not db.execute('SELECT 1 FROM project_orchestrators WHERE session_id=? AND root_session=?', (target, caller)).fetchone():
            if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_direct_creations'").fetchone():
                direct = db.execute('''SELECT d.reset_id,d.project_id,rp.home_session FROM root_direct_creations d
                  JOIN agent_management m ON m.target_session=d.target_session AND m.manager_session=d.root_session AND m.request_id=d.request_id
                  JOIN root_projections rp ON rp.session_id=d.root_session AND rp.home_project='default' AND rp.paused=0
                  JOIN root_team_reset_requests r ON r.id=d.reset_id AND r.root_session=rp.home_session AND r.project_id=d.project_id AND r.status='completed'
                  JOIN session_resets j ON j.id=d.request_id AND j.target_session=d.target_session AND j.requester_session=d.root_session AND j.kind='create'
                  JOIN bridge_bindings b ON b.session_id=d.root_session AND b.thread_id=d.thread_id AND b.thread_id=rp.thread_id AND b.parent_session IS NULL
                  JOIN remote_session_hosts rh ON rh.session_id=d.root_session AND rh.host_id=d.host_id
                  JOIN remote_session_hosts h ON h.session_id=d.target_session AND h.host_id=d.host_id
                  LEFT JOIN bridge_bindings child ON child.session_id=d.target_session
                  WHERE d.target_session=? AND d.root_session=? AND d.project_id=?
                    AND (child.session_id IS NULL OR (child.thread_id=j.new_thread_id AND child.parent_session IS NULL))
                    AND NOT EXISTS (SELECT 1 FROM project_orchestrators p WHERE p.session_id=d.target_session)
                    AND NOT EXISTS (SELECT 1 FROM root_projections x WHERE x.session_id=d.target_session)''', (target,caller,'project_5fb8d3d13ec348e0993b25a3f3169289')).fetchone()
                if direct:
                    from .management import assert_governing_reset
                    assert_governing_reset(db,direct['reset_id'],direct['home_session'],direct['project_id'])
                    return
            raise CoordError('global root manages project orchestrators only; delegate worker lifecycle to their project orchestrator')
        return
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='project_orchestrators'").fetchone():
        # Isolated/legacy project stores have no root hierarchy to enforce.
        return
    if caller == target:
        return
    if not db.execute('SELECT 1 FROM project_orchestrators WHERE session_id=?', (caller,)).fetchone():
        raise CoordError('agents may manage only their own lifecycle')
    current, seen = target, set()
    while current not in seen:
        seen.add(current)
        from .management import manager
        binding = db.execute('SELECT parent_session FROM bridge_bindings WHERE session_id=?', (current,)).fetchone()
        if (not binding or not binding['parent_session']) and manager(db, current) == caller:
            return
        if not binding or not binding['parent_session']:
            break
        current = binding['parent_session']
        if current == caller:
            return
    raise CoordError('project orchestrators manage only their own bound children; retained parentless mains need explicit root delegate-management')


def workspace(value):
    if (not isinstance(value, str) or not value.startswith('/') or len(value) > 4096
            or str(PurePosixPath(value)) != value or '..' in PurePosixPath(value).parts
            or any(ord(c) < 32 for c in value)):
        raise CoordError('project workspace must be a normalized absolute path on the execution host')
    return value


def overlaps(left, right):
    a, b = PurePosixPath(left), PurePosixPath(right)
    return a == b or a in b.parents or b in a.parents


def _scoped_operator_store_safe(db, project_session=None):
    """Permit server-stamped browser identities, never agent or native work."""
    if db.execute('''SELECT 1 FROM sessions s LEFT JOIN operator_identities o ON o.session_id=s.id
        WHERE s.id<>COALESCE(?, '') AND (o.session_id IS NULL OR s.agent<>'operator') LIMIT 1''',(project_session,)).fetchone():
        return False
    if db.execute('''SELECT 1 FROM messages m WHERE NOT EXISTS
        (SELECT 1 FROM operator_identities o WHERE o.session_id=m.sender_session)
        OR NOT EXISTS (SELECT 1 FROM operator_identities o WHERE o.session_id=m.recipient_session) LIMIT 1''').fetchone():
        return False
    tables = {row['name'] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    for name in ('bridge_jobs','resources','resource_queue','project_work','agent_control_requests','session_resets'):
        if name in tables and db.execute('SELECT 1 FROM '+name+' LIMIT 1').fetchone():
            return False
    return True


class RootOrchestrator:
    def __init__(self, projects):
        self.projects = projects
        with projects.connection() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS root_config (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    home_project TEXT NOT NULL, home_session TEXT NOT NULL,
                    thread_id TEXT NOT NULL, host_id TEXT NOT NULL, cwd TEXT NOT NULL,
                    agent TEXT NOT NULL, created_at REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS root_projects (
                    project_id TEXT PRIMARY KEY, session_id TEXT NOT NULL,
                    joined INTEGER NOT NULL DEFAULT 0, error TEXT,
                    cwd TEXT, agent TEXT, model TEXT, effort TEXT, prompt TEXT,
                    request_id TEXT, orchestrator_session TEXT, adopted INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS root_directory_owner (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1), home_project TEXT NOT NULL, home_session TEXT NOT NULL);
            ''')
            db.executescript('''
                CREATE TABLE IF NOT EXISTS root_team_operations (id TEXT PRIMARY KEY,root_session TEXT NOT NULL,created_at REAL NOT NULL,resumed_at REAL);
                CREATE TABLE IF NOT EXISTS root_team_operation_projects (request_id TEXT NOT NULL,project_id TEXT NOT NULL,root_session TEXT NOT NULL,error TEXT,PRIMARY KEY(request_id,project_id));
                CREATE TABLE IF NOT EXISTS root_scoped_onboards (
                    request_id TEXT PRIMARY KEY, project_id TEXT UNIQUE NOT NULL,
                    stop_id TEXT NOT NULL, home_session TEXT NOT NULL, host_id TEXT NOT NULL,
                    thread_id TEXT NOT NULL, project_session TEXT NOT NULL,
                    state TEXT NOT NULL CHECK(state IN ('prepared','joined')));
                CREATE TABLE IF NOT EXISTS root_project_workspaces (project_id TEXT NOT NULL,position INTEGER NOT NULL,path TEXT NOT NULL,PRIMARY KEY(project_id,position));
                CREATE TABLE IF NOT EXISTS root_workspace_roots (position INTEGER PRIMARY KEY,path TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS root_reconfigurations (project_id TEXT PRIMARY KEY,request_id TEXT UNIQUE NOT NULL,target_session TEXT NOT NULL,old_thread_id TEXT NOT NULL,prompt TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS root_reconfiguration_workspaces (project_id TEXT NOT NULL,position INTEGER NOT NULL,path TEXT NOT NULL,PRIMARY KEY(project_id,position));
                CREATE TABLE IF NOT EXISTS root_routing_pauses (
                    id TEXT PRIMARY KEY, home_project TEXT NOT NULL, home_session TEXT NOT NULL,
                    host_id TEXT NOT NULL, thread_id TEXT NOT NULL, created_at REAL NOT NULL,
                    status TEXT NOT NULL, resumed_at REAL);
                CREATE TABLE IF NOT EXISTS root_routing_pause_projects (
                    request_id TEXT NOT NULL, project_id TEXT NOT NULL, session_id TEXT NOT NULL,
                    PRIMARY KEY(request_id,project_id));
                CREATE TRIGGER IF NOT EXISTS root_routing_pause_payload_immutable
                    BEFORE UPDATE OF id,home_project,home_session,host_id,thread_id,created_at ON root_routing_pauses
                    BEGIN SELECT RAISE(ABORT,'root routing pause audit is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS root_routing_pause_delete_immutable BEFORE DELETE ON root_routing_pauses
                    BEGIN SELECT RAISE(ABORT,'root routing pause audit is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS root_routing_pause_projects_update_immutable BEFORE UPDATE ON root_routing_pause_projects
                    BEGIN SELECT RAISE(ABORT,'root routing pause cohort is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS root_routing_pause_projects_delete_immutable BEFORE DELETE ON root_routing_pause_projects
                    BEGIN SELECT RAISE(ABORT,'root routing pause cohort is immutable'); END;
            ''')
            if 'mode' not in {row['name'] for row in db.execute('PRAGMA table_info(root_config)')}:
                db.execute("ALTER TABLE root_config ADD COLUMN mode TEXT NOT NULL DEFAULT 'running'")

            from .root_restart import schema
            schema(db)
            from .goal_selection_generation import install as install_goal_generation, REGISTRY as GOAL_REGISTRY
            install_goal_generation(db, GOAL_REGISTRY)

    def config(self):
        with self.projects.connection() as db:
            row = db.execute('SELECT * FROM root_config WHERE singleton=1').fetchone()
            return dict(row, workspaces=[item['path'] for item in db.execute('SELECT path FROM root_workspace_roots ORDER BY position')] or [row['cwd']]) if row else None

    def path(self, project):
        path = self.projects.db_path(project)
        if not path.is_file():
            raise CoordError('project database is missing; restore it before continuing')
        return path

    def authorize(self, project, session, host):
        root = self.config()
        if not root or (project, session, host) != (root['home_project'], root['home_session'], root['host_id']):
            raise CoordError('use the global root own canonical home project/session and host')
        with contextlib.closing(Coordinator(self.path(project), session)) as coord:
            binding = coord.db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?', (session,)).fetchone()
            owner = coord.db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (session,)).fetchone()
            if not binding or binding['thread_id'] != root['thread_id'] or not owner or owner['host_id'] != host:
                raise CoordError('canonical root binding or host changed; routing withheld')
        return root

    @staticmethod
    @contextlib.contextmanager
    def _readiness_db(path):
        # Never instantiate Coordinator/Projects or initialize/migrate a store.
        with contextlib.closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)) as db:
            db.row_factory = sqlite3.Row
            db.execute('BEGIN')
            yield db

    def operator_attachment(self, project, session, host, source_project, message_id, attachment_id):
        """Read one operator-to-managed-PO attachment; request IDs confer no authority."""
        import hashlib
        from .core import Coordinator
        from .root_team_reset import quarantined
        try:
            # Serialize against application-local enrollment/designation changes.
            # Separate stores are read snapshots, not a cross-store fencing claim.
            with self.projects.root_lock, self._readiness_db(self.projects.path) as directory:
                root = directory.execute('SELECT * FROM root_config WHERE singleton=1').fetchone()
                owner = directory.execute('SELECT * FROM root_directory_owner WHERE singleton=1').fetchone()
                if (not root or not owner or (project, session, host) !=
                        (root['home_project'], root['home_session'], root['host_id'])
                        or (owner['home_project'], owner['home_session']) != (project, session)):
                    raise CoordError('attachment download requires the canonical Root home, session and host')
                enrolled = directory.execute('SELECT * FROM root_projects WHERE project_id=? AND joined=1',
                                             (source_project,)).fetchone()
                if not enrolled or not enrolled['orchestrator_session']:
                    raise CoordError('attachment source requires a currently enrolled designated PO')
                # Paths derive only from exact existing registry IDs, never request paths.
                def existing_path(selected):
                    item = directory.execute('SELECT id FROM projects WHERE id=?', (selected,)).fetchone()
                    if not item or (selected != 'default' and
                                    (not selected.startswith('project_') or
                                     any(char not in '0123456789abcdef' for char in selected[8:]) or
                                     not selected[8:])):
                        raise CoordError('attachment project registry association is unavailable')
                    return (self.projects.database if selected == 'default' else
                            self.projects.database.parent / (self.projects.database.name + '.projects') /
                            selected / 'state.sqlite3')
                with self._readiness_db(existing_path(project)) as home:
                    binding = self._readiness_binding(home, session, host)
                    if (not binding or binding['parent_session'] or binding['thread_id'] != root['thread_id']
                            or not self._readiness_projection(home, session, root)
                            or quarantined(home, session=session, thread=root['thread_id'])):
                        raise CoordError('canonical Root attachment binding is unverifiable')
                with self._readiness_db(existing_path(source_project)) as source:
                    binding = self._readiness_binding(source, enrolled['session_id'], host)
                    po = enrolled['orchestrator_session']
                    role = source.execute('SELECT root_session FROM project_orchestrators WHERE session_id=?', (po,)).fetchone()
                    registered = source.execute('SELECT 1 FROM sessions WHERE id=?', (po,)).fetchone()
                    retired = source.execute('SELECT 1 FROM retired_sessions WHERE id=?', (po,)).fetchone()
                    if (not binding or binding['parent_session'] or binding['thread_id'] != root['thread_id']
                            or not self._readiness_projection(source, enrolled['session_id'], root)
                            or not role or role['root_session'] != enrolled['session_id'] or not registered or retired
                            or quarantined(source, session=enrolled['session_id'], thread=root['thread_id'])
                            or quarantined(source, session=po)):
                        raise CoordError('source enrollment or exact designated PO management is unverifiable')
                    row = source.execute('''SELECT a.id,a.name,a.mime,a.size,a.content,m.id AS message_id
                        FROM attachments a JOIN messages m ON m.id=a.message_id
                        JOIN operator_message_origins o ON o.message_id=m.id AND o.sender_session=m.sender_session
                        JOIN operator_identities i ON i.session_id=m.sender_session
                        WHERE a.id=? AND m.id=? AND m.recipient_session=?
                          AND o.channel='authenticated_operator_ui'
                          AND NOT EXISTS (SELECT 1 FROM message_batches b WHERE b.message_id=m.id)''',
                                         (attachment_id, message_id, po)).fetchone()
                    if not row:
                        raise CoordError('attachment is not an exact authenticated operator-to-designated-PO message')
                    content = bytes(row['content'])
                    if (not isinstance(row['size'], int) or isinstance(row['size'], bool)
                            or row['size'] != len(content)
                            or not 0 < len(content) <= Coordinator.attachment_size_limit(row['name'])):
                        raise CoordError('stored attachment size is invalid')
                    name, mime, _ = Coordinator._prepare_attachment(row['name'], content)
                    if (name, mime) != (row['name'], row['mime']):
                        raise CoordError('stored attachment metadata is invalid')
                    return dict(attachment_id=row['id'], message_id=row['message_id'], name=name,
                                mime=mime, size=len(content), sha256=hashlib.sha256(content).hexdigest(), content=content)
        except (sqlite3.Error, OSError, KeyError, TypeError, ValueError):
            raise CoordError('trusted attachment associations are unavailable or unsupported') from None

    @staticmethod
    def _readiness_table(db, table):
        return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone())

    @classmethod
    def _readiness_binding(cls, db, session, host):
        if not all(cls._readiness_table(db, table) for table in ('sessions', 'bridge_bindings', 'remote_session_hosts')):
            return None
        registered = db.execute('SELECT registered_at FROM sessions WHERE id=?', (session,)).fetchone()
        retired = (cls._readiness_table(db, 'retired_sessions') and
                   db.execute('SELECT 1 FROM retired_sessions WHERE id=?', (session,)).fetchone())
        binding = db.execute('SELECT thread_id,parent_session FROM bridge_bindings WHERE session_id=?', (session,)).fetchone()
        owner = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (session,)).fetchone()
        if not registered or retired or not binding or not binding['thread_id'] or not owner or owner['host_id'] != host:
            return None
        if cls._readiness_table(db, 'agent_controls'):
            admission = db.execute('SELECT mode FROM agent_controls WHERE target_session=?', (session,)).fetchone()
            if admission and admission['mode'] != 'running':
                return None
        return binding

    def readiness(self, project, session, host, targets, expected_thread):
        """Allowlisted metadata from existing records only; selectors never confer authority."""
        if (not isinstance(targets, list) or not targets or len(targets) > 64
                or any(not isinstance(item, str) or not item or len(item) > 200
                       or any(ord(char) < 32 for char in item) for item in targets)
                or len(set(targets)) != len(targets)
                or not isinstance(expected_thread, str) or not expected_thread):
            raise CoordError('readiness requires unique explicit enrolled project IDs and expected thread')
        try:
            with self._readiness_db(self.projects.path) as directory:
                root = directory.execute('SELECT home_project,home_session,thread_id,host_id,mode FROM root_config WHERE singleton=1').fetchone()
                owner = directory.execute('SELECT home_project,home_session FROM root_directory_owner WHERE singleton=1').fetchone()
                if (not root or not owner or (project, session, host, expected_thread) !=
                        (root['home_project'], root['home_session'], root['host_id'], root['thread_id'])
                        or (owner['home_project'], owner['home_session']) != (project, session)):
                    raise CoordError('readiness requires the canonical enrolled home caller, host and thread')
                selected = []
                for target in targets:
                    row = directory.execute('SELECT project_id,session_id,joined,orchestrator_session FROM root_projects WHERE project_id=?', (target,)).fetchone()
                    setup = directory.execute('SELECT workspace,instructions FROM projects WHERE id=?', (target,)).fetchone()
                    if not row or not setup:
                        raise CoordError('readiness target is not an existing Root-enrolled project')
                    selected.append((row, bool(setup['workspace'] and setup['instructions'])))
                now = time.time()
                with self._readiness_db(self.path(project)) as home:
                    binding = self._readiness_binding(home, session, host)
                    projection = self._readiness_projection(home, session, root)
                    observed = (home.execute('SELECT thread_id,checked_at FROM agent_control_observations WHERE target_session=?',
                                            (session,)).fetchone() if self._readiness_table(home, 'agent_control_observations') else None)
                    if (not binding or binding['parent_session'] or binding['thread_id'] != expected_thread or not projection
                            or not observed or observed['thread_id'] != expected_thread
                            or not isinstance(observed['checked_at'], (int, float)) or not math.isfinite(observed['checked_at'])
                            or not 0 <= now - observed['checked_at'] <= 120):
                        raise CoordError('canonical readiness registration/binding/observation is missing, stale or unverifiable')
                    caller = dict(record_match=True, project=project, session=session, thread_id=expected_thread,
                                  host_id=host, observed_at=observed['checked_at'])
                entries = [self._readiness_project(root, row, setup_ready, now) for row, setup_ready in selected]
                return dict(authority=dict(scope='machine_read', named_session_authentication='UNVERIFIED'),
                            caller=caller, projects=entries)
        except (sqlite3.Error, OSError, KeyError, TypeError, ValueError):
            # Neither SQL errors nor stored setup/error strings may disclose private paths/payloads.
            raise CoordError('readiness trusted records are unavailable or unsupported') from None

    def startup_plan(self, project, session, host, targets, expected_thread):
        """Bounded, constructor-free metadata view of records consulted by startup sync.

        This is an informational read across independent SQLite snapshots. It is
        deliberately not a readiness proof and never invokes sync/join/notify.
        """
        limit = 64
        if (not isinstance(targets, list) or not targets or len(targets) > limit
                or any(not isinstance(item, str) or not item or len(item) > 200
                       or any(ord(char) < 32 for char in item) for item in targets)
                or len(set(targets)) != len(targets)
                or not isinstance(expected_thread, str) or not expected_thread or len(expected_thread) > 200):
            raise CoordError('startup plan requires unique explicit enrolled project IDs and expected thread')

        sampled_start = time.time()
        complete = True
        blockers = []
        stores = []

        def bounded(value, maximum=256):
            if value is None:
                return None
            if not isinstance(value, (str, int, float)) or isinstance(value, bool):
                return None
            if isinstance(value, str) and (len(value) > maximum or any(ord(c) < 32 for c in value)):
                return None
            if isinstance(value, float) and not math.isfinite(value):
                return None
            return value

        def table_info(db, table, required):
            if not self._readiness_table(db, table):
                return None
            columns = {row['name'] for row in db.execute('PRAGMA table_info("' + table + '")')}
            return columns if set(required) <= columns else None

        def stored_binding(db, selected_session):
            requirements = {
                'sessions': ('id',),
                'bridge_bindings': ('session_id', 'thread_id', 'parent_session'),
                'remote_session_hosts': ('session_id', 'host_id'),
                'root_projections': ('session_id', 'home_project', 'home_session', 'thread_id'),
            }
            if any(table_info(db, name, columns) is None for name, columns in requirements.items()):
                return None
            registered = bool(db.execute('SELECT 1 FROM sessions WHERE id=?', (selected_session,)).fetchone())
            retired_known = self._readiness_table(db, 'retired_sessions')
            retired = (bool(db.execute('SELECT 1 FROM retired_sessions WHERE id=?',
                (selected_session,)).fetchone()) if retired_known else None)
            binding = db.execute('SELECT thread_id,parent_session FROM bridge_bindings WHERE session_id=?',
                (selected_session,)).fetchone()
            host_row = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?',
                (selected_session,)).fetchone()
            projection = db.execute('''SELECT home_project,home_session,thread_id FROM root_projections
                WHERE session_id=?''', (selected_session,)).fetchone()
            controls = table_info(db, 'agent_controls', ('target_session', 'mode'))
            control = (db.execute('SELECT mode FROM agent_controls WHERE target_session=?',
                (selected_session,)).fetchone() if controls else None)
            return dict(registered=registered, retired=retired,
                thread_id=bounded(binding['thread_id']) if binding else None,
                parent_session=bounded(binding['parent_session']) if binding else None,
                host_matches=bool(host_row and host_row['host_id'] == host),
                projection_matches=bool(projection and
                    (projection['home_project'], projection['home_session'], projection['thread_id']) ==
                    (root['home_project'], root['home_session'], root['thread_id'])),
                control_mode=bounded(control['mode']) if control else None,
                control_record_present=bool(control),
                retired_schema_known=retired_known,
                control_schema_known=controls is not None)

        def store_begin(db, name):
            before = db.execute('PRAGMA data_version').fetchone()[0]
            return {'store': name, 'sampled_at_start': time.time(), 'data_version_start': before}

        def store_end(db, record):
            after = db.execute('PRAGMA data_version').fetchone()[0]
            record.update(sampled_at_end=time.time(), data_version_end=after,
                          stable_during_read=(after == record['data_version_start']))
            if not record['stable_during_read']:
                blockers.append('store_changed_during_read')

        try:
            with self.projects.root_lock, self._readiness_db(self.projects.path) as directory:
                registry_sample = store_begin(directory, 'configured_registry')
                required = {
                    'root_config': ('home_project', 'home_session', 'thread_id', 'host_id', 'mode'),
                    'root_directory_owner': ('home_project', 'home_session'),
                    'root_projects': ('project_id', 'session_id', 'joined', 'error', 'orchestrator_session'),
                    'projects': ('id', 'workspace', 'instructions'),
                    'root_team_operations': ('id', 'root_session', 'created_at', 'resumed_at'),
                    'root_team_operation_projects': ('request_id', 'project_id', 'root_session', 'error'),
                    'root_fresh_requests': ('id', 'home_project', 'home_session', 'host_id', 'old_thread_id',
                        'status', 'new_thread_id', 'queue_id', 'created_at', 'updated_at'),
                }
                schemas = {table: table_info(directory, table, columns) for table, columns in required.items()}
                if any(value is None for value in schemas.values()):
                    complete = False
                    blockers.append('registry_schema_missing_or_unsupported')
                root = None
                owner = None
                enrolled = []
                setup = {}
                team_operations = []
                fresh_requests = []
                other_team_operation_scope_present = False
                other_fresh_request_scope_present = False
                if schemas['root_config'] and schemas['root_directory_owner']:
                    root = directory.execute('''SELECT home_project,home_session,thread_id,host_id,mode
                        FROM root_config WHERE singleton=1''').fetchone()
                    owner = directory.execute('''SELECT home_project,home_session FROM root_directory_owner
                        WHERE singleton=1''').fetchone()
                if not root or not owner or (project, session, host, expected_thread) != (
                        root['home_project'], root['home_session'], root['host_id'], root['thread_id']) or (
                        owner['home_project'], owner['home_session']) != (project, session):
                    raise CoordError('startup plan requires the configured canonical Root identity and thread')
                if schemas['root_projects'] and schemas['projects']:
                    rows = directory.execute('''SELECT rp.project_id,rp.session_id,rp.joined,rp.error,
                        rp.orchestrator_session,p.id AS registered_project_id,p.workspace,p.instructions
                        FROM projects p LEFT JOIN root_projects rp ON rp.project_id=p.id
                        UNION ALL
                        SELECT rp.project_id,rp.session_id,rp.joined,rp.error,
                        rp.orchestrator_session,NULL AS registered_project_id,NULL AS workspace,NULL AS instructions
                        FROM root_projects rp LEFT JOIN projects p ON p.id=rp.project_id
                        WHERE p.id IS NULL
                        ORDER BY project_id LIMIT 65''').fetchall()
                    if len(rows) > limit:
                        complete = False
                        blockers.append('enrolled_project_limit_exceeded')
                    else:
                        enrolled = rows
                        actual = [row['project_id'] for row in rows]
                        if actual != sorted(targets):
                            raise CoordError('startup plan requires the exact current enrolled project set')
                        if not any(row['project_id'] == project for row in rows):
                            complete = False
                            blockers.append('canonical_root_missing_from_enrolled_set')
                        for row in rows:
                            if not row['project_id'] or not row['session_id']:
                                complete = False
                                blockers.append('enrolled_project_record_malformed')
                            setup[row['project_id']] = dict(
                                workspace_present=bool(row['workspace']) if row['registered_project_id'] else None,
                                instructions_present=bool(row['instructions']) if row['registered_project_id'] else None,
                                record_present=row['registered_project_id'] is not None)
                            if not setup[row['project_id']]['record_present']:
                                complete = False
                                blockers.append('project_setup_metadata_unavailable')
                if schemas['root_team_operations'] and schemas['root_team_operation_projects']:
                    rows = directory.execute('''SELECT id,root_session,created_at,resumed_at
                        FROM root_team_operations WHERE resumed_at IS NULL ORDER BY created_at,id LIMIT 65''').fetchall()
                    if len(rows) > limit:
                        complete = False
                        blockers.append('unsettled_team_operation_limit_exceeded')
                    else:
                        for row in rows:
                            if row['root_session'] != session:
                                other_team_operation_scope_present = True
                                continue
                            placeholders = ','.join('?' for _ in targets)
                            targets_rows = directory.execute('''SELECT project_id,root_session,error
                                FROM root_team_operation_projects WHERE request_id=? AND project_id IN ('''
                                + placeholders + ''') ORDER BY project_id LIMIT 65''',
                                (row['id'], *targets)).fetchall()
                            if len(targets_rows) > limit:
                                complete = False
                                blockers.append('team_operation_target_limit_exceeded')
                                target_list = []
                            else:
                                target_list = [dict(project=bounded(item['project_id']),
                                    root_session=bounded(item['root_session']), error_recorded=item['error'] is not None)
                                    for item in targets_rows]
                            team_operations.append(dict(id=bounded(row['id']), root_session=bounded(row['root_session']),
                                created_at=bounded(row['created_at']), resumed_at=None, targets=target_list))
                if schemas['root_fresh_requests']:
                    rows = directory.execute('''SELECT id,home_project,home_session,host_id,old_thread_id,
                        status,new_thread_id,queue_id,created_at,updated_at FROM root_fresh_requests
                        WHERE status NOT IN ('completed','retired') ORDER BY created_at,id LIMIT 65''').fetchall()
                    if len(rows) > limit:
                        complete = False
                        blockers.append('unsettled_fresh_request_limit_exceeded')
                    else:
                        for row in rows:
                            if (row['home_project'], row['home_session'], row['host_id']) != (project, session, host):
                                other_fresh_request_scope_present = True
                                continue
                            fresh_requests.append(dict(id=bounded(row['id']), home_project=bounded(row['home_project']),
                                home_session=bounded(row['home_session']), host_id=bounded(row['host_id']),
                                old_thread_id=bounded(row['old_thread_id']), status=bounded(row['status']),
                                new_thread_id=bounded(row['new_thread_id']), queue_id=bounded(row['queue_id']),
                                created_at=bounded(row['created_at']), updated_at=bounded(row['updated_at'])))
                store_end(directory, registry_sample)
                stores.append(registry_sample)

            caller_entry = None
            project_entries = []
            home_notices = {}
            duplicate_home_notice_projects = set()
            notice_coverage_available = False
            try:
                with self._readiness_db(self.path(project)) as home_db:
                    home_sample = store_begin(home_db, 'canonical_root_project_store')
                    notice_schema = (table_info(home_db, 'root_project_notices', ('project_id', 'message_id'))
                                     and table_info(home_db, 'messages',
                                         ('id', 'sender_session', 'recipient_session', 'created_at'))
                                     and table_info(home_db, 'sessions', ('id', 'agent')))
                    if notice_schema:
                        placeholders = ','.join('?' for _ in targets)
                        notice_rows = home_db.execute('''SELECT n.project_id,n.message_id,m.id AS found_id,
                            s.agent AS sender_agent,m.recipient_session,m.created_at
                            FROM root_project_notices n LEFT JOIN messages m ON m.id=n.message_id
                            LEFT JOIN sessions s ON s.id=m.sender_session
                            WHERE n.project_id IN (''' + placeholders + ''')
                            ORDER BY n.project_id LIMIT 65''', tuple(targets)).fetchall()
                        if len(notice_rows) > limit:
                            complete = False
                            blockers.append('root_notice_limit_exceeded')
                        else:
                            seen_notice_projects = set()
                            for item in notice_rows:
                                project_id = item['project_id']
                                if project_id in seen_notice_projects:
                                    duplicate_home_notice_projects.add(project_id)
                                else:
                                    seen_notice_projects.add(project_id)
                                    home_notices[project_id] = item
                            if duplicate_home_notice_projects:
                                complete = False
                                blockers.append('root_notice_duplicate_association')
                            notice_coverage_available = True
                    else:
                        complete = False
                        blockers.append('root_notice_schema_unavailable')
                    store_end(home_db, home_sample)
                    stores.append(home_sample)
            except (sqlite3.Error, OSError, KeyError, TypeError, ValueError):
                complete = False
                blockers.append('root_notice_store_unavailable')
            for row in enrolled:
                project_id, root_session = row['project_id'], row['session_id']
                try:
                    path = self.path(project_id)
                    with self._readiness_db(path) as db:
                        sample = store_begin(db, 'project:' + project_id)
                        inspection_session = session if project_id == project else root_session
                        binding = stored_binding(db, inspection_session) if inspection_session else None
                        projection = binding['projection_matches'] if binding else None
                        root_registration = (None if binding is None or binding['retired'] is None else
                            bool(root_session and row['joined'] is not None and binding['registered'] and
                            binding['retired'] is False and binding['parent_session'] is None and
                            binding['thread_id'] == expected_thread and binding['host_matches'] and projection))
                        if binding is None or not binding['retired_schema_known']:
                            complete = False
                            blockers.append('project_binding_or_retirement_schema_unavailable')
                        if binding is None or not binding['control_schema_known']:
                            complete = False
                            blockers.append('project_control_schema_unavailable')
                        elif not binding['control_record_present']:
                            complete = False
                            blockers.append('project_control_record_unavailable')
                        po = row['orchestrator_session']
                        po_role = None
                        po_binding = None
                        if po and self._readiness_table(db, 'project_orchestrators'):
                            po_role = db.execute('SELECT root_session FROM project_orchestrators WHERE session_id=?',
                                (po,)).fetchone()
                            if po_role and po_role['root_session'] == root_session:
                                po_binding = stored_binding(db, po)
                        elif po:
                            complete = False
                            blockers.append('designated_po_role_schema_unavailable')
                        po_binding_matches = (None if po_binding is None or po_binding['retired'] is None else
                            bool(po_binding['registered'] and po_binding['retired'] is False
                                and po_binding['parent_session'] is None
                                and po_binding['thread_id'] == expected_thread
                                and po_binding['host_matches'] and po_binding['projection_matches']))
                        if po_role and po_role['root_session'] == root_session and (
                                po_binding is None or not po_binding['retired_schema_known']
                                or not po_binding['control_schema_known'] or not po_binding['control_record_present']):
                            complete = False
                            blockers.append('designated_po_binding_metadata_unavailable')
                        if project_id == project:
                            caller_entry = dict(project=project_id, session=session,
                                thread_id=binding['thread_id'] if binding else None,
                                host_id=host if binding else None, parent_session=binding['parent_session'] if binding else None,
                                registered=binding['registered'] if binding else None,
                                retired=binding['retired'] if binding else None,
                                control_mode=binding['control_mode'] if binding else None,
                                projection_matches=projection,
                                binding_matches=(None if binding is None or binding['retired'] is None else bool(
                                    binding['registered'] and binding['retired'] is False and
                                    binding['parent_session'] is None and binding['thread_id'] == expected_thread
                                    and binding['host_matches'] and projection)))
                        notice = None
                        if project_id == project:
                            notice = dict(state='not_applicable_home_project', message_id=None,
                                          type='root_project_registration_notice')
                        elif project_id in duplicate_home_notice_projects:
                            notice = dict(state='unknown', message_id=None,
                                type='root_project_registration_notice')
                        elif project_id in home_notices:
                            notice_row = home_notices[project_id]
                            valid_notice = (notice_row['found_id'] == notice_row['message_id'] and
                                notice_row['sender_agent'] == 'operator' and
                                notice_row['recipient_session'] == session)
                            notice = dict(state='associated' if valid_notice else 'association_inconsistent',
                                    message_id=bounded(notice_row['message_id']),
                                    type='root_project_registration_notice',
                                    sender_matches_operator=notice_row['sender_agent'] == 'operator',
                                    recipient_matches_root=notice_row['recipient_session'] == session,
                                    created_at=bounded(notice_row['created_at']))
                            if not valid_notice:
                                complete = False
                                blockers.append('root_notice_association_unverifiable')
                        else:
                            notice = dict(state='unmapped_or_unavailable', message_id=None,
                                type='root_project_registration_notice')
                            if not notice_coverage_available:
                                complete = False
                                blockers.append('root_notice_coverage_unverifiable')
                        project_entries.append(dict(project=project_id, registration=dict(
                            record_present=row['session_id'] is not None,
                            joined=bool(row['joined']) if row['joined'] is not None else None,
                            error_recorded=row['error'] is not None,
                            session=bounded(root_session), thread_id=bounded(binding['thread_id']) if binding else None,
                            parent_session=bounded(binding['parent_session']) if binding else None,
                            host_matches=binding['host_matches'] if binding else None,
                            control_mode=binding['control_mode'] if binding else None,
                            binding_matches=root_registration,
                            projection_matches=bool(projection)),
                            designated_po=dict(session=bounded(po), role_matches=bool(po_role and
                                po_role['root_session'] == root_session), binding_matches=po_binding_matches,
                                binding_comparison=dict(state='uncompared',
                                    reason='independent_expected_po_thread_not_supplied'),
                                thread_id=bounded(po_binding['thread_id']) if po_binding else None,
                                parent_session=bounded(po_binding['parent_session']) if po_binding else None,
                                host_matches=po_binding['host_matches'] if po_binding else None,
                                control_mode=po_binding['control_mode'] if po_binding else None,
                                control_record_present=po_binding['control_record_present'] if po_binding else None),
                            setup=setup.get(project_id, dict(workspace_present=None,
                                instructions_present=None, record_present=False)), notice=notice))
                        store_end(db, sample)
                        stores.append(sample)
                except (CoordError, sqlite3.Error, OSError, KeyError, TypeError, ValueError):
                    complete = False
                    blockers.append('project_store_unavailable_or_unsupported')
                    project_entries.append(dict(project=bounded(project_id),
                        registration=dict(record_present=row['session_id'] is not None,
                            joined=bool(row['joined']) if row['joined'] is not None else None,
                            error_recorded=row['error'] is not None,
                            session=bounded(root_session), binding_matches=None, projection_matches=None),
                        designated_po=dict(session=bounded(row['orchestrator_session']),
                            role_matches=None, binding_matches=None, thread_id=None),
                        setup=setup.get(project_id, dict(workspace_present=None,
                            instructions_present=None, record_present=False)),
                        notice=dict(state='unknown', message_id=None,
                            type='root_project_registration_notice')))
            if len(stores) > limit + 2:
                complete = False
                blockers.append('store_count_limit_exceeded')
            if caller_entry is None:
                complete = False
                blockers.append('canonical_root_binding_unavailable')
            if root['mode'] is None:
                complete = False
                blockers.append('root_mode_unavailable')
            elif root['mode'] not in ('running', 'maintenance_paused', 'detached'):
                complete = False
                blockers.append('root_mode_unsupported')
            if any(item['id'] is None or item['root_session'] is None or item['created_at'] is None
                    for item in team_operations):
                complete = False
                blockers.append('team_operation_record_malformed')
            if any(item['id'] is None or item['home_project'] is None or item['home_session'] is None
                    or item['host_id'] is None or item['old_thread_id'] is None or item['status'] is None
                    or item['created_at'] is None or item['updated_at'] is None for item in fresh_requests):
                complete = False
                blockers.append('fresh_request_record_malformed')
            sync_enabled = (root['mode'] == 'running' and not team_operations and not fresh_requests
                and not other_team_operation_scope_present and not other_fresh_request_scope_present)
            effect_known = complete and all(item.get('stable_during_read') for item in stores)
            result = dict(operation='startup-plan', complete=complete,
                authority=dict(scope='machine_read', named_session_authentication='UNVERIFIED'),
                sampled_at_start=sampled_start, sampled_at_end=time.time(),
                source=dict(configured_home=bounded(project), root_session=bounded(session),
                    expected_thread=bounded(expected_thread), host_id=bounded(host),
                    root_mode=bounded(root['mode']), registry_path_identity='configured_root_registry'),
                consistency=dict(kind='independent_read_only_store_snapshots_not_atomic', stores=stores),
                caller=caller_entry, projects=project_entries,
                unsettled=dict(team_operations=team_operations, fresh_requests=fresh_requests,
                    other_team_operation_scope_present=other_team_operation_scope_present,
                    other_fresh_request_scope_present=other_fresh_request_scope_present),
                startup_effects=dict(
                    sync_may_run=sync_enabled if effect_known else None,
                    sync_may_register_projects=(sync_enabled and any(
                        not item['registration']['record_present'] for item in project_entries)
                        if effect_known else None),
                    join_may_write_bindings_and_projections=(sync_enabled and any(
                        not item['registration']['joined'] or not item['registration']['binding_matches']
                        for item in project_entries) if effect_known else None),
                    notices_may_be_created_for_unmapped_projects=(sync_enabled and any(
                        item['notice']['state'] == 'unmapped_or_unavailable'
                        for item in project_entries if item['project'] != project) if effect_known else None),
                    withheld_by_unsettled_team_operation=bool(team_operations),
                    withheld_by_unsettled_fresh_request=bool(fresh_requests),
                    withheld_by_other_team_operation=other_team_operation_scope_present,
                    withheld_by_other_fresh_request=other_fresh_request_scope_present,
                    native_owner_and_runtime_effects='unknown_not_inspected'))
            result['blockers'] = sorted(set(blockers))
            # Bound the returned object too; large or pathological stored identifiers
            # never become a partial success claim.
            if len(json.dumps(result, ensure_ascii=True).encode('utf-8')) > 65536:
                return dict(operation='startup-plan', complete=False, blockers=['serialized_projection_limit_exceeded'],
                    authority=dict(scope='machine_read', named_session_authentication='UNVERIFIED'),
                    sampled_at_start=sampled_start, sampled_at_end=time.time(),
                    source=dict(configured_home=bounded(project), root_session=bounded(session),
                        expected_thread=bounded(expected_thread), host_id=bounded(host)), projects=[],
                    consistency=dict(kind='independent_read_only_store_snapshots_not_atomic', stores=stores),
                    startup_effects=dict(sync_may_run=None, sync_may_register_projects=None,
                        join_may_write_bindings_and_projections=None,
                        notices_may_be_created_for_unmapped_projects=None,
                        native_owner_and_runtime_effects='unknown_not_inspected'))
            return result
        except CoordError:
            raise
        except (sqlite3.Error, OSError, KeyError, TypeError, ValueError):
            raise CoordError('startup plan trusted metadata is unavailable or unsupported') from None

    @classmethod
    def _readiness_projection(cls, db, session, root):
        if not cls._readiness_table(db, 'root_projections'):
            return False
        row = db.execute('SELECT home_project,home_session,thread_id FROM root_projections WHERE session_id=?', (session,)).fetchone()
        return bool(row and (row['home_project'], row['home_session'], row['thread_id']) ==
                    (root['home_project'], root['home_session'], root['thread_id']))

    def _readiness_project(self, root, row, setup_ready, now):
        host = root['host_id']
        entry = dict(project=row['project_id'], root_session=row['session_id'],
                     registration=dict(state='unknown', reason='project_records_unavailable', observed_at=None),
                     routing=dict(state='ready' if root['mode'] == 'running' else 'not_ready',
                                  reason=None if root['mode'] == 'running' else 'root_routing_paused', observed_at=None),
                     setup=dict(state='ready' if setup_ready else 'not_ready',
                                reason=None if setup_ready else 'setup_incomplete', observed_at=None),
                     po=dict(session=row['orchestrator_session'], thread_id=None, host_id=None,
                             readiness='unknown', reason='po_not_designated', observed_at=None),
                     bridge=dict(host_id=host, state='unknown', reason='host_coverage_not_recorded', observed_at=None,
                                 native_endpoint_identity=None, endpoint_reason='native_endpoint_not_verified'))
        try:
            with self._readiness_db(self.path(row['project_id'])) as db:
                binding = self._readiness_binding(db, row['session_id'], host)
                joined = bool(row['joined'] and binding and binding['thread_id'] == root['thread_id']
                              and self._readiness_projection(db, row['session_id'], root))
                entry['registration'].update(state='ready' if joined else 'not_ready',
                                             reason=None if joined else 'root_projection_binding_unverified')
                if not joined:
                    entry['routing'].update(state='not_ready', reason='root_projection_binding_unverified')
                po = row['orchestrator_session']
                if po:
                    role = (db.execute('SELECT root_session FROM project_orchestrators WHERE session_id=?', (po,)).fetchone()
                            if self._readiness_table(db, 'project_orchestrators') else None)
                    po_binding = self._readiness_binding(db, po, host) if joined and role and role['root_session'] == row['session_id'] else None
                    entry['po'].update(readiness='ready' if po_binding else 'not_ready',
                                       reason=None if po_binding else 'po_registration_binding_unverified',
                                       thread_id=po_binding['thread_id'] if po_binding else None,
                                       host_id=host if po_binding else None)
                coverage = (db.execute('SELECT pid,heartbeat,error FROM bridge_client_runtime WHERE host_id=?', (host,)).fetchone()
                            if self._readiness_table(db, 'bridge_client_runtime') else None)
                if coverage:
                    stamp = coverage['heartbeat']
                    valid = isinstance(stamp, (int, float)) and math.isfinite(stamp) and stamp <= now
                    recent = valid and now - stamp < 30
                    state = ('unknown' if not valid else 'stale' if not recent else
                             'unhealthy' if not coverage['pid'] or coverage['error'] else 'healthy')
                    reason = dict(unknown='coverage_time_unverifiable', stale='coverage_observation_stale',
                                  unhealthy='coverage_reports_unhealthy', healthy=None)[state]
                    entry['bridge'].update(state=state, reason=reason, observed_at=stamp if valid else None)
        except (CoordError, sqlite3.Error, OSError, KeyError, TypeError, ValueError):
            entry['routing'].update(state='unknown', reason='project_records_unavailable')
            entry['po'].update(readiness='unknown', reason='project_records_unavailable')
        return entry

    def fresh(self, root):
        with contextlib.closing(Coordinator(self.path(root['home_project']), root['home_session'])) as coord:
            coord._fresh(coord.db, root['home_session'])

    def _scoped_onboard_binding(self, target_project, project_session, root):
        with self._readiness_db(self.path(target_project)) as target:
            binding = target.execute('SELECT thread_id,parent_session FROM bridge_bindings WHERE session_id=?', (project_session,)).fetchone()
            owner = target.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (project_session,)).fetchone()
            projection = target.execute('SELECT home_project,home_session,thread_id,paused FROM root_projections WHERE session_id=?', (project_session,)).fetchone()
            if (not binding or binding['thread_id'] != root['thread_id'] or binding['parent_session'] is not None
                    or not owner or owner['host_id'] != root['host_id'] or not projection or projection['paused']
                    or (projection['home_project'],projection['home_session'],projection['thread_id']) !=
                       (root['home_project'],root['home_session'],root['thread_id'])):
                raise CoordError('scoped onboarding target binding changed')

    def active(self, root, target_project=None, parked_stop=None):
        with self.projects.connection() as db:
            from .root_admin_attachment import fresh_requests_blocking
            scoped = db.execute("SELECT 1 FROM root_scoped_onboards WHERE project_id=? AND state='joined' AND home_session=? AND host_id=? AND thread_id=?", (target_project,root['home_session'],root['host_id'],root['thread_id'])).fetchone() if target_project else None
            if scoped:
                if fresh_requests_blocking(db,root['home_project'],root['home_session'],root['host_id']):
                    raise CoordError('root fresh-start is pending; inspect root fresh-status')
                from .root_pending_fresh import routing_fresh_blocker
                blocker = routing_fresh_blocker(db, self.projects.database, root, terminal_only=True)
                if blocker: raise CoordError(blocker)
            else:
                from .root_pending_fresh import routing_fresh_blocker
                blocker = routing_fresh_blocker(db, self.projects.database, root)
                if blocker: raise CoordError(blocker)
        if root['mode'] != 'running':
            raise CoordError('root is detaching; finish clean detach or use root resume-routing')
        with self.projects.connection() as db:
            stops = db.execute('SELECT id,root_session FROM root_team_operations WHERE resumed_at IS NULL').fetchall()
            if parked_stop and not stops:
                raise CoordError('parked provisioning requires its active original stop')
            if stops:
                if parked_stop:
                    original = db.execute('SELECT root_session FROM root_team_operation_projects WHERE request_id=? AND project_id=?', (parked_stop,target_project)).fetchone()
                    current = db.execute('SELECT session_id,joined FROM root_projects WHERE project_id=?', (target_project,)).fetchone()
                    if (len(stops) != 1 or tuple(stops[0]) != (parked_stop,root['home_session']) or not original
                            or not current or not current['joined'] or current['session_id'] != original['root_session']):
                        raise CoordError('parked provisioning requires the exact original stopped project')
                    self._scoped_onboard_binding(target_project,current['session_id'],root)
                    return
                admission = db.execute('SELECT * FROM root_scoped_onboards WHERE project_id=? AND state=?',
                                       (target_project, 'joined')).fetchone() if target_project else None
                projection = db.execute('SELECT session_id,joined FROM root_projects WHERE project_id=?',
                                        (target_project,)).fetchone() if target_project else None
                if (len(stops) != 1 or not admission or admission['stop_id'] != stops[0]['id']
                        or not projection or not projection['joined'] or projection['session_id'] != admission['project_session']
                        or stops[0]['root_session'] != root['home_session']
                        or (admission['home_session'], admission['host_id'], admission['thread_id']) !=
                           (root['home_session'], root['host_id'], root['thread_id'])
                        or db.execute('SELECT 1 FROM root_team_operation_projects WHERE request_id=? AND project_id=?',
                                      (stops[0]['id'], target_project)).fetchone()):
                    raise CoordError('root stop-all barrier is active; target has no current scoped onboarding')
                self._scoped_onboard_binding(target_project, admission['project_session'], root)

    def admit_po(self, project, session, host, target_project, po, expected_thread, expected_host, request_id, confirmed):
        root = self.authorize(project,session,host)
        try:
            request_id = str(uuid.UUID(request_id))
        except (ValueError,TypeError,AttributeError):
            raise CoordError('PO admission requires an exact original stop UUID')
        if confirmed is not True:
            raise CoordError('PO admission requires explicit orchestration confirmation')
        with self.projects.connection() as db:
            operation = db.execute('SELECT root_session FROM root_team_operations WHERE id=?', (request_id,)).fetchone()
            original = db.execute('SELECT root_session FROM root_team_operation_projects WHERE request_id=? AND project_id=?', (request_id,target_project)).fetchone()
            current = db.execute('SELECT session_id,orchestrator_session,joined FROM root_projects WHERE project_id=?', (target_project,)).fetchone()
            if not operation or operation['root_session'] != root['home_session'] or not original:
                raise CoordError('PO admission project is not in the exact original stop scope')
            if not current or not current['joined'] or current['session_id'] != original['root_session'] or current['orchestrator_session'] != po:
                raise CoordError('PO admission requires unchanged enrollment and current designated PO')
        from .root_lifecycle import admit_po
        with contextlib.closing(Coordinator(self.path(target_project),original['root_session'])) as coord:
            return dict(admit_po(coord,original['root_session'],request_id,po,expected_thread,expected_host),
                        project=target_project,operation='po-admit')

    def admit_investigation(self, project, session, host, target_project, po, target,
                            expected_thread, expected_host, stop_id, request_id,
                            message_id, inspection_request_id, expires_at, confirmed):
        """Admit one exact existing PO task message while its original stop stays active."""
        root = self.authorize(project, session, host)
        try:
            request_id = str(uuid.UUID(request_id))
            stop_id = str(uuid.UUID(stop_id))
        except (ValueError, TypeError, AttributeError) as error:
            raise CoordError('investigation admission requires exact request and stop UUIDs') from error
        if confirmed is not True:
            raise CoordError('investigation admission requires explicit Root confirmation')
        with self.projects.connection() as db:
            operation = db.execute('SELECT root_session,resumed_at FROM root_team_operations WHERE id=?',
                                   (stop_id,)).fetchone()
            original = db.execute('''SELECT root_session FROM root_team_operation_projects
                WHERE request_id=? AND project_id=?''', (stop_id,target_project)).fetchone()
            current = db.execute('''SELECT session_id,orchestrator_session,joined FROM root_projects
                WHERE project_id=?''', (target_project,)).fetchone()
            if (not operation or operation['root_session'] != root['home_session']
                    or operation['resumed_at'] is not None or not original
                    or not current or current['joined'] != 1
                    or current['session_id'] != original['root_session']
                    or current['orchestrator_session'] != po):
                raise CoordError('investigation admission requires the current enrolled project and active original stop')
        from .root_lifecycle import admit_investigation_task
        with contextlib.closing(Coordinator(self.path(target_project),original['root_session'])) as coord:
            result = admit_investigation_task(coord, original['root_session'], request_id, stop_id,
                po, target, expected_thread, expected_host, message_id, inspection_request_id, expires_at)
        return dict(result, project=target_project, operation='investigation-admit',
                    workers_released=False, native_execution_proven=False)

    def continue_investigation_settings(self, project, session, host, target_project, po, target,
                                        expected_thread, expected_host, stop_id, admission_request_id,
                                        baseline_id, continuation_id, message_id, inspection_request_id,
                                        source_message_id, source_artifact_name, source_artifact_sha256, source_limit,
                                        expected_settings, expires_at, confirmed):
        """Freeze PO-attested expected settings and one finite same-task continuation."""
        root = self.authorize(project, session, host)
        try:
            stop_id = str(uuid.UUID(stop_id))
            admission_request_id = str(uuid.UUID(admission_request_id))
            baseline_id = str(uuid.UUID(baseline_id))
            continuation_id = str(uuid.UUID(continuation_id))
        except (ValueError, TypeError, AttributeError) as error:
            raise CoordError('settings continuation IDs must be UUIDs') from error
        if confirmed is not True:
            raise CoordError('same-task settings continuation requires explicit Root confirmation')
        with self.projects.connection() as db:
            operation = db.execute('SELECT root_session,resumed_at FROM root_team_operations WHERE id=?',
                                   (stop_id,)).fetchone()
            original = db.execute('''SELECT root_session FROM root_team_operation_projects
                WHERE request_id=? AND project_id=?''', (stop_id,target_project)).fetchone()
            current = db.execute('''SELECT session_id,orchestrator_session,joined FROM root_projects
                WHERE project_id=?''', (target_project,)).fetchone()
            if (not operation or operation['root_session'] != root['home_session']
                    or operation['resumed_at'] is not None or not original
                    or not current or current['joined'] != 1
                    or current['session_id'] != original['root_session']
                    or current['orchestrator_session'] != po):
                raise CoordError('settings continuation requires the unchanged enrolled PO and active original stop')
        if (not isinstance(source_artifact_name, str) or not source_artifact_name
                or not isinstance(source_artifact_sha256, str)
                or len(source_artifact_sha256) != 64
                or any(char not in '0123456789abcdef' for char in source_artifact_sha256.lower())):
            raise CoordError('baseline source artifact name and SHA-256 are required')
        # The canonical HOME Root remains the caller, while the retained source
        # message is addressed to this target project's enrolled Root projection.
        # Tie its claimed artifact to the exact direct message as well.
        with self._readiness_db(self.path(target_project)) as source_db:
            source = source_db.execute('SELECT sender_session,recipient_session FROM messages WHERE id=?',
                                       (source_message_id,)).fetchone()
            if (not source or (source['sender_session'], source['recipient_session']) !=
                    (po, original['root_session']) or source_db.execute(
                        'SELECT 1 FROM message_batches WHERE message_id=?', (source_message_id,)).fetchone()):
                raise CoordError('baseline provenance must be a retained direct current-PO message to the enrolled project Root')
            attachments = source_db.execute('''SELECT name,size,content FROM attachments
                WHERE message_id=? AND name=? ORDER BY id''',
                (source_message_id, source_artifact_name)).fetchall()
            matches = 0
            for attachment in attachments:
                content = bytes(attachment['content'])
                if (attachment['name'] == source_artifact_name
                        and attachment['size'] == len(content)
                        and hashlib.sha256(content).hexdigest() == source_artifact_sha256.lower()):
                    matches += 1
            if matches != 1:
                raise CoordError('baseline artifact must match exactly one attachment on the retained PO message')
        with contextlib.closing(Coordinator(self.path(target_project), original['root_session'])) as coord:
            row = coord.db.execute('''SELECT stop_id,po_session,target_session,thread_id,host_id,message_id,
                inspection_request_id FROM root_team_stop_task_admissions WHERE request_id=?''',
                (admission_request_id,)).fetchone()
            if (not row or row['stop_id'] != stop_id or row['po_session'] != po
                    or row['target_session'] != target or row['thread_id'] != expected_thread
                    or row['host_id'] != expected_host or row['message_id'] != message_id
                    or row['inspection_request_id'] != inspection_request_id):
                raise CoordError('same-task continuation selectors do not match the original immutable admission')
            from .root_lifecycle import create_task_settings_continuation
            result = create_task_settings_continuation(coord, actor_root=original['root_session'],
                admission_request_id=admission_request_id, baseline_id=baseline_id,
                continuation_id=continuation_id, expected_settings=expected_settings,
                source_message_id=source_message_id, source_artifact_name=source_artifact_name,
                source_artifact_sha256=source_artifact_sha256,
                source_kind='po_attested_historical', source_limit=source_limit, expires_at=expires_at)
        return dict(result, project=target_project, operation='investigation-settings-continuation',
                    native_preload_started=False, original_message_released=False)

    def managed_native_diagnose(self, project, session, host, target_project, po, target,
                               expected_thread, expected_host, stop_id, admission_request_id,
                               message_id, job_id, inspection_request_id, request_id):
        with self.projects.root_lock:
            return self._managed_native_diagnose_locked(project, session, host, target_project, po, target,
                expected_thread, expected_host, stop_id, admission_request_id, message_id, job_id,
                inspection_request_id, request_id)

    def _managed_native_diagnose_locked(self, project, session, host, target_project, po, target,
                                        expected_thread, expected_host, stop_id, admission_request_id,
                                        message_id, job_id, inspection_request_id, request_id):
        """Return a value-free diagnosis of the exact read guard, without effects."""
        request_id = str(uuid.UUID(request_id))
        home_path, target_path = self.path(project), self.path(target_project)
        with self._readiness_db(self.projects.path) as db:
            root = db.execute('SELECT home_project,home_session,thread_id,host_id FROM root_config WHERE singleton=1').fetchone()
            owner = db.execute('SELECT home_project,home_session FROM root_directory_owner WHERE singleton=1').fetchone()
            enrolled = db.execute("SELECT session_id,orchestrator_session,joined FROM root_projects WHERE project_id=?", (target_project,)).fetchone()
        if (not root or not owner or (project, session, host) !=
                (root['home_project'], root['home_session'], root['host_id'])
                or (owner['home_project'], owner['home_session']) != (project, session)
                or not enrolled or enrolled['joined'] != 1 or enrolled['orchestrator_session'] != po):
            raise CoordError('managed native diagnosis requires canonical Root and current project enrollment')
        project_root = enrolled['session_id']
        with self._readiness_db(self.projects.path) as db:
            current = db.execute('SELECT home_project,home_session,thread_id,host_id FROM root_config WHERE singleton=1').fetchone()
        with self._readiness_db(home_path) as home:
            binding = self._readiness_binding(home, session, host)
            if (not current or tuple(current) != tuple(root) or not binding
                    or binding['parent_session'] is not None or binding['thread_id'] != root['thread_id']):
                raise CoordError('managed native diagnosis canonical Root binding changed')
        row = dict(request_id=request_id, requester_session=session, home_project=project,
            home_session=session, home_thread=root['thread_id'], home_host=host, target_session=target,
            host_id=expected_host, thread_id=expected_thread, project_id=target_project,
            project_root=project_root, po_session=po, stop_id=stop_id,
            admission_request_id=admission_request_id, message_id=message_id, job_id=job_id,
            inspection_request_id=inspection_request_id)
        with self._readiness_db(target_path) as target_db:
            context = target_db.execute('''SELECT stop_id,root_session,po_session,target_session,
                message_id,inspection_request_id,purpose FROM root_team_stop_task_admissions
                WHERE request_id=?''', (admission_request_id,)).fetchone()
        if (not context or tuple(context) != (stop_id, project_root, po, target,
                message_id, inspection_request_id, 'investigation')):
            raise CoordError('managed native diagnosis requires the exact current admitted task scope')
        sampled_at = time.time()
        names = (
            'project_schema_matches','actor_matches_request','task_association_present',
            'project_root_is_current','project_root_binding_matches','project_root_host_matches',
            'actor_is_authorized_root_or_admitted_po','po_role_matches_root','po_binding_matches_admission',
            'po_host_matches_admission','worker_role_is_worker','worker_binding_matches_request',
            'worker_host_matches_request','task_scope_matches_request','only_expected_active_stop',
            'stop_matches_project_root','stop_is_active','target_admission_matches_request',
            'target_admission_requester_matches_root','target_admission_status_admissible',
            'po_admission_matches_current_binding','message_sender_recipient_matches',
            'inspection_request_matches_target_thread_host','inspection_requester_is_root_or_admitted_po',
            'inspection_request_status_admissible','original_wake_is_unique_exact_delivery',
            'original_wake_not_cancelled','message_not_batched','no_other_pending_control',
            'no_pending_reset','worker_not_quarantined')
        try:
            with self._readiness_db(target_path) as target_db:
                projection = _managed_native_scope(target_db, row, actor_session=session, diagnostic=True)
            checks = projection['checks']
        except (CoordError, sqlite3.Error, KeyError, TypeError, ValueError):
            checks = {name: 'unknown' for name in names}
        sampled_at_end = time.time()
        verdict = ('blocked' if 'false' in checks.values() or True in checks.values() and False in checks.values()
                   else 'eligible_for_separate_review' if all(value is True for value in checks.values())
                   else 'unknown')
        checks = {name: ('true' if value is True else 'false' if value is False else 'unknown')
                  for name, value in checks.items()}
        return {'operation':'managed-native-diagnose', 'verdict':verdict,
                'sampled_at_start':sampled_at, 'sampled_at_end':sampled_at_end,
                'checks':checks}

    def managed_native_read(self, project, session, host, target_project, po, target,
                            expected_thread, expected_host, stop_id, admission_request_id,
                            message_id, job_id, inspection_request_id, request_id):
        # Keep the canonical Root registration/enrollment stable across the
        # foreground RPC and its final project-scope compare-and-set.
        with self.projects.root_lock:
            return self._managed_native_read_locked(project, session, host, target_project, po, target,
                expected_thread, expected_host, stop_id, admission_request_id, message_id, job_id,
                inspection_request_id, request_id)

    def _managed_native_read_locked(self, project, session, host, target_project, po, target,
                                   expected_thread, expected_host, stop_id, admission_request_id,
                                   message_id, job_id, inspection_request_id, request_id):
        """Read one exact admitted worker thread through this foreground Root command."""
        request_id = str(uuid.UUID(request_id))
        home_path, target_path = self.path(project), self.path(target_project)
        with self._readiness_db(self.projects.path) as db:
            root = db.execute('SELECT home_project,home_session,thread_id,host_id FROM root_config WHERE singleton=1').fetchone()
            owner = db.execute('SELECT home_project,home_session FROM root_directory_owner WHERE singleton=1').fetchone()
            enrolled = db.execute('''SELECT session_id,orchestrator_session,joined FROM root_projects
                WHERE project_id=?''', (target_project,)).fetchone()
        if (not root or not owner or (project, session, host) !=
                (root['home_project'], root['home_session'], root['host_id'])
                or (owner['home_project'], owner['home_session']) != (project, session)
                or not enrolled or enrolled['joined'] != 1
                or enrolled['orchestrator_session'] != po):
            raise CoordError('managed native read requires canonical Root and current project enrollment')
        project_root = enrolled['session_id']
        with self._readiness_db(self.projects.path) as directory:
            current = directory.execute('SELECT home_project,home_session,thread_id,host_id FROM root_config WHERE singleton=1').fetchone()
        with self._readiness_db(home_path) as home:
            binding = self._readiness_binding(home, session, host)
            if (not current or tuple(current) != tuple(root) or not binding or binding['parent_session'] is not None
                    or binding['thread_id'] != root['thread_id']):
                raise CoordError('managed native read canonical Root binding changed')
        row = dict(request_id=request_id, requester_session=session, home_project=project,
            home_session=session, home_thread=root['thread_id'], home_host=host, target_session=target,
            host_id=expected_host, thread_id=expected_thread, project_id=target_project,
            project_root=project_root, po_session=po, stop_id=stop_id,
            admission_request_id=admission_request_id, message_id=message_id, job_id=job_id,
            inspection_request_id=inspection_request_id)
        # Validate target admission/task/wake links read-only before the dedicated
        # audit initializer can create a table or persist a request.
        with self._readiness_db(target_path) as target_db:
            _managed_native_scope(target_db, row, actor_session=session)
        with _managed_native_write_db(target_path) as target_db:
            created, is_new = _managed_native_request(target_db, actor_session=session,
                home_project=project, home_session=session, home_thread=root['thread_id'], home_host=host,
                target_session=target, expected_thread=expected_thread, expected_host=expected_host,
                project_id=target_project, project_root=project_root, po_session=po,
                stop_id=stop_id, admission_request_id=admission_request_id,
                message_id=message_id, job_id=job_id, inspection_request_id=inspection_request_id,
                request_id=request_id)
        request_id = created['request_id']
        if not is_new:
            return created
        try:
            with self._readiness_db(self.projects.path) as db:
                current = db.execute('SELECT home_project,home_session,thread_id,host_id FROM root_config WHERE singleton=1').fetchone()
                current_enrollment = db.execute('''SELECT session_id,orchestrator_session,joined FROM root_projects
                    WHERE project_id=?''', (target_project,)).fetchone()
            with self._readiness_db(home_path) as home:
                home_binding = self._readiness_binding(home, session, host)
            if (not current or tuple(current) != tuple(root) or not current_enrollment
                    or current_enrollment['joined'] != 1 or current_enrollment['session_id'] != project_root
                    or current_enrollment['orchestrator_session'] != po or not home_binding
                    or home_binding['parent_session'] is not None
                    or home_binding['thread_id'] != root['thread_id']):
                raise CoordError('managed native read canonical enrollment changed')
            with self._readiness_db(target_path) as target_db:
                saved = target_db.execute(f'SELECT * FROM {_MANAGED_NATIVE_READS} WHERE request_id=?',
                                          (request_id,)).fetchone()
                _managed_native_scope(target_db, dict(saved), actor_session=session)
        except (CoordError, sqlite3.Error, KeyError, TypeError):
            observation = {'native_state':'unknown','direct_input':None,'turn_id':None,'turn_id_known':False,
                'queue_count':None,'queue_state':'unknown','checked_at':time.time(),'error_code':'scope_changed'}
            with _managed_native_write_db(target_path) as target_db:
                return _managed_native_complete(target_db, request_id, 'unknown', observation, actor_session=session)
        from .rpc import RpcClient
        endpoint = os.environ.get('AGENT_CHAT_CODEX_SERVER') or 'ws://127.0.0.1:4500'
        try:
            with RpcClient(endpoint, timeout=3) as rpc:
                observation = _read_managed_native_observation(rpc, expected_thread)
        except (CoordError, OSError, ValueError):
            observation = {'native_state':'unknown','direct_input':None,'turn_id':None,'turn_id_known':False,
                'queue_count':None,'queue_state':'unknown','checked_at':time.time(),'error_code':'native_read_failed'}
        try:
            with self._readiness_db(self.projects.path) as db:
                current = db.execute('SELECT home_project,home_session,thread_id,host_id FROM root_config WHERE singleton=1').fetchone()
                current_enrollment = db.execute('''SELECT session_id,orchestrator_session,joined FROM root_projects
                    WHERE project_id=?''', (target_project,)).fetchone()
            with self._readiness_db(home_path) as home:
                home_binding = self._readiness_binding(home, session, host)
            if (not current or tuple(current) != tuple(root) or not current_enrollment
                    or current_enrollment['joined'] != 1 or current_enrollment['session_id'] != project_root
                    or current_enrollment['orchestrator_session'] != po or not home_binding
                    or home_binding['parent_session'] is not None
                    or home_binding['thread_id'] != root['thread_id']):
                raise CoordError('managed native read canonical enrollment changed')
        except (CoordError, sqlite3.Error, KeyError, TypeError):
            observation = {'native_state':'unknown','direct_input':None,'turn_id':None,'turn_id_known':False,
                'queue_count':None,'queue_state':'unknown','checked_at':time.time(),'error_code':'scope_changed'}
        status = ('unknown' if observation['error_code'] == 'scope_changed' else
            'failed' if observation['error_code'] is not None else
            'unavailable' if observation['native_state'] == 'notLoaded' else 'observed')
        with _managed_native_write_db(target_path) as target_db:
            return _managed_native_complete(target_db, request_id, status, observation, actor_session=session)

    def investigation_preflight(self, project, session, host, target_project, po, target,
                                expected_thread, expected_host, stop_id, request_id,
                                message_id, inspection_request_id, expires_at):
        """Read-only, exact-scope diagnostics; never creates or admits a task."""
        from . import root_lifecycle
        try:
            request_id = str(uuid.UUID(request_id))
            stop_id = str(uuid.UUID(stop_id))
        except (ValueError, TypeError, AttributeError) as error:
            raise CoordError('investigation preflight requires exact request and stop UUIDs') from error

        # All stores are opened read-only here. In particular, do not use
        # Coordinator: its constructor may initialize project-local schema.
        with self.projects.root_lock, self._readiness_db(self.projects.path) as directory:
            root = directory.execute('SELECT * FROM root_config WHERE singleton=1').fetchone()
            owner = directory.execute('SELECT * FROM root_directory_owner WHERE singleton=1').fetchone()
            if (not root or not owner or (project, session, host) !=
                    (root['home_project'], root['home_session'], root['host_id'])
                    or (owner['home_project'], owner['home_session']) != (project, session)):
                raise CoordError('investigation preflight requires canonical Root home, session and host')
            with self._readiness_db(self.path(project)) as home_db:
                home_binding = home_db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?',
                                               (session,)).fetchone()
                home_host = home_db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?',
                                            (session,)).fetchone()
                if (not home_binding or home_binding['thread_id'] != root['thread_id']
                        or not home_host or home_host['host_id'] != host):
                    raise CoordError('canonical Root binding or host changed; preflight withheld')
            operation = directory.execute('SELECT root_session,resumed_at FROM root_team_operations WHERE id=?',
                                          (stop_id,)).fetchone()
            original = directory.execute('''SELECT root_session FROM root_team_operation_projects
                WHERE request_id=? AND project_id=?''', (stop_id, target_project)).fetchone()
            current = directory.execute('''SELECT session_id,orchestrator_session,joined FROM root_projects
                WHERE project_id=?''', (target_project,)).fetchone()
            directory_checks = {
                'original_stop_is_current_active': bool(operation and
                    operation['root_session'] == root['home_session'] and
                    operation['resumed_at'] is None and original),
                'target_project_enrollment_is_current': bool(original and current and
                    current['joined'] == 1 and current['session_id'] == original['root_session'] and
                    current['orchestrator_session'] == po),
            }
            if not all(directory_checks.values()):
                failed = [name for name, passed in directory_checks.items() if not passed]
                return {'operation': 'investigation-preflight', 'status': 'blocked',
                        'checks': directory_checks, 'failed_checks': failed}
            target_root = original['root_session']
            with self._readiness_db(self.path(target_project)) as target_db:
                checks = root_lifecycle.investigation_preflight(
                    target_db, target_root, stop_id, po, target, expected_thread, expected_host,
                    request_id, message_id, inspection_request_id, expires_at)
        combined = dict(directory_checks)
        combined.update(checks['checks'])
        failed = [name for name, passed in directory_checks.items() if not passed]
        failed.extend(checks['failed_checks'])
        return {'operation': 'investigation-preflight',
                'status': 'eligible' if not failed else 'blocked',
                'checks': combined,
                'failed_checks': failed}

    def team_lifecycle(self, project, session, host, operation, request_id, confirmed=True, *, target_project=None):
        root = self.authorize(project, session, host)
        try:
            request_id = str(uuid.UUID(request_id))
        except (ValueError, TypeError, AttributeError):
            raise CoordError('root team lifecycle request_id must be a UUID')
        if operation not in ('stop-all','resume-all','resume-project','stop-status') or (operation != 'stop-status' and confirmed is not True):
            raise CoordError('root team lifecycle requires explicit user confirmation')
        if (operation == 'resume-project') != (target_project is not None):
            raise CoordError('project resume requires exactly one explicit original project')
        with self.projects.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            prior = db.execute('SELECT * FROM root_team_operations WHERE id=?', (request_id,)).fetchone()
            if not prior:
                if operation != 'stop-all':
                    raise CoordError('use the exact original stop-all request ID')
                if db.execute('SELECT 1 FROM root_team_operations WHERE resumed_at IS NULL').fetchone():
                    raise CoordError('another stop-all request is active; inspect or resume that exact request')
                db.execute('INSERT INTO root_team_operations VALUES(?,?,?,NULL)', (request_id,root['home_session'],time.time()))
                db.execute('''INSERT INTO root_team_operation_projects(request_id,project_id,root_session)
                              SELECT ?,project_id,session_id FROM root_projects WHERE joined=1''', (request_id,))
            elif prior['root_session'] != root['home_session']:
                raise CoordError('root team lifecycle request belongs to another root')
            full_scope = [dict(row) for row in db.execute('''SELECT s.*,p.joined AS current_joined,
                p.session_id AS current_root_session,p.orchestrator_session AS current_orchestrator_session
                FROM root_team_operation_projects s LEFT JOIN root_projects p ON p.project_id=s.project_id
                WHERE s.request_id=? ORDER BY s.project_id''', (request_id,))]
            scope = full_scope
            if operation == 'resume-project':
                scope = [item for item in full_scope if item['project_id'] == target_project]
                if len(scope) != 1:
                    raise CoordError('project is not in the original stop scope')
                current = db.execute('SELECT session_id,joined FROM root_projects WHERE project_id=?', (target_project,)).fetchone()
                if not current or not current['joined'] or current['session_id'] != scope[0]['root_session']:
                    raise CoordError('project root enrollment changed since the original stop')
            db.commit()
        from .root_lifecycle import pause, resume, status
        result, failures = [], []
        for item in scope:
            try:
                with contextlib.closing(Coordinator(self.path(item['project_id']),item['root_session'])) as coord:
                    if operation == 'stop-all':
                        state = pause(coord,item['root_session'],request_id)
                    elif operation in ('resume-all', 'resume-project'):
                        state = resume(coord,item['root_session'],request_id)
                    else:
                        state = status(coord.db,request_id)
                        from .root_lifecycle import coordination_status
                        current_enrollment = bool(item.get('current_joined') and
                                                  item.get('current_root_session') == item['root_session'])
                        state['po_coordination'] = coordination_status(
                            coord.db, item['root_session'],
                            item.get('current_orchestrator_session') if current_enrollment else None,
                            request_id)
                        state['po_coordination']['root_enrollment_current'] = current_enrollment
                    result.append(dict(project=item['project_id'],**state))
            except (CoordError,sqlite3.Error,OSError,TypeError) as error:
                failures.append(dict(project=item['project_id'],error=str(error)))
        all_resumed = operation == 'resume-all'
        if operation == 'resume-project' and not failures:
            # Finalize the original global journal only when every original
            # barrier is already inactive. Other project stores are read-only.
            all_resumed = True
            for item in full_scope:
                try:
                    with self._readiness_db(self.path(item['project_id'])) as db:
                        saved = db.execute('SELECT active,root_session FROM root_team_stops WHERE id=?', (request_id,)).fetchone()
                        if not saved or saved['active'] or saved['root_session'] != item['root_session']:
                            all_resumed = False
                            break
                except (CoordError,sqlite3.Error,OSError):
                    all_resumed = False
                    break
        if all_resumed and not failures:
            with self.projects.connection() as db:
                db.execute('UPDATE root_team_operations SET resumed_at=COALESCE(resumed_at,?) WHERE id=?', (time.time(),request_id))
        counts = dict(total=0, pending=0, waiting=0, interrupting=0, completed=0, stopped=0, other=0)
        for project_result in result:
            for target in project_result.get('targets', []):
                counts['total'] += 1
                state = target.get('status')
                counts[state if state in counts and state != 'total' else 'other'] += 1
        return dict(request_id=request_id,operation=operation,projects=result,errors=failures,
                    barrier_complete=not failures,process_closure_proven=False,
                    target_counts=counts, target_counts_complete=not failures)

    def enroll(self, project, session, host, expected_thread, confirmed):
        from .bridge_state import BridgeState
        from .session_reset import SessionResetState
        self.projects.get(project)
        if confirmed is not True:
            raise CoordError('root enrollment requires explicit confirmation')
        existing = self.config()
        if existing:
            self.authorize(project, session, host)
            if existing['thread_id'] != expected_thread:
                raise CoordError('root enrollment retry must retain the original thread')
            self.sync()
            return self.status()
        with contextlib.closing(Coordinator(self.path(project), session)) as coord:
            coord.require_session()
            BridgeState(coord)
            from .core import assert_agent_running
            assert_agent_running(coord.db, session)
            owner = coord.db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (session,)).fetchone()
            if not owner or owner['host_id'] != host:
                raise CoordError('root must use its authenticated existing host')
            SessionResetState(coord).clean(session, expected_thread)
            coord._fresh(coord.db, session)
            if coord.db.execute("SELECT 1 FROM session_resets WHERE requester_session=? AND status NOT IN ('completed','failed','cancelled')", (session,)).fetchone():
                raise CoordError('resolve your pending independent-agent creations before root enrollment')
            if coord.db.execute('SELECT 1 FROM resource_queue WHERE session=?', (session,)).fetchone():
                raise CoordError('cancel root resource queue entries before control-only enrollment')
            observed = coord.db.execute('SELECT * FROM agent_control_observations WHERE target_session=?', (session,)).fetchone()
            if not observed or observed['thread_id'] != expected_thread or time.time() - observed['checked_at'] > 120:
                raise CoordError('refresh your own agent-status through the existing bridge before root enrollment')
            cwd = workspace(observed['cwd'])
            from .agent_control import AgentControlState
            from .projects import Projects
            members = Projects.workspace_list(AgentControlState.observed_workspaces(coord.db, session, cwd))
            if members[0] != cwd:
                raise CoordError('root cwd must be its first observed workspace')
            agent = coord.db.execute('SELECT agent FROM sessions WHERE id=?', (session,)).fetchone()['agent']
        with self.projects.connection() as db:
            directory = db.execute('SELECT * FROM root_directory_owner WHERE singleton=1').fetchone()
            retained = list(db.execute('SELECT project_id,cwd FROM root_projects WHERE cwd IS NOT NULL')) if directory and (directory['home_project'], directory['home_session']) == (project, session) else []
            retained_members = [member for row in retained for member in self.projects._members(db, row['project_id'], row['cwd'], 'root_project_workspaces')]
            saved_members = [member for row in db.execute('SELECT id,workspace FROM projects') for member in self.projects._members(db, row['id'], row['workspace'])]
        if any(overlaps(root, member) for root in members for member in retained_members + saved_members):
            raise CoordError('root workspace overlaps a retained project; start from a neutral directory')
        with self.projects.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            old = db.execute('SELECT * FROM root_config WHERE singleton=1').fetchone()
            if old and (old['home_project'], old['home_session'], old['thread_id'], old['host_id'], old['cwd']) != (project, session, expected_thread, host, cwd):
                db.rollback()
                raise CoordError('a different global root is already enrolled; detach it cleanly first')
            if not old:
                directory = db.execute('SELECT * FROM root_directory_owner WHERE singleton=1').fetchone()
                if directory and (directory['home_project'], directory['home_session']) != (project, session):
                    # A distinct new root gets distinct registrations; do not
                    # borrow the prior root's retained identities.
                    db.execute('DELETE FROM root_projects')
                db.execute('INSERT OR REPLACE INTO root_directory_owner VALUES(1,?,?)', (project, session))
                db.execute('INSERT INTO root_config(singleton,home_project,home_session,thread_id,host_id,cwd,agent,created_at) VALUES(1,?,?,?,?,?,?,?)', (project, session, expected_thread, host, cwd, agent, time.time()))
            db.execute('DELETE FROM root_workspace_roots')
            db.executemany('INSERT INTO root_workspace_roots VALUES(?,?)', list(enumerate(members)))
            db.commit()
        self.sync()
        return self.status()

    def onboard_project(self, project, session, host, target_project, stop_id, expected_thread, request_id, confirmed):
        """Join one fresh project during one retained stop, without resuming it."""
        root = self.authorize(project, session, host)
        if confirmed is not True or root['mode'] != 'running' or root['thread_id'] != expected_thread:
            raise CoordError('scoped onboarding requires the current Root thread and confirmation')
        try:
            request_id, stop_id = str(uuid.UUID(request_id)), str(uuid.UUID(stop_id))
        except (ValueError, TypeError, AttributeError) as error:
            raise CoordError('scoped onboarding requires exact request and stop UUIDs') from error
        self.fresh(root)
        item = self.projects.get(target_project)
        if target_project == root['home_project']:
            raise CoordError('scoped onboarding requires a fresh non-Home project')
        project_session = 'session_' + uuid.uuid5(uuid.NAMESPACE_URL, root['home_project'] + ':' + root['home_session'] + ':' + target_project).hex
        values = (request_id, target_project, stop_id, session, host, expected_thread, project_session)

        def check_scope(db):
            from .root_admin_attachment import fresh_requests_blocking
            from .root_admin_attachment_members import cutover_pending
            if cutover_pending(db):
                raise CoordError('Root administrative attachment needs reconciliation')
            stops = db.execute('SELECT id,root_session,created_at FROM root_team_operations WHERE resumed_at IS NULL').fetchall()
            current_project = db.execute('SELECT created_at FROM projects WHERE id=?', (target_project,)).fetchone()
            if (len(stops) != 1 or stops[0]['id'] != stop_id or stops[0]['root_session'] != session
                    or not current_project or current_project['created_at'] != item['created_at']
                    or item['created_at'] <= stops[0]['created_at']
                    or db.execute('SELECT 1 FROM root_team_operation_projects WHERE project_id=?', (target_project,)).fetchone()
                    or fresh_requests_blocking(db, project, session, host)):
                raise CoordError('scoped onboarding stop or fresh-target scope differs')
            current = db.execute('SELECT * FROM root_config WHERE singleton=1').fetchone()
            if not current or (current['home_project'],current['home_session'],current['host_id'],current['thread_id'],current['mode']) != (project,session,host,expected_thread,'running'):
                raise CoordError('scoped onboarding Root identity changed')
            original = db.execute('SELECT project_id,root_session FROM root_team_operation_projects WHERE request_id=?', (stop_id,)).fetchall()
            if not original:
                raise CoordError('scoped onboarding original stop scope is unavailable')
            any_active = False
            for entry in original:
                with self._readiness_db(self.path(entry['project_id'])) as stopped_project:
                    barrier = stopped_project.execute('SELECT root_session,active FROM root_team_stops WHERE id=?', (stop_id,)).fetchone()
                    if not barrier or barrier['root_session'] != entry['root_session'] or barrier['active'] not in (0,1):
                        raise CoordError('scoped onboarding original barrier is unavailable')
                    any_active = any_active or bool(barrier['active'])
            if not any_active:
                raise CoordError('scoped onboarding original stop has no active barrier')

        with self.projects.root_lock:
            with self.projects.connection() as db:
                db.execute('BEGIN IMMEDIATE')
                check_scope(db)
                old = db.execute('SELECT * FROM root_scoped_onboards WHERE request_id=? OR project_id=?',
                                 (request_id, target_project)).fetchone()
                if old:
                    if tuple(old[key] for key in ('request_id','project_id','stop_id','home_session','host_id','thread_id','project_session')) != values:
                        raise CoordError('scoped onboarding retry differs from retained intent')
                    if old['state'] == 'joined':
                        projection = db.execute('SELECT session_id,joined FROM root_projects WHERE project_id=?', (target_project,)).fetchone()
                        if not projection or not projection['joined'] or projection['session_id'] != project_session:
                            raise CoordError('scoped onboarding joined projection changed')
                        self._scoped_onboard_binding(target_project, project_session, root)
                        return dict(project=target_project, session=project_session, request_id=request_id, state='joined')
                else:
                    if db.execute('SELECT 1 FROM root_projects WHERE project_id=?', (target_project,)).fetchone():
                        raise CoordError('scoped onboarding target already has a Root projection')
                    with self._readiness_db(self.path(target_project)) as target:
                        meta = dict(target.execute("SELECT key,value FROM meta WHERE key IN ('project_id','project_registry')").fetchall())
                        if (meta.get('project_id') != target_project or meta.get('project_registry') != str(self.projects.database)
                                or not _scoped_operator_store_safe(target)
                                or target.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_team_stops'").fetchone() and target.execute('SELECT 1 FROM root_team_stops LIMIT 1').fetchone()):
                            raise CoordError('scoped onboarding target is not a fresh project store')
                    db.execute('INSERT INTO root_scoped_onboards VALUES(?,?,?,?,?,?,?,?)', (*values, 'prepared'))
                db.commit()
            with self.projects.connection() as db:
                db.execute('BEGIN IMMEDIATE')
                check_scope(db)
                saved = db.execute('SELECT * FROM root_scoped_onboards WHERE request_id=?', (request_id,)).fetchone()
                if not saved or saved['state'] != 'prepared':
                    raise CoordError('scoped onboarding retained intent changed')
                row = db.execute('SELECT * FROM root_projects WHERE project_id=?', (target_project,)).fetchone()
                if row and row['session_id'] != project_session:
                    raise CoordError('scoped onboarding projection identity differs')
                db.execute('INSERT OR IGNORE INTO root_projects(project_id,session_id) VALUES(?,?)', (target_project, project_session))
                self.join(root, dict(project_id=target_project, session_id=project_session, cwd=None, scoped_onboard=True))
                db.execute('UPDATE root_projects SET joined=1,error=NULL WHERE project_id=?', (target_project,))
                db.execute("UPDATE root_scoped_onboards SET state='joined' WHERE request_id=?", (request_id,))
                db.commit()
        return dict(project=target_project, session=project_session, request_id=request_id, state='joined')

    def sync(self):
        """Repair partial onboarding with stable IDs, never recreate native threads."""
        from .root_admin_attachment_members import cutover_pending
        from .root_pending_fresh import routing_fresh_blocker

        with self.projects.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            root = db.execute('SELECT * FROM root_config WHERE singleton=1').fetchone()
            if not root or root['mode'] in ('detached', 'maintenance_paused') or cutover_pending(db):
                return
            root = dict(root)
            if db.execute('SELECT 1 FROM root_team_operations WHERE resumed_at IS NULL').fetchone() or routing_fresh_blocker(db, self.projects.database, root):
                return
            for project in self.projects.list():
                session = root['home_session'] if project['id'] == root['home_project'] else 'session_' + uuid.uuid5(uuid.NAMESPACE_URL, root['home_project'] + ':' + root['home_session'] + ':' + project['id']).hex
                db.execute('INSERT OR IGNORE INTO root_projects(project_id,session_id) VALUES(?,?)', (project['id'], session))
            rows = [dict(row, workspaces=self.projects._members(db, row['project_id'], row['cwd'], 'root_project_workspaces')) for row in db.execute('SELECT * FROM root_projects')]
            for row in rows:
                try:
                    self.join(root, row)
                    db.execute('UPDATE root_projects SET joined=1,error=NULL WHERE project_id=?', (row['project_id'],))
                except (CoordError, OSError, sqlite3.Error) as error:
                    if isinstance(error, sqlite3.Error) and ((getattr(error, 'sqlite_errorcode', 0) or 0) & 0xff) in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
                        raise
                    db.execute('UPDATE root_projects SET joined=0,error=? WHERE project_id=?', (str(error), row['project_id']))
            db.commit()
        for row in rows:
            self.notify_project(root, row['project_id'])

    def join(self, root, item):
        from .bridge_state import BridgeState
        from .remote_service import _schema
        with contextlib.closing(Coordinator(self.path(item['project_id']), item['session_id'])) as coord:
            BridgeState(coord)
            _schema(coord)
            coord.db.execute('''CREATE TABLE IF NOT EXISTS root_projections (
                session_id TEXT PRIMARY KEY, home_project TEXT NOT NULL, home_session TEXT NOT NULL,
                thread_id TEXT NOT NULL, cwd TEXT)''')
            if 'paused' not in {row['name'] for row in coord.db.execute('PRAGMA table_info(root_projections)')}:
                coord.db.execute('ALTER TABLE root_projections ADD COLUMN paused INTEGER NOT NULL DEFAULT 0')
            for column in ('model','effort'):
                if column not in {row['name'] for row in coord.db.execute('PRAGMA table_info(root_projections)')}:
                    coord.db.execute(f'ALTER TABLE root_projections ADD COLUMN {column} TEXT')
            with coord.tx() as db:
                db.execute('CREATE TABLE IF NOT EXISTS quiet_root_sessions (session_id TEXT PRIMARY KEY)')
                if item.get('scoped_onboard'):
                    if (not _scoped_operator_store_safe(db,item['session_id'])
                            or db.execute('SELECT 1 FROM bridge_bindings WHERE session_id<>? LIMIT 1', (item['session_id'],)).fetchone()
                            or db.execute('SELECT 1 FROM remote_session_hosts WHERE session_id<>? LIMIT 1', (item['session_id'],)).fetchone()
                            or db.execute('SELECT 1 FROM root_projections WHERE session_id<>? LIMIT 1', (item['session_id'],)).fetchone()
                            or db.execute('SELECT 1 FROM quiet_root_sessions WHERE session_id<>? LIMIT 1', (item['session_id'],)).fetchone()
                            or db.execute('SELECT 1 FROM session_resets LIMIT 1').fetchone()
                            or db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_team_stops'").fetchone()
                               and db.execute('SELECT 1 FROM root_team_stops LIMIT 1').fetchone()):
                        raise CoordError('scoped onboarding target acquired other state')
                    existing_projection = db.execute('SELECT * FROM root_projections WHERE session_id=?', (item['session_id'],)).fetchone()
                    existing_session = db.execute('SELECT agent FROM sessions WHERE id=?', (item['session_id'],)).fetchone()
                    existing_binding = db.execute('SELECT thread_id,parent_session,agent_path FROM bridge_bindings WHERE session_id=?', (item['session_id'],)).fetchone()
                    existing_host = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (item['session_id'],)).fetchone()
                    existing_quiet = db.execute('SELECT 1 FROM quiet_root_sessions WHERE session_id=?', (item['session_id'],)).fetchone()
                    bundle = (existing_session, existing_binding, existing_host, existing_projection, existing_quiet)
                    if any(bundle):
                        if (not all(bundle) or existing_session['agent'] != root['agent']
                                or (existing_binding['thread_id'], existing_binding['parent_session'], existing_binding['agent_path']) != (root['thread_id'], None, None)
                                or existing_host['host_id'] != root['host_id']
                                or (existing_projection['home_project'], existing_projection['home_session'],
                                    existing_projection['thread_id'], existing_projection['cwd'], existing_projection['paused'],
                                    existing_projection['model'], existing_projection['effort']) != (root['home_project'], root['home_session'],
                                    root['thread_id'], None, 0, None, None)):
                            raise CoordError('scoped onboarding target projection changed or incomplete')
                db.execute('INSERT OR IGNORE INTO quiet_root_sessions VALUES(?)', (item['session_id'],))
                prior = db.execute('SELECT * FROM sessions WHERE id=?', (item['session_id'],)).fetchone()
                if not prior:
                    if db.execute('SELECT 1 FROM retired_sessions WHERE id=?', (item['session_id'],)).fetchone():
                        raise CoordError('root registration was retired; explicit root recovery required')
                    label = root['agent']
                    if db.execute('SELECT 1 FROM sessions WHERE agent=?', (label,)).fetchone():
                        label = 'root-' + item['session_id'][-12:]
                    db.execute('INSERT INTO sessions(id,agent,registered_at) VALUES(?,?,?)', (item['session_id'], label, time.time()))
                binding = db.execute('SELECT * FROM bridge_bindings WHERE session_id=?', (item['session_id'],)).fetchone()
                if binding and (binding['thread_id'] != root['thread_id'] or binding['parent_session']):
                    raise CoordError('root project binding changed; refusing to overwrite it')
                other = db.execute('SELECT session_id FROM bridge_bindings WHERE thread_id=? AND session_id<>?', (root['thread_id'], item['session_id'])).fetchone()
                if other:
                    raise CoordError('root thread already belongs to another identity in this project')
                owner = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (item['session_id'],)).fetchone()
                if owner and owner['host_id'] != root['host_id']:
                    raise CoordError('root project identity belongs to another host')
                if not owner:
                    db.execute('INSERT INTO remote_session_hosts VALUES(?,?,?,?)', (item['session_id'], root['host_id'], time.time(), time.time()))
                if not binding:
                    db.execute('INSERT INTO bridge_bindings VALUES(?,?,NULL,NULL,?)', (item['session_id'], root['thread_id'], time.time()))
                db.execute('INSERT OR REPLACE INTO root_projections VALUES(?,?,?,?,?,?,?,?)', (item['session_id'], root['home_project'], root['home_session'], root['thread_id'], item['cwd'], int(root['mode'] != 'running'), item.get('model'), item.get('effort')))
                db.execute('CREATE TABLE IF NOT EXISTS project_orchestrators (session_id TEXT PRIMARY KEY, root_session TEXT NOT NULL)')
                from .goal_selection_generation import install as install_goal_generation, PROJECT as GOAL_PROJECT
                install_goal_generation(db, GOAL_PROJECT)
                if item.get('orchestrator_session'):
                    db.execute('DELETE FROM project_orchestrators')
                    db.execute('INSERT INTO project_orchestrators VALUES(?,?)', (item['orchestrator_session'], item['session_id']))

    def notify_project(self, root, project):
        if project == root['home_project']:
            return
        with contextlib.closing(Coordinator(self.path(root['home_project']), root['home_session'])) as coord:
            coord.db.execute('CREATE TABLE IF NOT EXISTS root_project_notices (project_id TEXT PRIMARY KEY, message_id TEXT NOT NULL)')
            with coord.tx() as db:
                if db.execute('SELECT 1 FROM root_project_notices WHERE project_id=?', (project,)).fetchone():
                    return
                operator = db.execute("SELECT id FROM sessions WHERE agent='operator' ORDER BY registered_at LIMIT 1").fetchone()
                if not operator:
                    raise CoordError('root home operator identity is missing')
                mid = 'message_' + uuid.uuid4().hex
                item = self.projects.get(project)
                setup = (f"Saved workspaces: {', '.join(item['workspaces'])}. Operator supplied project instructions. "
                         f"Provision this project's orchestrator through root provision --project-id {project} with one saved request UUID; "
                         "the saved assignment is forwarded without loading it into root's conversation. "
                         if item.get('workspace') and item.get('instructions') else
                         "Project creation scope is incomplete; ask the operator for missing workspace/task, then configure or adopt. ")
                body = (f"Project registered: {item['name']} (project ID {project}). Your distinct quiet root registration is bound to this same conversation. "
                        + setup + "Inspect root status; retain an existing designated team rather than creating a duplicate. "
                        "Do not acquire project resource holds as root. This notice creates no native agent or resource ownership.")
                db.execute('INSERT INTO messages(id,sender_session,recipient_session,body,created_at) VALUES(?,?,?,?,?)',
                           (mid, operator['id'], root['home_session'], body, time.time()))
                db.execute('INSERT INTO root_project_notices VALUES(?,?)', (project, mid))

    def route(self, project, session, host, target_project):
        root = self.authorize(project, session, host)
        if root['mode'] == 'detached':
            raise CoordError('finish root detach before routing or enrolling again')
        self.projects.get(target_project)
        self.sync()
        with self.projects.connection() as db:
            row = db.execute('SELECT * FROM root_projects WHERE project_id=?', (target_project,)).fetchone()
        if not row or not row['joined']:
            raise CoordError('root project onboarding incomplete: ' + (row['error'] if row else target_project))
        return dict(project=target_project, session=row['session_id'], cwd=row['cwd'])

    def exact_file_binding(self, project, session, host, target_project, operation, params):
        if operation not in ('exact-file-bind','exact-file-binding-status','exact-file-revoke'):
            raise CoordError('unsupported exact file Root operation')
        root=self.authorize(project,session,host)
        self.active(root,target_project)
        if operation!='exact-file-binding-status':self.fresh(root)
        with self.projects.connection() as db:
            row=db.execute('SELECT * FROM root_projects WHERE project_id=? AND joined=1',(target_project,)).fetchone()
        if not row or not row['orchestrator_session']:
            raise CoordError('exact file binding requires the existing enrolled project and PO')
        self._scoped_onboard_binding(target_project,row['session_id'],root)
        from . import exact_file_bindings as bindings
        with contextlib.closing(Coordinator(self.path(target_project),row['session_id'])) as coord:
            po=bindings._roles(coord.db)
            if (po['session_id'],po['root_session'])!=(row['orchestrator_session'],row['session_id']):
                raise CoordError('exact file binding project designation changed')
            if operation=='exact-file-bind':return bindings.issue(coord,target_project,params)
            if operation=='exact-file-revoke':return bindings.revoke(coord,target_project,params)
            if set(params)!={'binding_id'}:raise CoordError('invalid exact file binding status')
            return bindings.status(coord,target_project,params['binding_id'],host)

    def delegate_management(self, project, session, host, target_project, po, target, expected_thread, expected_host, request_id, confirmed):
        root = self.authorize(project, session, host)
        self.fresh(root)
        with self.projects.connection() as db:
            row = db.execute('SELECT * FROM root_projects WHERE project_id=? AND joined=1', (target_project,)).fetchone()
        if not row or row['orchestrator_session'] != po:
            raise CoordError('management requires exact enrolled project and current designated PO')
        from .management import delegate
        with contextlib.closing(Coordinator(self.path(target_project), row['session_id'])) as coord:
            return delegate(coord, row['session_id'], po, target, expected_thread, expected_host, request_id, confirmed)

    def provision(self, project, session, host, target_project, agent, request_id, confirmed, model=None, effort=None):
        self.authorize(project, session, host)
        item = self.projects.get(target_project)
        if not item.get('workspace') or not item.get('instructions'):
            raise CoordError('project creation needs workspace and instructions before provisioning')
        prompt = (f"Act as project orchestrator for {item['name']} (project ID {target_project}). "
                  "Manage this project's workers through agent-chat using your own project/session. "
                  "Keep goals, implementation and detailed project context with this team. Global root relays bounded operator questions/answers; "
                  "send only routing/readiness metadata unless an operator question needs limited project detail. Tag each question with project and request ID. "
                  "Read the agent-chat quickstart and agent guide; preserve exact resource ownership and receipt guarantees.\n\n"
                  "Operator's project assignment (literal):\n" + item['instructions'])
        return self.configure(project, session, host, target_project, agent, item['workspace'], prompt,
                              request_id, confirmed, model, effort, workspaces=item['workspaces'])

    def direct_create(self, project, session, host, target_project, reset_id, agent, cwd, prompt, request_id, confirmed, model, effort, parked_stop=None):
        """Private prototype: direct management only for the exact joined Agent Chat projection."""
        allowed = 'project_5fb8d3d13ec348e0993b25a3f3169289'
        root = self.authorize(project, session, host)
        if root['home_project'] != 'default' or (not parked_stop and target_project != allowed):
            raise CoordError('direct Root worker creation is restricted to the exact Agent Chat projection')
        if not model or not effort:
            raise CoordError('direct Root worker creation requires explicit model and effort')
        if parked_stop:
            try:
                parked_stop = str(uuid.UUID(parked_stop))
            except (ValueError,TypeError,AttributeError) as error:
                raise CoordError('parked provisioning requires an exact stop UUID') from error
        self.active(root, target_project, parked_stop=parked_stop)
        self.fresh(root)
        route = self.route(project, session, host, target_project)
        self._scoped_onboard_binding(target_project, route['session'], root)
        with contextlib.closing(Coordinator(self.path(target_project), route['session'])) as coord:
            from .root_team_reset import assert_ready
            from .session_reset import SessionResetState
            assert_ready(coord.db)
            if not coord.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_team_reset_requests'").fetchone():
                raise CoordError('completed target reset evidence is unavailable')
            if not coord.db.execute("SELECT 1 FROM root_team_reset_requests WHERE id=? AND root_session=? AND project_id=? AND status='completed'", (reset_id, root['home_session'], target_project)).fetchone():
                raise CoordError('direct Root worker requires the exact completed target reset')
            from .management import assert_governing_reset
            try:
                assert_governing_reset(coord.db,reset_id,root['home_session'],target_project)
            except CoordError:
                # A stale exact retry may read its original receipt only; it
                # never calls creation, schedules input or resurrects identity.
                table = coord.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_direct_creations'").fetchone()
                old = coord.db.execute('''SELECT d.*,j.prompt,j.requested_cwd,j.requested_model,j.requested_effort,j.status,j.kind
                    FROM root_direct_creations d JOIN session_resets j ON j.id=d.request_id WHERE d.request_id=?''', (request_id,)).fetchone() if table else None
                from .management import parked_creation
                parked = parked_creation(coord.db,request_id)
                if (confirmed is True and old and bool(parked) == bool(parked_stop) and (not parked or parked['stop_id'] == parked_stop) and (old['project_id'],old['reset_id'],old['root_session'],old['target_agent'],old['prompt'],old['requested_cwd'],old['requested_model'],old['requested_effort'],old['kind']) ==
                    (target_project,reset_id,route['session'],agent,prompt,cwd,model,effort,'create')):
                    return dict(project=target_project,root_session=route['session'],historical_only=True,
                                creation=dict(id=request_id,target_session=old['target_session'],status=old['status']))
                raise
            job = SessionResetState(coord).create(agent, root['thread_id'], prompt, request_id, confirmed,
                model=model, reasoning_effort=effort, cwd=cwd, project_scope=True, workspaces=[cwd],
                project_orchestrator=bool(parked_stop), _direct_scope=(target_project,reset_id), _park_stop=parked_stop)
        if parked_stop:
            with self.projects.connection() as db:
                db.execute('BEGIN IMMEDIATE')
                row = db.execute('SELECT session_id,orchestrator_session FROM root_projects WHERE project_id=? AND joined=1', (target_project,)).fetchone()
                if not row or row['session_id'] != route['session'] or row['orchestrator_session'] not in (None,job['target_session']):
                    db.rollback()
                    raise CoordError('project PO designation changed; parked creation retained for inspection')
                db.execute('UPDATE root_projects SET orchestrator_session=?,agent=?,cwd=?,model=?,effort=?,prompt=?,request_id=? WHERE project_id=?',
                           (job['target_session'],agent,cwd,model,effort,prompt,request_id,target_project))
                db.commit()
            # A partially saved registry intent cannot be consumed by the bridge.
            # Exact retry repairs this second commit after a lost response.
            with contextlib.closing(Coordinator(self.path(target_project), route['session'])) as coord:
                with coord.tx():
                    from .management import assert_parked_creation
                    assert_parked_creation(coord.db,request_id)
                    coord.db.execute('INSERT OR IGNORE INTO root_parked_activations VALUES(?)', (request_id,))
        return dict(project=target_project, root_session=route['session'], creation=job)

    def configure(self, project, session, host, target_project, agent, cwd, prompt, request_id, confirmed, model=None, effort=None, workspaces=None):
        from .root_team_reset import assert_ready
        with contextlib.closing(Coordinator(self.path(target_project))) as reset_coord:
            assert_ready(reset_coord.db)
        root = self.authorize(project, session, host)
        self.active(root, target_project)
        self.fresh(root)
        route = self.route(project, session, host, target_project)
        from .projects import Projects
        members = Projects.workspace_list(workspaces if workspaces is not None else [cwd])
        if cwd is not None and cwd != members[0]:
            raise CoordError('cwd must equal the first project workspace')
        cwd = members[0]
        self.separate_workspace(target_project, members)
        try:
            request_id = str(uuid.UUID(request_id))
        except (ValueError, TypeError, AttributeError):
            raise CoordError('project orchestration request_id must be a UUID')
        if (confirmed is not True or not isinstance(agent, str) or not agent.strip() or len(agent) > 64
                or not isinstance(prompt, str) or not prompt.strip() or len(prompt.encode()) > 60000):
            raise CoordError('project configuration requires confirmation, unique role and bounded prompt')
        for value, limit in ((model, 128), (effort, 32)):
            if value is not None and (not isinstance(value, str) or not value.strip() or len(value) > limit):
                raise CoordError('invalid project model or reasoning effort')
        values = (cwd, agent, model, effort, prompt, request_id)
        # A replacement requires explicit retirement of the former main and a
        # new UUID. Never replace a live registration or replay a cancelled job.
        with self.projects.connection() as db:
            prior = dict(db.execute('SELECT * FROM root_projects WHERE project_id=?', (target_project,)).fetchone())
            prior_members = self.projects._members(db, target_project, prior['cwd'], 'root_project_workspaces')
        replace = False
        if prior['agent']:
            with contextlib.closing(Coordinator(self.path(target_project))) as coord:
                target_exists = coord.db.execute('SELECT 1 FROM sessions WHERE id=?', (prior['orchestrator_session'],)).fetchone()
                pending = coord.db.execute("SELECT 1 FROM session_resets WHERE id=? AND status NOT IN ('completed','failed','cancelled')", (prior['request_id'],)).fetchone()
                replace = not target_exists and not pending and request_id != prior['request_id']
        with contextlib.closing(Coordinator(self.path(target_project))) as coord:
            from .session_reset import SessionResetState
            resets = SessionResetState(coord)
            raw = coord.db.execute('SELECT * FROM session_resets WHERE id=?', (request_id,)).fetchone()
            original = resets.job(request_id) if raw else None
            if original and (prior['request_id'] != request_id or original['requester_session'] != route['session']
                    or original['kind'] != 'create' or not original['project_scope']
                    or original['prompt'] != self.project_prompt(target_project, route['session'], prompt)
                    or original['requested_cwd'] != cwd or original['requested_workspaces'] != members or original['requested_model'] != model or original['requested_effort'] != effort
                    or (prior['orchestrator_session'] and prior['orchestrator_session'] != original['target_session'])):
                raise CoordError('project setup UUID belongs to a different creation; preserve original request and inspect it')
        with self.projects.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            old = db.execute('SELECT * FROM root_projects WHERE project_id=?', (target_project,)).fetchone()
            if old['agent'] and not replace and (tuple(old[key] for key in ('cwd', 'agent', 'model', 'effort', 'prompt', 'request_id')) != values or prior_members != members):
                db.rollback()
                raise CoordError('project orchestrator already configured; inspect original request or adopt an existing main')
            db.execute('UPDATE root_projects SET cwd=?,agent=?,model=?,effort=?,prompt=?,request_id=?,adopted=0 WHERE project_id=?', (*values, target_project))
            self.projects._save_members(db, target_project, members, 'root_project_workspaces')
            db.commit()
        self.join(root, dict(project_id=target_project, session_id=route['session'], cwd=cwd))
        # The registry records the immutable intent before project mutation. A
        # retry always reuses this UUID, including after a dropped HTTP response.
        from .bridge_state import BridgeState
        from .session_reset import SessionResetState
        with contextlib.closing(Coordinator(self.path(target_project), route['session'])) as coord:
            BridgeState(coord)
            coord._fresh(coord.db, route['session'])
            resets = SessionResetState(coord)
            existing = coord.db.execute('SELECT * FROM session_resets WHERE id=?', (request_id,)).fetchone()
            if existing and existing['old_thread_id'] != root['thread_id'] and existing['status'] == 'completed':
                job = resets.public(resets.job(request_id))
            else:
                job = resets.create(agent, root['thread_id'], self.project_prompt(target_project, route['session'], prompt), request_id, True,
                    model=model, reasoning_effort=effort, cwd=cwd, project_scope=True, project_orchestrator=True, workspaces=members)
        with self.projects.connection() as db:
            db.execute('UPDATE root_projects SET orchestrator_session=? WHERE project_id=?', (job['target_session'], target_project))
        self.sync()
        return dict(project=target_project, root_session=route['session'], creation=job)

    def retry_setup(self, project, session, host, target_project, request_id, confirmed):
        from .root_team_reset import assert_ready
        with contextlib.closing(Coordinator(self.path(target_project))) as reset_coord:
            assert_ready(reset_coord.db)
        self.authorize(project, session, host)
        with self.projects.connection() as db:
            row = db.execute('SELECT * FROM root_projects WHERE project_id=?', (target_project,)).fetchone()
        if not row or row['request_id'] != request_id or row['adopted']:
            raise CoordError('retry setup requires the exact original configured project UUID')
        with self.projects.connection() as db:
            members = self.projects._members(db, target_project, row['cwd'], 'root_project_workspaces')
        return self.configure(project, session, host, target_project, row['agent'], row['cwd'], row['prompt'], row['request_id'], confirmed, row['model'], row['effort'], workspaces=members)

    @staticmethod
    def project_prompt(project, root_session, prompt):
        return (f'You are the project orchestrator for exact project ID {project}. '
                'Keep project requirements, architecture, decisions, instructions, design reasoning and detailed logs in this conversation and project checkpoints. '
                'Delegate all coding, source inspection, investigation, testing, technical verification and independent artifact review to bounded workers; do not perform those activities personally, including as a quick check or review substitute. Retain coordination, requirements and decision context. '
                'Stay responsive through concise asynchronous status/blocker updates, exact assignment ownership and request IDs. '
                'Send root only concise reports containing project ID, goal, status, blocker, decision needed and checkpoint path. '
                f'Send reports to the exact root session {root_session} within this project; do not export project requirements as global defaults. '
                'Manage independent workers through supported agent-create/control commands. Exact holds, queues and receipts remain mandatory. '
                'For a typed inbox-currentness no-effect result, route recovery to the same worker: fully consume its own messages and recheck the original scope, ownership and lease before continuation. Security denial, changed source or owner, revocation, expiry and unknown transport effects remain distinct; never blindly retry or manufacture approval. Continue an independent authorized task while the worker recovers. '
                'Delegate local-work hygiene to bounded workers: use narrow ignore rules for local checkpoints, preserve source/assets/docs and local copies when untracking, and never delete broadly, rewrite history, reset or publish without separate authority. '
                'Use the project workspace AGENTS.md when present; do not change model, identity, permissions, bindings, bridge or service without separate supported authorization.\n\n' + prompt)

    def adopt(self, project, session, host, target_project, target, expected_thread, cwd, confirmed, workspaces=None):
        from .root_team_reset import assert_ready
        with contextlib.closing(Coordinator(self.path(target_project))) as reset_coord:
            assert_ready(reset_coord.db)
        root = self.authorize(project, session, host)
        self.active(root)
        self.fresh(root)
        self.route(project, session, host, target_project)
        from .projects import Projects
        members = Projects.workspace_list(workspaces if workspaces is not None else [cwd])
        if cwd is not None and cwd != members[0]:
            raise CoordError('cwd must equal the first project workspace')
        cwd = members[0]
        if confirmed is not True or any(overlaps(member, neutral) for member in members for neutral in root['workspaces']):
            raise CoordError('adoption requires confirmation and a separate project workspace')
        self.separate_workspace(target_project, members)
        with self.projects.connection() as db:
            prior = db.execute('SELECT request_id FROM root_projects WHERE project_id=?', (target_project,)).fetchone()
        with contextlib.closing(Coordinator(self.path(target_project))) as coord:
            if prior['request_id']:
                job = coord.db.execute('SELECT status FROM session_resets WHERE id=?', (prior['request_id'],)).fetchone()
                if job and job['status'] not in ('completed','cancelled','failed'):
                    raise CoordError('resolve the pending project creation before adopting another main')
            row = coord.db.execute('SELECT id,agent FROM sessions WHERE id=? OR agent=?', (target, target)).fetchone()
            binding = coord.db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?', (row['id'],)).fetchone() if row else None
            observed = coord.db.execute('SELECT * FROM agent_control_observations WHERE target_session=?', (row['id'],)).fetchone() if row else None
            owner = coord.db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (row['id'],)).fetchone() if row else None
            if (not binding or binding['thread_id'] != expected_thread or expected_thread == root['thread_id']
                    or not owner or owner['host_id'] != host or not observed or time.time() - observed['checked_at'] > 120
                    or observed['thread_id'] != expected_thread or observed['cwd'] != cwd):
                raise CoordError('adopt an exact existing main on this host after a fresh agent-status observation of its workspace')
            from .agent_control import AgentControlState
            if AgentControlState.observed_workspaces(coord.db, row['id'], cwd) != members:
                raise CoordError('adoption requires the exact observed project workspace member list')
        with self.projects.connection() as db:
            db.execute('UPDATE root_projects SET cwd=?,agent=?,orchestrator_session=?,model=?,effort=?,adopted=1 WHERE project_id=?', (cwd, row['agent'], row['id'], observed['model'], observed['reasoning_effort'], target_project))
            self.projects._save_members(db, target_project, members, 'root_project_workspaces')
        self.sync()
        return self.status()

    def separate_workspace(self, project, members):
        if isinstance(members, str):
            members = [members]
        with self.projects.connection() as db:
            self.projects.validate_workspace_changes(db, {project: members})

    def reconfigure_workspaces(self, project, session, host, target_project, expected_thread, workspaces, prompt, request_id, confirmed):
        from .root_team_reset import assert_ready
        with contextlib.closing(Coordinator(self.path(target_project))) as reset_coord:
            assert_ready(reset_coord.db)
        from .projects import Projects
        from .bridge_state import BridgeState
        from .session_reset import SessionResetState
        root = self.authorize(project, session, host)
        self.active(root)
        self.fresh(root)
        route = self.route(project, session, host, target_project)
        members = Projects.workspace_list(workspaces)
        try:
            request_id = str(uuid.UUID(request_id))
        except (TypeError, ValueError, AttributeError):
            raise CoordError('project reconfiguration request_id must be a UUID')
        if confirmed is not True or not isinstance(prompt, str) or not prompt.strip() or len(prompt.encode()) > 60000:
            raise CoordError('project reconfiguration requires confirmation and bounded prompt')
        with self.projects.connection() as db:
            prior = db.execute('SELECT * FROM root_projects WHERE project_id=?', (target_project,)).fetchone()
            pending = db.execute('SELECT * FROM root_reconfigurations WHERE project_id=?', (target_project,)).fetchone()
            collision = db.execute('SELECT project_id FROM root_reconfigurations WHERE request_id=? AND project_id<>?', (request_id, target_project)).fetchone()
            if collision:
                raise CoordError('project reconfiguration UUID already belongs to another project')
        if not prior or not prior['orchestrator_session']:
            raise CoordError('reconfigure an exact existing project orchestrator')
        with contextlib.closing(Coordinator(self.path(target_project), route['session'])) as coord:
            BridgeState(coord)
            resets = SessionResetState(coord)
            if pending and pending['request_id'] == request_id:
                original = resets.job(request_id)
                if (pending['old_thread_id'], pending['prompt'], original['requested_workspaces']) != (expected_thread, prompt, members):
                    raise CoordError('project reconfiguration retry payload differs from the original')
                self.reconcile_reconfiguration(target_project)
                return dict(project=target_project, reconfiguration=resets.public(original))
            if pending and resets.job(pending['request_id'])['status'] not in ('completed', 'cancelled', 'failed'):
                raise CoordError('resolve the pending project workspace reconfiguration first')
            target = prior['orchestrator_session']
            binding = coord.db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?', (target,)).fetchone()
            existing = coord.db.execute('SELECT 1 FROM session_resets WHERE id=?', (request_id,)).fetchone()
            if not existing and (not binding or binding['thread_id'] != expected_thread or expected_thread == root['thread_id']):
                raise CoordError('project reconfiguration requires the exact current project main thread')
            self.separate_workspace(target_project, members)
            job = resets.request(target, expected_thread, self.project_prompt(target_project, route['session'], prompt),
                                 request_id, True, project_scope=True, workspaces=members)
        # Existing reset UUID is durable before the bounded registry journal.
        # If this write fails, retrying the same request reconstructs the intent.
        with self.projects.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('INSERT OR REPLACE INTO root_reconfigurations VALUES(?,?,?,?,?)',
                       (target_project, request_id, target, expected_thread, prompt))
            self.projects._save_members(db, target_project, members, 'root_reconfiguration_workspaces')
            db.commit()
        return dict(project=target_project, reconfiguration=job)

    def reconcile_reconfiguration(self, project):
        from .session_reset import SessionResetState
        with self.projects.connection() as db:
            pending = db.execute('SELECT * FROM root_reconfigurations WHERE project_id=?', (project,)).fetchone()
        if not pending:
            return None
        with contextlib.closing(Coordinator(self.path(project))) as coord:
            resets = SessionResetState(coord)
            job = resets.job(pending['request_id'])
            binding = coord.db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?', (pending['target_session'],)).fetchone()
        if job['status'] == 'completed':
            if not binding or binding['thread_id'] != job['new_thread_id']:
                raise CoordError('completed reconfiguration binding differs; preserve current assignment')
            with self.projects.connection() as db:
                db.execute('BEGIN IMMEDIATE')
                db.execute('UPDATE root_projects SET cwd=? WHERE project_id=?', (job['requested_workspaces'][0], project))
                self.projects._save_members(db, project, job['requested_workspaces'], 'root_project_workspaces')
                db.execute('DELETE FROM root_reconfiguration_workspaces WHERE project_id=?', (project,))
                row = dict(db.execute('SELECT * FROM root_projects WHERE project_id=?', (project,)).fetchone())
                db.commit()
            root = self.config()
            if root and root['mode'] == 'running':
                self.join(root, row)
        elif job['status'] in ('failed', 'cancelled'):
            with self.projects.connection() as db:
                db.execute('DELETE FROM root_reconfiguration_workspaces WHERE project_id=?', (project,))
        return resets.public(job)

    def active_projects(self):
        """Select enrolled project teams by durable unfinished assignments."""
        return {'projects': [row for row in self.status().get('projects', [])
                             if row['joined'] and row['activity']['active']]}

    def status(self):
        root = self.config()
        if not root:
            return dict(enabled=False, projects=[])
        with self.projects.connection() as db:
            rows = [dict(row, workspaces=self.projects._members(db, row['project_id'], row['cwd'], 'root_project_workspaces')) for row in db.execute('SELECT * FROM root_projects ORDER BY project_id')]
        for row in rows:
            row.pop('prompt', None)
            setup = self.projects.get(row['project_id'])
            row['name'] = setup['name']
            row['activity'] = self.projects.activity_summary(row['project_id'])
            row['creation_workspace'] = setup.get('workspace')
            row['creation_workspaces'] = setup['workspaces']
            row['reconfiguration'] = self.reconcile_reconfiguration(row['project_id'])
            if row['reconfiguration'] and row['reconfiguration']['status'] == 'completed':
                row['workspaces'] = row['reconfiguration']['requested_workspaces']
                row['cwd'] = row['workspaces'][0]
            row['has_instructions'] = bool(setup.get('instructions'))
            row['provision_ready'] = bool(setup.get('workspace') and setup.get('instructions'))
            row['creation'] = None
            row['orchestrator'] = None
            from .bridge_state import runtime_snapshot
            try:
                with contextlib.closing(Coordinator(self.path(row['project_id']))) as coord:
                    runtime = runtime_snapshot(coord.db)
                    from .root_lifecycle import coordination_status
                    row['po_coordination'] = coordination_status(
                        coord.db, row['session_id'], row['orchestrator_session'] if row['joined'] else None)
                    row['po_coordination']['root_enrollment_current'] = bool(row['joined'])
                    if row['orchestrator_session']:
                        from .agent_control import AgentControlState
                        row['orchestrator'] = next((main for main in AgentControlState(coord).roster() if main['id'] == row['orchestrator_session']),None)
                        if row['orchestrator'] and row['orchestrator']['thread_id']:
                            observed = coord.db.execute('''SELECT model,reasoning_effort
                                FROM agent_control_observations WHERE target_session=? AND thread_id=?''',
                                (row['orchestrator_session'], row['orchestrator']['thread_id'])).fetchone()
                            if observed and observed['model']:
                                # The directory keeps provisioning settings; the
                                # overview shows the currently bound thread.
                                row.update(model=observed['model'], effort=observed['reasoning_effort'])
            except (CoordError, OSError, sqlite3.Error) as error:
                row.update(joined=0,error=str(error))
                runtime = None
            client = next((client for client in (runtime or {}).get('clients', []) if client['host_id'] == root['host_id']), None)
            row['bridge'] = dict(recent=bool(client and client['recent'] and not client['error']),
                                 project_scope_worker=bool(client and client.get('project_scope_worker')),
                                 project_workspaces_worker=bool(client and client.get('project_workspaces_worker')), error=client['error'] if client else 'no bridge heartbeat on root host')
            if row['request_id'] and not row['adopted'] and row['joined']:
                from .session_reset import SessionResetState
                with contextlib.closing(Coordinator(self.path(row['project_id']))) as coord:
                    if coord.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='session_resets'").fetchone():
                        job = coord.db.execute('SELECT * FROM session_resets WHERE id=?', (row['request_id'],)).fetchone()
                        if job:
                            row['creation'] = SessionResetState.public(SessionResetState(coord).job(job['id']))
            row['setup'] = 'routing_error' if not row['joined'] else ('adopted' if row['adopted'] else
                (row['creation']['status'] if row['creation'] else ('awaiting_original_request_retry' if row['request_id'] else
                 ('ready_to_provision' if row['provision_ready'] else 'needs_workspace_and_prompt'))))
            if row['reconfiguration'] and row['reconfiguration']['status'] not in ('completed', 'failed', 'cancelled'):
                row['setup'] = 'workspace_reconfiguration_' + row['reconfiguration']['status']
            if len(row['creation_workspaces']) > 1 and not row['bridge']['project_workspaces_worker']:
                row['bridge']['workspace_upgrade_required'] = True
        with self.projects.connection() as db:
            team_stops = [dict(item) for item in db.execute('SELECT * FROM root_team_operations WHERE resumed_at IS NULL')]
        return dict(enabled=True, root=root, projects=rows,team_stops=team_stops,
                    supported_lifecycle=['root fresh-start --expected-thread OLD_ROOT_THREAD_UUID --prompt-file BOOT_FILE --request-id UUID --confirm',
                                         'root fresh-status --request-id UUID',
                                         'root stop-all --request-id UUID --confirm',
                                         'root stop-status --request-id UUID',
                                         'root resume-all --request-id UUID --confirm'])

    def detach(self, project, session, host, confirmed):
        root = self.authorize(project, session, host)
        if confirmed is not True:
            raise CoordError('root detach requires explicit confirmation')
        if root['mode'] == 'detached':
            return self.finish_detach(project)
        self.fresh(root)
        with contextlib.closing(Coordinator(self.path(project))) as coord:
            observed = coord.db.execute('SELECT * FROM agent_control_observations WHERE target_session=?', (session,)).fetchone()
            if not observed or time.time()-observed['checked_at'] > 120 or observed['queue_count']:
                raise CoordError('refresh root agent-status and preserve queued app-server input before detaching')
        # Journal the admission pause first. Already crossed RPCs remain in the
        # ordinary wake ledger and block closure until reconciled. No queue input
        # is deleted. Failed checks leave routing paused and registration intact.
        from .root_admin_attachment_members import cutover_pending
        with self.projects.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            if cutover_pending(db):
                raise CoordError('Root administrative attachment needs reconciliation')
            current = db.execute('SELECT home_project,home_session,host_id,thread_id,mode FROM root_config WHERE singleton=1').fetchone()
            if not current or tuple(current) != (project,session,host,root['thread_id'],root['mode']):
                raise CoordError('canonical Root changed before detach')
            db.execute("UPDATE root_config SET mode='detaching' WHERE singleton=1")
            db.commit()
        self.sync()
        # Retain registrations and inboxes. Only remove the global role once all
        # projections are clean; a failure makes a retry safe and reviewable.
        with self.projects.connection() as db:
            rows = [dict(row, workspaces=self.projects._members(db, row['project_id'], row['cwd'], 'root_project_workspaces')) for row in db.execute('SELECT * FROM root_projects')]
        from .session_reset import SessionResetState
        for row in rows:
            with contextlib.closing(Coordinator(self.path(row['project_id']), row['session_id'])) as coord:
                SessionResetState(coord).clean(row['session_id'], self.config()['thread_id'])
                if coord.db.execute("SELECT 1 FROM session_resets WHERE (requester_session=? OR (old_thread_id=? AND project_scope=1)) AND status NOT IN ('completed','failed','cancelled')", (row['session_id'], root['thread_id'])).fetchone():
                    raise CoordError('resolve pending root-created mains before detaching')
                coord._fresh(coord.db, row['session_id'])
                if coord.db.execute("SELECT 1 FROM agent_control_requests WHERE target_session=? AND status NOT IN ('completed','failed','cancelled')", (row['session_id'],)).fetchone():
                    raise CoordError('resolve root runtime inspection/control requests before detaching')
                if coord.db.execute('SELECT 1 FROM messages WHERE recipient_session=? AND acked_at IS NULL', (row['session_id'],)).fetchone():
                    raise CoordError('consume and acknowledge every root project delivery before detaching')
        for row in rows:
            with contextlib.closing(Coordinator(self.path(row['project_id']))) as coord:
                with coord.tx() as db:
                    if row['project_id'] != project:
                        db.execute('DELETE FROM bridge_bindings WHERE session_id=?', (row['session_id'],))
        with self.projects.connection() as db:
            db.execute("UPDATE root_config SET mode='detached'")
        return self.finish_detach(project)

    def finish_detach(self, project):
        with self.projects.connection() as db:
            rows = [dict(row, workspaces=self.projects._members(db, row['project_id'], row['cwd'], 'root_project_workspaces')) for row in db.execute('SELECT * FROM root_projects')]
        for row in rows:
            with contextlib.closing(Coordinator(self.path(row['project_id']))) as coord:
                coord.db.execute('DELETE FROM root_projections WHERE session_id=?', (row['session_id'],))
        with self.projects.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('DELETE FROM root_config')
            db.execute('DELETE FROM root_workspace_roots')
            db.commit()
        return dict(detached=True, registrations_retained=True)

    @staticmethod
    def _routing_inspection(db, root):
        now = time.time()
        for table in ('agent_control_requests', 'agent_control_observations'):
            if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone():
                raise CoordError('root routing requires a fresh completed own settings inspection')
        inspected = db.execute('''SELECT 1 FROM agent_control_requests r
            JOIN agent_control_observations o ON o.target_session=r.target_session AND o.thread_id=r.thread_id
            WHERE r.target_session=? AND r.thread_id=? AND r.host_id=?
            AND r.action='inspect' AND r.status='completed'
            AND r.updated_at BETWEEN ? AND ? AND o.checked_at BETWEEN ? AND ?''',
            (root['home_session'], root['thread_id'], root['host_id'], now-120, now, now-120, now)).fetchone()
        if not inspected:
            raise CoordError('root routing requires a fresh completed own settings inspection')

    def _routing_fresh(self, root):
        # A completed native inspection is distinct from the consumed-inbox guard.
        with self._readiness_db(self.path(root['home_project'])) as db:
            self._routing_inspection(db, root)
        self.fresh(root)

    def _routing_projection(self, root, item, paused):
        # Projection-only maintenance: no onboarding, notices, native dispatch,
        # registration repair, PO updates or team-barrier changes.
        path = self.path(item['project_id'])
        with contextlib.closing(sqlite3.connect(path.as_uri() + ('?mode=ro' if paused is None else '?mode=rw'), uri=True)) as db:
            db.row_factory = sqlite3.Row
            db.execute('BEGIN' if paused is None else 'BEGIN IMMEDIATE')
            route = projection(db, item['session_id'])
            binding = db.execute('SELECT thread_id,parent_session FROM bridge_bindings WHERE session_id=?', (item['session_id'],)).fetchone()
            owner = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (item['session_id'],)).fetchone()
            registered = db.execute('SELECT 1 FROM sessions WHERE id=?', (item['session_id'],)).fetchone()
            if (not route or not binding or not owner or not registered or binding['parent_session']
                    or (route['home_project'], route['home_session'], route['thread_id']) !=
                    (root['home_project'], root['home_session'], root['thread_id'])
                    or binding['thread_id'] != root['thread_id'] or owner['host_id'] != root['host_id']):
                raise CoordError('root routing retained projection, binding or host differs')
            if paused is not None:
                db.execute('UPDATE root_projections SET paused=? WHERE session_id=?', (int(paused), item['session_id']))
                db.commit()
            else:
                return bool(route.get('paused'))

    def pause_routing(self, project, session, host, expected_thread, request_id, confirmed):
        root = self.authorize(project, session, host)
        if confirmed is not True:
            raise CoordError('root routing pause requires explicit confirmation')
        try:
            request_id = str(uuid.UUID(request_id))
        except (ValueError, TypeError, AttributeError):
            raise CoordError('root routing pause request_id must be a UUID')
        if expected_thread != root['thread_id']:
            raise CoordError('root routing expected thread differs')
        with self.projects.connection() as db:
            from .root_pending_fresh import routing_fresh_blocker
            blocker = routing_fresh_blocker(db, self.projects.database, root)
            if blocker: raise CoordError(blocker + '; routing pause withheld')
            if root['mode'] not in ('running', 'maintenance_paused'):
                raise CoordError('root is detaching or detached; routing pause withheld')
            prior = db.execute('SELECT * FROM root_routing_pauses WHERE id=?', (request_id,)).fetchone()
            if prior and (prior['home_project'], prior['home_session'], prior['host_id'], prior['thread_id']) != (project, session, host, expected_thread):
                raise CoordError('root routing pause retry payload differs')
            rows = [dict(row) for row in db.execute('SELECT project_id,session_id FROM root_projects ORDER BY project_id')]
            if dict(project_id=project, session_id=session) not in rows:
                raise CoordError('root routing canonical retained projection is missing')
            if prior:
                saved = [dict(row) for row in db.execute('SELECT project_id,session_id FROM root_routing_pause_projects WHERE request_id=? ORDER BY project_id', (request_id,))]
                if saved != rows:
                    raise CoordError('root routing retained cohort differs')
            elif root['mode'] != 'running':
                raise CoordError('root routing pause already pending; reuse its exact request')
        if prior and prior['status'] == 'resumed':
            with self.projects.connection() as db:
                latest = db.execute('SELECT id FROM root_routing_pauses ORDER BY created_at DESC,id DESC LIMIT 1').fetchone()
            if (root['mode'] != 'running' or latest['id'] != request_id
                    or any(self._routing_projection(root, row, None) for row in rows)):
                raise CoordError('completed root routing pause readback differs')
            return dict(request_id=request_id, status='resumed', projects=rows)
        self._routing_fresh(root)
        for row in rows:
            self._routing_projection(root, row, None)
        if not prior:
            from .root_admin_attachment_members import cutover_pending
            with self.projects.connection() as db:
                db.execute('BEGIN IMMEDIATE')
                if cutover_pending(db):
                    raise CoordError('Root administrative attachment needs reconciliation')
                current = db.execute('SELECT home_project,home_session,host_id,thread_id,mode FROM root_config WHERE singleton=1').fetchone()
                if not current or tuple(current) != (project,session,host,expected_thread,'running'):
                    raise CoordError('canonical Root changed before routing pause')
                current_rows = [dict(row) for row in db.execute(
                    'SELECT project_id,session_id FROM root_projects ORDER BY project_id'
                )]
                if current_rows != rows:
                    raise CoordError('Root projection scope changed before routing pause')
                blocker = routing_fresh_blocker(db, self.projects.database, root)
                if blocker: raise CoordError(blocker + '; routing pause withheld')
                db.execute("INSERT INTO root_routing_pauses VALUES(?,?,?,?,?,?,'pausing',NULL)", (request_id, project, session, host, expected_thread, time.time()))
                db.executemany('INSERT INTO root_routing_pause_projects VALUES(?,?,?)', [(request_id, row['project_id'], row['session_id']) for row in rows])
                db.execute("UPDATE root_config SET mode='maintenance_paused'")
                db.commit()
        for row in rows:
            self._routing_projection(root, row, True)
        if not all(self._routing_projection(root, row, None) for row in rows):
            raise CoordError('root routing projection pause readback differs; retry exact request')
        with self.projects.connection() as db:
            db.execute("UPDATE root_routing_pauses SET status='paused' WHERE id=? AND status='pausing'", (request_id,))
        return dict(request_id=request_id, status='paused', projects=rows)

    def _routing_resumed_result(self, root, request_id, rows):
        from .supervisor import SupervisorState
        with contextlib.closing(Coordinator(self.path(root['home_project']), root['home_session'])) as coord:
            notice = SupervisorState(coord).root_routing_resumed(self, root['home_project'], root['home_session'], root['host_id'], request_id)
        return dict(root=self.config(), request_id=request_id, status='resumed', projects=rows, notice_message_id=notice)

    def resume_routing(self, project, session, host, confirmed):
        root = self.authorize(project, session, host)
        if root['mode'] == 'detached':
            raise CoordError('detach committed; finish root detach rather than resuming old routing')
        if confirmed is not True:
            raise CoordError('root routing resume requires explicit confirmation')
        if root['mode'] == 'running':
            with self.projects.connection() as db:
                latest = db.execute('SELECT * FROM root_routing_pauses ORDER BY created_at DESC,id DESC LIMIT 1').fetchone()
                rows = [dict(row) for row in db.execute('SELECT project_id,session_id FROM root_projects ORDER BY project_id')]
            if (latest and latest['status'] == 'resumed'
                    and (latest['home_project'], latest['home_session'], latest['host_id'], latest['thread_id']) ==
                    (project, session, host, root['thread_id'])):
                # Only recover/report the already committed lifecycle effect.
                # Do not consume subsequent operator input or repeat routing sync.
                return self._routing_resumed_result(root, latest['id'], rows)
        if root['mode'] == 'maintenance_paused':
            self._routing_fresh(root)
        else:
            self.fresh(root)
        with self.projects.connection() as db:
            from .root_pending_fresh import routing_fresh_blocker
            blocker = routing_fresh_blocker(db, self.projects.database, root)
            if blocker: raise CoordError(blocker + '; routing resume withheld')
            rows = [dict(row) for row in db.execute('SELECT project_id,session_id FROM root_projects ORDER BY project_id')]
            if root['mode'] == 'maintenance_paused':
                pauses = list(db.execute("SELECT * FROM root_routing_pauses WHERE status IN ('pausing','paused')"))
                if (len(pauses) != 1 or (pauses[0]['home_project'], pauses[0]['home_session'], pauses[0]['host_id'], pauses[0]['thread_id']) !=
                        (project, session, host, root['thread_id'])):
                    raise CoordError('root routing pause audit differs; resume withheld')
                saved = [dict(row) for row in db.execute('SELECT project_id,session_id FROM root_routing_pause_projects WHERE request_id=? ORDER BY project_id', (pauses[0]['id'],))]
                if saved != rows:
                    raise CoordError('root routing retained cohort differs; resume withheld')
        if root['mode'] == 'maintenance_paused':
            for row in rows:
                self._routing_projection(root, row, None)
            for row in rows:
                self._routing_projection(root, row, False)
            if any(self._routing_projection(root, row, None) for row in rows):
                raise CoordError('root routing projection resume readback differs; retry resume')
            with self.projects.connection() as db:
                db.execute('BEGIN IMMEDIATE')
                db.execute("UPDATE root_config SET mode='running'")
                db.execute("UPDATE root_routing_pauses SET status='resumed',resumed_at=? WHERE status IN ('pausing','paused')", (time.time(),))
                db.commit()
            return self._routing_resumed_result(root, pauses[0]['id'], rows)
        with self.projects.connection() as db:
            db.execute("UPDATE root_config SET mode='running'")
        self.sync()
        return self.status()
