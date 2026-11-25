"""Explicit project-manager exact-file task bindings; never native completion proof."""
import math
import uuid
from . import core
from .core import CoordError, assert_agent_running
from .exact_file_read import exact_path, MAX_BYTES, SELECTED_PATH, CHARACTERS_FILES
from .management import manager


def _schema(db):
    # execute, not executescript: callers keep the surrounding transaction.
    db.execute('''CREATE TABLE IF NOT EXISTS exact_file_bindings (
        id TEXT PRIMARY KEY, request_id TEXT NOT NULL UNIQUE, project_id TEXT NOT NULL,
        issuer_session TEXT NOT NULL, po_session TEXT NOT NULL, root_session TEXT NOT NULL,
        owner_session TEXT NOT NULL, host_id TEXT NOT NULL, thread_id TEXT NOT NULL,
        task_id TEXT NOT NULL, source_request_id TEXT NOT NULL,
        source_path TEXT NOT NULL, output_path TEXT NOT NULL, max_bytes INTEGER NOT NULL,
        minutes REAL NOT NULL, created_at REAL NOT NULL, expires_at REAL NOT NULL)''')
    db.execute('''CREATE TABLE IF NOT EXISTS exact_file_binding_revocations (
        binding_id TEXT PRIMARY KEY, request_id TEXT NOT NULL UNIQUE, actor_session TEXT NOT NULL,
        reason TEXT NOT NULL, revoked_at REAL NOT NULL)''')
    db.execute('''CREATE TABLE IF NOT EXISTS exact_file_binding_reservations (
        reservation_id TEXT PRIMARY KEY, binding_id TEXT NOT NULL)''')
    for table in ('exact_file_bindings','exact_file_binding_revocations','exact_file_binding_reservations'):
        for action in ('UPDATE','DELETE'):
            db.execute(f'''CREATE TRIGGER IF NOT EXISTS {table}_no_{action.lower()}
                BEFORE {action} ON {table} BEGIN SELECT RAISE(ABORT,'exact file binding audit is immutable'); END''')


def _uuid(value):
    try:result=str(uuid.UUID(value))
    except (ValueError,TypeError,AttributeError):raise CoordError('exact file binding request_id must be a UUID')
    if result!=value:raise CoordError('exact file binding request_id must be canonical')
    return result


def _roles(db):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='project_orchestrators'").fetchone():
        raise CoordError('exact file binding requires a designated project orchestrator')
    rows=db.execute('SELECT session_id,root_session FROM project_orchestrators').fetchall()
    if len(rows)!=1:raise CoordError('exact file binding requires one current project orchestrator')
    return rows[0]


def _managed(db,owner,po):
    current,seen=owner,set()
    while current and current not in seen:
        seen.add(current)
        row=db.execute('SELECT parent_session FROM bridge_bindings WHERE session_id=?',(current,)).fetchone()
        current=(row['parent_session'] if row else None) or manager(db,current)
        if current==po:return True
    return False


def _task(db,project,owner,host,thread,task_id,source_id,po):
    if owner in (po['session_id'],po['root_session']) or not _managed(db,owner,po['session_id']):
        raise CoordError('exact file owner must be a current managed worker')
    if db.execute('SELECT 1 FROM retired_sessions WHERE id=?',(owner,)).fetchone():
        raise CoordError('exact file owner is retired')
    bound=db.execute('SELECT thread_id,parent_session FROM bridge_bindings WHERE session_id=?',(owner,)).fetchone()
    registered=db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?',(owner,)).fetchone()
    if (not bound or bound['thread_id']!=thread or bound['parent_session'] is not None
            or not registered or registered['host_id']!=host):
        raise CoordError('exact file owner main thread or host differs')
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='project_work'").fetchone():
        raise CoordError('exact file binding requires an assigned task')
    task=db.execute('SELECT * FROM project_work WHERE id=? AND project_id=?',(task_id,project)).fetchone()
    if (not task or task['kind']!='task' or task['owner_session']!=owner
            or task['assigned_by']!=po['session_id'] or task['source_request_id']!=source_id
            or task['state'] not in ('queued','in_progress')):
        raise CoordError('exact file binding requires the exact current runnable task')
    source=db.execute('SELECT sender_session,recipient_session FROM messages WHERE id=?',(source_id,)).fetchone()
    if not source or (source['sender_session'],source['recipient_session'])!=(po['session_id'],owner):
        raise CoordError('exact file assignment source differs')
    assert_agent_running(db,owner)


def _row(db,binding_id,project):
    if not isinstance(binding_id,str) or not binding_id.startswith('exact_binding_'):raise CoordError('unknown exact file binding')
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='exact_file_bindings'").fetchone():
        raise CoordError('unknown exact file binding')
    row=db.execute('SELECT * FROM exact_file_bindings WHERE id=? AND project_id=?',(binding_id,project)).fetchone()
    if not row:raise CoordError('unknown exact file binding')
    return row


def _manager(db,actor):
    po=_roles(db)
    if actor not in (po['session_id'],po['root_session']):
        raise CoordError('only the current project PO or Root may issue or revoke an exact file binding')
    assert_agent_running(db,actor)
    return po


def _live(db,row):
    revoked=db.execute('SELECT 1 FROM exact_file_binding_revocations WHERE binding_id=?',(row['id'],)).fetchone()
    if revoked:raise CoordError('exact file binding is revoked')
    if core._now()>=row['expires_at']:raise CoordError('exact file binding has expired')
    po=_roles(db)
    if (po['session_id'],po['root_session'])!=(row['po_session'],row['root_session']):
        raise CoordError('exact file binding project authority changed')
    _task(db,row['project_id'],row['owner_session'],row['host_id'],row['thread_id'],row['task_id'],row['source_request_id'],po)


def public(db,row):
    result=dict(row)
    try:_live(db,row);result['state']='active';result['invalid_reason']=None
    except CoordError as error:
        result['state']='revoked' if 'is revoked' in str(error) else ('expired' if 'has expired' in str(error) else 'invalid')
        result['invalid_reason']=str(error)
    result['native_completion_proven']=False
    return result


def issue(coord,project,p):
    required={'to','task_id','source_request_id','expected_host','expected_thread','path','output','max_bytes','minutes','request_id','confirm'}
    if set(p)!=required or p['confirm'] is not True:raise CoordError('invalid exact file binding intent')
    request=_uuid(p['request_id']);path=exact_path(p['path']);output=exact_path(p['output'])
    if path==output:raise CoordError('exact file source and destination must differ')
    if any(value==SELECTED_PATH or value in CHARACTERS_FILES for value in (path,output)):
        raise CoordError('historical exact file selections retain their original authority')
    if (isinstance(p['max_bytes'],bool) or not isinstance(p['max_bytes'],int)
            or not 1<=p['max_bytes']<=MAX_BYTES):raise CoordError('exact file binding byte limit out of range')
    minutes=p['minutes']
    if isinstance(minutes,bool) or not isinstance(minutes,(int,float)) or not math.isfinite(minutes) or not 0<minutes<=60:
        raise CoordError('exact file binding minutes must be positive and at most 60')
    for key in ('to','task_id','source_request_id','expected_host','expected_thread'):
        if not isinstance(p[key],str) or not p[key]:raise CoordError('exact file binding identities must be exact')
    actor=coord.require_session()
    with coord.tx() as db:
        po=_manager(db,actor);coord._fresh(db,actor)
        _task(db,project,p['to'],p['expected_host'],p['expected_thread'],p['task_id'],p['source_request_id'],po)
        _schema(db)
        fields=('project_id','issuer_session','po_session','root_session','owner_session','host_id','thread_id','task_id','source_request_id','source_path','output_path','max_bytes','minutes')
        values=(project,actor,po['session_id'],po['root_session'],p['to'],p['expected_host'],p['expected_thread'],p['task_id'],p['source_request_id'],path,output,p['max_bytes'],minutes)
        old=db.execute('SELECT * FROM exact_file_bindings WHERE request_id=?',(request,)).fetchone()
        if old:
            if tuple(old[key] for key in fields)!=values:raise CoordError('exact file binding retry payload differs')
            return public(db,old)
        now=core._now();binding='exact_binding_'+uuid.UUID(request).hex
        db.execute('INSERT INTO exact_file_bindings VALUES('+','.join('?' for _ in range(17))+')',(binding,request,*values,now,now+minutes*60))
        return public(db,_row(db,binding,project))


def status(coord,project,binding_id,host):
    actor=coord.require_session()
    with coord.tx() as db:
        row=_row(db,binding_id,project);po=_roles(db)
        if actor not in (po['session_id'],po['root_session']) and (actor,host)!=(row['owner_session'],row['host_id']):
            raise CoordError('exact file binding belongs to another worker or host')
        return public(db,row)


def revoke(coord,project,p):
    if set(p)!={'binding_id','request_id','reason','confirm'} or p['confirm'] is not True:raise CoordError('invalid exact file revocation intent')
    request=_uuid(p['request_id'])
    if not isinstance(p['reason'],str) or not p['reason'].strip() or len(p['reason'])>500:raise CoordError('exact file revocation reason required')
    actor=coord.require_session()
    with coord.tx() as db:
        _manager(db,actor);coord._fresh(db,actor);row=_row(db,p['binding_id'],project)
        old=db.execute('SELECT * FROM exact_file_binding_revocations WHERE binding_id=? OR request_id=?',(row['id'],request)).fetchone()
        values=(row['id'],request,actor,p['reason'])
        if old:
            if tuple(old[key] for key in ('binding_id','request_id','actor_session','reason'))!=values:raise CoordError('exact file revocation retry differs')
        else:db.execute('INSERT INTO exact_file_binding_revocations VALUES(?,?,?,?,?)',(*values,core._now()))
        return public(db,row)


def authorize(db,project,owner,host,path,binding_id,*,output=None,max_bytes=None,reservation_id=None,active=True):
    row=_row(db,binding_id,project)
    if (row['owner_session'],row['host_id'],row['source_path'])!=(owner,host,path):
        raise CoordError('exact file binding owner/host/path differs')
    if output is not None and (type(max_bytes) is not int or (row['output_path'],row['max_bytes'])!=(output,max_bytes)):
        raise CoordError('exact file binding destination or byte limit differs')
    if active:_live(db,row)
    if reservation_id is not None:
        relation=db.execute('SELECT binding_id FROM exact_file_binding_reservations WHERE reservation_id=?',(reservation_id,)).fetchone()
        if not relation or relation['binding_id']!=binding_id:raise CoordError('exact file reservation binding differs')
    return row


def reserved_binding(db,reservation_id):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='exact_file_binding_reservations'").fetchone():return None
    row=db.execute('SELECT binding_id FROM exact_file_binding_reservations WHERE reservation_id=?',(reservation_id,)).fetchone()
    return row['binding_id'] if row else None
