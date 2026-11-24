"""Scoped host-reported snapshot execution, separate from custody release.

The server binds reports to the admitted typed helper and acknowledged guard
closure. These reports are not kernel attestation or proof of all host effects.
"""
import hashlib
import base64
from pathlib import Path
import time
import uuid
import re
import subprocess

from .core import CoordError
from . import exact_file_bindings as bindings

PRODUCED = ('pid', 'helper_sha256', 'bytes', 'sha256', 'device', 'inode',
            'mtime_ns', 'output_device', 'output_inode', 'output_mtime_ns')
OUTPUT = ('bytes', 'sha256', 'output_device', 'output_inode', 'output_mtime_ns')


def helper_sha256():
    return hashlib.sha256(Path(__file__).with_name('exact_file_read.py').read_bytes()).hexdigest()


def schema(db):
    # No executescript: admission and intent must commit together.
    db.execute('''CREATE TABLE IF NOT EXISTS exact_file_execution_intents (
        run_id TEXT PRIMARY KEY, generation TEXT NOT NULL UNIQUE,
        binding_id TEXT NOT NULL, reservation_id TEXT NOT NULL,
        project_id TEXT NOT NULL, owner_session TEXT NOT NULL, host_id TEXT NOT NULL,
        thread_id TEXT NOT NULL, task_id TEXT NOT NULL, source_request_id TEXT NOT NULL,
        path TEXT NOT NULL, output TEXT NOT NULL, max_bytes INTEGER NOT NULL,
        helper_sha256 TEXT NOT NULL, created_at REAL NOT NULL)''')
    db.execute('''CREATE TABLE IF NOT EXISTS exact_file_execution_produced (
        run_id TEXT PRIMARY KEY, pid INTEGER NOT NULL, helper_sha256 TEXT NOT NULL,
        bytes INTEGER NOT NULL, sha256 TEXT NOT NULL, device INTEGER NOT NULL,
        inode INTEGER NOT NULL, mtime_ns INTEGER NOT NULL, output_device INTEGER NOT NULL,
        output_inode INTEGER NOT NULL, output_mtime_ns INTEGER NOT NULL,
        recorded_at REAL NOT NULL)''')
    db.execute('''CREATE TABLE IF NOT EXISTS exact_file_execution_completed (
        run_id TEXT PRIMARY KEY, close_evidence_sha256 TEXT NOT NULL,
        recorded_at REAL NOT NULL)''')
    db.execute('''CREATE TABLE IF NOT EXISTS exact_file_execution_observations (
        run_id TEXT PRIMARY KEY, request_id TEXT NOT NULL UNIQUE, binding_id TEXT NOT NULL,
        reservation_id TEXT NOT NULL, project_id TEXT NOT NULL, owner_session TEXT NOT NULL,
        host_id TEXT NOT NULL, thread_id TEXT NOT NULL, task_id TEXT NOT NULL,
        source_request_id TEXT NOT NULL, path TEXT NOT NULL, output TEXT NOT NULL,
        pid INTEGER NOT NULL, bytes INTEGER NOT NULL,
        sha256 TEXT NOT NULL, device INTEGER NOT NULL, inode INTEGER NOT NULL, mtime_ns INTEGER NOT NULL,
        output_device INTEGER NOT NULL, output_inode INTEGER NOT NULL, output_mtime_ns INTEGER NOT NULL,
        receipt_sha256 TEXT NOT NULL, journal_sha256 TEXT NOT NULL, log_sha256 TEXT NOT NULL,
        close_evidence_sha256 TEXT NOT NULL, recorded_at REAL NOT NULL)''')
    for table in ('exact_file_execution_intents', 'exact_file_execution_produced', 'exact_file_execution_completed', 'exact_file_execution_observations'):
        for action in ('UPDATE', 'DELETE'):
            db.execute(f'''CREATE TRIGGER IF NOT EXISTS {table}_no_{action.lower()}
                BEFORE {action} ON {table} BEGIN
                SELECT RAISE(ABORT,'exact file execution evidence is immutable'); END''')


def begin(db, run_id, reservation_id, bound):
    schema(db)
    generation = str(uuid.uuid4())
    digest = helper_sha256()
    db.execute('INSERT INTO exact_file_execution_intents VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
               (run_id, generation, bound['id'], reservation_id, bound['project_id'],
                bound['owner_session'], bound['host_id'], bound['thread_id'], bound['task_id'],
                bound['source_request_id'], bound['source_path'], bound['output_path'],
                bound['max_bytes'], digest, time.time()))
    return {'generation': generation, 'helper_sha256': digest}


def _hash(value):
    return isinstance(value, str) and len(value) == 64 and all(c in '0123456789abcdef' for c in value)


def report(coord, project, session, host, scope, path, p, *, complete):
    common = {'path', 'run_id', 'reservation_id', 'generation', 'token'}
    fields = set(OUTPUT) | {'pid', 'returncode', 'group_closed', 'close_evidence_sha256'} if complete else set(PRODUCED)
    if set(p) != common | fields:
        raise CoordError('invalid exact file execution report')
    for key in ('run_id', 'reservation_id', 'generation', 'token'):
        if not isinstance(p[key], str) or not p[key]:
            raise CoordError('exact file execution identity required')
    for key in fields - {'helper_sha256', 'sha256', 'close_evidence_sha256', 'group_closed'}:
        if type(p[key]) is not int or (p[key] < 0 and key not in ('mtime_ns', 'output_mtime_ns')):
            raise CoordError('invalid exact file execution measurement')
    if p['pid'] <= 0 or not _hash(p['sha256']):
        raise CoordError('invalid exact file execution PID or digest')
    if complete and (p['returncode'] != 0 or p['group_closed'] is not True or not _hash(p['close_evidence_sha256'])):
        raise CoordError('exact file execution requires successful scoped group closure')
    with coord.tx() as db:
        schema(db)
        intent = db.execute('SELECT * FROM exact_file_execution_intents WHERE run_id=?', (p['run_id'],)).fetchone()
        if not intent:
            raise CoordError('typed execution intent unavailable; do not recreate or replay the copy')
        expected = (project, session, host, scope['binding_id'], path, scope['output'], scope['max_bytes'], p['reservation_id'], p['generation'])
        actual = tuple(intent[k] for k in ('project_id', 'owner_session', 'host_id', 'binding_id', 'path', 'output', 'max_bytes', 'reservation_id', 'generation'))
        if actual != expected:
            raise CoordError('exact file execution tuple differs')
        bindings.authorize(db, project, session, host, path, scope['binding_id'],
                           output=scope['output'], max_bytes=scope['max_bytes'],
                           reservation_id=p['reservation_id'], active=False)
        current = coord._verify_owner(db, path, session, p['token'], True)
        run = db.execute('SELECT * FROM remote_guarded_runs WHERE run_id=?', (p['run_id'],)).fetchone()
        if (not run or current['reservation_id'] != p['reservation_id'] or
                (run['session_id'], run['host_id'], run['resource'], run['reservation_id'], run['local_pid']) !=
                (session, host, path, p['reservation_id'], p['pid']) or p['bytes'] > intent['max_bytes']):
            raise CoordError('exact file execution guard or byte limit differs')
        produced = db.execute('SELECT * FROM exact_file_execution_produced WHERE run_id=?', (p['run_id'],)).fetchone()
        if not complete:
            if p['helper_sha256'] != intent['helper_sha256']:
                raise CoordError('exact file helper generation differs')
            if produced:
                if any(produced[k] != p[k] for k in PRODUCED):
                    raise CoordError('exact file production retry conflicts')
            else:
                if run['closed_at'] is not None:
                    raise CoordError('cannot create historical production evidence after guard closure')
                bindings.authorize(db, project, session, host, path, scope['binding_id'],
                                   output=scope['output'], max_bytes=scope['max_bytes'],
                                   reservation_id=p['reservation_id'])
                db.execute('INSERT INTO exact_file_execution_produced VALUES('+','.join('?' for _ in range(12))+')',
                           (p['run_id'], *(p[k] for k in PRODUCED), time.time()))
        else:
            close_hash = hashlib.sha256((str(p['pid']) + ':0').encode()).hexdigest()
            if (run['closed_at'] is None or run['close_evidence_sha256'] != close_hash or
                    p['close_evidence_sha256'] != close_hash):
                raise CoordError('exact file execution requires acknowledged matching guard closure')
            if not produced or any(produced[k] != p[k] for k in OUTPUT + ('pid',)):
                raise CoordError('exact file completion differs from typed production evidence')
            prior = db.execute('SELECT * FROM exact_file_execution_completed WHERE run_id=?', (p['run_id'],)).fetchone()
            if prior and prior['close_evidence_sha256'] != close_hash:
                raise CoordError('exact file completion retry conflicts')
            if not prior:
                db.execute('INSERT INTO exact_file_execution_completed VALUES(?,?,?)', (p['run_id'], close_hash, time.time()))
        return read_run(db, intent)


def read_run(db, intent):
    produced = db.execute('SELECT * FROM exact_file_execution_produced WHERE run_id=?', (intent['run_id'],)).fetchone()
    completed = db.execute('SELECT * FROM exact_file_execution_completed WHERE run_id=?', (intent['run_id'],)).fetchone()
    result = dict(intent)
    result.update(state='completed' if completed else ('produced' if produced else 'pending'),
                  evidence_origin='scoped_host_report',
                  coverage='typed_snapshot_and_instrumented_process_group' if completed else 'typed_snapshot' if produced else 'admission_only',
                  native_completion_proven=False, global_effects_closed=False, custody_released=False,
                  recorded_at=completed['recorded_at'] if completed else None)
    if produced:
        result.update({k: produced[k] for k in PRODUCED})
        result['produced_recorded_at'] = produced['recorded_at']
    if completed:
        result['close_evidence_sha256'] = completed['close_evidence_sha256']
    return result


def result(db, reservation_id, binding_id):
    runs=[]
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='exact_file_execution_observations'").fetchone():
        runs.extend(public_observation(row) for row in db.execute('SELECT * FROM exact_file_execution_observations WHERE reservation_id=? AND binding_id=? ORDER BY recorded_at',
                            (reservation_id,binding_id)))
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='exact_file_execution_intents'").fetchone():
        runs.extend(read_run(db,row) for row in db.execute('SELECT * FROM exact_file_execution_intents WHERE reservation_id=? AND binding_id=? ORDER BY created_at',
                         (reservation_id, binding_id)))
    if not runs:
        return {'state': 'unknown', 'reason': 'no retained typed execution intent/report'}
    if len(runs) != 1:
        return {'state': 'ambiguous', 'runs': runs}
    return runs[0]


def _legacy_context(coord,db,project,session,host,scope,path,p):
    bound=bindings.authorize(db,project,session,host,path,scope['binding_id'],output=scope['output'],
                             max_bytes=scope['max_bytes'],reservation_id=p['reservation_id'],active=False)
    # Read-only reconciliation does not revive execution authority. Expiry and
    # task completion/blocking do not erase retained evidence. Explicit revoke,
    # changed owner/host/thread/authority or changed task/source still reject.
    if db.execute('SELECT 1 FROM exact_file_binding_revocations WHERE binding_id=?',(bound['id'],)).fetchone():
        raise CoordError('exact file binding is revoked; physical verification is not authorized')
    roles=bindings._roles(db)
    route=db.execute('SELECT thread_id,parent_session FROM bridge_bindings WHERE session_id=?',(session,)).fetchone()
    registered=db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?',(session,)).fetchone()
    task=db.execute('SELECT * FROM project_work WHERE id=? AND project_id=?',(bound['task_id'],project)).fetchone()
    source=db.execute('SELECT sender_session,recipient_session FROM messages WHERE id=?',(bound['source_request_id'],)).fetchone()
    if ((roles['session_id'],roles['root_session'])!=(bound['po_session'],bound['root_session']) or
            not bindings._managed(db,session,roles['session_id']) or not route or route['thread_id']!=bound['thread_id'] or
            route['parent_session'] is not None or not registered or registered['host_id']!=host or
            not task or (task['kind'],task['owner_session'],task['assigned_by'],task['source_request_id'])!=
            ('task',session,roles['session_id'],bound['source_request_id']) or not source or
            (source['sender_session'],source['recipient_session'])!=(roles['session_id'],session)):
        raise CoordError('legacy readback owner/host/thread/task/source authority changed')
    current=coord._verify_owner(db,path,session,p['token'],True)
    run=db.execute('SELECT * FROM remote_guarded_runs WHERE run_id=?',(p['run_id'],)).fetchone()
    if (not run or current['reservation_id']!=p['reservation_id'] or
            (run['session_id'],run['host_id'],run['resource'],run['reservation_id'])!=
            (session,host,path,p['reservation_id']) or run['local_pid']<=0 or run['closed_at'] is None):
        raise CoordError('legacy observation requires the exact retained closed run')
    close=hashlib.sha256((str(run['local_pid'])+':0').encode()).hexdigest()
    if run['close_evidence_sha256']!=close:
        raise CoordError('legacy run lacks matching reported successful closure')
    if db.execute('SELECT 1 FROM exact_file_execution_intents WHERE run_id=?',(p['run_id'],)).fetchone():
        raise CoordError('new typed run must use its own retained production/completion evidence')
    return bound,run


def verify_context(coord,project,session,host,scope,path,p):
    if set(p)!={'path','run_id','reservation_id','token'}:
        raise CoordError('invalid legacy verification context')
    with coord.tx() as db:
        schema(db)
        bound,run=_legacy_context(coord,db,project,session,host,scope,path,p)
        return dict(run_id=run['run_id'],reservation_id=run['reservation_id'],path=path,output=scope['output'],
                    max_bytes=scope['max_bytes'],pid=run['local_pid'],close_evidence_sha256=run['close_evidence_sha256'])


def _encoded(p,key):
    value=p[key]
    if not isinstance(value,str) or len(value)>1500000:raise CoordError('legacy evidence exceeds bound')
    try:data=base64.b64decode(value,validate=True)
    except ValueError as error:raise CoordError('legacy evidence is invalid base64') from error
    if not data or len(data)>1024*1024:raise CoordError('legacy evidence missing or too large')
    return data


def _jq_check(expression,data,args=()):
    try:checked=subprocess.run(['jq',*args,'-e',expression],input=data,capture_output=True,timeout=5)
    except (OSError,subprocess.SubprocessError) as error:raise CoordError('legacy evidence requires bounded jq validation') from error
    if checked.returncode:raise CoordError('legacy CLI/journal evidence does not match the retained run')


def reconcile(coord,project,session,host,scope,path,p):
    required={'path','run_id','reservation_id','token','request_id',
              'receipt_base64','journal_base64','log_base64','bytes','sha256','device','inode','mtime_ns',
              'output_device','output_inode','output_mtime_ns'}
    if set(p)!=required:raise CoordError('invalid legacy execution observation')
    try:
        if str(uuid.UUID(p['request_id']))!=p['request_id']:raise ValueError()
    except (TypeError,ValueError,AttributeError):raise CoordError('legacy observation request must be a canonical UUID')
    for key in ('bytes','device','inode','mtime_ns','output_device','output_inode','output_mtime_ns'):
        if type(p[key]) is not int or (p[key]<0 and key not in ('mtime_ns','output_mtime_ns')):
            raise CoordError('invalid legacy artifact observation')
    if not _hash(p['sha256']) or p['bytes']>scope['max_bytes']:raise CoordError('invalid legacy artifact hash or size')
    receipt,journal,log=(_encoded(p,key) for key in ('receipt_base64','journal_base64','log_base64'))
    # This is the fixed plain-text output of the historical helper, not JSON.
    line=re.fullmatch(rb'bytes=(\d+) sha256=([0-9a-f]{64}) device=(\d+) inode=(\d+) mtime_ns=(-?\d+) snapshot_stable=true\s*',log)
    if not line or (int(line[1]),line[2].decode(),int(line[3]),int(line[4]),int(line[5])) != tuple(p[k] for k in ('bytes','sha256','device','inode','mtime_ns')):
        raise CoordError('current source differs from retained helper observation')
    with coord.tx() as db:
        schema(db)
        bound,run=_legacy_context(coord,db,project,session,host,scope,path,p)
        args=('--arg','run',run['run_id'],'--arg','rid',run['reservation_id'],'--arg','session',session,
              '--arg','host',host,'--arg','project',project,'--arg','path',path,
              '--arg','close',run['close_evidence_sha256'],'--argjson','pid',str(run['local_pid']))
        _jq_check('type=="object" and .exit_code==0 and .run_id==$run and (.process_evidence_file|type=="string" and length>0)',receipt,args)
        _jq_check('''([to_entries[]|select(.value.event=="guard_call_succeeded" and .value.operation=="exact-file-begin" and .value.run_id==$run)|.key]) as $begin |
          ([to_entries[]|select(.value.event=="guard_call_succeeded" and .value.operation=="attach-guard-pid" and .value.run_id==$run)|.key]) as $attach |
          ([to_entries[]|select(.value.event=="gate_released" and .value.child_pid==$pid and .value.child_pgid==$pid)|.key]) as $gate |
          ([to_entries[]|select(.value.event=="leader_reaped" and .value.child_pid==$pid and .value.child_pgid==$pid and .value.returncode==0)|.key]) as $exit |
          ([to_entries[]|select(.value.event=="group_absent" and .value.child_pgid==$pid)|.key]) as $group |
          ([to_entries[]|select(.value.event=="guard_call_started" and .value.operation=="close-guard" and .value.run_id==$run and .value.evidence_sha256==$close)|.key]) as $close_start |
          ([to_entries[]|select(.value.event=="guard_call_succeeded" and .value.operation=="close-guard" and .value.run_id==$run)|.key]) as $close_end |
          length>0 and all(.[]; .session_id==$session and .host_id==$host and .project==$project and .coverage=="instrumented_process_group_only") and
          ([.[].sequence]==[range(0;length)]) and
          ([.[]|select(.event=="admitted")|.reservations]==[[{resource:$path,reservation_id:$rid,run_id:$run}]]) and
          all([$begin,$attach,$gate,$exit,$group,$close_start,$close_end][]; length==1) and
          $begin[0]<$attach[0] and $attach[0]<$gate[0] and $gate[0]<$exit[0] and $exit[0]<$group[0] and $group[0]<$close_start[0] and $close_start[0]<$close_end[0]''',journal,('-s',*args))
        columns=('run_id','request_id','binding_id','reservation_id','project_id','owner_session','host_id','thread_id','task_id','source_request_id','path','output','pid','bytes','sha256','device','inode','mtime_ns','output_device','output_inode','output_mtime_ns','receipt_sha256','journal_sha256','log_sha256','close_evidence_sha256')
        values=(run['run_id'],p['request_id'],bound['id'],run['reservation_id'],project,session,host,bound['thread_id'],bound['task_id'],bound['source_request_id'],path,scope['output'],run['local_pid'],*(p[k] for k in ('bytes','sha256','device','inode','mtime_ns','output_device','output_inode','output_mtime_ns')),*(hashlib.sha256(v).hexdigest() for v in (receipt,journal,log)),run['close_evidence_sha256'])
        prior=db.execute('SELECT * FROM exact_file_execution_observations WHERE run_id=? OR request_id=?',(run['run_id'],p['request_id'])).fetchone()
        if prior:
            if tuple(prior[k] for k in columns)!=values:raise CoordError('legacy observation retry conflicts')
            return public_observation(prior)
        db.execute('INSERT INTO exact_file_execution_observations VALUES('+','.join('?' for _ in range(26))+')',(*values,time.time()))
        return public_observation(db.execute('SELECT * FROM exact_file_execution_observations WHERE run_id=?',(run['run_id'],)).fetchone())


def public_observation(row):
    return dict(dict(row),state='observed',copy_observed=True,reported_executor_exit=0,
                evidence_origin='retained_host_reports_and_current_host_verification',
                coverage='retained_runner_journal_and_current_equal_artifacts',
                historical_helper_generation='unknown',native_completion_proven=False,
                global_effects_closed=False,custody_released=False)
