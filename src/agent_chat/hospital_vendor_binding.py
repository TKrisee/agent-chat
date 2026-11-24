"""Fixed Hospital vendor read-only binding; no World rows or native authority.

PRIVATE work in progress: NOT ready for installation or operational use.
"""
import hashlib
import math
import sqlite3
import time
from . import hospital_vendor_scope as scope
from . import capacity_admission as capacity
from .core import assert_agent_running

ROOT=capacity.ROOT_OPERATION_HOME_SESSION
ROOT_THREAD=capacity.ROOT_OPERATION_HOME_THREAD
HOST=scope.HOST
BINDING='hospital-vendor-readcopy-waKM-v1'
TERMINAL=('consumed','revoked','expired','failed_or_unknown')
class Refused(RuntimeError): pass

def _schema(db):
    db.execute('''CREATE TABLE IF NOT EXISTS hospital_vendor_binding(
      id TEXT PRIMARY KEY, task TEXT NOT NULL, consumer TEXT NOT NULL,
      host TEXT NOT NULL, manifest_sha256 TEXT NOT NULL, issued_at REAL NOT NULL,
      expires_at REAL NOT NULL, root TEXT NOT NULL)''')
    db.execute('''CREATE TABLE IF NOT EXISTS hospital_vendor_events(
      seq INTEGER PRIMARY KEY AUTOINCREMENT, binding_id TEXT NOT NULL,
      state TEXT NOT NULL CHECK(state IN ('pending','active','consumed','revoked','expired','failed_or_unknown')),
      at REAL NOT NULL, control_rid TEXT, detail TEXT NOT NULL)''')
    db.execute('''CREATE TABLE IF NOT EXISTS hospital_vendor_file_events(
      seq INTEGER PRIMARY KEY AUTOINCREMENT, binding_id TEXT NOT NULL,
      resource TEXT NOT NULL, operation_id TEXT NOT NULL,
      state TEXT NOT NULL CHECK(state IN ('started','unverified','failed_or_unknown')),
      at REAL NOT NULL, reported_digest TEXT, reported_byte_count INTEGER, run_id TEXT NOT NULL, output_run_id TEXT NOT NULL,
      output_resource TEXT NOT NULL, output_path TEXT NOT NULL)''')
    for table in ('hospital_vendor_binding','hospital_vendor_events','hospital_vendor_file_events'):
        for action in ('UPDATE','DELETE'):
            db.execute(f'''CREATE TRIGGER IF NOT EXISTS {table}_immutable_{action.lower()}
                BEFORE {action} ON {table} BEGIN SELECT RAISE(ABORT,'immutable hospital vendor evidence'); END''')
    _verify_schema(db)

def _verify_schema(db):
    expected={
      'hospital_vendor_binding':['id','task','consumer','host','manifest_sha256','issued_at','expires_at','root'],
      'hospital_vendor_events':['seq','binding_id','state','at','control_rid','detail'],
      'hospital_vendor_file_events':['seq','binding_id','resource','operation_id','state','at','reported_digest','reported_byte_count','run_id','output_run_id','output_resource','output_path']}
    for table,columns in expected.items():
        info=list(db.execute(f'PRAGMA table_info({table})'))
        if [r[1] for r in info] != columns or not info or info[0][5]!=1:
            raise Refused('binding_schema_changed')
        for row in info[1:]:
            if row[1] not in ('control_rid','reported_digest','reported_byte_count') and row[3]!=1:
                raise Refused('binding_schema_changed')
        triggers=db.execute("SELECT name,sql FROM sqlite_master WHERE type='trigger' AND tbl_name=?",(table,)).fetchall()
        expected_triggers={}
        for action in ('UPDATE','DELETE'):
            name=f'{table}_immutable_{action.lower()}'
            expected_triggers[name]=f"CREATE TRIGGER {name} BEFORE {action} ON {table} BEGIN SELECT RAISE(ABORT,'immutable hospital vendor evidence'); END"
        if {row[0]:' '.join(row[1].split()) for row in triggers} != expected_triggers:
            raise Refused('binding_triggers_changed')

def _state(db,binding_id=BINDING):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='hospital_vendor_binding'").fetchone(): return None,None
    _verify_schema(db)
    row=db.execute('SELECT * FROM hospital_vendor_binding WHERE id=?',(binding_id,)).fetchone()
    if not row: return None,None
    event=db.execute('SELECT * FROM hospital_vendor_events WHERE binding_id=? ORDER BY seq DESC LIMIT 1',(binding_id,)).fetchone()
    if not event: raise Refused('binding_history_missing')
    return row,event

def _check_fixed(row):
    if (row['id'],row['task'],row['consumer'],row['host'],row['manifest_sha256'],row['root']) != (BINDING,scope.TASK,scope.CONSUMER,HOST,scope.MANIFEST_SHA256,ROOT):
        raise Refused('binding_changed')
    if (type(row['expires_at']) not in (int,float) or not math.isfinite(row['expires_at'])
        or type(row['issued_at']) not in (int,float) or not math.isfinite(row['issued_at'])
        or not row['issued_at'] < row['expires_at'] <= scope.TASK_PRODUCTIVE_UNTIL):
        raise Refused('binding_clock_invalid')

def _task_and_scope(db,now,selection=None,*,require_fresh=True):
    if selection is None: scope.check_exact_task(db)
    else: scope.check_selected_task(db,task_id=selection['task'],intake_id=selection['intake'],essential_id=selection['essential'],body_sha256=selection['body_sha256'],intake_sha256=selection['intake_sha256'],essential_sha256=selection['essential_sha256'],started_at=selection['started_at'],expires_at=selection['expires_at'],now=now)
    capacity._verify_operator_source_db(db,capacity.ROOT_OPERATION_SOURCE_ID,ROOT)
    assert_agent_running(db,scope.CONSUMER)
    unseen=db.execute('''SELECT 1 FROM messages m WHERE m.recipient_session=?
      AND NOT EXISTS (SELECT 1 FROM message_reads r WHERE r.message_id=m.id
        AND r.reader_session=m.recipient_session AND r.provenance IN ('recipient_full_message','recipient_context_full'))
      AND NOT (m.acked_at IS NOT NULL AND m.acked_at <= COALESCE(CAST((SELECT value FROM meta WHERE key='message_delivery_archive_before') AS REAL),-1)) LIMIT 1''',(scope.CONSUMER,)).fetchone() if require_fresh else None
    if require_fresh and unseen: raise Refused('recipient_has_unconsumed_input')
    scope.check_current(db,project=scope.PROJECT,consumer=scope.CONSUMER,host=HOST,now=now)

def _check_registry(gate):
    if not isinstance(gate,capacity._RegistryGate): raise Refused('root_gate_missing')
    gate.check()
    root_scope=dict(home_project='default',home_session=ROOT,root_thread=ROOT_THREAD,host=HOST,
                    target_project='default',project_root_session=ROOT,project_orchestrator_session=scope.UNITY_PO)
    capacity._verify_registry_scope(gate.db,root_scope)

def _selection_schema(db):
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='hospital_vendor_selection'").fetchone():
        _verify_selection_schema(db); return
    db.execute('''CREATE TABLE IF NOT EXISTS hospital_vendor_selection(
      id TEXT PRIMARY KEY, task TEXT NOT NULL, intake TEXT NOT NULL, essential TEXT NOT NULL,
      body_sha256 TEXT NOT NULL, intake_sha256 TEXT NOT NULL, essential_sha256 TEXT NOT NULL,
      started_at REAL NOT NULL, expires_at REAL NOT NULL)''')
    for action in ('UPDATE','DELETE'):
        db.execute(f'''CREATE TRIGGER IF NOT EXISTS hospital_vendor_selection_immutable_{action.lower()}
            BEFORE {action} ON hospital_vendor_selection BEGIN SELECT RAISE(ABORT,'immutable hospital vendor selection'); END''')
    _verify_selection_schema(db)

def _verify_selection_schema(db):
    info=list(db.execute('PRAGMA table_info(hospital_vendor_selection)'))
    if ([row[1] for row in info]!=['id','task','intake','essential','body_sha256','intake_sha256','essential_sha256','started_at','expires_at']
        or not info or info[0][5]!=1 or any(row[3]!=1 for row in info[1:])):
        raise Refused('selection_schema_changed')
    actual=db.execute("SELECT name,sql FROM sqlite_master WHERE type='trigger' AND tbl_name='hospital_vendor_selection'").fetchall()
    expected={}
    for action in ('UPDATE','DELETE'):
        name=f'hospital_vendor_selection_immutable_{action.lower()}'
        expected[name]=f"CREATE TRIGGER {name} BEFORE {action} ON hospital_vendor_selection BEGIN SELECT RAISE(ABORT,'immutable hospital vendor selection'); END"
    if {row[0]:' '.join(row[1].split()) for row in actual}!=expected: raise Refused('selection_triggers_changed')

def _selected(db,binding_id):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='hospital_vendor_selection'").fetchone():
        raise Refused('selection_missing')
    _verify_selection_schema(db)
    row=db.execute('SELECT * FROM hospital_vendor_selection WHERE id=?',(binding_id,)).fetchone()
    if not row or not isinstance(row['task'],str) or row['id']!='hospital-vendor-task-'+hashlib.sha256(row['task'].encode()).hexdigest():
        raise Refused('selection_changed')
    return row

def _check_selection_binding(row,selection):
    if (row['id'],row['task'],row['consumer'],row['host'],row['manifest_sha256'],row['root'],row['expires_at']) != (
        selection['id'],selection['task'],scope.CONSUMER,HOST,scope.MANIFEST_SHA256,ROOT,selection['expires_at']):
        raise Refused('selection_binding_changed')
    if type(row['issued_at']) not in (int,float) or not math.isfinite(row['issued_at']) or row['issued_at']>=row['expires_at']:
        raise Refused('selection_binding_clock_invalid')

def _current_state(db,now):
    """Choose the unique unexpired selected binding; legacy stays separate."""
    selected=[]
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='hospital_vendor_selection'").fetchone():
        _verify_selection_schema(db)
        for item in db.execute('SELECT id FROM hospital_vendor_selection').fetchall():
            row,event=_state(db,item['id']); selection=_selected(db,item['id'])
            if not row: raise Refused('selection_binding_missing')
            _check_selection_binding(row,selection)
            if event['state'] in ('pending','active') and now<row['expires_at']: selected.append((row,event,selection))
    legacy,legacy_event=_state(db)
    if legacy and legacy_event['state'] in ('pending','active') and now<legacy['expires_at']:
        selected.append((legacy,legacy_event,None))
    if len(selected)>1: raise Refused('multiple_current_bindings')
    if selected: return selected[0]
    return (legacy,legacy_event,None)

def select_in_transaction(db,gate,params):
    """Root-only non-usable pending record for one explicitly selected task."""
    _check_registry(gate); now=time.time()
    task,intake,essential,digest,intake_digest,essential_digest,expires=(params.get(k) for k in ('task_id','intake_id','essential_id','body_sha256','intake_sha256','essential_sha256','expires_at'))
    if any(not isinstance(x,str) or not x.startswith('message_') or len(x)>160 for x in (task,intake,essential)):
        raise Refused('selection_invalid')
    if any(not isinstance(x,str) or len(x)!=64 or any(c not in '0123456789abcdef' for c in x) for x in (digest,intake_digest,essential_digest)): raise Refused('selection_invalid')
    if type(expires) not in (int,float) or not math.isfinite(expires) or now>=expires: raise Refused('selection_expired')
    intake_row=db.execute('SELECT created_at FROM messages WHERE id=?',(intake,)).fetchone()
    if not intake_row: raise Refused('selection_intake_missing')
    started=intake_row[0]
    scope.check_selected_task(db,task_id=task,intake_id=intake,essential_id=essential,body_sha256=digest,intake_sha256=intake_digest,essential_sha256=essential_digest,started_at=started,expires_at=expires,now=now)
    if started>now: raise Refused('selection_clock_invalid')
    names=('hospital_vendor_binding','hospital_vendor_events','hospital_vendor_file_events')
    present={row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN (?,?,?)",names)}
    if present and len(present)!=len(names): raise Refused('binding_schema_partial')
    if present: _verify_schema(db)
    else: _schema(db)
    _selection_schema(db)
    current,event,_=_current_state(db,now)
    if current and event['state'] in ('pending','active') and now<current['expires_at']: raise Refused('current_binding_exists')
    binding_id='hospital-vendor-task-'+hashlib.sha256(task.encode()).hexdigest()
    if _state(db,binding_id)[0] is not None or db.execute('SELECT 1 FROM hospital_vendor_selection WHERE id=?',(binding_id,)).fetchone():
        raise Refused('selection_already_exists')
    selection={'id':binding_id,'task':task,'intake':intake,'essential':essential,'body_sha256':digest,'intake_sha256':intake_digest,'essential_sha256':essential_digest,'started_at':started,'expires_at':expires}
    _task_and_scope(db,now,selection,require_fresh=False)
    if time.time()>=expires: raise Refused('selection_expired')
    db.execute('INSERT INTO hospital_vendor_selection VALUES(?,?,?,?,?,?,?,?,?)',(binding_id,task,intake,essential,digest,intake_digest,essential_digest,started,expires))
    db.execute('INSERT INTO hospital_vendor_binding VALUES(?,?,?,?,?,?,?,?)',(binding_id,task,scope.CONSUMER,HOST,scope.MANIFEST_SHA256,now,expires,ROOT))
    db.execute('INSERT INTO hospital_vendor_events(binding_id,state,at,control_rid,detail) VALUES(?,?,?,?,?)',(binding_id,'pending',now,None,'root_selected_task'))
    gate.check()
    if time.time()>=expires: raise Refused('selection_expired')
    return {'binding_id':binding_id,'state':'pending','expires_at':expires}

def issue_in_transaction(db, gate):
    """Configured Root caller only; caller MUST hold registry-first authority gate.

    Caller owns target transaction and MUST call gate.check again before commit.
    """
    _check_registry(gate)
    now=time.time()
    if now >= scope.TASK_PRODUCTIVE_UNTIL: raise Refused('task_expired')
    _schema(db)
    if _state(db)[0] is not None: raise Refused('binding_already_exists')
    _task_and_scope(db,now)
    db.execute('INSERT INTO hospital_vendor_binding VALUES(?,?,?,?,?,?,?,?)',
               (BINDING,scope.TASK,scope.CONSUMER,HOST,scope.MANIFEST_SHA256,now,scope.TASK_PRODUCTIVE_UNTIL,ROOT))
    db.execute('INSERT INTO hospital_vendor_events(binding_id,state,at,control_rid,detail) VALUES(?,?,?,?,?)',
               (BINDING,'pending',now,None,'root_configured_issue'))
    gate.check()
    return {'binding_id':BINDING,'state':'pending','expires_at':scope.TASK_PRODUCTIVE_UNTIL}

def authorize_control_in_transaction(db,session,host,gate):
    _check_registry(gate)
    now=time.time(); row,event,selection=_current_state(db,now)
    if not row: raise Refused('binding_missing')
    if selection is None: _check_fixed(row)
    else: _check_selection_binding(row,selection)
    if session!=scope.CONSUMER or host!=HOST or event['state']!='pending' or now>=row['expires_at']:
        raise Refused('control_not_authorized')
    _task_and_scope(db,now,selection)
    if selection is not None:
        for table in ('guarded_runs','remote_guarded_runs'):
            if (not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(table,)).fetchone()
                or db.execute(f'SELECT 1 FROM {table} WHERE resource=? AND closed_at IS NULL LIMIT 1',(scope.RUN_RESOURCE,)).fetchone()):
                raise Refused('prior_copy_guard_open')
    if time.time()>=row['expires_at']: raise Refused('control_not_authorized')
    return row['id']

def activate_control_in_transaction(db,session,host,acquired,gate):
    binding_id=authorize_control_in_transaction(db,session,host,gate)
    if acquired['name']!=scope.CONTROL or acquired['owner_session']!=session:
        raise Refused('control_tuple_changed')
    db.execute('INSERT INTO hospital_vendor_events(binding_id,state,at,control_rid,detail) VALUES(?,?,?,?,?)',
               (binding_id,'active',time.time(),acquired['reservation_id'],'control_acquired'))
    deadline=db.execute('SELECT expires_at FROM hospital_vendor_binding WHERE id=?',(binding_id,)).fetchone()[0]
    if time.time()>=deadline: raise Refused('control_not_authorized')

def validate_in_transaction(db,session,host,control_rid,token,resource,gate):
    _check_registry(gate)
    now=time.time(); row,event,selection=_current_state(db,now)
    if not row: raise Refused('binding_missing')
    if selection is None: _check_fixed(row)
    else: _check_selection_binding(row,selection)
    if session!=scope.CONSUMER or host!=HOST or event['state']!='active' or now>=row['expires_at']:
        raise Refused('binding_inactive')
    if event['control_rid'] != control_rid: raise Refused('control_changed')
    _task_and_scope(db,now,selection)
    scope.check_owner_capability(db,bound_resource=scope.CONTROL,bound_rid=control_rid,supplied_token=token,now=now)
    deadline=db.execute('SELECT deadline FROM resources WHERE name=?',(scope.CONTROL,)).fetchone()[0]
    if deadline-now<=2700: raise Refused('cleanup_reserve_unavailable')
    if resource not in scope.manifest(): raise Refused('resource_not_selected')
    if time.time()>=row['expires_at'] or deadline-time.time()<=2700: raise Refused('binding_inactive')
    return {'binding_id':row['id'],'resource':resource,'expires_at':row['expires_at'],'state':'active'}

def start_file_in_transaction(db,session,host,control_rid,token,resource,run_id,output_run_id,output_resource,output_path,gate):
    import uuid
    binding_id=validate_in_transaction(db,session,host,control_rid,token,resource,gate)['binding_id']
    prior=db.execute('SELECT state FROM hospital_vendor_file_events WHERE binding_id=? AND resource=? ORDER BY seq DESC LIMIT 1',(binding_id,resource)).fetchone()
    if prior: raise Refused('file_already_started')
    running=db.execute('''SELECT e.resource FROM hospital_vendor_file_events e
      WHERE e.binding_id=? AND e.seq=(SELECT MAX(x.seq) FROM hospital_vendor_file_events x
      WHERE x.binding_id=e.binding_id AND x.resource=e.resource) AND e.state='started' LIMIT 1''',(binding_id,)).fetchone()
    if running: raise Refused('other_file_unresolved')
    op='vendor-copy-'+uuid.uuid4().hex
    db.execute('INSERT INTO hospital_vendor_file_events(binding_id,resource,operation_id,state,at,run_id,output_run_id,output_resource,output_path) VALUES(?,?,?,?,?,?,?,?,?)',
               (binding_id,resource,op,'started',time.time(),run_id,output_run_id,output_resource,output_path))
    gate.check()
    return {'operation_id':op,'resource':resource,'state':'started'}

def pulse_file_in_transaction(db,session,host,control_rid,token,resource,run_id,output_run_id,output_resource,output_path,operation_id,gate):
    result=validate_in_transaction(db,session,host,control_rid,token,resource,gate)
    row=db.execute('SELECT operation_id,state,run_id,output_run_id,output_resource,output_path FROM hospital_vendor_file_events WHERE binding_id=? AND resource=? ORDER BY seq DESC LIMIT 1',(result['binding_id'],resource)).fetchone()
    if not row or tuple(row)!=(operation_id,'started',run_id,output_run_id,output_resource,output_path): raise Refused('file_operation_changed')
    gate.check()
    return dict(result,operation_id=operation_id)

def finish_file_in_transaction(db,session,host,control_rid,token,resource,run_id,output_run_id,output_resource,output_path,operation_id,digest,byte_count,gate):
    binding_id=pulse_file_in_transaction(db,session,host,control_rid,token,resource,run_id,output_run_id,output_resource,output_path,operation_id,gate)['binding_id']
    if (not isinstance(digest,str) or len(digest)!=64 or any(c not in '0123456789abcdef' for c in digest)
        or type(byte_count) is not int or not 0<=byte_count<=9223372036854775807):
        raise Refused('file_result_invalid')
    db.execute('INSERT INTO hospital_vendor_file_events(binding_id,resource,operation_id,state,at,reported_digest,reported_byte_count,run_id,output_run_id,output_resource,output_path) VALUES(?,?,?,?,?,?,?,?,?,?,?)',
               (binding_id,resource,operation_id,'unverified',time.time(),digest,byte_count,run_id,output_run_id,output_resource,output_path))
    reported=db.execute("SELECT COUNT(DISTINCT resource) FROM hospital_vendor_file_events WHERE binding_id=? AND state='unverified'",(binding_id,)).fetchone()[0]
    gate.check()
    return {'binding_id':binding_id,'resource':resource,'state':'unverified','reported':reported}

def revoke_in_transaction(db,gate,reason,binding_id=BINDING):
    _check_registry(gate)
    if reason not in ('root_revoked','failed_or_unknown'): raise Refused('invalid_terminal_reason')
    row,event=_state(db,binding_id)
    if not row: raise Refused('binding_missing')
    if binding_id==BINDING: _check_fixed(row)
    else: _check_selection_binding(row,_selected(db,binding_id))
    if binding_id!=BINDING and time.time()>=row['expires_at']:
        return root_status_in_transaction(db,gate,binding_id)
    if event['state'] not in ('pending','active'): raise Refused('binding_not_revocable')
    unfinished=db.execute('''SELECT 1 FROM hospital_vendor_file_events e WHERE e.binding_id=?
       AND e.seq=(SELECT MAX(x.seq) FROM hospital_vendor_file_events x WHERE x.binding_id=e.binding_id AND x.resource=e.resource)
       AND e.state IN ('started','unverified') LIMIT 1''',(binding_id,)).fetchone()
    state='revoked' if reason=='root_revoked' and not unfinished else 'failed_or_unknown'
    db.execute('INSERT INTO hospital_vendor_events(binding_id,state,at,control_rid,detail) VALUES(?,?,?,?,?)',
               (binding_id,state,time.time(),event['control_rid'],reason))
    gate.check()
    return {'binding_id':binding_id,'state':state}

def root_status_in_transaction(db,gate,binding_id=BINDING):
    _check_registry(gate)
    row,event=_state(db,binding_id)
    if not row: return {'binding_id':binding_id,'state':'absent'}
    if binding_id==BINDING: _check_fixed(row)
    else: _check_selection_binding(row,_selected(db,binding_id))
    unfinished=db.execute('''SELECT resource,operation_id,state,run_id,output_run_id,output_resource,output_path FROM hospital_vendor_file_events e
       WHERE e.binding_id=? AND e.seq=(SELECT MAX(x.seq) FROM hospital_vendor_file_events x
       WHERE x.binding_id=e.binding_id AND x.resource=e.resource) AND e.state IN ('started','unverified') ''',(binding_id,)).fetchall()
    if event['state'] in ('pending','active') and time.time()>=row['expires_at']:
        state='failed_or_unknown' if unfinished else 'expired'
        db.execute('INSERT INTO hospital_vendor_events(binding_id,state,at,control_rid,detail) VALUES(?,?,?,?,?)',
                   (binding_id,state,time.time(),event['control_rid'],'deadline_passed'))
        row,event=_state(db,binding_id)
    gate.check()
    return {'binding_id':binding_id,'state':event['state'],'expires_at':row['expires_at'],
            'unresolved_files':[dict(item) for item in unfinished]}

def diagnose_in_transaction(db,gate):
    """Observe the real issue predicates without issuing or mutating a binding.

    This is a current, locked observation, never a reservation or historical
    explanation.  When the binding schema is absent it cannot prove DDL or
    writeability; an actual later issue must repeat every check.
    """
    try: return _diagnose_in_transaction(db,gate)
    except (sqlite3.Error,OSError):
        return {'binding_id':BINDING,'state':'unknown','stage':'observation',
                'reason':'observation_unavailable','issue_attempted':False,
                'historical_cause':'unknown','issue_readiness':'not_established',
                'schema_writeability':'not_checked'}

def _diagnose_in_transaction(db,gate):
    from .core import CoordError
    result={'binding_id':BINDING,'state':'observed','issue_attempted':False,
            'historical_cause':'unknown','issue_readiness':'not_established',
            'schema_writeability':'not_checked'}
    def blocked(stage,reason):
        return dict(result,stage=stage,reason=reason)
    try: capacity._verify_operator_source_db(db,capacity.ROOT_OPERATION_SOURCE_ID,ROOT)
    except capacity.AdmissionRefused: return blocked('operator_authority','operator_authority_refused')
    try: _check_registry(gate)
    except (capacity.AdmissionRefused,Refused): return blocked('registry','registry_refused')
    now=time.time()
    result['observed_at']=now
    if now>=scope.TASK_PRODUCTIVE_UNTIL: return blocked('task_deadline','task_expired')
    names=('hospital_vendor_binding','hospital_vendor_events','hospital_vendor_file_events')
    present={row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN (?,?,?)",names)}
    if present and len(present)!=len(names): return dict(result,state='unknown',stage='binding_schema',reason='schema_partial_unverified')
    if present:
        try: row,_=_state(db)
        except Refused:
            # Issue runs CREATE IF NOT EXISTS before this validation; a missing
            # trigger may be repairable, so observation alone cannot assert
            # that schema would fail on the mutating issue path.
            return dict(result,state='unknown',stage='binding_schema',reason='schema_requires_issue_validation')
        if row is not None: return blocked('binding','binding_already_exists')
        result['schema']='existing_checked'
    else: return dict(result,state='unknown',stage='binding_schema',reason='schema_absent_unavailable')
    try: _task_and_scope(db,now)
    except scope.Refused as error:
        safe={'manifest_changed','manifest_invalid','manifest_count','caller_mismatch','host_mismatch',
              'extra_or_missing_claim','claim_changed','claim_not_stale','task_tuple_invalid',
              'task_changed','task_receipt_missing','task_intake_time_changed','essential_task_association_missing'}
        return blocked('task_or_cohort',str(error) if str(error) in safe else 'task_or_cohort_refused')
    except Refused as error:
        return blocked('task_or_cohort','recipient_has_unconsumed_input' if str(error)=='recipient_has_unconsumed_input' else 'task_or_cohort_refused')
    except capacity.AdmissionRefused: return blocked('task_or_cohort','operator_authority_refused')
    except CoordError: return blocked('task_or_cohort','agent_admission_refused')
    gate.check()
    return dict(result,stage='observed_predicates',reason='no_observed_refusal',issue_readiness='not_established')

def root_action(master,action,params=None):
    """Same-UID Root adapter calls only after configured Root preflight."""
    from pathlib import Path
    from . import root_local
    from .service import _installed_server_config, configured_server_database
    import os
    config=_installed_server_config()
    if os.getuid()!=config.uid or os.geteuid()!=config.uid:
        raise Refused('root_uid_mismatch')
    canonical=configured_server_database(config.plist)
    if Path(master).resolve()!=canonical or canonical!=config.master.resolve():
        raise Refused('root_master_mismatch')
    checked=root_local._file(canonical,config.uid); info=checked.lstat()
    identity=(info.st_dev,info.st_ino,info.st_uid,info.st_mode)
    registry=root_local._file(Path(str(canonical)+'.projects.sqlite3'),config.uid)
    st=registry.lstat(); registry_identity=(st.st_dev,st.st_ino,st.st_uid,st.st_mode)
    with capacity.registry_authority_gate(registry,config.uid,registry_identity) as (gate,check):
        db=sqlite3.connect(canonical.as_uri()+('?mode=ro' if action=='diagnose' else '?mode=rw'),uri=True,isolation_level=None,timeout=3)
        db.row_factory=sqlite3.Row
        try:
            db.execute('BEGIN' if action=='diagnose' else 'BEGIN IMMEDIATE')
            if action=='diagnose':
                result=diagnose_in_transaction(db,gate)
                check(); current=root_local._file(canonical,config.uid).lstat()
                if (current.st_dev,current.st_ino,current.st_uid,current.st_mode)!=identity: raise Refused('master_changed')
                db.rollback(); return result
            capacity._verify_operator_source_db(db,capacity.ROOT_OPERATION_SOURCE_ID,ROOT)
            if action=='issue': result=issue_in_transaction(db,gate)
            elif action=='select': result=select_in_transaction(db,gate,params or {})
            elif action in ('selection-status','selection-revoke','selection-unknown'):
                binding_id=(params or {}).get('binding_id')
                prefix='hospital-vendor-task-'
                if (not isinstance(binding_id,str) or not binding_id.startswith(prefix) or len(binding_id)!=len(prefix)+64
                    or any(c not in '0123456789abcdef' for c in binding_id[len(prefix):])):
                    raise Refused('selection_id_invalid')
                if action=='selection-status': result=root_status_in_transaction(db,gate,binding_id)
                else: result=revoke_in_transaction(db,gate,'root_revoked' if action=='selection-revoke' else 'failed_or_unknown',binding_id)
            elif action=='status': result=root_status_in_transaction(db,gate)
            elif action=='revoke': result=revoke_in_transaction(db,gate,'root_revoked')
            elif action=='unknown': result=revoke_in_transaction(db,gate,'failed_or_unknown')
            else: raise Refused('root_action_invalid')
            check(); current=root_local._file(canonical,config.uid).lstat()
            if (current.st_dev,current.st_ino,current.st_uid,current.st_mode)!=identity:
                raise Refused('master_changed')
            db.commit(); return result
        finally:
            if db.in_transaction: db.rollback()
            db.close()
