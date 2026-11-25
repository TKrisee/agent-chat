"""Configured-machine diagnostic projection. Never preflight clearance."""
from contextlib import ExitStack, contextmanager
import math
import sqlite3
import time
from .core import CoordError

BOUND = 64
REQUEST = ('id','home_project','home_session','host_id','old_thread_id','status',
           'new_thread_id','queue_id','created_at','updated_at')
AUDITS = {
 'root_fresh_pending_cancellations': ('cancellation_id','request_id','home_project','home_session','host_id','expected_thread','candidate_thread_id','outcome','state','created_at','updated_at'),
 'root_fresh_retirements': ('retirement_id','request_id','home_project','home_session','host_id','expected_thread','reconciliation_id','previous_candidate_id','replacement_candidate_id','outcome','state','created_at','updated_at'),
 'root_fresh_settings_retirements': ('retirement_id','request_id','home_project','home_session','host_id','expected_thread','candidate_thread_id','outcome','state','created_at','updated_at'),
}

@contextmanager
def _read(path):
    from .root_local import _read as existing_read
    with existing_read(path) as db:
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        yield db

def _has(db,table):
    return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(table,)).fetchone())

def _columns(db,table,columns):
    if not _has(db,table) or not set(columns) <= {r['name'] for r in db.execute('PRAGMA table_info('+table+')')}:
        raise CoordError('pending-fresh required metadata schema is unavailable: '+table)

def _typed(row,columns):
    output={}
    for key in columns:
        value=row[key]
        if key in ('created_at','updated_at'):
            if isinstance(value,bool) or not isinstance(value,(int,float)) or not math.isfinite(value):
                raise CoordError('pending-fresh timestamp metadata is malformed')
        elif value is not None and (not isinstance(value,str) or not value or len(value.encode('utf-8'))>512
                                    or any(ord(c)<32 or ord(c)==127 for c in value)):
            raise CoordError('pending-fresh identifier metadata is malformed')
        output[key]=value
    return output

def _requests(db):
    _columns(db,'root_fresh_requests',REQUEST)
    return db.execute('SELECT '+','.join(REQUEST)+" FROM root_fresh_requests WHERE status<>'completed' ORDER BY created_at,id LIMIT 65").fetchall()

def _audits(db,request):
    result={}
    for table,columns in AUDITS.items():
        if not _has(db,table):
            result[table]={'present':None,'evidence_state':'UNAVAILABLE'}
            continue
        _columns(db,table,columns)
        rows=db.execute('SELECT '+','.join(columns)+' FROM '+table+' WHERE request_id=? LIMIT 2',(request['id'],)).fetchall()
        if len(rows)>1:
            result[table]={'present':None,'evidence_state':'UNKNOWN','reason':'ambiguous_audit_association'}
            continue
        if not rows:
            result[table]={'present':False}
            continue
        audit=rows[0]
        matches=(audit['request_id']==request['id'] and
            all(audit[k]==request[k] for k in ('home_project','home_session','host_id')) and
            audit['expected_thread']==request['old_thread_id'])
        if not matches:
            result[table]={'present':None,'evidence_state':'UNKNOWN',
                           'reason':'audit_request_identity_mismatch'}
            continue
        item=_typed(audit,columns)
        item['present']=True
        item['matches_request_identity']=True
        result[table]=item
    return result

def _scopes(db,request_id):
    columns=('request_id','project_id','root_session')
    _columns(db,'root_fresh_projects',columns)
    return db.execute('SELECT request_id,project_id,root_session FROM root_fresh_projects WHERE request_id=? ORDER BY project_id,root_session LIMIT 65',(request_id,)).fetchall()

def _scope_evidence(db,scope,request,root):
    tables={'root_fresh_gates':('request_id','root_session','old_thread_id','new_thread_id','active'),
            'bridge_bindings':('session_id','thread_id','parent_session'),
            'root_projections':('session_id','home_project','home_session','thread_id'),
            'remote_session_hosts':('session_id','host_id'),
            'sessions':('id',)}
    for table,columns in tables.items(): _columns(db,table,columns)
    gate=db.execute('SELECT request_id,root_session,old_thread_id,new_thread_id,active FROM root_fresh_gates WHERE request_id=?',(request['id'],)).fetchone()
    binding=db.execute('SELECT session_id,thread_id,parent_session FROM bridge_bindings WHERE session_id=?',(scope['root_session'],)).fetchone()
    projection=db.execute('SELECT session_id,home_project,home_session,thread_id FROM root_projections WHERE session_id=?',(scope['root_session'],)).fetchone()
    host=db.execute('SELECT session_id,host_id FROM remote_session_hosts WHERE session_id=?',(scope['root_session'],)).fetchone()
    registered=db.execute('SELECT 1 FROM sessions WHERE id=?',(scope['root_session'],)).fetchone()
    if gate and (type(gate['active']) is not int or gate['active'] not in (0,1)):
        raise CoordError('pending-fresh gate state is malformed')
    result={'project_id':scope['project_id'],'root_session':scope['root_session'],
            'gate':None if gate is None else {**_typed(gate,('request_id','root_session','old_thread_id','new_thread_id')),'active':gate['active']},
            'binding':None if binding is None else _typed(binding,tables['bridge_bindings']),
            'projection':None if projection is None else _typed(projection,tables['root_projections']),
            'host':None if host is None else _typed(host,tables['remote_session_hosts']),'registered':bool(registered)}
    result['current_binding_matches']=bool(registered and binding and not binding['parent_session'] and projection and host and
        binding['thread_id']==projection['thread_id']==root['thread_id'] and
        (projection['home_project'],projection['home_session'])==(root['home_project'],root['home_session']) and
        host['host_id']==root['host_id'])
    result['request_gate_relation_matches']=bool(gate and gate['root_session']==scope['root_session'] and
        gate['old_thread_id']==request['old_thread_id'])
    result['evidence_state']='OBSERVED' if result['current_binding_matches'] and result['request_gate_relation_matches'] else 'UNKNOWN'
    return result

def _project_pending(master,home_project,session,host_id,expected_thread):
    """Called only after maintained root_local configured-provider preflight."""
    from pathlib import Path
    from .root_local import _file,_target
    owner=master.lstat().st_uid
    registry=_file(Path(str(master)+'.projects.sqlite3'),owner)
    with ExitStack() as stack:
        db=stack.enter_context(_read(registry))
        root_columns=('home_project','home_session','host_id','thread_id','mode')
        _columns(db,'root_config',root_columns)
        row=db.execute('SELECT '+','.join(root_columns)+' FROM root_config WHERE singleton=1').fetchone()
        if not row: raise CoordError('pending-fresh current Root binding is unavailable')
        root=_typed(row,root_columns)
        if tuple(root[k] for k in ('home_project','home_session','host_id','thread_id'))!=(home_project,session,host_id,expected_thread):
            raise CoordError('pending-fresh current Root binding changed')
        home=stack.enter_context(_read(_target(master,registry,home_project,owner)))
        for table,columns in (('sessions',('id',)),('bridge_bindings',('session_id','thread_id','parent_session')),
                              ('remote_session_hosts',('session_id','host_id')),
                              ('root_projections',('session_id','home_project','home_session','thread_id'))):
            _columns(home,table,columns)
        binding=home.execute('SELECT thread_id,parent_session FROM bridge_bindings WHERE session_id=?',(session,)).fetchone()
        host=home.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?',(session,)).fetchone()
        projection=home.execute('SELECT home_project,home_session,thread_id FROM root_projections WHERE session_id=?',(session,)).fetchone()
        if (not home.execute('SELECT 1 FROM sessions WHERE id=?',(session,)).fetchone() or not binding or not host or not projection
                or binding['parent_session'] or binding['thread_id']!=expected_thread or host['host_id']!=host_id
                or tuple(projection)!=(home_project,session,expected_thread)):
            raise CoordError('pending-fresh current home binding changed or unavailable')
        root['home_binding_observed']=True
        rows=_requests(db)
        if len(rows)>BOUND: raise CoordError('pending-fresh request bound exceeded; no partial projection')
        requests=[]; scope_count=0; readers={home_project:home}; complete=True; seen=set()
        for row in rows:
            request=_typed(row,REQUEST)
            if any(request[k] is None for k in ('id','home_project','home_session','host_id','old_thread_id','status')):
                raise CoordError('pending-fresh required request identity is malformed')
            if request['id'] in seen: raise CoordError('pending-fresh duplicate request association')
            seen.add(request['id'])
            request['matches_current_root']=all(request[k]==root[k] for k in ('home_project','home_session','host_id'))
            request['old_thread_matches_current']=request['old_thread_id']==root['thread_id']
            request['audits']=_audits(db,request)
            scopes=_scopes(db,request['id'])
            scope_count+=len(scopes)
            if scope_count>BOUND: raise CoordError('pending-fresh total scope bound exceeded; no partial projection')
            request['scopes']=[]
            seen_projects=set()
            if not scopes: complete=False
            for scope_row in scopes:
                scope=_typed(scope_row,('request_id','project_id','root_session'))
                if scope['request_id']!=request['id'] or not scope['project_id'] or not scope['root_session']:
                    raise CoordError('pending-fresh scope association is malformed')
                if scope['project_id'] in seen_projects: raise CoordError('pending-fresh duplicate request scope')
                seen_projects.add(scope['project_id'])
                if not request['matches_current_root']:
                    item={**scope,'evidence_state':'UNKNOWN','reason':'foreign_request_binding_not_opened'}
                else:
                    project=scope['project_id']
                    if project not in readers:
                        readers[project]=stack.enter_context(_read(_target(master,registry,project,owner)))
                    item=_scope_evidence(readers[project],scope,request,root)
                if item['evidence_state']!='OBSERVED': complete=False
                request['scopes'].append(item)
            if sum(bool(a.get('present')) for a in request['audits'].values())>1: complete=False
            if any(a.get('evidence_state') in ('UNAVAILABLE','UNKNOWN') or (a.get('present') and not a['matches_request_identity']) for a in request['audits'].values()): complete=False
            request['native_closure']='UNKNOWN'
            request['terminal_or_active_disposition']='UNASSESSED'
            requests.append(request)
        return {'schema':'configured-root-pending-fresh-v1','predicate':"status <> 'completed'",'observed_at':time.time(),
                'current_root':root,'requests':requests,'request_count':len(requests),'scope_count':scope_count,
                'complete':complete,'evidence_state':'OBSERVED' if complete else 'UNKNOWN',
                'pending_predicate_matches':bool(rows),'preflight_clearance':False,'native_closure':'UNKNOWN','cross_store_atomic':False}


def project_pending(master,home_project,session,host_id,expected_thread):
    try:
        return _project_pending(master,home_project,session,host_id,expected_thread)
    except sqlite3.Error as error:
        raise CoordError('pending-fresh metadata snapshot is unavailable; no clearance') from error


def released_attachment(db, root):
    from .root_admin_attachment_members import table
    return table(db, 'root_admin_attachment_releases') and bool(db.execute('''SELECT 1
        FROM root_admin_attachment_requests a
        JOIN root_admin_attachment_releases r ON r.request_id=a.request_id
        WHERE a.state='attached' AND a.home_project=? AND a.home_session=? AND a.host_id=? AND a.new_thread=?''',
        (root['home_project'], root['home_session'], root['host_id'], root['thread_id'])).fetchone())


def routing_fresh_blocker(db, master, root, *, terminal_only=False):
    """Classify exact terminal evidence; never infer native closure or clear unknown rows."""
    if released_attachment(db, root):
        # The completed attachment made the former Root thread historical.
        # Only a pending request involving the current binding can affect it.
        if not terminal_only and db.execute('''SELECT 1 FROM root_fresh_requests
                WHERE status NOT IN ('completed','retired','cancelled')
                  AND (old_thread_id=? OR new_thread_id=?) LIMIT 1''',
                (root['thread_id'], root['thread_id'])).fetchone():
            return 'root fresh-start is pending; inspect root fresh-status'
        return None
    predicate = "status IN ('retired','cancelled')" if terminal_only else "status<>'completed'"
    rows=db.execute("SELECT id,status,home_project,home_session,host_id,old_thread_id,new_thread_id,queue_id,native_action,native_proof_json,response_proof_json,owner_proof_json,error FROM root_fresh_requests WHERE "+predicate+" ORDER BY id LIMIT 65").fetchall()
    if not rows: return None
    if len(rows)>64: return 'root fresh-start eligibility is unresolved; inspect root-local pending-fresh'
    if any(row['status'] not in ('retired','cancelled') for row in rows):
        return 'root fresh-start is pending; inspect root fresh-status'
    try:
        snapshot=project_pending(master,root['home_project'],root['home_session'],root['host_id'],root['thread_id'])
    except (CoordError, OSError):
        return 'root fresh-start terminal eligibility is unresolved; inspect root-local pending-fresh'
    entries=[entry for entry in snapshot['requests'] if not terminal_only or entry['status'] in ('retired','cancelled')]
    if len(entries)!=len(rows):
        return 'root fresh-start terminal eligibility is unresolved; inspect root-local pending-fresh'
    by_id={entry['id']:entry for entry in entries}
    for row in rows:
        entry=by_id.get(row['id'])
        if not entry or any(entry[k]!=row[k] for k in ('id','status','home_project','home_session','host_id','old_thread_id','new_thread_id','queue_id')):
            return 'root fresh-start terminal eligibility is unresolved; inspect root-local pending-fresh'
        audits=entry['audits']; cancel=audits['root_fresh_pending_cancellations']
        if (row['id']!='876fd5a8-8966-411e-8236-47df1fc8bf99' or row['status']!='retired'
            or not entry['matches_current_root'] or not entry['old_thread_matches_current'] or row['queue_id'] is not None
            or cancel.get('present') is not True or cancel.get('matches_request_identity') is not True or cancel.get('state')!='cancelled'
            or cancel.get('cancellation_id')!='e7e2075c-4ab8-4791-a952-e3321c505c03'
            or row['new_thread_id']!='01a148f4-3358-79b3-b487-4210622bf57b'
            or row['native_action']!='creating'
            or any(row[k] is not None for k in ('native_proof_json','response_proof_json','owner_proof_json'))
            or row['error']!=('Candidate recovery required: exact retained thread is not materialized: RPC error -32600: no rollout found for thread id '+row['new_thread_id'])
            or cancel.get('candidate_thread_id')!=row['new_thread_id']
            or cancel.get('outcome')!='candidate_not_materialized_native_outcome_unknown'
            or any(audits[name].get('present') is not False for name in ('root_fresh_retirements','root_fresh_settings_retirements'))
            or not entry['scopes'] or any(scope['evidence_state']!='OBSERVED' or not scope['gate']
                    or scope['gate']['active']!=0 or scope['gate']['new_thread_id'] is not None
                    for scope in entry['scopes'])):
            return 'root fresh-start terminal eligibility is unresolved; inspect root-local pending-fresh'
        actual_scopes={(r['project_id'],r['root_session']) for r in db.execute(
            'SELECT project_id,root_session FROM root_fresh_projects WHERE request_id=?',(row['id'],))}
        observed_scopes={(item['project_id'],item['root_session']) for item in entry['scopes']}
        if actual_scopes!=observed_scopes:
            return 'root fresh-start terminal eligibility is unresolved; inspect root-local pending-fresh'
    return None
