"""Configured-Root exact-worker OBSERVATION only; no runtime or domain grant."""
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import time
import types
import uuid
from agent_chat.core import CoordError
from agent_chat.agent_control import AgentControlState
from agent_chat import root_local, service
from agent_chat.rpc import RpcClient
if __package__:
    from .native_owner_evidence import DarwinEvidence, EvidenceError, same_owner
else:
    from native_owner_evidence import DarwinEvidence, EvidenceError, same_owner
monotonic = time.monotonic

SELECTED = ('session_dfa927fafd0e46bebebc9ab3062fe4fd','01a18a68-f8fc-76b0-89a3-d11688729205','host_o6KZLd0kkjchCaH8OjoJZ9aZXHDlFiE2')
UNAVAILABLE = ('activePermissionProfile','approvalPolicy','approvalsReviewer','sandbox')
FIELDS = {'project','target','expected_thread','expected_host','request_id','confirm'}
SETTINGS = ('model','modelProvider','reasoningEffort','cwd','runtimeWorkspaceRoots',*UNAVAILABLE)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def require(condition, reason):
    if not condition: raise CoordError(reason)


def _open(master, home_project, session, host, p):
    root_local._preflight(master, service._installed_server_config().plist, home_project, session, host,
                          'worker-native-observe', p)
    master = Path(master)
    registry = root_local._file(Path(str(master)+'.projects.sqlite3'),os.geteuid())
    target = root_local._target(master,registry,p['project'],os.geteuid())
    home = root_local._target(master,registry,home_project,os.geteuid())
    require(target != home,'observer requires separate managed project')
    db = sqlite3.connect(target.as_uri()+'?mode=rw',uri=True,timeout=3)
    db.row_factory = sqlite3.Row
    try:
        db.execute('ATTACH DATABASE ? AS registry',(registry.as_uri()+'?mode=rw',))
        db.execute('ATTACH DATABASE ? AS home',(home.as_uri()+'?mode=rw',))
    except BaseException:
        db.close()
        raise
    return db


def scope(db, home_project, session, host, p):
    root = db.execute('SELECT * FROM registry.root_config WHERE singleton=1').fetchone()
    require(root is not None and (root['home_project'],root['home_session'],root['host_id'])==(home_project,session,host)
            and root['mode']=='running','configured Root identity/admission differs')
    rb = db.execute('''SELECT b.thread_id,b.parent_session,h.host_id,b.updated_at FROM home.bridge_bindings b
      JOIN home.remote_session_hosts h ON h.session_id=b.session_id JOIN home.sessions s ON s.id=b.session_id
      WHERE b.session_id=?''',(session,)).fetchone()
    require(rb is not None and tuple(rb)[:3]==(root['thread_id'],None,host),'Root main binding differs')
    project = db.execute('SELECT * FROM registry.root_projects WHERE project_id=?',(p['project'],)).fetchone()
    require(project is not None and project['joined'] and project['orchestrator_session'],'managed project is not enrolled')
    worker=p['target']
    binding=db.execute('''SELECT b.thread_id,b.parent_session,h.host_id,b.updated_at FROM bridge_bindings b
      JOIN sessions s ON s.id=b.session_id JOIN remote_session_hosts h ON h.session_id=b.session_id
      WHERE b.session_id=?''',(worker,)).fetchone()
    manager=db.execute('SELECT manager_session FROM agent_management WHERE target_session=?',(worker,)).fetchone()
    po=db.execute('SELECT root_session FROM project_orchestrators WHERE session_id=?',(project['orchestrator_session'],)).fetchone()
    require(binding is not None and tuple(binding)[:3]==(p['expected_thread'],None,host) and p['expected_host']==host
            and manager is not None and manager[0]==project['orchestrator_session'] and po is not None
            and po[0]==project['session_id'],'worker/project/main/host/manager differs')
    require(not db.execute('SELECT 1 FROM retired_sessions WHERE id=?',(worker,)).fetchone(),'worker retired')
    for sql in ('SELECT 1 FROM resources WHERE owner_session=?',
                'SELECT 1 FROM resource_queue WHERE session=?',
                'SELECT 1 FROM guarded_runs WHERE session=? AND closed_at IS NULL',
                'SELECT 1 FROM remote_guarded_runs WHERE session_id=? AND closed_at IS NULL',
                "SELECT 1 FROM agent_control_requests WHERE target_session=? AND status NOT IN ('completed','failed','cancelled','resolved_unchanged')"):
        require(not db.execute(sql,(worker,)).fetchone(),'target custody/queue/control is not quiescent')
    mode=db.execute('SELECT mode FROM agent_controls WHERE target_session=?',(worker,)).fetchone()
    require(mode is not None and mode[0]=='stopped','target requires pre-existing ordinary stopped admission')
    require(not db.execute("SELECT 1 FROM bridge_jobs WHERE thread_id=? AND status NOT IN ('completed','failed','cancelled')",(p['expected_thread'],)).fetchone(),'target bridge work is not quiescent')
    lease=db.execute('SELECT owner,acquired_at FROM bridge_dispatcher WHERE host_id=?',(host,)).fetchone()
    runtime=db.execute('SELECT server,pid,heartbeat,error FROM bridge_client_runtime WHERE host_id=?',(host,)).fetchone()
    require(lease is not None and runtime is not None and runtime['pid']>1 and runtime['error'] is None,
            'registered native owner unavailable')
    require(0<=time.time()-runtime['heartbeat']<=15,'registered bridge heartbeat stale')
    registration={'owner':lease['owner'],'acquired_at':lease['acquired_at'],'pid':runtime['pid'],
                  'endpoint':runtime['server'],'heartbeat':runtime['heartbeat']}
    identity=(home_project,session,tuple(rb),p['project'],project['session_id'],project['orchestrator_session'],
              worker,*tuple(binding),manager[0],lease['owner'],lease['acquired_at'],runtime['server'],runtime['pid'])
    return digest(identity),registration


def schema(db):
    db.execute('''CREATE TABLE IF NOT EXISTS registry.worker_native_samples (
      root_session TEXT NOT NULL,request_id TEXT NOT NULL,request_digest TEXT NOT NULL,
      project TEXT NOT NULL,worker TEXT NOT NULL,main TEXT NOT NULL,host TEXT NOT NULL,
      status TEXT NOT NULL,created_at REAL NOT NULL,completed_at REAL,
      scope_digest TEXT NOT NULL,settings_json TEXT,goal_json TEXT,owner_generation TEXT,result_json TEXT,
      PRIMARY KEY(root_session,request_id))''')
    db.execute('''CREATE TABLE IF NOT EXISTS registry.worker_native_events (
      root_session TEXT NOT NULL,request_id TEXT NOT NULL,sequence INTEGER NOT NULL,
      event TEXT NOT NULL,created_at REAL NOT NULL,detail TEXT NOT NULL,
      PRIMARY KEY(root_session,request_id,sequence))''')
    for operation in ('UPDATE','DELETE'):
        db.execute(f'''CREATE TRIGGER IF NOT EXISTS registry.worker_native_events_no_{operation.lower()}
          BEFORE {operation} ON worker_native_events BEGIN SELECT RAISE(ABORT,'immutable native observation history'); END''')


def event(db,session,rid,kind,detail):
    seq=db.execute('SELECT COUNT(*) FROM registry.worker_native_events WHERE root_session=? AND request_id=?',(session,rid)).fetchone()[0]
    db.execute('INSERT INTO registry.worker_native_events VALUES(?,?,?,?,?,?)',(session,rid,seq,kind,time.time(),detail))


def pre_read(rpc, main):
    response=rpc.request('thread/read',{'threadId':main,'includeTurns':False})
    require(isinstance(response,dict) and isinstance(response.get('thread'),dict),'missing thread read')
    t=response['thread']
    require(t.get('id')==main and t.get('sessionId')==main and t.get('status',{}).get('type')=='idle' and t.get('canAcceptDirectInput') is True,
            'target is wrong, unloaded, active or not directly idle')
    values={}
    for key in ('model','modelProvider','reasoningEffort','cwd'):
        require(isinstance(t.get(key),str) and bool(t[key]),'missing pre-read '+key)
        values[key]=t[key]
    require(Path(values['cwd']).is_absolute(),'invalid native cwd')
    env=t.get('environments')
    require(isinstance(env,list),'missing native environments')
    selected=[e for e in env if isinstance(e,dict) and e.get('cwd')==values['cwd']]
    require(len(selected)==1,'ambiguous native workspace environment')
    roots=selected[0].get('runtimeWorkspaceRoots')
    require(isinstance(roots,list) and all(isinstance(x,str) and Path(x).is_absolute() for x in roots),'invalid workspace roots')
    values['runtimeWorkspaceRoots']=roots
    q=rpc.request('thread/queue/list',{'threadId':main,'limit':100})
    require(isinstance(q,dict) and q.get('data')==[] and q.get('nextCursor') is None,'native queue is nonempty or unknown')
    g=rpc.request('thread/goal/get',{'threadId':main})
    require(isinstance(g,dict) and set(g)=={'goal'},'unknown native goal')
    goal=g['goal']
    if goal is not None:
        require(isinstance(goal,dict) and goal.get('threadId')==main and goal.get('status')=='paused'
                and isinstance(goal.get('objective'),str) and bool(goal['objective']),'active, malformed or unknown native goal')
    return values,goal


def returned_settings(value, main, before):
    require(isinstance(value,dict) and isinstance(value.get('thread'),dict) and value['thread'].get('id')==main,
            'returned thread identity differs')
    require(all(k in value for k in SETTINGS),'missing returned protected setting')
    for key, expected in before.items(): require(value[key]==expected,'pre-read setting changed: '+key)
    profile=value['activePermissionProfile']
    require(profile is None or (isinstance(profile,dict) and set(profile)<= {'id','extends'}
            and isinstance(profile.get('id'),str) and bool(profile['id'])
            and (profile.get('extends') is None or isinstance(profile['extends'],str))),'malformed native permission profile')
    require(value['approvalPolicy'] in ('untrusted','on-request','never'),'unsupported native approval policy')
    require(value['approvalsReviewer'] in ('user','auto_review','guardian_subagent'),'unsupported native approval reviewer')
    sb=value['sandbox']
    require(isinstance(sb,dict) and sb.get('type') in ('readOnly','workspaceWrite') and type(sb.get('networkAccess')) is bool,
            'incompatible or incomplete native sandbox')
    if sb['type']=='workspaceWrite':
        require(isinstance(sb.get('writableRoots'),list) and all(isinstance(x,str) and Path(x).is_absolute() for x in sb['writableRoots'])
                and type(sb.get('excludeSlashTmp')) is bool and type(sb.get('excludeTmpdirEnvVar')) is bool,
                'incomplete native write sandbox')
    # Preserve all returned settings, including optional collaboration/service fields;
    # history paging and thread metadata are not permission/settings fields.
    settings={k:v for k,v in value.items() if k not in ('thread','initialTurnsPage','turnsBackwardsCursor','itemsBackwardsCursor')}
    return settings


def observe(master,home_project,session,host,params,*,_kernel=None,_rpc=None):
    """Private Python integration seam; Root dispatch supplies no injectable providers."""
    require(isinstance(params,dict) and set(params)==FIELDS and params['confirm'] is True,'invalid observer fields')
    p=dict(params)
    for k in FIELDS-{'confirm'}: root_local._text(p[k],k)
    require((p['target'],p['expected_thread'],p['expected_host'])==SELECTED,'target is outside selected retained worker')
    require(str(uuid.UUID(p['request_id']))==p['request_id'],'request ID must be canonical UUID')
    request_digest=digest((home_project,session,host,p))
    db=_open(master,home_project,session,host,p)
    entered=False; resume_started=False; connect_started=False; rpc=None
    try:
        db.execute('BEGIN IMMEDIATE')
        scope_pin,registration=scope(db,home_project,session,host,p)
        schema(db)
        old=db.execute('SELECT * FROM registry.worker_native_samples WHERE root_session=? AND request_id=?',(session,p['request_id'])).fetchone()
        if old is not None:
            require(old['request_digest']==request_digest,'request ID reused for another target')
            db.commit()
            return {'request_id':p['request_id'],'status':old['status'],'lookup_only':True,'fresh_sample':False,
                    'original_result_json':old['result_json'],'observation_only':True,'runtime_authorized':False}
        unfinished=db.execute("SELECT 1 FROM registry.worker_native_samples WHERE worker=? AND project=? AND status IN ('attempt','uncertain')",(p['target'],p['project'])).fetchone()
        require(not unfinished,'prior uncertain/in-flight sample requires explicit disposition; no automatic repeat')
        prior=db.execute("SELECT settings_json,owner_generation FROM registry.worker_native_samples WHERE project=? AND worker=? AND status='observed' ORDER BY completed_at DESC,root_session DESC,request_id DESC LIMIT 1",(p['project'],p['target'])).fetchone()
        baseline=tuple(prior) if prior else None
        origin=db.execute("SELECT request_id FROM registry.worker_native_samples WHERE project=? AND worker=? AND status='observed' ORDER BY completed_at,root_session,request_id LIMIT 1",(p['project'],p['target'])).fetchone()
        baseline_origin=origin[0] if origin else p['request_id']
        db.execute('INSERT INTO registry.worker_native_samples VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            (session,p['request_id'],request_digest,p['project'],p['target'],p['expected_thread'],host,'attempt',time.time(),None,scope_pin,None,None,None,None))
        event(db,session,p['request_id'],'attempt','first_baseline' if baseline is None else 'compare_complete_baseline')
        db.commit(); entered=True
        start=monotonic()
        kernel=_kernel if _kernel is not None else DarwinEvidence()
        initial=kernel.sample(registration)
        generation=digest((initial.endpoint,initial.listener_pid,[vars(x) for x in initial.chain],initial.image_sha,initial.code_hash))
        if baseline: require(generation==baseline[1],'established native owner generation differs; no rebaseline')
        rpc=(_rpc if _rpc is not None else RpcClient)(registration['endpoint'],timeout=3)
        connect_started=True
        rpc.connect()
        connected=kernel.sample(registration,rpc)
        same_owner(initial,connected)
        before,goal=pre_read(rpc,p['expected_thread'])
        # Second coordinator/kernel checks precede the potentially uncertain resume.
        db.execute('BEGIN IMMEDIATE')
        pin,current=scope(db,home_project,session,host,p)
        require(pin==scope_pin,'binding changed before resume')
        db.commit()
        same_owner(connected,kernel.sample(current,rpc),require_peer=True)
        boundary=rpc.notification_boundary(deadline=time.monotonic()+3)
        resume_started=True
        value=rpc.request('thread/resume',{'threadId':p['expected_thread'],'excludeTurns':True})
        settings=returned_settings(value,p['expected_thread'],before)
        settings_json=canonical(settings)
        if baseline: require(settings_json==baseline[0],'complete protected baseline changed; no silent rebaseline')
        after,after_goal=pre_read(rpc,p['expected_thread'])
        require(after==before and canonical(after_goal)==canonical(goal),'goal or available settings changed after resume')
        for sequence,message in rpc.notifications_since(boundary):
            method=message.get('method','')
            values=message.get('params')
            require(isinstance(values,dict),'malformed native notification')
            notified=values.get('threadId') or (values.get('goal') or {}).get('threadId')
            if notified==p['expected_thread']:
                require(method!='thread/goal/cleared','goal-cleared notification during observation')
                if method=='thread/goal/updated': require(canonical(values.get('goal'))==canonical(goal),'goal changed transiently')
                require(method not in ('turn/started','thread/settings/updated'),'target changed during observation')
        final=kernel.sample(current,rpc)
        same_owner(connected,final,require_peer=True)
        require(monotonic()-start<=20,'observation exceeded freshness bound')
        # Close before publishing; a close/disconnect error cannot yield success.
        rpc.close(); rpc=None
        same_owner(initial,kernel.sample(current))
        db.execute('BEGIN IMMEDIATE')
        pin,current=scope(db,home_project,session,host,p)
        require(pin==scope_pin,'binding changed before authoritative persistence')
        same_owner(initial,kernel.sample(current))
        require(monotonic()-start<=20,'observation stale before persistence')
        latest=db.execute("SELECT settings_json,owner_generation FROM registry.worker_native_samples WHERE project=? AND worker=? AND status='observed' ORDER BY completed_at DESC,root_session DESC,request_id DESC LIMIT 1",(p['project'],p['target'])).fetchone()
        require((tuple(latest) if latest else None)==baseline,'baseline changed concurrently')
        state=AgentControlState.existing_schema(types.SimpleNamespace(db=db))
        permissions={k:settings[k] for k in ('modelProvider','sandbox','approvalPolicy','approvalsReviewer','activePermissionProfile','runtimeWorkspaceRoots')}
        observed=time.time()
        state.db.execute('INSERT OR REPLACE INTO agent_control_observations VALUES(?,?,?,?,?,?,?,?,?,?)',
            (p['target'],p['expected_thread'],'idle',None,0,settings['model'],settings['reasoningEffort'],settings['cwd'],canonical(permissions),observed))
        state.db.execute('DELETE FROM agent_control_workspace_roots WHERE target_session=?',(p['target'],))
        state.db.executemany('INSERT INTO agent_control_workspace_roots VALUES(?,?,?)',[(p['target'],i,x) for i,x in enumerate(settings['runtimeWorkspaceRoots'])])
        result={'request_id':p['request_id'],'status':'observed','lookup_only':False,'fresh_sample':True,'project':p['project'],
          'worker':p['target'],'main':p['expected_thread'],'host':host,'checked_at':observed,'settings_sha256':digest(settings),
          'first_sample':baseline is None,'unavailable_preimage':list(UNAVAILABLE) if baseline is None else [],
          'first_effect_preservation_independently_checked':False if baseline is None else None,
          'baseline_provenance':{'initial_request_id':baseline_origin,'unavailable_preimage':list(UNAVAILABLE),'first_effect_preservation_independently_checked':False},
          'complete_baseline_preserved':baseline is not None,'association_limit':'bounded_non_atomic_kernel_samples',
          'observation_only':True,'runtime_authorized':False}
        db.execute("UPDATE registry.worker_native_samples SET status='observed',completed_at=?,settings_json=?,goal_json=?,owner_generation=?,result_json=? WHERE root_session=? AND request_id=? AND status='attempt'",
          (observed,settings_json,canonical(goal),generation,canonical(result),session,p['request_id']))
        event(db,session,p['request_id'],'observed',digest(result)); db.commit()
        return result
    except BaseException as error:
        if db.in_transaction: db.rollback()
        if entered:
            # Keep immutable failure history; no authoritative partial observation.
            db.execute('BEGIN IMMEDIATE')
            status='uncertain' if resume_started or (connect_started and isinstance(error,(OSError,TimeoutError))) else 'failed'
            db.execute('UPDATE registry.worker_native_samples SET status=?,completed_at=? WHERE root_session=? AND request_id=? AND status=\'attempt\'',(status,time.time(),session,p['request_id']))
            event(db,session,p['request_id'],status,type(error).__name__+': '+str(error)[:300]); db.commit()
        raise
    finally:
        try:
            if rpc is not None: rpc.close()
        finally:
            db.close()
