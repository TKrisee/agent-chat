"""Explicit administrative team retirement; never a process-closure receipt."""
import contextlib
import hashlib
import json
import sqlite3
import time
import uuid
from .core import CoordError, Coordinator


def encoded(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)


def approval_digest(state,authority):
    # Ordinary authenticated reads refresh this heartbeat. It is audit evidence,
    # not action scope; keep the full snapshot and every host identity field.
    scope=dict(state,hosts=[{key:value for key,value in row.items() if key!='last_seen_at'}
                            for row in state['hosts']])
    return hashlib.sha256(encoded(dict(state=scope,authority=authority)).encode()).hexdigest()


@contextlib.contextmanager
def transaction(db):
    # This exception must not expire or otherwise rewrite unrelated ownership.
    db.execute('BEGIN IMMEDIATE')
    try:
        yield db
    except BaseException:
        db.rollback();raise
    else:
        db.commit()


def quarantined(db, *, session=None, thread=None, job=None):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE name='root_team_reset_targets'").fetchone():
        return False
    if session and db.execute('SELECT 1 FROM root_team_reset_targets WHERE session_id=?',(session,)).fetchone():
        return True
    if thread and db.execute('SELECT 1 FROM root_team_reset_targets WHERE thread_id=?',(thread,)).fetchone():
        return True
    if job and db.execute('SELECT 1 FROM root_team_reset_jobs WHERE job_id=?',(job,)).fetchone():
        return True
    if job and db.execute('''SELECT 1 FROM bridge_deliveries d JOIN messages m ON m.id=d.message_id
            JOIN root_team_reset_targets t ON t.session_id=m.recipient_session WHERE d.job_id=? LIMIT 1''',(job,)).fetchone():
        return True
    return False


def assert_ready(db):
    if (db.execute("SELECT 1 FROM sqlite_master WHERE name='root_team_reset_requests'").fetchone()
            and db.execute("SELECT 1 FROM root_team_reset_requests WHERE status IN ('quiescing','quarantined')").fetchone()):
        raise CoordError('team reset finalization is pending; retry its exact request before new team admission')


def handoff_schema(db):
    db.execute('''CREATE TABLE IF NOT EXISTS root_native_handoffs (
      kind TEXT NOT NULL,job_id TEXT NOT NULL,phase TEXT NOT NULL,session_id TEXT,
      thread_id TEXT,host_id TEXT NOT NULL,active INTEGER NOT NULL,started_at REAL NOT NULL,
      settled_at REAL,settlement TEXT,PRIMARY KEY(kind,job_id,phase))''')


def handoff_begin(db,kind,job,host,phase):
    handoff_schema(db)
    with transaction(db):
        target=job.get('target_session')
        thread=job.get('new_thread_id') or job.get('thread_id') or job.get('old_thread_id')
        if quarantined(db,session=target,thread=thread,job=job['id']):
            raise CoordError('team reset quarantined native admission')
        prior=db.execute('SELECT active FROM root_native_handoffs WHERE kind=? AND job_id=? AND phase=?',
            (kind,job['id'],phase)).fetchone()
        if prior and prior['active']:
            return  # Same durable permission acknowledgement, not a native RPC.
        db.execute('''INSERT INTO root_native_handoffs VALUES(?,?,?,?,?,?,1,?,NULL,NULL)
          ON CONFLICT(kind,job_id,phase) DO UPDATE SET active=1,started_at=excluded.started_at,settled_at=NULL,settlement=NULL''',
          (kind,job['id'],phase,target,thread,host,time.time()))


def handoff_pending(db):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE name='root_native_handoffs'").fetchone(): return []
    return [dict(row) for row in db.execute('SELECT * FROM root_native_handoffs WHERE active=1 ORDER BY started_at')]


def handoff_settling(db,kind,job_id,host,status):
    return any(row['kind']==kind and row['job_id']==job_id and row['host_id']==host
        and (status in ('uncertain','failed') or definite_settlement(kind,row['phase'],status)) for row in handoff_pending(db))


def definite_settlement(kind,phase,status):
    if status=='rejected-before-dispatch': return True
    if kind=='wake': return (phase,status) in (('adding','queued'),('starting','dispatched'))
    if kind=='reset': return (phase,status) in (('creating','created'),('adding','queued'),('starting','completed'))
    if kind=='control': return (phase=='changing' and status in ('completed','resolved_unchanged')) or (phase.startswith('interrupting:') and status in ('completed','waiting'))
    return False


def handoff_settle(db,kind,job_id,host,status):
    if status in ('uncertain','failed','cancelled'):
        return  # A timeout/error is bookkeeping, never definitive settlement.
    if db.execute("SELECT 1 FROM sqlite_master WHERE name='root_native_handoffs'").fetchone():
        for row in handoff_pending(db):
            if (row['kind'],row['job_id'],row['host_id'])==(kind,job_id,host) and definite_settlement(kind,row['phase'],status):
                db.execute('''UPDATE root_native_handoffs SET active=0,settled_at=?,settlement=?
                  WHERE kind=? AND job_id=? AND host_id=? AND phase=? AND active=1''',
                  (time.time(),status,kind,job_id,host,row['phase']))


def handoff_retry_verified(db,job_id,host):
    # Only called after the existing explicit retry's checking->ready transition
    # verifies the exact empty native thread; never on a timeout/queue-only view.
    if db.execute("SELECT 1 FROM sqlite_master WHERE name='root_native_handoffs'").fetchone():
        db.execute("UPDATE root_native_handoffs SET active=0,settled_at=?,settlement='explicit verified empty-thread retry' WHERE kind='reset' AND job_id=? AND host_id=? AND active=1",
            (time.time(),job_id,host))


def schema(db):
    db.executescript('''
      CREATE TABLE IF NOT EXISTS root_team_reset_requests (
        id TEXT PRIMARY KEY,root_session TEXT NOT NULL,project_id TEXT NOT NULL,
        digest TEXT NOT NULL,authority_json TEXT NOT NULL,snapshot_json TEXT NOT NULL,
        status TEXT NOT NULL CHECK(status IN ('quiescing','quarantined','completed')),created_at REAL NOT NULL,completed_at REAL,
        CHECK((status IN ('quiescing','quarantined') AND completed_at IS NULL) OR (status='completed' AND completed_at IS NOT NULL)));
      CREATE TABLE IF NOT EXISTS root_team_reset_targets (
        session_id TEXT PRIMARY KEY,request_id TEXT NOT NULL,thread_id TEXT,host_id TEXT);
      CREATE TABLE IF NOT EXISTS root_team_reset_jobs (
        job_id TEXT PRIMARY KEY,request_id TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS root_team_reset_events (
        id TEXT PRIMARY KEY,request_id TEXT NOT NULL,event TEXT NOT NULL,
        evidence_json TEXT NOT NULL,recorded_at REAL NOT NULL);
      CREATE TRIGGER IF NOT EXISTS root_team_reset_event_update BEFORE UPDATE ON root_team_reset_events
        BEGIN SELECT RAISE(ABORT,'team reset audit is immutable'); END;
      CREATE TRIGGER IF NOT EXISTS root_team_reset_event_delete BEFORE DELETE ON root_team_reset_events
        BEGIN SELECT RAISE(ABORT,'team reset audit is immutable'); END;
      CREATE TRIGGER IF NOT EXISTS root_team_reset_target_update BEFORE UPDATE ON root_team_reset_targets
        BEGIN SELECT RAISE(ABORT,'team reset quarantine is immutable'); END;
      CREATE TRIGGER IF NOT EXISTS root_team_reset_target_delete BEFORE DELETE ON root_team_reset_targets
        BEGIN SELECT RAISE(ABORT,'team reset quarantine is immutable'); END;
      CREATE TRIGGER IF NOT EXISTS root_team_reset_job_update BEFORE UPDATE ON root_team_reset_jobs
        BEGIN SELECT RAISE(ABORT,'team reset journal quarantine is immutable'); END;
      CREATE TRIGGER IF NOT EXISTS root_team_reset_job_delete BEFORE DELETE ON root_team_reset_jobs
        BEGIN SELECT RAISE(ABORT,'team reset journal quarantine is immutable'); END;
      CREATE TRIGGER IF NOT EXISTS root_team_reset_request_delete BEFORE DELETE ON root_team_reset_requests
        BEGIN SELECT RAISE(ABORT,'team reset request is immutable'); END;
      CREATE TRIGGER IF NOT EXISTS root_team_reset_request_update BEFORE UPDATE ON root_team_reset_requests
        WHEN NEW.id IS NOT OLD.id OR NEW.root_session IS NOT OLD.root_session OR NEW.project_id IS NOT OLD.project_id
          OR NEW.digest IS NOT OLD.digest OR NEW.authority_json IS NOT OLD.authority_json
          OR NEW.snapshot_json IS NOT OLD.snapshot_json OR NEW.created_at IS NOT OLD.created_at
          OR NOT ((OLD.status='quiescing' AND NEW.status='quarantined' AND NEW.completed_at IS NULL)
            OR (OLD.status='quarantined' AND NEW.status='completed' AND NEW.completed_at IS NOT NULL))
        BEGIN SELECT RAISE(ABORT,'team reset request scope is immutable'); END;
    ''')


class RootTeamReset:
    def __init__(self,roots):
        self.roots=roots

    def scope(self,project,session,host,target):
        root=self.roots.authorize(project,session,host)
        with self.roots.projects.connection() as db:
            row=db.execute('SELECT * FROM root_projects WHERE project_id=? AND joined=1',(target,)).fetchone()
        if not row:
            raise CoordError('team reset requires an exact enrolled project')
        return root,dict(row)

    def authority(self,root,message_ids):
        if message_ids==[]:
            return [dict(kind='authenticated_canonical_root_control',root_session=root['home_session'],
                host_id=root['host_id'],operator_instruction_attested=False,origin='canonical_root_control',
                body=None)]
        if (not isinstance(message_ids,list) or not 1<=len(message_ids)<=16
                or any(not isinstance(mid,str) or not mid for mid in message_ids)
                or len(set(message_ids))!=len(message_ids)):
            raise CoordError('provide exact original operator UI message IDs for normal review')
        with contextlib.closing(Coordinator(self.roots.path(root['home_project']))) as coord:
            result=[]
            for mid in message_ids:
                row=coord.db.execute('''SELECT m.id,m.sender_session,m.recipient_session,m.body,m.created_at,o.channel AS origin
                  FROM messages m JOIN operator_message_origins o ON o.message_id=m.id AND o.sender_session=m.sender_session
                  JOIN operator_identities i ON i.session_id=m.sender_session WHERE m.id=?''',(mid,)).fetchone()
                if not row or row['recipient_session']!=root['home_session'] or row['origin']!='authenticated_operator_ui':
                    raise CoordError('team reset authority must reference an original authenticated operator UI message to canonical root')
                result.append(dict(row,operator_instruction_attested=True))
        return result

    def snapshot(self,db,registry):
        tables={r['name'] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        def rows(table,where='',args=()):
            return [dict(row) for row in db.execute('SELECT * FROM '+table+(' WHERE '+where if where else '')+' ORDER BY rowid',args)] if table in tables else []
        protected={r['session_id'] for r in rows('root_projections')}|{r['session_id'] for r in rows('operator_identities')}
        targets=[row for row in rows('sessions') if row['id'] not in protected and not row['id'].startswith('system:')]
        ids={row['id'] for row in targets}
        bindings=[row for row in rows('bridge_bindings') if row['session_id'] in ids]
        threads={row['thread_id'] for row in bindings if row['thread_id']}
        deliveries=rows('bridge_deliveries');messages={r['id']:r for r in rows('messages')}
        jobs=[row for row in rows('bridge_jobs') if row['thread_id'] in threads or any(
            d['job_id']==row['id'] and (messages.get(d['message_id'],{}).get('sender_session') in ids or
            messages.get(d['message_id'],{}).get('recipient_session') in ids) for d in deliveries)]
        held=[row for row in rows('resources') if row['owner_session'] in ids]
        rids={row['reservation_id'] for row in held}
        return dict(registry=registry,targets=targets,bindings=bindings,
            hosts=[row for row in rows('remote_session_hosts') if row['session_id'] in ids],
            ownership=held,queue=[row for row in rows('resource_queue') if row['session'] in ids],
            local_guards=[row for row in rows('guarded_runs') if row['session'] in ids or row['reservation_id'] in rids],
            remote_guards=[row for row in rows('remote_guarded_runs') if row['session_id'] in ids or row['reservation_id'] in rids],
            outcomes=[row for row in rows('remote_reservation_outcomes') if row['owner_session'] in ids],
            jobs=jobs,controls=[row for row in rows('agent_control_requests') if row['target_session'] in ids],
            observations=[row for row in rows('agent_control_observations') if row['target_session'] in ids],
            resets=[row for row in rows('session_resets') if row['target_session'] in ids or row['requester_session'] in ids],
            handoffs=[row for row in handoff_pending(db) if row['session_id'] in ids or row['thread_id'] in threads or row['job_id'] in {j['id'] for j in jobs}],
            designated=[row for row in rows('project_orchestrators') if row['session_id'] in ids])

    def public(self,request):
        state=json.loads(request['snapshot_json'])
        return dict(request_id=request['id'],project=request['project_id'],status=request['status'],
            digest=request['digest'],authority=json.loads(request['authority_json']),
            targets=[dict(session_id=r['id'],agent=r['agent']) for r in state['targets']],
            retired_session_ids=[r['id'] for r in state['targets']] if request['status'] in ('quarantined','completed') else [],
            ownership_disposition=[dict(resource=r['name'],reservation_id=r['reservation_id'],owner_session=r['owner_session'],
                resource_reusable=request['status'] in ('quarantined','completed'),will_make_resource_reusable=True,
                process_closure='unverified') for r in state['ownership']],
            native_processes_stopped=False,process_closure='unverified',
            retained=dict(messages=True,history=True,queue_history=True,guards=True,outcomes=True,native_journals=True),
            queued_claims_retired=[dict(seq=r['seq'],resource=r['resource'],session=r['session']) for r in state['queue']] if request['status'] in ('quarantined','completed') else [],
            quarantined_jobs=[r['id'] for r in state['jobs']],new_team_created=False)

    def preview(self,project,session,host,target,message_ids):
        root,registry=self.scope(project,session,host,target)
        authority=self.authority(root,message_ids)
        with contextlib.closing(Coordinator(self.roots.path(target))) as coord:
            state=self.snapshot(coord.db,registry)
        digest=approval_digest(state,authority)
        request=dict(id=None,project_id=target,status='preview',digest=digest,
            snapshot_json=encoded(state),authority_json=encoded(authority))
        return self.public(request)

    def apply(self,project,session,host,target,message_ids,request_id,expected_digest,confirmed):
        root,registry=self.scope(project,session,host,target)
        try: request_id=str(uuid.UUID(request_id))
        except (ValueError,TypeError,AttributeError): raise CoordError('team reset request_id must be a UUID')
        if confirmed is not True:
            raise CoordError('team reset requires --confirm operation intent, not another operator dialogue')
        authority=self.authority(root,message_ids)
        with contextlib.closing(Coordinator(self.roots.path(target))) as coord:
            schema(coord.db)
            with transaction(coord.db) as db:
                prior=db.execute('SELECT * FROM root_team_reset_requests WHERE id=?',(request_id,)).fetchone()
                if prior:
                    if (prior['root_session'],prior['project_id'],prior['digest'],prior['authority_json'])!=(root['home_session'],target,expected_digest,encoded(authority)):
                        raise CoordError('team reset retry differs from immutable original scope')
                else:
                    state=self.snapshot(db,registry)
                    digest=approval_digest(state,authority)
                    if digest!=expected_digest:
                        raise CoordError('team reset preview changed; inspect a new exact preview before applying')
                    now=time.time();snapshot=encoded(state)
                    db.execute('INSERT INTO root_team_reset_requests VALUES(?,?,?,?,?,?,?,?,NULL)',
                        (request_id,root['home_session'],target,digest,encoded(authority),snapshot,'quiescing',now))
                    hostmap={r['session_id']:r['host_id'] for r in state['hosts']}
                    bound={r['session_id']:r['thread_id'] for r in state['bindings']}
                    for row in state['targets']:
                        db.execute('INSERT INTO root_team_reset_targets VALUES(?,?,?,?)',(row['id'],request_id,bound.get(row['id']),hostmap.get(row['id'])))
                    for row in state['jobs']:
                        db.execute('INSERT INTO root_team_reset_jobs VALUES(?,?)',(row['id'],request_id))
                    db.execute('INSERT INTO root_team_reset_events VALUES(?,?,?,?,?)',(request_id+':quiescing',request_id,'admission-quarantined',encoded(dict(preimage=state,authority=authority)),now))
                request=dict(db.execute('SELECT * FROM root_team_reset_requests WHERE id=?',(request_id,)).fetchone())
                if request['status']=='quiescing':
                    state=json.loads(request['snapshot_json'])
                    ids={row['id'] for row in state['targets']};threads={row['thread_id'] for row in state['bindings'] if row['thread_id']}
                    pending=[row for row in handoff_pending(db) if row['session_id'] in ids or row['thread_id'] in threads or row['job_id'] in {j['id'] for j in state['jobs']}]
                    if pending:
                        return dict(self.public(request),outstanding_native_handoffs=pending,
                            next_action='Wait for exact owning host to settle the already-admitted native call; retry same request. Never replay it.')
                    now=time.time()
                    for row in state['targets']:
                        if not db.execute('SELECT 1 FROM sessions WHERE id=? AND agent=? AND registered_at=?',(row['id'],row['agent'],row['registered_at'])).fetchone():
                            raise CoordError('quarantined reset roster changed; inspect original request')
                        db.execute('INSERT INTO retired_sessions(id,agent,registered_at,removed_at) VALUES(?,?,?,?)',(row['id'],row['agent'],row['registered_at'],now))
                        db.execute('DELETE FROM project_orchestrators WHERE session_id=?',(row['id'],))
                        db.execute('DELETE FROM sessions WHERE id=?',(row['id'],))
                    for row in state['queue']:
                        if db.execute('DELETE FROM resource_queue WHERE seq=? AND resource=? AND session=? AND queued_at=?',
                                (row['seq'],row['resource'],row['session'],row['queued_at'])).rowcount!=1:
                            raise CoordError('team reset queued claim changed')
                    for row in state['ownership']:
                        if db.execute('''UPDATE resources SET owner_session=NULL,reservation_id=NULL,token=NULL,
                          granted_at=NULL,deadline=NULL,stale=0,reason=? WHERE name=? AND owner_session=? AND reservation_id=? AND token IS ?''',
                          ('team-reset ownership exception '+request_id,row['name'],row['owner_session'],row['reservation_id'],row['token'])).rowcount!=1:
                            raise CoordError('team reset ownership changed')
                    db.execute("UPDATE root_team_reset_requests SET status='quarantined' WHERE id=? AND status='quiescing'",(request_id,))
                    db.execute('INSERT INTO root_team_reset_events VALUES(?,?,?,?,?)',(request_id+':quarantined',request_id,'administrative-retirement',
                        encoded(dict(preimage=state,authority=authority,no_process_closure=True,ownership_reusable=True)),now))
                request=dict(db.execute('SELECT * FROM root_team_reset_requests WHERE id=?',(request_id,)).fetchone())
        return self.finalize(target,request)

    def finalize(self,target,request):
        if request['status']=='completed': return self.public(request)
        old=json.loads(request['snapshot_json'])['registry']
        with self.roots.projects.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            current=db.execute('SELECT * FROM root_projects WHERE project_id=?',(target,)).fetchone()
            replacement=dict(old,agent=None,request_id=None,orchestrator_session=None,adopted=0)
            if not current or dict(current) not in (old,replacement):
                db.rollback();raise CoordError('team reset is quarantined; registry changed; inspect retained request without replay')
            db.execute('UPDATE root_projects SET agent=NULL,request_id=NULL,orchestrator_session=NULL,adopted=0 WHERE project_id=?',(target,))
            db.commit()
        with contextlib.closing(Coordinator(self.roots.path(target))) as coord:
            with transaction(coord.db) as db:
                db.execute("UPDATE root_team_reset_requests SET status='completed',completed_at=? WHERE id=? AND status='quarantined'",(time.time(),request['id']))
                db.execute('INSERT OR IGNORE INTO root_team_reset_events VALUES(?,?,?,?,?)',(request['id']+':completed',request['id'],'registry-finalized',encoded(dict(no_native_action=True)),time.time()))
                request=dict(db.execute('SELECT * FROM root_team_reset_requests WHERE id=?',(request['id'],)).fetchone())
        return self.public(request)

    def status(self,project,session,host,target,request_id):
        self.scope(project,session,host,target)
        with contextlib.closing(Coordinator(self.roots.path(target))) as coord:
            row=coord.db.execute('SELECT * FROM root_team_reset_requests WHERE id=?',(request_id,)).fetchone()
        if not row: raise CoordError('unknown team reset request')
        return self.public(dict(row))
