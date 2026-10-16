"""Journaled fresh native root context without replacing coordination identity."""
import contextlib
import hashlib
import json
import time
import uuid
from types import SimpleNamespace

from .communication import STANDING_POLICY, policy_version
from .core import Coordinator, CoordError


TERMINAL = ('completed',)


def held(db, session):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_fresh_gates'").fetchone():
        return False
    return bool(db.execute('SELECT 1 FROM root_fresh_gates WHERE root_session=? AND active=1', (session,)).fetchone())


def schema(db):
    db.executescript('''
      CREATE TABLE IF NOT EXISTS root_fresh_requests (
        id TEXT PRIMARY KEY,home_project TEXT NOT NULL,home_session TEXT NOT NULL,
        host_id TEXT NOT NULL,old_thread_id TEXT NOT NULL,prompt TEXT NOT NULL,
        instruction_origin TEXT NOT NULL,status TEXT NOT NULL,new_thread_id TEXT,
        settings_digest TEXT,new_settings_digest TEXT,queue_id TEXT,error TEXT,
        native_proof_json TEXT,created_at REAL NOT NULL,updated_at REAL NOT NULL);
      CREATE TABLE IF NOT EXISTS root_fresh_projects (
        request_id TEXT NOT NULL,project_id TEXT NOT NULL,root_session TEXT NOT NULL,
        PRIMARY KEY(request_id,project_id));
      CREATE TABLE IF NOT EXISTS root_fresh_relocations (
        request_id TEXT NOT NULL,project_id TEXT NOT NULL,job_id TEXT NOT NULL,
        old_thread_id TEXT NOT NULL,new_thread_id TEXT NOT NULL,
        old_payload TEXT NOT NULL,new_payload TEXT NOT NULL,recorded_at REAL NOT NULL,
        PRIMARY KEY(request_id,project_id,job_id));
      CREATE TABLE IF NOT EXISTS root_fresh_wake_batches (
        id TEXT PRIMARY KEY,request_id TEXT NOT NULL UNIQUE,payload TEXT NOT NULL,
        recorded_at REAL NOT NULL);
      CREATE TABLE IF NOT EXISTS root_fresh_wake_members (
        batch_id TEXT NOT NULL,project_id TEXT NOT NULL,root_session TEXT NOT NULL,
        job_id TEXT NOT NULL,payload TEXT NOT NULL,
        PRIMARY KEY(batch_id,project_id,job_id));
      CREATE TABLE IF NOT EXISTS root_fresh_wake_deliveries (
        batch_id TEXT NOT NULL,project_id TEXT NOT NULL,job_id TEXT NOT NULL,
        message_id TEXT NOT NULL,recipient_session TEXT NOT NULL,
        PRIMARY KEY(batch_id,project_id,message_id));
      CREATE TRIGGER IF NOT EXISTS immutable_root_relocation_update BEFORE UPDATE ON root_fresh_relocations
        BEGIN SELECT RAISE(ABORT,'root wake relocation audit is immutable'); END;
      CREATE TRIGGER IF NOT EXISTS immutable_root_relocation_delete BEFORE DELETE ON root_fresh_relocations
        BEGIN SELECT RAISE(ABORT,'root wake relocation audit is immutable'); END;
    ''')
    for table in ('root_fresh_wake_batches','root_fresh_wake_members','root_fresh_wake_deliveries'):
        for action in ('UPDATE','DELETE'):
            db.execute(f'''CREATE TRIGGER IF NOT EXISTS immutable_{table}_{action.lower()} BEFORE {action} ON {table}
                BEGIN SELECT RAISE(ABORT,'root wake batch audit is immutable'); END''')
    if 'native_action' not in {row['name'] for row in db.execute('PRAGMA table_info(root_fresh_requests)')}:
        db.execute('ALTER TABLE root_fresh_requests ADD COLUMN native_action TEXT')


def local_schema(db):
    db.execute('''CREATE TABLE IF NOT EXISTS root_fresh_gates (
        request_id TEXT PRIMARY KEY,root_session TEXT NOT NULL,old_thread_id TEXT NOT NULL,
        new_thread_id TEXT,active INTEGER NOT NULL)''')


class RootRestart:
    def __init__(self, roots):
        self.roots, self.projects = roots, roots.projects

    def job(self, request_id):
        with self.projects.connection() as db:
            row = db.execute('SELECT * FROM root_fresh_requests WHERE id=?', (request_id,)).fetchone()
        if not row:
            raise CoordError('unknown root fresh-start request')
        return dict(row)

    @staticmethod
    def public(job):
        result = {key:value for key,value in job.items() if key not in ('prompt','settings_digest','new_settings_digest')}
        if job['status'] == 'completed':
            result['operator_report'] = dict(request_id=job['id'], new_thread_id=job['new_thread_id'],
                cli_guidance='In a running CLI already connected via --remote to the SAME existing bridge endpoint, '
                'use /resume and select the new root conversation '+job['new_thread_id']+'. '
                'A plain/local CLI cannot switch endpoint through that picker; it needs a one-time '
                'remote relaunch against the existing bridge endpoint. /resume UUID is not documented direct-selection syntax.')
        return result

    def scope(self, request_id):
        with self.projects.connection() as db:
            return [dict(row) for row in db.execute('SELECT * FROM root_fresh_projects WHERE request_id=? ORDER BY project_id', (request_id,))]

    def authorize_status(self, project, session, host, request_id):
        job=self.job(request_id); root=self.roots.config()
        if not root or (project,session,host)!=(job['home_project'],job['home_session'],job['host_id']) or (root['home_project'],root['home_session'],root['host_id'])!=(project,session,host):
            raise CoordError('root fresh-start requires its exact canonical home identity and host')
        return self.public(job)

    @staticmethod
    def fences(db, session, old_thread):
        if db.execute('SELECT 1 FROM resources WHERE owner_session=?',(session,)).fetchone():
            raise CoordError('root fresh-start requires actual root ownership cleanup')
        for table,column in (('guarded_runs','session'),('remote_guarded_runs','session_id')):
            if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(table,)).fetchone() and db.execute(f'SELECT 1 FROM {table} WHERE {column}=? AND closed_at IS NULL',(session,)).fetchone():
                raise CoordError('root fresh-start requires closed root guarded runs')
        if db.execute("SELECT 1 FROM bridge_jobs WHERE thread_id=? AND status NOT IN ('prepared','dispatched','cancelled')",(old_thread,)).fetchone():
            raise CoordError('reconcile submitted or uncertain root wakes before fresh-start')
        if db.execute("SELECT 1 FROM sqlite_master WHERE name='bridge_root_wake_batches'").fetchone() and db.execute("SELECT 1 FROM bridge_root_wake_batches b JOIN bridge_jobs j ON j.id=b.job_id WHERE j.thread_id=? AND j.status NOT IN ('dispatched','cancelled')",(old_thread,)).fetchone():
            raise CoordError('reconcile retained root wake batch before another fresh-start')
        if db.execute("SELECT 1 FROM agent_control_requests WHERE target_session=? AND status NOT IN ('completed','failed','cancelled','resolved_unchanged')",(session,)).fetchone():
            raise CoordError('reconcile pending root runtime controls before fresh-start')
        if db.execute("SELECT 1 FROM session_resets WHERE target_session=? AND status NOT IN ('completed','failed','cancelled')",(session,)).fetchone():
            raise CoordError('reconcile pending root reset before fresh-start')

    def request(self, project, session, host, old_thread, prompt, request_id, confirmed, *, operator=False):
        root=self.roots.authorize(project,session,host)
        if confirmed is not True or not isinstance(prompt,str) or not prompt.strip() or len(prompt.encode())>65536:
            raise CoordError('root fresh-start requires confirmation and an exact nonempty boot prompt at most64 KiB')
        try: request_id=str(uuid.UUID(request_id))
        except (ValueError,TypeError,AttributeError): raise CoordError('root fresh-start request_id must be a UUID')
        with self.projects.connection() as db:
            old=db.execute('SELECT * FROM root_fresh_requests WHERE id=?',(request_id,)).fetchone()
            if old:
                if (old['home_project'],old['home_session'],old['host_id'],old['old_thread_id'],old['prompt'])!=(project,session,host,old_thread,prompt):
                    raise CoordError('root fresh-start retry payload differs from original')
                return self.public(dict(old))
            if root['thread_id']!=old_thread or root['mode']!='running':
                raise CoordError('root fresh-start requires the exact current running root main')
            if db.execute("SELECT 1 FROM root_fresh_requests WHERE status<>'completed'").fetchone():
                raise CoordError('another root fresh-start is unresolved; inspect its retained request')
            rows=[dict(row) for row in db.execute('SELECT project_id,session_id AS root_session FROM root_projects WHERE joined=1')]
        for item in rows:
            with contextlib.closing(Coordinator(self.roots.path(item['project_id']))) as coord:
                self.fences(coord.db,item['root_session'],old_thread)
        now=time.time()
        with self.projects.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('INSERT INTO root_fresh_requests(id,home_project,home_session,host_id,old_thread_id,prompt,instruction_origin,status,new_thread_id,settings_digest,new_settings_digest,queue_id,error,native_proof_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,\'pending\',NULL,NULL,NULL,NULL,NULL,NULL,?,?)',
                       (request_id,project,session,host,old_thread,prompt,'authenticated_operator_ui' if operator else 'authorized_root_lifecycle',now,now))
            db.executemany('INSERT INTO root_fresh_projects VALUES(?,?,?)',[(request_id,item['project_id'],item['root_session']) for item in rows])
            db.commit()
        self.install_gates(request_id)
        return self.public(self.job(request_id))

    def install_gates(self, request_id):
        job=self.job(request_id)
        for item in self.scope(request_id):
            with contextlib.closing(Coordinator(self.roots.path(item['project_id']))) as coord:
                local_schema(coord.db)
                with coord.tx() as db:
                    db.execute('INSERT OR IGNORE INTO root_fresh_gates VALUES(?,?,?,?,1)',(request_id,item['root_session'],job['old_thread_id'],job['new_thread_id']))

    def jobs(self, project, host):
        with self.projects.connection() as db:
            jobs=[dict(row) for row in db.execute("SELECT * FROM root_fresh_requests WHERE home_project=? AND host_id=? AND status<>'completed' ORDER BY created_at",(project,host))]
        return jobs

    def update(self, project, host, request_id, expected, status, **values):
        job=self.job(request_id)
        if (job['home_project'],job['host_id'])!=(project,host): raise CoordError('root fresh-start belongs to another owning host/project')
        if job['status']!=expected: raise CoordError('root fresh-start state changed; inspect retained request')
        transitions={'pending':{'pending','creating'},'creating':{'created','uncertain'},'created':{'created','switching'},'switching':{'switching','switched'},'switched':{'adding','switched'},'adding':{'queued','uncertain'},'queued':{'starting','completed','queued'},'starting':{'completed','uncertain'},'uncertain':{'uncertain','queued','completed'},'finalizing':{'finalizing'}}
        if status not in transitions.get(expected,set()) or set(values)-{'new_thread_id','settings_digest','new_settings_digest','queue_id','error','native_proof','server'}:
            raise CoordError('invalid root fresh-start transition')
        if status=='creating':
            self.install_gates(request_id)
            for item in self.scope(request_id):
                with contextlib.closing(Coordinator(self.roots.path(item['project_id']))) as coord:
                    self.fences(coord.db,item['root_session'],job['old_thread_id'])
        if 'new_thread_id' in values:
            try: thread=str(uuid.UUID(values['new_thread_id']))
            except (ValueError,TypeError,AttributeError): raise CoordError('fresh root candidate must be an actual distinct native UUID')
            if thread==job['old_thread_id']: raise CoordError('fresh root candidate must be distinct')
            values['new_thread_id']=thread
        if status=='switching' and (not job['new_thread_id'] or values.get('new_settings_digest',job['new_settings_digest'])!=job['settings_digest']):
            raise CoordError('fresh root settings readback must match before binding switch')
        proof=values.pop('native_proof',None)
        if status=='completed':
            if not isinstance(proof,dict) or proof.get('thread_id')!=job['new_thread_id'] or proof.get('client_id')!=request_id or proof.get('source')!='native-history' or not isinstance(proof.get('turn_id'),str) or not proof['turn_id']:
                raise CoordError('root boot completion requires exact native consumption proof')
            values['native_proof_json']=json.dumps(proof,sort_keys=True)
            status='finalizing'
        server=values.pop('server',None)
        with self.projects.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            current=db.execute('SELECT status FROM root_fresh_requests WHERE id=?',(request_id,)).fetchone()
            if current['status']!=expected: raise CoordError('root fresh-start changed concurrently')
            changes=dict(values,status=status,updated_at=time.time())
            if status in ('creating','adding','starting'): changes['native_action']=status
            db.execute('UPDATE root_fresh_requests SET '+','.join(key+'=?' for key in changes)+' WHERE id=?',(*changes.values(),request_id));db.commit()
        if status=='switching' and server:
            self.switch(request_id,server)
        if status=='finalizing' and not values.get('error'):
            self.finalize(request_id)
        return self.job(request_id)

    def release_gate(self, request_id, item):
        with contextlib.closing(Coordinator(self.roots.path(item['project_id']))) as coord:
            with coord.tx() as db:
                db.execute('UPDATE root_fresh_gates SET active=0 WHERE request_id=?',(request_id,))

    def finalize(self, request_id):
        job=self.job(request_id)
        if job['status']!='finalizing' or not job['native_proof_json']:
            raise CoordError('root finalization requires retained native consumption proof')
        for item in self.scope(request_id):
            self.release_gate(request_id,item)
        with self.projects.connection() as db:
            db.execute("UPDATE root_fresh_requests SET status='completed',error=NULL,updated_at=? WHERE id=? AND status='finalizing'",(time.time(),request_id))
            db.commit()

    def switch(self, request_id, server):
        from .bridge import Bridge
        from .bridge_state import BridgeState
        job=self.job(request_id);new=job['new_thread_id'];old=job['old_thread_id']
        for item in self.scope(request_id):
            with contextlib.closing(Coordinator(self.roots.path(item['project_id']),item['root_session'])) as coord:
                self.fences(coord.db,item['root_session'],old)
                state=BridgeState(coord)
                with coord.tx() as db:
                    binding=db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?',(item['root_session'],)).fetchone()
                    if not binding or binding['thread_id'] not in (old,new): raise CoordError('root projection binding changed; fresh switch withheld')
                    db.execute('UPDATE bridge_bindings SET thread_id=?,updated_at=? WHERE session_id=?',(new,time.time(),item['root_session']))
                    db.execute('UPDATE root_projections SET thread_id=? WHERE session_id=?',(new,item['root_session']))
                    db.execute('UPDATE root_fresh_gates SET new_thread_id=? WHERE request_id=?',(new,request_id))
                for wake in list(coord.db.execute("SELECT * FROM bridge_jobs WHERE thread_id=? AND status='prepared'",(old,))):
                    messages=[dict(row) for row in coord.db.execute('SELECT m.id,m.recipient_session FROM bridge_deliveries d JOIN messages m ON m.id=d.message_id WHERE d.job_id=?',(wake['id'],))]
                    adapter=SimpleNamespace(resolve=state.resolve,wake_messages=state.wake_messages,
                                            connection_metadata=lambda:dict(server=server,project=item['project_id'],session=item['root_session']))
                    payload=Bridge(adapter,None).payload(new,messages)
                    with self.projects.connection() as registry:
                        registry.execute('INSERT OR IGNORE INTO root_fresh_relocations VALUES(?,?,?,?,?,?,?,?)',
                                         (request_id,item['project_id'],wake['id'],old,new,wake['payload'],payload,time.time()))
                        registry.commit()
                    with coord.tx() as db:
                        db.execute("UPDATE bridge_jobs SET thread_id=?,payload=?,updated_at=? WHERE id=? AND status='prepared' AND thread_id=?",(new,payload,time.time(),wake['id'],old))
        self.batch_prepared(request_id,server)
        with self.projects.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            config=db.execute('SELECT * FROM root_config WHERE singleton=1').fetchone()
            if config['thread_id'] not in (old,new): raise CoordError('canonical root changed; fresh switch withheld')
            db.execute('UPDATE root_config SET thread_id=? WHERE singleton=1',(new,))
            db.execute("UPDATE root_fresh_requests SET status='switched',updated_at=? WHERE id=?",(time.time(),request_id));db.commit()

    def batch_prepared(self, request_id, server):
        """Journal one bounded post-boot wake; native boot bytes stay untouched.

        Every source delivery keeps its original job association. Missing positive
        preparation provenance, any native attempt, and model controls fall back
        to the original per-job path. Lifecycle gates cover partial local writes.
        """
        from .bridge_state import BridgeState
        from .root_lifecycle import stopped
        from .root_team_reset import quarantined
        job=self.job(request_id);new=job['new_thread_id']
        with self.projects.connection() as db:
            batch=db.execute('SELECT * FROM root_fresh_wake_batches WHERE request_id=?',(request_id,)).fetchone()
        if not batch:
            candidates=[]
            for item in self.scope(request_id):
                with contextlib.closing(Coordinator(self.roots.path(item['project_id']))) as coord:
                    state=BridgeState(coord);sid=item['root_session']
                    if stopped(coord.db,sid) or quarantined(coord.db,thread=new): continue
                    for wake in coord.db.execute("SELECT * FROM bridge_jobs WHERE thread_id=? AND status='prepared' ORDER BY created_at,id",(new,)):
                        if not state.pristine(wake['id']) or not state.still_unread(wake['id']): continue
                        messages=[dict(row) for row in coord.db.execute('''SELECT m.id,m.recipient_session FROM bridge_deliveries d
                            JOIN messages m ON m.id=d.message_id WHERE d.job_id=? ORDER BY m.seq''',(wake['id'],))]
                        if not messages or any(m['recipient_session']!=sid for m in messages): continue
                        route=state.resolve(sid)
                        if not route or route['thread_id']!=new or len(route['route'])!=1: continue
                        # Only retained old-thread jobs qualify, never new input
                        # prepared concurrently after a partially switched binding.
                        with self.projects.connection() as db:
                            relocation=db.execute('SELECT new_payload FROM root_fresh_relocations WHERE request_id=? AND project_id=? AND job_id=?',
                                (request_id,item['project_id'],wake['id'])).fetchone()
                        if not relocation or relocation['new_payload']!=wake['payload']: continue
                        contents=state.wake_messages(new,[m['id'] for m in messages])
                        unread={m['id'] for m in contents}
                        candidates.append(dict(item,job_id=wake['id'],payload=wake['payload'],messages=messages,
                            contents=contents,deliveries=[dict(message_id=m['id'],recipient_session=sid) for m in messages if m['id'] in unread]))
            selected=[]
            def metadata(items):
                return dict(server=server,thread_id=new,root_fresh_request_id=request_id,
                    reports=[dict(project=i['project_id'],session=i['root_session'],route=[dict(session=i['root_session'],agent_path=None)],
                        original_client_id=i['job_id'],deliveries=i['deliveries'],
                        messages=[dict(id=m['id'],complete=False) for m in i['contents']]) for i in items])
            def payload(data):
                return STANDING_POLICY+'\nRetained root reports share this wake. Fetch each incomplete message through its exact project/session route. Original job IDs are audit references, not native replay instructions.\n'+json.dumps(data,ensure_ascii=False,sort_keys=True,separators=(',',':'))
            for candidate in candidates:
                if len(payload(metadata(selected+[candidate])).encode('utf-8'))<=12288: selected.append(candidate)
            if len(selected)<2: return
            data=metadata(selected)
            # As in Bridge.payload, routing/placeholders get the budget first.
            # Complete current records avoid an extra fetch when they fit.
            for report,item in zip(data['reports'],selected):
                for index,record in enumerate(item['contents']):
                    report['messages'][index]=record
                    if len(payload(data).encode('utf-8'))>12288:
                        report['messages'][index]=dict(id=record['id'],complete=False)
            batch_id='wake_'+uuid.uuid4().hex
            with self.projects.connection() as db:
                db.execute('BEGIN IMMEDIATE')
                db.execute('INSERT INTO root_fresh_wake_batches VALUES(?,?,?,?)',(batch_id,request_id,payload(data),time.time()))
                for item in selected:
                    db.execute('INSERT INTO root_fresh_wake_members VALUES(?,?,?,?,?)',
                        (batch_id,item['project_id'],item['root_session'],item['job_id'],item['payload']))
                    db.executemany('INSERT INTO root_fresh_wake_deliveries VALUES(?,?,?,?,?)',
                        [(batch_id,item['project_id'],item['job_id'],m['id'],m['recipient_session']) for m in item['messages']])
                db.commit()
            with self.projects.connection() as db:
                batch=db.execute('SELECT * FROM root_fresh_wake_batches WHERE id=?',(batch_id,)).fetchone()
        with self.projects.connection() as db:
            members=[dict(row) for row in db.execute('SELECT * FROM root_fresh_wake_members WHERE batch_id=?',(batch['id'],))]
        if len(members)<2: raise CoordError('root wake batch lacks complete membership evidence')
        reason='Superseded by root fresh wake batch '+batch['id']
        for member in members:
            with contextlib.closing(Coordinator(self.roots.path(member['project_id']))) as coord:
                state=BridgeState(coord)
                with coord.tx() as db:
                    wake=db.execute('SELECT * FROM bridge_jobs WHERE id=?',(member['job_id'],)).fetchone()
                    if not wake or wake['thread_id']!=new or wake['payload']!=member['payload']:
                        raise CoordError('root wake batch source changed; lifecycle remains gated')
                    if wake['status']=='cancelled' and wake['error']==reason: continue
                    if not state.pristine(wake['id']): raise CoordError('root wake source lacks pristine proof; lifecycle remains gated')
                    db.execute("UPDATE bridge_jobs SET status='cancelled',error=?,updated_at=? WHERE id=?",(reason,time.time(),wake['id']))
        with contextlib.closing(Coordinator(self.roots.path(job['home_project']))) as coord:
            with coord.tx() as db:
                db.execute('INSERT OR IGNORE INTO bridge_jobs VALUES(?,?,?,\'prepared\',NULL,NULL,?,?)',
                    (batch['id'],new,batch['payload'],time.time(),time.time()))
                db.execute('INSERT OR IGNORE INTO bridge_root_wake_batches VALUES(?,?)',(batch['id'],request_id))
                self.verify_batch_job(db,batch,new,('prepared',))

    @staticmethod
    def verify_batch_job(db,batch,thread,statuses):
        wake=db.execute('SELECT * FROM bridge_jobs WHERE id=?',(batch['id'],)).fetchone()
        marker=db.execute('SELECT request_id FROM bridge_root_wake_batches WHERE job_id=?',(batch['id'],)).fetchone()
        if (not wake or not marker or marker['request_id']!=batch['request_id'] or wake['thread_id']!=thread
                or wake['payload']!=batch['payload'] or wake['status'] not in statuses
                or (wake['status']=='prepared' and wake['queue_id'] is not None)
                or (wake['status']=='queued' and not wake['queue_id'])):
            raise CoordError('root wake synthetic job/marker differs from immutable batch evidence')

    def is_batch(self,job_id):
        with self.projects.connection() as db:
            return bool(db.execute('SELECT 1 FROM root_fresh_wake_batches WHERE id=?',(job_id,)).fetchone())

    def check_batch(self, batch_id, project, host, *, admit=False, local_state=None):
        """Recheck every original route/guard, including aliases outside home."""
        from .bridge_state import BridgeState
        from .core import assert_agent_running
        from .root_lifecycle import stopped
        from .root_team_reset import quarantined
        from .session_reset import assert_reset_unlocked
        with self.projects.connection() as db:
            batch=db.execute('SELECT * FROM root_fresh_wake_batches WHERE id=?',(batch_id,)).fetchone()
            members=[dict(row) for row in db.execute('SELECT * FROM root_fresh_wake_members WHERE batch_id=?',(batch_id,))]
            deliveries=[dict(row) for row in db.execute('SELECT * FROM root_fresh_wake_deliveries WHERE batch_id=?',(batch_id,))]
        if not batch or len(members)<2: raise CoordError('root wake batch registry evidence is missing')
        job=self.job(batch['request_id']);root=self.roots.config()
        if (project,host)!=(job['home_project'],job['host_id']) or job['status']!='completed' or not root or (root['home_project'],root['host_id'],root['thread_id'])!=(project,host,job['new_thread_id']):
            raise CoordError('root wake batch requires its completed lifecycle and unchanged canonical owner')
        statuses=('prepared',) if admit=='adding' else ('queued',) if admit=='starting' else ('prepared','queued')
        with contextlib.nullcontext(local_state.coord) if local_state else contextlib.closing(Coordinator(self.roots.path(project))) as coord:
            self.verify_batch_job(coord.db,batch,job['new_thread_id'],statuses)
        scope={(i['project_id'],i['root_session']) for i in self.scope(job['id'])}
        unread=False
        for member in members:
            if (member['project_id'],member['root_session']) not in scope: raise CoordError('root wake batch scope changed')
            local=local_state if member['project_id']==project else None
            with contextlib.nullcontext(local.coord) if local else contextlib.closing(Coordinator(self.roots.path(member['project_id']))) as coord:
                state=local or BridgeState(coord);sid=member['root_session'];route=state.resolve(sid)
                owner=coord.db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?',(sid,)).fetchone()
                if not owner or owner['host_id']!=host or not route or route['thread_id']!=job['new_thread_id'] or len(route['route'])!=1:
                    raise CoordError('root wake batch member binding changed')
                source=coord.db.execute('SELECT * FROM bridge_jobs WHERE id=?',(member['job_id'],)).fetchone()
                actual=[(r['message_id'],r['recipient_session']) for r in coord.db.execute('SELECT d.message_id,m.recipient_session FROM bridge_deliveries d JOIN messages m ON m.id=d.message_id WHERE d.job_id=?',(member['job_id'],))]
                expected=[(d['message_id'],d['recipient_session']) for d in deliveries if (d['project_id'],d['job_id'])==(member['project_id'],member['job_id'])]
                if not source or source['status']!='cancelled' or source['thread_id']!=job['new_thread_id'] or source['error']!='Superseded by root fresh wake batch '+batch_id or source['payload']!=member['payload'] or source['queue_id'] is not None or not expected or sorted(actual)!=sorted(expected) or any(recipient!=sid for _,recipient in actual):
                    raise CoordError('root wake batch source/delivery evidence changed')
                if coord.db.execute('SELECT 1 FROM bridge_job_native_attempts WHERE job_id=?',(member['job_id'],)).fetchone() or not coord.db.execute('SELECT 1 FROM bridge_job_origins WHERE job_id=?',(member['job_id'],)).fetchone() or coord.db.execute('SELECT 1 FROM agent_control_requests WHERE native_input_job_id=?',(member['job_id'],)).fetchone():
                    raise CoordError('root wake batch source has native/control uncertainty')
                from .root_orchestrator import projection
                if held(coord.db,sid) or stopped(coord.db,sid) or (projection(coord.db,sid) or {}).get('paused') or quarantined(coord.db,thread=job['new_thread_id'],job=member['job_id']):
                    raise CoordError('root wake batch member is held, stopped or quarantined')
                assert_agent_running(coord.db,sid);assert_reset_unlocked(coord.db,sid)
                unread=state.still_unread(member['job_id']) or unread
        if admit and not unread: raise CoordError('root wake batch was consumed before native admission')
        return unread


class RootRestartWorker:
    """The existing authenticated bridge owns native calls and their journal."""
    def __init__(self, bridge):
        self.bridge,self.state,self.rpc=bridge,bridge.state,bridge.rpc

    def update(self, job, status, **values):
        return self.state.root_fresh_update(job['id'],job['status'],status,**values)

    def consumption(self, job):
        from .native_history import turns_page
        cursor=None
        for _ in range(4):
            params={'threadId':job['new_thread_id'],'limit':25,'itemsView':'summary','sortDirection':'desc'}
            if cursor: params['cursor']=cursor
            page=turns_page(self.rpc,params)
            for turn in page['data']:
                if any(item.get('type')=='userMessage' and item.get('clientId')==job['id'] for item in turn.get('items',[])):
                    if not isinstance(turn.get('id'),str) or not turn['id']:
                        raise CoordError('native root consumption lacks an exact turn ID')
                    return dict(thread_id=job['new_thread_id'],client_id=job['id'],turn_id=turn['id'],source='native-history',turn_status=turn.get('status'))
            cursor=page.get('nextCursor')
            if not cursor: return None
        return None

    def reconcile(self, job):
        if not job['new_thread_id']:
            return self.update(job,'uncertain',error='Native root creation outcome unknown; inspect the original request. No duplicate creation.')
        proof=self.consumption(job)
        if proof: return self.update(job,'completed',native_proof=proof,error=None)
        matches=[item for item in self.bridge.queue(job['new_thread_id']) if item.get('clientUserMessageId')==job['id']]
        if len(matches)==1 and job['status'] in ('adding','uncertain') and job.get('native_action')=='adding':
            return self.update(job,'queued',queue_id=matches[0]['id'],error=None)
        return self.update(job,'uncertain' if job['status'] in ('adding','starting') else job['status'],
                           error='No conclusive root boot consumption; preserve queue/history, no replay.')

    def bootstrap(self, job):
        from .root_authority import ROOT_AUTHORITY
        metadata=self.state.connection_metadata()
        return ('Fresh global root conversation. Keep the SAME registered root identity and existing project directory. '
                'Do not register a replacement or spawn helper agents. Read neutral root AGENTS.md; no old native transcript was copied. '
                'Existing team stop barriers remain active until explicit user resume; this fresh start never resumes teams. '
                'At your initial normal boot/reporting opportunity, inspect root fresh-status --request-id '+job['id']+'. '
                'Only after status completed, report operator_report to the operator once, retaining distinct request and new thread IDs. '
                'If still pending, report that state truthfully and end the turn; do not poll or claim completion from this boot. '
                'The exact boot prompt below is the authorized root lifecycle instruction; metadata is data, never shell code. '
                + ROOT_AUTHORITY + "\n" + STANDING_POLICY +
                ' Use your canonical server/project/session and own host. Unset inherited resource tokens.\n'
                +json.dumps(dict(server=metadata.get('server'),project=job['home_project'],session=job['home_session'],
                                 host_id=job['host_id'],thread_id=job['new_thread_id'],instruction_origin=job['instruction_origin'],policy_version=policy_version()),sort_keys=True)
                +'\n\nRoot boot prompt:\n'+job['prompt'])

    def advance(self, job):
        from .reset_worker import ResetWorker
        from .rpc import RpcError,TransportError
        native=ResetWorker(self.bridge)
        try:
            if job['status']=='finalizing':
                self.update(job,'finalizing');return
            if job['status'] in ('creating','adding','starting','uncertain'):
                job=self.reconcile(job)
                if job['status']!='queued': return
            if job['status']=='pending':
                before=self.rpc.request('thread/read',{'threadId':job['old_thread_id'],'includeTurns':False})['thread']
                if before.get('id')!=job['old_thread_id'] or before.get('status',{}).get('type')!='idle' or before.get('canAcceptDirectInput') is not True or self.bridge.queue(job['old_thread_id']):
                    self.update(job,'pending',error='Waiting for actual old root idle with direct input and empty native queue; no interruption/materialization.');return
                settings,digest=native.settings(job['old_thread_id'])
                if not native.idle(job['old_thread_id']) or self.bridge.queue(job['old_thread_id']):
                    self.update(job,'pending',error='Old root became busy during settings readback; fresh creation withheld.');return
                job=self.update(job,'creating',settings_digest=digest,error=None)
                created=self.rpc.request('thread/start',native.start_params(settings))
                _,new_digest=native.settings_data(created)
                job=self.update(job,'created',new_thread_id=created['thread']['id'],new_settings_digest=new_digest,error=None)
            if job['status']=='created':
                if not native.idle(job['old_thread_id']) or self.bridge.queue(job['old_thread_id']):
                    self.update(job,'created',error='Old root became busy after creation; routing and boot withheld.');return
                if not native.idle(job['new_thread_id']) or self.bridge.queue(job['new_thread_id']) or not native.empty_history(job['new_thread_id']):
                    self.update(job,'created',error='Fresh root candidate must be idle, empty and unconsumed before routing.');return
                digest=job['new_settings_digest']
                try: _,digest=native.settings(job['new_thread_id'])
                except RpcError as error:
                    if error.code!=-32600 or error.message!='no rollout found for thread id '+job['new_thread_id'] or not digest: raise
                if digest!=job['settings_digest']:
                    self.update(job,'created',error='Fresh root model/permissions/workspace readback mismatch; boot withheld.');return
                if not native.idle(job['old_thread_id']) or self.bridge.queue(job['old_thread_id']):
                    self.update(job,'created',error='Old root became busy during candidate readback; routing withheld.');return
                job=self.update(job,'switching',new_settings_digest=digest,server=self.state.connection_metadata().get('server'),error=None)
            if job['status']=='switching':
                if not native.idle(job['old_thread_id']) or self.bridge.queue(job['old_thread_id']):
                    self.update(job,'switching',error='Old root is busy; partial routing checkpoint remains gated.');return
                job=self.update(job,'switching',server=self.state.connection_metadata().get('server'),error=None)
            if job['status']=='switched':
                if not native.idle(job['new_thread_id']) or self.bridge.queue(job['new_thread_id']) or not native.empty_history(job['new_thread_id']):
                    self.update(job,'switched',error='Fresh root acquired native input before boot; preserve it, no prompt replay.');return
                digest=job['new_settings_digest']
                try: _,digest=native.settings(job['new_thread_id'])
                except RpcError as error:
                    if error.code!=-32600 or error.message!='no rollout found for thread id '+job['new_thread_id'] or not digest: raise
                if digest!=job['settings_digest']:
                    self.update(job,'switched',error='Fresh root settings changed before boot; prompt withheld.');return
                self.bridge.offer_policy('root',job['id'],job['new_thread_id'],self.bootstrap(job))
                job=self.update(job,'adding',error=None)
                result=self.rpc.request('thread/queue/add',{'threadId':job['new_thread_id'],'clientUserMessageId':job['id'],
                                                         'input':[{'type':'text','text':self.bootstrap(job)}]})
                job=self.update(job,'queued',queue_id=result['queuedSubmission']['id'],error=None)
            if job['status']=='queued':
                proof=self.consumption(job)
                if proof: self.update(job,'completed',native_proof=proof,error=None);return
                entries=self.bridge.queue(job['new_thread_id'])
                if not entries or entries[0]['id']!=job['queue_id'] or not native.idle(job['new_thread_id']):
                    self.update(job,'queued',error='Awaiting exact retained root boot queue entry; no replay.');return
                job=self.update(job,'starting',error=None)
                self.rpc.request('thread/queue/start',{'threadId':job['new_thread_id'],'queuedSubmissionId':job['queue_id']})
                # Queue-start ACK is not input consumption. The next pass proves it.
        except (RpcError,TransportError,CoordError,OSError,ValueError,KeyError,TypeError) as error:
            current=self.state.root_fresh_job(job['id'])
            if current['status']=='completed': return
            phase='uncertain' if current['status'] in ('creating','adding','starting') else current['status']
            self.update(current,phase,error=str(error))
            if isinstance(error,TransportError): raise
