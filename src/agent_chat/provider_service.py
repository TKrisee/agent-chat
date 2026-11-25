"""One-time external-to-owning handoff and subsequent owning-client restart.

The bridge owns Popen; this controller observes it and never invents/transfers a
child handle. A transient launchd job runs the controller independently.
"""
from contextlib import closing
import hashlib,os,plistlib,pwd,re,select,shlex,signal,sqlite3,subprocess,time
from pathlib import Path
from . import service,native_owner_evidence as native
from .client_continuity import ClientContinuity
from . import provider_owner as owner
from . import provider_recovery_lifecycle as lifecycle
from . import provider_recovery_peer as peer
from .provider_recovery_wire import jq,field
Blocked=peer.Blocked

def configured(manager):
 paths=manager.paths('client')
 raw=peer.trusted_file(paths['wrapper'],os.getuid())
 lines=[line[5:] for line in raw.decode().splitlines() if line.startswith('exec ')]
 command=shlex.split(lines[0]) if len(lines)==1 else []
 if len(command)<3 or command[2]!='bridge':raise Blocked('configured managed client command unavailable')
 return raw,command

def option(command,name):
 if command.count(name)!=1 or command.index(name)+1>=len(command):raise Blocked('exact configured '+name+' unavailable')
 return command[command.index(name)+1]

class OwningContinuity(ClientContinuity):
 """Separate owning inspector; original connect-only predicate is untouched."""
 def _config(self):
  paths=self.manager.paths('client');raw=self._file(paths['config']);wrapper,command=configured(self.manager)
  config=plistlib.loads(raw)
  if (config.get('Label')!=service.LABEL+'.client' or config.get('ProgramArguments')!=[str(paths['wrapper'])]
      or config.get('KeepAlive') is not True or '--connect-only' in command):raise Blocked('exact managed owning client configuration differs')
  path=option(command,'--managed-provider-manifest');pin=option(command,'--managed-provider-sha256')
  owner.manifest(path,pin,option(command,'--codex-server'))
  return hashlib.sha256(raw+b'\0'+wrapper).hexdigest(),command
 def inspect(self):
  result=super().inspect();result['mode']='owning';return result

class LegacyOwningContinuity(ClientContinuity):
 """Existing owning wrapper, before managed launch receipts are introduced."""
 def _config(self):
  paths=self.manager.paths('client');raw=self._file(paths['config']);wrapper,command=configured(self.manager)
  config=plistlib.loads(raw)
  if (config.get('Label')!=service.LABEL+'.client' or config.get('ProgramArguments')!=[str(paths['wrapper'])]
      or config.get('KeepAlive') is not True or '--connect-only' in command
      or '--managed-provider-manifest' in command or '--managed-provider-sha256' in command):
   raise Blocked('legacy owning client configuration differs')
  option(command,'--codex-bin');native.endpoint(option(command,'--codex-server'))
  return hashlib.sha256(raw+b'\0'+wrapper).hexdigest(),command
 def inspect(self):
  result=super().inspect();result['mode']='legacy-owning';return result

def continuity(manager):
 _,command=configured(manager)
 if '--connect-only' in command:return ClientContinuity(manager)
 return OwningContinuity(manager) if '--managed-provider-manifest' in command else LegacyOwningContinuity(manager)

class HandoffActors(lifecycle.Actors):
 allow_retained_cli=True
 def continuity(self,manager):return continuity(manager)
 def cli_evidence(self,pid,identity,image,argv,owner_identity):
  try:return super().cli_evidence(pid,identity,image,argv,owner_identity)
  except native.EvidenceError as original:
   if not self.allow_retained_cli or identity[:3]!=(pid,os.getuid(),os.geteuid()):
    raise
   try:
    generation=pid,identity[1],identity[3],identity[4]
    return owner.configured_cli_attestation(self.selected.endpoint,generation,image,argv,owner_identity)
   except (Blocked,OSError,ValueError):
    raise Blocked('canonical Root CLI signer unavailable and managed CLI provenance was not verified') from original

class ObservedBridgeChild:
 """Kernel and durable launch evidence, deliberately not a Popen handle."""
 def __init__(self,process,parent,ledger,serial):
  self.pid=process.pid;self.generation=(process.pid,process.uid,process.birth_sec,process.birth_usec)
  self.parent=parent;self.ledger=ledger;self.serial=serial
 def poll(self):
  if owner.absent(self.generation):return 1
  process=native.DarwinEvidence().process(self.pid)
  if process.parent!=self.parent[0] or owner.identity(self.parent[0])!=self.parent:raise Blocked('managed replacement parent generation changed')
  with closing(peer.readonly(self.ledger)) as db:
   row=db.execute('SELECT state,parent_pid,parent_uid,parent_sec,parent_usec,child_pid,child_uid,child_sec,child_usec FROM child_launches WHERE serial=?',(self.serial,)).fetchone()
  if row!=('started',*self.parent,*self.generation):raise Blocked('managed child launch evidence changed')
  return None

class HandoffRecovery(lifecycle.AdministrativeRecovery):
 def __init__(self,selected,manifest,manifest_sha,authority):
  self.manager=service.ServiceManager(system='Darwin',home=Path(pwd.getpwuid(selected.uid).pw_dir))
  self.original_wrapper,self.original_command=configured(self.manager)
  self.owning='--connect-only' not in self.original_command
  self.dynamic_replacement_image=self.owning
  super().__init__(selected,manifest,manifest_sha,authority)
  self.actors=HandoffActors(self.authority,self.selected)
  self.legacy=self.owning and '--managed-provider-manifest' not in self.original_command
  if self.authority.source_kind=='configured_local_service_owner' and not self.owning:raise Blocked('local owning-service authority cannot select an external provider')
  self.client_sent=False;self.old_client=None;self.original_root_cli=None
  if self.owning and not self.legacy:
   if (option(self.original_command,'--managed-provider-manifest')!=str(self.manifest)
       or option(self.original_command,'--managed-provider-sha256')!=self.manifest_sha256):raise Blocked('owning client launch manifest differs')
 def selection(self):
  super().selection()
  if self.old_client is None:
   app=self.actor_proof[0];self.old_client=(app[0],app[1],app[3],app[4]);self.original_root_cli=self.actor_proof[4]
  if self.legacy:
   owner.legacy_chain((self.selected.pid,self.selected.uid,self.selected.birth_sec,self.selected.birth_usec),self.old_client)
  elif self.owning:
   process=native.DarwinEvidence().process(self.selected.pid)
   if process.parent!=self.old_client[0]:raise Blocked('selected provider is not actual configured client child')
   self._owned_record(self.selected.pid,self.old_client)
 def _ledger(self):return self.manifest.parent/'owned-provider.sqlite3'
 def _owned_record(self,pid,parent):
  peer.trusted_file(self._ledger(),os.getuid(),contents=False)
  with closing(peer.readonly(self._ledger())) as db:
   config=db.execute('SELECT pin,endpoint FROM owner_config WHERE singleton=1').fetchone()
   row=db.execute('SELECT serial,state,parent_pid,parent_uid,parent_sec,parent_usec,child_pid,child_uid,child_sec,child_usec FROM child_launches ORDER BY serial DESC LIMIT 1').fetchone()
  if config!=(self.manifest_sha256,self.selected.endpoint) or not row or row[1]!='started' or tuple(row[2:6])!=parent or row[6]!=pid:
   raise Blocked('actual configured owning child receipt unavailable')
  if tuple(row[6:])!=owner.identity(pid):raise Blocked('owned child generation differs')
  return row
 def _signal_client(self):
  if self.client_sent:raise Blocked('client signal already claimed')
  self.client_sent=True
  if owner.identity(self.old_client[0])!=self.old_client:raise Blocked('client generation changed before TERM')
  if time.monotonic()>=self.deadline:raise TimeoutError('deadline before client TERM')
  # Positive exact generation, with the same disclosed Darwin signal-CAS limit.
  os.kill(self.old_client[0],signal.SIGTERM)
 def stop_once(self):
  if not self.owning:return super().stop_once()
  if self.term_claimed or self.watch is None:raise Blocked('stop already claimed or provider watcher absent')
  self.term_claimed=True;self.selection()
  if self.legacy:
   with owner.LaunchLedger(self._ledger(),self.manifest_sha256,self.selected.endpoint) as ledger:
    owner.adopt_legacy(ledger,(self.selected.pid,self.selected.uid,self.selected.birth_sec,self.selected.birth_usec),self.old_client)
   self._activate_wrapper()
   peer.selected_kernel(self.selected)
  self._signal_client()
 def closed(self):
  if not self.owning:return super().closed()
  if not self.closed_observed:
   events=self.watch.control(None,1,0)
   self.closed_observed=any(e.ident==self.selected.pid and e.fflags&select.KQ_NOTE_EXIT and not e.flags&select.KQ_EV_ERROR for e in events)
  if not self.closed_observed:return False
  generation=(self.selected.pid,self.selected.uid,self.selected.birth_sec,self.selected.birth_usec)
  if not owner.absent(generation):return False
  if self.legacy:
   if not owner.absent(self.old_client):return False
   with owner.LaunchLedger(self._ledger(),self.manifest_sha256,self.selected.endpoint) as ledger:
    try:ledger.reconcile()
    except Blocked:return False
  # Actual bridge cleanup records closure only after retained Popen reap and
  # empty selected listener. A successor may now legitimately own that port.
  with closing(peer.readonly(self._ledger())) as db:
   row=db.execute('SELECT state FROM child_launches WHERE child_pid=? AND child_uid=? AND child_sec=? AND child_usec=?',generation).fetchone()
  if row in (('started',),('release_intent',)):return False
  if row!=('closed',):raise Blocked('old owned provider cleanup state is unavailable')
  return True
 def _activate_wrapper(self):
  path=self.manager.paths('client')['wrapper']
  if peer.trusted_file(path,os.getuid())!=self.original_wrapper:raise Blocked('client wrapper changed before activation')
  command=list(self.original_command)
  if command.count('--connect-only')!=(0 if self.legacy else 1):raise Blocked('handoff source mode changed')
  if '--connect-only' in command:command.remove('--connect-only')
  command+=['--managed-provider-manifest',str(self.manifest),'--managed-provider-sha256',self.manifest_sha256]
  lines=self.original_wrapper.decode().splitlines()
  changed='\n'.join('exec '+' '.join(shlex.quote(x) for x in command) if line.startswith('exec ') else line for line in lines)+'\n'
  publish(self.manifest.parent/'client-owning.sh',changed.encode())
  fd=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW);tmp='.provider-owning-'+str(os.getpid())
  try:
   out=os.open(tmp,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o700,dir_fd=fd)
   with os.fdopen(out,'wb') as stream:stream.write(changed.encode());stream.flush();os.fsync(stream.fileno())
   if peer.trusted_file(path,os.getuid())!=self.original_wrapper:raise Blocked('client wrapper changed during activation')
   os.replace(tmp,path.name,src_dir_fd=fd,dst_dir_fd=fd);os.fsync(fd)
  finally:
   try:os.unlink(tmp,dir_fd=fd)
   except FileNotFoundError:pass
   os.close(fd)
 def start_once(self):
  if self.launch_claimed or not self.closed():raise Blocked('duplicate start or exact old provider closure unproven')
  self.launch_claimed=True
  if not self.owning:
   # Existing client still existed for every original provider pre-TERM check.
   current=ClientContinuity(self.manager).inspect()
   if not current['process'] or current['process']['pid']!=self.old_client[0] or owner.identity(self.old_client[0])!=self.old_client:
    raise Blocked('original connect-only client changed after provider closure')
   self._activate_wrapper();self._signal_client()
  while time.monotonic()<self.deadline:
   try:
    inspected=OwningContinuity(self.manager).inspect();pid=inspected['process']['pid'] if inspected['process'] else None
    if pid and pid!=self.old_client[0] and owner.absent(self.old_client):
     parent=owner.identity(pid);host,port=native.endpoint(self.selected.endpoint)
     rows=native.DarwinEvidence.lsof('-iTCP@'+host+':'+str(port),'-sTCP:LISTEN')
     if len(rows)==1:
      process=native.DarwinEvidence().process(rows[0][0]);receipt=self._owned_record(process.pid,parent)
      if process.parent!=pid:raise Blocked('replacement provider parent differs')
      self.child=ObservedBridgeChild(process,parent,self._ledger(),receipt[0]);self.child.poll();return self.child.pid
   except (native.EvidenceError,service.ServiceError,FileNotFoundError):pass
   time.sleep(.05)
  raise TimeoutError('same managed client did not provide a new owned provider')
 def restored_connections(self):
  next_actors=HandoffActors(self.authority,self.selected)
  if getattr(self,'_resume_receipt_recorded',False) and not getattr(self,'reattachment_context',None):
   next_actors.pending_client_transition=(self.old_client[0],self.child.parent[0])
  while True:
   if getattr(next_actors,'pending_client_transition',None) and time.monotonic()>=self.deadline:raise TimeoutError('deadline waiting for previous client registration to transition')
   try:actual=next_actors.observe();break
   except (lifecycle.PreviousClientRegistrationPending,lifecycle.CurrentOwnedClientNotReady):
    if time.monotonic()>=self.deadline:raise TimeoutError('deadline waiting for previous client registration to transition')
    if not owner.absent(self.old_client) or self.child.poll() is not None:raise Blocked('owned client transition changed')
    self.authority.check()
    time.sleep(min(.05,max(0,self.deadline-time.monotonic())))
  if getattr(next_actors,'pending_client_transition',None) and time.monotonic()>=self.deadline:raise TimeoutError('deadline during restored client proof')
  if getattr(self,'reattachment_context',None) is not None:
   from .provider_reattachment import record_owner
   journal,request=self.reattachment_context
   record_owner(self,journal,request,actual,next_actors)
  elif actual[4][:3]!=self.original_root_cli[:3] or actual[4][4]!=self.original_root_cli[4]:raise Blocked('canonical Root CLI changed across owning-client handoff')
  if (actual[0][0],actual[0][1],actual[0][3],actual[0][4])!=self.child.parent:raise Blocked('restored App is not replacement provider owner')
  self.actor_proof=actual;self.actors=next_actors

# Service entry: request artifacts live beside the configured master, never an
# environment-selected Root. No secret values or old native inputs are stored.
def home_directory():
 config=service._installed_server_config()
 return config.master.parent/'provider-owning'

def private_directory(path):
 path=Path(path)
 for parent in reversed(path.parents):
  if parent.is_symlink():raise Blocked('unsafe service handoff ancestor')
 path.mkdir(mode=0o700,exist_ok=True)
 info=path.lstat()
 if not path.is_dir() or info.st_uid!=os.getuid() or info.st_mode&0o077:raise Blocked('service handoff directory is not owner private')
 return path

def publish(path,raw):
 """Exclusive durable artifact; never overwrite an existing different plan."""
 fd=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
 try:
  try:out=os.open(path.name,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600,dir_fd=fd)
  except FileExistsError:
   if peer.trusted_file(path,os.getuid())!=raw:raise Blocked('existing handoff artifact differs')
  else:
   with os.fdopen(out,'wb') as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())
   os.fsync(fd)
 finally:os.close(fd)

def operation_db():
 directory=private_directory(home_directory());journal=peer.v1.Journal(directory/'service-requests.sqlite3')
 journal.db.execute('CREATE TABLE IF NOT EXISTS service_requests(id TEXT PRIMARY KEY,source TEXT,source_sha TEXT,manifest TEXT,pin TEXT,uid INTEGER,pid INTEGER,birth_sec INTEGER,birth_usec INTEGER,endpoint TEXT,restart_server INTEGER,state TEXT,detail TEXT,worker_pid INTEGER,worker_sec INTEGER,worker_usec INTEGER,UNIQUE(uid,pid,birth_sec,birth_usec))')
 journal.db.commit();return journal

def result(row):
 return jq('{}','{request_id:$id,state:$state,detail:$detail,source_kind:$kind,ready:false,no_replay:true}',id=row[0],state=row[11],detail=row[12],kind='configured_local_service_owner' if row[1]==LOCAL_SOURCE else 'authenticated_operator_ui')

def selected_listener(address):
 host,port=native.endpoint(address);rows=native.DarwinEvidence.lsof('-iTCP@'+host+':'+str(port),'-sTCP:LISTEN')
 if len(rows)!=1 or rows[0][2]!=host+':'+str(port):raise Blocked('exact configured native listener unavailable')
 # An administratively selected OLD image can be unavailable/unverified.
 # Kernel generation is sufficient for selection; replacement peer guards still
 # require their full image/signature evidence before any trusted native RPC.
 pid,uid,ruid,sec,usec=peer.kernel_identity(rows[0][0])
 if uid!=os.getuid() or uid!=os.geteuid() or ruid!=uid:raise Blocked('selected old real/effective UID differs')
 selected=peer.old_selection(uid,pid,sec,usec,address)
 peer.selected_kernel(selected)
 return selected

def _stage_service_request(db, request, stage):
 """Serialize no-effect capture and durably claim the one native submission."""
 db.execute('BEGIN IMMEDIATE')
 try:
  row=db.execute('SELECT state,worker_pid,worker_sec,worker_usec FROM service_requests WHERE id=?',(request,)).fetchone()
  if not row:raise Blocked('unknown service handoff request')
  if row[0]=='staging' and any(value is not None for value in row[1:]):
   raise Blocked('staging contains a worker identity; effects require reconciliation')
  if row[0]!='staging':
   db.commit();return False
  for attempt in range(3):
   try:
    staged=stage();break
   except Blocked as error:
    if str(error)!='canonical SQLite row changed during capture' or attempt==2:raise
  if staged.state!='staged':raise Blocked('same Root checkpoint not staged: '+staged.reason)
  changed=db.execute("UPDATE service_requests SET state='submit_intent' WHERE id=? AND state='staging' AND worker_pid IS NULL AND worker_sec IS NULL AND worker_usec IS NULL",(request,)).rowcount
  if changed!=1:raise Blocked('service staging ownership changed')
  db.commit();return True
 except BaseException:
  db.rollback();raise

def submit(manager,args):
 import uuid
 directory=private_directory(home_directory());profile_path=directory/'profile.plist'
 if args.provider_manifest:
  if args.provider_source_id==LOCAL_SOURCE:raise Blocked('configured service authority is not an operator-message ID')
  if not args.provider_manifest_sha256 or not args.provider_source_id:raise Blocked('first handoff requires existing manifest pin and authenticated repair source')
  raw=peer.trusted_file(Path(args.provider_manifest),os.getuid())
  if hashlib.sha256(raw).hexdigest()!=args.provider_manifest_sha256:raise Blocked('supplied replacement manifest differs')
  manifest_path=directory/'provider.plist';source=args.provider_source_id;pin=args.provider_manifest_sha256
  profile=dict(Manifest=str(manifest_path),SHA256=pin,Source=source)
 else:
  profile=plistlib.loads(peer.trusted_file(profile_path,os.getuid())) if profile_path.exists() else prepare_legacy_profile(manager)
  source=profile['Source'];pin=profile['SHA256'];manifest_path=Path(profile['Manifest'])
  if source==LOCAL_SOURCE and profile.get('SourceKind')!='configured_local_service_owner':raise Blocked('local service profile provenance is unclassified')
  raw=peer.trusted_file(manifest_path,os.getuid())
 authority=authority_for_handoff(source)
 wrapper,command=configured(manager);address=option(command,'--codex-server')
 if args.provider_manifest:
  # Validate supplied bytes before publishing the fixed configuration artifact.
  owner.manifest(Path(args.provider_manifest),pin,address,require_manifest_image=True)
  publish(manifest_path,raw);publish(profile_path,plistlib.dumps(profile))
  if '--connect-only' in command:publish(directory/'client-before.sh',wrapper)
 owner.manifest(manifest_path,pin,address)
 if args.request_id:
  existing=operation_db()
  try:
   prior=existing.db.execute('SELECT * FROM service_requests WHERE id=?',(args.request_id,)).fetchone()
   if prior:
    if prior[1:5]!=(source,authority.source_digest,str(manifest_path),pin) or prior[10]!=int(args.component=='all'):
     raise Blocked('same request conflicts with immutable service operation')
    if prior[11]!='staging':return result(prior)
    selected=selected_listener(address)
    if prior[5:10]!=(selected.uid,selected.pid,selected.birth_sec,selected.birth_usec,address):
     raise Blocked('staged provider generation or endpoint changed')
  finally:existing.close()
 selected=selected_listener(address)
 journal=operation_db();db=journal.db
 try:
  db.execute('BEGIN IMMEDIATE')
  previous=db.execute('SELECT * FROM service_requests WHERE uid=? AND pid=? AND birth_sec=? AND birth_usec=?',(selected.uid,selected.pid,selected.birth_sec,selected.birth_usec)).fetchone()
  if previous:
   if previous[1:5]!=(source,authority.source_digest,str(manifest_path),pin):raise Blocked('selected generation belongs to another immutable handoff')
   # Preserve normal status readback for other invocations. Only the exact
   # original request can resume capture before the submission intent.
   if previous[0]!=args.request_id or previous[11]!='staging':
    db.commit();return result(previous)
   if previous[10]!=int(args.component=='all') or previous[9]!=address:
    raise Blocked('staged generation belongs to another operation')
   request=previous[0]
  else:
   pending=db.execute("SELECT id FROM service_requests WHERE endpoint=? AND state!='completed'",(address,)).fetchone()
   if pending:raise Blocked('existing service handoff remains unknown: '+pending[0])
   request=args.request_id or str(uuid.uuid4())
   if str(uuid.UUID(request))!=request:raise Blocked('request ID must be a canonical UUID')
   db.execute('INSERT INTO service_requests VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,NULL,NULL)',(request,source,authority.source_digest,str(manifest_path),pin,selected.uid,selected.pid,selected.birth_sec,selected.birth_usec,address,int(args.component=='all'),'staging','known prior effects remain UNKNOWN'))
  db.commit()
  def stage_current():
   if authority_for_handoff(source).source_digest!=authority.source_digest or selected_listener(address)!=selected:
    raise Blocked('service source or provider generation changed before capture')
   owner.manifest(manifest_path,pin,address)
   staged=lifecycle.stage_local_checkpoint(request,source,selected,manifest_path,pin,_service_handoff=True)
   if authority_for_handoff(source).source_digest!=authority.source_digest or selected_listener(address)!=selected:
    raise Blocked('service source or provider generation changed during capture')
   owner.manifest(manifest_path,pin,address)
   return staged
  if not _stage_service_request(db,request,stage_current):
   return result(db.execute('SELECT * FROM service_requests WHERE id=?',(request,)).fetchone())
  service_bin=str(Path(service.__file__).resolve().parents[2]/'bin/agent-chat-service')
  job_path=directory/(request+'.plist')
  job=dict(Label=service.LABEL+'.handoff.'+request,ProgramArguments=[manager.python,service_bin,'provider-handoff-run','--request-id',request],RunAtLoad=True,KeepAlive=False,WorkingDirectory=str(directory),StandardOutPath=str(directory/(request+'.out.log')),StandardErrorPath=str(directory/(request+'.err.log')))
  publish(job_path,plistlib.dumps(job))
  command=['launchctl','bootstrap','gui/'+str(os.getuid()),str(job_path)]
  answer=manager.run(command,check=False,capture_output=True,text=True,timeout=10)
  with db:db.execute("UPDATE service_requests SET detail=? WHERE id=?",('launchd accepted; completion requires status' if answer.returncode==0 else 'launchd submit outcome unknown; '+answer.stderr[:500],request))
  return result(db.execute('SELECT * FROM service_requests WHERE id=?',(request,)).fetchone())
 except BaseException:
  db.rollback();raise
 finally:journal.close()

def status(request):
 journal=operation_db()
 try:
  row=journal.db.execute('SELECT * FROM service_requests WHERE id=?',(request,)).fetchone()
  if row is None:raise Blocked('unknown service handoff request')
  authority=authority_for_handoff(row[1])
  if authority.source_digest!=row[2]:raise Blocked('service handoff source changed')
  return result(row)
 finally:journal.close()

def independently_hosted(manager,request,*,force=False):
 """Actual launchd job/parent evidence; a detached shell label is insufficient."""
 kind='force' if force else 'handoff'
 action='provider-handoff-force-run' if force else 'provider-handoff-run'
 target='gui/'+str(os.getuid())+'/'+service.LABEL+'.'+kind+'.'+request
 checked=manager.run(['launchctl','print',target],check=False,capture_output=True,text=True,timeout=5)
 if checked.returncode or checked.stderr or re.findall(r'^\s*pid = (\d+)\s*$',checked.stdout,re.M)!=[str(os.getpid())]:raise Blocked('worker is not exact independently hosted launchd job')
 job_path=home_directory()/(request+('.force.plist' if force else '.plist'));job=plistlib.loads(peer.trusted_file(job_path,os.getuid()))
 service_bin=str(Path(service.__file__).resolve().parents[2]/'bin/agent-chat-service')
 command=job.get('ProgramArguments',[])
 if (job.get('Label')!=service.LABEL+'.'+kind+'.'+request or job.get('RunAtLoad') is not True or job.get('KeepAlive') is not False
     or len(command)!=5 or command[1:]!=[service_bin,action,'--request-id',request]
     or re.findall(r'^\s*path = (.+?)\s*$',checked.stdout,re.M)!=[str(job_path)]):raise Blocked('loaded transient controller configuration differs')
 inspector=ClientContinuity(manager);image,argv=inspector._kernel_command(os.getpid())
 inspector._qualified_image(command[0],image)
 if argv[1:]!=command[1:]:raise Blocked('controller kernel argv differs from configured request')
 process=native.DarwinEvidence().process(os.getpid())
 if process.parent!=1 or process.uid!=os.getuid() or process.ruid!=os.getuid():raise Blocked('controller is not independently parented by launchd')
 return process

def run_worker(manager,request):
 journal=operation_db();db=journal.db
 try:
  process=independently_hosted(manager,request)
  db.execute('BEGIN IMMEDIATE');row=db.execute('SELECT * FROM service_requests WHERE id=?',(request,)).fetchone()
  if row is None:raise Blocked('unknown submitted handoff')
  if row[11]!='submit_intent':db.commit();return result(row)
  authority=authority_for_handoff(row[1])
  if authority.source_digest!=row[2]:raise Blocked('operator source changed before worker admission')
  db.execute("UPDATE service_requests SET state='running',worker_pid=?,worker_sec=?,worker_usec=? WHERE id=?",(process.pid,process.birth_sec,process.birth_usec,request));db.commit()
  selected=peer.old_selection(row[5],row[6],row[7],row[8],row[9])
  outcome=lifecycle.recover_uncertain(request,row[1],selected,Path(row[3]),row[4],_service_handoff=True)
  restored=outcome.state in ('restored_known_local_state_prior_effects_unknown','restored_checkpoint_old_state_unknown')
  with db:db.execute('UPDATE service_requests SET state=?,detail=? WHERE id=?',('provider_restored' if restored else 'outcome_unknown',outcome.state+': '+outcome.reason,request))
  if restored:complete_server_restart(manager,db,request)
  return result(db.execute('SELECT * FROM service_requests WHERE id=?',(request,)).fetchone())
 except BaseException as error:
  db.rollback()
  with db:db.execute("UPDATE service_requests SET detail=? WHERE id=?",(type(error).__name__+': '+str(error),request))
  raise
 finally:journal.close()

def reconcile(manager,request):
 """Same-request kernel reconciliation; no signals, input or journal reset."""
 journal=operation_db()
 try:
  row=journal.db.execute('SELECT * FROM service_requests WHERE id=?',(request,)).fetchone()
  if row is None:raise Blocked('unknown service handoff request')
  authority=authority_for_handoff(row[1])
  if authority.source_digest!=row[2]:raise Blocked('service handoff source changed')
  outcome=peer.v1.reconcile_private(authority.master.parent/'provider-recovery-v3/requests.sqlite3',request)
  path=Path(row[3]).parent/'owned-provider.sqlite3';observed='no managed launch receipt exists'
  if path.exists():
   owner.manifest(Path(row[3]),row[4],row[9])
   with owner.LaunchLedger(path,row[4],row[9]) as ledger:
    try:observed=ledger.reconcile()
    except Blocked as error:observed='UNKNOWN: '+str(error)
  authority.check()
  if row[11] in ('outcome_unknown','provider_restored'):
   observed += '; '+finish_reconciled_start(manager,row,authority,journal)
   row=journal.db.execute('SELECT * FROM service_requests WHERE id=?',(request,)).fetchone()
  with journal.db:
   journal.db.execute('CREATE TABLE IF NOT EXISTS service_reconciliations(request_id TEXT,observed_at REAL,old_generation TEXT,listener TEXT,owned_launch TEXT)')
   journal.db.execute('INSERT INTO service_reconciliations VALUES(?,?,?,?,?)',(request,time.time(),outcome.old_generation,outcome.listener,observed))
  return jq(result(row),'.+{old_generation:$old,listener:$listener,owned_launch:$owned,native_input_outcomes:"UNKNOWN"}',old=outcome.old_generation,listener=outcome.listener,owned=observed)
 finally:journal.close()


def wait(request,timeout=120):
 deadline=time.monotonic()+timeout
 while True:
  raw=status(request);state=field(raw,'.state')
  if state in ('completed','outcome_unknown'):return raw
  if time.monotonic()>=deadline:return jq(raw,'.+{deadline_reached:true}')
  time.sleep(min(.1,max(0,deadline-time.monotonic())))

def finish_reconciled_start(manager,row,authority,service_journal,timeout=120):
 """Verify an observed managed child after a safe launch reconciliation.

 No stop/start command is replayed. Resume is issued only when the old durable
 operation never reached resume_intent; an unknown resume remains unreplayed.
 """
 import ast
 journal=peer.v1.Journal(authority.master.parent/'provider-recovery-v3/requests.sqlite3')
 request=row[0];deadline=time.monotonic()+timeout;adapter=None
 try:
  operation=journal.db.execute('SELECT state,effects FROM requests WHERE id=?',(request,)).fetchone()
  if row[13] is not None and not owner.absent((row[13],row[5],row[14],row[15])):
   raise Blocked('original independent controller still exists; no concurrent completion')
  before_resume=operation is not None and operation[0] in ('start_intent','replacement_started')
  readonly=operation is not None and operation[0] in ('resume_intent','restored_known_local_state_prior_effects_unknown','service_lifecycle_reconciled_receipt_settings')
  if not before_resume and not readonly:
   return 'verification withheld: prior phase has no supported no-replay continuation'
  if readonly:
   from .provider_reattachment import _schema
   _schema(journal)
   if not journal.db.execute('SELECT 1 FROM authenticated_resume_receipts WHERE request_id=?',(request,)).fetchone():
    return 'UNKNOWN: authenticated resume response receipt unavailable; no resume replay or settings invention'
  selected=peer.old_selection(row[5],row[6],row[7],row[8],row[9])
  if not owner.absent((selected.pid,selected.uid,selected.birth_sec,selected.birth_usec)):
   raise Blocked('original provider generation still present; no reconciliation continuation')
  checkpoint=journal.db.execute('SELECT baseline,baseline_sha,actor_proof,source_id,source_sha FROM prestop_proofs WHERE request_id=?',(request,)).fetchone()
  if (not checkpoint or checkpoint[3:]!=(authority.source_id,authority.source_digest)
      or peer.digest(checkpoint[0].encode())!=checkpoint[1]):raise Blocked('reconciliation checkpoint/source binding changed')
  adapter=HandoffRecovery(selected,Path(row[3]),row[4],authority)
  adapter.baseline,adapter.baseline_sha=checkpoint[:2];adapter.deadline=deadline
  recorded=ast.literal_eval(checkpoint[2]);app=recorded[0]
  adapter.old_client=(app[0],app[1],app[3],app[4]);adapter.original_root_cli=recorded[4]
  adapter._config()
  from .provider_recovery_local_persistence import preserved_file
  preserved_file(adapter.baseline)
  while time.monotonic()<deadline:
   try:
    current=OwningContinuity(manager).inspect();pid=current['process']['pid'] if current['process'] else None
    if pid and pid!=adapter.old_client[0]:
     parent=owner.identity(pid);host,port=native.endpoint(selected.endpoint)
     listeners=native.DarwinEvidence.lsof('-iTCP@'+host+':'+str(port),'-sTCP:LISTEN')
     if len(listeners)==1:
      process=native.DarwinEvidence().process(listeners[0][0]);receipt=adapter._owned_record(process.pid,parent)
      if process.parent!=parent[0]:raise Blocked('observed replacement has another parent')
      adapter.child=ObservedBridgeChild(process,parent,adapter._ledger(),receipt[0]);adapter.child.poll();break
   except (native.EvidenceError,service.ServiceError,FileNotFoundError):pass
   time.sleep(.1)
  if adapter.child is None:raise TimeoutError('reconciled managed launch has no authenticated replacement')
  if readonly:
   from .provider_reattachment import verify_readonly
   adapter.reattachment_context=(journal,request)
   evidence=verify_readonly(adapter,journal,request,deadline)
   meaning='service lifecycle verified read-only after same-thread CLI reattachment; current readable settings verified; other settings remain authenticated resume-receipt-time; full current settings and overall ticket completion NOT attested; prior inputs UNKNOWN'
   journal.save(request,'service_lifecycle_reconciled_receipt_settings',meaning,'root_restored_prior_inputs_unknown')
   with service_journal.db:
    changed=service_journal.db.execute("UPDATE service_requests SET state='provider_restored',detail=? WHERE id=? AND state='outcome_unknown'",(meaning,request)).rowcount
   if changed or row[11]=='provider_restored':complete_server_restart(manager,service_journal.db,request)
   return meaning
  # Claim verification before its first metadata RPC. A crash never retries it.
  with journal.db:
   changed=journal.db.execute("UPDATE requests SET state='reconcile_verify_intent' WHERE id=? AND state IN ('start_intent','replacement_started')",(request,)).rowcount
   if changed!=1:raise Blocked('reconciliation verification already claimed')
  proof=adapter.verify(journal,request,deadline)
  with journal.db:journal.db.execute('INSERT INTO poststop_proofs VALUES('+','.join('?' for _ in range(25))+')',(request,*proof,repr(adapter.actor_proof)))
  outcome=journal.save(request,'restored_known_local_state_prior_effects_unknown','observed managed replacement verified after launch reconciliation; prior native inputs remain UNKNOWN','root_restored_prior_inputs_unknown')
  with service_journal.db:service_journal.db.execute("UPDATE service_requests SET state='provider_restored',detail=? WHERE id=?",(outcome.reason,request))
  complete_server_restart(manager,service_journal.db,request)
  return outcome.reason
 finally:
  if adapter is not None:adapter.close()
  journal.close()

LOCAL_SOURCE='configured-local-service-owner'

def authority_for_handoff(source):
 return lifecycle.AdministrativeAuthority(source,_configured_service_owner=source==LOCAL_SOURCE)

def prepare_legacy_profile(manager):
 """Automatic migration for the already supported local owning service.

 No App message is synthesized. Exact native ancestry must be owned by the
 configured running client; external/connect-only providers cannot use this.
 """
 inspector=LegacyOwningContinuity(manager);inspected=inspector.inspect()
 if not inspected['process']:raise Blocked('legacy owning client must actually be running')
 wrapper,command=configured(manager);address=option(command,'--codex-server')
 selected=selected_listener(address);parent=owner.identity(inspected['process']['pid'])
 generation=(selected.pid,selected.uid,selected.birth_sec,selected.birth_usec)
 owner.legacy_chain(generation,parent)
 authority=authority_for_handoff(LOCAL_SOURCE)
 from .provider_recovery_local_persistence import capture
 from .provider_recovery_provenance import open_store
 from . import provider_recovery_launch_config as locations
 checkpoint=capture(selected,authority.root[3]);cwd=field(checkpoint,'.known_settings.cwd')
 observed=locations.kernel_locations(selected);native_home=observed.get('CODEX_HOME')
 if native_home is None:
  if not observed.get('HOME'):raise Blocked('legacy provider home is unknown; no replacement defaults inferred')
  native_home=str(Path(observed['HOME'])/'.codex')
 store,_,_=open_store(selected);directory=private_directory(home_directory());temporary=private_directory(directory/'tmp')
 image=str(native.IMAGE.resolve());native_home=locations.directory(native_home,selected.uid)
 env=dict(HOME=str(Path(pwd.getpwuid(selected.uid).pw_dir).resolve()),CODEX_HOME=native_home,CODEX_SQLITE_HOME=str(store.parent),TMPDIR=str(temporary),PATH=os.pathsep.join(dict.fromkeys((str(Path(image).parent),str(Path(manager.python).parent),'/opt/homebrew/bin','/usr/bin','/bin','/usr/sbin','/sbin'))),LANG='en_US.UTF-8')
 config=dict(ProgramArguments=[image,'app-server','--listen',address,'-c','sqlite_home='+jq('{}','$path',path=str(store.parent))],Image=image,Endpoint=address,WorkingDirectory=cwd,EnvironmentVariables=env,Artifacts={})
 locations.manifest(config,selected.uid)
 raw=plistlib.dumps(config);pin=peer.digest(raw);path=directory/'provider.plist'
 if inspector.inspect()!=inspected or owner.legacy_chain(generation,parent)[0]!=native.DarwinEvidence().process(selected.pid).group:
  raise Blocked('legacy owning generation/config changed during preparation')
 authority.check();publish(path,raw);owner.manifest(path,pin,address)
 profile=dict(Manifest=str(path),SHA256=pin,Source=LOCAL_SOURCE,SourceKind='configured_local_service_owner')
 publish(directory/'client-before.sh',wrapper);publish(directory/'profile.plist',plistlib.dumps(profile))
 return profile


def complete_server_restart(manager,db,request):
 """Durable single HTTP effect claim; an unknown call is never repeated."""
 db.execute('BEGIN IMMEDIATE')
 try:
  row=db.execute('SELECT state,restart_server FROM service_requests WHERE id=?',(request,)).fetchone()
  if row is None:raise Blocked('unknown service request')
  if row[0]!='provider_restored':db.commit();return False
  state='server_restart_intent' if row[1] else 'completed'
  db.execute('UPDATE service_requests SET state=? WHERE id=? AND state=\'provider_restored\'',(state,request));db.commit()
 except BaseException:db.rollback();raise
 if row[1]:
  manager.restart('server')
  with db:
   changed=db.execute("UPDATE service_requests SET state='completed' WHERE id=? AND state='server_restart_intent'",(request,)).rowcount
   if changed!=1:raise Blocked('HTTP restart outcome journal changed')
 return True
