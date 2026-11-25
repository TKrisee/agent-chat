"""Actual owning-bridge child launch ledger; unknown starts are never replayed."""
import ctypes,errno,hashlib,os,plistlib,re,sqlite3,subprocess,time,sys
from contextlib import closing
from pathlib import Path
from . import native_owner_evidence as native
from . import provider_recovery_peer as peer
from . import provider_recovery_launch_config as locations
Blocked=peer.Blocked

def identity(pid=None):
 p=native.DarwinEvidence().process(os.getpid() if pid is None else pid)
 if p.uid!=os.getuid() or p.ruid!=os.getuid():raise Blocked('owned process UID differs')
 return p.pid,p.uid,p.birth_sec,p.birth_usec

def absent(generation):
 ctypes.set_errno(0)
 try:return identity(generation[0])!=tuple(generation)
 except native.EvidenceError:
  if ctypes.get_errno()==errno.ESRCH:return True
  raise Blocked('kernel process absence unavailable') from None

def assert_listener_absent(address):
 host,port=native.endpoint(address)
 p=subprocess.run(['/usr/sbin/lsof','-nP','-a','-iTCP@'+host+':'+str(port),'-sTCP:LISTEN','-F','pfn'],capture_output=True,text=True,timeout=3)
 if p.returncode!=1 or p.stdout or p.stderr:raise Blocked('selected listener remains or absence is unavailable')

def installed_image_sha(path):
 """Observe a stable, signed release at the configured executable location."""
 from .provider_image_admission import REQUIREMENT
 image=Path(path)
 if image!=native.IMAGE.resolve():raise Blocked('managed provider executable path differs')
 before=peer.digest(peer.trusted_file(image,os.getuid(),root_owned=True))
 command=['/usr/bin/codesign','--verify','--strict','--verbose=2']
 if before!=native.IMAGE_SHA:command.append('-R='+REQUIREMENT)
 try:
  checked=subprocess.run([*command,str(image)],capture_output=True,text=True,timeout=10)
 except (OSError,subprocess.SubprocessError) as error:
  raise Blocked('managed provider signature verification unavailable') from error
 if checked.returncode:raise Blocked('managed provider signature verification failed')
 if peer.digest(peer.trusted_file(image,os.getuid(),root_owned=True))!=before:
  raise Blocked('managed provider executable changed during signature verification')
 return before

def manifest(path,digest,address,*,require_manifest_image=False):
 raw=peer.trusted_file(path,os.getuid())
 if hashlib.sha256(raw).hexdigest()!=digest:raise Blocked('managed launch manifest changed')
 config=plistlib.loads(raw)
 image=str(native.IMAGE.resolve())
 if (config.get('Image')!=image or config.get('Endpoint')!=address
     or config.get('ProgramArguments',[])[:4]!=[image,'app-server','--listen',address]
     or len(config['ProgramArguments'])!=6):raise Blocked('managed provider exact artifact/argv/endpoint differs')
 expected=config.get('ImageSHA256')
 if expected is not None and not isinstance(expected,str):raise Blocked('managed provider historical image evidence is invalid')
 if expected is not None and expected!=native.IMAGE_SHA:
  from . import provider_image_admission as admission
  receipt=Path(path).parent/('provider-admission-'+digest+'.plist')
  evidence=plistlib.loads(peer.trusted_file(receipt,os.getuid()))
  previous=evidence.get('PreviousManifestSHA256')
  if (not isinstance(previous,str) or len(previous)!=64 or any(ch not in '0123456789abcdef' for ch in previous)
      or evidence.get('ImageSHA256')!=expected or evidence.get('BaselineSHA256')!=native.IMAGE_SHA
      or evidence.get('Requirement')!=admission.REQUIREMENT or evidence.get('Image')!=image
      or evidence.get('Endpoint')!=address or evidence.get('ManifestSHA256')!=digest):
   raise Blocked('managed provider admission differs')
 actual=installed_image_sha(image)
 if require_manifest_image and actual!=expected:
  raise Blocked('managed provider disk bytes differ; see docs/provider-image-admission.md')
 env,_,_=locations.manifest(config,os.getuid())
 cwd=locations.directory(config['WorkingDirectory'],os.getuid())
 return config,env,cwd


def retained_child_attestation(manifest_path,pin,address,process,parent):
 """Validate a positive prior witness for this exact currently owned child."""
 manifest_path=Path(manifest_path)
 raw=peer.trusted_file(manifest_path,os.getuid())
 if peer.digest(raw)!=pin:raise Blocked('managed attestation manifest changed')
 configuration=plistlib.loads(raw)
 if (configuration.get('Image'),configuration.get('Endpoint'))!=(process.image,address):
  raise Blocked('managed attestation image location or endpoint changed')
 if process.parent!=parent.pid:raise Blocked('managed attestation direct parent differs')
 ledger=manifest_path.parent/'owned-provider.sqlite3'
 peer.trusted_file(ledger,os.getuid(),contents=False)
 with closing(peer.readonly(ledger)) as db:
  present=db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='child_launch_attestations'").fetchone()
  if not present:raise Blocked('managed launch has no prior verified signing evidence')
  evidence=db.execute('''SELECT l.state,l.parent_pid,l.parent_uid,l.parent_sec,l.parent_usec,
      l.child_pid,l.child_uid,l.child_sec,l.child_usec,a.image_path,a.image_sha,a.code_hash,a.kernel_cdhash
      FROM child_launches l JOIN child_launch_attestations a ON a.serial=l.serial
      JOIN owner_config c ON c.singleton=1 AND c.pin=a.manifest_pin AND c.endpoint=a.endpoint
      WHERE l.serial=(SELECT MAX(serial) FROM child_launches) AND a.manifest_pin=? AND a.endpoint=?''',
      (pin,address)).fetchone()
 expected=('started',parent.pid,parent.uid,parent.birth_sec,parent.birth_usec,
           process.pid,process.uid,process.birth_sec,process.birth_usec,process.image)
 if not evidence or evidence[:10]!=expected:
  raise Blocked('managed launch has no prior verified evidence for this generation')
 actual=native._kernel_code_hash(process.pid)
 image_sha,code_hash,kernel_cdhash=evidence[10:]
 if (not re.fullmatch('[0-9a-f]{64}',image_sha) or not re.fullmatch('[0-9a-f]{40,64}',code_hash)
     or actual!=kernel_cdhash or code_hash[:40]!=actual):
  raise Blocked('running managed code differs from its verified launch')
 if peer.digest(peer.trusted_file(process.image,os.getuid(),root_owned=True))==image_sha:
  raise Blocked('retained managed signing evidence requires actual disk image drift')
 return image_sha,code_hash


def configured_attestation_manifest(address):
 """Select only the locally configured managed owning profile for fallback."""
 import pwd
 from . import provider_service, service
 config=service._installed_server_config()
 if config.uid!=os.getuid() or config.uid!=os.geteuid():
  raise Blocked('configured managed attestation UID differs')
 manager=service.ServiceManager(system='Darwin',home=Path(pwd.getpwuid(config.uid).pw_dir))
 _,command=provider_service.configured(manager)
 if '--connect-only' in command or provider_service.option(command,'--codex-server')!=address:
  raise Blocked('configured managed attestation endpoint differs')
 manifest_path=Path(provider_service.option(command,'--managed-provider-manifest'))
 pin=provider_service.option(command,'--managed-provider-sha256')
 if manifest_path.parent!=config.master.parent/'provider-owning':
  raise Blocked('configured managed attestation directory differs')
 return manifest_path,pin


def configured_child_attestation(address,process,parent):
 manifest_path,pin=configured_attestation_manifest(address)
 try:return retained_child_attestation(manifest_path,pin,address,process,parent)
 except sqlite3.Error as error:raise Blocked('configured managed signing journal unavailable') from error


def cli_argv_sha(argv):
 return peer.digest('\0'.join(argv).encode())


def configured_cli_attestation(address,generation,image,argv,owner_identity):
 """Check an independently witnessed canonical CLI, including across child rollover."""
 manifest_path,pin=configured_attestation_manifest(address)
 raw=peer.trusted_file(manifest_path,os.getuid())
 if peer.digest(raw)!=pin:raise Blocked('managed CLI attestation manifest changed')
 if (plistlib.loads(raw).get('Image'),str(native.IMAGE))!=(image,image):
  raise Blocked('managed CLI attestation image location differs')
 ledger=manifest_path.parent/'owned-provider.sqlite3'
 peer.trusted_file(ledger,os.getuid(),contents=False)
 try:
  with closing(peer.readonly(ledger)) as db:
   present=db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_cli_attestations'").fetchone()
   if not present:raise Blocked('canonical Root CLI has no prior verified signing evidence')
   evidence=db.execute('''SELECT r.image_sha,r.code_hash,r.kernel_cdhash FROM root_cli_attestations r
        JOIN child_launches l ON l.serial=r.serial AND l.state IN ('started','closed')
        JOIN child_launch_attestations a ON a.serial=r.serial AND a.manifest_pin=? AND a.endpoint=?
        JOIN owner_config c ON c.singleton=1 AND c.pin=a.manifest_pin AND c.endpoint=a.endpoint
        WHERE r.owner_identity=? AND r.argv_sha=? AND r.pid=? AND r.uid=? AND r.birth_sec=? AND r.birth_usec=?
        AND r.image_path=? ORDER BY r.serial DESC LIMIT 1''',
        (pin,address,owner_identity,cli_argv_sha(argv),*generation,image)).fetchone()
 except sqlite3.Error as error:raise Blocked('configured managed CLI signing journal unavailable') from error
 if not evidence:raise Blocked('canonical Root CLI has no prior verified evidence for this generation')
 image_sha,code_hash,kernel_cdhash=evidence
 actual=native._kernel_code_hash(generation[0])
 if (not re.fullmatch('[0-9a-f]{64}',image_sha) or not re.fullmatch('[0-9a-f]{40,64}',code_hash)
     or actual!=kernel_cdhash or code_hash[:40]!=actual):
  raise Blocked('running Root CLI code differs from its verified attestation')
 if peer.digest(peer.trusted_file(image,os.getuid(),root_owned=True))==image_sha:
  raise Blocked('retained Root CLI signing evidence requires actual disk image drift')
 return image_sha,code_hash


class LaunchLedger:
 def __init__(self,path,pin,address,*,rollover=None,require_admitted_transition=False):
  self.path=Path(path);self.address=address;self.pin=pin;self.child=None;self.serial=None
  self.journal=peer.v1.Journal(self.path);self.db=self.journal.db
  try:
   self.db.execute('CREATE TABLE IF NOT EXISTS owner_config(singleton INTEGER PRIMARY KEY,pin TEXT,endpoint TEXT)')
   self.db.execute('CREATE TABLE IF NOT EXISTS child_launches(serial INTEGER PRIMARY KEY,state TEXT,parent_pid INTEGER,parent_uid INTEGER,parent_sec INTEGER,parent_usec INTEGER,child_pid INTEGER,child_uid INTEGER,child_sec INTEGER,child_usec INTEGER,observed_at REAL)')
   self.db.execute('CREATE TABLE IF NOT EXISTS provider_pin_transitions(old_pin TEXT PRIMARY KEY,new_pin TEXT NOT NULL,endpoint TEXT NOT NULL,changed_at REAL NOT NULL)')
   self.db.execute('''CREATE TABLE IF NOT EXISTS child_launch_attestations(
       serial INTEGER PRIMARY KEY,manifest_pin TEXT NOT NULL,endpoint TEXT NOT NULL,
       image_path TEXT NOT NULL,image_sha TEXT NOT NULL,code_hash TEXT NOT NULL,
       kernel_cdhash TEXT NOT NULL)''')
   self.db.execute('''CREATE TABLE IF NOT EXISTS root_cli_attestations(
       serial INTEGER NOT NULL,owner_identity TEXT NOT NULL,argv_sha TEXT NOT NULL,
       pid INTEGER NOT NULL,uid INTEGER NOT NULL,birth_sec INTEGER NOT NULL,birth_usec INTEGER NOT NULL,
       image_path TEXT NOT NULL,image_sha TEXT NOT NULL,code_hash TEXT NOT NULL,kernel_cdhash TEXT NOT NULL,
       PRIMARY KEY(serial,owner_identity))''')
   for operation in ('UPDATE','DELETE'):
    self.db.execute(f'''CREATE TRIGGER IF NOT EXISTS child_attestations_no_{operation.lower()}
        BEFORE {operation} ON child_launch_attestations
        BEGIN SELECT RAISE(ABORT,'managed launch attestation is immutable'); END''')
    self.db.execute(f'''CREATE TRIGGER IF NOT EXISTS root_cli_attestations_no_{operation.lower()}
        BEFORE {operation} ON root_cli_attestations
        BEGIN SELECT RAISE(ABORT,'managed CLI attestation is immutable'); END''')
   self.db.execute('BEGIN IMMEDIATE')
   try:
    old=self.db.execute('SELECT pin,endpoint FROM owner_config WHERE singleton=1').fetchone()
    if require_admitted_transition:
     transition=self.db.execute('SELECT 1 FROM provider_pin_transitions WHERE new_pin=? AND endpoint=?',(pin,address)).fetchone()
     if old!=(pin,address) or not transition:
      raise Blocked('admitted successor requires an existing matching ledger transition')
     self.db.commit();return
    if old and old!=(pin,address):
     self._rollover_locked(old[0],pin,rollover)
    if not old:self.db.execute('INSERT INTO owner_config VALUES(1,?,?)',(pin,address))
    self.db.commit()
   except BaseException:
    self.db.rollback();raise
  except BaseException:self.journal.close();raise
 def _rollover_locked(self,old_pin,new_pin,admission):
  """One authenticated successor may replace an inactive exact prior pin."""
  if not isinstance(admission,dict) or set(admission)!={'old_pin','new_pin','receipt'}:
   raise Blocked('owning bridge configuration conflicts with durable ledger')
  receipt=admission['receipt']
  if (admission['old_pin'],admission['new_pin'])!=(old_pin,new_pin) or not isinstance(receipt,dict):
   raise Blocked('provider pin rollover admission differs')
  required={'ImageSHA256','BaselineSHA256','Requirement','Image','Endpoint','ManifestSHA256','PreviousManifestSHA256'}
  if (set(receipt)!=required or receipt['ManifestSHA256']!=new_pin
      or receipt['PreviousManifestSHA256']!=old_pin or receipt['Endpoint']!=self.address):
   raise Blocked('provider pin rollover receipt differs')
  if any(not isinstance(receipt[key],str) or not receipt[key] for key in required):
   raise Blocked('provider pin rollover receipt is malformed')
  existing=self.db.execute('SELECT new_pin,endpoint FROM provider_pin_transitions WHERE old_pin=?',(old_pin,)).fetchone()
  if existing:
   if tuple(existing)!=(new_pin,self.address):raise Blocked('provider pin rollover conflicts with prior transition')
   return
  rows=self.db.execute('SELECT state,parent_pid,parent_uid,parent_sec,parent_usec,child_pid,child_uid,child_sec,child_usec FROM child_launches').fetchall()
  if any(row[0] not in ('closed','cancelled_before_release') for row in rows):
   raise Blocked('previous child generation is unresolved')
  if any(not absent(row[1:5]) for row in rows):
   raise Blocked('previous bridge parent generation still exists')
  if any(row[5] is not None and not absent(row[5:]) for row in rows):
   raise Blocked('previous child generation still exists')
  assert_listener_absent(self.address)
  if self.db.execute('UPDATE owner_config SET pin=? WHERE singleton=1 AND pin=? AND endpoint=?',(new_pin,old_pin,self.address)).rowcount!=1:
   raise Blocked('provider pin rollover lost its exact prior configuration')
  self.db.execute('INSERT INTO provider_pin_transitions VALUES(?,?,?,?)',(old_pin,new_pin,self.address,time.time()))
 def rollover_manifest(self,old_pin,new_pin,admission):
  self.db.execute('BEGIN IMMEDIATE')
  try:
   current=self.db.execute('SELECT pin,endpoint FROM owner_config WHERE singleton=1').fetchone()
   if current!=(old_pin,self.address):raise Blocked('provider pin rollover lost its exact prior configuration')
   self._rollover_locked(old_pin,new_pin,admission);self.db.commit();return new_pin
  except BaseException:self.db.rollback();raise
 def __enter__(self):return self
 def __exit__(self,*unused):self.journal.close()
 def rows(self):return self.db.execute('SELECT * FROM child_launches ORDER BY serial').fetchall()
 def record_attestation(self,process,parent,image_sha,code_hash,kernel_cdhash):
  generation=(process.pid,process.uid,process.birth_sec,process.birth_usec)
  if not all(re.fullmatch(pattern,value) for pattern,value in (
      ('[0-9a-f]{64}',image_sha),('[0-9a-f]{40,64}',code_hash),('[0-9a-f]{40}',kernel_cdhash))):
   raise Blocked('managed launch attestation is malformed')
  if process.parent!=parent[0] or self.child is None or self.child.pid!=process.pid:
   raise Blocked('actual managed child differs during attestation')
  record=(self.serial,self.pin,self.address,process.image,image_sha,code_hash,kernel_cdhash)
  self.db.execute('BEGIN IMMEDIATE')
  try:
   scope=self.db.execute('SELECT pin,endpoint FROM owner_config WHERE singleton=1').fetchone()
   launch=self.db.execute('''SELECT state,parent_pid,parent_uid,parent_sec,parent_usec,child_pid,child_uid,child_sec,child_usec
       FROM child_launches WHERE serial=?''',(self.serial,)).fetchone()
   if scope!=(self.pin,self.address) or launch!=('started',*parent,*generation):
    raise Blocked('managed launch generation changed before attestation')
   self.db.execute('INSERT OR IGNORE INTO child_launch_attestations VALUES(?,?,?,?,?,?,?)',record)
   if self.db.execute('SELECT * FROM child_launch_attestations WHERE serial=?',(self.serial,)).fetchone()!=record:
    raise Blocked('managed launch attestation conflicts with retained evidence')
   self.db.commit()
  except BaseException:self.db.rollback();raise
 def record_cli_attestation(self,parent,child,generation,image,argv,owner_identity,image_sha,code_hash,kernel_cdhash):
  record=(self.serial,owner_identity,cli_argv_sha(argv),*generation,image,image_sha,code_hash,kernel_cdhash)
  self.db.execute('BEGIN IMMEDIATE')
  try:
   launch=self.db.execute('''SELECT l.state,l.parent_pid,l.parent_uid,l.parent_sec,l.parent_usec,
       l.child_pid,l.child_uid,l.child_sec,l.child_usec
       FROM child_launches l JOIN child_launch_attestations a ON a.serial=l.serial
       JOIN owner_config c ON c.singleton=1 AND c.pin=a.manifest_pin AND c.endpoint=a.endpoint
       WHERE l.serial=? AND a.manifest_pin=? AND a.endpoint=?''',
       (self.serial,self.pin,self.address)).fetchone()
   if launch!=('started',*parent,*child):raise Blocked('managed launch changed before CLI attestation')
   self.db.execute('INSERT OR IGNORE INTO root_cli_attestations VALUES(?,?,?,?,?,?,?,?,?,?,?)',record)
   prior=self.db.execute('SELECT * FROM root_cli_attestations WHERE serial=? AND owner_identity=?',
                         (self.serial,owner_identity)).fetchone()
   if prior!=record:raise Blocked('managed CLI attestation conflicts with retained evidence')
   self.db.commit()
  except BaseException:self.db.rollback();raise
 def claim(self):
  self.db.execute('BEGIN IMMEDIATE')
  try:
   if self.db.execute('SELECT pin,endpoint FROM owner_config WHERE singleton=1').fetchone()!=(self.pin,self.address):
    raise Blocked('owning bridge pin changed before claim')
   last=self.db.execute('SELECT serial,state FROM child_launches ORDER BY serial DESC LIMIT 1').fetchone()
   if last and last[1] not in ('closed','cancelled_before_release'):raise Blocked('previous child start is unknown or unclosed; reconcile without launching')
   assert_listener_absent(self.address)
   parent=identity();serial=last[0]+1 if last else 1
   self.db.execute('INSERT INTO child_launches VALUES(?,?,?,?,?,?,NULL,NULL,NULL,NULL,?)',(serial,'gate_intent',*parent,time.time()))
   self.db.commit();self.serial=serial;return serial
  except BaseException:self.db.rollback();raise
 def launched(self,child):
  if self.serial is None or self.child is not None:raise Blocked('child launch has no fresh durable claim')
  self.child=child
  process=native.DarwinEvidence().process(child.pid)
  if process.parent!=os.getpid() or child.poll() is not None:raise Blocked('actual retained child parent differs or exited')
  generation=identity(child.pid)
  with self.db:
   changed=self.db.execute("UPDATE child_launches SET state='gated',child_pid=?,child_uid=?,child_sec=?,child_usec=?,observed_at=? WHERE serial=? AND state='gate_intent'",(*generation,time.time(),self.serial)).rowcount
   if changed!=1:raise Blocked('durable child launch claim changed')
 def release(self,descriptor):
  if self.child is None or self.child.poll() is not None:raise Blocked('gated retained child unavailable')
  with self.db:
   changed=self.db.execute("UPDATE child_launches SET state='release_intent' WHERE serial=? AND state='gated'",(self.serial,)).rowcount
   if changed!=1:raise Blocked('gated launch claim changed')
  # The exact child generation is durable BEFORE the gate can execute Codex.
  if os.write(descriptor,b'1')!=1:raise Blocked('exec gate release unknown')
  with self.db:
   changed=self.db.execute("UPDATE child_launches SET state='started' WHERE serial=? AND state='release_intent'",(self.serial,)).rowcount
   if changed!=1:raise Blocked('exec release outcome changed')
 def reconcile(self):
  self.db.execute('BEGIN IMMEDIATE')
  try:
   row=self.db.execute('SELECT * FROM child_launches ORDER BY serial DESC LIMIT 1').fetchone()
   if row is None or row[1] in ('closed','cancelled_before_release'):
    self.db.commit();return 'no_unresolved_launch'
   if not absent(row[2:6]):raise Blocked('recorded bridge generation still exists; no reconciliation effect')
   assert_listener_absent(self.address)
   if row[1]=='gate_intent' and row[6] is None:
    # No release intent was committed; the only possible child waits on EOF.
    state='cancelled_before_release'
   else:
    if row[6] is None or not absent(row[6:10]):raise Blocked('exact child still exists or identity is unknown')
    group=row[6]
    if row[1]=='selected_legacy':
     adoption=self.db.execute('SELECT group_id,leader_pid,leader_uid,leader_sec,leader_usec FROM legacy_adoptions WHERE serial=?',(row[0],)).fetchone()
     if not adoption or not absent(adoption[1:]):raise Blocked('legacy launcher generation remains or is unknown')
     group=adoption[0]
    try:os.killpg(group,0)
    except ProcessLookupError:pass
    else:raise Blocked('recorded child process group remains; no closure claim')
    state='closed'
   self.db.execute('UPDATE child_launches SET state=?,observed_at=? WHERE serial=? AND state=?',(state,time.time(),row[0],row[1]))
   if row[1]=='selected_legacy':self.db.execute('UPDATE legacy_adoptions SET closed_at=? WHERE serial=?',(time.time(),row[0]))
   self.db.commit()
   return state+': native input outcomes remain UNKNOWN'
  except BaseException:self.db.rollback();raise
 def closed(self,child):
  if child is None or child is not self.child:raise Blocked('only the actual retained child handle can establish cleanup')
  from .bridge_client import _stop_codex
  _stop_codex(child)
  if child.poll() is None:raise Blocked('retained child not reaped')
  assert_listener_absent(self.address)
  with self.db:
   changed=self.db.execute("UPDATE child_launches SET state='closed',observed_at=? WHERE serial=? AND state IN ('gate_intent','gated','release_intent','started')",(time.time(),self.serial)).rowcount
   if changed!=1:raise Blocked('unknown child launch remains unreconciled')

class ReconnectedProvider:
 """Monitor the surviving provider without claiming a retained child handle."""
 def __init__(self,generation):
  self.generation=generation
  self.returncode=None
 def poll(self):
  if absent(self.generation):self.returncode='unknown (previous bridge owned the child)'
  return self.returncode


class OwnedProvider:
 def __init__(self,path,pin,address):
  self.path=Path(path);self.pin=pin;self.address=address
  self.config,self.environment,self.cwd=manifest(path,pin,address)
  successor=self.config.get('ImageSHA256',native.IMAGE_SHA)!=native.IMAGE_SHA
  self.ledger=LaunchLedger(self.path.parent/'owned-provider.sqlite3',pin,address,
                           require_admitted_transition=successor)
  self.child=None
  self.attested=False
  self.next_attestation=0
  self.next_cli_attestation=0
  self.last_cli_attestation=None
 def attest_connected(self,rpc):
  """Remember the verified native child once its actual RPC socket is connected."""
  if self.child is None:return
  if self.attested:
   self._attest_cli()
   return
  if time.monotonic()<self.next_attestation:return
  self.next_attestation=time.monotonic()+5
  try:
   parent=identity()
   kernel=native.DarwinEvidence()
   process=kernel.process(self.child.pid)
   image_sha=installed_image_sha(self.config['Image'])
   birth=process.birth_sec,process.birth_usec
   proof=kernel.sample_independent_listener(self.address,process.pid,birth,rpc,image_sha)
   kernel_cdhash=native._kernel_code_hash(process.pid)
   if (proof.chain!=(process,) or proof.code_hash[:40]!=kernel_cdhash or kernel.process(process.pid)!=process
       or peer.digest(peer.trusted_file(self.config['Image'],os.getuid(),root_owned=True))!=image_sha
       or identity()!=parent or self.child.poll() is not None):
    raise Blocked('managed native generation changed during attestation')
   self.ledger.record_attestation(process,parent,image_sha,proof.code_hash,kernel_cdhash)
   self.attested=True
  except (Blocked,native.EvidenceError,OSError,sqlite3.Error,subprocess.SubprocessError):
   # Ordinary bridge operation remains available. A later operation that needs
   # retained provenance can only use a positive attestation.
   pass
 def _attest_cli(self):
  """Record a fresh signed Root actor through the existing exact managed observation."""
  if time.monotonic()<self.next_cli_attestation:return
  self.next_cli_attestation=time.monotonic()+30
  from . import provider_service
  try:
   parent=identity()
   child=identity(self.child.pid)
   authority=provider_service.authority_for_handoff(provider_service.LOCAL_SOURCE)
   selected=peer.old_selection(child[1],child[0],child[2],child[3],self.address)
   actors=provider_service.HandoffActors(authority,selected)
   actors.allow_retained_cli=False
   app,_,_,_,cli=actors.observe()
   actor,argv,code_hash,_,owner_identity=cli
   generation=actor[0],actor[1],actor[3],actor[4]
   if (app[0],app[1],app[3],app[4])!=parent:
    raise Blocked('managed bridge differs while recording Root CLI signing evidence')
   if owner_identity==self.last_cli_attestation:return
   image=self.config['Image']
   image_sha=installed_image_sha(image)
   dynamic=native.dynamic_disk_association(generation[0],generation[2:],image)
   kernel_cdhash=native._kernel_code_hash(generation[0])
   if (dynamic[:40]!=kernel_cdhash or code_hash[:40]!=kernel_cdhash
       or peer.digest(peer.trusted_file(image,os.getuid(),root_owned=True))!=image_sha
       or identity(generation[0])!=generation or identity()!=parent or identity(self.child.pid)!=child):
    raise Blocked('managed Root CLI signing generation changed during attestation')
   self.ledger.record_cli_attestation(parent,child,generation,image,argv,owner_identity,image_sha,code_hash,kernel_cdhash)
   self.last_cli_attestation=owner_identity
  except (Blocked,native.EvidenceError,OSError,sqlite3.Error,subprocess.SubprocessError):
   pass
 def _handoff_baseline(self):
  """Load exact per-operation evidence when launchd restarts the owning bridge."""
  from contextlib import closing
  directory=self.path.parent
  service_journal=directory/'service-requests.sqlite3'
  try:peer.trusted_file(service_journal,os.getuid(),contents=False)
  except FileNotFoundError:return None
  with closing(peer.readonly(service_journal)) as db:
   rows=db.execute("""SELECT id,source,source_sha,manifest,pin,uid,pid,birth_sec,birth_usec,endpoint,state
       FROM service_requests WHERE endpoint=? AND state IN ('running','force_submit_intent','force_running','outcome_unknown')""",
       (self.address,)).fetchall()
  if not rows:return None
  if len(rows)!=1:raise Blocked('managed launch handoff is ambiguous')
  request,source,source_sha,manifest_path,pin,*selection,state=rows[0]
  recovery_journal=directory.parent/'provider-recovery-v3'/'requests.sqlite3'
  peer.trusted_file(recovery_journal,os.getuid(),contents=False)
  with closing(peer.readonly(recovery_journal)) as db:
   row=db.execute('SELECT uid,pid,birth_sec,birth_usec,endpoint,state FROM requests WHERE id=?',(request,)).fetchone()
   if not row or row[:5]!=tuple(selection):raise Blocked('managed launch original generation differs')
   if row[5] in ('staged','armed_unknown_prestate'):return None
   if ((manifest_path,pin)!=(str(self.path),self.pin) or state not in ('running','force_running')
       or row[5] not in ('stop_intent','force_stop_intent','closed','start_intent')):
    raise Blocked('managed launch does not match an active handoff')
   checkpoint=db.execute('SELECT baseline,baseline_sha,source_id,source_sha FROM prestop_proofs WHERE request_id=?',
                         (request,)).fetchone()
  if not checkpoint or checkpoint[2:]!=(source,source_sha) or peer.digest(checkpoint[0].encode())!=checkpoint[1]:
   raise Blocked('managed launch checkpoint differs')
  return request,checkpoint[0]
 def _check_handoff_inputs(self, handoff):
  if handoff is not None:
   locations.check_baseline(self.config,handoff[1],os.getuid())
   captured=locations.field(handoff[1],'.replacement_configuration.image_sha256 // ""')
   expected=captured or self.config.get('ImageSHA256')
   if installed_image_sha(self.config['Image'])!=expected:
    raise Blocked('staged replacement executable changed before provider release')
 def _reconnect_survivor(self):
  row=self.ledger.db.execute('''SELECT l.state,l.parent_pid,l.parent_uid,l.parent_sec,l.parent_usec,
    l.child_pid,l.child_uid,l.child_sec,l.child_usec,a.manifest_pin,a.endpoint,a.image_path,a.image_sha,a.kernel_cdhash
    FROM child_launches l LEFT JOIN child_launch_attestations a USING(serial)
    ORDER BY l.serial DESC LIMIT 1''').fetchone()
  if not row or row[0]!='started':return None
  parent,child=row[1:5],row[5:9]
  if not absent(parent):return None
  if absent(child):
   self.ledger.reconcile()
   return None
  from .provider_service import selected_listener
  selected=selected_listener(self.address)
  if (selected.pid,selected.uid,selected.birth_sec,selected.birth_usec)!=child:
   raise Blocked('listener differs from the recorded surviving provider')
  process=native.DarwinEvidence().process(child[0])
  image=self.config['Image']
  if (process.parent!=1 or process.image!=image or identity(child[0])!=child
      or row[9:13]!=(self.pin,self.address,image,installed_image_sha(image))
      or not row[13] or native._kernel_code_hash(child[0])!=row[13]):
   raise Blocked('surviving provider does not match its recorded launch attestation')
  return ReconnectedProvider(child)
 def start(self,runtime_environment,stderr):
  self.config,self.environment,self.cwd=manifest(self.path,self.pin,self.address)
  handoff=self._handoff_baseline()
  self._check_handoff_inputs(handoff)
  if handoff is None:
   surviving=self._reconnect_survivor()
   if surviving is not None:return surviving
  self.ledger.claim()
  env=dict(self.environment)
  # The bridge's already authenticated tool transport stays private in memory.
  for key in ('AGENT_CHAT_SERVER','AGENT_CHAT_API_TOKEN','AGENT_CHAT_PROJECT'):
   if key in runtime_environment:env[key]=runtime_environment[key]
  reader,writer=os.pipe()
  try:
   gate='import os,sys;fd=int(sys.argv[1]);permit=os.read(fd,1);os.close(fd);sys.exit(73) if permit!=b"1" else os.execvpe(sys.argv[2],sys.argv[2:],os.environ)'
   self.child=subprocess.Popen([sys.executable,'-I','-c',gate,str(reader),*self.config['ProgramArguments']],cwd=self.cwd,env=env,stdin=subprocess.DEVNULL,
       stdout=stderr,stderr=stderr,start_new_session=True,pass_fds=(reader,))
   os.close(reader);reader=None
   self.ledger.launched(self.child)
   if self._handoff_baseline()!=handoff:raise Blocked('managed launch handoff changed before release')
   self._check_handoff_inputs(handoff)
   self.ledger.release(writer)
   return self.child
  finally:
   if reader is not None:os.close(reader)
   os.close(writer)
 def close(self):
  try:
   if self.child is not None:self.ledger.closed(self.child)
  finally:self.ledger.journal.close()

def legacy_chain(provider,parent):
 """Observed ancestry/group, not an invented retained Popen receipt."""
 kernel=native.DarwinEvidence();chain=[];process=kernel.process(provider[0])
 if identity(process.pid)!=tuple(provider) or identity(parent[0])!=tuple(parent):raise Blocked('legacy provider/client generation changed')
 for _ in range(5):
  if process.pid==parent[0]:break
  if process.uid!=parent[1] or process.ruid!=parent[1] or process.parent<=1:raise Blocked('legacy native process is outside configured client ancestry')
  chain.append(process);process=kernel.process(process.parent)
 else:raise Blocked('legacy provider ancestry exceeds bound')
 if not chain or process.pid!=parent[0]:raise Blocked('legacy client ancestry absent')
 leader=chain[-1]
 if any(item.group!=leader.pid for item in chain):raise Blocked('legacy native group is not the retained launcher group')
 if any(kernel.process(item.pid)!=item for item in chain) or identity(parent[0])!=tuple(parent):raise Blocked('legacy process chain changed')
 return leader.pid,identity(leader.pid)

def adopt_legacy(ledger,provider,parent):
 group,leader=legacy_chain(provider,parent)
 ledger.db.execute('BEGIN IMMEDIATE')
 try:
  if ledger.db.execute('SELECT 1 FROM child_launches LIMIT 1').fetchone():raise Blocked('legacy handoff cannot overwrite existing launch history')
  ledger.db.execute('CREATE TABLE IF NOT EXISTS legacy_adoptions(serial INTEGER PRIMARY KEY,group_id INTEGER,leader_pid INTEGER,leader_uid INTEGER,leader_sec INTEGER,leader_usec INTEGER,selected_at REAL,closed_at REAL)')
  ledger.db.execute('INSERT INTO child_launches VALUES(1,?,?,?,?,?,?,?,?,?,?)',('selected_legacy',*parent,*provider,time.time()))
  ledger.db.execute('INSERT INTO legacy_adoptions VALUES(1,?,?,?,?,?,?,NULL)',(group,*leader,time.time()))
  ledger.db.commit()
 except BaseException:ledger.db.rollback();raise
