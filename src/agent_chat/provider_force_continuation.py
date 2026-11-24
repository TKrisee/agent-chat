"""Explicit one-shot force continuation of a retained external stop timeout.

Original effect evidence is preserved. No TERM, force, provider start or native
input is retried. Unknown stronger effects remain unknown, not permission to retry.
"""
import ctypes,errno,os,plistlib,signal,time
from pathlib import Path
from . import provider_service as service,provider_owner as owner
from . import provider_recovery_peer as peer,provider_recovery_lifecycle as lifecycle
Blocked=peer.Blocked

def generation_absent(selected):
 ctypes.set_errno(0)
 try:actual=peer.kernel_identity(selected.pid)
 except Blocked:
  if ctypes.get_errno()==errno.ESRCH:return True
  raise Blocked('exact old generation absence unavailable') from None
 return actual!=(selected.pid,selected.uid,selected.uid,selected.birth_sec,selected.birth_usec)

def _effects(journal):
 journal.db.execute('CREATE TABLE IF NOT EXISTS force_effects(request_id TEXT PRIMARY KEY,claimed_at REAL,original_state TEXT,original_reason TEXT,original_effects TEXT,selected_generation TEXT,source_id TEXT,source_sha TEXT,baseline_sha TEXT,manifest_sha TEXT,actor_proof TEXT,uncertain_inputs TEXT,signal_returned_at REAL,closed_at REAL,start_claimed_at REAL)')

def claim_and_force(journal,request,adapter):
 _effects(journal)
 if journal.db.execute('SELECT 1 FROM force_effects WHERE request_id=?',(request,)).fetchone():raise Blocked('stronger signal already claimed; unknown effect never replayed')
 if adapter.owning:raise Blocked('explicit external-provider continuation cannot bypass owning-child cleanup')
 adapter.selection();peer.selected_kernel(adapter.selected)
 if time.monotonic()>=adapter.deadline:raise TimeoutError('deadline before stronger effect claim')
 journal.db.execute('BEGIN IMMEDIATE')
 try:
  original=journal.db.execute('SELECT state,reason,effects FROM requests WHERE id=?',(request,)).fetchone()
  if original is None or original[0]!='stop_intent':raise Blocked('force requires retained original stop_intent')
  selected=adapter.selected;generation=(selected.pid,selected.uid,selected.birth_sec,selected.birth_usec,selected.endpoint)
  journal.db.execute('INSERT INTO force_effects VALUES('+','.join('?' for _ in range(15))+')',(request,time.time(),*original,repr(generation),adapter.authority.source_id,adapter.authority.source_digest,adapter.baseline_sha,adapter.manifest_sha256,repr(adapter.actor_proof),repr(adapter.authority.unknown),None,None,None))
  journal.db.execute("UPDATE requests SET state='force_stop_intent',reason='explicit SIGKILL outcome unknown; original TERM evidence retained in force_effects',effects='force_unknown_prior_inputs_unknown' WHERE id=?",(request,));journal.db.commit()
 except BaseException:journal.db.rollback();raise
 # Admission and exact generation/listener are sampled again after durable claim.
 # Darwin still has no atomic generation-qualified kill syscall; residual race is disclosed.
 adapter.selection();peer.selected_kernel(adapter.selected)
 if time.monotonic()>=adapter.deadline:raise TimeoutError('deadline after stronger claim; no effect replay')
 os.kill(adapter.selected.pid,signal.SIGKILL)
 with journal.db:journal.db.execute('UPDATE force_effects SET signal_returned_at=? WHERE request_id=?',(time.time(),request))

def claim_start_after_closure(journal,request,selected):
 _effects(journal)
 if not generation_absent(selected):raise Blocked('exact old generation closure unproven')
 try:owner.assert_listener_absent(selected.endpoint)
 except Blocked as error:raise Blocked('selected listener closure unproven: '+str(error)) from error
 journal.db.execute('BEGIN IMMEDIATE')
 try:
  row=journal.db.execute('SELECT start_claimed_at FROM force_effects WHERE request_id=?',(request,)).fetchone()
  if row is None:raise Blocked('stronger effect claim absent; no replacement start')
  if row[0] is not None:raise Blocked('replacement start already claimed; no replay')
  journal.db.execute('UPDATE force_effects SET closed_at=?,start_claimed_at=? WHERE request_id=?',(time.time(),time.time(),request))
  journal.db.execute("UPDATE requests SET state='start_intent',reason='exact old generation and listener closed; managed replacement start unknown',effects='start_unknown' WHERE id=?",(request,));journal.db.commit()
 except BaseException:journal.db.rollback();raise

def _schema(db):
 db.execute('CREATE TABLE IF NOT EXISTS force_continuations(request_id TEXT PRIMARY KEY,state TEXT,submitted_at REAL,original_service_state TEXT,original_detail TEXT,source_sha TEXT,baseline_sha TEXT,worker_pid INTEGER,worker_sec INTEGER,worker_usec INTEGER,detail TEXT,original_worker TEXT)')

def _old_worker_absent(row):
 if row[13] is None:raise Blocked('original controller generation was not durably recorded')
 selected=peer.old_selection(row[5],row[13],row[14],row[15],row[9])
 if not generation_absent(selected):raise Blocked('original controller still exists; no force continuation')

def _prepare(row,deadline):
 authority=service.authority_for_handoff(row[1])
 if authority.source_digest!=row[2]:raise Blocked('original repair source digest differs')
 _old_worker_absent(row)
 selected=peer.old_selection(row[5],row[6],row[7],row[8],row[9])
 journal=peer.v1.Journal(authority.master.parent/'provider-recovery-v3/requests.sqlite3');adapter=None
 try:
  original=journal.db.execute('SELECT state,reason,effects FROM requests WHERE id=?',(row[0],)).fetchone()
  if original is None or original[0]!='stop_intent' or original[1] not in (
      'TimeoutError: process/listener closure unproven',
      'TimeoutError: graceful stop phase expired; process/listener closure unproven'):
   raise Blocked('only the retained expired stop_intent admits this explicit continuation')
  checkpoint=journal.db.execute('SELECT thread_id,baseline,baseline_sha,source_id,source_sha FROM prestop_proofs WHERE request_id=?',(row[0],)).fetchone()
  if (not checkpoint or checkpoint[0]!=authority.root[3] or checkpoint[3:]!=(row[1],row[2]) or peer.digest(checkpoint[1].encode())!=checkpoint[2]):raise Blocked('original checkpoint/source binding differs')
  adapter=service.HandoffRecovery(selected,Path(row[3]),row[4],authority);adapter.deadline=deadline
  if adapter.owning:raise Blocked('explicit stronger continuation is scoped to retained external provider')
  adapter.baseline,adapter.baseline_sha=checkpoint[1:3]
  from .provider_recovery_local_persistence import preserved_file
  preserved_file(adapter.baseline);adapter.selection()
  binding=journal.db.execute('SELECT home_project,root_session,host,thread_id,binding_generation FROM checkpoint_bindings WHERE request_id=?',(row[0],)).fetchone()
  if binding!=(*authority.root,adapter.actors.generation):raise Blocked('same Root checkpoint binding generation differs')
  return journal,adapter
 except BaseException:
  if adapter is not None:adapter.close()
  journal.close();raise

def submit(manager,request):
 journal=service.operation_db();db=journal.db;native_journal=adapter=None
 try:
  _schema(db);row=db.execute('SELECT * FROM service_requests WHERE id=?',(request,)).fetchone()
  if row is None:raise Blocked('unknown existing handoff request')
  authority=service.authority_for_handoff(row[1])
  if authority.source_digest!=row[2]:raise Blocked('original source differs')
  previous=db.execute('SELECT state FROM force_continuations WHERE request_id=?',(request,)).fetchone()
  if previous:return service.result(row)  # Never bootstrap/re-signal on retry.
  if row[11]!='outcome_unknown':raise Blocked('original service outcome is not the retained timeout')
  native_journal,adapter=_prepare(row,time.monotonic()+30)
  directory=service.home_directory();job_path=directory/(request+'.force.plist')
  service_bin=str(Path(service.service.__file__).resolve().parents[2]/'bin/agent-chat-service')
  job=dict(Label=service.service.LABEL+'.force.'+request,ProgramArguments=[manager.python,service_bin,'provider-handoff-force-run','--request-id',request],RunAtLoad=True,KeepAlive=False,WorkingDirectory=str(directory),StandardOutPath=str(directory/(request+'.force.out.log')),StandardErrorPath=str(directory/(request+'.force.err.log')))
  service.publish(job_path,plistlib.dumps(job))
  db.execute('BEGIN IMMEDIATE')
  try:
   current=db.execute('SELECT state FROM service_requests WHERE id=?',(request,)).fetchone()
   if current!=('outcome_unknown',):raise Blocked('original service request changed before force submission')
   db.execute('INSERT INTO force_continuations VALUES(?,?,?,?,?,?,?,NULL,NULL,NULL,?,?)',(request,'submit_intent',time.time(),row[11],row[12],row[2],adapter.baseline_sha,'explicit independent stronger-effect continuation; native inputs UNKNOWN',repr((row[13],row[5],row[14],row[15]))))
   db.execute("UPDATE service_requests SET state='force_submit_intent' WHERE id=?",(request,));db.commit()
  except BaseException:db.rollback();raise
  answer=manager.run(['launchctl','bootstrap','gui/'+str(os.getuid()),str(job_path)],check=False,capture_output=True,text=True,timeout=10)
  with db:db.execute('UPDATE force_continuations SET detail=? WHERE request_id=?',('launchd accepted' if answer.returncode==0 else 'launchd submission UNKNOWN: '+answer.stderr[:500],request))
  return service.result(db.execute('SELECT * FROM service_requests WHERE id=?',(request,)).fetchone())
 finally:
  if adapter is not None:adapter.close()
  if native_journal is not None:native_journal.close()
  journal.close()

def run(manager,request,timeout=120):
 if type(timeout) not in (int,float) or not 0<timeout<=120:raise Blocked('bounded continuation budget required')
 deadline=time.monotonic()+timeout;journal=service.operation_db();db=journal.db;native_journal=adapter=None;claimed=False
 try:
  process=service.independently_hosted(manager,request,force=True);_schema(db)
  db.execute('BEGIN IMMEDIATE')
  row=db.execute('SELECT * FROM service_requests WHERE id=?',(request,)).fetchone()
  continuation=db.execute('SELECT state,source_sha,baseline_sha FROM force_continuations WHERE request_id=?',(request,)).fetchone()
  if not row or not continuation:raise Blocked('unknown force continuation')
  if continuation[0]!='submit_intent':db.commit();return service.result(row)
  if row[11]!='force_submit_intent':raise Blocked('force submission phase differs')
  db.execute("UPDATE force_continuations SET state='running',worker_pid=?,worker_sec=?,worker_usec=? WHERE request_id=?",(process.pid,process.birth_sec,process.birth_usec,request))
  db.execute("UPDATE service_requests SET state='force_running',worker_pid=?,worker_sec=?,worker_usec=? WHERE id=?",(process.pid,process.birth_sec,process.birth_usec,request));db.commit();claimed=True
  native_journal,adapter=_prepare(row,deadline)
  if continuation[1:]!=(adapter.authority.source_digest,adapter.baseline_sha):raise Blocked('submitted force scope changed')
  adapter.arm();claim_and_force(native_journal,request,adapter)
  closure_deadline=min(deadline,time.monotonic()+min(15,timeout/4))
  while not adapter.closed():
   if time.monotonic()>=closure_deadline:raise TimeoutError('stronger effect closure unproven; no replacement')
   time.sleep(.02)
  if time.monotonic()>=deadline:raise TimeoutError('original continuation deadline before replacement')
  claim_start_after_closure(native_journal,request,adapter.selected)
  adapter.start_once()
  native_journal.save(request,'replacement_started','one managed replacement observed after explicit stronger closure','replacement_started')
  proof=adapter.verify(native_journal,request,deadline)
  with native_journal.db:native_journal.db.execute('INSERT INTO poststop_proofs VALUES('+','.join('?' for _ in range(25))+')',(request,*proof,repr(adapter.actor_proof)))
  native_journal.save(request,'restored_known_local_state_prior_effects_unknown','explicit stronger continuation restored known same-thread state; original TERM/force/input history retained','root_restored_prior_inputs_unknown')
  with db:
   db.execute("UPDATE force_continuations SET state='provider_restored' WHERE request_id=?",(request,))
   db.execute("UPDATE service_requests SET state='provider_restored',detail='explicit stronger continuation restored; prior inputs UNKNOWN' WHERE id=?",(request,))
  service.complete_server_restart(manager,db,request)
  with db:db.execute("UPDATE force_continuations SET state='completed' WHERE request_id=?",(request,))
  return service.result(db.execute('SELECT * FROM service_requests WHERE id=?',(request,)).fetchone())
 except BaseException as error:
  db.rollback()
  if not claimed:raise
  with db:
   db.execute("UPDATE force_continuations SET state='outcome_unknown',detail=? WHERE request_id=? AND state!='completed'",(type(error).__name__+': '+str(error),request))
   db.execute("UPDATE service_requests SET state=CASE WHEN state='server_restart_intent' THEN state ELSE 'outcome_unknown' END,detail=? WHERE id=? AND state!='completed'",(type(error).__name__+': '+str(error),request))
  raise
 finally:
  if adapter is not None:adapter.close()
  if native_journal is not None:native_journal.close()
  journal.close()
