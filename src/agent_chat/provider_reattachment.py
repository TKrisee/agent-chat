"""Typed replacement receipts and read-only reattachment evidence.

A historical resume response is not a current thread-settings/read API.
Unknown responses never become permission/settings defaults or resume retries.
"""
import time
from . import provider_recovery_peer as peer
from .provider_recovery_wire import jq,field
Blocked=peer.Blocked

def _schema(journal):
 journal.db.execute('CREATE TABLE IF NOT EXISTS authenticated_resume_receipts(request_id TEXT PRIMARY KEY,observed_at REAL,provider_pid INTEGER,provider_uid INTEGER,birth_sec INTEGER,birth_usec INTEGER,code_hash TEXT,baseline_sha TEXT,manifest_sha TEXT,source_id TEXT,source_sha TEXT,settings TEXT,settings_sha TEXT)')

def _binding(adapter):
 adapter.pin_peer();process,code,_,_=adapter.proof
 return (process.pid,process.uid,process.birth_sec,process.birth_usec,code,adapter.baseline_sha,adapter.manifest_sha256,adapter.authority.source_id,adapter.authority.source_digest)

def record_resume(adapter,journal,request,response):
 # Called only after the actual pinned resume response passes known-setting checks.
 settings=jq(response,'with_entries(select(.key as $key | ["model","modelProvider","cwd","approvalPolicy","approvalsReviewer","reasoningEffort","sandbox","activePermissionProfile","runtimeWorkspaceRoots","multiAgentMode","serviceTier"]|index($key)))')
 binding=_binding(adapter);_schema(journal)
 with journal.db:
  if journal.db.execute('SELECT 1 FROM authenticated_resume_receipts WHERE request_id=?',(request,)).fetchone():raise Blocked('authenticated resume receipt already recorded; no overwrite')
  journal.db.execute('INSERT INTO authenticated_resume_receipts VALUES('+','.join('?' for _ in range(13))+')',(request,time.time(),*binding,settings,peer.digest(settings.encode())))

def resume_receipt(adapter,journal,request):
 _schema(journal)
 row=journal.db.execute('SELECT provider_pid,provider_uid,birth_sec,birth_usec,code_hash,baseline_sha,manifest_sha,source_id,source_sha,settings,settings_sha FROM authenticated_resume_receipts WHERE request_id=?',(request,)).fetchone()
 if not row:raise Blocked('authenticated resume response receipt unavailable; no resume replay or full-settings invention')
 if row[:9]!=_binding(adapter) or peer.digest(row[9].encode())!=row[10]:raise Blocked('authenticated resume receipt generation/config/source/baseline binding changed')
 return row[9]

def current_settings(baseline,current):
 # ThreadReadResponse explicitly exposes these fields for a loaded thread.
 # Permission/workspace fields are absent from the read API; global config/read
 # cannot establish their current thread-scoped values.
 return jq('{"baseline":'+baseline+',"current":'+current+'}', '''
 .baseline as $b | .current.thread as $t |
 ["model","modelProvider","cwd","reasoningEffort"] as $readable |
 ($b.known_settings|with_entries(select(.key as $k|$readable|index($k)))) as $known |
 ($t|with_entries(select(.key as $k|$known|has($k)))) as $actual |
 if $actual!=$known then error("current loaded thread settings differ or are unavailable") else
 {current_verified:$actual,
  current_unobservable_known_fields:(([($b.known_settings|keys[])|select(. as $k|$readable|index($k)|not)]
    +[($b.known_sandbox|keys[])|"sandbox."+.]
    +(if $b.known_profile_id!=null then ["activePermissionProfile.id"] else [] end))|sort),
  source:"pinned replacement thread/read; omitted known fields only have historical authenticated resume evidence"} end''')

def record_owner(adapter,journal,request,actual,actors):
 from . import provider_owner as owner
 old=adapter.original_root_cli;previous=(old[0][0],old[0][1],old[0][3],old[0][4])
 changed=actual[4][:3]!=old[:3] or actual[4][4]!=old[4]
 if changed and not owner.absent(previous):raise Blocked('original CLI generation remains; new owner cannot replace it')
 binding=journal.db.execute('SELECT home_project,root_session,host,thread_id,binding_generation FROM checkpoint_bindings WHERE request_id=?',(request,)).fetchone()
 if binding!=(*adapter.authority.root,actors.generation):raise Blocked('canonical Root binding generation changed before reattachment')
 # Actors.observe already proves sole kernel socket, exact no-prompt argv,
 # signed executable/dynamic code and same configured Root/thread ownership.
 if actors.observe()!=actual:raise Blocked('same-thread reattachment actor evidence changed')
 with journal.db:
  journal.db.execute('CREATE TABLE IF NOT EXISTS cli_reattachment_observations(request_id TEXT,observed_at REAL,prior_cli TEXT,current_cli TEXT,binding_generation REAL,original_exited INTEGER)')
  journal.db.execute('INSERT INTO cli_reattachment_observations VALUES(?,?,?,?,?,?)',(request,time.time(),repr(old),repr(actual[4]),actors.generation,int(changed)))

def verify_readonly(adapter,journal,request,deadline):
 from .provider_recovery_wire import Wire
 from . import native_owner_evidence as native
 from .provider_recovery_local_persistence import preserved_file
 from .provider_recovery_lifecycle import queue
 if time.monotonic()>=deadline:raise TimeoutError('read-only reconciliation deadline expired')
 preserved_file(adapter.baseline)
 adapter.rpc=Wire(native.endpoint(adapter.selected.endpoint),deadline)
 adapter.pin_peer();adapter.rpc.initialize();adapter.pin_peer()
 receipt=resume_receipt(adapter,journal,request)
 adapter.replacement_configuration_proof(journal,request)
 thread=adapter.authority.root[3]
 current=adapter.read('thread/read',jq('{}','{threadId:$thread,includeTurns:false}',thread=thread))
 jq(current,'if .thread.id!=$thread or (.thread.status.type!="idle" and .thread.status.type!="active") then error("same materialized Root not loaded") else . end',thread=thread)
 evidence=current_settings(adapter.baseline,current)
 # Also compare all readable fields actually returned by the earlier resume.
 current_settings(jq('{}','{known_settings:('+receipt+'|with_entries(select(.key as $key|["model","modelProvider","cwd","reasoningEffort"]|index($key)))),known_sandbox:{},known_profile_id:null}'),current)
 pending=queue(adapter,thread)
 preserved_file(adapter.baseline);adapter.restored_connections();adapter.authority.check()
 adapter.replacement_configuration_proof(journal,request);adapter.pin_peer()
 if time.monotonic()>=deadline:raise TimeoutError('read-only reconciliation original deadline expired')
 historical=journal.db.execute('SELECT observed_at,settings_sha FROM authenticated_resume_receipts WHERE request_id=?',(request,)).fetchone()
 evidence=jq(evidence,'.+{scope:"service_lifecycle_only",receipt_time_settings:('+receipt+'),receipt_observed_at:($at|tonumber),receipt_sha256:$sha,prior_native_input_outcomes:"UNKNOWN",full_current_settings_attested:false,overall_ticket_complete:false}',at=historical[0],sha=historical[1])
 with journal.db:
  journal.db.execute('CREATE TABLE IF NOT EXISTS readonly_restoration_proofs(request_id TEXT,observed_at REAL,provider_identity TEXT,baseline_sha TEXT,manifest_sha TEXT,evidence TEXT,evidence_sha TEXT,queue_sha TEXT,actor_proof TEXT)')
  journal.db.execute('INSERT INTO readonly_restoration_proofs VALUES(?,?,?,?,?,?,?,?,?)',(request,time.time(),repr(adapter.proof[:2]),adapter.baseline_sha,adapter.manifest_sha256,evidence,peer.digest(evidence.encode()),peer.digest(pending.encode()),repr(adapter.actor_proof)))
 return evidence
