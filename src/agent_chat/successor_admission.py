"""Fixed, immutable one-use successor packets under configured Root authority."""
import sqlite3
import contextlib
import time
RESOURCE="validation-clone"
REQUEST="4621FB6B-9D22-4C37-82AC-B4DC05999709"
E2D_REQUEST="E2D422F3-7D22-4412-9A3F-923D7D8999BF"
E2D_UNIT="HospitalNavFailureTimeCapture01-20261103"
BUILD_REQUEST="E2443DB8-68A9-4F08-A345-DFF2DEB35D21"
BUILD_UNIT="HospitalDowntownCombinedSessionBuild01-20261104"
NATIVE02_REQUEST="4CF2D3E5-E026-4720-90CF-7E379B9DA461"
NATIVE02_UNIT="HospitalDowntownCorrectedRouteSessionNative02-20261105"
BUILD_SOURCE="message_AfkfmfDcRtRKdYPcoSTC_B-Cw7jRZ9be"
BUILD_BODY="can we hurry up? we are ~1month behind schedule just start working on the damn maps, models and gameplay features!"
BUILD_REQUESTS=(BUILD_REQUEST,NATIVE02_REQUEST)
REQUESTS=(REQUEST,E2D_REQUEST)+BUILD_REQUESTS
CONSUMER="session_8f421d93a401439ebdb5906138c808fd"
THREAD="01a11d1f-76b8-7603-84c4-df6ad8e36891"
HOST="host_o6KZLd0kkjchCaH8OjoJZ9aZXHDlFiE2"
ROOT="session_tcnSV4Qo3EH4YU3gg2swqjO_GS1QQ9Fv"
ROOT_THREAD="01a1448a-c377-73e0-9025-02c4f2f6f2d9"
GUARD=("remote_guarded_runs","remote_run_d6b4b581f256e9f9c2b0ed9954221b3f",RESOURCE,
       "reservation_ozTM0ysovUwnzjB95HWDce4hRIw4bP__","session_d96238daad0345b699ec1dd1972ab11b",
       HOST,27633,None,1791669860.7163389,None)
class Refused(RuntimeError): pass
class PendingSuccessorRefused(Refused): pass
class DiagnosticRefused(Refused):
    def __init__(self, request_id, attempt_id, phase, predicate, outcome='refused'):
        super().__init__('fixed successor diagnostic refusal')
        self.request_id, self.attempt_id = request_id, attempt_id
        self.phase, self.predicate, self.outcome = phase, predicate, outcome


def _require_unconsumed_allowance(request_id, actor, host, target, registry, project):
    # These fixed historical IDs have no positive eligibility evidence. The journal
    # describes observations only; neither absence nor a diagnostic ID permits replay.
    from .native02_readback import inspect_attempt, ReadbackUnavailable
    try:
        result=inspect_attempt(target,registry=registry,home_project=project,
                               home_session=actor,host_id=host,request_id=request_id,attempt_id=None)
        observation=result['producer_attempt']
        state=observation['state']
        predicate=('recorded_consumed' if result['consumption']=='recorded_use' else
                   'legacy_history_unknown' if state=='legacy_history_unknown' else 'recorded_'+state)
        attempt=observation.get('attempt_id')
    except ReadbackUnavailable:
        predicate='history_unavailable'; attempt=None
    raise DiagnosticRefused(request_id,attempt,'historical_classification',predicate,'unavailable')


def _build_source(db):
    rows=db.execute("""SELECT m.sender_session,m.recipient_session,m.body,o.sender_session,o.channel
        FROM messages m JOIN operator_message_origins o ON o.message_id=m.id
        JOIN operator_identities i ON i.session_id=m.sender_session WHERE m.id=?""",(BUILD_SOURCE,)).fetchmany(2)
    if len(rows)!=1 or tuple(rows[0])!=("web_operator_qYmkTK9qVoubfWPPTLr8iSzaIKqKk3Si",ROOT,BUILD_BODY,"web_operator_qYmkTK9qVoubfWPPTLr8iSzaIKqKk3Si","authenticated_operator_ui"):
        raise Refused("Build01 operator authority differs")
    tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if 'message_batches' in tables and db.execute('SELECT 1 FROM message_batches WHERE message_id=?',(BUILD_SOURCE,)).fetchone():
        raise Refused("Build01 operator authority is batched")

def _build_capacity(db,old):
    row=db.execute("SELECT owner_session,reservation_id,token,granted_at,deadline,stale,reason FROM resources WHERE name='heavy-unity:2'").fetchone()
    if row is None or tuple(row)!=(None,None,None,None,None,0,None) or db.execute("SELECT 1 FROM resource_queue WHERE resource='heavy-unity:2'").fetchone() or old._raw_guards(db,'heavy-unity:2'):
        raise Refused("Build01 heavy capacity unavailable")
def install_schema(db):
    db.execute("""CREATE TABLE IF NOT EXISTS successor_admissions(
        request_id TEXT PRIMARY KEY,resource TEXT NOT NULL,consumer TEXT NOT NULL,
        thread TEXT NOT NULL,host TEXT NOT NULL,project_root TEXT NOT NULL,
        manager TEXT,authority_kind TEXT NOT NULL,management_request TEXT,generation INTEGER NOT NULL,
        guard_table TEXT NOT NULL,guard_run TEXT NOT NULL,guard_reservation TEXT NOT NULL,
        guard_owner TEXT NOT NULL,guard_host TEXT NOT NULL,guard_pid INTEGER NOT NULL,
        guard_pgid INTEGER,guard_started REAL NOT NULL,guard_closed REAL,
        process_state TEXT NOT NULL CHECK(process_state='UNKNOWN'))""")
    db.execute("""CREATE TABLE IF NOT EXISTS successor_admission_uses(
        request_id TEXT PRIMARY KEY REFERENCES successor_admissions(request_id),
        resource TEXT NOT NULL,consumer TEXT NOT NULL,reservation_id TEXT UNIQUE NOT NULL,used_at REAL NOT NULL)""")
    db.execute("""CREATE TABLE IF NOT EXISTS successor_admission_observations(
        request_id TEXT PRIMARY KEY REFERENCES successor_admissions(request_id),
        consumer TEXT NOT NULL,thread TEXT NOT NULL,host TEXT NOT NULL,native_state TEXT NOT NULL,
        direct_input INTEGER NOT NULL,queue_count INTEGER NOT NULL,turn_id_known INTEGER NOT NULL,
        started_at REAL NOT NULL,finished_at REAL NOT NULL)""")
    for table in ("successor_admissions","successor_admission_uses","successor_admission_observations"):
        for op in ("UPDATE","DELETE"):
            db.execute(f"CREATE TRIGGER IF NOT EXISTS {table}_{op.lower()} BEFORE {op} ON {table} BEGIN SELECT RAISE(ABORT,'immutable successor evidence'); END")
def _check(db,resource,session,gate,new_rid=None,native_guard=None):
    from . import capacity_admission as old, capacity_observer as observer
    if resource!=RESOURCE: return None
    tables={row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if "successor_admissions" not in tables: return None
    if "successor_admission_uses" not in tables: return False
    rows=db.execute("""SELECT * FROM successor_admissions t WHERE resource=?
      AND NOT EXISTS(SELECT 1 FROM successor_admission_uses u WHERE u.request_id=t.request_id) LIMIT 2""",(resource,)).fetchall()
    if not rows: return None
    if len(rows)!=1 or session!=CONSUMER or not isinstance(gate,old._RegistryGate): return False
    row=rows[0]
    try:
        gate.check()
        if not isinstance(native_guard,_NativeGuard): return False
        native_guard.check(db)
        scope={"home_project":"default","home_session":ROOT,"root_thread":ROOT_THREAD,"host":HOST,
               "target_project":"default","project_root_session":row["project_root"],
               "project_orchestrator_session":row["manager"]}
        old._verify_registry_scope(gate.db,scope)
        if old._admission_candidate(db,resource)[0]!="none": return False
        if row["request_id"] not in REQUESTS: return False
        if row["request_id"] in BUILD_REQUESTS: _build_capacity(db,old)
        observation=db.execute("SELECT consumer,thread,host,native_state,direct_input,queue_count,turn_id_known FROM successor_admission_observations WHERE request_id=?",(row["request_id"],)).fetchone()
        if not observation or tuple(observation)!=(CONSUMER,THREAD,HOST,"idle",1,0,0): return False
        project=db.execute("SELECT value FROM meta WHERE key='project_id'").fetchone()
        generation=db.execute("SELECT generation FROM root_guard_generation WHERE singleton=1").fetchone()
        binding=db.execute("SELECT thread_id,parent_session FROM bridge_bindings WHERE session_id=?",(CONSUMER,)).fetchone()
        host=db.execute("SELECT host_id FROM remote_session_hosts WHERE session_id=?",(CONSUMER,)).fetchone()
        if not project or project["value"]!="default" or not generation or generation["generation"]!=row["generation"] or not binding or binding["thread_id"]!=THREAD or not host or host["host_id"]!=HOST:
            return False
        authority=observer._owner_authority(db,scope,CONSUMER,binding,host)
        manager=authority["link"]["manager_session"] if authority["kind"]=="managed" else scope["project_orchestrator_session"]
        management=authority["request"]["id"] if authority["kind"]=="managed" else None
        if (row["consumer"],row["thread"],row["host"],row["process_state"],row["authority_kind"],row["manager"],row["management_request"]) != (CONSUMER,THREAD,HOST,"UNKNOWN",authority["kind"],manager,management):
            return False
        resource_row=db.execute("SELECT owner_session,reservation_id,stale FROM resources WHERE name=?",(RESOURCE,)).fetchone()
        if new_rid is None:
            if resource_row is None or tuple(resource_row)!=(None,None,0): return False
            if db.execute("SELECT 1 FROM resource_queue WHERE resource=?",(RESOURCE,)).fetchone(): return False
        elif resource_row is None or tuple(resource_row)!=(CONSUMER,new_rid,0): return False
        guards=old._raw_guards(db,resource)
        if len(guards)!=1: return False
        table,g=guards[0]
        actual=(table,g["run_id"],g["resource"],g["reservation_id"],g["owner_session"],g.get("host_id"),g["pid"],g.get("pgid"),g["started_at"],g["closed_at"])
        audit=(row["guard_table"],row["guard_run"],resource,row["guard_reservation"],row["guard_owner"],row["guard_host"],row["guard_pid"],row["guard_pgid"],row["guard_started"],row["guard_closed"])
        return actual==GUARD and audit==GUARD
    except (Refused,old.AdmissionRefused,observer.Blocked,sqlite3.Error,KeyError,TypeError):
        return False
def current(db,resource,session,registry_gate,native_guard=None):
    return _check(db,resource,session,registry_gate,native_guard=native_guard)
def consume(db,resource,session,new_rid,registry_gate,native_guard=None):
    if _check(db,resource,session,registry_gate,new_rid,native_guard) is not True:
        raise Refused("successor admission changed before use")
    request=db.execute("""SELECT request_id FROM successor_admissions t WHERE resource=?
        AND NOT EXISTS(SELECT 1 FROM successor_admission_uses u WHERE u.request_id=t.request_id)""",(resource,)).fetchone()[0]
    db.execute("INSERT INTO successor_admission_uses VALUES(?,?,?,?,?)",(request,resource,session,new_rid,time.time()))

def _require_e2d_predecessor(db):
    """A new packet never edits, replenishes or substitutes the spent 4621 use."""
    prior=db.execute("""SELECT t.resource,t.consumer,t.thread,t.host,t.process_state,
        u.resource,u.consumer,u.reservation_id FROM successor_admissions t
        JOIN successor_admission_uses u ON u.request_id=t.request_id WHERE t.request_id=?""",(REQUEST,)).fetchone()
    if (not prior or tuple(prior)[:7] != (RESOURCE,CONSUMER,THREAD,HOST,"UNKNOWN",RESOURCE,CONSUMER)
            or not isinstance(prior[7],str) or not prior[7]):
        raise Refused("E2D requires the matching spent 4621 packet")
    if db.execute("""SELECT 1 FROM successor_admissions t WHERE resource=?
        AND NOT EXISTS(SELECT 1 FROM successor_admission_uses u WHERE u.request_id=t.request_id)""",(RESOURCE,)).fetchone():
        raise Refused("successor packet still pending")
    if db.execute("SELECT 1 FROM successor_admissions WHERE request_id=?",(E2D_REQUEST,)).fetchone():
        raise Refused("E2D packet already recorded")

def _producer_snapshot(db,scope,observer,old):
    if not db.execute("SELECT 1 FROM sessions WHERE id=?",(CONSUMER,)).fetchone():
        raise Refused("consumer registration missing")
    project=db.execute("SELECT value FROM meta WHERE key='project_id'").fetchone()
    generation=db.execute("SELECT generation FROM root_guard_generation WHERE singleton=1").fetchone()
    binding=db.execute("SELECT thread_id,parent_session FROM bridge_bindings WHERE session_id=?",(CONSUMER,)).fetchone()
    host=db.execute("SELECT host_id FROM remote_session_hosts WHERE session_id=?",(CONSUMER,)).fetchone()
    resource=db.execute("SELECT name,owner_session,reservation_id,token,granted_at,deadline,stale,reason FROM resources WHERE name=?",(RESOURCE,)).fetchone()
    if not project or project["value"]!="default" or not generation or not binding or binding["thread_id"]!=THREAD or not host or host["host_id"]!=HOST:
        raise Refused("consumer, project or generation differs")
    if resource is None or tuple(resource)!=(RESOURCE,None,None,None,None,None,0,None):
        raise Refused("resource is not exactly free")
    if db.execute("SELECT 1 FROM resource_queue WHERE resource=?",(RESOURCE,)).fetchone():
        raise Refused("selected FIFO is not empty")
    guards=old._raw_guards(db,RESOURCE)
    if len(guards)!=1: raise Refused("guard set differs")
    table,g=guards[0]
    actual=(table,g["run_id"],g["resource"],g["reservation_id"],g["owner_session"],g.get("host_id"),g["pid"],g.get("pgid"),g["started_at"],g["closed_at"])
    if actual!=GUARD: raise Refused("guard tuple differs")
    authority=observer._owner_authority(db,scope,CONSUMER,binding,host)
    return {"generation":generation["generation"],"authority":authority,
            "registration":observer._registered_bridge(db,HOST),
            "binding":{"session":CONSUMER,"thread":THREAD,"host":HOST}}

def _same_snapshot(a,b,observer):
    return (a["generation"],a["authority"],a["binding"],observer._bridge_identity(a["registration"])) == (b["generation"],b["authority"],b["binding"],observer._bridge_identity(b["registration"]))

def issue(target_path,*,master,registry,home_project,home_session,host_id,target_project,request_id,authorization_source_id,confirm):
    from pathlib import Path
    from . import capacity_admission as old, capacity_observer as observer, successor_attempt_journal as journal
    source=BUILD_SOURCE if request_id in BUILD_REQUESTS else "message_KJqomMYywtU6GYfczB_fxftks4D_gz6y"
    if (home_project,home_session,host_id,target_project,authorization_source_id,confirm)!=("default",ROOT,HOST,"default",source,True) or request_id not in REQUESTS:
        raise Refused('operation packet differs')
    old._require_configured_root_caller(master,home_project,home_session,host_id,target_project,request_id,authorization_source_id,True,registry)
    if Path(target_path).resolve()!=Path(master).resolve(): raise Refused('target differs')
    _require_unconsumed_allowance(request_id,home_session,host_id,Path(target_path),registry,home_project)
    with journal.verified_store(Path(target_path), actor=home_session,host=host_id,project=target_project,root_thread=ROOT_THREAD) as (db,check_store):
        return _issue_verified(target_path, master=master, registry=registry,home_project=home_project,home_session=home_session,host_id=host_id,target_project=target_project,request_id=request_id,authorization_source_id=authorization_source_id,confirm=confirm,db=db,check_store=check_store)


def _issue_verified(target_path, *, master,registry,home_project,home_session,host_id,target_project,request_id,authorization_source_id,confirm,db,check_store):
    from pathlib import Path
    from . import capacity_admission as old, capacity_observer as observer, successor_attempt_journal as journal
    operation={REQUEST:'capacity-successor-4621',E2D_REQUEST:'capacity-successor-e2d',BUILD_REQUEST:'capacity-successor-build01',NATIVE02_REQUEST:'capacity-successor-native02'}[request_id]
    try:
        attempt=journal.begin(db,check_store,request_id=request_id,operation=operation,actor=home_session,host=host_id,target=str(Path(target_path).resolve()),source=authorization_source_id,project=target_project)
    except journal.AttemptAlreadyRecorded as error:
        check_store(); recorded=journal.status_db(db,request_id); check_store()
        raise DiagnosticRefused(request_id,recorded.get('attempt_id'),'admission','attempt_already_recorded') from error
    store_db=db
    tracker={'phase':'attempt_started'}
    def event(phase, db=None):
        tracker['phase']=phase
        check_store()
        if db is None: journal.append(store_db,check_store,request_id,attempt,phase)
        else: journal.append_on(db,request_id,attempt,phase)
    try:
        return _issue_impl(target_path,master=master,registry=registry,home_project=home_project,home_session=home_session,host_id=host_id,target_project=target_project,request_id=request_id,authorization_source_id=authorization_source_id,confirm=confirm,_event=event,_store_db=db,_check_store=check_store)
    except BaseException as error:
        # A committed event is atomic with the packet; a later lost response cannot erase it.
        known={
            'consumer direct input false':'native_direct_input_false',
            'consumer direct input unknown':'native_direct_input_unknown',
            'consumer native observation missing':'native_observation_missing',
            'consumer native observation differs':'native_tuple_differs',
            'current native owner is not positively idle':'native_owner_not_positively_idle',
            'current native owner is not positively idle: direct-input false':'native_direct_input_false',
            'current native direct-input status is unknown':'native_direct_input_unknown',
            'Build01 operator authority differs':'operator_authority_differs',
            'Build01 operator authority is batched':'operator_authority_batched',
            'Build01 heavy capacity unavailable':'heavy_capacity_unavailable',
            'consumer registration missing':'consumer_registration_missing',
            'consumer, project or generation differs':'consumer_project_or_generation_differs',
            'resource is not exactly free':'resource_not_exactly_free',
            'selected FIFO is not empty':'selected_fifo_not_empty',
            'guard set differs':'guard_set_differs',
            'guard tuple differs':'guard_tuple_differs',
            'Root thread differs':'root_thread_differs',
            'preflight changed':'preflight_snapshot_changed',
            'action-boundary snapshot differs':'action_boundary_snapshot_changed',
            'old ticket still pending':'old_ticket_pending',
            'E2D requires the matching spent 4621 packet':'predecessor_not_qualified',
            'E2D packet already recorded':'predecessor_packet_already_recorded',
            'successor packet already recorded':'successor_packet_already_recorded',
            'precommit changed':'precommit_snapshot_changed',
            'successor pending set changed':'successor_pending_set_changed',
            'successor packet still pending':'successor_packet_pending',
        }
        is_refusal=isinstance(error,(Refused,old.AdmissionRefused,observer.Blocked))
        predicate=known.get(str(error),'unclassified_or_unavailable')
        known_failure = is_refusal and predicate not in ('native_direct_input_unknown','native_observation_missing','unclassified_or_unavailable')
        try:
            journal.failure(db,check_store,request_id,attempt,tracker['phase'],predicate,known_failure)
        except Exception as journal_error:
            # A missing terminal record remains unknown; never replace the original failure.
            raise error from journal_error
        if is_refusal: raise DiagnosticRefused(request_id,attempt,tracker['phase'],predicate,
                                              'refused' if known_failure else 'unknown') from error
        raise


def _issue_impl(target_path,*,master,registry,home_project,home_session,host_id,target_project,request_id,authorization_source_id,confirm,_event,_store_db,_check_store):
    from contextlib import closing
    from pathlib import Path
    from . import capacity_admission as old, capacity_observer as observer
    from .successor_attempt_journal import _validate_schema as journal_schema_check
    source=BUILD_SOURCE if request_id in BUILD_REQUESTS else "message_KJqomMYywtU6GYfczB_fxftks4D_gz6y"
    if (home_project,home_session,host_id,target_project,authorization_source_id,confirm)!=("default",ROOT,HOST,"default",source,True) or request_id not in REQUESTS:
        raise Refused("operation packet differs")
    registry,owner,identity=old._require_configured_root_caller(master,home_project,home_session,host_id,target_project,request_id,authorization_source_id,True,registry)
    if Path(target_path).resolve()!=Path(master).resolve(): raise Refused("target differs")
    _event('source_read_started')
    if request_id in BUILD_REQUESTS:
        _build_source(_store_db)
    else: old._verify_operator_source_db(_store_db,authorization_source_id,home_session)
    _event('source_read_completed')
    scope=observer._root_scope(registry,home_project,home_session,host_id,target_project)
    if scope["root_thread"]!=ROOT_THREAD: raise Refused("Root thread differs")
    _event('snapshot_before_started')
    before=_producer_snapshot(_store_db,scope,observer,old)
    if request_id in BUILD_REQUESTS: _build_capacity(_store_db,old)
    _event('snapshot_before_completed')
    _event('native_read_started')
    with observer._native_observations({CONSUMER:before["binding"]},before["registration"]) as (native,started,finished,revalidate):
        _event('native_read_completed')
        if set(native)!={CONSUMER}: raise Refused("native observation missing")
        n=native[CONSUMER]
        if n.get('direct_input') is False: raise Refused('consumer direct input false')
        if n.get('direct_input') is not True: raise Refused('consumer direct input unknown')
        if (n.get("thread_id"),n.get("host"),n.get("native_state"),n.get("queue_count"),n.get("turn_id_known"))!=(THREAD,HOST,"idle",0,False):
            raise Refused("consumer native observation differs")
        _event('snapshot_after_started')
        after=_producer_snapshot(_store_db,scope,observer,old)
        if request_id in BUILD_REQUESTS: _build_capacity(_store_db,old)
        if not _same_snapshot(before,after,observer): raise Refused("preflight changed")
        _event('snapshot_after_completed')
        _event('transaction_pending')
        with old.registry_authority_gate(registry,owner,identity) as (gate,check):
            db=_store_db
            _check_store()
            try:
                db.execute("BEGIN IMMEDIATE")
                _check_store(); journal_schema_check(db)
                _event('transaction_started', db=db)
                check(); old._verify_registry_scope(gate.db,scope)
                if request_id in BUILD_REQUESTS: _build_source(db); _build_capacity(db,old)
                else: old._verify_operator_source_db(db,authorization_source_id,home_session)
                old._require_fresh_observation(started,finished,time.time())
                current=_producer_snapshot(db,scope,observer,old)
                if not _same_snapshot(before,current,observer): raise Refused("action-boundary snapshot differs")
                revalidate(current["registration"])
                old._require_fresh_observation(started,finished,time.time())
                if old._admission_candidate(db,RESOURCE)[0]!="none": raise Refused("old ticket still pending")
                if request_id==E2D_REQUEST: _require_e2d_predecessor(db)
                if request_id in BUILD_REQUESTS and db.execute("""SELECT 1 FROM successor_admissions t WHERE resource=?
                    AND NOT EXISTS(SELECT 1 FROM successor_admission_uses u WHERE u.request_id=t.request_id)""",(RESOURCE,)).fetchone(): raise PendingSuccessorRefused("successor packet still pending")
                if request_id in BUILD_REQUESTS and db.execute("SELECT 1 FROM successor_admissions WHERE request_id=?",(request_id,)).fetchone(): raise Refused("successor packet already recorded")
                a=current["authority"]
                manager=a["link"]["manager_session"] if a["kind"]=="managed" else scope["project_orchestrator_session"]
                management=a["request"]["id"] if a["kind"]=="managed" else None
                db.execute("INSERT INTO successor_admissions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                  (request_id,RESOURCE,CONSUMER,THREAD,HOST,scope["project_root_session"],manager,a["kind"],management,current["generation"],
                   GUARD[0],GUARD[1],GUARD[3],GUARD[4],GUARD[5],GUARD[6],GUARD[7],GUARD[8],GUARD[9],"UNKNOWN"))
                db.execute("INSERT INTO successor_admission_observations VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (request_id,CONSUMER,THREAD,HOST,"idle",1,0,0,started,finished))
                check()
                if not _same_snapshot(_producer_snapshot(db,scope,observer,old),before,observer): raise Refused("precommit changed")
                if request_id in BUILD_REQUESTS: _build_source(db); _build_capacity(db,old)
                if request_id in BUILD_REQUESTS:
                    pending=db.execute("""SELECT request_id FROM successor_admissions t WHERE resource=?
                        AND NOT EXISTS(SELECT 1 FROM successor_admission_uses u WHERE u.request_id=t.request_id)""",(RESOURCE,)).fetchall()
                    if len(pending)!=1 or pending[0][0]!=request_id: raise Refused("successor pending set changed")
                old._require_fresh_observation(started,finished,time.time())
                _event('committed', db=db)
                _check_store(); db.commit(); _check_store()
            except BaseException:
                if db.in_transaction: db.rollback()
                raise
    result={"request_id":request_id,"resource":RESOURCE,"process_state":"UNKNOWN","ordinary_receipt":False}
    if request_id==E2D_REQUEST: result["unit"]=E2D_UNIT
    if request_id==BUILD_REQUEST: result["unit"]=BUILD_UNIT
    if request_id==NATIVE02_REQUEST: result["unit"]=NATIVE02_UNIT
    return result

class _NativeGuard:
    def __init__(self, registration, native, started, finished, revalidate):
        self.registration=registration; self.native=native; self.started=started
        self.finished=finished; self.revalidate=revalidate; self.active=True
    def check(self, db):
        from . import capacity_admission as old, capacity_observer as observer
        if not self.active: raise Refused("native continuity closed")
        old._require_fresh_observation(self.started,self.finished,time.time())
        now=observer._registered_bridge(db,HOST)
        if observer._bridge_identity(now)!=observer._bridge_identity(self.registration):
            raise Refused("registered bridge changed")
        self.revalidate(now)
        old._require_fresh_observation(self.started,self.finished,time.time())

@contextlib.contextmanager
def observe_for_request(db,resource,session):
    from . import capacity_observer as observer
    if resource!=RESOURCE or session!=CONSUMER:
        yield None
        return
    tables={row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if not {"successor_admissions","successor_admission_uses"}<=tables:
        yield None
        return
    pending=db.execute("""SELECT 1 FROM successor_admissions t WHERE resource=? AND
        NOT EXISTS(SELECT 1 FROM successor_admission_uses u WHERE u.request_id=t.request_id) LIMIT 1""",(resource,)).fetchone()
    if not pending:
        yield None
        return
    registration=observer._registered_bridge(db,HOST)
    binding={CONSUMER:{"session":CONSUMER,"thread":THREAD,"host":HOST}}
    with observer._consumer_native_observations(binding,registration) as (native,started,finished,revalidate):
        n=native.get(CONSUMER)
        if (not n or (n.get("thread_id"),n.get("host"),n.get("direct_input"),n.get("queue_count"),n.get("turn_id_known")) != (THREAD,HOST,True,0,False) or n.get("native_state") not in ("idle","active")):
            raise Refused("current native observation unavailable")
        guard=_NativeGuard(registration,native,started,finished,revalidate)
        try:
            yield guard
        finally:
            guard.active=False
