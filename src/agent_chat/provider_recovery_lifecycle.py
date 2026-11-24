"""V3 private correction: derived actors and fresh trusted pre-stop evidence."""
from contextlib import closing
import hashlib
import math
import os
from pathlib import Path
import plistlib
import pwd
import re
import signal
import sqlite3
import sys
import time
import subprocess
from .provider_recovery_wire import Wire, jq, field  # Load this candidate's transport before inherited v2 paths.

from . import provider_recovery_peer as v2
from . import provider_recovery_launch_config as launch_config
from agent_chat import native_owner_evidence as native
from agent_chat.client_continuity import ClientContinuity
from agent_chat.root_owner import RootOwnerMonitor
from .provider_recovery_wire import Wire, jq, field

Blocked = v2.Blocked
class PreviousClientRegistrationPending(Blocked):
    """The exact previous client registration remains during owned turnover."""

class CurrentOwnedClientNotReady(Blocked):
    """Current owned client is reporting an error, not readiness."""

def previous_client_registration_pending(registration, binding, endpoint, thread, app_pid, pending):
    return bool(pending and registration and binding and registration[0] == endpoint
        and app_pid == pending[1] and binding[0] == thread
        and ((registration[1] == pending[0] and not registration[3])
             or (registration[1] == 0 and registration[3] == 'bridge stopped')))

def actor_registration_failures(registration, binding, endpoint, thread, app, now):
    """Return categorical predicate failures only; never emit field values."""
    failures = []
    if not registration:
        failures.append('registration_missing')
    else:
        if registration[0] != endpoint: failures.append('endpoint_mismatch')
        if registration[1] != app[0]: failures.append('client_pid_mismatch')
        if registration[3]: failures.append('runtime_error')
        heartbeat = registration[2]
        if not isinstance(heartbeat, (int, float)) or not math.isfinite(heartbeat):
            failures.append('heartbeat_unavailable')
        else:
            age = now - heartbeat
            if age < 0: failures.append('heartbeat_future')
            if age > 15: failures.append('heartbeat_stale')
            if heartbeat < app[3] + app[4] / 1e6: failures.append('heartbeat_before_birth')
    if not binding:
        failures.append('binding_missing')
    elif binding[0] != thread:
        failures.append('thread_mismatch')
    return failures

CLI_IMAGE = native.IMAGE
CLI_SHA = native.IMAGE_SHA


def terminal_thread(raw, thread):
    return jq(raw, 'if .thread.id != $thread or .thread.status.type != "idle" '
        'or .thread.canAcceptDirectInput != true or (.thread.turns|type)!="array" '
        'then error("current Root thread state is not positively idle/direct/materialized") else .thread end '
        '| if any(.turns[]; .status != "completed" and .status != "failed" and .status != "interrupted") '
        'then error("current Root turn state is active or unknown") else . end '
        '| if (.turns|length)>0 and (.turns[-1].status != "completed" or .turns[-1].error != null '
        'or ([.turns[-1].items[]? | select(.type=="agentMessage" and .phase=="final_answer" '
        'and (.id|type)=="string" and (.text|type)=="string" and (.text|length)>0)]|length)==0) '
        'then error("latest Root response is not positively terminal") else . end', thread=thread)


class Actors:
    """Actual configured client plus exact connected, signed canonical resume CLI."""
    def __init__(self, authority, selected):
        self.authority, self.selected = authority, selected
        self.first = None

    def continuity(self, manager):
        return ClientContinuity(manager)

    def cli_evidence(self, pid, identity, image, argv, owner_identity):
        return native.observed_generation(pid,(identity[3],identity[4]),image,CLI_SHA)

    def observe(self):
        authority, selected = self.authority, self.selected
        authority.check()
        config = v2.service._installed_server_config()
        manager = v2.service.ServiceManager(system='Darwin', home=Path(pwd.getpwuid(config.uid).pw_dir),
                                            server_bin=str(config.executable))
        continuity = self.continuity(manager)
        inspected = continuity.inspect()
        process = inspected['process']
        if not process:
            raise Blocked('configured managed client is not running')
        paths = manager.paths('client')
        loaded = continuity._run(['launchctl', 'print', continuity.target])
        if loaded.returncode or loaded.stderr:
            raise Blocked('configured client loaded service observation unavailable')
        for name, expected in (('path', str(paths['config'])), ('program', str(paths['wrapper']))):
            values = re.findall(r'^\s*' + name + r' = (.+?)\s*$', loaded.stdout, re.MULTILINE)
            if values != [expected]:
                raise Blocked('loaded configured client ' + name + ' differs')
        try:
            with closing(v2.readonly(authority.master)) as db:
                registration = db.execute('SELECT server,pid,heartbeat,error FROM bridge_client_runtime WHERE host_id=?',
                                          (authority.root[2],)).fetchone()
                binding = db.execute('SELECT thread_id,updated_at FROM bridge_bindings WHERE session_id=?',
                                     (authority.root[1],)).fetchone()
        except (OSError, sqlite3.Error) as error:
            raise Blocked('actor_store_unavailable: ' + type(error).__name__) from None
        app = v2.kernel_identity(process['pid'])
        pending = getattr(self, 'pending_client_transition', None)
        if previous_client_registration_pending(registration,binding,selected.endpoint,
                                                authority.root[3],app[0],pending):
            raise PreviousClientRegistrationPending('previous client registration has not transitioned')
        failures = actor_registration_failures(registration, binding, selected.endpoint, authority.root[3], app, time.time())
        if (failures == ['runtime_error'] and pending and registration[1] == pending[1]
                and app[0] == pending[1]):
            raise CurrentOwnedClientNotReady('owned client reports an error; readiness unverified')
        if failures:
            raise Blocked('configured client runtime/generation or canonical Root binding differs: ' + ','.join(failures))
        host, port = native.endpoint(selected.endpoint)
        suffix = '->%s:%s' % (host, port)
        rows = native.DarwinEvidence.lsof('-iTCP@' + host + ':' + str(port), '-sTCP:ESTABLISHED')
        links = [row for row in rows if row[2].endswith(suffix)]
        app_links = [row for row in links if row[0] == app[0]]
        if len(app_links) != 1:
            raise Blocked('exact configured client connection absent or ambiguous')
        monitor = RootOwnerMonitor(selected.endpoint)
        target = dict(session_id=authority.root[1], thread_id=authority.root[3], generation=binding[1])
        self.generation = binding[1]
        candidates = []
        for pid in sorted({row[0] for row in links} - {app[0], selected.pid}):
            identity = v2.kernel_identity(pid)
            image, argv = continuity._kernel_command(pid)
            if Path(image).resolve() != Path(CLI_IMAGE).resolve():
                continue
            observed = monitor.process(pid)
            if not observed:
                continue
            # Kernel argv, not ps text, determines the exact no-prompt layout.
            observed['argv'] = argv
            observed['program'] = Path(image).name
            if not monitor.matches(observed, target):
                continue
            owner_identity=monitor.proof(observed,target)['owner_identity']
            self.current_image_sha,code=self.cli_evidence(pid,identity,image,argv,owner_identity)
            cli_links = [row for row in links if row[0] == pid]
            current_links = [row for row in native.DarwinEvidence.lsof('-iTCP@' + host + ':' + str(port), '-sTCP:ESTABLISHED')
                             if row[0] == pid and row[2].endswith(suffix)]
            if (len(cli_links) != 1 or current_links!=cli_links or v2.kernel_identity(pid) != identity
                    or continuity._kernel_command(pid)!=(image,argv)):
                raise Blocked('canonical Root CLI connection or generation changed')
            candidates.append((identity, tuple(argv), code, cli_links[0], owner_identity))
        if len(candidates) != 1:
            raise Blocked('exact connected canonical Root CLI absent or ambiguous')
        cli = candidates[0]
        if app[0] == cli[0][0]:
            raise Blocked('configured client and Root CLI must be distinct actor processes')
        if continuity.inspect() != inspected:
            raise Blocked('configured client changed during actor derivation')
        result = (app, inspected['config_sha256'], inspected['service_runs'], app_links[0], cli)
        stable = (app, result[1], result[2], cli[0], cli[1], cli[2], cli[4])
        if self.first is not None and stable != self.first:
            raise Blocked('configured client or canonical CLI ownership changed')
        self.first = stable
        return result


class OldPeer:
    """Existing native peer guard, unchanged; UNKNOWN image cannot make RPC trusted."""
    def __init__(self, selected, deadline, expected_image_sha=None, *, signed_live=False):
        self.selected, self.deadline = selected, deadline
        self.expected_image_sha = expected_image_sha or native.IMAGE_SHA
        self.signed_live = signed_live
        self.rpc = Wire(native.endpoint(selected.endpoint), deadline)
        self.proof = None
        try:
            self.pin()
            self.rpc.initialize()
            self.pin()
        except BaseException:
            self.rpc.close()
            raise

    def connection_identity(self):
        return dict(pid=os.getpid(), fd=self.rpc.sock.fileno(), local=self.rpc.sock.getsockname(), peer=self.rpc.sock.getpeername())

    def pin(self):
        try:
            kernel = native.DarwinEvidence()
            args = self.selected.endpoint,self.selected.pid,(self.selected.birth_sec,self.selected.birth_usec),self
            if self.signed_live:
                observed = kernel.sample_signed_listener(*args)
            else:
                observed = kernel.sample_independent_listener(*args,self.expected_image_sha)
            if self.proof is not None:
                native.same_owner(self.proof, observed, require_peer=True)
            self.proof = observed
        except Exception as error:
            raise Blocked('pre-stop Root turn/input state unavailable: existing old-peer trust guard failed: ' + str(error)) from error

    def read(self, method, params):
        self.pin()
        result = self.rpc.request(method, params)
        self.pin()
        return result

    def close(self):
        self.rpc.close()


def queue(peer, thread):
    pages, cursor, seen = [], None, set()
    for _ in range(10):
        params = jq('{}', '{threadId:$thread,limit:100}', thread=thread)
        if cursor:
            params = jq(params, '. + {cursor:$cursor}', cursor=cursor)
        page = peer.read('thread/queue/list', params)
        pages.append(jq(page, 'if (.data|type)!="array" then error("unknown queue") else .data end'))
        cursor = field(page, 'if .nextCursor==null then "<END>" else .nextCursor end')
        if cursor == '<END>':
            return jq('[' + ','.join(pages) + ']', 'add')
        if not cursor or cursor in seen:
            raise Blocked('queue cursor repeated')
        seen.add(cursor)
    raise Blocked('queue exceeds bounded capture')


class Recovery(v2.Recovery):
    dynamic_replacement_image = False

    def __init__(self, selected, manifest, sha, authority):
        self.actors = Actors(authority, selected)
        self.old_peer = None
        super().__init__(selected, manifest, sha, authority)

    def _config(self):
        raw = v2.trusted_file(self.manifest, os.getuid())
        if v2.digest(raw) != self.manifest_sha256:
            raise Blocked('replacement manifest changed')
        config = plistlib.loads(raw)
        if any(key in config for key in ('ObservedPeers','Baseline','BaselineSHA256')):
            raise Blocked('caller role labels or caller historical baselines are not accepted')
        argv = config.get('ProgramArguments')
        if (not isinstance(argv,list) or not argv or any(not isinstance(x,str) or not x for x in argv)
                or argv.count('--listen') != 1 or argv.index('--listen')+1 >= len(argv)
                or argv[argv.index('--listen')+1] != self.selected.endpoint or config.get('Endpoint') != self.selected.endpoint):
            raise Blocked('replacement exact command/endpoint unavailable')
        image = str(Path(config['Image']).resolve(strict=True))
        if str(Path(argv[0]).resolve(strict=True)) != image:
            raise Blocked('replacement executable differs')
        if self.dynamic_replacement_image:
            from .provider_owner import installed_image_sha
            image_sha = installed_image_sha(image)
            if getattr(self, 'replacement_sha', image_sha) != image_sha:
                raise Blocked('replacement executable changed during handoff')
            if self.baseline is not None:
                captured = jq(self.baseline, '.replacement_configuration')
                if captured != 'null':
                    staged_sha = field(captured, '.image_sha256 // ""') or config.get('ImageSHA256')
                    if staged_sha != image_sha:
                        raise Blocked('staged replacement executable changed')
        else:
            image_sha = config['ImageSHA256']
            if v2.digest(v2.trusted_file(image,os.getuid(),root_owned=True)) != image_sha:
                raise Blocked('replacement image changed')
        for path, sha in config.get('Artifacts',{}).items():
            if v2.digest(v2.trusted_file(path,os.getuid(),root_owned=True)) != sha:
                raise Blocked('replacement support artifact changed')
        cwd = config.get('WorkingDirectory')
        if not isinstance(cwd,str) or not Path(cwd).is_absolute() or not Path(cwd).is_dir():
            raise Blocked('replacement cwd unavailable')
        self.replacement_image, self.replacement_sha = image,image_sha
        self.launch_environment,_,_=launch_config.manifest(config,self.selected.uid)
        self.launch_manifest=config
        if self.baseline is not None:
            launch_config.check_baseline(config,self.baseline,self.selected.uid)
        return tuple(argv),cwd

    def replacement_configuration(self):
        actual = launch_config.bind(self.selected, self.launch_manifest)
        if self.dynamic_replacement_image:
            captured = jq(self.baseline, '.replacement_configuration') if self.baseline is not None else 'null'
            if captured == 'null' or field(captured, '.image_sha256 // ""'):
                actual = jq(actual, '.+{image_sha256:$sha}', sha=self.replacement_sha)
        return actual

    def selection(self):
        self.authority.check()
        self._config()
        v2.selected_kernel(self.selected)
        self.actor_proof = self.actors.observe()
        if self.baseline is not None:
            actual=self.replacement_configuration()
            if actual!=jq(self.baseline,'.replacement_configuration'):
                raise Blocked('selected store/location differs from staged replacement configuration')
        self.peers = [dict(PID=item[0], BirthSec=item[3], BirthUsec=item[4])
                      for item in (self.actor_proof[0],self.actor_proof[4][0])]
        v2.selected_kernel(self.selected)

    def prestate(self):
        thread = self.authority.root[3]
        raw = self.old_peer.read('thread/read',jq('{}','{threadId:$thread,includeTurns:true}',thread=thread))
        observed = terminal_thread(raw,thread)
        return jq(observed,'.turns'),queue(self.old_peer,thread)

    def capture(self,journal,request):
        self.selection()
        image_sha=getattr(self.actors,'current_image_sha',native.IMAGE_SHA)
        self.old_peer = (OldPeer(self.selected,self.deadline,signed_live=True) if image_sha is None
                         else OldPeer(self.selected,self.deadline) if image_sha==native.IMAGE_SHA
                         else OldPeer(self.selected,self.deadline,image_sha))
        history,pending = self.prestate()
        # Metadata attachment is a real mutation: intent BEFORE sending, no input
        # or settings overrides, and same-request retries never submit it again.
        journal.save(request,'prestop_resume_intent','same Root metadata attachment unknown','prestop_resume_unknown')
        observed = self.old_peer.read('thread/resume',jq('{}','{threadId:$thread,excludeTurns:true}',thread=self.authority.root[3]))
        if field(observed,'.thread.id') != self.authority.root[3]:
            raise Blocked('pre-stop metadata belongs to another thread')
        actual_settings = v2.settings(observed)
        pair = '{"baseline":'+actual_settings+',"recorded":'+self.authority.recorded+'}'
        if jq(pair,'.recorded as $r | .baseline | with_entries(select(.key as $k | $r|has($k)))') != self.authority.recorded:
            raise Blocked('current trusted settings disagree with available authenticated recorded fields')
        if self.prestate() != (history,pending):
            raise Blocked('current Root state changed during baseline capture')
        self.baseline = jq(actual_settings,'{threadId:$thread,settings:.,history:('+history+'),queue:('+pending+')}',thread=self.authority.root[3])
        self.baseline=jq(self.baseline,'.+{replacement_configuration:('+self.replacement_configuration()+')}')
        self.baseline_sha = v2.digest(self.baseline.encode())
        with journal.db:
            journal.db.execute('INSERT INTO prestop_proofs VALUES(?,?,?,?,?,?,?,?,?)',
                (request,time.time(),self.authority.root[3],self.baseline,self.baseline_sha,
                 repr(self.actor_proof),repr(self.old_peer.proof),self.authority.source_id,self.authority.source_digest))
            app,cli=self.actor_proof[0],self.actor_proof[4][0]
            journal.db.execute('INSERT INTO checkpoint_bindings VALUES('+','.join('?' for _ in range(15))+')',
                (request,*self.authority.root,self.actors.generation,app[0],app[3],app[4],cli[0],cli[3],cli[4],
                 self.actor_proof[1],self.actor_proof[4][2],self.actor_proof[4][4]))
        journal.save(request,'prestop_verified','fresh trusted same-Root terminal response/settings/history/queue captured','prestop_metadata_observed')

    def start_once(self):
        if self.launch_claimed or not self.closed():
            raise Blocked('replacement already claimed or exact original closure unproven')
        self.launch_claimed=True
        argv,cwd=self._config()
        environment=launch_config.check_baseline(self.launch_manifest,self.baseline,self.selected.uid)
        self.child=subprocess.Popen(argv,cwd=cwd,env=environment,start_new_session=True,
            stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        return self.child.pid

    def replacement_configuration_proof(self,journal,request):
        self._config()
        identity=v2.kernel_identity(self.child.pid)
        selected=v2.old_selection(identity[1],identity[0],identity[3],identity[4],self.selected.endpoint)
        proof=launch_config.verify_new(selected,self.baseline)
        with journal.db:
            journal.db.execute('CREATE TABLE IF NOT EXISTS replacement_configuration_proofs(request_id TEXT PRIMARY KEY,observed_at REAL,proof TEXT)')
            journal.db.execute('INSERT OR REPLACE INTO replacement_configuration_proofs VALUES(?,?,?)',(request,time.time(),proof))
        return proof

    def verify(self,journal,request,deadline):
        result=super().verify(journal,request,deadline)
        self.replacement_configuration_proof(journal,request)
        return result

    def stop_once(self):
        if self.term_claimed or self.watch is None:
            raise Blocked('TERM already claimed or watcher absent')
        self.term_claimed = True
        self.selection()
        if self.prestate() != (jq(self.baseline,'.history'),jq(self.baseline,'.queue')):
            raise Blocked('Root turn/input changed immediately before TERM')
        self.old_peer.pin()
        if time.monotonic() >= self.deadline:
            raise TimeoutError('deadline before TERM; no signal')
        os.kill(self.selected.pid,signal.SIGTERM)

    def restored_connections(self):
        # Re-derive actual configured service and exact canonical resume CLI; do
        # not merely count connections belonging to remembered role labels.
        self.actor_proof = self.actors.observe()

    def close(self):
        if self.old_peer is not None:
            self.old_peer.close()
            self.old_peer = None
        super().close()


def recover(request,source,selected,manifest,manifest_sha,*,timeout=120,_stage_only=False):
    if type(timeout) not in (int,float) or not 0 < timeout <= 120:
        raise Blocked('positive at-most-120-second budget required')
    deadline = time.monotonic()+timeout
    authority = v2.Authority(source)
    adapter = Recovery(selected,manifest,manifest_sha,authority)
    adapter.deadline = deadline
    directory = authority.master.parent/'provider-recovery-v3'
    directory.mkdir(mode=0o700,exist_ok=True)
    journal = v2.v1.Journal(directory/'requests.sqlite3')
    journal.db.execute('CREATE TABLE IF NOT EXISTS prestop_proofs(request_id TEXT PRIMARY KEY,observed_at REAL,'
        'thread_id TEXT,baseline TEXT,baseline_sha TEXT,actor_proof TEXT,peer_proof TEXT,source_id TEXT,source_sha TEXT)')
    journal.db.execute('CREATE TABLE IF NOT EXISTS checkpoint_bindings(request_id TEXT PRIMARY KEY,home_project TEXT,'
        'root_session TEXT,host TEXT,thread_id TEXT,binding_generation REAL,app_pid INTEGER,app_birth_sec INTEGER,'
        'app_birth_usec INTEGER,cli_pid INTEGER,cli_birth_sec INTEGER,cli_birth_usec INTEGER,client_config_sha TEXT,'
        'cli_code_hash TEXT,cli_owner_identity TEXT)')
    journal.db.execute('CREATE TABLE IF NOT EXISTS poststop_proofs(request_id TEXT PRIMARY KEY,verified_at REAL,'
        'source_id TEXT,source_sha TEXT,home_project TEXT,root_session TEXT,host TEXT,thread_id TEXT,provider_pid INTEGER,'
        'birth_sec INTEGER,birth_usec INTEGER,image_path TEXT,image_sha TEXT,code_hash TEXT,connection_local TEXT,'
        'connection_peer TEXT,history_sha TEXT,settings_sha TEXT,queue_sha TEXT,recorded_present TEXT,recorded_missing TEXT,'
        'recorded_checked_at REAL,manifest_sha TEXT,baseline_sha TEXT,actor_proof TEXT)')
    journal.db.commit()
    try:
        plan = adapter.plan_identity()+'\n'+repr(authority.root)+'\n'+source+'\n'+authority.source_digest
        old = journal.claim(request,selected,plan)
        if old:
            return v2.Result(old.request_id,old.state,old.reason,old.effects,old.state=='verified')
        try:
            adapter.capture(journal,request)
            if _stage_only:
                saved=journal.save(request,'staged','independently verified materialized Root checkpoint retained; no TERM/launch','prestop_metadata_observed')
                return v2.Result(saved.request_id,saved.state,saved.reason,saved.effects,False)
            adapter.arm()
            journal.save(request,'stop_intent','TERM effect unknown','stop_unknown')
            adapter.stop_once()
            while not adapter.closed():
                if time.monotonic()>=deadline:
                    raise TimeoutError('closure unproven; no replacement')
                time.sleep(.02)
            journal.save(request,'closed','exact old generation and listener closed','old_closed')
            if time.monotonic()>=deadline:
                raise TimeoutError('deadline before launch')
            journal.save(request,'start_intent','replacement effect unknown','start_unknown')
            adapter.start_once()
            journal.save(request,'replacement_started','owned replacement started','replacement_started')
            proof = adapter.verify(journal,request,deadline)
            with journal.db:
                journal.db.execute('INSERT INTO poststop_proofs VALUES('+','.join('?' for _ in range(25))+')',
                                   (request,*proof,repr(adapter.actor_proof)))
            saved = journal.save(request,'verified','derived actual actors; fresh pre-stop terminal Root and exact post-start continuity verified','restored_verified')
        except Exception as error:
            state,effects = journal.db.execute('SELECT state,effects FROM requests WHERE id=?',(request,)).fetchone()
            saved = journal.save(request,state,type(error).__name__+': '+str(error),effects)
        return v2.Result(saved.request_id,saved.state,saved.reason,saved.effects,saved.state=='verified')
    finally:
        journal.close()
        adapter.close()


def stage_checkpoint(request,source,selected,manifest,manifest_sha,*,timeout=120):
    """Persist a verified checkpoint while evidence is available; no process stop."""
    return recover(request,source,selected,manifest,manifest_sha,timeout=timeout,_stage_only=True)


class AdministrativeAuthority:
    """Explicit uncertain-prestate Root administration, not ordinary native admission.

    Preserve hard custody/epoch guards. Actual uncertain request records are
    inventoried and retained, rather than misreported as closed or empty.
    """
    def __init__(self,source,*,_configured_service_owner=False):
        self.config=v2.service._installed_server_config()
        config=self.config
        if os.getuid()!=config.uid or os.geteuid()!=config.uid:
            raise Blocked('configured real/effective UID differs')
        self.artifacts=tuple((str(p),v2.digest(v2.trusted_file(p,config.uid)))
                             for p in (config.plist,config.wrapper,config.executable))
        self.master=v2.service.configured_server_database(config.plist)
        if self.master!=config.master.resolve():raise Blocked('configured canonical store differs')
        v2.trusted_file(self.master,config.uid,contents=False)
        self.registry=Path(str(self.master)+'.projects.sqlite3')
        v2.trusted_file(self.registry,config.uid,contents=False)
        with closing(v2.readonly(self.registry)) as db:
            self.root=db.execute('SELECT home_project,home_session,host_id,thread_id FROM root_config WHERE singleton=1').fetchone()
            owner=db.execute('SELECT home_project,home_session FROM root_directory_owner WHERE singleton=1').fetchone()
        if not self.root or self.root[:2]!=owner or self.root[0]!='default':raise Blocked('canonical Root owner differs')
        self.source_id=source
        with closing(v2.readonly(self.master)) as db:
            v2.assert_original_owner_admission(db,self.root[1])
            v2.execution_epoch_native_check(db,thread=self.root[3],actor=self.root[1])
            binding=db.execute('SELECT b.thread_id,b.parent_session,h.host_id FROM bridge_bindings b '
                'JOIN remote_session_hosts h ON h.session_id=b.session_id WHERE b.session_id=?',(self.root[1],)).fetchone()
            if binding!=(self.root[3],None,self.root[2]):raise Blocked('canonical Root binding/host differs')
            message=db.execute('SELECT m.body,m.recipient_session,o.channel FROM messages m '
                'JOIN operator_message_origins o ON o.message_id=m.id AND o.sender_session=m.sender_session '
                'JOIN operator_identities i ON i.session_id=m.sender_session WHERE m.id=?',(source,)).fetchone()
            if _configured_service_owner:
                if source!='configured-local-service-owner':raise Blocked('invalid configured service principal')
                self.source_kind='configured_local_service_owner'
                self.source_body='existing local service restart authority\n'+str(config.uid)+'\n'+repr(self.root)
            else:
                if not message or message[1:]!=(self.root[1],'authenticated_operator_ui'):
                    raise Blocked('original authenticated Root operator source required')
                self.source_kind='authenticated_operator_ui';self.source_body=message[0]
            self.source_digest=v2.digest(self.source_body.encode())
            tables={row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            self.unknown=[]
            for row in db.execute("SELECT id,status FROM bridge_jobs WHERE thread_id=? AND status IN "
                                  "('adding','starting','uncertain','dispatching') ORDER BY id",(self.root[3],)):
                self.unknown.append(('bridge_jobs',*row))
            if 'root_native_handoffs' in tables:
                self.unknown.extend(('root_native_handoffs',*row) for row in db.execute(
                    'SELECT kind,job_id,phase FROM root_native_handoffs WHERE active=1 AND (session_id=? OR thread_id=?) '
                    'ORDER BY kind,job_id,phase',(self.root[1],self.root[3])))
            if 'execution_epoch_handoffs' in tables:
                self.unknown.extend(('execution_epoch_handoffs',*row) for row in db.execute(
                    'SELECT id,method FROM execution_epoch_handoffs WHERE settled_at IS NULL AND (actor=? OR thread=?) '
                    'ORDER BY id',(self.root[1],self.root[3])))
            for table in ('session_resets','agent_control_requests'):
                if table in tables:
                    self.unknown.extend((table,*row) for row in db.execute('SELECT id,status FROM '+table+
                        " WHERE target_session=? AND status NOT IN ('completed','failed','cancelled','resolved_unchanged') ORDER BY id",(self.root[1],)))
            with closing(v2.readonly(self.registry)) as registry:
                if registry.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_fresh_requests'").fetchone():
                    self.unknown.extend(('root_fresh_requests',*row) for row in registry.execute(
                        "SELECT id,status,native_action FROM root_fresh_requests WHERE home_session=? AND status NOT IN ('completed','cancelled','retired') ORDER BY id",(self.root[1],)))
            self.unknown=tuple(self.unknown)
            self.recorded='{}';self.recorded_checked_at=None
            if 'agent_control_observations' in tables:
                row=db.execute('SELECT model,reasoning_effort,cwd,permissions_json,checked_at FROM agent_control_observations '
                               'WHERE target_session=? AND thread_id=?',(self.root[1],self.root[3])).fetchone()
                if row:
                    self.recorded_checked_at=row[4]
                    self.recorded=jq(row[3] or '{}','with_entries(select(.value!=null))')
                    for key,value in zip(('model','reasoningEffort','cwd'),row[:3]):
                        if value is not None:self.recorded=jq(self.recorded,'.+{($key):$value}',key=key,value=value)

    def check(self):
        current=AdministrativeAuthority(self.source_id,_configured_service_owner=self.source_kind=='configured_local_service_owner')
        for name in ('root','source_kind','source_digest','artifacts','unknown','recorded','recorded_checked_at'):
            if getattr(current,name)!=getattr(self,name):raise Blocked('administrative authority/input inventory changed: '+name)


class AdministrativeRecovery(Recovery):
    def verify(self,journal,request,deadline):
        if field(self.baseline,'if .kind=="local-persistence" then "local" else "full" end')!='local':
            return super().verify(journal,request,deadline)
        from . import provider_recovery_local_persistence as local_persistence
        local_persistence.preserved_file(self.baseline)
        while True:
            if time.monotonic()>=deadline:raise TimeoutError('replacement listener unavailable')
            try:self.rpc=Wire(native.endpoint(self.selected.endpoint),deadline);break
            except ConnectionRefusedError:
                if self.child.poll() is not None:raise Blocked('replacement exited')
                time.sleep(.02)
        self.pin_peer();self.rpc.initialize();self.pin_peer()
        self.replacement_configuration_proof(journal,request)
        thread=self.authority.root[3]
        # PreserveExistingThread: exact existing UUID, no thread/start, fork,
        # model/permission override, input or queue mutation.
        journal.save(request,'resume_intent','PreserveExistingThread metadata attachment unknown','resume_unknown')
        observed=self.read('thread/resume',jq('{}','{threadId:$thread,excludeTurns:true}',thread=thread))
        if field(observed,'.thread.id')!=thread:raise Blocked('replacement loaded another Root')
        known=jq(self.baseline,'.known_settings')
        pair='{"actual":'+observed+',"known":'+known+'}'
        projection=jq(pair,'.known as $k | .actual | with_entries(select(.key as $n | $k|has($n)))')
        if projection!=known:raise Blocked('known local settings differ after no-override same-thread restore')
        profile=field(self.baseline,'if .known_profile_id==null then "<UNKNOWN>" else .known_profile_id end')
        if profile!='<UNKNOWN>' and field(observed,'.activePermissionProfile.id')!=profile:
            raise Blocked('known permission profile ID differs')
        sandbox=jq(self.baseline,'.known_sandbox')
        pair='{"actual":'+jq(observed,'.sandbox')+',"known":'+sandbox+'}'
        if jq(pair,'.known as $k | .actual | with_entries(select(.key as $n | $k|has($n)))')!=sandbox:
            raise Blocked('known serialized sandbox fields differ')
        from .provider_reattachment import record_resume
        record_resume(self,journal,request,observed)
        self._resume_receipt_recorded = True
        pending=queue(self,thread)  # Observe new queue; never equate it to UNKNOWN old queue.
        current=self.read('thread/read',jq('{}','{threadId:$thread,includeTurns:false}',thread=thread))
        jq(current,'if .thread.id!=$thread or (.thread.status.type!="idle" and .thread.status.type!="active") '
           'then error("same materialized Root not loaded") else . end',thread=thread)
        local_persistence.preserved_file(self.baseline)
        self.restored_connections();self.authority.check()
        self.replacement_configuration_proof(journal,request)
        if time.monotonic()>=deadline:raise TimeoutError('deadline before local continuity proof')
        process,code,local,peer=self.proof
        return (time.time(),self.authority.source_id,self.authority.source_digest,*self.authority.root,
            process.pid,process.birth_sec,process.birth_usec,self.replacement_image,self.replacement_sha,
            code,repr(local),repr(peer),None,v2.digest(jq(observed,'del(.thread)').encode()),v2.digest(pending.encode()),
            self.authority.recorded,jq(self.baseline,'.unknown_settings'),self.authority.recorded_checked_at,
            self.manifest_sha256,self.baseline_sha)

    def stop_once(self):
        if self.term_claimed or self.watch is None:raise Blocked('TERM already claimed or watcher absent')
        self.term_claimed=True
        self.selection()
        if time.monotonic()>=self.deadline:raise TimeoutError('deadline before administrative TERM')
        # Explicit administrative termination with UNKNOWN old native state.
        # Neither the name SIGTERM nor process exit implies native input closure.
        os.kill(self.selected.pid,signal.SIGTERM)



def stop_phase_deadline(operation_deadline,timeout):
    """Reserve half of the ORIGINAL operation budget for start/verification."""
    now=time.monotonic()
    deadline=min(operation_deadline-timeout/2,now+min(30,timeout/4))
    if deadline<=now:raise TimeoutError('insufficient original operation budget before TERM; no stop signal')
    return deadline


def recover_uncertain(request,source,selected,manifest,manifest_sha,*,timeout=120,_service_handoff=False):
    """Arm an existing materialized checkpoint for explicit uncertain-state reset.

    No old-provider RPC is made. This cannot manufacture a missing checkpoint.
    Full current Root restoration is checked independently on the replacement;
    prior uncertain effects remain recorded and are never reported closed.
    """
    if type(timeout) not in (int,float) or not 0<timeout<=120:raise Blocked('invalid bounded deadline')
    deadline=time.monotonic()+timeout
    if _service_handoff:
        from .provider_service import authority_for_handoff
        authority=authority_for_handoff(source)
    else:
        authority=AdministrativeAuthority(source)
    path=authority.master.parent/'provider-recovery-v3/requests.sqlite3'
    if not path.exists():
        raise Blocked('materialized Root checkpoint unavailable: exact retained history, complete effective settings, queue and authenticated peer provenance are absent; use an existing independently verified checkpoint, not new customer wording')
    journal=v2.v1.Journal(path)
    if _service_handoff:
        from .provider_service import HandoffRecovery
        adapter=HandoffRecovery(selected,manifest,manifest_sha,authority)
    else:
        adapter=AdministrativeRecovery(selected,manifest,manifest_sha,authority)
    adapter.deadline=deadline
    try:
        plan=adapter.plan_identity()+'\n'+repr(authority.root)+'\n'+source+'\n'+authority.source_digest
        old=journal.claim(request,selected,plan)
        if not old or old.state!='staged':
            if old:return v2.Result(old.request_id,old.state,old.reason,old.effects,False)
            raise Blocked('no independently verified staged checkpoint for this exact request')
        checkpoint=journal.db.execute('SELECT thread_id,baseline,baseline_sha,source_id,source_sha FROM prestop_proofs WHERE request_id=?',(request,)).fetchone()
        if (not checkpoint or checkpoint[0]!=authority.root[3] or checkpoint[3:]!=(source,authority.source_digest)
                or v2.digest(checkpoint[1].encode())!=checkpoint[2]):
            raise Blocked('staged materialized Root checkpoint binding changed')
        adapter.baseline=checkpoint[1];adapter.baseline_sha=checkpoint[2]
        local=field(adapter.baseline,'if .kind=="local-persistence" then "local" else "full" end')=='local'
        if local:
            from . import provider_recovery_local_persistence as local_persistence
            local_persistence.preserved_file(adapter.baseline)
        else:v2.settings(jq(adapter.baseline,'.settings'))
        adapter.selection()
        binding=journal.db.execute('SELECT home_project,root_session,host,thread_id,binding_generation FROM checkpoint_bindings WHERE request_id=?',(request,)).fetchone()
        if binding!=(*authority.root,adapter.actors.generation):
            raise Blocked('checkpoint canonical Root binding generation differs')
        with journal.db:
            journal.db.execute('CREATE TABLE IF NOT EXISTS administrative_arms(request_id TEXT PRIMARY KEY,armed_at REAL,'
                               'old_native_state TEXT,checkpoint_sha TEXT,uncertain_requests TEXT,actor_proof TEXT)')
            journal.db.execute('INSERT INTO administrative_arms VALUES(?,?,?,?,?,?)',
                (request,time.time(),'UNKNOWN',checkpoint[2],repr(authority.unknown),repr(adapter.actor_proof)))
        journal.save(request,'armed_unknown_prestate','explicit Root administration; old native turn/input state UNKNOWN','prior_inputs_unknown')
        try:
            adapter.arm()
            # Preserve time for replacement/verification inside the same original
            # absolute operation deadline. No automatic stronger external signal.
            stop_deadline=stop_phase_deadline(deadline,timeout) if _service_handoff else deadline
            with journal.db:
                journal.db.execute('CREATE TABLE IF NOT EXISTS stop_phase_budgets(request_id TEXT PRIMARY KEY,recorded_at REAL,operation_deadline REAL,stop_deadline REAL)')
                journal.db.execute('INSERT INTO stop_phase_budgets VALUES(?,?,?,?)',(request,time.time(),deadline,stop_deadline))
            journal.save(request,'stop_intent','administrative TERM outcome unknown; native inputs remain unknown','stop_unknown')
            adapter.stop_once()
            while not adapter.closed():
                if time.monotonic()>=stop_deadline:raise TimeoutError('graceful stop phase expired; process/listener closure unproven')
                time.sleep(.02)
            journal.save(request,'closed','process/listener closure only; native input closure not asserted','old_closed_inputs_unknown')
            if time.monotonic()>=deadline:raise TimeoutError('deadline before replacement')
            journal.save(request,'start_intent','replacement launch unknown','start_unknown')
            adapter.start_once()
            journal.save(request,'replacement_started','owned replacement observed; prior inputs unknown','replacement_started')
            proof=adapter.verify(journal,request,deadline)
            with journal.db:
                journal.db.execute('INSERT INTO poststop_proofs VALUES('+','.join('?' for _ in range(25))+')',
                                   (request,*proof,repr(adapter.actor_proof)))
            state='restored_known_local_state_prior_effects_unknown' if local else 'restored_checkpoint_old_state_unknown'
            meaning=('same existing Root loaded without overrides; known mapped settings and actual actors verified; '
                     'known history file identity/no-shrink observed; complete history equality and old queue UNKNOWN'
                     if local else 'same Root materialized history/settings/queue and actual actors verified; old prestate UNKNOWN')
            saved=journal.save(request,state,
                meaning+'; preserved uncertain IDs '+repr(authority.unknown)+'; no input replay/closure claim',
                'root_restored_prior_inputs_unknown')
        except Exception as error:
            state,effects=journal.db.execute('SELECT state,effects FROM requests WHERE id=?',(request,)).fetchone()
            saved=journal.save(request,state,type(error).__name__+': '+str(error),effects)
        return v2.Result(saved.request_id,saved.state,saved.reason,saved.effects,False)
    finally:
        adapter.close();journal.close()


def _resumable_local_stage(journal, request, old, service_handoff):
    if old.state != 'unresolved' or old.effects != 'none' or not service_handoff:
        return False
    for table in ('prestop_proofs','checkpoint_bindings','poststop_proofs'):
        exists=journal.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",(table,)).fetchone()
        if exists and journal.db.execute('SELECT 1 FROM '+table+' WHERE request_id=?',(request,)).fetchone():
            raise Blocked('partial checkpoint requires explicit reconciliation')
    return True


def stage_local_checkpoint(request,source,selected,manifest,manifest_sha,*,_service_handoff=False):
    """Stage known local persistence with UNKNOWN native state; zero old RPC.

    Missing runtime settings/old queue/terminal state are retained as unknown,
    not security denials. Actual owner/epoch guards and known facts still bind.
    """
    from . import provider_recovery_local_persistence as local_persistence
    if _service_handoff:
        from .provider_service import authority_for_handoff
        authority=authority_for_handoff(source)
    else:
        authority=AdministrativeAuthority(source)
    if _service_handoff:
        from .provider_service import HandoffRecovery
        adapter=HandoffRecovery(selected,manifest,manifest_sha,authority)
    else:
        adapter=AdministrativeRecovery(selected,manifest,manifest_sha,authority)
    adapter.deadline=time.monotonic()+120
    directory=authority.master.parent/'provider-recovery-v3'
    directory.mkdir(mode=0o700,exist_ok=True)
    journal=v2.v1.Journal(directory/'requests.sqlite3')
    try:
        plan=adapter.plan_identity()+'\n'+repr(authority.root)+'\n'+source+'\n'+authority.source_digest
        old=journal.claim(request,selected,plan)
        if old and not _resumable_local_stage(journal,request,old,_service_handoff):
            return v2.Result(old.request_id,old.state,old.reason,old.effects,False)
        adapter.selection()
        baseline=local_persistence.capture(selected,authority.root[3])
        baseline=jq(baseline,'.+{authority_source_kind:$kind}',kind=authority.source_kind)
        baseline=jq(baseline,'.+{replacement_configuration:('+adapter.replacement_configuration()+')}')
        journal.db.execute('CREATE TABLE IF NOT EXISTS prestop_proofs(request_id TEXT PRIMARY KEY,observed_at REAL,'
            'thread_id TEXT,baseline TEXT,baseline_sha TEXT,actor_proof TEXT,peer_proof TEXT,source_id TEXT,source_sha TEXT)')
        journal.db.execute('CREATE TABLE IF NOT EXISTS checkpoint_bindings(request_id TEXT PRIMARY KEY,home_project TEXT,'
            'root_session TEXT,host TEXT,thread_id TEXT,binding_generation REAL,app_pid INTEGER,app_birth_sec INTEGER,'
            'app_birth_usec INTEGER,cli_pid INTEGER,cli_birth_sec INTEGER,cli_birth_usec INTEGER,client_config_sha TEXT,'
            'cli_code_hash TEXT,cli_owner_identity TEXT)')
        journal.db.execute('CREATE TABLE IF NOT EXISTS poststop_proofs(request_id TEXT PRIMARY KEY,verified_at REAL,'
            'source_id TEXT,source_sha TEXT,home_project TEXT,root_session TEXT,host TEXT,thread_id TEXT,provider_pid INTEGER,'
            'birth_sec INTEGER,birth_usec INTEGER,image_path TEXT,image_sha TEXT,code_hash TEXT,connection_local TEXT,'
            'connection_peer TEXT,history_sha TEXT,settings_sha TEXT,queue_sha TEXT,recorded_present TEXT,recorded_missing TEXT,'
            'recorded_checked_at REAL,manifest_sha TEXT,baseline_sha TEXT,actor_proof TEXT)')
        with journal.db:
            journal.db.execute('INSERT INTO prestop_proofs VALUES(?,?,?,?,?,?,?,?,?)',
                (request,time.time(),authority.root[3],baseline,v2.digest(baseline.encode()),repr(adapter.actor_proof),
                 'UNKNOWN: local persistence only; no old-provider RPC',source,authority.source_digest))
            app,cli=adapter.actor_proof[0],adapter.actor_proof[4][0]
            journal.db.execute('INSERT INTO checkpoint_bindings VALUES('+','.join('?' for _ in range(15))+')',
                (request,*authority.root,adapter.actors.generation,app[0],app[3],app[4],cli[0],cli[3],cli[4],
                 adapter.actor_proof[1],adapter.actor_proof[4][2],adapter.actor_proof[4][4]))
        saved=journal.save(request,'staged','known local Root facts staged; current native turn, old queue and unmapped settings UNKNOWN','none')
        return v2.Result(saved.request_id,saved.state,saved.reason,saved.effects,False)
    finally:
        adapter.close();journal.close()
