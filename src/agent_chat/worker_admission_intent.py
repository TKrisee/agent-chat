"""Configured-Root worker intent. No reset, provisioning or native grant.

Only root_local dispatch calls this adapter. Every call revalidates authority and
observations, including retries; the journal is not a reusable capability.
"""
import base64
import fcntl
from dataclasses import astuple, replace
import hashlib
import os
from pathlib import Path
import re
import sqlite3
import stat
import subprocess
import time
import uuid

from .core import CoordError
from .native_admission_intent import (WorkerIdentity, Domain, PhysicalIdentity,
    RuntimeSettings, EvidencePin, IdentityEvidence, DomainEvidence,
    SettingsEvidence, AdmissionRequest, prepare_intent, IntentError)


def pin(reference, value):
    return EvidencePin(reference, hashlib.sha256(repr(value).encode()).hexdigest())


def physical(path, *, directory=False, private=False):
    path = Path(path)
    info = path.lstat()
    kind = stat.S_ISDIR if directory else stat.S_ISREG
    if (not path.is_absolute() or path.resolve() != path or not kind(info.st_mode)
            or info.st_uid != os.geteuid() or (not directory and info.st_nlink != 1)
            or (private and info.st_mode & 0o077)):
        raise CoordError('worker intent physical owner/path/mode differs')
    return PhysicalIdentity(str(path), info.st_dev, info.st_ino)


def read_small(path, limit=4096):
    physical(path)
    fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
    with os.fdopen(fd, 'rb') as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_nlink != 1:
            raise CoordError('worker intent proof descriptor differs')
        value = stream.read(limit + 1)
        after = physical(path)
        if (after.device, after.inode) != (info.st_dev, info.st_ino):
            raise CoordError('worker intent proof changed while read')
    if len(value) > limit:
        raise CoordError('worker intent proof exceeds bound')
    return value


def settings(row):
    # jq is the sole JSON decoder. The output consists of base64 scalar records,
    # not JSON; type/shape validation happens in jq before values are extracted.
    query = '''
      if (.modelProvider|type)!="string" or (.modelProvider|length)==0 or
         (.approvalPolicy|type)!="string" or (.approvalsReviewer|type)!="string" or
         ((.activePermissionProfile != null) and (.activePermissionProfile|type)!="string") or
         .sandbox.type!="workspaceWrite" or
         (.sandbox.networkAccess|type)!="boolean" or
         (.sandbox.excludeTmpdirEnvVar|type)!="boolean" or
         (.sandbox.excludeSlashTmp|type)!="boolean" or
         (.runtimeWorkspaceRoots|type)!="array" or
         (.sandbox.writableRoots|type)!="array" or
         any(.runtimeWorkspaceRoots[]; type!="string") or
         any(.sandbox.writableRoots[]; type!="string")
      then error("incomplete native settings observation") else
        ([.modelProvider,.approvalPolicy,.approvalsReviewer,
          (.activePermissionProfile // ""),.sandbox.type,
          (.sandbox.networkAccess|tostring),(.sandbox.excludeTmpdirEnvVar|tostring),
          (.sandbox.excludeSlashTmp|tostring),
          (.runtimeWorkspaceRoots|length|tostring)] + .runtimeWorkspaceRoots +
          [(.sandbox.writableRoots|length|tostring)] + .sandbox.writableRoots)[] | @base64
      end'''
    try:
        result = subprocess.run(['jq', '-er', query], input=row['permissions_json'],
            text=True, capture_output=True, timeout=3, check=True)
        values = [base64.b64decode(value, validate=True).decode() for value in result.stdout.splitlines()]
        provider, policy, reviewer, profile, sandbox, network, tmpdir, slash, count = values[:9]
        count = int(count)
        roots = tuple(values[9:9+count])
        writable_count = int(values[9+count])
        writes = tuple(values[10+count:])
        if len(writes) != writable_count:
            raise ValueError('invalid settings record count')
        return RuntimeSettings(row['model'], provider, row['reasoning_effort'], policy,
            reviewer, profile or None, sandbox, network == 'true', tmpdir == 'true',
            slash == 'true', row['cwd'], roots, writes)
    except (subprocess.SubprocessError, ValueError, TypeError, UnicodeError, IndexError) as error:
        raise CoordError('worker intent requires complete provider/settings readback') from error


def domain_evidence(db, params, identity):
    producer = db.execute('''SELECT p.*,s.workspace,s.leaf_prefix,s.gitdir,s.index_resource,s.scope_digest
       FROM lifecycle_producers p JOIN execution_producer_scopes s ON s.producer_id=p.id
       WHERE p.id=? AND p.owner_session=? AND p.host_id=?''',
       (params['producer_id'], identity.worker, identity.host)).fetchone()
    if producer is None:
        raise CoordError('worker intent requires authoritative same-worker producer scope')
    root = Path(producer['workspace'].removeprefix('workspace:'))
    if str(root) != params['expected_root'] or producer['workspace'] != 'workspace:' + str(root):
        raise CoordError('worker intent expected physical root differs')
    credential = Path(params['producer_credential'])
    if credential.is_relative_to(root) or credential.is_relative_to(Path(producer['gitdir'])):
        raise CoordError('worker intent producer credential must remain outside workspace/backing')
    physical(credential.parent, directory=True, private=True)
    physical(credential, private=True)
    secret = read_small(credential, 256).strip()
    if not secret or hashlib.sha256(secret).hexdigest() != producer['capability_sha256']:
        raise CoordError('worker intent private producer proof differs')
    gitfile = root / '.git'
    gitdir = Path(producer['gitdir'])
    if read_small(gitfile).decode().strip() != 'gitdir: ' + str(gitdir):
        raise CoordError('worker intent worktree pointer differs')
    common = (gitdir / read_small(gitdir / 'commondir').decode().strip()).resolve()
    if Path(read_small(gitdir / 'gitdir').decode().strip()) != gitfile:
        raise CoordError('worker intent backing reverse pointer differs')
    if not re.fullmatch(rb'[0-9a-f]{40,64}\n?', read_small(gitdir / 'HEAD')):
        raise CoordError('worker intent requires a detached worktree')
    domain = Domain(str(root), str(gitdir), str(common), str(gitdir / 'index'),
        producer['leaf_prefix'], producer['workspace'], params['canonical_resource'], producer['index_resource'])
    identities = (physical(root, directory=True), physical(gitfile),
        physical(gitdir, directory=True), physical(common, directory=True), physical(domain.index))
    material = (tuple(astuple(value) for value in identities),
        hashlib.sha256(read_small(domain.index, 64 * 1024 * 1024)).hexdigest(),
        read_small(gitdir / 'HEAD'), read_small(gitdir / 'commondir'), read_small(gitdir / 'gitdir'))
    # Hash actual checked metadata; caller supplies neither evidence SHA nor inode.
    return DomainEvidence(domain, *identities, pin('verified-physical-backing', material),
        producer['id'], pin('authoritative-producer-scope', tuple(producer)))


def _schema(db):
    db.execute('''CREATE TABLE IF NOT EXISTS registry.worker_admission_intents (
       root_session TEXT NOT NULL, request_id TEXT NOT NULL, project_id TEXT NOT NULL,
       worker_session TEXT NOT NULL, main_id TEXT NOT NULL, host_id TEXT NOT NULL,
       digest TEXT NOT NULL, material TEXT NOT NULL, created_at REAL NOT NULL,
       PRIMARY KEY(root_session,request_id))''')
    for operation in ('UPDATE', 'DELETE'):
        db.execute(f'''CREATE TRIGGER IF NOT EXISTS registry.worker_intent_no_{operation.lower()}
          BEFORE {operation} ON worker_admission_intents BEGIN
          SELECT RAISE(ABORT,'worker intent is immutable'); END''')


def _request_digest(params):
    return hashlib.sha256(repr(tuple(sorted(params.items()))).encode('utf-8')).hexdigest()


def _journal_begin(registry, session, host, params, *, enroll=False, enrollment_key=None):
    """Persist a prospective intent, never backfilling an existing legacy row."""
    db = sqlite3.connect(str(registry), timeout=10, isolation_level=None)
    try:
        db.execute('BEGIN IMMEDIATE')
        db.execute('''CREATE TABLE IF NOT EXISTS worker_admission_journal (
            root_session TEXT NOT NULL,request_id TEXT NOT NULL,payload_sha256 TEXT NOT NULL,
            project_id TEXT NOT NULL,host_id TEXT NOT NULL,created_at REAL NOT NULL,
            enrollment_key TEXT NOT NULL, PRIMARY KEY(root_session,request_id),
            UNIQUE(root_session,enrollment_key))''')
        db.execute('''CREATE TABLE IF NOT EXISTS worker_admission_journal_events (
            root_session TEXT NOT NULL,request_id TEXT NOT NULL,attempt_id TEXT NOT NULL,
            phase TEXT NOT NULL,observed_at REAL NOT NULL,
            PRIMARY KEY(root_session,request_id,attempt_id,phase))''')
        for table in ('worker_admission_journal','worker_admission_journal_events'):
            for action in ('UPDATE','DELETE'):
                db.execute(f'''CREATE TRIGGER IF NOT EXISTS {table}_no_{action.lower()} BEFORE {action} ON {table}
                    BEGIN SELECT RAISE(ABORT,'worker admission journal is immutable'); END''')
        if enroll:
            prior=db.execute('SELECT request_id,payload_sha256,project_id,host_id FROM worker_admission_journal WHERE root_session=? AND enrollment_key=?',(session,enrollment_key)).fetchone()
            if prior:
                original=dict(params,request_id=prior[0])
                if (prior[1],prior[2],prior[3]) != (_request_digest(original),params['project'],host):
                    raise CoordError('worker intent enrollment key conflicts with its original scope')
                db.rollback(); return prior[0], True
        key=(session,params['request_id']); digest=_request_digest(params)
        old=db.execute('SELECT payload_sha256,project_id,host_id FROM worker_admission_journal WHERE root_session=? AND request_id=?',key).fetchone()
        if old and old != (digest,params['project'],host):
            raise CoordError('worker intent request conflicts with its prospective journal')
        if enroll and old:
            raise CoordError('fresh worker intent identifier collision')
        if old:
            committed=db.execute("SELECT 1 FROM worker_admission_journal_events WHERE root_session=? AND request_id=? AND phase='committed'",key).fetchone()
            if committed:
                db.rollback(); return None
            last=db.execute("SELECT attempt_id FROM worker_admission_journal_events WHERE root_session=? AND request_id=? AND phase='commit_intent' ORDER BY rowid DESC LIMIT 1",key).fetchone()
            phases=([r[0] for r in db.execute('SELECT phase FROM worker_admission_journal_events WHERE root_session=? AND request_id=? AND attempt_id=?',(*key,last[0]))] if last else [])
            known_refusal=('transaction_rolled_back' in phases and any(phase.startswith('predicate_') and phase != 'predicate_other_or_unavailable' for phase in phases))
            if last and not known_refusal:
                raise CoordError('prior worker intent outcome is unknown; inspect exact request status and do not replay')
        if not old:
            if not enroll:
                db.rollback(); return None
            has_legacy=db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='worker_admission_intents'").fetchone()
            if has_legacy and db.execute('SELECT 1 FROM worker_admission_intents WHERE root_session=? AND request_id=?',key).fetchone():
                raise CoordError('fresh worker intent identifier collision')
            db.execute('INSERT INTO worker_admission_journal VALUES(?,?,?,?,?,?,?)',(*key,digest,params['project'],host,time.time(),enrollment_key))
            db.execute('INSERT INTO worker_admission_journal_events VALUES(?,?,?,?,?)',(*key,'enrollment','enrolled',time.time()))
            db.commit()
            return params['request_id'], False
        attempt=uuid.uuid4().hex
        db.execute('INSERT INTO worker_admission_journal_events VALUES(?,?,?,?,?)',(*key,attempt,'configured_root_verified',time.time()))
        db.execute('INSERT INTO worker_admission_journal_events VALUES(?,?,?,?,?)',(*key,attempt,'commit_intent',time.time()))
        db.commit()
        return attempt
    except BaseException:
        if db.in_transaction: db.rollback()
        raise
    finally: db.close()


def _journal_refused(registry, session, request_id, attempt, error):
    if attempt is None: return
    categories = {
        'worker intent exact worker/main/host/manager differs': 'worker_binding_mismatch',
        'worker intent requires fresh idle same-main settings readback': 'observation_unavailable_or_stale',
        'worker intent requires complete provider/settings readback': 'provider_settings_unavailable',
        'worker intent target is retired': 'target_retired',
        'worker intent waits for ordinary ownership/guard closure': 'ownership_or_guard_pending',
        'worker intent waits for remote guard closure': 'remote_guard_pending',
        'worker intent waits for explicit queue disposition': 'queue_pending',
        'worker intent Root binding changed': 'root_binding_changed',
        'worker intent requires running Root admission': 'root_not_running',
    }
    category=categories.get(str(error),'other_or_unavailable')
    with sqlite3.connect(str(registry),timeout=10) as db:
        db.execute('INSERT OR IGNORE INTO worker_admission_journal_events VALUES(?,?,?,?,?)',
                   (session,request_id,attempt,'transaction_rolled_back',time.time()))
        db.execute('INSERT OR IGNORE INTO worker_admission_journal_events VALUES(?,?,?,?,?)',
                   (session,request_id,attempt,'predicate_'+category,time.time()))


def enroll(master, home_project, session, host, params):
    """Mint a new journal-only request ID before any admission attempt."""
    from .root_local import _file, _preflight, _text
    from .service import _installed_server_config
    required={'project','target','expected_thread','expected_host','producer_id',
              'producer_credential','expected_root','canonical_resource','confirm','enrollment_key'}
    if not isinstance(params,dict) or set(params)!=required or params['confirm'] is not True:
        raise CoordError('invalid worker intent enrollment fields')
    for key in required-{'confirm'}: _text(params[key],key)
    try:
        if str(uuid.UUID(params['enrollment_key'])) != params['enrollment_key']: raise ValueError()
    except (ValueError,AttributeError):
        raise CoordError('enrollment key must be a canonical UUID') from None
    _preflight(master, _installed_server_config().plist, home_project, session, host,
               'worker-admission-intent', params)
    registry=_file(Path(str(master)+'.projects.sqlite3'),os.geteuid())
    key=params['enrollment_key']
    full={name:value for name,value in params.items() if name!='enrollment_key'}
    full['request_id']=str(uuid.uuid4())
    request_id, duplicate=_journal_begin(registry,session,host,full,enroll=True,enrollment_key=key)
    return {'request_id':request_id,'state':'enrolled','runtime_authorized':False,
            'native_execution':'unavailable_no_consumer','duplicate':duplicate}


def enrollment_status(master, home_project, session, host, params):
    """Reconcile an enrollment key without creating or admitting anything."""
    from .root_local import _file, _preflight
    from .service import _installed_server_config
    _preflight(master,_installed_server_config().plist,home_project,session,host,
               'worker-admission-enroll',params)
    try:
        if str(uuid.UUID(params['enrollment_key'])) != params['enrollment_key']: raise ValueError()
    except (ValueError,AttributeError,KeyError):
        raise CoordError('enrollment key must be a canonical UUID') from None
    registry=_file(Path(str(master)+'.projects.sqlite3'),os.geteuid())
    db=sqlite3.connect(registry.resolve().as_uri()+'?mode=ro',uri=True,timeout=10)
    try:
        db.execute('PRAGMA query_only=ON'); db.execute('BEGIN')
        if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='worker_admission_journal'").fetchone():
            return {'state':'not_recorded_at_read','runtime_authorized':False,'next_action':'reconcile_same_key_and_exact_scope'}
        row=db.execute('SELECT request_id,payload_sha256,project_id,host_id FROM worker_admission_journal WHERE root_session=? AND enrollment_key=?',(session,params['enrollment_key'])).fetchone()
        if not row:
            return {'state':'not_recorded_at_read','runtime_authorized':False,'next_action':'reconcile_same_key_and_exact_scope'}
        original={name:value for name,value in params.items() if name!='enrollment_key'}
        original['request_id']=row[0]
        if (row[1],row[2],row[3]) != (_request_digest(original),params['project'],host):
            raise CoordError('worker intent enrollment key conflicts with its original scope')
        return {'state':'enrolled','request_id':row[0],'runtime_authorized':False,
                'native_execution':'unavailable_no_consumer','next_action':'inspect_exact_request_before_admission'}
    finally: db.close()


def status(master, home_project, session, host, params):
    """Read one exact prospective intent; never resubmit it."""
    from .root_local import _file, _preflight
    from .service import _installed_server_config
    _preflight(master, _installed_server_config().plist, home_project, session, host,
               'worker-admission-intent', params)
    registry=_file(Path(str(master)+'.projects.sqlite3'),os.geteuid())
    db=sqlite3.connect(registry.resolve().as_uri()+'?mode=ro',uri=True,timeout=10)
    try:
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='worker_admission_journal'").fetchone():
            return {'state':'legacy_or_unknown','native_execution':'unavailable_no_consumer','next_action':'retain_unknown_no_replay'}
        row=db.execute('SELECT payload_sha256,project_id,host_id FROM worker_admission_journal WHERE root_session=? AND request_id=?',(session,params['request_id'])).fetchone()
        if row is None:
            return {'state':'legacy_or_unknown','native_execution':'unavailable_no_consumer','next_action':'retain_unknown_no_replay'}
        if row != (_request_digest(params),params['project'],host):
            raise CoordError('worker intent request conflicts with its prospective journal')
        events=[{'attempt_id':r[0],'phase':r[1],'observed_at':r[2]} for r in db.execute('SELECT attempt_id,phase,observed_at FROM worker_admission_journal_events WHERE root_session=? AND request_id=? ORDER BY rowid',(session,params['request_id']))]
        phases=[event['phase'] for event in events]
        intent_table=db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='worker_admission_intents'").fetchone()
        intent = (db.execute('SELECT project_id,worker_session,main_id,host_id FROM worker_admission_intents WHERE root_session=? AND request_id=?',
                             (session,params['request_id'])).fetchone() if intent_table else None)
        committed=('committed' in phases and intent == (params['project'],params['target'],params['expected_thread'],host))
        latest=next((event['attempt_id'] for event in reversed(events) if event['phase']=='commit_intent'),None)
        latest_phases=[event['phase'] for event in events if event['attempt_id']==latest]
        rollback_observed = 'committed' not in phases and intent is None and 'transaction_rolled_back' in latest_phases
        refused = rollback_observed and any(phase.startswith('predicate_') and phase != 'predicate_other_or_unavailable' for phase in latest_phases)
        enrolled_only = latest is None and 'enrolled' in phases
        return {'request_id':params['request_id'],'state':'committed' if committed else 'refused' if refused else 'enrolled' if enrolled_only else 'unknown',
                'phases':phases,'events':events,'rollback_observed':rollback_observed,'native_execution':'unavailable_no_consumer',
                'next_action':('readback_only_no_native_association' if committed else
                               'inspect_and_correct_refused_predicate' if refused else
                               'first_admission_requires_current_guards' if enrolled_only else 'retain_unknown_no_replay')}
    finally: db.close()


def admit(master, home_project, session, host, params):
    """Serialize the exact request so a pending journal cannot be replayed."""
    from .root_local import _file, _preflight
    from .service import _installed_server_config
    if not isinstance(params, dict) or not isinstance(params.get('request_id'), str):
        raise CoordError('invalid worker intent fields')
    _preflight(master, _installed_server_config().plist, home_project, session, host,
               'worker-admission-intent', params)
    registry=_file(Path(str(master)+'.projects.sqlite3'),os.geteuid())
    with sqlite3.connect(registry.resolve().as_uri()+'?mode=ro',uri=True,timeout=10) as probe:
        table=probe.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='worker_admission_journal'").fetchone()
        enrolled=(table and probe.execute('SELECT 1 FROM worker_admission_journal WHERE root_session=? AND request_id=?',(session,params['request_id'])).fetchone())
    if not enrolled:
        return _admit_locked(master,home_project,session,host,params,prospective=False)
    key=hashlib.sha256((session+'\0'+params['request_id']).encode('utf-8')).hexdigest()
    lock_path=registry.parent/(registry.name+'.worker-intent-'+key+'.lock')
    fd=os.open(lock_path,os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW,0o600)
    try:
        info=os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.geteuid() or info.st_nlink!=1 or info.st_mode & 0o077:
            raise CoordError('worker intent request lock is not private and owned')
        fcntl.flock(fd,fcntl.LOCK_EX)
        return _admit_locked(master,home_project,session,host,params,prospective=True)
    finally:
        os.close(fd)


def _admit_locked(master, home_project, session, host, params, *, prospective):
    from .root_local import _file, _target, _preflight
    required = {'project','target','expected_thread','expected_host','request_id','producer_id',
                'producer_credential','expected_root','canonical_resource','confirm'}
    if not isinstance(params, dict) or set(params) != required or params['confirm'] is not True:
        raise CoordError('invalid worker intent fields')
    from .root_local import _text
    for key in required - {'confirm'}:
        _text(params[key], key)
    from .service import _installed_server_config
    _preflight(master, _installed_server_config().plist, home_project, session, host,
               'worker-admission-intent', params)
    owner = os.geteuid()
    master = Path(master)
    registry = _file(Path(str(master) + '.projects.sqlite3'), owner)
    target = _target(master, registry, params['project'], owner)
    home = _target(master, registry, home_project, owner)
    attempt = _journal_begin(registry, session, host, params) if prospective else None
    db = sqlite3.connect(target.as_uri() + '?mode=rw', uri=True, timeout=10)
    db.row_factory = sqlite3.Row
    try:
        db.execute('ATTACH DATABASE ? AS registry', (str(registry),))
        # Attach home even when target==home; this path is rejected below before
        # BEGIN, so a worker cannot be admitted as Root in its home project.
        if target == home:
            raise CoordError('worker intent requires a distinct managed project')
        db.execute('ATTACH DATABASE ? AS home', (str(home),))
        db.execute('BEGIN IMMEDIATE')
        root = db.execute('SELECT * FROM registry.root_config WHERE singleton=1').fetchone()
        if root is None or (root['home_project'], root['home_session'], root['host_id']) != (home_project, session, host):
            raise CoordError('worker intent canonical Root identity changed')
        if root['mode'] != 'running':
            raise CoordError('worker intent requires running Root admission')
        root_binding = db.execute('''SELECT b.thread_id,b.parent_session,h.host_id
           FROM home.bridge_bindings b JOIN home.remote_session_hosts h ON h.session_id=b.session_id
           JOIN home.sessions s ON s.id=b.session_id WHERE b.session_id=?''', (session,)).fetchone()
        if root_binding is None or tuple(root_binding) != (root['thread_id'], None, host):
            raise CoordError('worker intent Root binding changed')
        project = db.execute('SELECT * FROM registry.root_projects WHERE project_id=?', (params['project'],)).fetchone()
        if project is None or not project['joined'] or not project['orchestrator_session']:
            raise CoordError('worker intent project lacks designated orchestrator')
        worker = params['target']
        binding = db.execute('''SELECT b.thread_id,b.parent_session,h.host_id
           FROM bridge_bindings b JOIN sessions s ON s.id=b.session_id
           JOIN remote_session_hosts h ON h.session_id=b.session_id WHERE b.session_id=?''', (worker,)).fetchone()
        manager = db.execute('SELECT manager_session FROM agent_management WHERE target_session=?', (worker,)).fetchone()
        po = db.execute('SELECT root_session FROM project_orchestrators WHERE session_id=?', (project['orchestrator_session'],)).fetchone()
        if (binding is None or tuple(binding) != (params['expected_thread'], None, params['expected_host'])
                or params['expected_host'] != host or manager is None
                or manager[0] != project['orchestrator_session'] or po is None or po[0] != project['session_id']):
            raise CoordError('worker intent exact worker/main/host/manager differs')
        if db.execute('SELECT 1 FROM retired_sessions WHERE id=?', (worker,)).fetchone():
            raise CoordError('worker intent target is retired')
        if db.execute('SELECT 1 FROM resources WHERE owner_session=?', (worker,)).fetchone() or db.execute('SELECT 1 FROM guarded_runs WHERE session=? AND closed_at IS NULL', (worker,)).fetchone():
            raise CoordError('worker intent waits for ordinary ownership/guard closure')
        if db.execute('SELECT 1 FROM remote_guarded_runs WHERE session_id=? AND closed_at IS NULL', (worker,)).fetchone():
            raise CoordError('worker intent waits for remote guard closure')
        if db.execute('SELECT 1 FROM resource_queue WHERE session=?', (worker,)).fetchone():
            raise CoordError('worker intent waits for explicit queue disposition')
        observation = db.execute('SELECT * FROM agent_control_observations WHERE target_session=?', (worker,)).fetchone()
        now = time.time()
        if (observation is None or observation['thread_id'] != binding['thread_id']
                or not 0 <= now - observation['checked_at'] <= 120
                or observation['state'] != 'idle' or observation['queue_count'] != 0):
            raise CoordError('worker intent requires fresh idle same-main settings readback')
        current = settings(observation)
        identity = WorkerIdentity(params['project'], worker, binding['thread_id'], binding['host_id'])
        physical_proof = domain_evidence(db, params, identity)
        selected = replace(current, cwd=physical_proof.domain.root,
            workspace_roots=(physical_proof.domain.root,), writable_roots=(physical_proof.domain.root,))
        request = AdmissionRequest(params['request_id'], identity, physical_proof.domain, selected)
        intent = prepare_intent(request, identity=IdentityEvidence(identity,
            pin('authoritative-worker-binding', (tuple(root_binding), tuple(binding), tuple(manager), tuple(po)))),
            domain=physical_proof, settings=SettingsEvidence(current, selected,
                pin('authoritative-native-observation', tuple(observation))))
        # A second filesystem check detects backing replacement during collection.
        if domain_evidence(db, params, identity) != physical_proof:
            raise CoordError('worker intent physical proof changed before commit')
        _schema(db)
        old = db.execute('SELECT digest,material,created_at FROM registry.worker_admission_intents WHERE root_session=? AND request_id=?', (session, request.request_id)).fetchone()
        material = repr(astuple(intent))
        if old is not None and (old['digest'], old['material']) != (intent.digest, material):
            raise CoordError('worker intent request conflicts with current verified evidence')
        if old is None:
            db.execute('INSERT INTO registry.worker_admission_intents VALUES(?,?,?,?,?,?,?,?,?)',
                (session, request.request_id, identity.project, identity.worker, identity.main,
                 identity.host, intent.digest, material, now))
        if attempt is not None:
            db.execute('INSERT INTO registry.worker_admission_journal_events VALUES(?,?,?,?,?)',
                       (session,request.request_id,attempt,'transaction_validated',time.time()))
            db.execute('INSERT INTO registry.worker_admission_journal_events VALUES(?,?,?,?,?)',
                       (session,request.request_id,attempt,'committed',time.time()))
        db.commit()
        return {'request_id':request.request_id, 'project':identity.project, 'worker':identity.worker,
            'main':identity.main, 'host':identity.host, 'digest':intent.digest,
            'status':'intent_only', 'runtime_authorized':False,
            'duplicate':old is not None, 'created_at':old['created_at'] if old else now}
    except (OSError, sqlite3.Error, IntentError, UnicodeError) as error:
        active = db.in_transaction
        if active: db.rollback()
        if active: _journal_refused(registry,session,params['request_id'],attempt,error)
        raise CoordError('worker intent evidence unavailable or invalid: ' + str(error)) from error
    except BaseException as error:
        active = db.in_transaction
        if active: db.rollback()
        if active: _journal_refused(registry,session,params['request_id'],attempt,error)
        raise
    finally:
        db.close()
