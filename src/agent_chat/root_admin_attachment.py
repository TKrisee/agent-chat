"""Read-only native observation for explicit Root administrative attachment.

This proves only a bounded configured-peer/thread observation. It does not prove
caller ownership, a sole CLI, fresh boot, or execution of any queued input.
"""
import time

from .core import CoordError
from . import root_admin_attachment_members as members
from .native_owner_evidence import DarwinEvidence, EvidenceError, same_owner
from .rpc import RpcClient, RpcError, TransportError


def observe(registration, thread_id, turn_id, cwd, expected_pid, expected_birth, expected_image_sha, *, allow_terminal=False, _kernel=None, _rpc=None):
    if (not isinstance(registration, dict) or set(registration) != {'endpoint', 'heartbeat'}
            or not isinstance(registration['heartbeat'], (int, float))
            or not 0 <= time.time() - registration['heartbeat'] <= 15):
        raise CoordError('configured bridge endpoint observation is stale or unavailable')
    kernel = _kernel if _kernel is not None else DarwinEvidence()
    rpc = _rpc if _rpc is not None else RpcClient(registration['endpoint'], timeout=3)
    try:
        rpc.connect()
        before = kernel.sample_independent_listener(registration['endpoint'], expected_pid, expected_birth, rpc, expected_image_sha)
        connection = rpc.connection_identity()
        response = rpc.request('thread/read', {'threadId':thread_id, 'includeTurns':True})
        thread = response.get('thread') if isinstance(response, dict) else None
        turns = thread.get('turns') if isinstance(thread, dict) else None
        status = thread.get('status') if isinstance(thread, dict) else None
        active = []
        exact_turns = []
        if isinstance(turns, list):
            active = [turn for turn in turns if isinstance(turn, dict) and turn.get('status') == 'inProgress']
            exact_turns = [turn for turn in turns if isinstance(turn, dict) and turn.get('id') == turn_id]
        if (not isinstance(thread, dict) or thread.get('id') != thread_id or thread.get('cwd') != cwd
                or not isinstance(status, dict) or len(exact_turns) != 1):
            raise CoordError('configured peer did not confirm the exact active thread and turn')
        turn = exact_turns[0]
        active_turn = (status.get('type') == 'active' and len(active) == 1 and active[0] is turn
                       and turn.get('error') is None)
        terminal_turn = (allow_terminal and status.get('type') in ('active', 'idle')
                         and turn.get('status') in ('completed', 'interrupted', 'failed'))
        if not (active_turn or terminal_turn):
            raise CoordError('configured peer did not confirm the exact active thread and turn')
        after = kernel.sample_independent_listener(registration['endpoint'], expected_pid, expected_birth, rpc, expected_image_sha)
        same_owner(before, after, require_peer=True)
        if rpc.connection_identity() != connection:
            raise CoordError('native observation connection changed')
        return {'mode':'administrative_app_server_attachment', 'thread_id':thread_id, 'turn_id':turn_id,
                'endpoint':registration['endpoint'], 'listener_pid':after.listener_pid,
                'listener_birth':list(expected_birth), 'image_sha':after.image_sha,
                'code_hash':after.code_hash, 'connection_epoch':connection['connection_epoch'],
                'owner_state':'unknown', 'fresh_boot_proven':False}
    except (EvidenceError, RpcError, TransportError, OSError, ValueError, KeyError, TypeError) as error:
        raise CoordError('administrative native peer observation is unverified: ' + str(error)[:512]) from error
    finally:
        rpc.close()


def status(master, registry, home_project, session, host, request_id, *, resolve_project=None):
    """Historical attachment state only; this is not a current native observation."""
    import contextlib
    import sqlite3
    import uuid
    from pathlib import Path
    try: request_id = str(uuid.UUID(request_id))
    except (ValueError, TypeError, AttributeError) as error: raise CoordError('invalid administrative attachment request') from error
    with contextlib.closing(sqlite3.connect(Path(registry).resolve().as_uri()+'?mode=ro', uri=True)) as db:
        db.row_factory = sqlite3.Row
        if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_admin_attachment_requests'").fetchone():
            raise CoordError('administrative attachment request is unavailable')
        root = db.execute('SELECT home_project,home_session,host_id FROM root_config WHERE singleton=1').fetchone()
        row = db.execute('SELECT * FROM root_admin_attachment_requests WHERE request_id=?',(request_id,)).fetchone()
        pins = members.retained_members(db, request_id)
        released = members.release_completed(db, request_id)
        if not root or tuple(root) != (home_project,session,host) or not row or (row['home_project'],row['home_session'],row['host_id']) != (home_project,session,host):
            raise CoordError('administrative attachment request is unavailable')
    if pins:
        projections = []
        for pin in pins:
            project, projection_session = pin[:2]
            try:
                if members.pin_members((pin[:2],), resolve_project) != (pin,):
                    raise CoordError('administrative attachment retained project scope differs')
                with members.open_member(pin, master, write=False) as project_db:
                    allowed = None if released else (row['old_thread'], row['new_thread'])
                    thread = members.exact_binding(project_db, projection_session, home_project, session, host, allowed)
                    fence = members.fence_state(project_db, request_id, projection_session, row['old_thread'], row['new_thread'])
            except (CoordError, OSError, sqlite3.Error):
                if not released:
                    raise
                thread, fence = None, None
            projections.append({'project_id': project, 'session_id': projection_session, 'thread_id': thread, 'fence': fence})
        return {'request_id': request_id, 'state': row['state'], 'mode': 'administrative_app_server_attachment',
                'thread_id': row['new_thread'], 'owner_state': 'unknown', 'fresh_boot_proven': False,
                'old_work_fenced': any(item['fence'] for item in projections),
                'cutover_pending': row['state'] == 'prepared' or not released,
                'projections_verified': all(item['thread_id'] == row['new_thread'] and item['fence'] is not None
                                            for item in projections),
                'projections': projections, 'historical_readback': True}
    with contextlib.closing(sqlite3.connect(Path(master).resolve().as_uri()+'?mode=ro', uri=True)) as db:
        exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_admin_attachment_fences'").fetchone()
        fence = db.execute('SELECT active FROM root_admin_attachment_fences WHERE request_id=?',(request_id,)).fetchone() if exists else None
    return {'request_id':request_id, 'state':row['state'], 'mode':'administrative_app_server_attachment',
            'thread_id':row['new_thread'], 'owner_state':'unknown', 'fresh_boot_proven':False,
            'old_work_fenced':bool(fence and fence[0]), 'cutover_pending':bool(fence and fence[0] == 1), 'historical_readback':True}


def fresh_request_snapshot(row):
    import hashlib
    return hashlib.sha256(repr(tuple(row)).encode('utf-8')).hexdigest()


def fresh_requests_blocking(db, home_project, session, host, *, old_thread=None, admitting=False, attachment_id=None):
    """Recognize only an exact retained unknown creation, never infer its outcome."""
    rows = db.execute("SELECT * FROM root_fresh_requests WHERE status NOT IN ('completed','retired','cancelled')").fetchall()
    quarantine_exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_admin_fresh_quarantine'").fetchone()
    for row in rows:
        if admitting and (row['home_project'],row['home_session'],row['host_id'],row['old_thread_id'],row['status'],row['native_action'],row['new_thread_id'],row['queue_id']) == (home_project,session,host,old_thread,'uncertain','creating',None,None):
            continue
        saved = db.execute('''SELECT q.snapshot FROM root_admin_fresh_quarantine q
            JOIN root_admin_attachment_requests a ON a.request_id=q.attachment_id
            JOIN root_config c ON c.singleton=1
            WHERE q.fresh_id=? AND ((a.state='attached' AND c.thread_id=a.new_thread) OR a.request_id=?) AND a.home_project=? AND a.home_session=? AND a.host_id=?''',
            (row['id'],attachment_id,home_project,session,host)).fetchone() if quarantine_exists else None
        if not saved or saved['snapshot'] != fresh_request_snapshot(row):
            return True
    return False


def attach(master, registry, home_project, session, host, params, *, observer=observe, resolve_project=None):
    """Attach the existing Root registration across its joined project projections."""
    import contextlib
    import sqlite3
    import uuid
    from pathlib import Path

    fields = {'expected_thread','current_thread','current_turn','expected_endpoint',
              'expected_listener_pid','expected_birth_sec','expected_birth_usec','expected_image_sha','request_id','confirm'}
    if not isinstance(params, dict) or set(params) != fields or params['confirm'] is not True:
        raise CoordError('administrative attachment requires exact scope and confirmation')
    p = dict(params)
    import re
    if not isinstance(p['expected_image_sha'],str) or not re.fullmatch('[0-9a-f]{64}',p['expected_image_sha']):
        raise CoordError('administrative attachment requires an exact executable SHA256')
    try:
        for key in ('expected_thread','current_thread','current_turn','request_id'):
            p[key] = str(uuid.UUID(p[key]))
        for key in ('expected_listener_pid','expected_birth_sec','expected_birth_usec'):
            if isinstance(p[key], bool): raise ValueError(key)
            p[key] = int(p[key])
    except (TypeError, ValueError, AttributeError) as error:
        raise CoordError('invalid administrative attachment identity') from error
    if (p['current_thread'] == p['expected_thread'] or p['expected_listener_pid'] <= 1
            or p['expected_birth_sec'] <= 0 or not 0 <= p['expected_birth_usec'] < 1_000_000):
        raise CoordError('invalid administrative attachment target')
    # Before peer observation this is used only for read-only preflight. The
    # exact listener identity is verified before any attachment write.
    listener_started_at = p['expected_birth_sec'] + p['expected_birth_usec'] / 1_000_000
    values = (p['request_id'],home_project,session,host,p['expected_thread'],p['current_thread'],p['current_turn'],
              p['expected_endpoint'],p['expected_listener_pid'],p['expected_birth_sec'],p['expected_birth_usec'])

    @contextlib.contextmanager
    def db_at(path):
        db = sqlite3.connect(Path(path).resolve().as_uri()+'?mode=rw', uri=True, timeout=10, isolation_level=None)
        db.row_factory = sqlite3.Row
        try: yield db
        finally: db.close()

    def table(db, name):
        return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone())

    def validate_home(db, allow_new):
        runtime = db.execute('SELECT server,pid,heartbeat,error FROM bridge_client_runtime WHERE host_id=?', (host,)).fetchone()
        lease = db.execute('SELECT owner,acquired_at FROM bridge_dispatcher WHERE host_id=?', (host,)).fetchone() if table(db,'bridge_dispatcher') else None
        if (not runtime or not lease or runtime['pid'] <= 1 or runtime['error'] is not None
                or runtime['server'] != p['expected_endpoint'] or not 0 <= time.time()-runtime['heartbeat'] <= 15):
            raise CoordError('configured native bridge endpoint or dispatcher is unavailable')
        binding = db.execute('SELECT thread_id,parent_session,agent_path FROM bridge_bindings WHERE session_id=?', (session,)).fetchone()
        projection = db.execute('SELECT home_project,home_session,thread_id FROM root_projections WHERE session_id=?', (session,)).fetchone()
        owner = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (session,)).fetchone()
        permitted = (p['expected_thread'],p['current_thread']) if allow_new else (p['expected_thread'],)
        if (not binding or binding['thread_id'] not in permitted or binding['parent_session'] is not None
                or binding['agent_path'] is not None or not projection
                or (projection['home_project'],projection['home_session']) != (home_project,session)
                or projection['thread_id'] != binding['thread_id'] or not owner or owner['host_id'] != host):
            raise CoordError('administrative attachment HOME binding or projection differs')
        if db.execute('SELECT 1 FROM bridge_bindings WHERE thread_id=? AND session_id<>?', (p['current_thread'],session)).fetchone():
            raise CoordError('administrative attachment target thread belongs to another session')
        gates = db.execute('SELECT request_id,root_session,old_thread_id,new_thread_id,active FROM root_fresh_gates WHERE root_session=? AND active=1',(session,)).fetchall() if table(db,'root_fresh_gates') else []
        if len(gates)>1 or (gates and (gates[0]['request_id'] not in eligible_gates or tuple(gates[0])[1:] != (session,p['expected_thread'],None,1))):
            raise CoordError('administrative attachment conflicts with an active Root turnover gate')
        if table(db,'root_native_handoffs') and db.execute('SELECT 1 FROM root_native_handoffs WHERE active=1 AND (session_id=? OR thread_id=?)',(session,p['expected_thread'])).fetchone():
            raise CoordError('administrative attachment requires native handoff reconciliation')
        if members.has_execution_handoff_conflict(db, session, p['expected_thread'], host, listener_started_at):
            raise CoordError('administrative attachment requires execution handoff reconciliation')
        if db.execute("SELECT 1 FROM bridge_jobs WHERE thread_id=? AND status IN ('adding','starting')", (p['expected_thread'],)).fetchone():
            raise CoordError('administrative attachment requires in-flight wake reconciliation')
        for name, column, predicate in (('session_resets','target_session',"status NOT IN ('completed','failed','cancelled')"),
                                        ('agent_control_requests','target_session',"status NOT IN ('completed','failed','cancelled','resolved_unchanged')")):
            if table(db,name) and db.execute(f'SELECT 1 FROM {name} WHERE {column}=? AND {predicate}', (session,)).fetchone():
                raise CoordError('administrative attachment requires pending Root control reconciliation')
        return (runtime['server'],runtime['pid'],lease['owner'],lease['acquired_at']), runtime['heartbeat'], (tuple(gates[0]) if gates else None)

    request_id, old_thread, new_thread = p['request_id'], p['expected_thread'], p['current_thread']

    def advance_projects(phase):
        if not pins:
            return
        with db_at(registry) as scope_db:
            scope_db.execute('BEGIN IMMEDIATE')
            require_scope(scope_db)
            canonical = scope_db.execute('SELECT home_project,home_session,host_id,thread_id,mode FROM root_config WHERE singleton=1').fetchone()
            expected_thread = new_thread if phase == 'release' else old_thread
            if not canonical or tuple(canonical) != (home_project,session,host,expected_thread,'running'):
                raise CoordError('administrative attachment canonical Root changed')
            if members.retained_members(scope_db, request_id) != pins:
                raise CoordError('administrative attachment retained project scope differs')
            request = scope_db.execute('SELECT state FROM root_admin_attachment_requests WHERE request_id=?',
                                       (request_id,)).fetchone()
            if not request or request[0] != ('attached' if phase == 'release' else 'prepared'):
                raise CoordError('administrative attachment phase changed')
            if phase == 'release':
                verify_projects(pins, new_thread)
            for pin in pins:
                project, projection_session = pin[:2]
                if phase != 'release' and project == home_project:
                    continue
                with members.open_member(pin, master, write=True) as project_db:
                    if phase == 'fence':
                        members.prepare_member(project_db, request_id, projection_session, home_project, session,
                                               host, old_thread, new_thread, listener_started_at=listener_started_at)
                    elif phase == 'switch':
                        members.switch_member(project_db, request_id, projection_session, home_project, session,
                                              host, old_thread, new_thread, listener_started_at=listener_started_at)
                    else:
                        members.exact_binding(project_db, projection_session, home_project, session, host, (new_thread,))
                        members.release_member(project_db, request_id, projection_session, old_thread, new_thread)
                    project_db.commit()
            if phase == 'release':
                members.record_release(scope_db, request_id)
                scope_db.commit()

    def verify_projects(project_pins, thread, *, locked=None):
        for pin in project_pins:
            _, projection_session = pin[:2]
            opener = members.open_member(pin, master, write=locked is not None)
            with (contextlib.nullcontext(locked.enter_context(opener)) if locked is not None else opener) as project_db:
                members.exact_binding(project_db, projection_session, home_project, session, host, (thread,))
                fence = members.fence_state(project_db, request_id, projection_session, old_thread, new_thread)
            if fence is None or (locked is not None and fence != 1):
                raise CoordError('administrative attachment member fence differs')

    def require_scope(db):
        if members.joined_members(db, home_project, session) != scope:
            raise CoordError('administrative attachment joined Root scope changed')

    with db_at(registry) as db:
        root = db.execute('SELECT * FROM root_config WHERE singleton=1').fetchone()
        if not root or (root['home_project'],root['home_session'],root['host_id'],root['mode']) != (home_project,session,host,'running'):
            raise CoordError('administrative attachment canonical Root identity differs')
        previous = db.execute('SELECT * FROM root_admin_attachment_requests WHERE request_id=?',(p['request_id'],)).fetchone() if table(db,'root_admin_attachment_requests') else None
        if previous:
            keys = ('request_id','home_project','home_session','host_id','old_thread','new_thread','turn_id','endpoint','listener_pid','birth_sec','birth_usec')
            if tuple(previous[key] for key in keys) != values or previous['image_sha'] != p['expected_image_sha']:
                raise CoordError('administrative attachment retry differs from retained request')
        scope = members.joined_members(db, home_project, session)
        retained = members.retained_members(db, request_id)
        if previous and previous['state'] == 'attached':
            saved_status = status(master, registry, home_project, session, host, request_id, resolve_project=resolve_project)
            if not saved_status['cutover_pending']:
                return saved_status
            if retained:
                pins = retained
                if tuple(pin[:2] for pin in pins) != scope or not saved_status['projections_verified']:
                    raise CoordError('administrative attachment retained project scope or member binding changed')
                advance_projects('release')
                return status(master, registry, home_project, session, host, request_id, resolve_project=resolve_project)
        pins = members.pin_members(scope, resolve_project) if len(scope) > 1 or retained else ()
        if previous and pins != retained:
            raise CoordError('administrative attachment retained project scope differs')
        for pin in pins:
            project, projection_session = pin[:2]
            if project == home_project:
                continue
            with members.open_member(pin, master, write=False) as project_db:
                fence = members.fence_state(project_db, request_id, projection_session, old_thread, new_thread)
                permitted = (old_thread, new_thread) if previous and fence == 1 else (old_thread,)
                members.exact_binding(project_db, projection_session, home_project, session, host, permitted)
                members.ensure_no_member_conflicts(project_db, projection_session, old_thread, new_thread,
                                                   host=host, listener_started_at=listener_started_at)
        if fresh_requests_blocking(db, home_project, session, host, old_thread=p['expected_thread'], admitting=True):
            raise CoordError('administrative attachment requires prior Root turnover reconciliation')
        eligible_gates = {row['id'] for row in db.execute("SELECT * FROM root_fresh_requests WHERE status='uncertain' AND native_action='creating' AND home_project=? AND home_session=? AND host_id=? AND old_thread_id=? AND new_thread_id IS NULL AND queue_id IS NULL",(home_project,session,host,p['expected_thread']))}
        prior_gate = db.execute('SELECT fresh_id FROM root_admin_attachment_gate_intents WHERE attachment_id=?',(p['request_id'],)).fetchone() if table(db,'root_admin_attachment_gate_intents') else None
        cwd = root['cwd']
        original_config_thread = root['thread_id']
    with db_at(master) as db:
        runtime_pin, heartbeat, initial_gate = validate_home(db, bool(previous))
        if previous and initial_gate and not prior_gate:
            raise CoordError('administrative attachment gate appeared after preparation')
        if prior_gate and (initial_gate is None or initial_gate[0] != prior_gate['fresh_id']):
            audit = db.execute('SELECT fresh_id,session_id,old_thread_id FROM root_admin_attachment_gate_transfers WHERE attachment_id=?',(p['request_id'],)).fetchone() if table(db,'root_admin_attachment_gate_transfers') else None
            retained = db.execute('SELECT request_id,root_session,old_thread_id,new_thread_id,active FROM root_fresh_gates WHERE request_id=?',(prior_gate['fresh_id'],)).fetchone() if table(db,'root_fresh_gates') else None
            if initial_gate or not audit or tuple(audit) != (prior_gate['fresh_id'],session,p['expected_thread']) or not retained or tuple(retained) != (prior_gate['fresh_id'],session,p['expected_thread'],None,0):
                raise CoordError('administrative attachment retained gate differs')
        registration = {'endpoint':runtime_pin[0],'heartbeat':heartbeat}
    observer_options = {'allow_terminal': True} if pins and previous else {}
    proof = observer(registration, p['current_thread'], p['current_turn'], cwd,
                     p['expected_listener_pid'], (p['expected_birth_sec'],p['expected_birth_usec']), p['expected_image_sha'], **observer_options)
    if (not isinstance(proof, dict) or proof.get('mode') != 'administrative_app_server_attachment' or proof.get('thread_id') != p['current_thread']
            or proof.get('turn_id') != p['current_turn'] or proof.get('endpoint') != p['expected_endpoint']
            or proof.get('listener_pid') != p['expected_listener_pid']
            or proof.get('listener_birth') != [p['expected_birth_sec'],p['expected_birth_usec']]
            or proof.get('image_sha') != p['expected_image_sha']
            or not isinstance(proof.get('code_hash'),str) or not proof['code_hash']
            or proof.get('owner_state') != 'unknown' or proof.get('fresh_boot_proven') is not False):
        raise CoordError('administrative native observation is unverified')
    from .core import Coordinator
    with contextlib.closing(Coordinator(master, session)) as coord:
        with coord.tx() as db:
            coord._fresh(db, session)
    with db_at(registry) as db, contextlib.ExitStack() as member_locks:
        db.execute('BEGIN IMMEDIATE')
        current = db.execute('SELECT home_project,home_session,host_id,thread_id,mode FROM root_config WHERE singleton=1').fetchone()
        permitted_threads = (p['expected_thread'],p['current_thread']) if previous else (p['expected_thread'],)
        if (not current or (current['home_project'],current['home_session'],current['host_id'],current['mode']) != (home_project,session,host,'running')
                or current['thread_id'] not in permitted_threads):
            raise CoordError('administrative attachment canonical Root changed')
        require_scope(db)
        if pins and table(db, 'root_fresh_replacements'):
            replacement = db.execute('''SELECT 1 FROM root_fresh_replacements
                WHERE home_project=? AND home_session=? AND host_id=? AND expected_thread=?
                AND state IN ('requested','preflight_verified','start_started','start_uncertain') LIMIT 1''',
                                     (home_project, session, host, old_thread)).fetchone()
            if replacement:
                raise CoordError('administrative attachment requires native replacement reconciliation')
        db.execute('''CREATE TABLE IF NOT EXISTS root_admin_attachment_requests (
            request_id TEXT PRIMARY KEY,home_project TEXT NOT NULL,home_session TEXT NOT NULL,host_id TEXT NOT NULL,
            old_thread TEXT NOT NULL,new_thread TEXT NOT NULL,turn_id TEXT NOT NULL,endpoint TEXT NOT NULL,
            listener_pid INTEGER NOT NULL,birth_sec INTEGER NOT NULL,birth_usec INTEGER NOT NULL,
            image_sha TEXT NOT NULL,code_hash TEXT NOT NULL,observed_at REAL NOT NULL,
            state TEXT NOT NULL CHECK(state IN ('prepared','attached')))''')
        db.execute('''CREATE TRIGGER IF NOT EXISTS root_admin_attachment_immutable
            BEFORE UPDATE OF request_id,home_project,home_session,host_id,old_thread,new_thread,turn_id,endpoint,listener_pid,birth_sec,birth_usec,image_sha,code_hash,observed_at ON root_admin_attachment_requests
            BEGIN SELECT RAISE(ABORT,'administrative attachment evidence is immutable'); END''')
        db.execute('''CREATE TRIGGER IF NOT EXISTS root_admin_attachment_no_delete
            BEFORE DELETE ON root_admin_attachment_requests BEGIN SELECT RAISE(ABORT,'administrative attachment evidence is immutable'); END''')
        db.execute('''CREATE TABLE IF NOT EXISTS root_admin_fresh_quarantine (
            fresh_id TEXT PRIMARY KEY, attachment_id TEXT NOT NULL, snapshot TEXT NOT NULL)''')
        db.execute('''CREATE TABLE IF NOT EXISTS root_admin_attachment_gate_intents (
            attachment_id TEXT PRIMARY KEY, fresh_id TEXT NOT NULL UNIQUE)''')
        db.execute('''CREATE TRIGGER IF NOT EXISTS root_admin_attachment_gate_intent_no_update BEFORE UPDATE ON root_admin_attachment_gate_intents BEGIN SELECT RAISE(ABORT,'gate intent is immutable'); END''')
        db.execute('''CREATE TRIGGER IF NOT EXISTS root_admin_attachment_gate_intent_no_delete BEFORE DELETE ON root_admin_attachment_gate_intents BEGIN SELECT RAISE(ABORT,'gate intent is immutable'); END''')
        db.execute('''CREATE TRIGGER IF NOT EXISTS root_admin_fresh_quarantine_no_update
            BEFORE UPDATE ON root_admin_fresh_quarantine BEGIN SELECT RAISE(ABORT,'quarantine evidence is immutable'); END''')
        db.execute('''CREATE TRIGGER IF NOT EXISTS root_admin_fresh_quarantine_no_delete
            BEFORE DELETE ON root_admin_fresh_quarantine BEGIN SELECT RAISE(ABORT,'quarantine evidence is immutable'); END''')
        if fresh_requests_blocking(db, home_project, session, host, old_thread=p['expected_thread'], admitting=True):
            raise CoordError('administrative attachment fresh request scope changed')
        old = db.execute('SELECT * FROM root_admin_attachment_requests WHERE request_id=?', (p['request_id'],)).fetchone()
        if old:
            keys = ('request_id','home_project','home_session','host_id','old_thread','new_thread','turn_id','endpoint','listener_pid','birth_sec','birth_usec')
            if tuple(old[key] for key in keys) != values:
                raise CoordError('administrative attachment retry differs from retained request')
            if (old['image_sha'],old['code_hash']) != (proof['image_sha'],proof['code_hash']):
                raise CoordError('administrative attachment peer image differs from retained evidence')
        else:
            if original_config_thread != p['expected_thread']:
                raise CoordError('administrative attachment expected old Root thread differs')
            if (members.cutover_pending(db)
                    or db.execute("SELECT 1 FROM root_admin_attachment_requests WHERE state='prepared'").fetchone()):
                raise CoordError('another administrative attachment needs reconciliation')
            for pin in pins:
                project_db = member_locks.enter_context(members.open_member(pin, master, write=True))
                if (table(project_db, 'root_admin_attachment_fences')
                        and project_db.execute('SELECT 1 FROM root_admin_attachment_fences WHERE active<>0').fetchone()):
                    raise CoordError('another administrative attachment fence is active')
            db.execute('INSERT INTO root_admin_attachment_requests VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                       (*values,proof['image_sha'],proof['code_hash'],time.time(),'prepared'))
        if pins:
            members.retain_members(db, request_id, pins, existing=bool(old))
        if initial_gate:
            db.execute('INSERT OR IGNORE INTO root_admin_attachment_gate_intents VALUES(?,?)',(p['request_id'],initial_gate[0]))
            intent = db.execute('SELECT fresh_id FROM root_admin_attachment_gate_intents WHERE attachment_id=?',(p['request_id'],)).fetchone()
            if not intent or intent['fresh_id'] != initial_gate[0]:
                raise CoordError('administrative attachment gate intent differs')
        for fresh in db.execute("SELECT * FROM root_fresh_requests WHERE status NOT IN ('completed','retired','cancelled')").fetchall():
            if (fresh['home_project'],fresh['home_session'],fresh['host_id'],fresh['old_thread_id'],fresh['status'],fresh['native_action'],fresh['new_thread_id'],fresh['queue_id']) == (home_project,session,host,p['expected_thread'],'uncertain','creating',None,None):
                db.execute('INSERT OR IGNORE INTO root_admin_fresh_quarantine VALUES(?,?,?)', (fresh['id'],p['request_id'],fresh_request_snapshot(fresh)))
                saved = db.execute('SELECT attachment_id,snapshot FROM root_admin_fresh_quarantine WHERE fresh_id=?',(fresh['id'],)).fetchone()
                if tuple(saved) != (p['request_id'],fresh_request_snapshot(fresh)):
                    raise CoordError('administrative attachment quarantine differs')
        db.commit()
    advance_projects('fence')
    with db_at(master) as db:
        db.execute('BEGIN IMMEDIATE')
        current_runtime, _, current_gate = validate_home(db, bool(previous))
        if current_runtime != runtime_pin:
            raise CoordError('configured native bridge identity changed during attachment')
        if current_gate != initial_gate:
            raise CoordError('administrative attachment turnover gate changed')
        db.execute('''CREATE TABLE IF NOT EXISTS root_admin_attachment_fences (
            request_id TEXT PRIMARY KEY,session_id TEXT NOT NULL,old_thread_id TEXT NOT NULL,new_thread_id TEXT NOT NULL,active INTEGER NOT NULL)''')
        existing = db.execute('SELECT * FROM root_admin_attachment_fences WHERE active<>0').fetchall()
        if existing and (len(existing) != 1 or tuple(existing[0]) != (p['request_id'],session,p['expected_thread'],p['current_thread'],1)):
            raise CoordError('another administrative attachment fence is active')
        db.execute('INSERT OR IGNORE INTO root_admin_attachment_fences VALUES(?,?,?,?,1)', (p['request_id'],session,p['expected_thread'],p['current_thread']))
        if current_gate:
            db.execute('''CREATE TABLE IF NOT EXISTS root_admin_attachment_gate_transfers (
                attachment_id TEXT PRIMARY KEY,fresh_id TEXT NOT NULL UNIQUE,session_id TEXT NOT NULL,old_thread_id TEXT NOT NULL)''')
            db.execute('''CREATE TRIGGER IF NOT EXISTS root_admin_gate_transfer_no_update BEFORE UPDATE ON root_admin_attachment_gate_transfers BEGIN SELECT RAISE(ABORT,'gate transfer is immutable'); END''')
            db.execute('''CREATE TRIGGER IF NOT EXISTS root_admin_gate_transfer_no_delete BEFORE DELETE ON root_admin_attachment_gate_transfers BEGIN SELECT RAISE(ABORT,'gate transfer is immutable'); END''')
            db.execute('INSERT INTO root_admin_attachment_gate_transfers VALUES(?,?,?,?)',(p['request_id'],current_gate[0],session,p['expected_thread']))
            db.execute('UPDATE root_fresh_gates SET active=0 WHERE request_id=? AND root_session=? AND old_thread_id=? AND new_thread_id IS NULL AND active=1',(current_gate[0],session,p['expected_thread']))
            if db.execute('SELECT changes()').fetchone()[0] != 1:
                raise CoordError('administrative attachment turnover gate changed')
        db.execute('UPDATE bridge_bindings SET thread_id=?,updated_at=? WHERE session_id=?', (p['current_thread'],time.time(),session))
        db.execute('UPDATE root_projections SET thread_id=? WHERE session_id=?', (p['current_thread'],session))
        db.commit()
    advance_projects('switch')
    # Re-observe before publishing the registry transition. A failed read leaves
    # the exact durable request and HOME fence for supervised reconciliation.
    proof2 = observer(registration, p['current_thread'], p['current_turn'], cwd,
                      p['expected_listener_pid'], (p['expected_birth_sec'],p['expected_birth_usec']), p['expected_image_sha'], **observer_options)
    if not isinstance(proof2, dict) or any(proof2.get(key) != proof.get(key) for key in ('mode','thread_id','turn_id','endpoint','listener_pid','listener_birth','image_sha','code_hash','owner_state','fresh_boot_proven')):
        raise CoordError('administrative native peer observation changed during attachment')
    with db_at(master) as db:
        current_runtime, _, remaining_gate = validate_home(db, True)
        fence = db.execute('SELECT session_id,old_thread_id,new_thread_id,active FROM root_admin_attachment_fences WHERE request_id=?', (p['request_id'],)).fetchone()
        if remaining_gate or current_runtime != runtime_pin or not fence or tuple(fence) != (session,p['expected_thread'],p['current_thread'],1):
            raise CoordError('administrative attachment HOME fence or bridge changed')
    with db_at(registry) as db:
        db.execute('BEGIN IMMEDIATE')
        current = db.execute('SELECT home_project,home_session,host_id,thread_id,mode FROM root_config WHERE singleton=1').fetchone()
        if not current or tuple(current) not in ((home_project,session,host,p['expected_thread'],'running'),(home_project,session,host,p['current_thread'],'running')):
            raise CoordError('administrative attachment canonical Root changed')
        require_scope(db)
        if fresh_requests_blocking(db,home_project,session,host,attachment_id=p['request_id']):
            raise CoordError('administrative attachment fresh request scope changed')
        with contextlib.ExitStack() as locked:
            if pins:
                verify_projects(pins, new_thread, locked=locked)
            db.execute('UPDATE root_config SET thread_id=? WHERE singleton=1',(p['current_thread'],))
            db.execute("UPDATE root_admin_attachment_requests SET state='attached' WHERE request_id=?",(p['request_id'],))
            db.commit()
    if pins:
        advance_projects('release')
        result = status(master, registry, home_project, session, host, request_id, resolve_project=resolve_project)
        result.pop('historical_readback')
        return result
    with db_at(master) as db:
        db.execute('BEGIN IMMEDIATE')
        outstanding = db.execute("SELECT 1 FROM bridge_jobs WHERE thread_id=? AND status NOT IN ('dispatched','cancelled') LIMIT 1", (p['expected_thread'],)).fetchone()
        db.execute('UPDATE root_admin_attachment_fences SET active=? WHERE request_id=?', (2 if outstanding else 0,p['request_id']))
        db.commit()
    return {'request_id':p['request_id'],'mode':'administrative_app_server_attachment',
            'thread_id':p['current_thread'],'owner_state':'unknown','fresh_boot_proven':False,
            'old_work_fenced':bool(outstanding)}
