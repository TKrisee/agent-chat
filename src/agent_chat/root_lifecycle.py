"""Root team stop barriers: preserve policy, identities, inputs and ownership."""
import time
import uuid
import sqlite3
import json
import hashlib
import math
from .core import CoordError


def schema(db):
    db.executescript('''
      CREATE TABLE IF NOT EXISTS root_team_stops (
        id TEXT PRIMARY KEY, root_session TEXT NOT NULL, active INTEGER NOT NULL,
        created_at REAL NOT NULL, resumed_at REAL);
      CREATE TABLE IF NOT EXISTS root_team_stop_targets (
        id TEXT PRIMARY KEY, stop_id TEXT NOT NULL, requester_session TEXT NOT NULL,
        target_session TEXT NOT NULL, host_id TEXT, thread_id TEXT,
        status TEXT NOT NULL, turn_id TEXT, error TEXT, created_at REAL NOT NULL,
        updated_at REAL NOT NULL, UNIQUE(stop_id,target_session));
      CREATE TABLE IF NOT EXISTS root_team_stop_po_admissions (
        stop_id TEXT PRIMARY KEY, root_session TEXT NOT NULL,
        target_session TEXT NOT NULL, thread_id TEXT NOT NULL,
        host_id TEXT NOT NULL, created_at REAL NOT NULL);
      CREATE TRIGGER IF NOT EXISTS root_team_stop_po_admissions_no_update
        BEFORE UPDATE ON root_team_stop_po_admissions
        BEGIN SELECT RAISE(ABORT,'PO admission audit is immutable'); END;
      CREATE TRIGGER IF NOT EXISTS root_team_stop_po_admissions_no_delete
        BEFORE DELETE ON root_team_stop_po_admissions
        BEGIN SELECT RAISE(ABORT,'PO admission audit is immutable'); END;
      CREATE TABLE IF NOT EXISTS root_team_stop_interrupts (
        request_id TEXT NOT NULL, turn_id TEXT NOT NULL, recorded_at REAL NOT NULL,
        PRIMARY KEY(request_id,turn_id));
      CREATE TABLE IF NOT EXISTS root_team_stop_task_admissions (
        request_id TEXT PRIMARY KEY, stop_id TEXT NOT NULL, root_session TEXT NOT NULL,
        po_session TEXT NOT NULL, target_session TEXT NOT NULL, thread_id TEXT NOT NULL,
        host_id TEXT NOT NULL, message_id TEXT NOT NULL UNIQUE,
        inspection_request_id TEXT NOT NULL, purpose TEXT NOT NULL,
        expires_at REAL NOT NULL, created_at REAL NOT NULL);
      CREATE TRIGGER IF NOT EXISTS root_team_stop_task_admissions_no_update
        BEFORE UPDATE ON root_team_stop_task_admissions
        BEGIN SELECT RAISE(ABORT,'investigation admission audit is immutable'); END;
      CREATE TRIGGER IF NOT EXISTS root_team_stop_task_admissions_no_delete
        BEFORE DELETE ON root_team_stop_task_admissions
        BEGIN SELECT RAISE(ABORT,'investigation admission audit is immutable'); END;
      CREATE TABLE IF NOT EXISTS root_team_stop_task_baselines (
        baseline_id TEXT PRIMARY KEY, admission_request_id TEXT NOT NULL UNIQUE,
        stop_id TEXT NOT NULL, root_session TEXT NOT NULL, po_session TEXT NOT NULL,
        target_session TEXT NOT NULL, thread_id TEXT NOT NULL, host_id TEXT NOT NULL,
        message_id TEXT NOT NULL UNIQUE, inspection_request_id TEXT NOT NULL,
        expected_json TEXT NOT NULL, expected_sha256 TEXT NOT NULL,
        source_message_id TEXT NOT NULL, source_artifact_name TEXT NOT NULL,
        source_artifact_sha256 TEXT NOT NULL,
        source_kind TEXT NOT NULL, source_limit TEXT NOT NULL, created_at REAL NOT NULL);
      CREATE TRIGGER IF NOT EXISTS root_team_stop_task_baselines_no_update
        BEFORE UPDATE ON root_team_stop_task_baselines
        BEGIN SELECT RAISE(ABORT,'investigation settings baseline is immutable'); END;
      CREATE TRIGGER IF NOT EXISTS root_team_stop_task_baselines_no_delete
        BEFORE DELETE ON root_team_stop_task_baselines
        BEGIN SELECT RAISE(ABORT,'investigation settings baseline is immutable'); END;
      CREATE TABLE IF NOT EXISTS root_team_stop_task_continuations (
        continuation_id TEXT PRIMARY KEY, admission_request_id TEXT NOT NULL UNIQUE,
        baseline_id TEXT NOT NULL, stop_id TEXT NOT NULL, root_session TEXT NOT NULL,
        po_session TEXT NOT NULL, target_session TEXT NOT NULL, thread_id TEXT NOT NULL,
        host_id TEXT NOT NULL, message_id TEXT NOT NULL UNIQUE,
        inspection_request_id TEXT NOT NULL, expires_at REAL NOT NULL, created_at REAL NOT NULL);
      CREATE TRIGGER IF NOT EXISTS root_team_stop_task_continuations_no_update
        BEFORE UPDATE ON root_team_stop_task_continuations
        BEGIN SELECT RAISE(ABORT,'investigation continuation grant is immutable'); END;
      CREATE TRIGGER IF NOT EXISTS root_team_stop_task_continuations_no_delete
        BEFORE DELETE ON root_team_stop_task_continuations
        BEGIN SELECT RAISE(ABORT,'investigation continuation grant is immutable'); END;
      CREATE TABLE IF NOT EXISTS root_team_stop_task_preload_requests (
        control_request_id TEXT PRIMARY KEY, continuation_id TEXT NOT NULL UNIQUE,
        admission_request_id TEXT NOT NULL, requester_session TEXT NOT NULL,
        created_at REAL NOT NULL);
      CREATE TRIGGER IF NOT EXISTS root_team_stop_task_preload_requests_no_update
        BEFORE UPDATE ON root_team_stop_task_preload_requests
        BEGIN SELECT RAISE(ABORT,'settings preload request association is immutable'); END;
      CREATE TRIGGER IF NOT EXISTS root_team_stop_task_preload_requests_no_delete
        BEFORE DELETE ON root_team_stop_task_preload_requests
        BEGIN SELECT RAISE(ABORT,'settings preload request association is immutable'); END;
      CREATE TABLE IF NOT EXISTS root_team_stop_task_preload_events (
        attempt_id TEXT NOT NULL, sequence INTEGER NOT NULL, continuation_id TEXT NOT NULL,
        admission_request_id TEXT NOT NULL, state TEXT NOT NULL, observed_json TEXT,
        observed_sha256 TEXT, reason TEXT, created_at REAL NOT NULL,
        PRIMARY KEY(attempt_id,sequence));
      CREATE TRIGGER IF NOT EXISTS root_team_stop_task_preload_events_no_update
        BEFORE UPDATE ON root_team_stop_task_preload_events
        BEGIN SELECT RAISE(ABORT,'settings preload evidence is append-only'); END;
      CREATE TRIGGER IF NOT EXISTS root_team_stop_task_preload_events_no_delete
        BEFORE DELETE ON root_team_stop_task_preload_events
        BEGIN SELECT RAISE(ABORT,'settings preload evidence is append-only'); END;
    ''')


def po_admitted(db, stop_id, target):
    if not all(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone()
               for name in ('root_team_stop_po_admissions', 'project_orchestrators')):
        return False
    return bool(db.execute('''SELECT 1 FROM root_team_stop_po_admissions a
        JOIN root_team_stops s ON s.id=a.stop_id AND s.active=1 AND s.root_session=a.root_session
        JOIN project_orchestrators p ON p.session_id=a.target_session AND p.root_session=a.root_session
        JOIN bridge_bindings b ON b.session_id=a.target_session AND b.thread_id=a.thread_id AND b.parent_session IS NULL
        JOIN remote_session_hosts h ON h.session_id=a.target_session AND h.host_id=a.host_id
        WHERE a.stop_id=? AND a.target_session=?''', (stop_id,target)).fetchone())


def task_admission(db, message_id, *, now=None, include_expired=False, allowed_control_id=None):
    """Return a currently valid one-message investigation admission, fail closed."""
    now = time.time() if now is None else now
    if not all(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone()
               for name in ('root_team_stop_task_admissions', 'project_orchestrators')):
        return None
    row = db.execute('''SELECT a.* FROM root_team_stop_task_admissions a
        JOIN root_team_stops s ON s.id=a.stop_id AND s.root_session=a.root_session AND s.active=1
        JOIN root_team_stop_targets t ON t.stop_id=a.stop_id AND t.requester_session=a.root_session
          AND t.target_session=a.target_session AND t.thread_id=a.thread_id AND t.host_id=a.host_id
          AND t.status IN ('pending','waiting','completed')
        JOIN root_team_stop_po_admissions p ON p.stop_id=a.stop_id AND p.root_session=a.root_session
          AND p.target_session=a.po_session
        JOIN bridge_bindings pb ON pb.session_id=a.po_session AND pb.thread_id=p.thread_id AND pb.parent_session IS NULL
        JOIN remote_session_hosts ph ON ph.session_id=a.po_session AND ph.host_id=p.host_id
        JOIN project_orchestrators o ON o.session_id=a.po_session AND o.root_session=a.root_session
        JOIN bridge_bindings b ON b.session_id=a.target_session AND b.thread_id=a.thread_id AND b.parent_session IS NULL
        JOIN remote_session_hosts h ON h.session_id=a.target_session AND h.host_id=a.host_id
        JOIN messages m ON m.id=a.message_id AND m.sender_session=a.po_session
          AND m.recipient_session=a.target_session
        JOIN agent_control_requests c ON c.id=a.inspection_request_id AND c.action='inspect'
          AND c.target_session=a.target_session AND c.thread_id=a.thread_id AND c.host_id=a.host_id
          AND c.requester_session IN (a.root_session,a.po_session) AND c.interrupt=0
          AND c.status IN ('pending','waiting','completed')
        WHERE a.message_id=? AND a.purpose='investigation' AND (?=1 OR a.expires_at>?)
          AND NOT EXISTS (SELECT 1 FROM root_team_stops other WHERE other.active=1 AND other.id<>a.stop_id)
          AND NOT EXISTS (SELECT 1 FROM message_reads r WHERE r.message_id=a.message_id
            AND r.reader_session=a.target_session AND r.provenance IN ('recipient_full_message','recipient_context_full'))
          AND NOT EXISTS (SELECT 1 FROM message_batches mb WHERE mb.message_id=a.message_id)
          AND NOT EXISTS (SELECT 1 FROM agent_control_requests x WHERE x.target_session=a.target_session
            AND x.id<>a.inspection_request_id AND ( ? IS NULL OR x.id<>? )
            AND x.status NOT IN ('completed','failed','cancelled','resolved_unchanged'))
          AND NOT EXISTS (SELECT 1 FROM session_resets r WHERE r.target_session=a.target_session
            AND r.status NOT IN ('completed','cancelled','failed'))
          AND NOT EXISTS (SELECT 1 FROM agent_controls ac WHERE ac.target_session=a.target_session AND ac.mode<>'running')
        ''', (message_id, int(include_expired), now, allowed_control_id, allowed_control_id)).fetchone()
    if not row:
        return None
    active_stops = db.execute('SELECT count(*) FROM root_team_stops WHERE active=1').fetchone()[0]
    if active_stops != 1 or not po_admitted(db, row['stop_id'], row['po_session']):
        return None
    from .root_team_reset import quarantined
    if quarantined(db, session=row['target_session']):
        return None
    return dict(row)


_EXPECTED_SETTING_KEYS = {'model', 'reasoning_effort', 'cwd', 'workspaces', 'permissions'}
_EXPECTED_PERMISSION_KEYS = {'activePermissionProfile', 'approvalPolicy', 'approvalsReviewer',
                             'runtimeWorkspaceRoots', 'sandbox'}
_EXPECTED_PROFILE_KEYS = {'id', 'extends'}
_EXPECTED_SANDBOX_KEYS = {'type', 'writableRoots', 'networkAccess',
                          'excludeSlashTmp', 'excludeTmpdirEnvVar'}


def canonical_expected_settings(value):
    """Validate the exact PO-attested comparison contract and return stable JSON+digest."""
    if not isinstance(value, dict) or set(value) != _EXPECTED_SETTING_KEYS:
        raise CoordError('investigation baseline must contain the complete protected settings tuple')
    for key in ('model', 'reasoning_effort', 'cwd'):
        item = value.get(key)
        if not isinstance(item, str) or not item or item != item.strip() or len(item) > 1024:
            raise CoordError('investigation baseline contains an invalid ' + key)
    if (not isinstance(value['workspaces'], list) or not value['workspaces']
            or any(not isinstance(path, str) or not path.startswith('/') for path in value['workspaces'])
            or len(set(value['workspaces'])) != len(value['workspaces'])):
        raise CoordError('investigation baseline workspace roots are incomplete or invalid')
    permissions = value['permissions']
    if not isinstance(permissions, dict) or set(permissions) != _EXPECTED_PERMISSION_KEYS:
        raise CoordError('investigation baseline protected permission tuple is incomplete')
    profile = permissions['activePermissionProfile']
    if not isinstance(profile, dict) or set(profile) != _EXPECTED_PROFILE_KEYS:
        raise CoordError('investigation baseline permission profile is incomplete')
    if not isinstance(profile['id'], str) or not profile['id'] or profile['extends'] is not None:
        raise CoordError('investigation baseline permission profile is invalid')
    if any(not isinstance(permissions[key], str) or not permissions[key]
           for key in ('approvalPolicy', 'approvalsReviewer')):
        raise CoordError('investigation baseline approval settings are invalid')
    roots = permissions['runtimeWorkspaceRoots']
    if roots != value['workspaces']:
        raise CoordError('investigation baseline workspace roots disagree')
    sandbox = permissions['sandbox']
    if not isinstance(sandbox, dict) or set(sandbox) != _EXPECTED_SANDBOX_KEYS:
        raise CoordError('investigation baseline sandbox settings are incomplete')
    if (not isinstance(sandbox['type'], str) or not sandbox['type']
            or not isinstance(sandbox['writableRoots'], list)
            or any(not isinstance(path, str) or path not in roots for path in sandbox['writableRoots'])
            or len(set(sandbox['writableRoots'])) != len(sandbox['writableRoots'])
            or any(not isinstance(sandbox[key], bool)
                   for key in ('networkAccess', 'excludeSlashTmp', 'excludeTmpdirEnvVar'))):
        raise CoordError('investigation baseline sandbox settings are invalid')
    encoded = json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False)
    return encoded, hashlib.sha256(encoded.encode('utf-8')).hexdigest()


def create_task_settings_continuation(coord, *, actor_root, admission_request_id,
                                      baseline_id, continuation_id, expected_settings,
                                      source_message_id, source_artifact_name, source_artifact_sha256,
                                      source_kind, source_limit, expires_at, now=None):
    """Attach immutable expected settings and one finite continuation to one expired, untouched task."""
    now = time.time() if now is None else now
    db = coord.db
    try:
        baseline_id = str(uuid.UUID(baseline_id))
        continuation_id = str(uuid.UUID(continuation_id))
    except (ValueError, TypeError, AttributeError) as error:
        raise CoordError('baseline and continuation IDs must be UUIDs') from error
    if (not isinstance(expires_at, (int, float)) or isinstance(expires_at, bool)
            or not math.isfinite(expires_at) or expires_at <= now or expires_at > now + 3600):
        raise CoordError('same-task continuation expiry must be finite, future and at most one hour')
    for value, label, maximum in ((source_message_id, 'source message ID', 256),
                                  (source_artifact_name, 'source artifact name', 256),
                                  (source_artifact_sha256, 'source artifact digest', 128),
                                  (source_kind, 'source kind', 64), (source_limit, 'source limit', 1024)):
        if not isinstance(value, str) or not value or value != value.strip() or len(value) > maximum:
            raise CoordError('invalid baseline ' + label)
    if source_kind != 'po_attested_historical':
        raise CoordError('investigation baseline provenance must remain PO-attested historical evidence')
    if (len(source_artifact_sha256) != 64
            or any(char not in '0123456789abcdef' for char in source_artifact_sha256.lower())):
        raise CoordError('baseline source artifact must have an exact SHA-256 digest')
    expected_json, expected_sha256 = canonical_expected_settings(expected_settings)
    with coord.tx():
        original = db.execute('SELECT * FROM root_team_stop_task_admissions WHERE request_id=?',
                              (admission_request_id,)).fetchone()
        if not original or original['root_session'] != actor_root:
            raise CoordError('continuation requires the exact Root-owned original admission')
        if original['expires_at'] > now:
            raise CoordError('same-task continuation is only for an already expired original admission')
        if task_admission(db, original['message_id'], now=now, include_expired=True) is None:
            raise CoordError('original task admission scope is no longer current')
        if db.execute('SELECT 1 FROM bridge_deliveries WHERE message_id=?', (original['message_id'],)).fetchone():
            raise CoordError('original message already has a delivery; continuation withheld')
        if db.execute('SELECT 1 FROM message_reads WHERE message_id=? AND reader_session=? AND provenance IN (\'recipient_full_message\',\'recipient_context_full\')',
                      (original['message_id'], original['target_session'])).fetchone():
            raise CoordError('original message was already consumed; continuation withheld')
        if db.execute('SELECT 1 FROM bridge_jobs WHERE thread_id=? AND status NOT IN (\'dispatched\',\'cancelled\')',
                      (original['thread_id'],)).fetchone():
            raise CoordError('target has an unresolved bridge job; continuation withheld')
        if db.execute('SELECT 1 FROM root_team_stop_task_baselines WHERE admission_request_id=?',
                      (admission_request_id,)).fetchone() or db.execute(
                'SELECT 1 FROM root_team_stop_task_continuations WHERE admission_request_id=?',
                (admission_request_id,)).fetchone():
            raise CoordError('this original task already has a settings baseline or continuation')
        db.execute('''INSERT INTO root_team_stop_task_baselines(
            baseline_id,admission_request_id,stop_id,root_session,po_session,target_session,thread_id,host_id,
            message_id,inspection_request_id,expected_json,expected_sha256,source_message_id,
            source_artifact_name,source_artifact_sha256,source_kind,source_limit,created_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
            (baseline_id, original['request_id'], original['stop_id'], original['root_session'],
             original['po_session'], original['target_session'], original['thread_id'], original['host_id'],
             original['message_id'], original['inspection_request_id'], expected_json, expected_sha256,
             source_message_id, source_artifact_name, source_artifact_sha256, source_kind, source_limit, now))
        db.execute('''INSERT INTO root_team_stop_task_continuations(
            continuation_id,admission_request_id,baseline_id,stop_id,root_session,po_session,target_session,
            thread_id,host_id,message_id,inspection_request_id,expires_at,created_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)''',
            (continuation_id, original['request_id'], baseline_id, original['stop_id'], original['root_session'],
             original['po_session'], original['target_session'], original['thread_id'], original['host_id'],
             original['message_id'], original['inspection_request_id'], expires_at, now))
        return {'baseline_id': baseline_id, 'baseline_sha256': expected_sha256,
                'continuation_id': continuation_id, 'admission_request_id': original['request_id'],
                'message_id': original['message_id'], 'expires_at': expires_at,
                'state': 'awaiting_current_po_metadata_preload',
                'source_kind': source_kind, 'source_limit': source_limit}


def task_settings_probe(db, message_id, *, now=None, require_match=False, allowed_control_id=None):
    """Return a currently valid same-task continuation and its immutable baseline/probe state."""
    now = time.time() if now is None else now
    admission = task_admission(db, message_id, now=now, include_expired=True,
                               allowed_control_id=allowed_control_id)
    if not admission:
        return None
    baseline = db.execute('SELECT * FROM root_team_stop_task_baselines WHERE admission_request_id=?',
                          (admission['request_id'],)).fetchone()
    continuation = db.execute('SELECT * FROM root_team_stop_task_continuations WHERE admission_request_id=?',
                              (admission['request_id'],)).fetchone()
    if not baseline or not continuation or continuation['expires_at'] <= now:
        return None
    if (baseline['admission_request_id'] != admission['request_id']
            or any(baseline[key] != admission[key] for key in (
                'stop_id','root_session','po_session','target_session','thread_id','host_id',
                'message_id','inspection_request_id'))):
        return None
    if any(continuation[key] != baseline[key] for key in (
            'admission_request_id','stop_id','root_session','po_session','target_session','thread_id',
            'host_id','message_id','inspection_request_id')) or continuation['baseline_id'] != baseline['baseline_id']:
        return None
    try:
        encoded, digest = canonical_expected_settings(json.loads(baseline['expected_json']))
    except (ValueError, TypeError, CoordError):
        return None
    if encoded != baseline['expected_json'] or digest != baseline['expected_sha256']:
        return None
    latest = db.execute('''SELECT e.*,c.status AS control_status,c.action AS control_action,
        c.requester_session AS control_requester,c.target_session AS control_target,
        c.thread_id AS control_thread,c.host_id AS control_host
        FROM root_team_stop_task_preload_requests r
        JOIN root_team_stop_task_preload_events e ON e.attempt_id=r.control_request_id
        JOIN agent_control_requests c ON c.id=r.control_request_id
        WHERE r.admission_request_id=? ORDER BY e.sequence DESC LIMIT 1''',
        (admission['request_id'],)).fetchone()
    if require_match and (not latest or latest['state'] != 'matched' or not latest['observed_json']
                          or latest['observed_sha256'] != digest or latest['control_status'] != 'completed'
                          or latest['control_action'] != 'investigation-preload'
                          or latest['control_requester'] != admission['po_session']
                          or latest['control_target'] != admission['target_session']
                          or latest['control_thread'] != admission['thread_id']
                          or latest['control_host'] != admission['host_id']):
        return None
    return dict(admission, original_admission_expired=admission['expires_at'] <= now,
                baseline=dict(baseline), continuation=dict(continuation),
                preload_event=dict(latest) if latest else None)


def task_execution_admission(db, message_id, *, now=None):
    """The original input is exposed only after the exact protected settings match."""
    return task_settings_probe(db, message_id, now=now, require_match=True)


def task_preload_scope(db, continuation_id, po_session, target_session, thread_id, host_id, *, now=None,
                       allowed_control_id=None):
    """Validate the current designated PO's exact no-input preload scope."""
    now = time.time() if now is None else now
    row = db.execute('''SELECT a.message_id FROM root_team_stop_task_continuations c
        JOIN root_team_stop_task_admissions a ON a.request_id=c.admission_request_id
        WHERE c.continuation_id=? AND c.po_session=? AND c.target_session=?
          AND c.thread_id=? AND c.host_id=? AND c.expires_at>?''',
        (continuation_id, po_session, target_session, thread_id, host_id, now)).fetchone()
    if not row:
        return None
    scoped = task_settings_probe(db, row['message_id'], now=now, allowed_control_id=allowed_control_id)
    if not scoped:
        return None
    if (scoped['po_session'], scoped['target_session'], scoped['thread_id'], scoped['host_id']) != (
            po_session, target_session, thread_id, host_id):
        return None
    if db.execute('SELECT 1 FROM bridge_deliveries WHERE message_id=?', (row['message_id'],)).fetchone():
        return None
    if db.execute('SELECT 1 FROM message_reads WHERE message_id=? AND reader_session=? AND provenance IN (\'recipient_full_message\',\'recipient_context_full\')',
                  (row['message_id'], target_session)).fetchone():
        return None
    return scoped


def task_preload_request(db, request_id, *, now=None):
    """Internal worker projection for one current-PO settings-preload request."""
    now = time.time() if now is None else now
    row = db.execute('''SELECT r.*,c.requester_session,c.target_session,c.thread_id,c.host_id,
        c.action,c.status FROM root_team_stop_task_preload_requests r
        JOIN agent_control_requests c ON c.id=r.control_request_id WHERE r.control_request_id=?''',
        (request_id,)).fetchone()
    if not row or row['action'] != 'investigation-preload':
        return None
    continuation = db.execute('SELECT * FROM root_team_stop_task_continuations WHERE continuation_id=?',
                               (row['continuation_id'],)).fetchone()
    baseline = db.execute('SELECT * FROM root_team_stop_task_baselines WHERE admission_request_id=?',
                          (row['admission_request_id'],)).fetchone()
    if not continuation or not baseline:
        return dict(row, scope_current=False, reason='immutable baseline or continuation is missing')
    scope = task_preload_scope(db, continuation['continuation_id'], row['requester_session'],
                               row['target_session'], row['thread_id'], row['host_id'], now=now,
                               allowed_control_id=request_id)
    valid = bool(scope and row['requester_session'] == continuation['po_session']
                 and continuation['admission_request_id'] == row['admission_request_id']
                 and continuation['baseline_id'] == baseline['baseline_id'])
    return dict(row, scope_current=valid,
                reason=None if valid else 'current PO, task continuation, host, stop or input scope changed',
                continuation=dict(continuation), baseline=dict(baseline))


def append_task_preload_event(coord, attempt_id, continuation_id, admission_request_id,
                              state, *, observed=None, reason=None, now=None):
    """Append one idempotency-fenced event for the single metadata-preload attempt."""
    now = time.time() if now is None else now
    if state not in ('requested', 'preloading', 'matched', 'blocked', 'uncertain'):
        raise CoordError('invalid investigation settings preload event')
    if observed is not None:
        observed_json = json.dumps(observed, sort_keys=True, separators=(',', ':'), ensure_ascii=False)
        observed_sha256 = hashlib.sha256(observed_json.encode('utf-8')).hexdigest()
    else:
        observed_json = observed_sha256 = None
    if reason is not None and (not isinstance(reason, str) or len(reason) > 1024):
        raise CoordError('invalid investigation settings preload reason')
    with coord.tx():
        request = coord.db.execute('''SELECT * FROM root_team_stop_task_preload_requests
            WHERE control_request_id=? AND continuation_id=? AND admission_request_id=?''',
            (attempt_id, continuation_id, admission_request_id)).fetchone()
        if not request:
            raise CoordError('settings preload event does not match its immutable PO request')
        latest = coord.db.execute('SELECT * FROM root_team_stop_task_preload_events WHERE attempt_id=? ORDER BY sequence DESC LIMIT 1',
                                  (attempt_id,)).fetchone()
        if latest and latest['state'] in ('matched', 'blocked', 'uncertain'):
            if (latest['state'], latest['observed_json'], latest['reason']) != (state, observed_json, reason):
                raise CoordError('settings preload attempt is already terminal; no retry is permitted')
            return dict(latest)
        allowed = {None: {'requested'}, 'requested': {'preloading', 'blocked'},
                   'preloading': {'matched', 'blocked', 'uncertain'}}
        if state not in allowed.get(latest['state'] if latest else None, set()):
            raise CoordError('settings preload attempt has an invalid event order')
        if state == 'preloading' and latest and latest['state'] == 'preloading':
            return dict(latest)
        sequence = (latest['sequence'] + 1) if latest else 1
        coord.db.execute('''INSERT INTO root_team_stop_task_preload_events(
            attempt_id,sequence,continuation_id,admission_request_id,state,observed_json,
            observed_sha256,reason,created_at) VALUES(?,?,?,?,?,?,?,?,?)''',
            (attempt_id, sequence, continuation_id, admission_request_id, state,
             observed_json, observed_sha256, reason, now))
        return dict(coord.db.execute('SELECT * FROM root_team_stop_task_preload_events WHERE attempt_id=? AND sequence=?',
                                      (attempt_id, sequence)).fetchone())


def task_admission_for_job(db, job_id, *, now=None):
    rows = db.execute('''SELECT m.id,m.recipient_session FROM bridge_deliveries d
        JOIN messages m ON m.id=d.message_id WHERE d.job_id=? ORDER BY m.seq''', (job_id,)).fetchall()
    if len(rows) != 1:
        return None
    admission = task_execution_admission(db, rows[0]['id'], now=now)
    if not admission:
        return None
    job = db.execute('SELECT thread_id,status,queue_id FROM bridge_jobs WHERE id=?', (job_id,)).fetchone()
    if not job or job['thread_id'] != admission['thread_id'] or job['status'] not in ('prepared','adding','queued','starting'):
        return None
    return admission


def task_inspection_admission(db, request_id, *, now=None):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_team_stop_task_admissions'").fetchone():
        return None
    rows = db.execute('SELECT message_id FROM root_team_stop_task_admissions WHERE inspection_request_id=?',
                      (request_id,)).fetchall()
    if len(rows) != 1:
        return None
    return task_admission(db, rows[0]['message_id'], now=now)


def investigation_preflight(db, root, stop_id, po, target, thread, host,
                            request_id, message_id, inspection_request_id, expires_at,
                            *, now=None):
    """Read-only, value-free diagnostics for one proposed investigation admission."""
    now = time.time() if now is None else now

    def has_table(name):
        return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                               (name,)).fetchone())

    def row(sql, values):
        try:
            return db.execute(sql, values).fetchone()
        except sqlite3.OperationalError:
            return None

    tables = {name: has_table(name) for name in (
        'root_team_stops', 'root_team_stop_targets', 'root_team_stop_po_admissions',
        'project_orchestrators', 'bridge_bindings', 'remote_session_hosts', 'messages',
        'agent_control_requests', 'message_batches', 'bridge_deliveries',
        'root_team_stop_task_admissions', 'message_reads', 'session_resets',
        'agent_controls')}
    stop = row('SELECT root_session,active FROM root_team_stops WHERE id=?', (stop_id,)) if tables['root_team_stops'] else None
    original = row('''SELECT requester_session,thread_id,host_id,status FROM root_team_stop_targets
        WHERE stop_id=? AND target_session=?''', (stop_id, target)) if tables['root_team_stop_targets'] else None
    po_admission = row('''SELECT root_session,target_session,thread_id,host_id
        FROM root_team_stop_po_admissions WHERE stop_id=?''', (stop_id,)) if tables['root_team_stop_po_admissions'] else None
    designation = row('SELECT root_session FROM project_orchestrators WHERE session_id=?',
                      (po,)) if tables['project_orchestrators'] else None
    po_binding = row('SELECT thread_id,parent_session FROM bridge_bindings WHERE session_id=?',
                     (po,)) if tables['bridge_bindings'] else None
    po_host = row('SELECT host_id FROM remote_session_hosts WHERE session_id=?',
                  (po,)) if tables['remote_session_hosts'] else None
    binding = row('SELECT thread_id,parent_session FROM bridge_bindings WHERE session_id=?',
                  (target,)) if tables['bridge_bindings'] else None
    host_row = row('SELECT host_id FROM remote_session_hosts WHERE session_id=?',
                   (target,)) if tables['remote_session_hosts'] else None
    message = row('SELECT sender_session,recipient_session FROM messages WHERE id=?',
                  (message_id,)) if tables['messages'] else None
    inspection = row('SELECT * FROM agent_control_requests WHERE id=?',
                     (inspection_request_id,)) if tables['agent_control_requests'] else None
    saved = row('SELECT * FROM root_team_stop_task_admissions WHERE request_id=? OR message_id=?',
                (request_id, message_id)) if tables['root_team_stop_task_admissions'] else None
    saved_matches = True
    if saved:
        saved_matches = (saved['request_id'] == request_id and tuple(saved[key] for key in (
            'stop_id', 'root_session', 'po_session', 'target_session', 'thread_id', 'host_id',
            'message_id', 'inspection_request_id', 'purpose', 'expires_at')) ==
            (stop_id, root, po, target, thread, host, message_id, inspection_request_id,
             'investigation', expires_at))
    checks = {
        'target_project_stop_matches_target_root': bool(stop and stop['root_session'] == root),
        'original_stop_is_active': bool(stop and stop['active'] == 1),
        'stop_target_matches_target_root_thread_host': bool(original and
            (original['requester_session'], original['thread_id'], original['host_id']) ==
            (root, thread, host)),
        'stop_target_status_admissible': bool(original and original['status'] in
            ('pending', 'waiting', 'completed')),
        'po_stop_admission_matches_target_root': bool(po_admission and
            (po_admission['root_session'], po_admission['target_session']) == (root, po)),
        'po_designation_matches_target_root': bool(designation and designation['root_session'] == root),
        'po_binding_matches_recorded_admission': bool(po_admission and po_binding and
            (po_binding['thread_id'], po_binding['parent_session']) ==
            (po_admission['thread_id'], None)),
        'po_host_matches_recorded_admission': bool(po_admission and po_host and
            po_host['host_id'] == po_admission['host_id']),
        'worker_binding_matches_stop_target': bool(binding and
            (binding['thread_id'], binding['parent_session']) == (thread, None)),
        'worker_host_matches_stop_target': bool(host_row and host_row['host_id'] == host),
        'message_sender_recipient_matches_po_worker': bool(message and
            (message['sender_session'], message['recipient_session']) == (po, target)),
        'inspection_request_present': bool(inspection),
        'inspection_request_requester_matches_target_root': bool(inspection and
            inspection['requester_session'] == root),
        'inspection_request_requester_matches_current_admitted_po': bool(inspection and
            inspection['requester_session'] == po and po_admitted(db, stop_id, po)),
        'inspection_request_target_matches_worker': bool(inspection and
            inspection['target_session'] == target),
        'inspection_request_thread_matches_worker': bool(inspection and
            inspection['thread_id'] == thread),
        'inspection_request_host_matches_worker': bool(inspection and
            inspection['host_id'] == host),
        'inspection_request_action_is_inspect': bool(inspection and
            inspection['action'] == 'inspect'),
        'inspection_request_is_noninterrupting': bool(inspection and
            inspection['interrupt'] == 0),
        'inspection_request_matches_root_worker_binding': bool(inspection and
            (inspection['target_session'], inspection['thread_id'], inspection['host_id'],
             inspection['action'], inspection['interrupt']) == (target, thread, host, 'inspect', 0)
            and (inspection['requester_session'] == root or
                 (inspection['requester_session'] == po and po_admitted(db, stop_id, po)))),
        'inspection_request_status_admissible': bool(inspection and
            inspection['status'] in ('pending', 'waiting', 'completed')),
        'expiry_within_one_hour': bool(isinstance(expires_at, (int, float)) and
            not isinstance(expires_at, bool) and now < expires_at <= now + 3600),
        'request_and_message_scope_available': saved_matches,
        'message_not_batched': bool(tables['message_batches'] and not row(
            'SELECT 1 FROM message_batches WHERE message_id=?', (message_id,))),
        'message_has_no_existing_delivery': bool(tables['bridge_deliveries'] and not row(
            'SELECT 1 FROM bridge_deliveries WHERE message_id=?', (message_id,))),
        'no_other_active_stop_barrier': bool(tables['root_team_stops'] and not row(
            'SELECT 1 FROM root_team_stops WHERE active=1 AND id<>?', (stop_id,))),
        'message_unread_by_worker': bool(tables['message_reads'] and not row(
            "SELECT 1 FROM message_reads WHERE message_id=? AND reader_session=? "
            "AND provenance IN ('recipient_full_message','recipient_context_full')", (message_id, target))),
        'worker_has_no_other_pending_control': bool(tables['agent_control_requests'] and not row(
            "SELECT 1 FROM agent_control_requests WHERE target_session=? AND id<>? "
            "AND status NOT IN ('completed','failed','cancelled','resolved_unchanged')",
            (target, inspection_request_id))),
        'worker_has_no_pending_reset': bool(tables['session_resets'] and not row(
            "SELECT 1 FROM session_resets WHERE target_session=? "
            "AND status NOT IN ('completed','cancelled','failed')", (target,))),
        'worker_control_is_running': bool(tables['agent_controls'] and
            (row('SELECT mode FROM agent_controls WHERE target_session=?', (target,)) or
             {'mode': 'running'})['mode'] == 'running'),
    }
    from .root_team_reset import quarantined
    checks['worker_is_not_quarantined'] = not quarantined(db, session=target)
    alternatives = {'inspection_request_requester_matches_target_root',
                    'inspection_request_requester_matches_current_admitted_po'}
    required = {name: passed for name, passed in checks.items() if name not in alternatives}
    return {'status': 'eligible' if all(required.values()) else 'blocked',
            'checks': checks,
            'failed_checks': [name for name, passed in required.items() if not passed]}


def admit_investigation_task(coord, root, request_id, stop_id, po, target, thread, host,
                             message_id, inspection_request_id, expires_at, *, now=None):
    """Persist one Root-authorized existing PO message under one active stop."""
    schema(coord.db)
    try:
        request_id = str(uuid.UUID(request_id))
    except (TypeError, ValueError, AttributeError) as error:
        raise CoordError('investigation admission request_id must be a UUID') from error
    now = time.time() if now is None else now
    if (not isinstance(expires_at, (int,float)) or isinstance(expires_at, bool)
            or expires_at <= now or expires_at > now + 3600):
        raise CoordError('investigation admission expiry must be within the next hour')
    db = coord.db
    with coord.tx():
        saved = db.execute('SELECT * FROM root_team_stop_task_admissions WHERE request_id=? OR message_id=?',
                           (request_id,message_id)).fetchone()
        wanted = (stop_id,root,po,target,thread,host,message_id,inspection_request_id,'investigation',expires_at)
        if saved:
            actual = tuple(saved[key] for key in ('stop_id','root_session','po_session','target_session','thread_id',
                                                   'host_id','message_id','inspection_request_id','purpose','expires_at'))
            if saved['request_id'] != request_id or actual != wanted:
                raise CoordError('investigation admission retry differs from its immutable scope')
            current = task_admission(db,message_id,now=now)
            if not current:
                raise CoordError('investigation admission is no longer effective')
            return dict(saved, status='admitted', workers_released=False, native_execution_proven=False)
        stop = db.execute('SELECT root_session,active FROM root_team_stops WHERE id=?',(stop_id,)).fetchone()
        original = db.execute('''SELECT requester_session,thread_id,host_id,status FROM root_team_stop_targets
            WHERE stop_id=? AND target_session=?''',(stop_id,target)).fetchone()
        po_admission = db.execute('SELECT root_session,target_session,thread_id,host_id FROM root_team_stop_po_admissions WHERE stop_id=?',(stop_id,)).fetchone()
        designation = db.execute('SELECT root_session FROM project_orchestrators WHERE session_id=?',(po,)).fetchone()
        binding = db.execute('SELECT thread_id,parent_session FROM bridge_bindings WHERE session_id=?',(target,)).fetchone()
        host_row = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?',(target,)).fetchone()
        message = db.execute('SELECT sender_session,recipient_session FROM messages WHERE id=?',(message_id,)).fetchone()
        inspection = db.execute('SELECT * FROM agent_control_requests WHERE id=?',(inspection_request_id,)).fetchone()
        inspection_requester_authorized = bool(inspection and
            (inspection['requester_session'] == root or
             (inspection['requester_session'] == po and po_admitted(db, stop_id, po))))
        inspection_binding_matches = bool(inspection and
            (inspection['target_session'],inspection['thread_id'],inspection['host_id'],
             inspection['action'],inspection['interrupt']) == (target,thread,host,'inspect',0))
        if (not stop or stop['root_session'] != root or stop['active'] != 1 or not original
                or (original['requester_session'],original['thread_id'],original['host_id']) != (root,thread,host)
                or original['status'] == 'interrupting' or not po_admission
                or (po_admission['root_session'],po_admission['target_session']) != (root,po)
                or not designation or designation['root_session'] != root
                or not binding or (binding['thread_id'],binding['parent_session']) != (thread,None)
                or not host_row or host_row['host_id'] != host
                or not message or (message['sender_session'],message['recipient_session']) != (po,target)
                or not inspection_binding_matches or not inspection_requester_authorized
                or inspection['status'] not in ('pending','waiting','completed')):
            raise CoordError('investigation admission current PO, stop, task message, inspect request or worker binding differs')
        if db.execute('SELECT 1 FROM message_batches WHERE message_id=?',(message_id,)).fetchone():
            raise CoordError('investigation task message belongs to a batch and cannot be isolated')
        delivery = db.execute('SELECT 1 FROM bridge_deliveries WHERE message_id=?', (message_id,)).fetchone()
        if delivery:
            raise CoordError('existing task delivery cannot be adopted; original message will not be replayed')
        db.execute('''INSERT INTO root_team_stop_task_admissions VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''',
            (request_id,stop_id,root,po,target,thread,host,message_id,inspection_request_id,'investigation',expires_at,now))
        current = task_admission(db,message_id,now=now)
        if not current:
            raise CoordError('investigation admission failed its commit-time scope recheck')
        return dict(current, status='admitted', workers_released=False, native_execution_proven=False)


def coordination_status(db, root_session, designated_session, stop_id=None):
    """Read the retained PO binding and distinguish designation from stop admission."""
    result = {
        'designated_session': designated_session,
        'designated_role_current': False,
        'native_thread_id': None,
        'native_parent_session': None,
        'registered_host_id': None,
        'admissions': [],
        'task_admissions': [],
    }
    tables = {name for name in ('project_orchestrators','bridge_bindings','remote_session_hosts')
              if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone()}
    if designated_session and 'project_orchestrators' in tables:
        result['designated_role_current'] = bool(db.execute(
            'SELECT 1 FROM project_orchestrators WHERE session_id=? AND root_session=?',
            (designated_session, root_session)).fetchone())
    if designated_session and 'bridge_bindings' in tables:
        binding = db.execute('SELECT thread_id,parent_session FROM bridge_bindings WHERE session_id=?',
                             (designated_session,)).fetchone()
        if binding:
            result['native_thread_id'] = binding['thread_id']
            result['native_parent_session'] = binding['parent_session']
    if designated_session and 'remote_session_hosts' in tables:
        host = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?',
                          (designated_session,)).fetchone()
        result['registered_host_id'] = host['host_id'] if host else None

    if not all(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone()
               for name in ('root_team_stops','root_team_stop_po_admissions')):
        return result
    if stop_id is None:
        stops = [row['id'] for row in db.execute(
            'SELECT id FROM root_team_stops WHERE root_session=? AND active=1 ORDER BY created_at,id',
            (root_session,))]
    else:
        stops = [stop_id]
    for current_stop in stops:
        row = db.execute('SELECT * FROM root_team_stop_po_admissions WHERE stop_id=? AND root_session=?',
                         (current_stop, root_session)).fetchone()
        if not row:
            continue
        target = row['target_session']
        binding = db.execute('SELECT thread_id,parent_session FROM bridge_bindings WHERE session_id=?',
                             (target,)).fetchone() if 'bridge_bindings' in tables else None
        host = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?',
                          (target,)).fetchone() if 'remote_session_hosts' in tables else None
        effective = po_admitted(db, current_stop, target)
        result['admissions'].append({
            'stop_id': current_stop,
            'target_session': target,
            'recorded_thread_id': row['thread_id'],
            'recorded_host_id': row['host_id'],
            'effective': effective,
            'matches_current_designation': target == designated_session,
            'matches_current_native_binding': bool(binding and binding['parent_session'] is None
                                                   and binding['thread_id'] == row['thread_id']),
            'matches_current_registered_host': bool(host and host['host_id'] == row['host_id']),
            'created_at': row['created_at'],
        })
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_team_stop_task_admissions'").fetchone():
        task_rows = []
        if stops:
            placeholders = ','.join('?' for _ in stops)
            task_rows = db.execute(f'''SELECT * FROM root_team_stop_task_admissions
                WHERE root_session=? AND stop_id IN ({placeholders}) ORDER BY created_at,message_id''',
                (root_session, *stops)).fetchall()
        for row in task_rows:
            result['task_admissions'].append({
                'request_id': row['request_id'],
                'stop_id': row['stop_id'],
                'po_session': row['po_session'],
                'target_session': row['target_session'],
                'thread_id': row['thread_id'],
                'host_id': row['host_id'],
                'message_id': row['message_id'],
                'inspection_request_id': row['inspection_request_id'],
                'purpose': row['purpose'],
                'expires_at': row['expires_at'],
                'created_at': row['created_at'],
                'effective': bool(task_admission(db, row['message_id'])),
            })
    return result


def admit_po(coord, root, request_id, target, thread, host):
    schema(coord.db)
    with coord.tx() as db:
        stop = db.execute('SELECT * FROM root_team_stops WHERE id=?', (request_id,)).fetchone()
        saved = db.execute('SELECT * FROM root_team_stop_po_admissions WHERE stop_id=?', (request_id,)).fetchone()
        original = db.execute('SELECT * FROM root_team_stop_targets WHERE stop_id=? AND target_session=?', (request_id,target)).fetchone()
        if not stop or stop['root_session'] != root or not original:
            raise CoordError('PO admission requires its exact original root stop scope')
        binding = db.execute('SELECT thread_id,parent_session FROM bridge_bindings WHERE session_id=?', (target,)).fetchone()
        owner = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (target,)).fetchone()
        role = db.execute('SELECT root_session FROM project_orchestrators WHERE session_id=?', (target,)).fetchone()
        if not role or role['root_session'] != root or target == root:
            raise CoordError('PO admission requires the retained designated project orchestrator')
        if not binding or binding['parent_session'] or binding['thread_id'] != thread or not owner or owner['host_id'] != host:
            raise CoordError('PO admission current native binding or host differs')
        if (original['thread_id'],original['host_id']) != (thread,host):
            raise CoordError('PO admission differs from the original stop target binding or host')
        from .root_team_reset import quarantined
        if quarantined(db,session=target):
            raise CoordError('quarantined former team cannot receive PO admission')
        if original['status'] == 'interrupting':
            raise CoordError('resolve the existing stop interrupt before PO admission')
        # Admitting the PO must not accidentally release an older queued model,
        # resume or native reset. Preserve those journals for normal resolution.
        for table, pending in (
                ('agent_control_requests', "action<>'inspect' AND status NOT IN ('completed','failed','cancelled','resolved_unchanged')"),
                ('session_resets', "status NOT IN ('completed','cancelled','failed')")):
            if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone() and db.execute(
                    'SELECT 1 FROM ' + table + ' WHERE target_session=? AND ' + pending, (target,)).fetchone():
                raise CoordError('resolve the existing PO native lifecycle request before admission')
        if saved:
            if (saved['root_session'],saved['target_session'],saved['thread_id'],saved['host_id']) != (root,target,thread,host):
                raise CoordError('PO admission retry payload differs from the immutable original')
        else:
            if not stop['active']:
                raise CoordError('PO admission requires an active original barrier')
            db.execute('INSERT INTO root_team_stop_po_admissions VALUES(?,?,?,?,?,?)',
                       (request_id,root,target,thread,host,time.time()))
        result = dict(db.execute('SELECT * FROM root_team_stop_po_admissions WHERE stop_id=?', (request_id,)).fetchone())
        return dict(result, effective=po_admitted(db,request_id,target),
                    project_stop_active=bool(stop['active']), workers_released=False,
                    native_execution_proven=False, purpose='PO coordination only')


def parked(db, target):
    # A verified, bound parked or completed creation is governed by the stop
    # itself. Incomplete or uncertain creations remain withheld after resume.
    for table in ('root_parked_creations', 'root_turnover_subordinates'):
        if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone():
            continue
        row = db.execute('''SELECT r.id AS original_request,p.stop_id,r.status,r.new_thread_id,r.settings_digest,r.new_settings_digest,
                    r.host_id,b.thread_id,b.parent_session,h.host_id AS current_host,
                    s.active AS stop_active,s.resumed_at,s.root_session AS stop_root,p.root_session AS expected_root
                FROM ''' + table + ''' p JOIN session_resets r ON r.id=p.request_id
                LEFT JOIN bridge_bindings b ON b.session_id=p.target_session
                LEFT JOIN remote_session_hosts h ON h.session_id=p.target_session
                LEFT JOIN root_team_stops s ON s.id=p.stop_id
                WHERE p.target_session=? AND r.status NOT IN ('cancelled','failed')''', (target,)).fetchone()
        if row:
            expected_thread = row['new_thread_id']
            if table == 'root_turnover_subordinates':
                from .parked_binding_repair import effective_thread
                expected_thread = effective_thread(db, row['original_request'], row['stop_id'], target, expected_thread,
                                                   row['host_id'], row['settings_digest'])
            verified = (row['status'] in ('parked','completed') and expected_thread
                        and row['thread_id'] == expected_thread and row['parent_session'] is None
                        and row['settings_digest'] and row['settings_digest'] == row['new_settings_digest']
                        and row['current_host'] == row['host_id'] and row['stop_active'] == 0
                        and row['resumed_at'] is not None and row['stop_root'] == row['expected_root'])
            return not bool(verified)
    return False


def stopped(db, target):
    from .root_team_reset import quarantined
    if quarantined(db,session=target) or parked(db,target):
        return True
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_team_stops'").fetchone():
        return False
    active = [row['id'] for row in db.execute('SELECT id FROM root_team_stops WHERE active=1')]
    return bool(active
                and not db.execute('SELECT 1 FROM root_projections WHERE session_id=?', (target,)).fetchone()
                and not db.execute('SELECT 1 FROM operator_identities WHERE session_id=?', (target,)).fetchone()
                and any(not po_admitted(db,stop_id,target) for stop_id in active))


def released_parked(db, target):
    """Only a verified parked creation becomes ordinary after its stop resumes."""
    for table in ('root_parked_creations', 'root_turnover_subordinates'):
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone():
            if db.execute('''SELECT 1 FROM ''' + table + ''' p JOIN session_resets r ON r.id=p.request_id
                             WHERE p.target_session=? AND r.status='parked' ''', (target,)).fetchone():
                return not stopped(db, target)
    return False


def job(db, request_id):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_team_stop_targets'").fetchone():
        return None
    row = db.execute('SELECT * FROM root_team_stop_targets WHERE id=?', (request_id,)).fetchone()
    return dict(row, action='stop', interrupt=1, root_team_stop=True) if row else None


def pause(coord, root, request_id):
    schema(coord.db)
    with coord.tx() as db:
        old = db.execute('SELECT * FROM root_team_stops WHERE id=?', (request_id,)).fetchone()
        if old:
            if old['root_session'] != root:
                raise CoordError('stop-all request belongs to a different root')
            return status(db, request_id)
        active = db.execute('SELECT id FROM root_team_stops WHERE active=1').fetchone()
        if active:
            raise CoordError('another stop-all barrier is active; resume its exact request first')
        now = time.time()
        db.execute('INSERT INTO root_team_stops VALUES(?,?,1,?,NULL)', (request_id, root, now))
        rows = db.execute('''SELECT s.id,b.thread_id,h.host_id FROM sessions s
            LEFT JOIN bridge_bindings b ON b.session_id=s.id
            LEFT JOIN remote_session_hosts h ON h.session_id=s.id
            WHERE s.id NOT IN (SELECT session_id FROM root_projections)
              AND s.id NOT IN (SELECT session_id FROM operator_identities)''').fetchall()
        for row in rows:
            identifier = str(uuid.uuid5(uuid.UUID(request_id), row['id']))
            pending = bool(row['thread_id'] and row['host_id'])
            db.execute('INSERT INTO root_team_stop_targets VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                       (identifier, request_id, root, row['id'], row['host_id'], row['thread_id'],
                        'pending' if pending else 'stopped', None,
                        None if pending else 'No bound authenticated main turn; admission barrier retained.', now, now))
        return status(db, request_id)


def resume(coord, root, request_id):
    schema(coord.db)
    with coord.tx() as db:
        row = db.execute('SELECT * FROM root_team_stops WHERE id=?', (request_id,)).fetchone()
        if not row or row['root_session'] != root:
            raise CoordError('resume-all requires the exact original root stop request')
        if row['active']:
            if (db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_turnover_bootstraps'").fetchone()
                    and db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='execution_epoch_handoffs'").fetchone()
                    and db.execute('''SELECT 1 FROM root_turnover_bootstraps b
                        JOIN session_resets j ON j.id=b.request_id AND j.target_session=b.target_session
                        JOIN execution_epoch_handoffs e ON e.actor=b.target_session AND e.host=b.host_id
                          AND e.thread=j.old_thread_id AND e.started_at>=j.updated_at
                        WHERE b.stop_id=? AND e.settled_at IS NULL
                          AND ((j.status='adding' AND e.method='thread/queue/add')
                            OR (j.status='starting' AND e.method='thread/queue/start'))''', (request_id,)).fetchone()):
                raise CoordError('turnover bootstrap native input is in flight; reconcile it before resuming this stop')
            db.execute('UPDATE root_team_stops SET active=0,resumed_at=? WHERE id=?', (time.time(), request_id))
        return status(db, request_id)


def status(db, request_id):
    row = db.execute('SELECT * FROM root_team_stops WHERE id=?', (request_id,)).fetchone()
    return dict(row, targets=[dict(item) for item in db.execute(
        'SELECT * FROM root_team_stop_targets WHERE stop_id=? ORDER BY target_session', (request_id,))])


def jobs(db, host):
    from .root_team_reset import quarantined
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_team_stops'").fetchone():
        return []
    return [item for row in db.execute('''SELECT t.id FROM root_team_stop_targets t
        JOIN root_team_stops s ON s.id=t.stop_id WHERE s.active=1 AND t.host_id=?
        AND t.thread_id IS NOT NULL ORDER BY t.created_at''', (host,))
        if not quarantined(db,session=(item:=job(db,row['id']))['target_session'])
        and not po_admitted(db,item['stop_id'],item['target_session'])]


def update(coord, request_id, expected, status_value, *, turn_id=None, error=None, observation=None):
    with coord.tx() as db:
        current = job(db, request_id)
        from .root_team_reset import quarantined,handoff_settling,handoff_pending
        if current and quarantined(db,session=current['target_session']):
            same_interrupt=(status_value=='interrupting' and expected=='interrupting' and turn_id==current['turn_id']
                and any(row['kind']=='control' and row['job_id']==request_id and row['host_id']==current['host_id'] for row in handoff_pending(db)))
            if not same_interrupt and not handoff_settling(db,'control',request_id,current['host_id'],status_value):
                raise CoordError('team reset quarantined this former target; native calls withheld')
        if not current or current['status'] != expected:
            raise CoordError('root team stop state changed; inspect current request')
        active = db.execute('SELECT active FROM root_team_stops WHERE id=?', (current['stop_id'],)).fetchone()
        if not active or not active['active']:
            raise CoordError('root team stop barrier was resumed; native interruption withheld')
        if po_admitted(db,current['stop_id'],current['target_session']):
            raise CoordError('PO coordination admission withheld further stop interruption')
        binding = db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?', (current['target_session'],)).fetchone()
        host = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (current['target_session'],)).fetchone()
        if not binding or binding['thread_id'] != current['thread_id'] or not host or host['host_id'] != current['host_id']:
            raise CoordError('root team stop target binding or host changed; native interruption withheld')
        if status_value not in ('pending','waiting','interrupting','completed'):
            raise CoordError('invalid root team stop transition')
        if status_value == 'completed' and (not observation or observation['state'] != 'idle' or observation['queue_count']):
            raise CoordError('root team stop completion requires idle and empty native queue')
        if status_value == 'interrupting':
            if not turn_id:
                raise CoordError('root team stop interruption requires an observed exact main turn')
            if expected == 'interrupting' and turn_id != current['turn_id']:
                raise CoordError('unresolved root stop interrupt cannot target another turn')
            if expected != 'interrupting':
                try:
                    db.execute('INSERT INTO root_team_stop_interrupts VALUES(?,?,?)', (request_id, turn_id, time.time()))
                except sqlite3.IntegrityError as failure:
                    raise CoordError('root stop interrupt is already recorded; no replay') from failure
        db.execute('UPDATE root_team_stop_targets SET status=?,turn_id=?,error=?,updated_at=? WHERE id=?',
                   (status_value, turn_id, error, time.time(), request_id))
        return job(db, request_id)
