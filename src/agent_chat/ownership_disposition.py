"""Root-only local disposition with a bounded passive native admission sample.

There is intentionally no HTTP, coordinator-RPC, or ordinary-client adapter for
this module.  The configured local service provider is the authority; all CLI
identity fields are exact target data/audit data, never caller credentials.
"""
from __future__ import annotations

import argparse
import contextlib
import json
import math
from dataclasses import dataclass
from pathlib import Path
import sqlite3
import time
import uuid

from .core import CoordError
from .rpc import RpcClient, RpcError, TransportError
from .service import ServiceError, configured_server_database, _installed_server_config


ENDPOINT = 'ws://127.0.0.1:4500'
SAMPLE_MAX_AGE = 5.0
_TERMINAL = ('completed', 'failed', 'cancelled', 'resolved_unchanged')


@contextlib.contextmanager
def _read_existing(path: Path):
    try:
        db = sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True)
        db.row_factory = sqlite3.Row
    except sqlite3.Error as error:
        raise CoordError('configured application state is unreadable') from error
    try:
        yield db
    finally:
        db.close()


def _has_table(db, table: str) -> bool:
    return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone())


def _require(db, *tables: str) -> None:
    if not all(_has_table(db, table) for table in tables):
        raise CoordError('configured application state is incomplete')


def _clean(value: object, label: str, limit: int = 2048) -> str:
    if not isinstance(value, str) or not value or value != value.strip() or len(value) > limit or any(ord(c) < 32 for c in value):
        raise CoordError(label + ' must be an exact bounded string')
    return value


@dataclass(frozen=True)
class ConfiguredRoot:
    master: Path
    registry: Path
    home_project: str
    home_session: str
    host_id: str
    endpoint: str


def load_configured_root(master_db: str | Path, service_plist: str | Path) -> ConfiguredRoot:
    """Validate provider lineage before a candidate target is selected."""
    try:
        configured_master = configured_server_database(service_plist)
        config = _installed_server_config()
    except ServiceError as error:
        raise CoordError(str(error)) from error
    master = Path(master_db).expanduser().resolve()
    if master != configured_master:
        raise CoordError('master database does not match the installed configured server')
    if config.host != '127.0.0.1' or config.port != 8765:
        raise CoordError('configured local server endpoint is not the pinned localhost endpoint')
    registry = Path(str(master) + '.projects.sqlite3')
    if not master.is_file() or not registry.is_file():
        raise CoordError('configured master database or registry is missing')
    with _read_existing(master) as db:
        _require(db, 'meta')
        if any(_has_table(db, item) for item in ('root_config', 'root_fresh_requests', 'root_team_operations')):
            raise CoordError('configured Root lifecycle state must reside in the canonical registry')
    with _read_existing(registry) as db:
        _require(db, 'projects', 'root_config', 'root_fresh_requests', 'root_team_operations')
        row = db.execute('SELECT home_project,home_session,host_id,mode FROM root_config WHERE singleton=1').fetchone()
        if not row or row['mode'] != 'running' or not all(isinstance(row[name], str) and row[name] for name in ('home_project', 'home_session', 'host_id')):
            raise CoordError('configured Root is unavailable for administrative disposition')
        if db.execute("SELECT 1 FROM root_fresh_requests WHERE status<>'completed'").fetchone():
            raise CoordError('configured Root has a pending fresh-start')
        if db.execute('SELECT 1 FROM root_team_operations WHERE resumed_at IS NULL').fetchone():
            raise CoordError('configured Root stop barrier is active')
        if not db.execute('SELECT 1 FROM projects WHERE id=?', (row['home_project'],)).fetchone():
            raise CoordError('configured Root home project is absent from the registry')
        return ConfiguredRoot(master, registry, row['home_project'], row['home_session'], row['host_id'], ENDPOINT)


def target_path(root: ConfiguredRoot, project: str) -> Path:
    project = _clean(project, 'project', 512)
    with _read_existing(root.registry) as db:
        if not db.execute('SELECT 1 FROM projects WHERE id=?', (project,)).fetchone():
            raise CoordError('target project is absent from the configured registry')
    path = root.master if project == 'default' else root.master.parent / (root.master.name + '.projects') / project / 'state.sqlite3'
    if not path.is_file():
        raise CoordError('registered target project database is missing')
    with _read_existing(path) as db:
        _require(db, 'resources', 'meta', 'sessions', 'bridge_bindings', 'bridge_jobs', 'remote_session_hosts')
        project_id = db.execute("SELECT value FROM meta WHERE key='project_id'").fetchone()
        source = db.execute("SELECT value FROM meta WHERE key='project_registry'").fetchone()
        if project == 'default':
            # The configured master is authoritative for default.  Older valid
            # default stores need not carry project_registry metadata, but any
            # supplied lineage must not redirect it.
            if project_id and project_id['value'] != 'default':
                raise CoordError('target metadata does not match the configured registry')
            if source:
                try:
                    if Path(source['value']).expanduser().resolve() != root.master:
                        raise CoordError('target metadata does not match the configured registry')
                except (TypeError, ValueError, OSError) as error:
                    raise CoordError('target metadata does not match the configured registry') from error
        else:
            try:
                valid = project_id and project_id['value'] == project and source and Path(source['value']).expanduser().resolve() == root.master
            except (TypeError, ValueError, OSError):
                valid = False
            if not valid:
                raise CoordError('target metadata does not match the configured registry')
    return path


def _payload(*, root: ConfiguredRoot, project: str, resource: str, reservation_id: str, owner_session: str,
             expected_state: str, request_id: str, reason: str, continuation_request_id: str | None = None,
             historical_blocked_request_id: str | None = None,
             legacy_selection: bool = False) -> dict[str, str]:
    if continuation_request_id is not None and historical_blocked_request_id is not None:
        raise CoordError('historical linkage and ordinary continuation are distinct operations')
    if expected_state != 'stale':
        raise CoordError('administrative disposition requires expected stale state')
    try:
        request_id = str(uuid.UUID(request_id))
    except (ValueError, TypeError, AttributeError) as error:
        raise CoordError('request_id must be a UUID') from error
    resource = _clean(resource, 'resource', 1024)
    forms = ('file:', 'validation:', 'operation:')
    physical = Path(resource)
    raw_physical = (physical.anchor == '/' and len(physical.parts) > 1
                    and physical.as_posix() == resource and '..' not in physical.parts)
    if (not legacy_selection and resource != 'git-index' and not raw_physical
            and not any(resource.startswith(prefix) and len(resource) > len(prefix) for prefix in forms)):
        raise CoordError('administrative disposition requires an approved canonical resource form')
    payload = {'project': _clean(project, 'project', 512), 'resource': resource,
            'reservation_id': _clean(reservation_id, 'reservation_id', 512),
            'owner_session': _clean(owner_session, 'owner_session', 512), 'expected_state': expected_state,
            'request_id': request_id, 'reason': _clean(reason, 'reason'),
            'requester_session': root.home_session, 'requester_host': root.host_id,
            'endpoint': root.endpoint}
    if continuation_request_id is not None:
        try:
            payload['continuation_of'] = str(uuid.UUID(continuation_request_id))
        except (ValueError, TypeError, AttributeError) as error:
            raise CoordError('continuation_request_id must be a UUID') from error
    if historical_blocked_request_id is not None:
        try:
            original_id = str(uuid.UUID(historical_blocked_request_id))
        except (ValueError, TypeError, AttributeError) as error:
            raise CoordError('historical_blocked_request_id must be a UUID') from error
        if original_id == request_id:
            raise CoordError('current operation requires a distinct request UUID')
        payload['historical_blocked_of'] = original_id
    return payload


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def _sample(thread_id: str, endpoint: str = ENDPOINT, *, strict: bool = False) -> dict[str, object]:
    """Connect only; this never starts, resumes, or materializes an app server."""
    started_monotonic = time.monotonic()
    started_at = time.time()
    try:
        with RpcClient(endpoint, timeout=SAMPLE_MAX_AGE) as rpc:
            response = rpc.request('thread/read', {'threadId': thread_id, 'includeTurns': False})
            thread = response.get('thread') if isinstance(response, dict) else None
            if not isinstance(thread, dict):
                raise CoordError('native passive reader returned an invalid thread')
            if strict and thread.get('id') != thread_id:
                raise CoordError('native passive reader returned a different thread')
            status = thread.get('status')
            state = status.get('type') if isinstance(status, dict) else None
            if state not in ('idle', 'notLoaded'):
                raise CoordError('native thread is not idle or notLoaded')
            turns = thread.get('turns', [])
            if strict and (not isinstance(turns, list) or any(not isinstance(turn, dict) for turn in turns)):
                raise CoordError('native active-turn state is unavailable')
            if isinstance(turns, list) and any(isinstance(turn, dict) and turn.get('status') == 'inProgress' for turn in turns):
                raise CoordError('native thread has an active turn')
            queue = rpc.request('thread/queue/list', {'threadId': thread_id, 'limit': 100})
            items = queue.get('data') if isinstance(queue, dict) else None
            if not isinstance(items, list) or queue.get('nextCursor'):
                raise CoordError('native queue is unavailable or exceeds the passive sample bound')
            if items:
                raise CoordError('native queue is not empty')
            elapsed = time.monotonic() - started_monotonic
            if elapsed > SAMPLE_MAX_AGE:
                raise CoordError('native passive sample exceeded the maximum capture interval')
            return {'thread_id': thread_id, 'state': state, 'turn_id': None,
                    'queue_count': 0, 'direct_input_available': thread.get('canAcceptDirectInput') is True,
                    # Freshness starts at the oldest participating component,
                    # not after the final queue response.
                    'started_at': started_at, 'captured_at': time.time(), 'elapsed_ms': int(elapsed * 1000)}
    except (RpcError, TransportError, OSError, ValueError) as error:
        raise CoordError('passive native sample is unavailable') from error


_AUDIT_CORE = {'request_id', 'payload_json', 'outcome', 'checks_json',
               'process_evidence_state', 'recorded_at'}
_AUDIT_TUPLE = {'project', 'resource', 'reservation_id', 'owner_session',
                'expected_state', 'requester_session', 'requester_host', 'reason'}
_AUDIT_OPTIONAL = {'sample_json', 'continuation_of'}


def _audit_columns(db: sqlite3.Connection) -> set[str]:
    """Recognize only the two application-created layouts and their additions."""
    rows = list(db.execute('PRAGMA table_info(admin_ownership_dispositions)'))
    columns = {row['name'] for row in rows}
    compact = _AUDIT_CORE | _AUDIT_OPTIONAL
    legacy = _AUDIT_CORE | _AUDIT_TUPLE
    if not (columns == compact or legacy.issubset(columns) and
            columns.issubset(legacy | _AUDIT_OPTIONAL)):
        raise CoordError('administrative disposition audit schema is incompatible')
    tuple_nullability = {row['notnull'] for row in rows if row['name'] in _AUDIT_TUPLE}
    if len(tuple_nullability) > 1:
        raise CoordError('administrative disposition audit schema is incompatible')
    for row in rows:
        expected_type = 'REAL' if row['name'] == 'recorded_at' else 'TEXT'
        expected_pk = 1 if row['name'] == 'request_id' else 0
        if row['type'].upper() != expected_type or row['pk'] != expected_pk or row['dflt_value'] is not None:
            raise CoordError('administrative disposition audit schema is incompatible')
        if row['name'] in _AUDIT_CORE - {'request_id'} and row['notnull'] != 1:
            raise CoordError('administrative disposition audit schema is incompatible')
        if row['name'] in _AUDIT_OPTIONAL and row['notnull']:
            # The sampler-created compact layout requires sample_json. Its
            # additive tuple columns remain nullable; legacy tuple layouts do
            # not acquire this otherwise unfamiliar required constraint.
            if not (row['name'] == 'sample_json' and row['notnull'] == 1
                    and tuple_nullability.issubset({0})):
                raise CoordError('administrative disposition audit schema is incompatible')
    # An adapted compact layout has all tuple columns nullable.  A legacy
    # layout has all eight required.  A partial/foreign hybrid is not accepted.
    return columns


def _ensure_audit_schema(db: sqlite3.Connection) -> None:
    """Create or additively adapt the durable local audit inside this transaction.

    The installed table predates passive sampling.  Its immutable records are
    evidence of the historic administrative result, not a source for inferred
    native samples or requester facts.  New columns therefore remain nullable
    for those rows, while new dispositions record the complete legacy tuple.
    Individual DDL statements (not ``executescript``) keep a caller's SQLite
    transaction rollback-safe.
    """
    db.execute('''CREATE TABLE IF NOT EXISTS admin_ownership_dispositions (
        request_id TEXT PRIMARY KEY,
        project TEXT NOT NULL,
        resource TEXT NOT NULL,
        reservation_id TEXT NOT NULL,
        owner_session TEXT NOT NULL,
        expected_state TEXT NOT NULL,
        requester_session TEXT NOT NULL,
        requester_host TEXT NOT NULL,
        reason TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        outcome TEXT NOT NULL,
        checks_json TEXT NOT NULL,
        sample_json TEXT,
        process_evidence_state TEXT NOT NULL,
        recorded_at REAL NOT NULL,
        continuation_of TEXT)''')
    columns = _audit_columns(db)
    # Compact historic records never acquire invented tuple/requester facts,
    # even if similar values happen to appear in their immutable payload.
    for name in sorted(_AUDIT_TUPLE - columns):
        db.execute(f'ALTER TABLE admin_ownership_dispositions ADD COLUMN {name} TEXT')
    # Old records intentionally retain SQL NULL rather than a synthetic sample.
    if 'sample_json' not in columns:
        db.execute('ALTER TABLE admin_ownership_dispositions ADD COLUMN sample_json TEXT')
    if 'continuation_of' not in columns:
        db.execute('ALTER TABLE admin_ownership_dispositions ADD COLUMN continuation_of TEXT')
    db.execute('''CREATE TABLE IF NOT EXISTS admin_ownership_disposition_history (
        request_id TEXT NOT NULL, event TEXT NOT NULL, details_json TEXT NOT NULL,
        recorded_at REAL NOT NULL, PRIMARY KEY(request_id,event))''')


def _recorded_result(row: sqlite3.Row, request_id: str) -> dict[str, object]:
    """Return durable evidence without inventing fields absent from legacy rows."""
    sample = row['sample_json'] if 'sample_json' in row.keys() else None
    try:
        checks = json.loads(row['checks_json'])
        parsed_sample = json.loads(sample) if sample is not None else None
    except (TypeError, ValueError) as error:
        raise CoordError('administrative disposition audit record is malformed') from error
    result = {'request_id': request_id, 'outcome': row['outcome'], 'checks': checks,
            'sample': parsed_sample, 'process_evidence': row['process_evidence_state'],
            'replayed': True}
    try:
        payload = json.loads(row['payload_json'])
    except (TypeError, ValueError) as error:
        raise CoordError('administrative disposition audit record is malformed') from error
    if not isinstance(payload, dict):
        raise CoordError('administrative disposition audit record is malformed')
    if 'historical_blocked_of' in payload:
        result['historical_blocked_of'] = payload['historical_blocked_of']
    return result


def _existing_outcome(path: Path, request_id: str, payload_json: str):
    with _read_existing(path) as db:
        if not _has_table(db, 'admin_ownership_dispositions'):
            return None
        _audit_columns(db)
        row = db.execute('SELECT * FROM admin_ownership_dispositions WHERE request_id=?', (request_id,)).fetchone()
        if row is None:
            return None
        if row['payload_json'] != payload_json:
            raise CoordError('administrative disposition retry payload differs from the original')
        return _recorded_result(row, request_id)


def _continuation_contract(db: sqlite3.Connection, payload: dict[str, str]) -> None:
    original_id = payload.get('continuation_of')
    if original_id is None:
        return
    original = db.execute('SELECT * FROM admin_ownership_dispositions WHERE request_id=?', (original_id,)).fetchone()
    if original is None or original['outcome'] != 'blocked':
        raise CoordError('continuation requires the exact original blocked disposition')
    if (original['requester_session'] != payload['requester_session'] or
            original['requester_host'] != payload['requester_host'] or
            original['sample_json'] is None):
        raise CoordError('historic blocked disposition lacks continuation provenance')
    try:
        original_payload = json.loads(original['payload_json'])
        original_checks = json.loads(original['checks_json'])
    except (TypeError, ValueError) as error:
        raise CoordError('original blocked disposition is malformed') from error
    if original['continuation_of'] is not None:
        raise CoordError('continuation chains are not supported')
    expected = {key: value for key, value in payload.items() if key not in ('request_id', 'continuation_of')}
    historical = {key: value for key, value in original_payload.items() if key not in ('request_id', 'continuation_of')}
    if expected != historical or original_checks.get('current_state') != payload['expected_state']:
        raise CoordError('continuation payload or original blocked state differs')


def _historical_blocked_contract(db: sqlite3.Connection, payload: dict[str, str]) -> None:
    """A historical link describes the target; it grants no current admission."""
    original_id = payload.get('historical_blocked_of')
    if original_id is None:
        return
    if not _has_table(db, 'admin_ownership_dispositions'):
        raise CoordError('historical linkage requires the exact blocked disposition')
    _audit_columns(db)
    original = db.execute('SELECT * FROM admin_ownership_dispositions WHERE request_id=?', (original_id,)).fetchone()
    if original is None or original['outcome'] != 'blocked':
        raise CoordError('historical linkage requires the exact blocked disposition')
    try:
        historical = json.loads(original['payload_json'])
    except (TypeError, ValueError) as error:
        raise CoordError('historical blocked payload is malformed') from error
    if not isinstance(historical, dict) or historical.get('request_id') != original_id:
        raise CoordError('historical blocked payload is incomplete or conflicting')
    for name in ('project', 'resource', 'owner_session', 'reservation_id', 'expected_state'):
        if not isinstance(historical.get(name), str) or historical[name] != payload[name]:
            raise CoordError('historical blocked target tuple is incomplete or differs')
        # The compact layout lacks these columns.  An actual stored legacy value
        # must agree; absent historic columns remain unknown without backfill.
        if name in original.keys() and original[name] is not None and original[name] != historical[name]:
            raise CoordError('historical blocked target tuple is conflicting')


def _checks(db: sqlite3.Connection, payload: dict[str, str], sample: dict[str, object]) -> dict[str, object]:
    resource = db.execute('SELECT owner_session,reservation_id,stale FROM resources WHERE name=?', (payload['resource'],)).fetchone()
    blockers: list[str] = []
    exact = bool(resource and resource['owner_session'] == payload['owner_session'] and resource['reservation_id'] == payload['reservation_id'])
    if not exact:
        blockers.append('exact reservation is absent or changed')
    elif not resource['stale']:
        blockers.append('reservation state changed')
    binding = db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?', (payload['owner_session'],)).fetchone()
    host = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (payload['owner_session'],)).fetchone()
    if not binding or binding['thread_id'] != sample['thread_id']:
        blockers.append('exact owner binding changed')
    if not host or host['host_id'] != payload['requester_host']:
        blockers.append('exact owner host binding changed')
    if time.time() - float(sample['started_at']) > SAMPLE_MAX_AGE:
        blockers.append('native passive sample expired')
    guards = []
    for table, pid in (('guarded_runs', 'pid'), ('remote_guarded_runs', 'local_pid')):
        if _has_table(db, table):
            guards.extend(str(row[0]) for row in db.execute(f'SELECT run_id FROM {table} WHERE reservation_id=? AND closed_at IS NULL', (payload['reservation_id'],)))
    if guards:
        blockers.append('exact reservation has an open guard')
    app_work = []
    for row in db.execute("SELECT id,status FROM bridge_jobs WHERE thread_id=? AND status NOT IN ('dispatched','cancelled')", (sample['thread_id'],)):
        app_work.append('bridge job ' + row['id'] + ':' + row['status'])
    if _has_table(db, 'agent_control_requests') and db.execute("SELECT 1 FROM agent_control_requests WHERE target_session=? AND status NOT IN ('completed','failed','cancelled','resolved_unchanged')", (payload['owner_session'],)).fetchone():
        app_work.append('agent control request')
    if _has_table(db, 'session_resets') and db.execute("SELECT 1 FROM session_resets WHERE target_session=? AND status NOT IN ('completed','failed','cancelled')", (payload['owner_session'],)).fetchone():
        app_work.append('session reset')
    if _has_table(db, 'messages'):
        if (not _has_table(db, 'message_reads') or
                'provenance' not in {row['name'] for row in db.execute('PRAGMA table_info(message_reads)')}):
            raise CoordError('recipient consumption provenance is unavailable')
        deliveries = db.execute('''SELECT m.id,m.acked_at,
            EXISTS(SELECT 1 FROM message_reads r WHERE r.message_id=m.id
                AND r.reader_session=m.recipient_session
                AND r.provenance IN ('recipient_full_message','recipient_context_full')) AS consumed
            FROM messages m WHERE m.recipient_session=? AND m.sender_session<>?''',
                                (payload['owner_session'], payload['owner_session'])).fetchall()
        # A proven full recipient read is consumption.  Legacy/unknown reads
        # and acknowledgements do not supply that evidence. It is sufficient even
        # when an ordinary acknowledged reply is still outstanding: that reply
        # is coordination bookkeeping, not native work.  Conversely an ack bit
        # without the supported recipient-read evidence never proves delivery.
        for row in deliveries:
            if row['consumed']:
                continue
            app_work.append(('acknowledged-but-unconsumed input ' if row['acked_at'] is not None
                             else 'unconsumed input ') + row['id'])
    if app_work:
        blockers.append('known application work is pending')
    queue_count = db.execute('SELECT count(*) FROM resource_queue WHERE resource=?', (payload['resource'],)).fetchone()[0]
    return {'exact': exact, 'current_state': 'stale' if resource and resource['stale'] else 'changed',
            'blockers': blockers, 'open_guards': guards, 'known_app_work': app_work,
            'queue_count': queue_count, 'queue_preserved': True,
            'direct_input_metadata': sample['direct_input_available'],
            'external_native_residual': 'UNFENCED_AFTER_CAPTURE'}


def _recheck_root_barriers(registry: sqlite3.Connection) -> None:
    """Run under the registry lock, immediately before target mutation."""
    _require(registry, 'root_config', 'root_fresh_requests', 'root_team_operations')
    root = registry.execute('SELECT mode FROM root_config WHERE singleton=1').fetchone()
    if not root or root['mode'] != 'running':
        raise CoordError('configured Root is unavailable for administrative disposition')
    if registry.execute("SELECT 1 FROM root_fresh_requests WHERE status<>'completed'").fetchone():
        raise CoordError('configured Root has a pending fresh-start')
    if registry.execute('SELECT 1 FROM root_team_operations WHERE resumed_at IS NULL').fetchone():
        raise CoordError('configured Root stop barrier is active')


def _recheck_registered_target(root: ConfiguredRoot, registry: sqlite3.Connection,
                               project: str, expected_path: Path) -> None:
    """Keep registry mapping and selected target bound under lock."""
    if not registry.execute('SELECT 1 FROM projects WHERE id=?', (project,)).fetchone():
        raise CoordError('target project is absent from the configured registry')
    current = root.master if project == 'default' else root.master.parent / (root.master.name + '.projects') / project / 'state.sqlite3'
    if current != expected_path or not current.is_file():
        raise CoordError('registered target path changed before administrative disposition')


def apply_local(*, master_db, service_plist, project, resource, reservation_id, owner_session,
                expected_state, request_id, reason, continuation_request_id=None,
                historical_blocked_request_id=None):
    root = load_configured_root(master_db, service_plist)
    path = target_path(root, project)
    payload = _payload(root=root, project=project, resource=resource, reservation_id=reservation_id,
                       owner_session=owner_session, expected_state=expected_state, request_id=request_id, reason=reason,
                       continuation_request_id=continuation_request_id,
                       historical_blocked_request_id=historical_blocked_request_id)
    payload_json = _json(payload)
    previous = _existing_outcome(path, payload['request_id'], payload_json)
    if previous:
        return previous
    # Capture happens only after authority and exact target metadata gates.
    with _read_existing(path) as db:
        # Raw physical names are exact registered literals, not aliases for
        # file: resources or a general new namespace. Durable replay above is
        # payload-bound and remains independent of a later resource change.
        if payload['resource'].startswith('/') and not db.execute(
                'SELECT 1 FROM resources WHERE name=?', (payload['resource'],)).fetchone():
            raise CoordError('raw physical resource requires the exact registered literal')
        _historical_blocked_contract(db, payload)
        binding = db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?', (payload['owner_session'],)).fetchone()
        host = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (payload['owner_session'],)).fetchone()
        if not binding or not host or host['host_id'] != root.host_id:
            raise CoordError('exact owner binding or configured local host is unavailable')
        thread_id = binding['thread_id']
    sample = _sample(thread_id, root.endpoint)
    # This is the only mutation boundary.  No Coordinator is constructed: no
    # schema/WAL initialization can precede the above gates or passive capture.
    # The canonical registry always locks before the target/binding/host store.
    # There is no cross-file SQLite atomic commit; the registry is read-only for
    # this operation and is held solely to make its cooperative barriers stable.
    db = None
    registry_db = None
    try:
        registry_db = sqlite3.connect(root.registry, timeout=5, isolation_level=None)
        registry_db.row_factory = sqlite3.Row
        registry_db.execute('BEGIN IMMEDIATE')
        _recheck_root_barriers(registry_db)
        _recheck_registered_target(root, registry_db, payload['project'], path)
        db = sqlite3.connect(path, timeout=5, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute('BEGIN IMMEDIATE')
        _ensure_audit_schema(db)
        prior = db.execute('SELECT * FROM admin_ownership_dispositions WHERE request_id=?', (payload['request_id'],)).fetchone()
        if prior:
            if prior['payload_json'] != payload_json:
                raise CoordError('administrative disposition retry payload differs from the original')
            db.rollback(); registry_db.rollback()
            return _recorded_result(prior, payload['request_id'])
        _continuation_contract(db, payload)
        _historical_blocked_contract(db, payload)
        # Match normal coordinator admission: expiry becomes durable only under
        # the locked transaction, after all authority/native preconditions.
        db.execute('UPDATE resources SET stale=1 WHERE owner_session IS NOT NULL AND deadline<?', (time.time(),))
        checks = _checks(db, payload, sample)
        outcome = 'blocked' if checks['blockers'] else 'administratively_abandoned'
        now = time.time()
        db.execute('''INSERT INTO admin_ownership_dispositions
            (request_id,project,resource,reservation_id,owner_session,expected_state,
             requester_session,requester_host,reason,payload_json,outcome,checks_json,
             sample_json,process_evidence_state,recorded_at,continuation_of)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                   (payload['request_id'], payload['project'], payload['resource'],
                    payload['reservation_id'], payload['owner_session'], payload['expected_state'],
                    payload['requester_session'], payload['requester_host'], payload['reason'],
                    payload_json, outcome, _json(checks), _json(sample), 'UNVERIFIED', now,
                    payload.get('continuation_of')))
        db.execute('INSERT INTO admin_ownership_disposition_history VALUES(?,?,?,?)',
                   (payload['request_id'], 'sample-consumed', _json({'outcome': outcome, 'process_evidence': 'UNVERIFIED'}), now))
        if payload.get('continuation_of'):
            db.execute('INSERT INTO admin_ownership_disposition_history VALUES(?,?,?,?)',
                       (payload['request_id'], 'continued-from', _json({'original_request_id': payload['continuation_of'],
                                                                         'expected_outcome': 'blocked',
                                                                         'expected_state': payload['expected_state']}), now))
        if payload.get('historical_blocked_of'):
            db.execute('INSERT INTO admin_ownership_disposition_history VALUES(?,?,?,?)',
                       (payload['request_id'], 'historical-blocked-link',
                        _json({'original_request_id': payload['historical_blocked_of'],
                               'link_scope': 'HISTORY_ONLY', 'original_outcome': 'blocked'}), now))
        if outcome == 'administratively_abandoned':
            changed = db.execute('''UPDATE resources SET owner_session=NULL,reservation_id=NULL,token=NULL,
                 granted_at=NULL,deadline=NULL,stale=0,reason=?
                 WHERE name=? AND reservation_id=? AND owner_session=? AND stale=1''',
                 ('administratively abandoned; process closure UNVERIFIED', payload['resource'], payload['reservation_id'], payload['owner_session'])).rowcount
            if changed != 1:
                raise CoordError('exact stale reservation changed before administrative disposition')
            db.execute('INSERT INTO admin_ownership_disposition_history VALUES(?,?,?,?)',
                       (payload['request_id'], 'abandoned', _json({'queue_preserved': True, 'process_evidence': 'UNVERIFIED'}), now))
        db.commit()
        registry_db.commit()
        result = {'request_id': payload['request_id'], 'continuation_of': payload.get('continuation_of'),
                'outcome': outcome, 'checks': checks, 'sample': sample,
                'process_evidence': 'UNVERIFIED', 'replayed': False}
        if payload.get('historical_blocked_of'):
            result['historical_blocked_of'] = payload['historical_blocked_of']
        return result
    except BaseException:
        if db is not None:
            db.rollback()
        if registry_db is not None:
            registry_db.rollback()
        raise
    finally:
        if db is not None:
            db.close()
        if registry_db is not None:
            registry_db.close()


def _deadline(value: object) -> float:
    if isinstance(value, bool):
        raise CoordError('expected deadline must be a finite timestamp')
    try:
        timestamp = float(value)
    except (TypeError, ValueError, OverflowError) as error:
        raise CoordError('expected deadline must be a finite timestamp') from error
    if not math.isfinite(timestamp) or timestamp <= 0:
        raise CoordError('expected deadline must be a finite timestamp')
    return timestamp


def _legacy_selection(root, *, project, resource, reservation_id, owner_session,
                      expected_state, request_id, reason, guard_run_id, expected_deadline,
                      additional_obligations, historical_blocked_request_id):
    # This is a separate privileged selection, NOT the ordinary resource allowlist.
    # Every literal still has to exist with this exact stale tuple under lock.
    payload = _payload(root=root, project=project, resource=resource,
                       reservation_id=reservation_id, owner_session=owner_session,
                       expected_state=expected_state, request_id=request_id, reason=reason,
                       historical_blocked_request_id=historical_blocked_request_id,
                       legacy_selection=True)
    payload.update(kind='legacy-stale-guard-administrative-selection',
                   resource=_clean(resource, 'registered literal resource', 1024),
                   guard_run_id=_clean(guard_run_id, 'guard run', 512),
                   deadline=_deadline(expected_deadline), acting_root_project=root.home_project)
    if not isinstance(additional_obligations, list) or len(additional_obligations) > 31:
        raise CoordError('select at most 31 exact additional stale obligations')
    members = []
    names = {payload['resource']}
    for item in additional_obligations:
        if not isinstance(item, dict) or set(item) != {'resource', 'reservation_id', 'owner_session', 'deadline'}:
            raise CoordError('additional obligation requires an exact stored tuple')
        member = {name: _clean(item[name], name, 1024 if name == 'resource' else 512)
                  for name in ('resource', 'reservation_id', 'owner_session')}
        member['deadline'] = _deadline(item['deadline'])
        if member['owner_session'] != payload['owner_session'] or member['resource'] in names:
            raise CoordError('additional obligation is foreign or duplicated')
        names.add(member['resource'])
        members.append(member)
    payload['additional_obligations'] = members
    return payload


def _legacy_records(db, root, payload):
    """Authenticate stored records, without inferring any historical relation."""
    owner = payload['owner_session']
    if owner == root.home_session or not db.execute('SELECT 1 FROM sessions WHERE id=?', (owner,)).fetchone():
        raise CoordError('exact registered project owner is unavailable')
    if _has_table(db, 'retired_sessions') and db.execute('SELECT 1 FROM retired_sessions WHERE id=?', (owner,)).fetchone():
        raise CoordError('exact owner is retired')
    binding = db.execute('SELECT * FROM bridge_bindings WHERE session_id=?', (owner,)).fetchone()
    host = db.execute('SELECT * FROM remote_session_hosts WHERE session_id=?', (owner,)).fetchone()
    if not binding or not host or host['host_id'] != root.host_id:
        raise CoordError('current registered owner thread or permitted host is unavailable')
    _clean(binding['thread_id'], 'registered thread', 512)
    if (type(host['registered_at']) not in (int, float) or not math.isfinite(host['registered_at']) or host['registered_at'] > time.time()
            or host['registered_at'] < 0):
        raise CoordError('current host registration is unsupported')
    if _has_table(db, 'agent_controls') and db.execute("SELECT 1 FROM agent_controls WHERE target_session=? AND mode<>'running'", (owner,)).fetchone():
        raise CoordError('owner control state is not running')
    if _has_table(db, 'root_team_stops') and db.execute('SELECT 1 FROM root_team_stops WHERE active=1').fetchone():
        raise CoordError('owner stop barrier is active')
    if _has_table(db, 'root_team_reset_targets') and db.execute('SELECT 1 FROM root_team_reset_targets WHERE session_id=? OR thread_id=?', (owner, binding['thread_id'])).fetchone():
        raise CoordError('owner is quarantined')
    if _has_table(db, 'root_team_reset_requests') and db.execute("SELECT 1 FROM root_team_reset_requests WHERE status IN ('quiescing','quarantined')").fetchone():
        raise CoordError('team reset is unresolved')
    selected = [dict(resource=payload['resource'], reservation_id=payload['reservation_id'],
                     owner_session=owner, deadline=payload['deadline'])] + payload['additional_obligations']
    members, runs, anchor, private_tokens = [], [], [], []
    for expected in selected:
        row = db.execute('SELECT * FROM resources WHERE name=?', (expected['resource'],)).fetchone()
        if (not row or row['owner_session'] != owner or row['reservation_id'] != expected['reservation_id']
                or row['deadline'] != expected['deadline'] or row['stale'] != 1
                or row['deadline'] >= time.time()):
            raise CoordError('exact selected stale obligation is missing, live or changed')
        if db.execute('SELECT 1 FROM resource_queue WHERE resource=?', (expected['resource'],)).fetchone():
            raise CoordError('selected obligation has a FIFO claimant')
        # Tokens never enter audit/output. CAS below invalidates them in place.
        members.append({key: row[key] for key in row.keys() if key != 'token'})
        private_tokens.append(row['token'])
        for table, session_field in (('guarded_runs', 'session'), ('remote_guarded_runs', 'session_id')):
            if not _has_table(db, table):
                continue
            for stored in db.execute(f'SELECT * FROM {table} WHERE resource=? OR reservation_id=? OR run_id=? ORDER BY run_id',
                                     (expected['resource'], expected['reservation_id'], payload['guard_run_id'])):
                run = dict(stored)
                if (run['resource'] != expected['resource'] or run['reservation_id'] != expected['reservation_id']):
                    # The anchor belongs only to its own member, not every extra.
                    if run['run_id'] == payload['guard_run_id'] and expected is not selected[0]:
                        continue
                    if run['closed_at'] is None or run['reservation_id'] == expected['reservation_id']:
                        raise CoordError('guard association is foreign or inconsistent')
                    continue
                if run[session_field] != owner or ('host_id' in run and run['host_id'] != root.host_id):
                    raise CoordError('guard owner or recorded host is conflicting')
                pid = run['pid'] if table == 'guarded_runs' else run['local_pid']
                if (type(pid) is not int or pid < 0 or not isinstance(run['started_at'], (int, float))
                        or not math.isfinite(run['started_at']) or run['started_at'] <= 0):
                    raise CoordError('stored guard identity is incomplete')
                record = dict(table=table, **run)
                runs.append(record)
                if run['run_id'] == payload['guard_run_id']:
                    anchor.append(record)
    if len(anchor) != 1 or anchor[0]['closed_at'] is not None:
        raise CoordError('one exact OPEN guard is required')
    historical_batch = 'UNKNOWN'
    if anchor[0]['table'] == 'remote_guarded_runs' and _has_table(db, 'remote_guard_batches'):
        batch = db.execute('SELECT batch_id FROM remote_guard_batches WHERE run_id=?', (payload['guard_run_id'],)).fetchone()
        if batch:
            historical_batch = batch['batch_id']
    return dict(binding=dict(binding), host={key: host[key] for key in host.keys() if key != 'last_seen_at'},
                members=members, guard=anchor[0], associated_runs=runs,
                historical_batch=historical_batch, private_tokens=private_tokens)


def _legacy_admission(db, payload, records, sample):
    now = time.time()
    if (sample.get('thread_id') != records['binding']['thread_id']
            or sample.get('state') not in ('idle', 'notLoaded') or sample.get('turn_id') is not None
            or type(sample.get('queue_count')) is not int or sample['queue_count'] != 0):
        raise CoordError('exact passive idle and queue state is unavailable')
    try:
        started, captured = float(sample['started_at']), float(sample['captured_at'])
        valid = (math.isfinite(started) and math.isfinite(captured)
                 and 0 <= now - started <= SAMPLE_MAX_AGE and started <= captured <= now
                 and type(sample['elapsed_ms']) is int and 0 <= sample['elapsed_ms'] <= SAMPLE_MAX_AGE * 1000)
    except (KeyError, TypeError, ValueError, OverflowError):
        valid = False
    if not valid:
        raise CoordError('exact passive sample is expired or uncertain')
    checks = _checks(db, payload, sample)
    # OPEN is deliberately retained ONLY after _legacy_records has authenticated
    # every selected stored guard. This exception never reaches ordinary recovery.
    blockers = [item for item in checks['blockers'] if item != 'exact reservation has an open guard']
    if blockers:
        raise CoordError('; '.join(blockers))
    checks.update(blockers=[], open_guard_retained=True, selected_queue_count=0,
                  process='UNVERIFIED', restoration='UNVERIFIED', detached_helper_coverage='UNVERIFIED',
                  external_native_residual='UNVERIFIED', execution_grant=False, ordinary_free=False)
    return checks


def apply_legacy_local(*, master_db, service_plist, project, resource, reservation_id, owner_session,
                       expected_state, request_id, reason, guard_run_id, expected_deadline,
                       additional_obligations=None, historical_blocked_request_id=None):
    """New privileged selection; invalidate stale tokens, never clear ownership."""
    root = load_configured_root(master_db, service_plist)
    path = target_path(root, project)
    payload = _legacy_selection(root, project=project, resource=resource, reservation_id=reservation_id,
        owner_session=owner_session, expected_state=expected_state, request_id=request_id, reason=reason,
        guard_run_id=guard_run_id, expected_deadline=expected_deadline,
        additional_obligations=[] if additional_obligations is None else additional_obligations,
        historical_blocked_request_id=historical_blocked_request_id)
    payload_json = _json(payload)
    previous = _existing_outcome(path, payload['request_id'], payload_json)
    if previous:
        return previous
    with _read_existing(path) as db:
        _historical_blocked_contract(db, payload)
        before = _legacy_records(db, root, payload)
    sample = _sample(before['binding']['thread_id'], root.endpoint, strict=True)
    db = registry = None
    try:
        registry = sqlite3.connect(root.registry, timeout=5, isolation_level=None)
        registry.row_factory = sqlite3.Row
        registry.execute('BEGIN IMMEDIATE')
        _recheck_root_barriers(registry)
        current_root = registry.execute('SELECT home_project,home_session,host_id FROM root_config WHERE singleton=1').fetchone()
        if tuple(current_root) != (root.home_project, root.home_session, root.host_id):
            raise CoordError('configured acting Root identity changed')
        _recheck_registered_target(root, registry, project, path)
        db = sqlite3.connect(path, timeout=5, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute('BEGIN IMMEDIATE')
        if _has_table(db, 'admin_ownership_dispositions'):
            columns = _audit_columns(db)
            prior = db.execute('SELECT * FROM admin_ownership_dispositions WHERE request_id=?', (payload['request_id'],)).fetchone()
            if prior:
                if prior['payload_json'] != payload_json:
                    raise CoordError('administrative disposition retry payload differs from the original')
                db.rollback(); registry.rollback()
                return _recorded_result(prior, payload['request_id'])
            if {'resource', 'reservation_id', 'owner_session'} <= columns:
                if db.execute("SELECT 1 FROM admin_ownership_dispositions WHERE resource=? AND reservation_id=? AND owner_session=? AND outcome='legacy_stale_authority_invalidated'", (resource, reservation_id, owner_session)).fetchone():
                    raise CoordError('exact guard selection already has a durable administrative result; replay its UUID')
            elif db.execute("SELECT 1 FROM admin_ownership_dispositions WHERE outcome='legacy_stale_authority_invalidated'").fetchone():
                raise CoordError('prior legacy selection lacks exact tuple provenance')
        _historical_blocked_contract(db, payload)
        records = _legacy_records(db, root, payload)
        if records != before:
            raise CoordError('selected guard, obligations or current registration changed after capture')
        checks = _legacy_admission(db, payload, records, sample)
        guard = records['guard']
        checks['intent'] = dict(kind=payload['kind'], request=payload,
            association_provenance='NEW_ROOT_ADMINISTRATIVE_SELECTION',
            additional_basis='ROOT_SELECTED_ADDITIONAL_OBLIGATIONS',
            current_host_basis='CURRENT_REGISTERED_HOST', current_binding=records['binding'],
            current_host=records['host'], guard=guard, members=records['members'],
            associated_runs=records['associated_runs'], validation_time=time.time(),
            validation_basis='LOCKED_EXACT_STALE_CAS_AND_FRESH_PASSIVE_FULL_CONSUMPTION',
            historical_host=guard.get('host_id', 'UNKNOWN'), historical_thread='UNKNOWN',
            historical_batch=records['historical_batch'])
        # All refusal gates precede DDL, audit, or token writes. Target transaction
        # makes the typed intent, linked audit and exact invalidations atomic.
        _ensure_audit_schema(db)
        now = time.time()
        for member in records['members']:
            changed = db.execute('''UPDATE resources SET token=NULL WHERE name=? AND reservation_id=?
                AND owner_session=? AND deadline=? AND stale=1''',
                (member['name'], member['reservation_id'], member['owner_session'], member['deadline'])).rowcount
            if changed != 1:
                raise CoordError('exact stale selection changed before invalidation')
        outcome = 'legacy_stale_authority_invalidated'
        db.execute('''INSERT INTO admin_ownership_dispositions
            (request_id,project,resource,reservation_id,owner_session,expected_state,requester_session,
             requester_host,reason,payload_json,outcome,checks_json,sample_json,process_evidence_state,recorded_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
            (payload['request_id'], project, resource, reservation_id, owner_session, 'stale',
             root.home_session, root.host_id, reason, payload_json, outcome, _json(checks), _json(sample), 'UNVERIFIED', now))
        db.execute('INSERT INTO admin_ownership_disposition_history VALUES(?,?,?,?)',
                   (payload['request_id'], 'legacy-stale-selection', _json(checks['intent']), now))
        if payload.get('historical_blocked_of'):
            db.execute('INSERT INTO admin_ownership_disposition_history VALUES(?,?,?,?)',
                       (payload['request_id'], 'historical-blocked-link', _json(dict(
                           original_request_id=payload['historical_blocked_of'], link_scope='HISTORY_ONLY')), now))
        db.commit(); registry.commit()
        return dict(request_id=payload['request_id'], outcome=outcome, checks=checks, sample=sample,
                    process_evidence='UNVERIFIED', replayed=False)
    except BaseException:
        if db is not None:
            db.rollback()
        if registry is not None:
            registry.rollback()
        raise
    finally:
        if db is not None:
            db.close()
        if registry is not None:
            registry.close()


def main(argv=None):
    parser = argparse.ArgumentParser(prog='agent-chat-ownership-disposition')
    parser.add_argument('--master-db', required=True, help='asserts the configured local server master path')
    parser.add_argument('--service-plist', required=True, help='asserts the installed local server plist')
    parser.add_argument('--project-id', dest='project', required=True)
    parser.add_argument('--resource', required=True)
    parser.add_argument('--reservation-id', dest='reservation_id', required=True)
    parser.add_argument('--owner-session', dest='owner_session', required=True)
    parser.add_argument('--expected-state', dest='expected_state', choices=('stale',), required=True)
    parser.add_argument('--request-id', required=True)
    parser.add_argument('--continue-request-id', dest='continuation_request_id')
    parser.add_argument('--historical-blocked-request-id', dest='historical_blocked_request_id',
                        help='history-only link for a distinct current privileged operation')
    parser.add_argument('--reason', required=True)
    parser.add_argument('--legacy-guard-run-id', dest='guard_run_id', help='separate stale-token-only administrative selection')
    parser.add_argument('--expected-deadline', help='exact stored guard reservation deadline')
    parser.add_argument('--additional-stale-obligation', action='append', nargs=4, default=[],
                        metavar=('RESOURCE', 'RID', 'OWNER', 'DEADLINE'))
    args = parser.parse_args(argv)
    values = vars(args)
    guard = values.pop('guard_run_id')
    deadline = values.pop('expected_deadline')
    additional = values.pop('additional_stale_obligation')
    if guard is not None:
        if deadline is None or values.pop('continuation_request_id') is not None:
            parser.error('legacy selection requires --expected-deadline; use only an explicit history link, not ordinary continuation')
        result = apply_legacy_local(**values, guard_run_id=guard, expected_deadline=deadline,
            additional_obligations=[dict(zip(('resource','reservation_id','owner_session','deadline'), item)) for item in additional])
    else:
        if deadline is not None or additional:
            parser.error('legacy tuple selectors require --legacy-guard-run-id')
        result = apply_local(**values)
    print(_json(result))
    return 0


if __name__ == '__main__':
    main()
