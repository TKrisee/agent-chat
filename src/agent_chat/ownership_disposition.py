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
import subprocess
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
    thread_id: str | None = None


def load_configured_root(master_db: str | Path, service_plist: str | Path, *,
                         project_scope: str | None = None,
                         allow_retired_intents: bool = False) -> ConfiguredRoot:
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
        row = db.execute('SELECT * FROM root_config WHERE singleton=1').fetchone()
        if not row or row['mode'] != 'running' or not all(isinstance(row[name], str) and row[name] for name in ('home_project', 'home_session', 'host_id')):
            raise CoordError('configured Root is unavailable for administrative disposition')
        root = ConfiguredRoot(master, registry, row['home_project'], row['home_session'],
                              row['host_id'], ENDPOINT,
                              row['thread_id'] if 'thread_id' in row.keys() else None)
        if allow_retired_intents:
            _check_administrative_fresh_history(db, root)
        elif db.execute("SELECT 1 FROM root_fresh_requests WHERE status<>'completed'").fetchone():
            raise CoordError('configured Root has a pending fresh-start')
        active = db.execute('SELECT 1 FROM root_team_operations WHERE resumed_at IS NULL LIMIT 2').fetchall()
        if active and project_scope is None:
            raise CoordError('configured Root stop barrier is active')
        if not db.execute('SELECT 1 FROM projects WHERE id=?', (row['home_project'],)).fetchone():
            raise CoordError('configured Root home project is absent from the registry')
    if project_scope is not None:
        path = target_path(root, project_scope)
        with _read_existing(registry) as db:
            _project_resumed_stop_context(root, db, project_scope, path)
    return root


def _check_administrative_fresh_history(registry, root: ConfiguredRoot) -> None:
    """Accept audited terminal intents only for local ownership disposition.

    This is rechecked under the registry writer lock before target mutation.
    It neither completes old requests nor attests historical native closure.
    Ordinary loaders and other lifecycle operations retain their strict gate.
    """
    from .root_pending_fresh import BOUND, _scope_evidence, _scopes

    current = registry.execute('SELECT * FROM root_config WHERE singleton=1').fetchone()
    if (not current or (current['home_project'], current['home_session'],
                        current['host_id'], current['mode']) !=
            (root.home_project, root.home_session, root.host_id, 'running') or
            (root.thread_id is not None and current['thread_id'] != root.thread_id)):
        raise CoordError('configured Root generation changed')
    pending = registry.execute(
        "SELECT * FROM root_fresh_requests WHERE status<>'completed' ORDER BY id LIMIT ?",
        (BOUND + 1,)).fetchall()
    if not pending:
        return
    if len(pending) > BOUND or not _wbt_retired_pending_intents_are_typed_admin_records(registry, current):
        raise CoordError('configured Root has a pending fresh-start: terminal audit is unavailable')
    scope_count = 0
    for request in pending:
        scopes = _scopes(registry, request['id'])
        scope_count += len(scopes)
        if (not scopes or scope_count > BOUND or
                len({item['project_id'] for item in scopes}) != len(scopes) or
                not any(item['project_id'] == root.home_project and
                        item['root_session'] == root.home_session for item in scopes)):
            raise CoordError('retired Root intent scope is incomplete')
        for scope in scopes:
            path = target_path(root, scope['project_id'])
            with _read_existing(path) as target:
                evidence = _scope_evidence(target, scope, request, current)
            gate = evidence['gate']
            if (evidence['evidence_state'] != 'OBSERVED' or
                    gate['active'] != 0 or gate['new_thread_id'] is not None):
                raise CoordError('retired Root intent gate or current binding changed')


def _wbt_retired_pending_intents_check(db, current) -> dict[str, object]:
    """Classify only the history already required by the WBT Root guard."""
    try:
        pending = db.execute("SELECT * FROM root_fresh_requests WHERE status<>'completed' ORDER BY id").fetchall()
    except sqlite3.Error:
        return {'ok': False, 'classification': 'unknown', 'request_id': None,
                'check_code': 'fresh_request_history_unreadable'}
    if not pending:
        return {'ok': True, 'classification': 'clear', 'request_id': None,
                'check_code': 'typed_history_clear'}

    def own_request_id(request):
        try:
            request_id = request['id']
            if str(uuid.UUID(request_id)) != request_id:
                return None
            if any(request[name] != current[name]
                   for name in ('home_project', 'home_session', 'host_id')):
                return None
            return request_id
        except (KeyError, IndexError, TypeError, ValueError, AttributeError):
            return None

    def failed(code, request=None, classification='unknown'):
        request_id = own_request_id(request) if request is not None else None
        return {'ok': False, 'classification': classification if request_id else 'unknown',
                'request_id': request_id, 'check_code': code}

    required_request = {
        'id', 'status', 'home_project', 'home_session', 'host_id', 'old_thread_id',
        'native_action', 'new_thread_id', 'queue_id', 'native_proof_json',
        'response_proof_json', 'owner_proof_json', 'error',
    }
    try:
        request_schema = list(db.execute('PRAGMA table_info(root_fresh_requests)'))
        request_columns = {row['name'] for row in request_schema}
    except sqlite3.Error:
        return failed('fresh_request_schema_unreadable', pending[0])
    if not any(row['name'] == 'id' and row['pk'] == 1 for row in request_schema):
        return failed('fresh_request_identifier_not_primary_key', pending[0])
    required_audit = {
        'cancellation_id', 'request_id', 'home_project', 'home_session', 'host_id',
        'expected_thread', 'candidate_thread_id', 'outcome', 'reason',
        'authorization_json', 'state', 'created_at', 'updated_at',
    }
    if not required_request <= request_columns:
        return failed('fresh_request_schema_incomplete', pending[0])
    try:
        if not _has_table(db, 'root_fresh_pending_cancellations'):
            return failed('cancellation_audit_table_missing', pending[0])
        audit_columns = {row['name'] for row in db.execute('PRAGMA table_info(root_fresh_pending_cancellations)')}
    except sqlite3.Error:
        return failed('cancellation_audit_schema_unreadable', pending[0])
    if not required_audit <= audit_columns:
        return failed('cancellation_audit_schema_incomplete', pending[0])
    request_id_unique = False
    try:
        for index in db.execute("PRAGMA index_list('root_fresh_pending_cancellations')"):
            if not index['unique']:
                continue
            index_name = str(index['name']).replace('"', '""')
            indexed_columns = [row['name'] for row in db.execute(
                'PRAGMA index_info("' + index_name + '")')]
            if indexed_columns == ['request_id']:
                request_id_unique = True
                break
    except sqlite3.Error:
        return failed('cancellation_request_index_unreadable', pending[0])
    if not request_id_unique:
        return failed('cancellation_request_id_not_unique', pending[0])
    try:
        triggers = {row['name']: (row['tbl_name'], _wbt_normalize_trigger_sql(row['sql']))
                    for row in db.execute(
            "SELECT name,tbl_name,sql FROM sqlite_master WHERE type='trigger' AND name IN (?,?)",
            ('immutable_root_fresh_pending_cancellation_identity',
             'immutable_root_fresh_pending_cancellation_delete'))}
    except sqlite3.Error:
        return failed('cancellation_immutability_triggers_unreadable', pending[0])
    identity_sql = triggers.get('immutable_root_fresh_pending_cancellation_identity', ('', ''))[1]
    delete_sql = triggers.get('immutable_root_fresh_pending_cancellation_delete', ('', ''))[1]
    expected_identity_sql = _wbt_normalize_trigger_sql('''CREATE TRIGGER immutable_root_fresh_pending_cancellation_identity
      BEFORE UPDATE ON root_fresh_pending_cancellations
      WHEN OLD.cancellation_id<>NEW.cancellation_id OR OLD.request_id<>NEW.request_id
        OR OLD.home_project<>NEW.home_project OR OLD.home_session<>NEW.home_session
        OR OLD.host_id<>NEW.host_id OR OLD.expected_thread<>NEW.expected_thread
        OR OLD.candidate_thread_id<>NEW.candidate_thread_id OR OLD.outcome<>NEW.outcome
        OR OLD.reason<>NEW.reason OR OLD.authorization_json<>NEW.authorization_json
        OR OLD.created_at<>NEW.created_at OR NOT (OLD.state='cancelling' AND NEW.state='cancelled')
      BEGIN SELECT RAISE(ABORT,'root fresh-start pending cancellation audit is immutable'); END''')
    expected_delete_sql = _wbt_normalize_trigger_sql('''CREATE TRIGGER immutable_root_fresh_pending_cancellation_delete
      BEFORE DELETE ON root_fresh_pending_cancellations
      BEGIN SELECT RAISE(ABORT,'root fresh-start pending cancellation audit is immutable'); END''')
    if (triggers.get('immutable_root_fresh_pending_cancellation_identity', ('',))[0]
            != 'root_fresh_pending_cancellations' or identity_sql != expected_identity_sql
            or triggers.get('immutable_root_fresh_pending_cancellation_delete', ('',))[0]
            != 'root_fresh_pending_cancellations' or delete_sql != expected_delete_sql):
        return failed('cancellation_immutability_triggers_not_exact', pending[0])
    if not all(key in current.keys() and current[key]
               for key in ('home_project', 'home_session', 'host_id', 'thread_id')):
        return failed('current_root_thread_unavailable')
    for request in pending:
        request_id = own_request_id(request)
        if request_id is None:
            return {'ok': False, 'classification': 'unknown', 'request_id': None,
                    'check_code': 'pending_request_not_bound_to_current_root'}
        candidate = request['new_thread_id']
        try:
            if str(uuid.UUID(request['id'])) != request['id'] or str(uuid.UUID(candidate)) != candidate:
                return failed('request_candidate_identifier_invalid', request, 'blocked')
        except (ValueError, TypeError, AttributeError):
            return failed('request_candidate_identifier_invalid', request, 'blocked')
        if request['status'] != 'retired' or request['native_action'] != 'creating':
            return failed('request_not_terminal_typed_cancellation', request, 'blocked')
        if (not isinstance(candidate, str) or not candidate or candidate == current['thread_id']
                or request['home_project'] != current['home_project']
                or request['home_session'] != current['home_session']
                or request['host_id'] != current['host_id']
                or request['old_thread_id'] != current['thread_id']):
            return failed('request_root_or_thread_binding_mismatch', request, 'blocked')
        if (request['queue_id'] is not None or request['native_proof_json'] is not None
                or request['response_proof_json'] is not None or request['owner_proof_json'] is not None):
            return failed('request_has_queue_or_native_proof', request, 'blocked')
        retirement_status = _wbt_retirement_status(db, request, current)
        if retirement_status == 'valid':
            continue
        if retirement_status == 'invalid':
            return failed('retirement_audit_not_typed_for_request', request, 'blocked')
        if retirement_status == 'unavailable':
            return failed('retirement_audit_unavailable', request)
        if request['error'] != ('Candidate recovery required: exact retained thread is not materialized: '
                                'RPC error -32600: no rollout found for thread id ' + candidate):
            return failed('request_not_exact_candidate_not_loaded_evidence', request, 'blocked')
        try:
            audits = db.execute('''SELECT cancellation_id,request_id,home_project,home_session,host_id,
                expected_thread,candidate_thread_id,outcome,reason,authorization_json,state
                FROM root_fresh_pending_cancellations WHERE request_id=? LIMIT 2''',
                (request_id,)).fetchall()
        except sqlite3.Error:
            return failed('cancellation_audit_unreadable', request)
        if len(audits) != 1:
            return failed('cancellation_audit_missing_or_ambiguous', request)
        audit = audits[0]
        try:
            if str(uuid.UUID(audit['cancellation_id'])) != audit['cancellation_id']:
                return failed('cancellation_identifier_invalid', request, 'blocked')
        except (ValueError, TypeError, AttributeError):
            return failed('cancellation_identifier_invalid', request, 'blocked')
        if (audit['request_id'] != request['id']
                or audit['home_project'] != current['home_project']
                or audit['home_session'] != current['home_session']
                or audit['host_id'] != current['host_id']
                or audit['expected_thread'] != current['thread_id']
                or audit['candidate_thread_id'] != candidate):
            return failed('cancellation_request_binding_mismatch', request, 'blocked')
        if audit['outcome'] != 'candidate_not_materialized_native_outcome_unknown':
            return failed('cancellation_native_outcome_not_unknown', request, 'blocked')
        if not _wbt_cancellation_reason_matches(audit['reason'], request_id):
            return failed('cancellation_reason_not_typed_for_request', request, 'blocked')
        authorization_status = _wbt_cancellation_authorization_status(
            audit['authorization_json'], request_id, candidate, current['thread_id'])
        if authorization_status == 'unavailable':
            return failed('cancellation_authorization_unavailable', request)
        if authorization_status != 'valid':
            return failed('cancellation_authorization_not_typed_for_request', request, 'blocked')
        if audit['state'] != 'cancelled':
            return failed('cancellation_not_terminal', request, 'blocked')
    return {'ok': True, 'classification': 'clear', 'request_id': None,
            'check_code': 'typed_history_clear'}


def _wbt_retired_pending_intents_are_typed_admin_records(db, current) -> bool:
    """Keep the WBT gate on the same strict typed administrative classifier."""
    return bool(_wbt_retired_pending_intents_check(db, current)['ok'])


def _wbt_normalize_trigger_sql(sql: object) -> str:
    if not isinstance(sql, str):
        return ''
    return ' '.join(sql.strip().rstrip(';').split()).casefold()


def _wbt_cancellation_reason_matches(reason: object, request_id: str) -> bool:
    prefix = 'Request '
    suffix = (' handoff intent cancelled after exact retained candidate not-loaded evidence. '
              'Candidate/history and native outcome remain unknown; no native closure, deletion, '
              'or project-stop change is asserted.')
    if not isinstance(reason, str) or len(reason) > 512 or not reason.startswith(prefix) or not reason.endswith(suffix):
        return False
    label = reason[len(prefix):-len(suffix)]
    return len(label) == 3 and label.isascii() and label.isdecimal() and label == request_id[:3]


def _wbt_cancellation_authorization_matches(encoded: object, request_id: str,
                                            candidate: str, expected_thread: str) -> bool:
    return _wbt_cancellation_authorization_status(encoded, request_id, candidate,
                                                  expected_thread) == 'valid'


def _wbt_cancellation_authorization_status(encoded: object, request_id: str,
                                           candidate: str, expected_thread: str) -> str:
    if not isinstance(encoded, str) or not encoded or len(encoded) > 4096:
        return 'invalid'
    program = '''type == "object"
      and (keys | sort) == ["assignment_message_id","boundary","candidate_thread_id","expected_thread","instruction_origin","request_id"]
      and .instruction_origin == "app_po_pending_intent_cancellation_assignment"
      and (.assignment_message_id | type == "string" and startswith("message_") and length > 8)
      and .request_id == $request_id
      and .candidate_thread_id == $candidate
      and .expected_thread == $expected_thread
      and .boundary == "cancel only this future app handoff intent and its request gates; preserve native unknown and all candidate/history"'''
    try:
        result = subprocess.run(
            ['jq', '-e', '--arg', 'request_id', request_id, '--arg', 'candidate', candidate,
             '--arg', 'expected_thread', expected_thread, program],
            input=encoded, text=True, capture_output=True, timeout=1, check=False)
    except (OSError, subprocess.SubprocessError):
        return 'unavailable'
    return 'valid' if result.returncode == 0 and result.stdout.strip() == 'true' else 'invalid'


def _wbt_retirement_status(db, request, current) -> str:
    """Validate one immutable typed Root retirement without implying native closure."""
    try:
        if not _has_table(db, 'root_fresh_retirements') or not _has_table(db, 'root_fresh_replacements'):
            return 'none'
        retirement_schema = list(db.execute("PRAGMA table_info(root_fresh_retirements)"))
        replacement_schema = list(db.execute("PRAGMA table_info(root_fresh_replacements)"))
        event_schema = list(db.execute("PRAGMA table_info(root_fresh_replacement_events)")) \
            if _has_table(db, 'root_fresh_replacement_events') else []
        if [(r['name'],r['type'].upper(),r['notnull'],r['pk']) for r in retirement_schema] != [
                ('retirement_id','TEXT',0,1),('request_id','TEXT',1,0),('home_project','TEXT',1,0),
                ('home_session','TEXT',1,0),('host_id','TEXT',1,0),('expected_thread','TEXT',1,0),
                ('reconciliation_id','TEXT',1,0),('previous_candidate_id','TEXT',1,0),
                ('replacement_candidate_id','TEXT',0,0),('outcome','TEXT',1,0),('reason','TEXT',1,0),
                ('authorization_json','TEXT',1,0),('state','TEXT',1,0),('created_at','REAL',1,0),
                ('updated_at','REAL',1,0)]:
            return 'unavailable'
        if [(r['name'],r['type'].upper(),r['notnull'],r['pk']) for r in replacement_schema] != [
                ('request_id','TEXT',0,1),('reconciliation_id','TEXT',1,0),('home_project','TEXT',1,0),
                ('home_session','TEXT',1,0),('host_id','TEXT',1,0),('expected_thread','TEXT',1,0),
                ('previous_candidate_id','TEXT',1,0),('replacement_candidate_id','TEXT',0,0),
                ('settings_digest','TEXT',0,0),('state','TEXT',1,0),('error','TEXT',0,0),
                ('authorization_json','TEXT',1,0),('created_at','REAL',1,0),('updated_at','REAL',1,0)]:
            return 'unavailable'
        if [(r['name'],r['type'].upper(),r['notnull'],r['pk']) for r in event_schema] != [
                ('event_id','INTEGER',0,1),('reconciliation_id','TEXT',1,0),
                ('event','TEXT',1,0),('detail','TEXT',0,0),('recorded_at','REAL',1,0)]:
            return 'unavailable'
        retirements = db.execute(
            'SELECT * FROM root_fresh_retirements WHERE request_id=? LIMIT 2',
            (request['id'],)).fetchall()
        if not retirements:
            return 'none'
        if len(retirements) != 1:
            return 'invalid'
        replacements = db.execute(
            'SELECT * FROM root_fresh_replacements WHERE request_id=? LIMIT 2',
            (request['id'],)).fetchall()
        if len(replacements) != 1:
            return 'invalid'
        audit, replacement = retirements[0], replacements[0]

        def exact_unique_index(table, columns):
            for index in db.execute("PRAGMA index_list('" + table + "')"):
                if not index['unique']:
                    continue
                name = str(index['name']).replace('"', '""')
                indexed = [item['name'] for item in db.execute('PRAGMA index_info("' + name + '")')]
                if indexed == columns:
                    return True
            return False

        if (not exact_unique_index('root_fresh_retirements', ['retirement_id'])
                or not exact_unique_index('root_fresh_retirements', ['request_id'])
                or not exact_unique_index('root_fresh_replacements', ['request_id'])
                or not exact_unique_index('root_fresh_replacements', ['reconciliation_id'])):
            return 'unavailable'

        expected_triggers = {
            'immutable_root_fresh_retirement_identity': ('''CREATE TRIGGER immutable_root_fresh_retirement_identity BEFORE UPDATE ON root_fresh_retirements
              WHEN OLD.retirement_id<>NEW.retirement_id OR OLD.request_id<>NEW.request_id
                OR OLD.home_project<>NEW.home_project OR OLD.home_session<>NEW.home_session OR OLD.host_id<>NEW.host_id
                OR OLD.expected_thread<>NEW.expected_thread OR OLD.reconciliation_id<>NEW.reconciliation_id
                OR OLD.previous_candidate_id<>NEW.previous_candidate_id
                OR COALESCE(OLD.replacement_candidate_id,'')<>COALESCE(NEW.replacement_candidate_id,'')
                OR OLD.outcome<>NEW.outcome OR OLD.reason<>NEW.reason
                OR OLD.authorization_json<>NEW.authorization_json OR OLD.created_at<>NEW.created_at
                OR NOT (OLD.state='retiring' AND NEW.state='retired')
              BEGIN SELECT RAISE(ABORT,'root fresh-start retirement audit is immutable'); END'''),
            'immutable_root_fresh_retirement_delete': ('''CREATE TRIGGER immutable_root_fresh_retirement_delete BEFORE DELETE ON root_fresh_retirements
              BEGIN SELECT RAISE(ABORT,'root fresh-start retirement audit is immutable'); END'''),
            'immutable_root_replacement_update': ('''CREATE TRIGGER immutable_root_replacement_update BEFORE UPDATE ON root_fresh_replacements
              WHEN OLD.reconciliation_id<>NEW.reconciliation_id OR OLD.request_id<>NEW.request_id
                OR OLD.home_project<>NEW.home_project OR OLD.home_session<>NEW.home_session OR OLD.host_id<>NEW.host_id
                OR OLD.expected_thread<>NEW.expected_thread OR OLD.previous_candidate_id<>NEW.previous_candidate_id
                OR (OLD.replacement_candidate_id IS NOT NULL AND OLD.replacement_candidate_id<>NEW.replacement_candidate_id)
                OR (OLD.settings_digest IS NOT NULL AND OLD.settings_digest<>NEW.settings_digest)
                OR OLD.authorization_json<>NEW.authorization_json OR OLD.created_at<>NEW.created_at
              BEGIN SELECT RAISE(ABORT,'root candidate replacement identity is immutable'); END'''),
            'immutable_root_replacement_delete': ('''CREATE TRIGGER immutable_root_replacement_delete BEFORE DELETE ON root_fresh_replacements
              BEGIN SELECT RAISE(ABORT,'root candidate replacement audit is immutable'); END'''),
            'immutable_root_replacement_event_update': ('''CREATE TRIGGER immutable_root_replacement_event_update BEFORE UPDATE ON root_fresh_replacement_events
              BEGIN SELECT RAISE(ABORT,'root candidate replacement events are immutable'); END'''),
            'immutable_root_replacement_event_delete': ('''CREATE TRIGGER immutable_root_replacement_event_delete BEFORE DELETE ON root_fresh_replacement_events
              BEGIN SELECT RAISE(ABORT,'root candidate replacement events are immutable'); END'''),
        }
        for name, expected_sql in expected_triggers.items():
            found = db.execute("SELECT tbl_name,sql FROM sqlite_master WHERE type='trigger' AND name=?",
                               (name,)).fetchall()
            expected_table = ('root_fresh_retirements' if 'retirement' in name
                              else 'root_fresh_replacement_events' if 'replacement_event' in name
                              else 'root_fresh_replacements')
            if (len(found) != 1 or found[0]['tbl_name'] != expected_table
                    or _wbt_normalize_trigger_sql(found[0]['sql'])
                    != _wbt_normalize_trigger_sql(expected_sql)):
                return 'unavailable'

        request_id = request['id']
        candidate = request['new_thread_id']
        try:
            if (str(uuid.UUID(audit['retirement_id'])) != audit['retirement_id']
                    or str(uuid.UUID(audit['reconciliation_id'])) != audit['reconciliation_id']
                    or str(uuid.UUID(replacement['reconciliation_id'])) != replacement['reconciliation_id']
                    or str(uuid.UUID(candidate)) != candidate):
                return 'invalid'
        except (ValueError, TypeError, AttributeError):
            return 'invalid'
        if (request['status'] != 'retired' or request['native_action'] != 'creating'
                or request['queue_id'] is not None or request['native_proof_json'] is not None
                or request['response_proof_json'] is not None or request['owner_proof_json'] is not None
                or request['home_project'] != current['home_project']
                or request['home_session'] != current['home_session']
                or request['host_id'] != current['host_id']
                or request['old_thread_id'] != current['thread_id']
                or not isinstance(candidate, str) or not candidate or candidate == current['thread_id']):
            return 'invalid'
        if (audit['request_id'] != request_id or audit['home_project'] != current['home_project']
                or audit['home_session'] != current['home_session'] or audit['host_id'] != current['host_id']
                or audit['expected_thread'] != current['thread_id']
                or audit['reconciliation_id'] != replacement['reconciliation_id']
                or audit['previous_candidate_id'] != candidate
                or audit['replacement_candidate_id'] is not None
                or audit['outcome'] != 'replacement_start_outcome_unknown_no_native_closure'
                or audit['reason'] != ('Fresh-start request retired by operator direction; native replacement '
                    'outcome remains uncertain and no native closure or cleanup is asserted.')
                or audit['state'] != 'retired'):
            return 'invalid'
        if (replacement['request_id'] != request_id
                or replacement['home_project'] != current['home_project']
                or replacement['home_session'] != current['home_session']
                or replacement['host_id'] != current['host_id']
                or replacement['expected_thread'] != current['thread_id']
                or replacement['previous_candidate_id'] != candidate
                or replacement['replacement_candidate_id'] is not None
                or replacement['state'] != 'start_uncertain'):
            return 'invalid'
        events = db.execute('''SELECT event,recorded_at FROM root_fresh_replacement_events
            WHERE reconciliation_id=? ORDER BY event_id LIMIT 129''',
            (replacement['reconciliation_id'],)).fetchall()
        if len(events) > 128 or not events or events[0]['event'] != 'requested':
            return 'invalid'
        event_state = 'requested'
        start_started = 0
        start_uncertain = 0
        previous_at = 0.0
        for event_index,event in enumerate(events):
            if (type(event['recorded_at']) not in (int,float)
                    or not math.isfinite(event['recorded_at'])
                    or event['recorded_at'] <= 0 or event['recorded_at'] < previous_at):
                return 'invalid'
            previous_at = event['recorded_at']
            if event['event'] == 'waiting_old_root_idle' and event_state in ('requested','preflight_verified'):
                continue
            if event['event'] == 'preflight_verified' and event_state in ('requested','preflight_verified'):
                event_state = 'preflight_verified'
            elif event['event'] == 'start_started' and event_state == 'preflight_verified':
                event_state = 'start_started'
                start_started += 1
            elif event['event'] == 'start_uncertain' and event_state == 'start_started':
                event_state = 'start_uncertain'
                start_uncertain += 1
            elif event['event'] == 'requested' and event_index == 0:
                event_state = 'requested'
            else:
                return 'invalid'
        if event_state != 'start_uncertain' or start_started != 1 or start_uncertain != 1:
            return 'invalid'
        try:
            if _has_table(db, 'root_fresh_pending_cancellations') and db.execute(
                    'SELECT 1 FROM root_fresh_pending_cancellations WHERE request_id=? LIMIT 1',
                    (request_id,)).fetchone():
                return 'invalid'
            if _has_table(db, 'root_fresh_settings_retirements') and db.execute(
                    'SELECT 1 FROM root_fresh_settings_retirements WHERE request_id=? LIMIT 1',
                    (request_id,)).fetchone():
                return 'invalid'
        except sqlite3.Error:
            return 'unavailable'
        for encoded, kind in ((audit['authorization_json'], 'retirement'),
                              (replacement['authorization_json'], 'replacement')):
            result = _wbt_root_authorization_status(
                encoded, request_id, candidate, current['home_session'], current['host_id'], kind,
                expected_thread=current['thread_id'],
                reconciliation_id=audit['reconciliation_id'])
            if result == 'unavailable':
                return 'unavailable'
            if result != 'valid':
                return 'invalid'
        return 'valid'
    except (sqlite3.Error, KeyError, IndexError, TypeError, ValueError, AttributeError):
        return 'unavailable'


def _wbt_root_authorization_status(encoded: object, request_id: str, candidate: str,
                                   home_session: str, host_id: str, kind: str, *,
                                   expected_thread: str, reconciliation_id: str) -> str:
    if not isinstance(encoded, str) or not encoded or len(encoded) > 8192:
        return 'invalid'
    if kind == 'retirement':
        program = '''type == "object"
          and (keys|sort)==["action","answer","expected_thread","instruction_origin","question","question_item_id","reconciliation_id","record_provenance","request_id","supporting_native_user_authorization_ref"]
          and .instruction_origin=="root_delegated_bounded_recovery_instruction"
          and ((.supporting_native_user_authorization_ref|type)=="string" and (.supporting_native_user_authorization_ref|startswith("request_user_input_async:")) and (.supporting_native_user_authorization_ref|length)<=256)
          and ((.supporting_native_user_authorization_ref|split(":")) as $ref | ($ref|length)==3 and $ref[0]=="request_user_input_async" and ($ref[1]|startswith("call_")) and ($ref[1]|length)>=10 and $ref[2]=="0")
          and ((.question_item_id|type)=="array" and (.question_item_id|length)==3 and .question_item_id[0]=="request_user_input_async" and .question_item_id[1]==(.supporting_native_user_authorization_ref|split(":")[1]) and .question_item_id[2]==0)
          and ((.question|type)=="string" and (.question|length)>0 and (.question|length)<=2048 and (.question|contains($request_id)) and (.question|contains($candidate)))
          and .answer=="Replace candidate within same request (Recommended)"
          and .record_provenance=="Root neutral record of actual native replacement authorization; the retirement is the separately delegated bounded recovery action, not a new direct-native reply"
          and .action=="retire_uncertain_same_request_after_replacement_settings_drift"
          and .request_id==$request_id and .expected_thread==$expected_thread
          and .reconciliation_id==$reconciliation_id'''
    else:
        program = '''type == "object"
          and (keys|sort)==["answer","authorization_ref","canonical_session","host_id","instruction_origin","original_candidate_thread_id","question","question_item_id","record_provenance","request_id"]
          and .instruction_origin=="direct_native_user_reply"
          and ((.authorization_ref|type)=="string" and (.authorization_ref|startswith("request_user_input_async:")) and (.authorization_ref|length)<=256)
          and ((.authorization_ref|split(":")) as $ref | ($ref|length)==3 and $ref[0]=="request_user_input_async" and ($ref[1]|startswith("call_")) and ($ref[1]|length)>=10 and $ref[2]=="0")
          and ((.question_item_id|type)=="array" and (.question_item_id|length)==3 and .question_item_id[0]=="request_user_input_async" and .question_item_id[1]==(.authorization_ref|split(":")[1]) and .question_item_id[2]==0)
          and ((.question|type)=="string" and (.question|length)>0 and (.question|length)<=2048 and (.question|contains($request_id)) and (.question|contains($candidate)))
          and .answer=="Replace candidate within same request (Recommended)"
          and .record_provenance=="Root neutral record of actual native user question/reply; not an agent-chat server-stamped message"
          and .request_id==$request_id and .original_candidate_thread_id==$candidate
          and .canonical_session==$home_session and .host_id==$host_id'''
    try:
        result = subprocess.run(
            ['jq', '-e', '--arg', 'request_id', request_id, '--arg', 'candidate', candidate,
             '--arg', 'expected_thread', expected_thread,
             '--arg', 'reconciliation_id', reconciliation_id,
             '--arg', 'home_session', home_session,
             '--arg', 'host_id', host_id, program], input=encoded, text=True,
            capture_output=True, timeout=1, check=False)
    except (OSError, subprocess.SubprocessError):
        return 'unavailable'
    return 'valid' if result.returncode == 0 and result.stdout.strip() == 'true' else 'invalid'


def _load_wbt_configured_root_base(master_db: str | Path,
                                   service_plist: str | Path) -> ConfiguredRoot:
    """Load canonical configured Root identity without classifying request history."""
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
        row = db.execute('SELECT * FROM root_config WHERE singleton=1').fetchone()
        if (not row or row['mode'] != 'running'
                or not all(isinstance(row[name], str) and row[name]
                           for name in ('home_project', 'home_session', 'host_id'))):
            raise CoordError('configured Root is unavailable for administrative disposition')
        if not db.execute('SELECT 1 FROM projects WHERE id=?', (row['home_project'],)).fetchone():
            raise CoordError('configured Root home project is absent from the registry')
        root = ConfiguredRoot(master, registry, row['home_project'], row['home_session'], row['host_id'], ENDPOINT)
    return root


def load_wbt_preview_configured_root(master_db: str | Path,
                                     service_plist: str | Path) -> ConfiguredRoot:
    """WBT-only configured Root load with typed terminal-intent validation."""
    root = _load_wbt_configured_root_base(master_db, service_plist)
    with _read_existing(root.registry) as db:
        row = db.execute('SELECT * FROM root_config WHERE singleton=1').fetchone()
        if not row or not _wbt_retired_pending_intents_are_typed_admin_records(db, row):
            raise CoordError('configured Root fresh-start history is not terminal typed WBT administration')
    path = target_path(root, 'default')
    with _read_existing(root.registry) as db:
        _project_resumed_stop_context(root, db, 'default', path)
    return root


def load_wbt_diagnosis_configured_root(master_db: str | Path,
                                        service_plist: str | Path) -> ConfiguredRoot:
    """Dedicated WBT diagnostic loader; does not permit resource-ledger access."""
    root = _load_wbt_configured_root_base(master_db, service_plist)
    path = target_path(root, 'default')
    with _read_existing(root.registry) as db:
        _project_resumed_stop_context(root, db, 'default', path)
    return root


def load_wbt_resumed_configured_root(master_db: str | Path,
                                    service_plist: str | Path,
                                    project: str,
                                    stop_request_id: str) -> ConfiguredRoot:
    """Load canonical Root only for an exact, durably completed scoped resume."""
    root = _load_wbt_configured_root_base(master_db, service_plist)
    path = target_path(root, project)
    with _read_existing(root.registry) as registry, _read_existing(path) as target:
        _wbt_resumed_project_stop_context(root, registry, project, path,
                                          target, stop_request_id)
    return root


def _wbt_resumed_project_stop_context(root: ConfiguredRoot,
                                      registry: sqlite3.Connection,
                                      project: str, path: Path,
                                      target_db: sqlite3.Connection,
                                      stop_request_id: str):
    """Require the exact Root-owned global and target resume records to agree."""
    project = _clean(project, 'project', 512)
    expected = (root.master if project == 'default' else
                root.master.parent / (root.master.name + '.projects') /
                project / 'state.sqlite3')
    if path != expected:
        raise CoordError('selected project store changed during resumed-stop validation')
    try:
        stop_request_id = str(uuid.UUID(stop_request_id))
    except (ValueError, TypeError, AttributeError) as error:
        raise CoordError('one canonical completed Root stop request is required') from error
    _require(registry, 'root_config', 'root_fresh_requests', 'root_team_operations',
             'root_team_operation_projects', 'root_projects')
    config = registry.execute('SELECT * FROM root_config WHERE singleton=1').fetchone()
    if (not config or (config['home_project'], config['home_session'],
                       config['host_id'], config['mode']) !=
            (root.home_project, root.home_session, root.host_id, 'running')):
        raise CoordError('configured Root generation changed')
    if not _wbt_retired_pending_intents_are_typed_admin_records(registry, config):
        raise CoordError('configured Root fresh-start history is not terminal typed WBT administration')
    if registry.execute('SELECT 1 FROM root_team_operations WHERE resumed_at IS NULL LIMIT 1').fetchone():
        raise CoordError('another Root stop operation is active')
    operation_columns = {row['name'] for row in registry.execute(
        'PRAGMA table_info(root_team_operations)')}
    if not {'id', 'root_session', 'created_at', 'resumed_at'} <= operation_columns:
        raise CoordError('Root stop operation schema is unsupported')
    operations = registry.execute('''SELECT id,root_session,created_at,resumed_at
        FROM root_team_operations WHERE id=? LIMIT 2''', (stop_request_id,)).fetchall()
    if (len(operations) != 1 or operations[0]['root_session'] != root.home_session or
            operations[0]['resumed_at'] is None):
        raise CoordError('exact current Root-owned completed stop operation is unavailable')
    operation = operations[0]
    scope = registry.execute('''SELECT project_id,root_session,error
        FROM root_team_operation_projects WHERE request_id=? AND project_id=? LIMIT 2''',
        (stop_request_id, project)).fetchall()
    if (len(scope) != 1 or scope[0]['error'] is not None or
            not isinstance(scope[0]['root_session'], str) or not scope[0]['root_session']):
        raise CoordError('project is not in the exact completed Root stop scope')
    enrolled = registry.execute('''SELECT session_id,joined FROM root_projects
        WHERE project_id=? LIMIT 2''', (project,)).fetchall()
    if (len(enrolled) != 1 or type(enrolled[0]['joined']) is not int or
            enrolled[0]['joined'] != 1 or enrolled[0]['session_id'] != scope[0]['root_session']):
        raise CoordError('target project Root enrollment changed')
    _require(target_db, 'root_team_stops')
    stop_columns = {row['name'] for row in target_db.execute(
        'PRAGMA table_info(root_team_stops)')}
    if not {'id', 'root_session', 'active', 'created_at', 'resumed_at'} <= stop_columns:
        raise CoordError('target Root stop schema is unsupported')
    if target_db.execute('''SELECT 1 FROM root_team_stops
        WHERE active IS NULL OR active NOT IN (0,1) LIMIT 1''').fetchone():
        raise CoordError('target Root stop state is malformed')
    if target_db.execute('SELECT 1 FROM root_team_stops WHERE active<>0 LIMIT 1').fetchone():
        raise CoordError('target project has an active stop barrier')
    stops = target_db.execute('''SELECT id,root_session,active,created_at,resumed_at
        FROM root_team_stops WHERE id=? LIMIT 2''', (stop_request_id,)).fetchall()
    if (len(stops) != 1 or stops[0]['root_session'] != enrolled[0]['session_id'] or
            type(stops[0]['active']) is not int or stops[0]['active'] != 0 or
            stops[0]['resumed_at'] is None):
        raise CoordError('exact target stop resume witness is unavailable')
    times = (operation['created_at'], stops[0]['created_at'], stops[0]['resumed_at'],
             operation['resumed_at'])
    if (any(type(value) not in (int, float) or not math.isfinite(value) or value <= 0
            for value in times) or not
            (times[0] <= times[1] <= times[2] <= times[3])):
        raise CoordError('Root and target resume witness timestamps are malformed or unordered')
    return ('resumed-project-stop-v1', stop_request_id, root.home_session,
            project, enrolled[0]['session_id'], *times)


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


def _project_resumed_stop_context(root: ConfiguredRoot, registry: sqlite3.Connection,
                                 project: str, path: Path,
                                 target_db: sqlite3.Connection | None = None):
    """Bind the sole active global stop to this project's durable scoped resume."""
    project = _clean(project, 'project', 512)
    expected = root.master if project == 'default' else root.master.parent / (root.master.name + '.projects') / project / 'state.sqlite3'
    if path != expected:
        raise CoordError('selected project store changed during stop-barrier validation')
    active = registry.execute('''SELECT id,root_session,created_at FROM root_team_operations
        WHERE resumed_at IS NULL ORDER BY id LIMIT 2''').fetchall()

    def check_project_store(db):
        if not _has_table(db, 'root_team_stops'):
            if active:
                raise CoordError('exact scoped project resume is unavailable')
            return None
        columns = {row['name'] for row in db.execute('PRAGMA table_info(root_team_stops)')}
        required = {'id', 'root_session', 'active', 'created_at', 'resumed_at'}
        if not required <= columns:
            raise CoordError('project stop-barrier schema is unsupported')
        if db.execute('SELECT 1 FROM root_team_stops WHERE active IS NULL OR active NOT IN (0,1) LIMIT 1').fetchone():
            raise CoordError('project stop-barrier state is unsupported')
        if db.execute('SELECT 1 FROM root_team_stops WHERE active<>0 LIMIT 1').fetchone():
            raise CoordError('target project stop barrier is active')
        if not active:
            return None
        if len(active) != 1:
            raise CoordError('configured Root has conflicting active stop operations')
        operation = active[0]
        if operation['root_session'] != root.home_session:
            raise CoordError('active stop operation belongs to another Root')
        if not _has_table(registry, 'root_team_operation_projects') or not _has_table(registry, 'root_projects'):
            raise CoordError('original stop-project scope is unavailable')
        operation_columns = {row['name'] for row in registry.execute('PRAGMA table_info(root_team_operation_projects)')}
        project_columns = {row['name'] for row in registry.execute('PRAGMA table_info(root_projects)')}
        if not {'request_id', 'project_id', 'root_session', 'error'} <= operation_columns or not {'project_id', 'session_id', 'joined'} <= project_columns:
            raise CoordError('original stop-project scope schema is unsupported')
        scope = registry.execute('''SELECT project_id,root_session,error FROM root_team_operation_projects
            WHERE request_id=? AND project_id=? LIMIT 2''', (operation['id'], project)).fetchall()
        if len(scope) != 1 or scope[0]['error'] is not None:
            raise CoordError('project is not in the exact original stop scope')
        current = registry.execute('SELECT session_id,joined FROM root_projects WHERE project_id=?', (project,)).fetchone()
        if (not current or type(current['joined']) is not int or current['joined'] != 1
                or current['session_id'] != scope[0]['root_session']):
            raise CoordError('project Root enrollment changed since the original stop')
        stopped = db.execute('''SELECT root_session,active,created_at,resumed_at FROM root_team_stops
            WHERE id=? LIMIT 2''', (operation['id'],)).fetchall()
        if (len(stopped) != 1 or stopped[0]['root_session'] != scope[0]['root_session']
                or type(stopped[0]['active']) is not int or stopped[0]['active'] != 0):
            raise CoordError('exact scoped project resume is unavailable')
        created_at, resumed_at = stopped[0]['created_at'], stopped[0]['resumed_at']
        operation_created = operation['created_at']
        if (type(created_at) not in (int, float) or not math.isfinite(created_at) or created_at <= 0
                or type(resumed_at) not in (int, float) or not math.isfinite(resumed_at) or resumed_at < created_at
                or type(operation_created) not in (int, float) or not math.isfinite(operation_created) or operation_created <= 0):
            raise CoordError('scoped project resume timestamps are unsupported')
        return ('resumed-project-stop-v1', operation['id'], root.home_session,
                project, scope[0]['root_session'], operation_created, created_at, resumed_at)

    if target_db is not None:
        return check_project_store(target_db)
    with _read_existing(path) as db:
        return check_project_store(db)


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


def _recheck_root_barriers(registry: sqlite3.Connection, *, root: ConfiguredRoot | None = None,
                           project: str | None = None, target_path: Path | None = None,
                           target_db: sqlite3.Connection | None = None,
                           allow_retired_intents: bool = False):
    """Run under the registry lock; optional scope is only for expired claims."""
    _require(registry, 'root_config', 'root_fresh_requests', 'root_team_operations')
    root_row = registry.execute('SELECT mode FROM root_config WHERE singleton=1').fetchone()
    if not root_row or root_row['mode'] != 'running':
        raise CoordError('configured Root is unavailable for administrative disposition')
    if allow_retired_intents:
        if root is None:
            raise CoordError('exact configured Root is required')
        _check_administrative_fresh_history(registry, root)
    elif registry.execute("SELECT 1 FROM root_fresh_requests WHERE status<>'completed'").fetchone():
        raise CoordError('configured Root has a pending fresh-start')
    active = registry.execute('SELECT 1 FROM root_team_operations WHERE resumed_at IS NULL LIMIT 1').fetchone()
    if active and (root is None or project is None or target_path is None):
        raise CoordError('configured Root stop barrier is active')
    if project is None:
        return None
    if root is None or target_path is None:
        raise CoordError('exact scoped project resume is unavailable')
    return _project_resumed_stop_context(root, registry, project, target_path, target_db)


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
    root = load_configured_root(master_db, service_plist, allow_retired_intents=True)
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
        _recheck_root_barriers(registry_db, root=root, allow_retired_intents=True)
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
