"""Same-UID, configured-app Root adapter. Never a network privilege grant."""
from __future__ import annotations

import argparse
import contextlib
import json
import math
import os
from pathlib import Path
import sqlite3
import stat
from types import SimpleNamespace
import threading
import time
import uuid

from .core import CoordError, Coordinator
from .service import ServiceError, _installed_server_config, configured_server_database


FIELDS = {
    'exact-file-bind': {'project','to','task_id','source_request_id','expected_host','expected_thread','path','output','max_bytes','minutes','request_id','confirm'},
    'exact-file-binding-status': {'project','binding_id'},
    'exact-file-revoke': {'project','binding_id','request_id','reason','confirm'},
    'hospital-vendor-issue': {'confirm'},
    'hospital-vendor-status': set(),
    'hospital-vendor-diagnose': set(),
    'hospital-vendor-select': {'task_id','intake_id','essential_id','body_sha256','intake_sha256','essential_sha256','expires_at','confirm'},
    'hospital-vendor-selection-status': {'binding_id'},
    'hospital-vendor-selection-revoke': {'binding_id','confirm'},
    'hospital-vendor-selection-unknown': {'binding_id','confirm'},
    'hospital-vendor-revoke': {'confirm'},
    'hospital-vendor-unknown': {'confirm'},
    'capacity-observe': {'project'},
    'capacity-admit': {'project', 'request_id', 'authorization_source_id', 'confirm'},
    'capacity-successor-4621': {'confirm'},
    'capacity-successor-e2d': {'confirm'},
    'capacity-successor-build01': {'confirm'},
    'capacity-successor-native02': {'confirm'},
    'capacity-successor-native02-readback': set(),
    'capacity-successor-attempt-status': {'request_id','attempt_id'},
    'retire-obsolete-d6b4': {'confirm'},
    'worker-native-observe': {'project','target','expected_thread','expected_host','request_id','confirm'},
    'worker-admission-intent': {'project','target','expected_thread','expected_host','request_id','producer_id','producer_credential','expected_root','canonical_resource','confirm'},
    'worker-admission-enroll': {'project','target','expected_thread','expected_host','producer_id','producer_credential','expected_root','canonical_resource','confirm','enrollment_key'},
    'worker-admission-enrollment-status': {'project','target','expected_thread','expected_host','producer_id','producer_credential','expected_root','canonical_resource','confirm','enrollment_key'},
    'worker-admission-status': {'project','target','expected_thread','expected_host','request_id','producer_id','producer_credential','expected_root','canonical_resource','confirm'},
    'active-projects': set(),
    'readiness': {'projects', 'expected_thread'},
    'startup-plan': {'projects', 'expected_thread'},
    'pending-fresh': {'expected_thread'},
    'status': {'project_id', 'expected_po_session', 'expected_thread', 'expected_host', 'expected_stop_id'}, 'enroll': {'expected_thread', 'confirm'},
    'po-status-inspect': {'project_id', 'expected_po_session', 'expected_thread', 'expected_host', 'expected_stop_id', 'request_id'},
    'in': {'project'}, 'route': {'project'},
    'configure': {'project', 'agent', 'cwd', 'prompt', 'request_id', 'confirm', 'model', 'effort'},
    'direct-create': {'project', 'reset_id', 'agent', 'cwd', 'prompt', 'request_id', 'confirm', 'model', 'effort'},
    'retry-setup': {'project', 'request_id', 'confirm'},
    'adopt': {'project', 'target', 'expected_thread', 'confirm'},
    'detach': {'confirm'},
    'provision': {'project', 'agent', 'request_id', 'confirm', 'model', 'effort'},
    'delegate-management': {'project', 'po', 'target', 'expected_thread', 'expected_host', 'request_id', 'confirm'},
    'team-lifecycle': {'operation', 'request_id', 'confirm'},
    'team-reset-preview': {'project', 'message_ids'},
    'team-reset': {'project', 'message_ids', 'request_id', 'expected_digest', 'confirm'},
    'team-reset-status': {'project', 'request_id'},
    'stop-all': {'request_id', 'confirm'}, 'resume-all': {'request_id', 'confirm'},
    'stop-status': {'request_id'},
    'resume-project': {'project', 'request_id', 'confirm'},
    'onboard-project': {'project', 'stop_id', 'expected_thread', 'request_id', 'confirm'},
    'attach-current': {'expected_thread','current_thread','current_turn','expected_endpoint','expected_listener_pid','expected_birth_sec','expected_birth_usec','expected_image_sha','request_id','confirm'},
    'attach-status': {'request_id'},
    'attach-handoff-diagnose': {'expected_thread'},
    'po-admit': {'project', 'po', 'expected_thread', 'expected_host', 'request_id', 'confirm'},
    'turnover-bootstrap': {'project', 'request_id', 'confirm'},
    'retire-unbound-parked': {'project', 'bootstrap_id', 'stop_id', 'po', 'expected_thread', 'expected_host', 'request_id', 'confirm'},
    'retire-bound-parked': {'project', 'bootstrap_id', 'stop_id', 'po', 'expected_thread', 'expected_host',
                            'request_id', 'target', 'expected_agent', 'logical_owner', 'expected_bound_thread',
                            'expected_old_rollout', 'repair_request_id', 'expected_candidate', 'confirm'},
    'repair-parked-binding': {'project', 'stop_id', 'target', 'expected_thread', 'expected_host',
                              'original_request_id', 'request_id', 'confirm'},
    "turnover-bootstrap-reconcile": {"project", "request_id", "confirm"},
    "turnover-bootstrap-continue": {"project", "request_id", "message_id", "confirm"},
    "turnover-bootstrap-continue-second": {"project", "request_id", "message_id", "continuation_id",
                                           "first_retirement_id", "second_retirement_id", "confirm"},
    "turnover-admin-continue": {"project", "request_id", "message_id", "continuation_id",
                                "previous_id", "retirement_ids", "confirm"},
    'investigation-admit': {'target_project', 'po', 'target', 'expected_thread', 'expected_host', 'stop_id', 'request_id', 'message_id', 'inspection_request_id', 'expires_at', 'confirm'},
    'investigation-preflight': {'target_project', 'po', 'target', 'expected_thread', 'expected_host', 'stop_id', 'request_id', 'message_id', 'inspection_request_id', 'expires_at'},
    'investigation-settings-continuation': {'target_project', 'po', 'target', 'expected_thread', 'expected_host',
        'stop_id', 'admission_request_id', 'baseline_id', 'continuation_id', 'message_id',
        'inspection_request_id', 'source_message_id', 'source_artifact_sha256', 'source_limit',
        'source_artifact_name',
        'expected_settings', 'expires_at', 'confirm'},
    'managed-native-read': {'target_project', 'po', 'target', 'expected_thread', 'expected_host',
        'stop_id', 'admission_request_id', 'message_id', 'job_id', 'inspection_request_id',
        'request_id', 'confirm'},
    'managed-native-diagnose': {'target_project', 'po', 'target', 'expected_thread', 'expected_host',
        'stop_id', 'admission_request_id', 'message_id', 'job_id', 'inspection_request_id', 'request_id'},
    'fresh-start': {'expected_thread', 'prompt', 'request_id', 'confirm'},
    'fresh-status': {'request_id'},
    'fresh-recover': {'expected_thread', 'request_id', 'attempt_id', 'confirm'},
    'fresh-verify': {'expected_thread', 'request_id', 'verification_id', 'confirm'},
    'fresh-replace': {'expected_thread', 'request_id', 'reconciliation_id', 'confirm'},
    'fresh-retire': {'expected_thread', 'request_id', 'reconciliation_id', 'retirement_id', 'confirm'},
    'fresh-retire-settings-mismatch': {'expected_thread', 'request_id', 'retirement_id', 'confirm'},
    'fresh-cancel-pending': {'expected_thread', 'request_id', 'cancellation_id', 'confirm'},
    'delivery-metadata': {'expected_thread', 'message_ids'},
    'policy-effective-status': {'expected_thread'},
    'pause-routing': {'expected_thread', 'request_id', 'confirm'},
    'resume-routing': {'confirm'},
}


def _text(value, label):
    if not isinstance(value, str) or not value or len(value) > 4096 or any(ord(c) < 32 for c in value):
        raise CoordError('invalid local Root ' + label)
    return value


def _investigation_preflight_result(result):
    expected_checks = {
        'original_stop_is_current_active', 'target_project_enrollment_is_current',
        'target_project_stop_matches_target_root', 'original_stop_is_active',
        'stop_target_matches_target_root_thread_host', 'stop_target_status_admissible',
        'po_stop_admission_matches_target_root', 'po_designation_matches_target_root',
        'po_binding_matches_recorded_admission', 'po_host_matches_recorded_admission',
        'worker_binding_matches_stop_target', 'worker_host_matches_stop_target',
        'message_sender_recipient_matches_po_worker', 'inspection_request_present',
        'inspection_request_requester_matches_target_root',
        'inspection_request_requester_matches_current_admitted_po',
        'inspection_request_target_matches_worker', 'inspection_request_thread_matches_worker',
        'inspection_request_host_matches_worker', 'inspection_request_action_is_inspect',
        'inspection_request_is_noninterrupting', 'inspection_request_matches_root_worker_binding',
        'inspection_request_status_admissible', 'expiry_within_one_hour',
        'request_and_message_scope_available', 'message_not_batched',
        'message_has_no_existing_delivery', 'no_other_active_stop_barrier',
        'message_unread_by_worker', 'worker_has_no_other_pending_control',
        'worker_has_no_pending_reset', 'worker_control_is_running', 'worker_is_not_quarantined',
    }
    requester_alternatives = {
        'inspection_request_requester_matches_target_root',
        'inspection_request_requester_matches_current_admitted_po',
    }
    requester_gate = 'inspection_request_matches_root_worker_binding'
    requester_tuple_checks = {
        'inspection_request_present', 'inspection_request_target_matches_worker',
        'inspection_request_thread_matches_worker', 'inspection_request_host_matches_worker',
        'inspection_request_action_is_inspect', 'inspection_request_is_noninterrupting',
    }
    expected = {'operation', 'status', 'checks', 'failed_checks'}
    if not isinstance(result, dict) or set(result) != expected:
        raise CoordError('local Root investigation preflight returned an invalid projection')
    if result.get('operation') != 'investigation-preflight' or result.get('status') not in ('eligible', 'blocked'):
        raise CoordError('local Root investigation preflight returned an invalid projection')
    checks = result.get('checks')
    failed = result.get('failed_checks')
    if not isinstance(checks, dict) or set(checks) != expected_checks or not isinstance(failed, list):
        raise CoordError('local Root investigation preflight returned an invalid projection')
    for name, value in checks.items():
        _text(name, 'preflight check name')
        if not isinstance(value, bool):
            raise CoordError('local Root investigation preflight returned an invalid projection')
    if (checks[requester_gate] != (all(checks[name] for name in requester_tuple_checks)
                                  and any(checks[name] for name in requester_alternatives))
            or any(not isinstance(name, str) for name in failed) or len(set(failed)) != len(failed)
            or set(failed) != {name for name, value in checks.items()
                               if value is False and name not in requester_alternatives}
            or (result['status'] == 'eligible') != (not failed)):
        raise CoordError('local Root investigation preflight returned an invalid projection')
    return {'operation': result['operation'], 'status': result['status'],
            'checks': dict(checks), 'failed_checks': list(failed)}


_MANAGED_NATIVE_DIAGNOSIS_CHECKS = {
    'project_schema_matches', 'actor_matches_request', 'task_association_present',
    'project_root_is_current', 'project_root_binding_matches', 'project_root_host_matches',
    'actor_is_authorized_root_or_admitted_po', 'po_role_matches_root',
    'po_binding_matches_admission', 'po_host_matches_admission', 'worker_role_is_worker',
    'worker_binding_matches_request', 'worker_host_matches_request', 'task_scope_matches_request',
    'only_expected_active_stop', 'stop_matches_project_root', 'stop_is_active',
    'target_admission_matches_request', 'target_admission_requester_matches_root',
    'target_admission_status_admissible', 'po_admission_matches_current_binding',
    'message_sender_recipient_matches', 'inspection_request_matches_target_thread_host',
    'inspection_requester_is_root_or_admitted_po', 'inspection_request_status_admissible',
    'original_wake_is_unique_exact_delivery', 'original_wake_not_cancelled',
    'message_not_batched', 'no_other_pending_control', 'no_pending_reset', 'worker_not_quarantined',
}


def _managed_native_diagnosis_result(result):
    expected = {'operation', 'verdict', 'sampled_at_start', 'sampled_at_end', 'checks'}
    if not isinstance(result, dict) or set(result) != expected or result.get('operation') != 'managed-native-diagnose':
        raise CoordError('local Root managed-native diagnosis returned an invalid projection')
    checks = result.get('checks')
    if not isinstance(checks, dict) or set(checks) != _MANAGED_NATIVE_DIAGNOSIS_CHECKS:
        raise CoordError('local Root managed-native diagnosis returned an invalid projection')
    if any(value not in ('true', 'false', 'unknown') for value in checks.values()):
        raise CoordError('local Root managed-native diagnosis returned an invalid projection')
    start, end = result.get('sampled_at_start'), result.get('sampled_at_end')
    if (isinstance(start, bool) or not isinstance(start, (int, float)) or not math.isfinite(start)
            or isinstance(end, bool) or not isinstance(end, (int, float)) or not math.isfinite(end)
            or end < start):
        raise CoordError('local Root managed-native diagnosis returned an invalid projection')
    expected_verdict = ('blocked' if 'false' in checks.values() else
                        'eligible_for_separate_review' if all(value == 'true' for value in checks.values())
                        else 'unknown')
    if result.get('verdict') != expected_verdict:
        raise CoordError('local Root managed-native diagnosis returned an invalid projection')
    return {'operation': result['operation'], 'verdict': expected_verdict,
            'sampled_at_start': start, 'sampled_at_end': end, 'checks': dict(checks)}


def fresh_status_notice(result):
    """Add a concise operator-routing notice to local CLI status output."""
    if not isinstance(result, dict) or not result.get('id'):
        return result
    result = dict(result)
    result.setdefault('operator_notice', {
        'request_id': result['id'],
        'state': result.get('handoff_state', result.get('status')),
        'reason': result.get('blocker'),
        'prerequisite': result.get('next_action'),
    })
    return result


def _file(path, uid):
    path = Path(path)
    try:
        info = path.lstat()
    except OSError as error:
        raise CoordError('local Root trusted file is missing') from error
    if not stat.S_ISREG(info.st_mode) or info.st_uid != uid or path.resolve() != path:
        raise CoordError('local Root trusted file owner or path differs')
    return path


@contextlib.contextmanager
def _read(path):
    db = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    try:
        yield db
    finally:
        db.close()


@contextlib.contextmanager
def _write_existing(path):
    """Open an existing same-UID store without schema initialization."""
    db = sqlite3.connect(path.as_uri() + '?mode=rw', uri=True, timeout=10)
    db.row_factory = sqlite3.Row
    try:
        yield db
    finally:
        db.close()


def _table(db, name):
    return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone())


def _target(master, registry, project, uid):
    _text(project, 'project')
    with _read(registry) as db:
        if not _table(db, 'projects') or not db.execute('SELECT 1 FROM projects WHERE id=?', (project,)).fetchone():
            raise CoordError('local Root project is not registered')
    if project == 'default':
        path = master
    else:
        # Registered project IDs still cannot introduce path traversal.
        if '/' in project or project in ('.', '..'):
            raise CoordError('invalid registered project path')
        path = master.parent / (master.name + '.projects') / project / 'state.sqlite3'
    _file(path, uid)
    with _read(path) as db:
        if not _table(db, 'meta'):
            raise CoordError('local Root project metadata is unavailable')
        metadata = dict(db.execute("SELECT key,value FROM meta WHERE key IN ('project_id','project_registry','schema_version')"))
        if metadata.get('schema_version') != '1':
            raise CoordError('local Root project schema is unsupported')
        if project == 'default':
            if metadata.get('project_id', 'default') != 'default':
                raise CoordError('local Root default project differs')
            if 'project_registry' in metadata and Path(metadata['project_registry']).resolve() != master:
                raise CoordError('local Root registry lineage differs')
        elif metadata.get('project_id') != project or not metadata.get('project_registry') or Path(metadata['project_registry']).resolve() != master:
            raise CoordError('local Root registry lineage differs')
    return path


def _completed_routing_resume(master, registry, root, uid):
    """Read-only exception for an already committed, unchanged lifecycle effect."""
    if root['mode'] != 'running':
        return False

    from .root_orchestrator import RootOrchestrator
    try:
        with _read(registry) as db:
            if not all(_table(db, name) for name in ('root_routing_pauses', 'root_routing_pause_projects')):
                return False
            audit = db.execute('SELECT * FROM root_routing_pauses ORDER BY created_at DESC,id DESC LIMIT 1').fetchone()
            if (not audit or audit['status'] != 'resumed' or audit['resumed_at'] is None
                    or (audit['home_project'], audit['home_session'], audit['host_id'], audit['thread_id']) !=
                    (root['home_project'], root['home_session'], root['host_id'], root['thread_id'])):
                return False
            saved = [dict(row) for row in db.execute('SELECT project_id,session_id FROM root_routing_pause_projects WHERE request_id=? ORDER BY project_id', (audit['id'],))]
            current = [dict(row) for row in db.execute('SELECT project_id,session_id FROM root_projects ORDER BY project_id')]
            if not saved or saved != current or dict(project_id=root['home_project'], session_id=root['home_session']) not in saved:
                return False

        class Readback(RootOrchestrator):
            def __init__(self):
                pass  # Do not initialize or migrate any schema during preflight.

            def path(self, project):
                return _target(master, registry, project, uid)

        readback = Readback()
        return all(not readback._routing_projection(root, row, None) for row in saved)
    except (CoordError, sqlite3.Error, OSError):
        return False


def _validate_project_lineage(master, project, metadata):
    """Validate a project store's canonical project and master-registry lineage."""
    if (metadata.get('schema_version') != '1'
            or metadata.get('project_id', 'default' if project == 'default' else None) != project):
        raise CoordError('local Root scoped PO project lineage differs')
    if project != 'default':
        lineage = metadata.get('project_registry')
        if not isinstance(lineage, str) or not lineage or Path(lineage).resolve() != master:
            raise CoordError('local Root scoped PO project lineage differs')


def _policy_effective_status(master, home_project, session, host, expected_thread):
    """Read own current thread's historical correlation and verified policy receipts."""
    from .communication import policy_version
    registry = Path(str(master) + '.projects.sqlite3')
    home = _target(master, registry, home_project, master.lstat().st_uid)
    with _read(registry) as roots, _read(home) as db:
        roots.execute('BEGIN'); db.execute('BEGIN')
        root = roots.execute('SELECT * FROM root_config WHERE singleton=1').fetchone()
        if not root or (root['home_project'],root['home_session'],root['host_id'],root['thread_id']) != (home_project,session,host,expected_thread):
            raise CoordError('local Root policy canonical identity or thread differs')
        if not _table(roots,'root_directory_owner'):
            raise CoordError('local Root policy directory owner is unavailable')
        owner=roots.execute('SELECT home_project,home_session FROM root_directory_owner WHERE singleton=1').fetchone()
        if not owner or tuple(owner)!=(home_project,session):
            raise CoordError('local Root policy directory owner differs')
        binding=db.execute('''SELECT b.thread_id,b.parent_session,h.host_id
            FROM bridge_bindings b JOIN remote_session_hosts h ON h.session_id=b.session_id
            WHERE b.session_id=?''',(session,)).fetchone()
        if not binding or tuple(binding)!=(expected_thread,None,host):
            raise CoordError('local Root policy current binding or host differs')
        historical=[]; verified=[]
        def has_columns(table, required):
            if not _table(roots,table): return False
            return required <= {row['name'] for row in roots.execute('PRAGMA table_info(' + table + ')')}
        legacy_schema=(has_columns('native_policy_offers',{'host','thread','client_id','version','project','kind','payload_sha256'})
                       and has_columns('native_policy_receipts',{'host','thread','client_id','version','turn_id','observed_at'}))
        verified_schema=(legacy_schema and
            has_columns('native_policy_verified_offers',{'host','thread','client_id','version','payload_sha256'}) and
            has_columns('native_policy_verified_receipts',{'host','thread','project','client_id','version','turn_id','observed_at'}))
        if legacy_schema:
            rows=roots.execute('''SELECT r.version,r.client_id,r.turn_id,r.observed_at,o.payload_sha256,o.kind
                FROM native_policy_receipts r JOIN native_policy_offers o
                ON o.host=r.host AND o.thread=r.thread AND o.client_id=r.client_id AND o.version=r.version
                WHERE r.host=? AND r.thread=? AND o.project=? ORDER BY r.observed_at,r.client_id LIMIT 65''',(host,expected_thread,home_project)).fetchall()
            if len(rows)>64: raise CoordError('local Root policy historical evidence exceeds limit')
            for row in rows:
                if isinstance(row['client_id'],str) and row['client_id'] and isinstance(row['turn_id'],str) and row['turn_id']:
                    historical.append({**dict(row),'full_policy_completeness':'unknown'})
        if verified_schema:
            rows=roots.execute('''SELECT r.version,r.client_id,r.turn_id,r.observed_at,v.payload_sha256,o.kind
                FROM native_policy_verified_receipts r JOIN native_policy_verified_offers v
                ON v.host=r.host AND v.thread=r.thread AND v.client_id=r.client_id AND v.version=r.version
                JOIN native_policy_offers o ON o.host=v.host AND o.thread=v.thread AND o.client_id=v.client_id
                    AND o.version=v.version AND o.payload_sha256=v.payload_sha256
                WHERE r.host=? AND r.thread=? AND r.project=o.project AND r.project=? ORDER BY r.observed_at,r.client_id LIMIT 65''',(host,expected_thread,home_project)).fetchall()
            if len(rows)>64: raise CoordError('local Root policy verified evidence exceeds limit')
            for row in rows:
                digest=row['payload_sha256']
                if (isinstance(row['client_id'],str) and row['client_id'] and isinstance(row['turn_id'],str) and row['turn_id']
                    and isinstance(row['observed_at'],(int,float)) and not isinstance(row['observed_at'],bool)
                    and math.isfinite(row['observed_at']) and row['observed_at']>0
                    and row['kind'] in ('root','reset','wake')
                    and isinstance(digest,str) and len(digest)==64 and all(c in '0123456789abcdef' for c in digest)):
                    verified.append(dict(row))
        version=policy_version()
        return {'authority':{'scope':'machine_read','named_session_authentication':'UNVERIFIED'},
                'project':home_project,'session':session,'host_id':host,'thread_id':expected_thread,
                'current_policy_version':version,'effective_state':'verified_current' if any(r['version']==version for r in verified) else 'unknown',
                'verified_schema':'present' if verified_schema else 'unavailable',
                'historical_receipts':historical,'verified_receipts':verified,
                'classification_scope':'recorded exact policy evidence; not consent or native process closure'}


def _delivery_metadata(master, home_project, session, host, expected_thread, message_ids):
    """Exact own-recipient history without content, native reads or writes."""
    registry = Path(str(master) + '.projects.sqlite3')
    home = _target(master, registry, home_project, master.lstat().st_uid)
    with _read(registry) as roots, _read(home) as db:
        roots.execute('BEGIN')
        db.execute('BEGIN')
        root = roots.execute('SELECT * FROM root_config WHERE singleton=1').fetchone()
        if not root or (root['home_project'], root['home_session'], root['host_id'], root['thread_id']) != (
                home_project, session, host, expected_thread):
            raise CoordError('local Root delivery canonical identity or thread differs')
        binding = db.execute('''SELECT b.thread_id,b.parent_session,h.host_id
            FROM bridge_bindings b JOIN remote_session_hosts h ON h.session_id=b.session_id
            WHERE b.session_id=?''', (session,)).fetchone()
        if not binding or (binding['thread_id'], binding['parent_session'], binding['host_id']) != (expected_thread, None, host):
            raise CoordError('local Root delivery current binding or host differs')
        if any(not _table(db, table) for table in ('messages', 'bridge_deliveries', 'bridge_jobs')):
            raise CoordError('local Root delivery metadata schema is unavailable')
        deliveries = []
        for message_id in message_ids:
            row = db.execute('''SELECT m.id AS message_id,m.created_at AS message_created_at,
                m.acked_at AS message_acked_at,d.job_id,j.thread_id,j.status AS job_status,j.queue_id,
                j.created_at AS job_created_at,j.updated_at AS job_updated_at
                FROM messages m LEFT JOIN bridge_deliveries d ON d.message_id=m.id
                LEFT JOIN bridge_jobs j ON j.id=d.job_id
                WHERE m.id=? AND m.recipient_session=?''', (message_id, session)).fetchone()
            if not row:
                raise CoordError('local Root message is unavailable for the canonical recipient')
            item = dict(row)
            if item['job_id'] is not None and item['thread_id'] != expected_thread:
                raise CoordError('local Root delivery job is unavailable for the current thread')
            item['client_user_message_id'] = item['job_id']
            item['job_prepared_at'] = None
            item['native_attempt_recorded_at_julian_day'] = None
            item['native_proof'] = []
            if item['job_id'] is not None:
                for table, column, field in (
                        ('bridge_job_origins', 'prepared_at', 'job_prepared_at'),
                        ('bridge_job_native_attempts', 'recorded_at', 'native_attempt_recorded_at_julian_day')):
                    if _table(db, table):
                        stamp = db.execute(f'SELECT {column} FROM {table} WHERE job_id=?', (item['job_id'],)).fetchone()
                        if stamp:
                            item[field] = stamp[0]
                if all(_table(roots, table) for table in ('native_policy_offers', 'native_policy_receipts')):
                    proofs = roots.execute('''SELECT r.version AS policy_version,r.turn_id AS native_turn_id,r.observed_at
                        FROM native_policy_receipts r JOIN native_policy_offers o
                        ON o.host=r.host AND o.thread=r.thread AND o.client_id=r.client_id AND o.version=r.version
                        WHERE o.host=? AND o.thread=? AND o.client_id=? AND o.project=? AND o.kind='wake'
                        ORDER BY r.observed_at,r.version''', (host, expected_thread, item['job_id'], home_project))
                    item['native_proof'] = [dict(proof) for proof in proofs
                                            if isinstance(proof['native_turn_id'], str) and proof['native_turn_id']]
            deliveries.append(item)
    return {
        'authority': {'scope': 'machine_read', 'named_session_authentication': 'UNVERIFIED'},
        'project': home_project, 'session': session, 'host_id': host, 'thread_id': expected_thread,
        'timestamp_semantics': {
            'message_created_at': 'persisted Unix seconds: message row creation',
            'message_acked_at': 'mutable Unix seconds: message acknowledgement, not native consumption',
            'job_created_at': 'persisted Unix seconds: job row creation',
            'job_updated_at': 'mutable Unix seconds: latest job write, not a dispatch, start or consumption timestamp',
            'job_prepared_at': 'persisted Unix seconds: immutable preparation provenance, when present',
            'native_attempt_recorded_at_julian_day': 'SQLite Julian day: first status update beyond prepared/cancelled, not native acceptance',
            'native_proof.observed_at': 'persisted Unix seconds: exact policy receipt observation, not native turn start',
        },
        'client_id_semantics': 'bridge job ID used as clientUserMessageId; not proof of native acceptance',
        'native_proof_semantics': 'existing exact host/thread/client/project policy receipt only; empty means no persisted proof',
        'deliveries': deliveries,
    }


def _preflight(master_db, service_plist, home_project, session, host, operation, params):
    config = _installed_server_config()
    # The caller cannot supply a UID. File ownership anchors both actual UIDs.
    try:
        owner = config.plist.lstat().st_uid
    except OSError as error:
        raise CoordError('local Root configured provider is unavailable') from error
    if os.getuid() != owner or os.geteuid() != owner:
        raise CoordError('local Root requires the configured application real and effective UID')
    for path in (config.plist, config.wrapper, config.executable):
        _file(path, owner)
    try:
        master = configured_server_database(service_plist)
    except ServiceError as error:
        raise CoordError(str(error)) from error
    if Path(os.path.abspath(master_db)) != master or master != config.master.resolve():
        raise CoordError('local Root master differs from the canonical configured master')
    _file(master, owner)
    registry = _file(Path(str(master) + '.projects.sqlite3'), owner)
    home = _target(master, registry, home_project, owner)
    attachment = None
    historical_attachment = False
    with _read(registry) as db:
        root = db.execute('SELECT * FROM root_config WHERE singleton=1').fetchone() if _table(db, 'root_config') else None
        if root and (root['home_project'], root['home_session'], root['host_id']) != (home_project, session, host):
            raise CoordError('local Root canonical identity differs')
        if not root and operation != 'enroll' and operation != 'status':
            raise CoordError('local Root is not enrolled')
        if root and any(not _table(db, name) for name in ('root_fresh_requests', 'root_team_operations')):
            raise CoordError('local Root lifecycle schema is incomplete')
        if operation in ('attach-current','attach-status') and _table(db, 'root_admin_attachment_requests'):
            attachment = db.execute('SELECT * FROM root_admin_attachment_requests WHERE request_id=? AND home_project=? AND home_session=? AND host_id=?',
                                    (params['request_id'],home_project,session,host)).fetchone()
            if attachment and operation == 'attach-current' and (attachment['old_thread'],attachment['new_thread'],attachment['turn_id'],attachment['endpoint'],str(attachment['listener_pid']),str(attachment['birth_sec']),str(attachment['birth_usec']),attachment['image_sha']) != (params['expected_thread'],params['current_thread'],params['current_turn'],params['expected_endpoint'],str(params['expected_listener_pid']),str(params['expected_birth_sec']),str(params['expected_birth_usec']),params['expected_image_sha']):
                raise CoordError('administrative attachment retry differs from retained request')
            if attachment and operation == 'attach-current' and attachment['state'] == 'attached':
                from .root_admin_attachment_members import release_completed
                historical_attachment = release_completed(db, params['request_id'])
        if operation in ('pause-routing', 'resume-routing'):
            from .root_pending_fresh import routing_fresh_blocker
            blocker = routing_fresh_blocker(db, master, root)
            if blocker:
                raise CoordError(blocker + '; routing change withheld')
            if operation == 'pause-routing' and root['mode'] not in ('running', 'maintenance_paused'):
                raise CoordError('root is detaching or detached; routing pause withheld')
    with _read(home) as db:
        if any(not _table(db, name) for name in ('sessions', 'bridge_bindings', 'remote_session_hosts')):
            raise CoordError('local Root registration schema is incomplete')
        registered = db.execute('SELECT 1 FROM sessions WHERE id=?', (session,)).fetchone()
        binding = db.execute('SELECT thread_id,parent_session FROM bridge_bindings WHERE session_id=?', (session,)).fetchone()
        host_row = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (session,)).fetchone()
        if not registered or not binding or binding['parent_session'] or not host_row or host_row['host_id'] != host:
            raise CoordError('local Root existing registration, main binding or host differs')
        partial_attachment = bool(attachment and root and root['thread_id'] in (attachment['old_thread'],attachment['new_thread']) and binding['thread_id'] in (attachment['old_thread'],attachment['new_thread']))
        if root and binding['thread_id'] != root['thread_id'] and not partial_attachment:
            raise CoordError('local Root current thread differs')
        if operation in ('enroll', 'onboard-project', 'attach-current', 'pause-routing', 'delivery-metadata', 'policy-effective-status', 'fresh-recover', 'fresh-verify', 'fresh-replace', 'fresh-retire', 'fresh-retire-settings-mismatch', 'fresh-cancel-pending', 'startup-plan', 'pending-fresh') and params['expected_thread'] != binding['thread_id'] and not (operation == 'attach-current' and (partial_attachment or historical_attachment)):
            raise CoordError('local Root expected thread differs')
        if operation in ('pause-routing', 'resume-routing'):
            # Reject stale/missing native inspection before any schema migration.
            from .root_orchestrator import RootOrchestrator
            if operation != 'resume-routing' or not _completed_routing_resume(master, registry, root, owner):
                RootOrchestrator._routing_inspection(db, root)
    if 'project' in params:
        _target(master, registry, params['project'], owner)
    # Preserve the HTTP application's setup measurement barrier without creating
    # a MeasurementStore or starting its workers. Missing state is not inactive.
    if operation in ('enroll', 'configure', 'provision', 'retry-setup', 'direct-create'):
        selected = home_project if operation == 'enroll' else params['project']
        with _read(master) as db:
            if not _table(db, 'usage_measurements'):
                raise CoordError('local Root measurement state is unavailable')
            if db.execute("SELECT 1 FROM usage_measurements WHERE project_id=? AND state IN ('starting','running')", (selected,)).fetchone():
                raise CoordError('finish the active project measurement before local Root setup')
    return master, bool(root)


def _scoped_po_control_status(master, home_project, session, host, params, *, include_internal=False):
    """Read only the current enrolled PO's stored mode and native observation."""
    project = params['project_id']
    po = params['expected_po_session']
    thread = params['expected_thread']
    po_host = params['expected_host']
    stop_id = params['expected_stop_id']
    owner = master.lstat().st_uid
    registry = _file(Path(str(master) + '.projects.sqlite3'), owner)
    if '/' in project or project in ('.', '..'):
        raise CoordError('invalid registered project path')
    project_db = master if project == 'default' else _file(
        master.parent / (master.name + '.projects') / project / 'state.sqlite3', owner)
    # Read-only attached transaction plus per-database version checks reject
    # mixed snapshots when a concurrent commit occurs. Never migrate stores.
    db = sqlite3.connect(master.as_uri() + '?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    try:
        db.execute('ATTACH DATABASE ? AS root_registry', (registry.as_uri() + '?mode=ro',))
        db.execute('ATTACH DATABASE ? AS po_project', (project_db.as_uri() + '?mode=ro',))
        schemas = ('main', 'root_registry', 'po_project')
        before_versions = {schema: db.execute(f'PRAGMA {schema}.data_version').fetchone()[0]
                           for schema in schemas}
        db.execute('BEGIN')
        required_registry = ('root_config', 'root_projects', 'projects', 'root_team_operations',
                             'root_team_operation_projects')
        required_project = ('meta', 'root_projections', 'project_orchestrators', 'bridge_bindings',
                            'remote_session_hosts', 'agent_controls', 'agent_control_observations', 'root_team_stops',
                            'root_team_stop_po_admissions', 'root_team_stop_targets')
        for schema_name, tables in (('root_registry', required_registry), ('po_project', required_project)):
            available = {r['name'] for r in db.execute(f'SELECT name FROM {schema_name}.sqlite_master WHERE type=\'table\'')}
            if not set(tables) <= available:
                raise CoordError('local Root scoped PO status schema is incomplete')
        root = db.execute('SELECT home_project,home_session,host_id,thread_id FROM root_registry.root_config WHERE singleton=1').fetchone()
        if not root or tuple(root[:3]) != (home_project, session, host):
            raise CoordError('local Root scoped PO canonical binding differs')
        registered = db.execute('SELECT 1 FROM root_registry.projects WHERE id=?', (project,)).fetchone()
        enrollment = db.execute('SELECT session_id,orchestrator_session,joined FROM root_registry.root_projects WHERE project_id=?', (project,)).fetchone()
        if not registered or not enrollment or enrollment['joined'] != 1 or enrollment['orchestrator_session'] != po:
            raise CoordError('local Root scoped PO designation or enrollment differs')
        meta = dict(db.execute("SELECT key,value FROM po_project.meta WHERE key IN ('project_id','project_registry','schema_version')"))
        _validate_project_lineage(master, project, meta)
        projection = db.execute('SELECT home_project,home_session,thread_id FROM po_project.root_projections WHERE session_id=?',
                                (enrollment['session_id'],)).fetchone()
        if not projection or tuple(projection) != (home_project, session, root['thread_id']):
            raise CoordError('local Root scoped PO project projection differs')
        po_role = db.execute('SELECT 1 FROM po_project.project_orchestrators WHERE session_id=? AND root_session=?',
                             (po, enrollment['session_id'])).fetchone()
        binding = db.execute('SELECT thread_id,parent_session FROM po_project.bridge_bindings WHERE session_id=?', (po,)).fetchone()
        host_row = db.execute('SELECT host_id FROM po_project.remote_session_hosts WHERE session_id=?', (po,)).fetchone()
        if not po_role or not binding or (binding['thread_id'], binding['parent_session']) != (thread, None) or not host_row or host_row['host_id'] != po_host:
            raise CoordError('local Root scoped PO current designation, thread or host differs')
        operation = db.execute('SELECT root_session,resumed_at FROM root_registry.root_team_operations WHERE id=?',
                               (stop_id,)).fetchone()
        project_scope = db.execute('''SELECT root_session FROM root_registry.root_team_operation_projects
            WHERE request_id=? AND project_id=?''', (stop_id, project)).fetchone()
        active_stop = db.execute('SELECT root_session,active FROM po_project.root_team_stops WHERE id=?',
                                 (stop_id,)).fetchone()
        original = db.execute('''SELECT requester_session,target_session,thread_id,host_id,status
            FROM po_project.root_team_stop_targets WHERE stop_id=? AND target_session=?''', (stop_id, po)).fetchone()
        admission = db.execute('''SELECT root_session,target_session,thread_id,host_id
            FROM po_project.root_team_stop_po_admissions WHERE stop_id=?''', (stop_id,)).fetchone()
        project_root = enrollment['session_id']
        if (not operation or operation['root_session'] != session or operation['resumed_at'] is not None
                or not project_scope or project_scope['root_session'] != project_root
                or not active_stop or active_stop['active'] != 1 or active_stop['root_session'] != project_root
                or not original or tuple(original[:4]) != (project_root, po, thread, po_host)
                or original['status'] == 'interrupting'
                or not admission or tuple(admission) != (project_root, po, thread, po_host)):
            raise CoordError('local Root scoped PO stop admission differs or is inactive')
        # Mode is a separate persisted control. Admission does not imply running.
        mode_row = db.execute('SELECT mode FROM po_project.agent_controls WHERE target_session=?', (po,)).fetchone()
        mode = mode_row['mode'] if mode_row else 'unknown'
        observation = db.execute('SELECT thread_id,state,turn_id,queue_count,checked_at FROM po_project.agent_control_observations WHERE target_session=?', (po,)).fetchone()
        now = time.time()
        if observation:
            checked_at = observation['checked_at']
            if (isinstance(checked_at, (int, float)) and not isinstance(checked_at, bool)
                    and math.isfinite(checked_at)):
                raw_age = now - checked_at
                age = max(0.0, raw_age)
                fresh = -5 <= raw_age <= 120 and observation['thread_id'] == thread
                observed = {
                    'checked_at': checked_at, 'age_seconds': age,
                    'state': observation['state'] if fresh else 'unknown',
                    'queue_count': observation['queue_count'] if fresh else None,
                    'turn_id': observation['turn_id'] if fresh else None,
                }
            else:
                observed = {'checked_at': None, 'age_seconds': None, 'state': 'unknown',
                            'queue_count': None, 'turn_id': None}
        else:
            observed = {'checked_at': observation['checked_at'] if observation else None,
                        'age_seconds': None, 'state': 'unknown', 'queue_count': None, 'turn_id': None}
        result = {'project_id': project, 'po_session': po, 'thread': thread, 'host': po_host,
                  'stop_id': stop_id, 'admission_effective': True, 'stored_mode': mode,
                  'observation': observed}
        # Attached WAL reads can acquire snapshots at different times. Accept
        # the result only if none of those databases changed during the read.
        db.commit()
        after_versions = {schema: db.execute(f'PRAGMA {schema}.data_version').fetchone()[0]
                          for schema in schemas}
        if after_versions != before_versions:
            raise CoordError('local Root scoped PO data changed during read; retry when stable')
        if include_internal:
            return result, dict(project_root=enrollment['session_id'], project_id=project,
                target_session=po, thread_id=thread, host_id=po_host, stop_id=stop_id,
                home_project=home_project, home_session=session, home_thread=root['thread_id'])
        return result
    finally:
        db.close()


def observe_root_attachment(*args, **kwargs):
    from .root_admin_attachment import observe
    return observe(*args, **kwargs)


def execute(operation, params, *, master_db, service_plist, home_project, session, host_id):
    if not isinstance(operation, str) or operation not in FIELDS or not isinstance(params, dict):
        raise CoordError('unsupported local Root method')
    optional = ({'cwd', 'workspaces'} if operation == 'adopt' else
                {'workspaces'} if operation == 'configure' else
                {'parked_stop'} if operation == 'direct-create' else set())
    if operation == 'status':
        scope = FIELDS['status']
        valid_fields = set(params) in (set(), scope)
    else:
        valid_fields = FIELDS[operation] <= set(params) <= FIELDS[operation] | optional
    if not valid_fields:
        raise CoordError('invalid local Root method fields')
    if operation == 'status':
        scope = {'project_id', 'expected_po_session', 'expected_thread', 'expected_host', 'expected_stop_id'}
        if set(params) & scope and not scope <= set(params):
            raise CoordError('local Root scoped PO status requires the complete scope tuple')
        if scope <= set(params):
            try:
                uuid.UUID(params['expected_stop_id'])
            except (ValueError, TypeError, AttributeError) as error:
                raise CoordError('local Root scoped PO status requires an exact stop UUID') from error
    for field in ('project', 'project_id', 'target_project', 'agent', 'po', 'target', 'expected_po_session', 'expected_thread', 'expected_host', 'expected_stop_id', 'stop_id', 'request_id', 'admission_request_id', 'baseline_id', 'continuation_id', 'attempt_id', 'verification_id', 'reconciliation_id', 'retirement_id', 'cancellation_id', 'message_id', 'job_id', 'inspection_request_id', 'source_message_id', 'source_artifact_name', 'source_artifact_sha256', 'source_limit'):
        if field in params:
            _text(params[field], field)
    if 'message_ids' in params:
        ids = params['message_ids']
        team_reset = operation in ('team-reset-preview', 'team-reset')
        minimum, maximum = (0, 16) if team_reset else (1, 20)
        if not isinstance(ids, list) or not minimum <= len(ids) <= maximum:
            if team_reset:
                raise CoordError('local Root team reset requires 0 to 16 exact operator message IDs')
            raise CoordError('local Root delivery requires 1 to 20 exact message IDs')
        for message_id in ids:
            _text(message_id, 'message ID')
        if len(set(ids)) != len(ids):
            raise CoordError('local Root delivery message IDs must be distinct')
    if operation == 'team-reset':
        digest = params['expected_digest']
        if not isinstance(digest, str) or len(digest) != 64 or any(char not in '0123456789abcdef' for char in digest):
            raise CoordError('local Root team reset requires an exact preview SHA256 digest')
    if operation == 'startup-plan':
        selected = params.get('projects')
        if (not isinstance(selected, list) or not 1 <= len(selected) <= 64
                or any(not isinstance(item, str) for item in selected)
                or len(set(selected)) != len(selected)):
            raise CoordError('local Root startup plan requires 1 to 64 distinct project IDs')
        for item in selected:
            _text(item, 'project ID')
    if operation == 'capacity-admit':
        try:
            uuid.UUID(params['request_id'])
        except (ValueError, TypeError, AttributeError) as error:
            raise CoordError('capacity admission request_id must be a UUID') from error
        _text(params['authorization_source_id'], 'authorization source ID')
        if not params['authorization_source_id'].startswith('message_') or params['confirm'] is not True:
            raise CoordError('capacity admission requires an exact authorization message ID and --confirm')
    for field in ('request_id', 'admission_request_id', 'baseline_id', 'continuation_id', 'attempt_id', 'verification_id', 'reconciliation_id', 'retirement_id', 'cancellation_id', 'stop_id', 'inspection_request_id'):
        if field in params:
            try:
                uuid.UUID(params[field])
            except ValueError as error:
                raise CoordError('local Root ' + field + ' must be a UUID') from error
    if 'confirm' in params and params['confirm'] is not True:
        raise CoordError('local Root mutation requires explicit confirmation')
    if operation == 'adopt':
        from .projects import Projects
        members = Projects.workspace_list(params.get('workspaces', [params.get('cwd')]))
        if params.get('cwd') is not None and params['cwd'] != members[0]:
            raise CoordError('cwd must equal the first project workspace')
    if operation in ('investigation-admit', 'investigation-preflight', 'investigation-settings-continuation'):
        expires_at = params['expires_at']
        if isinstance(expires_at, bool) or not isinstance(expires_at, (int, float)) or not math.isfinite(expires_at):
            raise CoordError('local Root investigation expiry must be a finite Unix timestamp')
    for value, label in ((home_project, 'home project'), (session, 'session'), (host_id, 'host')):
        _text(value, label)
    if operation == 'capacity-successor-native02-readback' and (home_project, session, host_id) != (
            'default', 'session_tcnSV4Qo3EH4YU3gg2swqjO_GS1QQ9Fv', 'host_o6KZLd0kkjchCaH8OjoJZ9aZXHDlFiE2'):
        raise CoordError('fixed successor readback Root identity differs')
    if operation == 'retire-obsolete-d6b4' and (home_project, session, host_id, params['confirm']) != (
            'default', 'session_tcnSV4Qo3EH4YU3gg2swqjO_GS1QQ9Fv',
            'host_o6KZLd0kkjchCaH8OjoJZ9aZXHDlFiE2', True):
        raise CoordError('fixed d6b4 retirement Root identity or confirmation differs')
    if operation == 'team-lifecycle' and params['operation'] not in ('stop-all', 'resume-all', 'stop-status'):
        raise CoordError('unsupported local Root team lifecycle operation')
    if operation in ('capacity-successor-4621', 'capacity-successor-e2d', 'capacity-successor-build01', 'capacity-successor-native02') and (home_project, session, host_id, params['confirm']) != (
            'default', 'session_tcnSV4Qo3EH4YU3gg2swqjO_GS1QQ9Fv',
            'host_o6KZLd0kkjchCaH8OjoJZ9aZXHDlFiE2', True):
        raise CoordError('fixed successor Root identity or confirmation differs')
    if operation in ('capacity-successor-native02-readback','capacity-successor-attempt-status'):
        from .native02_readback import inspect, inspect_attempt, ReadbackUnavailable, _family, failure
        if operation == 'capacity-successor-attempt-status':
            from .successor_admission import REQUESTS
            if params['request_id'] not in REQUESTS or len(params['attempt_id'])!=32 or any(c not in '0123456789abcdef' for c in params['attempt_id']):
                raise CoordError('fixed successor attempt tuple differs')
        try:
            # Check the configured family before any maintained preflight opens it.
            config = _installed_server_config()
            configured = config.master.resolve(strict=True)
            if Path(os.path.abspath(master_db)) != configured:
                raise ReadbackUnavailable('configured_identity_mismatch')
            _family(configured, allow_closed=True)
            master, enrolled = _preflight(master_db, service_plist, home_project, session, host_id, operation, params)
            owner = master.lstat().st_uid
            registry = _file(Path(str(master) + '.projects.sqlite3'), owner)
            target = _target(master, registry, 'default', owner)
            if operation == 'capacity-successor-attempt-status':
                return inspect_attempt(target,registry=registry,home_project=home_project,home_session=session,host_id=host_id,request_id=params['request_id'],attempt_id=params['attempt_id'])
            return inspect(target, registry=registry, home_project=home_project, home_session=session, host_id=host_id)
        except (ReadbackUnavailable, CoordError, ServiceError, OSError, sqlite3.Error) as error:
            return {'operation':operation, 'state':'UNKNOWN', 'request_id':params['request_id'] if operation == 'capacity-successor-attempt-status' else '4CF2D3E5-E026-4720-90CF-7E379B9DA461',
                    **failure(error), 'issuer_completion':'not asserted', 'native_state':'not observed',
                    **({'next_action':'no_replay'} if operation == 'capacity-successor-attempt-status' else {}),
                    'sql_mutation_performed':False}
    if operation.startswith('hospital-vendor-') and (home_project,session,host_id) != ('default','session_tcnSV4Qo3EH4YU3gg2swqjO_GS1QQ9Fv','host_o6KZLd0kkjchCaH8OjoJZ9aZXHDlFiE2'):
        raise CoordError('fixed hospital vendor Root identity differs')
    master, enrolled = _preflight(master_db, service_plist, home_project, session, host_id, operation, params)
    if operation == 'attach-handoff-diagnose':
        from .root_admin_handoff_diagnostic import diagnose
        registry = _file(Path(str(master) + '.projects.sqlite3'), master.lstat().st_uid)
        return diagnose(master, registry, home_project, session, host_id, params['expected_thread'],
                        resolve_project=lambda project: _target(master, registry, project, master.lstat().st_uid))
    if operation in ('attach-current', 'attach-status'):
        from .root_admin_attachment import attach, status as attachment_status
        registry = _file(Path(str(master) + '.projects.sqlite3'), master.lstat().st_uid)
        def resolve_project(project):
            return _target(master, registry, project, master.lstat().st_uid)

        if operation == 'attach-status':
            return attachment_status(master, registry, home_project, session, host_id, params['request_id'],
                                     resolve_project=resolve_project)
        return attach(master, registry, home_project, session, host_id, params, observer=observe_root_attachment,
                      resolve_project=resolve_project)
    if operation.startswith('hospital-vendor-'):
        from .hospital_vendor_binding import root_action, Refused
        from .hospital_vendor_scope import Refused as ScopeRefused
        from .capacity_admission import AdmissionRefused
        try:
            return root_action(master,operation.removeprefix('hospital-vendor-'),params)
        except (Refused,ScopeRefused,AdmissionRefused,sqlite3.Error,OSError) as error:
            raise CoordError('hospital vendor binding refused') from error
    if operation == 'retire-obsolete-d6b4':
        from .d6b4_retirement import retire, RetirementRefused
        try:
            return retire(master, home_project=home_project, home_session=session,
                          host_id=host_id, enrolled=enrolled)
        except RetirementRefused as error:
            raise CoordError('d6b4 retirement refused: ' + error.code) from error
    if operation == 'capacity-observe':
        # Private observer-only admission check: no RootOrchestrator lifecycle
        # constructor, job, stop, audit write, claim mutation or native input.
        from .capacity_observer import observe
        owner = master.lstat().st_uid
        registry = _file(Path(str(master) + '.projects.sqlite3'), owner)
        target = _target(master, registry, params['project'], owner)
        return observe(master, registry, target, home_project, session, host_id, params['project'])
    if operation in ('capacity-successor-4621', 'capacity-successor-e2d', 'capacity-successor-build01', 'capacity-successor-native02'):
        from .successor_admission import issue, Refused, DiagnosticRefused, PendingSuccessorRefused, REQUEST, E2D_REQUEST, BUILD_REQUEST, NATIVE02_REQUEST, BUILD_SOURCE
        from .capacity_admission import AdmissionRefused
        from .capacity_observer import Blocked
        owner = master.lstat().st_uid
        registry = _file(Path(str(master) + '.projects.sqlite3'), owner)
        target = _target(master, registry, 'default', owner)
        try:
            return issue(target, master=master, registry=registry, home_project=home_project,
                home_session=session, host_id=host_id, target_project='default',
                request_id=NATIVE02_REQUEST if operation == 'capacity-successor-native02' else BUILD_REQUEST if operation == 'capacity-successor-build01' else E2D_REQUEST if operation == 'capacity-successor-e2d' else REQUEST,
                authorization_source_id=BUILD_SOURCE if operation in ('capacity-successor-build01', 'capacity-successor-native02') else 'message_KJqomMYywtU6GYfczB_fxftks4D_gz6y', confirm=True)
        except DiagnosticRefused as error:
            raise CoordError(f'fixed successor admission {error.outcome}; request_id={error.request_id}; attempt_id={error.attempt_id or "unavailable"}; phase={error.phase}; predicate={error.predicate}; inspect exact readback; no replay') from error
        except (Refused, AdmissionRefused, Blocked) as error:
            if operation == 'capacity-successor-native02' and isinstance(error, PendingSuccessorRefused):
                raise CoordError(f'fixed successor admission refused; request_id={NATIVE02_REQUEST}; phase=transaction; predicate=successor packet still pending; requested admission transaction rolled back; inspect the existing exact-request readback before any further action') from error
            raise CoordError('fixed successor admission refused') from error
    if operation == 'capacity-admit':
        from .capacity_admission import admit_current
        owner = master.lstat().st_uid
        registry = _file(Path(str(master) + '.projects.sqlite3'), owner)
        target = _target(master, registry, params['project'], owner)
        return admit_current(target, master=master, registry=registry, home_project=home_project,
            home_session=session, host_id=host_id, target_project=params['project'],
            request_id=params['request_id'], authorization_source_id=params['authorization_source_id'],
            confirm=params['confirm'])
    if operation == 'worker-native-observe':
        from .worker_native_observation import observe
        return observe(master, home_project, session, host_id, params)
    if operation == 'worker-admission-intent':
        from .worker_admission_intent import admit
        return admit(master, home_project, session, host_id, params)
    if operation == 'worker-admission-enroll':
        from .worker_admission_intent import enroll as enroll_worker_intent
        return enroll_worker_intent(master, home_project, session, host_id, params)
    if operation == 'worker-admission-enrollment-status':
        from .worker_admission_intent import enrollment_status
        return enrollment_status(master, home_project, session, host_id, params)
    if operation == 'worker-admission-status':
        from .worker_admission_intent import status as worker_intent_status
        return worker_intent_status(master, home_project, session, host_id, params)
    if operation in ('managed-native-read', 'managed-native-diagnose'):
        _target(master, _file(Path(str(master) + '.projects.sqlite3'), master.lstat().st_uid),
                params['target_project'], master.lstat().st_uid)
    if operation == 'status' and not enrolled:
        return {'enrolled': False}
    if operation in ('status', 'po-status-inspect') and 'project_id' in params:
        status, scope = _scoped_po_control_status(master, home_project, session, host_id, params,
                                                  include_internal=True)
        if operation == 'status':
            return status
        project_path = master if scope['project_id'] == 'default' else master.parent / (master.name + '.projects') / scope['project_id'] / 'state.sqlite3'
        _file(project_path, master.lstat().st_uid)
        from .agent_control import AgentControlState
        def scope_guard():
            _, current_scope = _scoped_po_control_status(master, home_project, session, host_id, params,
                                                         include_internal=True)
            if current_scope != scope:
                raise CoordError('local Root PO inspection scope changed before request commit')
        with _write_existing(project_path) as db:
            return AgentControlState.request_po_status_inspection(
                db, scope, params['request_id'], scope_guard=scope_guard)
    if operation == 'pending-fresh':
        from .root_pending_fresh import project_pending
        return project_pending(master,home_project,session,host_id,params['expected_thread'])
    if operation == 'policy-effective-status':
        return _policy_effective_status(master, home_project, session, host_id, params['expected_thread'])
    if operation == 'delivery-metadata':
        return _delivery_metadata(master, home_project, session, host_id, params['expected_thread'], params['message_ids'])
    if operation == 'managed-native-diagnose':
        # Diagnosis shares the foreground guard evaluator but never initializes
        # audit schema, records a request, or constructs the provider client.
        from .root_orchestrator import RootOrchestrator
        owner = master.lstat().st_uid
        registry = _file(Path(str(master) + '.projects.sqlite3'), owner)
        class LocalRoot(RootOrchestrator):
            def path(self, project):
                return _target(master, registry, project, owner)
        roots = LocalRoot.__new__(LocalRoot)
        roots.projects = SimpleNamespace(path=registry, root_lock=threading.RLock(),
            db_path=lambda project: _target(master, registry, project, owner))
        result = roots.managed_native_diagnose(home_project, session, host_id,
            params['target_project'], params['po'], params['target'], params['expected_thread'],
            params['expected_host'], params['stop_id'], params['admission_request_id'],
            params['message_id'], params['job_id'], params['inspection_request_id'], params['request_id'])
        return _managed_native_diagnosis_result(result)
    if operation == 'managed-native-read':
        # This foreground path must validate first and initialize only its own
        # audit table. Projects/RootOrchestrator constructors run broad DDL.
        from .root_orchestrator import RootOrchestrator
        owner = master.lstat().st_uid
        registry = _file(Path(str(master) + '.projects.sqlite3'), owner)

        class LocalRoot(RootOrchestrator):
            def path(self, project):
                return _target(master, registry, project, owner)

        roots = LocalRoot.__new__(LocalRoot)
        roots.projects = SimpleNamespace(path=registry, root_lock=threading.RLock(),
            db_path=lambda project: _target(master, registry, project, owner))
        return roots.managed_native_read(home_project, session, host_id,
            params['target_project'], params['po'], params['target'], params['expected_thread'],
            params['expected_host'], params['stop_id'], params['admission_request_id'],
            params['message_id'], params['job_id'], params['inspection_request_id'], params['request_id'])
    from .projects import Projects
    from .root_orchestrator import RootOrchestrator
    owner = master.lstat().st_uid
    registry = Path(str(master) + '.projects.sqlite3')

    class LocalRoot(RootOrchestrator):
        def path(self, project):
            # sync and team/fresh workflows may reach projects other than the
            # nominated target. Validate every actual open, not only preflight.
            return _target(master, registry, project, owner)

    if operation == 'investigation-preflight':
        # This operation is strictly read-only. Avoid Projects and RootOrchestrator
        # constructors, whose normal setup paths initialize schemas.
        roots = LocalRoot.__new__(LocalRoot)
        roots.projects = SimpleNamespace(path=registry, root_lock=threading.RLock(),
            db_path=lambda project: _target(master, registry, project, owner))
        result = roots.investigation_preflight(home_project, session, host_id,
            params['target_project'], params['po'], params['target'], params['expected_thread'],
            params['expected_host'], params['stop_id'], params['request_id'], params['message_id'],
            params['inspection_request_id'], params['expires_at'])
        return _investigation_preflight_result(result)

    if operation == 'readiness':
        # This transport's same-UID provider trust does not attest the named
        # selector. Reuse exactly the machine-read projection without schema
        # initialization, sync, routing resolution or writable project access.
        class LocalReadiness(LocalRoot):
            def __init__(self):
                self.projects = SimpleNamespace(path=registry)

        return LocalReadiness().readiness(home_project, session, host_id,
                                         params['projects'], params['expected_thread'])
    if operation == 'startup-plan':
        # Startup-plan is a separate constructor-free metadata projection. It
        # must not pass through Projects/RootOrchestrator initialization.
        roots = LocalRoot.__new__(LocalRoot)
        roots.projects = SimpleNamespace(path=registry, root_lock=threading.RLock(),
            db_path=lambda project: _target(master, registry, project, owner))
        return roots.startup_plan(home_project, session, host_id,
                                  params['projects'], params['expected_thread'])

    roots = LocalRoot(Projects(master))
    args = (home_project, session, host_id)
    with roots.projects.root_lock:
        if operation == 'enroll':
            return roots.enroll(*args, params['expected_thread'], params['confirm'])
        roots.authorize(*args)
        if operation == 'pause-routing':
            return roots.pause_routing(*args, params['expected_thread'], params['request_id'], params['confirm'])
        if operation == 'resume-routing':
            return roots.resume_routing(*args, params['confirm'])
        if operation == 'status':
            roots.sync()
            return roots.status()
        if operation == 'active-projects':
            return roots.active_projects()
        if operation in ('in', 'route'):
            return roots.route(*args, params['project'])
        if operation == 'adopt':
            return roots.adopt(*args, params['project'], params['target'], params['expected_thread'],
                               params.get('cwd'), params['confirm'], workspaces=params.get('workspaces'))
        if operation == 'detach':
            return roots.detach(*args, params['confirm'])
        if operation in ("turnover-bootstrap", "turnover-bootstrap-reconcile", "turnover-bootstrap-continue",
                         "retire-unbound-parked", "retire-bound-parked", "turnover-bootstrap-continue-second", "turnover-admin-continue", "repair-parked-binding"):
            roots.authorize(*args)
            with roots.projects.connection() as directory:
                directory.execute('BEGIN IMMEDIATE')
                route = directory.execute('SELECT session_id FROM root_projects WHERE project_id=? AND joined=1', (params['project'],)).fetchone()
                if not route:
                    raise CoordError('turnover project is not joined')
                from .turnover_bootstrap import admit, continue_turnover, reconcile_consumed
                with contextlib.closing(Coordinator(roots.path(params['project']), route['session_id'])) as coord:
                    if operation == "repair-parked-binding":
                        from .parked_binding_repair import repair
                        if params['expected_host'] != host_id:
                            raise CoordError('parked binding repair must use the exact current provider host')
                        result = repair(coord, directory, request=params['request_id'],
                            original=params['original_request_id'], project=params['project'], stop=params['stop_id'],
                            target=params['target'], old_thread=params['expected_thread'], host=host_id,
                            confirmed=params['confirm'])
                    elif operation == "retire-bound-parked":
                        from .parked_retirement import retire_bound
                        if params['expected_host'] != host_id:
                            raise CoordError('bound parked retirement must use the exact current provider host')
                        result = retire_bound(coord, directory, project=params['project'], bootstrap_id=params['bootstrap_id'],
                            stop_id=params['stop_id'], po=params['po'], thread=params['expected_thread'], host=host_id,
                            request_id=params['request_id'], target=params['target'], agent=params['expected_agent'],
                            logical_owner=params['logical_owner'], old_thread=params['expected_bound_thread'],
                            old_rollout=params['expected_old_rollout'], repair_request=params['repair_request_id'],
                            candidate=params['expected_candidate'], confirmed=params['confirm'])
                    elif operation == "retire-unbound-parked":
                        from .parked_retirement import retire
                        result = retire(coord, directory, project=params['project'], bootstrap_id=params['bootstrap_id'],
                            stop_id=params['stop_id'], po=params['po'], thread=params['expected_thread'],
                            host=params['expected_host'], request_id=params['request_id'], confirmed=params['confirm'])
                    elif operation == "turnover-bootstrap":
                        result = admit(coord, directory, params["request_id"], params["confirm"])
                    elif operation == "turnover-bootstrap-reconcile":
                        result = reconcile_consumed(coord, directory, params["request_id"],
                                                    host_id, params["confirm"])
                    elif operation == "turnover-bootstrap-continue":
                        result = continue_turnover(coord, directory, params["request_id"],
                                                   params["message_id"], host_id, params["confirm"])
                    elif operation == 'turnover-admin-continue':
                        from .turnover_admin_continuation import continue_admin
                        result = continue_admin(coord, directory, params['request_id'], params['continuation_id'],
                            params['previous_id'], params['message_id'], params['retirement_ids'], host_id, params['confirm'])
                    else:
                        from .turnover_bootstrap import continue_turnover_second
                        result = continue_turnover_second(coord, directory, params["request_id"],
                            params["continuation_id"], params["message_id"], params["first_retirement_id"],
                            params["second_retirement_id"], host_id, params["confirm"])
                directory.commit()
            return result
        if operation == 'po-admit':
            return roots.admit_po(*args, params['project'], params['po'], params['expected_thread'],
                                  params['expected_host'], params['request_id'], params['confirm'])
        if operation == 'investigation-admit':
            return roots.admit_investigation(*args, params['target_project'], params['po'], params['target'],
                params['expected_thread'], params['expected_host'], params['stop_id'], params['request_id'],
                params['message_id'], params['inspection_request_id'], params['expires_at'], params['confirm'])
        if operation == 'investigation-preflight':
            result = roots.investigation_preflight(*args, params['target_project'], params['po'], params['target'],
                params['expected_thread'], params['expected_host'], params['stop_id'], params['request_id'],
                params['message_id'], params['inspection_request_id'], params['expires_at'])
            return _investigation_preflight_result(result)
        if operation == 'investigation-settings-continuation':
            return roots.continue_investigation_settings(*args, params['target_project'], params['po'],
                params['target'], params['expected_thread'], params['expected_host'], params['stop_id'],
                params['admission_request_id'], params['baseline_id'], params['continuation_id'],
                params['message_id'], params['inspection_request_id'], params['source_message_id'],
                params['source_artifact_name'], params['source_artifact_sha256'], params['source_limit'], params['expected_settings'],
                params['expires_at'], params['confirm'])
        if operation == 'resume-project':
            return roots.team_lifecycle(*args, operation, params['request_id'], params['confirm'],
                                        target_project=params['project'])
        if operation == 'onboard-project':
            return roots.onboard_project(*args, params['project'], params['stop_id'],
                                         params['expected_thread'], params['request_id'], params['confirm'])
        if operation in ('stop-all', 'resume-all', 'stop-status', 'team-lifecycle'):
            selected = params['operation'] if operation == 'team-lifecycle' else operation
            return roots.team_lifecycle(*args, selected, params['request_id'], params.get('confirm', True))
        if operation in ('team-reset-preview', 'team-reset', 'team-reset-status'):
            from .root_team_reset import RootTeamReset
            reset = RootTeamReset(roots)
            if operation == 'team-reset-preview':
                return reset.preview(*args, params['project'], params['message_ids'])
            if operation == 'team-reset-status':
                return reset.status(*args, params['project'], params['request_id'])
            return reset.apply(*args, params['project'], params['message_ids'], params['request_id'],
                               params['expected_digest'], params['confirm'])
        if operation in ('fresh-start', 'fresh-status', 'fresh-recover', 'fresh-verify', 'fresh-replace', 'fresh-retire', 'fresh-retire-settings-mismatch', 'fresh-cancel-pending'):
            from .root_restart import RootRestart
            restart = RootRestart(roots)
            if operation == 'fresh-status':
                return restart.authorize_status(*args, params['request_id'])
            if operation == 'fresh-recover':
                return restart.recover(*args, params['expected_thread'], params['request_id'],
                                       params['attempt_id'], params['confirm'])
            if operation == 'fresh-verify':
                return restart.verify_handoff(*args, params['expected_thread'], params['request_id'],
                                               params['verification_id'], params['confirm'])
            if operation == 'fresh-replace':
                return restart.replace_candidate(*args, params['expected_thread'], params['request_id'],
                                                 params['reconciliation_id'], params['confirm'])
            if operation == 'fresh-retire':
                return restart.retire_uncertain(*args, params['expected_thread'], params['request_id'],
                                                params['reconciliation_id'], params['retirement_id'], params['confirm'])
            if operation == 'fresh-retire-settings-mismatch':
                return restart.retire_settings_mismatch(*args, params['expected_thread'], params['request_id'],
                                                        params['retirement_id'], params['confirm'])
            if operation == 'fresh-cancel-pending':
                return restart.cancel_pending(*args, params['expected_thread'], params['request_id'],
                                              params['cancellation_id'], params['confirm'])
            return restart.request(*args, params['expected_thread'], params['prompt'], params['request_id'], params['confirm'])
        if operation in ('exact-file-bind','exact-file-binding-status','exact-file-revoke'):
            selected=dict(params);target=selected.pop('project')
            return roots.exact_file_binding(*args,target,operation,selected)
        if operation == 'delegate-management':
            return roots.delegate_management(*args, params['project'], params['po'], params['target'],
                params['expected_thread'], params['expected_host'], params['request_id'], params['confirm'])
        if operation == 'provision':
            return roots.provision(*args, params['project'], params['agent'], params['request_id'],
                params['confirm'], params['model'], params['effort'])
        if operation == 'direct-create':
            return roots.direct_create(*args, params['project'], params['reset_id'], params['agent'], params['cwd'],
                params['prompt'], params['request_id'], params['confirm'], params['model'], params['effort'], params.get('parked_stop'))
        if operation == 'retry-setup':
            return roots.retry_setup(*args, params['project'], params['request_id'], params['confirm'])
        return roots.configure(*args, params['project'], params['agent'], params['cwd'], params['prompt'],
            params['request_id'], params['confirm'], params['model'], params['effort'], workspaces=params.get('workspaces'))


def discover():
    """Read configured Root identity without adopting or proving native ownership."""
    import shlex
    config = _installed_server_config()
    try:
        owner = config.plist.lstat().st_uid
    except OSError as error:
        raise CoordError('local Root configured provider is unavailable') from error
    if os.getuid() != owner or os.geteuid() != owner:
        raise CoordError('local Root requires the configured application real and effective UID')
    for path in (config.plist, config.wrapper, config.executable):
        _file(path, owner)
    try:
        master = configured_server_database(config.plist)
    except ServiceError as error:
        raise CoordError(str(error)) from error
    if master != config.master.resolve():
        raise CoordError('local Root master differs from the canonical configured master')
    _file(master, owner)
    registry = _file(Path(str(master) + '.projects.sqlite3'), owner)
    with _read(registry) as db:
        if not _table(db, 'root_config'):
            raise CoordError('local Root registration schema is unavailable')
        row = db.execute('SELECT home_project,home_session,host_id,thread_id FROM root_config WHERE singleton=1').fetchone()
    if not row:
        return {'state': 'not_enrolled', 'attached_proven': False,
                'next_action': 'Inspect configured Root enrollment; no registration was changed.'}
    project, session, host, thread = tuple(row)
    _preflight(master, config.plist, project, session, host, 'status', {})
    with _read(registry) as db:
        current = db.execute('SELECT home_project,home_session,host_id,thread_id FROM root_config WHERE singleton=1').fetchone()
    if not current or tuple(current) != (project, session, host, thread):
        raise CoordError('local Root registration changed during discovery')
    hint = os.environ.get('CODEX_THREAD_ID')
    return {'state': 'registered', 'home_project': project, 'home_session': session,
            'host_id': host, 'binding_thread': thread,
            'conversation_relation': ('unknown' if not hint else 'same_hint' if hint == thread else 'different'),
            'conversation_hint_is_authoritative': False, 'attached_proven': False,
            'status_command': 'agent-chat-client root-local --home-project ' + shlex.quote(project)
                              + ' --home-session ' + shlex.quote(session) + ' --host-id ' + shlex.quote(host) + ' status',
            'next_action': 'Read the status above. For a different conversation inspect root-local attach-current --help and reconcile the exact peer and ownership proofs; no attachment is implied.'}


def main(argv=None):
    if argv and argv[0] == 'provider-recovery':
        from .provider_recovery_cli import main as provider_recovery_main
        return provider_recovery_main(argv[1:])
    if argv == ['discover']:
        print(json.dumps(discover(), sort_keys=True))
        return 0
    parser = argparse.ArgumentParser(prog='agent-chat-client root-local', description=__doc__,
        epilog='To find the configured retained Root without local session context, run: agent-chat-client root-local discover')
    try:
        config = _installed_server_config()
    except (ServiceError, OSError) as error:
        if argv and argv[-1] == 'capacity-successor-native02-readback':
            from .native02_readback import failure
            print(json.dumps({'operation':argv[-1], 'state':'UNKNOWN',
                              'request_id':'4CF2D3E5-E026-4720-90CF-7E379B9DA461',
                              **failure(error), 'issuer_completion':'not asserted',
                              'native_state':'not observed', 'sql_mutation_performed':False}, sort_keys=True))
            return 0
        raise
    parser.add_argument('--master-db', default=str(config.master), help='canonical configured master equality assertion')
    parser.add_argument('--service-plist', default=str(config.plist), help='canonical installed provider equality assertion')
    parser.add_argument('--home-project', required=True)
    parser.add_argument('--home-session', required=True, dest='session')
    parser.add_argument('--host-id', required=True)
    commands = parser.add_subparsers(dest='method', required=True)
    for method, fields in FIELDS.items():
        command = commands.add_parser(method, help=(
            'issue the fixed one-use HospitalNavFailureTimeCapture01 E2D packet under configured Root authority'
            if method == 'capacity-successor-e2d' else
            'one-shot Root-owned current observation and exact two-row nonordinary capacity admission'
            if method == 'capacity-admit' else
            'read exact validation-clone and heavy-unity:2 eligibility; no settlement or worker input'
            if method == 'capacity-observe' else
            'read a bounded metadata-only view of configured Root startup writes; creates no state'
            if method == 'startup-plan' else
            'metadata-only retry of the retained candidate; reuses request and attempt UUIDs, sends no boot input'
            if method == 'fresh-recover' else
            'explicitly select one retained request for bounded response and owner verification; reads no old history automatically'
            if method == 'fresh-verify' else
            'audited one-time replacement candidate for the same request; does not replay boot input or rebind routing'
            if method == 'fresh-replace' else
            'retire one exact uncertain Root fresh-start request without asserting native closure'
            if method == 'fresh-retire' else
            'retire the exact settings-mismatch request without asserting native closure'
            if method == 'fresh-retire-settings-mismatch' else
            'cancel only the exact pending Root intent without asserting native closure'
            if method == 'fresh-cancel-pending' else
            'admit one exact existing task message for bounded stopped-project investigation; does not release workers'
            if method == 'investigation-admit' else
            'read named eligibility checks for one exact existing stopped-project investigation message; creates no admission'
            if method == 'investigation-preflight' else
            'bind PO-attested historical settings and one finite continuation to an expired untouched investigation task'
            if method == 'investigation-settings-continuation' else
            'diagnose one exact managed-native-read guard without audit or provider calls'
            if method == 'managed-native-diagnose' else
            'perform one same-UID foreground metadata read for an exact admitted worker; no task input or lifecycle action'
            if method == 'managed-native-read' else None))
        for field in sorted(fields):
            option = ('--project-id' if field in ('project', 'projects') else
                      '--target-project-id' if field == 'target_project' else
                      '--message-id' if field == 'message_ids' else
                      '--retirement-id' if field == 'retirement_ids' else '--' + field.replace('_', '-'))
            if field in ('projects', 'message_ids', 'retirement_ids'):
                team_reset = method in ('team-reset-preview', 'team-reset') and field == 'message_ids'
                command.add_argument(option, dest=field, action='append', required=not team_reset,
                                     default=[] if team_reset else None)
            elif field == 'confirm':
                command.add_argument(option, action='store_true')
            elif field in ('model', 'effort'):
                command.add_argument(option)
            elif field in ('expires_at','minutes'):
                command.add_argument(option, type=float, required=(field=='minutes'))
            elif field == 'max_bytes':
                command.add_argument(option,type=int,required=True)
            elif field == 'agent':
                command.add_argument(option, default='orchestrator')
            elif field == 'expected_settings':
                command.add_argument('--expected-settings-file', dest='expected_settings_file', required=True)
            else:
                command.add_argument(option, dest=field, required=(method != 'status'))
        if method in ('configure', 'adopt'):
            command.add_argument('--workspace', dest='workspaces', action='append')
        if method == 'adopt':
            command.add_argument('--cwd')
        if method == 'direct-create':
            command.add_argument('--parked-stop', help='exact active stop UUID; bind a fresh PO without submitting input')
    values = vars(parser.parse_args(argv))
    method = values.pop('method')
    identity = {key: values.pop(key) for key in ('master_db', 'service_plist', 'home_project', 'session', 'host_id')}
    if method == 'status':
        values = {key: value for key, value in values.items() if value is not None}
    if values.get('workspaces') is None:
        values.pop('workspaces', None)
    if values.get('parked_stop') is None:
        values.pop('parked_stop', None)
    if 'expected_settings_file' in values:
        source = Path(values.pop('expected_settings_file'))
        try:
            raw = source.read_text(encoding='utf-8')
            if len(raw.encode('utf-8')) > 65536:
                raise CoordError('expected settings file exceeds the bounded size')
            values['expected_settings'] = json.loads(raw)
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            raise CoordError('cannot read the expected settings JSON file') from error
    result = execute(method, values, **identity)
    if method in ('fresh-status', 'fresh-recover', 'fresh-verify', 'fresh-replace', 'fresh-retire', 'fresh-retire-settings-mismatch', 'fresh-cancel-pending'):
        result = fresh_status_notice(result)
    print(json.dumps(result, sort_keys=True))
    return 0
