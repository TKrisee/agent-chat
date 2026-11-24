"""Exact legacy target LOCAL installed-custodian quarantine, never physical release.

The installed account/control trust domain is inherited from custody(); this is
not a remote owner credential or isolation from hostile same-UID processes.
"""
import argparse
import hashlib
import json
from pathlib import Path
import sqlite3
import time
import uuid

from .core import CoordError
from . import custodian_recovery as custody_contract
from .guard_generation import guard_generation
from .root_orchestrator import projection

TARGET = dict(custody_contract.TARGET,
    owner_session='session_d96238daad0345b699ec1dd1972ab11b',
    thread_id='01a11d1f-bb15-7750-affe-3029d7b4057d')
POLICY = 'exact-original-owner-failed-quarantine-v1'
FENCE_ERROR = 'original owner disposition quarantine is active; future admission withheld'


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest(value):
    return hashlib.sha256(encoded(value).encode()).hexdigest()


def present(db):
    return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='original_owner_dispositions'").fetchone())


def _schema(db):
    db.execute('''CREATE TABLE IF NOT EXISTS original_owner_dispositions (
        request_id TEXT PRIMARY KEY, owner_session TEXT NOT NULL,
        reservation_id TEXT UNIQUE NOT NULL, target_json TEXT NOT NULL,
        payload_json TEXT NOT NULL, evidence_json TEXT NOT NULL,
        result_json TEXT NOT NULL, recorded_at REAL NOT NULL)''')
    for action in ('UPDATE', 'DELETE'):
        db.execute(f'''CREATE TRIGGER IF NOT EXISTS original_owner_dispositions_no_{action.lower()}
            BEFORE {action} ON original_owner_dispositions BEGIN
            SELECT RAISE(ABORT,'original owner disposition is immutable'); END''')


def _capture(db, boundary):
    target = dict(TARGET)
    if target['project'] != 'default' or boundary['project'] != target['project']:
        raise CoordError('fixed disposition project differs from configured custody')
    host = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?',
                      (target['owner_session'],)).fetchone()
    binding = db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?',
                         (target['owner_session'],)).fetchone()
    if not host or not binding or binding['thread_id'] != target['thread_id']:
        raise CoordError('exact original owner host/thread association unavailable or changed')
    target['host_id'] = host['host_id']
    if projection(db, target['owner_session']):
        raise CoordError('global root cannot disposition resource work')
    prior = db.execute('SELECT target_json FROM original_owner_dispositions WHERE reservation_id=?',
        (target['reservation_id'],)).fetchone() if present(db) else None
    try:
        custody_contract._target_actor(db, target['owner_session'], target['host_id'])
    except CoordError as error:
        # All existing actor/lifecycle checks precede the new central fence.
        # A local admin may inspect/replay its exact quarantine, never execute it.
        if str(error) != FENCE_ERROR or not prior or prior['target_json'] != encoded(target):
            raise
    row = db.execute('SELECT * FROM resources WHERE name=?', (target['resource'],)).fetchone()
    run = db.execute('SELECT * FROM remote_guarded_runs WHERE run_id=?', (target['run_id'],)).fetchone()
    if (not row or row['owner_session'] != target['owner_session'] or
            row['reservation_id'] != target['reservation_id'] or row['deadline'] is None or
            row['deadline'] >= time.time() or not row['stale']):
        raise CoordError('exact durable stale original reservation unavailable')
    if (not run or run['session_id'] != target['owner_session'] or run['host_id'] != target['host_id'] or
            run['resource'] != target['resource'] or run['reservation_id'] != target['reservation_id'] or
            run['closed_at'] is not None):
        raise CoordError('exact original OPEN guard association unavailable')
    outcome = db.execute('SELECT * FROM remote_reservation_outcomes WHERE reservation_id=?',
                         (target['reservation_id'],)).fetchone()
    if not outcome or (outcome['resource'], outcome['owner_session'], outcome['host_id'], outcome['state']) != (
            target['resource'], target['owner_session'], target['host_id'], 'pending'):
        raise CoordError('exact original pending outcome association differs')
    tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    jobs = [dict(r) for r in db.execute('SELECT id,status,queue_id FROM bridge_jobs WHERE thread_id=? ORDER BY id',
            (target['thread_id'],))] if 'bridge_jobs' in tables else []
    controls = [dict(r) for r in db.execute('SELECT id,status FROM agent_control_requests WHERE target_session=? ORDER BY id',
                (target['owner_session'],))] if 'agent_control_requests' in tables else []
    resets = [dict(r) for r in db.execute('SELECT id,status FROM session_resets WHERE target_session=? ORDER BY id',
              (target['owner_session'],))] if 'session_resets' in tables else []
    handoffs = [dict(r) for r in db.execute('''SELECT * FROM root_native_handoffs
        WHERE session_id=? OR thread_id=? ORDER BY kind,job_id,phase''',
        (target['owner_session'], target['thread_id']))] if 'root_native_handoffs' in tables else []
    historical = dict(association='UNKNOWN', records=[])
    if 'admin_ownership_dispositions' in tables:
        columns = {r['name'] for r in db.execute('PRAGMA table_info(admin_ownership_dispositions)')}
        needed = {'request_id','resource','reservation_id','owner_session','outcome','process_evidence_state'}
        if needed <= columns:
            historical = dict(association='stored_exact_tuple', records=[dict(r) for r in db.execute(
                '''SELECT request_id,outcome,process_evidence_state FROM admin_ownership_dispositions
                WHERE resource=? AND reservation_id=? AND owner_session=? ORDER BY request_id''',
                (target['resource'], target['reservation_id'], target['owner_session']))])
    resource = dict(row)
    resource.pop('token', None)  # Old capability is never exported/reconstructed.
    state = dict(target=target, resource=resource, original_run=dict(run),
                 original_outcome=dict(outcome), bridge_jobs=jobs, controls=controls, resets=resets,
                 historical_dispositions=historical, native_handoffs=handoffs,
                 guard_generation=guard_generation(db), boundary_sha256=boundary['boundary_sha256'])
    return state, digest(state)


def _return_checks(db, state):
    target = state['target']; run = state['original_run']
    blockers = ['no physical-return mutation selected',
                'current whole-domain native continuation containment is unproved',
                'historical workspace preimages/restoration remain UNKNOWN']
    if any(h['active'] for h in state['native_handoffs']):
        blockers.append('already admitted native handoff remains unresolved')
    if any(j['status'] not in ('dispatched', 'cancelled') for j in state['bridge_jobs']):
        blockers.append('queued/pending/uncertain App native continuation remains')
    if any(j['status'] not in ('completed', 'failed', 'cancelled', 'resolved_unchanged')
           for j in state['controls'] + state['resets']):
        blockers.append('control/reset continuation remains pending or uncertain')
    trusted = False
    tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    if {'lifecycle_producers', 'execution_instances', 'current_instance_closures'} <= tables:
        fact = db.execute('''SELECT i.*,p.enrolled_at,c.evidence,c.evidence_sha256,c.recorded_at
            FROM execution_instances i JOIN lifecycle_producers p ON p.id=i.producer_id
            JOIN current_instance_closures c ON c.instance_id=i.id WHERE i.run_id=?
            AND p.owner_session=? AND p.host_id=?''',
            (target['run_id'], target['owner_session'], target['host_id'])).fetchone()
        trusted = bool(fact and run['local_pid'] > 0 and
            fact['enrolled_at'] <= run['started_at'] <= fact['established_at'] <= fact['recorded_at'] <= time.time() and
            (fact['resource'], fact['reservation_id'], fact['owner_session'], fact['host_id']) ==
            (target['resource'], target['reservation_id'], target['owner_session'], target['host_id']) and
            custody_contract.closure_matches(fact, run))
    if not trusted:
        blockers.append('original trusted entire-execution closure unavailable; no retrospective enrollment/PID inference')
    return dict(eligible=False, physical_return_supported=False, blockers=blockers,
                trusted_original_closure_available=trusted, current_process_state='UNKNOWN',
                already_queued_or_racing_effects='UNKNOWN', restoration='UNKNOWN',
                resources_returned=False, native_admission=False)


def execute(operation, *, request_id=None, expected_prestate=None, authority_reference=None):
    if operation not in ('preflight', 'quarantine-failed', 'return-preflight'):
        raise CoordError('unsupported original owner disposition operation; no physical return')
    boundary = custody_contract._boundary()
    readonly = operation != 'quarantine-failed'
    db = sqlite3.connect(Path(boundary['store']).as_uri() + ('?mode=ro' if readonly else '?mode=rw'),
                         uri=True, timeout=3)
    db.row_factory = sqlite3.Row
    try:
        if readonly: db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN' if readonly else 'BEGIN IMMEDIATE')
        state, pin = _capture(db, boundary)
        if operation == 'return-preflight':
            return dict(operation=operation, target=state['target'], prestate_sha256=pin,
                        **_return_checks(db, state))
        if readonly:
            return dict(operation=operation, target=state['target'], prestate_sha256=pin,
                        guard_generation=state['guard_generation'],
                        already_quarantined=present(db) and bool(db.execute(
                            'SELECT 1 FROM original_owner_dispositions WHERE reservation_id=?',
                            (TARGET['reservation_id'],)).fetchone()),
                        effect='future App admission fence only; OPEN/holds/FIFO/UNKNOWN preserved')
        try:
            if str(uuid.UUID(request_id)) != request_id: raise ValueError()
        except (ValueError, TypeError, AttributeError):
            raise CoordError('request_id must be a canonical UUID')
        if (not isinstance(authority_reference, str) or not 1 <= len(authority_reference) <= 512 or
                authority_reference != authority_reference.strip()):
            raise CoordError('bounded nonsecret custodian authority reference required')
        payload = encoded(dict(target=state['target'], expected_prestate=expected_prestate,
                               authority_reference=authority_reference, policy=POLICY,
                               actor=boundary['actor'], boundary_sha256=boundary['boundary_sha256']))
        previous = db.execute('SELECT * FROM original_owner_dispositions WHERE request_id=?',
                               (request_id,)).fetchone() if present(db) else None
        if previous:
            if pin != expected_prestate:
                raise CoordError('original owner disposition current generation/prestate drift')
            if previous['payload_json'] != payload:
                raise CoordError('original owner disposition replay conflicts with immutable payload')
            custody_contract._same_boundary(boundary)
            return json.loads(previous['result_json'])
        if pin != expected_prestate:
            raise CoordError('original owner disposition prestate drift')
        if present(db) and db.execute('SELECT 1 FROM original_owner_dispositions WHERE reservation_id=?',
                                     (TARGET['reservation_id'],)).fetchone():
            raise CoordError('original reservation already has an immutable disposition')
        custody_contract._target_fresh(db, TARGET['owner_session'])
        result = dict(request_id=request_id, policy=POLICY, actor=boundary['actor'], target=state['target'],
            outcome='failed_abandoned_quarantine', future_app_admission_fenced=True,
            already_queued_or_racing_effects='UNKNOWN', process_state='UNKNOWN',
            restoration='UNKNOWN', original_guard='OPEN', resources_returned=False,
            native_admission=False, receipt_created=False)
        _schema(db)
        db.execute('INSERT INTO original_owner_dispositions VALUES(?,?,?,?,?,?,?,?)',
            (request_id, TARGET['owner_session'], TARGET['reservation_id'], encoded(state['target']),
             payload, encoded(state), encoded(result), time.time()))
        custody_contract._same_boundary(boundary)
        db.commit()
        return result
    except CoordError:
        raise
    except (sqlite3.Error, RuntimeError) as error:
        raise CoordError('original owner disposition state/schema unavailable or transaction refused') from error
    finally:
        db.rollback(); db.close()
        if readonly: custody_contract._same_boundary(boundary)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=('preflight', 'quarantine-failed', 'return-preflight'))
    parser.add_argument('--request-id'); parser.add_argument('--expected-prestate')
    parser.add_argument('--authority-reference')
    args = vars(parser.parse_args(argv))
    try:
        print(encoded(execute(**args)))
        return 0
    except (CoordError, OSError, ValueError) as error:
        print(encoded(dict(error=str(error))))
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
