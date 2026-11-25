"""Fixed-target LOCAL SERVER ACCOUNT administrative recovery prototype.

Not an owner RPC, remote bearer endpoint, or hostile-same-UID boundary. Local
principals controlling the installed account/store/process are explicitly trusted.
No target/process evidence is manufactured by administrative authority.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import pwd
import re
import secrets
import sqlite3
import stat
import subprocess
import sys
import time

from .core import CoordError, assert_agent_running, assert_reset_unlocked
from . import service
from .guard_generation import guard_generation
from .root_team_reset import quarantined
from .root_orchestrator import projection

POLICY = 'local-server-custodian-stored-closure-v1'
TARGET = {'project': 'default', 'resource': 'validation-clone',
          'reservation_id': 'reservation_ozTM0ysovUwnzjB95HWDce4hRIw4bP__',
          'run_id': 'remote_run_d6b4b581f256e9f9c2b0ed9954221b3f'}
ROUTING_ENV = ('AGENT_CHAT_DB', 'AGENT_CHAT_PROJECT', 'AGENT_CHAT_SERVER')
DENIED = 'fixed local custodian boundary or conditional recovery unavailable'


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)

def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()

# Pure target/evidence helpers derived from frozen-v3 recovery_abandonment.py
# SHA256 0f891992500bbb29fbabe078b64eaa53edf7a33fabe832a2aba3ddffc50e4366.
# No unintegrated-v3 module import or owner-authenticated RPC is required.
def _target_actor(db, session, host):
    if (not db.execute('SELECT 1 FROM sessions WHERE id=?', (session,)).fetchone()
            or db.execute('SELECT 1 FROM retired_sessions WHERE id=?', (session,)).fetchone()
            or quarantined(db, session=session)):
        raise CoordError('recovery requires an active registered owner host')
    row = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (session,)).fetchone()
    if not row or row['host_id'] != host:
        raise CoordError('recovery requires the exact registered owner host')
    assert_reset_unlocked(db, session)
    assert_agent_running(db, session)
    if projection(db, session):
        raise CoordError('global root cannot recover resource work')


def _target_fresh(db, session):
    # Same full-consumption contract as Coordinator._fresh, with no read marker write.
    if db.execute("SELECT 1 FROM messages m WHERE m.recipient_session=? AND NOT EXISTS "
                  "(SELECT 1 FROM message_reads r WHERE r.message_id=m.id AND "
                  "r.reader_session=m.recipient_session AND r.provenance IN "
                  "('recipient_full_message','recipient_context_full')) AND NOT "
                  "(m.acked_at IS NOT NULL AND m.acked_at <= COALESCE(CAST((SELECT value "
                  "FROM meta WHERE key='message_delivery_archive_before') AS REAL),-1)) LIMIT 1",
                  (session,)).fetchone():
        raise CoordError('read your inbox after the newest message before changing ownership')


def closure_matches(fact, run):
    if fact is None:
        return False
    data = bytes(fact['evidence'])
    try:
        lines = data.decode('ascii').splitlines()
        code = int(lines[3].removeprefix('returncode='))
    except (UnicodeError, ValueError, IndexError):
        return False
    expected = ('instance=' + fact['id'] + '\nrun=' + run['run_id'] + '\npid=' + str(run['local_pid'])
                + '\nreturncode=' + str(code) + '\ngroup_closed=true\n').encode()
    return data == expected and hashlib.sha256(data).hexdigest() == fact['evidence_sha256']


def _snapshot(db, session, host, project, resource, reservation, now):
    _target_actor(db, session, host)
    row = db.execute('SELECT * FROM resources WHERE name=?', (resource,)).fetchone()
    if not row or row['owner_session'] != session or row['reservation_id'] != reservation:
        raise CoordError('recovery requires the exact owned reservation')
    if row['deadline'] is None or row['deadline'] >= now:
        raise CoordError('recovery requires an expired reservation')
    runs = [dict(r) for r in db.execute('SELECT * FROM remote_guarded_runs WHERE reservation_id=? ORDER BY run_id', (reservation,))]
    if not runs or not any(r['closed_at'] is None for r in runs):
        raise CoordError('recovery requires an open guarded run')
    if db.execute('SELECT 1 FROM guarded_runs WHERE reservation_id=? AND closed_at IS NULL', (reservation,)).fetchone():
        raise CoordError('local guarded execution is outside this recovery evidence')
    outcome = db.execute('SELECT * FROM remote_reservation_outcomes WHERE reservation_id=?', (reservation,)).fetchone()
    if not outcome or (outcome['resource'], outcome['owner_session'], outcome['host_id'], outcome['state']) != (resource, session, host, 'pending'):
        raise CoordError('original reservation outcome association is inconsistent')
    state = dict(row)
    # Never disclose the old private capability, even to its owner.
    state['token'] = hashlib.sha256((state['token'] or '').encode()).hexdigest()
    evidence = []
    available = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    eligible = {'execution_instances', 'current_instance_closures', 'lifecycle_producers'} <= available
    for run in runs:
        if (run['session_id'], run['host_id'], run['resource']) != (session, host, resource):
            raise CoordError('guard history differs from exact owner host reservation')
        if run['closed_at'] is not None:
            continue
        fact = db.execute('''SELECT i.*,p.enrolled_at,c.id closure_id,c.evidence,
            c.evidence_sha256,c.recorded_at FROM execution_instances i
            JOIN lifecycle_producers p ON p.id=i.producer_id
            JOIN current_instance_closures c ON c.instance_id=i.id
            WHERE i.run_id=? AND p.owner_session=? AND p.host_id=?''',
                          (run['run_id'], session, host)).fetchone() if eligible else None
        if (not fact or run['local_pid'] <= 0 or fact['enrolled_at'] > run['started_at']
                or fact['established_at'] < run['started_at']
                or fact['recorded_at'] < fact['established_at']
                or (fact['resource'], fact['reservation_id'], fact['owner_session'], fact['host_id'])
                != (resource, reservation, session, host)
                or fact['recorded_at'] > now or not closure_matches(fact, run)):
            eligible = False
            continue
        evidence.append({k: (bytes(fact[k]).decode() if k == 'evidence' else fact[k]) for k in fact.keys()})
    result = {'project': project, 'owner_session': session, 'host_id': host,
              'resource': state, 'runs': runs, 'closures': evidence,
              'original_outcome': dict(outcome), 'guard_generation': guard_generation(db), 'eligible': eligible}
    return result




def _acl(path):
    if sys.platform == 'darwin':
        result = subprocess.run(['/bin/ls', '-lde', str(path)], capture_output=True, text=True, check=False)
        if result.returncode:
            raise CoordError('ACL metadata unavailable')
        entries = []
        for line in result.stdout.splitlines()[1:]:
            match = re.fullmatch(r'\s*\d+:\s+(user|group):(\S+)\s+(?:inherited\s+)?(allow|deny)\s+(.+)', line)
            if not match:
                raise CoordError('unsupported ACL metadata')
            kind, principal, effect, rights = match.groups()
            entries.append(dict(kind=kind, principal=principal, effect=effect, rights=sorted(rights.split(','))))
        return entries
    if sys.platform.startswith('linux'):
        if any(name.startswith('system.posix_acl_') for name in os.listxattr(path, follow_symlinks=False)):
            raise CoordError('POSIX ACL present but interpretation unsupported')
        return []
    raise CoordError('ACL platform unsupported')


def _metadata(path):
    before = path.lstat()
    acl = _acl(path) if not stat.S_ISLNK(before.st_mode) else []
    after = path.lstat()
    if (before.st_dev, before.st_ino, before.st_uid, before.st_mode) != (after.st_dev, after.st_ino, after.st_uid, after.st_mode):
        raise CoordError('path metadata changed during acquisition')
    return dict(path=str(path), device=before.st_dev, inode=before.st_ino, owner_uid=before.st_uid,
                mode=stat.S_IMODE(before.st_mode), symlink=stat.S_ISLNK(before.st_mode),
                type='directory' if stat.S_ISDIR(before.st_mode) else 'regular' if stat.S_ISREG(before.st_mode) else 'unsupported', acl=acl)


def _acl_safe(entries, uid):
    reads = {'read', 'readattr', 'readextattr', 'readsecurity', 'list', 'search', 'execute', 'synchronize'}
    for entry in entries:
        if entry['effect'] == 'deny':
            continue
        trusted = False
        if entry['kind'] == 'user':
            try:
                principal_uid = int(entry['principal']) if entry['principal'].isdigit() else pwd.getpwnam(entry['principal']).pw_uid
                trusted = principal_uid in (0, uid)
            except KeyError:
                pass
        if not trusted and not set(entry['rights']) <= reads:
            return False
    return True


def _acl_content_private(entries, uid):
    # Store content includes private messages and capabilities. Metadata-only
    # read grants are harmless; untrusted content-read grants are not.
    for entry in entries:
        if entry['effect'] != 'allow' or not set(entry['rights']) & {'read', 'readextattr', 'list'}:
            continue
        if entry['kind'] != 'user':
            return False
        try:
            principal = entry['principal']
            owner = int(principal) if principal.isdigit() else pwd.getpwnam(principal).pw_uid
        except KeyError:
            return False
        if owner not in (0, uid):
            return False
    return True


def custody():
    """Filtered metadata ONLY: never opens/hashes the master database contents."""
    config = service._installed_server_config()
    report = dict(admitted=False, policy=POLICY, actor=dict(kind='local_server_custodian', real_uid=os.getuid(), effective_uid=os.geteuid()),
                  trust_domain='installed local account/control; no hostile-same-UID isolation', metadata=[], errors=[])
    if os.geteuid() != config.uid or any(name in os.environ for name in ROUTING_ENV):
        report['errors'].append('wrong effective account or routing environment present')
        return report
    roles = {config.plist: 'provider_file', config.wrapper: 'provider_executable', config.executable: 'executable',
             config.master: 'private_database', config.working_directory: 'working_directory'}
    paths = dict(roles)
    for path in roles:
        if not path.is_absolute() or str(path) != os.path.normpath(str(path)):
            report['errors'].append('original fixed path is not absolute/canonical')
            return report
        for parent in path.parents:
            paths.setdefault(parent, 'ancestor')
    stable = []
    for path, role in sorted(paths.items(), key=lambda pair: str(pair[0])):
        try:
            meta = _metadata(path)
            meta['role'] = role
            allowed_owner = meta['owner_uid'] in ((0, config.uid) if role == 'ancestor' else (config.uid,))
            expected_type = 'directory' if role in ('ancestor', 'working_directory') else 'regular'
            writable_other = bool(meta['mode'] & 0o022)
            sticky_root = role == 'ancestor' and meta['owner_uid'] == 0 and bool(meta['mode'] & stat.S_ISVTX)
            private = role == 'private_database'
            if (meta['symlink'] or meta['type'] != expected_type or not allowed_owner
                    or (writable_other and not sticky_root) or (private and meta['mode'] & 0o077)
                    or not _acl_safe(meta['acl'], config.uid)
                    or (private and not _acl_content_private(meta['acl'], config.uid))):
                report['metadata'].append(meta)
                report['errors'].append(dict(path=str(path), reason='unsafe type/owner/mode/symlink/ACL'))
                return report
            if role in ('provider_file', 'provider_executable', 'executable') and meta['type'] == 'regular' and not meta['symlink']:
                meta['sha256'] = hashlib.sha256(path.read_bytes()).hexdigest()
            report['metadata'].append(meta)
            stable.append(meta)
        except (OSError, CoordError) as error:
            report['errors'].append(dict(path=str(path), reason=str(error) if isinstance(error, CoordError) else 'metadata unavailable'))
            return report
    # SQLite sidecars are validated if present but excluded from the stable
    # identity: their legitimate creation/removal is not a provider change.
    for suffix in ('-wal', '-shm', '-journal'):
        path = Path(str(config.master) + suffix)
        if os.path.lexists(path):
            try:
                meta = _metadata(path)
                meta['role'] = 'sqlite_sidecar'
                report['metadata'].append(meta)
                if meta['symlink'] or meta['type'] != 'regular' or meta['owner_uid'] != config.uid or meta['mode'] & 0o077 or not _acl_safe(meta['acl'], config.uid) or not _acl_content_private(meta['acl'], config.uid):
                    report['errors'].append(dict(path=str(path), reason='unsafe SQLite sidecar'))
                    return report
            except (OSError, CoordError):
                report['errors'].append(dict(path=str(path), reason='sidecar metadata unavailable'))
                return report
    if report['errors']:
        return report
    try:
        resolved = service.configured_server_database(config.plist)
        if resolved != config.master or not os.access(config.master, os.R_OK | os.W_OK, effective_ids=True):
            raise CoordError('fixed store identity/control mismatch')
    except (OSError, ValueError, NotImplementedError, service.ServiceError, CoordError):
        report['errors'].append('installed provider/store binding unavailable')
        return report
    report.update(admitted=True, store=str(config.master), project='default', component='com.agent-chat.server',
                  boundary_sha256=digest(dict(policy=POLICY, uid=config.uid, metadata=stable)))
    return report


def _boundary():
    report = custody()
    if not report['admitted']:
        raise CoordError(DENIED)
    return report


def _state(db, boundary, now):
    row = db.execute('SELECT owner_session,reservation_id FROM resources WHERE name=?', (TARGET['resource'],)).fetchone()
    if not row or not row['owner_session'] or row['reservation_id'] != TARGET['reservation_id']:
        raise CoordError('exact fixed target reservation unavailable')
    host = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (row['owner_session'],)).fetchone()
    if not host:
        raise CoordError('authoritative original host association unavailable')
    state = _snapshot(db, row['owner_session'], host['host_id'], 'default', TARGET['resource'], TARGET['reservation_id'], now)
    if TARGET['run_id'] not in {r['run_id'] for r in state['runs']}:
        raise CoordError('exact fixed guarded run unavailable')
    return state, digest(dict(boundary=boundary['boundary_sha256'], target=state, policy=POLICY))


def _schema(db):
    db.execute('''CREATE TABLE IF NOT EXISTS custodian_recovery_intents (
        id TEXT PRIMARY KEY,reservation_id TEXT UNIQUE NOT NULL,target TEXT NOT NULL,
        boundary_sha256 TEXT NOT NULL,prestate_sha256 TEXT NOT NULL,policy TEXT NOT NULL,
        authority_reference TEXT NOT NULL,prepared_by TEXT NOT NULL,prepared_at REAL NOT NULL,
        expires_at REAL NOT NULL,applied_at REAL,revoked_at REAL)''')
    db.execute('''CREATE TABLE IF NOT EXISTS custodian_abandonments (
        reservation_id TEXT PRIMARY KEY,intent_id TEXT UNIQUE NOT NULL,actor TEXT NOT NULL,
        authority_reference TEXT NOT NULL,target TEXT NOT NULL,evidence TEXT NOT NULL,
        evidence_sha256 TEXT NOT NULL,recorded_at REAL NOT NULL)''')
    db.execute('''CREATE TRIGGER IF NOT EXISTS custodian_intent_scope_immutable BEFORE UPDATE ON custodian_recovery_intents
        WHEN NEW.id IS NOT OLD.id OR NEW.reservation_id IS NOT OLD.reservation_id OR NEW.target IS NOT OLD.target
          OR NEW.boundary_sha256 IS NOT OLD.boundary_sha256 OR NEW.prestate_sha256 IS NOT OLD.prestate_sha256
          OR NEW.policy IS NOT OLD.policy OR NEW.authority_reference IS NOT OLD.authority_reference
          OR NEW.prepared_by IS NOT OLD.prepared_by OR NEW.prepared_at IS NOT OLD.prepared_at OR NEW.expires_at IS NOT OLD.expires_at
          OR (OLD.applied_at IS NOT NULL AND NEW.applied_at IS NOT OLD.applied_at)
          OR (OLD.revoked_at IS NOT NULL AND NEW.revoked_at IS NOT OLD.revoked_at)
        BEGIN SELECT RAISE(ABORT,'custodian intent scope immutable'); END''')
    for table in ('custodian_recovery_intents', 'custodian_abandonments'):
        db.execute(f"CREATE TRIGGER IF NOT EXISTS {table}_no_delete BEFORE DELETE ON {table} BEGIN SELECT RAISE(ABORT,'custodian history immutable'); END")
    db.execute("CREATE TRIGGER IF NOT EXISTS custodian_audit_no_update BEFORE UPDATE ON custodian_abandonments BEGIN SELECT RAISE(ABORT,'custodian audit immutable'); END")


def _same_boundary(boundary):
    if _boundary()['boundary_sha256'] != boundary['boundary_sha256']:
        raise CoordError('installed provider/store source identity changed')


def execute(operation, *, expected_prestate=None, authority_reference=None, intent_id=None):
    boundary = _boundary()
    readonly = operation in ('preflight', 'status')
    store = Path(boundary['store'])
    db = sqlite3.connect(store.as_uri() + ('?mode=ro' if readonly else '?mode=rw'), uri=True, timeout=3)
    db.row_factory = sqlite3.Row
    try:
        db.execute('BEGIN' if readonly else 'BEGIN IMMEDIATE')
        now = time.time()
        if operation in ('status', 'revoke'):
            intent = db.execute('SELECT * FROM custodian_recovery_intents WHERE id=? AND reservation_id=?', (intent_id, TARGET['reservation_id'])).fetchone()
            if not intent or intent['boundary_sha256'] != boundary['boundary_sha256'] or intent['policy'] != POLICY:
                raise CoordError('exact current custodian intent unavailable')
            target = json.loads(intent['target'])
            _target_actor(db, target['owner_session'], target['host_id'])
            if operation == 'status':
                return dict(intent_id=intent['id'], state='applied' if intent['applied_at'] is not None else 'revoked' if intent['revoked_at'] is not None else 'expired' if intent['expires_at'] <= now else 'prepared', actor_kind='local_server_custodian', task_result='unknown')
            _target_fresh(db, target['owner_session'])
            if intent['applied_at'] is not None or intent['revoked_at'] is not None:
                raise CoordError('custodian intent already terminal')
            db.execute('UPDATE custodian_recovery_intents SET revoked_at=? WHERE id=? AND applied_at IS NULL AND revoked_at IS NULL', (now, intent['id']))
            _same_boundary(boundary)
            db.commit()
            return dict(intent_id=intent['id'], state='revoked', mutation='named administrative intent only')
        state, pin = _state(db, boundary, now)
        target = dict(TARGET, owner_session=state['owner_session'], host_id=state['host_id'])
        if operation == 'preflight':
            return dict(eligible=state['eligible'], missing_evidence=None if state['eligible'] else 'original trusted entire-execution closure unavailable; no PID/boot inference',
                        actor=boundary['actor'], target=target, prestate_sha256=pin, boundary_sha256=boundary['boundary_sha256'],
                        effect='explicit admin PRESENT abandonment; original task unknown; no capability/native/FIFO grant')
        _target_fresh(db, state['owner_session'])
        if not state['eligible']:
            raise CoordError('independently sufficient original entire-execution closure unavailable')
        if operation == 'prepare':
            if pin != expected_prestate or not isinstance(authority_reference, str) or not 1 <= len(authority_reference) <= 512:
                raise CoordError('exact prestate and nonsecret administrative audit reference required')
            _schema(db)
            iid = 'custodian_intent_' + secrets.token_hex(16)
            db.execute('INSERT INTO custodian_recovery_intents VALUES(?,?,?,?,?,?,?,?,?,?,NULL,NULL)',
                       (iid, TARGET['reservation_id'], canonical(target), boundary['boundary_sha256'], pin, POLICY,
                        authority_reference, canonical(dict(boundary['actor'], pid=os.getpid())), now, now + 60))
            _same_boundary(boundary)
            db.commit()
            return dict(intent_id=iid, expires_at=now + 60, mutation='finite administrative intent/schema only')
        if operation != 'apply':
            raise CoordError('unsupported local administrative operation')
        intent = db.execute('SELECT * FROM custodian_recovery_intents WHERE id=? AND reservation_id=?', (intent_id, TARGET['reservation_id'])).fetchone()
        if (not intent or intent['target'] != canonical(target) or intent['boundary_sha256'] != boundary['boundary_sha256']
                or intent['prestate_sha256'] != pin or intent['policy'] != POLICY or intent['expires_at'] <= now
                or intent['applied_at'] is not None or intent['revoked_at'] is not None):
            raise CoordError('custodian intent changed, expired, revoked or used')
        evidence = canonical(dict(original=state, outcome='admin_present_abandonment', task_result='unknown'))
        evidence_hash = hashlib.sha256(evidence.encode()).hexdigest()
        db.execute('INSERT INTO custodian_abandonments VALUES(?,?,?,?,?,?,?,?)',
                   (TARGET['reservation_id'], intent['id'], canonical(dict(boundary['actor'], pid=os.getpid())), intent['authority_reference'], canonical(target), evidence, evidence_hash, now))
        for run in state['runs']:
            if run['closed_at'] is None and db.execute('UPDATE remote_guarded_runs SET closed_at=?,close_evidence_sha256=? WHERE run_id=? AND closed_at IS NULL', (now, evidence_hash, run['run_id'])).rowcount != 1:
                raise CoordError('custodian run compare-and-set failed')
        if db.execute('UPDATE resources SET owner_session=NULL,reservation_id=NULL,token=NULL,granted_at=NULL,deadline=NULL,stale=0,reason=NULL WHERE name=? AND owner_session=? AND reservation_id=?',
                      (TARGET['resource'], target['owner_session'], TARGET['reservation_id'])).rowcount != 1:
            raise CoordError('custodian target compare-and-set failed')
        if db.execute('UPDATE custodian_recovery_intents SET applied_at=? WHERE id=? AND applied_at IS NULL AND revoked_at IS NULL', (now, intent['id'])).rowcount != 1:
            raise CoordError('custodian authority compare-and-set failed')
        _same_boundary(boundary)
        db.commit()
        return dict(outcome='admin_present_abandonment', actor_kind='local_server_custodian', target=target,
                    task_result='unknown', capability_issued=False, native_admission=False, evidence_sha256=evidence_hash)
    except CoordError:
        raise
    except (sqlite3.Error, RuntimeError) as error:
        raise CoordError('custodian state/schema unavailable or transaction refused') from error
    finally:
        db.rollback()
        db.close()
        if readonly:
            _same_boundary(boundary)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='operation', required=True)
    for operation in ('custody', 'preflight', 'prepare', 'apply', 'status', 'revoke'):
        command = commands.add_parser(operation)
        if operation == 'prepare':
            command.add_argument('--expected-prestate', required=True)
            command.add_argument('--authority-reference', required=True, help='audit reference only; never grants authority')
        if operation in ('apply', 'status', 'revoke'):
            command.add_argument('--intent-id', required=True)
    args = vars(parser.parse_args(argv))
    operation = args.pop('operation')
    try:
        result = custody() if operation == 'custody' else execute(operation, **args)
        print(json.dumps(result, sort_keys=True))
        return 0 if result.get('admitted', True) else 2
    except (CoordError, OSError, ValueError) as error:
        print(str(error) if isinstance(error, CoordError) else 'custodian acquisition unavailable', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
