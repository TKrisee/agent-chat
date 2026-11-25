"""Opt-in, host-reported identity of an inert guarded launcher.

A retained report is not server-side kernel attestation, confinement, or proof
that any process exited or external effects were restored.
"""
import hashlib
import math
import os
import stat
import time
from pathlib import Path

from .core import CoordError


def schema(db):
    # Called before the guarded attachment transaction; no executescript commit.
    db.execute('CREATE TABLE IF NOT EXISTS remote_guard_witness_intents (run_id TEXT PRIMARY KEY)')
    db.execute('''CREATE TABLE IF NOT EXISTS remote_guard_witnesses (
        run_id TEXT PRIMARY KEY, reservation_id TEXT NOT NULL, session_id TEXT NOT NULL,
        host_id TEXT NOT NULL, pid INTEGER NOT NULL, parent_pid INTEGER NOT NULL,
        pgid INTEGER NOT NULL, uid INTEGER NOT NULL, ruid INTEGER NOT NULL,
        birth_sec INTEGER NOT NULL, birth_usec INTEGER NOT NULL, boot_id TEXT NOT NULL,
        executable TEXT NOT NULL, executable_sha256 TEXT NOT NULL, recorded_at REAL NOT NULL)''')
    db.execute('''CREATE TABLE IF NOT EXISTS remote_guard_file_scopes (
        run_id TEXT PRIMARY KEY, project_id TEXT NOT NULL, workspace TEXT NOT NULL, path TEXT NOT NULL,
        task_id TEXT NOT NULL, source_id TEXT NOT NULL, thread_id TEXT NOT NULL)''')
    db.execute('''CREATE TABLE IF NOT EXISTS remote_guard_file_observations (
        run_id TEXT NOT NULL, stage TEXT NOT NULL, device INTEGER, inode INTEGER, size INTEGER,
        mode INTEGER, sha256 TEXT, recorded_at REAL NOT NULL, PRIMARY KEY(run_id, stage))''')
    db.execute('CREATE TABLE IF NOT EXISTS remote_guard_file_checks (run_id TEXT PRIMARY KEY, checked_at REAL NOT NULL)')
    db.execute('CREATE TABLE IF NOT EXISTS remote_guard_recovery_intents (run_id TEXT PRIMARY KEY, recorded_at REAL NOT NULL)')
    for table in ('remote_guard_witness_intents', 'remote_guard_witnesses',
                  'remote_guard_file_scopes', 'remote_guard_file_observations', 'remote_guard_file_checks',
                  'remote_guard_recovery_intents'):
        for action in ('UPDATE', 'DELETE'):
            db.execute(f'''CREATE TRIGGER IF NOT EXISTS {table}_no_{action.lower()}
                BEFORE {action} ON {table} BEGIN SELECT RAISE(ABORT,'guard witness is immutable'); END''')


def observe_file(path):
    """Sample one declared file; no symlinks, hard links or unbounded reads."""
    from .exact_file_read import _walk, _stamp, MAX_BYTES
    fds = []; fd = None
    try:
        fds, fd = _walk(path)
        before = os.fstat(fd)
        if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.getuid() or
                before.st_nlink != 1 or before.st_size > MAX_BYTES):
            raise CoordError('declared file identity or size is invalid')
        digest = hashlib.sha256(); count = 0
        while True:
            chunk = os.read(fd, min(65536, MAX_BYTES + 1 - count))
            if not chunk: break
            count += len(chunk); digest.update(chunk)
            if count > MAX_BYTES: raise CoordError('declared file exceeds size bound')
        if count != before.st_size or _stamp(os.fstat(fd)) != _stamp(before):
            raise CoordError('declared file changed while sampled')
        again_dirs, again_fd = _walk(path)
        try:
            original_dirs = [(os.fstat(item).st_dev, os.fstat(item).st_ino) for item in fds]
            current_dirs = [(os.fstat(item).st_dev, os.fstat(item).st_ino) for item in again_dirs]
            if current_dirs != original_dirs or _stamp(os.fstat(again_fd)) != _stamp(before):
                raise CoordError('declared file path changed while sampled')
        finally:
            os.close(again_fd)
            for directory in reversed(again_dirs): os.close(directory)
        return dict(device=before.st_dev, inode=before.st_ino, size=count,
                    mode=stat.S_IMODE(before.st_mode), sha256=digest.hexdigest())
    except CoordError:
        raise
    except OSError as error:
        raise CoordError('declared file observation unavailable') from error
    finally:
        if fd is not None: os.close(fd)
        for directory in reversed(fds): os.close(directory)


FILE_FIELDS = ('device', 'inode', 'size', 'mode', 'sha256')


def validate_file_observation(value):
    from .exact_file_read import MAX_BYTES
    if not isinstance(value, dict) or set(value) != set(FILE_FIELDS):
        raise CoordError('invalid declared file observation')
    if any(type(value[key]) is not int or not 0 <= value[key] <= 2**63-1 for key in FILE_FIELDS[:4]):
        raise CoordError('invalid declared file identity')
    if (value['inode'] <= 0 or value['size'] > MAX_BYTES or value['mode'] > 0o7777 or not isinstance(value['sha256'], str) or
            len(value['sha256']) != 64 or any(c not in '0123456789abcdef' for c in value['sha256'])):
        raise CoordError('invalid declared file digest or mode')


def task_file_scope(db, projects, project, session, host, resource, task_id, source_id):
    from .exact_file_bindings import _roles, _task
    from .exact_file_read import exact_path
    from .remote_service import resource_name
    if not projects or not project or not isinstance(task_id, str) or not task_id or not isinstance(source_id, str) or not source_id:
        raise CoordError('declared file needs exact project task and source')
    normalized = resource_name(resource)
    if not normalized.startswith('file:') or normalized != resource:
        raise CoordError('declared file requires a canonical file resource')
    relative = normalized[5:]
    if any(part.startswith('.') for part in relative.split('/')):
        raise CoordError('declared file cannot use hidden path components')
    workspace = projects.get(project)['workspace']
    if not isinstance(workspace, str) or not workspace.startswith('/'):
        raise CoordError('declared file needs a configured project workspace')
    path = exact_path(workspace.rstrip('/') + '/' + relative)
    binding = db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?', (session,)).fetchone()
    if not binding or not binding['thread_id']:
        raise CoordError('declared file requires the current worker thread')
    _task(db, project, session, host, binding['thread_id'], task_id, source_id, _roles(db))
    return (project, workspace, path, task_id, source_id, binding['thread_id'])


def verify_scope(db, projects, scope, session, host, resource):
    current = task_file_scope(db, projects, scope['project_id'], session, host, resource,
                              scope['task_id'], scope['source_id'])
    if current != tuple(scope[key] for key in ('project_id','workspace','path','task_id','source_id','thread_id')):
        raise CoordError('declared file task or workspace changed')


def observe_launcher(pid, parent_pid, expected_image):
    from .capacity_observer import _boot_time
    from .native_owner_evidence import DarwinEvidence
    try:
        observer = DarwinEvidence()
        boot = _boot_time()
        parent = observer.process(parent_pid)
        expected = Path(parent.image).resolve(strict=True)
        if not Path(expected_image).resolve(strict=True).is_file():
            raise CoordError('launcher executable unavailable')
        deadline = time.monotonic() + 1
        while True:
            first = observer.process(pid)
            image = Path(first.image).resolve(strict=True)
            if image == expected or time.monotonic() >= deadline:
                break
            time.sleep(0.01)
        if first.pid != pid or first.parent != parent_pid or first.group != pid:
            raise CoordError('inert launcher process relationship differs')
        if first.uid != os.getuid() or first.ruid != os.getuid():
            raise CoordError('inert launcher user identity differs')
        if not image.is_file() or image != expected:
            raise CoordError('inert launcher executable differs')
        digest = hashlib.sha256()
        with image.open('rb') as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                digest.update(chunk)
        if observer.process(pid) != first or observer.process(parent_pid) != parent or not math.isfinite(boot) or boot <= 0:
            raise CoordError('inert launcher identity changed')
        return dict(pid=pid, parent_pid=parent_pid, pgid=first.group, uid=first.uid, ruid=first.ruid,
                    birth_sec=first.birth_sec, birth_usec=first.birth_usec,
                    boot_id='darwin-boot-' + str(round(boot * 1000000)), executable=str(image),
                    executable_sha256=digest.hexdigest())
    except CoordError:
        raise
    except Exception as error:
        raise CoordError('inert launcher witness unavailable') from error


def attach(coord, session, host, params, projects=None):
    from .remote_service import _check_guard
    fields = ('pid', 'parent_pid', 'pgid', 'uid', 'ruid', 'birth_sec', 'birth_usec',
              'boot_id', 'executable', 'executable_sha256')
    if set(params) not in ({'run_id', 'token', *fields}, {'run_id', 'token', 'file_baseline', *fields}):
        raise CoordError('invalid guard witness fields')
    for key in fields[:7]:
        if type(params[key]) is not int or params[key] < 0:
            raise CoordError('invalid guard witness identity')
    if (params['pid'] <= 1 or params['parent_pid'] <= 1 or params['pgid'] != params['pid'] or
            params['birth_sec'] <= 0 or params['birth_usec'] >= 1000000 or
            not isinstance(params['boot_id'], str) or not params['boot_id'].startswith('darwin-boot-') or
            not params['boot_id'][12:].isdigit() or not isinstance(params['executable'], str) or
            not os.path.isabs(params['executable']) or not isinstance(params['executable_sha256'], str) or
            len(params['executable_sha256']) != 64 or any(c not in '0123456789abcdef' for c in params['executable_sha256']) or
            not isinstance(params['run_id'], str) or not isinstance(params['token'], str)):
        raise CoordError('invalid guard witness identity')
    with coord.tx() as db:
        row = db.execute('SELECT * FROM remote_guarded_runs WHERE run_id=?', (params['run_id'],)).fetchone()
        if (not row or row['session_id'] != session or row['host_id'] != host or
                row['closed_at'] is not None or row['local_pid'] != 0):
            raise CoordError('remote guarded run cannot be attached')
        if not db.execute('SELECT 1 FROM remote_guard_witness_intents WHERE run_id=?', (row['run_id'],)).fetchone():
            raise CoordError('guarded run did not request a host witness')
        _check_guard(coord, db, row, session, params['token'])
        scope = db.execute('SELECT * FROM remote_guard_file_scopes WHERE run_id=?', (row['run_id'],)).fetchone()
        if bool(scope) != ('file_baseline' in params):
            raise CoordError('declared file baseline does not match run scope')
        if scope:
            verify_scope(db, projects, scope, session, host, row['resource'])
            validate_file_observation(params['file_baseline'])
            db.execute('INSERT INTO remote_guard_file_observations VALUES(?,?,?,?,?,?,?,?)',
                       (row['run_id'], 'baseline', *(params['file_baseline'][key] for key in FILE_FIELDS), time.time()))
        db.execute('INSERT INTO remote_guard_witnesses VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                   (row['run_id'], row['reservation_id'], session, host,
                    *(params[key] for key in fields), time.time()))
        db.execute('UPDATE remote_guarded_runs SET local_pid=? WHERE run_id=?', (params['pid'], row['run_id']))
    return {'run_id': row['run_id'], 'attached': True, 'witness': 'host_reported'}


def file_observe(coord, session, host, params, projects):
    from .remote_service import _check_guard
    if set(params) != {'run_id', 'token', 'stage', 'observation'} or params['stage'] not in ('check','changed','final'):
        raise CoordError('invalid declared file observation request')
    if any(not isinstance(params[key], str) or not params[key] for key in ('run_id','token')):
        raise CoordError('invalid declared file run or token')
    value = params['observation']
    if value is not None:
        validate_file_observation(value)
    elif params['stage'] != 'final':
        raise CoordError('declared file observation is unavailable')
    with coord.tx() as db:
        row = db.execute('SELECT * FROM remote_guarded_runs WHERE run_id=?', (params['run_id'],)).fetchone()
        if not row or row['session_id'] != session or row['host_id'] != host:
            raise CoordError('declared file run is not owned on this host')
        if params['stage'] == 'final':
            if row['closed_at'] is None: raise CoordError('declared file final requires closed guard')
            owner = coord._verify_owner(db, row['resource'], session, params['token'], True)
            if owner['reservation_id'] != row['reservation_id']:
                raise CoordError('declared file reservation changed')
        else:
            if row['closed_at'] is not None: raise CoordError('declared file run is not open on this host')
            _check_guard(coord, db, row, session, params['token'])
        scope = db.execute('SELECT * FROM remote_guard_file_scopes WHERE run_id=?', (row['run_id'],)).fetchone()
        baseline = db.execute("SELECT * FROM remote_guard_file_observations WHERE run_id=? AND stage='baseline'", (row['run_id'],)).fetchone()
        if not scope or not baseline:
            raise CoordError('declared file baseline is unavailable')
        original = {key: baseline[key] for key in FILE_FIELDS}
        if params['stage'] == 'check':
            verify_scope(db, projects, scope, session, host, row['resource'])
            if value != original: raise CoordError('declared file changed before gate release')
            db.execute('INSERT INTO remote_guard_file_checks VALUES(?,?)', (row['run_id'], time.time()))
            return {'run_id':row['run_id'], 'checked':True}
        if not db.execute('SELECT 1 FROM remote_guard_file_checks WHERE run_id=?', (row['run_id'],)).fetchone():
            raise CoordError('declared file requires its pre-gate recheck')
        if params['stage'] == 'changed' and value == original:
            raise CoordError('changed observation matches baseline')
        db.execute('INSERT INTO remote_guard_file_observations VALUES(?,?,?,?,?,?,?,?)',
                   (row['run_id'], params['stage'], *(value[key] if value else None for key in FILE_FIELDS), time.time()))
        return {'run_id':row['run_id'], 'recorded':True, 'provenance':'host_reported'}


def recovery(coord, session, host, params, projects):
    """Atomically settle one prospectively admitted observed file run."""
    from .remote_service import _check_guard
    expected = {'run_id', 'token', 'evidence_sha256', 'observation', 'process_observation'}
    if set(params) != expected or any(not isinstance(params[k], str) or not params[k] for k in ('run_id', 'token')):
        raise CoordError('invalid declared recovery request')
    digest = params['evidence_sha256']
    if not isinstance(digest, str) or len(digest) != 64 or any(c not in '0123456789abcdef' for c in digest):
        raise CoordError('invalid recovery evidence digest')
    validate_file_observation(params['observation'])
    proof = params['process_observation']
    proof_keys = {'pid','parent_pid','pgid','birth_sec','birth_usec','boot_id','observed_at','leader_absent','parent_absent','group_absent'}
    if (not isinstance(proof, dict) or set(proof) != proof_keys or
            any(proof[key] is not True for key in ('leader_absent','parent_absent','group_absent')) or
            any(type(proof[key]) is not int for key in ('pid','parent_pid','pgid','birth_sec','birth_usec')) or
            not isinstance(proof['boot_id'], str) or
            isinstance(proof['observed_at'], bool) or not isinstance(proof['observed_at'], (int,float)) or
            not 0 < proof['observed_at'] <= time.time()):
        raise CoordError('declared recovery needs a current host group-absence observation')
    with coord.tx() as db:
        row = db.execute('SELECT * FROM remote_guarded_runs WHERE run_id=?', (params['run_id'],)).fetchone()
        if not row or row['session_id'] != session or row['host_id'] != host:
            raise CoordError('declared recovery requires the original owner and host')
        scope = db.execute('SELECT * FROM remote_guard_file_scopes WHERE run_id=?', (row['run_id'],)).fetchone()
        witness = db.execute('SELECT * FROM remote_guard_witnesses WHERE run_id=?', (row['run_id'],)).fetchone()
        check = db.execute('SELECT 1 FROM remote_guard_file_checks WHERE run_id=?', (row['run_id'],)).fetchone()
        if not scope or not witness or not check:
            raise CoordError('declared recovery needs the prospective scope, witness and pre-gate check')
        if (any(proof[key] != witness[key] for key in ('pid','parent_pid','pgid','birth_sec','birth_usec','boot_id'))
                or proof['observed_at'] < witness['recorded_at']):
            raise CoordError('declared recovery process generation differs from original witness')
        verify_scope(db, projects, scope, session, host, row['resource'])
        if not db.execute('SELECT 1 FROM remote_guard_recovery_intents WHERE run_id=?',(row['run_id'],)).fetchone():
            raise CoordError('declared recovery was not prospectively prepared')
        if row['closed_at'] is not None:
            owner = coord._verify_owner(db, row['resource'], session, params['token'], True)
            if owner['reservation_id'] != row['reservation_id']:
                raise CoordError('declared recovery reservation changed')
            if row['close_evidence_sha256'] != digest:
                raise CoordError('declared recovery conflicts with existing closure')
            final = db.execute("SELECT * FROM remote_guard_file_observations WHERE run_id=? AND stage='final'", (row['run_id'],)).fetchone()
            if not final or {key:final[key] for key in FILE_FIELDS} != params['observation']:
                raise CoordError('declared recovery conflicts with final observation')
            return {'run_id':row['run_id'], 'closed':True, 'reconciled':True}
        _check_guard(coord, db, row, session, params['token'])
        baseline = db.execute("SELECT * FROM remote_guard_file_observations WHERE run_id=? AND stage='baseline'",(row['run_id'],)).fetchone()
        if not baseline or {key:baseline[key] for key in FILE_FIELDS} != params['observation']:
            raise CoordError('declared recovery final differs from the original baseline')
        db.execute('INSERT INTO remote_guard_file_observations VALUES(?,?,?,?,?,?,?,?)',
                   (row['run_id'],'final',*(params['observation'][key] for key in FILE_FIELDS),time.time()))
        db.execute('UPDATE remote_guarded_runs SET closed_at=?,close_evidence_sha256=? WHERE run_id=?',
                   (time.time(),digest,row['run_id']))
        return {'run_id':row['run_id'], 'closed':True, 'reconciled':False}


def file_status(db, run):
    scope = db.execute('SELECT * FROM remote_guard_file_scopes WHERE run_id=?', (run['run_id'],)).fetchone()
    if not scope: return None
    observations = {row['stage']: row for row in db.execute(
        'SELECT * FROM remote_guard_file_observations WHERE run_id=?', (run['run_id'],))}
    def fields(row):
        return {key:row[key] for key in FILE_FIELDS} if row and row['sha256'] is not None else None
    baseline, final = fields(observations.get('baseline')), fields(observations.get('final'))
    if baseline is None or final is None:
        state = 'unknown'
    elif final != baseline:
        state = 'changed'
    elif 'changed' in observations:
        state = 'restored_observed'
    else:
        state = 'matches_baseline'
    return dict(path=scope['path'], task_id=scope['task_id'], source_id=scope['source_id'],
                baseline=baseline, final=final, state=state, provenance='host_reported',
                process_closure='host_reported_closed' if run['closed_at'] is not None else 'unknown')
