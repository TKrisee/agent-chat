"""Explicit local administrator enrollment for trusted guarded launchers.

The enrolled launcher is an operational principal: its capability must never be
provided to the command it supervises. Enrollment does not certify old runs.
"""
import argparse
import contextlib
import hashlib
import os
from pathlib import Path
import secrets
import time

from .core import Coordinator, CoordError


def schema(db):
    db.execute("""CREATE TABLE IF NOT EXISTS execution_epoch_attestations (
      id TEXT PRIMARY KEY, producer_id TEXT NOT NULL, instance_id TEXT NOT NULL,
      owner TEXT NOT NULL, host TEXT NOT NULL, observed_at REAL NOT NULL,
      canonical_resource TEXT NOT NULL, old_reservation TEXT NOT NULL,
      root_generation TEXT NOT NULL, boot_id TEXT NOT NULL, boot_epoch REAL NOT NULL,
      old_backing TEXT NOT NULL, backing TEXT NOT NULL, execution_id TEXT NOT NULL,
      digest TEXT NOT NULL, recorded_at REAL NOT NULL)""")
    for operation in ('UPDATE','DELETE'):
        db.execute(f"CREATE TRIGGER IF NOT EXISTS epoch_attestations_no_{operation.lower()} BEFORE {operation} ON execution_epoch_attestations BEGIN SELECT RAISE(ABORT,'epoch attestation is immutable'); END")
    db.execute('''CREATE TABLE IF NOT EXISTS lifecycle_producers (
        id TEXT PRIMARY KEY, capability_sha256 TEXT UNIQUE NOT NULL,
        owner_session TEXT NOT NULL, host_id TEXT NOT NULL, enrolled_at REAL NOT NULL)''')
    db.execute("""CREATE TABLE IF NOT EXISTS execution_producer_scopes (
        producer_id TEXT PRIMARY KEY,workspace TEXT NOT NULL,leaf_prefix TEXT NOT NULL,
        gitdir TEXT NOT NULL,index_resource TEXT NOT NULL,scope_digest TEXT NOT NULL)""")
    for operation in ('UPDATE','DELETE'):
        db.execute(f"CREATE TRIGGER IF NOT EXISTS producer_scope_no_{operation.lower()} BEFORE {operation} ON execution_producer_scopes BEGIN SELECT RAISE(ABORT,'producer scope is immutable'); END")
    db.execute('''CREATE TABLE IF NOT EXISTS execution_instances (
        id TEXT PRIMARY KEY, run_id TEXT UNIQUE NOT NULL, producer_id TEXT NOT NULL,
        resource TEXT NOT NULL, reservation_id TEXT NOT NULL, owner_session TEXT NOT NULL,
        host_id TEXT NOT NULL, established_at REAL NOT NULL)''')
    db.execute('''CREATE TABLE IF NOT EXISTS current_instance_closures (
        id TEXT PRIMARY KEY, instance_id TEXT UNIQUE NOT NULL, evidence BLOB NOT NULL,
        evidence_sha256 TEXT NOT NULL, recorded_at REAL NOT NULL)''')


def enroll(coord, owner, host, capability_path, *, current_scope=None):
    """Local DB administration only; deliberately absent from remote dispatch."""
    secret = secrets.token_urlsafe(48)
    producer_id = 'producer_' + secrets.token_hex(16)
    path = Path(capability_path)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with coord.tx() as db:
            schema(db)
            pinned = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (owner,)).fetchone()
            if not pinned or pinned['host_id'] != host:
                raise CoordError('producer enrollment requires the exact existing owner host')
            db.execute('INSERT INTO lifecycle_producers VALUES(?,?,?,?,?)',
                       (producer_id, hashlib.sha256(secret.encode()).hexdigest(), owner, host, time.time()))
            if current_scope is not None:
                if set(current_scope)!= {'workspace','leaf_prefix','gitdir','index_resource'}:
                    raise CoordError('exact local current scope mapping required')
                from .remote_service import resource_name
                ws,leaf,git,index=(current_scope[k] for k in ('workspace','leaf_prefix','gitdir','index_resource'))
                if (any(not isinstance(v,str) or not v or len(v)>2048 or any(ord(ch)<32 for ch in v) for v in (ws,leaf,git,index)) or
                    not ws.startswith('workspace:/') or not git.startswith('/') or not leaf.startswith('file:') or not leaf.endswith('/') or
                    resource_name(leaf[:-1])+'/'!=leaf or not index.startswith('file:') or resource_name(index)!=index):
                    raise CoordError('normalized owner-local current scope mapping required')
                scope_digest=hashlib.sha256(repr((producer_id,owner,host,ws,leaf,git,index)).encode()).hexdigest()
                db.execute('INSERT INTO execution_producer_scopes VALUES(?,?,?,?,?,?)',(producer_id,ws,leaf,git,index,scope_digest))
            with os.fdopen(fd, 'w') as output:
                fd = None
                output.write(secret + '\n')
                output.flush()
                os.fsync(output.fileno())
    except BaseException:
        if fd is not None:
            os.close(fd)
        path.unlink(missing_ok=True)
        raise
    return producer_id


def credential(path):
    path = Path(path)
    flags = os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0)
    fd = os.open(path, flags)
    with os.fdopen(fd) as source:
        info = os.fstat(source.fileno())
        if info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise CoordError('producer capability must be a private owner file')
        value = source.read(256).strip()
    if not value or len(value) > 128:
        raise CoordError('producer capability file is invalid')
    return value


def dispatch(coord, session, host, operation, params):
    with coord.tx() as db:
        schema(db)
        capability = params.get('capability')
        if not isinstance(capability, str):
            raise CoordError('trusted producer capability required')
        producer = db.execute('SELECT * FROM lifecycle_producers WHERE capability_sha256=?',
                              (hashlib.sha256(capability.encode()).hexdigest(),)).fetchone()
        if not producer or producer['owner_session'] != session or producer['host_id'] != host:
            raise CoordError('trusted producer capability does not match owner host')
        run = db.execute('SELECT * FROM remote_guarded_runs WHERE run_id=?', (params.get('run_id'),)).fetchone()
        if not run or run['session_id'] != session or run['host_id'] != host:
            raise CoordError('trusted producer run does not match owner host')
        instance = db.execute('SELECT * FROM execution_instances WHERE run_id=?', (run['run_id'],)).fetchone()
        if operation == 'producer-epoch-attest':
            if not instance or instance['producer_id'] != producer['id']:
                raise CoordError('epoch observation requires preexisting current execution instance')
            return _record_epoch_attestation(db, producer, run, instance, params)
        if operation == 'producer-status':
            fact = db.execute('SELECT * FROM current_instance_closures WHERE instance_id=?', (instance['id'],)).fetchone() if instance else None
            return {'instance': dict(instance) if instance else None,
                    'closure': {'id': fact['id'], 'sha256': fact['evidence_sha256'],
                                'evidence': bytes(fact['evidence']).decode(), 'recorded_at': fact['recorded_at']} if fact else None}
        if operation == 'producer-establish':
            if instance:
                if instance['producer_id'] != producer['id']:
                    raise CoordError('execution instance producer cannot change')
                return dict(instance)
            if run['local_pid'] != 0 or run['closed_at'] is not None or producer['enrolled_at'] > run['started_at']:
                raise CoordError('execution instance requires prior enrollment and an unlaunched open run')
            instance_id = 'instance_' + secrets.token_hex(16)
            db.execute('INSERT INTO execution_instances VALUES(?,?,?,?,?,?,?,?)',
                       (instance_id, run['run_id'], producer['id'], run['resource'], run['reservation_id'], session, host, time.time()))
            return dict(db.execute('SELECT * FROM execution_instances WHERE id=?', (instance_id,)).fetchone())
        if operation != 'producer-close' or not instance or instance['producer_id'] != producer['id']:
            raise CoordError('trusted closure requires the preexisting execution instance')
        if set(params) != {'capability', 'run_id', 'instance_id', 'pid', 'returncode', 'group_closed'}:
            raise CoordError('trusted closure requires exact observed exit and group closure')
        code = params['returncode']
        if (params['instance_id'] != instance['id'] or params['pid'] != run['local_pid'] or run['local_pid'] <= 0
                or not isinstance(code, int) or isinstance(code, bool) or params['group_closed'] is not True):
            raise CoordError('trusted closure observation does not match execution instance')
        evidence = ('instance=' + instance['id'] + '\nrun=' + run['run_id'] + '\npid=' + str(run['local_pid'])
                    + '\nreturncode=' + str(code) + '\ngroup_closed=true\n').encode()
        previous = db.execute('SELECT * FROM current_instance_closures WHERE instance_id=?', (instance['id'],)).fetchone()
        if previous:
            if bytes(previous['evidence']) != evidence:
                raise CoordError('trusted closure retry differs from immutable observation')
            return {'instance_id': instance['id'], 'closure_fact_id': previous['id'], 'sha256': previous['evidence_sha256']}
        if run['closed_at'] is not None:
            raise CoordError('trusted closure cannot be established retrospectively after guard closure')
        fact_id, digest = 'closure_' + secrets.token_hex(16), hashlib.sha256(evidence).hexdigest()
        db.execute('INSERT INTO current_instance_closures VALUES(?,?,?,?,?)', (fact_id, instance['id'], evidence, digest, time.time()))
        return {'instance_id': instance['id'], 'closure_fact_id': fact_id, 'sha256': digest}


def main(argv=None):
    parser = argparse.ArgumentParser(description='Local server administrator producer enrollment; never enroll an agent child.')
    parser.add_argument('--db', required=True)
    parser.add_argument('--owner', required=True)
    parser.add_argument('--host', required=True)
    parser.add_argument('--capability-file', required=True)
    for field in ('workspace','leaf-prefix','gitdir','index-resource'): parser.add_argument('--current-'+field)
    args = parser.parse_args(argv)
    values=(args.current_workspace,args.current_leaf_prefix,args.current_gitdir,args.current_index_resource)
    if any(v is not None for v in values) and not all(v is not None for v in values): raise CoordError('all current mapping fields required together')
    scope=dict(zip(('workspace','leaf_prefix','gitdir','index_resource'),values)) if all(v is not None for v in values) else None
    if not Path(args.db).is_file():
        raise CoordError('producer enrollment requires an existing exact project database')
    with contextlib.closing(Coordinator(args.db)) as coord:
        producer_id = enroll(coord, args.owner, args.host, args.capability_file,current_scope=scope)
    print('Enrolled ' + producer_id + '; private capability: ' + args.capability_file)



EPOCH_OBSERVATION_FIELDS = frozenset(('observed_at','canonical_resource','old_reservation',
    'root_generation','boot_id','boot_epoch','old_backing','backing','execution_id'))


def _record_epoch_attestation(db, producer, run, instance, params):
    if set(params) != {'capability','run_id','instance_id','request_id','observation','prepare_id','gitdir','native_processes','leaf_prefix','index_resource'}:
        raise CoordError('exact typed epoch observation required')
    evidence=params['observation']
    if not isinstance(evidence,dict) or set(evidence)!=EPOCH_OBSERVATION_FIELDS:
        raise CoordError('epoch observation fields do not match')
    identifier=params['request_id']
    import uuid
    try:
        if str(uuid.UUID(identifier))!=identifier: raise ValueError()
    except (ValueError,TypeError,AttributeError):
        raise CoordError('one canonical attestation request UUID required') from None
    for key in EPOCH_OBSERVATION_FIELDS-{'observed_at','boot_epoch'}:
        value=evidence[key]
        if not isinstance(value,str) or not 1<=len(value)<=2048 or any(ord(c)<32 for c in value):
            raise CoordError('epoch observation requires bounded exact strings')
    now=time.time()
    for key in ('observed_at','boot_epoch'):
        value=evidence[key]
        if isinstance(value,bool) or not isinstance(value,(int,float)) or not 0<value<=now:
            raise CoordError('epoch observation clock is invalid')
    def backing_identity(value):
        parts=value.rsplit(':',2)
        if len(parts)!=3 or not parts[0].startswith('/') or not all(part.isascii() and part.isdecimal() for part in parts[1:]) or int(parts[2])<=0:
            raise CoordError('epoch physical backing identity is malformed')
        return (int(parts[1]),int(parts[2]))
    backing_identity(evidence['backing'])
    if evidence['boot_epoch']>evidence['observed_at']:
        raise CoordError('epoch observation requires distinct known physical backing and ordered boot')
    import uuid
    try: uuid.UUID(evidence['boot_id'])
    except (ValueError,AttributeError): raise CoordError('epoch kernel boot identity is malformed') from None
    if evidence['execution_id']!=instance['id']:
        raise CoordError('epoch execution identity must be the preexisting authenticated instance')
    prepare=db.execute("SELECT * FROM execution_epoch_prepares WHERE id=? AND state='prepared'",(params['prepare_id'],)).fetchone()
    if (not prepare or prepare['instance_id']!=instance['id'] or prepare['host']!=run['host_id'] or
        prepare['root_generation']!=evidence['root_generation'] or prepare['old_reservation']!=evidence['old_reservation'] or
        prepare['resource']!=evidence['canonical_resource'] or prepare['workspace']!='workspace:'+evidence['backing'].rsplit(':',2)[0] or
        prepare['expires_at']<=now):
        raise CoordError('current sample requires exact active configured-Root prepared scope')
    scope=db.execute('SELECT * FROM execution_producer_scopes WHERE producer_id=?',(producer['id'],)).fetchone()
    if (not scope or scope['scope_digest']!=prepare['scope_digest'] or
        any(scope[k]!=prepare[k] for k in ('workspace','leaf_prefix','gitdir','index_resource')) or
        params['leaf_prefix']!=scope['leaf_prefix'] or params['index_resource']!=scope['index_resource']):
        raise CoordError('authenticated current sample scope mapping mismatched')
    gitdir=params['gitdir'];processes=params['native_processes']
    if (not isinstance(gitdir,str) or len(gitdir)>2048 or gitdir.rsplit(':',2)[0]!=prepare['gitdir'] or
        not isinstance(processes,list) or len(processes)>128 or
        any(not isinstance(item,str) or not 1<=len(item)<=512 for item in processes)):
        raise CoordError('current sample gitdir/process scope malformed or mismatched')
    backing_identity(gitdir)
    material=(producer['id'],instance['id'],tuple(sorted(evidence.items())),prepare['id'],gitdir,tuple(processes),scope['scope_digest'])
    digest=hashlib.sha256(repr(material).encode()).hexdigest()
    existing=db.execute('SELECT * FROM execution_epoch_attestations WHERE id=?',(identifier,)).fetchone()
    if existing:
        if existing['digest']!=digest: raise CoordError('epoch observation replay conflicts')
        return dict(existing)
    if (params['instance_id']!=instance['id'] or run['closed_at'] is not None or
        run['local_pid']!=0 or producer['enrolled_at']>run['started_at'] or
        now-evidence['observed_at']>5):
        raise CoordError('epoch observation requires fresh prelaunch instance and prior enrollment')
    db.execute('INSERT INTO execution_epoch_attestations VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
        (identifier,producer['id'],instance['id'],run['session_id'],run['host_id'],evidence['observed_at'],
         evidence['canonical_resource'],evidence['old_reservation'],evidence['root_generation'],
         evidence['boot_id'],evidence['boot_epoch'],evidence['old_backing'],evidence['backing'],
         evidence['execution_id'],digest,now))
    db.execute('INSERT INTO execution_epoch_samples VALUES(?,?,?,?,?,?,?)',(identifier,prepare['id'],gitdir,len(processes),'current-project argv + managed guards; outside/shared domains excluded',hashlib.sha256(repr(tuple(processes)).encode()).hexdigest(),scope['scope_digest']))
    return dict(db.execute('SELECT * FROM execution_epoch_attestations WHERE id=?',(identifier,)).fetchone())


def read_current_boot_id():
    """Fixed local passive kernel identity; unsupported hosts fail closed."""
    import subprocess,uuid
    value=subprocess.run(['/usr/sbin/sysctl','-n','kern.bootsessionuuid'],capture_output=True,text=True,timeout=3,check=True).stdout.strip()
    try: uuid.UUID(value)
    except (ValueError,AttributeError): raise CoordError('current kernel boot identity unavailable') from None
    return value


def collect_epoch_observation(*, canonical_resource, old_reservation, root_generation,
                              old_backing_path=None, current_backing_path=None, execution_id):
    """Current-only bounded observation under a configured-Root managed fence.

    No old path/PID lookup. Coverage excludes external/shared domains and is not
    a universal process-absence certificate. Producer credential remains private.
    """
    import re,subprocess,shlex
    if current_backing_path is None:
        raise CoordError('physical backing observation unavailable; no retrospective substitute')
    project=Path(current_backing_path)
    if not project.is_absolute() or project.is_symlink(): raise CoordError('exact current backing required')
    project=project.resolve(strict=True)
    if not project.is_dir(): raise CoordError('current backing is not a directory')
    def identity(path):
        value=path.resolve(strict=True);info=value.stat()
        return str(value)+':'+str(info.st_dev)+':'+str(info.st_ino)
    pointer=project/'.git'
    if not pointer.is_file() or pointer.is_symlink(): raise CoordError('current detached worktree gitdir unavailable')
    with pointer.open() as source: text=source.read(4097)
    if len(text)>4096: raise CoordError('current worktree pointer exceeds bound')
    lines=text.splitlines()
    if len(lines)!=1 or not lines[0].startswith('gitdir: '): raise CoordError('current worktree pointer unsupported')
    gitdir=Path(lines[0][8:]);gitdir=gitdir if gitdir.is_absolute() else project/gitdir
    boot=subprocess.run(['/usr/sbin/sysctl','-n','kern.boottime'],capture_output=True,text=True,timeout=3,check=True).stdout
    match=re.fullmatch(r'\{ sec = ([0-9]+), usec = ([0-9]+) \}.*',boot.strip())
    if not match: raise CoordError('current kernel boot observation unsupported')
    boot_id=read_current_boot_id()
    scan=subprocess.run(['/usr/bin/pgrep','-f',re.escape(str(project))],capture_output=True,text=True,timeout=3)
    if scan.returncode not in (0,1) or len(scan.stdout)>4096: raise CoordError('current project process observation unavailable')
    words=scan.stdout.split()
    if len(words)>128 or any(not value.isascii() or not value.isdecimal() for value in words): raise CoordError('current project process observation exceeds scope')
    pids=[value for value in words if int(value)!=os.getpid()]
    processes=[]
    if pids:
        observed=subprocess.run(['/bin/ps','-p',','.join(pids),'-o','pid=,ppid=,lstart=,command='],capture_output=True,text=True,timeout=3)
        if observed.returncode not in (0,1) or len(observed.stdout)>131072: raise CoordError('current project identity observation unavailable')
        for line in observed.stdout.splitlines():
            fields=line.split(None,7)
            if len(fields)!=8: raise CoordError('current project process identity malformed')
            args=shlex.split(fields[7])
            # Recheck exact observed argument membership after PID/birth sampling.
            if any(arg==str(project) or arg.startswith(str(project)+'/') for arg in args):
                processes.append(fields[0]+':'+ ' '.join(fields[2:7])+':'+hashlib.sha256(fields[7].encode()).hexdigest())
    observation=dict(observed_at=time.time(),canonical_resource=canonical_resource,old_reservation=old_reservation,
        root_generation=root_generation,boot_id=boot_id,boot_epoch=int(match[1])+int(match[2])/1000000,
        old_backing='UNVERIFIED:separate-old-domain',backing=identity(project),execution_id=execution_id)
    return observation,identity(gitdir),processes

if __name__ == '__main__':
    main()
