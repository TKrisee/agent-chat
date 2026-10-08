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
    db.execute('''CREATE TABLE IF NOT EXISTS lifecycle_producers (
        id TEXT PRIMARY KEY, capability_sha256 TEXT UNIQUE NOT NULL,
        owner_session TEXT NOT NULL, host_id TEXT NOT NULL, enrolled_at REAL NOT NULL)''')
    db.execute('''CREATE TABLE IF NOT EXISTS execution_instances (
        id TEXT PRIMARY KEY, run_id TEXT UNIQUE NOT NULL, producer_id TEXT NOT NULL,
        resource TEXT NOT NULL, reservation_id TEXT NOT NULL, owner_session TEXT NOT NULL,
        host_id TEXT NOT NULL, established_at REAL NOT NULL)''')
    db.execute('''CREATE TABLE IF NOT EXISTS current_instance_closures (
        id TEXT PRIMARY KEY, instance_id TEXT UNIQUE NOT NULL, evidence BLOB NOT NULL,
        evidence_sha256 TEXT NOT NULL, recorded_at REAL NOT NULL)''')


def enroll(coord, owner, host, capability_path):
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
    args = parser.parse_args(argv)
    if not Path(args.db).is_file():
        raise CoordError('producer enrollment requires an existing exact project database')
    with contextlib.closing(Coordinator(args.db)) as coord:
        producer_id = enroll(coord, args.owner, args.host, args.capability_file)
    print('Enrolled ' + producer_id + '; private capability: ' + args.capability_file)


if __name__ == '__main__':
    main()
