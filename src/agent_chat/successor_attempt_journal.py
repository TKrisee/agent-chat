"""Prospective diagnostic records, never an authorization or an eligibility source."""
import hashlib
from contextlib import closing
import sqlite3
import time
import uuid
import os
import stat
from contextlib import contextmanager
from pathlib import Path


class AttemptAlreadyRecorded(ValueError):
    pass


def _digest(*fields):
    h=hashlib.sha256()
    for field in fields:
        value=str(field).encode('utf-8')
        h.update(len(value).to_bytes(8,'big')); h.update(value)
    return h.hexdigest()


def _schema(db):
    db.execute('''CREATE TABLE IF NOT EXISTS successor_producer_attempts (
        request_id TEXT PRIMARY KEY, attempt_id TEXT UNIQUE NOT NULL,
        operation TEXT NOT NULL, actor TEXT NOT NULL, host TEXT NOT NULL,
        source_id TEXT NOT NULL, project_id TEXT NOT NULL, target_digest TEXT NOT NULL, payload_digest TEXT NOT NULL, recorded_at REAL NOT NULL)''')
    db.execute('''CREATE TABLE IF NOT EXISTS successor_producer_attempt_events (
        seq INTEGER PRIMARY KEY AUTOINCREMENT, request_id TEXT NOT NULL, attempt_id TEXT NOT NULL,
        phase TEXT NOT NULL, predicate TEXT, recorded_at REAL NOT NULL)''')
    for table in ('successor_producer_attempts','successor_producer_attempt_events'):
        for op in ('UPDATE','DELETE'):
            db.execute(f"CREATE TRIGGER IF NOT EXISTS {table}_{op.lower()} BEFORE {op} ON {table} BEGIN SELECT RAISE(ABORT,'immutable successor attempt'); END")


def _identity(path):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o022 or path.resolve(strict=True) != path:
        raise ValueError("untrusted database path")
    return info.st_dev, info.st_ino


def _validate_schema(db):
    from . import successor_admission
    with closing(sqlite3.connect(':memory:')) as expected:
        _schema(expected)
        successor_admission.install_schema(expected)
        names = ('successor_producer_attempts', 'successor_producer_attempt_events',
                 'successor_admissions', 'successor_admission_uses', 'successor_admission_observations')
        def objects(conn):
            return {(r[0], r[1], r[2], r[3]) for r in conn.execute(
                "SELECT type,name,tbl_name,sql FROM sqlite_master WHERE tbl_name IN (?,?,?,?,?) AND type IN ('table','trigger')", names)}
        wanted=objects(expected)
    actual=objects(db)
    journal_names=set(names[:2]); packet_names=set(names[2:])
    for group, required in ((journal_names,False),(packet_names,True)):
        found={r for r in actual if r[2] in group}; canonical={r for r in wanted if r[2] in group}
        if (required or found) and found != canonical:
            raise ValueError('untrusted or incomplete successor schema')


@contextmanager
def verified_store(path, *, actor, host, project, root_thread):
    path=Path(path)
    identity=_identity(path)
    db=sqlite3.connect(path.as_uri()+'?mode=rw',uri=True,timeout=10,isolation_level=None)
    db.row_factory=sqlite3.Row
    sidecars = None
    def sidecar_ids():
        result=[]
        for suffix in ('-wal','-shm'):
            member=Path(str(path)+suffix)
            try: result.append(_identity(member))
            except FileNotFoundError: result.append(None)
        if (result[0] is None) != (result[1] is None):
            raise RuntimeError('database sidecar family incomplete; outcome unknown')
        return tuple(result)
    def check():
        try: current=_identity(path)
        except (FileNotFoundError, ValueError) as error:
            raise RuntimeError('database identity unavailable; outcome unknown') from error
        if current != identity: raise RuntimeError('database identity changed; outcome unknown')
        if sidecars is not None and sidecar_ids()!=sidecars:
            raise RuntimeError('database sidecar identity changed; outcome unknown')
    try:
        check()
        db.execute('PRAGMA trusted_schema=OFF')
        if db.execute("SELECT value FROM meta WHERE key='project_id'").fetchone()[0] != project:
            raise ValueError('project identity differs')
        row=db.execute('SELECT b.thread_id,b.parent_session,h.host_id FROM bridge_bindings b JOIN remote_session_hosts h ON b.session_id=h.session_id WHERE b.session_id=?',(actor,)).fetchone()
        if row is None or tuple(row)!=(root_thread,None,host): raise ValueError('root identity differs')
        _validate_schema(db)
        check()
        sidecars=sidecar_ids()
        check()
        yield db,check
    finally:
        db.close()


def begin(db, check, *, request_id, operation, actor, host, target, source, project):
    attempt=uuid.uuid4().hex
    check(); db.execute('BEGIN IMMEDIATE')
    try:
        check(); _validate_schema(db); _schema(db)
        if db.execute('SELECT 1 FROM successor_producer_attempts WHERE request_id=?',(request_id,)).fetchone():
            raise AttemptAlreadyRecorded('attempt already recorded; inspect exact status, do not replay')
        now=time.time()
        db.execute('INSERT INTO successor_producer_attempts VALUES(?,?,?,?,?,?,?,?,?,?)',
                   (request_id,attempt,operation,actor,host,source,project,_digest(str(Path(target))),_digest(request_id,operation,actor,host,target,source,project),now))
        db.execute('INSERT INTO successor_producer_attempt_events(request_id,attempt_id,phase,predicate,recorded_at) VALUES(?,?,?,?,?)',
                   (request_id,attempt,'attempt_started',None,now))
        check(); db.commit(); check()
    except BaseException:
        if db.in_transaction: db.rollback()
        raise
    return attempt

def append_on(db,request_id,attempt_id,phase,predicate=None):
    row=db.execute('SELECT 1 FROM successor_producer_attempts WHERE request_id=? AND attempt_id=?',(request_id,attempt_id)).fetchone()
    if not row: raise ValueError('exact attempt unavailable')
    db.execute('INSERT INTO successor_producer_attempt_events(request_id,attempt_id,phase,predicate,recorded_at) VALUES(?,?,?,?,?)',
               (request_id,attempt_id,phase,predicate,time.time()))


def append(db,check,request_id,attempt_id,phase,predicate=None):
    check(); db.execute('BEGIN IMMEDIATE')
    try:
        check(); _validate_schema(db); append_on(db,request_id,attempt_id,phase,predicate)
        check(); db.commit(); check()
    except BaseException:
        if db.in_transaction: db.rollback()
        raise


def failure(db,check,request_id,attempt_id,phase,predicate,known_failure):
    check(); db.execute('BEGIN IMMEDIATE')
    try:
        check(); _validate_schema(db)
        append_on(db,request_id,attempt_id,'failure_at_'+phase,predicate)
        append_on(db,request_id,attempt_id,'refused' if known_failure else 'outcome_unknown',predicate)
        check(); db.commit(); check()
    except BaseException:
        if db.in_transaction: db.rollback()
        raise

def status_db(db,request_id,attempt_id=None):
    tables={r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    required={'successor_producer_attempts','successor_producer_attempt_events'}
    if not required & tables:
        return {'request_id':request_id,'state':'legacy_history_unknown','next_action':'no_replay','allowance':'unknown'}
    if not required <= tables:
        raise ValueError('incomplete successor journal schema')
    _validate_schema(db)
    row=db.execute('SELECT request_id,attempt_id,operation,actor,host,source_id,project_id,target_digest,payload_digest,recorded_at FROM successor_producer_attempts WHERE request_id=?',(request_id,)).fetchone()
    if row is None:
        return {'request_id':request_id,'state':'legacy_history_unknown','next_action':'no_replay','allowance':'unknown'}
    if attempt_id is not None and row[1]!=attempt_id: raise ValueError('exact attempt mismatch')
    events=db.execute('SELECT phase,predicate,recorded_at FROM successor_producer_attempt_events WHERE request_id=? AND attempt_id=? ORDER BY seq',(request_id,row[1])).fetchall()
    phases=[r[0] for r in events]
    state='committed' if 'committed' in phases else 'refused' if 'refused' in phases else 'unknown'
    result=dict(zip(('request_id','attempt_id','operation','actor','host','source_id','project_id','target_digest','payload_digest','recorded_at'),row))
    result.update(state=state,events=[dict(phase=e[0],predicate=e[1],recorded_at=e[2]) for e in events],
                  historical_prior_attempts='unknown',allowance='unknown',next_action='readback_only_no_replay')
    return result


def status(path,request_id,attempt_id=None):
    uri=Path(path).resolve(strict=True).as_uri()+'?mode=ro'
    with closing(sqlite3.connect(uri,uri=True,timeout=10)) as db:
        db.execute('PRAGMA query_only=ON');db.execute('BEGIN')
        return status_db(db,request_id,attempt_id)
