"""One configured Root administrative abandonment of the exact obsolete d6b4 guard.

This preserves the OPEN guard and never claims process closure or capacity recovery.
"""
from __future__ import annotations

from pathlib import Path
import os
import math
import sqlite3
import subprocess
import time

RUN = 'remote_run_d6b4b581f256e9f9c2b0ed9954221b3f'
RID = 'reservation_ozTM0ysovUwnzjB95HWDce4hRIw4bP__'
OWNER = 'session_d96238daad0345b699ec1dd1972ab11b'
HOST = 'host_o6KZLd0kkjchCaH8OjoJZ9aZXHDlFiE2'
ROOT = 'session_tcnSV4Qo3EH4YU3gg2swqjO_GS1QQ9Fv'
THREAD = '01a1448a-c377-73e0-9025-02c4f2f6f2d9'
RESOURCE = 'validation-clone'
PID = 27633
START = 1791669860.7163389
OPERATOR = 'message_Lp8OPSOOkC8779Pq2WYEfBIYj6c5RSq1'
SENDER = 'web_operator_qYmkTK9qVoubfWPPTLr8iSzaIKqKk3Si'
BODY = ('"The Hospital build is still blocked by the old native guard and World proof." - '
        'the fuck does this mean? delete the db record, the agent, the file whatever i dont '
        'give a flying fuck, just make progress')

class RetirementRefused(RuntimeError):
    _CODES = frozenset(('identity_mismatch', 'provider_unverified', 'operator_unverified',
        'guard_changed', 'guard_ambiguous', 'pid_unknown', 'original_process_may_be_live',
        'pid_changed', 'registry_changed', 'target_changed', 'audit_changed',
        'database_unavailable'))
    def __init__(self, code):
        self.code = code if code in self._CODES else 'database_unavailable'
        super().__init__(self.code)


def _exact_pid():
    """Inspect one PID only. A reused PID is never treated as the old process."""
    try:
        result = subprocess.run(('/bin/ps', '-p', str(PID), '-o',
            'pid=,ppid=,uid=,lstart=,comm='), capture_output=True, text=True, timeout=3)
    except (OSError, subprocess.TimeoutExpired) as error:
        raise RetirementRefused('pid_unknown') from error
    if result.returncode == 1 and not result.stdout and not result.stderr:
        return ('absent_exact_pid', None, None)
    if result.returncode != 0 or result.stderr or len(result.stdout.splitlines()) != 1:
        raise RetirementRefused('pid_unknown')
    parts = result.stdout.split()
    if len(parts) < 9 or parts[0] != str(PID) or not parts[2].isdigit():
        raise RetirementRefused('pid_unknown')
    from .native_owner_evidence import DarwinEvidence, EvidenceError
    try:
        process = DarwinEvidence().process(PID)
    except (EvidenceError, OSError) as error:
        raise RetirementRefused('pid_unknown') from error
    birth = process.birth_sec + process.birth_usec / 1_000_000
    if (process.pid != PID or process.uid != int(parts[2])
            or not isinstance(birth, (int, float)) or not math.isfinite(birth)):
        raise RetirementRefused('pid_unknown')
    if birth <= START:
        raise RetirementRefused('original_process_may_be_live')
    return ('pid_reused_after_guard_start', birth, process.uid)


def _tables(db):
    return {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _operator(db):
    if not {'messages','operator_identities','operator_message_origins'} <= _tables(db):
        raise RetirementRefused('operator_unverified')
    rows = db.execute('''SELECT m.sender_session,m.recipient_session,m.body,
        o.sender_session,o.channel FROM messages m
        JOIN operator_message_origins o ON o.message_id=m.id
        JOIN operator_identities i ON i.session_id=m.sender_session WHERE m.id=?''',
        (OPERATOR,)).fetchmany(2)
    if len(rows) != 1 or tuple(rows[0]) != (SENDER, ROOT, BODY, SENDER, 'authenticated_operator_ui'):
        raise RetirementRefused('operator_unverified')
    if 'message_batches' in _tables(db) and db.execute(
            'SELECT 1 FROM message_batches WHERE message_id=?', (OPERATOR,)).fetchone():
        raise RetirementRefused('operator_unverified')


def _guard(db, *, require_neutral=True):
    if not {'remote_guarded_runs','guarded_runs','resources','resource_queue'} <= _tables(db):
        raise RetirementRefused('guard_changed')
    local = db.execute('SELECT 1 FROM guarded_runs WHERE resource=? AND closed_at IS NULL',
                       (RESOURCE,)).fetchone()
    rows = db.execute('''SELECT run_id,resource,reservation_id,session_id,host_id,
        local_pid,started_at,closed_at FROM remote_guarded_runs
        WHERE resource=? AND closed_at IS NULL''', (RESOURCE,)).fetchmany(2)
    if local is not None or len(rows) != 1:
        raise RetirementRefused('guard_ambiguous')
    actual = ('remote_guarded_runs', *tuple(rows[0])[:6], None,
              rows[0]['started_at'], rows[0]['closed_at'])
    expected = ('remote_guarded_runs', RUN, RESOURCE, RID, OWNER, HOST, PID, None, START, None)
    if actual != expected:
        raise RetirementRefused('guard_changed')
    if require_neutral:
        for resource in ('validation-clone', 'heavy-unity:2'):
            claims = db.execute('''SELECT owner_session,reservation_id,token,granted_at,
                deadline,stale FROM resources WHERE name=?''', (resource,)).fetchmany(2)
            if len(claims) != 1 or tuple(claims[0]) != (None,None,None,None,None,0):
                raise RetirementRefused('guard_changed')
            if db.execute('SELECT 1 FROM resource_queue WHERE resource=?',
                          (resource,)).fetchone() is not None:
                raise RetirementRefused('guard_changed')
    return expected


def is_retired_guard(db, table, row):
    """Classify only the immutable exact audit and still-matching source guard."""
    if table != 'remote_guarded_runs' or row.get('run_id') != RUN:
        return False
    expected = (RUN, RESOURCE, RID, OWNER, HOST, PID, None, START, None)
    actual = tuple(row.get(key) for key in ('run_id','resource','reservation_id',
        'owner_session','host_id','pid','pgid','started_at','closed_at'))
    if actual != expected:
        return False
    try:
        return _existing(db) is not None
    except RetirementRefused:
        return False


def _registry(db):
    if 'root_config' not in _tables(db):
        raise RetirementRefused('registry_changed')
    row = db.execute('SELECT home_project,home_session,host_id,thread_id FROM root_config WHERE singleton=1').fetchone()
    if row is None or tuple(row) != ('default', ROOT, HOST, THREAD):
        raise RetirementRefused('registry_changed')


def _existing(db):
    if 'root_d6b4_guard_retirements' not in _tables(db):
        return None
    info = list(db.execute('PRAGMA table_info(root_d6b4_guard_retirements)'))
    columns = [row[1] for row in info]
    if columns != ['run_id','reservation_id','owner_session','host_id','pid','started_at',
                   'operator_message_id','root_session','root_thread','state',
                   'original_process','restoration','pid_observation','retired_at'] or info[0][5] != 1:
        raise RetirementRefused('audit_changed')
    triggers = {row[0] for row in db.execute("""SELECT name FROM sqlite_master
        WHERE type='trigger' AND tbl_name='root_d6b4_guard_retirements'""")}
    if triggers != {'root_d6b4_guard_retirements_no_update',
                    'root_d6b4_guard_retirements_no_delete'}:
        raise RetirementRefused('audit_changed')
    rows = db.execute('SELECT * FROM root_d6b4_guard_retirements WHERE run_id=?', (RUN,)).fetchmany(2)
    if not rows:
        raise RetirementRefused('audit_changed')
    if len(rows) != 1:
        raise RetirementRefused('audit_changed')
    row = rows[0]
    if (tuple(row)[:12] != (RUN,RID,OWNER,HOST,PID,START,OPERATOR,ROOT,THREAD,
                           'administratively_abandoned','unverified','unverified')
            or row['pid_observation'] not in ('absent_exact_pid','pid_reused_after_guard_start')
            or not isinstance(row['retired_at'], (int, float))
            or not math.isfinite(row['retired_at']) or row['retired_at'] <= 0):
        raise RetirementRefused('audit_changed')
    return row


def _create_audit(db):
    db.execute('''CREATE TABLE IF NOT EXISTS root_d6b4_guard_retirements(
        run_id TEXT PRIMARY KEY, reservation_id TEXT NOT NULL, owner_session TEXT NOT NULL,
        host_id TEXT NOT NULL, pid INTEGER NOT NULL, started_at REAL NOT NULL,
        operator_message_id TEXT NOT NULL, root_session TEXT NOT NULL,
        root_thread TEXT NOT NULL, state TEXT NOT NULL CHECK(state='administratively_abandoned'),
        original_process TEXT NOT NULL CHECK(original_process='unverified'),
        restoration TEXT NOT NULL CHECK(restoration='unverified'),
        pid_observation TEXT NOT NULL, retired_at REAL NOT NULL)''')
    for operation in ('UPDATE','DELETE'):
        db.execute(f'''CREATE TRIGGER IF NOT EXISTS root_d6b4_guard_retirements_no_{operation.lower()}
            BEFORE {operation} ON root_d6b4_guard_retirements BEGIN
            SELECT RAISE(ABORT,'immutable administrative retirement'); END''')


def retire(master, *, home_project, home_session, host_id, enrolled, pid_probe=None):
    """Commit only a fixed immutable administrative record under registry-before-target locks."""
    from . import root_local
    from .capacity_admission import registry_authority_gate
    if (home_project,home_session,host_id,enrolled) != ('default',ROOT,HOST,True):
        raise RetirementRefused('identity_mismatch')
    config = root_local._installed_server_config()
    owner = config.plist.lstat().st_uid
    registry = root_local._file(Path(str(master) + '.projects.sqlite3'), owner)
    target = root_local._file(master, owner)
    identity = registry.lstat()
    identity = (identity.st_dev, identity.st_ino, identity.st_uid, identity.st_mode)
    target_identity = target.lstat()
    target_identity = (target_identity.st_dev,target_identity.st_ino,target_identity.st_uid,target_identity.st_mode)
    probe = pid_probe or _exact_pid
    try:
        with registry_authority_gate(registry, owner, identity) as (gate, check):
            _registry(gate.db)
            db = sqlite3.connect(target.as_uri() + '?mode=rw', uri=True,
                                 timeout=3, isolation_level=None)
            db.row_factory = sqlite3.Row
            try:
                attached = db.execute('PRAGMA database_list').fetchone()
                if attached is None or Path(attached['file']).resolve() != target.resolve():
                    raise RetirementRefused('target_changed')
                db.execute('BEGIN IMMEDIATE')
                check(); _registry(gate.db)
                now = target.lstat()
                if (now.st_dev,now.st_ino,now.st_uid,now.st_mode) != target_identity:
                    raise RetirementRefused('target_changed')
                _operator(db)
                _guard(db, require_neutral=False)
                prior = _existing(db)
                if prior is not None:
                    db.rollback()
                    return _result('already_retired', prior['pid_observation'])
                _guard(db)
                first = probe()
                if first[0] not in ('absent_exact_pid','pid_reused_after_guard_start'):
                    raise RetirementRefused('pid_unknown')
                second = probe()
                if second != first:
                    raise RetirementRefused('pid_changed')
                check(); _registry(gate.db)
                _guard(db)
                _create_audit(db)
                db.execute('''INSERT INTO root_d6b4_guard_retirements VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                    (RUN,RID,OWNER,HOST,PID,START,OPERATOR,ROOT,THREAD,
                     'administratively_abandoned','unverified','unverified',
                     first[0],time.time()))
                check()
                db.commit()
                return _result('retired', first[0])
            except BaseException:
                if db.in_transaction:
                    db.rollback()
                raise
            finally:
                db.close()
    except RetirementRefused:
        raise
    except (sqlite3.Error, OSError, ValueError, TypeError) as error:
        raise RetirementRefused('database_unavailable') from error


def _result(action, pid_observation):
    return {'operation':'retire-obsolete-d6b4','action':action,
        'run_id':RUN,'reservation_id':RID,'state':'administratively_abandoned',
        'original_process':'unverified','restoration':'unverified',
        'pid_observation':pid_observation,'guard_row':'preserved_open',
        'resource_state':'not_asserted','issuer_completion':'not_asserted',
        'native_state':'not_observed','mutation_scope':'retirement_record_only'}
