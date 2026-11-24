"""Fixed Root-local Native02 evidence, read only and without native observation."""
from contextlib import closing
from pathlib import Path
import math
import os
import sqlite3
import stat
import time
from . import capacity_observer as observer, successor_admission as successor
from .core import CoordError
from .service import ServiceError

class ReadbackUnavailable(RuntimeError):
    _CODES=frozenset(('row_limit','family_untrusted','family_changed','family_unavailable',
                     'family_device_mismatch','root_packet_mismatch','root_thread_mismatch',
                     'master_not_wal','schema_unavailable','request_rows_ambiguous',
                     'generation_unavailable','registry_unverified','sqlite_read_error',
                     'filesystem_read_error','snapshot_data_invalid','configured_identity_mismatch'))
    def __init__(self, code, *, number=None):
        if code not in self._CODES:
            code='snapshot_data_invalid'
        self.code=code
        self.number=number if type(number) is int and 0 <= number <= 65535 else None
        super().__init__(code)

def failure(error):
    """Return only bounded, approved diagnostic identifiers; never exception text."""
    if isinstance(error,ReadbackUnavailable):
        result={'reason':error.code}
        if error.number is not None: result['error_number']=error.number
    elif isinstance(error,sqlite3.Error):
        result={'reason':'sqlite_read_error'}
        number=getattr(error,'sqlite_errorcode',None)
        if type(number) is int and 0 <= number <= 65535: result['error_number']=number
    elif isinstance(error,OSError):
        result={'reason':'filesystem_read_error'}
        if type(error.errno) is int and 0 <= error.errno <= 65535: result['error_number']=error.errno
    elif isinstance(error,ServiceError):
        result={'reason':'provider_unavailable'}
    elif isinstance(error,CoordError):
        result={'reason':'configured_preflight_unverified'}
    else:
        result={'reason':'readback_unavailable'}
    return result

_REQUIRED=('successor_admissions','successor_admission_observations','successor_admission_uses',
           'resources','resource_queue','guarded_runs','remote_guarded_runs','root_guard_generation')

def _bounded(db, query, args):
    rows=db.execute(query, args).fetchmany(65)
    if len(rows)>64:
        raise ReadbackUnavailable('row_limit')
    return [dict(row) for row in rows]

def _family(target, *, allow_closed=False):
    """Check a trusted WAL family, including SQLite's closed sidecar state."""
    identities=[]
    paths=(Path(target), Path(str(target)+'-wal'), Path(str(target)+'-shm'))
    for path in paths:
        try:
            before=path.lstat()
        except FileNotFoundError as error:
            if path == paths[0] or not allow_closed:
                raise ReadbackUnavailable('family_unavailable',number=error.errno) from error
            identities.append(None)
            continue
        except OSError as error:
            raise ReadbackUnavailable('family_unavailable',number=error.errno) from error
        try:
            if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.getuid()
                    or before.st_mode & 0o022 or path.resolve(strict=True) != path):
                raise ReadbackUnavailable('family_untrusted')
            fd=os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
            try:
                opened=os.fstat(fd)
                if (opened.st_dev,opened.st_ino)!=(before.st_dev,before.st_ino):
                    raise ReadbackUnavailable('family_changed')
            finally:
                os.close(fd)
            identities.append((before.st_dev,before.st_ino))
        except OSError as error:
            raise ReadbackUnavailable('family_unavailable',number=error.errno) from error
    if identities[1:] == [None,None]:
        return tuple(identities)
    if None in identities:
        raise ReadbackUnavailable('family_unavailable',number=2)
    if len({item[0] for item in identities}) != 1:
        raise ReadbackUnavailable('family_device_mismatch')
    return tuple(identities)

def _valid_time(value):
    return isinstance(value,(int,float)) and not isinstance(value,bool) and math.isfinite(value) and value > 0

def inspect(target, *, registry, home_project, home_session, host_id):
    """Single master read transaction; registry scope is separately sampled."""
    if (home_project,home_session,host_id)!=('default',successor.ROOT,successor.HOST):
        raise ReadbackUnavailable('root_packet_mismatch')
    try:
        family_before=_family(target,allow_closed=True)
        registry_observed_at=time.time()
        scope=observer._root_scope(registry,home_project,home_session,host_id,'default')
        registry_read_completed_at=time.time()
        if scope['root_thread']!=successor.ROOT_THREAD:
            raise ReadbackUnavailable('root_thread_mismatch')
        with closing(observer._open_ro(target)) as db:
            if db.execute('PRAGMA journal_mode').fetchone()[0].lower() != 'wal':
                raise ReadbackUnavailable('master_not_wal')
            family_open=_family(target)
            if family_before[1:] != (None,None) and family_open != family_before:
                raise ReadbackUnavailable('family_changed')
            if family_open[0] != family_before[0]:
                raise ReadbackUnavailable('family_changed')
            tables={row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            snapshot_started_at=time.time()
            if not set(_REQUIRED)<=tables:
                raise ReadbackUnavailable('schema_unavailable')
            rid=successor.NATIVE02_REQUEST
            ticket=_bounded(db,'SELECT request_id,resource,consumer,thread,host,project_root,manager,authority_kind,management_request,generation,guard_table,guard_run,guard_reservation,guard_owner,guard_host,guard_pid,guard_pgid,guard_started,guard_closed,process_state FROM successor_admissions WHERE request_id=?',(rid,))
            obs=_bounded(db,'SELECT request_id,consumer,thread,host,native_state,direct_input,queue_count,turn_id_known,started_at,finished_at FROM successor_admission_observations WHERE request_id=?',(rid,))
            use=_bounded(db,'SELECT request_id,resource,consumer,reservation_id,used_at FROM successor_admission_uses WHERE request_id=?',(rid,))
            if max(len(ticket),len(obs),len(use))>1:
                raise ReadbackUnavailable('request_rows_ambiguous')
            state='inconsistent'
            if not ticket and not obs and not use:
                state='no_rows_at_snapshot'
            elif len(ticket)==1 and len(obs)==1:
                t,o=ticket[0],obs[0]
                guard=(t['guard_table'],t['guard_run'],t['resource'],t['guard_reservation'],t['guard_owner'],t['guard_host'],t['guard_pid'],t['guard_pgid'],t['guard_started'],t['guard_closed'])
                authority_ok=(t['project_root']==scope['project_root_session'] and
                    t['authority_kind'] in ('managed','native-child') and
                    isinstance(t['manager'],str) and bool(t['manager']) and
                    ((t['authority_kind']=='managed' and isinstance(t['management_request'],str) and bool(t['management_request'])) or
                     (t['authority_kind']=='native-child' and t['management_request'] is None and t['manager']==scope['project_orchestrator_session'])) and
                    isinstance(t['generation'],int) and not isinstance(t['generation'],bool) and t['generation']>=0)
                times_ok=_valid_time(o['started_at']) and _valid_time(o['finished_at']) and o['started_at']<=o['finished_at']<=snapshot_started_at
                if (t['request_id'],t['resource'],t['consumer'],t['thread'],t['host'],t['process_state']) == (rid,successor.RESOURCE,successor.CONSUMER,successor.THREAD,successor.HOST,'UNKNOWN') and guard == successor.GUARD and authority_ok and times_ok and (o['request_id'],o['consumer'],o['thread'],o['host'],o['native_state'],o['direct_input'],o['queue_count'],o['turn_id_known']) == (rid,successor.CONSUMER,successor.THREAD,successor.HOST,'idle',1,0,0):
                    state='recorded_no_use_at_snapshot' if not use else 'inconsistent'
                    if len(use)==1 and (use[0]['request_id'],use[0]['resource'],use[0]['consumer']) == (rid,successor.RESOURCE,successor.CONSUMER) and isinstance(use[0]['reservation_id'],str) and use[0]['reservation_id'] and _valid_time(use[0]['used_at']) and o['finished_at']<=use[0]['used_at']<=snapshot_started_at:
                        state='recorded_use_at_snapshot'
            resources=_bounded(db,"SELECT name,owner_session,reservation_id,deadline,stale FROM resources WHERE name IN (?,?) ORDER BY name",(successor.RESOURCE,'heavy-unity:2'))
            queue=_bounded(db,"SELECT resource,seq,session,queued_at FROM resource_queue WHERE resource IN (?,?) ORDER BY resource,seq",(successor.RESOURCE,'heavy-unity:2'))
            local=_bounded(db,"SELECT run_id,resource,reservation_id,session AS owner_session,pid,pgid,started_at,closed_at FROM guarded_runs WHERE resource IN (?,?) AND closed_at IS NULL ORDER BY run_id",(successor.RESOURCE,'heavy-unity:2'))
            remote=_bounded(db,"SELECT run_id,resource,reservation_id,session_id AS owner_session,host_id,local_pid AS pid,started_at,closed_at FROM remote_guarded_runs WHERE (resource IN (?,?) AND closed_at IS NULL) OR run_id=? ORDER BY run_id",(successor.RESOURCE,'heavy-unity:2',successor.GUARD[1]))
            generation=_bounded(db,'SELECT generation FROM root_guard_generation WHERE singleton=1',())
            if len(generation)!=1: raise ReadbackUnavailable('generation_unavailable')
            if _family(target)!=family_open:
                raise ReadbackUnavailable('family_changed')
            from .successor_attempt_journal import status_db
            producer_attempt = status_db(db, rid)
            return {'operation':'capacity-successor-native02-readback','request_id':rid,
                    'producer_attempt':producer_attempt,
                    'state':state,'ticket':ticket,'observation':obs,'use':use,
                    'resources':resources,'queue':queue,'local_open_guards':local,'remote_guards':remote,
                    'guard_generation':generation[0]['generation'], 'registry_scope':scope,
                    'consistency':'single master read transaction; registry sampled separately',
                    'classification_scope':'recorded packet and basic authority/time coherence only; current consumer authority not asserted',
                    'registry_observed_at':registry_observed_at,
                    'registry_read_completed_at':registry_read_completed_at,
                    'snapshot_started_at':snapshot_started_at,
                    'snapshot_read_completed_at':time.time(),
                    'issuer_completion':'not asserted','native_state':'not observed',
                    'sql_mutation_performed':False,
                    'filesystem_limit':'normal SQLite read-only WAL/SHM coordination; no byte-level sidecar guarantee'}
    except ReadbackUnavailable:
        raise
    except observer.Blocked as error:
        raise ReadbackUnavailable('registry_unverified') from error
    except sqlite3.Error as error:
        raise ReadbackUnavailable('sqlite_read_error',number=getattr(error,'sqlite_errorcode',None)) from error
    except OSError as error:
        raise ReadbackUnavailable('filesystem_read_error',number=error.errno) from error
    except (KeyError, TypeError, ValueError) as error:
        raise ReadbackUnavailable('snapshot_data_invalid') from error


def inspect_attempt(target, *, registry, home_project, home_session, host_id, request_id, attempt_id):
    """Read one observed attempt; its presence or absence conveys no authority."""
    from .successor_attempt_journal import status_db
    if home_project != 'default':
        raise ReadbackUnavailable('root_packet_mismatch')
    if request_id not in successor.REQUESTS or (attempt_id is not None and (not isinstance(attempt_id,str) or len(attempt_id)!=32 or any(c not in '0123456789abcdef' for c in attempt_id))):
        raise ReadbackUnavailable('snapshot_data_invalid')
    try:
        before=_family(target,allow_closed=True)
        scope=observer._root_scope(registry,home_project,home_session,host_id,'default')
        with closing(observer._open_ro(target)) as db:
            if db.execute('PRAGMA journal_mode').fetchone()[0].lower() != 'wal':
                raise ReadbackUnavailable('master_not_wal')
            opened=_family(target)
            if opened[0] != before[0] or (before[1:] != (None,None) and before != opened):
                raise ReadbackUnavailable('family_changed')
            caller=db.execute('''SELECT b.thread_id,b.parent_session,h.host_id
                FROM sessions s JOIN bridge_bindings b ON b.session_id=s.id
                JOIN remote_session_hosts h ON h.session_id=s.id WHERE s.id=?''',(home_session,)).fetchmany(2)
            if len(caller)!=1 or tuple(caller[0])!=(scope['root_thread'],None,host_id):
                raise ReadbackUnavailable('configured_identity_mismatch')
            result=status_db(db,request_id,attempt_id)
            uses=db.execute('''SELECT u.request_id,u.resource,u.consumer,u.reservation_id,
                    t.request_id,t.resource,t.consumer FROM successor_admission_uses u
                    LEFT JOIN successor_admissions t ON t.request_id=u.request_id WHERE u.request_id=?''',
                    (request_id,)).fetchmany(2)
            if len(uses)>1:
                raise ReadbackUnavailable('request_rows_ambiguous')
            consumption='not_recorded_at_snapshot'
            if uses:
                row=uses[0]
                if (row[0],row[1],row[2],row[4],row[5],row[6]) != (request_id,successor.RESOURCE,successor.CONSUMER,request_id,successor.RESOURCE,successor.CONSUMER) or not isinstance(row[3],str) or not row[3]:
                    raise ReadbackUnavailable('snapshot_data_invalid')
                consumption='recorded_use'
            if _family(target)!=opened:
                raise ReadbackUnavailable('family_changed')
            if observer._root_scope(registry,home_project,home_session,host_id,'default') != scope:
                raise ReadbackUnavailable('configured_identity_mismatch')
            return {'operation':'capacity-successor-attempt-status','request_id':request_id,
                    'state':'UNKNOWN' if result['state'] in ('unknown','legacy_history_unknown') else result['state'],
                    'attempt_id':attempt_id,'attempt_id_semantics':'requested selector, not proof of an attempt',
                    'producer_attempt':result,'consumption':consumption,'next_action':'no_replay','sql_mutation_performed':False,
                    'native_state':'not observed','issuer_completion':'not asserted',
                    'classification_scope':'journal observation only; no execution or allowance authorization'}
    except ReadbackUnavailable:
        raise
    except observer.Blocked as error:
        raise ReadbackUnavailable('registry_unverified') from error
    except sqlite3.Error as error:
        raise ReadbackUnavailable('sqlite_read_error',number=getattr(error,'sqlite_errorcode',None)) from error
    except OSError as error:
        raise ReadbackUnavailable('filesystem_read_error',number=error.errno) from error
    except (KeyError,TypeError,ValueError) as error:
        raise ReadbackUnavailable('snapshot_data_invalid') from error
