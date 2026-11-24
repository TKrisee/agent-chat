"""HTTP client commands and the local Codex wake bridge."""
import argparse
import base64
import hashlib
import json
import os
import secrets
from pathlib import Path
import signal
import subprocess
import threading
import time
import sqlite3
import sys
import urllib.error
import uuid

from . import core
from .processes import receipt_pid_alive
from .receipts import process_closure_proofs, read_process_evidence


def _finalizer_read(path, limit):
    from .private_artifacts import _parent, _check
    parent, name = _parent(path)
    try:
        fd = os.open(name, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW, dir_fd=parent)
        try:
            _check(os.fstat(fd))
            with os.fdopen(fd, 'rb', closefd=False) as source: data = source.read(limit + 1)
        finally: os.close(fd)
    finally: os.close(parent)
    if not data or len(data) > limit: raise core.CoordError('finalizer private input is empty or oversized')
    return data


def _finalizer_jq(data, expression, *, slurp=False, args=()):
    try:
        result = subprocess.run(['jq', '-er' + ('s' if slurp else ''), *args, expression],
                                input=data, capture_output=True, timeout=15)
    except (OSError, subprocess.TimeoutExpired):
        raise core.CoordError('finalizer requires bounded jq validation') from None
    if result.returncode: raise core.CoordError('finalizer input or prospective journal is invalid')
    return result.stdout.decode().rstrip('\n').splitlines()


def _recover_declared_guard(call, params, *, session, host, server, project):
    """Observe one prospectively enabled same-boot group; do not execute or release."""
    from .private_artifacts import ProcessEvidence, read_capability
    from .capacity_observer import _boot_time
    from .guard_witness import observe_file
    cap = read_capability(params['capability_file'])
    if (cap['session_id'],cap['host_id'],cap['server'],cap['project'],cap['reservation_id']) != (session,host,server,project,params['reservation_id']):
        raise core.CoordError('declared recovery requires exact original owner, host and reservation')
    journal = _finalizer_read(params['process_evidence_file'], 16*1024*1024)
    expr = '''. as $all |
      if [.[].sequence] != [range(0;length)] or any(.[];.session_id!=$sid or .host_id!=$host or .server!=$server or .project!=$project) then error("identity") else . end |
      (map(select(.event=="prospective_recovery_intent" and .run_id==$run))) as $i |
      (map(select(.event=="inert_launcher_revalidated"))) as $w |
      if ($i|length)!=1 or ($w|length)!=1 or $i[0].observed_at<$w[0].observed_at or $i[0].child_pid!=$w[0].pid or $w[0].pgid!=$w[0].pid
        then error("no prospective intent") else
        [$w[0].pid,$w[0].parent_pid,$w[0].pgid,$w[0].birth_sec,$w[0].birth_usec,$w[0].boot_id,$w[0].uid,$w[0].ruid,$w[0].executable_sha256] | @tsv end'''
    fields = _finalizer_jq(journal,expr,slurp=True,args=('--arg','sid',session,'--arg','host',host,'--arg','server',server,'--arg','project',project,'--arg','run',params['run_id']))
    if len(fields)!=1: raise core.CoordError('declared recovery journal is ambiguous')
    values=fields[0].split('\t')
    if len(values)!=9: raise core.CoordError('declared recovery witness is incomplete')
    pid,parent,pgid,birth,usec,boot,uid,ruid,image = values
    pid,parent,pgid,birth,usec,uid,ruid=map(int,(pid,parent,pgid,birth,usec,uid,ruid))
    if pid<=1 or parent<=1 or pgid!=pid or uid!=os.getuid() or ruid!=os.getuid() or boot!='darwin-boot-'+str(round(_boot_time()*1000000)):
        raise core.CoordError('declared recovery host generation differs')
    def absent():
        if core.Coordinator._alive(parent) or core.Coordinator._alive(pid) or core.Coordinator._alive(pgid,True):
            raise core.CoordError('declared recovery parent or instrumented group remains live or ambiguous')
    context=call('guard-context',{'resource':cap['resource'],'token':cap['token']})
    matches=[r for r in context['runs'] if r['run_id']==params['run_id']]
    if context['reservation_id']!=cap['reservation_id'] or len(matches)!=1:
        raise core.CoordError('declared recovery run or reservation differs')
    run=matches[0]
    if tuple(run.get(k) for k in ('local_pid','parent_pid','pgid','birth_sec','birth_usec','boot_id','uid','ruid','executable_sha256')) != (pid,parent,pgid,birth,usec,boot,uid,ruid,image):
        raise core.CoordError('declared recovery original generation differs')
    file=run.get('file_evidence')
    if not file or not file.get('baseline'):
        raise core.CoordError('declared recovery file baseline unavailable')
    absent(); observed=observe_file(file['path']); absent()
    if observed!=file['baseline']:
        raise core.CoordError('declared recovery file does not match baseline')
    journal_digest=hashlib.sha256(journal).hexdigest()
    output=ProcessEvidence(params['result_file'],session_id=session,host_id=host,server=server,project=project,
                           kind='declared_observational_recovery',original_journal_sha256=journal_digest)
    try:
        output.record('preflight_complete',run_id=params['run_id'],child_pid=pid,child_pgid=pgid,
                      parent_pid=parent,birth_sec=birth,birth_usec=usec,boot_id=boot)
        absent()
        if observe_file(file['path']) != observed:
            raise core.CoordError('declared recovery file changed during observation')
        proof=dict(pid=pid,parent_pid=parent,pgid=pgid,birth_sec=birth,birth_usec=usec,boot_id=boot,
                   observed_at=time.time(),leader_absent=True,parent_absent=True,group_absent=True)
        output.record('group_absence_and_file_observed',run_id=params['run_id'],file_observation=observed,process_observation=proof)
        absent()
        # The digest binds the original journal and the fsynced pre-effect
        # recovery-evidence prefix. Later outcome records are not in that prefix.
        prefix = _finalizer_read(params['result_file'],1024*1024)
        digest = hashlib.sha256(journal + prefix).hexdigest()
        try:
            result=call('guard-recovery-settle',{'run_id':params['run_id'],'token':cap['token'],'evidence_sha256':digest,'observation':observed,'process_observation':proof})
            if result.get('run_id')!=params['run_id'] or result.get('closed') is not True:
                raise core.CoordError('declared recovery response unresolved')
        except Exception as error:
            output.record('response_unknown',run_id=params['run_id'])
            latest=call('guard-context',{'resource':cap['resource'],'token':cap['token']})
            found=[r for r in latest['runs'] if r['run_id']==params['run_id']]
            if latest.get('reservation_id')!=cap['reservation_id'] or len(found)!=1 or found[0]['closed_at'] is None or found[0]['close_evidence_sha256']!=digest or found[0].get('file_evidence',{}).get('final')!=observed:
                raise core.CoordError('declared recovery remains UNKNOWN; no retry: '+str(error)) from None
            result={'run_id':params['run_id'],'closed':True,'reconciled':True}
        output.record('closed_observed',run_id=params['run_id'])
        return result
    finally:
        output.close()


def _finalize_existing_guards(call, params, *, session, host, server, project):
    """Close only journal-bound ended runs; never restore bytes or return rights."""
    from .private_artifacts import ProcessEvidence
    manifest = _finalizer_read(params['batch_manifest'], 65536)
    lines = _finalizer_jq(manifest, '''if type=="array" and length>=1 and length<=64 and
      all(.[];type=="object" and (keys|sort)==["host_id","reservation_id","resource","session_id","token"] and
        all(.[];type=="string" and length>0 and (test("[\\u0000-\\u001f\\u007f]")|not) and (contains("\\\\")|not)))
      then .[]|[.resource,.session_id,.host_id,.reservation_id,.token]|@tsv else error("invalid") end''')
    tuples = [dict(zip(('resource','session_id','host_id','reservation_id','token'), line.split('\t'))) for line in lines]
    if (len({v['resource'] for v in tuples}) != len(tuples) or
        any(v['session_id'] != session or v['host_id'] != host for v in tuples)):
        raise core.CoordError('finalizer requires distinct original own host/session tuples')
    journal = _finalizer_read(params['process_evidence_file'], 16 * 1024 * 1024)
    expression = '''. as $all |
      if length<4 or .[0].event!="guard_call_started" or (.[0].operation!="begin-guard-batch" and .[0].operation!="begin-guard") or [.[].sequence] != [range(0;length)] or
        any(.[];.session_id!=$sid or .host_id!=$host or .server!=$server or .project!=$project or
          (.observed_at|type)!="number" or .observed_at<=0 or .observed_at>$now) then error("identity/sequence") else . end |
      (map(select(.event=="admitted"))) as $a |
      (map(select(.event=="launcher_created"))) as $l |
      (map(select(.event=="leader_reaped"))) as $r |
      if ($a|length)!=1 or ($l|length)!=1 or ($r|length)!=1 or
        ($a[0].reservations|type)!="array" or ($a[0].reservations|length)<1 or ($a[0].reservations|length)>64 or
        ($l[0].child_pid|type)!="number" or $l[0].child_pid<=0 or ($l[0].child_pid|floor)!=$l[0].child_pid or
        $l[0].child_pgid!=$l[0].child_pid or $l[0].gate_released!=false or
        $r[0].child_pid!=$l[0].child_pid or $r[0].child_pgid!=$l[0].child_pgid or
        ($r[0].returncode|type)!="number" or ($r[0].returncode|floor)!=$r[0].returncode or
        $all[0].observed_at>$a[0].observed_at or $a[0].observed_at>$l[0].observed_at or $l[0].observed_at>$r[0].observed_at then error("launcher") else . end |
      (map(select(.event=="group_absent" and .child_pgid==$l[0].child_pid and .observed_at>=$r[0].observed_at))|last) as $g |
      if $g==null then error("no group closure") else . end |
      (["META",$l[0].child_pid,$all[0].observed_at,$a[0].observed_at,$l[0].observed_at,$r[0].observed_at,$g.observed_at]|@tsv),
      ($a[0].reservations[] as $v |
       if any([$v.resource,$v.reservation_id,$v.run_id][];type!="string" or length==0 or test("[\\u0000-\\u001f\\u007f]")) then error("run") else . end |
       ($all|map(select(.event=="guard_call_succeeded" and .operation=="attach-guard-pid" and .run_id==$v.run_id and .observed_at>=$l[0].observed_at and .observed_at<=$r[0].observed_at))|first) as $attach |
       if $attach==null then error("missing attached run") else
       ["RUN",$v.resource,$v.reservation_id,$v.run_id,$attach.observed_at]|@tsv end)'''
    lines = _finalizer_jq(journal, expression, slurp=True, args=(
        '--arg','sid',session,'--arg','host',host,'--arg','server',server,'--arg','project',project,
        '--argjson','now',str(time.time())))
    meta = lines[0].split('\t')
    if len(meta) != 7 or meta[0] != 'META': raise core.CoordError('finalizer journal metadata is incomplete')
    pid, begin_at, admitted_at, launched_at, reaped_at, closed_at = int(meta[1]), *map(float, meta[2:])
    admitted = {}
    for line in lines[1:]:
        kind, resource, rid, run_id, attached = line.split('\t')
        if kind != 'RUN' or resource in admitted: raise core.CoordError('finalizer journal has duplicate resources')
        admitted[resource] = (rid, run_id, float(attached))
    if set(admitted) != {v['resource'] for v in tuples} or len({v[1] for v in admitted.values()}) != len(tuples):
        raise core.CoordError('finalizer requires the entire exact journal cohort')
    if receipt_pid_alive(pid, closed_at) or core.Coordinator._alive(pid) or core.Coordinator._alive(pid, True):
        raise core.CoordError('finalizer process/group remains live or PID reuse is ambiguous')
    digest = hashlib.sha256(journal).hexdigest()
    def inspect(item):
        context = call('guard-context', {'resource':item['resource'],'token':item['token']})
        rid, run_id, attached = admitted[item['resource']]
        if context.get('reservation_id') != rid or rid != item['reservation_id']:
            raise core.CoordError('finalizer journal/current reservation mismatch')
        runs = [v for v in context['runs'] if v['run_id'] == run_id]
        if len(runs) != 1: raise core.CoordError('finalizer exact original run is unavailable')
        row = runs[0]
        if (row['local_pid'] != pid or not 0 < begin_at <= row['started_at'] <= admitted_at <= launched_at <= attached <= reaped_at <= closed_at):
            raise core.CoordError('finalizer original run PID/start journal mismatch')
        if 'close_evidence_sha256' not in row:
            raise core.CoordError('finalizer server closure-hash projection unavailable')
        if row['closed_at'] is not None and row['close_evidence_sha256'] != digest:
            raise core.CoordError('finalizer existing closure evidence differs')
        return row
    # Complete every tuple, journal, liveness and authoritative context check before ANY close.
    for item in tuples: inspect(item)
    output = ProcessEvidence(params['result_file'],session_id=session,host_id=host,server=server,project=project,
                             kind='existing_guard_finalization_only',evidence_sha256=digest)
    count = 0
    try:
        output.record('preflight_complete',cohort_size=len(tuples),child_pid=pid,closed_at=closed_at)
        for item in tuples:
            rid, run_id, attached = admitted[item['resource']]
            row = inspect(item)
            if row['closed_at'] is not None:
                output.record('already_closed_verified',resource=item['resource'],reservation_id=rid,run_id=run_id)
                count += 1
                continue
            if receipt_pid_alive(pid, closed_at) or core.Coordinator._alive(pid) or core.Coordinator._alive(pid, True):
                raise core.CoordError('finalizer liveness changed before closure')
            output.record('close_requested',resource=item['resource'],reservation_id=rid,run_id=run_id)
            try:
                response = call('close-guard',{'run_id':run_id,'token':item['token'],'evidence_sha256':digest})
                if response.get('run_id') != run_id or response.get('closed') is not True:
                    raise core.CoordError('finalizer close response unresolved')
            except Exception:
                output.record('close_response_unknown',resource=item['resource'],reservation_id=rid,run_id=run_id)
                # Reconcile by READ only. Never replay an uncertain mutation here.
                reconciled = inspect(item)
                if reconciled['closed_at'] is None:
                    raise core.CoordError('finalizer close remains UNKNOWN; no retry; preserve partial results') from None
                output.record('closed_reconciled',resource=item['resource'],reservation_id=rid,run_id=run_id)
            else:
                verified = inspect(item)
                if verified['closed_at'] is None: raise core.CoordError('finalizer close was not retained')
                output.record('closed_response_verified',resource=item['resource'],reservation_id=rid,run_id=run_id)
            count += 1
        output.record('cohort_closed',count=count)
    except BaseException:
        output.record('finalization_incomplete',confirmed_count=count,cohort_size=len(tuples))
        raise
    finally: output.close()
    return {'all_closed':True,'confirmed_count':count,'evidence_sha256':digest,'result_file':params['result_file'],
            'scope':'exact ended journal cohort only; no restoration, receipt or resource return'}


class _RunLifecycleEvents:
    """Optional, deliberately public-only execution attribution on stderr."""

    _PHASES = frozenset({'begin', 'launcher', 'attach', 'inbox', 'pulse', 'close',
                         'interruption', 'child_exit'})
    _EDGES = frozenset({'start', 'end'})
    _OUTCOMES = frozenset({'started', 'succeeded', 'timeout', 'remote_error',
                           'coordination_error', 'local_error',
                           'received_interruption', 'handled_interruption',
                           'launcher_created', 'child_exit'})

    def __init__(self, enabled, admission_level, timeout_seconds):
        self.enabled = enabled
        self.admission_level = admission_level
        self.timeout_seconds = timeout_seconds
        self.started_at = time.monotonic()
        self.sequence = 0
        self.run_id = None
        self.launcher_pid = None

    def emit(self, phase, edge, outcome, *, inbox_trace_id=None):
        if not self.enabled:
            return
        assert phase in self._PHASES and edge in self._EDGES and outcome in self._OUTCOMES
        assert inbox_trace_id is None or phase == 'inbox'
        event = {
            'pid': os.getpid(),
            'admission_level': self.admission_level,
            'sequence': self.sequence,
            'phase': phase,
            'edge': edge,
            'timeout_seconds': self.timeout_seconds,
            'elapsed_seconds': round(max(0.0, time.monotonic() - self.started_at), 6),
            'outcome': outcome,
        }
        if self.run_id is not None:
            event['run_id'] = self.run_id
        if self.launcher_pid is not None:
            event['launcher_pid'] = self.launcher_pid
        if inbox_trace_id is not None:
            event['inbox_trace_id'] = inbox_trace_id
        self.sequence += 1
        print(json.dumps(event, sort_keys=True, separators=(',', ':')), file=sys.stderr, flush=True)


def _lifecycle_error_category(error):
    """Return a fixed public category, never arbitrary exception text."""
    from .remote import RemoteCoordError
    current, seen = error, set()
    while current is not None and id(current) not in seen:
        if isinstance(current, (TimeoutError, subprocess.TimeoutExpired)):
            return 'timeout'
        if (isinstance(current, urllib.error.URLError)
                and isinstance(current.reason, (TimeoutError, subprocess.TimeoutExpired))):
            return 'timeout'
        seen.add(id(current))
        current = current.__cause__ or current.__context__
    if isinstance(error, RemoteCoordError):
        return 'remote_error'
    if isinstance(error, core.CoordError):
        return 'coordination_error'
    return 'local_error'


def main(argv=None):
    raw = list(sys.argv[1:] if argv is None else argv)
    globals_parser = argparse.ArgumentParser(add_help=False)
    globals_parser.add_argument('--session')
    globals_parser.add_argument('--server', default=os.environ.get('AGENT_CHAT_SERVER') or 'http://127.0.0.1:8765')
    globals_parser.add_argument('--api-token', default=os.environ.get('AGENT_CHAT_API_TOKEN'))
    globals_parser.add_argument('--project', default=os.environ.get('AGENT_CHAT_PROJECT'))
    globals_parser.add_argument('--log-level', choices=('OFF', 'ERROR', 'WARNING', 'INFO', 'DEBUG'), default='OFF')
    globals_parser.add_argument('--log-file')
    global_raw = raw[:raw.index('--')] if '--' in raw else raw
    args, remainder = globals_parser.parse_known_args(global_raw)
    if '--' in raw:
        remainder += raw[raw.index('--'):]
    from .app_logging import LoggingConfig, configure
    if args.log_level != 'OFF' and (not args.log_file or not os.path.isabs(args.log_file)):
        raise core.CoordError('configuration_invalid')
    try:
        # The runtime owns file creation and sink failures.  In particular, a
        # selected but unusable destination must not turn a client command
        # into a logging configuration failure before it can dispatch.
        runtime = configure(LoggingConfig(level=args.log_level, path=args.log_file,
                                          max_bytes=1048576, backups=1))
    except ValueError:
        raise core.CoordError('configuration_invalid') from None
    try:
        return _dispatch_main(remainder, args, args.server, globals_parser, runtime)
    finally:
        try:
            runtime.close()
        except OSError:
            # A best-effort selected sink must not replace the command result
            # or error once dispatch has completed.
            pass


def _dispatch_main(remainder, args, server, globals_parser, runtime):
    # Ordinary client commands use HTTP, with no local database fallback.
    # root-local is an explicit configured-app transport, never a fallback.
    # Flags without a command belong to the default bridge launcher. Keep
    # command-specific flags (including reservation --token) with their command.
    default_bridge = not remainder or (remainder[0].startswith('-') and remainder[0] not in ('-h', '--help'))
    if default_bridge or remainder[0] == 'bridge':
        from .bridge_client import main as bridge_main
        bridge_args = ['--server', args.server]
        if args.api_token is not None:
            bridge_args += ['--api-token', args.api_token]
        if args.project is not None:
            bridge_args += ['--project', args.project]
        if args.session is not None:
            raise core.CoordError('--session applies to agent commands, not the bridge')
        return bridge_main(bridge_args + (remainder if default_bridge else remainder[1:]))
    if remainder[0] == 'project':
        return _project_main(remainder[1:], args, server, runtime)
    if remainder[0] == 'root':
        return _root_main(remainder[1:], args, server, runtime)
    if remainder[0] == 'expired-claim-disposition':
        # Explicit configured-Root local transport, never an HTTP fallback.
        from .expired_claim_disposition import main as expired_claim_main
        return expired_claim_main(remainder[1:])
    if remainder[0] == 'root-local':
        from .root_local import main as root_local_main
        return root_local_main(remainder[1:])
    if remainder[0] == 'doctor':
        from .doctor import main as doctor_main
        return doctor_main(remainder[1:], connection=args)
    if remainder in (['--help'], ['-h']):
        globals_parser.prog = 'agent-chat-client'
        globals_parser.print_help()
        print('\nRun without a command (or use bridge) to start the local Codex app-server and wake bridge.\n'
              'Use bridge --help for startup options; project --help for project commands.\n'
              'Use doctor for read-only connection and tool checks; doctor --bridge also checks Codex.\n')
    return _remote_main(remainder, args, server, runtime)


def entrypoint():
    try:
        return main()
    except (core.CoordError, sqlite3.Error, OSError, ValueError) as error:
        response = {'error': str(error)}
        from .remote import RemoteCurrentnessNoEffect
        if isinstance(error, RemoteCurrentnessNoEffect): response['recovery'] = error.recovery
        print(json.dumps(response), file=sys.stderr)
        return 2


def _logged_call(logger, client, path, payload, *, inbox_trace_id=None):
    params = payload.get('params') if isinstance(payload, dict) else None
    run_id = params.get('run_id') if isinstance(params, dict) else None
    logger.event('operation_start', run_id=run_id, inbox_trace_id=inbox_trace_id)
    try:
        if inbox_trace_id is None:
            result = client.call(path, payload)
        else:
            result = client.call(path, payload, inbox_trace_id=inbox_trace_id)
    except BaseException as error:
        logger.error('operation_failed', error, run_id=run_id, inbox_trace_id=inbox_trace_id)
        raise
    run_id = run_id or result.get('run_id')
    logger.event('operation_complete', run_id=run_id, inbox_trace_id=inbox_trace_id)
    return result


def _publish_attachment_download(result, destination, source_project, attachment_id,
                                 recipient_session, host_id, *, project=None, message_id=None):
    expected = {"attachment_id", "message_id", "name", "mime", "size", "sha256",
                "source_project", "content_base64", "project", "recipient_session", "host_id"}
    if not isinstance(result, dict) or set(result) != expected:
        raise core.CoordError("attachment download response fields are invalid")
    if (result["source_project"] != source_project or result["project"] != (project or source_project)
            or result["attachment_id"] != attachment_id
            or (message_id is not None and result["message_id"] != message_id)
            or result["recipient_session"] != recipient_session or result["host_id"] != host_id):
        raise core.CoordError("attachment download response identity does not match the request")
    if not isinstance(result["message_id"], str) or not result["message_id"].startswith("message_"):
        raise core.CoordError("attachment download message metadata is invalid")
    if not isinstance(result["name"], str) or not isinstance(result["mime"], str):
        raise core.CoordError("attachment download metadata is invalid")
    size = result["size"]
    if (isinstance(size, bool) or not isinstance(size, int)
            or not 0 <= size <= core.Coordinator.attachment_size_limit(result["name"])):
        raise core.CoordError("attachment download size is invalid")
    digest = result["sha256"]
    if (not isinstance(digest, str) or len(digest) != 64
            or any(char not in "0123456789abcdef" for char in digest)):
        raise core.CoordError("attachment download hash is invalid")
    if not isinstance(result["content_base64"], str):
        raise core.CoordError("attachment download content is invalid")
    try:
        content = base64.b64decode(result["content_base64"], validate=True)
    except ValueError as error:
        raise core.CoordError("attachment download content is invalid") from error
    if len(content) != size:
        raise core.CoordError("attachment download size does not match its content")
    if not secrets.compare_digest(hashlib.sha256(content).hexdigest(), digest):
        raise core.CoordError("attachment download hash does not match its content")
    name, mime, _ = core.Coordinator._prepare_attachment(result["name"], content)
    if name != result["name"] or mime != result["mime"]:
        raise core.CoordError("attachment download MIME metadata does not match its content")
    path = Path(destination)
    if not path.is_absolute() or path != Path(os.path.abspath(path)) or path.name in ("", ".", ".."):
        raise core.CoordError("attachment destination must be a normalized absolute file path")
    parent = path.parent
    try:
        if parent.resolve(strict=True) != parent:
            raise core.CoordError("attachment destination must not traverse a symlink")
        directory = os.open(parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
                            | getattr(os, "O_NOFOLLOW", 0))
    except core.CoordError:
        raise
    except OSError as error:
        raise core.CoordError("attachment destination directory is unavailable") from error
    temporary = None
    published = False
    try:
        import stat
        if not stat.S_ISDIR(os.fstat(directory).st_mode):
            raise core.CoordError("attachment destination parent is not a directory")
        temporary = "." + path.name + ".agent-chat-" + secrets.token_hex(12)
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(temporary, flags, 0o600, dir_fd=directory)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
        except BaseException:
            try: os.close(fd)
            except OSError: pass
            raise
        try:
            os.link(temporary, path.name, src_dir_fd=directory, dst_dir_fd=directory,
                    follow_symlinks=False)
            published = True
            os.fsync(directory)
        except FileExistsError as error:
            raise core.CoordError("attachment destination already exists") from error
        except OSError as error:
            if published:
                raise core.CoordError("attachment destination outcome is uncertain; stop and inspect it") from error
            raise core.CoordError("attachment destination could not be published") from error
    finally:
        if temporary is not None:
            try: os.unlink(temporary, dir_fd=directory)
            except FileNotFoundError: pass
        os.close(directory)
    return {key: value for key, value in result.items() if key != "content_base64"} | {"path": str(path)}


def _project_main(raw, args, server, runtime):
    parser = argparse.ArgumentParser(prog='agent-chat-client project')
    sub = parser.add_subparsers(dest='op', required=True)
    sub.add_parser('list')
    create = sub.add_parser('create'); create.add_argument('--name', required=True)
    create.add_argument('--workspace', dest='workspaces', action='append'); create.add_argument('--prompt-file')
    configure = sub.add_parser('configure-workspaces'); configure.add_argument('id')
    configure.add_argument('--workspace', dest='workspaces', action='append', required=True)
    grouped = sub.add_parser('configure-workspaces-batch')
    grouped.add_argument('--project-workspace', action='append', required=True)
    rename = sub.add_parser('rename'); rename.add_argument('id'); rename.add_argument('--name', required=True)
    move = sub.add_parser('move-history', help='preview or confirm a bounded operator/agent history move into your project')
    move.add_argument('--source-project', required=True); move.add_argument('--source-session', required=True)
    move.add_argument('--request-id', required=True)
    move.add_argument('--expected-count', type=int); move.add_argument('--expected-digest')
    move.add_argument('--confirm', action='store_true')
    values = vars(parser.parse_args(raw))
    if values['op'] == 'configure-workspaces-batch':
        grouped = {}
        for item in values.pop('project_workspace'):
            project, separator, member = item.partition('=')
            if not separator or not project or not member:
                raise core.CoordError('--project-workspace requires exact PROJECT_ID=PATH')
            grouped.setdefault(project, []).append(member)
        values['assignments'] = [{'id': project, 'workspaces': members} for project, members in grouped.items()]
    if values['op'] == 'move-history':
        from .remote import HttpClient, client_host_id
        session = args.session or os.environ.get('AGENT_CHAT_SESSION')
        if not session or not args.project:
            raise core.CoordError('history move requires your own session and explicit destination project')
        if values['confirm'] and (values['expected_count'] is None or not values['expected_digest']):
            raise core.CoordError('confirm the exact count and digest from the preview')
        values.update(session=session, host_id=client_host_id(), token=os.environ.get('AGENT_CHAT_TOKEN') if values['confirm'] else None)
        result = _logged_call(runtime.logger('agent_chat.cli'),
                              HttpClient(server, args.api_token, project=args.project,
                                         logger=runtime.logger('agent_chat.remote')),
                              '/api/projects/rpc', values)
        print(json.dumps(result, sort_keys=True))
        return 0
    if 'prompt_file' in values:
        prompt_file = values.pop('prompt_file')
        if prompt_file:
            values['instructions'] = Path(prompt_file).read_text(encoding='utf-8')
        if values.get('workspaces') is None:
            values.pop('workspaces', None)
    from .remote import HttpClient
    # Registry actions work even when a previously selected project no longer exists.
    result = _logged_call(runtime.logger('agent_chat.cli'),
                          HttpClient(server, args.api_token, project='default',
                                     logger=runtime.logger('agent_chat.remote')),
                          '/api/projects/rpc', values)
    print(json.dumps(result, sort_keys=True))
    return 0


def _root_main(raw, args, server, runtime):
    """Resolve only the caller's own root projection; never borrow a peer."""
    from .remote import HttpClient, client_host_id
    session = args.session or os.environ.get('AGENT_CHAT_SESSION')
    if not session:
        raise core.CoordError('root commands require your canonical home --session')
    client = HttpClient(server, args.api_token, project=args.project,
                        logger=runtime.logger('agent_chat.remote'))
    identity = dict(session=session)
    if raw and raw[0] == 'in':
        if len(raw) < 3:
            raise core.CoordError('use root in PROJECT_ID COMMAND ...')
        allowed = {'policy','context','message','inbox','acknowledge','send','agents','agent-status','agent-investigation-preload',
                   'agent-stop','agent-resume','agent-model','agent-model-resolve','session-reset','session-reset-status',
                   'session-reset-cancel','session-reset-resolve','session-reset-retry',
                   'remove-session','bridge-status','status','supervisor-configure','supervisor-status'}
        if raw[2] not in allowed:
            raise core.CoordError('root in accepts project-scoped messaging, readback and agent management only')
        identity['host_id'] = client_host_id()
        route = _logged_call(runtime.logger('agent_chat.cli'), client,
                             '/api/root/rpc', dict(identity, op='route', params={'project':raw[1]}))
        routed = argparse.Namespace(session=route['session'], server=server, api_token=args.api_token,
                                   project=route['project'], root_scope=True)
        return _remote_main(raw[2:], routed, server, runtime)
    from .root_authority import ROOT_AUTHORITY
    parser = argparse.ArgumentParser(prog='agent-chat-client root', description=ROOT_AUTHORITY,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest='op', required=True)
    for operation in ('team-reset-preview','team-reset','team-reset-status'):
        command=sub.add_parser(operation,help='administrative old-team retirement; no process closure or project deletion')
        command.add_argument('--project-id',required=True,dest='project')
        if operation!='team-reset-status':
            command.add_argument('--operator-message',action='append',default=[],dest='message_ids',
                help='UI-triggered work: original server-stamped message; absent refs attest root role only, not human consent')
        if operation!='team-reset-preview': command.add_argument('--request-id',required=True)
        if operation=='team-reset':
            command.add_argument('--expected-digest',required=True)
            command.add_argument('--confirm',action='store_true',help='intent for already authorized operation, not another user dialogue')
    for operation in ('stop-all','resume-all','stop-status'):
        command = sub.add_parser(operation, help='root team lifecycle: preserve policies, identities, inputs and ownership')
        command.add_argument('--request-id', required=True, help='retain the same stop UUID for exact retry/status/resume')
        if operation != 'stop-status': command.add_argument('--confirm', action='store_true')
    fresh = sub.add_parser('fresh-start', help='fresh root conversation, same identity and team state; wait for actual boot consumption')
    fresh.add_argument('--expected-thread', required=True); fresh.add_argument('--prompt-file', required=True)
    fresh.add_argument('--request-id', required=True); fresh.add_argument('--confirm', action='store_true')
    fresh_status = sub.add_parser('fresh-status', help='show retained request state, current binding, proof states, next action and blocker; candidate is not resumable before boot and response proof')
    fresh_status.add_argument('--request-id', required=True)
    enroll = sub.add_parser('enroll', help='join every project with your one neutral main conversation')
    enroll.add_argument('--expected-thread', required=True); enroll.add_argument('--confirm', action='store_true')
    sub.add_parser('status', help='root directory and durable project onboarding state')
    readiness = sub.add_parser('readiness', help='read only minimal existing enrollment/coverage for explicit targets')
    readiness.add_argument('--host-id', required=True, help='exact enrolled caller/coverage host; not authority by itself')
    readiness.add_argument('--expected-thread', required=True, help='exact currently bound canonical Root thread')
    readiness.add_argument('--project-id', required=True, action='append', dest='projects',
                           help='existing Root-enrolled target ID; repeat explicitly, never wildcard discovery')
    configure = sub.add_parser('configure', help='create one independent project orchestrator, with project-only workspace grants')
    configure.add_argument('--project-id', required=True, dest='project')
    configure.add_argument('--agent', default='orchestrator'); configure.add_argument('--cwd'); configure.add_argument('--workspace', dest='workspaces', action='append')
    configure.add_argument('--model'); configure.add_argument('--reasoning-effort', dest='effort')
    configure.add_argument('--prompt-file', required=True); configure.add_argument('--request-id', required=True)
    configure.add_argument('--confirm', action='store_true')
    reconfigure = sub.add_parser('reconfigure-workspaces', help='reset one clean project main to explicit workspace members')
    reconfigure.add_argument('--project-id', required=True, dest='project')
    reconfigure.add_argument('--expected-thread', required=True)
    reconfigure.add_argument('--workspace', dest='workspaces', action='append', required=True)
    reconfigure.add_argument('--model'); reconfigure.add_argument('--reasoning-effort', dest='reasoning_effort')
    reconfigure.add_argument('--prompt-file', required=True); reconfigure.add_argument('--request-id', required=True)
    reconfigure.add_argument('--confirm', action='store_true')
    provision = sub.add_parser('provision', help='start a project orchestrator using its saved creation workspace and instructions')
    provision.add_argument('--project-id', required=True, dest='project')
    provision.add_argument('--agent', default='orchestrator')
    provision.add_argument('--model'); provision.add_argument('--reasoning-effort', dest='effort')
    provision.add_argument('--request-id', required=True); provision.add_argument('--confirm', action='store_true')
    management = sub.add_parser('delegate-management', help='assign retained-main management to designated PO within authorized work; native routing unchanged',
                                description=ROOT_AUTHORITY, formatter_class=argparse.RawDescriptionHelpFormatter)
    management.add_argument('--project-id', required=True, dest='project'); management.add_argument('--po-session', required=True, dest='po')
    management.add_argument('--target-session', required=True, dest='target'); management.add_argument('--expected-host', dest='expected_host')
    binding = management.add_mutually_exclusive_group(required=True)
    binding.add_argument('--expected-thread'); binding.add_argument('--unbound', action='store_true')
    management.add_argument('--request-id', required=True); management.add_argument('--confirm', action='store_true',
        help='confirm existing authorized orchestration intent; no new human approval round trip by default')
    adopt = sub.add_parser('adopt', help='designate an existing project main, without dispatch or replacement')
    adopt.add_argument('--project-id', required=True, dest='project'); adopt.add_argument('--to', required=True, dest='target')
    adopt.add_argument('--expected-thread', required=True); adopt.add_argument('--cwd')
    adopt.add_argument('--workspace', dest='workspaces', action='append')
    adopt.add_argument('--confirm', action='store_true')
    detach = sub.add_parser('detach', help='remove global routing role after every root projection is clean; retain registrations and inboxes')
    detach.add_argument('--confirm', action='store_true')
    resume = sub.add_parser('resume-routing', help='resume a refused clean detach without deleting queued input')
    resume.add_argument('--confirm', action='store_true')
    retry = sub.add_parser('retry-setup', help='reconcile the exact journaled project setup; no new UUID or thread replay')
    retry.add_argument('--project-id', required=True, dest='project'); retry.add_argument('--request-id', required=True)
    retry.add_argument('--confirm', action='store_true')
    params = vars(parser.parse_args(raw)); op = params.pop('op')
    if op == 'readiness':
        identity['host_id'] = params.pop('host_id')
    else:
        identity['host_id'] = client_host_id()
    if op == 'delegate-management':
        params.pop('unbound')
    if 'prompt_file' in params:
        params['prompt'] = Path(params.pop('prompt_file')).read_text(encoding='utf-8')
    result = _logged_call(runtime.logger('agent_chat.cli'), client,
                          '/api/root/rpc', dict(identity, op=op, params=params))
    if op == 'fresh-status':
        from .root_local import fresh_status_notice
        result = fresh_status_notice(result)
    print(json.dumps(result, sort_keys=True))
    return 0


def _prepare_return(params, call, *, expected=None, session=None, host=None):
    """The ordinary per-RID receipt/host proof path, shared by cohort recovery."""
    receipt_path = Path(params.pop('receipt'))
    if expected is not None:
        from .recover_cohort import _read_private
        receipt_bytes = _read_private(receipt_path)
        if hashlib.sha256(receipt_bytes).hexdigest() != expected['receipt_sha256']:
            raise core.CoordError('cohort receipt bytes changed')
    else:
        receipt_bytes = receipt_path.read_bytes()
    receipt = json.loads(receipt_bytes.decode('utf-8'))
    try:
        proofs = process_closure_proofs(receipt)
        reports = read_process_evidence(proofs, receipt_path)
    except (ValueError, OSError) as error:
        raise core.CoordError(str(error)) from error
    if receipt['resource'] != params['resource'] or receipt['restored'] is not True or receipt['processes_closed'] is not True:
        raise core.CoordError('receipt does not bind this restored reservation')
    if not isinstance(receipt['pids'], list) or any(not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0 for pid in receipt['pids']):
        raise core.CoordError('receipt PIDs are invalid')
    if (isinstance(receipt['closed_at'], bool) or not isinstance(receipt['closed_at'], (int, float))
            or not 0 < receipt['closed_at'] <= time.time()):
        raise core.CoordError('receipt closed_at is invalid')
    for pid in receipt['pids']:
        if receipt_pid_alive(pid, proofs.get(pid, receipt)['closed_at']):
            raise core.CoordError('receipt lists a still-live PID (or its start time cannot prove PID reuse)')
    state = call('status', {'resources': [params['resource']]})
    row = next((item for item in state['resources'] if item['resource'] == params['resource']), None)
    if not row or row['reservation_id'] != receipt['reservation_id']:
        raise core.CoordError('receipt does not bind the server reservation')
    context = call('guard-context', {'resource': params['resource']})
    if context['reservation_id'] != receipt['reservation_id']:
        raise core.CoordError('reservation changed while checking proof')
    if expected is not None:
        from .recover_cohort import validate_prepared
        validate_prepared(expected, receipt_bytes, receipt, row, context, params, session, host)
    for run in context['runs']:
        if run['closed_at'] is not None:
            continue
        process_closed_at = proofs.get(run['local_pid'], receipt)['closed_at']
        if process_closed_at < run['started_at']:
            raise core.CoordError('process closure proof predates an unclosed guarded run')
        if run['local_pid'] and (receipt_pid_alive(run['local_pid'], process_closed_at) or core.Coordinator._alive(run['local_pid'], True)):
            raise core.CoordError('guarded process group is still alive; close it before recovery')
    evidence_path = Path(receipt['evidence'])
    if not evidence_path.is_absolute(): evidence_path = receipt_path.parent / evidence_path
    params['receipt'] = receipt
    params['evidence_base64'] = base64.b64encode(evidence_path.read_bytes()).decode('ascii')
    if receipt['version'] == 2:
        params['process_evidence_base64'] = {str(pid): base64.b64encode(data).decode('ascii') for pid, data in reports.items()}
    if expected is not None:
        evidence = base64.b64decode(params['evidence_base64'])
        if hashlib.sha256(evidence).hexdigest() != expected['evidence_sha256']:
            raise core.CoordError('cohort evidence bytes changed')
    return params


def _remote_main(raw, global_args, server, runtime):
    """Client CLI for operations whose state lives exclusively on the server."""
    from .remote import HttpClient, client_host_id
    p = argparse.ArgumentParser(prog='agent-chat-client')
    sub = p.add_subparsers(dest='op', required=True)
    x = sub.add_parser('register'); x.add_argument('--agent', required=True)
    x = sub.add_parser('rename', help='change your name while preserving your session'); x.add_argument('--agent', required=True)
    x = sub.add_parser('activity-adopt', help='adopt an exact routed request as an owned PO goal')
    x.add_argument('--source-request-id', required=True); x.add_argument('--title', required=True)
    x = sub.add_parser('activity-assign', help='associate an exact routed assignment with a delegated worker')
    x.add_argument('--goal-id', required=True); x.add_argument('--worker', required=True)
    x.add_argument('--source-request-id', required=True); x.add_argument('--title', required=True)
    x = sub.add_parser('activity-transition', help='record owned work state; this does not prove native execution')
    x.add_argument('work_id'); x.add_argument('--state', required=True,
        choices=('queued', 'in_progress', 'blocked', 'completed', 'cancelled'))
    x = sub.add_parser('activity-metadata', help='record objective dependencies and next action')
    x.add_argument('work_id'); x.add_argument('--dependency', dest='dependencies', action='append', default=[])
    x.add_argument('--next-action', default=None)
    sub.add_parser('activity-snapshot', help='read durable owned work and linked diagnostic escalations')
    sub.add_parser('activity-export', help='export current PO-owned sources and backlog with explicit evidence and acceptance gaps')
    x = sub.add_parser('activity-evidence-link', help='link a verified customer request to one owned task')
    x.add_argument('work_id'); x.add_argument('--request-id', required=True)
    x = sub.add_parser('activity-evidence-record', help='record scoped artifact, validation or delivery evidence')
    x.add_argument('work_id'); x.add_argument('--request-id', required=True); x.add_argument('--kind', choices=('change','validation','delivery'), required=True)
    x.add_argument('--message-id', required=True); x.add_argument('--before-id'); x.add_argument('--after-id'); x.add_argument('--summary', required=True)
    x = sub.add_parser('activity-evidence-list', help='list request-scoped evidence for one owned task')
    x.add_argument('work_id')
    x = sub.add_parser('activity-history', help='read the recorded state history for exact work')
    x.add_argument('work_id')
    x = sub.add_parser('activity-link-escalation', help='link a distinct diagnostic task to an owner-reported denial')
    x.add_argument('--origin-work-id', required=True); x.add_argument('--diagnostic-work-id', required=True)
    x.add_argument('--denial-message-id', required=True); x.add_argument('--next-action', required=True)
    x = sub.add_parser('inbox'); x.add_argument('--all', action='store_true'); x.add_argument('--agent')
    sub.add_parser('policy', help='read the current complete standing coordination policy')
    x = sub.add_parser('context', help='bounded unread messages and relevant resources')
    x.add_argument('--limit', type=int, default=20); x.add_argument('--max-bytes', type=int, default=12288)
    x.add_argument('--cursor', type=int); x.add_argument('--resource-cursor')
    x.add_argument('--message-id', action='append', dest='message_ids')
    x.add_argument('--resource', action='append', dest='resources')
    x = sub.add_parser('message', help='read one complete message from your own inbox'); x.add_argument('id')
    x = sub.add_parser('sent-message', help='inspect one exact message you sent'); x.add_argument('id')
    x = sub.add_parser('attachment-download', help='download one canonical-home private attachment without overwriting a file')
    x.add_argument('--source-project', required=True); x.add_argument('--attachment-id', required=True)
    x.add_argument('--message-id', help='exact original operator message; required for managed-project downloads')
    x.add_argument('--output', required=True)
    x.add_argument('--resume', action='store_true', help='resume an explicit large TXT/diff attachment')
    x = sub.add_parser('acknowledge'); x.add_argument('id')
    x = sub.add_parser('first-ack-lookup', help='read the exact typed first-ACK association for one source message')
    x.add_argument('source_id'); x.add_argument('--expected-recipient', required=True,
        help='canonical session ID of the original source sender; agent names are not resolved')
    x = sub.add_parser('send'); x.add_argument('--to', action='append'); x.add_argument('--body-file', required=True); x.add_argument('--attach', action='append', default=[]); x.add_argument('--reply-to'); x.add_argument('--ack-reply', action='store_true'); x.add_argument('--assignment-id', help='explicit current owned task provenance for one same-team recipient; first-ACK retry must preserve the same ID or its absence'); x.add_argument('--update-for', help='PO-only opt-in marker for one later direct progress update to the owner of this exact in-progress task'); x.add_argument('--decision-needed', action='store_true', help='explicitly mark this new direct message as requiring an operator decision')
    x.add_argument('--large-attach', help='one resumable inert TXT/diff, Unity package, ZIP, ITR draft or EXR; ordinary attachment limits stay unchanged')
    x.add_argument('--transfer-id', help='retained UUID for this exact large file and message; reuse it after interruption')
    x = sub.add_parser('publication-plan-create', help='request a decision on all declared repository and gitlink effects')
    x.add_argument('--to', required=True); x.add_argument('--repo', action='append', nargs=4, required=True, metavar=('NAME','DESTINATION','REF','COMMIT')); x.add_argument('--gitlink', action='append', nargs=4, default=[], metavar=('PARENT','PATH','CHILD','COMMIT'))
    x = sub.add_parser('publication-plan-status', help='compare a declared plan with an author-owned decision')
    x.add_argument('id'); x.add_argument('--repo', action='append', nargs=4, required=True, metavar=('NAME','DESTINATION','REF','COMMIT')); x.add_argument('--gitlink', action='append', nargs=4, default=[], metavar=('PARENT','PATH','CHILD','COMMIT'))
    x = sub.add_parser('decision-mark', help='author-mark one exact existing direct message as requiring an operator decision'); x.add_argument('id')
    x = sub.add_parser('decision-status', help='read one exact original-author decision outcome and bounded reply state'); x.add_argument('id')
    x = sub.add_parser('decision-diagnostic', help='inspect one exact decision and a supported native dependency boundary'); x.add_argument('id'); x.add_argument('--dependency', choices=['retained-child-review-event'], required=True)
    x = sub.add_parser('decision-dismiss', help='hide one authored decision reminder without recording an outcome'); x.add_argument('id'); x.add_argument('--reason', required=True)
    x = sub.add_parser('decision-answer', help='author-record an exact operator reply to a marked decision; does not assert task completion')
    x.add_argument('id'); x.add_argument('--reply-id', required=True); x.add_argument('--outcome', required=True, choices=('accepted','declined','conditional','clarification'))
    x = sub.add_parser('request'); x.add_argument('resource'); x.add_argument('--minutes', required=True, type=float)
    x.add_argument('--capability-file', required=True, help='owner-only atomic reservation capability destination')
    x = sub.add_parser('deferred-request', help='reserve an exact task file without issuing execution authority')
    x.add_argument('resource'); x.add_argument('--minutes',required=True,type=float)
    x.add_argument('--task-id',required=True); x.add_argument('--source-id',required=True); x.add_argument('--request-id',required=True)
    for name in ('deferred-status','deferred-cancel'):
        x = sub.add_parser(name); x.add_argument('--request-id',required=True)
    x = sub.add_parser('deferred-activate'); x.add_argument('--request-id',required=True); x.add_argument('--capability-file',required=True)
    x = sub.add_parser('exact-file-bind', help='manager: bind one runnable task to an exact snapshot source and destination')
    x.add_argument('--to',required=True); x.add_argument('--task-id',required=True); x.add_argument('--source-request-id',required=True)
    x.add_argument('--expected-host',required=True); x.add_argument('--expected-thread',required=True)
    x.add_argument('--path',required=True); x.add_argument('--output',required=True); x.add_argument('--max-bytes',required=True,type=int)
    x.add_argument('--minutes',required=True,type=float); x.add_argument('--request-id',required=True); x.add_argument('--confirm',action='store_true')
    x = sub.add_parser('exact-file-binding-status'); x.add_argument('--binding-id',required=True)
    x = sub.add_parser('exact-file-revoke'); x.add_argument('--binding-id',required=True); x.add_argument('--request-id',required=True)
    x.add_argument('--reason',required=True); x.add_argument('--confirm',action='store_true')
    x = sub.add_parser('exact-file-request', help='reserve one exact absolute file for a bounded read')
    x.add_argument('--binding-id',default=argparse.SUPPRESS)
    x.add_argument('--path',required=True); x.add_argument('--minutes',required=True,type=float); x.add_argument('--capability-file',required=True)
    x = sub.add_parser('exact-file-read', help='guarded exact-file snapshot into a new private output')
    x.add_argument('--binding-id',default=argparse.SUPPRESS)
    x.add_argument('--path',required=True); x.add_argument('--reservation-id',required=True); x.add_argument('--capability-file',required=True)
    x.add_argument('--output',required=True); x.add_argument('--max-bytes',required=True,type=int); x.add_argument('--process-evidence-file',required=True)
    x = sub.add_parser('exact-file-reconcile', help='record a new read-only observation of a retained closed copy; never replay or release it')
    for flag in ('binding-id','path','output','reservation-id','run-id','capability-file','copy-receipt-file','copy-log-file','process-evidence-file','request-id'):
        x.add_argument('--'+flag,required=True)
    x.add_argument('--max-bytes',required=True,type=int)
    x = sub.add_parser('hospital-vendor-copy', help='fixed task read-only copy under separate owned control and guarded scopes')
    x.add_argument('--resource',required=True); x.add_argument('--output',required=True)
    x.add_argument('--control-capability-file',required=True); x.add_argument('--run-capability-file',required=True)
    x.add_argument('--output-capability-file',required=True); x.add_argument('--process-evidence-file',required=True)
    x = sub.add_parser('exact-file-release', help='return an exact-file reservation with ordinary receipt evidence')
    x.add_argument('--binding-id',default=argparse.SUPPRESS)
    x.add_argument('--path',required=True); x.add_argument('--reservation-id',required=True); x.add_argument('--capability-file',required=True); x.add_argument('--receipt',required=True)
    x = sub.add_parser('exact-file-recover', help='recover a stale exact-file reservation with truthful receipt evidence')
    x.add_argument('--binding-id',default=argparse.SUPPRESS)
    x.add_argument('--path',required=True); x.add_argument('--reservation-id',required=True); x.add_argument('--receipt',required=True)
    for method in ('exact-file-status','exact-file-cancel','exact-file-guard-context'):
        x=sub.add_parser(method); x.add_argument('--path',required=True)
        x.add_argument('--binding-id',default=argparse.SUPPRESS)
    x=sub.add_parser('exact-file-result'); x.add_argument('--path',required=True); x.add_argument('--reservation-id',required=True)
    x.add_argument('--binding-id',default=argparse.SUPPRESS)
    x = sub.add_parser('renew', help='explicitly extend your exact live reservation near its deadline')
    x.add_argument('resource'); x.add_argument('--minutes', required=True, type=float)
    x.add_argument('--reservation-id', required=True); x.add_argument('--capability-file', required=True)
    x = sub.add_parser('status'); x.add_argument('--mine', action='store_true'); x.add_argument('--resource', action='append', dest='resources')
    x = sub.add_parser('guard-status', help='read your exact reservation guards on their owning host')
    x.add_argument('resource'); x.add_argument('--reservation-id', required=True)
    x = sub.add_parser('session-reset', help='queue a fresh Codex conversation, preserving chat identity and queue')
    x.add_argument('--to'); x.add_argument('--expected-thread', required=True)
    x.add_argument('--model'); x.add_argument('--reasoning-effort')
    x.add_argument('--prompt-file', required=True); x.add_argument('--request-id', required=True)
    x.add_argument('--confirm', action='store_true')
    x = sub.add_parser('session-reset-status'); x.add_argument('--to')
    x = sub.add_parser('session-reset-cancel'); x.add_argument('id')
    x = sub.add_parser('session-reset-resolve'); x.add_argument('id')
    x.add_argument('--thread', required=True); x.add_argument('--confirm-created', action='store_true')
    x = sub.add_parser('session-reset-retry'); x.add_argument('id')
    x.add_argument('--confirm-not-started', action='store_true')
    x = sub.add_parser('session-reset-revalidate', help='revalidate a withheld explicit-workspace creation against its captured settings; never recreate')
    x.add_argument('id'); x.add_argument('--confirm', action='store_true')
    x.add_argument('--codex-server', default=os.environ.get('AGENT_CHAT_CODEX_SERVER') or 'ws://127.0.0.1:4500')
    x = sub.add_parser('agent-create', help='create an independent registered agent on your existing host bridge')
    x.add_argument('--agent', required=True); x.add_argument('--expected-thread', required=True)
    x.add_argument('--model'); x.add_argument('--reasoning-effort')
    x.add_argument('--cwd', help='existing absolute workspace on the agent host; permissions remain copied')
    x.add_argument('--prompt-file', required=True); x.add_argument('--request-id', required=True)
    x.add_argument('--confirm', action='store_true')
    x = sub.add_parser('parked-agent-create', help='create a parked subordinate under an admitted stopped PO')
    x.add_argument('--agent', required=True); x.add_argument('--expected-thread', required=True)
    x.add_argument('--model', required=True); x.add_argument('--reasoning-effort', required=True)
    x.add_argument('--cwd'); x.add_argument('--bootstrap-id', required=True)
    x.add_argument('--prompt-file', required=True); x.add_argument('--request-id', required=True)
    x.add_argument('--confirm', action='store_true')
    for command in ('agent-stop','agent-resume'):
        x = sub.add_parser(command); x.add_argument('--to'); x.add_argument('--expected-thread', required=True)
        x.add_argument('--request-id', required=True); x.add_argument('--confirm', action='store_true')
        if command == 'agent-stop': x.add_argument('--interrupt', action='store_true', help='interrupt only the observed current turn; guards and holds remain owned')
    x = sub.add_parser('agent-investigation-preload', help='request one no-override settings preload for an exact admitted same-task continuation')
    x.add_argument('--to', required=True); x.add_argument('--expected-thread', required=True)
    x.add_argument('--continuation-id', required=True); x.add_argument('--request-id', required=True)
    x.add_argument('--confirm', action='store_true')
    x = sub.add_parser('agent-model', help='change model and reasoning effort on the exact existing idle main thread')
    x.add_argument('--to'); x.add_argument('--expected-thread', required=True)
    x.add_argument('--model', required=True); x.add_argument('--reasoning-effort', required=True)
    x.add_argument('--request-id', required=True); x.add_argument('--confirm', action='store_true')
    x.add_argument('--allow-cold-resume', action='store_true',
                   help='allow this bridge to detach its subscription for a verified same-thread cold resume')
    x = sub.add_parser('agent-model-resolve', help='abandon an uncertain model intent only after unchanged-baseline proof')
    x.add_argument('--request-id', required=True, help='the original uncertain model request UUID')
    x.add_argument('--resolution-id', required=True); x.add_argument('--expected-thread', required=True)
    x.add_argument('--confirm', action='store_true')
    x = sub.add_parser('agent-status'); x.add_argument('--to')
    x.add_argument('--refresh', action='store_true'); x.add_argument('--expected-thread'); x.add_argument('--request-id')
    x = sub.add_parser('supervisor-configure', help='enable or disable deterministic checks for your authorized supervision scope')
    enabled = x.add_mutually_exclusive_group(required=True)
    enabled.add_argument('--enable', dest='enabled', action='store_true', default=argparse.SUPPRESS)
    enabled.add_argument('--disable', dest='enabled', action='store_false', default=argparse.SUPPRESS)
    enabled.add_argument('--recovery-enable', dest='recovery_enabled', action='store_true', default=argparse.SUPPRESS,
                         help='consent to recovery independently of passive monitoring')
    enabled.add_argument('--recovery-disable', dest='recovery_enabled', action='store_false', default=argparse.SUPPRESS,
                         help='withdraw recovery consent without changing passive monitoring')
    x = sub.add_parser('supervisor-status', help='read policy, metadata observations and deduplicated incidents without model inference')
    x.add_argument('--to')
    sub.add_parser('agents', help='compact project registration and admission roster')
    x = sub.add_parser('producer-status', help='read exact enrolled producer instance and immutable closure evidence')
    x.add_argument('--run-id', required=True)
    x = sub.add_parser('cancel'); x.add_argument('resource')
    x = sub.add_parser('check'); x.add_argument('resource'); x.add_argument('--token')
    x = sub.add_parser('release'); x.add_argument('resource'); x.add_argument('--receipt', required=True); x.add_argument('--token')
    x = sub.add_parser('resource-uncertainty-record', help='record incomplete evidence without returning a reservation')
    x.add_argument('resource'); x.add_argument('--reservation-id',required=True); x.add_argument('--token')
    x.add_argument('--request-id',required=True); x.add_argument('--reason',required=True)
    x = sub.add_parser('resource-uncertainty-status', help='read an exact owner-scoped uncertainty record')
    x.add_argument('resource'); x.add_argument('--reservation-id',required=True); x.add_argument('--token')
    x.add_argument('--request-id',required=True)
    x = sub.add_parser('recover'); x.add_argument('resource'); x.add_argument('--receipt', required=True)
    x = sub.add_parser('recover-cohort', help='explicit bounded same-owner recovery with a durable resume journal')
    x.add_argument('--manifest', required=True); x.add_argument('--journal', required=True)
    x.add_argument('--resume', action='store_true')
    x = sub.add_parser('block'); x.add_argument('resource'); x.add_argument('--reason', required=True)
    x = sub.add_parser('link-reply'); x.add_argument('id'); x.add_argument('--reply-to', required=True)
    x = sub.add_parser('remove-session'); x.add_argument('id')
    sub.add_parser('deregister')
    x = sub.add_parser('bind'); x.add_argument('--thread'); x.add_argument('--parent-session'); x.add_argument('--agent-path')
    sub.add_parser('unbind')
    x = sub.add_parser('bridge-status')
    scope = x.add_mutually_exclusive_group()
    scope.add_argument('--all', action='store_true', help='show the legacy global bridge diagnostic')
    scope.add_argument('--mine', action='store_true', help='show this session bridge route (the default)')
    x = sub.add_parser('source-trace', help='read timing correlation for one own direct message')
    x.add_argument('source_id')
    x = sub.add_parser('bridge-retry'); x.add_argument('job_id'); x.add_argument('--confirm-not-started', action='store_true')
    x = sub.add_parser('bridge-resolve'); x.add_argument('job_id'); x.add_argument('--confirm-delivered', action='store_true')
    x = sub.add_parser('guard-finalize', help='finalize an exact ended own journal cohort; never restore or return rights')
    x.add_argument('--batch-manifest', required=True)
    x.add_argument('--process-evidence-file', required=True)
    x.add_argument('--result-file', required=True)
    x = sub.add_parser('guard-recover-declared', help='observe and settle a prospectively prepared same-owner declared file run')
    x.add_argument('--capability-file', required=True); x.add_argument('--reservation-id', required=True)
    x.add_argument('--run-id', required=True); x.add_argument('--process-evidence-file', required=True)
    x.add_argument('--result-file', required=True)
    x = sub.add_parser('run'); x.add_argument('resource'); x.add_argument('--token')
    x.add_argument('--capability-file', help='private exact reservation tuple; requires --reservation-id')
    x.add_argument('--process-evidence-file', help='new private journal for factual process and guard evidence')
    x.add_argument('--require-host-witness', action='store_true', help='require a host-reported inert launcher identity before execution')
    x.add_argument('--prospective-recovery', action='store_true', help='prepare observational recovery of this witnessed declared file run')
    x.add_argument('--declared-task-id', help='exact owned task for a declared project file')
    x.add_argument('--declared-source-id', help='exact routed source for the declared task')
    x.add_argument('--batch-manifest', help='private exact same-caller tuples; resource must be batch')
    x.add_argument('--restore', action='store_true', help='bounded same-owner stale reservation restoration only')
    x.add_argument('--reservation-id')
    x.add_argument('--max-seconds', type=int,
                   help='restoration-only deadline budget in seconds (default 120; range 1-900); requires --restore')
    x.add_argument('--lifecycle-events', action='store_true',
                   help='emit sanitized guarded-run lifecycle JSON Lines on stderr')
    x.add_argument('--admission-level', type=int,
                   help='zero-based caller admission level; requires --lifecycle-events')
    x.add_argument('command', nargs=argparse.REMAINDER)
    child_args = raw[raw.index('--') + 1:] if raw and raw[0] == 'run' and '--' in raw else None
    a = p.parse_args(raw[:raw.index('--')] if child_args is not None else raw)
    if child_args is not None: a.command = child_args
    if a.op == 'run' and a.max_seconds is not None and not a.restore:
        raise core.CoordError('--max-seconds requires --restore; it is only a restoration deadline')
    session = global_args.session or os.environ.get('AGENT_CHAT_SESSION')
    if not session and a.op == 'register':
        import secrets
        session = 'session_' + secrets.token_urlsafe(24)
    if not session:
        raise core.CoordError('AGENT_CHAT_SESSION or --session is required in remote mode; missing local session context does not establish that the registration was deleted. For retained Root discovery, run agent-chat-client root-local discover.')
    params = vars(a).copy(); op = params.pop('op')
    if op in ('publication-plan-create', 'publication-plan-status'):
        params['plan'] = {'repositories': params.pop('repo'), 'gitlinks': params.pop('gitlink')}
    vendor_request = params.copy() if op == 'hospital-vendor-copy' else None
    vendor_batch = None; vendor_control = None; vendor_output = None
    exact_op = op if op in ('exact-file-request','exact-file-read','exact-file-release','exact-file-recover','exact-file-status','exact-file-cancel','exact-file-guard-context','exact-file-result','exact-file-reconcile') else None
    exact_binding_scope=None
    if exact_op:
        from .exact_file_read import exact_path, MAX_BYTES
        path=exact_path(params.pop('path'))
        if exact_op=='exact-file-read':
            limit=params.pop('max_bytes'); output=params.pop('output')
            binding=params.pop('binding_id',None)
            if binding is not None:exact_binding_scope=dict(binding_id=binding,output=output,max_bytes=limit)
            if not 1 <= limit <= MAX_BYTES: raise core.CoordError('exact file byte limit out of range')
            params.update(resource=path,token=None,batch_manifest=None,restore=False,max_seconds=None,
                          lifecycle_events=False,admission_level=None,
                          command=[sys.executable,'-I','-c',
                              'import sys; sys.path.insert(0, sys.argv.pop(1)); from agent_chat.exact_file_read import main; main()',
                              str(Path(__file__).resolve().parent.parent),
                              '--path',path,'--output',output,'--max-bytes',str(limit)])
            if exact_binding_scope:params['command'].extend(['--binding-id',binding])
            op='run'
        elif exact_op in ('exact-file-release','exact-file-recover'): params.update(resource=path,token=None)
        elif exact_op=='exact-file-request': params['resource']=path
        else: params['path']=path
    if op == 'agent-status':
        request_id = params.get('request_id')
        if request_id is None and params.get('refresh'):
            params['request_id'] = str(uuid.uuid4())
        elif request_id is not None:
            try:
                uuid.UUID(request_id)
            except (ValueError, TypeError, AttributeError):
                raise core.CoordError('request_id must be a UUID') from None
    download_output = params.pop('output') if op == 'attachment-download' else None
    download_resume = params.pop('resume') if op == 'attachment-download' else False
    large_source = transfer_id = None
    if op == 'bridge-status':
        params['mine'] = not params.pop('all')
    if 'token' in params and not params['token']:
        params['token'] = os.environ.get('AGENT_CHAT_TOKEN')
    if op == 'inbox' and params.pop('agent') is not None:
        raise core.CoordError('--agent cannot read another agent inbox in remote mode')
    if op == 'first-ack-lookup':
        params = {'source_id': params['source_id'],
                  'expected_recipient': params['expected_recipient']}
    if op in ('session-reset', 'agent-create', 'parked-agent-create'):
        params['prompt'] = Path(params.pop('prompt_file')).read_text(encoding='utf-8')
    if op == 'send':
        targets = params['to']
        large_source = params.pop('large_attach')
        transfer_id = params.pop('transfer_id')
        if bool(large_source) != bool(transfer_id):
            raise core.CoordError('--large-attach and --transfer-id must be used together')
        if large_source and (not targets or len(targets) != 1 or params['attach']
                             or params['ack_reply'] or params['decision_needed']):
            raise core.CoordError('large transfer requires one direct recipient and cannot mix attachments, ACK replies or decisions')
        if params['ack_reply'] and not params['reply_to']:
            raise core.CoordError('--ack-reply requires --reply-to')
        if params['ack_reply'] and (not targets or len(targets) != 1):
            raise core.CoordError('--ack-reply is only supported with one --to recipient')
        if not targets and params['reply_to']:
            raise core.CoordError('--reply-to requires an explicit --to recipient')
        if params['decision_needed'] and (not targets or len(targets) != 1 or params['reply_to'] or params['ack_reply']):
            raise core.CoordError('--decision-needed requires one new direct message to one operator recipient')
        if params['update_for'] and (not targets or len(targets) != 1 or params['ack_reply']):
            raise core.CoordError('--update-for requires one later direct non-ACK message to one recipient')
        params['to'] = targets[0] if targets and len(targets) == 1 else targets
        params['body'] = Path(params.pop('body_file')).read_text(encoding='utf-8')
        raw_attachments = params.pop('attach')
        if len(raw_attachments) > core.MAX_ATTACHMENTS:
            raise core.CoordError(f'at most {core.MAX_ATTACHMENTS} attachments are allowed')
        if sum(Path(raw).stat().st_size for raw in raw_attachments) > core.MAX_ATTACHMENT_TOTAL_SIZE:
            raise core.CoordError('attachments exceed 500 MiB total raw size')
        attachments = []
        total = 0
        for index, raw_path in enumerate(raw_attachments, 1):
            name = Path(raw_path).name
            size_limit = core.Coordinator.attachment_size_limit(name)
            with Path(raw_path).open('rb') as stream:
                data = stream.read(size_limit + 1)
            if len(data) > size_limit:
                raise core.CoordError(f'attachment #{index} {name!r} ({Path(raw_path).stat().st_size} bytes): exceeds its format-specific size limit ({size_limit} bytes)')
            total += len(data)
            if total > core.MAX_ATTACHMENT_TOTAL_SIZE:
                raise core.CoordError('attachments exceed 500 MiB total raw size')
            attachments.append({'name': name, 'content_base64': base64.b64encode(data).decode('ascii')})
        params['attachments'] = attachments
    cli_logger = runtime.logger('agent_chat.cli')
    client = HttpClient(server, global_args.api_token, project=global_args.project,
                        logger=runtime.logger('agent_chat.remote'))
    if op == 'attachment-download':
        if params['source_project'] != client.project and not params.get('message_id'):
            raise core.CoordError('managed-project attachment download requires the exact original message ID')
        if params.get('message_id') is None:
            params.pop('message_id', None)
    host = client_host_id()
    def call(operation, values, *, inbox_trace_id=None):
        payload = {'op': operation, 'session': session, 'host_id': host, 'params': values}
        result = _logged_call(cli_logger, client, '/api/coord', payload,
                              inbox_trace_id=inbox_trace_id)
        return dict(result, project=client.project) if getattr(global_args, 'root_scope', False) else result
    if vendor_request is not None:
        from . import hospital_vendor_scope as vendor_scope
        from .private_artifacts import read_capability
        if (server,client.project,session,host)!=('http://127.0.0.1:8765',vendor_scope.PROJECT,vendor_scope.CONSUMER,vendor_scope.HOST):
            raise core.CoordError('fixed vendor caller context differs')
        if vendor_request['resource'] not in vendor_scope.manifest():
            raise core.CoordError('fixed vendor resource not selected')
        vendor_control=read_capability(vendor_request['control_capability_file'])
        vendor_run=read_capability(vendor_request['run_capability_file'])
        vendor_output=read_capability(vendor_request['output_capability_file'])
        for cap in (vendor_control,vendor_run,vendor_output):
            if (cap['session_id'],cap['host_id'],cap['server'],cap['project'])!=(session,host,server,client.project):
                raise core.CoordError('fixed vendor private capability differs')
        if vendor_control['resource']!=vendor_scope.CONTROL or vendor_run['resource']!=vendor_scope.RUN_RESOURCE:
            raise core.CoordError('fixed vendor control or copy scope differs')
        if not vendor_output['resource'].startswith('/') or vendor_output['resource'] in (vendor_run['resource'],vendor_control['resource']):
            raise core.CoordError('fixed vendor output scope differs')
        vendor_batch=[dict(resource=cap['resource'],reservation_id=cap['reservation_id'],session_id=session,host_id=host,token=cap['token']) for cap in (vendor_run,vendor_output)]
        params=dict(resource='batch',token=None,reservation_id=None,capability_file=None,batch_manifest=None,
                    process_evidence_file=vendor_request['process_evidence_file'],restore=False,max_seconds=None,
                    lifecycle_events=False,admission_level=None,
                    command=[sys.executable,'-I','-c',
                      'import sys; sys.path.insert(0, sys.argv.pop(1)); from agent_chat.hospital_vendor_exec import main; main()',
                      str(Path(__file__).resolve().parent.parent),'--resource',vendor_request['resource'],
                      '--output',vendor_request['output'],'--control-rid',vendor_control['reservation_id'],
                      '--output-rid',vendor_output['reservation_id']])
        op='run'
    if op == 'recover-cohort':
        from .recover_cohort import recover_cohort
        def prepare(entry):
            return _prepare_return(dict(resource=entry['resource'], receipt=entry['receipt']), call, expected=entry, session=session, host=host)
        result = recover_cohort(params['manifest'], params['journal'], resume=params['resume'],
                                server=server, project=client.project, session=session, host=host,
                                call=call, prepare=prepare)
        print(json.dumps(result, sort_keys=True))
        return int(result['exit_code'])
    if op == 'guard-finalize':
        result = _finalize_existing_guards(call, params, session=session, host=host, server=server, project=client.project)
        print(json.dumps(result, sort_keys=True))
        return 0
    if op == 'guard-recover-declared':
        result = _recover_declared_guard(call, params, session=session, host=host, server=server, project=client.project)
        print(json.dumps(result, sort_keys=True))
        return 0
    if op == 'producer-status':
        from .lifecycle_producer import credential
        path = os.environ.get('AGENT_CHAT_PRODUCER_CAPABILITY_FILE')
        if not path:
            raise core.CoordError('producer-status requires the private producer capability file')
        params['capability'] = credential(path)
    if op == 'session-reset-revalidate':
        if not params['confirm']:
            raise core.CoordError('workspace revalidation requires --confirm')
        job = call('session-reset-revalidate-info', {'id': params['id']})
        if job.get('workspace_revalidated'):
            print(json.dumps(job, sort_keys=True))
            return 0
        from .creation_settings import revalidation_proof
        from .rpc import RpcClient, RpcError
        try:
            with RpcClient(params.pop('codex_server')) as rpc:
                params['proof'] = revalidation_proof(rpc, job)
        except RpcError as error:
            raise core.CoordError(str(error)) from error
        result = call(op, params)
        print(json.dumps(result, sort_keys=True))
        return 0
    if 'resource' in params and not exact_op:
        from .remote_service import resource_name
        params['resource'] = resource_name(params['resource'])
    if op in ('release', 'recover'):
        params = _prepare_return(params, call)
    if exact_op in ('exact-file-release','exact-file-recover'):
        expected_rid=params.pop('reservation_id')
        if exact_op=='exact-file-release':
            from .private_artifacts import read_capability
            capability=read_capability(params.pop('capability_file'))
            expected=dict(resource=params['resource'],session_id=session,host_id=host,
                          reservation_id=expected_rid,server=server,project=client.project)
            if any(capability[key]!=value for key,value in expected.items()): raise core.CoordError('private exact file capability differs')
            params['token']=capability['token']
        else: params.pop('token',None)
        def typed_call(operation, values):
            scope={'binding_id':params['binding_id']} if 'binding_id' in params else {}
            if operation=='status': return call('exact-file-status',{'path':params['resource'],**scope})
            if operation=='guard-context': return call('exact-file-guard-context',{'path':params['resource'],**scope})
            return call(operation,values)
        params=_prepare_return(params,typed_call)
        if params['receipt']['reservation_id']!=expected_rid: raise core.CoordError('exact file receipt reservation differs')
    if op == 'renew':
        from .private_artifacts import read_capability
        capability = read_capability(params.pop('capability_file'))
        expected = dict(resource=params['resource'], session_id=session, host_id=host,
                        reservation_id=params['reservation_id'], server=server, project=client.project)
        if any(capability[key] != value for key, value in expected.items()):
            raise core.CoordError('private capability tuple does not match this renewal')
        params['token'] = capability['token']
    if op == 'run':
        command = params.pop('command')
        if command and command[0] == '--': command = command[1:]
        if not command: raise core.CoordError('run requires a command after --')
        batch_path = params.pop('batch_manifest')
        capability_path = params.pop('capability_file')
        evidence_path = params.pop('process_evidence_file')
        require_host_witness = params.pop('require_host_witness', False)
        prospective_recovery = params.pop('prospective_recovery', False)
        declared_task = params.pop('declared_task_id', None)
        declared_source = params.pop('declared_source_id', None)
        declared_file = declared_task is not None or declared_source is not None
        if prospective_recovery and (not declared_file or not require_host_witness or not evidence_path or batch_path or params.get('restore')):
            raise core.CoordError('prospective recovery requires one witnessed declared file and a private journal')
        if prospective_recovery: params['require_prospective_recovery'] = True
        if declared_file and (not require_host_witness or not declared_task or not declared_source):
            raise core.CoordError('declared file requires a host witness and exact task/source IDs')
        if require_host_witness and (batch_path or params.get('restore') or not params.get('reservation_id')):
            raise core.CoordError('host witness requires one exact non-restoration reservation')
        if require_host_witness:
            params['require_host_witness'] = True
        if declared_file:
            params.update(declared_task_id=declared_task, declared_source_id=declared_source)
        from .private_artifacts import read_capability, artifact_path, ProcessEvidence
        if capability_path:
            if batch_path or not params.get('reservation_id'):
                raise core.CoordError('private capability requires a single exact --reservation-id')
            capability = read_capability(capability_path)
            expected = dict(resource=params['resource'], session_id=session, host_id=host,
                            reservation_id=params['reservation_id'], server=server, project=client.project)
            if any(capability[key] != value for key, value in expected.items()):
                raise core.CoordError('private capability tuple does not match this request')
            if params.get('token') and params['token'] != capability['token']:
                raise core.CoordError('private capability conflicts with supplied token')
            params['token'] = capability['token']
        elif not batch_path and vendor_batch is None:
            raise core.CoordError('run requires --capability-file and exact --reservation-id')
        batch_tuples = vendor_batch
        if batch_path:
            if params['resource'] != 'batch' or params.get('reservation_id'):
                raise core.CoordError('batch run requires resource batch and manifest-bound reservations')
            import stat
            # Do not wait for a FIFO writer before rejecting its non-regular descriptor.
            fd = os.open(batch_path, os.O_RDONLY | os.O_NONBLOCK | getattr(os, 'O_NOFOLLOW', 0))
            try:
                info = os.fstat(fd)
                if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_nlink != 1 or info.st_mode & 0o077 or info.st_size > 65536:
                    raise core.CoordError('batch manifest must be a bounded private owner file')
                with os.fdopen(fd, 'rb', closefd=False) as source: manifest = source.read(65537)
            finally: os.close(fd)
            expression = '''if type == "array" and length >= 1 and length <= 64 and
                all(.[]; type == "object" and (keys | sort) == ["host_id","reservation_id","resource","session_id","token"] and
                    all(.[]; type == "string" and length > 0 and (test("[\\u0000-\\u001f\\u007f]") | not) and (contains("\\\\") | not)))
                then .[] | [.resource,.session_id,.host_id,.reservation_id,.token] | @tsv else error("invalid") end'''
            try:
                parsed = subprocess.run(['jq', '-er', expression], input=manifest, capture_output=True, check=False)
            except OSError:
                raise core.CoordError('batch manifest requires jq') from None
            if parsed.returncode:
                raise core.CoordError('invalid batch manifest')
            batch_tuples = [dict(zip(('resource', 'session_id', 'host_id', 'reservation_id', 'token'), line.split('\t')))
                            for line in parsed.stdout.decode('utf-8').splitlines()]
            if any(item['session_id'] != session or item['host_id'] != host for item in batch_tuples):
                raise core.CoordError('batch manifest requires the authenticated caller host session')
            if len({item['resource'] for item in batch_tuples}) != len(batch_tuples):
                raise core.CoordError('batch manifest requires distinct resources')
        lifecycle_enabled = params.pop('lifecycle_events')
        admission_level = params.pop('admission_level')
        if admission_level is not None and not lifecycle_enabled:
            raise core.CoordError('--admission-level requires --lifecycle-events')
        if lifecycle_enabled and (admission_level is None or admission_level < 0):
            raise core.CoordError('--lifecycle-events requires a zero-based --admission-level')
        lifecycle = _RunLifecycleEvents(lifecycle_enabled, admission_level, client.timeout)
        evidence = ProcessEvidence(evidence_path or artifact_path('process-evidence', str(uuid.uuid4())),
                                  runner_pid=os.getpid(), runner_pgid=os.getpgrp(),
                                  session_id=session, host_id=host, server=server, project=client.project,
                                  coverage='instrumented_process_group_only')
        def prelaunch_record(event, **facts):
            try:
                evidence.record(event, **facts)
            except BaseException:
                evidence.close()
                raise
        def run_call(phase, operation, values):
            record = prelaunch_record if phase == 'begin' else evidence.record
            record('guard_call_started', operation=operation,
                            run_id=values.get('run_id'), resource=values.get('resource'),
                            reservation_id=values.get('reservation_id'),
                            evidence_sha256=values.get('evidence_sha256'))
            inbox_trace_id = str(uuid.uuid4()) if lifecycle.enabled and phase == 'inbox' else None
            lifecycle.emit(phase, 'start', 'started', inbox_trace_id=inbox_trace_id)
            try:
                response = call(operation, values, inbox_trace_id=inbox_trace_id)
                if operation in ('attach-guard-pid', 'attach-guard-witness') and (
                        response.get('attached') is not True or response.get('run_id') != values['run_id']):
                    raise core.CoordError('guard attach response unresolved; preserve reservation')
                if operation == 'guard-file-observe' and (response.get('run_id') != values['run_id'] or
                        response.get('checked' if values['stage'] == 'check' else 'recorded') is not True):
                    raise core.CoordError('declared file observation response unresolved; preserve reservation')
                if operation == 'exact-file-complete' and (
                        response.get('run_id')!=values['run_id'] or response.get('generation')!=values['generation'] or
                        response.get('state')!='completed'):
                    raise core.CoordError('typed completion response unresolved; reconcile without replaying copy')
            except BaseException as error:
                lifecycle.emit(phase, 'end', _lifecycle_error_category(error), inbox_trace_id=inbox_trace_id)
                record('guard_call_failed', operation=operation,
                                run_id=values.get('run_id'), resource=values.get('resource'),
                                reservation_id=values.get('reservation_id'),
                                outcome=_lifecycle_error_category(error))
                if phase == 'begin': evidence.close()
                raise
            if phase == 'begin':
                lifecycle.run_id = response.get('run_id')
            lifecycle.emit(phase, 'end', 'succeeded', inbox_trace_id=inbox_trace_id)
            record('guard_call_succeeded', operation=operation,
                            run_id=values.get('run_id') or response.get('run_id'))
            return response
        restoration_deadline = None
        if params.get('restore'):
            budget = params.get('max_seconds')
            budget = 120 if budget is None else budget
            if (batch_tuples is None and not params.get('reservation_id')) or not 1 <= budget <= 900:
                raise core.CoordError('--restore requires --reservation-id and --max-seconds from 1 to 900')
            restoration_deadline = time.monotonic() + budget
        # Authorization is established before any child can execute.
        if batch_tuples is not None:
            values = {'tuples': batch_tuples, 'restore': params.get('restore', False)}
            if params.get('restore'): values['max_seconds'] = budget
            if vendor_request is not None:
                values={'tuples':batch_tuples,'resource':vendor_request['resource'],
                        'control_rid':vendor_control['reservation_id'],'control_token':vendor_control['token'],
                        'output_path':vendor_request['output']}
            batch = run_call('begin', 'hospital-vendor-begin' if vendor_request is not None else 'begin-guard-batch', values)
            admissions = batch.get('admissions')
            if not isinstance(admissions, list) or len(admissions) != len(batch_tuples):
                raise core.CoordError('batch admission response unresolved; preserve all reservations')
            if any(admission.get('resource') != item['resource'] or admission.get('reservation_id') != item['reservation_id']
                   or admission.get('host_id') != host or not isinstance(admission.get('run_id'), str)
                   for admission, item in zip(admissions, batch_tuples)):
                raise core.CoordError('batch admission response unresolved; preserve all reservations')
            if vendor_request is not None:
                command.extend(['--run-id',admissions[0]['run_id'],'--output-run-id',admissions[1]['run_id']])
            run = admissions[0]
            guards = [(admission, item['token']) for admission, item in zip(admissions, batch_tuples)]
        else:
            if exact_op=='exact-file-read':
                run = run_call('begin', 'exact-file-begin', {'path':params['resource'],'token':params['token'],'reservation_id':params['reservation_id'],**(exact_binding_scope or {})})
            else:
                run = run_call('begin', 'begin-guard', params)
            if prospective_recovery and run.get('prospective_recovery') is not True:
                raise core.CoordError('prospective recovery admission is unresolved')
            if (run.get('reservation_id') != params['reservation_id'] or
                    run.get('host_id') != host or not isinstance(run.get('run_id'), str)):
                evidence.close()
                raise core.CoordError('guard admission response unresolved; preserve reservation')
            if exact_op=='exact-file-read':
                command.extend(['--run-id',run['run_id'],'--reservation-id',run['reservation_id']])
                if exact_binding_scope:
                    intent=run.get('execution')
                    from .exact_file_execution import helper_sha256
                    if (not isinstance(intent,dict) or not isinstance(intent.get('generation'),str)
                            or intent.get('helper_sha256')!=helper_sha256()):
                        raise core.CoordError('typed execution admission unresolved; preserve run and do not replay copy')
                    command.extend(['--execution-generation',intent['generation'],'--expected-helper-sha256',intent['helper_sha256']])
            guards = [(run, params.get('token'))]
        prelaunch_record('admitted', reservations=[dict(resource=item['resource'],
                        reservation_id=admission['reservation_id'], run_id=admission['run_id'])
                        for admission, item in zip(admissions, batch_tuples)] if batch_tuples is not None
                        else [dict(resource=params['resource'], reservation_id=run['reservation_id'],
                                   run_id=run['run_id'])])
        producer_capability = None
        producer_instances = {}
        producer_path = os.environ.get('AGENT_CHAT_PRODUCER_CAPABILITY_FILE')
        if producer_path:
            from .lifecycle_producer import credential
            producer_capability = credential(producer_path)
            for admission, _ in guards:
                instance = run_call('producer_establish', 'producer-establish', {
                    'run_id': admission['run_id'], 'capability': producer_capability})
                producer_instances[admission['run_id']] = instance['id']
        proc = None
        seen = set()
        interrupted = False
        interruption_reported = False
        old_handlers = {}
        gate_read = gate_write = None
        restoration_timer = None
        restoration_expired = threading.Event()
        lifecycle_lock = threading.RLock()
        group_closed = False
        reaped = False
        def cleanup_evidence(event, **facts):
            # Retention failure must not prevent owned group cleanup. The
            # failed journal still blocks every subsequent guard-close call.
            try: evidence.record(event, **facts)
            except core.CoordError:
                if not evidence.failed: raise
        def record_reap():
            nonlocal reaped
            if not reaped and proc.returncode is not None:
                reaped = True
                cleanup_evidence('leader_reaped', child_pid=proc.pid, child_pgid=proc.pid,
                                returncode=proc.returncode)
        def expire_restoration():
            restoration_expired.set()
            # Socket reads may block the main thread. Stop only this run's
            # process group independently, including an inert attach launcher.
            with lifecycle_lock:
                if not group_closed:
                    try: os.killpg(proc.pid, signal.SIGKILL)
                    except ProcessLookupError: pass
        def check_restoration_deadline():
            if restoration_deadline is not None and (restoration_expired.is_set() or time.monotonic() >= restoration_deadline):
                raise core.CoordError('restoration deadline has expired')
        def group_alive():
            nonlocal group_closed
            if group_closed: return False
            proc.poll()  # reap an exited group leader before probing its group
            record_reap()
            try: os.killpg(proc.pid, 0); return True
            except ProcessLookupError:
                group_closed = True
                cleanup_evidence('group_absent', child_pgid=proc.pid)
                return False
            except PermissionError: return True
        def _stop_group():
            if proc is None: return
            if group_alive():
                try: os.killpg(proc.pid, signal.SIGTERM)
                except ProcessLookupError: pass
            deadline = time.monotonic() + 3
            while group_alive() and time.monotonic() < deadline:
                proc.poll(); time.sleep(.05)
            if group_alive():
                try: os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError: pass
            deadline = time.monotonic() + 3
            while group_alive() and time.monotonic() < deadline:
                proc.poll(); time.sleep(.05)
            if group_alive(): raise core.CoordError('remote guarded process group did not close')
        def stop_group():
            nonlocal group_closed
            with lifecycle_lock:
                if group_closed: return
                _stop_group()
                group_closed = True
        def poll_group():
            # Reaping permits PID/PGID reuse. Finish descendant closure and
            # disable the watchdog atomically with respect to its signaler.
            with lifecycle_lock:
                result = proc.poll()
                if result is not None: stop_group()
                return result
        def report_received_interruption():
            nonlocal interruption_reported
            if interrupted and not interruption_reported:
                interruption_reported = True
                lifecycle.emit('interruption', 'start', 'received_interruption')
        def forward(sig, frame=None):
            nonlocal interrupted
            interrupted = True
            stop_group()
        try:
            for sig in (signal.SIGINT, signal.SIGTERM): old_handlers[sig] = signal.signal(sig, forward)
            env = dict(os.environ, AGENT_CHAT_SERVER=server,
                       AGENT_CHAT_PROJECT=client.project,
                       AGENT_CHAT_SESSION=session,
                       AGENT_CHAT_TOKEN=params.get('token') or '',
                       AGENT_CHAT_API_TOKEN=client.token or '',
                       AGENT_CHAT_HOST_ID=host,
                       AGENT_CHAT_ROOT=os.environ.get('AGENT_CHAT_ROOT') or os.getcwd())
            env.pop('AGENT_CHAT_DB', None)
            env.pop('AGENT_CHAT_PRODUCER_CAPABILITY_FILE', None)
            if exact_op == 'exact-file-read' or vendor_request is not None:
                for name in ('PYTHONPATH', 'PYTHONHOME', 'PYTHONSTARTUP', 'PYTHONUSERBASE'):
                    env.pop(name, None)
            if vendor_request is not None:
                env['HOSPITAL_VENDOR_CONTROL_TOKEN']=vendor_control['token']
                env['HOSPITAL_VENDOR_RUN_TOKEN']=vendor_batch[0]['token']
                env['HOSPITAL_VENDOR_OUTPUT_TOKEN']=vendor_batch[1]['token']
                env['HOSPITAL_VENDOR_OUTPUT_RESOURCE']=vendor_output['resource']
            if batch_tuples is not None:
                env.pop('AGENT_CHAT_TOKEN', None)
                # Tuple credentials remain private to this launcher client.
                env.pop('AGENT_CHAT_BATCH_MANIFEST', None)
            # The requested command cannot execute until its process group is
            # recorded remotely. EOF closes the inert launcher if this client dies.
            gate_read, gate_write = os.pipe()
            launcher = 'import os,sys; fd=int(sys.argv[1]); allowed=os.read(fd,1); os.close(fd); os.execvpe(sys.argv[2],sys.argv[2:],os.environ) if allowed==b"G" else sys.exit(125)'
            check_restoration_deadline()
            lifecycle.emit('launcher', 'start', 'started')
            try:
                launcher_python = ([sys.executable, '-I', '-S'] if require_host_witness else
                                   [sys.executable, '-I'] if exact_op == 'exact-file-read' or vendor_request is not None else
                                   [sys.executable])
                proc = subprocess.Popen([*launcher_python, '-c', launcher, str(gate_read), *command],
                                        start_new_session=True, env=env, pass_fds=(gate_read,))
            except BaseException as error:
                lifecycle.emit('launcher', 'end', _lifecycle_error_category(error))
                raise
            lifecycle.launcher_pid = proc.pid
            evidence.record('launcher_created', child_pid=proc.pid, child_pgid=proc.pid,
                            gate_released=False)
            lifecycle.emit('launcher', 'end', 'launcher_created')
            report_received_interruption()
            if restoration_deadline is not None:
                restoration_timer = threading.Timer(max(0, restoration_deadline-time.monotonic()), expire_restoration)
                restoration_timer.daemon = True
                restoration_timer.start()
            os.close(gate_read); gate_read = None
            witness = None
            baseline = None
            if require_host_witness:
                from .guard_witness import observe_launcher
                witness = observe_launcher(proc.pid, os.getpid(), sys.executable)
                evidence.record('inert_launcher_witness', provenance='host_reported', **witness)
            if declared_file:
                from .guard_witness import observe_file
                file_path = run.get('declared_file_path')
                if not isinstance(file_path, str): raise core.CoordError('declared file path unavailable')
                baseline = observe_file(file_path)
                evidence.record('declared_file_baseline', provenance='host_reported', path=file_path, **baseline)
            for admission, token in guards:
                if witness is None:
                    run_call('attach', 'attach-guard-pid', {'run_id': admission['run_id'], 'token': token, 'pid': proc.pid})
                else:
                    values = {'run_id': admission['run_id'], 'token': token, **witness}
                    if baseline is not None: values['file_baseline'] = baseline
                    run_call('attach', 'attach-guard-witness', values)
                check_restoration_deadline()
            if witness is not None:
                after_witness = observe_launcher(proc.pid, os.getpid(), sys.executable)
                if after_witness != witness:
                    raise core.CoordError('inert launcher witness changed before gate release')
                evidence.record('inert_launcher_revalidated', provenance='host_reported', **after_witness)
            if baseline is not None:
                checked = observe_file(file_path)
                run_call('pulse', 'guard-file-observe', {'run_id':run['run_id'], 'token':params['token'],
                         'stage':'check', 'observation':checked})
            if prospective_recovery:
                evidence.record('prospective_recovery_intent',run_id=run['run_id'],child_pid=proc.pid,child_pgid=proc.pid)
            report_received_interruption()
            check_restoration_deadline()
            os.write(gate_write, b'G')
            evidence.record('gate_released', child_pid=proc.pid, child_pgid=proc.pid)
            os.close(gate_write); gate_write = None
            lifecycle.emit('child_exit', 'start', 'started')
            report_received_interruption()
            changed_seen = False
            file_unknown = False
            while poll_group() is None:
                time.sleep(0.5)
                if interrupted: raise core.CoordError('command interrupted; process group closed')
                check_restoration_deadline()
                # A short command may finish during the poll wait. Close its
                # process group before doing another inbox/pulse network round.
                if poll_group() is not None:
                    break
                if baseline is not None and not changed_seen and not file_unknown:
                    try:
                        current_file = observe_file(file_path)
                    except core.CoordError:
                        file_unknown = True
                    else:
                        if current_file != baseline:
                            evidence.record('declared_file_changed', provenance='host_reported', path=file_path, **current_file)
                            run_call('pulse', 'guard-file-observe', {'run_id':run['run_id'], 'token':params['token'],
                                     'stage':'changed', 'observation':current_file})
                            changed_seen = True
                messages = run_call('inbox', 'inbox', {})['messages']
                fresh = [m for m in messages if m['id'] not in seen]
                seen.update(m['id'] for m in messages)
                if fresh: print(json.dumps({'messages': fresh}), file=sys.stderr, flush=True)
                check_restoration_deadline()
                for admission, token in guards:
                    run_call('pulse', 'guard-pulse', {'run_id': admission['run_id'], 'token': token})
                report_received_interruption()
            check_restoration_deadline()
            lifecycle.emit('child_exit', 'end', 'child_exit')
            # The leader exiting is insufficient: descendants may retain the
            # reservation's process group and must be closed before attestation.
            stop_group()
            if baseline is not None:
                final_file = None
                if not file_unknown:
                    try: final_file = observe_file(file_path)
                    except core.CoordError: pass
                evidence.record('declared_file_final', provenance='host_reported', path=file_path,
                                observation=final_file, group_closed=group_closed, returncode=proc.returncode)
            if restoration_timer is not None:
                restoration_timer.cancel()
                restoration_timer.join()
            close_hash = hashlib.sha256((str(proc.pid) + ':' + str(proc.returncode)).encode()).hexdigest()
            for admission, token in guards:
                if producer_capability:
                    run_call('producer_close', 'producer-close', {
                        'capability': producer_capability, 'run_id': admission['run_id'],
                        'instance_id': producer_instances[admission['run_id']],
                        'pid': proc.pid, 'returncode': proc.returncode, 'group_closed': group_closed})
                run_call('close', 'close-guard', {'run_id': admission['run_id'], 'token': token, 'evidence_sha256': close_hash})
            if exact_binding_scope and proc.returncode==0 and group_closed:
                from .exact_file_read import observe_output
                observed=observe_output(exact_binding_scope['output'],exact_binding_scope['max_bytes'])
                run_call('execution_complete','exact-file-complete',{
                    'path':params['resource'],**exact_binding_scope,'run_id':run['run_id'],
                    'reservation_id':run['reservation_id'],'generation':run['execution']['generation'],
                    'token':params['token'],'pid':proc.pid,'returncode':proc.returncode,
                    'group_closed':group_closed,'close_evidence_sha256':close_hash,**observed})
            if baseline is not None:
                run_call('pulse', 'guard-file-observe', {'run_id':run['run_id'], 'token':params['token'],
                         'stage':'final', 'observation':final_file})
        except BaseException as error:
            # A remote disconnect means the process must not continue without a
            # live reservation check.  The server keeps the run open for receipt
            # backed recovery; it never attempts to signal this client PID.
            stop_group()
            cleanup_evidence('runner_failed', outcome=_lifecycle_error_category(error),
                            guards_may_remain_open=True)
            raise
        finally:
            if restoration_timer is not None:
                restoration_timer.cancel()
                restoration_timer.join()
            for fd in (gate_read, gate_write):
                if fd is not None: os.close(fd)
            for sig, handler in old_handlers.items(): signal.signal(sig, handler)
            if proc is not None:
                proc.wait()
                record_reap()
            if interrupted:
                report_received_interruption()
                lifecycle.emit('interruption', 'end', 'handled_interruption')
            evidence.close()
        result = {'exit_code': proc.returncode, 'run_id': None if run is None else run['run_id']}
        if batch_tuples is not None:
            result = {'exit_code': proc.returncode, 'batch_id': batch['batch_id'],
                      'admissions': admissions, 'processes_closed': True, 'restored': False, 'released': False}
        result['process_evidence_file'] = evidence.path
    elif op == 'exact-file-reconcile':
        from .exact_file_read import observe_output,MAX_BYTES
        from .private_artifacts import read_capability
        if type(params['max_bytes']) is not int or not 1<=params['max_bytes']<=MAX_BYTES:
            raise core.CoordError('exact file byte limit out of range')
        cap=read_capability(params.pop('capability_file'))
        expected=dict(resource=params['path'],session_id=session,host_id=host,
                      reservation_id=params['reservation_id'],server=server,project=client.project)
        if any(cap[k]!=value for k,value in expected.items()):
            raise core.CoordError('legacy readback capability tuple differs')
        receipt=_finalizer_read(params.pop('copy_receipt_file'),1024*1024)
        journal=_finalizer_read(params.pop('process_evidence_file'),1024*1024)
        log=_finalizer_read(params.pop('copy_log_file'),1024*1024)
        params['token']=cap['token']
        check={k:v for k,v in params.items() if k!='request_id'}
        def validate_readback():
            context=call('exact-file-verify-context',check)
            if any(context.get(k)!=check[k] for k in ('run_id','reservation_id','path','output','max_bytes')):
                raise core.CoordError('legacy verification context differs')
        source=observe_output(params['path'],params['max_bytes'],validate=validate_readback)
        output=observe_output(params['output'],params['max_bytes'],validate=validate_readback)
        if (source['bytes'],source['sha256'])!=(output['bytes'],output['sha256']):
            raise core.CoordError('legacy source and output currently differ; preserve history')
        validate_readback()
        params.update(output,device=source['output_device'],inode=source['output_inode'],mtime_ns=source['output_mtime_ns'],
                      receipt_base64=base64.b64encode(receipt).decode(),journal_base64=base64.b64encode(journal).decode(),
                      log_base64=base64.b64encode(log).decode())
        result=call('exact-file-reconcile',params)
    elif op == 'guard-status':
        state = call('status', {'mine': True, 'resources': [params['resource']]})
        row = next((item for item in state['resources'] if item['resource'] == params['resource']), None)
        if not row or row['owner_session'] != session:
            raise core.CoordError('guard-status requires the reservation owning session')
        if row['reservation_id'] != params['reservation_id']:
            raise core.CoordError('guard-status reservation does not match the current hold')
        context = call('guard-context', {'resource': params['resource']})
        if context['reservation_id'] != params['reservation_id']:
            raise core.CoordError('reservation changed while reading guard status')
        result = {'resource': params['resource'], 'reservation_id': context['reservation_id'],
                  'runs': context['runs']}
    elif op == 'attachment-download':
        if download_resume:
            from .attachment_transfer import download_file
            result = download_file(call, download_output, params, client.project, session, host)
        else:
            result = _publish_attachment_download(call(op, params), download_output,
                                                  params['source_project'], params['attachment_id'], session, host,
                                                  project=client.project, message_id=params.get('message_id'))
    elif op == 'send' and large_source:
        from .attachment_transfer import upload_file
        result = upload_file(call, large_source, transfer_id, params)
    elif op == 'deferred-activate':
        from .private_artifacts import atomic_capture, validate_destination
        capture_path=params.pop('capability_file')
        validate_destination(capture_path)
        result=call(op,params)
        if result.get('state') != 'active' or not all(isinstance(result.get(k),str) and result[k] for k in ('resource','reservation_id','token')):
            raise core.CoordError('deferred activation capability is unresolved')
        atomic_capture(capture_path,dict(resource=result['resource'],session_id=session,host_id=host,
                       reservation_id=result['reservation_id'],token=result['token'],server=server,project=client.project))
        result=dict(result,capability_file=capture_path)
        result.pop('token',None)
    elif op in ('request','exact-file-request'):
        from .private_artifacts import atomic_capture, validate_destination
        capture_path = params.pop('capability_file')
        validate_destination(capture_path)
        if exact_op=='exact-file-request': params['path']=params.pop('resource')
        result = call(op, params)
        if result.get('state') == 'owned':
            if not all(isinstance(result.get(key), str) and result[key]
                       for key in ('resource', 'reservation_id', 'token')):
                raise core.CoordError('owned reservation capability is unresolved')
            atomic_capture(capture_path, dict(resource=result['resource'], session_id=session,
                           host_id=host, reservation_id=result['reservation_id'], token=result['token'],
                           server=server, project=client.project))
            result = dict(result, capability_file=capture_path)
        result.pop('token', None)
    elif exact_op in ('exact-file-release','exact-file-recover'):
        params['path']=params.pop('resource')
        result=call(exact_op,params)
    else:
        result = call(op, params)
    result.pop('token', None)
    if op == 'context':
        print(json.dumps(result, sort_keys=True, ensure_ascii=False, separators=(',', ':')))
    else:
        print(json.dumps(result, sort_keys=True))
    return int(result.get('exit_code', 0))


if __name__ == '__main__':
    sys.exit(entrypoint())
