"""Command dispatcher; legacy coordination commands retain their exact semantics."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import sqlite3
import sys

from . import core

BRIDGE_COMMANDS = ('bind', 'unbind', 'bridge-status', 'bridge-retry', 'bridge-resolve')


def main(argv=None):
    raw = list(sys.argv[1:] if argv is None else argv)
    # Remote mode is intentionally selected before local dispatch.  A remote
    # command never constructs Coordinator, so a typo/outage cannot make a new
    # local SQLite database and split a team's coordination state.
    globals_parser = argparse.ArgumentParser(add_help=False)
    globals_parser.add_argument('--db')
    globals_parser.add_argument('--session')
    globals_parser.add_argument('--server')
    globals_parser.add_argument('--api-token')
    global_raw = raw[:raw.index('--')] if '--' in raw else raw
    global_args, remainder = globals_parser.parse_known_args(global_raw)
    if '--' in raw:
        remainder += raw[raw.index('--'):]
    server = global_args.server or os.environ.get('AGENT_CHAT_SERVER') or os.environ.get('ITR_COORD_SERVER')
    if server:
        if global_args.db:
            raise core.CoordError('--db and --server are mutually exclusive')
        return _remote_main(remainder, global_args, server)
    # Find the command after known global options, never words in a body/path.
    pos = 0
    while pos < len(raw) and (raw[pos] in ('--db', '--session') or raw[pos].startswith(('--db=', '--session='))):
        pos += 1 if '=' in raw[pos] else 2
    if pos >= len(raw) or raw[pos] not in BRIDGE_COMMANDS:
        if raw in (['--help'], ['-h']):
            print('Wake bridge commands: bind, unbind, bridge-status, bridge-retry, bridge-resolve\n')
        return core.main(raw)
    parser = argparse.ArgumentParser(prog='agent-chat')
    parser.add_argument('--db')
    parser.add_argument('--session')
    sub = parser.add_subparsers(dest='op', required=True)
    bind = sub.add_parser('bind', help='opt this coordination session into Codex wake-ups')
    group = bind.add_mutually_exclusive_group(required=True)
    group.add_argument('--thread')
    group.add_argument('--parent-session')
    bind.add_argument('--agent-path')
    sub.add_parser('unbind')
    sub.add_parser('bridge-status')
    retry = sub.add_parser('bridge-retry')
    retry.add_argument('job_id')
    retry.add_argument('--confirm-not-started', action='store_true')
    resolve = sub.add_parser('bridge-resolve')
    resolve.add_argument('job_id')
    resolve.add_argument('--confirm-delivered', action='store_true')
    args = parser.parse_args(raw)
    from .bridge_state import BridgeState
    coord = core.Coordinator(args.db, args.session)
    try:
        state = BridgeState(coord)
        if args.op == 'bind':
            result = state.bind(args.thread, args.parent_session, args.agent_path)
        elif args.op == 'unbind':
            result = state.unbind()
        elif args.op == 'bridge-retry':
            from .bridge import exclusive_bridge
            with exclusive_bridge(coord.path):
                result = state.retry(args.job_id, args.confirm_not_started)
        elif args.op == 'bridge-resolve':
            from .bridge import exclusive_bridge
            with exclusive_bridge(coord.path):
                result = state.resolve_delivered(args.job_id, args.confirm_delivered)
        else:
            result = state.status()
        print(json.dumps(result, sort_keys=True))
        return 0
    finally:
        coord.close()


def entrypoint():
    try:
        return main()
    except (core.CoordError, sqlite3.Error, OSError, ValueError) as error:
        print(json.dumps({'error': str(error)}), file=sys.stderr)
        return 2


def _remote_main(raw, global_args, server):
    """Client CLI for operations whose state lives exclusively on the server."""
    from .remote import HttpClient, client_host_id
    p = argparse.ArgumentParser(prog='agent-chat --server ' + server)
    sub = p.add_subparsers(dest='op', required=True)
    x = sub.add_parser('register'); x.add_argument('--agent', required=True)
    x = sub.add_parser('inbox'); x.add_argument('--all', action='store_true'); x.add_argument('--agent')
    x = sub.add_parser('acknowledge'); x.add_argument('id')
    x = sub.add_parser('send'); x.add_argument('--to', required=True); x.add_argument('--body-file', required=True); x.add_argument('--attach', action='append', default=[]); x.add_argument('--reply-to')
    x = sub.add_parser('request'); x.add_argument('resource'); x.add_argument('--minutes', required=True, type=float)
    sub.add_parser('status')
    x = sub.add_parser('cancel'); x.add_argument('resource')
    x = sub.add_parser('check'); x.add_argument('resource'); x.add_argument('--token')
    x = sub.add_parser('release'); x.add_argument('resource'); x.add_argument('--receipt', required=True); x.add_argument('--token')
    x = sub.add_parser('recover'); x.add_argument('resource'); x.add_argument('--receipt', required=True)
    x = sub.add_parser('block'); x.add_argument('resource'); x.add_argument('--reason', required=True)
    x = sub.add_parser('link-reply'); x.add_argument('id'); x.add_argument('--reply-to', required=True)
    x = sub.add_parser('remove-session'); x.add_argument('id')
    x = sub.add_parser('bind'); x.add_argument('--thread'); x.add_argument('--parent-session'); x.add_argument('--agent-path')
    sub.add_parser('unbind'); sub.add_parser('bridge-status')
    x = sub.add_parser('bridge-retry'); x.add_argument('job_id'); x.add_argument('--confirm-not-started', action='store_true')
    x = sub.add_parser('bridge-resolve'); x.add_argument('job_id'); x.add_argument('--confirm-delivered', action='store_true')
    x = sub.add_parser('run'); x.add_argument('resource'); x.add_argument('--token'); x.add_argument('command', nargs=argparse.REMAINDER)
    child_args = raw[raw.index('--') + 1:] if raw and raw[0] == 'run' and '--' in raw else None
    a = p.parse_args(raw[:raw.index('--')] if child_args is not None else raw)
    if child_args is not None: a.command = child_args
    session = global_args.session or os.environ.get('AGENT_CHAT_SESSION') or os.environ.get('ITR_COORD_SESSION')
    if not session and a.op == 'register':
        import secrets
        session = 'session_' + secrets.token_urlsafe(24)
    if not session:
        raise core.CoordError('AGENT_CHAT_SESSION or --session is required in remote mode')
    params = vars(a).copy(); op = params.pop('op')
    if 'token' in params and not params['token']:
        params['token'] = os.environ.get('AGENT_CHAT_TOKEN') or os.environ.get('ITR_COORD_TOKEN')
    if op == 'inbox' and params.pop('agent') is not None:
        raise core.CoordError('--agent cannot read another agent inbox in remote mode')
    if op == 'send':
        params['body'] = Path(params.pop('body_file')).read_text(encoding='utf-8')
        attachments = []
        for raw_path in params.pop('attach'):
            data = Path(raw_path).read_bytes()
            attachments.append({'name': Path(raw_path).name, 'content_base64': base64.b64encode(data).decode('ascii')})
        params['attachments'] = attachments
    client, host = HttpClient(server, global_args.api_token), client_host_id()
    def call(operation, values):
        return client.call('/api/coord', {'op': operation, 'session': session, 'host_id': host, 'params': values})
    if 'resource' in params:
        from .remote_service import resource_name
        params['resource'] = resource_name(params['resource'])
    if op in ('release', 'recover'):
        receipt_path = Path(params.pop('receipt'))
        receipt = json.loads(receipt_path.read_text(encoding='utf-8'))
        needed = {'version', 'resource', 'reservation_id', 'restored', 'processes_closed', 'closed_at', 'evidence', 'pids'}
        if not isinstance(receipt, dict) or set(receipt) != needed or receipt['version'] != 1 or receipt['resource'] != params['resource'] or receipt['restored'] is not True or receipt['processes_closed'] is not True:
            raise core.CoordError('receipt does not bind this restored reservation')
        if not isinstance(receipt['pids'], list) or any(not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0 for pid in receipt['pids']):
            raise core.CoordError('receipt PIDs are invalid')
        for pid in receipt['pids']:
            try: os.kill(pid, 0)
            except ProcessLookupError: continue
            except PermissionError: raise core.CoordError('receipt lists a still-live PID')
            raise core.CoordError('receipt lists a still-live PID')
        state = call('status', {})
        row = next((item for item in state['resources'] if item['resource'] == params['resource']), None)
        if not row or row['reservation_id'] != receipt['reservation_id']:
            raise core.CoordError('receipt does not bind the server reservation')
        context = call('guard-context', {'resource': params['resource']})
        if context['reservation_id'] != receipt['reservation_id']:
            raise core.CoordError('reservation changed while checking proof')
        for run in context['runs']:
            if run['local_pid'] and (core.Coordinator._alive(run['local_pid']) or core.Coordinator._alive(run['local_pid'], True)):
                raise core.CoordError('guarded process group is still alive; close it before recovery')
        evidence_path = Path(receipt['evidence'])
        if not evidence_path.is_absolute(): evidence_path = receipt_path.parent / evidence_path
        params['receipt'] = receipt
        params['evidence_base64'] = base64.b64encode(evidence_path.read_bytes()).decode('ascii')
    if op == 'run':
        command = params.pop('command')
        if command and command[0] == '--': command = command[1:]
        if not command: raise core.CoordError('run requires a command after --')
        # Authorization is established before any child can execute.
        run = call('begin-guard', params)
        proc = None
        seen = set()
        interrupted = False
        old_handlers = {}
        gate_read = gate_write = None
        def group_alive():
            proc.poll()  # reap an exited group leader before probing its group
            try: os.killpg(proc.pid, 0); return True
            except ProcessLookupError: return False
            except PermissionError: return True
        def stop_group():
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
        def forward(sig, frame=None):
            nonlocal interrupted
            interrupted = True
            stop_group()
        try:
            for sig in (signal.SIGINT, signal.SIGTERM): old_handlers[sig] = signal.signal(sig, forward)
            env = dict(os.environ, AGENT_CHAT_SERVER=server, ITR_COORD_SERVER=server,
                       AGENT_CHAT_SESSION=session, ITR_COORD_SESSION=session,
                       AGENT_CHAT_TOKEN=params.get('token') or '', ITR_COORD_TOKEN=params.get('token') or '',
                       AGENT_CHAT_API_TOKEN=client.token or '', ITR_COORD_API_TOKEN=client.token or '',
                       AGENT_CHAT_HOST_ID=host, ITR_COORD_HOST_ID=host,
                       AGENT_CHAT_ROOT=os.environ.get('AGENT_CHAT_ROOT') or os.environ.get('ITR_COORD_ROOT') or os.getcwd(),
                       ITR_COORD_ROOT=os.environ.get('AGENT_CHAT_ROOT') or os.environ.get('ITR_COORD_ROOT') or os.getcwd())
            env.pop('AGENT_CHAT_DB', None); env.pop('ITR_COORD_DB', None)
            # The requested command cannot execute until its process group is
            # recorded remotely. EOF closes the inert launcher if this client dies.
            gate_read, gate_write = os.pipe()
            launcher = 'import os,sys; fd=int(sys.argv[1]); allowed=os.read(fd,1); os.close(fd); os.execvpe(sys.argv[2],sys.argv[2:],os.environ) if allowed==b"G" else sys.exit(125)'
            proc = subprocess.Popen([sys.executable, '-c', launcher, str(gate_read), *command],
                                    start_new_session=True, env=env, pass_fds=(gate_read,))
            os.close(gate_read); gate_read = None
            call('attach-guard-pid', {'run_id': run['run_id'], 'token': params.get('token'), 'pid': proc.pid})
            os.write(gate_write, b'G')
            os.close(gate_write); gate_write = None
            while proc.poll() is None:
                time.sleep(0.5)
                if interrupted: raise core.CoordError('command interrupted; process group closed')
                messages = call('inbox', {})['messages']
                fresh = [m for m in messages if m['id'] not in seen]
                seen.update(m['id'] for m in messages)
                if fresh: print(json.dumps({'messages': fresh}), file=sys.stderr, flush=True)
                call('guard-pulse', {'run_id': run['run_id'], 'token': params.get('token')})
            # The leader exiting is insufficient: descendants may retain the
            # reservation's process group and must be closed before attestation.
            stop_group()
            evidence = hashlib.sha256((str(proc.pid) + ':' + str(proc.returncode)).encode()).hexdigest()
            call('close-guard', {'run_id': run['run_id'], 'token': params.get('token'), 'evidence_sha256': evidence})
        except BaseException:
            # A remote disconnect means the process must not continue without a
            # live reservation check.  The server keeps the run open for receipt
            # backed recovery; it never attempts to signal this client PID.
            stop_group()
            raise
        finally:
            for fd in (gate_read, gate_write):
                if fd is not None: os.close(fd)
            for sig, handler in old_handlers.items(): signal.signal(sig, handler)
            if proc is not None: proc.wait()
        result = {'exit_code': proc.returncode, 'run_id': None if run is None else run['run_id']}
    else:
        result = call(op, params)
    print(json.dumps(result, sort_keys=True))
    return int(result.get('exit_code', 0))


if __name__ == '__main__':
    sys.exit(entrypoint())
