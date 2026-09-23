"""HTTP client commands and the local Codex wake bridge."""
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

def main(argv=None):
    raw = list(sys.argv[1:] if argv is None else argv)
    globals_parser = argparse.ArgumentParser(add_help=False)
    globals_parser.add_argument('--session')
    globals_parser.add_argument('--server', default=os.environ.get('AGENT_CHAT_SERVER') or 'http://127.0.0.1:8765')
    globals_parser.add_argument('--api-token', default=os.environ.get('AGENT_CHAT_API_TOKEN'))
    globals_parser.add_argument('--project', default=os.environ.get('AGENT_CHAT_PROJECT'))
    global_raw = raw[:raw.index('--')] if '--' in raw else raw
    args, remainder = globals_parser.parse_known_args(global_raw)
    if '--' in raw:
        remainder += raw[raw.index('--'):]
    # Every public client command uses HTTP. No local database fallback.
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
        return _project_main(remainder[1:], args, args.server)
    if remainder in (['--help'], ['-h']):
        globals_parser.prog = 'agent-chat-client'
        globals_parser.print_help()
        print('\nRun without a command (or use bridge) to start the local Codex app-server and wake bridge.\n'
              'Use bridge --help for startup options; project --help for project commands.\n')
    return _remote_main(remainder, args, args.server)


def entrypoint():
    try:
        return main()
    except (core.CoordError, sqlite3.Error, OSError, ValueError) as error:
        print(json.dumps({'error': str(error)}), file=sys.stderr)
        return 2


def _project_main(raw, args, server):
    parser = argparse.ArgumentParser(prog='agent-chat-client project')
    sub = parser.add_subparsers(dest='op', required=True)
    sub.add_parser('list')
    create = sub.add_parser('create'); create.add_argument('--name', required=True)
    rename = sub.add_parser('rename'); rename.add_argument('id'); rename.add_argument('--name', required=True)
    values = vars(parser.parse_args(raw))
    from .remote import HttpClient
    # Registry actions work even when a previously selected project no longer exists.
    result = HttpClient(server, args.api_token, project='default').call('/api/projects/rpc', values)
    print(json.dumps(result, sort_keys=True))
    return 0


def _remote_main(raw, global_args, server):
    """Client CLI for operations whose state lives exclusively on the server."""
    from .remote import HttpClient, client_host_id
    p = argparse.ArgumentParser(prog='agent-chat-client')
    sub = p.add_subparsers(dest='op', required=True)
    x = sub.add_parser('register'); x.add_argument('--agent', required=True)
    x = sub.add_parser('inbox'); x.add_argument('--all', action='store_true'); x.add_argument('--agent')
    x = sub.add_parser('context', help='bounded unread messages and relevant resources')
    x.add_argument('--limit', type=int, default=20); x.add_argument('--max-bytes', type=int, default=12288)
    x.add_argument('--cursor', type=int); x.add_argument('--resource-cursor')
    x.add_argument('--message-id', action='append', dest='message_ids')
    x.add_argument('--resource', action='append', dest='resources')
    x = sub.add_parser('message', help='read one complete message from your own inbox'); x.add_argument('id')
    x = sub.add_parser('acknowledge'); x.add_argument('id')
    x = sub.add_parser('send'); x.add_argument('--to', required=True); x.add_argument('--body-file', required=True); x.add_argument('--attach', action='append', default=[]); x.add_argument('--reply-to'); x.add_argument('--ack-reply', action='store_true')
    x = sub.add_parser('request'); x.add_argument('resource'); x.add_argument('--minutes', required=True, type=float)
    x = sub.add_parser('status'); x.add_argument('--mine', action='store_true'); x.add_argument('--resource', action='append', dest='resources')
    x = sub.add_parser('cancel'); x.add_argument('resource')
    x = sub.add_parser('check'); x.add_argument('resource'); x.add_argument('--token')
    x = sub.add_parser('release'); x.add_argument('resource'); x.add_argument('--receipt', required=True); x.add_argument('--token')
    x = sub.add_parser('recover'); x.add_argument('resource'); x.add_argument('--receipt', required=True)
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
    x = sub.add_parser('bridge-retry'); x.add_argument('job_id'); x.add_argument('--confirm-not-started', action='store_true')
    x = sub.add_parser('bridge-resolve'); x.add_argument('job_id'); x.add_argument('--confirm-delivered', action='store_true')
    x = sub.add_parser('run'); x.add_argument('resource'); x.add_argument('--token'); x.add_argument('command', nargs=argparse.REMAINDER)
    child_args = raw[raw.index('--') + 1:] if raw and raw[0] == 'run' and '--' in raw else None
    a = p.parse_args(raw[:raw.index('--')] if child_args is not None else raw)
    if child_args is not None: a.command = child_args
    session = global_args.session or os.environ.get('AGENT_CHAT_SESSION')
    if not session and a.op == 'register':
        import secrets
        session = 'session_' + secrets.token_urlsafe(24)
    if not session:
        raise core.CoordError('AGENT_CHAT_SESSION or --session is required in remote mode')
    params = vars(a).copy(); op = params.pop('op')
    if op == 'bridge-status':
        params['mine'] = not params.pop('all')
    if 'token' in params and not params['token']:
        params['token'] = os.environ.get('AGENT_CHAT_TOKEN')
    if op == 'inbox' and params.pop('agent') is not None:
        raise core.CoordError('--agent cannot read another agent inbox in remote mode')
    if op == 'send':
        if params['ack_reply'] and not params['reply_to']:
            raise core.CoordError('--ack-reply requires --reply-to')
        params['body'] = Path(params.pop('body_file')).read_text(encoding='utf-8')
        attachments = []
        for raw_path in params.pop('attach'):
            data = Path(raw_path).read_bytes()
            attachments.append({'name': Path(raw_path).name, 'content_base64': base64.b64encode(data).decode('ascii')})
        params['attachments'] = attachments
    client, host = HttpClient(server, global_args.api_token, project=global_args.project), client_host_id()
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
            env = dict(os.environ, AGENT_CHAT_SERVER=server,
                       AGENT_CHAT_PROJECT=client.project,
                       AGENT_CHAT_SESSION=session,
                       AGENT_CHAT_TOKEN=params.get('token') or '',
                       AGENT_CHAT_API_TOKEN=client.token or '',
                       AGENT_CHAT_HOST_ID=host,
                       AGENT_CHAT_ROOT=os.environ.get('AGENT_CHAT_ROOT') or os.getcwd())
            env.pop('AGENT_CHAT_DB', None)
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
    if op == 'context':
        print(json.dumps(result, sort_keys=True, ensure_ascii=False, separators=(',', ':')))
    else:
        print(json.dumps(result, sort_keys=True))
    return int(result.get('exit_code', 0))


if __name__ == '__main__':
    sys.exit(entrypoint())
