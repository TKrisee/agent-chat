"""HTTP client commands and the local Codex wake bridge."""
import argparse
import base64
import hashlib
import json
import os
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
        return _project_main(remainder[1:], args, server, runtime)
    if remainder[0] == 'root':
        return _root_main(remainder[1:], args, server, runtime)
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
        print(json.dumps({'error': str(error)}), file=sys.stderr)
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
    identity = dict(session=session, host_id=client_host_id())
    if raw and raw[0] == 'in':
        if len(raw) < 3:
            raise core.CoordError('use root in PROJECT_ID COMMAND ...')
        allowed = {'policy','context','message','inbox','acknowledge','send','agents','agent-status',
                   'agent-stop','agent-resume','agent-model','agent-model-resolve','session-reset','session-reset-status',
                   'session-reset-cancel','session-reset-resolve','session-reset-retry',
                   'remove-session','bridge-status','status','supervisor-configure','supervisor-status'}
        if raw[2] not in allowed:
            raise core.CoordError('root in accepts project-scoped messaging, readback and agent management only')
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
    fresh_status = sub.add_parser('fresh-status', help='inspect exact retained fresh root request')
    fresh_status.add_argument('--request-id', required=True)
    enroll = sub.add_parser('enroll', help='join every project with your one neutral main conversation')
    enroll.add_argument('--expected-thread', required=True); enroll.add_argument('--confirm', action='store_true')
    sub.add_parser('status', help='root directory and durable project onboarding state')
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
    if op == 'delegate-management':
        params.pop('unbound')
    if 'prompt_file' in params:
        params['prompt'] = Path(params.pop('prompt_file')).read_text(encoding='utf-8')
    result = _logged_call(runtime.logger('agent_chat.cli'), client,
                          '/api/root/rpc', dict(identity, op=op, params=params))
    print(json.dumps(result, sort_keys=True))
    return 0


def _remote_main(raw, global_args, server, runtime):
    """Client CLI for operations whose state lives exclusively on the server."""
    from .remote import HttpClient, client_host_id
    p = argparse.ArgumentParser(prog='agent-chat-client')
    sub = p.add_subparsers(dest='op', required=True)
    x = sub.add_parser('register'); x.add_argument('--agent', required=True)
    x = sub.add_parser('rename', help='change your name while preserving your session'); x.add_argument('--agent', required=True)
    x = sub.add_parser('inbox'); x.add_argument('--all', action='store_true'); x.add_argument('--agent')
    sub.add_parser('policy', help='read the current complete standing coordination policy')
    x = sub.add_parser('context', help='bounded unread messages and relevant resources')
    x.add_argument('--limit', type=int, default=20); x.add_argument('--max-bytes', type=int, default=12288)
    x.add_argument('--cursor', type=int); x.add_argument('--resource-cursor')
    x.add_argument('--message-id', action='append', dest='message_ids')
    x.add_argument('--resource', action='append', dest='resources')
    x = sub.add_parser('message', help='read one complete message from your own inbox'); x.add_argument('id')
    x = sub.add_parser('acknowledge'); x.add_argument('id')
    x = sub.add_parser('send'); x.add_argument('--to', action='append'); x.add_argument('--body-file', required=True); x.add_argument('--attach', action='append', default=[]); x.add_argument('--reply-to'); x.add_argument('--ack-reply', action='store_true')
    x = sub.add_parser('request'); x.add_argument('resource'); x.add_argument('--minutes', required=True, type=float)
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
    for command in ('agent-stop','agent-resume'):
        x = sub.add_parser(command); x.add_argument('--to'); x.add_argument('--expected-thread', required=True)
        x.add_argument('--request-id', required=True); x.add_argument('--confirm', action='store_true')
        if command == 'agent-stop': x.add_argument('--interrupt', action='store_true', help='interrupt only the observed current turn; guards and holds remain owned')
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
    x = sub.add_parser('run'); x.add_argument('resource'); x.add_argument('--token')
    x.add_argument('--batch-manifest', help='private exact same-caller tuples; resource must be batch')
    x.add_argument('--restore', action='store_true', help='bounded same-owner stale reservation restoration only')
    x.add_argument('--reservation-id'); x.add_argument('--max-seconds', type=int)
    x.add_argument('--lifecycle-events', action='store_true',
                   help='emit sanitized guarded-run lifecycle JSON Lines on stderr')
    x.add_argument('--admission-level', type=int,
                   help='zero-based caller admission level; requires --lifecycle-events')
    x.add_argument('command', nargs=argparse.REMAINDER)
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
    if op in ('session-reset', 'agent-create'):
        params['prompt'] = Path(params.pop('prompt_file')).read_text(encoding='utf-8')
    if op == 'send':
        targets = params['to']
        if params['ack_reply'] and not params['reply_to']:
            raise core.CoordError('--ack-reply requires --reply-to')
        if params['ack_reply'] and (not targets or len(targets) != 1):
            raise core.CoordError('--ack-reply is only supported with one --to recipient')
        if not targets and params['reply_to']:
            raise core.CoordError('--reply-to requires an explicit --to recipient')
        params['to'] = targets[0] if targets and len(targets) == 1 else targets
        params['body'] = Path(params.pop('body_file')).read_text(encoding='utf-8')
        attachments = []
        for raw_path in params.pop('attach'):
            data = Path(raw_path).read_bytes()
            attachments.append({'name': Path(raw_path).name, 'content_base64': base64.b64encode(data).decode('ascii')})
        params['attachments'] = attachments
    cli_logger = runtime.logger('agent_chat.cli')
    client = HttpClient(server, global_args.api_token, project=global_args.project,
                        logger=runtime.logger('agent_chat.remote'))
    host = client_host_id()
    def call(operation, values, *, inbox_trace_id=None):
        payload = {'op': operation, 'session': session, 'host_id': host, 'params': values}
        result = _logged_call(cli_logger, client, '/api/coord', payload,
                              inbox_trace_id=inbox_trace_id)
        return dict(result, project=client.project) if getattr(global_args, 'root_scope', False) else result
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
    if 'resource' in params:
        from .remote_service import resource_name
        params['resource'] = resource_name(params['resource'])
    if op in ('release', 'recover'):
        receipt_path = Path(params.pop('receipt'))
        receipt = json.loads(receipt_path.read_text(encoding='utf-8'))
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
        state = call('status', {})
        row = next((item for item in state['resources'] if item['resource'] == params['resource']), None)
        if not row or row['reservation_id'] != receipt['reservation_id']:
            raise core.CoordError('receipt does not bind the server reservation')
        context = call('guard-context', {'resource': params['resource']})
        if context['reservation_id'] != receipt['reservation_id']:
            raise core.CoordError('reservation changed while checking proof')
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
    if op == 'run':
        command = params.pop('command')
        if command and command[0] == '--': command = command[1:]
        if not command: raise core.CoordError('run requires a command after --')
        batch_path = params.pop('batch_manifest')
        batch_tuples = None
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
            expression = '''if type == "array" and length >= 1 and length <= 42 and
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
        def run_call(phase, operation, values):
            inbox_trace_id = str(uuid.uuid4()) if lifecycle.enabled and phase == 'inbox' else None
            lifecycle.emit(phase, 'start', 'started', inbox_trace_id=inbox_trace_id)
            try:
                response = call(operation, values, inbox_trace_id=inbox_trace_id)
            except BaseException as error:
                lifecycle.emit(phase, 'end', _lifecycle_error_category(error), inbox_trace_id=inbox_trace_id)
                raise
            if phase == 'begin':
                lifecycle.run_id = response.get('run_id')
            lifecycle.emit(phase, 'end', 'succeeded', inbox_trace_id=inbox_trace_id)
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
            batch = run_call('begin', 'begin-guard-batch', values)
            admissions = batch.get('admissions')
            if not isinstance(admissions, list) or len(admissions) != len(batch_tuples):
                raise core.CoordError('batch admission response unresolved; preserve all reservations')
            if any(admission.get('resource') != item['resource'] or admission.get('reservation_id') != item['reservation_id']
                   or admission.get('host_id') != host or not isinstance(admission.get('run_id'), str)
                   for admission, item in zip(admissions, batch_tuples)):
                raise core.CoordError('batch admission response unresolved; preserve all reservations')
            run = admissions[0]
            guards = [(admission, item['token']) for admission, item in zip(admissions, batch_tuples)]
        else:
            run = run_call('begin', 'begin-guard', params)
            guards = [(run, params.get('token'))]
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
            try: os.killpg(proc.pid, 0); return True
            except ProcessLookupError:
                group_closed = True
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
                proc = subprocess.Popen([sys.executable, '-c', launcher, str(gate_read), *command],
                                        start_new_session=True, env=env, pass_fds=(gate_read,))
            except BaseException as error:
                lifecycle.emit('launcher', 'end', _lifecycle_error_category(error))
                raise
            lifecycle.launcher_pid = proc.pid
            lifecycle.emit('launcher', 'end', 'launcher_created')
            report_received_interruption()
            if restoration_deadline is not None:
                restoration_timer = threading.Timer(max(0, restoration_deadline-time.monotonic()), expire_restoration)
                restoration_timer.daemon = True
                restoration_timer.start()
            os.close(gate_read); gate_read = None
            for admission, token in guards:
                run_call('attach', 'attach-guard-pid', {'run_id': admission['run_id'], 'token': token, 'pid': proc.pid})
                check_restoration_deadline()
            report_received_interruption()
            check_restoration_deadline()
            os.write(gate_write, b'G')
            os.close(gate_write); gate_write = None
            lifecycle.emit('child_exit', 'start', 'started')
            report_received_interruption()
            while poll_group() is None:
                time.sleep(0.5)
                if interrupted: raise core.CoordError('command interrupted; process group closed')
                check_restoration_deadline()
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
            if restoration_timer is not None:
                restoration_timer.cancel()
                restoration_timer.join()
            evidence = hashlib.sha256((str(proc.pid) + ':' + str(proc.returncode)).encode()).hexdigest()
            for admission, token in guards:
                if producer_capability:
                    run_call('producer_close', 'producer-close', {
                        'capability': producer_capability, 'run_id': admission['run_id'],
                        'instance_id': producer_instances[admission['run_id']],
                        'pid': proc.pid, 'returncode': proc.returncode, 'group_closed': group_closed})
                run_call('close', 'close-guard', {'run_id': admission['run_id'], 'token': token, 'evidence_sha256': evidence})
        except BaseException:
            # A remote disconnect means the process must not continue without a
            # live reservation check.  The server keeps the run open for receipt
            # backed recovery; it never attempts to signal this client PID.
            stop_group()
            raise
        finally:
            if restoration_timer is not None:
                restoration_timer.cancel()
                restoration_timer.join()
            for fd in (gate_read, gate_write):
                if fd is not None: os.close(fd)
            for sig, handler in old_handlers.items(): signal.signal(sig, handler)
            if proc is not None: proc.wait()
            if interrupted:
                report_received_interruption()
                lifecycle.emit('interruption', 'end', 'handled_interruption')
        result = {'exit_code': proc.returncode, 'run_id': None if run is None else run['run_id']}
        if batch_tuples is not None:
            result = {'exit_code': proc.returncode, 'batch_id': batch['batch_id'],
                      'admissions': admissions, 'processes_closed': True, 'restored': False, 'released': False}
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
    else:
        result = call(op, params)
    if op == 'context':
        print(json.dumps(result, sort_keys=True, ensure_ascii=False, separators=(',', ':')))
    else:
        print(json.dumps(result, sort_keys=True))
    return int(result.get('exit_code', 0))


if __name__ == '__main__':
    sys.exit(entrypoint())
