"""Command dispatcher; legacy coordination commands retain their exact semantics."""
import argparse
import json
import sqlite3
import sys

from . import core

BRIDGE_COMMANDS = ('bind', 'unbind', 'bridge-status', 'bridge-retry', 'bridge-resolve')


def main(argv=None):
    raw = list(sys.argv[1:] if argv is None else argv)
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


if __name__ == '__main__':
    sys.exit(entrypoint())
