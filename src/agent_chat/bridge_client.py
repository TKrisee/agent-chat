"""Authenticated HTTP bridge state with Codex execution kept on this machine."""
from __future__ import annotations

import argparse
import contextlib
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import secrets
import signal
import sys
import threading
import uuid

from .bridge import Bridge
from .core import CoordError
from .remote import HttpClient, host_id
from .rpc import RpcClient, RpcError, TransportError


class RemoteBridgeState:
    def __init__(self, client, server_url, identity):
        self.client, self.server_url, self.identity = client, server_url, identity

    def call(self, op, **params):
        return self.client.call('/api/bridge/rpc', dict(self.identity, op=op, params=params))['result']

    def pending(self): return self.call('pending')
    def jobs(self): return self.call('jobs')
    def job(self, job_id): return self.call('job', job_id=job_id)
    def resolve(self, session_id): return self.call('resolve', session_id=session_id)
    def still_unread(self, job_id): return self.call('still_unread', job_id=job_id)
    def connection_metadata(self): return {'server': self.server_url}
    def prepare(self, thread_id, messages, payload):
        return self.call('prepare', thread_id=thread_id, messages=messages, payload=payload)
    def update(self, job_id, status, queue_id=None, error=None):
        return self.call('update', job_id=job_id, status=status, queue_id=queue_id, error=error)
    def observe(self, thread_id, state, error=None):
        return self.call('observe', thread_id=thread_id, state=state, error=error)
    def heartbeat(self, server, pid, error=None):
        return self.call('heartbeat', server=server, pid=pid, error=error)


@contextlib.contextmanager
def client_identity(server_url, state_file=None):
    directory = Path(os.environ.get('AGENT_CHAT_STATE_DIR', '~/.local/state/agent-chat')).expanduser()
    path = Path(state_file).expanduser() if state_file else directory / ('bridge-' + hashlib.sha256(server_url.encode()).hexdigest()[:24] + '.json')
    path.parent.mkdir(parents=True, exist_ok=True)
    with Path(str(path) + '.lock').open('a+') as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise CoordError('a bridge client already uses this identity file')
        try:
            identity = {'owner': 'worker_' + uuid.uuid4().hex, 'secret': secrets.token_urlsafe(32), 'host_id': host_id()}
            try:
                fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            except FileExistsError:
                identity = json.loads(path.read_text())
            else:
                with os.fdopen(fd, 'w') as stream:
                    json.dump(identity, stream)
                    stream.flush()
                    os.fsync(stream.fileno())
            if (not isinstance(identity, dict) or set(identity) != {'owner', 'secret', 'host_id'} or
                    not all(isinstance(v, str) and v for v in identity.values()) or identity['host_id'] != host_id()):
                raise CoordError('bridge identity file is invalid or belongs to another host')
            yield identity
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, prog='agent-chat-bridge-client')
    parser.add_argument('--server', default=os.environ.get('AGENT_CHAT_SERVER'))
    parser.add_argument('--api-token', '--token', dest='api_token', default=os.environ.get('AGENT_CHAT_API_TOKEN'))
    parser.add_argument('--codex-server', default='ws://127.0.0.1:4500')
    parser.add_argument('--state-file', help='private persistent worker identity; do not share between machines')
    parser.add_argument('--interval', type=float, default=2)
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--recover', action='store_true', help='release a lost dispatcher identity after confirming the old client stopped')
    parser.add_argument('--confirm-stopped', action='store_true')
    args = parser.parse_args(argv)
    if not args.server:
        parser.error('--server or AGENT_CHAT_SERVER is required')
    if not math.isfinite(args.interval) or args.interval < .2:
        parser.error('--interval must be finite and at least 0.2 seconds')
    if args.recover != args.confirm_stopped:
        parser.error('--recover requires --confirm-stopped, and vice versa')
    previous, stop = {}, threading.Event()
    rpc = None
    try:
        client = HttpClient(args.server, args.api_token)
        with client_identity(args.server.rstrip('/'), args.state_file) as identity:
            state = RemoteBridgeState(client, args.server.rstrip('/'), identity)
            if args.recover:
                state.call('reset', confirm_stopped=True)
                print(json.dumps({'dispatcher': 'released'}))
                return 0
            rpc = RpcClient(args.codex_server)
            for sig in (signal.SIGINT, signal.SIGTERM):
                previous[sig] = signal.signal(sig, lambda *_: stop.set())
            bridge = Bridge(state, rpc)
            connected, acquired, last_error = False, False, None
            print(json.dumps({'bridge': 'started', 'server': args.server, 'codex': args.codex_server}), flush=True)
            try:
                while not stop.is_set():
                    try:
                        # Reacquiring the same durable identity is idempotent and
                        # verifies the server still recognizes this dispatcher.
                        state.call('acquire')
                        acquired = True
                        if not connected:
                            rpc.connect()
                            connected = True
                        bridge.tick()
                        state.heartbeat(args.codex_server, os.getpid())
                        last_error = None
                    except (CoordError, RpcError, TransportError, OSError, ValueError, KeyError, TypeError) as error:
                        rpc.close()
                        connected = False
                        if str(error) != last_error:
                            print(json.dumps({'bridge': 'waiting', 'error': str(error)}), file=sys.stderr, flush=True)
                        last_error = str(error)
                        if acquired:
                            try:
                                state.heartbeat(args.codex_server, os.getpid(), last_error)
                            except (CoordError, OSError, ValueError):
                                pass  # keep original outage visible; never crash while reporting it
                    if args.once:
                        return 2 if last_error else 0
                    stop.wait(args.interval)
            finally:
                rpc.close()
                if acquired:
                    try:
                        state.heartbeat(args.codex_server, 0, 'bridge stopped')
                        state.call('release')
                    except (CoordError, OSError, ValueError) as error:
                        print(json.dumps({'bridge': 'lease_retained', 'error': str(error),
                                          'action': 'Restart with the same identity to reconcile; no automatic takeover.'}), file=sys.stderr)
    except (CoordError, OSError, ValueError) as error:
        print(json.dumps({'error': str(error)}), file=sys.stderr)
        return 2
    finally:
        if rpc:
            rpc.close()
        for sig, handler in previous.items():
            signal.signal(sig, handler)
    return 0


if __name__ == '__main__':
    sys.exit(main())
