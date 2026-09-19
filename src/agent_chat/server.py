"""Serve chat and its authenticated API, with an optional local Codex bridge."""
from __future__ import annotations

import argparse
import json
import math
import os
import signal
import sqlite3
import sys
import threading

from .bridge import Bridge, exclusive_bridge
from .bridge_state import BridgeState
from .core import CoordError, Coordinator, default_db
from .rpc import RpcClient, RpcError, TransportError
from .web import create_server


def run_bridge(db_path, endpoint, interval, stop):
    coord = Coordinator(db_path)
    rpc = None
    try:
        with exclusive_bridge(coord.path):
            state = BridgeState(coord)
            state.operator()
            class StoppableRpc(RpcClient):
                def request(self, method, params=None):
                    if stop.is_set():
                        raise TransportError('bridge is stopping')
                    return super().request(method, params)
            rpc = StoppableRpc(endpoint)
            bridge = Bridge(state, rpc)
            connected, last_error = False, None
            while not stop.is_set():
                try:
                    if not connected:
                        rpc.connect()
                        connected = True
                    bridge.tick()
                    state.heartbeat(endpoint, os.getpid())
                    last_error = None
                except (TransportError, RpcError, OSError, CoordError, ValueError, KeyError, TypeError) as error:
                    rpc.close()
                    connected = False
                    state.heartbeat(endpoint, os.getpid(), str(error))
                    if str(error) != last_error and not stop.is_set():
                        print(json.dumps({'bridge': 'waiting', 'error': str(error)}), file=sys.stderr, flush=True)
                    last_error = str(error)
                stop.wait(interval)
            state.heartbeat(endpoint, 0, 'bridge stopped')
    finally:
        if rpc:
            rpc.close()
        coord.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, prog='agent-chat-server')
    parser.add_argument('--db', default=os.environ.get('AGENT_CHAT_DB') or os.environ.get('ITR_COORD_DB') or str(default_db()))
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=8765)
    parser.add_argument('--api-token', default=os.environ.get('AGENT_CHAT_API_TOKEN'))
    parser.add_argument('--public-url', help='External HTTP(S) origin; required when binding beyond loopback')
    parser.add_argument('--no-bridge', action='store_true', help='host chat/API only; run bridge-client on the agents machine')
    parser.add_argument('--codex-server', default='ws://127.0.0.1:4500')
    parser.add_argument('--interval', type=float, default=2)
    args = parser.parse_args(argv)
    if not 0 <= args.port <= 65535:
        parser.error('--port must be between 0 and 65535')
    if not math.isfinite(args.interval) or args.interval < .2:
        parser.error('--interval must be finite and at least 0.2 seconds')
    if not args.no_bridge:
        try:
            RpcClient(args.codex_server)
        except ValueError as error:
            parser.error(str(error))
    server, worker = None, None
    stop, failed = threading.Event(), threading.Event()
    previous = {}
    def shutdown(*_):
        stop.set()
        if server:
            threading.Thread(target=server.shutdown, daemon=True).start()
    try:
        Coordinator(args.db).close()
        server = create_server(args.db, port=args.port, host=args.host, api_token=args.api_token, public_url=args.public_url)
        for sig in (signal.SIGINT, signal.SIGTERM):
            previous[sig] = signal.signal(sig, shutdown)
        print(server.origin + '/', flush=True)
        if not args.no_bridge:
            def run():
                try:
                    run_bridge(args.db, args.codex_server, args.interval, stop)
                except Exception as error:
                    failed.set()
                    print(json.dumps({'bridge': 'failed', 'error': str(error)}), file=sys.stderr, flush=True)
                    shutdown()
            worker = threading.Thread(target=run, name='agent-chat-bridge', daemon=True)
            worker.start()
        server.serve_forever(poll_interval=.2)
    except (CoordError, OSError, ValueError, sqlite3.Error) as error:
        print(json.dumps({'error': str(error)}), file=sys.stderr)
        failed.set()
    finally:
        stop.set()
        if worker:
            worker.join(25)
            if worker.is_alive():
                failed.set()
                print('bridge did not finish within shutdown timeout', file=sys.stderr)
        if server:
            server.stop_event.set()
            server.server_close()
        for sig, handler in previous.items():
            signal.signal(sig, handler)
    return 2 if failed.is_set() else 0


if __name__ == '__main__':
    sys.exit(main())
