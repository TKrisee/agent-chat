"""Serve chat and its authenticated HTTP API."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import secrets
import signal
import sqlite3
import sys
import threading

from .core import CoordError, Coordinator
from .web import create_server


def api_token_file(db_path):
    """Create or read the owner-only API token stored beside ``db_path``."""
    database = Path(db_path).expanduser().resolve()
    path = Path(str(database) + '.api-token')
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        token = path.read_text(encoding='utf-8').strip()
        if not token:
            raise ValueError(f'API token file is empty: {path}')
        os.chmod(path, 0o600)
        return token, path
    try:
        token = secrets.token_urlsafe(32)
        with os.fdopen(fd, 'w', encoding='utf-8') as stream:
            stream.write(token + '\n')
    except BaseException:
        try:
            os.unlink(path)
        except OSError:
            pass
        raise
    return token, path


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, prog='agent-chat-server')
    parser.add_argument('--db', default=str(Path.cwd() / '.agent-chat' / 'state.sqlite3'))
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=8765)
    parser.add_argument('--api-token', default=os.environ.get('AGENT_CHAT_API_TOKEN'))
    parser.add_argument('--public-url', help='External HTTP(S) origin; required when binding beyond loopback')
    args = parser.parse_args(argv)
    if not 0 <= args.port <= 65535:
        parser.error('--port must be between 0 and 65535')
    args.db = str(Path(args.db).expanduser().resolve())

    server = None
    previous = {}

    def shutdown(*_):
        if server:
            threading.Thread(target=server.shutdown, daemon=True).start()

    try:
        if not args.api_token:
            args.api_token, token_path = api_token_file(args.db)
            print(f'API token file: {token_path}', file=sys.stderr, flush=True)
        # Bootstrap the database before WebServer creates its operator session.
        Coordinator(args.db).close()
        server = create_server(args.db, port=args.port, host=args.host,
                               api_token=args.api_token, public_url=args.public_url)
        for sig in (signal.SIGINT, signal.SIGTERM):
            previous[sig] = signal.signal(sig, shutdown)
        print(server.origin + '/', flush=True)
        server.serve_forever(poll_interval=.2)
    except (CoordError, OSError, ValueError, sqlite3.Error) as error:
        print(str(error), file=sys.stderr, flush=True)
        return 2
    finally:
        if server:
            server.stop_event.set()
            server.server_close()
        for sig, handler in previous.items():
            signal.signal(sig, handler)
    return 0


if __name__ == '__main__':
    sys.exit(main())
