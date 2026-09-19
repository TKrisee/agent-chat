#!/usr/bin/env python3
"""Local live conversation viewer and operator messaging for agent-chat."""
from __future__ import annotations

import argparse
import contextlib
import fcntl
import http.server
import json
import os
from pathlib import Path
import secrets
import signal
import socket
import sqlite3
import sys
import threading
import time
from urllib.parse import parse_qs, urlsplit

from agent_chat.core import CoordError, Coordinator, default_db, agent_labels
from .projects import Projects

from .auth import authorized, validate_config, origin

HERE = Path(__file__).resolve().parent
STATIC = {
    '/': ('index.html', 'text/html; charset=utf-8'),
    '/markdown.js': ('markdown.js', 'application/javascript; charset=utf-8'),
    '/app.js': ('app.js', 'application/javascript; charset=utf-8'),
    '/style.css': ('style.css', 'text/css; charset=utf-8'),
}
MESSAGE_COLUMNS = 'seq,id,sender_session,recipient_session,body,created_at,acked_at'
CSP = "default-src 'self'; connect-src 'self'; style-src 'self'; script-src 'self'; img-src 'self' data:; base-uri 'none'; frame-ancestors 'none'; form-action 'self'"


@contextlib.contextmanager
def reader(db_path):
    path = Path(db_path).expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f'Coordinator database does not exist: {path}')
    conn = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=3)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute('BEGIN')
        row = conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone()
        if not row or row['value'] != '1':
            raise RuntimeError('Unsupported coordinator schema')
        yield conn
    finally:
        conn.close()


def attachment_table(db):
    return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='attachments'").fetchone())


def reply_table(db):
    return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='message_replies'").fetchone())


def batch_table(db):
    return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='message_batches'").fetchone())


def message_rows(rows, agents, db):
    items = [dict(row, sender_agent=agents.get(row['sender_session']),
                  recipient_agent=agents.get(row['recipient_session']), attachments=[],
                  reply_to=None, reply_preview=None, batch_id=None, deliveries=[]) for row in rows]
    if not items:
        return items
    by_message = {item['id']: item for item in items}
    marks = ','.join('?' for _ in by_message)
    if attachment_table(db):
        for row in db.execute(f'SELECT id,message_id,name,mime,size FROM attachments WHERE message_id IN ({marks}) ORDER BY rowid', list(by_message)):
            by_message[row['message_id']]['attachments'].append({
                'id': row['id'], 'name': row['name'], 'mime': row['mime'], 'size': row['size'],
                'url': '/api/attachments/' + row['id'],
            })
    if reply_table(db):
        query = f'''SELECT r.message_id,r.reply_to,p.seq,p.sender_session,p.recipient_session,p.body
                     FROM message_replies r JOIN messages p ON p.id=r.reply_to
                     WHERE r.message_id IN ({marks})'''
        for row in db.execute(query, list(by_message)):
            by_message[row['message_id']].update(reply_to=row['reply_to'], reply_preview={
                'id': row['reply_to'], 'seq': row['seq'], 'sender_session': row['sender_session'],
                'recipient_session': row['recipient_session'],
                'sender_agent': agents.get(row['sender_session']), 'body': row['body'][:240],
            })
    if batch_table(db):
        memberships = db.execute(
            f'SELECT message_id,batch_id FROM message_batches WHERE message_id IN ({marks})', list(by_message)).fetchall()
        batch_ids = sorted({row['batch_id'] for row in memberships})
        if batch_ids:
            batch_marks = ','.join('?' for _ in batch_ids)
            deliveries = {batch_id: [] for batch_id in batch_ids}
            for row in db.execute(
                f'''SELECT b.batch_id,m.id,m.recipient_session,m.acked_at
                     FROM message_batches b JOIN messages m ON m.id=b.message_id
                     WHERE b.batch_id IN ({batch_marks}) ORDER BY m.seq''', batch_ids):
                deliveries[row['batch_id']].append({
                    'id': row['id'], 'recipient_session': row['recipient_session'],
                    'recipient_agent': agents.get(row['recipient_session']), 'acked_at': row['acked_at'],
                })
            for row in memberships:
                by_message[row['message_id']].update(batch_id=row['batch_id'], deliveries=deliveries[row['batch_id']])
    return items


def snapshot(db_path, limit=500):
    """Read a consistent view without advancing inboxes or changing ownership."""
    with reader(db_path) as db:
        now = time.time()
        sessions = [dict(row) for row in db.execute(
            'SELECT id,agent,registered_at FROM sessions ORDER BY registered_at,id')]
        agents = agent_labels(db)
        total = db.execute('SELECT COUNT(*) FROM messages').fetchone()[0]
        rows = db.execute(f'SELECT {MESSAGE_COLUMNS} FROM messages ORDER BY seq DESC LIMIT ?',
                          (limit,)).fetchall()
        messages = message_rows(list(reversed(rows)), agents, db)
        resources = []
        columns = 'name,owner_session,reservation_id,deadline,stale,reason'
        for row in db.execute(f'SELECT {columns} FROM resources ORDER BY name'):
            state = 'free' if not row['owner_session'] else (
                'stale' if row['stale'] or row['deadline'] <= now else 'owned')
            queue = [dict(session=item['session'], agent=agents.get(item['session']), position=i + 1)
                     for i, item in enumerate(db.execute(
                         'SELECT session FROM resource_queue WHERE resource=? ORDER BY seq',
                         (row['name'],)))]
            resources.append(dict(resource=row['name'], owner_session=row['owner_session'],
                                  owner_agent=agents.get(row['owner_session']),
                                  reservation_id=row['reservation_id'], state=state,
                                  deadline=row['deadline'], reason=row['reason'], queue=queue))
        bridge = None
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='bridge_runtime'").fetchone():
            row = db.execute('SELECT * FROM bridge_runtime WHERE singleton=1').fetchone()
            if row:
                bridge = dict(row)
                bridge['recent'] = row['pid'] != 0 and now - row['heartbeat'] < 30
        return dict(bridge=bridge, server_time=now, sessions=sessions, messages=messages,
                    resources=resources, total_messages=total,
                    history_truncated=total > len(messages))


def page(db_path, before=None, limit=200):
    if not 1 <= limit <= 200 or (before is not None and before < 1):
        raise ValueError('History requires a positive cursor and a limit from 1 to 200')
    with reader(db_path) as db:
        agents = agent_labels(db)
        where = 'WHERE seq<?' if before is not None else ''
        args = (before, limit + 1) if before is not None else (limit + 1,)
        rows = db.execute(f'SELECT {MESSAGE_COLUMNS} FROM messages {where} ORDER BY seq DESC LIMIT ?', args).fetchall()
        return dict(messages=message_rows(list(reversed(rows[:limit])), agents, db), has_more=len(rows) > limit)


def operator_session(db_path):
    """Keep one web operator identity per database, independent of agent env vars."""
    session_file = Path(str(db_path) + '.web-session.json')
    fd = os.open(session_file, os.O_RDWR | os.O_CREAT, 0o600)
    with os.fdopen(fd, 'r+') as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        raw = stream.read(4096)
        stored = json.loads(raw) if raw else None
        if stored is not None and (not isinstance(stored, dict) or not isinstance(stored.get('id'), str) or not stored['id']):
            raise ValueError('Invalid saved web operator identity')
        sid = stored['id'] if stored else 'web_operator_' + secrets.token_urlsafe(24)
        coord = Coordinator(db_path, sid)
        try:
            info = coord.register('operator')
        finally:
            coord.close()
        if not stored:
            stream.seek(0)
            json.dump({'id': info['session']}, stream)
            stream.truncate()
            stream.flush()
            os.fsync(stream.fileno())
        return info['session']


class WebServer(http.server.ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, handler, db_path, web_root, api_token=None, public_url=None):
        self.db_path = str(Path(db_path).expanduser().resolve())
        self.web_root = Path(web_root).resolve()
        self.stop_event = threading.Event()
        self.csrf_token = secrets.token_urlsafe(32)
        self.api_token = api_token
        self.listen_host = address[0]
        self.address_family = socket.AF_INET6 if ':' in address[0] else socket.AF_INET
        self.public_url = validate_config(address[0], api_token, public_url)
        self.bridge_manager = None
        self.projects = Projects(self.db_path)
        self.project_contexts = {}
        self.project_lock = threading.RLock()
        super().__init__(address, handler)
        try:
            self.sender_session = operator_session(self.db_path)
            if api_token:
                from .bridge_lease import BridgeManager
                self.bridge_manager = BridgeManager(self.db_path)
        except BaseException:
            self.server_close()
            raise

    @property
    def origin(self):
        host = '[' + self.listen_host + ']' if ':' in self.listen_host else self.listen_host
        return self.public_url or origin(f'http://{host}:{self.server_port}')

    def project_context(self, project):
        item = self.projects.get(project)
        if item['id'] == 'default':
            return self.db_path, self.sender_session, self.bridge_manager
        with self.project_lock:
            if item['id'] not in self.project_contexts:
                path = str(self.projects.db_path(item['id']))
                if not Path(path).is_file(): raise CoordError('project database is missing; restore it before continuing')
                sender = operator_session(path)
                manager = None
                if self.api_token:
                    from .bridge_lease import BridgeManager
                    manager = BridgeManager(path)
                self.project_contexts[item['id']] = (path, sender, manager)
            return self.project_contexts[item['id']]

    def server_close(self):
        for _, _, manager in self.project_contexts.values():
            if manager: manager.close()
        if self.bridge_manager:
            self.bridge_manager.close()
        super().server_close()

    def shutdown(self):
        self.stop_event.set()
        super().shutdown()


class Handler(http.server.BaseHTTPRequestHandler):
    server: WebServer
    protocol_version = 'HTTP/1.1'

    def setup(self):
        super().setup()
        self.connection.settimeout(10)

    def log_message(self, *args):
        pass

    def trusted(self):
        expected = self.server.origin
        hosts = self.headers.get_all('Host', [])
        try:
            return (len(hosts) == 1 and origin(urlsplit(expected).scheme + '://' + hosts[0]) == expected and
                    (self.headers.get('Origin') is None or origin(self.headers['Origin']) == expected))
        except ValueError:
            return False

    def authenticate(self, bearer_only=False):
        if authorized(self.headers.get('Authorization', ''), self.server.api_token, bearer_only):
            return True
        raw = b'{"error":"Authentication required"}'
        self.send_response(401)
        self.send_header('WWW-Authenticate', 'Bearer realm="Agent chat"' if bearer_only else 'Basic realm="Agent chat", charset="UTF-8"')
        self.send_header('Content-Type', 'application/json')
        self.send_header('Cache-Control', 'no-store')
        self.send_header('Content-Length', str(len(raw)))
        self.send_header('Connection', 'close')
        self.end_headers()
        self.close_connection = True
        self.wfile.write(raw)
        return False

    def read_json(self, limit=65536):
        if self.headers.get('Transfer-Encoding') or len(self.headers.get_all('Content-Length', [])) != 1:
            raise ValueError('one Content-Length header is required; chunked requests are unsupported')
        if self.headers.get('Content-Type', '').split(';', 1)[0].strip().lower() != 'application/json':
            raise ValueError('Expected JSON content')
        size = int(self.headers['Content-Length'])
        if not 1 <= size <= limit:
            raise ValueError('request exceeds the allowed size')
        raw = self.rfile.read(size)
        if len(raw) != size:
            raise ValueError('incomplete request')
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise ValueError('Expected a JSON object')
        return value

    def respond(self, code, raw, mime):
        self.send_response(code)
        self.send_header('Content-Type', mime)
        self.send_header('Content-Length', str(len(raw)))
        self.send_header('Cache-Control', 'no-store')
        self.send_header('X-Content-Type-Options', 'nosniff')
        self.send_header('Content-Security-Policy', CSP)
        self.send_header('Connection', 'close')
        self.end_headers()
        self.close_connection = True
        try:
            self.wfile.write(raw)
        except (BrokenPipeError, ConnectionResetError, TimeoutError):
            pass

    def select_project(self):
        query = parse_qs(urlsplit(self.path).query, keep_blank_values=True)
        values = query.get('project', [])
        headers = self.headers.get_all('X-Agent-Chat-Project', [])
        if len(values) > 1 or len(headers) > 1 or (values and headers and values[0] != headers[0]):
            raise CoordError('choose exactly one project')
        project = (values or headers or ['default'])[0]
        self.project = self.server.projects.get(project)
        self.db_path, self.sender_session, self.bridge_manager = self.server.project_context(project)

    def scoped(self, data):
        if not hasattr(self, 'project') or self.project['id'] == 'default': return data
        if isinstance(data, list): return [self.scoped(item) for item in data]
        if isinstance(data, dict):
            return {key: (value + '?project=' + self.project['id'] if key == 'url' and isinstance(value, str) and value.startswith('/api/attachments/') else self.scoped(value)) for key, value in data.items()}
        return data

    def project_snapshot(self):
        return dict(snapshot(self.db_path), project=self.server.projects.get(self.project['id']), projects=self.server.projects.list())

    def json(self, code, data):
        self.respond(code, json.dumps(self.scoped(data), separators=(',', ':')).encode(), 'application/json; charset=utf-8')

    def do_GET(self):
        if not self.trusted():
            self.json(403, {'error': 'Untrusted host or origin'})
            return
        url = urlsplit(self.path)
        if not self.authenticate():
            return
        try:
            if url.path.startswith('/api/'):
                self.select_project()
            if url.path == '/api/projects':
                self.json(200, {'projects': self.server.projects.list(), 'project': self.project})
            elif url.path == '/api/snapshot':
                self.json(200, self.project_snapshot())
            elif url.path == '/api/config':
                self.json(200, {'sender': {'id': self.sender_session, 'agent': 'operator'},
                                'csrf_token': self.server.csrf_token, 'project': self.project,
                                'projects': self.server.projects.list()})
            elif url.path == '/api/messages':
                params = parse_qs(url.query)
                before = int(params['before'][0]) if 'before' in params else None
                limit = int(params.get('limit', ['200'])[0])
                self.json(200, page(self.db_path, before, limit))
            elif url.path.startswith('/api/messages/'):
                message_id = url.path.removeprefix('/api/messages/')
                if set(parse_qs(url.query)) - {'project'} or not message_id.startswith('message_') or '/' in message_id or not all(char.isalnum() or char in '_-' for char in message_id):
                    self.json(404, {'error': 'Not found'})
                    return
                with reader(self.db_path) as db:
                    agents = agent_labels(db)
                    row = db.execute(f'SELECT {MESSAGE_COLUMNS} FROM messages WHERE id=?', (message_id,)).fetchone()
                    item = message_rows([row], agents, db)[0] if row else None
                if item is None:
                    self.json(404, {'error': 'Not found'})
                else:
                    self.json(200, item)
            elif url.path.startswith('/api/attachments/'):
                attachment_id = url.path.removeprefix('/api/attachments/')
                if set(parse_qs(url.query)) - {'project'} or not attachment_id.startswith('attachment_') or '/' in attachment_id:
                    self.json(404, {'error': 'Not found'})
                    return
                with reader(self.db_path) as db:
                    if not attachment_table(db):
                        self.json(404, {'error': 'Not found'})
                        return
                    item = db.execute('SELECT mime,content FROM attachments WHERE id=?', (attachment_id,)).fetchone()
                if item is None:
                    self.json(404, {'error': 'Not found'})
                    return
                self.respond(200, item['content'], item['mime'])
            elif url.path == '/api/events':
                self.events()
            else:
                spec = STATIC.get(url.path)
                if spec is None:
                    self.json(404, {'error': 'Not found'})
                    return
                path = (self.server.web_root / spec[0]).resolve()
                if path.parent != self.server.web_root or not path.is_file():
                    self.json(404, {'error': 'Not found'})
                    return
                self.respond(200, path.read_bytes(), spec[1])
        except (ValueError, sqlite3.Error, RuntimeError, OSError) as error:
            self.json(400, {'error': str(error)})

    def do_POST(self):
        if not self.trusted():
            self.json(403, {'error': 'Untrusted host or origin'})
            return
        path = urlsplit(self.path).path
        machine_api = path in ('/api/coord', '/api/bridge/rpc', '/api/projects/rpc')
        if not self.authenticate(bearer_only=machine_api):
            return
        try:
            self.select_project()
        except (CoordError, ValueError, sqlite3.Error, OSError) as error:
            self.json(400, {'error': str(error)})
            return
        if machine_api:
            try:
                data = self.read_json(60 * 1024 * 1024 if path == '/api/coord' else 1024 * 1024)
                if path == '/api/projects/rpc':
                    self.json(200, self.project_action(data))
                    return
                if path == '/api/coord':
                    from .remote_service import dispatch
                    coord = Coordinator(self.db_path)
                    coord.session = None
                    try:
                        result = dispatch(coord, data)
                    finally:
                        coord.close()
                else:
                    result = {'result': self.bridge_manager.dispatch(data)}
                self.json(200, result)
            except (CoordError, ValueError, TypeError, KeyError, sqlite3.Error, OSError) as error:
                self.json(400, {'error': str(error)})
            return
        if path not in ('/api/messages', '/api/sessions/remove', '/api/projects', '/api/projects/rename'):
            self.json(404, {'error': 'Not found'})
            return
        csrf = self.headers.get('X-Agent-Chat-CSRF', '')
        if not csrf.isascii() or not secrets.compare_digest(csrf, self.server.csrf_token):
            self.json(403, {'error': 'Messaging connection expired. Reconnect and try again.'})
            return
        if path in ('/api/projects', '/api/projects/rename'):
            try:
                data = self.read_json()
                result = self.project_action(dict(data, op='create' if path == '/api/projects' else 'rename'))
                self.json(200, result)
            except (CoordError, ValueError, TypeError, sqlite3.Error, OSError) as error:
                self.json(400, {'error': str(error)})
            return
        if path == '/api/sessions/remove':
            try:
                data = self.read_json()
                if set(data) != {'id'} or not isinstance(data['id'], str) or not data['id']:
                    raise ValueError('Session id must be a nonempty string')
                coord = Coordinator(self.db_path, self.sender_session)
                try:
                    result = coord.remove_session(data['id'])
                finally:
                    coord.close()
                self.json(200, result)
            except (CoordError, ValueError, TypeError, sqlite3.Error, OSError) as error:
                self.json(400, {'error': str(error)})
            return
        if self.headers.get('Content-Type', '').split(';', 1)[0].strip().lower() != 'application/json':
            self.json(415, {'error': 'Expected a JSON message'})
            return
        try:
            size = int(self.headers.get('Content-Length', '0'))
            if not 1 <= size <= 65536:
                self.json(413, {'error': 'Message must fit within 64 KiB'})
                return
            data = json.loads(self.rfile.read(size))
            if not isinstance(data, dict) or set(data) not in ({'to', 'body'}, {'to', 'body', 'reply_to'}):
                raise ValueError('Choose a recipient and write a message')
            recipient, body = data['to'], data['body']
            is_many = isinstance(recipient, list)
            if (not isinstance(recipient, (str, list)) or not isinstance(body, str) or not body.strip() or
                    (is_many and (not recipient or any(not isinstance(item, str) or not item for item in recipient)))):
                raise ValueError('Choose a recipient and write a nonempty message')
            reply_to = data.get('reply_to')
            if reply_to is not None and (not isinstance(reply_to, str) or not reply_to):
                raise ValueError('Reply parent is invalid')
            with reader(self.db_path) as db:
                if isinstance(recipient, str):
                    if not db.execute('SELECT 1 FROM sessions WHERE id=?', (recipient,)).fetchone():
                        raise ValueError('Recipient is not a registered agent session')
                elif any(not db.execute('SELECT 1 FROM sessions WHERE id=?', (item,)).fetchone() for item in recipient):
                    raise ValueError('Recipient is not a registered agent session')
            coord = Coordinator(self.db_path, self.sender_session)
            try:
                sent = coord.send_many(recipient, body.strip(), reply_to=reply_to) if is_many else coord.send(recipient, body.strip(), reply_to=reply_to)
            finally:
                coord.close()
            self.json(200, {'messages': sent} if is_many else sent)
        except (CoordError, ValueError, sqlite3.Error, RuntimeError, OSError) as error:
            self.json(400, {'error': str(error)})

    def project_action(self, data):
        op = data.get('op')
        if op == 'list' and set(data) == {'op'}:
            return {'projects': self.server.projects.list()}
        if op == 'create' and set(data) == {'op', 'name'}:
            item = self.server.projects.create(data['name'])
            self.server.project_context(item['id'])
            return {'project': item, 'projects': self.server.projects.list()}
        if op == 'rename' and set(data) == {'op', 'id', 'name'}:
            return {'project': self.server.projects.rename(data['id'], data['name']), 'projects': self.server.projects.list()}
        raise CoordError('invalid project operation')

    def events(self):
        self.send_response(200)
        self.send_header('Content-Type', 'text/event-stream')
        self.send_header('Cache-Control', 'no-store')
        self.send_header('X-Content-Type-Options', 'nosniff')
        self.end_headers()
        prior = None
        try:
            while not self.server.stop_event.is_set():
                data = self.scoped(self.project_snapshot())
                comparable = dict(data)
                comparable.pop('server_time')
                if comparable != prior:
                    payload = 'event: snapshot\ndata: ' + json.dumps(data, separators=(',', ':')) + '\n\n'
                    prior = comparable
                else:
                    payload = ': heartbeat\n\n'
                self.wfile.write(payload.encode())
                self.wfile.flush()
                self.server.stop_event.wait(1)
        except (OSError, sqlite3.Error, RuntimeError):
            pass
        finally:
            self.close_connection = True


def create_server(db_path, port=8765, web_root=None, *, host='127.0.0.1', api_token=None, public_url=None):
    with reader(db_path):
        pass
    return WebServer((host, port), Handler, db_path, web_root or HERE / 'web', api_token, public_url)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', default=os.environ.get('AGENT_CHAT_DB') or os.environ.get('ITR_COORD_DB') or str(default_db()))
    parser.add_argument('--port', type=int, default=8765)
    parser.add_argument('--host', default='127.0.0.1',
                        help='Bind address (use 0.0.0.0 for remote access)')
    parser.add_argument('--api-token', default=os.environ.get('AGENT_CHAT_API_TOKEN'))
    parser.add_argument('--public-url', help='External HTTP(S) origin, e.g. https://chat.example.com')
    args = parser.parse_args(argv)
    if not 0 <= args.port <= 65535:
        parser.error('port must be between 0 and 65535')
    try:
        # The standalone web command owns first-run initialization; the lower
        # level server factory remains read-only for callers that require an
        # already-existing database.
        Coordinator(args.db).close()
        server = create_server(args.db, args.port, host=args.host, api_token=args.api_token, public_url=args.public_url)
    except (OSError, ValueError, sqlite3.Error, RuntimeError) as error:
        print(f'agent-chat-web: {error}', file=sys.stderr)
        return 2
    print(server.origin + '/', flush=True)

    def stop(signum, frame):
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    try:
        server.serve_forever(poll_interval=.2)
    finally:
        server.stop_event.set()
        server.server_close()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
