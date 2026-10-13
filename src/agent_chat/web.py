#!/usr/bin/env python3
"""Local live conversation viewer and operator messaging for agent-chat."""
from __future__ import annotations

import contextlib
from email.parser import BytesParser
from email.policy import default as email_policy
import fcntl
import http.server
import json
import os
from pathlib import Path
import secrets
import socket
import socketserver
import sqlite3
import threading
import time
import tempfile
from urllib.parse import parse_qs, quote, urlsplit

from agent_chat.core import CoordError, Coordinator, agent_labels, MAX_ATTACHMENTS, MAX_ATTACHMENT_SIZE
from .remote import MAX_COORD_REQUEST_BYTES
from .projects import Projects
from .bridge_state import runtime_snapshot
from .usage import UsageStore
from .measurement import MeasurementStore
from .session_models import SessionModels
from .measurement_pause import request_pauses

from .auth import authorized, validate_config, origin, proxy_networks, RequestOrigins
from .app_logging import LoggingConfig, configure, new_correlation_id

HERE = Path(__file__).resolve().parent
STATIC = {
    '/': ('index.html', 'text/html; charset=utf-8'),
    '/root': ('index.html', 'text/html; charset=utf-8'),
    '/communications': ('index.html', 'text/html; charset=utf-8'),
    '/communication-view.js': ('communication-view.js', 'application/javascript; charset=utf-8'),
    '/communication-view.css': ('communication-view.css', 'text/css; charset=utf-8'),
    '/markdown.js': ('markdown.js', 'application/javascript; charset=utf-8'),
    '/app.js': ('app.js', 'application/javascript; charset=utf-8'),
    '/style.css': ('style.css', 'text/css; charset=utf-8'),
}
MESSAGE_COLUMNS = 'seq,id,sender_session,recipient_session,body,created_at,acked_at'
UI_MESSAGE_LIMIT = 50
MAX_MULTIPART_BYTES = MAX_ATTACHMENTS * MAX_ATTACHMENT_SIZE + 64 * 1024
CSP = "default-src 'self'; connect-src 'self'; style-src 'self'; script-src 'self'; img-src 'self' data: blob:; media-src 'self' blob:; base-uri 'none'; frame-ancestors 'none'; form-action 'self'"


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


def project_message_filter(db, operator, alias='m'):
    """Exclude private operator/root pairs; shared batches stay in project rooms.

    Quiet identities persist after routing removal so retained private history
    cannot reappear when a root detaches or is replaced.
    """
    if not operator or not db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='quiet_root_sessions'").fetchone():
        return '1', ()
    private = f'''(({alias}.recipient_session=? AND EXISTS
        (SELECT 1 FROM quiet_root_sessions q WHERE q.session_id={alias}.sender_session))
        OR ({alias}.sender_session=? AND EXISTS
        (SELECT 1 FROM quiet_root_sessions q WHERE q.session_id={alias}.recipient_session)))'''
    if batch_table(db):
        private = f'({private} AND NOT EXISTS (SELECT 1 FROM message_batches b WHERE b.message_id={alias}.id))'
    return f'NOT ({private})', (operator, operator)


def service_notice_ids(db, ids):
    if not ids or not db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_project_notices'").fetchone():
        return set()
    marks = ','.join('?' for _ in ids)
    return {row['message_id'] for row in db.execute(
        f'SELECT message_id FROM root_project_notices WHERE message_id IN ({marks})', list(ids))}


def message_rows(rows, agents, db, project_operator=None):
    operators = {row['session_id'] for row in db.execute('SELECT session_id FROM operator_identities')}
    items = [dict(row, sender_role='human_operator' if row['sender_session'] in operators else 'agent',
                  recipient_role='human_operator' if row['recipient_session'] in operators else 'agent', sender_agent=agents.get(row['sender_session']),
                  recipient_agent=agents.get(row['recipient_session']), attachments=[],
                  reply_to=None, reply_preview=None, batch_id=None, deliveries=[]) for row in rows]
    if not items:
        return items
    by_message = {item['id']: item for item in items}
    marks = ','.join('?' for _ in by_message)
    notices = service_notice_ids(db, by_message)
    origins = set()
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='operator_message_origins'").fetchone():
        origins = {row['message_id'] for row in db.execute(
            f'SELECT message_id FROM operator_message_origins WHERE message_id IN ({marks})',list(by_message))}
    for item in items:
        item['service_notice'] = item['id'] in notices
        item['instruction_origin'] = 'authenticated_operator_ui' if item['id'] in origins else 'agent_message'
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
        condition, args = project_message_filter(db, project_operator, 'p')
        parents = db.execute(query + f' AND ({condition})', (*by_message, *args)).fetchall()
        notices = service_notice_ids(db, {row['reply_to'] for row in parents})
        for row in parents:
            by_message[row['message_id']].update(reply_to=row['reply_to'], reply_preview={
                'id': row['reply_to'], 'seq': row['seq'], 'sender_session': row['sender_session'],
                'recipient_session': row['recipient_session'],
                'sender_agent': agents.get(row['sender_session']), 'body': row['body'][:240],
                'service_notice': row['reply_to'] in notices,
            })
    if batch_table(db):
        memberships = db.execute(
            f'SELECT message_id,batch_id FROM message_batches WHERE message_id IN ({marks})', list(by_message)).fetchall()
        batch_ids = sorted({row['batch_id'] for row in memberships})
        if batch_ids:
            batch_marks = ','.join('?' for _ in batch_ids)
            attention = ('EXISTS (SELECT 1 FROM message_attention a WHERE a.message_id=m.id)'
                         if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='message_attention'").fetchone()
                         else '0')
            deliveries = {batch_id: [] for batch_id in batch_ids}
            for row in db.execute(
                f'''SELECT b.batch_id,m.id,m.recipient_session,m.acked_at,{attention} AS wake_requested
                     FROM message_batches b JOIN messages m ON m.id=b.message_id
                     WHERE b.batch_id IN ({batch_marks}) ORDER BY m.seq''', batch_ids):
                deliveries[row['batch_id']].append({
                    'id': row['id'], 'recipient_session': row['recipient_session'],
                    'recipient_agent': agents.get(row['recipient_session']), 'acked_at': row['acked_at'],
                    'wake_requested': bool(row['wake_requested']),
                })
            for row in memberships:
                by_message[row['message_id']].update(batch_id=row['batch_id'], deliveries=deliveries[row['batch_id']])
    for item in items:
        candidates = item['deliveries'] or [item]
        incoming = next((copy for copy in candidates if copy['recipient_session'] == project_operator
                         and item['sender_session'] != project_operator), None)
        item['operator_ack'] = ({'id': incoming['id'], 'recipient_session': project_operator,
                                 'acked_at': incoming['acked_at']}
                                if incoming else None)
    return items


def snapshot(db_path, limit=UI_MESSAGE_LIMIT, usage_store=None, session_models=None,
             resource_limit=None, resource_offset=0, operator=None):
    """Read a consistent view without advancing inboxes or changing ownership."""
    with reader(db_path) as db:
        now = time.time()
        sessions = [dict(row) for row in db.execute(
            'SELECT id,agent,registered_at FROM sessions ORDER BY registered_at,id')]
        bindings = {}
        if db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='bridge_bindings'").fetchone():
            bindings = {row['session_id']: dict(row) for row in db.execute(
                'SELECT session_id,thread_id,parent_session,agent_path FROM bridge_bindings')}
        agents = agent_labels(db)
        condition, args = project_message_filter(db, operator)
        total = db.execute(f'SELECT COUNT(*) FROM messages m WHERE {condition}', args).fetchone()[0]
        rows = db.execute(f'SELECT {MESSAGE_COLUMNS} FROM messages m WHERE {condition} ORDER BY seq DESC LIMIT ?',
                          (*args, limit)).fetchall()
        messages = message_rows(list(reversed(rows)), agents, db, operator)
        resources = []
        columns = 'name,owner_session,reservation_id,deadline,stale,reason'
        resource_page = None
        if resource_limit is not None:
            if (type(resource_limit) is not int or not 1 <= resource_limit <= 200
                    or type(resource_offset) is not int or resource_offset < 0):
                raise ValueError('Resource page requires a limit from 1 to 200 and a nonnegative offset')
            counts = db.execute('''SELECT COUNT(*),COUNT(owner_session),
                COALESCE(SUM(owner_session IS NOT NULL AND (stale OR deadline<=?)),0)
                FROM resources''', (now,)).fetchone()
            total_resources, held_resources, stale_resources = counts
            resource_offset = min(resource_offset, max(0, (total_resources-1)//resource_limit)*resource_limit)
            resource_page = dict(offset=resource_offset, limit=resource_limit, total=total_resources,
                                 held=held_resources, stale=stale_resources,
                                 free=total_resources-held_resources,
                                 waiting=db.execute('SELECT COUNT(*) FROM resource_queue').fetchone()[0])
            resource_rows = db.execute(f'''SELECT {columns} FROM resources
                ORDER BY CASE WHEN owner_session IS NULL THEN 2 WHEN stale OR deadline<=? THEN 1 ELSE 0 END,
                CASE name WHEN 'validation-clone' THEN 0 WHEN 'main-inputs' THEN 1 WHEN 'git-index' THEN 2 ELSE 3 END,name
                LIMIT ? OFFSET ?''', (now, resource_limit, resource_offset))
        else:
            resource_rows = db.execute(f'SELECT {columns} FROM resources ORDER BY name')
        for row in resource_rows:
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
        bridge = runtime_snapshot(db, now)
        data = dict(bridge=bridge, server_time=now, sessions=sessions, messages=messages,
                    resources=resources, total_messages=total,
                    history_truncated=total > len(messages))
        if resource_page is not None:
            data['resource_page'] = resource_page
        data['session_resets'] = []
        orchestrators = set()
        if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='session_resets'").fetchone():
            # Role follows durable main-creation relationships, not names/models
            # or the limited recent lifecycle activity shown in the browser.
            creations = list(db.execute('''SELECT requester_session,target_session FROM session_resets
                WHERE kind='create' AND status='completed'
                AND requester_session IN (SELECT id FROM sessions)
                AND target_session IN (SELECT id FROM sessions)'''))
            created = {row['target_session'] for row in creations}
            orchestrators = {row['requester_session'] for row in creations} - created
            data['session_resets'] = [dict(row) for row in db.execute('''SELECT id,requester_session,target_session,
                old_thread_id,new_thread_id,status,error,created_at,updated_at,kind FROM session_resets
                WHERE status NOT IN ('completed','cancelled','failed') OR id IN
                    (SELECT id FROM session_resets ORDER BY created_at DESC LIMIT 20)
                ORDER BY created_at DESC''')]
        for session in sessions:
            route = bindings.get(session['id'], {})
            session.update(thread_id=route.get('thread_id'), parent_session=route.get('parent_session'))
            session['is_orchestrator'] = (session['id'] in orchestrators
                                          and bool(route.get('thread_id'))
                                          and not route.get('parent_session'))
            from .root_orchestrator import projection
            session['is_root_orchestrator'] = bool(projection(db, session['id']))
            if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='project_orchestrators'").fetchone():
                role = db.execute('SELECT root_session FROM project_orchestrators WHERE session_id=?', (session['id'],)).fetchone()
                if role and route.get('thread_id'):
                    session['is_orchestrator'] = True
                    if projection(db, role['root_session']):
                        session['orchestration_parent'] = role['root_session']
            if session['is_root_orchestrator']:
                session['is_orchestrator'] = True
    if usage_store is not None:
        data['usage'] = usage_store.status()
    if session_models is not None:
        metadata = session_models.read(
            (route['thread_id'] for route in bindings.values() if route['thread_id']),
            (route['agent_path'] for route in bindings.values() if route['agent_path']))
        for session in sessions:
            route = bindings.get(session['id'], {})
            observed = metadata.get(route.get('thread_id') or route.get('agent_path'), {})
            session.update(model=observed.get('model'), reasoning_effort=observed.get('reasoning_effort'))
    return data


def page(db_path, before=None, limit=UI_MESSAGE_LIMIT, after=None, operator=None,
         agent=None, to_me=False, project_feed=False):
    def valid_cursor(value):
        return value is None or (type(value) is int and 1 <= value <= 2**63 - 1)
    if ((before is not None and after is not None) or type(limit) is not int
            or not 1 <= limit <= 200 or not valid_cursor(before) or not valid_cursor(after)
            or (agent is not None and (not isinstance(agent, str) or not agent or len(agent) > 256))
            or (agent is not None or to_me) and not operator):
        raise ValueError('History requires a positive cursor and a limit from 1 to 200')
    with reader(db_path) as db:
        agents = agent_labels(db)
        conditions = []
        args = []
        if project_feed:
            condition, values = project_message_filter(db, operator)
            conditions.append(condition)
            args.extend(values)
        if agent is not None or to_me:
            if batch_table(db):
                conditions.append('NOT EXISTS (SELECT 1 FROM message_batches b WHERE b.message_id=m.id)')
            if agent is not None:
                inbound = '(m.sender_session=? AND m.recipient_session=?)'
                args.extend((agent, operator))
                if to_me:
                    conditions.append(inbound)
                else:
                    conditions.append(f'({inbound} OR (m.sender_session=? AND m.recipient_session=?))')
                    args.extend((operator, agent))
            else:
                conditions.append('m.recipient_session=?')
                args.append(operator)
        if after is not None:
            conditions.append('m.seq>?')
            args.append(after)
            direction = 'ASC'
        else:
            if before is not None:
                conditions.append('m.seq<?')
                args.append(before)
            direction = 'DESC'
        where = 'WHERE ' + ' AND '.join(conditions) if conditions else ''
        rows = db.execute(
            f'SELECT {MESSAGE_COLUMNS} FROM messages m {where} ORDER BY m.seq {direction} LIMIT ?',
            (*args, limit + 1)).fetchall()
        if after is not None:
            messages = rows[:limit]
        else:
            messages = list(reversed(rows[:limit]))
        return dict(messages=message_rows(messages, agents, db, operator if project_feed else None), has_more=len(rows) > limit)


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
            coord.db.execute('INSERT OR IGNORE INTO operator_identities VALUES(?)', (info['session'],))
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

    def server_bind(self):
        # HTTPServer resolves a display name through reverse DNS during bind.
        # Routing uses our explicit origin; readiness must not wait for DNS.
        socketserver.TCPServer.server_bind(self)
        self.server_name, self.server_port = self.server_address[:2]

    def __init__(self, address, handler, db_path, web_root, api_token=None, public_url=None, sessions_root=None,
                 trusted_proxies=(), inbox_diagnostics=None, logging_config=None):
        self.db_path = str(Path(db_path).expanduser().resolve())
        self.web_root = Path(web_root).resolve()
        self.stop_event = threading.Event()
        self.csrf_token = secrets.token_urlsafe(32)
        self.api_token = api_token
        self.inbox_diagnostics = None
        self.logging = configure(logging_config or LoggingConfig())
        self.log = self.logging.logger('agent_chat.web')
        self.server_log = self.logging.logger('agent_chat.server')
        self.coordinator_log = self.logging.logger('agent_chat.coordinator')
        self.log.event('startup')
        if inbox_diagnostics:
            from .inbox_diagnostics import InboxDiagnostics
            self.inbox_diagnostics = InboxDiagnostics(inbox_diagnostics)
        self.listen_host = address[0]
        self.address_family = socket.AF_INET6 if ':' in address[0] else socket.AF_INET
        self.public_url = validate_config(address[0], api_token, public_url)
        self.trusted_proxies = proxy_networks(trusted_proxies, api_token, self.public_url)
        self.bridge_manager = None
        self.usage = UsageStore(self.db_path)
        self.measurements = None
        self.projects = Projects(self.db_path)
        from .root_orchestrator import RootOrchestrator
        self.roots = RootOrchestrator(self.projects)
        self.root_lock = self.projects.root_lock
        self.project_contexts = {}
        self.project_lock = threading.RLock()
        super().__init__(address, handler)
        try:
            self.request_origins = RequestOrigins(address[0], self.server_port, self.public_url, self.trusted_proxies)
            self.sender_session = operator_session(self.db_path)
            if api_token:
                from .bridge_lease import BridgeManager
                self.bridge_manager = BridgeManager(self.db_path, usage_store=self.usage)
            self.measurements = MeasurementStore(self.db_path, sessions_root=sessions_root, usage_store=self.usage,
                                                 pause_callback=self.pause_measured_agents)
            if self.bridge_manager:
                self.bridge_manager.roots = self.roots
                self.bridge_manager.measurements = self.measurements
            self.session_models = SessionModels(self.measurements.sessions_root)
            self.roots.sync()
        except BaseException:
            self.server_close()
            raise

    @property
    def origin(self):
        host = '[' + self.listen_host + ']' if ':' in self.listen_host else self.listen_host
        return self.public_url or origin(f'http://{host}:{self.server_port}')

    def pause_measured_agents(self, project_id, database, measurement_id, recipients):
        current_path, sender, _ = self.project_context(project_id)
        if Path(database).resolve() != Path(current_path).resolve():
            raise CoordError('measurement project database changed')
        request_pauses(current_path, sender, measurement_id, recipients)

    def listed_projects(self):
        active = self.measurements.active_projects()
        return [dict(project, measurement_running=project['id'] in active)
                for project in self.projects.list()]

    def measurement_models(self, result):
        """Annotate reports using their recorded threads, including retired agents."""
        agents = [agent for key in ('active', 'latest') if result.get(key)
                  for agent in result[key]['agents']]
        metadata = self.session_models.read(agent['thread_id'] for agent in agents)
        for agent in agents:
            observed = metadata.get(agent['thread_id'], {})
            agent.update(model=observed.get('model'), reasoning_effort=observed.get('reasoning_effort'))
        return result

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
                    manager = BridgeManager(path, usage_store=self.usage)
                    manager.measurements, manager.project_id = self.measurements, item['id']
                    manager.roots = self.roots
                self.project_contexts[item['id']] = (path, sender, manager)
            return self.project_contexts[item['id']]

    def server_close(self):
        self.log.event('shutdown')
        self.logging.close()
        if self.inbox_diagnostics:
            self.inbox_diagnostics.close()
        if self.measurements:
            self.measurements.close()
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
        self._inbox_trace = None
        self._log_id = new_correlation_id()
        self._request_started = False
        self._request_terminal = False

    def _event(self, event, *, level='INFO', error=None, category=None):
        if error is not None:
            self.server.log.error(event, error, correlation_id=self._log_id)
        else:
            self.server.log.event(event, level=level, correlation_id=self._log_id,
                                  error_category=category)

    def _finish_request(self, *, error=None, rejected=False):
        if not self._request_started or self._request_terminal:
            return
        self._request_terminal = True
        if rejected:
            self._event('request_rejected', level='WARNING', category='authentication')
        elif error is None:
            self._event('request_complete')
        else:
            from .app_logging import error_category
            self._event('request_complete', level='ERROR', category=error_category(error))

    def log_message(self, *args):
        pass

    def trusted(self):
        return self.server.request_origins.trusted(self.headers, self.client_address[0])

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

    def operator_browser_channel(self):
        # Existing Basic UI enrollment contract only; never promote machine
        # bearer/quoted IDs to a named operator or borrow an agent recipient.
        return bool(self.server.api_token and self.headers.get('Authorization', '').startswith('Basic ')
                    and authorized(self.headers.get('Authorization', ''), self.server.api_token))

    def operator_ack_rows(self, message_id=None, root_view=False):
        with reader(self.db_path) as db:
            if not db.execute('SELECT 1 FROM operator_identities WHERE session_id=?', (self.sender_session,)).fetchone():
                raise CoordError('current operator enrollment is unavailable')
            if root_view:
                root = self.server.roots.config()
                if not root or root['home_project'] != self.project['id']:
                    raise CoordError('current Root conversation is unavailable')
                condition = '(m.sender_session=? AND m.recipient_session=?)'
                arguments = [root['home_session'], self.sender_session]
            else:
                condition, arguments = project_message_filter(db, self.sender_session)
                arguments = list(arguments)
            condition += ' AND m.recipient_session=? AND m.sender_session<>?'
            arguments.extend((self.sender_session, self.sender_session))
            if message_id is None:
                condition += ' AND m.acked_at IS NULL'
            else:
                condition += ' AND m.id=?'; arguments.append(message_id)
            return list(db.execute('SELECT m.id,m.acked_at FROM messages m WHERE '+condition+' ORDER BY m.seq', arguments))

    def select_existing_operator_context(self, root_view=False):
        # Planning or rejecting an ACK must not create an operator identity,
        # sidecar, project store or bridge manager as a side effect.
        if root_view:
            root = self.server.roots.config()
            if not root: raise CoordError('current Root conversation is unavailable')
            self.project = {'id': root['home_project']}
            query = parse_qs(urlsplit(self.path).query, keep_blank_values=True)
            if set(query) - {'project'} or query.get('project', [self.project['id']]) != [self.project['id']]:
                raise CoordError('choose exactly the current Root project')
        else:
            self.select_project(metadata_only=True)
            query = parse_qs(urlsplit(self.path).query, keep_blank_values=True)
            if set(query) - {'project'}: raise CoordError('invalid operator ACK context')
        if self.project['id'] == 'default':
            current = (self.server.db_path, self.server.sender_session, self.server.bridge_manager)
        else:
            with self.server.project_lock:
                current = self.server.project_contexts.get(self.project['id'])
        if not current:
            raise CoordError('operator context is not established; open the legitimate project context first')
        self.db_path, self.sender_session, self.bridge_manager = current

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

    def respond(self, code, raw, mime, *, download_name=None, extra_headers=None):
        trace = self._inbox_trace
        if trace: trace.mark('response', 'start')
        try:
            self.send_response(code)
            self.send_header('Content-Type', mime)
            self.send_header('Content-Length', str(len(raw)))
            self.send_header('Cache-Control', 'no-store')
            self.send_header('X-Content-Type-Options', 'nosniff')
            self.send_header('Content-Security-Policy', "default-src 'none'; sandbox" if download_name is not None else CSP)
            if download_name is not None:
                self.send_header('Content-Disposition', "attachment; filename*=UTF-8''" + quote(download_name, safe=''))
            for name, value in (extra_headers or {}).items():
                self.send_header(name, value)
            self.send_header('Connection', 'close')
            self.end_headers()
            self.close_connection = True
            try:
                self.wfile.write(raw)
            except (BrokenPipeError, ConnectionResetError, TimeoutError):
                if trace: trace.mark('response', 'end', 'error')
                self._finish_request(error=TimeoutError())
            else:
                if trace: trace.mark('response', 'end')
                self._finish_request()
        except BaseException:
            if trace: trace.mark('response', 'end', 'error')
            self._finish_request(error=RuntimeError())
            raise

    def respond_video(self, content, mime):
        headers = {'Accept-Ranges': 'bytes'}
        ranges = self.headers.get_all('Range', [])
        if not ranges:
            self.respond(200, content, mime, extra_headers=headers)
            return
        try:
            if len(ranges) != 1 or not ranges[0].startswith('bytes='):
                raise ValueError('invalid byte range')
            first, last = ranges[0][6:].split('-')
            if not ((first and first.isascii() and first.isdecimal()) or first == ''):
                raise ValueError('invalid range start')
            if not ((last and last.isascii() and last.isdecimal()) or last == ''):
                raise ValueError('invalid range end')
            if not first:
                suffix = int(last)
                if suffix <= 0:
                    raise ValueError('invalid suffix')
                start, end = max(0, len(content) - suffix), len(content) - 1
            else:
                start = int(first)
                end = min(int(last), len(content) - 1) if last else len(content) - 1
            if not 0 <= start <= end < len(content):
                raise ValueError('unsatisfiable range')
        except ValueError:
            headers['Content-Range'] = f'bytes */{len(content)}'
            self.respond(416, b'', mime, extra_headers=headers)
            return
        headers['Content-Range'] = f'bytes {start}-{end}/{len(content)}'
        self.respond(206, content[start:end+1], mime, extra_headers=headers)

    def select_project(self, *, metadata_only=False):
        query = parse_qs(urlsplit(self.path).query, keep_blank_values=True)
        values = query.get('project', [])
        headers = self.headers.get_all('X-Agent-Chat-Project', [])
        if len(values) > 1 or len(headers) > 1 or (values and headers and values[0] != headers[0]):
            raise CoordError('choose exactly one project')
        project = (values or headers or ['default'])[0]
        if metadata_only:
            if not isinstance(project, str) or not project:
                raise CoordError('project ID is required')
            try:
                with self.server.roots._readiness_db(self.server.projects.path) as db:
                    row = db.execute('SELECT id FROM projects WHERE id=?', (project,)).fetchone()
            except (sqlite3.Error, OSError):
                raise CoordError('readiness project registry is unavailable or unsupported') from None
            if not row:
                raise CoordError('unknown project; select an existing project or create one explicitly')
            self.project = dict(row)
        else:
            self.project = self.server.projects.get(project)
            self.db_path, self.sender_session, self.bridge_manager = self.server.project_context(project)

    def select_root_home(self):
        root = self.server.roots.config()
        self.project = self.server.projects.get(root['home_project'] if root else 'default')
        self.db_path, self.sender_session, self.bridge_manager = self.server.project_context(self.project['id'])
        return root

    def root_snapshot(self):
        root = self.select_root_home()
        root_id = root['home_session'] if root else None
        messages = self.root_page() if root else {'messages':[], 'has_more':False}
        with reader(self.db_path) as db:
            from .bridge_state import runtime_snapshot
            bridge = runtime_snapshot(db)
            sessions = [dict(row) for row in db.execute('SELECT id,agent,registered_at FROM sessions WHERE id IN (?,?)', (self.sender_session,root_id))]
            total = db.execute('''SELECT COUNT(*) FROM messages m WHERE
                ((sender_session=? AND recipient_session=?) OR (sender_session=? AND recipient_session=?))
                AND NOT EXISTS (SELECT 1 FROM message_batches b WHERE b.message_id=m.id)''',
                (self.sender_session,root_id,root_id,self.sender_session)).fetchone()[0]
            for session in sessions:
                if session['id'] == root_id:
                    session.update(thread_id=root['thread_id'],is_root_orchestrator=True,is_orchestrator=True)
        return dict(messages=messages['messages'],history_truncated=messages['has_more'],total_messages=total,
            sessions=sessions,resources=[],session_resets=[],server_time=time.time(),
            usage=self.server.usage.status(),bridge=bridge,project=self.project,
            projects=self.server.listed_projects(),root_orchestrator=self.root_summary(),root_overview=self.server.roots.status())

    def root_page(self, before=None, limit=UI_MESSAGE_LIMIT, after=None, to_me=False):
        root = self.server.roots.config()
        if not root:
            return {'messages':[], 'has_more':False}
        result = page(self.db_path,before,limit,after,self.sender_session,root['home_session'],to_me)
        return result

    def scoped(self, data):
        if not hasattr(self, 'project') or self.project['id'] == 'default': return data
        if isinstance(data, list): return [self.scoped(item) for item in data]
        if isinstance(data, dict):
            return {key: (value + '?project=' + self.project['id'] if key == 'url' and isinstance(value, str) and value.startswith('/api/attachments/') else self.scoped(value)) for key, value in data.items()}
        return data

    def project_snapshot(self):
        if urlsplit(self.path).path == '/api/root/events':
            return self.root_snapshot()
        params = parse_qs(urlsplit(self.path).query, keep_blank_values=True)
        paging = {}
        if 'resource_limit' in params or 'resource_offset' in params:
            for name, default in [('resource_limit', '50'), ('resource_offset', '0')]:
                values = params.get(name, [default])
                if len(values) != 1 or not values[0].isascii() or not values[0].isdecimal():
                    raise ValueError('Resource page parameters must be single nonnegative integers')
                paging[name] = int(values[0])
            if not 1 <= paging['resource_limit'] <= 200:
                raise ValueError('Resource page limit must be from 1 to 200')
        return dict(snapshot(self.db_path, operator=self.sender_session, usage_store=self.server.usage, session_models=self.server.session_models, **paging), project=self.selected_project(), projects=self.server.listed_projects(), root_orchestrator=self.root_summary())

    def selected_project(self):
        project = dict(self.project)
        for configured in self.server.roots.status().get('projects', []):
            if configured['project_id'] == project['id'] and configured.get('cwd'):
                project['cwd'] = configured['cwd']
                break
        return project

    def root_summary(self):
        root = self.server.roots.config()
        return {key: root[key] for key in ('home_project','home_session','agent','mode')} if root else None

    def json(self, code, data):
        self.respond(code, json.dumps(self.scoped(data), ensure_ascii=False, separators=(',', ':')).encode('utf-8'), 'application/json; charset=utf-8')

    def do_GET(self):
        if not self.trusted():
            self.json(403, {'error': 'Untrusted host or origin'})
            return
        url = urlsplit(self.path)
        if not self.authenticate():
            return
        try:
            root_view = url.path.startswith('/api/root/') and url.path != '/api/root/rpc'
            ack_plan = url.path in ('/api/messages/ack-plan','/api/root/messages/ack-plan')
            if ack_plan:
                if not self.operator_browser_channel():
                    self.json(403, {'error': 'authenticated operator browser channel required'}); return
                self.select_existing_operator_context(root_view)
            root = self.select_root_home() if root_view and not ack_plan else None
            if url.path.startswith('/api/') and url.path != '/api/usage' and not root_view and not ack_plan and not url.path.startswith(('/api/communications', '/api/attachment-gallery')):
                self.select_project()
            if url.path in ('/api/attachment-gallery', '/api/root/attachment-gallery'):
                from .attachment_gallery import AttachmentGallery
                params = parse_qs(url.query, keep_blank_values=True)
                if set(params) - {'project', 'q', 'before', 'limit'} or any(len(value) != 1 for value in params.values()):
                    raise ValueError('invalid attachment query parameters')
                values = {key: value[0] for key, value in params.items()}
                raw_limit = values.pop('limit', '48')
                if not raw_limit.isascii() or not raw_limit.isdigit():
                    raise ValueError('invalid attachment limit')
                self.project = {'id': 'default'}  # File URLs carry their source project.
                if root_view and not root:
                    self.json(200, dict(attachments=[], has_more=False, next_cursor=None, projects=[], partial=False, errors=[]))
                else:
                    scope = (root['home_project'], root['home_session'], self.sender_session) if root_view else None
                    self.json(200, AttachmentGallery(self.server.projects).page(limit=int(raw_limit), root=scope, **values))
            elif url.path in ('/api/communications','/api/communications/message'):
                from .communication_view import CommunicationView
                params = parse_qs(url.query,keep_blank_values=True)
                allowed = {'project','id'} if url.path.endswith('/message') else {'project','agent','q','before','after','limit'}
                if set(params)-allowed or any(len(value)!=1 for value in params.values()):
                    raise ValueError('invalid communication query parameters')
                values = {key:value[0] for key,value in params.items()}
                view = CommunicationView(self.server.projects)
                self.project = {'id':'default'}  # Aggregate rows carry their own source project.
                if url.path.endswith('/message'):
                    if set(values)!={'project','id'}: raise ValueError('project and message ID required')
                    self.json(200,view.message(values['project'],values['id']))
                else:
                    raw_limit=values.pop('limit','50')
                    if not raw_limit.isascii() or not raw_limit.isdigit(): raise ValueError('invalid communication limit')
                    self.json(200,view.page(limit=int(raw_limit),**values))
            elif url.path == '/api/projects':
                self.json(200, {'projects': self.server.listed_projects(), 'project': self.project})
            elif url.path == '/api/root':
                self.json(200, self.server.roots.status())
            elif url.path == '/api/usage':
                self.json(200, self.server.usage.status())
            elif url.path == '/api/measurements':
                self.json(200, self.server.measurement_models(self.server.measurements.status(self.project['id'])))
            elif url.path in ('/api/snapshot','/api/root/snapshot'):
                self.json(200, self.root_snapshot() if root_view else self.project_snapshot())
            elif url.path in ('/api/messages/ack-plan','/api/root/messages/ack-plan'):
                if not self.operator_browser_channel():
                    self.json(403, {'error': 'authenticated operator browser channel required'}); return
                rows = self.operator_ack_rows(root_view=root_view)
                self.json(200, dict(version=1, observed_at=time.time(), project_id=self.project['id'], recipient_session=self.sender_session,
                                    ids=[row['id'] for row in rows], count=len(rows), effect='ACK_ONLY',
                                    execution='REVIEW_EXACT_IDS_THEN_INDIVIDUAL_IDEMPOTENT_ACK', read_only=True))
            elif url.path in ('/api/config','/api/root/config'):
                self.json(200, {'sender': {'id': self.sender_session, 'agent': 'operator'},
                                'operator_ack_enabled': self.operator_browser_channel(),
                                'csrf_token': self.server.csrf_token, 'project': self.selected_project(),
                                'projects': self.server.listed_projects(), 'root_orchestrator': self.root_summary()})
            elif url.path in ('/api/messages','/api/root/messages'):
                params = parse_qs(url.query, keep_blank_values=True)
                before_values = params.get('before', [])
                after_values = params.get('after', [])
                limit_values = params.get('limit', [str(UI_MESSAGE_LIMIT)])
                agent_values = params.get('agent', [])
                to_me_values = params.get('to_me', [])
                if (len(before_values) > 1 or len(after_values) > 1 or len(limit_values) != 1
                        or len(agent_values) > 1 or len(to_me_values) > 1
                        or to_me_values not in ([], ['1'])):
                    raise ValueError('History cursor and limit parameters must appear once')
                if before_values and after_values:
                    raise ValueError('Choose either a before or after history cursor')

                def positive_integer(values, name):
                    if not values:
                        return None
                    value = values[0]
                    if not value.isascii() or not value.isdigit() or int(value) < 1:
                        raise ValueError(f'{name} must be a positive integer')
                    return int(value)

                before = positive_integer(before_values, 'before')
                after = positive_integer(after_values, 'after')
                limit = positive_integer(limit_values, 'limit')
                if root_view and (not root or agent_values and agent_values != [root['home_session']]):
                    if root: raise CoordError('root screen shows only the operator/root conversation')
                    self.json(200, {'messages':[], 'has_more':False})
                else:
                    self.json(200, self.root_page(before,limit,after,bool(to_me_values)) if root_view else
                        page(self.db_path,before,limit,after,self.sender_session,agent_values[0] if agent_values else None,bool(to_me_values),project_feed=True))
            elif url.path.startswith(('/api/messages/','/api/root/messages/')):
                message_id = url.path.rsplit('/',1)[1]
                if set(parse_qs(url.query)) - {'project'} or not message_id.startswith('message_') or '/' in message_id or not all(char.isalnum() or char in '_-' for char in message_id):
                    self.json(404, {'error': 'Not found'})
                    return
                with reader(self.db_path) as db:
                    agents = agent_labels(db)
                    condition, args = project_message_filter(db, None if root_view else self.sender_session)
                    row = db.execute(f'SELECT {MESSAGE_COLUMNS} FROM messages m WHERE id=? AND ({condition})',
                                     (message_id, *args)).fetchone()
                    if root_view and row and (not root or (row['sender_session'],row['recipient_session']) not in
                            ((self.sender_session,root['home_session']),(root['home_session'],self.sender_session))
                            or db.execute('SELECT 1 FROM message_batches WHERE message_id=?',(message_id,)).fetchone()):
                        row = None
                    item = message_rows([row], agents, db, None if root_view else self.sender_session)[0] if row else None
                if item is None:
                    self.json(404, {'error': 'Not found'})
                else:
                    self.json(200, item)
            elif url.path.startswith(('/api/attachments/', '/api/gallery-attachments/', '/api/root/gallery-attachments/')):
                attachment_id = url.path.rsplit('/', 1)[1]
                gallery_file = '/gallery-attachments/' in url.path
                if set(parse_qs(url.query)) - {'project'} or not attachment_id.startswith('attachment_') or '/' in attachment_id:
                    self.json(404, {'error': 'Not found'})
                    return
                with reader(self.db_path) as db:
                    if not attachment_table(db):
                        self.json(404, {'error': 'Not found'})
                        return
                    if gallery_file:
                        if root_view:
                            condition = '((m.sender_session=? AND m.recipient_session=?) OR (m.sender_session=? AND m.recipient_session=?))'
                            args = (root['home_session'], self.sender_session, self.sender_session, root['home_session']) if root else (None,) * 4
                            if batch_table(db):
                                condition += ' AND NOT EXISTS (SELECT 1 FROM message_batches b WHERE b.message_id=m.id)'
                        else:
                            from .communication_view import CommunicationView
                            condition, args, _ = CommunicationView.private_filter(db)
                        item = db.execute('SELECT a.name,a.mime,a.content FROM attachments a JOIN messages m ON m.id=a.message_id '
                                          'WHERE a.id=? AND (' + condition + ')', (attachment_id, *args)).fetchone()
                    else:
                        item = db.execute('SELECT name,mime,content FROM attachments WHERE id=?', (attachment_id,)).fetchone()
                if item is None:
                    self.json(404, {'error': 'Not found'})
                    return
                if item['mime'].startswith('video/'):
                    self.respond_video(item['content'], item['mime'])
                else:
                    self.respond(200, item['content'], item['mime'],
                                 download_name=None if item['mime'] in {'image/png', 'image/jpeg', 'image/gif', 'image/webp'} else item['name'])
            elif url.path in ('/api/events','/api/root/events'):
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
                raw = path.read_bytes()
                if spec[0] == 'index.html':
                    raw = raw.replace(b'<!-- agent-chat:all-communication-menu -->',
                        b'<a class="room-button" id="all-communication-view" href="/communications"><span class="room-icon" aria-hidden="true">&#9678;</span><span>All communication</span></a>')
                self.respond(200, raw, spec[1])
        except (ValueError, sqlite3.Error, RuntimeError, OSError) as error:
            self.json(400, {'error': str(error)})

    def do_POST(self):
        self._request_started = True
        self._event('request_start')
        handler_wall, handler_monotonic = time.time(), time.monotonic()
        if not self.trusted():
            self._finish_request(rejected=True)
            self.json(403, {'error': 'Untrusted host or origin'})
            return
        path = urlsplit(self.path).path
        machine_api = path in ('/api/coord', '/api/bridge/rpc', '/api/projects/rpc', '/api/usage/rpc', '/api/root/rpc')
        if not self.authenticate(bearer_only=machine_api):
            self._finish_request(rejected=True)
            return
        timeline = []
        if path == '/api/coord' and self.server.inbox_diagnostics:
            timeline.append(('handler', 'start', 'ok', handler_wall, handler_monotonic))
        if path in ('/api/messages/ack','/api/root/messages/ack') and not self.operator_browser_channel():
            self.json(403, {'error': 'authenticated operator browser channel required'}); return
        try:
            if path not in ('/api/usage', '/api/usage/rpc', '/api/coord'):
                if timeline: timeline.append(('project', 'start', 'ok', time.time(), time.monotonic()))
                if path in ('/api/messages/ack','/api/root/messages/ack'): self.select_existing_operator_context(path.startswith('/api/root/'))
                else: self.select_project(metadata_only=path == '/api/root/rpc')
                if timeline: timeline.append(('project', 'end', 'ok', time.time(), time.monotonic()))
        except (CoordError, ValueError, sqlite3.Error, OSError) as error:
            self._event('operation_failed', error=error)
            self._finish_request(error=error)
            self.json(400, {'error': str(error)})
            return
        if machine_api:
            trace = None
            try:
                self._event('operation_start')
                if timeline: timeline.append(('body', 'start', 'ok', time.time(), time.monotonic()))
                data = self.read_json(MAX_COORD_REQUEST_BYTES if path == '/api/coord' else 1024 * 1024)
                if timeline: timeline.append(('body', 'end', 'ok', time.time(), time.monotonic()))
                if path == '/api/coord':
                    if timeline: timeline.append(('project', 'start', 'ok', time.time(), time.monotonic()))
                    attachment_read = data.get('op') == 'attachment-download'
                    self.select_project(metadata_only=attachment_read)
                    if (attachment_read and isinstance(data.get('params'), dict)
                            and data['params'].get('source_project') == self.project['id']):
                        # Preserve the existing direct-home coordinator path.
                        self.select_project()
                    if timeline: timeline.append(('project', 'end', 'ok', time.time(), time.monotonic()))
                if path == '/api/coord' and data.get('op') == 'inbox' and self.server.inbox_diagnostics:
                    from .inbox_diagnostics import TRACE_HEADER
                    trace = self.server.inbox_diagnostics.trace(
                        self.headers.get_all(TRACE_HEADER), timeline,
                        started_wall=handler_wall, started_monotonic=handler_monotonic)
                    self._inbox_trace = trace
                    if trace: trace.mark('dispatch', 'start')
                if path == '/api/usage/rpc':
                    self.json(200, self.usage_action(data, machine=True))
                    return
                if path == '/api/projects/rpc':
                    self.json(200, self.project_action(data))
                    return
                if path == '/api/root/rpc':
                    self.json(200, self.shared_bearer_root_action(data))
                    return
                if path == '/api/coord':
                    from .remote_service import dispatch, managed_operator_attachment
                    self.server.coordinator_log.event('operation_start', correlation_id=self._log_id)
                    if attachment_read and (not isinstance(data.get('params'), dict)
                                            or data['params'].get('source_project') != self.project['id']):
                        result = managed_operator_attachment(data, self.project['id'], self.server.roots)
                        self.server.coordinator_log.event('operation_complete', correlation_id=self._log_id)
                        self._event('operation_complete')
                        self.json(200, self.scoped(result))
                        return
                    coord = Coordinator(self.db_path)
                    coord.session = None
                    coord.context_size = lambda value: len(json.dumps(
                        self.scoped(value), ensure_ascii=False, separators=(',', ':')).encode('utf-8'))
                    try:
                        with self.server.measurements.lock if data.get('op') in ('session-reset', 'agent-create', 'agent-stop', 'agent-resume', 'agent-model', 'supervisor-configure', 'session-reset-revalidate') else contextlib.nullcontext():
                            if data.get('op') in ('session-reset', 'agent-create', 'agent-stop', 'agent-resume', 'agent-model', 'supervisor-configure', 'session-reset-revalidate') and self.server.measurements.status(self.project['id'])['active']:
                                raise CoordError('finish the active project measurement before resetting a conversation')
                            result = dispatch(coord, data, bridge_manager=self.bridge_manager,
                                              inbox_diagnostics=trace, canonical_project=self.project['id'],
                                              roots=self.server.roots)
                    finally:
                        coord.close()
                    if trace: trace.mark('dispatch', 'end')
                    self.server.coordinator_log.event('operation_complete', correlation_id=self._log_id)
                    self._event('operation_complete')
                else:
                    result = {'result': self.bridge_manager.dispatch(data)}
                self.json(200, result)
            except (CoordError, ValueError, TypeError, KeyError, sqlite3.Error, OSError) as error:
                if path == '/api/coord':
                    self.server.coordinator_log.error('operation_failed', error, correlation_id=self._log_id)
                self._event('operation_failed', error=error)
                self._finish_request(error=error)
                if trace: trace.mark('dispatch', 'end', 'error')
                self.json(400, {'error': str(error)})
            finally:
                self._inbox_trace = None
            return
        if path not in ('/api/messages', '/api/messages/ack', '/api/root/messages/ack', '/api/sessions/remove', '/api/sessions/reset', '/api/projects', '/api/projects/rename', '/api/projects/workspaces', '/api/projects/workspaces-batch', '/api/root/workspaces', '/api/root/provision', '/api/root/fresh-start', '/api/usage', '/api/measurements'):
            self._finish_request(rejected=True)
            self.json(404, {'error': 'Not found'})
            return
        csrf = self.headers.get('X-Agent-Chat-CSRF', '')
        if not csrf.isascii() or not secrets.compare_digest(csrf, self.server.csrf_token):
            self._finish_request(rejected=True)
            self.json(403, {'error': 'Messaging connection expired. Reconnect and try again.'})
            return
        if path in ('/api/messages/ack', '/api/root/messages/ack'):
            try:
                data = self.read_json(2048)
                if not isinstance(data, dict) or set(data) != {'id'} or not isinstance(data['id'], str) or not 1 <= len(data['id']) <= 512:
                    raise ValueError('one exact message ID is required')
                if not self.operator_ack_rows(data['id'], root_view=path.startswith('/api/root/')):
                    self.json(404, {'error': 'received operator message unavailable in this context'}); return
                # Recheck exact recipient under Coordinator's atomic ACK-only
                # transaction. Neither message/full-context reading nor _fresh
                # is invoked; ACK never creates durable consumption evidence.
                with contextlib.closing(Coordinator(self.db_path, self.sender_session)) as coord:
                    result = coord.acknowledge(data['id'])
                self.json(200, dict(result, project_id=self.project['id']))
            except (CoordError, ValueError, sqlite3.Error, OSError) as error:
                self.json(400, {'error': str(error)})
            return
        if path == '/api/measurements':
            try:
                data = self.read_json()
                if (isinstance(data, dict) and {'op', 'duration_seconds'} <= set(data)
                        and set(data) <= {'op', 'duration_seconds', 'pause_at_end'} and data['op'] == 'start'):
                    with self.server.measurements.lock:
                        with contextlib.closing(Coordinator(self.db_path)) as coord:
                            if coord.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='session_resets'").fetchone() and coord.db.execute("SELECT 1 FROM session_resets WHERE status NOT IN ('completed','cancelled','failed')").fetchone():
                                raise CoordError('finish or cancel the pending fresh-session reset before measuring')
                        result = self.server.measurements.start(self.project['id'], self.db_path, data['duration_seconds'],
                                                                pause_at_end=data.get('pause_at_end', False))
                elif isinstance(data, dict) and data == {'op': 'stop'}:
                    result = self.server.measurements.stop(self.project['id'])
                else:
                    raise CoordError('invalid measurement operation')
                self.json(200, self.server.measurement_models(result))
            except (CoordError, ValueError, TypeError, sqlite3.Error, OSError) as error:
                self.json(400, {'error': str(error)})
            return
        if path == '/api/usage':
            try:
                self.json(200, self.usage_action(self.read_json(), machine=False))
            except (CoordError, ValueError, TypeError, sqlite3.Error, OSError) as error:
                self.json(400, {'error': str(error)})
            return
        if path == '/api/root/fresh-start':
            try:
                data=self.read_json(512 * 1024)
                if not isinstance(data,dict) or set(data)!={'expected_thread','prompt','request_id','confirm'}:
                    raise CoordError('invalid protected root fresh-start fields')
                from .root_restart import RootRestart
                with self.server.root_lock:
                    root=self.server.roots.config()
                    if not root: raise CoordError('enroll the root before fresh-start')
                    result=RootRestart(self.server.roots).request(root['home_project'],root['home_session'],root['host_id'],data['expected_thread'],data['prompt'],data['request_id'],data['confirm'],operator=True)
                self.json(200,result)
            except (CoordError,ValueError,TypeError,sqlite3.Error,OSError) as error:
                self.json(400,{'error':str(error)})
            return
        if path in ('/api/root/workspaces', '/api/root/provision'):
            try:
                data = self.read_json(512 * 1024)
                result = self.browser_root_action(data, 'reconfigure-workspaces' if path == '/api/root/workspaces' else 'provision')
                self.json(200, result)
            except (CoordError, ValueError, TypeError, sqlite3.Error, OSError) as error:
                self.json(400, {'error': str(error)})
            return
        if path in ('/api/projects', '/api/projects/rename', '/api/projects/workspaces', '/api/projects/workspaces-batch'):
            try:
                # Escaped JSON can be larger than the validated UTF-8 assignment.
                data = self.read_json(65536 if path == '/api/projects/rename' else 512 * 1024)
                operation = {'/api/projects': 'create', '/api/projects/rename': 'rename', '/api/projects/workspaces': 'configure-workspaces', '/api/projects/workspaces-batch': 'configure-workspaces-batch'}[path]
                result = self.project_action(dict(data, op=operation))
                self.json(200, result)
            except (CoordError, ValueError, TypeError, sqlite3.Error, OSError) as error:
                self.json(400, {'error': str(error)})
            return
        if path == '/api/sessions/reset':
            try:
                data = self.read_json()
                if not isinstance(data, dict):
                    raise CoordError('invalid fresh-session reset request')
                from .bridge_state import BridgeState
                from .session_reset import SessionResetState
                with self.server.measurements.lock, contextlib.closing(Coordinator(self.db_path, self.sender_session)) as coord:
                    BridgeState(coord)
                    resets = SessionResetState(coord)
                    if data.get('op') == 'cancel' and set(data) == {'op', 'id'}:
                        result = resets.cancel(data['id'])
                    elif data.get('op') == 'request' and set(data) == {'op', 'to', 'expected_thread', 'prompt', 'request_id', 'confirm'}:
                        if self.server.measurements.status(self.project['id'])['active']:
                            raise CoordError('finish the active project measurement before resetting a conversation')
                        result = resets.request(data['to'], data['expected_thread'], data['prompt'], data['request_id'], data['confirm'], human=True)
                    elif data.get('op') == 'create' and set(data) == {'op','agent','template_session','expected_thread','prompt','request_id','confirm'}:
                        if self.server.measurements.status(self.project['id'])['active']:
                            raise CoordError('finish the active project measurement before creating an agent')
                        result = resets.create(data['agent'], data['expected_thread'], data['prompt'], data['request_id'], data['confirm'], template_session=data['template_session'], human=True)
                    else:
                        raise CoordError('invalid fresh-session reset operation')
                self.json(200, result)
            except (CoordError, ValueError, TypeError, KeyError, sqlite3.Error, OSError) as error:
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
        content_type = self.headers.get('Content-Type', '').split(';', 1)[0].strip().lower()
        if content_type not in ('application/json', 'multipart/form-data'):
            self.json(415, {'error': 'Expected a JSON message'})
            return
        try:
            if content_type == 'multipart/form-data':
                if len(self.headers.get_all('Content-Length', [])) != 1 or self.headers.get_all('Transfer-Encoding', []):
                    raise ValueError('Multipart uploads require one Content-Length and no Transfer-Encoding')
            size = int(self.headers.get('Content-Length', '0'))
            if content_type == 'application/json':
                if not 1 <= size <= 65536:
                    self.json(413, {'error': 'Message must fit within 64 KiB'})
                    return
                data = json.loads(self.rfile.read(size))
                attachments = None
            else:
                if not 1 <= size <= MAX_MULTIPART_BYTES:
                    self.json(413, {'error': f'Files must fit within {MAX_ATTACHMENTS * MAX_ATTACHMENT_SIZE // (1024 * 1024)} MiB plus 64 KiB of metadata'})
                    return
                data, attachments = self.read_multipart_message(size)
            if not isinstance(data, dict) or 'body' not in data or set(data) - {'to', 'body', 'reply_to'}:
                raise ValueError('Write a message with optional recipients')
            recipient, body = data.get('to'), data['body']
            is_many = recipient is None or isinstance(recipient, list)
            if ((recipient is not None and not isinstance(recipient, (str, list))) or not isinstance(body, str) or
                    (content_type == 'application/json' and not body.strip()) or
                    (content_type == 'multipart/form-data' and not body.strip() and not attachments) or
                    (isinstance(recipient, list) and (not recipient or any(not isinstance(item, str) or not item for item in recipient)))):
                raise ValueError('Choose a recipient and write a nonempty message')
            reply_to = data.get('reply_to')
            if reply_to is not None and (not isinstance(reply_to, str) or not reply_to):
                raise ValueError('Reply parent is invalid')
            with reader(self.db_path) as db:
                if isinstance(recipient, str):
                    if not db.execute('SELECT 1 FROM sessions WHERE id=?', (recipient,)).fetchone():
                        raise ValueError('Recipient is not a registered agent session')
                elif isinstance(recipient, list) and any(not db.execute('SELECT 1 FROM sessions WHERE id=?', (item,)).fetchone() for item in recipient):
                    raise ValueError('Recipient is not a registered agent session')
            with tempfile.TemporaryDirectory(prefix='agent-chat-upload-') as directory:
                upload_paths = self.write_uploads(directory, attachments or [])
                coord = Coordinator(self.db_path, self.sender_session)
                try:
                    coord._operator_message_origin = True
                    sent = (coord.send_group_prepared(recipient, body.strip(), coord._prepare_attachments(upload_paths), reply_to)
                            if is_many else coord.send(recipient, body.strip(), attachments=upload_paths, reply_to=reply_to))
                finally:
                    coord.close()
            self.json(200, {'messages': sent} if is_many else sent)
        except (CoordError, ValueError, sqlite3.Error, RuntimeError, OSError) as error:
            self.json(400, {'error': str(error)})

    def read_multipart_message(self, size):
        """Decode the deliberately small form shape accepted by browser uploads."""
        raw = self.rfile.read(size)
        if len(raw) != size:
            raise ValueError('Malformed multipart message')
        content_type = self.headers.get('Content-Type', '')
        try:
            header = content_type.encode('ascii', 'strict')
        except UnicodeEncodeError as error:
            raise ValueError('Malformed multipart message') from error
        message = BytesParser(policy=email_policy).parsebytes(b'Content-Type: ' + header + b'\r\n\r\n' + raw)
        if not message.is_multipart() or message.defects:
            raise ValueError('Malformed multipart message')
        data = None
        has_message = False
        images = []
        for part in message.iter_parts():
            if part.defects or part.get_content_disposition() != 'form-data':
                raise ValueError('Malformed multipart message')
            name = part.get_param('name', header='content-disposition')
            if name == 'message':
                if has_message or part.get_filename() is not None:
                    raise ValueError('Multipart message must contain one message field')
                has_message = True
                try:
                    payload = part.get_payload(decode=True)
                    if payload is None:
                        raise ValueError('Multipart message field must be UTF-8 JSON')
                    if len(payload) > 64 * 1024:
                        raise ValueError('Multipart message metadata must fit within 64 KiB')
                    data = json.loads(payload.decode('utf-8'))
                except (UnicodeDecodeError, json.JSONDecodeError) as error:
                    raise ValueError('Multipart message field must be UTF-8 JSON') from error
            elif name == 'images':
                filename = part.get_filename()
                if not filename:
                    raise ValueError('Attachment filename is required')
                content = part.get_payload(decode=True)
                if content is None:
                    raise ValueError('Malformed attachment upload')
                images.append((self.sanitize_upload_name(filename), content))
            else:
                raise ValueError('Multipart message contains an unsupported field')
        if not has_message:
            raise ValueError('Multipart message must contain one message field')
        return data, images

    @staticmethod
    def sanitize_upload_name(name):
        return Coordinator._attachment_name(name)

    @staticmethod
    def write_uploads(directory, uploads):
        if len(uploads) > MAX_ATTACHMENTS:
            raise CoordError(f'at most {MAX_ATTACHMENTS} attachments are allowed')
        if any(len(content) > MAX_ATTACHMENT_SIZE for _, content in uploads):
            raise CoordError('attachment exceeds 10 MiB')
        paths = []
        for index, (name, content) in enumerate(uploads):
            child = Path(directory) / str(index)
            child.mkdir()
            path = child / name
            path.write_bytes(content)
            paths.append(path)
        return paths

    def project_action(self, data):
        op = data.get('op')
        if op == 'move-history':
            return self.move_history(data)
        if op == 'list' and set(data) == {'op'}:
            return {'projects': self.server.listed_projects()}
        if op == 'create' and {'op', 'name'} <= set(data) <= {'op', 'name', 'workspace', 'workspaces', 'instructions'}:
            item = self.server.projects.create(data['name'], data.get('workspace'), data.get('instructions'), workspaces=data.get('workspaces'))
            self.server.project_context(item['id'])
        elif op == 'configure-workspaces' and set(data) == {'op', 'id', 'workspaces'}:
            item = self.server.projects.configure_workspaces(data['id'], data['workspaces'])
        elif op == 'configure-workspaces-batch' and set(data) == {'op', 'assignments'}:
            items = self.server.projects.configure_workspaces_batch(data['assignments'])
            return {'projects': self.server.listed_projects(), 'configured_projects': [{key: value for key, value in item.items() if key != 'instructions'} for item in items]}
        elif op == 'rename' and set(data) == {'op', 'id', 'name'}:
            item = self.server.projects.rename(data['id'], data['name'])
        else:
            raise CoordError('invalid project operation')
        public = {key: value for key, value in item.items() if key != 'instructions'}
        public['has_instructions'] = bool(item.get('instructions'))
        return {'project': public, 'projects': self.server.listed_projects()}

    def move_history(self, data):
        from .history_move import HistoryMove
        from .core import assert_agent_running
        fields = {'op','source_project','source_session','request_id','expected_count','expected_digest',
                  'confirm','session','host_id','token'}
        if set(data) != fields or not isinstance(data['confirm'],bool):
            raise CoordError('invalid history move request')
        if any(not isinstance(data[key],str) or not data[key] or len(data[key]) > 256
               for key in ('source_project','source_session','request_id','session','host_id')):
            raise CoordError('history move requires exact project, session, host and request identities')
        source_project = data['source_project']
        with self.server.root_lock, self.server.measurements.lock:
            if any(self.server.measurements.status(project)['active'] for project in (source_project,self.project['id'])):
                raise CoordError('finish source and destination project measurements before moving history')
            with contextlib.closing(Coordinator(self.db_path, data['session'])) as coord:
                db = coord.db
                if not db.execute('SELECT 1 FROM project_orchestrators WHERE session_id=?', (data['session'],)).fetchone():
                    raise CoordError('only the designated destination project orchestrator may move history')
                if not db.execute('SELECT 1 FROM remote_session_hosts WHERE session_id=? AND host_id=?',
                                  (data['session'],data['host_id'])).fetchone():
                    raise CoordError('history move must use the destination orchestrator own host')
                assert_agent_running(db,data['session'])
                resource = 'history-move:' + source_project + ':' + data['source_session']
                if data['confirm']:
                    if (isinstance(data['expected_count'],bool) or not isinstance(data['expected_count'],int)
                            or data['expected_count'] < 1 or not isinstance(data['expected_digest'],str)
                            or len(data['expected_digest']) != 64
                            or any(c not in '0123456789abcdef' for c in data['expected_digest'])):
                        raise CoordError('confirm the exact positive count and SHA-256 digest from preview')
                    reservation = coord.check(resource,data['token'],require_fresh=False)
                    if not db.execute('''SELECT 1 FROM remote_guarded_runs WHERE resource=? AND session_id=?
                            AND host_id=? AND reservation_id=? AND local_pid>0 AND closed_at IS NULL''',
                            (resource,data['session'],data['host_id'],reservation['reservation_id'])).fetchone():
                        raise CoordError('confirm history moves through a guarded run for the exact history resource')
            def authorize_locked(db):
                if not db.execute('SELECT 1 FROM project_orchestrators WHERE session_id=?', (data['session'],)).fetchone():
                    raise CoordError('destination orchestrator designation changed')
                if not db.execute('SELECT 1 FROM remote_session_hosts WHERE session_id=? AND host_id=?',
                                  (data['session'],data['host_id'])).fetchone():
                    raise CoordError('destination orchestrator host changed')
                assert_agent_running(db,data['session'])
                held = db.execute('SELECT * FROM resources WHERE name=?', (resource,)).fetchone()
                if (not held or held['owner_session'] != data['session'] or Coordinator._state(held) != 'owned'
                        or not isinstance(data['token'],str)
                        or not secrets.compare_digest(held['token'] or '',data['token'])):
                    raise CoordError('history reservation changed or expired before mutation')
                if not db.execute('''SELECT 1 FROM remote_guarded_runs WHERE resource=? AND session_id=?
                        AND host_id=? AND reservation_id=? AND local_pid>0 AND closed_at IS NULL''',
                        (resource,data['session'],data['host_id'],held['reservation_id'])).fetchone():
                    raise CoordError('history guarded execution changed before mutation')
            source_path, source_operator, _ = self.server.project_context(source_project)
            move = HistoryMove(source_path,self.db_path,source_project,self.project['id'],
                               data['source_session'],data['session'],source_operator,self.sender_session)
            result = (move.execute(data['request_id'],data['expected_count'],data['expected_digest'],authorize_locked)
                      if data['confirm'] else move.preview(data['request_id']))
            return dict(result,resource=resource)

    def browser_root_action(self, data, operation):
        fields = ({'project','expected_thread','workspaces','prompt','request_id','confirm'}
                  if operation == 'reconfigure-workspaces' else {'project','agent','request_id','confirm','model','effort'})
        if not isinstance(data, dict) or set(data) != fields:
            raise CoordError('invalid browser root workspace operation')
        with self.server.root_lock, self.server.measurements.lock:
            root = self.server.roots.config()
            if not root:
                raise CoordError('enroll an existing authorized root before project setup')
            args = (root['home_project'], root['home_session'], root['host_id'])
            self.server.roots.authorize(*args)
            if self.server.measurements.status(data['project'])['active']:
                raise CoordError('finish the active target project measurement before orchestration setup')
            if operation == 'reconfigure-workspaces':
                return self.server.roots.reconfigure_workspaces(*args, data['project'], data['expected_thread'], data['workspaces'], data['prompt'], data['request_id'], data['confirm'])
            return self.server.roots.provision(*args, data['project'], data['agent'], data['request_id'], data['confirm'], data['model'], data['effort'])

    def shared_bearer_root_action(self, data):
        """Expose only exact, owner-bound Root RPC status readbacks to the shared API token."""
        fields = {
            'readiness': {'projects', 'expected_thread'},
            'fresh-status': {'request_id'},
            'stop-status': {'request_id'},
        }
        if (not isinstance(data, dict) or set(data) != {'op', 'session', 'host_id', 'params'}
                or not all(isinstance(data.get(key), str) and data[key] for key in ('op', 'session', 'host_id'))
                or not isinstance(data.get('params'), dict)):
            raise CoordError('invalid shared-bearer root readback request')
        op, params = data['op'], data['params']
        if op not in fields or set(params) != fields[op]:
            raise CoordError('shared bearer permits only exact root status readbacks')
        if ('request_id' in params and
                (not isinstance(params['request_id'], str) or not params['request_id'])):
            raise CoordError('invalid shared-bearer root readback request')
        if op == 'readiness':
            with self.server.root_lock:
                return self.server.roots.readiness(self.project['id'], data['session'], data['host_id'],
                                                   params['projects'], params['expected_thread'])
        return self.root_action(data)

    def root_action(self, data):
        if not isinstance(data, dict) or set(data) != {'op','session','host_id','params'} or not isinstance(data['params'], dict):
            raise CoordError('invalid root orchestrator request')
        op, params = data['op'], data['params']
        fields = {
            'fresh-start': {'expected_thread','prompt','request_id','confirm'}, 'fresh-status': {'request_id'},
            'team-reset-preview': {'project','message_ids'},
            'team-reset': {'project','message_ids','request_id','expected_digest','confirm'},
            'team-reset-status': {'project','request_id'},
            'delegate-management': {'project','po','target','expected_thread','expected_host','request_id','confirm'},
            'enroll': {'expected_thread','confirm'}, 'status': set(), 'route': {'project'},
            'stop-all': {'request_id','confirm'}, 'resume-all': {'request_id','confirm'}, 'stop-status': {'request_id'},
            'configure': {'project','agent','cwd','prompt','request_id','confirm','model','effort'},
            'provision': {'project','agent','request_id','confirm','model','effort'},
            'adopt': {'project','target','expected_thread','cwd','confirm'}, 'detach': {'confirm'}, 'resume-routing': {'confirm'},
            'retry-setup': {'project','request_id','confirm'},
            'reconfigure-workspaces': {'project','expected_thread','workspaces','prompt','request_id','confirm'},
        }
        optional = {'workspaces'} if op in ('configure', 'adopt') else set()
        if op not in fields or not fields[op] <= set(params) <= fields[op] | optional:
            raise CoordError('invalid root orchestrator operation or fields')
        roots = self.server.roots
        args = (self.project['id'], data['session'], data['host_id'])
        with self.server.root_lock, self.server.measurements.lock:
            if op in ('team-reset-preview','team-reset','team-reset-status'):
                from .root_team_reset import RootTeamReset
                reset = RootTeamReset(roots)
                if op == 'team-reset-preview':
                    return reset.preview(*args,params['project'],params['message_ids'])
                if op == 'team-reset-status':
                    return reset.status(*args,params['project'],params['request_id'])
                return reset.apply(*args,params['project'],params['message_ids'],params['request_id'],params['expected_digest'],params['confirm'])
            if op in ('fresh-start','fresh-status'):
                from .root_restart import RootRestart
                restart = RootRestart(roots)
                if op == 'fresh-status':
                    return restart.authorize_status(*args,params['request_id'])
                return restart.request(*args,params['expected_thread'],params['prompt'],params['request_id'],params['confirm'])
            if op in ('stop-all','resume-all','stop-status'):
                return roots.team_lifecycle(*args,op,params['request_id'],params.get('confirm',True))
            if op == 'enroll':
                if self.server.measurements.status(self.project['id'])['active']:
                    raise CoordError('finish the active project measurement before root enrollment')
                return roots.enroll(*args, params['expected_thread'], params['confirm'])
            roots.authorize(*args)
            if op == 'status':
                roots.sync()
                return roots.status()
            if op == 'route': return roots.route(*args, params['project'])
            if op == 'delegate-management':
                return roots.delegate_management(*args,params['project'],params['po'],params['target'],params['expected_thread'],params['expected_host'],params['request_id'],params['confirm'])
            if op == 'detach': return roots.detach(*args, params['confirm'])
            if op == 'resume-routing': return roots.resume_routing(*args, params['confirm'])
            if self.server.measurements.status(params['project'])['active']:
                raise CoordError('finish the active target project measurement before orchestration setup')
            if op == 'retry-setup': return roots.retry_setup(*args, params['project'], params['request_id'], params['confirm'])
            if op == 'reconfigure-workspaces':
                return roots.reconfigure_workspaces(*args, params['project'], params['expected_thread'], params['workspaces'], params['prompt'], params['request_id'], params['confirm'])
            if op == 'provision':
                return roots.provision(*args, params['project'], params['agent'], params['request_id'], params['confirm'], params['model'], params['effort'])
            if op == 'configure':
                return roots.configure(*args, params['project'], params['agent'], params['cwd'], params['prompt'], params['request_id'], params['confirm'], params['model'], params['effort'], workspaces=params.get('workspaces'))
            return roots.adopt(*args, params['project'], params['target'], params['expected_thread'], params['cwd'], params['confirm'], workspaces=params.get('workspaces'))

    def usage_action(self, data, machine):
        if not isinstance(data, dict) or not isinstance(data.get('op'), str):
            raise CoordError('invalid usage operation')
        op = data['op']
        if machine:
            if op == 'status' and set(data) == {'op', 'host_id'}:
                return self.server.usage.status(data['host_id'])
            fields = {'op', 'host_id', 'remaining_percent', 'resets_at', 'error', 'stopped_threads', 'enforcement_error'}
            if op == 'report' and set(data) <= fields and {'op', 'host_id', 'remaining_percent', 'resets_at'} <= set(data):
                return self.server.usage.report(data['host_id'], data['remaining_percent'], data['resets_at'],
                                                data.get('error'), data.get('stopped_threads', 0), data.get('enforcement_error'))
            if op == 'enforcement' and set(data) == {'op', 'host_id', 'stopped_threads', 'enforcement_error'}:
                return self.server.usage.enforcement(data['host_id'], data['stopped_threads'], data['enforcement_error'])
        else:
            if op == 'configure' and set(data) == {'op', 'enabled', 'threshold_percent'}:
                return self.server.usage.configure(data['enabled'], data['threshold_percent'])
            if op == 'resume' and set(data) == {'op'}:
                return self.server.usage.resume()
        raise CoordError('invalid usage operation')

    def events(self):
        # Validate paging before committing the streaming response headers.
        data = self.scoped(self.project_snapshot())
        self.send_response(200)
        self.send_header('Content-Type', 'text/event-stream')
        self.send_header('Cache-Control', 'no-store')
        self.send_header('X-Content-Type-Options', 'nosniff')
        self.end_headers()
        prior = None
        try:
            while not self.server.stop_event.is_set():
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
                data = self.scoped(self.project_snapshot())
        except (OSError, sqlite3.Error, RuntimeError):
            pass
        finally:
            self.close_connection = True


def create_server(db_path, port=8765, web_root=None, *, host='127.0.0.1', api_token=None, public_url=None, sessions_root=None,
                  trusted_proxies=(), inbox_diagnostics=None, logging_config=None):
    with reader(db_path):
        pass
    return WebServer((host, port), Handler, db_path, web_root or HERE / 'web', api_token, public_url, sessions_root,
                     trusted_proxies, inbox_diagnostics, logging_config)
