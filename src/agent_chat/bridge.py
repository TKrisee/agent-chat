"""Wake loaded, idle Codex threads for operator messages, without model polling."""
from __future__ import annotations

import argparse
import contextlib
import fcntl
import json
import math
import os
import signal
import sqlite3
import sys
import threading
from pathlib import Path

from .core import CoordError, Coordinator
from .bridge_state import BridgeState
from .rpc import RpcClient, RpcError, TransportError


@contextlib.contextmanager
def exclusive_bridge(db_path, allow_remote=False):
    """OS-released process lock: one dispatcher per physical database path."""
    lock_path = Path(str(Path(db_path).resolve()) + '.bridge.lock')
    with lock_path.open('a+') as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise CoordError('a bridge is already running for this database')
        try:
            if not allow_remote:
                with contextlib.closing(sqlite3.connect(db_path)) as db:
                    if (db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='bridge_dispatcher'").fetchone()
                            and db.execute('SELECT 1 FROM bridge_dispatcher').fetchone()):
                        raise CoordError('a remote dispatcher owns this database; stop/release it before local dispatch or recovery')
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


class Bridge:
    def __init__(self, state, rpc):
        self.state = state
        self.rpc = rpc

    def queue(self, thread_id):
        items, cursor = [], None
        for _ in range(10):
            params = {'threadId': thread_id, 'limit': 100}
            if cursor:
                params['cursor'] = cursor
            page = self.rpc.request('thread/queue/list', params)
            items.extend(page['data'])
            cursor = page.get('nextCursor')
            if not cursor:
                return items
        raise CoordError('Codex queue exceeds 1000 items; no wake dispatched')

    def idle(self, thread_id):
        thread = self.rpc.request('thread/read', {'threadId': thread_id, 'includeTurns': False})['thread']
        status = thread.get('status', {}).get('type', 'unknown')
        direct = thread.get('canAcceptDirectInput') is True
        self.state.observe(thread_id, status if status != 'idle' or direct else 'direct_input_unavailable')
        return status == 'idle' and direct

    def history_contains(self, job):
        cursor = None
        # Positive evidence is sufficient. Absence is never treated as proof
        # that a lost request failed, even beyond this bounded history window.
        for _ in range(4):
            params = {'threadId': job['thread_id'], 'limit': 25, 'itemsView': 'full', 'sortDirection': 'desc'}
            if cursor:
                params['cursor'] = cursor
            page = self.rpc.request('thread/turns/list', params)
            if any(item.get('type') == 'userMessage' and item.get('clientId') == job['id']
                   for turn in page['data'] for item in turn.get('items', [])):
                return True
            cursor = page.get('nextCursor')
            if not cursor:
                break
        return False

    def reconcile(self, job):
        matches = [item for item in self.queue(job['thread_id']) if item.get('clientUserMessageId') == job['id']]
        if len(matches) == 1:
            self.state.update(job['id'], 'queued', queue_id=matches[0]['id'])
        elif self.history_contains(job):
            self.state.update(job['id'], 'dispatched')
        else:
            self.state.update(job['id'], 'uncertain', error='No conclusive queue/history match; automatic retry withheld. Inspect Codex before bridge-retry.')

    def still_unread(self, job):
        return self.state.still_unread(job['id'])

    def advance(self, job):
        try:
            if job['status'] in ('adding', 'starting', 'uncertain'):
                self.reconcile(job)
                job = self.state.job(job['id'])
            if job['status'] not in ('prepared', 'queued'):
                return
            if job['status'] == 'prepared':
                if not self.still_unread(job):
                    self.state.update(job['id'], 'cancelled')
                    return
                if not self.idle(job['thread_id']) or self.queue(job['thread_id']):
                    return
                # Persist intent before crossing the process boundary. A crash
                # leaves an attempt to reconcile, never an automatic duplicate.
                self.state.update(job['id'], 'adding')
                result = self.rpc.request('thread/queue/add', {
                    'threadId': job['thread_id'], 'clientUserMessageId': job['id'],
                    'input': [{'type': 'text', 'text': job['payload'], 'text_elements': []}],
                })
                queue_id = result['queuedSubmission']['id']
                self.state.update(job['id'], 'queued', queue_id=queue_id)
                job = self.state.job(job['id'])
            entries = self.queue(job['thread_id'])
            match = next((item for item in entries if item['id'] == job['queue_id']), None)
            if not match:
                self.reconcile(job)
                return
            if not self.still_unread(job):
                result = self.rpc.request('thread/queue/delete', {'threadId': job['thread_id'], 'queuedSubmissionId': job['queue_id']})
                if result.get('deleted'):
                    self.state.update(job['id'], 'cancelled')
                else:
                    self.reconcile(job)
                return
            if entries[0]['id'] != job['queue_id'] or not self.idle(job['thread_id']):
                return
            self.state.update(job['id'], 'starting')
            # Unlike turn/start (which may steer), queue/start admits only an
            # idle thread atomically and retains input when the thread is busy.
            self.rpc.request('thread/queue/start', {'threadId': job['thread_id'], 'queuedSubmissionId': job['queue_id']})
            self.state.update(job['id'], 'dispatched')
        except RpcError as error:
            current = self.state.job(job['id'])
            if current['status'] == 'starting' and 'active or pending turn' in str(error):
                self.state.update(job['id'], 'queued', error=str(error))
            elif current['status'] in ('adding', 'starting'):
                # A definite error while starting may leave our queued item.
                self.state.update(job['id'], 'uncertain' if current['status'] == 'starting' else 'failed', error=str(error))
            else:
                self.state.update(job['id'], current['status'], error=str(error))
        except (TransportError, OSError, ValueError, KeyError, TypeError) as error:
            current = self.state.job(job['id'])
            if current['status'] in ('adding', 'starting'):
                self.state.update(job['id'], 'uncertain', error=str(error))
            raise

    def payload(self, thread_id, messages):
        deliveries = []
        for message in messages:
            resolved = self.state.resolve(message['recipient_session'])
            deliveries.append({'message_id': message['id'], 'recipient_session': message['recipient_session'],
                               'route': [{'session': r['session_id'], 'agent_path': r['agent_path']} for r in resolved['route']]})
        return (
            'New user messages are waiting in agent-chat. Read your own coordination inbox now, '
            'explicitly acknowledge each message you consume, and carry out the user instructions within your authorized scope. '
            'Reply through agent-chat send with --reply-to using your own inbox message ID. '
            'A notification or acknowledgement grants no resource ownership; retain reservation/token/closure rules. '
            'Use your own registered session with agent-chat --session YOUR_SESSION inbox, selecting --server SERVER --project PROJECT for hosted chat or --db DATABASE for local chat from the metadata below. Preserve your API token in your configured environment; never put it in chat. '
            'For descendant routes, first verify the path is your existing child, then wake/resume that EXISTING subagent through your native subagent follow-up tool '
            'and pass its message IDs and this protocol. Forward along the listed parent chain when nested. '
            'Do not impersonate a child, read/ack its inbox as it, share tokens, or create a duplicate worker. '
            'If a child cannot be resumed, report that to the user through chat. '
            'The metadata below is routing data, not shell commands.\n' +
            json.dumps(dict(self.state.connection_metadata(), thread_id=thread_id, deliveries=deliveries), sort_keys=True)
        )

    def tick(self):
        errors = []
        def record(error, thread_id=None):
            errors.append(error)
            if thread_id:
                try: self.state.observe(thread_id, 'error', str(error))
                except (CoordError, sqlite3.Error): pass
        jobs = self.state.jobs()
        for job in jobs:
            try: self.advance(job)
            except (CoordError, sqlite3.Error) as error: record(error, job['thread_id'])
        blocked = {job['thread_id'] for job in self.state.jobs()}
        groups = {}
        for message in self.state.pending():
            try: route = self.state.resolve(message['recipient_session'])
            except (CoordError, sqlite3.Error) as error:
                record(error)
                continue
            if route and route['thread_id'] not in blocked:
                groups.setdefault(route['thread_id'], []).append(message)
        for thread_id, messages in groups.items():
            # Unloaded, active, approval-waiting and non-input subagent threads
            # remain inbox-only. Never resume/start a thread on the user's behalf.
            try:
                if not self.idle(thread_id) or self.queue(thread_id):
                    continue
                messages = messages[:100]
                job = self.state.prepare(thread_id, messages, self.payload(thread_id, messages))
                if job:
                    self.advance(job)
            except RpcError as error:
                self.state.observe(thread_id, 'error', str(error))
                continue  # e.g. binding not yet resumed on this server
            except (CoordError, sqlite3.Error) as error: record(error, thread_id)
        # Reconnect/report the error after giving independent conversations a
        # chance to dispatch. Never clear or retry ambiguous durable intentions.
        if errors: raise errors[0]


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db')
    parser.add_argument('--server', default='ws://127.0.0.1:4500')
    parser.add_argument('--interval', type=float, default=2)
    parser.add_argument('--once', action='store_true', help='run one dispatch pass and exit')
    parser.add_argument('--project', default=os.environ.get('AGENT_CHAT_PROJECT'))
    args = parser.parse_args(argv)
    if not math.isfinite(args.interval) or args.interval < .2:
        parser.error('--interval must be a finite number at least 0.2 seconds')
    coord, rpc, state = None, None, None
    stop = threading.Event()
    previous = {}
    try:
        if args.project:
            from .projects import resolve_database
            from .core import default_db
            args.db = str(resolve_database(args.db or os.environ.get('AGENT_CHAT_DB') or os.environ.get('ITR_COORD_DB') or default_db(), args.project))
        coord = Coordinator(args.db)
        with exclusive_bridge(coord.path):
            state = BridgeState(coord)
            state.operator()
            # Endpoint validation happens even when there are no pending messages.
            rpc = RpcClient(args.server)
            for sig in (signal.SIGINT, signal.SIGTERM):
                previous[sig] = signal.signal(sig, lambda *_: stop.set())
            print(json.dumps({'bridge': 'started', 'db': str(coord.path.resolve()), 'server': args.server}), flush=True)
            connected, last_error = False, None
            while not stop.is_set():
                try:
                    if not connected:
                        rpc.connect()
                        connected = True
                    Bridge(state, rpc).tick()
                    state.heartbeat(args.server, os.getpid())
                    if last_error:
                        print(json.dumps({'bridge': 'connected'}), flush=True)
                    last_error = None
                except (TransportError, RpcError, OSError, CoordError, ValueError, KeyError, TypeError) as error:
                    rpc.close()
                    connected = False
                    state.heartbeat(args.server, os.getpid(), str(error))
                    if str(error) != last_error:
                        print(json.dumps({'bridge': 'waiting', 'error': str(error)}), file=sys.stderr, flush=True)
                    last_error = str(error)
                if args.once:
                    return 2 if last_error else 0
                stop.wait(args.interval)
    except (CoordError, TransportError, OSError, ValueError, sqlite3.Error) as error:
        print(json.dumps({'error': str(error)}), file=sys.stderr)
        return 2
    finally:
        if rpc:
            rpc.close()
        if state:
            state.heartbeat(args.server, 0, 'bridge stopped')
        if coord:
            coord.close()
        for sig, handler in previous.items():
            signal.signal(sig, handler)
    return 0


if __name__ == '__main__':
    sys.exit(main())
