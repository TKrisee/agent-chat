"""Wake loaded, idle Codex threads for incoming chat, without model polling."""
from __future__ import annotations

import contextlib
import fcntl
import json
import os
import sqlite3
from pathlib import Path

from .core import CoordError
from .rpc import RpcError, TransportError


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


class WakePayloadTooLarge(CoordError):
    pass


class Bridge:
    def __init__(self, state, rpc):
        self.state = state
        self.rpc = rpc
        self._history_pages = {}
        self._in_tick = False
        from .capacity_recovery import CapacityWorker
        self.capacity_worker = CapacityWorker(self)

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

    def history_page(self, params):
        key=(params['threadId'],params.get('cursor'))
        if key not in self._history_pages:
            from .native_history import turns_page
            page=turns_page(self.rpc,params)
            if not isinstance(page,dict) or not isinstance(page.get('data'),list):
                raise CoordError('native history page unavailable for policy/reconciliation')
            if any(not isinstance(turn,dict) or not isinstance(turn.get('items',[]),list)
                   or any(not isinstance(item,dict) for item in turn.get('items',[])) for turn in page['data']):
                raise CoordError('malformed native history turns/items; evidence withheld')
            self._history_pages[key]=page
        return self._history_pages[key]

    def history_contains(self, job):
        if not self._in_tick: self._history_pages.clear()
        cursor = None
        # Positive evidence is sufficient. Absence is never treated as proof
        # that a lost request failed, even beyond this bounded history window.
        # Summaries retain user-message client IDs without full tool outputs,
        # which can exceed the WebSocket limit even for a short conversation.
        for _ in range(4):
            params = {'threadId': job['thread_id'], 'limit': 25, 'itemsView': 'summary', 'sortDirection': 'desc'}
            if cursor:
                params['cursor'] = cursor
            page = self.history_page(params)
            if any(item.get('type') == 'userMessage' and item.get('clientId') == job['id']
                   for turn in page['data'] for item in turn.get('items', [])):
                return True
            cursor = page.get('nextCursor')
            if not cursor:
                break
        return False

    def reconcile(self, job):
        selection = job.get('model_control')
        if selection:
            if self.history_contains(job):
                if selection['status'] in ('submitting', 'uncertain'):
                    self.state.control_update(selection['id'], selection['status'], 'completed',
                        input_job_id=job['id'], input_evidence='history_client_id',
                        error='Original genuine input observed in history; ACK/start-or-steer outcome and effective native defaults remain unverified.')
                self.state.update(job['id'], 'dispatched')
            else:
                self.state.update(job['id'], 'uncertain',
                    error='No positive original input correlation; model/input automatic replay withheld.')
            return
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
            if job.get('model_control'):
                self.advance_ordinary_input(job)
                return
            if job['status'] == 'prepared':
                if not self.still_unread(job):
                    self.state.update(job['id'], 'cancelled')
                    return
                if not self.idle(job['thread_id']) or self.queue(job['thread_id']):
                    return
                # Persist intent before crossing the process boundary. A crash
                # leaves an attempt to reconcile, never an automatic duplicate.
                self.offer_policy('wake',job['id'],job['thread_id'],job['payload'])
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
            if current.get('model_control') and current['status'] == 'starting':
                self.state.update(job['id'], 'failed', error='Ordinary native turn explicitly refused; input and selection retained: ' + str(error))
            elif current['status'] == 'starting' and 'active or pending turn' in str(error):
                self.state.update(job['id'], 'queued', error=str(error))
            elif current['status'] in ('adding', 'starting'):
                # A definite error while starting may leave our queued item.
                self.state.update(job['id'], 'uncertain' if current['status'] == 'starting' else 'failed', error=str(error))
            else:
                self.state.update(job['id'], current['status'], error=str(error))
        except (TransportError, OSError, ValueError, KeyError, TypeError, CoordError) as error:
            current = self.state.job(job['id'])
            if current['status'] in ('adding', 'starting'):
                self.state.update(job['id'], 'uncertain', error=str(error))
            raise

    def advance_ordinary_input(self, job):
        selection = job['model_control']
        if (job['status'] != 'prepared' or selection['status'] not in ('pending', 'waiting')
                or selection.get('model_route') != 'ordinary_input'
                or selection.get('native_input_job_id') != job['id']):
            raise CoordError('ordinary model input changed; submission withheld')
        if not self.still_unread(job):
            self.state.update(job['id'], 'cancelled')
            return
        if not self.idle(job['thread_id']) or self.queue(job['thread_id']):
            return  # Our known busy/queued thread is never steered.
        self.offer_policy('wake', job['id'], job['thread_id'], job['payload'])
        self.state.update(job['id'], 'starting')
        # The durable original wake UUID correlates the genuine input. Native
        # clients can race this observation: ordinary start may start or steer,
        # reject or interrupt. An ACK is not a defaults/readback guarantee.
        result = self.rpc.request('turn/start', {
            'threadId': job['thread_id'], 'clientUserMessageId': job['id'],
            'input': [{'type': 'text', 'text': job['payload'], 'text_elements': []}],
            'model': selection['requested_model'], 'effort': selection['requested_effort'],
        })
        turn = result['turn']
        current = self.state.control_job(selection['id'])
        self.state.control_update(selection['id'], current['status'], 'completed',
            input_job_id=job['id'], input_evidence='rpc_ack', native_response=result, turn_id=turn['id'],
            error='Ordinary native turn accepted; effective thread defaults and external start-or-steer/interruption are not verified.')
        self.state.update(job['id'], 'dispatched')

    def payload(self, thread_id, messages):
        deliveries = []
        for message in messages:
            resolved = self.state.resolve(message['recipient_session'])
            deliveries.append({'message_id': message['id'], 'recipient_session': message['recipient_session'],
                               'route': [{'session': r['session_id'], 'agent_path': r['agent_path']} for r in resolved['route']]})
        from .communication import STANDING_POLICY, COMPACT_WAKE, policy_version
        try: warm=hasattr(self.state,'policy_consumed') and self.state.policy_consumed(thread_id)
        except (CoordError,sqlite3.Error,OSError): warm=False
        # A parent's native receipt never proves an independently routed child consumed policy.
        if any(len(delivery['route'])>1 for delivery in deliveries): warm=False
        prompt=(COMPACT_WAKE if warm else STANDING_POLICY+' ')
        if any(len(delivery['route']) > 1 for delivery in deliveries):
            prompt += (
                'For child routes, verify the existing child and resume it through native follow-up along the listed parent chain. '
                'Pass its message IDs, routing metadata and these instructions. '
                'Never impersonate a child, read/ack its inbox, share tokens or create a replacement. '
                'Report unavailable children in chat. '
            )
        prompt += '\n'
        metadata = dict(self.state.connection_metadata(), thread_id=thread_id, deliveries=deliveries,
                        policy_version=policy_version(),policy_included=not warm)
        contents = self.state.wake_messages(thread_id, [m['id'] for m in messages])
        if contents:
            metadata['messages'] = [{'id': item['id'], 'complete': False} for item in contents]
        def serialized():
            return prompt + json.dumps(metadata, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
        if len(serialized().encode('utf-8')) > 12288:
            raise WakePayloadTooLarge('wake routing metadata exceeds 12 KiB; inspect the bound route')
        for index, item in enumerate(contents):
            metadata['messages'][index] = item
            if len(serialized().encode('utf-8')) > 12288:
                metadata['messages'][index] = {'id': item['id'], 'complete': False}
        return serialized()

    def offer_policy(self, kind, client_id, thread, payload):
        if not hasattr(self.state,'policy_offer'): return
        try: self.state.policy_offer(kind,client_id,thread,payload)
        except (CoordError,sqlite3.Error,OSError):
            pass  # Optional cache failure remains cold; never replay native input.

    def observe_policies(self):
        self._history_pages.clear()
        if not hasattr(self.state,'policy_pending'): return
        offers=self.state.policy_pending()
        for thread in {offer['thread'] for offer in offers}:
            by_client={offer['client_id']:offer for offer in offers if offer['thread']==thread}
            cursor=None
            for _ in range(4):
                params=dict(threadId=thread,limit=25,itemsView='summary',sortDirection='desc')
                if cursor: params['cursor']=cursor
                try: page=self.history_page(params)
                except (RpcError,CoordError): break  # Unavailable evidence keeps policy cold.
                for turn in page['data']:
                    if not isinstance(turn.get('id'),str) or not turn['id']: continue
                    for item in turn.get('items',[]):
                        offer=by_client.get(item.get('clientId')) if item.get('type')=='userMessage' and isinstance(item.get('clientId'),str) else None
                        if offer: self.state.policy_receipt(offer,turn['id'])
                cursor=page.get('nextCursor')
                if not cursor: break

    def tick(self):
        self._in_tick=True
        self._history_pages.clear()
        errors = []
        def record(error, thread_id=None):
            errors.append(error)
            if thread_id:
                try: self.state.observe(thread_id, 'error', str(error))
                except (CoordError, sqlite3.Error): pass
            if isinstance(error, TransportError):
                # A rejected frame may leave unread payload on the socket.
                # Reconnect for independent jobs, never replay the failed RPC.
                # If Codex is unavailable, let reconnect fail into poll backoff.
                self.rpc.close()
                self.rpc.connect()
        try: self.observe_policies()
        except (CoordError,sqlite3.Error,TransportError) as error: record(error)
        if hasattr(self.state, 'root_fresh_jobs'):
            from .root_restart import RootRestartWorker
            for job in self.state.root_fresh_jobs():
                try: RootRestartWorker(self).advance(job)
                except (CoordError, sqlite3.Error, TransportError) as error: record(error)
        if hasattr(self.state, 'control_jobs'):
            from .control_worker import ControlWorker
            for control in self.state.control_jobs():
                try: ControlWorker(self).advance(control)
                except (CoordError, sqlite3.Error, TransportError) as error: record(error)
        if hasattr(self.state, 'reset_jobs'):
            from .reset_worker import ResetWorker
            for reset in self.state.reset_jobs():
                try: ResetWorker(self).advance(reset)
                except (CoordError, sqlite3.Error, TransportError) as error: record(error)
        if hasattr(self.state, 'supervisor_jobs'):
            from .supervisor_worker import SupervisorWorker
            for supervisor in self.state.supervisor_jobs():
                try: SupervisorWorker(self).advance(supervisor)
                except (CoordError, sqlite3.Error, TransportError) as error: record(error)
        if hasattr(self.state, 'capacity_targets'):
            try: self.capacity_worker.tick(enabled=os.environ.get('AGENT_CHAT_CAPACITY_RECOVERY', '1') != '0')
            except (CoordError, sqlite3.Error, TransportError) as error: record(error)
        jobs = self.state.jobs()
        for job in jobs:
            try: self.advance(job)
            except (CoordError, sqlite3.Error, TransportError) as error: record(error, job['thread_id'])
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
                messages = messages[:20]
                while True:
                    try:
                        payload = self.payload(thread_id, messages)
                        break
                    except WakePayloadTooLarge:
                        if len(messages) == 1:
                            raise
                        messages = messages[:max(1, len(messages) // 2)]
                job = self.state.prepare(thread_id, messages, payload)
                if job:
                    self.advance(job)
            except RpcError as error:
                self.state.observe(thread_id, 'error', str(error))
                continue  # e.g. binding not yet resumed on this server
            except (CoordError, sqlite3.Error, TransportError) as error: record(error, thread_id)
        # Report the error after giving independent conversations a
        # chance to dispatch. Never clear or retry ambiguous durable intentions.
        self._in_tick=False
        if errors: raise errors[0]
