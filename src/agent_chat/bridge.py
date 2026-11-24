"""Wake loaded, idle Codex threads for incoming chat, without model polling."""
from __future__ import annotations

import contextlib
import fcntl
import json
import os
import sqlite3
import time
from pathlib import Path

from .core import CoordError
from .rpc import RpcError, TransportError


def _eligibility_connection(rpc):
    """Return a usable local connection identity, or omit telemetry."""
    get_identity = getattr(rpc, 'connection_identity', None)
    if not callable(get_identity):
        return None
    try:
        identity = get_identity()
    except TransportError:
        return None
    if not isinstance(identity, dict):
        return None
    instance, epoch = identity.get('client_instance'), identity.get('connection_epoch')
    if (not isinstance(instance, str) or not 1 <= len(instance) <= 256
            or type(epoch) is not int or not 0 < epoch < 2**63):
        return None
    return instance, epoch


def _eligibility_observation(thread_id, before, after, start_ns, end_ns):
    if (before is None or before != after
            or any(type(value) is not int or not 0 < value < 2**63 for value in (start_ns, end_ns))
            or start_ns > end_ns):
        return None
    return dict(thread_id=thread_id, connection_instance=before[0], connection_epoch=before[1],
                start_monotonic_ns=start_ns, end_monotonic_ns=end_ns)


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


class _PolicyHistoryRpc:
    """One optional page costs at most a list probe and its legacy readback."""
    def __init__(self,rpc,snapshot=None):
        self.rpc,self.snapshot=rpc,snapshot
        self.requests=0
        self.readback=None

    def request(self,method,params):
        fallback=method=='thread/read' and params.get('includeTurns') is True
        if fallback and self.snapshot is not None: return self.snapshot
        if self.requests>=2: raise CoordError('optional policy history request budget exhausted')
        self.requests+=1
        result=self.rpc.request(method,params)
        if fallback: self.readback=result
        return result



class BeforeSubmissionRejected(CoordError):
    """Coordinator proved admission was rejected before native submission."""


class _ExecutionEpochRpc:
    """Same dispatcher/connection; durable admission before any native send.

    An exception leaves an uncertain active handoff. Never infer rejection or
    replay a mutating RPC after a lost response. Reads and genuine stop remain.
    """
    MUTATING=frozenset(('thread/start','thread/resume','thread/fork','thread/settings/update','thread/name/set',
                       'thread/queue/add','thread/queue/start','turn/start','turn/steer'))
    def __init__(self,state,rpc): self.state,self.inner=state,rpc;self.context=None
    def __getattr__(self,name): return getattr(self.inner,name)
    def request(self,method,params,**kwargs):
        if method not in self.MUTATING: return self.inner.request(method,params,**kwargs)
        job=self.context or {}
        thread=params.get('threadId') or job.get('thread_id') or job.get('old_thread_id')
        actor=job.get('target_session') or job.get('session_id')
        values=dict(method=method,thread=thread,actor=actor,job_id=job.get('id'))
        if hasattr(self.state,'call'):
            result=self.state.call('execution-epoch-begin',**values)
            if result.get('rejected'):
                raise BeforeSubmissionRejected('native epoch rejected before submission: '+str(result['rejected']))
            identifier=result['id']
            settle=lambda outcome: self.state.call('execution-epoch-settle',id=identifier,outcome=outcome)
        elif hasattr(self.state,'coord'):
            from .core import execution_epoch_handoff,execution_epoch_settle
            coord=self.state.coord
            try:
                with coord.tx() as db:
                    from .supervisor import cold_epoch_check
                    cold_epoch_check(coord, db, method=method, thread=thread, actor=actor,
                                     host='local', wake_id=job.get('id'))
                    if hasattr(self.state, 'manager_update_epoch_check'):
                        self.state.manager_update_epoch_check(thread, job.get('id'), actor, method)
                    identifier=execution_epoch_handoff(db,method,thread,actor,'local')
            except CoordError as error:
                from .supervisor import reject_cold_epoch
                with coord.tx() as db:
                    reject_cold_epoch(db,method=method,thread=thread,actor=actor,host='local',
                                      wake_id=job.get('id'),reason=error)
                raise BeforeSubmissionRejected('native epoch rejected before submission: ' + str(error)) from error
            def settle(outcome):
                with coord.tx() as db: execution_epoch_settle(db,identifier,'local',outcome)
        else:
            raise CoordError('native epoch admission requires an actual coordinator or authenticated dispatcher')
        try:
            result=self.inner.request(method,params,**kwargs)
        except RpcError as error:
            if method == 'thread/start' and job.get('parked') and error.code == -32602:
                settle('rejected')
            raise
        settle('submitted')
        return result


class Bridge:
    def __init__(self, state, rpc):
        self.state = state
        self.rpc = _ExecutionEpochRpc(state,rpc)
        from .native_stage_observer import StageWindows
        self.native_stage_windows = StageWindows(self.rpc, state)
        self._history_pages = {}
        self._in_tick = False
        self._policy_scans = {}
        self._policy_last_thread = None
        self._policy_clock = time.monotonic
        from .capacity_recovery import CapacityWorker
        self.capacity_worker = CapacityWorker(self)

    def epoch_request(self, job, method, params, **kwargs):
        prior=self.rpc.context
        self.rpc.context=job
        try: return self.rpc.request(method,params,**kwargs)
        finally: self.rpc.context=prior

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

    def usage_error_queue_ready(self, thread_id, *, queued_id=None):
        """Qualify a new queue input after a stable terminal usage failure."""
        from .native_history import turns_page
        def ready():
            thread = self.rpc.request('thread/read', {'threadId': thread_id, 'includeTurns': False}).get('thread')
            return (isinstance(thread, dict) and thread.get('id') == thread_id
                    and isinstance(thread.get('status'), dict)
                    and thread['status'].get('type') == 'systemError'
                    and thread.get('canAcceptDirectInput') is True)
        def queue_matches():
            entries = self.queue(thread_id)
            return (not entries if queued_id is None else
                    len(entries) == 1 and entries[0].get('id') == queued_id)
        if not ready() or not queue_matches():
            return False
        params = {'threadId': thread_id, 'limit': 1, 'itemsView': 'notLoaded', 'sortDirection': 'desc'}
        latest = turns_page(self.rpc, params)['data']
        if len(latest) != 1:
            return False
        turn = latest[0]
        fields, error = turn.get('error_field_status', {}), turn.get('error')
        if (not turn.get('id') or turn.get('status') != 'failed'
                or fields.get('error') != 'supported'
                or any(value in ('unsupported', 'unknown') for value in fields.values())
                or not isinstance(error, dict) or error.get('codexErrorInfo') != 'usageLimitExceeded'):
            return False
        limits = self.rpc.request('account/rateLimits/read', {'excludeResetCreditDetails': True})
        return (isinstance(limits, dict) and limits.get('ordinaryUsageAllowed') is True
                and ready() and queue_matches() and turns_page(self.rpc, params)['data'] == latest)

    def ordinary_input_ready(self, thread_id, selection):
        """Explicit model input may follow a proven terminal provider failure.

        This is not generic wake eligibility or automatic capacity recovery.
        Native clients can still race the final read; no atomic no-steering claim.
        """
        current = self.state.control_job(selection['id'])
        if (current.get('action') != 'model' or current.get('model_route') != 'ordinary_input'
                or current.get('status') not in ('pending', 'waiting')
                or current.get('thread_id') != thread_id
                or current.get('native_input_job_id') != selection.get('native_input_job_id')):
            return False
        if self.idle(thread_id):
            return not self.queue(thread_id)
        from .native_history import turns_page
        from .capacity_recovery import capacity_error
        before = self.rpc.request('thread/read', {'threadId': thread_id, 'includeTurns': False}).get('thread')
        def ready(thread):
            return (isinstance(thread, dict) and thread.get('id') == thread_id
                    and thread.get('status', {}).get('type') == 'systemError'
                    and thread.get('canAcceptDirectInput') is True)
        if not ready(before) or self.queue(thread_id):
            return False
        params = {'threadId': thread_id, 'limit': 1, 'itemsView': 'notLoaded', 'sortDirection': 'desc'}
        latest = turns_page(self.rpc, params)['data']
        if len(latest) != 1:
            return False
        turn = latest[0]
        fields = turn.get('error_field_status', {})
        if (not turn.get('id') or turn.get('status') != 'failed'
                or fields.get('error') != 'supported'
                or any(value in ('unsupported', 'unknown') for value in fields.values())
                or not capacity_error(turn.get('error'))):
            return False
        after = self.rpc.request('thread/read', {'threadId': thread_id, 'includeTurns': False}).get('thread')
        # A second bounded latest-turn read rejects a changed/contradictory sample.
        return (ready(after) and not self.queue(thread_id)
                and turns_page(self.rpc, params)['data'] == latest)

    def history_page(self, params, *, rpc=None):
        key=(params['threadId'],params.get('cursor'))
        if key not in self._history_pages:
            from .native_history import turns_page
            page=turns_page(rpc or self.rpc,params)
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

    def manager_update_history_contains(self, job):
        """Only the claimed turn and stable client id prove an uncertain steer."""
        from .native_history import turns_page
        cursor = None
        for _ in range(4):
            params = {'threadId': job['thread_id'], 'limit': 25, 'itemsView': 'summary', 'sortDirection': 'desc'}
            if cursor:
                params['cursor'] = cursor
            page = turns_page(self.rpc, params)
            for turn in page['data']:
                if turn.get('id') == job['manager_update']['turn_id']:
                    return any(item.get('type') == 'userMessage' and item.get('clientId') == job['id']
                               for item in turn.get('items', []))
            cursor = page.get('nextCursor')
            if not cursor:
                break
        return False

    def manager_update_turn(self, thread_id, message_id, candidate):
        """Return the active native turn only when its inputs belong to this assignment."""
        from .native_history import turns_page

        original_job_id = candidate.get('original_job_id')
        if not isinstance(original_job_id, str) or not original_job_id:
            return None
        thread = self.rpc.request('thread/read', {'threadId': thread_id, 'includeTurns': False}).get('thread')
        if not isinstance(thread, dict) or thread.get('id') != thread_id:
            return None
        status = thread.get('status')
        if (not isinstance(status, dict) or status.get('type') != 'active'
                or status.get('activeFlags') != [] or thread.get('canAcceptDirectInput') is not True):
            return None
        if self.queue(thread_id):
            return None
        page = turns_page(self.rpc, {'threadId': thread_id, 'limit': 1, 'itemsView': 'summary', 'sortDirection': 'desc'})
        turns = page.get('data')
        if not isinstance(turns, list) or len(turns) != 1:
            return None
        turn = turns[0]
        if not isinstance(turn, dict) or turn.get('status') != 'inProgress' or not isinstance(turn.get('id'), str) or not turn['id']:
            return None
        items = turn.get('items')
        if not isinstance(items, list) or any(not isinstance(item, dict) for item in items):
            return None
        inputs = [item for item in items if item.get('type') == 'userMessage']
        if not inputs or inputs[0].get('clientId') != original_job_id:
            return None
        remaining = [item.get('clientId') for item in inputs[1:]]
        if remaining:
            if (any(not isinstance(client, str) or not client for client in remaining)
                    or len(set(remaining)) != len(remaining) or original_job_id in remaining):
                return None
            allowed = self.state.manager_update_prior(thread_id, message_id, candidate['task_id'],
                                                      original_job_id, turn['id'])
            if (not isinstance(allowed, list) or any(not isinstance(item, str) for item in allowed)
                    or any(client not in allowed for client in remaining)):
                return None
        return turn['id']

    def manager_update_submit(self, thread_id, messages):
        """Claim once before steer; any ambiguity is reconciled, never replayed."""
        message = messages[0]
        if message.get('manager_update_candidate') is not True:
            return False
        candidate = self.state.manager_update_candidate(thread_id, message['id'])
        if not candidate or candidate.get('recipient_session') != message['recipient_session']:
            return False
        turn_id = self.manager_update_turn(thread_id, message['id'], candidate)
        if not turn_id:
            return False
        payload = self.payload(thread_id, [message])
        job = self.state.manager_update_claim(thread_id, message['id'], candidate['task_id'],
                                              candidate['original_job_id'], turn_id, payload)
        if not job:
            return False
        try:
            self.state.update(job['id'], 'starting')
            result = self.epoch_request(job, 'turn/steer', {
                'threadId': thread_id, 'expectedTurnId': turn_id, 'clientUserMessageId': job['id'],
                'input': [{'type': 'text', 'text': payload, 'text_elements': []}],
            })
            if not isinstance(result, dict) or result.get('turnId') != turn_id:
                raise CoordError('steer response does not prove the claimed turn')
            self.state.update(job['id'], 'dispatched')
        except BeforeSubmissionRejected as error:
            self.state.update(job['id'], 'cancelled', error='Coordinator rejected before native submission; source retained for explicit reconciliation: ' + str(error))
        except (RpcError, CoordError, TransportError, OSError, ValueError, KeyError, TypeError) as error:
            self.state.update(job['id'], 'uncertain', error='Steer outcome unverified; no automatic replay: ' + str(error))
        return True

    def reconcile(self, job):
        if job.get('manager_update'):
            if self.manager_update_history_contains(job):
                self.state.update(job['id'], 'dispatched')
            else:
                self.state.update(job['id'], 'uncertain', error='No exact turn and client ID evidence; automatic replay withheld.')
            return
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

    def advance(self, job, *, fresh_usage=False):
        try:
            if job.get('manager_update'):
                self.reconcile(job)
                return
            if job['status'] in ('adding', 'starting', 'uncertain'):
                self.reconcile(job)
                job = self.state.job(job['id'])
            if job['status'] not in ('prepared', 'queued'):
                return
            if job.get('model_control'):
                self.advance_ordinary_input(job)
                return
            # Passive local observation only. No resume, subscribe or native input.
            self.native_stage_windows.arm(job)
            if job['status'] == 'prepared':
                fresh_usage = job.get('pristine') is True
                if not self.still_unread(job):
                    self.state.update(job['id'], 'cancelled')
                    return
                if not ((self.idle(job['thread_id']) and not self.queue(job['thread_id']))
                        or (fresh_usage and self.usage_error_queue_ready(job['thread_id']))):
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
            if (job.get('investigation_admission_request_id')
                    and (len(entries) != 1 or not job.get('queue_id')
                         or entries[0].get('id') != job['queue_id'])):
                # Preserve every native queue item. An admitted continuation
                # starts only while its own item is the sole queued entry.
                return
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
            if entries[0]['id'] != job['queue_id'] or not (
                    self.idle(job['thread_id']) or
                    (fresh_usage and self.usage_error_queue_ready(job['thread_id'], queued_id=job['queue_id']))):
                return
            self.state.update(job['id'], 'starting')
            # Unlike turn/start (which may steer), queue/start admits only an
            # idle thread atomically and retains input when the thread is busy.
            response = self.rpc.request('thread/queue/start', {'threadId': job['thread_id'], 'queuedSubmissionId': job['queue_id']})
            self.native_stage_windows.started(job['id'], response)
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
        finally:
            self.native_stage_windows.poll()

    def advance_ordinary_input(self, job):
        selection = job['model_control']
        if (job['status'] != 'prepared' or selection['status'] not in ('pending', 'waiting')
                or selection.get('model_route') != 'ordinary_input'
                or selection.get('native_input_job_id') != job['id']):
            raise CoordError('ordinary model input changed; submission withheld')
        if not self.still_unread(job):
            self.state.update(job['id'], 'cancelled')
            return
        if not self.ordinary_input_ready(job['thread_id'], selection):
            return  # Known busy/queued/ambiguous failure is never knowingly steered.
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

    def payload(self, thread_id, messages, *, investigation_admission=None):
        deliveries = []
        for message in messages:
            resolved = self.state.resolve(message['recipient_session'])
            deliveries.append({'message_id': message['id'], 'recipient_session': message['recipient_session'],
                               'route': [{'session': r['session_id'], 'agent_path': r['agent_path']} for r in resolved['route']]})
        from .communication import BOOT_AUTHORIZATION, STANDING_POLICY, COMPACT_WAKE, policy_version
        try: warm=hasattr(self.state,'policy_consumed') and self.state.policy_consumed(thread_id)
        except (CoordError,sqlite3.Error,OSError): warm=False
        # A parent's native receipt never proves an independently routed child consumed policy.
        if any(len(delivery['route'])>1 for delivery in deliveries): warm=False
        prompt=(COMPACT_WAKE if warm else STANDING_POLICY+' '+BOOT_AUTHORIZATION+' ')
        if investigation_admission is not None:
            prompt += (
                'The current PO-attested expected settings were compared before this exact original message was released. '
                'This finite same-task continuation applies only to that one existing message; it does not change settings, '
                'expand task permissions, admit other queued messages, or release the project stop. Follow only the original task scope. '
            )
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
        if investigation_admission is not None:
            metadata['investigation_admission'] = {
                'request_id': investigation_admission['request_id'],
                'stop_id': investigation_admission['stop_id'],
                'message_id': investigation_admission['message_id'],
                'purpose': investigation_admission['purpose'],
                'expires_at': investigation_admission['expires_at'],
                'workers_released': False,
                'expected_settings_source': 'po_attested_historical',
                'original_admission_expired': investigation_admission.get('original_admission_expired', True),
                'settings_match_required_before_message': True,
            }
            continuation = investigation_admission.get('continuation')
            if continuation:
                metadata['investigation_admission']['continuation_id'] = continuation['continuation_id']
                metadata['investigation_admission']['continuation_expires_at'] = continuation['expires_at']
                metadata['investigation_admission']['settings_preload_state'] = (
                    (investigation_admission.get('preload_event') or {}).get('state', 'not_started'))
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

    def observe_policies(self, *, offers=None, scans=None, receipt=None):
        # Optional proof collection follows ready work. One fair page per tick
        # prevents unloaded/stale threads from monopolizing native RPCs.
        if not self._in_tick: self._history_pages.clear()
        if not hasattr(self.state,'policy_pending'): return
        from .communication import policy_version
        groups={};version=policy_version()
        for offer in self.state.policy_pending() if offers is None else offers:
            if offer['version']==version:
                groups.setdefault(offer['thread'],{})[offer['client_id']]=offer
        if scans is None:
            self._policy_scans={thread:scan for thread,scan in self._policy_scans.items() if thread in groups and scan['version']==version}
            scans=self._policy_scans
        now=self._policy_clock();eligible=[]
        for thread,by_client in groups.items():
            scan=scans.setdefault(thread,dict(version=version,cursor=None,clients=set(by_client),retry_at=0,snapshot=None))
            if scan['version']!=version:
                scan=dict(version=version,cursor=None,clients=set(by_client),retry_at=0,snapshot=None)
                scans[thread]=scan
            # New input may restart an ended scan promptly, but never reset an
            # in-progress cohort and starve evidence beyond its newest pages.
            if scan['cursor'] is None and set(by_client)-scan['clients']:
                scan.update(clients=set(by_client),retry_at=0,snapshot=None)
            if scan['retry_at']<=now: eligible.append(thread)
        if not eligible: return
        ordered=sorted(eligible)
        thread=next((t for t in ordered if self._policy_last_thread is None or t>self._policy_last_thread),ordered[0])
        self._policy_last_thread=thread
        scan=scans[thread];by_client=groups[thread]
        params=dict(threadId=thread,limit=25,itemsView='summary',sortDirection='desc')
        if scan['cursor']: params['cursor']=scan['cursor']
        native=_PolicyHistoryRpc(self.rpc,scan['snapshot'])
        try:
            page=self.history_page(params,rpc=native)
            following=page.get('nextCursor')
            if following is not None and (not isinstance(following,str) or not following or following==scan['cursor']):
                raise CoordError('optional policy history cursor is invalid or stalled')
            if native.readback is not None:
                # turns_page already validated the complete legacy readback.
                # Retain only exact proof fields until this scan ends; paging
                # it must not refetch the full native transcript every tick.
                scan['snapshot']=dict(thread=dict(id=thread,turns=[dict(id=turn['id'],status=turn['status'],items=[
                    dict(type='userMessage',clientId=item['clientId']) for item in turn['items']
                    if item.get('type')=='userMessage' and isinstance(item.get('clientId'),str)])
                    for turn in native.readback['thread']['turns']]))
            for turn in page['data']:
                if not isinstance(turn.get('id'),str) or not turn['id']: continue
                for item in turn.get('items',[]):
                    client=item.get('clientId')
                    offer=by_client.get(client) if item.get('type')=='userMessage' and isinstance(client,str) and client in scan['clients'] else None
                    if offer:
                        (receipt or self.state.policy_receipt)(offer,turn['id'])
                        scans.pop(thread,None)
                        return thread
            scan['cursor']=following
            if not following:
                scan.update(retry_at=self._policy_clock()+30 if set(by_client)<=scan['clients'] else 0,clients=set(by_client),snapshot=None)
        except (RpcError,CoordError,sqlite3.Error,OSError) as error:
            scan['retry_at']=self._policy_clock()+30
            if isinstance(error,TransportError): raise  # Reconnect without replay.
            # Optional cache/history failures leave policy cold; ready work has
            # already run and other threads retain their next fair opportunity.
        return thread

    def tick(self, *, observe_policies=True):
        self.native_stage_windows.poll()
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
        if hasattr(self.state, 'root_fresh_jobs'):
            from .root_restart import RootRestartWorker
            for job in self.state.root_fresh_jobs():
                try: RootRestartWorker(self).advance(job)
                except (CoordError, sqlite3.Error, TransportError) as error: record(error)
        ordinary_selections = {}
        if hasattr(self.state, 'control_jobs'):
            from .control_worker import ControlWorker
            for control in self.state.control_jobs():
                if (control.get('action') == 'model' and control.get('model_route') == 'ordinary_input'
                        and control.get('status') in ('pending', 'waiting')
                        and control.get('native_input_job_id') is None):
                    ordinary_selections[control['thread_id']] = control
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
                except (CoordError, sqlite3.Error, TransportError, RpcError, OSError, ValueError, KeyError, TypeError) as error: record(error)
        if hasattr(self.state, 'capacity_targets'):
            try: self.capacity_worker.tick(enabled=os.environ.get('AGENT_CHAT_CAPACITY_RECOVERY', '1') != '0')
            except (CoordError, sqlite3.Error, TransportError) as error: record(error)
        jobs = self.state.jobs()
        def pending_groups(blocked):
            groups, routes = {}, {}
            for message in self.state.pending():
                recipient = message['recipient_session']
                try:
                    route = routes.get(recipient)
                    if route is None:
                        route = self.state.resolve(recipient)
                        if route is not None:
                            routes[recipient] = route
                except (CoordError, sqlite3.Error) as error:
                    record(error)
                    continue
                if route and route['thread_id'] not in blocked:
                    groups.setdefault(route['thread_id'], []).append(message)
            return groups
        def advance_group(thread_id, messages):
            # Ordinary messages use the idle queue. A task-linked PO update
            # may also qualify for the worker’s existing turn.
            try:
                selection = ordinary_selections.get(thread_id)
                fresh_usage = False
                eligibility_observation = None
                if selection:
                    if not self.ordinary_input_ready(thread_id, selection):
                        return False
                else:
                    # Passive metadata only; missing identity must not affect admission.
                    identity = _eligibility_connection(self.rpc)
                    start_ns = time.monotonic_ns()
                    ordinary_ready = self.idle(thread_id) and not self.queue(thread_id)
                    end_ns = time.monotonic_ns()
                    if ordinary_ready:
                        eligibility_observation = _eligibility_observation(
                            thread_id, identity, _eligibility_connection(self.rpc), start_ns, end_ns)
                    else:
                        if (messages and messages[0].get('manager_update_candidate') is True
                                and self.manager_update_submit(thread_id, messages)):
                            return True
                        if not self.usage_error_queue_ready(thread_id):
                            return False
                        fresh_usage = True
                messages = messages[:20]
                while True:
                    try:
                        payload = self.payload(thread_id, messages)
                        break
                    except WakePayloadTooLarge:
                        if len(messages) == 1:
                            raise
                        messages = messages[:max(1, len(messages) // 2)]
                if eligibility_observation is None:
                    job = self.state.prepare(thread_id, messages, payload)
                else:
                    job = self.state.prepare(thread_id, messages, payload,
                        eligibility_observation=eligibility_observation)
                if job:
                    self.advance(job, fresh_usage=fresh_usage)
                    return True
                return False
            except RpcError as error:
                self.state.observe(thread_id, 'error', str(error))
                return False
            except (CoordError, sqlite3.Error, TransportError) as error:
                record(error, thread_id)
                return False
        early_attempted = set()
        if jobs:
            early_groups = pending_groups({job['thread_id'] for job in jobs})
            priority_thread = next(iter(early_groups), None)
            if priority_thread is not None and advance_group(priority_thread, early_groups[priority_thread]):
                early_attempted.add(priority_thread)
        for job in jobs:
            try: self.advance(job)
            except (CoordError, sqlite3.Error, TransportError) as error: record(error, job['thread_id'])
        # A Root-authorized stopped-worker investigation is always its own
        # exact one-message job. It never joins the ordinary per-thread batch.
        if hasattr(self.state, 'investigation_tasks'):
            blocked_task_threads = {job['thread_id'] for job in self.state.jobs()}
            for item in self.state.investigation_tasks():
                admission, message = item['admission'], item['message']
                thread_id = admission['thread_id']
                if thread_id in blocked_task_threads:
                    continue
                try:
                    if not self.idle(thread_id) or self.queue(thread_id):
                        continue
                    payload = self.payload(thread_id, [message], investigation_admission=admission)
                    job = self.state.prepare(thread_id, [message], payload,
                        investigation_request_id=admission['request_id'])
                    if job:
                        self.advance(job)
                        blocked_task_threads.add(thread_id)
                except RpcError as error:
                    self.state.observe(thread_id, 'error', str(error))
                except (CoordError, sqlite3.Error, TransportError) as error:
                    record(error, thread_id)
        blocked = {job['thread_id'] for job in self.state.jobs()}
        for thread_id, messages in pending_groups(blocked).items():
            if thread_id not in early_attempted:
                advance_group(thread_id, messages)
        if observe_policies:
            try: self.observe_policies()
            except (RpcError,CoordError,sqlite3.Error,OSError,TransportError) as error: record(error)
        # Report the error after giving independent conversations a
        # chance to dispatch. Never clear or retry ambiguous durable intentions.
        self._in_tick=False
        if errors: raise errors[0]
