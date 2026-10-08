"""Passive sampling plus one durable incident-authorized exact-thread recovery."""
from .rpc import RpcError, TransportError
from .core import CoordError
from .native_history import HistoryUnavailable
import time


class SupervisorWorker:
    def __init__(self, bridge):
        self.bridge, self.state, self.rpc = bridge, bridge.state, bridge.rpc


    def _snapshot(self, job):
        started_at = time.time()
        before = self.rpc.request('thread/read', {'threadId': job['thread_id'], 'includeTurns': False})['thread']
        before_at = time.time()
        queue = self.bridge.queue(job['thread_id'])
        queue_at = time.time()
        history_state, history_error = 'available', None
        try:
            from .native_history import turns_page
            turns = turns_page(self.rpc, {
                'threadId': job['thread_id'], 'limit': 1, 'itemsView': 'summary', 'sortDirection': 'desc'})['data']
        except (RpcError, HistoryUnavailable) as error:
            if isinstance(error, RpcError) and error.code != -32601:
                raise
            # An unsupported history RPC is not an empty thread and cannot
            # authorize recovery, replay, or consumption completion.
            turns, history_state = [], 'unavailable'
            history_error = {'code': error.code if isinstance(error, RpcError) else -32601,
                             'message': (error.message if isinstance(error, RpcError) else str(error))[:256]}
        turns_at = time.time()
        after = self.rpc.request('thread/read', {'threadId': job['thread_id'], 'includeTurns': False})['thread']
        after_at = time.time()
        before_status, after_status = before.get('status', {}).get('type'), after.get('status', {}).get('type')
        contradictory = (before.get('id') != job['thread_id'] or after.get('id') != job['thread_id']
                         or before.get('status') != after.get('status')
                         or before.get('canAcceptDirectInput') != after.get('canAcceptDirectInput'))
        return dict(
            thread_id=job['thread_id'], queue_count=len(queue),
            turn_count=len(turns) if history_state == 'available' else None,
            history_state=history_state, history_error=history_error,
            before=before_status, after=after_status, contradictory=contradictory,
            admission=job['admission'], wake=job['wake_summary'],
            holds=job['holds'], open_guards=job['open_guards'],
            sample_started_at=started_at, read_before_at=before_at, queue_read_at=queue_at,
            turn_read_at=turns_at, read_after_at=after_at,
            turn_id=turns[0].get('id') if turns and isinstance(turns[0], dict) else None,
            turn_status=turns[0].get('status') if turns and isinstance(turns[0], dict) else None,
            direct_input=after.get('canAcceptDirectInput'))

    def _report(self, job, state, summary):
        return self.state.supervisor_observe(job, dict(state=state, summary=summary, source='native'))

    def _recover(self, job, summary):
        claim = self._report(job, 'recovery_claim', summary)
        if not claim.get('claimed'):
            return
        recovery = claim['recovery']
        wake_id = recovery['wake_id']
        try:
            if wake_id is None:
                messages = recovery['messages']
                # Preparation uses the existing ordinary inbox journal and only
                # the pre-existing messages pinned by the atomic incident claim.
                wake = self.state.prepare(job['thread_id'], messages,
                                          self.bridge.payload(job['thread_id'], messages))
                if not wake:
                    return  # Durable claim remains incomplete; no replay.
                wake_id = wake['id']
            fresh = dict(self._snapshot(job), recovery_id=recovery['id'], wake_id=wake_id)
            admitted = self._report(job, 'recovery_attempt', fresh)
            if not admitted.get('resume_allowed'):
                return
            result = self.rpc.request('thread/resume', {'threadId': job['thread_id'], 'excludeTurns': True})
            if not isinstance(result, dict) or result.get('thread', {}).get('id') != job['thread_id']:
                raise ValueError('exact recovery thread identity missing')
            loaded = dict(self._snapshot(job), recovery_id=recovery['id'], wake_id=wake_id)
            self._report(job, 'recovery_ready', loaded)
            # Resume/load is only readiness. Ordinary Bridge.advance owns the
            # eventual queue/start; completion awaits the exact native client ID.
        except (RpcError, TransportError, CoordError, OSError, ValueError, KeyError, TypeError):
            if wake_id is not None:
                self._report(job, 'recovery_uncertain', {
                    'recovery_id': recovery['id'], 'wake_id': wake_id})
            raise

    def _consumption(self, job, summary):
        recovery = job.get('recovery')
        if (not recovery or recovery['status'] not in ('attempting', 'uncertain', 'awaiting_consumption')
                or not recovery['wake_id'] or summary['contradictory'] or job['admission'] != 'running'):
            return
        wake = self.state.job(recovery['wake_id'])
        if summary.get('history_state') != 'available':
            return
        try:
            consumed = self.bridge.history_contains(wake)
        except RpcError as error:
            if error.code != -32601:
                raise
            self._report(job, 'history_unavailable', dict(summary,
                history_state='unavailable',
                history_error={'code': error.code, 'message': error.message[:256]}))
            return
        if wake['thread_id'] == job['thread_id'] and consumed:
            self._report(job, 'recovery_consumed', {
                'recovery_id': recovery['id'], 'wake_id': wake['id'],
                'consumed_client_id': wake['id'], 'observed_thread_id': job['thread_id'],
                'proof': 'native-history'})

    def advance(self, job):
        """Healthy/default-disabled supervision stays passive; recovery is incident-linked."""
        try:
            summary = self._snapshot(job)
            after_status, contradictory = summary['after'], summary['contradictory']
            if job['admission'] != 'running':
                state = 'intentional_gated'
            elif contradictory:
                state = 'inconclusive'
            elif summary.get('history_state') != 'available':
                state = 'history_unavailable'
            elif job['wake_summary']['uncertain_wake']:
                state = 'uncertain_wake'
            elif summary['queue_count']:
                state = 'queued'
            elif after_status in ('unloaded', 'notLoaded', None) and (job['wake_summary']['pending_wakes'] or job['wake_summary']['pending_unread_messages']):
                state = 'unloaded_pending'
            elif after_status:
                state = after_status
            else:
                state = 'inconclusive'
            result = self._report(job, state, summary)
            if result.get('recovery_candidate'):
                self._recover(job, summary)
            else:
                self._consumption(job, summary)
        except (RpcError, TransportError, CoordError, OSError, ValueError, KeyError, TypeError) as error:
            self.state.supervisor_observe(job, {'state': 'error', 'summary': {
                'error': str(error)[:512], 'admission': job.get('admission'),
                'wake': job.get('wake_summary'), 'holds': job.get('holds'),
                'open_guards': job.get('open_guards'),
            }, 'source': 'native'})
