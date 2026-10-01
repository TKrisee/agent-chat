"""Observe and control only the exact main turn on the existing app-server."""
from .core import CoordError
from .rpc import RpcError, TransportError


class ControlWorker:
    def __init__(self, bridge):
        self.bridge, self.state, self.rpc = bridge, bridge.state, bridge.rpc

    def update(self, job, status, **values):
        return self.state.control_update(job['id'], job['status'], status, **values)

    def observe(self, job):
        thread = self.rpc.request('thread/read', {'threadId':job['thread_id'], 'includeTurns':False})['thread']
        state = thread.get('status', {}).get('type', 'unknown')
        turns = []
        if state == 'active' or job.get('turn_id'):
            cursor = None
            for _ in range(4 if job.get('turn_id') else 1):
                params = {'threadId':job['thread_id'], 'limit':25 if job.get('turn_id') else 1, 'itemsView':'summary', 'sortDirection':'desc'}
                if cursor: params['cursor'] = cursor
                page = self.rpc.request('thread/turns/list', params)
                turns.extend(page['data'])
                if not job.get('turn_id') or any(t['id'] == job['turn_id'] for t in turns): break
                cursor = page.get('nextCursor')
                if not cursor: break
        current = next((turn['id'] for turn in turns if turn.get('status') == 'inProgress'), None)
        observation = dict(state=state, turn_id=current, queue_count=len(self.bridge.queue(job['thread_id'])),
                           model=thread.get('model'), cwd=thread.get('cwd'))
        # Same-thread resume attaches metadata without input or settings overrides.
        # It may be unavailable for an empty, unmaterialized conversation.
        try:
            settings = self.rpc.request('thread/resume', {'threadId':job['thread_id'], 'excludeTurns':True})
            observation.update(model=settings.get('model'), reasoning_effort=settings.get('reasoningEffort'), cwd=settings.get('cwd'),
                permissions={key:settings.get(key) for key in ('sandbox','approvalPolicy','approvalsReviewer','activePermissionProfile','runtimeWorkspaceRoots')})
        except RpcError:
            if job['action'] == 'inspect': raise
        return observation, thread.get('canAcceptDirectInput') is True, turns

    def advance(self, job):
        try:
            observation, direct, turns = self.observe(job)
            if job['action'] == 'inspect':
                self.update(job, 'completed', observation=observation)
                return
            if job['action'] == 'resume':
                # Reopens the same route; existing messages supply normal input.
                self.update(job, 'completed', observation=observation)
                return
            if job['status'] == 'interrupting':
                exact = next((turn for turn in turns if turn['id'] == job['turn_id']), None)
                if (not exact or exact.get('status') == 'inProgress') and not (observation['state'] == 'idle' and direct and not observation['queue_count']):
                    self.update(job, 'interrupting', turn_id=job['turn_id'], observation=observation,
                                error='Interrupt outcome unresolved; no replay or replacement-turn interrupt.')
                    return
            if observation['state'] == 'idle' and direct and not observation['queue_count']:
                own = job['ownership']
                if not job['interrupt'] and (own['reservations'] or own['open_guards']):
                    self.update(job, 'waiting', observation=observation,
                        error='Graceful stop awaits owner receipt-backed cleanup. Resume and send the owner a cleanup prompt if needed.')
                    return
                self.update(job, 'completed', observation=observation)
                return
            if job['interrupt'] and job['status'] != 'interrupting' and observation['turn_id'] and not observation['queue_count']:
                job = self.update(job, 'interrupting', turn_id=observation['turn_id'], observation=observation,
                                  error='Exact interrupt recorded; external processes and reservations remain owned.')
                self.rpc.request('turn/interrupt', {'threadId':job['thread_id'], 'turnId':job['turn_id']})
                return  # Require a later readback; response alone is not closure.
            self.update(job, 'interrupting' if job['status'] == 'interrupting' else 'waiting',
                turn_id=job.get('turn_id'), observation=observation,
                error='Waiting for idle/direct-input admission and empty app-server queue. Existing queued inputs are preserved; no queue deletion or further interrupt.')
        except (RpcError, CoordError, TransportError, KeyError, TypeError) as error:
            current = self.state.control_job(job['id'])
            if current['status'] not in ('completed','failed','cancelled'):
                self.update(current, 'interrupting' if current['status'] == 'interrupting' else 'waiting',
                            turn_id=current.get('turn_id'), error=str(error))
            if isinstance(error, TransportError): raise
