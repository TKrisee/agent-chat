"""Observe and control only the exact main turn on the existing app-server."""
import copy
import json
import os
import re
import time

from .core import CoordError
from .rpc import RpcError, TransportError


class ControlWorker:
    def __init__(self, bridge):
        self.bridge, self.state, self.rpc = bridge, bridge.state, bridge.rpc

    def update(self, job, status, **values):
        return self.state.control_update(job['id'], job['status'], status, **values)

    def observe(self, job, *, require_settings=False):
        thread = self.rpc.request('thread/read', {'threadId':job['thread_id'], 'includeTurns':False})['thread']
        state = thread.get('status', {}).get('type', 'unknown')
        turns = []
        if state == 'active' or job.get('turn_id'):
            cursor = None
            for _ in range(4 if job.get('turn_id') else 1):
                params = {'threadId':job['thread_id'], 'limit':25 if job.get('turn_id') else 1, 'itemsView':'summary', 'sortDirection':'desc'}
                if cursor: params['cursor'] = cursor
                from .native_history import turns_page
                page = turns_page(self.rpc, params)
                turns.extend(page['data'])
                if not job.get('turn_id') or any(t['id'] == job['turn_id'] for t in turns): break
                cursor = page.get('nextCursor')
                if not cursor: break
        current = next((turn['id'] for turn in turns if turn.get('status') == 'inProgress'), None)
        observation = dict(state=state, turn_id=current, queue_count=len(self.bridge.queue(job['thread_id'])),
                           model=thread.get('model'), cwd=thread.get('cwd'))
        # Same-thread resume attaches metadata without input or settings overrides.
        # It may be unavailable for an empty, unmaterialized conversation.
        settings = None
        if job.get('root_team_stop'):
            return observation, thread.get('canAcceptDirectInput') is True, turns, None
        try:
            settings = self.rpc.request('thread/resume', {'threadId':job['thread_id'], 'excludeTurns':True})
            if not isinstance(settings, dict):
                raise CoordError('app-server returned invalid thread settings')
            observation.update(model=settings.get('model'), reasoning_effort=settings.get('reasoningEffort'), cwd=settings.get('cwd'),
                permissions={key:settings.get(key) for key in ('sandbox','approvalPolicy','approvalsReviewer','activePermissionProfile','runtimeWorkspaceRoots')})
        except RpcError:
            if require_settings or job['action'] == 'inspect': raise
        return observation, thread.get('canAcceptDirectInput') is True, turns, settings

    def passive_observe(self, job):
        """Read admission only; never materialize an unloaded thread."""
        thread = self.rpc.request('thread/read', {'threadId': job['thread_id'], 'includeTurns': False})['thread']
        state = thread.get('status', {}).get('type', 'unknown')
        turns = []
        if state == 'active':
            from .native_history import turns_page
            turns = turns_page(self.rpc, {'threadId': job['thread_id'], 'limit': 1,
                                      'itemsView': 'summary', 'sortDirection': 'desc'}).get('data', [])
        return (dict(state=state, turn_id=next((item.get('id') for item in turns if item.get('status') == 'inProgress'), None),
                     queue_count=len(self.bridge.queue(job['thread_id']),), model=thread.get('model'), cwd=thread.get('cwd')),
                thread.get('canAcceptDirectInput') is True)

    def supported_model(self, job, *, deadline=None):
        matches, cursor = [], None
        for _ in range(10):
            params = {} if cursor is None else {'cursor':cursor}
            listed = self.rpc.request('model/list', params, **({} if deadline is None else {'deadline': deadline}))
            models = listed.get('models', listed.get('data')) if isinstance(listed, dict) else None
            if not isinstance(models, list):
                raise CoordError('app-server model/list response is invalid; model transition withheld')
            matches.extend(item for item in models if isinstance(item, dict)
                           and item.get('model', item.get('id', item.get('slug'))) == job['requested_model'])
            cursor = listed.get('nextCursor')
            if not cursor:
                break
        else:
            raise CoordError('app-server model/list exceeds 10 pages; model transition withheld')
        if len(matches) != 1:
            raise CoordError('requested model is not available to the target app-server')
        efforts = matches[0].get('supportedReasoningEfforts', matches[0].get(
            'supportedReasoningLevels', matches[0].get('supported_reasoning_levels')))
        supported = {item.get('reasoningEffort') if isinstance(item, dict) else item for item in efforts} if isinstance(efforts, list) else set()
        if job['requested_effort'] not in supported:
            raise CoordError('requested reasoning_effort is not supported by the target model')

    @staticmethod
    def configuration(settings, thread_id):
        if not isinstance(settings, dict):
            raise CoordError('app-server returned invalid thread settings')
        runtime = settings.get('thread')
        if not isinstance(runtime, dict) or runtime.get('id') != thread_id:
            raise CoordError('app-server settings are not for the exact target thread')
        return {key:value for key, value in settings.items() if key != 'thread'}

    @classmethod
    def expected_settings(cls, job, *, target=True):
        try:
            baseline = json.loads(job['baseline_settings_json'])
        except (KeyError, TypeError, ValueError):
            raise CoordError('model transition baseline is unavailable; keep admission gated and investigate')
        if not isinstance(baseline, dict):
            raise CoordError('model transition baseline is invalid; keep admission gated and investigate')
        expected = cls.configuration(copy.deepcopy(baseline), job['thread_id'])
        if target:
            expected['model'] = job['requested_model']
            expected['reasoningEffort'] = job['requested_effort']
            collaboration = expected.get('collaborationMode')
            if not isinstance(collaboration, dict) or not isinstance(collaboration.get('settings'), dict):
                raise CoordError('model transition baseline lacks collaboration mode settings')
            collaboration['settings']['model'] = job['requested_model']
            collaboration['settings']['reasoning_effort'] = job['requested_effort']
        return expected

    @classmethod
    def settings_match(cls, job, settings, *, target=True):
        return cls.configuration(settings, job['thread_id']) == cls.expected_settings(job, target=target)

    def _experimental_originator(self):
        # No independently trustworthy runtime attestation exists. This is not
        # a caller option; declared test subclasses supply fixture authority only.
        return None

    def _experimental_runtime_version(self):
        originator = self._experimental_originator()
        if not isinstance(originator, str) or not originator:
            raise CoordError('Experimental settings update withheld: independently trusted runtime originator unavailable.')
        response = getattr(self.rpc, 'initialize_response', None)
        fields = ('userAgent', 'codexHome', 'platformFamily', 'platformOs')
        if not isinstance(response, dict) or any(not isinstance(response.get(key), str) or not response[key] for key in fields):
            raise CoordError('Experimental settings update withheld: raw initialize response is missing or malformed.')
        if not os.path.isabs(response['codexHome']):
            raise CoordError('Experimental settings update withheld: initialize codexHome is not an absolute path.')
        # Match only the independently trusted prefix, never clientInfo, a
        # suffix substring, binary/source lineage or an inferred client name.
        if re.match(r'\A' + re.escape(originator) + r'/0\.161\.0 \(', response['userAgent']) is None:
            raise CoordError('Experimental settings update withheld: exact anchored 0.161.0 runtime version unverified.')

    @staticmethod
    def _opaque_equal(left, right):
        # Object member order is immaterial; array order, unknown members,
        # absent/null and JSON scalar types are not normalized or coerced.
        return json.dumps(left, sort_keys=True, separators=(',', ':'), allow_nan=False) == json.dumps(
            right, sort_keys=True, separators=(',', ':'), allow_nan=False)

    def _experimental_admission(self, job, deadline):
        thread = self.rpc.request('thread/read', {'threadId': job['thread_id'], 'includeTurns': False}, deadline=deadline)['thread']
        if (thread.get('id') != job['thread_id'] or thread.get('status', {}).get('type') != 'idle'
                or thread.get('canAcceptDirectInput') is not True or self.bridge.queue(job['thread_id'])):
            raise CoordError('Experimental settings update requires the exact already-loaded idle/direct-ready thread with an empty queue.')
        return dict(state='idle', turn_id=None, queue_count=0)

    @staticmethod
    def _experimental_observation(admission, settings):
        admission.update(model=settings.get('model'), reasoning_effort=settings.get('reasoningEffort'),
                         cwd=settings.get('cwd'), permissions={key: settings.get(key) for key in
                         ('sandbox', 'approvalPolicy', 'approvalsReviewer', 'activePermissionProfile', 'runtimeWorkspaceRoots')})
        return admission

    def _experimental_events(self, boundary, job, expected):
        event = None
        required = ('disabledPluginIds', 'cwd', 'approvalPolicy', 'approvalsReviewer', 'sandboxPolicy',
                    'activePermissionProfile', 'model', 'modelProvider', 'serviceTier', 'effort', 'summary',
                    'collaborationMode', 'multiAgentMode', 'personality')
        for _, notice in self.rpc.notifications_since(boundary):
            if notice.get('method') != 'thread/settings/updated':
                continue
            params = notice.get('params')
            if not isinstance(params, dict) or not isinstance(params.get('threadId'), str):
                raise CoordError('Ambiguous settings event: missing exact thread identity.')
            if params['threadId'] != job['thread_id']:
                continue
            settings = params.get('threadSettings')
            if not isinstance(settings, dict) or any(key not in settings for key in required):
                raise CoordError('Ambiguous settings event: incomplete public ThreadSettings object.')
            if settings['model'] != job['requested_model'] or settings['effort'] != job['requested_effort']:
                raise CoordError('Ambiguous settings event: conflicting model/effort observation.')
            for key, value in expected.items():
                public_key = {'sandbox': 'sandboxPolicy', 'reasoningEffort': 'effort'}.get(key, key)
                # Roots have a separate authoritative readback; selected
                # before-values are unobserved. Every other available value,
                # including unknown top-level members, must match this event.
                if public_key not in ('model', 'effort', 'summary', 'personality', 'runtimeWorkspaceRoots'):
                    if public_key not in settings or not self._opaque_equal(value, settings[public_key]):
                        raise CoordError('Ambiguous settings event: protected complete value missing or changed: ' + public_key)
            if event is not None and not self._opaque_equal(event['params']['threadSettings'], settings):
                raise CoordError('Ambiguous settings event: competing same-thread settings observations.')
            event = notice
        return event

    def _experimental_settings_update(self, job):
        """Observed-state protocol; production remains closed without attestation.

        The native notification has no intent/request ID. This does not certify
        unique causality; it requires serialization, a receive boundary, ACK,
        applicable observed post-state and separate fresh roots readback.
        """
        self._experimental_runtime_version()  # Before any native request.
        if job['status'] != 'pending' or job.get('resolution_id'):
            raise CoordError('Experimental settings update cannot replay an existing mutation boundary.')
        deadline = time.monotonic() + self.rpc.timeout
        current = self.state.control_job(job['id'])
        if not self._opaque_equal(current, job) or job['ownership']['open_guards']:
            raise CoordError('Experimental settings update admission/binding/intent or guard state changed.')
        self._experimental_admission(job, deadline)
        baseline = self.rpc.request('thread/resume', {'threadId': job['thread_id'], 'excludeTurns': True}, deadline=deadline)
        before = self.configuration(baseline, job['thread_id'])
        roots = before.get('runtimeWorkspaceRoots')
        if not isinstance(roots, list) or not roots or any(not isinstance(root, str) for root in roots):
            raise CoordError('Experimental settings update lacks authoritative ordered runtime roots.')
        self.supported_model(job, deadline=deadline)
        admission = self._experimental_admission(job, deadline)
        recheck = self.rpc.request('thread/resume', {'threadId': job['thread_id'], 'excludeTurns': True}, deadline=deadline)
        if (not self._opaque_equal(self.configuration(recheck, job['thread_id']), before)
                or not self._opaque_equal(self.state.control_job(job['id']), current)):
            raise CoordError('Experimental settings update baseline/admission/binding changed before dispatch.')
        boundary = self.rpc.notification_boundary(deadline=deadline)
        expected = self.expected_settings(dict(job, baseline_settings_json=json.dumps(baseline)))
        job = self.update(job, 'changing', baseline_settings=baseline, direct_input=True,
                          observation=self._experimental_observation(admission, recheck),
                          error='Experimental model mutation boundary recorded; no automatic replay.')
        try:
            ack = self.rpc.request('thread/settings/update', {'threadId': job['thread_id'],
                                   'model': job['requested_model'], 'effort': job['requested_effort']}, deadline=deadline)
            if not isinstance(ack, dict) or ack != {}:
                raise CoordError('Ambiguous settings update: malformed empty acknowledgement.')
            job = self.update(self.state.control_job(job['id']), 'changing', native_response={'ack': ack},
                              error='RPC acknowledgement is queued submission, not settings completion.')
            frozen = copy.deepcopy(self.state.control_job(job['id']))
            cursor = boundary
            event = self._experimental_events(boundary, job, expected)
            while event is None:
                cursor, _ = self.rpc.next_notification(cursor, deadline=deadline)
                event = self._experimental_events(boundary, job, expected)
            readback = self.rpc.request('thread/resume', {'threadId': job['thread_id'], 'excludeTurns': True}, deadline=deadline)
            admission = self._experimental_admission(job, deadline)
            self.rpc.notification_boundary(deadline=deadline)  # Retain/read all already-buffered conflicts.
            event = self._experimental_events(boundary, job, expected)
            if (not self._opaque_equal(self.configuration(readback, job['thread_id']), expected)
                    or not self._opaque_equal(self.state.control_job(job['id']), frozen)):
                raise CoordError('Ambiguous settings update: fresh protected settings/ordered roots or admission/binding changed.')
            evidence = {'ack': ack, 'settings_event': event,
                        'selected_before': 'unobserved; exact-version sparse-omission preservation only',
                        'selected_post': {key: event['params']['threadSettings'][key] for key in ('summary', 'personality')},
                        'correlation': 'post-boundary same-thread observed state; no native request/intent ID'}
            return self.update(self.state.control_job(job['id']), 'completed', observed_settings=readback,
                               observation=self._experimental_observation(admission, readback),
                               native_response=evidence, error=None)
        except (RpcError, TransportError, CoordError, KeyError, TypeError, ValueError) as error:
            self.update(self.state.control_job(job['id']), 'uncertain',
                        error='Experimental settings outcome ambiguous; no retry or rollback: ' + str(error))
            raise CoordError('Experimental settings outcome ambiguous; no retry or rollback: ' + str(error)) from error

    def reconcile_model(self, job, error=None):
        try:
            observation, _, _, settings = self.observe(job, require_settings=True)
            if self.settings_match(job, settings):
                self.update(job, 'completed', observation=observation, observed_settings=settings, error=None)
                return
            if job.get('native_response_json') and self.settings_match(job, settings, target=False):
                self.update(job, 'failed', observation=observation, observed_settings=settings,
                            error='Native model resume acknowledged but loaded/subscribed settings remained at the captured baseline; model intent was ignored and prior admission restored.')
                return
            detail = 'Model mutation outcome is uncertain; requested settings and preserved baseline do not match. No replay or rollback will occur.'
            if error:
                detail += ' Last RPC error: ' + str(error)
            self.update(job, 'uncertain', observation=observation, observed_settings=settings, error=detail)
        except (RpcError, CoordError, KeyError, TypeError) as read_error:
            current = self.state.control_job(job['id'])
            if current['status'] in ('changing', 'uncertain'):
                self.update(current, 'uncertain', error='Model mutation outcome is uncertain; read-only reconciliation failed: ' + str(read_error))

    @classmethod
    def cold_supported(cls, settings, thread_id):
        value = cls.configuration(settings, thread_id)
        cwd = value.get('cwd')
        expected_sandbox = {'type': 'workspaceWrite', 'writableRoots': [], 'networkAccess': False,
                            'excludeTmpdirEnvVar': False, 'excludeSlashTmp': False}
        gameplay_sandbox = {'type': 'workspaceWrite', 'writableRoots': ['/Users/kristoftischler/.local/state/agent-chat'],
                            'networkAccess': True, 'excludeTmpdirEnvVar': False, 'excludeSlashTmp': False}
        supported_permission_policy = (
            value.get('activePermissionProfile') == {'id': ':workspace', 'extends': None}
            and value.get('sandbox') == expected_sandbox
        ) or (
            value.get('activePermissionProfile') == {'id': 'itr-agent-chat', 'extends': ':workspace'}
            and value.get('sandbox') == gameplay_sandbox
        )
        return (isinstance(cwd, str) and cwd.startswith('/')
                and value.get('modelProvider') == 'openai' and supported_permission_policy
                and value.get('runtimeWorkspaceRoots') == [cwd]
                and value.get('approvalPolicy') == 'on-request' and value.get('approvalsReviewer') == 'auto_review'
                and value.get('serviceTier') is None and value.get('multiAgentMode') == 'explicitRequestOnly'
                and value.get('collaborationMode') == {'mode': 'default', 'settings': {'model': value.get('model'),
                    'reasoning_effort': value.get('reasoningEffort'), 'developer_instructions': None}})

    def resolve_unchanged(self, job):
        observation, direct = self.passive_observe(job)
        if observation['state'] != 'idle' or not direct or observation['queue_count'] or observation.get('turn_id'):
            self.update(job, 'uncertain', observation=observation,
                        error='Model abandonment withheld: target is not loaded idle/direct-ready with an empty queue.')
            return
        if job['ownership']['open_guards']:
            self.update(job, 'uncertain', observation=observation,
                        error='Model abandonment withheld: open guarded runs remain.')
            return
        observation, direct, _, settings = self.observe(job, require_settings=True)
        recheck, redirect = self.passive_observe(job)
        if recheck['state'] != 'idle' or not redirect or recheck['queue_count'] or recheck.get('turn_id'):
            self.update(job, 'uncertain', observation=recheck,
                        error='Model abandonment withheld: target admission changed during readback.')
            return
        self.update(job, 'resolved_unchanged', observation=observation, direct_input=direct,
                    observed_settings=settings, resolution_evidence={'resolution_id': job['resolution_id'],
                    'native_sample': observation, 'settings': settings},
                    error='Model intent explicitly abandoned; the historical mutation outcome remains unknown.')

    def advance_model(self, job):
        if job.get('model_route') == 'ordinary_input':
            if job['status'] in ('submitting', 'uncertain'):
                return  # The original wake reconciles positive input evidence.
            observation, direct = self.passive_observe(job)
            self.update(job, 'waiting', observation=observation,
                        error='Selection staged for one genuine managed input; known busy/queued input waits without steering. Native defaults are not yet observed.')
            return
        if job.get('resolution_id') and job['status'] in ('changing', 'uncertain'):
            self.resolve_unchanged(job)
            return
        if not job.get('allow_cold_resume'):
            if job['status'] in ('changing', 'uncertain'):
                self.update(job, 'uncertain', error='Experimental settings outcome lacks complete observed-event proof; no automatic replay or convenient readback completion.')
                return
            self._experimental_settings_update(job)
            return
        if job['status'] in ('changing', 'uncertain'):
            self.reconcile_model(job)
            return
        observation, direct, _, settings = self.observe(job, require_settings=True)
        if observation['state'] != 'idle' or not direct or observation['queue_count']:
            if observation['state'] == 'active' or observation.get('turn_id'):
                error = 'Model change refused: the exact target has an active turn; no settings RPC was issued.'
            elif observation['queue_count']:
                error = 'Model change refused: the target app-server queue is nonempty; queued inputs were preserved.'
            else:
                error = 'Model change refused: direct input is not ready on the exact target thread; no settings RPC was issued.'
            self.update(job, 'failed', observation=observation, error=error)
            return
        own = job['ownership']
        if own['open_guards']:
            self.update(job, 'failed', observation=observation,
                        error='Model change refused: an open guarded run remains; no settings RPC was issued.')
            return
        if not job.get('allow_cold_resume'):
            self.update(job, 'failed', observation=observation,
                        error='Model change refused: loaded-thread overrides are unsupported; pass explicit cold-resume opt-in only for a proven preservation baseline.')
            return
        if not self.cold_supported(settings, job['thread_id']):
            self.update(job, 'failed', observation=observation,
                        error='Model change refused: baseline is outside the proven cold-resume preservation profile.')
            return
        self.supported_model(job)
        # Recheck the bound, idle target after model discovery and immediately
        # before writing the durable at-most-once mutation boundary.
        observation, direct, _, settings = self.observe(job, require_settings=True)
        if observation['state'] != 'idle' or not direct or observation['queue_count']:
            self.update(job, 'failed', observation=observation,
                        error='Model change refused: target admission changed during pre-mutation recheck; no settings RPC was issued.')
            return
        if not self.cold_supported(settings, job['thread_id']):
            self.update(job, 'failed', observation=observation,
                        error='Model change refused: baseline changed during model discovery and is outside the proven cold-resume preservation profile.')
            return
        job = self.update(job, 'changing', observation=observation, direct_input=direct, baseline_settings=settings,
                          error='Model mutation boundary recorded; automatic replay is permanently disabled.')
        try:
            self.rpc.request('thread/unsubscribe', {'threadId': job['thread_id']})
            payload = {'threadId': job['thread_id'], 'model': job['requested_model'],
                       'modelProvider': settings['modelProvider'], 'cwd': settings['cwd'],
                       'approvalPolicy': settings['approvalPolicy'], 'approvalsReviewer': settings['approvalsReviewer'],
                       'config': {'model_reasoning_effort': job['requested_effort'],
                                  'default_permissions': settings['activePermissionProfile']['id']},
                       'excludeTurns': True}
            response = self.rpc.request('thread/resume', payload)
            job = self.update(self.state.control_job(job['id']), 'changing', native_response=response,
                              error='Cold model resume response captured; readback reconciliation follows.')
        except RpcError as error:
            current = self.state.control_job(job['id'])
            try:
                rejected_observation, _, _, rejected_settings = self.observe(current, require_settings=True)
                if self.settings_match(current, rejected_settings, target=False):
                    self.update(current, 'failed', observation=rejected_observation,
                                observed_settings=rejected_settings, error=str(error))
                else:
                    self.update(current, 'uncertain', observation=rejected_observation,
                                observed_settings=rejected_settings,
                                error='Model RPC rejected but settings no longer match the captured baseline; no replay or rollback will occur.')
            except (RpcError, CoordError, KeyError, TypeError) as read_error:
                self.update(self.state.control_job(job['id']), 'uncertain',
                            error='Model RPC rejected but baseline readback failed; no replay or rollback will occur: ' + str(read_error))
            return
        except TransportError as error:
            self.reconcile_model(self.state.control_job(job['id']), error)
            raise
        self.reconcile_model(self.state.control_job(job['id']))

    def advance(self, job):
        try:
            if not job.get('root_team_stop'):
                current = self.state.control_job(job['id'])
                if current.get('root_team_barrier_active'):
                    return
            if job['action'] == 'model':
                self.advance_model(job)
                return
            observation, direct, turns, _ = self.observe(job)
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
                if job.get('root_team_stop'):
                    job = self.update(job, 'waiting', observation=observation, turn_id=None)
            if observation['state'] == 'idle' and direct and not observation['queue_count']:
                if not job['interrupt'] and (job['ownership']['reservations'] or job['ownership']['open_guards']):
                    self.update(job, 'waiting', observation=observation,
                        error='Graceful stop awaits owner receipt-backed cleanup. Resume and send the owner a cleanup prompt if needed.')
                    return
                self.update(job, 'completed', observation=observation)
                return
            if job['interrupt'] and job['status'] != 'interrupting' and observation['turn_id'] and (job.get('root_team_stop') or not observation['queue_count']):
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
                if current['action'] == 'model':
                    if current.get('model_route') == 'ordinary_input':
                        if current['status'] in ('pending','waiting'):
                            self.update(current, 'waiting', error='Ordinary model input staged; passive admission unavailable: ' + str(error))
                        if isinstance(error, TransportError): raise
                        return
                    if current['status'] in ('changing', 'uncertain'):
                        self.reconcile_model(current, error)
                    else:
                        self.update(current, 'failed', error='Model change refused before mutation: ' + str(error))
                else:
                    self.update(current, 'interrupting' if current['status'] == 'interrupting' else 'waiting',
                                turn_id=current.get('turn_id'), error=str(error))
            if isinstance(error, TransportError): raise
