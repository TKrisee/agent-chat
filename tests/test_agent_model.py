"""Model control through real HTTP/CLI and a stateful app-server contract fixture."""
import contextlib
import copy
import io
import json
import os
import subprocess
import time
import uuid
import unittest
from types import SimpleNamespace
from unittest import mock

from agent_chat import cli
from agent_chat.bridge import Bridge
from agent_chat.bridge_state import BridgeState
from agent_chat.bridge_client import RemoteBridgeState
from agent_chat.agent_control import AgentControlState
from agent_chat.core import CoordError, Coordinator
from agent_chat.control_worker import ControlWorker
from agent_chat.remote import RemoteCoordError
from agent_chat.rpc import RpcError, TransportError
from test_agent_control import ControlRpc
from test_remote_web import RemoteWebFixture, TOKEN

TARGET = 'gpt-5.6-terra'
GAMEPLAY_PROFILE = {'id': 'itr-agent-chat', 'extends': ':workspace'}
GAMEPLAY_SANDBOX = {'type': 'workspaceWrite',
                    'writableRoots': ['/Users/kristoftischler/.local/state/agent-chat'],
                    'networkAccess': True, 'excludeTmpdirEnvVar': False,
                    'excludeSlashTmp': False}


class ModelRpc(ControlRpc):
    def __init__(self, thread):
        super().__init__(thread)
        self.settings = copy.deepcopy(self.original)
        self.settings.update(
            activePermissionProfile={'id': ':workspace', 'extends': None},
            sandbox={'type': 'workspaceWrite', 'writableRoots': [], 'networkAccess': False,
                     'excludeTmpdirEnvVar': False, 'excludeSlashTmp': False},
            approvalsReviewer='auto_review', runtimeWorkspaceRoots=[self.settings['cwd']],
            multiAgentMode='explicitRequestOnly', serviceTier=None,
            collaborationMode={'mode': 'default', 'settings': {'model': self.settings['model'],
                'reasoning_effort': self.settings['reasoningEffort'], 'developer_instructions': None}})
        self.mutations = []
        self.drop_model = None
        self.reject_model = False
        self.change_policy = False
        self.available = True
        self.paged = False
        self.missing_identity = False
        self.bad_identity_after = False
        self.metadata_revision = 0
        self.subscribed = True
        self.other_observer = False
        self.drift_on_catalogue = False
        self.fail_readback_once = False

    def request(self, method, params):
        if method == 'model/list':
            self.calls.append((method, copy.deepcopy(params)))
            if self.drift_on_catalogue:
                self.settings['activePermissionProfile'] = {'id': ':custom', 'extends': None}
            data = [dict(id=TARGET, model=TARGET,
                         supportedReasoningEfforts=[dict(reasoningEffort='high')])] if self.available else []
            if self.paged and not params.get('cursor'):
                return dict(data=[], nextCursor='next')
            return dict(data=data, nextCursor=None)
        if method == 'thread/resume' and params.get('threadId') == self.old:
            self.calls.append((method, copy.deepcopy(params)))
            if not params.get('model') and self.fail_readback_once and self.mutations:
                self.fail_readback_once = False
                raise RpcError(-32000, 'fixture transient readback failure')
            if params.get('model'):
                self.mutations.append(copy.deepcopy(params))
                if self.reject_model:
                    raise RpcError(-32602, 'fixture rejects selected model')
                if params.get('config', {}).get('default_permissions') != self.settings['activePermissionProfile']['id']:
                    raise RpcError(-32602, 'fixture rejects mismatched permission profile')
                if self.drop_model == 'before':
                    raise TransportError('lost request before observable effect')
                if not self.subscribed and not self.other_observer:
                    self.settings['model'] = params['model']
                    self.settings['reasoningEffort'] = params.get('config', {}).get('model_reasoning_effort')
                    self.settings['collaborationMode']['settings']['model'] = params['model']
                    self.settings['collaborationMode']['settings']['reasoning_effort'] = params.get('config', {}).get('model_reasoning_effort')
                if self.change_policy:
                    self.settings['sandbox'] = dict(type='dangerFullAccess')
                if self.drop_model == 'after':
                    self.drop_model = None
                    raise TransportError('lost model-change response after apply')
            self.metadata_revision += 1
            result = copy.deepcopy(self.settings)
            if not self.missing_identity:
                identity = 'wrong-thread' if self.bad_identity_after and self.mutations else self.old
                result['thread'] = dict(id=identity, updatedAt=self.metadata_revision,
                                        model=self.settings['model'])
            return result
        if method == 'thread/unsubscribe' and params.get('threadId') == self.old:
            self.calls.append((method, copy.deepcopy(params)))
            self.subscribed = False
            return {}
        return super().request(method, params)


class _SyntheticSettingsState:
    """Declared application-state fixture, never production anchor authority."""
    def __init__(self, job):
        self.job = copy.deepcopy(job)
        self.evidence = None

    def control_job(self, request):
        assert request == self.job['id']
        return copy.deepcopy(self.job)

    def control_update(self, request, expected, status, **values):
        assert request == self.job['id'] and self.job['status'] == expected
        if expected == 'pending' and status == 'changing':
            assert values['direct_input'] is True and values['observation']['state'] == 'idle'
            assert values['observation']['queue_count'] == 0 and values['observation']['turn_id'] is None
            assert values['observation']['permissions']['runtimeWorkspaceRoots'] == values['baseline_settings']['runtimeWorkspaceRoots']
        self.job['status'] = status
        for key, value in values.items():
            if key in ('baseline_settings', 'native_response'):
                self.job[key + '_json'] = json.dumps(value)
                if key == 'native_response': self.evidence = copy.deepcopy(value)
            else:
                self.job[key] = copy.deepcopy(value)
        return self.control_job(request)


class _SyntheticSettingsRpc:
    """Exact public field names, explicitly synthetic runtime/serialization."""
    def __init__(self, thread, timing='before'):
        self.thread, self.timing, self.timeout = thread, timing, .2
        self.initialize_response = dict(userAgent='fixture-originator/0.161.0 (fixture-os) (agent_chat_bridge; 0.1.0)',
            codexHome='/fixture/private', platformFamily='unix', platformOs='fixture')
        self.settings = dict(thread={'id': thread}, model='gpt-6-luna', reasoningEffort='low',
            cwd='/fixture/a', runtimeWorkspaceRoots=['/fixture/a', '/fixture/b'], modelProvider='openai',
            disabledPluginIds=[], approvalPolicy={'granular': {'future': [True, None]}},
            approvalsReviewer='auto_review', sandbox={'type': 'workspaceWrite', 'unknown': {'keep': [None]}},
            activePermissionProfile={'id': ':workspace', 'extends': None}, serviceTier=None,
            collaborationMode={'mode': 'default', 'settings': {'model': 'gpt-6-luna',
                'reasoning_effort': 'low', 'developer_instructions': None, 'unknown': [True, None]}},
            multiAgentMode='explicitRequestOnly', futureAuthoritative={'unknown': [None, True]})
        self.calls, self.notifications, self.pending = [], [], []
        self.sequence = 0
        self.payloads = []
        self.event_factory = lambda event: [event]
        self.readback_effect = lambda rpc: None
        self.wait_error = None
        self.ack_error = None
        self.ack_value = {}
        self.native_state = 'idle'

    def event(self):
        value = {key: copy.deepcopy(item) for key, item in self.settings.items()
                 if key not in ('thread', 'runtimeWorkspaceRoots', 'reasoningEffort')}
        value['sandboxPolicy'] = value.pop('sandbox')
        value.update(effort=self.settings['reasoningEffort'], summary=None, personality=None)
        return dict(method='thread/settings/updated', params=dict(threadId=self.thread, threadSettings=value))

    def record(self, event):
        self.sequence += 1
        self.notifications.append((self.sequence, copy.deepcopy(event)))

    def notification_boundary(self, *, deadline):
        return self.sequence

    def notifications_since(self, boundary):
        return copy.deepcopy([item for item in self.notifications if item[0] > boundary])

    def next_notification(self, boundary, *, deadline):
        if self.wait_error: raise TransportError(self.wait_error)
        if self.pending: self.record(self.pending.pop(0))
        available = self.notifications_since(boundary)
        if not available: raise TransportError('declared fixture notification timeout')
        return available[0]

    def request(self, method, params, *, deadline=None):
        self.calls.append((method, copy.deepcopy(params), deadline))
        if method == 'thread/read':
            return {'thread': dict(id=self.thread, status={'type': self.native_state}, canAcceptDirectInput=True)}
        if method == 'model/list':
            return {'data': [dict(model='gpt-6.1-sol', supportedReasoningEfforts=[{'reasoningEffort': 'high'}])]}
        if method == 'thread/resume':
            if self.payloads: self.readback_effect(self)
            return copy.deepcopy(self.settings)
        if method == 'thread/settings/update':
            self.payloads.append(copy.deepcopy(params))
            if self.ack_error: raise TransportError(self.ack_error)
            self.settings.update(model=params['model'], reasoningEffort=params['effort'])
            self.settings['collaborationMode']['settings'].update(model=params['model'], reasoning_effort=params['effort'])
            events = self.event_factory(self.event())
            if self.timing == 'before':
                for event in events: self.record(event)
            else:
                self.pending.extend(events)
            return copy.deepcopy(self.ack_value)
        raise AssertionError(method)


class _SyntheticAnchoredWorker(ControlWorker):
    def _experimental_originator(self):
        return 'fixture-originator'  # Declared fixture input, not runtime attestation.


class ObservedSettingsWorkflowTests(unittest.TestCase):
    def fixture(self, timing='before', production=False):
        thread = str(uuid.uuid4())
        job = dict(id=str(uuid.uuid4()), action='model', status='pending', thread_id=thread,
                   requested_model='gpt-6.1-sol', requested_effort='high', allow_cold_resume=False,
                   ownership={'open_guards': []}, host_id='fixture-host', target_session='fixture-session')
        state, rpc = _SyntheticSettingsState(job), _SyntheticSettingsRpc(thread, timing)
        bridge = SimpleNamespace(state=state, rpc=rpc, queue=lambda _: [])
        worker = (ControlWorker if production else _SyntheticAnchoredWorker)(bridge)
        return worker, state, rpc, job

    def test_production_anchor_is_unavailable_and_never_dispatches(self):
        worker, state, rpc, job = self.fixture(production=True)
        with self.assertRaisesRegex(CoordError, 'trusted runtime originator unavailable'):
            worker._experimental_settings_update(job)
        self.assertEqual(rpc.calls, [])
        worker.advance(job)
        self.assertEqual(state.job['status'], 'failed')
        self.assertEqual(rpc.calls, [])

    def test_missing_malformed_wrong_anchor_and_unsupported_version_stop_before_mutation(self):
        for raw in (None, [], {}, dict(userAgent=3), dict(codexHome='relative'),
                    dict(platformFamily=None), dict(platformOs=False),
                    dict(userAgent='agent_chat_bridge/0.161.0 (fixture)'),
                    dict(userAgent='fixture-originator/0.160.0 (fixture)'),
                    dict(userAgent='untrusted/0.161.0 (fixture) (fixture-originator/0.161.0 (suffix))'),
                    dict(userAgent='fixture-originator/0.161.0-extra (fixture)')):
            worker, state, rpc, job = self.fixture()
            rpc.initialize_response = raw if not isinstance(raw, dict) else dict(rpc.initialize_response, **raw)
            if raw == {}: rpc.initialize_response = {}
            with self.subTest(raw=raw), self.assertRaises(CoordError): worker._experimental_settings_update(job)
            self.assertEqual(rpc.payloads, [])
            self.assertEqual(state.job['status'], 'pending')

    def test_before_and_after_ack_sparse_update_full_event_and_separate_roots(self):
        for timing in ('before', 'after'):
            worker, state, rpc, job = self.fixture(timing)
            result = worker._experimental_settings_update(job)
            self.assertEqual(result['status'], 'completed')
            self.assertEqual(rpc.payloads, [dict(threadId=job['thread_id'], model='gpt-6.1-sol', effort='high')])
            self.assertEqual(state.evidence['ack'], {})
            self.assertEqual(state.evidence['selected_post'], dict(summary=None, personality=None))
            self.assertIn('unobserved', state.evidence['selected_before'])
            self.assertNotIn('runtimeWorkspaceRoots', state.evidence['settings_event']['params']['threadSettings'])
            self.assertEqual(state.job['observed_settings']['runtimeWorkspaceRoots'], ['/fixture/a', '/fixture/b'])
            self.assertEqual(state.job['observation']['model'], 'gpt-6.1-sol')
            self.assertEqual(state.job['observation']['reasoning_effort'], 'high')
            self.assertEqual(len({deadline for _, _, deadline in rpc.calls}), 1)
            with self.assertRaisesRegex(CoordError, 'replay'): worker._experimental_settings_update(state.control_job(job['id']))
            self.assertEqual(len(rpc.payloads), 1)

    def test_stale_and_unrelated_events_are_preserved_but_cannot_complete(self):
        worker, state, rpc, job = self.fixture('after')
        stale = rpc.event()
        rpc.record(stale)
        def events(fresh):
            unrelated = copy.deepcopy(fresh); unrelated['params']['threadId'] = 'other-thread'
            return [unrelated, fresh]
        rpc.event_factory = events
        worker._experimental_settings_update(job)
        self.assertEqual(len(rpc.notifications), 3)
        self.assertEqual(state.evidence['settings_event']['params']['threadId'], job['thread_id'])
        worker, state, rpc, job = self.fixture('after')
        rpc.record(rpc.event())
        rpc.event_factory = lambda _: []
        with self.assertRaisesRegex(CoordError, 'ambiguous'): worker._experimental_settings_update(job)
        self.assertEqual(state.job['status'], 'uncertain')

    def test_conflicting_malformed_and_opaque_missing_null_scalar_changes_are_ambiguous(self):
        def changed(event, scenario):
            value = copy.deepcopy(event)
            settings = value['params']['threadSettings']
            if scenario == 'model': settings['model'] = 'conflict'
            elif scenario == 'identity': value['params'].pop('threadId')
            elif scenario == 'missing': settings.pop('serviceTier')
            elif scenario == 'null': settings['approvalPolicy']['granular']['newMember'] = None
            elif scenario == 'scalar': settings['approvalPolicy']['granular']['future'][0] = 1
            elif scenario == 'nested': settings['sandboxPolicy']['unknown']['keep'] = []
            return value
        for scenario in ('model', 'identity', 'missing', 'null', 'scalar', 'nested'):
            worker, state, rpc, job = self.fixture()
            rpc.event_factory = lambda event, scenario=scenario: [changed(event, scenario), event]
            with self.subTest(scenario=scenario), self.assertRaisesRegex(CoordError, 'ambiguous'):
                worker._experimental_settings_update(job)
            self.assertEqual(state.job['status'], 'uncertain')
            self.assertEqual(len(rpc.payloads), 1)

    def test_changed_unknown_authoritative_event_cannot_be_erased_by_matching_readback(self):
        self.assert_unknown_event_refused(omitted=False)

    def test_omitted_unknown_authoritative_event_cannot_be_erased_by_matching_readback(self):
        self.assert_unknown_event_refused(omitted=True)

    def assert_unknown_event_refused(self, *, omitted):
        for timing in ('before', 'after'):
            worker, state, rpc, job = self.fixture(timing)
            # Available authoritative opaque baseline; the event gets its own
            # copy, so fresh readback retains the exact expected complete value.
            rpc.settings['futureAuthoritative']['nullable'] = None

            def conflict(event):
                settings = event['params']['threadSettings']
                if omitted:
                    settings.pop('futureAuthoritative')
                else:
                    settings['futureAuthoritative'].pop('nullable')
                    settings['futureAuthoritative']['unknown'][1] = 1
                return [event]

            rpc.event_factory = conflict
            with self.subTest(timing=timing), self.assertRaisesRegex(CoordError, 'ambiguous'):
                worker._experimental_settings_update(job)
            self.assertEqual(state.job['status'], 'uncertain')
            expected = worker.expected_settings(state.control_job(job['id']))
            matching = rpc.request('thread/resume', {'threadId': job['thread_id'], 'excludeTurns': True})
            self.assertEqual(worker.configuration(matching, job['thread_id']), expected)
            worker.advance_model(state.control_job(job['id']))
            self.assertEqual(state.job['status'], 'uncertain')
            self.assertEqual(len(rpc.payloads), 1)

    def test_post_event_roots_unknown_settings_binding_admission_and_late_conflict(self):
        for scenario in ('roots', 'unknown', 'binding', 'active', 'late-conflict'):
            worker, state, rpc, job = self.fixture()
            def effect(rpc, scenario=scenario):
                if scenario == 'roots': rpc.settings['runtimeWorkspaceRoots'].reverse()
                elif scenario == 'unknown': rpc.settings['futureAuthoritative']['unknown'].append('changed')
                elif scenario == 'binding': state.job['host_id'] = 'changed-host'
                elif scenario == 'active': rpc.native_state = 'active'
                else:
                    conflict = rpc.event(); conflict['params']['threadSettings']['effort'] = 'low'; rpc.record(conflict)
            rpc.readback_effect = effect
            with self.subTest(scenario=scenario), self.assertRaisesRegex(CoordError, 'ambiguous'):
                worker._experimental_settings_update(job)
            self.assertEqual(state.job['status'], 'uncertain')

    def test_timeout_disconnect_missing_ack_and_malformed_ack_never_replay(self):
        for scenario in ('timeout', 'disconnect', 'missing-ack', 'malformed-ack'):
            worker, state, rpc, job = self.fixture('after')
            if scenario == 'missing-ack': rpc.ack_error = 'fixture lost acknowledgement'
            elif scenario == 'malformed-ack': rpc.ack_value = None
            else: rpc.wait_error = 'fixture ' + scenario
            with self.subTest(scenario=scenario), self.assertRaisesRegex(CoordError, 'ambiguous'):
                worker._experimental_settings_update(job)
            self.assertEqual(state.job['status'], 'uncertain')
            worker.advance_model(state.control_job(job['id']))
            self.assertEqual(len(rpc.payloads), 1)

    def test_unloaded_queue_guard_and_missing_roots_refuse_without_update(self):
        for scenario in ('unloaded', 'queue', 'guard', 'roots'):
            worker, state, rpc, job = self.fixture()
            if scenario == 'unloaded': rpc.native_state = 'notLoaded'
            elif scenario == 'queue': worker.bridge.queue = lambda _: ['preserved-input']
            elif scenario == 'guard': job['ownership']['open_guards'] = ['preserved-guard']; state.job = copy.deepcopy(job)
            else: rpc.settings.pop('runtimeWorkspaceRoots')
            with self.subTest(scenario=scenario), self.assertRaises(CoordError): worker._experimental_settings_update(job)
            self.assertEqual(rpc.payloads, [])
            self.assertFalse(any(method == 'thread/unsubscribe' for method, _, _ in rpc.calls))
            if scenario == 'unloaded': self.assertFalse(any(method == 'thread/resume' for method, _, _ in rpc.calls))

    def test_ordinary_inspection_does_not_require_experimental_anchor(self):
        worker, state, rpc, job = self.fixture(production=True)
        job['action'] = 'inspect'; state.job = copy.deepcopy(job)
        worker.advance(job)
        self.assertEqual(state.job['status'], 'completed')
        self.assertEqual(rpc.payloads, [])

    def test_complete_synthetic_websocket_worker_flow_uses_real_capture_transport(self):
        from agent_chat.rpc import RpcClient
        from test_rpc import FakeServer, _frame, _initialize, _send
        _, state, native_fixture, job = self.fixture()

        def handler(sock):
            _initialize(sock, native_fixture.initialize_response)
            while True:
                opcode, _, body = _frame(sock)
                if opcode == 8: return
                request = json.loads(body)  # Synthetic application protocol endpoint.
                previous = native_fixture.sequence
                result = native_fixture.request(request['method'], request.get('params', {}), deadline=time.monotonic() + 2)
                for _, event in native_fixture.notifications_since(previous):
                    _send(sock, 1, json.dumps(event).encode())
                _send(sock, 1, json.dumps(dict(id=request['id'], result=result)).encode())

        server = FakeServer(handler)
        print('synthetic_ws_thread_start ident=' + str(server.thread.ident))
        try:
            with RpcClient(server.endpoint) as rpc:
                worker = _SyntheticAnchoredWorker(SimpleNamespace(state=state, rpc=rpc, queue=lambda _: []))
                worker._experimental_settings_update(job)
                self.assertEqual(state.job['status'], 'completed')
                self.assertEqual(native_fixture.payloads, [dict(threadId=job['thread_id'], model='gpt-6.1-sol', effort='high')])
                self.assertEqual(state.evidence['selected_post'], dict(summary=None, personality=None))
        finally:
            server.join()
        self.assertFalse(server.thread.is_alive())
        print('synthetic_ws_thread_joined ident=' + str(server.thread.ident))


class AgentModelTests(RemoteWebFixture):
    def setUp(self):
        super().setUp()
        self.call('register', agent='worker')
        self.call('register', session='b', agent='orchestrator')
        self.target_thread = str(uuid.uuid4())
        self.call('bind', thread=self.target_thread)
        self.identity = dict(owner='model-fixture-worker', secret='x'*40, host_id='mac')
        self.state = RemoteBridgeState(self.client, self.url, self.identity)
        self.state.call('acquire')
        self.rpc = ModelRpc(self.target_thread)
        self.bridge = Bridge(self.state, self.rpc)

    def change(self, **values):
        params = dict(to='worker', expected_thread=self.target_thread, request_id=str(uuid.uuid4()),
                      model=TARGET, reasoning_effort='high', confirm=True, allow_cold_resume=True)
        params.update(values)
        return self.call('agent-model', session='b', **params)

    def status(self):
        return self.call('agent-status', session='b', to='worker')

    def advance(self):
        for job in self.state.control_jobs():
            ControlWorker(self.bridge).advance(job)

    def gameplay_profile(self):
        self.rpc.settings.update(activePermissionProfile=copy.deepcopy(GAMEPLAY_PROFILE),
                                 sandbox=copy.deepcopy(GAMEPLAY_SANDBOX))

    def uncertain_model_request(self):
        request = str(uuid.uuid4())
        self.rpc.drop_model = 'before'
        self.change(request_id=request)
        with self.assertRaises(TransportError):
            self.advance()
        self.rpc.drop_model = None
        self.assertEqual(self.status()['requests'][0]['status'], 'uncertain')
        return request

    def resolve(self, request_id, *, session='b', resolution_id=None):
        return self.call('agent-model-resolve', session=session, request_id=request_id,
                         expected_thread=self.target_thread,
                         resolution_id=resolution_id or str(uuid.uuid4()), confirm=True)

    def captured_changing(self):
        job = self.change()
        baseline = self.rpc.request('thread/resume', dict(threadId=self.target_thread, excludeTurns=True))
        observation = dict(state='idle', queue_count=0, turn_id=None,
                           model=baseline['model'], reasoning_effort=baseline['reasoningEffort'],
                           cwd=baseline['cwd'], permissions={key:baseline[key] for key in
                               ('sandbox', 'approvalPolicy', 'approvalsReviewer',
                                'activePermissionProfile', 'runtimeWorkspaceRoots')})
        self.state.control_update(job['id'], 'pending', 'changing', baseline_settings=baseline,
                                  observation=observation, direct_input=True)
        return job, baseline, observation

    def test_resolution_passive_probe_never_resumes_an_unloaded_thread(self):
        self.rpc.status[self.target_thread] = 'notLoaded'
        job = dict(thread_id=self.target_thread)
        observation, direct = ControlWorker(self.bridge).passive_observe(job)
        self.assertEqual(observation['state'], 'notLoaded')
        self.assertFalse(direct)
        self.assertFalse(any(method == 'thread/resume' for method, _ in self.rpc.calls))

    def test_cold_resume_requires_explicit_opt_in_before_unsubscribe(self):
        self.change(allow_cold_resume=False)
        self.advance()
        self.assertEqual(self.status()['requests'][0]['status'], 'waiting')
        self.assertEqual(self.status()['admission'], 'running')
        self.assertFalse(any(method == 'thread/unsubscribe' for method, _ in self.rpc.calls))
        self.assertEqual(self.rpc.mutations, [])

    def test_exact_gameplay_named_profile_cold_resume_preserves_nonmodel_settings(self):
        self.gameplay_profile()
        baseline = copy.deepcopy(self.rpc.settings)
        self.change()
        self.advance()
        self.assertEqual(self.status()['requests'][0]['status'], 'completed')
        self.assertEqual(self.rpc.mutations[0]['config']['default_permissions'], 'itr-agent-chat')
        for key, value in baseline.items():
            if key not in ('model', 'reasoningEffort', 'collaborationMode'):
                self.assertEqual(self.rpc.settings[key], value, key)
        self.assertEqual(self.rpc.settings['collaborationMode']['settings']['developer_instructions'],
                         baseline['collaborationMode']['settings']['developer_instructions'])

    def test_gameplay_named_profile_refuses_any_effective_sandbox_difference(self):
        self.gameplay_profile()
        self.rpc.settings['sandbox']['writableRoots'].append('/tmp/unproven-root')
        self.change()
        self.advance()
        self.assertEqual(self.status()['requests'][0]['status'], 'failed')
        self.assertFalse(any(method == 'thread/unsubscribe' for method, _ in self.rpc.calls))
        self.assertEqual(self.rpc.mutations, [])

    def test_gameplay_named_profile_refuses_a_different_profile_inheritance(self):
        self.gameplay_profile()
        self.rpc.settings['activePermissionProfile']['extends'] = None
        self.change()
        self.advance()
        self.assertEqual(self.status()['requests'][0]['status'], 'failed')
        self.assertFalse(any(method == 'thread/unsubscribe' for method, _ in self.rpc.calls))
        self.assertEqual(self.rpc.mutations, [])

    def test_gameplay_named_profile_remaining_observer_is_acknowledged_noop(self):
        self.gameplay_profile()
        self.rpc.other_observer = True
        baseline = copy.deepcopy(self.rpc.settings)
        self.change()
        self.advance()
        self.assertEqual(self.status()['requests'][0]['status'], 'failed')
        self.assertEqual(self.status()['admission'], 'running')
        self.assertEqual(self.rpc.settings, baseline)

    def test_other_observer_acknowledged_noop_restores_admission(self):
        self.rpc.other_observer = True
        self.change()
        self.advance()
        status = self.status()
        self.assertEqual(status['requests'][0]['status'], 'failed')
        self.assertEqual(status['admission'], 'running')
        self.assertEqual(self.rpc.settings['model'], self.rpc.original['model'])

    def test_acknowledged_baseline_reconciliation_restores_after_temporary_readback_error(self):
        self.rpc.other_observer = True
        self.rpc.fail_readback_once = True
        self.change()
        self.advance()
        self.assertEqual(self.status()['requests'][0]['status'], 'uncertain')
        self.assertEqual(self.status()['admission'], 'stopping')
        self.advance()
        row = self.status()['requests'][0]
        self.assertEqual(row['status'], 'failed')
        self.assertEqual(self.status()['admission'], 'running')
        self.assertEqual(len(self.rpc.mutations), 1)

    def test_profile_drift_during_catalogue_refuses_before_cold_detach(self):
        self.rpc.drift_on_catalogue = True
        self.change()
        self.advance()
        row = self.status()['requests'][0]
        self.assertEqual(row['status'], 'failed')
        self.assertIn('baseline changed during model discovery', row['error'])
        self.assertFalse(any(method == 'thread/unsubscribe' for method, _ in self.rpc.calls))
        self.assertEqual(self.rpc.mutations, [])

    def test_confirmed_unknown_model_request_resolves_unchanged_idempotently(self):
        request = self.uncertain_model_request()
        resolution = str(uuid.uuid4())
        self.resolve(request, resolution_id=resolution)
        self.advance()
        row = self.status()['requests'][0]
        self.assertEqual(row['status'], 'resolved_unchanged')
        self.assertEqual(row['resolution_id'], resolution)
        self.assertTrue(row['uncertainty_provenance'])
        self.resolve(request, resolution_id=resolution)
        self.assertEqual(self.status()['requests'][0]['status'], 'resolved_unchanged')
        self.rpc.drop_model = None
        self.change()
        self.advance()
        self.assertEqual(self.status()['requests'][0]['status'], 'completed')

    def test_resolution_refuses_an_unauthorized_actor(self):
        unknown = self.uncertain_model_request()
        with self.assertRaises(RemoteCoordError):
            self.resolve(unknown, session='a')
        self.assertIsNone(self.status()['requests'][0]['resolution_id'])

    def test_resolution_refuses_a_changing_phase(self):
        job, _, _ = self.captured_changing()
        with self.assertRaisesRegex(RemoteCoordError, 'stable uncertain'):
            self.resolve(job['id'])

    def assert_resolution_withheld(self, setup):
        request = self.uncertain_model_request()
        setup()
        self.resolve(request)
        self.advance()
        row = self.status()['requests'][0]
        self.assertEqual(row['status'], 'uncertain')
        self.assertEqual(self.status()['admission'], 'stopping')

    def test_resolution_refuses_active_target_without_restoring_admission(self):
        self.assert_resolution_withheld(lambda: self.rpc.status.__setitem__(self.target_thread, 'active'))

    def test_resolution_refuses_nonempty_queue_without_restoring_admission(self):
        self.assert_resolution_withheld(lambda: self.rpc.queues.__setitem__(self.target_thread, [{'id': 'retained-resolution-input'}]))

    def test_resolution_refuses_open_guard_without_restoring_admission(self):
        hold = self.call('request', resource='resolution-guard', minutes=5)
        def open_guard():
            self.call('begin-guard', resource=hold['resource'], token=hold['token'], pid=0)
        self.assert_resolution_withheld(open_guard)

    def test_resolution_refuses_changed_binding(self):
        request = self.uncertain_model_request()
        coord = Coordinator(self.db)
        try:
            with coord.tx():
                coord.db.execute('UPDATE bridge_bindings SET thread_id=? WHERE session_id=?',
                                 (str(uuid.uuid4()), 'a'))
        finally:
            coord.close()
        with self.assertRaisesRegex(RemoteCoordError, 'route or host changed'):
            self.resolve(request)
        self.assertIsNone(self.status()['requests'][0]['resolution_id'])

    def test_host_reassignment_blocks_model_restore_and_settings_updates(self):
        job, baseline, observation = self.captured_changing()
        coord = Coordinator(self.db, 'b')
        try:
            with coord.tx():
                coord.db.execute('UPDATE remote_session_hosts SET host_id=? WHERE session_id=?', ('other-host', 'a'))
            controls = AgentControlState(coord)
            with self.assertRaisesRegex(CoordError, 'host changed'):
                controls.update(job['id'], 'changing', 'changing', native_response=baseline)
            with self.assertRaisesRegex(CoordError, 'host changed'):
                controls.update(job['id'], 'changing', 'failed', observation=observation,
                                observed_settings=baseline, error='would restore admission')
        finally:
            coord.close()
        self.assertEqual(self.status()['requests'][0]['status'], 'changing')
        self.assertEqual(self.status()['admission'], 'stopping')

    def test_cli_http_bridge_changes_exact_thread_and_retry_is_idempotent(self):
        request = str(uuid.uuid4())
        args = ['--server', self.url, '--session', 'b', 'agent-model',
                '--to', 'worker', '--expected-thread', self.target_thread, '--model', TARGET,
                '--reasoning-effort', 'high', '--request-id', request, '--allow-cold-resume', '--confirm']
        with mock.patch.dict(os.environ, dict(AGENT_CHAT_API_TOKEN=TOKEN), clear=True), mock.patch('agent_chat.remote.client_host_id', return_value='mac'):
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                self.assertEqual(cli.main(args), 0)
        result = subprocess.run(['jq', '-er', '.id'], input=out.getvalue(),
                                text=True, capture_output=True, check=True)
        self.assertEqual(result.stdout.strip(), request)
        baseline = copy.deepcopy(self.rpc.settings)
        self.bridge.tick()
        status = self.status()
        self.assertEqual(status['thread_id'], self.target_thread)
        self.assertEqual(status['requests'][0]['status'], 'completed')
        self.assertEqual(status['admission'], 'running')
        self.assertEqual(status['observation']['model'], TARGET)
        self.assertEqual(status['observation']['reasoning_effort'], 'high')
        for key, value in baseline.items():
            if key not in ('model', 'reasoningEffort', 'collaborationMode'):
                self.assertEqual(self.rpc.settings[key], value, key)
        self.assertEqual(self.rpc.settings['collaborationMode']['settings']['model'], TARGET)
        self.assertEqual(self.rpc.settings['collaborationMode']['settings']['reasoning_effort'], 'high')
        self.change(request_id=request)
        self.bridge.tick()
        self.assertEqual(len(self.rpc.mutations), 1)
        self.assertFalse(any(method in ('thread/start', 'thread/fork', 'turn/start')
                             for method, _ in self.rpc.calls))

    def test_active_turn_is_refused_without_interrupt_or_model_mutation(self):
        self.rpc.status[self.target_thread] = 'active'
        self.change()
        self.advance()
        self.assertEqual(self.status()['requests'][0]['status'], 'failed')
        self.assertEqual(self.status()['admission'], 'running')
        self.assertEqual(self.rpc.mutations, [])
        self.assertFalse(any(method == 'turn/interrupt' for method, _ in self.rpc.calls))

    def test_native_queue_is_preserved_and_blocks_mutation(self):
        queued = dict(id='retained-input', clientUserMessageId='operator-input')
        self.rpc.queues[self.target_thread] = [queued]
        self.change()
        self.advance()
        self.assertEqual(self.rpc.queues[self.target_thread], [queued])
        self.assertEqual(self.rpc.mutations, [])
        self.assertEqual(self.status()['requests'][0]['status'], 'failed')

    def test_children_reservations_and_inbox_survive_model_change(self):
        child = str(uuid.uuid4())
        self.call('register', session='child', agent='audit')
        self.call('bind', session='child', parent_session='a', agent_path='/root/audit')
        hold = self.call('request', resource='retained-resource', minutes=5)
        self.call('request', session='b', resource='retained-resource', minutes=5)
        message = self.call('send', session='b', to='worker', body='Retain this inbox delivery')
        before = self.call('inbox')
        self.change()
        self.advance()
        self.assertEqual(self.status()['requests'][0]['status'], 'completed')
        self.assertEqual(self.call('inbox'), before)
        self.assertEqual(self.call('message', id=message['id'])['message']['acked_at'], None)
        self.assertEqual(self.status()['ownership']['reservations'][0]['reservation_id'], hold['reservation_id'])
        child_status = self.call('agent-status', session='b', to='audit')
        self.assertIsNone(child_status['thread_id'])
        self.assertEqual(child_status['parent_session'], 'a')
        self.assertEqual(child_status['admission'], 'running')

    def test_open_guard_blocks_change_without_closing_or_transferring_it(self):
        hold = self.call('request', resource='guarded-work', minutes=5)
        guard = self.call('begin-guard', resource='guarded-work', token=hold['token'], pid=0)
        self.change()
        self.advance()
        self.assertEqual(self.rpc.mutations, [])
        self.assertEqual(self.status()['requests'][0]['status'], 'failed')
        self.assertEqual(self.status()['ownership']['open_guards'][0]['run_id'], guard['run_id'])

    def test_lost_success_response_reconciles_without_replaying_mutation(self):
        self.rpc.drop_model = 'after'
        self.change()
        try:
            self.advance()
        except TransportError:
            pass
        self.advance()
        self.assertEqual(self.status()['requests'][0]['status'], 'completed')
        self.assertEqual(len(self.rpc.mutations), 1)

    def test_unknown_result_with_unchanged_settings_remains_uncertain_without_retry(self):
        self.rpc.drop_model = 'before'
        self.change()
        try:
            self.advance()
        except TransportError:
            pass
        self.advance()
        self.assertEqual(self.status()['requests'][0]['status'], 'uncertain')
        self.assertEqual(len(self.rpc.mutations), 1)
        self.assertNotEqual(self.status()['admission'], 'running')

    def test_observed_permission_change_is_not_success_and_no_rollback(self):
        self.rpc.change_policy = True
        self.change()
        self.advance()
        self.assertEqual(self.status()['requests'][0]['status'], 'uncertain')
        self.assertEqual(len(self.rpc.mutations), 1)

    def test_explicit_rpc_rejection_is_reported_and_gate_restored(self):
        self.rpc.reject_model = True
        self.change()
        self.advance()
        self.assertEqual(self.status()['requests'][0]['status'], 'failed')
        self.assertEqual(self.status()['admission'], 'running')
        self.assertIn('fixture rejects', self.status()['requests'][0]['error'])
        self.assertEqual(len(self.rpc.mutations), 1)

    def test_unavailable_model_is_refused_before_mutation(self):
        self.rpc.available = False
        self.change()
        self.advance()
        self.assertEqual(self.status()['requests'][0]['status'], 'failed')
        self.assertEqual(self.rpc.mutations, [])

    def test_payload_identity_concurrency_and_host_scope(self):
        request = str(uuid.uuid4())
        first = self.change(request_id=request)
        self.assertEqual(self.change(request_id=request)['id'], first['id'])
        with self.assertRaisesRegex(RemoteCoordError, 'differs'):
            self.change(request_id=request, reasoning_effort='low')
        with self.assertRaises(RemoteCoordError):
            self.change(expected_thread=str(uuid.uuid4()))
        with self.assertRaises(RemoteCoordError):
            self.change(confirm=False)
        with self.assertRaises(RemoteCoordError):
            self.call('agent-resume', session='b', to='worker', expected_thread=self.target_thread,
                      request_id=str(uuid.uuid4()), confirm=True)
        other = RemoteBridgeState(self.client, self.url,
                                  dict(self.identity, owner='other-owner', host_id='other'))
        other.call('acquire')
        self.assertEqual(other.control_jobs(), [])
        with self.assertRaisesRegex(RemoteCoordError, 'another host'):
            other.control_job(first['id'])

    def test_missing_or_changed_readback_identity_cannot_complete(self):
        self.rpc.missing_identity = True
        self.change()
        self.advance()
        self.assertEqual(self.status()['requests'][0]['status'], 'failed')
        self.assertEqual(self.rpc.mutations, [])
        self.rpc.missing_identity = False
        self.rpc.bad_identity_after = True
        self.change()
        self.advance()
        self.assertEqual(self.status()['requests'][0]['status'], 'uncertain')
        self.assertEqual(len(self.rpc.mutations), 1)

    def test_catalogue_pagination_and_runtime_metadata_preserve_configuration(self):
        self.rpc.paged = True
        self.change()
        self.advance()
        self.assertEqual(self.status()['requests'][0]['status'], 'completed')
        self.assertEqual(len(self.rpc.mutations), 1)
        pages = [params for method, params in self.rpc.calls if method == 'model/list']
        self.assertEqual(pages, [{}, {'cursor': 'next'}])
        self.assertEqual(self.rpc.settings['cwd'], self.rpc.original['cwd'])

    def test_post_boundary_failure_requires_original_settings_proof(self):
        job = self.change()
        baseline = self.rpc.request('thread/resume', dict(threadId=self.target_thread, excludeTurns=True))
        observation = dict(state='idle', queue_count=0, turn_id=None,
                           model=baseline['model'], reasoning_effort=baseline['reasoningEffort'],
                           cwd=baseline['cwd'], permissions={key:baseline[key] for key in
                               ('sandbox', 'approvalPolicy', 'approvalsReviewer',
                                'activePermissionProfile', 'runtimeWorkspaceRoots')})
        changing = self.state.control_update(job['id'], 'pending', 'changing',
                                             baseline_settings=baseline, observation=observation,
                                             direct_input=True)
        with self.assertRaises(RemoteCoordError):
            self.state.control_update(job['id'], 'changing', 'failed', error='unproved failure')
        altered = copy.deepcopy(baseline)
        altered['sandbox'] = dict(type='dangerFullAccess')
        with self.assertRaises(RemoteCoordError):
            self.state.control_update(job['id'], 'changing', 'failed',
                                      observed_settings=altered, error='drift is not original baseline')
        self.assertEqual(self.status()['admission'], 'stopping')
        self.state.control_update(job['id'], 'changing', 'failed',
                                  observed_settings=baseline, observation=observation,
                                  error='explicit rejection with verified original settings')
        self.assertEqual(self.status()['admission'], 'running')
        self.assertEqual(self.rpc.mutations, [])

    def test_stopped_admission_is_preserved(self):
        self.call('agent-stop', session='b', to='worker', expected_thread=self.target_thread,
                  request_id=str(uuid.uuid4()), confirm=True)
        self.advance()
        self.assertEqual(self.status()['admission'], 'stopped')
        self.change()
        self.advance()
        self.assertEqual(self.status()['requests'][0]['status'], 'completed')
        self.assertEqual(self.status()['admission'], 'stopped')

    def test_resume_retains_bound_children_while_stop_still_refuses(self):
        child = str(uuid.uuid4())
        self.call('register', session='child', agent='audit')
        self.call('bind', session='child', parent_session='a', agent_path='/root/audit')
        self.call('agent-resume', session='b', to='worker', expected_thread=self.target_thread,
                  request_id=str(uuid.uuid4()), confirm=True)
        self.advance()
        self.assertEqual(self.status()['admission'], 'running')
        self.assertEqual(self.call('agent-status', session='b', to='audit')['parent_session'], 'a')
        with self.assertRaisesRegex(RemoteCoordError, 'retire bound children'):
            self.call('agent-stop', session='b', to='worker', expected_thread=self.target_thread,
                      request_id=str(uuid.uuid4()), confirm=True)

# Reuse the existing root bootstrap helpers without inheriting unrelated tests.
import test_root_orchestrator as _root_tests
from pathlib import Path
from agent_chat.remote import HttpClient


class OrdinaryTurnRpc(ModelRpc):
    """Neutral ordinary-turn fixture; no native/model authority outside tests."""
    def __init__(self, thread):
        super().__init__(thread)
        self.inputs = []
        self.failure = None
        self.race = False
        self.settings.update(runtimeWorkspaceRoots=['/neutral/a', '/neutral/b', '/neutral/c'],
                             opaque={'ordered': [3, None, {'future': True}]}, summary=None, personality='neutral')

    def request(self, method, params):
        if method == 'thread/turns/list':
            self.calls.append((method, copy.deepcopy(params)))
            return {'data': [{'id': 'neutral-history', 'status': 'completed',
                              'items': copy.deepcopy(self.history[self.old])}], 'nextCursor': None}
        if method == 'turn/start':
            self.calls.append((method, copy.deepcopy(params)))
            if self.failure == 'reject':
                raise RpcError(-32602, 'neutral explicit refusal')
            if self.failure == 'before':
                raise TransportError('neutral lost response before effect')
            self.inputs.append(copy.deepcopy(params))
            self.settings['model'] = params.get('model', self.settings['model'])
            self.settings['reasoningEffort'] = params.get('effort', self.settings['reasoningEffort'])
            self.history[self.old].append({'type': 'userMessage', 'clientId': params['clientUserMessageId'],
                                           'content': params['input']})
            self.status[self.old] = 'active'
            result = {'turn': {'id': 'ordinary-'+str(uuid.uuid4()),
                               'status': 'interrupted' if self.race else 'inProgress'}}
            if self.failure == 'wrong-response':
                result['threadId'] = 'different-thread'
            if self.failure == 'after':
                raise TransportError('neutral lost response after effect')
            return result
        return super().request(method, params)


class OrdinaryModelInputTests(RemoteWebFixture):
    def setUp(self):
        RemoteWebFixture.setUp(self)
        self.call('register', agent='worker')
        self.call('register', session='b', agent='orchestrator')
        self.target_thread = str(uuid.uuid4())
        self.call('bind', thread=self.target_thread)
        self.identity = dict(owner='ordinary-model-fixture', secret='x'*40, host_id='mac')
        self.state = RemoteBridgeState(self.client, self.url, self.identity)
        self.state.call('acquire')
        self.rpc = OrdinaryTurnRpc(self.target_thread)
        self.bridge = Bridge(self.state, self.rpc)

    def change(self, **values):
        values.setdefault('allow_cold_resume', False)
        values.setdefault('model', 'gpt-6.1-sol')
        return AgentModelTests.change(self, **values)

    status = AgentModelTests.status
    advance = AgentModelTests.advance

    def test_ordinary_request_keeps_admission_and_waits_for_genuine_input(self):
        self.rpc.status[self.target_thread] = 'active'
        request = self.change()
        self.assertEqual(self.status()['admission'], 'running')
        self.advance()
        self.assertEqual(self.status()['requests'][0]['status'], 'waiting')
        self.assertEqual(self.rpc.inputs, [])
        self.assertFalse(any(method in ('thread/resume', 'thread/unsubscribe', 'thread/settings/update')
                             for method, _ in self.rpc.calls))
        self.assertEqual(request['requested_model'], 'gpt-6.1-sol')

    def test_genuine_input_bears_model_once_and_next_input_is_unforced(self):
        original = copy.deepcopy(self.rpc.settings)
        request = self.change()
        message = self.call('send', session='b', to='worker', body='neutral genuine input')['id']
        self.bridge.tick()
        self.assertEqual(len(self.rpc.inputs), 1)
        params = self.rpc.inputs[0]
        self.assertEqual(set(params), {'threadId', 'clientUserMessageId', 'input', 'model', 'effort'})
        self.assertEqual((params['threadId'], params['model'], params['effort']),
                         (self.target_thread, 'gpt-6.1-sol', 'high'))
        self.assertIn(message, params['input'][0]['text'])
        status = self.status()['requests'][0]
        self.assertEqual(status['native_input_state'], 'accepted')
        self.assertEqual(status['status'], 'completed')
        self.assertFalse(status['observed_defaults_verified'])
        self.assertEqual(status['id'], request['id'])
        self.assertEqual(status['native_input_job_id'], params['clientUserMessageId'])
        self.assertIsNone(status['observed_settings_json'])
        for key in ('runtimeWorkspaceRoots', 'opaque', 'summary', 'personality', 'activePermissionProfile', 'sandbox'):
            self.assertEqual(self.rpc.settings[key], original[key], key)
        self.call('message', id=message)
        self.rpc.status[self.target_thread] = 'idle'
        self.call('send', session='b', to='worker', body='later genuine input')
        self.bridge.tick()
        self.assertEqual(sum(method == 'turn/start' for method, _ in self.rpc.calls), 1)
        self.assertFalse(any('model' in params or 'effort' in params
                             for method, params in self.rpc.calls if method == 'thread/queue/add'))

    def test_busy_and_native_queue_stage_original_order_without_steering(self):
        request = self.change()
        self.rpc.status[self.target_thread] = 'active'
        first = self.call('send', session='b', to='worker', body='first original input')['id']
        second = self.call('send', session='b', to='worker', body='second original input')['id']
        self.bridge.tick()
        self.assertEqual(self.rpc.inputs, [])
        self.rpc.status[self.target_thread] = 'idle'
        existing = {'id': 'native-other', 'clientUserMessageId': 'native-external'}
        self.rpc.queues[self.target_thread] = [existing]
        self.bridge.tick()
        self.assertEqual(self.rpc.queues[self.target_thread], [existing])
        self.assertEqual(self.rpc.inputs, [])
        self.rpc.queues[self.target_thread] = []
        self.bridge.tick()
        text = self.rpc.inputs[0]['input'][0]['text']
        self.assertLess(text.index(first), text.index(second))
        self.assertEqual(self.status()['requests'][0]['id'], request['id'])

    def test_duplicate_uuid_and_conflicting_requests_do_not_replace_selection(self):
        request = self.change()
        self.assertEqual(self.change(request_id=request['id'])['id'], request['id'])
        with self.assertRaises(RemoteCoordError):
            self.change(request_id=request['id'], reasoning_effort='low')
        with self.assertRaises(RemoteCoordError):
            self.change()
        self.call('send', session='b', to='worker', body='single genuine input')
        self.bridge.tick()
        self.assertEqual(self.change(request_id=request['id'])['native_input_state'], 'accepted')
        self.bridge.tick()
        self.assertEqual(len(self.rpc.inputs), 1)

        self.assertEqual(self.change()['status'], 'pending')

    def test_lost_before_response_survives_server_restart_without_replay(self):
        request = self.change()
        self.call('send', session='b', to='worker', body='original uncertain input')
        self.rpc.failure = 'before'
        with self.assertRaises(TransportError):
            self.bridge.tick()
        self.assertEqual(self.status()['requests'][0]['status'], 'uncertain')
        self.stop()
        self.start()
        self.state = RemoteBridgeState(self.client, self.url, self.identity)
        self.state.call('acquire')
        self.bridge = Bridge(self.state, self.rpc)
        self.rpc.failure = None
        self.rpc.history[self.target_thread].append({'type': 'userMessage', 'clientId': 'unrelated-old-input', 'content': []})
        self.bridge.tick()
        status = self.status()['requests'][0]
        self.assertEqual((status['id'], status['status']), (request['id'], 'uncertain'))
        self.assertEqual(sum(method == 'turn/start' for method, _ in self.rpc.calls), 1)
        job = self.state.jobs()[0]
        coord = Coordinator(self.db, 'a')
        try:
            with self.assertRaises(CoordError):
                BridgeState(coord).retry(job['id'], confirmed=True)
        finally:
            coord.close()

    def test_lost_after_response_reconciles_only_original_positive_input(self):
        self.change()
        self.call('send', session='b', to='worker', body='correlated input')
        self.rpc.failure = 'after'
        with self.assertRaises(TransportError):
            self.bridge.tick()
        self.rpc.failure = None
        self.bridge = Bridge(self.state, self.rpc)
        self.bridge.tick()
        status = self.status()['requests'][0]
        self.assertEqual(status['native_input_state'], 'accepted')
        self.assertEqual(status['native_input_evidence'], 'history_client_id')
        self.assertIsNone(status['native_response_json'])
        self.assertIsNone(status['observed_settings_json'])
        self.assertEqual(len(self.rpc.inputs), 1)

    def test_explicit_refusal_retains_input_then_explicit_retry_is_unforced(self):
        request = self.change()
        self.call('send', session='b', to='worker', body='refused genuine input')
        self.rpc.failure = 'reject'
        self.bridge.tick()
        self.assertEqual(self.status()['requests'][0]['status'], 'failed')
        self.assertEqual(self.status()['admission'], 'running')
        job = self.state.jobs()[0]
        self.bridge.tick()
        self.assertEqual(sum(method == 'turn/start' for method, _ in self.rpc.calls), 1)
        coord = Coordinator(self.db, 'a')
        try:
            BridgeState(coord).retry(job['id'], confirmed=True)
        finally:
            coord.close()
        self.rpc.failure = None
        self.bridge.tick()
        self.assertEqual(sum(method == 'turn/start' for method, _ in self.rpc.calls), 1)
        self.assertFalse(any('model' in p or 'effort' in p for m, p in self.rpc.calls if m == 'thread/queue/add'))
        self.assertEqual(self.status()['requests'][0]['native_input_job_id'], job['id'])
        self.assertEqual(self.status()['requests'][0]['id'], request['id'])

    def test_four_ordered_roots_and_opaque_nulls_are_never_overridden(self):
        self.rpc.settings['runtimeWorkspaceRoots'] = ['/neutral/C', '/neutral/A/../A', '/neutral/B ', '/neutral/D']
        self.rpc.settings['opaque']['future'] = {'summary': None, 'personality': ['untouched', False]}
        original = copy.deepcopy(self.rpc.settings)
        self.change()
        self.call('send', session='b', to='worker', body='four roots neutral input')
        self.bridge.tick()
        for key, value in original.items():
            if key not in ('model', 'reasoningEffort'):
                self.assertEqual(self.rpc.settings[key], value, key)
        self.assertEqual(set(self.rpc.inputs[0]), {'threadId', 'clientUserMessageId', 'input', 'model', 'effort'})

    def test_external_start_or_steer_interruption_is_not_defaults_proof(self):
        self.change()
        self.call('send', session='b', to='worker', body='external race neutral input')
        self.rpc.race = True
        self.bridge.tick()
        status = self.status()['requests'][0]
        self.assertEqual(status['native_input_state'], 'accepted')
        self.assertIn('interrupted', status['native_response_json'])
        self.assertIsNone(status['observed_settings_json'])
        self.assertIn('not verified', status['error'])

    def test_changed_host_before_original_input_is_withheld(self):
        self.change()
        self.call('send', session='b', to='worker', body='host-bound input')
        messages = self.state.pending()
        job = self.state.prepare(self.target_thread, messages, self.bridge.payload(self.target_thread, messages))
        coord = Coordinator(self.db)
        try:
            with coord.tx():
                coord.db.execute('UPDATE remote_session_hosts SET host_id=? WHERE session_id=?', ('other-host', 'a'))
        finally:
            coord.close()
        with self.assertRaises(CoordError):
            self.bridge.advance(job)
        self.assertEqual(self.rpc.inputs, [])

    def test_wrong_response_is_uncertain_not_success_or_replay(self):
        self.change()
        self.call('send', session='b', to='worker', body='mismatched response input')
        self.rpc.failure = 'wrong-response'
        with self.assertRaises(CoordError):
            self.bridge.tick()
        status = self.status()['requests'][0]
        self.assertEqual(status['status'], 'uncertain')
        self.assertIsNone(status['native_response_json'])
        self.assertEqual(len(self.rpc.inputs), 1)


class ScopedModelRpc(_root_tests.ScopedRpc):
    def __init__(self, *args):
        super().__init__(*args)
        # Keep the source settings and the freshly created orchestrator on the
        # one actually proven cold-resume profile.  Reset creation compares the
        # new thread to this source before it binds the project route.
        self.original.update(
            activePermissionProfile={'id': ':workspace', 'extends': None},
            sandbox={'type': 'workspaceWrite', 'writableRoots': [], 'networkAccess': False,
                     'excludeTmpdirEnvVar': False, 'excludeSlashTmp': False},
            approvalsReviewer='auto_review', runtimeWorkspaceRoots=[self.original['cwd']],
            multiAgentMode='explicitRequestOnly', serviceTier=None,
            collaborationMode={'mode': 'default', 'settings': {'model': self.original['model'],
                'reasoning_effort': self.original['reasoningEffort'], 'developer_instructions': None}})
        self.unsubscribed = set()

    def request(self, method, params):
        if method == 'model/list':
            self.calls.append((method, copy.deepcopy(params)))
            return dict(data=[dict(id=TARGET, model=TARGET,
                                   supportedReasoningEfforts=[dict(reasoningEffort='high')])],
                        nextCursor=None)
        if method == 'thread/unsubscribe' and params.get('threadId') in self.started:
            self.calls.append((method, copy.deepcopy(params)))
            self.unsubscribed.add(params['threadId'])
            return {}
        if method == 'thread/resume' and params.get('model'):
            self.calls.append((method, copy.deepcopy(params)))
            settings = self.started[params['threadId']]
            if params['threadId'] in self.unsubscribed:
                settings['model'] = params['model']
                settings['reasoningEffort'] = params['config']['model_reasoning_effort']
                settings['collaborationMode']['settings']['model'] = params['model']
                settings['collaborationMode']['settings']['reasoning_effort'] = params['config']['model_reasoning_effort']
            return copy.deepcopy(settings)
        result = super().request(method, params)
        if method == 'thread/start':
            result.update(approvalsReviewer='auto_review', multiAgentMode='explicitRequestOnly', serviceTier=None,
                collaborationMode={'mode':'default','settings':{'model':result['model'],
                    'reasoning_effort':result['reasoningEffort'],'developer_instructions':None}})
            self.started[result['thread']['id']] = copy.deepcopy(result)
        return result


class RootModelRoutingTests(RemoteWebFixture):
    root = _root_tests.RootOrchestratorTests.root
    project = _root_tests.RootOrchestratorTests.project
    routed = _root_tests.RootOrchestratorTests.routed
    configure = _root_tests.RootOrchestratorTests.configure
    project_bridge = _root_tests.RootOrchestratorTests.project_bridge

    def setUp(self):
        super().setUp()
        self.call('register', agent='root')
        self.thread_id = str(uuid.uuid4())
        self.call('bind', thread=self.thread_id)
        self.neutral = Path(self.tmp.name).resolve() / 'neutral'
        self.neutral.mkdir()
        self.workspace = Path(self.tmp.name).resolve() / 'project'
        self.workspace.mkdir()
        self.rpc = ScopedModelRpc(self.thread_id, str(self.neutral))
        self.identity = dict(owner='root-model-fixture', secret='x'*40, host_id='mac')
        self.state = RemoteBridgeState(self.client, self.url, self.identity)
        self.state.call('acquire')
        self.bridge = Bridge(self.state, self.rpc)
        self.call('agent-status', refresh=True, expected_thread=self.thread_id,
                  request_id=str(uuid.uuid4()))
        self.bridge.tick()
        self.root('enroll', expected_thread=self.thread_id, confirm=True)

    def test_root_cli_routes_only_designated_orchestrator_without_changing_root(self):
        project = self.project()
        self.configure(project)
        state, bridge = self.project_bridge(project)
        bridge.tick()
        target = self.routed(project, 'session-reset-status', to='orchestrator')['thread_id']
        self.rpc.status[target] = 'idle'  # Fixture completes the bootstrap turn.
        baseline = copy.deepcopy(self.rpc.started[target])
        root_baseline = copy.deepcopy(self.rpc.original)
        request = str(uuid.uuid4())
        args = ['--server', self.url, '--session', 'a', 'root', 'in', project, 'agent-model',
                '--to', 'orchestrator', '--expected-thread', target, '--model', TARGET,
                '--reasoning-effort', 'high', '--request-id', request, '--allow-cold-resume', '--confirm']
        with mock.patch.dict(os.environ, dict(AGENT_CHAT_API_TOKEN=TOKEN), clear=True), mock.patch('agent_chat.remote.client_host_id', return_value='mac'):
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(cli.main(args), 0)
        bridge.tick()
        status = self.routed(project, 'agent-status', to='orchestrator')
        self.assertEqual(status['thread_id'], target)
        self.assertEqual(status['requests'][0]['status'], 'completed', status['requests'][0].get('error'))
        self.assertEqual(status['observation']['model'], TARGET)
        expected = copy.deepcopy(baseline)
        expected['model'] = TARGET
        expected['reasoningEffort'] = 'high'
        expected['collaborationMode']['settings']['model'] = TARGET
        expected['collaborationMode']['settings']['reasoning_effort'] = 'high'
        self.assertEqual(self.rpc.started[target], expected)
        self.assertEqual(self.rpc.original, root_baseline)
        client = HttpClient(self.url, TOKEN, project=project)
        client.call('/api/coord', dict(op='register', session='unmanaged', host_id='mac',
                                     params=dict(agent='unmanaged-worker')))
        with self.assertRaisesRegex(RemoteCoordError, 'orchestrators only'):
            self.routed(project, 'agent-model', to='unmanaged-worker',
                        expected_thread=str(uuid.uuid4()), model=TARGET,
                        reasoning_effort='high', request_id=str(uuid.uuid4()), confirm=True)
        with self.assertRaises(RemoteCoordError):
            self.call('agent-model', to='root', expected_thread=self.thread_id, model=TARGET,
                      reasoning_effort='high', request_id=str(uuid.uuid4()), confirm=True)

    def test_root_readonly_inspection_remains_available(self):
        request = str(uuid.uuid4())
        self.call('agent-status', refresh=True, expected_thread=self.thread_id, request_id=request)
        self.bridge.tick()
        status = self.call('agent-status')
        row = next(item for item in status['requests'] if item['id'] == request)
        self.assertEqual(row['status'], 'completed')
        self.assertEqual(status['admission'], 'running')
