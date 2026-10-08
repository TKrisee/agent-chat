"""Model control through real HTTP/CLI and a stateful app-server contract fixture."""
import contextlib
import copy
import io
import os
import subprocess
import uuid
from unittest import mock

from agent_chat import cli
from agent_chat.bridge import Bridge
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
        self.assertEqual(self.status()['requests'][0]['status'], 'failed')
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
