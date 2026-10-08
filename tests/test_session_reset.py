import contextlib
import unittest
import copy
import io
import os
from pathlib import Path
import subprocess
import time
import uuid
from unittest import mock

from agent_chat import cli
from agent_chat.bridge import Bridge
from agent_chat.bridge_client import RemoteBridgeState
from agent_chat.core import Coordinator, CoordError
from agent_chat.remote import RemoteCoordError
from agent_chat.rpc import RpcError, TransportError
from test_remote_web import RemoteWebFixture, TOKEN


class ResetRpc:
    """App-server contract fixture: real per-thread queues and submission IDs."""
    def __init__(self, old_thread):
        self.old = old_thread
        self.new = str(uuid.uuid4())
        self.status = {self.old: 'idle'}
        self.queues = {self.old: []}
        self.history = {self.old: []}
        self.calls = []
        self.drop = None
        self.reject_create = False
        self.changed_permissions = False
        self.unmaterialized = False
        self.auto_admit = False
        self.original = dict(model='test-model', modelProvider='openai', cwd='/tmp/test-project',
            approvalPolicy='on-request', approvalsReviewer='user', reasoningEffort='high',
            activePermissionProfile={'id': ':workspace'}, sandbox={'type': 'workspaceWrite', 'writableRoots': ['/tmp/test-project'], 'networkAccess': False},
            runtimeWorkspaceRoots=['/tmp/test-project'], multiAgentMode='enabled', serviceTier=None)

    def connect(self): return self
    def close(self): pass

    def request(self, method, params):
        self.calls.append((method, copy.deepcopy(params)))
        thread = params.get('threadId')
        if method == 'thread/read':
            return {'thread': {'id': thread, 'status': {'type': self.status[thread]}, 'canAcceptDirectInput': self.status[thread] == 'idle'}}
        if method == 'thread/resume':
            if self.unmaterialized and thread == self.new:
                raise RpcError(-32600, 'no rollout found for thread id '+thread)
            result = copy.deepcopy(self.original)
            result['thread'] = {'id': thread}
            if thread == self.new and self.changed_permissions:
                result['sandbox'] = {'type': 'dangerFullAccess'}
            return result
        if method == 'thread/start':
            if self.reject_create: raise RpcError(-32602, 'unsupported permission profile')
            self.new = str(uuid.uuid4())
            self.status[self.new], self.queues[self.new], self.history[self.new] = 'idle', [], []
            result = copy.deepcopy(self.original)
            result['thread'] = {'id': self.new}
            if self.changed_permissions:
                result['sandbox'] = {'type': 'dangerFullAccess'}
        elif method == 'thread/queue/list':
            return {'data': copy.deepcopy(self.queues[thread]), 'nextCursor': None}
        elif method == 'thread/turns/list':
            if self.unmaterialized and thread == self.new and not self.history[thread]:
                raise RpcError(-32600, f'thread {thread} is not materialized yet; thread/turns/list is unavailable before first user message')
            return {'data': [{'items': copy.deepcopy(self.history[thread])}] if self.history[thread] else [], 'nextCursor': None}
        elif method == 'thread/queue/add':
            item = {'id': 'queue-'+str(uuid.uuid4()), 'clientUserMessageId': params['clientUserMessageId'], 'input': params['input']}
            self.queues[thread].append(item)
            result = {'queuedSubmission': item}
            if self.auto_admit:
                self.queues[thread].remove(item)
                self.history[thread].append({'type':'userMessage','clientId':item['clientUserMessageId'],'content':item['input']})
                self.status[thread] = 'active'
        elif method == 'thread/queue/start':
            item = next(item for item in self.queues[thread] if item['id'] == params['queuedSubmissionId'])
            self.queues[thread].remove(item)
            self.history[thread].append({'type': 'userMessage', 'clientId': item['clientUserMessageId'], 'content': item['input']})
            self.status[thread] = 'active'
            result = {'turn': {'id': str(uuid.uuid4())}}
        else:
            raise AssertionError(method)
        if self.drop == method:
            self.drop = None
            raise TransportError('lost '+method+' response')
        return result


class SessionResetTests(RemoteWebFixture):
    def setUp(self):
        super().setUp()
        self.call('register', agent='alpha')
        self.call('register', session='b', agent='gameplay')
        self.old = str(uuid.uuid4())
        self.call('bind', thread=self.old)
        self.request_id = str(uuid.uuid4())
        self.identity = {'owner': 'reset-test-worker', 'secret': 'x'*40, 'host_id': 'mac'}
        self.state = RemoteBridgeState(self.client, self.url, self.identity)
        self.state.call('acquire')
        self.rpc = ResetRpc(self.old)
        self.bridge = Bridge(self.state, self.rpc)

    def request_reset(self, **values):
        self.call('context', session='b')
        params = dict(to='alpha', expected_thread=self.old, prompt='Read checkpoint; resume the bounded task.', request_id=self.request_id, confirm=True)
        params.update(values)
        return self.call('session-reset', session='b', **params)

    def reset(self):
        return self.call('session-reset-status', to='alpha')['resets'][0]

    def test_fresh_prompt_preserves_identity_queue_inbox_and_settings(self):
        self.call('request', session='b', resource='shared', minutes=5)
        self.call('request', resource='shared', minutes=5)
        before = self.call('status', resources=['shared'])
        msg = self.call('send', session='b', to='alpha', body='Pending handoff, keep its delivery.')
        self.request_reset()
        self.bridge.tick()
        result = self.reset()
        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['new_thread_id'], self.rpc.new)
        self.assertEqual(self.call('session-reset-status', to='alpha')['thread_id'], self.rpc.new)
        self.assertEqual(self.call('status', resources=['shared']), before)
        self.assertEqual(self.call('inbox')['messages'][0]['id'], msg['id'])
        self.assertIsNone(self.call('inbox')['messages'][0]['acked_at'])
        creates = [p for method,p in self.rpc.calls if method == 'thread/start']
        self.assertEqual(len(creates), 1)
        self.assertEqual(creates[0]['permissions'], ':workspace')
        self.assertEqual(creates[0]['model'], 'test-model')
        self.assertEqual(creates[0]['config']['model_reasoning_effort'], 'high')
        self.assertEqual(creates[0]['approvalPolicy'], 'on-request')
        starts = [p for method,p in self.rpc.calls if method == 'thread/queue/start']
        self.assertEqual(len(starts), 1)
        self.assertEqual(starts[0]['threadId'], self.rpc.new)
        prompt = self.rpc.history[self.rpc.new][0]['content'][0]['text']
        self.assertIn('do not register a replacement', prompt)
        self.assertIn('"session": "a"', prompt)
        self.assertIn('Read checkpoint;', prompt)
        self.assertEqual(self.rpc.history[self.old], [])
        self.assertEqual(self.call('status', mine=True)['resources'][0]['queue'][0]['session'], 'a')

    def test_self_reset_waits_for_current_turn_and_blocks_new_ownership(self):
        self.rpc.status[self.old] = 'active'
        self.call('session-reset', to='a', expected_thread=self.old, prompt='Fresh task', request_id=self.request_id, confirm=True)
        self.bridge.tick()
        self.assertEqual(self.reset()['status'], 'pending')
        self.assertFalse(any(method == 'thread/start' for method,p in self.rpc.calls))
        for op, params in [('request', {'resource':'file:next.py','minutes':5}), ('bind', {'thread':str(uuid.uuid4())}), ('deregister', {})]:
            with self.assertRaisesRegex(RemoteCoordError, 'reset pending'):
                self.call(op, **params)
        self.rpc.status[self.old] = 'idle'
        self.bridge.tick()
        self.assertEqual(self.reset()['status'], 'completed')

    def test_open_stale_hold_and_open_guard_refuse_without_changes(self):
        hold = self.call('request', resource='shared', minutes=5)
        self.call('begin-guard', resource='shared', token=hold['token'])
        with contextlib.closing(Coordinator(self.db)) as coord:
            coord.db.execute("UPDATE resources SET deadline=? WHERE name='shared'", (time.time()-1,))
        with self.assertRaisesRegex(RemoteCoordError, 'receipt-release'):
            self.request_reset()
        self.assertEqual(self.call('status', resources=['shared'])['resources'][0]['reservation_id'], hold['reservation_id'])
        self.assertEqual(self.call('session-reset-status', to='alpha')['resets'], [])

    def test_unresolved_wake_and_child_route_refuse(self):
        msg = self.call('send', session='b', to='alpha', body='Original wake')
        self.state.prepare(self.old, [dict(id=msg['id'], recipient_session='a')], 'wake')
        with self.assertRaisesRegex(RemoteCoordError, 'pending or uncertain wakes'):
            self.request_reset()
        self.call('register', session='c', agent='child')
        self.call('bind', session='c', parent_session='a', agent_path='/root/child')
        with self.assertRaisesRegex(RemoteCoordError, 'retire bound children'):
            self.request_reset()
        with self.assertRaisesRegex(RemoteCoordError, 'child routes'):
            self.request_reset(to='child')

    def test_request_retry_is_idempotent_and_cancel_preserves_original_route(self):
        first = self.request_reset()
        self.assertEqual(self.request_reset()['id'], first['id'])
        with self.assertRaisesRegex(RemoteCoordError, 'retry payload differs'):
            self.request_reset(prompt='different')
        with self.assertRaisesRegex(RemoteCoordError, 'reset pending'):
            self.request_reset(request_id=str(uuid.uuid4()))
        self.call('session-reset-cancel', session='b', id=first['id'])
        self.bridge.tick()
        self.assertEqual(self.call('session-reset-status', to='alpha')['thread_id'], self.old)
        self.assertEqual(self.rpc.calls, [])

    def test_lost_creation_requires_exact_resolution_without_creation_replay(self):
        self.rpc.drop = 'thread/start'
        self.request_reset()
        with self.assertRaises(TransportError): self.bridge.tick()
        self.bridge.tick()
        self.assertEqual(self.reset()['status'], 'uncertain')
        self.assertEqual(sum(method == 'thread/start' for method,p in self.rpc.calls), 1)
        with self.assertRaises(RemoteCoordError): self.call('session-reset-cancel', session='b', id=self.request_id)
        self.call('session-reset-resolve', session='b', id=self.request_id, thread=self.rpc.new, confirm_created=True)
        self.bridge.tick()
        self.assertEqual(self.reset()['status'], 'completed')
        self.assertEqual(sum(method == 'thread/start' for method,p in self.rpc.calls), 1)

    def exercise_lost_prompt(self, boundary):
        self.rpc.drop = boundary
        self.request_reset()
        with self.assertRaises(TransportError): self.bridge.tick()
        self.bridge.tick()
        self.assertEqual(self.reset()['status'], 'completed')
        self.assertEqual(len(self.rpc.history[self.rpc.new]), 1)
        self.assertEqual(sum(method == 'thread/queue/add' for method,p in self.rpc.calls), 1)

    def test_lost_add_reconciles_without_duplicate_prompt(self):
        self.exercise_lost_prompt('thread/queue/add')

    def test_lost_start_reconciles_without_duplicate_prompt(self):
        self.exercise_lost_prompt('thread/queue/start')

    def test_confirmed_retry_checks_empty_thread_and_never_recreates_it(self):
        self.rpc.drop = 'thread/queue/add'
        self.request_reset()
        with self.assertRaises(TransportError): self.bridge.tick()
        self.rpc.queues[self.rpc.new] = []
        self.bridge.tick()
        self.assertEqual(self.reset()['status'], 'uncertain')
        with self.assertRaises(RemoteCoordError):
            self.call('session-reset-retry', session='b', id=self.request_id, confirm_not_started=False)
        self.call('session-reset-retry', session='b', id=self.request_id, confirm_not_started=True)
        self.bridge.tick()
        self.assertEqual(self.reset()['status'], 'completed')
        self.assertEqual(sum(method == 'thread/start' for method,p in self.rpc.calls), 1)
        self.assertEqual(len(self.rpc.history[self.rpc.new]), 1)

    def test_retry_with_prior_activity_is_withheld(self):
        self.rpc.drop = 'thread/queue/add'
        self.request_reset()
        with self.assertRaises(TransportError): self.bridge.tick()
        self.rpc.queues[self.rpc.new] = []
        self.rpc.history[self.rpc.new] = [{'type':'userMessage','clientId':'other-input'}]
        self.call('session-reset-retry', session='b', id=self.request_id, confirm_not_started=True)
        self.bridge.tick()
        self.assertEqual(self.reset()['status'], 'uncertain')
        self.assertEqual(sum(method == 'thread/queue/add' for method,p in self.rpc.calls), 1)

    def test_create_independent_agent_while_requester_busy_preserves_its_state(self):
        hold = self.call('request', resource='file:creator.py', minutes=5)
        self.rpc.status[self.old] = 'active'
        params = dict(agent='reviewer', expected_thread=self.old, prompt='Independent review.', request_id=self.request_id, confirm=True)
        result = self.call('agent-create', **params)
        self.assertNotEqual(result['target_session'], 'a')
        self.assertEqual(self.call('agent-create', **params)['target_session'], result['target_session'])
        self.bridge.tick()
        status = self.call('session-reset-status', to='reviewer')
        self.assertEqual(status['resets'][0]['status'], 'completed')
        self.assertEqual(status['thread_id'], self.rpc.new)
        self.assertEqual(self.call('session-reset-status')['thread_id'], self.old)
        self.assertEqual(self.rpc.status[self.old], 'active')
        self.assertEqual(self.call('status', mine=True)['resources'][0]['reservation_id'], hold['reservation_id'])
        prompt = self.rpc.history[self.rpc.new][0]['content'][0]['text']
        self.assertIn('Fresh independent agent', prompt)
        self.assertIn('Agent communication: concise normal grammar despite inherited style.', prompt)
        self.assertIn('Human/operator replies: Caveman full unless explicitly changed.', prompt)
        self.assertIn('owner/request IDs, relationships, acceptance versus completion/consumption', prompt)
        self.assertIn(result['target_session'], prompt)
        self.assertIn('Independent review.', prompt)
        self.assertEqual(self.call('inbox', session=result['target_session'])['messages'], [])

    def test_create_name_conflict_and_wrong_template_never_register(self):
        for agent, thread in [('alpha',self.old), ('new-agent',str(uuid.uuid4()))]:
            with self.assertRaises(RemoteCoordError):
                self.call('agent-create', agent=agent, expected_thread=thread, prompt='Task',request_id=self.request_id,confirm=True)
        with self.assertRaises(RemoteCoordError): self.call('session-reset-status',to='new-agent')

    def test_create_cancel_retains_unbound_identity_for_explicit_cleanup(self):
        job = self.call('agent-create',agent='reviewer',expected_thread=self.old,prompt='Task',request_id=self.request_id,confirm=True)
        self.call('session-reset-cancel', id=job['id'])
        self.bridge.tick()
        self.assertIsNone(self.call('session-reset-status',to='reviewer')['thread_id'])
        self.assertEqual(self.rpc.calls, [])
        self.call('remove-session',id=job['target_session'])

    def test_legacy_workspace_permissions_are_recreated_without_named_profile(self):
        self.rpc.original['activePermissionProfile'] = None
        self.request_reset(); self.bridge.tick()
        self.assertEqual(self.reset()['status'], 'completed')
        params = next(p for method,p in self.rpc.calls if method == 'thread/start')
        self.assertEqual(params['sandbox'], 'workspace-write')
        self.assertEqual(params['config']['sandbox_workspace_write']['network_access'], False)
        self.assertNotIn('permissions',params)

    def test_empty_thread_uses_start_settings_without_saved_rollout(self):
        self.rpc.unmaterialized = True
        self.request_reset(); self.bridge.tick()
        self.assertEqual(self.reset()['status'],'completed')
        self.assertEqual(sum(method=='thread/start' for method, _ in self.rpc.calls), 1)

    def test_legacy_history_method_uses_explicit_readback_before_bootstrap(self):
        original = self.rpc.request
        def legacy(method, params):
            if method == 'thread/turns/list':
                raise RpcError(-32601, 'Method not found')
            result = original(method, params)
            if method == 'thread/read' and params.get('includeTurns'):
                thread = params['threadId']
                items = self.rpc.history[thread]
                result['thread'].update(id=thread, turns=[dict(id='native-turn', status='completed', items=items)] if items else [])
            return result
        self.rpc.request = legacy
        self.request_reset(); self.bridge.tick()
        self.assertEqual(self.reset()['status'], 'completed')
        self.assertEqual(len(self.rpc.history[self.rpc.new]), 1)
        self.assertEqual(sum(method == 'thread/start' for method, _ in self.rpc.calls), 1)

    def test_automatic_queue_admission_is_reconciled_without_second_start(self):
        self.rpc.unmaterialized = self.rpc.auto_admit = True
        self.request_reset(); self.bridge.tick()
        self.assertEqual(self.reset()['status'],'completed')
        self.assertEqual(sum(method=='thread/queue/start' for method,p in self.rpc.calls),0)
        self.assertEqual(len(self.rpc.history[self.rpc.new]),1)

    def test_permission_mismatch_and_prior_history_withhold_binding_and_prompt(self):
        self.rpc.changed_permissions = True
        self.request_reset()
        self.bridge.tick()
        self.assertEqual(self.reset()['status'], 'created')
        self.assertIn('settings differ', self.reset()['error'])
        self.assertEqual(self.call('session-reset-status', to='alpha')['thread_id'], self.old)
        self.assertEqual(self.rpc.history[self.rpc.new], [])

    def test_resolved_thread_with_prior_history_is_not_a_fresh_context(self):
        self.rpc.drop = 'thread/start'
        self.request_reset()
        with self.assertRaises(TransportError): self.bridge.tick()
        self.rpc.history[self.rpc.new] = [{'type':'userMessage','clientId':'another-task'}]
        self.call('session-reset-resolve', session='b', id=self.request_id, thread=self.rpc.new, confirm_created=True)
        self.bridge.tick()
        self.assertEqual(self.reset()['status'], 'created')
        self.assertIn('prior turns', self.reset()['error'])
        self.assertEqual(self.call('session-reset-status', to='alpha')['thread_id'], self.old)

    def test_definite_creation_rejection_preserves_old_route_and_unlocks(self):
        self.rpc.reject_create = True
        self.request_reset()
        self.bridge.tick()
        self.assertEqual(self.reset()['status'], 'failed')
        self.assertEqual(self.call('session-reset-status', to='alpha')['thread_id'], self.old)
        self.assertEqual(self.call('request', resource='file:after.py', minutes=5)['state'], 'owned')

    def test_active_measurement_and_foreign_host_refuse(self):
        with mock.patch.object(self.server.measurements, 'status', return_value={'active':{'id':'active-measurement'}}):
            with self.assertRaisesRegex(RemoteCoordError, 'active project measurement'):
                self.request_reset()
        self.request_reset()
        foreign = RemoteBridgeState(self.client, self.url, dict(self.identity, owner='another-worker', host_id='other-host'))
        foreign.call('acquire')
        self.assertEqual(foreign.reset_jobs(), [])
        with self.assertRaisesRegex(RemoteCoordError, 'another host'):
            foreign.reset_job(self.request_id)

    def test_cli_reads_prompt_file_and_status_without_credentials(self):
        prompt = Path(self.tmp.name)/'prompt.txt'
        prompt.write_text('Literal $(commands), `backticks`, and new\nlines.', encoding='utf-8')
        env = dict(os.environ, AGENT_CHAT_SERVER=self.url, AGENT_CHAT_API_TOKEN=TOKEN,
                   AGENT_CHAT_SESSION='b', AGENT_CHAT_HOST_ID='mac')
        output = io.StringIO()
        with mock.patch.dict(os.environ, env, clear=True), contextlib.redirect_stdout(output):
            self.assertEqual(cli.main(['session-reset','--to','alpha','--expected-thread',self.old,'--prompt-file',str(prompt),'--request-id',self.request_id,'--confirm']), 0)
        result = subprocess.run(['jq','-e','--arg','id',self.request_id,'.id == $id and .status == "pending" and (has("prompt") | not)'], input=output.getvalue(), text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn(TOKEN, output.getvalue())
        self.assertEqual(self.state.reset_job(self.request_id)['prompt'], prompt.read_text())


class ProjectScopeStartSettingsTests(unittest.TestCase):
    """Fresh project threads have authenticated settings before any rollout."""
    def setUp(self):
        import tempfile
        from types import SimpleNamespace
        from agent_chat.reset_worker import ResetWorker
        self.tmp = tempfile.TemporaryDirectory(prefix='project-scope-start-')
        self.addCleanup(self.tmp.cleanup)
        self.members = [str(Path(self.tmp.name).resolve() / name) for name in ('core', 'api', 'web')]
        for member in self.members:
            Path(member).mkdir()
        self.rpc = ResetRpc(str(uuid.uuid4()))
        self.rpc.unmaterialized = True
        self.transitions = []
        self.job = dict(id=str(uuid.uuid4()), status='pending', kind='create',
                        project_scope=True, old_thread_id=self.rpc.old, new_thread_id=None,
                        requested_cwd=self.members[0], requested_workspaces=self.members,
                        target_session='project-po', target_agent='orchestrator',
                        prompt='Read the saved project brief.', queue_id=None,
                        settings_digest=None, new_settings_digest=None, error=None)
        def update(job_id, expected, status, **values):
            self.assertEqual(job_id, self.job['id'])
            self.assertEqual(expected, self.job['status'])
            self.job.update(values, status=status)
            self.transitions.append(status)
            return copy.deepcopy(self.job)
        self.state = SimpleNamespace(reset_update=update,
                                     reset_job=lambda job_id: copy.deepcopy(self.job),
                                     connection_metadata=lambda: {})
        self.worker = ResetWorker(Bridge(self.state, self.rpc))
        request = self.rpc.request
        def scoped_start(method, params):
            result = request(method, params)
            if method == 'thread/start':
                result['model'] = params['model']
                result['reasoningEffort'] = params.get('config', {}).get('model_reasoning_effort', result['reasoningEffort'])
                result['cwd'] = params['cwd']
                result['runtimeWorkspaceRoots'] = list(params['runtimeWorkspaceRoots'])
                result['activePermissionProfile'] = {'id':params['permissions'], 'extends':None}
                result['sandbox'] = dict(type='workspaceWrite', writableRoots=params['runtimeWorkspaceRoots'][1:], networkAccess=False,
                                         excludeTmpdirEnvVar=False, excludeSlashTmp=False)
            return result
        self.rpc.request = scoped_start

    def test_project_scope_uses_start_settings_without_rollout(self):
        self.worker.advance(copy.deepcopy(self.job))
        self.assertEqual(self.job['status'], 'completed')
        self.assertEqual(self.job['new_settings_digest'], self.job['settings_digest'])
        self.assertIn('ready', self.transitions)
        self.assertEqual(len(self.rpc.history[self.rpc.new]), 1)
        settings = next(result for method, result in self.rpc.calls if method == 'thread/start')
        self.assertEqual(settings['runtimeWorkspaceRoots'], self.members)
        self.assertEqual(sum(method == 'thread/start' for method, _ in self.rpc.calls), 1)

    def test_project_scope_intentional_model_effort_override_is_in_digest_and_start(self):
        self.job.update(requested_model='gpt-6.1-sol', requested_effort='high')
        self.worker.advance(copy.deepcopy(self.job))
        self.assertEqual(self.job['status'], 'completed')
        start = next(params for method, params in self.rpc.calls if method == 'thread/start')
        self.assertEqual(start['model'], 'gpt-6.1-sol')
        self.assertEqual(start['config']['model_reasoning_effort'], 'high')

    def test_project_scope_returned_model_mismatch_withholds_prompt(self):
        self.job.update(requested_model='gpt-6.1-sol', requested_effort='high')
        request = self.rpc.request
        def mismatched_start(method, params):
            result = request(method, params)
            if method == 'thread/start': result['model'] = 'other-model'
            return result
        self.rpc.request = mismatched_start
        self.worker.advance(copy.deepcopy(self.job))
        self.assertEqual(self.job['status'], 'created')
        self.assertIn('settings differ', self.job['error'])
        self.assertEqual(self.rpc.history[self.rpc.new], [])

    def test_missing_extra_or_reordered_secondary_roots_withhold_prompt(self):
        for roots in ([self.members[0]], self.members[1:] + ['/synthetic/extra'], list(reversed(self.members[1:]))):
            with self.subTest(roots=roots):
                self.setUp()
                request = self.rpc.request
                def mismatched_start(method, params):
                    result = request(method, params)
                    if method == 'thread/start': result['sandbox']['writableRoots'] = roots
                    return result
                self.rpc.request = mismatched_start
                self.worker.advance(copy.deepcopy(self.job))
                self.assertEqual(self.job['status'], 'created')
                self.assertIn('settings differ', self.job['error'])
                self.assertEqual(self.rpc.history[self.rpc.new], [])

    def test_mismatched_ordered_roots_withhold_prompt(self):
        request = self.rpc.request
        def reversed_start(method, params):
            result = request(method, params)
            if method == 'thread/start':
                result['runtimeWorkspaceRoots'].reverse()
            return result
        self.rpc.request = reversed_start
        self.worker.advance(copy.deepcopy(self.job))
        self.assertEqual(self.job['status'], 'created')
        self.assertIn('settings differ', self.job['error'])
        self.assertNotIn('ready', self.transitions)
        self.assertEqual(self.rpc.history[self.rpc.new], [])

    def test_retained_created_job_uses_captured_digest_without_new_start(self):
        settings, digest = self.worker.creation_settings(self.rpc.original, self.job)
        created = self.rpc.request('thread/start', self.worker.start_params(settings))
        self.job.update(status='created', new_thread_id=created['thread']['id'],
                        settings_digest=digest, new_settings_digest=self.worker.settings_data(created)[1])
        self.rpc.calls.clear()
        self.worker.advance(copy.deepcopy(self.job))
        self.assertEqual(self.job['status'], 'completed')
        self.assertFalse(any(method == 'thread/start' for method, _ in self.rpc.calls))

    def test_missing_captured_digest_is_not_inferred(self):
        settings, digest = self.worker.creation_settings(self.rpc.original, self.job)
        created = self.rpc.request('thread/start', self.worker.start_params(settings))
        self.job.update(status='created', new_thread_id=created['thread']['id'], settings_digest=digest)
        self.worker.advance(copy.deepcopy(self.job))
        self.assertEqual(self.job['status'], 'created')
        self.assertIn('no rollout found', self.job['error'])
        self.assertNotIn('ready', self.transitions)
        self.assertEqual(self.rpc.history[self.rpc.new], [])


class WorkspaceSettingsTests(unittest.TestCase):
    def test_ordered_explicit_members_change_digest_and_narrow_project_grants(self):
        from agent_chat.creation_settings import project_scope_settings, settings_data
        source = ResetRpc(str(uuid.uuid4())).original
        members = ['/synthetic/inlst-core', '/synthetic/inlst-api']
        job = {'requested_cwd':members[0], 'requested_workspaces':members, 'project_scope':True}
        settings = project_scope_settings(source, job, members[0])
        self.assertEqual(settings['runtimeWorkspaceRoots'], members)
        self.assertEqual(settings['sandbox']['writableRoots'], members[1:])
        self.assertEqual(settings['activePermissionProfile'], {'id':':workspace','extends':None})
        reversed_settings = copy.deepcopy(settings); reversed_settings['runtimeWorkspaceRoots'].reverse()
        self.assertNotEqual(settings_data(settings)[1], settings_data(reversed_settings)[1])
        self.assertEqual(source['runtimeWorkspaceRoots'], ['/tmp/test-project'])

    def test_singleton_project_scope_has_no_explicit_writable_root(self):
        from agent_chat.creation_settings import project_scope_settings
        source = ResetRpc(str(uuid.uuid4())).original
        settings = project_scope_settings(source, {'requested_cwd':'/synthetic/project', 'requested_workspaces':['/synthetic/project'], 'project_scope':True}, '/synthetic/project')
        self.assertEqual(settings['runtimeWorkspaceRoots'], ['/synthetic/project'])
        self.assertEqual(settings['sandbox']['writableRoots'], [])

    def test_singleton_workspace_profile_creation_has_no_inherited_writable_roots(self):
        from agent_chat.creation_settings import creation_settings
        source = ResetRpc(str(uuid.uuid4())).original
        source['sandbox']['writableRoots'].append('/synthetic/parent-secondary')
        desired = creation_settings(source, {'requested_cwd':'/synthetic/project',
                                             'requested_workspaces':['/synthetic/project']}, '/synthetic/project')
        self.assertEqual(desired['runtimeWorkspaceRoots'], ['/synthetic/project'])
        self.assertEqual(desired['sandbox']['writableRoots'], [])
        self.assertEqual(source['sandbox']['writableRoots'], ['/tmp/test-project', '/synthetic/parent-secondary'])

    def test_non_workspace_profile_creation_preserves_its_sandbox(self):
        from agent_chat.creation_settings import creation_settings
        source = ResetRpc(str(uuid.uuid4())).original
        source['activePermissionProfile'] = {'id':'approved', 'extends':':workspace'}
        source['sandbox']['writableRoots'].append('/synthetic/approved-secondary')
        desired = creation_settings(source, {'requested_cwd':'/synthetic/project',
                                             'requested_workspaces':['/synthetic/project']}, '/synthetic/project')
        self.assertEqual(desired['sandbox'], source['sandbox'])
