"""Named policy relocation and receipt-free, read-only creation revalidation."""
import contextlib
import copy
import io
import os
from pathlib import Path
import uuid
from unittest import mock

from agent_chat import cli
from agent_chat.bridge import Bridge
from agent_chat.bridge_client import RemoteBridgeState
from agent_chat.creation_settings import creation_settings, revalidation_proof, settings_data
from agent_chat.core import CoordError
from agent_chat.remote import RemoteCoordError
from agent_chat.rpc import RpcError, TransportError
from test_remote_web import RemoteWebFixture, TOKEN
from test_session_reset import ResetRpc


class NamedProfileRpc(ResetRpc):
    def __init__(self, old):
        super().__init__(old)
        self.original['activePermissionProfile'] = {'id': 'approved', 'extends': ':workspace'}
        self.original['sandbox']['writableRoots'] = ['/tmp/agent-chat-state']
        self.profile = {'approved': {'extends': ':workspace', 'workspace_roots': {self.original['cwd']: True},
                                    'filesystem': {'/tmp/agent-chat-state': 'write'}, 'network': {'enabled': False}}}
        self.target_profile = copy.deepcopy(self.profile)
        self.unmaterialized = True

    def request(self, method, params):
        if method == 'config/read':
            self.calls.append((method, copy.deepcopy(params)))
            profile = self.profile if params['cwd'] == self.original['cwd'] else self.target_profile
            return {'config': {'permissions': copy.deepcopy(profile)}}
        result = super().request(method, params)
        if method == 'thread/start':
            for key in ('model', 'cwd', 'runtimeWorkspaceRoots'):
                if key in params: result[key] = params[key]
            result['reasoningEffort'] = params.get('config', {}).get('model_reasoning_effort', result['reasoningEffort'])
            if result['cwd'] != self.original['cwd'] and not self.changed_permissions:
                result['sandbox']['writableRoots'].append(self.original['cwd'])
        return result


class WorkspaceProfileRpc(ResetRpc):
    """Native start returns the effective singleton :workspace policy."""
    def __init__(self, old):
        super().__init__(old)
        self.original['activePermissionProfile'] = {'id': ':workspace', 'extends': None}
        self.original['sandbox']['writableRoots'].append('/tmp/parent-secondary')
        self.unmaterialized = True

    def request(self, method, params):
        result = super().request(method, params)
        if method == 'thread/start':
            for key in ('model', 'cwd', 'runtimeWorkspaceRoots'):
                result[key] = params[key]
            result['reasoningEffort'] = params.get('config', {}).get('model_reasoning_effort', result['reasoningEffort'])
            result['activePermissionProfile'] = {'id': params['permissions'], 'extends': None}
            result['sandbox'] = dict(type='workspaceWrite', writableRoots=params['runtimeWorkspaceRoots'][1:], networkAccess=False)
        return result


class CreationWorkspaceTests(RemoteWebFixture):
    def setUp(self):
        super().setUp()
        self.call('register', agent='orchestrator')
        self.old, self.request_id = str(uuid.uuid4()), str(uuid.uuid4())
        self.call('bind', thread=self.old)
        self.rpc = NamedProfileRpc(self.old)
        self.state = RemoteBridgeState(self.client, self.url, {'owner': 'workspace-worker', 'secret': 'x'*40, 'host_id': 'mac'})
        self.state.call('acquire')
        self.bridge = Bridge(self.state, self.rpc)

    def create(self):
        self.call('context')
        return self.call('agent-create', agent='world', expected_thread=self.old, prompt='Read checkpoint, do bounded work.',
                         request_id=self.request_id, confirm=True, model='gpt-6.1-sol', reasoning_effort='high', cwd=str(Path(self.tmp.name).resolve()))

    def status(self):
        return self.call('session-reset-status', to='world')

    def mutation_calls(self):
        return copy.deepcopy([(method, params) for method, params in self.rpc.calls
                              if method in ('thread/start', 'thread/queue/add', 'thread/queue/start', 'thread/queue/delete')])

    def mark_project_orchestrator(self, session='a', root='root'):
        from agent_chat.core import Coordinator
        with contextlib.closing(Coordinator(self.db)) as coord:
            coord.db.execute('CREATE TABLE IF NOT EXISTS project_orchestrators (session_id TEXT PRIMARY KEY, root_session TEXT NOT NULL)')
            coord.db.execute('INSERT OR REPLACE INTO project_orchestrators VALUES(?,?)', (session, root))

    def mark_root_projection(self, session='root'):
        from agent_chat.core import Coordinator
        with contextlib.closing(Coordinator(self.db)) as coord:
            coord.db.execute('''CREATE TABLE IF NOT EXISTS root_projections (
                session_id TEXT PRIMARY KEY, home_project TEXT NOT NULL, home_session TEXT NOT NULL,
                thread_id TEXT NOT NULL, cwd TEXT)''')
            coord.db.execute('INSERT OR REPLACE INTO root_projections VALUES(?,?,?,?,?)',
                             (session, 'home', session, self.old, str(Path(self.tmp.name).resolve())))

    def creation_row(self, job_id):
        from agent_chat.core import Coordinator
        with contextlib.closing(Coordinator(self.db)) as coord:
            return dict(coord.db.execute('SELECT * FROM session_resets WHERE id=?', (job_id,)).fetchone())

    def legacy_created(self):
        job = self.create()
        source, _ = settings_data(self.rpc.original)
        desired = creation_settings(source, job, job['requested_cwd'])
        job = self.state.reset_update(job['id'], 'pending', 'creating', settings_digest=settings_data(desired)[1])
        from agent_chat.reset_worker import ResetWorker
        result = self.rpc.request('thread/start', ResetWorker.start_params(desired))
        job = self.state.reset_update(job['id'], 'creating', 'created', new_thread_id=result['thread']['id'], new_settings_digest=settings_data(result)[1])
        self.bridge.tick()
        self.assertIn('settings differ', self.status()['resets'][0]['error'])
        self.assertIsNone(self.status()['thread_id'])
        self.call('agent-status', refresh=True, expected_thread=self.old, request_id=str(uuid.uuid4()))
        self.bridge.tick()
        return job

    def proof(self, job):
        return revalidation_proof(self.rpc, job)

    def revalidate(self, proof, session='a', host='mac', **extra):
        self.call('context', session=session, host=host)
        return self.call('session-reset-revalidate', session=session, host=host, id=self.request_id, proof=proof, confirm=True, **extra)

    def run_cli(self):
        env = dict(AGENT_CHAT_SERVER=self.url, AGENT_CHAT_API_TOKEN=TOKEN, AGENT_CHAT_SESSION='a')
        with mock.patch.dict(os.environ, env, clear=True), mock.patch('agent_chat.rpc.RpcClient', return_value=contextlib.nullcontext(self.rpc)), mock.patch('agent_chat.remote.client_host_id', return_value='mac'), contextlib.redirect_stdout(io.StringIO()):
            return cli.main(['session-reset-revalidate', self.request_id, '--confirm'])

    def test_new_workspace_preserves_existing_grants_and_exact_runtime_settings(self):
        job = self.create(); self.bridge.tick()
        status = self.status()
        self.assertEqual(status['resets'][0]['status'], 'completed')
        self.assertEqual(status['id'], job['target_session'])
        self.assertEqual(status['thread_id'], self.rpc.new)
        self.assertEqual(len(self.rpc.history[self.rpc.new]), 1)
        self.assertEqual(sum(method == 'thread/start' for method, _ in self.rpc.calls), 1)
        self.assertEqual(self.rpc.history[self.old], [])

    def test_changed_target_profile_withholds_creation_before_thread_start(self):
        for key, value in [('network', {'enabled': True}), ('workspace_roots', {'/unapproved': True}), ('filesystem', {'/unapproved': 'write'})]:
            with self.subTest(key=key):
                self.rpc.target_profile = copy.deepcopy(self.rpc.profile)
                self.rpc.target_profile['approved'][key] = value
                if not self.status_exists(): self.create()
                self.bridge.tick()
                self.assertEqual(self.status()['resets'][0]['status'], 'pending')
                self.assertIn('profile differs', self.status()['resets'][0]['error'])
                self.assertFalse(any(method == 'thread/start' for method, _ in self.rpc.calls))

    def status_exists(self):
        return any(row['agent'] == 'world' for row in self.call('agents')['agents'])

    def test_owner_cli_recovers_same_created_candidate_once_without_replacement(self):
        job = self.legacy_created()
        self.assertEqual(self.run_cli(), 0)
        self.assertTrue(self.status()['resets'][0]['workspace_revalidated'])
        self.assertEqual(self.run_cli(), 0)  # Lost response/repeated command is read-only.
        self.bridge.tick()
        self.assertEqual(self.run_cli(), 0)
        self.assertEqual(self.status()['resets'][0]['status'], 'completed')
        self.assertEqual(self.status()['thread_id'], job['new_thread_id'])
        self.assertEqual(len(self.rpc.history[self.rpc.new]), 1)
        self.assertEqual(sum(method == 'thread/start' for method, _ in self.rpc.calls), 1)

    def test_project_orchestrator_revalidates_its_own_unbound_created_candidate(self):
        job = self.legacy_created()
        self.mark_project_orchestrator()
        before = self.creation_row(job['id'])
        self.assertIsNone(self.status()['thread_id'])
        proof = self.proof(job)
        before_mutations = self.mutation_calls()
        self.revalidate(proof)
        after = self.creation_row(job['id'])
        row = self.status()['resets'][0]
        self.assertTrue(row['workspace_revalidated'])
        self.assertEqual(row['id'], job['id'])
        self.assertEqual(row['new_thread_id'], job['new_thread_id'])
        self.assertIsNone(self.status()['thread_id'])
        self.assertEqual(after['settings_digest'], before['new_settings_digest'])
        self.assertEqual((after['id'], after['target_session'], after['old_thread_id'], after['new_thread_id'],
                          after['new_settings_digest'], after['requested_model'], after['requested_effort']),
                         (before['id'], before['target_session'], before['old_thread_id'], before['new_thread_id'],
                          before['new_settings_digest'], before['requested_model'], before['requested_effort']))
        self.assertEqual(self.rpc.history[self.rpc.new], [])
        self.assertFalse(any(method == 'thread/queue/add' for method, _ in self.rpc.calls))
        self.assertEqual(self.mutation_calls(), before_mutations)

    def test_unbound_project_candidate_rejects_other_project_orchestrator(self):
        job = self.legacy_created(); proof = self.proof(job)
        self.mark_project_orchestrator()
        self.call('register', session='b', agent='peer')
        self.mark_project_orchestrator('b')
        with self.assertRaisesRegex(RemoteCoordError, 'original requester'):
            self.revalidate(proof, session='b')
        self.assertFalse(self.status()['resets'][0]['workspace_revalidated'])
        self.assertIsNone(self.status()['thread_id'])
        self.assertEqual(self.rpc.history[self.rpc.new], [])

    def test_unbound_project_candidate_rejects_root_and_project_scope(self):
        job = self.legacy_created(); proof = self.proof(job)
        self.mark_project_orchestrator()
        self.call('register', session='root', agent='global-root')
        self.mark_root_projection()
        with self.assertRaisesRegex(RemoteCoordError, 'original requester'):
            self.revalidate(proof, session='root')
        from agent_chat.core import Coordinator
        with contextlib.closing(Coordinator(self.db)) as coord:
            coord.db.execute('UPDATE session_resets SET project_scope=1 WHERE id=?', (job['id'],))
        with self.assertRaisesRegex(RemoteCoordError, 'unbound revalidation'):
            self.revalidate(proof)
        self.assertFalse(self.creation_row(job['id'])['workspace_revalidated'])
        self.assertIsNone(self.status()['thread_id'])
        self.assertEqual(self.rpc.history[self.rpc.new], [])

    def test_bound_project_candidate_uses_normal_management_then_withholds(self):
        job = self.legacy_created(); proof = self.proof(job)
        self.mark_project_orchestrator()
        from agent_chat.core import Coordinator
        with contextlib.closing(Coordinator(self.db)) as coord:
            coord.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?,?,?)',
                             (job['target_session'], job['new_thread_id'], 'a', '/root/world', 1.0))
        with self.assertRaisesRegex(RemoteCoordError, 'unexpected binding'):
            self.revalidate(proof)
        self.assertFalse(self.creation_row(job['id'])['workspace_revalidated'])
        self.assertEqual(self.rpc.history[self.rpc.new], [])

    def test_wrong_requester_host_candidate_and_confirmation_rejected(self):
        job = self.legacy_created(); proof = self.proof(job)
        self.call('register', session='b', agent='peer')
        with self.assertRaisesRegex(RemoteCoordError, 'original requester'):
            self.revalidate(proof, session='b')
        with self.assertRaisesRegex(RemoteCoordError, 'different remote host'):
            self.call('session-reset-revalidate-info', host='other', id=job['id'])
        with self.assertRaisesRegex(RemoteCoordError, 'confirmation'):
            self.call('session-reset-revalidate', id=job['id'], proof=proof, confirm=False)
        proof['thread_id'] = str(uuid.uuid4())
        with self.assertRaisesRegex(RemoteCoordError, 'exact candidate'):
            self.revalidate(proof)
        self.assertIsNone(self.status()['thread_id'])

    def test_changed_source_permissions_and_target_profile_cannot_revalidate(self):
        job = self.legacy_created(); proof = self.proof(job)
        changed = copy.deepcopy(proof); changed['source']['approvalPolicy'] = 'never'
        with self.assertRaisesRegex(RemoteCoordError, 'original creation settings changed'):
            self.revalidate(changed)
        changed = copy.deepcopy(proof); changed['target_profiles']['approved']['network']['enabled'] = True
        with self.assertRaisesRegex(RemoteCoordError, 'profile differs'):
            self.revalidate(changed)
        changed = copy.deepcopy(proof)
        for key in ('source_profiles', 'target_profiles'):
            changed[key]['approved']['workspace_roots']['/unapproved'] = True
        with self.assertRaisesRegex(RemoteCoordError, 'not present in original'):
            self.revalidate(changed)
        self.assertIsNone(self.status()['thread_id'])

    def test_forged_implicit_source_root_and_stale_bridge_observation_rejected(self):
        job = self.legacy_created(); proof = self.proof(job)
        # cwd and runtime roots are explicitly replaced by creation; the legacy
        # desired digest alone cannot anchor their prior implicit permissions.
        forged = copy.deepcopy(proof)
        forged['source']['cwd'] = '/forged-root'
        forged['source']['runtimeWorkspaceRoots'] = ['/forged-root']
        with self.assertRaisesRegex(RemoteCoordError, 'authenticated bridge observation'):
            self.revalidate(forged)
        from agent_chat.core import Coordinator
        with contextlib.closing(Coordinator(self.db)) as coord:
            coord.db.execute('UPDATE agent_control_observations SET checked_at=0')
        with self.assertRaisesRegex(RemoteCoordError, 'refresh the original requester'):
            self.revalidate(proof)
        self.assertIsNone(self.status()['thread_id'])

    def test_actual_permission_mismatch_stays_withheld(self):
        self.rpc.changed_permissions = True
        job = self.legacy_created()
        with self.assertRaisesRegex(RemoteCoordError, 'captured candidate settings'):
            self.revalidate(self.proof(job))
        self.assertFalse(self.rpc.history[self.rpc.new])

    def test_existing_input_busy_or_unknown_history_withholds_cli_revalidation(self):
        job = self.legacy_created()
        self.rpc.queues[self.rpc.new] = [{'id': 'other'}]
        with self.assertRaisesRegex(CoordError, 'queue is not empty'): self.run_cli()
        self.rpc.queues[self.rpc.new] = []
        self.rpc.history[self.rpc.new] = [{'type': 'userMessage'}]
        with self.assertRaisesRegex(CoordError, 'input history'): self.run_cli()
        self.rpc.history[self.rpc.new] = []
        self.rpc.status[self.rpc.new] = 'active'
        with self.assertRaisesRegex(CoordError, 'not idle'): self.run_cli()
        self.rpc.status[self.rpc.new] = 'idle'
        original = self.rpc.request
        def failed(method, params):
            if method == 'thread/turns/list': raise RpcError(-32600, 'unrelated failure')
            return original(method, params)
        with mock.patch.object(self.rpc, 'request', side_effect=failed), self.assertRaisesRegex(CoordError, 'unrelated failure'):
            self.run_cli()
        self.assertFalse(self.status()['resets'][0]['workspace_revalidated'])
        self.assertIsNone(self.status()['thread_id'])

    def test_worker_rechecks_admission_after_revalidation_before_binding(self):
        job = self.legacy_created(); self.revalidate(self.proof(job))
        self.rpc.history[self.rpc.new] = [{'type': 'userMessage', 'content': 'unexpected'}]
        self.bridge.tick()
        self.assertEqual(self.status()['resets'][0]['status'], 'created')
        self.assertIsNone(self.status()['thread_id'])
        self.assertFalse(any(method == 'thread/queue/add' for method, _ in self.rpc.calls))


class WorkspaceProfileCreationTests(RemoteWebFixture):
    """Ordinary agent-create keeps only the child's explicit roots."""
    def setUp(self):
        super().setUp()
        self.call('register', agent='orchestrator')
        self.old, self.request_id = str(uuid.uuid4()), str(uuid.uuid4())
        self.call('bind', thread=self.old)
        self.rpc = WorkspaceProfileRpc(self.old)
        self.state = RemoteBridgeState(self.client, self.url, {'owner': 'workspace-profile-worker', 'secret': 'x'*40, 'host_id': 'mac'})
        self.state.call('acquire')
        self.bridge = Bridge(self.state, self.rpc)

    def create(self):
        self.call('context')
        return self.call('agent-create', agent='world', expected_thread=self.old, prompt='Read checkpoint, do bounded work.',
                         request_id=self.request_id, confirm=True, model='gpt-6.1-sol', reasoning_effort='high', cwd=str(Path(self.tmp.name).resolve()))

    def status(self):
        return self.call('session-reset-status', to='world')

    def test_singleton_workspace_child_completes_without_parent_secondary_roots(self):
        job = self.create()
        self.bridge.tick()
        status = self.status()
        self.assertEqual(status['resets'][0]['status'], 'completed')
        self.assertEqual(status['thread_id'], self.rpc.new)
        self.assertEqual(sum(method == 'thread/start' for method, _ in self.rpc.calls), 1)
        start = next(params for method, params in self.rpc.calls if method == 'thread/start')
        self.assertEqual(start['runtimeWorkspaceRoots'], [str(Path(self.tmp.name).resolve())])
        self.assertEqual(len(self.rpc.history[self.rpc.new]), 1)
        self.assertEqual(self.rpc.history[self.rpc.old], [])

    def test_singleton_workspace_mismatches_withhold_binding_and_prompt(self):
        for field, value in (
            ('model', 'other-model'),
            ('reasoningEffort', 'low'),
            ('sandbox', {'type':'dangerFullAccess'}),
            ('runtimeWorkspaceRoots', ['/tmp/not-the-child']),
        ):
            with self.subTest(field=field):
                self.setUp()
                request = self.rpc.request
                def mismatched_start(method, params):
                    result = request(method, params)
                    if method == 'thread/start': result[field] = copy.deepcopy(value)
                    return result
                self.rpc.request = mismatched_start
                self.create(); self.bridge.tick()
                self.assertEqual(self.status()['resets'][0]['status'], 'created')
                self.assertIn('settings differ', self.status()['resets'][0]['error'])
                self.assertIsNone(self.status()['thread_id'])
                self.assertEqual(self.rpc.history[self.rpc.new], [])
