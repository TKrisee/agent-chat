"""Cold ordinary delivery after real coordinator turnover in a disposable store."""
import os
from pathlib import Path
import time
from types import SimpleNamespace
import uuid
from unittest import mock

from agent_chat import root_lifecycle
from agent_chat.bridge import Bridge
from agent_chat.bridge_client import RemoteBridgeState
from agent_chat.core import CoordError, Coordinator
from agent_chat.remote import HttpClient
import test_project_activity as activity_fixture
import test_remote_web as web_fixture


class UnloadedProvider(activity_fixture.ObservedProvider):
    def __init__(self):
        super().__init__()
        self.unloaded = set()
        self.calls = []

    def request(self, method, params, **kwargs):
        thread = params.get('threadId')
        self.calls.append((method, thread))
        if thread in self.unloaded:
            if method == 'thread/read':
                return {'thread': {'id': thread, 'status': {'type': 'notLoaded'}},
                        'canAcceptDirectInput': False}
            if method == 'thread/resume':
                self.unloaded.remove(thread)
        return super().request(method, params, **kwargs)


class DirectRootColdDeliveryTests(web_fixture.RemoteWebFixture):
    install_local_provider = activity_fixture.ActivityWorkflow.install_local_provider
    root = activity_fixture.ActivityWorkflow.root

    def setUp(self):
        super().setUp()
        self.install_local_provider()
        self.provider = UnloadedProvider()
        self.call('register', agent='root')
        self.root_thread = str(uuid.uuid4())
        self.call('bind', thread=self.root_thread)
        neutral = Path(self.tmp.name).resolve() / 'neutral'
        neutral.mkdir()
        self.provider.add(self.root_thread, str(neutral))
        identity = dict(owner='direct-cold-fixture', secret='x'*40, host_id='mac')
        home = RemoteBridgeState(self.client, self.url, identity)
        home.call('acquire')
        self.call('agent-status', refresh=True, expected_thread=self.root_thread,
                  request_id=str(uuid.uuid4()))
        Bridge(home, self.provider).tick(observe_policies=False)
        self.root('enroll', expected_thread=self.root_thread, confirm=True)

        project_uuid = uuid.UUID('5fb8d3d13ec348e0993b25a3f3169289')
        with mock.patch('agent_chat.projects.uuid', SimpleNamespace(uuid4=lambda: project_uuid)):
            self.project = self.client.call('/api/projects/rpc',
                                            dict(op='create', name='Direct delivery'))['project']['id']
        self.project_client = HttpClient(self.url, web_fixture.TOKEN, project=self.project)
        self.state = RemoteBridgeState(self.project_client, self.url, identity)
        self.state.call('acquire')
        self.bridge = Bridge(self.state, self.provider)
        workspace = Path(self.tmp.name).resolve() / 'project'
        workspace.mkdir()
        self.call('context')
        configured = self.root('configure', project=self.project, agent='former-po', cwd=str(workspace),
            prompt='Configure an isolated project.', request_id=str(uuid.uuid4()),
            confirm=True, model='test-model', effort='high')['creation']
        self.bridge.tick(observe_policies=False)
        self.assertEqual(self.state.reset_job(configured['id'])['status'], 'completed')
        self.project_root = self.root('route', project=self.project)['session']
        preview = self.root('team-reset-preview', project=self.project, message_ids=[])
        reset_id = str(uuid.uuid4())
        reset = self.root('team-reset', project=self.project, message_ids=[], request_id=reset_id,
                          expected_digest=preview['digest'], confirm=True)
        self.assertEqual(reset['status'], 'completed')
        self.call('context')
        created = self.root('direct-create', project=self.project, reset_id=reset_id, agent='direct-worker',
            cwd=str(workspace), prompt='Await a fixture message.', request_id=str(uuid.uuid4()),
            confirm=True, model='test-model', effort='high')['creation']
        self.bridge.tick(observe_policies=False)
        job = self.state.reset_job(created['id'])
        self.assertEqual(job['status'], 'completed')
        self.worker, self.worker_thread = created['target_session'], job['new_thread_id']
        self.coord = Coordinator(self.server.projects.db_path(self.project), self.project_root)
        self.addCleanup(self.coord.close)
        self.provider.calls.clear()
        self.provider.unloaded.add(self.worker_thread)
        self.project_call('context')
        self.source = self.project_call('send', to=self.worker, body='Original ordinary message')['id']
        self.state.heartbeat(self.url, os.getpid())
        self.assertEqual(self.wakes(), [])

    def project_call(self, op, session=None, **params):
        return self.project_client.call('/api/coord', dict(op=op, session=session or self.project_root,
                                                           host_id='mac', params=params))

    def wakes(self):
        return self.coord.db.execute('''SELECT j.id,j.status FROM bridge_jobs j
            JOIN bridge_deliveries d ON d.job_id=j.id WHERE d.message_id=?''', (self.source,)).fetchall()

    def assert_cold_withheld(self):
        self.assertFalse(any(job.get('source_id') == self.source for job in self.state.supervisor_jobs()))
        self.assertEqual(self.wakes(), [])

    def test_unloaded_direct_worker_dispatches_the_original_message_once(self):
        self.bridge.tick(observe_policies=False)
        self.bridge.tick(observe_policies=False)
        self.assertEqual(self.provider.calls.count(('thread/resume', self.worker_thread)), 1)
        self.assertEqual(self.provider.calls.count(('thread/queue/add', self.worker_thread)), 1)
        wake, = self.wakes()
        self.assertEqual(wake['status'], 'dispatched')
        self.assertEqual(sum(item.get('clientId') == wake['id']
                             for item in self.provider.history[self.worker_thread]), 1)
        self.assertIsNone(self.coord.db.execute('SELECT acked_at FROM messages WHERE id=?',
                                               (self.source,)).fetchone()['acked_at'])

    def test_wrong_or_retired_manager_cannot_load_a_worker(self):
        self.project_call('register', session='unassigned', agent='unassigned')
        self.coord.db.execute('UPDATE agent_management SET manager_session=? WHERE target_session=?',
                              ('unassigned', self.worker))
        self.assert_cold_withheld()
        self.coord.db.execute('UPDATE agent_management SET manager_session=? WHERE target_session=?',
                              (self.project_root, self.worker))
        original_call = self.state.call

        def retire_manager_before_resume(op, **params):
            if op == 'execution-epoch-begin' and params.get('method') == 'thread/resume':
                self.coord.db.execute('''INSERT INTO retired_sessions
                    SELECT id,agent,registered_at,? FROM sessions WHERE id=?''',
                    (time.time(), self.project_root))
                self.coord.db.execute('DELETE FROM sessions WHERE id=?', (self.project_root,))
            return original_call(op, **params)

        with mock.patch.object(self.state, 'call', side_effect=retire_manager_before_resume):
            with self.assertRaisesRegex(CoordError, 'native epoch rejected before submission'):
                self.bridge.tick(observe_policies=False)
        self.assertNotIn(('thread/resume', self.worker_thread), self.provider.calls)
        self.assertNotIn(('thread/queue/add', self.worker_thread), self.provider.calls)
        self.assertEqual(self.coord.db.execute('SELECT status FROM cold_message_loads WHERE source_id=?',
                                              (self.source,)).fetchone()['status'], 'rejected')

    def test_protected_target_and_wrong_store_cannot_load(self):
        self.coord.db.execute('INSERT INTO operator_identities VALUES(?)', (self.worker,))
        self.assert_cold_withheld()
        self.coord.db.execute('DELETE FROM operator_identities WHERE session_id=?', (self.worker,))
        self.coord.db.execute('''INSERT INTO root_projections(session_id,home_project,home_session,thread_id)
            VALUES(?,?,?,?)''', (self.worker, 'default', 'a', self.worker_thread))
        self.assert_cold_withheld()
        self.coord.db.execute('DELETE FROM root_projections WHERE session_id=?', (self.worker,))
        # Probe the local authorization in a store with discordant durable identity.
        from agent_chat.supervisor import SupervisorState
        self.coord.db.execute("UPDATE meta SET value='foreign-project' WHERE key='project_id'")
        self.assertIsNone(SupervisorState(self.coord)._cold_owner(self.worker))

    def test_worker_and_project_stops_withhold_cold_wakes(self):
        self.coord.db.execute('INSERT INTO agent_controls VALUES(?,?,?)',
                              (self.worker, 'stopped', time.time()))
        self.assert_cold_withheld()
        self.coord.db.execute('DELETE FROM agent_controls WHERE target_session=?', (self.worker,))
        root_lifecycle.pause(self.coord, self.project_root, str(uuid.uuid4()))
        self.assert_cold_withheld()
