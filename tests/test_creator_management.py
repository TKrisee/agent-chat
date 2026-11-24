"""Authenticated creator-to-manager lifecycle in private project fixtures."""
import contextlib
import os
import sqlite3
import tempfile
import threading
import unittest
import uuid
from pathlib import Path
from unittest import mock

from agent_chat import core, management
from agent_chat.measurement import MeasurementStore
from agent_chat.rpc import TransportError
from agent_chat.remote import HttpClient, RemoteCoordError
from test_remote_web import TOKEN
from test_root_orchestrator import ScopedRpc
from test_root_local import RootLocalTests
from agent_chat.bridge import Bridge
from agent_chat.bridge_client import RemoteBridgeState
from agent_chat.web import create_server


class CreatorManagementTests(RootLocalTests):
    def setUp(self):
        scratch = tempfile.TemporaryDirectory(prefix='agent-chat-creator-management-')
        self.addCleanup(scratch.cleanup)
        self.private = Path(scratch.name)
        self.sessions_root = self.private / 'native-sessions'; self.sessions_root.mkdir()
        self.stack = contextlib.ExitStack(); self.addCleanup(self.stack.close)
        self.stack.enter_context(mock.patch.object(core, 'default_db',
            side_effect=AssertionError('default DB is outside this fixture')))
        self.stack.enter_context(mock.patch('test_root_local.MeasurementStore',
            side_effect=lambda database: MeasurementStore(database, sessions_root=self.sessions_root)))
        self.stack.enter_context(mock.patch.dict(os.environ, {
            'AGENT_CHAT_STATE_DIR': str(self.private / 'client-state'),
            'AGENT_CHAT_DB': str(self.private / 'unused-explicit-db.sqlite3'),
            'AGENT_CHAT_HOST_ID': 'host', 'AGENT_CHAT_PROJECT': 'default',
            'AGENT_CHAT_SESSION': 'root', 'AGENT_CHAT_API_TOKEN': TOKEN,
        }))
        # Base fixture patches both installed-config resolvers to its own
        # temporary plist/wrapper/database before any supported local Root call.
        super().setUp()
        self.enroll(); self.consume()
        self.project_id = self.projects.create('Creator Management Fixture',
            workspace=str(self.workspace), instructions='Fixture project only.')['id']
        self.consume()
        configured = self.call('configure', project=self.project_id, agent='fixture-po',
            cwd=str(self.workspace), prompt='Create and manage only your fixture workers.',
            request_id=str(uuid.uuid4()), confirm=True, model='test-model', effort='high')
        self.po = configured['creation']['target_session']
        self.server = create_server(self.master, host='127.0.0.1', port=0,
            api_token=TOKEN, sessions_root=self.sessions_root)
        self.addCleanup(self.stop_server)
        self.server_thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.server_thread.start()
        self.url = 'http://127.0.0.1:' + str(self.server.server_port)
        self.po_client = HttpClient(self.url, TOKEN, project=self.project_id)
        identity = dict(owner='creator-management-fixture', secret='x'*40, host_id='host')
        self.project_state = RemoteBridgeState(self.po_client, self.url, identity)
        self.project_state.call('acquire')
        self.rpc = ScopedRpc(self.thread, str(self.neutral))
        self.project_bridge_instance = Bridge(self.project_state, self.rpc)
        self.project_bridge_instance.tick()
        status = self.po_call('session-reset-status', to=self.po)
        self.assertEqual(status['resets'][0]['status'], 'completed')
        self.po_thread = status['thread_id']
        self.rpc.status[self.po_thread] = 'idle'  # Owned provider fixture ends bootstrap turn.
        self.po_call('context')

    def stop_server(self):
        self.server.stop_event.set(); self.server.shutdown(); self.server.server_close()
        self.server_thread.join(3)
        self.assertFalse(self.server_thread.is_alive())

    def po_call(self, operation, session=None, host='host', **params):
        return self.po_client.call('/api/coord', dict(op=operation, session=session or self.po,
            host_id=host, params=params))

    def create_worker(self, name='fixture-worker', **extra):
        self.po_call('context')
        params=dict(agent=name, expected_thread=self.po_thread,
            prompt='Create a benign fixture result; report to the exact creator.',
            request_id=str(uuid.uuid4()), confirm=True)
        params.update(extra)
        created=self.po_call('agent-create', **params)
        self.project_bridge_instance.tick()
        status=self.po_call('session-reset-status', to=created['target_session'])
        self.assertEqual(status['resets'][0]['status'], 'completed')
        self.rpc.status[status['thread_id']]='idle'
        return created, status['thread_id'], params

    def test_creator_can_hold_own_completed_creation_without_root_delegation(self):
        created, thread, params=self.create_worker()
        target=created['target_session']
        self.po_call('context')
        request=str(uuid.uuid4())
        held=self.po_call('agent-stop', to=target, expected_thread=thread, request_id=request, confirm=True)
        self.assertEqual(held['id'],request)
        self.project_bridge_instance.tick()
        status=self.po_call('agent-status', to=target)
        self.assertEqual(status['admission'],'stopped')
        self.assertEqual(status['manager_session'],self.po)
        self.assertIsNone(status['parent_session'])
        self.assertEqual(status['thread_id'],thread)
        self.assertEqual(status['requests'][0]['status'],'completed')
        retry=self.po_call('agent-create', **params)
        self.assertEqual(retry['target_session'],target)
        with contextlib.closing(core.Coordinator(self.server.projects.db_path(self.project_id))) as coord:
            self.assertFalse(coord.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='agent_management_requests'").fetchone()
                and coord.db.execute('SELECT 1 FROM agent_management_requests').fetchone(), 'creation is not a fabricated Root delegation')
            audit=coord.db.execute('SELECT * FROM agent_management_creations WHERE request_id=?',(created['id'],)).fetchone()
            self.assertEqual((audit['creator_session'],audit['target_session'],audit['creator_thread'],audit['host_id']),
                (self.po,target,self.po_thread,'host'))
            self.assertEqual(audit['root_session'],self.call('route',project=self.project_id)['session'])
            self.assertEqual(coord.db.execute('SELECT count(*) FROM agent_management_creations').fetchone()[0],1)
            for statement in ('UPDATE agent_management_creations SET host_id=\'other\'','DELETE FROM agent_management_creations'):
                with self.assertRaisesRegex(sqlite3.IntegrityError,'immutable'):
                    coord.db.execute(statement)

    def test_retained_main_and_other_actor_are_not_implicitly_owned(self):
        self.po_call('register',session='retained-fixture',agent='retained-fixture')
        retained_thread=str(uuid.uuid4())
        self.po_call('bind',session='retained-fixture',thread=retained_thread)
        self.po_call('context')
        with self.assertRaisesRegex(RemoteCoordError,'delegate-management'):
            self.po_call('agent-stop',to='retained-fixture',expected_thread=retained_thread,
                request_id=str(uuid.uuid4()),confirm=True)
        created,thread,_=self.create_worker()
        self.po_call('context',session='retained-fixture')
        with self.assertRaisesRegex(RemoteCoordError,'own lifecycle'):
            self.po_call('agent-stop',session='retained-fixture',to=created['target_session'],
                expected_thread=thread,request_id=str(uuid.uuid4()),confirm=True)
        self.assertEqual(self.po_call('agent-status',to=created['target_session'])['manager_session'],self.po)
        self.assertIsNone(self.po_call('agent-status',to='retained-fixture')['manager_session'])

    def test_retained_main_still_uses_real_explicit_root_delegation(self):
        target='retained-for-root-grant'; thread=str(uuid.uuid4())
        self.po_call('register',session=target,agent=target)
        self.po_call('bind',session=target,thread=thread)
        self.po_call('context')
        with self.assertRaisesRegex(RemoteCoordError,'delegate-management'):
            self.po_call('agent-stop',to=target,expected_thread=thread,request_id=str(uuid.uuid4()),confirm=True)
        self.consume()
        request=str(uuid.uuid4())
        grant=self.call('delegate-management',project=self.project_id,po=self.po,target=target,
            expected_thread=thread,expected_host='host',request_id=request,confirm=True)
        self.assertEqual(grant['id'],request)
        held=self.po_call('agent-stop',to=target,expected_thread=thread,request_id=str(uuid.uuid4()),confirm=True)
        self.assertEqual(held['target_session'],target)
        with contextlib.closing(core.Coordinator(self.server.projects.db_path(self.project_id))) as coord:
            self.assertEqual(coord.db.execute('SELECT request_id FROM agent_management WHERE target_session=?',(target,)).fetchone()[0],request)
            self.assertTrue(coord.db.execute('SELECT 1 FROM agent_management_requests WHERE id=?',(request,)).fetchone())
            self.assertFalse(coord.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='agent_management_creations'").fetchone())

    def test_duplicate_name_and_foreign_retry_cannot_claim_existing_worker(self):
        created,thread,params=self.create_worker()
        self.po_call('context')
        with self.assertRaisesRegex(RemoteCoordError,'name is already registered'):
            self.po_call('agent-create',**dict(params,request_id=str(uuid.uuid4())))
        self.po_call('register',session='foreign-creator',agent='foreign-creator')
        with self.assertRaisesRegex(RemoteCoordError,'retry payload differs'):
            self.po_call('agent-create',session='foreign-creator',**params)
        self.assertEqual(self.po_call('agent-status',to=created['target_session'])['manager_session'],self.po)
        with contextlib.closing(core.Coordinator(self.server.projects.db_path(self.project_id))) as coord:
            self.assertEqual(coord.db.execute('SELECT count(*) FROM agent_management_creations').fetchone()[0],1)
            self.assertEqual(coord.db.execute('SELECT request_id FROM agent_management WHERE target_session=?',
                (created['target_session'],)).fetchone()[0],created['id'])

    def test_exact_thread_host_payload_and_current_role_boundaries(self):
        created,thread,params=self.create_worker()
        target=created['target_session']; self.po_call('context')
        for changes in ({'expected_thread':str(uuid.uuid4())},{'host':'foreign-host'}):
            with self.subTest(changes=changes),self.assertRaises(RemoteCoordError):
                self.po_call('agent-stop',**dict(dict(to=target,expected_thread=thread,
                    request_id=str(uuid.uuid4()),confirm=True),**changes))
        with self.assertRaisesRegex(RemoteCoordError,'retry payload differs'):
            self.po_call('agent-create',**dict(params,prompt='Changed request'))
        with self.assertRaisesRegex(RemoteCoordError,'exact main-conversation binding'):
            self.po_call('agent-create',**dict(params,request_id=str(uuid.uuid4()),
                expected_thread=str(uuid.uuid4()),agent='wrong-template'))
        with contextlib.closing(core.Coordinator(self.server.projects.db_path(self.project_id))) as coord:
            # Remove only a fixture role to model revoked PO designation; never
            # manufacture a manager grant or authorizing provenance.
            coord.db.execute('DELETE FROM project_orchestrators WHERE session_id=?',(self.po,))
        with self.assertRaisesRegex(RemoteCoordError,'own lifecycle'):
            self.po_call('agent-stop',to=target,expected_thread=thread,request_id=str(uuid.uuid4()),confirm=True)
        status=self.po_call('agent-status',to=target)
        self.assertEqual(status['requests'],[])
        self.assertEqual(status['admission'],'running')

    def test_creation_transaction_rolls_back_management_and_new_identity_together(self):
        request=str(uuid.uuid4()); record=management.record_creation
        def interrupted(coord,request_id):
            record(coord,request_id)
            raise core.CoordError('owned fixture interrupts before creation commit')
        with mock.patch.object(management,'record_creation',side_effect=interrupted):
            with self.assertRaisesRegex(RemoteCoordError,'before creation commit'):
                self.po_call('agent-create',agent='interrupted-worker',expected_thread=self.po_thread,
                    prompt='Inert fixture',request_id=request,confirm=True)
        with contextlib.closing(core.Coordinator(self.server.projects.db_path(self.project_id))) as coord:
            self.assertIsNone(coord.db.execute('SELECT 1 FROM sessions WHERE agent=?',('interrupted-worker',)).fetchone())
            self.assertIsNone(coord.db.execute('SELECT 1 FROM session_resets WHERE id=?',(request,)).fetchone())
            for table in ('agent_management','agent_management_creations'):
                if coord.db.execute('SELECT 1 FROM sqlite_master WHERE type=\'table\' AND name=?',(table,)).fetchone():
                    self.assertEqual(coord.db.execute('SELECT count(*) FROM '+table).fetchone()[0],0)

    def test_lost_native_creation_response_stays_unknown_and_is_not_replayed(self):
        request=str(uuid.uuid4())
        params=dict(agent='uncertain-worker',expected_thread=self.po_thread,prompt='Inert fixture',request_id=request,confirm=True)
        created=self.po_call('agent-create',**params)
        starts=sum(method=='thread/start' for method,_ in self.rpc.calls)
        self.rpc.drop='thread/start'
        with self.assertRaises(TransportError): self.project_bridge_instance.tick()
        self.project_bridge_instance.tick()
        status=self.po_call('session-reset-status',to=created['target_session'])
        self.assertEqual(status['resets'][0]['status'],'uncertain')
        self.assertIsNone(status['thread_id'])
        self.assertEqual(sum(method=='thread/start' for method,_ in self.rpc.calls),starts+1)
        self.assertEqual(self.rpc.history[self.rpc.new],[])
        self.assertEqual(self.po_call('agent-create',**params)['target_session'],created['target_session'])
        self.assertEqual(self.po_call('agent-status',to=created['target_session'])['manager_session'],self.po)
        with self.assertRaises(RemoteCoordError):
            self.po_call('agent-stop',to=created['target_session'],expected_thread=self.rpc.new,
                request_id=str(uuid.uuid4()),confirm=True)

    def test_same_session_reset_retains_creator_and_stop_barrier_still_withholds(self):
        created,thread,_=self.create_worker()
        target=created['target_session']; self.po_call('context')
        reset=self.po_call('session-reset',to=target,expected_thread=thread,
            prompt='Inert same-session reset fixture',request_id=str(uuid.uuid4()),confirm=True)
        self.project_bridge_instance.tick()
        status=self.po_call('session-reset-status',to=target)
        self.assertEqual(status['resets'][0]['id'],reset['id'])
        self.assertEqual(status['resets'][0]['status'],'completed')
        self.assertNotEqual(status['thread_id'],thread)
        self.assertEqual(self.po_call('agent-status',to=target)['manager_session'],self.po)
        self.consume()
        stop=str(uuid.uuid4()); self.call('stop-all',request_id=stop,confirm=True)
        with self.assertRaisesRegex(RemoteCoordError,'stop-all barrier'):
            self.po_call('agent-stop',to=target,expected_thread=status['thread_id'],
                request_id=str(uuid.uuid4()),confirm=True)
        with self.assertRaisesRegex(RemoteCoordError,'stop-all barrier'):
            self.po_call('agent-create',agent='stopped-worker',expected_thread=self.po_thread,
                prompt='Inert fixture',request_id=str(uuid.uuid4()),confirm=True)


def load_tests(loader, tests, pattern):
    return unittest.TestSuite(
        CreatorManagementTests(name) for name in CreatorManagementTests.__dict__ if name.startswith('test_'))
