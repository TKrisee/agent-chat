import copy
import contextlib
import sqlite3
import tempfile
import time
import unittest
import uuid
from pathlib import Path
from unittest import mock
from agent_chat import root_lifecycle, root_team_reset, remote_service
from agent_chat.bridge_state import BridgeState
from agent_chat.bridge_lease import BridgeManager
from agent_chat.core import Coordinator, CoordError, assert_agent_running
from agent_chat.creation_settings import project_scope_settings
from agent_chat.reset_worker import ResetWorker
from agent_chat.rpc import TransportError
from agent_chat.session_reset import SessionResetState
from test_session_reset import ResetRpc

PROJECT = 'project_disposable_target'

class ParkedRpc(ResetRpc):
    def request(self, method, params):
        if method == 'model/list':
            self.calls.append((method, copy.deepcopy(params)))
            return {'data': [{'model': 'gpt-6.2-sol', 'supportedReasoningEfforts': [{'reasoningEffort': 'high'}]}]}
        if method == 'thread/name/set':
            self.calls.append((method, copy.deepcopy(params)))
            self.named = (params['threadId'], params['name'], str(Path(self.workspace) / 'fake-parked-rollout'))
            Path(self.named[2]).write_bytes(b'metadata')
            self.unmaterialized = False
            return {}
        result = super().request(method, params)
        if method == 'thread/read' and getattr(self, 'named', None) and params['threadId'] == self.named[0]:
            result['thread'].update(name=self.named[1], path=self.named[2])
        if method == 'thread/start' or (method == 'thread/resume' and params.get('threadId') == self.new):
            result = project_scope_settings(result, {'requested_model': 'gpt-6.2-sol', 'requested_effort': 'high', 'requested_cwd': self.workspace, 'requested_workspaces': [self.workspace]}, self.workspace)
            result['thread'] = {'id': self.new}
            if self.changed_permissions:
                result['sandbox'] = {'type': 'dangerFullAccess'}
        return result

class State:
    def __init__(self, resets): self.resets = resets
    def reset_update(self, job, expected, status, **values): return self.resets.update(job,expected,status,**values)
    def reset_job(self, job): return self.resets.job(job)

class FakeBridge:
    def __init__(self, resets, rpc): self.state,self.rpc = State(resets),rpc
    def epoch_request(self, job, method, params): return self.rpc.request(method,params)
    def queue(self, thread): return self.rpc.request('thread/queue/list',{'threadId':thread})['data']

class ParkedProvisionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.workspace = str(Path(self.tmp.name).resolve())
        self.coord = Coordinator(Path(self.workspace)/'state.sqlite3','a'); self.addCleanup(self.coord.close)
        self.coord.register('root')
        self.thread = str(uuid.uuid4())
        BridgeState(self.coord).bind(self.thread)
        remote_service._schema(self.coord)
        db = self.coord.db
        db.execute('INSERT INTO remote_session_hosts VALUES(?,?,?,?)',('a','mac',time.time(),time.time()))
        db.execute('CREATE TABLE IF NOT EXISTS root_projections (session_id TEXT PRIMARY KEY,home_project TEXT,home_session TEXT,thread_id TEXT,cwd TEXT,paused INTEGER,model TEXT,effort TEXT)')
        db.execute('INSERT INTO root_projections VALUES(?,?,?,?,?,?,?,?)',('a','default','a',self.thread,self.workspace,0,None,None))
        db.execute('CREATE TABLE IF NOT EXISTS project_orchestrators (session_id TEXT PRIMARY KEY,root_session TEXT NOT NULL)')
        root_lifecycle.schema(db); root_team_reset.schema(db)
        self.stop,self.reset,self.request = (str(uuid.uuid4()) for _ in range(3))
        db.execute('INSERT INTO root_team_stops VALUES(?,?,?,?,?)',(self.stop,'a',1,time.time(),None))
        db.execute('INSERT INTO root_team_reset_requests VALUES(?,?,?,?,?,?,?,?,?)',(self.reset,'a',PROJECT,'digest','[]','{}','completed',time.time(),time.time()))
        self.resets = SessionResetState(self.coord)
        self.rpc = ParkedRpc(self.thread); self.rpc.workspace = self.workspace
        self.bridge = FakeBridge(self.resets,self.rpc)

    def create(self, **changes):
        args = dict(agent='new-po',template_thread=self.thread,prompt='retained brief',request_id=self.request,confirmed=True,model='gpt-6.2-sol',reasoning_effort='high',cwd=self.workspace,project_scope=True,project_orchestrator=True,workspaces=[self.workspace],_direct_scope=(PROJECT,self.reset),_park_stop=self.stop)
        args.update(changes)
        return self.resets.create(**args)

    def test_parked_binding_exact_retry_and_no_input(self):
        created = self.create()
        self.assertEqual(self.create()['target_session'],created['target_session'])
        with self.assertRaisesRegex(CoordError,'retry payload'): self.create(prompt='different')
        self.assertEqual(self.resets.jobs('mac',True,True,True),[])
        self.coord.db.execute('INSERT INTO root_parked_activations VALUES(?)',(self.request,))
        self.assertEqual(self.resets.jobs('mac',True,True,False),[])
        job = self.resets.jobs('mac',True,True,True)[0]
        ResetWorker(self.bridge).advance(job)
        job = self.resets.job(self.request)
        self.assertEqual(job['status'],'parked',job['error'])
        self.assertEqual(job['prompt'],'retained brief')
        self.assertEqual(self.coord.db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?',(job['target_session'],)).fetchone()[0],self.rpc.new)
        with self.assertRaises(CoordError): self.resets.update(self.request,'parked','adding')
        self.assertEqual(len([m for m,p in self.rpc.calls if m == 'thread/start']),1)
        self.assertFalse([m for m,p in self.rpc.calls if m in ('thread/queue/add','thread/queue/start')])
        self.assertEqual(self.rpc.history[self.rpc.new],[])
        self.coord.db.execute('UPDATE root_team_stops SET active=0 WHERE id=?',(self.stop,))
        self.assertTrue(root_lifecycle.stopped(self.coord.db,job['target_session']))
        with self.assertRaises(CoordError): assert_agent_running(self.coord.db,job['target_session'])

    def test_scope_and_stop_are_checked_before_creation_and_dispatch(self):
        with self.assertRaises(CoordError): self.create(_park_stop=str(uuid.uuid4()))
        with self.assertRaises(CoordError): self.create(_direct_scope=('wrong-project',self.reset))
        with self.assertRaises(CoordError): self.create(_direct_scope=(PROJECT,str(uuid.uuid4())))
        self.create()
        self.coord.db.execute('INSERT INTO root_parked_activations VALUES(?)',(self.request,))
        self.coord.db.execute('UPDATE root_team_stops SET active=0 WHERE id=?',(self.stop,))
        self.assertEqual(self.resets.jobs('mac',True,True,True),[])
        with self.assertRaises(CoordError): self.resets.update(self.request,'pending','creating')
        self.assertFalse([m for m,p in self.rpc.calls if m == 'thread/start'])

    def test_unsupported_model_and_lost_create_never_replay(self):
        self.create()
        self.coord.db.execute('INSERT INTO root_parked_activations VALUES(?)',(self.request,))
        job = self.resets.jobs('mac',True,True,True)[0]
        original = self.rpc.request
        def unsupported(method,params):
            if method == 'model/list': return {'data': []}
            return original(method,params)
        with mock.patch.object(self.rpc,'request',side_effect=unsupported):
            ResetWorker(self.bridge).advance(job)
        self.assertEqual(self.resets.job(self.request)['status'],'pending')
        self.assertFalse([m for m,p in self.rpc.calls if m == 'thread/start'])
        self.rpc.drop = 'thread/start'
        with self.assertRaises(TransportError): ResetWorker(self.bridge).advance(self.resets.job(self.request))
        self.assertEqual(self.resets.job(self.request)['status'],'uncertain')
        with self.assertRaisesRegex(CoordError,'no prompt retry admission'):
            self.resets.retry_prompt(self.request,True)
        ResetWorker(self.bridge).advance(self.resets.job(self.request))
        self.assertEqual(len([m for m,p in self.rpc.calls if m == 'thread/start']),1)
        self.resets.resolve_creation(self.request,self.rpc.new,True)
        ResetWorker(self.bridge).advance(self.resets.job(self.request))
        self.assertEqual(self.resets.job(self.request)['status'],'parked')
        self.assertFalse([m for m,p in self.rpc.calls if m in ('thread/queue/add','thread/queue/start')])

    def test_superseded_reset_or_wrong_actor_withholds_dispatch(self):
        self.create()
        self.coord.db.execute('INSERT INTO root_parked_activations VALUES(?)',(self.request,))
        self.coord.db.execute('INSERT INTO root_team_reset_requests VALUES(?,?,?,?,?,?,?,?,?)',
            (str(uuid.uuid4()),'a',PROJECT,'digest','[]','{}','completed',time.time(),time.time()))
        self.assertEqual(self.resets.jobs('mac',True,True,True),[])
        with self.assertRaises(CoordError): self.resets.update(self.request,'pending','creating')
        with self.assertRaises(CoordError): self.create()

    def test_settings_mismatch_never_binds_or_sends_input(self):
        self.create()
        self.coord.db.execute('INSERT INTO root_parked_activations VALUES(?)',(self.request,))
        self.rpc.changed_permissions = True
        ResetWorker(self.bridge).advance(self.resets.jobs('mac',True,True,True)[0])
        job = self.resets.job(self.request)
        self.assertEqual(job['status'],'created')
        self.assertIsNone(self.coord.db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?',(job['target_session'],)).fetchone())
        self.assertFalse([m for m,p in self.rpc.calls if m in ('thread/queue/add','thread/queue/start')])

    def test_lost_registry_commit_does_not_replay_native_creation(self):
        self.create()
        self.coord.db.execute('INSERT INTO root_parked_activations VALUES(?)',(self.request,))
        # The local CAS committed but its caller never received registry commit.
        self.resets.update(self.request,'pending','creating',settings_digest='x')
        ResetWorker(self.bridge).advance(self.resets.job(self.request))
        self.assertEqual(self.resets.job(self.request)['status'],'uncertain')
        self.assertFalse([m for m,p in self.rpc.calls if m == 'thread/start'])

    def test_bridge_requires_current_registry_and_capability(self):
        job = self.create()
        self.coord.db.execute('INSERT INTO root_parked_activations VALUES(?)',(self.request,))
        registry_path = Path(self.workspace)/'registry.sqlite3'
        class Registry:
            @contextlib.contextmanager
            def connection(inner):
                db = sqlite3.connect(registry_path, isolation_level=None)
                db.row_factory = sqlite3.Row
                try: yield db
                finally: db.close()
        class Roots: projects = Registry()
        with Roots.projects.connection() as db:
            db.executescript('''CREATE TABLE root_projects(project_id TEXT,session_id TEXT,orchestrator_session TEXT,joined INTEGER,request_id TEXT);
                CREATE TABLE root_team_operations(id TEXT,root_session TEXT,resumed_at REAL);
                CREATE TABLE root_team_operation_projects(request_id TEXT,project_id TEXT,root_session TEXT);''')
            db.execute('INSERT INTO root_projects VALUES(?,?,?,?,?)',(PROJECT,'a',job['target_session'],1,self.request))
            db.execute('INSERT INTO root_team_operations VALUES(?,?,NULL)',(self.stop,'a'))
            db.execute('INSERT INTO root_team_operation_projects VALUES(?,?,?)',(self.stop,PROJECT,'a'))
        manager = BridgeManager(self.coord.path); self.addCleanup(manager.close)
        manager.roots,manager.project_id = Roots(),PROJECT
        identity = dict(owner='parked-test',secret='x'*40,host_id='mac')
        def call(op,**params): return manager.dispatch(dict(identity,op=op,params=params))
        call('acquire')
        old = ['project-scope-v1','project-workspaces-v1']
        current = old + ['parked-provision-v1', 'turnover-bootstrap-v1', 'parked-durability-v1']
        self.assertEqual(call('reset-jobs',capabilities=old),[])
        self.assertEqual(call('reset-jobs',capabilities=old + ['parked-provision-v1']),[])
        self.assertEqual(len(call('reset-jobs',capabilities=current)),1)
        with Roots.projects.connection() as db:
            db.execute('UPDATE root_projects SET orchestrator_session=?',('changed',))
        self.assertEqual(call('reset-jobs',capabilities=current),[])
        with self.assertRaisesRegex(CoordError,'registry scope changed'):
            call('reset-update',job_id=self.request,expected='pending',status='creating',settings_digest='x',capabilities=current)
        with Roots.projects.connection() as db:
            db.execute('UPDATE root_projects SET orchestrator_session=?',(job['target_session'],))
        call('reset-update',job_id=self.request,expected='pending',status='creating',settings_digest='x',capabilities=current)
        with Roots.projects.connection() as db:
            db.execute('UPDATE root_projects SET orchestrator_session=?',('changed',))
        candidate = str(uuid.uuid4())
        call('reset-update',job_id=self.request,expected='creating',status='created',new_thread_id=candidate,new_settings_digest='x',capabilities=current)
        with self.assertRaisesRegex(CoordError,'registry scope changed'):
            call('reset-update',job_id=self.request,expected='created',status='parked',new_settings_digest='x',capabilities=current)
        self.assertIsNone(self.coord.db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?',(job['target_session'],)).fetchone())
