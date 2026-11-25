"""Disposable Root local administration through the real HTTP bridge lease."""
import contextlib
import time
import uuid
from pathlib import Path
from unittest import mock

from agent_chat.core import Coordinator, CoordError
from agent_chat.rpc import TransportError
from agent_chat import root_team_reset
from test_root_orchestrator import RootOrchestratorTests


class ParkedProvisionHttpTests(RootOrchestratorTests):
    def test_lost_metadata_response_reconciles_through_http_once(self):
        project, route, path, params, state, bridge = self.prepare_parked()
        self.root('direct-create', **params)
        original = self.rpc.request
        rollout = Path(self.workspace) / 'lost-metadata-rollout'
        named = {}
        names = []
        def request(method, values):
            if method == 'model/list':
                return {'data': [{'model': 'gpt-6.2-sol', 'supportedReasoningEfforts': [{'reasoningEffort': 'high'}]}]}
            if method == 'thread/name/set':
                names.append(values['threadId'])
                named[values['threadId']] = values['name']
                rollout.write_bytes(b'metadata')
                raise TransportError('lost metadata response')
            result = original(method, values)
            if method == 'thread/read' and values['threadId'] in named:
                result['thread'].update(name=named[values['threadId']], path=str(rollout))
            return result
        with mock.patch.object(self.rpc, 'request', side_effect=request):
            with self.assertRaises(TransportError):
                bridge.tick()
            bridge.tick()
        with contextlib.closing(Coordinator(path, route['session'])) as coord:
            from agent_chat.session_reset import SessionResetState
            job = SessionResetState(coord).job(params['request_id'])
            self.assertEqual(job['status'], 'parked', job['error'])
            self.assertEqual(coord.db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?', (job['target_session'],)).fetchone()['thread_id'], job['new_thread_id'])
            self.assertFalse(coord.db.execute("SELECT 1 FROM execution_epoch_handoffs WHERE method='thread/name/set' AND settled_at IS NULL").fetchone())
            # Simulate the persisted boundary after parked commit but before
            # epoch settlement, without another native call.
            coord.db.execute("UPDATE execution_epoch_handoffs SET settled_at=NULL,outcome=NULL WHERE method='thread/name/set'")
            state.reset_jobs()
            self.assertFalse(coord.db.execute("SELECT 1 FROM execution_epoch_handoffs WHERE method='thread/name/set' AND settled_at IS NULL").fetchone())
        self.assertEqual(names, [job['new_thread_id']])
        self.assertEqual(self.rpc.queues[job['new_thread_id']], [])
        self.assertEqual(self.rpc.history[job['new_thread_id']], [])

    def prepare_parked(self):
        project = self.project('Disposable stopped project')
        route = self.root('route', project=project)
        path = self.server.roots.path(project)
        # Simulate the persisted post-reset workspace assignment. The target,
        # stop and journal all belong only to this disposable fixture.
        with self.server.projects.connection() as db:
            db.execute('UPDATE root_projects SET cwd=? WHERE project_id=?', (str(self.workspace),project))
        with contextlib.closing(Coordinator(path)) as coord:
            coord.db.execute('UPDATE root_projections SET cwd=? WHERE session_id=?', (str(self.workspace),route['session']))
        self.routed(project,'context')
        self.call('context')
        stop_id,reset_id,setup_id = (str(uuid.uuid4()) for _ in range(3))
        stopped = self.root('stop-all',request_id=stop_id,confirm=True)
        self.assertTrue(stopped['barrier_complete'],stopped['errors'])
        with contextlib.closing(Coordinator(path)) as coord:
            root_team_reset.schema(coord.db)
            coord.db.execute('INSERT INTO root_team_reset_requests VALUES(?,?,?,?,?,?,?,?,?)',
                (reset_id,'a',project,'fixture-digest','[]','{}','completed',time.time(),time.time()))
        params = dict(project=project,reset_id=reset_id,agent='fresh-po',cwd=str(self.workspace),
                      prompt='Retained project roster brief',request_id=setup_id,confirm=True,
                      model='gpt-6.2-sol',effort='high',parked_stop=stop_id)
        state,bridge = self.project_bridge(project)
        return project,route,path,params,state,bridge

    def test_local_root_to_http_bridge_parks_exact_retry_without_input(self):
        project,route,path,params,state,bridge = self.prepare_parked()
        setup_id = params['request_id']
        created = self.root('direct-create',**params)['creation']
        self.assertEqual(self.root('direct-create',**params)['creation']['target_session'],created['target_session'])
        with self.assertRaises(CoordError):
            self.root('direct-create',**dict(params,prompt='different brief'))
        old = ['project-scope-v1','project-workspaces-v1']
        self.assertEqual(state.call('reset-jobs',capabilities=old),[])
        original = self.rpc.request
        rollout = Path(self.workspace) / 'parked-rollout'
        named = {}
        def request(method,values):
            if method == 'model/list':
                self.rpc.calls.append((method,dict(values)))
                return {'data':[{'model':'gpt-6.2-sol','supportedReasoningEfforts':[{'reasoningEffort':'high'}]}]}
            if method == 'thread/name/set':
                self.rpc.calls.append((method,dict(values)))
                named[values['threadId']] = values['name']
                rollout.write_bytes(b'metadata')
                return {}
            result = original(method,values)
            if method == 'thread/read' and values['threadId'] in named:
                result['thread'].update(name=named[values['threadId']],path=str(rollout))
            return result
        with mock.patch.object(self.rpc,'request',side_effect=request):
            bridge.tick()
        with contextlib.closing(Coordinator(path,route['session'])) as coord:
            from agent_chat.session_reset import SessionResetState
            job = SessionResetState(coord).job(setup_id)
            self.assertEqual(job['status'],'parked',job['error'])
            self.assertEqual(job['prompt'],params['prompt'])
            bound = coord.db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?',(job['target_session'],)).fetchone()
            self.assertEqual(bound['thread_id'],job['new_thread_id'])
            self.assertEqual(self.rpc.history[job['new_thread_id']],[])
            self.assertEqual(self.rpc.queues[job['new_thread_id']],[])
        self.assertEqual(self.root('direct-create',**params)['creation']['target_session'],created['target_session'])
        bridge.tick()
        self.assertEqual(len([m for m,_ in self.rpc.calls if m == 'thread/start']),1)
        self.assertFalse([m for m,_ in self.rpc.calls if m in ('thread/queue/add','thread/queue/start')])

    def test_stale_designation_after_native_start_remains_unbound(self):
        project,route,path,params,state,bridge = self.prepare_parked()
        created = self.root('direct-create',**params)['creation']
        original = self.rpc.request
        def request(method,values):
            if method == 'model/list':
                return {'data':[{'model':'gpt-6.2-sol','supportedReasoningEfforts':[{'reasoningEffort':'high'}]}]}
            result = original(method,values)
            if method == 'thread/start':
                with self.server.projects.connection() as db:
                    db.execute('UPDATE root_projects SET orchestrator_session=? WHERE project_id=?',('other',project))
            return result
        with mock.patch.object(self.rpc,'request',side_effect=request):
            with self.assertRaisesRegex(CoordError,'registry scope changed'):
                bridge.tick()
        with contextlib.closing(Coordinator(path,route['session'])) as coord:
            from agent_chat.session_reset import SessionResetState
            job = SessionResetState(coord).job(params['request_id'])
            self.assertEqual(job['status'],'created')
            self.assertTrue(job['new_thread_id'])
            self.assertIsNone(coord.db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?',(created['target_session'],)).fetchone())
            self.assertEqual(self.rpc.history[job['new_thread_id']],[])
        self.assertEqual(len([m for m,_ in self.rpc.calls if m == 'thread/start']),1)
        self.assertFalse([m for m,_ in self.rpc.calls if m in ('thread/queue/add','thread/queue/start')])

    def test_resume_after_created_before_binding_withholds_stale_candidate(self):
        project,route,path,params,state,bridge = self.prepare_parked()
        created = self.root('direct-create',**params)['creation']
        request_rpc = self.rpc.request
        update = state.reset_update
        def request(method,values):
            if method == 'model/list':
                return {'data':[{'model':'gpt-6.2-sol','supportedReasoningEfforts':[{'reasoningEffort':'high'}]}]}
            return request_rpc(method,values)
        def update_and_resume(job_id,expected,status,**values):
            result = update(job_id,expected,status,**values)
            if expected == 'creating' and status == 'created':
                resumed = self.root('resume-project',project=project,request_id=params['parked_stop'],confirm=True)
                self.assertTrue(resumed['barrier_complete'],resumed['errors'])
            return result
        with mock.patch.object(self.rpc,'request',side_effect=request), mock.patch.object(state,'reset_update',side_effect=update_and_resume):
            with self.assertRaises(CoordError): bridge.tick()
        with contextlib.closing(Coordinator(path,route['session'])) as coord:
            from agent_chat.session_reset import SessionResetState
            job = SessionResetState(coord).job(params['request_id'])
            self.assertEqual(job['status'],'created')
            self.assertTrue(job['new_thread_id'])
            self.assertIsNone(coord.db.execute('SELECT 1 FROM bridge_bindings WHERE session_id=?',(created['target_session'],)).fetchone())
            self.assertEqual(self.rpc.history[job['new_thread_id']],[])
        self.assertFalse([m for m,_ in self.rpc.calls if m in ('thread/queue/add','thread/queue/start')])

    def test_lost_response_resolves_exact_existing_empty_candidate(self):
        project,route,path,params,state,bridge = self.prepare_parked()
        created = self.root('direct-create',**params)['creation']
        original = self.rpc.request
        def request(method,values):
            if method == 'model/list':
                return {'data':[{'model':'gpt-6.2-sol','supportedReasoningEfforts':[{'reasoningEffort':'high'}]}]}
            result = original(method,values)
            if method == 'thread/start':
                raise TransportError('fixture lost correlated response')
            return result
        with mock.patch.object(self.rpc,'request',side_effect=request):
            with self.assertRaises(TransportError):
                bridge.tick()
        with contextlib.closing(Coordinator(path,route['session'])) as coord:
            from agent_chat.session_reset import SessionResetState
            self.assertEqual(SessionResetState(coord).job(params['request_id'])['status'],'uncertain')
        candidate = self.rpc.new
        self.routed(project,'session-reset-resolve',id=params['request_id'],thread=candidate,confirm_created=True)
        bridge.tick()
        with contextlib.closing(Coordinator(path,route['session'])) as coord:
            job = SessionResetState(coord).job(params['request_id'])
            self.assertEqual(job['status'],'parked',job['error'])
            self.assertEqual(job['new_thread_id'],candidate)
            self.assertEqual(coord.db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?',(created['target_session'],)).fetchone()[0],candidate)
            self.assertEqual(self.rpc.history[candidate],[])
            from agent_chat.root_team_reset import handoff_pending
            self.assertFalse([h for h in handoff_pending(coord.db) if h['job_id']==params['request_id']])
            epoch = coord.db.execute("SELECT outcome FROM execution_epoch_handoffs WHERE method='thread/start' AND actor=?",(created['target_session'],)).fetchone()
            self.assertEqual(epoch['outcome'],'submitted')
        self.assertEqual(len([m for m,_ in self.rpc.calls if m == 'thread/start']),1)
        self.assertFalse([m for m,_ in self.rpc.calls if m in ('thread/queue/add','thread/queue/start')])

    def test_definite_provider_rejection_records_settled_effect(self):
        project,route,path,params,state,bridge = self.prepare_parked()
        created = self.root('direct-create',**params)['creation']
        self.rpc.reject_create = True
        original = self.rpc.request
        def request(method,values):
            if method == 'model/list':
                return {'data':[{'model':'gpt-6.2-sol','supportedReasoningEfforts':[{'reasoningEffort':'high'}]}]}
            return original(method,values)
        with mock.patch.object(self.rpc,'request',side_effect=request):
            bridge.tick()
        with contextlib.closing(Coordinator(path,route['session'])) as coord:
            from agent_chat.session_reset import SessionResetState
            job = SessionResetState(coord).job(params['request_id'])
            self.assertEqual(job['status'],'failed')
            self.assertIsNone(job['new_thread_id'])
            self.assertEqual(coord.db.execute('SELECT code FROM parked_native_rejections WHERE request_id=?',(params['request_id'],)).fetchone()[0],-32602)
            self.assertIsNone(coord.db.execute('SELECT 1 FROM bridge_bindings WHERE session_id=?',(created['target_session'],)).fetchone())
            from agent_chat.root_team_reset import handoff_pending
            self.assertFalse([h for h in handoff_pending(coord.db) if h['job_id']==params['request_id']])
            epoch = coord.db.execute("SELECT outcome FROM execution_epoch_handoffs WHERE method='thread/start' AND actor=?",(created['target_session'],)).fetchone()
            self.assertEqual(epoch['outcome'],'rejected')
        bridge.tick()
        self.assertEqual(len([m for m,_ in self.rpc.calls if m == 'thread/start']),1)
        self.assertFalse([m for m,_ in self.rpc.calls if m in ('thread/queue/add','thread/queue/start')])
