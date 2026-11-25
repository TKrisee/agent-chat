import os
from pathlib import Path
from unittest import mock
from test_supervisor import SupervisorWorkflowTests

class ColdWakeProbe(SupervisorWorkflowTests):
    def start(self, **kwargs):
        from agent_chat import service, root_local
        for mod,name in ((service,'_installed_server_config'),(service,'configured_server_database'),
                         (root_local,'_installed_server_config'),(root_local,'configured_server_database')):
            guard=mock.patch.object(mod,name,side_effect=AssertionError('installed discovery forbidden'))
            guard.start(); self.addCleanup(guard.stop)
        home=Path(self.tmp.name)/'home'; home.mkdir(exist_ok=True)
        env=mock.patch.dict(os.environ,{'HOME':str(home),'CODEX_HOME':str(home/'.codex')})
        env.start(); self.addCleanup(env.stop)
        sessions=Path(self.tmp.name)/'sessions'; sessions.mkdir(exist_ok=True)
        return super().start(sessions_root=sessions, **kwargs)

from test_creator_management import CreatorManagementTests

class CreatedWorkerColdWakeProbe(CreatorManagementTests):
    def test_root_owned_po_cold_input_dispatches(self):
        root=self.call('route',project=self.project_id)['session']
        self.po_call('context',session=root)
        source=self.po_call('send',session=root,to=self.po,body='New root task')['id']
        thread=self.po_thread
        self.rpc.status[thread]='notLoaded'; self.rpc.calls.clear()
        self.project_state.heartbeat(self.url,os.getpid())
        provider=self.rpc.request; resumed=[]
        def request(method,params):
            result=provider(method,params)
            if method=='thread/resume' and params['threadId']==thread:
                resumed.append(dict(params)); self.rpc.status[thread]='idle'
            return result
        with mock.patch.object(self.rpc,'request',side_effect=request):
            self.project_bridge_instance.tick(observe_policies=False)
        self.assertEqual(resumed,[{'threadId':thread,'excludeTurns':True}])
        self.assertEqual(sum(m=='thread/queue/add' and p.get('threadId')==thread for m,p in self.rpc.calls),1)

    def setup_cold(self, name='fixture-worker'):
        created, thread, _ = self.create_worker(name=name)
        target = created['target_session']
        self.po_call('context')
        source = self.po_call('send',to=target,body='New independent task')['id']
        self.rpc.status[thread] = 'notLoaded'
        self.rpc.calls.clear(); self.project_state.heartbeat(self.url,os.getpid())
        return target,thread,source

    def test_changed_manager_after_selection_cannot_resume(self):
        import contextlib
        from agent_chat.core import Coordinator
        from agent_chat.supervisor_worker import SupervisorWorker
        from agent_chat.remote import RemoteCoordError
        target,thread,source=self.setup_cold()
        job=next(job for job in self.project_state.supervisor_jobs() if job.get('source_id')==source)
        with contextlib.closing(Coordinator(self.server.projects.db_path(self.project_id))) as coord:
            coord.db.execute('DELETE FROM agent_management WHERE target_session=?',(target,))
        with self.assertRaises(RemoteCoordError): SupervisorWorker(self.project_bridge_instance).advance(job)
        self.assertFalse(any(m=='thread/resume' and p.get('threadId')==thread for m,p in self.rpc.calls))

    def test_changed_manager_after_claim_before_native_epoch_cannot_resume(self):
        import contextlib
        from agent_chat.core import Coordinator, CoordError
        target,thread,source=self.setup_cold()
        original=self.project_state.call
        def call(op,**params):
            if op=='execution-epoch-begin' and params.get('method')=='thread/resume':
                with contextlib.closing(Coordinator(self.server.projects.db_path(self.project_id))) as coord:
                    coord.db.execute('DELETE FROM agent_management WHERE target_session=?',(target,))
            return original(op,**params)
        native=[]; provider=self.rpc.request
        def request(method,params):
            if method=='thread/resume' and params.get('threadId')==thread: native.append(method)
            return provider(method,params)
        with self.assertRaisesRegex(CoordError,'native epoch rejected before submission'):
            with mock.patch.object(self.project_state,'call',side_effect=call), mock.patch.object(self.rpc,'request',side_effect=request):
                self.project_bridge_instance.tick(observe_policies=False)
        self.assertFalse(native)
        with contextlib.closing(Coordinator(self.server.projects.db_path(self.project_id))) as coord:
            self.assertEqual(coord.db.execute('SELECT status FROM cold_message_loads WHERE source_id=?',(source,)).fetchone()['status'],'rejected')
            self.assertEqual([row['status'] for row in coord.db.execute('SELECT status FROM cold_message_load_events WHERE source_id=? ORDER BY id',(source,))],['claimed','rejected'])
            from agent_chat.bridge_state import cold_load_pending
            self.assertFalse(cold_load_pending(coord.db,thread))

    def check_route_or_stop_after_claim(self, only):
        import contextlib
        from agent_chat.core import Coordinator
        from agent_chat.core import CoordError
        for change in (only,):
            with self.subTest(change=change):
                target,thread,source=self.setup_cold(name='fixture-'+change)
                original=self.project_state.call
                def call(op,**params):
                    if op=='execution-epoch-begin' and params.get('method')=='thread/resume':
                        with contextlib.closing(Coordinator(self.server.projects.db_path(self.project_id))) as coord:
                            if change=='route':
                                coord.db.execute('UPDATE bridge_bindings SET thread_id=? WHERE session_id=?',(str(__import__('uuid').uuid4()),target))
                            else:
                                coord.db.execute("INSERT OR REPLACE INTO agent_controls VALUES(?,?,?)",(target,'stopped',0))
                    return original(op,**params)
                native=[]; provider=self.rpc.request
                def request(method,params):
                    if method=='thread/resume' and params.get('threadId')==thread: native.append(method)
                    return provider(method,params)
                with self.assertRaisesRegex(CoordError,'native epoch rejected before submission'):
                    with mock.patch.object(self.project_state,'call',side_effect=call), mock.patch.object(self.rpc,'request',side_effect=request):
                        self.project_bridge_instance.tick(observe_policies=False)
                self.assertFalse(native)

    def test_route_change_after_claim_withholds_resume(self):
        self.check_route_or_stop_after_claim('route')

    def test_stop_after_claim_withholds_resume(self):
        self.check_route_or_stop_after_claim('stop')

    def test_stopped_worker_stays_stopped(self):
        target,thread,source=self.setup_cold()
        self.po_call('context')
        self.po_call('agent-stop',to=target,expected_thread=thread,request_id=str(__import__('uuid').uuid4()),confirm=True)
        self.project_bridge_instance.tick(observe_policies=False)
        self.assertNotEqual(self.po_call('agent-status',to=target)['admission'],'running')
        self.assertFalse(any(m=='thread/resume' and p.get('threadId')==thread for m,p in self.rpc.calls))


    def test_existing_native_queue_withholds_load(self):
        target,thread,source=self.setup_cold()
        self.rpc.queues[thread].append({'id':'foreign','clientUserMessageId':'foreign','input':[]})
        self.project_bridge_instance.tick(observe_policies=False)
        self.assertFalse(any(m in ('thread/resume','thread/queue/add','thread/queue/start') for m,_ in self.rpc.calls))

    def test_lost_resume_response_never_repeats_load(self):
        from agent_chat.rpc import TransportError
        other,other_thread,_=self.create_worker(name='independent-worker')
        target,thread,source=self.setup_cold()
        self.po_call('context')
        self.po_call('send',to=other['target_session'],body='Independent task')
        original=self.rpc.request
        def provider(method,params):
            result=original(method,params)
            if method=='thread/resume' and params['threadId']==thread:
                self.rpc.calls.append((method,dict(params)))
                self.rpc.status[thread]='idle'
                raise TransportError('lost resume response after load')
            return result
        with mock.patch.object(self.rpc,'request',side_effect=provider):
            with self.assertRaises(TransportError): self.project_bridge_instance.tick(observe_policies=False)
            self.project_bridge_instance.tick(observe_policies=False)
        self.assertEqual(sum(m=='thread/resume' and p.get('threadId')==thread for m,p in self.rpc.calls),1)
        self.assertFalse(any(m=='thread/queue/add' and p.get('threadId')==thread for m,p in self.rpc.calls))
        self.assertTrue(any(m=='thread/queue/add' and p.get('threadId')==other_thread for m,p in self.rpc.calls))
        import contextlib
        from agent_chat.core import Coordinator
        with contextlib.closing(Coordinator(self.server.projects.db_path(self.project_id))) as coord:
            self.assertEqual(coord.db.execute('SELECT status FROM cold_message_loads WHERE source_id=?',(source,)).fetchone()['status'],'attempting')
            self.assertEqual([row['status'] for row in coord.db.execute('SELECT status FROM cold_message_load_events WHERE source_id=? ORDER BY id',(source,))],['claimed','attempting'])
            self.assertFalse(coord.db.execute('SELECT 1 FROM message_reads WHERE message_id=?',(source,)).fetchone())
            self.assertIsNone(coord.db.execute('SELECT acked_at FROM messages WHERE id=?',(source,)).fetchone()['acked_at'])

    def test_new_input_for_actual_created_worker_with_supervision_off(self):
        created, thread, _ = self.create_worker()
        target=created['target_session']
        status=self.po_call('agent-status',to=target)
        self.assertEqual(status['manager_session'],self.po)
        policy=self.po_call('supervisor-status')
        self.assertFalse(policy['enabled']); self.assertFalse(policy['recovery_enabled'])
        self.po_call('context')
        source=self.po_call('send',to=target,body='New independent task')['id']
        self.rpc.status[thread]='notLoaded'
        self.rpc.calls.clear()
        self.project_state.heartbeat(self.url,os.getpid())
        original = self.rpc.request
        def provider(method, params):
            result = original(method, params)
            if method == 'thread/resume' and params['threadId'] == thread:
                self.rpc.calls.append((method, dict(params)))
                self.rpc.status[thread] = 'idle'
            return result
        from agent_chat import core
        epoch = core.execution_epoch_handoff
        def checked_epoch(db,*args,**kwargs):
            self.assertTrue(db.in_transaction, 'native epoch must retain admission transaction')
            return epoch(db,*args,**kwargs)
        with mock.patch.object(self.rpc, 'request', side_effect=provider), mock.patch.object(core,'execution_epoch_handoff',side_effect=checked_epoch):
            self.project_bridge_instance.tick(observe_policies=False)
        self.assertTrue(any(m=='thread/resume' and p.get('threadId')==thread for m,p in self.rpc.calls),
                        'new task did not rehydrate actual creator-managed worker')
        from agent_chat.core import Coordinator
        import contextlib
        with contextlib.closing(Coordinator(self.server.projects.db_path(self.project_id))) as coord:
            self.assertIsNone(coord.db.execute('SELECT acked_at FROM messages WHERE id=?',(source,)).fetchone()['acked_at'])
            self.assertFalse(coord.db.execute('SELECT 1 FROM message_reads WHERE message_id=?',(source,)).fetchone())
            self.assertEqual([row['status'] for row in coord.db.execute('SELECT status FROM cold_message_load_events WHERE source_id=? ORDER BY id',(source,))],['claimed','attempting','loaded'])
        self.assertEqual(sum(m=='thread/queue/add' and p.get('threadId')==thread for m,p in self.rpc.calls),1)


def load_tests(loader, standard_tests, pattern):
    names = (
        'CreatedWorkerColdWakeProbe.test_root_owned_po_cold_input_dispatches',
        'CreatedWorkerColdWakeProbe.test_new_input_for_actual_created_worker_with_supervision_off',
        'CreatedWorkerColdWakeProbe.test_existing_native_queue_withholds_load',
        'CreatedWorkerColdWakeProbe.test_lost_resume_response_never_repeats_load',
        'CreatedWorkerColdWakeProbe.test_changed_manager_after_selection_cannot_resume',
        'CreatedWorkerColdWakeProbe.test_changed_manager_after_claim_before_native_epoch_cannot_resume',
        'CreatedWorkerColdWakeProbe.test_route_change_after_claim_withholds_resume',
        'CreatedWorkerColdWakeProbe.test_stop_after_claim_withholds_resume',
        'CreatedWorkerColdWakeProbe.test_stopped_worker_stays_stopped',
        'ColdWakeProbe.test_internal_recovery_requires_native_consumption_and_deduplicates',
    )
    import unittest
    return unittest.TestSuite(loader.loadTestsFromName(name, __import__(__name__)) for name in names)
