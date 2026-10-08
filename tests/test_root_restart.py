"""Public root API + existing authenticated host bridge workflow regressions."""
import contextlib
import copy
import uuid
import sqlite3
from unittest import mock
from agent_chat.root_restart import RootRestart

from agent_chat.core import Coordinator
from agent_chat.remote import RemoteCoordError
from agent_chat.rpc import TransportError
import test_root_orchestrator as root_tests
from test_session_reset import ResetRpc


class RootFreshStartTests(root_tests.RootOrchestratorTests):
    # Avoid inheriting the full unrelated root suite.
    def setUp(self):
        super().setUp()
        self.rpc = ResetRpc(self.thread_id)
        self.rpc.original.update(cwd=str(self.neutral),runtimeWorkspaceRoots=[str(self.neutral)])
        self.bridge.rpc=self.rpc
        original=self.rpc.request
        self.publish_consumption=False
        def request(method,params):
            if method=='thread/turns/list' and params['threadId']!=self.thread_id and self.rpc.history.get(params['threadId']):
                self.rpc.calls.append((method,copy.deepcopy(params)))
                return {'data':[dict(id='native-boot-turn',status='inProgress',items=copy.deepcopy(self.rpc.history[params['threadId']]))] if self.publish_consumption else [],'nextCursor':None}
            return original(method,params)
        self.rpc.request=request

    def fresh(self,rid,prompt='Read the checkpoint. Keep all stopped teams stopped.'):
        return self.root('fresh-start',expected_thread=self.thread_id,prompt=prompt,request_id=rid,confirm=True)

    def status(self,rid):
        return self.root('fresh-status',request_id=rid)

    def test_fresh_root_waits_for_idle_and_actual_boot_consumption(self):
        project=self.project()
        stop=str(uuid.uuid4());self.root('stop-all',request_id=stop,confirm=True)
        before=self.server.roots.config()
        self.rpc.status[self.thread_id]='active'
        rid=str(uuid.uuid4());self.fresh(rid)
        self.bridge.tick()
        self.assertEqual(self.status(rid)['status'],'pending')
        self.assertFalse(any(method=='thread/start' for method,_ in self.rpc.calls))
        self.rpc.status[self.thread_id]='idle'
        self.bridge.tick()
        job=self.status(rid)
        self.assertEqual(job['status'],'starting',job)
        self.assertNotEqual(job['new_thread_id'],self.thread_id)
        self.assertEqual(self.server.roots.config()['home_session'],before['home_session'])
        self.assertEqual(self.server.roots.config()['thread_id'],job['new_thread_id'])
        for item in self.server.roots.status()['projects']:
            if not item['joined']: continue
            with contextlib.closing(Coordinator(self.server.roots.path(item['project_id']))) as coord:
                binding=coord.db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?',(item['session_id'],)).fetchone()
                self.assertEqual(binding['thread_id'],job['new_thread_id'])
        self.assertIsNone(job['native_proof_json'])
        self.assertNotIn('operator_report',job)
        boot=self.rpc.history[job['new_thread_id']][0]['content'][0]['text']
        self.assertIn('Agent communication: concise normal grammar despite inherited style.',boot)
        self.assertIn('Human/operator replies: Caveman full unless explicitly changed.',boot)
        self.assertIn('owner/request IDs, relationships, acceptance versus completion/consumption',boot)
        self.publish_consumption=True
        self.bridge.tick()
        completed=self.status(rid)
        self.assertEqual(completed['status'],'completed')
        report=completed['operator_report']
        self.assertEqual(report['request_id'],rid)
        self.assertEqual(report['new_thread_id'],job['new_thread_id'])
        self.assertIn('use /resume and select',report['cli_guidance'])
        self.assertIn('plain/local CLI cannot switch endpoint',report['cli_guidance'])
        self.assertIn('Only after status completed',boot)
        self.assertEqual(self.status(rid)['operator_report'],report)
        self.assertEqual(len([call for call in self.rpc.calls if call[0]=='thread/start']),1)
        self.assertEqual(len([call for call in self.rpc.calls if call[0]=='thread/queue/add']),1)
        self.assertTrue(self.root('stop-status',request_id=stop)['barrier_complete'])
        with self.server.projects.connection() as db:
            self.assertIsNone(db.execute('SELECT resumed_at FROM root_team_operations WHERE id=?',(stop,)).fetchone()['resumed_at'])

    def test_unknown_creation_does_not_replay_native_start(self):
        rid=str(uuid.uuid4());self.fresh(rid);self.rpc.drop='thread/start'
        with self.assertRaises(TransportError): self.bridge.tick()
        self.bridge.tick()
        job=self.status(rid)
        self.assertEqual(job['status'],'uncertain',job)
        self.assertIsNone(job['new_thread_id'])
        self.assertEqual(len([call for call in self.rpc.calls if call[0]=='thread/start']),1)
        self.assertFalse(any(method=='thread/queue/add' for method,_ in self.rpc.calls))

    def test_ambiguous_boot_start_never_replays_retained_queue_entry(self):
        rid=str(uuid.uuid4());self.fresh(rid)
        original=self.rpc.request
        def request(method,params):
            if method=='thread/queue/start':
                self.rpc.calls.append((method,copy.deepcopy(params)))
                raise TransportError('lost queue-start response; retained queue is not nonexecution proof')
            return original(method,params)
        self.rpc.request=request
        with self.assertRaises(TransportError): self.bridge.tick()
        self.bridge.tick();self.bridge.tick()
        self.assertEqual(self.status(rid)['status'],'uncertain')
        self.assertEqual(len([call for call in self.rpc.calls if call[0]=='thread/queue/start']),1)
        self.assertEqual(len(self.rpc.queues[self.rpc.new]),1)

    def test_partial_gate_release_retries_without_native_replay(self):
        self.project()
        rid=str(uuid.uuid4());self.fresh(rid);self.bridge.tick()
        self.publish_consumption=True
        release=RootRestart.release_gate
        calls=[]
        def fail_second(instance,request_id,item):
            calls.append(item['project_id'])
            if len(calls)==2: raise sqlite3.OperationalError('simulated project gate release failure')
            return release(instance,request_id,item)
        with mock.patch.object(RootRestart,'release_gate',fail_second):
            self.bridge.tick()
        self.assertEqual(self.status(rid)['status'],'finalizing')
        before=len(self.rpc.calls)
        self.bridge.tick()
        self.assertEqual(self.status(rid)['status'],'completed')
        self.assertFalse(any(method in ('thread/start','thread/queue/add','thread/queue/start') for method,_ in self.rpc.calls[before:]))
        for item in RootRestart(self.server.roots).scope(rid):
            with contextlib.closing(Coordinator(self.server.roots.path(item['project_id']))) as coord:
                self.assertEqual(coord.db.execute('SELECT active FROM root_fresh_gates WHERE request_id=?',(rid,)).fetchone()['active'],0)

    def test_protected_operator_boot_and_prepared_unread_relocation(self):
        self.call('register',session='reporter',agent='reporter')
        message=self.call('send',session='reporter',to='a',body='Unread report retained across fresh root.')
        wake=self.state.prepare(self.thread_id,[dict(id=message['id'],recipient_session='a')],'old prepared root payload')
        rid=str(uuid.uuid4())
        payload=dict(expected_thread=self.thread_id,prompt='Keep stopped teams stopped; read the retained inbox.',request_id=rid,confirm=True)
        code,_,_=self.request('POST','/api/root/fresh-start',payload,auth='basic')
        self.assertEqual(code,403)
        code,_,_=self.request('POST','/api/root/fresh-start',payload,auth='basic',headers={'X-Agent-Chat-CSRF':self.server.csrf_token})
        self.assertEqual(code,200)
        self.assertEqual(self.status(rid)['instruction_origin'],'authenticated_operator_ui')
        self.bridge.tick()
        job=self.status(rid)
        self.assertEqual(job['status'],'starting',job)
        with contextlib.closing(Coordinator(self.db)) as coord:
            retained=coord.db.execute('SELECT * FROM bridge_jobs WHERE id=?',(wake['id'],)).fetchone()
            self.assertEqual(retained['status'],'prepared')
            self.assertEqual(retained['thread_id'],job['new_thread_id'])
            self.assertIsNone(coord.db.execute('SELECT acked_at FROM messages WHERE id=?',(message['id'],)).fetchone()['acked_at'])
            self.assertEqual(coord.db.execute('SELECT job_id FROM bridge_deliveries WHERE message_id=?',(message['id'],)).fetchone()['job_id'],wake['id'])
        with self.server.projects.connection() as db:
            audit=db.execute('SELECT * FROM root_fresh_relocations WHERE request_id=? AND job_id=?',(rid,wake['id'])).fetchone()
            self.assertEqual(audit['old_payload'],'old prepared root payload')
            self.assertIn(job['new_thread_id'],audit['new_payload'])
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute('DELETE FROM root_fresh_relocations WHERE request_id=?',(rid,))
        self.assertEqual(len([call for call in self.rpc.calls if call[0]=='thread/queue/add']),1)

    def test_old_native_queue_and_late_active_turn_withhold_creation_or_switch(self):
        rid=str(uuid.uuid4());self.fresh(rid)
        self.rpc.queues[self.thread_id]=[dict(id='old-native',clientUserMessageId='external',input=[])]
        self.bridge.tick()
        self.assertEqual(self.status(rid)['status'],'pending')
        self.assertFalse(any(method=='thread/start' for method,_ in self.rpc.calls))
        self.rpc.queues[self.thread_id]=[]
        original=self.rpc.request
        def request(method,params):
            result=original(method,params)
            if method=='thread/start': self.rpc.status[self.thread_id]='active'
            return result
        self.rpc.request=request
        self.bridge.tick()
        self.assertEqual(self.status(rid)['status'],'created')
        self.assertEqual(self.server.roots.config()['thread_id'],self.thread_id)
        self.assertFalse(any(method=='thread/queue/add' for method,_ in self.rpc.calls))

    def test_changed_fresh_settings_or_nonempty_history_withhold_boot(self):
        rid=str(uuid.uuid4());self.fresh(rid);self.rpc.changed_permissions=True
        self.bridge.tick()
        self.assertEqual(self.status(rid)['status'],'created')
        self.assertEqual(self.server.roots.config()['thread_id'],self.thread_id)
        self.assertFalse(any(method=='thread/queue/add' for method,_ in self.rpc.calls))
        self.rpc.changed_permissions=False
        self.rpc.history[self.rpc.new]=[dict(type='userMessage',clientId='unrelated',content=[])]
        self.publish_consumption=True
        self.bridge.tick()
        self.assertEqual(self.status(rid)['status'],'created')
        self.assertFalse(any(method=='thread/queue/add' for method,_ in self.rpc.calls))

    def test_start_ack_without_exact_client_id_never_completes(self):
        rid=str(uuid.uuid4());self.fresh(rid);self.bridge.tick()
        self.publish_consumption=True
        self.rpc.history[self.rpc.new][0].pop('clientId')
        self.bridge.tick();self.bridge.tick()
        self.assertEqual(self.status(rid)['status'],'uncertain')
        self.assertIsNone(self.status(rid)['native_proof_json'])
        self.assertEqual(len([call for call in self.rpc.calls if call[0]=='thread/queue/start']),1)

    def test_settings_changed_during_switch_withhold_boot(self):
        rid=str(uuid.uuid4());self.fresh(rid)
        switch=RootRestart.switch
        def changed(instance,request_id,server):
            result=switch(instance,request_id,server)
            self.rpc.changed_permissions=True
            return result
        with mock.patch.object(RootRestart,'switch',changed): self.bridge.tick()
        self.assertEqual(self.status(rid)['status'],'switched')
        self.assertFalse(any(method=='thread/queue/add' for method,_ in self.rpc.calls))

    def test_old_root_became_active_during_candidate_readback(self):
        rid=str(uuid.uuid4());self.fresh(rid)
        original=self.rpc.request
        def request(method,params):
            result=original(method,params)
            if method=='thread/resume' and params['threadId']!=self.thread_id:
                self.rpc.status[self.thread_id]='active'
            return result
        self.rpc.request=request;self.bridge.tick()
        self.assertEqual(self.status(rid)['status'],'created')
        self.assertEqual(self.server.roots.config()['thread_id'],self.thread_id)
        self.assertFalse(any(method=='thread/queue/add' for method,_ in self.rpc.calls))

    def test_exact_request_payload_and_canonical_actor_required(self):
        rid=str(uuid.uuid4());self.fresh(rid)
        self.fresh(rid)
        with self.assertRaisesRegex(RemoteCoordError,'retry payload'):
            self.fresh(rid,'Different boot prompt')
        with self.assertRaisesRegex(RemoteCoordError,'canonical home'):
            self.root('fresh-start',session='outsider',expected_thread=self.thread_id,prompt='Boot',request_id=str(uuid.uuid4()),confirm=True)


# The reusable fixture also contains test methods; expose only this task's cases.
for name in list(vars(root_tests.RootOrchestratorTests)):
    if name.startswith('test_') and name not in vars(RootFreshStartTests):
        setattr(RootFreshStartTests,name,None)
