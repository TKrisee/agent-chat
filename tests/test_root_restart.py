"""Public root API + existing authenticated host bridge workflow regressions."""
import contextlib
import copy
import uuid
import sqlite3
import subprocess
from unittest import mock
from agent_chat.root_restart import RootRestart

from agent_chat.core import Coordinator, CoordError
from agent_chat.remote import RemoteCoordError
from agent_chat.rpc import TransportError
import test_root_orchestrator as root_tests
from test_session_reset import ResetRpc


class RootFreshStartTests(root_tests.RootOrchestratorTests):
    # Avoid inheriting the full unrelated root suite.
    def root(self,op,session='a',host='mac',**params):
        # Unit fixture calls the owning in-process API. Shared bearer mutation
        # is intentionally forbidden; its production authorization is untouched.
        roots=self.server.roots;args=('default',session,host)
        try:
            with self.server.root_lock:
                if op=='enroll': return roots.enroll(*args,params['expected_thread'],params['confirm'])
                if op in ('fresh-start','fresh-status'):
                    restart=RootRestart(roots)
                    if op=='fresh-status': return restart.authorize_status(*args,params['request_id'])
                    return restart.request(*args,params['expected_thread'],params['prompt'],params['request_id'],params['confirm'])
                if op in ('stop-all','stop-status'):
                    return roots.team_lifecycle(*args,op,params['request_id'],params.get('confirm',True))
                roots.authorize(*args)
                if op=='route': return roots.route(*args,params['project'])
                raise AssertionError('unsupported fixture operation '+op)
        except CoordError as error:
            raise RemoteCoordError(str(error)) from error

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

    def test_prepared_reports_across_aliases_share_one_post_boot_wake(self):
        self.exercise_prepared_reports()

    def test_unbatched_reproduction_requires_three_report_turns(self):
        with mock.patch.object(RootRestart,'batch_prepared',return_value=None):
            self.exercise_prepared_reports(batched=False)

    def exercise_prepared_reports(self,batched=True):
        projects=[self.project('Reports A'),self.project('Reports B')]
        reports=[];bridges=[self.bridge]
        scopes=[(self.server.roots.config()['home_project'],'a',self.state)]
        for project in projects:
            state,bridge=self.project_bridge(project);bridges.append(bridge)
            scopes.append((project,self.root('route',project=project)['session'],state))
        for index,(project,recipient,state) in enumerate(scopes):
            with contextlib.closing(Coordinator(self.server.roots.path(project),recipient)) as coord:
                with contextlib.closing(Coordinator(coord.path,'reporter')) as sender:
                    sender.register('reporter')
                    message=sender.send(recipient,'Retained report '+str(index))
                    already_read=sender.send(recipient,'Already read '+str(index))
                wake=state.prepare(self.thread_id,[dict(id=m['id'],recipient_session=recipient) for m in (message,already_read)],'old report '+str(index))
                coord.message(already_read['id'])
                if index==0: coord.acknowledge(message['id'])
                reports.append((project,recipient,message,already_read,wake))
        stop=str(uuid.uuid4());self.root('stop-all',request_id=stop,confirm=True)
        rid=str(uuid.uuid4());self.fresh(rid);self.bridge.tick()
        new=self.status(rid)['new_thread_id']
        boot=self.rpc.history[new][0]['content'][0]['text']
        self.assertNotIn('Retained report 0',boot)
        self.assertNotIn(reports[0][2]['id'],boot)
        self.publish_consumption=True
        for _ in range(3):
            self.rpc.status[new]='idle'
            for bridge in bridges: bridge.tick()
        additions=[params for method,params in self.rpc.calls if method=='thread/queue/add']
        self.assertEqual(len(additions),2 if batched else 4)
        if not batched:
            self.assertTrue(self.root('stop-status',request_id=stop)['barrier_complete'])
            return
        batch_payload=additions[1]['input'][0]['text']
        self.assertLessEqual(len(batch_payload.encode('utf-8')),12288)
        hydrated=subprocess.run(['jq','-Rse', 'split("\n")[-1] | fromjson | [.reports[].messages[] | select(.complete == true and (.body | startswith("Retained report ")))] | length'],
            input=batch_payload,text=True,capture_output=True,check=True).stdout.strip()
        self.assertEqual(hydrated,'3')
        for project,recipient,message,already_read,wake in reports:
            self.assertIn(message['id'],batch_payload)
            self.assertNotIn(already_read['id'],batch_payload)
            self.assertIn(project,batch_payload)
            self.assertIn(wake['id'],batch_payload)
            with contextlib.closing(Coordinator(self.server.roots.path(project),recipient)) as coord:
                self.assertEqual(coord.db.execute('SELECT job_id FROM bridge_deliveries WHERE message_id=?',(message['id'],)).fetchone()['job_id'],wake['id'])
                self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM message_reads WHERE message_id=?',(message['id'],)).fetchone()[0],0)
                self.assertEqual(coord.db.execute('SELECT status FROM bridge_jobs WHERE id=?',(wake['id'],)).fetchone()['status'],'cancelled')
        self.assertTrue(self.root('stop-status',request_id=stop)['barrier_complete'])
        with self.server.projects.connection() as db:
            self.assertIsNone(db.execute('SELECT resumed_at FROM root_team_operations WHERE id=?',(stop,)).fetchone()['resumed_at'])

    def prepare_home_reports(self,count=3):
        self.call('register',session='reporter',agent='reporter')
        jobs=[]
        for index in range(count):
            message=self.call('send',session='reporter',to='a',body='Report '+str(index))
            jobs.append(self.state.prepare(self.thread_id,[dict(id=message['id'],recipient_session='a')],'Original '+str(index)))
        return jobs

    def test_batch_preserves_legacy_retried_and_control_associated_fallbacks(self):
        jobs=self.prepare_home_reports(5)
        with contextlib.closing(Coordinator(self.db,'a')) as coord:
            from agent_chat.bridge_state import BridgeState
            state=BridgeState(coord)
            state.update(jobs[2]['id'],'uncertain')
            state.retry(jobs[2]['id'],True)
            # A legacy row predates positive provenance; insert a distinct row
            # without forging/removing the immutable preparation journal.
            message=self.call('send',session='reporter',to='a',body='Legacy report')
            coord.db.execute("INSERT INTO bridge_jobs VALUES('legacy',?,'legacy payload','prepared',NULL,NULL,1,1)",(self.thread_id,))
            coord.db.execute("INSERT INTO bridge_deliveries VALUES(?,'legacy')",(message['id'],))
            coord.db.execute('''INSERT INTO agent_control_requests
                (id,requester_session,target_session,host_id,thread_id,action,interrupt,status,created_at,updated_at,native_input_job_id)
                VALUES('associated','a','a','mac',?,'inspect',0,'completed',1,1,?)''',(self.thread_id,jobs[3]['id']))
            # An unrelated native route must never join the root batch.
            coord.db.execute('UPDATE bridge_jobs SET thread_id=? WHERE id=?',(str(uuid.uuid4()),jobs[4]['id']))
        rid=str(uuid.uuid4());self.fresh(rid);self.bridge.tick()
        with self.server.projects.connection() as db:
            members={r['job_id'] for r in db.execute('SELECT job_id FROM root_fresh_wake_members')}
        self.assertEqual(members,{jobs[0]['id'],jobs[1]['id']})
        with contextlib.closing(Coordinator(self.db)) as coord:
            for job_id in ('legacy',jobs[2]['id'],jobs[3]['id'],jobs[4]['id']):
                self.assertEqual(coord.db.execute('SELECT status FROM bridge_jobs WHERE id=?',(job_id,)).fetchone()['status'],'prepared')

    def test_batch_size_falls_back_without_losing_original_delivery(self):
        jobs=self.prepare_home_reports(60)
        rid=str(uuid.uuid4());self.fresh(rid);self.bridge.tick()
        with self.server.projects.connection() as db:
            members={r['job_id'] for r in db.execute('SELECT job_id FROM root_fresh_wake_members')}
            payload=db.execute('SELECT payload FROM root_fresh_wake_batches').fetchone()['payload']
        self.assertGreater(len(members),1)
        self.assertLess(len(members),len(jobs))
        self.assertLessEqual(len(payload.encode()),12288)
        with contextlib.closing(Coordinator(self.db)) as coord:
            for source in jobs:
                self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM bridge_deliveries WHERE job_id=?',(source['id'],)).fetchone()[0],1)
                self.assertEqual(coord.db.execute('SELECT status FROM bridge_jobs WHERE id=?',(source['id'],)).fetchone()['status'],
                    'cancelled' if source['id'] in members else 'prepared')

    def test_partial_batch_supersession_recovers_before_gate_release(self):
        from agent_chat.bridge_state import BridgeState
        jobs=self.prepare_home_reports(3)
        original=BridgeState.pristine;calls=[]
        def fail_second_supersession(state,job_id):
            calls.append(job_id)
            if len(calls)==5: return False
            return original(state,job_id)
        rid=str(uuid.uuid4());self.fresh(rid)
        with mock.patch.object(BridgeState,'pristine',fail_second_supersession): self.bridge.tick()
        self.assertEqual(self.status(rid)['status'],'switching')
        self.assertFalse(any(method=='thread/queue/add' for method,_ in self.rpc.calls))
        with contextlib.closing(Coordinator(self.db)) as coord:
            self.assertEqual(coord.db.execute('SELECT active FROM root_fresh_gates WHERE request_id=?',(rid,)).fetchone()['active'],1)
            self.assertEqual(coord.db.execute("SELECT COUNT(*) FROM bridge_jobs WHERE status='cancelled'").fetchone()[0],1)
        self.bridge.tick()
        self.assertEqual(self.status(rid)['status'],'starting')
        with self.server.projects.connection() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM root_fresh_wake_batches').fetchone()[0],1)
            self.assertEqual({r['job_id'] for r in db.execute('SELECT job_id FROM root_fresh_wake_members')},{j['id'] for j in jobs})
        self.assertEqual(len([1 for method,_ in self.rpc.calls if method=='thread/start']),1)
        self.assertEqual(len([1 for method,_ in self.rpc.calls if method=='thread/queue/add']),1)

    def test_missing_original_delivery_evidence_withholds_batch_input(self):
        jobs=self.prepare_home_reports(3)
        rid=str(uuid.uuid4());self.fresh(rid);self.bridge.tick()
        new=self.status(rid)['new_thread_id']
        with contextlib.closing(Coordinator(self.db)) as coord:
            coord.db.execute('DELETE FROM bridge_deliveries WHERE job_id=?',(jobs[0]['id'],))
        self.publish_consumption=True;self.rpc.status[new]='idle'
        with self.assertRaisesRegex(RemoteCoordError,'evidence changed'): self.bridge.tick()
        self.assertEqual(len([1 for method,_ in self.rpc.calls if method=='thread/queue/add']),1)

    def test_uncertain_batch_submission_preserves_exact_payload_without_replay(self):
        self.prepare_home_reports(3)
        rid=str(uuid.uuid4());self.fresh(rid);self.bridge.tick()
        new=self.status(rid)['new_thread_id']
        with self.server.projects.connection() as db:
            batch=dict(db.execute('SELECT * FROM root_fresh_wake_batches').fetchone())
        self.publish_consumption=True;self.rpc.status[new]='idle';self.rpc.drop='thread/queue/start'
        with self.assertRaises(TransportError): self.bridge.tick()
        self.bridge.tick()
        retained=self.state.job(batch['id'])
        self.assertEqual((retained['status'],retained['payload']),('dispatched',batch['payload']))
        self.assertEqual(len([1 for method,_ in self.rpc.calls if method=='thread/queue/add']),2)
        self.assertEqual(len([1 for method,_ in self.rpc.calls if method=='thread/queue/start']),2)
        with self.server.projects.connection() as db:
            with self.assertRaisesRegex(sqlite3.IntegrityError,'immutable'):
                db.execute("UPDATE root_fresh_wake_members SET job_id='rewritten' WHERE batch_id=?",(batch['id'],))

    def test_changed_synthetic_payload_thread_and_marker_withhold_native_add(self):
        self.prepare_home_reports(3)
        rid=str(uuid.uuid4());self.fresh(rid);self.bridge.tick()
        new=self.status(rid)['new_thread_id']
        with self.server.projects.connection() as db:
            batch=dict(db.execute('SELECT * FROM root_fresh_wake_batches').fetchone())
        self.publish_consumption=True;self.bridge.tick()
        self.rpc.status[new]='idle'
        for fault in ('payload','thread','marker','missing_marker'):
            with self.subTest(fault=fault),contextlib.closing(Coordinator(self.db)) as coord:
                if fault=='payload': coord.db.execute("UPDATE bridge_jobs SET payload='changed' WHERE id=?",(batch['id'],))
                if fault=='thread': coord.db.execute('UPDATE bridge_jobs SET thread_id=? WHERE id=?',(str(uuid.uuid4()),batch['id']))
                if fault=='marker': coord.db.execute("UPDATE bridge_root_wake_batches SET request_id='wrong' WHERE job_id=?",(batch['id'],))
                if fault=='missing_marker': coord.db.execute('DELETE FROM bridge_root_wake_batches WHERE job_id=?',(batch['id'],))
                with self.assertRaises(RemoteCoordError): self.state.still_unread(batch['id'])
                if fault=='payload':
                    with self.assertRaisesRegex(CoordError,'synthetic'):
                        RootRestart(self.server.roots).batch_prepared(rid,self.url)
                    self.assertEqual(coord.db.execute('SELECT payload FROM bridge_jobs WHERE id=?',(batch['id'],)).fetchone()['payload'],'changed')
                coord.db.execute('UPDATE bridge_jobs SET payload=?,thread_id=? WHERE id=?',(batch['payload'],new,batch['id']))
                coord.db.execute('INSERT OR REPLACE INTO bridge_root_wake_batches VALUES(?,?)',(batch['id'],rid))
        self.assertEqual(len([1 for method,_ in self.rpc.calls if method=='thread/queue/add']),1)

    def test_changed_queued_synthetic_payload_withholds_native_start(self):
        self.prepare_home_reports(3)
        rid=str(uuid.uuid4());self.fresh(rid);self.bridge.tick()
        new=self.status(rid)['new_thread_id'];self.publish_consumption=True;self.rpc.status[new]='idle'
        request=self.rpc.request
        def become_busy_after_add(method,params):
            result=request(method,params)
            if method=='thread/queue/add': self.rpc.status[new]='active'
            return result
        self.rpc.request=become_busy_after_add
        self.bridge.tick()
        with self.server.projects.connection() as db:
            batch=dict(db.execute('SELECT * FROM root_fresh_wake_batches').fetchone())
        self.assertEqual(self.state.job(batch['id'])['status'],'queued')
        with contextlib.closing(Coordinator(self.db)) as coord:
            coord.db.execute("UPDATE bridge_jobs SET payload='changed queued bytes' WHERE id=?",(batch['id'],))
        self.rpc.status[new]='idle'
        with self.assertRaisesRegex(RemoteCoordError,'synthetic'): self.bridge.tick()
        self.assertEqual(len([1 for method,_ in self.rpc.calls if method=='thread/queue/start']),1)
        retained=self.state.job(batch['id'])
        self.assertEqual((retained['status'],retained['payload']),('queued','changed queued bytes'))

    def test_stopped_or_rebound_foreign_member_holds_batch_until_explicit_restore(self):
        project=self.project();foreign,_=self.project_bridge(project)
        recipient=self.root('route',project=project)['session']
        self.prepare_home_reports(2)
        with contextlib.closing(Coordinator(self.server.roots.path(project),'reporter')) as sender:
            sender.register('reporter');message=sender.send(recipient,'Foreign retained report')
        source=foreign.prepare(self.thread_id,[dict(id=message['id'],recipient_session=recipient)],'foreign source')
        rid=str(uuid.uuid4());self.fresh(rid);self.bridge.tick()
        new=self.status(rid)['new_thread_id'];self.publish_consumption=True;self.bridge.tick()
        with self.server.projects.connection() as db:
            batch=dict(db.execute('SELECT * FROM root_fresh_wake_batches').fetchone())
        with contextlib.closing(Coordinator(self.server.roots.path(project))) as coord:
            prior=coord.db.execute('SELECT * FROM agent_controls WHERE target_session=?',(recipient,)).fetchone()
            coord.db.execute("INSERT OR REPLACE INTO agent_controls VALUES(?,'stopped',1)",(recipient,))
        with self.assertRaisesRegex(RemoteCoordError,'stopped'): self.bridge.tick()
        with contextlib.closing(Coordinator(self.server.roots.path(project))) as coord:
            self.assertEqual(coord.db.execute('SELECT mode FROM agent_controls WHERE target_session=?',(recipient,)).fetchone()['mode'],'stopped')
            if prior:
                coord.db.execute('UPDATE agent_controls SET mode=?,updated_at=? WHERE target_session=?',(prior['mode'],prior['updated_at'],recipient))
            else: coord.db.execute('DELETE FROM agent_controls WHERE target_session=?',(recipient,))
            coord.db.execute('UPDATE bridge_bindings SET thread_id=? WHERE session_id=?',(str(uuid.uuid4()),recipient))
        with self.assertRaisesRegex(RemoteCoordError,'binding changed'): self.bridge.tick()
        with contextlib.closing(Coordinator(self.db)) as coord:
            visible=coord.db.execute('SELECT state,error FROM bridge_observations WHERE thread_id=?',(new,)).fetchone()
            self.assertEqual(visible['state'],'error');self.assertIn('binding changed',visible['error'])
        self.assertEqual(len([1 for method,_ in self.rpc.calls if method=='thread/queue/add']),1)
        with self.server.projects.connection() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM root_fresh_wake_members WHERE batch_id=?',(batch['id'],)).fetchone()[0],3)
        with contextlib.closing(Coordinator(self.server.roots.path(project))) as coord:
            self.assertEqual(coord.db.execute('SELECT status FROM bridge_jobs WHERE id=?',(source['id'],)).fetchone()['status'],'cancelled')
            # Only the fixture explicitly restores the original member route.
            coord.db.execute('UPDATE bridge_bindings SET thread_id=? WHERE session_id=?',(new,recipient))
        self.rpc.status[new]='idle';self.bridge.tick()
        self.assertEqual(self.state.job(batch['id'])['status'],'dispatched')
        self.assertEqual(len([1 for method,_ in self.rpc.calls if method=='thread/queue/add']),2)

    def test_partial_read_keeps_prepared_snapshot_and_all_read_cancels_without_input(self):
        jobs=self.prepare_home_reports(3)
        rid=str(uuid.uuid4());self.fresh(rid);self.bridge.tick()
        new=self.status(rid)['new_thread_id'];self.publish_consumption=True;self.bridge.tick()
        with self.server.projects.connection() as db:
            batch=dict(db.execute('SELECT * FROM root_fresh_wake_batches').fetchone())
        with contextlib.closing(Coordinator(self.db,'a')) as coord:
            messages=[coord.db.execute('SELECT message_id FROM bridge_deliveries WHERE job_id=?',(source['id'],)).fetchone()['message_id'] for source in jobs]
            coord.message(messages[0])
        self.assertTrue(self.state.still_unread(batch['id']))
        # Like an ordinary prepared multi-message wake, one newly read report
        # does not rewrite the durable snapshot while another remains unread.
        self.assertEqual(self.state.job(batch['id'])['payload'],batch['payload'])
        with contextlib.closing(Coordinator(self.db,'a')) as coord:
            for message_id in messages[1:]: coord.message(message_id)
        self.rpc.status[new]='idle';self.bridge.tick()
        retained=self.state.job(batch['id'])
        self.assertEqual((retained['status'],retained['payload']),('cancelled',batch['payload']))
        self.assertEqual(len([1 for method,_ in self.rpc.calls if method=='thread/queue/add']),1)
        with self.server.projects.connection() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM root_fresh_wake_members WHERE batch_id=?',(batch['id'],)).fetchone()[0],3)

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
