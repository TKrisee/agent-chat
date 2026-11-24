"""Public root API + existing authenticated host bridge workflow regressions."""
import contextlib
import copy
import uuid
import sqlite3
import subprocess
import time
from unittest import mock
from agent_chat.root_restart import RootRestart

from agent_chat.core import Coordinator, CoordError
from agent_chat.remote import RemoteCoordError
from agent_chat.rpc import TransportError, RpcError
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
                if op=='fresh-recover':
                    restart=RootRestart(roots)
                    return restart.recover(*args,params['expected_thread'],params['request_id'],params['attempt_id'],params['confirm'])
                if op=='fresh-replace':
                    restart=RootRestart(roots)
                    return restart.replace_candidate(*args,params['expected_thread'],params['request_id'],params['reconciliation_id'],params['confirm'])
                if op=='fresh-retire':
                    restart=RootRestart(roots)
                    return restart.retire_uncertain(*args,params['expected_thread'],params['request_id'],params['reconciliation_id'],params['retirement_id'],params['confirm'])
                if op=='fresh-cancel':
                    restart=RootRestart(roots)
                    return restart.cancel_pending(*args,params['expected_thread'],params['request_id'],params['cancellation_id'],params['confirm'])
                if op=='fresh-verify':
                    restart=RootRestart(roots)
                    return restart.verify_handoff(*args,params['expected_thread'],params['request_id'],params['verification_id'],params['confirm'])
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
        self.publish_response=False
        self.response_phase='commentary'
        self.turn_error=None
        self.root_candidates=set()
        self.capture_root_candidate=False
        def request(method,params):
            if method=='thread/start' and self.capture_root_candidate:
                result=original(method,params)
                self.root_candidates.add(result['thread']['id'])
                self.capture_root_candidate=False
                return result
            if method=='thread/turns/list' and params.get('threadId') in self.root_candidates and self.rpc.history.get(params['threadId']):
                self.rpc.calls.append((method,copy.deepcopy(params)))
                items=copy.deepcopy(self.rpc.history[params['threadId']])
                if self.publish_response: items.append(dict(type='agentMessage',id='native-boot-response',phase=self.response_phase,text='Root status checked.'))
                return {'data':[dict(id='native-boot-turn',status='completed' if self.publish_response else 'inProgress',error=self.turn_error,items=items)] if self.publish_consumption else [],'nextCursor':None}
            if method=='thread/read' and params.get('includeTurns') is True and params.get('threadId') in self.root_candidates and self.rpc.history.get(params['threadId']):
                self.rpc.calls.append((method,copy.deepcopy(params)))
                items=copy.deepcopy(self.rpc.history[params['threadId']])
                if self.publish_response: items.append(dict(type='agentMessage',id='native-boot-response',phase=self.response_phase,text='Root status checked.'))
                return {'thread':dict(id=params['threadId'],turns=[dict(id='native-boot-turn',status='completed' if self.publish_response else 'inProgress',error=self.turn_error,items=items)] if self.publish_consumption else [])}
            if method=='thread/items/list' and params.get('threadId') in self.root_candidates:
                self.rpc.calls.append((method,copy.deepcopy(params)))
                items=copy.deepcopy(self.rpc.history[params['threadId']])
                if self.publish_response: items.append(dict(type='agentMessage',id='native-boot-response',phase=self.response_phase,text='Root status checked.'))
                return {'data':[dict(turnId=params['turnId'],item=item) for item in items],'nextCursor':None}
            return original(method,params)
        self.rpc.request=request

    def fresh(self,rid,prompt='Read the checkpoint. Keep all stopped teams stopped.'):
        self.capture_root_candidate=True
        return self.root('fresh-start',expected_thread=self.thread_id,prompt=prompt,request_id=rid,confirm=True)

    def status(self,rid):
        return self.root('fresh-status',request_id=rid)

    def test_settings_mismatch_retirement_preserves_candidate_and_releases_only_own_gates(self):
        rid='876fd5a8-8966-411e-8236-47df1fc8bf99'
        candidate='01a148f4-3358-79b3-b487-4210622bf57b'
        retirement='a5b6a5d3-b622-495d-9454-164af0bfb245'
        base=self.rpc.request
        def drift(method,params):
            result=base(method,params)
            if method=='thread/start':
                generated=result['thread']['id']
                for mapping in (self.rpc.status,self.rpc.queues,self.rpc.history): mapping[candidate]=mapping.pop(generated)
                self.rpc.new=candidate
                self.root_candidates.discard(generated);self.root_candidates.add(candidate)
                result['thread']['id']=candidate
                result['model']='unexpected-model'
            return result
        self.rpc.request=drift
        self.fresh(rid)
        self.bridge.tick()
        self.rpc.request=base
        mismatch=self.status(rid)
        restart=RootRestart(self.server.roots)
        mismatch_job=restart.job(rid)
        self.assertEqual(mismatch['status'],'created')
        self.assertEqual(mismatch['new_thread_id'],candidate)
        self.assertNotEqual(mismatch_job['settings_digest'],mismatch_job['new_settings_digest'])
        self.assertFalse(mismatch['queue_id'])
        self.assertIsNone(mismatch['native_proof_json'])
        self.assertEqual(self.server.roots.config()['thread_id'],self.thread_id)
        self.assertFalse(any(method in ('thread/queue/add','thread/queue/start') for method,_ in self.rpc.calls))
        scopes=RootRestart(self.server.roots).scope(rid)
        sentinel='c9b15e91-04bb-47af-8994-cf03e76fdc65'
        for item in scopes:
            with contextlib.closing(Coordinator(self.server.roots.path(item['project_id']),item['root_session'])) as coord:
                coord.db.execute('INSERT INTO root_fresh_gates VALUES(?,?,?,?,1)',(sentinel,item['root_session'],self.thread_id,None))
        prior_calls=list(self.rpc.calls)
        retired=restart.retire_settings_mismatch('default','a','mac',self.thread_id,rid,retirement,True)
        self.assertEqual(self.rpc.calls,prior_calls)
        self.assertEqual(retired['status'],'retired')
        self.assertFalse(retired['handoff_ready'])
        self.assertEqual(retired['handoff_state'],'retired_settings_mismatch')
        self.assertEqual(retired['next_action'],'continue_current_root')
        self.assertEqual(retired['new_thread_id'],candidate)
        self.assertEqual(retired['settings_retirement']['retirement_id'],retirement)
        self.assertEqual(retired['settings_retirement']['candidate_thread_id'],candidate)
        self.assertEqual(retired['settings_retirement']['outcome'],'candidate_settings_mismatch_native_outcome_unknown')
        self.assertIn('no route change, boot, or native closure',retired['blocker'])
        for item in scopes:
            with contextlib.closing(Coordinator(self.server.roots.path(item['project_id']),item['root_session'])) as coord:
                own=coord.db.execute('SELECT active FROM root_fresh_gates WHERE request_id=? AND root_session=?',(rid,item['root_session'])).fetchone()
                other=coord.db.execute('SELECT active FROM root_fresh_gates WHERE request_id=? AND root_session=?',(sentinel,item['root_session'])).fetchone()
                self.assertEqual(own['active'],0)
                self.assertEqual(other['active'],1)
        with self.server.projects.connection() as db:
            audit=db.execute('SELECT * FROM root_fresh_settings_retirements WHERE request_id=?',(rid,)).fetchone()
            self.assertEqual(audit['candidate_thread_id'],candidate)
            self.assertEqual(audit['expected_settings_digest'],mismatch_job['settings_digest'])
            self.assertEqual(audit['observed_settings_digest'],mismatch_job['new_settings_digest'])
            self.assertEqual(audit['state'],'retired')
        replay=restart.retire_settings_mismatch('default','a','mac',self.thread_id,rid,retirement,True)
        self.assertEqual(replay['settings_retirement']['retirement_id'],retirement)
        with self.assertRaises(CoordError):
            restart.retire_settings_mismatch('default','a','mac',self.thread_id,rid,
                'f7a6d6d8-5663-45cb-9ef8-c62832b32418',True)
        rpc_after=list(self.rpc.calls)
        with self.assertRaises(CoordError):
            restart.update('default','mac',rid,'created','switching',
                           new_settings_digest=mismatch_job['new_settings_digest'],
                           server='http://127.0.0.1:8765')
        self.bridge.tick()
        self.assertEqual(self.rpc.calls,rpc_after)
        self.assertEqual(restart.job(rid)['status'],'retired')
        self.assertEqual(self.server.roots.config()['thread_id'],self.thread_id)

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
        self.assertTrue(completed['boot_consumed'])
        self.assertEqual(completed['response_state'],'pending')
        self.assertEqual(completed['owner_state'],'unknown')
        self.assertFalse(completed['handoff_ready'])
        self.assertNotIn('operator_report',completed)
        self.publish_response=True
        self.bridge.tick()
        self.assertEqual(self.status(rid)['response_state'],'pending')
        self.response_phase='final_answer'
        self.turn_error={'message':'turn failed'}
        self.bridge.tick()
        self.assertEqual(self.status(rid)['response_state'],'pending')
        self.turn_error=None
        self.bridge.tick()
        response_seen=self.status(rid)
        self.assertEqual(response_seen['handoff_state'],'response_observed_owner_unverified')
        self.assertEqual(response_seen['response_state'],'observed')
        exact_items=[params for method,params in self.rpc.calls if method=='thread/items/list']
        self.assertTrue(exact_items)
        self.assertTrue(all(params.get('turnId')=='native-boot-turn' and params.get('threadId')==response_seen['new_thread_id'] for params in exact_items))
        summary_calls=[params for method,params in self.rpc.calls if method=='thread/turns/list' and params.get('threadId')==response_seen['new_thread_id']]
        self.assertTrue(summary_calls)
        self.assertTrue(all(params.get('itemsView')=='summary' for params in summary_calls))
        from agent_chat.native_history import bounded_turns_summary_page
        projected=bounded_turns_summary_page(self.rpc,response_seen['new_thread_id'])
        assistant_summary=next(item for item in projected['data'][0]['items'] if item.get('type')=='agentMessage')
        self.assertNotIn('text',assistant_summary)
        self.assertNotIn('phase',assistant_summary)
        self.assertNotIn('operator_report',response_seen)
        restart=RootRestart(self.server.roots)
        owner_proof=dict(state='open',source='native-cli-owner',thread_id=response_seen['new_thread_id'],
            session_id='a',owner_identity='test-owner-proof',owner_pid=4242,owner_start='test-start',
            owner_uid=501,owner_endpoint='ws://127.0.0.1:4500',owner_generation=1.0,observed_at=time.time())
        restart.update('default','mac',rid,'completed','completed',owner_proof=dict(owner_proof,state='closed'))
        closed=self.status(rid)
        self.assertEqual(closed['handoff_state'],'candidate_cli_not_currently_owned')
        self.assertEqual(closed['next_action'],'await_same_candidate_cli_owner')
        stale_proof=dict(owner_proof,observed_at=time.time()-31)
        restart.update('default','mac',rid,'completed','completed',owner_proof=stale_proof)
        stale=self.status(rid)
        self.assertEqual(stale['owner_state'],'stale')
        self.assertFalse(stale['handoff_ready'])
        restart.update('default','mac',rid,'completed','completed',owner_proof=owner_proof)
        ready=self.status(rid)
        self.assertTrue(ready['handoff_ready'])
        self.assertEqual(ready['operator_notice']['request_id'],rid)
        self.assertEqual(ready['operator_notice']['state'],'ready')
        self.assertIsNone(ready['operator_notice']['reason'])
        report=ready['operator_report']
        self.assertEqual(report['request_id'],rid)
        self.assertEqual(report['new_thread_id'],job['new_thread_id'])
        self.assertIn('sole CLI process was verified',report['cli_guidance'])
        self.assertIn('do not open a second CLI',report['cli_guidance'])
        self.assertIn('Only after status completed',boot)
        self.assertEqual(self.status(rid)['operator_report'],report)
        self.assertEqual(len([call for call in self.rpc.calls if call[0]=='thread/start']),1)
        self.assertEqual(len([call for call in self.rpc.calls if call[0]=='thread/queue/add']),1)
        self.assertTrue(self.root('stop-status',request_id=stop)['barrier_complete'])
        with self.server.projects.connection() as db:
            self.assertIsNone(db.execute('SELECT resumed_at FROM root_team_operations WHERE id=?',(stop,)).fetchone()['resumed_at'])

    def test_fresh_in_memory_candidate_does_not_require_saved_rollout_to_pass_readiness(self):
        rid=str(uuid.uuid4())
        original=self.rpc.request
        def no_saved_rollout(method,params):
            result=original(method,params)
            if method=='thread/start': self.rpc.no_rollout.add(result['thread']['id'])
            return result
        self.rpc.request=no_saved_rollout
        self.fresh(rid);self.bridge.tick()
        job=self.status(rid);candidate=job['new_thread_id']
        self.assertEqual(job['status'],'starting',job)
        self.assertNotIn('candidate recovery required:',(job['error'] or '').lower())
        candidate_calls=[method for method,params in self.rpc.calls if params.get('threadId')==candidate
                         and method in ('thread/read','thread/resume')]
        self.assertEqual(candidate_calls[0],'thread/read')
        self.assertEqual(len([1 for method,_ in self.rpc.calls if method=='thread/queue/add']),1)
        self.assertEqual(len([1 for method,_ in self.rpc.calls if method=='thread/start']),1)

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

    def _create_unloaded_candidate(self, rid):
        original=self.rpc.request
        def request(method,params):
            result=original(method,params)
            if method=='thread/start':
                self.rpc.status[self.thread_id]='active'
                self.rpc.unmaterialized=True
                self.rpc.unloaded.add(self.rpc.new)
            return result
        self.rpc.request=request
        self.fresh(rid)
        self.bridge.tick()
        job=self.status(rid)
        self.assertEqual(job['status'],'created',job)
        self.assertIn('Candidate recovery required:',job['error'])
        self.assertEqual(job['handoff_state'],'candidate_recovery_required')
        return job

    def _create_exact_876_no_rollout(self):
        candidate='01a148f4-3358-79b3-b487-4210622bf57b'
        original=self.rpc.request
        def request(method,params):
            result=original(method,params)
            if method=='thread/start':
                generated=result['thread']['id']
                for mapping in (self.rpc.status,self.rpc.queues,self.rpc.history):
                    mapping[candidate]=mapping.pop(generated)
                self.rpc.new=candidate
                self.root_candidates.discard(generated);self.root_candidates.add(candidate)
                self.rpc.no_rollout.add(candidate)
                self.rpc.unmaterialized=True
                self.rpc.unloaded.add(candidate)
                result['thread']['id']=candidate
            return result
        self.rpc.request=request
        self.fresh('876fd5a8-8966-411e-8236-47df1fc8bf99')
        self.bridge.tick()
        job=self.status('876fd5a8-8966-411e-8236-47df1fc8bf99')
        self.assertEqual(job['new_thread_id'],candidate)
        self.assertEqual(job['error'],('Candidate recovery required: exact retained thread is not materialized: '
            'RPC error -32600: no rollout found for thread id '+candidate))
        self.assertIn(('thread/resume',{'threadId':candidate,'excludeTurns':True}),self.rpc.calls)
        return job

    def test_pending_intent_cancellation_preserves_unknown_candidate_and_only_releases_own_gates(self):
        rid='876fd5a8-8966-411e-8236-47df1fc8bf99'
        candidate='01a148f4-3358-79b3-b487-4210622bf57b'
        cancellation='e7e2075c-4ab8-4791-a952-e3321c505c03'
        job=self._create_exact_876_no_rollout()
        self.assertEqual(job['status'],'created')
        self.assertEqual(job['native_action'],'creating')
        self.assertIsNone(job['queue_id'])
        self.assertIsNone(job['native_proof_json'])
        self.assertIsNone(job['response_proof_json'])
        self.assertIsNone(job['owner_proof_json'])
        self.assertIsNone(job.get('settings_retirement'))
        self.assertIsNone(job.get('retirement'))
        self.assertIsNone(job.get('pending_cancellation'))
        scopes=RootRestart(self.server.roots).scope(rid)
        sentinel='99999999-8888-4777-8666-555555555555'
        for item in scopes:
            with contextlib.closing(Coordinator(self.server.roots.path(item['project_id']),item['root_session'])) as coord:
                coord.db.execute('INSERT INTO root_fresh_gates VALUES(?,?,?,?,1)',(sentinel,item['root_session'],self.thread_id,None))
        before=list(self.rpc.calls)
        result=self.root('fresh-cancel',expected_thread=self.thread_id,request_id=rid,
            cancellation_id=cancellation,confirm=True)
        self.assertEqual(result['status'],'retired')
        self.assertEqual(result['handoff_state'],'cancelled_pending_intent')
        self.assertFalse(result['handoff_ready'])
        self.assertEqual(result['next_action'],'continue_current_root')
        self.assertEqual(result['new_thread_id'],candidate)
        self.assertEqual(result['error'],job['error'])
        self.assertEqual(result['pending_cancellation']['state'],'cancelled')
        self.assertEqual(result['pending_cancellation']['outcome'],'candidate_not_materialized_native_outcome_unknown')
        self.assertEqual(result['pending_cancellation']['candidate_thread_id'],candidate)
        self.assertEqual(result['pending_cancellation']['cancellation_id'],cancellation)
        self.assertEqual(self.server.roots.config()['thread_id'],self.thread_id)
        self.assertIn(candidate,self.rpc.no_rollout)
        self.assertEqual(self.rpc.calls,before)
        with self.assertRaisesRegex(CoordError,'state changed'):
            RootRestart(self.server.roots).update('default','mac',rid,'created','switching')
        for item in scopes:
            with contextlib.closing(Coordinator(self.server.roots.path(item['project_id']),item['root_session'])) as coord:
                self.assertEqual(coord.db.execute('SELECT active FROM root_fresh_gates WHERE request_id=?',(rid,)).fetchone()['active'],0)
                self.assertEqual(coord.db.execute('SELECT active FROM root_fresh_gates WHERE request_id=?',(sentinel,)).fetchone()['active'],1)
        replay=self.root('fresh-cancel',expected_thread=self.thread_id,request_id=rid,
            cancellation_id=cancellation,confirm=True)
        self.assertEqual(replay['pending_cancellation'],result['pending_cancellation'])
        self.assertEqual(self.rpc.calls,before)
        with self.server.projects.connection() as db:
            row=db.execute('SELECT status,new_thread_id,error,queue_id,native_proof_json FROM root_fresh_requests WHERE id=?',(rid,)).fetchone()
            self.assertEqual((row['status'],row['new_thread_id'],row['error'],row['queue_id'],row['native_proof_json']),
                ('retired',candidate,job['error'],None,None))
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute("UPDATE root_fresh_pending_cancellations SET outcome='native_closed' WHERE request_id=?",(rid,))

    def test_pending_intent_cancellation_refuses_changed_or_ineligible_state(self):
        rid='876fd5a8-8966-411e-8236-47df1fc8bf99'
        cancellation='e7e2075c-4ab8-4791-a952-e3321c505c03'
        self._create_exact_876_no_rollout()
        restart=RootRestart(self.server.roots)
        with self.assertRaisesRegex(CoordError,'exact reviewed 876 action'):
            restart.cancel_pending('default','a','mac',self.thread_id,rid,str(uuid.uuid4()),True)
        with self.assertRaisesRegex(CoordError,'confirmation'):
            restart.cancel_pending('default','a','mac',self.thread_id,rid,cancellation,False)
        # A queued intent or any consumed/booted state is never cancelled by this operation.
        with self.server.projects.connection() as db:
            db.execute("UPDATE root_fresh_requests SET queue_id='native-queue' WHERE id=?",(rid,))
        with self.assertRaisesRegex(CoordError,'unqueued, unconsumed'):
            restart.cancel_pending('default','a','mac',self.thread_id,rid,cancellation,True)
        with self.server.projects.connection() as db:
            db.execute('UPDATE root_fresh_requests SET queue_id=NULL,native_proof_json=? WHERE id=?',('{"thread_id":"candidate"}',rid))
        with self.assertRaisesRegex(CoordError,'unqueued, unconsumed'):
            restart.cancel_pending('default','a','mac',self.thread_id,rid,cancellation,True)
        with self.server.projects.connection() as db:
            db.execute("UPDATE root_fresh_requests SET status='switching' WHERE id=?",(rid,))
        with self.assertRaisesRegex(CoordError,'unqueued, unconsumed'):
            restart.cancel_pending('default','a','mac',self.thread_id,rid,cancellation,True)
        with self.server.projects.connection() as db:
            db.execute("UPDATE root_fresh_requests SET status='created' WHERE id=?",(rid,))
        with self.server.projects.connection() as db:
            db.execute('UPDATE root_fresh_requests SET native_proof_json=NULL,error=? WHERE id=?',
                ('Candidate recovery required: exact retained thread is not materialized: thread not loaded: 01a148f4-3358-79b3-b487-4210622bf57b',rid))
        with self.assertRaisesRegex(CoordError,'unqueued, unconsumed'):
            restart.cancel_pending('default','a','mac',self.thread_id,rid,cancellation,True)
        with self.server.projects.connection() as db:
            db.execute('UPDATE root_fresh_requests SET error=? WHERE id=?',
                ('Candidate recovery required: exact retained thread is not materialized: RPC error -32600: no rollout found for thread id 01a148f4-3358-79b3-b487-4210622bf57b',rid))
        with contextlib.closing(Coordinator(self.server.roots.path('default'),'a')) as coord:
            coord.db.execute("UPDATE bridge_bindings SET thread_id='99999999-8888-4777-8666-555555555555' WHERE session_id='a'")
        with self.assertRaisesRegex(CoordError,'canonical root binding'):
            restart.cancel_pending('default','a','mac',self.thread_id,rid,cancellation,True)

    def test_created_unloaded_candidate_reads_then_resumes_exact_id_and_boots_once(self):
        rid=str(uuid.uuid4());job=self._create_unloaded_candidate(rid)
        starts=len([1 for method,_ in self.rpc.calls if method=='thread/start'])
        self.rpc.unmaterialized=False
        self.rpc.unloaded.add(job['new_thread_id'])
        attempt=str(uuid.uuid4())
        requested=self.root('fresh-recover',expected_thread=self.thread_id,request_id=rid,attempt_id=attempt,confirm=True)
        repeated=self.root('fresh-recover',expected_thread=self.thread_id,request_id=rid,attempt_id=attempt,confirm=True)
        self.assertEqual(requested['recovery_attempt'],repeated['recovery_attempt'])
        with self.server.projects.connection() as db:
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute("UPDATE root_fresh_recovery_attempts SET candidate_thread_id='other' WHERE attempt_id=?",(attempt,))
        with self.assertRaises(RemoteCoordError):
            self.root('fresh-recover',expected_thread=self.thread_id,request_id=rid,attempt_id=str(uuid.uuid4()),confirm=True)
        self.rpc.calls=[]
        self.bridge.tick()
        recovery=self.status(rid)
        self.assertEqual(recovery['recovery_attempt']['recovery_event'],'loaded')
        self.assertEqual(self.server.roots.config()['thread_id'],self.thread_id)
        self.assertFalse(any(method in ('thread/start','thread/queue/add','thread/queue/start') for method,_ in self.rpc.calls))
        self.bridge.tick()
        self.assertEqual(self.status(rid)['status'],'created')
        self.assertEqual(self.server.roots.config()['thread_id'],self.thread_id)
        self.rpc.status[self.thread_id]='idle'
        self.bridge.tick()
        result=self.status(rid)
        self.assertEqual(result['status'],'starting',result)
        self.assertEqual(result['new_thread_id'],job['new_thread_id'])
        with self.assertRaises(RemoteCoordError):
            self.root('fresh-recover',expected_thread=self.thread_id,request_id=rid,attempt_id=attempt,confirm=True)
        candidate=job['new_thread_id']
        resume=[i for i,(method,params) in enumerate(self.rpc.calls)
                if method=='thread/resume' and params=={'threadId':candidate,'excludeTurns':True}]
        read=[i for i,(method,params) in enumerate(self.rpc.calls)
              if method=='thread/read' and params.get('threadId')==candidate]
        self.assertGreaterEqual(len(resume),1)
        self.assertTrue(read and resume[0]>read[0],self.rpc.calls)
        self.assertEqual(len([1 for method,_ in self.rpc.calls if method=='thread/start']),0)
        self.assertEqual(len([1 for method,_ in self.rpc.calls if method=='thread/queue/add']),1)
        self.assertEqual(len([1 for method,_ in self.rpc.calls if method=='thread/queue/start']),1)
        self.assertEqual(len([1 for method,_ in self.rpc.calls if method=='thread/start'])+starts,1)

    def test_unloadable_candidate_fails_closed_without_recreating_or_booting(self):
        rid=str(uuid.uuid4());job=self._create_unloaded_candidate(rid)
        self.rpc.unmaterialized=True
        attempt=str(uuid.uuid4())
        self.root('fresh-recover',expected_thread=self.thread_id,request_id=rid,attempt_id=attempt,confirm=True)
        self.bridge.tick()
        result=self.status(rid)
        self.assertEqual((result['status'],result['new_thread_id']),('created',job['new_thread_id']))
        self.assertIn('no rollout found for thread id',result['error'])
        self.assertEqual(self.server.roots.config()['thread_id'],self.thread_id)
        self.assertFalse(any(method in ('thread/queue/add','thread/queue/start') for method,_ in self.rpc.calls))
        self.assertEqual(result['recovery_attempt']['recovery_event'],'not_loaded')
        self.assertEqual(len([1 for method,_ in self.rpc.calls if method=='thread/start']),1)

    def test_same_request_new_attempt_retries_only_exact_candidate_metadata_load(self):
        rid=str(uuid.uuid4());job=self._create_unloaded_candidate(rid)
        self.rpc.status[self.thread_id]='idle'
        first=str(uuid.uuid4())
        self.root('fresh-recover',expected_thread=self.thread_id,request_id=rid,attempt_id=first,confirm=True)
        self.bridge.tick()
        failed=self.status(rid)
        self.assertEqual(failed['recovery_attempt']['recovery_event'],'not_loaded')
        self.rpc.unmaterialized=False
        second=str(uuid.uuid4())
        self.root('fresh-recover',expected_thread=self.thread_id,request_id=rid,attempt_id=second,confirm=True)
        self.rpc.calls=[]
        self.bridge.tick()
        loaded=self.status(rid)
        self.assertEqual(loaded['recovery_attempt']['attempt_id'],second)
        self.assertEqual(loaded['recovery_attempt']['recovery_event'],'loaded')
        self.assertEqual([method for method,_ in self.rpc.calls],['thread/read','thread/resume','thread/read'])
        self.assertTrue(all(params['threadId']==job['new_thread_id'] for _,params in self.rpc.calls))
        self.assertTrue(self.rpc.calls[1][1]['excludeTurns'])
        self.assertEqual(self.status(rid)['new_thread_id'],job['new_thread_id'])
        self.assertEqual(len([1 for method,_ in self.rpc.calls if method=='thread/start']),0)
        self.assertFalse(any(method in ('thread/queue/add','thread/queue/start') for method,_ in self.rpc.calls))

    def test_mismatched_resume_id_fails_closed(self):
        rid=str(uuid.uuid4());job=self._create_unloaded_candidate(rid)
        self.rpc.resume_id_override=str(uuid.uuid4())
        self.rpc.unmaterialized=False
        self.rpc.unloaded.add(job['new_thread_id'])
        self.rpc.status[self.thread_id]='idle'
        attempt=str(uuid.uuid4())
        self.root('fresh-recover',expected_thread=self.thread_id,request_id=rid,attempt_id=attempt,confirm=True)
        self.bridge.tick()
        result=self.status(rid)
        self.assertEqual((result['status'],result['new_thread_id']),('created',job['new_thread_id']))
        self.assertIn('different thread ID',result['error'])
        self.assertEqual(self.server.roots.config()['thread_id'],self.thread_id)
        self.assertFalse(any(method in ('thread/queue/add','thread/queue/start') for method,_ in self.rpc.calls))
        self.assertEqual(len([1 for method,_ in self.rpc.calls if method=='thread/start']),1)

    def test_completed_history_is_opt_in_and_exact_same_request_verification_is_idempotent(self):
        rid=str(uuid.uuid4());self.fresh(rid);self.bridge.tick()
        candidate=self.status(rid)['new_thread_id']
        self.publish_consumption=True
        self.bridge.tick()
        self.assertEqual(self.status(rid)['status'],'completed')
        with self.server.projects.connection() as db:
            db.execute('UPDATE root_fresh_requests SET handoff_proof_version=0 WHERE id=?',(rid,))
        self.rpc.calls=[]
        restart=RootRestart(self.server.roots)
        self.assertNotIn(rid,[job['id'] for job in restart.jobs('default','mac')])
        self.bridge.tick()
        self.assertFalse(any(method in ('thread/turns/list','thread/items/list','thread/read') and params.get('threadId')==candidate
                             for method,params in self.rpc.calls))
        unselected=self.status(rid)
        self.assertEqual(unselected['handoff_state'],'completed_not_selected_for_verification')
        self.assertEqual(unselected['next_action'],'no_automatic_history_read')
        verification_id=str(uuid.uuid4())
        first=self.root('fresh-verify',expected_thread=candidate,request_id=rid,verification_id=verification_id,confirm=True)
        repeated=self.root('fresh-verify',expected_thread=candidate,request_id=rid,verification_id=verification_id,confirm=True)
        self.assertEqual(first['handoff_proof_version'],2)
        self.assertEqual(first['handoff_proof_version'],repeated['handoff_proof_version'])
        with self.server.projects.connection() as db:
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute('UPDATE root_fresh_handoff_verifications SET expected_thread=\'other\' WHERE verification_id=?',(verification_id,))
        self.publish_response=True;self.response_phase='final_answer'
        self.rpc.calls=[]
        self.bridge.tick()
        observed=self.status(rid)
        self.assertEqual(observed['response_state'],'observed')
        self.assertFalse(observed['handoff_ready'])
        self.assertTrue(any(method=='thread/items/list' and params=={'threadId':candidate,'turnId':'native-boot-turn','limit':100,'sortDirection':'asc'}
                            for method,params in self.rpc.calls))
        self.assertFalse(any(method in ('thread/start','thread/queue/add','thread/queue/start') for method,_ in self.rpc.calls))

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
            if method=='thread/read' and params.get('threadId')!=self.thread_id:
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

    def prepare_failed_original_candidate(self, rid, candidate):
        original_request=self.rpc.request
        starts=0
        def create_unloaded(method,params):
            nonlocal starts
            result=original_request(method,params)
            if method=='thread/start':
                starts+=1
                if starts==1:
                    self.rpc.status[self.thread_id]='active';self.rpc.unmaterialized=True
            return result
        self.rpc.request=create_unloaded
        self.fresh(rid); self.bridge.tick()
        with self.server.projects.connection() as db:
            db.execute('UPDATE root_fresh_requests SET new_thread_id=?,error=?,updated_at=updated_at+1 WHERE id=?',
                (candidate,'Candidate recovery required: exact retained thread is not materialized: thread not loaded: '+candidate,rid))
            attempt='00000000-0000-4000-8000-000000000051'
            db.execute('INSERT INTO root_fresh_recovery_attempts VALUES(?,?,?,?,?,?,?,?)',
                (attempt,rid,'default','a','mac',self.thread_id,candidate,time.time()))
            db.execute('INSERT INTO root_fresh_recovery_events(attempt_id,event,detail,recorded_at) VALUES(?,?,?,?)',
                (attempt,'not_loaded','Exact original candidate remains unavailable.',time.time()))
            db.commit()
        self.rpc.new=candidate;self.rpc.status[candidate]='idle'; self.rpc.queues[candidate]=[];self.rpc.history[candidate]=[]
        self.root_candidates.add(candidate)
        self.rpc.status[self.thread_id]='idle'
        return attempt

    def test_same_request_replacement_preserves_failed_candidate_and_waits_for_exact_new_start(self):
        rid='41900475-4bf1-419d-897d-6c227f0d432c'
        original='01a146d5-e07a-74a2-af98-8c8a18b98034'
        reconciliation='bc2716b4-1362-4480-aa36-5403ee0d9a13'
        attempt=self.prepare_failed_original_candidate(rid,original)
        result=self.root('fresh-replace',expected_thread=self.thread_id,request_id=rid,reconciliation_id=reconciliation,confirm=True)
        self.assertEqual(result['replacement']['state'],'requested')
        self.assertEqual(result['replacement']['previous_candidate_id'],original)
        self.assertIsNone(result['replacement']['replacement_candidate_id'])
        self.assertEqual(result['next_action'],'wait_for_candidate_validation')
        with self.server.projects.connection() as db:
            row=db.execute('SELECT candidate_thread_id FROM root_fresh_recovery_attempts WHERE attempt_id=?',(attempt,)).fetchone()
            self.assertEqual(row['candidate_thread_id'],original)
        self.rpc.calls=[]
        self.rpc.read_unavailable.add(self.thread_id)
        self.rpc.unmaterialized=False
        self.capture_root_candidate=True
        self.bridge.tick()
        current=self.status(rid)
        self.assertEqual(current['replacement']['state'],'candidate_verification_required')
        replacement=current['replacement']['replacement_candidate_id']
        self.assertTrue(replacement)
        self.assertNotEqual(replacement,original)
        self.assertEqual(current['new_thread_id'],replacement)
        self.assertEqual(current['handoff_proof_version'],2)
        self.assertEqual(self.status(rid)['replacement']['previous_candidate_id'],original)
        self.assertEqual(len([call for call in self.rpc.calls if call[0]=='thread/start']),1)
        self.assertFalse(any(call[0]=='thread/queue/add' for call in self.rpc.calls))
        old_reads=[method for method,params in self.rpc.calls if params.get('threadId')==self.thread_id and method in ('thread/read','thread/resume')]
        self.assertEqual(old_reads[:3],['thread/read','thread/resume','thread/read'])
        self.assertEqual(old_reads.count('thread/resume'),1)
        self.assertFalse(any(method in ('thread/read','thread/resume','thread/turns/list','thread/items/list')
                             and params.get('threadId')==original for method,params in self.rpc.calls))
        repeated=self.root('fresh-replace',expected_thread=self.thread_id,request_id=rid,reconciliation_id=reconciliation,confirm=True)
        self.assertEqual(repeated['replacement']['replacement_candidate_id'],replacement)
        self.assertEqual(len([call for call in self.rpc.calls if call[0]=='thread/start']),1)
        self.bridge.tick()
        progressed=self.status(rid)
        self.assertEqual(progressed['new_thread_id'],replacement)
        self.assertEqual(progressed['handoff_proof_version'],2)
        self.assertEqual(self.server.roots.config()['thread_id'],replacement)
        self.assertEqual(len([call for call in self.rpc.calls if call[0]=='thread/queue/add']),1)
        self.publish_consumption=True
        self.bridge.tick()
        consumed=self.status(rid)
        self.assertEqual(consumed['status'],'completed')
        self.publish_response=True;self.response_phase='final_answer'
        self.rpc.calls=[];self.bridge.tick()
        verified=self.status(rid)
        self.assertEqual(verified['response_state'],'observed')
        self.assertTrue(verified['boot_consumed'])
        self.assertFalse(any(method in ('thread/read','thread/resume','thread/turns/list','thread/items/list')
                             and params.get('threadId')==original for method,params in self.rpc.calls))

    def test_replacement_uncertain_start_is_never_replayed(self):
        rid='41900475-4bf1-419d-897d-6c227f0d432c'
        original='01a146d5-e07a-74a2-af98-8c8a18b98034'
        reconciliation='bc2716b4-1362-4480-aa36-5403ee0d9a13'
        self.prepare_failed_original_candidate(rid,original)
        self.root('fresh-replace',expected_thread=self.thread_id,request_id=rid,reconciliation_id=reconciliation,confirm=True)
        original_request=self.rpc.request
        def lost_start(method,params):
            if method=='thread/start' and self.capture_root_candidate:
                self.capture_root_candidate=False
                original_request(method,params)
                raise TransportError('lost correlated start response')
            return original_request(method,params)
        self.capture_root_candidate=True; self.rpc.request=lost_start
        self.bridge.tick()
        self.rpc.request=original_request
        after=self.status(rid)
        self.assertEqual(after['replacement']['state'],'start_uncertain')
        self.assertEqual(after['new_thread_id'],original)
        count=len([call for call in self.rpc.calls if call[0]=='thread/start'])
        self.bridge.tick();self.bridge.tick()
        self.assertEqual(len([call for call in self.rpc.calls if call[0]=='thread/start']),count)
        self.assertEqual(self.status(rid)['new_thread_id'],original)

    def test_replacement_waits_for_old_turn_and_queue_then_starts_once(self):
        rid='41900475-4bf1-419d-897d-6c227f0d432c'
        original='01a146d5-e07a-74a2-af98-8c8a18b98034'
        reconciliation='bc2716b4-1362-4480-aa36-5403ee0d9a13'
        self.prepare_failed_original_candidate(rid,original)
        self.rpc.status[self.thread_id]='active'
        self.root('fresh-replace',expected_thread=self.thread_id,request_id=rid,reconciliation_id=reconciliation,confirm=True)
        self.rpc.calls=[]
        self.bridge.tick()
        result=self.status(rid)
        self.assertEqual(result['replacement']['state'],'requested')
        self.assertIn('current canonical old Root turn',result['replacement']['error'])
        with self.server.projects.connection() as db:
            event=db.execute('SELECT event FROM root_fresh_replacement_events WHERE reconciliation_id=? ORDER BY event_id DESC LIMIT 1',(reconciliation,)).fetchone()
            self.assertEqual(event['event'],'waiting_old_root_idle')
        self.assertEqual(result['new_thread_id'],original)
        self.assertFalse(any(method=='thread/start' for method,_ in self.rpc.calls))
        self.assertFalse(any(method in ('thread/queue/add','thread/queue/start') for method,_ in self.rpc.calls))
        self.rpc.status[self.thread_id]='idle'
        self.rpc.queues[self.thread_id]=[dict(id='old-native-pending',clientUserMessageId='unrelated',input=[])]
        self.bridge.tick()
        queued=self.status(rid)
        self.assertEqual(queued['replacement']['state'],'requested')
        self.assertIn('queue to drain',queued['replacement']['error'])
        with self.server.projects.connection() as db:
            event=db.execute('SELECT event FROM root_fresh_replacement_events WHERE reconciliation_id=? ORDER BY event_id DESC LIMIT 1',(reconciliation,)).fetchone()
            self.assertEqual(event['event'],'waiting_old_root_idle')
        self.assertFalse(any(method=='thread/start' for method,_ in self.rpc.calls))
        self.rpc.queues[self.thread_id]=[]
        self.capture_root_candidate=True
        self.bridge.tick()
        progressed=self.status(rid)
        self.assertEqual(progressed['replacement']['state'],'candidate_verification_required')
        self.assertNotEqual(progressed['replacement']['replacement_candidate_id'],original)
        self.assertEqual(progressed['new_thread_id'],progressed['replacement']['replacement_candidate_id'])
        self.assertEqual(len([1 for method,_ in self.rpc.calls if method=='thread/start']),1)
        self.assertFalse(any(method in ('thread/queue/add','thread/queue/start') for method,_ in self.rpc.calls))

    def test_retire_uncertain_replacement_releases_only_its_gates_and_allows_new_request(self):
        rid='41900475-4bf1-419d-897d-6c227f0d432c'
        original='01a146d5-e07a-74a2-af98-8c8a18b98034'
        reconciliation='bc2716b4-1362-4480-aa36-5403ee0d9a13'
        retirement='28134343-fb30-4d1e-a62a-bf02e5d593d2'
        self.prepare_failed_original_candidate(rid,original)
        self.root('fresh-replace',expected_thread=self.thread_id,request_id=rid,reconciliation_id=reconciliation,confirm=True)
        restart=RootRestart(self.server.roots)
        restart.replacement_event(rid,reconciliation,'preflight_verified',detail='fixture preflight only')
        restart.replacement_event(rid,reconciliation,'start_started',detail='fixture represents one uncertain returned start')
        restart.replacement_event(rid,reconciliation,'start_uncertain',detail='thread/start returned a candidate with settings drift; no retry or route change.')
        scopes=restart.scope(rid)
        sentinel='99999999-8888-4777-8666-555555555555'
        for item in scopes:
            with contextlib.closing(Coordinator(self.server.roots.path(item['project_id']),item['root_session'])) as coord:
                coord.db.execute('INSERT INTO root_fresh_gates VALUES(?,?,?,?,1)',(sentinel,item['root_session'],self.thread_id,None))
        self.rpc.calls=[]
        retired=self.root('fresh-retire',expected_thread=self.thread_id,request_id=rid,
            reconciliation_id=reconciliation,retirement_id=retirement,confirm=True)
        self.assertEqual(retired['status'],'retired')
        self.assertEqual(retired['handoff_state'],'retired_uncertain')
        self.assertFalse(retired['handoff_ready'])
        self.assertEqual(retired['next_action'],'start_new_fresh_root_request')
        self.assertIn('no native closure or cleanup is asserted',retired['blocker'])
        self.assertEqual(retired['retirement']['state'],'retired')
        self.assertEqual(retired['retirement']['retirement_id'],retirement)
        self.assertEqual(retired['replacement']['state'],'start_uncertain')
        self.assertEqual(retired['replacement']['previous_candidate_id'],original)
        self.assertIsNone(retired['replacement']['replacement_candidate_id'])
        self.assertEqual(retired['new_thread_id'],original)
        self.assertEqual(self.server.roots.config()['thread_id'],self.thread_id)
        for item in scopes:
            with contextlib.closing(Coordinator(self.server.roots.path(item['project_id']),item['root_session'])) as coord:
                self.assertEqual(coord.db.execute('SELECT active FROM root_fresh_gates WHERE request_id=?',(rid,)).fetchone()['active'],0)
                self.assertEqual(coord.db.execute('SELECT active FROM root_fresh_gates WHERE request_id=?',(sentinel,)).fetchone()['active'],1)
        with self.server.projects.connection() as db:
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute("UPDATE root_fresh_retirements SET outcome='native_closed' WHERE request_id=?",(rid,))
        replay=self.root('fresh-retire',expected_thread=self.thread_id,request_id=rid,
            reconciliation_id=reconciliation,retirement_id=retirement,confirm=True)
        self.assertEqual(replay['retirement']['retirement_id'],retirement)
        self.assertFalse(any(method in ('thread/start','thread/queue/add','thread/queue/start','thread/resume') for method,_ in self.rpc.calls))
        next_request=str(uuid.uuid4())
        pending=self.fresh(next_request)
        self.assertEqual(pending['status'],'pending')
        self.assertEqual(self.server.roots.config()['thread_id'],self.thread_id)
        self.assertFalse(any(method in ('thread/start','thread/queue/add','thread/queue/start','thread/resume') for method,_ in self.rpc.calls))


# The reusable fixture also contains test methods; expose only this task's cases.
for name in list(vars(root_tests.RootOrchestratorTests)):
    if name.startswith('test_') and name not in vars(RootFreshStartTests):
        setattr(RootFreshStartTests,name,None)
