"""Root-owned administrative retirement: actual HTTP/CLI and host fences."""
import contextlib
import io
import os
import sqlite3
import subprocess
import uuid
from unittest import mock
from agent_chat import cli
from agent_chat.core import Coordinator
from agent_chat.remote import RemoteCoordError
from agent_chat.root_team_reset import RootTeamReset
from test_remote_web import TOKEN
import test_root_orchestrator as fixtures


class RootTeamResetTests(fixtures.RootOrchestratorTests):
    def setUp(self):
        super().setUp()
        self.native={sid:str(uuid.uuid4()) for sid in ('po','worker')}
        for sid,thread in self.native.items():
            self.call('register',session=sid,agent=sid)
            self.call('bind',session=sid,thread=thread)
            self.rpc.status[thread]='active';self.rpc.queues[thread]=[];self.rpc.history[thread]=[]
        with contextlib.closing(Coordinator(self.db)) as coord:
            coord.db.execute('INSERT INTO project_orchestrators VALUES(?,?)',('po','a'))
            coord.db.execute('UPDATE bridge_bindings SET parent_session=? WHERE session_id=?',('po','worker'))
        with self.server.projects.connection() as db:
            db.execute("UPDATE root_projects SET agent='po',orchestrator_session='po' WHERE project_id='default'")
        code,raw,_=self.request('POST','/api/messages',dict(to='a',body='Hard reset the existing project team. Preserve project history.'),
            headers={'X-Agent-Chat-CSRF':self.server.csrf_token})
        self.assertEqual(code,200,raw)
        self.authority=subprocess.check_output(['jq','-r','.id'],input=raw).decode().strip()
        self.assertNotEqual(self.authority,'null',raw)
        message=self.call('send',to='worker',body='Prepared old input')
        self.prepared=self.state.prepare(self.native['worker'],[dict(id=message['id'],recipient_session='worker')],'Old input')
        self.control=self.call('agent-model',session='po',to='worker',expected_thread=self.native['worker'],request_id=str(uuid.uuid4()),confirm=True,model='gpt-6.1-sol',reasoning_effort='high')
        with contextlib.closing(Coordinator(self.db)) as coord:
            coord.db.execute("INSERT INTO resources VALUES('file:held.py','worker','held-rid','fixture-token',1,2,1,NULL)")
            coord.db.execute("INSERT INTO resource_queue(resource,session,queued_at) VALUES('file:held.py','worker',3)")
            coord.db.execute("INSERT INTO guarded_runs VALUES('unknown-local','file:held.py','held-rid','worker',999999,NULL,1,NULL)")
            coord.db.execute("INSERT INTO remote_reservation_outcomes VALUES('held-rid','file:held.py','worker','mac','pending',NULL,NULL,NULL,1,NULL)")
            self.before=self.preserved(coord.db)

    def preserved(self,db):
        rows=[db.execute(query).fetchall() for query in (
            'SELECT id,body,acked_at FROM messages ORDER BY seq',
            'SELECT * FROM guarded_runs ORDER BY run_id',
            'SELECT * FROM remote_reservation_outcomes ORDER BY reservation_id',
            'SELECT id,status FROM agent_control_requests ORDER BY id')]
        rows.insert(3,db.execute('SELECT id,status,queue_id,payload FROM bridge_jobs WHERE thread_id IN (?,?) ORDER BY id',
            tuple(self.native.values())).fetchall())
        return tuple(rows)

    def preview(self,**changes):
        return self.root('team-reset-preview',**dict(dict(project='default',message_ids=[self.authority]),**changes))

    def reset(self,rid,digest,**changes):
        return self.root('team-reset',**dict(dict(project='default',message_ids=[self.authority],request_id=rid,expected_digest=digest,confirm=True),**changes))

    def test_root_cli_reset_preserves_unknown_work_and_revokes_old_dispatch(self):
        output=io.StringIO()
        with mock.patch.dict(os.environ,{'AGENT_CHAT_HOST_ID':'mac'}),contextlib.redirect_stdout(output):
            self.assertEqual(cli.main(['--server',self.url,'--api-token',TOKEN,'--session','a',
                'root','team-reset-preview','--project-id','default','--operator-message',self.authority]),0)
        raw=output.getvalue().encode()
        subprocess.run(['jq','-e','.status=="preview" and .authority[0].origin=="authenticated_operator_ui" and (.targets|map(.session_id)|sort)==["po","worker"] and .ownership_disposition[0].process_closure=="unverified"'],input=raw,capture_output=True,check=True)
        digest=subprocess.check_output(['jq','-r','.digest'],input=raw).decode().strip();rid=str(uuid.uuid4())
        result=self.reset(rid,digest)
        self.assertEqual(result['status'],'completed');self.assertFalse(result['native_processes_stopped'])
        self.assertEqual(sorted(result['retired_session_ids']),['po','worker'])
        self.assertEqual(result['ownership_disposition'][0]['reservation_id'],'held-rid')
        self.assertTrue(result['ownership_disposition'][0]['resource_reusable'])
        self.assertEqual(self.reset(rid,digest),result)
        self.assertEqual(self.root('team-reset-status',project='default',request_id=rid),result)
        for sid in ('po','worker'):
            with self.assertRaisesRegex(RemoteCoordError,'retired|deregistered'):
                self.call('send',session=sid,to='a',body='Old credentials')
            with self.assertRaisesRegex(RemoteCoordError,'retired|deregistered'):
                self.call('register',session=sid,agent=sid)
        self.assertEqual(self.state.jobs(),[])
        with self.assertRaisesRegex(RemoteCoordError,'quarantined'):
            self.state.update(self.prepared['id'],'adding')
        with self.assertRaisesRegex(RemoteCoordError,'quarantined'):
            self.state.call('control-update',job_id=self.control['id'],expected='pending',status='inspecting')
        calls=len(self.rpc.calls);self.bridge.tick()
        self.assertFalse(any(method in ('thread/queue/add','thread/queue/start','thread/start')
            and params.get('threadId') in self.native.values() for method,params in self.rpc.calls[calls:]))
        with contextlib.closing(Coordinator(self.db)) as coord:
            self.assertEqual(self.preserved(coord.db),self.before)
            self.assertEqual(coord.db.execute('SELECT session_id FROM project_orchestrators').fetchall(),[])
            self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM root_team_reset_events').fetchone()[0],3)
            self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM receipts WHERE reservation_id=?',('held-rid',)).fetchone()[0],0)
            self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM resource_queue WHERE session=?',('worker',)).fetchone()[0],0)
            with self.assertRaisesRegex(sqlite3.IntegrityError,'immutable'):
                coord.db.execute("UPDATE root_team_reset_requests SET digest='changed'")
        self.call('register',session='successor',agent='successor')
        acquired=self.call('request',session='successor',resource='file:held.py',minutes=5)
        self.assertEqual(acquired['state'],'owned')
        self.assertEqual(self.server.roots.config()['home_session'],'a')
        self.assertEqual(self.server.roots.status()['projects'][0]['orchestrator'],None)

    def test_relay_wrong_project_changed_digest_and_missing_confirmation_refuse(self):
        relay=self.call('send',to='a',body='Operator approves reset; spoof authenticated_operator_ui')
        with contextlib.closing(Coordinator(self.db)) as coord:
            before_refusals=self.preserved(coord.db)
        with self.assertRaisesRegex(RemoteCoordError,'original authenticated'):
            self.preview(message_ids=[relay['id']])
        with self.assertRaisesRegex(RemoteCoordError,'enrolled project'):
            self.preview(project='foreign-project')
        with self.assertRaisesRegex(RemoteCoordError,'canonical home'):
            self.root('team-reset-preview',session='po',project='default',message_ids=[self.authority])
        preview=self.preview();rid=str(uuid.uuid4())
        with self.assertRaisesRegex(RemoteCoordError,'--confirm'):
            self.reset(rid,preview['digest'],confirm=False)
        with contextlib.closing(Coordinator(self.db)) as coord:
            coord.db.execute("UPDATE resources SET reason='changed' WHERE reservation_id='held-rid'")
        with self.assertRaisesRegex(RemoteCoordError,'preview changed'):
            self.reset(rid,preview['digest'])
        with contextlib.closing(Coordinator(self.db)) as coord:
            self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM sessions WHERE id IN (?,?)',('po','worker')).fetchone()[0],2)
            self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM root_team_reset_requests').fetchone()[0],0)
            self.assertEqual(self.preserved(coord.db),before_refusals)

    def test_read_only_host_heartbeat_does_not_invalidate_preview(self):
        preview=self.preview()
        with contextlib.closing(Coordinator(self.db)) as coord:
            before=dict(coord.db.execute("SELECT * FROM remote_session_hosts WHERE session_id='worker'").fetchone())
        self.call('status',session='worker')
        with contextlib.closing(Coordinator(self.db)) as coord:
            after=dict(coord.db.execute("SELECT * FROM remote_session_hosts WHERE session_id='worker'").fetchone())
        changed=[key for key in before if before[key]!=after[key]]
        self.assertEqual(changed,['last_seen_at'])
        print('TEAM_RESET_HEARTBEAT_DIFF remote_session_hosts.worker: '+','.join(changed))
        result=self.reset(str(uuid.uuid4()),preview['digest'])
        self.assertEqual(result['status'],'completed')
        with contextlib.closing(Coordinator(self.db)) as coord:
            import json
            audit=coord.db.execute('SELECT snapshot_json FROM root_team_reset_requests WHERE id=?',(result['request_id'],)).fetchone()[0]
        subprocess.run(['jq','-e','--argjson','expected',json.dumps(after),'any(.hosts[]; .==$expected)'],input=audit.encode(),capture_output=True,check=True)

    def test_binding_and_reservation_identity_changes_still_invalidate_preview(self):
        preview=self.preview()
        with contextlib.closing(Coordinator(self.db)) as coord:
            coord.db.execute("UPDATE bridge_bindings SET thread_id=? WHERE session_id='worker'",(str(uuid.uuid4()),))
        with self.assertRaisesRegex(RemoteCoordError,'preview changed'):
            self.reset(str(uuid.uuid4()),preview['digest'])
        for change in ("host_id='different-host'", "registered_at=registered_at+1"):
            preview=self.preview()
            with contextlib.closing(Coordinator(self.db)) as coord:
                coord.db.execute("UPDATE remote_session_hosts SET "+change+" WHERE session_id='worker'")
            with self.assertRaisesRegex(RemoteCoordError,'preview changed'):
                self.reset(str(uuid.uuid4()),preview['digest'])
        preview=self.preview()
        with contextlib.closing(Coordinator(self.db)) as coord:
            coord.db.execute("UPDATE resources SET token='changed-reservation-token' WHERE reservation_id='held-rid'")
        with self.assertRaisesRegex(RemoteCoordError,'preview changed'):
            self.reset(str(uuid.uuid4()),preview['digest'])
        with contextlib.closing(Coordinator(self.db)) as coord:
            self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM root_team_reset_requests').fetchone()[0],0)
            self.assertEqual(coord.db.execute("SELECT COUNT(*) FROM sessions WHERE id IN ('po','worker')").fetchone()[0],2)

    def test_registry_finalization_retry_has_no_retirement_replay_and_blocks_new_team(self):
        preview=self.preview();rid=str(uuid.uuid4());original=RootTeamReset.finalize
        with mock.patch.object(RootTeamReset,'finalize',side_effect=OSError('injected registry transport failure')):
            with self.assertRaisesRegex(RemoteCoordError,'injected registry'):
                self.reset(rid,preview['digest'])
        status=self.root('team-reset-status',project='default',request_id=rid)
        self.assertEqual(status['status'],'quarantined')
        with self.assertRaisesRegex(RemoteCoordError,'finalization is pending'):
            self.configure('default')
        result=self.reset(rid,preview['digest']);self.assertEqual(result['status'],'completed')
        with contextlib.closing(Coordinator(self.db)) as coord:
            self.assertEqual(self.preserved(coord.db),self.before)
            self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM root_team_reset_events').fetchone()[0],3)

    def handoff_race(self,phase):
        import threading
        # This fixture's separate pending-model contract intentionally stops
        # admission. Seed a settled model intent for the native-wake race.
        with contextlib.closing(Coordinator(self.db)) as coord:
            coord.db.execute("UPDATE agent_control_requests SET status='cancelled' WHERE id=?",(self.control['id'],))
            coord.db.execute("UPDATE agent_controls SET mode='running' WHERE target_session='worker'")
        thread=self.native['worker']
        queue=None
        if phase=='starting':
            self.state.update(self.prepared['id'],'adding')
            queue=self.rpc.request('thread/queue/add',dict(threadId=thread,clientUserMessageId=self.prepared['id'],input=[{'type':'text','text':'Old input','text_elements':[]}]))['queuedSubmission']['id']
            self.state.update(self.prepared['id'],'queued',queue_id=queue)
            self.rpc.status[thread]='idle'
        admitted=threading.Event();release=threading.Event();errors=[]
        def host():
            try:
                self.state.update(self.prepared['id'],phase)
                admitted.set()
                if not release.wait(10): raise AssertionError('fixture native handoff release missing')
                if phase=='adding':
                    result=self.rpc.request('thread/queue/add',dict(threadId=thread,clientUserMessageId=self.prepared['id'],input=[{'type':'text','text':'Old input','text_elements':[]}]))
                    self.state.update(self.prepared['id'],'queued',queue_id=result['queuedSubmission']['id'])
                else:
                    self.rpc.request('thread/queue/start',dict(threadId=thread,queuedSubmissionId=queue))
                    self.state.update(self.prepared['id'],'dispatched')
            except BaseException as error:
                errors.append(error);admitted.set()
        worker=threading.Thread(target=host);worker.start()
        try:
            self.assertTrue(admitted.wait(5));self.assertEqual(errors,[])
            preview=self.preview();rid=str(uuid.uuid4())
            waiting=self.reset(rid,preview['digest'])
            self.assertEqual(waiting['status'],'quiescing')
            self.assertEqual(waiting['retired_session_ids'],[])
            self.assertFalse(waiting['ownership_disposition'][0]['resource_reusable'])
            self.assertEqual(waiting['outstanding_native_handoffs'][0]['phase'],phase)
            with contextlib.closing(Coordinator(self.db)) as coord:
                self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM sessions WHERE id IN (?,?)',('po','worker')).fetchone()[0],2)
                self.assertEqual(coord.db.execute("SELECT owner_session FROM resources WHERE name='file:held.py'").fetchone()[0],'worker')
            with self.assertRaisesRegex(RemoteCoordError,'quarantined'):
                self.state.update(self.prepared['id'],phase)
        finally:
            release.set();worker.join(10)
        self.assertFalse(worker.is_alive());self.assertEqual(errors,[])
        result=self.reset(rid,preview['digest']);self.assertEqual(result['status'],'completed')
        calls=[method for method,params in self.rpc.calls if params.get('threadId')==thread]
        self.assertEqual(calls.count('thread/queue/add'),1)
        self.assertEqual(calls.count('thread/queue/start'),1 if phase=='starting' else 0)
        with contextlib.closing(Coordinator(self.db)) as coord:
            self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM root_native_handoffs WHERE active=1').fetchone()[0],0)
            self.assertEqual(coord.db.execute("SELECT status FROM bridge_jobs WHERE id=?",(self.prepared['id'],)).fetchone()[0],
                'queued' if phase=='adding' else 'dispatched')
        print('TEAM_RESET_HANDOFF',phase,rid,self.prepared['id'],'quiescing -> settled -> completed; native call once; host thread joined')

    def test_reset_waits_for_admitted_queue_add_before_retirement(self):
        self.handoff_race('adding')

    def test_reset_waits_for_admitted_queue_start_before_retirement(self):
        self.handoff_race('starting')

    def test_retired_child_recipient_is_fenced_on_retained_root_parent_thread(self):
        self.call('register',session='legacy-child',agent='legacy-child')
        with contextlib.closing(Coordinator(self.db)) as coord:
            coord.db.execute('INSERT INTO bridge_bindings(session_id,thread_id,parent_session,agent_path,updated_at) VALUES(?,NULL,?,?,?)',
                ('legacy-child','a','/legacy-child',1))
        message=self.call('send',to='legacy-child',body='Historical child route on retained root')
        pending=dict(id=message['id'],recipient_session='legacy-child')
        prepared=self.state.prepare(self.thread_id,[pending],'Retained parent forwarding')
        preview=self.preview();result=self.reset(str(uuid.uuid4()),preview['digest'])
        self.assertEqual(result['status'],'completed')
        self.assertIsNone(self.state.resolve('legacy-child'))
        self.assertNotIn(message['id'],[row['id'] for row in self.state.pending()])
        with self.assertRaisesRegex(RemoteCoordError,'quarantined'):
            self.state.prepare(self.thread_id,[pending],'New forwarding after reset')
        with self.assertRaisesRegex(RemoteCoordError,'quarantined'):
            self.state.update(prepared['id'],'adding')
        self.assertEqual(self.server.roots.config()['thread_id'],self.thread_id)

    def test_uncertain_current_start_handoff_does_not_allow_retirement_or_queue_replay(self):
        with contextlib.closing(Coordinator(self.db)) as coord:
            coord.db.execute("UPDATE agent_control_requests SET status='cancelled' WHERE id=?",(self.control['id'],))
            coord.db.execute("UPDATE agent_controls SET mode='running' WHERE target_session='worker'")
        thread=self.native['worker']
        self.state.update(self.prepared['id'],'adding')
        queue=self.rpc.request('thread/queue/add',dict(threadId=thread,clientUserMessageId=self.prepared['id'],input=[{'type':'text','text':'Old input','text_elements':[]}]))['queuedSubmission']['id']
        self.state.update(self.prepared['id'],'queued',queue_id=queue)
        self.state.update(self.prepared['id'],'starting')
        self.state.update(self.prepared['id'],'uncertain',error='Native start response timed out; exact outcome unknown')
        preview=self.preview();rid=str(uuid.uuid4());waiting=self.reset(rid,preview['digest'])
        self.assertEqual(waiting['status'],'quiescing')
        self.assertEqual(waiting['retired_session_ids'],[])
        self.assertEqual(waiting['outstanding_native_handoffs'][0]['phase'],'starting')
        with self.assertRaisesRegex(RemoteCoordError,'quarantined'):
            self.state.update(self.prepared['id'],'queued',queue_id=queue)
        with self.assertRaisesRegex(RemoteCoordError,'quarantined'):
            self.state.update(self.prepared['id'],'starting')
        retry=self.reset(rid,preview['digest']);self.assertEqual(retry['status'],'quiescing')
        with contextlib.closing(Coordinator(self.db)) as coord:
            self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM sessions WHERE id IN (?,?)',('po','worker')).fetchone()[0],2)
            self.assertEqual(coord.db.execute("SELECT owner_session FROM resources WHERE name='file:held.py'").fetchone()[0],'worker')
            self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM root_native_handoffs WHERE active=1').fetchone()[0],1)
        self.assertFalse(any(method=='thread/queue/start' and params.get('threadId')==thread for method,params in self.rpc.calls))
        print('TEAM_RESET_UNCERTAIN',rid,self.prepared['id'],'quiescing; exact current starting handoff retained; no queue/start replay')

    def test_direct_cli_intent_does_not_fabricate_operator_ui_authority(self):
        args=['--server',self.url,'--api-token',TOKEN,'--session','a','root']
        output=io.StringIO()
        with mock.patch.dict(os.environ,{'AGENT_CHAT_HOST_ID':'mac'}),contextlib.redirect_stdout(output):
            self.assertEqual(cli.main(args+['team-reset-preview','--project-id','default']),0)
        raw=output.getvalue().encode()
        subprocess.run(['jq','-e','.authority[0].operator_instruction_attested==false and .authority[0].origin=="canonical_root_control" and .authority[0].body==null'],input=raw,capture_output=True,check=True)
        digest=subprocess.check_output(['jq','-r','.digest'],input=raw).decode().strip();rid=str(uuid.uuid4())
        output=io.StringIO()
        with mock.patch.dict(os.environ,{'AGENT_CHAT_HOST_ID':'mac'}),contextlib.redirect_stdout(output):
            self.assertEqual(cli.main(args+['team-reset','--project-id','default','--request-id',rid,'--expected-digest',digest,'--confirm']),0)
        subprocess.run(['jq','-e','.status=="completed" and .authority[0].operator_instruction_attested==false and (.retired_session_ids|length)==2 and (.ownership_disposition|length)==1 and .native_processes_stopped==false'],input=output.getvalue().encode(),capture_output=True,check=True)
        print(subprocess.check_output(['jq','-c','{request_id,project,status,retired_session_ids,ownership_disposition,queued_claims_retired,native_processes_stopped,new_team_created,authority_kind:.authority[0].origin,operator_instruction_attested:.authority[0].operator_instruction_attested}'],input=output.getvalue().encode()).decode().strip())

    def test_current_model_handoff_settles_before_retirement(self):
        import threading
        from agent_chat.control_worker import ControlWorker
        from test_agent_model import ModelRpc,TARGET
        target='flight-model';thread=str(uuid.uuid4())
        self.call('register',session=target,agent=target);self.call('bind',session=target,thread=thread)
        self.call('inbox')
        self.root('delegate-management',project='default',po='po',target=target,expected_thread=thread,expected_host='mac',request_id=str(uuid.uuid4()),confirm=True)
        rpc=ModelRpc(thread);rpc.status[thread]='idle';rpc.turn['status']='completed'
        self.bridge.rpc=rpc
        job=self.call('agent-model',session='po',to=target,expected_thread=thread,request_id=str(uuid.uuid4()),
            confirm=True,model=TARGET,reasoning_effort='high',allow_cold_resume=True)
        entered=threading.Event();release=threading.Event();errors=[];original=rpc.request
        def request(method,params):
            if method=='thread/resume' and params.get('model'):
                entered.set()
                if not release.wait(10): raise AssertionError('fixture model release missing')
            return original(method,params)
        rpc.request=request
        def host():
            try: ControlWorker(self.bridge).advance(next(item for item in self.state.control_jobs() if item['id']==job['id']))
            except BaseException as error: errors.append(error);entered.set()
        worker=threading.Thread(target=host);worker.start()
        try:
            if not entered.wait(5):
                with contextlib.closing(Coordinator(self.db)) as coord:
                    details=tuple(coord.db.execute('SELECT status,error FROM agent_control_requests WHERE id=?',(job['id'],)).fetchone())
                self.fail('Model native mutation was not reached: '+str(details))
            self.assertEqual(errors,[])
            preview=self.preview();rid=str(uuid.uuid4());pending=self.reset(rid,preview['digest'])
            self.assertEqual(pending['status'],'quiescing')
            self.assertEqual(pending['outstanding_native_handoffs'][0]['kind'],'control')
        finally: release.set();worker.join(10)
        self.assertFalse(worker.is_alive());self.assertEqual(errors,[])
        with contextlib.closing(Coordinator(self.db)) as coord:
            self.assertEqual(coord.db.execute('SELECT status FROM agent_control_requests WHERE id=?',(job['id'],)).fetchone()[0],'completed')
        self.assertEqual(self.reset(rid,preview['digest'])['status'],'completed')
        self.assertEqual(len(rpc.mutations),1)
        print('TEAM_RESET_MODEL',rid,job['id'],'definite model readback completed; native mutation once; host joined')

    def test_current_thread_creation_result_is_retained_without_boot_after_quarantine(self):
        import threading
        from agent_chat.reset_worker import ResetWorker
        self.call('register',session='flight-reset',agent='flight-reset')
        old=str(uuid.uuid4());self.call('bind',session='flight-reset',thread=old)
        self.call('inbox')
        self.root('delegate-management',project='default',po='po',target='flight-reset',expected_thread=old,expected_host='mac',request_id=str(uuid.uuid4()),confirm=True)
        self.rpc.status[old]='idle';self.rpc.queues[old]=[];self.rpc.history[old]=[]
        job=self.call('session-reset',session='po',to='flight-reset',expected_thread=old,prompt='Retained old-team boot',request_id=str(uuid.uuid4()),confirm=True)
        entered=threading.Event();release=threading.Event();errors=[];original=self.rpc.request
        def request(method,params):
            if method=='thread/start':
                entered.set()
                if not release.wait(10): raise AssertionError('fixture creation release missing')
            return original(method,params)
        self.rpc.request=request
        def host():
            try: ResetWorker(self.bridge).advance(self.state.reset_job(job['id']))
            except BaseException as error: errors.append(error);entered.set()
        worker=threading.Thread(target=host);worker.start()
        try:
            self.assertTrue(entered.wait(5));self.assertEqual(errors,[])
            preview=self.preview();rid=str(uuid.uuid4());pending=self.reset(rid,preview['digest'])
            self.assertEqual(pending['status'],'quiescing')
            self.assertEqual(pending['outstanding_native_handoffs'][0]['phase'],'creating')
        finally: release.set();worker.join(10)
        self.assertFalse(worker.is_alive())
        self.assertTrue(all('quarantined' in str(error) for error in errors),errors)
        with contextlib.closing(Coordinator(self.db)) as coord:
            saved=coord.db.execute('SELECT new_thread_id,status FROM session_resets WHERE id=?',(job['id'],)).fetchone()
            self.assertTrue(saved['new_thread_id']);self.assertEqual(saved['status'],'created')
        self.assertEqual(self.reset(rid,preview['digest'])['status'],'completed')
        self.assertEqual(sum(method=='thread/start' for method,_ in self.rpc.calls),1)
        self.assertFalse(any(method=='thread/queue/add' and params.get('threadId')==saved['new_thread_id'] for method,params in self.rpc.calls))
        print('TEAM_RESET_CREATION',rid,job['id'],saved['new_thread_id'],'created result retained; no boot; no closure claimed; host joined')

    def test_fresh_po_native_boot_and_guarded_source_work_continue_after_reset(self):
        import sys
        preview=self.preview();rid=str(uuid.uuid4());self.reset(rid,preview['digest'])
        self.call('inbox')
        created=self.configure('default',agent='fresh-po')
        self.bridge.tick()
        actor=created['creation']['target_session']
        status=self.call('session-reset-status',to=actor)
        self.assertEqual(status['resets'][0]['status'],'completed',status)
        self.assertTrue(status['thread_id'])
        self.assertNotIn(actor,('po','worker','a'))
        self.call('inbox',session=actor)
        owned=self.call('request',session=actor,resource='file:held.py',minutes=5)
        self.assertEqual(owned['state'],'owned')
        output=io.StringIO()
        with mock.patch.dict(os.environ,{'AGENT_CHAT_HOST_ID':'mac','AGENT_CHAT_STATE_DIR':str(self.tmp.name)+'/client-state'}),contextlib.redirect_stdout(output):
            self.assertEqual(cli.main(['--server',self.url,'--api-token',TOKEN,'--session',actor,'run',
                '--token',owned['token'],'file:held.py','--',sys.executable,'-c','print("guarded-source-continuation")']),0)
        self.bridge.tick()
        with contextlib.closing(Coordinator(self.db)) as coord:
            old=coord.db.execute("SELECT closed_at FROM guarded_runs WHERE run_id='unknown-local'").fetchone()
            self.assertIsNone(old['closed_at'])
            self.assertEqual(coord.db.execute("SELECT state FROM remote_reservation_outcomes WHERE reservation_id='held-rid'").fetchone()[0],'pending')
            runs=coord.db.execute('SELECT local_pid,closed_at,close_evidence_sha256 FROM remote_guarded_runs WHERE session_id=?',(actor,)).fetchall()
            self.assertTrue(runs);self.assertTrue(all(row['local_pid']>0 and row['closed_at'] and row['close_evidence_sha256'] for row in runs))
            self.assertEqual(coord.db.execute("SELECT COUNT(*) FROM supervisor_incidents WHERE target_session IN ('po','worker')").fetchone()[0],0)
            self.assertEqual(coord.db.execute('SELECT status FROM bridge_jobs WHERE id=?',(self.prepared['id'],)).fetchone()[0],'prepared')
        print('TEAM_RESET_CONTINUATION',rid,actor,status['thread_id'],'new PO native boot; actual guarded child exited with own closure evidence; old unknown guard/outcome unchanged; no old wake')


for name in list(vars(fixtures.RootOrchestratorTests)):
    if name.startswith('test_') and name not in vars(RootTeamResetTests):
        setattr(RootTeamResetTests,name,None)
