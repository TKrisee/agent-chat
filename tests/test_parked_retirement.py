import contextlib
import sqlite3
import time
import uuid
from unittest import mock

from agent_chat.core import Coordinator, CoordError
from agent_chat.remote import HttpClient, RemoteCoordError
from agent_chat.root_team_reset import handoff_begin, quarantined
from agent_chat.session_reset import SessionResetState
from test_remote_web import TOKEN
from test_parked_provision_http import ParkedProvisionHttpTests


class ParkedRetirementTests(ParkedProvisionHttpTests):
    def setup_attempts(self):
        project, route, path, params, state, bridge = self.prepare_parked()
        self.root('direct-create', **params)
        original = self.rpc.request
        def request(method, values):
            if method == 'model/list':
                return {'data':[{'model':'gpt-6.2-sol','supportedReasoningEfforts':[{'reasoningEffort':'high'}]}]}
            return original(method,values)
        with mock.patch.object(self.rpc,'request',side_effect=request):
            bridge.tick()
            self.root('turnover-bootstrap',project=project,request_id=params['request_id'],confirm=True)
            bridge.tick()
        with contextlib.closing(Coordinator(path,route['session'])) as coord:
            po = SessionResetState(coord).job(params['request_id'])
        client = HttpClient(self.url,TOKEN,project=project)
        client.call('/api/coord', {'op':'context','session':po['target_session'],'host_id':'mac','params':{}})
        attempts = []
        for label in ('worker-old-1','worker-old-2'):
            req = str(uuid.uuid4())
            payload = {'agent':label,'expected_thread':po['new_thread_id'],'prompt':'Retained brief',
                       'request_id':req,'bootstrap_id':params['request_id'],'confirm':True,
                       'model':'gpt-6.1-terra','reasoning_effort':'high','cwd':str(self.workspace)}
            result = client.call('/api/coord', {'op':'parked-agent-create','session':po['target_session'],'host_id':'mac','params':payload})
            with contextlib.closing(Coordinator(path,route['session'])) as coord:
                coord.db.execute("UPDATE session_resets SET status='uncertain',error='native outcome unknown' WHERE id=?",(req,))
                coord.db.execute('INSERT INTO root_native_handoffs VALUES(?,?,?,?,?,?,1,?,NULL,NULL)',
                    ('reset',req,'creating',result['target_session'],po['new_thread_id'],'mac',time.time()))
                coord.db.execute('INSERT INTO execution_epoch_handoffs VALUES(?,?,?,?,?,?,?,NULL,NULL)',
                    ('epoch-'+req,po['new_thread_id'],result['target_session'],'mac','fixture','thread/start',time.time()))
            attempts.append((req,result['target_session'],payload))
        return project,route,path,params,state,bridge,po,client,attempts

    def retire(self,project,params,po_job,original_request,**changes):
        args = dict(project=project,bootstrap_id=params['request_id'],stop_id=params['parked_stop'],
                    po=po_job['target_session'],expected_thread=po_job['new_thread_id'],expected_host='mac',
                    request_id=original_request,confirm=True)
        args.update(changes)
        return self.root('retire-unbound-parked',**args)

    def test_exact_retirement_preserves_unknown_effects_and_allows_distinct_new_request(self):
        project,route,path,params,state,bridge,po,client,attempts = self.setup_attempts()
        with contextlib.closing(Coordinator(path,route['session'])) as coord:
            before_jobs = [tuple(r) for r in coord.db.execute('SELECT * FROM session_resets WHERE id IN (?,?) ORDER BY id',tuple(x[0] for x in attempts))]
            before_handoffs = [tuple(r) for r in coord.db.execute('SELECT * FROM root_native_handoffs WHERE job_id IN (?,?) ORDER BY job_id',tuple(x[0] for x in attempts))]
            before_epochs = [tuple(r) for r in coord.db.execute('SELECT * FROM execution_epoch_handoffs WHERE id IN (?,?) ORDER BY id',tuple('epoch-'+x[0] for x in attempts))]
        for req,target,_ in attempts:
            result = self.retire(project,params,po,req)
            self.assertEqual(result,self.retire(project,params,po,req))
            self.assertEqual(result['native_outcome'],'unknown')
            self.assertFalse(result['native_closure_proven'])
            with self.assertRaisesRegex(RemoteCoordError,'quarantined'):
                client.call('/api/coord',{'op':'context','session':target,'host_id':'mac','params':{}})
            with self.assertRaisesRegex(RemoteCoordError,'retired'):
                client.call('/api/coord',{'op':'send','session':po['target_session'],'host_id':'mac',
                    'params':{'to':target,'body':'must not be scheduled'}})
            with self.assertRaisesRegex(RemoteCoordError,'retired'):
                client.call('/api/coord',{'op':'session-reset-resolve','session':po['target_session'],'host_id':'mac',
                    'params':{'id':req,'thread':str(uuid.uuid4()),'confirm_created':True}})
            with self.assertRaisesRegex(RemoteCoordError,'quarantined'):
                state.call('execution-epoch-begin',method='thread/start',thread=po['new_thread_id'],actor=target,job_id=req)
        with contextlib.closing(Coordinator(path,route['session'])) as coord:
            self.assertEqual(before_jobs,[tuple(r) for r in coord.db.execute('SELECT * FROM session_resets WHERE id IN (?,?) ORDER BY id',tuple(x[0] for x in attempts))])
            self.assertEqual(before_handoffs,[tuple(r) for r in coord.db.execute('SELECT * FROM root_native_handoffs WHERE job_id IN (?,?) ORDER BY job_id',tuple(x[0] for x in attempts))])
            self.assertEqual(before_epochs,[tuple(r) for r in coord.db.execute('SELECT * FROM execution_epoch_handoffs WHERE id IN (?,?) ORDER BY id',tuple('epoch-'+x[0] for x in attempts))])
            for req,target,_ in attempts:
                self.assertTrue(quarantined(coord.db,session=target,job=req))
                self.assertIsNotNone(coord.db.execute('SELECT 1 FROM sessions WHERE id=?',(target,)).fetchone())
                with self.assertRaisesRegex(CoordError,'quarantined'):
                    handoff_begin(coord.db,'reset',SessionResetState(coord).job(req),'mac','creating')
                with self.assertRaisesRegex(CoordError,'retired'):
                    SessionResetState(coord).update(req,'uncertain','uncertain',error='changed')
                with self.assertRaises(sqlite3.IntegrityError): coord.db.execute("UPDATE session_resets SET status='failed' WHERE id=?",(req,))
            self.assertEqual(coord.db.execute('SELECT active FROM root_team_stops WHERE id=?',(params['parked_stop'],)).fetchone()['active'],1)
        fresh = dict(attempts[0][2],agent='worker-fresh',request_id=str(uuid.uuid4()))
        result = client.call('/api/coord',{'op':'parked-agent-create','session':po['target_session'],'host_id':'mac','params':fresh})
        self.assertNotIn(result['target_session'], [x[1] for x in attempts])
        self.assertEqual(result['status'],'pending')
        selected = state.reset_jobs()
        self.assertIn(fresh['request_id'], [job['id'] for job in selected])
        self.assertFalse(set(x[0] for x in attempts) & set(job['id'] for job in selected))
        original = self.rpc.request
        def worker_request(method, values):
            if method == 'model/list':
                return {'data':[{'model':'gpt-6.1-terra','supportedReasoningEfforts':[{'reasoningEffort':'high'}]}]}
            return original(method,values)
        with mock.patch.object(self.rpc,'request',side_effect=worker_request): bridge.tick()
        with contextlib.closing(Coordinator(path,route['session'])) as coord:
            new_job = SessionResetState(coord).job(fresh['request_id'])
            self.assertEqual(new_job['status'],'parked',new_job['error'])
            self.assertEqual(before_handoffs,[tuple(r) for r in coord.db.execute('SELECT * FROM root_native_handoffs WHERE job_id IN (?,?) ORDER BY job_id',tuple(x[0] for x in attempts))])
            self.assertEqual(before_epochs,[tuple(r) for r in coord.db.execute('SELECT * FROM execution_epoch_handoffs WHERE id IN (?,?) ORDER BY id',tuple('epoch-'+x[0] for x in attempts))])
        self.assertEqual(self.rpc.queues[new_job['new_thread_id']],[])
        self.assertEqual(self.rpc.history[new_job['new_thread_id']],[])

    def test_wrong_scope_bound_or_nonuncertain_is_refused(self):
        project,route,path,params,state,bridge,po,client,attempts = self.setup_attempts()
        req,target,_ = attempts[0]
        for changes in ({'stop_id':str(uuid.uuid4())},{'po':attempts[1][1]},
                        {'bootstrap_id':str(uuid.uuid4())},{'request_id':params['request_id']},
                        {'expected_thread':str(uuid.uuid4())},{'expected_host':'other'},{'confirm':False}):
            with self.subTest(changes=changes), self.assertRaises(CoordError): self.retire(project,params,po,req,**changes)
        for column, value in (('status','pending'),('new_thread_id',str(uuid.uuid4())),('queue_id','native-item')):
            with contextlib.closing(Coordinator(path,route['session'])) as coord:
                coord.db.execute('UPDATE session_resets SET '+column+'=? WHERE id=?', (value,req))
            with self.subTest(column=column), self.assertRaises(CoordError): self.retire(project,params,po,req)
            with contextlib.closing(Coordinator(path,route['session'])) as coord:
                coord.db.execute('UPDATE session_resets SET '+column+'=? WHERE id=?', ('uncertain' if column=='status' else None,req))
        with contextlib.closing(Coordinator(path,route['session'])) as coord:
            coord.db.execute('INSERT INTO bridge_bindings VALUES(?,?,NULL,NULL,?)',(target,str(uuid.uuid4()),time.time()))
        with self.assertRaises(CoordError): self.retire(project,params,po,req)
        with contextlib.closing(Coordinator(path,route['session'])) as coord:
            self.assertFalse(coord.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_parked_retirements'").fetchone())
