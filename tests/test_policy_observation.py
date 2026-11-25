"""Service-free policy maintenance latency/proof regressions; no real waits."""
import copy
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from agent_chat import communication
from agent_chat.bridge import Bridge
from agent_chat.policy_store import PolicyStore
from agent_chat.core import CoordError
from agent_chat.rpc import RpcError
from agent_chat.root_restart import RootRestartWorker


def full_policy_payload():
    metadata=subprocess.run(['jq','-nc','--arg','version',communication.policy_version(),
                             '{policy_version:$version}'],text=True,capture_output=True,check=True).stdout.strip()
    return communication.STANDING_POLICY+' '+communication.BOOT_AUTHORIZATION+'\n'+metadata


class PolicyState:
    def __init__(self,path,project='project'):
        self.project=project
        self.store=PolicyStore(path);self.messages=[];self.delivered=set();self.work={};self.observations=[]

    def policy_pending(self): return self.store.pending('host',project=self.project)
    def policy_receipt(self,offer,turn): return self.store.receipt('host',offer['thread'],offer['client_id'],offer['version'],turn,project=self.project)
    def policy_consumed(self,thread): return self.store.consumed('host',thread,project=self.project)
    def policy_offer(self,kind,client,thread,payload): return self.store.offer('host',self.project,kind,client,thread,payload)
    def jobs(self): return [dict(job) for job in self.work.values() if job['status'] not in ('dispatched','cancelled')]
    def pending(self): return [m for m in self.messages if m['id'] not in self.delivered]
    def resolve(self,session): return dict(thread_id=session,route=[dict(session_id=session,agent_path=None)])
    def connection_metadata(self): return dict(project='project')
    def wake_messages(self,thread,ids): return []
    def observe(self,thread,state,error=None): self.observations.append((thread,state,error))
    def prepare(self,thread,messages,payload):
        job=dict(id='wake-'+str(len(self.work)),thread_id=thread,payload=payload,status='prepared',queue_id=None)
        self.work[job['id']]=job;self.delivered.update(m['id'] for m in messages)
        return dict(job)
    def job(self,job): return dict(self.work[job])
    def still_unread(self,job): return True
    def update(self,job,status,queue_id=None,error=None):
        self.work[job].update(status=status,error=error)
        if queue_id: self.work[job]['queue_id']=queue_id


class PolicyRpc:
    def __init__(self):
        self.calls=[];self.now=0.;self.history={};self.unavailable=set();self.unsupported=set();self.malformed=set();self.queues={};self.active=set()

    def close(self): pass
    def connect(self): return self
    def request(self,method,params):
        self.calls.append((method,copy.deepcopy(params),self.now))
        thread=params['threadId']
        if method=='thread/turns/list':
            self.now+=10
            if thread in self.unavailable: raise RpcError(-32600,'thread is unloaded')
            if thread in self.unsupported: raise RpcError(-32601,'unsupported')
            turns=self.history.get(thread,[]);offset=int(params.get('cursor',0));end=offset+params['limit']
            return dict(data=copy.deepcopy(turns[offset:end]),nextCursor=str(end) if end<len(turns) else None)
        if method=='thread/read' and params.get('includeTurns'):
            self.now+=10
            if thread in self.malformed: return dict(thread=dict(id='wrong',turns=[]))
            return dict(thread=dict(id=thread,turns=copy.deepcopy(list(reversed(self.history.get(thread,[]))))))
        self.now+=1
        if method=='thread/read': return dict(thread=dict(id=thread,status=dict(type='active' if thread in self.active else 'idle'),canAcceptDirectInput=thread not in self.active))
        if method=='thread/queue/list': return dict(data=copy.deepcopy(self.queues.get(thread,[])),nextCursor=None)
        if method=='thread/queue/add':
            item=dict(id='queue-'+str(len(self.calls)),clientUserMessageId=params['clientUserMessageId'],input=params['input'])
            self.queues.setdefault(thread,[]).append(item)
            return dict(queuedSubmission=item)
        if method=='thread/queue/start':
            item=self.queues[thread].pop(0);self.active.add(thread)
            self.history.setdefault(thread,[]).insert(0,dict(id='native-'+item['id'],status='completed',items=[dict(type='userMessage',clientId=item['clientUserMessageId'])]))
            return dict(turn=dict(id='native-'+item['id']))
        raise AssertionError(method)


class PolicyObservationTests(unittest.TestCase):
    def setUp(self):
        temporary=tempfile.TemporaryDirectory(prefix='policy-observation-');self.addCleanup(temporary.cleanup)
        from agent_chat.core import Coordinator
        self.state=PolicyState(Path(temporary.name)/'policy.sqlite3')
        self.state.coord=Coordinator(Path(temporary.name)/'coordinator.sqlite3');self.addCleanup(self.state.coord.close)
        self.rpc=PolicyRpc();self.bridge=Bridge(self.state,self.rpc)
        self.bridge._policy_clock=lambda:self.rpc.now

    def offer(self,thread,client='offered'):
        return self.state.policy_offer('wake',client,thread,full_policy_payload())

    def test_incomplete_authorization_and_foreign_project_remain_cold(self):
        full=full_policy_payload();version=communication.policy_version()
        auth=communication.BOOT_AUTHORIZATION
        self.assertIn('An assigned goal',auth)
        for client,payload in (
                ('missing',full.replace(auth,'')),
                ('altered',full.replace(auth,auth.replace('An assigned goal','An altered goal',1))),
                ('truncated',full.replace(auth,auth[:-1]))):
            with self.subTest(client=client):
                self.assertFalse(self.state.policy_offer('wake',client,'negative',payload)['offered'])
                with self.assertRaises(CoordError):
                    self.state.store.receipt('host','negative',client,version,'synthetic-turn',project=self.state.project)
        self.assertEqual(self.state.policy_pending(),[])
        self.assertFalse(self.state.policy_consumed('negative'))
        self.assertTrue(self.offer('own','own-client')['offered'])
        with self.assertRaises(CoordError):
            self.state.store.receipt('host','own','own-client',version,'synthetic-turn',project='foreign-project')
        with self.assertRaises(CoordError):
            self.state.store.receipt('host','own','own-client',version,'synthetic-turn')
        self.assertFalse(self.state.policy_consumed('own'))
        self.assertEqual(self.state.store.pending('host',project='foreign-project'),[])
        with self.state.store.connection() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM native_policy_verified_receipts').fetchone()[0],0)

    def turns(self,count):
        return [dict(id='turn-'+str(i),status='completed',items=[]) for i in range(count)]

    def test_ready_wake_precedes_stale_scans_with_global_request_bound(self):
        for index in range(4):
            thread='stale-'+str(index);self.offer(thread);self.rpc.history[thread]=self.turns(150)
        with mock.patch.object(communication,'STANDING_POLICY','An obsolete policy.'):
            self.offer('obsolete')
        self.state.messages.append(dict(id='ready-message',recipient_session='ready'))
        self.bridge.tick()
        add=next(i for i,(method,_,_) in enumerate(self.rpc.calls) if method=='thread/queue/add')
        before=sum(method=='thread/turns/list' or (method=='thread/read' and params.get('includeTurns')) for method,params,_ in self.rpc.calls[:add])
        history=sum(method=='thread/turns/list' or (method=='thread/read' and params.get('includeTurns')) for method,params,_ in self.rpc.calls)
        self.assertEqual(before,0,dict(history_calls_before_add=before,virtual_add_time=self.rpc.calls[add][2]))
        self.assertLessEqual(history,2)
        self.assertEqual(self.state.job('wake-0')['status'],'dispatched')

    def test_obsolete_offers_stay_immutable_and_cold_but_leave_active_scan(self):
        with mock.patch.object(communication,'STANDING_POLICY','An obsolete policy.'):
            self.offer('old')
        self.assertEqual(self.state.policy_pending(),[])
        with self.state.store.connection() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM native_policy_offers').fetchone()[0],1)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM native_policy_receipts').fetchone()[0],0)
        self.assertFalse(self.state.policy_consumed('old'))

    def test_unavailable_thread_does_not_starve_exact_current_offer_and_backs_off(self):
        self.offer('a-unavailable');self.rpc.unavailable.add('a-unavailable')
        self.offer('b-exact','genuine')
        self.rpc.history['b-exact']=[dict(id='exact-native-turn',status='completed',items=[dict(type='userMessage',clientId='genuine')])]
        self.bridge.observe_policies();self.bridge.observe_policies()
        self.assertTrue(self.state.policy_consumed('b-exact'))
        self.assertFalse(self.state.policy_consumed('a-unavailable'))
        calls=len(self.rpc.calls)
        for _ in range(4): self.bridge.observe_policies()
        self.assertEqual(len(self.rpc.calls),calls)
        with self.state.store.connection() as db:
            receipt=db.execute("SELECT client_id,turn_id FROM native_policy_receipts WHERE thread='b-exact'").fetchone()
            self.assertEqual(tuple(receipt),('genuine','exact-native-turn'))

    def test_pagination_advances_beyond_newest_hundred_without_restarting_for_new_offers(self):
        self.offer('deep','deep-client');self.rpc.history['deep']=self.turns(151)
        self.rpc.history['deep'][137]['items']=[dict(type='userMessage',clientId='deep-client')]
        cursors=[]
        for index in range(6):
            before=len(self.rpc.calls);self.bridge.observe_policies()
            calls=self.rpc.calls[before:];self.assertLessEqual(len(calls),2)
            cursors.append(calls[0][1].get('cursor'))
            if index==1: self.offer('deep','newer-unmatched-client')
        self.assertEqual(cursors,[None,'25','50','75','100','125'])
        self.assertTrue(self.state.policy_consumed('deep'))

    def test_legacy_fallback_pages_reuse_validated_compact_snapshot(self):
        self.offer('legacy','deep-client');self.rpc.unsupported.add('legacy');self.rpc.history['legacy']=self.turns(151)
        self.rpc.history['legacy'][137]['items']=[dict(type='userMessage',clientId='deep-client')]
        for _ in range(6):
            before=len(self.rpc.calls);self.bridge.observe_policies()
            self.assertLessEqual(len(self.rpc.calls)-before,2)
        self.assertTrue(self.state.policy_consumed('legacy'))
        self.assertEqual([method for method,_,_ in self.rpc.calls],['thread/turns/list','thread/read'])

    def test_missing_exact_turn_or_client_and_malformed_fallback_stay_cold(self):
        self.offer('a-proof','offered')
        self.rpc.history['a-proof']=[dict(id='',status='completed',items=[dict(type='userMessage',clientId='offered')]),
            dict(id='assistant-only',status='completed',items=[dict(type='agentMessage',clientId='offered')]),
            dict(id='wrong-client',status='completed',items=[dict(type='userMessage',clientId='other')])]
        self.offer('b-malformed');self.rpc.unsupported.add('b-malformed');self.rpc.malformed.add('b-malformed')
        self.bridge.observe_policies();self.bridge.observe_policies()
        self.assertFalse(self.state.policy_consumed('a-proof'));self.assertFalse(self.state.policy_consumed('b-malformed'))
        with self.state.store.connection() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM native_policy_receipts').fetchone()[0],0)
        calls=len(self.rpc.calls);self.bridge.observe_policies();self.assertEqual(len(self.rpc.calls),calls)

    def test_end_without_match_waits_then_restarts_and_new_input_can_prompt_retry(self):
        self.offer('ended');self.bridge.observe_policies();calls=len(self.rpc.calls)
        self.bridge.observe_policies();self.assertEqual(len(self.rpc.calls),calls)
        self.rpc.now+=31;self.bridge.observe_policies();self.assertEqual(len(self.rpc.calls),calls+1)
        self.offer('ended','new-client')
        self.rpc.history['ended']=[dict(id='new-proof',status='completed',items=[dict(type='userMessage',clientId='new-client')])]
        self.bridge.observe_policies();self.assertTrue(self.state.policy_consumed('ended'))

    def test_policy_version_change_starts_current_cohort_without_obsolete_cursor(self):
        self.offer('versioned','old-client');self.rpc.history['versioned']=self.turns(150)
        self.bridge.observe_policies()
        with mock.patch.object(communication,'STANDING_POLICY',communication.STANDING_POLICY+' Revision.'):
            self.offer('versioned','new-client')
            self.rpc.history['versioned'][0]['items']=[dict(type='userMessage',clientId='new-client')]
            self.bridge.observe_policies()
            self.assertNotIn('cursor',self.rpc.calls[-1][1])
            self.assertTrue(self.state.policy_consumed('versioned'))

    def test_multiline_root_boot_records_exact_offer_and_requires_native_proof(self):
        job=dict(id='fresh-request',home_project='project',home_session='root',host_id='host',
            new_thread_id='new-root',instruction_origin='authorized_root_lifecycle',prompt='Exact authorized boot prompt.')
        payload=RootRestartWorker(self.bridge).bootstrap(job)
        self.assertTrue(self.state.policy_offer('root',job['id'],'new-root',payload)['offered'])
        self.assertTrue(payload.endswith(job['prompt']))
        self.assertFalse(self.state.policy_consumed('new-root'))
        self.rpc.history['new-root']=[dict(id='exact-boot-turn',status='completed',items=[dict(type='userMessage',clientId=job['id'])])]
        self.bridge.observe_policies()
        self.assertTrue(self.state.policy_consumed('new-root'))
        with self.state.store.connection() as db:
            receipt=db.execute('SELECT client_id,turn_id FROM native_policy_receipts').fetchone()
            self.assertEqual(tuple(receipt),(job['id'],'exact-boot-turn'))
        # The multiline exception is restricted to genuine root-kind offers.
        self.assertFalse(self.state.policy_offer('wake','wrong-kind','other-thread',payload)['offered'])
        self.assertFalse(self.state.policy_offer('root','malformed','other-thread',None)['offered'])

