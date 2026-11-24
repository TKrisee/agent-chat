import sqlite3
import unittest
import uuid
from agent_chat.bridge import Bridge
from agent_chat.bridge_client import RemoteBridgeState
from agent_chat.remote import RemoteCoordError
from test_bridge import FakeRpc
from test_source_trace import SourceTraceTests

class DurableStageTests(SourceTraceTests):
    def test_real_http_lease_trace_censor_and_replay(self):
        self.call('register', session='sender', agent='sender')
        self.call('register', session='recipient', agent='recipient')
        thread = str(uuid.uuid4())
        self.call('bind', session='recipient', thread=thread)
        identity={'owner':'worker','secret':'x'*40,'host_id':'mac'}
        state=RemoteBridgeState(self.client,self.url,identity)
        state.call('acquire')
        source=self.call('send',session='sender',to='recipient',body='private fixture')['id']
        Bridge(state,FakeRpc()).tick(observe_policies=False)
        jobs=self.call('bridge-status',session='recipient')['jobs']
        job=jobs[0]['id']
        observation={'status':'observed','attribution':'job_turn_only','coverage':'unverified','job_id':job,'thread_id':thread,
                     'turn_id':'turn','item_id':'item','observed_at':42.0,'observed_monotonic_ns':100,
                     'connection_instance':'instance','connection_epoch':1,'sequence':2}
        self.assertEqual(state.native_stage_record(job,observation),{'recorded':True,'replayed':False})
        self.assertEqual(state.native_stage_record(job,observation),{'recorded':True,'replayed':True})
        trace=self.call('source-trace',session='sender',source_id=source)
        self.assertEqual(trace['job_native_content']['turn_id'],'turn')
        self.assertIsNone(trace['native_first_response_at'])
        self.assertIsNone(trace['recipient_delivery_at'])
        conflict=dict(observation,turn_id='other')
        with self.assertRaises(RemoteCoordError): state.native_stage_record(job,conflict)
        foreign=RemoteBridgeState(self.client,self.url,dict(identity, secret='y'*40))
        with self.assertRaises(RemoteCoordError): foreign.native_stage_record(job,observation)
        foreign_host=RemoteBridgeState(self.client,self.url,dict(identity, host_id='elsewhere'))
        with self.assertRaises(RemoteCoordError): foreign_host.native_stage_record(job,observation)
        with self.assertRaises(RemoteCoordError): state.native_stage_record(job,dict(observation,thread_id='other'))
        self.assertEqual(self.call('source-trace',session='sender',host='wrong',source_id=source)['status'],'unauthorized')
        self.assertEqual(state.native_stage_censor(job,'window_unavailable')['censored'], True)
        self.assertEqual(self.call('source-trace',session='sender',source_id=source)['job_native_content']['status'],'unknown')
        with self.assertRaises(RemoteCoordError): state.native_stage_record(job,observation)

    def test_real_bridge_observed_event_durable_and_stale_lease_unknown(self):
        class ObservedRpc(FakeRpc):
            def __init__(self):
                super().__init__(); self.events=[]; self.timeout=1
            def connection_identity(self): return {'client_instance':'fixture','connection_epoch':1}
            def notification_boundary(self, **kwargs): return len(self.events)
            def notification_snapshot_since(self, cursor): return self.notifications_since(cursor), self.notification_observations_since(cursor)
            def notifications_since(self, cursor): return [(i,e) for i,e in enumerate(self.events,1) if i>cursor]
            def notification_observations_since(self,cursor):
                return [{'sequence':i,'observed_at':100.0+i,'observed_monotonic_ns':1000+i,
                         'connection_instance':'fixture','connection_epoch':1} for i,_ in enumerate(self.events,1) if i>cursor]
            def request(self, method, params):
                if method=='thread/queue/start':
                    entry=next(e for e in self.entries if e['id']==params['queuedSubmissionId'])
                    self.events.extend([
                        {'method':'item/started','params':{'threadId':params['threadId'],'turnId':'turn','item':{'type':'userMessage','clientId':entry['clientUserMessageId']}}},
                        {'method':'item/agentMessage/delta','params':{'threadId':params['threadId'],'turnId':'turn','itemId':'item','delta':'answer'}}])
                return super().request(method,params)
        self.call('register',session='sender',agent='sender')
        self.call('register',session='recipient',agent='recipient')
        thread=str(uuid.uuid4()); self.call('bind',session='recipient',thread=thread)
        identity={'owner':'worker','secret':'x'*40,'host_id':'mac'}
        state=RemoteBridgeState(self.client,self.url,identity); state.call('acquire')
        source=self.call('send',session='sender',to='recipient',body='private batch fixture')['id']
        rpc=ObservedRpc(); Bridge(state,rpc).tick(observe_policies=False)
        stage=self.call('source-trace',session='sender',source_id=source)['job_native_content']
        self.assertEqual((stage['status'],stage['turn_id'],stage['observed_monotonic_ns'],stage['coverage']),('observed','turn',1002,'unverified'))
        job=stage['job_id']
        replay={key:stage[key] for key in ('status','attribution','coverage','job_id','turn_id','item_id','observed_at','observed_monotonic_ns','connection_instance','connection_epoch','sequence')}
        replay['thread_id']=thread
        self.assertEqual(sum(method=='thread/queue/start' for method,_ in rpc.calls),1)
        state.call('release')
        stage=self.call('source-trace',session='sender',source_id=source)['job_native_content']
        self.assertEqual((stage['status'],stage['reason']),('unknown','dispatcher_changed'))
        with self.assertRaises(RemoteCoordError):
            state.native_stage_censor(job,'window_unavailable')
        successor=RemoteBridgeState(self.client,self.url,{'owner':'new-worker','secret':'y'*40,'host_id':'mac'})
        successor.call('acquire')
        with self.assertRaises(RemoteCoordError): successor.native_stage_record(job,replay)
        with self.assertRaises(RemoteCoordError): successor.native_stage_censor(job,'window_unavailable')

    def test_real_bridge_lost_start_keeps_one_input_and_receipt(self):
        from agent_chat.rpc import TransportError
        class LostRpc(FakeRpc):
            def __init__(self):
                super().__init__(); self.drop_start=True; self.events=[]; self.timeout=1
            def connection_identity(self): return {'client_instance':'fixture','connection_epoch':1}
            def notification_boundary(self, **kwargs): return len(self.events)
            def notification_snapshot_since(self, cursor): return self.notifications_since(cursor), self.notification_observations_since(cursor)
            def notifications_since(self, cursor): return [(i,e) for i,e in enumerate(self.events,1) if i>cursor]
            def notification_observations_since(self,cursor):
                return [{'sequence':i,'observed_at':100.0+i,'observed_monotonic_ns':1000+i,'connection_instance':'fixture','connection_epoch':1} for i,_ in enumerate(self.events,1) if i>cursor]
            def request(self,method,params):
                if method=='thread/queue/start':
                    entry=next(e for e in self.entries if e['id']==params['queuedSubmissionId'])
                    self.events.extend([{'method':'item/started','params':{'threadId':params['threadId'],'turnId':'turn','item':{'type':'userMessage','clientId':entry['clientUserMessageId']}}}, {'method':'item/agentMessage/delta','params':{'threadId':params['threadId'],'turnId':'turn','itemId':'item','delta':'answer'}}])
                return super().request(method,params)
        self.call('register',session='sender',agent='sender'); self.call('register',session='recipient',agent='recipient')
        thread=str(uuid.uuid4()); self.call('bind',session='recipient',thread=thread)
        state=RemoteBridgeState(self.client,self.url,{'owner':'worker','secret':'x'*40,'host_id':'mac'}); state.call('acquire')
        source=self.call('send',session='sender',to='recipient',body='private lost response')['id']
        rpc=LostRpc(); bridge=Bridge(state,rpc)
        with self.assertRaises(TransportError): bridge.tick(observe_policies=False)
        self.assertEqual(self.call('source-trace',session='sender',source_id=source)['job_native_content']['status'],'observed')
        bridge.tick(observe_policies=False)
        self.assertEqual(sum(method=='thread/queue/start' for method,_ in rpc.calls),1)
        self.assertEqual(sum(method=='thread/queue/add' for method,_ in rpc.calls),1)
        self.assertEqual(self.call('source-trace',session='sender',source_id=source)['job_status'],'dispatched')
