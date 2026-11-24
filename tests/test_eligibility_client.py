"""Local client contract; no server, native process, or model input."""
import unittest
from unittest import mock
from agent_chat.bridge import Bridge, _eligibility_observation
from agent_chat.bridge_client import RemoteBridgeState
from agent_chat.rpc import TransportError

class State:
    def __init__(self): self.prepares=[]
    def jobs(self): return []
    def pending(self): return [{'id':'message','recipient_session':'recipient'}]
    def resolve(self, _): return {'thread_id':'thread'}
    def observe(self, *args): pass
    def prepare(self, thread, messages, payload, **kwargs):
        self.prepares.append(kwargs); return {'id':'job','thread_id':thread}
class Rpc:
    def __init__(self, identities, status='idle', queue=None):
        self.identities=iter(identities); self.status=status; self.entries=queue or []; self.calls=[]
    def connection_identity(self):
        value=next(self.identities)
        if isinstance(value,Exception): raise value
        return value
    def request(self, method, params):
        self.calls.append(method)
        if method=='thread/read': return {'thread':{'status':{'type':self.status},'canAcceptDirectInput':True}}
        if method=='thread/queue/list': return {'data':self.entries}
        raise AssertionError(method)
class Windows:
    def poll(self): pass
class ClientObservationTests(unittest.TestCase):
    def run_tick(self, identities, status='idle', queue=None):
        bridge=Bridge.__new__(Bridge); bridge.state=State(); bridge.rpc=Rpc(identities,status,queue)
        bridge.native_stage_windows=Windows(); bridge._history_pages={}; bridge._in_tick=False
        bridge.payload=lambda thread,messages:'payload'; bridge.advance=lambda *a,**kw:None
        bridge.usage_error_queue_ready=lambda thread:False
        with mock.patch('agent_chat.bridge.time.monotonic_ns',side_effect=[100,200]):
            bridge.tick(observe_policies=False)
        return bridge
    def test_stable_identity_reports_bracket_without_extra_rpc(self):
        identity={'client_instance':'instance','connection_epoch':1}
        b=self.run_tick([identity,identity])
        self.assertEqual(b.state.prepares,[{'eligibility_observation':{'thread_id':'thread','connection_instance':'instance','connection_epoch':1,'start_monotonic_ns':100,'end_monotonic_ns':200}}])
        self.assertEqual(b.rpc.calls,['thread/read','thread/queue/list'])
    def test_missing_or_changed_identity_keeps_legacy_prepare(self):
        identity={'client_instance':'instance','connection_epoch':1}
        for identities in ([TransportError('unavailable'), identity], [identity,dict(identity,connection_epoch=2)],
                           [dict(identity, connection_epoch=2**63), identity],
                           [dict(identity, client_instance='x'*257), identity],
                           [dict(identity, connection_epoch=True), identity]):
            with self.subTest(identities=identities): self.assertEqual(self.run_tick(identities).state.prepares,[{}])
    def test_nonidle_or_nonempty_queue_does_not_prepare(self):
        identity={'client_instance':'instance','connection_epoch':1}
        for status,queue in [('active',None),('idle',[{'id':'busy'}])]:
            with self.subTest(status=status,queue=queue): self.assertEqual(self.run_tick([identity],status,queue).state.prepares,[])
    def test_remote_prepare_forwards_only_when_present(self):
        state=RemoteBridgeState.__new__(RemoteBridgeState); calls=[]
        state.call=lambda op,**kw:calls.append((op,kw))
        state.prepare('t',[],'p'); state.prepare('t',[],'p',eligibility_observation={'thread_id':'t'})
        self.assertNotIn('eligibility_observation',calls[0][1])
        self.assertEqual(calls[1][1]['eligibility_observation'],{'thread_id':'t'})

    def test_invalid_clock_bounds_are_omitted(self):
        identity=('instance',1)
        for start,end in ((0,1),(2,1),(1,2**63),(True,2)):
            with self.subTest(start=start,end=end):
                self.assertIsNone(_eligibility_observation('thread',identity,identity,start,end))

    def test_unrelated_identity_error_is_not_swallowed(self):
        with self.assertRaises(RuntimeError):
            self.run_tick([RuntimeError('unexpected defect')])
