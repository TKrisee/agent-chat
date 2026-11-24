import os, time, unittest
from types import SimpleNamespace
from unittest.mock import patch
from agent_chat import capacity_observer, native_owner_evidence

class ObservationBoundary(unittest.TestCase):
    def observe(self, thread):
        calls=[]
        registration={'endpoint':'ws://127.0.0.1:34971','pid':42,'heartbeat':time.time()}
        bindings={'owner':{'thread':'thread-a','host':'fixture-host'}}
        class Kernel:
            def sample(self, reg, rpc=None): return SimpleNamespace(endpoint=('127.0.0.1',34971),listener_pid=7,chain=('fixture',),image_sha='synthetic',code_hash='synthetic',peer=None)
        class Provider:
            def __enter__(self): return self
            def __exit__(self,*args): return False
            def connection_identity(self): return {'client_instance':'a'*32,'connection_epoch':1,'pid':os.getpid(),'fd':8,'local':('127.0.0.1',31002),'peer':('127.0.0.1',34971)}
            def request(self,method,params):
                calls.append((method,params))
                if method=='thread/read': return {'thread':thread}
                if method=='thread/queue/list': return {'data':[],'nextCursor':None}
                raise AssertionError(method)
        with (patch.object(native_owner_evidence,'DarwinEvidence',Kernel),
              patch.object(capacity_observer,'_selected_client',side_effect=lambda reg,k: ('client','link',k.sample(reg),'plist','files')),
              patch.object(capacity_observer,'_observing_peer',return_value='synthetic-pair')):
            try:
                with capacity_observer._native_observations(bindings,registration,lambda endpoint:Provider()) as (observed,_,_,_): return ('ok',observed,calls)
            except capacity_observer.Blocked as error: return ('blocked',str(error),calls)
    def test_false_and_missing_direct_input_are_distinct_only_in_observer_error(self):
        base={'id':'thread-a','status':{'type':'idle'}}
        false=self.observe(dict(base,canAcceptDirectInput=False))
        missing=self.observe(base)
        self.assertEqual(false[:2],('blocked','current native owner is not positively idle: direct-input false'))
        self.assertEqual(missing[:2],('blocked','current native direct-input status is unknown'))
        self.assertEqual([x[0] for x in false[2]],['thread/read'])
        self.assertEqual([x[0] for x in missing[2]],['thread/read'])
        print('OBSERVED: false direct-input is named in the refusal; missing direct-input explicitly unknown; neither reaches queue read')
    def test_positive_observation_is_only_synthetic_thread_and_queue(self):
        result=self.observe({'id':'thread-a','status':{'type':'idle'},'canAcceptDirectInput':True})
        self.assertEqual(result[0],'ok')
        self.assertEqual(result[1]['owner']['direct_input'],True)
        self.assertEqual([x[0] for x in result[2]],['thread/read','thread/queue/list'])
        print('OBSERVED: synthetic idle/direct input and empty queue; selected process, parent and image were mocked, not proven')

if __name__=='__main__': unittest.main()
