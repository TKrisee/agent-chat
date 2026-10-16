"""Actual host dispatch loop with isolated states and deterministic native RPC."""
import contextlib
import io
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from agent_chat import bridge_client
from agent_chat.bridge import Bridge
from agent_chat.core import CoordError
from agent_chat.rpc import TransportError
import test_policy_observation as observation


class HostState(observation.PolicyState):
    def __init__(self,path,project):
        super().__init__(path);self.routes={project};self.owned_clients=None;self.fail_ready=False;self.fail_pending=False;self.events=[]

    def policy_pending(self):
        if self.fail_pending: raise CoordError('metadata unavailable')
        return [offer for offer in super().policy_pending() if offer['thread'] in self.routes
                and (self.owned_clients is None or offer['client_id'] in self.owned_clients)]

    def policy_receipt(self,offer,turn):
        if self.owned_clients is not None and offer['client_id'] not in self.owned_clients:
            raise AssertionError('receipt routed through another project owner')
        self.events.append(('receipt',dict(client=offer['client_id'],turn=turn)))
        return super().policy_receipt(offer,turn)

    def call(self,op,**params):
        self.events.append((op,params))
        if op=='acquire' and self.fail_ready: raise CoordError('ready work unavailable')
        return {}

    def heartbeat(self,server,pid,error=None): self.events.append(('heartbeat',dict(error=error)))


class HostPolicyCycleTests(unittest.TestCase):
    def setUp(self):
        temporary=tempfile.TemporaryDirectory(prefix='policy-host-cycle-');self.addCleanup(temporary.cleanup)
        self.rpc=observation.PolicyRpc();self.states={p:HostState(Path(temporary.name)/'policy.sqlite3',p) for p in ('a','b','c')}
        self.cycle_ends=[];self.output=io.StringIO()

    def offer(self,thread,client='offered'):
        fixture=SimpleNamespace(state=self.states['a'])
        observation.PolicyObservationTests.offer(fixture,thread,client)

    def run_host(self,cycles=1,once=True):
        parent=self
        class Cycles:
            def __init__(self): self.waits=0
            def is_set(self): return self.waits>=cycles
            def wait(self,_):
                parent.cycle_ends.append(len(parent.rpc.calls));self.waits+=1
            def set(self): self.waits=cycles
        def bridge(state,rpc):
            value=Bridge(state,rpc);value._policy_clock=lambda:self.rpc.now
            return value
        with contextlib.ExitStack() as stack:
            replacements={
                'HttpClient':lambda server,token,project='default':SimpleNamespace(token='isolated-test',project=project),
                'RpcClient':lambda _:self.rpc,'host_id':lambda:'host',
                'UsageGuard':lambda *_:SimpleNamespace(check=lambda _:dict(blocked=False)),
                '_project_ids':lambda _:list(self.states),
                'client_identity':lambda *_:contextlib.nullcontext(dict(owner='test',secret='not-used',host_id='host')),
                'RemoteBridgeState':lambda client,*_:self.states[client.project], 'Bridge':bridge,
            }
            for name,value in replacements.items(): stack.enter_context(mock.patch.object(bridge_client,name,value))
            stack.enter_context(mock.patch.dict(os.environ,{'AGENT_CHAT_PROJECT':''}))
            stack.enter_context(mock.patch.object(bridge_client.threading,'Event',Cycles))
            stack.enter_context(contextlib.redirect_stdout(self.output));stack.enter_context(contextlib.redirect_stderr(self.output))
            return bridge_client.main(['--server','https://isolated.example','--connect-only']+(['--once'] if once else []))

    def history_calls(self,calls):
        return [call for call in calls if call[0]=='thread/turns/list' or (call[0]=='thread/read' and call[1].get('includeTurns'))]

    def test_last_project_ready_input_precedes_first_project_stale_maintenance(self):
        for project in self.states:
            self.offer(project);self.rpc.history[project]=observation.PolicyObservationTests.turns(self,150)
        self.states['c'].messages.append(dict(id='last-ready',recipient_session='c'))
        self.assertEqual(self.run_host(),0)
        add=next(i for i,(method,_,_) in enumerate(self.rpc.calls) if method=='thread/queue/add')
        self.assertEqual(self.rpc.calls[add][1]['threadId'],'c')
        self.assertEqual(self.history_calls(self.rpc.calls[:add]),[])
        self.assertLessEqual(len(self.history_calls(self.rpc.calls)),2)
        self.assertEqual(self.states['c'].job('wake-0')['status'],'dispatched')

    def test_project_slots_rotate_and_whole_cycles_keep_request_budget(self):
        for project in self.states:
            self.offer(project);self.rpc.history[project]=observation.PolicyObservationTests.turns(self,150)
        self.assertEqual(self.run_host(cycles=3,once=False),0)
        history=self.history_calls(self.rpc.calls)
        self.assertEqual([params['threadId'] for _,params,_ in history],['a','b','c'])
        start=0
        for end in self.cycle_ends:
            self.assertLessEqual(len(self.history_calls(self.rpc.calls[start:end])),2);start=end

    def test_shared_root_aliases_reuse_cursor_and_legacy_snapshot_across_cycles(self):
        self.states['a'].routes={'root'};self.states['b'].routes={'root'};self.states['c'].routes=set()
        self.offer('root','deep-client');self.rpc.unsupported.add('root')
        self.rpc.history['root']=observation.PolicyObservationTests.turns(self,151)
        self.rpc.history['root'][137]['items']=[dict(type='userMessage',clientId='deep-client')]
        self.assertEqual(self.run_host(cycles=6,once=False),0)
        self.assertEqual([method for method,_,_ in self.history_calls(self.rpc.calls)],['thread/turns/list','thread/read'])
        self.assertTrue(self.states['a'].policy_consumed('root'));self.assertTrue(self.states['b'].policy_consumed('root'))

    def test_failed_ready_and_policy_metadata_do_not_drop_other_project_input(self):
        self.states['a'].fail_ready=True;self.states['b'].fail_pending=True
        self.states['c'].messages.append(dict(id='independent',recipient_session='c'))
        self.assertEqual(self.run_host(),2)
        self.assertEqual(self.states['c'].job('wake-0')['status'],'dispatched')
        self.assertLessEqual(len(self.history_calls(self.rpc.calls)),2)
        self.assertTrue(any(event=='heartbeat' and params.get('error')=='metadata unavailable' for event,params in self.states['b'].events))

    def test_aliases_share_unavailable_thread_backoff_without_repeated_probes(self):
        self.states['a'].routes={'root'};self.states['b'].routes={'root'};self.states['c'].routes=set()
        self.offer('root');self.rpc.unavailable.add('root')
        self.assertEqual(self.run_host(cycles=6,once=False),0)
        self.assertEqual(len(self.history_calls(self.rpc.calls)),1)
        self.assertFalse(self.states['a'].policy_consumed('root'))

    def test_distinct_alias_clients_at_second_page_get_exact_owning_receipt(self):
        self.states['a'].routes={'root'};self.states['a'].owned_clients={'client-a'}
        self.states['b'].routes={'root'};self.states['b'].owned_clients={'client-b'}
        self.states['c'].routes=set()
        self.offer('root','client-a');self.offer('root','client-b')
        self.rpc.history['root']=observation.PolicyObservationTests.turns(self,175)
        self.rpc.history['root'][25]['items']=[dict(type='userMessage',clientId='client-b')]
        self.assertEqual(self.run_host(cycles=2,once=False),0)
        self.assertTrue(self.states['b'].policy_consumed('root'))
        self.assertEqual([params for event,params in self.states['b'].events if event=='receipt'],[dict(client='client-b',turn='turn-25')])
        self.assertFalse(any(event=='receipt' for event,_ in self.states['a'].events))
        start=0
        for end in self.cycle_ends:
            self.assertLessEqual(len(self.history_calls(self.rpc.calls[start:end])),2);start=end

    def test_native_observation_transport_error_does_not_replay_independent_input(self):
        self.offer('a');self.states['c'].messages.append(dict(id='independent',recipient_session='c'))
        request=self.rpc.request
        def fail_optional(method,params):
            result=request(method,params)
            if method=='thread/turns/list' and params['threadId']=='a': raise TransportError('optional history response lost')
            return result
        self.rpc.request=fail_optional
        self.assertEqual(self.run_host(cycles=2,once=False),0)
        self.assertEqual(self.states['c'].job('wake-0')['status'],'dispatched')
        self.assertEqual(sum(method=='thread/queue/add' for method,_,_ in self.rpc.calls),1)
        self.assertFalse(self.states['a'].policy_consumed('a'))
        self.assertTrue(self.states['c'].policy_consumed('c'))
        self.assertIn('optional history response lost',self.output.getvalue())
        start=0
        for end in self.cycle_ends:
            self.assertLessEqual(len(self.history_calls(self.rpc.calls[start:end])),2);start=end
