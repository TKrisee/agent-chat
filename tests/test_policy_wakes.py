"""Actual authenticated wake delivery: policy omission requires native evidence."""
import copy
import json
import uuid
import sqlite3
from agent_chat.policy_store import PolicyStore
from agent_chat.bridge import Bridge
import test_root_restart as root_tests
import test_session_reset as reset_tests
from unittest import mock

from agent_chat import communication
from agent_chat.remote import RemoteCoordError
import test_bridge_remote as remote_tests


class PolicyWakeTests(remote_tests.RemoteBridgeTests):
    def setUp(self):
        super().setUp()
        self.prove=False
        original=self.rpc.request
        def request(method,params):
            result=original(method,params)
            if method=='thread/turns/list' and self.prove:
                for turn in result['data']: turn['id']='native-policy-turn'
            return result
        self.rpc.request=request

    def emitted(self):
        return [params['input'][0]['text'] for method,params in self.rpc.calls if method=='thread/queue/add']

    def test_cold_ack_is_not_proof_then_warm_wake_is_smaller(self):
        first=self.send_operator();self.bridge.tick()
        cold=self.emitted()[-1]
        self.assertIn(communication.STANDING_POLICY,cold)
        self.assertFalse(self.state.policy_consumed(self.tid))
        self.rpc.status='idle'
        second=self.send_operator();self.bridge.tick()
        self.assertIn(communication.STANDING_POLICY,self.emitted()[-1])
        self.assertFalse(self.state.policy_consumed(self.tid))
        self.prove=True
        self.rpc.status='idle'
        third=self.send_operator();self.bridge.tick()
        warm=self.emitted()[-1]
        self.assertTrue(self.state.policy_consumed(self.tid))
        self.assertNotIn(communication.STANDING_POLICY,warm)
        self.assertNotIn('Preserve scope, owner/request IDs',warm)
        self.assertNotIn('no redundant confirmation',warm)
        self.assertIn('agent-chat-client policy',warm)
        self.assertLess(len(warm.encode()),len(cold.encode()))
        print(f'POLICY_METRICS cold_bytes={len(cold.encode())} warm_bytes={len(warm.encode())} cold_instruction_words={len(cold.split(chr(10))[0].split())} warm_instruction_words={len(warm.split(chr(10))[0].split())}')
        metadata=json.loads(warm.split('\n')[1])
        self.assertFalse(metadata['policy_included'])
        record=next(record for record in metadata['messages'] if record['id']==third)
        self.assertEqual(record['sender_role'],'human_operator')
        self.assertEqual(record['recipient_role'],'agent')
        self.assertEqual(record['instruction_origin'],'agent_message')
        self.assertEqual([delivery['message_id'] for delivery in metadata['deliveries']],[third])
        self.assertEqual(len(self.emitted()),3)
        self.assertEqual(self.call('policy')['version'],metadata['policy_version'])

    def test_new_hash_and_new_native_conversation_are_cold(self):
        self.send_operator();self.bridge.tick();self.prove=True;self.bridge.observe_policies()
        self.assertTrue(self.state.policy_consumed(self.tid))
        with mock.patch.object(communication,'STANDING_POLICY',communication.STANDING_POLICY+' Revised policy.'):
            self.assertFalse(self.state.policy_consumed(self.tid))
            self.assertIn('Revised policy.',self.bridge.payload(self.tid,[dict(id=self.send_operator(),recipient_session='a')]))
        self.call('register',session='new-agent',agent='new-agent')
        thread=str(uuid.uuid4());self.call('bind',session='new-agent',thread=thread)
        self.assertFalse(self.state.policy_consumed(thread))
        with self.assertRaisesRegex(RemoteCoordError,'not remotely bound'):
            self.state.policy_consumed(str(uuid.uuid4()))

    def test_warm_parent_keeps_full_unknown_child_handoff(self):
        self.send_operator();self.bridge.tick();self.prove=True;self.bridge.observe_policies()
        self.assertTrue(self.state.policy_consumed(self.tid))
        self.call('register',session='child',agent='alpha/child')
        self.call('bind',session='child',parent_session='a',agent_path='/root/child')
        message=self.send_operator(to='child');self.rpc.status='idle';self.bridge.tick()
        payload=self.emitted()[-1]
        self.assertIn(communication.STANDING_POLICY,payload)
        self.assertIn('Pass its message IDs, routing metadata and these instructions.',payload)
        metadata=json.loads(payload.split('\n')[1])
        self.assertTrue(metadata['policy_included'])
        self.assertEqual(metadata['deliveries'][0]['message_id'],message)
        self.assertEqual(metadata['deliveries'][0]['route'][-1]['agent_path'],'/root/child')

    def test_wrong_version_and_wrong_thread_proof_stay_cold(self):
        self.send_operator();self.bridge.tick();self.prove=True;self.bridge.observe_policies()
        self.assertTrue(self.state.policy_consumed(self.tid))
        self.assertFalse(self.state.call('policy-consumed',thread_id=self.tid,version='wrong')['consumed'])
        self.call('register',session='other',agent='other')
        thread=str(uuid.uuid4());self.call('bind',session='other',thread=thread)
        with self.server.projects.connection() as db:
            source=dict(db.execute('SELECT * FROM native_policy_offers WHERE thread=?',(self.tid,)).fetchone())
        source['thread']=thread
        with self.assertRaisesRegex(RemoteCoordError,'exact immutable'):
            self.state.policy_receipt(source,'some-turn')
        self.assertFalse(self.state.policy_consumed(thread))

    def test_failed_offer_and_unavailable_history_remain_cold_without_replay(self):
        with mock.patch.object(PolicyStore,'offer',side_effect=sqlite3.OperationalError('cache unavailable')):
            first=self.send_operator();self.bridge.tick()
        self.assertFalse(self.state.policy_consumed(self.tid))
        self.rpc.status='idle';second=self.send_operator();self.bridge.tick()
        original=self.rpc.request
        ids=[first,second]
        for bad in ('not a turns list',['not a turn'],[dict(id='turn',items=['not an item'])]):
            def malformed(method,params):
                if method=='thread/turns/list': return {'data':bad,'nextCursor':None}
                return original(method,params)
            self.rpc.request=malformed;self.rpc.status='idle'
            ids.append(self.send_operator());self.bridge.tick()
            self.assertFalse(self.state.policy_consumed(self.tid))
        self.assertEqual(len(self.emitted()),len(ids))
        for payload,mid in zip(self.emitted(),ids):
            self.assertIn(communication.STANDING_POLICY,payload)
            self.assertEqual(json.loads(payload.split('\n')[1])['deliveries'][0]['message_id'],mid)

    def test_legacy_style_only_bootstrap_cannot_claim_full_policy(self):
        mid=self.send_operator()
        legacy=communication.COORDINATION_STYLE+'\n'+json.dumps(dict(policy_version=communication.policy_version()))
        job=self.state.prepare(self.tid,[dict(id=mid,recipient_session='a')],legacy)
        self.bridge.tick();self.prove=True;self.bridge.observe_policies()
        self.assertFalse(self.state.policy_consumed(self.tid))
        self.rpc.status='idle';self.send_operator();self.bridge.tick()
        self.assertIn(communication.STANDING_POLICY,self.emitted()[-1])
        self.assertEqual(len(self.emitted()),2)


for name in vars(remote_tests.RemoteBridgeTests):
    if name.startswith('test_') and name not in vars(PolicyWakeTests): setattr(PolicyWakeTests,name,None)


class RootPolicyBootstrapTests(root_tests.RootFreshStartTests):
    def test_root_bootstrap_proof_warms_aliases_but_ack_does_not(self):
        project=self.project()
        rid=str(uuid.uuid4());self.fresh(rid);self.bridge.tick()
        job=self.status(rid)
        self.assertEqual(job['status'],'starting')
        self.assertFalse(self.state.policy_consumed(job['new_thread_id']))
        self.publish_consumption=True;self.bridge.tick()
        self.assertEqual(self.status(rid)['status'],'completed')
        self.assertTrue(self.state.policy_consumed(job['new_thread_id']))
        alias,bridge=self.project_bridge(project)
        self.assertTrue(alias.policy_consumed(job['new_thread_id']))
        with self.server.projects.connection() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM native_policy_receipts WHERE thread=?',(job['new_thread_id'],)).fetchone()[0],1)


class ResetPolicyBootstrapTests(reset_tests.SessionResetTests):
    def test_reset_ack_stays_cold_and_positive_boot_turn_warms(self):
        self.request_reset();self.bridge.tick()
        self.assertEqual(self.reset()['status'],'completed')  # Existing lifecycle contract remains unchanged.
        self.assertFalse(self.state.policy_consumed(self.rpc.new))
        original=self.rpc.request
        def proof(method,params):
            result=original(method,params)
            if method=='thread/turns/list' and params['threadId']==self.rpc.new:
                for turn in result['data']: turn['id']='native-reset-bootstrap-turn'
            return result
        self.rpc.request=proof;self.bridge.observe_policies()
        self.assertTrue(self.state.policy_consumed(self.rpc.new))
        self.assertEqual(len([item for item in self.rpc.calls if item[0]=='thread/start']),1)
        self.assertEqual(len([item for item in self.rpc.calls if item[0]=='thread/queue/add']),1)


for cls in (RootPolicyBootstrapTests,ResetPolicyBootstrapTests):
    for name in dir(cls):
        if name.startswith('test_') and name not in vars(cls): setattr(cls,name,None)

del cls
