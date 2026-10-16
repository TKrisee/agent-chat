"""Root bootstrap offers require a policy line and preserve full raw evidence."""
import hashlib
import tempfile
import unittest
from pathlib import Path

from agent_chat import communication
from agent_chat.bridge import Bridge
from agent_chat.root_restart import RootRestartWorker
import test_policy_observation as observation


class RootPolicyBoundaryTests(unittest.TestCase):
    def setUp(self):
        temporary=tempfile.TemporaryDirectory(prefix='root-policy-boundary-')
        self.addCleanup(temporary.cleanup)
        self.state=observation.PolicyState(Path(temporary.name)/'policy.sqlite3')
        self.rpc=observation.PolicyRpc()
        self.bridge=Bridge(self.state,self.rpc)
        self.bridge._policy_clock=lambda:self.rpc.now
        self.job=dict(id='root-boundary-request',home_project='project',home_session='root',host_id='host',
            new_thread_id='root-boundary-thread',instruction_origin='authorized_root_lifecycle',prompt='Retain this exact prompt.')
        self.payload=RootRestartWorker(self.bridge).bootstrap(self.job)

    def test_actual_multiline_boot_preserves_raw_digest_and_requires_exact_native_proof(self):
        self.assertGreater(self.payload.index(communication.STANDING_POLICY),self.payload.index('\n'))
        self.assertTrue(self.state.policy_offer('root',self.job['id'],self.job['new_thread_id'],self.payload)['offered'])
        with self.state.store.connection() as db:
            digest=db.execute('SELECT payload_sha256 FROM native_policy_offers').fetchone()[0]
        self.assertEqual(digest,hashlib.sha256(self.payload.encode()).hexdigest())
        self.assertFalse(self.state.policy_consumed(self.job['new_thread_id']))
        self.rpc.history[self.job['new_thread_id']]=[dict(id='exact-root-turn',status='completed',
            items=[dict(type='userMessage',clientId=self.job['id'])])]
        self.bridge.observe_policies()
        self.assertTrue(self.state.policy_consumed(self.job['new_thread_id']))

    def test_embedded_policy_after_same_line_junk_stays_cold(self):
        first_line=self.payload[self.payload.index(communication.STANDING_POLICY):]
        for malformed in (self.payload.replace('\n'+communication.STANDING_POLICY,'\njunk'+communication.STANDING_POLICY,1),
                          'junk'+first_line):
            with self.subTest(payload_prefix=malformed[:20]):
                self.assertNotEqual(malformed,self.payload)
                self.assertFalse(self.state.policy_offer('root','malformed',self.job['new_thread_id'],malformed)['offered'])
        self.rpc.history[self.job['new_thread_id']]=[dict(id='unoffered-turn',status='completed',
            items=[dict(type='userMessage',clientId='malformed')])]
        self.bridge.observe_policies()
        self.assertEqual(self.state.policy_pending(),[])
        self.assertFalse(self.state.policy_consumed(self.job['new_thread_id']))

    def test_first_line_root_policy_remains_accepted_without_native_receipt(self):
        payload=self.payload[self.payload.index(communication.STANDING_POLICY):]
        self.assertTrue(self.state.policy_offer('root',self.job['id'],self.job['new_thread_id'],payload)['offered'])
        self.assertFalse(self.state.policy_consumed(self.job['new_thread_id']))
