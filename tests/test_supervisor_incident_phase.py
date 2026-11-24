import subprocess
import unittest
from agent_chat.rpc import RpcError
from test_supervisor import SupervisorWorkflowTests
from test_supervisor_incident_identity import IncidentIdentityTests

class IncidentPhaseTests(SupervisorWorkflowTests):
    def inbox_json(self):
        status, raw, _ = self.request('POST','/api/coord',{'op':'inbox','session':'b','host_id':'mac','params':{'all':True}})
        self.assertEqual(status,200,raw)
        return raw

    def check(self, expression):
        result=subprocess.run(['jq','-e',expression],input=self.inbox_json(),capture_output=True)
        if result.returncode:
            bodies=subprocess.run(['jq','-r','.messages[] | select(.sender_session=="system:supervisor") | .body'],input=self.inbox_json(),capture_output=True).stdout.decode()
            self.fail('jq check failed: '+expression+'; notices: '+bodies)

    def test_same_error_text_in_distinct_native_phases_is_not_collapsed(self):
        self.enable()
        original=self.rpc.request
        selected=['thread/read']
        def request(method,params):
            if method==selected[0] and params.get('threadId')==self.target_thread:
                raise RpcError(-32001,'same fixture error')
            return original(method,params)
        self.rpc.request=request
        self.sample(); self.sample(61)
        self.check('[.messages[] | select(.sender_session=="system:supervisor") ] | length==1')
        body=subprocess.run(['jq','-er','first(.messages[] | select(.sender_session=="system:supervisor") | .body)'],
                            input=self.inbox_json(),capture_output=True,check=True).stdout.decode()
        self.assertIn('phase=thread/read:before',body)
        self.assertIn('owner=b',body)
        self.assertIn('do not replay an uncertain input',body)
        self.assertIn('not proof of useful execution',body)
        self.sample(61)
        self.check('[.messages[] | select(.sender_session=="system:supervisor") ] | length==1')
        selected[0]='thread/queue/list'
        self.sample(301); self.sample(301)
        self.check('[.messages[] | select(.sender_session=="system:supervisor") ] | length==2 and any(.[]; .body | contains("phase=thread/queue/list"))')

class LegacyPhaseIdentityTests(IncidentIdentityTests):
    def test_legacy_same_error_adopts_phase_without_duplicate(self):
        self.sample('same error',1); self.sample('same error',1)
        before=self.notices(); self.assertEqual(len(before),1)
        report=dict(state='error',source='native',summary={'error':'same error','phase':'thread/read:before','wake':{}})
        self.state.observe(self.job,report,'host'); self.now+=61
        self.state.observe(self.job,report,'host'); self.assertEqual(self.notices(),before)
        old_report=dict(state='error',source='native',summary={'error':'same error','wake':{}})
        self.now+=61; self.state.observe(self.job,old_report,'host'); self.now+=61
        self.state.observe(self.job,old_report,'host'); self.assertEqual(self.notices(),before)
        report['summary']['phase']='thread/queue/list'; self.now+=61
        self.state.observe(self.job,report,'host'); self.now+=61
        self.state.observe(self.job,report,'host'); self.assertEqual(len(self.notices()),2)

def load_tests(loader,tests,pattern):
    return unittest.TestSuite([IncidentPhaseTests('test_same_error_text_in_distinct_native_phases_is_not_collapsed'),
                               LegacyPhaseIdentityTests('test_legacy_same_error_adopts_phase_without_duplicate')])
