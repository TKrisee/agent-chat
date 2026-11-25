import subprocess
import tempfile
from pathlib import Path
from types import SimpleNamespace
import unittest
from agent_chat import communication
from agent_chat.bridge import Bridge
from agent_chat.policy_store import PolicyStore
from agent_chat.reset_worker import ResetWorker
from agent_chat.root_restart import RootRestartWorker

class StandingAuthorizationTests(unittest.TestCase):
    EXPECTED_POLICY_VERSION = '5eb8f846b832b90cc12bd9012e4fb2ef18e1e44f21bdf6c4c5719f0ad97a86a8'

    def check_authority(self, text):
        self.assertIn('An assigned goal authorizes the routine actions reasonably necessary to complete it', text)
        self.assertIn('The orchestrator is authorized to perform project, PO, and agent administration', text)
        self.assertIn('Standing authorization does not bypass tool permissions, sandbox restrictions, approval reviews', text)

    def test_boots_preserve_task_and_policy_offer_format(self):
        self.assertEqual(communication.policy_version(), self.EXPECTED_POLICY_VERSION)
        state = SimpleNamespace(connection_metadata=lambda: dict(project='project'))
        bridge = SimpleNamespace(state=state,rpc=None)
        task = 'Exact retained task prompt.'
        jobs = [('reset',ResetWorker(bridge),dict(kind='create',target_session='po',target_agent='po',new_thread_id='new',prompt=task)),
                ('root',RootRestartWorker(bridge),dict(id='request',home_project='project',home_session='root',host_id='host',new_thread_id='new',instruction_origin='authenticated_operator_ui',prompt=task))]
        with tempfile.TemporaryDirectory() as directory:
            store = PolicyStore(str(Path(directory)/'policy.sqlite3'))
            for kind,worker,job in jobs:
                with self.subTest(kind=kind):
                    original = dict(job)
                    payload = worker.bootstrap(job)
                    self.check_authority(payload)
                    self.assertTrue(payload.endswith(task))
                    self.assertEqual(job,original)
                    self.assertTrue(store.offer('host','project',kind,kind+'-input','new',payload)['offered'])

    def test_cold_wake_policy_and_metadata_budget_with_large_message(self):
        self.assertEqual(communication.policy_version(), self.EXPECTED_POLICY_VERSION)
        state = SimpleNamespace(resolve=lambda session: dict(route=[dict(session_id=session,agent_path=None)]),
            policy_consumed=lambda thread: False, connection_metadata=lambda: dict(project='project'),
            wake_messages=lambda thread,ids: [dict(id='message',body='retained body '*1000,complete=True)])
        bridge = Bridge.__new__(Bridge)
        bridge.state = state
        payload = bridge.payload('thread',[dict(id='message',recipient_session='po')])
        self.check_authority(payload)
        self.assertLessEqual(len(payload.encode()),12288)
        result = subprocess.run(['jq','-Rse','--arg','version',communication.policy_version(),
            r'split("\n")[1]|fromjson|.policy_version==$version and .deliveries[0].recipient_session=="po" and .messages[0].id=="message"'],input=payload,text=True,capture_output=True)
        self.assertEqual(result.returncode,0,result.stderr)

    def test_warm_wake_keeps_compact_policy_behavior(self):
        self.assertEqual(communication.policy_version(), self.EXPECTED_POLICY_VERSION)
        state = SimpleNamespace(resolve=lambda session: dict(route=[dict(session_id=session,agent_path=None)]),
            policy_consumed=lambda thread: True, connection_metadata=lambda: dict(project='project'),
            wake_messages=lambda thread,ids: [dict(id='message',body='retained body',complete=True)])
        bridge = Bridge.__new__(Bridge)
        bridge.state = state
        payload = bridge.payload('thread',[dict(id='message',recipient_session='po')])
        self.assertTrue(payload.startswith(communication.COMPACT_WAKE+'\n'))
        self.assertNotIn('An assigned goal authorizes the routine actions', payload)
        self.assertIn('"policy_included":false', payload)
