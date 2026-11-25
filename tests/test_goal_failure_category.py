"""Real operator HTTP route; failures and native reads are synthetic fixtures."""
import contextlib, time, unittest
from types import SimpleNamespace
from unittest import mock
from test_goal_visibility import GoalVisibility

class GoalFailureCategory(GoalVisibility):
 def read(self, auth='basic'):
  project=self.projects['Alpha'][0]
  return self.request('GET','/api/root/goal-status?project='+project,auth=auth)
 def assert_unavailable(self, body, reason):
  self.assertEqual(self.cli_query(body, '.status'), 'unavailable')
  self.assertEqual(self.cli_query(body, '.reason'), reason)
  self.assertEqual(self.cli_query(body, 'keys | sort | join(",")'), 'reason,status')
  self.assertNotIn(b'PRIVATE_SECRET',body)
 def test_sanitized_stages_and_success_shape(self):
  selection=SimpleNamespace(registration={'endpoint':'PRIVATE_SECRET_ENDPOINT'})
  obs=SimpleNamespace(status='active',observed_at=time.time())
  with mock.patch('agent_chat.goal_local_provider.select_designated_goal',return_value=selection) as select, \
       mock.patch('agent_chat.goal_provider_evidence.selected_goal_client') as peer, \
       mock.patch('agent_chat.native_owner_evidence.DarwinEvidence'), \
       mock.patch('agent_chat.goal_local_provider.RpcClient') as rpc, \
       mock.patch('agent_chat.goal_local_provider.read_selected_goal',return_value=obs) as read, \
       mock.patch('agent_chat.goal_local_provider.require_current') as current:
   for stage,setup in (
     ('selection_unavailable',lambda: setattr(select,'side_effect',RuntimeError('PRIVATE_SECRET'))),
     ('peer_verification_unavailable',lambda: setattr(peer,'side_effect',RuntimeError('PRIVATE_SECRET'))),
     ('native_observation_unavailable',lambda: setattr(rpc,'side_effect',RuntimeError('PRIVATE_SECRET'))),
     ('native_observation_unavailable',lambda: setattr(read,'side_effect',RuntimeError('PRIVATE_SECRET'))),
     ('selection_revalidation_unavailable',lambda: setattr(current,'side_effect',RuntimeError('PRIVATE_SECRET'))),
     ('native_observation_unavailable',lambda: setattr(rpc.return_value.__exit__,'side_effect',RuntimeError('PRIVATE_SECRET')))):
    select.side_effect=peer.side_effect=rpc.side_effect=read.side_effect=current.side_effect=None;rpc.return_value.__exit__.side_effect=None;setup()
    status,body,_=self.read();self.assertEqual(status,200);self.assert_unavailable(body,stage)
   select.side_effect=peer.side_effect=rpc.side_effect=read.side_effect=current.side_effect=None
   rpc.return_value.__exit__.side_effect=None
   read.return_value=SimpleNamespace(status='active',observed_at=time.time()-10)
   status,body,_=self.read();self.assertEqual(status,200);self.assert_unavailable(body,'observation_stale')
   rpc.return_value.__exit__.side_effect=RuntimeError('PRIVATE_SECRET')
   status,body,_=self.read();self.assertEqual(status,200);self.assert_unavailable(body,'native_observation_unavailable')
   rpc.return_value.__exit__.side_effect=None
   read.return_value=SimpleNamespace(status='active',observed_at=time.time())
   with mock.patch('agent_chat.goal_local_provider._readonly') as readonly:
    readonly.return_value.close.side_effect=RuntimeError('PRIVATE_SECRET')
    status,body,_=self.read();self.assertEqual(status,200);self.assert_unavailable(body,'diagnostic_unavailable')
   status,body,_=self.read();self.assertEqual(status,200)
   self.assertEqual(self.cli_query(body,'.status'),'active')
   self.assertEqual(self.cli_query(body,'keys | sort | join(",")'),'observed_at,status')
   read.return_value=SimpleNamespace(status='none',observed_at=time.time())
   status,body,_=self.read();self.assertEqual(status,200);self.assertEqual(self.cli_query(body,'.status'),'none')
 def test_authentication_and_root_unavailable(self):
  with mock.patch('agent_chat.goal_local_provider.read_project_goal') as provider:
   status,body,_=self.read(auth='none');self.assertIn(status,(401,403));provider.assert_not_called()
   status,body,_=self.read(auth='bearer');self.assertEqual(status,403);provider.assert_not_called()
  with mock.patch.object(self.server.roots,'config',return_value=None):
   status,body,_=self.read();self.assertEqual(status,200);self.assert_unavailable(body,'root_unavailable')

def load_tests(loader,tests,pattern):
 return unittest.TestSuite(GoalFailureCategory(n) for n in ('test_sanitized_stages_and_success_shape','test_authentication_and_root_unavailable'))
