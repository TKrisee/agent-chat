import sqlite3,time,unittest
from contextlib import closing
from unittest.mock import Mock,patch
import test_provider_owning_controller as fixtures
from agent_chat import bridge_state,provider_service as s,provider_recovery_lifecycle as l
class Readiness(unittest.TestCase):
 def setUp(self):
  self.f=fixtures.Controller('test_actual_initial_controller_once_with_owned_transition');self.f.setUp();self.addCleanup(self.f.doCleanups)
  with closing(sqlite3.connect(self.f.dbpath)) as db,db:
   db.execute('DROP TABLE bridge_client_runtime')
   db.execute('CREATE TABLE bridge_client_runtime(host_id TEXT PRIMARY KEY,server,pid,heartbeat,error,project_scope_worker INTEGER NOT NULL DEFAULT 0,project_workspaces_worker INTEGER NOT NULL DEFAULT 0)')
  self.heartbeat('synthetic transport error')
 def heartbeat(self,error):
  with closing(sqlite3.connect(self.f.dbpath)) as db,db:
   state=object.__new__(bridge_state.BridgeState);state.db=db
   state.heartbeat('ws://127.0.0.1:4500',202,error,host_id='host')
 def test_owned_error_clears_via_real_heartbeat_then_succeeds_once(self):
  calls=[]
  def ready(seconds):calls.append(seconds);self.heartbeat(None)
  with patch.object(s.time,'sleep',side_effect=ready):result=self.f.run_controller()
  self.assertEqual(result.state,'restored_known_local_state_prior_effects_unknown',result.reason)
  self.assertEqual(len(calls),1);self.f.adapter.stop_once.assert_called_once();self.f.adapter.start_once.assert_called_once()
  self.assertEqual([x.args[0] for x in self.f.adapter.read.call_args_list],['thread/resume','thread/read'])
 def test_error_persists_until_deadline_without_replay(self):
  clock=[100.0]
  def expire(seconds):clock[0]=106.0
  with patch.object(s.time,'monotonic',side_effect=lambda:clock[0]),patch.object(s.time,'sleep',side_effect=expire),patch.object(l,'time',s.time):result=self.f.run_controller()
  self.assertEqual(result.state,'resume_intent');self.assertIn('deadline',result.reason);self.assertEqual(self.f.adapter.deadline,105.0)
  self.f.adapter.stop_once.assert_called_once();self.f.adapter.start_once.assert_called_once()
  self.assertEqual([x.args[0] for x in self.f.adapter.read.call_args_list],['thread/resume','thread/read'])
 def test_error_with_wrong_thread_fails_without_wait(self):
  with closing(sqlite3.connect(self.f.dbpath)) as db,db:db.execute("UPDATE bridge_bindings SET thread_id='foreign'")
  with patch.object(s.time,'sleep') as sleep:result=self.f.run_controller()
  self.assertEqual(result.state,'resume_intent');self.assertIn('thread_mismatch',result.reason);sleep.assert_not_called()
 def test_error_with_wrong_pid_fails_without_wait(self):
  with closing(sqlite3.connect(self.f.dbpath)) as db,db:db.execute('UPDATE bridge_client_runtime SET pid=404')
  with patch.object(s.time,'sleep') as sleep:result=self.f.run_controller()
  self.assertEqual(result.state,'resume_intent');self.assertIn('client_pid_mismatch',result.reason);sleep.assert_not_called()
 def test_error_with_wrong_parent_fails_without_wait(self):
  self.f.adapter.child.parent=(203,501,self.f.app[3],0)
  with patch.object(s.time,'sleep') as sleep:result=self.f.run_controller()
  self.assertEqual(result.state,'resume_intent');self.assertIn('runtime_error',result.reason);sleep.assert_not_called()
 def test_previous_client_still_alive_fails_without_wait(self):
  with patch.object(s.owner,'absent',return_value=False),patch.object(s.time,'sleep') as sleep:result=self.f.run_controller()
  self.assertEqual(result.state,'resume_intent');self.assertIn('owned client transition changed',result.reason);sleep.assert_not_called()
 def test_owned_child_generation_change_fails_without_wait(self):
  from agent_chat import provider_recovery_lifecycle as l
  self.f.adapter.child.poll=Mock(side_effect=l.Blocked('owned child generation differs'))
  with patch.object(s.time,'sleep') as sleep:result=self.f.run_controller()
  self.assertEqual(result.state,'resume_intent');self.assertIn('owned child generation differs',result.reason);sleep.assert_not_called()
 def test_failed_receipt_never_enters_wait(self):
  from agent_chat import provider_reattachment as r,provider_recovery_lifecycle as l
  with patch.object(r,'record_resume',side_effect=l.Blocked('receipt unavailable')),patch.object(s.time,'sleep') as sleep:result=self.f.run_controller()
  self.assertEqual(result.state,'resume_intent');self.assertIn('receipt unavailable',result.reason);sleep.assert_not_called()
  self.assertEqual([x.args[0] for x in self.f.adapter.read.call_args_list],['thread/resume'])
if __name__=='__main__':unittest.main(verbosity=2)
