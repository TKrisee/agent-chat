import sqlite3,time,unittest
from contextlib import closing
from unittest.mock import Mock,patch
import test_provider_owning_controller as fixtures
from agent_chat import bridge_state,provider_service as s,provider_recovery_lifecycle as l
class StoppedRegistration(unittest.TestCase):
 def setUp(self):
  self.f=fixtures.Controller('test_actual_initial_controller_once_with_owned_transition');self.f.setUp();self.addCleanup(self.f.doCleanups)
  with closing(sqlite3.connect(self.f.dbpath)) as db,db:
   db.execute('DROP TABLE bridge_client_runtime')
   db.execute('CREATE TABLE bridge_client_runtime(host_id TEXT PRIMARY KEY,server,pid,heartbeat,error,project_scope_worker INTEGER NOT NULL DEFAULT 0,project_workspaces_worker INTEGER NOT NULL DEFAULT 0)')
  self.heartbeat(0,'bridge stopped')
  self.change('UPDATE bridge_client_runtime SET heartbeat=?',(time.time()-60,))
 def change(self,sql,args=()):
  with closing(sqlite3.connect(self.f.dbpath)) as db,db:db.execute(sql,args)
 def heartbeat(self,pid,error):
  with closing(sqlite3.connect(self.f.dbpath)) as db,db:
   state=object.__new__(bridge_state.BridgeState);state.db=db
   state.heartbeat('ws://127.0.0.1:4500',pid,error,host_id='host')
 def test_stopped_sentinel_then_healthy_writer_once(self):
  sleeps=[]
  def ready(seconds):sleeps.append(seconds);self.heartbeat(202,None)
  with patch.object(s.time,'sleep',side_effect=ready):result=self.f.run_controller()
  self.assertEqual(result.state,'restored_known_local_state_prior_effects_unknown',result.reason)
  self.assertEqual(len(sleeps),1);self.f.adapter.stop_once.assert_called_once();self.f.adapter.start_once.assert_called_once()
  self.assertEqual([x.args[0] for x in self.f.adapter.read.call_args_list],['thread/resume','thread/read'])
 def test_stopped_sentinel_persists_to_original_deadline(self):
  clock=[100.0]
  def expire(seconds):clock[0]=106.0
  with patch.object(s.time,'monotonic',side_effect=lambda:clock[0]),patch.object(s.time,'sleep',side_effect=expire),patch.object(l,'time',s.time):result=self.f.run_controller()
  self.assertEqual(result.state,'resume_intent');self.assertIn('deadline',result.reason);self.assertEqual(self.f.adapter.deadline,105.0)
  self.f.adapter.stop_once.assert_called_once();self.f.adapter.start_once.assert_called_once()
  self.assertEqual([x.args[0] for x in self.f.adapter.read.call_args_list],['thread/resume','thread/read'])
 def reject_without_wait(self):
  with patch.object(s.time,'sleep') as sleep:result=self.f.run_controller()
  self.assertEqual(result.state,'resume_intent');sleep.assert_not_called();return result
 def test_zero_pid_other_error_never_waits(self):
  self.change("UPDATE bridge_client_runtime SET error='unrelated denial'")
  self.assertIn('runtime_error',self.reject_without_wait().reason)
 def test_foreign_pid_sentinel_never_waits(self):
  self.change('UPDATE bridge_client_runtime SET pid=404')
  self.assertIn('client_pid_mismatch',self.reject_without_wait().reason)
 def test_foreign_endpoint_never_waits(self):
  self.change("UPDATE bridge_client_runtime SET server='ws://127.0.0.1:9999'")
  self.assertIn('endpoint_mismatch',self.reject_without_wait().reason)
 def test_foreign_root_never_waits(self):
  self.change("UPDATE bridge_bindings SET thread_id='foreign'")
  self.assertIn('thread_mismatch',self.reject_without_wait().reason)
 def test_wrong_new_parent_never_waits(self):
  self.f.adapter.child.parent=(203,501,self.f.app[3],0)
  self.assertIn('client_pid_mismatch',self.reject_without_wait().reason)
 def test_missing_receipt_never_waits(self):
  from agent_chat import provider_reattachment as r,provider_recovery_lifecycle as l
  with patch.object(r,'record_resume',side_effect=l.Blocked('receipt unavailable')),patch.object(s.time,'sleep') as sleep:result=self.f.run_controller()
  self.assertIn('receipt unavailable',result.reason);self.assertFalse(any(c.args[0]>=0.04 for c in sleep.call_args_list))
  self.assertEqual([x.args[0] for x in self.f.adapter.read.call_args_list],['thread/resume'])
 def test_original_client_present_never_waits(self):
  with patch.object(s.owner,'absent',return_value=False),patch.object(s.time,'sleep') as sleep:result=self.f.run_controller()
  self.assertIn('owned client transition changed',result.reason);self.assertFalse(any(c.args[0]>=0.04 for c in sleep.call_args_list))
 def test_new_child_generation_changed_never_waits(self):
  from agent_chat import provider_recovery_lifecycle as l
  self.f.adapter.child.poll=Mock(side_effect=l.Blocked('owned child generation differs'))
  with patch.object(s.time,'sleep') as sleep:result=self.f.run_controller()
  self.assertIn('owned child generation differs',result.reason);self.assertFalse(any(c.args[0]>=0.04 for c in sleep.call_args_list))
 def test_authority_refusal_remains_failed(self):
  from agent_chat import provider_recovery_lifecycle as l
  self.f.authority.check.side_effect=[None,l.Blocked('authority refused')]
  with patch.object(s.time,'sleep') as sleep:result=self.f.run_controller()
  self.assertIn('authority refused',result.reason);self.assertFalse(any(c.args[0]>=0.04 for c in sleep.call_args_list))
if __name__=='__main__':unittest.main(verbosity=2)
