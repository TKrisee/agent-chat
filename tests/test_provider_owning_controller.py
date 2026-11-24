import os,sqlite3,unittest
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock,patch
import test_provider_owning_integration as fixtures
from agent_chat import provider_service as s,provider_recovery_lifecycle as l

class Controller(fixtures.Integration):
 def setUp(self):
  super().setUp();a=self.adapter;self.authority.unknown=[]
  selected=l.v2.v1.Selection(os.getuid(),999,3,4,'ws://127.0.0.1:4500','/fake/codex','sha')
  self.selected=selected;a.selected=selected
  a.plan_identity=Mock(return_value='plan');a.selection=Mock();a.actors=SimpleNamespace(generation=1)
  a.actor_proof=('synthetic',);a.arm=Mock();a.stop_once=Mock();a.closed=Mock(return_value=True)
  a.start_once=Mock();a.close=Mock()
  directory=Path(self.temp.name)/'provider-recovery-v3';directory.mkdir(mode=0o700)
  path=directory/'requests.sqlite3';j=l.v2.v1.Journal(path)
  plan='plan\n'+repr(self.authority.root)+'\nsource\ndigest'
  j.claim('request',selected,plan);j.save('request','staged','fixture','unknown')
  j.db.execute('CREATE TABLE prestop_proofs(request_id,thread_id,baseline,baseline_sha,source_id,source_sha)')
  j.db.execute('INSERT INTO prestop_proofs VALUES(?,?,?,?,?,?)',('request','thread',a.baseline,'digest','source','digest'))
  j.db.execute('CREATE TABLE checkpoint_bindings(request_id,home_project,root_session,host,thread_id,binding_generation)')
  j.db.execute("INSERT INTO checkpoint_bindings VALUES('request','project','root','host','thread',1)")
  j.db.execute('CREATE TABLE poststop_proofs('+','.join('c'+str(i) for i in range(25))+')')
  j.db.commit();j.close()
  self.stack.enter_context(patch.object(s,'authority_for_handoff',return_value=self.authority))
  self.stack.enter_context(patch.object(s,'HandoffRecovery',return_value=a))
 def run_controller(self):
  return l.recover_uncertain('request','source',self.selected,Path('/fake/manifest'),'manifest',timeout=5,_service_handoff=True)
 def test_actual_initial_controller_once_with_owned_transition(self):
  result=self.run_controller()
  self.assertEqual(result.state,'restored_known_local_state_prior_effects_unknown',result.reason)
  self.adapter.stop_once.assert_called_once();self.adapter.start_once.assert_called_once()
  self.assertEqual([x.args[0] for x in self.adapter.read.call_args_list],['thread/resume','thread/read'])
  with closing(sqlite3.connect(Path(self.temp.name)/'provider-recovery-v3/requests.sqlite3')) as db:
   self.assertEqual(db.execute('select count(*) from authenticated_resume_receipts').fetchone()[0],1)
   operation,stop=db.execute('select operation_deadline,stop_deadline from stop_phase_budgets').fetchone()
   self.assertLessEqual(stop,operation);self.assertEqual(operation,self.adapter.deadline)
  again=self.run_controller();self.assertEqual(again.state,result.state)
  self.adapter.stop_once.assert_called_once();self.adapter.start_once.assert_called_once()
  self.assertEqual([x.args[0] for x in self.adapter.read.call_args_list],['thread/resume','thread/read'])
 def test_controller_wrong_parent_refused_once(self):
  self.adapter.child.parent=(203,501,self.app[3],0)
  result=self.run_controller();self.assertEqual(result.state,'resume_intent');self.assertIn('binding differs',result.reason)
  self.adapter.stop_once.assert_called_once();self.adapter.start_once.assert_called_once()
  self.assertEqual([x.args[0] for x in self.adapter.read.call_args_list],['thread/resume','thread/read'])
 def test_controller_changed_cli_generation_refused_once(self):
  changed=(101,501,501,11,20)
  with patch.object(l.v2,'kernel_identity',side_effect=lambda pid:self.app if pid==202 else changed):result=self.run_controller()
  self.assertEqual(result.state,'resume_intent');self.assertIn('Root CLI changed',result.reason)
  self.adapter.stop_once.assert_called_once();self.adapter.start_once.assert_called_once()
 def test_controller_missing_receipt_refused_once(self):
  from agent_chat import provider_reattachment as r
  with patch.object(r,'record_resume',side_effect=l.Blocked('receipt unavailable')):result=self.run_controller()
  self.assertEqual(result.state,'resume_intent');self.assertIn('receipt unavailable',result.reason)
  self.adapter.stop_once.assert_called_once();self.adapter.start_once.assert_called_once()
  self.assertEqual([x.args[0] for x in self.adapter.read.call_args_list],['thread/resume'])
 def test_controller_deadline_after_receipt_never_replays(self):
  clock=[100.0]
  def expire(seconds):clock[0]=106.0
  with patch.object(s.time,'monotonic',side_effect=lambda:clock[0]),patch.object(s.time,'sleep',side_effect=expire),patch.object(l,'time',s.time):result=self.run_controller()
  self.assertEqual(result.state,'resume_intent');self.assertIn('deadline',result.reason)
  self.assertEqual(self.adapter.deadline,105.0)
  self.adapter.stop_once.assert_called_once();self.adapter.start_once.assert_called_once()
  self.assertEqual([x.args[0] for x in self.adapter.read.call_args_list],['thread/resume','thread/read'])
if __name__=='__main__': unittest.main(verbosity=2)
