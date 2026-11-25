"""Synthetic Root preflight/registry/SQLite diagnostics; no live authorities."""
import contextlib, io, os, sqlite3, subprocess, unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import test_hospital_binding_private as fixture
from agent_chat import hospital_vendor_binding as b, hospital_vendor_scope as s, root_local
from agent_chat.core import CoordError

class DiagnosticTests(unittest.TestCase):
 def setUp(self):
  fixture.BindingTests.setUp(self); self.reg.rollback()
  self.master=Path(self.tmp.name)/'project.sqlite3'; self.registry=Path(str(self.master)+'.projects.sqlite3')
  target=sqlite3.connect(self.registry); self.reg.backup(target); target.close()
  b._schema(self.db)
  self.config=SimpleNamespace(master=self.master,uid=os.getuid(),plist=Path(self.tmp.name)/'fixture.plist')
  for obj in (patch('agent_chat.service._installed_server_config',return_value=self.config), patch('agent_chat.service.configured_server_database',return_value=self.master), patch('agent_chat.root_local._preflight',return_value=(self.master,False))):
   obj.start(); self.addCleanup(obj.stop)
 def execute(self,op,params=None):
  return root_local.execute('hospital-vendor-'+op,params or {},master_db=self.master,service_plist=self.config.plist,home_project='default',session=b.ROOT,host_id=s.HOST)
 def snapshot(self):
  return (self.db.execute('SELECT name,owner_session,reservation_id,stale,deadline FROM resources ORDER BY name').fetchall(),
          self.db.execute("SELECT name FROM sqlite_master WHERE name LIKE 'hospital_vendor_%' ORDER BY name").fetchall())
 def test_real_generic_issue_and_diagnostic_cohort_failure(self):
  key=next(iter(self.manifest)); self.db.execute('UPDATE resources SET reservation_id=? WHERE name=?',('reservation_fixture_changed',key))
  before=self.snapshot()
  with self.assertRaisesRegex(CoordError,'hospital vendor binding refused'): self.execute('issue',{'confirm':True})
  self.assertEqual(before,self.snapshot())
  result=self.execute('diagnose'); self.assertEqual(result['stage'],'task_or_cohort'); self.assertEqual(result['reason'],'claim_changed')
  self.assertEqual(result['schema'],'existing_checked'); self.assertFalse(result['issue_attempted']); self.assertEqual(before,self.snapshot())
 def test_expired_observation_never_issues(self):
  before=self.snapshot()
  with patch('agent_chat.hospital_vendor_binding.time.time',return_value=s.TASK_PRODUCTIVE_UNTIL): result=self.execute('diagnose')
  self.assertEqual((result['stage'],result['reason']),('task_deadline','task_expired')); self.assertEqual(result['historical_cause'],'unknown'); self.assertEqual(before,self.snapshot())
 def test_sentinel_task_body_never_leaks(self):
  self.db.execute('UPDATE messages SET body=? WHERE id=?',('PRIVATE_SYNTHETIC_SENTINEL',s.TASK)); before=self.snapshot()
  result=self.execute('diagnose'); self.assertEqual(result['reason'],'task_changed'); self.assertNotIn('PRIVATE_SYNTHETIC_SENTINEL',str(result)); self.assertEqual(before,self.snapshot())
 def test_unrecognized_exception_payload_never_leaks(self):
  with patch('agent_chat.hospital_vendor_scope.check_current',side_effect=s.Refused('PRIVATE_SYNTHETIC_SENTINEL')):
   result=self.execute('diagnose')
  self.assertEqual(result['reason'],'task_or_cohort_refused'); self.assertNotIn('PRIVATE_SYNTHETIC_SENTINEL',str(result))
 def test_valid_snapshot_not_claimed_ready(self):
  before=self.snapshot(); result=self.execute('diagnose'); self.assertEqual(result['reason'],'no_observed_refusal'); self.assertEqual(result['issue_readiness'],'not_established'); self.assertEqual(before,self.snapshot())
 def test_wrong_root_identity_rejected(self):
  with self.assertRaises(CoordError): root_local.execute('hospital-vendor-diagnose',{},master_db=self.master,service_plist=self.config.plist,home_project='default',session='wrong',host_id=s.HOST)
 def test_root_local_cli_diagnostic_parser_and_output(self):
  stream=io.StringIO()
  with contextlib.redirect_stdout(stream):
   rc=root_local.main(['--home-project','default','--home-session',b.ROOT,'--host-id',s.HOST,'hospital-vendor-diagnose'])
  self.assertEqual(rc,0)
  probe=subprocess.run(['jq','-e','.issue_attempted == false and .issue_readiness == "not_established" and .reason == "no_observed_refusal"'],input=stream.getvalue(),text=True,capture_output=True)
  self.assertEqual(probe.returncode,0,probe.stderr)
 def test_unread_input_and_missing_ack_use_actual_predicates(self):
  self.db.execute('INSERT INTO messages(id,sender_session,recipient_session,body,created_at) VALUES(?,?,?,?,?)',('message_fixture',s.UNITY_PO,s.CONSUMER,'fixture',s.TASK_STARTED+3))
  self.assertEqual(self.execute('diagnose')['reason'],'recipient_has_unconsumed_input')
  self.db.execute('DELETE FROM messages WHERE id=?',('message_fixture',))
  self.db.execute('DELETE FROM message_reads WHERE message_id=?',(s.ESSENTIAL,))
  self.assertEqual(self.execute('diagnose')['reason'],'essential_task_association_missing')
 def test_prior_binding_does_not_claim_absent(self):
  self.execute('issue',{'confirm':True}); before=self.snapshot()
  result=self.execute('diagnose'); self.assertEqual((result['stage'],result['reason']),('binding','binding_already_exists'))
  self.assertEqual(before,self.snapshot())
 def test_partial_schema_is_unknown_not_an_issue_failure_claim(self):
  for table in ('hospital_vendor_file_events','hospital_vendor_events','hospital_vendor_binding'): self.db.execute('DROP TABLE '+table)
  self.db.execute('CREATE TABLE hospital_vendor_binding(id TEXT PRIMARY KEY)'); before=self.snapshot()
  result=self.execute('diagnose'); self.assertEqual((result['state'],result['reason']),('unknown','schema_partial_unverified'))
  self.assertEqual(before,self.snapshot())
 def test_absent_schema_is_unavailable_without_creation(self):
  for table in ('hospital_vendor_file_events','hospital_vendor_events','hospital_vendor_binding'): self.db.execute('DROP TABLE '+table)
  before=self.snapshot(); result=self.execute('diagnose')
  self.assertEqual((result['state'],result['reason']),('unknown','schema_absent_unavailable')); self.assertEqual(before,self.snapshot())
 def test_readonly_target_rejects_write_from_predicate(self):
  def attempt_write(db,now): db.execute("DELETE FROM resources")
  before=self.snapshot()
  with patch('agent_chat.hospital_vendor_binding._task_and_scope',side_effect=attempt_write):
   result=self.execute('diagnose')
  self.assertEqual((result['state'],result['reason']),('unknown','observation_unavailable'))
  self.assertEqual(before,self.snapshot())
