"""Root-selected continuation fixtures: synthetic records and private SQLite only."""
import hashlib, unittest
from unittest.mock import patch
import test_hospital_binding_private as fixture
from agent_chat import hospital_vendor_binding as b, hospital_vendor_scope as s
from agent_chat.core import CoordError

class AssociationTests(unittest.TestCase):
 def setUp(self): fixture.BindingTests.setUp(self)
 def packet(self,label,start):
  task='message_task_'+label; intake='message_intake_'+label; essential='message_essential_'+label
  body='Synthetic selected full Hospital task '+label; digest=hashlib.sha256(body.encode()).hexdigest()
  self.db.executemany('INSERT INTO messages(id,sender_session,recipient_session,body,created_at,acked_at) VALUES(?,?,?,?,?,?)',[
   (task,s.UNITY_PO,s.CONSUMER,body,start-2,start+1),
   (intake,s.CONSUMER,s.UNITY_PO,'fixture intake',start,start+2),
   (essential,s.UNITY_PO,s.CONSUMER,'fixture essential',start+1,None)])
  self.db.executemany('INSERT INTO message_reads VALUES(?,?,?,?)',[(task,s.CONSUMER,start,'recipient_full_message'),(intake,s.UNITY_PO,start+1,'recipient_full_message'),(essential,s.CONSUMER,start+2,'recipient_full_message')])
  self.db.executemany('INSERT INTO message_replies VALUES(?,?)',[(intake,task),(essential,intake)])
  self.db.executemany('INSERT INTO reply_ack_idempotency VALUES(?,?,?,?)',[(s.CONSUMER,task,'fingerprint-'+label,intake),(s.UNITY_PO,intake,'essential-'+label,essential)])
  return {'task_id':task,'intake_id':intake,'essential_id':essential,'body_sha256':digest,
          'intake_sha256':hashlib.sha256(b'fixture intake').hexdigest(),
          'essential_sha256':hashlib.sha256(b'fixture essential').hexdigest(),
          'expires_at':start+7100,'confirm':True}
 def select(self,params):
  self.db.execute('BEGIN IMMEDIATE')
  try: result=b.select_in_transaction(self.db,self.gate,params); self.db.commit(); return result
  except BaseException: self.db.rollback(); raise
 def revoke(self,binding_id):
  self.db.execute('BEGIN IMMEDIATE')
  try: result=b.revoke_in_transaction(self.db,self.gate,'root_revoked',binding_id); self.db.commit(); return result
  except BaseException: self.db.rollback(); raise
 def test_distinct_tasks_keep_legacy_and_separate_immutable_history(self):
  old=fixture.BindingTests.issue(self); oldrows=self.db.execute('SELECT * FROM hospital_vendor_binding WHERE id=?',(old['binding_id'],)).fetchall()
  oldevents=self.db.execute('SELECT * FROM hospital_vendor_events WHERE binding_id=? ORDER BY seq',(old['binding_id'],)).fetchall()
  world=self.db.execute('SELECT name,owner_session,reservation_id,stale,deadline FROM resources WHERE owner_session=? ORDER BY name',(s.OWNER,)).fetchall()
  start=s.TASK_PRODUCTIVE_UNTIL+5; one=self.packet('one',start); two=self.packet('two',start+1)
  with patch('agent_chat.hospital_vendor_binding.time.time',return_value=start+10):
   first=self.select(one); self.assertNotEqual(first['binding_id'],old['binding_id']); self.assertEqual(self.revoke(first['binding_id'])['state'],'revoked')
   second=self.select(two); self.assertNotEqual(second['binding_id'],first['binding_id'])
   self.assertEqual(oldrows,self.db.execute('SELECT * FROM hospital_vendor_binding WHERE id=?',(old['binding_id'],)).fetchall())
   self.assertEqual(oldevents,self.db.execute('SELECT * FROM hospital_vendor_events WHERE binding_id=? ORDER BY seq',(old['binding_id'],)).fetchall())
   self.assertEqual(world,self.db.execute('SELECT name,owner_session,reservation_id,stale,deadline FROM resources WHERE owner_session=? ORDER BY name',(s.OWNER,)).fetchall())
   self.assertEqual(self.db.execute('SELECT COUNT(*) FROM hospital_vendor_selection').fetchone()[0],2)
   with self.assertRaises(Exception): self.db.execute('UPDATE hospital_vendor_selection SET task=? WHERE id=?',('fake',first['binding_id']))
 def test_unrelated_unread_does_not_block_root_selection_but_blocks_consumer(self):
  start=s.TASK_STARTED+1; packet=self.packet('unread',start)
  self.db.execute('INSERT INTO messages(id,sender_session,recipient_session,body,created_at) VALUES(?,?,?,?,?)',('message_unrelated',s.UNITY_PO,s.CONSUMER,'fixture',start+3))
  selected=self.select(packet); self.assertEqual(selected['state'],'pending')
  with self.assertRaises(CoordError): self.coord.request(s.CONTROL,60,registry_db=self.gate)
  self.db.execute('INSERT INTO message_reads VALUES(?,?,?,?)',('message_unrelated',s.CONSUMER,start+4,'recipient_full_message'))
  control=self.coord.request(s.CONTROL,60,registry_db=self.gate); self.assertEqual(control['state'],'owned')
 def test_prior_open_copy_guard_blocks_new_control(self):
  packet=self.packet('oldguard',s.TASK_STARTED+1); self.select(packet)
  self.db.execute('INSERT INTO remote_guarded_runs VALUES(?,?,?,?,?,?,?,?,?)',('remote_run_old',s.RUN_RESOURCE,'reservation_old',s.CONSUMER,s.HOST,12345,s.TASK_STARTED,None,None))
  with self.assertRaises(CoordError): self.coord.request(s.CONTROL,60,registry_db=self.gate)
  self.db.execute('UPDATE remote_guarded_runs SET closed_at=? WHERE run_id=?',(s.TASK_STARTED+5,'remote_run_old'))
  self.db.execute('INSERT INTO guarded_runs VALUES(?,?,?,?,?,?,?,?)',('local_run_old',s.RUN_RESOURCE,'reservation_old',s.CONSUMER,12345,None,s.TASK_STARTED,None))
  with self.assertRaises(CoordError): self.coord.request(s.CONTROL,60,registry_db=self.gate)
  self.db.execute('UPDATE guarded_runs SET closed_at=? WHERE run_id=?',(s.TASK_STARTED+5,'local_run_old'))
  self.assertEqual(self.coord.request(s.CONTROL,60,registry_db=self.gate)['state'],'owned')
 def test_wrong_sender_ack_digest_deadline_and_cohort_fail(self):
  start=s.TASK_STARTED+1; packet=self.packet('bad',start); bad=dict(packet,body_sha256='0'*64)
  with self.assertRaises(s.Refused): self.select(bad)
  with self.assertRaises(s.Refused): self.select(dict(packet,expires_at=start+7201))
  self.db.execute('UPDATE messages SET sender_session=? WHERE id=?',('foreign',packet['task_id']))
  with self.assertRaises(s.Refused): self.select(packet)
  self.db.execute('UPDATE messages SET sender_session=? WHERE id=?',(s.UNITY_PO,packet['task_id']))
  self.db.execute('UPDATE messages SET recipient_session=? WHERE id=?',('foreign',packet['task_id']))
  with self.assertRaises(s.Refused): self.select(packet)
  self.db.execute('UPDATE messages SET recipient_session=? WHERE id=?',(s.CONSUMER,packet['task_id']))
  self.db.execute('DELETE FROM reply_ack_idempotency WHERE message_id=?',(packet['intake_id'],))
  with self.assertRaises(s.Refused): self.select(packet)
  self.db.execute('INSERT INTO reply_ack_idempotency VALUES(?,?,?,?)',(s.CONSUMER,packet['task_id'],'restored-fixture',packet['intake_id']))
  self.db.execute('UPDATE messages SET body=? WHERE id=?',('changed',packet['essential_id']))
  with self.assertRaises(s.Refused): self.select(packet)
  self.db.execute('UPDATE messages SET body=? WHERE id=?',('fixture essential',packet['essential_id']))
  self.db.execute('DELETE FROM message_reads WHERE message_id=?',(packet['intake_id'],))
  with self.assertRaises(s.Refused): self.select(packet)
  self.db.execute('INSERT INTO message_reads VALUES(?,?,?,?)',(packet['intake_id'],s.UNITY_PO,start+1,'recipient_full_message'))
  self.db.execute('DELETE FROM message_reads WHERE message_id=?',(packet['essential_id'],))
  with self.assertRaises(s.Refused): self.select(packet)
 def test_fixed_cohort_change_refuses_new_selection(self):
  packet=self.packet('cohort',s.TASK_STARTED+1); key=next(iter(self.manifest))
  self.db.execute('UPDATE resources SET reservation_id=? WHERE name=?',('reservation_other',key))
  with self.assertRaises(s.Refused): self.select(packet)
 def test_future_acknowledgment_timestamp_refuses_selection(self):
  packet=self.packet('future-ack',s.TASK_STARTED+1)
  self.db.execute('UPDATE messages SET acked_at=? WHERE id=?',(s.TASK_STARTED+100,packet['intake_id']))
  with self.assertRaises(s.Refused): self.select(packet)
 def test_late_reads_do_not_fabricate_prior_consumption(self):
  start=s.TASK_STARTED+1; packet=self.packet('late-read',start)
  self.db.execute('UPDATE message_reads SET read_at=? WHERE message_id=?',(start+0.5,packet['task_id']))
  with self.assertRaises(s.Refused): self.select(packet)
  self.db.execute('UPDATE message_reads SET read_at=? WHERE message_id=?',(start,packet['task_id']))
  self.db.execute('UPDATE message_reads SET read_at=? WHERE message_id=?',(start+1.5,packet['intake_id']))
  with self.assertRaises(s.Refused): self.select(packet)
 def test_wrong_operator_authority_refuses_new_selection(self):
  from agent_chat import capacity_admission as capacity
  packet=self.packet('operator',s.TASK_STARTED+1)
  self.db.execute('UPDATE messages SET body=? WHERE id=?',('changed',capacity.ROOT_OPERATION_SOURCE_ID))
  with self.assertRaises(capacity.AdmissionRefused): self.select(packet)
 def test_selected_copy_unverified_no_replay_and_revocation(self):
  packet=self.packet('use',s.TASK_STARTED+1); selected=self.select(packet)
  control=self.coord.request(s.CONTROL,60,registry_db=self.gate); key=next(iter(self.manifest))
  args=(self.db,s.CONSUMER,s.HOST,control['reservation_id'],control['token'],key,'run-fixture','out-run','/private/tmp/output','/private/tmp/output/file')
  self.db.execute('BEGIN IMMEDIATE'); start=b.start_file_in_transaction(*args,self.gate); self.db.commit()
  self.db.execute('BEGIN IMMEDIATE'); result=b.finish_file_in_transaction(*args,start['operation_id'],'a'*64,12,self.gate); self.db.commit()
  self.assertEqual((result['binding_id'],result['state']),(selected['binding_id'],'unverified'))
  self.db.execute('BEGIN IMMEDIATE')
  try:
   with self.assertRaises(b.Refused): b.start_file_in_transaction(*args,self.gate)
  finally: self.db.rollback()
  self.assertEqual(self.revoke(selected['binding_id'])['state'],'failed_or_unknown')
 def test_selection_identity_expiry_and_no_duplicate(self):
  start=s.TASK_STARTED+1; one=self.packet('unique',start); two=self.packet('compete',start+1)
  selected=self.select(one)
  with self.assertRaises(b.Refused): self.select(two)
  with patch('agent_chat.hospital_vendor_binding.time.time',return_value=one['expires_at']):
   self.assertEqual(self.revoke(selected['binding_id'])['state'],'expired')
   with self.assertRaises(b.Refused): self.select(one)
  with self.assertRaises(b.Refused): self.select(one)
 def test_selection_schema_tamper_is_not_silently_repaired(self):
  self.select(self.packet('first',s.TASK_STARTED+1))
  self.db.execute('DROP TRIGGER hospital_vendor_selection_immutable_update')
  with self.assertRaisesRegex(b.Refused,'selection_triggers_changed'):
   self.select(self.packet('second',s.TASK_STARTED+2))
  self.assertIsNone(self.db.execute("SELECT 1 FROM sqlite_master WHERE name='hospital_vendor_selection_immutable_update'").fetchone())
 def test_expiry_crossing_during_selection_rolls_back(self):
  packet=self.packet('cross',s.TASK_STARTED+1); clock=[s.TASK_STARTED+10]; original=b._task_and_scope
  def cross(*args,**kwargs):
   result=original(*args,**kwargs); clock[0]=packet['expires_at']; return result
  with patch('agent_chat.hospital_vendor_binding.time.time',side_effect=lambda:clock[0]), patch.object(b,'_task_and_scope',side_effect=cross):
   with self.assertRaisesRegex(b.Refused,'selection_expired'): self.select(packet)
  self.assertIsNone(self.db.execute("SELECT 1 FROM sqlite_master WHERE name='hospital_vendor_selection'").fetchone())
 def test_expiry_crossing_during_control_acquisition_rolls_back(self):
  packet=self.packet('control-cross',s.TASK_STARTED+1); self.select(packet)
  clock=[s.TASK_STARTED+10]; original=b._task_and_scope
  def cross(*args,**kwargs):
   result=original(*args,**kwargs); clock[0]=packet['expires_at']; return result
  with patch('agent_chat.hospital_vendor_binding.time.time',side_effect=lambda:clock[0]), patch.object(b,'_task_and_scope',side_effect=cross):
   with self.assertRaises(CoordError): self.coord.request(s.CONTROL,60,registry_db=self.gate)
  self.assertIsNone(self.db.execute('SELECT owner_session FROM resources WHERE name=?',(s.CONTROL,)).fetchone())
 def test_selected_body_change_and_host_change_block_capability(self):
  packet=self.packet('changed',s.TASK_STARTED+1); self.select(packet)
  self.db.execute('UPDATE messages SET body=? WHERE id=?',('changed',packet['task_id']))
  with self.assertRaises(CoordError): self.coord.request(s.CONTROL,60,registry_db=self.gate)
  self.db.execute('UPDATE messages SET body=? WHERE id=?',('Synthetic selected full Hospital task changed',packet['task_id']))
  self.db.execute('UPDATE messages SET body=? WHERE id=?',('altered ack',packet['intake_id']))
  with self.assertRaises(CoordError): self.coord.request(s.CONTROL,60,registry_db=self.gate)
  self.db.execute('UPDATE messages SET body=? WHERE id=?',('fixture intake',packet['intake_id']))
  self.db.execute('UPDATE remote_session_hosts SET host_id=? WHERE session_id=?',('foreign',s.CONSUMER))
  with self.assertRaises(CoordError): self.coord.request(s.CONTROL,60,registry_db=self.gate)
 def test_ack_change_during_selected_copy_denies_pulse(self):
  packet=self.packet('pulse',s.TASK_STARTED+1); self.select(packet)
  control=self.coord.request(s.CONTROL,60,registry_db=self.gate); key=next(iter(self.manifest))
  args=(self.db,s.CONSUMER,s.HOST,control['reservation_id'],control['token'],key,'run-fixture','out-run','/private/tmp/output','/private/tmp/output/file')
  self.db.execute('BEGIN IMMEDIATE'); start=b.start_file_in_transaction(*args,self.gate); self.db.commit()
  self.db.execute('DELETE FROM message_reads WHERE message_id=?',(packet['essential_id'],))
  self.db.execute('BEGIN IMMEDIATE')
  try:
   with self.assertRaises(s.Refused): b.pulse_file_in_transaction(*args,start['operation_id'],self.gate)
  finally: self.db.rollback()

class RootSelectionTests(unittest.TestCase):
 def setUp(self):
  import os, sqlite3
  from pathlib import Path
  from types import SimpleNamespace
  fixture.BindingTests.setUp(self); self.reg.rollback()
  self.master=Path(self.tmp.name)/'project.sqlite3'; registry=Path(str(self.master)+'.projects.sqlite3')
  target=sqlite3.connect(registry); self.reg.backup(target); target.close()
  config=SimpleNamespace(master=self.master,uid=os.getuid(),plist=Path(self.tmp.name)/'fixture.plist'); self.config=config
  for obj in (patch('agent_chat.service._installed_server_config',return_value=config), patch('agent_chat.service.configured_server_database',return_value=self.master), patch('agent_chat.root_local._preflight',return_value=(self.master,False))):
   obj.start(); self.addCleanup(obj.stop)
 def test_root_selection_parser_status_and_revoke(self):
  from agent_chat import root_local
  packet=AssociationTests.packet(self,'root',s.TASK_STARTED+1)
  def call(op,params): return root_local.execute('hospital-vendor-'+op,params,master_db=self.master,service_plist=self.config.plist,home_project='default',session=b.ROOT,host_id=s.HOST)
  selected=call('select',packet); self.assertEqual(selected['state'],'pending')
  self.assertEqual(call('selection-status',{'binding_id':selected['binding_id']})['state'],'pending')
  self.assertEqual(call('selection-revoke',{'binding_id':selected['binding_id'],'confirm':True})['state'],'revoked')
  with self.assertRaises(CoordError): root_local.execute('hospital-vendor-select',packet,master_db=self.master,service_plist=self.config.plist,home_project='default',session='foreign',host_id=s.HOST)
 def test_lost_selection_response_reconciles_by_deterministic_id(self):
  from agent_chat import root_local
  packet=AssociationTests.packet(self,'lost-root',s.TASK_STARTED+1)
  def call(op,params): return root_local.execute('hospital-vendor-'+op,params,master_db=self.master,service_plist=self.config.plist,home_project='default',session=b.ROOT,host_id=s.HOST)
  call('select',packet)  # committed result deliberately not used
  binding_id='hospital-vendor-task-'+hashlib.sha256(packet['task_id'].encode()).hexdigest()
  self.assertEqual(call('selection-status',{'binding_id':binding_id})['state'],'pending')
  with self.assertRaisesRegex(CoordError,'hospital vendor binding refused'): call('select',packet)
 def test_root_local_cli_parses_exact_selection(self):
  import contextlib, io, subprocess
  from agent_chat import root_local
  packet=AssociationTests.packet(self,'cli',s.TASK_STARTED+1); stream=io.StringIO()
  args=['--home-project','default','--home-session',b.ROOT,'--host-id',s.HOST,'hospital-vendor-select',
        '--task-id',packet['task_id'],'--intake-id',packet['intake_id'],'--essential-id',packet['essential_id'],
        '--body-sha256',packet['body_sha256'],'--intake-sha256',packet['intake_sha256'],
        '--essential-sha256',packet['essential_sha256'],'--expires-at',str(packet['expires_at']),'--confirm']
  with contextlib.redirect_stdout(stream): rc=root_local.main(args)
  self.assertEqual(rc,0)
  result=subprocess.run(['jq','-e','.state=="pending" and (.binding_id|startswith("hospital-vendor-task-"))'],input=stream.getvalue(),text=True,capture_output=True)
  self.assertEqual(result.returncode,0,result.stderr)

import test_hospital_cli_private as cli_fixture
class SelectedCLIFlowTests(cli_fixture.CLIFlowTests):
 def prepare_server(self):
  packet=AssociationTests.packet(self,'http',s.TASK_STARTED+1)
  self.issue=lambda: AssociationTests.select(self,packet)
  super().prepare_server()
 def test_selected_task_real_cli_http_guard_copy(self):
  self._workflow()
  binding=self.db.execute('SELECT id FROM hospital_vendor_selection').fetchone()[0]
  self.assertEqual(self.db.execute('SELECT state FROM hospital_vendor_file_events WHERE binding_id=? ORDER BY seq DESC LIMIT 1',(binding,)).fetchone()[0],'unverified')
 def test_selected_task_revoke_during_stream(self): self._workflow('revoke')
 def test_selected_task_stop_during_stream(self): self._workflow('stop')
 def test_selected_task_lost_finish_no_replay(self): self._workflow('lost')
