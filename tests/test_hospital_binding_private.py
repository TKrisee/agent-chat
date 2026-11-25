"""Isolated real Coordinator schema; synthetic subjects, no live authorities."""
import os, sqlite3, tempfile, time, unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from agent_chat.core import Coordinator, CoordError
from agent_chat import remote_service, capacity_admission as c, hospital_vendor_scope as s, hospital_vendor_binding as b

class BindingTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory(dir=os.environ.get('HOSPITAL_FIXTURE_TMP')); self.addCleanup(self.tmp.cleanup)
  self.coord=Coordinator(Path(self.tmp.name)/'project.sqlite3',session=s.CONSUMER); self.addCleanup(self.coord.close); self.db=self.coord.db
  remote_service._schema(self.coord)
  self.db.execute('CREATE TABLE bridge_bindings(session_id TEXT PRIMARY KEY,thread_id TEXT,parent_session TEXT,agent_path TEXT,updated_at REAL)')
  for sid in (b.ROOT,s.UNITY_PO,s.CONSUMER,s.OWNER,c.ROOT_OPERATION_SOURCE_SENDER):
   self.db.execute('INSERT INTO sessions(id,agent,registered_at,inbox_read_seq) VALUES(?,?,?,0)',(sid,sid,1))
  for sid in (b.ROOT,s.UNITY_PO,s.CONSUMER,s.OWNER):
   self.db.execute('INSERT INTO remote_session_hosts VALUES(?,?,?,?)',(sid,s.HOST,1,1))
  self.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?,?,?)',(s.CONSUMER,s.THREAD,s.UNITY_PO,None,1))
  task=(Path(__file__).parent / 'fixtures' / 'HospitalCurrentCopyBuildPlay02-task.txt').read_text()
  rows=[(c.ROOT_OPERATION_SOURCE_ID,c.ROOT_OPERATION_SOURCE_SENDER,b.ROOT,c.ROOT_OPERATION_SOURCE_BODY,1,None),
        (s.TASK,s.UNITY_PO,s.CONSUMER,task,2,3),(s.INTAKE,s.CONSUMER,s.UNITY_PO,'fixture intake',s.TASK_STARTED,s.TASK_STARTED+2),
        (s.ESSENTIAL,s.UNITY_PO,s.CONSUMER,'fixture essential',s.TASK_STARTED+1,None)]
  self.db.executemany('INSERT INTO messages(id,sender_session,recipient_session,body,created_at,acked_at) VALUES(?,?,?,?,?,?)',rows)
  self.db.execute('INSERT INTO operator_identities VALUES(?)',(c.ROOT_OPERATION_SOURCE_SENDER,))
  self.db.execute('INSERT INTO operator_message_origins VALUES(?,?,?)',(c.ROOT_OPERATION_SOURCE_ID,c.ROOT_OPERATION_SOURCE_SENDER,'authenticated_operator_ui'))
  self.db.executemany('INSERT INTO message_reads VALUES(?,?,?,?)',[(s.TASK,s.CONSUMER,s.TASK_STARTED,'recipient_full_message'),(s.ESSENTIAL,s.CONSUMER,s.TASK_STARTED+2,'recipient_full_message')])
  self.db.execute('INSERT INTO message_replies VALUES(?,?)',(s.INTAKE,s.TASK)); self.db.execute('INSERT INTO message_replies VALUES(?,?)',(s.ESSENTIAL,s.INTAKE))
  self.db.execute('INSERT INTO reply_ack_idempotency VALUES(?,?,?,?)',(s.CONSUMER,s.TASK,'fixture-fingerprint',s.INTAKE))
  self.db.execute('INSERT INTO reply_ack_idempotency VALUES(?,?,?,?)',(s.UNITY_PO,s.INTAKE,'fixture-fingerprint-2',s.ESSENTIAL))
  self.manifest=s.manifest(); self.db.executemany('INSERT INTO resources(name,owner_session,reservation_id,token,granted_at,deadline,stale,reason) VALUES(?,?,?,?,?,?,?,NULL)',[(name,s.OWNER,rid,'fixture-world-token',deadline-100,deadline,1) for name,(rid,deadline) in self.manifest.items()])
  self.reg=sqlite3.connect(':memory:',isolation_level=None); self.addCleanup(self.reg.close); self.reg.row_factory=sqlite3.Row
  self.reg.executescript('''CREATE TABLE root_config(singleton INTEGER PRIMARY KEY,home_project TEXT,home_session TEXT,thread_id TEXT,host_id TEXT,mode TEXT);
   CREATE TABLE root_directory_owner(singleton INTEGER PRIMARY KEY,home_project TEXT,home_session TEXT);
   CREATE TABLE root_projects(project_id TEXT PRIMARY KEY,session_id TEXT,orchestrator_session TEXT,joined INTEGER);''')
  self.reg.execute('INSERT INTO root_config VALUES(1,?,?,?,?,?)',('default',b.ROOT,b.ROOT_THREAD,s.HOST,'running'))
  self.reg.execute('INSERT INTO root_directory_owner VALUES(1,?,?)',('default',b.ROOT))
  self.reg.execute('INSERT INTO root_projects VALUES(?,?,?,1)',('default',b.ROOT,s.UNITY_PO))
  self.reg.execute('BEGIN IMMEDIATE'); self.gate=c._RegistryGate(self.reg,lambda:None)
  self.now=patch('agent_chat.hospital_vendor_binding.time.time',return_value=s.TASK_STARTED+10); self.now.start(); self.addCleanup(self.now.stop)
 def issue(self):
  self.db.execute('BEGIN IMMEDIATE')
  try:
   result=b.issue_in_transaction(self.db,self.gate); self.db.commit(); return result
  except BaseException: self.db.rollback(); raise
 def test_issue_pending_preserves_world_rows(self):
  before=self.db.execute('SELECT name,owner_session,reservation_id,stale,deadline FROM resources ORDER BY name').fetchall()
  self.assertEqual(self.issue()['state'],'pending'); self.assertEqual(before,self.db.execute('SELECT name,owner_session,reservation_id,stale,deadline FROM resources ORDER BY name').fetchall())
 def test_wrong_world_rid_fails(self):
  self.db.execute('UPDATE resources SET reservation_id=? WHERE name=?',('reservation_changed',next(iter(self.manifest))))
  with self.assertRaises(s.Refused): self.issue()
 def test_missing_ack_fails(self):
  self.db.execute('DELETE FROM message_reads WHERE message_id=?',(s.ESSENTIAL,))
  with self.assertRaises(s.Refused): self.issue()
 def test_wrong_operator_origin_fails(self):
  self.db.execute('UPDATE messages SET body=? WHERE id=?',('changed',c.ROOT_OPERATION_SOURCE_ID))
  with self.assertRaises(c.AdmissionRefused): self.issue()
 def test_control_requires_pending_then_possession(self):
  self.issue(); result=self.coord.request(s.CONTROL,60,registry_db=self.gate)
  self.assertEqual(result['state'],'owned')
  self.db.execute('BEGIN IMMEDIATE')
  try:
   accepted=b.validate_in_transaction(self.db,s.CONSUMER,s.HOST,result['reservation_id'],result['token'],next(iter(self.manifest)),self.gate)
   self.assertEqual(accepted['state'],'active')
  finally: self.db.rollback()
  with self.assertRaises(CoordError): self.coord.request(s.CONTROL,60,registry_db=self.gate)
  self.db.execute('BEGIN IMMEDIATE')
  try:
   with self.assertRaises(s.Refused): b.validate_in_transaction(self.db,s.CONSUMER,s.HOST,result['reservation_id'],'forged',next(iter(self.manifest)),self.gate)
  finally: self.db.rollback()
 def test_control_without_pending_fails(self):
  with self.assertRaises(CoordError): self.coord.request(s.CONTROL,60,registry_db=self.gate)
 def test_all_forged_reports_never_consume_binding(self):
  self.issue(); control=self.coord.request(s.CONTROL,60,registry_db=self.gate)
  for key in self.manifest:
   args=(self.db,s.CONSUMER,s.HOST,control['reservation_id'],control['token'],key,'run-fixture','out-run','/private/tmp/output','/private/tmp/output/file')
   self.db.execute('BEGIN IMMEDIATE'); start=b.start_file_in_transaction(*args,self.gate); self.db.commit()
   self.db.execute('BEGIN IMMEDIATE'); result=b.finish_file_in_transaction(*args,start['operation_id'],'f'*64,1,self.gate); self.db.commit()
   self.assertEqual(result['state'],'unverified')
  self.assertEqual(result['reported'],108)
  self.db.execute('BEGIN IMMEDIATE'); status=b.root_status_in_transaction(self.db,self.gate); self.db.commit()
  self.assertEqual(status['state'],'active'); self.assertEqual(len(status['unresolved_files']),108)
  self.db.execute('BEGIN IMMEDIATE'); result=b.revoke_in_transaction(self.db,self.gate,'root_revoked'); self.db.commit()
  self.assertEqual(result['state'],'failed_or_unknown')
 def test_control_cleanup_reserve_is_required(self):
  self.issue(); result=self.coord.request(s.CONTROL,45,registry_db=self.gate)
  self.db.execute('BEGIN IMMEDIATE')
  try:
   with self.assertRaisesRegex(b.Refused,'cleanup_reserve_unavailable'):
    b.validate_in_transaction(self.db,s.CONSUMER,s.HOST,result['reservation_id'],result['token'],next(iter(self.manifest)),self.gate)
  finally: self.db.rollback()
 def test_control_never_authorizes_generic_guard(self):
  self.issue(); result=self.coord.request(s.CONTROL,60,registry_db=self.gate)
  with self.assertRaisesRegex(CoordError,'cannot authorize arbitrary execution'):
   self.coord.begin_guard(s.CONTROL,result['token'],12345)
  with self.assertRaisesRegex(CoordError,'cannot authorize arbitrary execution'):
   remote_service._begin_guard(self.coord,s.CONSUMER,s.HOST,{'resource':s.CONTROL,'token':result['token']})
  with self.assertRaisesRegex(CoordError,'cannot authorize arbitrary execution'):
   remote_service._begin_guard_batch(self.coord,s.CONSUMER,s.HOST,{'tuples':[dict(resource=s.CONTROL,reservation_id=result['reservation_id'],token=result['token'],session_id=s.CONSUMER,host_id=s.HOST)],'restore':False})
 def test_run_resource_cannot_authorize_generic_guard(self):
  result=self.coord.request(s.RUN_RESOURCE,60)
  with self.assertRaisesRegex(CoordError,'cannot authorize arbitrary execution'):
   self.coord.begin_guard(s.RUN_RESOURCE,result['token'],12345)
  with self.assertRaisesRegex(CoordError,'cannot authorize arbitrary execution'):
   remote_service._begin_guard(self.coord,s.CONSUMER,s.HOST,{'resource':s.RUN_RESOURCE,'token':result['token']})
  with self.assertRaisesRegex(CoordError,'cannot authorize arbitrary execution'):
   remote_service._begin_guard_batch(self.coord,s.CONSUMER,s.HOST,{'tuples':[dict(resource=s.RUN_RESOURCE,reservation_id=result['reservation_id'],token=result['token'],session_id=s.CONSUMER,host_id=s.HOST)],'restore':False})
 def test_exact_file_lifecycle_and_duplicate_refusal(self):
  self.issue(); control=self.coord.request(s.CONTROL,60,registry_db=self.gate); key=next(iter(self.manifest))
  args=(self.db,s.CONSUMER,s.HOST,control['reservation_id'],control['token'],key,'run-fixture','output-run-fixture','/private/tmp/output','/private/tmp/output/file')
  before=self.db.execute('SELECT name,owner_session,reservation_id,stale,deadline FROM resources WHERE owner_session=? ORDER BY name',(s.OWNER,)).fetchall()
  self.db.execute('BEGIN IMMEDIATE'); started=b.start_file_in_transaction(*args,self.gate); self.db.commit()
  self.db.execute('BEGIN IMMEDIATE'); b.pulse_file_in_transaction(*args,started['operation_id'],self.gate); self.db.commit()
  self.db.execute('BEGIN IMMEDIATE'); result=b.finish_file_in_transaction(*args,started['operation_id'],'a'*64,12,self.gate); self.db.commit()
  self.assertEqual(result['state'],'unverified')
  self.db.execute('BEGIN IMMEDIATE'); status=b.root_status_in_transaction(self.db,self.gate); self.db.commit()
  self.assertEqual(status['state'],'active'); self.assertEqual(status['unresolved_files'][0]['state'],'unverified')
  self.assertEqual(before,self.db.execute('SELECT name,owner_session,reservation_id,stale,deadline FROM resources WHERE owner_session=? ORDER BY name',(s.OWNER,)).fetchall())
  self.db.execute('BEGIN IMMEDIATE')
  try:
   with self.assertRaises(b.Refused): b.start_file_in_transaction(*args,self.gate)
  finally: self.db.rollback()
 def test_changed_host_or_extra_foreign_claim_fails_closed(self):
  self.issue(); control=self.coord.request(s.CONTROL,60,registry_db=self.gate); key=next(iter(self.manifest))
  self.db.execute('INSERT INTO resources(name,owner_session,reservation_id,token,granted_at,deadline,stale) VALUES(?,?,?,?,?,?,?)',('file:Assets/other','foreign','reservation_other','token',1,1,1))
  self.db.execute('BEGIN IMMEDIATE')
  try:
   with self.assertRaises(s.Refused): b.validate_in_transaction(self.db,s.CONSUMER,s.HOST,control['reservation_id'],control['token'],key,self.gate)
   with self.assertRaises(b.Refused): b.validate_in_transaction(self.db,s.CONSUMER,'foreign',control['reservation_id'],control['token'],key,self.gate)
  finally: self.db.rollback()
 def test_revoke_after_unfinished_is_unknown(self):
  self.issue(); control=self.coord.request(s.CONTROL,60,registry_db=self.gate); key=next(iter(self.manifest))
  args=(self.db,s.CONSUMER,s.HOST,control['reservation_id'],control['token'],key,'run-fixture','output-run-fixture','/private/tmp/output','/private/tmp/output/file')
  self.db.execute('BEGIN IMMEDIATE'); b.start_file_in_transaction(*args,self.gate); self.db.commit()
  self.db.execute('BEGIN IMMEDIATE'); result=b.revoke_in_transaction(self.db,self.gate,'root_revoked'); self.db.commit()
  self.assertEqual(result['state'],'failed_or_unknown')
 def test_revoke_during_copy_denies_pulse_and_no_replay(self):
  self.issue(); control=self.coord.request(s.CONTROL,60,registry_db=self.gate); key=next(iter(self.manifest))
  args=(self.db,s.CONSUMER,s.HOST,control['reservation_id'],control['token'],key,'run-fixture','out-run','/private/tmp/output','/private/tmp/output/file')
  self.db.execute('BEGIN IMMEDIATE'); started=b.start_file_in_transaction(*args,self.gate); self.db.commit()
  self.db.execute('BEGIN IMMEDIATE'); result=b.revoke_in_transaction(self.db,self.gate,'root_revoked'); self.db.commit()
  self.assertEqual(result['state'],'failed_or_unknown')
  self.db.execute('BEGIN IMMEDIATE')
  try:
   with self.assertRaises(b.Refused): b.pulse_file_in_transaction(*args,started['operation_id'],self.gate)
   with self.assertRaises(b.Refused): b.start_file_in_transaction(*args,self.gate)
  finally: self.db.rollback()
 def test_new_unread_input_blocks_authorization(self):
  self.db.execute('INSERT INTO messages(id,sender_session,recipient_session,body,created_at) VALUES(?,?,?,?,?)',('message_new_input',s.UNITY_PO,s.CONSUMER,'fixture input',s.TASK_STARTED+3))
  with self.assertRaises(b.Refused): self.issue()
 def test_stop_control_blocks_authorization(self):
  self.db.execute('CREATE TABLE agent_controls(target_session TEXT PRIMARY KEY,mode TEXT)')
  self.db.execute('INSERT INTO agent_controls VALUES(?,?)',(s.CONSUMER,'stopped'))
  with self.assertRaises(CoordError): self.issue()
 def test_task_body_changed_fails(self):
  self.db.execute('UPDATE messages SET body=? WHERE id=?',('changed',s.TASK))
  with self.assertRaises(s.Refused): self.issue()
 def test_expiry_fails_and_status_marks_expired(self):
  self.issue()
  with patch('agent_chat.hospital_vendor_binding.time.time',return_value=s.TASK_PRODUCTIVE_UNTIL):
   self.db.execute('BEGIN IMMEDIATE'); status=b.root_status_in_transaction(self.db,self.gate); self.db.commit()
   self.assertEqual(status['state'],'expired')
   with self.assertRaises(CoordError): self.coord.request(s.CONTROL,60,registry_db=self.gate)
 def test_guard_switch_fails(self):
  self.issue(); control=self.coord.request(s.CONTROL,60,registry_db=self.gate); key=next(iter(self.manifest))
  args=(self.db,s.CONSUMER,s.HOST,control['reservation_id'],control['token'],key,'run-fixture','output-run-fixture','/private/tmp/output','/private/tmp/output/file')
  self.db.execute('BEGIN IMMEDIATE'); started=b.start_file_in_transaction(*args,self.gate); self.db.commit()
  changed=list(args); changed[6]='other-run'
  self.db.execute('BEGIN IMMEDIATE')
  try:
   with self.assertRaises(b.Refused): b.pulse_file_in_transaction(*changed,started['operation_id'],self.gate)
  finally: self.db.rollback()
 def test_changed_task_cohort_owner_and_host_during_copy_fail(self):
  self.issue(); control=self.coord.request(s.CONTROL,60,registry_db=self.gate); key=next(iter(self.manifest))
  args=(self.db,s.CONSUMER,s.HOST,control['reservation_id'],control['token'],key,'run-fixture','out-run','/private/tmp/output','/private/tmp/output/file')
  self.db.execute('BEGIN IMMEDIATE'); started=b.start_file_in_transaction(*args,self.gate); self.db.commit()
  cases=(('task',"UPDATE messages SET body='changed' WHERE id=?",s.TASK),
         ('cohort',"UPDATE resources SET reservation_id='changed' WHERE name=?",key),
         ('owner',"UPDATE resources SET owner_session='changed' WHERE name=?",s.CONTROL))
  for label,sql,value in cases:
   with self.subTest(label=label):
    self.db.execute('BEGIN IMMEDIATE')
    try:
     self.db.execute(sql,(value,))
     with self.assertRaises((b.Refused,s.Refused,CoordError)): b.pulse_file_in_transaction(*args,started['operation_id'],self.gate)
    finally: self.db.rollback()
  changed=list(args); changed[2]='host_wrong'
  self.db.execute('BEGIN IMMEDIATE')
  try:
   with self.assertRaises(b.Refused): b.pulse_file_in_transaction(*changed,started['operation_id'],self.gate)
  finally: self.db.rollback()
  with patch('agent_chat.hospital_vendor_binding.time.time',return_value=s.TASK_PRODUCTIVE_UNTIL):
   self.db.execute('BEGIN IMMEDIATE')
   try:
    with self.assertRaises(b.Refused): b.pulse_file_in_transaction(*args,started['operation_id'],self.gate)
   finally: self.db.rollback()

class RootActionTests(BindingTests):
 def test_root_action_real_transactions_with_fixture_provider(self):
  self.reg.rollback()
  master=Path(self.tmp.name)/'project.sqlite3'; registry=Path(str(master)+'.projects.sqlite3')
  target=sqlite3.connect(registry); self.reg.backup(target); target.close()
  config=SimpleNamespace(master=master,uid=os.getuid(),plist=Path(self.tmp.name)/'fixture.plist')
  with patch('agent_chat.service._installed_server_config',return_value=config), patch('agent_chat.service.configured_server_database',return_value=master):
   self.assertEqual(b.root_action(master,'issue')['state'],'pending')
   self.assertEqual(b.root_action(master,'status')['state'],'pending')
   self.assertEqual(b.root_action(master,'revoke')['state'],'revoked')

class ServerFlowTests(BindingTests):
 def prepare_server(self):
  self.issue(); self.reg.rollback()
  registry=Path(self.tmp.name)/'registry.sqlite3'; target=sqlite3.connect(registry)
  self.reg.backup(target); target.close()
  master=Path(self.tmp.name)/'project.sqlite3'
  class Roots:
   projects=SimpleNamespace(database=master,path=registry)
   def path(self,project):
    if project!='default': raise ValueError('wrong project')
    return master
  self.roots=Roots()
  config=SimpleNamespace(master=master,uid=os.getuid())
  patcher=patch('agent_chat.service._installed_server_config',return_value=config); patcher.start(); self.addCleanup(patcher.stop)
 def call(self,op,params):
  return remote_service.dispatch(self.coord,{'op':op,'session':s.CONSUMER,'host_id':s.HOST,'params':params},canonical_project='default',roots=self.roots)
 def test_real_dispatch_fixed_scope_and_guard(self):
  self.prepare_server()
  control=self.call('request',{'resource':s.CONTROL,'minutes':60})
  run=self.call('request',{'resource':s.RUN_RESOURCE,'minutes':60})
  outroot=str(Path(self.tmp.name)/'output'); Path(outroot).mkdir(mode=0o700)
  out=self.call('request',{'resource':outroot,'minutes':60})
  with self.assertRaises(CoordError): self.call('begin-guard',{'resource':s.CONTROL,'token':control['token'],'reservation_id':control['reservation_id']})
  tuples=[dict(resource=x['resource'],reservation_id=x['reservation_id'],session_id=s.CONSUMER,host_id=s.HOST,token=x['token']) for x in (run,out)]
  key=next(iter(self.manifest))
  begin={'tuples':tuples,'resource':key,'control_rid':control['reservation_id'],'control_token':control['token'],'output_path':outroot+'/copy'}
  with self.assertRaises(CoordError): self.call('begin-guard-batch',{'tuples':tuples,'restore':False})
  with self.assertRaises(CoordError): self.call('hospital-vendor-begin',dict(begin,control_token='forged'))
  forged_tuples=[dict(item) for item in tuples]; forged_tuples[1]['token']='forged'
  with self.assertRaises(CoordError): self.call('hospital-vendor-begin',dict(begin,tuples=forged_tuples))
  with self.assertRaises(CoordError): self.call('hospital-vendor-begin',dict(begin,output_path=outroot+'/../escape'))
  self.assertEqual(self.db.execute('SELECT COUNT(*) FROM remote_guarded_runs').fetchone()[0],0)
  guards=self.call('hospital-vendor-begin',begin)['admissions']
  for guard,entry in zip(guards,tuples): self.call('attach-guard-pid',{'run_id':guard['run_id'],'token':entry['token'],'pid':12345})
  params=dict(resource=key,control_rid=control['reservation_id'],control_token=control['token'],run_id=guards[0]['run_id'],run_token=run['token'],output_resource=outroot,output_rid=out['reservation_id'],output_token=out['token'],output_run_id=guards[1]['run_id'],output_path=outroot+'/copy')
  before=self.db.execute('SELECT name,owner_session,reservation_id,stale,deadline FROM resources WHERE owner_session=? ORDER BY name',(s.OWNER,)).fetchall()
  with self.assertRaises(CoordError): self.call('hospital-vendor-start',dict(params,control_token='forged'))
  with self.assertRaises(CoordError): self.call('hospital-vendor-start',dict(params,output_path=outroot+'/../escape'))
  self.assertEqual(self.db.execute('SELECT COUNT(*) FROM hospital_vendor_file_events').fetchone()[0],0)
  start=self.call('hospital-vendor-start',params); params['operation_id']=start['operation_id']
  self.assertEqual(self.call('hospital-vendor-pulse',params)['state'],'active')
  old=self.manifest[key][0]; self.db.execute('UPDATE resources SET reservation_id=? WHERE name=?',('reservation_changed',key))
  with self.assertRaises(CoordError): self.call('hospital-vendor-pulse',params)
  self.db.execute('UPDATE resources SET reservation_id=? WHERE name=?',(old,key))
  result=self.call('hospital-vendor-finish',dict(params,digest='a'*64,byte_count=10)); self.assertEqual(result['state'],'unverified')
  self.assertEqual(before,self.db.execute('SELECT name,owner_session,reservation_id,stale,deadline FROM resources WHERE owner_session=? ORDER BY name',(s.OWNER,)).fetchall())
  with self.assertRaises(CoordError): self.call('hospital-vendor-start',{k:v for k,v in params.items() if k!='operation_id'})
if __name__=='__main__': unittest.main(verbosity=2)
