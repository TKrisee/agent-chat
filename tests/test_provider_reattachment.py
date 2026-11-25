"""Recorded authenticated receipts; real actor/peer qualification is separate."""
import contextlib,sqlite3,tempfile,unittest,time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock,patch
from agent_chat import provider_service as service,provider_reattachment as reattach
from agent_chat.provider_recovery_wire import jq,field
class Receipt(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
  self.db=sqlite3.connect(Path(self.temp.name)/'receipt.sqlite3');self.addCleanup(self.db.close)
  self.journal=SimpleNamespace(db=self.db)
  self.process=SimpleNamespace(pid=101,uid=501,birth_sec=10,birth_usec=20)
  self.adapter=SimpleNamespace(proof=(self.process,'code',('127.0.0.1',50100),('127.0.0.1',50101)),pin_peer=Mock(),baseline_sha='baseline',manifest_sha256='manifest',authority=SimpleNamespace(source_id='source',source_digest='source-pin'))
 def test_absent_receipt_cannot_be_invented_and_response_pins_generation(self):
  with self.assertRaisesRegex(reattach.Blocked,'receipt'):reattach.resume_receipt(self.adapter,self.journal,'request')
  raw=jq('{}','{thread:{id:"thread",preview:"excluded content"},model:"model",sandbox:{type:"readOnly"},approvalPolicy:"on-request"}')
  reattach.record_resume(self.adapter,self.journal,'request',raw)
  actual=reattach.resume_receipt(self.adapter,self.journal,'request')
  self.assertEqual(field(actual,'.model'),'model');self.assertEqual(field(actual,'has("thread")|tostring'),'false')
  self.process.pid=102
  with self.assertRaisesRegex(reattach.Blocked,'generation|binding'):reattach.resume_receipt(self.adapter,self.journal,'request')
 def test_conflicting_receipt_or_missing_current_fields_cannot_complete(self):
  reattach.record_resume(self.adapter,self.journal,'request','{"model":"model"}')
  with self.assertRaisesRegex(reattach.Blocked,'already|conflict'):reattach.record_resume(self.adapter,self.journal,'request','{"model":"other"}')
  baseline='{"known_settings":{"model":"model","approvalPolicy":"on-request"},"known_sandbox":{"type":"readOnly"},"known_profile_id":null}'
  current='{"thread":{"model":"model","cwd":"/private","modelProvider":"openai","reasoningEffort":null}}'
  evidence=reattach.current_settings(baseline,current)
  self.assertEqual(field(evidence,'.current_unobservable_known_fields|join(",")'),'approvalPolicy,sandbox.type')
  with self.assertRaisesRegex(Exception,'differ'):reattach.current_settings(baseline,'{"thread":{"model":"other"}}')
class HttpClaim(unittest.TestCase):
 def test_only_unattempted_http_restart_is_claimed_once_even_if_call_fails(self):
  db=sqlite3.connect(':memory:');self.addCleanup(db.close)
  db.execute('CREATE TABLE service_requests(id TEXT PRIMARY KEY,state TEXT,detail TEXT,restart_server INTEGER)')
  db.execute("INSERT INTO service_requests VALUES('request','provider_restored','',1)");db.commit()
  manager=SimpleNamespace(restart=Mock(side_effect=RuntimeError('unknown HTTP effect')))
  with self.assertRaisesRegex(RuntimeError,'unknown HTTP'):service.complete_server_restart(manager,db,'request')
  self.assertEqual(db.execute('SELECT state FROM service_requests').fetchone()[0],'server_restart_intent')
  self.assertFalse(service.complete_server_restart(manager,db,'request'));manager.restart.assert_called_once_with('server')
 def test_successful_http_once_and_no_http_target_completion(self):
  db=sqlite3.connect(':memory:');self.addCleanup(db.close)
  db.execute('CREATE TABLE service_requests(id TEXT PRIMARY KEY,state TEXT,detail TEXT,restart_server INTEGER)')
  db.execute("INSERT INTO service_requests VALUES('request','provider_restored','',1)");db.commit()
  manager=SimpleNamespace(restart=Mock())
  self.assertTrue(service.complete_server_restart(manager,db,'request'));self.assertFalse(service.complete_server_restart(manager,db,'request'));manager.restart.assert_called_once_with('server')
  self.assertEqual(db.execute('SELECT state FROM service_requests').fetchone()[0],'completed')

class OwnerProof(unittest.TestCase):
 def test_new_owner_requires_original_absence_exact_binding_and_stable_observation(self):
  db=sqlite3.connect(':memory:');self.addCleanup(db.close)
  db.execute('CREATE TABLE checkpoint_bindings(request_id,home_project,root_session,host,thread_id,binding_generation)')
  db.execute("INSERT INTO checkpoint_bindings VALUES('request','default','root','host','thread',1)")
  old=((101,501,501,10,20),('codex','--remote','ws://127.0.0.1:59999','resume','thread'),'code',('socket',),'old-owner')
  new=((102,501,501,11,21),old[1],'code',('new-socket',),'new-owner')
  actual=('app','config',1,'socket',new)
  adapter=SimpleNamespace(original_root_cli=old,authority=SimpleNamespace(root=('default','root','host','thread')))
  actors=SimpleNamespace(generation=1,observe=Mock(return_value=actual));journal=SimpleNamespace(db=db)
  from agent_chat import provider_owner
  with patch.object(provider_owner,'absent',return_value=False):
   with self.assertRaisesRegex(reattach.Blocked,'original CLI'):reattach.record_owner(adapter,journal,'request',actual,actors)
  with patch.object(provider_owner,'absent',return_value=True):
   actors.generation=2
   with self.assertRaisesRegex(reattach.Blocked,'binding'):reattach.record_owner(adapter,journal,'request',actual,actors)
   actors.generation=1;actors.observe.return_value=('changed',)
   with self.assertRaisesRegex(reattach.Blocked,'changed'):reattach.record_owner(adapter,journal,'request',actual,actors)
   actors.observe.return_value=actual;reattach.record_owner(adapter,journal,'request',actual,actors)
   self.assertEqual(db.execute('SELECT original_exited FROM cli_reattachment_observations').fetchone()[0],1)

class UnknownResume(unittest.TestCase):
 def test_receiptless_resume_intent_has_no_continuation_or_http_effect(self):
  with tempfile.TemporaryDirectory() as directory:
   root=Path(directory).resolve();(root/'provider-recovery-v3').mkdir(mode=0o700)
   with contextlib.closing(service.peer.v1.Journal(root/'provider-recovery-v3/requests.sqlite3')) as journal:
    with journal.db:journal.db.execute("INSERT INTO requests(id,state,effects) VALUES('request','resume_intent','resume_unknown')")
   authority=SimpleNamespace(master=root/'master.sqlite3')
   row=('request',None,None,None,None,None,None,None,None,None,1,'outcome_unknown',None,None,None,None)
   manager=SimpleNamespace(restart=Mock())
   with patch.object(service,'HandoffRecovery') as adapter:
    result=service.finish_reconciled_start(manager,row,authority,SimpleNamespace(db=None))
   self.assertIn('UNKNOWN',result);self.assertIn('no resume replay',result);adapter.assert_not_called();manager.restart.assert_not_called()
