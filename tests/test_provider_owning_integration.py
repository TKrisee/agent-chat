import os,sqlite3,tempfile,time,unittest
from contextlib import ExitStack,closing
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock,patch
from agent_chat import provider_service as s,provider_recovery_lifecycle as l,provider_recovery_local_persistence as lp

class Integration(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
  self.dbpath=Path(self.temp.name)/'synthetic.sqlite3'
  with closing(sqlite3.connect(self.dbpath)) as db,db:
   db.execute('CREATE TABLE bridge_client_runtime(host_id,server,pid,heartbeat,error)')
   db.execute('CREATE TABLE bridge_bindings(session_id,thread_id,updated_at)')
   db.execute("INSERT INTO bridge_client_runtime VALUES('host','ws://127.0.0.1:4500',201,?,NULL)",(time.time(),))
   db.execute("INSERT INTO bridge_bindings VALUES('root','thread',1)")
  self.jdb=sqlite3.connect(':memory:');self.addCleanup(self.jdb.close)
  self.journal=SimpleNamespace(db=self.jdb,save=Mock())
  self.authority=SimpleNamespace(root=('project','root','host','thread'),master=self.dbpath,source_id='source',source_digest='digest',recorded='{}',recorded_checked_at=1,check=Mock())
  self.app=(202,501,501,int(time.time())-2,0);self.cli=(101,501,501,10,20)
  self.oldcli=(self.cli,('/fake/codex','--remote','ws://127.0.0.1:4500','resume','thread'),'code',('old-socket',),'owner')
  self.cont=SimpleNamespace(target='fake',inspect=Mock(return_value={'process':{'pid':202},'config_sha256':'config','service_runs':2}),_run=Mock(return_value=SimpleNamespace(returncode=0,stderr='',stdout=' path = /fake/config\n program = /fake/wrapper\n')),_kernel_command=Mock(return_value=('/fake/codex',self.oldcli[1])))
  self.monitor=SimpleNamespace(process=Mock(return_value={'pid':101}),matches=Mock(return_value=True),proof=Mock(return_value={'owner_identity':'owner'}))
  self.adapter=object.__new__(s.HandoffRecovery);a=self.adapter
  a.authority=self.authority;a.selected=SimpleNamespace(endpoint='ws://127.0.0.1:4500',pid=999)
  a.deadline=time.monotonic()+5;a.old_client=(201,501,25,35);a.original_root_cli=self.oldcli
  a.child=SimpleNamespace(parent=(202,501,self.app[3],0),poll=Mock(return_value=None))
  a.baseline='{"kind":"local-persistence","known_settings":{},"known_profile_id":null,"known_sandbox":{},"unknown_settings":{}}'
  a.baseline_sha='baseline';a.manifest_sha256='manifest';a.replacement_image='/fake/codex';a.replacement_sha='sha'
  a.proof=(SimpleNamespace(pid=999,uid=501,birth_sec=3,birth_usec=4),'code',('local',),('peer',))
  a.pin_peer=Mock();a.replacement_configuration_proof=Mock();a.read=Mock(side_effect=self.read)
  self.stack=ExitStack();self.addCleanup(self.stack.close)
  def p(obj,name,value):self.stack.enter_context(patch.object(obj,name,value))
  p(l,'Wire',Mock(return_value=SimpleNamespace(initialize=Mock())))
  p(lp,'preserved_file',Mock());p(l,'queue',Mock(return_value='[]'))
  p(s.HandoffActors,'continuity',lambda actor,manager:self.cont)
  p(l.v2.service,'_installed_server_config',Mock(return_value=SimpleNamespace(uid=os.getuid(),executable='/fake/exe')))
  p(l.v2.service,'ServiceManager',Mock(return_value=SimpleNamespace(paths=Mock(return_value={'config':Path('/fake/config'),'wrapper':Path('/fake/wrapper')}))))
  p(l.v2,'readonly',lambda path:sqlite3.connect(path))
  p(l.v2,'kernel_identity',lambda pid:self.app if pid==202 else self.cli)
  p(l.native.DarwinEvidence,'lsof',Mock(return_value=[(202,'a','x->127.0.0.1:4500'),(101,'b','y->127.0.0.1:4500')]))
  p(l,'RootOwnerMonitor',Mock(return_value=self.monitor));p(l,'CLI_IMAGE',Path('/fake/codex'));p(l,'CLI_SHA','digest')
  p(l.v2,'trusted_file',Mock(return_value=b'file'));p(l.v2,'digest',Mock(return_value='digest'))
  p(l.native,'dynamic_disk_association',Mock(return_value='code'));p(s.owner,'absent',Mock(return_value=True))
  p(s,'time',SimpleNamespace(monotonic=time.monotonic,time=time.time,sleep=self.advance))
 def read(self,method,params):
  if method=='thread/resume': return '{"thread":{"id":"thread"},"sandbox":{}}'
  if method=='thread/read': return '{"thread":{"id":"thread","status":{"type":"idle"}}}'
  raise AssertionError('unexpected native request '+method)
 def advance(self,seconds):
  with closing(sqlite3.connect(self.dbpath)) as db,db:db.execute('UPDATE bridge_client_runtime SET pid=202,heartbeat=?',(time.time(),))
 def verify(self):return self.adapter.verify(self.journal,'request',self.adapter.deadline)
 def test_receipt_and_real_actor_transition(self):
  self.verify();self.assertEqual(self.jdb.execute('select count(*) from authenticated_resume_receipts').fetchone()[0],1)
  self.assertEqual([c.args[0] for c in self.adapter.read.call_args_list],['thread/resume','thread/read'])
 def test_wrong_parent_fails_after_receipt(self):
  self.adapter.child.parent=(203,501,self.app[3],0)
  with self.assertRaises(l.Blocked):self.verify()
 def test_stale_registration_without_receipt_fails_without_retry(self):
  with self.assertRaises(l.Blocked):self.adapter.restored_connections()
 def test_failed_receipt_cannot_enter_transition_or_replay(self):
  from agent_chat import provider_reattachment as r
  with patch.object(r,'record_resume',side_effect=l.Blocked('receipt unavailable')):
   with self.assertRaisesRegex(l.Blocked,'receipt unavailable'):self.verify()
  self.assertEqual([c.args[0] for c in self.adapter.read.call_args_list],['thread/resume'])
  self.assertFalse(getattr(self.adapter,'_resume_receipt_recorded',False))
 def test_changed_root_generation_refused(self):
  changed=(101,501,501,11,20)
  with patch.object(l.v2,'kernel_identity',side_effect=lambda pid:self.app if pid==202 else changed):
   with self.assertRaisesRegex(l.Blocked,'Root CLI changed'):self.verify()
  self.assertEqual([c.args[0] for c in self.adapter.read.call_args_list],['thread/resume','thread/read'])
 def test_deadline_after_receipt_does_not_replay(self):
  def expire(seconds):self.adapter.deadline=time.monotonic()-1
  with patch.object(s.time,'sleep',side_effect=expire):
   with self.assertRaises(TimeoutError):self.verify()
  self.assertEqual(self.jdb.execute('select count(*) from authenticated_resume_receipts').fetchone()[0],1)
  self.assertEqual([c.args[0] for c in self.adapter.read.call_args_list],['thread/resume','thread/read'])
 def test_original_deadline_not_extended(self):
  self.adapter.deadline=time.monotonic()-1
  with self.assertRaises(TimeoutError):self.verify()
  self.adapter.read.assert_not_called()
if __name__=='__main__':unittest.main(verbosity=2)
