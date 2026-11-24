"""Service outcome contract after owning-provider handoff replaces notice-only all."""
import contextlib,io,tempfile,subprocess,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from agent_chat import service,provider_service
from agent_chat.provider_recovery_wire import jq,field
class RestartNotice(unittest.TestCase):
 def invoke(self,component='all',fail=False):
  with tempfile.TemporaryDirectory() as directory:
   calls=[]
   def run(command,**kwargs):
    calls.append(command)
    if fail:raise subprocess.CalledProcessError(1,command)
   manager=service.ServiceManager(system='Darwin',home=Path(directory).resolve(),server_bin=str(Path(directory)/'server'),client_bin=str(Path(directory)/'client'),run=run)
   out=io.StringIO();err=io.StringIO()
   with patch.object(service,'ServiceManager',return_value=manager),patch.object(provider_service,'home_directory',return_value=Path(directory).resolve()/'handoff'),contextlib.redirect_stdout(out),contextlib.redirect_stderr(err):
    code=service.main(['restart','--component',component])
   return code,out.getvalue(),err.getvalue(),calls
 def test_all_without_first_handoff_fails_before_any_service_effect(self):
  code,out,error,calls=self.invoke();self.assertEqual(code,2);self.assertEqual(out,'');self.assertIn('client.plist',error);self.assertEqual(calls,[])
 def test_server_only_restart_does_not_require_provider_handoff(self):
  code,out,error,calls=self.invoke('server');self.assertEqual(code,0);self.assertEqual(out,'');self.assertEqual(error,'');self.assertEqual(len(calls),1);self.assertTrue(calls[0][-1].endswith('.server'))
 def test_failed_server_restart_keeps_stdout_clean(self):
  code,out,error,calls=self.invoke('server',True);self.assertEqual(code,2);self.assertEqual(out,'');self.assertIn('non-zero exit status',error);self.assertEqual(len(calls),1)
 def test_unknown_provider_outcome_is_nonzero_not_silent_success(self):
  raw=jq('{}','{request_id:"private",state:"outcome_unknown",ready:false}')
  with patch.object(provider_service,'submit',return_value=raw),patch.object(provider_service,'wait',return_value=raw):
   code,out,error,calls=self.invoke()
  self.assertEqual(code,2);self.assertEqual(field(out,'.state'),'outcome_unknown');self.assertEqual(error,'');self.assertEqual(calls,[])

class LegacyAuthority(unittest.TestCase):
 def test_local_service_principal_is_distinct_and_original_operator_guard_remains(self):
  import sqlite3,os
  from agent_chat import provider_recovery_lifecycle as life
  with tempfile.TemporaryDirectory() as directory:
   root=Path(directory).resolve();master=root/'master.sqlite3'
   paths=[root/name for name in ('server.plist','wrapper','server')]
   for path in paths:path.write_text('private recorded installed service artifact')
   config=SimpleNamespace(uid=os.getuid(),plist=paths[0],wrapper=paths[1],executable=paths[2],master=master)
   with sqlite3.connect(master) as db:
    db.executescript('CREATE TABLE bridge_bindings(session_id,thread_id,parent_session);CREATE TABLE remote_session_hosts(session_id,host_id);CREATE TABLE messages(id,sender_session,recipient_session,body);CREATE TABLE operator_identities(session_id);CREATE TABLE operator_message_origins(message_id,sender_session,channel);CREATE TABLE bridge_jobs(id,thread_id,status);')
    db.execute("INSERT INTO bridge_bindings VALUES('root','thread',NULL)");db.execute("INSERT INTO remote_session_hosts VALUES('root','host')");db.execute("INSERT INTO bridge_jobs VALUES('unknown','thread','uncertain')")
   with sqlite3.connect(str(master)+'.projects.sqlite3') as db:
    db.executescript('CREATE TABLE root_config(singleton,home_project,home_session,host_id,thread_id);CREATE TABLE root_directory_owner(singleton,home_project,home_session);')
    db.execute("INSERT INTO root_config VALUES(1,'default','root','host','thread')");db.execute("INSERT INTO root_directory_owner VALUES(1,'default','root')")
   with patch.object(life.v2.service,'_installed_server_config',return_value=config),patch.object(life.v2.service,'configured_server_database',return_value=master):
    with self.assertRaisesRegex(life.Blocked,'authenticated Root operator'):life.AdministrativeAuthority(provider_service.LOCAL_SOURCE)
    authority=provider_service.authority_for_handoff(provider_service.LOCAL_SOURCE)
    self.assertEqual(authority.source_kind,'configured_local_service_owner');self.assertEqual(authority.unknown,(('bridge_jobs','unknown','uncertain'),));authority.check()
    with patch.object(life.v2,'execution_epoch_native_check',side_effect=life.Blocked('actual epoch fence')):
     with self.assertRaisesRegex(life.Blocked,'actual epoch fence'):provider_service.authority_for_handoff(provider_service.LOCAL_SOURCE)
    with sqlite3.connect(str(master)+'.projects.sqlite3') as db:db.execute("UPDATE root_directory_owner SET home_session='other'")
    with self.assertRaisesRegex(life.Blocked,'canonical Root owner'):provider_service.authority_for_handoff(provider_service.LOCAL_SOURCE)
