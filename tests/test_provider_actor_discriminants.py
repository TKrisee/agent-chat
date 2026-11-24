import sqlite3,time,unittest
from contextlib import closing
from unittest.mock import patch
import test_provider_owning_integration as fixtures
from agent_chat import provider_service as s,provider_recovery_lifecycle as l
class Discriminants(unittest.TestCase):
 read=fixtures.Integration.read
 advance=fixtures.Integration.advance
 def observe(self): return s.HandoffActors(self.authority,self.adapter.selected).observe()
 def change(self,sql,args=()):
  with closing(sqlite3.connect(self.dbpath)) as db,db:db.execute(sql,args)
 def setUp(self):
  fixtures.Integration.setUp(self);self.change('UPDATE bridge_client_runtime SET pid=202,heartbeat=?',(time.time(),))
 def assert_failure(self,label,sql,args=()):
  self.change(sql,args)
  with self.assertRaises(l.Blocked) as error:self.observe()
  self.assertIn(label,str(error.exception));self.assertNotIn('SECRET-SENTINEL',str(error.exception))
 def test_absent_registration(self):self.assert_failure('registration_missing','DELETE FROM bridge_client_runtime')
 def test_endpoint(self):self.assert_failure('endpoint_mismatch',"UPDATE bridge_client_runtime SET server='foreign'")
 def test_pid(self):self.assert_failure('client_pid_mismatch','UPDATE bridge_client_runtime SET pid=404')
 def test_runtime_error_redacted(self):self.assert_failure('runtime_error',"UPDATE bridge_client_runtime SET error='SECRET-SENTINEL'")
 def test_future_heartbeat(self):self.assert_failure('heartbeat_future','UPDATE bridge_client_runtime SET heartbeat=?',(time.time()+30,))
 def test_stale_heartbeat(self):self.assert_failure('heartbeat_stale','UPDATE bridge_client_runtime SET heartbeat=?',(time.time()-60,))
 def test_before_birth(self):self.assert_failure('heartbeat_before_birth','UPDATE bridge_client_runtime SET heartbeat=?',(self.app[3]-1,))
 def test_missing_heartbeat(self):self.assert_failure('heartbeat_unavailable','UPDATE bridge_client_runtime SET heartbeat=NULL')
 def test_missing_binding(self):self.assert_failure('binding_missing','DELETE FROM bridge_bindings')
 def test_foreign_thread(self):self.assert_failure('thread_mismatch',"UPDATE bridge_bindings SET thread_id='foreign'")
 def test_store_unavailable_no_detail_leak(self):
  with patch.object(l.v2,'readonly',side_effect=sqlite3.OperationalError('SECRET-SENTINEL')):
   with self.assertRaises(l.Blocked) as error:self.observe()
  self.assertIn('actor_store_unavailable',str(error.exception));self.assertNotIn('SECRET-SENTINEL',str(error.exception))
if __name__=='__main__':unittest.main(verbosity=2)
