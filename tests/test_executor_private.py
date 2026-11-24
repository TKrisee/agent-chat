"""Private synthetic pulse only; NOT an auth or CLI integration test."""
import os, unittest
from unittest.mock import patch
from agent_chat import hospital_vendor_exec as e, hospital_vendor_scope as s, hospital_vendor_output as o
from test_composed_fixture import ComposedFixture
class ExecutorTests(ComposedFixture):
 def copy(self,output=None,pulse=None): return e._copy_one(str(self.project),self.key,str(output or (self.out/'copy')),str(self.out),pulse or self.gate)
 def test_real_selected_fixture_readonly(self):
  before=self.db.execute('SELECT * FROM resources ORDER BY name').fetchall(); self.copy()
  self.assertEqual((self.out/'copy').read_bytes(),b'fixture payload'); self.assertEqual(self.source.read_bytes(),b'fixture payload')
  self.assertEqual(before,self.db.execute('SELECT * FROM resources ORDER BY name').fetchall())
 def test_inside_output_rejected_before_create(self):
  with self.assertRaises(o.Refused): self.copy(self.project/'copy')
  self.assertFalse((self.project/'copy').exists())
 def test_unselected_source_refused(self):
  with self.assertRaises(s.Refused): e._copy_one(str(self.project),'file:Assets/not-selected',str(self.out/'copy'),str(self.out),self.gate)
 def test_unreadable_source_fails_closed(self):
  self.source.chmod(0)
  with self.assertRaises(PermissionError): self.copy()
  self.assertFalse((self.out/'copy').exists())
 def test_revoked_before_commit_cleans_temp(self):
  count=[]
  def pulse():
   count.append(1); self.gate()
   if len(count)==3: raise s.Refused('synthetic revoked')
  with self.assertRaises(s.Refused): self.copy(pulse=pulse)
  self.assertEqual(list(self.out.iterdir()),[])
 def test_source_changed_before_commit_cleans_temp(self):
  count=[]
  def pulse():
   count.append(1); self.gate()
   if len(count)==3: self.source.write_bytes(b'new synthetic bytes')
  with self.assertRaises(s.Refused): self.copy(pulse=pulse)
  self.assertEqual(list(self.out.iterdir()),[])
 def test_output_parent_replaced_no_project_write(self):
  count=[]; moved=self.base/'moved'
  def pulse():
   count.append(1); self.gate()
   if len(count)==3:
    self.out.rename(moved); self.out.symlink_to(self.project,target_is_directory=True)
  with self.assertRaises(o.Refused): self.copy(pulse=pulse)
  self.assertFalse((self.project/'copy').exists()); self.assertEqual(list(moved.iterdir()),[])
 def test_existing_output_not_overwritten(self):
  (self.out/'copy').write_bytes(b'keep')
  with self.assertRaises(FileExistsError): self.copy()
  self.assertEqual((self.out/'copy').read_bytes(),b'keep')
 def test_existing_temp_not_removed_on_collision(self):
  collision=self.out/('.hospital-vendor-'+'0'*32); collision.write_bytes(b'keep')
  with patch('agent_chat.hospital_vendor_exec.secrets.token_hex',return_value='0'*32):
   with self.assertRaises(FileExistsError): self.copy()
  self.assertEqual(collision.read_bytes(),b'keep')
if __name__=='__main__': unittest.main(verbosity=2)
