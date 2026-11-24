"""Recorded libproc failure; no process, listener, service or RPC effects."""
import ctypes,os,unittest
from types import SimpleNamespace
from unittest.mock import Mock,patch
from agent_chat import provider_service as service,native_owner_evidence as native
class KernelFixture:
 def __init__(self):self.calls=0;self.path_calls=0;self.uid=os.getuid();self.ruid=self.uid;self.births=[(10,20)];self.actual_pid=1234
 def proc_pidinfo(self,pid,flavor,arg,buffer,size):
  self.calls+=1
  b=ctypes.cast(buffer,ctypes.POINTER(native.Bsd)).contents
  b.pid=self.actual_pid;b.ppid=1;b.pgid=pid;b.uid=self.uid;b.ruid=self.ruid
  b.start_sec,b.start_usec=self.births[min(self.calls-1,len(self.births)-1)]
  return ctypes.sizeof(native.Bsd)
 def proc_pidpath(self,*unused):self.path_calls+=1;return 0
class SelectedOldIdentity(unittest.TestCase):
 def setUp(self):
  self.kernel=KernelFixture();fixture=self.kernel
  self.patch=patch.object(native.DarwinEvidence,'__init__',lambda target:setattr(target,'lib',fixture));self.patch.start();self.addCleanup(self.patch.stop)
  self.rows=[(1234,'7','127.0.0.1:59999')]
 def test_unreadable_old_image_uses_exact_kernel_generation_and_listener(self):
  with patch.object(native.DarwinEvidence,'lsof',return_value=self.rows) as lsof:
   selected=service.selected_listener('ws://127.0.0.1:59999')
  self.assertEqual((selected.uid,selected.pid,selected.birth_sec,selected.birth_usec),(os.getuid(),1234,10,20))
  self.assertEqual((selected.image_path,selected.image_sha256),('/UNKNOWN','0'*64))
  self.assertEqual(self.kernel.path_calls,0);self.assertGreaterEqual(self.kernel.calls,3);self.assertEqual(lsof.call_count,2)
 def test_real_uid_pid_birth_and_unique_listener_guards_remain(self):
  with patch.object(native.DarwinEvidence,'lsof',return_value=self.rows):
   self.kernel.ruid=os.getuid()+1
   with self.assertRaises(service.Blocked):service.selected_listener('ws://127.0.0.1:59999')
   self.kernel.ruid=os.getuid();self.kernel.uid=os.getuid()+1
   with self.assertRaises(service.Blocked):service.selected_listener('ws://127.0.0.1:59999')
   self.kernel.uid=os.getuid();self.kernel.actual_pid=4321
   with self.assertRaises(service.Blocked):service.selected_listener('ws://127.0.0.1:59999')
   self.kernel.actual_pid=1234;self.kernel.calls=0;self.kernel.births=[(10,20),(10,20),(10,21)]
   with self.assertRaisesRegex(service.Blocked,'generation changed'):service.selected_listener('ws://127.0.0.1:59999')
  self.kernel.births=[(10,20)]
  with patch.object(native.DarwinEvidence,'lsof',side_effect=[self.rows,self.rows+[(2222,'8','127.0.0.1:59999')]]):
   with self.assertRaisesRegex(service.Blocked,'ambiguous'):service.selected_listener('ws://127.0.0.1:59999')
 def test_unverified_replacement_still_fails_existing_full_peer_guard(self):
  rpc=SimpleNamespace(connection_identity=lambda:dict(local=('127.0.0.1',59998),peer=('127.0.0.1',59999)))
  with patch.object(native.DarwinEvidence,'lsof',return_value=self.rows),patch.object(native,'dynamic_disk_association') as signing:
   with self.assertRaisesRegex(native.EvidenceError,'image path unavailable'):
    native.DarwinEvidence().sample_independent_listener('ws://127.0.0.1:59999',1234,(10,20),rpc,native.IMAGE_SHA)
   signing.assert_not_called()
  self.assertEqual(self.kernel.path_calls,1)
