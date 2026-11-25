"""Private journal tests. Kernel/listener probes are explicit recorded fixtures."""
import tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from agent_chat import provider_owner as owner
class OwnershipJournal(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
  self.path=Path(self.temp.name)/'owner.sqlite3'
 def test_durable_intent_cannot_launch_twice_or_be_closed_without_child(self):
  with owner.LaunchLedger(self.path,'pin','ws://127.0.0.1:59999') as ledger:
   with patch.object(owner,'assert_listener_absent'),patch.object(owner,'identity',return_value=(101,501,10,20)):
    serial=ledger.claim()
    self.assertEqual(serial,1)
    with self.assertRaisesRegex(owner.Blocked,'unknown|unclosed'):ledger.claim()
    with self.assertRaisesRegex(owner.Blocked,'retained child'):ledger.closed(None)
  with owner.LaunchLedger(self.path,'pin','ws://127.0.0.1:59999') as ledger:
   with self.assertRaisesRegex(owner.Blocked,'unknown|unclosed'):ledger.claim()
 def test_conflicting_manifest_and_listener_fail_before_claim(self):
  with owner.LaunchLedger(self.path,'pin','ws://127.0.0.1:59999') as ledger:
   with patch.object(owner,'assert_listener_absent',side_effect=owner.Blocked('listener remains')):
    with self.assertRaisesRegex(owner.Blocked,'listener remains'):ledger.claim()
   self.assertEqual(ledger.rows(),[])
  with self.assertRaisesRegex(owner.Blocked,'configuration'):owner.LaunchLedger(self.path,'other','ws://127.0.0.1:59999')

 def test_authenticated_rollover_preserves_rows_and_rejects_old_owner(self):
  address='ws://127.0.0.1:59999'
  receipt={'ImageSHA256':'image','BaselineSHA256':'baseline','Requirement':'requirement',
           'Image':'/private/image','Endpoint':address,'ManifestSHA256':'new','PreviousManifestSHA256':'old'}
  admission={'old_pin':'old','new_pin':'new','receipt':receipt}
  with owner.LaunchLedger(self.path,'old',address) as old:
   with patch.object(owner,'assert_listener_absent'),patch.object(owner,'identity',return_value=(101,501,10,20)):
    old.claim()
    with self.assertRaises(owner.Blocked):old.rollover_manifest('old','new',admission)
    old.db.execute("UPDATE child_launches SET state='closed' WHERE serial=1");old.db.commit()
    with patch.object(owner,'absent',return_value=True):old.rollover_manifest('old','new',admission)
    self.assertEqual([row[1] for row in old.rows()],['closed'])
    with self.assertRaises(owner.Blocked):old.claim()
  with owner.LaunchLedger(self.path,'new',address) as current:
   self.assertEqual(current.db.execute('SELECT old_pin,new_pin FROM provider_pin_transitions').fetchone(),('old','new'))
  with owner.LaunchLedger(self.path,'new',address,require_admitted_transition=True): pass
  fresh=self.path.parent/'fresh.sqlite3'
  with self.assertRaisesRegex(owner.Blocked,'existing matching ledger transition'):
   owner.LaunchLedger(fresh,'new',address,require_admitted_transition=True)

 def test_restart_reconnects_only_an_attested_surviving_provider(self):
  address='ws://127.0.0.1:59999';image='/private/provider'
  parent=(101,501,10,20);child=(102,501,11,21)
  native=SimpleNamespace(parent=1,image=image)
  listener=SimpleNamespace(pid=102,uid=501,birth_sec=11,birth_usec=21)
  with patch.object(owner,'manifest',return_value=({'Image':image},{},self.temp.name)):
   provider=owner.OwnedProvider(self.path.parent/'manifest.plist','pin',address)
   self.addCleanup(provider.close)
   with provider.ledger.db:
    provider.ledger.db.execute('INSERT INTO child_launches VALUES(?,?,?,?,?,?,?,?,?,?,?)',
     (1,'started',*parent,*child,12))
    provider.ledger.db.execute('INSERT INTO child_launch_attestations VALUES(?,?,?,?,?,?,?)',
     (1,'pin',address,image,'image-sha','signed-code','kernel-hash'))
   with (patch.object(owner,'absent',side_effect=lambda candidate:candidate==parent),
         patch.object(owner,'identity',return_value=child),
         patch.object(owner,'installed_image_sha',return_value='image-sha'),
         patch.object(owner.native,'DarwinEvidence',return_value=SimpleNamespace(process=lambda pid:native)),
         patch.object(owner.native,'_kernel_code_hash',return_value='kernel-hash'),
         patch('agent_chat.provider_service.selected_listener',return_value=listener),
         patch.object(owner.subprocess,'Popen') as launch):
    surviving=provider.start({},None)
    self.assertEqual(surviving.generation,child)
    self.assertIsNone(surviving.poll())
    self.assertEqual(provider.ledger.rows()[0][1],'started')
    launch.assert_not_called()
    listener.pid=103
    with self.assertRaisesRegex(owner.Blocked,'listener differs'):provider.start({},None)
    launch.assert_not_called()

class RealChildCleanup(unittest.TestCase):
 def test_actual_retained_child_cleanup_allows_exactly_one_successor(self):
  import os,socket,subprocess,time,sys
  with tempfile.TemporaryDirectory(prefix='owned-provider-',dir=Path(__file__).parent) as directory:
   root=Path(directory);ready=root/'ready';program=root/'provider.py'
   program.write_text('import socket,time,sys\ns=socket.socket();s.bind(("127.0.0.1",int(sys.argv[1])));s.listen();open(sys.argv[2],"w").write(str(s.getsockname()[1]));time.sleep(60)\n')
   with socket.socket() as reservation:reservation.bind(('127.0.0.1',0));port=reservation.getsockname()[1]
   self.assertGreaterEqual(port,49152);self.assertNotIn(port,(4500,35717,8765))
   address='ws://127.0.0.1:'+str(port)
   config={'ProgramArguments':[sys.executable,str(program),str(port),str(ready)]}
   env={'HOME':str(root),'PATH':'/usr/bin:/bin','TMPDIR':str(root)}
   handles=[]
   try:
    with patch.object(owner,'manifest',return_value=(config,env,str(root))):
     first=owner.OwnedProvider(root/'manifest.plist','fixture-only',address);handles.append(first)
     child=first.start({},subprocess.DEVNULL)
     deadline=time.monotonic()+5
     while not ready.exists() and time.monotonic()<deadline:time.sleep(.02)
     self.assertTrue(ready.exists());old=owner.identity(child.pid)
     with owner.LaunchLedger(root/'owned-provider.sqlite3','fixture-only',address) as contender:
      with self.assertRaisesRegex(owner.Blocked,'unknown|unclosed'):contender.claim()
     first.close();handles.remove(first)
     self.assertIsNotNone(child.poll());self.assertTrue(owner.absent(old));owner.assert_listener_absent(address)
     ready.unlink();second=owner.OwnedProvider(root/'manifest.plist','fixture-only',address);handles.append(second)
     next_child=second.start({},subprocess.DEVNULL)
     deadline=time.monotonic()+5
     while not ready.exists() and time.monotonic()<deadline:time.sleep(.02)
     self.assertTrue(ready.exists());self.assertNotEqual(owner.identity(next_child.pid),old)
     second.close();handles.remove(second)
     with owner.LaunchLedger(root/'owned-provider.sqlite3','fixture-only',address) as ledger:
      self.assertEqual([row[1] for row in ledger.rows()],['closed','closed'])
   finally:
    for handle in handles:
     try:handle.close()
     except Exception:
      if handle.child is not None and handle.child.poll() is None:handle.child.terminate();handle.child.wait(timeout=5)


class LegacySelection(unittest.TestCase):
 def process(self,pid,parent,group):
  from types import SimpleNamespace
  return SimpleNamespace(pid=pid,parent=parent,group=group,uid=501,ruid=501,birth_sec=10,birth_usec=pid)
 def test_native_node_launcher_chain_requires_actual_client_and_one_group(self):
  rows={101:self.process(101,1,101),102:self.process(102,101,102),103:self.process(103,102,102)}
  def generation(pid):return pid,501,10,pid
  with patch.object(owner.native.DarwinEvidence,'process',side_effect=lambda pid:rows[pid]),patch.object(owner,'identity',side_effect=generation):
   self.assertEqual(owner.legacy_chain(generation(103),generation(101)),(102,generation(102)))
   rows[103].group=999
   with self.assertRaisesRegex(owner.Blocked,'group'):owner.legacy_chain(generation(103),generation(101))
   rows[103].group=102;rows[102].parent=1
   with self.assertRaisesRegex(owner.Blocked,'ancestry'):owner.legacy_chain(generation(103),generation(101))
 def test_adopted_legacy_is_never_closed_by_claim_and_reconcile_requires_actual_absence(self):
  with tempfile.TemporaryDirectory() as directory:
   with owner.LaunchLedger(Path(directory)/'ledger.sqlite3','pin','ws://127.0.0.1:59999') as ledger:
    with patch.object(owner,'legacy_chain',return_value=(102,(102,501,10,102))):owner.adopt_legacy(ledger,(103,501,10,103),(101,501,10,101))
    with self.assertRaisesRegex(owner.Blocked,'unknown|unclosed'):ledger.claim()
    with patch.object(owner,'absent',return_value=False):
     with self.assertRaisesRegex(owner.Blocked,'still exists'):ledger.reconcile()
    self.assertEqual(ledger.rows()[0][1],'selected_legacy')
    with patch.object(owner,'absent',return_value=True),patch.object(owner,'assert_listener_absent',side_effect=owner.Blocked('listener remains')):
     with self.assertRaisesRegex(owner.Blocked,'listener remains'):ledger.reconcile()
    with patch.object(owner,'absent',return_value=True),patch.object(owner,'assert_listener_absent'),patch.object(owner.os,'killpg',side_effect=ProcessLookupError):
     self.assertTrue(ledger.reconcile().startswith('closed:'))
    self.assertEqual(ledger.rows()[0][1],'closed')
    self.assertIsNotNone(ledger.db.execute('SELECT closed_at FROM legacy_adoptions').fetchone()[0])
