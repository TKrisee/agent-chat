"""Private force effect tests; recorded admission is not live Root authority."""
import contextlib,os,signal,socket,subprocess,sys,tempfile,time,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock,patch
from agent_chat import provider_force_continuation as force,provider_recovery_peer as peer,provider_service as service
class ForceRecord(unittest.TestCase):
 def test_durable_claim_never_repeats_unknown_signal(self):
  with tempfile.TemporaryDirectory() as directory,contextlib.closing(peer.v1.Journal(Path(directory)/'journal.sqlite3')) as journal:
   with journal.db:journal.db.execute("INSERT INTO requests(id,state,reason,effects) VALUES('request','stop_intent','TimeoutError: process/listener closure unproven','stop_unknown')")
   selected=peer.old_selection(os.getuid(),1234,10,20,'ws://127.0.0.1:59999')
   adapter=SimpleNamespace(selected=selected,owning=False,selection=Mock(),authority=SimpleNamespace(source_id='source',source_digest='source-sha',unknown=(('input','uncertain'),)),baseline_sha='baseline',manifest_sha256='manifest',actor_proof=('recorded private authority',),deadline=time.monotonic()+10)
   with patch.object(peer,'selected_kernel'),patch.object(force.os,'kill',side_effect=OSError('unknown signal result')) as kill:
    with self.assertRaisesRegex(OSError,'unknown signal'):force.claim_and_force(journal,'request',adapter)
    with self.assertRaisesRegex(force.Blocked,'already|claimed'):force.claim_and_force(journal,'request',adapter)
    kill.assert_called_once_with(1234,signal.SIGKILL)
   row=journal.db.execute('SELECT original_state,original_reason,original_effects FROM force_effects').fetchone()
   self.assertEqual(row,('stop_intent','TimeoutError: process/listener closure unproven','stop_unknown'))
 def test_stale_generation_fails_before_force_claim(self):
  with tempfile.TemporaryDirectory() as directory,contextlib.closing(peer.v1.Journal(Path(directory)/'journal.sqlite3')) as journal:
   with journal.db:journal.db.execute("INSERT INTO requests(id,state,reason,effects) VALUES('request','stop_intent','TimeoutError: process/listener closure unproven','stop_unknown')")
   adapter=SimpleNamespace(selected=peer.old_selection(os.getuid(),1234,10,20,'ws://127.0.0.1:59999'),owning=False,selection=Mock(side_effect=force.Blocked('selected generation changed')),deadline=time.monotonic()+10)
   with patch.object(force.os,'kill') as kill:
    with self.assertRaisesRegex(force.Blocked,'generation changed'):force.claim_and_force(journal,'request',adapter)
    kill.assert_not_called()
class RealResistantChild(unittest.TestCase):
 def test_term_resistant_owned_fixture_force_closure_before_one_successor(self):
  with tempfile.TemporaryDirectory(prefix='force-child-',dir=Path(__file__).parent) as directory:
   root=Path(directory);program=root/'child.py';ready=root/'ready';term=root/'term'
   program.write_text('import signal,socket,sys,time\ndef term(*args):open(sys.argv[3],"a").write("TERM\\n")\nsignal.signal(signal.SIGTERM,term)\ns=socket.socket();s.bind(("127.0.0.1",int(sys.argv[1])));s.listen();open(sys.argv[2],"w").write(str(s.getsockname()[1]));time.sleep(60)\n')
   with socket.socket() as reserved:reserved.bind(('127.0.0.1',0));port=reserved.getsockname()[1]
   self.assertGreaterEqual(port,49152);self.assertNotIn(port,(4500,8765))
   child=subprocess.Popen([sys.executable,str(program),str(port),str(ready),str(term)],env={'HOME':str(root),'TMPDIR':str(root),'PATH':'/usr/bin:/bin'},stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)
   successor=None
   try:
    deadline=time.monotonic()+5
    while not ready.exists():
     if time.monotonic()>=deadline:raise AssertionError('private listener not ready')
     time.sleep(.02)
    pid,uid,ruid,sec,usec=peer.kernel_identity(child.pid);selected=peer.old_selection(uid,pid,sec,usec,'ws://127.0.0.1:'+str(port))
    child.terminate();time.sleep(.1);self.assertIsNone(child.poll());self.assertEqual(term.read_text(),'TERM\n')
    with contextlib.closing(peer.v1.Journal(root/'journal.sqlite3')) as journal:
     with journal.db:journal.db.execute("INSERT INTO requests(id,state,reason,effects) VALUES('request','stop_intent','TimeoutError: process/listener closure unproven','stop_unknown')")
     adapter=SimpleNamespace(selected=selected,owning=False,selection=lambda:peer.selected_kernel(selected),authority=SimpleNamespace(source_id='private-fixture',source_digest='recorded-only',unknown=('uncertain-input',)),baseline_sha='private',manifest_sha256='private',actor_proof=('recorded only; not actual Root',),deadline=time.monotonic()+10)
     with self.assertRaisesRegex(force.Blocked,'closure'):force.claim_start_after_closure(journal,'request',selected)
     force.claim_and_force(journal,'request',adapter)
     child.wait(timeout=5);self.assertEqual(child.returncode,-signal.SIGKILL)
     self.assertEqual(term.read_text(),'TERM\n')
     force.claim_start_after_closure(journal,'request',selected)
     with self.assertRaisesRegex(force.Blocked,'already|claimed'):force.claim_start_after_closure(journal,'request',selected)
     ready.unlink();successor=subprocess.Popen([sys.executable,str(program),str(port),str(ready),str(term)],env={'HOME':str(root),'TMPDIR':str(root),'PATH':'/usr/bin:/bin'},stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)
     deadline=time.monotonic()+5
     while not ready.exists():
      if time.monotonic()>=deadline:raise AssertionError('private successor not ready')
      time.sleep(.02)
     self.assertNotEqual(peer.kernel_identity(successor.pid),(pid,uid,ruid,sec,usec))
   finally:
    for process in (child,successor):
     if process is not None:
      if process.poll() is None:process.kill()
      process.wait(timeout=5)
    from agent_chat.provider_owner import assert_listener_absent
    assert_listener_absent('ws://127.0.0.1:'+str(port))

class RealManagedResistantChild(unittest.TestCase):
 def test_existing_retained_owning_cleanup_forces_resistant_child_within_reserved_budget(self):
  from agent_chat import provider_owner as owner
  with tempfile.TemporaryDirectory(prefix='managed-resistant-',dir=Path(__file__).parent) as directory:
   root=Path(directory);program=root/'resistant.py';ready=root/'ready';terms=root/'terms'
   program.write_text('import signal,socket,sys,time\ndef term(*args):open(sys.argv[3],"a").write("TERM\\n")\nsignal.signal(signal.SIGTERM,term)\ns=socket.socket();s.bind(("127.0.0.1",int(sys.argv[1])));s.listen();open(sys.argv[2],"w").write("ready");time.sleep(60)\n')
   with socket.socket() as reservation:reservation.bind(('127.0.0.1',0));port=reservation.getsockname()[1]
   self.assertGreaterEqual(port,49152);address='ws://127.0.0.1:'+str(port)
   config={'ProgramArguments':[sys.executable,str(program),str(port),str(ready),str(terms)]};env={'HOME':str(root),'TMPDIR':str(root),'PATH':'/usr/bin:/bin'}
   actual=None
   try:
    with patch.object(owner,'manifest',return_value=(config,env,str(root))):actual=owner.OwnedProvider(root/'manifest.plist','recorded-private-config',address)
    with patch.object(owner,'manifest',return_value=(config,env,str(root))):child=actual.start({},subprocess.DEVNULL)
    deadline=time.monotonic()+5
    while not ready.exists():
     if time.monotonic()>=deadline:raise AssertionError('resistant managed child did not bind')
     time.sleep(.02)
    with owner.LaunchLedger(root/'owned-provider.sqlite3','recorded-private-config',address) as contender:
     with self.assertRaisesRegex(owner.Blocked,'unclosed|unknown'):contender.claim()
    started=time.monotonic();actual.close();actual=None
    self.assertLess(time.monotonic()-started,12);self.assertEqual(child.returncode,-signal.SIGKILL);self.assertEqual(terms.read_text(),'TERM\n')
    owner.assert_listener_absent(address)
    with owner.LaunchLedger(root/'owned-provider.sqlite3','recorded-private-config',address) as ledger:
     self.assertEqual(ledger.rows()[0][1],'closed');self.assertEqual(ledger.claim(),2)
     with self.assertRaisesRegex(owner.Blocked,'unclosed|unknown'):ledger.claim()
   finally:
    if actual is not None:
     if actual.child is not None and actual.child.poll() is None:actual.child.kill();actual.child.wait(timeout=5)
     actual.ledger.journal.close()
    owner.assert_listener_absent(address)

class OriginalDeadlineBudget(unittest.TestCase):
 def test_grace_phase_reserves_original_half_without_reset(self):
  from agent_chat import provider_recovery_lifecycle as life
  with patch.object(life.time,'monotonic',return_value=10):self.assertEqual(life.stop_phase_deadline(120,120),40)
  with patch.object(life.time,'monotonic',return_value=55):self.assertEqual(life.stop_phase_deadline(120,120),60)
  with patch.object(life.time,'monotonic',return_value=60):
   with self.assertRaisesRegex(TimeoutError,'original operation budget'):life.stop_phase_deadline(120,120)

class ReusedAndSubmitted(unittest.TestCase):
 def test_actual_selected_kernel_predicate_rejects_reused_birth_before_signal(self):
  with tempfile.TemporaryDirectory() as directory,contextlib.closing(peer.v1.Journal(Path(directory)/'journal.sqlite3')) as journal:
   with journal.db:journal.db.execute("INSERT INTO requests(id,state,reason,effects) VALUES('request','stop_intent','TimeoutError: process/listener closure unproven','stop_unknown')")
   selected=peer.old_selection(os.getuid(),1234,10,20,'ws://127.0.0.1:59999')
   adapter=SimpleNamespace(selected=selected,owning=False,selection=Mock(),deadline=time.monotonic()+10)
   with patch.object(peer,'kernel_identity',return_value=(1234,os.getuid(),os.getuid(),11,20)),patch.object(force.os,'kill') as kill:
    with self.assertRaisesRegex(force.Blocked,'birth differs'):force.claim_and_force(journal,'request',adapter)
    kill.assert_not_called()
   self.assertEqual(journal.db.execute('SELECT count(*) FROM force_effects').fetchone()[0],0)
 def test_repeat_unknown_submission_never_bootstraps_or_revalidates_for_another_effect(self):
  import sqlite3
  db=sqlite3.connect(':memory:');self.addCleanup(db.close)
  db.execute('CREATE TABLE service_requests(id,source,source_sha,manifest,pin,uid,pid,birth_sec,birth_usec,endpoint,restart_server,state,detail,worker_pid,worker_sec,worker_usec)')
  db.execute("INSERT INTO service_requests VALUES('request','source','sha','manifest','pin',501,1234,10,20,'ws://127.0.0.1:59999',1,'outcome_unknown','unknown signal',111,9,1)")
  force._schema(db);db.execute("INSERT INTO force_continuations(request_id,state) VALUES('request','outcome_unknown')");db.commit()
  manager=SimpleNamespace(run=Mock())
  with patch.object(service,'operation_db',return_value=SimpleNamespace(db=db,close=Mock())),patch.object(service,'authority_for_handoff',return_value=SimpleNamespace(source_digest='sha')),patch.object(force,'_prepare') as prepare:
   result=force.submit(manager,'request')
  self.assertIn('outcome_unknown',result);prepare.assert_not_called();manager.run.assert_not_called()
