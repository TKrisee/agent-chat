"""One real private launchd fixture; no production labels/store/listener/input."""
import contextlib,hashlib,io,os,plistlib,re,signal,socket,sqlite3,subprocess,sys,tempfile,time,unittest,uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from agent_chat import service,native_owner_evidence as native,client_continuity
from agent_chat import provider_owner as owner,provider_service as controller
from agent_chat.provider_recovery_wire import Wire,jq,field
import handoff_fixture
BASE=Path('/private/tmp/provider-owning-handoff-v3').resolve()
@unittest.skipUnless(os.environ.get('AGENT_CHAT_PRIVATE_LAUNCHD_TEST')=='1','explicit private launchd qualification only')
class ManagedLaunchd(unittest.TestCase):
 LEGACY_OWNING=False
 ATTEMPTS=4
 REATTACH=False
 def test_external_handoff_and_normal_all_restart(self):
  directory=tempfile.TemporaryDirectory(prefix='launchd-',dir=BASE/'validation/tests');root=Path(directory.name).resolve()
  self.assertTrue(root.is_relative_to(BASE/'validation/tests'))
  label='com.agent-chat.private-test.'+uuid.uuid4().hex
  self.assertNotEqual(label,'com.agent-chat');self.assertIn('.private-test.',label)
  jobs=[];external=None;root_actor=None;old_manager=service.ServiceManager;old_config=service.INSTALLED_SERVER_CONFIG;old_label=service.LABEL;old_c_label=client_continuity.LABEL
  import pwd
  original_pwd=pwd.getpwuid
  from agent_chat import provider_recovery_lifecycle as life
  old_cli,old_cli_sha=life.CLI_IMAGE,life.CLI_SHA
  try:
   artifact=native.IMAGE;self.assertEqual(hashlib.sha256(artifact.read_bytes()).hexdigest(),native.IMAGE_SHA)
   subprocess.run(['/usr/bin/codesign','--verify','--strict','--verbose=2',str(artifact)],capture_output=True,check=True)
   with socket.socket() as reserved:reserved.bind(('127.0.0.1',0));port=reserved.getsockname()[1]
   self.assertGreaterEqual(port,49152);self.assertNotIn(port,(4500,35717,8765));address='ws://127.0.0.1:'+str(port)
   (root/'fixture.plist').write_bytes(plistlib.dumps(dict(label=label,python=sys.executable)))
   for name in ('server','server-wrapper'):(root/name).write_text('inert private fixture\n')
   (root/'server-wrapper').write_text('exec '+str(root/'server')+' --db '+str(root/'master.sqlite3')+'\n')
   (root/'server.plist').write_bytes(plistlib.dumps(dict(Label=label+'.server',AssociatedBundleIdentifiers=[label+'.server'],WorkingDirectory=str(root),RunAtLoad=True,KeepAlive=True,ThrottleInterval=10,ProgramArguments=[str(root/'server-wrapper')],EnvironmentVariables={})))
   thread=str(uuid.uuid4())
   with contextlib.closing(sqlite3.connect(root/'master.sqlite3')) as db:
    db.executescript('CREATE TABLE bridge_bindings(session_id,thread_id,parent_session,updated_at);CREATE TABLE remote_session_hosts(session_id,host_id);CREATE TABLE messages(id,sender_session,recipient_session,body);CREATE TABLE operator_identities(session_id);CREATE TABLE operator_message_origins(message_id,sender_session,channel);CREATE TABLE bridge_jobs(id,thread_id,status);CREATE TABLE bridge_client_runtime(host_id,server,pid,heartbeat,error);')
    db.execute('INSERT INTO bridge_bindings VALUES(?,?,NULL,1)',('root',thread));db.execute("INSERT INTO remote_session_hosts VALUES('root','private-host')");db.execute("INSERT INTO messages VALUES('source','operator','root','Scoped synthetic private provider handoff and restart')");db.execute("INSERT INTO operator_identities VALUES('operator')");db.execute("INSERT INTO operator_message_origins VALUES('source','operator','authenticated_operator_ui')");db.execute('INSERT INTO bridge_client_runtime VALUES(?,?,0,0,NULL)',('private-host',address));db.execute('INSERT INTO bridge_jobs VALUES(?,?,?)',('uncertain-fixture',thread,'uncertain'));db.commit()
   with contextlib.closing(sqlite3.connect(str(root/'master.sqlite3')+'.projects.sqlite3')) as db:
    db.executescript('CREATE TABLE root_config(singleton,home_project,home_session,host_id,thread_id);CREATE TABLE root_directory_owner(singleton,home_project,home_session);');db.execute('INSERT INTO root_config VALUES(1,?,?,?,?)',('default','root','private-host',thread));db.execute("INSERT INTO root_directory_owner VALUES(1,'default','root')");db.commit()
   subprocess.run(['/usr/bin/cc',str(BASE/'overlay/tests/handoff_actor.c'),'-o',str(root/'codex')],capture_output=True,check=True)
   subprocess.run(['/usr/bin/codesign','--force','--sign','-',str(root/'codex')],capture_output=True,check=True)
   nativehome=root/'native';nativehome.mkdir();(nativehome/'config.toml').write_text('cli_auth_credentials_store="file"\nmcp_oauth_credentials_store="file"\n[analytics]\nenabled=false\n[feedback]\nenabled=false\n')
   rollouts=nativehome/'sessions/2026/11/15';rollouts.mkdir(parents=True)
   rollout=rollouts/('rollout-2026-11-15T00-00-00-'+thread+'.jsonl')
   rollout.write_text(jq('{}','{timestamp:"2026-11-15T00:00:00Z",type:"session_meta",payload:{session_id:$id,id:$id,timestamp:"2026-11-15T00:00:00Z",cwd:$cwd,originator:"synthetic-private-launchd-test",cli_version:"0.169.0",source:"cli",model_provider:"openai",history_mode:"legacy"}}',id=thread,cwd=str(root))+'\n')
   env=dict(HOME=str(root),CODEX_HOME=str(nativehome),CODEX_SQLITE_HOME=str(nativehome),TMPDIR=str(root),PATH='/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin',LANG='en_US.UTF-8')
   argv=[str(artifact),'app-server','--listen',address,'-c','sqlite_home='+jq('{}','$path',path=str(nativehome))]
   log=(root/'native.log').open('w');external=subprocess.Popen(argv,env=env,cwd=root,stdin=subprocess.DEVNULL,stdout=log,stderr=log)
   def wire():
    deadline=time.monotonic()+15
    while True:
     try:return Wire(('127.0.0.1',port),deadline)
     except ConnectionRefusedError:
      if time.monotonic()>=deadline:raise
      time.sleep(.05)
   w=wire();w.initialize();initial=w.request('thread/resume',jq('{}','{threadId:$id,path:$path,excludeTurns:true}',id=thread,path=str(rollout)));w.close();self.assertEqual(field(initial,'.thread.id'),thread)
   manifest=root/'launch.plist';manifest.write_bytes(plistlib.dumps(dict(ProgramArguments=argv,Image=str(artifact),ImageSHA256=native.IMAGE_SHA,Endpoint=address,WorkingDirectory=str(root),EnvironmentVariables=env,CodexConfigSHA256=hashlib.sha256((nativehome/'config.toml').read_bytes()).hexdigest(),Artifacts={})))
   pin=hashlib.sha256(manifest.read_bytes()).hexdigest()
   root_actor=subprocess.Popen([str(root/'codex'),'resume','--remote',address,thread],stdin=subprocess.PIPE,env=env)
   fixture_module=str(BASE/'validation/tests')
   (root/'client').write_text('import sys\nsys.path.insert(0,'+repr(str(BASE/'validation/src'))+')\nsys.path.insert(0,'+repr(fixture_module)+')\nimport handoff_fixture\nraise SystemExit(handoff_fixture.client('+repr(str(root))+',sys.argv[2:]))\n')
   bindir=BASE/'validation/bin';bindir.mkdir(exist_ok=True)
   (bindir/'agent-chat-service').write_text('import sys\nsys.path.insert(0,'+repr(str(BASE/'validation/src'))+')\nsys.path.insert(0,'+repr(fixture_module)+')\nimport handoff_fixture\nhandoff_fixture.configure('+repr(str(root))+')\nfrom agent_chat.service import main\nraise SystemExit(main())\n')
   manager_factory,data=handoff_fixture.configure(root);manager=manager_factory()
   paths=manager.paths('client');paths['wrapper'].parent.mkdir(parents=True);paths['config'].parent.mkdir(parents=True);paths['logs'].mkdir(parents=True)
   token=root/'fixture-token';token.write_text('synthetic-private-transport-token')
   args=SimpleNamespace(db=str(root/'master.sqlite3'),token_file=str(token),project_root=str(root),state_dir=str(root/'bridge-state'),server='http://127.0.0.1:59998',codex_server=address,codex_bin=str(artifact),project=None,connect_only=not self.LEGACY_OWNING)
   paths['wrapper'].write_text(manager._wrapper('client',args));paths['wrapper'].chmod(0o700)
   definition=plistlib.loads(manager.render('client',args).encode())
   if self.LEGACY_OWNING:
    definition['EnvironmentVariables'].update(env)
    external.terminate();external.wait(timeout=5)
   paths['config'].write_bytes(plistlib.dumps(definition))
   # This is the ONLY persistent test job; both path and label are private.
   target='gui/'+str(os.getuid())+'/'+label+'.client';jobs.append(target)
   subprocess.run(['launchctl','bootstrap','gui/'+str(os.getuid()),str(paths['config'])],capture_output=True,check=True)
   def ready(deadline):
    while time.monotonic()<deadline:
     with contextlib.closing(sqlite3.connect(root/'master.sqlite3')) as db:row=db.execute('SELECT pid,heartbeat FROM bridge_client_runtime').fetchone()
     if row[0] and time.time()-row[1]<2:return row[0]
     time.sleep(.1)
    raise AssertionError('private bridge failed to connect; '+paths['logs'].joinpath('client.err.log').read_text())
   old_client=ready(time.monotonic()+15)
   if self.LEGACY_OWNING:
    native_rows=native.DarwinEvidence.lsof('-iTCP@127.0.0.1:'+str(port),'-sTCP:LISTEN');self.assertEqual(len(native_rows),1);old_provider=owner.identity(native_rows[0][0])
    self.assertEqual(native.DarwinEvidence().process(old_provider[0]).parent,old_client)
   else:old_provider=owner.identity(external.pid)
   for attempt in range(self.ATTEMPTS):
    if attempt==2:(root/'inject-gate-crash').write_text('one controlled private crash before gate receipt')
    request=str(uuid.uuid4()) if attempt==0 and not self.LEGACY_OWNING else None
    if request:jobs.append('gui/'+str(os.getuid())+'/'+label+'.handoff.'+request)
    command=['restart','--component','all']
    if attempt==0 and not self.LEGACY_OWNING:command+=['--request-id',request]+['--provider-manifest',str(manifest),'--provider-manifest-sha256',pin,'--provider-source-id','source']
    # Server-only restart is a recorded fixture: there is no real HTTP service.
    # The transient worker, client and native provider lifecycle are real.
    if self.REATTACH and attempt==0:
     import threading
     (root/'inject-reattach').write_text('controlled exit after actual durable resume response')
     original_cli=owner.identity(root_actor.pid)
     def exit_after_receipt():
      limit=time.monotonic()+90
      while not (root/'resume-receipt-ready').exists():
       if time.monotonic()>=limit:return
       time.sleep(.02)
      root_actor.stdin.close();root_actor.wait(timeout=5)
     exiting=threading.Thread(target=exit_after_receipt);exiting.start()
    output=io.StringIO()
    with contextlib.redirect_stdout(output):code=service.main(command)
    actual_request=field(output.getvalue(),'.request_id')
    if request:self.assertEqual(actual_request,request)
    else:
     request=actual_request;jobs.append('gui/'+str(os.getuid())+'/'+label+'.handoff.'+request)
    if self.REATTACH and attempt==0:
     exiting.join(timeout=6);self.assertFalse(exiting.is_alive());self.assertTrue(owner.absent(original_cli))
     self.assertEqual(code,2,output.getvalue());self.assertEqual(field(output.getvalue(),'.state'),'outcome_unknown')
     (root/'inject-reattach').unlink()
     with contextlib.closing(sqlite3.connect(root/'provider-recovery-v3/requests.sqlite3')) as db:
      self.assertEqual(db.execute('SELECT count(*) FROM authenticated_resume_receipts').fetchone()[0],1)
     root_actor=subprocess.Popen([str(root/'codex'),'--remote',address,'resume',thread],stdin=subprocess.PIPE,env=env)
     limit=time.monotonic()+5
     while not [r for r in native.DarwinEvidence.lsof('-iTCP@127.0.0.1:'+str(port),'-sTCP:ESTABLISHED') if r[0]==root_actor.pid]:
      if time.monotonic()>=limit:raise AssertionError('private reattached CLI has no actual socket')
      time.sleep(.02)
     # Controller must be gone before another process can complete its request.
     with contextlib.closing(sqlite3.connect(root/'provider-owning/service-requests.sqlite3')) as db:
      worker=db.execute('SELECT worker_pid,uid,worker_sec,worker_usec FROM service_requests WHERE id=?',(request,)).fetchone()
     limit=time.monotonic()+5
     while not owner.absent(worker):
      if time.monotonic()>=limit:raise AssertionError('private original controller did not exit')
      time.sleep(.02)
     before=(root/'rpc-methods.log').read_text().splitlines().count('thread/resume')
     reconciled=io.StringIO()
     with contextlib.redirect_stdout(reconciled):self.assertEqual(service.main(['provider-handoff-reconcile','--request-id',request]),0)
     self.assertEqual(field(reconciled.getvalue(),'.state'),'completed',reconciled.getvalue())
     self.assertEqual((root/'rpc-methods.log').read_text().splitlines().count('thread/resume'),before)
     with contextlib.closing(sqlite3.connect(root/'provider-recovery-v3/requests.sqlite3')) as db:
      proof=db.execute('SELECT evidence FROM readonly_restoration_proofs').fetchone()[0]
      self.assertEqual(field(proof,'.scope'),'service_lifecycle_only');self.assertEqual(field(proof,'.full_current_settings_attested|tostring'),'false')
      self.assertEqual(db.execute('SELECT original_exited FROM cli_reattachment_observations').fetchone()[0],1)
     self.assertEqual(len((root/'recorded-server-restarts.txt').read_text().splitlines()),1)
     again=io.StringIO()
     with contextlib.redirect_stdout(again):self.assertEqual(service.main(['provider-handoff-reconcile','--request-id',request]),0)
     self.assertEqual(len((root/'recorded-server-restarts.txt').read_text().splitlines()),1)
     self.assertEqual((root/'rpc-methods.log').read_text().splitlines().count('thread/resume'),before)
     (BASE/'evidence/reattachment-result.json').write_text(reconciled.getvalue())
    elif attempt==2:
     self.assertEqual(code,2,output.getvalue());self.assertEqual(field(output.getvalue(),'.state'),'outcome_unknown')
     self.assertTrue((root/'crashed-gate-pid.txt').exists())
     reconciled=io.StringIO()
     with contextlib.redirect_stdout(reconciled):self.assertEqual(service.main(['provider-handoff-reconcile','--request-id',request]),0)
     self.assertEqual(field(reconciled.getvalue(),'.state'),'completed',reconciled.getvalue())
     (BASE/'evidence/launchd-crash-reconciliation.json').write_text(reconciled.getvalue())
    else:self.assertEqual(code,0,output.getvalue())
    deadline=time.monotonic()+60
    while time.monotonic()<deadline:
     raw=controller.status(request);phase=field(raw,'.state')
     if phase in ('completed','outcome_unknown','server_restart_intent'):break
     time.sleep(.1)
    (BASE/'evidence'/('launchd-result-'+str(attempt)+'.json')).write_text(raw+'\n')
    if phase=='server_restart_intent':
     # No test server is loaded, so the final server command must fail rather
     # than pretending HTTP restart happened. Provider restoration already passed.
     self.assertIn('restored_known_local_state',field(raw,'.detail'))
    else:self.assertEqual(phase,'completed',raw)
    new_client=ready(time.monotonic()+15);self.assertNotEqual(new_client,old_client)
    rows=native.DarwinEvidence.lsof('-iTCP@127.0.0.1:'+str(port),'-sTCP:LISTEN');self.assertEqual(len(rows),1)
    new=owner.identity(rows[0][0]);self.assertNotEqual(new,old_provider);self.assertTrue(owner.absent(old_provider));self.assertEqual(native.DarwinEvidence().process(new[0]).parent,new_client)
    w=wire();peer=SimpleNamespace(connection_identity=lambda:dict(local=w.sock.getsockname(),peer=w.sock.getpeername()))
    proof=native.DarwinEvidence().sample_independent_listener(address,new[0],new[2:],peer,native.IMAGE_SHA);w.initialize();read=w.request('thread/read',jq('{}','{threadId:$id,includeTurns:false}',id=thread));w.close();self.assertEqual(field(read,'.thread.id'),thread)
    with contextlib.closing(sqlite3.connect(root/'master.sqlite3')) as db:self.assertEqual(db.execute('SELECT status FROM bridge_jobs').fetchone()[0],'uncertain')
    # Same request returns durable result, with no new submit or child.
    output=io.StringIO()
    replay=command if attempt==0 and not self.LEGACY_OWNING else command+['--request-id',request]
    with contextlib.redirect_stdout(output):self.assertEqual(service.main(replay),0)
    self.assertEqual(owner.identity(new[0]),new)
    old_client,old_provider=new_client,new
    if phase=='server_restart_intent':break
   print('PRIVATE_LAUNCHD_OFFICIAL_OWNING_PROVIDER_PASS',label,flush=True)
  finally:
   for job in reversed(jobs):
    answer=subprocess.run(['launchctl','bootout',job],capture_output=True,text=True)
    print('PRIVATE_JOB_CLEANUP',job,answer.returncode,answer.stderr.strip(),flush=True)
   if root_actor is not None:
    root_actor.stdin.close();root_actor.wait(timeout=5)
   if external is not None:
    if external.poll() is None:external.terminate()
    external.wait(timeout=5)
   if 'log' in locals():log.close()
   # Actual managed bridge cleanup must finish; no hidden provider is accepted.
   if 'address' in locals():
    deadline=time.monotonic()+12
    while True:
     try:owner.assert_listener_absent(address);break
     except owner.Blocked:
      if time.monotonic()>=deadline:raise
      time.sleep(.1)
   service.ServiceManager=old_manager;service.INSTALLED_SERVER_CONFIG=old_config;service.LABEL=old_label;client_continuity.LABEL=old_c_label;pwd.getpwuid=original_pwd;life.CLI_IMAGE=old_cli;life.CLI_SHA=old_cli_sha
   # Keep bounded logs/plists for review; no production bytes copied.
   print('PRIVATE_FIXTURE_EVIDENCE',root,flush=True)
   directory._finalizer.detach()

class LegacyManagedLaunchd(ManagedLaunchd):
 LEGACY_OWNING=True
 ATTEMPTS=2

class ReattachedManagedLaunchd(ManagedLaunchd):
 REATTACH=True
 ATTEMPTS=2
