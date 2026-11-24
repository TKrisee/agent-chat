"""Private goal verifier contracts; all process/socket/native evidence is synthetic."""
import contextlib, hashlib, os, plistlib, shlex, sqlite3, tempfile, time, unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
from agent_chat import service, capacity_observer, native_owner_evidence as evidence
from agent_chat import goal_local_provider, goal_peer_read, goal_registry_read
from agent_chat.client_continuity import ClientContinuity

class SyntheticKernel:
    sample=evidence.DarwinEvidence.sample
    sample_independent_listener=evidence.DarwinEvidence.sample_independent_listener
    def __init__(self, fixture): self.f=fixture
    def process(self,pid):
        if pid==self.f.client.pid: return self.f.client
        if pid==self.f.provider.pid: return self.f.provider
        raise evidence.EvidenceError('unknown synthetic PID')
    def lsof(self,*args):
        f=self.f;ep='127.0.0.1:34971'
        if '-sTCP:LISTEN' in args:return ((f.provider.pid,'9',ep),)
        pid=int(args[args.index('-p')+1])
        if pid==f.client.pid:return ((pid,'6','127.0.0.1:31001->'+ep),)
        if pid==f.provider.pid:return ((pid,'12',ep+'->127.0.0.1:31001'),(pid,'13',ep+'->127.0.0.1:31002'))
        if pid==os.getpid():return ((pid,'8','127.0.0.1:31002->'+ep),)
        raise AssertionError('unexpected synthetic socket lookup')

class SyntheticRpc:
    def __init__(self): self.calls=[]
    def __enter__(self): return self
    def __exit__(self,*args): pass
    def connection_identity(self):return dict(client_instance='a'*32,connection_epoch=1,pid=os.getpid(),fd=8,local=('127.0.0.1',31002),peer=('127.0.0.1',34971))
    def request(self,method,params):
        self.calls.append((method,params))
        if method!='thread/goal/get' or params!={'threadId':'private-po-thread'}:raise AssertionError('wrong native read')
        return {'goal':{'threadId':'private-po-thread','status':'active'}}

class GoalConnectOnly(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='goal-connect-only-');self.addCleanup(self.tmp.cleanup)
        self.base=Path(self.tmp.name);self.host=goal_peer_read.SELECTED_HOST
        self.registry=self.base/'registry.sqlite3';self.project=self.base/'project.sqlite3'
        self.image=self.base/'fixture-codex';self.image.write_bytes(b'inert synthetic image; never executed')
        self.python=self.base/'fixture-python';self.python.write_bytes(b'inert synthetic launcher; never executed')
        self.client_bin=self.base/'fixture-client';self.client_bin.write_text('inert')
        self.endpoint='ws://127.0.0.1:34971';self.now=time.time()
        self.client=evidence.Process(44300,1,44300,os.getuid(),os.getuid(),int(self.now)-20,0,str(self.python))
        self.provider=evidence.Process(35700,1,35700,os.getuid(),os.getuid(),int(self.now)-200,0,str(self.image))
        self.command=[str(self.python),str(self.client_bin),'bridge','--codex-server',self.endpoint,'--connect-only']
        self.manager=service.ServiceManager(system='Darwin',home=self.base,python=str(self.python),server_bin=str(self.client_bin),client_bin=str(self.client_bin),run=self.run_command)
        paths=self.manager.paths('client');self.paths=paths
        paths['wrapper'].parent.mkdir(parents=True);paths['config'].parent.mkdir(parents=True)
        paths['wrapper'].write_text('exec '+shlex.join(self.command)+'\n')
        paths['config'].write_bytes(plistlib.dumps({'Label':service.LABEL+'.client','ProgramArguments':[str(paths['wrapper'])],'KeepAlive':True}))
        with contextlib.closing(sqlite3.connect(self.registry)) as db:
            db.executescript("CREATE TABLE root_config(singleton,home_project,home_session,thread_id,host_id,mode);CREATE TABLE root_directory_owner(singleton,home_project,home_session);CREATE TABLE root_projects(project_id,session_id,orchestrator_session,joined);")
            db.execute("INSERT INTO root_config VALUES(1,'default','private-root','private-root-thread',?,'running')",(self.host,));db.execute("INSERT INTO root_directory_owner VALUES(1,'default','private-root')");db.execute("INSERT INTO root_projects VALUES('private-project','private-projection','private-po',1)");db.commit()
        with contextlib.closing(sqlite3.connect(self.project)) as db:
            db.executescript("CREATE TABLE meta(key,value);CREATE TABLE sessions(id);CREATE TABLE project_orchestrators(session_id,root_session);CREATE TABLE root_projections(session_id,home_project,home_session,thread_id);CREATE TABLE bridge_bindings(session_id,thread_id,parent_session,updated_at);CREATE TABLE remote_session_hosts(session_id,host_id);CREATE TABLE bridge_dispatcher(host_id,owner,acquired_at);CREATE TABLE bridge_client_runtime(host_id,server,pid,heartbeat,error);")
            db.execute("INSERT INTO meta VALUES('project_id','private-project')");db.execute("INSERT INTO sessions VALUES('private-po')");db.execute("INSERT INTO project_orchestrators VALUES('private-po','private-projection')");db.execute("INSERT INTO root_projections VALUES('private-projection','default','private-root','private-root-thread')");db.execute("INSERT INTO bridge_bindings VALUES('private-po','private-po-thread',NULL,1)");db.execute("INSERT INTO remote_session_hosts VALUES('private-po',?)",(self.host,));db.execute("INSERT INTO bridge_dispatcher VALUES(?,'private-dispatcher',1)",(self.host,));db.execute("INSERT INTO bridge_client_runtime VALUES(?,?,?,?,NULL)",(self.host,self.endpoint,self.client.pid,self.now));db.commit()
        self.projects=SimpleNamespace(path=str(self.registry),get=lambda project:{'id':project},db_path=lambda project:str(self.project))
        self.rpc=SyntheticRpc();self.kernel=SyntheticKernel(self);self.stack=contextlib.ExitStack();self.addCleanup(self.stack.close)
        config=SimpleNamespace(uid=os.getuid(),executable=self.client_bin)
        for patch in (mock.patch.object(service,'_installed_server_config',return_value=config),mock.patch.object(service,'ServiceManager',return_value=self.manager),mock.patch.object(capacity_observer.pwd,'getpwuid',return_value=SimpleNamespace(pw_dir=str(self.base))),mock.patch.object(capacity_observer.subprocess,'run',side_effect=self.run_command),mock.patch.object(evidence,'DarwinEvidence',return_value=self.kernel),mock.patch.object(evidence,'IMAGE',self.image),mock.patch.object(evidence,'IMAGE_SHA',hashlib.sha256(self.image.read_bytes()).hexdigest()),mock.patch.object(evidence,'dynamic_disk_association',return_value='f'*40),mock.patch.object(ClientContinuity,'_kernel_command',side_effect=lambda pid:(str(self.python),list(self.command))),mock.patch.object(goal_local_provider,'RpcClient',return_value=self.rpc)):
            entered=self.stack.enter_context(patch)
            if patch.attribute == 'RpcClient': self.rpc_factory=entered
    def run_command(self,argv,**kwargs):
        if argv[0] not in ('/bin/launchctl','launchctl') or argv[1]!='print':raise AssertionError('unexpected process call '+repr(argv))
        return SimpleNamespace(returncode=0,stderr='',stdout='path = '+str(self.paths['config'])+'\nprogram = '+str(self.paths['wrapper'])+'\npid = '+str(self.client.pid)+'\nruns = 1\n')
    def read(self):return goal_local_provider.read_project_goal(self.projects,'private-project')
    def test_declared_connect_only_existing_listener_goal_is_read_under_positive_proof(self):
        result=self.read()
        self.assertEqual(result.get('status'),'active',result)
        self.assertEqual(self.rpc.calls,[('thread/goal/get',{'threadId':'private-po-thread'})])
        self.assertGreater(result['observed_at'],0)


    def assert_denied(self, reason='peer_verification_unavailable'):
        self.assertEqual(self.read(), {'status':'unavailable','reason':reason})
        self.assertEqual(self.rpc.calls, [])
        self.rpc_factory.assert_not_called()

    def registration(self):
        return dict(endpoint=self.endpoint,pid=self.client.pid,heartbeat=self.now)

    def write_command(self):
        self.paths['wrapper'].write_text('exec '+shlex.join(self.command)+'\n')

    def test_owned_goal_mode_and_capacity_keep_direct_child_contract(self):
        self.command.remove('--connect-only');self.write_command()
        self.provider=replace(self.provider,parent=self.client.pid)
        self.assertEqual(self.read()['status'],'active')
        selected=capacity_observer._selected_client(self.registration(),self.kernel)
        self.assertEqual(selected[2].chain,(self.provider,self.client))

    def test_owned_mode_does_not_fallback_for_independent_listener(self):
        self.command.remove('--connect-only');self.write_command()
        self.assert_denied()

    def test_capacity_does_not_adopt_goal_connect_only_exception(self):
        with self.assertRaisesRegex(evidence.EvidenceError,'outside registered bridge ancestry'):
            capacity_observer._selected_client(self.registration(),self.kernel)
        self.assertEqual(self.rpc.calls,[])

    def test_registered_client_pid_mismatch_denied_before_rpc(self):
        with contextlib.closing(sqlite3.connect(self.project)) as db:
            db.execute('UPDATE bridge_client_runtime SET pid=44301');db.commit()
        self.assert_denied()

    def test_wrong_provider_pid_from_socket_selection_denied(self):
        original=self.kernel.lsof
        def rows(*args):
            if '-sTCP:LISTEN' in args:return ((35701,'9','127.0.0.1:34971'),)
            return original(*args)
        with mock.patch.object(self.kernel,'lsof',side_effect=rows):self.assert_denied()

    def test_provider_uid_and_real_uid_must_match(self):
        original=self.provider
        for field in ('uid','ruid'):
            with self.subTest(field=field):
                self.provider=replace(original,**{field:os.getuid()+1});self.assert_denied()

    def test_client_uid_and_stale_heartbeat_denied(self):
        original=self.client
        self.client=replace(original,uid=os.getuid()+1);self.assert_denied()
        self.client=original
        with contextlib.closing(sqlite3.connect(self.project)) as db:
            db.execute('UPDATE bridge_client_runtime SET heartbeat=?',(self.now-100,));db.commit()
        self.assert_denied('selection_unavailable')

    def test_provider_image_path_unavailable_does_not_fallback(self):
        original=self.kernel.process
        def process(pid):
            if pid==self.provider.pid:raise evidence.EvidenceError('image path unavailable')
            return original(pid)
        with mock.patch.object(self.kernel,'process',side_effect=process):self.assert_denied()

    def test_provider_wrong_image_and_untrusted_digest_denied(self):
        original=self.provider
        self.provider=replace(original,image=str(self.base/'wrong-image'));self.assert_denied()
        self.provider=original
        with mock.patch.object(evidence,'IMAGE_SHA','0'*64):self.assert_denied()

    def test_dynamic_image_association_failure_denied(self):
        with mock.patch.object(evidence,'dynamic_disk_association',side_effect=evidence.EvidenceError('unverified code')):
            self.assert_denied()

    def test_provider_birth_change_during_sample_denied(self):
        original=self.kernel.process;count=0
        def process(pid):
            nonlocal count
            if pid==self.provider.pid:
                count+=1
                if count>1:return replace(self.provider,birth_usec=1)
            return original(pid)
        with mock.patch.object(self.kernel,'process',side_effect=process):self.assert_denied()

    def test_wrong_managed_socket_peer_and_ambiguous_listener_denied(self):
        original=self.kernel.lsof
        def wrong(*args):
            if '-p' in args and args[args.index('-p')+1]==str(self.client.pid):
                return ((self.client.pid,'6','127.0.0.1:31001->127.0.0.1:34972'),)
            return original(*args)
        with mock.patch.object(self.kernel,'lsof',side_effect=wrong):self.assert_denied()
        def ambiguous(*args):
            rows=original(*args)
            return rows+rows if '-sTCP:LISTEN' in args else rows
        with mock.patch.object(self.kernel,'lsof',side_effect=ambiguous):self.assert_denied()

    def test_provider_reverse_socket_must_exist(self):
        original=self.kernel.lsof
        def rows(*args):
            if '-p' in args and args[args.index('-p')+1]==str(self.provider.pid):return ()
            return original(*args)
        with mock.patch.object(self.kernel,'lsof',side_effect=rows):self.assert_denied()

    def test_connect_only_requires_exact_configured_argv_and_keepalive(self):
        with mock.patch.object(ClientContinuity,'_kernel_command',return_value=(str(self.python),self.command[:-1])):
            self.assert_denied()
        obj=plistlib.loads(self.paths['config'].read_bytes());obj['KeepAlive']=False
        self.paths['config'].write_bytes(plistlib.dumps(obj));self.assert_denied()

    def test_declared_endpoint_and_duplicate_mode_cannot_select_another_peer(self):
        index=self.command.index('--codex-server')+1
        self.command[index]='ws://127.0.0.1:34972';self.write_command();self.assert_denied()
        self.command[index]=self.endpoint;self.command.append('--connect-only');self.write_command();self.assert_denied()

    def test_current_role_and_host_are_required_before_rpc(self):
        with contextlib.closing(sqlite3.connect(self.project)) as db:
            db.execute("UPDATE project_orchestrators SET root_session='wrong-root'");db.commit()
        self.assert_denied('selection_unavailable')
        with contextlib.closing(sqlite3.connect(self.project)) as db:
            db.execute("UPDATE project_orchestrators SET root_session='private-projection'")
            db.execute("UPDATE remote_session_hosts SET host_id='wrong-host'");db.commit()
        self.assert_denied('selection_unavailable')

    def test_role_change_during_read_prevents_publication(self):
        original=self.rpc.request
        def request(method,params):
            response=original(method,params)
            with contextlib.closing(sqlite3.connect(self.project)) as db:
                db.execute("UPDATE project_orchestrators SET root_session='new-root'");db.commit()
            return response
        with mock.patch.object(self.rpc,'request',side_effect=request):
            self.assertEqual(self.read(),{'status':'unavailable','reason':'selection_revalidation_unavailable'})
        self.assertEqual(len(self.rpc.calls),1)

    def test_provider_generation_change_after_read_prevents_publication(self):
        original=self.rpc.request
        def request(method,params):
            response=original(method,params);self.provider=replace(self.provider,birth_usec=1);return response
        with mock.patch.object(self.rpc,'request',side_effect=request):
            self.assertEqual(self.read(),{'status':'unavailable','reason':'native_observation_unavailable'})
        self.assertEqual(len(self.rpc.calls),1)

    def test_goal_rpc_socket_cannot_borrow_managed_client_socket(self):
        identity=self.rpc.connection_identity();identity['fd']=999
        with mock.patch.object(self.rpc,'connection_identity',return_value=identity):
            self.assertEqual(self.read(),{'status':'unavailable','reason':'native_observation_unavailable'})
        self.assertEqual(self.rpc.calls,[])

if __name__=='__main__':unittest.main()
