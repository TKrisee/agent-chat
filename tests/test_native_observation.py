"""Real local Root/Coordinator + real WebSocket RPC; OS/provider results controlled."""
import base64
import contextlib
import io
import tempfile
import plistlib
import types
import copy
import hashlib
import importlib.util
import os
from pathlib import Path
import socket
import socketserver
import struct
import subprocess
import sys
import threading
import time
import unittest
import uuid

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('admission_fixture',ROOT/'tests/test_worker_admission_intent.py')
fixture=importlib.util.module_from_spec(spec); spec.loader.exec_module(fixture)
all_pins={}
for line in (Path(os.environ['Q5_ALL_IMPORT_MANIFEST']).read_text().splitlines() if os.environ.get('Q5_ALL_IMPORT_MANIFEST') else ()):
    module,path,pin,mode=line.split('\t'); all_pins[module]=(Path(path),pin,mode)
for name in ('bridge_lease','approvals','bridge','worker_native_observation','native_owner_evidence'):
    fixture.ORIGINS['agent_chat.'+name]=ROOT/'src/agent_chat'/ (name+'.py')
    if fixture.PINS: fixture.PINS['agent_chat.'+name]=all_pins['agent_chat.'+name]
from agent_chat.bridge_lease import BridgeManager
from agent_chat.agent_control import AgentControlState
from agent_chat.bridge_state import BridgeState
from agent_chat.core import Coordinator, CoordError
from agent_chat.rpc import RpcClient, TransportError
from agent_chat import worker_native_observation as candidate
from agent_chat.native_owner_evidence import Process, Sample, EvidenceError, same_owner, lsof_records, DarwinEvidence

class Wire(socketserver.BaseRequestHandler):
    def read(self,n):
        out=b''
        while len(out)<n:
            part=self.request.recv(n-len(out))
            if not part: raise EOFError()
            out+=part
        return out
    def send(self,value):
        data=candidate.canonical(value).encode()
        head=b'\x81'+(bytes([len(data)]) if len(data)<126 else b'\x7e'+struct.pack('!H',len(data)))
        self.request.sendall(head+data)
    def handle(self):
        server=self.server
        try:
            data=b''
            while not data.endswith(b'\r\n\r\n'): data+=self.read(1)
            keys=[line.split(':',1)[1].strip() for line in data.decode().split('\r\n') if line.lower().startswith('sec-websocket-key:')]
            accept=base64.b64encode(hashlib.sha1((keys[0]+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest()).decode()
            self.request.sendall(('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+accept+'\r\n\r\n').encode())
            while True:
                head=self.read(2); opcode=head[0]&15; length=head[1]&127
                if length==126: length=struct.unpack('!H',self.read(2))[0]
                elif length==127: length=struct.unpack('!Q',self.read(8))[0]
                mask=self.read(4) if head[1]&128 else b''
                payload=self.read(length)
                if mask: payload=bytes(v^mask[i%4] for i,v in enumerate(payload))
                if opcode==8: return
                # jq is the sole decoder of incoming protocol JSON.
                q=subprocess.run(['jq','-er','[.id//-1,.method,(.params.threadId//""),((.params|keys|sort)|join(","))]|@tsv'],input=payload,capture_output=True)
                if q.returncode: raise AssertionError(q.stderr.decode())
                rid,method,main,keys=q.stdout.decode().strip('\n').split('\t')
                if method=='initialized': continue
                server.calls.append(method)
                if method=='initialize': value={'serverInfo':{'name':'CONTROLLED TEST PROVIDER','version':'fixture'}}
                else:
                    if main!=server.main: raise AssertionError('wrong main at native boundary')
                    if method=='thread/read':
                        value={'thread':{'id':server.main,'sessionId':server.main,'status':{'type':'idle'},'canAcceptDirectInput':True,
                          **{k:server.settings[k] for k in ('model','modelProvider','reasoningEffort','cwd')},
                          'environments':[{'cwd':server.settings['cwd'],'runtimeWorkspaceRoots':server.settings['runtimeWorkspaceRoots']}]}}
                    elif method=='thread/queue/list': value={'data':[],'nextCursor':None}
                    elif method=='thread/goal/get': value={'goal':copy.deepcopy(server.goal)}
                    elif method=='thread/resume':
                        if keys!='excludeTurns,threadId': raise AssertionError('settings override/extra resume field')
                        server.resumes+=1
                        if server.hook: server.hook()
                        if server.mode=='disconnect': return
                        if server.mode=='partial': self.request.sendall(b'\x81\x7e\x01\x00{'); return
                        value=copy.deepcopy(server.settings); value['thread']={'id':server.main}
                        if server.mode=='wrong_thread': value['thread']['id']=str(uuid.uuid4())
                        if server.mode=='missing': value.pop('sandbox')
                        if server.mode=='malformed': value['sandbox']={'type':'dangerFullAccess'}
                        if server.mode=='model_changed': value['model']='changed-after-preread'
                        if server.mode=='goal_clear': server.goal=None
                        if server.mode=='clear_notification': self.send({'method':'thread/goal/cleared','params':{'threadId':server.main}})
                    else: raise AssertionError('unexpected native method '+method)
                self.send({'id':int(rid),'result':value})
        except (EOFError,ConnectionError,OSError): return
        except BaseException as error: server.errors.append(repr(error))

class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address=True
    daemon_threads=True

class Kernel:
    """Controlled OS result boundary; does NOT establish production kernel proof."""
    def __init__(self,host,endpoint):
        self.host,self.endpoint=host,endpoint; self.count=0; self.replace_at=None; self.fail_at=None
        self.proc=Process(os.getpid(),os.getppid(),os.getpgrp(),os.getuid(),os.getuid(),int(time.time())-1,123,'CONTROLLED TEST IMAGE')
    def sample(self,registration,rpc=None):
        self.count+=1
        if self.fail_at==self.count: raise EvidenceError('controlled missing peer')
        if registration['pid']!=os.getpid() or registration['endpoint']!=self.endpoint: raise EvidenceError('registration mismatch')
        p=self.proc
        if self.replace_at is not None and self.count>=self.replace_at:
            p=Process(p.pid,p.parent,p.group,p.uid,p.ruid,p.birth_sec+1,p.birth_usec,p.image)
        ep=('127.0.0.1',int(self.endpoint.rsplit(':',1)[1]))
        pair=None if rpc is None else (rpc._sock.getsockname(),rpc._sock.getpeername(),'controlled-fd')
        return Sample(ep,p.pid,(p,),'controlled-image-pin','controlled-signing-pin',pair)

class Observation(fixture.Flow):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='q5-observer-')
        self.base=Path(self.temp.name).resolve(); self.master=self.base/'state.sqlite3'
        self.projects=fixture.Projects(self.master); self.host='host-isolated'; self.root_sid='session-root-isolated'; self.connections=[]
        self.neutral=self.base/'neutral'; self.neutral.mkdir()
        self.project_dir=self.base/'project'; self.project_dir.mkdir()
        self.worker_dir=self.base/'existing-worker'; self.worker_dir.mkdir()
        self.root_coord,self.root_thread,_=self.register('default',self.root_sid,'root',self.neutral)
        self.register('default','session-notice-recipient','operator',self.neutral)
        self.root=fixture.RootOrchestrator(self.projects); self.root_coord.context()
        self.root.enroll('default',self.root_sid,self.host,self.root_thread,True)
        self.project=self.projects.create('Isolated observer target',str(self.project_dir),'private observer validation')['id']
        self.po,po_thread,_=self.register(self.project,'session-po','po',self.project_dir)
        self.root_coord.context(); self.root.adopt('default',self.root_sid,self.host,self.project,'session-po',po_thread,str(self.project_dir),True)
        self.worker,self.thread,self.provider=self.register(self.project,'session-worker','worker',self.worker_dir)
        self.root_coord.context(); self.root.delegate_management('default',self.root_sid,self.host,self.project,'session-po','session-worker',self.thread,self.host,str(uuid.uuid4()),True)
        self.config=fixture.service.INSTALLED_SERVER_CONFIG
        plist=self.base/'server.plist'; wrapper=self.base/'server-wrapper'; executable=self.base/'server'
        executable.write_text('# isolated fixture executable\n')
        wrapper.write_text('exec '+str(executable)+' --db '+str(self.master)+' --host 127.0.0.1 --port 9999\n')
        with plist.open('wb') as f:
            plistlib.dump({'Label':'com.agent-chat.server','AssociatedBundleIdentifiers':['com.agent-chat.server'],'WorkingDirectory':str(self.neutral),'RunAtLoad':True,'KeepAlive':True,'ThrottleInterval':10,'ProgramArguments':[str(wrapper)],'EnvironmentVariables':{}},f)
        fixture.service.INSTALLED_SERVER_CONFIG=fixture.service.InstalledServerConfig(plist,wrapper,executable,self.master,self.neutral,os.getuid(),'127.0.0.1',9999)
        self.original_selected=candidate.SELECTED
        candidate.SELECTED=('session-worker',self.thread,self.host)
        self.server=Server(('127.0.0.1',0),Wire)
        self.server.main=self.thread; self.server.calls=[]; self.server.errors=[]; self.server.resumes=0; self.server.hook=None; self.server.mode='normal'
        self.server.settings={'model':'isolated-model','modelProvider':'isolated-provider','reasoningEffort':'high','cwd':str(self.worker_dir),
          'runtimeWorkspaceRoots':[str(self.worker_dir)],'activePermissionProfile':None,'approvalPolicy':'on-request','approvalsReviewer':'user',
          'sandbox':{'type':'workspaceWrite','writableRoots':[str(self.worker_dir)],'networkAccess':False,'excludeSlashTmp':True,'excludeTmpdirEnvVar':True}}
        self.server.goal={'threadId':self.thread,'objective':'SYNTHETIC CONTROLLED PAUSED GOAL','status':'paused','tokensUsed':0,'timeUsedSeconds':0,'createdAt':1,'updatedAt':1}
        self.thread_server=threading.Thread(target=self.server.serve_forever,daemon=True); self.thread_server.start()
        self.endpoint='ws://127.0.0.1:'+str(self.server.server_address[1])
        self.kernel=Kernel(self.host,self.endpoint)
        self.manager=BridgeManager(self.projects.db_path(self.project))
        self.identity={'owner':'isolated-observer-owner','secret':'s'*40,'host_id':self.host}
        self.manager.dispatch(dict(self.identity,op='acquire',params={}))
        self.manager.dispatch(dict(self.identity,op='heartbeat',params={'server':self.endpoint,'pid':os.getpid(),'error':None}))
        self.worker.context()
        control=AgentControlState(self.worker)
        stop=control.request('session-worker',self.thread,'stop',str(uuid.uuid4()),True)
        control.update(stop['id'],'pending','completed',observation={'state':'idle','queue_count':0,'turn_id':None})
        self.p={'project':self.project,'target':'session-worker','expected_thread':self.thread,'expected_host':self.host,'request_id':str(uuid.uuid4()),'confirm':True}
    def tearDown(self):
        if hasattr(self,'manager'):
            self.manager.dispatch(dict(self.identity,op='release',params={}))
            self.manager.close()
        if hasattr(self,'server'):
            self.server.shutdown(); self.server.server_close(); self.thread_server.join(2)
        if hasattr(self,'original_selected'): candidate.SELECTED=self.original_selected
        super().tearDown()
    def observe_native(self,p=None,actor=None):
        return candidate.observe(self.master,'default',actor or self.root_sid,self.host,p or self.p,_kernel=self.kernel)
    def observation_row(self):
        return tuple(self.worker.db.execute('SELECT * FROM agent_control_observations WHERE target_session=?',('session-worker',)).fetchone())
    def assert_failure_unchanged(self):
        before=self.observation_row()
        with self.assertRaises((CoordError,EvidenceError,TransportError)): self.observe_native()
        self.assertEqual(before,self.observation_row()); self.assertEqual([],self.server.errors)
    def test_first_baseline_and_later_sample(self):
        first=self.observe_native()
        self.assertTrue(first['first_sample']); self.assertFalse(first['first_effect_preservation_independently_checked'])
        self.assertEqual(list(candidate.UNAVAILABLE),first['unavailable_preimage'])
        self.p['request_id']=str(uuid.uuid4())
        later=self.observe_native()
        self.assertFalse(later['first_sample']); self.assertTrue(later['complete_baseline_preserved'])
        self.assertFalse(later['runtime_authorized']); self.assertEqual(2,self.server.resumes)
        self.assertEqual(first['baseline_provenance'],later['baseline_provenance'])
        self.assertEqual(first['request_id'],later['baseline_provenance']['initial_request_id'])
        self.assertEqual(list(candidate.UNAVAILABLE),later['baseline_provenance']['unavailable_preimage'])
        self.assertIs(False,later['baseline_provenance']['first_effect_preservation_independently_checked'])
    def test_exact_retry_lookup_no_second_resume(self):
        self.observe_native(); n=len(self.server.calls)
        result=self.observe_native(); self.assertTrue(result['lookup_only']); self.assertFalse(result['fresh_sample'])
        self.assertEqual(n,len(self.server.calls))
    def test_later_setting_mismatch_no_rebaseline(self):
        self.observe_native(); self.p['request_id']=str(uuid.uuid4()); self.server.settings['approvalPolicy']='never'
        self.assert_failure_unchanged()
    def test_missing_setting(self): self.server.mode='missing'; self.assert_failure_unchanged()
    def test_wrong_returned_thread(self): self.server.mode='wrong_thread'; self.assert_failure_unchanged()
    def test_malformed_protected_setting(self): self.server.mode='malformed'; self.assert_failure_unchanged()
    def test_preread_model_mismatch(self): self.server.mode='model_changed'; self.assert_failure_unchanged()
    def test_active_goal_before_resume(self):
        self.server.goal['status']='active'; self.assert_failure_unchanged(); self.assertEqual(0,self.server.resumes)
    def test_unknown_goal_before_resume(self):
        self.server.goal={'status':'unknown'}; self.assert_failure_unchanged(); self.assertEqual(0,self.server.resumes)
    def test_absent_goal(self): self.server.goal=None; self.assertEqual('observed',self.observe_native()['status'])
    def test_goal_clear(self): self.server.mode='goal_clear'; self.assert_failure_unchanged()
    def test_clear_notification(self): self.server.mode='clear_notification'; self.assert_failure_unchanged()
    def test_disconnect_uncertain_and_no_auto_repeat(self):
        self.server.mode='disconnect'; self.assert_failure_unchanged(); n=self.server.resumes
        self.assertEqual('uncertain',self.observe_native()['status']); self.assertEqual(n,self.server.resumes)
        self.p['request_id']=str(uuid.uuid4()); self.assert_failure_unchanged(); self.assertEqual(n,self.server.resumes)
    def test_partial_frame_uncertain(self): self.server.mode='partial'; self.assert_failure_unchanged()
    def test_owner_replacement(self): self.kernel.replace_at=3; self.assert_failure_unchanged(); self.assertEqual(0,self.server.resumes)
    def test_peer_missing(self): self.kernel.fail_at=2; self.assert_failure_unchanged(); self.assertEqual(0,self.server.resumes)
    def test_rebind_before_persistence(self):
        def rebind():
            c=Coordinator(self.projects.db_path(self.project),'session-worker')
            try:
                c.context()
                state=AgentControlState(c)
                job=state.request('session-worker',self.thread,'resume',str(uuid.uuid4()),True)
                state.update(job['id'],'pending','completed',observation={'state':'idle','queue_count':0,'turn_id':None})
                self.rebind_snapshot=tuple(c.db.execute('SELECT * FROM agent_control_observations WHERE target_session=?',('session-worker',)).fetchone())
                BridgeState(c).bind(str(uuid.uuid4()))
            finally: c.close()
        self.server.hook=rebind
        with self.assertRaises(CoordError): self.observe_native()
        self.assertEqual(self.rebind_snapshot,self.observation_row())
        self.assertEqual([],self.server.errors)
    def test_wrong_actor(self):
        with self.assertRaises(CoordError): self.observe_native(actor='session-notice-recipient')
        self.assertEqual([],self.server.calls)
    def test_wrong_target_tuple(self):
        for key in ('target','expected_thread','expected_host'):
            p=dict(self.p); p[key]='wrong'
            with self.assertRaises(CoordError): self.observe_native(p)
        self.assertEqual([],self.server.calls)
    def test_wrong_project(self):
        p=dict(self.p); p['project']='default'
        with self.assertRaises(CoordError): self.observe_native(p)
        self.assertEqual([],self.server.calls)
    def test_request_conflict(self):
        self.observe_native(); p=dict(self.p); p['project']='default'
        with self.assertRaises(CoordError): self.observe_native(p)
        self.assertEqual(1,self.server.resumes)
    def test_freshness_rejected_before_write(self):
        with fixture.mock.patch.object(candidate,'monotonic',side_effect=[0,21]):
            self.assert_failure_unchanged()
    def test_concurrent_same_request_one_native_effect(self):
        ready=threading.Event(); release=threading.Event()
        def hold():
            ready.set()
            if not release.wait(3): raise AssertionError('test synchronization timed out')
        self.server.hook=hold
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            first=pool.submit(self.observe_native)
            self.assertTrue(ready.wait(3))
            try:
                lookup=self.observe_native()
                self.assertTrue(lookup['lookup_only']); self.assertEqual('attempt',lookup['status'])
            finally: release.set()
            self.assertEqual('observed',first.result(timeout=5)['status'])
        self.assertEqual(1,self.server.resumes)
    def test_immutable_history(self):
        self.observe_native()
        with self.projects.connection() as db:
            with self.assertRaises(Exception): db.execute("DELETE FROM worker_native_events")
            rows=db.execute('SELECT event FROM worker_native_events ORDER BY sequence').fetchall()
        self.assertEqual(['attempt','observed'],[row[0] for row in rows])
    def test_real_kernel_rejects_private_python_listener(self):
        with self.assertRaisesRegex(EvidenceError,'unselected native image'):
            DarwinEvidence().sample({'endpoint':self.endpoint,'pid':os.getpid(),'heartbeat':time.time()})
    def test_owned_python_incomplete_signature_fails_closed(self):
        proc=DarwinEvidence().process(os.getpid())
        helper=Path(candidate.__file__).with_name('native-image-proof')
        result=subprocess.run([str(helper),str(proc.pid),str(proc.birth_sec),str(proc.birth_usec),proc.image],text=True,capture_output=True,timeout=5)
        self.assertEqual(0,result.returncode,result.stderr)
        self.assertIn('projection_hash_equal\ttrue',result.stdout.splitlines())
        self.assertTrue(any(x.startswith('disk_validity\t-67056\t') for x in result.stdout.splitlines()))
        self.assertIn('validated_dynamic_disk_association\tUNPROVEN',result.stdout.splitlines())
    def test_close_failure_no_authoritative_write(self):
        class BrokenClose(RpcClient):
            def close(self):
                super().close()
                raise TransportError('controlled local close failure')
        before=self.observation_row()
        with self.assertRaises(TransportError):
            candidate.observe(self.master,'default',self.root_sid,self.host,self.p,_kernel=self.kernel,_rpc=BrokenClose)
        self.assertEqual(before,self.observation_row())
        self.assertEqual('uncertain',self.observe_native()['status'])
    def test_kernel_tuple_parser(self):
        rows=lsof_records('p123\nf29u\nn127.0.0.1:4500->127.0.0.1:12345\n')
        self.assertEqual(((123,'29u','127.0.0.1:4500->127.0.0.1:12345'),),rows)
        with self.assertRaises(EvidenceError): lsof_records('n127.0.0.1:4500\n')


    def cli_observe(self,actor=None,extra=()):
        module=fixture.root_local
        argv=['--master-db',str(self.master),'--service-plist',str(fixture.service.INSTALLED_SERVER_CONFIG.plist),'--home-project','default','--home-session',actor or self.root_sid,'--host-id',self.host,'worker-native-observe','--project-id',self.project,'--target','session-worker','--expected-thread',self.thread,'--expected-host',self.host,'--request-id',self.p['request_id'],'--confirm',*extra]
        output=io.StringIO()
        with fixture.mock.patch.object(candidate,'root_local',module), fixture.mock.patch.object(candidate,'DarwinEvidence',return_value=self.kernel), contextlib.redirect_stdout(output):
            self.assertEqual(0,module.main(argv))
        return output.getvalue()
    def domain_state(self):
        return {name:tuple(tuple(row) for row in self.worker.db.execute('SELECT * FROM '+name)) for name in ('bridge_bindings','agent_controls','agent_control_requests','bridge_jobs','resources','resource_queue')}
    def assert_allowed_calls(self):
        self.assertTrue(set(self.server.calls)<={'initialize','thread/read','thread/queue/list','thread/goal/get','thread/resume'})
        self.assertEqual([],self.server.errors)
    def test_composed_cli_success_lookup_and_baseline_provenance(self):
        domain=self.domain_state(); goal=copy.deepcopy(self.server.goal); settings=copy.deepcopy(self.server.settings); before=self.observation_row()
        raw=self.cli_observe()
        check=subprocess.run(['jq','-e','.status=="observed" and .observation_only==true and .runtime_authorized==false and .first_effect_preservation_independently_checked==false and (.baseline_provenance.unavailable_preimage|length)==4'],input=raw,text=True,capture_output=True)
        self.assertEqual(0,check.returncode,check.stderr)
        self.assertNotEqual(before,self.observation_row())
        self.assertEqual(domain,self.domain_state()); self.assertEqual(goal,self.server.goal); self.assertEqual(settings,self.server.settings)
        count=len(self.server.calls); raw=self.cli_observe()
        check=subprocess.run(['jq','-e','.lookup_only==true and .fresh_sample==false'],input=raw,text=True,capture_output=True)
        self.assertEqual(0,check.returncode,check.stderr); self.assertEqual(count,len(self.server.calls))
        self.assert_allowed_calls()
    def test_other_root_history_origin_through_cli(self):
        first=self.observe_native()
        historical='00000000-0000-4000-8000-000000000001'
        # Labeled synthetic private HISTORY fixture, not Root authority or real chronology.
        # Copy the real first sample's complete data, then vary only its historical key/time.
        with self.projects.connection() as db:
            db.execute("INSERT INTO worker_native_samples SELECT ?,?,request_digest,project,worker,main,host,status,created_at-1,completed_at-1,scope_digest,settings_json,goal_json,owner_generation,result_json FROM worker_native_samples WHERE root_session=? AND request_id=?",
                ('SYNTHETIC-PRIOR-ROOT',historical,self.root_sid,first['request_id']))
        self.p['request_id']=str(uuid.uuid4())
        raw=self.cli_observe()
        check=subprocess.run(['jq','-e','--arg','origin',historical,
            '.first_sample==false and .complete_baseline_preserved==true and .baseline_provenance.initial_request_id==$origin and .baseline_provenance.unavailable_preimage==["activePermissionProfile","approvalPolicy","approvalsReviewer","sandbox"] and .baseline_provenance.first_effect_preservation_independently_checked==false and .runtime_authorized==false'],
            input=raw,text=True,capture_output=True)
        self.assertEqual(0,check.returncode,check.stderr+' expected prior other-Root initial origin')
        self.assert_allowed_calls()

    def test_composed_cli_rejects_actor_and_override(self):
        before=self.observation_row(); domain=self.domain_state()
        with self.assertRaises(CoordError): self.cli_observe(actor='session-notice-recipient')
        with contextlib.redirect_stderr(io.StringIO()),self.assertRaises(SystemExit): self.cli_observe(extra=('--approval-policy','never'))
        self.assertEqual([],self.server.calls); self.assertEqual(before,self.observation_row()); self.assertEqual(domain,self.domain_state())
    def test_composed_cli_partial_failure_no_success(self):
        self.server.mode='partial'; before=self.observation_row(); domain=self.domain_state(); goal=copy.deepcopy(self.server.goal); settings=copy.deepcopy(self.server.settings)
        with self.assertRaises(TransportError): self.cli_observe()
        self.assertEqual(before,self.observation_row()); self.assertEqual(domain,self.domain_state()); self.assertEqual(goal,self.server.goal); self.assertEqual(settings,self.server.settings)
        with self.projects.connection() as db:
            self.assertEqual('uncertain',db.execute('SELECT status FROM worker_native_samples WHERE request_id=?',(self.p['request_id'],)).fetchone()[0])
            self.assertEqual(0,db.execute("SELECT COUNT(*) FROM worker_native_events WHERE event='observed'").fetchone()[0])
        self.assert_allowed_calls()
    def test_composed_cli_close_failure_no_success(self):
        class BrokenClose(RpcClient):
            def close(self):
                super().close(); raise TransportError('controlled dispatch close failure')
        before=self.observation_row();domain=self.domain_state();goal=copy.deepcopy(self.server.goal);settings=copy.deepcopy(self.server.settings)
        with fixture.mock.patch.object(candidate,'RpcClient',BrokenClose),self.assertRaises(TransportError): self.cli_observe()
        self.assertEqual(before,self.observation_row());self.assertEqual(domain,self.domain_state());self.assertEqual(goal,self.server.goal);self.assertEqual(settings,self.server.settings)
        with self.projects.connection() as db:
            self.assertEqual('uncertain',db.execute('SELECT status FROM worker_native_samples WHERE request_id=?',(self.p['request_id'],)).fetchone()[0])
            self.assertEqual(0,db.execute("SELECT COUNT(*) FROM worker_native_events WHERE event='observed'").fetchone()[0])
        self.assert_allowed_calls()

# Run ONLY this new class's own tests; do not replay inherited intent suites.
def load_tests(loader, tests, pattern):
    # Observation uses Flow helpers with its own setup; Flow's tests run in
    # their owning module and require a different fixture.
    return unittest.TestSuite(Observation(name) for name in Observation.__dict__ if name.startswith('test_'))


if __name__=='__main__':
    selected=os.environ.get('OBSERVER_TEST_SELECTION')
    names=selected.split(',') if selected else [name for name in Observation.__dict__ if name.startswith('test_')]
    tests=[Observation(name) for name in names]
    result=unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(tests))
    raise SystemExit(0 if result.wasSuccessful() else 1)
