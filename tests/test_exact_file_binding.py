"""Actual private App management/task admission, without historical selectors."""
import contextlib
import io
import hashlib
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
import uuid
from unittest import mock

from agent_chat import cli, core, root_local, service, remote_service, exact_file_read
from agent_chat.remote import RemoteCoordError
from test_creator_management import CreatorManagementTests

class ExactFileBindingTests(CreatorManagementTests):
    def setUp(self):
        private=tempfile.TemporaryDirectory(prefix='exact-binding-',dir='/private/tmp')
        self.addCleanup(private.cleanup)
        self.fixture=Path(private.name).resolve()
        for name in ('home','codex','client-state','output'):(self.fixture/name).mkdir(mode=0o700)
        self.env_stack=contextlib.ExitStack();self.addCleanup(self.env_stack.close)
        env={k:v for k,v in os.environ.items() if not k.startswith('AGENT_CHAT_') and k not in ('HOME','CODEX_HOME','PYTHONPATH')}
        env.update(HOME=str(self.fixture/'home'),CODEX_HOME=str(self.fixture/'codex'),AGENT_CHAT_STATE_DIR=str(self.fixture/'client-state'))
        self.env_stack.enter_context(mock.patch.dict(os.environ,env,clear=True))
        # Base fixture replaces BOTH installed-server resolvers with its exact
        # private plist/wrapper and constructs explicit private DB/registry.
        # Block any configured-master fallback; no real installed config opens.
        for module in (service,root_local):
            original=module.configured_server_database
            def private_config(plist=None,fn=original):
                if plist is None or Path(plist).resolve()!=self.plist.resolve():
                    raise AssertionError('installed master discovery outside exact private plist')
                result=fn(plist)
                if Path(result).resolve()!=self.master.resolve():
                    raise AssertionError('configured database escaped private fixture')
                return result
            self.env_stack.enter_context(mock.patch.object(module,'configured_server_database',side_effect=private_config))
        super().setUp()
        created,self.worker_thread,_=self.create_worker('snapshot-specialist')
        self.worker=created['target_session']
        self.root_projection=self.call('route',project=self.project_id)['session']
        self.po_call('context',session=self.root_projection)
        objective=self.po_call('send',session=self.root_projection,to=self.po,body='Own one new private exact-file snapshot task.')['id']
        self.po_call('context')
        goal=self.po_call('activity-adopt',source_request_id=objective,title='Private snapshot objective')
        self.assignment=self.po_call('send',to=self.worker,body='Copy only the new exact fixture source to its new private output; preserve truthful completion evidence.')['id']
        self.task=self.po_call('activity-assign',goal_id=goal['id'],worker=self.worker,source_request_id=self.assignment,title='New App snapshot')
        self.po_call('context',session=self.worker)
        self.source=self.fixture/'source.bin';self.source.write_bytes(b'new inert snapshot\n')
        self.output=self.fixture/'output'/'copy.bin'
        self.cap=self.fixture/'output'/'capability.json'
        self.binding_request=str(uuid.uuid4())

    def cli_call(self,actor,*args,expected_rc=0):
        stream=io.StringIO()
        with contextlib.redirect_stdout(stream):
            rc=cli.main(['--server',self.url,'--api-token',self.po_client.token,'--project',self.project_id,'--session',actor,*args])
        self.assertEqual(rc,expected_rc)
        return stream.getvalue()

    def binding_payload(self):
        return dict(to=self.worker,task_id=self.task['id'],source_request_id=self.assignment,
            expected_host='host',expected_thread=self.worker_thread,path=str(self.source),output=str(self.output),
            max_bytes=8*1024*1024+1,minutes=5,request_id=self.binding_request,confirm=True)

    def grant(self):
        self.po_call('context')
        return self.po_call('exact-file-bind',**self.binding_payload())

    def acquire(self,binding):
        raw=self.cli_call(self.worker,'exact-file-request','--binding-id',binding['id'],
            '--path',str(self.source),'--minutes','5','--capability-file',str(self.cap))
        return raw,subprocess.run(['jq','-er','.reservation_id'],input=raw,text=True,capture_output=True,check=True).stdout.strip()

    def copy(self,binding,rid,*,expected_rc=0):
        return self.cli_call(self.worker,'exact-file-read','--binding-id',binding['id'],
            '--path',str(self.source),'--output',str(self.output),'--max-bytes',str(binding['max_bytes']),
            '--reservation-id',rid,'--capability-file',str(self.cap),
            '--process-evidence-file',str(self.fixture/'output'/'process.jsonl'),expected_rc=expected_rc)

    def test_real_app_task_can_receive_explicit_manager_binding(self):
        # Correct current boundary: a normal assignment alone does not grant an
        # arbitrary path. This must remain denied until the manager binds it.
        with self.assertRaisesRegex(RemoteCoordError,'reviewed actor/project/host/path'):
            self.po_call('exact-file-request',session=self.worker,path=str(self.source),minutes=5)
        self.po_call('context')
        bound=self.cli_call(self.po,'exact-file-bind','--to',self.worker,
            '--task-id',self.task['id'],'--source-request-id',self.assignment,
            '--expected-host','host','--expected-thread',self.worker_thread,
            '--path',str(self.source),'--output',str(self.output),'--max-bytes',str(8*1024*1024+1),
            '--minutes','5','--request-id',self.binding_request,'--confirm')
        binding=subprocess.run(['jq','-er','.id'],input=bound,text=True,capture_output=True,check=True).stdout.strip()
        claim=self.cli_call(self.worker,'exact-file-request','--binding-id',binding,
            '--path',str(self.source),'--minutes','5','--capability-file',str(self.cap))
        subprocess.run(['jq','-e','.state=="owned"'],input=claim,text=True,capture_output=True,check=True)
        rid=subprocess.run(['jq','-er','.reservation_id'],input=claim,text=True,capture_output=True,check=True).stdout.strip()
        size=8*1024*1024+1;block=b'new inert fixture snapshot\n'*2048;digest=hashlib.sha256()
        with self.source.open('wb') as stream:
            left=size
            while left:
                part=block[:min(left,len(block))];stream.write(part);digest.update(part);left-=len(part)
        receipt=self.copy(dict(id=binding,max_bytes=size),rid)
        subprocess.run(['jq','-e','.exit_code==0'],input=receipt,text=True,capture_output=True,check=True)
        actual=hashlib.sha256()
        with self.output.open('rb') as stream:
            while chunk:=stream.read(65536):actual.update(chunk)
        self.assertEqual(self.output.stat().st_size,size);self.assertEqual(actual.hexdigest(),digest.hexdigest())
        outcome=self.po_call('exact-file-result',session=self.worker,path=str(self.source),reservation_id=rid,binding_id=binding)
        self.assertEqual(outcome['state'],'pending');self.assertFalse(outcome['receipt_backed_clear'])
        subprocess.run(['jq','-es','any(.event=="admitted") and any(.event=="leader_reaped" and .returncode==0)'],input=(self.fixture/'output'/'process.jsonl').read_text(),text=True,capture_output=True,check=True)
        print('new App-owned copy bytes='+str(size)+' sha256='+actual.hexdigest()+' server_result=pending',flush=True)

    def test_worker_cannot_grant_or_change_audit_tuple(self):
        payload=self.binding_payload()
        with self.assertRaisesRegex(RemoteCoordError,'current project PO or Root'):
            self.po_call('exact-file-bind',session=self.worker,**payload)
        for key,value in (('expected_host','other-host'),('expected_thread',str(uuid.uuid4())),('task_id','foreign-task'),('source_request_id','foreign-source')):
            with self.subTest(key=key),self.assertRaises(RemoteCoordError):self.po_call('exact-file-bind',**dict(payload,**{key:value}))
        for protected in (exact_file_read.SELECTED_PATH,next(iter(exact_file_read.CHARACTERS_FILES))):
            with self.assertRaisesRegex(RemoteCoordError,'historical exact file selections'):
                self.po_call('exact-file-bind',**dict(payload,path=protected))
        granted=self.grant();self.assertEqual(granted['id'],self.po_call('exact-file-bind',**payload)['id'])
        with self.assertRaisesRegex(RemoteCoordError,'retry payload differs'):
            self.po_call('exact-file-bind',**dict(payload,output=str(self.fixture/'output'/'other.bin')))
        with self.assertRaises(RemoteCoordError):
            self.po_call('exact-file-binding-status',session=self.worker,host='wrong-host',binding_id=granted['id'])
        with self.assertRaisesRegex(RemoteCoordError,'current project PO or Root'):
            self.po_call('exact-file-revoke',session=self.worker,binding_id=granted['id'],request_id=str(uuid.uuid4()),reason='worker cannot change grant authority',confirm=True)
        self.po_call('activity-transition',session=self.worker,work_id=self.task['id'],state='blocked')
        with self.assertRaisesRegex(RemoteCoordError,'runnable task'):
            self.po_call('exact-file-request',session=self.worker,path=str(self.source),minutes=5,binding_id=granted['id'])


    def test_bound_capability_cannot_admit_generic_or_wrong_destination_executor(self):
        binding=self.grant();raw,rid=self.acquire(binding)
        token=subprocess.run(['jq','-er','.token'],input=self.cap.read_text(),text=True,capture_output=True,check=True).stdout.strip()
        with self.assertRaisesRegex(RemoteCoordError,'typed snapshot executor'):
            self.po_call('begin-guard',session=self.worker,resource=str(self.source),token=token,reservation_id=rid)
        with self.assertRaisesRegex(RemoteCoordError,'batch executor'):
            self.po_call('begin-guard-batch',session=self.worker,tuples=[dict(resource=str(self.source),session_id=self.worker,host_id='host',token=token,reservation_id=rid)])
        for output,limit in ((str(self.fixture/'output'/'other.bin'),binding['max_bytes']),(str(self.output),binding['max_bytes']+1),(str(self.output),True)):
            with self.assertRaisesRegex(RemoteCoordError,'destination or byte limit differs'):
                self.po_call('exact-file-begin',session=self.worker,path=str(self.source),token=token,reservation_id=rid,
                    binding_id=binding['id'],output=output,max_bytes=limit)
        self.assertFalse(self.output.exists())

    def test_manager_revocation_and_expiry_prevent_new_acquisition(self):
        binding=self.grant();request=str(uuid.uuid4())
        revoked=self.po_call('exact-file-revoke',binding_id=binding['id'],request_id=request,reason='fixture task withdrawn',confirm=True)
        self.assertEqual(revoked['state'],'revoked')
        self.assertEqual(self.po_call('exact-file-revoke',binding_id=binding['id'],request_id=request,reason='fixture task withdrawn',confirm=True)['state'],'revoked')
        with self.assertRaisesRegex(RemoteCoordError,'revoked'):
            self.po_call('exact-file-request',session=self.worker,path=str(self.source),minutes=5,binding_id=binding['id'])
        self.binding_request=str(uuid.uuid4());second=self.grant()
        with mock.patch.object(core,'_now',return_value=second['expires_at']+1):
            with self.assertRaisesRegex(RemoteCoordError,'expired'):
                self.po_call('exact-file-request',session=self.worker,path=str(self.source),minutes=5,binding_id=second['id'])
        self.assertFalse(self.cap.exists());self.assertFalse(self.output.exists())

    def test_revocation_during_real_stream_keeps_no_final_output(self):
        self.source.write_bytes(b'bounded inert bytes'*12000)
        binding=self.grant();_,rid=self.acquire(binding)
        original=remote_service.dispatch;pulses=[];revoked=[];staged=[]
        def dispatch(coord,body,**kwargs):
            if body.get('op')=='exact-file-pulse':
                pulses.append(body['params']['run_id'])
                sizes=[path.stat().st_size for path in self.output.parent.glob('.agent-chat-snapshot-*')]
                # Buffered writes need not reach disk after the first chunk.
                # Withdraw only after observing a real, incomplete owned stage.
                self.assertLessEqual(len(pulses),8)
                if not revoked and len(sizes)==1 and 0<sizes[0]<self.source.stat().st_size:
                    staged.extend(sizes)
                    revoked.append(self.po_call('exact-file-revoke',binding_id=binding['id'],request_id=str(uuid.uuid4()),reason='withdraw during owned fixture stream',confirm=True))
            return original(coord,body,**kwargs)
        with mock.patch.object(remote_service,'dispatch',side_effect=dispatch):
            receipt=self.copy(binding,rid,expected_rc=1)
        subprocess.run(['jq','-e','.exit_code!=0'],input=receipt,text=True,capture_output=True,check=True)
        self.assertEqual(revoked[0]['state'],'revoked');self.assertEqual(len(staged),1)
        self.assertGreater(staged[0],0);self.assertLess(staged[0],self.source.stat().st_size);self.assertFalse(self.output.exists())
        print('revoked actual partial stage bytes='+str(staged[0])+' typed_pulses='+str(len(pulses)),flush=True)
        self.assertFalse(list(self.output.parent.glob('.agent-chat-snapshot-*')))
        outcome=self.po_call('exact-file-result',session=self.worker,path=str(self.source),reservation_id=rid,binding_id=binding['id'])
        self.assertEqual(outcome['state'],'pending');self.assertFalse(outcome['receipt_backed_clear'])


    def test_revocation_after_outer_check_rejected_inside_begin_transaction(self):
        binding=self.grant();raw,rid=self.acquire(binding)
        token=subprocess.run(['jq','-er','.token'],input=self.cap.read_text(),text=True,capture_output=True,check=True).stdout.strip()
        original=remote_service._begin_guard;revoked=[]
        def before_begin(coord,session,host,params,**kwargs):
            revoked.append(self.po_call('exact-file-revoke',binding_id=binding['id'],request_id=str(uuid.uuid4()),reason='withdraw after outer validation',confirm=True))
            return original(coord,session,host,params,**kwargs)
        with mock.patch.object(remote_service,'_begin_guard',side_effect=before_begin):
            with self.assertRaisesRegex(RemoteCoordError,'revoked'):
                self.po_call('exact-file-begin',session=self.worker,path=str(self.source),token=token,reservation_id=rid,
                    binding_id=binding['id'],output=str(self.output),max_bytes=binding['max_bytes'])
        context=self.po_call('exact-file-guard-context',session=self.worker,path=str(self.source),binding_id=binding['id'])
        self.assertEqual(context['runs'],[]);self.assertEqual(revoked[0]['state'],'revoked');self.assertFalse(self.output.exists())

    def test_revoked_cleanup_readbacks_keep_binding_without_fake_release(self):
        binding=self.grant();_,rid=self.acquire(binding)
        self.po_call('exact-file-revoke',binding_id=binding['id'],request_id=str(uuid.uuid4()),reason='withdraw fixture',confirm=True)
        observed=[]
        def read_only_boundary(params,call):
            observed.append(call('status',{'resources':[str(self.source)]}))
            observed.append(call('guard-context',{'resource':str(self.source)}))
            raise core.CoordError('fixture stops before a real closure receipt is supplied')
        with mock.patch.object(cli,'_prepare_return',side_effect=read_only_boundary):
            with self.assertRaisesRegex(core.CoordError,'real closure receipt'):
                self.cli_call(self.worker,'exact-file-release','--binding-id',binding['id'],'--path',str(self.source),
                    '--reservation-id',rid,'--capability-file',str(self.cap),'--receipt',str(self.fixture/'missing-real-receipt.json'))
        self.assertEqual(observed[0]['resources'][0]['reservation_id'],rid)
        self.assertEqual(observed[1]['reservation_id'],rid)
        result=self.po_call('exact-file-result',session=self.worker,path=str(self.source),reservation_id=rid,binding_id=binding['id'])
        self.assertEqual(result['state'],'pending');self.assertFalse(result['receipt_backed_clear'])

    def test_canonical_own_root_local_binding_route_and_identity_negatives(self):
        self.consume()
        payload=self.binding_payload()
        args=['--master-db',str(self.master),'--service-plist',str(self.plist),
            '--home-project','default','--home-session','root','--host-id','host',
            'exact-file-bind','--project-id',self.project_id,'--to',self.worker,
            '--task-id',payload['task_id'],'--source-request-id',payload['source_request_id'],
            '--expected-host','host','--expected-thread',self.worker_thread,
            '--path',str(self.source),'--output',str(self.output),'--max-bytes',str(payload['max_bytes']),
            '--minutes','5','--request-id',self.binding_request,'--confirm']
        stream=io.StringIO()
        with contextlib.redirect_stdout(stream):self.assertEqual(root_local.main(args),0)
        binding=subprocess.run(['jq','-er','.id'],input=stream.getvalue(),text=True,capture_output=True,check=True).stdout.strip()
        readback=self.call('exact-file-binding-status',project=self.project_id,binding_id=binding)
        self.assertEqual(readback['issuer_session'],self.root_projection)
        self.assertEqual(readback['owner_session'],self.worker)
        self.assertEqual(readback['state'],'active')
        for key,value in (('session',self.po),('host_id','different-host'),('home_project',self.project_id)):
            with self.subTest(identity=key),self.assertRaises(core.CoordError):
                root_local.execute('exact-file-binding-status',dict(project=self.project_id,binding_id=binding),**dict(self.identity,**{key:value}))
        with self.assertRaises(core.CoordError):self.call('exact-file-binding-status',project='missing-project',binding_id=binding)
        self.assertEqual(self.call('exact-file-revoke',project=self.project_id,binding_id=binding,
            request_id=str(uuid.uuid4()),reason='Root withdraws only its own exact fixture grant',confirm=True)['state'],'revoked')
        self.call('stop-all',request_id=str(uuid.uuid4()),confirm=True)
        self.consume()
        with self.assertRaisesRegex(core.CoordError,'stop-all'):
            self.call('exact-file-bind',project=self.project_id,**dict(payload,request_id=str(uuid.uuid4())))

def load_tests(loader,standard,pattern):
    return unittest.TestSuite([ExactFileBindingTests(name) for name in ExactFileBindingTests.__dict__ if name.startswith('test_')])
