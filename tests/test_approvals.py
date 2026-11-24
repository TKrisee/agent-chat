"""Real authenticated HTTP/lease plus native framed socket approval workflows."""
import base64
import hashlib
import os
import select
import socket
import subprocess
import threading
import time
import unittest
import uuid
from pathlib import Path
from agent_chat.approvals import ApprovalPump, METHODS, permission_profile, selected_permissions
from agent_chat.core import CoordError
from agent_chat.bridge_client import RemoteBridgeState
from agent_chat.remote import HttpClient, RemoteCoordError
from agent_chat.rpc import RpcClient, TransportError
from test_remote_web import RemoteWebFixture, TOKEN


def query(raw, expression):
    return subprocess.run(['jq', '-er', expression], input=raw, capture_output=True, check=True).stdout.decode().strip()


def payload(expression, **values):
    args = ['jq', '-cn']
    for key, value in values.items(): args += ['--arg', key, str(value)]
    return subprocess.run(args + [expression], capture_output=True, check=True).stdout.strip()


def send(peer, raw):
    size = len(raw)
    peer.sendall((bytes((0x81, size)) if size < 126 else b'\x81\x7e' + size.to_bytes(2, 'big')) + raw)


def exact(peer, count):
    result = b''
    while len(result) < count:
        part = peer.recv(count-len(result))
        if not part: raise EOFError('peer closed')
        result += part
    return result


def frame(peer):
    header = exact(peer, 2); size = header[1] & 127
    if size == 126: size = int.from_bytes(exact(peer,2),'big')
    elif size == 127: size = int.from_bytes(exact(peer,8),'big')
    mask = exact(peer,4) if header[1] & 128 else None
    raw = exact(peer,size)
    if mask: raw = bytes(value ^ mask[index%4] for index,value in enumerate(raw))
    return header[0]&15, raw


class PermissionProfileBoundary(unittest.TestCase):
    def test_unknown_or_ambiguous_fields_fail_closed(self):
        for profile in (
            {'network': {'enabled': True, 'other': True}, 'fileSystem': None},
            {'network': None, 'fileSystem': {'read': ['/tmp/a\nwrite: /'], 'write': None}},
            {'network': None, 'fileSystem': {'read': ['/tmp/a'], 'write': None, 'unknown': []}},
        ):
            with self.subTest(profile=profile), self.assertRaises(CoordError):
                permission_profile(profile)

    def test_write_subset_cannot_drop_requested_read_constraint(self):
        profile = permission_profile({'network': None, 'fileSystem': {'read': None, 'write': None,
            'entries': [{'path': {'type':'path', 'path':'/project'}, 'access':'write'},
                        {'path': {'type':'path', 'path':'/project/.git'}, 'access':'read'}]}})
        with self.assertRaises(CoordError):
            selected_permissions(profile, ['fs:0'])

    def test_documented_relative_glob_is_preserved(self):
        profile = permission_profile({'network':None, 'fileSystem':{'read':None,'write':None,
            'entries':[{'path':{'type':'glob_pattern','pattern':'src/**/*.py'},'access':'read'}]}})
        self.assertEqual(selected_permissions(profile, ['fs:0'])['fileSystem']['entries'][0]['path']['pattern'], 'src/**/*.py')

    def test_disjoint_unselected_read_is_not_silently_granted(self):
        profile = permission_profile({'network':None, 'fileSystem':{'read':['/other'], 'write':['/project']}})
        with self.assertRaises(CoordError):
            selected_permissions(profile, ['write:0'])

    def test_universal_read_does_not_constrain_specific_write(self):
        profile = permission_profile({'network':None, 'fileSystem':{'read':['/'], 'write':['/project']}})
        granted = selected_permissions(profile, ['write:0'])
        self.assertIsNone(granted['fileSystem']['read'])
        self.assertEqual(granted['fileSystem']['write'], ['/project'])

    def test_glob_and_special_read_constraints_cannot_be_dropped(self):
        for path in ({'type':'glob_pattern','pattern':'src/**'}, {'type':'special','value':{'kind':'project_roots','subpath':'.git'}}):
            profile = permission_profile({'network':None,'fileSystem':{'read':None,'write':None,'entries':[
                {'path':{'type':'special','value':{'kind':'project_roots','subpath':None}},'access':'write'},
                {'path':path,'access':'read'}]}})
            with self.subTest(path=path), self.assertRaises(CoordError):
                selected_permissions(profile, ['fs:0'])


class ApprovalFlow(RemoteWebFixture):
    def start(self, **kwargs):
        self.audit_path = str(self.db) + ".approvals.jsonl"
        super().start(**kwargs)

    def setUp(self):
        super().setUp()
        self.call('register', agent='approval-worker')
        self.native_thread = str(uuid.uuid4())
        self.call('bind', thread=self.native_thread)
        self.identity = dict(owner='approval-fixture',secret='test-lease-'+('x'*40),host_id='mac')
        self.state = RemoteBridgeState(self.client, self.url, self.identity)
        self.state.call('acquire')
        self.rpc = None; self.peer = None; self.pump = None; self.workers=[]
        self.connect()

    def connect(self, lifetime=3):
        left,self.peer=socket.socketpair();left.setblocking(False);self.peer.settimeout(3)
        self.rpc=RpcClient('ws://127.0.0.1:1',timeout=3,server_request_methods=METHODS,server_request_timeout=lifetime)
        self.rpc._sock=left;self.rpc._ensure_reader()
        self.pump=ApprovalPump(self.rpc,lambda:(self.state,)).start()

    def tearDown(self):
        if self.pump: self.pump.close()
        if self.rpc: self.rpc.close()
        if self.peer: self.peer.close()
        for worker in self.workers:
            worker.join(3);self.assertFalse(worker.is_alive())
        self.state.call('release')
        super().tearDown()

    def listing(self, project=None):
        route='/api/approvals'+('?project='+project if project else '')
        code,raw,_=self.request('GET',route,auth='basic');self.assertEqual(code,200,raw)
        return raw

    def wait_status(self,status,handle=None):
        deadline=time.monotonic()+4
        while time.monotonic()<deadline:
            raw=self.listing()
            expr='.requests[] | select(.status=="'+status+'") | .handle'
            if handle: expr='.requests[] | select(.handle=="'+handle+'" and .status=="'+status+'") | .handle'
            result=subprocess.run(['jq','-r',expr],input=raw,capture_output=True,check=True).stdout.decode().strip()
            if result:return result
            time.sleep(.02)
        self.fail('approval did not reach '+status+': '+raw.decode())

    def native(self,method=METHODS[0],thread=None,extra=''):
        raw=payload('{id:91,method:$method,params:({threadId:$thread,turnId:"turn-1",itemId:"item-1",command:"echo test",reason:"test reason"}'+extra+')}',method=method,thread=thread or self.native_thread)
        send(self.peer,raw)
        return self.wait_status('pending')

    def decide(self,handle,choice='accept',project=None,auth='basic',csrf=True,**extra):
        headers={'X-Agent-Chat-CSRF':self.server.csrf_token} if csrf else {}
        return self.request('POST','/api/approvals/decision'+('?project='+project if project else ''),
            dict(handle=handle,decision=choice,**extra),auth=auth,headers=headers)

    def test_command_while_outbound_waits_and_resolved_item_correlation(self):
        result={}
        def outbound():
            try:result['value']=self.rpc.request('thread/read',{'threadId':self.native_thread},deadline=time.monotonic()+3)
            except Exception as error:result['error']=error
        worker=threading.Thread(target=outbound);self.workers.append(worker);worker.start()
        opcode,outgoing=frame(self.peer);request_id=query(outgoing,'.id')
        handle=self.native()
        code,raw,_=self.decide(handle);self.assertEqual(code,200,raw);self.assertEqual(query(raw,'.status'),'proposed')
        opcode,response=frame(self.peer);self.assertEqual(query(response,'.id'),'91');self.assertEqual(query(response,'.result.decision'),'accept')
        self.wait_status('response_sent',handle)
        send(self.peer,payload('{id:($id|tonumber),result:{ordinary:"ok"}}',id=request_id))
        worker.join(2);self.assertEqual(result,{'value':{'ordinary':'ok'}})
        # Wrong request/item events cannot advance the association.
        send(self.peer,payload('{method:"serverRequest/resolved",params:{threadId:$thread,requestId:92}}',thread=self.native_thread))
        time.sleep(.15);self.assertIn(handle,self.wait_status('response_sent',handle))
        send(self.peer,payload('{method:"serverRequest/resolved",params:{threadId:$thread,requestId:91}}',thread=self.native_thread))
        self.wait_status('resolved',handle)
        send(self.peer,payload('{method:"item/completed",params:{threadId:$thread,turnId:"turn-1",item:{id:"item-1"}}}',thread=self.native_thread))
        self.wait_status('completed',handle)
        self.server.approval_logging._queue.join()
        with open(self.audit_path, 'rb') as stream: audit=stream.read()
        self.assertEqual(query(audit, 'select(.event=="approval_completed") | .correlation_id'),handle)
        self.assertNotIn(self.identity['secret'].encode(),audit)
        self.assertNotIn(b'echo test',audit)
        self.assertEqual(self.server.logging.config.level,'OFF')
        from pathlib import Path
        self.assertEqual(Path(self.audit_path).stat().st_mode & 0o777,0o600)
        self.assertEqual(self.decide(handle)[0],400)

    def test_permission_request_reaches_authenticated_operator(self):
        send(self.peer, payload('{id:91,method:"item/permissions/requestApproval",params:{threadId:$thread,turnId:"turn-1",itemId:"item-1",environmentId:null,startedAtMs:1,cwd:"/tmp",reason:"fixture",permissions:{network:{enabled:true},fileSystem:null}}}', thread=self.native_thread))
        handle = self.wait_status('pending')
        self.assertEqual(query(self.listing(), '.requests[] | select(.handle=="'+handle+'") | .preview.kind'), 'permissions')
        self.assertEqual(self.decide(handle, 'fileSystem_session')[0], 400)
        self.assertEqual(self.decide(handle, 'network_turn')[0], 200)
        _, response = frame(self.peer)
        self.assertEqual(query(response, '.result.permissions.network.enabled'), 'true')
        self.assertEqual(query(response, '.result.scope'), 'turn')
        self.assertEqual(query(response, '.result.permissions | if has("fileSystem") then "present" else "absent" end'), 'absent')
        self.assertEqual(self.decide(handle, 'network_turn')[0], 400)

    def test_permission_subset_session_scope_and_no_excess(self):
        send(self.peer, payload('{id:91,method:"item/permissions/requestApproval",params:{threadId:$thread,turnId:"turn-1",itemId:"item-1",environmentId:null,startedAtMs:1,cwd:"/tmp",reason:"fixture",permissions:{network:{enabled:true},fileSystem:{read:["/tmp/read"],write:["/tmp/write"]}}}}', thread=self.native_thread))
        handle = self.wait_status('pending')
        self.assertEqual(self.decide(handle, 'fileSystem_session')[0], 200)
        _, response = frame(self.peer)
        self.assertEqual(query(response, '.result.scope'), 'session')
        self.assertEqual(query(response, '.result.permissions.fileSystem.write[0]'), '/tmp/write')
        self.assertEqual(query(response, '.result.permissions | if has("network") then "present" else "absent" end'), 'absent')

    def test_permission_denial_returns_empty_turn_grant(self):
        send(self.peer, payload('{id:91,method:"item/permissions/requestApproval",params:{threadId:$thread,turnId:"turn-1",itemId:"item-1",environmentId:null,startedAtMs:1,cwd:"/tmp",reason:"fixture",permissions:{network:{enabled:true},fileSystem:null}}}', thread=self.native_thread))
        handle = self.wait_status('pending')
        self.assertEqual(self.decide(handle, 'deny')[0], 200)
        _, response = frame(self.peer)
        self.assertEqual(query(response, '.result.permissions | length'), '0')
        self.assertEqual(query(response, '.result.scope'), 'turn')
        self.wait_status('response_sent', handle)
        with self.assertRaises(RemoteCoordError):
            self.state.call('approval-finish', epoch=self.pump.epoch, handle=handle, outcome='completed', item_outcome={'type':'fileChange','status':'completed'})

    def test_modern_filesystem_subset_turn_grant(self):
        send(self.peer, payload('{id:91,method:"item/permissions/requestApproval",params:{threadId:$thread,turnId:"turn-1",itemId:"item-1",environmentId:null,startedAtMs:1,cwd:"/tmp",reason:"fixture",permissions:{network:null,fileSystem:{read:null,write:null,globScanMaxDepth:3,entries:[{path:{type:"path",path:"/tmp/a"},access:"read"},{path:{type:"glob_pattern",pattern:"/tmp/b/**"},access:"write"},{path:{type:"path",path:"/tmp/b/private"},access:"deny"},{path:{type:"path",path:"/tmp/unselected"},access:"write"}]}}}}', thread=self.native_thread))
        handle = self.wait_status('pending')
        choice = {'scope':'turn', 'selection':['fs:0', 'fs:1']}
        self.assertEqual(self.decide(handle, {'scope':'session','selection':['fs:99']})[0], 400)
        self.assertEqual(self.decide(handle, choice)[0], 200)
        _, response = frame(self.peer)
        self.assertEqual(query(response, '.result.scope'), 'turn')
        self.assertEqual(query(response, '.result.permissions.fileSystem.entries | length'), '3')
        self.assertEqual(query(response, '.result.permissions.fileSystem.entries[1].path.pattern'), '/tmp/b/**')
        self.assertEqual(query(response, '.result.permissions.fileSystem.globScanMaxDepth'), '3')
        self.assertEqual(query(response, '.result.permissions.fileSystem.entries[2].access'), 'deny')

    def browser_permission_decision(self, mode):
        self.pump.close(); self.rpc.close(); self.peer.close(); self.connect(lifetime=45)
        send(self.peer, payload('{id:91,method:"item/permissions/requestApproval",params:{threadId:$thread,turnId:"turn-1",itemId:"item-1",environmentId:null,startedAtMs:1,cwd:"/tmp",reason:"fixture",permissions:{network:null,fileSystem:{read:null,write:null,globScanMaxDepth:3,entries:[{path:{type:"path",path:"/tmp/a"},access:"read"},{path:{type:"glob_pattern",pattern:"/tmp/b/**"},access:"write"},{path:{type:"path",path:"/tmp/b/private"},access:"deny"},{path:{type:"path",path:"/tmp/unselected"},access:"write"}]}}}}', thread=self.native_thread))
        handle = self.wait_status('pending')
        env = dict(os.environ, PERMISSION_TEST_URL=self.url, PERMISSION_TEST_TOKEN=TOKEN, PERMISSION_TEST_MODE=mode)
        script = Path(__file__).with_name('permission_e2e_browser.cjs')
        result = subprocess.run(['node', str(script)], env=env, capture_output=True, text=True, timeout=40)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        _, response = frame(self.peer)
        self.wait_status('response_sent', handle)
        self.assertEqual(query(response, '.result.scope'), 'turn')
        if mode == 'grant':
            self.assertEqual(query(response, '.result.permissions.fileSystem.entries | length'), '3')
            self.assertEqual(query(response, '.result.permissions.fileSystem.entries[1].path.pattern'), '/tmp/b/**')
            self.assertEqual(query(response, '.result.permissions.fileSystem.entries[2].access'), 'deny')
        else:
            self.assertEqual(query(response, '.result.permissions | length'), '0')

    def test_browser_permission_subset_reaches_provider(self): self.browser_permission_decision('grant')
    def test_browser_permission_denial_reaches_provider(self): self.browser_permission_decision('deny')

    def test_idle_file_decline_and_redacted_projection(self):
        handle=self.native(METHODS[1])
        raw=self.listing()
        for secret in (TOKEN,self.identity['secret'],self.native_thread,'connection_epoch','requestId','turnId'):
            self.assertNotIn(secret.encode(),raw)
        self.assertEqual(self.decide(handle,'decline')[0],200)
        _,response=frame(self.peer);self.assertEqual(query(response,'.result.decision'),'decline')
        self.wait_status('response_sent',handle)
        self.pump.close()
        self.wait_status('response_sent',handle)

    def test_auth_csrf_invalid_duplicate_and_wrong_context(self):
        handle=self.native(extra='+{availableDecisions:["decline","cancel"]}')
        self.assertEqual(self.request('GET','/api/approvals',auth='bearer')[0],403)
        self.assertEqual(self.decide(handle,auth='bearer')[0],403)
        self.assertEqual(self.decide(handle,csrf=False)[0],403)
        wrong='Basic '+base64.b64encode(('other:'+TOKEN).encode()).decode()
        self.assertEqual(self.request('GET','/api/approvals',auth=None,headers={'Authorization':wrong})[0],401)
        self.assertEqual(self.decide(handle,'accept')[0],400)
        self.assertEqual(self.decide(handle,'decline',actor='other')[0],400)
        self.assertEqual(self.decide('unknown','decline')[0],400)
        project=self.client.call('/api/projects/rpc',dict(op='create',name='Other'))['project']['id']
        # Establish the legitimate other project operator context through HTTP.
        self.assertEqual(self.request('GET','/api/config?project='+project,auth='basic')[0],200)
        self.assertEqual(self.decide(handle,'decline',project=project)[0],400)
        wronglease=RemoteBridgeState(self.client,self.url,dict(self.identity,secret='z'*40))
        with self.assertRaises(RemoteCoordError):wronglease.call('approval-poll',epoch=self.pump.epoch,handles=[handle])
        self.assertEqual(self.decide(handle,'decline')[0],200)
        self.assertEqual(self.decide(handle,'decline')[0],400)
        _,response=frame(self.peer);self.assertEqual(query(response,'.result.decision'),'decline')

    def test_disconnect_reconnect_and_old_handle(self):
        handle=self.native();self.peer.close();self.wait_status('disconnected',handle)
        self.assertEqual(self.decide(handle)[0],400)
        self.pump.close();self.rpc.close();self.connect()
        next_handle=self.native();self.assertNotEqual(handle,next_handle)
        self.assertEqual(self.decide(handle)[0],400);self.assertEqual(self.decide(next_handle)[0],200)
        _,response=frame(self.peer);self.assertEqual(query(response,'.id'),'91')

    def test_timeout_fails_closed(self):
        self.pump.close();self.rpc.close();self.peer.close();self.connect(lifetime=.7)
        handle=self.native();self.wait_status('expired',handle)
        self.assertEqual(self.decide(handle)[0],400)
        deadline=time.monotonic()+2
        while self.rpc._sock is not None and time.monotonic()<deadline:time.sleep(.02)
        self.assertIsNone(self.rpc._sock)

    def test_native_resolution_revokes_unanswered_capability(self):
        handle=self.native()
        send(self.peer,payload('{method:"serverRequest/resolved",params:{threadId:$thread,requestId:91}}',thread=self.native_thread))
        self.wait_status('resolved',handle)
        self.assertEqual(self.decide(handle)[0],400)
        self.assertFalse(self.rpc._server_pending)
        self.assertFalse(select.select([self.peer],[],[],.1)[0])

    def test_declined_item_completion_retains_decision_in_projection(self):
        handle = self.native()
        self.assertEqual(self.decide(handle, 'decline')[0], 200)
        _, response = frame(self.peer)
        self.assertEqual(query(response, '.result.decision'), 'decline')
        self.wait_status('response_sent', handle)
        send(self.peer, payload('{method:"item/completed",params:{threadId:$thread,turnId:"turn-1",item:{id:"item-1",status:"declined"}}}', thread=self.native_thread))
        self.wait_status('completed', handle)
        self.assertEqual(query(self.listing(), '.requests[] | select(.handle=="'+handle+'") | .decision'), 'decline')

    def test_correlated_command_failure_is_preserved_in_projection(self):
        handle = self.native()
        self.assertEqual(self.decide(handle, 'accept')[0], 200)
        frame(self.peer)
        self.wait_status('response_sent', handle)
        send(self.peer, payload('{method:"item/completed",params:{threadId:$thread,turnId:"turn-1",item:{id:"item-1",type:"commandExecution",status:"failed",exitCode:7}}}', thread=self.native_thread))
        self.wait_status('completed', handle)
        self.assertEqual(query(self.listing(), '.requests[] | select(.handle=="'+handle+'") | .item_outcome.status'), 'failed')
        self.assertEqual(query(self.listing(), '.requests[] | select(.handle=="'+handle+'") | .item_outcome.exitCode'), '7')
        outcome = {'type': 'commandExecution', 'status': 'failed', 'exitCode': 7}
        self.state.call('approval-finish', epoch=self.pump.epoch, handle=handle, outcome='completed', item_outcome=outcome)
        with self.assertRaises(RemoteCoordError):
            self.state.call('approval-finish', epoch=self.pump.epoch, handle=handle, outcome='completed', item_outcome={'type':'fileChange','status':'completed'})
        with self.assertRaises(RemoteCoordError):
            self.state.call('approval-finish', epoch=self.pump.epoch, handle=handle, outcome='completed', item_outcome={'type':'commandExecution','status':'completed','exitCode':0})
        self.assertEqual(query(self.listing(), '.requests[] | select(.handle=="'+handle+'") | .item_outcome.exitCode'), '7')

    def test_terminal_replay_can_enrich_previously_unknown_item_outcome(self):
        handle = self.native()
        self.assertEqual(self.decide(handle, 'accept')[0], 200)
        frame(self.peer)
        self.wait_status('response_sent', handle)
        send(self.peer, payload('{method:"item/completed",params:{threadId:$thread,turnId:"turn-1",item:{id:"item-1"}}}', thread=self.native_thread))
        self.wait_status('completed', handle)
        self.state.call('approval-finish', epoch=self.pump.epoch, handle=handle, outcome='completed', item_outcome={'type':'commandExecution','status':'failed','exitCode':7})
        self.assertEqual(query(self.listing(), '.requests[] | select(.handle=="'+handle+'") | .item_outcome.status'), 'failed')

    def check_command_item_status(self, status, code):
        handle = self.native()
        self.assertEqual(self.decide(handle, 'accept')[0], 200)
        frame(self.peer)
        self.wait_status('response_sent', handle)
        item = '{id:"item-1",type:"commandExecution",status:$status' + (',exitCode:($code|tonumber)' if code is not None else '') + '}'
        send(self.peer, payload('{method:"item/completed",params:{threadId:$thread,turnId:"turn-1",item:'+item+'}}', thread=self.native_thread, status=status, code=code))
        self.wait_status('completed', handle)
        row = '.requests[] | select(.handle=="'+handle+'")'
        if status == 'cancelled':
            self.assertEqual(query(self.listing(), row+' | (.item_outcome == null)'), 'true')
        else:
            self.assertEqual(query(self.listing(), row+' | .item_outcome.status'), status)
            if code is not None:
                self.assertEqual(query(self.listing(), row+' | .item_outcome.exitCode'), str(code))

    def test_provider_declined_item(self): self.check_command_item_status('declined', None)
    def test_provider_success_item(self): self.check_command_item_status('completed', 0)
    def test_provider_nonzero_exit_item(self): self.check_command_item_status('completed', 3)
    def test_unrecognized_item_status_stays_unknown(self): self.check_command_item_status('cancelled', None)

    def test_unbound_thread_cannot_publish_or_approve(self):
        send(self.peer,payload('{id:91,method:$method,params:{threadId:$thread,turnId:"t",itemId:"i"}}',method=METHODS[0],thread=str(uuid.uuid4())))
        deadline=time.monotonic()+2
        while self.rpc._sock is not None and time.monotonic()<deadline:time.sleep(.02)
        self.assertIsNone(self.rpc._sock);self.assertEqual(query(self.listing(),'.requests|length'),'0')

    def test_lease_release_invalidates_pending(self):
        handle=self.native();self.state.call('release')
        self.wait_status('disconnected',handle);self.assertEqual(self.decide(handle)[0],400)
        self.state.call('acquire')

    def test_concurrent_decisions_are_one_use(self):
        handle=self.native();barrier=threading.Barrier(3);outcomes=[]
        def decide():
            barrier.wait();outcomes.append(self.decide(handle,'cancel')[0])
        for _ in range(2):
            worker=threading.Thread(target=decide);self.workers.append(worker);worker.start()
        barrier.wait()
        for worker in self.workers:worker.join(2)
        self.assertEqual(sorted(outcomes),[200,400])
        _,raw=frame(self.peer);self.assertEqual(query(raw,'.result.decision'),'cancel')
        self.assertFalse(select.select([self.peer],[],[],.2)[0])

    def test_server_restart_does_not_recover_pending(self):
        handle=self.native();self.pump.close();self.rpc.close();self.peer.close()
        self.stop();self.start()
        self.state=RemoteBridgeState(self.client,self.url,self.identity);self.state.call('acquire')
        self.assertEqual(query(self.listing(),'.requests|length'),'0')
        self.assertEqual(self.decide(handle)[0],400)
        self.connect();fresh=self.native();self.assertNotEqual(handle,fresh)
        self.assertEqual(self.decide(fresh)[0],200);self.assertEqual(query(frame(self.peer)[1],'.id'),'91')

    def test_pump_waits_for_delayed_real_tcp_handshake_and_native_initialization(self):
        self.pump.close();self.rpc.close();self.peer.close()
        listener=socket.socket();listener.bind(('127.0.0.1',0));listener.listen(1);listener.settimeout(3)
        result={};ready=threading.Event();finished=threading.Event()
        def provider():
            try:
                peer,_=listener.accept();self.peer=peer;peer.settimeout(3)
                request=b''
                while b'\r\n\r\n' not in request:request+=peer.recv(1024)
                key=next(line.split(b':',1)[1].strip() for line in request.split(b'\r\n') if line.lower().startswith(b'sec-websocket-key:'))
                accept=base64.b64encode(hashlib.sha1(key+b'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest())
                time.sleep(.25)
                handshake=b'HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+accept+b'\r\n\r\n'
                peer.sendall(handshake[:19]);time.sleep(.05);peer.sendall(handshake[19:])
                _,initialize=frame(peer);rid=query(initialize,'.id')
                send(peer,payload('{id:($id|tonumber),result:{userAgent:"controlled-provider"}}',id=rid))
                frame(peer) # initialized notification
                ready.set();finished.wait(4)
            except Exception as error:result['error']=repr(error);ready.set()
            finally:listener.close()
        self.rpc=RpcClient('ws://127.0.0.1:'+str(listener.getsockname()[1]),timeout=3,server_request_methods=METHODS,server_request_timeout=3)
        self.pump=ApprovalPump(self.rpc,lambda:(self.state,)).start()
        worker=threading.Thread(target=provider);self.workers.append(worker);worker.start()
        try:
            self.rpc.connect();self.assertTrue(ready.wait(2));self.assertEqual(result,{})
            handle=self.native();self.assertEqual(self.decide(handle)[0],200)
            self.assertEqual(query(frame(self.peer)[1],'.result.decision'),'accept')
        finally:finished.set();worker.join(3)

if __name__=='__main__':unittest.main(verbosity=2)
