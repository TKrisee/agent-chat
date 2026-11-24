"""Private structured file locator contracts; no live external path."""
import base64
import contextlib
import io
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch
from types import SimpleNamespace

from agent_chat.core import Coordinator, CoordError
from agent_chat.remote_service import dispatch, resource_name
from agent_chat.exact_file_read import snapshot
from agent_chat import cli

class ExactFileReadTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='exact-file-')
        self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name)
        self.db=self.root/'coord.sqlite3'
        self.src=self.root/'external path'/'file name.txt'
        self.src.parent.mkdir()
        self.src.write_bytes(b'spaced content')
        self.outdir=self.root/'output'; self.outdir.mkdir(mode=0o700)
        from agent_chat import exact_file_read
        for key,value in (('SELECTED_PATH',str(self.src)),('SELECTED_PROJECT','default'),('SELECTED_SESSION','a'),('SELECTED_HOST','host-a')):
            item=patch.object(exact_file_read,key,value); item.start(); self.addCleanup(item.stop)
        self.call('register',agent='fixture')

    def call(self,op,session='a',host='host-a',**params):
        with contextlib.closing(Coordinator(self.db)) as coord:
            return dispatch(coord,{'op':op,'session':session,'host_id':host,'params':params},canonical_project='default')

    def test_spaced_path_reservation_guard_snapshot_and_receipt(self):
        path=str(self.src)
        with self.assertRaises(CoordError): resource_name(path)
        claim=self.call('exact-file-request',path=path,minutes=5)
        self.assertEqual((claim['state'],claim['resource']),('owned',path))
        self.assertEqual(self.call('exact-file-result',path=path,reservation_id=claim['reservation_id'])['state'],'pending')
        with self.assertRaises(CoordError): self.call('exact-file-begin',session='b',host='host-b',path=path,token=claim['token'],reservation_id=claim['reservation_id'])
        with self.assertRaises(CoordError): self.call('exact-file-begin',host='host-b',path=path,token=claim['token'],reservation_id=claim['reservation_id'])
        guard=self.call('exact-file-begin',path=path,token=claim['token'],reservation_id=claim['reservation_id'])
        self.assertEqual(self.call('exact-file-pulse',path=path,run_id=guard['run_id'],token=claim['token'])['state'],'owned')
        with self.assertRaises(CoordError): self.call('exact-file-pulse',host='host-b',path=path,run_id=guard['run_id'],token=claim['token'])
        result=snapshot(path,str(self.outdir/'snapshot'),1024)
        self.assertEqual((self.outdir/'snapshot').read_bytes(),b'spaced content')
        self.assertTrue(result['snapshot_stable'])
        self.call('close-guard',run_id=guard['run_id'],token=claim['token'],evidence_sha256='a'*64)
        receipt=dict(version=1,resource=path,reservation_id=claim['reservation_id'],restored=True,processes_closed=True,closed_at=time.time(),evidence='private.txt',pids=[])
        self.assertTrue(self.call('exact-file-release',path=path,token=claim['token'],receipt=receipt,evidence_base64=base64.b64encode(b'private read fixture, no child').decode())['released'])
        self.assertTrue(self.call('exact-file-result',path=path,reservation_id=claim['reservation_id'])['receipt_backed_clear'])

    def test_symlink_traversal_size_and_replacement_fail_closed(self):
        with self.assertRaises(CoordError): self.call('exact-file-request',path=str(self.src.parent/'..'/'file'),minutes=5)
        link=self.root/'link'; link.symlink_to(self.src)
        with self.assertRaises(CoordError): snapshot(str(link),str(self.outdir/'a'),1024)
        parent=self.root/'linked parent'; parent.symlink_to(self.src.parent,target_is_directory=True)
        with self.assertRaises(CoordError): snapshot(str(parent/'file name.txt'),str(self.outdir/'b'),1024)
        outlink=self.root/'out link'; outlink.symlink_to(self.outdir,target_is_directory=True)
        with self.assertRaises(CoordError): snapshot(str(self.src),str(outlink/'z'),1024)
        with self.assertRaises(CoordError): snapshot(str(self.src),str(self.outdir/'c'),2)
        existing=self.outdir/'existing'; existing.write_bytes(b'keep')
        with self.assertRaises(CoordError): snapshot(str(self.src),str(existing),1024)
        self.assertEqual(existing.read_bytes(),b'keep'); existing.unlink()
        checks=[]
        def revoked():
            checks.append(1)
            if len(checks)==2: raise CoordError('guard changed')
        with self.assertRaises(CoordError): snapshot(str(self.src),str(self.outdir/'e'),1024,validate=revoked)
        original=os.read
        changed=False
        def read_then_replace(fd,n):
            nonlocal changed
            data=original(fd,n)
            if not changed:
                changed=True
                replacement=self.src.parent/'new'; replacement.write_bytes(b'other'); replacement.replace(self.src)
            return data
        with patch('agent_chat.exact_file_read.os.read',side_effect=read_then_replace):
            with self.assertRaises(CoordError): snapshot(str(self.src),str(self.outdir/'d'),1024)
        self.assertEqual(list(self.outdir.iterdir()),[])

    def test_queue_cancel_and_generic_validation_unchanged(self):
        path=str(self.src)
        self.call('register',session='b',host='host-b',agent='second')
        with self.assertRaises(CoordError): self.call('exact-file-request',session='b',host='host-b',path=path,minutes=5)
        with contextlib.closing(Coordinator(self.db)) as coord,coord.tx() as db:
            db.execute('INSERT INTO resource_queue(resource,session,queued_at) VALUES(?,?,?)',(path,'b',time.time()))
        queued=self.call('exact-file-request',path=path,minutes=5)
        self.assertEqual((queued['state'],queued['queue_position']),('queued',2))
        self.assertTrue(self.call('exact-file-cancel',path=path)['cancelled'])
        with self.assertRaises(CoordError): self.call('request',path=path,resource=path,minutes=5)
        with contextlib.closing(Coordinator(self.db)) as coord:
            with self.assertRaises(CoordError):
                dispatch(coord,{'op':'exact-file-request','session':'a','host_id':'host-a','params':{'path':path,'minutes':5}},canonical_project='other')

    def test_stale_same_owner_recovery_requires_exact_receipt(self):
        path=str(self.src)
        claim=self.call('exact-file-request',path=path,minutes=5)
        with contextlib.closing(Coordinator(self.db)) as coord,coord.tx() as db:
            db.execute('UPDATE resources SET stale=1 WHERE name=?',(path,))
        receipt=dict(version=1,resource=path,reservation_id='wrong',restored=True,processes_closed=True,closed_at=time.time(),evidence='private.txt',pids=[])
        values=dict(path=path,receipt=receipt,evidence_base64=base64.b64encode(b'genuine isolated fixture').decode())
        with self.assertRaises(CoordError): self.call('exact-file-recover',**values)
        receipt['reservation_id']=claim['reservation_id']
        with self.assertRaises(CoordError): self.call('exact-file-recover',host='host-b',**values)
        self.assertTrue(self.call('exact-file-recover',**values)['recovered'])

    def test_direct_helper_without_guard_context_reads_nothing(self):
        output=self.outdir/'unguarded'
        env=dict(os.environ)
        for name in ('AGENT_CHAT_SERVER','AGENT_CHAT_PROJECT','AGENT_CHAT_SESSION','AGENT_CHAT_HOST_ID','AGENT_CHAT_TOKEN','AGENT_CHAT_API_TOKEN'):
            env.pop(name,None)
        result=subprocess.run([sys.executable,'-m','agent_chat.exact_file_read','--path',str(self.src),'--output',str(output),'--max-bytes','100','--run-id','unowned','--reservation-id','unowned'],env=env,capture_output=True)
        self.assertNotEqual(result.returncode,0)
        self.assertFalse(output.exists())

    def test_real_http_cli_snapshot(self):
        from agent_chat.web import create_server
        token='private-test-token-01234567890123456789'
        server=create_server(self.db,port=0,api_token=token)
        worker=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':0.01})
        worker.start()
        def stop():
            server.shutdown(); worker.join(timeout=3); server.server_close()
            self.assertFalse(worker.is_alive())
        self.addCleanup(stop)
        class Logger:
            def event(self,*args,**kwargs): pass
            def error(self,*args,**kwargs): pass
        class Runtime:
            def logger(self,*args): return Logger()
        args=SimpleNamespace(session='a',api_token=token,project='default',root_scope=False)
        url='http://127.0.0.1:'+str(server.server_port)
        cap=self.outdir/'http-cap.json'; proof=self.outdir/'http-process.json'; out=self.outdir/'http-bytes'
        def run(items):
            stdout=io.StringIO()
            with contextlib.redirect_stdout(stdout):
                result=cli._remote_main(items,args,url,Runtime())
            self.assertEqual(result,0)
            return stdout.getvalue()
        with patch('agent_chat.remote.client_host_id',return_value='host-a'):
            run(['exact-file-request','--path',str(self.src),'--minutes','5','--capability-file',str(cap)])
            rid=subprocess.run(['jq','-er','.reservation_id',str(cap)],capture_output=True,text=True,check=True).stdout.strip()
            wrong=SimpleNamespace(session='a',api_token=token,project='other-project',root_scope=False)
            with self.assertRaises(CoordError):
                cli._remote_main(['exact-file-read','--path',str(self.src),'--reservation-id',rid,'--capability-file',str(cap),'--output',str(out),'--max-bytes','100','--process-evidence-file',str(proof)],wrong,url,Runtime())
            self.assertFalse(out.exists())
            run(['exact-file-read','--path',str(self.src),'--reservation-id',rid,'--capability-file',str(cap),'--output',str(out),'--max-bytes','100','--process-evidence-file',str(proof)])
            self.assertEqual(out.read_bytes(),b'spaced content')
            ev=self.outdir/'http-proof.txt'; ev.write_text('private guarded read complete')
            receipt=self.outdir/'http-receipt.json'
            with receipt.open('w') as stream:
                subprocess.run(['jq','-cn','--arg','resource',str(self.src),'--arg','rid',rid,'--arg','proof',str(ev),'--argjson','closed',str(time.time()),'{version:1,resource:$resource,reservation_id:$rid,restored:true,processes_closed:true,closed_at:$closed,evidence:$proof,pids:[]}'],stdout=stream,check=True)
            result=run(['exact-file-release','--path',str(self.src),'--reservation-id',rid,'--capability-file',str(cap),'--receipt',str(receipt)])
            self.assertEqual(subprocess.run(['jq','-er','.released'],input=result,capture_output=True,text=True,check=True).stdout.strip(),'true')

if __name__=='__main__': unittest.main()
