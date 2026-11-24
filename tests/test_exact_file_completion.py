"""Request-bound copy completion is independent of reservation release."""
import hashlib
import subprocess
import unittest
import copy
from unittest import mock
from agent_chat import core, remote_service
from agent_chat.remote import RemoteCoordError
from test_exact_file_binding import ExactFileBindingTests

class ExactFileCompletionTests(ExactFileBindingTests):
    def result(self,binding,rid):
        return self.po_call('exact-file-result',session=self.worker,binding_id=binding['id'],
                            path=str(self.source),reservation_id=rid)

    def record_dispatch(self,records,hook=None):
        original=remote_service.dispatch
        def dispatched(coord,body,**kwargs):
            if body['op'].startswith('exact-file-'):
                records[body['op']]=copy.deepcopy(body)
            if hook:return hook(original,coord,body,kwargs)
            return original(coord,body,**kwargs)
        return mock.patch.object(remote_service,'dispatch',side_effect=dispatched)

    def replay(self,body):
        return self.po_client.call('/api/coord',body)

    def test_real_copy_completion_is_reported_while_custody_remains_held(self):
        self.source.write_bytes(b'z'*(8*1024*1024+1))
        binding=self.grant();_,rid=self.acquire(binding)
        copied=self.copy(binding,rid)
        run=subprocess.run(['jq','-er','.run_id'],input=copied,text=True,capture_output=True,check=True).stdout.strip()
        self.assertEqual(self.output.stat().st_size,8388609)
        self.assertEqual(hashlib.sha256(self.output.read_bytes()).digest(),hashlib.sha256(self.source.read_bytes()).digest())
        result=self.cli_call(self.worker,'exact-file-result','--binding-id',binding['id'],'--path',str(self.source),'--reservation-id',rid)
        checked=subprocess.run(['jq','-e','--arg','run',run,
            '.state=="pending" and .receipt_backed_clear==false and .execution.state=="completed" and .execution.run_id==$run and .execution.bytes==8388609'],input=result,text=True,capture_output=True)
        self.assertEqual(checked.returncode,0,result)

    def test_reports_are_exact_idempotent_and_scoped_without_release(self):
        binding=self.grant();_,rid=self.acquire(binding);records={}
        with self.record_dispatch(records):self.copy(binding,rid)
        completed=self.result(binding,rid)['execution']
        self.assertEqual(completed['task_id'],self.task['id'])
        self.assertEqual(completed['source_request_id'],self.assignment)
        self.assertEqual(completed['evidence_origin'],'scoped_host_report')
        self.assertFalse(completed['global_effects_closed'])
        self.assertFalse(completed['native_completion_proven'])
        for op in ('exact-file-produced','exact-file-complete'):
            self.assertEqual(self.replay(records[op])['recorded_at'],completed['recorded_at'])
        variants=[('session',self.po),('host_id','foreign-host'),
                  ('run_id','remote_run_absent'),('generation','wrong-generation'),
                  ('path',str(self.fixture/'other-source')),('output',str(self.fixture/'output'/'foreign')),
                  ('pid',completed['pid']+1),('sha256','0'*64),('bytes',completed['bytes']+1),
                  ('close_evidence_sha256','0'*64),('returncode',1),('group_closed',False)]
        for key,value in variants:
            with self.subTest(key=key):
                bad=copy.deepcopy(records['exact-file-complete'])
                (bad if key in ('session','host_id') else bad['params'])[key]=value
                with self.assertRaises(RemoteCoordError):self.replay(bad)
        forged=copy.deepcopy(records['exact-file-produced']);forged['params']['helper_sha256']='0'*64
        with self.assertRaisesRegex(RemoteCoordError,'generation'):self.replay(forged)
        final=self.result(binding,rid)
        self.assertEqual(final['execution'],completed)
        self.assertEqual(final['state'],'pending');self.assertFalse(final['receipt_backed_clear'])

    def test_missing_production_report_leaves_real_artifact_and_unknown_completion(self):
        binding=self.grant();_,rid=self.acquire(binding);records={}
        def loss(original,coord,body,kwargs):
            if body['op']=='exact-file-produced':raise core.CoordError('fixture lost production report before acceptance')
            return original(coord,body,**kwargs)
        with self.record_dispatch(records,loss):copied=self.copy(binding,rid,expected_rc=1)
        self.assertTrue(self.output.exists())
        result=self.result(binding,rid)
        self.assertEqual(result['execution']['state'],'pending')
        self.assertNotIn('exact-file-complete',records)
        self.assertFalse(result['receipt_backed_clear'])
        # A later report cannot fabricate a before-closure production event.
        with self.assertRaisesRegex(RemoteCoordError,'historical production'):
            self.replay(records['exact-file-produced'])

    def test_lost_completion_response_reconciles_exact_record_without_copy_replay(self):
        binding=self.grant();_,rid=self.acquire(binding);records={}
        def loss(original,coord,body,kwargs):
            result=original(coord,body,**kwargs)
            if body['op']=='exact-file-complete':raise core.CoordError('fixture response lost after durable completion')
            return result
        with self.record_dispatch(records,loss),self.assertRaisesRegex(RemoteCoordError,'response lost'):
            self.copy(binding,rid)
        before=self.output.stat()
        result=self.result(binding,rid)['execution']
        self.assertEqual(result['state'],'completed')
        self.assertEqual(self.replay(records['exact-file-complete']),result)
        self.assertEqual((self.output.stat().st_ino,self.output.stat().st_mtime_ns),(before.st_ino,before.st_mtime_ns))

    def test_changed_output_or_unacknowledged_close_cannot_complete(self):
        binding=self.grant();_,rid=self.acquire(binding);records={}
        def change(original,coord,body,kwargs):
            if body['op']=='close-guard':
                # Real production exists, but the guard is still open: exact
                # completion through the actual server function must reject.
                produced=records['exact-file-produced']['params']
                p={k:v for k,v in produced.items() if k not in ('helper_sha256','device','inode','mtime_ns')}
                p.update(returncode=0,group_closed=True,close_evidence_sha256=body['params']['evidence_sha256'])
                with self.assertRaisesRegex(core.CoordError,'acknowledged matching guard closure'):
                    original(coord,dict(op='exact-file-complete',session=self.worker,host_id='host',params=p),**kwargs)
                result=original(coord,body,**kwargs)
                self.output.write_bytes(b'changed output after child report')
                return result
            return original(coord,body,**kwargs)
        with self.record_dispatch(records,change),self.assertRaisesRegex(RemoteCoordError,'differs from typed production'):
            self.copy(binding,rid)
        result=self.result(binding,rid)
        self.assertEqual(result['execution']['state'],'produced')
        self.assertFalse(result['receipt_backed_clear'])

    def test_mismatched_completion_response_does_not_claim_client_success(self):
        binding=self.grant();_,rid=self.acquire(binding);records={}
        def mismatch(original,coord,body,kwargs):
            result=original(coord,body,**kwargs)
            if body['op']=='exact-file-complete':return dict(result,run_id='foreign-run')
            return result
        with self.record_dispatch(records,mismatch),self.assertRaisesRegex(core.CoordError,'response unresolved'):
            self.copy(binding,rid)
        self.assertEqual(self.result(binding,rid)['execution']['state'],'completed')
        self.assertTrue(self.output.exists())


def load_tests(loader,tests,pattern):
    return unittest.TestSuite(ExactFileCompletionTests(name) for name in ExactFileCompletionTests.__dict__ if name.startswith('test_'))
