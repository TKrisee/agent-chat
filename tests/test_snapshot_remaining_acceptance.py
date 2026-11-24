"""Current App-owned snapshot interruption, Root revocation and finite memory."""
import contextlib
import hashlib
import resource
import socket
import subprocess
import sys
import uuid
from pathlib import Path
from unittest import mock
import unittest
from agent_chat import core, remote_service
from agent_chat.web import Handler
from test_exact_file_binding import ExactFileBindingTests


class SnapshotRemainingAcceptance(ExactFileBindingTests):
    def root_grant(self, limit):
        self.consume()
        binding=self.call('exact-file-bind',project=self.project_id,
            **dict(self.binding_payload(),max_bytes=limit))
        self.assertEqual(binding['issuer_session'],self.root_projection)
        self.assertEqual(binding['owner_session'],self.worker)
        return binding

    def root_revoke(self,binding,reason):
        result=self.call('exact-file-revoke',project=self.project_id,binding_id=binding['id'],
            request_id=str(uuid.uuid4()),reason=reason,confirm=True)
        self.assertEqual(result['state'],'revoked')
        return result

    def outcome(self,binding,rid):
        return self.po_call('exact-file-result',session=self.worker,binding_id=binding['id'],
            path=str(self.source),reservation_id=rid)

    def assert_partial_outcome(self,binding,rid,receipt):
        subprocess.run(['jq','-e','.exit_code!=0'],input=receipt,text=True,capture_output=True,check=True)
        self.assertFalse(self.output.exists())
        self.assertFalse(list(self.output.parent.glob('.agent-chat-snapshot-*')))
        result=self.outcome(binding,rid)
        self.assertEqual(result['execution']['state'],'pending')
        self.assertFalse(result['receipt_backed_clear'])
        context=self.po_call('exact-file-guard-context',session=self.worker,binding_id=binding['id'],path=str(self.source))
        self.assertEqual(len(context['runs']),1)
        # One actual failed executor is retained; no false release, retry,
        # produced/completed record, or global process closure is inferred.
        self.assertFalse(result['execution']['global_effects_closed'])
        return result

    def revoke_boundary(self,phase):
        size=4*65536+1
        self.source.write_bytes(b'x'*size)
        binding=self.root_grant(size);_,rid=self.acquire(binding)
        original=remote_service.dispatch;events=[];pulses=[]
        def dispatch(coord,body,**kwargs):
            if body.get('op')=='exact-file-pulse':
                pulses.append(body['params']['run_id']);self.assertLess(len(pulses),16)
                sizes=[p.stat().st_size for p in self.output.parent.glob('.agent-chat-snapshot-*')]
                eligible=len(sizes)==1 and (0<sizes[0]<size if phase=='progress' else sizes[0]==size)
                if not events and eligible:
                    self.assertFalse(self.output.exists())
                    events.append(sizes[0])
                    self.root_revoke(binding,'Root withdraws exact fixture at '+phase)
            return original(coord,body,**kwargs)
        with mock.patch.object(remote_service,'dispatch',side_effect=dispatch):
            receipt=self.copy(binding,rid,expected_rc=1)
        self.assertEqual(len(events),1,'actual staging boundary must be reached')
        if phase=='progress':self.assertTrue(0<events[0]<size)
        else:self.assertEqual(events[0],size)
        self.assert_partial_outcome(binding,rid,receipt)
        self.assertEqual(self.call('exact-file-binding-status',project=self.project_id,binding_id=binding['id'])['state'],'revoked')
        print('root_revocation_phase='+phase+' staged_bytes='+str(events[0])+' final_absent=true execution=pending custody_clear=false',flush=True)

    def test_root_revocation_at_actual_progress_prevents_publication(self):
        self.revoke_boundary('progress')

    def test_root_revocation_at_flushed_precommit_prevents_publication(self):
        # Odd-size tail is buffered until snapshot's flush/fsync; a full stage
        # therefore selects its final capability check, before the no-clobber link.
        self.revoke_boundary('precommit')

    def test_transport_interruption_after_partial_stage_does_not_complete_or_retry(self):
        size=4*65536+1;self.source.write_bytes(b'y'*size)
        binding=self.root_grant(size);_,rid=self.acquire(binding)
        original_dispatch=remote_service.dispatch;original_json=Handler.json
        armed=[];dropped=[]
        def dispatch(coord,body,**kwargs):
            if body.get('op')=='exact-file-pulse' and not armed:
                sizes=[p.stat().st_size for p in self.output.parent.glob('.agent-chat-snapshot-*')]
                if len(sizes)==1 and 0<sizes[0]<size:armed.extend(sizes)
            return original_dispatch(coord,body,**kwargs)
        def response(handler,code,value):
            if (armed and not dropped and code==200 and isinstance(value,dict)
                    and value.get('binding_id')==binding['id'] and value.get('reservation_id')==rid
                    and value.get('resource')==str(self.source) and value.get('state')=='owned'):
                dropped.append(True)
                handler.close_connection=True
                handler.connection.shutdown(socket.SHUT_RDWR);handler.connection.close()
                handler._finish_request(error=ConnectionResetError('owned fixture drops a committed progress pulse'))
                return
            return original_json(handler,code,value)
        with mock.patch.object(remote_service,'dispatch',side_effect=dispatch),mock.patch.object(Handler,'json',response):
            receipt=self.copy(binding,rid,expected_rc=1)
        self.assertEqual(dropped,[True]);self.assertEqual(len(armed),1)
        self.assertTrue(0<armed[0]<size)
        self.assert_partial_outcome(binding,rid,receipt)
        self.assertEqual(self.call('exact-file-binding-status',project=self.project_id,binding_id=binding['id'])['state'],'active')
        print('transport_interrupted_after_staged_bytes='+str(armed[0])+' final_absent=true one_run=true execution=pending custody_clear=false',flush=True)

    def test_current_typed_sixty_four_mib_path_has_bounded_memory(self):
        def measure(size):
            digest=hashlib.sha256();block=b'z'*65536
            with self.source.open('wb') as stream:
                remaining=size
                while remaining:
                    part=block[:min(remaining,len(block))];stream.write(part);digest.update(part);remaining-=len(part)
            binding=self.root_grant(size);_,rid=self.acquire(binding)
            copied=self.cli_call(self.worker,'exact-file-read','--binding-id',binding['id'],
                '--path',str(self.source),'--output',str(self.output),'--max-bytes',str(size),
                '--reservation-id',rid,'--capability-file',str(self.cap),
                '--process-evidence-file',str(self.output.parent/('process-'+binding['id']+'.jsonl')))
            subprocess.run(['jq','-e','.exit_code==0'],input=copied,text=True,capture_output=True,check=True)
            actual=hashlib.sha256()
            with self.output.open('rb') as stream:
                for part in iter(lambda:stream.read(65536),b''):actual.update(part)
            self.assertEqual(actual.hexdigest(),digest.hexdigest());self.assertEqual(self.output.stat().st_size,size)
            outcome=self.outcome(binding,rid)
            self.assertEqual(outcome['execution']['state'],'completed')
            self.assertEqual(outcome['execution']['bytes'],size)
            self.assertEqual(outcome['execution']['sha256'],digest.hexdigest())
            self.assertFalse(outcome['receipt_backed_clear'])
            return outcome['execution']['generation']
        small_generation=measure(262145)
        multiplier=1 if sys.platform=='darwin' else 1024
        small_child=resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss*multiplier
        small_parent=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*multiplier
        self.source=self.fixture/'large-source.bin';self.output=self.fixture/'output'/'large-copy.bin'
        self.cap=self.fixture/'output'/'large-capability.json';self.binding_request=str(uuid.uuid4())
        large_generation=measure(64*1024*1024)
        child=resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss*multiplier
        parent=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*multiplier
        self.assertNotEqual(small_generation,large_generation)
        self.assertGreater(small_child,0)
        self.assertLess(child-small_child,16*1024*1024,'child allocation must not scale by the64MiB payload')
        self.assertLess(parent-small_parent,16*1024*1024,'CLI/server must not buffer the64MiB payload')
        print('typed_memory_small_bytes=262145 typed_memory_large_bytes=67108864 small_reaped_child_max_rss_bytes='+str(small_child)
            +' large_reaped_child_max_rss_bytes='+str(child)+' parent_peak_growth_bytes='+str(parent-small_parent)
            +' scope=reaped_fixture_children_not_exact_executor_pid_peak',flush=True)


def load_tests(loader,tests,pattern):
    return unittest.TestSuite(SnapshotRemainingAcceptance(name) for name in SnapshotRemainingAcceptance.__dict__ if name.startswith('test_'))
