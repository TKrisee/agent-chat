"""Private owned task fixture: witness currently lacks declared file evidence."""
import os
import hashlib
from pathlib import Path
import platform
import subprocess
import sys
import unittest
from unittest import mock
from test_exact_file_binding import ExactFileBindingTests
from agent_chat.remote import RemoteCoordError
from agent_chat.core import CoordError


class DeclaredFileEvidence(ExactFileBindingTests):
    def claim_file(self):
        target = self.workspace / 'guard-file.txt'; target.write_bytes(b'baseline\n')
        cap = self.fixture / 'output' / 'file-capability.json'
        acquired = self.cli_call(self.worker, 'request', 'file:guard-file.txt', '--minutes', '5', '--capability-file', str(cap))
        rid = subprocess.run(['jq','-er','.reservation_id'],input=acquired,text=True,capture_output=True,check=True).stdout.strip()
        return target, cap, rid

    def status_file(self, rid, expression, *args):
        status = self.cli_call(self.worker, 'guard-status', 'file:guard-file.txt', '--reservation-id', rid)
        checked = subprocess.run(['jq','-e',*args,expression],input=status,text=True,capture_output=True)
        self.assertEqual(checked.returncode,0,checked.stdout+checked.stderr)

    @unittest.skipUnless(platform.system() == 'Darwin', 'Darwin host witness required')
    def test_actual_guarded_file_restoration_needs_durable_baseline(self):
        target, cap, rid = self.claim_file()
        script = self.fixture / 'child.sh'
        script.write_text('#!/bin/sh\nprintf "changed\\n" > "$1"\nsleep 1.5\nprintf "baseline\\n" > "$1"\n')
        self.cli_call(self.worker, 'run', '--require-host-witness', '--declared-task-id', self.task['id'], '--declared-source-id', self.assignment, '--capability-file', str(cap), '--reservation-id', rid,
                      '--process-evidence-file', str(self.fixture / 'output' / 'file-journal.jsonl'), 'file:guard-file.txt', '--', '/bin/sh', str(script), str(target))
        self.assertEqual(target.read_bytes(), b'baseline\n')
        journal = self.fixture / 'output' / 'file-journal.jsonl'
        subprocess.run(['jq','-es','any(.event=="declared_file_final" and .group_closed==true and .returncode==0) and any(.event=="leader_reaped" and .returncode==0)',str(journal)],capture_output=True,check=True)
        self.status_file(rid, '.runs[0] | (.closed_at != null) and (.witness_provenance == "host_reported") and (.file_evidence.state == "restored_observed") and (.file_evidence.process_closure == "host_reported_closed") and (.file_evidence.baseline == .file_evidence.final) and (.file_evidence.baseline.sha256 == $hash) and (.file_evidence.task_id == $task) and (.file_evidence.source_id == $source)', '--arg','hash',hashlib.sha256(b'baseline\n').hexdigest(),'--arg','task',self.task['id'],'--arg','source',self.assignment)

    @unittest.skipUnless(platform.system() == 'Darwin', 'Darwin host witness required')
    def test_wrong_source_denied_before_guard_or_child(self):
        target, cap, rid = self.claim_file(); marker = self.fixture / 'effect'
        with self.assertRaisesRegex(RemoteCoordError, 'exact current runnable task'):
            self.cli_call(self.worker, 'run', '--require-host-witness', '--declared-task-id', self.task['id'], '--declared-source-id', 'message_wrong', '--capability-file', str(cap), '--reservation-id', rid,
                          '--process-evidence-file', str(self.fixture / 'output' / 'denied.jsonl'), 'file:guard-file.txt', '--', '/usr/bin/touch', str(marker))
        self.assertFalse(marker.exists())
        self.status_file(rid, '.runs | length == 0')

    @unittest.skipUnless(platform.system() == 'Darwin', 'Darwin host witness required')
    def test_hidden_declared_file_is_rejected_before_guard(self):
        target = self.workspace / '.private'; target.write_bytes(b'private')
        cap = self.fixture / 'output' / 'hidden-capability.json'; marker = self.fixture / 'effect'
        acquired = self.cli_call(self.worker, 'request', 'file:.private', '--minutes','5','--capability-file',str(cap))
        rid = subprocess.run(['jq','-er','.reservation_id'],input=acquired,text=True,capture_output=True,check=True).stdout.strip()
        with self.assertRaisesRegex(RemoteCoordError, 'hidden path components'):
            self.cli_call(self.worker,'run','--require-host-witness','--declared-task-id',self.task['id'],'--declared-source-id',self.assignment,
                          '--capability-file',str(cap),'--reservation-id',rid,'--process-evidence-file',str(self.fixture/'output'/'hidden.jsonl'),
                          'file:.private','--','/usr/bin/touch',str(marker))
        self.assertFalse(marker.exists()); self.assertEqual(target.read_bytes(),b'private')
        status=self.cli_call(self.worker,'guard-status','file:.private','--reservation-id',rid)
        subprocess.run(['jq','-e','.runs | length == 0'],input=status,text=True,capture_output=True,check=True)

    @unittest.skipUnless(platform.system() == 'Darwin', 'Darwin host witness required')
    def test_parent_exit_before_gate_preserves_baseline_and_unknown(self):
        target, cap, rid = self.claim_file(); marker = self.fixture / 'effect'
        wrapper = self.fixture / 'exit-before-gate.py'
        wrapper.write_text('import os,runpy,sys\nold=os.write\ndef write(fd,data):\n if data==b"G": os._exit(97)\n return old(fd,data)\nos.write=write\nsys.argv=["agent_chat.cli",*sys.argv[1:]]\nrunpy.run_module("agent_chat.cli",run_name="__main__")\n')
        env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1]/'src'))
        command = [sys.executable,str(wrapper),'--server',self.url,'--api-token',self.po_client.token,'--project',self.project_id,'--session',self.worker,'run',
                   '--require-host-witness','--declared-task-id',self.task['id'],'--declared-source-id',self.assignment,'--capability-file',str(cap),
                   '--reservation-id',rid,'--process-evidence-file',str(self.fixture/'output'/'fault.jsonl'),'file:guard-file.txt','--','/usr/bin/touch',str(marker)]
        result = subprocess.run(command,env=env,capture_output=True,timeout=12)
        self.assertEqual(result.returncode,97,result.stderr.decode())
        self.assertFalse(marker.exists())
        self.status_file(rid, '.runs[0] | (.closed_at == null) and (.file_evidence.baseline != null) and (.file_evidence.final == null) and (.file_evidence.state == "unknown") and (.file_evidence.process_closure == "unknown")')

    @unittest.skipUnless(platform.system() == 'Darwin', 'Darwin host witness required')
    def test_parent_exit_after_gate_keeps_file_and_process_outcome_unknown(self):
        target, cap, rid = self.claim_file(); wrapper = self.fixture / 'exit-after-gate.py'
        wrapper.write_text('import os,runpy,sys\nold=os.write\ndef write(fd,data):\n result=old(fd,data)\n if data==b"G": os._exit(98)\n return result\nos.write=write\nsys.argv=["agent_chat.cli",*sys.argv[1:]]\nrunpy.run_module("agent_chat.cli",run_name="__main__")\n')
        env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1]/'src'))
        command = [sys.executable,str(wrapper),'--server',self.url,'--api-token',self.po_client.token,'--project',self.project_id,'--session',self.worker,'run',
                   '--require-host-witness','--declared-task-id',self.task['id'],'--declared-source-id',self.assignment,'--capability-file',str(cap),
                   '--reservation-id',rid,'--process-evidence-file',str(self.fixture/'output'/'after-gate.jsonl'),'file:guard-file.txt','--',
                   '/bin/sh','-c','printf changed > "$1"','fixture',str(target)]
        result = subprocess.run(command,env=env,capture_output=True,timeout=12)
        self.assertEqual(result.returncode,98,result.stderr.decode())
        self.assertEqual(target.read_bytes(),b'changed')
        self.status_file(rid, '.runs[0] | (.closed_at == null) and (.file_evidence.baseline != null) and (.file_evidence.final == null) and (.file_evidence.state == "unknown") and (.file_evidence.process_closure == "unknown")')

    @unittest.skipUnless(platform.system() == 'Darwin', 'Darwin host witness required')
    def test_file_changes_between_attach_and_gate_prevent_execution(self):
        target, cap, rid = self.claim_file(); marker = self.fixture / 'effect'
        wrapper = self.fixture / 'change-before-gate.py'
        wrapper.write_text('import runpy,sys\nfrom pathlib import Path\nfrom agent_chat.remote import HttpClient\nold=HttpClient.call\ndef call(self,path,payload,**kwargs):\n result=old(self,path,payload,**kwargs)\n if payload.get("op")=="attach-guard-witness": Path(' + repr(str(target)) + ').write_bytes(b"changed-before-gate\\n")\n return result\nHttpClient.call=call\nsys.argv=["agent_chat.cli",*sys.argv[1:]]\nrunpy.run_module("agent_chat.cli",run_name="__main__")\n')
        env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1]/'src'))
        command = [sys.executable,str(wrapper),'--server',self.url,'--api-token',self.po_client.token,'--project',self.project_id,'--session',self.worker,'run',
                   '--require-host-witness','--declared-task-id',self.task['id'],'--declared-source-id',self.assignment,'--capability-file',str(cap),
                   '--reservation-id',rid,'--process-evidence-file',str(self.fixture/'output'/'changed-before-gate.jsonl'),'file:guard-file.txt','--','/usr/bin/touch',str(marker)]
        result = subprocess.run(command,env=env,capture_output=True,timeout=12)
        self.assertNotEqual(result.returncode,0)
        self.assertIn('declared file changed before gate release',result.stderr.decode())
        self.assertFalse(marker.exists())
        self.status_file(rid, '.runs[0] | (.closed_at == null) and (.file_evidence.baseline != null) and (.file_evidence.state == "unknown")')

    @unittest.skipUnless(platform.system() == 'Darwin', 'Darwin host witness required')
    def test_final_changed_is_separate_from_process_closure(self):
        target, cap, rid = self.claim_file()
        self.cli_call(self.worker, 'run', '--require-host-witness', '--declared-task-id', self.task['id'], '--declared-source-id', self.assignment, '--capability-file', str(cap), '--reservation-id', rid,
                      '--process-evidence-file', str(self.fixture/'output'/'changed.jsonl'), 'file:guard-file.txt', '--', '/bin/sh', '-c', 'printf changed > "$1"', 'fixture', str(target))
        self.assertEqual(target.read_bytes(),b'changed')
        self.status_file(rid, '.runs[0] | (.file_evidence.state == "changed") and (.file_evidence.process_closure == "host_reported_closed")')

    @unittest.skipUnless(platform.system() == 'Darwin', 'Darwin host witness required')
    def test_missing_final_file_is_unknown_despite_closed_process(self):
        target, cap, rid = self.claim_file()
        self.cli_call(self.worker, 'run', '--require-host-witness', '--declared-task-id', self.task['id'], '--declared-source-id', self.assignment, '--capability-file', str(cap), '--reservation-id', rid,
                      '--process-evidence-file', str(self.fixture/'output'/'missing.jsonl'), 'file:guard-file.txt', '--', '/bin/rm', str(target))
        self.assertFalse(target.exists())
        self.status_file(rid, '.runs[0] | (.file_evidence.state == "unknown") and (.file_evidence.final == null) and (.file_evidence.process_closure == "host_reported_closed")')

    @unittest.skipUnless(platform.system() == 'Darwin', 'Darwin host witness required')
    def test_matching_bytes_without_observed_change_are_not_called_restored(self):
        target, cap, rid = self.claim_file()
        self.cli_call(self.worker, 'run', '--require-host-witness', '--declared-task-id', self.task['id'], '--declared-source-id', self.assignment, '--capability-file', str(cap), '--reservation-id', rid,
                      '--process-evidence-file', str(self.fixture/'output'/'matching.jsonl'), 'file:guard-file.txt', '--', '/usr/bin/true')
        self.assertEqual(target.read_bytes(), b'baseline\n')
        self.status_file(rid, '.runs[0] | (.file_evidence.state == "matches_baseline") and (.file_evidence.process_closure == "host_reported_closed")')

    def test_declared_run_cannot_pulse_before_file_recheck(self):
        from agent_chat.guard_witness import observe_file
        target, cap, rid = self.claim_file()
        token = subprocess.run(['jq','-er','.token',str(cap)],text=True,capture_output=True,check=True).stdout.strip()
        run = self.po_call('begin-guard',session=self.worker,resource='file:guard-file.txt',token=token,
                           reservation_id=rid,require_host_witness=True,declared_task_id=self.task['id'],declared_source_id=self.assignment)
        # Synthetic process tuple tests server admission only, never OS process evidence.
        self.po_call('attach-guard-witness',session=self.worker,run_id=run['run_id'],token=token,pid=123,parent_pid=124,
                     pgid=123,uid=501,ruid=501,birth_sec=1,birth_usec=1,boot_id='darwin-boot-1',
                     executable='/fixture/python',executable_sha256='a'*64,file_baseline=observe_file(str(target)))
        with self.assertRaisesRegex(RemoteCoordError, 'final requires closed guard'):
            self.po_call('guard-file-observe',session=self.worker,run_id=run['run_id'],token=token,stage='final',observation=observe_file(str(target)))
        with self.assertRaisesRegex(RemoteCoordError, 'file recheck'):
            self.po_call('guard-pulse',session=self.worker,run_id=run['run_id'],token=token)
        self.status_file(rid, '.runs[0] | (.closed_at == null) and (.file_evidence.state == "unknown")')

    def test_oversize_baseline_report_is_rejected_without_attachment(self):
        from agent_chat.guard_witness import observe_file
        from agent_chat.exact_file_read import MAX_BYTES
        target, cap, rid = self.claim_file()
        token = subprocess.run(['jq','-er','.token',str(cap)],text=True,capture_output=True,check=True).stdout.strip()
        run = self.po_call('begin-guard',session=self.worker,resource='file:guard-file.txt',token=token,
                           reservation_id=rid,require_host_witness=True,declared_task_id=self.task['id'],declared_source_id=self.assignment)
        baseline = observe_file(str(target)); baseline['size'] = MAX_BYTES + 1
        # Synthetic tuple checks rejection only and does not attest a process.
        with self.assertRaisesRegex(RemoteCoordError, 'invalid declared file digest or mode'):
            self.po_call('attach-guard-witness',session=self.worker,run_id=run['run_id'],token=token,pid=123,parent_pid=124,
                         pgid=123,uid=501,ruid=501,birth_sec=1,birth_usec=1,boot_id='darwin-boot-1',
                         executable='/fixture/python',executable_sha256='a'*64,file_baseline=baseline)
        self.status_file(rid, '.runs[0] | (.local_pid == 0) and (.closed_at == null) and (.file_evidence.baseline == null)')

    def test_file_parent_replacement_during_hash_is_unknown(self):
        from agent_chat.guard_witness import observe_file
        directory = self.workspace / 'subdir'; directory.mkdir()
        target = directory / 'target'; target.write_bytes(b'original')
        expected = target.stat(); original_read = os.read; changed = False
        def replace_after_read(fd, count):
            nonlocal changed
            data = original_read(fd, count)
            actual = os.fstat(fd)
            if data and not changed and (actual.st_dev,actual.st_ino) == (expected.st_dev,expected.st_ino):
                changed = True
                directory.rename(self.workspace / 'old-subdir')
                directory.mkdir(); target.write_bytes(b'replacement')
            return data
        with mock.patch('agent_chat.guard_witness.os.read', side_effect=replace_after_read):
            with self.assertRaisesRegex(CoordError, 'path changed while sampled'):
                observe_file(str(target))
        self.assertTrue(changed)

    @unittest.skipUnless(platform.system() == 'Darwin', 'Darwin host witness required')
    def test_declared_symlink_is_not_followed(self):
        external = self.fixture / 'outside'; external.write_bytes(b'outside')
        target = self.workspace / 'guard-file.txt'; target.symlink_to(external)
        cap = self.fixture / 'output' / 'file-capability.json'
        acquired = self.cli_call(self.worker, 'request', 'file:guard-file.txt', '--minutes', '5', '--capability-file', str(cap))
        rid = subprocess.run(['jq','-er','.reservation_id'],input=acquired,text=True,capture_output=True,check=True).stdout.strip()
        marker = self.fixture / 'effect'
        with self.assertRaisesRegex(CoordError, 'declared file observation unavailable'):
            self.cli_call(self.worker, 'run', '--require-host-witness', '--declared-task-id', self.task['id'], '--declared-source-id', self.assignment, '--capability-file', str(cap), '--reservation-id', rid,
                          '--process-evidence-file', str(self.fixture/'output'/'symlink.jsonl'), 'file:guard-file.txt', '--', '/usr/bin/touch', str(marker))
        self.assertFalse(marker.exists()); self.assertEqual(external.read_bytes(), b'outside')
        self.status_file(rid, '.runs[0] | (.closed_at == null) and (.file_evidence.state == "unknown") and (.file_evidence.baseline == null)')


def load_tests(loader, tests, pattern):
    return unittest.TestSuite(DeclaredFileEvidence(name) for name in DeclaredFileEvidence.__dict__ if name.startswith('test_'))
