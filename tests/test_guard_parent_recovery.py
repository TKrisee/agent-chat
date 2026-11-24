import os
import hashlib
from pathlib import Path
import platform
import subprocess
import sys
import time
import unittest
from agent_chat.guard_witness import observe_file
from agent_chat.core import CoordError
from agent_chat.remote import RemoteCoordError
from test_guard_file_evidence import DeclaredFileEvidence

class ParentRecovery(DeclaredFileEvidence):
    @unittest.skipUnless(platform.system() == 'Darwin', 'Darwin host witness required')
    def test_lost_parent_restoration_recovery_contract(self):
        target, cap, rid = self.claim_file()
        journal = self.fixture/'output'/'parent-loss.jsonl'
        wrapper = self.fixture/'exit-after-gate.py'
        wrapper.write_text('import os,runpy,sys\nold=os.write\ndef write(fd,data):\n result=old(fd,data)\n if data==b"G": os._exit(98)\n return result\nos.write=write\nsys.argv=["agent_chat.cli",*sys.argv[1:]]\nrunpy.run_module("agent_chat.cli",run_name="__main__")\n')
        env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1]/'src'))
        command=[sys.executable,str(wrapper),'--server',self.url,'--api-token',self.po_client.token,'--project',self.project_id,'--session',self.worker,'run',
                 '--require-host-witness','--declared-task-id',self.task['id'],'--declared-source-id',self.assignment,
                 '--capability-file',str(cap),'--reservation-id',rid,'--process-evidence-file',str(journal),'file:guard-file.txt','--',
                 '/bin/sh','-c','printf "changed\\n" > "$1"; sleep 4; printf "baseline\\n" > "$1"','fixture',str(target)]
        proc = subprocess.Popen(command,env=env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        self.assertEqual(proc.wait(timeout=10),98)
        deadline=time.monotonic()+1.5
        while target.read_bytes()!=b'changed\n' and time.monotonic()<deadline: time.sleep(.02)
        self.assertEqual(target.read_bytes(),b'changed\n')
        pid=int(subprocess.run(['jq','-er','select(.event=="launcher_created") | .child_pgid',str(journal)],text=True,capture_output=True,check=True).stdout.strip())
        os.killpg(pid,0)  # Read-only: the actual original group is still present here.
        status=self.cli_call(self.worker,'guard-status','file:guard-file.txt','--reservation-id',rid)
        run_id=subprocess.run(['jq','-er','.runs[0].run_id'],input=status,text=True,capture_output=True,check=True).stdout.strip()
        token=subprocess.run(['jq','-er','.token',str(cap)],text=True,capture_output=True,check=True).stdout.strip()
        self.po_call('guard-file-observe',session=self.worker,run_id=run_id,token=token,stage='changed',observation=observe_file(str(target)))
        manifest=self.fixture/'output'/'manifest.json'
        with manifest.open('w') as stream:
            subprocess.run(['jq','-c','[{resource,session_id,host_id,reservation_id,token}]',str(cap)],stdout=stream,check=True)
        os.chmod(manifest,0o600)
        with self.assertRaisesRegex(CoordError,'finalizer input or prospective journal is invalid'):
            self.cli_call(self.worker,'guard-finalize','--batch-manifest',str(manifest),'--process-evidence-file',str(journal),'--result-file',str(self.fixture/'output'/'live-final.jsonl'))
        os.killpg(pid,0)
        self.status_file(rid,'.runs[0] | (.closed_at == null) and (.file_evidence.process_closure == "unknown")')
        deadline=time.monotonic()+5
        while target.read_bytes()!=b'baseline\n' and time.monotonic()<deadline: time.sleep(.05)
        self.assertEqual(target.read_bytes(),b'baseline\n')
        deadline=time.monotonic()+5
        while time.monotonic()<deadline:
            try: os.killpg(pid,0)
            except ProcessLookupError: break
            time.sleep(.05)
        else: self.fail('fixture group did not disappear')
        try: os.kill(pid,0)
        except ProcessLookupError: pass
        else: self.fail('original PID is still present or reused')
        self.status_file(rid,'.runs[0] | (.closed_at == null) and (.file_evidence.state == "unknown") and (.file_evidence.process_closure == "unknown")')
        output=self.fixture/'output'
        sanitized=subprocess.run(['jq','-cs','map({event,sequence,observed_at,child_pid,child_pgid,gate_released,boot_id,birth_sec,birth_usec})',str(journal)],capture_output=True,check=True).stdout
        (output/'sanitized-journal.json').write_bytes(sanitized)
        current=self.cli_call(self.worker,'guard-status','file:guard-file.txt','--reservation-id',rid)
        safe=subprocess.run(['jq','-c','.runs[0] | {closed_at,witness_provenance,file_evidence}'],input=current,text=True,capture_output=True,check=True).stdout
        (output/'sanitized-status.json').write_text(safe)
        # The supported finalizer should be able to reconcile this exact ended run.
        with self.assertRaisesRegex(CoordError,'finalizer input or prospective journal is invalid'):
            self.cli_call(self.worker,'guard-finalize','--batch-manifest',str(manifest),'--process-evidence-file',str(journal),'--result-file',str(self.fixture/'output'/'final.jsonl'))
        with self.assertRaisesRegex(CoordError,'finalizer input or prospective journal is invalid'):
            self.cli_call(self.worker,'guard-recover-declared','--capability-file',str(cap),'--reservation-id',rid,'--run-id',run_id,'--process-evidence-file',str(journal),'--result-file',str(self.fixture/'output'/'new-final.jsonl'))
        context=self.po_call('guard-context',session=self.worker,resource='file:guard-file.txt',token=token)
        original=next(row for row in context['runs'] if row['run_id']==run_id)
        proof={key:original[key] for key in ('pid','parent_pid','pgid','birth_sec','birth_usec','boot_id')}
        proof.update(observed_at=time.time(),leader_absent=True,parent_absent=True,group_absent=True)
        with self.assertRaisesRegex(RemoteCoordError,'not prospectively prepared'):
            self.po_call('guard-recovery-settle',session=self.worker,run_id=run_id,token=token,
                         evidence_sha256=hashlib.sha256(journal.read_bytes()).hexdigest(),observation=observe_file(str(target)),process_observation=proof)
        self.status_file(rid,'.runs[0] | (.closed_at == null) and (.file_evidence.state == "unknown")')

def load_tests(loader,tests,pattern):
    return unittest.TestSuite([ParentRecovery('test_lost_parent_restoration_recovery_contract')])
