import os
from pathlib import Path
import platform
import subprocess
import shutil
import sys
import time
import unittest
from agent_chat.guard_witness import observe_file
from agent_chat.core import CoordError
from agent_chat.remote import HttpClient, RemoteCoordError
from unittest import mock
from test_guard_file_evidence import DeclaredFileEvidence

class ProspectiveRecovery(DeclaredFileEvidence):
    @unittest.skipUnless(platform.system() == 'Darwin', 'Darwin host witness required')
    def exercise_recovery(self, variant):
        target, cap, rid = self.claim_file()
        baseline = observe_file(str(target))
        journal = self.fixture/'output'/'parent-loss.jsonl'
        wrapper = self.fixture/'exit-after-gate.py'
        wrapper.write_text('import os,runpy,sys\nold=os.write\ndef write(fd,data):\n result=old(fd,data)\n if data==b"G": os._exit(98)\n return result\nos.write=write\nsys.argv=["agent_chat.cli",*sys.argv[1:]]\nrunpy.run_module("agent_chat.cli",run_name="__main__")\n')
        env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1]/'src'))
        child = 'sleep 4' if variant=='no-change' else 'printf "changed\\n" > "$1"; sleep 4; printf "baseline\\n" > "$1"'
        command=[sys.executable,str(wrapper),'--server',self.url,'--api-token',self.po_client.token,'--project',self.project_id,'--session',self.worker,'run',
                 '--require-host-witness','--prospective-recovery','--declared-task-id',self.task['id'],'--declared-source-id',self.assignment,
                 '--capability-file',str(cap),'--reservation-id',rid,'--process-evidence-file',str(journal),'file:guard-file.txt','--',
                 '/bin/sh','-c',child,'fixture',str(target)]
        proc = subprocess.Popen(command,env=env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        self.assertEqual(proc.wait(timeout=10),98)
        deadline=time.monotonic()+1.5
        if variant!='no-change':
            while target.read_bytes()!=b'changed\n' and time.monotonic()<deadline: time.sleep(.02)
            self.assertEqual(target.read_bytes(),b'changed\n')
        pid=int(subprocess.run(['jq','-er','select(.event=="launcher_created") | .child_pgid',str(journal)],text=True,capture_output=True,check=True).stdout.strip())
        os.killpg(pid,0)  # Read-only: the actual original group is still present here.
        status=self.cli_call(self.worker,'guard-status','file:guard-file.txt','--reservation-id',rid)
        run_id=subprocess.run(['jq','-er','.runs[0].run_id'],input=status,text=True,capture_output=True,check=True).stdout.strip()
        token=subprocess.run(['jq','-er','.token',str(cap)],text=True,capture_output=True,check=True).stdout.strip()
        if variant=='direct-live':
            import hashlib
            with self.assertRaisesRegex(RemoteCoordError,'invalid declared recovery request'):
                self.po_call('guard-recovery-settle',session=self.worker,run_id=run_id,token=token,
                             evidence_sha256=hashlib.sha256(journal.read_bytes()).hexdigest(),observation=baseline)
        if variant!='no-change':
            self.po_call('guard-file-observe',session=self.worker,run_id=run_id,token=token,stage='changed',observation=observe_file(str(target)))
        manifest=self.fixture/'output'/'manifest.json'
        with manifest.open('w') as stream:
            subprocess.run(['jq','-c','[{resource,session_id,host_id,reservation_id,token}]',str(cap)],stdout=stream,check=True)
        os.chmod(manifest,0o600)
        with self.assertRaisesRegex(CoordError,'remains live or ambiguous'):
            self.cli_call(self.worker,'guard-recover-declared','--capability-file',str(cap),'--reservation-id',rid,'--run-id',run_id,'--process-evidence-file',str(journal),'--result-file',str(self.fixture/'output'/'live-final.jsonl'))
        os.killpg(pid,0)
        self.status_file(rid,'.runs[0] | (.closed_at == null) and (.file_evidence.process_closure == "unknown")')
        deadline=time.monotonic()+5
        while (target.read_bytes()!=b'baseline\n' or self._alive_group(pid)) and time.monotonic()<deadline: time.sleep(.05)
        self.assertEqual(target.read_bytes(),b'baseline\n')
        self.status_file(rid,'.runs[0] | (.closed_at == null) and (.file_evidence.state == "unknown") and (.file_evidence.process_closure == "unknown")')
        output=self.fixture/'output'
        sanitized=subprocess.run(['jq','-cs','map({event,sequence,observed_at,child_pid,child_pgid,gate_released,boot_id,birth_sec,birth_usec})',str(journal)],capture_output=True,check=True).stdout
        (output/'prospective-sanitized-journal.json').write_bytes(sanitized)
        current=self.cli_call(self.worker,'guard-status','file:guard-file.txt','--reservation-id',rid)
        safe=subprocess.run(['jq','-c','.runs[0] | {closed_at,witness_provenance,file_evidence}'],input=current,text=True,capture_output=True,check=True).stdout
        (output/'prospective-sanitized-status.json').write_text(safe)
        # The supported finalizer should be able to reconcile this exact ended run.
        args=('guard-recover-declared','--capability-file',str(cap),'--reservation-id',rid,'--run-id',run_id,'--process-evidence-file',str(journal),'--result-file',str(self.fixture/'output'/'final.jsonl'))
        if variant=='changed': target.write_bytes(b'other\n')
        if variant=='missing': target.unlink()
        if variant=='replaced': target.unlink(); target.write_bytes(b'baseline\n')
        if variant=='revoked':
            self.po_call('context'); self.po_call('activity-transition',work_id=self.task['id'],state='blocked')
        if variant=='boot':
            from agent_chat import capacity_observer
            with mock.patch.object(capacity_observer,'_boot_time',return_value=1):
                with self.assertRaisesRegex(CoordError,'generation differs'): self.cli_call(self.worker,*args)
        elif variant=='birth':
            original=HttpClient.call
            def drift(client,path,payload,**kwargs):
                value=original(client,path,payload,**kwargs)
                if payload.get('op')=='guard-context': value['runs'][0]['birth_usec'] += 1
                return value
            with mock.patch.object(HttpClient,'call',drift):
                with self.assertRaisesRegex(CoordError,'original generation differs'): self.cli_call(self.worker,*args)
        elif variant in ('wrongowner','wronghost','wrongrun'):
            import hashlib
            context=self.po_call('guard-context',session=self.worker,resource='file:guard-file.txt',token=token)
            original_run=next(row for row in context['runs'] if row['run_id']==run_id)
            process_observation={key:original_run[key] for key in ('pid','parent_pid','pgid','birth_sec','birth_usec','boot_id')}
            process_observation.update(observed_at=time.time(),leader_absent=True,parent_absent=True,group_absent=True)
            values=dict(run_id=('not-the-original-run' if variant=='wrongrun' else run_id),token=token,
                        evidence_sha256=hashlib.sha256(journal.read_bytes()).hexdigest(),observation=observe_file(str(target)),
                        process_observation=process_observation)
            with self.assertRaises(RemoteCoordError):
                self.po_call('guard-recovery-settle',session=(self.po if variant=='wrongowner' else self.worker),
                             host=('other-host' if variant=='wronghost' else 'host'),**values)
        elif variant in ('changed','missing','replaced','revoked'):
            with self.assertRaises(CoordError): self.cli_call(self.worker,*args)
        elif variant=='lost':
            original=HttpClient.call; calls=[]
            def lose(client,path,payload,**kwargs):
                value=original(client,path,payload,**kwargs)
                if payload.get('op')=='guard-recovery-settle':
                    calls.append(1); raise OSError('lost response after commit')
                return value
            with mock.patch.object(HttpClient,'call',lose): self.cli_call(self.worker,*args)
            self.assertEqual(len(calls),1)
        else:
            self.cli_call(self.worker,*args)
        if variant in ('changed','missing','replaced','revoked','boot','birth','wrongowner','wronghost','wrongrun'):
            self.status_file(rid,'.runs[0] | (.closed_at == null) and (.file_evidence.state == "unknown")')
        else:
            state='matches_baseline' if variant=='no-change' else 'restored_observed'
            self.status_file(rid,'.runs[0] | (.closed_at != null) and (.file_evidence.state == $state) and (.file_evidence.process_closure == "host_reported_closed")','--arg','state',state)
            import hashlib
            lines=(self.fixture/'output'/'final.jsonl').read_bytes().splitlines(keepends=True)
            expected=hashlib.sha256(journal.read_bytes()+b''.join(lines[:2])).hexdigest()
            self.status_file(rid,'.runs[0].close_evidence_sha256 == $digest','--arg','digest',expected)
            subprocess.run(['jq','-es','.[1] | .event=="group_absence_and_file_observed" and .process_observation.group_absent and .process_observation.parent_absent and .process_observation.leader_absent',str(self.fixture/'output'/'final.jsonl')],capture_output=True,check=True)
            if variant == 'lost':
                ctx=self.po_call('guard-context',session=self.worker,resource='file:guard-file.txt',token=token)
                closed=next(row for row in ctx['runs'] if row['run_id']==run_id)
                proof={key:closed[key] for key in ('pid','parent_pid','pgid','birth_sec','birth_usec','boot_id')}
                proof.update(observed_at=time.time(),leader_absent=True,parent_absent=True,group_absent=True)
                replay=self.po_call('guard-recovery-settle',session=self.worker,run_id=run_id,token=token,
                    evidence_sha256=expected,observation=closed['file_evidence']['final'],process_observation=proof)
                self.assertTrue(replay['reconciled'])
                after=self.po_call('guard-context',session=self.worker,resource='file:guard-file.txt',token=token)
                self.assertEqual(next(row for row in after['runs'] if row['run_id']==run_id)['closed_at'],closed['closed_at'])
            saved=self.cli_call(self.worker,'guard-status','file:guard-file.txt','--reservation-id',rid)
            safe=subprocess.run(['jq','-c','.runs[0] | {closed_at,close_evidence_sha256,witness_provenance,file_evidence}'],input=saved,text=True,capture_output=True,check=True).stdout
            (output/('final-status-'+variant+'.json')).write_text(safe)
            events=subprocess.run(['jq','-cs','map({event,observed_at,run_id,child_pid,child_pgid,parent_pid,birth_sec,birth_usec,boot_id,file_observation,process_observation,original_journal_sha256})',str(self.fixture/'output'/'final.jsonl')],capture_output=True,check=True).stdout
            (output/('recovery-evidence-'+variant+'.json')).write_bytes(events)
            if variant == 'release':
                status_path=self.fixture/'output'/'closed-status.json'
                status_path.write_text(self.cli_call(self.worker,'guard-status','file:guard-file.txt','--reservation-id',rid))
                proof=self.fixture/'output'/'observed-proof.json'
                with proof.open('w') as stream:
                    subprocess.run(['jq','-n','--slurpfile','original',str(journal),'--slurpfile','recovery',str(self.fixture/'output'/'final.jsonl'),
                                    '--slurpfile','status',str(status_path),'{original:$original,recovery:$recovery,status:$status}'],stdout=stream,check=True)
                receipt=self.fixture/'output'/'receipt.json'
                with receipt.open('w') as stream:
                    subprocess.run(['jq','-n','--arg','resource','file:guard-file.txt','--arg','reservation',rid,'--arg','evidence',str(proof),
                                    '--argjson','pid',str(pid),'--argjson','now',str(time.time()),
                                    '{version:1,resource:$resource,reservation_id:$reservation,restored:true,processes_closed:true,closed_at:$now,evidence:$evidence,pids:[$pid]}'],stdout=stream,check=True)
                result=self.cli_call(self.worker,'release','file:guard-file.txt','--receipt',str(receipt),'--token',token)
                subprocess.run(['jq','-e','.released == true'],input=result,text=True,capture_output=True,check=True)
                (output/'release-result.json').write_text(subprocess.run(['jq','-c','{released,resource}'],input=result,text=True,capture_output=True,check=True).stdout)
                cleared=self.po_call('reservation-result',session=self.worker,resource='file:guard-file.txt',reservation_id=rid)
                self.assertTrue(cleared['receipt_backed_clear'])
                for name, path in (('actual-journal.jsonl',journal),('actual-recovery.jsonl',self.fixture/'output'/'final.jsonl'),('actual-receipt.json',receipt),('actual-proof.json',proof)):
                    subprocess.run(['jq','-se','[.. | objects | keys[] | select(. == "token")] | length == 0',str(path)],capture_output=True,check=True)
                    shutil.copyfile(path,output/name)

    def test_recovered(self): self.exercise_recovery('ok')
    def test_lost_response_reconciles_without_replay(self): self.exercise_recovery('lost')
    def test_changed_file_refused(self): self.exercise_recovery('changed')
    def test_missing_file_refused(self): self.exercise_recovery('missing')
    def test_replaced_file_refused(self): self.exercise_recovery('replaced')
    def test_revoked_task_refused(self): self.exercise_recovery('revoked')
    def test_boot_mismatch_refused(self): self.exercise_recovery('boot')
    def test_birth_mismatch_refused(self): self.exercise_recovery('birth')
    def test_wrong_owner_refused(self): self.exercise_recovery('wrongowner')
    def test_wrong_host_refused(self): self.exercise_recovery('wronghost')
    def test_wrong_run_refused(self): self.exercise_recovery('wrongrun')
    def test_receipt_backed_release_after_observed_restoration(self): self.exercise_recovery('release')
    def test_live_direct_bare_settlement_refused(self): self.exercise_recovery('direct-live')
    def test_matching_final_without_change_does_not_claim_restoration(self): self.exercise_recovery('no-change')

    @staticmethod
    def _alive_group(pid):
        try: os.killpg(pid,0); return True
        except ProcessLookupError: return False

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(ProspectiveRecovery(name) for name in ProspectiveRecovery.__dict__ if name.startswith('test_'))
