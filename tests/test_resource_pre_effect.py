"""Private fault probe: client dies at the existing inert launcher gate."""
import os
import platform
from pathlib import Path
import subprocess
import sys
import unittest
from test_remote_web import TOKEN
from test_unused_return_repro import UnusedReturnProbe, jq


class PreEffectProbe(UnusedReturnProbe):
    @unittest.skipUnless(platform.system() == 'Darwin', 'Darwin kernel witness required')
    def test_parent_exit_before_gate_lacks_durable_process_identity(self):
        status, raw, _ = self.request('POST', '/api/coord', {'op':'register','session':'worker','host_id':'mac','params':{'agent':'worker'}})
        self.assertEqual(status, 200, raw)
        root = Path(self.tmp.name); cap = root / 'capability.json'; journal = root / 'journal.jsonl'; marker = root / 'effect'
        env = dict(os.environ, AGENT_CHAT_HOST_ID='mac', PYTHONPATH=str(Path(__file__).resolve().parents[1] / 'src'))
        base = [sys.executable, '-m', 'agent_chat.cli', '--server', self.url, '--api-token', TOKEN, '--project', 'default', '--session', 'worker']
        acquired = subprocess.run(base + ['request','fixture:gate','--minutes','5','--capability-file',str(cap)], env=env, capture_output=True, timeout=8)
        self.assertEqual(acquired.returncode, 0, acquired.stderr.decode())
        rid = jq(cap.read_bytes(), '.reservation_id')
        wrapper = root / 'abort_before_gate.py'
        wrapper.write_text('import os,runpy,sys\nold=os.write\ndef write(fd,data):\n if data==b"G": os._exit(97)\n return old(fd,data)\nos.write=write\nsys.argv=["agent_chat.cli",*sys.argv[1:]]\nrunpy.run_module("agent_chat.cli",run_name="__main__")\n')
        invocation = [sys.executable, str(wrapper), *base[3:], 'run','--require-host-witness','--capability-file',str(cap),'--reservation-id',rid,'--process-evidence-file',str(journal),'fixture:gate','--','/usr/bin/touch',str(marker)]
        result = subprocess.run(invocation, env=env, capture_output=True, timeout=12)
        self.assertEqual(result.returncode,97,result.stderr.decode())
        self.assertFalse(marker.exists())
        status, raw, _ = self.request('POST','/api/coord',{'op':'guard-context','session':'worker','host_id':'mac','params':{'resource':'fixture:gate'}})
        self.assertEqual(status,200,raw)
        self.assertEqual(jq(raw,'.reservation_id'), rid)
        self.assertEqual(jq(raw,'.runs | length'),'1')
        self.assertEqual(jq(raw,'.runs[0] | (.local_pid > 0) and (.closed_at == null)'), 'true')
        self.assertEqual(jq(raw,'.runs[0] | has("birth_sec") and has("boot_id") and has("executable")'), 'true')

    def prepare_run(self, label):
        status, raw, _ = self.request('POST', '/api/coord', {'op':'register','session':'worker','host_id':'mac','params':{'agent':'worker'}})
        self.assertEqual(status, 200, raw)
        root = Path(self.tmp.name); cap = root / 'capability.json'; journal = root / 'journal.jsonl'
        env = dict(os.environ, AGENT_CHAT_HOST_ID='mac', PYTHONPATH=str(Path(__file__).resolve().parents[1] / 'src'))
        base = [sys.executable, '-m', 'agent_chat.cli', '--server', self.url, '--api-token', TOKEN, '--project', 'default', '--session', 'worker']
        resource = 'fixture:' + label
        acquired = subprocess.run(base + ['request',resource,'--minutes','5','--capability-file',str(cap)], env=env, capture_output=True, timeout=8)
        self.assertEqual(acquired.returncode, 0, acquired.stderr.decode())
        return root, env, base, cap, journal, resource, jq(cap.read_bytes(), '.reservation_id')

    def context(self, resource):
        status, raw, _ = self.request('POST','/api/coord',{'op':'guard-context','session':'worker','host_id':'mac','params':{'resource':resource}})
        self.assertEqual(status, 200, raw)
        return raw

    @unittest.skipUnless(platform.system() == 'Darwin', 'Darwin kernel witness required')
    def test_completed_guard_keeps_witness_and_actual_restoration(self):
        root, env, base, cap, journal, resource, rid = self.prepare_run('complete')
        target = root / 'target'; target.write_text('baseline\n')
        script = root / 'child.sh'; script.write_text('#!/bin/sh\nprintf "changed\\n" > "$1"\nprintf "baseline\\n" > "$1"\n')
        result = subprocess.run(base + ['run','--require-host-witness','--capability-file',str(cap),'--reservation-id',rid,
                                '--process-evidence-file',str(journal),resource,'--','/bin/sh',str(script),str(target)],
                                env=env, capture_output=True, timeout=12)
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertEqual(target.read_text(), 'baseline\n')
        raw = self.context(resource)
        self.assertEqual(jq(raw, '.runs[0] | (.local_pid > 0) and (.closed_at != null) and (.witness_provenance == "host_reported")'), 'true')

    @unittest.skipUnless(platform.system() == 'Darwin', 'Darwin kernel witness required')
    def test_target_command_retains_its_requested_python_environment(self):
        root, env, base, cap, journal, resource, rid = self.prepare_run('target-env')
        startup = root / 'startup'; startup.mkdir(); marker = root / 'target-site'; output = root / 'target-output'
        (startup / 'sitecustomize.py').write_text('import sys\nfrom pathlib import Path\nif sys.argv[0]=="-c": Path(' + repr(str(marker)) + ').write_text("target startup")\n')
        env['PYTHONPATH'] = str(startup) + ':' + env['PYTHONPATH']
        result = subprocess.run(base + ['run','--require-host-witness','--capability-file',str(cap),'--reservation-id',rid,
                                '--process-evidence-file',str(journal),resource,'--',sys.executable,'-c',
                                'from pathlib import Path; Path(' + repr(str(output)) + ').write_text("target")'],
                                env=env, capture_output=True, timeout=12)
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertEqual(marker.read_text(), 'target startup')
        self.assertEqual(output.read_text(), 'target')
        count = subprocess.run(['jq', '-sr', '[.[] | select(.event=="gate_released")] | length', str(journal)],
                               capture_output=True, text=True, check=True).stdout.strip()
        self.assertEqual(count, '1')

    @unittest.skipUnless(platform.system() == 'Darwin', 'Darwin kernel witness required')
    def test_lost_attach_response_does_not_release_gate_or_close_guard(self):
        root, env, base, cap, journal, resource, rid = self.prepare_run('lost')
        marker = root / 'effect'; wrapper = root / 'lost_response.py'
        wrapper.write_text('import runpy,sys\nfrom agent_chat.remote import HttpClient,RemoteCoordError\nold=HttpClient.call\ndef call(self,path,payload,**kwargs):\n result=old(self,path,payload,**kwargs)\n if payload.get("op")=="attach-guard-witness": raise RemoteCoordError("injected lost response")\n return result\nHttpClient.call=call\nsys.argv=["agent_chat.cli",*sys.argv[1:]]\nrunpy.run_module("agent_chat.cli",run_name="__main__")\n')
        result = subprocess.run([sys.executable,str(wrapper),*base[3:],'run','--require-host-witness','--capability-file',str(cap),
                                '--reservation-id',rid,'--process-evidence-file',str(journal),resource,'--','/usr/bin/touch',str(marker)],
                                env=env,capture_output=True,timeout=12)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('injected lost response', result.stderr.decode())
        self.assertFalse(marker.exists())
        self.assertEqual(jq(self.context(resource), '.runs[0] | (.local_pid > 0) and (.closed_at == null) and (.witness_provenance == "host_reported")'), 'true')

    def test_unavailable_witness_does_not_release_gate(self):
        root, env, base, cap, journal, resource, rid = self.prepare_run('unavailable')
        marker = root / 'effect'; wrapper = root / 'unavailable.py'
        wrapper.write_text('import runpy,sys\nfrom agent_chat import guard_witness\nfrom agent_chat.core import CoordError\ndef unavailable(*args): raise CoordError("fixture witness unavailable")\nguard_witness.observe_launcher=unavailable\nsys.argv=["agent_chat.cli",*sys.argv[1:]]\nrunpy.run_module("agent_chat.cli",run_name="__main__")\n')
        result = subprocess.run([sys.executable,str(wrapper),*base[3:],'run','--require-host-witness','--capability-file',str(cap),
                                '--reservation-id',rid,'--process-evidence-file',str(journal),resource,'--','/usr/bin/touch',str(marker)],
                                env=env,capture_output=True,timeout=12)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('fixture witness unavailable', result.stderr.decode())
        self.assertFalse(marker.exists())
        self.assertEqual(jq(self.context(resource), '.runs[0] | (.local_pid == 0) and (.closed_at == null) and (has("witness_provenance") | not)'), 'true')

    def test_witness_required_run_rejects_legacy_attach(self):
        root, env, base, cap, journal, resource, rid = self.prepare_run('downgrade')
        token = jq(cap.read_bytes(), '.token')
        def call(op, params):
            return self.request('POST','/api/coord',{'op':op,'session':'worker','host_id':'mac','params':params})
        status, raw, _ = call('begin-guard', {'resource':resource, 'token':token, 'reservation_id':rid,
                                              'require_host_witness':True})
        self.assertEqual(status, 200, raw)
        run = jq(raw, '.run_id')
        status, raw, _ = call('attach-guard-pid', {'run_id':run,'token':token,'pid':os.getpid()})
        self.assertNotEqual(status, 200, raw)
        self.assertEqual(jq(self.context(resource), '.runs[0] | (.local_pid == 0) and (.closed_at == null)'), 'true')

    def test_witness_required_run_cannot_pulse_before_attach(self):
        root, env, base, cap, journal, resource, rid = self.prepare_run('early-pulse')
        token = jq(cap.read_bytes(), '.token')
        def call(op, params):
            return self.request('POST','/api/coord',{'op':op,'session':'worker','host_id':'mac','params':params})
        status, raw, _ = call('begin-guard', {'resource':resource,'token':token,'reservation_id':rid,'require_host_witness':True})
        self.assertEqual(status, 200, raw)
        status, raw, _ = call('guard-pulse', {'run_id':jq(raw,'.run_id'),'token':token})
        self.assertNotEqual(status, 200, raw)
        self.assertEqual(jq(self.context(resource), '.runs[0] | (.local_pid == 0) and (.closed_at == null)'), 'true')

    def test_witness_attach_rejects_wrong_host_token_and_invalid_identity(self):
        root, env, base, cap, journal, resource, rid = self.prepare_run('bad-identity')
        token = jq(cap.read_bytes(), '.token')
        def call(op, params, host='mac'):
            return self.request('POST','/api/coord',{'op':op,'session':'worker','host_id':host,'params':params})
        status, raw, _ = call('begin-guard', {'resource':resource,'token':token,'reservation_id':rid,'require_host_witness':True})
        self.assertEqual(status, 200, raw)
        run = jq(raw, '.run_id')
        # Deliberately synthetic values test rejection only; they are not process evidence.
        witness = dict(run_id=run,token=token,pid=123,parent_pid=124,pgid=123,uid=501,ruid=501,
                       birth_sec=1,birth_usec=1,boot_id='darwin-boot-1',executable='/fixture/python',executable_sha256='a'*64)
        for changed, host in ((witness, 'foreign'), (dict(witness,token='wrong'), 'mac'),
                              (dict(witness,pgid=999), 'mac')):
            status, _, _ = call('attach-guard-witness', changed, host)
            self.assertNotEqual(status, 200)
        self.assertEqual(jq(self.context(resource), '.runs[0] | (.local_pid == 0) and (.closed_at == null)'), 'true')

    @unittest.skipUnless(platform.system() == 'Darwin', 'Darwin kernel witness required')
    def test_changed_witness_after_attach_does_not_release_gate(self):
        root, env, base, cap, journal, resource, rid = self.prepare_run('changed')
        marker = root / 'effect'; wrapper = root / 'changed.py'
        wrapper.write_text('import runpy,sys\nfrom agent_chat import guard_witness\nold=guard_witness.observe_launcher\ncalls=0\ndef changed(*args):\n global calls\n calls+=1\n result=old(*args)\n if calls==2: result["birth_usec"]=(result["birth_usec"]+1)%1000000\n return result\nguard_witness.observe_launcher=changed\nsys.argv=["agent_chat.cli",*sys.argv[1:]]\nrunpy.run_module("agent_chat.cli",run_name="__main__")\n')
        result = subprocess.run([sys.executable,str(wrapper),*base[3:],'run','--require-host-witness','--capability-file',str(cap),
                                '--reservation-id',rid,'--process-evidence-file',str(journal),resource,'--','/usr/bin/touch',str(marker)],
                                env=env,capture_output=True,timeout=12)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('witness changed before gate release', result.stderr.decode())
        self.assertFalse(marker.exists())
        self.assertEqual(jq(self.context(resource), '.runs[0] | (.local_pid > 0) and (.closed_at == null) and (.witness_provenance == "host_reported")'), 'true')


def load_tests(loader, tests, pattern):
    return unittest.TestSuite(PreEffectProbe(name) for name in PreEffectProbe.__dict__ if name.startswith('test_'))
