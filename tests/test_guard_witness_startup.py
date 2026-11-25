from pathlib import Path
import subprocess
import sys
import unittest
from test_resource_pre_effect import PreEffectProbe

class StartupGate(PreEffectProbe):
    def test_unavailable_witness_does_not_execute_inherited_startup(self):
        root,env,base,cap,journal,resource,rid=self.prepare_run('startup')
        startup=root/'startup';startup.mkdir()
        marker=root/'startup-effect';command_effect=root/'requested-command-effect'
        # This inert private marker executes only in Python's -c gate launcher,
        # not the parent CLI or acquisition. No external resource is touched.
        (startup/'sitecustomize.py').write_text('import sys\nfrom pathlib import Path\nif sys.argv[0]=="-c": Path('+repr(str(marker))+').write_text("startup ran before gate")\n')
        env['PYTHONPATH']=str(startup)+':'+str(Path(__file__).resolve().parents[1]/'src')
        wrapper=root/'unavailable.py'
        wrapper.write_text('import runpy,sys,time\nfrom pathlib import Path\nfrom agent_chat import guard_witness\nfrom agent_chat.core import CoordError\ndef unavailable(*args):\n deadline=time.monotonic()+0.3\n while time.monotonic()<deadline and not Path('+repr(str(marker))+').exists(): time.sleep(0.01)\n raise CoordError("fixture witness unavailable before G")\nguard_witness.observe_launcher=unavailable\nsys.argv=["agent_chat.cli",*sys.argv[1:]]\nrunpy.run_module("agent_chat.cli",run_name="__main__")\n')
        result=subprocess.run([sys.executable,str(wrapper),*base[3:],'run','--require-host-witness',
            '--capability-file',str(cap),'--reservation-id',rid,'--process-evidence-file',str(journal),
            resource,'--','/usr/bin/touch',str(command_effect)],env=env,capture_output=True,timeout=12)
        self.assertNotEqual(result.returncode,0)
        self.assertIn('fixture witness unavailable before G',result.stderr.decode())
        self.assertFalse(command_effect.exists())
        print('startup_effect='+str(marker.exists())+' requested_command_effect='+str(command_effect.exists()),flush=True)
        self.assertFalse(marker.exists(),'opt-in inert launcher executed inherited Python startup before witness/G')


def load_tests(loader, tests, pattern):
    return unittest.TestSuite([StartupGate('test_unavailable_witness_does_not_execute_inherited_startup')])
