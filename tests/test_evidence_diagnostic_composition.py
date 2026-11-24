"""Private HTTP/CLI check that evidence integration preserves diagnostics."""
import os
import subprocess
import sys
import unittest
from pathlib import Path
from test_decision_boundary import DecisionBoundary
from test_remote_web import TOKEN
from test_source_trace import jq

class EvidenceDiagnosticComposition(DecisionBoundary):
    def test_diagnostic_cli_and_api_are_both_preserved(self):
        code,_,_=self.call_api('register',{'agent':'author'})
        self.assertEqual(code,200)
        code,config,_=self.request('GET','/api/config',auth='basic')
        operator=jq(config,'.sender.id')
        code,sent,_=self.call_api('send',{'to':operator,'body':'Synthetic diagnostic','decision_needed':True})
        self.assertEqual(code,200); source=jq(sent,'.id')
        code,api,_=self.call_api('decision-diagnostic',{'id':source,'dependency':'retained-child-review-event'})
        self.assertEqual(code,200); self.assertEqual(jq(api,'.verified_consent.state'),'unknown')
        env={key:value for key,value in os.environ.items() if not key.startswith('AGENT_CHAT_')}
        env.update(AGENT_CHAT_HOST_ID='mac', AGENT_CHAT_STATE_DIR=str(self.tmp.name), PYTHONPATH=str(Path(__file__).resolve().parents[1]/'src'))
        result=subprocess.run([sys.executable,'-m','agent_chat','--server',self.url,'--api-token',TOKEN,'--project','default','--session','author','decision-diagnostic',source,'--dependency','retained-child-review-event'],env=env,capture_output=True,timeout=15)
        self.assertEqual(result.returncode,0,result.stderr.decode())
        self.assertEqual(jq(result.stdout,'.verified_consent.state'),'unknown')


def load_tests(loader, tests, pattern):
    return unittest.TestSuite([EvidenceDiagnosticComposition('test_diagnostic_cli_and_api_are_both_preserved')])
