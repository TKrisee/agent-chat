import os, subprocess
import unittest
from pathlib import Path
from test_decision_boundary import DecisionBoundary, jq
from test_remote_web import TOKEN

class PublicationConsent(DecisionBoundary):
    def test_recorded_publication_classification_is_not_verified_consent(self):
        status, raw, _=self.call_api('register',{'agent':'author'}); self.assertEqual(status,200,raw)
        status, config, _=self.request('GET','/api/config',auth='basic'); operator=jq(config,'.sender.id')
        plan={'repositories':[['fixture','origin','refs/heads/main','a'*40]],'gitlinks':[]}
        status, sent, _=self.call_api('publication-plan-create',{'to':operator,'plan':plan}); self.assertEqual(status,200,sent)
        source=jq(sent,'.id')
        status, reply, _=self.request('POST','/api/messages',{'to':'author','body':'No, do not do it.','reply_to':source},auth='basic',headers={'X-Agent-Chat-CSRF':self.server.csrf_token})
        self.assertEqual(status,200,reply); reply_id=jq(reply,'.id')
        status, raw, _=self.call_api('message',{'id':reply_id}); self.assertEqual(status,200,raw)
        status, raw, _=self.call_api('decision-answer',{'id':source,'reply_id':reply_id,'outcome':'accepted'}); self.assertEqual(status,200,raw)
        status, raw, _=self.call_api('publication-plan-status',{'id':source,'plan':plan}); self.assertEqual(status,200,raw)
        self.assertEqual(jq(raw,'.state'),'accepted')
        result=subprocess.run(['jq','-e','.state == "accepted" and .classification_provenance == "author_recorded" and .verified_consent == "unknown"'],input=raw,capture_output=True)
        self.assertEqual(result.returncode,0,'publication classification lacked author/consent limits')
        env=dict(os.environ,UI_URL=self.url,UI_TOKEN=TOKEN,UI_SOURCE=source,UI_REPLY=reply_id)
        node='/Users/kristoftischler/Library/Application Support/Herd/config/nvm/versions/node/v22.22.3/bin/node'
        browser=subprocess.run([node,str(Path(__file__).with_name('publication_decision_composed_browser.cjs'))],env=env,text=True,capture_output=True,timeout=35)
        self.assertEqual(browser.returncode,0,browser.stdout+browser.stderr)
        print(browser.stdout.strip(),flush=True)

def load_tests(loader,tests,pattern):
    return unittest.TestSuite([PublicationConsent('test_recorded_publication_classification_is_not_verified_consent')])
