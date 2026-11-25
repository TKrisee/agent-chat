import os, sqlite3, subprocess, unittest
from pathlib import Path
from test_source_trace import SourceTraceTests, jq

class DecisionBoundary(SourceTraceTests):
    def call_api(self, op, params, session='author', host='mac'):
        return self.request('POST','/api/coord',{'op':op,'session':session,'host_id':host,'params':params})
    def test_authenticated_decision_and_unavailable_child_are_separate(self):
        status, raw, _ = self.call_api('register',{'agent':'author'})
        self.assertEqual(status,200,raw)
        status, config, _ = self.request('GET','/api/config',auth='basic')
        operator=jq(config,'.sender.id')
        status, sent, _=self.call_api('send',{'to':operator,'body':'May I perform <svg onload=alert(1)> exact scoped task?', 'decision_needed':True})
        self.assertEqual(status,200,sent); source=jq(sent,'.id')
        status, reply, _=self.request('POST','/api/messages',{'to':'author','body':'No, do not do it','reply_to':source},auth='basic',headers={'X-Agent-Chat-CSRF':self.server.csrf_token})
        self.assertEqual(status,200,reply); reply_id=jq(reply,'.id')
        status, raw, _=self.call_api('message',{'id':reply_id}); self.assertEqual(status,200,raw)
        status, raw, _=self.call_api('decision-answer',{'id':source,'reply_id':reply_id,'outcome':'accepted'}); self.assertEqual(status,200,raw)
        with sqlite3.connect(self.db) as db:
            before = [db.execute('SELECT count(*) FROM '+name).fetchone()[0] for name in ('message_reads','operator_decision_events','messages')]
        status, raw, _=self.call_api('decision-diagnostic',{'id':source,'dependency':'retained-child-review-event'})
        self.assertEqual(status,200,raw)
        self.assertEqual(jq(raw,'.author_classification.outcome'),'accepted')
        self.assertEqual(jq(raw,'.verified_consent.state'),'unknown')
        self.assertEqual(jq(raw,'.platform_dependency.state'),'unavailable')
        self.assertEqual(jq(raw,'.review_outcome.state'),'unknown')
        status, visible, _ = self.request('GET','/api/messages/'+source,auth='basic')
        self.assertEqual(status,200,visible)
        self.assertEqual(jq(visible,'.retained_child_diagnostic.author_classification'),'accepted')
        self.assertEqual(jq(visible,'.retained_child_diagnostic.provenance'),'authenticated_operator_ui')
        self.assertEqual(jq(visible,'.retained_child_diagnostic.review_outcome'),'unknown')
        env=dict(os.environ,UI_URL=self.url,UI_TOKEN='test-token',UI_SOURCE=source,UI_REPLY=reply_id)
        env['UI_TOKEN']=__import__('test_remote_web').TOKEN
        node='/Users/kristoftischler/Library/Application Support/Herd/config/nvm/versions/node/v22.22.3/bin/node'
        browser=subprocess.run([node,str(Path(__file__).with_name('decision_boundary_browser.cjs'))],env=env,text=True,capture_output=True,timeout=35)
        self.assertEqual(browser.returncode,0,browser.stdout+browser.stderr)
        print(browser.stdout.strip(),flush=True)
        with sqlite3.connect(self.db) as db:
            after = [db.execute('SELECT count(*) FROM '+name).fetchone()[0] for name in ('message_reads','operator_decision_events','messages')]
        self.assertEqual(before,after)

    def test_pending_declined_wrong_scope_and_invalid_dependency(self):
        for sid in ('author','other'):
            status, raw, _ = self.call_api('register',{'agent':sid},session=sid)
            self.assertEqual(status,200,raw)
        status, config, _=self.request('GET','/api/config',auth='basic'); operator=jq(config,'.sender.id')
        status, sent, _=self.call_api('send',{'to':operator,'body':'Specific question', 'decision_needed':True}); source=jq(sent,'.id')
        args={'id':source,'dependency':'retained-child-review-event'}
        status, raw, _=self.call_api('decision-diagnostic',args)
        self.assertEqual(status,200,raw); self.assertEqual(jq(raw,'.author_classification.outcome'),'unknown')
        self.assertEqual(jq(raw,'.verified_consent.expiration'),'unknown')
        self.assertEqual(jq(raw,'.review_outcome.state'),'unknown')
        for session,host,params in [('other','mac',args),('author','wrong-host',args),('author','mac',{'id':source,'dependency':'arbitrary'}),('author','mac',{**args,'actor':'other'})]:
            status, raw, _=self.call_api('decision-diagnostic',params,session=session,host=host)
            self.assertNotEqual(status,200,raw)
        status, reply, _=self.request('POST','/api/messages',{'to':'author','body':'No','reply_to':source},auth='basic',headers={'X-Agent-Chat-CSRF':self.server.csrf_token})
        self.assertEqual(status,200,reply); reply_id=jq(reply,'.id')
        self.call_api('message',{'id':reply_id})
        status, raw, _=self.call_api('decision-answer',{'id':source,'reply_id':reply_id,'outcome':'declined'}); self.assertEqual(status,200,raw)
        status, raw, _=self.call_api('decision-diagnostic',args)
        self.assertEqual(status,200,raw); self.assertEqual(jq(raw,'.author_classification.outcome'),'declined')
        self.assertEqual(jq(raw,'.operator_reply.provenance'),'authenticated_operator_ui')
        self.assertEqual(jq(raw,'.review_outcome.state'),'unknown')
        status, sent, _=self.call_api('send',{'to':operator,'body':'Second exact question','decision_needed':True}); self.assertEqual(status,200,sent)
        second=jq(sent,'.id')
        status, direct, _=self.call_api('send',{'to':'author','body':'Yes','reply_to':second},session=operator); self.assertEqual(status,200,direct)
        direct_id=jq(direct,'.id'); self.call_api('message',{'id':direct_id})
        status, answer, _=self.call_api('decision-answer',{'id':second,'reply_id':direct_id,'outcome':'accepted'}); self.assertEqual(status,200,answer)
        status, weak, _=self.call_api('decision-diagnostic',{'id':second,'dependency':'retained-child-review-event'})
        self.assertEqual(status,200,weak); self.assertEqual(jq(weak,'.operator_reply.provenance'),'registered_operator_direct')
        self.assertEqual(jq(weak,'.verified_consent.state'),'unknown')

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(DecisionBoundary(name) for name in DecisionBoundary.__dict__ if name.startswith('test_'))

from test_current_ui_acceptance import CurrentUiAcceptance
class DecisionProjectBoundary(CurrentUiAcceptance):
    def test_operator_recipient_and_project_boundary(self):
        from agent_chat.web import message_rows, MESSAGE_COLUMNS
        alpha, po, _, _, client = self.projects['Alpha']
        beta = self.projects['Beta'][0]
        status, config, _ = self.request('GET','/api/config?project='+alpha,auth='basic')
        self.assertEqual(status,200,config); operator=jq(config,'.sender.id')
        status, other_config, _ = self.request('GET','/api/config?project='+beta,auth='basic')
        self.assertEqual(status,200,other_config); other=jq(other_config,'.sender.id')
        self.assertNotEqual(operator,other)
        sent=self.coord_call(client,po,'send',to=operator,body='Exact scoped decision',decision_needed=True)
        status, raw, _=self.request('GET','/api/messages/'+sent['id']+'?project='+alpha,auth='basic')
        self.assertEqual(status,200,raw); self.assertEqual(jq(raw,'.retained_child_diagnostic.eligibility'),'unavailable')
        status, _, _=self.request('GET','/api/messages/'+sent['id']+'?project='+beta,auth='basic')
        self.assertNotEqual(status,200)
        with sqlite3.connect(self.server.projects.db_path(alpha)) as db:
            db.row_factory=sqlite3.Row
            row=db.execute('SELECT '+MESSAGE_COLUMNS+' FROM messages WHERE id=?',(sent['id'],)).fetchone()
            hidden=message_rows([row],{},db,receipt_operator=other)[0]
            self.assertNotIn('retained_child_diagnostic',hidden)

def load_tests(loader,tests,pattern):
    return unittest.TestSuite([DecisionBoundary('test_authenticated_decision_and_unavailable_child_are_separate'),DecisionBoundary('test_pending_declined_wrong_scope_and_invalid_dependency'),DecisionProjectBoundary('test_operator_recipient_and_project_boundary')])
