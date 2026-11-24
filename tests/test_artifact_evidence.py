"""Private synthetic HTTP and CLI evidence lifecycle regression."""
import base64
import os
import sqlite3
import subprocess
import unittest
from pathlib import Path
from test_project_activity import ActivityWorkflow
from agent_chat.remote import RemoteCoordError

class ArtifactEvidenceTests(ActivityWorkflow):
    def test_request_scoped_evidence_api_exists(self):
        _, po, worker, root, client = self.projects['Alpha']
        source=self.message(root,po,'original objective')
        goal=self.cli_query(self.activity_cli(po,'activity-adopt','--source-request-id',source,'--title','Goal'),'.id')
        task_source=self.message(po,worker,'change artifact')
        task=self.cli_query(self.activity_cli(po,'activity-assign','--goal-id',goal,'--worker',worker,'--source-request-id',task_source,'--title','Task'),'.id')
        with self.assertRaises(RemoteCoordError): self.coord_call(client,po,'activity-evidence-link',work_id=task,request_id=source)
        result=self.coord_call(client,po,'activity-evidence-list',work_id=task)
        self.assertEqual(result['work_id'],task)

    def test_scoped_actual_artifact_provenance_and_unverified_acceptance(self):
        project, po, worker, root, client=self.projects['Alpha']
        source=self.message(root,po,'objective')
        goal=self.cli_query(self.activity_cli(po,'activity-adopt','--source-request-id',source,'--title','Goal'),'.id')
        csrf=self.server.csrf_token
        def request(body):
            payload=subprocess.run(['jq','-nc','--arg','to',po,'--arg','body',body,'{to:$to,body:$body}'],capture_output=True,check=True).stdout
            auth=base64.b64encode(('operator:'+__import__('test_remote_web').TOKEN).encode()).decode()
            response=subprocess.run(['curl','-fsS','-H','Authorization: Basic '+auth,'-H','X-Agent-Chat-CSRF: '+csrf,'-H','Content-Type: application/json','--data-binary','@-',self.url+'/api/messages?project='+project],input=payload,capture_output=True,check=True).stdout
            return self.cli_query(response,'.id')
        first=request('Request a change')
        second=request('Another actual request')
        rootdir=Path(self.tmp.name); before=rootdir/'before'; after=rootdir/'after'; before.mkdir(); after.mkdir()
        (before/'artifact.txt').write_text('before',encoding='utf-8'); (after/'artifact.txt').write_text('after',encoding='utf-8')
        body=rootdir/'body.txt'; body.write_text('Assigned artifact',encoding='utf-8')
        assignment=self.activity_cli(po,'send','--to',worker,'--body-file',str(body),'--attach',str(before/'artifact.txt'))
        task_source=self.cli_query(assignment,'.id'); before_id=self.cli_query(assignment,'.attachments[0].id')
        task=self.cli_query(self.activity_cli(po,'activity-assign','--goal-id',goal,'--worker',worker,'--source-request-id',task_source,'--title','Task'),'.id')
        self.activity_cli(worker,'context')
        body.write_text('Changed artifact and validation report. Customer accepted.',encoding='utf-8')
        response=self.activity_cli(worker,'send','--to',po,'--reply-to',task_source,'--ack-reply','--body-file',str(body),'--attach',str(after/'artifact.txt'))
        result_id=self.cli_query(response,'.id'); after_id=self.cli_query(response,'.attachments[0].id')
        for request_id in (first,second): self.activity_cli(po,'activity-evidence-link',task,'--request-id',request_id)
        with self.assertRaises(RemoteCoordError): self.coord_call(client,po,'activity-evidence-record',work_id=task,request_id=first,kind='change',message_id=result_id,before_id=after_id,after_id=after_id,summary='no change')
        self.activity_cli(po,'activity-evidence-record',task,'--request-id',first,'--kind','change','--message-id',result_id,'--before-id',before_id,'--after-id',after_id,'--summary','Changed contents')
        self.activity_cli(po,'activity-evidence-record',task,'--request-id',first,'--kind','validation','--message-id',result_id,'--summary','Worker reports validation')
        self.activity_cli(po,'activity-evidence-record',task,'--request-id',second,'--kind','validation','--message-id',result_id,'--summary','Baseline compilation and launch only')
        self.activity_cli(po,'context')
        original=self.activity_cli(po,'message',first)
        customer=self.cli_query(original,'.message.sender_session')
        body.write_text('Artifact delivery',encoding='utf-8')
        delivered=self.activity_cli(po,'send','--to',customer,'--body-file',str(body),'--attach',str(after/'artifact.txt'))
        delivery_id=self.cli_query(delivered,'.id'); delivered_attachment=self.cli_query(delivered,'.attachments[0].id')
        delivery=self.activity_cli(po,'activity-evidence-record',task,'--request-id',first,'--kind','delivery','--message-id',delivery_id,'--after-id',delivered_attachment,'--summary','Sent matching bytes')
        delivery_event=self.cli_query(delivery,'.id')
        listing=self.activity_cli(po,'activity-evidence-list',task)
        self.assertEqual(self.cli_query(listing,'[.requests[] | select(.request_id==$id) | .dimensions.change.state] | .[0]',id=first),'reported')
        self.assertEqual(self.cli_query(listing,'[.requests[] | select(.request_id==$id) | .dimensions.change.state] | .[0]',id=second),'unknown')
        self.assertEqual(self.cli_query(listing,'[.requests[].customer_acceptance.state] | unique | join(",")'),'unknown')
        self.assertEqual(self.cli_query(listing,'[.requests[] | select(.request_id==$id) | .events[] | select(.kind=="delivery") | .recipient_read == null] | .[0]',id=first),'true')
        auth=base64.b64encode(('operator:'+__import__('test_remote_web').TOKEN).encode()).decode()
        delivered_view=subprocess.run(['curl','-fsS','-H','Authorization: Basic '+auth,self.url+'/api/messages/'+delivery_id+'?project='+project],capture_output=True,check=True).stdout
        self.assertEqual(self.cli_query(delivered_view,'.artifact_decisions[0].id'),delivery_event)
        listing=self.activity_cli(po,'activity-evidence-list',task)
        self.assertEqual(self.cli_query(listing,'[.requests[] | select(.request_id==$id) | .events[] | select(.kind=="delivery") | (.recipient_read == null)] | .[0]',id=first),'true')
        request('I accept this artifact')
        listing=self.activity_cli(po,'activity-evidence-list',task)
        self.assertEqual(self.cli_query(listing,'[.requests[].customer_acceptance.state] | unique | join(",")'),'unknown')
        acceptance=subprocess.run(['jq','-nc','--arg','work',task,'--arg','request',first,'--arg','delivery',delivery_event,'{work_id:$work,request_id:$request,delivery_id:$delivery,outcome:"accepted"}'],capture_output=True,check=True).stdout
        auth=base64.b64encode(('operator:'+__import__('test_remote_web').TOKEN).encode()).decode()
        url=self.url+'/api/artifact-acceptance?project='+project
        def action(payload, authorization, csrf_header=True):
            args=['curl','-sS','-o',str(rootdir/'acceptance.json'),'-w','%{http_code}','-H','Authorization: '+authorization,'-H','Content-Type: application/json']
            if csrf_header: args += ['-H','X-Agent-Chat-CSRF: '+csrf]
            return subprocess.run(args+['--data-binary','@-',url],input=payload,capture_output=True,check=True).stdout
        self.assertEqual(action(acceptance,'Bearer '+__import__('test_remote_web').TOKEN),b'403')
        self.assertEqual(action(acceptance,'Basic '+auth,False),b'403')
        asserted_actor=subprocess.run(['jq','--arg','actor',customer,'.operator_session=$actor'],input=acceptance,capture_output=True,check=True).stdout
        self.assertEqual(action(asserted_actor,'Basic '+auth),b'400')
        foreign_project=self.projects['Beta'][0]
        wrong_project=subprocess.run(['curl','-sS','-o',str(rootdir/'foreign.json'),'-w','%{http_code}','-H','Authorization: Basic '+auth,'-H','X-Agent-Chat-CSRF: '+csrf,'-H','Content-Type: application/json','--data-binary','@-',self.url+'/api/artifact-acceptance?project='+foreign_project],input=acceptance,capture_output=True,check=True).stdout
        self.assertEqual(wrong_project,b'400')
        wrong_request=subprocess.run(['jq','--arg','id',second,'.request_id=$id'],input=acceptance,capture_output=True,check=True).stdout
        self.assertEqual(action(wrong_request,'Basic '+auth),b'400')
        with sqlite3.connect(self.server.projects.db_path(project)) as db:
            db.execute('UPDATE attachments SET content=? WHERE id=?',(b'xxxxx',delivered_attachment))
        self.assertEqual(action(acceptance,'Basic '+auth),b'400')
        with sqlite3.connect(self.server.projects.db_path(project)) as db:
            db.execute('UPDATE attachments SET content=? WHERE id=?',(b'after',delivered_attachment))
        self.activity_cli(po,'activity-evidence-record',task,'--request-id',second,'--kind','change','--message-id',result_id,'--before-id',before_id,'--after-id',after_id,'--summary','Changed contents')
        delivered2=self.activity_cli(po,'send','--to',customer,'--body-file',str(body),'--attach',str(after/'artifact.txt'))
        delivery2=self.cli_query(delivered2,'.id'); attachment2=self.cli_query(delivered2,'.attachments[0].id')
        event2=self.activity_cli(po,'activity-evidence-record',task,'--request-id',second,'--kind','delivery','--message-id',delivery2,'--after-id',attachment2,'--summary','Sent matching bytes')
        event2id=self.cli_query(event2,'.id')
        decline2=subprocess.run(['jq','-nc','--arg','work',task,'--arg','request',second,'--arg','delivery',event2id,'{work_id:$work,request_id:$request,delivery_id:$delivery,outcome:"declined"}'],capture_output=True,check=True).stdout
        self.assertEqual(action(decline2,'Basic '+auth),b'200')
        listing=self.activity_cli(po,'activity-evidence-list',task)
        self.assertEqual(self.cli_query(listing,'[.requests[] | select(.request_id==$id) | .customer_acceptance.state] | .[0]',id=second),'decisions_recorded')
        self.assertEqual(self.cli_query(listing,'[.requests[] | select(.request_id==$id) | .customer_acceptance.decisions[0].outcome] | .[0]',id=second),'declined')
        self.assertEqual(self.cli_query(listing,'[.requests[] | select(.request_id==$id) | .customer_acceptance.undecided_delivery_count] | .[0]',id=first),'1')
        exported=self.activity_cli(po,'activity-export')
        self.assertEqual(self.cli_query(exported,'[.work[] | select(.id==$id) | .acceptance.customer_acceptance.state] | .[0]',id=task),'decisions_recorded')
        self.assertEqual(self.cli_query(exported,'[.work[] | select(.id==$id) | .acceptance.customer_acceptance.unknown_request_count] | .[0]',id=task),'1')
        self.assertEqual(self.cli_query(exported,'.completeness.customer_acceptance'),'decisions_recorded')
        env=os.environ.copy()
        env.update(SYNTHETIC_TEST_TOKEN=__import__('test_remote_web').TOKEN,SYNTHETIC_TEST_URL=self.url,SYNTHETIC_PROJECT=project,SYNTHETIC_DELIVERY=delivery_id,SYNTHETIC_REQUEST=first,SYNTHETIC_HASH=self.cli_query(delivery,'.after_sha256'))
        browser=subprocess.run(['node',str(Path(__file__).with_name('artifact_acceptance_browser.cjs'))],env=env,capture_output=True,timeout=45)
        self.assertEqual(browser.returncode,0,browser.stderr.decode())
        self.assertIn(b'explicit authenticated artifact action and persisted reload',browser.stdout)
        accepted=subprocess.run(['curl','-sS','-o',str(rootdir/'acceptance.json'),'-w','%{http_code}','-H','Authorization: Basic '+auth,'-H','X-Agent-Chat-CSRF: '+csrf,'-H','Content-Type: application/json','--data-binary','@-',self.url+'/api/artifact-acceptance?project='+project],input=acceptance,capture_output=True,check=True)
        self.assertEqual(accepted.stdout,b'200')
        self.assertEqual(action(acceptance,'Basic '+auth),b'200')
        declined=subprocess.run(['jq','.outcome="declined"'],input=acceptance,capture_output=True,check=True).stdout
        self.assertEqual(action(declined,'Basic '+auth),b'400')
        listing=self.activity_cli(po,'activity-evidence-list',task)
        self.assertEqual(self.cli_query(listing,'[.requests[] | select(.request_id==$id) | .customer_acceptance.decisions[0].outcome] | .[0]',id=first),'accepted')
        self.assertEqual(self.cli_query(listing,'[.requests[] | select(.request_id==$id) | .customer_acceptance.decisions[0] | (.work_id==$task and .request_id==$id and .artifact_id==$artifact and .operator_session==$operator and .origin=="operator_basic_csrf" and (.recorded_at>0))] | .[0]',id=first,task=task,artifact=delivered_attachment,operator=customer),'true')
        self.assertEqual(self.cli_query(listing,'[.requests[] | select(.request_id==$id) | .customer_acceptance.state] | .[0]',id=second),'decisions_recorded')
        listing=self.activity_cli(po,'activity-evidence-list',task)
        self.assertEqual(self.cli_query(listing,'[.requests[] | select(.request_id==$id) | .customer_acceptance.decisions[0].outcome] | .[0]',id=second),'declined')
        self.activity_cli(worker,'activity-evidence-list',task,succeeds=False)
        _, foreign_po, _, _, _=self.projects['Beta']
        self.activity_cli(foreign_po,'activity-evidence-list',task,name='Alpha',succeeds=False)
        with self.assertRaises(RemoteCoordError): self.coord_call(client,po,'activity-evidence-record',work_id=task,request_id=first,kind='customer_acceptance',message_id=result_id,before_id=None,after_id=None,summary='claim')
        with self.assertRaises(RemoteCoordError): self.coord_call(client,worker,'activity-evidence-link',work_id=task,request_id=first)
        exported=self.activity_cli(po,'activity-export')
        self.assertEqual(self.cli_query(exported,'[.work[] | select(.id==$id) | .request_evidence | length] | .[0]',id=task),'2')
        self.assertEqual(self.cli_query(exported,'[.work[] | select(.id==$id) | .acceptance.customer_acceptance.state] | .[0]',id=task),'decisions_recorded')
        self.assertEqual(self.cli_query(exported,'[.work[] | select(.id==$id) | .request_evidence[].customer_acceptance.decisions[].outcome] | sort | join(",")',id=task),'accepted,declined')
        dependency_source=self.message(po,worker,'Earlier publication ordering')
        dependency=self.cli_query(self.activity_cli(po,'activity-assign','--goal-id',goal,'--worker',worker,'--source-request-id',dependency_source,'--title','Earlier dependency'),'.id')
        self.activity_cli(po,'activity-metadata',goal,'--dependency',dependency,'--next-action','Wait')
        self.activity_cli(po,'activity-metadata',goal,'--next-action','Changed sequencing')
        exported=self.activity_cli(po,'activity-export')
        self.assertEqual(self.cli_query(exported,'[.work[] | select(.id==$id) | (.dependencies | length)] | .[0]',id=goal),'0')


def load_tests(loader, tests, pattern):
    return unittest.TestSuite(ArtifactEvidenceTests(name) for name in (
        'test_request_scoped_evidence_api_exists',
        'test_scoped_actual_artifact_provenance_and_unverified_acceptance'))
