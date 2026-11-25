import contextlib
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
import io
from pathlib import Path
from agent_chat.core import Coordinator, CoordError
from agent_chat.remote_service import dispatch
from agent_chat.web import message_rows
from agent_chat import cli, remote

class PublicationIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.db=Path(self.tmp.name)/'app.db'
        self.author=Coordinator(self.db,'author'); self.addCleanup(self.author.close); self.author.register('Author')
        self.operator=Coordinator(self.db,'operator'); self.addCleanup(self.operator.close); self.operator.register('Operator')
        self.other=Coordinator(self.db,'other'); self.addCleanup(self.other.close); self.other.register('Other')
        with contextlib.closing(sqlite3.connect(self.db)) as con, con:
            con.execute('INSERT INTO operator_identities(session_id) VALUES(?)',('operator',))
        self.plan={'repositories':[['parent','origin-parent','refs/heads/main','a'*40],['child','origin-child','refs/heads/main','b'*40]],'gitlinks':[['parent','vendor/child','child','b'*40]]}
    def call(self, actor, op, params):
        return dispatch(actor, {'op':op,'session':actor.session,'host_id':'test-host','params':params},canonical_project='default')
    def test_create_render_approve_and_altered(self):
        sent=self.call(self.author,'publication-plan-create',{'to':'operator','plan':self.plan}); mid=sent['id']
        with contextlib.closing(sqlite3.connect(self.db)) as con:
            con.row_factory=sqlite3.Row
            row=message_rows(con.execute('SELECT * FROM messages WHERE id=?',(mid,)).fetchall(),{},con)[0]
        self.assertIn('origin-parent',row['body']); self.assertIn('vendor/child',row['body']); self.assertEqual(row['decision']['status'],'pending')
        self.author.close()
        self.author=Coordinator(self.db,'author'); self.addCleanup(self.author.close)
        self.assertEqual(self.call(self.author,'publication-plan-status',{'id':mid,'plan':self.plan})['state'],'pending')
        self.operator._operator_message_origin=True
        try: reply=self.operator.send('author','approve',reply_to=mid)
        finally: self.operator._operator_message_origin=False
        self.author.record_decision_answer(mid,reply['id'],'accepted')
        self.assertEqual(self.call(self.author,'publication-plan-status',{'id':mid,'plan':self.plan})['state'],'accepted')
        changed={'repositories':[self.plan['repositories'][0],['child','origin-child','refs/heads/other','b'*40]],'gitlinks':self.plan['gitlinks']}
        self.assertEqual(self.call(self.author,'publication-plan-status',{'id':mid,'plan':changed})['state'],'mismatch')
        with self.assertRaises(CoordError): self.call(self.other,'publication-plan-status',{'id':mid,'plan':self.plan})
    def test_decline_remains_distinct_and_invalid_plan_rejected(self):
        sent=self.call(self.author,'publication-plan-create',{'to':'operator','plan':self.plan}); mid=sent['id']
        self.operator._operator_message_origin=True
        try: reply=self.operator.send('author','decline',reply_to=mid)
        finally: self.operator._operator_message_origin=False
        self.author.record_decision_answer(mid,reply['id'],'declined')
        self.assertEqual(self.call(self.author,'publication-plan-status',{'id':mid,'plan':self.plan})['state'],'declined')
        with self.assertRaises(CoordError): self.call(self.author,'publication-plan-create',{'to':'operator','plan':{'repositories':[self.plan['repositories'][0]],'gitlinks':self.plan['gitlinks']}})

    def test_cli_constructs_declared_plan_and_dispatches(self):
        received=[]
        results=[]
        def transport(logger, client, path, payload, **kwargs):
            received.append(payload)
            result=dispatch(self.author, payload, canonical_project='default')
            results.append(result.copy())
            return result
        class Client:
            def __init__(self, *args, **kwargs): self.project='default'
        argv=['--session','author','publication-plan-create','--to','operator',
              '--repo','parent','origin-parent','refs/heads/main','a'*40,
              '--repo','child','origin-child','refs/heads/main','b'*40,
              '--gitlink','parent','vendor/child','child','b'*40]
        with patch.object(remote,'HttpClient',Client), patch.object(cli,'_logged_call',side_effect=transport), patch.object(remote,'client_host_id',return_value='test-host'), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(cli.main(argv),0)
            mid=results[0]['id']
            self.assertEqual(cli.main(['--session','author','publication-plan-status',mid,*argv[5:]]),0)
        self.assertEqual(received[0]['op'],'publication-plan-create')
        self.assertEqual(received[0]['params']['plan'],self.plan)
        self.assertEqual(results[1]['state'],'pending')

if __name__=='__main__': unittest.main()
