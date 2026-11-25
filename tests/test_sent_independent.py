import contextlib
import os
import sqlite3
import subprocess
import sys
import unittest
import uuid
from pathlib import Path
from agent_chat.bridge import Bridge
from agent_chat.bridge_state import BridgeState
from agent_chat.core import Coordinator
from agent_chat.remote import HttpClient
from test_project_activity import ObservedProvider
from test_remote_web import TOKEN
from test_source_trace import SourceTraceTests, jq

class IndependentSent(SourceTraceTests):
    def prepare_message(self):
        for sid in ('sender','recipient'):
            status, raw, _ = self.request('POST','/api/coord',{'op':'register','session':sid,'host_id':'mac','params':{'agent':sid}})
            self.assertEqual(status,200,raw)
        status, raw, _ = self.request('POST','/api/coord',{'op':'send','session':'sender','host_id':'mac','params':{'to':'recipient','body':'private exact body'}})
        self.assertEqual(status,200,raw)
        return jq(raw,'.id')

    def cli_sent(self, project, source):
        env = dict(os.environ, AGENT_CHAT_HOST_ID='mac', PYTHONPATH=str(Path(__file__).resolve().parents[1] / 'src'))
        return subprocess.run([sys.executable,'-m','agent_chat.cli','--server',self.url,'--api-token',TOKEN,
            '--project',project,'--session','sender','sent-message',source], env=env,capture_output=True,timeout=8)

    def test_wrong_project_rejects_even_same_sender_session(self):
        source = self.prepare_message()
        project = self.server.projects.create('Other project')['id']
        client = HttpClient(self.url,TOKEN,project=project)
        client.call('/api/coord',dict(op='register',session='sender',host_id='mac',params=dict(agent='sender')))
        output = self.cli_sent(project,source)
        self.assertNotEqual(output.returncode,0)
        self.assertNotIn(b'private exact body',output.stdout+output.stderr)
        with sqlite3.connect(self.db) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM message_reads WHERE message_id=?',(source,)).fetchone()[0],0)
            self.assertIsNone(db.execute('SELECT acked_at FROM messages WHERE id=?',(source,)).fetchone()[0])

    def test_recorded_bridge_job_reports_only_actual_status_without_read(self):
        source = self.prepare_message()
        with contextlib.closing(Coordinator(self.db,'recipient')) as coord:
            state = BridgeState(coord); thread = str(uuid.uuid4()); state.bind(thread_id=thread)
            provider = ObservedProvider(); provider.add(thread,self.tmp.name)
            Bridge(state,provider).tick(observe_policies=False)
        with sqlite3.connect(self.db) as db:
            job = db.execute('SELECT j.id,j.status FROM bridge_deliveries d JOIN bridge_jobs j ON j.id=d.job_id WHERE d.message_id=?',(source,)).fetchone()
            self.assertIsNotNone(job)
            before = db.execute('SELECT acked_at FROM messages WHERE id=?',(source,)).fetchone()
            reads = db.execute('SELECT count(*) FROM message_reads WHERE message_id=?',(source,)).fetchone()[0]
        output = self.cli_sent('default',source)
        self.assertEqual(output.returncode,0,output.stderr.decode())
        self.assertEqual(jq(output.stdout,'.message.bridge_delivery.observation'),'recorded')
        self.assertEqual(jq(output.stdout,'.message.bridge_delivery.id'),job[0])
        self.assertEqual(jq(output.stdout,'.message.bridge_delivery.status'),job[1])
        self.assertEqual(jq(output.stdout,'.message.bridge_delivery | (.created_at > 0 and .updated_at > 0)'),'true')
        with sqlite3.connect(self.db) as db:
            self.assertEqual(db.execute('SELECT acked_at FROM messages WHERE id=?',(source,)).fetchone(),before)
            self.assertEqual(db.execute('SELECT count(*) FROM message_reads WHERE message_id=?',(source,)).fetchone()[0],reads)

    def test_recorded_first_ack_and_sent_reply_association(self):
        source = self.prepare_message()
        status, _, _ = self.request('POST','/api/coord',{'op':'message','session':'recipient','host_id':'mac','params':{'id':source}})
        self.assertEqual(status,200)
        status, raw, _ = self.request('POST','/api/coord',{'op':'send','session':'recipient','host_id':'mac',
            'params':{'to':'sender','body':'actual first reply','reply_to':source,'ack_reply':True}})
        self.assertEqual(status,200,raw)
        reply = jq(raw,'.id')
        original = self.cli_sent('default',source)
        self.assertEqual(original.returncode,0,original.stderr.decode())
        self.assertEqual(jq(original.stdout,'.message.acked_at > 0 and .message.recipient_read.provenance == "recipient_full_message"'),'true')
        env = dict(os.environ, AGENT_CHAT_HOST_ID='mac', PYTHONPATH=str(Path(__file__).resolve().parents[1] / 'src'))
        result = subprocess.run([sys.executable,'-m','agent_chat.cli','--server',self.url,'--api-token',TOKEN,
            '--project','default','--session','recipient','sent-message',reply], env=env,capture_output=True,timeout=8)
        self.assertEqual(result.returncode,0,result.stderr.decode())
        self.assertEqual(jq(result.stdout,'.message.reply_to'),source)
        self.assertEqual(jq(result.stdout,'.message.body'),'actual first reply')

def load_tests(loader, tests, pattern):
    return unittest.TestSuite(IndependentSent(name) for name in IndependentSent.__dict__ if name.startswith('test_'))
