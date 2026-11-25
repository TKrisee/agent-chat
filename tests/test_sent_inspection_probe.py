import base64, os, subprocess, sys
from pathlib import Path
from test_source_trace import SourceTraceTests, jq
from test_remote_web import TOKEN

class SentInspectionProbe(SourceTraceTests):
    def test_sender_can_inspect_exact_sent_content(self):
        for sid in ('sender','recipient'):
            status, raw, _ = self.request('POST','/api/coord',{'op':'register','session':sid,'host_id':'mac','params':{'agent':sid}})
            self.assertEqual(status,200,raw)
        status, raw, _ = self.request('POST','/api/coord',{'op':'send','session':'sender','host_id':'mac','params':{'to':'recipient','body':'exact sent payload','attachments':[{'name':'evidence.txt','content_base64':base64.b64encode(b'fixture').decode()}]}})
        self.assertEqual(status,200,raw)
        source = jq(raw,'.id')
        env = dict(os.environ, AGENT_CHAT_HOST_ID='mac', PYTHONPATH=str(Path(__file__).resolve().parents[1] / 'src'))
        cmd = [sys.executable,'-m','agent_chat.cli','--server',self.url,'--api-token',TOKEN,'--project','default','--session','sender','sent-message',source]
        result = subprocess.run(cmd,env=env,capture_output=True,timeout=8)
        self.assertEqual(result.returncode,0,result.stderr.decode())
        self.assertEqual(jq(result.stdout,'.message.body'),'exact sent payload')
        self.assertEqual(jq(result.stdout,'.message.recipient_session'),'recipient')
        self.assertEqual(jq(result.stdout,'.message | [.acked_at,.recipient_read] | all(. == null)'), 'true')
        self.assertEqual(jq(result.stdout,'.message.bridge_delivery.observation'), 'not_recorded')
        self.assertEqual(jq(result.stdout,'.message.attachments[0].name'), 'evidence.txt')
        self.assertEqual(jq(result.stdout,'.message.attachments[0].size'), '7')
        foreign = cmd.copy(); foreign[foreign.index('sender')] = 'recipient'
        denied = subprocess.run(foreign,env=env,capture_output=True,timeout=8)
        self.assertNotEqual(denied.returncode,0)
        import sqlite3
        with sqlite3.connect(self.db) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM message_reads WHERE message_id=?',(source,)).fetchone()[0],0)
            self.assertIsNone(db.execute('SELECT acked_at FROM messages WHERE id=?',(source,)).fetchone()[0])
        wrong_env = dict(env, AGENT_CHAT_HOST_ID='other-host')
        self.assertNotEqual(subprocess.run(cmd,env=wrong_env,capture_output=True,timeout=8).returncode, 0)
        status, _, _ = self.request('POST','/api/coord',{'op':'message','session':'recipient','host_id':'mac','params':{'id':source}})
        self.assertEqual(status,200)
        after = subprocess.run(cmd,env=env,capture_output=True,timeout=8)
        self.assertEqual(after.returncode,0,after.stderr.decode())
        self.assertEqual(jq(after.stdout,'.message.recipient_read.provenance'),'recipient_full_message')
        self.assertEqual(jq(after.stdout,'.message.acked_at == null'),'true')

