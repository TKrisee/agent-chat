"""Real private authenticated CLI/HTTP reply timing; no native provider calls."""
import contextlib
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import unittest
from test_source_trace import SourceTraceTests, jq
from test_remote_web import TOKEN

class SourceReplyTrace(SourceTraceTests):
    def setUp(self):
        super().setUp()
        for actor in ('sender','recipient','foreign'):
            status,body,_=self.request('POST','/api/coord',dict(op='register',session=actor,host_id='mac',params={'agent':actor}))
            self.assertEqual(status,200,body)
        self.serial=0

    def cli(self,actor,*args,host='mac',project='default',success=True):
        env={k:v for k,v in os.environ.items() if not k.startswith('AGENT_CHAT_')}
        env.update(AGENT_CHAT_HOST_ID=host,PYTHONPATH=str(Path(__file__).resolve().parents[1]/'src'),PYTHONDONTWRITEBYTECODE='1')
        command=[sys.executable,'-m','agent_chat.cli','--server',self.url,'--api-token',TOKEN,'--project',project,'--session',actor,*args]
        result=subprocess.run(command,capture_output=True,env=env,timeout=8)
        if success:self.assertEqual(result.returncode,0,result.stderr.decode())
        return result

    def send(self,actor,to,body,reply=None,ack=False,more=()):
        self.serial+=1;p=Path(self.tmp.name)/('body-'+str(self.serial)+'.txt');p.write_text(body)
        args=['send','--to',to,'--body-file',str(p)]
        for other in more:args+=['--to',other]
        if reply:args+=['--reply-to',reply]
        if ack:args+=['--ack-reply']
        return jq(self.cli(actor,*args).stdout,'.deliveries[0].id' if more else '.id')

    def reply_pair(self,body='Received; investigation pending.'):
        source=self.send('sender','recipient','New private bounded source')
        self.cli('recipient','message',source)
        reply=self.send('recipient','sender',body,reply=source,ack=True)
        return source,reply

    def trace(self,source,actor='sender',**kwargs):
        return self.cli(actor,'source-trace',source,**kwargs).stdout

    def dbrow(self,query,args=()):
        with contextlib.closing(sqlite3.connect(self.db)) as db:return db.execute(query,args).fetchone()

    def assert_unknown_native(self,body):
        self.assertEqual(jq(body,'.eligibility_at == null and .native_first_response_at == null and .recipient_delivery_at == null'),'true')
        self.assertNotIn(b'Received;',body)

    def test_directed_reply_acceptance_then_real_complete_consumption(self):
        source,reply=self.reply_pair()
        first=self.trace(source)
        self.assertEqual(jq(first,'.first_reply_message_id // "MISSING"'),reply)
        accepted=self.dbrow('SELECT created_at FROM messages WHERE id=?',(reply,))[0]
        self.assertEqual(float(jq(first,'.first_reply_accepted_at')),accepted)
        self.assertEqual(jq(first,'.first_reply_read_at == null'),'true')
        self.assert_unknown_native(first)
        self.cli('sender','message',reply)
        read=self.dbrow('SELECT read_at,provenance FROM message_reads WHERE message_id=? AND reader_session=?',(reply,'sender'))
        self.assertEqual(read[1],'recipient_full_message')
        for actor in ('sender','recipient'):
            traced=self.trace(source,actor)
            self.assertEqual(jq(traced,'.first_reply_message_id'),reply)
            self.assertEqual(float(jq(traced,'.first_reply_read_at')),read[0])
            self.assert_unknown_native(traced)


    def test_no_reply_and_absent_or_legacy_read_evidence_stay_unknown(self):
        source=self.send('sender','recipient','Source without reply')
        raw=self.trace(source)
        self.assertEqual(jq(raw,'has("first_reply_message_id") and .first_reply_message_id == null and .first_reply_accepted_at == null and .first_reply_read_at == null'),'true')
        self.cli('recipient','message',source)
        reply=self.send('recipient','sender','Pending read',reply=source,ack=True)
        with contextlib.closing(sqlite3.connect(self.db)) as db:
            db.execute('DROP TABLE message_reads');db.commit()
        raw=self.trace(source)
        self.assertEqual(jq(raw,'.first_reply_message_id'),reply)
        self.assertEqual(jq(raw,'.first_reply_read_at == null'),'true')
        with contextlib.closing(sqlite3.connect(self.db)) as db:
            db.execute('CREATE TABLE message_reads(message_id,reader_session,read_at)')
            db.execute('INSERT INTO message_reads VALUES(?,?,?)',(reply,'sender',1));db.commit()
        self.assertEqual(jq(self.trace(source),'.first_reply_read_at == null'),'true')

    def test_ack_and_partial_context_do_not_become_full_read(self):
        source,reply=self.reply_pair('Pending requested answer. '+('x'*12000))
        self.cli('sender','acknowledge',reply)
        self.assertIsNotNone(self.dbrow('SELECT acked_at FROM messages WHERE id=?',(reply,))[0])
        self.assertEqual(jq(self.trace(source),'.first_reply_read_at == null'),'true')
        partial=self.cli('sender','context','--message-id',reply,'--max-bytes','2048').stdout
        self.assertEqual(jq(partial,'.messages[0].body_truncated'),'true')
        self.assertEqual(jq(self.trace(source),'.first_reply_read_at == null'),'true')
        full=self.cli('sender','context','--message-id',reply,'--max-bytes','32000').stdout
        self.assertEqual(jq(full,'.messages[0].body_truncated == false'),'true')
        first=self.dbrow('SELECT read_at,provenance FROM message_reads WHERE message_id=? AND reader_session=?',(reply,'sender'))
        self.assertEqual(first[1],'recipient_context_full')
        self.assertEqual(float(jq(self.trace(source),'.first_reply_read_at')),first[0])
        self.cli('sender','message',reply)
        self.assertEqual(self.dbrow('SELECT read_at,provenance FROM message_reads WHERE message_id=? AND reader_session=?',(reply,'sender')),first)

    def test_legacy_unknown_requires_genuine_later_read_and_keeps_its_time(self):
        source,reply=self.reply_pair()
        # Deliberately untrusted historical marker: never promoted by tracing.
        with contextlib.closing(sqlite3.connect(self.db)) as db:
            db.execute('INSERT INTO message_reads VALUES(?,?,?,?)',(reply,'sender',1,'legacy_unknown'))
            db.execute('INSERT INTO message_reads VALUES(?,?,?,?)',(reply,'foreign',2,'recipient_full_message'));db.commit()
        self.assertEqual(jq(self.trace(source),'.first_reply_read_at == null'),'true')
        self.assertEqual(self.dbrow('SELECT read_at,provenance FROM message_reads WHERE message_id=? AND reader_session=?',(reply,'sender')),(1,'legacy_unknown'))
        denied=self.cli('foreign','message',reply,success=False);self.assertNotEqual(denied.returncode,0)
        self.cli('sender','message',reply)
        actual=self.dbrow('SELECT read_at,provenance FROM message_reads WHERE message_id=? AND reader_session=?',(reply,'sender'))
        self.assertGreater(actual[0],2);self.assertEqual(actual[1],'recipient_full_message')
        self.assertEqual(float(jq(self.trace(source),'.first_reply_read_at')),actual[0])

    def test_only_first_opposite_direct_reply_qualifies(self):
        source=self.send('sender','recipient','Original source')
        self.send('sender','recipient','Same-direction followup',reply=source)
        foreign=self.send('foreign','sender','Foreign reply candidate')
        batch=self.send('recipient','sender','Batch reply candidate',more=('foreign',))
        self.assertEqual(self.dbrow('SELECT sender_session,recipient_session FROM messages WHERE id=?',(batch,)),('recipient','sender'))
        self.cli('recipient','link-reply',batch,'--reply-to',source)
        denied=self.cli('foreign','link-reply',foreign,'--reply-to',source,success=False)
        self.assertNotEqual(denied.returncode,0)
        # An invalid legacy/corrupt foreign association cannot expose that sender.
        with contextlib.closing(sqlite3.connect(self.db)) as db:
            db.execute('INSERT INTO message_replies VALUES(?,?)',(foreign,source))
            db.commit()
        self.assertEqual(jq(self.trace(source),'.first_reply_message_id == null'),'true')
        self.cli('recipient','message',source)
        first=self.send('recipient','sender','First directed response',reply=source,ack=True)
        second=self.send('recipient','sender','Later requested result',reply=source)
        self.cli('sender','message',second)
        raw=self.trace(source)
        self.assertEqual(jq(raw,'.first_reply_message_id'),first)
        self.assertEqual(jq(raw,'.first_reply_read_at == null'),'true')
        # Stored sequence remains the ordering authority if a wall clock steps.
        with contextlib.closing(sqlite3.connect(self.db)) as db:
            db.execute('UPDATE messages SET created_at=1 WHERE id=?',(second,));db.commit()
        self.assertEqual(jq(self.trace(source),'.first_reply_message_id'),first)

    def test_foreign_caller_host_project_and_source_direction_remain_scoped(self):
        source,reply=self.reply_pair()
        self.assertEqual(jq(self.trace(source,'foreign'),'.status'),'unauthorized')
        self.assertEqual(jq(self.trace(source,host='wrong-host'),'.status'),'unauthorized')
        status,body,_=self.request('POST','/api/projects/rpc',dict(op='create',name='Private other project'))
        self.assertEqual(status,200,body);other=jq(body,'.project.id')
        self.cli('sender','register','--agent','sender',project=other)
        traced=self.trace(source,project=other)
        self.assertEqual(jq(traced,'.status'),'unauthorized');self.assertNotIn(reply.encode(),traced)
        status,_,_=self.request('POST','/api/coord',dict(op='source-trace',session='sender',host_id='mac',params={'source_id':source}),auth=None)
        self.assertIn(status,(401,403))
        raw=self.trace(reply)
        self.assertEqual(jq(raw,'.first_reply_message_id == null'),'true')

    def test_trace_does_not_change_any_fixture_rows_schema_or_receipts(self):
        source,reply=self.reply_pair()
        def snapshot():
            with contextlib.closing(sqlite3.connect(self.db)) as db:return tuple(db.iterdump())
        before=snapshot()
        for actor in ('sender','recipient'):
            raw=self.trace(source,actor);self.assertEqual(jq(raw,'.first_reply_message_id'),reply)
        self.assertEqual(snapshot(),before)
        self.assertEqual(self.dbrow('SELECT COUNT(*) FROM message_reads WHERE message_id=?',(reply,))[0],0)
        self.assertIsNone(self.dbrow('SELECT acked_at FROM messages WHERE id=?',(reply,))[0])


def load_tests(loader,tests,pattern):
    return unittest.TestSuite(SourceReplyTrace(name) for name in SourceReplyTrace.__dict__ if name.startswith('test_'))
