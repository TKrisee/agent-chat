"""Read-only trace attribution against a private synthetic SQLite schema."""
import sqlite3
from contextlib import closing
import tempfile
import unittest
from pathlib import Path
from agent_chat.bridge_state import source_trace

class TraceEnrichmentTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)/'fixture.db'
        self.db = sqlite3.connect(self.path); self.addCleanup(self.db.close)
        self.db.executescript('''
          CREATE TABLE meta(key TEXT, value TEXT);
          INSERT INTO meta VALUES ('schema_version','1'),('project_id','default');
          CREATE TABLE sessions(id TEXT); INSERT INTO sessions VALUES ('sender'),('recipient'),('other');
          CREATE TABLE retired_sessions(id TEXT);
          CREATE TABLE remote_session_hosts(session_id TEXT,host_id TEXT);
          INSERT INTO remote_session_hosts VALUES ('sender','host'),('recipient','host'),('other','host');
          CREATE TABLE messages(id TEXT,sender_session TEXT,recipient_session TEXT,created_at REAL,seq INTEGER);
          INSERT INTO messages VALUES ('source','sender','recipient',100,1),('reply','recipient','sender',103,2),('wrong','other','sender',101,0);
          CREATE TABLE message_batches(message_id TEXT);
          CREATE TABLE message_replies(message_id TEXT,reply_to TEXT);
          INSERT INTO message_replies VALUES ('reply','source'),('wrong','source');
          CREATE TABLE message_reads(message_id TEXT,reader_session TEXT,read_at REAL,provenance TEXT);
          CREATE TABLE bridge_jobs(id TEXT,status TEXT);
          INSERT INTO bridge_jobs VALUES ('job','dispatched');
          CREATE TABLE bridge_deliveries(message_id TEXT,job_id TEXT);
          INSERT INTO bridge_deliveries VALUES ('source','job');
          CREATE TABLE bridge_job_origins(job_id TEXT,prepared_at REAL);
          INSERT INTO bridge_job_origins VALUES ('job',101);
          CREATE TABLE bridge_job_native_attempts(job_id TEXT,recorded_at REAL);
          INSERT INTO bridge_job_native_attempts VALUES ('job',2461000.5);
        '''); self.db.commit()
    def trace(self, session='sender'):
        return source_trace(self.path,project_id='default',session_id=session,host_id='host',source_id='source')
    def test_typed_reads_and_attempt_marker_are_separate(self):
        self.db.executemany('INSERT INTO message_reads VALUES (?,?,?,?)', [
            ('source','recipient',102,'recipient_full_message'),('reply','sender',104,'recipient_context_full')])
        self.db.commit(); result=self.trace()
        self.assertEqual(result['source_first_full_read_at'],102)
        self.assertEqual(result['first_reply_read_at'],104)
        self.assertEqual(result['first_reply_message_id'],'reply')
        self.assertEqual(result['job_transition_recorded_at'],1763683200.0)
        self.assertIsNone(result['eligibility_at']); self.assertIsNone(result['native_first_response_at']); self.assertIsNone(result['recipient_delivery_at'])
        self.assertEqual(result['job_native_content']['attribution'],'job_turn_only')
        self.assertEqual(self.trace('other')['status'],'unauthorized')
    def test_legacy_reads_and_missing_attempt_are_unknown(self):
        self.db.execute("INSERT INTO message_reads VALUES ('source','recipient',99,'legacy_unknown')")
        self.db.execute('DROP TABLE bridge_job_native_attempts'); self.db.commit()
        result=self.trace()
        self.assertIsNone(result['source_first_full_read_at'])
        self.assertIsNone(result['job_transition_recorded_at'])
        self.assertIsNone(result['first_reply_read_at'])

class ProducerIntegrationTests(unittest.TestCase):
    def test_actual_full_read_and_job_transition_receipts(self):
        from agent_chat.core import Coordinator
        from agent_chat.bridge_state import BridgeState
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/'actual.db'
            with closing(Coordinator(path,'sender')) as sender:
                sender.register('sender')
                with closing(Coordinator(path,'recipient')) as recipient:
                    recipient.register('recipient')
                    state=BridgeState(recipient)
                    state.bind(thread_id='00000000-0000-4000-8000-000000000001')
                    source=sender.send('recipient','fixture')['id']
                    job=state.prepare('00000000-0000-4000-8000-000000000001', state.pending(), 'fixture')
                    # The real trigger excludes prepared/cancelled statuses and
                    # records the first later status transition, not queue acceptance.
                    state.update(job['id'],'prepared')
                    self.assertIsNone(recipient.db.execute('SELECT recorded_at FROM bridge_job_native_attempts WHERE job_id=?', (job['id'],)).fetchone())
                    state.update(job['id'],'adding')
                    recipient.message(source)
                    recipient.db.execute('CREATE TABLE IF NOT EXISTS remote_session_hosts (session_id TEXT PRIMARY KEY,host_id TEXT,registered_at REAL,last_seen_at REAL)')
                    recipient.db.execute("INSERT INTO remote_session_hosts VALUES ('sender','host',0,0)")
                    recipient.db.commit()
            result=source_trace(path,project_id='default',session_id='sender',host_id='host',source_id=source)
            self.assertEqual(result['status'],'ok')
            self.assertIsInstance(result['source_first_full_read_at'],float)
            self.assertIsInstance(result['job_transition_recorded_at'],float)
            self.assertEqual(result['job_id'],job['id'])
