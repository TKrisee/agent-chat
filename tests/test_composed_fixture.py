"""PRIVATE composition experiment. Synthetic authority; NOT an API or grant."""
import hashlib, os, sqlite3, tempfile, time, unittest
from pathlib import Path
from agent_chat import hospital_vendor_scope as s, hospital_vendor_output as o
from agent_chat.exact_file_read import snapshot
class ComposedFixture(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory(dir=os.environ.get('HOSPITAL_FIXTURE_TMP')); self.addCleanup(self.tmp.cleanup)
  self.base=Path(self.tmp.name); self.project=self.base/'project'; self.project.mkdir(mode=0o700)
  self.out=self.base/'outside'; self.out.mkdir(mode=0o700)
  self.db=sqlite3.connect(':memory:'); self.addCleanup(self.db.close)
  self.db.executescript('''CREATE TABLE resources(name TEXT PRIMARY KEY,owner_session TEXT,reservation_id TEXT,token TEXT,stale INTEGER,deadline REAL);
   CREATE TABLE remote_session_hosts(session_id TEXT PRIMARY KEY,host_id TEXT);
   CREATE TABLE messages(id TEXT PRIMARY KEY,sender_session TEXT,recipient_session TEXT,body TEXT,acked_at REAL);
   CREATE TABLE message_reads(message_id TEXT,reader_session TEXT,provenance TEXT);
   CREATE TABLE message_replies(message_id TEXT,reply_to TEXT);
   CREATE TABLE reply_ack_idempotency(message_id TEXT,sender_session TEXT,reply_to TEXT);''')
  self.db.executemany('INSERT INTO remote_session_hosts VALUES(?,?)',[(s.OWNER,s.HOST),(s.CONSUMER,s.HOST)])
  self.expected=s.manifest(); self.db.executemany('INSERT INTO resources VALUES(?,?,?,?,?,?)',[(k,s.OWNER,v[0],None,1,v[1]) for k,v in self.expected.items()])
  self.resource='operation:gameplay/private-test/read-copy'; self.rid='reservation_private'; self.token='fixture-only'
  self.db.execute('INSERT INTO resources VALUES(?,?,?,?,?,?)',(self.resource,s.CONSUMER,self.rid,self.token,0,time.time()+100))
  self.task='message_synthetic_task'; self.ack='message_synthetic_ack'; body='synthetic non-live task'; self.digest=hashlib.sha256(body.encode()).hexdigest()
  self.db.execute('INSERT INTO messages VALUES(?,?,?,?,?)',(self.task,s.UNITY_PO,s.CONSUMER,body,1)); self.db.execute('INSERT INTO messages VALUES(?,?,?,?,?)',(self.ack,s.CONSUMER,s.UNITY_PO,'ack',None))
  self.db.execute('INSERT INTO message_reads VALUES(?,?,?)',(self.task,s.CONSUMER,'recipient_full_message')); self.db.execute('INSERT INTO message_replies VALUES(?,?)',(self.ack,self.task)); self.db.execute('INSERT INTO reply_ack_idempotency VALUES(?,?,?)',(self.ack,s.CONSUMER,self.task))
  self.key=next(iter(self.expected)); self.source=self.project/self.key[5:]; self.source.parent.mkdir(parents=True); self.source.write_bytes(b'fixture payload')
 def gate(self,token=None):
  s.check_task_association(self.db,task_id=self.task,ack_id=self.ack,body_sha256=self.digest)
  s.check_owner_capability(self.db,bound_resource=self.resource,bound_rid=self.rid,supplied_token=token or self.token,now=time.time())
  s.check_current(self.db,project=s.PROJECT,consumer=s.CONSUMER,host=s.HOST,now=time.time())
 def compose(self,out,token=None):
  self.gate(token); fd,name=o.external_output_parent(str(self.project),str(out)); os.close(fd)
  return snapshot(str(self.source),str(out),1024,validate=lambda:self.gate(token))
 def test_private_synthetic_read_copy_leaves_claims_unchanged(self):
  before=self.db.execute('SELECT * FROM resources ORDER BY name').fetchall()
  self.compose(self.out/'copy'); self.assertEqual((self.out/'copy').read_bytes(),b'fixture payload')
  self.assertEqual(before,self.db.execute('SELECT * FROM resources ORDER BY name').fetchall()); self.assertEqual(self.source.read_bytes(),b'fixture payload')
 def test_foreign_token_no_copy(self):
  with self.assertRaises(s.Refused): self.compose(self.out/'copy',token='foreign')
  self.assertFalse((self.out/'copy').exists())
 def test_extra_claim_no_copy(self):
  self.db.execute('INSERT INTO resources VALUES(?,?,?,?,?,?)',('file:Assets/extra',s.OWNER,'other',None,1,1))
  with self.assertRaises(s.Refused): self.compose(self.out/'copy')
  self.assertFalse((self.out/'copy').exists())
 def test_project_output_no_copy(self):
  with self.assertRaises(o.Refused): self.compose(self.project/'copy')
  self.assertFalse((self.project/'copy').exists())
if __name__=='__main__': unittest.main(verbosity=2)
