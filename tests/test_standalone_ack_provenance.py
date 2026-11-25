import contextlib,sqlite3,unittest
from agent_chat.core import CoordError,first_ack_lookup
from test_reply_ack import ReplyAcknowledgementTests

class StandaloneAckProvenance(ReplyAcknowledgementTests):
 def lookup(self,source):
  return first_ack_lookup(self.db,project_id='default',session_id=self.a,host_id='test-host',source_id=source,expected_recipient=self.b)
 def test_standalone_read_ack_does_not_claim_first_reply(self):
  source=self.call('send',self.b,to=self.a,body='informational source')['id']
  self.call('message',self.a,id=source); self.call('acknowledge',self.a,id=source)
  self.assertEqual(self.lookup(source)['status'],'not_recorded_at_read')
  with self.assertRaisesRegex(CoordError,'no message sent.*--ack-reply'):
   self.call('send',self.a,to=self.b,body='progress',reply_to=source)
  with contextlib.closing(sqlite3.connect(self.db)) as db:
   self.assertEqual(db.execute('select count(*) from messages').fetchone()[0],1)
  answer=self.call('send',self.a,to=self.b,body='progress',reply_to=source,ack_reply=True)
  retry=self.call('send',self.a,to=self.b,body='progress',reply_to=source,ack_reply=True)
  self.assertEqual(answer['id'],retry['id']);self.assertEqual(self.lookup(source)['reply_id'],answer['id'])
  with self.assertRaisesRegex(CoordError,'payload differs'):
   self.call('send',self.a,to=self.b,body='changed',reply_to=source,ack_reply=True)
  self.assertEqual(first_ack_lookup(self.db,project_id='default',session_id=self.b,host_id='test-host',source_id=source,expected_recipient=self.a)['status'],'unauthorized')
 def test_legacy_marker_is_not_retroactively_reclassified(self):
  source=self.call('send',self.b,to=self.a,body='old source')['id']
  with contextlib.closing(sqlite3.connect(self.db)) as db:
   with db:db.execute('update messages set acked_at=1 where id=?',(source,))
  self.call('acknowledge',self.a,id=source)
  self.assertEqual(self.lookup(source)['status'],'inconsistent')

 def test_ack_provenance_and_marker_rollback_together(self):
  source=self.call('send',self.b,to=self.a,body='rollback')['id']
  with contextlib.closing(sqlite3.connect(self.db)) as db:
   with db:db.execute("CREATE TRIGGER fail_standalone BEFORE INSERT ON standalone_message_acknowledgements BEGIN SELECT RAISE(ABORT, 'forced evidence failure'); END")
  with self.assertRaisesRegex(sqlite3.IntegrityError,'forced evidence failure'):
   self.call('acknowledge',self.a,id=source)
  with contextlib.closing(sqlite3.connect(self.db)) as db:
   self.assertIsNone(db.execute('select acked_at from messages where id=?',(source,)).fetchone()[0])
   self.assertEqual(db.execute('select count(*) from standalone_message_acknowledgements').fetchone()[0],0)
 def test_repeated_ack_and_unmatched_evidence_fail_closed(self):
  source=self.call('send',self.b,to=self.a,body='scope')['id']
  self.call('acknowledge',self.a,id=source);self.call('acknowledge',self.a,id=source)
  with contextlib.closing(sqlite3.connect(self.db)) as db:
   self.assertEqual(db.execute('select count(*) from standalone_message_acknowledgements').fetchone()[0],1)
   with db:db.execute('update standalone_message_acknowledgements set recipient_session=? where message_id=?',(self.c,source))
  self.assertEqual(self.lookup(source)['status'],'inconsistent')

def load_tests(loader,tests,pattern):
 return unittest.TestSuite(StandaloneAckProvenance(n) for n in ('test_standalone_read_ack_does_not_claim_first_reply','test_legacy_marker_is_not_retroactively_reclassified','test_ack_provenance_and_marker_rollback_together','test_repeated_ack_and_unmatched_evidence_fail_closed'))
