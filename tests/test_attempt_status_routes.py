import sqlite3
import unittest
from unittest.mock import patch
import test_build01_admission as fixture
from agent_chat import successor_admission as s, successor_attempt_journal as journal, root_local, capacity_observer
from agent_chat.core import CoordError

class AttemptStatusRoutes(fixture.Build01Tests):
    def status(self,request,attempt,session=s.ROOT,host=s.HOST):
        return root_local.execute('capacity-successor-attempt-status',{'request_id':request,'attempt_id':attempt},master_db=self.master,service_plist=self.root/'service.plist',home_project='default',session=session,host_id=host)
    def begin(self,request):
        with journal.verified_store(self.master,actor=s.ROOT,host=s.HOST,project='default',root_thread=s.ROOT_THREAD) as (db,check):
            return journal.begin(db,check,request_id=request,operation='synthetic',actor=s.ROOT,host=s.HOST,target=str(self.master),source=s.BUILD_SOURCE,project='default')
    def test_all_four_exact_attempts_through_root_without_effect(self):
        rows=[(request,self.begin(request)) for request in s.REQUESTS]
        with patch.object(s,'issue',side_effect=AssertionError('producer called')), patch.object(capacity_observer,'_native_observations',side_effect=AssertionError('native called')):
            for request,attempt in rows:
                result=self.status(request,attempt)
                self.assertEqual(result['producer_attempt']['attempt_id'],attempt)
                self.assertEqual(result['producer_attempt']['state'],'unknown')
                self.assertEqual(result['producer_attempt']['next_action'],'readback_only_no_replay')
                self.assertFalse(result['sql_mutation_performed'])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM successor_producer_attempt_events').fetchone()[0],4)
    def test_legacy_and_mismatch_do_not_claim_eligibility(self):
        legacy=self.status(s.REQUEST,'a'*32)
        self.assertEqual(legacy['producer_attempt']['state'],'legacy_history_unknown')
        self.assertEqual(legacy['producer_attempt']['next_action'],'no_replay')
        self.begin(s.REQUEST)
        self.assertEqual(self.status(s.REQUEST,'b'*32)['state'],'UNKNOWN')
    def test_identity_scope_and_tuple_rejected_before_read(self):
        foreign=self.status(s.REQUEST,'a'*32,session='not-root')
        self.assertEqual(foreign['state'],'UNKNOWN')
        self.assertNotIn('producer_attempt',foreign)
        with self.assertRaises(CoordError): self.status('not-a-fixed-id','a'*32)
        with self.assertRaises(CoordError): self.status(s.REQUEST,'not-an-attempt')
    def test_untrusted_journal_schema_reports_unknown(self):
        self.begin(s.REQUEST)
        self.db.execute('CREATE TRIGGER forged AFTER INSERT ON successor_producer_attempts BEGIN SELECT 1; END')
        self.db.commit()
        result=self.status(s.REQUEST,'a'*32)
        self.assertEqual(result['state'],'UNKNOWN')
        self.assertFalse(result['sql_mutation_performed'])
