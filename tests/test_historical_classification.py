from unittest.mock import patch
import test_build01_admission as fixture
from agent_chat import successor_admission as s, successor_attempt_journal as j, root_local, capacity_observer
from agent_chat.core import CoordError

class HistoricalClassification(fixture.Build01Tests):
    def invoke(self,request):
        name={s.REQUEST:'capacity-successor-4621',s.E2D_REQUEST:'capacity-successor-e2d',s.BUILD_REQUEST:'capacity-successor-build01',s.NATIVE02_REQUEST:'capacity-successor-native02'}[request]
        return root_local.execute(name,{'confirm':True},master_db=self.master,service_plist=self.root/'service.plist',home_project='default',session=s.ROOT,host_id=s.HOST)
    def test_four_legacy_requests_report_unknown_and_no_replay_without_write(self):
        with patch.object(capacity_observer,'_native_observations',side_effect=AssertionError('native called')):
            for request in s.REQUESTS:
                with self.assertRaises(CoordError) as error: self.invoke(request)
                self.assertIn('request_id='+request,str(error.exception))
                self.assertIn('predicate=legacy_history_unknown',str(error.exception))
                self.assertIn('no replay',str(error.exception))
        self.assertFalse(self.db.execute("SELECT 1 FROM sqlite_master WHERE name='successor_producer_attempts'").fetchone())
    def test_known_committed_attempt_classified_without_replay(self):
        with j.verified_store(self.master,actor=s.ROOT,host=s.HOST,project='default',root_thread=s.ROOT_THREAD) as (db,check):
            attempt=j.begin(db,check,request_id=s.BUILD_REQUEST,operation='synthetic',actor=s.ROOT,host=s.HOST,target=str(self.master),source=s.BUILD_SOURCE,project='default')
            j.append(db,check,s.BUILD_REQUEST,attempt,'committed')
        with patch.object(capacity_observer,'_native_observations',side_effect=AssertionError('native called')):
            with self.assertRaises(CoordError) as error: self.invoke(s.BUILD_REQUEST)
        self.assertIn('attempt_id='+attempt,str(error.exception))
        self.assertIn('predicate=recorded_committed',str(error.exception))
        self.assertIn('no replay',str(error.exception))
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM successor_producer_attempt_events').fetchone()[0],2)
    def test_recorded_consumption_is_distinct_from_missing_journal(self):
        row=(s.REQUEST,s.RESOURCE,s.CONSUMER,s.THREAD,s.HOST,s.ROOT,'synthetic-manager','managed','synthetic-request',1,
             s.GUARD[0],s.GUARD[1],s.GUARD[3],s.GUARD[4],s.GUARD[5],s.GUARD[6],s.GUARD[7],s.GUARD[8],s.GUARD[9],'UNKNOWN')
        self.db.execute('INSERT INTO successor_admissions VALUES('+','.join('?' for _ in row)+')',row)
        self.db.execute('INSERT INTO successor_admission_uses VALUES(?,?,?,?,?)',(s.REQUEST,s.RESOURCE,s.CONSUMER,'synthetic-reservation',1.0)); self.db.commit()
        with patch.object(capacity_observer,'_native_observations',side_effect=AssertionError('native called')):
            with self.assertRaises(CoordError) as error: self.invoke(s.REQUEST)
        self.assertIn('predicate=recorded_consumed',str(error.exception))
        self.assertIn('no replay',str(error.exception))
        self.assertFalse(self.db.execute("SELECT 1 FROM sqlite_master WHERE name='successor_producer_attempts'").fetchone())
    def test_untrusted_history_is_unknown_and_never_runnable(self):
        self.db.execute('CREATE TABLE successor_producer_attempts(request_id TEXT)'); self.db.commit()
        with patch.object(capacity_observer,'_native_observations',side_effect=AssertionError('native called')):
            with self.assertRaises(CoordError) as error: self.invoke(s.REQUEST)
        self.assertIn('predicate=history_unavailable',str(error.exception))
        self.assertIn('no replay',str(error.exception))
