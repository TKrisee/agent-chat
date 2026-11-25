import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from agent_chat import provider_service as service

class StagingRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.db=sqlite3.connect(Path(self.tmp.name)/'journal.sqlite', timeout=0.1)
        self.addCleanup(self.db.close)
        self.db.execute('CREATE TABLE service_requests(id TEXT PRIMARY KEY,state TEXT,worker_pid INTEGER,worker_sec INTEGER,worker_usec INTEGER)')
        self.db.execute("INSERT INTO service_requests VALUES('request','staging',NULL,NULL,NULL)"); self.db.commit()
    def test_staging_transient_retries_and_commits_once(self):
        calls=[]
        def stage():
            calls.append(1)
            if len(calls)==1: raise service.Blocked('canonical SQLite row changed during capture')
            return SimpleNamespace(state='staged',reason='ok')
        self.assertTrue(service._stage_service_request(self.db,'request',stage))
        self.assertEqual(len(calls),2)
        self.assertEqual(self.db.execute('SELECT state FROM service_requests').fetchone()[0],'submit_intent')
        self.assertFalse(service._stage_service_request(self.db,'request',stage))
        self.assertEqual(len(calls),2)
    def test_other_failure_stays_staging(self):
        def stage(): raise service.Blocked('identity changed')
        with self.assertRaisesRegex(service.Blocked,'identity changed'):
            service._stage_service_request(self.db,'request',stage)
        self.assertEqual(self.db.execute('SELECT state FROM service_requests').fetchone()[0],'staging')
    def test_worker_identity_blocks_staging_recovery(self):
        self.db.execute("UPDATE service_requests SET worker_pid=123"); self.db.commit()
        with self.assertRaisesRegex(service.Blocked,'worker identity'):
            service._stage_service_request(self.db,'request',lambda:self.fail('must not capture'))
    def test_concurrent_attempt_cannot_capture(self):
        other=sqlite3.connect(Path(self.tmp.name)/'journal.sqlite',timeout=0.01)
        self.addCleanup(other.close)
        self.db.execute('BEGIN IMMEDIATE')
        with self.assertRaises(sqlite3.OperationalError):
            service._stage_service_request(other,'request',lambda:self.fail('concurrent capture'))
        self.db.rollback()

class LocalJournalSafetyTests(unittest.TestCase):
    def test_only_zero_effect_unresolved_service_checkpoint_resumes(self):
        from agent_chat import provider_recovery_lifecycle as lifecycle
        db=sqlite3.connect(':memory:'); self.addCleanup(db.close)
        db.execute('CREATE TABLE prestop_proofs(request_id TEXT PRIMARY KEY)')
        journal=SimpleNamespace(db=db)
        state=SimpleNamespace(state='unresolved',effects='none')
        self.assertTrue(lifecycle._resumable_local_stage(journal,'r',state,True))
        self.assertFalse(lifecycle._resumable_local_stage(journal,'r',state,False))
        self.assertFalse(lifecycle._resumable_local_stage(journal,'r',SimpleNamespace(state='stop_intent',effects='stop_unknown'),True))
        db.execute("INSERT INTO prestop_proofs VALUES('r')")
        with self.assertRaisesRegex(lifecycle.Blocked,'partial checkpoint'):
            lifecycle._resumable_local_stage(journal,'r',state,True)
