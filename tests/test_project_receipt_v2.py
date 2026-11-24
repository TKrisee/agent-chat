import contextlib
import sqlite3
import unittest
import test_root_local as local
from agent_chat import communication

class ProjectReceiptV2(unittest.TestCase):
    setUp=local.RootLocalTests.setUp
    call=local.RootLocalTests.call
    enroll=local.RootLocalTests.enroll
    def setUp(self):
        local.RootLocalTests.setUp(self);self.enroll()
    def scenario(self,receipt_project,include_project=True):
        v=communication.policy_version();thread=self.thread
        with contextlib.closing(sqlite3.connect(self.projects.path)) as db:
            db.executescript('''CREATE TABLE native_policy_offers(host TEXT,thread TEXT,client_id TEXT,version TEXT,project TEXT,kind TEXT,payload_sha256 TEXT,created_at REAL,PRIMARY KEY(host,thread,client_id));
                CREATE TABLE native_policy_receipts(host TEXT,thread TEXT,version TEXT,client_id TEXT,turn_id TEXT,observed_at REAL,PRIMARY KEY(host,thread,version));
                CREATE TABLE native_policy_verified_offers(host TEXT,thread TEXT,client_id TEXT,version TEXT,payload_sha256 TEXT,PRIMARY KEY(host,thread,client_id));''')
            if include_project:
                db.execute('CREATE TABLE native_policy_verified_receipts(host TEXT,thread TEXT,project TEXT,version TEXT,client_id TEXT,turn_id TEXT,observed_at REAL,PRIMARY KEY(host,thread,project,version))')
                db.execute('INSERT INTO native_policy_verified_receipts VALUES(?,?,?,?,?,?,?)',('host',thread,receipt_project,v,'client','turn',3))
            else:
                db.execute('CREATE TABLE native_policy_verified_receipts(host TEXT,thread TEXT,version TEXT,client_id TEXT,turn_id TEXT,observed_at REAL,PRIMARY KEY(host,thread,version))')
                db.execute('INSERT INTO native_policy_verified_receipts VALUES(?,?,?,?,?,?)',('host',thread,v,'client','turn',3))
            db.execute('INSERT INTO native_policy_offers VALUES(?,?,?,?,?,?,?,?)',('host',thread,'client',v,'default','wake','a'*64,1))
            db.execute('INSERT INTO native_policy_verified_offers VALUES(?,?,?,?,?)',('host',thread,'client',v,'a'*64));db.commit()
        return self.call('policy-effective-status',expected_thread=thread)
    def test_missing_project_column_never_verifies(self):
        result=self.scenario('default',False)
        self.assertEqual(result['effective_state'],'unknown');self.assertEqual(result['verified_schema'],'unavailable')
        self.assertEqual(result['verified_receipts'],[])
    def test_receipt_project_mismatch_never_verifies(self):
        result=self.scenario('other-synthetic-project')
        self.assertEqual(result['effective_state'],'unknown');self.assertEqual(result['verified_receipts'],[])
    def test_exact_project_receipt_verifies(self):
        result=self.scenario('default')
        self.assertEqual(result['effective_state'],'verified_current')
        self.assertEqual(result['verified_receipts'][0]['client_id'],'client')
