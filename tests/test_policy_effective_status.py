import contextlib
import sqlite3
import unittest
import test_root_local as local
from agent_chat import communication, root_local
from agent_chat.core import CoordError

class EffectiveStatusTests(unittest.TestCase):
    setUp=local.RootLocalTests.setUp
    call=local.RootLocalTests.call
    enroll=local.RootLocalTests.enroll
    snapshot=local.RootLocalTests.snapshot
    def setUp(self):
        local.RootLocalTests.setUp(self); self.enroll()
        with contextlib.closing(sqlite3.connect(self.projects.path)) as db:
            db.executescript('''CREATE TABLE native_policy_offers(host TEXT,thread TEXT,client_id TEXT,version TEXT,project TEXT,kind TEXT,payload_sha256 TEXT,created_at REAL,PRIMARY KEY(host,thread,client_id));
                CREATE TABLE native_policy_receipts(host TEXT,thread TEXT,version TEXT,client_id TEXT,turn_id TEXT,observed_at REAL,PRIMARY KEY(host,thread,version));''');db.commit()
    def rows(self,table,values):
        with contextlib.closing(sqlite3.connect(self.projects.path)) as db:
            db.execute('INSERT INTO '+table+' VALUES('+','.join('?' for _ in values)+')',values);db.commit()
    def schema(self):
        with contextlib.closing(sqlite3.connect(self.projects.path)) as db:
            db.executescript('''CREATE TABLE native_policy_verified_offers(host TEXT,thread TEXT,client_id TEXT,version TEXT,payload_sha256 TEXT,PRIMARY KEY(host,thread,client_id));
                CREATE TABLE native_policy_verified_receipts(host TEXT,thread TEXT,project TEXT,version TEXT,client_id TEXT,turn_id TEXT,observed_at REAL,PRIMARY KEY(host,thread,project,version));''');db.commit()
    def offer(self,client,project='default',host='host',thread=None,digest='a'*64):
        thread=thread or self.thread; v=communication.policy_version()
        self.rows('native_policy_offers',(host,thread,client,v,project,'wake',digest,1))
        return v
    def status(self,**params): return self.call('policy-effective-status',expected_thread=params.get('thread',self.thread))
    def test_legacy_only_is_historical_unknown_no_schema_mutation(self):
        v=self.offer('old');self.rows('native_policy_receipts',('host',self.thread,v,'old','old-turn',2))
        before=self.snapshot(); result=self.status();self.assertEqual(self.snapshot(),before)
        self.assertEqual(result['effective_state'],'unknown')
        self.assertEqual(result['verified_schema'],'unavailable')
        self.assertEqual(result['verified_receipts'],[])
        self.assertEqual(result['historical_receipts'][0]['client_id'],'old')
        self.assertEqual(result['historical_receipts'][0]['full_policy_completeness'],'unknown')
    def test_separate_verified_current_and_legacy_rows(self):
        self.schema(); v=self.offer('old');self.rows('native_policy_receipts',('host',self.thread,v,'old','old-turn',2))
        self.offer('new');self.rows('native_policy_verified_offers',('host',self.thread,'new',v,'a'*64));self.rows('native_policy_verified_receipts',('host',self.thread,'default',v,'new','new-turn',3))
        result=self.status();self.assertEqual(result['effective_state'],'verified_current')
        self.assertEqual([(x['client_id'],x['turn_id']) for x in result['historical_receipts']],[('old','old-turn')])
        self.assertEqual([(x['client_id'],x['turn_id']) for x in result['verified_receipts']],[('new','new-turn')])
    def test_unrelated_or_digest_mismatch_never_associates(self):
        self.schema();v=communication.policy_version()
        for client,project,host,thread,digest in [('x','other','host',self.thread,'a'*64),('y','default','foreign',self.thread,'a'*64),('z','default','host','other','a'*64),('bad','default','host',self.thread,'b'*64)]:
            actual_version=v+'-other' if client=='bad' else v
            self.rows('native_policy_offers',(host,thread,client,actual_version,project,'wake',digest,1));self.rows('native_policy_verified_offers',(host,thread,client,actual_version,'a'*64));self.rows('native_policy_verified_receipts',(host,thread,project,actual_version,client,'turn',3))
        result=self.status();self.assertEqual(result['effective_state'],'unknown');self.assertEqual(result['verified_receipts'],[])
    def test_invalid_verified_timestamp_and_kind_are_not_effective(self):
        self.schema();v=self.offer('bad')
        self.rows('native_policy_verified_offers',('host',self.thread,'bad',v,'a'*64))
        self.rows('native_policy_verified_receipts',('host',self.thread,'default',v,'bad','turn',0))
        result=self.status();self.assertEqual(result['effective_state'],'unknown');self.assertEqual(result['verified_receipts'],[])
    def test_wrong_client_and_partial_schema_are_unknown(self):
        self.schema();v=self.offer('actual')
        self.rows('native_policy_verified_offers',('host',self.thread,'actual',v,'a'*64))
        self.rows('native_policy_verified_receipts',('host',self.thread,'default',v,'foreign-client','turn',3))
        result=self.status();self.assertEqual(result['effective_state'],'unknown');self.assertEqual(result['verified_receipts'],[])
    def test_incomplete_verified_schema_is_unknown_and_keeps_history(self):
        v=self.offer('old');self.rows('native_policy_receipts',('host',self.thread,v,'old','old-turn',2))
        with contextlib.closing(sqlite3.connect(self.projects.path)) as db:
            db.execute('CREATE TABLE native_policy_verified_offers(host TEXT)');db.commit()
        result=self.status();self.assertEqual(result['effective_state'],'unknown')
        self.assertEqual(result['verified_schema'],'unavailable')
        self.assertEqual(result['historical_receipts'][0]['client_id'],'old')
    def test_verified_receipt_with_missing_required_columns_is_unknown(self):
        with contextlib.closing(sqlite3.connect(self.projects.path)) as db:
            db.execute('CREATE TABLE native_policy_verified_offers(host TEXT)')
            db.execute('CREATE TABLE native_policy_verified_receipts(host TEXT)');db.commit()
        result=self.status();self.assertEqual(result['effective_state'],'unknown')
        self.assertEqual(result['verified_schema'],'unavailable')
    def test_directory_owner_mismatch_is_refused(self):
        with contextlib.closing(sqlite3.connect(self.projects.path)) as db:
            db.execute("UPDATE root_directory_owner SET home_session='foreign' WHERE singleton=1");db.commit()
        with self.assertRaises(CoordError): self.status()
    def test_foreign_scope_and_thread_are_refused(self):
        for key in ('home_project','session','host_id'):
            identity=dict(self.identity);identity[key]='foreign'
            with self.subTest(key=key),self.assertRaises(CoordError): root_local.execute('policy-effective-status',{'expected_thread':self.thread},**identity)
        with self.assertRaises(CoordError):self.status(thread='other')
