"""Real disposable configured-provider CLI projection; no production stores."""
import contextlib
import hashlib
import io
from pathlib import Path
import sqlite3
import subprocess
import time
import unittest
import uuid
from unittest import mock
from agent_chat import root_local, root_restart
from agent_chat.core import CoordError
from agent_chat import root_pending_fresh as pending
import test_root_local as local_fixtures

class PendingFreshTests(unittest.TestCase):
    def setUp(self):
        self.fixture=local_fixtures.RootLocalTests('test_unenrolled_status_enrollment_and_route_use_real_local_apis')
        self.fixture.setUp(); self.addCleanup(self.fixture.doCleanups); self.fixture.enroll()
        self.registry=Path(str(self.fixture.master)+'.projects.sqlite3')
        # Existing sanctioned schema builder is fixture setup only, before snapshots.
        with contextlib.closing(sqlite3.connect(self.fixture.master)) as db, db: root_restart.local_schema(db)
    def insert(self, db, table, values):
        with contextlib.closing(sqlite3.connect(db)) as connection, connection:
            connection.execute('INSERT INTO '+table+' ('+','.join(values)+') VALUES ('+','.join('?' for _ in values)+')',tuple(values.values()))
    def request(self,status='created',**changes):
        values=dict(id=str(uuid.uuid4()),home_project='default',home_session='root',host_id='host',old_thread_id=self.fixture.thread,
                    status=status,prompt='PRIVATE_PROMPT',instruction_origin='synthetic',error='PRIVATE_ERROR',native_proof_json='PRIVATE_PROOF',created_at=time.time()-20,updated_at=time.time()-1)
        values.update(changes);self.insert(self.registry,'root_fresh_requests',values);return values
    def scope(self,row,**changes):
        values=dict(request_id=row['id'],project_id='default',root_session='root');values.update(changes)
        self.insert(self.registry,'root_fresh_projects',values)
    def gate(self,row,active=1):
        self.insert(self.fixture.master,'root_fresh_gates',dict(request_id=row['id'],root_session='root',old_thread_id=row['old_thread_id'],new_thread_id=None,active=active))
    def audit(self,row,state='cancelled',**changes):
        values=dict(cancellation_id=str(uuid.uuid4()),request_id=row['id'],home_project='default',home_session='root',host_id='host',expected_thread=row['old_thread_id'],candidate_thread_id=str(uuid.uuid4()),outcome='no_native_effect',reason='PRIVATE_REASON',authorization_json='PRIVATE_AUTH',state=state,created_at=time.time()-10,updated_at=time.time())
        values.update(changes);self.insert(self.registry,'root_fresh_pending_cancellations',values)
    def snapshot(self):
        result=[]
        for path in (self.fixture.master,self.registry):
            with contextlib.closing(sqlite3.connect(path)) as db: result.append(tuple(db.iterdump()))
        return result
    def cli(self,expected='.preflight_clearance==false and .native_closure=="UNKNOWN"',**identity):
        before=self.snapshot();out=io.StringIO()
        values=dict(self.fixture.identity,**identity)
        args=['--master-db',str(values['master_db']),'--service-plist',str(values['service_plist']),'--home-project',values['home_project'],'--home-session',values['session'],'--host-id',values['host_id'],'pending-fresh','--expected-thread',self.fixture.thread]
        with contextlib.redirect_stdout(out): self.assertEqual(root_local.main(args),0)
        raw=out.getvalue();check=subprocess.run(['jq','-e',expected],input=raw,text=True,capture_output=True)
        self.assertEqual(check.returncode,0,check.stderr+raw)
        self.assertNotIn('PRIVATE_',raw);self.assertEqual(before,self.snapshot());return raw
    def test_no_row_real_cli_no_constructor_or_logical_write(self):
        with mock.patch('agent_chat.projects.Projects',side_effect=AssertionError('constructor forbidden')):
            self.cli('.request_count==0 and .complete and (.cross_store_atomic|not) and (.preflight_clearance|not)')
        self.assertEqual(root_local.FIELDS['startup-plan'],{'projects','expected_thread'})
    def test_own_pending_and_completed_exact_predicate(self):
        row=self.request();self.scope(row);self.gate(row);self.request('completed')
        self.cli('.request_count==1 and .requests[0].status=="created" and .requests[0].scopes[0].gate.active==1 and .requests[0].scopes[0].evidence_state=="OBSERVED" and (.preflight_clearance|not)')
    def test_retired_cancelled_and_partial_audits_stay_nonclearance(self):
        for status,state in [('retired','cancelled'),('cancelled','cancelled'),('retired','cancelling')]:
            row=self.request(status);self.scope(row);self.gate(row,0);self.audit(row,state)
        self.cli('.request_count==3 and all(.requests[]; .audits.root_fresh_pending_cancellations.present and .terminal_or_active_disposition=="UNASSESSED") and (.preflight_clearance|not)')
    def test_foreign_scope_never_opens_foreign_store(self):
        row=self.request(home_session='foreign');self.scope(row,project_id='not-registered')
        self.cli('(.complete|not) and .requests[0].scopes[0].evidence_state=="UNKNOWN" and .requests[0].scopes[0].reason=="foreign_request_binding_not_opened"')
    def test_missing_and_foreign_audit_associations_unknown(self):
        row=self.request();self.scope(row);self.gate(row);self.audit(row,home_session='foreign')
        self.cli('(.complete|not) and .requests[0].audits.root_fresh_pending_cancellations == {"present":null,"evidence_state":"UNKNOWN","reason":"audit_request_identity_mismatch"}')
    def test_bound_all_or_refuse_and_no_logical_write(self):
        for i in range(65): self.request()
        before=self.snapshot()
        with self.assertRaisesRegex(CoordError,'bound exceeded'): self.cli()
        self.assertEqual(before,self.snapshot())
    def test_wrong_caller_thread_host_and_uid_denied(self):
        before=self.snapshot()
        for changes in ({'session':'foreign'},{'host_id':'foreign'},{'home_project':'foreign'}):
            with self.subTest(changes=changes),self.assertRaises(CoordError): self.cli(**changes)
        with self.assertRaises(CoordError): root_local.execute('pending-fresh',{'expected_thread':str(uuid.uuid4())},**self.fixture.identity)
        for field in ('getuid','geteuid'):
            with mock.patch('agent_chat.root_local.os.'+field,return_value=999999),self.assertRaises(CoordError): self.cli()
        self.assertEqual(before,self.snapshot())
    def test_ambiguous_audit_is_unknown_not_arbitrary_first(self):
        row=self.request();self.scope(row);self.gate(row)
        with contextlib.closing(sqlite3.connect(self.registry)) as db, db:
            db.execute('DROP TABLE root_fresh_pending_cancellations')
            db.execute('CREATE TABLE root_fresh_pending_cancellations ('+','.join(c+' '+('REAL' if c in ('created_at','updated_at') else 'TEXT') for c in (*pending.AUDITS['root_fresh_pending_cancellations'],'reason','authorization_json'))+')')
        self.audit(row);self.audit(row)
        self.cli('(.complete|not) and .requests[0].audits.root_fresh_pending_cancellations.evidence_state=="UNKNOWN"')
    def test_scope_binding_and_gate_mismatch_unknown(self):
        row=self.request();self.scope(row,root_session='foreign');self.gate(row)
        self.cli('(.complete|not) and .requests[0].scopes[0].evidence_state=="UNKNOWN"')
    def test_malformed_timestamp_refuses_without_logical_write(self):
        self.request(created_at='invalid');before=self.snapshot()
        with self.assertRaisesRegex(CoordError,'timestamp'): self.cli()
        self.assertEqual(before,self.snapshot())

    def test_total_scope_bound_refuses_before_foreign_open(self):
        row=self.request(home_session='foreign')
        for i in range(65): self.scope(row,project_id='synthetic-'+str(i))
        before=self.snapshot()
        with self.assertRaisesRegex(CoordError,'scope bound exceeded'): self.cli()
        self.assertEqual(before,self.snapshot())
    def test_absent_optional_audit_unknown(self):
        row=self.request();self.scope(row);self.gate(row)
        with contextlib.closing(sqlite3.connect(self.registry)) as db, db: db.execute('DROP TABLE root_fresh_pending_cancellations')
        self.cli('(.complete|not) and .requests[0].audits.root_fresh_pending_cancellations.evidence_state=="UNAVAILABLE"')
