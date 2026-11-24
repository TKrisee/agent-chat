import uuid
import threading
import sqlite3
import contextlib
import subprocess
import unittest
import test_exact_file_binding as fixture
from agent_chat.remote import RemoteCoordError

class ProspectiveDeferredTests(fixture.ExactFileBindingTests):
    def acquire(self):
        request_id=str(uuid.uuid4())
        result=self.po_call('deferred-request',session=self.worker,request_id=request_id,
            resource='file:guard-file.txt',minutes=5,task_id=self.task['id'],source_id=self.assignment)
        return request_id,result

    def test_cancel_is_idempotent_and_repeated_grant_withholds_token(self):
        request_id,claim=self.acquire()
        self.assertIsNone(claim['token'])
        repeated=self.po_call('request',session=self.worker,resource='file:guard-file.txt',minutes=5)
        self.assertIsNone(repeated['token'])
        first=self.po_call('deferred-cancel',session=self.worker,request_id=request_id)
        self.assertEqual(first['state'],'cancelled')
        self.assertEqual(first['physical_effects'],'unknown')
        self.assertEqual(self.po_call('deferred-cancel',session=self.worker,request_id=request_id)['state'],'cancelled')
        with self.assertRaises(RemoteCoordError): self.po_call('deferred-activate',session=self.worker,request_id=request_id)

    def test_activation_reconciles_and_prevents_unused_return_and_unscoped_guard(self):
        request_id,claim=self.acquire()
        active=self.po_call('deferred-activate',session=self.worker,request_id=request_id)
        self.assertTrue(active['token'])
        self.assertEqual(self.po_call('deferred-activate',session=self.worker,request_id=request_id)['token'],active['token'])
        with self.assertRaises(RemoteCoordError): self.po_call('deferred-cancel',session=self.worker,request_id=request_id)
        with self.assertRaises(RemoteCoordError): self.po_call('begin-guard',session=self.worker,resource='file:guard-file.txt',token=active['token'])

    def test_wrong_source_creates_no_reservation(self):
        with self.assertRaises(RemoteCoordError):
            self.po_call('deferred-request',session=self.worker,request_id=str(uuid.uuid4()),resource='file:guard-file.txt',minutes=5,task_id=self.task['id'],source_id='wrong')
        with contextlib.closing(sqlite3.connect(self.server.projects.db_path(self.project_id))) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM resources WHERE name='file:guard-file.txt' AND owner_session IS NOT NULL").fetchone()[0],0)

    def test_activation_cancel_race_has_one_winner(self):
        request_id,_=self.acquire()
        barrier=threading.Barrier(3); outcomes=[]
        def call(op):
            barrier.wait()
            try: outcomes.append(self.po_call(op,session=self.worker,request_id=request_id)['state'])
            except RemoteCoordError: outcomes.append('rejected')
        threads=[threading.Thread(target=call,args=(op,)) for op in ('deferred-activate','deferred-cancel')]
        for t in threads: t.start()
        barrier.wait()
        for t in threads: t.join(8)
        self.assertTrue(all(not t.is_alive() for t in threads))
        self.assertEqual(outcomes.count('rejected'),1)
        winner=next(x for x in outcomes if x!='rejected')
        self.assertIn(winner,('active','cancelled'))
        self.assertEqual(self.po_call('deferred-status',session=self.worker,request_id=request_id)['state'],winner)

    def test_batch_and_wrong_host_are_denied(self):
        request_id,claim=self.acquire()
        with self.assertRaises(RemoteCoordError): self.po_call('deferred-activate',session=self.worker,host='other-host',request_id=request_id)
        token=self.po_call('deferred-activate',session=self.worker,request_id=request_id)['token']
        with self.assertRaisesRegex(RemoteCoordError,'deferred-origin reservation cannot authorize batch'):
            self.po_call('begin-guard-batch',session=self.worker,tuples=[dict(resource='file:guard-file.txt',session_id=self.worker,host_id='host',token=token,reservation_id=claim['reservation_id'])])

    def test_activated_declared_guard_admits_exact_scope(self):
        request_id,claim=self.acquire()
        token=self.po_call('deferred-activate',session=self.worker,request_id=request_id)['token']
        guard=self.po_call('begin-guard',session=self.worker,resource='file:guard-file.txt',token=token,
            reservation_id=claim['reservation_id'],require_host_witness=True,declared_task_id=self.task['id'],declared_source_id=self.assignment)
        self.assertEqual(guard['reservation_id'],claim['reservation_id'])

    def test_cli_request_activate_and_private_capability(self):
        request_id=str(uuid.uuid4())
        output=self.cli_call(self.worker,'deferred-request','file:guard-file.txt','--minutes','5',
            '--task-id',self.task['id'],'--source-id',self.assignment,'--request-id',request_id)
        subprocess.run(['jq','-e','.token == null and .authority_issued == false'],input=output,text=True,capture_output=True,check=True)
        cap=self.fixture/'output'/'deferred-cap.json'
        active=self.cli_call(self.worker,'deferred-activate','--request-id',request_id,'--capability-file',str(cap))
        subprocess.run(['jq','-e','.state == "active" and (has("token") | not)'],input=active,text=True,capture_output=True,check=True)
        subprocess.run(['jq','-e','.token | type == "string"',str(cap)],capture_output=True,check=True)

    def test_task_no_longer_runnable_prevents_activation(self):
        request_id,_=self.acquire()
        self.po_call('activity-transition',session=self.worker,work_id=self.task['id'],state='blocked')
        with self.assertRaises(RemoteCoordError): self.po_call('deferred-activate',session=self.worker,request_id=request_id)
        self.assertEqual(self.po_call('deferred-status',session=self.worker,request_id=request_id)['state'],'deferred')
        self.assertEqual(self.po_call('deferred-cancel',session=self.worker,request_id=request_id)['state'],'cancelled')

    def test_cancelled_request_uuid_cannot_be_reused(self):
        request_id,_=self.acquire()
        self.po_call('deferred-cancel',session=self.worker,request_id=request_id)
        with self.assertRaisesRegex(RemoteCoordError,'already exists'):
            self.po_call('deferred-request',session=self.worker,request_id=request_id,resource='file:guard-file.txt',minutes=5,task_id=self.task['id'],source_id=self.assignment)
        self.assertEqual(self.po_call('deferred-status',session=self.worker,request_id=request_id)['state'],'cancelled')

    def test_activated_stale_reservation_retains_restoration_admission(self):
        request_id,claim=self.acquire()
        token=self.po_call('deferred-activate',session=self.worker,request_id=request_id)['token']
        with sqlite3.connect(self.server.projects.db_path(self.project_id)) as db:
            db.execute('UPDATE resources SET stale=1 WHERE reservation_id=?',(claim['reservation_id'],))
        guard=self.po_call('begin-guard',session=self.worker,resource='file:guard-file.txt',token=token,
                           reservation_id=claim['reservation_id'],restore=True,max_seconds=30)
        self.assertEqual(guard['reservation_id'],claim['reservation_id'])
        with self.assertRaises(RemoteCoordError): self.po_call('deferred-cancel',session=self.worker,request_id=request_id)

    def test_another_owner_cannot_observe_or_activate(self):
        request_id,_=self.acquire()
        with self.assertRaises(RemoteCoordError): self.po_call('deferred-status',session=self.po,request_id=request_id)
        with self.assertRaises(RemoteCoordError): self.po_call('deferred-activate',session=self.po,request_id=request_id)

    def test_busy_resource_does_not_enqueue_deferred_request(self):
        self.po_call('request',session=self.po,resource='file:guard-file.txt',minutes=5)
        with self.assertRaisesRegex(RemoteCoordError,'no queue entry'):
            self.acquire()
        with contextlib.closing(sqlite3.connect(self.server.projects.db_path(self.project_id))) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM resource_queue WHERE session=?',(self.worker,)).fetchone()[0],0)
            self.assertEqual(db.execute('SELECT count(*) FROM deferred_reservations').fetchone()[0],0)

    def test_unexpected_guard_history_blocks_unused_return(self):
        request_id,claim=self.acquire()
        with sqlite3.connect(self.server.projects.db_path(self.project_id)) as db:
            db.execute('INSERT INTO remote_guarded_runs VALUES(?,?,?,?,?,?,?,?,?)',
                ('synthetic-run','file:guard-file.txt',claim['reservation_id'],self.worker,'host',0,1.0,None,None))
        with self.assertRaisesRegex(RemoteCoordError,'unexpected execution history'):
            self.po_call('deferred-cancel',session=self.worker,request_id=request_id)
        self.assertEqual(self.po_call('deferred-status',session=self.worker,request_id=request_id)['state'],'deferred')


def load_tests(loader, standard_tests, pattern):
    # The private fixture's original tests remain owned by their own module.
    names=sorted(name for name in ProspectiveDeferredTests.__dict__ if name.startswith('test_'))
    return unittest.TestSuite(ProspectiveDeferredTests(name) for name in names)
