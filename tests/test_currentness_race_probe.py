"""Private test: binding revocation between authorization and transaction."""
import unittest
import uuid
import time
import contextlib
import sqlite3
from unittest import mock
from agent_chat import exact_file_bindings as bindings
from agent_chat.remote import RemoteCoordError
from test_currentness_recovery import CurrentnessRecoveryTests

class RaceProbe(CurrentnessRecoveryTests):
    def assert_no_acquisition(self, binding):
        with contextlib.closing(sqlite3.connect(self.server.projects.db_path(self.project_id))) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM resources WHERE name=? AND owner_session IS NOT NULL',(str(self.source),)).fetchone()[0],0)
            self.assertEqual(db.execute('SELECT count(*) FROM exact_file_binding_reservations WHERE binding_id=?',(binding['id'],)).fetchone()[0],0)
            self.assertEqual(db.execute('SELECT count(*) FROM resource_queue WHERE resource=? AND session=?',(str(self.source),self.worker)).fetchone()[0],0)
            self.assertEqual(db.execute('SELECT count(*) FROM remote_guarded_runs WHERE resource=? AND session_id=?',(str(self.source),self.worker)).fetchone()[0],0)
            self.assertEqual(db.execute('SELECT count(*) FROM guarded_runs WHERE resource=? AND session=?',(str(self.source),self.worker)).fetchone()[0],0)
        self.assertFalse(self.cap.exists()); self.assertFalse(self.output.exists())

    def test_revoked_between_outer_check_and_currentness_is_not_recovery(self):
        binding=self.grant()
        self.po_call('send',to=self.worker,body='Private fresh intake')
        self.po_call('context')
        original=bindings.authorize
        called=[]
        def revoke_after_outer(*args,**kwargs):
            result=original(*args,**kwargs)
            if not called:
                called.append(True)
                self.po_call('exact-file-revoke',binding_id=binding['id'],request_id=str(uuid.uuid4()),reason='private race test',confirm=True)
            return result
        with mock.patch.object(bindings,'authorize',side_effect=revoke_after_outer):
            with self.assertRaisesRegex(RemoteCoordError,'revoked') as error:
                self.po_call('exact-file-request',session=self.worker,binding_id=binding['id'],path=str(self.source),minutes=5)
        self.assertEqual(len(called),1)
        self.assertIsNone(getattr(error.exception,'recovery',None))
        self.assert_no_acquisition(binding)

    def test_expired_between_outer_check_and_currentness_is_not_recovery(self):
        payload=self.binding_payload(); payload['minutes']=0.02
        self.po_call('context'); binding=self.po_call('exact-file-bind',**payload)
        self.po_call('send',to=self.worker,body='Private fresh intake')
        original=bindings.authorize; called=[]
        def expire_after_outer(*args,**kwargs):
            result=original(*args,**kwargs)
            if not called:
                called.append(True)
                remaining=binding['expires_at']-time.time()
                if remaining>0: time.sleep(remaining+0.02)
            return result
        with mock.patch.object(bindings,'authorize',side_effect=expire_after_outer):
            with self.assertRaisesRegex(RemoteCoordError,'expired') as error:
                self.po_call('exact-file-request',session=self.worker,binding_id=binding['id'],path=str(self.source),minutes=5)
        self.assertEqual(len(called),1)
        self.assertIsNone(getattr(error.exception,'recovery',None)); self.assert_no_acquisition(binding)

    def test_task_blocked_between_outer_check_and_currentness_is_not_recovery(self):
        binding=self.grant()
        self.po_call('send',to=self.worker,body='Private fresh intake')
        self.po_call('context')
        original=bindings.authorize; called=[]
        def block_after_outer(*args,**kwargs):
            result=original(*args,**kwargs)
            if not called:
                called.append(True)
                self.po_call('activity-transition',work_id=self.task['id'],state='blocked')
            return result
        with mock.patch.object(bindings,'authorize',side_effect=block_after_outer):
            with self.assertRaisesRegex(RemoteCoordError,'runnable task') as error:
                self.po_call('exact-file-request',session=self.worker,binding_id=binding['id'],path=str(self.source),minutes=5)
        self.assertEqual(len(called),1)
        self.assertIsNone(getattr(error.exception,'recovery',None)); self.assert_no_acquisition(binding)

def load_tests(loader,tests,pattern):
    names=sorted(k for k in RaceProbe.__dict__ if k.startswith('test_'))
    return unittest.TestSuite(RaceProbe(name) for name in names)
