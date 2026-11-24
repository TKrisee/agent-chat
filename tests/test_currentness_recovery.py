import contextlib
import sqlite3
import unittest
import uuid
import urllib.error
import io
import sys
import subprocess
from unittest import mock
import test_exact_file_binding as fixture
from agent_chat.remote import RemoteCoordError

class CurrentnessRecoveryTests(fixture.ExactFileBindingTests):
    def test_real_freshness_failure_is_typed_and_recoverable(self):
        binding=self.grant()
        notice=self.po_call('send',to=self.worker,body='Private coordination update before acquisition.')
        with self.assertRaises(RemoteCoordError) as caught:
            self.acquire(binding)
        with contextlib.closing(sqlite3.connect(self.server.projects.db_path(self.project_id))) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM resources WHERE name=? AND owner_session IS NOT NULL',(str(self.source),)).fetchone()[0],0)
            self.assertEqual(db.execute('SELECT count(*) FROM exact_file_binding_reservations WHERE binding_id=?',(binding['id'],)).fetchone()[0],0)
            self.assertEqual(db.execute('SELECT count(*) FROM remote_guarded_runs').fetchone()[0],0)
        self.assertFalse(self.cap.exists()); self.assertFalse(self.output.exists())
        recovery=getattr(caught.exception,'recovery',None)
        self.assertIsInstance(recovery,dict)
        self.assertEqual(recovery['kind'],'inbox_currentness')
        self.assertEqual(recovery['action'],'exact-file-request')
        self.assertEqual(recovery['owner_session'],self.worker)
        self.assertTrue(recovery['requested_effect_absent'])
        self.assertTrue(recovery['eligibility_unverified'])
        self.assertEqual(recovery['minutes'],5.0)
        self.po_call('message',session=self.worker,id=notice['id'])
        _,rid=self.acquire(binding)
        self.assertTrue(rid)

    def test_new_message_after_consumption_retriggers_guard(self):
        binding=self.grant()
        first=self.po_call('send',to=self.worker,body='First update')
        with self.assertRaises(RemoteCoordError) as caught: self.acquire(binding)
        self.assertEqual(caught.exception.recovery['kind'],'inbox_currentness')
        self.po_call('message',session=self.worker,id=first['id'])
        second=self.po_call('send',to=self.worker,body='Later update')
        with self.assertRaises(RemoteCoordError) as again: self.acquire(binding)
        self.assertEqual(again.exception.recovery['kind'],'inbox_currentness')
        self.po_call('message',session=self.worker,id=second['id'])
        self.assertTrue(self.acquire(binding)[1])

    def test_existing_acquisition_never_claims_no_effect(self):
        binding=self.grant(); self.acquire(binding)
        self.po_call('send',to=self.worker,body='After acquisition')
        with self.assertRaises(RemoteCoordError) as caught:
            self.po_call('exact-file-request',session=self.worker,binding_id=binding['id'],path=str(self.source),minutes=5)
        self.assertIsNone(getattr(caught.exception,'recovery',None))

    def test_wrong_scope_and_revoked_binding_are_not_currentness(self):
        binding=self.grant()
        notice=self.po_call('send',to=self.worker,body='Freshness pending')
        with self.assertRaises(RemoteCoordError) as bad:
            self.po_call('exact-file-request',session=self.worker,binding_id=binding['id'],path=str(self.source)+'wrong',minutes=5)
        self.assertIsNone(getattr(bad.exception,'recovery',None))
        self.po_call('context')
        self.po_call('exact-file-revoke',binding_id=binding['id'],request_id=str(uuid.uuid4()),reason='private fixture',confirm=True)
        self.po_call('message',session=self.worker,id=notice['id'])
        with self.assertRaises(RemoteCoordError) as revoked: self.acquire(binding)
        self.assertIsNone(getattr(revoked.exception,'recovery',None))
        self.assertFalse(self.cap.exists())

    def test_transport_unknown_is_never_labeled_no_effect(self):
        binding=self.grant()
        with mock.patch('urllib.request.OpenerDirector.open',side_effect=urllib.error.URLError('private transport loss')):
            with self.assertRaises(RemoteCoordError) as lost: self.acquire(binding)
        self.assertIsNone(getattr(lost.exception,'recovery',None))
        self.assertFalse(self.cap.exists())

    def test_coordinator_routes_to_owner_while_independent_lane_advances(self):
        binding=self.grant()
        notice=self.po_call('send',to=self.worker,body='Own intake and resume the original task.')
        with self.assertRaises(RemoteCoordError) as blocked: self.acquire(binding)
        self.assertEqual(blocked.exception.recovery['owner_session'],self.worker)
        created,_,_=self.create_worker('independent-worker')
        other=created['target_session']
        msg=self.po_call('send',to=other,body='Advance the independent private task.')
        self.assertEqual(self.po_call('message',session=other,id=msg['id'])['message']['id'],msg['id'])
        self.po_call('message',session=self.worker,id=notice['id'])
        self.assertTrue(self.acquire(binding)[1])

    def test_expired_binding_and_wrong_owner_are_not_recovery(self):
        payload=self.binding_payload(); payload['minutes']=1e-9
        self.po_call('context')
        expired=self.po_call('exact-file-bind',**payload)
        with self.assertRaises(RemoteCoordError) as error: self.acquire(expired)
        self.assertIsNone(getattr(error.exception,'recovery',None))
        with self.assertRaises(RemoteCoordError) as wrong:
            self.po_call('exact-file-request',session=self.po,binding_id=expired['id'],path=str(self.source),minutes=5)
        self.assertIsNone(getattr(wrong.exception,'recovery',None))

    def test_cli_entrypoint_emits_typed_json_without_token(self):
        binding=self.grant()
        self.po_call('send',to=self.worker,body='Freshness before CLI')
        argv=['agent-chat-client','--server',self.url,'--api-token',self.po_client.token,'--project',self.project_id,'--session',self.worker,'exact-file-request','--binding-id',binding['id'],'--path',str(self.source),'--minutes','5','--capability-file',str(self.cap)]
        stream=io.StringIO()
        with mock.patch.object(sys,'argv',argv),contextlib.redirect_stderr(stream):
            self.assertEqual(fixture.cli.entrypoint(),2)
        subprocess.run(['jq','-e','(.recovery.kind == "inbox_currentness") and (.recovery.requested_effect_absent == true) and (has("token") | not)'],input=stream.getvalue(),text=True,capture_output=True,check=True)
        self.assertFalse(self.cap.exists())

    def test_synthetic_active_stop_is_not_currentness_recovery(self):
        binding=self.grant()
        self.po_call('send',to=self.worker,body='Pending intake')
        with sqlite3.connect(self.server.projects.db_path(self.project_id)) as db:
            db.execute('INSERT INTO root_team_stops(id,root_session,active,created_at) VALUES(?,?,1,?)',('synthetic-stop',self.root_projection,1.0))
        with self.assertRaises(RemoteCoordError) as stopped: self.acquire(binding)
        self.assertIsNone(getattr(stopped.exception,'recovery',None))
        self.assertFalse(self.cap.exists())

    def test_preview_and_ack_do_not_substitute_for_full_read(self):
        binding=self.grant()
        notice=self.po_call('send',to=self.worker,body='Requires full own consumption')
        self.po_call('inbox',session=self.worker)
        self.po_call('acknowledge',session=self.worker,id=notice['id'])
        with self.assertRaises(RemoteCoordError) as denied: self.acquire(binding)
        self.assertEqual(denied.exception.recovery['kind'],'inbox_currentness')
        self.po_call('message',session=self.worker,id=notice['id'])
        self.assertTrue(self.acquire(binding)[1])

def load_tests(loader, standard_tests, pattern):
    names=sorted(k for k in CurrentnessRecoveryTests.__dict__ if k.startswith('test_'))
    return unittest.TestSuite(CurrentnessRecoveryTests(name) for name in names)
