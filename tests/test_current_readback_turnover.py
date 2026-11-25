import contextlib
import sqlite3
import unittest
from unittest.mock import patch
import test_build01_admission as fixture
from agent_chat import root_local, successor_admission, capacity_observer
from agent_chat.core import CoordError

class CurrentReadbackTurnover(fixture.Build01Tests):
    def setUp(self):
        super().setUp()
        self.current_thread='01a1aaaa-bbbb-7ccc-8ddd-eeeeeeeeeeee'
        self.db.execute('UPDATE bridge_bindings SET thread_id=? WHERE session_id=?',(self.current_thread,successor_admission.ROOT));self.db.commit()
        with contextlib.closing(sqlite3.connect(self.registry)) as db:
            db.execute('UPDATE root_config SET thread_id=? WHERE singleton=1',(self.current_thread,));db.commit()
    def read(self,request,**identity):
        caller=dict(home_project='default',session=successor_admission.ROOT,host_id=successor_admission.HOST);caller.update(identity)
        return root_local.execute('capacity-successor-attempt-status',{'request_id':request,'attempt_id':'0'*32},master_db=self.master,service_plist=self.root/'service.plist',**caller)
    def test_current_thread_can_reconcile_historical_subject_without_inventing_attempt(self):
        with patch.object(successor_admission,'issue',side_effect=AssertionError('producer called')),patch.object(capacity_observer,'_native_observations',side_effect=AssertionError('native called')):
            for request in successor_admission.REQUESTS:
                with self.subTest(request=request):
                    result=self.read(request)
                    self.assertNotEqual(result.get('reason'),'root_thread_mismatch')
                    self.assertEqual(result['state'],'UNKNOWN')
                    self.assertEqual(result['producer_attempt']['state'],'legacy_history_unknown')
                    self.assertEqual(result['producer_attempt']['next_action'],'no_replay')
                    self.assertNotIn('attempt_id',result['producer_attempt'])
                    self.assertEqual(result['attempt_id'],'0'*32)
                    self.assertIn('not proof',result['attempt_id_semantics'])
                    self.assertEqual(result['next_action'],'no_replay')
                    self.assertFalse(result['sql_mutation_performed'])
                    self.assertEqual(result['native_state'],'not observed')
    def test_configured_synthetic_caller_is_not_historical_subject(self):
        current='synthetic-current-root'
        self.db.execute('INSERT INTO sessions(id,agent,registered_at,inbox_read_seq) VALUES(?,?,?,0)',(current,'fixture-root',1))
        self.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?)',(current,self.current_thread,None))
        self.db.execute('INSERT INTO remote_session_hosts(session_id,host_id) VALUES(?,?)',(current,successor_admission.HOST));self.db.commit()
        with contextlib.closing(sqlite3.connect(self.registry)) as db:
            db.execute('UPDATE root_config SET home_session=? WHERE singleton=1',(current,))
            db.execute('UPDATE root_directory_owner SET home_session=? WHERE singleton=1',(current,))
            db.execute('UPDATE root_projects SET session_id=? WHERE project_id=?',(current,'default'));db.commit()
        result=self.read(successor_admission.REQUEST,session=current)
        self.assertEqual(result['state'],'UNKNOWN')
        self.assertEqual(result['producer_attempt']['state'],'legacy_history_unknown')
        self.assertEqual(result['next_action'],'no_replay')
        self.assertEqual(successor_admission.ROOT,'session_tcnSV4Qo3EH4YU3gg2swqjO_GS1QQ9Fv')
    def test_unmatched_requested_id_is_unknown_even_if_other_attempt_exists(self):
        from agent_chat import successor_attempt_journal as journal
        with journal.verified_store(self.master,actor=successor_admission.ROOT,host=successor_admission.HOST,project='default',root_thread=self.current_thread) as (db,check):
            journal.begin(db,check,request_id=successor_admission.REQUEST,operation='synthetic',actor=successor_admission.ROOT,host=successor_admission.HOST,target=str(self.master),source='synthetic-source',project='default')
        result=self.read(successor_admission.REQUEST)
        self.assertEqual(result['state'],'UNKNOWN')
        self.assertEqual(result['next_action'],'no_replay')
        self.assertNotIn('producer_attempt',result)
        self.assertFalse(result['sql_mutation_performed'])
    def test_unrelated_identity_host_and_project_cannot_read_evidence(self):
        for caller in ({'session':'other'},{'host_id':'other'},{'home_project':'other'}):
            with self.subTest(caller=caller):
                try: result=self.read(successor_admission.REQUEST,**caller)
                except CoordError: continue
                self.assertEqual(result['state'],'UNKNOWN');self.assertNotIn('producer_attempt',result)
    def test_internal_readback_rejects_foreign_home_before_scope_lookup(self):
        from agent_chat import native02_readback
        with patch.object(capacity_observer,'_root_scope',side_effect=AssertionError('foreign project lookup')):
            with self.assertRaises(native02_readback.ReadbackUnavailable) as error:
                native02_readback.inspect_attempt(self.master,registry=self.registry,home_project='other',home_session=successor_admission.ROOT,host_id=successor_admission.HOST,request_id=successor_admission.REQUEST,attempt_id='0'*32)
        self.assertEqual(error.exception.code,'root_packet_mismatch')
    def test_changed_binding_during_read_fails_closed(self):
        original=capacity_observer._root_scope
        def race(*args,**kwargs):
            result=original(*args,**kwargs)
            self.db.execute('UPDATE bridge_bindings SET thread_id=? WHERE session_id=?',('synthetic-changed',successor_admission.ROOT));self.db.commit()
            return result
        with patch.object(capacity_observer,'_root_scope',side_effect=race):
            result=self.read(successor_admission.REQUEST)
        self.assertEqual(result['state'],'UNKNOWN');self.assertNotIn('producer_attempt',result)
        self.assertEqual(result['reason'],'configured_identity_mismatch')
        self.assertEqual(result['next_action'],'no_replay')
    def test_changed_registry_during_read_fails_closed(self):
        from agent_chat import successor_attempt_journal
        original=successor_attempt_journal.status_db
        def race(*args,**kwargs):
            result=original(*args,**kwargs)
            with contextlib.closing(sqlite3.connect(self.registry)) as db:
                db.execute('UPDATE root_config SET thread_id=? WHERE singleton=1',('synthetic-newer',));db.commit()
            return result
        with patch.object(successor_attempt_journal,'status_db',side_effect=race):
            result=self.read(successor_admission.REQUEST)
        self.assertEqual(result['state'],'UNKNOWN');self.assertNotIn('producer_attempt',result)
        self.assertEqual(result['reason'],'configured_identity_mismatch')
        self.assertEqual(result['next_action'],'no_replay')
