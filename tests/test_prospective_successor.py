import contextlib, os, sqlite3, unittest
from types import SimpleNamespace
from unittest.mock import patch
import test_build01_admission as fixture
from agent_chat import successor_admission as successor, successor_attempt_journal as journal, root_local, capacity_observer
from agent_chat.core import CoordError
from agent_chat import native_owner_evidence
_REAL_NATIVE_OBSERVATIONS=capacity_observer._native_observations

class ProspectiveFixture(fixture.Build01Tests):
    def fixture_begin(self, **kwargs):
        with journal.verified_store(self.master,actor=successor.ROOT,host=successor.HOST,project='default',root_thread=successor.ROOT_THREAD) as (db,check):
            return journal.begin(db,check,**kwargs)
    def root_call(self):
        with patch.object(root_local,'_preflight',return_value=(self.master,True)):
            return root_local.execute('capacity-successor-build01',{'confirm':True},master_db=self.master,
                service_plist=self.root/'service.plist',home_project='default',session=successor.ROOT,host_id=successor.HOST)
    def allow_fixture(self):
        # This stipulates a synthetic fresh allowance; it is not production authorization.
        return patch.object(successor,'_require_unconsumed_allowance',return_value=None)
    def test_legacy_and_production_no_allowance_stay_unknown_and_refused(self):
        status=journal.status(self.master,successor.BUILD_REQUEST)
        self.assertEqual(status['state'],'legacy_history_unknown');self.assertEqual(status['next_action'],'no_replay')
        with self.assertRaisesRegex(CoordError,'predicate=legacy_history_unknown'):
            self.root_call()
        self.assertEqual(journal.status(self.master,successor.BUILD_REQUEST)['state'],'legacy_history_unknown')
    def test_precise_refusal_durable_and_no_second_attempt(self):
        self.free_heavy();self.direct=False
        with self.allow_fixture():
            with self.assertRaises(CoordError) as failure: self.root_call()
            self.assertIn('predicate=native_direct_input_false',str(failure.exception))
            result=journal.status(self.master,successor.BUILD_REQUEST)
            self.assertEqual(result['state'],'refused');self.assertEqual(result['events'][-1]['predicate'],'native_direct_input_false')
            self.assertEqual(result['historical_prior_attempts'],'unknown');self.assertEqual(result['allowance'],'unknown')
            self.assertEqual(result['actor'],successor.ROOT);self.assertEqual(result['host'],successor.HOST)
            self.assertEqual(result['operation'],'capacity-successor-build01')
            self.assertEqual((result['source_id'],result['project_id']),(successor.BUILD_SOURCE,'default'))
            self.assertEqual(len(result['target_digest']),64);self.assertEqual(len(result['payload_digest']),64)
            self.assertEqual(journal.status(self.master,successor.BUILD_REQUEST,result['attempt_id'])['attempt_id'],result['attempt_id'])
            with self.assertRaisesRegex(ValueError,'exact attempt mismatch'): journal.status(self.master,successor.BUILD_REQUEST,'different')
            with patch.object(capacity_observer,'_native_observations',side_effect=AssertionError('replay crossed native read')):
                with self.assertRaises(CoordError): self.root_call()
            self.assertEqual(journal.status(self.master,successor.BUILD_REQUEST)['attempt_id'],result['attempt_id'])
    def test_committed_packet_survives_lost_response_without_replay(self):
        self.free_heavy(); original=successor._issue_impl
        def lost(*a,**k): original(*a,**k); raise ConnectionError('synthetic response lost')
        with self.allow_fixture(),patch.object(successor,'_issue_impl',side_effect=lost):
            with self.assertRaises(ConnectionError): self.root_call()
        result=journal.status(self.master,successor.BUILD_REQUEST)
        self.assertEqual(result['state'],'committed')
        self.assertIn('committed',[e['phase'] for e in result['events']])
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM successor_admissions WHERE request_id=?',(successor.BUILD_REQUEST,)).fetchone()[0],1)
        with self.allow_fixture(),patch.object(capacity_observer,'_native_observations',side_effect=AssertionError('replay crossed native read')):
            with self.assertRaises(CoordError): self.root_call()
    def test_interrupted_read_remains_unknown_and_no_replay(self):
        self.free_heavy()
        @contextlib.contextmanager
        def interrupted(*a,**k):
            raise KeyboardInterrupt('synthetic interrupted read')
            yield
        with self.allow_fixture(),patch.object(capacity_observer,'_native_observations',side_effect=interrupted):
            with self.assertRaises(KeyboardInterrupt): self.root_call()
        result=journal.status(self.master,successor.BUILD_REQUEST)
        self.assertEqual(result['state'],'unknown')
        self.assertEqual(result['next_action'],'readback_only_no_replay')
        self.assertIn('native_read_started',[e['phase'] for e in result['events']])
        self.assertEqual(result['events'][-1]['phase'],'outcome_unknown')
        self.assertIsNone(self.db.execute('SELECT 1 FROM successor_admissions WHERE request_id=?',(successor.BUILD_REQUEST,)).fetchone())
    def test_actual_observer_direct_false_is_exact_refusal(self):
        self.free_heavy(); calls=[]
        class Kernel:
            def sample(self, reg): return SimpleNamespace(endpoint=('127.0.0.1',34971),listener_pid=7,chain=('fixture',),image_sha='synthetic',code_hash='synthetic',peer=None)
        class Rpc:
            def __enter__(self): return self
            def __exit__(self,*args): return False
            def connection_identity(self): return {'client_instance':'a'*32,'connection_epoch':1,'pid':os.getpid(),'fd':8,'local':('127.0.0.1',31002),'peer':('127.0.0.1',34971)}
            def request(self,method,params):
                calls.append((method,params))
                if method=='thread/read': return {'thread':{'id':successor.THREAD,'status':{'type':'idle'},'canAcceptDirectInput':False}}
                raise AssertionError('unexpected read')
        @contextlib.contextmanager
        def native(bindings,registration):
            with _REAL_NATIVE_OBSERVATIONS(bindings,registration,lambda endpoint:Rpc()) as result: yield result
        with self.allow_fixture(),patch.object(native_owner_evidence,'DarwinEvidence',Kernel),              patch.object(capacity_observer,'_selected_client',side_effect=lambda reg,k: ('client','link',k.sample(reg),'plist','files')),              patch.object(capacity_observer,'_observing_peer',return_value='synthetic-pair'),              patch.object(capacity_observer,'_native_observations',side_effect=native):
            with self.assertRaisesRegex(CoordError,'predicate=native_direct_input_false'): self.root_call()
        result=journal.status(self.master,successor.BUILD_REQUEST)
        self.assertEqual(result['state'],'refused');self.assertEqual(result['events'][-1]['predicate'],'native_direct_input_false')
        self.assertEqual([method for method,_ in calls],['thread/read'])

    def test_pre_read_capacity_refusal_is_journalled_without_native_read(self):
        self.db.execute("UPDATE resources SET reason='fixture-blocked' WHERE name='heavy-unity:2'");self.db.commit()
        with self.allow_fixture(),patch.object(capacity_observer,'_native_observations',side_effect=AssertionError('unexpected native read')):
            with self.assertRaisesRegex(CoordError,'predicate=heavy_capacity_unavailable'): self.root_call()
        result=journal.status(self.master,successor.BUILD_REQUEST)
        self.assertEqual(result['state'],'refused')
        self.assertEqual(result['events'][-1]['predicate'],'heavy_capacity_unavailable')
        self.assertNotIn('native_read_started',[e['phase'] for e in result['events']])

    def test_transaction_predicate_refusal_is_recorded_after_rollback(self):
        self.free_heavy();original=journal.append_on
        def inject(db,request_id,attempt_id,phase,predicate=None):
            original(db,request_id,attempt_id,phase,predicate)
            if phase=='transaction_started': db.execute("UPDATE resources SET reason='fixture-race' WHERE name='validation-clone'")
        with self.allow_fixture(),patch.object(journal,'append_on',side_effect=inject):
            with self.assertRaisesRegex(CoordError,'phase=transaction_started; predicate=resource_not_exactly_free'): self.root_call()
        result=journal.status(self.master,successor.BUILD_REQUEST)
        self.assertEqual((result['state'],result['events'][-1]['predicate']),('refused','resource_not_exactly_free'))
        self.assertIn('failure_at_transaction_started',[event['phase'] for event in result['events']])
        self.assertIsNone(self.db.execute('SELECT 1 FROM successor_admissions WHERE request_id=?',(successor.BUILD_REQUEST,)).fetchone())
        self.assertIsNone(self.db.execute("SELECT reason FROM resources WHERE name='validation-clone'").fetchone()[0])

    def test_journal_begin_failure_never_reaches_native_reads(self):
        self.free_heavy()
        with self.allow_fixture(),patch.object(journal,'begin',side_effect=sqlite3.OperationalError('synthetic journal unavailable')),              patch.object(capacity_observer,'_native_observations',side_effect=AssertionError('native read crossed failed journal')):
            with self.assertRaises(sqlite3.OperationalError): self.root_call()
        self.assertEqual(journal.status(self.master,successor.BUILD_REQUEST)['state'],'legacy_history_unknown')

    def test_existing_root_readback_stays_exact_and_read_only(self):
        # Synthetic Build01 journal cannot be relabelled as the fixed Native02 history.
        self.fixture_begin(request_id=successor.BUILD_REQUEST,operation='capacity-successor-build01',actor=successor.ROOT,host=successor.HOST,target=str(self.master),source=successor.BUILD_SOURCE,project='default')
        with patch.object(root_local,'_preflight',return_value=(self.master,True)),              patch.object(successor,'issue',side_effect=AssertionError('producer called')),              patch.object(capacity_observer,'_native_observations',side_effect=AssertionError('native read called')):
            result=root_local.execute('capacity-successor-native02-readback',{},master_db=self.master,
                service_plist=self.root/'service.plist',home_project='default',session=successor.ROOT,host_id=successor.HOST)
        self.assertEqual(result['request_id'],successor.NATIVE02_REQUEST)
        self.assertEqual(result['producer_attempt']['state'],'legacy_history_unknown')
        self.assertEqual(result['producer_attempt']['next_action'],'no_replay')
        self.assertFalse(result['sql_mutation_performed'])

    def test_started_without_terminal_evidence_is_unknown_and_not_replayed(self):
        attempt=self.fixture_begin(request_id=successor.BUILD_REQUEST,operation='capacity-successor-build01',actor=successor.ROOT,host=successor.HOST,target=str(self.master),source=successor.BUILD_SOURCE,project='default')
        result=journal.status(self.master,successor.BUILD_REQUEST,attempt)
        self.assertEqual(result['state'],'unknown');self.assertEqual(result['next_action'],'readback_only_no_replay')
        with self.allow_fixture(),patch.object(capacity_observer,'_native_observations',side_effect=AssertionError('replay crossed native read')):
            with self.assertRaisesRegex(CoordError,'predicate=attempt_already_recorded'): self.root_call()
        self.assertEqual(journal.status(self.master,successor.BUILD_REQUEST)['attempt_id'],attempt)

    def test_unknown_field_is_not_claimed_false(self):
        self.free_heavy();self.direct=None
        with self.allow_fixture():
            with self.assertRaisesRegex(CoordError,'predicate=native_direct_input_unknown'): self.root_call()
        result=journal.status(self.master,successor.BUILD_REQUEST)
        self.assertEqual(result['state'],'unknown')
        self.assertEqual(result['events'][-1]['predicate'],'native_direct_input_unknown')

if __name__=='__main__': unittest.main()
