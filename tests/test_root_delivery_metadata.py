"""Exact terminal Root delivery history stays scoped, metadata-only and read-only."""
import contextlib
import io
import json
import unittest
from unittest.mock import patch

from agent_chat import cli, communication
from agent_chat.bridge_state import BridgeState
from agent_chat.core import Coordinator, CoordError
from agent_chat.policy_store import PolicyStore
from agent_chat.root_orchestrator import RootOrchestrator
import test_root_local as local


class RootDeliveryMetadataTests(unittest.TestCase):
    setUp = local.RootLocalTests.setUp
    call = local.RootLocalTests.call
    enroll = local.RootLocalTests.enroll
    snapshot = local.RootLocalTests.snapshot

    def fixture(self):
        self.enroll()
        with contextlib.closing(Coordinator(self.master, 'root')) as coord:
            state = BridgeState(coord)
            coord.db.execute('''INSERT INTO messages(id,sender_session,recipient_session,body,created_at,acked_at)
                VALUES('exact-message','root','root','SECRET MESSAGE BODY',10,18)''')
            coord.db.execute('''INSERT INTO messages(id,sender_session,recipient_session,body,created_at)
                VALUES('pending-message','root','root','SECRET PENDING BODY',11)''')
            coord.db.execute('''INSERT INTO bridge_jobs VALUES('exact-job',?,'SECRET WAKE PAYLOAD','prepared',NULL,'SECRET ERROR',12,13)''',
                             (self.thread,))
            coord.db.execute("INSERT INTO bridge_job_origins VALUES('exact-job',12.5)")
            coord.db.execute("INSERT INTO bridge_deliveries VALUES('exact-message','exact-job')")
            coord.db.execute("UPDATE bridge_jobs SET status='dispatched',queue_id='exact-queue',updated_at=17 WHERE id='exact-job'")
            self.assertEqual(state.status(mine=True)['jobs'], [])

    def metadata(self, ids=None, **overrides):
        params = dict(expected_thread=self.thread, message_ids=ids or ['exact-message'])
        params.update(overrides)
        return self.call('delivery-metadata', **params)

    def proof(self, **overrides):
        values = dict(host='host', project='default', kind='wake', client_id='exact-job',
                      thread=self.thread, turn='exact-native-turn')
        values.update(overrides)
        store = PolicyStore(self.projects.path)
        payload = communication.STANDING_POLICY + '\n' + json.dumps(dict(policy_version=communication.policy_version()))
        store.offer(values['host'], values['project'], values['kind'], values['client_id'], values['thread'], payload)
        store.receipt(values['host'], values['thread'], values['client_id'], communication.policy_version(), values['turn'])

    def test_terminal_exact_message_mapping_without_content_or_writes(self):
        self.fixture()
        before = self.snapshot()
        with patch('agent_chat.projects.Projects', side_effect=AssertionError('no bootstrap')), \
                patch.object(RootOrchestrator, '__init__', side_effect=AssertionError('no schema')), \
                patch.object(RootOrchestrator, 'sync', side_effect=AssertionError('no sync')), \
                patch('agent_chat.remote.HttpClient.call', side_effect=AssertionError('no HTTP')):
            result = self.metadata(['exact-message', 'pending-message'])
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(result['authority'], {'scope': 'machine_read', 'named_session_authentication': 'UNVERIFIED'})
        item, pending = result['deliveries']
        self.assertEqual({field: item[field] for field in (
            'message_id', 'job_id', 'thread_id', 'job_status', 'queue_id', 'client_user_message_id',
            'message_created_at', 'message_acked_at', 'job_created_at', 'job_updated_at', 'job_prepared_at')},
            dict(message_id='exact-message', job_id='exact-job', thread_id=self.thread, job_status='dispatched',
                 queue_id='exact-queue', client_user_message_id='exact-job', message_created_at=10,
                 message_acked_at=18, job_created_at=12, job_updated_at=17, job_prepared_at=12.5))
        self.assertGreater(item['native_attempt_recorded_at_julian_day'], 2000000)
        self.assertEqual(item['native_proof'], [])
        self.assertIsNone(pending['job_id'])
        self.assertIsNone(pending['client_user_message_id'])
        self.assertIsNone(pending['job_updated_at'])
        self.assertEqual(pending['native_proof'], [])
        for field in ('body', 'payload', 'error', 'prompt', 'credentials'):
            self.assertNotIn(field, item)
        self.assertNotIn('SECRET', repr(result))
        self.assertIn('mutable', result['timestamp_semantics']['job_updated_at'])

    def test_exact_owned_persisted_policy_receipt_is_the_only_native_turn_proof(self):
        self.fixture()
        self.proof()
        before = self.snapshot()
        proof = self.metadata()['deliveries'][0]['native_proof']
        self.assertEqual(len(proof), 1)
        self.assertEqual(proof[0]['native_turn_id'], 'exact-native-turn')
        self.assertEqual(proof[0]['policy_version'], communication.policy_version())
        self.assertGreater(proof[0]['observed_at'], 0)
        self.assertEqual(self.snapshot(), before)

    def test_other_client_receipt_does_not_prove_this_job(self):
        self.fixture()
        self.proof(client_id='other-client')
        self.assertEqual(self.metadata()['deliveries'][0]['native_proof'], [])

    def test_other_project_receipt_does_not_prove_this_job(self):
        self.fixture()
        self.proof(project='other-project')
        self.assertEqual(self.metadata()['deliveries'][0]['native_proof'], [])

    def test_other_host_receipt_does_not_prove_this_job(self):
        self.fixture()
        self.proof(host='other-host')
        self.assertEqual(self.metadata()['deliveries'][0]['native_proof'], [])

    def test_other_thread_receipt_does_not_prove_this_job(self):
        self.fixture()
        self.proof(thread='other-thread')
        self.assertEqual(self.metadata()['deliveries'][0]['native_proof'], [])

    def test_unknown_and_other_recipient_messages_are_equally_unavailable(self):
        self.fixture()
        with contextlib.closing(Coordinator(self.master, 'root')) as coord:
            coord.db.execute('''INSERT INTO messages(id,sender_session,recipient_session,body,created_at)
                VALUES('other-message','root','other-recipient','SECRET OTHER BODY',21)''')
        before = self.snapshot()
        for message_id in ('unknown-message', 'other-message'):
            with self.subTest(message_id=message_id), self.assertRaisesRegex(CoordError, 'unavailable for the canonical recipient'):
                self.metadata([message_id])
        self.assertEqual(self.snapshot(), before)

    def test_stale_thread_wrong_identity_host_and_other_thread_job_are_refused(self):
        self.fixture()
        before = self.snapshot()
        with self.assertRaisesRegex(CoordError, 'expected thread differs'):
            self.metadata(expected_thread='stale-thread')
        for field, value in (('session', 'other-root'), ('host_id', 'other-host')):
            with self.subTest(field=field), self.assertRaisesRegex(CoordError, 'canonical identity differs'):
                identity = dict(self.identity, **{field: value})
                local.root_local.execute('delivery-metadata', dict(expected_thread=self.thread, message_ids=['exact-message']), **identity)
        self.assertEqual(self.snapshot(), before)
        with contextlib.closing(Coordinator(self.master, 'root')) as coord:
            coord.db.execute("UPDATE bridge_jobs SET thread_id='other-thread' WHERE id='exact-job'")
        before = self.snapshot()
        with self.assertRaisesRegex(CoordError, 'unavailable for the current thread'):
            self.metadata()
        self.assertEqual(self.snapshot(), before)

    def test_selectors_are_exact_distinct_and_bounded_before_preflight(self):
        for ids in ([], 'exact-message', ['same', 'same'], ['id'] * 21, ['line\nbreak'], [None]):
            with self.subTest(ids=ids), patch('agent_chat.root_local._preflight', side_effect=AssertionError('no preflight')):
                with self.assertRaises(CoordError):
                    self.call('delivery-metadata', expected_thread=self.thread, message_ids=ids)
        with self.assertRaisesRegex(CoordError, 'invalid local Root method fields'):
            self.call('delivery-metadata', expected_thread=self.thread, message_ids=['exact-message'], project='other-project')

    def test_missing_schema_is_not_created_by_the_read(self):
        self.fixture()
        with contextlib.closing(Coordinator(self.master, 'root')) as coord:
            coord.db.execute('DROP TABLE bridge_deliveries')
        before = self.snapshot()
        with self.assertRaisesRegex(CoordError, 'metadata schema is unavailable'):
            self.metadata()
        self.assertEqual(self.snapshot(), before)

    def test_cli_requires_explicit_identity_thread_and_exact_message_ids(self):
        with patch('agent_chat.root_local.execute', return_value={}) as execute, \
                patch('agent_chat.remote.HttpClient.call', side_effect=AssertionError('no HTTP')), \
                contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(cli.main(['root-local', '--home-project', 'default', '--home-session', 'root',
                '--host-id', 'host', 'delivery-metadata', '--expected-thread', self.thread,
                '--message-id', 'first-message', '--message-id', 'second-message']), 0)
        execute.assert_called_once_with('delivery-metadata',
            {'expected_thread': self.thread, 'message_ids': ['first-message', 'second-message']}, **{
                **self.identity, 'master_db': str(self.master), 'service_plist': str(self.plist)})
