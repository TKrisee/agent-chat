import tempfile
import time
import unittest
import uuid
from unittest import mock
from pathlib import Path

from agent_chat import root_lifecycle
from agent_chat.root_local import _investigation_preflight_result
from agent_chat.agent_control import AgentControlState
from agent_chat.bridge import Bridge
from agent_chat.bridge_state import BridgeState
from agent_chat.control_worker import ControlWorker
from agent_chat.core import CoordError, Coordinator
from agent_chat.remote_service import dispatch


class InvestigationRpc:
    def __init__(self, thread):
        self.thread = thread
        self.status = 'idle'
        self.queue_items = []
        self.calls = []
        self.settings = expected_settings()

    def request(self, method, params):
        self.calls.append((method, dict(params)))
        if method == 'thread/read':
            return {'thread': {'id': params['threadId'], 'status': {'type': self.status},
                               'canAcceptDirectInput': self.status == 'idle'}}
        if method == 'thread/resume':
            self.status = 'idle'
            permissions = self.settings['permissions']
            return {
                'thread': {'id': params['threadId']},
                'model': self.settings['model'],
                'reasoningEffort': self.settings['reasoning_effort'],
                'cwd': self.settings['cwd'],
                'runtimeWorkspaceRoots': self.settings['workspaces'],
                'activePermissionProfile': permissions['activePermissionProfile'],
                'approvalPolicy': permissions['approvalPolicy'],
                'approvalsReviewer': permissions['approvalsReviewer'],
                'sandbox': permissions['sandbox'],
            }
        if method == 'thread/queue/list':
            return {'data': list(self.queue_items), 'nextCursor': None}
        if method == 'thread/queue/add':
            item = {'id': 'queued-task', 'clientUserMessageId': params['clientUserMessageId'],
                    'input': params['input']}
            self.queue_items.append(item)
            return {'queuedSubmission': item}
        if method == 'thread/queue/start':
            self.queue_items = [item for item in self.queue_items if item['id'] != params['queuedSubmissionId']]
            self.status = 'active'
            return {'turn': {'id': 'task-turn'}}
        raise AssertionError(method)


def expected_settings():
    roots = ['/workspace/project']
    return {
        'model': 'gpt-6-luna', 'reasoning_effort': 'high', 'cwd': roots[0], 'workspaces': roots,
        'permissions': {
            'activePermissionProfile': {'id': 'standard', 'extends': None},
            'approvalPolicy': 'on-request', 'approvalsReviewer': 'auto_review',
            'runtimeWorkspaceRoots': roots,
            'sandbox': {'type': 'workspace-write', 'writableRoots': roots,
                        'networkAccess': False, 'excludeSlashTmp': False,
                        'excludeTmpdirEnvVar': False},
        },
    }


class InvestigationAdmissionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='investigation-admission-')
        self.addCleanup(self.temp.cleanup)
        self.coord = Coordinator(Path(self.temp.name) / 'state.sqlite3', 'root')
        self.addCleanup(self.coord.close)
        self.root_thread = str(uuid.uuid4())
        self.po_thread = str(uuid.uuid4())
        self.worker_thread = str(uuid.uuid4())
        for session, label in (('root', 'root'), ('po', 'orchestrator'), ('worker', 'worker'),
                               ('other', 'worker')):
            dispatch(self.coord, {'op': 'register', 'session': session, 'host_id': 'host',
                                  'params': {'agent': label}})
        self.state = BridgeState(self.coord)
        self.dispatch_state = InvestigationBridgeState(self.state, 'host')
        for session, thread in (('root', self.root_thread), ('po', self.po_thread),
                                ('worker', self.worker_thread), ('other', str(uuid.uuid4()))):
            self.coord.session = session
            self.state.bind(thread)
        self.coord.session = 'root'
        self.coord.db.execute('CREATE TABLE IF NOT EXISTS project_orchestrators(session_id TEXT PRIMARY KEY,root_session TEXT NOT NULL)')
        self.coord.db.execute("INSERT INTO project_orchestrators VALUES('po','root')")
        self.coord.db.execute('CREATE TABLE IF NOT EXISTS root_projections(session_id TEXT PRIMARY KEY)')
        self.controls = AgentControlState(self.coord)
        self.stop_id = str(uuid.uuid4())
        root_lifecycle.pause(self.coord, 'root', self.stop_id)
        root_lifecycle.admit_po(self.coord, 'root', self.stop_id, 'po', self.po_thread, 'host')
        self.inspect_id = str(uuid.uuid4())
        self.controls.request('worker', self.worker_thread, 'inspect', self.inspect_id, False)
        self.coord.session = 'po'
        self.message = self.coord.send('worker', 'Read the assigned email and implicated tree; report findings only.')['id']
        self.coord.session = 'other'
        self.unrelated = self.coord.send('worker', 'Unrelated later task must remain inbox-only.')['id']
        self.coord.session = 'root'
        self.request_id = str(uuid.uuid4())
        self.expires = time.time() + 600

    def admit(self, *, message_id=None, target='worker', thread=None, host='host', stop_id=None,
              inspection_id=None, request_id=None, expires_at=None):
        return root_lifecycle.admit_investigation_task(self.coord, 'root', request_id or self.request_id,
            stop_id or self.stop_id, 'po', target, thread or self.worker_thread, host,
            message_id or self.message, inspection_id or self.inspect_id,
            self.expires if expires_at is None else expires_at)

    def test_coordinator_startup_migrates_legacy_task_baseline_tables_before_bridge_read(self):
        self.admit()
        path = self.coord.path
        # Model an existing store from before the continuation tables shipped.
        for table in ('root_team_stop_task_baselines', 'root_team_stop_task_continuations',
                      'root_team_stop_task_preload_requests', 'root_team_stop_task_preload_events'):
            self.coord.db.execute(f'DROP TABLE {table}')
        self.coord.close()

        self.coord = Coordinator(path, 'root')
        self.addCleanup(self.coord.close)
        self.state = BridgeState(self.coord)
        for table in ('root_team_stop_task_baselines', 'root_team_stop_task_continuations',
                      'root_team_stop_task_preload_requests', 'root_team_stop_task_preload_events'):
            self.assertTrue(self.coord.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone())
        self.assertTrue(self.coord.db.execute(
            'SELECT 1 FROM root_team_stop_task_admissions WHERE request_id=?', (self.request_id,)).fetchone())
        # A legacy admitted task without a baseline stays withheld, but no
        # longer crashes the bridge while checking its exact task scope.
        self.assertEqual(self.state.investigation_tasks('host'), [])

    def continuation_and_preload(self, *, settings=None):
        """Exercise the one expired-grant continuation and current-PO preload setup."""
        self.admit(expires_at=time.time() + 0.01)
        time.sleep(0.02)
        self.coord.db.execute("UPDATE agent_control_requests SET status='completed' WHERE id=?",
                              (self.inspect_id,))
        self.coord.session = 'po'
        source_message = self.coord.send('root', 'Attested historical expected settings for the exact task.')['id']
        self.coord.session = 'root'
        now = time.time()
        baseline_id, continuation_id = str(uuid.uuid4()), str(uuid.uuid4())
        root_lifecycle.create_task_settings_continuation(
            self.coord, actor_root='root', admission_request_id=self.request_id,
            baseline_id=baseline_id, continuation_id=continuation_id,
            expected_settings=settings or expected_settings(), source_message_id=source_message,
            source_artifact_name='po-attested-settings-v1.json', source_artifact_sha256='a' * 64,
            source_kind='po_attested_historical', source_limit='retained PO-attested transcription; raw snapshot absent',
            expires_at=now + 900, now=now)
        preload_id = str(uuid.uuid4())
        unresolved = [tuple(row) for row in self.coord.db.execute(
            "SELECT id,action,status FROM agent_control_requests WHERE target_session='worker'")]
        self.assertTrue(all(row[2] in ('completed', 'failed', 'cancelled', 'resolved_unchanged')
                            for row in unresolved), unresolved)
        dispatch(self.coord, {'op':'agent-investigation-preload','session':'po','host_id':'host',
            'params':{'to':'worker','expected_thread':self.worker_thread,
                      'continuation_id':continuation_id,'request_id':preload_id,'confirm':True}})
        self.coord.session = 'root'
        self.assertEqual(self.state.investigation_tasks('host'), [])
        return preload_id

    def complete_preload_only(self, rpc, preload_id):
        bridge = Bridge(self.dispatch_state, rpc)
        ControlWorker(bridge).advance(self.controls.job(preload_id))
        return self.controls.job(preload_id)

    def test_one_admitted_original_message_runs_after_its_exact_inspect_and_never_batches(self):
        rpc = InvestigationRpc(self.worker_thread)
        preload_id = self.continuation_and_preload()
        receipt = root_lifecycle.task_admission(self.coord.db, self.message, include_expired=True,
                                                allowed_control_id=preload_id)
        self.assertEqual((receipt['message_id'], receipt['inspection_request_id']),
                         (self.message, self.inspect_id))
        self.assertTrue(root_lifecycle.stopped(self.coord.db, 'worker'))
        bridge = Bridge(self.dispatch_state, rpc)
        bridge.tick(observe_policies=False)
        inspection = self.controls.job(self.inspect_id)
        self.assertEqual(inspection['status'], 'completed')
        self.assertEqual(self.controls.job(preload_id)['status'], 'completed',
                         self.controls.job(preload_id).get('error'))
        deliveries = [tuple(row) for row in self.coord.db.execute('SELECT message_id,job_id FROM bridge_deliveries')]
        self.assertEqual(len(deliveries), 1)
        self.assertEqual(deliveries[0][0], self.message)
        self.assertNotEqual(deliveries[0][0], self.unrelated)
        self.assertEqual(self.state.pending(), [])
        self.assertEqual([method for method, _ in rpc.calls].count('thread/queue/add'), 1)
        self.assertEqual([method for method, _ in rpc.calls].count('thread/queue/start'), 1)
        methods = [method for method, _ in rpc.calls]
        self.assertLess(methods.index('thread/resume'), methods.index('thread/queue/add'))
        wake = next(params for method, params in rpc.calls if method == 'thread/queue/add')
        wake_text = wake['input'][0]['text']
        self.assertIn('current PO-attested expected settings were compared', wake_text)
        self.assertIn('"investigation_admission"', wake_text)
        self.assertIn(self.request_id, wake_text)
        self.assertIn(self.stop_id, wake_text)
        self.assertIn(self.message, wake_text)
        self.assertIn(str(receipt['expires_at']), wake_text)
        self.assertIn('"workers_released":false', wake_text)
        job = self.state.job(deliveries[0][1])
        self.assertEqual((job['thread_id'], job['status']), (self.worker_thread, 'dispatched'))
        self.assertTrue(root_lifecycle.stopped(self.coord.db, 'worker'))

    def test_current_designated_po_inspection_uses_same_original_control_and_message(self):
        self.coord.db.execute("UPDATE agent_control_requests SET status='completed' WHERE id=?", (self.inspect_id,))
        self.coord.session = 'po'
        inspection = self.controls.request('worker', self.worker_thread, 'inspect',
                                           str(uuid.uuid4()), False)
        self.coord.session = 'root'
        receipt = self.admit(inspection_id=inspection['id'])
        self.assertEqual(receipt['status'], 'admitted')
        saved = self.coord.db.execute('SELECT requester_session,id FROM agent_control_requests WHERE id=?',
                                      (inspection['id'],)).fetchone()
        self.assertEqual((saved['requester_session'], saved['id']), ('po', inspection['id']))
        self.assertEqual(receipt['message_id'], self.message)
        self.assertIsNotNone(root_lifecycle.task_admission(self.coord.db, self.message))

    def test_unadmitted_stopped_worker_cannot_prepare_or_start_even_with_same_po(self):
        rpc = InvestigationRpc(self.worker_thread)
        bridge = Bridge(self.dispatch_state, rpc)
        bridge.tick(observe_policies=False)
        self.assertEqual(self.coord.db.execute('SELECT COUNT(*) FROM bridge_jobs').fetchone()[0], 0)
        self.assertEqual([method for method, _ in rpc.calls if method in ('thread/queue/add','thread/queue/start')], [])
        self.assertEqual(self.state.pending(), [])
        self.assertIn(self.message, [row[0] for row in self.coord.db.execute('SELECT id FROM messages')])

    def test_exact_admission_rejects_wrong_message_control_binding_and_expiry(self):
        with self.assertRaises(CoordError):
            self.admit(message_id=self.unrelated)
        with self.assertRaises(CoordError):
            self.admit(inspection_id=str(uuid.uuid4()))
        with self.assertRaises(CoordError):
            self.admit(thread=str(uuid.uuid4()))
        with self.assertRaises(CoordError):
            self.admit(expires_at=time.time() - 1)
        self.assertEqual(self.coord.db.execute('SELECT COUNT(*) FROM root_team_stop_task_admissions').fetchone()[0], 0)

    def test_preflight_is_read_only_named_and_uses_target_project_root_identity(self):
        # A project-local Root can legitimately differ from canonical HOME Root.
        # The immutable stop/admission producer records the project-local Root.
        project_root = 'project-root'
        project_root_thread = str(uuid.uuid4())
        root_lifecycle.resume(self.coord, 'root', self.stop_id)
        self.coord.db.execute("UPDATE agent_control_requests SET status='completed' WHERE id=?", (self.inspect_id,))
        dispatch(self.coord, {'op': 'register', 'session': project_root, 'host_id': 'host',
                              'params': {'agent': 'orchestrator'}})
        self.coord.session = project_root
        self.state.bind(project_root_thread)
        self.coord.session = 'root'
        self.coord.db.execute('UPDATE project_orchestrators SET root_session=? WHERE session_id=?',
                              (project_root, 'po'))
        project_stop = str(uuid.uuid4())
        root_lifecycle.pause(self.coord, project_root, project_stop)
        root_lifecycle.admit_po(self.coord, project_root, project_stop, 'po', self.po_thread, 'host')
        project_inspection = str(uuid.uuid4())
        self.coord.session = project_root
        self.controls.request('worker', self.worker_thread, 'inspect', project_inspection, False)

        def adapt_preflight(result):
            checks = dict(result['checks'])
            # The RootOrchestrator layer adds these two current HOME/enrollment checks.
            checks['original_stop_is_current_active'] = True
            checks['target_project_enrollment_is_current'] = True
            return _investigation_preflight_result({
                'operation': 'investigation-preflight', 'status': result['status'],
                'checks': checks, 'failed_checks': list(result['failed_checks'])})

        before = self.coord.db.total_changes
        result = root_lifecycle.investigation_preflight(
            self.coord.db, project_root, project_stop, 'po', 'worker', self.worker_thread, 'host',
            self.request_id, self.message, project_inspection, self.expires, now=time.time())
        self.assertEqual(result['status'], 'eligible', result['failed_checks'])
        self.assertEqual(adapt_preflight(result)['status'], 'eligible')
        self.assertEqual(set(result), {'status', 'checks', 'failed_checks'})
        self.assertEqual(set(result['checks']), {
            'target_project_stop_matches_target_root', 'original_stop_is_active',
            'stop_target_matches_target_root_thread_host', 'stop_target_status_admissible',
            'po_stop_admission_matches_target_root', 'po_designation_matches_target_root',
            'po_binding_matches_recorded_admission', 'po_host_matches_recorded_admission',
            'worker_binding_matches_stop_target', 'worker_host_matches_stop_target',
            'message_sender_recipient_matches_po_worker',
            'inspection_request_present', 'inspection_request_requester_matches_target_root',
            'inspection_request_requester_matches_current_admitted_po',
            'inspection_request_target_matches_worker', 'inspection_request_thread_matches_worker',
            'inspection_request_host_matches_worker', 'inspection_request_action_is_inspect',
            'inspection_request_is_noninterrupting', 'inspection_request_matches_root_worker_binding',
            'inspection_request_status_admissible',
            'expiry_within_one_hour', 'request_and_message_scope_available', 'message_not_batched',
            'message_has_no_existing_delivery', 'no_other_active_stop_barrier',
            'message_unread_by_worker', 'worker_has_no_other_pending_control',
            'worker_has_no_pending_reset', 'worker_control_is_running', 'worker_is_not_quarantined'})
        self.assertEqual(result['failed_checks'], [])
        self.assertTrue(all(value for name, value in result['checks'].items()
                            if name not in ('inspection_request_requester_matches_target_root',
                                            'inspection_request_requester_matches_current_admitted_po')))
        self.assertTrue(result['checks']['inspection_request_requester_matches_target_root'])
        self.assertFalse(result['checks']['inspection_request_requester_matches_current_admitted_po'])
        self.assertEqual(self.coord.db.total_changes, before)
        self.assertNotEqual(project_root, 'root')
        self.assertEqual(self.coord.db.execute('SELECT root_session FROM root_team_stop_po_admissions WHERE stop_id=?',
                                               (project_stop,)).fetchone()[0], project_root)

        self.coord.db.execute('UPDATE agent_control_requests SET target_session=? WHERE id=?',
                              ('other-worker', project_inspection))
        root_with_tuple_mismatch = root_lifecycle.investigation_preflight(
            self.coord.db, project_root, project_stop, 'po', 'worker', self.worker_thread, 'host',
            self.request_id, self.message, project_inspection, self.expires, now=time.time())
        self.assertTrue(root_with_tuple_mismatch['checks']['inspection_request_requester_matches_target_root'])
        self.assertFalse(root_with_tuple_mismatch['checks']['inspection_request_matches_root_worker_binding'])
        self.assertCountEqual(root_with_tuple_mismatch['failed_checks'], [
            'inspection_request_target_matches_worker', 'inspection_request_matches_root_worker_binding'])
        adapted_tuple_mismatch = adapt_preflight(root_with_tuple_mismatch)
        self.assertEqual(adapted_tuple_mismatch['status'], 'blocked')
        self.assertCountEqual(adapted_tuple_mismatch['failed_checks'], [
            'inspection_request_target_matches_worker', 'inspection_request_matches_root_worker_binding'])
        self.coord.db.execute('UPDATE agent_control_requests SET target_session=? WHERE id=?',
                              ('worker', project_inspection))

        self.coord.db.execute('UPDATE agent_control_requests SET requester_session=? WHERE id=?',
                              ('po', project_inspection))
        po_requester = root_lifecycle.investigation_preflight(
            self.coord.db, project_root, project_stop, 'po', 'worker', self.worker_thread, 'host',
            self.request_id, self.message, project_inspection, self.expires, now=time.time())
        self.assertEqual(po_requester['status'], 'eligible', po_requester['failed_checks'])
        self.assertEqual(adapt_preflight(po_requester)['status'], 'eligible')
        self.assertFalse(po_requester['checks']['inspection_request_requester_matches_target_root'])
        self.assertTrue(po_requester['checks']['inspection_request_requester_matches_current_admitted_po'])

        self.coord.db.execute('UPDATE project_orchestrators SET root_session=? WHERE session_id=?',
                              ('stale-root', 'po'))
        stale_po = root_lifecycle.investigation_preflight(
            self.coord.db, project_root, project_stop, 'po', 'worker', self.worker_thread, 'host',
            self.request_id, self.message, project_inspection, self.expires, now=time.time())
        self.assertEqual(stale_po['status'], 'blocked')
        self.assertEqual(adapt_preflight(stale_po)['status'], 'blocked')
        self.assertFalse(stale_po['checks']['inspection_request_requester_matches_current_admitted_po'])
        self.assertIn('po_designation_matches_target_root', stale_po['failed_checks'])
        with self.assertRaises(CoordError):
            root_lifecycle.admit_investigation_task(
                self.coord, project_root, self.request_id, project_stop, 'po', 'worker',
                self.worker_thread, 'host', self.message, project_inspection, self.expires)
        self.assertEqual(self.coord.db.execute('SELECT COUNT(*) FROM root_team_stop_task_admissions').fetchone()[0], 0)
        self.coord.db.execute('UPDATE project_orchestrators SET root_session=? WHERE session_id=?',
                              (project_root, 'po'))

        self.coord.db.execute('INSERT INTO project_orchestrators VALUES(?,?)', ('other', project_root))
        self.coord.db.execute('UPDATE agent_control_requests SET requester_session=? WHERE id=?',
                              ('other', project_inspection))
        wrong_requester = root_lifecycle.investigation_preflight(
            self.coord.db, project_root, project_stop, 'po', 'worker', self.worker_thread, 'host',
            self.request_id, self.message, project_inspection, self.expires, now=time.time())
        self.assertFalse(wrong_requester['checks']['inspection_request_requester_matches_target_root'])
        self.assertFalse(wrong_requester['checks']['inspection_request_requester_matches_current_admitted_po'])
        self.assertEqual(wrong_requester['failed_checks'], ['inspection_request_matches_root_worker_binding'])
        adapted_wrong_requester = adapt_preflight(wrong_requester)
        self.assertEqual(adapted_wrong_requester['status'], 'blocked')
        self.assertEqual(adapted_wrong_requester['failed_checks'],
                         ['inspection_request_matches_root_worker_binding'])
        with self.assertRaises(CoordError):
            root_lifecycle.admit_investigation_task(
                self.coord, project_root, self.request_id, project_stop, 'po', 'worker',
                self.worker_thread, 'host', self.message, project_inspection, self.expires)
        self.assertEqual(self.coord.db.execute('SELECT COUNT(*) FROM root_team_stop_task_admissions').fetchone()[0], 0)
        self.coord.db.execute('UPDATE agent_control_requests SET requester_session=? WHERE id=?',
                              (project_root, project_inspection))
        self.coord.db.execute("UPDATE agent_control_requests SET status='failed' WHERE id=?", (project_inspection,))
        blocked = root_lifecycle.investigation_preflight(
            self.coord.db, project_root, project_stop, 'po', 'worker', self.worker_thread, 'host',
            self.request_id, self.message, project_inspection, self.expires, now=time.time())
        self.assertEqual(blocked['status'], 'blocked')
        self.assertEqual(blocked['failed_checks'], ['inspection_request_status_admissible'])
        adapted_ordinary_failure = adapt_preflight(blocked)
        self.assertEqual(adapted_ordinary_failure['status'], 'blocked')
        self.assertEqual(adapted_ordinary_failure['failed_checks'], ['inspection_request_status_admissible'])

    def test_mixed_existing_delivery_and_batch_membership_are_refused(self):
        self.coord.db.execute('INSERT INTO bridge_jobs VALUES(?,?,?,?,?,?,?,?)',
            ('mixed-job', self.worker_thread, 'mixed', 'prepared', None, None, time.time(), time.time()))
        self.coord.db.executemany('INSERT INTO bridge_deliveries VALUES(?,?)',
            [(self.message, 'mixed-job'), (self.unrelated, 'mixed-job')])
        with self.assertRaisesRegex(CoordError, 'will not be replayed'):
            self.admit()
        self.coord.db.execute('DELETE FROM bridge_deliveries')
        self.coord.db.execute('DELETE FROM bridge_jobs')
        self.coord.db.execute('INSERT INTO bridge_jobs VALUES(?,?,?,?,?,?,?,?)',
            ('already-prepared', self.worker_thread, 'old prepared payload', 'prepared', None, None,
             time.time(), time.time()))
        self.coord.db.execute('INSERT INTO bridge_deliveries VALUES(?,?)', (self.message, 'already-prepared'))
        with self.assertRaisesRegex(CoordError, 'will not be replayed'):
            self.admit()
        self.coord.db.execute('DELETE FROM bridge_deliveries')
        self.coord.db.execute('DELETE FROM bridge_jobs')
        self.coord.db.execute('INSERT INTO message_batches VALUES(?,?)', (self.message, 'batch'))
        with self.assertRaisesRegex(CoordError, 'batch'):
            self.admit()

    def test_preexisting_queued_delivery_cannot_be_adopted(self):
        self.coord.db.execute('INSERT INTO bridge_jobs VALUES(?,?,?,?,?,?,?,?)',
            ('already-queued', self.worker_thread, 'old payload', 'queued', 'native-queue-item', None,
             time.time(), time.time()))
        self.coord.db.execute('INSERT INTO bridge_deliveries VALUES(?,?)', (self.message, 'already-queued'))
        with self.assertRaisesRegex(CoordError, 'will not be replayed'):
            self.admit()

    def test_admitted_queued_item_waits_if_any_unrelated_native_entry_is_behind_it(self):
        rpc = InvestigationRpc(self.worker_thread)
        preload_id = self.continuation_and_preload()
        self.complete_preload_only(rpc, preload_id)
        admission = root_lifecycle.task_execution_admission(self.coord.db, self.message)
        bridge = Bridge(self.dispatch_state, rpc)
        message = {'id': self.message, 'recipient_session': 'worker'}
        payload = bridge.payload(self.worker_thread, [message], investigation_admission=admission)
        job = self.state.prepare(self.worker_thread, [message], payload,
                                 investigation_request_id=self.request_id)
        queue_id = 'admitted-native-item'
        self.state.update(job['id'], 'queued', queue_id=queue_id)
        rpc.queue_items.extend([
            {'id': queue_id, 'clientUserMessageId': job['id'], 'input': []},
            {'id': 'unrelated-behind', 'clientUserMessageId': 'other-job', 'input': []},
        ])
        bridge.advance(self.state.job(job['id']))
        self.assertEqual([item['id'] for item in rpc.queue_items], [queue_id, 'unrelated-behind'])
        self.assertEqual([method for method, _ in rpc.calls if method == 'thread/queue/start'], [])

    def test_settings_mismatch_keeps_original_message_held_and_never_sends_task_input(self):
        actual = expected_settings()
        actual['permissions']['approvalPolicy'] = 'never'
        preload_id = self.continuation_and_preload()
        rpc = InvestigationRpc(self.worker_thread)
        rpc.settings = actual
        bridge = Bridge(self.dispatch_state, rpc)
        ControlWorker(bridge).advance(self.controls.job(preload_id))
        self.assertEqual(self.controls.job(preload_id)['status'], 'failed')
        self.assertEqual(self.state.investigation_tasks('host'), [])
        self.assertEqual(self.coord.db.execute('SELECT COUNT(*) FROM bridge_deliveries').fetchone()[0], 0)
        self.assertEqual([method for method, _ in rpc.calls if method in ('thread/queue/add','thread/queue/start')], [])

    def test_every_protected_settings_component_is_part_of_the_exact_match(self):
        mutations = (
            lambda value: value.update(model='different-model'),
            lambda value: value.update(reasoning_effort='low'),
            lambda value: value.update(cwd='/workspace/elsewhere'),
            lambda value: value.update(workspaces=['/workspace/elsewhere']),
            lambda value: value['permissions']['activePermissionProfile'].update(id='different-profile'),
            lambda value: value['permissions'].update(approvalPolicy='never'),
            lambda value: value['permissions'].update(approvalsReviewer='different-reviewer'),
            lambda value: value['permissions']['sandbox'].update(networkAccess=True),
            lambda value: value['permissions']['sandbox'].update(writableRoots=[]),
        )
        for index, mutate in enumerate(mutations):
            with self.subTest(component=index):
                if index:
                    self.setUp()
                preload_id = self.continuation_and_preload()
                actual = expected_settings()
                mutate(actual)
                rpc = InvestigationRpc(self.worker_thread)
                rpc.settings = actual
                worker = ControlWorker(Bridge(self.dispatch_state, rpc))
                worker.advance(self.controls.job(preload_id))
                self.assertEqual(self.controls.job(preload_id)['status'], 'failed')
                self.assertEqual(self.state.investigation_tasks('host'), [])
                self.assertEqual(self.coord.db.execute('SELECT COUNT(*) FROM bridge_deliveries').fetchone()[0], 0)
                self.assertEqual([method for method, _ in rpc.calls
                                  if method in ('thread/queue/add','thread/queue/start')], [])

    def test_missing_protected_readback_fails_closed_without_replaying(self):
        preload_id = self.continuation_and_preload()
        rpc = InvestigationRpc(self.worker_thread)
        rpc.settings['permissions']['sandbox'] = None
        ControlWorker(Bridge(self.dispatch_state, rpc)).advance(self.controls.job(preload_id))
        self.assertEqual(self.controls.job(preload_id)['status'], 'failed')
        self.assertEqual(self.state.investigation_tasks('host'), [])
        self.assertEqual([method for method, _ in rpc.calls].count('thread/resume'), 1)
        self.assertEqual([method for method, _ in rpc.calls
                          if method in ('thread/queue/add','thread/queue/start')], [])

    def test_scope_drift_before_metadata_load_prevents_native_resume(self):
        preload_id = self.continuation_and_preload()
        rpc = InvestigationRpc(self.worker_thread)
        self.coord.db.execute("UPDATE project_orchestrators SET root_session='other-root' WHERE session_id='po'")
        ControlWorker(Bridge(self.dispatch_state, rpc)).advance(self.controls.job(preload_id))
        self.assertEqual(self.controls.job(preload_id)['status'], 'failed')
        self.assertEqual(rpc.calls, [])
        self.assertEqual(self.state.investigation_tasks('host'), [])

    def test_interrupted_preload_is_terminal_uncertain_and_never_retried(self):
        preload_id = self.continuation_and_preload()
        pending = self.controls.job(preload_id)
        self.controls.update(preload_id, pending['status'], 'preloading')
        rpc = InvestigationRpc(self.worker_thread)
        worker = ControlWorker(Bridge(self.dispatch_state, rpc))
        worker.advance(self.controls.job(preload_id))
        self.assertEqual(self.controls.job(preload_id)['status'], 'uncertain')
        self.assertEqual(rpc.calls, [])
        worker.advance(self.controls.job(preload_id))
        self.assertEqual(rpc.calls, [])
        self.assertEqual(self.state.investigation_tasks('host'), [])
        self.assertEqual(self.coord.db.execute('SELECT COUNT(*) FROM bridge_deliveries').fetchone()[0], 0)

    def test_preload_requires_current_po_and_preserves_nonempty_native_queue(self):
        self.admit(expires_at=time.time() + 0.01)
        time.sleep(0.02)
        self.coord.db.execute("UPDATE agent_control_requests SET status='completed' WHERE id=?",
                              (self.inspect_id,))
        self.coord.session = 'po'
        source_message = self.coord.send('root', 'Attested expected settings.')['id']
        self.coord.session = 'root'
        now = time.time()
        baseline_id, continuation_id = str(uuid.uuid4()), str(uuid.uuid4())
        root_lifecycle.create_task_settings_continuation(
            self.coord, actor_root='root', admission_request_id=self.request_id,
            baseline_id=baseline_id, continuation_id=continuation_id,
            expected_settings=expected_settings(), source_message_id=source_message,
            source_artifact_name='po-settings.json', source_artifact_sha256='b' * 64,
            source_kind='po_attested_historical', source_limit='historical PO-attested transcription',
            expires_at=now + 900, now=now)
        with self.assertRaisesRegex(CoordError, 'only the exact current PO'):
            dispatch(self.coord, {'op':'agent-investigation-preload','session':'other','host_id':'host',
                'params':{'to':'worker','expected_thread':self.worker_thread,
                          'continuation_id':continuation_id,'request_id':str(uuid.uuid4()),'confirm':True}})
        preload_id = str(uuid.uuid4())
        dispatch(self.coord, {'op':'agent-investigation-preload','session':'po','host_id':'host',
            'params':{'to':'worker','expected_thread':self.worker_thread,
                      'continuation_id':continuation_id,'request_id':preload_id,'confirm':True}})
        rpc = InvestigationRpc(self.worker_thread)
        unrelated = {'id':'unrelated', 'clientUserMessageId':'other', 'input':[{'type':'text','text':'preserve'}]}
        rpc.queue_items.append(unrelated)
        self.complete_preload_only(rpc, preload_id)
        self.assertEqual(self.controls.job(preload_id)['status'], 'failed')
        self.assertEqual(rpc.queue_items, [unrelated])
        self.assertEqual([method for method, _ in rpc.calls if method == 'thread/resume'], [])
        self.assertEqual(self.state.investigation_tasks('host'), [])

    def test_worker_host_drift_revokes_admission_before_native_effect(self):
        self.admit()
        rpc = InvestigationRpc(self.worker_thread)
        self.coord.db.execute("UPDATE remote_session_hosts SET host_id='other-host' WHERE session_id='worker'")
        self.assertEqual(self.state.investigation_tasks('host'), [])
        with self.assertRaises(CoordError):
            self.state.prepare(self.worker_thread, [{'id': self.message, 'recipient_session': 'worker'}],
                'stale-host payload', investigation_request_id=self.request_id)
        self.assertEqual(rpc.calls, [])
        self.assertEqual(self.coord.db.execute('SELECT COUNT(*) FROM bridge_deliveries').fetchone()[0], 0)

    def test_expired_admission_cannot_accept_a_late_native_start_transition(self):
        preload_id = self.continuation_and_preload()
        rpc = InvestigationRpc(self.worker_thread)
        self.complete_preload_only(rpc, preload_id)
        admission = root_lifecycle.task_execution_admission(self.coord.db, self.message)
        bridge = Bridge(self.dispatch_state, InvestigationRpc(self.worker_thread))
        message = {'id': self.message, 'recipient_session': 'worker'}
        job = self.state.prepare(self.worker_thread, [message],
            bridge.payload(self.worker_thread, [message], investigation_admission=admission),
            investigation_request_id=self.request_id)
        continuation_expiry = admission['continuation']['expires_at']
        with mock.patch('agent_chat.root_lifecycle.time.time', return_value=continuation_expiry + 1):
            with self.assertRaises(CoordError):
                self.state.update(job['id'], 'starting')
        self.assertEqual(self.state.job(job['id'])['status'], 'prepared')
        self.assertEqual(bridge.rpc.calls, [])

    def test_admission_expires_or_stop_drift_blocks_native_effect(self):
        self.admit()
        rpc = InvestigationRpc(self.worker_thread)
        bridge = Bridge(self.dispatch_state, rpc)
        with mock.patch('agent_chat.root_lifecycle.time.time', return_value=self.expires + 1):
            bridge.tick(observe_policies=False)
        self.assertEqual([method for method, _ in rpc.calls if method in ('thread/queue/add','thread/queue/start')], [])
        self.assertEqual(self.coord.db.execute('SELECT COUNT(*) FROM bridge_jobs').fetchone()[0], 0)

    def test_nonempty_native_queue_is_preserved_and_withholds_the_admitted_task(self):
        self.admit()
        rpc = InvestigationRpc(self.worker_thread)
        unrelated = {'id': 'preexisting', 'clientUserMessageId': 'unrelated', 'input': [{'type': 'text', 'text': 'keep'}]}
        rpc.queue_items.append(unrelated)
        Bridge(self.dispatch_state, rpc).tick(observe_policies=False)
        self.assertEqual(rpc.queue_items, [unrelated])
        self.assertEqual([method for method, _ in rpc.calls].count('thread/queue/add'), 0)
        self.assertEqual([method for method, _ in rpc.calls].count('thread/queue/start'), 0)
        self.assertEqual(self.coord.db.execute('SELECT COUNT(*) FROM bridge_deliveries').fetchone()[0], 0)

    def test_po_binding_or_designation_drift_revokes_admission_without_native_effect(self):
        self.admit()
        rpc = InvestigationRpc(self.worker_thread)
        self.coord.db.execute("UPDATE project_orchestrators SET root_session='other-root' WHERE session_id='po'")
        self.assertEqual(self.state.investigation_tasks('host'), [])
        Bridge(self.dispatch_state, rpc).tick(observe_policies=False)
        self.assertEqual([method for method, _ in rpc.calls if method in ('thread/queue/add','thread/queue/start')], [])
        self.assertEqual(self.coord.db.execute('SELECT COUNT(*) FROM bridge_deliveries').fetchone()[0], 0)

    def test_second_active_stop_denies_task_without_releasing_either_stop(self):
        self.admit()
        second = str(uuid.uuid4())
        self.coord.db.execute('INSERT INTO root_team_stops VALUES(?,?,1,?,NULL)',
                              (second, 'root', time.time()))
        self.assertIsNone(root_lifecycle.task_admission(self.coord.db, self.message))
        rpc = InvestigationRpc(self.worker_thread)
        Bridge(self.dispatch_state, rpc).tick(observe_policies=False)
        self.assertEqual([method for method, _ in rpc.calls if method in ('thread/queue/add','thread/queue/start')], [])
        self.assertTrue(root_lifecycle.stopped(self.coord.db, 'worker'))

    def test_idempotent_retry_requires_identical_scope_and_audit_is_immutable(self):
        first = self.admit()
        again = self.admit()
        self.assertEqual(first['request_id'], again['request_id'])
        with self.assertRaises(CoordError):
            self.admit(message_id=self.unrelated)
        with self.assertRaisesRegex(Exception, 'immutable'):
            self.coord.db.execute('DELETE FROM root_team_stop_task_admissions WHERE request_id=?', (self.request_id,))

    def test_stop_status_exposes_only_exact_admission_metadata_and_live_effectiveness(self):
        self.admit()
        status = root_lifecycle.coordination_status(self.coord.db, 'root', 'po', self.stop_id)
        self.assertEqual(len(status['task_admissions']), 1)
        item = status['task_admissions'][0]
        self.assertEqual((item['request_id'], item['stop_id'], item['message_id'], item['inspection_request_id']),
                         (self.request_id, self.stop_id, self.message, self.inspect_id))
        self.assertEqual((item['po_session'], item['target_session'], item['thread_id'], item['host_id']),
                         ('po', 'worker', self.worker_thread, 'host'))
        self.assertTrue(item['effective'])
        self.assertNotIn('body', item)
        with mock.patch('agent_chat.root_lifecycle.time.time', return_value=self.expires + 1):
            expired = root_lifecycle.coordination_status(self.coord.db, 'root', 'po', self.stop_id)
        self.assertFalse(expired['task_admissions'][0]['effective'])


class InvestigationBridgeState:
    """Local stand-in for the authenticated RemoteBridgeState protocol."""
    def __init__(self, state, host):
        self.state, self.host = state, host

    def __getattr__(self, name):
        return getattr(self.state, name)

    def control_jobs(self):
        return self.state.controls.jobs(self.host)

    def control_job(self, job_id):
        return self.state.controls.job(job_id)

    def control_update(self, job_id, expected, status, **values):
        return self.state.controls.update(job_id, expected, status, **values)

    def investigation_tasks(self):
        return self.state.investigation_tasks(self.host)

    def prepare(self, thread_id, messages, payload, *, investigation_request_id=None):
        return self.state.prepare(thread_id, messages, payload,
                                  investigation_request_id=investigation_request_id)


if __name__ == '__main__':
    unittest.main()
