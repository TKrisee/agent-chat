import contextlib
import hashlib
import io
import json
import sqlite3
import tempfile
import threading
import time
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from agent_chat.agent_control import AgentControlState
from agent_chat.bridge_state import BridgeState
from agent_chat.core import Coordinator, CoordError
from agent_chat.remote_service import dispatch
from agent_chat.root_lifecycle import admit_investigation_task, admit_po, pause
from agent_chat.root_orchestrator import RootOrchestrator
from agent_chat import root_local, root_orchestrator


class ProjectStores:
    def __init__(self, home_path, project_path, project_id):
        self.path = Path(home_path)
        self._paths = {'default': Path(home_path), project_id: Path(project_path)}

    @contextlib.contextmanager
    def connection(self):
        coord = Coordinator(self.path)
        try:
            yield coord.db
        finally:
            coord.close()

    def db_path(self, project):
        return self._paths[project]


class Native13ContinuationProjectionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='native13-root-projection-')
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name).resolve()
        self.project_id = 'project-inlst-synthetic'
        self.home_id = 'root-home'
        self.project_root = 'root-project'
        self.po = 'current-po'
        self.worker = 'assigned-worker'
        self.non_po = 'registered-non-po-sender'
        self.other_recipient = 'other-project-session'
        self.host = 'host-synthetic'
        self.home_thread = str(uuid.uuid4())
        self.project_thread = str(uuid.uuid4())
        self.po_thread = str(uuid.uuid4())
        self.worker_thread = str(uuid.uuid4())
        self.stop_id = str(uuid.uuid4())
        self.admission_id = str(uuid.uuid4())
        self.inspect_id = str(uuid.uuid4())
        self.task_message_id = None
        self.home_path = self.directory / 'home.sqlite3'
        self.project_path = self.directory / (self.home_path.name + '.projects') / self.project_id / 'state.sqlite3'
        self.project_path.parent.mkdir(parents=True)
        self.stores = ProjectStores(self.home_path, self.project_path, self.project_id)
        self.stores.root_lock = threading.RLock()
        self.home = Coordinator(self.home_path, self.home_id)
        self.addCleanup(self.home.close)
        self.project = Coordinator(self.project_path, self.project_root)
        self.addCleanup(self.project.close)

        for coord, sessions in (
                (self.home, ((self.home_id, 'root'), (self.po, 'orchestrator'),
                             (self.project_root, 'orchestrator'))),
                (self.project, ((self.project_root, 'root'), (self.po, 'orchestrator'),
                                (self.worker, 'worker'), (self.non_po, 'worker'),
                                (self.other_recipient, 'worker')))):
            for session, agent in sessions:
                dispatch(coord, {'op': 'register', 'session': session, 'host_id': self.host,
                                 'params': {'agent': agent}})

        self.home.session = self.home_id
        BridgeState(self.home).bind(self.home_thread)
        self.project.session = self.project_root
        target_state = BridgeState(self.project)
        target_state.bind(self.project_thread)
        self.project.session = self.po
        target_state.bind(self.po_thread)
        self.project.session = self.worker
        target_state.bind(self.worker_thread)

        self.project.db.execute('CREATE TABLE IF NOT EXISTS project_orchestrators('
                                'session_id TEXT PRIMARY KEY,root_session TEXT NOT NULL)')
        self.project.db.execute('INSERT INTO project_orchestrators VALUES(?,?)',
                                (self.po, self.project_root))
        self.project.db.execute('CREATE TABLE IF NOT EXISTS root_projections(session_id TEXT PRIMARY KEY)')
        self.project.db.execute('INSERT INTO root_projections VALUES(?)', (self.project_root,))
        pause(self.project, self.project_root, self.stop_id)
        admit_po(self.project, self.project_root, self.stop_id, self.po, self.po_thread, self.host)

        self.project.session = self.po
        AgentControlState(self.project).request(
            self.worker, self.worker_thread, 'inspect', self.inspect_id, False)
        self.task_message_id = self.project.send(
            self.worker, 'Read the existing assigned email and report cause only.')['id']
        with mock.patch('agent_chat.root_lifecycle.time.time', return_value=1_000_000):
            admit_investigation_task(
                self.project, self.project_root, self.admission_id, self.stop_id,
                self.po, self.worker, self.worker_thread, self.host,
                self.task_message_id, self.inspect_id, 1_000_010, now=1_000_000)

        self.roots = RootOrchestrator(self.stores)
        with self.stores.connection() as db:
            db.execute('''INSERT INTO root_config
                (singleton,home_project,home_session,thread_id,host_id,cwd,agent,created_at,mode)
                VALUES(1,?,?,?,?,?,?,?,?)''',
                ('default', self.home_id, self.home_thread, self.host,
                 str(self.directory), 'root', time.time(), 'running'))
            db.execute('INSERT INTO root_directory_owner VALUES(1,?,?)', ('default', self.home_id))
            db.execute('''INSERT INTO root_projects(project_id,session_id,joined,orchestrator_session)
                VALUES(?,?,1,?)''', (self.project_id, self.project_root, self.po))
            db.execute('INSERT INTO root_team_operations VALUES(?,?,?,NULL)',
                       (self.stop_id, self.home_id, time.time()))
            db.execute('INSERT INTO root_team_operation_projects VALUES(?,?,?,NULL)',
                       (self.stop_id, self.project_id, self.project_root))

        self.expected = {
            'model': 'gpt-6-luna', 'reasoning_effort': 'high',
            'cwd': str(self.directory), 'workspaces': [str(self.directory)],
            'permissions': {
                'activePermissionProfile': {'id': 'standard', 'extends': None},
                'approvalPolicy': 'on-request', 'approvalsReviewer': 'auto_review',
                'runtimeWorkspaceRoots': [str(self.directory)],
                'sandbox': {'type': 'workspace-write', 'writableRoots': [str(self.directory)],
                            'networkAccess': False, 'excludeSlashTmp': False,
                            'excludeTmpdirEnvVar': False},
            },
        }
        self.artifact_name = 'reviewer-expected-settings-v1.json'
        self.artifact_bytes = json.dumps(self.expected, sort_keys=True, separators=(',', ':')).encode()
        self.artifact_path = self.directory / self.artifact_name
        self.artifact_path.write_bytes(self.artifact_bytes)
        self.artifact_sha = hashlib.sha256(self.artifact_bytes).hexdigest()

    def source_message(self, sender, recipient):
        self.project.session = sender
        return self.project.send(recipient, 'PO-attested historical expected settings.',
                                 attachments=[str(self.artifact_path)])['id']

    def home_only_source_message(self):
        self.home.session = self.po
        return self.home.send(self.project_root, 'PO-attested historical expected settings.',
                              attachments=[str(self.artifact_path)])['id']

    def continue_settings(self, source_id, *, po=None, target_project=None,
                          artifact_name=None, artifact_sha=None):
        return self.roots.continue_investigation_settings(
            'default', self.home_id, self.host, target_project or self.project_id,
            po or self.po, self.worker, self.worker_thread, self.host, self.stop_id,
            self.admission_id, str(uuid.uuid4()), str(uuid.uuid4()), self.task_message_id,
            self.inspect_id, source_id, artifact_name or self.artifact_name,
            artifact_sha or self.artifact_sha,
            'PO-attested historical transcription; original raw snapshot unavailable.',
            self.expected, time.time() + 300, True)

    def test_accepts_current_po_attestation_to_exact_enrolled_project_root_with_attachment(self):
        source_id = self.source_message(self.po, self.project_root)
        result = self.continue_settings(source_id)
        self.assertEqual(result['state'], 'awaiting_current_po_metadata_preload')
        self.assertFalse(result['native_preload_started'])
        self.assertFalse(result['original_message_released'])
        with contextlib.closing(Coordinator(self.project_path, self.project_root)) as coord:
            row = coord.db.execute('''SELECT root_session,po_session,target_session,message_id,
                source_message_id,source_artifact_name,source_artifact_sha256 FROM root_team_stop_task_baselines
                WHERE admission_request_id=?''', (self.admission_id,)).fetchone()
            self.assertEqual(tuple(row), (self.project_root, self.po, self.worker,
                                          self.task_message_id, source_id,
                                          self.artifact_name, self.artifact_sha))

    def test_rejects_home_recipient_wrong_sender_and_artifact_mismatch_without_writing(self):
        wrong_recipient = self.source_message(self.po, self.other_recipient)
        with self.assertRaisesRegex(CoordError, 'enrolled project Root'):
            self.continue_settings(wrong_recipient)

        wrong_sender = self.source_message(self.non_po, self.project_root)
        with self.assertRaisesRegex(CoordError, 'enrolled project Root'):
            self.continue_settings(wrong_sender)

        right_message = self.source_message(self.po, self.project_root)
        with self.assertRaisesRegex(CoordError, 'exactly one attachment'):
            self.continue_settings(right_message, artifact_sha='0' * 64)

        self.project.session = self.po
        batch = self.project.send_many([self.project_root, self.worker],
            'PO-attested historical expected settings.', attachments=[str(self.artifact_path)])
        with self.assertRaisesRegex(CoordError, 'retained direct current-PO message'):
            self.continue_settings(batch[0]['id'])

        with contextlib.closing(Coordinator(self.project_path, self.project_root)) as coord:
            self.assertEqual(coord.db.execute(
                'SELECT COUNT(*) FROM root_team_stop_task_baselines WHERE admission_request_id=?',
                (self.admission_id,)).fetchone()[0], 0)
            self.assertEqual(coord.db.execute(
                'SELECT COUNT(*) FROM root_team_stop_task_continuations WHERE admission_request_id=?',
                (self.admission_id,)).fetchone()[0], 0)

    def test_home_only_source_is_not_used_for_project_provenance(self):
        source_id = self.home_only_source_message()
        with self.assertRaisesRegex(CoordError, 'retained direct current-PO message'):
            self.continue_settings(source_id)
        with contextlib.closing(Coordinator(self.project_path, self.project_root)) as coord:
            self.assertEqual(coord.db.execute(
                'SELECT COUNT(*) FROM root_team_stop_task_baselines WHERE admission_request_id=?',
                (self.admission_id,)).fetchone()[0], 0)
            self.assertEqual(coord.db.execute(
                'SELECT COUNT(*) FROM root_team_stop_task_continuations WHERE admission_request_id=?',
                (self.admission_id,)).fetchone()[0], 0)

    def test_wrong_project_or_po_still_fails_before_provenance_acceptance(self):
        source_id = self.source_message(self.po, self.project_root)
        with self.assertRaisesRegex(CoordError, 'unchanged enrolled PO'):
            self.continue_settings(source_id, target_project='another-project')
        with self.assertRaisesRegex(CoordError, 'unchanged enrolled PO'):
            self.continue_settings(source_id, po='other-po')

    def _prepare_project_root_projection(self):
        self.project.db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('project_id',?)",
                                (self.project_id,))
        self.project.db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('schema_version','1')")
        self.project.db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('project_registry',?)",
                                (str(self.home_path.resolve()),))
        self.project.db.execute('DROP TABLE IF EXISTS root_projections')
        self.project.db.execute('''CREATE TABLE root_projections(
            session_id TEXT PRIMARY KEY,home_project TEXT NOT NULL,home_session TEXT NOT NULL,
            thread_id TEXT NOT NULL,cwd TEXT,paused INTEGER,model TEXT,effort TEXT)''')
        self.project.db.execute('INSERT INTO root_projections VALUES(?,?,?,?,?,?,?,?)',
            (self.project_root, 'default', self.home_id, self.project_thread, str(self.directory), 0, None, None))

    def _managed_read_request(self, *, native_state='active', direct_input=False, queue_items=None,
                              after_queue=None, before_read=None, request_id=None, job_id=None):
        self._prepare_project_root_projection()
        job_id = job_id or 'wake_' + uuid.uuid4().hex
        now = time.time()
        self.project.db.execute('INSERT INTO bridge_jobs VALUES(?,?,?,?,?,?,?,?)',
            (job_id, self.worker_thread, '{"text":"PRIVATE TASK INPUT"}', 'dispatched',
             None, None, now, now))
        self.project.db.execute('INSERT INTO bridge_deliveries VALUES(?,?)',
                                (self.task_message_id, job_id))
        request_id = request_id or str(uuid.uuid4())
        class FakeRpc:
            def __init__(inner): inner.calls = []
            def __enter__(inner): return inner
            def __exit__(inner, *_): return None
            def request(inner, method, params):
                inner.calls.append((method, dict(params)))
                if method == 'thread/read':
                    inner.worker_jobs_during_read = [item['id'] for item in AgentControlState(self.project).jobs(self.host)]
                    inner.result_state_during_read = self.project.db.execute(
                        'SELECT status FROM root_managed_native_reads WHERE request_id=?',
                        (inner.request_id,)).fetchone()['status']
                    inner.control_rows_during_read = self.project.db.execute(
                        'SELECT COUNT(*) FROM agent_control_requests WHERE id=?',
                        (inner.request_id,)).fetchone()[0]
                    return {'thread': {'id': params['threadId'], 'status': {'type': native_state},
                        'canAcceptDirectInput': direct_input, 'turns': [{'text': 'PRIVATE TURN'}]}}
                if method == 'thread/queue/list':
                    if after_queue:
                        after_queue()
                    return {'data': queue_items if queue_items is not None else [
                        {'text': 'PRIVATE QUEUED INPUT'}], 'nextCursor': None}
                raise AssertionError('unexpected provider method: ' + method)
        rpc = FakeRpc()
        rpc.request_id = str(uuid.UUID(request_id))
        if before_read:
            before_read()
        with mock.patch('agent_chat.rpc.RpcClient', return_value=rpc), \
             mock.patch.dict('os.environ', {'AGENT_CHAT_CODEX_SERVER': 'ws://127.0.0.1:4500'}):
            result = self.roots.managed_native_read('default', self.home_id, self.host,
                self.project_id, self.po, self.worker, self.worker_thread, self.host,
                self.stop_id, self.admission_id, self.task_message_id, job_id,
                self.inspect_id, request_id)
        return request_id, job_id, result, rpc

    def test_managed_native_read_records_only_narrow_foreground_observation(self):
        self.assertIsNone(self.project.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_managed_native_reads'").fetchone())
        request_id, job_id, created, rpc = self._managed_read_request()
        self.assertEqual(created['status'], 'observed')
        self.assertEqual(created['original_job']['id'], job_id)
        self.assertNotIn('PRIVATE TASK INPUT', json.dumps(created))
        status = created
        self.assertEqual(status['result']['native_state'], 'active')
        self.assertIs(status['result']['direct_input'], False)
        self.assertEqual(status['result']['queue_count'], 1)
        self.assertEqual(status['result']['queue_state'], 'known')
        self.assertIsNone(status['result']['turn_id'])
        self.assertFalse(status['result']['turn_id_known'])
        self.assertNotIn('PRIVATE TASK INPUT', json.dumps(status))
        self.assertNotIn('PRIVATE TURN', json.dumps(status))
        self.assertNotIn('PRIVATE QUEUED INPUT', json.dumps(status))
        self.assertEqual([call[0] for call in rpc.calls], ['thread/read', 'thread/queue/list'])
        self.assertEqual(rpc.worker_jobs_during_read,
                         [item['id'] for item in AgentControlState(self.project).jobs(self.host)])
        self.assertEqual(rpc.result_state_during_read, 'reading')
        self.assertEqual(rpc.control_rows_during_read, 0)
        self.assertEqual(rpc.calls[0][1], {'threadId': self.worker_thread, 'includeTurns': False})
        # Same immutable ID is a readback; it cannot repeat provider calls.
        with mock.patch('agent_chat.rpc.RpcClient') as rpc_factory:
            repeated = self.roots.managed_native_read('default', self.home_id, self.host,
                self.project_id, self.po, self.worker, self.worker_thread, self.host,
                self.stop_id, self.admission_id, self.task_message_id, job_id,
                self.inspect_id, request_id)
        self.assertEqual(repeated['result'], created['result'])
        rpc_factory.assert_not_called()
        self.assertEqual(self.project.db.execute(
            'SELECT COUNT(*) FROM agent_control_requests WHERE id=?', (request_id,)).fetchone()[0], 0)

    def test_interrupted_read_remains_reading_and_same_id_never_reexecutes(self):
        request_id, job_id = str(uuid.uuid4()), 'wake_' + uuid.uuid4().hex
        with mock.patch('agent_chat.root_orchestrator._read_managed_native_observation',
                        side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                self._managed_read_request(request_id=request_id, job_id=job_id)
        saved = self.project.db.execute(
            'SELECT status FROM root_managed_native_reads WHERE request_id=?', (request_id,)).fetchone()
        self.assertEqual(saved['status'], 'reading')
        with mock.patch('agent_chat.rpc.RpcClient') as rpc_factory:
            replay = self.roots.managed_native_read('default', self.home_id, self.host,
                self.project_id, self.po, self.worker, self.worker_thread, self.host,
                self.stop_id, self.admission_id, self.task_message_id, job_id,
                self.inspect_id, request_id)
        self.assertEqual(replay['status'], 'reading')
        self.assertIsNone(replay['result'])
        rpc_factory.assert_not_called()

    def test_not_loaded_returns_unavailable_without_loading_or_payload_reads(self):
        request_id, _, created, rpc = self._managed_read_request(native_state='notLoaded', direct_input=True)
        result = created['result']
        self.assertEqual(created['status'], 'unavailable')
        self.assertEqual(result['native_state'], 'notLoaded')
        self.assertEqual(result['queue_state'], 'not_loaded')
        self.assertIsNone(result['queue_count'])
        self.assertIsNone(result['direct_input'])
        self.assertEqual([call[0] for call in rpc.calls], ['thread/read'])

    def test_post_read_binding_drift_withholds_values_and_records_unknown(self):
        def drift_binding():
            self.project.db.execute('UPDATE bridge_bindings SET thread_id=? WHERE session_id=?',
                                    (str(uuid.uuid4()), self.worker))
        request_id, _, created, _ = self._managed_read_request(after_queue=drift_binding)
        result = created['result']
        self.assertEqual(created['status'], 'unknown')
        self.assertEqual(result['native_state'], 'unknown')
        self.assertEqual(result['error_code'], 'scope_changed')
        self.assertIsNone(result['queue_count'])

    def test_pre_rpc_binding_drift_is_unknown_and_never_calls_provider(self):
        original = __import__('agent_chat.root_orchestrator', fromlist=['_managed_native_request'])._managed_native_request
        def commit_then_drift(*args, **kwargs):
            saved, is_new = original(*args, **kwargs)
            if is_new:
                self.project.db.execute('UPDATE bridge_bindings SET thread_id=? WHERE session_id=?',
                                         (str(uuid.uuid4()), self.worker))
            return saved, is_new
        with mock.patch('agent_chat.root_orchestrator._managed_native_request', side_effect=commit_then_drift), \
             mock.patch('agent_chat.rpc.RpcClient') as rpc_factory:
            request_id, _, created, _ = self._managed_read_request()
        self.assertEqual(created['status'], 'unknown')
        self.assertEqual(created['result']['error_code'], 'scope_changed')
        rpc_factory.assert_not_called()

    def test_uppercase_request_uuid_is_normalized_for_insert_and_completion(self):
        lower_id = str(uuid.uuid4())
        request_id, _, created, rpc = self._managed_read_request(request_id=lower_id.upper())
        self.assertEqual(created['request_id'], lower_id)
        self.assertEqual(created['status'], 'observed')
        self.assertEqual(len(rpc.calls), 2)

    def test_production_wake_id_is_accepted_as_opaque_job_selector(self):
        request_id, job_id, created, _ = self._managed_read_request(
            job_id='wake_83a1ea300d2144d78ebd01b428d47cd8')
        self.assertEqual(created['original_job']['id'], job_id)

    def test_unresolved_target_control_fails_before_schema_or_provider_read(self):
        def add_control():
            now = time.time()
            self.project.db.execute('''INSERT INTO agent_control_requests(
                id,requester_session,target_session,host_id,thread_id,action,interrupt,status,turn_id,error,
                created_at,updated_at,requested_model,requested_effort,previous_admission_mode,allow_cold_resume)
                VALUES(?,?,?,?,?,'inspect',0,'pending',NULL,NULL,?,?,NULL,NULL,NULL,0)''',
                (str(uuid.uuid4()), self.non_po, self.worker, self.host, self.worker_thread, now, now))
        with mock.patch('agent_chat.rpc.RpcClient') as factory:
            with self.assertRaisesRegex(CoordError, 'control, reset or quarantine'):
                self._managed_read_request(before_read=add_control)
        factory.assert_not_called()
        self.assertIsNone(self.project.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_managed_native_reads'").fetchone())

    def test_wrong_task_actor_or_delivery_linkage_fails_before_request_write(self):
        self._prepare_project_root_projection()
        request_id, job_id = str(uuid.uuid4()), str(uuid.uuid4())
        now = time.time()
        self.project.db.execute('INSERT INTO bridge_jobs VALUES(?,?,?,?,?,?,?,?)',
            (job_id, self.worker_thread, '{}', 'dispatched', None, None, now, now))
        self.project.db.execute('INSERT INTO bridge_deliveries VALUES(?,?)',
                                (self.task_message_id, job_id))
        with self.assertRaises(CoordError):
            from agent_chat.root_orchestrator import _managed_native_request
            _managed_native_request(self.project.db, actor_session=self.non_po,
                home_project='default', home_session=self.home_id, home_thread=self.home_thread,
                home_host=self.host,
                target_session=self.worker, expected_thread=self.worker_thread, expected_host=self.host,
                project_id=self.project_id, project_root=self.project_root, po_session=self.po,
                stop_id=self.stop_id, admission_request_id=self.admission_id,
                message_id=self.task_message_id, job_id=job_id,
                inspection_request_id=self.inspect_id, request_id=request_id)
        self.assertEqual(self.project.db.execute(
            'SELECT COUNT(*) FROM agent_control_requests WHERE id=?', (request_id,)).fetchone()[0], 0)
        self.assertIsNone(self.project.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_managed_native_reads'").fetchone())
        self.project.db.execute('DELETE FROM bridge_deliveries WHERE message_id=?', (self.task_message_id,))
        self.project.db.execute('DELETE FROM bridge_jobs WHERE id=?', (job_id,))

    def test_canonical_project_root_projection_drift_fails_before_audit_or_provider(self):
        self._prepare_project_root_projection()
        self.project.db.execute('UPDATE root_projections SET home_session=? WHERE session_id=?',
                                (self.non_po, self.project_root))
        job_id = 'wake_' + uuid.uuid4().hex
        now = time.time()
        self.project.db.execute('INSERT INTO bridge_jobs VALUES(?,?,?,?,?,?,?,?)',
            (job_id, self.worker_thread, '{}', 'dispatched', None, None, now, now))
        self.project.db.execute('INSERT INTO bridge_deliveries VALUES(?,?)', (self.task_message_id, job_id))
        with mock.patch('agent_chat.rpc.RpcClient') as rpc_factory:
            with self.assertRaisesRegex(CoordError, 'ownership, stop, task or original wake'):
                self.roots.managed_native_read('default', self.home_id, self.host,
                    self.project_id, self.po, self.worker, self.worker_thread, self.host,
                    self.stop_id, self.admission_id, self.task_message_id, job_id,
                    self.inspect_id, str(uuid.uuid4()))
        rpc_factory.assert_not_called()
        self.assertIsNone(self.project.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_managed_native_reads'").fetchone())

    def test_wake_with_second_delivery_is_refused_before_audit(self):
        self._prepare_project_root_projection()
        job_id = 'wake_' + uuid.uuid4().hex
        now = time.time()
        self.project.db.execute('INSERT INTO bridge_jobs VALUES(?,?,?,?,?,?,?,?)',
            (job_id, self.worker_thread, '{}', 'dispatched', None, None, now, now))
        self.project.db.execute('INSERT INTO bridge_deliveries VALUES(?,?)', (self.task_message_id, job_id))
        other = self.project.send(self.other_recipient, 'unrelated')
        self.project.db.execute('INSERT INTO bridge_deliveries VALUES(?,?)', (other['id'], job_id))
        with mock.patch('agent_chat.rpc.RpcClient') as rpc_factory:
            with self.assertRaisesRegex(CoordError, 'ownership, stop, task or original wake'):
                self.roots.managed_native_read('default', self.home_id, self.host,
                    self.project_id, self.po, self.worker, self.worker_thread, self.host,
                    self.stop_id, self.admission_id, self.task_message_id, job_id,
                    self.inspect_id, str(uuid.uuid4()))
        rpc_factory.assert_not_called()
        self.assertIsNone(self.project.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_managed_native_reads'").fetchone())

    def test_typed_root_local_cli_routes_exact_scope_to_foreground_operation(self):
        captured = {}
        request_id, job_id = str(uuid.uuid4()), 'wake_' + uuid.uuid4().hex

        class FakeProjects:
            def __init__(inner, _master):
                inner.root_lock = threading.RLock()

        class FakeRoots:
            def __init__(inner, projects):
                inner.projects = projects

            def managed_native_read(inner, *args):
                captured['args'] = args
                return {'status': 'completed', 'request_id': request_id}

        argv = ['--home-project', 'default', '--home-session', self.home_id,
            '--host-id', self.host, 'managed-native-read', '--target-project-id', self.project_id,
            '--po', self.po, '--target', self.worker, '--expected-thread', self.worker_thread,
            '--expected-host', self.host, '--stop-id', self.stop_id,
            '--admission-request-id', self.admission_id, '--message-id', self.task_message_id,
            '--job-id', job_id, '--inspection-request-id', self.inspect_id,
            '--request-id', request_id, '--confirm']
        output = io.StringIO()
        config = SimpleNamespace(master=self.home_path.resolve(), plist=self.directory / 'server.plist')
        Path(str(self.home_path.resolve()) + '.projects.sqlite3').touch()
        with mock.patch('agent_chat.root_local._installed_server_config', return_value=config), \
             mock.patch('agent_chat.root_local._preflight', return_value=(self.home_path.resolve(), True)), \
             mock.patch('agent_chat.root_local._target', return_value=self.project_path), \
             mock.patch('agent_chat.projects.Projects', FakeProjects), \
             mock.patch('agent_chat.root_orchestrator.RootOrchestrator', FakeRoots), \
             contextlib.redirect_stdout(output):
            self.assertEqual(root_local.main(argv), 0)
        self.assertEqual(captured['args'], ('default', self.home_id, self.host, self.project_id,
            self.po, self.worker, self.worker_thread, self.host, self.stop_id, self.admission_id,
            self.task_message_id, job_id, self.inspect_id, request_id))
        self.assertIn('"status": "completed"', output.getvalue())

    def test_worker_identity_uses_exact_admitted_target_not_free_form_label(self):
        target = 'assigned-target'
        target_record = {'agent': 'reviewer_group'}
        for label in ('reviewer_group', 'gameplay', 'worker'):
            with self.subTest(label=label):
                target_record['agent'] = label
                self.assertTrue(root_orchestrator._managed_native_target_is_worker(
                    target, target_record, project_root='project-root',
                    home_root='home-root', po_session='current-po', task_scope_matches=True,
                    binding_matches=True, host_matches=True))
        for protected in ('project-root', 'home-root', 'current-po'):
            with self.subTest(protected=protected):
                self.assertFalse(root_orchestrator._managed_native_target_is_worker(
                    protected, {'agent': 'worker'}, project_root='project-root',
                    home_root='home-root', po_session='current-po', task_scope_matches=True,
                    binding_matches=True, host_matches=True))
        self.assertFalse(root_orchestrator._managed_native_target_is_worker(
            target, None, project_root='project-root', home_root='home-root',
            po_session='current-po', task_scope_matches=True, binding_matches=True,
            host_matches=True))
        for task_scope, binding, host in ((False, True, True), (True, False, True), (True, True, False)):
            with self.subTest(task_scope=task_scope, binding=binding, host=host):
                self.assertFalse(root_orchestrator._managed_native_target_is_worker(
                    target, {'agent': 'worker'}, project_root='project-root',
                    home_root='home-root', po_session='current-po',
                    task_scope_matches=task_scope, binding_matches=binding, host_matches=host))

    def test_typed_diagnosis_shares_foreground_guard_without_effects(self):
        self._prepare_project_root_projection()
        job_id = 'wake_83a1ea300d2144d78ebd01b428d47cd8'
        now = time.time()
        self.project.db.execute('INSERT INTO bridge_jobs VALUES(?,?,?,?,?,?,?,?)',
            (job_id, self.worker_thread, '{"text":"PRIVATE TASK BODY"}', 'dispatched', None, None, now, now))
        self.project.db.execute('INSERT INTO bridge_deliveries VALUES(?,?)', (self.task_message_id, job_id))
        registry_path = Path(str(self.home_path) + '.projects.sqlite3')
        RootOrchestrator(ProjectStores(registry_path, self.home_path, self.project_id))
        with contextlib.closing(Coordinator(registry_path)) as registry:
            registry.db.execute('CREATE TABLE IF NOT EXISTS projects(id TEXT PRIMARY KEY)')
            registry.db.execute('INSERT OR REPLACE INTO projects(id) VALUES(?)', ('default',))
            registry.db.execute('INSERT OR REPLACE INTO projects(id) VALUES(?)', (self.project_id,))
            registry.db.execute('''INSERT OR REPLACE INTO root_config
                (singleton,home_project,home_session,thread_id,host_id,cwd,agent,created_at,mode)
                VALUES(1,?,?,?,?,?,?,?,?)''',
                ('default', self.home_id, self.home_thread, self.host, str(self.directory), 'root', now, 'running'))
            registry.db.execute('INSERT OR REPLACE INTO root_directory_owner VALUES(1,?,?)', ('default', self.home_id))
            registry.db.execute('''INSERT OR REPLACE INTO root_projects
                (project_id,session_id,joined,orchestrator_session) VALUES(?,?,1,?)''',
                (self.project_id, self.project_root, self.po))
            registry.db.execute('INSERT OR REPLACE INTO root_team_operations VALUES(?,?,?,NULL)',
                (self.stop_id, self.home_id, now))
            registry.db.execute('INSERT OR REPLACE INTO root_team_operation_projects VALUES(?,?,?,NULL)',
                (self.stop_id, self.project_id, self.project_root))
        plist, wrapper, executable = (self.directory / name for name in ('server.plist', 'wrapper', 'server'))
        for path in (plist, wrapper, executable): path.touch()
        config = SimpleNamespace(master=self.home_path.resolve(), plist=plist, wrapper=wrapper, executable=executable)
        argv = ['--home-project', 'default', '--home-session', self.home_id,
            '--host-id', self.host, 'managed-native-diagnose', '--target-project-id', self.project_id,
            '--po', self.po, '--target', self.worker, '--expected-thread', self.worker_thread,
            '--expected-host', self.host, '--stop-id', self.stop_id,
            '--admission-request-id', self.admission_id, '--message-id', self.task_message_id,
            '--job-id', job_id, '--inspection-request-id', self.inspect_id,
            '--request-id', str(uuid.uuid4())]
        rpc_calls = []
        def no_rpc(*args, **kwargs):
            rpc_calls.append((args, kwargs))
            raise AssertionError('diagnosis must not construct RpcClient')
        project_values_before = tuple(self.project.db.execute(
            'SELECT status,queue_id FROM bridge_jobs WHERE id=?', (job_id,)).fetchone())
        effect_tables = ('bridge_jobs', 'bridge_deliveries', 'agent_control_requests')
        effects_before = {table: tuple(tuple(row) for row in self.project.db.execute(
            'SELECT * FROM ' + table + ' ORDER BY 1')) for table in effect_tables}
        controls_before = [tuple(row) for row in self.project.db.execute(
            'SELECT id,status FROM agent_control_requests ORDER BY id')]
        audit_before = self.project.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_managed_native_reads'").fetchone()
        text = ''
        for label in ('reviewer_group', 'gameplay', 'worker'):
            self.project.db.execute('UPDATE sessions SET agent=? WHERE id=?', (label, self.worker))
            output = io.StringIO()
            with mock.patch('agent_chat.root_local._installed_server_config', return_value=config), \
                 mock.patch('agent_chat.root_local.configured_server_database', return_value=self.home_path.resolve()), \
                 mock.patch('agent_chat.projects.Projects', side_effect=AssertionError('broad constructor used')), \
                 mock.patch.object(RootOrchestrator, '__init__', side_effect=AssertionError('broad constructor used')), \
                 mock.patch('agent_chat.rpc.RpcClient', side_effect=no_rpc), \
                 contextlib.redirect_stdout(output):
                self.assertEqual(root_local.main(argv), 0)
            text = output.getvalue()
            with self.subTest(admitted_label=label):
                self.assertIn('eligible_for_separate_review', text)
                self.assertIn('"worker_role_is_worker": "true"', text)
        self.assertIn('eligible_for_separate_review', text)
        expected_checks = (
            'project_schema_matches','actor_matches_request','task_association_present',
            'project_root_is_current','project_root_binding_matches','project_root_host_matches',
            'actor_is_authorized_root_or_admitted_po','po_role_matches_root','po_binding_matches_admission',
            'po_host_matches_admission','worker_role_is_worker','worker_binding_matches_request',
            'worker_host_matches_request','task_scope_matches_request','only_expected_active_stop',
            'stop_matches_project_root','stop_is_active','target_admission_matches_request',
            'target_admission_requester_matches_root','target_admission_status_admissible',
            'po_admission_matches_current_binding','message_sender_recipient_matches',
            'inspection_request_matches_target_thread_host','inspection_requester_is_root_or_admitted_po',
            'inspection_request_status_admissible','original_wake_is_unique_exact_delivery',
            'original_wake_not_cancelled','message_not_batched','no_other_pending_control',
            'no_pending_reset','worker_not_quarantined')
        for check in expected_checks:
            self.assertIn('"' + check + '": "true"', text)
        self.assertIn('"original_wake_is_unique_exact_delivery": "true"', text)
        self.assertNotIn('PRIVATE TASK BODY', text)
        self.assertEqual(rpc_calls, [])
        self.assertEqual(tuple(self.project.db.execute(
            'SELECT status,queue_id FROM bridge_jobs WHERE id=?', (job_id,)).fetchone()), project_values_before)
        self.assertEqual([tuple(row) for row in self.project.db.execute(
            'SELECT id,status FROM agent_control_requests ORDER BY id')], controls_before)
        self.assertEqual(self.project.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_managed_native_reads'").fetchone(), audit_before)
        self.assertEqual({table: tuple(tuple(row) for row in self.project.db.execute(
            'SELECT * FROM ' + table + ' ORDER BY 1')) for table in effect_tables}, effects_before)

        self.project.db.execute('UPDATE bridge_bindings SET thread_id=? WHERE session_id=?',
                                (str(uuid.uuid4()), self.worker))
        blocked = io.StringIO()
        with mock.patch('agent_chat.root_local._installed_server_config', return_value=config), \
             mock.patch('agent_chat.root_local.configured_server_database', return_value=self.home_path.resolve()), \
             mock.patch('agent_chat.projects.Projects', side_effect=AssertionError('broad constructor used')), \
             mock.patch.object(RootOrchestrator, '__init__', side_effect=AssertionError('broad constructor used')), \
             mock.patch('agent_chat.rpc.RpcClient', side_effect=no_rpc), \
             contextlib.redirect_stdout(blocked):
            self.assertEqual(root_local.main(argv), 0)
        self.assertIn('"verdict": "blocked"', blocked.getvalue())
        self.assertIn('"worker_binding_matches_request": "false"', blocked.getvalue())
        self.assertNotIn(self.worker, blocked.getvalue())
        self.assertEqual(rpc_calls, [])
        self.assertEqual(self.project.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_managed_native_reads'").fetchone(), audit_before)

        self.project.db.execute('UPDATE bridge_bindings SET thread_id=? WHERE session_id=?',
                                (self.worker_thread, self.worker))
        self.project.db.execute('DELETE FROM remote_session_hosts WHERE session_id=?', (self.worker,))
        absent = io.StringIO()
        with mock.patch('agent_chat.root_local._installed_server_config', return_value=config), \
             mock.patch('agent_chat.root_local.configured_server_database', return_value=self.home_path.resolve()), \
             mock.patch('agent_chat.projects.Projects', side_effect=AssertionError('broad constructor used')), \
             mock.patch.object(RootOrchestrator, '__init__', side_effect=AssertionError('broad constructor used')), \
             mock.patch('agent_chat.rpc.RpcClient', side_effect=no_rpc), \
             contextlib.redirect_stdout(absent):
            self.assertEqual(root_local.main(argv), 0)
        self.assertIn('"verdict": "unknown"', absent.getvalue())
        self.assertIn('"worker_host_matches_request": "unknown"', absent.getvalue())
        self.assertNotIn('"worker_host_matches_request": "false"', absent.getvalue())
        self.assertNotIn(self.worker, absent.getvalue())
        self.assertEqual(rpc_calls, [])
        self.assertEqual({table: tuple(tuple(row) for row in self.project.db.execute(
            'SELECT * FROM ' + table + ' ORDER BY 1')) for table in effect_tables}, effects_before)
        self.assertEqual(self.project.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_managed_native_reads'").fetchone(), audit_before)
        self.project.db.execute('INSERT INTO remote_session_hosts(session_id,host_id,registered_at,last_seen_at) VALUES(?,?,?,?)',
                                (self.worker, 'known-wrong-host', time.time(), time.time()))
        mismatch = io.StringIO()
        with mock.patch('agent_chat.root_local._installed_server_config', return_value=config), \
             mock.patch('agent_chat.root_local.configured_server_database', return_value=self.home_path.resolve()), \
             mock.patch('agent_chat.projects.Projects', side_effect=AssertionError('broad constructor used')), \
             mock.patch.object(RootOrchestrator, '__init__', side_effect=AssertionError('broad constructor used')), \
             mock.patch('agent_chat.rpc.RpcClient', side_effect=no_rpc), \
             contextlib.redirect_stdout(mismatch):
            self.assertEqual(root_local.main(argv), 0)
        self.assertIn('"verdict": "blocked"', mismatch.getvalue())
        self.assertIn('"worker_host_matches_request": "false"', mismatch.getvalue())
        self.assertEqual(rpc_calls, [])
        self.assertEqual({table: tuple(tuple(row) for row in self.project.db.execute(
            'SELECT * FROM ' + table + ' ORDER BY 1')) for table in effect_tables}, effects_before)
        self.assertEqual(self.project.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_managed_native_reads'").fetchone(), audit_before)

        self.project.db.execute('UPDATE remote_session_hosts SET host_id=? WHERE session_id=?', (self.host, self.worker))
        original_readiness_db = RootOrchestrator._readiness_db
        target_reads = {'count': 0}
        @contextlib.contextmanager
        def unreadable_scope_read(path):
            if Path(path).resolve() == self.project_path.resolve():
                target_reads['count'] += 1
                if target_reads['count'] == 2:
                    raise sqlite3.OperationalError('isolated fixture unreadable metadata')
            with original_readiness_db(path) as db:
                yield db
        unavailable = io.StringIO()
        with mock.patch.object(RootOrchestrator, '_readiness_db', staticmethod(unreadable_scope_read)), \
             mock.patch('agent_chat.root_local._installed_server_config', return_value=config), \
             mock.patch('agent_chat.root_local.configured_server_database', return_value=self.home_path.resolve()), \
             mock.patch('agent_chat.projects.Projects', side_effect=AssertionError('broad constructor used')), \
             mock.patch.object(RootOrchestrator, '__init__', side_effect=AssertionError('broad constructor used')), \
             mock.patch('agent_chat.rpc.RpcClient', side_effect=no_rpc), \
             contextlib.redirect_stdout(unavailable):
            self.assertEqual(root_local.main(argv), 0)
        self.assertIn('"verdict": "unknown"', unavailable.getvalue())
        self.assertIn('"worker_binding_matches_request": "unknown"', unavailable.getvalue())
        self.assertNotIn(self.worker, unavailable.getvalue())
        self.assertEqual(rpc_calls, [])
        self.assertEqual({table: tuple(tuple(row) for row in self.project.db.execute(
            'SELECT * FROM ' + table + ' ORDER BY 1')) for table in effect_tables}, effects_before)
        self.assertEqual(self.project.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_managed_native_reads'").fetchone(), audit_before)

        project_identity = self.project.db.execute("SELECT value FROM meta WHERE key='project_id'").fetchone()[0]
        original_scope = root_orchestrator._managed_native_scope
        def project_identity_disappears(db, row, *, actor_session, diagnostic=False):
            with contextlib.closing(Coordinator(self.project_path)) as writer:
                writer.db.execute("DELETE FROM meta WHERE key='project_id'")
            return original_scope(db, row, actor_session=actor_session, diagnostic=diagnostic)
        identity_missing = io.StringIO()
        with mock.patch('agent_chat.root_orchestrator._managed_native_scope', side_effect=project_identity_disappears), \
             mock.patch('agent_chat.root_local._installed_server_config', return_value=config), \
             mock.patch('agent_chat.root_local.configured_server_database', return_value=self.home_path.resolve()), \
             mock.patch('agent_chat.projects.Projects', side_effect=AssertionError('broad constructor used')), \
             mock.patch.object(RootOrchestrator, '__init__', side_effect=AssertionError('broad constructor used')), \
             mock.patch('agent_chat.rpc.RpcClient', side_effect=no_rpc), \
             contextlib.redirect_stdout(identity_missing):
            self.assertEqual(root_local.main(argv), 0)
        self.assertIn('"verdict": "unknown"', identity_missing.getvalue())
        self.assertIn('"project_schema_matches": "unknown"', identity_missing.getvalue())
        self.assertNotIn(self.worker, identity_missing.getvalue())
        self.assertEqual(rpc_calls, [])
        self.assertEqual({table: tuple(tuple(row) for row in self.project.db.execute(
            'SELECT * FROM ' + table + ' ORDER BY 1')) for table in effect_tables}, effects_before)
        self.project.db.execute("INSERT INTO meta(key,value) VALUES('project_id',?)", (project_identity,))

        # Simulate an association disappearing after the outer exact-context
        # read and before the shared predicate evaluator opens its snapshot.
        # This is test-only corruption of the temporary fixture; production
        # callers never drop the immutable-admission trigger.
        target_reads = {'count': 0}
        original_readiness_db = RootOrchestrator._readiness_db
        @contextlib.contextmanager
        def task_disappears_after_context(path):
            with original_readiness_db(path) as db:
                yield db
            if Path(path).resolve() == self.project_path.resolve():
                target_reads['count'] += 1
                if target_reads['count'] == 1:
                    with contextlib.closing(Coordinator(self.project_path)) as writer:
                        writer.db.execute('DROP TRIGGER root_team_stop_task_admissions_no_delete')
                        writer.db.execute('DELETE FROM root_team_stop_task_admissions WHERE request_id=?',
                                          (self.admission_id,))
        task_race = io.StringIO()
        with mock.patch.object(RootOrchestrator, '_readiness_db', staticmethod(task_disappears_after_context)), \
             mock.patch('agent_chat.root_local._installed_server_config', return_value=config), \
             mock.patch('agent_chat.root_local.configured_server_database', return_value=self.home_path.resolve()), \
             mock.patch('agent_chat.projects.Projects', side_effect=AssertionError('broad constructor used')), \
             mock.patch.object(RootOrchestrator, '__init__', side_effect=AssertionError('broad constructor used')), \
             mock.patch('agent_chat.rpc.RpcClient', side_effect=no_rpc), \
             contextlib.redirect_stdout(task_race):
            self.assertEqual(root_local.main(argv), 0)
        self.assertIn('"verdict": "unknown"', task_race.getvalue())
        self.assertIn('"task_association_present": "unknown"', task_race.getvalue())
        self.assertNotIn(self.worker, task_race.getvalue())
        self.assertEqual(rpc_calls, [])
        self.assertEqual({table: tuple(tuple(row) for row in self.project.db.execute(
            'SELECT * FROM ' + table + ' ORDER BY 1')) for table in effect_tables}, effects_before)
        self.assertEqual(self.project.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_managed_native_reads'").fetchone(), audit_before)

        foreign = list(argv)
        foreign[foreign.index('--target-project-id') + 1] = 'foreign-project'
        with mock.patch('agent_chat.root_local._installed_server_config', return_value=config), \
             mock.patch('agent_chat.root_local.configured_server_database', return_value=self.home_path.resolve()), \
             mock.patch('agent_chat.projects.Projects', side_effect=AssertionError('broad constructor used')), \
             mock.patch.object(RootOrchestrator, '__init__', side_effect=AssertionError('broad constructor used')), \
             contextlib.redirect_stdout(io.StringIO()), self.assertRaises(CoordError):
            root_local.main(foreign)
        self.assertEqual(rpc_calls, [])

    def test_typed_root_local_composes_real_orchestrator_with_isolated_provider(self):
        registry_path = Path(str(self.home_path) + '.projects.sqlite3')
        registry_coord = Coordinator(registry_path)
        try:
            # Initialize only fixture schemas; the real local route below must
            # not invoke either broad constructor or AgentControlState migration.
            RootOrchestrator(ProjectStores(registry_path, self.home_path, self.project_id))
            registry_coord.db.execute('CREATE TABLE IF NOT EXISTS projects(id TEXT PRIMARY KEY)')
            registry_coord.db.execute('INSERT OR REPLACE INTO projects(id) VALUES(?)', ('default',))
            registry_coord.db.execute('INSERT OR REPLACE INTO projects(id) VALUES(?)', (self.project_id,))
            registry_coord.db.execute('''INSERT OR REPLACE INTO root_config
                (singleton,home_project,home_session,thread_id,host_id,cwd,agent,created_at,mode)
                VALUES(1,?,?,?,?,?,?,?,?)''',
                ('default', self.home_id, self.home_thread, self.host, str(self.directory),
                 'root', time.time(), 'running'))
            registry_coord.db.execute('INSERT OR REPLACE INTO root_directory_owner VALUES(1,?,?)',
                                      ('default', self.home_id))
            registry_coord.db.execute('''INSERT OR REPLACE INTO root_projects
                (project_id,session_id,joined,orchestrator_session) VALUES(?,?,1,?)''',
                (self.project_id, self.project_root, self.po))
        finally:
            registry_coord.close()
        self._prepare_project_root_projection()
        self.assertIsNone(self.project.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_managed_native_reads'").fetchone())
        job_id, request_id = 'wake_83a1ea300d2144d78ebd01b428d47cd8', str(uuid.uuid4())
        now = time.time()
        self.project.db.execute('INSERT INTO bridge_jobs VALUES(?,?,?,?,?,?,?,?)',
            (job_id, self.worker_thread, '{"text":"PRIVATE TASK"}', 'dispatched', None, None, now, now))
        self.project.db.execute('INSERT INTO bridge_deliveries VALUES(?,?)', (self.task_message_id, job_id))
        controls_before = [item['id'] for item in AgentControlState(self.project).jobs(self.host)]
        calls = []
        class FakeRpc:
            drift_registry = False
            def __enter__(inner): return inner
            def __exit__(inner, *_): return None
            def request(inner, method, params):
                calls.append((method, dict(params)))
                if method == 'thread/read':
                    if inner.drift_registry:
                        with contextlib.closing(Coordinator(registry_path)) as registry:
                            registry.db.execute('UPDATE root_config SET thread_id=? WHERE singleton=1',
                                                (str(uuid.uuid4()),))
                    return {'thread': {'id': params['threadId'], 'status': {'type': 'notLoaded'},
                                       'canAcceptDirectInput': True, 'turns': [{'text': 'private'}]}}
                raise AssertionError('notLoaded thread must not reach queue/list')
        argv = ['--home-project', 'default', '--home-session', self.home_id,
            '--host-id', self.host, 'managed-native-read', '--target-project-id', self.project_id,
            '--po', self.po, '--target', self.worker, '--expected-thread', self.worker_thread,
            '--expected-host', self.host, '--stop-id', self.stop_id,
            '--admission-request-id', self.admission_id, '--message-id', self.task_message_id,
            '--job-id', job_id, '--inspection-request-id', self.inspect_id,
            '--request-id', request_id, '--confirm']
        output = io.StringIO()
        plist, wrapper, executable = (self.directory / name for name in ('server.plist', 'wrapper', 'server'))
        for path in (plist, wrapper, executable): path.touch()
        config = SimpleNamespace(master=self.home_path.resolve(), plist=plist, wrapper=wrapper, executable=executable)
        self.home.db.execute('DROP TABLE root_config')
        self.assertIsNone(self.home.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_config'").fetchone())
        rpc = FakeRpc()
        with mock.patch('agent_chat.root_local._installed_server_config', return_value=config), \
             mock.patch('agent_chat.root_local.configured_server_database', return_value=self.home_path.resolve()), \
             mock.patch('agent_chat.projects.Projects', side_effect=AssertionError('broad project constructor used')), \
             mock.patch.object(RootOrchestrator, '__init__', side_effect=AssertionError('broad root constructor used')), \
             mock.patch('agent_chat.rpc.RpcClient', return_value=rpc), \
             mock.patch.dict('os.environ', {'AGENT_CHAT_CODEX_SERVER': 'ws://127.0.0.1:4500'}):
            bad_argv = list(argv)
            bad_argv[bad_argv.index('--expected-host') + 1] = 'host-wrong'
            with contextlib.redirect_stdout(io.StringIO()), self.assertRaisesRegex(CoordError, 'ownership, stop, task or original wake'):
                root_local.main(bad_argv)
            self.assertEqual(calls, [])
            self.assertIsNone(self.project.db.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_managed_native_reads'").fetchone())
            with contextlib.redirect_stdout(output):
                self.assertEqual(root_local.main(argv), 0)
        self.assertEqual(calls, [('thread/read', {'threadId': self.worker_thread, 'includeTurns': False})])
        self.assertIn('"status": "unavailable"', output.getvalue())
        self.assertIn('"direct_input": null', output.getvalue())
        replay_output = io.StringIO()
        with mock.patch('agent_chat.root_local._installed_server_config', return_value=config), \
             mock.patch('agent_chat.root_local.configured_server_database', return_value=self.home_path.resolve()), \
             mock.patch('agent_chat.projects.Projects', side_effect=AssertionError('broad project constructor used')), \
             mock.patch.object(RootOrchestrator, '__init__', side_effect=AssertionError('broad root constructor used')), \
             mock.patch('agent_chat.rpc.RpcClient', return_value=rpc), \
             mock.patch.dict('os.environ', {'AGENT_CHAT_CODEX_SERVER': 'ws://127.0.0.1:4500'}), \
             contextlib.redirect_stdout(replay_output):
            self.assertEqual(root_local.main(argv), 0)
        self.assertEqual(len(calls), 1)
        self.assertIn('"status": "unavailable"', replay_output.getvalue())
        drift_argv = list(argv)
        drift_request_id = str(uuid.uuid4())
        drift_argv[drift_argv.index('--request-id') + 1] = drift_request_id
        rpc.drift_registry = True
        drift_output = io.StringIO()
        with mock.patch('agent_chat.root_local._installed_server_config', return_value=config), \
             mock.patch('agent_chat.root_local.configured_server_database', return_value=self.home_path.resolve()), \
             mock.patch('agent_chat.projects.Projects', side_effect=AssertionError('broad project constructor used')), \
             mock.patch.object(RootOrchestrator, '__init__', side_effect=AssertionError('broad root constructor used')), \
             mock.patch('agent_chat.rpc.RpcClient', return_value=rpc), \
             mock.patch.dict('os.environ', {'AGENT_CHAT_CODEX_SERVER': 'ws://127.0.0.1:4500'}), \
             contextlib.redirect_stdout(drift_output):
            self.assertEqual(root_local.main(drift_argv), 0)
        self.assertIn('"status": "unknown"', drift_output.getvalue())
        self.assertIn('"error_code": "scope_changed"', drift_output.getvalue())
        self.assertEqual(len(calls), 2)
        self.assertNotIn('PRIVATE TASK', output.getvalue())
        self.assertEqual(self.project.db.execute(
            'SELECT COUNT(*) FROM agent_control_requests WHERE id=?', (request_id,)).fetchone()[0], 0)
        self.assertEqual([item['id'] for item in AgentControlState(self.project).jobs(self.host)], controls_before)
        bridge_job = self.project.db.execute('SELECT status,queue_id FROM bridge_jobs WHERE id=?',
                                             (job_id,)).fetchone()
        self.assertEqual(tuple(bridge_job), ('dispatched', None))
