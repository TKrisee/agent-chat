"""Private installed-custodian workflow; no real host or production store read."""
import contextlib
import hashlib
import os
import io
import json
import plistlib
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sqlite3
import time
import unittest
import uuid
from unittest import mock

from agent_chat import original_owner_disposition as disposition
from agent_chat.core import Coordinator, CoordError, assert_agent_running
import test_custodian_recovery as custodian_tests
from agent_chat import cli, custodian_recovery as admin, service
from agent_chat.lifecycle_producer import main as enroll
from agent_chat.private_artifacts import atomic_capture
from agent_chat.remote import HttpClient, RemoteCoordError
from test_remote_web import TOKEN
from test_remote_web import RemoteWebFixture


class OriginalOwnerDispositionTests(RemoteWebFixture):
    sql = custodian_tests.CustodianRecoveryTests.sql
    state = custodian_tests.CustodianRecoveryTests.state

    def setUp(self):
        # Existing sanctioned fixture establishes a real isolated guarded /true
        # execution with enrollment before launch, lost close transport, stale
        # reservation and fixed temporary installed provider. No role/auth seed.
        super().setUp()
        self.call('register', agent='fixture-original-owner')
        self.claim = self.call('request', resource='shared', minutes=1)
        producer = Path(self.tmp.name) / 'private-producer'
        with contextlib.redirect_stdout(io.StringIO()):
            enroll(['--db', str(self.db), '--owner', 'a', '--host', 'mac', '--capability-file', str(producer)])
        cap = Path(self.tmp.name) / 'original-capability'
        atomic_capture(cap,dict(resource='shared',session_id='a',host_id='mac',reservation_id=self.claim['reservation_id'],token=self.claim['token'],server=self.url,project='default'))
        env = dict(os.environ,AGENT_CHAT_SERVER=self.url,AGENT_CHAT_API_TOKEN=TOKEN,AGENT_CHAT_SESSION='a',AGENT_CHAT_HOST_ID='mac',AGENT_CHAT_TOKEN=self.claim['token'],AGENT_CHAT_PRODUCER_CAPABILITY_FILE=str(producer),AGENT_CHAT_STATE_DIR=str(Path(self.tmp.name)/'client'))
        original = HttpClient.call
        def disconnect(client,path,payload,**kw):
            if payload.get('op')=='close-guard': raise RemoteCoordError('fixture transport loss')
            return original(client,path,payload,**kw)
        with mock.patch.dict(os.environ,env,clear=True),mock.patch.object(HttpClient,'call',disconnect):
            with self.assertRaises(RemoteCoordError):
                cli.main(['run','--reservation-id',self.claim['reservation_id'],'--capability-file',str(cap),'shared','--','/usr/bin/true'])
        cap.unlink()
        self.claim.pop('token')
        self.sql("UPDATE resources SET deadline=?,stale=1 WHERE name='shared'",time.time()-1)
        self.run = self.sql('SELECT run_id FROM remote_guarded_runs')[0][0]
        root = Path(self.tmp.name).resolve()
        provider = root/'fixed-provider';provider.mkdir(mode=0o700)
        work = root/'work';work.mkdir(mode=0o700)
        self.cfg = service.InstalledServerConfig(plist=provider/'server.plist',wrapper=provider/'wrapper',executable=provider/'server-executable',master=self.db.resolve(),working_directory=work,uid=os.geteuid(),host='127.0.0.1',port=8765)
        self.cfg.executable.write_text('#!/bin/sh\nexit 0\n');self.cfg.executable.chmod(0o700)
        self.cfg.wrapper.write_text('exec '+str(self.cfg.executable)+' --db '+str(self.cfg.master)+' --host 127.0.0.1 --port 8765\n');self.cfg.wrapper.chmod(0o700)
        self.cfg.plist.write_bytes(plistlib.dumps(dict(Label='com.agent-chat.server',AssociatedBundleIdentifiers=['com.agent-chat.server'],WorkingDirectory=str(work),RunAtLoad=True,KeepAlive=True,ThrottleInterval=10,ProgramArguments=[str(self.cfg.wrapper)],EnvironmentVariables={})));self.cfg.plist.chmod(0o600)
        self.cfg.master.chmod(0o600)
        for suffix in ('-wal','-shm','-journal'):
            sidecar=Path(str(self.cfg.master)+suffix)
            if sidecar.exists():sidecar.chmod(0o600)
        self.provider_patch=mock.patch.object(service,'_installed_server_config',return_value=self.cfg);self.provider_patch.start();self.addCleanup(self.provider_patch.stop)
        self.target_patch=mock.patch.object(admin,'TARGET',dict(project='default',resource='shared',reservation_id=self.claim['reservation_id'],run_id=self.run));self.target_patch.start();self.addCleanup(self.target_patch.stop)
        self.thread_id = str(uuid.uuid4())
        self.call('bind', thread=self.thread_id)
        self.fixed = dict(project='default', resource='shared',
            reservation_id=self.claim['reservation_id'], run_id=self.run,
            owner_session='a', thread_id=self.thread_id)
        patcher = mock.patch.object(disposition, 'TARGET', self.fixed)
        patcher.start(); self.addCleanup(patcher.stop)
        self.request_id = str(uuid.uuid4())
        self.reference = 'synthetic-original-owner-approved-failed-quarantine'

    def execute(self, operation, **kwargs):
        with mock.patch.dict(os.environ, {}, clear=True):
            return disposition.execute(operation, **kwargs)

    def quarantine(self, **kwargs):
        pin = self.execute('preflight')['prestate_sha256']
        values = dict(request_id=self.request_id, expected_prestate=pin,
                      authority_reference=self.reference)
        values.update(kwargs)
        return self.execute('quarantine-failed', **values), values

    def obligations(self):
        with contextlib.closing(sqlite3.connect(self.db)) as db:
            return {table: db.execute('SELECT * FROM '+table+' ORDER BY 1').fetchall()
                for table in ('resources', 'resource_queue', 'remote_guarded_runs',
                              'remote_reservation_outcomes', 'receipts')}

    def test_atomic_quarantine_preserves_open_hold_fifo_history_and_cleanup(self):
        self.call('register', session='world', agent='world')
        self.call('request', session='world', resource='shared', minutes=1)
        before = self.obligations()
        result, values = self.quarantine()
        self.assertEqual(self.obligations(), before)
        self.assertEqual(result['outcome'], 'failed_abandoned_quarantine')
        self.assertEqual(result['original_guard'], 'OPEN')
        self.assertEqual(result['restoration'], 'UNKNOWN')
        self.assertEqual(result['already_queued_or_racing_effects'], 'UNKNOWN')
        self.assertFalse(result['resources_returned'])
        self.assertFalse(result['receipt_created'])
        self.assertEqual(self.execute('quarantine-failed', **values), result)
        self.assertEqual(self.obligations(), before)
        with contextlib.closing(Coordinator(self.db, 'a')) as coord:
            with self.assertRaisesRegex(CoordError, 'quarantine is active'):
                assert_agent_running(coord.db, 'a')
            assert_agent_running(coord.db, 'a', cleanup=True)
        with self.assertRaisesRegex(CoordError, 'conflicts'):
            self.execute('quarantine-failed', **dict(values, authority_reference='different'))
        with contextlib.closing(sqlite3.connect(self.db)) as db:
            for action in ('UPDATE original_owner_dispositions SET recorded_at=0',
                           'DELETE FROM original_owner_dispositions'):
                with self.assertRaisesRegex(sqlite3.IntegrityError, 'immutable'):
                    db.execute(action)
        self.assertEqual(self.obligations(), before)

    def test_fixed_custodian_foreign_account_and_changed_target_refuse_without_writes(self):
        before = self.state()
        with mock.patch('agent_chat.custodian_recovery.os.geteuid', return_value=os.geteuid()+1):
            with self.assertRaises(CoordError): self.execute('preflight')
        self.assertEqual(self.state(), before)
        with mock.patch.object(disposition, 'TARGET', dict(self.fixed, owner_session='foreign')):
            with self.assertRaisesRegex(CoordError, 'association'):
                self.execute('preflight')
        self.assertEqual(self.state(), before)
        with mock.patch.object(disposition, 'TARGET', dict(self.fixed, thread_id=str(uuid.uuid4()))):
            with self.assertRaisesRegex(CoordError, 'association'):
                self.execute('preflight')
        self.assertEqual(self.state(), before)

    def test_generation_and_binding_drift_refuse(self):
        pin = self.execute('preflight')['prestate_sha256']
        self.sql('UPDATE resources SET reason=? WHERE name=?', 'new actual OPEN fact', 'shared')
        before = self.state()
        with self.assertRaisesRegex(CoordError, 'prestate drift'):
            self.execute('quarantine-failed', request_id=self.request_id,
                expected_prestate=pin, authority_reference=self.reference)
        self.assertEqual(self.state(), before)
        result, values = self.quarantine()
        self.sql('UPDATE bridge_bindings SET thread_id=? WHERE session_id=?', str(uuid.uuid4()), 'a')
        before = self.state()
        with self.assertRaisesRegex(CoordError, 'association'):
            self.execute('quarantine-failed', **values)
        self.assertEqual(self.state(), before)

    def test_return_preflight_readonly_and_never_releases_for_enrolled_or_legacy_case(self):
        before = self.state()
        result = self.execute('return-preflight')
        self.assertTrue(result['trusted_original_closure_available'])
        self.assertFalse(result['eligible'])
        self.assertFalse(result['physical_return_supported'])
        self.assertEqual(self.state(), before)

    def test_absent_fence_preserves_normal_admission_and_no_return_verb(self):
        with contextlib.closing(Coordinator(self.db, 'a')) as coord:
            assert_agent_running(coord.db, 'a')
        before = self.state()
        with self.assertRaisesRegex(CoordError, 'no physical return'):
            self.execute('return')
        self.assertEqual(self.state(), before)

    def test_genuine_unenrolled_live_fixture_remains_unknown_and_all_holds_stay(self):
        # Fresh synthetic authorization; never retrieve/reconstruct an old token.
        claim = self.call('request', resource='fixture-unenrolled', minutes=1)
        run = self.call('begin-guard', resource='fixture-unenrolled', token=claim['token'])
        process = subprocess.Popen(['/bin/sleep', '30'])
        self.addCleanup(lambda: (process.terminate(), process.wait(timeout=5)) if process.poll() is None else None)
        self.call('attach-guard-pid', run_id=run['run_id'], token=claim['token'], pid=process.pid)
        for index in range(5):
            self.call('request', resource='fixture-hold-'+str(index), minutes=1)
        self.call('register', session='world', agent='world')
        for resource in ['shared', 'fixture-unenrolled'] + ['fixture-hold-'+str(i) for i in range(5)]:
            self.call('request', session='world', resource=resource, minutes=1)
        self.sql('UPDATE resources SET deadline=?,stale=1 WHERE name=?', time.time()-1, 'fixture-unenrolled')
        with mock.patch.object(disposition, 'TARGET', dict(self.fixed, resource='fixture-unenrolled',
                reservation_id=claim['reservation_id'], run_id=run['run_id'])):
            result = self.execute('return-preflight')
            self.assertFalse(result['trusted_original_closure_available'])
            self.assertFalse(result['eligible'])
            before = self.obligations()
            pin = self.execute('preflight')['prestate_sha256']
            quarantined = self.execute('quarantine-failed', request_id=self.request_id,
                expected_prestate=pin, authority_reference=self.reference)
            self.assertEqual(quarantined['process_state'], 'UNKNOWN')
            self.assertEqual(self.obligations(), before)
            self.assertIsNone(process.poll())  # Quarantine is not a signal/closure.

    def test_stopped_retired_reset_and_failed_leaf_states_are_not_rewritten(self):
        from agent_chat.agent_control import AgentControlState
        with contextlib.closing(Coordinator(self.db)) as coord:
            AgentControlState(coord)
        self.sql("INSERT INTO agent_controls VALUES('a','stopping',0)")
        before = self.state()
        with self.assertRaisesRegex(CoordError, 'admission'):
            self.execute('preflight')
        self.assertEqual(self.state(), before)
        self.sql("UPDATE agent_controls SET mode='running'")
        self.sql("INSERT INTO session_resets(id,requester_session,target_session,host_id,old_thread_id,prompt,status,created_at,updated_at) VALUES('fixture-reset','a','a','mac',?,'fixture','pending',0,0)", self.thread_id)
        before = self.state()
        with self.assertRaisesRegex(CoordError, 'reset pending'):
            self.execute('preflight')
        self.assertEqual(self.state(), before)
        self.sql("UPDATE session_resets SET status='failed'")
        self.sql("INSERT INTO agent_control_requests(id,requester_session,target_session,host_id,thread_id,action,interrupt,status,created_at,updated_at) VALUES('fixture-failed-leaf','a','a','mac',?,'inspect',0,'failed',0,0)", self.thread_id)
        before = self.sql('SELECT * FROM session_resets'), self.sql('SELECT * FROM agent_control_requests')
        self.quarantine()
        self.assertEqual((self.sql('SELECT * FROM session_resets'), self.sql('SELECT * FROM agent_control_requests')), before)
        self.sql("INSERT INTO retired_sessions SELECT id,agent,registered_at,? FROM sessions WHERE id='a'", time.time())
        before = self.state()
        with self.assertRaisesRegex(CoordError, 'active registered'):
            self.execute('return-preflight')
        self.assertEqual(self.state(), before)

    def test_remote_single_batch_restore_and_serialized_new_guard_refuse(self):
        claim = self.call('request', resource='fixture-new-guard', minutes=1)
        restore_claim = self.call('request', resource='fixture-new-restore', minutes=1)
        self.sql('UPDATE resources SET deadline=?,stale=1 WHERE name=?', time.time()-1, 'fixture-new-restore')
        result, _ = self.quarantine()
        runs = self.sql('SELECT * FROM remote_guarded_runs')
        for params in (dict(resource='fixture-new-guard', token=claim['token']),
                       dict(resource='fixture-new-restore', token=restore_claim['token'], restore=True,
                            reservation_id=restore_claim['reservation_id'], max_seconds=30)):
            with self.assertRaisesRegex(RemoteCoordError, 'quarantine is active'):
                self.call('begin-guard', **params)
        with self.assertRaisesRegex(RemoteCoordError, 'quarantine is active'):
            self.call('begin-guard-batch', restore=False,
                tuples=[dict(resource='fixture-new-guard', session_id='a', host_id='mac',
                    reservation_id=claim['reservation_id'], token=claim['token'])])
        self.assertEqual(self.sql('SELECT * FROM remote_guarded_runs'), runs)
        self.assertFalse(self.execute('return-preflight')['eligible'])

    def test_model_handoff_and_state_refuse_but_stop_status_and_settlement_remain(self):
        from agent_chat.bridge_client import RemoteBridgeState
        from agent_chat.agent_control import AgentControlState
        from agent_chat.root_team_reset import handoff_schema, handoff_begin
        model_id = str(uuid.uuid4())
        job = self.call('agent-model', to='fixture-original-owner', expected_thread=self.thread_id,
            request_id=model_id, confirm=True, model='fixture-model', reasoning_effort='high')
        state = RemoteBridgeState(self.client, self.url,
            dict(owner='fixture-disposition-dispatcher', secret='q'*40, host_id='mac'))
        state.call('acquire')
        with contextlib.closing(Coordinator(self.db, 'a')) as coord:
            handoff_schema(coord.db)
        self.quarantine()
        jobs = self.sql('SELECT * FROM agent_control_requests')
        handoffs = self.sql('SELECT * FROM root_native_handoffs')
        for phase in ('changing', 'submitting'):
            with self.assertRaisesRegex(RemoteCoordError, 'quarantine is active'):
                state.call('control-update', job_id=model_id, expected='pending', status=phase)
        with contextlib.closing(Coordinator(self.db, 'a')) as coord:
            controls = AgentControlState(coord)
            with self.assertRaisesRegex(CoordError, 'quarantine is active'):
                controls.update(model_id, 'pending', 'changing')
            with self.assertRaisesRegex(CoordError, 'quarantine is active'):
                handoff_begin(coord.db, 'control', job, 'mac', 'changing')
        self.assertEqual(self.sql('SELECT * FROM agent_control_requests'), jobs)
        self.assertEqual(self.sql('SELECT * FROM root_native_handoffs'), handoffs)
        # Settlement does not retry the refused model/native operation.
        state.call('control-update', job_id=model_id, expected='pending', status='failed', error='not submitted')
        self.call('agent-status', to='fixture-original-owner')
        stop_id = str(uuid.uuid4())
        self.call('agent-stop', to='fixture-original-owner', expected_thread=self.thread_id,
                  request_id=stop_id, confirm=True, interrupt=True)
        stopped = state.call('control-update', job_id=stop_id, expected='pending',
                             status='interrupting', turn_id='fixture-observed-active-turn')
        self.assertEqual(stopped['status'], 'interrupting')
        state.call('release')

    def test_quarantine_and_native_handoff_serialize_on_target_store(self):
        from agent_chat.root_team_reset import handoff_schema, handoff_begin
        with contextlib.closing(Coordinator(self.db, 'a')) as coord:
            handoff_schema(coord.db)
        pin = self.execute('preflight')['prestate_sha256']
        installed = threading.Event(); release = threading.Event(); attempted = threading.Event()
        original = admin._same_boundary
        def pause(boundary):
            original(boundary)
            if threading.current_thread().name.startswith('quarantine'):
                installed.set()
                if not release.wait(3): raise AssertionError('fixture serialization deadline')
        def quarantine():
            with mock.patch.dict(os.environ, {}, clear=True):
                return disposition.execute('quarantine-failed', request_id=self.request_id,
                    expected_prestate=pin, authority_reference=self.reference)
        def native_admission():
            attempted.set()
            with contextlib.closing(Coordinator(self.db, 'a')) as coord:
                try:
                    handoff_begin(coord.db, 'control', dict(id='race-model', target_session='a',
                        thread_id=self.thread_id, action='model'), 'mac', 'changing')
                except CoordError as error:
                    return str(error)
                return 'UNEXPECTED admitted'
        with mock.patch.object(admin, '_same_boundary', side_effect=pause), \
                ThreadPoolExecutor(max_workers=1, thread_name_prefix='quarantine') as writer, \
                ThreadPoolExecutor(max_workers=1, thread_name_prefix='admission') as reader:
            future = writer.submit(quarantine)
            self.assertTrue(installed.wait(3))
            admission = reader.submit(native_admission)
            self.assertTrue(attempted.wait(3))
            release.set()
            self.assertEqual(future.result(timeout=5)['outcome'], 'failed_abandoned_quarantine')
            self.assertIn('quarantine is active', admission.result(timeout=5))
        self.assertEqual(self.sql('SELECT * FROM root_native_handoffs'), [])


    def test_preexisting_admitted_handoff_is_preserved_unknown(self):
        from agent_chat.root_team_reset import handoff_begin
        with contextlib.closing(Coordinator(self.db, 'a')) as coord:
            handoff_begin(coord.db, 'control', dict(id='already-admitted', target_session='a',
                thread_id=self.thread_id, action='model'), 'mac', 'changing')
        before = self.sql('SELECT * FROM root_native_handoffs')
        result, _ = self.quarantine()
        self.assertEqual(result['already_queued_or_racing_effects'], 'UNKNOWN')
        self.assertEqual(self.sql('SELECT * FROM root_native_handoffs'), before)
        preflight = self.execute('return-preflight')
        self.assertFalse(preflight['eligible'])
        self.assertIn('already admitted native handoff remains unresolved', preflight['blockers'])
        self.assertEqual(self.sql('SELECT * FROM root_native_handoffs'), before)


class DenyOnlyLocalAdmissionTests(unittest.TestCase):
    # Persisted deny-state tests, not custodian/role proof positives.
    def test_local_guard_and_untrusted_preload_never_create_new_execution(self):
        import tempfile
        from agent_chat.bridge_state import BridgeState
        from agent_chat.agent_control import AgentControlState
        with tempfile.TemporaryDirectory(prefix='deny-only-local-') as tmp, \
                contextlib.closing(Coordinator(Path(tmp)/'state.sqlite3', 'local')) as coord:
            coord.register('local')
            BridgeState(coord).bind(str(uuid.uuid4()))
            claim = coord.request('fixture-local-guard', 1)
            # A deny-only persisted state fixture cannot grant any authority.
            disposition._schema(coord.db)
            coord.db.execute('INSERT INTO original_owner_dispositions VALUES(?,?,?,?,?,?,?,?)',
                ('deny-only', 'local', 'deny-rid', '{}', '{}', '{}', '{}', 0))
            before = coord.db.execute('SELECT * FROM guarded_runs').fetchall()
            with self.assertRaisesRegex(CoordError, 'quarantine is active'):
                coord.begin_guard('fixture-local-guard', claim['token'], os.getpid())
            self.assertEqual(coord.db.execute('SELECT * FROM guarded_runs').fetchall(), before)
            assert_agent_running(coord.db, 'local', cleanup=True)
            controls = AgentControlState(coord)
            thread = coord.db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?', ('local',)).fetchone()[0]
            # Deliberately untrusted persisted request: no role/admission is
            # supplied. The deny gate must run before any preload side effect;
            # this is NOT a supported preload-authority positive.
            coord.db.execute('''INSERT INTO agent_control_requests
                (id,requester_session,target_session,host_id,thread_id,action,interrupt,status,created_at,updated_at)
                VALUES('deny-preload','local','local','fixture-host',?,'investigation-preload',0,'pending',0,0)''', (thread,))
            before = coord.db.execute('SELECT * FROM agent_control_requests').fetchall()
            with self.assertRaisesRegex(CoordError, 'quarantine is active'):
                controls.update('deny-preload', 'pending', 'preloading')
            self.assertEqual(coord.db.execute('SELECT * FROM agent_control_requests').fetchall(), before)
