"""Administrative attachment: isolated app-server observation, no CLI ownership claim."""
import contextlib
import hashlib
import io
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from unittest import mock

from agent_chat.core import Coordinator
from agent_chat import native_owner_evidence as evidence
from agent_chat import root_admin_attachment
import test_root_local as fixtures


class RootAdminAttachmentTests(fixtures.RootLocalTests):
    def test_local_cli_requires_and_forwards_explicit_digest(self):
        digest = 'a'*64
        arguments = ['--home-project','default','--home-session','root','--host-id','host','attach-current',
            '--expected-thread',str(uuid.uuid4()),'--current-thread',str(uuid.uuid4()),'--current-turn',str(uuid.uuid4()),
            '--expected-endpoint','ws://127.0.0.1:4500','--expected-listener-pid','123','--expected-birth-sec','8',
            '--expected-birth-usec','9','--expected-image-sha',digest,'--request-id',str(uuid.uuid4()),'--confirm']
        with mock.patch.object(fixtures.root_local,'execute',return_value={}) as execute, contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(fixtures.root_local.main(arguments),0)
        self.assertEqual(execute.call_args.args[1]['expected_image_sha'],digest)

    def attachment_fixture(self):
        self.enroll()
        target, turn, request = (str(uuid.uuid4()) for _ in range(3))
        with contextlib.closing(Coordinator(self.master)) as coord:
            coord.db.execute('''CREATE TABLE IF NOT EXISTS bridge_dispatcher (
                host_id TEXT PRIMARY KEY, owner TEXT NOT NULL, secret_hash TEXT NOT NULL, acquired_at REAL NOT NULL)''')
            coord.db.execute('INSERT INTO bridge_dispatcher VALUES(?,?,?,?)', ('host','fixture-owner','fixture-hash',time.time()))
            coord.db.execute('INSERT OR REPLACE INTO bridge_client_runtime(host_id,server,pid,heartbeat,error) VALUES(?,?,?,?,?)',
                             ('host','ws://127.0.0.1:4500',124,time.time(),None))
        observed = {'mode':'administrative_app_server_attachment', 'thread_id':target,
                    'turn_id':turn, 'endpoint':'ws://127.0.0.1:4500',
                    'listener_pid':123, 'listener_birth':[8,9], 'image_sha':'a'*64, 'code_hash':'fixture',
                    'connection_epoch':1, 'owner_state':'unknown', 'fresh_boot_proven':False}
        params = dict(expected_thread=self.thread,current_thread=target,current_turn=turn,
                      expected_endpoint='ws://127.0.0.1:4500',expected_listener_pid=123,
                      expected_birth_sec=8,expected_birth_usec=9,expected_image_sha='a'*64,request_id=request,confirm=True)
        return params, observed

    def test_explicit_existing_session_attachment_is_distinct_from_native_owner_proof(self):
        params, observed = self.attachment_fixture()
        with mock.patch.object(fixtures.root_local, 'observe_root_attachment', create=True, return_value=observed):
            result = self.call('attach-current', **params)
        self.assertEqual(result['mode'], 'administrative_app_server_attachment')
        self.assertEqual(result['owner_state'], 'unknown')
        self.assertEqual(result['fresh_boot_proven'], False)
        self.assertEqual(result['thread_id'], params['current_thread'])
        with self.assertRaisesRegex(fixtures.CoordError, 'retry differs'):
            self.call('attach-current', **dict(params,expected_image_sha='b'*64))
        with self.assertRaises(fixtures.CoordError):
            self.call('attach-current', **dict(params,expected_image_sha='invalid'))
        with contextlib.closing(Coordinator(self.master)) as coord:
            self.assertEqual(coord.db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?', ('root',)).fetchone()[0], params['current_thread'])

    def test_attachment_fence_withholds_old_job_and_new_input_without_rewriting_them(self):
        self.enroll()
        with contextlib.closing(Coordinator(self.master)) as coord:
            fixtures.dispatch(coord, {'op':'register','session':'sender','host_id':'host','params':{'agent':'sender'}})
            source = fixtures.dispatch(coord, {'op':'send','session':'sender','host_id':'host','params':{'to':'root','body':'First message'}})['id']
            coord.session = 'root'
            state = fixtures.BridgeState(coord)
            job = state.prepare(self.thread, [{'id':source,'recipient_session':'root'}], 'fixture payload')
            coord.db.execute('''CREATE TABLE root_admin_attachment_fences (
                request_id TEXT PRIMARY KEY, session_id TEXT NOT NULL, old_thread_id TEXT NOT NULL,
                new_thread_id TEXT NOT NULL, active INTEGER NOT NULL)''')
            coord.db.execute('INSERT INTO root_admin_attachment_fences VALUES(?,?,?,?,1)', ('request','root',self.thread,str(uuid.uuid4())))
            new = fixtures.dispatch(coord, {'op':'send','session':'sender','host_id':'host','params':{'to':'root','body':'Later message'}})['id']
            self.assertNotIn(job['id'], [item['id'] for item in state.jobs()])
            self.assertNotIn(new, [item['id'] for item in state.pending()])
            self.assertIsNone(state.prepare(self.thread, [{'id':new,'recipient_session':'root'}], 'later payload'))
            with self.assertRaises(fixtures.CoordError): state.update(job['id'], 'adding')
            coord.session = 'root'
            with self.assertRaisesRegex(fixtures.CoordError, 'cutover'):
                state.bind(thread_id=str(uuid.uuid4()))
            self.assertEqual(state.job(job['id'])['status'], 'prepared')
            self.assertEqual(coord.db.execute('SELECT body FROM messages WHERE id=?',(new,)).fetchone()[0], 'Later message')

    def test_attachment_preserves_stop_then_new_onboarding_and_historical_status(self):
        params, observed = self.attachment_fixture()
        stop = str(uuid.uuid4()); self.call('stop-all', request_id=stop, confirm=True)
        project = self.projects.create('New isolated app',workspace=str(self.workspace),instructions='Synthetic assignment')['id']
        with self.projects.connection() as db:
            for status in ['completed'] * 7 + ['uncertain']:
                db.execute('''INSERT INTO root_fresh_requests
                    (id,home_project,home_session,host_id,old_thread_id,prompt,instruction_origin,status,created_at,updated_at,native_action)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?)''',
                    (str(uuid.uuid4()),'default','root','host',self.thread,'fixture','authenticated_operator_ui',status,1,1,
                     'creating' if status == 'uncertain' else None))
            unknown = tuple(db.execute("SELECT id,status,old_thread_id,new_thread_id,queue_id,native_action FROM root_fresh_requests WHERE status='uncertain'").fetchone())
            db.commit()
        with contextlib.closing(Coordinator(self.master)) as coord:
            from agent_chat.root_restart import local_schema
            local_schema(coord.db)
            coord.db.execute('INSERT INTO root_fresh_gates VALUES(?,?,?,?,1)',(unknown[0],'root',self.thread,None))
        with mock.patch.object(fixtures.root_local, 'observe_root_attachment', return_value=observed):
            attached = self.call('attach-current', **params)
        from types import SimpleNamespace
        from agent_chat.root_restart import RootRestart
        restart = RootRestart(SimpleNamespace(projects=self.projects))
        self.assertNotIn(unknown[0], [job['id'] for job in restart.jobs('default','host')])
        with self.assertRaisesRegex(fixtures.CoordError, 'quarantines'):
            restart.update('default','host',unknown[0],'uncertain','uncertain',error='automatic reconciliation')
        self.assertFalse(attached['old_work_fenced'])
        with contextlib.closing(Coordinator(self.master)) as coord:
            self.assertEqual(coord.db.execute('SELECT active FROM root_team_stops WHERE id=?',(stop,)).fetchone()[0],1)
        self.consume()
        onboard = self.call('onboard-project',project=project,stop_id=stop,expected_thread=params['current_thread'],request_id=str(uuid.uuid4()),confirm=True)
        self.assertEqual(onboard['state'],'joined')
        self.consume()
        provisioned = self.call('provision',project=project,agent='orchestrator',request_id=str(uuid.uuid4()),confirm=True,model=None,effort=None)
        self.assertEqual(provisioned['project'], project)
        # Unqualified retired requests are unknown, not historical closure proof.
        with self.projects.connection() as db:
            for _ in range(2):
                db.execute('''INSERT INTO root_fresh_requests
                    (id,home_project,home_session,host_id,old_thread_id,prompt,instruction_origin,status,created_at,updated_at,native_action)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?)''',
                    (str(uuid.uuid4()),'default','root','host',self.thread,'fixture','authenticated_operator_ui','retired',1,1,None))
            db.commit()
        self.consume()
        with self.assertRaisesRegex(fixtures.CoordError, 'terminal eligibility is unresolved'):
            self.call('provision',project=project,agent='another',request_id=str(uuid.uuid4()),confirm=True,model=None,effort=None)
        historical = self.call('attach-status',request_id=params['request_id'])
        self.assertEqual((historical['state'],historical['thread_id'],historical['owner_state']), ('attached',params['current_thread'],'unknown'))
        self.assertTrue(historical['historical_readback'])
        with mock.patch.object(fixtures.root_local, 'observe_root_attachment') as observer:
            self.assertEqual(self.call('attach-current', **params), historical)
        observer.assert_not_called()
        with contextlib.closing(Coordinator(self.master)) as coord:
            gate = coord.db.execute('SELECT request_id,root_session,old_thread_id,new_thread_id,active FROM root_fresh_gates WHERE request_id=?',(unknown[0],)).fetchone()
            self.assertEqual(tuple(gate),(unknown[0],'root',self.thread,None,0))
        with self.projects.connection() as db:
            self.assertEqual(tuple(db.execute("SELECT id,status,old_thread_id,new_thread_id,queue_id,native_action FROM root_fresh_requests WHERE status='uncertain'").fetchone()), unknown)
            db.execute("UPDATE root_fresh_requests SET error='changed evidence' WHERE id=?",(unknown[0],))
            from agent_chat.root_admin_attachment import fresh_requests_blocking
            self.assertTrue(fresh_requests_blocking(db,'default','root','host'))
        with contextlib.closing(Coordinator(self.master)) as coord:
            self.assertEqual(coord.db.execute('SELECT active FROM root_team_stops WHERE id=?',(stop,)).fetchone()[0],1)

    def test_uncertain_old_wake_remains_fenced_without_replay(self):
        params, observed = self.attachment_fixture()
        with contextlib.closing(Coordinator(self.master)) as coord:
            fixtures.dispatch(coord, {'op':'register','session':'old-sender','host_id':'host','params':{'agent':'old-sender'}})
            old_source = fixtures.dispatch(coord, {'op':'send','session':'old-sender','host_id':'host','params':{'to':'root','body':'Old unread request'}})['id']
            coord.session = 'root'
            old_job = fixtures.BridgeState(coord).prepare(self.thread,[{'id':old_source,'recipient_session':'root'}],'unchanged')
            coord.db.execute("UPDATE bridge_jobs SET status='uncertain',error='unknown' WHERE id=?",(old_job['id'],))
        with mock.patch.object(fixtures.root_local, 'observe_root_attachment', return_value=observed):
            with self.assertRaisesRegex(fixtures.CoordError, 'read your inbox'):
                self.call('attach-current', **params)
        with contextlib.closing(Coordinator(self.master)) as coord:
            self.assertEqual(coord.db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?',('root',)).fetchone()[0],self.thread)
        with self.projects.connection() as db:
            self.assertFalse(db.execute("SELECT 1 FROM sqlite_master WHERE name='root_admin_attachment_requests'").fetchone())
        self.consume()
        with mock.patch.object(fixtures.root_local, 'observe_root_attachment', return_value=observed):
            attached = self.call('attach-current', **params)
        self.assertTrue(attached['old_work_fenced'])
        with contextlib.closing(Coordinator(self.master,'root')) as coord:
            state = fixtures.BridgeState(coord)
            self.assertNotIn(old_job['id'], [job['id'] for job in state.jobs()])
            self.assertEqual((state.job(old_job['id'])['status'],state.job(old_job['id'])['payload']),('uncertain','unchanged'))
            with self.assertRaises(fixtures.CoordError): state.retry(old_job['id'], confirmed=True)
            self.assertNotIn(old_source, [message['id'] for message in state.pending()])
            self.assertIsNone(state.prepare(params['current_thread'],[{'id':old_source,'recipient_session':'root'}],'replay'))
            self.assertIsNone(coord.db.execute('SELECT acked_at FROM messages WHERE id=?',(old_source,)).fetchone()[0])
            self.assertIsNotNone(coord.db.execute("SELECT 1 FROM message_reads WHERE message_id=? AND reader_session='root' AND provenance='recipient_context_full'",(old_source,)).fetchone())
            self.assertNotEqual(state.capacity_gate('root', params['current_thread']), 'administrative attachment has unresolved old-thread work')
            fixtures.dispatch(coord, {'op':'register','session':'new-sender','host_id':'host','params':{'agent':'new-sender'}})
            new = fixtures.dispatch(coord, {'op':'send','session':'new-sender','host_id':'host','params':{'to':'root','body':'Independent new request'}})['id']
            coord.session = 'root'
            self.assertIn(new, [message['id'] for message in state.pending()])
            new_job = state.prepare(params['current_thread'], [{'id':new,'recipient_session':'root'}], 'new independent payload')
            self.assertIsNotNone(new_job)
            self.assertIn(new_job['id'], [job['id'] for job in state.jobs()])
            self.assertNotIn(old_job['id'], [job['id'] for job in state.jobs()])
            from agent_chat.bridge import Bridge
            from test_project_activity import ObservedProvider
            provider = ObservedProvider(); provider.add(params['current_thread'], str(self.neutral))
            output = self.base / 'independent-result.txt'
            original_request = provider.request
            def execute_new(method, arguments, **kwargs):
                if method == 'thread/queue/add':
                    self.assertEqual(arguments['threadId'], params['current_thread'])
                    self.assertIn('new independent payload', str(arguments['input']))
                    subprocess.run([sys.executable, '-c', 'from pathlib import Path; import sys; Path(sys.argv[1]).write_text("independent result")', str(output)], check=True, timeout=5)
                return original_request(method,arguments,**kwargs)
            provider.request = execute_new
            self.assertFalse(output.exists())
            Bridge(state, provider).tick(observe_policies=False)
            self.assertEqual(state.job(new_job['id'])['status'], 'dispatched')
            self.assertEqual(output.read_text(), 'independent result')
            self.assertEqual((state.job(old_job['id'])['status'],state.job(old_job['id'])['payload']),('uncertain','unchanged'))

    def test_unverified_peer_changes_no_binding_or_journal(self):
        params, _ = self.attachment_fixture()
        with mock.patch.object(fixtures.root_local, 'observe_root_attachment', side_effect=fixtures.CoordError('unverified peer')):
            with self.assertRaisesRegex(fixtures.CoordError, 'unverified peer'):
                self.call('attach-current', **params)
        with contextlib.closing(Coordinator(self.master)) as coord:
            self.assertEqual(coord.db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?',('root',)).fetchone()[0],self.thread)
        with self.projects.connection() as db:
            self.assertFalse(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_admin_attachment_requests'").fetchone())

    def test_changed_dispatcher_during_observation_does_not_switch_binding(self):
        params, observed = self.attachment_fixture()
        def changed(*args):
            with contextlib.closing(Coordinator(self.master)) as coord:
                coord.db.execute("UPDATE bridge_dispatcher SET owner='different' WHERE host_id='host'")
            return observed
        with mock.patch.object(fixtures.root_local, 'observe_root_attachment', side_effect=changed):
            with self.assertRaisesRegex(fixtures.CoordError, 'bridge identity changed'):
                self.call('attach-current', **params)
        with contextlib.closing(Coordinator(self.master)) as coord:
            self.assertEqual(coord.db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?',('root',)).fetchone()[0],self.thread)

    def test_pending_fresh_request_is_not_quarantined(self):
        params, observed = self.attachment_fixture()
        with self.projects.connection() as db:
            db.execute('''INSERT INTO root_fresh_requests (id,home_project,home_session,host_id,old_thread_id,prompt,instruction_origin,status,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?)''',(str(uuid.uuid4()),'default','root','host',self.thread,'fixture','authenticated_operator_ui','pending',1,1))
            db.commit()
        with mock.patch.object(fixtures.root_local, 'observe_root_attachment', return_value=observed):
            with self.assertRaisesRegex(fixtures.CoordError, 'turnover reconciliation'):
                self.call('attach-current', **params)

    def test_foreign_or_multiple_turnover_gates_are_rejected(self):
        from agent_chat.root_restart import local_schema
        params, observed = self.attachment_fixture()
        with contextlib.closing(Coordinator(self.master)) as coord:
            local_schema(coord.db)
            coord.db.execute('INSERT INTO root_fresh_gates VALUES(?,?,?,?,1)',(str(uuid.uuid4()),'root',self.thread,None))
        with mock.patch.object(fixtures.root_local, 'observe_root_attachment', return_value=observed):
            with self.assertRaisesRegex(fixtures.CoordError, 'turnover gate'):
                self.call('attach-current', **params)
        with contextlib.closing(Coordinator(self.master)) as coord:
            self.assertEqual(coord.db.execute('SELECT active FROM root_fresh_gates').fetchone()[0],1)

    def test_two_matching_turnover_gates_are_rejected(self):
        from agent_chat.root_restart import local_schema
        params, observed = self.attachment_fixture()
        requests = [str(uuid.uuid4()),str(uuid.uuid4())]
        with self.projects.connection() as db:
            for request in requests:
                db.execute('''INSERT INTO root_fresh_requests (id,home_project,home_session,host_id,old_thread_id,prompt,instruction_origin,status,created_at,updated_at,native_action)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?)''',(request,'default','root','host',self.thread,'fixture','authenticated_operator_ui','uncertain',1,1,'creating'))
            db.commit()
        with contextlib.closing(Coordinator(self.master)) as coord:
            local_schema(coord.db)
            for request in requests:
                coord.db.execute('INSERT INTO root_fresh_gates VALUES(?,?,?,?,1)',(request,'root',self.thread,None))
        with mock.patch.object(fixtures.root_local, 'observe_root_attachment', return_value=observed):
            with self.assertRaisesRegex(fixtures.CoordError,'turnover gate'):
                self.call('attach-current', **params)
        with contextlib.closing(Coordinator(self.master)) as coord:
            self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM root_fresh_gates WHERE active=1').fetchone()[0],2)

    def test_gate_transfer_interruption_reenters_and_detects_changed_gate(self):
        from agent_chat.root_restart import local_schema
        params, observed = self.attachment_fixture()
        request = str(uuid.uuid4())
        with self.projects.connection() as db:
            db.execute('''INSERT INTO root_fresh_requests (id,home_project,home_session,host_id,old_thread_id,prompt,instruction_origin,status,created_at,updated_at,native_action)
                VALUES(?,?,?,?,?,?,?,?,?,?,?)''',(request,'default','root','host',self.thread,'fixture','authenticated_operator_ui','uncertain',1,1,'creating'))
            db.commit()
        with contextlib.closing(Coordinator(self.master)) as coord:
            local_schema(coord.db)
            coord.db.execute('INSERT INTO root_fresh_gates VALUES(?,?,?,?,1)',(request,'root',self.thread,None))
        with mock.patch.object(fixtures.root_local, 'observe_root_attachment', side_effect=[observed,fixtures.CoordError('peer disappeared')]):
            with self.assertRaisesRegex(fixtures.CoordError,'peer disappeared'):
                self.call('attach-current', **params)
        with contextlib.closing(Coordinator(self.master)) as coord:
            self.assertEqual(coord.db.execute('SELECT active FROM root_fresh_gates WHERE request_id=?',(request,)).fetchone()[0],0)
            coord.db.execute('UPDATE root_fresh_gates SET new_thread_id=? WHERE request_id=?',(str(uuid.uuid4()),request))
        with mock.patch.object(fixtures.root_local, 'observe_root_attachment', return_value=observed):
            with self.assertRaisesRegex(fixtures.CoordError,'retained gate differs'):
                self.call('attach-current', **params)
        with contextlib.closing(Coordinator(self.master)) as coord:
            coord.db.execute('UPDATE root_fresh_gates SET new_thread_id=NULL WHERE request_id=?',(request,))
        with mock.patch.object(fixtures.root_local, 'observe_root_attachment', return_value=observed):
            self.assertEqual(self.call('attach-current', **params)['thread_id'],params['current_thread'])

    def test_quarantined_fresh_request_withholds_jobs_and_stale_updates(self):
        from agent_chat.root_restart import RootRestart, RootRestartWorker
        from agent_chat.root_admin_attachment import fresh_request_snapshot, fresh_requests_blocking
        from types import SimpleNamespace
        params, observed = self.attachment_fixture()
        request = str(uuid.uuid4())
        with self.projects.connection() as db:
            db.execute('''INSERT INTO root_fresh_requests (id,home_project,home_session,host_id,old_thread_id,prompt,instruction_origin,status,created_at,updated_at,native_action)
                VALUES(?,?,?,?,?,?,?,?,?,?,?)''',(request,'default','root','host',self.thread,'fixture','authenticated_operator_ui','uncertain',1,1,'creating'))
            db.commit()
        restart = RootRestart(self.roots)
        stale = restart.job(request)
        self.assertIn(request,[job['id'] for job in restart.jobs('default','host')])
        with mock.patch.object(fixtures.root_local, 'observe_root_attachment', side_effect=[observed,fixtures.CoordError('peer disappeared')]):
            with self.assertRaisesRegex(fixtures.CoordError, 'peer disappeared'):
                self.call('attach-current', **params)
        with self.projects.connection() as db:
            before = fresh_request_snapshot(db.execute('SELECT * FROM root_fresh_requests WHERE id=?',(request,)).fetchone())
        self.assertNotIn(request,[job['id'] for job in restart.jobs('default','host')])
        rpc = mock.Mock()
        state = SimpleNamespace(root_fresh_job=restart.job, root_fresh_update=lambda ident, expected, status, **values:
            restart.update('default','host',ident,expected,status,**values))
        worker = RootRestartWorker(SimpleNamespace(state=state,rpc=rpc))
        with self.assertRaisesRegex(fixtures.CoordError, 'quarantines'):
            worker.advance(stale)
        rpc.request.assert_not_called()
        with self.projects.connection() as db:
            self.assertEqual(before,fresh_request_snapshot(db.execute('SELECT * FROM root_fresh_requests WHERE id=?',(request,)).fetchone()))
        with mock.patch.object(fixtures.root_local, 'observe_root_attachment', return_value=observed):
            self.call('attach-current', **params)
        self.assertNotIn(request,[job['id'] for job in restart.jobs('default','host')])
        with self.assertRaisesRegex(fixtures.CoordError, 'quarantines'):
            restart.update('default','host',request,'uncertain','uncertain',error='late replay')
        with self.projects.connection() as db:
            self.assertEqual(before,fresh_request_snapshot(db.execute('SELECT * FROM root_fresh_requests WHERE id=?',(request,)).fetchone()))
            self.assertFalse(fresh_requests_blocking(db,'default','root','host'))

    def test_partial_home_switch_reenters_exact_request(self):
        params, observed = self.attachment_fixture()
        with mock.patch.object(fixtures.root_local, 'observe_root_attachment', side_effect=[observed,fixtures.CoordError('peer disappeared')]):
            with self.assertRaisesRegex(fixtures.CoordError, 'peer disappeared'):
                self.call('attach-current', **params)
        with self.projects.connection() as db:
            self.assertEqual(db.execute('SELECT thread_id FROM root_config').fetchone()[0],self.thread)
        with contextlib.closing(Coordinator(self.master)) as coord:
            self.assertEqual(coord.db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?',('root',)).fetchone()[0],params['current_thread'])
            self.assertEqual(coord.db.execute('SELECT active FROM root_admin_attachment_fences').fetchone()[0],1)
        with mock.patch.object(fixtures.root_local, 'observe_root_attachment', return_value=observed):
            result = self.call('attach-current', **params)
        self.assertEqual(result['thread_id'],params['current_thread'])
        with self.assertRaises(fixtures.CoordError):
            self.call('attach-current', **dict(params,current_turn=str(uuid.uuid4())))


class IndependentPeerTests(unittest.TestCase):
    def test_independent_listener_accepts_explicit_current_digest(self):
        with tempfile.TemporaryDirectory() as directory:
            image = Path(directory) / 'codex'; image.write_bytes(b'new verified fixture binary')
            digest = hashlib.sha256(image.read_bytes()).hexdigest()
            process = evidence.Process(123,1,1,os.geteuid(),os.getuid(),8,9,str(image))
            identity = {'client_instance':'fixture','connection_epoch':1,'pid':os.getpid(),'fd':5,
                        'local':('127.0.0.1',50001),'peer':('127.0.0.1',4500)}
            rpc = mock.Mock(); rpc.connection_identity.return_value = identity
            kernel = evidence.DarwinEvidence.__new__(evidence.DarwinEvidence)
            kernel.process = mock.Mock(return_value=process)
            kernel.lsof = mock.Mock(side_effect=[((123,'4','127.0.0.1:4500'),),((123,'7','127.0.0.1:4500->127.0.0.1:50001'),)])
            with mock.patch.object(evidence,'IMAGE',image), mock.patch.object(evidence,'dynamic_disk_association',return_value='signed-image'):
                result = kernel.sample_independent_listener('ws://127.0.0.1:4500',123,(8,9),rpc,digest)
            self.assertEqual(result.image_sha,digest)
            for invalid in ('0'*64, 'not-a-digest'):
                kernel.lsof = mock.Mock(return_value=((123,'4','127.0.0.1:4500'),))
                with self.subTest(invalid=invalid), mock.patch.object(evidence,'IMAGE',image), self.assertRaises(evidence.EvidenceError):
                    kernel.sample_independent_listener('ws://127.0.0.1:4500',123,(8,9),rpc,invalid)
            kernel.lsof = mock.Mock(return_value=((123,'4','127.0.0.1:4500'),))
            with mock.patch.object(evidence,'IMAGE',image), mock.patch.object(evidence,'dynamic_disk_association',side_effect=evidence.EvidenceError('signature mismatch')), self.assertRaises(evidence.EvidenceError):
                kernel.sample_independent_listener('ws://127.0.0.1:4500',123,(8,9),rpc,digest)
    def test_exact_listener_socket_and_identity_are_required(self):
        with tempfile.TemporaryDirectory() as directory:
            image = Path(directory) / 'codex'; image.write_bytes(b'synthetic executable')
            pid = 123
            process = evidence.Process(pid, 1, 1, os.geteuid(), os.getuid(), 8, 9, str(image))
            identity = {'client_instance':'fixture','connection_epoch':1,'pid':os.getpid(),'fd':5,
                        'local':('127.0.0.1',50001),'peer':('127.0.0.1',4500)}
            rpc = mock.Mock(); rpc.connection_identity.return_value = identity
            kernel = evidence.DarwinEvidence.__new__(evidence.DarwinEvidence)
            kernel.process = mock.Mock(return_value=process)
            kernel.lsof = mock.Mock(side_effect=[((pid,'4','127.0.0.1:4500'),),
                                                ((pid,'7','127.0.0.1:4500->127.0.0.1:50001'),)])
            with mock.patch.object(evidence, 'IMAGE', image), mock.patch.object(evidence, 'IMAGE_SHA', hashlib.sha256(image.read_bytes()).hexdigest()), \
                    mock.patch.object(evidence, 'dynamic_disk_association', return_value='signed-image'):
                sample = kernel.sample_independent_listener('ws://127.0.0.1:4500', pid, (8,9), rpc, hashlib.sha256(image.read_bytes()).hexdigest())
            self.assertEqual(sample.listener_pid, pid)
            self.assertEqual(sample.peer, (('127.0.0.1',50001),('127.0.0.1',4500),'7'))
            kernel.lsof = mock.Mock(return_value=((pid+1,'4','127.0.0.1:4500'),))
            with self.assertRaises(evidence.EvidenceError):
                kernel.sample_independent_listener('ws://127.0.0.1:4500', pid, (8,9), rpc, hashlib.sha256(image.read_bytes()).hexdigest())

    def test_observation_requires_exact_current_peer_thread_turn_and_cwd(self):
        target, turn = str(uuid.uuid4()), str(uuid.uuid4())
        process = evidence.Process(123, 1, 1, os.geteuid(), os.getuid(), 8, 9, '/fixture/codex')
        image_digest = 'a'*64
        sample = evidence.Sample(('127.0.0.1',4500), 123, (process,), image_digest, 'code', (('127.0.0.1',50001),('127.0.0.1',4500),'7'))
        kernel = mock.Mock(); kernel.sample_independent_listener.return_value = sample
        rpc = mock.Mock(); rpc.connection_identity.return_value = {'connection_epoch':1}
        rpc.request.return_value = {'thread':{'id':target,'cwd':'/neutral','status':{'type':'active'},
                                              'turns':[{'id':turn,'status':'inProgress','error':None}]}}
        registration = {'endpoint':'ws://127.0.0.1:4500','heartbeat':time.time()}
        result = root_admin_attachment.observe(registration, target, turn, '/neutral', 123, (8,9), image_digest, _kernel=kernel, _rpc=rpc)
        self.assertEqual(result['mode'], 'administrative_app_server_attachment')
        self.assertEqual(result['owner_state'], 'unknown')
        self.assertFalse(result['fresh_boot_proven'])
        rpc.request.return_value['thread']['turns'][0]['id'] = str(uuid.uuid4())
        with self.assertRaisesRegex(fixtures.CoordError, 'exact active thread and turn'):
            root_admin_attachment.observe(registration, target, turn, '/neutral', 123, (8,9), image_digest, _kernel=kernel, _rpc=rpc)
        rpc.request.return_value['thread']['turns'][0]['id'] = turn
        rpc.request.return_value['thread']['status'] = 'active'
        with self.assertRaisesRegex(fixtures.CoordError, 'exact active thread and turn'):
            root_admin_attachment.observe(registration, target, turn, '/neutral', 123, (8,9), image_digest, _kernel=kernel, _rpc=rpc)
        rpc.request.return_value['thread']['status'] = {'type': 'idle'}
        rpc.request.return_value['thread']['turns'] = [{'id': turn, 'status': 'interrupted', 'error': None}]
        with self.assertRaisesRegex(fixtures.CoordError, 'exact active thread and turn'):
            root_admin_attachment.observe(registration, target, turn, '/neutral', 123, (8,9), image_digest,
                                          _kernel=kernel, _rpc=rpc)
        recovered = root_admin_attachment.observe(registration, target, turn, '/neutral', 123, (8,9), image_digest,
                                                    allow_terminal=True, _kernel=kernel, _rpc=rpc)
        self.assertEqual(recovered['turn_id'], turn)
        self.assertEqual(recovered['owner_state'], 'unknown')
        self.assertFalse(recovered['fresh_boot_proven'])
        with self.assertRaisesRegex(fixtures.CoordError, 'exact active thread and turn'):
            root_admin_attachment.observe(registration, target, target, '/neutral', 123, (8,9), image_digest,
                                          allow_terminal=True, _kernel=kernel, _rpc=rpc)
        rpc.request.side_effect = root_admin_attachment.RpcError(-32600, 'thread not loaded')
        with self.assertRaisesRegex(fixtures.CoordError, 'unverified: .*thread not loaded'):
            root_admin_attachment.observe(registration, target, turn, '/neutral', 123, (8,9), image_digest, _kernel=kernel, _rpc=rpc)

    def test_composed_peer_observation_rejects_stale_or_changed_evidence(self):
        target, turn = str(uuid.uuid4()), str(uuid.uuid4())
        with tempfile.TemporaryDirectory() as directory:
            image = Path(directory) / 'codex'; image.write_bytes(b'fixture executable')
            image_digest = hashlib.sha256(image.read_bytes()).hexdigest()
            process = evidence.Process(123, 1, 1, os.geteuid(), os.getuid(), 8, 9, str(image))
            identity = {'client_instance':'fixture','connection_epoch':1,'pid':os.getpid(),'fd':5,
                        'local':('127.0.0.1',50001),'peer':('127.0.0.1',4500)}
            def build():
                kernel = evidence.DarwinEvidence.__new__(evidence.DarwinEvidence)
                kernel.process = mock.Mock(return_value=process)
                kernel.lsof = mock.Mock(side_effect=lambda *args: ((123,'4','127.0.0.1:4500'),) if '-sTCP:LISTEN' in args else ((123,'7','127.0.0.1:4500->127.0.0.1:50001'),))
                rpc = mock.Mock(); rpc.connection_identity.return_value = identity.copy()
                rpc.request.return_value = {'thread':{'id':target,'cwd':'/neutral','status':{'type':'active'},'turns':[{'id':turn,'status':'inProgress','error':None}]}}
                return kernel, rpc
            registration = {'endpoint':'ws://127.0.0.1:4500','heartbeat':time.time()}
            with mock.patch.object(evidence, 'IMAGE', image), mock.patch.object(evidence, 'IMAGE_SHA', hashlib.sha256(image.read_bytes()).hexdigest()), mock.patch.object(evidence, 'dynamic_disk_association', return_value='signed-image'):
                kernel, rpc = build()
                result = root_admin_attachment.observe(registration, target, turn, '/neutral', 123, (8,9), image_digest, _kernel=kernel, _rpc=rpc)
                self.assertEqual(result['listener_pid'], 123)
                self.assertEqual(result['owner_state'], 'unknown')
                for failure in ('stale', 'birth', 'disconnected', 'generation'):
                    kernel, rpc = build(); current = dict(registration)
                    if failure == 'stale': current['heartbeat'] -= 100
                    if failure == 'birth': kernel.process.return_value = evidence.Process(123,1,1,os.geteuid(),os.getuid(),8,10,str(image))
                    if failure == 'disconnected': rpc.connection_identity.side_effect = OSError('closed')
                    if failure == 'generation': rpc.connection_identity.side_effect = [identity]*3 + [dict(identity, connection_epoch=2)]*3
                    with self.subTest(failure=failure), self.assertRaises(fixtures.CoordError):
                        root_admin_attachment.observe(current, target, turn, '/neutral', 123, (8,9), image_digest, _kernel=kernel, _rpc=rpc)


def load_tests(loader, tests, pattern):
    return unittest.TestSuite([RootAdminAttachmentTests('test_local_cli_requires_and_forwards_explicit_digest'),
                               RootAdminAttachmentTests('test_explicit_existing_session_attachment_is_distinct_from_native_owner_proof'),
                               RootAdminAttachmentTests('test_attachment_fence_withholds_old_job_and_new_input_without_rewriting_them'),
                               RootAdminAttachmentTests('test_attachment_preserves_stop_then_new_onboarding_and_historical_status'),
                               RootAdminAttachmentTests('test_uncertain_old_wake_remains_fenced_without_replay'),
                               RootAdminAttachmentTests('test_unverified_peer_changes_no_binding_or_journal'),
                               RootAdminAttachmentTests('test_changed_dispatcher_during_observation_does_not_switch_binding'),
                               RootAdminAttachmentTests('test_pending_fresh_request_is_not_quarantined'),
                               RootAdminAttachmentTests('test_foreign_or_multiple_turnover_gates_are_rejected'),
                               RootAdminAttachmentTests('test_two_matching_turnover_gates_are_rejected'),
                               RootAdminAttachmentTests('test_gate_transfer_interruption_reenters_and_detects_changed_gate'),
                               RootAdminAttachmentTests('test_quarantined_fresh_request_withholds_jobs_and_stale_updates'),
                               RootAdminAttachmentTests('test_partial_home_switch_reenters_exact_request'),
                               IndependentPeerTests('test_exact_listener_socket_and_identity_are_required'),
                               IndependentPeerTests('test_independent_listener_accepts_explicit_current_digest'),
                               IndependentPeerTests('test_observation_requires_exact_current_peer_thread_turn_and_cwd'),
                               IndependentPeerTests('test_composed_peer_observation_rejects_stale_or_changed_evidence')])
