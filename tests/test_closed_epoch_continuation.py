"""Closed original-run epoch continuation; all authority observations are fixture data."""
import contextlib
import base64
import hashlib
import io
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import uuid
from unittest import mock

from agent_chat.core import Coordinator, CoordError
from agent_chat.lifecycle_producer import credential, main as enrollment_main
from agent_chat.remote import RemoteCoordError
from test_remote_web import RemoteWebFixture


class ClosedEpochContinuationTests(RemoteWebFixture):
    def start(self, **kwargs):
        sessions = Path(self.tmp.name) / 'sessions'
        sessions.mkdir(exist_ok=True)
        super().start(sessions_root=sessions, **kwargs)

    def tearDown(self):
        pass

    def setUp(self):
        from agent_chat import core, service, root_local
        self.tmp = tempfile.TemporaryDirectory(prefix='closed-epoch-')
        self.addCleanup(self.tmp.cleanup)
        base=Path(self.tmp.name)
        self.db=base/'state.sqlite3'
        home=base/'home'; (home/'.codex/sessions').mkdir(parents=True)
        environment=mock.patch.dict(os.environ, HOME=str(home), CODEX_HOME=str(home/'.codex'))
        environment.start(); self.addCleanup(environment.stop)
        config=service.InstalledServerConfig(base/'server.plist',base/'wrapper',base/'server-bin',
            self.db,base,os.getuid(),'127.0.0.1',8765)
        provider=mock.patch.object(service,'INSTALLED_SERVER_CONFIG',config)
        provider.start(); self.addCleanup(provider.stop)
        for module in (service, root_local):
            resolver=mock.patch.object(module, '_installed_server_config', return_value=config)
            resolver.start(); self.addCleanup(resolver.stop)
        default=mock.patch.object(core, 'default_db', side_effect=AssertionError('default discovery forbidden'))
        default.start(); self.addCleanup(default.stop)
        Coordinator(self.db).close()
        self.start()
        self.addCleanup(self.stop)
        from agent_chat.bridge_state import BridgeState
        self.call('register',agent='old-worker')
        self.old_thread=str(uuid.uuid4())
        self.call('bind',thread=self.old_thread)
        self.old=self.call('request',resource='validation-clone',minutes=5)
        self.oldrun=self.call('begin-guard',resource='validation-clone',token=self.old['token'])
        # This owned, inert child has a real waitable lifetime. Closing its
        # guard is permitted only after observing its successful termination.
        self.oldchild=subprocess.Popen([sys.executable,'-c','import sys;sys.stdin.buffer.read()'],
            stdin=subprocess.PIPE,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        self.addCleanup(self.finish_old_child)
        self.call('attach-guard-pid',run_id=self.oldrun['run_id'],token=self.old['token'],pid=self.oldchild.pid)
        self.call('register',session='b',agent='new-worker')
        self.newthread=str(uuid.uuid4());self.call('bind',session='b',thread=self.newthread)
        self.call('request',session='b',resource='workspace:/fixture/new',minutes=5)
        self.next=self.call('request',session='b',resource='validation-clone',minutes=5)
        self.assertEqual(self.next['state'],'queued')
        self.producer_path=Path(self.tmp.name)/'epoch-producer'
        with contextlib.redirect_stdout(io.StringIO()):
            enrollment_main(['--db',str(self.db),'--owner','b','--host','mac','--capability-file',str(self.producer_path),'--current-workspace','workspace:/fixture/new','--current-leaf-prefix','file:workspaces/fixture-new/','--current-gitdir','/fixture/gitdir','--current-index-resource','file:git/worktrees/fixture-new/index'])
        self.auth={'capability':credential(self.producer_path)}
        current=self.call('status',session='b',resources=['workspace:/fixture/new'])['resources'][0]
        # The fixture's current reservation result is retrieved by its own owner.
        claim=self.call('request',session='b',resource='workspace:/fixture/new',minutes=5)
        self.current_claim=claim
        self.run=self.call('begin-guard',session='b',resource='workspace:/fixture/new',token=claim['token'])
        self.instance=self.call('producer-establish',session='b',run_id=self.run['run_id'],**self.auth)
        now=time.time();self.deadline=now-100
        with contextlib.closing(Coordinator(self.db)) as c:
            c.db.execute('UPDATE resources SET deadline=? WHERE name=?',(self.deadline,'validation-clone'))
            c.db.execute('UPDATE remote_guarded_runs SET started_at=? WHERE run_id=?',(now-1000,self.oldrun['run_id']))
        self.selected=('expired-claim-v2','default','root-fixture','mac','default','validation-clone','a',self.old['reservation_id'],self.deadline)
        self.observation=dict(observed_at=now,canonical_resource='validation-clone',old_reservation=self.old['reservation_id'],root_generation='fixture-root-generation',boot_id=str(uuid.uuid4()),boot_epoch=now-500,old_backing='/fixture/old:1:11',backing='/fixture/new:1:12',execution_id=self.instance['id'])
        self.request=str(uuid.uuid4())
        from agent_chat.expired_claim_disposition import _prepare_execution_epoch
        from agent_chat.core import execution_epoch_handoff
        with contextlib.closing(Coordinator(self.db)) as c:
            with c.tx():
                _prepare_execution_epoch(c.db,self.selected,request_id=self.request,instance_id=self.instance['id'],workspace='workspace:/fixture/new',leaf_prefix='file:workspaces/fixture-new/',gitdir='/fixture/gitdir',index_resource='file:git/worktrees/fixture-new/index',root_generation='fixture-root-generation',host='mac',authority_digest='fixture-configured-root-authority')
        self.attestation=self.attest()

    def finish_old_child(self):
        if self.oldchild.stdin and not self.oldchild.stdin.closed:
            self.oldchild.stdin.close()
        self.assertEqual(self.oldchild.wait(timeout=5),0)

    def close_observed_old_child(self):
        self.finish_old_child()
        evidence=('private fixture child '+str(self.oldchild.pid)+' exited 0 and was reaped').encode()
        self.call('close-guard',run_id=self.oldrun['run_id'],token=self.old['token'],
                  evidence_sha256=hashlib.sha256(evidence).hexdigest())

    def attest(self,**changes):
        observation=dict(self.observation,**changes)
        return self.call('producer-epoch-attest',session='b',run_id=self.run['run_id'],instance_id=self.instance['id'],request_id=str(uuid.uuid4()),observation=observation,prepare_id=self.request,gitdir='/fixture/gitdir:1:99',leaf_prefix='file:workspaces/fixture-new/',index_resource='file:git/worktrees/fixture-new/index',native_processes=[],**self.auth)

    def preview(self,c):
        from agent_chat.expired_claim_disposition import _execution_epoch_prestate
        return _execution_epoch_prestate(c.db,self.selected,self.attestation['id'],'fixture-root-generation','mac')[0]

    def apply(self,c,stamp=None,**changes):
        from agent_chat.expired_claim_disposition import _apply_execution_epoch
        args=dict(request_id=self.request,expected_digest=stamp or self.preview(c),attestation_id=self.attestation['id'],root_generation='fixture-root-generation',host='mac',authority_digest='fixture-configured-root-authority')
        args.update(changes)
        return _apply_execution_epoch(c.db,self.selected,**args)

    def test_real_receiver_admits_fresh_guard_after_observed_old_child_exit(self):
        self.close_observed_old_child()
        with contextlib.closing(Coordinator(self.db)) as c:
            old = tuple(c.db.execute('SELECT * FROM remote_guarded_runs WHERE run_id=?', (self.oldrun['run_id'],)).fetchone())
            queue = tuple(tuple(row) for row in c.db.execute('SELECT * FROM resource_queue ORDER BY seq'))
            with c.tx():
                stamp = self.preview(c)
                result = self.apply(c, stamp)
            self.assertIn(('old_physical_state', 'UNVERIFIED'), result)
            self.assertEqual(tuple(c.db.execute('SELECT * FROM remote_guarded_runs WHERE run_id=?', (self.oldrun['run_id'],)).fetchone()), old)
            self.assertEqual(tuple(tuple(row) for row in c.db.execute('SELECT * FROM resource_queue ORDER BY seq')), queue)
            with c.tx(): self.assertIn(('replayed', True), self.apply(c, stamp))
            self.assertEqual(c.db.execute('SELECT count(*) FROM execution_epochs').fetchone()[0], 1)
            self.assertIsNone(c.db.execute('SELECT 1 FROM receipts WHERE reservation_id=?', (self.old['reservation_id'],)).fetchone())
        fresh=self.call('request',session='b',resource='validation-clone',minutes=5)
        self.assertEqual(fresh['state'],'owned')
        self.assertNotEqual(fresh['token'],self.old['token'])
        for session, token in [('a', self.old['token']), ('b', fresh['token'])]:
            with self.assertRaises(RemoteCoordError):
                self.call('close-guard',session=session,run_id=self.oldrun['run_id'],token=token,evidence_sha256='0'*64)
        with self.assertRaisesRegex(RemoteCoordError,'workspace rights'):
            self.call('begin-guard',session='b',resource='validation-clone',token=fresh['token'])

    def test_unknown_open_run_is_not_closed_or_made_reusable(self):
        self.finish_old_child()
        # A reaped test child is not itself an authenticated server close report.
        with contextlib.closing(Coordinator(self.db)) as c:
            before = tuple(c.db.execute('SELECT * FROM remote_guarded_runs WHERE run_id=?', (self.oldrun['run_id'],)).fetchone())
            with c.tx(): self.apply(c)
            self.assertEqual(tuple(c.db.execute('SELECT * FROM remote_guarded_runs WHERE run_id=?', (self.oldrun['run_id'],)).fetchone()), before)
            self.assertIsNone(c.db.execute('SELECT closed_at FROM remote_guarded_runs WHERE run_id=?', (self.oldrun['run_id'],)).fetchone()[0])
        with self.assertRaisesRegex(RemoteCoordError, 'unresolved open guarded run'):
            self.call('request',session='b',resource='validation-clone',minutes=5)
        with self.assertRaises(RemoteCoordError):
            self.call('close-guard',run_id=self.oldrun['run_id'],token=self.old['token'],evidence_sha256='0'*64)

    def test_closed_run_missing_or_foreign_evidence_is_not_epoch_authority(self):
        self.close_observed_old_child()
        mutations = [('close_evidence_sha256', None), ('close_evidence_sha256', 'not-a-digest'),
                     ('session_id', 'b'), ('host_id', 'other'), ('closed_at', 0),
                     ('closed_at', float('inf'))]
        with contextlib.closing(Coordinator(self.db)) as c:
            before = dict(c.db.execute('SELECT * FROM remote_guarded_runs WHERE run_id=?', (self.oldrun['run_id'],)).fetchone())
            for column, value in mutations:
                with self.subTest(column=column, value=value):
                    c.db.execute('UPDATE remote_guarded_runs SET '+column+'=? WHERE run_id=?', (value, self.oldrun['run_id']))
                    with self.assertRaisesRegex(CoordError, 'closed guard evidence'):
                        with c.tx(): self.apply(c)
                    self.assertEqual(c.db.execute('SELECT count(*) FROM execution_epochs').fetchone()[0], 0)
                    self.assertEqual(c.db.execute("SELECT reservation_id FROM resources WHERE name='validation-clone'").fetchone()[0], self.old['reservation_id'])
                    c.db.execute('UPDATE remote_guarded_runs SET '+column+'=? WHERE run_id=?', (before[column], self.oldrun['run_id']))

    def test_close_after_preview_requires_fresh_exact_preview(self):
        with contextlib.closing(Coordinator(self.db)) as c:
            old_stamp = self.preview(c)
            self.close_observed_old_child()
            with self.assertRaisesRegex(CoordError, 'prestate/FIFO/guard generation changed'):
                with c.tx(): self.apply(c, old_stamp)
            self.assertEqual(c.db.execute('SELECT count(*) FROM execution_epochs').fetchone()[0], 0)
            with c.tx(): self.apply(c)

    def test_closed_run_does_not_waive_native_fence_or_attestation_freshness(self):
        self.close_observed_old_child()
        from agent_chat.core import execution_epoch_handoff
        with contextlib.closing(Coordinator(self.db)) as c:
            original = c.db.execute('SELECT observed_at FROM execution_epoch_attestations WHERE id=?', (self.attestation['id'],)).fetchone()[0]
            # Immutable producer rows cannot be edited; advance only the consumer clock.
            with mock.patch('agent_chat.expired_claim_disposition.time.time', return_value=original + 6):
                with self.assertRaisesRegex(CoordError, 'producer observation expired'):
                    with c.tx(): self.apply(c)
            with self.assertRaisesRegex(CoordError, 'prepared gate'):
                with c.tx(): execution_epoch_handoff(c.db, 'turn/start', self.old_thread, 'a', 'mac')
            self.assertEqual(c.db.execute('SELECT count(*) FROM execution_epochs').fetchone()[0], 0)

    def test_preboot_and_unknown_legacy_fail_closed(self):
        from agent_chat.core import CoordError
        with contextlib.closing(Coordinator(self.db)) as c:
            c.db.execute('UPDATE remote_guarded_runs SET started_at=? WHERE run_id=?',(time.time(),self.oldrun['run_id']))
            with self.assertRaisesRegex(CoordError,'in-flight|boot ordering'):
                with c.tx(): self.apply(c)
            c.db.execute('DELETE FROM remote_guarded_runs WHERE run_id=?',(self.oldrun['run_id'],))
            with self.assertRaisesRegex(CoordError,'boot ordering'):
                with c.tx(): self.apply(c)
            self.assertEqual(c.db.execute('SELECT count(*) FROM execution_epochs').fetchone()[0],0)

    def test_fresh_canonical_then_workspace_rights_and_return_order(self):
        from agent_chat.core import CoordError
        self.close_observed_old_child()
        with contextlib.closing(Coordinator(self.db)) as c:
            with c.tx(): self.apply(c)
        with self.assertRaises(RemoteCoordError):
            self.call('begin-guard',session='b',resource='workspace:/fixture/new',token=self.current_claim['token'])
        canonical=self.call('request',session='b',resource='validation-clone',minutes=5)
        self.assertEqual(canonical['current_execution_epoch']['workspace_resource'],'workspace:/fixture/new')
        self.assertEqual(canonical['current_execution_epoch']['old_physical_state'],'UNVERIFIED')
        with self.assertRaises(RemoteCoordError):
            self.call('begin-guard',session='b',resource='workspace:/fixture/new',token=self.current_claim['token'])
        self.call('close-guard',session='b',run_id=self.run['run_id'],token=self.current_claim['token'],evidence_sha256=hashlib.sha256(b'fixture known prelaunch instance; no process started').hexdigest())
        receipt=dict(version=2,resource='workspace:/fixture/new',reservation_id=self.current_claim['reservation_id'],restored=True,processes_closed=True,closed_at=time.time(),evidence='fixture-known-unlaunched-scope',pids=[],process_closures=[])
        self.call('release',session='b',resource='workspace:/fixture/new',token=self.current_claim['token'],receipt=receipt,evidence_base64=base64.b64encode(b'Fixture current backing never launched or changed; no process started.').decode())
        fresh=self.call('request',session='b',resource='workspace:/fixture/new',minutes=5)
        self.assertNotEqual(fresh['reservation_id'],self.current_claim['reservation_id'])
        run=self.call('begin-guard',session='b',resource='workspace:/fixture/new',token=fresh['token'])
        canonical_run=self.call('begin-guard',session='b',resource='validation-clone',token=canonical['token'])
        self.call('close-guard',session='b',run_id=canonical_run['run_id'],token=canonical['token'],evidence_sha256=hashlib.sha256(b'fixture paired canonical guard never launched').hexdigest())
        canonical_receipt=dict(version=2,resource='validation-clone',reservation_id=canonical['reservation_id'],restored=True,processes_closed=True,closed_at=time.time(),evidence='fixture-new-role-not-launched',pids=[],process_closures=[])
        with self.assertRaisesRegex(RemoteCoordError,'native backing guards closed'):
            self.call('release',session='b',resource='validation-clone',token=canonical['token'],receipt=canonical_receipt,evidence_base64=base64.b64encode(b'Fixture canonical role had no launched command.').decode())
        self.call('close-guard',session='b',run_id=run['run_id'],token=fresh['token'],evidence_sha256=hashlib.sha256(b'fixture admitted run never attached or launched').hexdigest())
        with self.assertRaisesRegex(RemoteCoordError,'predates current backing closure'):
            self.call('release',session='b',resource='validation-clone',token=canonical['token'],receipt=canonical_receipt,evidence_base64=base64.b64encode(b'Fixture canonical role had no launched command.').decode())
        canonical_receipt['closed_at']=time.time()
        self.call('release',session='b',resource='validation-clone',token=canonical['token'],receipt=canonical_receipt,evidence_base64=base64.b64encode(b'Fixture canonical role had no launched command.').decode())
