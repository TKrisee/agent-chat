"""Trusted enrollment and real guarded process lifecycle regressions."""
import contextlib
import base64
import hashlib
import io
import os
from pathlib import Path
import sys
import subprocess
import time
import uuid
from unittest import mock

from agent_chat import cli
from agent_chat.core import Coordinator, CoordError
from agent_chat.lifecycle_producer import credential, main as enrollment_main
from agent_chat.private_artifacts import atomic_capture
from agent_chat.remote import HttpClient, RemoteCoordError
from test_remote_web import RemoteWebFixture, TOKEN


class LifecycleProducerTests(RemoteWebFixture):
    def setUp(self):
        super().setUp()
        self.call('register', agent='worker')
        self.claim = self.call('request', resource='shared', minutes=5)
        self.capability_path = Path(self.tmp.name) / 'trusted-capability'
        with contextlib.redirect_stdout(io.StringIO()):
            enrollment_main(['--db', str(self.db), '--owner', 'a', '--host', 'mac',
                             '--capability-file', str(self.capability_path)])
        self.auth = {'capability': credential(self.capability_path)}

    def run_capability(self):
        path = Path(self.tmp.name) / "reservation-capability"
        atomic_capture(path, dict(resource="shared", session_id="a", host_id="mac",
                                  reservation_id=self.claim["reservation_id"], token=self.claim["token"],
                                  server=self.url, project="default"))
        return path

    def begin(self):
        return self.call('begin-guard', resource='shared', token=self.claim['token'])

    def establish(self, run, **extra):
        return self.call('producer-establish', **dict(self.auth, run_id=run['run_id'], **extra))

    def test_enrollment_credentials_owner_host_and_late_start_are_enforced(self):
        run = self.begin()
        with self.assertRaisesRegex(RemoteCoordError, 'capability'):
            self.call('producer-establish', run_id=run['run_id'], capability='wrong')
        self.call('register', session='b', agent='peer')
        with self.assertRaisesRegex(RemoteCoordError, 'owner host'):
            self.call('producer-establish', session='b', run_id=run['run_id'], **self.auth)
        first = self.establish(run)
        self.assertEqual(self.establish(run)['id'], first['id'])
        second = self.begin()
        self.call('attach-guard-pid', run_id=second['run_id'], token=self.claim['token'], pid=os.getpid())
        with self.assertRaisesRegex(RemoteCoordError, 'unlaunched'):
            self.establish(second)
        third = self.begin()
        with contextlib.closing(Coordinator(self.db)) as coord:
            coord.db.execute('UPDATE lifecycle_producers SET enrolled_at=?', (time.time() + 1,))
        with self.assertRaisesRegex(RemoteCoordError, 'prior enrollment'):
            self.establish(third)

    def test_closure_is_exact_immutable_and_requires_established_instance(self):
        run = self.begin(); instance = self.establish(run)
        self.call('attach-guard-pid', run_id=run['run_id'], token=self.claim['token'], pid=os.getpid())
        values = dict(self.auth, run_id=run['run_id'], instance_id=instance['id'], pid=os.getpid(), returncode=0, group_closed=True)
        for replacement in ({'instance_id': 'other'}, {'pid': os.getpid()+1}, {'group_closed': False}):
            with self.assertRaisesRegex(RemoteCoordError, 'observation'):
                self.call('producer-close', **dict(values, **replacement))
        result = self.call('producer-close', **values)
        self.call('close-guard', run_id=run['run_id'], token=self.claim['token'], evidence_sha256=result['sha256'])
        self.assertEqual(self.call('producer-close', **values), result)
        with self.assertRaisesRegex(RemoteCoordError, 'immutable'):
            self.call('producer-close', **dict(values, returncode=1))
        with contextlib.closing(Coordinator(self.db)) as coord:
            self.assertEqual(coord.db.execute('SELECT count(*) FROM current_instance_closures').fetchone()[0], 1)
            self.assertIsNotNone(coord.db.execute('SELECT closed_at FROM remote_guarded_runs WHERE run_id=?', (run['run_id'],)).fetchone()[0])
        unobserved = self.begin(); unseen_instance = self.establish(unobserved)
        self.call('attach-guard-pid', run_id=unobserved['run_id'], token=self.claim['token'], pid=os.getpid())
        self.call('close-guard', run_id=unobserved['run_id'], token=self.claim['token'], evidence_sha256='a' * 64)
        with self.assertRaisesRegex(RemoteCoordError, 'retrospectively'):
            self.call('producer-close', **dict(values, run_id=unobserved['run_id'], instance_id=unseen_instance['id']))

    def test_real_launcher_closes_descendants_without_exposing_capability_to_child(self):
        child_pid = Path(self.tmp.name) / 'descendant-pid'
        script = "import os,time; assert 'AGENT_CHAT_PRODUCER_CAPABILITY_FILE' not in os.environ; pid=os.fork(); open(" + repr(str(child_pid)) + ", 'w').write(str(pid)) if pid else time.sleep(60)"
        env = dict(os.environ, AGENT_CHAT_SERVER=self.url, AGENT_CHAT_API_TOKEN=TOKEN,
                   AGENT_CHAT_SESSION='a', AGENT_CHAT_HOST_ID='mac', AGENT_CHAT_TOKEN=self.claim['token'],
                   AGENT_CHAT_PRODUCER_CAPABILITY_FILE=str(self.capability_path),
                   AGENT_CHAT_STATE_DIR=str(Path(self.tmp.name) / 'client'))
        with mock.patch.dict(os.environ, env, clear=True), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(cli.main(['run', '--capability-file', str(self.run_capability()), '--reservation-id', self.claim['reservation_id'], 'shared', '--', sys.executable, '-c', script]), 0)
        with contextlib.closing(Coordinator(self.db)) as coord:
            fact = coord.db.execute('SELECT c.*,i.run_id FROM current_instance_closures c JOIN execution_instances i ON i.id=c.instance_id').fetchone()
            self.assertIsNotNone(fact)
            self.assertIn(b'returncode=0\ngroup_closed=true', bytes(fact['evidence']))
            self.assertIsNotNone(coord.db.execute('SELECT closed_at FROM remote_guarded_runs WHERE run_id=?', (fact['run_id'],)).fetchone()[0])
        pid = int(child_pid.read_text())
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)

    def test_run_rejects_wrong_reservation_tuple_before_launch(self):
        marker = Path(self.tmp.name) / 'should-not-start'
        env = dict(os.environ, AGENT_CHAT_SERVER=self.url, AGENT_CHAT_API_TOKEN=TOKEN,
                   AGENT_CHAT_SESSION='a', AGENT_CHAT_HOST_ID='mac',
                   AGENT_CHAT_PRODUCER_CAPABILITY_FILE=str(self.capability_path),
                   AGENT_CHAT_STATE_DIR=str(Path(self.tmp.name) / 'client'))
        command = ['run', '--capability-file', str(self.run_capability()), '--reservation-id', 'wrong-reservation',
                   'shared', '--', sys.executable, '-c', 'open(' + repr(str(marker)) + ', "w").write("started")']
        with mock.patch.dict(os.environ, env, clear=True):
            with self.assertRaisesRegex(CoordError, 'capability tuple does not match'):
                cli.main(command)
        self.assertFalse(marker.exists())
        with contextlib.closing(Coordinator(self.db)) as coord:
            self.assertEqual(coord.db.execute('SELECT count(*) FROM remote_guarded_runs').fetchone()[0], 0)

    def test_admin_enroll_launch_read_independent_review_and_qualified_disposition(self):
        env = dict(os.environ, AGENT_CHAT_SERVER=self.url, AGENT_CHAT_API_TOKEN=TOKEN,
                   AGENT_CHAT_SESSION='a', AGENT_CHAT_HOST_ID='mac', AGENT_CHAT_TOKEN=self.claim['token'],
                   AGENT_CHAT_PRODUCER_CAPABILITY_FILE=str(self.capability_path),
                   AGENT_CHAT_STATE_DIR=str(Path(self.tmp.name) / 'client'))
        original_call = HttpClient.call
        def disconnect_close(client, path, payload, **kwargs):
            if payload.get('op') == 'close-guard':
                raise RemoteCoordError('ordinary guard close disconnected')
            return original_call(client, path, payload, **kwargs)
        with mock.patch.dict(os.environ, env, clear=True), mock.patch.object(HttpClient, 'call', disconnect_close):
            with self.assertRaisesRegex(RemoteCoordError, 'disconnected'):
                cli.main(['run', '--capability-file', str(self.run_capability()), '--reservation-id', self.claim['reservation_id'], 'shared', '--', sys.executable, '-c', 'pass'])
        with contextlib.closing(Coordinator(self.db)) as coord:
            run = dict(coord.db.execute('SELECT * FROM remote_guarded_runs').fetchone())
        status = self.call('producer-status', run_id=run['run_id'], **self.auth)
        self.assertIsNotNone(status['instance']); self.assertIsNotNone(status['closure'])
        with self.assertRaisesRegex(RemoteCoordError, 'capability'):
            self.call('producer-status', run_id=run['run_id'], capability='wrong')
        output = io.StringIO()
        with mock.patch.dict(os.environ, env, clear=True), contextlib.redirect_stdout(output):
            self.assertEqual(cli.main(['producer-status', '--run-id', run['run_id']]), 0)
        fact_id = subprocess.run(['jq', '-er', '.closure.id'], input=output.getvalue(),
                                 text=True, capture_output=True, check=True).stdout.strip()
        self.assertEqual(fact_id, status['closure']['id'])
        self.assertNotIn(self.auth['capability'], output.getvalue())
        self.call('register', session='root', agent='root')
        self.call('register', session='po', agent='reviewer')
        with contextlib.closing(Coordinator(self.db)) as coord:
            from agent_chat.bridge_state import BridgeState
            BridgeState(coord)
            coord.db.execute('''CREATE TABLE IF NOT EXISTS root_projections (
                session_id TEXT PRIMARY KEY,home_project TEXT NOT NULL,home_session TEXT NOT NULL,
                thread_id TEXT NOT NULL,cwd TEXT,paused INTEGER NOT NULL DEFAULT 0,model TEXT,effort TEXT)''')
            coord.db.execute('CREATE TABLE IF NOT EXISTS project_orchestrators(session_id TEXT PRIMARY KEY,root_session TEXT NOT NULL)')
            coord.db.execute('INSERT INTO root_projections(session_id,home_project,home_session,thread_id,cwd) VALUES(?,?,?,?,?)',
                             ('root', 'default', 'root', 'root-thread', self.tmp.name))
            coord.db.execute('INSERT INTO project_orchestrators VALUES(?,?)', ('po', 'root'))
            coord.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?,?,?)', ('po','po-thread','root','/po',time.time()))
            coord.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?,?,?)', ('a','worker-thread','po','/po/worker',time.time()))
            coord.db.execute('UPDATE resources SET deadline=?,reason=? WHERE name=?',
                             (time.time()-1, 'OPEN: original close result lost', 'shared'))
        instance, fact = status['instance'], status['closure']
        def canonical(program, **values):
            command = ['jq', '-cnS']
            for key, value in values.items():
                command += ['--arg', key, value]
            return subprocess.run(command + [program], capture_output=True, check=True).stdout.strip()
        original = dict(resource='shared', reservation_id=self.claim['reservation_id'], owner_session='a',
                        remote_run_id=run['run_id'], refusal='OPEN: original close result lost')
        closure_bytes = canonical('{version:1,kind:"qualified-current-closure",original:{resource:$resource,reservation_id:$rid,owner_session:"a",remote_run_id:$run,refusal:$refusal},current:{instance_id:$instance,closure_fact_id:$fact,host_id:"mac",closure_authority:$producer,closure_digest:$digest},known_instances:[$instance]}',
                                  resource='shared', rid=self.claim['reservation_id'], run=run['run_id'], refusal=original['refusal'],
                                  instance=instance['id'], fact=fact['id'], producer=instance['producer_id'], digest=fact['sha256'])
        def pin(sender, content):
            message = self.call('send', session=sender, to='root', body='Independent evidence',
                                attachments=[{'name':'evidence.txt','content_base64':base64.b64encode(content).decode()}])
            return {'attachment_id':message['attachments'][0]['id'], 'sha256':hashlib.sha256(content).hexdigest()}
        current_pin = pin('root', closure_bytes)
        review_bytes = canonical('{version:1,kind:"qualified-current-review",original:{resource:"shared",reservation_id:$rid,owner_session:"a",remote_run_id:$run,refusal:$refusal},closure_attachment_id:$attachment,closure_packet_sha256:$digest}',
                                 rid=self.claim['reservation_id'], run=run['run_id'], refusal=original['refusal'],
                                 attachment=current_pin['attachment_id'], digest=current_pin['sha256'])
        review_pin = pin('po', review_bytes)
        for message in self.call('inbox', session='root')['messages']:
            self.call('message', session='root', id=message['id'])
        evidence = {'qualified_current_disposition':dict(original_reservation_id=self.claim['reservation_id'], original_run_id=run['run_id'],
                    original_refusal=original['refusal'], current_closure=current_pin, reviewer_closure=review_pin,
                    historical_uncertainty=['Only enrolled instance exit and process group closure were observed.'])}
        result = self.call('admin-recover', session='root', project=self.server.projects.get()['id'], resource='shared',
                           reservation_id=self.claim['reservation_id'], owner_session='a', request_id=str(uuid.uuid4()),
                           operator='test administrator', reason='Qualified current closure', evidence=evidence)
        self.assertEqual(result['outcome'], 'qualified_released')
        with contextlib.closing(Coordinator(self.db)) as coord:
            self.assertIsNone(coord.db.execute('SELECT owner_session FROM resources WHERE name=?', ('shared',)).fetchone()[0])
            self.assertIsNone(coord.db.execute('SELECT closed_at FROM remote_guarded_runs WHERE run_id=?', (run['run_id'],)).fetchone()[0])
            self.assertEqual(coord.db.execute('SELECT state FROM remote_reservation_outcomes WHERE reservation_id=?', (self.claim['reservation_id'],)).fetchone()[0], 'pending')
            self.assertIsNone(coord.db.execute('SELECT 1 FROM receipts WHERE reservation_id=?', (self.claim['reservation_id'],)).fetchone())


class ExecutionEpochWorkflowTests(RemoteWebFixture):
    """Populated real HTTP/store fixture; no host observer or native RPC used."""
    def setUp(self):
        super().setUp()
        from agent_chat import service
        base=Path(self.tmp.name)
        config=service.InstalledServerConfig(base/'server.plist',base/'wrapper',base/'server-bin',
            self.db,base,os.getuid(),'127.0.0.1',8765)
        provider=mock.patch.object(service,'INSTALLED_SERVER_CONFIG',config)
        provider.start(); self.addCleanup(provider.stop)
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

    def test_real_receiver_fifo_old_open_replay_and_new_ordinary_guard(self):
        from agent_chat.core import CoordError
        with contextlib.closing(Coordinator(self.db)) as c:
            old=tuple(c.db.execute('SELECT * FROM remote_guarded_runs WHERE run_id=?',(self.oldrun['run_id'],)).fetchone())
            queue=tuple(tuple(r) for r in c.db.execute('SELECT * FROM resource_queue ORDER BY seq'))
            with c.tx():
                stamp=self.preview(c);result=self.apply(c,stamp)
            self.assertIn(('old_physical_state','UNVERIFIED'),result)
            self.assertEqual(tuple(c.db.execute('SELECT * FROM remote_guarded_runs WHERE run_id=?',(self.oldrun['run_id'],)).fetchone()),old)
            self.assertEqual(tuple(tuple(r) for r in c.db.execute('SELECT * FROM resource_queue ORDER BY seq')),queue)
            with c.tx(): self.assertIn(('replayed',True),self.apply(c,stamp))
            with self.assertRaisesRegex(CoordError,'replay conflicts'):
                with c.tx(): self.apply(c,stamp,authority_digest='different')
            with self.assertRaises(Exception): c.db.execute('DELETE FROM execution_epochs')
        with self.assertRaises(RemoteCoordError): self.call('check',resource='validation-clone',token=self.old['token'])
        with self.assertRaisesRegex(RemoteCoordError,'unresolved open guarded run'):
            self.call('request',session='b',resource='validation-clone',minutes=5)
        # Same old owner may still do unrelated ordinary guarded work.
        other=self.call('request',resource='unrelated',minutes=5)
        self.call('begin-guard',resource='unrelated',token=other['token'])

    def test_real_receiver_admits_fresh_guard_after_observed_old_child_exit(self):
        self.close_observed_old_child()
        with contextlib.closing(Coordinator(self.db)) as c:
            with c.tx(): self.apply(c)
        fresh=self.call('request',session='b',resource='validation-clone',minutes=5)
        self.assertEqual(fresh['state'],'owned')
        self.assertNotEqual(fresh['token'],self.old['token'])
        with self.assertRaisesRegex(RemoteCoordError,'workspace rights'):
            self.call('begin-guard',session='b',resource='validation-clone',token=fresh['token'])

    def test_authenticated_observer_wrong_host_backing_and_freshness(self):
        with self.assertRaises(RemoteCoordError): self.attest(backing='/wrong/current/backing:1:12')
        with self.assertRaises(RemoteCoordError): self.attest(backing='unsigned-backing-string')
        with self.assertRaises(RemoteCoordError): self.attest(execution_id='worker-invented-execution')
        with self.assertRaises(RemoteCoordError): self.attest(observed_at=time.time()-20)
        with self.assertRaises(RemoteCoordError):
            self.call('producer-epoch-attest',session='b',host='other',run_id=self.run['run_id'],instance_id=self.instance['id'],request_id=str(uuid.uuid4()),observation=self.observation,**self.auth)
        with self.assertRaises(RemoteCoordError):
            self.call('producer-epoch-attest',session='b',run_id=self.run['run_id'],instance_id=self.instance['id'],request_id=str(uuid.uuid4()),observation=self.observation,capability='unsigned-worker-assertion')

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

    def test_active_racing_handoff_then_old_managed_relaunch_is_fenced(self):
        from agent_chat.core import CoordError,execution_epoch_handoff,execution_epoch_native_check
        with contextlib.closing(Coordinator(self.db)) as c:
            with self.assertRaisesRegex(CoordError,'prepared gate'):
                with c.tx(): execution_epoch_handoff(c.db,'turn/start',self.old_thread,'a','mac')
            with c.tx(): self.apply(c)
            with self.assertRaises(CoordError): execution_epoch_native_check(c.db,thread=self.old_thread,actor='a')
            execution_epoch_native_check(c.db,thread=self.newthread,actor='a')

    def test_precommit_interruption_rolls_back_and_fifo_drift_refuses(self):
        from agent_chat.core import CoordError
        with contextlib.closing(Coordinator(self.db)) as c:
            stamp=self.preview(c)
            c.db.execute("UPDATE resource_queue SET queued_at=queued_at+1 WHERE resource='validation-clone'")
            with self.assertRaisesRegex(CoordError,'prestate/FIFO'):
                with c.tx(): self.apply(c,stamp)
            c.db.execute("CREATE TRIGGER fixture_interrupt BEFORE UPDATE ON resources WHEN OLD.name='validation-clone' BEGIN SELECT RAISE(ABORT,'fixture interrupted transaction'); END")
            with self.assertRaises(Exception):
                with c.tx(): self.apply(c)
            self.assertEqual(c.db.execute('SELECT count(*) FROM execution_epochs').fetchone()[0],0)
            self.assertEqual(c.db.execute("SELECT owner_session FROM resources WHERE name='validation-clone'").fetchone()[0],'a')

    def test_unknown_real_backing_collector_has_no_fixture_or_pid_fallback(self):
        from agent_chat.lifecycle_producer import collect_epoch_observation
        from agent_chat.core import CoordError
        with mock.patch('subprocess.run',side_effect=AssertionError('must not query a host')):
            with self.assertRaisesRegex(CoordError,'backing observation unavailable'):
                collect_epoch_observation(canonical_resource='validation-clone',old_reservation=self.old['reservation_id'],root_generation='x',old_backing_path=None,current_backing_path=None,execution_id='x')


    def test_current_launch_and_authenticated_wrong_selector_cannot_transition(self):
        from agent_chat.core import CoordError
        with self.assertRaisesRegex(RemoteCoordError,'prepared scope'):
            self.attest(root_generation='different-configured-root')
        with self.assertRaisesRegex(RemoteCoordError,'prepared gate'):
            self.call('attach-guard-pid',session='b',run_id=self.run['run_id'],token=self.current_claim['token'],pid=os.getpid())
        with self.assertRaisesRegex(RemoteCoordError,'prepared gate'):
            self.call('begin-guard',session='b',resource='workspace:/fixture/new',token=self.current_claim['token'])
        with self.assertRaisesRegex(RemoteCoordError,'mutable-leaf'):
            self.call('request',session='b',resource='file:workspaces/fixture-new/Assets/new.cs',minutes=5)
        # Unrelated same-owner work and ordinary messages remain possible.
        other=self.call('request',session='b',resource='file:unrelated/current.txt',minutes=5)
        self.call('begin-guard',session='b',resource='file:unrelated/current.txt',token=other['token'])

    def test_normal_configured_root_effect_cannot_promote_observation_authority(self):
        with self.assertRaises(RemoteCoordError):
            self.client.call('/api/root/rpc',dict(op='epoch-apply',session='b',host_id='mac',params=dict(self.auth,request_id=self.request)))
        with self.assertRaises(RemoteCoordError):
            self.call('producer-epoch-attest',session='b',run_id=self.run['run_id'],instance_id=self.instance['id'],request_id=str(uuid.uuid4()),observation=self.observation,prepare_id=str(uuid.uuid4()),gitdir='/fixture/gitdir:1:99',leaf_prefix='file:workspaces/fixture-new/',index_resource='file:git/worktrees/fixture-new/index',native_processes=[],**self.auth)

    def test_accepted_old_queue_is_unknown_and_never_deleted_for_transition(self):
        from agent_chat.core import CoordError
        with contextlib.closing(Coordinator(self.db)) as c:
            # Exact populated fake-native accepted queue state; no auth/enrollment seed.
            c.db.execute('INSERT INTO bridge_jobs(id,thread_id,payload,status,queue_id,error,created_at,updated_at) VALUES(?,?,?,?,?,NULL,?,?)',('fixture-accepted',self.old_thread,'synthetic fixture action','queued','fixture-native-q',time.time(),time.time()))
            with self.assertRaisesRegex(CoordError,'unsettled old'):
                with c.tx(): self.apply(c)
            self.assertEqual(c.db.execute('SELECT queue_id FROM bridge_jobs WHERE id=?',('fixture-accepted',)).fetchone()[0],'fixture-native-q')
            self.assertEqual(c.db.execute('SELECT count(*) FROM execution_epochs').fetchone()[0],0)


    def test_transient_reset_thread_does_not_evade_original_epoch_scope(self):
        from agent_chat.bridge_client import RemoteBridgeState
        from agent_chat.core import CoordError
        identifier=str(uuid.uuid4());new_thread=str(uuid.uuid4())
        with contextlib.closing(Coordinator(self.db)) as c:
            c.db.execute('INSERT INTO session_resets(id,requester_session,target_session,host_id,old_thread_id,new_thread_id,prompt,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)',(identifier,'a','a','mac',self.old_thread,new_thread,'synthetic existing reset','created',time.time(),time.time()))
            with self.assertRaisesRegex(CoordError,'unsettled old'):
                with c.tx(): self.apply(c)
        state=RemoteBridgeState(self.client,self.url,{'owner':'fixture-existing-dispatcher','secret':'x'*40,'host_id':'mac'})
        state.call('acquire')
        try:
            with self.assertRaisesRegex(RemoteCoordError,'prepared gate'):
                state.call('execution-epoch-begin',method='thread/queue/add',thread=new_thread,actor='a',job_id=identifier)
        finally:
            state.call('release')


    def test_current_only_collector_observes_fake_host_without_old_backing(self):
        from agent_chat.lifecycle_producer import collect_epoch_observation
        from types import SimpleNamespace
        import shlex
        project=Path(self.tmp.name)/'current-physical';project.mkdir()
        gitdir=Path(self.tmp.name)/'current-gitdir';gitdir.mkdir()
        (project/'.git').write_text('gitdir: '+str(gitdir)+'\n')
        boot=int(time.time()-500);boot_id=str(uuid.uuid4())
        def observed(args,**kwargs):
            if args==['/usr/sbin/sysctl','-n','kern.boottime']: return SimpleNamespace(stdout='{ sec = '+str(boot)+', usec = 0 }',returncode=0)
            if args==['/usr/sbin/sysctl','-n','kern.bootsessionuuid']: return SimpleNamespace(stdout=boot_id,returncode=0)
            if args[:3]==['/usr/bin/pgrep','-f',__import__('re').escape(str(project.resolve()))]: return SimpleNamespace(stdout='',returncode=1)
            raise AssertionError('unexpected observer command')
        with mock.patch('subprocess.run',side_effect=observed):
            observation,physical_gitdir,processes=collect_epoch_observation(canonical_resource='validation-clone',old_reservation=self.old['reservation_id'],root_generation='fixture-root-generation',current_backing_path=project,execution_id=self.instance['id'])
        self.assertEqual(processes,[])
        self.assertEqual(observation['old_backing'],'UNVERIFIED:separate-old-domain')
        self.assertEqual(observation['boot_id'],boot_id)
        self.assertTrue(physical_gitdir.startswith(str(gitdir.resolve())+':'))

    def test_observed_current_writer_expired_prepare_and_scope_tamper_refuse(self):
        from agent_chat.core import CoordError
        self.attestation=self.call('producer-epoch-attest',session='b',run_id=self.run['run_id'],instance_id=self.instance['id'],request_id=str(uuid.uuid4()),observation=dict(self.observation,observed_at=time.time()),prepare_id=self.request,gitdir='/fixture/gitdir:1:99',leaf_prefix='file:workspaces/fixture-new/',index_resource='file:git/worktrees/fixture-new/index',native_processes=['123:fixture birth:fixture command digest'],**self.auth)
        with contextlib.closing(Coordinator(self.db)) as c:
            with self.assertRaisesRegex(CoordError,'zero-writer'):
                with c.tx(): self.apply(c)
            with self.assertRaises(Exception): c.db.execute('UPDATE execution_epoch_prepares SET workspace=? WHERE id=?',('workspace:/different',self.request))
            with self.assertRaises(Exception): c.db.execute('DELETE FROM execution_epoch_prepare_events')
        self.attestation=self.attest()
        with mock.patch('agent_chat.expired_claim_disposition.time.time',return_value=time.time()+130):
            with contextlib.closing(Coordinator(self.db)) as c:
                with self.assertRaises(CoordError):
                    with c.tx(): self.apply(c)

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


    def test_prepared_gate_covers_batch_restoration_and_pending_next_task(self):
        current=dict(resource='workspace:/fixture/new',session_id='b',host_id='mac',reservation_id=self.current_claim['reservation_id'],token=self.current_claim['token'])
        for restore in (False,True):
            with self.assertRaisesRegex(RemoteCoordError,'prepared gate'):
                self.call('begin-guard-batch',session='b',tuples=[current],restore=restore)
        with self.assertRaisesRegex(RemoteCoordError,'prepared gate'):
            self.call('begin-guard',resource='validation-clone',token=self.old['token'],restore=True,reservation_id=self.old['reservation_id'],max_seconds=10)
        message=self.call('send',to='new-worker',body='Synthetic accepted next manifest; new native rights required.')
        inbox=self.call('message',session='b',id=message['id'])
        self.assertIn(message['id'],repr(inbox))
        with self.assertRaisesRegex(RemoteCoordError,'prepared gate'):
            self.call('begin-guard',session='b',resource='workspace:/fixture/new',token=self.current_claim['token'])
        with contextlib.closing(Coordinator(self.db)) as c:
            self.assertIsNone(c.db.execute('SELECT acked_at FROM messages WHERE id=?',(message['id'],)).fetchone()[0])
            self.assertEqual(c.db.execute('SELECT count(*) FROM execution_epochs').fetchone()[0],0)

    def test_mutable_leaf_owner_conflict_refuses_new_root_prepare_without_effect(self):
        from agent_chat.expired_claim_disposition import _prepare_execution_epoch
        from agent_chat.core import CoordError
        with contextlib.closing(Coordinator(self.db)) as c:
            # Cancel the fixture's actual Root preparation through its typed phase;
            # no underlying allocation/queue/guard is cleared.
            with c.tx():
                c.db.execute("UPDATE execution_epoch_prepares SET state='cancelled' WHERE id=?",(self.request,))
                c.db.execute('INSERT INTO execution_epoch_prepare_events VALUES(?,?,?,?)',(self.request,'cancelled',time.time(),'fixture-root-cancel-authority'))
        leaf=self.call('request',session='b',resource='file:workspaces/fixture-new/Assets/busy.cs',minutes=5)
        with contextlib.closing(Coordinator(self.db)) as c:
            with self.assertRaisesRegex(CoordError,'mutable leaf ownership'):
                with c.tx():
                    _prepare_execution_epoch(c.db,self.selected,request_id=str(uuid.uuid4()),instance_id=self.instance['id'],workspace='workspace:/fixture/new',leaf_prefix='file:workspaces/fixture-new/',gitdir='/fixture/gitdir',index_resource='file:git/worktrees/fixture-new/index',root_generation='fixture-root-generation',host='mac',authority_digest='fixture-root-authority')
            self.assertEqual(c.db.execute('SELECT reservation_id FROM resources WHERE name=?',(leaf['resource'],)).fetchone()[0],leaf['reservation_id'])



    def test_exact_index_gate_and_native_directory_noninheritance(self):
        from agent_chat.core import CoordError
        index='file:git/worktrees/fixture-new/index'
        leaf='file:workspaces/fixture-new/asset.txt'
        for target in (index,leaf):
            with self.assertRaises(RemoteCoordError):
                self.call('request',session='b',resource=target,minutes=5)
            with self.assertRaises(RemoteCoordError):
                self.call('begin-guard',session='b',resource=target,token=self.current_claim['token'])
        with contextlib.closing(Coordinator(self.db)) as c:
            with c.tx():
                c.db.execute("UPDATE execution_epoch_prepares SET state='cancelled' WHERE id=?",(self.request,))
        directory=self.call('request',session='b',resource='file:workspaces/fixture-new',minutes=5)
        for target in (index,leaf):
            with self.assertRaises(RemoteCoordError):
                self.call('begin-guard',session='b',resource=target,token=directory['token'])
        exact=self.call('request',session='b',resource=index,minutes=5)
        run=self.call('begin-guard',session='b',resource=index,token=exact['token'])
        self.call('close-guard',session='b',run_id=run['run_id'],token=exact['token'],evidence_sha256=hashlib.sha256(b'fixture exact index guard never launched').hexdigest())

    def test_owned_separate_index_blocks_prepare_without_clearing_it(self):
        from agent_chat.expired_claim_disposition import _prepare_execution_epoch
        from agent_chat.core import CoordError
        with contextlib.closing(Coordinator(self.db)) as c:
            with c.tx():
                c.db.execute("UPDATE execution_epoch_prepares SET state='cancelled' WHERE id=?",(self.request,))
        index=self.call('request',session='b',resource='file:git/worktrees/fixture-new/index',minutes=5)
        with contextlib.closing(Coordinator(self.db)) as c:
            with self.assertRaisesRegex(CoordError,'mutable leaf ownership'):
                with c.tx():
                    _prepare_execution_epoch(c.db,self.selected,request_id=str(uuid.uuid4()),instance_id=self.instance['id'],workspace='workspace:/fixture/new',leaf_prefix='file:workspaces/fixture-new/',gitdir='/fixture/gitdir',index_resource=index['resource'],root_generation='fixture-root-generation',host='mac',authority_digest='fixture-root-authority')
            self.assertEqual(c.db.execute('SELECT reservation_id FROM resources WHERE name=?',(index['resource'],)).fetchone()[0],index['reservation_id'])

    def return_fixture_workspace(self,claim,session,run=None):
        if run:
            self.call('close-guard',session=session,run_id=run['run_id'],token=claim['token'],evidence_sha256=hashlib.sha256(b'fixture role never launched a child').hexdigest())
        proof=dict(version=2,resource='workspace:/fixture/new',reservation_id=claim['reservation_id'],restored=True,processes_closed=True,closed_at=time.time(),evidence='fixture-known-unchanged-role',pids=[],process_closures=[])
        self.call('release',session=session,resource='workspace:/fixture/new',token=claim['token'],receipt=proof,evidence_base64=base64.b64encode(b'Fixture role had no source/native effects or process.').decode())

    def test_both_remote_entry_paths_require_same_owner_fresh_pair_atomically(self):
        self.close_observed_old_child()
        with contextlib.closing(Coordinator(self.db)) as c:
            with c.tx(): self.apply(c)
        canonical=self.call('request',session='b',resource='validation-clone',minutes=5)
        tuple_for=lambda claim:dict(resource=claim['resource'],session_id='b',host_id='mac',reservation_id=claim['reservation_id'],token=claim['token'])
        for resource,claim in (('validation-clone',canonical),('workspace:/fixture/new',self.current_claim)):
            for restore in (False,True):
                with self.assertRaisesRegex(RemoteCoordError,'workspace rights'):
                    self.call('begin-guard',session='b',resource=resource,token=claim['token'],restore=restore,reservation_id=claim['reservation_id'])
                with self.assertRaisesRegex(RemoteCoordError,'workspace rights'):
                    self.call('begin-guard-batch',session='b',tuples=[tuple_for(claim)],restore=restore)
        self.return_fixture_workspace(self.current_claim,'b',self.run)
        self.call('register',session='peer',agent='different-native-owner')
        peer=self.call('request',session='peer',resource='workspace:/fixture/new',minutes=5)
        with self.assertRaisesRegex(RemoteCoordError,'workspace rights'):
            self.call('begin-guard',session='b',resource='validation-clone',token=canonical['token'])
        with self.assertRaisesRegex(RemoteCoordError,'canonical FIFO rights'):
            self.call('begin-guard',session='peer',resource='workspace:/fixture/new',token=peer['token'])
        self.return_fixture_workspace(peer,'peer')
        fresh=self.call('request',session='b',resource='workspace:/fixture/new',minutes=5)
        batch=self.call('begin-guard-batch',session='b',tuples=[tuple_for(canonical),tuple_for(fresh)],restore=False)
        self.assertEqual(len(batch['admissions']),2)
        for run in batch['admissions']:
            claim=canonical if run['resource']=='validation-clone' else fresh
            self.call('close-guard',session='b',run_id=run['run_id'],token=claim['token'],evidence_sha256=hashlib.sha256(b'fixture paired admission had no launcher').hexdigest())
        with contextlib.closing(Coordinator(self.db)) as c:
            before=c.db.execute('SELECT count(*) FROM remote_guarded_runs').fetchone()[0]
            c.db.execute('UPDATE resources SET deadline=? WHERE name=?',(time.time()-1,'validation-clone'))
        for claim in (canonical,fresh):
            with self.assertRaisesRegex(RemoteCoordError,'canonical FIFO rights'):
                self.call('begin-guard-batch',session='b',tuples=[tuple_for(claim)],restore=True)
        with contextlib.closing(Coordinator(self.db)) as c:
            self.assertEqual(c.db.execute('SELECT count(*) FROM remote_guarded_runs').fetchone()[0],before)

    def test_local_single_guard_requires_fresh_pair_and_stale_is_not_restore(self):
        from agent_chat.core import CoordError
        self.close_observed_old_child()
        with contextlib.closing(Coordinator(self.db)) as c:
            with c.tx(): self.apply(c)
        self.call('cancel',session='b',resource='validation-clone')
        with contextlib.closing(Coordinator(self.db,'local-native')) as local:
            local.register('local-native-worker')
            canonical=local.request('validation-clone',5)
            with self.assertRaisesRegex(CoordError,'workspace rights'):
                local.begin_guard('validation-clone',canonical['token'],os.getpid())
            self.return_fixture_workspace(self.current_claim,'b',self.run)
            workspace=local.request('workspace:/fixture/new',5)
            canonical_run,_=local.begin_guard('validation-clone',canonical['token'],os.getpid())
            workspace_run,_=local.begin_guard('workspace:/fixture/new',workspace['token'],os.getpid())
            local.close_guard(workspace_run);local.close_guard(canonical_run)
            with mock.patch('agent_chat.core._now',return_value=time.time()+400):
                with self.assertRaisesRegex(CoordError,'stale'):
                    local.begin_guard('validation-clone',canonical['token'],os.getpid())


class ConfiguredRootEpochPositiveTests(RemoteWebFixture):
    """Distinct sanctioned owner-local provider fixture; no denied setup replay."""
    def setUp(self):
        super().setUp()
        import plistlib,shlex
        from agent_chat import service,root_local
        from agent_chat.bridge import Bridge
        from agent_chat.bridge_client import RemoteBridgeState
        from test_agent_model import ScopedModelRpc
        self.db=self.db.resolve();base=Path(self.tmp.name).resolve()
        self.plist=base/'epoch-server.plist';wrapper=base/'epoch-wrapper';executable=base/'epoch-server-bin'
        executable.write_text('')
        wrapper.write_text('exec '+shlex.join([str(executable),'--db',str(self.db),'--host','127.0.0.1','--port','8765'])+'\n')
        self.plist.write_bytes(plistlib.dumps({'Label':service.LABEL+'.server','AssociatedBundleIdentifiers':[service.LABEL+'.server'],'WorkingDirectory':str(base),'RunAtLoad':True,'KeepAlive':True,'ThrottleInterval':10,'ProgramArguments':[str(wrapper)],'EnvironmentVariables':{}}))
        config=service.InstalledServerConfig(self.plist,wrapper,executable,self.db,base,os.getuid(),'127.0.0.1',8765)
        for target in ('agent_chat.service._installed_server_config','agent_chat.root_local._installed_server_config','agent_chat.ownership_disposition._installed_server_config'):
            patcher=mock.patch(target,return_value=config);patcher.start();self.addCleanup(patcher.stop)
        self.boot=str(uuid.uuid4())
        patcher=mock.patch('agent_chat.lifecycle_producer.read_current_boot_id',return_value=self.boot);patcher.start();self.addCleanup(patcher.stop)
        self.call('register',session='r',agent='root')
        self.root_thread=str(uuid.uuid4());self.call('bind',session='r',thread=self.root_thread)
        neutral=base/'neutral';neutral.mkdir()
        state=RemoteBridgeState(self.client,self.url,dict(owner='epoch-configured-root-fixture',secret='r'*40,host_id='mac'))
        state.call('acquire');self.fixture_dispatcher=state
        bridge=Bridge(state,ScopedModelRpc(self.root_thread,str(neutral)))
        self.call('agent-status',session='r',refresh=True,expected_thread=self.root_thread,request_id=str(uuid.uuid4()))
        bridge.tick()
        # Existing supported owner-local bootstrap, never HTTP bearer enrollment.
        root_local.execute('enroll',dict(expected_thread=self.root_thread,confirm=True),master_db=self.db,service_plist=self.plist,home_project='default',session='r',host_id='mac')
        status,raw,_=self.request('POST','/api/messages',dict(to='r',body='Fixture-only exact current managed epoch preparation/CAS. Preserve old OPEN/FIFO; no native action.'),auth='basic',headers={'X-Agent-Chat-CSRF':self.server.csrf_token})
        self.assertEqual(status,200,raw)
        self.operator_id=subprocess.check_output(['jq','-r','.id'],input=raw).decode().strip()
        self.call('message',session='r',id=self.operator_id)
        self.call('register',session='old',agent='legacy-worker')
        self.old_thread=str(uuid.uuid4());self.call('bind',session='old',thread=self.old_thread)
        self.old=self.call('request',session='old',resource='validation-clone',minutes=5)
        self.old_run=self.call('begin-guard',session='old',resource='validation-clone',token=self.old['token'])
        self.call('register',session='new',agent='new-native-owner')
        self.workspace='workspace:/fixture/configured-current'
        self.claim=self.call('request',session='new',resource=self.workspace,minutes=5)
        self.call('request',session='new',resource='validation-clone',minutes=5)
        cap=base/'current-producer-capability'
        with contextlib.redirect_stdout(io.StringIO()): enrollment_main(['--db',str(self.db),'--owner','new','--host','mac','--capability-file',str(cap),'--current-workspace',self.workspace,'--current-leaf-prefix','file:workspaces/configured-current/','--current-gitdir','/fixture/configured-gitdir','--current-index-resource','file:git/worktrees/configured-current/index'])
        self.auth={'capability':credential(cap)}
        self.run=self.call('begin-guard',session='new',resource=self.workspace,token=self.claim['token'])
        self.instance=self.call('producer-establish',session='new',run_id=self.run['run_id'],**self.auth)
        now=time.time();self.deadline=now-100;self.request_id=str(uuid.uuid4())
        with contextlib.closing(Coordinator(self.db)) as c:
            c.db.execute('UPDATE resources SET deadline=? WHERE name=?',(self.deadline,'validation-clone'))
            c.db.execute('UPDATE remote_guarded_runs SET started_at=? WHERE run_id=?',(now-1000,self.old_run['run_id']))

    def tearDown(self):
        try:
            if getattr(self,'fixture_dispatcher',None): self.fixture_dispatcher.call('release')
        finally: super().tearDown()

    def command(self,action,**values):
        from agent_chat import expired_claim_disposition as module
        args=[action,'--master-db',str(self.db),'--service-plist',str(self.plist),'--project-id','default','--resource','validation-clone','--owner-session','old','--reservation-id',self.old['reservation_id'],'--expected-deadline',str(self.deadline),'--operator-message-id',self.operator_id,'--request-id',self.request_id]
        for key,value in values.items():
            args.append('--'+key.replace('_','-'))
            if value is not True: args.append(str(value))
        with contextlib.redirect_stdout(io.StringIO()) as output: self.assertEqual(module.main(args),0)
        return dict(line.split('\t',1) for line in output.getvalue().splitlines())

    def test_owner_local_configured_root_cli_prepare_receiver_preview_apply_status(self):
        prepared=self.command('epoch-prepare',instance_id=self.instance['id'],workspace_resource=self.workspace,leaf_prefix='file:workspaces/configured-current/',backing_gitdir='/fixture/configured-gitdir',index_resource='file:git/worktrees/configured-current/index',acknowledge_unverified=True)
        self.assertEqual(prepared['state'],'prepared')
        observation=dict(observed_at=time.time(),canonical_resource='validation-clone',old_reservation=self.old['reservation_id'],root_generation=prepared['root_generation'],boot_id=self.boot,boot_epoch=time.time()-500,old_backing='UNVERIFIED:separate-old-domain',backing='/fixture/configured-current:1:42',execution_id=self.instance['id'])
        att=self.call('producer-epoch-attest',session='new',run_id=self.run['run_id'],instance_id=self.instance['id'],request_id=str(uuid.uuid4()),observation=observation,prepare_id=self.request_id,gitdir='/fixture/configured-gitdir:1:43',leaf_prefix='file:workspaces/configured-current/',index_resource='file:git/worktrees/configured-current/index',native_processes=[],**self.auth)
        replay_att=self.call('producer-epoch-attest',session='new',run_id=self.run['run_id'],instance_id=self.instance['id'],request_id=att['id'],observation=observation,prepare_id=self.request_id,gitdir='/fixture/configured-gitdir:1:43',leaf_prefix='file:workspaces/configured-current/',index_resource='file:git/worktrees/configured-current/index',native_processes=[],**self.auth)
        self.assertEqual(replay_att['digest'],att['digest'])
        with self.assertRaises(RemoteCoordError):
            self.call('producer-epoch-attest',session='new',run_id=self.run['run_id'],instance_id=self.instance['id'],request_id=att['id'],observation=observation,prepare_id=self.request_id,gitdir='/fixture/configured-gitdir:1:43',leaf_prefix='file:unrelated/',index_resource='file:git/worktrees/configured-current/index',native_processes=[],**self.auth)
        with contextlib.closing(Coordinator(self.db)) as c:
            with self.assertRaisesRegex(Exception,'immutable'):
                c.db.execute("UPDATE execution_producer_scopes SET index_resource='file:unrelated/index' WHERE producer_id=?",(att['producer_id'],))
        preview=self.command('epoch-preview',attestation_id=att['id'])
        applied=self.command('epoch-apply',attestation_id=att['id'],expected_digest=preview['prestate_digest'],acknowledge_unverified=True)
        self.assertEqual(applied['state'],'managed_epoch_committed')
        replay=self.command('epoch-apply',attestation_id=att['id'],expected_digest=preview['prestate_digest'],acknowledge_unverified=True)
        self.assertEqual(replay['replayed'],'True')
        self.assertEqual(self.command('epoch-status')['state'],'committed')
        with contextlib.closing(Coordinator(self.db)) as c:
            self.assertIsNone(c.db.execute('SELECT closed_at FROM remote_guarded_runs WHERE run_id=?',(self.old_run['run_id'],)).fetchone()[0])
            self.assertEqual(c.db.execute('SELECT session FROM resource_queue WHERE resource=? ORDER BY seq',('validation-clone',)).fetchone()[0],'new')

    def test_owner_local_configured_root_cancel_preserves_allocation_and_releases_gate(self):
        self.command('epoch-prepare',instance_id=self.instance['id'],workspace_resource=self.workspace,leaf_prefix='file:workspaces/configured-current/',backing_gitdir='/fixture/configured-gitdir',index_resource='file:git/worktrees/configured-current/index',acknowledge_unverified=True)
        self.assertEqual(self.command('epoch-cancel')['state'],'cancelled')
        self.assertEqual(self.command('epoch-status')['state'],'cancelled')
        with contextlib.closing(Coordinator(self.db)) as c:
            self.assertEqual(c.db.execute('SELECT reservation_id FROM resources WHERE name=?',('validation-clone',)).fetchone()[0],self.old['reservation_id'])
            self.assertEqual(c.db.execute('SELECT count(*) FROM execution_epochs').fetchone()[0],0)

    def prepare_current(self,**changes):
        fields=dict(instance_id=self.instance['id'],workspace_resource=self.workspace,leaf_prefix='file:workspaces/configured-current/',backing_gitdir='/fixture/configured-gitdir',index_resource='file:git/worktrees/configured-current/index',acknowledge_unverified=True)
        fields.update(changes)
        return self.command('epoch-prepare',**fields)

    def assert_no_prepare_effect(self):
        with contextlib.closing(Coordinator(self.db)) as c:
            self.assertEqual(c.db.execute('SELECT count(*) FROM execution_epoch_prepares').fetchone()[0],0)
            self.assertEqual(c.db.execute('SELECT count(*) FROM execution_epoch_prepare_events').fetchone()[0],0)
            self.assertEqual(c.db.execute('SELECT reservation_id FROM resources WHERE name=?',('validation-clone',)).fetchone()[0],self.old['reservation_id'])
            self.assertEqual(c.db.execute('SELECT session FROM resource_queue WHERE resource=? ORDER BY seq',('validation-clone',)).fetchone()[0],'new')

    def test_configured_root_wrong_leaf_scope_cannot_hide_open_descendant(self):
        from agent_chat.core import CoordError
        leaf=self.call('request',session='new',resource='file:workspaces/configured-current/Assets/open.cs',minutes=5)
        guard=self.call('begin-guard',session='new',resource=leaf['resource'],token=leaf['token'])
        with self.assertRaisesRegex(CoordError,'trusted enrolled current scope'):
            self.prepare_current(leaf_prefix='file:unrelated/')
        self.assert_no_prepare_effect()
        with self.assertRaisesRegex(CoordError,'mutable leaf ownership'):
            self.prepare_current()
        self.assert_no_prepare_effect()
        self.call('close-guard',session='new',run_id=guard['run_id'],token=leaf['token'],evidence_sha256=hashlib.sha256(b'fixture no launched descendant').hexdigest())

    def test_configured_root_wrong_index_cannot_hide_open_exact_index(self):
        from agent_chat.core import CoordError
        index=self.call('request',session='new',resource='file:git/worktrees/configured-current/index',minutes=5)
        guard=self.call('begin-guard',session='new',resource=index['resource'],token=index['token'])
        with self.assertRaisesRegex(CoordError,'trusted enrolled current scope'):
            self.prepare_current(index_resource='file:unrelated/current-index')
        self.assert_no_prepare_effect()
        with self.assertRaisesRegex(CoordError,'mutable leaf ownership'):
            self.prepare_current()
        self.assert_no_prepare_effect()
        self.call('close-guard',session='new',run_id=guard['run_id'],token=index['token'],evidence_sha256=hashlib.sha256(b'fixture no launched index process').hexdigest())

    def test_configured_root_old_queue_refuses_prepare_and_racing_apply(self):
        from agent_chat.core import CoordError
        with contextlib.closing(Coordinator(self.db)) as c:
            c.db.execute('INSERT INTO bridge_jobs(id,thread_id,payload,status,queue_id,error,created_at,updated_at) VALUES(?,?,?,?,?,NULL,?,?)',('old-accepted',self.old_thread,'synthetic accepted action','queued','native-q',time.time(),time.time()))
        with self.assertRaisesRegex(CoordError,'unsettled old'):
            self.prepare_current()
        self.assert_no_prepare_effect()
        # A cancelled, never-submitted job has an explicit no-native-queue state.
        with contextlib.closing(Coordinator(self.db)) as c:
            c.db.execute("UPDATE bridge_jobs SET status='cancelled',queue_id=NULL WHERE id='old-accepted'")
        prepared=self.prepare_current()
        observation=dict(observed_at=time.time(),canonical_resource='validation-clone',old_reservation=self.old['reservation_id'],root_generation=prepared['root_generation'],boot_id=self.boot,boot_epoch=time.time()-500,old_backing='UNVERIFIED:separate-old-domain',backing='/fixture/configured-current:1:42',execution_id=self.instance['id'])
        params=dict(session='new',run_id=self.run['run_id'],instance_id=self.instance['id'],request_id=str(uuid.uuid4()),observation=observation,prepare_id=self.request_id,gitdir='/fixture/configured-gitdir:1:43',leaf_prefix='file:workspaces/configured-current/',index_resource='file:git/worktrees/configured-current/index',native_processes=[],**self.auth)
        with self.assertRaises(RemoteCoordError):
            self.call('producer-epoch-attest',**dict(params,index_resource='file:unrelated/index'))
        att=self.call('producer-epoch-attest',**params)
        preview=self.command('epoch-preview',attestation_id=att['id'])
        with contextlib.closing(Coordinator(self.db)) as c:
            c.db.execute("UPDATE bridge_jobs SET status='queued',queue_id='racing-q' WHERE id='old-accepted'")
        with self.assertRaisesRegex(CoordError,'unsettled old'):
            self.prepare_current()
        with self.assertRaisesRegex(CoordError,'unsettled old'):
            self.command('epoch-apply',attestation_id=att['id'],expected_digest=preview['prestate_digest'],acknowledge_unverified=True)
        with contextlib.closing(Coordinator(self.db)) as c:
            self.assertEqual(c.db.execute('SELECT count(*) FROM execution_epochs').fetchone()[0],0)
            self.assertEqual(c.db.execute("SELECT queue_id FROM bridge_jobs WHERE id='old-accepted'").fetchone()[0],'racing-q')
            self.assertEqual(c.db.execute('SELECT state FROM execution_epoch_prepares WHERE id=?',(self.request_id,)).fetchone()[0],'prepared')
