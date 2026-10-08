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
from agent_chat.core import Coordinator
from agent_chat.lifecycle_producer import credential, main as enrollment_main
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
            self.assertEqual(cli.main(['run', 'shared', '--', sys.executable, '-c', script]), 0)
        with contextlib.closing(Coordinator(self.db)) as coord:
            fact = coord.db.execute('SELECT c.*,i.run_id FROM current_instance_closures c JOIN execution_instances i ON i.id=c.instance_id').fetchone()
            self.assertIsNotNone(fact)
            self.assertIn(b'returncode=0\ngroup_closed=true', bytes(fact['evidence']))
            self.assertIsNotNone(coord.db.execute('SELECT closed_at FROM remote_guarded_runs WHERE run_id=?', (fact['run_id'],)).fetchone()[0])
        pid = int(child_pid.read_text())
        with self.assertRaises(ProcessLookupError):
            os.kill(pid, 0)

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
                cli.main(['run', 'shared', '--', sys.executable, '-c', 'pass'])
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
        self.call('inbox', session='root')
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
