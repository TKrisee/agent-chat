"""Host registration visibility is independent of native binding/liveness."""
import contextlib
import io
import os
import subprocess
import uuid
from unittest import mock
from agent_chat import cli
from agent_chat.core import Coordinator
from test_remote_web import RemoteWebFixture, TOKEN


class AgentHostStatusTests(RemoteWebFixture):
    def setUp(self):
        project_env=mock.patch.dict(os.environ,{"AGENT_CHAT_PROJECT":"default"})
        project_env.start();self.addCleanup(project_env.stop)
        super().setUp()
        self.call('register',agent='observer')
        self.call('register',session='target',host='recorded-host',agent='target')

    def recorded(self):
        with contextlib.closing(Coordinator(self.db)) as coord:
            return dict(coord.db.execute('SELECT host_id,registered_at,last_seen_at FROM remote_session_hosts WHERE session_id=?',('target',)).fetchone())

    def test_unbound_http_and_cli_expose_target_registration_without_refresh(self):
        before=self.recorded()
        status=self.call('agent-status',to='target')
        self.assertEqual(status['host_id'],'recorded-host')
        self.assertEqual(status['host_record'],before)
        self.assertIsNone(status['thread_id']);self.assertIsNone(status['observation'])
        out=io.StringIO()
        with mock.patch.dict(os.environ,{'AGENT_CHAT_HOST_ID':'mac'}),contextlib.redirect_stdout(out):
            self.assertEqual(cli.main(['--server',self.url,'--api-token',TOKEN,'--session','a','agent-status','--to','target']),0)
        subprocess.run(['jq','-e','.host_id == "recorded-host" and .host_record.host_id == .host_id and .host_record.registered_at > 0 and .host_record.last_seen_at > 0 and .thread_id == null and .observation == null'],input=out.getvalue(),text=True,capture_output=True,check=True)
        self.assertEqual(self.recorded(),before)

    def test_bound_status_does_not_infer_host_from_stale_observation(self):
        thread=str(uuid.uuid4());self.call('bind',session='target',host='recorded-host',thread=thread)
        self.call('agent-status',to='target')  # Initialize the existing observation schema.
        with contextlib.closing(Coordinator(self.db)) as coord:
            coord.db.execute('INSERT INTO agent_control_observations VALUES(?,?,?,?,?,?,?,?,?,?)',
                ('target',str(uuid.uuid4()),'notLoaded',None,0,'old-model','old-effort','/stale','{"host_id":"wrong-observation-host"}',1))
        before=self.recorded();status=self.call('agent-status',to='target')
        self.assertEqual(status['host_id'],'recorded-host');self.assertEqual(status['host_record'],before)
        self.assertEqual(status['thread_id'],thread)
        self.assertEqual(status['observation']['state'],'notLoaded')
        self.assertEqual(self.recorded(),before)

    def test_absent_host_is_null_without_creating_registration(self):
        with contextlib.closing(Coordinator(self.db,'legacy')) as coord:
            coord.register('legacy')
        status=self.call('agent-status',to='legacy')
        self.assertIsNone(status['host_id']);self.assertIsNone(status['host_record'])
        with contextlib.closing(Coordinator(self.db)) as coord:
            self.assertIsNone(coord.db.execute('SELECT 1 FROM remote_session_hosts WHERE session_id=?',('legacy',)).fetchone())

    def refresh_cli(self, thread, request_id=None):
        args=['--server',self.url,'--project','default','--api-token',TOKEN,'--session','a',
              'agent-status','--to','target','--refresh','--expected-thread',thread]
        if request_id is not None:args += ['--request-id',request_id]
        out=io.StringIO()
        with mock.patch.dict(os.environ,{'AGENT_CHAT_HOST_ID':'mac'}),contextlib.redirect_stdout(out):
            self.assertEqual(cli.main(args),0)
        return out.getvalue()

    def refresh_fixture(self):
        thread=str(uuid.uuid4());self.call('bind',session='target',host='recorded-host',thread=thread)
        self.call('agent-status',to='target')  # Existing optional schema before assertions.
        self.call('context')
        return thread

    def inspect_rows(self):
        with contextlib.closing(Coordinator(self.db)) as coord:
            return [dict(row) for row in coord.db.execute('SELECT id,requester_session,target_session,host_id,thread_id,action,status FROM agent_control_requests')]

    def test_cli_refresh_omitted_id_creates_one_exact_inspection(self):
        thread=self.refresh_fixture();before=self.recorded();raw=self.refresh_cli(thread);rows=self.inspect_rows()
        self.assertEqual(len(rows),1);request=rows[0]
        self.assertEqual(str(uuid.UUID(request['id'])),request['id']);self.assertEqual(uuid.UUID(request['id']).version,4)
        self.assertEqual((request['requester_session'],request['target_session'],request['host_id'],request['thread_id'],request['action']),('a','target','recorded-host',thread,'inspect'))
        subprocess.run(['jq','-e','--arg','id',request['id'],'.refresh.id == $id and .admission == "running"'],input=raw,text=True,capture_output=True,check=True)
        self.assertEqual(self.recorded(),before)
        self.assertEqual([r['action'] for r in rows],['inspect'])

    def test_cli_refresh_preserves_explicit_valid_id(self):
        thread=self.refresh_fixture();request_id=str(uuid.uuid4());raw=self.refresh_cli(thread,request_id)
        self.assertEqual([r['id'] for r in self.inspect_rows()],[request_id])
        subprocess.run(['jq','-e','--arg','id',request_id,'.refresh.id == $id'],input=raw,text=True,capture_output=True,check=True)

    def test_cli_refresh_invalid_explicit_id_rejected_before_http(self):
        from agent_chat.core import CoordError
        thread=self.refresh_fixture()
        for invalid in ('not-a-uuid',''):
            with self.subTest(invalid=invalid),mock.patch('agent_chat.remote.HttpClient',side_effect=AssertionError('HTTP client must not be constructed')):
                with self.assertRaisesRegex(CoordError,'request_id must be a UUID'):
                    self.refresh_cli(thread,invalid)
        self.assertEqual(self.inspect_rows(),[])
