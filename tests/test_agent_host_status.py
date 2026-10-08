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
