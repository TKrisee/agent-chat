"""Real loopback bridge/source-trace contract with a deterministic native fixture."""
import subprocess
import uuid
import os
from pathlib import Path
from unittest import mock
from agent_chat.bridge import Bridge
from agent_chat.bridge_client import RemoteBridgeState
from test_bridge import FakeRpc
from test_remote_web import RemoteWebFixture

class EligibleRpc(FakeRpc):
    timeout=1
    def connection_identity(self): return {'client_instance':'fixture-connection','connection_epoch':3}
    def notification_boundary(self, **kwargs): return 0
    def notification_snapshot_since(self, cursor): return [], []
    def notifications_since(self, cursor): return []

class EligibilityHttpContract(RemoteWebFixture):
    def start(self, **kwargs):
        from agent_chat import service, root_local
        for module, name in ((service, '_installed_server_config'),
                             (service, 'configured_server_database'),
                             (root_local, '_installed_server_config'),
                             (root_local, 'configured_server_database')):
            guard = mock.patch.object(module, name, side_effect=AssertionError('installed service discovery forbidden'))
            guard.start()
            self.addCleanup(guard.stop)
        home = Path(self.tmp.name) / 'home'
        home.mkdir(exist_ok=True)
        environment = mock.patch.dict(os.environ, {'HOME': str(home), 'CODEX_HOME': str(home / '.codex')})
        environment.start()
        self.addCleanup(environment.stop)
        sessions = Path(self.tmp.name) / 'sessions'
        sessions.mkdir(exist_ok=True)
        return super().start(sessions_root=sessions, **kwargs)

    def wire_assert(self, actor, source, expression):
        status, raw, _ = self.request('POST','/api/coord',{'op':'source-trace','session':actor,'host_id':'mac','params':{'source_id':source}})
        self.assertEqual(status,200,raw)
        result=subprocess.run(['jq','-e',expression],input=raw,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        self.assertEqual(result.returncode,0,result.stderr.decode())

    def test_exact_source_observation_and_nonready(self):
        for actor in ('sender','recipient','other','idle_target'):
            self.call('register',session=actor,agent=actor)
        thread=str(uuid.uuid4()); self.call('bind',session='recipient',thread=thread)
        idle_thread=str(uuid.uuid4()); self.call('bind',session='idle_target',thread=idle_thread)
        state=RemoteBridgeState(self.client,self.url,{'owner':'fixture','secret':'x'*40,'host_id':'mac'})
        state.call('acquire')
        source=self.call('send',session='sender',to='recipient',body='fixture')['id']
        rpc=EligibleRpc()
        with mock.patch('agent_chat.bridge.time.monotonic_ns',side_effect=[100,150]):
            Bridge(state,rpc).tick(observe_policies=False)
        self.wire_assert('sender',source,'.status == "ok" and .eligibility_at == null and .job_eligibility_observation.status == "observed" and .job_eligibility_observation.connection_instance == "fixture-connection" and .job_eligibility_observation.connection_epoch == 3 and .job_eligibility_observation.start_monotonic_ns == 100 and .job_eligibility_observation.end_monotonic_ns == 150 and .job_eligibility_observation.clock_domain == "host_monotonic_ns"')
        self.wire_assert('other',source,'.status == "unauthorized"')
        waiting=self.call('send',session='other',to='idle_target',body='fixture waiting')['id']
        rpc.status='active'
        with mock.patch('agent_chat.bridge.time.monotonic_ns',side_effect=[200,250]):
            Bridge(state,rpc).tick(observe_policies=False)
        self.wire_assert('other',waiting,'.status == "ok" and .job_id == null and .eligibility_at == null and .job_eligibility_observation.status == "unknown"')
