from test_bridge_remote import RemoteBridgeTests
from agent_chat.web import snapshot


class VersionTransportTests(RemoteBridgeTests):
    def test_version_observations_reach_public_snapshot_and_clear(self):
        self.state.heartbeat('ws://127.0.0.1:4500', 11, versions=('0.169.0', '0.170.0'))
        runtime = self.call('bridge-status')['runtime']
        self.assertEqual((runtime['running_version'], runtime['installed_version']), ('0.169.0', '0.170.0'))
        self.assertEqual(runtime, snapshot(self.db)['bridge'])
        self.state.heartbeat('ws://127.0.0.1:4500', 11, versions=('0.170.0', '0.170.0'))
        runtime = snapshot(self.db)['bridge']
        self.assertEqual(runtime['running_version'], runtime['installed_version'])
        self.state.heartbeat('ws://127.0.0.1:4500', 11)
        runtime = snapshot(self.db)['bridge']
        self.assertIsNone(runtime['running_version'])
        self.assertIsNone(runtime['installed_version'])
