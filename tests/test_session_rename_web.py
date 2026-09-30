"""Self-service rename through the hosted API preserves live coordination state."""
import subprocess

from agent_chat.bridge_state import BridgeState
from agent_chat.core import Coordinator
from test_remote_web import RemoteWebFixture


class SessionRenameWebTests(RemoteWebFixture):
    def test_hosted_self_rename_preserves_binding_and_expired_reservations(self):
        caller = {'session': 'live-gameplay', 'host_id': 'fixture-host'}
        status, raw, _ = self.request('POST', '/api/coord', dict(
            caller, op='register', params={'agent': 'gameplay-successor'}))
        self.assertEqual(status, 200, raw)
        coord = Coordinator(self.db, caller['session'])
        try:
            BridgeState(coord).bind(thread_id='12345678-1234-1234-1234-123456789abc')
            for index in range(10):
                coord.request('resource-' + str(index), 5)
            coord.db.execute('UPDATE resources SET deadline=0,stale=0')
            holds = [tuple(row) for row in coord.db.execute('SELECT * FROM resources ORDER BY name')]
            binding = tuple(coord.db.execute('SELECT * FROM bridge_bindings').fetchone())
            status, raw, _ = self.request('POST', '/api/coord', dict(
                caller, op='rename', params={'agent': 'gameplay'}))
            self.assertEqual(status, 200, raw)
            parsed = subprocess.run(['jq', '-e', '.session=="live-gameplay" and .agent=="gameplay"'],
                                    input=raw, capture_output=True, timeout=5)
            self.assertEqual(parsed.returncode, 0, parsed.stderr or parsed.stdout)
            self.assertEqual(holds, [tuple(row) for row in coord.db.execute('SELECT * FROM resources ORDER BY name')])
            self.assertEqual(binding, tuple(coord.db.execute('SELECT * FROM bridge_bindings').fetchone()))
        finally:
            coord.close()
