import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from agent_chat import communication
from agent_chat.bridge import Bridge
from agent_chat.policy_store import PolicyStore


class AuthorizationPolicyVersionTests(unittest.TestCase):
    def test_changed_role_scope_invalidates_consumed_warm_thread_policy(self):
        original_authorization = communication.BOOT_AUTHORIZATION
        with tempfile.TemporaryDirectory() as directory:
            store = PolicyStore(str(Path(directory) / 'policy.sqlite3'))
            state = SimpleNamespace(
                resolve=lambda session: dict(route=[dict(session_id=session, agent_path=None)]),
                policy_consumed=lambda thread: store.consumed('host', thread, project='project'),
                connection_metadata=lambda: dict(project='project'),
                wake_messages=lambda thread, ids: [dict(id='message', body='task', complete=True)],
            )
            bridge = Bridge.__new__(Bridge)
            bridge.state = state
            try:
                initial = bridge.payload('thread', [dict(id='message', recipient_session='agent')])
                old_version = communication.policy_version()
                self.assertIn(original_authorization, initial)
                self.assertTrue(store.offer('host', 'project', 'wake', 'first-input', 'thread', initial)['offered'])
                self.assertTrue(store.receipt('host', 'thread', 'first-input', old_version, 'first-turn', project='project')['consumed'])
                self.assertTrue(store.consumed('host', 'thread', project='project'))

                communication.BOOT_AUTHORIZATION = original_authorization + ' Updated role scope.'
                self.assertFalse(store.consumed('host', 'thread', project='project'))
                updated = bridge.payload('thread', [dict(id='message', recipient_session='agent')])
                self.assertIn('Updated role scope.', updated)
                self.assertIn('Standing authorization does not bypass tool permissions', updated)
                self.assertIn('"policy_included":true', updated)
            finally:
                communication.BOOT_AUTHORIZATION = original_authorization


if __name__ == '__main__':
    unittest.main()
