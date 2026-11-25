import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from agent_chat.core import CoordError
from agent_chat.parked_durability import verify_materialized


class Provider:
    def __init__(self, path):
        self.thread = {'id': 'thread', 'name': 'worker', 'path': str(path),
                       'status': {'type': 'idle'}, 'canAcceptDirectInput': True}
        self.items = []
        self.rpc = self.bridge = self
        self.digest = 'settings'

    def request(self, method, params):
        assert method == 'thread/read' and params['threadId'] == 'thread'
        return {'thread': self.thread}

    def queue(self, thread):
        return self.items

    def settings(self, thread, *, exact, model=None, effort=None):
        assert thread == 'thread' and exact
        return {}, self.digest


class DurableEvidenceTests(unittest.TestCase):
    def test_exact_materialization_requires_file_empty_work_and_settings(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'rollout'
            provider = Provider(path)
            job = {'new_thread_id': 'thread', 'target_agent': 'worker', 'settings_digest': 'settings'}
            with patch('agent_chat.parked_durability.turns_page', return_value={'data': [], 'nextCursor': None}) as turns:
                with self.assertRaises(CoordError): verify_materialized(provider, job)
                path.write_bytes(b'metadata')
                self.assertEqual(verify_materialized(provider, job)['path'], str(path))
                provider.thread['name'] = 'another'
                with self.assertRaises(CoordError): verify_materialized(provider, job)
                provider.thread['name'] = 'worker'
                provider.items = ['queued']
                with self.assertRaises(CoordError): verify_materialized(provider, job)
                provider.items = []
                provider.digest = 'different'
                with self.assertRaises(CoordError): verify_materialized(provider, job)
                provider.digest = 'settings'
                turns.return_value = {'data': [{'id': 'turn'}], 'nextCursor': None}
                with self.assertRaises(CoordError): verify_materialized(provider, job)


if __name__ == '__main__':
    unittest.main()
