import unittest
import contextlib
from pathlib import Path
from unittest import mock
from agent_chat.rpc import RpcError
from test_supervisor import SupervisorWorkflowTests
from agent_chat import core, root_local, service


class DuplicateIncidentProbe(SupervisorWorkflowTests):
    def setUp(self):
        for module in (service, root_local):
            patch = mock.patch.object(module, '_installed_server_config', side_effect=AssertionError('installed service forbidden'))
            patch.start(); self.addCleanup(patch.stop)
        patch = mock.patch.object(core, 'default_db', side_effect=AssertionError('default database forbidden'))
        patch.start(); self.addCleanup(patch.stop)
        super().setUp()

    def start(self, **kwargs):
        sessions = Path(self.tmp.name) / 'sessions'; sessions.mkdir()
        super().start(sessions_root=sessions, **kwargs)

    def test_transient_stale_host_does_not_restart_same_native_error(self):
        self.enable()
        original = self.rpc.request
        failing = [True]
        def broken(method, params):
            if failing[0] and method == 'thread/read' and params.get('threadId') == self.target_thread:
                raise RpcError(-32600, 'missing source rollout')
            return original(method, params)
        self.rpc.request = broken
        self.sample()
        self.sample(61)
        self.assertEqual(len(self.notices()), 1)
        first = self.notices()[0]['id']
        self.call('send', session='b', to='worker', body='New independent unread input')
        self.now += 31
        self.state.supervisor_jobs()
        with contextlib.closing(core.Coordinator(self.db)) as coord:
            rows = {row['condition']: (row['active'],row['episode']) for row in coord.db.execute('SELECT condition,active,episode FROM supervisor_incidents')}
            self.assertEqual(rows['native_error'], (1,1))
            self.assertEqual(rows['stale_host'], (1,1))
        self.sample()
        self.sample(61)
        self.sample(121)
        self.assertEqual([item['id'] for item in self.notices()], [first])
        failing[0] = False
        self.sample(301)
        self.assertTrue(any(item['condition'] == 'native_error' and item['state'] == 'resolved' for item in self.target()['incidents']))
        failing[0] = True
        self.sample(61)
        self.sample(61)
        notices = self.notices()
        self.assertEqual(len(notices),2)
        self.assertNotEqual(notices[1]['id'],first)


def load_tests(loader, tests, pattern):
    return unittest.TestSuite([DuplicateIncidentProbe('test_transient_stale_host_does_not_restart_same_native_error')])


if __name__ == '__main__': unittest.main()
