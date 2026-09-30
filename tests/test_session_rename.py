"""Renaming changes a display label while keeping the live coordination identity."""
import contextlib
import io
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from agent_chat import cli, core
from agent_chat.bridge_state import BridgeState
from agent_chat.remote_service import dispatch


class SessionRenameTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'state.sqlite3'
        self.coord = core.Coordinator(self.path, 'live-session')
        self.addCleanup(self.coord.close)
        self.coord.register('gameplay-successor')

    def test_rename_preserves_thread_and_ten_reservations(self):
        BridgeState(self.coord).bind(thread_id='12345678-1234-1234-1234-123456789abc')
        for index in range(10):
            self.coord.request(f'resource-{index}', 5)
        before = [tuple(row) for row in self.coord.db.execute('SELECT * FROM resources ORDER BY name')]
        binding = tuple(self.coord.db.execute('SELECT * FROM bridge_bindings').fetchone())
        registered = self.coord.db.execute('SELECT registered_at,inbox_read_seq FROM sessions').fetchone()
        self.assertEqual(self.coord.rename('gameplay'), {'session': 'live-session', 'agent': 'gameplay'})
        self.assertEqual(before, [tuple(row) for row in self.coord.db.execute('SELECT * FROM resources ORDER BY name')])
        self.assertEqual(binding, tuple(self.coord.db.execute('SELECT * FROM bridge_bindings').fetchone()))
        self.assertEqual(tuple(registered), tuple(self.coord.db.execute('SELECT registered_at,inbox_read_seq FROM sessions').fetchone()))
        self.assertEqual(self.coord.session, 'live-session')
        self.assertEqual(self.coord.rename('gameplay')['session'], 'live-session')
        self.assertTrue(all(row['owner_agent'] == 'gameplay' for row in self.coord.status()['resources']))

    def test_collision_and_invalid_names_leave_identity_unchanged(self):
        other = core.Coordinator(self.path, 'other')
        try:
            other.register('gameplay')
        finally:
            other.close()
        with self.assertRaisesRegex(core.CoordError, 'already registered'):
            self.coord.rename('gameplay')
        for name in ('', ' ', None):
            with self.assertRaises(core.CoordError):
                self.coord.rename(name)
        self.assertEqual(self.coord.db.execute('SELECT agent FROM sessions WHERE id=?', ('live-session',)).fetchone()[0], 'gameplay-successor')

    def test_hosted_rename_keeps_host_identity(self):
        coord = core.Coordinator(self.path)
        try:
            body = {'session': 'remote-session', 'host_id': 'host-a', 'params': {'agent': 'remote'}}
            dispatch(coord, dict(body, op='register'))
            before = tuple(coord.db.execute('SELECT * FROM remote_session_hosts WHERE session_id=?', ('remote-session',)).fetchone())
            result = dispatch(coord, dict(body, op='rename', params={'agent': 'renamed'}))
            self.assertEqual(result, {'session': 'remote-session', 'agent': 'renamed'})
            after = coord.db.execute('SELECT * FROM remote_session_hosts WHERE session_id=?', ('remote-session',)).fetchone()
            self.assertEqual(before[:3], tuple(after)[:3])
            with self.assertRaisesRegex(core.CoordError, 'different remote host'):
                dispatch(coord, dict(body, op='rename', host_id='host-b', params={'agent': 'wrong'}))
        finally:
            coord.close()

    def test_expired_holds_are_untouched_on_success_and_collision(self):
        dispatch(self.coord, {'op': 'status', 'session': 'live-session', 'host_id': 'host-a', 'params': {}})
        other = core.Coordinator(self.path, 'other')
        try:
            other.register('taken')
            other.request('unrelated-hold', 5)
        finally:
            other.close()
        self.coord.request('own-hold', 5)
        self.coord.db.execute('UPDATE resources SET deadline=0,stale=0')
        before = [tuple(row) for row in self.coord.db.execute('SELECT * FROM resources ORDER BY name')]
        for hosted in (False, True):
            def rename(name):
                if hosted:
                    return dispatch(self.coord, {'op': 'rename', 'session': 'live-session', 'host_id': 'host-a', 'params': {'agent': name}})
                return self.coord.rename(name)
            with self.subTest(hosted=hosted):
                rename('gameplay')
                self.assertEqual(before, [tuple(row) for row in self.coord.db.execute('SELECT * FROM resources ORDER BY name')])
                with self.assertRaisesRegex(core.CoordError, 'already registered'):
                    rename('taken')
                self.assertEqual(before, [tuple(row) for row in self.coord.db.execute('SELECT * FROM resources ORDER BY name')])

    def test_local_and_hosted_cli_route_rename(self):
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(core.main(['--db', str(self.path), '--session', 'live-session', 'rename', '--agent', 'gameplay']), 0)
        with mock.patch('agent_chat.remote.HttpClient.call', return_value={}) as call, mock.patch('agent_chat.remote.client_host_id', return_value='host-a'), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(cli.main(['--server', 'https://example.test', '--session', 'live-session', 'rename', '--agent', 'gameplay']), 0)
        self.assertEqual(call.call_args.args, ('/api/coord', {'op': 'rename', 'session': 'live-session', 'host_id': 'host-a', 'params': {'agent': 'gameplay'}}))
