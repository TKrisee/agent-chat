"""Configured own-Root route; all observations are private fixture data."""
from pathlib import Path
import contextlib
import io
import subprocess
import tempfile
import unittest
from unittest import mock

import test_root_local as local_fixture
from agent_chat import root_local
from agent_chat.core import CoordError


class OwnRootRouteTests(local_fixture.RootLocalTests):
    def setUp(self):
        sessions = tempfile.TemporaryDirectory(prefix='local-route-sessions-')
        self.addCleanup(sessions.cleanup)
        real = local_fixture.MeasurementStore
        with mock.patch.object(local_fixture, 'MeasurementStore',
                               side_effect=lambda path: real(path, sessions_root=Path(sessions.name))):
            super().setUp()
        self.enroll()
        self.consume()

    def test_supported_local_detach_retains_home_identity(self):
        result = self.call('detach', confirm=True)
        self.assertTrue(result['detached'])
        self.assertTrue(result['registrations_retained'])
        self.assertIsNone(self.roots.config())
        with local_fixture.contextlib.closing(local_fixture.Coordinator(self.master, 'root')) as coord:
            self.assertEqual(coord.db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?', ('root',)).fetchone()[0], self.thread)
            self.assertIsNotNone(coord.db.execute('SELECT id FROM sessions WHERE id=?', ('root',)).fetchone())

    def test_wrong_identity_host_and_project_do_not_detach(self):
        paths = (self.master, self.projects.path)
        before = {path: path.read_bytes() for path in paths}
        for field, value in (('session', 'foreign'), ('host_id', 'foreign-host'), ('home_project', 'foreign-project')):
            with self.subTest(field=field), self.assertRaises(CoordError):
                root_local.execute('detach', {'confirm': True}, **{**self.identity, field: value})
            self.assertEqual({path: path.read_bytes() for path in paths}, before)
        self.assertEqual(self.roots.config()['mode'], 'running')

    def test_invalid_adopt_fields_stop_before_provider_discovery(self):
        good = dict(project='default', target='existing', expected_thread=self.thread, confirm=True,
                    cwd=str(self.workspace))
        bad = [dict(good, confirm='true'), dict(good, extra='unexpected'),
               dict(good, target=[]), dict(good, cwd='relative'),
               dict(good, workspaces=[]), dict(good, workspaces=[str(self.neutral)])]
        with mock.patch.object(root_local, '_installed_server_config', side_effect=AssertionError('before provider')):
            for params in bad:
                with self.subTest(params=params), self.assertRaises(CoordError):
                    root_local.execute('adopt', params, **self.identity)

    def test_cli_detach_preserves_home_and_rejects_a_second_mutation(self):
        args = ['--home-project', 'default', '--home-session', 'root', '--host-id', 'host',
                'detach', '--confirm']
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            self.assertEqual(root_local.main(args), 0)
        result = subprocess.run(['jq', '-e', '.detached==true and .registrations_retained==true'],
                                input=output.getvalue(), text=True, capture_output=True, check=True)
        self.assertEqual(result.stdout.strip(), 'true')
        before = self.master.read_bytes()
        with self.assertRaisesRegex(CoordError, 'not enrolled'):
            root_local.main(args)
        self.assertEqual(self.master.read_bytes(), before)


def load_tests(loader, tests, pattern):
    return unittest.TestSuite(OwnRootRouteTests(name) for name in OwnRootRouteTests.__dict__ if name.startswith('test_'))
