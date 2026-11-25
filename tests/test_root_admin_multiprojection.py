import contextlib
import io
import time
import uuid
from unittest import TestSuite, mock

from agent_chat import root_local, root_admin_attachment_members
from agent_chat.core import Coordinator, CoordError
from test_root_admin_attachment import RootAdminAttachmentTests


def snapshot(db):
    names = ('root_team_stops', 'root_team_operations', 'root_team_reset_requests', 'root_team_reset_targets', 'root_team_reset_jobs', 'root_team_reset_events', 'session_resets', 'bridge_jobs', 'messages', 'sessions')
    return {name: tuple(sorted((tuple(row) for row in db.execute(('SELECT id,agent,registered_at FROM sessions' if name == 'sessions' else 'SELECT * FROM ' + name))), key=repr)) if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone() else None for name in names}


class MultiprojectionTests(RootAdminAttachmentTests):
    def prepare(self):
        params, observed = self.attachment_fixture()
        project = self.projects.create('Project A')['id']
        self.consume()
        self.call('route', project=project)
        self.call('stop-all', request_id=str(uuid.uuid4()), confirm=True)
        with self.projects.connection() as db:
            joined = list(db.execute('SELECT project_id,session_id FROM root_projects WHERE joined=1'))
        return params, observed, [self.master, self.projects.db_path(project)], joined

    def cli(self, params, observer):
        args = ['--home-project', 'default', '--home-session', 'root', '--host-id', 'host', 'attach-current']
        for key in ('expected_thread', 'current_thread', 'current_turn', 'expected_endpoint', 'expected_listener_pid', 'expected_birth_sec', 'expected_birth_usec', 'expected_image_sha', 'request_id'):
            args.extend(['--' + key.replace('_', '-'), str(params[key])])
        args.append('--confirm')
        with mock.patch.object(root_local, 'observe_root_attachment', **observer), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            return root_local.main(args)

    def readback(self, paths, joined):
        result, preserved = [], []
        for path in paths:
            with contextlib.closing(Coordinator(path)) as coord:
                preserved.append(snapshot(coord.db))
                for _, session in joined:
                    binding = coord.db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?', (session,)).fetchone()
                    projection = coord.db.execute('SELECT home_project,home_session,thread_id FROM root_projections WHERE session_id=?', (session,)).fetchone()
                    if binding is not None or projection is not None:
                        result.append((str(path), session, tuple(binding) if binding else None, tuple(projection) if projection else None))
        with self.projects.connection() as db:
            preserved.append(snapshot(db))
        return result, preserved

    def assert_threads(self, bindings, paths, thread):
        self.assertEqual({str(path) for path in paths}, {row[0] for row in bindings})
        for _, _, binding, projection in bindings:
            self.assertIsNotNone(binding)
            self.assertIsNotNone(projection)
            self.assertEqual(binding[0], thread)
            self.assertEqual(projection[2], thread)

    def test_positive_and_exact_retry(self):
        params, observed, paths, joined = self.prepare()
        before, preserved = self.readback(paths, joined)
        self.assertEqual(self.cli(params, {'return_value': observed}), 0)
        after, current = self.readback(paths, joined)
        self.assertNotEqual(before, after)
        self.assert_threads(after, paths, params['current_thread'])
        self.assertEqual(preserved, current)
        self.assertEqual(self.call('attach-status', request_id=params['request_id'])['state'], 'attached')
        self.assertEqual(self.cli(params, {'return_value': observed}), 0)
        self.assertEqual((after, preserved), self.readback(paths, joined))

    def test_interrupted_and_exact_retry(self):
        params, observed, paths, joined = self.prepare()
        _, preserved = self.readback(paths, joined)
        with self.assertRaisesRegex(CoordError, 'peer disappeared'):
            self.cli(params, {'side_effect': [observed, CoordError('peer disappeared')]})
        interrupted, current = self.readback(paths, joined)
        self.assertEqual(preserved, current)
        status = self.call('attach-status', request_id=params['request_id'])
        self.assertEqual(status['state'], 'prepared')
        self.assertTrue(status['cutover_pending'])
        wrong = dict(params, request_id=str(uuid.uuid4()))
        try:
            result = self.cli(wrong, {'return_value': observed})
        except CoordError:
            result = 1
        self.assertNotEqual(result, 0)
        self.assertEqual((interrupted, preserved), self.readback(paths, joined))
        changed = dict(params, current_turn=str(uuid.uuid4()))
        try:
            result = self.cli(changed, {'return_value': observed})
        except CoordError:
            result = 1
        self.assertNotEqual(result, 0)
        self.assertEqual((interrupted, preserved), self.readback(paths, joined))
        self.assertEqual(self.cli(params, {'return_value': observed}), 0)
        self.assertEqual(self.call('attach-status', request_id=params['request_id'])['state'], 'attached')
        recovered, current = self.readback(paths, joined)
        self.assert_threads(recovered, paths, params['current_thread'])
        self.assertEqual(preserved, current)
        self.assertEqual(self.cli(params, {'return_value': observed}), 0)
        self.assertEqual((recovered, preserved), self.readback(paths, joined))

    def test_interrupted_after_publication_releases_on_retry(self):
        params, observed, paths, joined = self.prepare()
        unknown_id = str(uuid.uuid4())
        with self.projects.connection() as db:
            now = time.time()
            db.execute('INSERT INTO root_fresh_requests (id,home_project,home_session,host_id,old_thread_id,prompt,instruction_origin,status,created_at,updated_at,native_action) VALUES (?,?,?,?,?,?,?,?,?,?,?)', (unknown_id, 'default', 'root', 'host', self.thread, 'old', 'authenticated_operator_ui', 'uncertain', now, now, 'creating'))
            unknown = tuple(db.execute('SELECT * FROM root_fresh_requests WHERE id=?', (unknown_id,)).fetchone())
        _, preserved = self.readback(paths, joined)
        original = root_admin_attachment_members.release_member
        calls = []

        def fail_second(db, request_id, session, old_thread, new_thread):
            calls.append(session)
            if len(calls) == 2:
                raise CoordError('release interrupted')
            return original(db, request_id, session, old_thread, new_thread)

        with mock.patch.object(root_admin_attachment_members, 'release_member', side_effect=fail_second):
            with self.assertRaisesRegex(CoordError, 'release interrupted'):
                self.cli(params, {'return_value': observed})
        self.assertEqual(len(calls), 2)
        status = self.call('attach-status', request_id=params['request_id'])
        self.assertEqual(status['state'], 'attached')
        self.assertTrue(status['cutover_pending'])
        with self.projects.connection() as db:
            self.assertTrue(root_admin_attachment_members.cutover_pending(db))
            self.assertEqual(unknown, tuple(db.execute('SELECT * FROM root_fresh_requests WHERE id=?', (unknown_id,)).fetchone()))
        next_params = dict(params, expected_thread=params['current_thread'], request_id=str(uuid.uuid4()), current_thread=str(uuid.uuid4()), current_turn=str(uuid.uuid4()))
        next_observed = dict(observed, thread_id=next_params['current_thread'], turn_id=next_params['current_turn'])
        with self.assertRaises(CoordError):
            self.cli(next_params, {'return_value': next_observed})
        with self.assertRaises(CoordError):
            self.call('attach-status', request_id=next_params['request_id'])
        bindings, current = self.readback(paths, joined)
        self.assert_threads(bindings, paths, params['current_thread'])
        self.assertEqual(preserved, current)
        self.assertEqual(self.cli(params, {'return_value': observed}), 0)
        status = self.call('attach-status', request_id=params['request_id'])
        self.assertEqual(status['state'], 'attached')
        self.assertFalse(status['cutover_pending'])
        with self.projects.connection() as db:
            self.assertFalse(root_admin_attachment_members.cutover_pending(db))
            self.assertEqual(unknown, tuple(db.execute('SELECT * FROM root_fresh_requests WHERE id=?', (unknown_id,)).fetchone()))
        self.assertEqual((bindings, preserved), self.readback(paths, joined))

    def test_stale_pause_cannot_overwrite_new_attachment(self):
        params, observed, paths, joined = self.prepare()
        pause_id = str(uuid.uuid4())
        projects = set()
        attached = []

        def interleave(root, item, paused):
            projects.add(item['project_id'])
            if len(projects) == len(joined):
                self.assertEqual(self.cli(params, {'return_value': observed}), 0)
                attached.append(True)
            return False

        with mock.patch.object(self.roots, '_routing_fresh', return_value=False), mock.patch.object(self.roots, '_routing_projection', side_effect=interleave):
            with self.assertRaisesRegex(CoordError, 'canonical Root changed before routing pause'):
                self.roots.pause_routing('default', 'root', 'host', self.thread, pause_id, True)
        self.assertEqual(len(projects), len(joined))
        self.assertEqual(attached, [True])
        with self.projects.connection() as db:
            root = db.execute('SELECT thread_id,mode FROM root_config WHERE singleton=1').fetchone()
            self.assertEqual(tuple(root), (params['current_thread'], 'running'))
            self.assertIsNone(db.execute('SELECT 1 FROM root_routing_pauses WHERE id=?', (pause_id,)).fetchone())


def load_tests(loader, tests, pattern):
    return TestSuite([MultiprojectionTests('test_positive_and_exact_retry'), MultiprojectionTests('test_interrupted_and_exact_retry'), MultiprojectionTests('test_interrupted_after_publication_releases_on_retry'), MultiprojectionTests('test_stale_pause_cannot_overwrite_new_attachment')])
