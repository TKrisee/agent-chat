"""Target-scoped onboarding while other project stop barriers remain active."""
import contextlib
import io
import unittest
import uuid
from unittest import mock

from agent_chat.core import Coordinator, CoordError
import test_root_local as fixtures


class ScopedOnboardingTests(fixtures.RootLocalTests):
    def test_new_target_onboards_without_releasing_existing_stops(self):
        self.enroll()
        other = self.projects.create('Existing stopped project')['id']
        self.consume()
        stop = str(uuid.uuid4())
        self.call('stop-all', request_id=stop, confirm=True)
        prior = {row['project']: row['active'] for row in self.call('stop-status', request_id=stop)['projects']}
        self.assertEqual(prior, {'default': 1, other: 1})
        target = self.projects.create('New app', workspace=str(self.workspace), instructions='Synthetic assignment')['id']
        with self.projects.connection() as db:
            self.assertIsNone(db.execute('SELECT 1 FROM root_projects WHERE project_id=?', (target,)).fetchone())
        with contextlib.closing(Coordinator(self.master)) as coord:
            before = tuple(coord.db.execute('SELECT active,root_session FROM root_team_stops WHERE id=?', (stop,)).fetchone())
        params = dict(project=target, stop_id=stop, expected_thread=self.thread, request_id=str(uuid.uuid4()), confirm=True)
        result = self.call('onboard-project', **params)
        self.assertEqual(result['project'], target)
        self.assertEqual(result['state'], 'joined')
        self.assertEqual(self.call('onboard-project', **params), result)
        with contextlib.closing(Coordinator(self.master)) as coord:
            self.assertEqual(tuple(coord.db.execute('SELECT active,root_session FROM root_team_stops WHERE id=?', (stop,)).fetchone()), before)
        self.assertEqual({row['project']: row['active'] for row in self.call('stop-status', request_id=stop)['projects']}, prior)
        with self.projects.connection() as db:
            self.assertIsNone(db.execute('SELECT resumed_at FROM root_team_operations WHERE id=?', (stop,)).fetchone()[0])
            self.assertIsNone(db.execute('SELECT 1 FROM root_team_operation_projects WHERE request_id=? AND project_id=?', (stop,target)).fetchone())
        with contextlib.closing(Coordinator(self.projects.db_path(target))) as coord:
            self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM session_resets').fetchone()[0], 0)
        self.consume()
        created = self.call('provision', project=target, agent='orchestrator', request_id=str(uuid.uuid4()), confirm=True, model=None, effort=None)
        self.assertEqual(created['project'], target)
        with contextlib.closing(Coordinator(self.master)) as coord:
            self.assertEqual(tuple(coord.db.execute('SELECT active,root_session FROM root_team_stops WHERE id=?', (stop,)).fetchone()), before)
        self.assertEqual({row['project']: row['active'] for row in self.call('stop-status', request_id=stop)['projects']}, prior)

    def test_bad_scope_is_rejected_before_target_projection(self):
        self.enroll()
        stop = str(uuid.uuid4())
        self.call('stop-all', request_id=stop, confirm=True)
        target = self.projects.create('Fresh target')['id']
        base = dict(project=target, stop_id=stop, expected_thread=self.thread, request_id=str(uuid.uuid4()), confirm=True)
        for changes in ({'stop_id':str(uuid.uuid4())}, {'expected_thread':str(uuid.uuid4())}, {'confirm':False}, {'project':'default'}):
            with self.subTest(changes=changes), self.assertRaises(CoordError):
                self.call('onboard-project', **dict(base, **changes))
        with self.assertRaises(CoordError):
            fixtures.root_local.execute('onboard-project', base, **dict(self.identity, session='wrong-actor'))
        with self.projects.connection() as db:
            self.assertIsNone(db.execute('SELECT 1 FROM root_projects WHERE project_id=?', (target,)).fetchone())

    def test_interrupted_join_reconciles_exact_prepared_request(self):
        self.enroll()
        stop = str(uuid.uuid4())
        self.call('stop-all', request_id=stop, confirm=True)
        target = self.projects.create('Fresh interrupted target')['id']
        params = dict(project=target, stop_id=stop, expected_thread=self.thread, request_id=str(uuid.uuid4()), confirm=True)
        original = fixtures.RootOrchestrator.join
        def interrupted(root, root_config, item):
            original(root, root_config, item)
            raise CoordError('simulated interruption after target join')
        with mock.patch.object(fixtures.RootOrchestrator, 'join', interrupted), self.assertRaisesRegex(CoordError, 'simulated interruption'):
            self.call('onboard-project', **params)
        with self.projects.connection() as db:
            self.assertEqual(db.execute('SELECT state FROM root_scoped_onboards WHERE request_id=?', (params['request_id'],)).fetchone()[0], 'prepared')
            self.assertIsNone(db.execute('SELECT 1 FROM root_projects WHERE project_id=?', (target,)).fetchone())
        with self.assertRaises(CoordError):
            self.call('onboard-project', **dict(params, request_id=str(uuid.uuid4())))
        result = self.call('onboard-project', **params)
        self.assertEqual(result['state'], 'joined')
        with contextlib.closing(Coordinator(self.master)) as coord:
            self.assertEqual(coord.db.execute('SELECT active FROM root_team_stops WHERE id=?', (stop,)).fetchone()[0], 1)
        with contextlib.closing(Coordinator(self.projects.db_path(target))) as coord:
            self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM session_resets').fetchone()[0], 0)

    def test_completed_admission_rejects_changed_binding(self):
        self.enroll()
        stop = str(uuid.uuid4())
        self.call('stop-all', request_id=stop, confirm=True)
        target = self.projects.create('Fresh changed binding', workspace=str(self.workspace), instructions='Synthetic assignment')['id']
        params = dict(project=target, stop_id=stop, expected_thread=self.thread, request_id=str(uuid.uuid4()), confirm=True)
        result = self.call('onboard-project', **params)
        with contextlib.closing(Coordinator(self.projects.db_path(target))) as coord:
            coord.db.execute('UPDATE bridge_bindings SET thread_id=? WHERE session_id=?', (str(uuid.uuid4()), result['session']))
        with self.assertRaisesRegex(CoordError, 'binding changed'):
            self.call('onboard-project', **params)
        self.consume()
        with self.assertRaisesRegex(CoordError, 'binding changed'):
            self.call('provision', project=target, agent='orchestrator', request_id=str(uuid.uuid4()), confirm=True, model=None, effort=None)
        with self.projects.connection() as db:
            self.assertIsNone(db.execute('SELECT request_id FROM root_projects WHERE project_id=?', (target,)).fetchone()[0])

    def test_interrupted_join_never_overwrites_changed_target_projection(self):
        self.enroll()
        stop = str(uuid.uuid4())
        self.call('stop-all', request_id=stop, confirm=True)
        target = self.projects.create('Fresh changed after interruption')['id']
        params = dict(project=target, stop_id=stop, expected_thread=self.thread, request_id=str(uuid.uuid4()), confirm=True)
        original = fixtures.RootOrchestrator.join
        def interrupted(root, root_config, item):
            original(root, root_config, item)
            raise CoordError('simulated interruption after target join')
        with mock.patch.object(fixtures.RootOrchestrator, 'join', interrupted), self.assertRaises(CoordError):
            self.call('onboard-project', **params)
        with contextlib.closing(Coordinator(self.projects.db_path(target))) as coord:
            coord.db.execute("UPDATE root_projections SET cwd='/changed'")
        with self.assertRaisesRegex(CoordError, 'projection changed'):
            self.call('onboard-project', **params)
        with contextlib.closing(Coordinator(self.projects.db_path(target))) as coord:
            self.assertEqual(coord.db.execute('SELECT cwd FROM root_projections').fetchone()[0], '/changed')

    def test_stale_global_journal_does_not_prove_active_barrier(self):
        self.enroll()
        stop = str(uuid.uuid4())
        self.call('stop-all', request_id=stop, confirm=True)
        target = self.projects.create('Fresh stale-stop target')['id']
        with contextlib.closing(Coordinator(self.master)) as coord:
            coord.db.execute('UPDATE root_team_stops SET active=0 WHERE id=?', (stop,))
        with self.assertRaisesRegex(CoordError, 'no active barrier'):
            self.call('onboard-project', project=target, stop_id=stop, expected_thread=self.thread, request_id=str(uuid.uuid4()), confirm=True)
        with self.projects.connection() as db:
            self.assertIsNone(db.execute('SELECT 1 FROM root_scoped_onboards WHERE project_id=?', (target,)).fetchone())

    def test_prepared_retry_rejects_stale_stop(self):
        self.enroll()
        stop = str(uuid.uuid4())
        self.call('stop-all', request_id=stop, confirm=True)
        target = self.projects.create('Fresh stale retry')['id']
        params = dict(project=target, stop_id=stop, expected_thread=self.thread, request_id=str(uuid.uuid4()), confirm=True)
        with mock.patch.object(fixtures.RootOrchestrator, 'join', side_effect=CoordError('fixture interruption')), self.assertRaises(CoordError):
            self.call('onboard-project', **params)
        with contextlib.closing(Coordinator(self.master)) as coord:
            coord.db.execute('UPDATE root_team_stops SET active=0 WHERE id=?', (stop,))
        with self.assertRaisesRegex(CoordError, 'no active barrier'):
            self.call('onboard-project', **params)
        with self.projects.connection() as db:
            self.assertEqual(db.execute('SELECT state FROM root_scoped_onboards WHERE request_id=?', (params['request_id'],)).fetchone()[0], 'prepared')
            self.assertIsNone(db.execute('SELECT 1 FROM root_projects WHERE project_id=?', (target,)).fetchone())

    def test_prepared_retry_does_not_promote_unrelated_reserved_session(self):
        self.enroll()
        stop = str(uuid.uuid4())
        self.call('stop-all', request_id=stop, confirm=True)
        target = self.projects.create('Fresh reserved session conflict')['id']
        params = dict(project=target, stop_id=stop, expected_thread=self.thread, request_id=str(uuid.uuid4()), confirm=True)
        with mock.patch.object(fixtures.RootOrchestrator, 'join', side_effect=CoordError('fixture before join')), self.assertRaises(CoordError):
            self.call('onboard-project', **params)
        with self.projects.connection() as db:
            reserved = db.execute('SELECT project_session FROM root_scoped_onboards WHERE request_id=?', (params['request_id'],)).fetchone()[0]
        with contextlib.closing(Coordinator(self.projects.db_path(target), reserved)) as coord:
            fixtures.dispatch(coord, {'op':'register', 'session':reserved, 'host_id':'host', 'params':{'agent':'unrelated-registration'}})
        with self.assertRaises(CoordError):
            self.call('onboard-project', **params)
        with contextlib.closing(Coordinator(self.projects.db_path(target))) as coord:
            self.assertEqual(coord.db.execute('SELECT agent FROM sessions WHERE id=?', (reserved,)).fetchone()[0], 'unrelated-registration')
            self.assertFalse(coord.db.execute("SELECT 1 FROM root_projections WHERE session_id=?", (reserved,)).fetchone())

    def test_prepared_retry_rejects_partial_root_identity_even_with_matching_label(self):
        self.enroll()
        stop = str(uuid.uuid4())
        self.call('stop-all', request_id=stop, confirm=True)
        target = self.projects.create('Fresh partial session conflict')['id']
        params = dict(project=target, stop_id=stop, expected_thread=self.thread, request_id=str(uuid.uuid4()), confirm=True)
        with mock.patch.object(fixtures.RootOrchestrator, 'join', side_effect=CoordError('fixture before join')), self.assertRaises(CoordError):
            self.call('onboard-project', **params)
        with self.projects.connection() as db:
            reserved = db.execute('SELECT project_session FROM root_scoped_onboards WHERE request_id=?', (params['request_id'],)).fetchone()[0]
        with contextlib.closing(Coordinator(self.projects.db_path(target), reserved)) as coord:
            fixtures.dispatch(coord, {'op':'register', 'session':reserved, 'host_id':'host', 'params':{'agent':'root'}})
            fixtures.BridgeState(coord).bind(self.thread)
        with self.assertRaisesRegex(CoordError, 'incomplete'):
            self.call('onboard-project', **params)
        with self.projects.connection() as db:
            self.assertEqual(db.execute('SELECT state FROM root_scoped_onboards WHERE request_id=?', (params['request_id'],)).fetchone()[0], 'prepared')

    def test_scoped_onboarding_local_cli_accepts_exact_arguments(self):
        self.enroll()
        stop = str(uuid.uuid4())
        self.call('stop-all', request_id=stop, confirm=True)
        target = self.projects.create('Fresh CLI target')['id']
        request = str(uuid.uuid4())
        args = ['--master-db',str(self.master),'--service-plist',str(self.plist),
                '--home-project','default','--home-session','root','--host-id','host',
                'onboard-project','--project-id',target,'--stop-id',stop,
                '--expected-thread',self.thread,'--request-id',request,'--confirm']
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(fixtures.root_local.main(args), 0)
        with self.projects.connection() as db:
            self.assertEqual(db.execute('SELECT state FROM root_scoped_onboards WHERE request_id=?', (request,)).fetchone()[0], 'joined')


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for name in ('test_new_target_onboards_without_releasing_existing_stops', 'test_bad_scope_is_rejected_before_target_projection',
                 'test_interrupted_join_reconciles_exact_prepared_request', 'test_completed_admission_rejects_changed_binding',
                 'test_interrupted_join_never_overwrites_changed_target_projection', 'test_stale_global_journal_does_not_prove_active_barrier',
                 'test_scoped_onboarding_local_cli_accepts_exact_arguments', 'test_prepared_retry_rejects_stale_stop',
                 'test_prepared_retry_does_not_promote_unrelated_reserved_session',
                 'test_prepared_retry_rejects_partial_root_identity_even_with_matching_label'):
        suite.addTest(ScopedOnboardingTests(name))
    return suite
