import contextlib
import hashlib
import subprocess
import io
import os
from pathlib import Path
import plistlib
import shlex
import sqlite3
import tempfile
import time
import unittest
import uuid
from unittest.mock import patch

from agent_chat import cli, root_local, service
from agent_chat.agent_control import AgentControlState
from agent_chat.bridge_state import BridgeState
from agent_chat.core import Coordinator, CoordError
from agent_chat.measurement import MeasurementStore
from agent_chat.projects import Projects
from agent_chat.remote_service import dispatch
from agent_chat.root_orchestrator import RootOrchestrator
from agent_chat.web import Handler
from agent_chat.web import operator_session


class RootLocalTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='root-local-0060-')
        self.addCleanup(temporary.cleanup)
        self.base = Path(temporary.name).resolve()
        self.master = self.base / 'state.sqlite3'
        self.neutral = self.base / 'neutral'; self.neutral.mkdir()
        self.workspace = self.base / 'project-workspace'; self.workspace.mkdir()
        self.plist = self.base / 'server.plist'
        self.wrapper = self.base / 'wrapper'
        self.executable = self.base / 'server-bin'; self.executable.write_text('')
        self.config = service.InstalledServerConfig(self.plist, self.wrapper, self.executable,
            self.master, self.base, os.getuid(), '127.0.0.1', 8765)
        self.wrapper.write_text('exec ' + shlex.join([str(self.executable), '--db', str(self.master),
                                                     '--host', '127.0.0.1', '--port', '8765']) + '\n')
        self.plist.write_bytes(plistlib.dumps({
            'Label': service.LABEL + '.server', 'AssociatedBundleIdentifiers': [service.LABEL + '.server'],
            'WorkingDirectory': str(self.base), 'RunAtLoad': True, 'KeepAlive': True, 'ThrottleInterval': 10,
            'ProgramArguments': [str(self.wrapper)], 'EnvironmentVariables': {}}))
        self.addCleanup(patch.stopall)
        patch('agent_chat.service._installed_server_config', return_value=self.config).start()
        patch('agent_chat.root_local._installed_server_config', return_value=self.config).start()
        self.projects = Projects(self.master)
        self.roots = RootOrchestrator(self.projects)
        self.thread = str(uuid.uuid4())
        with contextlib.closing(Coordinator(self.master)) as coord:
            dispatch(coord, {'op': 'register', 'session': 'root', 'host_id': 'host', 'params': {'agent': 'root'}})
            coord.session = 'root'
            BridgeState(coord).bind(self.thread)
            AgentControlState(coord)
            coord.db.execute('INSERT INTO agent_control_observations VALUES(?,?,?,?,?,?,?,?,?,?)',
                ('root', self.thread, 'idle', None, 0, None, None, str(self.neutral), '{}', time.time()))
            coord.context()
        measurements = MeasurementStore(self.master)
        measurements.close()
        operator_session(self.master)
        self.identity = dict(master_db=self.master, service_plist=self.plist,
                             home_project='default', session='root', host_id='host')

    def call(self, operation, **params):
        return root_local.execute(operation, params, **self.identity)

    def enroll(self):
        return self.call('enroll', expected_thread=self.thread, confirm=True)

    def consume(self):
        with contextlib.closing(Coordinator(self.master, 'root')) as coord:
            coord.context(limit=100, max_bytes=100000)

    def test_unenrolled_status_enrollment_and_route_use_real_local_apis(self):
        self.assertEqual(self.call('status'), {'enrolled': False})
        with self.assertRaisesRegex(CoordError, 'not enrolled'):
            self.call('route', project='default')
        self.enroll()
        route = self.call('in', project='default')
        self.assertEqual((route['project'], route['session']), ('default', 'root'))
        self.assertEqual(self.call('route', project='default'), route)
        self.assertEqual(self.roots.config()['home_session'], 'root')
        self.assertIn('root', self.call('status'))

    def test_real_effective_uid_and_provider_chain_refuse_before_constructors(self):
        for field in ('getuid', 'geteuid'):
            with self.subTest(field=field), patch('agent_chat.root_local.os.' + field, return_value=os.getuid() + 1), \
                    patch('agent_chat.projects.Projects') as constructor:
                with self.assertRaisesRegex(CoordError, 'real and effective UID'):
                    self.call('status')
                constructor.assert_not_called()
        saved = self.executable.read_bytes()
        self.executable.unlink(); self.executable.symlink_to(self.wrapper)
        with patch('agent_chat.projects.Projects') as constructor:
            with self.assertRaisesRegex(CoordError, 'owner or path'):
                self.call('status')
            constructor.assert_not_called()
        self.executable.unlink(); self.executable.write_bytes(saved)

    def test_wrong_master_project_host_thread_and_fields_have_no_effect(self):
        before = self.master.read_bytes()
        for identity in (dict(self.identity, master_db=self.base / 'different.sqlite3'),
                         dict(self.identity, home_project='unknown'), dict(self.identity, host_id='other-host'),
                         dict(self.identity, session='other-session')):
            with self.subTest(identity=identity), self.assertRaises(CoordError):
                root_local.execute('status', {}, **identity)
        for operation, params in (('authorize', {}), ('status', {'sql': 'anything'}),
                                  ('enroll', {'expected_thread': str(uuid.uuid4()), 'confirm': True}),
                                  ('team-lifecycle', {'operation': 'reset', 'request_id': str(uuid.uuid4()), 'confirm': True})):
            with self.subTest(operation=operation), self.assertRaises(CoordError):
                root_local.execute(operation, params, **self.identity)
        self.assertEqual(self.master.read_bytes(), before)

    def test_enrolled_binding_change_and_nondefault_lineage_refuse(self):
        self.enroll()
        project = self.projects.create('Project A')['id']
        self.consume()
        path = self.projects.db_path(project)
        with contextlib.closing(Coordinator(path)) as coord:
            coord.db.execute("DELETE FROM meta WHERE key='project_registry'")
        before = path.read_bytes()
        # Directory status may report an unjoined project, but cannot open its
        # inconsistent target through sync and initialize or rewrite its store.
        self.call('status')
        self.assertEqual(path.read_bytes(), before)
        with self.projects.connection() as db:
            row = db.execute('SELECT joined,error FROM root_projects WHERE project_id=?', (project,)).fetchone()
            self.assertEqual(row['joined'], 0)
            self.assertIn('lineage', row['error'])
        with self.assertRaisesRegex(CoordError, 'lineage'):
            self.call('route', project=project)
        with contextlib.closing(Coordinator(self.master)) as coord:
            coord.db.execute('UPDATE bridge_bindings SET thread_id=? WHERE session_id=?', (str(uuid.uuid4()), 'root'))
        with self.assertRaisesRegex(CoordError, 'current thread'):
            self.call('status')

    def test_setup_saved_request_replay_and_stop_barrier(self):
        self.enroll()
        project = self.projects.create('Project A', workspace=str(self.workspace), instructions='Synthetic project setup')['id']
        self.consume()
        request = str(uuid.uuid4())
        params = dict(project=project, agent='orchestrator', cwd=str(self.workspace), prompt='Synthetic project setup',
                      request_id=request, confirm=True, model=None, effort=None)
        first = self.call('configure', **params)
        self.assertEqual(self.call('configure', **params), first)
        with self.assertRaises(CoordError):
            self.call('configure', **dict(params, prompt='Different payload'))
        with self.assertRaisesRegex(CoordError, 'different creation'):
            self.call('provision', project=project, agent='orchestrator', request_id=request,
                      confirm=True, model=None, effort=None)
        self.consume()
        stop = str(uuid.uuid4())
        self.call('stop-all', request_id=stop, confirm=True)
        self.assertEqual(self.call('stop-status', request_id=stop)['request_id'], stop)
        with self.assertRaisesRegex(CoordError, 'stop-all barrier'):
            self.call('configure', **params)
        self.call('resume-all', request_id=stop, confirm=True)
        with self.projects.connection() as db:
            self.assertIsNotNone(db.execute('SELECT resumed_at FROM root_team_operations WHERE id=?', (stop,)).fetchone()[0])

    def test_saved_provision_and_exact_management_request_replay(self):
        self.enroll()
        project = self.projects.create('Project A', workspace=str(self.workspace), instructions='Synthetic project setup')['id']
        self.consume()
        request = str(uuid.uuid4())
        params = dict(project=project, agent='orchestrator', request_id=request, confirm=True, model=None, effort=None)
        first = self.call('provision', **params)
        self.assertEqual(self.call('provision', **params), first)
        po = first['creation']['target_session']
        thread = str(uuid.uuid4())
        with contextlib.closing(Coordinator(self.projects.db_path(project))) as coord:
            dispatch(coord, {'op': 'register', 'session': 'retained-worker', 'host_id': 'host', 'params': {'agent': 'retained-worker'}})
            coord.session = 'retained-worker'
            BridgeState(coord).bind(thread)
        self.consume()
        management = dict(project=project, po=po, target='retained-worker', expected_thread=thread,
                          expected_host='host', request_id=str(uuid.uuid4()), confirm=True)
        result = self.call('delegate-management', **management)
        self.assertEqual(self.call('delegate-management', **management), result)
        with self.assertRaisesRegex(CoordError, 'retry payload differs'):
            self.call('delegate-management', **dict(management, expected_host='other-host'))
        with self.assertRaisesRegex(CoordError, 'current designated PO'):
            self.call('delegate-management', **dict(management, po='not-po', request_id=str(uuid.uuid4())))

    def test_fresh_start_saved_outcome_replay_and_setup_barrier(self):
        self.enroll(); self.consume()
        request = str(uuid.uuid4())
        params = dict(expected_thread=self.thread, prompt='Read retained synthetic checkpoint', request_id=request, confirm=True)
        first = self.call('fresh-start', **params)
        self.assertEqual(self.call('fresh-start', **params), first)
        self.assertEqual(self.call('fresh-status', request_id=request), first)
        with self.assertRaisesRegex(CoordError, 'retry payload differs'):
            self.call('fresh-start', **dict(params, prompt='Changed'))
        project = self.projects.create('Project A')['id']
        with self.assertRaisesRegex(CoordError, 'fresh-start is pending'):
            self.call('configure', project=project, agent='orchestrator', cwd=str(self.workspace), prompt='Synthetic',
                      request_id=str(uuid.uuid4()), confirm=True, model=None, effort=None)
        with self.projects.connection() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM root_fresh_requests').fetchone()[0], 1)

    def test_missing_or_active_measurement_state_is_not_inactive(self):
        with contextlib.closing(sqlite3.connect(self.master)) as db:
            db.execute('INSERT INTO usage_measurements(id,project_id,project_db_path,state,duration_seconds) VALUES(?,?,?,?,?)',
                       ('synthetic-measurement', 'default', str(self.master), 'running', 60))
            db.commit()
        with self.assertRaisesRegex(CoordError, 'active project measurement'):
            self.enroll()
        with contextlib.closing(sqlite3.connect(self.master)) as db:
            db.execute('DROP TABLE usage_measurements')
        with self.assertRaisesRegex(CoordError, 'measurement state is unavailable'):
            self.enroll()

    def test_http_privileged_gate_unchanged_and_cli_local_never_calls_http(self):
        class NoDispatch:
            def root_action(self, value):
                raise AssertionError('privileged Root HTTP dispatch must not occur')
        for method in ('enroll', 'route', 'configure', 'fresh-start', 'stop-all'):
            with self.subTest(method=method), self.assertRaisesRegex(CoordError, 'shared bearer permits only'):
                Handler.shared_bearer_root_action(NoDispatch(),
                    dict(op=method, session='root', host_id='host', params={}))
        with patch('agent_chat.remote.HttpClient.call', side_effect=AssertionError('no HTTP')), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(cli.main(['root-local', '--master-db', str(self.master), '--service-plist', str(self.plist),
                '--home-project', 'default', '--home-session', 'root', '--host-id', 'host', 'status']), 0)


    def snapshot(self):
        # SQLite mode=ro can create an empty WAL and transient shared-memory
        # read marks. Assert durable bytes and reject nonempty new WAL payloads.
        result = {}
        for path in self.base.rglob('*'):
            if not path.is_file() or path.name.endswith('-shm'):
                continue
            if path.name.endswith('-wal') and path.stat().st_size == 0:
                continue
            result[str(path.relative_to(self.base))] = hashlib.sha256(path.read_bytes()).hexdigest()
        return result

    def test_local_readiness_real_projection_has_no_bootstrap_or_writes(self):
        self.enroll()
        before = self.snapshot()
        with patch('agent_chat.projects.Projects', side_effect=AssertionError('no bootstrap')), \
                patch.object(RootOrchestrator, '__init__', side_effect=AssertionError('no schema')), \
                patch.object(RootOrchestrator, 'sync', side_effect=AssertionError('no sync')), \
                patch.object(RootOrchestrator, 'route', side_effect=AssertionError('no route')):
            result = self.call('readiness', projects=['default'], expected_thread=self.thread)
        self.assertEqual(result['authority'], {'scope': 'machine_read', 'named_session_authentication': 'UNVERIFIED'})
        self.assertTrue(result['caller']['record_match'])
        self.assertEqual([row['project'] for row in result['projects']], ['default'])
        self.assertEqual(self.snapshot(), before)

    def test_local_readiness_cli_exact_explicit_cohort_and_no_http(self):
        self.enroll()
        before = self.snapshot()
        output = io.StringIO()
        with patch('agent_chat.remote.HttpClient.call', side_effect=AssertionError('no HTTP')), \
                patch('agent_chat.remote.client_host_id', side_effect=AssertionError('no implicit host')), \
                contextlib.redirect_stdout(output):
            self.assertEqual(cli.main(['root-local', '--home-project', 'default', '--home-session', 'root',
                '--host-id', 'host', 'readiness', '--expected-thread', self.thread, '--project-id', 'default']), 0)
        checked = subprocess.run(['jq', '-e', '.authority.scope == "machine_read" and '
            '.authority.named_session_authentication == "UNVERIFIED" and '
            '(.projects | length) == 1'],
            input=output.getvalue(), text=True, capture_output=True)
        self.assertEqual(checked.returncode, 0, checked.stderr)
        self.assertEqual(self.snapshot(), before)

    def test_local_readiness_rejects_invalid_fields_cohort_and_stale_records(self):
        self.enroll()
        before = self.snapshot()
        valid = dict(projects=['default'], expected_thread=self.thread)
        for values in ({}, dict(valid, confirm=True), dict(valid, projects=[]),
                       dict(valid, projects=['default', 'default']), dict(valid, projects=['unknown']),
                       dict(valid, projects=['default'] * 65), dict(valid, expected_thread='other')):
            with self.subTest(values=values), self.assertRaises(CoordError):
                self.call('readiness', **values)
        self.assertEqual(self.snapshot(), before)
        with contextlib.closing(Coordinator(self.master)) as coord:
            coord.db.execute('UPDATE agent_control_observations SET checked_at=?', (time.time()-121,))
        before = self.snapshot()
        with self.assertRaisesRegex(CoordError, 'stale or unverifiable'):
            self.call('readiness', **valid)
        self.assertEqual(self.snapshot(), before)

    def test_local_readiness_retains_uid_provider_identity_and_missing_store_checks(self):
        self.enroll()
        valid = dict(projects=['default'], expected_thread=self.thread)
        for field in ('getuid', 'geteuid'):
            with patch('agent_chat.root_local.os.' + field, return_value=os.getuid()+1), \
                    self.assertRaisesRegex(CoordError, 'real and effective UID'):
                self.call('readiness', **valid)
        for identity in (dict(self.identity, session='other'), dict(self.identity, host_id='other'),
                         dict(self.identity, master_db=self.base/'other.sqlite3')):
            with self.assertRaises(CoordError):
                root_local.execute('readiness', valid, **identity)
        self.executable.unlink(); self.executable.symlink_to(self.wrapper)
        with self.assertRaisesRegex(CoordError, 'owner or path'):
            self.call('readiness', **valid)
        self.executable.unlink(); self.executable.write_text('')
        registry = self.projects.path
        saved = registry.read_bytes(); registry.unlink()
        before = self.snapshot()
        with self.assertRaises(CoordError):
            self.call('readiness', **valid)
        self.assertFalse(registry.exists())
        self.assertEqual(self.snapshot(), before)
        registry.write_bytes(saved)

    def test_local_readiness_missing_target_is_unknown_without_replacement(self):
        self.enroll()
        project = self.projects.create('Readiness target')['id']
        self.roots.sync()
        target = self.projects.db_path(project)
        target.unlink()
        before = self.snapshot()
        result = self.call('readiness', projects=[project, 'default'], expected_thread=self.thread)
        self.assertEqual([row['project'] for row in result['projects']], [project, 'default'])
        self.assertEqual(result['projects'][0]['registration']['state'], 'unknown')
        self.assertFalse(target.exists())
        self.assertEqual(self.snapshot(), before)


if __name__ == '__main__':
    unittest.main()
