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
from agent_chat.agent_control import AgentControlState, request_po_status_inspection
from agent_chat.bridge_state import BridgeState
from agent_chat.core import Coordinator, CoordError
from agent_chat.measurement import MeasurementStore
from agent_chat.projects import Projects
from agent_chat.remote_service import dispatch
from agent_chat.root_orchestrator import RootOrchestrator
from agent_chat.supervisor import SupervisorState
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

    def po_admission_fixture(self):
        self.enroll()
        other = self.projects.create('Other stopped project', workspace=str(self.workspace))['id']
        self.call('route', project=other)
        thread = str(uuid.uuid4())
        with contextlib.closing(Coordinator(self.master)) as coord:
            for name, binding in (('po', thread), ('worker', str(uuid.uuid4()))):
                dispatch(coord, {'op': 'register', 'session': name, 'host_id': 'host', 'params': {'agent': name}})
                coord.session = name
                BridgeState(coord).bind(binding)
            coord.session = 'root'
            coord.context(limit=100, max_bytes=100000)
            coord.send('po', 'Resolve coordination blockers; do not restart workers yet')
        with self.projects.connection() as db:
            db.execute('UPDATE root_projects SET orchestrator_session=? WHERE project_id=?', ('po','default'))
        self.roots.sync()
        self.consume()
        stop = str(uuid.uuid4())
        self.call('stop-all', request_id=stop, confirm=True)
        return dict(project='default',po='po',expected_thread=thread,expected_host='host',request_id=stop,confirm=True)

    def test_po_admission_keeps_project_workers_and_other_barriers_stopped(self):
        from agent_chat.root_lifecycle import jobs, stopped, update
        params = self.po_admission_fixture()
        before = self.call('stop-status', request_id=params['request_id'])
        result = self.call('po-admit', **params)
        self.assertTrue(result['effective'])
        self.assertTrue(result['project_stop_active'])
        self.assertFalse(result['workers_released'])
        self.assertEqual(self.call('po-admit', **params), result)
        after = self.call('stop-status', request_id=params['request_id'])
        for old_project, current_project in zip(before['projects'], after['projects']):
            self.assertEqual(current_project['active'], old_project['active'])
            self.assertEqual(current_project['targets'], old_project['targets'])
        project_status = next(item for item in after['projects'] if item['project'] == params['project'])
        self.assertTrue(project_status['po_coordination']['admissions'][0]['effective'])
        with contextlib.closing(Coordinator(self.master)) as coord:
            self.assertFalse(stopped(coord.db,'po'))
            self.assertTrue(stopped(coord.db,'worker'))
            self.assertEqual({job['target_session'] for job in jobs(coord.db,'host')}, {'worker'})
            old = coord.db.execute('SELECT id,status FROM root_team_stop_targets WHERE target_session=?', ('po',)).fetchone()
            with self.assertRaisesRegex(CoordError, 'PO coordination admission'):
                update(coord,old['id'],old['status'],'waiting')
            coord.db.execute('UPDATE bridge_bindings SET thread_id=? WHERE session_id=?', (str(uuid.uuid4()),'po'))
            self.assertTrue(stopped(coord.db,'po'))
        with self.assertRaisesRegex(CoordError, 'binding or host differs'):
            self.call('po-admit', **params)

    def test_scoped_status_reads_only_current_po_mode_and_fresh_or_unknown_observation(self):
        params = self.po_admission_fixture()
        self.call('po-admit', **params)
        scope = dict(project_id=params['project'], expected_po_session=params['po'],
                     expected_thread=params['expected_thread'], expected_host=params['expected_host'],
                     expected_stop_id=params['request_id'])
        path = self.projects.db_path(params['project'])
        checked = time.time()
        with contextlib.closing(Coordinator(path)) as coord:
            coord.db.execute("INSERT OR REPLACE INTO agent_controls VALUES(?,?,?)", (params['po'],'stopped',checked))
            coord.db.execute("INSERT OR REPLACE INTO agent_control_observations VALUES(?,?,?,?,?,?,?,?,?,?)",
                (params['po'],params['expected_thread'],'active','turn-visible-only-as-id',3,None,None,str(self.workspace),'{}',checked))
        root_before = self.master.read_bytes(); project_before = path.read_bytes()
        result = self.call('status', **scope)
        self.assertEqual(result['stored_mode'], 'stopped')
        self.assertTrue(result['admission_effective'])
        self.assertEqual(result['observation']['state'], 'active')
        self.assertEqual(result['observation']['queue_count'], 3)
        self.assertEqual(result['observation']['turn_id'], 'turn-visible-only-as-id')
        self.assertGreaterEqual(result['observation']['age_seconds'], 0)
        self.assertEqual(set(result), {'project_id','po_session','thread','host','stop_id',
                                       'admission_effective','stored_mode','observation'})
        self.assertEqual(set(result['observation']), {'checked_at','age_seconds','state','queue_count','turn_id'})
        self.assertEqual((self.master.read_bytes(), path.read_bytes()), (root_before, project_before))
        with contextlib.closing(Coordinator(path)) as coord:
            coord.db.execute('UPDATE agent_control_observations SET checked_at=? WHERE target_session=?',
                             (time.time()-121, params['po']))
        stale = self.call('status', **scope)['observation']
        self.assertEqual(stale['state'], 'unknown')
        self.assertIsNone(stale['queue_count']); self.assertIsNone(stale['turn_id'])
        self.assertGreater(stale['age_seconds'], 120)
        with contextlib.closing(Coordinator(path)) as coord:
            coord.db.execute('DELETE FROM agent_control_observations WHERE target_session=?', (params['po'],))
        missing = self.call('status', **scope)['observation']
        self.assertEqual(missing, {'checked_at': None, 'age_seconds': None, 'state': 'unknown',
                                   'queue_count': None, 'turn_id': None})
        with contextlib.closing(Coordinator(path)) as coord:
            coord.db.execute('INSERT OR REPLACE INTO agent_control_observations VALUES(?,?,?,?,?,?,?,?,?,?)',
                (params['po'],params['expected_thread'],'active','bad-time-turn',3,None,None,str(self.workspace),'{}','not-a-time'))
        malformed = self.call('status', **scope)['observation']
        self.assertEqual(malformed, {'checked_at': None, 'age_seconds': None, 'state': 'unknown',
                                     'queue_count': None, 'turn_id': None})
        with contextlib.closing(Coordinator(path)) as coord:
            coord.db.execute('UPDATE agent_control_observations SET checked_at=? WHERE target_session=?',
                             (float('inf'), params['po']))
        nonfinite = self.call('status', **scope)['observation']
        self.assertEqual(nonfinite, {'checked_at': None, 'age_seconds': None, 'state': 'unknown',
                                     'queue_count': None, 'turn_id': None})
        self.call('status')

    def test_scoped_status_uses_projects_create_master_lineage_and_rejects_wrong_lineage(self):
        project = self.projects.create('Scoped PO lineage project', workspace=str(self.workspace))['id']
        project_path = self.projects.db_path(project)
        registry_path = Path(str(self.master) + '.projects.sqlite3')
        with contextlib.closing(Coordinator(project_path)) as coord:
            meta = dict(coord.db.execute("SELECT key,value FROM meta WHERE key IN ('project_id','project_registry','schema_version')"))
            self.assertEqual(meta['project_id'], project)
            self.assertEqual(meta['project_registry'], str(self.master.resolve()))
            self.assertEqual(meta['schema_version'], '1')
            root_local._validate_project_lineage(self.master.resolve(), project, meta)
            cases = (
                dict(meta, project_registry=str(registry_path)),
                {key: value for key, value in meta.items() if key != 'project_registry'},
                dict(meta, project_id='wrong-project'),
                dict(meta, schema_version='2'),
            )
            for invalid in cases:
                with self.subTest(metadata=invalid), self.assertRaisesRegex(CoordError, 'project lineage differs'):
                    root_local._validate_project_lineage(self.master.resolve(), project, invalid)

    def test_scoped_status_nondefault_project_root_and_po_role_match_join_writer(self):
        self.enroll()
        project = self.projects.create('Nondefault scoped status project', workspace=str(self.workspace))['id']
        route = self.call('route', project=project)
        project_root = route['session']
        po_thread = str(uuid.uuid4())
        project_path = self.projects.db_path(project)
        with contextlib.closing(Coordinator(project_path)) as coord:
            for name, thread in (('scoped-po', po_thread), ('scoped-worker', str(uuid.uuid4()))):
                dispatch(coord, {'op': 'register', 'session': name, 'host_id': 'host', 'params': {'agent': name}})
                coord.session = name
                BridgeState(coord).bind(thread)
            coord.session = project_root
            coord.context(limit=100, max_bytes=100000)
            coord.send('scoped-po', 'Resolve coordination blockers; do not restart workers yet')
        with self.projects.connection() as db:
            db.execute('UPDATE root_projects SET orchestrator_session=? WHERE project_id=?', ('scoped-po', project))
        self.roots.sync()
        self.consume()
        stop = str(uuid.uuid4())
        self.call('stop-all', request_id=stop, confirm=True)
        admission = dict(project=project, po='scoped-po', expected_thread=po_thread,
                         expected_host='host', request_id=stop, confirm=True)
        self.call('po-admit', **admission)
        scope = dict(project_id=project, expected_po_session='scoped-po', expected_thread=po_thread,
                     expected_host='host', expected_stop_id=stop)
        readonly_paths = (self.master, self.projects.path, project_path)
        before_status = tuple(path.read_bytes() for path in readonly_paths)
        result = self.call('status', **scope)
        self.assertTrue(result['admission_effective'])
        self.assertEqual(result['project_id'], project)
        self.assertEqual(result['stored_mode'], 'unknown')
        self.assertEqual(set(result), {'project_id','po_session','thread','host','stop_id',
                                       'admission_effective','stored_mode','observation'})
        self.assertEqual(result['observation']['state'], 'unknown')
        self.assertEqual(tuple(path.read_bytes() for path in readonly_paths), before_status)
        with contextlib.closing(Coordinator(project_path)) as coord:
            row = coord.db.execute('SELECT session_id,root_session FROM project_orchestrators').fetchone()
            self.assertEqual((row['session_id'], row['root_session']), ('scoped-po', project_root))

        invalid_roles = (('scoped-po', self.identity['session']),
                         ('scoped-po', 'session-wrong-project-root'),
                         ('wrong-po-role', project_root))
        for role_session, role_root in invalid_roles:
            with contextlib.closing(Coordinator(project_path)) as coord:
                coord.db.execute('DELETE FROM project_orchestrators')
                coord.db.execute('INSERT INTO project_orchestrators VALUES(?,?)', (role_session, role_root))
            with self.subTest(role=(role_session, role_root)), self.assertRaisesRegex(
                    CoordError, 'current designation, thread or host differs'):
                self.call('status', **scope)
        with contextlib.closing(Coordinator(project_path)) as coord:
            coord.db.execute('DELETE FROM project_orchestrators')
            coord.db.execute('INSERT INTO project_orchestrators VALUES(?,?)', ('scoped-po', project_root))
            coord.db.execute('UPDATE bridge_bindings SET thread_id=? WHERE session_id=?',
                             (str(uuid.uuid4()), 'scoped-po'))
        with self.assertRaisesRegex(CoordError, 'current designation, thread or host differs'):
            self.call('status', **scope)
        with contextlib.closing(Coordinator(project_path)) as coord:
            coord.db.execute('UPDATE bridge_bindings SET thread_id=? WHERE session_id=?', (po_thread, 'scoped-po'))
            coord.db.execute('UPDATE remote_session_hosts SET host_id=? WHERE session_id=?', ('host', 'scoped-po'))
        with self.projects.connection() as db:
            db.execute('UPDATE root_team_operations SET root_session=? WHERE id=?',
                       ('wrong-canonical-root', stop))
        with self.assertRaisesRegex(CoordError, 'stop admission differs or is inactive'):
            self.call('status', **scope)
        with self.projects.connection() as db:
            db.execute('UPDATE root_team_operations SET root_session=? WHERE id=?', (self.identity['session'], stop))
            db.execute('UPDATE root_team_operation_projects SET root_session=? WHERE request_id=? AND project_id=?',
                       ('wrong-project-root', stop, project))
        with self.assertRaisesRegex(CoordError, 'stop admission differs or is inactive'):
            self.call('status', **scope)
        with self.projects.connection() as db:
            db.execute('UPDATE root_team_operation_projects SET root_session=? WHERE request_id=? AND project_id=?',
                       (project_root, stop, project))
        with contextlib.closing(Coordinator(project_path)) as coord:
            coord.db.execute('UPDATE root_team_stops SET active=0 WHERE id=?', (stop,))
        with self.assertRaisesRegex(CoordError, 'stop admission differs or is inactive'):
            self.call('status', **scope)
        with contextlib.closing(Coordinator(project_path)) as coord:
            coord.db.execute('UPDATE root_team_stops SET active=1 WHERE id=?', (stop,))
            coord.db.execute('UPDATE root_team_stop_targets SET thread_id=? WHERE stop_id=? AND target_session=?',
                             (str(uuid.uuid4()), stop, 'scoped-po'))
        with self.assertRaisesRegex(CoordError, 'stop admission differs or is inactive'):
            self.call('status', **scope)
        with contextlib.closing(Coordinator(project_path)) as coord:
            coord.db.execute('UPDATE root_team_stop_targets SET thread_id=? WHERE stop_id=? AND target_session=?',
                             (po_thread, stop, 'scoped-po'))
            coord.db.execute('DROP TRIGGER root_team_stop_po_admissions_no_update')
            coord.db.execute('UPDATE root_team_stop_po_admissions SET root_session=? WHERE stop_id=?',
                             ('wrong-project-root', stop))
        with self.assertRaisesRegex(CoordError, 'stop admission differs or is inactive'):
            self.call('status', **scope)
        with contextlib.closing(Coordinator(project_path)) as coord:
            coord.db.execute('UPDATE remote_session_hosts SET host_id=? WHERE session_id=?', ('wrong-host', 'scoped-po'))
        with self.assertRaisesRegex(CoordError, 'current designation, thread or host differs'):
            self.call('status', **scope)

    def _admitted_nondefault_po(self):
        self.enroll()
        project = self.projects.create('Nondefault PO status inspection', workspace=str(self.workspace))['id']
        route = self.call('route', project=project)
        project_root = route['session']
        po_thread = str(uuid.uuid4())
        project_path = self.projects.db_path(project)
        with contextlib.closing(Coordinator(project_path)) as coord:
            dispatch(coord, {'op': 'register', 'session': 'inspect-po', 'host_id': 'host',
                             'params': {'agent': 'inspect-po'}})
            coord.session = 'inspect-po'
            BridgeState(coord).bind(po_thread)
            coord.session = project_root
            coord.context(limit=100, max_bytes=100000)
        with self.projects.connection() as db:
            db.execute('UPDATE root_projects SET orchestrator_session=? WHERE project_id=?', ('inspect-po', project))
        self.roots.sync()
        self.consume()
        stop = str(uuid.uuid4())
        self.call('stop-all', request_id=stop, confirm=True)
        self.call('po-admit', project=project, po='inspect-po', expected_thread=po_thread,
                  expected_host='host', request_id=stop, confirm=True)
        scope = dict(project_id=project, expected_po_session='inspect-po', expected_thread=po_thread,
                     expected_host='host', expected_stop_id=stop)
        return project_path, project_root, scope

    def test_scoped_po_status_inspection_persists_only_one_idempotent_request(self):
        project_path, project_root, scope = self._admitted_nondefault_po()
        request_id = str(uuid.uuid4())
        with contextlib.closing(Coordinator(project_path)) as coord:
            schema_before = [tuple(row) for row in coord.db.execute(
                "SELECT type,name,sql FROM sqlite_master ORDER BY type,name")]
            controls_before = [tuple(row) for row in coord.db.execute('SELECT * FROM agent_controls ORDER BY target_session')]
            stops_before = [tuple(row) for row in coord.db.execute('SELECT * FROM root_team_stops ORDER BY id')]
            target_before = [tuple(row) for row in coord.db.execute('SELECT * FROM root_team_stop_targets ORDER BY id')]
            admissions_before = [tuple(row) for row in coord.db.execute('SELECT * FROM root_team_stop_po_admissions ORDER BY stop_id')]
            observations_before = [tuple(row) for row in coord.db.execute('SELECT * FROM agent_control_observations ORDER BY target_session')]
            messages_before = coord.db.execute('SELECT count(*) FROM messages').fetchone()[0]
        created = self.call('po-status-inspect', **scope, request_id=request_id)
        replayed = self.call('po-status-inspect', **scope, request_id=request_id)
        self.assertEqual(created, replayed)
        self.assertEqual(created['status'], 'pending')
        self.assertEqual(created['request_id'], request_id)
        with contextlib.closing(Coordinator(project_path)) as coord:
            self.assertEqual(coord.db.execute('SELECT count(*) FROM agent_control_requests WHERE id=?',
                                               (request_id,)).fetchone()[0], 1)
            row = coord.db.execute('SELECT requester_session,target_session,host_id,thread_id,action,status '
                                   'FROM agent_control_requests WHERE id=?', (request_id,)).fetchone()
            self.assertEqual((row['requester_session'], row['target_session'], row['host_id'], row['thread_id'],
                              row['status']), (project_root, created['po_session'], created['host'],
                                               created['thread'], 'pending'))
            self.assertTrue(row['action'].startswith('root_po_status_inspect|'))
            self.assertEqual([tuple(item) for item in coord.db.execute(
                "SELECT type,name,sql FROM sqlite_master ORDER BY type,name")], schema_before)
            self.assertEqual([tuple(item) for item in coord.db.execute('SELECT * FROM agent_controls ORDER BY target_session')], controls_before)
            self.assertEqual([tuple(item) for item in coord.db.execute('SELECT * FROM root_team_stops ORDER BY id')], stops_before)
            self.assertEqual([tuple(item) for item in coord.db.execute('SELECT * FROM root_team_stop_targets ORDER BY id')], target_before)
            self.assertEqual([tuple(item) for item in coord.db.execute('SELECT * FROM root_team_stop_po_admissions ORDER BY stop_id')], admissions_before)
            self.assertEqual([tuple(item) for item in coord.db.execute('SELECT * FROM agent_control_observations ORDER BY target_session')], observations_before)
            self.assertEqual(coord.db.execute('SELECT count(*) FROM messages').fetchone()[0], messages_before)
            AgentControlState.existing_schema(coord).update(request_id, 'pending', 'completed',
                observation={'state': 'idle', 'turn_id': None, 'queue_count': 0})
            self.assertEqual(coord.db.execute('SELECT status FROM agent_control_requests WHERE id=?',
                                               (request_id,)).fetchone()[0], 'completed')
            observation = coord.db.execute('SELECT thread_id,state,turn_id,queue_count,checked_at,model,cwd '
                                           'FROM agent_control_observations WHERE target_session=?',
                                           (created['po_session'],)).fetchone()
            self.assertEqual(tuple(observation[:4]), (created['thread'], 'idle', None, 0))
            self.assertIsInstance(observation['checked_at'], (int, float))
            self.assertEqual((observation['model'], observation['cwd']), (None, None))
            self.assertEqual([tuple(item) for item in coord.db.execute('SELECT * FROM agent_controls ORDER BY target_session')], controls_before)
            self.assertEqual([tuple(item) for item in coord.db.execute('SELECT * FROM root_team_stops ORDER BY id')], stops_before)
            self.assertEqual([tuple(item) for item in coord.db.execute('SELECT * FROM root_team_stop_targets ORDER BY id')], target_before)
            self.assertEqual([tuple(item) for item in coord.db.execute('SELECT * FROM root_team_stop_po_admissions ORDER BY stop_id')], admissions_before)
        fresh = self.call('status', **scope)
        self.assertEqual(fresh['stored_mode'], 'unknown')
        self.assertEqual((fresh['observation']['state'], fresh['observation']['queue_count'],
                          fresh['observation']['turn_id']), ('idle', 0, None))

    def test_scoped_po_status_inspection_refuses_pending_control_and_scope_drift(self):
        project_path, project_root, scope = self._admitted_nondefault_po()
        with contextlib.closing(Coordinator(project_path)) as coord:
            now = time.time()
            coord.db.execute('''INSERT INTO agent_control_requests
                (id,requester_session,target_session,host_id,thread_id,action,interrupt,status,created_at,updated_at)
                VALUES(?,?,?,?,?,'inspect',0,'waiting',?,?)''',
                (str(uuid.uuid4()), project_root,
                 scope['expected_po_session'], scope['expected_host'], scope['expected_thread'], now, now))
        with self.assertRaisesRegex(CoordError, 'another control request is unresolved'):
            self.call('po-status-inspect', **scope, request_id=str(uuid.uuid4()))
        with contextlib.closing(Coordinator(project_path)) as coord:
            coord.db.execute("UPDATE agent_control_requests SET status='completed'")
            now = time.time()
            coord.db.execute('''INSERT INTO agent_control_requests
                (id,requester_session,target_session,host_id,thread_id,action,interrupt,status,created_at,updated_at)
                VALUES(?,?,?,?,?,'stop',0,'pending',?,?)''',
                (str(uuid.uuid4()), project_root, scope['expected_po_session'], scope['expected_host'],
                 scope['expected_thread'], now, now))
        with self.assertRaisesRegex(CoordError, 'another control request is unresolved'):
            self.call('po-status-inspect', **scope, request_id=str(uuid.uuid4()))
        with contextlib.closing(Coordinator(project_path)) as coord:
            coord.db.execute("UPDATE agent_control_requests SET status='completed'")
            now = time.time()
            coord.db.execute('''INSERT INTO session_resets
                (id,requester_session,target_session,host_id,old_thread_id,prompt,status,created_at,updated_at)
                VALUES(?,?,?,?,?,'preserve assignment','creating',?,?)''',
                (str(uuid.uuid4()), project_root, scope['expected_po_session'], scope['expected_host'],
                 scope['expected_thread'], now, now))
        with self.assertRaisesRegex(CoordError, 'session reset is unresolved'):
            self.call('po-status-inspect', **scope, request_id=str(uuid.uuid4()))
        with contextlib.closing(Coordinator(project_path)) as coord:
            coord.db.execute("UPDATE session_resets SET status='failed'")
            coord.db.execute('UPDATE root_team_stops SET active=0 WHERE id=?', (scope['expected_stop_id'],))
        with self.assertRaisesRegex(CoordError, 'stop admission differs or is inactive'):
            self.call('po-status-inspect', **scope, request_id=str(uuid.uuid4()))

    def test_po_status_inspection_revalidates_project_scope_after_readback(self):
        project_path, _, scope = self._admitted_nondefault_po()
        original = request_po_status_inspection
        def drift_then_request(db, current_scope, request_id, **kwargs):
            with contextlib.closing(Coordinator(project_path)) as coord:
                coord.db.execute('UPDATE remote_session_hosts SET host_id=? WHERE session_id=?',
                                 ('drifted-host', scope['expected_po_session']))
            return original(db, current_scope, request_id, **kwargs)
        with patch('agent_chat.agent_control.request_po_status_inspection', side_effect=drift_then_request):
            with self.assertRaisesRegex(CoordError, 'ownership, binding or original admission changed'):
                self.call('po-status-inspect', **scope, request_id=str(uuid.uuid4()))
        with contextlib.closing(Coordinator(project_path)) as coord:
            self.assertEqual(coord.db.execute('SELECT count(*) FROM agent_control_requests WHERE action LIKE ?',
                                               ('root_po_status_inspect|%',)).fetchone()[0], 0)

    def test_po_status_inspection_rechecks_registry_scope_before_commit(self):
        project_path, _, scope = self._admitted_nondefault_po()
        original = request_po_status_inspection
        def drift_registry_then_request(db, current_scope, request_id, **kwargs):
            with self.projects.connection() as registry:
                registry.execute('UPDATE root_team_operation_projects SET root_session=? '
                                 'WHERE request_id=? AND project_id=?',
                                 ('changed-project-root', scope['expected_stop_id'], scope['project_id']))
            return original(db, current_scope, request_id, **kwargs)
        with patch('agent_chat.agent_control.request_po_status_inspection', side_effect=drift_registry_then_request):
            with self.assertRaisesRegex(CoordError, 'stop admission differs or is inactive'):
                self.call('po-status-inspect', **scope, request_id=str(uuid.uuid4()))
        with contextlib.closing(Coordinator(project_path)) as coord:
            self.assertEqual(coord.db.execute('SELECT count(*) FROM agent_control_requests WHERE action LIKE ?',
                                               ('root_po_status_inspect|%',)).fetchone()[0], 0)

    def test_po_status_inspection_incomplete_schema_fails_without_ddl(self):
        path = self.base / 'incomplete-po-inspection.sqlite3'
        db = sqlite3.connect(path)
        scope = dict(project_id='project-a', project_root='project-root', home_project='default',
                     home_session='canonical-root', home_thread=str(uuid.uuid4()), stop_id=str(uuid.uuid4()),
                     target_session='po', thread_id=str(uuid.uuid4()), host_id='host')
        try:
            before = [tuple(row) for row in db.execute('SELECT type,name,sql FROM sqlite_master ORDER BY type,name')]
            with self.assertRaisesRegex(CoordError, 'complete existing project schema'):
                AgentControlState.request_po_status_inspection(db, scope, str(uuid.uuid4()))
            after = [tuple(row) for row in db.execute('SELECT type,name,sql FROM sqlite_master ORDER BY type,name')]
            self.assertEqual(after, before)
        finally:
            db.close()

    def test_scoped_status_fails_closed_if_attached_database_changes_mid_read(self):
        params = self.po_admission_fixture(); self.call('po-admit', **params)
        scope = dict(project_id=params['project'], expected_po_session=params['po'],
                     expected_thread=params['expected_thread'], expected_host=params['expected_host'],
                     expected_stop_id=params['request_id'])
        path = self.projects.db_path(params['project'])
        with contextlib.closing(Coordinator(path)) as coord:
            coord.db.execute("INSERT OR REPLACE INTO agent_controls VALUES(?,?,?)", (params['po'],'stopped',time.time()))
            coord.db.execute("INSERT OR REPLACE INTO agent_control_observations VALUES(?,?,?,?,?,?,?,?,?,?)",
                (params['po'],params['expected_thread'],'idle',None,0,None,None,str(self.workspace),'{}',time.time()))

        real_connect = sqlite3.connect
        fired = {'value': False}
        class HookConnection(sqlite3.Connection):
            def execute(self, sql, parameters=()):
                cursor = super().execute(sql, parameters)
                if not fired['value'] and 'FROM root_registry.sqlite_master' in sql:
                    fired['value'] = True
                    writer = real_connect(path, timeout=10, isolation_level=None)
                    try:
                        writer.execute("UPDATE agent_control_observations SET state='changed-during-read' WHERE target_session=?",
                                       (params['po'],))
                        writer.commit()
                    finally:
                        writer.close()
                return cursor

        def traced_connect(*args, **kwargs):
            kwargs['factory'] = HookConnection
            return real_connect(*args, **kwargs)

        with patch('agent_chat.root_local.sqlite3.connect', side_effect=traced_connect):
            with self.assertRaisesRegex(CoordError, 'data changed during read'):
                self.call('status', **scope)
        self.assertTrue(fired['value'])

    def test_scoped_status_rejects_partial_types_and_scope_drift(self):
        params = self.po_admission_fixture(); self.call('po-admit', **params)
        scope = dict(project_id=params['project'], expected_po_session=params['po'],
                     expected_thread=params['expected_thread'], expected_host=params['expected_host'],
                     expected_stop_id=params['request_id'])
        for partial in ({'project_id': params['project']},
                        dict(scope, expected_thread=[]), dict(scope, expected_host=4),
                        dict(scope, expected_po_session='wrong-po'),
                        dict(scope, expected_stop_id='not-the-stop'),
                        dict(scope, project_id='../../elsewhere')):
            with self.subTest(partial=partial), self.assertRaises(CoordError):
                self.call('status', **partial)
        with self.assertRaises(CoordError):
            self.call('status', **dict(scope, unexpected='field'))
        self.call('status')
        path = self.projects.db_path(params['project'])
        with contextlib.closing(Coordinator(path)) as coord:
            coord.db.execute('UPDATE project_orchestrators SET root_session=? WHERE session_id=?',
                             ('foreign-root', params['po']))
        with self.assertRaisesRegex(CoordError, 'designation, thread or host'):
            self.call('status', **scope)
        with contextlib.closing(Coordinator(path)) as coord:
            coord.db.execute('UPDATE project_orchestrators SET root_session=? WHERE session_id=?',
                             ('root', params['po']))
            coord.db.execute('UPDATE root_projections SET thread_id=? WHERE session_id=?',
                             ('stale-root-thread', 'root'))
        with self.assertRaisesRegex(CoordError, 'project projection differs'):
            self.call('status', **scope)
        with contextlib.closing(Coordinator(path)) as coord:
            coord.db.execute('UPDATE root_projections SET thread_id=? WHERE session_id=?',
                             (self.thread, 'root'))
            coord.db.execute('UPDATE root_team_stops SET active=0 WHERE id=?', (params['request_id'],))
        with self.assertRaisesRegex(CoordError, 'admission differs or is inactive'):
            self.call('status', **scope)

    def test_root_readbacks_separate_designation_binding_and_effective_po_admission(self):
        from agent_chat.root_lifecycle import stopped
        params = self.po_admission_fixture()
        self.call('po-admit', **params)

        def po_status():
            status = self.call('status')
            return next(row['po_coordination'] for row in status['projects']
                        if row['project_id'] == params['project'])

        initial = po_status()
        self.assertTrue(initial['root_enrollment_current'])
        self.assertEqual(initial['designated_session'], params['po'])
        self.assertTrue(initial['designated_role_current'])
        self.assertEqual(initial['native_thread_id'], params['expected_thread'])
        self.assertIsNone(initial['native_parent_session'])
        self.assertEqual(initial['registered_host_id'], params['expected_host'])
        admission, = initial['admissions']
        self.assertTrue(admission['effective'])
        self.assertTrue(admission['matches_current_designation'])
        self.assertTrue(admission['matches_current_native_binding'])
        self.assertTrue(admission['matches_current_registered_host'])

        stopped_status = self.call('stop-status', request_id=params['request_id'])
        project_status = next(row for row in stopped_status['projects'] if row['project'] == params['project'])
        self.assertEqual(project_status['active'], 1)
        self.assertTrue(project_status['po_coordination']['admissions'][0]['effective'])
        with contextlib.closing(Coordinator(self.master)) as coord:
            self.assertTrue(stopped(coord.db, 'worker'))

        with contextlib.closing(Coordinator(self.master)) as coord:
            coord.db.execute('UPDATE bridge_bindings SET thread_id=? WHERE session_id=?',
                             (str(uuid.uuid4()), params['po']))
        binding_drift = po_status()['admissions'][0]
        self.assertFalse(binding_drift['effective'])
        self.assertFalse(binding_drift['matches_current_native_binding'])

        with contextlib.closing(Coordinator(self.master)) as coord:
            coord.db.execute('UPDATE bridge_bindings SET thread_id=? WHERE session_id=?',
                             (params['expected_thread'], params['po']))
            coord.db.execute('UPDATE remote_session_hosts SET host_id=? WHERE session_id=?',
                             ('other-host', params['po']))
        host_drift = po_status()['admissions'][0]
        self.assertEqual(po_status()['registered_host_id'], 'other-host')
        self.assertFalse(host_drift['effective'])
        self.assertFalse(host_drift['matches_current_registered_host'])

        with contextlib.closing(Coordinator(self.master)) as coord:
            coord.db.execute('UPDATE remote_session_hosts SET host_id=? WHERE session_id=?',
                             (params['expected_host'], params['po']))
            coord.db.execute('UPDATE project_orchestrators SET root_session=? WHERE session_id=?',
                             ('other-root', params['po']))
        owner_drift = po_status()
        self.assertFalse(owner_drift['designated_role_current'])
        self.assertFalse(owner_drift['admissions'][0]['effective'])
        self.assertTrue(owner_drift['admissions'][0]['matches_current_designation'])

        with contextlib.closing(Coordinator(self.master)) as coord:
            coord.db.execute('UPDATE project_orchestrators SET root_session=? WHERE session_id=?',
                             ('root', params['po']))
        with self.projects.connection() as db:
            db.execute('UPDATE root_projects SET orchestrator_session=? WHERE project_id=?',
                       ('worker', params['project']))
        designation_drift = po_status()
        self.assertEqual(designation_drift['designated_session'], 'worker')
        self.assertFalse(designation_drift['designated_role_current'])
        stale_record = designation_drift['admissions'][0]
        self.assertTrue(stale_record['effective'])
        self.assertFalse(stale_record['matches_current_designation'])
        self.assertEqual(stale_record['target_session'], params['po'])
        with self.projects.connection() as db:
            db.execute('UPDATE root_projects SET orchestrator_session=NULL WHERE project_id=?',
                       (params['project'],))
        missing_designation = po_status()
        self.assertIsNone(missing_designation['designated_session'])
        self.assertTrue(missing_designation['admissions'][0]['effective'])
        self.assertFalse(missing_designation['admissions'][0]['matches_current_designation'])

    def test_po_admission_rejects_wrong_target_scope_host_thread_and_interrupt(self):
        from agent_chat.root_lifecycle import stopped, update
        params = self.po_admission_fixture()
        for changed in (dict(po='worker'), dict(expected_thread=str(uuid.uuid4())),
                        dict(expected_host='other-host'), dict(request_id=str(uuid.uuid4())),
                        dict(confirm=False)):
            with self.subTest(changed=changed), self.assertRaises(CoordError):
                self.call('po-admit', **dict(params, **changed))
        with contextlib.closing(Coordinator(self.master)) as coord:
            self.assertTrue(stopped(coord.db,'po'))
            self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM root_team_stop_po_admissions').fetchone()[0], 0)
            target = coord.db.execute('SELECT id,status FROM root_team_stop_targets WHERE target_session=?', ('po',)).fetchone()
            update(coord,target['id'],target['status'],'interrupting',turn_id='existing-turn')
        with self.assertRaisesRegex(CoordError, 'existing stop interrupt'):
            self.call('po-admit', **params)
        self.assertTrue(all(item['active'] for item in self.call('stop-status', request_id=params['request_id'])['projects']))

    def test_po_admission_does_not_release_preexisting_native_controls(self):
        params = self.po_admission_fixture()
        with contextlib.closing(Coordinator(self.master)) as coord:
            coord.db.execute("INSERT INTO agent_control_requests(id,requester_session,target_session,host_id,thread_id,action,interrupt,status,created_at,updated_at) VALUES(?,?,?,?,?,'model',0,'pending',?,?)",
                ('retained-model','root','po','host',params['expected_thread'],time.time(),time.time()))
        with self.assertRaisesRegex(CoordError, 'existing PO native lifecycle request'):
            self.call('po-admit', **params)
        with contextlib.closing(Coordinator(self.master)) as coord:
            self.assertEqual(coord.db.execute('SELECT status FROM agent_control_requests WHERE id=?', ('retained-model',)).fetchone()[0], 'pending')
            self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM root_team_stop_po_admissions').fetchone()[0], 0)

    def test_project_resume_releases_only_selected_original_barrier(self):
        from agent_chat.root_lifecycle import stopped

        self.enroll()
        other = self.projects.create('Other stopped project', workspace=str(self.workspace))['id']
        other_route = self.call('route', project=other)
        self.consume()
        stores = [('default', self.master, 'root', 'unity-worker'),
                  (other, self.projects.db_path(other), other_route['session'], 'other-worker')]
        preserved = {}
        for project, path, root, worker in stores:
            with contextlib.closing(Coordinator(path)) as coord:
                dispatch(coord, {'op': 'register', 'session': worker, 'host_id': 'host',
                                 'params': {'agent': worker}})
                coord.session = worker
                coord.context()
                coord.request('retained-resource', 10)
                coord.session = root
                coord.context(limit=100, max_bytes=100000)
                coord.send(worker, 'Existing queued assignment must be preserved')
                preserved[project] = {table: [tuple(row) for row in coord.db.execute(
                    'SELECT * FROM ' + table + ' ORDER BY rowid')]
                    for table in ('sessions', 'messages', 'resources', 'resource_queue', 'bridge_bindings')}
        stop = str(uuid.uuid4())
        self.call('stop-all', request_id=stop, confirm=True)
        for params in (
                dict(project='default', request_id=str(uuid.uuid4()), confirm=True),
                dict(project='default', request_id=stop, confirm=False)):
            with self.assertRaises(CoordError):
                self.call('resume-project', **params)
        self.assertTrue(all(item['active'] for item in self.call('stop-status', request_id=stop)['projects']))
        result = self.call('resume-project', project='default', request_id=stop, confirm=True)
        self.assertTrue(result['barrier_complete'])
        self.assertEqual([item['project'] for item in result['projects']], ['default'])
        self.assertEqual(result['projects'][0]['active'], 0)
        self.assertEqual(self.call('resume-project', project='default', request_id=stop, confirm=True), result)
        status = self.call('stop-status', request_id=stop)
        self.assertEqual({item['project']: item['active'] for item in status['projects']},
                         {'default': 0, other: 1})
        with self.projects.connection() as db:
            self.assertIsNone(db.execute('SELECT resumed_at FROM root_team_operations WHERE id=?', (stop,)).fetchone()[0])
        for project, path, root, worker in stores:
            with contextlib.closing(Coordinator(path)) as coord:
                self.assertEqual(stopped(coord.db, worker), project != 'default')
                current = {table: [tuple(row) for row in coord.db.execute(
                    'SELECT * FROM ' + table + ' ORDER BY rowid')]
                    for table in preserved[project]}
                self.assertEqual(current, preserved[project])
        self.call('resume-project', project=other, request_id=stop, confirm=True)
        with self.projects.connection() as db:
            self.assertIsNotNone(db.execute('SELECT resumed_at FROM root_team_operations WHERE id=?', (stop,)).fetchone()[0])

    def test_project_resume_requires_original_enrollment_projection(self):
        self.enroll()
        project = self.projects.create('Other project', workspace=str(self.workspace))['id']
        self.call('route', project=project)
        self.consume()
        stop = str(uuid.uuid4())
        self.call('stop-all', request_id=stop, confirm=True)
        late = self.projects.create('Not in original stop')['id']
        with self.assertRaisesRegex(CoordError, 'original stop scope'):
            self.call('resume-project', project=late, request_id=stop, confirm=True)
        with self.projects.connection() as db:
            db.execute('UPDATE root_projects SET joined=0 WHERE project_id=?', (project,))
        with self.assertRaisesRegex(CoordError, 'enrollment changed'):
            self.call('resume-project', project=project, request_id=stop, confirm=True)
        self.assertTrue(all(item['active'] for item in self.call('stop-status', request_id=stop)['projects']))

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

    def test_fresh_recover_uses_exact_local_scope_and_immutable_attempt_id(self):
        self.enroll(); self.consume()
        request_id = str(uuid.uuid4())
        attempt_id = str(uuid.uuid4())
        output = io.StringIO()
        with patch('agent_chat.remote.HttpClient.call', side_effect=AssertionError('fresh recovery must stay local')), \
                patch('agent_chat.root_restart.RootRestart.recover', create=True,
                      return_value={'id': request_id, 'recovery_attempt': attempt_id}) as recover, \
                contextlib.redirect_stdout(output):
            self.assertEqual(cli.main(['root-local', '--home-project', 'default', '--home-session', 'root',
                '--host-id', 'host', 'fresh-recover', '--expected-thread', self.thread,
                '--request-id', request_id, '--attempt-id', attempt_id, '--confirm']), 0)

        recover.assert_called_once_with('default', 'root', 'host', self.thread,
                                        request_id, attempt_id, True)
        checked = subprocess.run(['jq', '-e', '--arg', 'request', request_id, '--arg', 'attempt', attempt_id,
            '.id == $request and .recovery_attempt == $attempt'],
            input=output.getvalue(), text=True, capture_output=True)
        self.assertEqual(checked.returncode, 0, checked.stderr)

    def test_fresh_recover_rejects_bad_attempt_stale_binding_and_extra_candidate_fields(self):
        self.enroll(); self.consume()
        request_id = str(uuid.uuid4())
        with patch('agent_chat.root_restart.RootRestart.recover', create=True) as recover:
            with self.assertRaisesRegex(CoordError, 'attempt_id must be a UUID'):
                self.call('fresh-recover', expected_thread=self.thread, request_id=request_id,
                          attempt_id='not-a-uuid', confirm=True)
            with self.assertRaisesRegex(CoordError, 'expected thread differs'):
                self.call('fresh-recover', expected_thread='stale-thread', request_id=request_id,
                          attempt_id=str(uuid.uuid4()), confirm=True)
            with self.assertRaisesRegex(CoordError, 'invalid local Root method fields'):
                self.call('fresh-recover', expected_thread=self.thread, request_id=request_id,
                          attempt_id=str(uuid.uuid4()), candidate_thread_id=str(uuid.uuid4()), confirm=True)
        recover.assert_not_called()

    def test_fresh_verify_is_exact_local_selection_and_never_uses_http(self):
        self.enroll(); self.consume()
        request_id = str(uuid.uuid4())
        verification_id = str(uuid.uuid4())
        output = io.StringIO()
        with patch('agent_chat.remote.HttpClient.call', side_effect=AssertionError('fresh verification must stay local')), \
                patch('agent_chat.root_restart.RootRestart.verify_handoff', create=True,
                      return_value={'id':request_id,'verification_id':verification_id,
                                    'operator_notice':{'request_id':request_id,'state':'pending','reason':'awaiting proof','prerequisite':'wait'}}) as verify, \
                contextlib.redirect_stdout(output):
            self.assertEqual(cli.main(['root-local','--home-project','default','--home-session','root',
                '--host-id','host','fresh-verify','--expected-thread',self.thread,
                '--request-id',request_id,'--verification-id',verification_id,'--confirm']),0)
        verify.assert_called_once_with('default','root','host',self.thread,request_id,verification_id,True)
        checked = subprocess.run(['jq','-e','--arg','request',request_id,'--arg','verification',verification_id,
            '.id == $request and .verification_id == $verification and .operator_notice.state == "pending"'],
            input=output.getvalue(),text=True,capture_output=True)
        self.assertEqual(checked.returncode,0,checked.stderr)

    def test_fresh_verify_rejects_bad_uuid_stale_binding_and_candidate_argument(self):
        self.enroll(); self.consume()
        request_id = str(uuid.uuid4())
        with patch('agent_chat.root_restart.RootRestart.verify_handoff', create=True) as verify:
            with self.assertRaisesRegex(CoordError,'verification_id must be a UUID'):
                self.call('fresh-verify',expected_thread=self.thread,request_id=request_id,
                          verification_id='invalid',confirm=True)
            with self.assertRaisesRegex(CoordError,'expected thread differs'):
                self.call('fresh-verify',expected_thread='stale-thread',request_id=request_id,
                          verification_id=str(uuid.uuid4()),confirm=True)
            with self.assertRaisesRegex(CoordError,'invalid local Root method fields'):
                self.call('fresh-verify',expected_thread=self.thread,request_id=request_id,
                          verification_id=str(uuid.uuid4()),candidate_thread_id=str(uuid.uuid4()),confirm=True)
        verify.assert_not_called()

    def test_fresh_replace_is_exact_local_audited_request_and_never_uses_http(self):
        self.enroll(); self.consume()
        request_id = str(uuid.uuid4())
        reconciliation_id = str(uuid.uuid4())
        output = io.StringIO()
        replacement = {'reconciliation_id':reconciliation_id,'state':'requested',
            'previous_candidate_id':'failed-candidate','replacement_candidate_id':None,
            'reason':None,'prerequisite':'wait_for_replacement'}
        with patch('agent_chat.remote.HttpClient.call', side_effect=AssertionError('replacement must stay local')), \
                patch('agent_chat.root_restart.RootRestart.replace_candidate', create=True,
                      return_value={'id':request_id,'replacement':replacement,
                          'operator_notice':{'request_id':request_id,'state':'replacement_requested',
                              'reason':None,'prerequisite':'wait_for_replacement'}}) as replace, \
                contextlib.redirect_stdout(output):
            self.assertEqual(cli.main(['root-local','--home-project','default','--home-session','root',
                '--host-id','host','fresh-replace','--expected-thread',self.thread,
                '--request-id',request_id,'--reconciliation-id',reconciliation_id,'--confirm']),0)
        replace.assert_called_once_with('default','root','host',self.thread,request_id,reconciliation_id,True)
        checked = subprocess.run(['jq','-e','--arg','request',request_id,'--arg','reconciliation',reconciliation_id,
            '.id == $request and .replacement.reconciliation_id == $reconciliation and .replacement.state == "requested"'],
            input=output.getvalue(),text=True,capture_output=True)
        self.assertEqual(checked.returncode,0,checked.stderr)

    def test_fresh_replace_rejects_bad_uuid_stale_binding_and_candidate_override(self):
        self.enroll(); self.consume()
        request_id = str(uuid.uuid4())
        with patch('agent_chat.root_restart.RootRestart.replace_candidate', create=True) as replace:
            with self.assertRaisesRegex(CoordError,'reconciliation_id must be a UUID'):
                self.call('fresh-replace',expected_thread=self.thread,request_id=request_id,
                          reconciliation_id='invalid',confirm=True)
            with self.assertRaisesRegex(CoordError,'expected thread differs'):
                self.call('fresh-replace',expected_thread='stale-thread',request_id=request_id,
                          reconciliation_id=str(uuid.uuid4()),confirm=True)
            with self.assertRaisesRegex(CoordError,'invalid local Root method fields'):
                self.call('fresh-replace',expected_thread=self.thread,request_id=request_id,
                          reconciliation_id=str(uuid.uuid4()),candidate_thread_id=str(uuid.uuid4()),confirm=True)
        replace.assert_not_called()

    def test_fresh_retire_is_exact_local_audited_request_and_never_uses_http(self):
        self.enroll(); self.consume()
        request_id = str(uuid.uuid4())
        reconciliation_id = str(uuid.uuid4())
        retirement_id = str(uuid.uuid4())
        output = io.StringIO()
        retirement = {'retirement_id':retirement_id,'state':'retired',
            'outcome':'replacement_start_outcome_unknown_no_native_closure',
            'expected_thread':self.thread,'reason':'Native replacement outcome remains unknown; no native closure is asserted.'}
        result = {'id':request_id,'status':'retired','handoff_state':'retired_uncertain',
            'handoff_ready':False,'next_action':'start_new_fresh_root_request',
            'blocker':'Native replacement outcome remains unknown; no native closure is asserted.',
            'retirement':retirement,
            'operator_notice':{'request_id':request_id,'state':'retired_uncertain',
                'reason':retirement['reason'],'prerequisite':'start_new_fresh_root_request'}}
        with patch('agent_chat.remote.HttpClient.call', side_effect=AssertionError('retirement must stay local')), \
                patch('agent_chat.root_restart.RootRestart.retire_uncertain', create=True,
                      return_value=result) as retire, \
                contextlib.redirect_stdout(output):
            self.assertEqual(cli.main(['root-local','--home-project','default','--home-session','root',
                '--host-id','host','fresh-retire','--expected-thread',self.thread,
                '--request-id',request_id,'--reconciliation-id',reconciliation_id,
                '--retirement-id',retirement_id,'--confirm']),0)
        retire.assert_called_once_with('default','root','host',self.thread,request_id,
                                       reconciliation_id,retirement_id,True)
        checked = subprocess.run(['jq','-e','--arg','request',request_id,'--arg','retirement',retirement_id,
            '.id == $request and .status == "retired" and .handoff_state == "retired_uncertain" and .handoff_ready == false and .next_action == "start_new_fresh_root_request" and .retirement.retirement_id == $retirement and .retirement.outcome == "replacement_start_outcome_unknown_no_native_closure"'],
            input=output.getvalue(),text=True,capture_output=True)
        self.assertEqual(checked.returncode,0,checked.stderr)

    def test_fresh_retire_rejects_bad_uuid_stale_binding_and_candidate_override(self):
        self.enroll(); self.consume()
        request_id = str(uuid.uuid4())
        with patch('agent_chat.root_restart.RootRestart.retire_uncertain', create=True) as retire:
            with self.assertRaisesRegex(CoordError,'retirement_id must be a UUID'):
                self.call('fresh-retire',expected_thread=self.thread,request_id=request_id,
                          reconciliation_id=str(uuid.uuid4()),retirement_id='invalid',confirm=True)
            with self.assertRaisesRegex(CoordError,'expected thread differs'):
                self.call('fresh-retire',expected_thread='stale-thread',request_id=request_id,
                          reconciliation_id=str(uuid.uuid4()),retirement_id=str(uuid.uuid4()),confirm=True)
            with self.assertRaisesRegex(CoordError,'invalid local Root method fields'):
                self.call('fresh-retire',expected_thread=self.thread,request_id=request_id,
                          reconciliation_id=str(uuid.uuid4()),retirement_id=str(uuid.uuid4()),
                          replacement_candidate_id=str(uuid.uuid4()),confirm=True)
        retire.assert_not_called()

    def test_fresh_retire_settings_mismatch_is_typed_local_dispatch(self):
        self.enroll(); self.consume()
        request_id = '876fd5a8-8966-411e-8236-47df1fc8bf99'
        retirement_id = str(uuid.uuid4())
        output = io.StringIO()
        result = {'id': request_id, 'status': 'retired', 'handoff_state': 'retired_settings_mismatch',
                  'handoff_ready': False, 'next_action': 'continue_current_root',
                  'settings_retirement': {'retirement_id': retirement_id}}
        with patch('agent_chat.remote.HttpClient.call', side_effect=AssertionError('must remain local')), \
                patch('agent_chat.root_restart.RootRestart.retire_settings_mismatch', create=True,
                      return_value=result) as retire, contextlib.redirect_stdout(output):
            self.assertEqual(cli.main(['root-local','--home-project','default','--home-session','root',
                '--host-id','host','fresh-retire-settings-mismatch','--expected-thread',self.thread,
                '--request-id',request_id,'--retirement-id',retirement_id,'--confirm']), 0)
        retire.assert_called_once_with('default','root','host',self.thread,request_id,retirement_id,True)
        with self.assertRaisesRegex(CoordError, 'invalid local Root method fields'):
            self.call('fresh-retire-settings-mismatch', expected_thread=self.thread, request_id=request_id,
                      retirement_id=retirement_id, reconciliation_id=str(uuid.uuid4()), confirm=True)
        with self.assertRaisesRegex(CoordError, 'expected thread differs'):
            self.call('fresh-retire-settings-mismatch', expected_thread='stale', request_id=request_id,
                      retirement_id=retirement_id, confirm=True)

    def test_fresh_cancel_pending_is_exact_local_typed_dispatch(self):
        self.enroll(); self.consume()
        request_id = '876fd5a8-8966-411e-8236-47df1fc8bf99'
        cancellation_id = 'e7e2075c-4ab8-4791-a952-e3321c505c03'
        output = io.StringIO()
        result = {'id': request_id, 'status': 'retired', 'handoff_ready': False,
                  'new_thread_id': '01a148f4-3358-79b3-b487-4210622bf57b',
                  'pending_cancellation': {'cancellation_id': cancellation_id,
                      'state': 'cancelled_pending_intent', 'outcome': 'native_outcome_unknown'}}
        with patch('agent_chat.remote.HttpClient.call', side_effect=AssertionError('must remain local')), \
                patch('agent_chat.root_restart.RootRestart.cancel_pending', create=True,
                      return_value=result) as cancel, contextlib.redirect_stdout(output):
            self.assertEqual(cli.main(['root-local','--home-project','default','--home-session','root',
                '--host-id','host','fresh-cancel-pending','--expected-thread',self.thread,
                '--request-id',request_id,'--cancellation-id',cancellation_id,'--confirm']), 0)
        cancel.assert_called_once_with('default','root','host',self.thread,request_id,cancellation_id,True)
        checked = subprocess.run(['jq','-e','--arg','request',request_id,'--arg','cancel',cancellation_id,
            '.id == $request and .status == "retired" and .handoff_ready == false and .pending_cancellation.cancellation_id == $cancel and .pending_cancellation.state == "cancelled_pending_intent" and .pending_cancellation.outcome == "native_outcome_unknown"'],
            input=output.getvalue(),text=True,capture_output=True)
        self.assertEqual(checked.returncode,0,checked.stderr)
        with self.assertRaisesRegex(CoordError, 'cancellation_id must be a UUID'):
            self.call('fresh-cancel-pending', expected_thread=self.thread, request_id=request_id,
                      cancellation_id='bad-id', confirm=True)
        with self.assertRaisesRegex(CoordError, 'expected thread differs'):
            self.call('fresh-cancel-pending', expected_thread='stale', request_id=request_id,
                      cancellation_id=cancellation_id, confirm=True)
        with self.assertRaisesRegex(CoordError, 'invalid local Root method fields'):
            self.call('fresh-cancel-pending', expected_thread=self.thread, request_id=request_id,
                      cancellation_id=cancellation_id, reconciliation_id=str(uuid.uuid4()), confirm=True)

    def test_investigation_admit_dispatches_exact_three_existing_message_control_pairs_locally(self):
        self.enroll(); self.consume()
        inlst_workspace = self.base / 'inlst-workspace'; inlst_workspace.mkdir()
        lmx_workspace = self.base / 'lmx-workspace'; lmx_workspace.mkdir()
        inlst_project = self.projects.create('INLST adapter fixture', workspace=str(inlst_workspace))['id']
        lmx_project = self.projects.create('LMX adapter fixture', workspace=str(lmx_workspace))['id']
        self.call('route', project=inlst_project)
        self.call('route', project=lmx_project)
        cases = [
            (inlst_project, '3fc63326c5e84ee2b0c5072e16642ecd',
             'a98af69ad0c047a9900904e39343c59d', '01a11cf7-9e5b-7d50-9808-633e43a727c9',
             'message_aB7D3UpFRxbmSaz3vB-bHznOUEuGUU0J', 'e1f4c788-d056-4b6d-8383-5d06db600227'),
            (lmx_project, '0c50366f591945b7b515796c4dffccff',
             '609d991027bb44718381eeb90c53e6cd', '01a12b82-0434-7902-9556-ef40c84643dd',
             'message_8WRFcawOVK5lveBd-Qt71RnaNQiH_aoc', 'b0bdc0b0-f401-459a-9603-1e2f298b2825'),
            (lmx_project, '0c50366f591945b7b515796c4dffccff',
             '9527d0abd3cb4e11af238cf3607fd153', '01a11cfd-3ce5-71b3-9b02-762a39db61d0',
             'message_7pI-MoO3zgIS0iD2Na0bZQldRMkX-GBS', '30ddf724-33b8-4fb6-a4a0-81372307df5d'),
        ]
        stop_id = '09ddf267-e992-4fd7-a5ab-a5ee39bdc7e3'
        outputs = []
        with patch('agent_chat.remote.HttpClient.call', side_effect=AssertionError('must remain local')), \
                patch('agent_chat.root_orchestrator.RootOrchestrator.admit_investigation', create=True) as admit:
            for project, po, target, thread, message_id, inspect_id in cases:
                request_id = str(uuid.uuid4())
                expiry = time.time() + 1800
                result = {'status':'admitted','request_id':request_id,'stop_id':stop_id,
                    'project':project,'po_session':po,'target_session':target,'thread_id':thread,
                    'host_id':'host_o6KZLd0kkjchCaH8OjoJZ9aZXHDlFiE2','message_id':message_id,
                    'inspection_request_id':inspect_id,'expires_at':expiry,'workers_released':False,
                    'native_execution_proven':False,'operation':'investigation-admit'}
                admit.return_value = result
                output = io.StringIO()
                with contextlib.redirect_stdout(output):
                    self.assertEqual(cli.main(['root-local','--home-project','default','--home-session','root',
                        '--host-id','host','investigation-admit',
                        '--target-project-id',project,'--po',po,'--target',target,'--expected-thread',thread,
                        '--expected-host','host_o6KZLd0kkjchCaH8OjoJZ9aZXHDlFiE2','--stop-id',stop_id,
                        '--request-id',request_id,'--message-id',message_id,'--inspection-request-id',inspect_id,
                        '--expires-at',str(expiry),'--confirm']),0)
                admit.assert_called_with('default','root','host',
                    project,po,target,thread,'host_o6KZLd0kkjchCaH8OjoJZ9aZXHDlFiE2',stop_id,
                    request_id,message_id,inspect_id,expiry,True)
                checked = subprocess.run(['jq','-e','--arg','message',message_id,'--arg','inspection',inspect_id,
                    '.status == "admitted" and .message_id == $message and .inspection_request_id == $inspection and .workers_released == false and .native_execution_proven == false'],
                    input=output.getvalue(),text=True,capture_output=True)
                self.assertEqual(checked.returncode,0,checked.stderr)
                outputs.append(output.getvalue())
        self.assertEqual(admit.call_count,3)

    def test_investigation_admit_rejects_invalid_ids_expiry_and_extra_fields(self):
        self.enroll(); self.consume()
        request_id = str(uuid.uuid4())
        params = dict(target_project='target-project',po='po',target='worker',expected_thread=self.thread,
            expected_host='host',stop_id=str(uuid.uuid4()),request_id=request_id,
            message_id='message_existing',inspection_request_id=str(uuid.uuid4()),
            expires_at=time.time()+1200,confirm=True)
        with patch('agent_chat.root_orchestrator.RootOrchestrator.admit_investigation', create=True) as admit:
            with self.assertRaisesRegex(CoordError,'invalid local Root target_project'):
                self.call('investigation-admit',**dict(params,target_project=None))
            with self.assertRaisesRegex(CoordError,'stop_id must be a UUID'):
                self.call('investigation-admit',**dict(params,stop_id='not-a-uuid'))
            with self.assertRaisesRegex(CoordError,'investigation expiry must be a finite Unix timestamp'):
                self.call('investigation-admit',**dict(params,expires_at=float('inf')))
            with self.assertRaisesRegex(CoordError,'invalid local Root method fields'):
                self.call('investigation-admit',**dict(params,worker_released=True))
        admit.assert_not_called()

    def test_investigation_preflight_is_local_typed_and_has_no_confirm_flag(self):
        self.enroll(); self.consume()
        project_dir = self.base / 'preflight-project'; project_dir.mkdir()
        target_project = self.projects.create('Preflight fixture', workspace=str(project_dir))['id']
        self.call('route', project=target_project)
        params = dict(target_project=target_project, po='po-session', target='worker-session',
            expected_thread='worker-thread', expected_host='worker-host', stop_id=str(uuid.uuid4()),
            request_id=str(uuid.uuid4()), message_id='message_exact',
            inspection_request_id=str(uuid.uuid4()), expires_at=time.time()+1200)
        check_names = ('original_stop_is_current_active','target_project_enrollment_is_current',
            'target_project_stop_matches_target_root','original_stop_is_active',
            'stop_target_matches_target_root_thread_host','stop_target_status_admissible',
            'po_stop_admission_matches_target_root','po_designation_matches_target_root',
            'po_binding_matches_recorded_admission','po_host_matches_recorded_admission',
            'worker_binding_matches_stop_target','worker_host_matches_stop_target',
            'message_sender_recipient_matches_po_worker','inspection_request_present',
            'inspection_request_target_matches_worker','inspection_request_thread_matches_worker',
            'inspection_request_host_matches_worker','inspection_request_action_is_inspect',
            'inspection_request_is_noninterrupting','inspection_request_matches_root_worker_binding',
            'inspection_request_requester_matches_target_root',
            'inspection_request_requester_matches_current_admitted_po',
            'inspection_request_status_admissible','expiry_within_one_hour',
            'request_and_message_scope_available','message_not_batched','message_has_no_existing_delivery',
            'no_other_active_stop_barrier','message_unread_by_worker','worker_has_no_other_pending_control',
            'worker_has_no_pending_reset','worker_control_is_running','worker_is_not_quarantined')
        checks = {name: index != 0 for index, name in enumerate(check_names)}
        expected = {'operation':'investigation-preflight','status':'blocked',
            'checks':checks,'failed_checks':[check_names[0]]}
        with patch('agent_chat.remote.HttpClient.call', side_effect=AssertionError('must remain local')), \
                patch('agent_chat.root_orchestrator.RootOrchestrator.investigation_preflight', create=True) as preflight, \
                patch('agent_chat.projects.Projects.__init__', side_effect=AssertionError('must not initialize registry')), \
                patch('agent_chat.root_orchestrator.RootOrchestrator.__init__', side_effect=AssertionError('must not initialize Root schema')):
            preflight.return_value = expected
            self.assertEqual(self.call('investigation-preflight', **params), expected)
        preflight.assert_called_once_with('default','root','host',target_project,'po-session','worker-session',
            'worker-thread','worker-host',params['stop_id'],params['request_id'],'message_exact',
            params['inspection_request_id'],params['expires_at'])

    def test_investigation_preflight_rejects_invalid_scope_without_backend_call(self):
        self.enroll(); self.consume()
        params = dict(target_project='target-project',po='po',target='worker',expected_thread=self.thread,
            expected_host='host',stop_id=str(uuid.uuid4()),request_id=str(uuid.uuid4()),
            message_id='message_exact',inspection_request_id=str(uuid.uuid4()),expires_at=time.time()+1200)
        with patch('agent_chat.root_orchestrator.RootOrchestrator.investigation_preflight', create=True) as preflight:
            with self.assertRaisesRegex(CoordError,'invalid local Root target_project'):
                self.call('investigation-preflight', **dict(params,target_project=None))
            with self.assertRaisesRegex(CoordError,'stop_id must be a UUID'):
                self.call('investigation-preflight', **dict(params,stop_id='not-a-uuid'))
            with self.assertRaisesRegex(CoordError,'investigation expiry must be a finite Unix timestamp'):
                self.call('investigation-preflight', **dict(params,expires_at=float('nan')))
            with self.assertRaisesRegex(CoordError,'invalid local Root method fields'):
                self.call('investigation-preflight', **dict(params,confirm=False))
        preflight.assert_not_called()

    def test_investigation_preflight_rejects_unapproved_output_fields(self):
        self.enroll(); self.consume()
        params = dict(target_project='target-project',po='po',target='worker',expected_thread=self.thread,
            expected_host='host',stop_id=str(uuid.uuid4()),request_id=str(uuid.uuid4()),
            message_id='message_exact',inspection_request_id=str(uuid.uuid4()),expires_at=time.time()+1200)
        result = {'operation':'investigation-preflight','status':'eligible',
            'checks':{'scope':True},'failed_checks':[],'message_body':'must not escape'}
        with patch('agent_chat.root_orchestrator.RootOrchestrator.investigation_preflight',
                   create=True, return_value=result) as preflight:
            with self.assertRaisesRegex(CoordError,'invalid projection'):
                self.call('investigation-preflight', **params)
        preflight.assert_called_once()

    def test_settings_continuation_is_exact_local_root_typed_dispatch(self):
        self.enroll(); self.consume()
        target_project = '3ecb90cc6c98456a94319e2d3d09034a'
        expected = {
            'model':'gpt-6-luna','reasoning_effort':'high','cwd':'/workspace/project',
            'workspaces':['/workspace/project'],
            'permissions':{
                'activePermissionProfile':{'id':'standard','extends':None},
                'approvalPolicy':'on-request','approvalsReviewer':'auto_review',
                'runtimeWorkspaceRoots':['/workspace/project'],
                'sandbox':{'type':'workspace-write','writableRoots':['/workspace/project'],
                           'networkAccess':False,'excludeSlashTmp':False,'excludeTmpdirEnvVar':False}}}
        params = dict(target_project=target_project,po='po-session',target='worker-session',
            expected_thread='worker-thread',expected_host='worker-host',stop_id=str(uuid.uuid4()),
            admission_request_id=str(uuid.uuid4()),baseline_id=str(uuid.uuid4()),
            continuation_id=str(uuid.uuid4()),message_id='message_exact',
            inspection_request_id=str(uuid.uuid4()),source_message_id='po-source-message',
            source_artifact_name='expected-settings-v1.json',source_artifact_sha256='a'*64,
            source_limit='PO-attested historical transcription; raw snapshot absent',
            expected_settings=expected,expires_at=time.time()+1200,confirm=True)
        result = {'state':'awaiting_current_po_metadata_preload','native_preload_started':False,
                  'original_message_released':False,'operation':'investigation-settings-continuation'}
        with patch('agent_chat.remote.HttpClient.call', side_effect=AssertionError('must remain local')), \
                patch('agent_chat.root_orchestrator.RootOrchestrator.continue_investigation_settings',
                      create=True,return_value=result) as continuation:
            self.assertEqual(self.call('investigation-settings-continuation',**params),result)
        continuation.assert_called_once_with('default','root','host',target_project,'po-session',
            'worker-session','worker-thread','worker-host',params['stop_id'],params['admission_request_id'],
            params['baseline_id'],params['continuation_id'],'message_exact',params['inspection_request_id'],
            'po-source-message','expected-settings-v1.json','a'*64,params['source_limit'],expected,
            params['expires_at'],True)
        with patch('agent_chat.root_orchestrator.RootOrchestrator.continue_investigation_settings',
                   create=True) as continuation:
            with self.assertRaisesRegex(CoordError,'expiry must be a finite Unix timestamp'):
                self.call('investigation-settings-continuation',**dict(params,expires_at=float('inf')))
            with self.assertRaisesRegex(CoordError,'invalid local Root method fields'):
                self.call('investigation-settings-continuation',**dict(params,extra='refuse'))
        continuation.assert_not_called()

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
        for method in ('enroll', 'route', 'configure', 'fresh-start', 'fresh-recover', 'fresh-verify', 'fresh-replace', 'fresh-retire', 'stop-all', 'resume-project', 'po-admit', 'pause-routing', 'resume-routing'):
            with self.subTest(method=method), self.assertRaisesRegex(CoordError, 'shared bearer permits only'):
                Handler.shared_bearer_root_action(NoDispatch(),
                    dict(op=method, session='root', host_id='host', params={}))
        with self.assertRaisesRegex(CoordError, 'shared bearer permits only exact root status readbacks'):
            Handler.shared_bearer_root_action(NoDispatch(), dict(op='status', session='root', host_id='host',
                params={'project_id':'default','expected_po_session':'po','expected_thread':'thread',
                        'expected_host':'host','expected_stop_id':str(uuid.uuid4())}))
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

    def test_local_startup_plan_is_constructor_free_bounded_metadata_only(self):
        self.enroll()
        project = self.projects.create('Startup plan target')['id']
        self.roots.sync()
        with contextlib.closing(Coordinator(self.master)) as home_db:
            home_db.db.execute('INSERT OR REPLACE INTO agent_controls VALUES(?,?,?)',
                ('root', 'running', time.time()))
        with contextlib.closing(Coordinator(self.projects.path)) as directory:
            target_session = directory.db.execute('SELECT session_id FROM root_projects WHERE project_id=?',
                (project,)).fetchone()['session_id']
        with contextlib.closing(Coordinator(self.projects.db_path(project))) as target_db:
            AgentControlState(target_db)
            target_db.db.execute('INSERT OR REPLACE INTO agent_controls VALUES(?,?,?)',
                (target_session, 'running', time.time()))
        with contextlib.closing(Coordinator(self.projects.path)) as coord:
            coord.db.execute('INSERT INTO root_team_operations VALUES(?,?,?,NULL)',
                             ('operation-startup-plan', 'root', time.time()))
            coord.db.execute('INSERT INTO root_team_operation_projects VALUES(?,?,?,?)',
                             ('operation-startup-plan', project, 'root', 'fixture-error'))
            coord.db.execute('''INSERT INTO root_fresh_requests(
                id,home_project,home_session,host_id,old_thread_id,prompt,instruction_origin,status,
                new_thread_id,settings_digest,new_settings_digest,queue_id,error,native_proof_json,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                ('request-startup-plan', 'default', 'root', 'host', self.thread, 'private prompt',
                 'fixture', 'start_uncertain', 'candidate-private', 'digest', None, 'queue-private',
                 'private error', '{"secret":"proof"}', time.time(), time.time()))
        before = self.snapshot()
        with patch('agent_chat.projects.Projects', side_effect=AssertionError('no bootstrap')), \
                patch.object(RootOrchestrator, '__init__', side_effect=AssertionError('no schema')), \
                patch.object(RootOrchestrator, 'sync', side_effect=AssertionError('no sync')), \
                patch.object(RootOrchestrator, 'join', side_effect=AssertionError('no join')), \
                patch.object(RootOrchestrator, 'notify_project', side_effect=AssertionError('no notice')), \
                patch('agent_chat.remote.HttpClient.call', side_effect=AssertionError('no HTTP')):
            result = self.call('startup-plan', projects=['default', project], expected_thread=self.thread)
        self.assertTrue(result['complete'], repr((result['blockers'], result['projects'])))
        self.assertEqual(result['authority'], {'scope': 'machine_read', 'named_session_authentication': 'UNVERIFIED'})
        self.assertEqual(result['operation'], 'startup-plan')
        self.assertEqual(result['caller']['thread_id'], self.thread)
        self.assertEqual([row['project'] for row in result['projects']], ['default', project])
        self.assertEqual(result['unsettled']['team_operations'][0]['id'], 'operation-startup-plan')
        self.assertEqual(result['unsettled']['team_operations'][0]['targets'][0]['project'], project)
        fresh = result['unsettled']['fresh_requests'][0]
        self.assertEqual((fresh['id'], fresh['status'], fresh['new_thread_id'], fresh['queue_id']),
                         ('request-startup-plan', 'start_uncertain', 'candidate-private', 'queue-private'))
        self.assertNotIn('private prompt', repr(result))
        self.assertNotIn('private error', repr(result))
        self.assertNotIn('proof', repr(result))
        self.assertEqual(result['consistency']['kind'], 'independent_read_only_store_snapshots_not_atomic')
        self.assertTrue(result['startup_effects']['withheld_by_unsettled_team_operation'])
        self.assertTrue(result['startup_effects']['withheld_by_unsettled_fresh_request'])
        self.assertEqual(self.snapshot(), before)
        output = io.StringIO()
        with patch('agent_chat.remote.HttpClient.call', side_effect=AssertionError('no HTTP')), \
                patch('agent_chat.remote.client_host_id', side_effect=AssertionError('no implicit host')), \
                contextlib.redirect_stdout(output):
            self.assertEqual(cli.main(['root-local', '--home-project', 'default', '--home-session', 'root',
                '--host-id', 'host', 'startup-plan', '--expected-thread', self.thread,
                '--project-id', 'default', '--project-id', project]), 0)
        checked = subprocess.run(['jq', '-e', '.operation == "startup-plan" and '
            '.complete == true and (.projects | length) == 2 and '
            '.authority.named_session_authentication == "UNVERIFIED"'],
            input=output.getvalue(), text=True, capture_output=True)
        self.assertEqual(checked.returncode, 0, checked.stderr)
        self.assertEqual(self.snapshot(), before)

    def test_local_startup_plan_marks_distinct_po_thread_uncompared_without_changing_gates(self):
        self.enroll()
        project = self.projects.create('Startup plan PO binding scope')['id']
        self.roots.sync()
        po_session = 'po-startup-plan'
        po_thread = str(uuid.uuid4())
        with self.projects.connection() as directory:
            root_session = directory.execute('SELECT session_id FROM root_projects WHERE project_id=?',
                                              (project,)).fetchone()['session_id']
            directory.execute('UPDATE root_projects SET orchestrator_session=? WHERE project_id=?',
                              (po_session, project))
        with contextlib.closing(Coordinator(self.projects.db_path(project))) as target:
            dispatch(target, {'op': 'register', 'session': po_session, 'host_id': 'host',
                              'params': {'agent': 'po'}})
            target.session = po_session
            BridgeState(target).bind(po_thread)
            AgentControlState(target)
            target.db.execute('INSERT INTO project_orchestrators VALUES(?,?)', (po_session, root_session))

        before = {path: path.read_bytes() for path in
                  (self.projects.path, self.master, self.projects.db_path(project))}
        with patch('agent_chat.projects.Projects', side_effect=AssertionError('no bootstrap')), \
                patch.object(RootOrchestrator, '__init__', side_effect=AssertionError('no schema')), \
                patch.object(RootOrchestrator, 'sync', side_effect=AssertionError('no sync')), \
                patch.object(RootOrchestrator, 'join', side_effect=AssertionError('no join')), \
                patch.object(RootOrchestrator, 'notify_project', side_effect=AssertionError('no notice')), \
                patch('agent_chat.remote.HttpClient.call', side_effect=AssertionError('no HTTP')):
            result = self.call('startup-plan', projects=['default', project], expected_thread=self.thread)

        self.assertFalse(result['complete'])
        self.assertIn('project_control_record_unavailable', result['blockers'])
        self.assertIn('designated_po_binding_metadata_unavailable', result['blockers'])
        po = next(item['designated_po'] for item in result['projects'] if item['project'] == project)
        self.assertTrue(po['role_matches'])
        self.assertEqual(po['thread_id'], po_thread)
        self.assertNotEqual(po['thread_id'], result['source']['expected_thread'])
        self.assertFalse(po['binding_matches'])
        self.assertEqual(po['binding_comparison'], {
            'state': 'uncompared', 'reason': 'independent_expected_po_thread_not_supplied'})
        self.assertFalse(po['control_record_present'])
        self.assertIsNone(po['control_mode'])
        self.assertEqual({path: path.read_bytes() for path in before}, before)

    def test_local_startup_plan_rejects_wrong_scope_and_reports_missing_store_without_creation(self):
        self.enroll()
        project = self.projects.create('Startup plan missing store')['id']
        self.roots.sync()
        with self.assertRaises(CoordError):
            self.call('startup-plan', projects=['default'], expected_thread=self.thread)
        for values in ({'projects': [], 'expected_thread': self.thread},
                       {'projects': ['default', 'default'], 'expected_thread': self.thread},
                       {'projects': ['project'] * 65, 'expected_thread': self.thread},
                       {'projects': ['default', {}], 'expected_thread': self.thread},
                       {'projects': ['default'], 'expected_thread': 'wrong'}):
            with self.assertRaises(CoordError):
                self.call('startup-plan', **values)
        for identity in (dict(self.identity, session='other'), dict(self.identity, host_id='other'),
                         dict(self.identity, master_db=self.base/'other.sqlite3')):
            with self.assertRaises(CoordError):
                root_local.execute('startup-plan', {'projects': ['default', project],
                    'expected_thread': self.thread}, **identity)
        target = self.projects.db_path(project)
        saved = target.read_bytes()
        target.unlink()
        before = self.snapshot()
        result = self.call('startup-plan', projects=['default', project], expected_thread=self.thread)
        self.assertFalse(result['complete'])
        missing = next(row for row in result['projects'] if row['project'] == project)
        self.assertIsNone(missing['registration']['binding_matches'])
        self.assertFalse(target.exists())
        self.assertEqual(self.snapshot(), before)
        target.write_bytes(saved)

    def test_local_startup_plan_missing_notice_schema_stays_incomplete_without_bootstrap(self):
        self.enroll()
        project = self.projects.create('Startup plan notice schema')['id']
        self.roots.sync()
        with contextlib.closing(Coordinator(self.master)) as coord:
            coord.db.execute('DROP TABLE root_project_notices')
        before = self.snapshot()
        result = self.call('startup-plan', projects=['default', project], expected_thread=self.thread)
        self.assertFalse(result['complete'])
        self.assertIn('root_notice_schema_unavailable', result['blockers'])
        with root_local._read(self.master) as db:
            self.assertFalse(root_local._table(db, 'root_project_notices'))
        self.assertEqual(self.snapshot(), before)

    def test_local_startup_plan_duplicate_notice_association_is_unknown_and_incomplete(self):
        self.enroll()
        project = self.projects.create('Startup plan duplicate notice')['id']
        self.roots.sync()
        with contextlib.closing(Coordinator(self.master)) as coord:
            row = coord.db.execute('SELECT message_id FROM root_project_notices WHERE project_id=?',
                                   (project,)).fetchone()
            self.assertIsNotNone(row)
            message_id = row['message_id']
            coord.db.execute('DROP TABLE root_project_notices')
            coord.db.execute('CREATE TABLE root_project_notices(project_id TEXT NOT NULL, message_id TEXT NOT NULL)')
            coord.db.execute('INSERT INTO root_project_notices VALUES(?,?)', (project, message_id))
            coord.db.execute('INSERT INTO root_project_notices VALUES(?,?)', (project, message_id))
        before = self.snapshot()
        result = self.call('startup-plan', projects=['default', project], expected_thread=self.thread)
        self.assertFalse(result['complete'])
        self.assertIn('root_notice_duplicate_association', result['blockers'])
        notice = next(item['notice'] for item in result['projects'] if item['project'] == project)
        self.assertEqual(notice, {'state': 'unknown', 'message_id': None,
                                  'type': 'root_project_registration_notice'})
        self.assertEqual(self.snapshot(), before)

    def test_local_startup_plan_describes_unjoined_work_without_reconciling_it(self):
        self.enroll()
        project = self.projects.create('Startup plan unjoined target')['id']
        self.roots.sync()
        with contextlib.closing(Coordinator(self.master)) as home_db:
            home_db.db.execute('INSERT OR REPLACE INTO agent_controls VALUES(?,?,?)',
                ('root', 'running', time.time()))
        with contextlib.closing(Coordinator(self.projects.path)) as directory:
            target_session = directory.db.execute('SELECT session_id FROM root_projects WHERE project_id=?',
                (project,)).fetchone()['session_id']
        with contextlib.closing(Coordinator(self.projects.db_path(project))) as target_db:
            AgentControlState(target_db)
            target_db.db.execute('INSERT OR REPLACE INTO agent_controls VALUES(?,?,?)',
                (target_session, 'running', time.time()))
        with contextlib.closing(Coordinator(self.projects.path)) as directory:
            directory.db.execute('UPDATE root_projects SET joined=0,error=? WHERE project_id=?',
                                 ('stored partial onboarding error', project))
        with contextlib.closing(Coordinator(self.projects.db_path(project))) as target:
            target.db.execute('DELETE FROM root_projections')
        before = self.snapshot()
        result = self.call('startup-plan', projects=['default', project], expected_thread=self.thread)
        target_entry = next(row for row in result['projects'] if row['project'] == project)
        self.assertTrue(result['complete'], repr((result['blockers'], result['projects'])))
        self.assertFalse(target_entry['registration']['joined'])
        self.assertTrue(target_entry['registration']['error_recorded'])
        self.assertFalse(target_entry['registration']['binding_matches'])
        self.assertTrue(result['startup_effects']['sync_may_run'])
        self.assertTrue(result['startup_effects']['join_may_write_bindings_and_projections'])
        self.assertEqual(self.snapshot(), before)

    def maintenance_fixture(self):
        self.enroll()
        project = self.projects.create('Maintenance retained target')['id']
        self.roots.sync()
        self.consume()
        with contextlib.closing(Coordinator(self.projects.db_path(project))) as coord:
            dispatch(coord, {'op': 'register', 'session': 'sender', 'host_id': 'host', 'params': {'agent': 'sender'}})
            AgentControlState(coord)
            alias = coord.db.execute('SELECT session_id FROM root_projections').fetchone()[0]
            for identifier in ('message_prepared', 'message_unread'):
                coord.db.execute('INSERT INTO messages(id,sender_session,recipient_session,body,created_at) VALUES(?,?,?,?,?)',
                    (identifier, 'sender', alias, 'Synthetic retained Root input', time.time()))
            self.assertIsNotNone(BridgeState(coord).prepare(self.thread,
                [{'id': 'message_prepared', 'recipient_session': alias}], 'Synthetic retained wake'))
        with contextlib.closing(Coordinator(self.master, 'root')) as coord:
            control = AgentControlState(coord)
            request = str(uuid.uuid4())
            control.request('root', self.thread, 'inspect', request, False)
            control.update(request, 'pending', 'completed', observation={
                'state': 'active', 'turn_id': 'current-root-turn', 'queue_count': 0,
                'model': 'retained-model', 'reasoning_effort': 'high', 'cwd': str(self.neutral),
                'permissions': {'runtimeWorkspaceRoots': [str(self.neutral)]}})
        stop = str(uuid.uuid4())
        self.call('stop-all', request_id=stop, confirm=True)
        return project

    def maintenance_state(self):
        # Only the lifecycle journal, routing flags and its specifically
        # receipted system completion message may vary.
        result = {}
        with self.projects.connection() as db:
            result['root_projects'] = [tuple(row) for row in db.execute('SELECT * FROM root_projects ORDER BY project_id')]
            result['root_config'] = {key: value for key, value in self.roots.config().items() if key != 'mode'}
            for table in ('root_team_operations', 'root_team_operation_projects'):
                result[table] = [tuple(row) for row in db.execute('SELECT * FROM ' + table + ' ORDER BY 1')]
            projects = [dict(row) for row in db.execute('SELECT project_id,session_id FROM root_projects ORDER BY project_id')]
        for project in projects:
            with contextlib.closing(sqlite3.connect(self.projects.db_path(project['project_id']))) as db:
                for table in ('sessions', 'bridge_bindings', 'remote_session_hosts', 'messages', 'message_reads',
                              'project_orchestrators', 'agent_controls', 'agent_control_requests',
                              'agent_control_observations', 'agent_control_workspace_roots',
                              'operator_identities', 'operator_message_origins',
                              'bridge_jobs', 'bridge_deliveries', 'bridge_job_origins', 'bridge_job_native_attempts',
                              'root_team_stops', 'root_team_stop_targets', 'root_team_stop_interrupts'):
                    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone():
                        notice_filter = ''
                        if table == 'messages' and db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='supervisor_root_routing_notices'").fetchone():
                            notice_filter = ' WHERE id NOT IN (SELECT message_id FROM supervisor_root_routing_notices)'
                        result[(project['project_id'], table)] = db.execute('SELECT * FROM ' + table + notice_filter + ' ORDER BY 1').fetchall()
                result[(project['project_id'], 'projection')] = db.execute('SELECT session_id,home_project,home_session,thread_id,cwd,model,effort FROM root_projections').fetchall()
        return result

    def assert_routing_paused(self, expected):
        with self.projects.connection() as db:
            rows = list(db.execute('SELECT project_id,session_id FROM root_projects'))
        self.assertEqual(len(rows), 2)
        for row in rows:
            with contextlib.closing(sqlite3.connect(self.projects.db_path(row['project_id']))) as db:
                self.assertEqual(db.execute('SELECT paused FROM root_projections WHERE session_id=?', (row['session_id'],)).fetchone()[0], expected)

    def test_maintenance_pause_readback_replay_and_resume_preserve_retained_state(self):
        project = self.maintenance_fixture()
        before = self.maintenance_state()
        params = dict(expected_thread=self.thread, request_id=str(uuid.uuid4()), confirm=True)
        with patch.object(RootOrchestrator, 'join', side_effect=AssertionError('no onboarding')), \
                patch('agent_chat.remote.HttpClient.call', side_effect=AssertionError('no native or HTTP dispatch')):
            first = self.call('pause-routing', **params)
            self.assertEqual(first['status'], 'paused')
            self.assertEqual(self.call('status')['root']['mode'], 'maintenance_paused')
            self.assert_routing_paused(1)
            with contextlib.closing(Coordinator(self.projects.db_path(project))) as coord:
                self.assertEqual(BridgeState(coord).jobs(), [])
                self.assertEqual(BridgeState(coord).pending(), [])
            self.assertEqual(self.maintenance_state(), before)
            self.assertEqual(self.call('pause-routing', **params), first)
            with self.assertRaisesRegex(CoordError, 'reuse its exact request'):
                self.call('pause-routing', **dict(params, request_id=str(uuid.uuid4())))
            self.assertEqual(self.call('resume-routing', confirm=True)['root']['mode'], 'running')
            self.assert_routing_paused(0)
            with contextlib.closing(Coordinator(self.projects.db_path(project))) as coord:
                self.assertEqual(len(BridgeState(coord).jobs()), 1)
                self.assertEqual([row['id'] for row in BridgeState(coord).pending()], ['message_unread'])
            self.assertEqual(self.maintenance_state(), before)
            self.assertEqual(self.call('pause-routing', **params)['status'], 'resumed')
            self.assert_routing_paused(0)
        with self.projects.connection() as db:
            row = dict(db.execute('SELECT * FROM root_routing_pauses').fetchone())
            self.assertEqual(row['id'], params['request_id'])
            self.assertIsNotNone(row['resumed_at'])
            with self.assertRaisesRegex(sqlite3.IntegrityError, 'audit is immutable'):
                db.execute('UPDATE root_routing_pauses SET thread_id=?', ('different',))
            with self.assertRaisesRegex(sqlite3.IntegrityError, 'audit is immutable'):
                db.execute('DELETE FROM root_routing_pauses')
            with self.assertRaisesRegex(sqlite3.IntegrityError, 'cohort is immutable'):
                db.execute('DELETE FROM root_routing_pause_projects')

    def test_maintenance_invalid_identity_thread_confirm_and_inspection_refuse_before_pause(self):
        self.maintenance_fixture()
        params = dict(expected_thread=self.thread, request_id=str(uuid.uuid4()), confirm=True)
        before = self.snapshot()
        for identity in (dict(self.identity, session='different'), dict(self.identity, host_id='different')):
            with self.assertRaises(CoordError):
                root_local.execute('pause-routing', params, **identity)
        for values in (dict(params, confirm=False), dict(params, expected_thread='different'), dict(params, request_id='bad')):
            with self.assertRaises(CoordError):
                self.call('pause-routing', **values)
        for field in ('getuid', 'geteuid'):
            with patch('agent_chat.root_local.os.' + field, return_value=os.getuid()+1), self.assertRaises(CoordError):
                self.call('pause-routing', **params)
        self.assertEqual(self.snapshot(), before)
        with contextlib.closing(Coordinator(self.master)) as coord:
            coord.db.execute("UPDATE agent_control_requests SET updated_at=? WHERE action='inspect'", (time.time()-121,))
        before = self.snapshot()
        for operation, values in (('pause-routing', params), ('resume-routing', dict(confirm=True))):
            with self.assertRaisesRegex(CoordError, 'fresh completed own settings inspection'):
                self.call(operation, **values)
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(self.roots.config()['mode'], 'running')
        self.assert_routing_paused(0)

    def test_maintenance_saved_uuid_rejects_changed_thread_payload(self):
        self.maintenance_fixture()
        request = str(uuid.uuid4())
        self.call('pause-routing', expected_thread=self.thread, request_id=request, confirm=True)
        replacement = str(uuid.uuid4())
        # Simulate a later lifecycle binding. The old UUID cannot acquire the
        # new payload even when the canonical selector now matches that thread.
        with self.projects.connection() as db:
            db.execute('UPDATE root_config SET thread_id=?', (replacement,))
        with contextlib.closing(Coordinator(self.master)) as coord:
            coord.db.execute('UPDATE bridge_bindings SET thread_id=? WHERE session_id=?', (replacement, 'root'))
            coord.db.execute('UPDATE agent_control_observations SET thread_id=? WHERE target_session=?', (replacement, 'root'))
            coord.db.execute("UPDATE agent_control_requests SET thread_id=? WHERE target_session=? AND action='inspect'", (replacement, 'root'))
        before = self.snapshot()
        with self.assertRaisesRegex(CoordError, 'retry payload differs'):
            self.call('pause-routing', expected_thread=replacement, request_id=request, confirm=True)
        self.assertEqual(self.snapshot(), before)

    def test_maintenance_cli_pauses_exact_retained_scope_without_http(self):
        self.maintenance_fixture()
        request = str(uuid.uuid4())
        output = io.StringIO()
        with patch('agent_chat.remote.HttpClient.call', side_effect=AssertionError('no HTTP')), contextlib.redirect_stdout(output):
            self.assertEqual(cli.main(['root-local', '--home-project', 'default', '--home-session', 'root',
                '--host-id', 'host', 'pause-routing', '--expected-thread', self.thread, '--request-id', request, '--confirm']), 0)
        checked = subprocess.run(['jq', '-e', '--arg', 'request', request,
            '.request_id == $request and .status == "paused" and (.projects | length) == 2'],
            input=output.getvalue(), text=True, capture_output=True)
        self.assertEqual(checked.returncode, 0, checked.stderr)
        self.assert_routing_paused(1)

    def test_maintenance_rejects_mismatched_retained_alias_before_pause(self):
        project = self.maintenance_fixture()
        with contextlib.closing(Coordinator(self.projects.db_path(project))) as coord:
            coord.db.execute("UPDATE remote_session_hosts SET host_id='different' WHERE session_id IN (SELECT session_id FROM root_projections)")
        before = self.snapshot()
        with self.assertRaisesRegex(CoordError, 'retained projection, binding or host differs'):
            self.call('pause-routing', expected_thread=self.thread, request_id=str(uuid.uuid4()), confirm=True)
        self.assertEqual(self.snapshot(), before)
        self.assertEqual(self.roots.config()['mode'], 'running')
        self.assert_routing_paused(0)

    def test_maintenance_stale_inspection_and_wrong_thread_never_initialize_new_journal(self):
        self.maintenance_fixture()
        with self.projects.connection() as db:
            db.execute('DROP TABLE root_routing_pause_projects')
            db.execute('DROP TABLE root_routing_pauses')
        before = self.snapshot()
        params = dict(expected_thread=self.thread, request_id=str(uuid.uuid4()), confirm=True)
        with patch('agent_chat.projects.Projects') as constructor:
            with self.assertRaisesRegex(CoordError, 'expected thread differs'):
                self.call('pause-routing', **dict(params, expected_thread='different'))
            constructor.assert_not_called()
        self.assertEqual(self.snapshot(), before)
        with contextlib.closing(Coordinator(self.master)) as coord:
            coord.db.execute("UPDATE agent_control_requests SET updated_at=? WHERE action='inspect'", (time.time()-121,))
        before = self.snapshot()
        with patch('agent_chat.projects.Projects') as constructor:
            with self.assertRaisesRegex(CoordError, 'fresh completed own settings inspection'):
                self.call('pause-routing', **params)
            constructor.assert_not_called()
        self.assertEqual(self.snapshot(), before)

    def test_maintenance_resume_requires_confirmation_consumed_inbox_and_completed_inspection(self):
        self.maintenance_fixture()
        self.call('pause-routing', expected_thread=self.thread, request_id=str(uuid.uuid4()), confirm=True)
        before = self.snapshot()
        with self.assertRaisesRegex(CoordError, 'explicit confirmation'):
            self.call('resume-routing', confirm=False)
        self.assertEqual(self.snapshot(), before)
        with contextlib.closing(Coordinator(self.master)) as coord:
            coord.db.execute("UPDATE agent_control_requests SET status='pending' WHERE action='inspect'")
        before = self.snapshot()
        with self.assertRaisesRegex(CoordError, 'fresh completed own settings inspection'):
            self.call('resume-routing', confirm=True)
        self.assertEqual(self.snapshot(), before)
        with contextlib.closing(Coordinator(self.master)) as coord:
            coord.db.execute("UPDATE agent_control_requests SET status='completed' WHERE action='inspect'")
            operator = coord.db.execute("SELECT id FROM sessions WHERE agent='operator'").fetchone()[0]
            coord.db.execute('INSERT INTO messages(id,sender_session,recipient_session,body,created_at) VALUES(?,?,?,?,?)',
                ('message_synthetic_unread', operator, 'root', 'Synthetic unread operator input', time.time()))
        before = self.snapshot()
        with self.assertRaisesRegex(CoordError, 'read your inbox'):
            self.call('resume-routing', confirm=True)
        self.assertEqual(self.snapshot(), before)
        self.assert_routing_paused(1)

    def test_maintenance_partial_projection_sync_retries_exact_journal(self):
        self.maintenance_fixture()
        params = dict(expected_thread=self.thread, request_id=str(uuid.uuid4()), confirm=True)
        before = self.maintenance_state()
        update = RootOrchestrator._routing_projection
        writes = []
        def fail_second(roots, root, item, paused):
            if paused is True:
                writes.append(item['project_id'])
                if len(writes) == 2:
                    raise OSError('synthetic second projection unavailable')
            return update(roots, root, item, paused)
        with patch.object(RootOrchestrator, '_routing_projection', new=fail_second):
            with self.assertRaisesRegex(OSError, 'second projection unavailable'):
                self.call('pause-routing', **params)
        self.assertEqual(self.call('status')['root']['mode'], 'maintenance_paused')
        with self.projects.connection() as db:
            self.assertEqual(db.execute('SELECT status FROM root_routing_pauses').fetchone()[0], 'pausing')
        self.assertEqual(self.maintenance_state(), before)
        self.assertEqual(self.call('pause-routing', **params)['status'], 'paused')
        self.assert_routing_paused(1)
        self.assertEqual(self.maintenance_state(), before)

    def test_maintenance_rejects_in_progress_turnover_or_detach(self):
        self.maintenance_fixture()
        params = dict(expected_thread=self.thread, request_id=str(uuid.uuid4()), confirm=True)
        for mode in ('detaching', 'detached'):
            with self.projects.connection() as db:
                db.execute('UPDATE root_config SET mode=?', (mode,))
            before = self.snapshot()
            with self.assertRaisesRegex(CoordError, 'detaching or detached'):
                self.call('pause-routing', **params)
            self.assertEqual(self.snapshot(), before)
        with self.projects.connection() as db:
            db.execute("UPDATE root_config SET mode='running'")
        self.call('fresh-start', expected_thread=self.thread, prompt='Synthetic pending turnover',
                  request_id=str(uuid.uuid4()), confirm=True)
        before = self.snapshot()
        with self.assertRaisesRegex(CoordError, 'fresh-start is pending'):
            self.call('pause-routing', **params)
        self.assertEqual(self.snapshot(), before)

    def test_maintenance_pause_readback_mismatch_keeps_exact_journal_recoverable(self):
        project = self.maintenance_fixture()
        params = dict(expected_thread=self.thread, request_id=str(uuid.uuid4()), confirm=True)
        before = self.maintenance_state()
        update = RootOrchestrator._routing_projection
        def revert_alias_write(roots, root, item, paused):
            result = update(roots, root, item, paused)
            if paused is True and item['project_id'] == project:
                # Simulate another writer reverting a successful projection
                # update before the final readback; no write error is raised.
                with contextlib.closing(Coordinator(roots.path(project))) as coord:
                    coord.db.execute('UPDATE root_projections SET paused=0 WHERE session_id=?', (item['session_id'],))
            return result
        with patch.object(RootOrchestrator, '_routing_projection', new=revert_alias_write):
            with self.assertRaisesRegex(CoordError, 'pause readback differs'):
                self.call('pause-routing', **params)
        self.assertEqual(self.roots.config()['mode'], 'maintenance_paused')
        with self.projects.connection() as db:
            journal = dict(db.execute('SELECT * FROM root_routing_pauses WHERE id=?', (params['request_id'],)).fetchone())
        self.assertEqual(journal['status'], 'pausing')
        self.assertEqual(self.maintenance_state(), before)
        self.assertEqual(self.call('pause-routing', **params)['status'], 'paused')
        self.assert_routing_paused(1)
        with self.projects.connection() as db:
            recovered = dict(db.execute('SELECT * FROM root_routing_pauses WHERE id=?', (params['request_id'],)).fetchone())
        self.assertEqual(recovered, dict(journal, status='paused'))
        self.assertEqual(self.maintenance_state(), before)

    def test_maintenance_resume_readback_mismatch_keeps_maintenance_until_retry(self):
        project = self.maintenance_fixture()
        request = str(uuid.uuid4())
        self.call('pause-routing', expected_thread=self.thread, request_id=request, confirm=True)
        before = self.maintenance_state()
        update = RootOrchestrator._routing_projection
        def revert_alias_write(roots, root, item, paused):
            result = update(roots, root, item, paused)
            if paused is False and item['project_id'] == project:
                with contextlib.closing(Coordinator(roots.path(project))) as coord:
                    coord.db.execute('UPDATE root_projections SET paused=1 WHERE session_id=?', (item['session_id'],))
            return result
        with patch.object(RootOrchestrator, '_routing_projection', new=revert_alias_write):
            with self.assertRaisesRegex(CoordError, 'resume readback differs'):
                self.call('resume-routing', confirm=True)
        self.assertEqual(self.roots.config()['mode'], 'maintenance_paused')
        with self.projects.connection() as db:
            journal = dict(db.execute('SELECT * FROM root_routing_pauses WHERE id=?', (request,)).fetchone())
        self.assertEqual(journal['status'], 'paused')
        self.assertIsNone(journal['resumed_at'])
        self.assertEqual(self.maintenance_state(), before)
        self.assertEqual(self.call('resume-routing', confirm=True)['status'], 'resumed')
        self.assert_routing_paused(0)
        self.assertEqual(self.maintenance_state(), before)

    def test_maintenance_resume_emits_one_trusted_pending_notice_and_replay_only_reports(self):
        self.maintenance_fixture()
        before = self.maintenance_state()
        request = str(uuid.uuid4())
        self.call('pause-routing', expected_thread=self.thread, request_id=request, confirm=True)
        result = self.call('resume-routing', confirm=True)
        notice = result['notice_message_id']
        with contextlib.closing(Coordinator(self.master, 'root')) as coord:
            pending = BridgeState(coord).pending()
            self.assertEqual([row['id'] for row in pending], [notice])
            row = coord.db.execute('SELECT * FROM messages WHERE id=?', (notice,)).fetchone()
            described = coord._message_dicts(coord.db, [row])[0]
            self.assertEqual(described['sender_session'], 'system:supervisor')
            self.assertEqual(described['sender_role'], 'agent')
            self.assertEqual(described['instruction_origin'], 'agent_message')
            self.assertIn(request, described['body'])
            self.assertEqual(tuple(coord.db.execute('SELECT sender_session,recipient_session FROM supervisor_notice_provenance WHERE message_id=?', (notice,)).fetchone()), ('system:supervisor', 'root'))
            self.assertFalse(coord.db.execute("SELECT 1 FROM sessions WHERE id='system:supervisor'").fetchone())
        self.assertEqual(self.maintenance_state(), before)
        before_replay = self.snapshot()
        with patch.object(RootOrchestrator, 'fresh', side_effect=AssertionError('completed replay must not consume inbox')), \
                patch.object(RootOrchestrator, 'sync', side_effect=AssertionError('completed replay must not sync routing')):
            self.assertEqual(self.call('resume-routing', confirm=True), result)
        self.assertEqual(self.snapshot(), before_replay)
        with contextlib.closing(Coordinator(self.master, 'root')) as coord:
            self.assertEqual([row['id'] for row in BridgeState(coord).pending()], [notice])
            self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM supervisor_root_routing_notices WHERE request_id=?', (request,)).fetchone()[0], 1)
            with self.assertRaisesRegex(sqlite3.IntegrityError, 'receipt is immutable'):
                coord.db.execute('DELETE FROM supervisor_root_routing_notices WHERE request_id=?', (request,))

    def test_maintenance_completion_notice_recovers_stale_inspection_without_consuming_operator_input(self):
        self.maintenance_fixture()
        before = self.maintenance_state()
        request = str(uuid.uuid4())
        self.call('pause-routing', expected_thread=self.thread, request_id=request, confirm=True)
        with patch.object(SupervisorState, 'root_routing_resumed', side_effect=OSError('synthetic notice store failure')):
            with self.assertRaisesRegex(OSError, 'notice store failure'):
                self.call('resume-routing', confirm=True)
        self.assertEqual(self.roots.config()['mode'], 'running')
        self.assertEqual(self.maintenance_state(), before)
        with self.projects.connection() as db:
            self.assertEqual(db.execute('SELECT status FROM root_routing_pauses WHERE id=?', (request,)).fetchone()[0], 'resumed')
        with contextlib.closing(Coordinator(self.master, 'root')) as coord:
            operator = coord.db.execute("SELECT id FROM sessions WHERE agent='operator'").fetchone()[0]
            coord.db.execute('INSERT INTO messages(id,sender_session,recipient_session,body,created_at) VALUES(?,?,?,?,?)',
                ('message_recovery_operator_input', operator, 'root', 'Unconsumed operator input', time.time()))
            coord.db.execute("UPDATE agent_control_requests SET updated_at=? WHERE action='inspect'", (time.time()-121,))
        before_recovery = self.maintenance_state()
        with patch.object(RootOrchestrator, 'fresh', side_effect=AssertionError('completed recovery must not consume inbox')), \
                patch.object(RootOrchestrator, 'sync', side_effect=AssertionError('completed recovery must not sync routing')):
            result = self.call('resume-routing', confirm=True)
        self.assertEqual(result['request_id'], request)
        self.assertEqual(self.maintenance_state(), before_recovery)
        with contextlib.closing(Coordinator(self.master, 'root')) as coord:
            self.assertFalse(coord.db.execute("SELECT 1 FROM message_reads WHERE message_id='message_recovery_operator_input'").fetchone())
            self.assertEqual({row['id'] for row in BridgeState(coord).pending()}, {'message_recovery_operator_input', result['notice_message_id']})
        before_replay = self.snapshot()
        self.assertEqual(self.call('resume-routing', confirm=True), result)
        self.assertEqual(self.snapshot(), before_replay)

    def test_maintenance_completed_replay_with_stale_inspection_rejects_partial_routing(self):
        project = self.maintenance_fixture()
        self.call('pause-routing', expected_thread=self.thread, request_id=str(uuid.uuid4()), confirm=True)
        self.call('resume-routing', confirm=True)
        with contextlib.closing(Coordinator(self.master)) as coord:
            coord.db.execute("UPDATE agent_control_requests SET updated_at=? WHERE action='inspect'", (time.time()-121,))
        with contextlib.closing(Coordinator(self.projects.db_path(project))) as coord:
            coord.db.execute('UPDATE root_projections SET paused=1')
        before = self.snapshot()
        with patch('agent_chat.projects.Projects') as constructor:
            with self.assertRaisesRegex(CoordError, 'fresh completed own settings inspection'):
                self.call('resume-routing', confirm=True)
            constructor.assert_not_called()
        self.assertEqual(self.snapshot(), before)

    def test_maintenance_supervisor_notice_rejects_uncommitted_or_different_audit(self):
        self.maintenance_fixture()
        request = str(uuid.uuid4())
        self.call('pause-routing', expected_thread=self.thread, request_id=request, confirm=True)
        with contextlib.closing(Coordinator(self.master, 'root')) as coord:
            supervisor = SupervisorState(coord)
            before = self.snapshot()
            with self.assertRaisesRegex(CoordError, 'completed current resume audit'):
                supervisor.root_routing_resumed(self.roots, 'default', 'root', 'host', request)
            self.assertEqual(self.snapshot(), before)
        result = self.call('resume-routing', confirm=True)
        with contextlib.closing(Coordinator(self.master, 'root')) as coord:
            supervisor = SupervisorState(coord)
            before = self.snapshot()
            with self.assertRaises(CoordError):
                supervisor.root_routing_resumed(self.roots, 'default', 'root', 'different-host', request)
            with self.assertRaisesRegex(CoordError, 'completed current resume audit'):
                supervisor.root_routing_resumed(self.roots, 'default', 'root', 'host', str(uuid.uuid4()))
            self.assertEqual(self.snapshot(), before)
            self.assertEqual([row['id'] for row in BridgeState(coord).pending()], [result['notice_message_id']])


if __name__ == '__main__':
    unittest.main()
