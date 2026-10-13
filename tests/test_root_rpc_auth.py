"""Shared bearer boundaries for the root orchestrator HTTP RPC."""
import contextlib
import subprocess
import time
import uuid
from unittest import mock

from agent_chat.agent_control import AgentControlState
from agent_chat.core import Coordinator
from agent_chat.root_lifecycle import schema as stop_schema
from agent_chat.root_team_reset import RootTeamReset, encoded, schema as team_reset_schema
from agent_chat.web import Handler
from test_remote_web import RemoteWebFixture


class RootRpcSharedBearerTests(RemoteWebFixture):
    def setUp(self):
        super().setUp()
        self.thread_id = str(uuid.uuid4())
        self.call('register', agent='root')
        self.call('bind', thread=self.thread_id)
        with contextlib.closing(Coordinator(self.db, 'a')) as coord:
            AgentControlState(coord)
            coord.db.execute('INSERT INTO agent_control_observations VALUES(?,?,?,?,?,?,?,?,?,?)', (
                'a', self.thread_id, 'idle', None, 0, 'test-model', 'high',
                self.tmp.name, '{}', time.time()))
        self.server.roots.enroll('default', 'a', 'mac', self.thread_id, True)

    def root_payload(self, op, *, session='a', host='mac', **params):
        return {'op': op, 'session': session, 'host_id': host, 'params': params}

    def root_request(self, op, *, session='a', host='mac', scope=None, **params):
        headers = {'X-Agent-Chat-Project': scope} if scope else None
        return self.request('POST', '/api/root/rpc',
                            self.root_payload(op, session=session, host=host, **params),
                            headers=headers)

    def assert_jq(self, raw, expression):
        checked = subprocess.run(['jq', '-e', expression], input=raw, capture_output=True)
        self.assertEqual(checked.returncode, 0, checked.stderr.decode())

    def jq_text(self, raw, expression):
        checked = subprocess.run(['jq', '-er', expression], input=raw, capture_output=True)
        self.assertEqual(checked.returncode, 0, checked.stderr.decode())
        return checked.stdout.decode().strip()

    def test_canonical_identity_cannot_use_shared_bearer_for_privileged_or_mixed_rpc(self):
        privileged = {
            'enroll': {'expected_thread': self.thread_id, 'confirm': True},
            'stop-all': {'request_id': str(uuid.uuid4()), 'confirm': True},
            'team-reset-preview': {'project': 'default', 'message_ids': []},
            'team-reset-status': {'project': 'default', 'request_id': str(uuid.uuid4())},
            'team-reset': {'project': 'default', 'message_ids': [],
                           'request_id': str(uuid.uuid4()), 'expected_digest': 'spoofed',
                           'confirm': True},
            'configure': {'project': 'default', 'agent': 'po', 'cwd': self.tmp.name,
                          'prompt': 'spoofed', 'request_id': str(uuid.uuid4()),
                          'confirm': True, 'model': None, 'effort': None},
        }
        mixed = self.root_payload('fresh-status', request_id=str(uuid.uuid4()), confirm=True)
        calls = []
        original = Handler.root_action

        def tracked(handler, data):
            calls.append(data)
            return original(handler, data)

        with mock.patch.object(Handler, 'root_action', tracked):
            for op, params in privileged.items():
                with self.subTest(op=op):
                    status, raw, _ = self.request('POST', '/api/root/rpc',
                                                  self.root_payload(op, **params))
                    self.assertEqual(status, 400, raw)
                    self.assertIn(b'shared bearer permits only', raw)
            status, raw, _ = self.request('POST', '/api/root/rpc', mixed)
            self.assertEqual(status, 400, raw)
            self.assertIn(b'shared bearer permits only', raw)
        self.assertEqual(calls, [])
        with self.server.projects.connection() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM root_team_operations').fetchone()[0], 0)

    def test_malformed_readbacks_are_rejected_before_root_dispatch(self):
        rid = str(uuid.uuid4())
        invalid = [
            None,
            [],
            {'op': 'fresh-status', 'session': 'a', 'host_id': 'mac'},
            dict(self.root_payload('fresh-status', request_id=rid), extra=True),
            self.root_payload('fresh-status', session=1, request_id=rid),
            {'op': 'fresh-status', 'session': 'a', 'host_id': 'mac', 'params': []},
            self.root_payload('fresh-status', request_id=''),
            self.root_payload('team-reset-status', project='', request_id=rid),
            self.root_payload('team-reset-preview', project='default', message_ids='not-a-list'),
            self.root_payload('team-reset-preview', project='default', message_ids=['']),
            self.root_payload('unknown-read', request_id=rid),
        ]
        calls = []
        original = Handler.root_action

        def tracked(handler, data):
            calls.append(data)
            return original(handler, data)

        with mock.patch.object(Handler, 'root_action', tracked):
            for payload in invalid:
                with self.subTest(payload=payload):
                    status, raw, _ = self.request('POST', '/api/root/rpc', payload)
                    self.assertEqual(status, 400, raw)
        self.assertEqual(calls, [])

    def test_team_reset_reads_do_not_reflect_persisted_or_caller_selected_authority(self):
        self.call('register', session='worker', agent='worker')
        held = self.call('request', session='worker', resource='file:secret', minutes=5)
        reservation_token = held['token']

        sensitive = 'sensitive operator reset authority must not cross the shared bearer'
        message_status, message_raw, _ = self.request(
            'POST', '/api/messages', {'to': 'a', 'body': sensitive},
            auth='basic', headers={'X-Agent-Chat-CSRF': self.server.csrf_token})
        self.assertEqual(message_status, 200, message_raw)
        authority_message = self.jq_text(message_raw, '.id')
        calls = []
        original = Handler.root_action

        def tracked(handler, data):
            calls.append(data)
            return original(handler, data)

        with mock.patch.object(Handler, 'root_action', tracked):
            preview_status, preview_raw, _ = self.root_request(
                'team-reset-preview', project='default', message_ids=[authority_message])
        self.assertEqual(preview_status, 400, preview_raw)
        self.assertEqual(calls, [])
        self.assertNotIn(sensitive.encode(), preview_raw)
        self.assertNotIn(reservation_token.encode(), preview_raw)
        self.assertNotIn(b'authority', preview_raw)
        self.assertIn(b'shared bearer permits only', preview_raw)

        reset = RootTeamReset(self.server.roots)
        root, registry = reset.scope('default', 'a', 'mac', 'default')
        authority = reset.authority(root, [authority_message])
        with contextlib.closing(Coordinator(self.db)) as coord:
            state = reset.snapshot(coord.db, registry)
            team_reset_schema(coord.db)
            request_id = str(uuid.uuid4())
            coord.db.execute('INSERT INTO root_team_reset_requests VALUES(?,?,?,?,?,?,?,?,NULL)', (
                request_id, 'a', 'default', 'fixture-digest', encoded(authority),
                encoded(state), 'quiescing', time.time()))
        calls.clear()
        with mock.patch.object(Handler, 'root_action', tracked):
            status, reset_raw, _ = self.root_request(
                'team-reset-status', project='default', request_id=request_id)
        self.assertEqual(status, 400, reset_raw)
        self.assertEqual(calls, [])
        self.assertNotIn(sensitive.encode(), reset_raw)
        self.assertNotIn(reservation_token.encode(), reset_raw)
        self.assertNotIn(b'authority', reset_raw)
        self.assertIn(b'shared bearer permits only', reset_raw)

        stop_id = str(uuid.uuid4())
        with self.server.projects.connection() as db:
            db.execute('INSERT INTO root_team_operations VALUES(?,?,?,NULL)',
                       (stop_id, 'a', time.time()))
            db.execute('INSERT INTO root_team_operation_projects VALUES(?,?,?,NULL)',
                       (stop_id, 'default', 'a'))
        with contextlib.closing(Coordinator(self.db)) as coord:
            stop_schema(coord.db)
            coord.db.execute('INSERT INTO root_team_stops VALUES(?,?,1,?,NULL)',
                             (stop_id, 'a', time.time()))
        status, stop_raw, _ = self.root_request('stop-status', request_id=stop_id)
        self.assertEqual(status, 200, stop_raw)
        self.assert_jq(stop_raw, '.request_id == "' + stop_id + '" and .operation == "stop-status"')

        other_project = self.server.projects.create('Other project')['id']
        for changes in ({'session': 'worker'}, {'host': 'other-host'}, {'scope': other_project}):
            with self.subTest(changes=changes):
                call = dict(session='a', host='mac', scope=None)
                call.update(changes)
                denied, raw, _ = self.root_request(
                    'stop-status', session=call['session'], host=call['host'],
                    scope=call['scope'], request_id=stop_id)
                self.assertEqual(denied, 400, raw)

    def test_csrf_operator_root_workflow_and_ordinary_coord_are_unchanged(self):
        status, raw, _ = self.request('POST', '/api/coord',
                                      self.root_payload('status'))
        self.assertEqual(status, 200, raw)

        request_id = str(uuid.uuid4())
        payload = {'expected_thread': self.thread_id, 'prompt': 'Read the retained checkpoint.',
                   'request_id': request_id, 'confirm': True}
        denied, raw, _ = self.request('POST', '/api/root/fresh-start', payload, auth='basic')
        self.assertEqual(denied, 403, raw)
        accepted, raw, _ = self.request(
            'POST', '/api/root/fresh-start', payload, auth='basic',
            headers={'X-Agent-Chat-CSRF': self.server.csrf_token})
        self.assertEqual(accepted, 200, raw)
        self.assert_jq(raw, '.id == "' + request_id + '" and .instruction_origin == "authenticated_operator_ui"')

        status, readback, _ = self.root_request('fresh-status', request_id=request_id)
        self.assertEqual(status, 200, readback)
        self.assertNotIn(b'Read the retained checkpoint.', readback)
        self.assert_jq(readback,
                       '.id == "' + request_id + '" and .instruction_origin == "authenticated_operator_ui" '
                       'and (has("prompt") | not) and (has("settings_digest") | not)')

    def test_readiness_explicit_projection_is_nonmutating_and_minimal(self):
        import hashlib
        from agent_chat.root_orchestrator import RootOrchestrator
        self.call('register', session='po', agent='po')
        po_thread = str(uuid.uuid4())
        self.call('bind', session='po', thread=po_thread)
        with self.server.projects.connection() as db:
            db.execute("UPDATE root_projects SET orchestrator_session='po' WHERE project_id='default'")
        with contextlib.closing(Coordinator(self.db)) as coord:
            coord.db.execute("INSERT INTO project_orchestrators VALUES('po','a')")
        before = {p: hashlib.sha256(p.read_bytes()).hexdigest()
                  for p in self.db.parent.rglob('*.sqlite3')}
        with mock.patch.object(RootOrchestrator, 'sync', side_effect=AssertionError('mutating sync')), \
                mock.patch.object(RootOrchestrator, 'status', side_effect=AssertionError('broad status')), \
                mock.patch('agent_chat.root_orchestrator.Coordinator', side_effect=AssertionError('schema constructor')):
            status, raw, _ = self.root_request('readiness', projects=['default'], expected_thread=self.thread_id)
        self.assertEqual(status, 200, raw)
        self.assert_jq(raw, '.caller.record_match == true and .caller.project == "default" '
                       'and .caller.session == "a" and .caller.host_id == "mac" '
                       'and (.projects | length) == 1 and .projects[0].project == "default" '
                       'and .projects[0].root_session == "a" and .projects[0].po.session == "po" '
                       'and .projects[0].po.host_id == "mac" and .projects[0].po.readiness == "ready" '
                       'and .projects[0].bridge.state == "unknown" '
                       'and .projects[0].bridge.observed_at == null')
        self.assert_jq(raw, '[.. | objects | keys[]] | all(. != "name" and . != "cwd" '
                       'and . != "workspaces" and . != "prompt" and . != "instructions" '
                       'and . != "model" and . != "effort" and . != "error" '
                       'and . != "resources" and . != "queue" and . != "token")')
        self.assertNotIn(str(self.db.parent).encode(), raw)
        self.assertEqual(before, {p: hashlib.sha256(p.read_bytes()).hexdigest()
                                 for p in self.db.parent.rglob('*.sqlite3')})

    def test_readiness_cli_requires_explicit_host_thread_and_targets(self):
        import argparse
        import io
        from agent_chat.cli import _root_main
        captured = []
        args = argparse.Namespace(session='a', api_token='neutral-fixture', project='default')
        with mock.patch('agent_chat.remote.client_host_id', side_effect=AssertionError('implicit host')), \
                mock.patch('agent_chat.cli._logged_call', side_effect=lambda logger, client, route, payload:
                           captured.append((route, payload)) or {'ok': True}), \
                contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(_root_main(['readiness', '--host-id', 'mac', '--expected-thread', self.thread_id,
                                        '--project-id', 'default'], args, 'https://neutral.invalid', mock.Mock()), 0)
        self.assertEqual(captured, [('/api/root/rpc', {'session': 'a', 'host_id': 'mac', 'op': 'readiness',
                                                     'params': {'projects': ['default'], 'expected_thread': self.thread_id}})])

    def test_readiness_rejects_malformed_duplicate_unrelated_and_mixed_selectors(self):
        params = dict(projects=['default'], expected_thread=self.thread_id)
        payloads = [self.root_payload('readiness', **dict(params, projects=value))
                    for value in (None, 'default', [], ['default', 'default'], [''], [1], [{}], ['unrelated'], ['*'])]
        payloads += [self.root_payload('readiness', **dict(params, expected_thread=value)) for value in (None, '', 1)]
        payloads += [self.root_payload('readiness', **dict(params, **extra))
                     for extra in ({'confirm': True}, {'request_id': str(uuid.uuid4())}, {'sync': True})]
        payloads += [dict(self.root_payload('readiness', **params), extra=True)]
        with mock.patch.object(self.server.roots, 'sync', side_effect=AssertionError('sync')):
            for payload in payloads:
                with self.subTest(payload=payload):
                    status, raw, _ = self.request('POST', '/api/root/rpc', payload)
                    self.assertEqual(status, 400, raw)

    def test_readiness_rejects_wrong_caller_host_thread_and_home_project(self):
        import hashlib
        self.call('register', session='worker', agent='worker')
        unrelated = self.server.projects.create('Unrelated neutral project')['id']
        with self.server.projects.connection() as db:
            db.execute('DELETE FROM root_projects WHERE project_id=?', (unrelated,))
        for changes in ({'session': 'worker'}, {'host': 'wrong-host'}, {'scope': unrelated},
                        {'expected_thread': str(uuid.uuid4())}, {'projects': [unrelated]}):
            with self.subTest(changes=changes):
                values = dict(projects=['default'], expected_thread=self.thread_id)
                values.update(changes)
                before = {p: hashlib.sha256(p.read_bytes()).hexdigest()
                          for p in self.db.parent.rglob('*.sqlite3')}
                status, raw, _ = self.root_request('readiness', **values)
                self.assertEqual(status, 400, raw)
                self.assertEqual(before, {p: hashlib.sha256(p.read_bytes()).hexdigest()
                                         for p in self.db.parent.rglob('*.sqlite3')})

    def test_readiness_rejects_missing_changed_stale_and_unverifiable_authority(self):
        import sqlite3
        cases = [
            ("UPDATE bridge_bindings SET thread_id='changed' WHERE session_id='a'",
             "UPDATE bridge_bindings SET thread_id=? WHERE session_id='a'", (self.thread_id,)),
            ("UPDATE bridge_bindings SET parent_session='foreign' WHERE session_id='a'",
             "UPDATE bridge_bindings SET parent_session=NULL WHERE session_id='a'", ()),
            ("UPDATE remote_session_hosts SET host_id='foreign' WHERE session_id='a'",
             "UPDATE remote_session_hosts SET host_id='mac' WHERE session_id='a'", ()),
            ("UPDATE sessions SET id='missing-root' WHERE id='a'",
             "UPDATE sessions SET id='a' WHERE id='missing-root'", ()),
            ("UPDATE root_projections SET home_session='foreign' WHERE session_id='a'",
             "UPDATE root_projections SET home_session='a' WHERE session_id='a'", ()),
            ("UPDATE agent_control_observations SET checked_at=0 WHERE target_session='a'",
             "UPDATE agent_control_observations SET checked_at=? WHERE target_session='a'", (time.time(),)),
            ("UPDATE agent_control_observations SET checked_at=99999999999 WHERE target_session='a'",
             "UPDATE agent_control_observations SET checked_at=? WHERE target_session='a'", (time.time(),)),
            ("UPDATE agent_control_observations SET thread_id='foreign' WHERE target_session='a'",
             "UPDATE agent_control_observations SET thread_id=? WHERE target_session='a'", (self.thread_id,)),
        ]
        for fault, restore, values in cases:
            with self.subTest(fault=fault):
                with sqlite3.connect(self.db) as db:
                    db.execute(fault)
                status, raw, _ = self.root_request('readiness', projects=['default'], expected_thread=self.thread_id)
                self.assertEqual(status, 400, raw)
                with sqlite3.connect(self.db) as db:
                    db.execute(restore, values)
        with sqlite3.connect(self.db) as db:
            db.execute('DROP TABLE agent_control_observations')
        status, raw, _ = self.root_request('readiness', projects=['default'], expected_thread=self.thread_id)
        self.assertEqual(status, 400, raw)
        with sqlite3.connect(self.db) as db:
            self.assertIsNone(db.execute("SELECT 1 FROM sqlite_master WHERE name='agent_control_observations'").fetchone())

    def test_readiness_exact_host_coverage_uses_real_record_times_without_endpoint_disclosure(self):
        import os
        import sqlite3
        from agent_chat.bridge_state import BridgeState
        secret = 'private-native-endpoint-and-error'
        with contextlib.closing(Coordinator(self.db)) as coord:
            BridgeState(coord).heartbeat(secret, os.getpid(), host_id='mac')
            BridgeState(coord).heartbeat('foreign-private-endpoint', os.getpid(), error=secret, host_id='foreign-host')
        cases = [('healthy', time.time(), None), ('stale', time.time() - 100, None),
                 ('unhealthy', time.time(), secret), ('unknown', time.time() + 100, None)]
        for state, stamp, error in cases:
            with self.subTest(state=state):
                with sqlite3.connect(self.db) as db:
                    db.execute("UPDATE bridge_client_runtime SET heartbeat=?,error=? WHERE host_id='mac'", (stamp, error))
                status, raw, _ = self.root_request('readiness', projects=['default'], expected_thread=self.thread_id)
                self.assertEqual(status, 200, raw)
                self.assert_jq(raw, '.projects[0].bridge.state == "' + state + '" '
                               'and .projects[0].bridge.host_id == "mac" '
                               'and .projects[0].bridge.native_endpoint_identity == null')
                self.assert_jq(raw, '.projects[0].bridge.observed_at == ' + ('null' if state == 'unknown' else str(stamp)))
                self.assertNotIn(secret.encode(), raw)
                self.assertNotIn(b'foreign-host', raw)

    def test_readiness_selected_cohort_order_and_unavailable_target_store_are_not_repaired(self):
        target = self.server.projects.create('Enrolled neutral target')['id']
        self.server.roots.sync()  # Fixture enrollment only, never called by the projection.
        target_path = self.server.projects.db_path(target)
        retained = target_path.with_suffix('.retained')
        target_path.rename(retained)
        try:
            with mock.patch.object(self.server.roots, 'sync', side_effect=AssertionError('sync')):
                status, raw, _ = self.root_request('readiness', projects=[target, 'default'], expected_thread=self.thread_id)
            self.assertEqual(status, 200, raw)
            self.assert_jq(raw, '.projects[0].project == "' + target + '" and .projects[1].project == "default" '
                           'and .projects[0].registration.state == "unknown" '
                           'and .projects[0].registration.observed_at == null')
            self.assertFalse(target_path.exists())
        finally:
            retained.rename(target_path)

    def test_readiness_changed_po_binding_and_paused_routing_are_not_repaired(self):
        import sqlite3
        self.call('register', session='po', agent='po')
        self.call('bind', session='po', thread=str(uuid.uuid4()))
        with self.server.projects.connection() as db:
            db.execute("UPDATE root_projects SET orchestrator_session='po' WHERE project_id='default'")
            db.execute("UPDATE root_config SET mode='paused'")
        with sqlite3.connect(self.db) as db:
            db.execute("INSERT INTO project_orchestrators VALUES('po','a')")
            db.execute("UPDATE remote_session_hosts SET host_id='foreign' WHERE session_id='po'")
        status, raw, _ = self.root_request('readiness', projects=['default'], expected_thread=self.thread_id)
        self.assertEqual(status, 200, raw)
        self.assert_jq(raw, '.projects[0].po.readiness == "not_ready" and .projects[0].po.thread_id == null '
                       'and .projects[0].po.host_id == null and .projects[0].routing.state == "not_ready"')
        with sqlite3.connect(self.db) as db:
            self.assertEqual(db.execute("SELECT host_id FROM remote_session_hosts WHERE session_id='po'").fetchone()[0], 'foreign')
        with self.server.projects.connection() as db:
            self.assertEqual(db.execute('SELECT mode FROM root_config').fetchone()[0], 'paused')

    def test_readiness_cli_missing_or_extra_selectors_never_dispatch(self):
        import argparse
        import io
        from agent_chat.cli import _root_main
        args = argparse.Namespace(session='a', api_token='neutral-fixture', project='default')
        invalid = [['readiness'], ['readiness', '--host-id', 'mac', '--expected-thread', self.thread_id],
                   ['readiness', '--project-id', 'default'],
                   ['readiness', '--host-id', 'mac', '--expected-thread', self.thread_id,
                    '--project-id', 'default', '--confirm']]
        with mock.patch('agent_chat.remote.client_host_id', side_effect=AssertionError('implicit host')), \
                mock.patch('agent_chat.cli._logged_call') as dispatch, \
                contextlib.redirect_stderr(io.StringIO()):
            for command in invalid:
                with self.subTest(command=command), self.assertRaises(SystemExit) as result:
                    _root_main(command, args, 'https://neutral.invalid', mock.Mock())
                self.assertEqual(result.exception.code, 2)
        dispatch.assert_not_called()

    def test_rejected_root_readiness_cli_never_creates_implicit_host_state(self):
        import argparse
        import io
        import os
        from pathlib import Path
        from agent_chat.cli import _root_main
        from agent_chat.core import CoordError
        args = argparse.Namespace(session='a', api_token='neutral-fixture', project='default')
        state = Path(self.tmp.name) / 'unused-client-state'
        invalid = [['in', 'default', 'readiness', '--host-id', 'mac'], ['in'],
                   ['in', 'default'], ['readiness'],
                   ['readiness', '--host-id', 'mac', '--expected-thread', self.thread_id]]
        with mock.patch.dict(os.environ, {'AGENT_CHAT_STATE_DIR': str(state)}, clear=True), \
                mock.patch('agent_chat.cli._logged_call') as dispatch, \
                contextlib.redirect_stderr(io.StringIO()):
            for command in invalid:
                with self.subTest(command=command), self.assertRaises((CoordError, SystemExit)):
                    _root_main(command, args, 'https://neutral.invalid', mock.Mock())
                self.assertFalse(state.exists(), 'rejected invocation created implicit host state')
        dispatch.assert_not_called()

    def _assert_missing_registry_read_is_noncreating(self, disappear_at_open=False):
        import hashlib
        import sqlite3
        registry = self.server.projects.path
        retained = registry.with_name(registry.name + '.retained')
        fixture_root = self.db.parent.resolve()
        before = {p: hashlib.sha256(p.read_bytes()).hexdigest()
                  for p in fixture_root.rglob('*.sqlite3')}
        original_connect = sqlite3.connect
        moved = False

        def remove_then_connect(database, *args, **kwargs):
            nonlocal moved
            if not moved and str(database) in (str(registry), registry.as_uri() + '?mode=ro'):
                registry.rename(retained)
                moved = True
            return original_connect(database, *args, **kwargs)

        if not disappear_at_open:
            registry.rename(retained)
            moved = True
        try:
            with mock.patch('agent_chat.web.sqlite3.connect', side_effect=remove_then_connect) if disappear_at_open else contextlib.nullcontext():
                status, raw, _ = self.root_request('readiness', projects=['default'], expected_thread=self.thread_id)
            self.assertTrue(moved, 'fixture did not remove the registry at the selected boundary')
            self.assertEqual(status, 400, raw)
            self.assertFalse(registry.exists(), 'metadata selection recreated a missing registry')
            self.assertEqual(hashlib.sha256(retained.read_bytes()).hexdigest(), before[registry])
            self.assertEqual({p: hashlib.sha256(p.read_bytes()).hexdigest()
                              for p in fixture_root.rglob('*.sqlite3')},
                             {p: digest for p, digest in before.items() if p != registry})
            self.assertNotIn(str(registry).encode(), raw)
        finally:
            if registry.exists():
                registry.rename(registry.with_name(registry.name + '.unexpected-created'))
            if retained.exists():
                retained.rename(registry)

    def test_readiness_missing_registry_does_not_create_database(self):
        self._assert_missing_registry_read_is_noncreating()

    def test_readiness_registry_disappearing_at_open_does_not_create_database(self):
        self._assert_missing_registry_read_is_noncreating(disappear_at_open=True)

    def test_valid_ordinary_root_cli_still_uses_implicit_host(self):
        import argparse
        import io
        from agent_chat.cli import _root_main
        args = argparse.Namespace(session='a', api_token='neutral-fixture', project='default')
        with mock.patch('agent_chat.remote.client_host_id', return_value='ordinary-host') as host, \
                mock.patch('agent_chat.cli._logged_call', return_value={'ok': True}) as dispatch, \
                contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(_root_main(['status'], args, 'https://neutral.invalid', mock.Mock()), 0)
        host.assert_called_once_with()
        self.assertEqual(dispatch.call_args.args[3],
                         {'session': 'a', 'host_id': 'ordinary-host', 'op': 'status', 'params': {}})

    def test_ordinary_project_selection_retains_get_and_context(self):
        from email.message import Message
        handler = object.__new__(Handler)
        handler.path = '/api/state?project=default'
        handler.headers = Message()
        handler.server = mock.Mock()
        handler.server.projects.get.return_value = {'id': 'default', 'name': 'Neutral'}
        handler.server.project_context.return_value = ('neutral-db', 'neutral-sender', 'neutral-manager')
        handler.select_project()
        handler.server.projects.get.assert_called_once_with('default')
        handler.server.project_context.assert_called_once_with('default')
        self.assertEqual((handler.db_path, handler.sender_session, handler.bridge_manager),
                         ('neutral-db', 'neutral-sender', 'neutral-manager'))

    def test_readiness_reports_machine_read_authority_not_named_session_authentication(self):
        status, raw, _ = self.root_request('readiness', projects=['default'], expected_thread=self.thread_id)
        self.assertEqual(status, 200, raw)
        self.assert_jq(raw, '.authority == {"scope":"machine_read", "named_session_authentication":"UNVERIFIED"} '
                       'and .caller.record_match == true and (.caller | has("validated") | not) '
                       'and .caller.project == "default" and .caller.session == "a"')


if __name__ == '__main__':
    import unittest
    unittest.main()
