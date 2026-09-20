import contextlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from agent_chat.core import CoordError
from agent_chat import bridge_client, cli


class _Http:
    instances = []

    def __init__(self, server, token, project='default'):
        self.server, self.token, self.project = server, token or 'test-token', project
        self.calls = []
        self.instances.append(self)

    def call(self, path, payload):
        self.calls.append((path, payload))
        if path == '/api/projects/rpc':
            return {'projects': [{'id': 'default'}, {'id': 'other'}]}
        return {'result': {}}


class _Rpc:
    instances = []

    def __init__(self, endpoint):
        self.endpoint = endpoint
        self.connected = 0
        self.closed = 0
        self.instances.append(self)

    def connect(self):
        self.connected += 1
        return self

    def close(self):
        self.closed += 1


class _State:
    instances = []

    def __init__(self, client, server, identity):
        self.client, self.server, self.identity = client, server, identity
        self.events = []
        self.instances.append(self)

    def call(self, op, **params):
        self.events.append((op, params))
        return {}

    def heartbeat(self, server, pid, error=None):
        self.events.append(('heartbeat', {'pid': pid, 'error': error}))


class _Bridge:
    ticks = []
    fail_default = False

    def __init__(self, state, rpc):
        self.state, self.rpc = state, rpc

    def tick(self):
        self.ticks.append(self.state.client.project)
        if self.fail_default and self.state.client.project == 'default':
            raise CoordError('default unavailable')


class BridgeClientTests(unittest.TestCase):
    def setUp(self):
        for value in (_Http.instances, _Rpc.instances, _State.instances, _Bridge.ticks):
            value.clear()
        _Bridge.fail_default = False
        self.patches = [
            mock.patch.object(bridge_client, 'host_id', return_value='h'),
            mock.patch.object(bridge_client, 'UsageGuard', return_value=mock.Mock(check=mock.Mock(return_value={'blocked': False}))),
            mock.patch.object(bridge_client, 'HttpClient', _Http),
            mock.patch.object(bridge_client, 'RpcClient', _Rpc),
            mock.patch.object(bridge_client, 'RemoteBridgeState', _State),
            mock.patch.object(bridge_client, 'Bridge', _Bridge),
            mock.patch.object(bridge_client, 'client_identity', side_effect=lambda *_: contextlib.nullcontext({'owner': 'o', 'secret': 's', 'host_id': 'h'})),
        ]
        for patch in self.patches:
            patch.start()
        self.addCleanup(mock.patch.stopall)

    def test_default_once_discovers_and_dispatches_each_project_after_another_fails(self):
        _Bridge.fail_default = True
        result = bridge_client.main(['--server', 'https://chat.example', '--connect-only', '--once'])
        self.assertEqual(result, 2)
        self.assertEqual(_Bridge.ticks, ['default', 'other'])
        self.assertEqual([state.client.project for state in _State.instances], ['default', 'other'])
        self.assertEqual(_Rpc.instances[0].endpoint, 'ws://127.0.0.1:4500')

    def test_usage_pause_skips_project_discovery_and_all_wakes(self):
        bridge_client.UsageGuard.return_value.check.return_value = {'blocked': True, 'reason': 'reserve reached'}
        with mock.patch.object(bridge_client, '_project_ids') as discover:
            result = bridge_client.main(['--server', 'https://chat.example', '--connect-only', '--once'])
        self.assertEqual(result, 2)
        discover.assert_not_called()
        self.assertEqual(_Bridge.ticks, [])

    def test_usage_pause_checks_again_and_dispatches_after_manual_clear(self):
        class TwoPassEvent:
            def __init__(self): self.waits = 0
            def is_set(self): return self.waits >= 2
            def wait(self, _): self.waits += 1
            def set(self): self.waits = 2

        bridge_client.UsageGuard.return_value.check.side_effect = [
            {'blocked': True, 'reason': 'reserve reached'}, {'blocked': False},
        ]
        with mock.patch.object(bridge_client.threading, 'Event', TwoPassEvent):
            result = bridge_client.main(['--server', 'https://chat.example', '--connect-only'])
        self.assertEqual(result, 0)
        self.assertEqual(_Bridge.ticks, ['default', 'other'])

    def test_client_launcher_accepts_token_flags_without_a_command(self):
        for flags in (['--token=flag-token'], ['--token', 'flag-token'],
                      ['--api-token=flag-token'], ['bridge', '--token=flag-token']):
            with self.subTest(flags=flags):
                process = mock.Mock()
                process.poll.return_value = None
                first_client = len(_Http.instances)
                with mock.patch.dict(os.environ, {'AGENT_CHAT_API_TOKEN': 'environment-token'}), \
                        mock.patch.object(bridge_client.subprocess, 'Popen', return_value=process) as popen:
                    result = cli.main([*flags, '--server', 'https://chat.example', '--once'])
                self.assertEqual(result, 0)
                popen.assert_called_once()
                self.assertEqual(popen.call_args.kwargs['env']['AGENT_CHAT_API_TOKEN'], 'flag-token')
                self.assertTrue(all(client.token == 'flag-token' for client in _Http.instances[first_client:]))
                process.terminate.assert_called_once_with()

    def test_client_launcher_accepts_connect_only_without_a_command(self):
        with mock.patch.object(bridge_client.subprocess, 'Popen') as popen:
            result = cli.main(['--connect-only', '--token=flag-token', '--once'])
        self.assertEqual(result, 0)
        popen.assert_not_called()
        self.assertEqual(_Http.instances[0].token, 'flag-token')

    def test_owned_local_app_server_inherits_chat_target_and_is_stopped(self):
        process = mock.Mock()
        process.poll.return_value = None
        inherited = {'AGENT_CHAT_SESSION': 'parent', 'AGENT_CHAT_TOKEN': 'lease'}
        with mock.patch.dict(os.environ, inherited, clear=False), \
                mock.patch.object(bridge_client.subprocess, 'Popen', return_value=process) as popen:
            result = bridge_client.main(['--server', 'https://chat.example', '--api-token', 'flag-token',
                                         '--project', 'other', '--once', '--codex-bin', 'custom-codex'])
        self.assertEqual(result, 0)
        self.assertEqual(popen.call_args.args[0], ['custom-codex', 'app-server', '--listen', 'ws://127.0.0.1:4500'])
        self.assertEqual(popen.call_args.kwargs['cwd'], __import__('os').getcwd())
        environment = popen.call_args.kwargs['env']
        self.assertEqual(environment['AGENT_CHAT_SERVER'], 'https://chat.example')
        self.assertEqual(environment['AGENT_CHAT_API_TOKEN'], 'flag-token')
        self.assertEqual(environment['AGENT_CHAT_PROJECT'], 'other')
        for name in inherited:
            self.assertNotIn(name, environment)
        process.terminate.assert_called_once_with()
        process.wait.assert_called_once_with(timeout=5)

    def test_recovery_defaults_to_default_project_without_starting_codex_or_rpc(self):
        with mock.patch.object(bridge_client.subprocess, 'Popen') as popen:
            result = bridge_client.main(['--server', 'https://chat.example', '--recover', '--confirm-stopped'])
        self.assertEqual(result, 0)
        self.assertFalse(popen.called)
        self.assertEqual(_Rpc.instances, [])
        self.assertEqual(_State.instances[0].client.project, 'default')
        self.assertIn(('reset', {'confirm_stopped': True}), _State.instances[0].events)

    def test_missing_api_token_never_starts_codex(self):
        class NoTokenHttp(_Http):
            def __init__(self, server, token, project='default'):
                super().__init__(server, token, project)
                self.token = None

        with mock.patch.object(bridge_client, 'HttpClient', NoTokenHttp), \
                mock.patch.object(bridge_client.subprocess, 'Popen') as popen:
            self.assertEqual(bridge_client.main(['--server', 'https://chat.example', '--once']), 2)
        self.assertFalse(popen.called)

    def test_missing_codex_binary_fails_cleanly(self):
        self.assertEqual(bridge_client.main([
            '--server', 'https://chat.example', '--project', 'default', '--once',
            '--codex-bin', 'definitely-not-an-installed-codex-binary',
        ]), 2)

    def test_owned_fake_executable_is_terminated_after_one_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            executable = Path(directory) / 'fake-codex'
            executable.write_text('#!/bin/sh\ntrap "exit 0" TERM\nwhile :; do sleep 1; done\n')
            executable.chmod(0o700)
            self.assertEqual(bridge_client.main([
                '--server', 'https://chat.example', '--project', 'default', '--once',
                '--codex-bin', str(executable),
            ]), 0)

    def test_connect_only_never_starts_or_stops_an_existing_server(self):
        with mock.patch.object(bridge_client.subprocess, 'Popen') as popen, \
                mock.patch.object(bridge_client, '_stop_codex') as stop_codex:
            self.assertEqual(bridge_client.main(['--server', 'https://chat.example', '--connect-only', '--once']), 0)
        self.assertFalse(popen.called)
        stop_codex.assert_called_once_with(None)

    def test_child_exit_after_dispatch_makes_once_fail(self):
        process = mock.Mock()
        process.poll.side_effect = [None, None, 7, 7]
        process.returncode = 7
        with mock.patch.object(bridge_client.subprocess, 'Popen', return_value=process):
            self.assertEqual(bridge_client.main(['--server', 'https://chat.example', '--project', 'default', '--once']), 2)
        self.assertEqual(_Bridge.ticks, ['default'])

    def test_discovery_adds_projects_on_the_next_poll(self):
        class ChangingHttp(_Http):
            project_lists = [[{'id': 'default'}], [{'id': 'default'}, {'id': 'later'}]]

            def call(self, path, payload):
                if path == '/api/projects/rpc':
                    return {'projects': self.project_lists.pop(0)}
                return super().call(path, payload)

        class TwoPassEvent:
            def __init__(self): self.waits = 0
            def is_set(self): return self.waits >= 2
            def wait(self, _): self.waits += 1
            def set(self): self.waits = 2

        with mock.patch.object(bridge_client, 'HttpClient', ChangingHttp), \
                mock.patch.object(bridge_client.threading, 'Event', TwoPassEvent):
            self.assertEqual(bridge_client.main(['--server', 'https://chat.example', '--connect-only']), 0)
        self.assertEqual(_Bridge.ticks, ['default', 'default', 'later'])
        default = next(state for state in _State.instances if state.client.project == 'default')
        self.assertEqual([op for op, _ in default.events].count('acquire'), 2)

    def test_startup_refusal_retries_and_announces_ready_once(self):
        class StartingRpc(_Rpc):
            def connect(self):
                super().connect()
                if self.connected == 1:
                    raise bridge_client.TransportError('Connection refused')
                return self

        class ThreePassEvent:
            def __init__(self): self.waits = 0
            def is_set(self): return self.waits >= 3
            def wait(self, _): self.waits += 1
            def set(self): self.waits = 3

        output, errors = io.StringIO(), io.StringIO()
        with mock.patch.object(bridge_client, 'RpcClient', StartingRpc), \
                mock.patch.object(bridge_client.threading, 'Event', ThreePassEvent), \
                contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors):
            result = bridge_client.main(['--server', 'https://chat.example', '--project', 'default', '--connect-only'])
        self.assertEqual(result, 0)
        events = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual([event['bridge'] for event in events], ['starting', 'ready'])
        self.assertEqual(events[1]['project'], 'default')
        self.assertEqual(_Rpc.instances[0].connected, 3)
        self.assertEqual(_Bridge.ticks, ['default', 'default'])
        self.assertEqual([json.loads(line)['bridge'] for line in errors.getvalue().splitlines()], ['waiting'])


if __name__ == '__main__':
    unittest.main()
