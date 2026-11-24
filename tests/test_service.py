import argparse
import os
import plistlib
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from agent_chat.service import ServiceError, ServiceManager


def private_provider(test):
    """Provide only a test-owned provider; never consult installed defaults."""
    from agent_chat import service
    scratch=tempfile.TemporaryDirectory(prefix='service-provider-')
    test.addCleanup(scratch.cleanup)
    base=Path(scratch.name)
    binary=base/'agent-chat-server'
    binary.write_text('#!/bin/sh\nexit 99\n')
    binary.chmod(0o700)
    environment=mock.patch.dict(os.environ, {'PATH':str(base)+os.pathsep+os.environ.get('PATH','')})
    environment.start();test.addCleanup(environment.stop)
    config=service.InstalledServerConfig(base/'server.plist',base/'wrapper',base/'server-bin',
        base/'state.sqlite3',base,os.getuid(),'127.0.0.1',8765)
    provider=mock.patch.object(service,'INSTALLED_SERVER_CONFIG',config)
    provider.start();test.addCleanup(provider.stop)


class ServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = Path(self.temp.name) / "home with $ % & apostrophe' space"
        self.calls = []
        self.args = argparse.Namespace(component="all", no_start=True,
            db=str(self.home / "data $ %.sqlite3"), server="http://127.0.0.1:8765",
            project_root=str(self.home / "project $ %"), state_dir=str(self.home / "state $ %"),
            codex_bin=str(self.home / "bin $ %" / "codex"), codex_server="ws://127.0.0.1:4500",
            token_file=None, project=None, inbox_diagnostics=None, log_level="OFF", log_file=None)

    def tearDown(self): self.temp.cleanup()

    def manager(self, system="Darwin"):
        return ServiceManager(system=system, home=self.home, python="/opt/python 3",
            server_bin="/opt/agent chat/server", client_bin="/opt/agent chat/client",
            run=lambda command, check=True, **kwargs: self.calls.append((command, check)))

    def test_launchd_render_keeps_secrets_out_and_escapes_paths(self):
        text = self.manager().render("client", self.args)
        self.assertIn("com.agent-chat.client", text)
        self.assertIn("&amp;", text)
        self.assertNotIn("AGENT_CHAT_API_TOKEN", text)
        wrapper = self.manager()._wrapper("client", self.args)
        self.assertIn("cat --", wrapper)
        self.assertNotIn("http://127.0.0.1:8765", text)

    def test_install_connect_only_persists_existing_server_without_codex_binary(self):
        from agent_chat import service
        import shlex
        manager = self.manager()
        argv = ['install', '--component', 'client', '--connect-only', '--no-start',
                '--db', self.args.db, '--server', self.args.server,
                '--project-root', self.args.project_root, '--state-dir', self.args.state_dir,
                '--codex-server', 'ws://127.0.0.1:4500', '--codex-bin', 'missing-fixture-codex']
        with mock.patch('agent_chat.service.ServiceManager', return_value=manager), \
                mock.patch('agent_chat.service.shutil.which', return_value=None):
            self.assertEqual(service.main(argv), 0)
        wrapper = manager.paths('client')['wrapper']
        command = shlex.split(next(line[5:] for line in wrapper.read_text().splitlines() if line.startswith('exec ')))
        self.assertEqual(command.count('--connect-only'), 1)
        self.assertEqual(command[command.index('--codex-server')+1], 'ws://127.0.0.1:4500')
        self.assertEqual(wrapper.stat().st_mode & 0o777, 0o700)
        self.assertFalse(any(call[0][0] == 'launchctl' for call in self.calls))

    def test_connect_only_does_not_change_default_or_server_wrapper(self):
        manager = self.manager()
        self.assertNotIn('--connect-only', manager._wrapper('client', self.args))
        server = manager._wrapper('server', self.args)
        self.args.connect_only = True
        self.assertEqual(manager._wrapper('server', self.args), server)

    def test_systemd_render_preserves_all_project_discovery(self):
        text = self.manager("Linux").render("client", self.args)
        self.assertIn("WantedBy=default.target", text)
        self.assertIn("Restart=on-failure", text)
        self.assertNotIn("AGENT_CHAT_PROJECT", text)
        self.assertNotIn("--project", self.manager("Linux")._wrapper("client", self.args))
        self.assertIn("$$", text)
        self.assertIn("%%", text)
        self.assertIn("apostrophe' space", text)

    def test_systemd_working_directory_is_a_literal_absolute_path(self):
        self.args.project_root = '/tmp/project with "quotes" \\backslash $ and %'
        text = self.manager("Linux").render("server", self.args)
        self.assertIn('WorkingDirectory=/tmp/project with "quotes" \\backslash $ and %%/\n', text)

    def test_systemd_directory_basename_can_end_in_parser_metacharacters(self):
        for path, expected in (('/tmp/final\\', 'WorkingDirectory=/tmp/final\\/\n'),
                               ('/tmp/final ', 'WorkingDirectory=/tmp/final /\n'),
                               ('/tmp/final/', 'WorkingDirectory=/tmp/final/\n')):
            with self.subTest(path=path):
                self.args.project_root = path
                self.assertIn(expected, self.manager("Linux").render("server", self.args))

    def test_linux_paths_honor_xdg_locations(self):
        manager = ServiceManager(system="Linux", home=self.home, python="/opt/python",
            server_bin="/opt/server", client_bin="/opt/client", run=lambda *_args, **_kwargs: None,
            environ={"XDG_DATA_HOME": str(self.home / "data"), "XDG_CONFIG_HOME": str(self.home / "config"),
                     "XDG_STATE_HOME": str(self.home / "state")})
        paths = manager.paths("client")
        self.assertEqual(paths["wrapper"], self.home / "data" / "agent-chat" / "services" / "agent-chat-client")
        self.assertEqual(paths["config"], self.home / "config" / "systemd" / "user" / "agent-chat-client.service")
        self.assertEqual(paths["logs"], self.home / "state" / "agent-chat" / "logs")

    def test_install_no_start_writes_private_files_without_lifecycle_call(self):
        manager = self.manager(); manager.install(self.args)
        for component in ("server", "client"):
            paths = manager.paths(component)
            self.assertTrue(paths["config"].exists())
            self.assertTrue(paths["wrapper"].exists())
            self.assertEqual(paths["wrapper"].stat().st_mode & 0o777, 0o700)
        self.assertEqual([call[0][0] for call in self.calls], ["/System/Library/Frameworks/CoreServices.framework/Frameworks/LaunchServices.framework/Support/lsregister"] * 2)

    def test_macos_bundles_have_descriptive_background_metadata(self):
        manager = self.manager(); manager.install(self.args)
        for component, name in (("server", "Agent Chat Server"), ("client", "Agent Chat Client")):
            paths = manager.paths(component)
            bundle = paths["bundle"]
            self.assertEqual(paths["wrapper"], bundle / "Contents" / "MacOS" / name)
            info = plistlib.loads((bundle / "Contents" / "Info.plist").read_bytes())
            self.assertEqual(info["CFBundleIdentifier"], "com.agent-chat." + component)
            self.assertEqual(info["CFBundleName"], name)
            self.assertEqual(info["CFBundleDisplayName"], name)
            self.assertEqual(info["CFBundleExecutable"], name)
            self.assertEqual(info["CFBundlePackageType"], "APPL")
            self.assertTrue(info["LSBackgroundOnly"])
            self.assertIn("AssociatedBundleIdentifiers", manager.render(component, self.args))
            self.assertIn("com.agent-chat." + component, manager.render(component, self.args))

    def test_systemd_units_use_descriptive_names(self):
        manager = self.manager("Linux")
        self.assertIn("Description=Agent Chat Server", manager.render("server", self.args))
        self.assertIn("Description=Agent Chat Client (Codex wake bridge)", manager.render("client", self.args))

    def test_bundle_registration_is_darwin_only(self):
        manager = self.manager("Linux"); manager.install(self.args)
        self.assertFalse(any(call[0][0] == "/System/Library/Frameworks/CoreServices.framework/Frameworks/LaunchServices.framework/Support/lsregister" for call in self.calls))

    def test_lifecycle_commands_are_native_argument_lists(self):
        manager = self.manager(); manager.start("server"); manager.restart("client"); manager.stop("server"); manager.status("client")
        self.assertEqual([item[0][0] for item in self.calls], ["launchctl"] * 4)
        self.assertTrue(all(isinstance(value, list) for value, _ in self.calls))

    def test_uninstall_preserves_data_token_identity_and_logs(self):
        manager = self.manager(); manager.install(self.args)
        token = Path(self.args.db + ".api-token"); token.parent.mkdir(parents=True, exist_ok=True); token.write_text("private")
        identity = Path(self.args.state_dir) / "client-host.json"; identity.parent.mkdir(parents=True); identity.write_text("identity")
        manager.uninstall("all")
        self.assertTrue(token.exists()); self.assertTrue(identity.exists())
        self.assertTrue(manager.paths("client")["logs"].exists())
        self.assertFalse(manager.paths("client")["bundle"].exists())
        self.assertIn("-u", self.calls[-1][0])

    def test_wrapper_executes_without_secret_argv_or_inherited_project(self):
        Path(self.args.project_root).mkdir(parents=True)
        output = Path(self.temp.name) / "captured"
        client = Path(self.temp.name) / "fixture-client"
        client.write_text("#!/bin/sh\nprintf 'cwd=%s\\nproject=%s\\nsession=%s\\ndb=%s\\ntoken=%s\\nargs=%s\\n' \"$PWD\" \"${AGENT_CHAT_PROJECT-unset}\" \"${AGENT_CHAT_SESSION-unset}\" \"${AGENT_CHAT_DB-unset}\" \"$AGENT_CHAT_API_TOKEN\" \"$*\" > \"$CAPTURE\"\n")
        client.chmod(0o700)
        token = Path(self.temp.name) / "token"; token.write_text("secret-value\n")
        self.args.token_file = str(token)
        manager = ServiceManager(system="Darwin", home=self.home, python="/usr/bin/env",
            server_bin="/bin/true", client_bin=str(client), run=lambda *_args, **_kwargs: None)
        wrapper = manager.paths("client")["wrapper"]; wrapper.parent.mkdir(parents=True); wrapper.write_text(manager._wrapper("client", self.args)); wrapper.chmod(0o700)
        env = dict(os.environ, CAPTURE=str(output), AGENT_CHAT_PROJECT="wrong", AGENT_CHAT_SESSION="wrong", AGENT_CHAT_DB="wrong")
        subprocess.run([str(wrapper)], env=env, check=True)
        captured = output.read_text()
        self.assertIn("cwd=" + self.args.project_root, captured)
        self.assertIn("project=unset", captured); self.assertIn("session=unset", captured); self.assertIn("db=unset", captured)
        self.assertIn("token=secret-value", captured); self.assertIn("cwd=", captured)
        self.assertNotIn("secret-value", wrapper.read_text())
        self.assertIn("bridge --codex-bin", captured)

    def test_symlinked_codex_path_is_retained_in_wrapper_and_path(self):
        node = Path(self.temp.name) / "node bin"; node.mkdir()
        target = Path(self.temp.name) / "package" / "codex"; target.parent.mkdir(); target.write_text("fixture")
        link = node / "codex"; link.symlink_to(target)
        self.args.codex_bin = str(link)
        manager = self.manager()
        self.assertIn(str(node), manager._environment(self.args)["PATH"].split(os.pathsep))
        self.assertIn(str(link), manager._wrapper("client", self.args))

    def test_server_only_install_never_needs_client_or_codex(self):
        self.args.component = "server"
        self.args.codex_bin = "/missing/codex"
        manager = ServiceManager(system="Darwin", home=self.home, python="/usr/bin/python3",
            server_bin="/bin/true", run=lambda command, check=True: self.calls.append((command, check)))
        manager.install(self.args)
        self.assertTrue(manager.paths("server")["wrapper"].exists())

    def test_server_uses_sidecar_by_default_and_explicit_token_file_when_given(self):
        manager = self.manager()
        self.assertIn("unset AGENT_CHAT_PROJECT AGENT_CHAT_SESSION AGENT_CHAT_TOKEN AGENT_CHAT_DB AGENT_CHAT_API_TOKEN", manager._wrapper("server", self.args))
        self.assertNotIn("cat --", manager._wrapper("server", self.args))
        self.args.token_file = str(self.home / "custom-token")
        self.assertIn("cat --", manager._wrapper("server", self.args))

    def test_server_wrapper_passes_only_explicit_owner_diagnostics_path(self):
        manager = self.manager()
        self.assertNotIn('--inbox-diagnostics', manager._wrapper('server', self.args))
        self.args.inbox_diagnostics = str(self.home / 'private diagnostics' / 'inbox.jsonl')
        wrapper = manager._wrapper('server', self.args)
        self.assertIn('--inbox-diagnostics', wrapper)
        self.assertIn('private diagnostics/inbox.jsonl', wrapper)

    def test_server_wrapper_passes_explicit_structured_logging_only(self):
        manager = self.manager()
        self.args.log_level = 'INFO'
        self.args.log_file = str(self.home / 'private logs' / 'server.jsonl')
        wrapper = manager._wrapper('server', self.args)
        self.assertIn('--log-level INFO', wrapper)
        self.assertIn('private logs/server.jsonl', wrapper)
        self.args.log_level = 'OFF'
        self.assertNotIn('--log-level', manager._wrapper('server', self.args))

    def test_unsupported_platform_is_clear(self):
        with self.assertRaisesRegex(ServiceError, "unsupported platform"):
            self.manager("Windows").paths("server")

    def test_client_launcher_is_discovered_without_an_explicit_override(self):
        manager = ServiceManager(system="Darwin", home=self.home, server_bin="/bin/true")
        with mock.patch("agent_chat.service._absolute_executable", return_value="/opt/agent-chat-client"):
            wrapper = manager._wrapper("client", self.args)
        self.assertIn("/opt/agent-chat-client bridge", wrapper)
        self.assertNotIn("None", wrapper)

    def test_uninstall_retains_files_when_stop_fails(self):
        manager = self.manager(); manager.install(self.args)
        def failed_stop(command, **kwargs):
            return subprocess.CompletedProcess(command, 0 if "print" in command else 1, "", "permission denied")
        manager.run = failed_stop
        with self.assertRaisesRegex(ServiceError, "refusing to remove"):
            manager.uninstall("server")
        self.assertTrue(manager.paths("server")["config"].exists())
        self.assertTrue(manager.paths("server")["wrapper"].exists())


class ServerDrainGuardTests(unittest.TestCase):
    def setUp(self):
        private_provider(self)

    def state(self, *, active=0, sealed=False):
        return dict(instance='a'*32, pid=123, epoch='b'*32, active=active,
                    sealed=sealed, state='sealed' if sealed else 'draining')

    def manager(self):
        from agent_chat.service import _installed_server_config
        manager = ServiceManager(system='Darwin', run=mock.Mock(return_value=subprocess.CompletedProcess([],0,stdout='    pid = 123\n')))
        manager._drain_binding = mock.Mock(return_value=(_installed_server_config(), ('pin',), 'synthetic-private-test-token'))
        return manager

    def test_zero_seal_then_exact_server_only_restart(self):
        manager = self.manager()
        manager._maintenance_call = mock.Mock(side_effect=[self.state(),self.state(),self.state(sealed=True)])
        result = manager.drain('server', restart=True)
        self.assertTrue(result['restart_requested'])
        commands = [call.args[0] for call in manager.run.call_args_list]
        self.assertEqual(commands, [['launchctl','print','gui/501/com.agent-chat.server'],
                                    ['launchctl','kickstart','-k','gui/501/com.agent-chat.server']])

    def test_timeout_and_provider_pid_mismatch_never_restart(self):
        manager = self.manager()
        manager._maintenance_call = mock.Mock(return_value=self.state(active=1))
        with self.assertRaisesRegex(ServiceError, 'timed out'): manager.drain('server', timeout=.001, restart=True)
        manager.run.assert_not_called()
        manager = self.manager()
        manager._maintenance_call = mock.Mock(side_effect=[self.state(),self.state(),self.state(sealed=True)])
        manager._drain_binding.side_effect = [manager._drain_binding.return_value,
            (manager._drain_binding.return_value[0], ('changed',), 'same')]
        with self.assertRaisesRegex(ServiceError, 'provider changed'): manager.drain('server', restart=True)
        manager.run.assert_not_called()
        manager = self.manager()
        manager.run.return_value.stdout = '    pid = 999\n'
        manager._maintenance_call = mock.Mock(side_effect=[self.state(),self.state(),self.state(sealed=True)])
        with self.assertRaisesRegex(ServiceError, 'PID differs'): manager.drain('server', restart=True)
        self.assertEqual(manager.run.call_count, 1)

    def test_scope_instance_change_unavailable_and_invalid_timeout_refuse(self):
        manager = self.manager()
        for component in ['client','all']:
            with self.assertRaises(ServiceError): manager.drain(component, restart=True)
        for value in [0,61,float('nan'),float('inf')]:
            with self.assertRaises(ServiceError): manager.drain('server',timeout=value,restart=True)
        manager.run.assert_not_called()
        manager._maintenance_call = mock.Mock(side_effect=ServiceError('unavailable'))
        with self.assertRaises(ServiceError): manager.drain('server',restart=True)
        manager.run.assert_not_called()
        changed = dict(self.state(),instance='c'*32)
        manager._maintenance_call = mock.Mock(side_effect=[self.state(),changed])
        with self.assertRaisesRegex(ServiceError,'instance changed'): manager.drain('server',restart=True)
        manager.run.assert_not_called()

    def test_fixed_binding_rejects_changed_provider_before_http(self):
        manager = ServiceManager(system='Darwin')
        with mock.patch('agent_chat.service.os.geteuid', return_value=501), mock.patch('agent_chat.service.configured_server_database', side_effect=ServiceError('wrong provider')):
            with self.assertRaisesRegex(ServiceError,'wrong provider'): manager._drain_binding()


    def test_token_rotation_during_drain_refuses_restart(self):
        manager = self.manager()
        config, pins, token = manager._drain_binding.return_value
        manager._drain_binding.side_effect = [(config,pins,token),(config,pins,'rotated')]
        manager._maintenance_call = mock.Mock(side_effect=[self.state(),self.state(),self.state(sealed=True)])
        with self.assertRaisesRegex(ServiceError,'provider changed'): manager.drain('server',restart=True)
        manager.run.assert_not_called()

    @unittest.skipUnless(os.geteuid() == 501, 'fixed-provider file ownership fixture requires UID501')
    def test_real_isolated_fixed_provider_http_seal_and_mocked_kickstart(self):
        import dataclasses
        import threading
        from agent_chat.core import Coordinator
        from agent_chat.web import create_server
        from agent_chat.service import _installed_server_config
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            master = root/'state.sqlite3'
            Coordinator(master).close()
            master.chmod(0o600)
            token = 'synthetic-maintenance-private-token-123456'
            tokenfile = Path(str(master)+'.api-token')
            tokenfile.write_text(token); tokenfile.chmod(0o600)
            server = create_server(master, port=0, api_token=token)
            thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
            try:
                config = dataclasses.replace(_installed_server_config(), plist=root/'server.plist',
                    wrapper=root/'wrapper', executable=root/'agent-chat-server', master=master,
                    working_directory=root, port=server.server_port)
                config.executable.write_text('synthetic fixed executable');config.executable.chmod(0o700)
                config.wrapper.write_text('exec '+__import__('sys').executable+' '+str(config.executable)+
                    ' --db '+str(master)+' --host 127.0.0.1 --port '+str(server.server_port)+'\n')
                config.wrapper.chmod(0o700)
                config.plist.write_bytes(plistlib.dumps(dict(Label='com.agent-chat.server',
                    AssociatedBundleIdentifiers=['com.agent-chat.server'], WorkingDirectory=str(root),
                    RunAtLoad=True, KeepAlive=True, ThrottleInterval=10,
                    ProgramArguments=[str(config.wrapper)], EnvironmentVariables={})))
                config.plist.chmod(0o600)
                run = mock.Mock(return_value=subprocess.CompletedProcess([],0,stdout='    pid = '+str(os.getpid())+'\n'))
                manager = ServiceManager(system='Darwin',run=run)
                with mock.patch('agent_chat.service._installed_server_config', return_value=config):
                    result = manager.drain('server',timeout=1,restart=True)
                self.assertEqual((result['active'],result['sealed'],result['restart_requested']), (0,True,True))
                self.assertTrue(server.http_sealed)
                self.assertEqual(run.call_args.args[0], ['launchctl','kickstart','-k','gui/501/com.agent-chat.server'])
            finally:
                server.shutdown();server.server_close();thread.join(3)
                self.assertFalse(thread.is_alive())


class OneShotDrainRestartTests(unittest.TestCase):
    def setUp(self):
        private_provider(self)

    def fixture(self, ambiguous=False):
        import contextlib
        import dataclasses
        import threading
        from agent_chat.core import Coordinator
        from agent_chat.web import create_server
        from agent_chat.service import _installed_server_config
        @contextlib.contextmanager
        def owned():
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory).resolve(); master = root/'state.sqlite3'
                Coordinator(master).close(); master.chmod(0o600)
                token = 'synthetic-one-shot-maintenance-token-123456'
                tokenfile = Path(str(master)+'.api-token'); tokenfile.write_text(token);tokenfile.chmod(0o600)
                server = create_server(master,port=0,api_token=token)
                thread = threading.Thread(target=server.serve_forever,daemon=True);thread.start()
                try:
                    config = dataclasses.replace(_installed_server_config(),plist=root/'server.plist',
                        wrapper=root/'wrapper',executable=root/'agent-chat-server',master=master,
                        working_directory=root,port=server.server_port)
                    config.executable.write_text('synthetic executable');config.executable.chmod(0o700)
                    config.wrapper.write_text('exec '+__import__('sys').executable+' '+str(config.executable)+
                        ' --db '+str(master)+' --host 127.0.0.1 --port '+str(server.server_port)+'\n')
                    config.wrapper.chmod(0o700)
                    config.plist.write_bytes(plistlib.dumps(dict(Label='com.agent-chat.server',
                        AssociatedBundleIdentifiers=['com.agent-chat.server'],WorkingDirectory=str(root),
                        RunAtLoad=True,KeepAlive=True,ThrottleInterval=10,
                        ProgramArguments=[str(config.wrapper)],EnvironmentVariables={})))
                    config.plist.chmod(0o600)
                    kicks = []; lock = threading.Lock()
                    def run(command, **kwargs):
                        if command[:2] == ['launchctl','print']:
                            return subprocess.CompletedProcess(command,0,stdout='    pid = '+str(os.getpid())+'\n')
                        assert command == ['launchctl','kickstart','-k','gui/501/com.agent-chat.server']
                        with lock:kicks.append(command)
                        if ambiguous:raise subprocess.CalledProcessError(1,command)
                        return subprocess.CompletedProcess(command,0)
                    manager = ServiceManager(system='Darwin',run=run)
                    with mock.patch('agent_chat.service._installed_server_config',return_value=config):
                        yield manager,server,kicks
                finally:
                    server.shutdown();server.server_close();thread.join(3)
                    self.assertFalse(thread.is_alive())
        return owned()

    @unittest.skipUnless(os.geteuid()==501,'fixed-provider ownership fixture requires UID501')
    def test_concurrent_and_sequential_attempts_share_only_one_seal(self):
        import threading
        from concurrent.futures import ThreadPoolExecutor
        with self.fixture() as (manager,server,kicks):
            barrier = threading.Barrier(2); local = threading.local()
            original = manager._maintenance_call
            def synchronized(*args,**kwargs):
                result = original(*args,**kwargs)
                if args[2] == 'status' and not getattr(local,'seen',False):
                    local.seen=True;barrier.wait(timeout=3)
                return result
            manager._maintenance_call=synchronized
            def attempt():
                try:manager.drain('server',timeout=2,restart=True);return 'requested'
                except ServiceError:return 'refused'
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures=[pool.submit(attempt),pool.submit(attempt)]
                outcomes=[future.result(timeout=5) for future in futures]
            self.assertEqual(sorted(outcomes),['refused','requested'])
            self.assertEqual(len(kicks),1)
            manager._maintenance_call=original
            with self.assertRaisesRegex(ServiceError,'initially sealed'):
                manager.drain('server',timeout=2,restart=True)
            self.assertEqual(len(kicks),1)
            self.assertTrue(server.http_sealed)

    @unittest.skipUnless(os.geteuid()==501,'fixed-provider ownership fixture requires UID501')
    def test_ambiguous_first_kickstart_cannot_be_blindly_repeated_in_same_epoch(self):
        with self.fixture(ambiguous=True) as (manager,server,kicks):
            with self.assertRaises(subprocess.CalledProcessError):manager.drain('server',restart=True)
            self.assertTrue(server.http_sealed)
            with self.assertRaisesRegex(ServiceError,'initially sealed'):
                manager.drain('server',restart=True)
            self.assertEqual(len(kicks),1)
