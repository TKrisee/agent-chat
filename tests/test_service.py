import argparse
import os
import plistlib
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from agent_chat.service import ServiceError, ServiceManager


class ServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.home = Path(self.temp.name) / "home with $ % & apostrophe' space"
        self.calls = []
        self.args = argparse.Namespace(component="all", no_start=True,
            db=str(self.home / "data $ %.sqlite3"), server="http://127.0.0.1:8765",
            project_root=str(self.home / "project $ %"), state_dir=str(self.home / "state $ %"),
            codex_bin=str(self.home / "bin $ %" / "codex"), codex_server="ws://127.0.0.1:4500",
            token_file=None, project=None)

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

    def test_systemd_render_preserves_all_project_discovery(self):
        text = self.manager("Linux").render("client", self.args)
        self.assertIn("WantedBy=default.target", text)
        self.assertIn("Restart=on-failure", text)
        self.assertNotIn("AGENT_CHAT_PROJECT", text)
        self.assertNotIn("--project", self.manager("Linux")._wrapper("client", self.args))
        self.assertIn("$$", text)
        self.assertIn("%%", text)
        self.assertIn("apostrophe' space", text)

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
