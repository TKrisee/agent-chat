"""Install and control per-user background agent-chat services."""
from __future__ import annotations

import argparse
import html
import os
from pathlib import Path
import platform
import plistlib
import shutil
import subprocess
import sys
import shlex


LABEL = "com.agent-chat"
COMPONENTS = ("server", "client")
LSREGISTER = "/System/Library/Frameworks/CoreServices.framework/Frameworks/LaunchServices.framework/Support/lsregister"


class ServiceError(RuntimeError):
    pass


def _absolute_executable(name: str) -> str:
    found = shutil.which(name)
    if found:
        return str(Path(found).resolve())
    checkout = Path(__file__).resolve().parents[2] / "bin" / name
    if checkout.exists():
        return str(checkout.resolve())
    raise ServiceError("cannot find " + name + "; install agent-chat or use its checkout")


def _quote(value: str | Path) -> str:
    return shlex.quote(str(value))


def _absolute(value: str | Path) -> str:
    """Make a path absolute without resolving executable symlinks."""
    return os.path.abspath(os.path.expanduser(str(value)))


def _systemd_value(value: str | Path, *, exec_start: bool = False) -> str:
    """Quote one systemd directive value without relying on shell quoting."""
    text = str(value)
    if any(ord(char) < 32 for char in text):
        raise ServiceError("service paths cannot contain control characters")
    text = text.replace("\\", "\\\\").replace('"', '\\"').replace("%", "%%")
    if exec_start: text = text.replace("$", "$$")
    return '"' + text + '"'


def _xml(value: str | Path) -> str:
    return html.escape(str(value), quote=True)


class ServiceManager:
    def __init__(self, *, system: str | None = None, home: Path | None = None,
                 python: str | None = None, server_bin: str | None = None,
                 client_bin: str | None = None, run=subprocess.run, environ: dict[str, str] | None = None):
        self.system = system or platform.system()
        self.home = Path(home or Path.home())
        self.python = _absolute(python or sys.executable)
        self.server_bin = _absolute(server_bin or _absolute_executable("agent-chat-server"))
        self.client_bin = _absolute(client_bin) if client_bin else None
        self.run = run
        self.environ = dict(os.environ if environ is None else environ)

    def _platform(self) -> str:
        if self.system == "Darwin": return "launchd"
        if self.system == "Linux": return "systemd"
        raise ServiceError("unsupported platform: " + self.system + "; supported platforms are Darwin and Linux")

    def paths(self, component: str) -> dict[str, Path]:
        if component not in COMPONENTS: raise ServiceError("component must be server, client, or all")
        if self._platform() == "launchd":
            services = self.home / ".local" / "share" / "agent-chat" / "services"
            config = self.home / "Library" / "LaunchAgents" / (LABEL + "." + component + ".plist")
            logs = self.home / "Library" / "Logs" / "agent-chat"
            name = self._display_name(component)
            bundle = services / (name + ".app")
            wrapper = bundle / "Contents" / "MacOS" / name
        else:
            data = Path(self.environ.get("XDG_DATA_HOME", str(self.home / ".local" / "share")))
            config_home = Path(self.environ.get("XDG_CONFIG_HOME", str(self.home / ".config")))
            state = Path(self.environ.get("XDG_STATE_HOME", str(self.home / ".local" / "state")))
            services = data / "agent-chat" / "services"
            config = config_home / "systemd" / "user" / ("agent-chat-" + component + ".service")
            logs = state / "agent-chat" / "logs"
            bundle = None
            wrapper = services / ("agent-chat-" + component)
        return {"wrapper": wrapper, "config": config, "logs": logs, "bundle": bundle}

    @staticmethod
    def _display_name(component: str) -> str:
        return "Agent Chat Server" if component == "server" else "Agent Chat Client"

    def _components(self, value: str) -> tuple[str, ...]:
        if value == "all": return COMPONENTS
        if value in COMPONENTS: return (value,)
        raise ServiceError("component must be server, client, or all")

    def _wrapper(self, component: str, args: argparse.Namespace) -> str:
        token = Path(_absolute(args.token_file or (str(args.db) + ".api-token")))
        lines = ["#!/bin/sh", "set -eu", "cd -- " + _quote(args.project_root),
                 "unset AGENT_CHAT_PROJECT AGENT_CHAT_SESSION AGENT_CHAT_TOKEN AGENT_CHAT_DB AGENT_CHAT_API_TOKEN"]
        if component == "server":
            if args.token_file:
                lines.extend(["AGENT_CHAT_API_TOKEN=$(cat -- " + _quote(token) + ")", "export AGENT_CHAT_API_TOKEN"])
            lines.append("exec " + " ".join(_quote(x) for x in (self.python, self.server_bin, "--db", args.db)))
        else:
            lines.extend([
                "AGENT_CHAT_API_TOKEN=$(cat -- " + _quote(token) + ")",
                "export AGENT_CHAT_API_TOKEN",
                "export AGENT_CHAT_SERVER=" + _quote(args.server),
                "export AGENT_CHAT_ROOT=" + _quote(args.project_root),
                "export AGENT_CHAT_STATE_DIR=" + _quote(args.state_dir),
                "export AGENT_CHAT_CODEX_SERVER=" + _quote(args.codex_server),
            ])
            command = [self.python, self.client_bin or _absolute_executable("agent-chat-client"), "bridge", "--codex-bin", args.codex_bin,
                       "--codex-server", args.codex_server]
            if args.project:
                lines.append("export AGENT_CHAT_PROJECT=" + _quote(args.project))
                command.extend(["--project", args.project])
            lines.append("exec " + " ".join(_quote(x) for x in command))
        return "\n".join(lines) + "\n"

    def _environment(self, args: argparse.Namespace) -> dict[str, str]:
        parent = str(Path(self.python).parent)
        codex_parent = str(Path(args.codex_bin).parent)
        inherited = self.environ.get("PATH", "").split(os.pathsep)
        return {"PATH": os.pathsep.join(item for item in dict.fromkeys((codex_parent, parent, *inherited, "/opt/homebrew/bin", "/usr/local/bin", "/usr/bin", "/bin")) if item),
                "HOME": str(self.home)}

    def _launchd(self, component: str, args: argparse.Namespace) -> str:
        paths = self.paths(component); label = LABEL + "." + component
        values = [("Label", label), ("ProgramArguments", [str(paths["wrapper"])]),
                  ("AssociatedBundleIdentifiers", [label]),
                  ("WorkingDirectory", args.project_root), ("RunAtLoad", True),
                  ("KeepAlive", True), ("ThrottleInterval", 10),
                  ("StandardOutPath", str(paths["logs"] / (component + ".out.log"))),
                  ("StandardErrorPath", str(paths["logs"] / (component + ".err.log")))]
        env = self._environment(args)
        body = []
        for key, value in values:
            body.append("<key>" + key + "</key>")
            if isinstance(value, list): body.append("<array>" + "".join("<string>" + _xml(x) + "</string>" for x in value) + "</array>")
            elif isinstance(value, bool): body.append("<true/>")
            elif isinstance(value, int): body.append("<integer>" + str(value) + "</integer>")
            else: body.append("<string>" + _xml(value) + "</string>")
        body.append("<key>EnvironmentVariables</key><dict>" + "".join("<key>" + _xml(k) + "</key><string>" + _xml(v) + "</string>" for k, v in env.items()) + "</dict>")
        return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\"><dict>" + "".join(body) + "</dict></plist>\n"

    def _systemd(self, component: str, args: argparse.Namespace) -> str:
        paths = self.paths(component); env = self._environment(args)
        description = self._display_name(component) + (" (Codex wake bridge)" if component == "client" else "")
        lines = ["[Unit]", "Description=" + description, "", "[Service]", "Type=simple",
                 "WorkingDirectory=" + _systemd_value(args.project_root), "ExecStart=" + _systemd_value(paths["wrapper"], exec_start=True),
                 "Restart=on-failure", "RestartSec=10"]
        lines.extend("Environment=" + _systemd_value(k + "=" + v) for k, v in env.items())
        lines.extend(["", "[Install]", "WantedBy=default.target", ""])
        return "\n".join(lines)

    def render(self, component: str, args: argparse.Namespace) -> str:
        return self._launchd(component, args) if self._platform() == "launchd" else self._systemd(component, args)

    def _call(self, command: list[str]) -> None:
        self.run(command, check=True)

    def _bundle_info(self, component: str) -> bytes:
        name = self._display_name(component)
        return plistlib.dumps({"CFBundleIdentifier": LABEL + "." + component,
                               "CFBundleName": name, "CFBundleDisplayName": name,
                               "CFBundleExecutable": name, "CFBundlePackageType": "APPL",
                               "LSBackgroundOnly": True}, fmt=plistlib.FMT_XML, sort_keys=True)

    @staticmethod
    def _unlink_empty(path: Path) -> None:
        try: path.rmdir()
        except (FileNotFoundError, OSError): pass

    def install(self, args: argparse.Namespace) -> None:
        for component in self._components(args.component):
            paths = self.paths(component)
            paths["wrapper"].parent.mkdir(parents=True, exist_ok=True)
            paths["config"].parent.mkdir(parents=True, exist_ok=True)
            paths["logs"].mkdir(parents=True, exist_ok=True)
            if component == "client" and self.client_bin is None:
                self.client_bin = _absolute(_absolute_executable("agent-chat-client"))
            paths["wrapper"].write_text(self._wrapper(component, args), encoding="utf-8")
            os.chmod(paths["wrapper"], 0o700)
            if paths["bundle"] is not None:
                info = paths["bundle"] / "Contents" / "Info.plist"
                info.parent.mkdir(parents=True, exist_ok=True)
                info.write_bytes(self._bundle_info(component))
                os.chmod(info, 0o600)
            paths["config"].write_text(self.render(component, args), encoding="utf-8")
            os.chmod(paths["config"], 0o600)
            if paths["bundle"] is not None:
                self._call([LSREGISTER, "-f", str(paths["bundle"])])
        if self._platform() == "systemd":
            self._call(["systemctl", "--user", "daemon-reload"])
            for component in self._components(args.component):
                self._call(["systemctl", "--user", "enable", "agent-chat-" + component + ".service"])
        if not args.no_start: self.start(args.component)

    def start(self, component: str) -> None:
        for item in self._components(component):
            path = self.paths(item)["config"]
            if self._platform() == "launchd": self._call(["launchctl", "bootstrap", "gui/" + str(os.getuid()), str(path)])
            else:
                self._call(["systemctl", "--user", "enable", "--now", "agent-chat-" + item + ".service"])

    def stop(self, component: str) -> None:
        for item in self._components(component):
            if self._platform() == "launchd": self._call(["launchctl", "bootout", "gui/" + str(os.getuid()), str(self.paths(item)["config"])])
            else: self._call(["systemctl", "--user", "stop", "agent-chat-" + item + ".service"])

    def restart(self, component: str) -> None:
        for item in self._components(component):
            if self._platform() == "launchd": self._call(["launchctl", "kickstart", "-k", "gui/" + str(os.getuid()) + "/" + LABEL + "." + item])
            else: self._call(["systemctl", "--user", "restart", "agent-chat-" + item + ".service"])

    def status(self, component: str) -> None:
        for item in self._components(component):
            if self._platform() == "launchd": self._call(["launchctl", "print", "gui/" + str(os.getuid()) + "/" + LABEL + "." + item])
            else: self._call(["systemctl", "--user", "--no-pager", "status", "agent-chat-" + item + ".service"])

    def uninstall(self, component: str) -> None:
        for item in self._components(component):
            paths = self.paths(item)
            if self._platform() == "launchd":
                target = "gui/" + str(os.getuid()) + "/" + LABEL + "." + item
                loaded = self.run(["launchctl", "print", target], check=False, capture_output=True, text=True)
                if loaded is not None and loaded.returncode != 0:
                    if "Could not find service" not in loaded.stderr:
                        raise ServiceError("cannot inspect service before removal: " + loaded.stderr.strip())
                    result = None
                else:
                    result = self.run(["launchctl", "bootout", "gui/" + str(os.getuid()), str(paths["config"])], check=False, capture_output=True, text=True)
            else:
                result = self.run(["systemctl", "--user", "disable", "--now", "agent-chat-" + item + ".service"], check=False, capture_output=True, text=True)
            if result is not None and getattr(result, "returncode", 0) != 0:
                output = str(getattr(result, "stderr", ""))
                if "No such process" not in output and "not loaded" not in output and "not found" not in output:
                    raise ServiceError("refusing to remove active service definition: " + output.strip())
            if paths["bundle"] is not None:
                bundle = paths["bundle"]
                result = self.run([LSREGISTER, "-u", str(bundle)], check=False, capture_output=True, text=True)
                if result is not None and getattr(result, "returncode", 0) != 0:
                    raise ServiceError("refusing to remove application bundle: " + str(getattr(result, "stderr", "")).strip())
            for key in ("config", "wrapper"):
                try: paths[key].unlink()
                except FileNotFoundError: pass
            if paths["bundle"] is not None:
                try: (bundle / "Contents" / "Info.plist").unlink()
                except FileNotFoundError: pass
                for directory in (paths["wrapper"].parent, bundle / "Contents"):
                    self._unlink_empty(directory)
                self._unlink_empty(bundle)
        if self._platform() == "systemd": self._call(["systemctl", "--user", "daemon-reload"])


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="agent-chat-service")
    p.add_argument("action", choices=("install", "start", "stop", "restart", "status", "uninstall"))
    p.add_argument("--component", choices=("server", "client", "all"), default="all")
    p.add_argument("--no-start", action="store_true")
    home = Path.home()
    p.add_argument("--db", default=str(home / ".local" / "share" / "agent-chat" / "state.sqlite3"))
    p.add_argument("--server", default="http://127.0.0.1:8765")
    p.add_argument("--project-root", default=os.getcwd())
    p.add_argument("--state-dir", default=str(home / ".local" / "state" / "agent-chat"))
    p.add_argument("--codex-bin", default=shutil.which("codex") or "codex")
    p.add_argument("--codex-server", default="ws://127.0.0.1:4500")
    p.add_argument("--token-file")
    p.add_argument("--project")
    return p


def _main(argv=None) -> int:
    args = parser().parse_args(argv)
    for name in ("db", "project_root", "state_dir", "token_file"):
        if getattr(args, name, None): setattr(args, name, _absolute(getattr(args, name)))
    if args.action == "install" and args.component in ("client", "all"):
        codex = shutil.which(args.codex_bin) if not Path(args.codex_bin).is_absolute() else args.codex_bin
        if not codex: raise ServiceError("cannot find codex; pass --codex-bin with an absolute path")
        args.codex_bin = _absolute(codex)
    manager = ServiceManager()
    getattr(manager, args.action)(args if args.action == "install" else args.component)
    return 0


def main(argv=None) -> int:
    try:
        return _main(argv)
    except (ServiceError, OSError, subprocess.CalledProcessError) as error:
        print(str(error), file=sys.stderr)
        return 2
