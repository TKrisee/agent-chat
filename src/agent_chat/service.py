"""Install and control per-user background agent-chat services."""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import html
import hashlib
import json
import math
import re
import time
import urllib.request
import os
from pathlib import Path
import platform
import plistlib
import shutil
import stat
import subprocess
import sys
import shlex

from .app_logging import LoggingConfig, configure, new_correlation_id


LABEL = "com.agent-chat"
COMPONENTS = ("server", "client")
LSREGISTER = "/System/Library/Frameworks/CoreServices.framework/Frameworks/LaunchServices.framework/Support/lsregister"


class _NoMaintenanceRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class ServiceError(RuntimeError):
    pass


@dataclass(frozen=True)
class InstalledServerConfig:
    """The configured local server is the only authority for local administration."""
    plist: Path
    wrapper: Path
    executable: Path
    master: Path
    working_directory: Path
    uid: int
    host: str
    port: int


# This is deliberately fixed source contract data, rather than CLI or environment
# routing.  Tests replace the provider; production must validate the installed
# files before a target database is selected.
INSTALLED_SERVER_CONFIG = InstalledServerConfig(
    plist=Path('/Users/kristoftischler/Library/LaunchAgents/com.agent-chat.server.plist'),
    wrapper=Path('/Users/kristoftischler/.local/share/agent-chat/services/Agent Chat Server.app/Contents/MacOS/Agent Chat Server'),
    executable=Path('/Users/kristoftischler/Work/agent-chat/bin/agent-chat-server'),
    master=Path('/Users/kristoftischler/Work/agent-chat/.agent-chat/state.sqlite3'),
    working_directory=Path('/Users/kristoftischler/Unity/into-the-rot'), uid=501,
    host='127.0.0.1', port=8765)


def _installed_server_config() -> InstalledServerConfig:
    return INSTALLED_SERVER_CONFIG


def _absolute_path(value: str | Path) -> Path:
    return Path(os.path.abspath(os.path.expanduser(str(value))))


def _trusted_regular_file(path: Path, *, description: str, uid: int) -> Path:
    try:
        info = path.lstat()
    except OSError as error:
        raise ServiceError(f'configured server {description} is missing or unsafe') from error
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode) or info.st_uid != uid:
        raise ServiceError(f'configured server {description} is missing or unsafe')
    return path


def _command_option(command: list[str], option: str, *, default: str | None = None) -> str:
    positions = [index for index, value in enumerate(command) if value == option]
    if not positions:
        if default is None:
            raise ServiceError(f'configured server wrapper lacks {option}')
        return default
    if len(positions) != 1 or positions[0] + 1 >= len(command):
        raise ServiceError(f'configured server wrapper has an invalid {option}')
    return command[positions[0] + 1]


def configured_server_database(plist_path: str | Path) -> Path:
    """Validate the installed provider without evaluating its wrapper."""
    config = _installed_server_config()
    if _absolute_path(plist_path) != config.plist:
        raise ServiceError('configured server plist is not the installed source')
    plist_file = _trusted_regular_file(config.plist, description='plist', uid=config.uid)
    try:
        with plist_file.open('rb') as stream:
            plist = plistlib.load(stream)
    except (OSError, ValueError, plistlib.InvalidFileException) as error:
        raise ServiceError('configured server plist is unreadable') from error
    expected = {'Label': LABEL + '.server', 'AssociatedBundleIdentifiers': [LABEL + '.server'],
                'WorkingDirectory': str(config.working_directory), 'RunAtLoad': True,
                'KeepAlive': True, 'ThrottleInterval': 10}
    if (not isinstance(plist, dict) or any(plist.get(key) != value for key, value in expected.items())
            or plist.get('ProgramArguments') != [str(config.wrapper)]
            or not isinstance(plist.get('EnvironmentVariables'), dict)
            or any(key in plist['EnvironmentVariables'] for key in ('AGENT_CHAT_DB', 'AGENT_CHAT_PROJECT'))):
        raise ServiceError('configured server plist has an unsupported shape')
    wrapper = _trusted_regular_file(config.wrapper, description='wrapper', uid=config.uid)
    try:
        commands = [line[5:] for line in wrapper.read_text(encoding='utf-8').splitlines() if line.startswith('exec ')]
        command = shlex.split(commands[0]) if len(commands) == 1 else []
    except (OSError, ValueError) as error:
        raise ServiceError('configured server wrapper is unreadable') from error
    executable_positions = [i for i, value in enumerate(command) if value == str(config.executable)]
    db_positions = [i for i, value in enumerate(command) if value == '--db']
    if (len(executable_positions) != 1 or len(db_positions) != 1
            or executable_positions[0] >= db_positions[0]
            or _absolute_path(_command_option(command, '--db')) != config.master
            or _command_option(command, '--host', default=config.host) != config.host
            or _command_option(command, '--port', default=str(config.port)) != str(config.port)):
        raise ServiceError('configured server wrapper lacks an exact server database binding')
    return config.master.resolve()


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


def _systemd_value(value: str | Path, *, exec_start: bool = False, quoted: bool = True) -> str:
    """Escape specifiers, and quote directives that support word parsing."""
    text = str(value)
    if any(ord(char) < 32 for char in text):
        raise ServiceError("service paths cannot contain control characters")
    text = text.replace("%", "%%")
    if not quoted: return text
    text = text.replace("\\", "\\\\").replace('"', '\\"')
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
            command = [self.python, self.server_bin, "--db", args.db]
            if getattr(args, 'inbox_diagnostics', None):
                command.extend(["--inbox-diagnostics", args.inbox_diagnostics])
            if getattr(args, 'log_level', 'OFF') != 'OFF':
                command.extend(["--log-level", args.log_level, "--log-file", args.log_file])
            lines.append("exec " + " ".join(_quote(x) for x in command))
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
            if getattr(args, 'connect_only', False):
                command.append('--connect-only')
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
        # Keep final basename whitespace/backslashes away from parser boundaries.
        directory = args.project_root if args.project_root.endswith("/") else args.project_root + "/"
        lines = ["[Unit]", "Description=" + description, "", "[Service]", "Type=simple",
                 "WorkingDirectory=" + _systemd_value(directory, quoted=False), "ExecStart=" + _systemd_value(paths["wrapper"], exec_start=True),
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
        connect_only = False
        if component == 'all':
            try:
                wrapper = self.paths('client')['wrapper'].read_text()
                commands = [shlex.split(line[5:]) for line in wrapper.splitlines() if line.startswith('exec ')]
                connect_only = (len(commands) == 1 and len(commands[0]) >= 3
                                and commands[0][2] == 'bridge' and commands[0].count('--connect-only') == 1)
            except (OSError, UnicodeError, ValueError):
                pass  # Reporting must not change the existing restart behavior.
        for item in self._components(component):
            if self._platform() == "launchd": self._call(["launchctl", "kickstart", "-k", "gui/" + str(os.getuid()) + "/" + LABEL + "." + item])
            else: self._call(["systemctl", "--user", "restart", "agent-chat-" + item + ".service"])
        if connect_only:
            print('Configured client: connect-only. This command targets Agent Chat server/client; it does not request an external Codex-provider restart.', file=sys.stderr)

    def _drain_binding(self):
        config = _installed_server_config()
        if self._platform() != 'launchd' or os.geteuid() != config.uid or config.uid != 501:
            raise ServiceError('drain requires fixed installed macOS server account')
        if configured_server_database(config.plist) != config.master:
            raise ServiceError('drain fixed provider/store mismatch')
        paths = (config.plist, config.wrapper, config.executable)
        pins = tuple((str(path), hashlib.sha256(_trusted_regular_file(path, description='provider', uid=config.uid).read_bytes()).hexdigest()) for path in paths)
        master = _trusted_regular_file(config.master, description='database', uid=config.uid).stat()
        if stat.S_IMODE(master.st_mode) & 0o077:
            raise ServiceError('drain database must be owner-private')
        pins += (('store_identity', master.st_dev, master.st_ino, master.st_uid, stat.S_IMODE(master.st_mode)),)
        token_path = Path(str(config.master) + '.api-token')
        token_file = _trusted_regular_file(token_path, description='token sidecar', uid=config.uid)
        if stat.S_IMODE(token_file.stat().st_mode) & 0o077:
            raise ServiceError('drain token sidecar must be owner-private')
        token = token_file.read_text().strip()
        if not token:
            raise ServiceError('drain token sidecar is empty')
        return config, pins, token

    def _maintenance_call(self, config, token, action, epoch=None):
        data = dict(action=action)
        if epoch is not None: data['epoch'] = epoch
        request = urllib.request.Request('http://127.0.0.1:' + str(config.port) + '/api/server-maintenance',
            data=json.dumps(data).encode(), headers={'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'}, method='POST')
        # No redirect, environment proxy or alternate endpoint fallback.
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoMaintenanceRedirect())
        try:
            with opener.open(request, timeout=2) as response:
                raw = response.read(8193)
                if response.status != 200 or len(raw) > 8192: raise ValueError()
                result = json.loads(raw)
        except Exception as error:
            if hasattr(error, 'close'): error.close()
            raise ServiceError('maintenance unavailable or outcome uncertain; no restart; reconcile before retry') from error
        if (not isinstance(result, dict) or set(result) != {'instance','pid','epoch','active','sealed','state'}
                or not isinstance(result['instance'], str) or not re.fullmatch('[0-9a-f]{32}', result['instance'])
                or type(result['pid']) is not int or result['pid'] <= 0
                or type(result['active']) is not int or result['active'] < 0
                or type(result['sealed']) is not bool
                or result['state'] not in ('open','draining','sealed')
                or (result['epoch'] is not None and (not isinstance(result['epoch'], str) or not re.fullmatch('[0-9a-f]{32}', result['epoch'])))):
            raise ServiceError('invalid maintenance response; no restart')
        return result

    def drain(self, component, *, timeout=30, restart=False, resume=False, inspect=False):
        if component != 'server':
            raise ServiceError('drain targets only the fixed server component')
        if not math.isfinite(timeout) or not 0 < timeout <= 60:
            raise ServiceError('drain timeout must be greater than zero and at most 60 seconds')
        config, pins, token = self._drain_binding()
        first = self._maintenance_call(config, token, 'status')
        def same(value):
            if (value['instance'], value['pid']) != (first['instance'], first['pid']):
                raise ServiceError('server instance changed; no restart')
            return value
        if inspect: return first
        if restart and (first['sealed'] or first['state'] == 'sealed'):
            raise ServiceError('server initially sealed; reconcile previous restart; no retry')
        if resume:
            if not first['epoch']: raise ServiceError('server is not draining')
            return same(self._maintenance_call(config, token, 'resume', first['epoch']))
        current = same(self._maintenance_call(config, token, 'start'))
        epoch = current['epoch']; deadline = time.monotonic() + timeout
        if not epoch: raise ServiceError('drain did not start; no restart')
        while current['active']:
            if time.monotonic() >= deadline:
                raise ServiceError('drain timed out; admission remains closed; no restart')
            time.sleep(min(.05, max(0, deadline-time.monotonic())))
            current = same(self._maintenance_call(config, token, 'status'))
            if current['epoch'] != epoch: raise ServiceError('drain epoch changed; no restart')
        if not restart: return current
        current = same(self._maintenance_call(config, token, 'seal', epoch))
        if current['epoch'] != epoch or current['active'] != 0 or current['state'] != 'sealed' or not current['sealed']:
            raise ServiceError('server is not sealed and empty; no restart')
        new_config, new_pins, new_token = self._drain_binding()
        if config != new_config or pins != new_pins or token != new_token:
            raise ServiceError('provider changed after drain; server remains sealed; no restart')
        status = self.run(['launchctl', 'print', 'gui/501/' + LABEL + '.server'], check=True, capture_output=True, text=True)
        pid = re.findall(r'^\s*pid = (\d+)\s*$', status.stdout, re.MULTILINE)
        if pid != [str(current['pid'])]:
            raise ServiceError('provider PID differs from sealed HTTP instance; no restart')
        self._call(['launchctl', 'kickstart', '-k', 'gui/501/' + LABEL + '.server'])
        return dict(current, restart_requested=True)

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
    p.add_argument("action", choices=("install", "start", "stop", "restart", "status", "uninstall", "drain", "drain-restart", "drain-resume", "drain-status", "graceful-inspect", "graceful-restart", "graceful-status", "provider-handoff-run", "provider-handoff-status", "provider-handoff-reconcile", "provider-handoff-force", "provider-handoff-force-run", "provider-image-baseline", "provider-image-admit", "provider-image-rollover", "provider-image-upgrade"))
    p.add_argument("--component", choices=("server", "client", "all"), default="all")
    p.add_argument("--no-start", action="store_true")
    p.add_argument("--drain-timeout", type=float, default=30)
    p.add_argument('--request-id', help='durable graceful client request UUID; never change it to retry an uncertain signal')
    p.add_argument('--provider-manifest')
    p.add_argument('--provider-manifest-sha256')
    p.add_argument('--previous-provider-sha256')
    p.add_argument('--provider-source-id')
    p.add_argument('--expected-pid', type=int)
    p.add_argument('--expected-start', help='exact process start returned by graceful-inspect')
    p.add_argument('--expected-config-sha', help='exact config_sha256 returned by graceful-inspect')
    p.add_argument('--graceful-timeout', type=float, default=30)
    home = Path.home()
    p.add_argument("--db", default=str(home / ".local" / "share" / "agent-chat" / "state.sqlite3"))
    p.add_argument("--server", default="http://127.0.0.1:8765")
    p.add_argument("--project-root", default=os.getcwd())
    p.add_argument("--state-dir", default=str(home / ".local" / "state" / "agent-chat"))
    p.add_argument("--codex-bin", default=shutil.which("codex") or "codex")
    p.add_argument("--codex-server", default="ws://127.0.0.1:4500")
    p.add_argument("--connect-only", action="store_true", help="attach the client to an existing Codex app-server without starting or owning it")
    p.add_argument("--token-file")
    p.add_argument("--inbox-diagnostics")
    p.add_argument("--log-level", choices=("OFF", "ERROR", "WARNING", "INFO", "DEBUG"), default="OFF")
    p.add_argument("--log-file")
    p.add_argument("--project")
    return p


def _main(argv=None) -> int:
    args = parser().parse_args(argv)
    if args.log_level != 'OFF' and not args.log_file:
        raise ServiceError('--log-file is required when --log-level is enabled')
    if args.log_level != 'OFF' and args.log_file and not Path(args.log_file).expanduser().is_absolute():
        raise ServiceError('--log-file must be an absolute path')
    logging_runtime = configure(LoggingConfig(level=args.log_level,
                                               path=args.log_file if args.log_level != 'OFF' else None))
    service_log = logging_runtime.logger('agent_chat.service')
    correlation_id = new_correlation_id()
    service_log.event('operation_start', correlation_id=correlation_id)
    for name in ("db", "project_root", "state_dir", "token_file"):
        if getattr(args, name, None): setattr(args, name, _absolute(getattr(args, name)))
    try:
        if args.action == "install" and args.component in ("client", "all") and not args.connect_only:
            codex = shutil.which(args.codex_bin) if not Path(args.codex_bin).is_absolute() else args.codex_bin
            if not codex: raise ServiceError("cannot find codex; pass --codex-bin with an absolute path")
            args.codex_bin = _absolute(codex)
        manager = ServiceManager()
        if args.action in ('provider-image-baseline','provider-image-admit','provider-image-rollover','provider-image-upgrade'):
            if manager._platform() != 'launchd':
                raise ServiceError('provider image admission requires launchd')
            from . import provider_image_admission, provider_service
            try:
                directory=provider_service.home_directory()
                if args.action=='provider-image-baseline':
                    path=provider_image_admission.enroll_baseline(directory)
                    print('baseline='+str(path))
                else:
                    _,command=provider_service.configured(manager)
                    address=provider_service.option(command,'--codex-server')
                    if args.action=='provider-image-upgrade':
                        result=provider_image_admission.upgrade(manager,directory,address)
                        print('provider image '+result['state']+' sha256='+result['sha256'])
                    elif not args.provider_manifest or not args.provider_manifest_sha256:
                        raise ServiceError('exact template manifest and SHA-256 required')
                    elif args.action=='provider-image-admit':
                        path,pin=provider_image_admission.admit(directory,args.provider_manifest,args.provider_manifest_sha256,address)
                        print('manifest='+str(path)+' sha256='+pin)
                    else:
                        if not args.previous_provider_sha256:
                            raise ServiceError('exact previous provider manifest SHA-256 required')
                        result=provider_image_admission.activate(manager,directory,args.provider_manifest,
                            args.provider_manifest_sha256,args.previous_provider_sha256,address)
                        print('provider manifest activated while stopped; start separately: '+result['sha256'])
            except provider_service.Blocked as error:
                raise ServiceError(str(error)) from error
        elif args.action.startswith('provider-handoff-') or (args.action == 'restart' and args.component == 'all' and manager._platform() == 'launchd'):
            if manager._platform() != 'launchd':
                raise ServiceError('managed provider handoff currently requires launchd; no components restarted')
            from . import provider_service
            try:
                if args.action in ('provider-handoff-force','provider-handoff-force-run'):
                    from . import provider_force_continuation
                    if args.action == 'provider-handoff-force-run':
                        output = provider_force_continuation.run(manager,args.request_id)
                    else:
                        output = provider_force_continuation.submit(manager,args.request_id)
                        if provider_service.field(output,'.state') not in ('completed','outcome_unknown'):
                            output = provider_service.wait(args.request_id)
                elif args.action == 'provider-handoff-run':
                    output = provider_service.run_worker(manager, args.request_id)
                elif args.action == 'provider-handoff-reconcile':
                    output = provider_service.reconcile(manager, args.request_id)
                elif args.action == 'provider-handoff-status':
                    output = provider_service.status(args.request_id)
                else:
                    output = provider_service.submit(manager, args)
                    output = provider_service.wait(provider_service.field(output, ".request_id"))
                print(output)
                if args.action in ("restart", "provider-handoff-force") and provider_service.field(output, ".state") != "completed":
                    return 2
            except provider_service.Blocked as error:
                raise ServiceError(str(error)) from error
        elif args.action.startswith('graceful-'):
            if args.component != 'client':
                raise ServiceError('graceful continuity supports only --component client')
            from .client_continuity import ClientContinuity
            controller = ClientContinuity(manager)
            if args.action == 'graceful-inspect':
                result = controller.inspect()
            elif args.action == 'graceful-status':
                result = controller.status(args.request_id)
            else:
                result = controller.restart(args.request_id, expected_pid=args.expected_pid,
                    expected_start=args.expected_start, expected_config_sha=args.expected_config_sha,
                    timeout=args.graceful_timeout)
            print(json.dumps(result, sort_keys=True))
            if args.action == 'graceful-restart' and result['phase'] != 'new_generation_observed':
                return 2
        elif args.action in ('drain', 'drain-restart', 'drain-resume', 'drain-status'):
            result = manager.drain(args.component, timeout=args.drain_timeout,
                                   restart=args.action == 'drain-restart', resume=args.action == 'drain-resume', inspect=args.action == 'drain-status')
            print(json.dumps(result, sort_keys=True))
        else:
            getattr(manager, args.action)(args if args.action == "install" else args.component)
        service_log.event('operation_complete', correlation_id=correlation_id)
        return 0
    except BaseException as error:
        service_log.error('operation_failed', error, correlation_id=correlation_id)
        raise
    finally:
        logging_runtime.close()


def main(argv=None) -> int:
    try:
        return _main(argv)
    except (ServiceError, OSError, subprocess.CalledProcessError) as error:
        print(str(error), file=sys.stderr)
        return 2
