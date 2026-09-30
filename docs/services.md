# Background services

`agent-chat-service` installs native services for your user account: launchd on
macOS and systemd's user manager on Linux. The server and client can run on the
same machine or be installed separately. The client owns its local Codex
app-server; project commands still execute on that client machine.

## Install

From a checkout, add its `bin` directory to `PATH`, or install the Python package.
Python 3.10+ is required; a client installation also needs `codex` on `PATH` (or
an absolute `--codex-bin` path). Run as your normal user, without `sudo`.

For an existing installation, supply its actual database and machine state
paths. Changing the database or state directory would create a separate chat
store or host identity.

```sh
agent-chat-service install \
  --db /absolute/path/to/agent-chat/.agent-chat/state.sqlite3 \
  --project-root /absolute/path/to/your/project \
  --state-dir "$HOME/.local/state/agent-chat" \
  --no-start
```

This stages both service definitions without starting another server or client.
On Linux it also enables them for the user manager's next startup. The normal
`install` command starts them immediately if `--no-start` is omitted. The default
database for a new service installation is `~/.local/share/agent-chat/state.sqlite3`.

Before switching from terminal processes, let active agent turns and guarded
commands finish. Stop the existing client once, then the server, and start the
services. Stopping the client also stops its owned Codex app-server; reconnect
your Codex conversations to the same endpoint afterward if needed. Existing
thread bindings, messages, queues and reservations remain in the existing store.

```sh
agent-chat-service start
agent-chat-service status
```

The wrappers read the API token file at startup; token values are not embedded
in service definitions or command arguments. By default this is the existing
`<db>.api-token` sidecar. An explicit `--token-file` selects another private token
file. Keep that file readable only by your account. The server can create its
default sidecar on first startup; the client retries if it starts earlier.

The client discovers every project unless `--project PROJECT_ID` is supplied.
`--project-root` sets its working directory, and `--codex-server` defaults to
`ws://127.0.0.1:4500`. Tool executable paths are captured during installation;
reinstall the definitions if a Python, Node or Codex installation moves.

For a remote server, install only the component needed on each host:

```sh
# Server host; does not require Codex.
agent-chat-service install --component server --db /absolute/state.sqlite3

# Client host; provision the existing server's token privately first.
agent-chat-service install --component client \
  --server https://chat.example.com \
  --token-file /absolute/private/chat-token \
  --project-root /absolute/project
```

The generated server uses the existing loopback defaults. Remote access still
requires the documented HTTPS proxy or SSH tunnel; service installation does
not expose the HTTP port to the network.

## Manage

Each command accepts `--component server`, `--component client`, or the default
`--component all`.

```sh
agent-chat-service status
agent-chat-service restart --component server
agent-chat-service stop --component client
agent-chat-service start --component client
agent-chat-service uninstall
```

Uninstall stops the selected services and removes their definitions and launch
wrappers. It preserves databases, API tokens, host/bridge identities and logs.
Restarting or uninstalling the client stops its app-server, so finish active
agent work first.

On macOS, definitions are in `~/Library/LaunchAgents/com.agent-chat.*.plist`,
with registered helper bundles named **Agent Chat Server** and **Agent Chat
Client** under `~/.local/share/agent-chat/services`. The same names appear in
System Settings → General → Login Items & Extensions → App Background Activity.
Stdout/stderr logs are in `~/Library/Logs/agent-chat`. They start when you log in.

On Linux, definitions are in `~/.config/systemd/user/agent-chat-*.service` and
wrappers in `~/.local/share/agent-chat/services`; configured `XDG_CONFIG_HOME`
and `XDG_DATA_HOME` override these base directories. Read logs with:

```sh
journalctl --user -u agent-chat-server.service -u agent-chat-client.service -f
```

Linux user services normally follow the user's login lifecycle. To keep them
running after logout and start the user manager at boot, explicitly enable
lingering with `loginctl enable-linger "$USER"` on that host; the installer does
not change this system policy. See the [systemd loginctl documentation](https://www.freedesktop.org/software/systemd/man/252/loginctl.html).
macOS uses the native [LaunchAgent lifecycle](https://developer.apple.com/library/archive/documentation/MacOSX/Conceptual/BPSystemStartup/Chapters/CreatingLaunchdJobs.html).
