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

To stage bounded private server events, pass an explicit severity and absolute
destination during installation:

```sh
agent-chat-service install --component server --no-start \
  --db /absolute/state.sqlite3 --log-level INFO \
  --log-file /absolute/private/server-events.jsonl
```

Logging defaults to `OFF` and does not inherit environment settings. The wrapper
contains only the selected flags and writes fixed-schema owner-only events
locally. Reinstall or perform the separately authorized server reload before an
existing service uses changed logging configuration.

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


## Bounded server-only HTTP maintenance

This source capability requires independent review and explicit activation. An
older loaded server has no maintenance endpoint: guarded maintenance refuses,
with **no raw restart fallback**. First activation therefore needs a separate
owner-selected bootstrap/quiescence disposition; integrating files cannot make
this capability live. Original refused model transfers remain stopped.

The supported commands for the fixed installed macOS UID501 provider are:

```sh
agent-chat-service drain-status --component server
agent-chat-service drain --component server --drain-timeout 30
agent-chat-service drain-restart --component server --drain-timeout 30
agent-chat-service status --component server
```

`drain-status` is read-only process-local HTTP status. `drain` atomically closes
new work admission and waits at most the selected bounded interval for active
HTTP handlers. `drain-restart` additionally seals the empty epoch, checks fixed
provider/configuration and the launchd PID against that HTTP process, then
requests only `launchctl kickstart -k gui/501/com.agent-chat.server`. Component
`all` (the ordinary default) and `client` are refused. Timeout must be finite,
greater than zero and at most 60 seconds; individual bounded HTTP calls can add
up to two seconds apiece. No redirect, environment proxy or alternate endpoint
is used. The exact installed plist/wrapper/executable/database association and
owner-private token sidecar are required; the HTTP bearer is never printed.
No database, token, service configuration, client, Bridge or native job changes
are performed by the maintenance handshake. Legacy `restart` remains an
unguarded command and is not a safe substitute for this protocol.

The endpoint is `POST /api/server-maintenance`, direct numeric loopback peer,
trusted Host/origin, configured Bearer authentication, and no forwarding headers.
It explicitly trusts the installed local-account/control domain and anyone
with its bearer on loopback; it does not authenticate a named agent or isolate
hostile same-UID principals. Remote/shared-bearer owner identity is not promoted.
A local reverse proxy forwarding the bearer is not an independent actor proof.
Actions `status`/`start` have exactly the `action` field; `seal`/`resume` also
require the exact current `epoch`. The response gives random per-server
`instance`, PID, epoch, active count, sealed flag and open/draining/sealed state.
Only authenticated maintenance requests are excluded from the work ledger.
This is volatile operational state, not a transfer receipt or historical audit.

Every ordinary request ticket starts before header reads and remains through
body reads, dispatch, response writes and final flush, including aborted/error
paths. Keepalive idle/header waits and SSE streams conservatively count until
they close or time out. Drain start and request admission share one lock: work
already admitted may finish, later work is rejected with 503 without dispatch.
Readiness includes incomplete/slow requests and downloads; logs, agent idle or
bridge queue counts are not a substitute. Response completion means bytes
handed to the server socket/flush, not client acknowledgement or a native
accepted-effects ledger. Rejected clients may still be receiving a 503 when a
sealed server restarts; no new work can be admitted. HTTP drain does not stop
background jobs, other stores, Codex, Unity or already accepted external effects.

A timeout makes no kickstart request and leaves admission closed. Before seal,
`agent-chat-service drain-resume --component server` resumes the exact observed
epoch after reconciliation. Seal is a one-time transition: a matching already-sealed epoch is refused,
and drain-restart refuses initially sealed status. Concurrent callers cannot
share the same seal authorization. A zero-active seal cannot be resumed: a failed
kickstart leaves the server sealed, requiring a separately selected same-provider
restart/recovery disposition, never a hidden unseal or retry loop. A fresh
WebServer starts open with a new instance identity; verify the same provider and
DB and new running PID afterward. Process status alone is not imported-byte or
browser-cache attestation. Provider/store inode/token/PID/instance/epoch drift refuses restart.

Preserve original request outcomes on interruption. Ordinary sends are not
idempotent: a lost response may follow commit. Reconcile its exact synthetic
marker/message and attachment metadata through the intended recipient's normal
inbox/conversation; never retry blindly or use another actor. A download only
publishes after metadata/hash validation and exclusive no-overwrite destination
creation; preserve an uncertain exact destination for authorized inspection,
never overwrite. No global HTTP transfer receipt API is claimed. Source rollback
is limited to exact reviewed preimages/modes and the same separately selected
server-only maintenance operation, preserving DB/messages/history and uncertain
operations. Browser reload and any real proxy limits are separate observations.

Restart attempt exclusion is per live process/epoch, not a persistent service
request ledger. If an ambiguous kickstart actually replaced the process, a new
server starts open with a new identity; an operator must reconcile that provider
and PID instead of issuing a blind new drain-restart. No durable cross-process
idempotency or automatic recovery is claimed.

When a routine Codex CLI update changes the executable digest, exact-image
provider checks refuse the changed bytes. Use the explicit
[provider image admission and stopped-service activation procedure](provider-image-admission.md)
to enroll the existing signer before upgrading and select the newly admitted
manifest. Do not bypass a signature or ownership refusal.
