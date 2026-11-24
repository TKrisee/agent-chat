# Agent Chat

Run from this directory with Go 1.25 or newer and a C compiler for the pinned SQLite driver. The service owns one SQLite database, a durable job queue, and its own project workspaces. The default listener is loopback only.

```sh
go test ./...
go run ./cmd/agentchat -data /tmp/agentchat-demo -listen 127.0.0.1:8877
```

For frontend browser tests, run `npm ci`, `npx playwright install chromium`, and `npm test` from `src/web`. The test builds the UI and starts a fixture service with a temporary data directory. Go and a C compiler must be available on PATH.

Use a new data directory for isolated runs. Reuse it to test restart. Pass `-static ./web/dist` to serve the built chat UI from the same origin. Root is always `root` in HTTP requests and state. Project conversations use the exact `po_id` from the state endpoint.

```sh
curl -sS -H 'Content-Type: application/json' \
  -d '{"id":"1eb24b23-c3c9-46cc-8075-d3209738d212","recipient":"root","text":"Start project atlas: Review the launch checklist"}' \
  http://127.0.0.1:8877/api/messages
curl -sS http://127.0.0.1:8877/api/state
```

The fixture accepts `Start project NAME: TASK` for a new project and `For project NAME: TASK` for a follow-up. It also responds to plain feedback sent to a PO and understands `Stop project NAME` and `Resume project NAME`. The scheduler runs with the browser closed. Worker output attaches to its task message. Use the returned attachment URL to download it. Send a fresh client UUID for each distinct message. Reposting the same UUID with the same recipient and text reads back the original receipt.

Root drafts the project outcome from the operator request. The PO gathers project context and returns one scope assessment. Root sends the final assignment after reading that assessment or tells the operator what blocks it. The final assignment is the first step that lets the PO queue a worker.

## Service endpoints

- `GET /api/state` lists messages, projects, tasks, agents, approvals, and job dispositions.
- `POST /api/messages` accepts an operator message addressed to Root or a registered PO.
- `GET /api/attachments/{id}` serves an immutable, bounded copy. Non-image content downloads.
- `POST /api/projects/{id}/stop` withholds new submissions. The response counts unresolved submitted, running, and uncertain effects. A project with unresolved effects is shown as stopping.
- `POST /api/projects/{id}/resume` admits pending work again.
- `POST /api/approvals/{id}` accepts `{"decision":"approved"}` or `{"decision":"denied"}` as an operator response. It records the decision and delivers a durable resolution to the requester.

The job handoff commits before the runtime call. A restart recovers submitted or running jobs by their original ID. Only a verified unstarted effect or a typed pre-submission capacity rejection can retry. A recovered completion applies its workflow transaction once. Unknown effects remain visible and fenced. Project stop is an admission decision, and it leaves existing effects available for reconciliation. The local fixture records its own start marker and completion receipt to exercise those boundaries independently of native threads.

Attachments are regular files within the runner's project workspace, up to 8 MiB per file and 32 MiB per result. SQLite stores the uploaded bytes and metadata atomically with the message. Follow-up runtime jobs receive scoped, locally materialized copies of task attachments. The first slice uses private service-owned data, without migrating the legacy bridge or project registry.

## Optional Codex adapter

Build with the `codexruntime` tag to include the adapter. It uses the installed Codex CLI and jq. The native adapter's own durable receipts are stored in the selected data directory, alongside the service database.

```sh
go test -tags codexruntime ./...
go run -tags codexruntime ./cmd/agentchat \
  -runtime codex -data /tmp/agentchat-native -listen 127.0.0.1:8877
```

Use `-codex-path` or `-jq-path` to select exact executables. The default fixture build works independently of this optional package.

## Distributions and background operation

The Go distribution workflow builds on native Linux amd64, macOS arm64, and Windows amd64 runners with CGO and the `codexruntime` tag. Each workflow artifact contains the executable, built `web` directory, service examples, and a SHA-256 file for the archive. Unix archives are tar.gz and preserve executable permissions; Windows archives are zip. Stable tagged releases publish these archives and an update manifest; verify the checksum from a trusted channel before use. Keep the executable and `web` directory together and choose a separate private data directory. For example:

```sh
./agentchat -runtime codex -data "$HOME/.local/share/agentchat" -static "$PWD/web" -listen 127.0.0.1:8877 -codex-path /absolute/path/to/codex -jq-path /absolute/path/to/jq
```

To use managed updates, start `agentchat-launcher` from the extracted release directory with the desired service options. It keeps downloaded versions alongside the launcher and restarts the service after an update. The installation directory must be writable by the service account. The direct `agentchat` executable remains available for service configurations that manage their own upgrades.

The binary defaults to the fixture runtime if `-runtime codex` is omitted. The Codex CLI and jq are separate prerequisites; install and authenticate Codex for the account that runs the service. Provide absolute paths to both executables, since background service PATH may differ from an interactive shell. The service stores its own SQLite and runtime state under `-data`; choose a directory writable only by that account. Open `http://127.0.0.1:8877` on the same machine. Do not expose the listener to the network.

### Linux user service

Copy `packaging/agentchat.service` to `~/.config/systemd/user/agentchat.service`, replace every `/ABSOLUTE/PATH/TO/...` with paths accessible to your account, and run:

```sh
systemctl --user daemon-reload
systemctl --user enable --now agentchat.service
systemctl --user status agentchat.service
journalctl --user -u agentchat.service
```

This runs under your user account and normally starts with its user manager at login. If the host administrator has enabled lingering for that user, it can also start at boot and remain active after logout. Stop and disable it with `systemctl --user disable --now agentchat.service`.

### macOS login agent

Copy `packaging/com.agentchat.server.plist` to `~/Library/LaunchAgents/`, replace every placeholder with an absolute path (XML-escape special characters), and create the parent directory for the log files. Then run:

```sh
launchctl bootstrap "gui/$(id -u)" ~/Library/LaunchAgents/com.agentchat.server.plist
launchctl print "gui/$(id -u)/com.agentchat.server"
```

The agent runs as the logged-in user, starts at login, and restarts if it exits. To stop and remove it from the current session, run `launchctl bootout "gui/$(id -u)" ~/Library/LaunchAgents/com.agentchat.server.plist`; remove the plist to prevent later login starts.

### Windows service

From an elevated PowerShell, install the native Windows service with absolute paths. For example, choose an account and paths that are accessible to that account:

```powershell
.\agentchat.exe service install -data 'C:\AgentChatData' -static 'C:\AgentChat\web' -runtime codex -codex-path 'C:\Tools\codex.exe' -jq-path 'C:\Tools\jq.exe' -start demand -account LocalSystem
.\agentchat.exe service start
```

The service uses the selected Windows service account, not the interactive user's environment or Codex login. Provision Codex configuration and authentication for that account, restrict access to its data directory, and grant it access to the executable, UI and tools. `LocalSystem` is highly privileged; select `NT AUTHORITY\LocalService` or `NT AUTHORITY\NetworkService` where suitable and configure the required access. The default start mode is `demand`; select `-start auto` for boot startup. The installer does not configure automatic recovery after a failure. Check service status and errors with Windows Services or Event Viewer. From an elevated shell, `agentchat.exe service stop` requests shutdown and `agentchat.exe service uninstall` removes the service registration. Stop it before moving or replacing its files; removal does not delete the data directory.

To opt into automatic recovery, run the following from an elevated PowerShell after installation:

```powershell
sc.exe failure AgentChat reset= 86400 actions= restart/60000
sc.exe failureflag AgentChat 1
sc.exe qfailure AgentChat
sc.exe qfailureflag AgentChat
```

This schedules a restart after 60 seconds on failure and resets the failure count after a day without failure; the last configured action is repeated for later failures. The failure flag also covers a service that reports a stopped status with a nonzero error, and its change takes effect after the next system restart. An ordinary successful stop does not trigger recovery. A queued restart can still happen after a manual stop; disable the service to prevent it. Review repeated failures in Event Viewer instead of allowing an unresolved failure loop to continue. See Microsoft's [service recovery configuration](https://learn.microsoft.com/en-us/dotnet/core/extensions/windows-service) and [failure flag semantics](https://learn.microsoft.com/en-us/windows/win32/api/winsvc/ns-winsvc-service_failure_actions_flag).
