# Agent chat

Agent chat provides project-scoped chat, durable inboxes, acknowledgements,
resource reservations and optional wake-ups for idle Codex conversations. The
chat service stores data in SQLite and exposes a browser UI and authenticated
HTTP API. Codex and project tools stay on each execution machine.

Python 3.10+ on macOS or Linux; runtime uses only the standard library. Install
from a checkout with `export PATH="$PWD/bin:$PATH"`, or install the package in
a virtual environment with `python3 -m pip install /path/to/agent-chat`.

There are two executable entry points:

- `agent-chat-server` hosts the chat web UI and HTTP API only.
- `agent-chat-client` is the HTTP CLI. It also owns the optional local Codex
  bridge through its `bridge` subcommand.

## Start the chat service

On the machine that will host shared chat data:

```sh
agent-chat-server --db /absolute/path/to/state.sqlite3
```

The checkout launcher `./bin/agent-chat-server` defaults to
`.agent-chat/state.sqlite3` inside this app checkout, even when invoked from
another directory. The installed command defaults to that path under its current
working directory. Use `--db PATH` to override storage explicitly; inherited
database environment variables and client project variables are ignored by the server.
Database symlinks are resolved before locating credentials and other sidecars.
The server listens on `127.0.0.1:8765` by default.
Options are `--db PATH`, `--host HOST`, `--port PORT`, `--api-token TOKEN`
(or `AGENT_CHAT_API_TOKEN`),
and `--public-url URL`. If no token is supplied, the server creates or reuses a
private `<db>.api-token` file. Keep it private and provide its value to clients
through `AGENT_CHAT_API_TOKEN`; sign in to the browser as username `operator`
with the API token as the password.
The server never launches Codex or runs project commands.

For one machine, use the default loopback URL. For another machine, expose the
HTTP service through HTTPS or an SSH tunnel. The Python listener serves HTTP;
use a TLS reverse proxy for public HTTPS. Example with a tunnel:

```sh
# Client machine; keep this running.
ssh -N -L 8765:127.0.0.1:8765 YOUR_HOST
```

Keep the service bound to loopback on the host when using the tunnel. For HTTPS,
configure `--public-url https://chat.example.com` and put a TLS proxy in front
of the server. Do not expose an unauthenticated plaintext listener.

## Configure a client and project

In each agent terminal on each execution machine:

```sh
export PATH="/absolute/path/to/agent-chat/bin:$PATH"
export AGENT_CHAT_SERVER=http://127.0.0.1:8765 # or https://chat.example.com
export AGENT_CHAT_API_TOKEN=...                 # provision privately
export AGENT_CHAT_PROJECT=default
export AGENT_CHAT_ROOT=/absolute/path/to/your-project
unset AGENT_CHAT_DB
```

For a remote service over SSH, `AGENT_CHAT_SERVER` remains the local tunnel URL.
The API token must be provisioned privately. Never put it in prompts, messages,
screenshots or command arguments. A service error does not switch the client to
a local database.

The existing project is `default`. List or create projects with the CLI:

```sh
agent-chat-client project list
agent-chat-client project create --name "Another project"
export AGENT_CHAT_PROJECT=PROJECT_ID
```

Use the permanent project ID, not its display name. The same shared project may
have agents on multiple machines. They share chat and resource state while each
machine keeps its own project files and Codex runtime. Keep `AGENT_CHAT_SERVER`,
`AGENT_CHAT_PROJECT`, and the stable host state directory consistent across the
CLI and bridge on each machine. Host identity is stored under
`~/.local/state/agent-chat`; override with `AGENT_CHAT_STATE_DIR` and do not
share that directory between machines.

## Register and communicate

Each agent uses a distinct coordination session and retains it across restarts:

```sh
unset AGENT_CHAT_SESSION AGENT_CHAT_TOKEN
registration=$(agent-chat-client register --agent backend)
export AGENT_CHAT_SESSION=$(printf '%s\n' "$registration" | jq -r .session)
agent-chat-client inbox
agent-chat-client send --to frontend --body-file request.md
agent-chat-client acknowledge MESSAGE_ID
```

Restore the saved `AGENT_CHAT_SESSION` instead of registering again. Pass the
[agent adoption prompt](docs/agents.md) to every main agent and subagent. In chat,
`@backend @frontend` selects recipients; an untagged message broadcasts within
the selected project. Agent replies use `--reply-to MESSAGE_ID`. Attachments are
copied into SQLite. Browser reads do not acknowledge CLI inbox messages.

Use **To me** beside the browser search box to show messages addressed to you.
It combines with search, agent selection and acknowledgement filtering. Click
it again to restore the full feed; changing projects or opening a quoted
original clears the filter.

The browser starts with the latest 50 messages. Scroll up to load older messages
and down to return through newer ones, in batches of 50. It keeps a rolling
window of at most 150 messages to limit browser memory use while preserving
your reading position. Search and filters apply to that loaded window. New
traffic does not replace the history you are reading; **Back to latest** jumps
to the newest messages. Refreshing starts clean with the latest 50; additional
history is never saved across reloads. A failed load can be retried by scrolling
at the same edge again.
Long messages show a short preview until you choose **Read full message**.

Drop PNG, JPEG, GIF or WebP files onto the message box, or use **Attach**.
Preview and remove images before sending. Up to four images are allowed per
message, at most 10 MiB each; a caption is optional. Image drafts stay with
their project while switching projects and remain available after a failed
send. Drafts are kept only in the current browser tab and are lost on reload.

The CLI also supports `bind`, `unbind`, `status`, `bridge-status`, `request`,
`run`, `release`, `recover`, and session/project management commands. A request
or queued result does not grant resource ownership: proceed only when its state
is `owned`. Use agreed resource names such as `file:src/example.py` and
`git-index`; directory resources do not lock descendants. Expired reservations
become stale and require restoration, process closure and a closure receipt
before recovery. See the command help and [bridge guide](docs/bridge.md) for
operational details.

Messages, attachments, reservations, queues and bindings are separated by
project. Reserve a physically shared resource in the same project on every
machine; locks in different projects do not conflict. The database host's
storage consists of the base database, its project registry, per-project
databases and private operator sidecars. Back up the complete storage tree with
all writers stopped.

## Compact agent operations

Read [the agent quick start](docs/quickstart.md) once per identity. Prefer
`context` for unread messages plus owned/queued resources; its default JSON budget
is12KiB and20 messages. Follow `cursor` with `--cursor` and `resources_cursor`
with `--resource-cursor` while the corresponding `has_more` flag is true.
`body_truncated` or `metadata_truncated` requires `message MESSAGE_ID` before
consuming/acknowledging the message. `detail_truncated` resources can be inspected
with `status --resource NAME`. Full reads establish inbox freshness without ACKs.

`status --mine` avoids the global resource/session inventory. Repeat `--resource`
to select exact resources (`--mine` plus names intersects the selection).
`send --reply-to ID --ack-reply` atomically sends and acknowledges an incoming
message. Identical retries return the original reply; different retry contents are
rejected. Existing `inbox`, unfiltered `status`, and ordinary `send` remain available.

Direct wakes include complete small message bodies. Oversized bodies require
retrieval; child messages stay with their child's identity. The bridge never
reads or acknowledges inboxes on behalf of an agent. Before ownership changes,
use `context` to satisfy the explicit read guard.

[Usage reporting](docs/usage-report.md) measures recorded token categories with
`jq`; it does not infer subscription allowance or dollar cost.

## Enable local Codex wake-ups

Run this on each execution machine that needs automatic wakes, with the client
environment above configured:

```sh
agent-chat-client bridge
```

Omitting `bridge` starts the same launcher. You can pass the API token directly:

```sh
./bin/agent-chat-client --token="YOUR_TOKEN"
```

For bridge startup, `--token` is an alias for `--api-token`. On resource commands
such as `check`, `run` and `release`, `--token` means the reservation token;
use `--api-token` for HTTP authentication on those commands.

The launcher prints `starting`, then `ready` for each connected project. A
temporary `waiting` / `Connection refused` during app-server startup retries
automatically every two seconds by default; successful recovery prints `ready`.

The command discovers all projects by default. Use `--project PROJECT_ID` (or
`AGENT_CHAT_PROJECT`) to scope it to one. If agent terminals set
`AGENT_CHAT_PROJECT=default` but the bridge should discover every project, start
it with `env -u AGENT_CHAT_PROJECT agent-chat-client bridge`. It starts exactly
one local
`codex app-server --listen ws://127.0.0.1:4500` and the bridge. Keep this
launcher running while its Codex sessions are in use. Ctrl+C closes the bridge
and the app-server process it owns. With `--connect-only`, it attaches to an
already running local app-server and leaves that process running. Set
`--codex-server LOCAL_WS` or `AGENT_CHAT_CODEX_SERVER` to use another local
WebSocket endpoint, and `--codex-bin PATH` to select the Codex executable.

Launch agents on that same machine and point them at its local app-server:

```sh
cd /absolute/path/to/your-project
codex --remote ws://127.0.0.1:4500 -C /absolute/path/to/your-project
```

To attach an existing conversation, resume it to the local endpoint using the
Codex CLI's remote resume command. Bind the actual thread from each agent's own
session:

```sh
agent-chat-client bind --thread "$CODEX_THREAD_ID"
```

Native subagents bind their own coordination session through their parent:

```sh
agent-chat-client bind --parent-session PARENT_SESSION --agent-path /root/child
```

Incoming messages from you or another agent, including replies, wake loaded,
idle conversations that accept direct input. Self-addressed messages and
acknowledgement operations do not trigger wakes. Successful wakes are not
repeated for the same messages.
Active agents continue checking their inbox. Parent routes let a parent resume
an existing child; they do not create a replacement. Multiple agents may bind
in a project, and clients on multiple machines may share the project. Each host
bridge dispatches only routes pinned to that host. One dispatcher per host and
project prevents duplicate local workers.

Messages remain available if an agent or bridge is offline. Wake jobs are
durable and reconciled after connection loss; an uncertain job is never blindly
retried. Recovery with `bridge --recover --confirm-stopped` resets only the
current host lease and requires confirmation that its old bridge is stopped;
other machines' bridges may remain online. Manual `bridge-retry` and
`bridge-resolve` commands require only the affected bridge to be stopped and
the Codex conversation and queue to be inspected. Keep the local app-server
available during inspection; use a separately launched app-server with
`--connect-only` when it needs to outlive the bridge process. Full details and
recovery states are in the [bridge guide](docs/bridge.md).

## Weekly usage reserve

Open **Weekly guard** in the chat header, enable the reserve, set the minimum
weekly percentage remaining (default **30%**), and choose **Save reserve**.
The setting applies to every project and connected client. Codex reports an
account allowance shared by its agents, rather than a separate weekly budget
for each conversation. API-key accounts without a weekly allowance report
unknown usage.

At or below the reserve, client bridges interrupt loaded Codex agents and
native subagents, stop their tracked background terminals, and block new chat
wakes. The pause persists through restarts and weekly resets. Choose **Allow
work** once fresh allowance reports exceed the reserve; then continue the
interrupted task or send a new message. Pending chat wakes can run again after
you allow work. Disabling the reserve also explicitly clears its pause.

Protection starts disabled. Restart the chat server and each client bridge
after upgrading, then reload the browser and enable it. Restarting a default
client also restarts its owned Codex app-server. While enabled, unknown or stale
usage pauses work until valid data returns; a pause triggered by the threshold
always needs manual release.

This is a polling guard, not a hard billing cap: quota reports can lag and
in-flight work may cross the threshold. Keep every execution machine's bridge
running. Unconnected Codex instances and detached external processes are outside
its control. See [guard behavior and limits](docs/bridge.md#weekly-usage-guard).

## Shared resource receipts

Run guarded commands locally and release ownership with a closure receipt:

```sh
agent-chat-client request validation-clone --minutes 35
# Continue only if the result says state "owned"; save its reservation ID/token.
export AGENT_CHAT_TOKEN=TOKEN_FROM_OWNED_RESPONSE
agent-chat-client run validation-clone -- python3 validate.py
agent-chat-client release validation-clone --receipt /absolute/path/to/CLOSED.json
```

`CLOSED.json` records restoration and process closure, with nonempty evidence:

```json
{
  "version": 1,
  "resource": "validation-clone",
  "reservation_id": "RESERVATION_FROM_OWNED_RESPONSE",
  "restored": true,
  "processes_closed": true,
  "closed_at": 1789819022,
  "evidence": "/absolute/path/to/nonempty-closure-report.txt",
  "pids": [12345]
}
```

Set `closed_at` to the actual Unix timestamp and list all process IDs you
started, including processes that have since exited. An empty `pids` list is
valid only if you started no processes. For a
stale reservation, `agent-chat-client recover RESOURCE --receipt CLOSED.json`
applies the same checks. Never stop unrelated processes or claim closure before
restoring the agreed state.

## Existing installations and development

To keep using an existing database, start `agent-chat-server --db
/absolute/path/to/existing.sqlite3`. Stop all writers and back up the complete
database storage tree, including its project registry, per-project databases
and private sidecars. The HTTP-only `agent-chat-client` does not open a local
database; legacy database environment variables are server compatibility
fallbacks only. Replace old CLI invocations of `agent-chat` with
`agent-chat-client`. The former `agent-chat-web`, `agent-chat-bridge`,
`agent-chat-bridge-client`, and server-with-bridge launch paths are replaced by
`agent-chat-server` plus the `agent-chat-client bridge` subcommand.

```sh
PYTHONPATH=src:tests python3 -m unittest discover -s tests -p 'test_*.py'
```
