# Codex wake bridge

The chat stores messages immediately. The bridge checks pending messages every
two seconds and asks a local Codex app-server about bound threads. It can run
inside `agent-chat-server`, as a separate local `agent-chat-bridge`, or on the
agents' machine as `agent-chat-bridge-client` against a hosted HTTP coordinator. Sending does not depend on either the bridge or Codex being available.
The browser and CLI also work with agents that do not use Codex; only automatic
wake-ups depend on Codex.

## Binding and lifecycle

1. Start `agent-chat-web` with the project's absolute `AGENT_CHAT_DB`.
2. Start `codex app-server --listen ws://127.0.0.1:4500`.
3. Resume existing conversations using `codex resume --remote ws://127.0.0.1:4500`.
4. Each main agent restores its own coordination identity, checks its inbox,
   and runs `agent-chat bind --thread "$CODEX_THREAD_ID"`.
5. Run `agent-chat-bridge --server ws://127.0.0.1:4500` with the same DB.

Keep the chat, bridge and Codex server running in their terminals. Ctrl+C stops
the bridge without interrupting a Codex turn or stopping the app server. Starting
the bridge before Codex is available is fine; it reports the connection problem
and retries. Bindings persist. Only one bridge can dispatch for a database; an
OS process lock protects local dispatch. Remote dispatch also uses a persistent
lease; it never expires or transfers merely because a client disconnects.

Bindings are opt-in and use exact identities. A conversation UUID can belong to
only one coordination session. `agent-chat unbind` removes the caller's route.
Outstanding jobs must be resolved before changing a route, to prevent a queued
wake from reaching an old conversation after a rebind.

Subagents retain separate sessions/tokens. Native subagents bind with
`agent-chat bind --parent-session EXACT_PARENT_SESSION --agent-path /root/child`.
Nested children can route through an already-bound parent chain. A wake tells
the root parent to use its native follow-up tool to resume the existing child;
it never reads or acknowledges the child's inbox under the parent identity.
If the underlying session no longer supports resuming that child, the parent
must report it instead of silently spawning a duplicate. Direct child binding
requires the child's distinct Codex UUID and server support for direct input.

## Dispatch rules

- Only unacknowledged messages from the exact persistent web operator ID are
  eligible. Agent-to-agent messages, replies and acknowledgements do not wake
  peers automatically. A newly bound agent may receive one wake for old messages
  it has not acknowledged yet.
- Messages for the same root thread are coalesced (up to 100 per wake). The wake
  contains routing metadata and message IDs; agents read actual message bodies
  from their inbox. Inbox reads, explicit acknowledgements, tokens and resource
  queues are never changed by the bridge.
- A thread must be loaded, idle, and have `canAcceptDirectInput == true`.
  Active/approval-waiting, unloaded and non-input child threads wait. The bridge
  never calls `thread/resume`, `turn/start`, `turn/steer`, or an approval API.
- Existing Codex queued input keeps its place. The bridge adds input with a
  durable `clientUserMessageId`, then calls `thread/queue/start` for its own item.
  That API admits only an idle thread atomically; a busy race leaves the queued
  item for later. The bridge never starts or deletes someone else's queue item.
- A successful wake is not repeated while its messages remain unacknowledged.
  If all its messages are acknowledged before dispatch, its own pending input
  is cancelled. New messages can trigger a later wake.

The WebSocket endpoint is restricted to `ws://127.0.0.1:PORT`,
`ws://localhost:PORT`, or `ws://[::1]:PORT`. The bridge does not alter model,
reasoning effort, credentials, sandbox or approval policy. It ignores notifications
and rejects server requests instead of approving tool actions. Approval decisions
remain in the existing Codex client. Local app-server access can control threads;
keep it on loopback and use only trusted local clients.

## Status and recovery

`agent-chat bridge-status` prints bindings, recent bridge heartbeat/error,
per-thread state/errors and recent dispatch jobs. An unbound recipient has no
route; a bound but unavailable conversation reports its last observed reason.
Job states:

| State | Meaning |
| --- | --- |
| prepared | Durable intent; not submitted yet. |
| adding / starting | An RPC may be in flight; restart reconciles before proceeding. |
| queued | This bridge's input is stored in Codex; waiting for admission. |
| dispatched | Codex accepted the wake turn, or matching input was found in history. |
| cancelled | Messages were acknowledged before dispatch; queued input removed if present. |
| uncertain | Connection/crash outcome cannot be proven; no blind retry. |
| failed | Codex definitely rejected enqueueing; inspect the error before retrying. |

After a lost response or process crash, the bridge checks the Codex queue and
recent user-message history for its stable client ID. A positive match recovers
the attempt. No match is **not** proof that no turn started: history can be
truncated or unavailable. The job remains uncertain and blocks additional wake
jobs for that thread. Messages still remain available through normal inbox checks.

Stop the bridge before manual recovery (the commands enforce its process lock).
Inspect the Codex conversation and its queue. Only after confirming the wake
never started and removing any leftover queued copy, run under a session bound
to that route:

```sh
agent-chat bridge-retry WAKE_JOB_ID --confirm-not-started
```

If the wake already ran but history is unavailable, verify its existing turn,
then run `agent-chat bridge-resolve WAKE_JOB_ID --confirm-delivered`. This records
your explicit confirmation without starting a turn or acknowledging messages.
Restart the bridge after resolving the job.
Do not assert a wake never ran when it already did.
No system can make a model turn and an unrelated SQLite write one atomic
transaction. The conservative recovery policy avoids accidental duplicate work.

## Protocol version

The bridge uses the experimental queue APIs from the locally generated Codex
0.154.0 schema. Initialization enables `experimentalApi`. Unsupported versions
report RPC errors; there is no fallback to a turn API with different admission
semantics. See the [official app-server documentation](https://learn.chatgpt.com/docs/app-server)
for transport and lifecycle details. Revalidate queue and thread capabilities
when upgrading Codex. Automated tests use a fake app server and never run models.

## Hosted coordinator

Follow the [remote setup and migration instructions](../README.md#remote-hosting).
Use `agent-chat-server --no-bridge` on the database host and one
`agent-chat-bridge-client` beside Codex on the agents' machine. The client uses
`AGENT_CHAT_SERVER` and the privately supplied `AGENT_CHAT_API_TOKEN`; it dispatches
through the same queue/reconciliation engine as local mode. HTTP response loss
therefore retains durable intent and the existing uncertain-outcome policy.

Remote wake metadata includes the server URL instead of the host's SQLite path.
Agents retain their API configuration separately. All main/child bindings and
messages remain in the hosted database. Only routes rooted in a session pinned
to the client's machine are eligible. This version supports one active Codex
execution host per project database; it does not multiplex multiple app servers.

A private persistent client identity and a local file lock prevent duplicate
workers. The server holds both a database lease and the local dispatch lock, so
local and remote bridges cannot overlap. A restart of the same client reclaims
its existing lease. Normal shutdown releases it; a lost connection during shutdown
leaves it reserved. The server never assumes an offline client has stopped.

For a lost identity, stop the previous client and verify its pending RPCs have
ended before `agent-chat-bridge-client --recover --confirm-stopped`. This explicit
operator confirmation only removes the dispatch lease. Job retry/resolution still
requires checking the Codex queue and conversation as described above. If a client
cannot release its lease because the server is offline, restore connectivity and
restart/stop that same client before manual job recovery.
