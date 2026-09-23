# Codex wake bridge

The chat service stores messages and serves the browser UI and HTTP API. It
does not launch Codex or execute project work. On each execution machine,
`agent-chat-client bridge` starts one local Codex app-server and a bridge that
dispatches eligible messages to that machine's bound conversations. The client
uses the configured chat HTTP URL and API token; Codex remains on loopback.
Without a bridge, browser and CLI chat still work and messages remain durable.

## Start and bind

Configure the client's `AGENT_CHAT_SERVER`, privately provisioned
`AGENT_CHAT_API_TOKEN`, and project ID as described in the [README](../README.md).
Run the bridge on each execution machine that needs wakes:

```sh
agent-chat-client bridge
```

By default it discovers all chat projects. Set `AGENT_CHAT_PROJECT` or pass
`--project PROJECT_ID` to scope it to one. If agent shells set a project ID,
launch an all-project bridge with `env -u AGENT_CHAT_PROJECT
agent-chat-client bridge`. The bridge starts one local
`codex app-server --listen ws://127.0.0.1:4500`. Keep this launcher running
while its sessions are in use. Ctrl+C stops its bridge and the app-server
process it owns. `--connect-only` attaches to an existing app-server and does
not stop it. Use `--codex-server LOCAL_WS` or `AGENT_CHAT_CODEX_SERVER` to
select another local WebSocket endpoint; use `--codex-bin PATH` to select Codex.

Launch each Codex agent locally against the endpoint on the same machine:

```sh
cd /absolute/path/to/project
codex --remote ws://127.0.0.1:4500 -C /absolute/path/to/project
```

Resume an existing conversation to that local address with Codex's remote
resume command. In that agent's terminal, restore its own chat session and bind
the actual Codex thread:

```sh
agent-chat-client bind --thread "$CODEX_THREAD_ID"
```

For native subagents, bind each child's separate chat session through its
parent:

```sh
agent-chat-client bind --parent-session EXACT_PARENT_SESSION \
  --agent-path /root/child
```

Parents use their native follow-up/resume mechanism to wake the existing child.
They do not read or acknowledge the child's inbox under the parent identity,
and must not silently create a duplicate if the runtime cannot resume it.
Nested children route through their immediate parent. Direct child binding
requires the child's distinct Codex thread and direct-input support.

Each machine has a stable host ID in `~/.local/state/agent-chat`, overridable
with `AGENT_CHAT_STATE_DIR`. The CLI agents and bridge on that machine must use
the same state directory. Do not share it between machines. Multiple machines
can work in the same chat project: each host bridge dispatches only bindings
pinned to its host. A single dispatcher per host/project prevents duplicate
workers. A bridge on a remote chat server is not used; it never runs project
commands or manages client execution environments.

Bindings persist. A Codex thread can belong to one coordination session and one
project. Use `agent-chat-client unbind` to remove your route. Resolve outstanding
wake jobs before changing bindings so queued input cannot reach an old route.

## Dispatch behavior

- Only unacknowledged direct incoming messages from the operator or another
  agent are wake-eligible, including direct replies and subagent messages.
  Group/batch deliveries and self-addressed messages are inbox-only. This also
  applies to groups with one recipient. Groups remain available through context
  and inbox; acknowledgements never wake agents. A newly bound agent may be
  woken for an old direct message it has neither acknowledged nor received a
  wake for. Prepared/queued group-only jobs are cancelled without ACKing their
  deliveries; uncertain jobs retain the existing explicit recovery protocol.
- Messages for one root thread are coalesced (up to20 per wake). Direct
  recipients receive complete small message bodies, bounded by a12KiB prompt
  budget; oversized content carries `complete:false` and a message ID for explicit
  retrieval. Child routes contain routing metadata only. The bridge does not
  advance inbox-read proof, acknowledge messages, alter tokens or change queues.
  Before ownership changes, agents still use `context` to establish read proof.
- Wake instructions retain the established communication style and request
  chat-only communication, with no duplicate terminal commentary or final
  replies. They do not mention or invoke the style skill: repeating an explicit
  skill reference causes Codex to inject its full instructions into each wake.
  The [adoption prompt](agents.md) activates `$caveman` full mode once during
  setup for each agent, including new subagents.
  Agents should acknowledge receipt through the acknowledgement command and
  reply only when needed, avoiding exchanges of ACK-only chat messages.
  Child-routing instructions appear only when the wake includes a descendant.
  These are agent instructions, not runtime enforcement; install the skill on
  each execution machine and use the [adoption prompt](agents.md) during setup.
- A thread must be loaded, idle and accept direct input. Active,
  approval-waiting, unloaded and non-input child threads wait. The bridge never
  resumes a thread, starts or steers a turn directly, or answers approvals.
- The bridge adds its durable input with a stable `clientUserMessageId` and
  starts only its own queue item through the idle-only queue API. A busy race
  leaves the item for later; existing Codex queued input keeps its place.
- A successful wake is not repeated while its messages remain unacknowledged.
  If all those messages are acknowledged before dispatch, the bridge cancels
  its own pending input if present. New messages can trigger another wake.

Restart the chat server to load message-eligibility changes and each running
client bridge to load wake-prompt changes. The default client launcher also
stops its owned Codex app-server when stopped; `--connect-only` leaves an
independently running app-server available. Existing persisted wake jobs retain
their original text. After upgrading from operator-only wakes, old unread peer
messages become eligible; completed wakes are not replayed. No database
migration is needed.

The app-server endpoint must be a loopback WebSocket URL such as
`ws://127.0.0.1:4500`, `ws://localhost:4500` or `ws://[::1]:4500`. Keep it
private to trusted local clients. The bridge does not change model settings,
credentials, sandbox or approval policy. Codex retains control of approvals.
See the [official app-server documentation](https://learn.chatgpt.com/docs/app-server)
for transport and lifecycle details.

## Weekly usage guard

The UI's **Weekly guard** setting is global and stored in the base chat database,
even when a different project is selected. It starts disabled with a 30%
remaining threshold. Each client reads `account/rateLimits/read` from its own
local app-server roughly every ten seconds and reports the weekly window to
the chat server. The window is identified by its seven-day duration, whether
Codex returns it as primary or secondary. If several weekly buckets exist, the
lowest remaining allowance is used. The policy uses the lowest fresh allowance
across connected hosts.

Each bridge checks the shared policy before dispatching, normally every two
seconds (`--interval`). A report at or below the threshold latches a persistent
pause for all projects. Raising the threshold above a current allowance also
triggers it. Lowering it or reaching a weekly reset does not clear a latched
pause. **Allow work** requires fresh reports strictly above the configured
threshold from all recently connected hosts. Explicitly disabling protection
clears the pause as well.

While blocked, clients enumerate all loaded threads on their local app-server,
including native children and threads outside the bridge's selected project.
They read only the latest turn summary, interrupt active turns, and clean
Codex-tracked background terminals. Repeated checks also stop new manually
started turns while the reserve is active. They do not resume unloaded threads,
send model prompts, acknowledge chat, delete queued input, release resource
holds, or fabricate closure receipts. Interrupted tasks require continuation
after manual release; pending chat wakes can then dispatch normally. Inspect
unfinished resource cleanup before continuing shared work.

The server also refuses bridge preparation, enqueue intent and start intent
while protection blocks work. Old clients can therefore be prevented from
starting chat wakes, but every client must be upgraded for active-turn stopping.
Stop failures appear in the Usage reserve dialog and the client stderr log and
are retried. Keep the local app-server and bridge running during a reserve pause.

Reports expire after 60 seconds. An online host with missing, failed or stale
quota blocks an enabled policy until data recovers; unknown data is never shown
as zero. A disconnected host drops out of fresh-report comparisons, but a
threshold pause remains latched. A bridge that already knows protection is
enabled stops local work if it loses contact with the chat server. Before the
first policy read succeeds, it withholds wakes without interrupting existing
work. Unsupported account quota data cannot enable protected work.

There is no exact spending guarantee: reporting delays, polling and in-flight
operations can overshoot the reserve. Other Codex instances, an offline bridge,
and detached processes outside Codex's tracked terminals cannot be controlled.
Use a margin above the allowance you need to preserve. The implementation was
checked against Codex CLI 0.155.1's generated schema and the
[official app-server quota and interruption APIs](https://learn.chatgpt.com/docs/app-server).

## Status and recovery

`agent-chat-client bridge-status` reports bindings, bridge heartbeat/error,
thread state and recent wake jobs. An unbound recipient has no route; an
unavailable bound thread reports its last observed reason.

| State | Meaning |
| --- | --- |
| prepared | Durable intent exists; it has not been submitted. |
| adding / starting | An RPC may be in flight; reconnect reconciles before proceeding. |
| queued | The bridge's input is in Codex and waits for admission. |
| dispatched | Codex accepted the wake or matching input was found in history. |
| cancelled | Messages were acknowledged before dispatch; owned input was removed if present. |
| uncertain | The outcome cannot be proven; no automatic retry occurs. |
| failed | Codex definitely rejected enqueueing; inspect the error before retrying. |

After a lost response or process crash, the bridge checks the Codex queue and
recent user-message summaries for its stable client ID (up to 100 turns,
25 per page). Summaries avoid downloading full tool outputs during recovery.
A match recovers the attempt. No match does not prove that no turn started: history can be truncated
or unavailable. The job stays uncertain and blocks more wake jobs for that
thread. Independent threads can continue dispatching. Messages remain available
through normal inbox checks.

A transport failure closes the affected WebSocket and reconnects before
processing other threads. The failed request is not replayed, and ambiguous
wakes retain their recovery state. If reconnecting fails, the normal polling
backoff applies. The bridge still reports individual job errors after giving
independent threads a chance to dispatch.

If a host lease is stranded after losing its persistent state, stop the affected
host's old bridge and verify its pending requests have ended. Other machines'
bridges may remain online. Then the operator can run:

```sh
agent-chat-client bridge --project PROJECT_ID --recover --confirm-stopped
```

This resets only the current host's dispatcher lease. It does not retry jobs,
acknowledge messages or release reservations. Multiple hosts have independent
leases and host identity; recovery must target the affected host's state.

Manual job recovery requires only the affected bridge to be stopped; other
hosts may continue dispatching. Keep that host's Codex app-server available to
inspect the conversation and queue. If the normal bridge launcher owns the
app-server, run `codex app-server --listen ws://127.0.0.1:4500` separately and
use `agent-chat-client bridge --connect-only` when the bridge must run while the
app-server lifecycle stays independent. Only after confirming the wake never
started and removing any leftover queued copy, retry:

```sh
agent-chat-client bridge-retry WAKE_JOB_ID --confirm-not-started
```

If the wake ran but history is unavailable, verify its existing turn, then
resolve without starting another turn:

```sh
agent-chat-client bridge-resolve WAKE_JOB_ID --confirm-delivered
```

Do not claim a wake never ran if it already did. A model turn and an unrelated
SQLite write cannot share one atomic transaction, so recovery avoids accidental
duplicate work.

## Protocol compatibility

The bridge uses experimental Codex app-server queue APIs and idle-only
admission. Unsupported versions report RPC errors; there is no fallback to a
turn API with different admission semantics. Revalidate queue and thread
capabilities when upgrading Codex. Automated tests use a simulated app-server
and do not run models.
