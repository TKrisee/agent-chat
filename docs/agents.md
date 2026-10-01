# Agent guide

Read once when establishing an identity. Revisit changed instructions or the
relevant recovery section; do not reload the whole protocol at each milestone.
Operators provide connection values and install the host tools using the
[operator README](../README.md). This guide covers agent work once that setup
is available.

Before registering, run `agent-chat-client doctor` with the private client
environment configured. See [fresh machine setup](../README.md#fresh-machine-setup)
for Python, `jq`, PATH and optional Codex requirements. The operator must also
provide the project's own tools and permissions on this host.

## Identity and connection

Use the operator-provided executable path and connection values in each terminal
context. For a checkout, the environment looks like this:

```sh
export PATH="/absolute/path/to/agent-chat/bin:$PATH"
export AGENT_CHAT_SERVER=http://127.0.0.1:8765
export AGENT_CHAT_PROJECT=default
export AGENT_CHAT_ROOT=/absolute/path/to/your-project
```

For an installed package, keep its virtual environment's `bin` directory on
`PATH`. Inherit `AGENT_CHAT_API_TOKEN` privately from the launcher; do not copy
it into the example or agent instructions. Restore your saved `AGENT_CHAT_SESSION`
on continuation.

Unset `AGENT_CHAT_DB` and legacy coordination variables. Never print credentials,
put them in command arguments or messages, or fall back to a local database.

A new agent clears inherited session/resource tokens and registers once:

```sh
unset AGENT_CHAT_SESSION AGENT_CHAT_TOKEN
registration=$(agent-chat-client register --agent UNIQUE_ROLE)
export AGENT_CHAT_SESSION=$(printf '%s\n' "$registration" | jq -er .session)
```

Choose a short, unique role or task name for each agent and subagent, such as
`gameplay` or `admission-review`. Model identifiers and reasoning levels are
shown separately in the sidebar and are not required in names.

Retain this ID across terminal calls. Restore it on continuation; never use a
parent's identity. Every child registers separately. Keep the host state
directory consistent with its bridge; do not share it between machines.

To change your display name, keep your existing `AGENT_CHAT_SESSION` and run:

```sh
agent-chat-client rename --agent gameplay
```

Rename preserves your session ID, thread binding, reservations, and tokens.
The new name must not belong to another registered session.

## Routine work

```sh
agent-chat-client context
agent-chat-client status --mine
agent-chat-client status --resource validation-clone
agent-chat-client bridge-status
agent-chat-client message MESSAGE_ID
agent-chat-client send --to RECIPIENT --body-file /absolute/reply.md \
  --reply-to ORIGINAL_MESSAGE_ID --ack-reply
```

`bridge-status` defaults to this session’s binding, its resolved thread and up to
10 unresolved jobs on that thread; `jobs_has_more` signals additional unresolved
jobs. Use `bridge-status --all` only for cross-session diagnostics or completed
job history, and select the needed fields before returning it to the model.

`context` returns at most 20 messages and 12 KiB by default, plus owned/queued
resources. Follow `cursor` with `--cursor` and `resources_cursor` with `--resource-cursor`
while the matching `has_more` flag is true. Fetch incomplete messages before
acting on their contents. `body_truncated`/`metadata_truncated` marks incomplete
message content;
`detail_truncated` resources need `status --resource NAME`. `status --mine`
plus explicit resource names intersects those selections. Filter a wake with
repeated `--message-id`; select resources with repeated `--resource`. Only the
full message command intentionally
returns an unrestricted body. Reading is not acknowledgement or ownership.

Check at task entry, before shared mutations and at meaningful work boundaries.
Reuse complete messages delivered in wakes or guarded-run output; do not fetch
them again merely to read them. `context` before ownership changes establishes
the server's inbox freshness proof. New messages arriving afterward still require
a fresh check. Do not acknowledge previews or omitted content.

An already-running guarded command continues when new messages arrive. Its
pulses still verify the reservation owner, token and expiry; messages are
delivered by the next inbox poll without automatic acknowledgement. Starting a
new run, explicit `check`, and ownership changes still require inbox freshness.

Use `--ack-reply` only after consuming the source message. Identical retries
return the original reply; changing an already-sent reply's contents is rejected.
For messages needing no reply, use `acknowledge MESSAGE_ID`; never send ACK-only
chat messages. Use exact recipients and `--reply-to` for real replies. Keep the
established communication style and avoid duplicating updates across channels.

Batch independent reads in a single tool call. Set output limits explicitly:
normally 1,500 tokens per command and 4,000 total per batched call. Expand only
specific sections needed for correctness; keep full logs on disk. If truncated,
narrow the query. Select exact files, symbols and JSON fields before printing;
never dump full geometry arrays or unrelated inventories. Instructions already
present in the prompt need not be reread unless changed. At takeover read the
current checkpoint and next action; open historical evidence only as needed.
Do not create model turns solely to poll. When idle, finish the turn and rely on
the existing bridge; active guarded runs already forward new messages.

## Direct and group messages

Use one direct recipient when the message needs that agent's attention:

```sh
agent-chat-client send --to gameplay --body-file /absolute/request.md
```

Send a shared update once instead of looping over recipients:

```sh
agent-chat-client send --to gameplay --to slices --body-file /absolute/update.md
agent-chat-client send --body-file /absolute/project-update.md
```

Repeated `--to` addresses the selected unique recipients in one message. Omitting
`--to` snapshots every other registered session in this project, including the
operator, at send time. It excludes the sender. The UI likewise treats an
unaddressed new message as group information. One or several explicit @agent tags
request attention from those agents. Empty or invalid explicit recipients fail atomically.

**Only explicitly addressed agents wake.** Untagged group information stays in
each recipient's inbox/context until their next check, even with only one member.
Explicit `--to` recipients (one or several) and UI @tags request wakes only for
those agents. Both kinds appear in the shared chat; the UI labels quiet updates
`Group · info`. Text inside the body is not parsed for wake targets by the CLI.
A group has one shared `batch_id` and one UI card, with separate delivery IDs and
ACK state. Consume and acknowledge your own delivery ID; never another member's.
Bounded context includes `batch_id` without the full group roster.

Direct replies retain `--reply-to ID --ack-reply`. Group sends do not support
`--ack-reply`; acknowledge a consumed source separately when appropriate. A reply
requires explicit recipients so a private reply cannot accidentally broadcast.
Earlier standalone messages are not merged based on matching text.
Historical groups retain their quiet delivery policy; upgrading does not replay
old group messages as new wake requests.

`send --attach PATH` accepts the same images and static text documents as the
[operator UI](../README.md#use-the-browser), with up to 50 files of 10 MiB each.
Review files before sending; attachments are copied into SQLite. Browser reads
do not acknowledge your inbox deliveries.

## Resources and validation

Reserve exact resources with `request RESOURCE --minutes N`; proceed only on
`owned`. Export its session-bound token as `AGENT_CHAT_TOKEN`, then run shared
validation through `run RESOURCE -- COMMAND ...`. Directory names do not lock
descendants. Queue order, stale holds and existing ownership remain authoritative.
Messages, bindings and resource locks are project-scoped. Reserve a physically
shared resource in the same project on every machine; locks in different
projects do not conflict. A queued request is not permission to start work.

An expired owner may need to read back or restore agreed state before it can
truthfully close its hold. For a reviewed restoration command, the HTTP client
supports a bounded same-owner exception:

```sh
agent-chat-client context
agent-chat-client run --restore --reservation-id EXACT_HELD_RESERVATION \
  --max-seconds 120 validation-clone -- COMMAND ...
```

Supply the existing owner's session, host state and private `AGENT_CHAT_TOKEN`.
The exact reservation must be stale. The server records restoration mode and a
separate deadline on the new run; attach and pulse recheck owner, token, host,
reservation identity and deadline. The default budget is 120 seconds; allowed
budgets are 1–900 seconds. Close existing guarded runs with truthful evidence
before starting a restoration run. Never mark a live or uncertain run closed.
The command may perform readback, restore agreed state or close owned processes;
it must not run feature work, tests, imports or builds. The CLI cannot infer that
purpose from an arbitrary shell command, so review its scope first.

The hold remains stale with its original deadline, token and queue. Ordinary
request/check/run still reject stale ownership. Timeout or connection loss stops
the client-owned process group and leaves the hold for receipt-backed recovery.
It does not cancel work already dispatched into an external Editor or prove its
terminal state. Successful restoration execution is not a CLOSED receipt. Verify
actual restoration and include the new run's PID in the eventual closure proof.
When a command touches multiple held resources, guard each exact hold separately
(nested restoration commands may cover distinct resources). For nested guards,
give the outer guard more time than the inner budget
and its cleanup, so it cannot kill an inner client before that client closes its
own process group. Reviewed wrappers which create separate groups must still
close those groups themselves; timeout is not evidence that detached work ended.
This mode is remote
only; local `ValidationGuard` has no stale exception. Reload the existing chat
server to load server checks, coordinating active guards, measurements and wakes;
the bridge and its app-server do not need restarting for this feature.

Before release, restore agreed state, close owned processes and retain a
reservation-bound CLOSED receipt. Expiry never transfers ownership. Read
[receipt details](#shared-resource-receipts) when closing or recovering
a hold. Never reset shared state or bypass a hold. Subagents cannot approve,
commit, push or mutate issues/PRs; parents review their work and final checks.

## Binding and recovery

Use the operator's single local bridge/app-server; never launch another bridge.
Before binding, restore or register your own coordination session. A main
conversation binds its actual thread:

```sh
agent-chat-client bind --thread "$CODEX_THREAD_ID"
```

Each native child binds its own session through the exact parent and path:

```sh
agent-chat-client bind --parent-session EXACT_PARENT_SESSION --agent-path /root/child
```

Main conversations run/resume through its local endpoint and bind their actual
`CODEX_THREAD_ID`. Children bind with their exact `--parent-session` and canonical
`--agent-path`, never an inherited parent thread ID. Parent wakes carry routing
metadata only; resume the existing child, do not read its inbox or replace it.

Read [bridge operations](bridge.md) for startup, reconnect or uncertain delivery.
Keep sessions registered while expecting follow-ups. On permanent completion,
finish replies/acknowledgements, close work and resources, retire children first,
then `deregister`. Resolve pending wake jobs before retirement. Weekly reserve
pauses require the existing explicit release procedure; efficiency work does
not override them.

For bridge startup, `--token` is a compatibility alias for `--api-token`. On
`check`, `run` and `release`, `--token` is the reservation token; HTTP credentials
come from `AGENT_CHAT_API_TOKEN` or `--api-token`. Keep both kinds private and
out of command arguments. Only perform manual recovery with the operator after
inspecting the existing conversation and queue; uncertain jobs must not be
blindly retried.

[Usage reporting](usage-report.md) measures recorded token categories with `jq`;
it does not infer subscription allowance or dollar cost.

## Shared resource receipts


Run guarded commands locally and release ownership with a closure receipt:

```sh
agent-chat-client context
agent-chat-client request validation-clone --minutes 35
# Continue only if the result says state "owned"; save its reservation ID/token.
export AGENT_CHAT_TOKEN=TOKEN_FROM_OWNED_RESPONSE
agent-chat-client run validation-clone -- python3 validate.py
agent-chat-client context
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

If a PID has since been reused, release/recovery accepts it when the owning host
can read an OS process start time strictly later than its attested closure. A
version 1 receipt uses the resource's `closed_at` for each PID. When a process
closed earlier but resource restoration finished after its PID was reused, use
version 2: keep the same resource, reservation ID, full PID list and truthful
resource `closed_at`, and add `process_closures` for PIDs with historical proof:

```json
"process_closures": [
  {
    "pid": 12345,
    "closed_at": 1789818000,
    "evidence": "/absolute/path/to/original-process-closure.txt",
    "evidence_sha256": "SHA256_OF_ORIGINAL_PROCESS_CLOSURE_REPORT"
  }
]
```

Set `version` to `2` in the copied receipt. Each entry must name a unique PID
already in `pids`, have a positive closure time no later than resource closure,
and bind a nonempty evidence file using its lowercase SHA-256 digest. Relative
evidence paths resolve beside the receipt. The client verifies each report and
uploads it; the server verifies its digest and records the complete receipt.
Historical process closure may predate the reservation, but cannot attest closure
of a guarded run started afterward. Keep the original receipts and reports.

A live original process, an unreadable start time, or an ambiguous start within
the same second still blocks recovery. Open guarded process groups must also be
gone; already recorded closed runs are not rechecked against unrelated processes
that subsequently reuse their PIDs. Version 2 requires an updated client and
server, with no database migration or reset.

## Adoption prompt

Give each new main agent this prompt once with the real connection values.
Pass this guide's location and a bounded task to children; do not paste the
full protocol repeatedly. Read the guide again only when changed or recovering.

---

Use agent-chat at `TOOL_CHECKOUT` for project `PROJECT_ID` (existing project:
`default`), server `CHAT_URL`, project root `PROJECT_ROOT`. Read
`TOOL_CHECKOUT/docs/agents.md` once and follow [this guide](#identity-and-connection)
for identity, compact context, explicit replies,
resource ownership and shutdown. Credentials come privately from the launcher.

Resource holding is mandatory for shared work. Workfiles stay on the shared
filesystem; read the actual files there. Use chat for scope, ownership coordination,
paths, results and evidence links. Do not pass workfile contents through chat as
a replacement for shared-file ownership. Messages, ACKs and an agent's agreement
never grant a resource hold.

Before creating, editing or deleting any shared file, read `context` and request
`file:<repo-relative-path>` for every exact affected path. Also reserve the
project's shared validation/build resources before using them, and `git-index`
before staging or committing. Proceed only when each required request returns
`owned`; `queued`, `blocked` and `stale` mean stop that work. Directory resources
do not cover descendants. Use your own matching project/session and each hold's
private token; never borrow another agent's reservation.

Execute shared file changes and validation through
`agent-chat-client run RESOURCE -- COMMAND ...` under the matching held resource.
If you lack ownership, queue and continue unrelated work or report the blocker;
do not negotiate a chat-only handoff or send file payloads to bypass the hold.
Afterward, restore agreed state, close owned processes, and release each hold
with its truthful reservation-bound CLOSED receipt. Share file paths and results
in chat after the guarded work, keeping files on the shared filesystem.

Use concise chat updates, questions and results. Do not duplicate these in
terminal commentary or final replies. If the operator supplies a style skill,
apply it once during setup; no personal skill is required. Automatic wakes
retain the established style without invoking a skill again.

Each agent restores its own session or registers a distinct new identity. Each
child clears inherited session/resource tokens and binds through its exact
parent session and canonical agent path. Never impersonate another agent.
Use short, unique role or task names for agents and subagents. Model identifiers
and reasoning levels are shown separately in the sidebar and are not required
in names. To rename yourself, keep your existing session and run
`agent-chat-client rename --agent NEW_NAME`.
Parents provide the bounded scope, exclusions and acceptance criteria, then
review results and own final verification. Subagents must not approve, commit,
push, or mutate issues/PRs.

Read [bridge operations](bridge.md) only for startup or recovery, and
[receipt details](#shared-resource-receipts) before closing a hold.
Do not start an extra bridge, reset shared state, bypass a hold, or infer release
from expiry. Idle agents end their turn; active guarded runs forward new messages.
