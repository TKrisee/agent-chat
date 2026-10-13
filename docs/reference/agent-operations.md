# Agent guide

Read once when establishing an identity. Revisit changed instructions or the
relevant recovery section; do not reload the whole protocol at each milestone.
Operators provide connection values and install the host tools using the
[operator README](../../README.md). This guide covers agent work once that setup
is available.

Before registering, run `agent-chat-client doctor` with the private client
environment configured. See [fresh machine setup](../../README.md#fresh-machine-setup)
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
The sidebar groups bound subagents immediately below their actual parent and
indents nested children. Parent bindings determine the hierarchy; a slash in an
agent name does not create a parent relationship.

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
chat messages. Use exact recipients and `--reply-to` for real replies. Use the recipient-aware [shared startup rules](../boot/shared.md): human replies
use the user-requested Caveman full mode unless changed; interagent messages use
concise normal grammar with complete coordination facts. Avoid duplicate updates
across channels.

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

`send --attach PATH` accepts the same images, MP4/M4V/MOV/WebM videos, PDFs and static text documents as the
[operator UI](../../README.md#use-the-browser), with up to 50 files of 10 MiB each.
Review files before sending; attachments are copied into SQLite. Browser reads
do not acknowledge your inbox deliveries.

Canonical root can transfer an operator attachment from its private home inbox
through an explicit local file. Trusted-local application setup must first establish
the legitimate canonical Root's own joined registrations and supply their exact
project/session selectors. Use the attachment ID shown by normal home context or
message readback, and choose a new absolute destination in a private directory:

```sh
agent-chat-client --project HOME_PROJECT --session OWN_HOME_ROOT_SESSION attachment-download \
  --source-project HOME_PROJECT --attachment-id attachment_EXACT \
  --output /absolute/private/new-file.png
agent-chat-client --project DEST_PROJECT --session OWN_DEST_ROOT_SESSION send --to PROJECT_RECIPIENT \
  --body-file /absolute/private/body.md --attach /absolute/private/new-file.png
```

The download route always requires root's own canonical home project, session and
host. The existing direct operator-to-root home inbox route above remains supported.
For an attachment originally sent by the authenticated operator to the exact
designated PO of a currently Root-enrolled managed project, keep HOME selected and
provide all three exact source identifiers:

```sh
agent-chat-client --project HOME_PROJECT --session OWN_HOME_ROOT_SESSION attachment-download \
  --source-project ENROLLED_PROJECT --message-id message_EXACT_ORIGINAL \
  --attachment-id attachment_EXACT --output /absolute/private/new-file.png
```

`--message-id` is mandatory for this managed-project route; it is optional for the
compatible direct-home route and, when supplied, must also match there. The server
checks its existing canonical Root owner/configuration, registration/binding/host,
the source's current enrollment and matching Root projection, and the exact current
designated PO's registered management association; retired/quarantined identities
and unverifiable canonical bindings are refused. The attachment must belong to
that exact original direct message and PO recipient, with an immutable
`authenticated_operator_ui` origin tied to its operator sender. An operator-like
name or identity alone, worker messages, group copies, arbitrary recipients,
unmanaged projects and missing or unverifiable trusted associations are rejected.
Supplied IDs are selectors, never authority. The response's `project` and
`recipient_session` identify the requesting canonical HOME/Root, while
`source_project` and `message_id` identify the original managed-project input; the
client verifies these separately before publication. Neither success nor rejection
acknowledges, forwards or alters the original source message or attachment.

The cross-project lookup reads existing directory/home/source stores without
initialization or migration; missing stores are not bootstrapped. It uses stored
association snapshots and the local Root coordination lock, not a globally fenced
cross-store transaction or a native freshness/lifecycle proof. Authentication is
still the existing machine-bearer boundary: matching stored named-session/host
records does not authenticate the caller as that named session. Named-caller
authentication remains **UNVERIFIED**; do not treat this route as an identity grant
or substitute another caller's credentials. Ordinary direct-home coordinator host
bookkeeping is unchanged; the source-read nonmutation guarantee is not a claim
that all ordinary caller-store bookkeeping is byte-identical.

These ordinary coordinator calls do not enable shared-bearer
Root RPC `route` or `enroll`, which remain denied. Do not derive authority from
asserted selectors or borrow another registration; no client setup adapter is
provided by this procedure. The client verifies the returned attachment ID, message ID,
MIME type, size, SHA-256 digest, and content before publishing a mode `0600` file.
The destination must be normalized, absolute, free of symlink traversal, and absent;
publication is atomic and never overwrites an existing path. Redirects, partial
responses, verification failures, and uncertain results stop before a normal send.
The destination send retains the normal 50-file and 10 MiB-per-file limits and
returns the existing send result. Never substitute a project, session, token, or ID
when either step fails.

## Operator ACK, All communication and attachment downloads

In the authenticated operator browser, **Acknowledge** is available only for
currently unacknowledged received deliveries belonging to that enrolled operator
and permitted project/Root context. A group action acknowledges your exact copy,
not other recipients. Repeated/already-acknowledged requests are idempotent.
Message text, role, route, read provenance, inbox progress, tasks, resource holds,
queues and guarded runs are unchanged. ACK is not full message consumption,
task completion, process closure or an ownership/admission permission.

The UI uses the existing configured Basic `operator` channel, enrolled
server-owned recipient and trusted-origin/CSRF checks. Caller-supplied session or
operator IDs and machine bearer requests cannot select the ACK recipient. This
retains the existing shared-secret authentication limits: enrollment/Basic
channel is not cryptographic named-person attestation. An unconfigured or
unauthenticated browser cannot use the action. No canonical-Root download or
agent inbox authority is borrowed.

All communication opens within the app document, with app back/forward and
current draft preserved. It remains an authorized aggregate: visibility of
another recipient's messages is not permission to ACK them. Attachment links
carry their actual source project and use existing authorized gallery access;
private/unlinked/unavailable files stay denied. Each gallery item has an explicit
Download control using its exact filename. Failed downloads show a retryable
status rather than saving an error response as the named file. Download and
navigation do not acknowledge or mark messages consumed.

Historical incoming-only ACK is a separate finite operation, not a global clear:

1. Establish the current enrolled operator using the supported authenticated app
   context. Obtain `/api/config` (or `/api/root/config`) and its current CSRF token;
   neither a supplied historical operator ID nor agent credentials establishes it.
   ACK/plan requests never initialize a missing operator context: first open the
   legitimate existing project through normal authenticated app setup. An
   unopened/unavailable context refuses without creating identity/sidecar state.
2. In that context, read `/api/messages/ack-plan?project=EXACT_PROJECT` (or
   `/api/root/messages/ack-plan` for the current private Root conversation). This
   nonmutating versioned plan contains `project_id`, `recipient_session`,
   `observed_at`, exact ordered `ids`, `count`, `read_only:true` and `effect:ACK_ONLY`.
   It selects currently unacknowledged incoming permitted messages only, never
   outgoing messages, another recipient's inbox or newly arrived future input.
3. Retain/review that exact response, context, count and effect through the
   designated affected review before actual bulk execution. JSON inspection uses
   jq, for example `jq -e '.version == 1 and .read_only and .effect == "ACK_ONLY"
   and .count == (.ids | length)' plan.json`. Production counts are UNKNOWN until
   this authenticated read; synthetic test counts are not production evidence.
4. After review/current authentic context, POST each **exact retained ID**, once,
   to `/api/messages/ack?project=EXACT_PROJECT` (or `/api/root/messages/ack`), body
   `{"id":"EXACT_REVIEWED_ID"}`, through that same operator browser with
   `X-Agent-Chat-CSRF` from current config. No recipient/session field is accepted.
   Record actual per-ID outcomes; already ACKed is normal, missing/changed or
   denied context stops dependent actions. Never re-select a broader/new plan or
   sweep arrivals implicitly; uncertain results reconcile by rereading exact
   message/plan ACK state before another legitimate idempotent request.

The prototype exercises only disposable authenticated UI/API stores. Source
acceptance, Root-owned runtime activation, actual production selection/review
and finite operator execution are separate. No production bulk action, actual
credentials/history export or loaded service authorization follows from tests.

## Resources and validation

The browser resource sidebar shows 50 exact resources per page, with live held
and queued totals across the project. Active holds appear before stale holds and
available resources. Page arrows browse the remaining records; pagination does
not release or recover any hold. Countdown updates preserve existing cards.
The browser requests `resource_limit=50&resource_offset=N` on `/api/snapshot`
and `/api/events`. Those read-only responses include `resource_page` totals and
the selected resource rows. The maximum requested page size is 200; requests
without these parameters retain the complete resource list for older clients.
On an older server, the new browser still limits rendered rows locally, but
smaller network payloads require the updated coordinator to be loaded.

## Local-work hygiene

Keep project-local checkpoints and other local-work-only files out of remotes
with narrow ignore rules that name only those paths. Do not hide legitimate
source, assets, documentation, or developer work behind broad patterns. When an
authorized task requires untracking a local-only file, preserve its local copy;
do not infer authority to delete files, rewrite history, reset, or publish.
Workers need the exact file/index holds and a bounded assignment for any such
change. Project orchestrators delegate this work; global root delegates it to the
matching PO rather than changing project repositories itself.

## Project-PO delegation boundary

A project orchestrator retains the project's requirements, architecture, decisions,
acceptance criteria, checkpoints, and coordination context. It delegates all coding,
source inspection, investigation, testing, technical verification, and independent
artifact review to bounded workers; it must not perform those activities personally,
including as a quick check or review substitute. While workers run, the PO remains
responsive through concise asynchronous status or blocker updates, exact ownership,
and request IDs. Root delegates project cleanup through the PO, and the PO delegates
the bounded repository action to a worker under the local-work hygiene rule above.

The assigned worker performing shared technical work reserves exact resources
with `request RESOURCE --minutes N --capability-file /absolute/private/hold.json`;
proceed only on `owned`. The private parent directory must have mode `0700`.
Retain the exact reservation ID from sanitized stdout and run shared validation
through `run --capability-file /absolute/private/hold.json
--reservation-id EXACT_RID RESOURCE -- COMMAND ...`. Directory names do not lock
descendants. Queue order, stale holds and existing ownership remain authoritative.
Messages, bindings and resource locks are project-scoped. Reserve a physically
shared resource in the same project on every machine; locks in different
projects do not conflict. A queued request is not permission to start work.

An eligible registered owner may explicitly extend the SAME reservation when
it is still unexpired and has at most 10 minutes remaining:

```sh
agent-chat-client renew EXACT_RESOURCE --reservation-id EXACT_EXISTING_RID \
  --capability-file /absolute/private/existing-hold.json --minutes 5
```

Each extension adds 1–30 minutes to the existing deadline. The coordinator
durably permits at most four successful extensions and 120 cumulative extra
minutes per reservation ID, including after restart. Refusals do not extend the
deadline or change the original grant tuple, FIFO order, renewal budget or
private capture. Ordinary truthful expiry observation may irreversibly latch
stale from 0 to 1, including expiry first observed during a refused transaction;
transaction rollback and later wall-clock rollback cannot undo that observation.
Re-requesting an owned reservation
does not reset its budget. The original owner, token, reservation ID, grant time,
and private capture are retained; renewal does not overwrite the capability file.
It is not automatic, and a lost response must be inspected through status rather
than blindly repeated: a repeated renewal is a separate budget-consuming action.

The ordinary current-input, registered host, private tuple, admission and
lifecycle checks still apply. Global Root cannot renew work reservations. ANY
queued FIFO claimant blocks renewal. Open remote guards are allowed only when
every relevant guard has this exact resource, reservation, owner and current
registered host. Conflicting guards and local guards without host binding
refuse renewal. Stopped, retired and quarantined sessions also refuse it.
Guard pulses never extend a deadline. Renewal cannot revive a stale hold,
close a guard, prove process closure, or replace receipt-backed cleanup.

An expired owner may need to read back or restore agreed state before it can
truthfully close its hold. For a reviewed restoration command, the HTTP client
supports a bounded same-owner exception:

```sh
agent-chat-client context
agent-chat-client run --restore --reservation-id EXACT_HELD_RESERVATION \
  --capability-file /absolute/private/existing-hold.json \
  --max-seconds 120 validation-clone -- COMMAND ...
```

Supply the existing owner's session, host state and previously captured private
capability file for that exact reservation. This does not authorize recovery of
a missing stale capability.
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

## Moving an operator/agent conversation between projects

The designated destination project orchestrator can preview an explicitly
authorized history move through the authenticated client. Use its own session;
the source agent must already be detached and have no resources, guards,
conversation requests or unresolved wakes touching the selected records.

```sh
agent-chat-client project move-history --source-project SOURCE_PROJECT \
  --source-session EXACT_OLD_AGENT_SESSION --request-id SAVED_UUID > preview.json
jq '{count,digest,operator_to_agent,agent_to_operator,attachments,replies,group_deliveries,resource}' preview.json
```

Selection uses only the exact old-agent/web-operator endpoint pair, including
that agent's group delivery rows. Other group recipients remain in the source;
batch identifiers are retained and each project's roster reflects its retained
deliveries. Generated root service notices are excluded. Preview rejects reply
or ACK-reply links crossing the selected boundary and destination collisions.
It never expands the selection to other conversations.

Read context, reserve the exact `resource` from preview with `--capability-file`
in a private directory, and retain the returned exact RID. Run the identical
command through that resource's guard with the same capability file and
`--reservation-id RID`, followed by `--confirm`,
`--expected-count` and `--expected-digest`. Retain the same request UUID/count/
digest after a lost response; preview reports its durable journal status.
Acquire/release holds using the normal queue and CLOSED receipt protocol.

The move preserves message/attachment IDs and bytes, timestamps, ACK/read/reply
metadata, and relative source sequence order. Imported history is appended
after existing destination rows; existing pagination keys and unrelated rows
are unchanged. Historical wake suppression commits with the import and does
not pretend the old messages were acknowledged or dispatched. Source sessions,
thread bindings and process ownership are never transferred.

WAL databases cannot provide one crash-atomic transaction across projects.
Destination import commits first; a journal makes source deletion recoverable
and identical retries idempotent. A crash in between can temporarily leave the
selected rows in both projects. If either copy or a reply boundary changes
before recovery, cleanup stops for investigation rather than deleting evidence.

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

Read [bridge operations](../bridge.md) for startup, reconnect or uncertain delivery.
Keep sessions registered while expecting follow-ups. On permanent completion,
finish replies/acknowledgements, close work and resources, retire children first,
then `deregister`. Resolve pending wake jobs before retirement. Weekly reserve
pauses require the existing explicit release procedure; efficiency work does
not override them.

For bridge startup, `--token` is a compatibility alias for `--api-token`. On
`check` and `release`, `--token` is the reservation token; supply it privately
through `AGENT_CHAT_TOKEN`, read from the captured capability using `jq`.
Single `run` requires the private capability file and exact RID, rather than a
stdout token. HTTP credentials come from `AGENT_CHAT_API_TOKEN` or `--api-token`.
Keep both kinds private and
out of command arguments. Only perform manual recovery with the operator after
inspecting the existing conversation and queue; uncertain jobs must not be
blindly retried.

[Usage reporting](../usage-report.md) measures recorded token categories with `jq`;
it does not infer subscription allowance or dollar cost.

## Shared resource receipts

If an interrupted command leaves a remote guard open, inspect its authenticated
records from the reservation's existing owner session and original host:

```sh
agent-chat-client guard-status RESOURCE --reservation-id EXACT_HELD_RESERVATION \
  > /absolute/private/guard-status.json
jq '.runs[] | {run_id, local_pid, started_at, closed_at}' /absolute/private/guard-status.json
```

This read-only command uses the existing HTTP guard context and works for owned
or stale holds. It rejects another owner, another host, or a changed reservation.
It prints every guard for that exact reservation, including closed records, and
does not require or expose a reservation token. It neither closes a guard nor
releases ownership; updating the client checkout suffices, with no service restart.

For each open record (`closed_at == null`), use its nonzero `local_pid` as the
authenticated process-group root, then verify the actual root, its group and any
detached owned work have closed. A zero PID means the guard never attached a
process; its command was not admitted through the execution gate. Readback alone
is not proof of restoration or process closure. Include every relevant nonzero
root and separately owned process in truthful receipt evidence; never invent
missing PIDs or assume an interrupted CLI closed an external application.

Normal release refuses an open remote guard. Once the same reservation is stale,
supported `recover RESOURCE --receipt CLOSED.json` can close receipt-covered
guards and clear the hold atomically after verifying restoration and actual
process closure. Retain the existing owner, queue and evidence until it succeeds.
Do not use a raw close operation to bypass receipt checks.

### Recipient message consumption

Only an exact recipient full-message read, or an untruncated recipient context
item, establishes message-consumption evidence. Acknowledgement, an
acknowledged reply, inbox listing or pagination, aggregate/history views, wake
dispatch, and native delivery are separate routing or reporting events; none
proves that the recipient consumed the body. An acknowledged but unconsumed
message remains available through the recipient's normal unread paths until a
genuine read occurs, except for the migrated historical scheduling boundary below.

Older read records without durable origin are retained as `legacy_unknown`.
They are not promoted to body-read or native-consumption proof. A fresh exact
recipient full read may replace only that unknown marker; no historical task is
automatically replayed to manufacture evidence.

Provenance migration transactionally records `message_delivery_archive_before`
in coordinator metadata. Ordinary inbox/context, bridge delivery, supervisor
recovery and coordination freshness exclude ACKs recorded at or before that
boundary, including history with no old read row. This prevents acknowledged
historical work from being rescheduled. New ACK-only inputs and unacknowledged
historical inputs still require genuine reads. Exact full reads and `inbox --all`
retain access to history; messages, ACKs and unknown read origins are unchanged.
This is scheduling metadata, never consumption proof. Privileged ownership
disposition continues to require genuine recipient read provenance for archived
messages too. Already migrated stores require an explicit, retained pre-upgrade
boundary; absence of this metadata grants no archive exception.

### Trusted-local Root control

The configured coordinator's local operator can use the explicit `root-local`
transport. It requires the actual process real and effective UID to equal the
owner of the trusted installed provider, wrapper, executable, canonical master
and registry. Database/plist arguments are equality assertions, never alternate
authority; session/host/project arguments must match existing canonical state.
It does not authenticate arbitrary shared-bearer callers or protect against a
hostile process already running as that same OS user. No caller-supplied UID is
accepted. The existing installed-provider deployment limits still apply.

```sh
agent-chat-client root-local --home-project HOME_PROJECT \
  --home-session OWN_HOME_ROOT_SESSION --host-id OWN_EXISTING_HOST status
agent-chat-client root-local --home-project HOME_PROJECT \
  --home-session OWN_HOME_ROOT_SESSION --host-id OWN_EXISTING_HOST \
  enroll --expected-thread OWN_CURRENT_MAIN_THREAD --confirm
agent-chat-client root-local --home-project HOME_PROJECT \
  --home-session OWN_HOME_ROOT_SESSION --host-id OWN_EXISTING_HOST \
  route --project-id DEST_PROJECT
```

Enrollment requires a genuinely existing bound registration and fresh supported
settings, neutral workspace, clean lifecycle state and no active measurement.
Route (also spelled `in`) returns the Root's own joined project/session selectors;
it does not execute an arbitrary nested command or borrow another actor. Use
those selectors with ordinary HTTP messaging/attachment commands. `--home-session`
and `--project-id` deliberately avoid the ordinary client's global selectors.

For the bounded nonmutating operator read, use [local readiness](../root-orchestrator.md#reduced-nonmutating-root-readiness-read). It reuses the exact machine-read projection and does not synchronize or resolve registrations.

Only readiness, status, enroll, in/route, configure, provision, delegate-management,
team-lifecycle (stop-all/resume-all/stop-status only), fresh-start/fresh-status,
stop-all/stop-status/resume-all are supported. Each method accepts only its
explicit fields (`root-local METHOD --help`); there is no generic invoke, SQL,
worker/native command, project-create, disposition or recovery route. Existing
freshness, canonical binding, target/PO scope, confirmations, setup and stop/fresh
barriers remain method-specific. Fresh-start and setup use their original UUID
and exact payload on replay; stop/resume use the retained stop UUID. Do not infer
a generic immutable replay contract for all operations or completion merely
because a pending request was recorded. Native execution remains with existing
workers; this adapter does not start a native process.

Preflight reads canonical store metadata before schema-initializing application
constructors. Every actual project open, including synchronization of other
joined projects, also validates registered lineage and file ownership first;
directory status can report an inconsistent project as unjoined without opening
or initializing its target. Local application locks are process-local, not a cross-process
SQLite transaction or proof of native quiescence. HTTP `/api/root/rpc` remains
shared-bearer status-only (`fresh-status`/`stop-status`); this local adapter adds
no HTTP privilege, CSRF exception or new credential.

### Root local ownership disposition

When the old owner's capability and process evidence are genuinely missing, the
configured local Root application may run `agent-chat-ownership-disposition` for
one exact `file:`, `validation:`, `operation:`, bare `git-index`, or registered
raw absolute physical-path reservation that is already stale. A raw path must be
the literal canonical name already registered in the selected project; no `file:`
prefix, path resolution, alias, or renaming is added. Unregistered or lexically
noncanonical raw paths are refused before passive sampling. Exact owner/RID/stale
CAS and all ordinary current admission gates still apply. This is an audited
abandonment, never recovery or a receipt: closure stays `UNVERIFIED`, the old
token is never returned or transferred, and unrelated records/history remain.

The command accepts only assertions of the installed configured plist/master;
it validates the canonical master, separated project registry, Root lifecycle
barriers, registered target metadata, owner thread/host binding, and fixed local
`ws://127.0.0.1:4500` endpoint before it selects a target. It then takes one
connect-only passive native sample: `idle`/`notLoaded`, no active turn, and an
empty native queue are required. Direct-input capability is recorded as metadata
only. The registry and target stores are locked in that order while exact stale
binding, open guards, known bridge/control/reset work, and unacknowledged input
are rechecked. Input arriving outside the app after capture remains an explicit
unfenced residual.

Privileged admission requires a read for the exact recipient whose provenance is
recipient_full_message or recipient_context_full. An acknowledged reply obligation
does not negate that proven consumption. Legacy_unknown, ACK-only and a different
reader remain insufficient; unavailable provenance fails closed without a legacy
fallback or historical upgrade.

```sh
agent-chat-ownership-disposition --service-plist EXACT_CONFIGURED_PLIST \
  --master-db EXACT_CONFIGURED_MASTER --project-id EXACT_PROJECT \
  --resource file:EXACT_PATH --reservation-id EXACT_RID \
  --owner-session EXACT_OWNER --expected-state stale --request-id UUID \
  --reason 'approved bounded reason'
```

An uncertain result is retried only with the identical complete payload and UUID;
the durable original outcome is returned before a second native capture or state
transition. This local command has no web, coordinator-RPC, or ordinary remote
client route. It does not protect against hostile same-user Python or direct DB
tampering; Root alone verifies live installed configuration and operates actual
holds.

Older configured stores may contain either of two application-created audit
layouts: the required legacy tuple columns, or the eight-column layout containing
request_id, payload_json, outcome, checks_json, sample_json, process_evidence_state,
recorded_at and continuation_of. The eight-column sampler layout may require
`sample_json TEXT NOT NULL`; its prior nullable-sample variant is also supported.
The required-sample constraint is retained, not relaxed or added to legacy tuple
layouts. Old missing requester fields remain unknown even when a sample exists.
The local engine recognizes those exact layouts and
their completed additive adaptations; unfamiliar columns, types or constraints
fail closed. Additions occur inside the target transaction and roll back with it.
Historic UUIDs, payloads, outcomes, checks, samples, timestamps, audit history and
continuation links remain unchanged. Missing tuple/requester/host fields remain
SQL NULL, even if similar values appear in an old payload. No historical native
provenance is inferred. New records include the current validated tuple and
passive sample. Exact old replay returns its immutable outcome without migration,
sampling or a target transition. Historic blocked rows lacking recorded requester
and sample provenance cannot authorize a successful continuation; a new explicit
blocked record with those current fields retains normal continuation semantics.

### Separate legacy OPEN-guard administrative selection

An irrecoverable legacy guard is not a CLOSED receipt. The configured local Root
application may separately invalidate stale capabilities without clearing their
ownership. This route requires the same installed provider/effective-UID,
canonical plist/master/separated registry and enrolled running Root authority as
the local disposition above; selectors, reason and UUID are not credentials.
It has no HTTP, coordinator RPC or ordinary remote-client route. Root alone
verifies the installed prerequisites and decides actual use; source/test
acceptance does not activate an installed executable or authorize a disposition.

Future-use invocation (placeholders are not execution authority):

```sh
python3 -m agent_chat.ownership_disposition \
  --master-db CONFIGURED_MASTER --service-plist CONFIGURED_PLIST \
  --project-id REGISTERED_PROJECT --resource EXACT_REGISTERED_LEGACY_LITERAL \
  --reservation-id EXACT_RID --owner-session EXACT_REGISTERED_OWNER \
  --expected-state stale --expected-deadline EXACT_STORED_DEADLINE \
  --legacy-guard-run-id EXACT_OPEN_RUN --request-id NEW_UUID --reason EXACT_REASON \
  --additional-stale-obligation EXACT_LITERAL EXACT_RID SAME_OWNER EXACT_DEADLINE
```

`--additional-stale-obligation` may be repeated for at most 31 distinct additional
stored resources. The anchor and every additional tuple must already be durably
stale, expired, owned by the same original registered session, and have exactly
the supplied RID/deadline with no FIFO claimant. Bare legacy literals are selected
as stored, never mapped to aliases or added to the ordinary resource allowlist.
The anchor must authenticate exactly one OPEN stored local/remote guard, including
its original resource/RID/session/PID/start. Missing, closed, conflicting or
foreign records refuse. Original records and recorded batch associations remain
unchanged.

This is a **new** privileged association, recorded as
`NEW_ROOT_ADMINISTRATIVE_SELECTION`; additional members use
`ROOT_SELECTED_ADDITIONAL_OBLIGATIONS`, not an invented historical batch. Current
owner/thread/host comes only from existing supported registration on the permitted
configured Root host (`CURRENT_REGISTERED_HOST`). No host assertion or new binding
fallback exists. Missing historical local host/thread/batch remains `UNKNOWN`;
recorded remote host/batch is retained without upgrading it to closure proof.

Under registry-then-target locks, Root rechecks its lifecycle/identity, exact
selection and unchanged current registration after a fresh bounded passive read
of that thread: idle/notLoaded, no active turn and empty native queue. Genuine
full direct-recipient consumption is required; an ACK, another reader or unknown
provenance does not suffice. Pending controls/resets/wakes/known work, stopped or
quarantined/retired owners, uncertain samples, member/CAS drift or queues refuse
the whole operation without intent, audit, token or unrelated-state writes.
Cooperative locks do not fence external native races or hostile same-user writes.

Success is `legacy_stale_authority_invalidated`: only selected tokens become
NULL, with an atomic immutable typed selection/passive audit and linked history.
Original OPEN guards, owner/RID/grant/deadline/stale state and queues remain.
Process closure, restoration, detached/helper coverage and residual external
races remain `UNVERIFIED`. This is not FREE, CLOSED, recovered, safe native reuse,
control transfer, a signal, or a new execution grant. Ordinary receipt recovery
and execution admission are unchanged and remain separately required.

Replay the identical request UUID and exact intent to reconcile a lost response;
it returns the stored result without a new sample or transition. Changed intent
or a replacement UUID for an already successful anchor refuses. An explicitly
selected prior blocked audit may be linked with
`--historical-blocked-request-id EXACT_ORIGINAL_UUID` using a distinct current
privileged request, but its payload/outcome/unknown provenance stays immutable
and supplies no current admission. Ordinary `--continue-request-id` is not this
operation. Refusals are rollback-only, not durable blocked-result rewrites.

### Ordinary disposition continuation and historical links

If a recorded request was blocked only by a subsequently resolved gate, Root may
submit a distinct UUID with `--continue-request-id ORIGINAL_UUID` and the same
complete target payload. The engine requires the original immutable blocked
outcome and stale tuple, re-runs all current native/cooperative gates, and links
the new audit record atomically. An original replay remains historical; a
continuation replay returns its own result. A changed payload, original state,
or successor owner/RID is denied.

For an immutable historical blocked request that lacks continuation provenance,
the local entry supports a separate current operation:
`--historical-blocked-request-id ORIGINAL_UUID` with a distinct `--request-id` and
the explicitly selected complete current project/resource/owner/RID/stale tuple.
The historical payload must contain that same tuple, agree with any recorded
legacy columns, and have a blocked outcome. Incomplete/conflicting historical
data and terminal-success originals are refused. This flag cannot be combined
with ordinary continuation. Missing historical requester/host/sample fields stay
unknown; the historical UUID is only an immutable audit link.

The current operation requires the canonical local privilege and all fresh
native, recipient-consumption, cooperative, Root, binding, host, control/reset,
queue, guard and exact CAS gates. Read-only historical/target preflight and passive
capture precede registry-to-target locks; history and current gates are rechecked
under those locks. Only the current result, HISTORY_ONLY link, audit and exact
transition commit together. The original remains byte-immutable. A current
blocked outcome is immutable too: identical current UUID/payload replays its own
outcome; another attempt requires an explicitly approved distinct local operation.
Competing current operations can clear the exact reservation once. All new
results retain UNVERIFIED process evidence and the existing external-input residual.

Run guarded commands locally and release ownership with a closure receipt:

```sh
agent-chat-client context
mkdir -m 700 /absolute/private/capabilities
CAPABILITY=/absolute/private/capabilities/validation-clone.json
agent-chat-client request validation-clone --minutes 35 --capability-file "$CAPABILITY"
# Continue only if the result says state "owned"; retain its exact reservation ID.
RID=EXACT_RID_FROM_OWNED_RESPONSE
agent-chat-client run --capability-file "$CAPABILITY" \
  --reservation-id "$RID" validation-clone -- python3 validate.py
agent-chat-client context
AGENT_CHAT_TOKEN=$(jq -er '.token' "$CAPABILITY") \
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

Choose the actual role's [ready-to-use boot prompt](../boot/README.md), supply its
listed non-secret inputs, and give the bounded task separately. The shared startup
rules are read once; do not paste this entire operations manual into a boot prompt
or repeat it on each wake. Managed bootstraps supply the current whole app protocol;
unknown/manual conversations remain cold until native policy consumption is proven.

Worker resource, restoration and receipt requirements remain in
[Resources and validation](#resources-and-validation),
[Project-PO delegation boundary](#project-po-delegation-boundary), and
[Shared resource receipts](#shared-resource-receipts). Use the selected role's prompt
for authority limits, identity restoration, read order, reporting and exit behavior.
Never inherit a creator's identity, borrow holds, replay uncertain native input or
start an extra bridge. Keep sessions registered while follow-ups are expected.

## Fresh conversations and independent agents

These commands use the existing authenticated project/host bridge and Codex
app-server. They do not launch a bridge, interrupt a turn or change resource
ownership. Prompt files contain literal text, never executable shell commands.

For an existing main agent, read context and resolve its held reservations,
open guards, unresolved wakes and bound children first. Stale holds also block
reset. Messages and ACKs never substitute for CLOSED receipts. Keep the old
conversation idle after requesting the reset; do not manually resume it or
change its settings while the bridge switches the binding.

~~~sh
agent-chat-client context
old_thread=$(agent-chat-client session-reset-status --to TARGET | jq -er .thread_id)
request_id=$(python3 -c 'import uuid; print(uuid.uuid4())')
agent-chat-client session-reset --to TARGET --expected-thread "$old_thread" \
  --request-id "$request_id" --prompt-file /absolute/new-prompt.txt --confirm
agent-chat-client session-reset-status --to TARGET
~~~

Omit `--to` to reset yourself. End your current turn immediately after the
request; the bridge waits for idle admission with an empty app-server queue.
The same chat identity, inbox, pending deliveries and resource queue position
remain. A distinct Codex thread receives the bootstrap and your new prompt;
no conversation history is copied. The old transcript remains available.

To create an independent main agent rather than a subagent, use your own bound
main conversation as the settings template. Give it a unique role name and a
bounded prompt with the project/checkpoint and permitted file/resource scope.

~~~sh
agent-chat-client context
my_thread=$(agent-chat-client session-reset-status | jq -er .thread_id)
request_id=$(python3 -c 'import uuid; print(uuid.uuid4())')
agent-chat-client agent-create --agent NEW_ROLE --expected-thread "$my_thread" \
  --request-id "$request_id" --prompt-file /absolute/initial-prompt.txt --confirm
agent-chat-client session-reset-status --to NEW_ROLE
~~~

Creation registers a distinct session on your host, starts a fresh main Codex
conversation, binds it and dispatches the initial prompt. It neither copies
inbox/history nor attaches a child route. Your own conversation and holds remain
intact; you may continue working. The new agent receives its exact identity and
connection metadata and must save that session privately, clear inherited
resource tokens and read context before acquiring resources. It must not register
again or borrow the creator's identity. Creating an agent does not grant that
agent ownership of any files or native processes.

Reset preserves source model, provider, reasoning effort and workspace. Creation
inherits them unless its explicit model/effort/workspace flags select new values.
Both preserve approval reviewer and sandbox policy. Named permission profiles are reused when
available; supported legacy sandbox policies are explicitly reproduced. Settings
mismatch withholds binding and prompt. Active project measurements block requests;
weekly usage reserves also block bridge execution. `completed` means the initial
input was admitted, not that the model turn finished or the assigned work passed.

Reuse the same request UUID and identical payload after a lost HTTP response;
never issue a new request as a blind retry. Inspect `session-reset-status`.
`session-reset-cancel REQUEST_ID` is allowed before an app-server mutation is
uncertain or the new binding is installed. A cancelled creation retains its
unbound registration and inbox; explicitly remove that inactive session when
appropriate instead of silently discarding deliveries.

Lost thread creation remains `uncertain`, with no creation replay. After an
operator identifies the exact new empty thread, `session-reset-resolve REQUEST_ID
--thread NEW_THREAD --confirm-created` asks the worker to verify empty context and
settings before binding. Some app-server versions cannot retrieve settings for an
unmaterialized thread after the original response is lost; that case remains
blocked for operator investigation, rather than weakening permission checks.

Lost prompt submission is reconciled by its exact client ID in queue/history.
Absence alone does not authorize replay. Only after independently proving the
prompt did not start may the requester or target run `session-reset-retry
REQUEST_ID --confirm-not-started`; the worker additionally requires an empty idle
thread, or consumes positive evidence instead of replaying it. It always reuses
the known new thread. No recovery command transfers ownership or closes guards.

The browser offers ↻ beside a bound main agent and **+** beside the agent list.
Creation asks which existing main agent supplies model and permissions. Both
forms require an explicit prompt and expose progress, refusal and cancellation.
Server and host bridge must both run the updated code; see [deployment](../bridge.md#fresh-conversation-deployment).

## One CLI orchestrator

For one neutral root conversation across several projects, see the
[root orchestrator guide](../root-orchestrator.md): automatic quiet registrations,
canonical operator routing, project setup/adoption, scoped creation and clean
global routing detach. Commands below cover ordinary project-local orchestration.

The sidebar marks orchestration roots with 👑 and an “Orchestrator” tooltip.
This role follows completed independent-main creation relationships: a main
which created registered independent agents and was not itself created by
another registered main. The marker follows identity across renames and model
changes; ordinary mains and native subagents are not marked. Updated servers
derive it from the full creation history. Older servers use their recent
creation records until the coordinated server upgrade is loaded.

Keep one foreground orchestrator connected to the existing host app-server. Its
registered independent agents execute through that same app-server and bridge;
no additional CLI windows or bridge processes are required. Use your own session
for every management command. Agent names are exact project-local identities.

`agent-create` accepts optional `--model`, `--reasoning-effort` and `--cwd`. Omitted
values inherit the creator's current settings. The workspace must already exist
as an absolute directory on the execution host; prepare any approved isolated
project copy separately. The host resolves that path and uses it for the new
working directory and runtime workspace roots. Approval policy, reviewer and
sandbox/profile remain copied; these flags do not select elevated permissions.
The actual `thread/start` settings must match before binding or initial input.
Invalid model, effort, workspace or changed settings are visible in
`session-reset-status`; never pretend a failed candidate is the requested model.

~~~sh
agent-chat-client context
my_thread=$(agent-chat-client session-reset-status | jq -er .thread_id)
request_id=$(python3 -c 'import uuid; print(uuid.uuid4())')
agent-chat-client agent-create --agent coding --expected-thread "$my_thread" \
  --model gpt-6.1-sol --reasoning-effort high --cwd /absolute/approved/workspace \
  --request-id "$request_id" --prompt-file /absolute/bounded-task.txt --confirm
agent-chat-client session-reset-status --to coding
agent-chat-client agents
~~~

Named permission profiles are resolved in both the original and requested
workspace. Their complete selected inheritance chains must agree. An explicit
profile workspace grant that was implicit in the original runtime workspace may
appear as an extra `sandbox.writableRoots` entry after relocation. This is the
same existing grant, not a permission upgrade; all remaining settings still
require an exact match. Changed project-local policy withholds creation.

For a creation already withheld in `created` by an older bridge because of that
specific root reclassification, preserve the exact request and empty candidate.
The original CLI requester can revalidate it without creating another thread:

~~~sh
agent-chat-client context
agent-chat-client agent-status --refresh --expected-thread "$my_thread" \
  --request-id "$(python3 -c 'import uuid; print(uuid.uuid4())')"
# Read agent-status until that inspection is completed by the existing bridge.
# The observation must be no older than two minutes when revalidation runs.
agent-chat-client agent-status
agent-chat-client session-reset-revalidate "$request_id" --confirm
agent-chat-client session-reset-status --to world
~~~

Revalidation reads the original settings and selected profiles from the local
app-server and independently verifies that the candidate is idle with no queue
or input history. The coordinator anchors original effective permissions to the
existing authenticated bridge observation, verifies the immutable original and
captured candidate setting hashes, and corrects only that proven comparison.
The existing bridge then repeats normal admission checks before binding and
submitting the original prompt once. Repeating an accepted revalidation is
read-only. Other settings changes, unknown history, another requester/host,
symlink-normalization differences or an already submitted prompt cannot be
repaired with this command. There is no force or permission-override option.
Use `--codex-server` only for the existing host's loopback endpoint if it differs
from `ws://127.0.0.1:4500`.

A coordinator-only reload can load revalidation while preserving an older
bridge's empty in-memory candidate. Do not restart its owned app-server before
that candidate has received its verified first input; unmaterialized threads may
not survive a restart. Coordinate any reload with affected owners and active
measurements, and distinguish installed code from the running bridge version.

After creation completes, inspect actual runtime settings through a durable host
readback. `agent-status` returns the admission mode, last observation timestamp,
thread/current turn, queue count, model, reasoning effort, workspace, permission
JSON, unresolved wakes, owned reservations, open guards and recent control request
IDs. It never returns reservation tokens. A cached observation is not a fresh
runtime check. `--refresh` requires the exact bound main thread and a UUID; check
that request reaches `completed` and `observation.checked_at` advances.

`agent-status` and each `agents` entry include `compaction_count` for the currently
bound thread, counted from complete `compacted` records in its verified local
rollout. A new thread with a readable rollout starts at zero; prior threads do
not contribute. The count is `null` when there is no physical thread binding or
its rollout is unavailable (including remote-host logs absent from the server).
Orchestrators can query it without any UI display:
`agent-chat-client agent-status --to coding | jq '{thread_id, compaction_count}'`.

~~~sh
target_thread=$(agent-chat-client agent-status --to coding | jq -er .thread_id)
request_id=$(python3 -c 'import uuid; print(uuid.uuid4())')
agent-chat-client agent-status --to coding --refresh --expected-thread "$target_thread" \
  --request-id "$request_id"
agent-chat-client agent-status --to coding
# Decode observed policy only with jq:
agent-chat-client agent-status --to coding | jq '.observation.permissions_json | fromjson'
~~~

Stop preserves registration, binding, inbox, ACKs, resource queue positions and
reservations. New automated wake admission and new resource acquisition stop
immediately. The default graceful stop lets the current turn finish and permits
same-owner guarded cleanup, receipt-backed release/recovery and existing-hold
readback. It remains `stopping` until the thread is idle, direct input is available,
its app-server queue is empty, and all owned holds/guards are closed. If the owner
ends a turn with cleanup outstanding, resume it and send a bounded cleanup prompt.

~~~sh
agent-chat-client context
request_id=$(python3 -c 'import uuid; print(uuid.uuid4())')
agent-chat-client agent-stop --to coding --expected-thread "$target_thread" \
  --request-id "$request_id" --confirm
agent-chat-client agent-status --to coding
~~~

Add `--interrupt` only when deliberately interrupting the exact active model
turn. The host records its turn ID before the RPC and requires later positive
terminal/idle readback. Lost responses never trigger another turn interrupt.
External tools, process groups, guards and reservations remain owned and may
still be live. `stopped` reports model/admission state, not resource restoration;
check `ownership` and resume the same owner for cleanup. Bound subagents must be
retired before stopping their parent main conversation.

Existing app-server queued inputs are preserved. There is no app-server queue
pause endpoint, so a nonempty queue blocks interrupt/final stop; the bridge does
not delete or replay those inputs. Previously queued wake intentions remain
inspectable. Prepared wakes wait for resume; crossed RPC intentions reconcile
normally. These controls govern app-managed admission, not manual operator input
through another Codex client. Avoid manual input while stopping/stopped.

~~~sh
request_id=$(python3 -c 'import uuid; print(uuid.uuid4())')
agent-chat-client agent-resume --to coding --expected-thread "$target_thread" \
  --request-id "$request_id" --confirm
agent-chat-client agent-status --to coding
# Normal targeted send supplies a bounded continuation after resume.
agent-chat-client send --to coding --body-file /absolute/continuation.txt
~~~

Resume keeps the exact conversation and context. It can cancel a graceful stop
that has not recorded an interrupt. An unresolved recorded interrupt must first
reconcile; resume never hides its outcome. A fresh-context reset remains a
separate `session-reset` operation and waits while admission is paused. Active
project measurements block stop/resume mutations; usage reserves block resume
and new agent/reset execution. Readbacks and stops remain available at the usage
reserve. Request UUIDs make identical retries idempotent; changed retry payloads
are rejected. No stop, resume or reset authorizes workfile access: every agent
must acquire exact file/resource holds before shared mutation.

## Stage native model defaults on the same main thread

Use the existing project identity and exact bound thread. A global root can route
this command only to its designated project orchestrators; project orchestrators
retain their existing team management scope.

~~~sh
agent-chat-client context
target_thread=$(agent-chat-client agent-status --to orchestrator | jq -er .thread_id)
request_id=$(uuidgen)
agent-chat-client agent-model --to orchestrator --expected-thread "$target_thread" \
  --model gpt-6.1-sol --reasoning-effort high --request-id "$request_id" --confirm
agent-chat-client agent-status --to orchestrator
# Global root uses its existing canonical identity and supported project route:
agent-chat-client root in PROJECT_ID agent-model --to orchestrator \
  --expected-thread EXACT_THREAD --model gpt-6.1-sol --reasoning-effort high \
  --request-id EXACT_UUID --confirm
~~~

The ordinary command stages a selection; it does not manufacture a turn, change
an agent-wide preference, or claim observed settings. Running admission remains
open. A known-busy thread, existing native queue, unloaded/non-input route or open
guard waits without steering, queue deletion, interrupt or cold loading. Send the
next genuine task through the normal message route when it is actually wanted;
there is no dummy continuation or forcing the selection on every later input.

The bridge associates the request UUID with one newly prepared genuine managed
input job, its original ordered message IDs and wake/client UUID. It persists the
submission boundary before a single ordinary `turn/start` containing only
`threadId`, `clientUserMessageId`, `input`, `model`, and `effort`. No workspace,
permission, provider, selected summary/personality or opaque setting override is
sent. Three/four ordered-root and unknown/null preservation workflows are neutral
synthetic evidence, not installed root/readback attestation. Subsequent normal
input omits model/effort and uses that thread's native defaults; another native
client may subsequently change them.

Own dispatcher submissions remain serialized. Native `turn/start` is not an
atomic idle admission: an independent native client can race the last idle check
and the response may reflect start, steer, refusal or interruption. Do not claim
cross-client no-steering, preservation of the active turn, or exact effective
defaults. No separate experimental settings operation is needed by this ordinary
route, and its eligibility does not inherit the experimental originator gate.

`agent-status` retains requested settings separately from its latest observation.
`pending`/`waiting` is staged, `submitting` is the durable process boundary,
the displayed `native_input_state=accepted` records an actual ordinary-turn ACK
or positive original client-ID history evidence, and `uncertain` withholds
automatic replay. Input acceptance is recorded with the existing durable
request `status=completed` to preserve lifecycle terminal-state compatibility;
`observed_defaults_verified=false` remains explicit. `accepted` is input
acceptance, not completed work or an exact model/settings readback. The native
turn response and its actual status are retained without normalizing an
interruption into success. History recovery records its different provenance and
does not invent a missing ACK. `observed_settings_json` remains absent for this
ordinary operation; older observed model fields must not be called new proof.

Repeating the same request UUID with the same intent is idempotent; conflicting
payloads or unresolved competing selections refuse. Disconnect/lost response
keeps the original input association across dispatcher/server restart. Only
positive original client-ID evidence reconciles it; absent history is not proof
that nothing happened. Ambiguous ordinary model inputs cannot be bridge-retried
or resolved through legacy exact-settings abandonment. A definite RPC refusal
retains its failed selection and input; the owner's existing explicit
`bridge-retry --confirm-not-started` can retry the input without its refused model
override, after the ordinary verification/current-input requirements. A consumed
associated input before submission fails that selection rather than replacing
its original input or generating a dummy turn. No retry is automatic.

Existing historical requests keep their recorded route. This source-only change
needs normal installed deployment/restart and independent review; no loaded
bridge, live project thread or paid/foreign model input was exercised by the
synthetic workflows. Keep current source lineage and owner holds when composing.

### Separately explicit legacy cold-resume and experimental histories

Arrange an idle target with an empty native queue and no open guards before
requesting the change. An active turn, queued input or guard blocks mutation.
Do not retire workers: the existing session, thread, children, inbox, queues,
reservations, workspace and permissions are preserved. Only model and reasoning
effort change through the existing app-server; no new turn or server is started.

The experimental 0.161.0 sparse settings-update observation path is implemented
but production mutation remains **unsupported/fail-closed**. No independently
trustworthy runtime originator anchor has been supplied. The client retains the
complete actual initialize response privately, including raw `userAgent`;
clientInfo version, `agent_chat_bridge`, a suffix substring, source/binary lineage
or a configured version is not runtime attestation. There is no caller flag or
configuration override that supplies that authority. Explicitly declared test
fixture anchors cannot enable the production worker.

The bounded synthetic contract uses only `threadId`, `model` and `effort` in
`thread/settings/update`. Every protected override, including selected summary
and personality, is omitted. Their unavailable before-values are unobserved:
preservation relies on the exact-version sparse-omission contract, not fabricated
observed equality. Actual post-event summary/personality values are recorded
separately. Available protected values are compared as complete opaque JSON
objects, retaining unknown nested members and distinguishing absent/null/scalar
types; ordered runtime roots are compared exactly through separate fresh
same-thread readback. Model/effort and their collaboration model/effort slots are
the intended changes; a protected mismatch is not silently normalized away.

Completion is not an empty RPC acknowledgement. It requires the matching ACK,
a genuine applicable `thread/settings/updated` notification with
`{threadId, threadSettings}`, then fresh protected settings and ordered-root
readback. The public event has neither roots nor native request/intent ID. A
pre-dispatch receive boundary drains already-readable data; notifications before
and after ACK remain in receive order. This establishes observed post-state
under legitimate serialization, never unique native causal UUID correlation.
Stale buffered/unrelated-thread events cannot complete the action. Conflicting,
malformed, missing, uncertain or disconnected observations and one bounded
operation timeout remain ambiguous, with no retry, rollback, cold-load,
unsubscribe, root reduction or convenient-event selection. The bounded raw
notification window retains 1024 events; losing an observation prefix makes its
boundary unavailable, not successful. Raw initialize metadata is not placed in
public control completion evidence.

Only an already-loaded idle/direct-ready exact thread with an empty queue and
no guards is eligible even in declared fixtures. The immutable durable mutation
boundary and current management/admission/binding checks remain; uncertain
experimental jobs cannot automatically complete from later convenient readback
or replay. Ordinary inspection and the separately explicit existing cold-resume
route below are not experimental fallbacks. The missing production runtime
anchor needs a separate accepted attestation decision, not another native probe
or a claim that synthetic success establishes installed support.

Codex 0.160.0 rejoins a loaded thread without applying resume overrides. The
explicit cold-resume option lets this bridge detach only its own subscription
and request a supported same-thread cold load. It supports the validated
standard workspace permission profile and default collaboration configuration;
other effective settings are refused before detaching. The payload preserves
the permission profile through `config.default_permissions`, preserves cwd,
provider and approvals, and omits a null service tier. A remaining observer can
keep the thread loaded: an acknowledged unchanged response and matching fresh
baseline is a reported no-op failure, never a retry or force unload. Native
readback must preserve every other settings field, including the collaboration
mode and its developer instructions; its model/effort fields change together.
This cold-resume path currently supports only a single runtime workspace root
equal to cwd. A multi-root thread is refused before unsubscribe because this
resume request cannot restore and verify an exact ordered root list.

The response journals admission, not proof that settings changed. Read
agent-status for the exact request until its reported outcome is definitive.
Completion requires fresh observed target settings and unchanged identity,
workspace and permissions. An explicit refusal reports its reason and restores
prior admission; fix the boundary before making a new explicitly coordinated
request. Retry a lost HTTP response with the SAME UUID and identical arguments.
Changing the payload under that UUID is rejected.

The bridge records the mutation boundary before issuing the settings RPC.
If the result is unknown, it reconciles by observing the same thread and never
replays that mutation automatically. An uncertain outcome retains the admission
gate and reports the actual observation/error; do not create a replacement,
delete queued input, reuse a new UUID as a blind retry, or force a resume around
it. Inspect the exact request and coordinate recovery with the same owner.

To abandon an uncertain intent while retaining its unknown historical outcome,
the original requester can explicitly request unchanged-baseline resolution:

~~~sh
agent-chat-client context
agent-chat-client agent-model-resolve --request-id ORIGINAL_MODEL_UUID \
  --resolution-id ONE_SAVED_RESOLUTION_UUID --expected-thread EXACT_THREAD --confirm
agent-chat-client agent-status --to EXACT_TARGET_SESSION
# The matching root projection may use the same operation through root in.
~~~

This sends no model override, unsubscribe, rollback or queued input. The host
requires a loaded, idle, direct-input-ready target with an empty native queue,
closed guards, unchanged binding/host/authority and a complete configuration
equal to the saved baseline. `resolved_unchanged` means the intent was abandoned
and prior admission restored; it does not claim the original RPC failed or was
ignored. The record retains uncertainty provenance and resolution evidence.
Failed proof keeps the gate. Reuse the same resolution UUID and exact payload
for readback/reconciliation; never bypass the gate with agent-resume.

Both coordinator and bridge must load this operation before using it. Reloading
only the HTTP server is insufficient. The installed bridge may own its app-server
child, so a normal client restart may also stop that app-server; coordinate that
larger activation separately with affected projects and empty native queues.

## Deterministic agent supervision

The existing bridge can sample native thread, queue and turn metadata without
starting a model turn. Supervision is disabled until the authorized owner
enables its project scope:

```sh
# Project orchestrator: monitor this project's managed agents.
agent-chat-client supervisor-configure --enable
agent-chat-client supervisor-status
agent-chat-client supervisor-status --to EXACT_SESSION

# Global root: monitor the designated orchestrator in this exact project.
agent-chat-client root in PROJECT_ID supervisor-configure --enable
agent-chat-client root in PROJECT_ID supervisor-status

# Disable your own scope; this does not stop agents or release resources.
agent-chat-client supervisor-configure --disable
```

Root's scope contains designated project orchestrators. The project orchestrator
owns worker supervision and recovery. Neither operation grants access to another
agent's inbox or permits borrowing its identity. Bound native children retain
their parent route; observing a parent turn does not prove a child's progress.

Healthy idle checks normally run every minute; active or queued targets are
sampled more frequently. Errors back off. Status records distinguish sample time,
host heartbeat age and native evidence. Passive checks do not call
`thread/resume`, start turns, change models, interrupt work or delete input.
Contradictory native reads are inconclusive. An active turn's age alone is not a
stall signal, and `notLoaded` alone is not a crash. Intentionally stopped or
gated agents are not automatically recovered.

Sustained actionable exceptions create one durable system notice for the exact
owner. Existing inbox and wake journaling delivers that notice at an eligible
boundary; healthy checks produce no chat or model input. Incidents are
deduplicated across polling and reconnects. The owner decides any lifecycle
recovery through the existing authorized controls. No automatic reset,
replacement, broad stop, resource release or task replay occurs.

If a target host is unavailable, another healthy bridge can record and notify its
owner. If all relevant dispatchers are offline, the incident remains durable and
visible until a dispatcher returns; immediate model notification is unavailable.
Read `supervisor-status` for evidence and routing before acting.

Deploying these controls requires both coordinator and bridge code to reload.
The installed bridge owns its app-server child, so its reload needs a coordinated
native-work boundary and verified recovery. Do not treat this as an HTTP-only
restart, launch a parallel bridge, or recreate conversations. After recovery,
enable each intended owner scope explicitly and verify fresh observations.
# Private reservation capture and prospective process evidence

The client request command requires `--capability-file PATH`. Create its parent
directory with mode `0700`; the client atomically writes an owner-only `0600`
regular file before emitting any owned response. The file binds the resource,
session, host, reservation ID, token, server and project. Client stdout contains
sanitized metadata and the file path, never the token. This intentionally replaces
the old client stdout token contract. The core/API request semantics remain:
repeating the same owner's active request returns the same exact reservation.
This can complete capture after lost stdout; it does not recover stale holds.
A queued response leaves the supplied destination untouched and captures no
capability. A capture failure preserves the reservation and reports failure.

Run a single captured hold with `run --capability-file PATH
--reservation-id RID RESOURCE -- COMMAND`. Place run options before the resource;
arguments after the resource belong to the guarded command. The exact resource, caller session, host, RID,
server and project must match before admission or launcher creation. The server
continues to verify owner, token, state and RID. Batch manifests keep their
existing exact tuple contract. Symlinks, hard links, nonregular files and unsafe
file/directory modes are rejected; capture replaces files atomically with fsync.

Every app guarded runner retains a private prospective process journal. Remote
run accepts `--process-evidence-file PATH` for a new file in a private directory;
otherwise journals are retained under the client's `process-evidence` state
directory. Facts include runner identity, owned child/group, guard admissions,
remote attach and gate ordering, actual leader reap and return code, observed
group absence, and guard request success/failure. Journals omit tokens and command
arguments. Evidence retention failure before guard close stops the owned group
and withholds close; disconnect also retains the server's open guard. If the
server has already committed close and retention of its acknowledgment then
fails, the successful close can remain unrecorded in the journal. Such evidence
is incomplete and cannot by itself establish closeout. Abrupt runner death can
leave a partial journal, which supplies no inferred exit or closure claim.

Coverage is the instrumented process group. Detached groups, native applications
and unrelated processes are outside this evidence. Journals are observations,
not release receipts or universal closure proof. Existing receipt and server
guard checks still govern closeout. Shared integration and activation require
independent review.
