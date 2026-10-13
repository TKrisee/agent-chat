# One root across projects

This is the operator/API reference. For model startup use the [root boot prompt](boot/root.md)
and [shared startup rules](boot/shared.md), not this whole manual. Keep literal
project briefs with the project PO; [role prompts](boot/README.md) distinguish
root, PO, bounded worker and independent reviewer.

Run one foreground Codex conversation from a dedicated neutral directory. It
coordinates independent project orchestrators, each with its own thread,
workspace, instructions, checkpoints and workers. The browser's **Root
orchestrator** screen at `/root` shows only your direct canonical-home conversation and a
compact project overview. Direct operator/root messages stay in Root chat and are
excluded from project feeds, history, search, message detail and live updates.
Root-to-team coordination and shared project batches remain in project rooms.
Automatic project-registration notices are labeled **Service notice**, including
quoted previews; their original routing and acknowledgement records are retained.
Project messages and attachments remain in their rooms.
Use **Open project** to enter a project room; drafts remain separate between views.
The left project selector shows a blank choice while Root chat is active. Select
any project there to open its room, including root's home project.
Every message in Root chat is addressed directly to root automatically. No
`@mention` is required; typed `@names` remain literal message text and do not
select recipients. Project rooms retain their normal mention/group behavior.

Root keeps project/orchestrator routing and relays bounded operator questions.
Initial assignments and detailed technical reasoning stay with project orchestrators.
This reduces requirements and
assumptions leaking between projects; it cannot guarantee that a model never
makes a cross-project assumption. Explicit project IDs and matching registrations
guard operations; project agents still review scope against local context.

## Start and enroll

Use the existing bridge/app-server. Do not launch another bridge. It must cover
all projects; a bridge restricted with `--project` does not discover others.
`root status` distinguishes registration/setup from missing/unhealthy host
coverage and support for scoped creation. Coordinate service changes with the
operator, accounting for active measurements, guards and pending wakes.

Prepare a dedicated neutral directory outside project workspaces and run the
one CLI there against the existing remote endpoint:

```sh
codex --remote ws://127.0.0.1:4500 -C /absolute/neutral-root
```

Register and bind a distinct root using the normal [agent guide](agents.md).
Save its home project/session privately. Never inherit a peer's identity.
Inside that conversation, inspect its actual settings, then enroll:

```sh
agent-chat-client context
agent-chat-client agent-status --refresh --expected-thread "$CODEX_THREAD_ID" \
  --request-id "$(python3 -c 'import uuid; print(uuid.uuid4())')"
# Once the bridge inspection completes; observation must be <2 minutes old:
agent-chat-client agent-status
agent-chat-client root enroll --expected-thread "$CODEX_THREAD_ID" --confirm
agent-chat-client root status
```

Enrollment requires a clean running main: no holds, resource queue entries,
open guards, bound native children, pending creations or unresolved wakes.
It retains the home identity and binds distinct, host-pinned root registrations
in every existing project to the same actual thread. Project creation adds
another root registration and sends one durable home setup notice. It dispatches
no model input until workspace and task scope are provided. Identical enrollment
retries and partial joins retain stable IDs rather than duplicate registrations.

Root projections receive explicitly addressed reports, but are excluded from
unaddressed group broadcasts. Existing deliveries stay unchanged. Root is
control-only and cannot acquire resource holds. Project agents acquire their own
exact file/native holds and retain truthful receipt-backed closure.

## Configure or retain a team

Root retains one stable registered identity with project-neutral standing rules;
a supported fresh-start can replace its native conversation. Project assignments arrive
later through project creation or operator messages; they are not root boot prompts.
In browser **New project**, supply a name, ordered canonical workspace paths
(one absolute directory per line) and project instructions. The first path is
the primary working directory. CLI creation accepts the same workspace option
more than once:

```sh
agent-chat-client project create --name NAME \
  --workspace /absolute/canonical/primary \
  --workspace /absolute/canonical/related \
  --prompt-file /absolute/project-assignment.txt
```

For existing projects, **Edit workspace groups** saves one or more project
assignments atomically. The command-line form accepts every exact member of each
group in one batch. For example, transition the legacy INLST4 and LMX3 group
together; replace the environment-variable placeholders with their exact
project IDs and canonical member paths on the execution host:

```sh
agent-chat-client project configure-workspaces-batch \
  --project-workspace "$INLST4_PROJECT_ID=$INLST4_WORKSPACE_1" \
  --project-workspace "$INLST4_PROJECT_ID=$INLST4_WORKSPACE_2" \
  --project-workspace "$LMX3_PROJECT_ID=$LMX3_WORKSPACE_1" \
  --project-workspace "$LMX3_PROJECT_ID=$LMX3_WORKSPACE_2"
```

List only the exact directories each team needs. Do not grant an enclosing
Herd or other shared parent directory. The batch is rejected as a whole if any
member overlaps another member, the neutral root, or another configured project.
An existing broad legacy assignment can be replaced only after its active or
pending project orchestration has been retired and reconciled.

Creation persists the literal assignment, joins root and sends one compact setup
notice. It does not start a native conversation before root acts. When the scope
is complete, root consumes that notice, saves a request UUID and provisions the
project orchestrator without another operator confirmation round trip:

```sh
agent-chat-client root provision --project-id EXACT_PROJECT_ID \
  --model gpt-6.1-sol --reasoning-effort high --request-id SAVED_UUID --confirm
```

Provisioning forwards the stored assignment to the independent project conversation
with project-orchestrator guidance. Root notices/directory contain only readiness
and workspace metadata, not the full assignment. Existing settings checks, scoped
permissions, measurement guards and receipt/queue guarantees remain enforced.
Retry only the saved UUID; reconcile uncertain setup through `root retry-setup`.
Root must retain an already configured/adopted team rather than creating duplicates.

Root delegates project repository hygiene to the designated PO. The PO may assign
a bounded worker to add narrow ignore rules for local checkpoints or other
local-work-only files, while preserving legitimate source, assets, documentation,
and local copies when untracking. Neither root nor the PO may infer authority for
blanket deletion, history rewriting, reset operations, or publication.

The PO retains project requirements, architecture, decisions, acceptance criteria,
checkpoints, and coordination context. It delegates all coding, source inspection,
investigation, testing, technical verification, and independent artifact review to
bounded workers; it must not perform those activities personally, including as a
quick check or review substitute. While work is delegated, it remains responsive
through concise asynchronous status or blocker updates, exact ownership, and request
IDs. Root routes and receives bounded reports; it does not replace the PO or its
workers for technical work.

Before creating a new project orchestrator, the developer or other authorized
workspace owner may install the portable PO baseline in the project workspace:

```sh
agent-chat-project-guidance --workspace /path/to/project-workspace --install
```

The installer creates `AGENTS.md` only when absent. If developers already have a
customized copy, it preserves that file and stages the shipped baseline at
`.agent-chat/project-orchestrator-AGENTS.baseline.md` for deliberate manual
merge. To refresh that staged baseline after an agent-chat upgrade, run the same
command with `--update`; it still never overwrites `AGENTS.md`. This is optional
workspace onboarding, not an action performed by `root provision`, and does not
alter the root's neutral-workspace baseline or project assignment.

Name-only creation remains supported. Missing workspace/task yields incomplete scope;
root asks for only those missing details instead of guessing directories or copying
another project's instructions. Root does not prepare or edit project files: an
approved project worker/operator does. For an assignment supplied directly in chat,
root creates/selects the project and configures the exact saved workspace group
before provisioning. The `--cwd` example below is the compatible single-workspace
form; multi-workspace setup uses every saved group member.

Use each workspace's canonical absolute path on the execution host. Resolve
symlinks there; members must not overlap each other, another project's members,
or root's neutral workspace. Save UUID and literal prompt file privately before
submission:

```sh
agent-chat-client context
agent-chat-client root configure --project-id EXACT_PROJECT_ID \
  --agent orchestrator --cwd /absolute/canonical/primary-workspace \
  --model gpt-6.1-sol --reasoning-effort high \
  --request-id SAVED_UUID --prompt-file /absolute/bounded-prompt.txt --confirm
agent-chat-client root status
agent-chat-client root in EXACT_PROJECT_ID session-reset-status --to orchestrator
```

Creation uses the existing durable independent-main workflow: exact settings,
empty-thread admission, lost-response reconciliation and no uncertain replay.
Bootstrap specifies exact project and root registration. `completed` means first
input admitted, not that the task finished or passed acceptance.

Root-created agents use built-in `:workspace`, the exact ordered project
workspace list as runtime roots, and no extra writable roots. The first member
is the primary working directory. Normal system temporary-directory access remains enabled;
network defaults off. Model/provider and approval policy/reviewer remain
root templates; explicit model/effort select project settings. Named global
profiles and grants to other projects are not copied. Read-only or unsupported
root policies refuse writable creation. Returned settings must match before
binding/input. Saving a workspace group changes the project assignment metadata;
it does not change a running orchestrator's permissions. On the root overview,
compare the current list with the saved list and explicitly apply it to an
existing orchestrator. This requests a fresh conversation for the same project
identity; the current assignment remains in effect until the exact new settings
are read back successfully. The overview reports pending or failed updates so
the same request can be reconciled. A completed verified update becomes current when the
root overview refreshes; refreshing reconciles the durable assignment and project display,
but does not change native permissions or start another thread. Root creates only project orchestrators through `root configure`. Browser/direct
creation using a root template is refused; use the project orchestrator as the
worker template.
Additional project tooling/permissions must be approved separately, never granted
silently. Adopted teams retain their already-authorized policies.

To retain an existing team, inspect and explicitly adopt its existing main:

```sh
agent-chat-client root in EXACT_PROJECT_ID context
agent-chat-client root in EXACT_PROJECT_ID agent-status --to EXISTING_MAIN \
  --refresh --expected-thread EXACT_THREAD --request-id INSPECTION_UUID
agent-chat-client root in EXACT_PROJECT_ID agent-status --to EXISTING_MAIN
agent-chat-client root adopt --project-id EXACT_PROJECT_ID --to EXISTING_MAIN \
  --expected-thread EXACT_THREAD --cwd /absolute/observed/workspace --confirm
```

Adoption requires a recent observation of the exact main/workspace on the same
host. It changes no binding, history, permissions, workers or holds, and dispatches
no input. Resolve any pending creation first. Adoption retires nobody; send the
existing owner its bounded orchestration scope through normal chat when needed.

## Explicit project routing

Use root's canonical home project/session for every `root` command. `root in`
resolves only its own distinct registration in the named project. Outputs carry
that project ID. Failed routing never falls back to another project's database.

```sh
agent-chat-client root in EXACT_PROJECT_ID context
agent-chat-client root in EXACT_PROJECT_ID agents
agent-chat-client root in EXACT_PROJECT_ID send --to orchestrator \
  --body-file /absolute/request.txt
agent-chat-client root in EXACT_PROJECT_ID message DELIVERY_ID
agent-chat-client root in EXACT_PROJECT_ID send --to orchestrator \
  --body-file /absolute/decision.txt --reply-to DELIVERY_ID --ack-reply
```

The wrapper supports status, stop/resume, reset, reconciliation and clean
registration removal for designated project orchestrators. Read-only main
inspection also supports adoption. Ordinary per-agent root controls cannot directly
create, stop, reset or retire an individual worker; project orchestrators manage
those lifecycles within their own project. The explicitly authorized dedicated
root stop-all operation is the project-wide exception, covering worker and other
agent identities in root-enrolled projects without retiring them. Existing
ownership, measurement, usage, expected-thread and receipt
checks remain. Root cannot bind native children to itself
or locally reset, stop, remove or rebind a projection shared across projects.

Human replies use the canonical home conversation. Project replies/ACKs use the
matching project and exact delivery IDs. Approvals, task state, checkpoints and
locks remain project-local. Physically shared tools require the same coordination
project: locks across projects do not conflict. Root never transfers ownership.

The existing all-project bridge admits root wakes against the same actual idle
thread and queue. Each report retains originating project metadata and its own
ACK state. No queue deletion or replay is introduced. Project orchestrators may
continue already-authorized bounded work while root is unavailable; their own
checkpoints and recovery remain authoritative.

## Recovery and lifecycle

`root status` reports original request UUIDs, setup/errors/uncertainty and matching
host bridge coverage. It repairs registration routing without starting model
conversations on a read. If setup was journaled but no project creation request
exists, it reports `awaiting_original_request_retry`:

```sh
agent-chat-client context
agent-chat-client root in EXACT_PROJECT_ID context
agent-chat-client root retry-setup --project-id EXACT_PROJECT_ID \
  --request-id ORIGINAL_UUID --confirm
```

This uses the stored literal prompt/settings and original UUID. It never replays
an uncertain creation. For an existing request, use normal project-scoped
`session-reset-status`, cancel/resolve/retry. Correcting a failed setup with a
new payload requires resolving/cancelling the old request and explicitly retiring
its clean target registration, then configuring with a new UUID. No live main
is silently replaced.

Before resetting/rebinding root itself, consume/ACK every project's deliveries,
resolve its pending creation/wake/control requests, refresh runtime status and
avoid manual input from another Codex client:

```sh
agent-chat-client context
agent-chat-client root in EXACT_PROJECT_ID context
# Consume full messages and ACK/reply in every listed project.
agent-chat-client root detach --confirm
```

Detach pauses root wake admission first. Failed checks leave root paused,
registered and its pending input intact. Inspect the error, or use
`root resume-routing --confirm`. Clean detach removes non-home bindings and the
global role, retaining chat registrations/messages and the home binding. It
stops no model/tool process and touches no team ownership. Normal home lifecycle
commands can then change its conversation, followed by fresh enrollment. A
persisted detach phase can finish without creating a replacement conversation.

## Root adoption prompt

Use [the ready-to-use root boot prompt](boot/root.md) with its supplied neutral
workspace, own canonical identity/binding, routing checkpoint and literal operator
instruction. It distinguishes authorized initial enrollment from continuation or
fresh native context. Read [root standing instructions](root-AGENTS.md) once;
refer to this manual only for the operation currently needed.

Ordinary root per-agent lifecycle targets designated POs. The supported explicit
[bulk stop exception](#explicit-operator-stop-all-and-resume) covers all agent
identities in root-enrolled projects without retiring them. A
[fresh root conversation](#fresh-root-conversation-with-a-boot-prompt) keeps stable
root identity/project links and existing team stop barriers; it is not detach or
team resume. POs use [their own role prompt](boot/project-orchestrator.md), and
supply bounded [worker](boot/worker.md)/[reviewer](boot/reviewer.md) assignments.

Timed project measurements exclude global root projections: one shared root
thread cannot be charged independently to several projects or paused by one
project's measurement. Project orchestrators and workers remain measured; the
global weekly usage guard still applies to root. Directory/team designations
survive clean detach and re-enrollment of the same home identity. Retained root
registrations remain quiet even during that refresh.
Use [root-AGENTS.md](root-AGENTS.md) as reusable standing guidance in the neutral
workspace's `AGENTS.md`. Keep per-project assignments in project creation or setup
messages. Create that file before launching the new root conversation.

Install the versioned baseline without overwriting an existing developer copy:

```sh
agent-chat-root-guidance --workspace /path/to/neutral-root-workspace --install
```

If `AGENTS.md` already exists, the command preserves it and writes the shipped
baseline once to `.agent-chat/root-AGENTS.baseline.md` for a deliberate manual merge.

## Explicit operator stop-all and resume

An authenticated operator UI message carries server-stamped
`instruction_origin=authenticated_operator_ui` through message/inbox/context and
native wake delivery. Supported orchestration has the same user authorization as
an explicit CLI request. Root uses that authorization without asking the operator
to repeat it. Ordinary agent messages, names and text claiming to be the operator
cannot supply the marker. Current browser and machine access share credentials;
this identifies the protected operator route, not cryptographic isolation between
people and processes possessing those credentials. System/tool approval remains
in force, and a refused action must never be disguised or bypassed.

Use the canonical root home project/session/host and one retained UUID:

```sh
agent-chat-client root stop-all --request-id UUID --confirm
agent-chat-client root stop-status --request-id UUID
agent-chat-client root resume-all --request-id SAME_UUID --confirm
```

The bulk stop intentionally covers all agent identities in the root's enrolled
project stores, including manual/orphan/unbound candidates. It excludes the root
and human operator and leaves unenrolled projects alone. It preserves identities,
bindings, queued native/messages, resource ownership, pending reset/model/control
journals, supervision/recovery configuration and prior admission modes. Do not
retire workers, spawn helpers or delegate per-project stop requests to implement
this operation.

Stop admission barriers apply immediately to the retained projects. Authenticated
host workers passively inspect known main turns and request exact interruptions;
they keep watching subsequent active turns while the barrier remains active and
never load an unloaded target merely to stop it. Journaled unknown interrupt
outcomes are not replayed. New bindings, creation, guarded work, prepared/queued
bridge wake launches and supervisor recovery are withheld. Newly created project
membership/provisioning remains withheld until explicit resume.

`barrier_complete` reports installation/readback of the durable admission barriers.
Each target's stop journal reports pending/waiting/interrupting/completed or an
unbound observation limitation. Accepted interruption is not observed idle, and
neither stop nor idle proves external process/guard closure. Report outstanding
native queue, stale binding/host, unavailable host and unknown observations instead
of claiming every native process is closed. Use explicit resume with the matching
stop UUID; it removes only that operation's barrier, without enabling supervision
or overriding a previously stopped/otherwise restricted admission policy.

## Reduced, nonmutating Root readiness read

The shared API bearer does **not** permit broad `root status`, enrollment,
orchestration writes, or lifecycle changes. Its additional `root readiness` read
requires the canonical enrolled home project/session, an explicit owning host and
expected bound Root thread, and explicit existing Root-enrolled target IDs:

```sh
agent-chat-client --server https://coordinator.example --project default --session CANONICAL_ROOT_SESSION root readiness --host-id ENROLLED_HOST_ID --expected-thread BOUND_ROOT_THREAD_ID --project-id TARGET_PROJECT_ID --project-id ANOTHER_TARGET_PROJECT_ID
```

This is a syntax template, not authority or an instruction to use another
identity. The existing shared bearer supplies machine-level read authority only
for this bounded nonsecret, explicitly selected, nonmutating projection. The
named session is **UNVERIFIED** as the bearer-holder's identity: canonical IDs
are selectors whose correspondence to stored records is checked, not caller
attestation, native control authority or permission to act on a target. The
server verifies directory ownership, registration, Root projection, native
thread binding, owning host and a matching stored observation no older than 120
seconds (future/unverifiable observations are rejected). The command does not
generate an implicit host identity or refresh that observation. Missing or wrong
required records, unknown/unrelated targets, duplicate/empty/malformed target
lists, extra fields and mixed/write requests are rejected. There is no wildcard
discovery. Up to 64 explicitly selected targets retain request order.

The configured coordinator's legitimate local operator can instead select the
same bounded cohort through the existing explicit same-UID `root-local`
transport, without a network token or borrowed agent identity:

```sh
agent-chat-client root-local --home-project OWN_CANONICAL_HOME_PROJECT \
  --home-session OWN_CANONICAL_ROOT_SESSION --host-id OWN_ENROLLED_HOST \
  readiness --expected-thread OWN_BOUND_ROOT_THREAD --project-id TARGET_PROJECT_ID
```

Repeat `--project-id` for each explicit target. This method checks the configured
provider, wrapper, executable, canonical master and registry ownership against
both actual process UIDs, and verifies existing canonical registration/binding
and project lineage. These are the existing local transport trust checks; the
named session remains a selector, with `named_session_authentication: UNVERIFIED`.
It returns exactly the same `machine_read` projection and freshness/cohort limits.
Missing required records fail closed. It does not initialize schema, synchronize
registrations, resolve routes, create missing databases, refresh observations or
use HTTP. The local `status` and `route` methods remain separate operations;
`status` synchronizes records and is not a nonmutating readiness prerequisite.
There is no automatic local fallback after a network rejection. Use only the
legitimate configured operator's own canonical selectors; this syntax does not
authorize a worker to impersonate Root. The same-UID trust does not isolate a
hostile process already running as that OS user.

The exact shared-bearer wire request is `POST /api/root/rpc`, scoped by the
canonical home project's normal project selector, with fields only `op`,
`session`, `host_id`, and `params`. For `op: "readiness"`, `params` has exactly
`projects` (the explicit unique ID list) and `expected_thread`. Existing exact
`fresh-status`/`stop-status` requests retain their original contracts; broad
status remains denied.

Rejected `root in PROJECT readiness` and incomplete readiness invocations are
validated before implicit host setup; they create no client host state. Metadata
selection itself opens the existing registry read-only, including when it is
missing or disappears immediately before open; it never creates a replacement.
Ordinary project selection and valid non-readiness CLI host behavior are unchanged.

The response states `authority.scope: "machine_read"` and
`authority.named_session_authentication: "UNVERIFIED"`. `caller.record_match`
means only the required canonical stored records matched, not authentication of
the bearer-holder as that session. This read-authority choice does not extend
to any write, lifecycle, resource, model or consequential-control operation.
The remaining response contains only caller project/session/thread/host and its
actual stored observation time, plus selected project IDs, matching directory
Root SID, registration/routing/setup readiness with bounded reasons, designated
PO SID and verified registration/thread/host binding, and the exact host's bridge
coverage state/reason/actual heartbeat time. Registration/PO readiness describes
persisted registration and binding checks, **not** native idle state, empty queues,
guard/process closure, safe restart or model-default verification. Absent setup,
PO, database or coverage records are reported as not-ready/unknown with bounded
reasons; no missing store or schema is created or repaired. Unavailable observation
timestamps stay null, not the request time. Coverage is healthy/stale/unhealthy/
unknown from the selected host's stored record; even a recent healthy record is
not an activation prerequisite check. The current records do not attest a safe
native endpoint identity, so `native_endpoint_identity` is null with
`endpoint_reason: "native_endpoint_not_verified"`; no stored server string is
silently reinterpreted or disclosed as that identity.

No project names, workspace/private paths, instructions, history/task bodies,
model/effort/protected snapshots, credentials, capabilities or resource/queue
contents are returned. The projection uses read-only existing database
transactions rather than `Coordinator` schema initialization, `roots.sync()`,
broad status/reconfiguration reconciliation, setup/routing updates or lifecycle
operations. Reads across project databases are not a cross-project/native atomic
snapshot and cannot fence later external changes. Each affected owner still needs
its own current intake, admission, queue, guard, candidate/recovery and process
checks before consequential action. Installing/loading this source and any later
Root-owned runtime control require separate current authority; a synthetic or
source-level success is not loaded-service readiness.

## Fresh root conversation with a boot prompt

Use a dedicated root fresh-start request rather than detaching the global root or
resetting its projected identities. The same root registration, enrolled projects,
project orchestrators, retained inbox and team stop barriers remain. This operation
never resumes stopped teams and copies no old native transcript.

From the canonical root context:

```sh
agent-chat-client root fresh-start --expected-thread OLD_ROOT_THREAD_UUID --prompt-file /absolute/path/root-boot.txt --request-id REQUEST_UUID --confirm
agent-chat-client root fresh-status --request-id REQUEST_UUID
```

Retain the exact request UUID, old thread UUID and prompt bytes for a retry. A root
can request its own turnover and then end its current turn immediately, without
polling for completion from the old turn. The owning
authenticated host bridge waits for the old main to be idle with direct input and
an empty native queue. It does not interrupt or materialize an unloaded old main.
Root-owned reservations/open guards, unresolved root runtime controls, and wakes
already submitted or with uncertain outcomes block the operation until their
normal reconciliation finishes. New ordinary root input launches and unsafe
orchestration writes are held during turnover; read-only context/status remains
available.

The protected operator route is `POST /api/root/fresh-start`, using the existing
browser authentication, trusted-origin and CSRF checks. Its exact fields are
`expected_thread`, `prompt`, `request_id`, and `confirm`. The server selects the
canonical root identity and records operator-route provenance. Root machine RPC
uses the same journal under the root's own identity. Neither path grants external
permissions or bypasses system/tool approval. Operator provenance has the existing
shared-credential authentication boundary.

The existing host bridge creates and verifies a fresh empty conversation using
the current root settings, switches all project aliases through a retained
checkpoint, and relocates only prepared wakes with immutable before/after audit.
Messages, acknowledgements and delivery rows are preserved. It then queues the
exact boot prompt with the request UUID as native client ID. `starting` or
`uncertain` does not prove the prompt was consumed; `finalizing` means exact native
history proved that client ID and turn ID, but durable gate release is still being
reconciled. Only `completed` means consumption and every gate release finished.

Unknown native creation/add/start results remain journaled. The bridge never
replays creation or an ambiguous queue-start call. Missing supported queue,
settings or consumption evidence is a visible staged blocker; there is no fallback
server-spawned process. Loading these changes into an already running installation
requires the documented server and bridge service adoption; these commands do not
restart either service.

## Complete coordination messages

Human/operator-directed replies use the user-requested Caveman full mode unless the user explicitly changes it. Agent-to-agent assignments, reviews, incidents and handoffs use concise normal grammar and structured complete context, overriding inherited compressed style. Use server-stamped sender_role/recipient_role solely for styling; role does not grant instruction authority. Never infer roles from names or body claims. Both channels retain scope, owner/request IDs, relationships, acceptance versus completion/native consumption, evidence, failures, blockers, dependencies and next actions; clarity overrides compression.
Server wake, create/reset and fresh-root boot prompts carry this default. Agent
claims cannot promote message provenance or invoke a user-only style preference.
The app supplies instructions and preserves complete delivered bodies/IDs; it
does not prove that model-generated replies contain every required fact. Read
incomplete deliveries through the supported message API before acting. Existing
user skill installations and explicit user-selected styling remain untouched.
Already persisted native wakes retain their payload; no queued input is rewritten
or replayed. Load updated server/bridge source through the existing service
adoption procedure; no service change is performed by this fix.

Bounded root/project summaries retain necessary operational request/session/
reservation IDs, validation/evidence/failures, dependencies and next action. IDs
are coordination references, not authority. Keep secret credentials and
capability/grant/reservation tokens private; do not copy unrelated full team context.

## Standing protocol once per native conversation

The complete standing protocol (communication style, chat/fetch/context/receipt
rules and operator-authority interpretation) is canonical and content-versioned.
Managed create/reset/fresh-root bootstraps carry it. A manual, existing or unknown
conversation gets the full protocol on its first cold wake. Wakes batch eligible
messages; the protocol is not injected separately for every individual message.

Later direct wakes carry a compact reference plus current message IDs, complete or
retrievable bodies, server-derived roles/origin and routes. Full policy is omitted
only after the owning authenticated host observed the exact offered native client
ID and a native turn ID. Queue/start acceptance alone does not prove consumption.
Immutable receipts use host, native conversation UUID and policy hash; the shared
server registry lets root aliases reuse the same proven receipt. A new UUID,
changed policy hash or mismatched client/server revision stays cold. This records
policy consumption, not a guarantee of the model's semantic memory.

If the reference is unfamiliar after context compaction, fetch the current policy
without dispatching model input:

```sh
agent-chat-client policy
```

Policy fetch is read-only and does not itself prove native consumption. Existing
child routes expose a parent UUID and child path, not the child's actual native
UUID; these handoffs retain full protocol and native-chain safety instructions,
even when the parent is warm. No child discovery or inferred child receipt is added.

Unavailable/malformed history, older style-only startup and cache failures remain
conservatively cold. Cache records are separate from lifecycle completion; registry
failure never causes input replay or ownership/history deletion. Standalone local
bridges use a local private store. Native developer/base instructions are untouched.
Existing persisted wakes retain their body/delivery state; no migration rewrites or
replays them. Server and bridge source adoption is required; no live restart or
conversation change accompanies this implementation.

### Retained parentless worker management

Ordinary worker lifecycle remains project-orchestrator owned. For a retained direct
main with no native parent route, the canonical root can explicitly delegate
management to that project's current designated PO:

```sh
agent-chat-client root delegate-management --project-id PROJECT_ID --po-session PO_SID \
  --target-session RETAINED_SID --expected-thread CURRENT_NATIVE_UUID \
  --expected-host OWNING_HOST --request-id REQUEST_UUID --confirm
```

For an actually unbound retained identity, replace `--expected-thread` with
`--unbound` and supply its recorded host if one exists. This only grants management:
it does not restore a native binding, create input, resume admission, close guards,
or modify native routing. Inspect exact target/session/host evidence first. Root,
operator, designated PO, native-child and conflicting-manager targets are refused.
The request is immutable and an identical retry returns the original audit.

The designated PO then uses normal `agent-status`, `agent-resume` or `session-reset`
with exact current thread IDs and all existing hold, host and native eligibility
gates. `manager_session` is separate from `parent_session`. Descendants of a
delegated main retain their original native routes. A missing reviewer binding
still requires verified native restoration; delegation is not proof it is runnable.
