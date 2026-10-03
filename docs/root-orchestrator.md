# One root across projects

Run one foreground Codex conversation from a dedicated neutral directory. It
coordinates independent project orchestrators, each with its own thread,
workspace, instructions, checkpoints and workers. The browser's **Root
orchestrator** screen at `/root` shows only your direct canonical-home conversation and a
compact project overview. Project messages and attachments remain in their rooms.
Use **Open project** to enter a project room; drafts remain separate between views.
The left project selector shows a blank choice while Root chat is active. Select
any project there to open its room, including root's home project.

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

Root boots once with project-neutral standing rules. Project assignments arrive
later through project creation or operator messages; they are not root boot prompts.
In browser **New project**, supply a name, an existing absolute workspace on the
agent host and project instructions. CLI creation supports the same inputs:

```sh
agent-chat-client project create --name NAME --workspace /absolute/project \
  --prompt-file /absolute/project-assignment.txt
```

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

Name-only creation remains supported. Missing workspace/task yields incomplete scope;
root asks for only those missing details instead of guessing directories or copying
another project's instructions. Root does not prepare or edit project files: an
approved project worker/operator does. For an assignment supplied directly in chat,
root creates/selects the project and uses `root configure` below with that assignment.

Use the workspace's canonical absolute path on the execution host. Symlinks must
be resolved there; project workspaces cannot overlap each other or root's neutral
workspace. Save UUID and literal prompt file privately before submission:

```sh
agent-chat-client context
agent-chat-client root configure --project-id EXACT_PROJECT_ID \
  --agent orchestrator --cwd /absolute/canonical/project-workspace \
  --model gpt-6.1-sol --reasoning-effort high \
  --request-id SAVED_UUID --prompt-file /absolute/bounded-prompt.txt --confirm
agent-chat-client root status
agent-chat-client root in EXACT_PROJECT_ID session-reset-status --to orchestrator
```

Creation uses the existing durable independent-main workflow: exact settings,
empty-thread admission, lost-response reconciliation and no uncertain replay.
Bootstrap specifies exact project and root registration. `completed` means first
input admitted, not that the task finished or passed acceptance.

Root-created agents use built-in `:workspace`, one project runtime root and no
extra writable roots. Normal system temporary-directory access remains enabled;
network defaults off. Model/provider and approval policy/reviewer remain
root templates; explicit model/effort select project settings. Named global
profiles and grants to other projects are not copied. Read-only or unsupported
root policies refuse writable creation. Returned settings must match before
binding/input. Root creates only project orchestrators through `root configure`. Browser/direct
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
inspection also supports adoption. Root cannot directly create, stop, reset or
retire workers; project orchestrators manage those lifecycles within their own
project. Existing ownership, measurement, usage, expected-thread and receipt
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

```text
Act as global root orchestrator through the existing agent-chat service.
Run from the dedicated neutral workspace. Retain your canonical home
project/session privately. Read docs/root-orchestrator.md and enroll your
actual bound Codex thread after a fresh agent-status observation.

Coordinate only. Keep source, project AGENTS.md instructions, detailed logs
and technical planning with independent project orchestrators. Receive short
reports: project ID, goal, status, blocker, decision needed, checkpoint path.
Global preferences must be explicitly global. Never promote project requirements
to defaults for other projects.

New projects automatically register your distinct quiet projection. Read context
and root status. Ask the operator for missing workspace/task scope; configure a
bounded independent project orchestrator or adopt an existing authorized main.
Create, stop, resume, reset and retire only project orchestrators at root level.
Project orchestrators manage their workers; do not bypass this hierarchy.
Use root in EXACT_PROJECT_ID for each operation. Never borrow identity, bypass
holds, replay uncertain input or launch another bridge. Project workers own
exact reservations and truthful CLOSED receipts.

Talk to the operator through your canonical home conversation. Reply to project
agents through matching project projections and exact delivery IDs. When idle,
end your turn and let the existing bridge wake you. Keep concise checkpoints
and decisions; do not poll or collect every project's technical context.
```

Timed project measurements exclude global root projections: one shared root
thread cannot be charged independently to several projects or paused by one
project's measurement. Project orchestrators and workers remain measured; the
global weekly usage guard still applies to root. Directory/team designations
survive clean detach and re-enrollment of the same home identity. Retained root
registrations remain quiet even during that refresh.
Use [root-AGENTS.md](root-AGENTS.md) as reusable standing guidance in the neutral
workspace's `AGENTS.md`. Keep per-project assignments in project creation or setup
messages. Create that file before launching the new root conversation.
