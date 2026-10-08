# Role boot prompts

Use one role file as the role portion of a boot prompt. These are ready-to-use
Markdown prompt text, not native skills, privilege grants or approval bypasses,
and not a replacement for the literal task or developer's
workspace `AGENTS.md`. The operator/parent supplies the non-secret inputs listed
in that role file; credentials remain inherited privately, never pasted here.

| Role | Prompt | Who assigns it |
| --- | --- | --- |
| Global root | [root.md](root.md) | Operator, for the canonical neutral root conversation |
| Project PO | [project-orchestrator.md](project-orchestrator.md) | Root/operator, for one retained project identity |
| Worker | [worker.md](worker.md) | Project PO/parent, for bounded technical work |
| Reviewer | [reviewer.md](reviewer.md) | Parent, for independent review of a concrete result |

Read order: supplied identity/binding and role inputs → [shared rules](shared.md)
once → role prompt → local `AGENTS.md` and current checkpoint/task → the specific
[operations reference](../reference/agent-operations.md) needed for this action.
Do not read the entire manual at every startup or milestone. If the shared rules
are already supplied unchanged, reuse them.

Managed native create/reset/fresh-root startup includes the canonical app protocol.
An unknown/manual conversation receives it on a cold wake. Later direct wakes may
carry a compact reference after actual native consumption proof; if unfamiliar,
run `agent-chat-client policy`. Do not manually mark it consumed, assume queue
acceptance is consumption, or replace this with a PO's informal memory.

Copy the selected prompt's role instructions and provide its inputs and bounded
assignment separately. Keep the task's exclusions, acceptance criteria and exact
IDs complete. Missing required identity/scope is a blocker, not permission to
borrow credentials, create a new service, invent a project or widen authority.

The shipped root/project `AGENTS.md` baselines are self-contained portable copies
for installed workspaces. Their installer preserves developer customizations;
these documentation links do not have to exist in an installed neutral workspace.
