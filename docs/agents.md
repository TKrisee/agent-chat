# Agent startup and operations

Choose one role prompt; do not paste the operations manual into every conversation.
Read the shared startup rules once, then the bounded assignment/checkpoint and only
the relevant reference section. Existing identities and queues survive a fresh
native conversation; startup is not permission to register replacements or resume
stopped work.

| Role | Boot prompt | Scope |
| --- | --- | --- |
| Global root | [Root](boot/root.md) | Cross-project orchestration through retained project POs |
| Project orchestrator | [Project PO](boot/project-orchestrator.md) | Retain project context, delegate technical work, own project decisions |
| Bounded worker | [Worker](boot/worker.md) | Assigned implementation, investigation or validation only |
| Independent reviewer | [Reviewer](boot/reviewer.md) | Independent bounded review; no overlapping source edits |

Start with [boot inputs and read order](boot/README.md) and
[shared startup rules](boot/shared.md). The complete command and recovery manual
is preserved in [Agent operations](reference/agent-operations.md).
[Root operations](root-orchestrator.md) is the operator/API reference;
[root standing instructions](root-AGENTS.md) keep project-specific briefs separate.
Host/operator installation belongs in the [operator README](../README.md), not in
an additional model role. Already provided instructions need not be reread unchanged.

## Existing section links

The named sections below retain the original guide's external anchors.

## Identity and connection

See [the retained operations section](reference/agent-operations.md#identity-and-connection).

## Routine work

See [the retained operations section](reference/agent-operations.md#routine-work).

## Direct and group messages

See [the retained operations section](reference/agent-operations.md#direct-and-group-messages).

## Resources and validation

See [the retained operations section](reference/agent-operations.md#resources-and-validation).

## Local-work hygiene

See [the retained operations section](reference/agent-operations.md#local-work-hygiene).

## Project-PO delegation boundary

See [the retained operations section](reference/agent-operations.md#project-po-delegation-boundary).

## Moving an operator/agent conversation between projects

See [the retained operations section](reference/agent-operations.md#moving-an-operatoragent-conversation-between-projects).

## Binding and recovery

See [the retained operations section](reference/agent-operations.md#binding-and-recovery).

## Shared resource receipts

See [the retained operations section](reference/agent-operations.md#shared-resource-receipts).

## Adoption prompt

See [the retained operations section](reference/agent-operations.md#adoption-prompt).

## Fresh conversations and independent agents

See [the retained operations section](reference/agent-operations.md#fresh-conversations-and-independent-agents).

## One CLI orchestrator

See [the retained operations section](reference/agent-operations.md#one-cli-orchestrator).

## Change a main agent's model on the same thread

See [the retained operations section](reference/agent-operations.md#change-a-main-agents-model-on-the-same-thread).

## Deterministic agent supervision

See [the retained operations section](reference/agent-operations.md#deterministic-agent-supervision).
