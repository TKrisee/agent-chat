# Agent quick start

Read once when establishing an identity. Revisit changed instructions or the
relevant recovery section; do not reload the whole protocol at each milestone.
Connection values belong to the consuming project's instructions.

## Identity and connection

In each terminal context export the app `bin` directory on `PATH`,
`AGENT_CHAT_SERVER`, `AGENT_CHAT_PROJECT`, `AGENT_CHAT_ROOT`, and your saved
`AGENT_CHAT_SESSION`. Inherit `AGENT_CHAT_API_TOKEN` privately from the launcher.
Unset `AGENT_CHAT_DB` and legacy coordination variables. Never print credentials,
put them in command arguments or messages, or fall back to a local database.

A new agent clears inherited session/resource tokens and registers once:

```sh
unset AGENT_CHAT_SESSION AGENT_CHAT_TOKEN
registration=$(agent-chat-client register --agent UNIQUE_ROLE)
export AGENT_CHAT_SESSION=$(printf '%s\n' "$registration" | jq -er .session)
```

Retain this ID across terminal calls. Restore it on continuation; never use a
parent's identity. Every child registers separately. Keep the host state
directory consistent with its bridge; do not share it between machines.

## Routine work

```sh
agent-chat-client context
agent-chat-client status --mine
agent-chat-client status --resource validation-clone
agent-chat-client message MESSAGE_ID
agent-chat-client send --to RECIPIENT --body-file /absolute/reply.md \
  --reply-to ORIGINAL_MESSAGE_ID --ack-reply
```

`context` returns at most 20 messages and 12 KiB by default, plus owned/queued
resources. Follow `cursor` with `--cursor` and `resources_cursor` with `--resource-cursor`
while the matching `has_more` flag is true. Fetch incomplete messages before
acting on their contents. `body_truncated`/`metadata_truncated` marks incomplete message content;
`detail_truncated` resources need `status --resource NAME`. Filter a wake with repeated `--message-id`; select
resources with repeated `--resource`. Only the full message command intentionally
returns an unrestricted body. Reading is not acknowledgement or ownership.

Check at task entry, before shared mutations and at meaningful work boundaries.
Reuse complete messages delivered in wakes or guarded-run output; do not fetch
them again merely to read them. `context` before ownership changes establishes
the server's inbox freshness proof. New messages arriving afterward still require
a fresh check. Do not acknowledge previews or omitted content.

Use `--ack-reply` only after consuming the source message. Identical retries
return the original reply; changing an already-sent reply's contents is rejected.
For messages needing no reply, use `acknowledge MESSAGE_ID`; never send ACK-only
chat messages. Use exact recipients and `--reply-to` for real replies. Keep the
established communication style and avoid duplicating updates across channels.

Batch independent reads in a single tool call. Prefer filtered output and
targeted file sections; retain full logs on disk and inspect details on failure.
Do not create model turns solely to poll. When idle, finish the turn and rely on
the existing bridge; active guarded runs already forward new messages.

## Resources and validation

Reserve exact resources with `request RESOURCE --minutes N`; proceed only on
`owned`. Export its session-bound token as `AGENT_CHAT_TOKEN`, then run shared
validation through `run RESOURCE -- COMMAND ...`. Directory names do not lock
descendants. Queue order, stale holds and existing ownership remain authoritative.

Before release, restore agreed state, close owned processes and retain a
reservation-bound CLOSED receipt. Expiry never transfers ownership. Read
[receipt details](../README.md#shared-resource-receipts) when closing or recovering
a hold. Never reset shared state or bypass a hold. Subagents cannot approve,
commit, push or mutate issues/PRs; parents review their work and final checks.

## Binding and recovery

Use the operator's single local bridge/app-server; never launch another bridge.
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
