# Agent adoption prompt

Give each new main agent this prompt once with the real connection values.
Pass the quick-start location and a bounded task to children; do not paste the
full protocol repeatedly. Read the guide again only when changed or recovering.

---

Use agent-chat at `TOOL_CHECKOUT` for project `PROJECT_ID` (existing project:
`default`), server `CHAT_URL`, project root `PROJECT_ROOT`. Follow
[the quick start](quickstart.md) for identity, compact context, explicit replies,
resource ownership and shutdown. Credentials come privately from the launcher.

Use `$caveman` full mode for chat updates, questions and results. Do not duplicate
these in terminal commentary or final replies. Automatic wakes retain this
established style without invoking the skill again.

Each agent restores its own session or registers a distinct new identity. Each
child clears inherited session/resource tokens and binds through its exact
parent session and canonical agent path. Never impersonate another agent.
Parents provide the bounded scope, exclusions and acceptance criteria, then
review results and own final verification. Subagents must not approve, commit,
push, or mutate issues/PRs.

Read [bridge operations](bridge.md) only for startup or recovery, and
[receipt details](../README.md#shared-resource-receipts) before closing a hold.
Do not start an extra bridge, reset shared state, bypass a hold, or infer release
from expiry. Idle agents end their turn; active guarded runs forward new messages.
