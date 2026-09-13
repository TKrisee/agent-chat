# Prompt for main agents and subagents

Replace the three placeholders, then share the following prompt with every main
agent. Parents must pass it to every subagent with that child's bounded scope.

---

Use agent-chat for communication with me and the other agents, including every
subagent you spawn. The tool is installed at `TOOL_CHECKOUT/bin`. This project's
root is `PROJECT_ROOT` and its one shared local SQLite database is `ABSOLUTE_DB`.

Export `PATH="TOOL_CHECKOUT/bin:$PATH"`, `AGENT_CHAT_ROOT="PROJECT_ROOT"`, and
`AGENT_CHAT_DB="ABSOLUTE_DB"` in each terminal context. Keep the same absolute DB
across worktrees/clones. If you already have a coordination session, restore its
exact ID as `AGENT_CHAT_SESSION` (legacy `ITR_COORD_SESSION` is also accepted).
Register only if this is a new session:

```sh
unset AGENT_CHAT_SESSION AGENT_CHAT_TOKEN ITR_COORD_SESSION ITR_COORD_TOKEN
registration=$(agent-chat register --agent YOUR_UNIQUE_LABEL)
export AGENT_CHAT_SESSION=$(printf '%s\n' "$registration" | jq -r .session)
```

Save and reuse that ID. Each subagent registers its own session, uses a distinct
label such as `backend/reviewer`, and clears inherited identity/token values.
Never share a parent's coordination identity or ownership token.

Read `agent-chat inbox` before shared mutations and while waiting. Explicitly
acknowledge messages you consume with `agent-chat acknowledge MESSAGE_ID`.
Reading is not acknowledgement; acknowledgement is not task completion.
Sending/acknowledging a request grants neither permission nor resource ownership.
Use `agent-chat status` to find exact recipient sessions if labels are ambiguous.

Reply to my chat instructions as `operator`, under your own session:

```sh
agent-chat send --to operator --body-file /path/to/answer.md --reply-to ORIGINAL_INBOX_ID
```

Use the exact ID in your own inbox, including for a broadcast/multiple-recipient
message. Include basic Markdown for readable status tables. Add `--attach IMAGE`
for up to four PNG/JPEG/GIF/WebP screenshots (10 MiB each). Attach actual relevant
evidence and explain what it shows. Use the same reply/ack protocol between agents.

When the shared Codex app-server and bridge are enabled, a main agent running
through that server binds its actual conversation:

```sh
agent-chat bind --thread "$CODEX_THREAD_ID"
```

Do not guess a thread ID. Native subagents instead bind **under their own
coordination identity** through their parent:

```sh
agent-chat bind --parent-session EXACT_PARENT_COORDINATION_SESSION --agent-path /root/EXACT_CHILD_TASK
```

Use the actual canonical child path. Parents provide their coordination session
ID and the child's exact path. Nested children route through their immediate
parent. Never bind an inherited parent thread UUID as a child's own thread.
A child may bind directly only with its distinct, directly addressable thread.

The bridge wakes idle loaded main threads for my messages. For child messages,
the parent verifies the supplied path belongs to its existing child, then uses
its native follow-up/resume tool to wake that child and
pass the message IDs, DB and protocol. The child reads/acks/replies under its own
identity. Do not impersonate it or create a duplicate worker. If the native
runtime cannot resume it, report the limitation through chat. Active agents must
still check their inbox; unbound/closed agents wait until resumed. Agent replies
do not automatically wake peers. Do not create polling model turns or wake loops.

Reserve shared resources with `agent-chat request RESOURCE --minutes N`, using
agreed names such as `validation-clone`, `file:src/example.py`, and `git-index`.
Proceed only on `state == "owned"`; retain that reservation ID and token as
`AGENT_CHAT_TOKEN`. A directory resource does not lock descendants. Queued agents
keep checking inbox/status and repeat `request` when next. Run shared validation
through `agent-chat run RESOURCE -- COMMAND ...`; preserve project-specific
validation flags and restoration guards. A token never expands your task scope.

Expiry marks ownership stale and never transfers it. Restore the exact agreed
state, close every owned process, and retain a nonempty closure report before
release/recovery. Use the reservation-bound JSON CLOSED receipt described in the
tool's README. Preserve unrelated user processes and work; never reset the DB
or bypass a queue to obtain ownership.

Pass this complete protocol to every subagent, with its exact allowed files,
resources, exclusions and acceptance criteria. Delegate without overlapping
mutable state. Parents review results and own integration/final verification.
Subagents must not request/grant approvals, commit/push or mutate issues/PRs.
