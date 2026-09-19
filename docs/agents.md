# Agent adoption prompt

Replace the placeholders, then give this prompt to every main agent. Parents
must pass it to each subagent together with that child's bounded task.

---

Use agent-chat to communicate with me and the other agents, including every
subagent. The checkout is `TOOL_CHECKOUT`, the project root is `PROJECT_ROOT`,
and the shared chat project ID is `PROJECT_ID` (`default` for the existing
project).

In every terminal context, configure the HTTP client:

```sh
export PATH="TOOL_CHECKOUT/bin:$PATH"
export AGENT_CHAT_SERVER="CHAT_URL" # HTTP loopback/tunnel URL or HTTPS URL
export AGENT_CHAT_PROJECT="PROJECT_ID"
export AGENT_CHAT_ROOT="PROJECT_ROOT"
unset AGENT_CHAT_DB
```

Inherit the privately provisioned `AGENT_CHAT_API_TOKEN` from your terminal or
machine environment; do not replace it with a placeholder. Never put the token
in a prompt, message, screenshot, saved file or command argument. Do not open a
remote SQLite file or fall back to a local database if HTTP fails. The chat server hosts only
the web UI and HTTP API; it does not run Codex, project commands or execution
environments. Keep all project files, tools, Codex sessions and subprocesses on
your assigned execution machine.

Clear inherited agent identity and resource tokens before registering. Restore
your exact saved coordination session when continuing an existing identity;
register only for a new identity:

```sh
unset AGENT_CHAT_SESSION AGENT_CHAT_TOKEN
registration=$(agent-chat-client register --agent YOUR_UNIQUE_LABEL)
export AGENT_CHAT_SESSION=$(printf '%s\n' "$registration" | jq -r .session)
```

Save and reuse that session ID. Every subagent registers its own identity with
a distinct label and clears inherited session/resource tokens. Never borrow a
parent's identity or ownership token. Keep the same project ID for every
conversation participating in this project.

Read `agent-chat-client inbox` before shared mutations and while waiting.
Acknowledge consumed messages explicitly with
`agent-chat-client acknowledge MESSAGE_ID`. Reading is not acknowledgement;
neither acknowledgement nor a request grants permission or resource ownership.
Use `agent-chat-client status` to find exact recipient sessions when labels are
ambiguous.

Reply to operator messages under your own session and reply to the exact inbox
message ID:

```sh
agent-chat-client send --to operator --body-file /path/to/answer.md \
  --reply-to ORIGINAL_INBOX_ID
```

Use readable Markdown. Attach up to four relevant PNG/JPEG/GIF/WebP screenshots
with `--attach IMAGE` and explain what each shows. Use the same reply and
acknowledgement protocol between agents.

## Codex wake bridge

The operator starts exactly one `agent-chat-client bridge` launcher per
execution machine that needs wake-ups. Agents and subagents must not start
additional bridge copies for their sessions. It starts one local
`codex app-server --listen
ws://127.0.0.1:4500` together with the bridge. Keep its launcher running while
your sessions are in use: Ctrl+C stops the bridge and its owned app-server.
`agent-chat-client bridge --connect-only` attaches to an already running local
app-server and leaves it running. Use `--codex-server LOCAL_WS` or
`AGENT_CHAT_CODEX_SERVER` to select a different local endpoint, and
`--codex-bin PATH` to select the executable.

The bridge discovers all chat projects by default. Use
`--project PROJECT_ID` or `AGENT_CHAT_PROJECT` to scope it to one project. If
agent terminals set `AGENT_CHAT_PROJECT`, the operator unsets it for an
all-project bridge with `env -u AGENT_CHAT_PROJECT agent-chat-client bridge`. Each
machine has a stable host ID in `~/.local/state/agent-chat`, configurable with
`AGENT_CHAT_STATE_DIR`. Use that same directory for its CLI agents and bridge;
do not share it between machines. Multiple execution machines may work in one
shared project. Each bridge dispatches only routes pinned to its own machine,
with at most one dispatcher per host/project.

Launch each Codex agent on the same machine as its bridge, pointed at that
machine's local endpoint:

```sh
codex --remote ws://127.0.0.1:4500 -C PROJECT_ROOT
```

Existing conversations must be resumed to that local endpoint. Bind the actual
thread from the owning agent, never guess its ID:

```sh
agent-chat-client bind --thread "$CODEX_THREAD_ID"
```

Native subagents bind their own coordination session through their parent:

```sh
agent-chat-client bind --parent-session EXACT_PARENT_SESSION \
  --agent-path /root/EXACT_CHILD_TASK
```

Use the child's actual canonical path. Nested children route through their
immediate parent. The parent uses its native follow-up/resume mechanism for the
existing child; do not impersonate it or start a duplicate worker. If the
runtime cannot resume that child, report the limitation in chat. Active agents
must still check their inbox. Do not create polling model turns or wake loops.

## Shared resources and shutdown

Reserve shared resources with
`agent-chat-client request RESOURCE --minutes N`, using agreed names such as
`validation-clone`, `file:src/example.py`, and `git-index`. Proceed only when
the returned state is `owned`; retain the reservation ID and token as
`AGENT_CHAT_TOKEN`. A directory resource does not lock descendants. Queued
agents keep checking inbox/status and repeat the request when next. A token
never expands your task scope.

Expiry marks ownership stale and never transfers it. Restore the agreed state,
close every process you own, and retain a nonempty closure report before
release or recovery. Use the receipt schema and commands in the
[README shared resource section](../README.md#shared-resource-receipts).
Preserve unrelated user processes and work; never reset the database or bypass
a queue to obtain ownership.

Pass this protocol to every subagent with its exact allowed files, resources,
exclusions and acceptance criteria. Avoid overlapping mutable state. Parents
review results and own integration/final verification. Subagents must not
request/grant approvals, commit/push, or mutate issues/PRs.

When permanently finished, send your final reply, acknowledge consumed
messages, close owned work/processes and release resources with receipts.
Retire children before their parent. Then run `agent-chat-client deregister`
under your own identity. Resolve any reported pending wake job first. Do not
deregister after one turn if you are expected to accept follow-up instructions;
a retired session ID cannot be reused.
