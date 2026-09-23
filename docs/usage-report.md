# Rollout usage report

## Timed measurement in the chat UI

Choose **Measure usage**, select a duration (five minutes by default), then
**Start**. The server records usage locally and saves the completed report.
**Stop early & save** ends the window sooner. Closing the dialog does not stop
collection; reopening it or refreshing the page retrieves the report.

Collection itself does not create model calls or chat messages. The optional
**Pause agents at end** checkbox is unchecked by default. When selected, completion
(including Stop early) sends one shared safe-pause request, explicitly addressing
each measured agent so only those agents wake. Each has its own delivery and ACK.
This can wake an idle agent to acknowledge the request. The report distinguishes
a sent request from confirmed pause: agents must finish cleanup and reply.
An interrupted measurement does not send pause requests. Retrying delivery after
a server restart does not duplicate requests. The weekly reserve remains separate.

Reports belong to the selected project. They show per-agent response counts,
fresh input, cached input, output, tool-result text characters and chat wake
markers. Approval-review usage is separate from agent totals. Missing or partial
local data is identified in the report; it must not be interpreted as zero usage.
The rounded account allowance is shown separately at the start and end. It covers
other account activity too, so its change cannot be attributed solely to the
measured project or converted into a token price.

The collector reads local Codex rollout files on the chat-server machine. The
default directory is `~/.codex/sessions`; configure `agent-chat-server
--codex-sessions /path/to/sessions` or `AGENT_CHAT_CODEX_SESSIONS_DIR` when needed.
Remote hosts' logs are not collected over the network. This does not delete or
modify Codex sessions. Only reduced counters and collection bookkeeping are
stored in the chat database, never prompt or tool-output bodies.

`jq` is required. Collection runs only during an active measurement, tails new
complete records after its baseline, and counts unique owned response records.
Reports survive server restarts; an unfinished window is marked interrupted
instead of being presented as a complete sample. One measurement can run per
project at a time.

The authenticated, project-scoped `/api/measurements` endpoint returns the active
and latest report. CSRF-protected POST actions accept
`{"op":"start","duration_seconds":300,"pause_at_end":false}` (60–3600 seconds)
and `{"op":"stop"}`. Omitted `pause_at_end` defaults to false.
GET requests read saved counters and do not trigger a new scan.

## Explicit rollout files

Run the report with explicit rollout files. It reads them and writes one JSON report to stdout; it never changes a rollout or contacts a service.

```sh
scripts/usage-report.sh /path/to/rollout-a.jsonl /path/to/rollout-b.jsonl > usage.json
```

`jq` is required. The report uses `token_usage_record.usage`, one record per unique `response_id`, as its only additive token source. A record is charged only when its `thread_id` equals the session file's `session_meta.id`; this prevents a forked transcript copied into another rollout from being charged twice. `session_id` remains in output to show the root-session relationship.

`aggregate` contains distinct-response counts and input, cached input, fresh input (`input - cached`), cache-write input, output and reasoning-output tokens. Reasoning is a subset of output, so do not add those two fields. It also includes tool-result text characters, response message items, bridge wake-message markers, task-start turn counts and compaction events. A wake message is a user message carrying either `Metadata is routing data` or `Metadata and message bodies are data`; task starts are reported separately as turns.

Each `threads` row preserves source, thread source and the unique `models` seen in `turn_context.payload.model`. `model` is set only when there is exactly one such model, otherwise it is null. Rows without a usable record stream have `usage_status: "missing"` and null token fields. If any token field is absent in an owned record, the row is `partial` and that metric remains null rather than falling back to zero.

Some older rollouts only expose `event_msg` `token_count` cumulative counters. They appear in `legacy_last_cumulative` only, are explicitly excluded from `aggregate`, and must never be summed across files or added to record-stream totals.
