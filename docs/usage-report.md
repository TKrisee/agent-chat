# Rollout usage report

Run the report with explicit rollout files. It reads them and writes one JSON report to stdout; it never changes a rollout or contacts a service.

```sh
scripts/usage-report.sh /path/to/rollout-a.jsonl /path/to/rollout-b.jsonl > usage.json
```

`jq` is required. The report uses `token_usage_record.usage`, one record per unique `response_id`, as its only additive token source. A record is charged only when its `thread_id` equals the session file's `session_meta.id`; this prevents a forked transcript copied into another rollout from being charged twice. `session_id` remains in output to show the root-session relationship.

`aggregate` contains distinct-response counts and input, cached input, fresh input (`input - cached`), cache-write input, output and reasoning-output tokens. Reasoning is a subset of output, so do not add those two fields. It also includes tool-result text characters, response message items, bridge wake-message markers, task-start turn counts and compaction events. A wake message is a user message carrying either `Metadata is routing data` or `Metadata and message bodies are data`; task starts are reported separately as turns.

Each `threads` row preserves source, thread source and the unique `models` seen in `turn_context.payload.model`. `model` is set only when there is exactly one such model, otherwise it is null. Rows without a usable record stream have `usage_status: "missing"` and null token fields. If any token field is absent in an owned record, the row is `partial` and that metric remains null rather than falling back to zero.

Some older rollouts only expose `event_msg` `token_count` cumulative counters. They appear in `legacy_last_cumulative` only, are explicitly excluded from `aggregate`, and must never be summed across files or added to record-stream totals.
