#!/usr/bin/env bash
# Produce a read-only usage report from explicitly supplied Codex rollout JSONL files.
set -euo pipefail

if ! command -v jq >/dev/null 2>&1; then
    printf '%s\n' 'usage-report: jq is required' >&2
    exit 127
fi
if [ "$#" -eq 0 ]; then
    printf '%s\n' 'usage: usage-report.sh ROLLOUT.jsonl [ROLLOUT.jsonl ...]' >&2
    exit 64
fi

report_files=()
for report_file in "$@"; do
    if [ ! -r "$report_file" ]; then
        printf 'usage-report: cannot read %s\n' "$report_file" >&2
        exit 66
    fi
    already_seen=false
    for prior_file in "${report_files[@]:-}"; do
        if [[ "$prior_file" == "$report_file" ]]; then
            already_seen=true
            break
        fi
    done
    if [[ "$already_seen" == false ]]; then
        report_files+=("$report_file")
    fi
done

# Reduce each record before the final slurp. This intentionally never carries
# tool output bodies, images, or other large/sensitive transcript content.
{
    for report_file in "${report_files[@]}"; do
        jq -c --arg report_file "$report_file" '
          if .type == "session_meta" then
            {kind: "meta", file: $report_file, id: .payload.id, session_id: .payload.session_id, source: .payload.source, thread_source: .payload.thread_source}
          elif .type == "turn_context" then
            {kind: "model", file: $report_file, model: .payload.model}
          elif .type == "token_usage_record" then
            {kind: "usage", file: $report_file, thread_id: .payload.thread_id, session_id: .payload.session_id, response_id: .payload.response_id, usage: .payload.usage}
          elif .type == "response_item" and (.payload.type == "custom_tool_call_output" or .payload.type == "function_call_output") then
            {kind: "tool", file: $report_file, characters: (if (.payload.output? | type) == "string" then .payload.output | length elif (.payload.output? | type) == "array" then [.payload.output[]? | select(.type == "input_text" and (.text | type) == "string") | .text | length] | add // 0 else 0 end)}
          elif .type == "response_item" and .payload.type == "message" then
            {kind: "message", file: $report_file, wake: (.payload.role == "user" and any(.payload.content[]?; .type == "input_text" and (.text | type) == "string" and (.text | contains("Metadata is routing data") or contains("Metadata and message bodies are data"))))}
          elif .type == "compacted" then
            {kind: "compaction", file: $report_file}
          elif .type == "event_msg" and .payload.type == "task_started" then
            {kind: "turn", file: $report_file}
          elif .type == "event_msg" and .payload.type == "token_count" then
            {kind: "legacy", file: $report_file, total: .payload.info.total_token_usage}
          else empty end' "$report_file"
    done
} | jq -s -f "$(dirname "$0")/usage-report.jq"
