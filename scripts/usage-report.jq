# Input has already been reduced to report fields by usage-report.sh.
def every_number($records; $field): all($records[]; (.usage[$field]? | type) == "number");
def summed($records; $field): if every_number($records; $field) then [$records[].usage[$field]] | add else null end;
def token_metrics($records):
  if ($records | length) == 0 then {response_count: 0, input_tokens: null, cached_input_tokens: null, fresh_input_tokens: null, cache_write_input_tokens: null, output_tokens: null, reasoning_output_tokens: null, usage_status: "missing"}
  else {response_count: ($records | length), input_tokens: summed($records; "input_tokens"), cached_input_tokens: summed($records; "cached_input_tokens"), fresh_input_tokens: (if every_number($records; "input_tokens") and every_number($records; "cached_input_tokens") then [$records[] | .usage.input_tokens - .usage.cached_input_tokens] | add else null end), cache_write_input_tokens: summed($records; "cache_write_input_tokens"), output_tokens: summed($records; "output_tokens"), reasoning_output_tokens: summed($records; "reasoning_output_tokens")} | .usage_status = (if ([.input_tokens, .cached_input_tokens, .fresh_input_tokens, .cache_write_input_tokens, .output_tokens, .reasoning_output_tokens] | any(. == null)) then "partial" else "record_stream" end)
  end;
def file_rows($kind; $file): [.[] | select(.kind == $kind and .file == $file)];

. as $all |
([$all[] | select(.kind == "meta") | {key: .file, value: .}] | from_entries) as $metadata |
([$all[] | select(.kind == "meta") | .file] | unique) as $files |
([$all[] | select(.kind == "usage") | . as $record | $metadata[$record.file] as $meta | select($meta != null and $record.thread_id == $meta.id and $record.response_id != null) | $record] | unique_by(.response_id)) as $records |
([$files[] as $file | $metadata[$file] as $meta |
  (file_rows("model"; $file) | map(.model) | map(select(type == "string")) | unique) as $models |
  ([$records[] | select(.file == $file)]) as $owned |
  (file_rows("tool"; $file) | map(.characters) | add // 0) as $tool_characters |
  (file_rows("message"; $file)) as $messages |
  {thread_id: $meta.id, session_id: $meta.session_id, source: $meta.source,
   thread_source: $meta.thread_source, models: $models,
   model: (if ($models | length) == 1 then $models[0] else null end)} + token_metrics($owned) +
  {tool_result_text_characters: $tool_characters, message_count: ($messages | length), wake_message_count: ([$messages[] | select(.wake)] | length), turn_count: (file_rows("turn"; $file) | length), compaction_count: (file_rows("compaction"; $file) | length)}]) as $threads |
token_metrics($records) as $token_aggregate |
([$files[] as $file | $metadata[$file] as $meta |
  (file_rows("model"; $file) | map(.model) | map(select(type == "string")) | unique) as $models |
  (file_rows("legacy"; $file) | map(.total) | last) as $legacy | select($legacy != null) |
  {thread_id: $meta.id, session_id: $meta.session_id, source: $meta.source,
   models: $models, model: (if ($models | length) == 1 then $models[0] else null end),
   last_cumulative_token_usage: $legacy}]) as $legacy |
{schema_version: 1,
 note: "Usage totals sum unique owned token_usage_record responses only. reasoning_output_tokens is a subset of output_tokens.",
 aggregate: ($token_aggregate + {tool_result_text_characters: ($threads | map(.tool_result_text_characters) | add // 0), message_count: ($threads | map(.message_count) | add // 0), wake_message_count: ($threads | map(.wake_message_count) | add // 0), turn_count: ($threads | map(.turn_count) | add // 0), compaction_count: ($threads | map(.compaction_count) | add // 0)}),
 threads: $threads,
 legacy_last_cumulative: $legacy,
 legacy_note: "Legacy token_count totals are per-file last cumulative counters. They are shown for diagnostics only and are not additive with, or included in, aggregate."}
