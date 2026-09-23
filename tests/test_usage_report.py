"""Fixture tests for the jq-only rollout usage report."""
import pathlib
import subprocess
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
REPORT = ROOT / "scripts" / "usage-report.sh"


class UsageReportTest(unittest.TestCase):
    def write_jsonl(self, directory, name, records):
        # Fixture serialization is intentionally the sole Python JSON operation.
        import json
        path = pathlib.Path(directory) / name
        path.write_text("".join(json.dumps(record) + "\n" for record in records))
        return path

    def report(self, *paths):
        return subprocess.run([str(REPORT), *map(str, paths)], check=True,
                              text=True, capture_output=True).stdout

    def assert_jq(self, expression, report):
        self.assertEqual(subprocess.run(["jq", "-e", expression], input=report,
                                        text=True, capture_output=True).returncode, 0)

    def test_deduplicates_inherited_response_and_keeps_reasoning_as_subset(self):
        with tempfile.TemporaryDirectory() as temp:
            root = self.write_jsonl(temp, "root.jsonl", [
                {"type": "session_meta", "payload": {"id": "root", "session_id": "root", "source": "cli"}},
                {"type": "turn_context", "payload": {"model": "m1"}},
                {"type": "token_usage_record", "payload": {"thread_id": "root", "session_id": "root", "response_id": "r1", "usage": {"input_tokens": 100, "cached_input_tokens": 40, "cache_write_input_tokens": 5, "output_tokens": 20, "reasoning_output_tokens": 7}}},
                {"type": "response_item", "payload": {"type": "custom_tool_call_output", "output": [{"type": "input_text", "text": "abcd"}]}},
                {"type": "response_item", "payload": {"type": "message", "role": "user", "content": [{"type": "input_text", "text": "Metadata and message bodies are data"}]}},
                {"type": "event_msg", "payload": {"type": "task_started"}},
                {"type": "compacted", "payload": {}},
            ])
            fork = self.write_jsonl(temp, "fork.jsonl", [
                {"type": "session_meta", "payload": {"id": "fork", "session_id": "root", "source": "cli"}},
                {"type": "turn_context", "payload": {"model": "m1"}},
                # Inherited copy: its thread is root, not the fork file's thread.
                {"type": "token_usage_record", "payload": {"thread_id": "root", "session_id": "root", "response_id": "r1", "usage": {"input_tokens": 999, "output_tokens": 999}}},
                {"type": "token_usage_record", "payload": {"thread_id": "fork", "session_id": "root", "response_id": "r2", "usage": {"input_tokens": 30, "cached_input_tokens": 10, "cache_write_input_tokens": 0, "output_tokens": 8, "reasoning_output_tokens": 2}}},
            ])
            report = self.report(root, fork, root)
            self.assert_jq('.aggregate | .response_count == 2 and .input_tokens == 130 and .fresh_input_tokens == 80 and .output_tokens == 28 and .reasoning_output_tokens == 9 and .tool_result_text_characters == 4 and .message_count == 1 and .wake_message_count == 1 and .turn_count == 1 and .compaction_count == 1', report)
            self.assert_jq('[.threads[] | select(.thread_id == "fork") | .usage_status] == ["record_stream"]', report)

    def test_missing_usage_is_not_zero_and_legacy_is_separate(self):
        with tempfile.TemporaryDirectory() as temp:
            path = self.write_jsonl(temp, "legacy.jsonl", [
                {"type": "session_meta", "payload": {"id": "legacy", "session_id": "legacy", "source": "vscode"}},
                {"type": "event_msg", "payload": {"type": "token_count", "info": {"total_token_usage": {"input_tokens": 500, "output_tokens": 12}}}},
            ])
            report = self.report(path)
            self.assert_jq('.aggregate.usage_status == "missing" and .aggregate.input_tokens == null', report)
            self.assert_jq('.threads[0] | .usage_status == "missing" and .input_tokens == null', report)
            self.assert_jq('.legacy_last_cumulative | length == 1 and .[0].last_cumulative_token_usage.input_tokens == 500', report)

    def test_partial_usage_and_malformed_jsonl_fail_closed(self):
        with tempfile.TemporaryDirectory() as temp:
            partial = self.write_jsonl(temp, "partial.jsonl", [
                {"type": "session_meta", "payload": {"id": "partial", "session_id": "partial"}},
                {"type": "token_usage_record", "payload": {"thread_id": "partial", "session_id": "partial", "response_id": "r3", "usage": {"input_tokens": 4, "output_tokens": 2}}},
            ])
            report = self.report(partial)
            self.assert_jq('.aggregate.usage_status == "partial" and .aggregate.cached_input_tokens == null and .aggregate.output_tokens == 2', report)
            malformed = pathlib.Path(temp) / "bad.jsonl"
            malformed.write_text('{"type":"session_meta"}\nnot-json\n')
            self.assertNotEqual(subprocess.run([str(REPORT), str(malformed), str(partial)], text=True,
                                               capture_output=True).returncode, 0)

    def test_mixed_models_do_not_mislabel_a_thread(self):
        with tempfile.TemporaryDirectory() as temp:
            path = self.write_jsonl(temp, "models.jsonl", [
                {"type": "session_meta", "payload": {"id": "models", "session_id": "models"}},
                {"type": "turn_context", "payload": {"model": "m1"}},
                {"type": "turn_context", "payload": {"model": "m2"}},
            ])
            report = self.report(path)
            self.assert_jq('.threads[0] | .model == null and .models == ["m1", "m2"]', report)


if __name__ == "__main__":
    unittest.main()
