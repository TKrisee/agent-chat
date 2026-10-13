"""Current-thread counting contracts through local rollouts and the HTTP API."""
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock
import uuid

from agent_chat.compaction import CompactionCounter
from test_remote_web import RemoteWebFixture


def rollout(root, thread, *records):
    path = Path(root) / f"rollout-{thread}.jsonl"
    path.write_text(json.dumps({"type": "session_meta", "payload": {"id": thread}}) + "\n"
                    + "".join(json.dumps(record) + "\n" for record in records))
    return path


class CompactionCounterTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.counter = CompactionCounter(self.tmp.name)
        self.thread = str(uuid.uuid4())

    def test_completed_records_advance_and_partial_malformed_unrelated_do_not(self):
        path = rollout(self.tmp.name, self.thread, {"type": "compacted"})
        self.assertEqual(self.counter.count(self.thread), 1)
        with path.open("a") as file:
            file.write('not json\nnull\n42\n"compacted"\n')
            file.write('{"type":"event_msg","payload":{"type":"compacted"}}\n')
            file.write('{"type":"compacted"}')
        self.assertEqual(self.counter.count(self.thread), 1)
        with path.open("a") as file:
            file.write("\n")
        self.assertEqual(self.counter.count(self.thread), 2)
        self.assertEqual(self.counter.count(self.thread), 2)

    def test_missing_wrong_identity_and_unreadable_evidence_remain_unknown(self):
        self.assertIsNone(self.counter.count(None))
        self.assertIsNone(self.counter.count(self.thread))
        path = rollout(self.tmp.name, str(uuid.uuid4()), {"type": "compacted"})
        path.rename(Path(self.tmp.name) / f"rollout-{self.thread}.jsonl")
        self.assertIsNone(self.counter.count(self.thread))
        path = rollout(self.tmp.name, self.thread)
        with mock.patch.object(Path, "open", side_effect=PermissionError):
            self.assertIsNone(self.counter.count(self.thread))
        with mock.patch("agent_chat.compaction.subprocess.run", side_effect=FileNotFoundError):
            self.assertIsNone(self.counter.count(self.thread))
        self.assertEqual(self.counter.count(self.thread), 0)
        path.unlink()
        self.assertIsNone(self.counter.count(self.thread))

    def test_malformed_metadata_shapes_do_not_crash_or_claim_a_known_count(self):
        path = Path(self.tmp.name) / f"rollout-{self.thread}.jsonl"
        for payload in ("bad", 42, [], None, {"id": 42}):
            with self.subTest(payload=payload):
                path.write_text(json.dumps({"type": "session_meta", "payload": payload})
                                + '\n{"type":"compacted"}\n')
                self.assertIsNone(self.counter.count(self.thread))

    def test_truncation_and_replacement_recount_the_current_log(self):
        path = rollout(self.tmp.name, self.thread, {"type": "compacted"}, {"type": "compacted"})
        self.assertEqual(self.counter.count(self.thread), 2)
        rollout(self.tmp.name, self.thread)
        self.assertEqual(self.counter.count(self.thread), 0)
        replacement = Path(self.tmp.name) / "replacement.jsonl"
        replacement.write_text(json.dumps({"type": "session_meta", "payload": {"id": self.thread}})
                               + '\n{"type":"compacted"}\n')
        replacement.replace(path)
        self.assertEqual(self.counter.count(self.thread), 1)

    def test_compactions_across_multiple_batches_are_not_lost_or_duplicated(self):
        self.counter.batch_bytes = 30
        path = rollout(self.tmp.name, self.thread, *[{"type": "compacted"}] * 5)
        self.assertEqual(self.counter.count(self.thread), 5)
        with path.open("a") as file:
            file.write('{"type":"compacted"}\n')
        self.assertEqual(self.counter.count(self.thread), 6)

    def test_replacement_after_open_never_caches_old_data_under_the_new_file(self):
        path = rollout(self.tmp.name, self.thread, {"type": "compacted"})
        replacement = Path(self.tmp.name) / "replacement.jsonl"
        replacement.write_text(json.dumps({"type": "session_meta", "payload": {"id": self.thread}}) + "\n")
        original_open = Path.open

        def replace_after_open(candidate, *args, **kwargs):
            source = original_open(candidate, *args, **kwargs)
            if candidate == path and replacement.exists():
                replacement.replace(path)
            return source

        with mock.patch.object(Path, "open", replace_after_open):
            self.assertIsNone(self.counter.count(self.thread))
        self.assertEqual(self.counter.count(self.thread), 0)


class CompactionApiTests(RemoteWebFixture):
    def start(self, **kwargs):
        self.rollouts = Path(self.tmp.name) / "sessions"
        self.rollouts.mkdir(exist_ok=True)
        super().start(sessions_root=self.rollouts, **kwargs)

    def setUp(self):
        super().setUp()
        self.call("register", agent="worker")
        self.call("register", session="b", agent="orchestrator")
        self.thread_id = str(uuid.uuid4())
        self.call("bind", thread=self.thread_id)

    def assert_jq(self, expression, value):
        result = subprocess.run(["jq", "-e", expression], input=json.dumps(value),
                                capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)

    def test_orchestrator_status_roster_and_new_thread_use_current_count(self):
        path = rollout(self.rollouts, self.thread_id, {"type": "compacted"})
        self.assert_jq('.compaction_count == 1', self.call("agent-status", session="b", to="worker"))
        self.assert_jq('.agents[] | select(.agent == "worker") | .compaction_count == 1', self.call("agents", session="b"))
        with path.open("a") as file:
            file.write('{"type":"compacted"}\n')
        self.assert_jq('.compaction_count == 2', self.call("agent-status", session="b", to="worker"))
        new_thread = str(uuid.uuid4())
        self.call("bind", thread=new_thread)
        self.assert_jq('.compaction_count == null', self.call("agent-status", session="b", to="worker"))
        rollout(self.rollouts, new_thread)
        self.assert_jq('.compaction_count == 0', self.call("agent-status", session="b", to="worker"))
        self.assert_jq('.agents[] | select(.agent == "worker") | .compaction_count == 0', self.call("agents", session="b"))

    def test_unbound_parent_routed_and_remote_missing_logs_are_unknown(self):
        self.assert_jq('.agents[] | select(.agent == "orchestrator") | .compaction_count == null', self.call("agents", session="b"))
        self.assert_jq('.compaction_count == null', self.call("agent-status", session="b", to="worker"))
        self.call("register", session="remote", host="other-machine", agent="remote-worker")
        self.call("bind", session="remote", host="other-machine", thread=str(uuid.uuid4()))
        self.assert_jq('.compaction_count == null', self.call("agent-status", session="b", to="remote-worker"))
        rollout(self.rollouts, self.thread_id, {"type": "compacted"})
        self.call("register", session="child", agent="child")
        self.call("bind", session="child", parent_session="a", agent_path="/root/child")
        self.assert_jq('.compaction_count == null', self.call("agent-status", session="b", to="child"))
