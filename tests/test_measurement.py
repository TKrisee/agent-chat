"""Focused contract tests for local, jq-parsed measurements."""
import json
import pathlib
import sqlite3
import tempfile
import unittest

from agent_chat.measurement import MeasurementStore


class MeasurementStoreTests(unittest.TestCase):
    def fixture(self, root, thread, records):
        path = pathlib.Path(root) / f"rollout-{thread}.jsonl"
        path.write_text("".join(json.dumps(item) + "\n" for item in records))
        return path

    def project_db(self, directory, session="session-a", thread="thread-a"):
        path = pathlib.Path(directory) / "project.sqlite3"
        db = sqlite3.connect(path)
        db.executescript("CREATE TABLE sessions(id TEXT, agent TEXT); CREATE TABLE bridge_bindings(session_id TEXT, thread_id TEXT, parent_session TEXT);")
        db.execute("INSERT INTO sessions VALUES(?,?)", (session, "main"))
        db.execute("INSERT INTO bridge_bindings VALUES(?,?,NULL)", (session, thread))
        db.commit(); db.close()
        return path

    def assert_jq(self, expression, value):
        import subprocess
        self.assertEqual(subprocess.run(["jq", "-e", expression], input=json.dumps(value), text=True,
                                        stdout=subprocess.DEVNULL).returncode, 0)

    def test_owned_new_records_are_deduplicated_and_partial_is_visible(self):
        with tempfile.TemporaryDirectory() as temp:
            project = self.project_db(temp)
            rollout = self.fixture(temp, "thread-a", [{"type": "session_meta", "payload": {"id": "thread-a", "session_id": "thread-a"}}])
            store = MeasurementStore(pathlib.Path(temp) / "base.sqlite3", sessions_root=temp)
            store.interval_seconds = 3600
            active = store.start("p", project, 60)["active"]
            store._establish(active["id"])
            with rollout.open("a") as file:
                file.write(json.dumps({"type": "token_usage_record", "payload": {"thread_id": "thread-a", "session_id": "thread-a", "response_id": "r1", "usage": {"input_tokens": 10, "cached_input_tokens": 4, "output_tokens": 3}}}) + "\n")
                file.write(json.dumps({"type": "token_usage_record", "payload": {"thread_id": "thread-a", "session_id": "thread-a", "response_id": "r1", "usage": {"input_tokens": 999}}}) + "\n")
            store._sample(active["id"])
            report = store.status("p")["active"]
            self.assert_jq('.agents[0] | .response_count == 1 and .input_tokens == 10 and .fresh_input_tokens == 6 and .reasoning_output_tokens == null and .usage_status == "partial"', report)
            store.close()

    def test_unbound_child_is_not_charged_to_its_parent(self):
        with tempfile.TemporaryDirectory() as temp:
            project = self.project_db(temp)
            db = sqlite3.connect(project)
            db.execute("INSERT INTO sessions VALUES(?,?)", ("child", "child"))
            db.execute("INSERT INTO bridge_bindings VALUES(?,?,?)", ("child", None, "session-a"))
            db.commit(); db.close()
            store = MeasurementStore(pathlib.Path(temp) / "base.sqlite3", sessions_root=temp)
            report = store.start("p", project, 60)["active"]
            self.assert_jq('.agents | length == 1 and .[0].session_id == "session-a"', report)
            store.close()
