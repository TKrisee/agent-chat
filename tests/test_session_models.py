import json
import shutil
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from agent_chat.session_models import SessionModels


THREAD = "00000000-0000-0000-0000-000000000001"
OTHER = "00000000-0000-0000-0000-000000000002"


def line(kind, **payload):
    return json.dumps({"type": kind, "payload": payload}) + "\n"


@unittest.skipUnless(shutil.which("jq"), "jq is required")
class SessionModelsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "sessions"
        self.root.mkdir()
        self.path = self.root / f"rollout-2026-09-30-{THREAD}.jsonl"
        self.reader = SessionModels(self.root)

    def write(self, contents):
        self.path.write_text(contents)

    def test_latest_context_and_incremental_cache(self):
        self.write(line("session_meta", id=THREAD) +
                   line("turn_context", model="older", effort="low") +
                   line("turn_context", model="gpt-6.1-sol", effort="high"))
        expected = {THREAD: {"model": "gpt-6.1-sol", "reasoning_effort": "high"}}
        self.assertEqual(self.reader.read([THREAD]), expected)
        with patch.object(self.reader, "_records", wraps=self.reader._records) as records:
            self.assertEqual(self.reader.read([THREAD]), expected)
            records.assert_not_called()
            with self.path.open("a") as stream:
                stream.write(line("turn_context", model="changed", effort="medium"))
            self.assertEqual(self.reader.read([THREAD])[THREAD]["model"], "changed")
            self.assertNotIn(b"session_meta", records.call_args.args[0])

    def test_partial_and_malformed_lines(self):
        context = line("turn_context", model="gpt-6.1-sol", effort="high")
        self.write(line("session_meta", id=THREAD) + "not json\n" + context[:-3])
        self.assertEqual(self.reader.read([THREAD])[THREAD],
                         {"model": None, "reasoning_effort": None})
        with self.path.open("a") as stream:
            stream.write(context[-3:])
        self.assertEqual(self.reader.read([THREAD])[THREAD]["reasoning_effort"], "high")

    def test_filename_does_not_establish_identity(self):
        self.write(line("session_meta", id=OTHER) +
                   line("turn_context", model="parent", effort="high"))
        self.assertEqual(self.reader.read([THREAD, OTHER]), {})

    def test_unknown_fields_and_no_guessed_defaults(self):
        self.write(line("session_meta", id=THREAD))
        self.assertEqual(self.reader.read([THREAD])[THREAD],
                         {"model": None, "reasoning_effort": None})
        with self.path.open("a") as stream:
            stream.write(line("turn_context", model="known"))
        self.assertEqual(self.reader.read([THREAD])[THREAD],
                         {"model": "known", "reasoning_effort": None})
        self.assertEqual(self.reader.read([OTHER]), {})

    def test_large_log_uses_header_identity_and_tail_context(self):
        self.write(line("session_meta", id=THREAD) + ("{}\n" * 200000) +
                   line("turn_context", model="latest", effort="high"))
        self.assertEqual(self.reader.read([THREAD])[THREAD]["model"], "latest")

    def test_jq_unavailable_is_best_effort(self):
        self.write(line("session_meta", id=THREAD))
        with patch("agent_chat.session_models.subprocess.run", side_effect=FileNotFoundError):
            self.assertEqual(self.reader.read([THREAD]), {})

    def test_context_before_large_tail_and_large_append(self):
        self.write(line("session_meta", id=THREAD) +
                   line("turn_context", model="first", effort="high") + "{}\n" * 200000)
        self.assertEqual(self.reader.read([THREAD])[THREAD]["model"], "first")
        with self.path.open("a") as stream:
            stream.write(line("turn_context", model="second", effort="low") +
                         "malformed\n" + "{}\n" * 200000 +
                         line("turn_context", model="unfinished", effort="high").rstrip("\n"))
        self.assertEqual(self.reader.read([THREAD])[THREAD],
                         {"model": "second", "reasoning_effort": "low"})

    def database(self, rows):
        connection = sqlite3.connect(self.root.parent / "state_5.sqlite")
        self.addCleanup(connection.close)
        connection.execute("CREATE TABLE threads (id TEXT, model TEXT, reasoning_effort TEXT, "
                           "agent_path TEXT, archived INTEGER)")
        connection.executemany("INSERT INTO threads VALUES (?, ?, ?, ?, ?)", rows)
        connection.commit()

    def test_database_direct_thread_and_unique_active_child(self):
        path = "/root/exact_child"
        self.database([(THREAD, "actual", "high", "/root", 0),
                       (OTHER, "child", "medium", path, 0),
                       ("archived", "old", "low", path, 1)])
        self.write(line("session_meta", id=THREAD) +
                   line("turn_context", model="outdated", effort="low"))
        self.assertEqual(self.reader.read([THREAD], [path]), {
            THREAD: {"model": "actual", "reasoning_effort": "high"},
            path: {"model": "child", "reasoning_effort": "medium"},
        })

    def test_database_ambiguous_child_is_unknown(self):
        path = "/root/exact_child"
        self.database([(THREAD, "one", "high", path, 0),
                       (OTHER, "two", "low", path, 0)])
        self.assertEqual(self.reader.read([], [path]), {})

    def test_unavailable_database_schema_uses_logs(self):
        with sqlite3.connect(self.root.parent / "state_5.sqlite") as connection:
            connection.execute("CREATE TABLE threads (id TEXT)")
        self.write(line("session_meta", id=THREAD) +
                   line("turn_context", model="from-log", effort="high"))
        self.assertEqual(self.reader.read([THREAD])[THREAD]["model"], "from-log")
