import subprocess
import os
import tempfile
import threading
import time
import unittest
import uuid
from pathlib import Path

from agent_chat.inbox_diagnostics import (FILE_BYTES, QUEUE_CAPACITY, RECORD_BYTES,
                                          TTL_SECONDS, InboxDiagnostics)


class InboxDiagnosticsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name) / 'private' / 'inbox.jsonl'

    def tearDown(self):
        self.temp.cleanup()

    def wait_lines(self, path, count=1):
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            if path.exists():
                result = subprocess.run(['jq', '-s', '-e', '--argjson', 'count', str(count),
                    'length >= $count and all(.[]; type == "object")', str(path)],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                if result.returncode == 0:
                    return
            time.sleep(.01)
        self.fail('diagnostic sink did not drain')

    def test_trace_uses_fixed_private_schema_and_owner_file_mode(self):
        sink = InboxDiagnostics(self.path)
        trace_id = str(uuid.uuid4())
        trace = sink.trace([trace_id], [('handler', 'start', 'ok', 1.0, 2.0)])
        self.assertIsNotNone(trace)
        self.assertIsNone(sink.trace([trace_id.upper()]))
        trace.mark('dispatch', 'start')
        trace.mark('dispatch', 'end')
        sink.close()
        self.wait_lines(self.path, 3)
        predicate = '''
          [.[].sequence] == [1,2,3] and
          all(.[]; .request_id == $id and
            ((keys - ["request_id","sequence","phase","edge","outcome","wall_time","monotonic_time","elapsed_ms","dropped"]) | length == 0) and
            (["request_id","sequence","phase","edge","outcome","wall_time","monotonic_time","elapsed_ms"] - keys | length == 0) and
            (has("header") | not) and (has("payload") | not)) and
          ([.. | strings, (objects | keys[])] | all(.[]; (contains("header") or contains("payload")) | not))
        '''
        subprocess.run(['jq', '-s', '-e', '--arg', 'id', trace_id, predicate, str(self.path)],
                       check=True, stdout=subprocess.DEVNULL)
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o600)

    def test_ttl_queue_drop_rotation_and_sink_failure_are_isolated(self):
        now = [0.0]
        sink = InboxDiagnostics(self.path, wall=lambda: now[0], monotonic=lambda: now[0])
        trace = sink.trace([str(uuid.uuid4())])
        self.assertIsNotNone(trace)
        now[0] = TTL_SECONDS + .01
        self.assertIsNone(sink.trace([str(uuid.uuid4())]))
        sink.close()

        rotating = InboxDiagnostics(self.path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_bytes(b'x' * (FILE_BYTES - 1))
        rotating.trace([str(uuid.uuid4())]).mark('handler', 'start')
        rotating.close()
        self.wait_lines(self.path)
        backup = self.path.with_name(self.path.name + '.1')
        self.assertTrue(backup.exists())
        self.assertLessEqual(self.path.stat().st_size, FILE_BYTES)
        self.assertLessEqual(backup.stat().st_size, FILE_BYTES)

        bounded = InboxDiagnostics(Path(self.temp.name) / 'bounded.jsonl')
        release = threading.Event()
        entered = threading.Event()
        def block_write(_raw):
            entered.set()
            release.wait(3)
        bounded._write = block_write
        bounded._queue.put_nowait(b'x')
        self.assertTrue(entered.wait(2), 'private sink did not enter the bounded write')
        for _ in range(QUEUE_CAPACITY):
            bounded._queue.put_nowait(b'x')
        bounded.record({'request_id': str(uuid.uuid4()), 'sequence': 1, 'phase': 'handler', 'edge': 'start',
                        'outcome': 'ok', 'wall_time': 0, 'monotonic_time': 0, 'elapsed_ms': 0})
        self.assertGreater(bounded._dropped, 0)
        release.set()
        bounded.close()

        failed = InboxDiagnostics(Path(self.temp.name))
        failed.trace([str(uuid.uuid4())]).mark('handler', 'start')
        failed.close()
        self.assertGreater(failed._sink_failures, 0)
        self.assertEqual(RECORD_BYTES, 4096)
