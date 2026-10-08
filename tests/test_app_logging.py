import contextlib
import io
import os
from pathlib import Path
import subprocess
import tempfile
import time
import unittest

from agent_chat.app_logging import LoggingConfig, configure, new_correlation_id


class ApplicationLoggingTests(unittest.TestCase):
    def test_disabled_and_machine_stdout(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'events.jsonl'
            output = io.StringIO()
            with contextlib.redirect_stdout(output), contextlib.redirect_stderr(output):
                runtime = configure(LoggingConfig(path=str(path)))
                runtime.logger('agent_chat.cli').event('startup')
                runtime.close()
            self.assertEqual(output.getvalue(), '')
            self.assertFalse(path.exists())

    def test_normal_error_privacy_and_severity(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'events.jsonl'
            runtime = configure(LoggingConfig(level='INFO', path=str(path)))
            logger = runtime.logger('agent_chat.remote')
            correlation = new_correlation_id()
            logger.event('request_start', correlation_id=correlation)
            logger.event('request_complete', level='DEBUG')
            logger.error('transport_failed', ConnectionError('SECRET token payload'), correlation_id=correlation)
            logger.event('SECRET token payload')
            logger.event('request_complete', correlation_id='SECRET token payload')
            runtime.close()
            self.assertNotIn(b'SECRET', path.read_bytes())
            result = subprocess.run(['jq', '-s', '-e', '--arg', 'id', correlation,
                'length == 3 and .[0].event == "request_start" and '
                '.[1].error_category == "transport" and '
                'all(.[0:2][]; .correlation_id == $id and .channel == "agent_chat.remote" and '
                '(.timestamp | endswith("+00:00"))) and '
                '.[1].sequence > .[0].sequence and .[1].monotonic_ns >= .[0].monotonic_ns', str(path)],
                capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_rotation_bounded(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'events.jsonl'
            runtime = configure(LoggingConfig(level='INFO', path=str(path), max_bytes=4096, backups=2))
            for _ in range(90): runtime.logger('agent_chat.server').event('startup')
            runtime.close()
            files = list(Path(directory).iterdir())
            self.assertEqual({item.name for item in files}, {'events.jsonl', 'events.jsonl.1', 'events.jsonl.2'})
            self.assertTrue(all(item.stat().st_size <= 4096 for item in files))

    def test_sink_failure_isolated_and_symlink_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            target = root / 'target'
            target.write_bytes(b'preserve')
            link = root / 'events.jsonl'
            link.symlink_to(target)
            runtime = configure(LoggingConfig(level='ERROR', path=str(link)))
            runtime.logger('agent_chat.server').error('operation_failed', OSError('SECRET'))
            runtime.close()
            self.assertEqual(target.read_bytes(), b'preserve')
            self.assertEqual(runtime.sink_failures, 1)
            # A regular file as parent supplies a deterministic unwritable boundary.
            runtime = configure(LoggingConfig(level='INFO', path=str(target / 'log')))
            runtime.logger('agent_chat.server').event('startup')
            runtime.close()
            self.assertEqual(runtime.sink_failures, 1)

    def test_invalid_configuration(self):
        for values in ({'level': 'SECRET'}, {'level': 'INFO'}, {'path': 'relative'},
                       {'backups': 11}, {'max_bytes': 100}):
            with self.assertRaises(ValueError): LoggingConfig(**values)

    def test_deferred_unwritable_sink_and_retained_correlations(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            blocked = root / 'blocked'
            blocked.write_bytes(b'preserve')
            runtime = configure(LoggingConfig(level='INFO', path=str(blocked / 'events')))
            self.assertEqual(runtime.sink_failures, 0)
            started = time.monotonic()
            runtime.logger('agent_chat.remote').event('request_start')
            self.assertLess(time.monotonic() - started, .5)
            runtime.close()
            self.assertEqual(runtime.sink_failures, 1)
            path = root / 'events'
            runtime = configure(LoggingConfig(level='INFO', path=str(path)))
            self.assertFalse(path.exists())
            logger = runtime.logger('agent_chat.remote')
            run_id = 'remote_run_' + 'a' * 32
            trace_id = new_correlation_id()
            logger.event('operation_start', correlation_id=run_id, run_id=run_id, inbox_trace_id=trace_id)
            logger.event('operation_complete', run_id='remote_run_' + 'A' * 32, inbox_trace_id=trace_id.upper())
            runtime.close()
            result = subprocess.run(['jq', '-s', '-e', '--arg', 'run', run_id, '--arg', 'trace', trace_id,
                'length == 2 and .[0].run_id == $run and .[0].inbox_trace_id == $trace and '
                '.[0].correlation_id == null and (.[1] | has("run_id") | not) and '
                '(.[1] | has("inbox_trace_id") | not)', str(path)], capture_output=True)
            self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == '__main__': unittest.main()
