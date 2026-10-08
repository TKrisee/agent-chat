"""Opt-in application events. Free text and exception messages never reach sinks."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import errno
import json
import logging
import os
from pathlib import Path
import queue
import stat
import threading
import time
import uuid

LEVELS = {'DEBUG': 10, 'INFO': 20, 'WARNING': 30, 'ERROR': 40, 'CRITICAL': 50, 'OFF': 100}
COMPONENTS = frozenset(('agent_chat.server', 'agent_chat.web', 'agent_chat.coordinator',
                        'agent_chat.service', 'agent_chat.cli', 'agent_chat.remote'))
EVENTS = frozenset(('startup', 'shutdown', 'request_start', 'request_complete',
                    'request_rejected', 'operation_start', 'operation_complete',
                    'operation_failed', 'transport_failed', 'configuration_invalid', 'sink_failure'))
ERROR_CATEGORIES = frozenset(('timeout', 'transport', 'authentication', 'authorization',
                            'validation', 'coordination', 'io', 'internal'))
RECORD_BYTES = 4096
QUEUE_CAPACITY = 512


@dataclass(frozen=True)
class LoggingConfig:
    level: str = 'OFF'
    path: str | None = None
    max_bytes: int = 1024 * 1024
    backups: int = 1

    def __post_init__(self):
        if self.level not in LEVELS:
            raise ValueError('invalid logging severity')
        if self.level != 'OFF' and not self.path:
            raise ValueError('enabled logging requires a private file destination')
        if self.path and not Path(self.path).is_absolute():
            raise ValueError('logging destination must be absolute')
        if type(self.max_bytes) is not int or not RECORD_BYTES <= self.max_bytes <= 64 * 1024 * 1024:
            raise ValueError('invalid logging file bound')
        if type(self.backups) is not int or not 0 <= self.backups <= 10:
            raise ValueError('invalid logging retention bound')


def new_correlation_id():
    return str(uuid.uuid4())


def _uuid4(value):
    try:
        parsed = uuid.UUID(value)
        return isinstance(value, str) and parsed.version == 4 and str(parsed) == value
    except (ValueError, TypeError, AttributeError):
        return False


def _run_id(value):
    return (isinstance(value, str) and value.startswith('remote_run_') and
            len(value) == 43 and all(char in '0123456789abcdef' for char in value[11:]))


def error_category(error):
    if isinstance(error, PermissionError): return 'authorization'
    if isinstance(error, TimeoutError): return 'timeout'
    if isinstance(error, ConnectionError): return 'transport'
    if isinstance(error, (ValueError, TypeError)): return 'validation'
    if isinstance(error, OSError): return 'io'
    return 'internal'


class ComponentLogger:
    def __init__(self, runtime, component):
        self._runtime = runtime
        self.name = component
        self.component = component

    def event(self, event, *, level='INFO', correlation_id=None, error_category=None,
              run_id=None, inbox_trace_id=None):
        self._runtime.emit(self.component, event, level, correlation_id, error_category,
                           run_id=run_id, inbox_trace_id=inbox_trace_id)

    def error(self, event, exception, *, correlation_id=None, run_id=None, inbox_trace_id=None):
        self.event(event, level='ERROR', correlation_id=correlation_id,
                   error_category=error_category(exception), run_id=run_id, inbox_trace_id=inbox_trace_id)


class _EventHandler(logging.Handler):
    def __init__(self, runtime):
        super().__init__()
        self.runtime = runtime

    def emit(self, record):
        self.runtime._enqueue(record.msg)


class LoggingRuntime:
    def __init__(self, config):
        self.config = config
        self._closed = False
        self._lock = threading.Lock()
        self._queue = queue.Queue(QUEUE_CAPACITY)
        self.dropped = 0
        self.sink_failures = 0
        self._sequence = 0
        # Isolated Logger instances avoid global/root handlers and stdout.
        self._loggers = {}
        self._worker = None
        if config.level != 'OFF':
            self._worker = threading.Thread(target=self._drain, name='agent-chat-app-log', daemon=True)
            self._worker.start()

    def logger(self, component):
        if component not in COMPONENTS:
            raise ValueError('invalid logging component')
        return ComponentLogger(self, component)

    def emit(self, component, event, level, correlation_id, category, *, run_id=None, inbox_trace_id=None):
        if self._closed or self.config.level == 'OFF': return
        if component not in COMPONENTS or event not in EVENTS or level not in LEVELS or level == 'OFF': return
        if (category is not None and category not in ERROR_CATEGORIES) or LEVELS[level] < LEVELS[self.config.level]: return
        if correlation_id is not None and not _uuid4(correlation_id):
            correlation_id = None
        with self._lock:
            if self._closed: return
            self._sequence += 1
            record = {'timestamp': datetime.now(timezone.utc).isoformat(timespec='milliseconds'),
                      'monotonic_ns': time.monotonic_ns(), 'sequence': self._sequence,
                      'component': component, 'channel': component,
                      'severity': level, 'event': event, 'correlation_id': correlation_id,
                      'error_category': category}
            if _run_id(run_id): record['run_id'] = run_id
            if _uuid4(inbox_trace_id): record['inbox_trace_id'] = inbox_trace_id
            logger = self._loggers.get(component)
            if logger is None:
                logger = logging.Logger(component, LEVELS[self.config.level])
                logger.propagate = False
                logger.addHandler(_EventHandler(self))
                self._loggers[component] = logger
            logger.log(LEVELS[level], record)

    def _enqueue(self, record):
        raw = json.dumps(record, separators=(',', ':'), ensure_ascii=True).encode('ascii') + b'\n'
        if len(raw) > RECORD_BYTES:
            self.dropped += 1
            return
        try: self._queue.put_nowait(raw)
        except queue.Full: self.dropped += 1

    def _validate_existing(self, path):
        try: info = path.lstat()
        except FileNotFoundError: return
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid():
            raise OSError(errno.EPERM, 'unsafe logging destination')

    def _open(self):
        path = Path(self.config.path)
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self._validate_existing(path)
        flags = os.O_WRONLY | os.O_APPEND | os.O_CREAT | getattr(os, 'O_NOFOLLOW', 0)
        fd = os.open(path, flags, 0o600)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid():
                raise OSError(errno.EPERM, 'unsafe logging destination')
            os.fchmod(fd, 0o600)
        except BaseException:
            os.close(fd)
            raise
        return fd

    def _write(self, raw):
        path = Path(self.config.path)
        fd = self._open()
        try:
            if os.fstat(fd).st_size + len(raw) > self.config.max_bytes:
                os.close(fd)
                fd = None
                for index in range(self.config.backups, 0, -1):
                    source = path if index == 1 else Path(str(path) + '.' + str(index - 1))
                    target = Path(str(path) + '.' + str(index))
                    self._validate_existing(source)
                    self._validate_existing(target)
                    try: os.replace(source, target)
                    except FileNotFoundError: pass
                if not self.config.backups: path.unlink()
                fd = self._open()
            view = memoryview(raw)
            while view:
                written = os.write(fd, view)
                if written <= 0: raise OSError('logging write failed')
                view = view[written:]
        finally:
            if fd is not None: os.close(fd)

    def _drain(self):
        while True:
            try: raw = self._queue.get(timeout=.05)
            except queue.Empty:
                if self._closed: return
                continue
            try: self._write(raw)
            except OSError:
                with self._lock: self.sink_failures += 1
            finally: self._queue.task_done()

    def close(self):
        with self._lock: self._closed = True
        if self._worker: self._worker.join(1)


def configure(config=None):
    return LoggingRuntime(config or LoggingConfig())
