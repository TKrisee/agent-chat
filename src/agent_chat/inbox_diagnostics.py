"""Private, short-lived timing records for explicitly traced inbox requests.

This module deliberately has no reader or coordinator/database integration.  It
is an owner-selected server sink, not a general observability facility.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import queue
import threading
import time
import uuid


TRACE_HEADER = 'X-Agent-Chat-Inbox-Trace'
TTL_SECONDS = 300
QUEUE_CAPACITY = 512
FILE_BYTES = 1024 * 1024
RECORD_BYTES = 4096
_PHASES = frozenset(('handler', 'project', 'body', 'dispatch', 'transaction_acquire',
                     'transaction_held', 'inbox_query', 'response'))
_EDGES = frozenset(('start', 'end'))
_OUTCOMES = frozenset(('ok', 'error'))


class InboxDiagnostics:
    """One process-local JSONL timing sink with a five-minute capture window."""

    def __init__(self, path, *, wall=time.time, monotonic=time.monotonic):
        self.path = Path(path)
        self._wall, self._monotonic = wall, monotonic
        self._started_monotonic = monotonic()
        self._deadline = self._started_monotonic + TTL_SECONDS
        self._queue = queue.Queue(maxsize=QUEUE_CAPACITY)
        self._lock = threading.Lock()
        self._closed = False
        self._dropped = 0
        self._sink_failures = 0
        self._worker = threading.Thread(target=self._drain, name='agent-chat-inbox-diagnostics', daemon=True)
        self._worker.start()

    def trace(self, header_values, timeline=(), *, started_wall=None, started_monotonic=None):
        """Start a trace only for one exact UUIDv4 header after inbox parsing."""
        if not self.active or not isinstance(header_values, list) or len(header_values) != 1:
            return None
        try:
            value = uuid.UUID(header_values[0])
        except (ValueError, AttributeError, TypeError):
            return None
        if value.version != 4 or str(value) != header_values[0]:
            return None
        trace = InboxTrace(self, str(value), started_wall=started_wall, started_monotonic=started_monotonic)
        for phase, edge, outcome, wall_time, monotonic_time in timeline:
            trace.mark(phase, edge, outcome, wall_time=wall_time, monotonic_time=monotonic_time)
        return trace

    @property
    def active(self):
        return not self._closed and self._monotonic() < self._deadline

    def record(self, item):
        """Queue a fixed, privacy-safe record without affecting a request."""
        if not self.active:
            return
        try:
            raw = json.dumps(item, separators=(',', ':'), ensure_ascii=True).encode('ascii')
        except (TypeError, ValueError):
            return
        if len(raw) > RECORD_BYTES:
            self._drop()
            return
        with self._lock:
            dropped = self._dropped
            if dropped:
                item = dict(item, dropped=dropped)
                raw = json.dumps(item, separators=(',', ':'), ensure_ascii=True).encode('ascii')
                if len(raw) > RECORD_BYTES:
                    self._dropped += 1
                    return
            try:
                self._queue.put_nowait(raw)
            except queue.Full:
                self._dropped += 1
                return
            self._dropped = 0

    def _drop(self):
        with self._lock:
            self._dropped += 1

    def _open(self):
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        flags = os.O_WRONLY | os.O_APPEND | os.O_CREAT
        if hasattr(os, 'O_NOFOLLOW'):
            flags |= os.O_NOFOLLOW
        fd = os.open(self.path, flags, 0o600)
        try:
            os.fchmod(fd, 0o600)
            if os.fstat(fd).st_nlink != 1:
                raise OSError('diagnostic sink must not have multiple links')
        except BaseException:
            os.close(fd)
            raise
        return fd

    def _write(self, raw):
        try:
            fd = self._open()
            try:
                if os.fstat(fd).st_size + len(raw) + 1 > FILE_BYTES:
                    os.close(fd)
                    backup = self.path.with_name(self.path.name + '.1')
                    os.replace(self.path, backup)
                    fd = self._open()
                os.write(fd, raw + b'\n')
            finally:
                os.close(fd)
        except OSError:
            with self._lock:
                self._sink_failures += 1

    def _drain(self):
        while True:
            try:
                raw = self._queue.get(timeout=.1)
            except queue.Empty:
                if self._closed:
                    return
                continue
            self._write(raw)
            self._queue.task_done()

    def close(self):
        self._closed = True
        self._worker.join(1)


class InboxTrace:
    """A request-local sequence writer whose fields never contain request data."""

    def __init__(self, sink, request_id, *, started_wall=None, started_monotonic=None):
        self._sink = sink
        self._request_id = request_id
        self._started_wall = sink._wall() if started_wall is None else started_wall
        self._started_monotonic = sink._monotonic() if started_monotonic is None else started_monotonic
        self._sequence = 0

    def mark(self, phase, edge, outcome='ok', *, wall_time=None, monotonic_time=None):
        if phase not in _PHASES or edge not in _EDGES or outcome not in _OUTCOMES:
            return
        wall_time = self._sink._wall() if wall_time is None else wall_time
        monotonic_time = self._sink._monotonic() if monotonic_time is None else monotonic_time
        self._sequence += 1
        self._sink.record({'request_id': self._request_id, 'sequence': self._sequence,
                           'phase': phase, 'edge': edge, 'outcome': outcome,
                           'wall_time': wall_time, 'monotonic_time': monotonic_time,
                           'elapsed_ms': max(0, (monotonic_time - self._started_monotonic) * 1000)})
