"""Current-thread compaction counts from verified local Codex rollouts.

Only jq parses JSON. No transcript content is returned or persisted.
"""
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
import os
import subprocess
import threading


_QUERY = r'''
reduce inputs as $line ({id: "", count: 0};
  (try ($line | fromjson) catch null) as $record |
  if ($record | type) != "object" then .
  elif $record.type == "session_meta" and .id == "" then
    .id = (if ($record.payload | type) == "object" then
      (if ($record.payload.id | type) == "string" then $record.payload.id else "" end)
      else "" end)
  elif $record.type == "compacted" then .count += 1
  else . end
) | [.id, .count] | @tsv
'''


@dataclass
class _Log:
    stamp: tuple[int, int, int, int] | None = None
    offset: int = 0
    thread_id: str = ""
    count: int = 0


class CompactionCounter:
    """Read complete appended records; unavailable evidence remains unknown."""
    batch_bytes = 8 * 1024 * 1024

    def __init__(self, sessions_root):
        self.sessions_root = Path(sessions_root).expanduser()
        self._paths: dict[str, Path] = {}
        self._logs: dict[Path, _Log] = {}
        self._lock = threading.Lock()

    @staticmethod
    def _records(data):
        try:
            result = subprocess.run(["jq", "-Rnr", _QUERY], input=data,
                                    capture_output=True, check=False, timeout=10)
        except (OSError, subprocess.TimeoutExpired):
            return None
        if result.returncode:
            return None
        thread_id, count = result.stdout.decode("utf-8").rstrip("\n").split("\t")
        return thread_id, int(count)

    def _read(self, path):
        log = self._logs.setdefault(path, _Log())
        try:
            with path.open("rb") as source:
                stat = os.fstat(source.fileno())
                stamp = (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)
                if stamp == log.stamp:
                    return log
                reset = (log.stamp is None or stamp[:2] != log.stamp[:2]
                         or stamp[2] < log.stamp[2]
                         or (stamp[2] == log.stamp[2] and stamp[3] != log.stamp[3]))
                offset, thread_id, count = (0, "", 0) if reset else (log.offset, log.thread_id, log.count)
                source.seek(offset)
                while True:
                    data = source.read(self.batch_bytes)
                    if not data:
                        break
                    if not data.endswith(b"\n"):
                        data += source.readline()
                    complete = data.rfind(b"\n") + 1
                    if not complete:
                        break
                    records = self._records(data[:complete])
                    if records is None:
                        return None
                    identity, added = records
                    thread_id = thread_id or identity
                    count += added
                    offset += complete
                    if complete != len(data):
                        break
                # A changing file must be retried rather than cached at a stale offset.
                after = path.stat()
                if stamp != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
                    return None
        except OSError:
            return None
        log.stamp, log.offset, log.thread_id, log.count = stamp, offset, thread_id, count
        return log

    def count(self, thread_id):
        if not thread_id:
            return None
        with self._lock:
            path = self._paths.get(thread_id)
            if path is not None:
                log = self._read(path)
                if log is not None and log.thread_id == thread_id:
                    return log.count
                self._paths.pop(thread_id, None)
            try:
                # Match a filename suffix literally, then verify its metadata identity.
                paths = self.sessions_root.rglob("*.jsonl")
                for candidate in paths:
                    if not candidate.stem.endswith("-" + thread_id):
                        continue
                    log = self._read(candidate)
                    if log is not None and log.thread_id == thread_id:
                        self._paths[thread_id] = candidate
                        return log.count
            except OSError:
                pass
        return None


@lru_cache(maxsize=8)
def counter(sessions_root=None):
    return CompactionCounter(sessions_root or "~/.codex/sessions")
