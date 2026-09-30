"""Best-effort model metadata from local Codex state and rollout logs."""
from __future__ import annotations

import subprocess
import time
import threading
import sqlite3
from dataclasses import dataclass, field
from contextlib import closing
from pathlib import Path
from typing import Iterable


_QUERY = r'''
fromjson? | if .type == "session_meta" then
  ["id", (.payload.id // "")] | @tsv
elif .type == "turn_context" then
  ["context", (if (.payload.model | type) == "string" then .payload.model else "" end),
   (if (.payload.effort | type) == "string" then .payload.effort else "" end)] | @tsv
else empty end
'''
_MAX_SCAN = 512 * 1024
_LATEST_QUERY = r'''
def consume($line):
  (try ($line | fromjson) catch null) as $record |
  if $record.type == "turn_context" then .latest = $record else . end;
reduce inputs as $line ({latest: null, pending: null};
  (if .pending != null then consume(.pending) else . end) | .pending = $line
) | (if $complete and .pending != null then consume(.pending) else . end) |
.latest | select(. != null) |
["context", (if (.payload.model | type) == "string" then .payload.model else "" end),
 (if (.payload.effort | type) == "string" then .payload.effort else "" end)] | @tsv
'''


@dataclass
class _Log:
    stamp: tuple[int, int] | None = None
    offset: int = 0
    thread_id: str | None = None
    metadata: dict[str, str | None] = field(
        default_factory=lambda: {"model": None, "reasoning_effort": None}
    )


class SessionModels:
    """Read only verified thread identities; unavailable fields remain unknown.

    Typed state database metadata takes priority; exact agent paths are resolved
    only when a single active thread matches. Rollouts provide an ID fallback.
    Initial discovery scans at most the last 512 KiB plus the header of each
    relevant rollout. If the tail has no context, jq streams the full file once
    to locate its latest context. Subsequent reads consume appended complete lines only.
    Directory discovery is refreshed every five seconds.
    """

    def __init__(self, sessions_root: str | Path):
        self.sessions_root = Path(sessions_root).expanduser()
        self._paths: dict[str, Path] = {}
        self._logs: dict[Path, _Log] = {}
        self._discovered_at: float | None = None
        self._lock = threading.Lock()

    def _discover(self) -> None:
        now = time.monotonic()
        if self._discovered_at is not None and now - self._discovered_at < 5:
            return
        self._discovered_at = now
        try:
            for path in self.sessions_root.rglob("rollout-*.jsonl"):
                # Codex rollout filenames end with the 36-character thread UUID.
                thread_id = path.stem[-36:]
                if len(thread_id) == 36:
                    self._paths[thread_id] = path
        except OSError:
            pass

    @staticmethod
    def _records(data: bytes) -> list[list[str]] | None:
        try:
            result = subprocess.run(
                ["jq", "-Rr", _QUERY], input=data, capture_output=True, timeout=2,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            return None
        if result.returncode:
            return None
        return [line.split("\t") for line in result.stdout.decode("utf-8").splitlines()]

    def _read_log(self, path: Path) -> _Log:
        log = self._logs.setdefault(path, _Log())
        try:
            stat = path.stat()
            stamp = (stat.st_size, stat.st_mtime_ns)
            if stamp == log.stamp:
                return log
            reset = log.stamp is None or stat.st_size < log.stamp[0] or (
                log.stamp[0] == stat.st_size and log.stamp != stamp
            )
            with path.open("rb") as source:
                header = source.readline(64 * 1024) if reset else b""
                start = 0 if reset else log.offset
                skipped = stat.st_size - start > _MAX_SCAN
                if skipped:
                    start = stat.st_size - _MAX_SCAN
                    source.seek(start)
                    source.readline()  # discard the possibly partial first line
                    start = source.tell()
                source.seek(start)
                data = source.read(_MAX_SCAN)
            complete = data.rfind(b"\n") + 1
            records = self._records(header + data[:complete])
            if records is None:
                return log
            if reset:
                log.thread_id = None
                log.metadata = {"model": None, "reasoning_effort": None}
            for record in records:
                if record[0] == "id" and len(record) == 2:
                    log.thread_id = record[1] or None
                elif record[0] == "context" and len(record) == 3:
                    log.metadata = {
                        "model": record[1] or None,
                        "reasoning_effort": record[2] or None,
                    }
            if skipped and log.thread_id and not any(
                record[0] == "context" for record in records
            ):
                latest = self._latest_context(path)
                if latest is not None:
                    log.metadata = latest
            log.offset = start + complete
            log.stamp = stamp
        except (OSError, UnicodeError):
            pass
        return log

    @staticmethod
    def _latest_context(path: Path) -> dict[str, str | None] | None:
        try:
            with path.open("rb") as source:
                source.seek(0, 2)
                size = source.tell()
                source.seek(max(0, size - 1))
                complete = source.read(1) == b"\n"
                source.seek(0)
                result = subprocess.run(
                    ["jq", "-Rn", "-r", "--argjson", "complete",
                     "true" if complete else "false", _LATEST_QUERY], stdin=source,
                    capture_output=True, timeout=5, check=False,
                )
            if result.returncode or not result.stdout:
                return None
            record = result.stdout.decode("utf-8").rstrip("\n").split("\t")
            if len(record) == 3:
                return {"model": record[1] or None, "reasoning_effort": record[2] or None}
        except (OSError, UnicodeError, subprocess.TimeoutExpired):
            pass
        return None

    def _database_models(
        self, thread_ids: set[str], agent_paths: set[str]
    ) -> dict[str, dict[str, str | None]]:
        matches: dict[str, list[dict[str, str | None]]] = {}
        try:
            databases = sorted(self.sessions_root.parent.glob("state_*.sqlite"))
        except OSError:
            return {}
        for database in databases:
            try:
                with closing(sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True,
                                             timeout=0.1)) as connection:
                    columns = {row[1] for row in connection.execute("PRAGMA table_info(threads)")}
                    if not {"id", "model", "reasoning_effort", "agent_path", "archived"} <= columns:
                        continue
                    requested = thread_ids | agent_paths
                    if not requested:
                        continue
                    placeholders = ",".join("?" for _ in requested)
                    rows = connection.execute(
                        f"SELECT id, model, reasoning_effort, agent_path, archived FROM threads "
                        f"WHERE id IN ({placeholders}) OR agent_path IN ({placeholders})",
                        (*requested, *requested),
                    )
                    for identity, model, effort, path, archived in rows:
                        metadata = {
                            "model": model if isinstance(model, str) and model else None,
                            "reasoning_effort": effort if isinstance(effort, str) and effort else None,
                        }
                        if identity in thread_ids:
                            matches.setdefault(identity, []).append(metadata)
                        if path in agent_paths and archived == 0:
                            matches.setdefault(path, []).append(metadata)
            except (sqlite3.Error, OSError):
                continue
        return {key: values[0] for key, values in matches.items() if len(values) == 1}

    def read(
        self, thread_ids: Iterable[str], agent_paths: Iterable[str] = ()
    ) -> dict[str, dict[str, str | None]]:
        with self._lock:
            ids = set(thread_ids)
            paths = {path for path in agent_paths if path.startswith("/")}
            result = self._database_models(ids, paths)
            missing = ids - result.keys()
            if missing:
                result.update(self._read(missing))
            return result

    def _read(self, thread_ids: Iterable[str]) -> dict[str, dict[str, str | None]]:
        self._discover()
        result = {}
        for thread_id in set(thread_ids):
            path = self._paths.get(thread_id)
            if path is None:
                continue
            log = self._read_log(path)
            if log.thread_id == thread_id:
                result[thread_id] = dict(log.metadata)
        return result
