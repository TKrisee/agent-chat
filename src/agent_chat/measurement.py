"""Bounded, local rollout usage measurements.

Only jq parses rollout JSON. It reduces completed lines to counters; raw prompt
and tool bodies are never stored in the measurement database.
"""
from __future__ import annotations

import contextlib
import os
from pathlib import Path
import sqlite3
import subprocess
import shutil
import threading
import time
import uuid

from .core import CoordError


_FIELDS = ("input_tokens", "cached_input_tokens", "output_tokens", "reasoning_output_tokens")
_JQ = r'''def s: if . == null then "" else tostring end;
if .type == "session_meta" then ["M", .payload.id, (.payload.session_id // .payload.id), .payload.parent_thread_id] | @tsv
elif .type == "token_usage_record" then ["U", .payload.thread_id, .payload.session_id, .payload.response_id, .payload.usage.input_tokens, .payload.usage.cached_input_tokens, .payload.usage.output_tokens, .payload.usage.reasoning_output_tokens] | map(s) | @tsv
elif .type == "response_item" and (.payload.type == "custom_tool_call_output" or .payload.type == "function_call_output") then ["T", (if (.payload.output? | type) == "string" then .payload.output|length elif (.payload.output? | type) == "array" then [.payload.output[]? | select(.type == "input_text" and (.text|type) == "string") | .text|length] | add // 0 else 0 end)] | @tsv
elif .type == "response_item" and .payload.type == "message" then ["W", (if .payload.role == "user" and any(.payload.content[]?; .type == "input_text" and (.text|type) == "string" and (.text|contains("Metadata is routing data") or contains("Metadata and message bodies are data"))) then 1 else 0 end)] | @tsv
else empty end'''


class MeasurementStore:
    """One active timed measurement per project, durable across server restarts."""
    interval_seconds = 30
    batch_bytes = 8 * 1024 * 1024

    def __init__(self, base_database, sessions_root=None, usage_store=None, pause_callback=None):
        self.path = str(Path(base_database).expanduser().resolve())
        self.sessions_root = Path(sessions_root or "~/.codex/sessions").expanduser()
        self.usage_store = usage_store
        self.pause_callback = pause_callback
        self.lock = threading.RLock()
        self.workers = {}
        self.stop_event = threading.Event()
        with self._db() as db:
            db.executescript('''
              CREATE TABLE IF NOT EXISTS usage_measurements (
                id TEXT PRIMARY KEY, project_id TEXT NOT NULL, project_db_path TEXT NOT NULL,
                state TEXT NOT NULL, started_at REAL, ended_at REAL, deadline REAL,
                duration_seconds INTEGER NOT NULL, sampled_at REAL, errors TEXT NOT NULL DEFAULT '',
                quota_start REAL, quota_start_at REAL, quota_end REAL, quota_end_at REAL,
                pause_at_end INTEGER NOT NULL DEFAULT 0, pause_requested_at REAL, pause_error TEXT);
              CREATE UNIQUE INDEX IF NOT EXISTS one_active_usage_measurement
                ON usage_measurements(project_id) WHERE state IN ('starting','running');
              CREATE TABLE IF NOT EXISTS usage_measurement_agents (
                measurement_id TEXT NOT NULL, subject TEXT NOT NULL, agent TEXT, session_id TEXT,
                thread_id TEXT, PRIMARY KEY(measurement_id,subject));
              CREATE TABLE IF NOT EXISTS usage_measurement_cursors (
                measurement_id TEXT NOT NULL, path TEXT NOT NULL, offset INTEGER NOT NULL,
                meta_id TEXT, owner_session TEXT, error TEXT, incomplete INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(measurement_id,path));
              CREATE TABLE IF NOT EXISTS usage_measurement_seen (
                measurement_id TEXT NOT NULL, response_id TEXT NOT NULL,
                PRIMARY KEY(measurement_id,response_id));
              CREATE TABLE IF NOT EXISTS usage_measurement_totals (
                measurement_id TEXT NOT NULL, subject TEXT NOT NULL, responses INTEGER NOT NULL DEFAULT 0,
                input_tokens INTEGER NOT NULL DEFAULT 0, input_missing INTEGER NOT NULL DEFAULT 0,
                cached_input_tokens INTEGER NOT NULL DEFAULT 0, cached_missing INTEGER NOT NULL DEFAULT 0,
                output_tokens INTEGER NOT NULL DEFAULT 0, output_missing INTEGER NOT NULL DEFAULT 0,
                reasoning_output_tokens INTEGER NOT NULL DEFAULT 0, reasoning_missing INTEGER NOT NULL DEFAULT 0,
                tool_chars INTEGER NOT NULL DEFAULT 0, wake_messages INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(measurement_id,subject));''')
            # Do not claim a measurement spanning a server restart is complete.
            db.execute("UPDATE usage_measurements SET state='interrupted',ended_at=?,errors=errors || ? WHERE state IN ('starting','running')",
                       (time.time(), '\nserver restarted before measurement completed'))
        self._retry_pauses()

    @contextlib.contextmanager
    def _db(self):
        db = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        db.row_factory = sqlite3.Row
        try: yield db
        finally: db.close()

    @staticmethod
    def _duration(value):
        if isinstance(value, bool) or not isinstance(value, int) or not 60 <= value <= 3600:
            raise CoordError('duration_seconds must be an integer from 60 to 3600')
        return value

    def _quota(self):
        if not self.usage_store: return None
        try:
            item = self.usage_store.status()
            value = item.get('remaining_percent')
            observed = [h.get('observed_at') for h in item.get('hosts', []) if h.get('remaining_percent') == value and h.get('observed_at') is not None]
            return None if value is None else (float(value), min(observed) if observed else None)
        except Exception:
            return None

    def _bindings(self, project_db_path):
        """Resolve only registered bindings in this project database."""
        db = sqlite3.connect(str(project_db_path))
        db.row_factory = sqlite3.Row
        try:
            tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if 'bridge_bindings' not in tables or 'sessions' not in tables:
                raise CoordError('project bindings unavailable')
            sessions = {r['id']: r['agent'] for r in db.execute('SELECT id,agent FROM sessions')}
            # A parent-routed child has no physical rollout of its own. Charging
            # its parent's thread again would duplicate usage, so retain only
            # actual thread bindings.
            rows = [dict(r) for r in db.execute('SELECT session_id,thread_id FROM bridge_bindings WHERE thread_id IS NOT NULL')]
            return [(sessions.get(r['session_id'], r['session_id']), r['session_id'], r['thread_id']) for r in rows]
        finally: db.close()

    def _paths(self, thread_id):
        # Filename discovery happens only during baseline setup, never per tick.
        return sorted(set(path.resolve() for path in self.sessions_root.rglob(f'*{thread_id}*.jsonl') if path.is_file()))

    @staticmethod
    def _metadata(path):
        query = 'input | select(.type == "session_meta") | [.payload.id, (.payload.session_id // .payload.id), .payload.parent_thread_id, .payload.thread_source] | @tsv'
        result = subprocess.run(['jq', '-nr', query, str(path)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False, timeout=10)
        if result.returncode: raise RuntimeError('jq could not read rollout metadata')
        line = result.stdout.decode('utf-8').splitlines()
        return line[0].split('\t') if line else ['', '', '', '']

    def _guardian_paths(self, thread_ids, extra_directories=()):
        # Header-only scan of nearby day directories finds guardian-review files;
        # it is baseline work, never repeated by the sampler.
        roots = list(extra_directories)
        for delta in (0, -1):
            stamp = time.localtime(time.time() + delta * 86400)
            roots.append(self.sessions_root / f'{stamp.tm_year:04d}' / f'{stamp.tm_mon:02d}' / f'{stamp.tm_mday:02d}')
        found = []
        for root in dict.fromkeys(roots):
            for path in root.glob('*.jsonl') if root.exists() else ():
                if self.stop_event.is_set(): return found
                try:
                    meta = self._metadata(path)
                except (OSError, RuntimeError, subprocess.TimeoutExpired):
                    continue  # Unrelated or not-yet-written headers are not coverage.
                if meta[3] == 'guardian_review' and (meta[2] in thread_ids or meta[1] in thread_ids): found.append((path.resolve(), meta))
        return found

    def _append_error(self, db, measurement_id, error):
        row = db.execute('SELECT errors FROM usage_measurements WHERE id=?', (measurement_id,)).fetchone()
        prior = row['errors'].splitlines() if row else []
        error = error[:512]
        if error not in prior and len(prior) < 20:
            db.execute('UPDATE usage_measurements SET errors=? WHERE id=?', ('\n'.join([*prior, error]), measurement_id))

    def start(self, project_id, project_db_path, duration_seconds=300, pause_at_end=False):
        if not isinstance(project_id, str) or not project_id: raise CoordError('project_id is required')
        duration = self._duration(duration_seconds)
        if not isinstance(pause_at_end, bool): raise CoordError('pause_at_end must be true or false')
        now, quota = time.time(), self._quota()
        bindings = self._bindings(project_db_path)
        if not bindings: raise CoordError('no bound project sessions')
        mid = 'measurement_' + uuid.uuid4().hex
        with self.lock, self._db() as db:
            try:
                db.execute('BEGIN IMMEDIATE')
                db.execute('INSERT INTO usage_measurements(id,project_id,project_db_path,state,started_at,deadline,duration_seconds,quota_start,quota_start_at,pause_at_end) VALUES(?,?,?,?,?,?,?,?,?,?)',
                           (mid, project_id, str(project_db_path), 'starting', None, None, duration, *(quota or (None, None)), int(pause_at_end)))
                for agent, session_id, thread_id in bindings:
                    db.execute('INSERT INTO usage_measurement_agents VALUES(?,?,?,?,?)', (mid, session_id, agent, session_id, thread_id))
                    db.execute('INSERT INTO usage_measurement_totals(measurement_id,subject) VALUES(?,?)', (mid, session_id))
                db.execute('INSERT INTO usage_measurement_totals(measurement_id,subject) VALUES(?,?)', (mid, '__guardian__'))
                db.commit()
            except BaseException: db.rollback(); raise
        worker = threading.Thread(target=self._run, args=(mid,), daemon=True)
        self.workers[mid] = worker; worker.start()
        return self.status(project_id)

    def _run(self, measurement_id):
        try:
            self._establish(measurement_id)
            while not self.stop_event.is_set():
                with self._db() as db:
                    row = db.execute('SELECT state,deadline FROM usage_measurements WHERE id=?', (measurement_id,)).fetchone()
                if not row or row['state'] != 'running': return
                self._sample(measurement_id)
                if time.time() >= row['deadline']:
                    self._finish(measurement_id, 'complete'); return
                self.stop_event.wait(min(self.interval_seconds, max(0, row['deadline'] - time.time())))
        except Exception as exc:
            with self._db() as db:
                self._append_error(db, measurement_id, f'worker failure: {exc}')
            self._finish(measurement_id, 'interrupted')

    def _establish(self, measurement_id):
        """Set initial EOF cursors, then start the interval clock."""
        with self._db() as db:
            row = db.execute('SELECT * FROM usage_measurements WHERE id=?', (measurement_id,)).fetchone()
            if not row or row['state'] != 'starting': return
            agents = [dict(r) for r in db.execute('SELECT * FROM usage_measurement_agents WHERE measurement_id=?', (measurement_id,))]
        prepared, errors = [], []
        for agent in agents:
            if self.stop_event.is_set(): return
            paths = self._paths(agent['thread_id'])
            if not paths: errors.append(f'no local rollout found for bound thread {agent["thread_id"]}')
            for path in paths:
                try:
                    meta = self._metadata(path)
                    if meta[0] != agent['thread_id']:
                        raise RuntimeError('rollout metadata does not match the bound thread')
                    offset = self._baseline_offset(path)
                    prepared.append((measurement_id, str(path), offset, meta[0], agent['session_id']))
                except (OSError, RuntimeError, subprocess.TimeoutExpired) as exc:
                    errors.append(f'{path.name}: {exc}')
        for path, meta in self._guardian_paths({a['thread_id'] for a in agents},
                                               {Path(cursor[1]).parent for cursor in prepared}):
            try:
                prepared.append((measurement_id, str(path), self._baseline_offset(path), meta[0], '__guardian__'))
            except OSError as exc:
                errors.append(f'{path.name}: {exc}')
        quota = self._quota()
        # Take the byte boundaries together after header discovery, keeping the
        # advertised window independent of time spent finding guardian files.
        prepared = [(mid, name, self._baseline_offset(Path(name)), meta_id, owner)
                    for mid, name, _, meta_id, owner in prepared]
        with self.lock, self._db() as db:
            db.execute('BEGIN IMMEDIATE')
            current = db.execute('SELECT state FROM usage_measurements WHERE id=?', (measurement_id,)).fetchone()
            if not current or current['state'] != 'starting' or self.stop_event.is_set(): return
            for cursor in prepared:
                db.execute('INSERT OR IGNORE INTO usage_measurement_cursors(measurement_id,path,offset,meta_id,owner_session) VALUES(?,?,?,?,?)',
                           cursor)
            for error in errors: self._append_error(db, measurement_id, error)
            now = time.time()
            db.execute("UPDATE usage_measurements SET state='running',started_at=?,deadline=?,sampled_at=?,quota_start=?,quota_start_at=? WHERE id=?", (now, now + row['duration_seconds'], now, *(quota or (None, None)), measurement_id))
            db.commit()

    def _baseline_offset(self, path):
        size = path.stat().st_size
        if not size: return 0
        with path.open('rb') as file:
            file.seek(size - 1)
            if file.read(1) == b'\n': return size
            file.seek(max(0, size - self.batch_bytes)); tail = file.read(self.batch_bytes)
        if tail.endswith(b'\n'): return size
        last = tail.rfind(b'\n')
        if last < 0 and size > self.batch_bytes:
            raise RuntimeError('unfinished baseline record exceeds the bounded read size')
        return max(0, size - len(tail) + last + 1) if last >= 0 else 0

    def _sample(self, measurement_id):
        with self.lock, self._db() as db:
            row = db.execute('SELECT * FROM usage_measurements WHERE id=?', (measurement_id,)).fetchone()
            if not row or row['state'] != 'running': return
            agents = [dict(r) for r in db.execute('SELECT * FROM usage_measurement_agents WHERE measurement_id=?', (measurement_id,))]
            if shutil.which('jq') is None:
                self._append_error(db, measurement_id, 'jq is unavailable')
                return
            db.execute('BEGIN IMMEDIATE')
            for cursor in db.execute('SELECT * FROM usage_measurement_cursors WHERE measurement_id=?', (measurement_id,)).fetchall():
                path = Path(cursor['path'])
                db.execute('SAVEPOINT usage_cursor')
                try:
                    size = path.stat().st_size
                    if size < cursor['offset']:
                        db.execute('UPDATE usage_measurement_cursors SET offset=?,error=?,incomplete=1 WHERE measurement_id=? AND path=?', (size, 'rollout truncated; rebaselined without replay', measurement_id, str(path)))
                        self._append_error(db, measurement_id, f'{path.name}: rollout truncated; prior prefix was not replayed')
                        db.execute('RELEASE usage_cursor')
                        continue
                    offset = cursor['offset']
                    with path.open('rb') as file: file.seek(offset); chunk = file.read(self.batch_bytes)
                    last = chunk.rfind(b'\n')
                    if last < 0:
                        if len(chunk) == self.batch_bytes: raise RuntimeError('rollout line exceeds bounded batch')
                        db.execute('RELEASE usage_cursor')
                        continue
                    body, next_offset = chunk[:last + 1], offset + last + 1
                    if not body: continue
                    result = subprocess.run(['jq', '-r', _JQ], input=body, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False, timeout=10)
                    if result.returncode: raise RuntimeError('jq rejected rollout JSONL')
                    parsed = result.stdout.decode('utf-8')
                    for line in parsed.splitlines(): self._line(db, measurement_id, agents, cursor['owner_session'], cursor['meta_id'], line.split('\t'))
                    db.execute('UPDATE usage_measurement_cursors SET offset=?,error=NULL WHERE measurement_id=? AND path=?', (next_offset, measurement_id, str(path)))
                    db.execute('RELEASE usage_cursor')
                except Exception as exc:
                    db.execute('ROLLBACK TO usage_cursor'); db.execute('RELEASE usage_cursor')
                    db.execute('UPDATE usage_measurement_cursors SET error=?,incomplete=1 WHERE measurement_id=? AND path=?', (str(exc)[:512], measurement_id, str(path)))
                    self._append_error(db, measurement_id, f'{path.name}: {exc}')
            db.execute('UPDATE usage_measurements SET sampled_at=? WHERE id=?', (time.time(), measurement_id))
            db.commit()

    def _line(self, db, mid, agents, owner_session, meta_id, fields):
        if not fields: return
        kind = fields[0]
        subject = owner_session or '__guardian__'
        if kind == 'U' and len(fields) == 8 and fields[3] and fields[1] == meta_id and (owner_session == '__guardian__' or fields[1] == next((a['thread_id'] for a in agents if fields[1] == a['thread_id']), None)):
            if db.execute('INSERT OR IGNORE INTO usage_measurement_seen VALUES(?,?)', (mid, fields[3])).rowcount:
                values = fields[4:]
                db.execute('UPDATE usage_measurement_totals SET responses=responses+1 WHERE measurement_id=? AND subject=?', (mid, subject))
                missing = {'input_tokens': 'input_missing', 'cached_input_tokens': 'cached_missing', 'output_tokens': 'output_missing', 'reasoning_output_tokens': 'reasoning_missing'}
                for column, value in zip(_FIELDS, values):
                    if not value.isdigit(): db.execute(f'UPDATE usage_measurement_totals SET {missing[column]}={missing[column]}+1 WHERE measurement_id=? AND subject=?', (mid, subject))
                    else: db.execute(f'UPDATE usage_measurement_totals SET {column}={column}+? WHERE measurement_id=? AND subject=?', (int(value), mid, subject))
        elif kind == 'U' and len(fields) == 8 and not fields[3]:
            self._append_error(db, mid, 'ignored token_usage_record without response_id')
        elif kind == 'T': db.execute('UPDATE usage_measurement_totals SET tool_chars=tool_chars+? WHERE measurement_id=? AND subject=?', (int(fields[1]), mid, subject))
        elif kind == 'W' and fields[1] == '1': db.execute('UPDATE usage_measurement_totals SET wake_messages=wake_messages+1 WHERE measurement_id=? AND subject=?', (mid, subject))

    def _finish(self, measurement_id, state):
        quota = self._quota()
        with self.lock, self._db() as db:
            row = db.execute('SELECT state,started_at FROM usage_measurements WHERE id=?', (measurement_id,)).fetchone()
            if not row or row['state'] not in ('starting', 'running'):
                return  # Saved windows must not absorb later rollout writes.
            for cursor in db.execute('SELECT path,offset FROM usage_measurement_cursors WHERE measurement_id=?', (measurement_id,)):
                try:
                    if Path(cursor['path']).stat().st_size > cursor['offset']:
                        db.execute('UPDATE usage_measurement_cursors SET error=?,incomplete=1 WHERE measurement_id=? AND path=?', ('unread bounded backlog at measurement end', measurement_id, cursor['path']))
                        self._append_error(db, measurement_id, f'{Path(cursor["path"]).name}: unread bounded backlog at measurement end')
                except OSError:
                    db.execute('UPDATE usage_measurement_cursors SET error=?,incomplete=1 WHERE measurement_id=? AND path=?', ('rollout unavailable at measurement end', measurement_id, cursor['path']))
                    self._append_error(db, measurement_id, f'{Path(cursor["path"]).name}: rollout unavailable at measurement end')
            now = time.time()
            duration = max(0, int(now - row['started_at'])) if state == 'complete' and row and row['started_at'] is not None else None
            if duration is None:
                db.execute("UPDATE usage_measurements SET state=?,ended_at=?,quota_end=?,quota_end_at=? WHERE id=? AND state IN ('starting','running')", (state, now, *(quota or (None, None)), measurement_id))
            else:
                db.execute("UPDATE usage_measurements SET state=?,ended_at=?,duration_seconds=?,quota_end=?,quota_end_at=? WHERE id=? AND state='running'", (state, now, duration, *(quota or (None, None)), measurement_id))
        if state == 'complete': self._request_pause(measurement_id)

    def _request_pause(self, measurement_id):
        if not self.pause_callback: return
        with self._db() as db:
            row = db.execute('SELECT * FROM usage_measurements WHERE id=?', (measurement_id,)).fetchone()
            if not row or not row['pause_at_end'] or row['pause_requested_at'] is not None or row['state'] != 'complete': return
            sessions = [r['session_id'] for r in db.execute('SELECT session_id FROM usage_measurement_agents WHERE measurement_id=?', (measurement_id,))]
        try:
            self.pause_callback(row['project_id'], row['project_db_path'], measurement_id, sessions)
            with self._db() as db: db.execute('UPDATE usage_measurements SET pause_requested_at=?,pause_error=NULL WHERE id=?', (time.time(), measurement_id))
        except Exception as exc:
            with self._db() as db: db.execute('UPDATE usage_measurements SET pause_error=? WHERE id=?', (str(exc)[:512], measurement_id))

    def _retry_pauses(self):
        with self._db() as db:
            rows = [r['id'] for r in db.execute("SELECT id FROM usage_measurements WHERE state='complete' AND pause_at_end=1 AND pause_requested_at IS NULL")]
        for measurement_id in rows: self._request_pause(measurement_id)

    def stop(self, project_id):
        with self._db() as db:
            row = db.execute("SELECT id FROM usage_measurements WHERE project_id=? AND state IN ('starting','running')", (project_id,)).fetchone()
        if row:
            self._establish(row['id']); self._sample(row['id']); self._finish(row['id'], 'complete')
        return self.status(project_id)

    def _report(self, db, row):
        def item(subject, agent=None):
            total = db.execute('SELECT * FROM usage_measurement_totals WHERE measurement_id=? AND subject=?', (row['id'], subject)).fetchone()
            base = dict(total) if total else {}
            missing = {'input_tokens': 'input_missing', 'cached_input_tokens': 'cached_missing', 'output_tokens': 'output_missing', 'reasoning_output_tokens': 'reasoning_missing'}
            cursors = db.execute('SELECT error,incomplete FROM usage_measurement_cursors WHERE measurement_id=? AND owner_session=?', (row['id'], subject)).fetchall()
            has_stream = bool(cursors)
            incomplete = any(cursor['incomplete'] or cursor['error'] for cursor in cursors)
            metrics = {key: (None if not base.get('responses', 0) or incomplete or base.get(missing[key], 0) else base.get(key, 0)) for key in _FIELDS}
            metrics['fresh_input_tokens'] = None if metrics['input_tokens'] is None or metrics['cached_input_tokens'] is None else metrics['input_tokens'] - metrics['cached_input_tokens']
            metrics.update(response_count=base.get('responses', 0), tool_result_text_characters=base.get('tool_chars', 0), wake_message_count=base.get('wake_messages', 0), usage_status=('missing' if not has_stream else ('partial' if incomplete else ('no_new_responses' if not base.get('responses', 0) else ('partial' if any(v is None for v in metrics.values()) else 'record_stream')))))
            if agent: metrics.update(agent=agent['agent'], session_id=agent['session_id'], thread_id=agent['thread_id'])
            return metrics
        agents = [dict(a) for a in db.execute('SELECT * FROM usage_measurement_agents WHERE measurement_id=? ORDER BY agent,session_id', (row['id'],))]
        listed = [item(a['session_id'], a) for a in agents]
        totals = {key: (None if any(a[key] is None for a in listed) else sum(a[key] for a in listed)) for key in ('response_count', *_FIELDS, 'fresh_input_tokens', 'tool_result_text_characters', 'wake_message_count')}
        return {'id': row['id'], 'project_id': row['project_id'], 'state': row['state'], 'started_at': row['started_at'], 'ended_at': row['ended_at'], 'deadline': row['deadline'], 'duration_seconds': row['duration_seconds'], 'sampled_at': row['sampled_at'], 'agents': listed, 'guardian': item('__guardian__'), 'totals': totals, 'quota_start': None if row['quota_start'] is None else {'remaining_percent': row['quota_start'], 'observed_at': row['quota_start_at']}, 'quota_end': None if row['quota_end'] is None else {'remaining_percent': row['quota_end'], 'observed_at': row['quota_end_at']}, 'pause_at_end': bool(row['pause_at_end']), 'pause_requested_at': row['pause_requested_at'], 'pause_error': row['pause_error'], 'errors': row['errors'].split('\n') if row['errors'] else [], 'coverage': 'Local logs for agents connected when this measurement started. Remote logs and agents connected later are not included. Missing or incomplete data is listed below.'}

    def status(self, project_id):
        try:
            if shutil.which('jq') is None: return {'available': False, 'error': 'jq is unavailable', 'active': None, 'latest': None}
            with self._db() as db:
                active = db.execute("SELECT * FROM usage_measurements WHERE project_id=? AND state IN ('starting','running') ORDER BY rowid DESC LIMIT 1", (project_id,)).fetchone()
                latest = db.execute('SELECT * FROM usage_measurements WHERE project_id=? ORDER BY rowid DESC LIMIT 1', (project_id,)).fetchone()
                return {'available': True, 'error': None, 'active': self._report(db, active) if active else None, 'latest': self._report(db, latest) if latest else None}
        except Exception as exc:
            return {'available': False, 'error': str(exc), 'active': None, 'latest': None}

    def close(self):
        self.stop_event.set()
        for worker in list(self.workers.values()): worker.join(timeout=12)
        for measurement_id in list(self.workers): self._finish(measurement_id, 'interrupted')
        self.workers.clear()
