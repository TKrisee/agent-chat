"""Durable, global weekly-usage reserve policy."""
from __future__ import annotations

import contextlib
import math
from pathlib import Path
import sqlite3
import threading
import time

from .core import CoordError


class UsageStore:
    """A small SQLite-backed policy shared by every project of one server."""
    ttl = 60.0

    def __init__(self, base_database):
        self.path = str(Path(base_database).expanduser().resolve())
        self.lock = threading.RLock()
        with self._connection() as db:
            db.execute('''CREATE TABLE IF NOT EXISTS usage_policy (
                singleton INTEGER PRIMARY KEY CHECK(singleton=1), enabled INTEGER NOT NULL,
                threshold_percent REAL NOT NULL, paused INTEGER NOT NULL)''')
            db.execute('''CREATE TABLE IF NOT EXISTS usage_hosts (
                host_id TEXT PRIMARY KEY, remaining_percent REAL, resets_at REAL,
                observed_at REAL, last_seen REAL NOT NULL, error TEXT,
                stopped_threads INTEGER NOT NULL DEFAULT 0, enforcement_error TEXT)''')
            db.execute('INSERT OR IGNORE INTO usage_policy VALUES(1,0,30,0)')

    @contextlib.contextmanager
    def _connection(self):
        db = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            yield db
        finally:
            db.close()

    @staticmethod
    def _number(value, name):
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or not 0 <= value <= 100:
            raise CoordError(f'{name} must be a finite number from 0 to 100')
        return float(value)

    @staticmethod
    def _host(host_id):
        if not isinstance(host_id, str) or not host_id or len(host_id) > 256:
            raise CoordError('host_id must be a nonempty string up to 256 characters')
        return host_id

    @staticmethod
    def _text(value, name):
        if value is not None and (not isinstance(value, str) or len(value) > 8192):
            raise CoordError(f'{name} must be text up to 8192 characters')
        return value

    @staticmethod
    def _reset(value):
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
            raise CoordError('resets_at must be a nonnegative finite timestamp or null')
        return float(value)

    def _rows(self, db, now):
        rows = []
        for row in db.execute('SELECT * FROM usage_hosts ORDER BY host_id'):
            item = dict(row)
            recent = item['last_seen'] >= now - self.ttl
            fresh = recent and item['observed_at'] is not None and item['observed_at'] >= now - self.ttl
            item['recent'] = recent
            item['_fresh'] = fresh
            rows.append(item)
        return rows

    def _status(self, db, now):
        policy = db.execute('SELECT enabled,threshold_percent,paused FROM usage_policy WHERE singleton=1').fetchone()
        rows = self._rows(db, now)
        online = [row for row in rows if row['recent']]
        fresh = [row for row in online if row['_fresh'] and row['error'] is None and row['remaining_percent'] is not None]
        enabled, paused = bool(policy['enabled']), bool(policy['paused'])
        reason = None
        if enabled and paused:
            reason = 'weekly usage reserve reached'
        elif enabled and not online:
            reason = 'usage data unavailable: no recent host reports'
        elif enabled and len(fresh) != len(online):
            reason = 'usage data unavailable for a recent host'
        blocked = enabled and reason is not None
        relevant = fresh
        return {
            'enabled': enabled,
            'threshold_percent': policy['threshold_percent'],
            'paused': paused,
            'blocked': blocked,
            'reason': reason,
            'remaining_percent': min((row['remaining_percent'] for row in relevant), default=None),
            'resets_at': min((row['resets_at'] for row in relevant if row['resets_at'] is not None), default=None),
            'hosts': [{key: row[key] for key in ('host_id', 'remaining_percent', 'resets_at', 'observed_at', 'last_seen', 'error', 'stopped_threads', 'enforcement_error', 'recent')} for row in rows],
        }

    def status(self, host_id=None, now=None):
        now = time.time() if now is None else float(now)
        with self.lock, self._connection() as db:
            db.execute('BEGIN IMMEDIATE' if host_id is not None else 'BEGIN')
            try:
                if host_id is not None:
                    host_id = self._host(host_id)
                    db.execute('''INSERT INTO usage_hosts(host_id,last_seen) VALUES(?,?)
                        ON CONFLICT(host_id) DO UPDATE SET last_seen=excluded.last_seen''', (host_id, now))
                result = self._status(db, now)
                db.commit()
                return result
            except BaseException:
                db.rollback()
                raise

    def configure(self, enabled, threshold_percent, now=None):
        if not isinstance(enabled, bool):
            raise CoordError('enabled must be true or false')
        threshold = self._number(threshold_percent, 'threshold_percent')
        now = time.time() if now is None else float(now)
        with self.lock, self._connection() as db:
            db.execute('BEGIN IMMEDIATE')
            try:
                prior = db.execute('SELECT enabled,threshold_percent,paused FROM usage_policy WHERE singleton=1').fetchone()
                paused = bool(prior['paused'])
                if not enabled:
                    paused = False
                elif not prior['enabled'] or threshold != prior['threshold_percent']:
                    rows = self._rows(db, now)
                    if any(row['_fresh'] and row['error'] is None and row['remaining_percent'] <= threshold for row in rows):
                        paused = True
                db.execute('UPDATE usage_policy SET enabled=?,threshold_percent=?,paused=? WHERE singleton=1',
                           (enabled, threshold, paused))
                result = self._status(db, now)
                db.commit()
                return result
            except BaseException:
                db.rollback()
                raise

    def resume(self, now=None):
        now = time.time() if now is None else float(now)
        with self.lock, self._connection() as db:
            db.execute('BEGIN IMMEDIATE')
            try:
                policy = db.execute('SELECT enabled,threshold_percent FROM usage_policy WHERE singleton=1').fetchone()
                if not policy['enabled']:
                    raise CoordError('usage policy is disabled')
                rows = self._rows(db, now)
                online = [row for row in rows if row['recent']]
                valid = online and all(row['_fresh'] and row['error'] is None and row['remaining_percent'] is not None and row['remaining_percent'] > policy['threshold_percent'] for row in online)
                if not valid:
                    raise CoordError('fresh usage reports above the reserve are required before resuming')
                db.execute('UPDATE usage_policy SET paused=0 WHERE singleton=1')
                result = self._status(db, now)
                db.commit()
                return result
            except BaseException:
                db.rollback()
                raise

    def report(self, host_id, remaining_percent, resets_at, error=None, stopped_threads=0, enforcement_error=None, now=None):
        host_id = self._host(host_id)
        error, enforcement_error = self._text(error, 'error'), self._text(enforcement_error, 'enforcement_error')
        if remaining_percent is None and resets_at is None and error:
            remaining = None
        else:
            remaining = self._number(remaining_percent, 'remaining_percent')
            resets_at = self._reset(resets_at)
        if remaining is None:
            resets_at = None
        if isinstance(stopped_threads, bool) or not isinstance(stopped_threads, int) or stopped_threads < 0:
            raise CoordError('stopped_threads must be a nonnegative integer')
        now = time.time() if now is None else float(now)
        with self.lock, self._connection() as db:
            db.execute('BEGIN IMMEDIATE')
            try:
                db.execute('''INSERT INTO usage_hosts(host_id,remaining_percent,resets_at,observed_at,last_seen,error,stopped_threads,enforcement_error)
                    VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(host_id) DO UPDATE SET
                    remaining_percent=excluded.remaining_percent,resets_at=excluded.resets_at,observed_at=excluded.observed_at,
                    last_seen=excluded.last_seen,error=excluded.error,stopped_threads=excluded.stopped_threads,enforcement_error=excluded.enforcement_error''',
                    (host_id, remaining, resets_at, now, now, error, stopped_threads, enforcement_error))
                policy = db.execute('SELECT enabled,threshold_percent FROM usage_policy WHERE singleton=1').fetchone()
                if policy['enabled'] and error is None and remaining <= policy['threshold_percent']:
                    db.execute('UPDATE usage_policy SET paused=1 WHERE singleton=1')
                result = self._status(db, now)
                db.commit()
                return result
            except BaseException:
                db.rollback()
                raise

    def enforcement(self, host_id, stopped_threads, enforcement_error=None, now=None):
        host_id = self._host(host_id)
        enforcement_error = self._text(enforcement_error, 'enforcement_error')
        if isinstance(stopped_threads, bool) or not isinstance(stopped_threads, int) or stopped_threads < 0:
            raise CoordError('stopped_threads must be a nonnegative integer')
        now = time.time() if now is None else float(now)
        with self.lock, self._connection() as db:
            db.execute('BEGIN IMMEDIATE')
            try:
                db.execute('''INSERT INTO usage_hosts(host_id,last_seen,stopped_threads,enforcement_error) VALUES(?,?,?,?)
                    ON CONFLICT(host_id) DO UPDATE SET last_seen=excluded.last_seen,
                    stopped_threads=excluded.stopped_threads,enforcement_error=excluded.enforcement_error''',
                    (host_id, now, stopped_threads, enforcement_error))
                result = self._status(db, now)
                db.commit()
                return result
            except BaseException:
                db.rollback()
                raise
