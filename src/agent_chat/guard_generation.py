"""Persistent bounded change tracking for guarded-work metadata.

The counter is deliberately global to the database: one scalar records every
insert/update/delete of guard rows and their remote associations. This trades
occasional safe preview invalidation for complete detection of guard-history
ABA changes without scanning or retaining unbounded history in a claim audit.
"""
from __future__ import annotations

import sqlite3


TRACKED_TABLES = (
    'guarded_runs',
    'remote_guarded_runs',
    'remote_guard_batches',
    'remote_restoration_runs',
)


def _trigger_sql(name: str, table: str, event: str) -> str:
    return f'''CREATE TRIGGER {name} AFTER {event} ON {table}
        BEGIN
          UPDATE root_guard_generation SET generation=generation+1 WHERE singleton=1;
          SELECT CASE WHEN changes()!=1 THEN RAISE(ABORT,'guard generation unavailable') END;
        END'''


def _normalized_sql(sql: str) -> str:
    return ' '.join(sql.lower().split())


def _require_hooks(db: sqlite3.Connection, existing: set[str]) -> None:
    for table in TRACKED_TABLES:
        if table not in existing:
            continue
        expected = {'root_guard_generation_' + table + '_' + suffix: event
                    for suffix, event in (('ai', 'INSERT'), ('au', 'UPDATE'), ('ad', 'DELETE'))}
        rows = db.execute("SELECT name,tbl_name,sql FROM sqlite_master WHERE type='trigger' AND name IN (?,?,?)",
                          tuple(sorted(expected))).fetchall()
        if {row[0] for row in rows} != set(expected):
            raise RuntimeError('guard generation hooks unavailable')
        for row in rows:
            name, target, sql = row
            if target != table or _normalized_sql(sql or '') != _normalized_sql(
                    _trigger_sql(name, table, expected[name])):
                raise RuntimeError('guard generation hook definition unsupported')


def install_guard_generation(db: sqlite3.Connection) -> None:
    """Install idempotent SQLite producer hooks for guard metadata changes."""
    db.execute('''CREATE TABLE IF NOT EXISTS root_guard_generation (
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
        generation INTEGER NOT NULL CHECK(typeof(generation)='integer' AND generation>=0))''')
    db.execute('INSERT OR IGNORE INTO root_guard_generation(singleton,generation) VALUES(1,0)')
    existing = {row[0] for row in db.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    for table in TRACKED_TABLES:
        if table not in existing:
            continue
        if table in ('guarded_runs', 'remote_guarded_runs'):
            db.execute(f'CREATE INDEX IF NOT EXISTS {table}_resource_closed_run '
                       f'ON {table}(resource,closed_at,run_id)')
            db.execute(f'CREATE INDEX IF NOT EXISTS {table}_reservation_run '
                       f'ON {table}(reservation_id,run_id)')
        for suffix, event in (('ai', 'INSERT'), ('au', 'UPDATE'), ('ad', 'DELETE')):
            name = 'root_guard_generation_' + table + '_' + suffix
            sql = _trigger_sql(name, table, event).replace(
                'CREATE TRIGGER ', 'CREATE TRIGGER IF NOT EXISTS ', 1)
            db.execute(sql)
    _require_hooks(db, existing)


def guard_generation(db: sqlite3.Connection) -> int:
    """Return a tracked generation, failing closed if hooks are absent."""
    row = db.execute("SELECT generation FROM root_guard_generation WHERE singleton=1").fetchone()
    if row is None:
        raise RuntimeError('guard generation metadata unavailable')
    existing = {row[0] for row in db.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    _require_hooks(db, existing)
    return row[0]
