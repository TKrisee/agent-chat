"""Read-only, bounded metadata diagnosis of the attachment handoff guard."""
import contextlib
import sqlite3
import time
from pathlib import Path

from .core import CoordError
from . import root_admin_attachment_members as members


def diagnose(master, registry, home_project, session, host, expected_thread, *, resolve_project):
    observed_at = time.time()
    result = {'authority': {'scope': 'machine_read', 'named_session_authentication': 'UNVERIFIED'},
              'observed_at': observed_at, 'cross_store_snapshot': 'non_atomic',
              'native_outcome': 'unknown', 'attachment_clearance': 'not_asserted',
              'state': 'UNKNOWN', 'projects': []}
    try:
        with contextlib.closing(sqlite3.connect(Path(registry).resolve().as_uri() + '?mode=ro', uri=True)) as db:
            db.execute('BEGIN')
            root = db.execute('SELECT home_project,home_session,host_id,thread_id,mode FROM root_config WHERE singleton=1').fetchone()
            if root != (home_project, session, host, expected_thread, 'running'):
                raise CoordError('canonical Root identity or thread differs')
            scope = members.joined_members(db, home_project, session)
            if len(scope) > 64:
                raise CoordError('joined project scope exceeds diagnostic bound')
            pins = []
            for project, projection_session in scope:
                item = {'project_id': project, 'projection_session': projection_session,
                        'state': 'UNKNOWN', 'handoffs': []}
                result['projects'].append(item)
                try:
                    pins.append(members.pin_members(((project, projection_session),), resolve_project)[0])
                except (CoordError, OSError, sqlite3.Error):
                    item['error'] = 'project store resolution or identity unavailable'
                    pins.append(None)
            identities = [(pin[3], pin[4]) for pin in pins if pin is not None]
            if len(set(identities)) != len(identities):
                for item in result['projects']:
                    item['error'] = 'project store identity overlap; coverage unavailable'
                raise CoordError('joined project stores overlap')
        for pin, item in zip(pins, result['projects']):
            if pin is None:
                continue
            project, projection_session = pin[:2]
            try:
                with members.open_member(pin, master, write=False) as db:
                    item['observed_at'] = time.time()
                    members.exact_binding(db, projection_session, home_project, session, host, (expected_thread,))
                    if not members.table(db, 'execution_epoch_handoffs'):
                        item['state'] = 'observed'; item['handoff_table'] = 'absent'; continue
                    item['handoff_table'] = 'present'
                    rows = db.execute('''SELECT id,actor,thread,method,host,dispatcher,started_at,settled_at,outcome
                        FROM execution_epoch_handoffs WHERE settled_at IS NULL AND (actor=? OR thread=?)
                        ORDER BY id LIMIT 101''', (projection_session, expected_thread)).fetchall()
                    if len(rows) > 100:
                        raise CoordError('matching handoffs exceed diagnostic bound')
                    for identifier, actor, thread, method, row_host, dispatcher, started, settled, outcome in rows:
                        match = 'both' if actor == projection_session and thread == expected_thread else ('actor' if actor == projection_session else 'thread')
                        item['handoffs'].append({'id': identifier, 'match': match, 'method': method,
                            'host': row_host, 'dispatcher': dispatcher, 'started_at': started,
                            'settled_at': settled, 'outcome': outcome, 'native_outcome': 'unknown'})
                    item['state'] = 'observed'
            except (CoordError, OSError, sqlite3.Error):
                item['error'] = 'project store binding, lineage, schema or read unavailable'
        with contextlib.closing(sqlite3.connect(Path(registry).resolve().as_uri() + '?mode=ro', uri=True)) as db:
            db.execute('BEGIN')
            current = db.execute('SELECT home_project,home_session,host_id,thread_id,mode FROM root_config WHERE singleton=1').fetchone()
            if current != root or members.joined_members(db, home_project, session) != scope:
                raise CoordError('Root scope or project store changed during observation')
            for pin, item in zip(pins, result['projects']):
                if pin is None:
                    continue
                try:
                    if members.pin_members((pin[:2],), resolve_project) != (pin,):
                        raise CoordError('store changed')
                except (CoordError, OSError, sqlite3.Error):
                    item['state'] = 'UNKNOWN'
                    item['error'] = 'project store identity changed or unavailable during recheck'
        if all(item['state'] == 'observed' for item in result['projects']):
            result['state'] = 'observed'
    except (CoordError, OSError, sqlite3.Error):
        result['error'] = 'Root scope or registry observation unavailable'
    result['finished_at'] = time.time()
    return result
