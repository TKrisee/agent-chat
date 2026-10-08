"""Root team stop barriers: preserve policy, identities, inputs and ownership."""
import time
import uuid
import sqlite3
from .core import CoordError


def schema(db):
    db.executescript('''
      CREATE TABLE IF NOT EXISTS root_team_stops (
        id TEXT PRIMARY KEY, root_session TEXT NOT NULL, active INTEGER NOT NULL,
        created_at REAL NOT NULL, resumed_at REAL);
      CREATE TABLE IF NOT EXISTS root_team_stop_targets (
        id TEXT PRIMARY KEY, stop_id TEXT NOT NULL, requester_session TEXT NOT NULL,
        target_session TEXT NOT NULL, host_id TEXT, thread_id TEXT,
        status TEXT NOT NULL, turn_id TEXT, error TEXT, created_at REAL NOT NULL,
        updated_at REAL NOT NULL, UNIQUE(stop_id,target_session));
      CREATE TABLE IF NOT EXISTS root_team_stop_interrupts (
        request_id TEXT NOT NULL, turn_id TEXT NOT NULL, recorded_at REAL NOT NULL,
        PRIMARY KEY(request_id,turn_id));
    ''')


def stopped(db, target):
    from .root_team_reset import quarantined
    if quarantined(db,session=target):
        return True
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_team_stops'").fetchone():
        return False
    return bool(db.execute('SELECT 1 FROM root_team_stops WHERE active=1').fetchone()
                and not db.execute('SELECT 1 FROM root_projections WHERE session_id=?', (target,)).fetchone()
                and not db.execute('SELECT 1 FROM operator_identities WHERE session_id=?', (target,)).fetchone())


def job(db, request_id):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_team_stop_targets'").fetchone():
        return None
    row = db.execute('SELECT * FROM root_team_stop_targets WHERE id=?', (request_id,)).fetchone()
    return dict(row, action='stop', interrupt=1, root_team_stop=True) if row else None


def pause(coord, root, request_id):
    schema(coord.db)
    with coord.tx() as db:
        old = db.execute('SELECT * FROM root_team_stops WHERE id=?', (request_id,)).fetchone()
        if old:
            if old['root_session'] != root:
                raise CoordError('stop-all request belongs to a different root')
            return status(db, request_id)
        active = db.execute('SELECT id FROM root_team_stops WHERE active=1').fetchone()
        if active:
            raise CoordError('another stop-all barrier is active; resume its exact request first')
        now = time.time()
        db.execute('INSERT INTO root_team_stops VALUES(?,?,1,?,NULL)', (request_id, root, now))
        rows = db.execute('''SELECT s.id,b.thread_id,h.host_id FROM sessions s
            LEFT JOIN bridge_bindings b ON b.session_id=s.id
            LEFT JOIN remote_session_hosts h ON h.session_id=s.id
            WHERE s.id NOT IN (SELECT session_id FROM root_projections)
              AND s.id NOT IN (SELECT session_id FROM operator_identities)''').fetchall()
        for row in rows:
            identifier = str(uuid.uuid5(uuid.UUID(request_id), row['id']))
            pending = bool(row['thread_id'] and row['host_id'])
            db.execute('INSERT INTO root_team_stop_targets VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                       (identifier, request_id, root, row['id'], row['host_id'], row['thread_id'],
                        'pending' if pending else 'stopped', None,
                        None if pending else 'No bound authenticated main turn; admission barrier retained.', now, now))
        return status(db, request_id)


def resume(coord, root, request_id):
    schema(coord.db)
    with coord.tx() as db:
        row = db.execute('SELECT * FROM root_team_stops WHERE id=?', (request_id,)).fetchone()
        if not row or row['root_session'] != root:
            raise CoordError('resume-all requires the exact original root stop request')
        if row['active']:
            db.execute('UPDATE root_team_stops SET active=0,resumed_at=? WHERE id=?', (time.time(), request_id))
        return status(db, request_id)


def status(db, request_id):
    row = db.execute('SELECT * FROM root_team_stops WHERE id=?', (request_id,)).fetchone()
    return dict(row, targets=[dict(item) for item in db.execute(
        'SELECT * FROM root_team_stop_targets WHERE stop_id=? ORDER BY target_session', (request_id,))])


def jobs(db, host):
    from .root_team_reset import quarantined
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_team_stops'").fetchone():
        return []
    return [item for row in db.execute('''SELECT t.id FROM root_team_stop_targets t
        JOIN root_team_stops s ON s.id=t.stop_id WHERE s.active=1 AND t.host_id=?
        AND t.thread_id IS NOT NULL ORDER BY t.created_at''', (host,))
        if not quarantined(db,session=(item:=job(db,row['id']))['target_session'])]


def update(coord, request_id, expected, status_value, *, turn_id=None, error=None, observation=None):
    with coord.tx() as db:
        current = job(db, request_id)
        from .root_team_reset import quarantined,handoff_settling,handoff_pending
        if current and quarantined(db,session=current['target_session']):
            same_interrupt=(status_value=='interrupting' and expected=='interrupting' and turn_id==current['turn_id']
                and any(row['kind']=='control' and row['job_id']==request_id and row['host_id']==current['host_id'] for row in handoff_pending(db)))
            if not same_interrupt and not handoff_settling(db,'control',request_id,current['host_id'],status_value):
                raise CoordError('team reset quarantined this former target; native calls withheld')
        if not current or current['status'] != expected:
            raise CoordError('root team stop state changed; inspect current request')
        active = db.execute('SELECT active FROM root_team_stops WHERE id=?', (current['stop_id'],)).fetchone()
        if not active or not active['active']:
            raise CoordError('root team stop barrier was resumed; native interruption withheld')
        binding = db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?', (current['target_session'],)).fetchone()
        host = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (current['target_session'],)).fetchone()
        if not binding or binding['thread_id'] != current['thread_id'] or not host or host['host_id'] != current['host_id']:
            raise CoordError('root team stop target binding or host changed; native interruption withheld')
        if status_value not in ('pending','waiting','interrupting','completed'):
            raise CoordError('invalid root team stop transition')
        if status_value == 'completed' and (not observation or observation['state'] != 'idle' or observation['queue_count']):
            raise CoordError('root team stop completion requires idle and empty native queue')
        if status_value == 'interrupting':
            if not turn_id:
                raise CoordError('root team stop interruption requires an observed exact main turn')
            if expected == 'interrupting' and turn_id != current['turn_id']:
                raise CoordError('unresolved root stop interrupt cannot target another turn')
            if expected != 'interrupting':
                try:
                    db.execute('INSERT INTO root_team_stop_interrupts VALUES(?,?,?)', (request_id, turn_id, time.time()))
                except sqlite3.IntegrityError as failure:
                    raise CoordError('root stop interrupt is already recorded; no replay') from failure
        db.execute('UPDATE root_team_stop_targets SET status=?,turn_id=?,error=?,updated_at=? WHERE id=?',
                   (status_value, turn_id, error, time.time(), request_id))
        return job(db, request_id)
