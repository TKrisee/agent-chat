"""Exact offered policy and positive native consumption, separate from job status."""
import contextlib
import hashlib
import json
import os
import sqlite3
import time

from .core import CoordError
from . import communication


class PolicyStore:
    def __init__(self, path):
        self.path = path
        try:
            fd=os.open(path,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
        except FileExistsError: pass
        else: os.close(fd)
        with self.connection() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS native_policy_offers (
                  host TEXT NOT NULL,thread TEXT NOT NULL,client_id TEXT NOT NULL,
                  version TEXT NOT NULL,project TEXT NOT NULL,kind TEXT NOT NULL,
                  payload_sha256 TEXT NOT NULL,created_at REAL NOT NULL,
                  PRIMARY KEY(host,thread,client_id));
                CREATE TABLE IF NOT EXISTS native_policy_receipts (
                  host TEXT NOT NULL,thread TEXT NOT NULL,version TEXT NOT NULL,
                  client_id TEXT NOT NULL,turn_id TEXT NOT NULL,observed_at REAL NOT NULL,
                  PRIMARY KEY(host,thread,version));
                CREATE TRIGGER IF NOT EXISTS policy_offer_no_update BEFORE UPDATE ON native_policy_offers
                  BEGIN SELECT RAISE(ABORT,'policy offer is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS policy_offer_no_delete BEFORE DELETE ON native_policy_offers
                  BEGIN SELECT RAISE(ABORT,'policy offer is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS policy_receipt_no_update BEFORE UPDATE ON native_policy_receipts
                  BEGIN SELECT RAISE(ABORT,'policy receipt is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS policy_receipt_no_delete BEFORE DELETE ON native_policy_receipts
                  BEGIN SELECT RAISE(ABORT,'policy receipt is immutable'); END;
            ''')

    @contextlib.contextmanager
    def connection(self):
        db=sqlite3.connect(self.path,timeout=10,isolation_level=None)
        db.row_factory=sqlite3.Row
        try: yield db
        finally: db.close()

    def consumed(self, host, thread):
        with self.connection() as db:
            return bool(db.execute('SELECT 1 FROM native_policy_receipts WHERE host=? AND thread=? AND version=?',
                                   (host,thread,communication.policy_version())).fetchone())

    def offer(self, host, project, kind, client_id, thread, payload):
        # Version is derived from the actual emitted body, never caller flags.
        try:
            metadata=json.loads(payload.split('\n')[1])
            version=metadata.get('policy_version')
        except (ValueError,IndexError,AttributeError): return {'offered':False}
        if version!=communication.policy_version() or communication.STANDING_POLICY not in payload.split('\n')[0]:
            return {'offered':False}
        digest=hashlib.sha256(payload.encode()).hexdigest()
        with self.connection() as db:
            db.execute('INSERT OR IGNORE INTO native_policy_offers VALUES(?,?,?,?,?,?,?,?)',
                       (host,thread,client_id,version,project,kind,digest,time.time()))
            old=db.execute('SELECT * FROM native_policy_offers WHERE host=? AND thread=? AND client_id=?',(host,thread,client_id)).fetchone()
            if (old['version'],old['project'],old['kind'],old['payload_sha256'])!=(version,project,kind,digest):
                raise CoordError('native policy offer differs from immutable original')
        return {'offered':True}

    def pending(self, host):
        with self.connection() as db:
            return [dict(row) for row in db.execute('''SELECT o.* FROM native_policy_offers o
                WHERE o.host=? AND NOT EXISTS(SELECT 1 FROM native_policy_receipts r
                WHERE r.host=o.host AND r.thread=o.thread AND r.version=o.version)''',(host,))]

    def receipt(self, host, thread, client_id, version, turn_id):
        if not isinstance(turn_id,str) or not turn_id:
            raise CoordError('policy consumption requires an exact native turn ID')
        with self.connection() as db:
            offer=db.execute('SELECT 1 FROM native_policy_offers WHERE host=? AND thread=? AND client_id=? AND version=?',
                             (host,thread,client_id,version)).fetchone()
            if not offer: raise CoordError('policy proof lacks an exact immutable offered body/version')
            db.execute('INSERT OR IGNORE INTO native_policy_receipts VALUES(?,?,?,?,?,?)',
                       (host,thread,version,client_id,turn_id,time.time()))
        return {'consumed':True}
