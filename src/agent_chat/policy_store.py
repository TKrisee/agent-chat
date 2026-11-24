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
                CREATE TABLE IF NOT EXISTS native_policy_verified_offers (
                  host TEXT NOT NULL,thread TEXT NOT NULL,client_id TEXT NOT NULL,
                  version TEXT NOT NULL,payload_sha256 TEXT NOT NULL,
                  PRIMARY KEY(host,thread,client_id));
                CREATE TABLE IF NOT EXISTS native_policy_verified_receipts (
                  host TEXT NOT NULL,thread TEXT NOT NULL,project TEXT NOT NULL,version TEXT NOT NULL,
                  client_id TEXT NOT NULL,turn_id TEXT NOT NULL,observed_at REAL NOT NULL,
                  PRIMARY KEY(host,thread,project,version));
                CREATE TRIGGER IF NOT EXISTS policy_offer_no_update BEFORE UPDATE ON native_policy_offers
                  BEGIN SELECT RAISE(ABORT,'policy offer is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS policy_offer_no_delete BEFORE DELETE ON native_policy_offers
                  BEGIN SELECT RAISE(ABORT,'policy offer is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS policy_receipt_no_update BEFORE UPDATE ON native_policy_receipts
                  BEGIN SELECT RAISE(ABORT,'policy receipt is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS policy_receipt_no_delete BEFORE DELETE ON native_policy_receipts
                  BEGIN SELECT RAISE(ABORT,'policy receipt is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS verified_policy_offer_no_update BEFORE UPDATE ON native_policy_verified_offers
                  BEGIN SELECT RAISE(ABORT,'verified policy offer is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS verified_policy_offer_no_delete BEFORE DELETE ON native_policy_verified_offers
                  BEGIN SELECT RAISE(ABORT,'verified policy offer is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS verified_policy_receipt_no_update BEFORE UPDATE ON native_policy_verified_receipts
                  BEGIN SELECT RAISE(ABORT,'verified policy receipt is immutable'); END;
                CREATE TRIGGER IF NOT EXISTS verified_policy_receipt_no_delete BEFORE DELETE ON native_policy_verified_receipts
                  BEGIN SELECT RAISE(ABORT,'verified policy receipt is immutable'); END;
            ''')

    @contextlib.contextmanager
    def connection(self):
        db=sqlite3.connect(self.path,timeout=10,isolation_level=None)
        db.row_factory=sqlite3.Row
        try: yield db
        finally: db.close()

    def consumed(self, host, thread, *, project=None):
        if not isinstance(project,str) or not project: return False
        with self.connection() as db:
            return bool(db.execute('''SELECT 1 FROM native_policy_verified_receipts r
                JOIN native_policy_verified_offers v ON v.host=r.host AND v.thread=r.thread
                  AND v.client_id=r.client_id AND v.version=r.version
                JOIN native_policy_offers o ON o.host=v.host AND o.thread=v.thread
                  AND o.client_id=v.client_id AND o.version=v.version AND o.payload_sha256=v.payload_sha256 AND o.project=r.project
                WHERE r.host=? AND r.thread=? AND r.version=? AND (? IS NULL OR o.project=?)''',
                                   (host,thread,communication.policy_version(),project,project)).fetchone())

    def offer(self, host, project, kind, client_id, thread, payload):
        # Version is derived from the actual emitted body, never caller flags.
        # Root's authority preamble spans lines. Its current policy still must
        # occur intact immediately before its metadata, without changing boot.
        body=payload
        try:
            if kind=='root' and not payload.startswith(communication.STANDING_POLICY):
                _,found,following=payload.partition('\n'+communication.STANDING_POLICY)
                if not found: return {'offered':False}
                body=communication.STANDING_POLICY+following
            elif kind=='reset' and not payload.startswith(communication.STANDING_POLICY):
                _,found,following=payload.partition(communication.STANDING_POLICY)
                if not found: return {'offered':False}
                body=communication.STANDING_POLICY+following
            metadata=json.loads(body.split('\n')[1])
            version=metadata.get('policy_version')
        except (ValueError,IndexError,AttributeError): return {'offered':False}
        if version!=communication.policy_version() or communication.STANDING_POLICY not in body.split('\n')[0]:
            return {'offered':False}
        if kind not in ('reset','root','wake') or not body.startswith(communication.STANDING_POLICY+' '+communication.BOOT_AUTHORIZATION):
            return {'offered':False}
        digest=hashlib.sha256(payload.encode()).hexdigest()
        with self.connection() as db:
            db.execute('INSERT OR IGNORE INTO native_policy_offers VALUES(?,?,?,?,?,?,?,?)',
                       (host,thread,client_id,version,project,kind,digest,time.time()))
            old=db.execute('SELECT * FROM native_policy_offers WHERE host=? AND thread=? AND client_id=?',(host,thread,client_id)).fetchone()
            if (old['version'],old['project'],old['kind'],old['payload_sha256'])!=(version,project,kind,digest):
                raise CoordError('native policy offer differs from immutable original')
            db.execute('INSERT OR IGNORE INTO native_policy_verified_offers VALUES(?,?,?,?,?)',
                       (host,thread,client_id,version,digest))
        return {'offered':True}

    def pending(self, host, *, project=None):
        if not isinstance(project,str) or not project: return []
        with self.connection() as db:
            return [dict(row) for row in db.execute('''SELECT o.* FROM native_policy_offers o
                JOIN native_policy_verified_offers v ON v.host=o.host AND v.thread=o.thread
                  AND v.client_id=o.client_id AND v.version=o.version AND v.payload_sha256=o.payload_sha256
                WHERE o.host=? AND o.version=? AND (? IS NULL OR o.project=?) AND NOT EXISTS(SELECT 1 FROM native_policy_verified_receipts r
                    JOIN native_policy_verified_offers rv ON rv.host=r.host AND rv.thread=r.thread
                      AND rv.client_id=r.client_id AND rv.version=r.version
                    JOIN native_policy_offers ro ON ro.host=rv.host AND ro.thread=rv.thread
                      AND ro.client_id=rv.client_id AND ro.version=rv.version AND ro.payload_sha256=rv.payload_sha256 AND ro.project=r.project
                    WHERE r.host=o.host AND r.thread=o.thread AND r.project=o.project AND r.version=o.version)''',(host,communication.policy_version(),project,project))]

    def receipt(self, host, thread, client_id, version, turn_id, *, project=None):
        if not isinstance(project,str) or not project:
            raise CoordError('policy receipt requires exact project scope')
        if not isinstance(turn_id,str) or not turn_id:
            raise CoordError('policy consumption requires an exact native turn ID')
        with self.connection() as db:
            offer=db.execute('''SELECT o.project FROM native_policy_verified_offers v JOIN native_policy_offers o
                ON o.host=v.host AND o.thread=v.thread AND o.client_id=v.client_id
                  AND o.version=v.version AND o.payload_sha256=v.payload_sha256
                WHERE v.host=? AND v.thread=? AND v.client_id=? AND v.version=? AND (? IS NULL OR o.project=?)''',
                             (host,thread,client_id,version,project,project)).fetchone()
            if not offer: raise CoordError('policy proof lacks an exact immutable offered body/version')
            db.execute('INSERT OR IGNORE INTO native_policy_receipts VALUES(?,?,?,?,?,?)',
                       (host,thread,version,client_id,turn_id,time.time()))
            db.execute('INSERT OR IGNORE INTO native_policy_verified_receipts VALUES(?,?,?,?,?,?,?)',
                       (host,thread,offer['project'],version,client_id,turn_id,time.time()))
            recorded=db.execute('SELECT client_id,turn_id FROM native_policy_verified_receipts WHERE host=? AND thread=? AND project=? AND version=?',
                                (host,thread,offer['project'],version)).fetchone()
            if (recorded['client_id'],recorded['turn_id'])!=(client_id,turn_id):
                raise CoordError('policy receipt conflicts with existing exact client/turn association')
        return {'consumed':True}
