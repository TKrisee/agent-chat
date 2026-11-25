import concurrent.futures
from contextlib import closing
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import test_build01_admission as fixture
from agent_chat import successor_admission as s, successor_attempt_journal as j, root_local

class IdentityContract(fixture.Build01Tests):
    def store(self):
        return j.verified_store(self.master,actor=s.ROOT,host=s.HOST,project='default',root_thread=s.ROOT_THREAD)
    def args(self):
        return dict(request_id=s.BUILD_REQUEST,operation='synthetic',actor=s.ROOT,host=s.HOST,target=str(self.master),source=s.BUILD_SOURCE,project='default')
    def test_missing_path_is_not_created(self):
        missing=self.root/'missing.db'
        with self.assertRaises(FileNotFoundError):
            with j.verified_store(missing,actor=s.ROOT,host=s.HOST,project='default',root_thread=s.ROOT_THREAD): pass
        self.assertFalse(missing.exists())
    def test_unexpected_journal_trigger_is_rejected_before_write(self):
        j._schema(self.db)
        self.db.execute('CREATE TABLE stolen(v TEXT)')
        self.db.execute('CREATE TRIGGER hostile AFTER INSERT ON successor_producer_attempts BEGIN INSERT INTO stolen VALUES(new.actor); END')
        self.db.commit()
        with self.assertRaisesRegex(ValueError,'untrusted or incomplete'):
            with self.store(): pass
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM stolen').fetchone()[0],0)
        self.assertEqual(self.db.execute('SELECT COUNT(*) FROM successor_producer_attempts').fetchone()[0],0)
    def test_replacement_during_open_is_rejected_before_write(self):
        replacement=self.root/'replacement.db'
        with closing(sqlite3.connect(replacement)) as other: self.db.backup(other)
        original=sqlite3.connect
        def swap(*args,**kwargs):
            os.replace(replacement,self.master)
            return original(*args,**kwargs)
        with patch.object(j.sqlite3,'connect',side_effect=swap):
            with self.assertRaisesRegex(RuntimeError,'identity changed'):
                with self.store(): pass
        with closing(original(self.master)) as db:
            self.assertFalse(db.execute("SELECT 1 FROM sqlite_master WHERE name='successor_producer_attempts'").fetchone())
    def test_replacement_after_journal_stops_before_packet(self):
        self.free_heavy(); replacement=self.root/'replacement.db'
        with closing(sqlite3.connect(replacement)) as other: self.db.backup(other)
        self.after_native=lambda: os.replace(replacement,self.master)
        with patch.object(s,'_require_unconsumed_allowance',return_value=None), patch.object(root_local,'_preflight',return_value=(self.master,True)):
            with self.assertRaisesRegex(RuntimeError,'identity changed'): self.build()
        with closing(sqlite3.connect(self.master)) as db:
            self.assertFalse(db.execute('SELECT 1 FROM successor_admissions WHERE request_id=?',(s.BUILD_REQUEST,)).fetchone())
    def test_unexpected_packet_trigger_rejected(self):
        self.db.execute('CREATE TABLE packet_stolen(v TEXT)')
        self.db.execute('CREATE TRIGGER packet_hostile AFTER INSERT ON successor_admissions BEGIN INSERT INTO packet_stolen VALUES(new.request_id); END')
        self.db.commit()
        with self.assertRaisesRegex(ValueError,'untrusted or incomplete'):
            with self.store(): pass
        self.assertFalse(self.db.execute('SELECT 1 FROM packet_stolen').fetchone())
    def test_sidecar_replacement_is_detected_before_next_write(self):
        self.free_heavy()
        sidecar=Path(str(self.master)+'-wal')
        replacement=self.root/'replacement-wal'
        replacement.write_bytes(sidecar.read_bytes())
        self.after_native=lambda: os.replace(replacement,sidecar)
        with patch.object(s,'_require_unconsumed_allowance',return_value=None), patch.object(root_local,'_preflight',return_value=(self.master,True)):
            with self.assertRaisesRegex(RuntimeError,'sidecar identity changed'): self.build()
        self.assertFalse(self.db.execute('SELECT 1 FROM successor_admissions WHERE request_id=?',(s.BUILD_REQUEST,)).fetchone())
    def test_serialized_same_request_and_immutable_events(self):
        def run(_):
            try:
                with self.store() as (db,check): j.begin(db,check,**self.args())
                return 'recorded'
            except ValueError as error:
                if 'attempt already recorded' in str(error): return 'duplicate'
                raise
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool: result=sorted(pool.map(run,range(2)))
        self.assertEqual(result,['duplicate','recorded'])
        with self.assertRaises(sqlite3.IntegrityError): self.db.execute("UPDATE successor_producer_attempt_events SET phase='changed'")
        self.db.rollback()
        self.assertEqual(self.db.execute('SELECT phase FROM successor_producer_attempt_events').fetchone()[0],'attempt_started')
