import contextlib
import io
import json
import os
from pathlib import Path
import plistlib
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from unittest.mock import patch

from agent_chat.core import Coordinator, CoordError
from agent_chat.ownership_disposition import (
    _ensure_audit_schema,
    _json,
    _payload,
    apply_local,
    load_configured_root,
    target_path,
    _sample,
)
from agent_chat.remote_service import dispatch
from agent_chat.service import InstalledServerConfig
from agent_chat import ownership_disposition as disposition


class OwnershipDispositionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.master, self.plist = root / 'state.sqlite3', root / 'server.plist'
        self.wrapper, self.executable = root / 'wrapper', root / 'server-bin'
        self.executable.write_text('', encoding='utf-8')
        self.config = InstalledServerConfig(self.plist, self.wrapper, self.executable, self.master,
                                            root, os.getuid(), '127.0.0.1', 8765)
        self.patch = patch('agent_chat.service._installed_server_config', return_value=self.config)
        self.patch.start(); self._write_config()
        self.coord = Coordinator(self.master)
        # Keep the fixture compatible with producers that already create the
        # provenance column, without promoting existing legacy reads to proof.
        if 'provenance' not in {row[1] for row in self.coord.db.execute('PRAGMA table_info(message_reads)')}:
            self.coord.db.execute("ALTER TABLE message_reads ADD COLUMN provenance TEXT NOT NULL DEFAULT 'legacy_unknown'")
        self.coord.db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('project_registry',?)", (str(self.master),))
        self.registry = Path(str(self.master) + '.projects.sqlite3')
        with contextlib.closing(sqlite3.connect(self.registry)) as db:
            db.executescript('''CREATE TABLE projects (id TEXT PRIMARY KEY); CREATE TABLE root_config
              (singleton INTEGER PRIMARY KEY,home_project TEXT,home_session TEXT,host_id TEXT,mode TEXT);
              CREATE TABLE root_fresh_requests (id TEXT PRIMARY KEY,status TEXT); CREATE TABLE root_team_operations
              (id TEXT PRIMARY KEY,resumed_at REAL);''')
            db.execute("INSERT INTO projects VALUES('default')")
            db.execute("INSERT INTO root_config VALUES(1,'default','root','host-root','running')")
            db.commit()
        self.coord.db.executescript('''CREATE TABLE bridge_bindings (session_id TEXT PRIMARY KEY,thread_id TEXT UNIQUE,parent_session TEXT,agent_path TEXT,updated_at REAL);
          CREATE TABLE remote_session_hosts (session_id TEXT PRIMARY KEY,host_id TEXT,registered_at REAL,last_seen_at REAL);
          CREATE TABLE bridge_jobs (id TEXT PRIMARY KEY,thread_id TEXT,payload TEXT,status TEXT,queue_id TEXT,error TEXT,created_at REAL,updated_at REAL);''')
        for session in ('old', 'waiter', 'other'):
            self.coord.session = session; self.coord.register('worker'); self.coord.inbox()
        self.coord.db.execute("INSERT INTO bridge_bindings VALUES('old','thread-old',NULL,NULL,?)", (time.time(),))
        self.coord.db.execute("INSERT INTO remote_session_hosts VALUES('old','host-root',?,?)", (time.time(), time.time()))

    def tearDown(self):
        self.coord.close(); self.patch.stop(); self.tmp.cleanup()

    def _write_config(self):
        self.wrapper.write_text('#!/bin/sh\nexec ' + str(self.executable) + ' --db ' + str(self.master) + ' --host 127.0.0.1 --port 8765\n', encoding='utf-8')
        with self.plist.open('wb') as stream:
            plistlib.dump({'Label':'com.agent-chat.server','AssociatedBundleIdentifiers':['com.agent-chat.server'],
                           'WorkingDirectory':str(self.config.working_directory),'RunAtLoad':True,'KeepAlive':True,
                           'ThrottleInterval':10,'EnvironmentVariables':{},'ProgramArguments':[str(self.wrapper)]}, stream)

    def _stale(self, name='file:lost.py'):
        self.coord.session = 'old'; claim = self.coord.request(name, 5)
        self.coord.db.execute('UPDATE resources SET deadline=?,token=NULL WHERE name=?', (time.time()-1, name))
        return claim

    def _apply(self, claim, **changes):
        value = dict(master_db=self.master, service_plist=self.plist, project='default', resource=claim['resource'],
                     reservation_id=claim['reservation_id'], owner_session='old', expected_state='stale',
                     request_id=str(uuid.uuid4()), reason='missing original evidence')
        value.update(changes)
        with patch('agent_chat.ownership_disposition._sample', return_value={'thread_id':'thread-old','state':'notLoaded','turn_id':None,'queue_count':0,'direct_input_available':False,'started_at':time.time(),'captured_at':time.time(),'elapsed_ms':1}):
            return apply_local(**value)

    def _legacy_audit(self, request_id, payload_json):
        """Model the original installed table; migration must leave it intact."""
        self.coord.db.execute('''CREATE TABLE admin_ownership_dispositions (
            request_id TEXT PRIMARY KEY,
            project TEXT NOT NULL,
            resource TEXT NOT NULL,
            reservation_id TEXT NOT NULL,
            owner_session TEXT NOT NULL,
            expected_state TEXT NOT NULL,
            requester_session TEXT NOT NULL,
            requester_host TEXT NOT NULL,
            reason TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            outcome TEXT NOT NULL,
            checks_json TEXT NOT NULL,
            process_evidence_state TEXT NOT NULL,
            recorded_at REAL NOT NULL,
            continuation_of TEXT)''')
        self.coord.db.execute('''CREATE TABLE admin_ownership_disposition_history (
            request_id TEXT NOT NULL,event TEXT NOT NULL,details_json TEXT NOT NULL,
            recorded_at REAL NOT NULL,PRIMARY KEY(request_id,event))''')
        payload = json.loads(payload_json)
        now = time.time()
        fields = ('request_id', 'project', 'resource', 'reservation_id', 'owner_session',
                  'expected_state', 'requester_session', 'requester_host', 'reason')
        for index in range(21):
            current_id = request_id if index == 0 else str(uuid.uuid4())
            # The non-selected legacy rows are immutable historic tuple data;
            # no passive sample is supplied or inferred for any of them.
            current = payload if index == 0 else dict(payload, request_id=current_id,
                                                       resource='file:historical-' + str(index))
            values = [current_id] + [current[name] for name in fields[1:]] + [
                _json(current), 'administratively_abandoned',
                _json({'current_state': 'stale', 'historical': True}), 'UNVERIFIED', now, None]
            self.coord.db.execute('''INSERT INTO admin_ownership_dispositions
                (request_id,project,resource,reservation_id,owner_session,expected_state,
                 requester_session,requester_host,reason,payload_json,outcome,checks_json,
                 process_evidence_state,recorded_at,continuation_of)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', values)
            for event in ('requested', 'abandoned'):
                self.coord.db.execute('INSERT INTO admin_ownership_disposition_history VALUES(?,?,?,?)',
                                      (current_id, event, _json({'historic': index}), now))

    def _historic_payload(self, claim, request_id):
        return _json(_payload(root=load_configured_root(self.master, self.plist),
                              project='default', resource=claim['resource'],
                              reservation_id=claim['reservation_id'], owner_session='old',
                              expected_state='stale', request_id=request_id,
                              reason='missing original evidence'))

    def _compact_audit(self, request_id, payload_json):
        """The other installed layout: three immutable blocked records."""
        self.coord.db.execute('''CREATE TABLE admin_ownership_dispositions (
            request_id TEXT PRIMARY KEY, payload_json TEXT NOT NULL,
            outcome TEXT NOT NULL, checks_json TEXT NOT NULL, sample_json TEXT,
            process_evidence_state TEXT NOT NULL, recorded_at REAL NOT NULL,
            continuation_of TEXT)''')
        self.coord.db.execute('''CREATE TABLE admin_ownership_disposition_history (
            request_id TEXT NOT NULL,event TEXT NOT NULL,details_json TEXT NOT NULL,
            recorded_at REAL NOT NULL,PRIMARY KEY(request_id,event))''')
        for index in range(3):
            current_id = request_id if index == 0 else str(uuid.uuid4())
            # Preserve both absent and existing native evidence byte for byte;
            # neither supplies absent requester columns.
            self.coord.db.execute('INSERT INTO admin_ownership_dispositions VALUES(?,?,?,?,?,?,?,?)',
                                  (current_id, payload_json, 'blocked',
                                   _json({'current_state': 'stale', 'historic': index}),
                                   None if index == 0 else _json({'historic_sample': index}),
                                   'UNVERIFIED', 123.0 + index, None))
            self.coord.db.execute('INSERT INTO admin_ownership_disposition_history VALUES(?,?,?,?)',
                                  (current_id, 'sample-consumed', _json({'historic': index}), 123.0 + index))

    def _sampler_audit(self, request_id, payload_json):
        """Literal installed eight-column shape: sample_json is required."""
        self.coord.db.execute('''CREATE TABLE admin_ownership_dispositions (
            request_id TEXT PRIMARY KEY, payload_json TEXT NOT NULL,
            outcome TEXT NOT NULL, checks_json TEXT NOT NULL, sample_json TEXT NOT NULL,
            process_evidence_state TEXT NOT NULL, recorded_at REAL NOT NULL,
            continuation_of TEXT)''')
        self.coord.db.execute('''CREATE TABLE admin_ownership_disposition_history (
            request_id TEXT NOT NULL,event TEXT NOT NULL,details_json TEXT NOT NULL,
            recorded_at REAL NOT NULL,PRIMARY KEY(request_id,event))''')
        self.coord.db.execute('INSERT INTO admin_ownership_dispositions VALUES(?,?,?,?,?,?,?,?)',
            (request_id, payload_json, 'blocked', _json({'current_state': 'stale'}),
             _json({'historic_sample': 'unknown'}), 'UNVERIFIED', 123.0, None))
        self.coord.db.execute('INSERT INTO admin_ownership_disposition_history VALUES(?,?,?,?)',
            (request_id, 'sample-consumed', _json({'historic': True}), 123.0))

    def test_sampler_eight_column_history_only_rollback_replay_preserves_original(self):
        claim = self._stale('operation:literal-sampler-layout')
        original_id, current_id = str(uuid.uuid4()), str(uuid.uuid4())
        self._sampler_audit(original_id, self._historic_payload(claim, original_id))
        old = dict(self.coord.db.execute('SELECT * FROM admin_ownership_dispositions').fetchone())
        history = [tuple(row) for row in self.coord.db.execute('SELECT * FROM admin_ownership_disposition_history')]
        before = (old, history)
        self.assertIsNone(self.coord.db.execute('SELECT request_id FROM admin_ownership_dispositions WHERE request_id=?', (current_id,)).fetchone())
        with patch('agent_chat.ownership_disposition._checks', side_effect=RuntimeError('rollback required after adaptation')):
            with self.assertRaisesRegex(RuntimeError, 'rollback required after adaptation'):
                self._apply(claim, request_id=current_id, historical_blocked_request_id=original_id,
                            reason='current local approval')
        self.assertEqual(len(list(self.coord.db.execute('PRAGMA table_info(admin_ownership_dispositions)'))), 8)
        self.assertIsNone(self.coord.db.execute('SELECT request_id FROM admin_ownership_dispositions WHERE request_id=?', (current_id,)).fetchone())
        self._assert_historical_unchanged(original_id, before)
        self.assertEqual(self.coord.db.execute('SELECT reservation_id FROM resources WHERE name=?', (claim['resource'],)).fetchone()[0], claim['reservation_id'])
        replay = self._apply(claim, request_id=original_id)
        self.assertEqual((replay['outcome'], replay['process_evidence'], replay['replayed']), ('blocked', 'UNVERIFIED', True))
        with self.assertRaisesRegex(CoordError, 'lacks continuation provenance'):
            self._apply(claim, continuation_request_id=original_id)
        result = self._apply(claim, request_id=current_id, historical_blocked_request_id=original_id,
                             reason='current local approval')
        self.assertEqual((result['outcome'], result['process_evidence'], result['historical_blocked_of']),
                         ('administratively_abandoned', 'UNVERIFIED', original_id))
        self._assert_historical_unchanged(original_id, before)
        new = self.coord.db.execute('SELECT requester_session,requester_host,sample_json FROM admin_ownership_dispositions WHERE request_id=?', (current_id,)).fetchone()
        self.assertEqual(tuple(new)[:2], ('root', 'host-root'))
        self.assertIsNotNone(new['sample_json'])
        replay = self._apply(claim, request_id=current_id, historical_blocked_request_id=original_id,
                             reason='current local approval')
        self.assertTrue(replay['replayed'])
        with self.assertRaisesRegex(CoordError, 'retry payload differs'):
            self._apply(claim, request_id=current_id, historical_blocked_request_id=original_id, reason='changed')

    def test_registered_raw_physical_literal_preserves_payload_cas_and_file(self):
        physical = self.master.parent / 'physical-helper.py'
        physical.write_text('unchanged fixture baseline\n', encoding='utf-8')
        claim = self._stale(str(physical))
        self.coord.session = 'waiter'; self.coord.request(claim['resource'], 5)
        self.coord.session = 'other'; unrelated = self.coord.request('file:unrelated-physical.py', 5)
        request_id = str(uuid.uuid4())
        result = self._apply(claim, request_id=request_id)
        self.assertEqual((result['outcome'], result['process_evidence']), ('administratively_abandoned', 'UNVERIFIED'))
        self.assertEqual(physical.read_text(encoding='utf-8'), 'unchanged fixture baseline\n')
        payload = self.coord.db.execute('SELECT payload_json FROM admin_ownership_dispositions WHERE request_id=?', (request_id,)).fetchone()[0]
        self.assertEqual(json.loads(payload)['resource'], str(physical))
        self.assertEqual(self.coord.db.execute('SELECT session FROM resource_queue WHERE resource=?', (str(physical),)).fetchone()[0], 'waiter')
        self.assertEqual(self.coord.db.execute('SELECT reservation_id FROM resources WHERE name=?', (unrelated['resource'],)).fetchone()[0], unrelated['reservation_id'])
        self.assertTrue(self._apply(claim, request_id=request_id)['replayed'])
        with self.assertRaisesRegex(CoordError, 'retry payload differs'):
            self._apply(claim, request_id=request_id, resource='file:' + str(physical))

    def test_raw_physical_registration_cas_and_recipient_gates_remain_exact(self):
        for scenario in ('owner', 'rid', 'live', 'legacy-read', 'wrong-reader', 'full-read'):
            with self.subTest(scenario=scenario):
                claim = self._stale(str(self.master.parent / (scenario + '.py')))
                if scenario == 'owner':
                    self.coord.db.execute('UPDATE resources SET owner_session=? WHERE name=?', ('other', claim['resource']))
                elif scenario == 'rid':
                    self.coord.db.execute('UPDATE resources SET reservation_id=? WHERE name=?', ('successor-rid', claim['resource']))
                elif scenario == 'live':
                    self.coord.db.execute('UPDATE resources SET deadline=?,stale=0 WHERE name=?', (time.time() + 300, claim['resource']))
                else:
                    self.coord.db.execute("INSERT INTO messages(id,sender_session,recipient_session,body,created_at,acked_at) VALUES('raw-input','other','old','input',?,?)", (time.time(), time.time()))
                    provenance = 'legacy_unknown' if scenario == 'legacy-read' else 'recipient_full_message'
                    reader = 'other' if scenario == 'wrong-reader' else 'old'
                    self.coord.db.execute("INSERT INTO message_reads(message_id,reader_session,read_at,provenance) VALUES('raw-input',?,?,?)", (reader, time.time(), provenance))
                before = tuple(self.coord.db.execute('SELECT owner_session,reservation_id FROM resources WHERE name=?', (claim['resource'],)).fetchone())
                result = self._apply(claim)
                self.assertEqual(result['outcome'], 'administratively_abandoned' if scenario == 'full-read' else 'blocked')
                after = tuple(self.coord.db.execute('SELECT owner_session,reservation_id FROM resources WHERE name=?', (claim['resource'],)).fetchone())
                self.assertEqual(after, (None, None) if scenario == 'full-read' else before)
                self.coord.db.execute("DELETE FROM message_reads WHERE message_id='raw-input'")
                self.coord.db.execute("DELETE FROM messages WHERE id='raw-input'")
        unregistered = dict(claim, resource=str(self.master.parent / 'not-registered.py'))
        with self.assertRaisesRegex(CoordError, 'exact registered literal'):
            self._apply(unregistered)
        for alias in (claim['resource'] + '/', str(self.master.parent) + '/./full-read.py',
                      str(self.master.parent) + '/other/../full-read.py', '/'):
            with self.subTest(alias=alias), self.assertRaisesRegex(CoordError, 'approved canonical resource form'):
                self._apply(dict(claim, resource=alias))

    def test_compact_layout_replay_rollback_and_new_apply_preserve_history(self):
        claim = self._stale('file:compact-history.py')
        request_id = str(uuid.uuid4())
        self._compact_audit(request_id, self._historic_payload(claim, request_id))
        projection = 'request_id,payload_json,outcome,checks_json,sample_json,process_evidence_state,recorded_at,continuation_of'
        old = [tuple(row) for row in self.coord.db.execute('SELECT ' + projection + ' FROM admin_ownership_dispositions ORDER BY request_id')]
        history = [tuple(row) for row in self.coord.db.execute('SELECT * FROM admin_ownership_disposition_history ORDER BY request_id,event')]
        with patch('agent_chat.ownership_disposition._sample', side_effect=AssertionError('replay must not sample')):
            replay = self._apply(claim, request_id=request_id)
        self.assertEqual((replay['outcome'], replay['sample'], replay['replayed']), ('blocked', None, True))
        self.assertEqual(len(list(self.coord.db.execute('PRAGMA table_info(admin_ownership_dispositions)'))), 8)
        self.coord.db.execute('BEGIN IMMEDIATE')
        _ensure_audit_schema(self.coord.db)
        self.coord.db.rollback()
        self.assertEqual(len(list(self.coord.db.execute('PRAGMA table_info(admin_ownership_dispositions)'))), 8)
        with patch('agent_chat.ownership_disposition._checks', side_effect=RuntimeError('interrupt after additions')):
            with self.assertRaisesRegex(RuntimeError, 'interrupt after additions'):
                self._apply(claim)
        self.assertEqual(len(list(self.coord.db.execute('PRAGMA table_info(admin_ownership_dispositions)'))), 8)
        with self.assertRaisesRegex(CoordError, 'lacks continuation provenance'):
            self._apply(claim, continuation_request_id=request_id)
        self.assertEqual(len(list(self.coord.db.execute('PRAGMA table_info(admin_ownership_dispositions)'))), 8)
        self.coord.session = 'other'
        unrelated = self.coord.request('file:compact-unrelated.py', 5)
        new_id = str(uuid.uuid4())
        result = self._apply(claim, request_id=new_id)
        self.assertEqual(result['outcome'], 'administratively_abandoned')
        self.assertEqual([tuple(row) for row in self.coord.db.execute('SELECT ' + projection + ' FROM admin_ownership_dispositions WHERE request_id<>? ORDER BY request_id', (new_id,))], old)
        self.assertEqual([tuple(row) for row in self.coord.db.execute('SELECT * FROM admin_ownership_disposition_history WHERE request_id<>? ORDER BY request_id,event', (new_id,))], history)
        for row in self.coord.db.execute('SELECT project,requester_session,requester_host FROM admin_ownership_dispositions WHERE request_id<>?', (new_id,)):
            self.assertEqual(tuple(row), (None, None, None))
        new = self.coord.db.execute('SELECT project,requester_session,requester_host FROM admin_ownership_dispositions WHERE request_id=?', (new_id,)).fetchone()
        self.assertEqual(tuple(new), ('default', 'root', 'host-root'))
        self.assertEqual(self.coord.db.execute('SELECT reservation_id FROM resources WHERE name=?', ('file:compact-unrelated.py',)).fetchone()[0], unrelated['reservation_id'])

    def test_unrecognized_audit_layout_refuses_before_sample_or_target_mutation(self):
        claim = self._stale('file:foreign-schema.py')
        request_id = str(uuid.uuid4())
        self._compact_audit(request_id, self._historic_payload(claim, request_id))
        self.coord.db.execute('ALTER TABLE admin_ownership_dispositions ADD COLUMN foreign_field TEXT')
        before = [tuple(row) for row in self.coord.db.execute('SELECT * FROM resources ORDER BY name')]
        audit = [tuple(row) for row in self.coord.db.execute('SELECT * FROM admin_ownership_dispositions ORDER BY request_id')]
        with patch('agent_chat.ownership_disposition._sample', side_effect=AssertionError('foreign layout must not sample')):
            with self.assertRaisesRegex(CoordError, 'schema is incompatible'):
                self._apply(claim)
        self.assertEqual([tuple(row) for row in self.coord.db.execute('SELECT * FROM resources ORDER BY name')], before)
        self.assertEqual([tuple(row) for row in self.coord.db.execute('SELECT * FROM admin_ownership_dispositions ORDER BY request_id')], audit)

    def test_legacy_blocked_without_sample_cannot_authorize_continuation(self):
        claim = self._stale('file:legacy-blocked.py')
        request_id = str(uuid.uuid4())
        self._legacy_audit(request_id, self._historic_payload(claim, request_id))
        self.coord.db.execute("UPDATE admin_ownership_dispositions SET outcome='blocked' WHERE request_id=?", (request_id,))
        audit = [tuple(row) for row in self.coord.db.execute('SELECT * FROM admin_ownership_dispositions ORDER BY request_id')]
        with self.assertRaisesRegex(CoordError, 'lacks continuation provenance'):
            self._apply(claim, continuation_request_id=request_id)
        self.assertEqual([tuple(row) for row in self.coord.db.execute('SELECT * FROM admin_ownership_dispositions ORDER BY request_id')], audit)
        self.assertEqual(self.coord.db.execute('SELECT reservation_id FROM resources WHERE name=?', (claim['resource'],)).fetchone()[0], claim['reservation_id'])
        self.assertNotIn('sample_json', {row['name'] for row in self.coord.db.execute('PRAGMA table_info(admin_ownership_dispositions)')})

    def test_legacy_audit_migrates_transactionally_without_fabricating_history(self):
        claim = self._stale('file:legacy-schema.py')
        request_id = str(uuid.uuid4())
        root = load_configured_root(self.master, self.plist)
        payload = _payload(root=root, project='default', resource=claim['resource'],
                           reservation_id=claim['reservation_id'], owner_session='old',
                           expected_state='stale', request_id=request_id,
                           reason='missing original evidence')
        payload_json = _json(payload)
        self._legacy_audit(request_id, payload_json)
        history = [tuple(row) for row in self.coord.db.execute('SELECT * FROM admin_ownership_disposition_history ORDER BY request_id,event')]
        before = self.coord.db.execute('''SELECT request_id,requester_session,requester_host,
            outcome,process_evidence_state FROM admin_ownership_dispositions
            WHERE request_id=?''', (request_id,)).fetchone()
        # DDL is in the caller's transaction: an interrupted migration does
        # not leave the legacy table partly altered.
        self.coord.db.execute('BEGIN IMMEDIATE')
        _ensure_audit_schema(self.coord.db)
        self.coord.db.rollback()
        columns = {row['name'] for row in self.coord.db.execute('PRAGMA table_info(admin_ownership_dispositions)')}
        self.assertNotIn('sample_json', columns)

        with patch('agent_chat.ownership_disposition._sample', side_effect=AssertionError('old replay must not sample')):
            replay = apply_local(master_db=self.master, service_plist=self.plist, project='default',
                                 resource=claim['resource'], reservation_id=claim['reservation_id'],
                                 owner_session='old', expected_state='stale', request_id=request_id,
                                 reason='missing original evidence')
        self.assertTrue(replay['replayed'])
        self.assertEqual(replay['outcome'], 'administratively_abandoned')
        self.assertIsNone(replay['sample'])
        # Replay is read-only; it neither applies a stale clear nor alters the
        # historic row simply because the new sampler understands it.
        self.assertEqual(self.coord.db.execute('SELECT reservation_id FROM resources WHERE name=?',
                                               (claim['resource'],)).fetchone()['reservation_id'],
                         claim['reservation_id'])
        self.assertEqual(tuple(self.coord.db.execute('''SELECT request_id,requester_session,requester_host,
            outcome,process_evidence_state FROM admin_ownership_dispositions WHERE request_id=?''',
            (request_id,)).fetchone()), tuple(before))
        self.assertEqual(self.coord.db.execute('SELECT count(*) FROM admin_ownership_dispositions').fetchone()[0], 21)

        successor = self._stale('file:legacy-successor.py')
        result = self._apply(successor)
        self.assertEqual(result['outcome'], 'administratively_abandoned')
        columns = {row['name'] for row in self.coord.db.execute('PRAGMA table_info(admin_ownership_dispositions)')}
        self.assertIn('sample_json', columns)
        old = self.coord.db.execute('SELECT sample_json,continuation_of FROM admin_ownership_dispositions WHERE request_id=?',
                                    (request_id,)).fetchone()
        self.assertEqual(tuple(old), (None, None))
        self.assertEqual([tuple(row) for row in self.coord.db.execute('SELECT * FROM admin_ownership_disposition_history WHERE request_id<>? ORDER BY request_id,event', (result['request_id'],))], history)
        with self.assertRaisesRegex(CoordError, 'exact original blocked'):
            self._apply(claim, continuation_request_id=request_id)

    def test_exact_replay_handoff_and_unrelated_preservation(self):
        claim = self._stale(); self.coord.session = 'waiter'; self.assertEqual(self.coord.request(claim['resource'], 5)['state'], 'queued')
        self.coord.session = 'other'; unrelated = self.coord.request('file:unrelated.py', 5)
        request_id = str(uuid.uuid4())
        first = self._apply(claim, request_id=request_id)
        with patch('agent_chat.ownership_disposition._sample', side_effect=AssertionError('replay must not recapture')):
            replay = apply_local(master_db=self.master, service_plist=self.plist, project='default', resource=claim['resource'],
                                 reservation_id=claim['reservation_id'], owner_session='old', expected_state='stale',
                                 request_id=request_id, reason='missing original evidence')
        self.assertEqual(first['outcome'], 'administratively_abandoned'); self.assertTrue(replay['replayed'])
        row = self.coord.db.execute('SELECT owner_session,reservation_id,token FROM resources WHERE name=?',(claim['resource'],)).fetchone()
        self.assertEqual(tuple(row),(None,None,None))
        self.assertEqual(self.coord.db.execute('SELECT session FROM resource_queue WHERE resource=?',(claim['resource'],)).fetchone()['session'],'waiter')
        self.assertEqual(self.coord.db.execute('SELECT reservation_id FROM resources WHERE name=?',('file:unrelated.py',)).fetchone()['reservation_id'], unrelated['reservation_id'])
        with self.assertRaisesRegex(CoordError, 'payload differs'):
            self._apply(claim, request_id=request_id, reason='changed')

    def test_successor_guard_known_work_and_host_all_block_without_change(self):
        for name, mutate in (
            ('file:successor.py', lambda claim: self.coord.db.execute('UPDATE resources SET owner_session=?,reservation_id=? WHERE name=?',('new','new-rid',claim['resource']))),
            ('file:guard.py', lambda claim: self.coord.db.execute('INSERT INTO guarded_runs VALUES(?,?,?,?,?,?,?,NULL)',('run',claim['resource'],claim['reservation_id'],'old',1,None,time.time()))),
            ('file:work.py', lambda claim: self.coord.db.execute("INSERT INTO bridge_jobs VALUES('job','thread-old','{}','prepared',NULL,NULL,?,?)",(time.time(),time.time()))),
            ('file:host.py', lambda claim: self.coord.db.execute("UPDATE remote_session_hosts SET host_id='other' WHERE session_id='old'")),
        ):
            claim = self._stale(name); mutate(claim)
            if name == 'file:host.py':
                with self.assertRaisesRegex(CoordError, 'configured local host'):
                    self._apply(claim)
                result = None
            else:
                result = self._apply(claim)
                self.assertEqual(result['outcome'],'blocked')
            self.assertIsNotNone(self.coord.db.execute('SELECT reservation_id FROM resources WHERE name=?',(name,)).fetchone()['reservation_id'])
            if name == 'file:work.py': self.coord.db.execute("DELETE FROM bridge_jobs WHERE id='job'")
            if name == 'file:host.py': self.coord.db.execute("UPDATE remote_session_hosts SET host_id='host-root' WHERE session_id='old'")

    def test_canonical_config_and_passive_reader_are_fail_closed(self):
        with self.assertRaisesRegex(CoordError, 'master database does not match'):
            load_configured_root(Path(self.tmp.name) / 'alternate.sqlite3', self.plist)
        wrong = Path(self.tmp.name) / 'other.plist'; wrong.write_bytes(self.plist.read_bytes())
        with self.assertRaisesRegex(CoordError, 'not the installed source'):
            load_configured_root(self.master, wrong)
        class FakeRpc:
            def __init__(self, endpoint, timeout): self.calls=[]
            def __enter__(self): return self
            def __exit__(self, *args): return None
            def request(self, method, params):
                self.calls.append((method,params))
                return {'thread':{'status':{'type':'notLoaded'},'canAcceptDirectInput':True}} if method == 'thread/read' else {'data':[]}
        with patch('agent_chat.ownership_disposition.RpcClient', FakeRpc):
            result = _sample('thread-old')
        self.assertEqual(result['state'],'notLoaded'); self.assertTrue(result['direct_input_available'])

    def test_sample_failure_and_interrupted_commit_leave_stale_hold_unchanged(self):
        claim = self._stale('file:sample-failure.py')
        with patch('agent_chat.ownership_disposition._sample', side_effect=CoordError('native sample unavailable')):
            with self.assertRaisesRegex(CoordError, 'native sample unavailable'):
                apply_local(master_db=self.master, service_plist=self.plist, project='default', resource=claim['resource'],
                            reservation_id=claim['reservation_id'], owner_session='old', expected_state='stale',
                            request_id=str(uuid.uuid4()), reason='missing original evidence')
        self.assertEqual(self.coord.db.execute('SELECT reservation_id FROM resources WHERE name=?',(claim['resource'],)).fetchone()['reservation_id'], claim['reservation_id'])
        request_id = str(uuid.uuid4())
        with patch('agent_chat.ownership_disposition._checks', side_effect=RuntimeError('synthetic interruption')):
            with self.assertRaisesRegex(RuntimeError, 'synthetic interruption'):
                self._apply(claim, request_id=request_id)
        self.assertEqual(self.coord.db.execute('SELECT reservation_id FROM resources WHERE name=?',(claim['resource'],)).fetchone()['reservation_id'], claim['reservation_id'])
        result = self._apply(claim, request_id=request_id)
        self.assertEqual(result['outcome'], 'administratively_abandoned', result)

    def test_native_queue_and_control_reset_are_pre_effect_denials(self):
        claim = self._stale('file:queue-control.py')
        self.coord.db.executescript('CREATE TABLE session_resets (id TEXT PRIMARY KEY,target_session TEXT,status TEXT);')
        self.coord.db.execute("INSERT INTO session_resets VALUES('reset','old','pending')")
        result = self._apply(claim)
        self.assertEqual(result['outcome'], 'blocked')
        self.assertIn('session reset', ' '.join(result['checks']['known_app_work']))
        self.assertEqual(self.coord.db.execute('SELECT reservation_id FROM resources WHERE name=?',(claim['resource'],)).fetchone()['reservation_id'], claim['reservation_id'])

    def test_passive_capture_rejects_delayed_queue_component(self):
        class FakeRpc:
            def __init__(self, endpoint, timeout): pass
            def __enter__(self): return self
            def __exit__(self, *args): return None
            def request(self, method, params):
                return {'thread':{'status':{'type':'idle'},'canAcceptDirectInput':False}} if method == 'thread/read' else {'data':[]}
        with patch('agent_chat.ownership_disposition.RpcClient', FakeRpc), \
             patch('agent_chat.ownership_disposition.time.monotonic', side_effect=(10.0, 15.1)):
            with self.assertRaisesRegex(CoordError, 'capture interval'):
                _sample('thread-old')

    def test_locked_registry_mapping_and_target_binding_interleavings_fail_closed(self):
        claim = self._stale('file:registry-race.py')
        sample = {'thread_id':'thread-old','state':'idle','turn_id':None,'queue_count':0,
                  'direct_input_available':False,'started_at':time.time(),'captured_at':time.time(),'elapsed_ms':1}
        def remove_mapping(*args, **kwargs):
            with contextlib.closing(sqlite3.connect(self.registry)) as db:
                db.execute("DELETE FROM projects WHERE id='default'"); db.commit()
            return sample
        with patch('agent_chat.ownership_disposition._sample', side_effect=remove_mapping):
            with self.assertRaisesRegex(CoordError, 'target project is absent'):
                apply_local(master_db=self.master, service_plist=self.plist, project='default', resource=claim['resource'],
                            reservation_id=claim['reservation_id'], owner_session='old', expected_state='stale',
                            request_id=str(uuid.uuid4()), reason='mapping changed')
        self.assertEqual(self.coord.db.execute('SELECT reservation_id FROM resources WHERE name=?',(claim['resource'],)).fetchone()['reservation_id'], claim['reservation_id'])
        with contextlib.closing(sqlite3.connect(self.registry)) as db:
            db.execute("INSERT INTO projects VALUES('default')"); db.commit()
        def change_binding(*args, **kwargs):
            self.coord.db.execute("UPDATE bridge_bindings SET thread_id='thread-new' WHERE session_id='old'")
            return sample
        with patch('agent_chat.ownership_disposition._sample', side_effect=change_binding):
            result = apply_local(master_db=self.master, service_plist=self.plist, project='default', resource=claim['resource'],
                                 reservation_id=claim['reservation_id'], owner_session='old', expected_state='stale',
                                 request_id=str(uuid.uuid4()), reason='binding changed')
        self.assertEqual(result['outcome'], 'blocked')
        self.assertIn('binding changed', ' '.join(result['checks']['blockers']))
        self.assertEqual(self.coord.db.execute('SELECT reservation_id FROM resources WHERE name=?',(claim['resource'],)).fetchone()['reservation_id'], claim['reservation_id'])

    def test_remote_dispatch_has_no_disposition_route(self):
        claim = self._stale('file:remote-negative.py')
        with self.assertRaisesRegex(CoordError, 'unsupported remote coordinator operation'):
            dispatch(self.coord, {'op': 'ownership-dispose', 'session': 'other', 'host_id': 'host', 'params': {}}, canonical_project='default')
        self.assertEqual(self.coord.db.execute('SELECT reservation_id FROM resources WHERE name=?',(claim['resource'],)).fetchone()['reservation_id'], claim['reservation_id'])

    def test_consumed_unacknowledged_reply_does_not_block_but_unconsumed_delivery_does(self):
        now = time.time()
        consumed = self._stale('file:consumed.py')
        self.coord.db.execute("INSERT INTO messages(id,sender_session,recipient_session,body,created_at,acked_at) VALUES('consumed','other','old','input',?,NULL)", (now,))
        self.coord.db.execute("INSERT INTO message_reads(message_id,reader_session,read_at,provenance) VALUES('consumed','old',?,'recipient_full_message')", (now,))
        self.coord.db.execute("INSERT INTO messages(id,sender_session,recipient_session,body,created_at,acked_at) VALUES('reply','old','other','reply',?,NULL)", (now,))
        self.coord.db.execute("INSERT INTO message_replies(message_id,reply_to) VALUES('reply','consumed')")
        self.assertEqual(self._apply(consumed)['outcome'], 'administratively_abandoned')

        unconsumed = self._stale('file:unconsumed.py')
        self.coord.db.execute("INSERT INTO messages(id,sender_session,recipient_session,body,created_at,acked_at) VALUES('ack-only','other','old','input',?,?)", (now, now))
        result = self._apply(unconsumed)
        self.assertEqual(result['outcome'], 'blocked')
        self.assertIn('acknowledged-but-unconsumed input ack-only', result['checks']['known_app_work'])
        self.assertEqual(self.coord.db.execute('SELECT reservation_id FROM resources WHERE name=?',(unconsumed['resource'],)).fetchone()['reservation_id'], unconsumed['reservation_id'])

    def test_privileged_consumption_requires_full_provenance_and_exact_recipient(self):
        self.coord.session = 'other'
        unrelated = self.coord.request('file:provenance-unrelated.py', 5)
        cases = (('legacy_unknown', 'old', False),
                 ('recipient_full_message', 'old', True),
                 ('recipient_context_full', 'old', True),
                 ('recipient_full_message', 'other', False))
        for index, (provenance, reader, accepted) in enumerate(cases):
            with self.subTest(provenance=provenance, reader=reader):
                message_id = 'provenance-' + str(index)
                claim = self._stale('file:provenance-' + str(index) + '.py')
                self.coord.db.execute('INSERT INTO messages(id,sender_session,recipient_session,body,created_at,acked_at) VALUES(?,?,?,?,?,NULL)',
                                      (message_id, 'other', 'old', 'synthetic input', time.time()))
                self.coord.db.execute('INSERT INTO message_reads(message_id,reader_session,read_at,provenance) VALUES(?,?,?,?)',
                                      (message_id, reader, time.time(), provenance))
                result = self._apply(claim)
                self.assertEqual(result['outcome'], 'administratively_abandoned' if accepted else 'blocked')
                expected_rid = None if accepted else claim['reservation_id']
                self.assertEqual(self.coord.db.execute('SELECT reservation_id FROM resources WHERE name=?', (claim['resource'],)).fetchone()[0], expected_rid)
                self.assertEqual(self.coord.db.execute('SELECT reservation_id FROM resources WHERE name=?', ('file:provenance-unrelated.py',)).fetchone()[0], unrelated['reservation_id'])
                self.coord.db.execute('DELETE FROM message_reads WHERE message_id=?', (message_id,))
                self.coord.db.execute('DELETE FROM messages WHERE id=?', (message_id,))

    def test_privileged_consumption_missing_provenance_refuses_without_transition(self):
        claim = self._stale('file:missing-provenance.py')
        self.coord.db.execute('DROP TABLE message_reads')
        self.coord.db.execute('CREATE TABLE message_reads(message_id TEXT,reader_session TEXT,read_at REAL,PRIMARY KEY(message_id,reader_session))')
        self.coord.db.execute("INSERT INTO messages(id,sender_session,recipient_session,body,created_at,acked_at) VALUES('unknown','other','old','synthetic input',?,?)", (time.time(), time.time()))
        self.coord.db.execute("INSERT INTO message_reads VALUES('unknown','old',?)", (time.time(),))
        before = tuple(self.coord.db.execute('SELECT * FROM resources WHERE name=?', (claim['resource'],)).fetchone())
        with self.assertRaisesRegex(CoordError, 'consumption provenance is unavailable'):
            self._apply(claim)
        self.assertEqual(tuple(self.coord.db.execute('SELECT * FROM resources WHERE name=?', (claim['resource'],)).fetchone()), before)
        self.assertFalse(self.coord.db.execute("SELECT 1 FROM sqlite_master WHERE name='admin_ownership_dispositions'").fetchone())

    def test_registry_lock_contention_refuses_without_target_change_then_succeeds(self):
        claim = self._stale('file:registry-lock.py')
        program = ('import sqlite3,sys; db=sqlite3.connect(sys.argv[1]); db.execute("BEGIN IMMEDIATE"); '
                   'print("ready", flush=True); sys.stdin.read(); db.rollback(); db.close()')
        holder = subprocess.Popen([sys.executable, '-c', program, str(self.registry)], stdin=subprocess.PIPE,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.assertEqual(holder.stdout.readline().strip(), 'ready')
        print('synthetic registry-lock holder pid=' + str(holder.pid), flush=True)
        try:
            with self.assertRaisesRegex(sqlite3.OperationalError, 'database is locked'):
                self._apply(claim)
            self.assertEqual(self.coord.db.execute('SELECT reservation_id FROM resources WHERE name=?',(claim['resource'],)).fetchone()['reservation_id'], claim['reservation_id'])
        finally:
            holder.stdin.close()
            self.assertEqual(holder.wait(timeout=5), 0)
            holder.stdout.close()
            holder.stderr.close()
        self.assertEqual(self._apply(claim)['outcome'], 'administratively_abandoned')

    def test_default_master_without_project_registry_is_valid(self):
        self.coord.db.execute("DELETE FROM meta WHERE key='project_registry'")
        claim = self._stale('file:default-no-registry.py')
        self.assertEqual(self._apply(claim)['outcome'], 'administratively_abandoned')

    def test_nondefault_requires_complete_project_metadata_lineage(self):
        project = 'project-nondefault'
        with contextlib.closing(sqlite3.connect(self.registry)) as db:
            db.execute('INSERT INTO projects VALUES(?)', (project,)); db.commit()
        path = self.master.parent / (self.master.name + '.projects') / project / 'state.sqlite3'
        path.parent.mkdir(parents=True)
        child = Coordinator(path)
        try:
            child.db.executescript('''CREATE TABLE bridge_bindings (session_id TEXT PRIMARY KEY,thread_id TEXT UNIQUE,parent_session TEXT,agent_path TEXT,updated_at REAL);
                CREATE TABLE remote_session_hosts (session_id TEXT PRIMARY KEY,host_id TEXT,registered_at REAL,last_seen_at REAL);
                CREATE TABLE bridge_jobs (id TEXT PRIMARY KEY,thread_id TEXT,payload TEXT,status TEXT,queue_id TEXT,error TEXT,created_at REAL,updated_at REAL);''')
            child.db.execute("INSERT INTO meta(key,value) VALUES('project_id',?)", (project,))
        finally:
            child.close()
        with self.assertRaisesRegex(CoordError, 'target metadata'):
            target_path(load_configured_root(self.master, self.plist), project)

    def test_all_approved_resource_forms_preserve_exact_stale_cas(self):
        for resource in ('file:entry.py', 'validation:admission', 'operation:root-disposition', 'git-index'):
            claim = self._stale(resource)
            if resource == 'git-index':
                self.coord.session = 'other'
                unrelated = self.coord.request('file:unrelated-git-index.py', 5)
            result = self._apply(claim)
            self.assertEqual(result['outcome'], 'administratively_abandoned')
            row = self.coord.db.execute('SELECT owner_session,reservation_id,stale FROM resources WHERE name=?', (resource,)).fetchone()
            self.assertEqual(tuple(row), (None, None, 0))
            if resource == 'git-index':
                retained = self.coord.db.execute('SELECT reservation_id FROM resources WHERE name=?', ('file:unrelated-git-index.py',)).fetchone()
                self.assertEqual(retained['reservation_id'], unrelated['reservation_id'])
        rejected = self._stale('git-index:entry')
        with self.assertRaisesRegex(CoordError, 'approved canonical resource form'):
            self._apply(rejected)
        self.assertEqual(self.coord.db.execute('SELECT reservation_id FROM resources WHERE name=?', ('git-index:entry',)).fetchone()['reservation_id'], rejected['reservation_id'])

    def test_blocked_continuation_rechecks_gates_and_is_exactly_replayable(self):
        claim = self._stale('file:continuation.py')
        original_id = str(uuid.uuid4())
        self.coord.db.execute('INSERT INTO guarded_runs VALUES(?,?,?,?,?,?,?,NULL)',
                              ('guard-original', claim['resource'], claim['reservation_id'], 'old', 1, None, time.time()))
        original = self._apply(claim, request_id=original_id)
        self.assertEqual(original['outcome'], 'blocked')
        with patch('agent_chat.ownership_disposition._sample', side_effect=AssertionError('historical replay must not sample')):
            replay = apply_local(master_db=self.master, service_plist=self.plist, project='default', resource=claim['resource'],
                                 reservation_id=claim['reservation_id'], owner_session='old', expected_state='stale',
                                 request_id=original_id, reason='missing original evidence')
        self.assertTrue(replay['replayed'])

        pending_id = str(uuid.uuid4())
        pending = self._apply(claim, request_id=pending_id, continuation_request_id=original_id)
        self.assertEqual(pending['outcome'], 'blocked')
        self.assertEqual(self.coord.db.execute('SELECT reservation_id FROM resources WHERE name=?',(claim['resource'],)).fetchone()['reservation_id'], claim['reservation_id'])
        self.coord.db.execute("UPDATE guarded_runs SET closed_at=? WHERE run_id='guard-original'", (time.time(),))

        continuation_id = str(uuid.uuid4())
        # Treat the first result as lost after its durable commit: same UUID
        # reconciles the immutable result without a second transition.
        committed = self._apply(claim, request_id=continuation_id, continuation_request_id=original_id)
        self.assertEqual(committed['outcome'], 'administratively_abandoned')
        replayed = self._apply(claim, request_id=continuation_id, continuation_request_id=original_id)
        self.assertTrue(replayed['replayed'])
        self.assertEqual(self.coord.db.execute('SELECT reservation_id FROM resources WHERE name=?',(claim['resource'],)).fetchone()['reservation_id'], None)

        with self.assertRaisesRegex(CoordError, 'exact original blocked'):
            self._apply(claim, request_id=str(uuid.uuid4()), continuation_request_id=str(uuid.uuid4()))
        with self.assertRaisesRegex(CoordError, 'payload or original blocked state differs'):
            self._apply(claim, request_id=str(uuid.uuid4()), continuation_request_id=original_id, reason='changed')
        competing = self._apply(claim, request_id=str(uuid.uuid4()), continuation_request_id=original_id)
        self.assertEqual(competing['outcome'], 'blocked')

    def test_continuation_rejects_changed_successor_before_transition(self):
        claim = self._stale('file:continuation-successor.py')
        original_id = str(uuid.uuid4())
        self.coord.db.execute('INSERT INTO guarded_runs VALUES(?,?,?,?,?,?,?,NULL)',
                              ('guard-successor', claim['resource'], claim['reservation_id'], 'old', 1, None, time.time()))
        self.assertEqual(self._apply(claim, request_id=original_id)['outcome'], 'blocked')
        self.coord.db.execute("UPDATE guarded_runs SET closed_at=? WHERE run_id='guard-successor'", (time.time(),))
        self.coord.db.execute('UPDATE resources SET owner_session=?,reservation_id=? WHERE name=?',
                              ('new-owner', 'new-rid', claim['resource']))
        result = self._apply(claim, request_id=str(uuid.uuid4()), continuation_request_id=original_id)
        self.assertEqual(result['outcome'], 'blocked')
        row = self.coord.db.execute('SELECT owner_session,reservation_id FROM resources WHERE name=?',(claim['resource'],)).fetchone()
        self.assertEqual(tuple(row), ('new-owner', 'new-rid'))

    def test_continuation_rejects_original_with_wrong_recorded_state(self):
        claim = self._stale('file:continuation-state.py')
        original_id = str(uuid.uuid4())
        self.coord.db.execute('INSERT INTO guarded_runs VALUES(?,?,?,?,?,?,?,NULL)',
                              ('guard-state', claim['resource'], claim['reservation_id'], 'old', 1, None, time.time()))
        self.assertEqual(self._apply(claim, request_id=original_id)['outcome'], 'blocked')
        self.coord.db.execute("UPDATE guarded_runs SET closed_at=? WHERE run_id='guard-state'", (time.time(),))
        self.coord.db.execute('UPDATE admin_ownership_dispositions SET checks_json=? WHERE request_id=?',
                              (json.dumps({'current_state': 'changed'}), original_id))
        with self.assertRaisesRegex(CoordError, 'original blocked state differs'):
            self._apply(claim, request_id=str(uuid.uuid4()), continuation_request_id=original_id)
        self.assertEqual(self.coord.db.execute('SELECT reservation_id FROM resources WHERE name=?',(claim['resource'],)).fetchone()['reservation_id'], claim['reservation_id'])

    def test_continuation_reconciles_after_durable_child_commit_before_output(self, layout='legacy'):
        historic = self._stale('file:prior-' + layout + '.py')
        historic_id = str(uuid.uuid4())
        fixture = self._legacy_audit if layout == 'legacy' else self._compact_audit
        fixture(historic_id, self._historic_payload(historic, historic_id))
        claim = self._stale('file:continuation-interrupted.py')
        original_id, continuation_id = str(uuid.uuid4()), str(uuid.uuid4())
        self.coord.db.execute('INSERT INTO guarded_runs VALUES(?,?,?,?,?,?,?,NULL)',
                              ('guard-interrupted', claim['resource'], claim['reservation_id'], 'old', 1, None, time.time()))
        self.assertEqual(self._apply(claim, request_id=original_id)['outcome'], 'blocked')
        self.coord.db.execute("UPDATE guarded_runs SET closed_at=? WHERE run_id='guard-interrupted'", (time.time(),))
        source = Path(__file__).parents[1] / 'src'
        program = '''import os,sys,time
from pathlib import Path
import agent_chat.service as service
from agent_chat.service import InstalledServerConfig
service._installed_server_config=lambda: InstalledServerConfig(Path(sys.argv[1]),Path(sys.argv[2]),Path(sys.argv[3]),Path(sys.argv[4]),Path(sys.argv[5]),int(sys.argv[6]),'127.0.0.1',8765)
import agent_chat.ownership_disposition as disposition
disposition._sample=lambda thread, endpoint: {'thread_id':thread,'state':'notLoaded','turn_id':None,'queue_count':0,'direct_input_available':False,'started_at':time.time(),'captured_at':time.time(),'elapsed_ms':1}
disposition.apply_local(master_db=sys.argv[4],service_plist=sys.argv[1],project='default',resource=sys.argv[7],reservation_id=sys.argv[8],owner_session='old',expected_state='stale',request_id=sys.argv[9],reason='missing original evidence',continuation_request_id=sys.argv[10])
os._exit(97)'''
        values = [str(self.plist), str(self.wrapper), str(self.executable), str(self.master),
                  str(self.config.working_directory), str(self.config.uid), claim['resource'], claim['reservation_id'],
                  continuation_id, original_id]
        child = subprocess.Popen([sys.executable, '-c', program, *values], env=dict(os.environ, PYTHONPATH=str(source)))
        print('synthetic continuation-commit child pid=' + str(child.pid), flush=True)
        self.assertEqual(child.wait(timeout=10), 97)
        replay = self._apply(claim, request_id=continuation_id, continuation_request_id=original_id)
        self.assertTrue(replay['replayed'])
        self.assertEqual(self.coord.db.execute('SELECT reservation_id FROM resources WHERE name=?',(claim['resource'],)).fetchone()['reservation_id'], None)

    def test_compact_continuation_reconciles_after_durable_child_commit(self):
        self.test_continuation_reconciles_after_durable_child_commit_before_output(layout='compact')

    def _historical_blocked_fixture(self, layout, claim, original_id):
        payload = self._historic_payload(claim, original_id)
        if layout == 'compact':
            self._compact_audit(original_id, payload)
        else:
            self._legacy_audit(original_id, payload)
            self.coord.db.execute("UPDATE admin_ownership_dispositions SET outcome='blocked' WHERE request_id=?", (original_id,))
            self.coord.db.execute("UPDATE admin_ownership_disposition_history SET event='blocked' WHERE request_id=? AND event='abandoned'", (original_id,))
        row = dict(self.coord.db.execute('SELECT * FROM admin_ownership_dispositions WHERE request_id=?', (original_id,)).fetchone())
        history = [tuple(item) for item in self.coord.db.execute('SELECT * FROM admin_ownership_disposition_history WHERE request_id=? ORDER BY event', (original_id,))]
        return row, history

    def _assert_historical_unchanged(self, original_id, before):
        old_row, old_history = before
        row = self.coord.db.execute('SELECT * FROM admin_ownership_dispositions WHERE request_id=?', (original_id,)).fetchone()
        self.assertEqual({key: row[key] for key in old_row}, old_row)
        self.assertEqual([tuple(item) for item in self.coord.db.execute('SELECT * FROM admin_ownership_disposition_history WHERE request_id=? ORDER BY event', (original_id,))], old_history)
        for key in ('requester_session', 'requester_host', 'sample_json'):
            if key not in old_row:
                self.assertIsNone(row[key] if key in row.keys() else None)

    def test_history_only_operation_preserves_legacy_and_replays_current(self, layout='legacy'):
        claim = self._stale('operation:historical-' + layout)
        original_id, current_id = str(uuid.uuid4()), str(uuid.uuid4())
        before = self._historical_blocked_fixture(layout, claim, original_id)
        self.coord.session = 'other'; unrelated = self.coord.request('file:history-unrelated.py', 5)
        self.coord.session = 'waiter'; self.assertEqual(self.coord.request(claim['resource'], 5)['state'], 'queued')
        with self.assertRaisesRegex(CoordError, 'lacks continuation provenance'):
            self._apply(claim, continuation_request_id=original_id)
        result = self._apply(claim, request_id=current_id, historical_blocked_request_id=original_id,
                             reason='current local approval')
        self.assertEqual((result['outcome'], result['historical_blocked_of']), ('administratively_abandoned', original_id))
        self._assert_historical_unchanged(original_id, before)
        self.assertEqual(self.coord.db.execute('SELECT reservation_id FROM resources WHERE name=?', ('file:history-unrelated.py',)).fetchone()[0], unrelated['reservation_id'])
        self.assertEqual(self.coord.db.execute('SELECT session FROM resource_queue WHERE resource=?', (claim['resource'],)).fetchone()[0], 'waiter')
        linked = self.coord.db.execute("SELECT count(*) FROM admin_ownership_disposition_history WHERE request_id=? AND event='historical-blocked-link'", (current_id,)).fetchone()[0]
        self.assertEqual(linked, 1)
        replay = self._apply(claim, request_id=current_id, historical_blocked_request_id=original_id,
                             reason='current local approval')
        self.assertEqual((replay['replayed'], replay['historical_blocked_of']), (True, original_id))
        with self.assertRaisesRegex(CoordError, 'retry payload differs'):
            self._apply(claim, request_id=current_id, historical_blocked_request_id=original_id, reason='changed')
        competing = self._apply(claim, historical_blocked_request_id=original_id, reason='current local approval')
        self.assertEqual(competing['outcome'], 'blocked')
        self.assertEqual(self.coord.db.execute("SELECT count(*) FROM admin_ownership_dispositions WHERE outcome='administratively_abandoned' AND request_id IN (?,?)", (current_id, competing['request_id'])).fetchone()[0], 1)

    def test_history_only_operation_preserves_compact_and_replays_current(self):
        self.test_history_only_operation_preserves_legacy_and_replays_current(layout='compact')

    def test_history_only_original_and_current_tuple_fail_closed(self):
        claim = self._stale('validation:historical-original')
        original_id = str(uuid.uuid4())
        self._historical_blocked_fixture('compact', claim, original_id)
        valid = self._historic_payload(claim, original_id)
        malformed = ('not-json', _json({'request_id': original_id}),
                     self._historic_payload(dict(claim, reservation_id='different-rid'), original_id))
        for payload_json in malformed:
            self.coord.db.execute('UPDATE admin_ownership_dispositions SET payload_json=? WHERE request_id=?', (payload_json, original_id))
            with self.assertRaises(CoordError):
                self._apply(claim, historical_blocked_request_id=original_id)
            self.assertEqual(self.coord.db.execute('SELECT count(*) FROM admin_ownership_dispositions').fetchone()[0], 3)
            self.assertEqual(self.coord.db.execute('SELECT reservation_id FROM resources WHERE name=?', (claim['resource'],)).fetchone()[0], claim['reservation_id'])
        self.coord.db.execute("UPDATE admin_ownership_dispositions SET payload_json=?,outcome='administratively_abandoned' WHERE request_id=?", (valid, original_id))
        with self.assertRaisesRegex(CoordError, 'exact blocked disposition'):
            self._apply(claim, historical_blocked_request_id=original_id)
        self.coord.db.execute("UPDATE admin_ownership_dispositions SET outcome='blocked' WHERE request_id=?", (original_id,))
        with self.assertRaisesRegex(CoordError, 'distinct request UUID'):
            self._apply(claim, request_id=original_id, historical_blocked_request_id=original_id)
        with self.assertRaisesRegex(CoordError, 'distinct operations'):
            self._apply(claim, historical_blocked_request_id=original_id, continuation_request_id=original_id)
        self.coord.db.execute('UPDATE resources SET owner_session=?,reservation_id=? WHERE name=?', ('successor', 'successor-rid', claim['resource']))
        result = self._apply(claim, historical_blocked_request_id=original_id)
        self.assertEqual(result['outcome'], 'blocked')
        self.assertEqual(tuple(self.coord.db.execute('SELECT owner_session,reservation_id FROM resources WHERE name=?', (claim['resource'],)).fetchone()), ('successor', 'successor-rid'))

    def test_history_only_current_pending_gate_and_payload_conflict_refuse(self):
        claim = self._stale('file:historical-pending.py')
        original_id, blocked_id = str(uuid.uuid4()), str(uuid.uuid4())
        before = self._historical_blocked_fixture('legacy', claim, original_id)
        self.coord.db.execute('INSERT INTO guarded_runs VALUES(?,?,?,?,?,?,?,NULL)',
                              ('guard-history', claim['resource'], claim['reservation_id'], 'old', 1, None, time.time()))
        blocked = self._apply(claim, request_id=blocked_id, historical_blocked_request_id=original_id)
        self.assertEqual(blocked['outcome'], 'blocked')
        self._assert_historical_unchanged(original_id, before)
        self.coord.db.execute("UPDATE guarded_runs SET closed_at=? WHERE run_id='guard-history'", (time.time(),))
        self.assertEqual(self._apply(claim, request_id=blocked_id, historical_blocked_request_id=original_id)['outcome'], 'blocked')
        self.assertEqual(self.coord.db.execute('SELECT reservation_id FROM resources WHERE name=?', (claim['resource'],)).fetchone()[0], claim['reservation_id'])
        # An actual legacy tuple column is authoritative evidence of a conflict,
        # even when the JSON tuple matches the explicitly selected current one.
        self.coord.db.execute("UPDATE admin_ownership_dispositions SET owner_session='conflict' WHERE request_id=?", (original_id,))
        with self.assertRaisesRegex(CoordError, 'tuple is conflicting'):
            self._apply(claim, historical_blocked_request_id=original_id)

    def _historical_child(self, claim, original_id, request_id, mode):
        source = Path(__file__).parents[1] / 'src'
        program = '''import os,sys,time
from pathlib import Path
import agent_chat.service as service
from agent_chat.service import InstalledServerConfig
service._installed_server_config=lambda: InstalledServerConfig(Path(sys.argv[1]),Path(sys.argv[2]),Path(sys.argv[3]),Path(sys.argv[4]),Path(sys.argv[5]),int(sys.argv[6]),'127.0.0.1',8765)
import agent_chat.ownership_disposition as disposition
def sample(thread, endpoint):
    if sys.argv[11]=='compete':
        print('ready',flush=True)
        sys.stdin.readline()
    return {'thread_id':thread,'state':'notLoaded','turn_id':None,'queue_count':0,'direct_input_available':False,'started_at':time.time(),'captured_at':time.time(),'elapsed_ms':1}
disposition._sample=sample
result=disposition.apply_local(master_db=sys.argv[4],service_plist=sys.argv[1],project='default',resource=sys.argv[7],reservation_id=sys.argv[8],owner_session='old',expected_state='stale',request_id=sys.argv[9],reason='current local approval',historical_blocked_request_id=sys.argv[10])
if sys.argv[11]=='interrupt': os._exit(97)
print(result['outcome'],flush=True)'''
        values = [str(self.plist), str(self.wrapper), str(self.executable), str(self.master),
                  str(self.config.working_directory), str(self.config.uid), claim['resource'],
                  claim['reservation_id'], request_id, original_id, mode]
        child = subprocess.Popen([sys.executable, '-c', program, *values],
                                 env=dict(os.environ, PYTHONPATH=str(source)), stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        print('historical current-operation child pid=' + str(child.pid) + ' mode=' + mode, flush=True)
        return child

    def test_history_only_concurrent_current_operations_clear_once(self):
        claim = self._stale('operation:historical-concurrent')
        original_id = str(uuid.uuid4())
        before = self._historical_blocked_fixture('compact', claim, original_id)
        children = [self._historical_child(claim, original_id, str(uuid.uuid4()), 'compete') for _ in range(2)]
        try:
            for child in children:
                self.assertEqual(child.stdout.readline().strip(), 'ready')
            # Both native captures reach the barrier before either final lock/CAS.
            for child in children:
                child.stdin.write('go\n'); child.stdin.flush()
            outcomes = []
            for child in children:
                output, error = child.communicate(timeout=10)
                self.assertEqual(child.returncode, 0, error)
                outcomes.append(output.strip())
            self.assertEqual(sorted(outcomes), ['administratively_abandoned', 'blocked'])
            self._assert_historical_unchanged(original_id, before)
            self.assertEqual(self.coord.db.execute("SELECT count(*) FROM admin_ownership_dispositions WHERE outcome='administratively_abandoned'").fetchone()[0], 1)
        finally:
            for child in children:
                if child.poll() is None:
                    child.terminate(); child.communicate(timeout=5)

    def test_history_only_durable_interruption_replays_new_operation(self):
        claim = self._stale('git-index')
        original_id, current_id = str(uuid.uuid4()), str(uuid.uuid4())
        before = self._historical_blocked_fixture('legacy', claim, original_id)
        child = self._historical_child(claim, original_id, current_id, 'interrupt')
        child.communicate(timeout=10)
        self.assertEqual(child.returncode, 97)
        replay = self._apply(claim, request_id=current_id, historical_blocked_request_id=original_id, reason='current local approval')
        self.assertEqual((replay['replayed'], replay['outcome'], replay['historical_blocked_of']), (True, 'administratively_abandoned', original_id))
        self._assert_historical_unchanged(original_id, before)
        self.assertEqual(self.coord.db.execute("SELECT count(*) FROM admin_ownership_disposition_history WHERE request_id=? AND event='abandoned'", (current_id,)).fetchone()[0], 1)


class LegacyGuardDispositionTests(unittest.TestCase):
    """Disposable application records; no native control or closure evidence."""
    _write_config = OwnershipDispositionTests._write_config
    tearDown = OwnershipDispositionTests.tearDown

    def setUp(self):
        OwnershipDispositionTests.setUp(self)
        self.coord.session = 'old'
        self.anchor = self.coord.request('validation-clone', 5)
        self.extra = self.coord.request('heavy-unity:1', 5)
        self.unselected = self.coord.request('operation:unselected', 5)
        self.deadline = time.time() - 10
        self.coord.db.execute('UPDATE resources SET deadline=?,stale=1 WHERE owner_session=?',
                              (self.deadline, 'old'))
        self.coord.db.execute('INSERT INTO guarded_runs VALUES(?,?,?,?,?,?,?,?)',
                              ('legacy-run', 'validation-clone', self.anchor['reservation_id'],
                               'old', 424242, None, self.deadline - 10, None))
        self.request_id = str(uuid.uuid4())

    def _arguments(self, **changes):
        value = dict(master_db=self.master, service_plist=self.plist, project='default',
                     resource='validation-clone', reservation_id=self.anchor['reservation_id'],
                     owner_session='old', expected_state='stale', request_id=self.request_id,
                     reason='neutral explicit administrative selection', guard_run_id='legacy-run',
                     expected_deadline=self.deadline, additional_obligations=[{
                         'resource': 'heavy-unity:1', 'reservation_id': self.extra['reservation_id'],
                         'owner_session': 'old', 'deadline': self.deadline}])
        value.update(changes)
        return value

    def _passive(self):
        now = time.time()
        return dict(thread_id='thread-old', state='notLoaded', turn_id=None, queue_count=0,
                    direct_input_available=False, started_at=now, captured_at=now, elapsed_ms=1)

    def _call(self, sample=None, side_effect=None, **changes):
        with patch.object(disposition, '_sample', return_value=sample or self._passive(),
                          side_effect=side_effect):
            return disposition.apply_legacy_local(**self._arguments(**changes))

    def _snapshot(self):
        return tuple(self.coord.db.iterdump())

    def _jq(self, value, predicate):
        # jq alone parses new JSON assertions, including persisted audit bytes.
        encoded = value if isinstance(value, str) else _json(value)
        result = subprocess.run(['jq', '-e', predicate], input=encoded, text=True,
                                capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr or result.stdout)

    def _refuses_unchanged(self, **changes):
        before = self._snapshot()
        with self.assertRaises(CoordError):
            self._call(**changes)
        self.assertEqual(self._snapshot(), before)

    def test_unbatched_selection_preserves_history_and_stale_ownership(self):
        before = [tuple(row) for row in self.coord.db.execute('SELECT * FROM resources ORDER BY name')]
        guards = list(self.coord.db.execute('SELECT * FROM guarded_runs'))
        result = self._call()
        self._jq(result, '.outcome == "legacy_stale_authority_invalidated" and .process_evidence == "UNVERIFIED" and .replayed == false')
        for old, new in zip(before, self.coord.db.execute('SELECT * FROM resources ORDER BY name')):
            expected = list(old)
            if new['name'] in ('validation-clone', 'heavy-unity:1'):
                expected[list(new.keys()).index('token')] = None
            self.assertEqual(tuple(new), tuple(expected))
        self.assertEqual([tuple(row) for row in self.coord.db.execute('SELECT * FROM guarded_runs')],
                         [tuple(row) for row in guards])
        audit = self.coord.db.execute('SELECT * FROM admin_ownership_dispositions WHERE request_id=?',
                                      (self.request_id,)).fetchone()
        self._jq(audit['checks_json'], '.intent.association_provenance == "NEW_ROOT_ADMINISTRATIVE_SELECTION" and .intent.additional_basis == "ROOT_SELECTED_ADDITIONAL_OBLIGATIONS" and .intent.current_host_basis == "CURRENT_REGISTERED_HOST" and .intent.historical_host == "UNKNOWN" and .intent.historical_thread == "UNKNOWN" and .intent.historical_batch == "UNKNOWN" and .intent.guard.run_id == "legacy-run" and (.intent.members|length) == 2')
        self.coord.session = 'old'
        with self.assertRaises(CoordError):
            self.coord.check('validation-clone', self.anchor['token'])
        with self.assertRaises(CoordError):
            self.coord.request('validation-clone', 5)

    def test_exact_replay_and_new_uuid_or_changed_intent_refuse(self):
        self._call()
        before = self._snapshot()
        with patch.object(disposition, '_sample', side_effect=AssertionError('replay sampled')):
            replay = disposition.apply_legacy_local(**self._arguments())
        self._jq(replay, '.replayed == true and .outcome == "legacy_stale_authority_invalidated"')
        self.assertEqual(self._snapshot(), before)
        self._refuses_unchanged(reason='changed intent')
        self._refuses_unchanged(request_id=str(uuid.uuid4()))

    def test_wrong_guard_member_owner_live_and_queue_refuse_without_mutation(self):
        for change in ({'guard_run_id': 'missing'}, {'reservation_id': 'wrong'},
                       {'expected_deadline': self.deadline - 1}, {'owner_session': 'other'},
                       {'additional_obligations': [{'resource': 'heavy-unity:1',
                         'reservation_id': 'wrong', 'owner_session': 'old', 'deadline': self.deadline}]}):
            with self.subTest(change=change):
                self._refuses_unchanged(**change)
        for update, parameters in (
            ('UPDATE resources SET stale=0,deadline=? WHERE name=?', (time.time()+600, 'heavy-unity:1')),
            ('UPDATE resources SET owner_session=? WHERE name=?', ('other', 'heavy-unity:1'))):
            self.coord.db.execute(update, parameters)
            self._refuses_unchanged()
            self.coord.db.execute('UPDATE resources SET owner_session=?,deadline=?,stale=1 WHERE name=?',
                                  ('old', self.deadline, 'heavy-unity:1'))
        self.coord.db.execute('INSERT INTO resource_queue(resource,session,queued_at) VALUES(?,?,?)',
                              ('heavy-unity:1', 'waiter', time.time()))
        self._refuses_unchanged()

    def test_current_registration_and_passive_state_refuse(self):
        for table, field, value in (('bridge_bindings', 'thread_id', 'changed-thread'),
                                    ('remote_session_hosts', 'host_id', 'foreign-host')):
            old = self.coord.db.execute(f'SELECT {field} FROM {table} WHERE session_id=?', ('old',)).fetchone()[0]
            self.coord.db.execute(f'UPDATE {table} SET {field}=? WHERE session_id=?', (value, 'old'))
            self._refuses_unchanged()
            self.coord.db.execute(f'UPDATE {table} SET {field}=? WHERE session_id=?', (old, 'old'))
        for changes in ({'state': 'active'}, {'turn_id': 'active-turn'}, {'queue_count': 1},
                        {'started_at': time.time()-10}, {'thread_id': 'other-thread'},
                        {'captured_at': time.time()+10}):
            sample = dict(self._passive(), **changes)
            before = self._snapshot()
            with self.assertRaises(CoordError):
                self._call(sample=sample)
            self.assertEqual(self._snapshot(), before)
        self.coord.db.execute("DELETE FROM remote_session_hosts WHERE session_id='old'")
        self._refuses_unchanged()

    def test_unread_control_and_wake_barriers_refuse(self):
        self.coord.session = 'other'
        message = self.coord.send('old', 'genuine neutral direct input')
        self._refuses_unchanged()
        self.coord.session = 'old'
        self.coord.message(message['id'])
        self.coord.db.execute("INSERT INTO bridge_jobs VALUES('wake_neutral','thread-old','','prepared',NULL,NULL,?,?)", (time.time(), time.time()))
        self._refuses_unchanged()
        self.coord.db.execute("UPDATE bridge_jobs SET status='cancelled' WHERE id='wake_neutral'")
        self.coord.db.execute('CREATE TABLE agent_control_requests(id TEXT,target_session TEXT,status TEXT)')
        self.coord.db.execute("INSERT INTO agent_control_requests VALUES('control','old','uncertain')")
        self._refuses_unchanged()
        self.coord.db.execute("UPDATE agent_control_requests SET status='completed'")
        self._call()

    def test_cas_binding_and_root_changes_during_capture_refuse(self):
        mutations = (
            "UPDATE resources SET deadline=deadline-1 WHERE name='heavy-unity:1'",
            "UPDATE bridge_bindings SET thread_id='other-thread' WHERE session_id='old'",
            "UPDATE guarded_runs SET pid=pid+1 WHERE run_id='legacy-run'",
            "UPDATE resources SET token='neutral-race' WHERE name='heavy-unity:1'")
        for sql in mutations:
            with self.subTest(sql=sql):
                def change(_thread, _endpoint, **_options):
                    self.coord.db.execute(sql)
                    self.after_race = self._snapshot()
                    return self._passive()
                with self.assertRaises(CoordError):
                    self._call(side_effect=change)
                self.assertEqual(self._snapshot(), self.after_race)
                self.coord.db.execute("UPDATE resources SET deadline=? WHERE name='heavy-unity:1'", (self.deadline,))
                self.coord.db.execute("UPDATE bridge_bindings SET thread_id='thread-old' WHERE session_id='old'")
                self.coord.db.execute("UPDATE guarded_runs SET pid=424242 WHERE run_id='legacy-run'")
                self.coord.db.execute("UPDATE resources SET token=? WHERE name='heavy-unity:1'", (self.extra['token'],))
        def root_change(_thread, _endpoint, **_options):
            with contextlib.closing(sqlite3.connect(self.registry)) as db:
                db.execute("UPDATE root_config SET host_id='changed-root-host'")
                db.commit()
            return self._passive()
        self._refuses_unchanged(side_effect=root_change)

    def test_mid_operation_failure_rolls_back_tokens_schema_and_audit(self):
        self.coord.db.execute("CREATE TRIGGER neutral_failure BEFORE UPDATE OF token ON resources WHEN OLD.name='heavy-unity:1' BEGIN SELECT RAISE(ABORT,'neutral second-member failure'); END")
        before = self._snapshot()
        with self.assertRaises(sqlite3.IntegrityError):
            self._call()
        self.assertEqual(self._snapshot(), before)

    def test_cli_explicit_tuple_selection_is_separate_from_ordinary_allowlist(self):
        arguments = self._arguments()
        argv = ['--master-db', str(self.master), '--service-plist', str(self.plist),
                '--project-id', 'default', '--resource', 'validation-clone',
                '--reservation-id', self.anchor['reservation_id'], '--owner-session', 'old',
                '--expected-state', 'stale', '--request-id', self.request_id,
                '--reason', arguments['reason'], '--legacy-guard-run-id', 'legacy-run',
                '--expected-deadline', str(self.deadline), '--additional-stale-obligation',
                'heavy-unity:1', self.extra['reservation_id'], 'old', str(self.deadline)]
        with patch.object(disposition, '_sample', return_value=self._passive()), patch('sys.stdout', new_callable=io.StringIO) as output:
            self.assertEqual(disposition.main(argv), 0)
        self._jq(output.getvalue(), '.outcome == "legacy_stale_authority_invalidated"')
        with self.assertRaises(CoordError):
            _payload(root=load_configured_root(self.master, self.plist), project='default',
                     resource='validation-clone', reservation_id=self.anchor['reservation_id'],
                     owner_session='old', expected_state='stale', request_id=str(uuid.uuid4()), reason='ordinary remains restricted')

    def test_remote_recorded_batch_is_preserved_not_reinterpreted(self):
        self.coord.db.execute('DELETE FROM guarded_runs')
        self.coord.db.executescript('''CREATE TABLE remote_guarded_runs (run_id TEXT PRIMARY KEY,
            resource TEXT, reservation_id TEXT, session_id TEXT, host_id TEXT, local_pid INTEGER,
            started_at REAL, closed_at REAL, close_evidence_sha256 TEXT);
            CREATE TABLE remote_guard_batches (run_id TEXT PRIMARY KEY,batch_id TEXT);''')
        for run, claim in (('legacy-run', self.anchor), ('extra-run', self.extra)):
            self.coord.db.execute('INSERT INTO remote_guarded_runs VALUES(?,?,?,?,?,?,?,?,?)',
                (run, claim['resource'], claim['reservation_id'], 'old', 'host-root', 424242,
                 self.deadline-10, None, None))
            self.coord.db.execute('INSERT INTO remote_guard_batches VALUES(?,?)', (run, 'recorded-batch'))
        before = [tuple(row) for row in self.coord.db.execute('SELECT * FROM remote_guard_batches ORDER BY run_id')]
        self.coord.db.execute("UPDATE remote_guarded_runs SET host_id='foreign' WHERE run_id='extra-run'")
        self._refuses_unchanged()
        self.coord.db.execute("UPDATE remote_guarded_runs SET host_id='host-root' WHERE run_id='extra-run'")
        result = self._call()
        self._jq(result, '.checks.intent.historical_host == "host-root" and .checks.intent.historical_batch == "recorded-batch" and .checks.intent.association_provenance == "NEW_ROOT_ADMINISTRATIVE_SELECTION" and (.checks.intent.associated_runs|length) == 2')
        self.assertEqual([tuple(row) for row in self.coord.db.execute('SELECT * FROM remote_guard_batches ORDER BY run_id')], before)
        self.assertEqual(self.coord.db.execute('SELECT count(*) FROM remote_guarded_runs WHERE closed_at IS NULL').fetchone()[0], 2)

    def test_blocked_history_link_is_immutable_and_cannot_be_replayed_as_new_selection(self):
        original = str(uuid.uuid4())
        historical = _json(dict(project='default', resource='validation-clone',
            reservation_id=self.anchor['reservation_id'], owner_session='old',
            expected_state='stale', request_id=original, reason='historic refusal'))
        OwnershipDispositionTests._compact_audit(self, original, historical)
        old = tuple(self.coord.db.execute('SELECT * FROM admin_ownership_dispositions WHERE request_id=?', (original,)).fetchone())
        self._refuses_unchanged(request_id=original)
        self._refuses_unchanged(historical_blocked_request_id=str(uuid.uuid4()))
        self._call(historical_blocked_request_id=original)
        current = self.coord.db.execute('SELECT * FROM admin_ownership_dispositions WHERE request_id=?', (original,)).fetchone()
        self.assertEqual(tuple(current)[:len(old)], old)
        self.assertIsNone(current['requester_session'])
        self._jq(self.coord.db.execute('SELECT details_json FROM admin_ownership_disposition_history WHERE request_id=? AND event=?', (self.request_id, 'historical-blocked-link')).fetchone()[0], '.link_scope == "HISTORY_ONLY"')

    def test_missing_provenance_control_reset_retired_and_quarantine_refuse(self):
        self.coord.db.execute('ALTER TABLE message_reads RENAME COLUMN provenance TO unavailable_provenance')
        self._refuses_unchanged()
        self.coord.db.execute('ALTER TABLE message_reads RENAME COLUMN unavailable_provenance TO provenance')
        for table, columns, values in (
            ('agent_controls', 'target_session TEXT,mode TEXT', ('old', 'stopped')),
            ('session_resets', 'target_session TEXT,status TEXT', ('old', 'uncertain')),
            ('root_team_reset_targets', 'session_id TEXT,thread_id TEXT', ('old', 'thread-old')),
            ('root_team_stops', 'active INTEGER', (1,))):
            self.coord.db.execute(f'CREATE TABLE {table} ({columns})')
            self.coord.db.execute(f'INSERT INTO {table} VALUES({",".join("?" for _ in values)})', values)
            self._refuses_unchanged()
            self.coord.db.execute(f'DROP TABLE {table}')
        self.coord.db.execute("INSERT INTO retired_sessions VALUES('old','old',1,2)")
        self._refuses_unchanged()

    def test_registry_lock_refuses_without_target_mutation_and_owned_wait(self):
        program = ('import sqlite3,sys; db=sqlite3.connect(sys.argv[1]); db.execute("BEGIN IMMEDIATE"); '
                   'print("ready",flush=True); sys.stdin.read(); db.rollback(); db.close()')
        child = subprocess.Popen([sys.executable, '-c', program, str(self.registry)],
                                 stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            self.assertEqual(child.stdout.readline().strip(), 'ready')
            print('OWNED_NEUTRAL_LOCK_CHILD_PID=' + str(child.pid), flush=True)
            before = self._snapshot()
            with self.assertRaisesRegex(sqlite3.OperationalError, 'database is locked'):
                self._call()
            self.assertEqual(self._snapshot(), before)
        finally:
            child.stdin.close()
            result = child.wait(timeout=5)
            print('OWNED_NEUTRAL_LOCK_CHILD_WAIT=' + str(result), flush=True)
            child.stdout.close(); child.stderr.close()
            self.assertEqual(result, 0)

    def test_ordinary_allowlisted_disposition_and_open_guard_denial_stay_distinct(self):
        self.coord.session = 'old'
        claim = self.coord.request('file:ordinary-neutral.py', 5)
        self.coord.db.execute('UPDATE resources SET deadline=? WHERE name=?', (self.deadline, claim['resource']))
        outcome = OwnershipDispositionTests._apply(self, claim)
        self._jq(outcome, '.outcome == "administratively_abandoned"')
        self.assertIsNone(self.coord.db.execute('SELECT owner_session FROM resources WHERE name=?', (claim['resource'],)).fetchone()[0])
        self.coord.db.execute("UPDATE guarded_runs SET resource='file:legacy-neutral.py' WHERE run_id='legacy-run'")
        self.coord.db.execute("UPDATE resources SET name='file:legacy-neutral.py' WHERE name='validation-clone'")
        guarded = dict(self.anchor, resource='file:legacy-neutral.py')
        blocked = OwnershipDispositionTests._apply(self, guarded)
        self._jq(blocked, '.outcome == "blocked" and (.checks.open_guards|length) == 1')
        self.assertEqual(self.coord.db.execute("SELECT owner_session FROM resources WHERE name='file:legacy-neutral.py'").fetchone()[0], 'old')

    def test_strict_passive_reader_rejects_wrong_thread_and_uncertain_turns(self):
        class Passive:
            def __enter__(inner):
                return inner

            def __exit__(inner, *args):
                return False

            def request(inner, method, params):
                return {'thread': self.native_thread} if method == 'thread/read' else {'data': [], 'nextCursor': None}

        for thread in ({'id': 'foreign', 'status': {'type': 'idle'}, 'turns': []},
                       {'id': 'thread-old', 'status': {'type': 'idle'}, 'turns': None},
                       {'id': 'thread-old', 'status': {'type': 'idle'}, 'turns': [{'status': 'inProgress'}]}):
            self.native_thread = thread
            with patch.object(disposition, 'RpcClient', return_value=Passive()), self.assertRaises(CoordError):
                disposition._sample('thread-old', strict=True)
        self.native_thread = {'id': 'thread-old', 'status': {'type': 'notLoaded'}, 'turns': []}
        with patch.object(disposition, 'RpcClient', return_value=Passive()):
            sample = disposition._sample('thread-old', strict=True)
        self.assertEqual((sample['state'], sample['queue_count'], sample['turn_id']), ('notLoaded', 0, None))

    def test_exact_selection_input_bounds_and_missing_guard_records_refuse(self):
        self._refuses_unchanged(additional_obligations=[dict(self._arguments()['additional_obligations'][0], owner_session='other')])
        self._refuses_unchanged(additional_obligations=[self._arguments()['additional_obligations'][0]] * 2)
        self._refuses_unchanged(expected_deadline='nan')
        self._refuses_unchanged(additional_obligations=[dict(self._arguments()['additional_obligations'][0], resource='missing')])
        self.coord.db.execute("UPDATE guarded_runs SET closed_at=? WHERE run_id='legacy-run'", (time.time(),))
        self._refuses_unchanged()
        self.coord.db.execute("DELETE FROM guarded_runs WHERE run_id='legacy-run'")
        self._refuses_unchanged()
