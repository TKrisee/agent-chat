"""Local administrative disposition with audited retired Root intents."""
import contextlib
import sqlite3
import time
import unittest
import uuid
from unittest.mock import patch

from agent_chat.core import CoordError
from agent_chat import ownership_disposition as disposition
from agent_chat.root_restart import local_schema
import test_expired_claim_barriers as fixtures


class RetiredIntentDispositionTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.ExpiredClaimBarrierEligibilityTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        self.addCleanup(self.fixture.tearDown)
        self.master, self.registry = self.fixture.master, self.fixture.registry
        self.plist = self.fixture.plist
        self.requests = [self.fixture._install_cancelled_fresh_intent()[0],
                         self.fixture._install_retired_replacement_intent()[0]]
        self.coord = self.fixture.coord
        local_schema(self.coord.db)
        self.coord.session = 'root'
        self.coord.register('root')
        self.coord.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?,?,?)',
                              ('root', 'thread-root', None, None, time.time()))
        self.coord.db.execute('INSERT INTO remote_session_hosts VALUES(?,?,?,?)',
                              ('root', 'host-root', time.time(), time.time()))
        self.coord.db.execute('''CREATE TABLE IF NOT EXISTS root_projections
            (session_id TEXT PRIMARY KEY,home_project TEXT,home_session TEXT,thread_id TEXT)''')
        self.coord.db.execute('''INSERT INTO root_projections
            (session_id,home_project,home_session,thread_id) VALUES('root','default','root','thread-root')''')
        with self.registry_db() as db:
            db.execute('UPDATE root_team_operations SET resumed_at=?', (time.time(),))
            for request in self.requests:
                db.execute('''INSERT INTO root_fresh_projects
                    (request_id,project_id,root_session) VALUES(?,'default','root')''', (request,))
                self.coord.db.execute('''INSERT INTO root_fresh_gates
                    (request_id,root_session,old_thread_id,new_thread_id,active)
                    VALUES(?,'root','thread-root',NULL,0)''', (request,))

    @contextlib.contextmanager
    def registry_db(self):
        with contextlib.closing(sqlite3.connect(self.registry)) as db, db:
            yield db

    def load(self):
        return disposition.load_configured_root(self.master, self.plist, allow_retired_intents=True)

    def snapshot(self):
        with self.registry_db() as db:
            return tuple(db.iterdump())

    def claim(self):
        self.coord.session = 'owner'
        self.coord.register('worker')
        claim = self.coord.request('file:retired-fixture.py', 5)
        self.coord.db.execute('UPDATE resources SET deadline=? WHERE name=?',
                              (time.time() - 10, claim['resource']))
        self.coord.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?,?,?)',
                              ('owner', 'thread-owner', None, None, time.time()))
        self.coord.db.execute('INSERT INTO remote_session_hosts VALUES(?,?,?,?)',
                              ('owner', 'host-root', time.time(), time.time()))
        return dict(master_db=self.master, service_plist=self.plist, project='default',
                    resource=claim['resource'], reservation_id=claim['reservation_id'],
                    owner_session='owner', expected_state='stale', request_id=str(uuid.uuid4()),
                    reason='isolated retired-intent regression')

    def sample(self, *_):
        return dict(thread_id='thread-owner', state='idle', turn_id=None, queue_count=0,
                    direct_input_available=True, started_at=time.time(), captured_at=time.time(), elapsed_ms=1)

    def test_real_disposition_accepts_both_typed_histories_without_rewriting_registry(self):
        args = self.claim()
        before = self.snapshot()
        with patch.object(disposition, '_sample', side_effect=self.sample):
            result = disposition.apply_local(**args)
        self.assertEqual(result['outcome'], 'administratively_abandoned')
        self.assertEqual(result['process_evidence'], 'UNVERIFIED')
        self.assertEqual(before, self.snapshot())
        self.assertEqual(self.coord.db.execute('SELECT owner_session FROM resources WHERE name=?',
                                               (args['resource'],)).fetchone()[0], None)
        with self.assertRaisesRegex(CoordError, 'pending fresh-start'):
            disposition.load_configured_root(self.master, self.plist)

    def test_active_request_still_refuses(self):
        with self.registry_db() as db:
            db.execute("UPDATE root_fresh_requests SET status='created' WHERE id=?", (self.requests[0],))
        with self.assertRaisesRegex(CoordError, 'pending fresh-start'):
            self.load()

    def test_retired_label_without_terminal_audit_still_refuses(self):
        request_id = str(uuid.uuid4())
        with self.registry_db() as db:
            db.execute("INSERT INTO root_fresh_requests(id,status) VALUES(?,'retired')", (request_id,))
        with self.assertRaisesRegex(CoordError, 'pending fresh-start'):
            self.load()

    def test_other_project_gate_is_checked(self):
        target = self.master.parent / (self.master.name + '.projects') / 'project_other' / 'state.sqlite3'
        target.parent.mkdir(parents=True)
        with contextlib.closing(sqlite3.connect(target)) as db:
            self.coord.db.backup(db)
            db.execute("UPDATE meta SET value='project_other' WHERE key='project_id'")
            db.execute('UPDATE root_fresh_gates SET active=1')
            db.commit()
        with self.registry_db() as db:
            db.execute("INSERT INTO projects VALUES('project_other')")
            db.execute("INSERT INTO root_fresh_projects VALUES(?,'project_other','root')", (self.requests[0],))
        with self.assertRaisesRegex(CoordError, 'gate or current binding'):
            self.load()

    def test_inactive_gate_and_exact_current_binding_are_required(self):
        for table, field, value in [('root_fresh_gates', 'active', 1),
                                     ('bridge_bindings', 'thread_id', 'changed-thread'),
                                     ('remote_session_hosts', 'host_id', 'foreign-host')]:
            with self.subTest(table=table):
                before = self.coord.db.execute('SELECT '+field+' FROM '+table).fetchone()[0]
                self.coord.db.execute('UPDATE '+table+' SET '+field+'=?', (value,))
                with self.assertRaisesRegex(CoordError, 'gate or current binding'):
                    self.load()
                self.coord.db.execute('UPDATE '+table+' SET '+field+'=?', (before,))

    def test_missing_scope_and_missing_gate_refuse(self):
        with self.registry_db() as db:
            db.execute('DELETE FROM root_fresh_projects WHERE request_id=?', (self.requests[0],))
        with self.assertRaisesRegex(CoordError, 'scope is incomplete'):
            self.load()
        with self.registry_db() as db:
            db.execute("INSERT INTO root_fresh_projects VALUES(?,'default','root')", (self.requests[0],))
        self.coord.db.execute('DELETE FROM root_fresh_gates WHERE request_id=?', (self.requests[0],))
        with self.assertRaisesRegex(CoordError, 'gate or current binding'):
            self.load()

    def test_gate_change_after_sample_refuses_before_resource_or_audit_mutation(self):
        args = self.claim()
        def changed_sample(*args):
            self.coord.db.execute('UPDATE root_fresh_gates SET active=1')
            return self.sample()
        with patch.object(disposition, '_sample', side_effect=changed_sample):
            with self.assertRaisesRegex(CoordError, 'gate or current binding'):
                disposition.apply_local(**args)
        self.assertEqual(self.coord.db.execute('SELECT owner_session FROM resources WHERE name=?',
                                               (args['resource'],)).fetchone()[0], 'owner')
        self.assertFalse(self.coord.db.execute("SELECT 1 FROM sqlite_master WHERE name='admin_ownership_dispositions'").fetchone())
