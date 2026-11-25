import contextlib
import sqlite3
import unittest

from agent_chat.core import CoordError
from agent_chat.turnover_bootstrap import admit, authorized, authorize_subordinate, check_scope


class Coord:
    def __init__(self, db): self.db = db
    def require_session(self): return 'root'
    @contextlib.contextmanager
    def tx(self):
        with self.db: yield self.db


class BootstrapAdmissionTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:'); self.db.row_factory = sqlite3.Row
        self.reg = sqlite3.connect(':memory:'); self.reg.row_factory = sqlite3.Row
        self.addCleanup(self.db.close); self.addCleanup(self.reg.close)
        self.db.executescript('''
        CREATE TABLE root_parked_creations(request_id,project_id,reset_id,stop_id,root_session,target_session,thread_id,host_id);
        CREATE TABLE root_parked_activations(request_id);
        CREATE TABLE session_resets(id,status,kind,requester_session,new_thread_id,settings_digest,new_settings_digest,queue_id);
        CREATE TABLE root_projections(session_id,home_session,thread_id,paused);
        CREATE TABLE root_team_stops(id,root_session,active);
        CREATE TABLE bridge_bindings(session_id,thread_id,parent_session);
        CREATE TABLE remote_session_hosts(session_id,host_id);
        CREATE TABLE project_orchestrators(session_id,root_session);
        CREATE TABLE agent_management(target_session,manager_session,request_id);
        CREATE TABLE root_team_reset_requests(id,root_session,project_id,status);
        CREATE TRIGGER root_team_reset_request_update BEFORE UPDATE ON root_team_reset_requests BEGIN SELECT RAISE(ABORT,'immutable'); END;
        CREATE TRIGGER root_team_reset_request_delete BEFORE DELETE ON root_team_reset_requests BEGIN SELECT RAISE(ABORT,'immutable'); END;
        INSERT INTO root_parked_creations VALUES('job','project','reset','stop','root','po','old','host');
        INSERT INTO root_parked_activations VALUES('job');
        INSERT INTO session_resets VALUES('job','parked','create','root','new','digest','digest',NULL);
        INSERT INTO root_projections VALUES('root','home','old',0);
        INSERT INTO root_team_stops VALUES('stop','root',1);
        INSERT INTO bridge_bindings VALUES('po','new',NULL);
        INSERT INTO bridge_bindings VALUES('root','old',NULL);
        INSERT INTO remote_session_hosts VALUES('po','host');
        INSERT INTO remote_session_hosts VALUES('root','host');
        INSERT INTO project_orchestrators VALUES('po','root');
        INSERT INTO agent_management VALUES('po','root','job');
        INSERT INTO root_team_reset_requests VALUES('reset','home','project','completed');
        ''')
        self.reg.executescript('''
        CREATE TABLE root_projects(project_id,session_id,orchestrator_session,request_id,joined);
        CREATE TABLE root_team_operation_projects(request_id,project_id,root_session);
        CREATE TABLE root_team_operations(id,root_session,resumed_at);
        INSERT INTO root_projects VALUES('project','root','po','job',1);
        INSERT INTO root_team_operation_projects VALUES('stop','project','root');
        INSERT INTO root_team_operations VALUES('stop','home',NULL);
        ''')
        self.coord = Coord(self.db)

    def test_exact_retry_is_idempotent(self):
        first = admit(self.coord, self.reg, 'job', True)
        self.assertEqual(first, admit(self.coord, self.reg, 'job', True))
        self.assertEqual(authorized(self.db, self.reg, 'job')['target_session'], 'po')
        self.db.execute("UPDATE session_resets SET status='completed',queue_id='existing'")
        self.assertEqual(first, admit(self.coord, self.reg, 'job', True))
        self.reg.execute("UPDATE root_projects SET orchestrator_session='other'")
        with self.assertRaises(CoordError): authorized(self.db, self.reg, 'job')

    def test_changed_designation_stop_or_binding_withholds_grant(self):
        for db, sql in ((self.reg, "UPDATE root_projects SET orchestrator_session='other'"),
                        (self.db, "UPDATE root_team_stops SET active=0"),
                        (self.db, "UPDATE bridge_bindings SET thread_id='other'")):
            db.execute('SAVEPOINT probe')
            db.execute(sql)
            with self.assertRaises(CoordError): check_scope(self.db, self.reg, 'job')
            db.execute('ROLLBACK TO probe'); db.execute('RELEASE probe')

    def test_unverified_settings_and_preexisting_queue_are_refused(self):
        self.db.execute("UPDATE session_resets SET new_settings_digest='different'")
        with self.assertRaises(CoordError): admit(self.coord, self.reg, 'job', True)
        self.db.execute("UPDATE session_resets SET new_settings_digest='digest', queue_id='existing'")
        with self.assertRaises(CoordError): admit(self.coord, self.reg, 'job', True)

    def test_admission_requires_root_and_confirmation(self):
        with self.assertRaises(CoordError): admit(self.coord, self.reg, 'job', False)
        self.coord.require_session = lambda: 'po'
        with self.assertRaises(CoordError): admit(self.coord, self.reg, 'job', True)

    def test_subordinate_requires_completed_bootstrap_and_exact_po(self):
        admit(self.coord, self.reg, 'job', True)
        with self.assertRaises(CoordError):
            authorize_subordinate(self.db, self.reg, 'job', 'po', 'new', 'host', 'model', 'high')
        self.db.execute("UPDATE session_resets SET status='completed'")
        authorize_subordinate(self.db, self.reg, 'job', 'po', 'new', 'host', 'model', 'high')
        for caller, thread, host, model, effort in (('other', 'new', 'host', 'model', 'high'),
                ('po', 'other', 'host', 'model', 'high'), ('po', 'new', 'host', '', 'high')):
            with self.assertRaises(CoordError):
                authorize_subordinate(self.db, self.reg, 'job', caller, thread, host, model, effort)
