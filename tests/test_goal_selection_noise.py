import sqlite3,tempfile,unittest
from contextlib import closing
from pathlib import Path
from agent_chat.goal_registry_read import select_designated_goal,require_current,GoalSelectionUnavailable
from agent_chat.goal_peer_read import SELECTED_HOST

class GoalSelectionNoiseTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
  self.rpath=Path(self.tmp.name)/'registry';self.ppath=Path(self.tmp.name)/'project'
  self.r=sqlite3.connect(self.rpath);self.p=sqlite3.connect(self.ppath)
  self.addCleanup(self.r.close);self.addCleanup(self.p.close)
  self.r.executescript("CREATE TABLE root_config(home_project,home_session,thread_id,host_id,mode,singleton); CREATE TABLE root_directory_owner(home_project,home_session,singleton); CREATE TABLE root_projects(project_id,session_id,orchestrator_session,joined); INSERT INTO root_config VALUES('home','root','root-thread','"+SELECTED_HOST+"','running',1); INSERT INTO root_directory_owner VALUES('home','root',1); INSERT INTO root_projects VALUES('proj','project-root','po',1);")
  self.p.executescript("CREATE TABLE meta(key,value); CREATE TABLE sessions(id); CREATE TABLE project_orchestrators(session_id,root_session); CREATE TABLE root_projections(session_id,home_project,home_session,thread_id); CREATE TABLE bridge_bindings(session_id,thread_id,parent_session,updated_at); CREATE TABLE remote_session_hosts(session_id,host_id); CREATE TABLE bridge_dispatcher(host_id,owner,secret_hash,acquired_at); CREATE TABLE session_resets(target_session,status); CREATE TABLE bridge_client_runtime(host_id PRIMARY KEY,server,pid,heartbeat,error,project_scope_worker,project_workspaces_worker); CREATE TABLE messages(id,body); INSERT INTO meta VALUES('project_id','proj'); INSERT INTO sessions VALUES('po'); INSERT INTO project_orchestrators VALUES('po','project-root'); INSERT INTO root_projections VALUES('project-root','home','root','root-thread'); INSERT INTO bridge_bindings VALUES('po','worker-thread',NULL,1); INSERT INTO remote_session_hosts VALUES('po','"+SELECTED_HOST+"'); INSERT INTO bridge_dispatcher VALUES('"+SELECTED_HOST+"','owner','secret',1); INSERT INTO bridge_client_runtime VALUES('"+SELECTED_HOST+"','ws://127.0.0.1:1',12,100,NULL,0,0);")
  from agent_chat.goal_selection_generation import install,REGISTRY,PROJECT
  install(self.r,REGISTRY);install(self.p,PROJECT);self.r.commit();self.p.commit()
 def change(self,sql,values=()):
  with closing(sqlite3.connect(self.ppath)) as writer:
   writer.execute(sql,values);writer.commit()
 def select(self):return select_designated_goal(self.r,self.p,'proj',now=100)
 def test_unrelated_chat_does_not_invalidate_current_selection(self):
  original=self.select();self.change("INSERT INTO messages VALUES('unrelated','hello')")
  self.assertEqual(require_current(original,self.r,self.p,now=100).thread_id,'worker-thread')
 def test_heartbeat_refresh_does_not_invalidate_current_selection(self):
  original=self.select();self.change('UPDATE bridge_client_runtime SET heartbeat=?',(101,))
  self.assertEqual(require_current(original,self.r,self.p,now=101).thread_id,'worker-thread')
 def test_binding_dispatcher_and_provider_changes_refuse(self):
  for sql in ("UPDATE bridge_bindings SET thread_id='other',updated_at=2", "UPDATE bridge_dispatcher SET owner='other',acquired_at=2", "UPDATE bridge_client_runtime SET pid=14"):
   with self.subTest(sql=sql):
    original=self.select();self.change(sql)
    with self.assertRaises(GoalSelectionUnavailable):require_current(original,self.r,self.p,now=100)
    self.change("UPDATE bridge_bindings SET thread_id='worker-thread',updated_at=1")
    self.change("UPDATE bridge_dispatcher SET owner='owner',acquired_at=1")
    self.change("UPDATE bridge_client_runtime SET pid=12")
 def test_actual_bridge_heartbeat_between_selection_and_revalidation(self):
  from agent_chat.bridge_state import BridgeState
  from unittest.mock import patch
  original=self.select()
  with closing(sqlite3.connect(self.ppath)) as writer:
   bridge=object.__new__(BridgeState);bridge.db=writer
   with patch('agent_chat.bridge_state.time.time',return_value=101):
    bridge.heartbeat('ws://127.0.0.1:1',12,host_id=SELECTED_HOST)
   writer.commit()
  self.assertEqual(require_current(original,self.r,self.p,now=101).thread_id,'worker-thread')
 def test_actual_heartbeat_rejects_changed_provider_and_error(self):
  from agent_chat.bridge_state import BridgeState
  from unittest.mock import patch
  for server,pid,error in (('ws://127.0.0.1:2',12,None),('ws://127.0.0.1:1',13,None),('ws://127.0.0.1:1',12,'failed')):
   with self.subTest(server=server,pid=pid,error=error):
    original=self.select()
    with closing(sqlite3.connect(self.ppath)) as writer:
     bridge=object.__new__(BridgeState);bridge.db=writer
     with patch('agent_chat.bridge_state.time.time',return_value=100):bridge.heartbeat(server,pid,error,host_id=SELECTED_HOST)
     writer.commit()
    with self.assertRaises(GoalSelectionUnavailable):require_current(original,self.r,self.p,now=100)
    self.change("UPDATE bridge_client_runtime SET server='ws://127.0.0.1:1',pid=12,error=NULL,heartbeat=100")
 def test_heartbeat_committed_during_snapshot_does_not_invalidate(self):
  from agent_chat.bridge_state import BridgeState
  from unittest.mock import patch
  self.p.execute('PRAGMA journal_mode=WAL');self.p.commit()
  original=self.select()
  writer=sqlite3.connect(self.ppath);bridge=object.__new__(BridgeState);bridge.db=writer
  changed=[]
  def trace(sql):
   if not changed and 'SELECT thread_id,parent_session,updated_at' in sql:
    changed.append(True)
    with patch('agent_chat.bridge_state.time.time',return_value=101):bridge.heartbeat('ws://127.0.0.1:1',12,host_id=SELECTED_HOST)
    writer.commit()
  try:
   self.p.set_trace_callback(trace)
   observed=select_designated_goal(self.r,self.p,'proj',now=100)
  finally:
   self.p.set_trace_callback(None);writer.close()
  self.assertTrue(changed)
  self.assertEqual(observed.designation_generation,original.designation_generation)
  self.assertEqual(require_current(observed,self.r,self.p,now=101).thread_id,'worker-thread')
 def test_caller_transaction_is_not_committed_or_rolled_back(self):
  self.p.execute('BEGIN')
  try:
   with self.assertRaises(GoalSelectionUnavailable):self.select()
   self.assertTrue(self.p.in_transaction)
  finally:self.p.rollback()
 def test_relevant_change_then_revert_rejects_aba(self):
  for first,second in (("UPDATE root_projections SET thread_id='other'", "UPDATE root_projections SET thread_id='root-thread'"),
                       ("UPDATE bridge_bindings SET thread_id='other'", "UPDATE bridge_bindings SET thread_id='worker-thread'"),
                       ("UPDATE bridge_dispatcher SET secret_hash='rotated'", "UPDATE bridge_dispatcher SET secret_hash='secret'"),
                       ("UPDATE bridge_client_runtime SET pid=99", "UPDATE bridge_client_runtime SET pid=12")):
   with self.subTest(first=first):
    original=self.select();self.change(first);self.change(second)
    with self.assertRaises(GoalSelectionUnavailable):require_current(original,self.r,self.p,now=100)
 def test_unrelated_identity_row_does_not_invalidate(self):
  original=self.select();self.change("INSERT INTO sessions VALUES('other-worker')")
  self.assertEqual(require_current(original,self.r,self.p,now=100).thread_id,'worker-thread')
 def test_legacy_missing_hook_keeps_strict_fence(self):
  self.p.execute('DROP TRIGGER goal_generation_bridge_bindings_au');self.p.commit()
  original=self.select();self.change("INSERT INTO messages VALUES('legacy-noise','hello')")
  with self.assertRaises(GoalSelectionUnavailable):require_current(original,self.r,self.p,now=100)
 def test_tampered_hook_fails_closed(self):
  self.p.execute('DROP TRIGGER goal_generation_bridge_bindings_au')
  self.p.execute('CREATE TRIGGER goal_generation_bridge_bindings_au AFTER UPDATE ON bridge_bindings BEGIN SELECT 1; END');self.p.commit()
  with self.assertRaises(GoalSelectionUnavailable):self.select()
 def test_selected_reset_created_and_removed_rejects(self):
  original=self.select();self.change("INSERT INTO session_resets VALUES('po','pending')");self.change("DELETE FROM session_resets")
  with self.assertRaises(GoalSelectionUnavailable):require_current(original,self.r,self.p,now=100)
 def test_registry_and_host_away_and_back_rejected(self):
  for path,first,second in ((self.rpath,"UPDATE root_projects SET orchestrator_session='other'","UPDATE root_projects SET orchestrator_session='po'"),
                            (self.ppath,"UPDATE remote_session_hosts SET host_id='other'","UPDATE remote_session_hosts SET host_id='"+SELECTED_HOST+"'")):
   with self.subTest(path=path):
    original=self.select()
    with closing(sqlite3.connect(path)) as writer:
     writer.execute(first);writer.commit();writer.execute(second);writer.commit()
    with self.assertRaises(GoalSelectionUnavailable):require_current(original,self.r,self.p,now=100)
 def test_migration_rejects_inflight_legacy_then_accepts_later_noise(self):
  from agent_chat.goal_selection_generation import install,PROJECT
  self.p.execute('DROP TRIGGER goal_generation_bridge_bindings_au');self.p.commit()
  original=self.select()
  install(self.p,PROJECT);self.p.commit()
  with self.assertRaises(GoalSelectionUnavailable):require_current(original,self.r,self.p,now=100)
  migrated=self.select();self.change("INSERT INTO messages VALUES('after-migration','hello')")
  self.assertEqual(require_current(migrated,self.r,self.p,now=100).thread_id,'worker-thread')

 def test_hook_drop_and_restore_between_reads_rejects(self):
  from agent_chat.goal_selection_generation import install,PROJECT
  original=self.select();self.change('DROP TRIGGER goal_generation_bridge_bindings_au')
  with closing(sqlite3.connect(self.ppath)) as writer:install(writer,PROJECT);writer.commit()
  with self.assertRaises(GoalSelectionUnavailable):require_current(original,self.r,self.p,now=100)


class GoalMigrationTests(unittest.TestCase):
 def test_existing_registry_and_project_schema_setup_installs_hooks(self):
  from agent_chat.core import Coordinator
  from agent_chat.projects import Projects
  from agent_chat.root_orchestrator import RootOrchestrator
  from agent_chat.bridge_state import BridgeState
  from agent_chat.bridge_lease import BridgeManager
  from agent_chat.remote_service import _schema
  from agent_chat.goal_selection_generation import install,validate,REGISTRY,PROJECT
  with tempfile.TemporaryDirectory() as directory:
   path=Path(directory)/'state.sqlite'
   projects=Projects(path);RootOrchestrator(projects)
   with projects.connection() as registry:self.assertTrue(validate(registry,REGISTRY))
   with closing(Coordinator(path,'po')) as coord:
    BridgeState(coord);_schema(coord)
    coord.db.execute('CREATE TABLE IF NOT EXISTS root_projections(session_id TEXT PRIMARY KEY,home_project,home_session,thread_id)')
    coord.db.execute('CREATE TABLE IF NOT EXISTS project_orchestrators(session_id TEXT PRIMARY KEY,root_session)')
    install(coord.db,PROJECT)
   bridge=BridgeManager(path);bridge.close()
   with closing(sqlite3.connect(path)) as db:self.assertTrue(validate(db,PROJECT))
