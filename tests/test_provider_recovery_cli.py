"""Private configured-Root status/auth tests; never execute lifecycle/provider calls."""
import contextlib,io,os,plistlib,sqlite3,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from agent_chat import root_local,service,cli
from agent_chat import provider_recovery_lifecycle as lifecycle
from agent_chat import provider_recovery_peer as peer
from agent_chat.provider_recovery_wire import field,jq

class RecoveryAdminStatus(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.home=Path(self.temp.name);self.master=self.home/'master.sqlite3'
        with contextlib.closing(sqlite3.connect(self.master)) as db:
            db.executescript('CREATE TABLE bridge_bindings(session_id,thread_id,parent_session);CREATE TABLE remote_session_hosts(session_id,host_id);CREATE TABLE messages(id,sender_session,recipient_session,body);CREATE TABLE operator_identities(session_id);CREATE TABLE operator_message_origins(message_id,sender_session,channel);CREATE TABLE bridge_jobs(id,thread_id,status);')
            db.execute("INSERT INTO bridge_bindings VALUES('root','private-thread',NULL)");db.execute("INSERT INTO remote_session_hosts VALUES('root','private-host')")
            db.execute("INSERT INTO operator_identities VALUES('operator')")
            for source in ('source','different-source'):
                db.execute('INSERT INTO messages VALUES(?,?,?,?)',(source,'operator','root','Scoped private administrative recovery '+source))
                db.execute('INSERT INTO operator_message_origins VALUES(?,?,?)',(source,'operator','authenticated_operator_ui'))
            db.commit()
        with contextlib.closing(sqlite3.connect(str(self.master)+'.projects.sqlite3')) as db:
            db.executescript("CREATE TABLE root_config(singleton,home_project,home_session,host_id,thread_id);CREATE TABLE root_directory_owner(singleton,home_project,home_session);INSERT INTO root_config VALUES(1,'default','root','private-host','private-thread');INSERT INTO root_directory_owner VALUES(1,'default','root');");db.commit()
        exe=self.home/'server';exe.write_text('inert private configured-service marker')
        wrapper=self.home/'wrapper';wrapper.write_text('exec '+str(exe)+' --db '+str(self.master)+'\n')
        plist=self.home/'server.plist';plist.write_bytes(plistlib.dumps(dict(Label='com.agent-chat.server',AssociatedBundleIdentifiers=['com.agent-chat.server'],WorkingDirectory=str(self.home),RunAtLoad=True,KeepAlive=True,ThrottleInterval=10,ProgramArguments=[str(wrapper)],EnvironmentVariables={})))
        config=service.InstalledServerConfig(plist,wrapper,exe,self.master,self.home,os.getuid(),'127.0.0.1',59999)
        replacement=patch.object(service,'_installed_server_config',return_value=config);replacement.start();self.addCleanup(replacement.stop)
        authority=lifecycle.AdministrativeAuthority('source')
        directory=self.home/'provider-recovery-v3';directory.mkdir(mode=0o700)
        self.journal=directory/'requests.sqlite3'
        journal=peer.v1.Journal(self.journal)
        try:
            selected=peer.old_selection(os.getuid(),999999,1,0,'ws://127.0.0.1:59999')
            plan='private-manifest\nprivate-pin\n'+repr(authority.root)+'\nsource\n'+authority.source_digest
            journal.claim('request',selected,plan);journal.save('request','stop_intent','effect outcome UNKNOWN','stop_unknown')
        finally:journal.close()
    def call(self,action='status',source='source',session='root'):
        output=io.StringIO()
        args=['provider-recovery',action,'--request-id','request','--source-id',source,'--home-session',session,'--host-id','private-host']
        with contextlib.redirect_stdout(output):code=cli._dispatch_main(['root-local']+args,None,None,None,None)
        return code,jq(output.getvalue(),'.')
    def test_status_is_authenticated_and_unknown_is_not_replayed(self):
        with patch.object(lifecycle,'recover_uncertain',side_effect=AssertionError('no lifecycle execution')):
            first=self.call();self.assertEqual(first,self.call())
        self.assertEqual(first[0],0);self.assertEqual(field(first[1],'.state'),'stop_intent')
        self.assertEqual(field(first[1],'.effects'),'stop_unknown');self.assertEqual(field(first[1],'.ready|tostring'),'false')
        with contextlib.closing(sqlite3.connect(self.journal)) as db:
            self.assertEqual(db.execute('SELECT state,effects FROM requests').fetchall(),[('stop_intent','stop_unknown')])
            self.assertEqual(db.execute('SELECT count(*) FROM observations').fetchone()[0],0)
    def test_different_authenticated_source_cannot_read_or_reconcile_request(self):
        with patch.object(peer.v1,'reconcile_private',side_effect=AssertionError('no OS observation')):
            for action in ('status','reconcile'):
                code,raw=self.call(action,source='different-source');self.assertEqual(code,2)
                self.assertIn('request does not belong',field(raw,'.error'))
    def test_unknown_source_and_wrong_root_are_refused(self):
        self.assertEqual(self.call(source='unknown')[0],2)
        code,raw=self.call(session='other-root');self.assertEqual(code,2)
        self.assertIn('Root/session/host assertion differs',field(raw,'.error'))
