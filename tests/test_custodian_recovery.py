"""Isolated fixed-provider LOCAL ADMIN workflows; no production metadata access."""
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import plistlib
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from unittest import mock

from agent_chat import cli, custodian_recovery as admin, service
from agent_chat.core import Coordinator, CoordError
from agent_chat.lifecycle_producer import main as enroll
from agent_chat.private_artifacts import atomic_capture
from agent_chat.remote import HttpClient, RemoteCoordError
from test_remote_web import RemoteWebFixture, TOKEN


class CustodianRecoveryTests(RemoteWebFixture):
    def setUp(self):
        super().setUp()
        self.call('register', agent='fixture-original-owner')
        self.claim = self.call('request', resource='shared', minutes=1)
        producer = Path(self.tmp.name) / 'private-producer'
        with contextlib.redirect_stdout(io.StringIO()):
            enroll(['--db', str(self.db), '--owner', 'a', '--host', 'mac', '--capability-file', str(producer)])
        cap = Path(self.tmp.name) / 'original-capability'
        atomic_capture(cap,dict(resource='shared',session_id='a',host_id='mac',reservation_id=self.claim['reservation_id'],token=self.claim['token'],server=self.url,project='default'))
        env = dict(os.environ,AGENT_CHAT_SERVER=self.url,AGENT_CHAT_API_TOKEN=TOKEN,AGENT_CHAT_SESSION='a',AGENT_CHAT_HOST_ID='mac',AGENT_CHAT_TOKEN=self.claim['token'],AGENT_CHAT_PRODUCER_CAPABILITY_FILE=str(producer),AGENT_CHAT_STATE_DIR=str(Path(self.tmp.name)/'client'))
        original = HttpClient.call
        def disconnect(client,path,payload,**kw):
            if payload.get('op')=='close-guard': raise RemoteCoordError('fixture transport loss')
            return original(client,path,payload,**kw)
        with mock.patch.dict(os.environ,env,clear=True),mock.patch.object(HttpClient,'call',disconnect):
            with self.assertRaises(RemoteCoordError):
                cli.main(['run','--reservation-id',self.claim['reservation_id'],'--capability-file',str(cap),'shared','--','/usr/bin/true'])
        cap.unlink()
        self.claim.pop('token')
        self.sql("UPDATE resources SET deadline=?,stale=1 WHERE name='shared'",time.time()-1)
        self.run = self.sql('SELECT run_id FROM remote_guarded_runs')[0][0]
        root = Path(self.tmp.name).resolve()
        provider = root/'fixed-provider';provider.mkdir(mode=0o700)
        work = root/'work';work.mkdir(mode=0o700)
        self.cfg = service.InstalledServerConfig(plist=provider/'server.plist',wrapper=provider/'wrapper',executable=provider/'server-executable',master=self.db.resolve(),working_directory=work,uid=os.geteuid(),host='127.0.0.1',port=8765)
        self.cfg.executable.write_text('#!/bin/sh\nexit 0\n');self.cfg.executable.chmod(0o700)
        self.cfg.wrapper.write_text('exec '+str(self.cfg.executable)+' --db '+str(self.cfg.master)+' --host 127.0.0.1 --port 8765\n');self.cfg.wrapper.chmod(0o700)
        self.cfg.plist.write_bytes(plistlib.dumps(dict(Label='com.agent-chat.server',AssociatedBundleIdentifiers=['com.agent-chat.server'],WorkingDirectory=str(work),RunAtLoad=True,KeepAlive=True,ThrottleInterval=10,ProgramArguments=[str(self.cfg.wrapper)],EnvironmentVariables={})));self.cfg.plist.chmod(0o600)
        self.cfg.master.chmod(0o600)
        for suffix in ('-wal','-shm','-journal'):
            sidecar=Path(str(self.cfg.master)+suffix)
            if sidecar.exists():sidecar.chmod(0o600)
        self.provider_patch=mock.patch.object(service,'_installed_server_config',return_value=self.cfg);self.provider_patch.start();self.addCleanup(self.provider_patch.stop)
        self.target_patch=mock.patch.object(admin,'TARGET',dict(project='default',resource='shared',reservation_id=self.claim['reservation_id'],run_id=self.run));self.target_patch.start();self.addCleanup(self.target_patch.stop)

    def sql(self,statement,*args):
        with contextlib.closing(sqlite3.connect(self.db)) as db,db:return db.execute(statement,args).fetchall()

    def state(self):
        with contextlib.closing(sqlite3.connect(self.db)) as db:return hashlib.sha256('\n'.join(db.iterdump()).encode()).hexdigest()

    def execute(self,operation,**kw):
        with mock.patch.dict(os.environ,{},clear=True):return admin.execute(operation,**kw)

    def prepare(self):
        pre=self.execute('preflight');self.assertTrue(pre['eligible'])
        return self.execute('prepare',expected_prestate=pre['prestate_sha256'],authority_reference='fixture-explicit-admin-reference')

    def refused(self,operation,**kw):
        before=self.state()
        with self.assertRaises(CoordError):self.execute(operation,**kw)
        self.assertEqual(self.state(),before)

    def test_actual_local_cli_fixed_provider_positive_and_readonly_fifo_history(self):
        self.call('register',session='b',host='host-b',agent='next-owner')
        self.call('request',session='b',host='host-b',resource='shared',minutes=1)
        queue=self.sql('SELECT * FROM resource_queue');history=self.sql('SELECT * FROM remote_reservation_outcomes');closures=self.sql('SELECT * FROM current_instance_closures')
        def command(args):
            out=io.StringIO()
            with mock.patch.dict(os.environ,{},clear=True),contextlib.redirect_stdout(out):self.assertEqual(admin.main(args),0)
            return out.getvalue()
        before=self.state();metadata=command(['custody']);self.assertEqual(self.state(),before)
        import subprocess
        subprocess.run(['jq','-e','.admitted and .actor.kind=="local_server_custodian"'],input=metadata,text=True,capture_output=True,check=True)
        before=self.state();pre=command(['preflight']);self.assertEqual(self.state(),before)
        pin=subprocess.run(['jq','-er','.prestate_sha256'],input=pre,text=True,capture_output=True,check=True).stdout.strip()
        prepared=command(['prepare','--expected-prestate',pin,'--authority-reference','fixture-explicit-admin-reference'])
        iid=subprocess.run(['jq','-er','.intent_id'],input=prepared,text=True,capture_output=True,check=True).stdout.strip()
        applied=command(['apply','--intent-id',iid])
        subprocess.run(['jq','-e','.actor_kind=="local_server_custodian" and .outcome=="admin_present_abandonment" and .task_result=="unknown" and .capability_issued==false and .native_admission==false'],input=applied,text=True,capture_output=True,check=True)
        before=self.state();status=command(['status','--intent-id',iid]);self.assertEqual(self.state(),before)
        subprocess.run(['jq','-e','.state=="applied"'],input=status,text=True,capture_output=True,check=True)
        self.assertEqual(self.sql('SELECT * FROM resource_queue'),queue);self.assertEqual(self.sql('SELECT * FROM remote_reservation_outcomes'),history);self.assertEqual(self.sql('SELECT * FROM current_instance_closures'),closures)
        self.assertEqual(self.sql('SELECT count(*) FROM receipts'),[(0,)])
        self.assertEqual(self.call('request',resource='shared',minutes=1)['state'],'queued')
        self.assertEqual(self.call('request',session='b',host='host-b',resource='shared',minutes=1)['state'],'owned')
        self.refused('apply',intent_id=iid)

    def test_missing_original_entire_execution_evidence_refuses_without_schema(self):
        self.sql('DELETE FROM current_instance_closures')
        before=self.state();pre=self.execute('preflight');self.assertFalse(pre['eligible']);self.assertEqual(self.state(),before)
        self.refused('prepare',expected_prestate=pre['prestate_sha256'],authority_reference='fixture')
        self.assertEqual(self.sql("SELECT count(*) FROM sqlite_master WHERE name LIKE 'custodian_%'"),[(0,)])

    def test_wrong_account_environment_provider_and_unsafe_metadata_refuse(self):
        with mock.patch.object(admin.os,'geteuid',return_value=self.cfg.uid+1):self.refused('preflight')
        before=self.state()
        with mock.patch.dict(os.environ,{'AGENT_CHAT_DB':'/not-the-installed-store'},clear=True):
            with self.assertRaises(CoordError):admin.execute('preflight')
        self.assertEqual(self.state(),before)
        self.cfg.plist.chmod(0o666);self.refused('preflight');self.cfg.plist.chmod(0o600)
        self.cfg.wrapper.write_text('exec /wrong --db /wrong\n');self.refused('preflight')

    def test_master_symlink_is_refused_before_normalized_resolver(self):
        original=self.cfg.master;replacement=original.with_name('real-state');original.rename(replacement);original.symlink_to(replacement)
        try:
            with mock.patch.object(service,'configured_server_database',side_effect=AssertionError('must not normalize unsafe source')):
                self.refused('preflight')
        finally:
            original.unlink();replacement.rename(original)

    def test_unsafe_acl_or_path_owner_and_nonprivate_database_refuse(self):
        real=admin._metadata
        def unsafe(path):
            record=real(path)
            if path==self.cfg.master:record['acl']=[dict(kind='group',principal='everyone',effect='allow',rights=['write'])]
            return record
        with mock.patch.object(admin,'_metadata',side_effect=unsafe):self.refused('preflight')
        self.cfg.master.chmod(0o644);self.refused('preflight');self.cfg.master.chmod(0o600)

    def test_public_read_provider_metadata_allowed_but_store_acl_content_read_refused(self):
        self.cfg.plist.chmod(0o644);self.cfg.wrapper.chmod(0o755)
        self.assertTrue(self.execute('preflight')['eligible'])
        real=admin._metadata
        def content_read(path):
            record=real(path)
            if path==self.cfg.master:record['acl']=[dict(kind='group',principal='everyone',effect='allow',rights=['read'])]
            return record
        with mock.patch.object(admin,'_metadata',side_effect=content_read):self.refused('preflight')

    def test_metadata_failure_stops_before_unacquired_subjects(self):
        real=admin._metadata;seen=[]
        def unavailable(path):
            seen.append(path)
            if path==Path('/'):raise OSError('fixture unavailable')
            return real(path)
        with mock.patch.object(admin,'_metadata',side_effect=unavailable),mock.patch.dict(os.environ,{},clear=True):
            result=admin.custody()
        self.assertFalse(result['admitted']);self.assertEqual(seen,[Path('/')])

    def test_readonly_database_allows_only_reader_sidecar_effects(self):
        before=self.cfg.master.read_bytes();logical=self.state()
        self.assertTrue(self.execute('preflight')['eligible'])
        self.assertEqual(self.cfg.master.read_bytes(),before)
        self.assertEqual(self.state(),logical)
        self.assertFalse(Path(str(self.cfg.master)+'-journal').exists())
        with contextlib.closing(sqlite3.connect(self.db)) as writer:
            writer.execute('BEGIN IMMEDIATE')
            self.assertTrue(self.execute('preflight')['eligible'])
            writer.rollback()

    def test_active_writer_snapshot_consistent_and_prepare_revalidates(self):
        original=admin._snapshot;observed=[]
        def writer_during_snapshot(db,*args):
            value=original(db,*args)
            old=db.execute("SELECT reason FROM resources WHERE name='shared'").fetchone()[0]
            self.sql("UPDATE resources SET reason='concurrent-writer' WHERE name='shared'")
            observed.append(db.execute("SELECT reason FROM resources WHERE name='shared'").fetchone()[0]==old)
            return value
        with mock.patch.object(admin,'_snapshot',side_effect=writer_during_snapshot):pre=self.execute('preflight')
        self.assertEqual(observed,[True])
        self.refused('prepare',expected_prestate=pre['prestate_sha256'],authority_reference='fixture-stale-pin')

    def test_source_replacement_during_read_refuses_and_connection_cleans_up(self):
        original=admin._state;held=Path(str(self.db)+'.held')
        def replace(db,*args):
            result=original(db,*args)
            self.db.rename(held)
            self.db.write_bytes(held.read_bytes());self.db.chmod(0o600)
            return result
        try:
            with mock.patch.object(admin,'_state',side_effect=replace):
                with self.assertRaisesRegex(CoordError,'source identity changed'):self.execute('preflight')
        finally:
            self.db.unlink();held.rename(self.db)
        with contextlib.closing(sqlite3.connect(self.db)) as writer:
            writer.execute('BEGIN IMMEDIATE');writer.rollback()

    def test_wrong_target_host_run_or_changed_prestate_refuse(self):
        with mock.patch.dict(admin.TARGET,{'reservation_id':'wrong'}):self.refused('preflight')
        with mock.patch.dict(admin.TARGET,{'run_id':'wrong'}):self.refused('preflight')
        intent=self.prepare();self.sql("UPDATE remote_guarded_runs SET host_id='foreign'");self.refused('apply',intent_id=intent['intent_id'])

    def test_expiry_revocation_replay_and_status_without_mutation(self):
        intent=self.prepare()
        with mock.patch.object(admin.time,'time',return_value=intent['expires_at']+1):self.refused('apply',intent_id=intent['intent_id'])
        result=self.execute('revoke',intent_id=intent['intent_id']);self.assertEqual(result['state'],'revoked')
        self.refused('apply',intent_id=intent['intent_id']);self.refused('revoke',intent_id=intent['intent_id'])
        before=self.state();self.assertEqual(self.execute('status',intent_id=intent['intent_id'])['state'],'revoked');self.assertEqual(self.state(),before)

    def test_guard_aba_and_current_provider_drift_invalidate_intent(self):
        intent=self.prepare();self.sql('UPDATE remote_guarded_runs SET local_pid=local_pid');self.refused('apply',intent_id=intent['intent_id'])
        self.cfg.executable.write_text('#!/bin/sh\nexit 1\n');self.refused('status',intent_id=intent['intent_id'])

    def test_real_retirement_quarantine_stop_reset_and_unread_inbox_refuse(self):
        intent=self.prepare()
        self.sql("INSERT INTO agent_controls(target_session,mode,updated_at) VALUES('a','stopping',0)");self.refused('apply',intent_id=intent['intent_id']);self.sql("UPDATE agent_controls SET mode='running'")
        self.sql("INSERT INTO session_resets(id,requester_session,target_session,host_id,old_thread_id,prompt,status,created_at,updated_at) VALUES('reset','a','a','mac','thread','fixture','pending',0,0)");self.refused('preflight');self.sql("UPDATE session_resets SET status='cancelled'")
        self.call('register',session='b',agent='sender');self.call('send',session='b',to='fixture-original-owner',body='consume before recovery');self.refused('apply',intent_id=intent['intent_id'])
        self.call('context')
        from agent_chat.root_team_reset import schema
        with contextlib.closing(sqlite3.connect(self.db)) as db:
            schema(db);db.execute("INSERT INTO root_team_reset_targets VALUES('a','reset','thread','mac')");db.commit()
        self.refused('preflight')
        # Real retirement marker/session removal remains a separate refusal even
        # though this same fixture is already quarantined.
        self.sql("INSERT INTO retired_sessions SELECT id,agent,registered_at,? FROM sessions WHERE id='a'",time.time());self.sql("DELETE FROM sessions WHERE id='a'");self.refused('preflight')

    def test_partial_failure_then_concurrent_apply_one_winner(self):
        intent=self.prepare()
        self.sql("CREATE TRIGGER fixture_abort BEFORE UPDATE ON resources BEGIN SELECT RAISE(ABORT,'fixture disk failure'); END");self.refused('apply',intent_id=intent['intent_id']);self.assertEqual(self.sql('SELECT count(*) FROM custodian_abandonments'),[(0,)])
        self.sql('DROP TRIGGER fixture_abort')
        def apply(_):
            try:return self.execute('apply',intent_id=intent['intent_id'])['outcome']
            except CoordError:return 'refused'
        # Environment is fixed outside threads; no per-call mock races.
        with mock.patch.dict(os.environ,{},clear=True),ThreadPoolExecutor(max_workers=2) as pool:
            def direct(_):
                try:return admin.execute('apply',intent_id=intent['intent_id'])['outcome']
                except CoordError:return 'refused'
            self.assertEqual(sorted(pool.map(direct,range(2))),['admin_present_abandonment','refused'])

    def test_remote_bearer_has_no_admin_interface_or_claim_effect(self):
        before=self.state()
        status,_,_=self.request('POST','/api/custodian-recovery',{'operation':'apply','session':'a','host_id':'mac'})
        self.assertEqual(status,404);self.assertEqual(self.state(),before)
        state=self.sql('SELECT * FROM resources');guards=self.sql('SELECT * FROM remote_guarded_runs')
        with self.assertRaises(RemoteCoordError):self.call('custodian-apply',resource='shared')
        self.assertEqual(self.sql('SELECT * FROM resources'),state);self.assertEqual(self.sql('SELECT * FROM remote_guarded_runs'),guards)
        # Existing generic unsupported coord dispatch may update ordinary host
        # timestamps; it creates no new admin endpoint/intent/effect.

    def test_additional_actual_live_guard_needs_its_own_entire_execution_closure(self):
        import subprocess
        proc=subprocess.Popen(['/bin/sleep','20'],start_new_session=True)
        try:
            self.sql("INSERT INTO remote_guarded_runs VALUES('extra-live','shared',?,'a','mac',?,?,NULL,NULL)",self.claim['reservation_id'],proc.pid,time.time())
            self.assertIsNone(proc.poll());pre=self.execute('preflight');self.assertFalse(pre['eligible'])
            self.refused('prepare',expected_prestate=pre['prestate_sha256'],authority_reference='fixture')
        finally:
            proc.terminate();proc.wait(timeout=5)

    def test_original_parent_symlink_is_rejected_before_config_content_resolution(self):
        parent=self.cfg.plist.parent;real=parent.with_name('real-provider')
        parent.rename(real);parent.symlink_to(real,target_is_directory=True)
        try:
            with mock.patch.object(service,'configured_server_database',side_effect=AssertionError('unsafe ancestor cannot be normalized')):self.refused('preflight')
        finally:
            parent.unlink();real.rename(parent)

    def test_synthetic_wrong_file_owner_and_real_wrong_working_type_refuse(self):
        original=admin._metadata
        def wrong_owner(path):
            metadata=original(path)
            if path==self.cfg.master:metadata['owner_uid']=self.cfg.uid+123
            return metadata
        with mock.patch.object(admin,'_metadata',side_effect=wrong_owner):self.refused('preflight')
        self.cfg.working_directory.rmdir();self.cfg.working_directory.write_text('not a directory');self.refused('preflight')

    def test_real_retirement_without_other_barrier_and_root_stop_refuse(self):
        intent=self.prepare()
        with contextlib.closing(Coordinator(self.db)) as coord:
            from agent_chat.root_lifecycle import schema
            schema(coord.db)
        self.sql("INSERT INTO root_team_stops VALUES('root-stop','root',1,0,NULL)");self.refused('apply',intent_id=intent['intent_id']);self.sql("UPDATE root_team_stops SET active=0")
        self.sql("INSERT INTO retired_sessions SELECT id,agent,registered_at,? FROM sessions WHERE id='a'",time.time());self.sql("DELETE FROM sessions WHERE id='a'");self.refused('preflight');self.refused('status',intent_id=intent['intent_id'])

    def test_changed_resource_prestate_and_missing_guard_schema_fail_without_repair(self):
        intent=self.prepare();self.sql("UPDATE resources SET reason='changed' WHERE name='shared'");self.refused('apply',intent_id=intent['intent_id'])
        self.sql('DROP TRIGGER root_guard_generation_remote_guarded_runs_au');self.refused('preflight')

    def test_custody_has_no_database_open_or_contents_read(self):
        with mock.patch.dict(os.environ,{},clear=True),mock.patch.object(admin.sqlite3,'connect',side_effect=AssertionError('custody may not open any SQLite store')):
            result=admin.custody()
            self.assertTrue(result['admitted'])
            database_record=next(row for row in result['metadata'] if row['role']=='private_database')
            self.assertNotIn('sha256',database_record)

    def test_cli_rejects_any_caller_selected_database_or_provider_flag(self):
        with contextlib.redirect_stderr(io.StringIO()),self.assertRaises(SystemExit):admin.main(['custody','--db','/untrusted'])
        with contextlib.redirect_stderr(io.StringIO()),self.assertRaises(SystemExit):admin.main(['custody','--provider','/untrusted'])
