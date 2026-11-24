import unittest
from test_worker_admission_intent import Flow
from agent_chat import root_local, service

def enroll(case):
    import uuid
    params={key:value for key,value in case.params.items() if key!='request_id'}
    params['enrollment_key']=str(uuid.uuid4())
    case.enrollment_params=dict(params)
    result=root_local.execute('worker-admission-enroll',params,master_db=case.master,service_plist=service.INSTALLED_SERVER_CONFIG.plist,home_project='default',session=case.root_sid,host_id=case.host)
    assert result['state']=='enrolled' and result['runtime_authorized'] is False
    case.params['request_id']=result['request_id']

class AdmissionJournalProbe(Flow):
    def setUp(self):
        super().setUp(); enroll(self)
    def test_fresh_intent_exact_status_is_durable_and_nonexecuting(self):
        result=self.call()
        self.assertEqual(result['status'],'intent_only')
        status=root_local.execute('worker-admission-status',dict(self.params),master_db=self.master,service_plist=service.INSTALLED_SERVER_CONFIG.plist,home_project='default',session=self.root_sid,host_id=self.host)
        self.assertEqual(status['state'],'committed')
        self.assertEqual(status['native_execution'],'unavailable_no_consumer')
        self.assertEqual(status['next_action'],'readback_only_no_native_association')

from unittest import mock
from agent_chat import worker_admission_intent as worker_intent
from agent_chat.core import CoordError
import sqlite3

class JournalFaults(Flow):
    def setUp(self):
        super().setUp(); enroll(self)

    def safe_count(self):
        with self.projects.connection() as db:
            exists=db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='worker_admission_intents'").fetchone()
            return db.execute('SELECT count(*) FROM worker_admission_intents').fetchone()[0] if exists else 0

    def status(self, params=None, actor=None):
        return root_local.execute('worker-admission-status',dict(params or self.params),master_db=self.master,service_plist=service.INSTALLED_SERVER_CONFIG.plist,home_project='default',session=actor or self.root_sid,host_id=self.host)

    def test_enrolled_concurrent_duplicate_commits_once(self):
        import concurrent.futures
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            results=list(pool.map(lambda _:self.call(),range(4)))
        self.assertEqual(sum(not result['duplicate'] for result in results),1)
        status=self.status(); self.assertEqual(status['state'],'committed')
        self.assertEqual(status['phases'].count('committed'),1)
        self.assertEqual(self.intent_count(),1)

    def test_old_failed_request_is_never_retroactively_enrolled(self):
        import uuid
        self.params['request_id']=str(uuid.uuid4())
        bad=dict(self.params,expected_thread='old-invalid-thread')
        self.params=bad
        with self.assertRaises(CoordError): self.call()
        self.assertEqual(self.status()['state'],'legacy_or_unknown')
        params=dict(self.params); params.pop('request_id')
        params['request_id']='caller-selected'
        with self.assertRaises(CoordError):
            root_local.execute('worker-admission-enroll',params,master_db=self.master,service_plist=service.INSTALLED_SERVER_CONFIG.plist,home_project='default',session=self.root_sid,host_id=self.host)
        self.assertEqual(self.status()['state'],'legacy_or_unknown')

    def test_actual_root_local_cli_status_reads_exact_request(self):
        import contextlib, io, subprocess
        self.call()
        argv=['--home-project','default','--home-session',self.root_sid,'--host-id',self.host,'worker-admission-status']
        for key,value in self.params.items():
            if key=='confirm': argv.append('--confirm')
            else: argv.extend(('--project-id' if key=='project' else '--'+key.replace('_','-'),str(value)))
        output=io.StringIO()
        with contextlib.redirect_stdout(output): code=root_local.main(argv)
        self.assertEqual(code,0)
        checked=subprocess.run(['jq','-e','.state == "committed" and .native_execution == "unavailable_no_consumer"'],input=output.getvalue(),text=True,capture_output=True)
        self.assertEqual(checked.returncode,0,checked.stderr)
        self.assertEqual(self.intent_count(),1)

    def test_loss_after_commit_is_read_back_without_second_admit(self):
        original=sqlite3.connect; target=self.projects.db_path(self.project).resolve().as_uri()+'?mode=rw'
        class LostCommit:
            def __init__(self,db): object.__setattr__(self,'db',db)
            def __getattr__(self,name): return getattr(self.db,name)
            def __setattr__(self,name,value): setattr(self.db,name,value)
            def commit(self): self.db.commit(); raise OSError('synthetic response lost after actual commit')
        def connect(database,*args,**kwargs):
            db=original(database,*args,**kwargs)
            return LostCommit(db) if str(database)==target else db
        with mock.patch.object(sqlite3,'connect',side_effect=connect):
            with self.assertRaises(CoordError): self.call()
        self.assertEqual(self.intent_count(),1)
        first=self.status(); second=self.status()
        self.assertEqual(first,second); self.assertEqual(first['state'],'committed')
        self.assertEqual(first['native_execution'],'unavailable_no_consumer')
        self.assertEqual(first['phases'].count('committed'),1)
        self.assertEqual(self.intent_count(),1)

    def test_parent_loss_after_intent_is_unknown_and_readback_does_not_admit(self):
        original=worker_intent._journal_begin
        def crash(*args): original(*args); raise SystemExit('synthetic parent exit before transaction')
        with mock.patch.object(worker_intent,'_journal_begin',side_effect=crash):
            with self.assertRaises(SystemExit): self.call()
        self.assertEqual(self.safe_count(),0)
        result=self.status(); self.assertEqual(result['state'],'unknown')
        self.assertEqual(result['phases'],['enrolled','configured_root_verified','commit_intent'])
        self.assertEqual(result['next_action'],'retain_unknown_no_replay')
        self.assertEqual(self.safe_count(),0)
        with self.assertRaises(CoordError): self.call()
        self.assertEqual(self.safe_count(),0)

    def test_refusal_and_conflict_remain_separate(self):
        import uuid
        from agent_chat.bridge_state import BridgeState
        BridgeState(self.worker).bind(str(uuid.uuid4()))
        with self.assertRaises(CoordError): self.call()
        result=self.status(); self.assertEqual(result['state'],'refused'); self.assertEqual(result['next_action'],'inspect_and_correct_refused_predicate')
        self.assertIn('transaction_rolled_back',result['phases'])
        self.assertIn('predicate_worker_binding_mismatch',result['phases'])
        self.assertEqual(self.safe_count(),0)
        changed=dict(self.params,expected_thread='wrong-thread')
        with self.assertRaises(CoordError): self.status(changed)
        with self.assertRaises(CoordError): self.status(actor='session-po')

    def test_legacy_existing_record_is_not_backfilled(self):
        import uuid
        self.params['request_id']=str(uuid.uuid4())
        self.call()
        self.assertEqual(self.status()['state'],'legacy_or_unknown')
        self.call()
        self.assertEqual(self.status()['state'],'legacy_or_unknown')
        self.assertEqual(self.intent_count(),1)

class ProcessLoss(Flow):
    def setUp(self):
        super().setUp(); enroll(self)

    def test_actual_caller_exit_after_durable_intent_never_readmits(self):
        import os, subprocess, sys
        script=self.base/'exit-after-journal.py'
        script.write_text('''import os, sys\nfrom pathlib import Path\nfrom agent_chat import service, root_local, worker_admission_intent as w\nmaster,plist,wrapper,executable,neutral,session,host = sys.argv[1:8]\nraw=sys.argv[8:]\nparams=dict(zip(raw[::2],raw[1::2]));params["confirm"]=True\nservice.INSTALLED_SERVER_CONFIG=service.InstalledServerConfig(Path(plist),Path(wrapper),Path(executable),Path(master),Path(neutral),os.getuid(),"127.0.0.1",9999)\noriginal=w._journal_begin\ndef crash(*args):\n original(*args);os._exit(91)\nw._journal_begin=crash\nroot_local.execute("worker-admission-intent",params,master_db=Path(master),service_plist=Path(plist),home_project="default",session=session,host_id=host)\n''')
        config=service.INSTALLED_SERVER_CONFIG
        args=[sys.executable,str(script),str(self.master),str(config.plist),str(config.wrapper),str(config.executable),str(self.neutral),self.root_sid,self.host]
        for key,value in self.params.items():
            if key!='confirm': args.extend((key,str(value)))
        env=dict(os.environ,PYTHONPATH=str(__import__('pathlib').Path(__file__).resolve().parents[1]/'src'))
        result=subprocess.run(args,env=env,capture_output=True,timeout=12)
        self.assertEqual(result.returncode,91,result.stderr.decode())
        status=root_local.execute('worker-admission-status',dict(self.params),master_db=self.master,service_plist=config.plist,home_project='default',session=self.root_sid,host_id=self.host)
        self.assertEqual(status['state'],'unknown');self.assertEqual(status['phases'],['enrolled','configured_root_verified','commit_intent'])
        with self.assertRaises(CoordError): self.call()
        with self.projects.connection() as db:
            self.assertFalse(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='worker_admission_intents'").fetchone())

class EnrollmentLoss(Flow):
    def test_closed_cli_stdout_after_commit_reconciles_exact_key(self):
        import os, subprocess, sys, uuid
        from pathlib import Path
        key=str(uuid.uuid4())
        key_path=self.base/'caller-enrollment-key.txt'
        descriptor=os.open(key_path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        with os.fdopen(descriptor,'w') as stream:
            stream.write(key+'\n'); stream.flush(); os.fsync(stream.fileno())
        key=key_path.read_text().strip()
        params={k:v for k,v in self.params.items() if k!='request_id'}
        params['enrollment_key']=key
        def lookup(values=None,actor=None,host=None):
            return root_local.execute('worker-admission-enrollment-status',dict(values or params),master_db=self.master,service_plist=service.INSTALLED_SERVER_CONFIG.plist,home_project='default',session=actor or self.root_sid,host_id=host or self.host)
        self.assertEqual(lookup()['state'],'not_recorded_at_read')
        script=self.base/'enroll-cli.py'
        script.write_text("import os,sys\nfrom pathlib import Path\nfrom agent_chat import service,root_local\nmaster,plist,wrapper,executable,neutral,session,host=sys.argv[1:8]\nservice.INSTALLED_SERVER_CONFIG=service.InstalledServerConfig(Path(plist),Path(wrapper),Path(executable),Path(master),Path(neutral),os.getuid(),'127.0.0.1',9999)\nargs=['--home-project','default','--home-session',session,'--host-id',host,'worker-admission-enroll',*sys.argv[8:]]\nsys.exit(root_local.main(args))\n")
        config=service.INSTALLED_SERVER_CONFIG
        args=[sys.executable,str(script),str(self.master),str(config.plist),str(config.wrapper),str(config.executable),str(self.neutral),self.root_sid,self.host]
        for field,value in params.items():
            if field=='confirm': args.append('--confirm')
            else: args.extend(('--project-id' if field=='project' else '--'+field.replace('_','-'),str(value)))
        env=dict(os.environ,PYTHONPATH=str(Path(__file__).resolve().parents[1]/'src'))
        proc=subprocess.Popen(args,env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        proc.stdout.close(); stderr=proc.stderr.read(); result=proc.wait(timeout=12); proc.stderr.close()
        self.assertNotEqual(result,0); self.assertIn(b'BrokenPipeError',stderr)
        first=lookup(); second=lookup()
        self.assertEqual(first,second); self.assertEqual(first['state'],'enrolled')
        self.assertFalse(first['runtime_authorized']); self.assertEqual(first['native_execution'],'unavailable_no_consumer')
        import contextlib, io
        status_argv=['--home-project','default','--home-session',self.root_sid,'--host-id',self.host,'worker-admission-enrollment-status']
        for field,value in params.items():
            if field=='confirm': status_argv.append('--confirm')
            else: status_argv.extend(('--project-id' if field=='project' else '--'+field.replace('_','-'),str(value)))
        output=io.StringIO()
        with contextlib.redirect_stdout(output): code=root_local.main(status_argv)
        self.assertEqual(code,0)
        checked=subprocess.run(['jq','-e','--arg','id',first['request_id'],'.state=="enrolled" and .request_id==$id and .runtime_authorized==false'],input=output.getvalue(),text=True,capture_output=True)
        self.assertEqual(checked.returncode,0,checked.stderr)
        retry=root_local.execute('worker-admission-enroll',params,master_db=self.master,service_plist=config.plist,home_project='default',session=self.root_sid,host_id=self.host)
        self.assertEqual(retry['request_id'],first['request_id']); self.assertTrue(retry['duplicate'])
        other_dir=self.base/'other-enrollment-project'; other_dir.mkdir()
        other_project=self.projects.create('Other enrollment project',str(other_dir),'private fixture')['id']
        for changed in (dict(params,target='another-worker'),dict(params,canonical_resource='native-unity:other'),dict(params,project=other_project)):
            with self.assertRaises(CoordError): lookup(changed)
            with self.assertRaises(CoordError): root_local.execute('worker-admission-enroll',changed,master_db=self.master,service_plist=config.plist,home_project='default',session=self.root_sid,host_id=self.host)
        with self.assertRaises(CoordError): lookup(actor='session-po')
        with self.assertRaises(CoordError): lookup(host='wrong-host')
        with self.projects.connection() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM worker_admission_journal WHERE enrollment_key=?',(key,)).fetchone()[0],1)
            table=db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='worker_admission_intents'").fetchone()
            self.assertFalse(table)

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for cls in (AdmissionJournalProbe, JournalFaults, ProcessLoss, EnrollmentLoss):
        for name in cls.__dict__:
            if name.startswith('test_'):
                suite.addTest(cls(name))
    return suite
