"""Private authenticated resource acquisition and unused-return probe."""
import os
from pathlib import Path
import subprocess
import sys
import unittest
from unittest import mock
from test_remote_web import RemoteWebFixture, TOKEN


def jq(raw, expr):
    return subprocess.run(['jq','-r',expr],input=raw,stdout=subprocess.PIPE,check=True).stdout.decode().strip()


class UnusedReturnProbe(RemoteWebFixture):
    def start(self, **kwargs):
        from agent_chat import service, root_local
        for module,name in ((service,'_installed_server_config'),(service,'configured_server_database'),
                            (root_local,'_installed_server_config'),(root_local,'configured_server_database')):
            guard=mock.patch.object(module,name,side_effect=AssertionError('installed discovery forbidden'))
            guard.start(); self.addCleanup(guard.stop)
        home=Path(self.tmp.name)/'home'; home.mkdir(exist_ok=True)
        environment=mock.patch.dict(os.environ,{'HOME':str(home),'CODEX_HOME':str(home/'.codex'),
                                               'AGENT_CHAT_STATE_DIR':str(home/'client')})
        environment.start(); self.addCleanup(environment.stop)
        sessions=Path(self.tmp.name)/'sessions'; sessions.mkdir(exist_ok=True)
        return super().start(sessions_root=sessions, **kwargs)

    def test_empty_guard_history_and_matching_bytes_do_not_prove_no_write(self):
        status,raw,_=self.request('POST','/api/coord',{'op':'register','session':'worker','host_id':'mac','params':{'agent':'worker'}})
        self.assertEqual(status,200,raw)
        cap=Path(self.tmp.name)/'capability.json'; fixture=Path(self.tmp.name)/'private.txt'
        fixture.write_bytes(b'original\n')
        env=dict(os.environ,AGENT_CHAT_HOST_ID='mac',PYTHONPATH='src')
        base=[sys.executable,'-m','agent_chat.cli','--server',self.url,'--api-token',TOKEN,'--project','default','--session','worker']
        acquired=subprocess.run(base+['request','fixture:unobserved-effects','--minutes','5','--capability-file',str(cap)],capture_output=True,env=env,timeout=8)
        self.assertEqual(acquired.returncode,0,acquired.stderr.decode())
        fixture.write_bytes(b'changed\n'); fixture.write_bytes(b'original\n')
        self.assertEqual(fixture.read_bytes(),b'original\n')
        with __import__('sqlite3').connect(self.db) as db:
            rid=db.execute('SELECT reservation_id FROM resources WHERE name=?',('fixture:unobserved-effects',)).fetchone()[0]
            self.assertEqual(db.execute('SELECT COUNT(*) FROM guarded_runs WHERE reservation_id=?',(rid,)).fetchone()[0],0)

    def test_real_guarded_write_and_restoration_are_distinct_from_unused(self):
        import sqlite3
        import time
        status,raw,_=self.request('POST','/api/coord',{'op':'register','session':'worker','host_id':'mac','params':{'agent':'worker'}})
        self.assertEqual(status,200,raw)
        root=Path(self.tmp.name); cap=root/'capability.json'; target=root/'private.txt'
        target.write_bytes(b'original\n'); target.chmod(0o600)
        env=dict(os.environ,AGENT_CHAT_HOST_ID='mac',PYTHONPATH='src')
        base=[sys.executable,'-m','agent_chat.cli','--server',self.url,'--api-token',TOKEN,'--project','default','--session','worker']
        acquired=subprocess.run(base+['request','fixture:restored','--minutes','5','--capability-file',str(cap)],capture_output=True,env=env,timeout=8)
        self.assertEqual(acquired.returncode,0,acquired.stderr.decode())
        script=root/'write-and-restore.sh'; script.write_text("printf 'changed\\n' > \"$1\"\nprintf 'original\\n' > \"$1\"\n")
        evidence=root/'process-evidence.txt'
        reservation=jq(cap.read_bytes(),'.reservation_id')
        run=subprocess.run(base+['run','--capability-file',str(cap),'--reservation-id',reservation,'--process-evidence-file',str(evidence),
                                 'fixture:restored','--','/bin/sh',str(script),str(target)],capture_output=True,env=env,timeout=15)
        self.assertEqual(run.returncode,0,run.stderr.decode())
        self.assertEqual(target.read_bytes(),b'original\n'); self.assertEqual(target.stat().st_mode & 0o777,0o600)
        with sqlite3.connect(self.db) as db:
            rid=db.execute('SELECT reservation_id FROM resources WHERE name=?',('fixture:restored',)).fetchone()[0]
            runs=db.execute('SELECT local_pid,closed_at FROM remote_guarded_runs WHERE reservation_id=?',(rid,)).fetchall()
        self.assertEqual(len(runs),1); self.assertIsNotNone(runs[0][1])
        with self.assertRaises(ProcessLookupError): os.kill(runs[0][0],0)
        import uuid
        token=jq(cap.read_bytes(),'.token')
        audit_request=str(uuid.uuid4())
        status,audit,_=self.request('POST','/api/coord',{'op':'resource-uncertainty-record','session':'worker','host_id':'mac',
            'params':{'resource':'fixture:restored','reservation_id':rid,'token':token,'request_id':audit_request,
                      'reason':'Full effect coverage remains unverified.'}})
        self.assertEqual(status,200,audit)
        self.assertEqual(jq(audit,'.remote_guarded_runs.total'), '1')
        self.assertEqual(jq(audit,'.remote_guarded_runs.open'), '0')
        self.assertEqual(jq(audit,'.physical_state'), 'unknown')
        proof=root/'restored.txt'; proof.write_text('Private fixture child exited; private target bytes and mode restored.\n')
        receipt=root/'receipt.json'
        built=subprocess.run(['jq','-n','--arg','rid',rid,'--argjson','pid',str(runs[0][0]),
            '--argjson','closed',str(time.time()),'{version:1,resource:"fixture:restored",reservation_id:$rid,restored:true,processes_closed:true,closed_at:$closed,evidence:"restored.txt",pids:[$pid]}'],capture_output=True,check=True)
        receipt.write_bytes(built.stdout)
        release=subprocess.run(base+['release','fixture:restored','--receipt',str(receipt),'--token',token],capture_output=True,env=env,timeout=8)
        self.assertEqual(release.returncode,0,release.stderr.decode())
        self.assertEqual(jq(release.stdout,'.released'),'true')
        with sqlite3.connect(self.db) as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM resource_uncertainty_dispositions WHERE request_id=?',(audit_request,)).fetchone()[0],1)

    def test_queue_cancellation_does_not_return_other_owners_grant(self):
        import sqlite3
        for sid in ('owner','waiter'):
            status,raw,_=self.request('POST','/api/coord',{'op':'register','session':sid,'host_id':'mac','params':{'agent':sid}})
            self.assertEqual(status,200,raw)
        def request(session,op,params):
            return self.request('POST','/api/coord',{'op':op,'session':session,'host_id':'mac','params':params})
        status,owner,_=request('owner','request',{'resource':'fixture:queue','minutes':5})
        self.assertEqual(status,200,owner); self.assertEqual(jq(owner,'.state'),'owned')
        status,waiter,_=request('waiter','request',{'resource':'fixture:queue','minutes':5})
        self.assertEqual(status,200,waiter); self.assertEqual(jq(waiter,'.state'),'queued')
        status,cancel,_=request('waiter','cancel',{'resource':'fixture:queue'})
        self.assertEqual(status,200,cancel); self.assertEqual(jq(cancel,'.cancelled'),'true')
        with sqlite3.connect(self.db) as db:
            self.assertEqual(db.execute('SELECT owner_session FROM resources WHERE name=?',('fixture:queue',)).fetchone()[0],'owner')
            self.assertEqual(db.execute('SELECT count(*) FROM resource_queue WHERE resource=?',('fixture:queue',)).fetchone()[0],0)

    def test_uncertainty_is_audited_without_clearing_ownership(self):
        import sqlite3
        import uuid
        status,raw,_=self.request('POST','/api/coord',{'op':'register','session':'worker','host_id':'mac','params':{'agent':'worker'}})
        self.assertEqual(status,200,raw)
        def call(op,params,session='worker',host='mac'):
            return self.request('POST','/api/coord',{'op':op,'session':session,'host_id':host,'params':params})
        status,raw,_=call('request',{'resource':'fixture:unknown','minutes':5})
        self.assertEqual(status,200,raw)
        rid=jq(raw,'.reservation_id'); token=jq(raw,'.token'); request_id=str(uuid.uuid4())
        params={'resource':'fixture:unknown','reservation_id':rid,'token':token,'request_id':request_id,
                'reason':'Historical execution evidence unavailable.'}
        status,raw,_=call('resource-uncertainty-record',params)
        self.assertEqual(status,200,raw)
        self.assertEqual(jq(raw,'.state'),'uncertain')
        self.assertEqual(jq(raw,'.administrative_state'),'evidence_recorded')
        self.assertEqual(jq(raw,'.physical_state'),'unknown')
        self.assertEqual(jq(raw,'.process_state'),'unknown')
        self.assertEqual(jq(raw,'.reuse_authorized'),'false')
        self.assertNotIn(token.encode(),raw)
        status,retry,_=call('resource-uncertainty-record',params)
        self.assertEqual(status,200,retry); self.assertEqual(jq(retry,'.request_id'),request_id)
        self.assertEqual(jq(retry,'.recorded_at'),jq(raw,'.recorded_at'))
        status,readback,_=call('resource-uncertainty-status',{'resource':params['resource'],'reservation_id':rid,'token':token,'request_id':request_id})
        self.assertEqual(status,200,readback); self.assertEqual(jq(readback,'.state'),'uncertain')
        env=dict(os.environ,AGENT_CHAT_HOST_ID='mac',AGENT_CHAT_TOKEN=token,PYTHONPATH='src')
        cli=[sys.executable,'-m','agent_chat.cli','--server',self.url,'--api-token',TOKEN,'--project','default','--session','worker']
        result=subprocess.run(cli+['resource-uncertainty-status',params['resource'],'--reservation-id',rid,
                                  '--request-id',request_id],capture_output=True,env=env,timeout=8)
        self.assertEqual(result.returncode,0,result.stderr.decode())
        self.assertEqual(jq(result.stdout,'.request_id'),request_id)
        self.assertNotIn(token.encode(),result.stdout)
        second_id=str(uuid.uuid4())
        result=subprocess.run(cli+['resource-uncertainty-record',params['resource'],'--reservation-id',rid,
            '--request-id',second_id,'--reason','Separate bounded evidence gap'],capture_output=True,env=env,timeout=8)
        self.assertEqual(result.returncode,0,result.stderr.decode())
        self.assertEqual(jq(result.stdout,'.state'),'uncertain')
        self.assertNotIn(token.encode(),result.stdout)
        changed=dict(params,reason='Different claim')
        status,_,_=call('resource-uncertainty-record',changed)
        self.assertNotEqual(status,200)
        status,_,_=call('resource-uncertainty-status',{'resource':params['resource'],'reservation_id':rid,'token':token,'request_id':request_id},host='foreign')
        self.assertNotEqual(status,200)
        status,_,_=call('resource-uncertainty-record',dict(params,token='wrong-token'))
        self.assertNotEqual(status,200)
        status,_,_=call('resource-uncertainty-record',dict(params,request_id=str(uuid.uuid4()),reason='secret '+token))
        self.assertNotEqual(status,200)
        status,raw,_=self.request('POST','/api/coord',{'op':'register','session':'intruder','host_id':'mac','params':{'agent':'intruder'}})
        self.assertEqual(status,200,raw)
        status,_,_=call('resource-uncertainty-record',params,session='intruder')
        self.assertNotEqual(status,200)
        with sqlite3.connect(self.db) as db:
            owner=db.execute('SELECT owner_session,reservation_id FROM resources WHERE name=?',(params['resource'],)).fetchone()
            self.assertEqual(owner,('worker',rid))
            self.assertEqual(db.execute('SELECT COUNT(*) FROM receipts WHERE reservation_id=?',(rid,)).fetchone()[0],0)
            self.assertEqual(db.execute('SELECT COUNT(*) FROM resource_uncertainty_dispositions WHERE request_id=?',(request_id,)).fetchone()[0],1)
            self.assertNotIn('token',[row[1] for row in db.execute('PRAGMA table_info(resource_uncertainty_dispositions)')])
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute('DELETE FROM resource_uncertainty_dispositions WHERE request_id=?',(request_id,))

    def test_concurrent_identical_uncertainty_requests_have_one_record(self):
        import concurrent.futures
        import sqlite3
        import uuid
        status,raw,_=self.request('POST','/api/coord',{'op':'register','session':'worker','host_id':'mac','params':{'agent':'worker'}})
        self.assertEqual(status,200,raw)
        status,raw,_=self.request('POST','/api/coord',{'op':'request','session':'worker','host_id':'mac','params':{'resource':'fixture:concurrent','minutes':5}})
        self.assertEqual(status,200,raw)
        rid=jq(raw,'.reservation_id'); token=jq(raw,'.token'); identifier=str(uuid.uuid4())
        payload={'op':'resource-uncertainty-record','session':'worker','host_id':'mac',
                 'params':{'resource':'fixture:concurrent','reservation_id':rid,'token':token,
                           'request_id':identifier,'reason':'Evidence is incomplete.'}}
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(lambda _: self.request('POST','/api/coord',payload),range(2)))
        self.assertEqual([result[0] for result in results],[200,200])
        self.assertEqual(jq(results[0][1],'.recorded_at'),jq(results[1][1],'.recorded_at'))
        with sqlite3.connect(self.db) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM resource_uncertainty_dispositions WHERE request_id=?',(identifier,)).fetchone()[0],1)

    def test_existing_grant_survives_missing_audit_table_upgrade(self):
        import contextlib
        import sqlite3
        from agent_chat.core import Coordinator
        status,raw,_=self.request('POST','/api/coord',{'op':'register','session':'worker','host_id':'mac','params':{'agent':'worker'}})
        self.assertEqual(status,200,raw)
        status,raw,_=self.request('POST','/api/coord',{'op':'request','session':'worker','host_id':'mac','params':{'resource':'fixture:legacy','minutes':5}})
        self.assertEqual(status,200,raw)
        with sqlite3.connect(self.db) as db:
            before=db.execute('SELECT owner_session,reservation_id,deadline,stale FROM resources WHERE name=?',('fixture:legacy',)).fetchone()
            db.execute('DROP TABLE resource_uncertainty_dispositions')
        with contextlib.closing(Coordinator(self.db)) as coord:
            after=tuple(coord.db.execute('SELECT owner_session,reservation_id,deadline,stale FROM resources WHERE name=?',('fixture:legacy',)).fetchone())
            self.assertEqual(after,before)
            self.assertEqual(coord.db.execute('SELECT count(*) FROM resource_uncertainty_dispositions').fetchone()[0],0)

    def test_legacy_uncertainty_record_does_not_invent_observation(self):
        import sqlite3
        import uuid
        status,raw,_=self.request('POST','/api/coord',{'op':'register','session':'worker','host_id':'mac','params':{'agent':'worker'}})
        self.assertEqual(status,200,raw)
        status,raw,_=self.request('POST','/api/coord',{'op':'request','session':'worker','host_id':'mac','params':{'resource':'fixture:legacy-audit','minutes':5}})
        self.assertEqual(status,200,raw)
        rid=jq(raw,'.reservation_id'); token=jq(raw,'.token'); identifier=str(uuid.uuid4())
        params={'resource':'fixture:legacy-audit','reservation_id':rid,'token':token,'request_id':identifier,'reason':'Existing retained audit.'}
        status,raw,_=self.request('POST','/api/coord',{'op':'resource-uncertainty-record','session':'worker','host_id':'mac','params':params})
        self.assertEqual(status,200,raw)
        with sqlite3.connect(self.db) as db:
            row=db.execute('SELECT request_id,resource,reservation_id,owner_session,host_id,reason,recorded_at FROM resource_uncertainty_dispositions WHERE request_id=?',(identifier,)).fetchone()
            db.execute('DROP TABLE resource_uncertainty_dispositions')
            db.execute('CREATE TABLE resource_uncertainty_dispositions (request_id TEXT PRIMARY KEY,resource TEXT NOT NULL,reservation_id TEXT NOT NULL,owner_session TEXT NOT NULL,host_id TEXT NOT NULL,reason TEXT NOT NULL,recorded_at REAL NOT NULL)')
            db.execute('INSERT INTO resource_uncertainty_dispositions VALUES(?,?,?,?,?,?,?)',row)
        status,raw,_=self.request('POST','/api/coord',{'op':'resource-uncertainty-status','session':'worker','host_id':'mac',
            'params':{key:params[key] for key in ('resource','reservation_id','token','request_id')}})
        self.assertEqual(status,200,raw)
        self.assertEqual(jq(raw,'.resource_state'),'unknown')
        self.assertEqual(jq(raw,'.observed_at == null and .local_guarded_runs == null and .remote_guarded_runs == null'),'true')
        self.assertEqual(jq(raw,'.reason'),'Existing retained audit.')

    def test_uncertainty_record_does_not_close_live_guard(self):
        import sqlite3
        import time
        import uuid
        status,raw,_=self.request('POST','/api/coord',{'op':'register','session':'worker','host_id':'mac','params':{'agent':'worker'}})
        self.assertEqual(status,200,raw)
        root=Path(self.tmp.name); cap=root/'live-capability.json'; evidence=root/'live-process.txt'
        env=dict(os.environ,AGENT_CHAT_HOST_ID='mac',PYTHONPATH='src')
        base=[sys.executable,'-m','agent_chat.cli','--server',self.url,'--api-token',TOKEN,'--project','default','--session','worker']
        acquired=subprocess.run(base+['request','fixture:live','--minutes','5','--capability-file',str(cap)],capture_output=True,env=env,timeout=8)
        self.assertEqual(acquired.returncode,0,acquired.stderr.decode())
        rid=jq(cap.read_bytes(),'.reservation_id'); token=jq(cap.read_bytes(),'.token')
        child=subprocess.Popen(base+['run','--capability-file',str(cap),'--reservation-id',rid,
            '--process-evidence-file',str(evidence),'fixture:live','--','/bin/sleep','3'],stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=env)
        try:
            deadline=time.monotonic()+5; row=None
            while time.monotonic()<deadline:
                with sqlite3.connect(self.db) as db:
                    row=db.execute('SELECT local_pid,closed_at FROM remote_guarded_runs WHERE reservation_id=?',(rid,)).fetchone()
                if row and row[0]>0 and row[1] is None: break
                time.sleep(.05)
            self.assertIsNotNone(row); self.assertGreater(row[0],0); self.assertIsNone(row[1])
            request_id=str(uuid.uuid4())
            status,raw,_=self.request('POST','/api/coord',{'op':'resource-uncertainty-record','session':'worker','host_id':'mac',
                'params':{'resource':'fixture:live','reservation_id':rid,'token':token,'request_id':request_id,
                          'reason':'Incomplete external effects.'}})
            self.assertEqual(status,200,raw)
            self.assertEqual(jq(raw,'.remote_guarded_runs.open'),'1')
            self.assertEqual(jq(raw,'.process_state'),'unknown')
            self.assertIsNone(child.poll())
            with sqlite3.connect(self.db) as db:
                self.assertEqual(db.execute('SELECT owner_session FROM resources WHERE name=?',('fixture:live',)).fetchone()[0],'worker')
                self.assertIsNone(db.execute('SELECT closed_at FROM remote_guarded_runs WHERE reservation_id=?',(rid,)).fetchone()[0])
            _,stderr=child.communicate(timeout=8)
            self.assertEqual(child.returncode,0,stderr.decode())
        finally:
            if child.poll() is None:
                child.terminate(); child.communicate(timeout=8)

    def test_uncertainty_replay_retains_original_observation_across_real_guard(self):
        import sqlite3
        import time
        import uuid
        status,raw,_=self.request('POST','/api/coord',{'op':'register','session':'worker','host_id':'mac','params':{'agent':'worker'}})
        self.assertEqual(status,200,raw)
        root=Path(self.tmp.name); cap=root/'snapshot-capability.json'; evidence=root/'snapshot-process.txt'
        env=dict(os.environ,AGENT_CHAT_HOST_ID='mac',PYTHONPATH='src')
        base=[sys.executable,'-m','agent_chat.cli','--server',self.url,'--api-token',TOKEN,'--project','default','--session','worker']
        acquired=subprocess.run(base+['request','fixture:snapshot','--minutes','5','--capability-file',str(cap)],capture_output=True,env=env,timeout=8)
        self.assertEqual(acquired.returncode,0,acquired.stderr.decode())
        rid=jq(cap.read_bytes(),'.reservation_id'); token=jq(cap.read_bytes(),'.token'); request_id=str(uuid.uuid4())
        params={'resource':'fixture:snapshot','reservation_id':rid,'token':token,'request_id':request_id,'reason':'Record existing evidence.'}
        payload={'op':'resource-uncertainty-record','session':'worker','host_id':'mac','params':params}
        status,first,_=self.request('POST','/api/coord',payload)
        self.assertEqual(status,200,first); self.assertEqual(jq(first,'.remote_guarded_runs.open'),'0')
        child=subprocess.Popen(base+['run','--capability-file',str(cap),'--reservation-id',rid,
            '--process-evidence-file',str(evidence),'fixture:snapshot','--','/bin/sleep','3'],stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=env)
        try:
            deadline=time.monotonic()+5; row=None
            while time.monotonic()<deadline:
                with sqlite3.connect(self.db) as db:
                    row=db.execute('SELECT local_pid,closed_at FROM remote_guarded_runs WHERE reservation_id=?',(rid,)).fetchone()
                if row and row[0]>0 and row[1] is None: break
                time.sleep(.05)
            self.assertIsNotNone(row); self.assertGreater(row[0],0); self.assertIsNone(row[1])
            status,retry,_=self.request('POST','/api/coord',payload)
            self.assertEqual(status,200,retry)
            self.assertEqual(jq(first,'.remote_guarded_runs'),jq(retry,'.remote_guarded_runs'), 'replay changed the recorded guard observation')
            self.assertEqual(jq(first,'.observed_at'),jq(retry,'.observed_at'))
            status,readback,_=self.request('POST','/api/coord',{'op':'resource-uncertainty-status','session':'worker','host_id':'mac',
                'params':{key:params[key] for key in ('resource','reservation_id','token','request_id')}})
            self.assertEqual(status,200,readback)
            self.assertEqual(jq(first,'.remote_guarded_runs'),jq(readback,'.remote_guarded_runs'))
            _,stderr=child.communicate(timeout=8); self.assertEqual(child.returncode,0,stderr.decode())
            status,after,_=self.request('POST','/api/coord',payload)
            self.assertEqual(status,200,after)
            self.assertEqual(jq(first,'.remote_guarded_runs'),jq(after,'.remote_guarded_runs'))
        finally:
            if child.poll() is None:
                child.terminate(); child.communicate(timeout=8)

    def test_owned_unrun_grant_has_no_unused_return_operation(self):
        status,raw,_=self.request('POST','/api/coord',{'op':'register','session':'worker','host_id':'mac','params':{'agent':'worker'}})
        self.assertEqual(status,200,raw)
        cap=Path(self.tmp.name)/'capability.json'
        env=dict(os.environ,AGENT_CHAT_HOST_ID='mac',PYTHONPATH='src')
        base=[sys.executable,'-m','agent_chat.cli','--server',self.url,'--api-token',TOKEN,'--project','default','--session','worker']
        acquired=subprocess.run(base+['request','fixture:unused-return','--minutes','5','--capability-file',str(cap)],capture_output=True,env=env,timeout=8)
        self.assertEqual(acquired.returncode,0,acquired.stderr.decode())
        self.assertTrue(cap.is_file())
        cancel=subprocess.run(base+['cancel','fixture:unused-return'],capture_output=True,env=env,timeout=8)
        self.assertEqual(cancel.returncode,0,cancel.stderr.decode())
        self.assertEqual(jq(cancel.stdout,'.cancelled'),'false')
        with __import__('sqlite3').connect(self.db) as db:
            row=db.execute('SELECT owner_session,reservation_id FROM resources WHERE name=?',('fixture:unused-return',)).fetchone()
            self.assertEqual(row[0],'worker'); self.assertTrue(row[1])
            self.assertEqual(db.execute('SELECT COUNT(*) FROM guarded_runs WHERE reservation_id=?',(row[1],)).fetchone()[0],0)
        released=subprocess.run(base+['release','fixture:unused-return','--unused','--capability-file',str(cap)],capture_output=True,env=env,timeout=8)
        self.assertEqual(released.returncode,0,'unused return unavailable: '+released.stderr.decode())


def load_tests(loader, standard_tests, pattern):
    names=(
        'test_uncertainty_is_audited_without_clearing_ownership',
        'test_concurrent_identical_uncertainty_requests_have_one_record',
        'test_existing_grant_survives_missing_audit_table_upgrade',
        'test_legacy_uncertainty_record_does_not_invent_observation',
        'test_uncertainty_record_does_not_close_live_guard',
        'test_uncertainty_replay_retains_original_observation_across_real_guard',
        'test_real_guarded_write_and_restoration_are_distinct_from_unused',
        'test_queue_cancellation_does_not_return_other_owners_grant',
        'test_empty_guard_history_and_matching_bytes_do_not_prove_no_write',
    )
    # The separately recorded unused-return red remains an open criterion.
    return unittest.TestSuite(UnusedReturnProbe(name) for name in names)
