"""Private synthetic successor fixture. No retained operator/native evidence."""
import contextlib, os, tempfile, time, unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from agent_chat import successor_admission as successor, capacity_observer as observer, root_local
from synthetic_successor_allowance import allowance
from test_capacity_admission import make_root_fixture

class Build01Tests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='synthetic-native02-');self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name).resolve()
        self.coord,self.master,self.registry=make_root_fixture(self.root);self.addCleanup(self.coord.close);self.db=self.coord.db
        from agent_chat.remote_service import _schema
        _schema(self.coord)
        self.config=SimpleNamespace(master=self.master,plist=self.root/'service.plist',wrapper=self.root/'wrapper',executable=self.root/'server')
        for module,name,value in ((root_local,'_installed_server_config',lambda:self.config),(root_local,'configured_server_database',lambda _:self.master)):
            p=patch.object(module,name,side_effect=value);p.start();self.addCleanup(p.stop)
        now=time.time();self.state='idle';self.direct=True;self.queue=0;self.after_native=None
        self.db.execute('INSERT INTO sessions(id,agent,registered_at,inbox_read_seq) VALUES(?,?,?,0)',(successor.CONSUMER,'synthetic-consumer',now))
        self.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?)',(successor.CONSUMER,successor.THREAD,successor.ROOT))
        self.db.execute('INSERT INTO remote_session_hosts(session_id,host_id) VALUES(?,?)',(successor.CONSUMER,successor.HOST))
        self.db.execute('INSERT INTO messages(id,sender_session,recipient_session,body,created_at,acked_at) VALUES(?,?,?,?,?,NULL)',(successor.BUILD_SOURCE,'web_operator_qYmkTK9qVoubfWPPTLr8iSzaIKqKk3Si',successor.ROOT,successor.BUILD_BODY,now))
        self.db.execute('INSERT INTO operator_message_origins VALUES(?,?,?)',(successor.BUILD_SOURCE,'web_operator_qYmkTK9qVoubfWPPTLr8iSzaIKqKk3Si','authenticated_operator_ui'))
        self.db.execute("UPDATE resources SET owner_session=NULL,reservation_id=NULL,token=NULL,granted_at=NULL,deadline=NULL,stale=0,reason=NULL WHERE name='validation-clone'")
        successor.install_schema(self.db);self.db.commit()
        @contextlib.contextmanager
        def native(bindings,registration):
            stamp=time.time();records={k:{'thread_id':v['thread'],'host':v['host'],'native_state':self.state,'direct_input':self.direct,'queue_count':self.queue,'turn_id_known':False,'checked_at':stamp} for k,v in bindings.items()}
            if self.after_native:self.after_native()
            yield records,stamp,stamp,lambda _:None
        for name in ('_native_observations','_consumer_native_observations'):
            p=patch.object(observer,name,side_effect=native);p.start();self.addCleanup(p.stop)
    def free_heavy(self):
        self.db.execute("UPDATE resources SET owner_session=NULL,reservation_id=NULL,token=NULL,granted_at=NULL,deadline=NULL,stale=0,reason=NULL WHERE name='heavy-unity:2'");self.db.commit()
    def issue(self,request_id,**changes):
        args=dict(master=self.master,registry=self.registry,home_project='default',home_session=successor.ROOT,host_id=successor.HOST,target_project='default',request_id=request_id,authorization_source_id=successor.BUILD_SOURCE,confirm=True);args.update(changes)
        with allowance(self.root,self.master,self.registry):
            return successor.issue(self.master,**args)
    def build(self):return self.issue(successor.BUILD_REQUEST)
    def request(self,session=None):
        from agent_chat.capacity_admission import _require_configured_root_caller,registry_authority_gate
        self.coord.session=session or successor.CONSUMER
        registry,owner,identity=_require_configured_root_caller(self.master,'default',successor.ROOT,successor.HOST,'default',successor.NATIVE02_REQUEST,successor.BUILD_SOURCE,True,self.registry)
        def record_pending(db,row):
            db.execute('INSERT INTO remote_reservation_outcomes (reservation_id,resource,owner_session,host_id,state,created_at) VALUES(?,?,?,?,?,?)',
                       (row['reservation_id'],row['name'],self.coord.session,successor.HOST,'pending',time.time()))
        with successor.observe_for_request(self.db,successor.RESOURCE,self.coord.session) as native:
            with registry_authority_gate(registry,owner,identity) as (gate,check):
                return self.coord.request(successor.RESOURCE,5,record_pending,registry_db=gate,registry_check=check,successor_native=native)
    def release(self,claim):
        from agent_chat.remote_service import _remote_clear
        self.coord.session=successor.CONSUMER
        import base64
        receipt=dict(version=1,resource=successor.RESOURCE,reservation_id=claim['reservation_id'],restored=True,processes_closed=True,closed_at=time.time(),evidence='synthetic-no-child',pids=[])
        return _remote_clear(self.coord,successor.CONSUMER,successor.HOST,{'resource':successor.RESOURCE,'token':claim['token'],'receipt':receipt,'evidence_base64':base64.b64encode(b'synthetic no child and no writes').decode()},'release')
    def evidence(self):
        names=('resources','resource_queue','guarded_runs','remote_guarded_runs','successor_admissions','successor_admission_observations','successor_admission_uses')
        return {name:[tuple(row) for row in self.db.execute('SELECT * FROM '+name)] for name in names}
