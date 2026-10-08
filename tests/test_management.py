import contextlib
import io
import os
import subprocess
import sqlite3
from unittest import mock
from agent_chat import cli
from test_remote_web import TOKEN
import time
import uuid
from agent_chat.core import Coordinator
from agent_chat.remote import RemoteCoordError
from test_root_orchestrator import RootOrchestratorTests


class RetainedManagementTests(RootOrchestratorTests):
    # Reuse actual authenticated HTTP/host worker fixture, not inherited test cases.
    def setUp(self):
        super().setUp()
        self.worker_thread=str(uuid.uuid4())
        for sid in ('po','retained','reviewer'):
            self.call('register',session=sid,agent=sid)
        self.call('bind',session='po',thread=str(uuid.uuid4()))
        self.call('bind',session='retained',thread=self.worker_thread)
        self.rpc.status[self.worker_thread]='idle';self.rpc.queues[self.worker_thread]=[];self.rpc.history[self.worker_thread]=[]
        with contextlib.closing(Coordinator(self.db)) as coord:
            coord.db.execute('INSERT INTO project_orchestrators VALUES(?,?)',('po','a'))
            coord.db.execute("INSERT INTO agent_controls VALUES('retained','stopped',?)",(time.time(),))
        with self.server.projects.connection() as db:
            db.execute("UPDATE root_projects SET orchestrator_session='po' WHERE project_id='default'")
            db.commit()

    def grant(self,target='retained',**kwargs):
        values=dict(project='default',po='po',target=target,expected_thread=self.worker_thread,
                    expected_host='mac',request_id=str(uuid.uuid4()),confirm=True)
        values.update(kwargs)
        return self.root('delegate-management',**values)

    def test_root_grant_retains_main_and_po_resume_completes(self):
        from agent_chat.root_orchestrator import assert_management_target
        with self.assertRaisesRegex(RemoteCoordError,'delegate-management'):
            self.call('agent-resume',session='po',to='retained',expected_thread=self.worker_thread,request_id=str(uuid.uuid4()),confirm=True)
        rid=str(uuid.uuid4());out=io.StringIO()
        with mock.patch.dict(os.environ,{'AGENT_CHAT_HOST_ID':'mac'}),contextlib.redirect_stdout(out):
            self.assertEqual(cli.main(['--server',self.url,'--api-token',TOKEN,'--session','a','root',
                'delegate-management','--project-id','default','--po-session','po','--target-session','retained',
                '--expected-thread',self.worker_thread,'--expected-host','mac','--request-id',rid,'--confirm']),0)
        subprocess.run(['jq','-e','--arg','rid',rid,'.id == $rid and .target_session == "retained"'],
                       input=out.getvalue(),text=True,capture_output=True,check=True)
        audit=self.grant(request_id=rid)
        self.assertEqual(self.grant(request_id=rid),audit)
        with contextlib.closing(Coordinator(self.db)) as coord:
            with self.assertRaisesRegex(sqlite3.IntegrityError,'immutable'):
                coord.db.execute('DELETE FROM agent_management_requests WHERE id=?',(rid,))
        status=self.call('agent-status',session='po',to='retained')
        self.assertEqual(status['manager_session'],'po');self.assertIsNone(status['parent_session'])
        self.assertEqual(status['thread_id'],self.worker_thread)
        self.assertEqual(self.call('context',session='retained')['manager_session'],'po')
        with contextlib.closing(Coordinator(self.db)) as coord:
            coord.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?,?,?)',('native-child',None,'retained','/child',time.time()))
            assert_management_target(coord.db,'po','native-child')
        job=self.call('agent-resume',session='po',to='retained',expected_thread=self.worker_thread,request_id=str(uuid.uuid4()),confirm=True)
        self.bridge.tick()
        status=self.call('agent-status',session='po',to='retained')
        self.assertEqual(status['requests'][0]['id'],job['id'])
        self.assertEqual(status['requests'][0]['status'],'completed',status)
        self.assertEqual(status['admission'],'running')
        self.assertFalse(any(method in ('thread/start','turn/start','turn/interrupt') for method,_ in self.rpc.calls))

    def test_unbound_reviewer_grant_is_not_native_restoration(self):
        self.grant(target='reviewer',expected_thread=None)
        with self.assertRaisesRegex(RemoteCoordError,'exact current main thread'):
            self.call('agent-resume',session='po',to='reviewer',expected_thread=str(uuid.uuid4()),request_id=str(uuid.uuid4()),confirm=True)
        with self.assertRaisesRegex(RemoteCoordError,'exact current main-conversation binding'):
            self.call('session-reset',session='po',to='reviewer',expected_thread=str(uuid.uuid4()),prompt='Review',request_id=str(uuid.uuid4()),confirm=True)
        self.assertIsNone(self.call('agent-status',session='po',to='reviewer')['thread_id'])

    def test_exact_authority_binding_and_immutable_retry(self):
        rid=str(uuid.uuid4());self.grant(request_id=rid)
        for values,pattern in ((dict(expected_host='other'),'differs'),(dict(target='reviewer'),'differs')):
            with self.assertRaisesRegex(RemoteCoordError,pattern):self.grant(request_id=rid,**values)
        with self.assertRaisesRegex(RemoteCoordError,'binding/host changed'):self.grant(expected_thread=str(uuid.uuid4()))
        with self.assertRaisesRegex(RemoteCoordError,'designated PO'):self.grant(po='retained')
        with self.assertRaisesRegex(RemoteCoordError,'canonical home'):self.root('delegate-management',session='po',project='default',po='po',target='retained',expected_thread=self.worker_thread,expected_host='mac',request_id=str(uuid.uuid4()),confirm=True)

    def test_excluded_identity_and_child_cannot_gain_edge(self):
        for target in ('a','po',self.server.sender_session):
            with self.assertRaisesRegex(RemoteCoordError,'cannot be delegated'):
                self.grant(target=target,expected_thread=None,expected_host=None)
        self.call('register',session='child-route',agent='child-route')
        self.call('bind',session='child-route',parent_session='po',agent_path='/child-route')
        with self.assertRaisesRegex(RemoteCoordError,'native child routes'):
            self.grant(target='child-route',expected_thread=None)
        with contextlib.closing(Coordinator(self.db)) as coord:
            coord.db.execute('CREATE TABLE IF NOT EXISTS agent_management (target_session TEXT PRIMARY KEY,manager_session TEXT NOT NULL,request_id TEXT NOT NULL)')
            coord.db.execute('INSERT INTO agent_management VALUES(?,?,?)',('retained','different-po',str(uuid.uuid4())))
        with self.assertRaisesRegex(RemoteCoordError,'conflicting explicit manager'):self.grant()
