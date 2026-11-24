import contextlib
import os
import sqlite3
import subprocess
import sys
import uuid
from pathlib import Path
from unittest import mock
from agent_chat.bridge import Bridge
from agent_chat.bridge_state import BridgeState
from agent_chat.core import Coordinator
from test_remote_web import RemoteWebFixture, TOKEN
from test_project_activity import ObservedProvider


def jq(raw, expr):
    return subprocess.run(['jq','-er',expr],input=raw,stdout=subprocess.PIPE,check=True).stdout.decode().strip()


class SourceTraceTests(RemoteWebFixture):
    def start(self, **kwargs):
        from agent_chat import service, root_local
        for module, name in ((service, '_installed_server_config'),
                             (service, 'configured_server_database'),
                             (root_local, '_installed_server_config'),
                             (root_local, 'configured_server_database')):
            guard = mock.patch.object(module, name, side_effect=AssertionError('installed service discovery forbidden'))
            guard.start(); self.addCleanup(guard.stop)
        home = Path(self.tmp.name) / 'home'; home.mkdir(exist_ok=True)
        environment = mock.patch.dict(os.environ, {'HOME':str(home), 'CODEX_HOME':str(home / '.codex')})
        environment.start(); self.addCleanup(environment.stop)
        sessions = Path(self.tmp.name) / 'sessions'; sessions.mkdir(exist_ok=True)
        return super().start(sessions_root=sessions, **kwargs)

    def test_legacy_missing_correlation_and_missing_host_fail_closed(self):
        from agent_chat.bridge_state import source_trace
        with contextlib.closing(Coordinator(self.db,'local')) as local:
            local.register('local')
        for sid in ('sender','recipient'):
            status, raw, _ = self.request('POST','/api/coord',{'op':'register','session':sid,'host_id':'mac','params':{'agent':sid}})
            self.assertEqual(status,200,raw)
        status, raw, _ = self.request('POST','/api/coord',{'op':'send','session':'sender','host_id':'mac','params':{'to':'recipient','body':'new'}})
        source=jq(raw,'.id')
        result=source_trace(self.db,project_id='default',session_id='sender',host_id='mac',source_id=source)
        self.assertEqual(result['status'],'ok')
        self.assertIsNone(result['job_id']); self.assertIsNone(result['prepared_at'])
        self.assertEqual(source_trace(self.db,project_id='default',session_id='local',host_id='mac',source_id=source)['status'],'unauthorized')
        self.assertEqual(source_trace(self.db,project_id='wrong-project',session_id='sender',host_id='mac',source_id=source)['status'],'unauthorized')
        status, _, _ = self.request('POST','/api/coord',{'op':'source-trace','session':'sender','host_id':'mac','params':{'source_id':source}},auth=None)
        self.assertIn(status,(401,403))

    def test_authenticated_cli_exact_source_and_readonly_boundaries(self):
        for sid in ('sender','recipient','other'):
            status, raw, _ = self.request('POST','/api/coord',{'op':'register','session':sid,'host_id':'mac','params':{'agent':sid}})
            self.assertEqual(status,200,raw)
        status, raw, _ = self.request('POST','/api/coord',{'op':'send','session':'sender','host_id':'mac','params':{'to':'recipient','body':'private body'}})
        self.assertEqual(status,200,raw); source=jq(raw,'.id')
        with contextlib.closing(Coordinator(self.db,'recipient')) as coord:
            state=BridgeState(coord); thread=str(uuid.uuid4()); state.bind(thread_id=thread)
            provider=ObservedProvider(); provider.add(thread,self.tmp.name)
            Bridge(state,provider).tick(observe_policies=False)
        with sqlite3.connect(self.db) as db:
            before=db.execute('SELECT session_id,host_id,last_seen_at FROM remote_session_hosts ORDER BY session_id').fetchall()
            unread=db.execute('SELECT acked_at FROM messages WHERE id=?',(source,)).fetchone()
        env=dict(os.environ,AGENT_CHAT_HOST_ID='mac',PYTHONPATH='src')
        base=[sys.executable,'-m','agent_chat.cli','--server',self.url,'--api-token',TOKEN,'--project','default']
        def run(sid, host='mac'):
            env2=dict(env,AGENT_CHAT_HOST_ID=host)
            return subprocess.run(base+['--session',sid,'source-trace',source],capture_output=True,env=env2,timeout=8)
        for sid in ('sender','recipient'):
            result=run(sid); self.assertEqual(result.returncode,0,result.stderr.decode())
            self.assertEqual(jq(result.stdout,'.status'),'ok')
            self.assertEqual(jq(result.stdout,'.job_status'),'dispatched')
            self.assertEqual(jq(result.stdout,'.clock'),'unix_wall_seconds')
            self.assertEqual(jq(result.stdout,'.native_first_response_at == null and .recipient_delivery_at == null and .eligibility_at == null'),'true')
            self.assertNotIn(b'private body',result.stdout)
        self.assertEqual(jq(run('other').stdout,'.status'),'unauthorized')
        self.assertEqual(jq(run('sender','wrong-host').stdout,'.status'),'unauthorized')
        bad_project=subprocess.run([sys.executable,'-m','agent_chat.cli','--server',self.url,'--api-token',TOKEN,
            '--project','missing-project','--session','sender','source-trace',source],capture_output=True,env=env,timeout=8)
        self.assertNotEqual(bad_project.returncode,0)
        with sqlite3.connect(self.db) as db:
            self.assertEqual(before,db.execute('SELECT session_id,host_id,last_seen_at FROM remote_session_hosts ORDER BY session_id').fetchall())
            self.assertEqual(unread,db.execute('SELECT acked_at FROM messages WHERE id=?',(source,)).fetchone())
            self.assertEqual(db.execute('SELECT count(*) FROM message_reads WHERE message_id=?',(source,)).fetchone()[0],0)
            db.execute('DROP TRIGGER immutable_bridge_job_origin_delete')
            db.execute('DELETE FROM bridge_job_origins')
        legacy=run('sender')
        self.assertEqual(jq(legacy.stdout,'.status'),'ok')
        self.assertEqual(jq(legacy.stdout,'.job_status'),'dispatched')
        self.assertEqual(jq(legacy.stdout,'.prepared_at == null'),'true')
