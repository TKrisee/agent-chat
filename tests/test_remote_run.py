import contextlib
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from unittest import mock

from agent_chat import cli
from agent_chat.core import Coordinator, CoordError
from agent_chat.remote import HttpClient, RemoteCoordError
from test_remote_web import RemoteWebFixture, TOKEN


class RemoteRunTests(RemoteWebFixture):
    def setUp(self):
        super().setUp()
        self.call('register', agent='alpha')
        self.claim = self.call('request', resource='shared', minutes=5)
        self.env = dict(os.environ, AGENT_CHAT_SERVER=self.url,
                        AGENT_CHAT_API_TOKEN=TOKEN, AGENT_CHAT_SESSION='a',
                        AGENT_CHAT_HOST_ID='mac', AGENT_CHAT_TOKEN=self.claim['token'],
                        AGENT_CHAT_STATE_DIR=str(Path(self.tmp.name)/'client-state'),
                        AGENT_CHAT_DB=str(Path(self.tmp.name)/'must-not-create.sqlite3'))

    def invoke(self, *args):
        output = io.StringIO()
        with mock.patch.dict(os.environ, self.env, clear=True), contextlib.redirect_stdout(output):
            result = cli.main(list(args))
        return result, output.getvalue()

    def runs(self):
        c = Coordinator(self.db)
        try: return [dict(r) for r in c.db.execute('SELECT * FROM remote_guarded_runs')]
        finally: c.close()

    def assert_group_dead(self, pid):
        self.assertFalse(Coordinator._alive(pid))
        self.assertFalse(Coordinator._alive(pid, True))

    def receipt(self, pids):
        directory = Path(self.tmp.name)
        (directory/'evidence.txt').write_text('Owned process group closed; fixture baseline restored.')
        value = dict(version=1, resource='shared', reservation_id=self.claim['reservation_id'],
                     restored=True, processes_closed=True, closed_at=time.time(),
                     evidence='evidence.txt', pids=pids)
        path=directory/'CLOSED'; path.write_text(json.dumps(value))
        return str(path)

    def expire(self):
        c=Coordinator(self.db)
        try: c.db.execute("UPDATE resources SET deadline=? WHERE name='shared'", (time.time()-1,))
        finally: c.close()

    def test_invalid_token_never_executes_command(self):
        marker=Path(self.tmp.name)/'sentinel'
        self.env['AGENT_CHAT_TOKEN']='invalid'
        with self.assertRaises(RemoteCoordError):
            self.invoke('run', 'shared', '--', sys.executable, '-c', 'from pathlib import Path; Path('+repr(str(marker))+').touch()')
        self.assertFalse(marker.exists())
        self.assertEqual(self.runs(), [])
        self.assertFalse(Path(self.env['AGENT_CHAT_DB']).exists())

    def test_run_propagates_remote_identity_preserves_child_flags_and_releases(self):
        marker=Path(self.tmp.name)/'env.json'
        code='import json,os,sys; open('+repr(str(marker))+',"w").write(json.dumps({"env":dict(os.environ),"args":sys.argv[1:]}))'
        result, output=self.invoke('run', 'shared', '--', sys.executable, '-c', code, '--server', 'child-server', '--session', 'child-session')
        self.assertEqual(result, 0)
        captured=json.loads(marker.read_text())
        self.assertEqual(captured['args'], ['--server','child-server','--session','child-session'])
        for key in ('AGENT_CHAT_SERVER','AGENT_CHAT_SESSION','AGENT_CHAT_API_TOKEN','AGENT_CHAT_TOKEN','AGENT_CHAT_HOST_ID'):
            self.assertEqual(captured['env'][key], self.env[key])
        self.assertNotIn('AGENT_CHAT_DB', captured['env'])
        run=self.runs()[0]; self.assertIsNotNone(run['closed_at']); self.assert_group_dead(run['local_pid'])
        self.assertTrue(json.loads(self.invoke('release','shared','--receipt',self.receipt([run['local_pid']]))[1])['released'])

    def test_descendant_is_stopped_even_after_leader_exits(self):
        marker=Path(self.tmp.name)/'child.pid'
        child='import time; time.sleep(60)'
        code='import subprocess,sys; p=subprocess.Popen([sys.executable,"-c",'+repr(child)+']); open('+repr(str(marker))+',"w").write(str(p.pid))'
        try:
            self.assertEqual(self.invoke('run','shared','--',sys.executable,'-c',code)[0], 0)
            run=self.runs()[0]; self.assert_group_dead(run['local_pid'])
            self.assertFalse(Coordinator._alive(int(marker.read_text())))
            self.assertIsNotNone(run['closed_at'])
        finally:
            if marker.exists():
                try: os.kill(int(marker.read_text()), signal.SIGKILL)
                except ProcessLookupError: pass

    def test_connection_loss_kills_group_but_leaves_reservation_for_proven_recovery(self):
        original=HttpClient.call
        def outage(client,path,payload):
            if payload.get('op')=='guard-pulse': raise RemoteCoordError('injected connection loss')
            return original(client,path,payload)
        with mock.patch.object(HttpClient,'call',outage), self.assertRaisesRegex(RemoteCoordError,'connection loss'):
            self.invoke('run','shared','--',sys.executable,'-c','import time; time.sleep(60)')
        run=self.runs()[0]; self.assert_group_dead(run['local_pid'])
        self.assertIsNone(run['closed_at'])
        self.assertEqual(self.call('status')['resources'][0]['owner_session'], 'a')
        self.expire()
        result=self.invoke('recover','shared','--receipt',self.receipt([run['local_pid']]))
        self.assertTrue(json.loads(result[1])['recovered'])

    def test_lost_attach_response_never_executes_unconfirmed_command(self):
        marker=Path(self.tmp.name)/'sentinel'
        original=HttpClient.call
        def lost(client,path,payload):
            result=original(client,path,payload)
            if payload.get('op')=='attach-guard-pid': raise RemoteCoordError('lost attach response')
            return result
        with mock.patch.object(HttpClient,'call',lost), self.assertRaises(RemoteCoordError):
            self.invoke('run','shared','--',sys.executable,'-c','from pathlib import Path; Path('+repr(str(marker))+').touch()')
        self.assertFalse(marker.exists())
        run=self.runs()[0]; self.assert_group_dead(run['local_pid'])
        self.expire()
        self.assertEqual(self.invoke('recover','shared','--receipt',self.receipt([run['local_pid']]))[0],0)

    def test_attach_failure_before_server_records_pid_can_recover_unstarted_run(self):
        original=HttpClient.call
        def lost(client,path,payload):
            if payload.get('op')=='attach-guard-pid': raise RemoteCoordError('offline before attach')
            return original(client,path,payload)
        with mock.patch.object(HttpClient,'call',lost), self.assertRaises(RemoteCoordError):
            self.invoke('run','shared','--',sys.executable,'-c','raise Exception("must not run")')
        self.assertEqual(self.runs()[0]['local_pid'],0)
        self.expire()
        self.assertEqual(self.invoke('recover','shared','--receipt',self.receipt([]))[0],0)

    def test_live_descendant_prevents_receipt_even_when_leader_is_dead(self):
        run=self.call('begin-guard', resource='shared',token=self.claim['token'])
        self.call('attach-guard-pid',run_id=run['run_id'],token=self.claim['token'],pid=123456789)
        self.expire()
        receipt=self.receipt([123456789])
        with mock.patch('agent_chat.cli.os.kill',side_effect=ProcessLookupError), mock.patch('agent_chat.cli.os.killpg',return_value=None):
            with self.assertRaisesRegex(CoordError,'process group'):
                self.invoke('recover','shared','--receipt',receipt)
        self.assertIsNone(self.runs()[0]['closed_at'])

    def test_local_caller_cannot_recover_remote_reservation(self):
        c=Coordinator(self.db,'local'); c.register('local')
        self.expire()
        try:
            with self.assertRaisesRegex(CoordError,'remote'):
                c.recover('shared',self.receipt([]))
        finally: c.close()
