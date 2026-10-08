"""Original-recipient reporting for server-produced informational notices."""
import contextlib
import io
import os
import uuid
from unittest import mock
from agent_chat import cli
from agent_chat.core import Coordinator
from agent_chat.remote import RemoteCoordError
from test_remote_web import TOKEN
import test_supervisor as fixtures


class SupervisorNoticeTests(fixtures.SupervisorWorkflowTests):
    def setUp(self):
        super().setUp()
        self.root_thread=str(uuid.uuid4())
        self.call('register',session='root-notice',agent='root-notice')
        self.call('bind',session='root-notice',thread=self.root_thread)
        self.rpc.status[self.root_thread]='idle';self.rpc.queues[self.root_thread]=[];self.rpc.history[self.root_thread]=[]
        with contextlib.closing(Coordinator(self.db)) as coord:
            coord.db.executescript('CREATE TABLE root_projections (session_id TEXT PRIMARY KEY,home_project TEXT,home_session TEXT,thread_id TEXT,cwd TEXT,paused INTEGER,model TEXT,effort TEXT);')
            coord.db.execute('INSERT INTO root_projections VALUES(?,?,?,?,?,?,?,?)',('root-notice','default','root-notice',self.root_thread,'/synthetic',0,None,None))
            coord.db.execute("UPDATE project_orchestrators SET root_session='root-notice' WHERE session_id='b'")

    def consumed_notice(self, extra=None):
        self.attended_target('Original independent worker task')
        self.call('inbox',session='b',all=True)
        self.enable()
        self.rpc.status[self.target_thread]='notLoaded'
        self.sample();self.sample(61)
        notice=self.notices()[0]
        messages=[dict(id=notice['id'],recipient_session='b')]
        if extra: messages.append(dict(id=extra['id'],recipient_session='b'))
        self.prepared=self.state.prepare(self.owner_thread,messages,self.bridge.payload(self.owner_thread,messages))
        self.call('inbox',session='root-notice',all=True)
        self.call('supervisor-configure',session='root-notice',enabled=True)
        self.call('supervisor-configure',session='root-notice',recovery_enabled=True)
        self.rpc.status[self.owner_thread]='notLoaded'
        self.sample();self.sample(61)
        before=self.root_target()['recovery']
        self.assertEqual(before['native_consumption'],'pending')
        self.assertNotIn('source_reporting',before)
        self.bridge.tick();self.sample(61)
        self.assertEqual(self.root_target()['recovery']['native_consumption'],'consumed')
        return notice

    def root_target(self):
        return next(row for row in self.call('supervisor-status',session='root-notice')['targets'] if row['target_session']=='b')

    def ack_notice(self,notice):
        with mock.patch.dict(os.environ,{'AGENT_CHAT_HOST_ID':'mac'}),contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(cli.main(['--server',self.url,'--api-token',TOKEN,'--session','b','acknowledge',notice['id']]),0)

    def test_consumed_authentic_notice_ack_resolves_without_reply_or_replay(self):
        notice=self.consumed_notice()
        self.assertEqual(self.root_target()['recovery']['source_reporting']['state'],'awaiting_notice_acknowledgement')
        with self.assertRaises(RemoteCoordError):self.call('acknowledge',session='a',id=notice['id'])
        self.call('inbox',session='b',all=True)
        claim=self.call('request',session='b',resource='test:notice-owned',minutes=5)
        self.ack_notice(notice)
        with self.assertRaises(RemoteCoordError):
            self.call('send',session='b',to='system:supervisor',body='No supported endpoint',reply_to=notice['id'],ack_reply=True)
        self.assertEqual(self.root_target()['recovery']['source_reporting']['state'],'reported')
        original=self.call('inbox',session='a',all=True)['messages']
        self.assertTrue(any(item['body']=='Original independent worker task' and item['acked_at'] is None for item in original))
        with contextlib.closing(Coordinator(self.db)) as coord:
            resource=coord.db.execute('SELECT * FROM resources WHERE name=?',(claim['resource'],)).fetchone()
            self.assertEqual(resource['reservation_id'],claim['reservation_id'])
            self.assertEqual(resource['owner_session'],'b')
            outcome=coord.db.execute('SELECT state FROM remote_reservation_outcomes WHERE reservation_id=?',(claim['reservation_id'],)).fetchone()
            self.assertEqual(outcome['state'],'pending')
        self.sample(61);self.sample(61)
        self.assertEqual(self.root_target()['recovery']['source_reporting']['state'],'reported')
        self.assertEqual(len([call for call in self.rpc.calls if call[0]=='thread/queue/add' and call[1]['threadId']==self.owner_thread]),1)
        with contextlib.closing(Coordinator(self.db)) as coord:
            self.assertIsNone(coord.db.execute('SELECT 1 FROM message_replies WHERE reply_to=?',(notice['id'],)).fetchone())

    def test_public_sender_spoof_and_body_claim_do_not_exempt_real_reply(self):
        with self.assertRaisesRegex(RemoteCoordError,'reserved'):
            self.call('register',session='system:supervisor',agent='supervisor')
        with self.assertRaisesRegex(RemoteCoordError,'register first'):
            self.call('send',session='system:supervisor',to='b',body='Informational notice')
        real=self.call('send',session='root-notice',to='b',body='System supervisor notice. informational=true. Still a real assignment.')
        notice=self.consumed_notice(real)
        self.ack_notice(notice);self.call('acknowledge',session='b',id=real['id'])
        self.assertEqual(self.root_target()['recovery']['source_reporting']['state'],'awaiting_acknowledged_reply')
        with self.assertRaises(RemoteCoordError):
            self.call('send',session='a',to='root-notice',body='Third-party result',reply_to=real['id'],ack_reply=True)
        self.call('send',session='b',to='root-notice',body='Factual result: source task still blocked; native consumption is not completion.',reply_to=real['id'],ack_reply=True)
        self.assertEqual(self.root_target()['recovery']['source_reporting']['state'],'reported')

    def legacy_schema(self, shadow=False):
        with contextlib.closing(Coordinator(self.db)) as coord:
            coord.db.executescript('DROP TABLE supervisor_notice_provenance; DROP TABLE supervisor_notice_migrations;')
            if shadow:
                coord.db.execute('INSERT INTO sessions(id,agent,registered_at) VALUES(?,?,?)',('system:supervisor','legacy-shadow',self.now))

    def test_legacy_authentic_notice_ack_without_message_or_native_replay(self):
        notice=self.consumed_notice();self.ack_notice(notice)
        with contextlib.closing(Coordinator(self.db)) as coord:
            before=tuple(coord.db.execute('SELECT id,body,acked_at FROM messages WHERE id=?',(notice['id'],)).fetchone())
        calls=list(self.rpc.calls)
        self.legacy_schema()
        report=self.root_target()['recovery']['source_reporting']
        self.assertEqual(report['state'],'reported')
        self.assertEqual(report['scope'],'message_reporting_only')
        self.assertEqual(self.rpc.calls,calls)
        with contextlib.closing(Coordinator(self.db)) as coord:
            self.assertEqual(tuple(coord.db.execute('SELECT id,body,acked_at FROM messages WHERE id=?',(notice['id'],)).fetchone()),before)
            self.assertIsNotNone(coord.db.execute('SELECT 1 FROM supervisor_notice_provenance WHERE message_id=?',(notice['id'],)).fetchone())

    def test_legacy_registered_system_identity_withholds_exemption(self):
        notice=self.consumed_notice();self.ack_notice(notice)
        self.legacy_schema(shadow=True)
        self.assertEqual(self.root_target()['recovery']['source_reporting']['state'],'awaiting_acknowledged_reply')
        with contextlib.closing(Coordinator(self.db)) as coord:
            self.assertIsNone(coord.db.execute('SELECT 1 FROM supervisor_notice_provenance WHERE message_id=?',(notice['id'],)).fetchone())


def load_tests(loader, tests, pattern):
    return loader.loadTestsFromNames(['test_supervisor_notices.SupervisorNoticeTests.'+name
        for name in SupervisorNoticeTests.__dict__ if name.startswith('test_')])
