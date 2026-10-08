"""Root receives complete role/authorization guidance without changing approvals."""
import contextlib
import hashlib
import io
import os
import uuid
from unittest import mock
from agent_chat import cli
from agent_chat.core import Coordinator
from agent_chat.remote import RemoteCoordError
from agent_chat.root_authority import ROOT_AUTHORITY
from test_remote_web import TOKEN
import test_root_restart as restart_fixture
import test_management as management_fixture


class RootAuthorityBootTests(restart_fixture.RootFreshStartTests):
    def test_fresh_root_receives_authority_and_preserves_approved_prompt(self):
        prompt='Resume authorized work across all projects. Retain every current restriction.'
        expected=hashlib.sha256(prompt.encode()).hexdigest()
        rid=str(uuid.uuid4());self.fresh(rid,prompt)
        self.bridge.tick()
        job=self.status(rid)
        self.assertEqual(job['status'],'starting')
        boot=self.rpc.history[job['new_thread_id']][0]['content'][0]['text']
        self.assertIn(ROOT_AUTHORITY,boot)
        self.assertIn('literal body and server-stamped',boot)
        self.assertIn('a later status/check request does not\ncancel it',boot)
        self.assertIn('Respect\nsystem/tool approval and every actual refusal',boot)
        self.assertEqual(boot.split('\n\nRoot boot prompt:\n',1)[1],prompt)
        with self.server.projects.connection() as db:
            stored=db.execute('SELECT prompt FROM root_fresh_requests WHERE id=?',(rid,)).fetchone()['prompt']
        self.assertEqual(hashlib.sha256(stored.encode()).hexdigest(),expected)
        self.publish_consumption=True;self.bridge.tick()
        self.assertEqual(self.status(rid)['status'],'completed')


class RootAuthorityCommandTests(management_fixture.RetainedManagementTests):
    def test_role_and_intent_are_discoverable_without_live_operation(self):
        for args in (['root','--help'],['root','delegate-management','--help']):
            out=io.StringIO()
            with mock.patch.dict(os.environ,{'AGENT_CHAT_HOST_ID':'mac'}),contextlib.redirect_stdout(out):
                with self.assertRaises(SystemExit) as exit:
                    cli.main(['--server',self.url,'--api-token',TOKEN,'--session','a',*args])
            self.assertEqual(exit.exception.code,0)
            self.assertIn(ROOT_AUTHORITY,out.getvalue())
        self.assertIn('no new human approval round trip',' '.join(out.getvalue().split()))
        with contextlib.closing(Coordinator(self.db)) as coord:
            self.assertIsNone(coord.db.execute("SELECT 1 FROM sqlite_master WHERE name='agent_management_requests'").fetchone())

    def test_missing_intent_flag_is_not_new_human_approval_dialogue(self):
        with self.assertRaisesRegex(RemoteCoordError,'--confirm intent confirmation for authorized orchestration'):
            self.grant(confirm=False)
        self.grant(confirm=True)
        self.assertEqual(self.call('agent-status',session='po',to='retained')['manager_session'],'po')


def load_tests(loader, tests, pattern):
    return loader.loadTestsFromNames([
        'test_root_authority.'+cls.__name__+'.'+name
        for cls in (RootAuthorityBootTests,RootAuthorityCommandTests)
        for name in cls.__dict__ if name.startswith('test_')])
