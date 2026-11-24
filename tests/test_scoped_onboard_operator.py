"""Fresh scoped onboarding after supported browser operator initialization."""
import contextlib
import unittest
import uuid
from unittest import mock
from agent_chat.core import Coordinator, CoordError
from agent_chat.web import operator_session
from test_scoped_onboarding import ScopedOnboardingTests


class OperatorInitializedOnboard(ScopedOnboardingTests):
    def setup_target(self):
        self.enroll()
        stop = str(uuid.uuid4()); self.call('stop-all',request_id=stop,confirm=True)
        target = self.projects.create('New app',workspace=str(self.workspace),instructions='Synthetic assignment')['id']
        path = self.projects.db_path(target)
        operator = operator_session(path)
        return stop,target,path,operator

    def test_browser_initialized_operator_is_preserved_through_onboarding(self):
        stop,target,path,operator = self.setup_target()
        with contextlib.closing(Coordinator(path)) as coord:
            self.assertTrue(coord.db.execute('SELECT 1 FROM operator_identities WHERE session_id=?',(operator,)).fetchone())
            coord.session = operator
            message = coord.send(operator,'Preserve operator note')['id']
        self.consume()
        args = dict(project=target,stop_id=stop,expected_thread=self.thread,request_id=str(uuid.uuid4()),confirm=True)
        result = self.call('onboard-project',**args)
        self.assertEqual(result['state'],'joined')
        self.assertEqual(self.call('onboard-project',**args),result)
        with contextlib.closing(Coordinator(path)) as coord:
            self.assertEqual(coord.db.execute('SELECT agent FROM sessions WHERE id=?',(operator,)).fetchone()[0],'operator')
            self.assertTrue(coord.db.execute('SELECT 1 FROM operator_identities WHERE session_id=?',(operator,)).fetchone())
            self.assertEqual(coord.db.execute('SELECT body FROM messages WHERE id=?',(message,)).fetchone()[0], 'Preserve operator note')
        with contextlib.closing(Coordinator(self.master)) as coord:
            self.assertEqual(coord.db.execute('SELECT active FROM root_team_stops WHERE id=?',(stop,)).fetchone()[0],1)

    def test_claimed_operator_without_server_stamp_is_rejected(self):
        stop,target,path,operator = self.setup_target()
        with contextlib.closing(Coordinator(path,'impostor')) as coord:
            coord.register('operator')
        self.consume()
        with self.assertRaises(CoordError):
            self.call('onboard-project',project=target,stop_id=stop,expected_thread=self.thread,request_id=str(uuid.uuid4()),confirm=True)

    def test_existing_native_work_is_rejected_without_mutation(self):
        from agent_chat.bridge_state import BridgeState
        stop,target,path,operator = self.setup_target()
        with contextlib.closing(Coordinator(path,operator)) as coord:
            BridgeState(coord)
            coord.db.execute('INSERT INTO bridge_jobs VALUES(?,?,?,?,?,?,?,?)',('wake_unknown','thread','payload','uncertain',None,'unknown',1,1))
        self.consume()
        with self.assertRaises(CoordError):
            self.call('onboard-project',project=target,stop_id=stop,expected_thread=self.thread,request_id=str(uuid.uuid4()),confirm=True)
        with contextlib.closing(Coordinator(path)) as coord:
            self.assertEqual(coord.db.execute('SELECT status FROM bridge_jobs WHERE id=?',('wake_unknown',)).fetchone()[0],'uncertain')

    def test_new_agent_between_preparation_and_join_is_rejected(self):
        stop,target,path,operator = self.setup_target()
        self.consume()
        original = self.roots.join
        def raced(root,item):
            with contextlib.closing(Coordinator(path,'late-agent')) as coord:
                coord.register('worker')
            return original(root,item)
        with mock.patch.object(type(self.roots),'join',side_effect=raced):
            with self.assertRaises(CoordError):
                self.call('onboard-project',project=target,stop_id=stop,expected_thread=self.thread,request_id=str(uuid.uuid4()),confirm=True)
        with contextlib.closing(Coordinator(path)) as coord:
            self.assertEqual(coord.db.execute('SELECT agent FROM sessions WHERE id=?',('late-agent',)).fetchone()[0],'worker')


def load_tests(loader, tests, pattern):
    return unittest.TestSuite([OperatorInitializedOnboard('test_browser_initialized_operator_is_preserved_through_onboarding'),OperatorInitializedOnboard('test_claimed_operator_without_server_stamp_is_rejected'),OperatorInitializedOnboard('test_existing_native_work_is_rejected_without_mutation'),OperatorInitializedOnboard('test_new_agent_between_preparation_and_join_is_rejected')])
