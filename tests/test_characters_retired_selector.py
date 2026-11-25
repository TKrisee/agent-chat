"""Private synthetic test of retired no-binding Characters admission."""
import contextlib
import time
from unittest.mock import patch
from agent_chat import exact_file_read
from agent_chat.bridge_state import BridgeState
from agent_chat.core import Coordinator, CoordError
from test_exact_file_read import ExactFileReadTests

class RetiredCharactersSelector(ExactFileReadTests):
    def route(self):
        self.call('register',session='p',host='host-a',agent='private-parent')
        self.call('register',session='c',host='host-a',agent='private-child')
        with contextlib.closing(Coordinator(self.db)) as coord:
            BridgeState(coord)
            coord.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?,?,?)',('p','private-thread',None,None,time.time()))
            coord.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?,?,?)',('c',None,'p','/private-child',time.time()))
    def selector(self):
        return patch.multiple(exact_file_read, CHARACTERS_PROJECT='default',CHARACTERS_SESSION='c',
                CHARACTERS_PARENT='p',CHARACTERS_PARENT_THREAD='private-thread',CHARACTERS_AGENT_PATH='/private-child',
                CHARACTERS_HOST='host-a',CHARACTERS_FILES={str(self.src):'synthetic-test-only'})
    def test_retired_selector_rejects_new_request_before_any_acquisition(self):
        self.route()
        self.call('register',session='other',host='host-a',agent='other')
        with self.selector():
            with self.assertRaisesRegex(CoordError,'retired'):
                self.call('exact-file-request',session='c',path=str(self.src),minutes=5)
            with self.assertRaisesRegex(CoordError,'selection differs'):
                self.call('exact-file-request',session='other',path=str(self.src),minutes=5)
        with contextlib.closing(Coordinator(self.db)) as coord:
            self.assertIsNone(coord.db.execute('SELECT owner_session FROM resources WHERE name=?',(str(self.src),)).fetchone())
    def test_effects_denied_but_retained_status_and_result_stay_readable(self):
        self.route()
        with contextlib.closing(Coordinator(self.db,'c')) as coord:
            coord.resource_name=exact_file_read.exact_path
            claim=coord.request(str(self.src),5)
        with self.selector():
            for op in ('exact-file-request','exact-file-begin','exact-file-pulse','exact-file-release',
                       'exact-file-recover','exact-file-cancel'):
                with self.subTest(op=op), self.assertRaisesRegex(CoordError,'retired'):
                    self.call(op,session='c',path=str(self.src))
            self.assertIn('resources',self.call('exact-file-status',session='c',path=str(self.src)))
            self.assertIn('state',self.call('exact-file-result',session='c',path=str(self.src),reservation_id=claim['reservation_id']))
        with contextlib.closing(Coordinator(self.db)) as coord:
            self.assertEqual(coord.db.execute('SELECT owner_session FROM resources WHERE name=?',(str(self.src),)).fetchone()[0],'c')
    def test_retained_guard_context_preserves_owner_and_run(self):
        from agent_chat.remote_service import _begin_guard
        self.route()
        with contextlib.closing(Coordinator(self.db,'c')) as coord:
            coord.resource_name=exact_file_read.exact_path
            claim=coord.request(str(self.src),5)
            run=_begin_guard(coord,'c','host-a',dict(resource=str(self.src),token=claim['token'],reservation_id=claim['reservation_id']))
        with self.selector():
            result=self.call('exact-file-guard-context',session='c',path=str(self.src))
            self.assertEqual(result['reservation_id'],claim['reservation_id'])
            self.assertEqual([r['run_id'] for r in result['runs']],[run['run_id']])
            self.assertIsNone(result['runs'][0]['closed_at'])
            with self.assertRaisesRegex(CoordError,'different remote host'):
                self.call('exact-file-guard-context',session='c',host='wrong-host',path=str(self.src))
        with contextlib.closing(Coordinator(self.db)) as coord:
            self.assertEqual(coord.db.execute('SELECT owner_session FROM resources WHERE name=?',(str(self.src),)).fetchone()[0],'c')
            self.assertIsNone(coord.db.execute('SELECT closed_at FROM remote_guarded_runs WHERE run_id=?',(run['run_id'],)).fetchone()[0])

def load_tests(loader, tests, pattern):
    return loader.loadTestsFromNames(['test_retired_selector_rejects_new_request_before_any_acquisition',
                                      'test_effects_denied_but_retained_status_and_result_stay_readable',
                                      'test_retained_guard_context_preserves_owner_and_run'],RetiredCharactersSelector)
