"""Legacy native history must supply proof, never an invented empty result."""
import unittest
import tempfile
from pathlib import Path
from types import SimpleNamespace

from agent_chat.bridge import Bridge
from agent_chat.core import CoordError, Coordinator
from agent_chat.control_worker import ControlWorker
from agent_chat.native_history import turns_page
from agent_chat.reset_worker import ResetWorker
from agent_chat.rpc import RpcError, TransportError
from agent_chat.supervisor_worker import SupervisorWorker


class LegacyRpc:
    def __init__(self):
        self.calls = []
        self.turns = [{'id': 'turn-1', 'status': 'completed', 'items': [
            {'type': 'userMessage', 'clientId': 'wake-1'}]}]
        self.thread = {'id': 'thread-1', 'status': {'type': 'idle'}, 'canAcceptDirectInput': True}
        self.error = RpcError(-32601, 'Method not found')

    def request(self, method, params):
        self.calls.append((method, params))
        if method == 'thread/turns/list':
            raise self.error
        if method == 'thread/read':
            result = dict(self.thread)
            if params.get('includeTurns'):
                result['turns'] = self.turns
            return {'thread': result}
        if method == 'thread/queue/list':
            return {'data': [], 'nextCursor': None}
        if method == 'thread/resume':
            return {'thread': dict(self.thread)}
        raise AssertionError(method)


class NativeHistoryTests(unittest.TestCase):
    def setUp(self):
        self.rpc = LegacyRpc()
        self.params = {'threadId': 'thread-1', 'limit': 1, 'itemsView': 'summary', 'sortDirection': 'desc'}

    def test_supported_read_history_is_ordered_and_paginated_without_resume(self):
        self.rpc.turns.append({'id': 'turn-2', 'status': 'completed', 'items': []})
        self.rpc.turns[0]['items'].append({'id': 'tool', 'type': 'commandExecution', 'aggregatedOutput': 'private output'})
        first = turns_page(self.rpc, self.params)
        second = turns_page(self.rpc, dict(self.params, cursor=first['nextCursor']))
        self.assertEqual([first['data'][0]['id'], second['data'][0]['id']], ['turn-2', 'turn-1'])
        self.assertIsNone(second['nextCursor'])
        self.assertNotIn('aggregatedOutput', second['data'][0]['items'][1])
        self.assertEqual(second['data'][0]['items'][0]['clientId'], 'wake-1')
        self.assertFalse(any(method == 'thread/resume' for method, _ in self.rpc.calls))
        self.assertEqual(sum(method == 'thread/turns/list' for method, _ in self.rpc.calls), 1)

    def test_wrong_thread_absent_or_malformed_history_withholds_proof(self):
        for response in ({'thread': {'id': 'other', 'turns': []}},
                         {'thread': {'id': 'thread-1'}},
                         {'thread': {'id': 'thread-1', 'turns': None}},
                         {'thread': {'id': 'thread-1', 'turns': [False]}}):
            rpc = LegacyRpc()
            original = rpc.request
            rpc.request = lambda method, params: response if method == 'thread/read' else original(method, params)
            with self.assertRaises(CoordError):
                turns_page(rpc, self.params)
        for turn in ({}, {'id': 'turn', 'status': 'completed', 'items': None},
                     {'id': 'turn', 'status': 'completed', 'items': [False]}):
            self.rpc.turns = [turn]
            with self.assertRaises(CoordError):
                turns_page(self.rpc, self.params)
        for error in (RpcError(-1, 'unavailable'), TransportError('disconnected')):
            self.rpc.error = error
            with self.assertRaises(type(error)):
                turns_page(self.rpc, self.params)

    def test_worker_proof_paths_share_the_supported_legacy_read(self):
        temporary=tempfile.TemporaryDirectory(prefix='native-history-coordinator-')
        self.addCleanup(temporary.cleanup)
        coord=Coordinator(Path(temporary.name)/'state.sqlite3')
        self.addCleanup(coord.close)
        bridge = Bridge(SimpleNamespace(coord=coord,observe=lambda *args: None), self.rpc)
        self.assertTrue(bridge.history_contains({'thread_id': 'thread-1', 'id': 'wake-1'}))
        self.assertFalse(bridge.history_contains({'thread_id': 'thread-1', 'id': 'missing'}))
        reset = ResetWorker(bridge)
        self.assertFalse(reset.empty_history('thread-1'))
        self.rpc.turns = []
        self.assertTrue(reset.empty_history('thread-1'))
        self.rpc.turns = [{'id': 'running-turn', 'status': 'inProgress', 'items': []}]
        self.rpc.thread['status'] = {'type': 'active'}
        controls = ControlWorker(bridge)
        observation, _, turns, _ = controls.observe({'thread_id': 'thread-1', 'action': 'inspect'})
        self.assertEqual(observation['turn_id'], 'running-turn')
        self.assertEqual(len(turns), 1)
        passive, _ = controls.passive_observe({'thread_id': 'thread-1'})
        self.assertEqual(passive['turn_id'], 'running-turn')
        job = {'thread_id': 'thread-1', 'admission': 'running', 'wake_summary': {}, 'holds': [], 'open_guards': []}
        snapshot = SupervisorWorker(bridge)._snapshot(job)
        self.assertEqual(snapshot['history_state'], 'available')
        self.assertEqual(snapshot['turn_id'], 'running-turn')
        self.assertGreater(coord.db.execute('SELECT count(*) FROM execution_epoch_handoffs').fetchone()[0],0)

# Exact managed-error projection requirements: synthetic fixtures, not live proof.
import copy
import contextlib
import os
import sys
import tempfile
import threading
from unittest import mock
import time
import uuid
from agent_chat.remote import RemoteCoordError
from agent_chat.core import Coordinator
from test_supervisor import SupervisorWorkflowTests
from agent_chat.web import create_server
from agent_chat.remote import HttpClient
from test_remote_web import TOKEN


class TurnErrorMetadataTests(unittest.TestCase):
    def test_fallback_not_loaded_preserves_named_error_without_items(self):
        rpc=LegacyRpc()
        rpc.turns=[dict(id="failed",status="failed",items=[dict(type="agentMessage",text="PRIVATE")],
                       error=dict(message="synthetic error",additionalDetails=None,codexErrorInfo="other",body="PRIVATE"))]
        got=turns_page(rpc,dict(threadId="thread-1",limit=1,sortDirection="desc",itemsView="notLoaded"))
        self.assertEqual(got["data"],[dict(id="failed",status="failed",error=dict(
            message="synthetic error",additionalDetails=None,codexErrorInfo="other"),
            error_field_status=dict(error="supported",message="supported",additionalDetails="null",codexErrorInfo="supported"))])
        self.assertNotIn("PRIVATE",repr(got))

    def snapshot(self, turns):
        rpc=LegacyRpc()
        original=rpc.request
        def request(method,params):
            if method=="thread/turns/list":
                rpc.calls.append((method,copy.deepcopy(params)))
                return dict(data=turns,nextCursor=None)
            return original(method,params)
        rpc.request=request
        bridge=Bridge(SimpleNamespace(observe=lambda *args:None),rpc)
        bridge.queue=lambda thread:[]
        job=dict(thread_id="thread-1",admission="running",wake_summary={},holds=[],open_guards=[])
        return SupervisorWorker(bridge)._snapshot(job),rpc.calls

    def test_exact_request_and_safe_native_error(self):
        got,calls=self.snapshot([dict(id="failed",status="failed",items=[dict(body="PRIVATE")],
            error=dict(message="synthetic error",additionalDetails="synthetic detail",codexErrorInfo="other",items="PRIVATE"),
            unrelated="PRIVATE")])
        self.assertEqual(next(p for method,p in calls if method=="thread/turns/list"),
                         dict(threadId="thread-1",limit=1,sortDirection="desc",itemsView="notLoaded"))
        self.assertEqual(got["turn_error"],dict(message="synthetic error",additionalDetails="synthetic detail",codexErrorInfo="other"))
        self.assertNotIn("PRIVATE",repr(got))
        self.assertIsInstance(got["turn_read_at"],float)
        self.assertFalse(any(method in ("thread/resume","turn/start") for method,_ in calls))

    def test_missing_turn_and_missing_error_are_explicit_null(self):
        for turns in ([],[dict(id="completed",status="completed")]):
            got,_=self.snapshot(turns)
            self.assertIsNone(got["turn_error"])
            self.assertEqual(got["turn_error_fields"]["error"],"absent")
            self.assertEqual(got["turn_id"],None if not turns else "completed")

    def test_unknown_error_shape_and_oversize_are_not_raw_disclosure(self):
        got,_=self.snapshot([dict(id="failed",status="failed",error=dict(
            message="X"*4097,additionalDetails={"body":"PRIVATE"},codexErrorInfo={"private":"PRIVATE"}))])
        self.assertEqual(got["turn_error"],dict(message=None,additionalDetails=None,codexErrorInfo=None))
        self.assertNotIn("PRIVATE",repr(got))


    def test_unpaired_surrogate_error_fields_are_explicit_null(self):
        got,_=self.snapshot([dict(id="failed",status="failed",error=dict(
            message="\\ud800".encode().decode("unicode_escape"),
            additionalDetails="\\udfff".encode().decode("unicode_escape"),
            codexErrorInfo="\\ud800".encode().decode("unicode_escape")))])
        self.assertEqual(got["turn_error"],dict(message=None,additionalDetails=None,codexErrorInfo=None))
        self.assertEqual(got["turn_error_fields"],dict(error="supported",message="unsupported",additionalDetails="unsupported",codexErrorInfo="unsupported"))


    def test_finite_known_codes_and_unknown_string_status(self):
        for code in ("contextWindowExceeded","sessionBudgetExceeded","usageLimitExceeded","rateLimitExceeded","flexUnavailable","serverOverloaded","cyberPolicy","misalignmentPolicyViolation","tooManyDenials","internalServerError","unauthorized","badRequest","threadRollbackFailed","sandboxError","other",):
            got,_=self.snapshot([dict(id="failed",status="failed",error=dict(message="synthetic",codexErrorInfo=code))])
            self.assertEqual(got["turn_error"]["codexErrorInfo"],code)
            self.assertEqual(got["turn_error_fields"]["codexErrorInfo"],"supported")
        got,_=self.snapshot([dict(id="failed",status="failed",error=dict(message="synthetic",codexErrorInfo="UNSELECTED_CODE"))])
        self.assertIsNone(got["turn_error"]["codexErrorInfo"])
        self.assertEqual(got["turn_error_fields"]["codexErrorInfo"],"unsupported")


    def test_finite_structured_status_and_turn_kind(self):
        for tag in ("httpConnectionFailed","responseStreamConnectionFailed","responseStreamDisconnected","responseTooManyFailedAttempts"):
            for inner in ({},{"httpStatusCode":None},{"httpStatusCode":0},{"httpStatusCode":65535}):
                value={tag:inner}
                got,_=self.snapshot([dict(id="failed",status="failed",error=dict(message="synthetic",codexErrorInfo=value))])
                self.assertEqual(got["turn_error"]["codexErrorInfo"],value)
                self.assertEqual(got["turn_error_fields"]["codexErrorInfo"],"supported")
            for inner in ({"httpStatusCode":True},{"httpStatusCode":1.0},{"httpStatusCode":-1},
                          {"httpStatusCode":65536},{"httpStatusCode":"403"},{"httpStatusCode":403,"body":"PRIVATE"}):
                got,_=self.snapshot([dict(id="failed",status="failed",error=dict(message="synthetic",codexErrorInfo={tag:inner}))])
                self.assertIsNone(got["turn_error"]["codexErrorInfo"])
                self.assertEqual(got["turn_error_fields"]["codexErrorInfo"],"unsupported")
        for kind in ("review","compact"):
            value={"activeTurnNotSteerable":{"turnKind":kind}}
            got,_=self.snapshot([dict(id="failed",status="failed",error=dict(message="synthetic",codexErrorInfo=value))])
            self.assertEqual(got["turn_error"]["codexErrorInfo"],value)
        for value in ({"activeTurnNotSteerable":{}},{"activeTurnNotSteerable":{"turnKind":"other"}},
                      {"activeTurnNotSteerable":{"turnKind":"review","reviewTarget":"PRIVATE"}},{"unknown":{"body":"PRIVATE"}}):
            got,_=self.snapshot([dict(id="failed",status="failed",error=dict(message="synthetic",codexErrorInfo=value))])
            self.assertIsNone(got["turn_error"]["codexErrorInfo"])
            self.assertEqual(got["turn_error_fields"]["codexErrorInfo"],"unsupported")


class ManagedTurnErrorTests(SupervisorWorkflowTests):
    def _make_fixture_thread(self):
        return threading.Thread(target=self.server.serve_forever,daemon=True)

    def _begin_fixture_thread(self,thread):
        thread.start()

    def start(self,**kwargs):
        self._fixture_server=None
        self._fixture_thread=None
        self._fixture_started=False
        self._fixture_start_attempted=False
        self._fixture_server_attempted=True
        self.server=create_server(self.db,port=0,api_token=TOKEN,**kwargs)
        self._fixture_server=self.server
        self.thread=self._make_fixture_thread()
        self._fixture_thread=self.thread
        self._fixture_start_attempted=True
        try:
            self._begin_fixture_thread(self.thread)
        finally:
            self._fixture_started=self.thread.ident is not None
        self.url="http://127.0.0.1:"+str(self.server.server_port)
        self.client=HttpClient(self.url,TOKEN)

    def _cleanup_fixture(self,first=None):
        server=getattr(self,"_fixture_server",None)
        thread=getattr(self,"_fixture_thread",None)
        started=getattr(self,"_fixture_started",False)
        records=[];uncertain=False
        if server is not None:
            try:
                server.stop_event.set()
                if thread is not None and started and thread.is_alive():
                    server.shutdown()
                server.server_close()
                records.append("server:close-returned")
            except BaseException as error:
                uncertain=True;records.append("server:close-unknown")
                if first is None:first=error
        else:
            if getattr(self,"_fixture_server_attempted",False):
                uncertain=True;records.append("server:constructor-unavailable")
            else:records.append("server:not-acquired")
        if thread is not None:
            try:
                if started or thread.ident is not None:
                    thread.join(3)
                    if thread.is_alive():
                        raise AssertionError("owned fixture thread remains")
                    records.append("thread:joined")
                else:
                    records.append("thread:never-started-observed")
            except BaseException as error:
                uncertain=True;records.append("thread:join-unknown")
                if first is None:first=error
        else:
            records.append("thread:not-acquired")
        self._cleanup_record=records
        # delete=False prevents implicit finalizer deletion on uncertain cleanup.
        if not uncertain and hasattr(self,"tmp"):
            try:self.tmp.cleanup();records.append("temporary:cleanup-returned")
            except BaseException as error:
                uncertain=True;records.append("temporary:cleanup-unknown")
                if first is None:first=error
        try:
            print("FIXTURE_CLEANUP managed-turn-error pid="+str(os.getpid())+
                  " scope="+repr(records)+" exhaustive=UNVERIFIED",file=sys.stderr,flush=True)
        except BaseException as error:
            records.append("record:unavailable")
            if first is None:first=error
        return first

    def setUp(self):
        self._fixture_server=None;self._fixture_thread=None
        self._fixture_started=False;self._fixture_start_attempted=False
        self._fixture_server_attempted=False
        original=tempfile.TemporaryDirectory
        try:
            # Selected pinned Python3.14 supports the public delete=False API.
            with mock.patch("test_remote_web.tempfile.TemporaryDirectory",
                    side_effect=lambda *a,**kw:original(*a,**dict(kw,delete=False))):
                super().setUp()
            print("FIXTURE_START managed-turn-error pid="+str(os.getpid())+
                  " root="+str(self.tmp.name),file=sys.stderr,flush=True)
        except BaseException as error:
            self._cleanup_fixture(error)
            raise

    def tearDown(self):
        error=self._cleanup_fixture()
        if error is not None:raise error

    def sample_error(self):
        self.enable()
        job=next(item for item in self.state.supervisor_jobs() if item["target_session"]=="a")
        original=self.rpc.request
        def request(method,params):
            if method=="thread/turns/list":
                self.rpc.calls.append((method,copy.deepcopy(params)))
                return dict(data=[dict(id="synthetic-failed",status="failed",
                    error=dict(message="synthetic provider detail",additionalDetails=None,codexErrorInfo=None),
                    items=[dict(text="PRIVATE")])],nextCursor=None)
            return original(method,params)
        self.rpc.request=request
        summary=SupervisorWorker(self.bridge)._snapshot(job)
        self.state.supervisor_observe(job,dict(state="error",summary=summary,source="native"))
        return job,summary

    def test_correct_managed_route_readback_and_freshness(self):
        job,summary=self.sample_error()
        row=self.target()
        self.assertEqual((row["target_session"],row["thread_id"],row["host_id"],row["route_generation"]),
                         (job["target_session"],job["thread_id"],job["host_id"],job["route_generation"]))
        got=row["observation"]
        self.assertEqual(got["summary"]["turn_error"]["message"],"synthetic provider detail")
        self.assertEqual(got["summary"]["turn_id"],"synthetic-failed")
        self.assertEqual(got["summary"]["turn_status"],"failed")
        self.assertEqual(got["summary"]["turn_read_at"],summary["turn_read_at"])
        self.assertGreaterEqual(got["age_seconds"],0)
        self.assertNotIn("PRIVATE",repr(got))

    def test_wrong_target_thread_host_generation_are_rejected(self):
        job,summary=self.sample_error()
        original=self.target()["observation"]["summary"]
        for key,value in (("target_session","b"),("thread_id","wrong"),("route_generation",-1)):
            changed=dict(job);changed[key]=value
            with self.assertRaises(RemoteCoordError):
                self.state.supervisor_observe(changed,dict(state="error",summary=dict(turn_error={"message":"SHOULD_NOT_PERSIST"}),source="native"))
            self.assertEqual(self.target()["observation"]["summary"],original)
        from agent_chat.supervisor import SupervisorState
        with contextlib.closing(Coordinator(self.db)) as coord:
            with self.assertRaises(CoordError):
                SupervisorState(coord).observe(job,dict(state="error",summary=summary,source="native"),"wrong-authenticated-host")
        self.assertEqual(self.target()["observation"]["summary"],original)

    def test_stale_route_label_and_old_generation_rejection(self):
        job,summary=self.sample_error()
        self.call("unbind");self.call("bind",thread=str(uuid.uuid4()))
        with self.assertRaises(RemoteCoordError):
            self.state.supervisor_observe(job,dict(state="error",summary=summary,source="native"))
        row=self.target()
        self.assertEqual(row["observation"]["state"],"stale_route")
        self.assertNotEqual(row["route_generation"],job["route_generation"])

    def test_exact_target_status_rejects_unmanaged_recipient(self):
        self.sample_error()
        with self.assertRaises(RemoteCoordError):
            self.status(to="b")

class DiagnosticPartialInitTests(unittest.TestCase):
    def case(self):
        return ManagedTurnErrorTests("test_correct_managed_route_readback_and_freshness")

    def test_server_acquired_thread_constructor_failure_closes_server(self):
        case=self.case()
        with mock.patch.object(case,"_make_fixture_thread",side_effect=RuntimeError("synthetic thread-constructor failure")):
            with self.assertRaisesRegex(RuntimeError,"synthetic thread-constructor"):
                case.setUp()
        case.doCleanups()
        self.assertIn("server:close-returned",case._cleanup_record)
        self.assertIn("thread:not-acquired",case._cleanup_record)
        self.assertEqual(case.server.socket.fileno(),-1)

    def test_thread_acquired_start_failure_never_joins_unstarted_thread(self):
        case=self.case()
        with mock.patch.object(case,"_begin_fixture_thread",side_effect=RuntimeError("synthetic start failure")):
            with self.assertRaisesRegex(RuntimeError,"synthetic start failure"):
                case.setUp()
        case.doCleanups()
        self.assertIn("server:close-returned",case._cleanup_record)
        self.assertIn("thread:never-started-observed",case._cleanup_record)
        self.assertEqual(case.server.socket.fileno(),-1)

    def test_only_started_thread_owned_join_without_server(self):
        case=self.case()
        case._fixture_server=None
        case._fixture_thread=threading.Thread(target=lambda:None)
        case._fixture_thread.start()
        case._fixture_started=True
        self.assertIsNone(case._cleanup_fixture())
        self.assertIn("server:not-acquired",case._cleanup_record)
        self.assertIn("thread:joined",case._cleanup_record)
