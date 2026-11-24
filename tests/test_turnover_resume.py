import contextlib
import time
import unittest
import uuid
from unittest import mock
from agent_chat.core import Coordinator
from agent_chat import root_lifecycle
from agent_chat.rpc import TransportError
from agent_chat.session_reset import SessionResetState
from agent_chat.remote import HttpClient
from test_remote_web import TOKEN
from test_turnover_bootstrap_http import TurnoverBootstrapHttpTests


class TurnoverResumeTests(TurnoverBootstrapHttpTests):
    def setup_bootstrap(self):
        project, route, path, params, state, bridge = self.prepare_parked()
        self.root('direct-create', **params)
        original = self.rpc.request
        def request(method, values):
            if method == 'model/list':
                return {'data':[{'model':'gpt-6.2-sol','supportedReasoningEfforts':[{'reasoningEffort':'high'}]}]}
            return original(method, values)
        with mock.patch.object(self.rpc, 'request', side_effect=request): bridge.tick()
        self.root('turnover-bootstrap', project=project, request_id=params['request_id'], confirm=True)
        return project, route, path, params, state, bridge, request

    def test_completed_po_unblocked_by_explicit_resume(self):
        project, route, path, params, state, bridge, request = self.setup_bootstrap()
        with mock.patch.object(self.rpc, 'request', side_effect=request): bridge.tick()
        result = self.root('resume-project', project=project, request_id=params['parked_stop'], confirm=True)
        self.assertTrue(result['barrier_complete'], result['errors'])
        with contextlib.closing(Coordinator(path, route['session'])) as coord:
            job = SessionResetState(coord).job(params['request_id'])
            self.assertEqual(job['status'], 'completed')
            self.assertFalse(root_lifecycle.stopped(coord.db, job['target_session']))
        self.rpc.status[job['new_thread_id']] = 'idle'
        client = HttpClient(self.url, TOKEN, project=project)
        client.call('/api/coord', {'op':'send','session':route['session'],'host_id':'mac',
                                 'params':{'to':job['target_session'],'body':'Fresh PO work after resume'}})
        bridge.tick()
        adds = [(m,v) for m,v in self.rpc.calls if m == 'thread/queue/add']
        self.assertEqual(len(adds), 2)
        self.assertEqual(adds[-1][1]['threadId'], job['new_thread_id'])

    def test_uncertain_add_allows_resume_without_replay(self):
        project, route, path, params, state, bridge, request = self.setup_bootstrap()
        lost = []
        def lost_add(method, values):
            result = request(method, values)
            if method == 'thread/queue/add' and not lost:
                lost.append(True)
                raise TransportError('lost add response after acceptance')
            return result
        with mock.patch.object(self.rpc, 'request', side_effect=lost_add):
            with self.assertRaises(TransportError): bridge.tick()
        with contextlib.closing(Coordinator(path, route['session'])) as coord:
            job = SessionResetState(coord).job(params['request_id'])
            self.assertEqual(job['status'], 'uncertain')
        result = self.root('resume-project', project=project, request_id=params['parked_stop'], confirm=True)
        self.assertTrue(result['barrier_complete'], result['errors'])
        bridge.tick()
        with contextlib.closing(Coordinator(path, route['session'])) as coord:
            self.assertEqual(SessionResetState(coord).job(params['request_id'])['status'], 'uncertain')
            self.assertTrue(root_lifecycle.stopped(coord.db, job['target_session']))
            unsettled = coord.db.execute('SELECT count(*) FROM execution_epoch_handoffs WHERE actor=? AND settled_at IS NULL', (job['target_session'],)).fetchone()[0]
            self.assertGreaterEqual(unsettled, 1)
        self.assertEqual([m for m, _ in self.rpc.calls if m in ('thread/queue/add','thread/queue/start')], ['thread/queue/add'])
        self.assertEqual([q['clientUserMessageId'] for q in self.rpc.queues[job['new_thread_id']]], [params['request_id']])

    def test_verified_parked_worker_receives_new_work_after_resume(self):
        project, route, path, params, state, bridge, request = self.setup_bootstrap()
        with mock.patch.object(self.rpc, 'request', side_effect=request): bridge.tick()
        with contextlib.closing(Coordinator(path, route['session'])) as coord:
            po = SessionResetState(coord).job(params['request_id'])
        client = HttpClient(self.url, TOKEN, project=project)
        client.call('/api/coord', {'op':'context','session':po['target_session'],'host_id':'mac','params':{}})
        worker_id = str(uuid.uuid4())
        worker = client.call('/api/coord', {'op':'parked-agent-create','session':po['target_session'],'host_id':'mac',
            'params':{'agent':'worker','expected_thread':po['new_thread_id'],'prompt':'Retained worker brief',
                      'request_id':worker_id,'bootstrap_id':params['request_id'],'confirm':True,
                      'model':'gpt-6.1-terra','reasoning_effort':'high','cwd':str(self.workspace)}})
        original = self.rpc.request
        def worker_request(method, values):
            if method == 'model/list':
                return {'data':[{'model':'gpt-6.1-terra','supportedReasoningEfforts':[{'reasoningEffort':'high'}]}]}
            return original(method, values)
        with mock.patch.object(self.rpc,'request',side_effect=worker_request): bridge.tick()
        with contextlib.closing(Coordinator(path, route['session'])) as coord:
            job = SessionResetState(coord).job(worker_id)
            self.assertEqual(job['status'], 'parked')
        self.assertTrue(self.root('resume-project',project=project,request_id=params['parked_stop'],confirm=True)['barrier_complete'])
        client.call('/api/coord', {'op':'send','session':route['session'],'host_id':'mac',
                                 'params':{'to':worker['target_session'],'body':'Fresh work after resume'}})
        bridge.tick()
        adds = [(m,v) for m,v in self.rpc.calls if m == 'thread/queue/add']
        self.assertEqual(len(adds), 2)
        self.assertEqual(adds[-1][1]['threadId'], job['new_thread_id'])

    def test_historical_unknown_epoch_does_not_block_later_phase(self):
        project, route, path, params, state, bridge, request = self.setup_bootstrap()
        with contextlib.closing(Coordinator(path, route['session'])) as coord:
            job = SessionResetState(coord).job(params['request_id'])
            now = time.time()
            coord.db.execute("UPDATE session_resets SET status='adding',updated_at=? WHERE id=?", (now,job['id']))
            coord.db.execute('INSERT INTO execution_epoch_handoffs VALUES(?,?,?,?,?,?,?,NULL,NULL)',
                ('old-epoch',job['old_thread_id'],job['target_session'],job['host_id'],'fixture','thread/queue/add',now-5))
        result = self.root('resume-project',project=project,request_id=params['parked_stop'],confirm=True)
        self.assertTrue(result['barrier_complete'], result['errors'])
        with contextlib.closing(Coordinator(path, route['session'])) as coord:
            epoch = coord.db.execute('SELECT settled_at,outcome FROM execution_epoch_handoffs WHERE id=?', ('old-epoch',)).fetchone()
            self.assertIsNone(epoch['settled_at']); self.assertIsNone(epoch['outcome'])
        self.assertFalse([m for m,_ in self.rpc.calls if m in ('thread/queue/add','thread/queue/start')])

class ParkedReleaseBoundaryTests(unittest.TestCase):
    def setUp(self):
        import sqlite3
        self.db = sqlite3.connect(':memory:'); self.db.row_factory = sqlite3.Row
        self.addCleanup(self.db.close)
        self.db.executescript('''
        CREATE TABLE root_parked_creations(request_id,target_session,stop_id,root_session);
        CREATE TABLE session_resets(id,status,new_thread_id,settings_digest,new_settings_digest,host_id);
        CREATE TABLE bridge_bindings(session_id,thread_id,parent_session);
        CREATE TABLE remote_session_hosts(session_id,host_id);
        CREATE TABLE root_team_stops(id,root_session,active,resumed_at);
        INSERT INTO root_parked_creations VALUES('job','target','stop','root');
        INSERT INTO session_resets VALUES('job','parked','thread','digest','digest','host');
        INSERT INTO bridge_bindings VALUES('target','thread',NULL);
        INSERT INTO remote_session_hosts VALUES('target','host');
        INSERT INTO root_team_stops VALUES('stop','root',0,1);
        ''')

    def test_only_verified_creation_with_exact_resumed_stop_releases(self):
        self.assertFalse(root_lifecycle.parked(self.db,'target'))
        for sql in ("UPDATE session_resets SET status='uncertain'", "UPDATE bridge_bindings SET thread_id='other'",
                    "UPDATE session_resets SET new_settings_digest='different'", "UPDATE root_team_stops SET resumed_at=NULL",
                    "UPDATE root_team_stops SET active=1", "UPDATE root_team_stops SET root_session='other'",
                    "UPDATE remote_session_hosts SET host_id='other'"):
            self.db.execute('SAVEPOINT probe'); self.db.execute(sql)
            with self.subTest(sql=sql): self.assertTrue(root_lifecycle.parked(self.db,'target'))
            self.db.execute('ROLLBACK TO probe'); self.db.execute('RELEASE probe')
