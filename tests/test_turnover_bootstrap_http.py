import contextlib
import uuid
from unittest import mock

from agent_chat.core import CoordError, Coordinator
from agent_chat.remote import HttpClient, RemoteCoordError
from agent_chat.session_reset import SessionResetState
from test_remote_web import TOKEN
from test_parked_provision_http import ParkedProvisionHttpTests


class TurnoverBootstrapHttpTests(ParkedProvisionHttpTests):
    def test_parked_po_receives_only_its_retained_bootstrap(self):
        project, route, path, params, state, bridge = self.prepare_parked()
        self.root('direct-create', **params)
        original = self.rpc.request
        def request(method, values):
            if method == 'model/list':
                return {'data': [{'model': 'gpt-6.2-sol', 'supportedReasoningEfforts': [{'reasoningEffort': 'high'}]}]}
            return original(method, values)
        with mock.patch.object(self.rpc, 'request', side_effect=request):
            bridge.tick()
            with contextlib.closing(Coordinator(path, route['session'])) as coord:
                from agent_chat.session_reset import SessionResetState
                job = SessionResetState(coord).job(params['request_id'])
                self.assertEqual(job['status'], 'parked')
            grant = self.root('turnover-bootstrap', project=project, request_id=params['request_id'], confirm=True)
            self.assertEqual(grant['request_id'], params['request_id'])
            self.assertEqual(grant, self.root('turnover-bootstrap', project=project, request_id=params['request_id'], confirm=True))
            bridge.tick()
        with contextlib.closing(Coordinator(path, route['session'])) as coord:
            from agent_chat.session_reset import SessionResetState
            job = SessionResetState(coord).job(params['request_id'])
            self.assertEqual(job['status'], 'completed', job['error'])
            self.assertEqual(coord.db.execute('SELECT active FROM root_team_stops WHERE id=?', (params['parked_stop'],)).fetchone()['active'], 1)
        calls = [(method, values) for method, values in self.rpc.calls if method in ('thread/queue/add', 'thread/queue/start')]
        self.assertEqual([method for method, _ in calls], ['thread/queue/add', 'thread/queue/start'])
        self.assertEqual(calls[0][1]['clientUserMessageId'], params['request_id'])
        self.assertIn(params['prompt'], calls[0][1]['input'][0]['text'])
        client = HttpClient(self.url, TOKEN, project=project)
        with self.assertRaisesRegex(RemoteCoordError, 'agent creation is withheld until explicit resume'):
            client.call('/api/coord', {'op': 'agent-create', 'session': job['target_session'], 'host_id': 'mac',
                'params': {'agent': 'worker', 'expected_thread': job['new_thread_id'], 'prompt': 'Retained brief',
                           'request_id': str(uuid.uuid4()), 'confirm': True, 'model': 'gpt-6.1-terra',
                           'reasoning_effort': 'high', 'cwd': str(self.workspace)}})
        client.call('/api/coord', {'op':'context', 'session':job['target_session'], 'host_id':'mac', 'params':{}})
        worker_id = str(uuid.uuid4())
        worker_call = {'op':'parked-agent-create', 'session':job['target_session'], 'host_id':'mac',
            'params':{'agent':'worker', 'expected_thread':job['new_thread_id'], 'prompt':'Retained brief',
                      'request_id':worker_id, 'bootstrap_id':params['request_id'], 'confirm':True,
                      'model':'gpt-6.1-terra', 'reasoning_effort':'high', 'cwd':str(self.workspace)}}
        with self.assertRaisesRegex(RemoteCoordError, 'active successor PO'):
            client.call('/api/coord', dict(worker_call, session=route['session']))
        worker = client.call('/api/coord', worker_call)
        self.assertEqual(client.call('/api/coord', worker_call)['target_session'], worker['target_session'])
        def worker_request(method, values):
            if method == 'model/list':
                return {'data':[{'model':'gpt-6.1-terra','supportedReasoningEfforts':[{'reasoningEffort':'high'}]}]}
            return original(method, values)
        before = len([m for m, _ in self.rpc.calls if m in ('thread/queue/add', 'thread/queue/start')])
        with mock.patch.object(self.rpc, 'request', side_effect=worker_request):
            bridge.tick()
        with contextlib.closing(Coordinator(path, route['session'])) as coord:
            worker_job = SessionResetState(coord).job(worker_id)
            self.assertEqual(worker_job['status'], 'parked', worker_job['error'])
            self.assertEqual(worker_job['requested_model'], 'gpt-6.1-terra')
            self.assertEqual(worker_job['settings_digest'], worker_job['new_settings_digest'])
            self.assertEqual(self.rpc.history[worker_job['new_thread_id']], [])
            self.assertEqual(self.rpc.queues[worker_job['new_thread_id']], [])
            manager = coord.db.execute('SELECT manager_session FROM agent_management WHERE target_session=?', (worker['target_session'],)).fetchone()
            self.assertEqual(manager['manager_session'], job['target_session'])
            # Recreate both persisted crash boundaries for this subordinate:
            # the root handoff may still be active or may already be settled.
            epoch = coord.db.execute("SELECT id FROM execution_epoch_handoffs WHERE actor=? AND method='thread/name/set'", (worker['target_session'],)).fetchone()
            self.assertIsNotNone(epoch)
            coord.db.execute("UPDATE execution_epoch_handoffs SET settled_at=NULL,outcome=NULL WHERE id=?", (epoch['id'],))
            coord.db.execute("UPDATE root_native_handoffs SET active=1,settled_at=NULL,settlement=NULL WHERE kind='reset' AND job_id=? AND phase='materializing'", (worker_id,))
            state.reset_jobs()
            self.assertIsNotNone(coord.db.execute('SELECT settled_at FROM execution_epoch_handoffs WHERE id=?', (epoch['id'],)).fetchone()['settled_at'])
            self.assertEqual(coord.db.execute("SELECT active FROM root_native_handoffs WHERE kind='reset' AND job_id=? AND phase='materializing'", (worker_id,)).fetchone()['active'], 0)
            coord.db.execute("UPDATE execution_epoch_handoffs SET settled_at=NULL,outcome=NULL WHERE id=?", (epoch['id'],))
            coord.db.execute('UPDATE bridge_bindings SET thread_id=? WHERE session_id=?', (str(uuid.uuid4()), worker['target_session']))
            state.reset_jobs()
            self.assertIsNone(coord.db.execute('SELECT settled_at FROM execution_epoch_handoffs WHERE id=?', (epoch['id'],)).fetchone()['settled_at'])
            coord.db.execute('UPDATE bridge_bindings SET thread_id=? WHERE session_id=?', (worker_job['new_thread_id'], worker['target_session']))
            state.reset_jobs()
            self.assertIsNotNone(coord.db.execute('SELECT settled_at FROM execution_epoch_handoffs WHERE id=?', (epoch['id'],)).fetchone()['settled_at'])
            self.assertIsNotNone(coord.db.execute('SELECT request_id FROM root_turnover_subordinates WHERE request_id=?', (worker_id,)).fetchone())
        self.assertEqual(before, len([m for m, _ in self.rpc.calls if m in ('thread/queue/add', 'thread/queue/start')]))
        self.assertEqual(client.call('/api/coord', worker_call)['target_session'], worker['target_session'])
        client.call('/api/coord', {'op':'send', 'session':job['target_session'], 'host_id':'mac',
            'params':{'to':worker['target_session'], 'body':'Future assignment remains stopped'}})
        bridge.tick()
        self.assertEqual(before, len([m for m, _ in self.rpc.calls if m in ('thread/queue/add', 'thread/queue/start')]))

    def test_changed_designation_withholds_admitted_bootstrap(self):
        project, route, path, params, state, bridge = self.prepare_parked()
        self.root('direct-create', **params)
        original = self.rpc.request
        def request(method, values):
            if method == 'model/list':
                return {'data': [{'model': 'gpt-6.2-sol', 'supportedReasoningEfforts': [{'reasoningEffort': 'high'}]}]}
            return original(method, values)
        with mock.patch.object(self.rpc, 'request', side_effect=request):
            bridge.tick()
            self.root('turnover-bootstrap', project=project, request_id=params['request_id'], confirm=True)
            with self.server.projects.connection() as db:
                db.execute("UPDATE root_projects SET orchestrator_session='other' WHERE project_id=?", (project,))
            bridge.tick()
        self.assertFalse([method for method, _ in self.rpc.calls if method in ('thread/queue/add', 'thread/queue/start')])

    def test_resumed_stop_cannot_authorize_subordinate(self):
        project, route, path, params, state, bridge = self.prepare_parked()
        self.root('direct-create', **params)
        original = self.rpc.request
        def request(method, values):
            if method == 'model/list':
                return {'data': [{'model': 'gpt-6.2-sol', 'supportedReasoningEfforts': [{'reasoningEffort': 'high'}]}]}
            return original(method, values)
        with mock.patch.object(self.rpc, 'request', side_effect=request):
            bridge.tick()
            self.root('turnover-bootstrap', project=project, request_id=params['request_id'], confirm=True)
            bridge.tick()
        with contextlib.closing(Coordinator(path, route['session'])) as coord:
            job = SessionResetState(coord).job(params['request_id'])
        self.assertTrue(self.root('resume-project', project=project, request_id=params['parked_stop'], confirm=True)['barrier_complete'])
        client = HttpClient(self.url, TOKEN, project=project)
        with self.assertRaises(RemoteCoordError):
            client.call('/api/coord', {'op':'parked-agent-create', 'session':job['target_session'], 'host_id':'mac',
                'params':{'agent':'worker', 'expected_thread':job['new_thread_id'], 'prompt':'Retained brief',
                          'request_id':str(uuid.uuid4()), 'bootstrap_id':params['request_id'], 'confirm':True,
                          'model':'gpt-6.1-terra', 'reasoning_effort':'high', 'cwd':str(self.workspace)}})

    def test_designation_change_after_adding_cas_withholds_native_add(self):
        project, route, path, params, state, bridge = self.prepare_parked()
        self.root('direct-create', **params)
        original_rpc = self.rpc.request
        def request(method, values):
            if method == 'model/list':
                return {'data': [{'model': 'gpt-6.2-sol', 'supportedReasoningEfforts': [{'reasoningEffort': 'high'}]}]}
            return original_rpc(method, values)
        with mock.patch.object(self.rpc, 'request', side_effect=request):
            bridge.tick()
            self.root('turnover-bootstrap', project=project, request_id=params['request_id'], confirm=True)
            update = state.reset_update
            def changed(job_id, expected, status, **values):
                result = update(job_id, expected, status, **values)
                if status == 'adding':
                    with self.server.projects.connection() as db:
                        db.execute("UPDATE root_projects SET orchestrator_session='other' WHERE project_id=?", (project,))
                return result
            with mock.patch.object(state, 'reset_update', side_effect=changed):
                with self.assertRaises(RemoteCoordError): bridge.tick()
        self.assertFalse([m for m, _ in self.rpc.calls if m in ('thread/queue/add', 'thread/queue/start')])

    def test_designation_change_after_starting_cas_preserves_queued_item(self):
        project, route, path, params, state, bridge = self.prepare_parked()
        self.root('direct-create', **params)
        original_rpc = self.rpc.request
        def request(method, values):
            if method == 'model/list':
                return {'data': [{'model': 'gpt-6.2-sol', 'supportedReasoningEfforts': [{'reasoningEffort': 'high'}]}]}
            return original_rpc(method, values)
        with mock.patch.object(self.rpc, 'request', side_effect=request):
            bridge.tick()
            self.root('turnover-bootstrap', project=project, request_id=params['request_id'], confirm=True)
            update = state.reset_update
            def changed(job_id, expected, status, **values):
                result = update(job_id, expected, status, **values)
                if status == 'starting':
                    with self.server.projects.connection() as db:
                        db.execute("UPDATE root_projects SET orchestrator_session='other' WHERE project_id=?", (project,))
                return result
            with mock.patch.object(state, 'reset_update', side_effect=changed):
                with self.assertRaises(RemoteCoordError): bridge.tick()
        calls = [m for m, _ in self.rpc.calls if m in ('thread/queue/add', 'thread/queue/start')]
        self.assertEqual(calls, ['thread/queue/add'])

    def test_designation_change_after_epoch_keeps_issued_item_from_starting(self):
        project, route, path, params, state, bridge = self.prepare_parked()
        self.root('direct-create', **params)
        original_rpc = self.rpc.request
        def request(method, values):
            if method == 'model/list':
                return {'data': [{'model': 'gpt-6.2-sol', 'supportedReasoningEfforts': [{'reasoningEffort': 'high'}]}]}
            return original_rpc(method, values)
        with mock.patch.object(self.rpc, 'request', side_effect=request):
            bridge.tick()
            self.root('turnover-bootstrap', project=project, request_id=params['request_id'], confirm=True)
            call = state.call
            def change_after_epoch(op, **values):
                result = call(op, **values)
                if op == 'execution-epoch-begin' and values.get('method') == 'thread/queue/add':
                    with self.server.projects.connection() as db:
                        db.execute("UPDATE root_projects SET orchestrator_session='other' WHERE project_id=?", (project,))
                return result
            with mock.patch.object(state, 'call', side_effect=change_after_epoch):
                try: bridge.tick()
                except RemoteCoordError: pass
            bridge.tick()
        self.assertEqual([m for m, _ in self.rpc.calls if m in ('thread/queue/add','thread/queue/start')], ['thread/queue/add'])
        with contextlib.closing(Coordinator(path, route['session'])) as coord:
            job = SessionResetState(coord).job(params['request_id'])
        self.assertEqual([q['clientUserMessageId'] for q in self.rpc.queues[job['new_thread_id']]], [params['request_id']])

    def test_lost_queue_add_response_never_replays(self):
        project, route, path, params, state, bridge = self.prepare_parked()
        self.root('direct-create', **params)
        original_rpc = self.rpc.request
        lost = []
        def request(method, values):
            if method == 'model/list':
                return {'data': [{'model': 'gpt-6.2-sol', 'supportedReasoningEfforts': [{'reasoningEffort': 'high'}]}]}
            result = original_rpc(method, values)
            if method == 'thread/queue/add' and not lost:
                lost.append(True)
                from agent_chat.rpc import TransportError
                raise TransportError('lost add response after native acceptance')
            return result
        with mock.patch.object(self.rpc, 'request', side_effect=request):
            bridge.tick()
            self.root('turnover-bootstrap', project=project, request_id=params['request_id'], confirm=True)
            from agent_chat.rpc import TransportError
            with self.assertRaises(TransportError): bridge.tick()
            try: bridge.tick()
            except RemoteCoordError: pass
        self.assertEqual([m for m, _ in self.rpc.calls].count('thread/queue/add'), 1)
        with contextlib.closing(Coordinator(path, route['session'])) as coord:
            job = SessionResetState(coord).job(params['request_id'])
            self.assertIn(job['status'], ('uncertain','queued','completed'))
        resumed = self.root('resume-project', project=project, request_id=params['parked_stop'], confirm=True)
        self.assertTrue(resumed['barrier_complete'])
        with contextlib.closing(Coordinator(path, route['session'])) as coord:
            stop = coord.db.execute('SELECT active FROM root_team_stops WHERE id=?', (params['parked_stop'],)).fetchone()
            self.assertEqual(stop['active'], 0)
        queue_ids = [q['clientUserMessageId'] for q in self.rpc.queues[job['new_thread_id']]]
        history_ids = [entry.get('clientId') for entry in self.rpc.history[job['new_thread_id']]]
        self.assertEqual(queue_ids.count(params['request_id']) + history_ids.count(params['request_id']), 1)

    def test_resume_after_issued_add_preserves_item_without_start(self):
        project, route, path, params, state, bridge = self.prepare_parked()
        self.root('direct-create', **params)
        original_rpc = self.rpc.request
        def request(method, values):
            if method == 'model/list':
                return {'data': [{'model': 'gpt-6.2-sol', 'supportedReasoningEfforts': [{'reasoningEffort': 'high'}]}]}
            return original_rpc(method, values)
        attempts = []
        with mock.patch.object(self.rpc, 'request', side_effect=request):
            bridge.tick()
            self.root('turnover-bootstrap', project=project, request_id=params['request_id'], confirm=True)
            update = state.reset_update
            def resumed(job_id, expected, status, **values):
                result = update(job_id, expected, status, **values)
                if status == 'queued':
                    attempts.append(self.root('resume-project', project=project, request_id=params['parked_stop'], confirm=True))
                return result
            with mock.patch.object(state, 'reset_update', side_effect=resumed):
                with self.assertRaises(RemoteCoordError): bridge.tick()
            bridge.tick()
        self.assertTrue(attempts[0]['barrier_complete'])
        self.assertEqual([m for m, _ in self.rpc.calls if m in ('thread/queue/add','thread/queue/start')], ['thread/queue/add'])
        with contextlib.closing(Coordinator(path, route['session'])) as coord:
            job = SessionResetState(coord).job(params['request_id'])
        self.assertEqual([q['clientUserMessageId'] for q in self.rpc.queues[job['new_thread_id']]], [params['request_id']])

    def test_supported_resume_during_epoch_is_withheld(self):
        project, route, path, params, state, bridge = self.prepare_parked()
        self.root('direct-create', **params)
        original_rpc = self.rpc.request
        def request(method, values):
            if method == 'model/list':
                return {'data': [{'model': 'gpt-6.2-sol', 'supportedReasoningEfforts': [{'reasoningEffort': 'high'}]}]}
            return original_rpc(method, values)
        with mock.patch.object(self.rpc, 'request', side_effect=request):
            bridge.tick()
            self.root('turnover-bootstrap', project=project, request_id=params['request_id'], confirm=True)
            call = state.call
            attempts = []
            def resume_after_epoch(op, **values):
                result = call(op, **values)
                if op == 'execution-epoch-begin' and values.get('method') == 'thread/queue/add':
                    attempts.append(self.root('resume-project', project=project, request_id=params['parked_stop'], confirm=True))
                return result
            with mock.patch.object(state, 'call', side_effect=resume_after_epoch):
                try: bridge.tick()
                except RemoteCoordError: pass
        self.assertEqual(len(attempts), 1)
        self.assertIn('native input is in flight', attempts[0]['errors'][0]['error'])
        with contextlib.closing(Coordinator(path, route['session'])) as coord:
            self.assertEqual(coord.db.execute('SELECT active FROM root_team_stops WHERE id=?', (params['parked_stop'],)).fetchone()['active'], 1)
        self.assertEqual([m for m, _ in self.rpc.calls if m == 'thread/queue/add'], ['thread/queue/add'])

    def test_resume_before_epoch_withholds_native_add(self):
        project, route, path, params, state, bridge = self.prepare_parked()
        self.root('direct-create', **params)
        original_rpc = self.rpc.request
        def request(method, values):
            if method == 'model/list':
                return {'data': [{'model': 'gpt-6.2-sol', 'supportedReasoningEfforts': [{'reasoningEffort': 'high'}]}]}
            return original_rpc(method, values)
        with mock.patch.object(self.rpc, 'request', side_effect=request):
            bridge.tick()
            self.root('turnover-bootstrap', project=project, request_id=params['request_id'], confirm=True)
            update = state.reset_update
            attempts = []
            def resumed(job_id, expected, status, **values):
                result = update(job_id, expected, status, **values)
                if status == 'adding':
                    attempts.append(self.root('resume-project', project=project, request_id=params['parked_stop'], confirm=True))
                return result
            with mock.patch.object(state, 'reset_update', side_effect=resumed):
                with self.assertRaises(RemoteCoordError): bridge.tick()
        self.assertTrue(attempts[0]['barrier_complete'])
        self.assertFalse([m for m, _ in self.rpc.calls if m == 'thread/queue/add'])

    def test_local_reconciliation_requires_exact_consumed_bootstrap(self):
        project, route, path, params, state, bridge = self.prepare_parked()
        self.root('direct-create', **params)
        original_rpc = self.rpc.request

        def request(method, values):
            if method == 'model/list':
                return {'data': [{'model': 'gpt-6.2-sol', 'supportedReasoningEfforts': [{'reasoningEffort': 'high'}]}]}
            return original_rpc(method, values)

        update = state.reset_update

        def interrupt_after_queue(job_id, expected, status, **values):
            result = update(job_id, expected, status, **values)
            if status == 'queued':
                raise RuntimeError('fixture stops bridge after recorded queue add')
            return result

        request_id = params['request_id']
        with mock.patch.object(self.rpc, 'request', side_effect=request):
            bridge.tick()
            self.root('turnover-bootstrap', project=project, request_id=request_id, confirm=True)
            with mock.patch.object(state, 'reset_update', side_effect=interrupt_after_queue):
                with self.assertRaisesRegex(RuntimeError, 'fixture stops bridge'):
                    bridge.tick()

        with mock.patch('agent_chat.provider_service.configured',
                        return_value=(b'wrapper', ['--codex-server', 'ws://127.0.0.1:4500'])), \
                mock.patch('agent_chat.provider_service.selected_listener', return_value='listener'), \
                mock.patch('agent_chat.rpc.RpcClient'), \
                mock.patch('agent_chat.native_history.turns_page') as history:
            history.return_value = {'data': [{'id': 'native-turn', 'items': [
                {'type': 'userMessage', 'clientId': 'another-input'}]}]}
            with self.assertRaisesRegex(CoordError, "exact bootstrap consumption was not established"):
                self.root('turnover-bootstrap-reconcile', project=project, request_id=request_id, confirm=True)
            history.return_value['data'][0]['items'][0]['clientId'] = request_id
            receipt = self.root('turnover-bootstrap-reconcile', project=project, request_id=request_id, confirm=True)
            self.assertEqual(receipt['status'], 'completed')
            self.assertEqual(receipt['native_turn_id'], 'native-turn')
            self.assertTrue(self.root('turnover-bootstrap-reconcile', project=project, request_id=request_id,
                                      confirm=True)['already_recorded'])
        self.assertEqual([method for method, _ in self.rpc.calls if method in ('thread/queue/add',
                                                                               'thread/queue/start')],
                         ['thread/queue/add'])

    def test_local_continuation_delivers_only_existing_reply_once(self):
        project, route, path, params, state, bridge = self.prepare_parked()
        self.root('direct-create', **params)
        original_rpc = self.rpc.request

        def request(method, values):
            if method == 'model/list':
                return {'data': [{'model': 'gpt-6.2-sol', 'supportedReasoningEfforts': [{'reasoningEffort': 'high'}]}]}
            return original_rpc(method, values)

        request_id = params['request_id']
        with mock.patch.object(self.rpc, 'request', side_effect=request):
            bridge.tick()
            self.root('turnover-bootstrap', project=project, request_id=request_id, confirm=True)
            bridge.tick()
        client = HttpClient(self.url, TOKEN, project=project)
        with contextlib.closing(Coordinator(path, route['session'])) as coord:
            job = SessionResetState(coord).job(request_id)
        po = job['target_session']
        source = client.call('/api/coord', {'op':'send', 'session':po, 'host_id':'mac',
                            'params':{'to':route['session'], 'body':'Parked admin continuation needed'}})
        self.routed(project, 'context')
        reply = self.routed(project, 'send', to=po, body='Continue saved parked requests', reply_to=source['id'])
        thread_id = job['new_thread_id']

        def native(method, params):
            if method == 'thread/read':
                return {'thread': {'id': thread_id, 'status': {'type':'idle'},
                                   'canAcceptDirectInput': True}}
            if method == 'thread/queue/list':
                return {'data': []}
            if method == 'turn/start':
                self.assertEqual(params['threadId'], thread_id)
                self.assertEqual(params['clientUserMessageId'], reply['id'])
                return {'turn': {'id': 'continuation-turn'}}
            raise AssertionError(method)

        with mock.patch('agent_chat.provider_service.configured',
                        return_value=(b'wrapper', ['--codex-server', 'ws://127.0.0.1:4500'])), \
                mock.patch('agent_chat.provider_service.selected_listener', return_value='listener'), \
                mock.patch('agent_chat.rpc.RpcClient') as rpc:
            rpc_request = rpc.return_value.__enter__.return_value.request
            rpc_request.side_effect = native
            with self.assertRaisesRegex(CoordError, 'exact Root reply'):
                self.root('turnover-bootstrap-continue', project=project, request_id=request_id,
                          message_id=source['id'], confirm=True)
            receipt = self.root('turnover-bootstrap-continue', project=project, request_id=request_id,
                                message_id=reply['id'], confirm=True)
            self.assertEqual(receipt['native_turn_id'], 'continuation-turn')
            self.root('turnover-bootstrap-continue', project=project, request_id=request_id,
                      message_id=reply['id'], confirm=True)
            self.assertEqual([call.args[0] for call in rpc_request.call_args_list].count('turn/start'), 1)
        with contextlib.closing(Coordinator(path, route['session'])) as coord:
            self.assertEqual(coord.db.execute('SELECT active FROM root_team_stops WHERE id=?',
                                              (params['parked_stop'],)).fetchone()['active'], 1)
