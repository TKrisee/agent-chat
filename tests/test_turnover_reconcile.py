import contextlib
from unittest import mock

from agent_chat.core import Coordinator
from agent_chat.session_reset import SessionResetState
from test_parked_provision_http import ParkedProvisionHttpTests


class TurnoverReconciliationTests(ParkedProvisionHttpTests):
    def prepared(self):
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

    def job(self, path, route, request):
        with contextlib.closing(Coordinator(path, route['session'])) as coord:
            return SessionResetState(coord).job(request)

    def test_auto_started_exact_client_id_reconciles_without_replay(self):
        project, route, path, params, state, bridge, request = self.prepared()
        self.rpc.auto_admit = True
        with mock.patch.object(self.rpc,'request',side_effect=request): bridge.tick()
        job = self.job(path, route, params['request_id'])
        self.assertEqual(job['status'], 'completed', job['error'])
        self.assertEqual(self.rpc.status[job['new_thread_id']], 'active')
        self.assertEqual(self.rpc.queues[job['new_thread_id']], [])
        self.assertEqual([m for m,_ in self.rpc.calls].count('thread/queue/add'), 1)
        self.assertEqual([m for m,_ in self.rpc.calls].count('thread/queue/start'), 0)
        self.assertEqual([x['clientId'] for x in self.rpc.history[job['new_thread_id']]], [params['request_id']])
        self.assertEqual(next(p for p in self.root('stop-status', request_id=params['parked_stop'])['projects'] if p['project']==project)['active'], 1)

    def test_absent_queue_and_unrelated_history_stays_uncertain_without_replay(self):
        project, route, path, params, state, bridge, request = self.prepared()
        self.rpc.auto_admit = True
        update = state.reset_update
        def unrelated(job_id, expected, status, **values):
            result = update(job_id, expected, status, **values)
            if status == 'queued':
                self.rpc.history[result['new_thread_id']][0]['clientId'] = 'unrelated'
            return result
        with mock.patch.object(self.rpc,'request',side_effect=request), mock.patch.object(state,'reset_update',side_effect=unrelated):
            bridge.tick()
        job = self.job(path, route, params['request_id'])
        self.assertEqual(job['status'], 'uncertain', job['error'])
        self.assertEqual([m for m,_ in self.rpc.calls].count('thread/queue/add'), 1)
        self.assertEqual([m for m,_ in self.rpc.calls].count('thread/queue/start'), 0)

    def test_exact_history_does_not_mutate_unrelated_native_queue(self):
        project, route, path, params, state, bridge, request = self.prepared()
        self.rpc.auto_admit = True
        update = state.reset_update
        def extra(job_id, expected, status, **values):
            result = update(job_id, expected, status, **values)
            if expected == 'adding' and status == 'queued':
                self.rpc.queues[result['new_thread_id']].append({'id':'other','clientUserMessageId':'other'})
            return result
        with mock.patch.object(self.rpc,'request',side_effect=request), mock.patch.object(state,'reset_update',side_effect=extra):
            bridge.tick()
        job = self.job(path, route, params['request_id'])
        self.assertEqual(job['status'], 'completed')
        self.assertEqual(self.rpc.queues[job['new_thread_id']], [{'id':'other','clientUserMessageId':'other'}])
        self.assertEqual([m for m,_ in self.rpc.calls].count('thread/queue/start'), 0)

    def test_other_native_queue_item_is_not_started_or_changed(self):
        project, route, path, params, state, bridge, request = self.prepared()
        update = state.reset_update
        def extra(job_id, expected, status, **values):
            result = update(job_id, expected, status, **values)
            if expected == 'adding' and status == 'queued':
                self.rpc.queues[result['new_thread_id']].append({'id':'other','clientUserMessageId':'other'})
            return result
        with mock.patch.object(self.rpc,'request',side_effect=request), mock.patch.object(state,'reset_update',side_effect=extra):
            bridge.tick()
        job = self.job(path, route, params['request_id'])
        self.assertEqual(job['status'], 'queued')
        self.assertIn('sole exact native queue item', job['error'])
        self.assertEqual(len(self.rpc.queues[job['new_thread_id']]), 2)
        self.assertEqual([m for m,_ in self.rpc.calls].count('thread/queue/start'), 0)

    def test_changed_queue_id_never_replaces_known_submission_or_starts(self):
        project, route, path, params, state, bridge, request = self.prepared()
        update = state.reset_update
        original_id = []
        def replace_id(job_id, expected, status, **values):
            result = update(job_id, expected, status, **values)
            if expected == 'adding' and status == 'queued':
                item = self.rpc.queues[result['new_thread_id']][0]
                original_id.append(item['id'])
                item['id'] = 'different-queue-id'
            return result
        with mock.patch.object(self.rpc,'request',side_effect=request), mock.patch.object(state,'reset_update',side_effect=replace_id):
            bridge.tick()
            bridge.tick()
        job = self.job(path, route, params['request_id'])
        self.assertEqual(job['status'], 'uncertain')
        self.assertEqual(job['queue_id'], original_id[0])
        self.assertEqual(self.rpc.queues[job['new_thread_id']][0]['id'], 'different-queue-id')
        self.assertEqual([m for m,_ in self.rpc.calls].count('thread/queue/add'), 1)
        self.assertEqual([m for m,_ in self.rpc.calls].count('thread/queue/start'), 0)
