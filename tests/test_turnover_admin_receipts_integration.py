"""Integration with the separate bound-retirement candidate, using disposable stores."""
import contextlib
import uuid
from pathlib import Path
from unittest import mock

from agent_chat.core import CoordError, Coordinator
from agent_chat.session_reset import SessionResetState
from test_turnover_admin_continuation import AdminContinuationTests


class AdminRetirementIntegrationTests(AdminContinuationTests):
    def test_seven_exact_bound_receipts_before_admission(self):
        state, bridge = self.project_bridge(self.project_id)
        original = self.rpc.request
        def provider(method, values):
            if method == 'model/list':
                return {'data': [{'model': 'gpt-6.1-terra', 'supportedReasoningEfforts': [{'reasoningEffort': 'high'}]}]}
            return original(method, values)
        workers = []
        self.po_client.call('/api/coord', dict(op='context', session=self.po['target_session'], host_id='mac', params={}))
        for i in range(7):
            request = str(uuid.uuid4())
            label = 'retired-worker-' + str(i)
            entry = self.po_client.call('/api/coord', dict(op='parked-agent-create', session=self.po['target_session'],
                host_id='mac', params=dict(agent=label, expected_thread=self.po_thread, prompt='role', request_id=request,
                bootstrap_id=self.bootstrap['request_id'], confirm=True, model='gpt-6.1-terra',
                reasoning_effort='high', cwd=str(self.workspace))))
            with mock.patch.object(self.rpc, 'request', side_effect=provider):
                bridge.tick()
            with contextlib.closing(Coordinator(self.path, self.project_root)) as coord:
                job = SessionResetState(coord).job(request)
                self.assertEqual(job['status'], 'parked', job['error'])
            workers.append((request, label, entry['target_session'], job['new_thread_id']))
        self.args['retirement_ids'] = [entry[0] for entry in workers]
        for index, (request, label, target, thread) in enumerate(workers):
            if index == 6:
                with self.assertRaisesRegex(CoordError, 'every exact bound-worker retirement'):
                    self.admin()
                self.assertEqual(self.start_count(), 0)
            self.root('retire-bound-parked', project=self.project_id, bootstrap_id=self.bootstrap['request_id'],
                stop_id=self.bootstrap['parked_stop'], po=self.po['target_session'], expected_thread=self.po_thread,
                expected_host='mac', request_id=request, target=target, expected_agent=label,
                logical_owner='fixture-owner', expected_bound_thread=thread, repair_request_id=str(uuid.uuid4()),
                expected_old_rollout=str(Path(self.workspace) / ('rollout-' + thread + '.jsonl')),
                expected_candidate='none', confirm=True)
        with self.assertRaisesRegex(CoordError, 'every exact bound-worker retirement'):
            self.admin(retirement_ids=self.args['retirement_ids'][:-1] + [self.attempts[0][0]])
        receipt = self.admin()
        self.assertEqual(receipt['status'], 'started')
        self.assertEqual(self.admin(), receipt)
        self.assertEqual(self.start_count(), 1)
        self.assert_stop_active()
