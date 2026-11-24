"""Authenticated PO update reaches the exact busy native turn once."""
import contextlib
import unittest
from unittest import mock

import test_creator_management as fixture
from agent_chat import core


class BusyWorkerDelivery(fixture.CreatorManagementTests):
    def test_authenticated_update_steers_original_busy_turn_once(self):
        created, thread, _ = self.create_worker()
        worker = created['target_session']
        project_root = self.call('route', project=self.project_id)['session']
        source = self.po_call('send', session=project_root, to=self.po, body='Own this fixture objective')['id']
        self.po_call('context')
        goal = self.po_call('activity-adopt', source_request_id=source, title='Fixture goal')
        assignment = self.po_call('send', to=worker, body='Perform the assigned fixture task')['id']
        task = self.po_call('activity-assign', goal_id=goal['id'], worker=worker,
                            source_request_id=assignment, title='Fixture task')
        self.project_bridge_instance.tick(observe_policies=False)
        with contextlib.closing(core.Coordinator(self.server.projects.db_path(self.project_id))) as coord:
            original = coord.db.execute('SELECT job_id FROM bridge_deliveries WHERE message_id=?', (assignment,)).fetchone()
            self.assertIsNotNone(original)
            original = original['job_id']
        self.assertEqual(self.rpc.history[thread][-1]['clientId'], original)
        self.po_call('activity-transition', session=worker, work_id=task['id'], state='in_progress')
        self.po_call('context')
        update = self.po_call('send', to=worker, body='Apply the new fixture guidance', update_for=task['id'])['id']
        prior_request = self.rpc.request
        turn_id = 'busy-fixture-turn'
        steers = []
        def native(method, params, **kwargs):
            if params.get('threadId') == thread:
                if method == 'thread/read':
                    return {'thread': {'id':thread, 'status':{'type':'active','activeFlags':[]}, 'canAcceptDirectInput':True}}
                if method == 'thread/turns/list':
                    return {'data':[{'id':turn_id, 'status':'inProgress', 'items':[
                        {'type':'userMessage','clientId':original},
                        {'type':'dynamicToolCall','id':'held-fixture-tool'},
                    ]}], 'nextCursor':None}
                if method == 'turn/steer':
                    steers.append(dict(params))
                    return {'turnId':turn_id}
            return prior_request(method, params, **kwargs)
        with mock.patch.object(self.rpc, 'request', side_effect=native):
            self.project_bridge_instance.tick(observe_policies=False)
            self.project_bridge_instance.tick(observe_policies=False)
        self.assertEqual(len(steers), 1)
        self.assertEqual(steers[0]['expectedTurnId'], turn_id)
        self.assertEqual(steers[0]['threadId'], thread)
        self.assertIn('Apply the new fixture guidance', steers[0]['input'][0]['text'])
        with contextlib.closing(core.Coordinator(self.server.projects.db_path(self.project_id))) as coord:
            claim = coord.db.execute('SELECT job_id,original_job_id,turn_id FROM manager_update_claims WHERE message_id=?', (update,)).fetchone()
            self.assertIsNotNone(claim)
            self.assertEqual((claim['original_job_id'],claim['turn_id']), (original,turn_id))
            self.assertEqual(steers[0]['clientUserMessageId'], claim['job_id'])
            self.assertEqual(coord.db.execute('SELECT status FROM bridge_jobs WHERE id=?',(claim['job_id'],)).fetchone()['status'], 'dispatched')
            self.assertIsNone(coord.db.execute('SELECT 1 FROM message_reads WHERE message_id=? AND reader_session=?',(update,worker)).fetchone())
            self.assertIsNone(coord.db.execute('SELECT acked_at FROM messages WHERE id=?',(update,)).fetchone()['acked_at'])


def load_tests(loader, tests, pattern):
    return unittest.TestSuite([BusyWorkerDelivery('test_authenticated_update_steers_original_busy_turn_once')])
