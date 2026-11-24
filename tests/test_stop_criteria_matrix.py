import unittest
import uuid
from unittest import mock
from agent_chat.control_worker import ControlWorker
from agent_chat.core import CoordError
from agent_chat import root_local
from agent_chat.rpc import TransportError
from test_root_orchestrator import RootOrchestratorTests

class StopMatrix(RootOrchestratorTests):
    def test_partial_project_failure_exposes_incomplete_counts_and_same_request_reconciliation(self):
        project = self.project('Partial matrix')
        self.root('route', project=project)
        with self.server.projects.connection() as db:
            self.assertEqual(db.execute('SELECT joined FROM root_projects WHERE project_id=?', (project,)).fetchone()['joined'], 1)
        original = root_local._target
        def path(master, registry, selected, uid):
            if selected == project: raise CoordError('fixture project unavailable')
            return original(master, registry, selected, uid)
        request = str(uuid.uuid4())
        with mock.patch.object(root_local, '_target', path):
            partial = self.root('stop-all', request_id=request, confirm=True)
        self.assertFalse(partial['barrier_complete'])
        self.assertFalse(partial['target_counts_complete'])
        self.assertFalse(partial['process_closure_proven'])
        self.assertEqual([item['project'] for item in partial['errors']], [project])
        recovered = self.root('stop-all', request_id=request, confirm=True)
        self.assertTrue(recovered['barrier_complete'])
        self.assertTrue(recovered['target_counts_complete'])
        self.assertFalse(recovered['process_closure_proven'])

    def test_queue_direct_and_transport_prerequisites_are_distinct(self):
        threads = {}
        for name in ('queue-target','direct-target','timeout-target','idle-target'):
            self.call('register', session=name, agent=name)
            threads[name] = str(uuid.uuid4())
            self.call('bind', session=name, thread=threads[name])
            self.rpc.status[threads[name]], self.rpc.queues[threads[name]], self.rpc.history[threads[name]] = 'idle', [], []
        self.rpc.queues[threads['queue-target']] = [dict(id='retained-queue', clientUserMessageId='old-input', input=[])]
        request = str(uuid.uuid4())
        stopped = self.root('stop-all', request_id=request, confirm=True)
        self.assertTrue(stopped['barrier_complete'])
        self.assertFalse(stopped['process_closure_proven'])
        original = self.rpc.request
        def observed(method, params):
            target = params.get('threadId')
            if method == 'thread/read' and target == threads['timeout-target']:
                raise TransportError('fixture transport timeout')
            value = original(method, params)
            if method == 'thread/read' and target == threads['direct-target']:
                value['thread']['canAcceptDirectInput'] = False
            return value
        self.rpc.request = observed
        for job in self.state.control_jobs():
            if job.get('target_session') in threads:
                try: ControlWorker(self.bridge).advance(job)
                except TransportError: pass
        result = self.root('stop-status', request_id=request)
        targets = {item['target_session']:item for project in result['projects'] for item in project['targets']}
        self.assertEqual(targets['idle-target']['status'], 'completed')
        for name in ('queue-target','direct-target','timeout-target'):
            self.assertEqual(targets[name]['status'], 'waiting')
        self.assertEqual(self.rpc.queues[threads['queue-target']][0]['id'], 'retained-queue')
        self.assertIn('timeout', targets['timeout-target']['error'])
        self.assertNotEqual(targets['queue-target']['error'], targets['direct-target']['error'],
                            'queue and direct-input blockers must be independently identifiable')
        self.assertIn('native queue count=1', targets['queue-target']['error'])
        self.assertIn('direct input=not confirmed', targets['direct-target']['error'])
        counts = result['target_counts']
        self.assertEqual(sum(counts[key] for key in ('pending','waiting','interrupting','completed','stopped','other')), counts['total'])
        self.assertGreaterEqual(counts['waiting'], 3)
        self.assertTrue(result['target_counts_complete'])
        # Simulate independent provider admission becoming available; no queue deletion RPC is issued.
        self.rpc.request = original
        self.rpc.queues[threads['queue-target']] = []
        for job in self.state.control_jobs():
            if job.get('target_session') in threads:
                ControlWorker(self.bridge).advance(job)
        after = self.root('stop-status', request_id=request)
        targets = {item['target_session']:item for project in after['projects'] for item in project['targets']}
        self.assertTrue(all(targets[name]['status'] == 'completed' for name in threads))
        self.assertFalse(after['process_closure_proven'])
        self.assertFalse(any(method in ('thread/queue/remove','thread/queue/delete') for method,_ in self.rpc.calls))

def load_tests(loader, tests, pattern):
    return unittest.TestSuite(StopMatrix(name) for name in StopMatrix.__dict__ if name.startswith('test_'))
