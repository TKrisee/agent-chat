from pathlib import Path

from agent_chat.reset_worker import ResetWorker
from agent_chat.rpc import TransportError
from test_parked_provision import ParkedProvisionTests


class MaterializationFlowTests(ParkedProvisionTests):
    def test_unknown_metadata_without_rollout_stays_unbound_without_replay(self):
        self.create()
        self.coord.db.execute('INSERT INTO root_parked_activations VALUES(?)', (self.request,))
        original = self.rpc.request
        calls = []
        def request(method, params):
            if method == 'thread/name/set':
                calls.append(method)
                raise TransportError('unknown name outcome')
            return original(method, params)
        self.rpc.request = request
        worker = ResetWorker(self.bridge)
        with self.assertRaises(TransportError):
            worker.advance(self.resets.job(self.request))
        worker.advance(self.resets.job(self.request))
        worker.advance(self.resets.job(self.request))
        job = self.resets.job(self.request)
        self.assertEqual(job['status'], 'materializing')
        self.assertEqual(calls, ['thread/name/set'])
        self.assertIsNone(self.coord.db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?', (job['target_session'],)).fetchone())

    def test_lost_name_response_reconciles_without_replay(self):
        self.create()
        self.coord.db.execute('INSERT INTO root_parked_activations VALUES(?)', (self.request,))
        original = self.rpc.request
        path = Path(self.workspace) / 'durable-rollout'
        name = {}
        calls = []

        def request(method, params):
            if method == 'thread/name/set':
                calls.append(method)
                name['value'] = params['name']
                path.write_bytes(b'metadata')
                raise TransportError('lost name response')
            result = original(method, params)
            if method == 'thread/read' and params.get('threadId') == self.rpc.new and name:
                result['thread'].update(name=name['value'], path=str(path))
            return result

        self.rpc.request = request
        worker = ResetWorker(self.bridge)
        with self.assertRaises(TransportError):
            worker.advance(self.resets.job(self.request))
        self.assertEqual(self.resets.job(self.request)['status'], 'materializing')
        self.assertIsNone(self.coord.db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?', (self.resets.job(self.request)['target_session'],)).fetchone())
        worker.advance(self.resets.job(self.request))
        self.assertEqual(self.resets.job(self.request)['status'], 'parked')
        self.assertEqual(calls, ['thread/name/set'])
        self.assertFalse([method for method, _ in self.rpc.calls if method in ('thread/queue/add','thread/queue/start')])
