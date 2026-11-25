import contextlib
import sqlite3
from unittest import mock

from agent_chat.core import Coordinator
from agent_chat.rpc import RpcError, TransportError
from agent_chat.session_reset import SessionResetState
from test_parked_provision_http import ParkedProvisionHttpTests


class ParkedFdReceiptTests(ParkedProvisionHttpTests):
    def test_provider_rpc_error_survives_reconciliation_without_replay(self):
        project, route, path, params, state, bridge = self.prepare_parked()
        created = self.root('direct-create', **params)['creation']
        original = self.rpc.request
        message = 'thread writer lock startup failed: Too many open files (os error 24)'
        def request(method, values):
            if method == 'model/list':
                return {'data':[{'model':'gpt-6.2-sol','supportedReasoningEfforts':[{'reasoningEffort':'high'}]}]}
            if method == 'thread/start':
                self.rpc.calls.append((method,dict(values)))
                raise RpcError(-32000, message)
            return original(method,values)
        with mock.patch.object(self.rpc,'request',side_effect=request):
            bridge.tick()
            bridge.tick()
        with contextlib.closing(Coordinator(path,route['session'])) as coord:
            job = SessionResetState(coord).job(params['request_id'])
            self.assertEqual(job['status'],'uncertain')
            self.assertIsNone(job['new_thread_id'])
            self.assertIn('Lost thread creation result',job['error'])
            self.assertEqual(job['native_rpc_error']['code'],-32000)
            self.assertEqual(job['native_rpc_error']['message'],message)
            self.assertIsNone(coord.db.execute('SELECT 1 FROM bridge_bindings WHERE session_id=?',(created['target_session'],)).fetchone())
            self.assertEqual(coord.db.execute('SELECT active FROM root_team_stops WHERE id=?',(params['parked_stop'],)).fetchone()['active'],1)
            with self.assertRaises(sqlite3.IntegrityError):
                coord.db.execute('UPDATE parked_native_rpc_errors SET message=? WHERE request_id=?',('changed',params['request_id']))
            with self.assertRaises(sqlite3.IntegrityError):
                coord.db.execute('DELETE FROM parked_native_rpc_errors WHERE request_id=?',(params['request_id'],))
        status = self.routed(project,'session-reset-status',to=created['target_session'])
        self.assertEqual(status['resets'][0]['native_rpc_error']['message'],message)
        self.assertEqual([m for m,_ in self.rpc.calls].count('thread/start'),1)
        self.assertFalse([m for m,_ in self.rpc.calls if m in ('thread/queue/add','thread/queue/start')])

    def test_invalid_parameters_keep_existing_definite_rejection_and_receipt(self):
        project, route, path, params, state, bridge = self.prepare_parked()
        self.root('direct-create', **params)
        original = self.rpc.request
        def request(method, values):
            if method == 'model/list':
                return {'data':[{'model':'gpt-6.2-sol','supportedReasoningEfforts':[{'reasoningEffort':'high'}]}]}
            if method == 'thread/start':
                self.rpc.calls.append((method,dict(values)))
                raise RpcError(-32602, 'unsupported provider parameters')
            return original(method,values)
        with mock.patch.object(self.rpc,'request',side_effect=request): bridge.tick()
        with contextlib.closing(Coordinator(path,route['session'])) as coord:
            job = SessionResetState(coord).job(params['request_id'])
            self.assertEqual(job['status'],'failed')
            self.assertEqual(job['native_rpc_error']['code'],-32602)
            self.assertEqual(job['native_rpc_error']['message'],'unsupported provider parameters')
            self.assertEqual(coord.db.execute('SELECT code FROM parked_native_rejections WHERE request_id=?',(params['request_id'],)).fetchone()[0],-32602)
        self.assertFalse([m for m,_ in self.rpc.calls if m in ('thread/queue/add','thread/queue/start')])

    def test_transport_loss_never_becomes_a_provider_rpc_receipt(self):
        project, route, path, params, state, bridge = self.prepare_parked()
        self.root('direct-create', **params)
        original = self.rpc.request
        def request(method, values):
            if method == 'model/list':
                return {'data':[{'model':'gpt-6.2-sol','supportedReasoningEfforts':[{'reasoningEffort':'high'}]}]}
            result = original(method,values)
            if method == 'thread/start': raise TransportError('lost response after native acceptance')
            return result
        with mock.patch.object(self.rpc,'request',side_effect=request):
            with self.assertRaises(TransportError): bridge.tick()
        with contextlib.closing(Coordinator(path,route['session'])) as coord:
            job = SessionResetState(coord).job(params['request_id'])
            self.assertEqual(job['status'],'uncertain')
            self.assertIsNone(job['native_rpc_error'])
        self.assertEqual([m for m,_ in self.rpc.calls].count('thread/start'),1)
