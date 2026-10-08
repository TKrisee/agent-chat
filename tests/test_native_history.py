"""Legacy native history must supply proof, never an invented empty result."""
import unittest
from types import SimpleNamespace

from agent_chat.bridge import Bridge
from agent_chat.core import CoordError
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
            return {}
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
        bridge = Bridge(SimpleNamespace(observe=lambda *args: None), self.rpc)
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
