import unittest
from agent_chat.bridge import Bridge
from agent_chat.rpc import TransportError
from agent_chat.native_stage_observer import StageWindows

class State:
    def __init__(self):
        self.row = {'id':'private-job','thread_id':'private-thread','status':'queued','queue_id':'private-queue','payload':'private input'}
    def job(self, _): return dict(self.row)
    def update(self, _, status, **values): self.row.update(status=status, **values)

class FakeRpc:
    def __init__(self, lose_response):
        self.lose_response = lose_response
        self.calls = []
        self.notifications = []
        self.started = False
        self.timeout = 1
        self.epoch = 1
    def connection_identity(self): return {'client_instance':'private-c', 'connection_epoch':self.epoch}
    def notification_boundary(self, **kwargs): return 0
    def notification_snapshot_since(self, boundary): return self.notifications_since(boundary), self.notification_observations_since(boundary)
    def notifications_since(self, boundary): return [(i, event) for i, event in enumerate(self.notifications, 1) if i > boundary]
    def notification_observations_since(self, boundary): return [{'sequence':i, 'connection_instance':'private-c', 'connection_epoch':self.epoch, 'observed_at':100+i, 'observed_monotonic_ns':1000+i} for i, event in enumerate(self.notifications, 1) if i > boundary]
    def request(self, method, params):
        self.calls.append((method, dict(params)))
        if method == 'thread/queue/start':
            self.started = True
            self.notifications.extend([
                {'method':'item/started','params':{'threadId':'private-thread','turnId':'private-turn','item':{'type':'userMessage','clientId':'private-job'}}},
                {'method':'item/agentMessage/delta','params':{'threadId':'private-thread','turnId':'private-turn','itemId':'private-item','delta':'hello'}}])
            if self.lose_response: raise TransportError('private lost response')
            return {'turn':{'id':'private-turn'}}
        raise AssertionError('unexpected native method: ' + method)

class CorrelationGap(unittest.TestCase):
    def exercise(self, lost):
        state, rpc = State(), FakeRpc(lost)
        bridge = Bridge.__new__(Bridge)
        bridge.state, bridge.rpc, bridge._in_tick = state, rpc, False
        bridge.native_stage_windows = StageWindows(rpc)
        bridge._history_pages = {}
        bridge.queue = lambda _: [] if rpc.started else [{'id':'private-queue','clientUserMessageId':'private-job'}]
        bridge.still_unread = lambda _: True
        bridge.idle = lambda _: True
        if lost:
            with self.assertRaises(TransportError): bridge.advance(state.job('private-job'))
            self.assertEqual(state.row['status'], 'uncertain')
            bridge.history_contains = lambda job: job['id'] == 'private-job'
            bridge.reconcile(state.job('private-job'))
        else:
            bridge.advance(state.job('private-job'))
        self.assertEqual(state.row['status'], 'dispatched')
        self.assertEqual([m for m, _ in rpc.calls], ['thread/queue/start'])
        item, delta = rpc.notifications
        self.assertEqual((item['params']['item']['clientId'], item['params']['turnId']), ('private-job','private-turn'))
        self.assertEqual(delta['params']['turnId'], 'private-turn')
        observation = bridge.native_stage_windows.windows['private-job']['result']
        self.assertEqual((observation['status'], observation['turn_id'], observation['observed_monotonic_ns'], observation['coverage']), ('observed','private-turn',1002,'unverified'))
    def test_before_start_reply(self): self.exercise(False)
    def test_lost_start_reply(self): self.exercise(True)

    def test_reconnect_after_observed_is_censored(self):
        rpc = FakeRpc(False)
        windows = StageWindows(rpc)
        job = {'id':'private-job', 'thread_id':'private-thread'}
        windows.arm(job)
        rpc.request('thread/queue/start', {'threadId':'private-thread','queuedSubmissionId':'private-queue'})
        windows.poll()
        self.assertEqual(windows.windows['private-job']['result']['status'], 'observed')
        rpc.epoch += 1
        windows.poll()
        self.assertEqual(windows.windows['private-job']['result']['reason'], 'window_unavailable')
    def test_missing_metadata_and_conflicting_start_are_unknown(self):
        rpc = FakeRpc(False)
        windows = StageWindows(rpc)
        job = {'id':'private-job', 'thread_id':'private-thread'}
        windows.arm(job)
        rpc.request('thread/queue/start', {'threadId':'private-thread','queuedSubmissionId':'private-queue'})
        windows.started('private-job', {'turn':{'id':'other-turn'}})
        windows.poll()
        self.assertEqual(windows.windows['private-job']['result']['reason'], 'turn_mismatch')
        rpc.notifications.append({'method':'fixture','params':{}})
        rpc.notification_observations_since = lambda _: []
        windows.poll()
        self.assertEqual(windows.windows['private-job']['result']['reason'], 'window_unavailable')
