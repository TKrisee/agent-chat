"""Exercise real RpcClient buffer contracts without sockets or model input."""
import threading
import unittest
from agent_chat.native_stage_observer import StageWindows
from agent_chat.rpc import RpcClient, TransportError

class AtomicSnapshotTests(unittest.TestCase):
    def setUp(self):
        self.rpc = RpcClient('ws://127.0.0.1:1')
        self.rpc._epoch = 1
        self.rpc.connection_identity = lambda: {'client_instance': self.rpc._connection_instance, 'connection_epoch': self.rpc._epoch}
        self.rpc.notification_boundary = lambda **_: 0
        self.windows = StageWindows(self.rpc)
        self.windows.arm({'id': 'job', 'thread_id': 'thread'})

    def record(self, method, **params):
        with self.rpc._state:
            self.rpc._record_notification({'method': method, 'params': params}, 100.0, 1000)

    def test_arrival_after_atomic_read_is_seen_on_next_poll(self):
        self.record('item/started', threadId='thread', turnId='turn', item={'type':'userMessage','clientId':'job'})
        snapshot = self.rpc.notification_snapshot_since
        def snapshot_then_arrival(cursor):
            result = snapshot(cursor)
            self.record('item/agentMessage/delta', threadId='thread', turnId='turn', itemId='item', delta='answer')
            self.rpc.notification_snapshot_since = snapshot
            return result
        self.rpc.notification_snapshot_since = snapshot_then_arrival
        self.windows.poll()
        self.assertIsNone(self.windows.windows['job'].get('gap'))
        self.windows.poll()
        self.assertEqual(self.windows.windows['job']['result']['status'], 'observed')
        self.assertEqual(self.windows.windows['job']['result']['attribution'], 'job_turn_only')

    def test_missing_receipt_and_epoch_change_censor(self):
        self.record('fixture')
        with self.rpc._state: self.rpc._notification_observations.clear()
        self.windows.poll()
        self.assertEqual(self.windows.windows['job']['result']['reason'], 'window_unavailable')
        self.windows = StageWindows(self.rpc); self.windows.arm({'id':'other','thread_id':'thread'})
        self.rpc._epoch += 1
        self.windows.poll()
        self.assertEqual(self.windows.windows['other']['result']['reason'], 'window_unavailable')

    def test_floor_rejects_expired_window_and_old_apis_remain(self):
        with self.rpc._state:
            for _ in range(1025): self.rpc._record_notification({'method':'fixture'},100.0,1000)
        for method in (self.rpc.notification_snapshot_since, self.rpc.notifications_since, self.rpc.notification_observations_since):
            with self.assertRaises(TransportError): method(0)
        raw, stamps = self.rpc.notification_snapshot_since(1)
        self.assertEqual(len(raw), 1024)
        self.assertEqual([seq for seq,_ in raw], [stamp['sequence'] for stamp in stamps])

class BaselineRaceRegression(unittest.TestCase):
    def test_notification_arriving_between_legacy_reads_is_not_a_gap(self):
        rpc = RpcClient('ws://127.0.0.1:1')
        rpc._epoch = 1
        rpc.connection_identity = lambda: {'client_instance':rpc._connection_instance, 'connection_epoch':rpc._epoch}
        rpc.notification_boundary = lambda **_: 0
        windows = StageWindows(rpc)
        windows.arm({'id':'job','thread_id':'thread'})
        with rpc._state:
            rpc._record_notification({'method':'item/started','params':{'threadId':'thread','turnId':'turn','item':{'type':'userMessage','clientId':'job'}}},100.0,1000)
        def deliver_delta():
            with rpc._state:
                rpc._record_notification({'method':'item/agentMessage/delta','params':{'threadId':'thread','turnId':'turn','itemId':'item','delta':'answer'}},101.0,1001)
        # The event arrives immediately after the reader's first snapshot in
        # either implementation. Both wrappers call the real RpcClient method.
        if hasattr(rpc, 'notification_snapshot_since'):
            real = rpc.notification_snapshot_since
            def snapshot(cursor):
                result = real(cursor)
                deliver_delta()
                rpc.notification_snapshot_since = real
                return result
            rpc.notification_snapshot_since = snapshot
        else:
            real = rpc.notifications_since
            def legacy_read(cursor):
                result = real(cursor)
                deliver_delta()
                rpc.notifications_since = real
                return result
            rpc.notifications_since = legacy_read
        windows.poll()
        self.assertIsNone(windows.windows['job'].get('gap'))
        windows.poll()
        self.assertEqual(windows.windows['job']['result']['status'], 'observed')
        self.assertEqual(windows.windows['job']['result']['observed_monotonic_ns'], 1001)
