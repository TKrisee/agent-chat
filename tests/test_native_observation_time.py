import socket
import struct
import threading
import time
import unittest
from agent_chat.rpc import RpcClient


def send_frame(sock, payload):
    header = bytes([0x81, len(payload)]) if len(payload) < 126 else bytes([0x81, 126]) + struct.pack('!H', len(payload))
    sock.sendall(header + payload)


class ReaderObservationTime(unittest.TestCase):
    def test_buffered_delta_keeps_reader_time_before_start_returns(self):
        left, peer = socket.socketpair()
        left.setblocking(False)
        peer.settimeout(2)
        client = RpcClient('ws://127.0.0.1:1', timeout=2)
        client._sock = left
        client._ensure_reader()
        result = {}
        def call():
            try:
                result['value'] = client.request('thread/queue/start', {'threadId': 'private-thread', 'queuedSubmissionId': 'private-job'}, deadline=time.monotonic()+2)
            except Exception as exc:
                result['error'] = exc
        worker = threading.Thread(target=call)
        try:
            worker.start()
            self.assertTrue(peer.recv(4096))
            before = time.monotonic_ns()
            send_frame(peer, b'{"method":"item/agentMessage/delta","params":{"threadId":"private-thread","turnId":"private-turn","itemId":"private-item","delta":"hello"}}')
            sequence, notice = client.next_notification(0, deadline=time.monotonic()+1)
            after = time.monotonic_ns()
            self.assertEqual(notice['params']['turnId'], 'private-turn')
            self.assertTrue(worker.is_alive(), 'notification arrived before queue/start response')
            send_frame(peer, b'{"id":1,"result":{"turn":{"id":"private-turn"}}}')
            worker.join(1)
            self.assertFalse(worker.is_alive())
            self.assertNotIn('error', result)
            self.assertEqual(result['value']['turn']['id'], 'private-turn')
            lookup = getattr(client, 'notification_observations_since', None)
            self.assertTrue(callable(lookup), 'no reader-stamped notification metadata accessor')
            observed_ns = lookup(sequence - 1)[0]['observed_monotonic_ns']
            self.assertLessEqual(before, observed_ns)
            self.assertLessEqual(observed_ns, after)
        finally:
            client.close()
            peer.close()
            worker.join(2)

if __name__ == '__main__':
    unittest.main()

class MetadataCompatibility(unittest.TestCase):
    def test_raw_api_and_metadata_share_sequence_and_overflow_floor(self):
        client = RpcClient('ws://127.0.0.1:1')
        for number in range(1025):
            client._record_notification({'method': 'fixture', 'params': {'number': number}}, 1000.0 + number, 2000 + number)
        raw = client.notifications_since(1)
        metadata = client.notification_observations_since(1)
        self.assertEqual(len(raw), 1024)
        self.assertEqual([row[0] for row in raw], [row['sequence'] for row in metadata])
        self.assertEqual(metadata[0]['observed_at'], 1001.0)
        self.assertEqual(metadata[0]['observed_monotonic_ns'], 2001)
        self.assertEqual(raw[0][1]['params']['number'], 1)
        from agent_chat.rpc import TransportError
        for query in (client.notifications_since, client.notification_observations_since):
            with self.assertRaises(TransportError):
                query(0)
        metadata[0]['observed_at'] = 0
        self.assertEqual(client.notification_observations_since(1)[0]['observed_at'], 1001.0)

    def test_reconnect_invalidates_both_windows(self):
        from unittest import mock
        from agent_chat.rpc import TransportError
        client = RpcClient('ws://127.0.0.1:1')
        client._record_notification({'method': 'before'}, 1.0, 2)
        fake = mock.Mock()
        addresses = [(socket.AF_INET, socket.SOCK_STREAM, 0, '', ('127.0.0.1', 1))]
        with mock.patch('agent_chat.rpc.socket.getaddrinfo', return_value=addresses), \
             mock.patch('agent_chat.rpc.socket.socket', return_value=fake), \
             mock.patch.object(client, '_handshake'), mock.patch.object(client, '_ensure_reader'), \
             mock.patch.object(client, '_initialize'):
            client.connect()
        for query in (client.notifications_since, client.notification_observations_since):
            with self.assertRaises(TransportError):
                query(0)
            self.assertEqual(query(1), [])
        self.assertEqual(len(client._notification_observations), 0)
