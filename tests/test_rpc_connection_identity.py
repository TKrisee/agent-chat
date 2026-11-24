import os
import socket
import unittest
from agent_chat.rpc import RpcClient, TransportError

class LiveReader:
    def is_alive(self): return True

class ConnectionIdentityTests(unittest.TestCase):
    def setUp(self):
        self.listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.listener.bind(('127.0.0.1', 0)); self.listener.listen(2)
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.connect(self.listener.getsockname())
        self.server_socket, _ = self.listener.accept()
        self.addCleanup(self.listener.close); self.addCleanup(self.client_socket.close); self.addCleanup(self.server_socket.close)
        self.client = RpcClient('ws://127.0.0.1:1')
        self.client._sock = self.client_socket
        self.client._reader_sock = self.client_socket
        self.client._reader = LiveReader()
        self.client._epoch = 1

    def test_actual_connected_socket_identity_is_stable(self):
        first = self.client.connection_identity(); second = self.client.connection_identity()
        self.assertEqual(first, second)
        self.assertEqual(first['pid'], os.getpid())
        self.assertEqual(first['fd'], self.client_socket.fileno())
        self.assertEqual(first['local'], self.client_socket.getsockname())
        self.assertEqual(first['peer'], self.client_socket.getpeername())

    def test_request_uses_the_observed_live_socket(self):
        before = self.client.connection_identity()
        def deliver(message, deadline, sock):
            self.assertIs(sock, self.client_socket)
            waiter = self.client._waiters[message['id']]
            waiter['response'] = {'result': 'ok'}
            waiter['event'].set()
        self.client._send_json = deliver
        self.assertEqual(self.client.request('test/read'), 'ok')
        self.assertEqual(before, self.client.connection_identity())

    def test_reader_mismatch_or_closed_socket_is_refused(self):
        self.client._reader_sock = None
        with self.assertRaises(TransportError): self.client.connection_identity()
        self.client._reader_sock = self.client_socket
        self.client_socket.close()
        with self.assertRaises(TransportError): self.client.connection_identity()

    def test_connection_epoch_and_client_instance_distinguish_replacement(self):
        first = self.client.connection_identity()
        self.client._epoch += 1
        second = self.client.connection_identity()
        self.assertNotEqual(first, second)
        other = RpcClient('ws://127.0.0.1:1')
        other._sock, other._reader_sock, other._reader, other._epoch = self.client_socket, self.client_socket, LiveReader(), 1
        self.assertNotEqual(first['client_instance'], other.connection_identity()['client_instance'])
