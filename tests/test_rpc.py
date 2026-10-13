import base64
import errno
import hashlib
import json
import select
import socket
import struct
import threading
import time
import unittest
from unittest import mock

from agent_chat.rpc import RpcClient, RpcError, TransportError


def _exact(sock, n):
    data = bytearray()
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise EOFError
        data.extend(chunk)
    return bytes(data)


def _frame(sock):
    first, second = _exact(sock, 2)
    length = second & 127
    if length == 126:
        length = struct.unpack("!H", _exact(sock, 2))[0]
    elif length == 127:
        length = struct.unpack("!Q", _exact(sock, 8))[0]
    mask = _exact(sock, 4) if second & 128 else b""
    payload = _exact(sock, length)
    if mask:
        payload = bytes(v ^ mask[i % 4] for i, v in enumerate(payload))
    return first & 15, bool(first & 128), payload


def _send(sock, opcode, payload, fin=True, split=False):
    prefix = bytes([(128 if fin else 0) | opcode])
    if len(payload) < 126:
        wire = prefix + bytes([len(payload)]) + payload
    else:
        wire = prefix + bytes([126]) + struct.pack("!H", len(payload)) + payload
    if split:
        for byte in wire:
            sock.sendall(bytes([byte]))
    else:
        sock.sendall(wire)


class FakeServer:
    def __init__(self, handler, connections=1):
        self.listener = socket.socket()
        try:
            self.listener.bind(("127.0.0.1", 0))
        except PermissionError as exc:
            self.listener.close()
            if exc.errno == errno.EPERM:
                raise unittest.SkipTest("sandbox does not permit local socket binding") from exc
            raise
        self.listener.listen(1)
        self.endpoint = "ws://127.0.0.1:%d" % self.listener.getsockname()[1]
        self.failure = None
        self.thread = threading.Thread(target=self._run, args=(handler, connections), daemon=True)
        self.thread.start()

    def _run(self, handler, connections):
        try:
            for _ in range(connections):
                sock, _ = self.listener.accept()
                with sock:
                    request = bytearray()
                    while b"\r\n\r\n" not in request:
                        request.extend(sock.recv(1))
                    key = next(line.split(":", 1)[1].strip() for line in request.decode().split("\r\n") if line.lower().startswith("sec-websocket-key:"))
                    accept = base64.b64encode(hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()).decode()
                    sock.sendall(("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n" % accept).encode())
                    handler(sock)
        except Exception as exc:  # reported by join
            self.failure = exc
        finally:
            self.listener.close()

    def join(self):
        self.thread.join(2)
        if self.failure:
            raise self.failure


def _initialize(sock, result=None):
    opcode, _, body = _frame(sock)
    assert opcode == 1
    message = json.loads(body)
    assert message["method"] == "initialize"
    _send(sock, 1, json.dumps({"id": message["id"], "result": {} if result is None else result}).encode())
    assert json.loads(_frame(sock)[2])["method"] == "initialized"


class RpcClientTests(unittest.TestCase):
    def test_initializes_and_handles_partial_fragment_ping_and_notification(self):
        def handler(sock):
            _initialize(sock)
            request = json.loads(_frame(sock)[2])
            _send(sock, 9, b"hi")
            self.assertEqual(_frame(sock), (10, True, b"hi"))
            _send(sock, 1, b'{"method":"notice"}', fin=False)
            _send(sock, 0, b'')
            result = json.dumps({"id": request["id"], "result": {"ok": True}}).encode()
            _send(sock, 1, result[:6], fin=False, split=True)
            _send(sock, 0, result[6:], split=True)
        server = FakeServer(handler)
        with RpcClient(server.endpoint) as client:
            self.assertEqual(client.request("thing", {"x": 1}), {"ok": True})
        server.join()

    def test_server_request_is_rejected_and_rpc_errors_are_typed(self):
        def handler(sock):
            _initialize(sock)
            request = json.loads(_frame(sock)[2])
            _send(sock, 1, json.dumps({"id": 77, "method": "command/exec", "params": {}}).encode())
            rejected = json.loads(_frame(sock)[2])
            self.assertEqual(rejected["error"]["code"], -32601)
            _send(sock, 1, json.dumps({"id": request["id"], "error": {"code": 42, "message": "no", "data": {"a": 1}}}).encode())
        server = FakeServer(handler)
        with RpcClient(server.endpoint) as client:
            with self.assertRaises(RpcError) as raised:
                client.request("danger")
        self.assertEqual((raised.exception.code, raised.exception.data), (42, {"a": 1}))
        server.join()

    def test_raw_initialize_and_notifications_before_and_after_ack_are_retained(self):
        initialize = dict(userAgent='fixture-origin/0.161.0 (fixture-os) (agent_chat_bridge; 0.1.0)',
                          codexHome='/fixture/private', platformFamily='unix', platformOs='fixture',
                          unknown={'nested': [None, True]})
        before = dict(method='thread/settings/updated', params=dict(threadId='fixture-thread',
                      threadSettings={'model': 'gpt-6.1-sol', 'effort': 'high', 'unknown': [None]}))
        after = dict(method='thread/settings/updated', params=dict(threadId='other-thread',
                     threadSettings={'model': 'gpt-6-luna', 'effort': None}))

        def handler(sock):
            _initialize(sock, initialize)
            request = json.loads(_frame(sock)[2])
            _send(sock, 1, json.dumps(before).encode())
            _send(sock, 1, json.dumps({'id': request['id'], 'result': {}}).encode())
            _send(sock, 1, json.dumps(after).encode())
            _frame(sock)  # Client close; no listener remains after join.

        server = FakeServer(handler)
        with RpcClient(server.endpoint) as client:
            self.assertEqual(client.initialize_response, initialize)
            deadline = time.monotonic() + client.timeout
            boundary = client.notification_boundary(deadline=deadline)
            self.assertEqual(client.request('thread/settings/update', {}, deadline=deadline), {})
            first, value = client.next_notification(boundary, deadline=deadline)
            self.assertEqual(value, before)
            _, value = client.next_notification(first, deadline=deadline)
            self.assertEqual(value, after)
            self.assertEqual([notice for _, notice in client.notifications_since(boundary)], [before, after])
        server.join()

    def test_pre_dispatch_boundary_excludes_stale_buffered_wire_notification(self):
        sent = threading.Event()
        stale = dict(method='thread/settings/updated', params=dict(threadId='fixture-thread', threadSettings={'model': 'old'}))
        fresh = dict(method='thread/settings/updated', params=dict(threadId='fixture-thread', threadSettings={'model': 'new'}))

        def handler(sock):
            _initialize(sock)
            _send(sock, 1, json.dumps(stale).encode())
            sent.set()
            request = json.loads(_frame(sock)[2])
            _send(sock, 1, json.dumps({'id': request['id'], 'result': {}}).encode())
            _send(sock, 1, json.dumps(fresh).encode())
            _frame(sock)

        server = FakeServer(handler)
        with RpcClient(server.endpoint) as client:
            self.assertTrue(sent.wait(1))
            # sendall completion is not a receive boundary (e.g. TCP buffering).
            self.assertTrue(select.select([client._sock], [], [], 1)[0])
            deadline = time.monotonic() + client.timeout
            boundary = client.notification_boundary(deadline=deadline)
            self.assertEqual([value for _, value in client.notifications_since(0)], [stale])
            client.request('thread/settings/update', {}, deadline=deadline)
            _, notice = client.next_notification(boundary, deadline=deadline)
            self.assertEqual(notice, fresh)
        server.join()

    def test_ack_and_notification_wait_share_one_bounded_deadline(self):
        def handler(sock):
            _initialize(sock)
            request = json.loads(_frame(sock)[2])
            time.sleep(.10)
            _send(sock, 1, json.dumps({'id': request['id'], 'result': {}}).encode())
            time.sleep(.50)

        server = FakeServer(handler)
        with RpcClient(server.endpoint, timeout=.40) as client:
            started = time.monotonic()
            deadline = started + client.timeout
            boundary = client.notification_boundary(deadline=deadline)
            client.request('thread/settings/update', {}, deadline=deadline)
            with self.assertRaises(TransportError):
                client.next_notification(boundary, deadline=deadline)
            self.assertLess(time.monotonic() - started, .47)
        server.join()

    def test_disconnect_after_ack_and_lost_observation_window_are_uncertain(self):
        def handler(sock):
            _initialize(sock)
            request = json.loads(_frame(sock)[2])
            _send(sock, 1, json.dumps({'id': request['id'], 'result': {}}).encode())

        server = FakeServer(handler)
        with RpcClient(server.endpoint) as client:
            deadline = time.monotonic() + client.timeout
            boundary = client.notification_boundary(deadline=deadline)
            client.request('thread/settings/update', {}, deadline=deadline)
            with self.assertRaises(TransportError):
                client.next_notification(boundary, deadline=deadline)
        server.join()
        client = RpcClient('ws://127.0.0.1:1')
        for number in range(1025):
            client._record_notification(dict(method='notice', params={'number': number}))
        with self.assertRaisesRegex(TransportError, 'boundary'):
            client.notifications_since(0)

    def test_invalid_endpoints(self):
        for endpoint in ("http://127.0.0.1:9", "ws://example.com:9", "ws://127.0.0.1:9/a", "ws://127.0.0.1:9?x=1", "ws://user@127.0.0.1:9"):
            with self.assertRaises(ValueError):
                RpcClient(endpoint)
        with self.assertRaises(ValueError):
            RpcClient("ws://127.0.0.1:9", float("inf"))

    def test_failed_connect_closes_created_socket(self):
        class BrokenSocket:
            closed = False

            def settimeout(self, _):
                pass

            def connect(self, _):
                raise OSError("dropped")

            def sendall(self, _):
                raise OSError("not connected")

            def close(self):
                self.closed = True

        sock = BrokenSocket()
        with mock.patch("agent_chat.rpc.socket.getaddrinfo", return_value=[(socket.AF_INET, socket.SOCK_STREAM, 0, "", ("127.0.0.1", 9))]), \
                mock.patch("agent_chat.rpc.socket.socket", return_value=sock):
            with self.assertRaises(TransportError):
                RpcClient("ws://127.0.0.1:9").connect()
        self.assertTrue(sock.closed)

    def test_message_limit(self):

        def handler(sock):
            _initialize(sock)
            _frame(sock)
            # A declared over-limit frame must fail before its payload is read.
            sock.sendall(b"\x81\x41")
        server = FakeServer(handler)
        with RpcClient(server.endpoint) as client:
            import agent_chat.rpc as rpc
            original = rpc._MAX_MESSAGE
            rpc._MAX_MESSAGE = 64
            try:
                with self.assertRaises(TransportError):
                    client.request("large")
            finally:
                rpc._MAX_MESSAGE = original
        server.join()

    def test_notification_does_not_extend_request_deadline(self):
        def handler(sock):
            _initialize(sock)
            _frame(sock)
            _send(sock, 1, b'{"method":"notice"}')
            time.sleep(0.15)
        server = FakeServer(handler)
        with RpcClient(server.endpoint, timeout=0.05) as client:
            started = time.monotonic()
            with self.assertRaises(TransportError):
                client.request("wait")
            self.assertLess(time.monotonic() - started, 0.12)
        server.join()

    def test_rsv_server_frame_is_rejected(self):
        def handler(sock):
            _initialize(sock)
            _frame(sock)
            sock.sendall(b"\xc1\x00")
        server = FakeServer(handler)
        with RpcClient(server.endpoint) as client:
            with self.assertRaises(TransportError):
                client.request("bad-frame")
        server.join()

    def test_disconnect_becomes_transport_error(self):
        def handler(sock):
            _initialize(sock)
            _frame(sock)
        server = FakeServer(handler)
        with RpcClient(server.endpoint) as client:
            with self.assertRaises(TransportError):
                client.request("gone")
        server.join()


if __name__ == "__main__":
    unittest.main()
