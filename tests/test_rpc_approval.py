import importlib.util
import base64
import hashlib
from pathlib import Path
import socket
import select
import threading
import time
import unittest
from types import SimpleNamespace

from agent_chat import rpc
COMMAND = 'item/commandExecution/requestApproval'
FILE = 'item/fileChange/requestApproval'


def send(peer, payload):
    size = len(payload)
    header = bytes((0x81, size)) if size < 126 else b'\x81\x7e' + size.to_bytes(2, 'big')
    peer.sendall(header + payload)


def read_exact(peer, size):
    result = bytearray()
    while len(result) < size:
        chunk = peer.recv(size - len(result))
        if not chunk:
            raise EOFError('socket closed')
        result.extend(chunk)
    return bytes(result)


def frame(peer):
    header = read_exact(peer, 2)
    size = header[1] & 127
    if size == 126:
        size = int.from_bytes(read_exact(peer, 2), 'big')
    elif size == 127:
        size = int.from_bytes(read_exact(peer, 8), 'big')
    mask = read_exact(peer, 4) if header[1] & 128 else None
    payload = read_exact(peer, size)
    if mask:
        payload = bytes(value ^ mask[index % 4] for index, value in enumerate(payload))
    return header[0] & 15, payload


class ProviderRequests(unittest.TestCase):
    def setUp(self):
        self.peer = None
        self.client = None
        self.workers = []
        self.connect()
    def connect(self, *, methods=(COMMAND, FILE), pending_timeout=1.0):
        left, self.peer = socket.socketpair()
        left.setblocking(False)
        self.peer.settimeout(3)
        self.client = rpc.RpcClient('ws://127.0.0.1:1', timeout=2,
                                   server_request_methods=methods, server_request_timeout=pending_timeout)
        self.client._sock = left
        self.client._ensure_reader()
    def tearDown(self):
        self.client.close()
        self.peer.close()
        for thread in self.workers:
            thread.join(3)
            self.assertFalse(thread.is_alive())
        self.assertFalse(self.client._reader.is_alive())
    def start_call(self, method, timeout=2):
        result = {}
        def call():
            try:
                result['value'] = self.client.request(method, deadline=time.monotonic() + timeout)
            except Exception as error:
                result['error'] = error
        thread = threading.Thread(target=call)
        self.workers.append(thread)
        thread.start()
        return thread, result
    def deadline_response_race(self, boundary):
        # Gate the caller after its real socket write, before Event.wait. This
        # forces an already-signalled waiter to race a consumed deadline.
        deadline = time.monotonic() + 0.15
        original_send = self.client._send_json
        original_receive = self.client._receive_json
        original_time = rpc.time
        reader_at_boundary = threading.Event()
        result = {}

        def clock():
            if (boundary == 'equal' and reader_at_boundary.is_set()
                    and threading.current_thread() is self.client._reader):
                reader_at_boundary.clear()
                return deadline  # Exact equality without scheduler rounding.
            return time.monotonic()

        def receive(*args):
            message = original_receive(*args)
            if message is not None and message.get('id') == 1 and 'result' in message:
                if boundary == 'after':
                    time.sleep(max(0, deadline + 0.02 - time.monotonic()))
                    self.assertGreaterEqual(time.monotonic(), deadline)
                elif boundary == 'equal':
                    reader_at_boundary.set()
            return message

        def write(message, *args):
            original_send(message, *args)
            if message.get('method') == 'deadline-race':
                with self.client._state:
                    event = self.client._waiters[message['id']]['event']
                if not event.wait(2):
                    raise AssertionError('reader did not settle the real response')
                # Timely replies remain valid even if the caller is scheduled
                # after the deadline; only the reader's acceptance time counts.
                time.sleep(max(0, deadline + 0.03 - time.monotonic()))

        rpc.time = SimpleNamespace(monotonic=clock, time=time.time, monotonic_ns=time.monotonic_ns)
        self.client._receive_json = receive
        self.client._send_json = write
        def call():
            try:
                result['value'] = self.client.request('deadline-race', deadline=deadline)
            except Exception as error:
                result['error'] = error
        caller = threading.Thread(target=call)
        self.workers.append(caller)
        try:
            caller.start()
            self.assertEqual(frame(self.peer), (1, b'{"id":1,"method":"deadline-race"}'))
            send(self.peer, b'{"id":1,"result":"response"}')
            caller.join(3)
            self.assertFalse(caller.is_alive())
            if boundary == 'before':
                self.assertEqual(result, {'value':'response'})
            else:
                self.assertNotIn('value', result)
                self.assertIsInstance(result.get('error'), rpc.TransportError)
                self.assertIn('timed out', str(result['error']))
                self.assertIsNone(self.client._sock)
        finally:
            rpc.time = original_time
            self.client._send_json = original_send
            self.client._receive_json = original_receive

    def test_response_at_deadline_cannot_win_waiter_race(self):
        self.deadline_response_race('equal')

    def test_response_after_deadline_cannot_win_waiter_race(self):
        self.deadline_response_race('after')

    def test_timely_response_survives_delayed_caller_wakeup(self):
        self.deadline_response_race('before')

    def pending(self):
        return self.client.next_server_request(deadline=time.monotonic()+1)

    def test_idle_command_and_file_exact_id_and_one_use(self):
        for payload, request_id, method in (
            (b'{"id":"command-1","method":"item/commandExecution/requestApproval","params":{"threadId":"thread-a","turnId":"turn-a","itemId":"item-a"}}', 'command-1', COMMAND),
            (b'{"id":71,"method":"item/fileChange/requestApproval","params":{"threadId":"thread-a","turnId":"turn-a","itemId":"item-b"}}', 71, FILE),
        ):
            send(self.peer, payload)
            item = self.pending()
            self.assertEqual(item['id'], request_id)
            self.assertEqual(item['method'], method)
            self.assertEqual(item['params']['threadId'], 'thread-a')
            self.assertEqual(item['params']['turnId'], 'turn-a')
            self.client.respond_server_request(item['handle'], {'decision':'decline'})
            expected = (b'{"id":"command-1","result":{"decision":"decline"}}' if request_id == 'command-1'
                        else b'{"id":71,"result":{"decision":"decline"}}')
            self.assertEqual(frame(self.peer), (1, expected))
            with self.assertRaises(rpc.TransportError):
                self.client.respond_server_request(item['handle'], {'decision':'accept'})
        self.assertEqual([item['outcome'] for item in self.client.server_request_outcomes()], ['responded','responded'])

    def test_request_during_outbound_call_does_not_block_reader(self):
        thread, result = self.start_call('work')
        self.assertEqual(frame(self.peer), (1, b'{"id":1,"method":"work"}'))
        send(self.peer, b'{"id":"approval","method":"item/commandExecution/requestApproval","params":{"threadId":"t","turnId":"u","itemId":"i"}}')
        item = self.pending()
        self.assertTrue(thread.is_alive())
        # The reader must continue delivering observations while the human-side
        # consumer holds this pending request without answering it.
        send(self.peer, b'{"method":"item/started","params":{"threadId":"t","turnId":"u","item":{"id":"i"}}}')
        notice = self.client.next_notification(0, deadline=time.monotonic()+1)
        self.assertEqual(notice[1]['params']['item']['id'], 'i')
        self.client.respond_server_request(item['handle'], {'decision':'accept'})
        self.assertEqual(frame(self.peer), (1, b'{"id":"approval","result":{"decision":"accept"}}'))
        send(self.peer, b'{"method":"serverRequest/resolved","params":{"requestId":"approval"}}')
        send(self.peer, b'{"method":"item/completed","params":{"threadId":"t","turnId":"u","item":{"id":"i"}}}')
        send(self.peer, b'{"id":1,"result":{"ok":true}}')
        thread.join(1)
        self.assertEqual(result, {'value': {'ok': True}})
        notice2 = self.client.next_notification(notice[0], deadline=time.monotonic()+1)
        self.assertEqual(notice2[1]['params']['requestId'], 'approval')
        completed = self.client.next_notification(notice2[0], deadline=time.monotonic()+1)
        self.assertEqual(completed[1]['method'], 'item/completed')
        self.assertEqual(completed[1]['params'], {'threadId':'t', 'turnId':'u', 'item':{'id':'i'}})

    def test_concurrent_calls_correlate_reversed_responses(self):
        first, a = self.start_call('first')
        self.assertEqual(frame(self.peer), (1, b'{"id":1,"method":"first"}'))
        second, b = self.start_call('second')
        self.assertEqual(frame(self.peer), (1, b'{"id":2,"method":"second"}'))
        send(self.peer, b'{"id":2,"result":"second-result"}')
        send(self.peer, b'{"id":1,"result":"first-result"}')
        first.join(1); second.join(1)
        self.assertEqual(a, {'value': 'first-result'})
        self.assertEqual(b, {'value': 'second-result'})

    def test_default_unknown_and_malformed_requests_fail_closed(self):
        self.client.close(); self.peer.close()
        self.connect(methods=())
        send(self.peer, b'{"id":8,"method":"item/commandExecution/requestApproval","params":{}}')
        self.assertEqual(frame(self.peer), (1, b'{"id":8,"error":{"code":-32601,"message":"Method not supported"}}'))
        send(self.peer, b'{"id":true,"method":"x"}')
        self.assertEqual(frame(self.peer), (1, b'{"id":null,"error":{"code":-32600,"message":"Invalid request"}}'))
        with self.assertRaises(rpc.TransportError):
            self.client.next_server_request(deadline=time.monotonic()+0.05)

    def test_invalid_params_and_unknown_handle_do_not_send_approval(self):
        send(self.peer, b'{"id":9,"method":"item/fileChange/requestApproval","params":[]}')
        self.assertEqual(frame(self.peer), (1, b'{"id":9,"error":{"code":-32602,"message":"Invalid params"}}'))
        with self.assertRaises(rpc.TransportError):
            self.client.respond_server_request('unissued-handle', {'decision':'accept'})
        self.peer.settimeout(0.05)
        with self.assertRaises(socket.timeout): self.peer.recv(1)

    def test_expiration_rejects_late_response(self):
        self.client.close(); self.peer.close()
        self.connect(pending_timeout=0.08)
        send(self.peer, b'{"id":10,"method":"item/fileChange/requestApproval","params":{}}')
        item = self.pending()
        self.assertEqual(frame(self.peer), (1, b'{"id":10,"error":{"code":-32000,"message":"Request timed out"}}'))
        with self.assertRaises(rpc.TransportError):
            self.client.respond_server_request(item['handle'], {'decision':'accept'})
        self.assertEqual(self.client.server_request_outcomes()[-1]['outcome'], 'timed_out')

    def test_disconnect_cancels_waiters_and_pending(self):
        thread, result = self.start_call('work')
        frame(self.peer)
        send(self.peer, b'{"id":11,"method":"item/fileChange/requestApproval","params":{}}')
        item = self.pending()
        self.peer.shutdown(socket.SHUT_RDWR)
        thread.join(1)
        self.assertIsInstance(result.get('error'), rpc.TransportError)
        with self.assertRaises(rpc.TransportError): self.client.respond_server_request(item['handle'], {})
        self.assertEqual(self.client.server_request_outcomes()[-1]['outcome'], 'disconnected')

    def test_old_handle_cannot_write_on_reconnected_socket(self):
        send(self.peer, b'{"id":12,"method":"item/fileChange/requestApproval","params":{}}')
        item = self.pending()
        self.client.close(); self.peer.close()
        left, self.peer = socket.socketpair()
        left.setblocking(False)
        self.peer.settimeout(0.05)
        self.client._sock = left
        self.client._ensure_reader()
        with self.assertRaises(rpc.TransportError): self.client.respond_server_request(item['handle'], {})
        with self.assertRaises(socket.timeout): self.peer.recv(1)
        send(self.peer, b'{"id":12,"method":"item/fileChange/requestApproval","params":{}}')
        current = self.pending()
        self.assertGreater(current['connection_epoch'], item['connection_epoch'])
        self.assertNotEqual(current['handle'], item['handle'])
        self.client.respond_server_request(current['handle'], {'decision':'decline'})
        self.assertEqual(frame(self.peer), (1, b'{"id":12,"result":{"decision":"decline"}}'))

    def test_outbound_deadline_cancels_pending_connection(self):
        thread, result = self.start_call('work', timeout=0.1)
        frame(self.peer)
        send(self.peer, b'{"id":13,"method":"item/fileChange/requestApproval","params":{}}')
        item = self.pending()
        thread.join(1)
        self.assertIsInstance(result.get('error'), rpc.TransportError)
        self.assertIsNone(self.client._sock)
        with self.assertRaises(rpc.TransportError): self.client.respond_server_request(item['handle'], {})

    def test_notifications_have_reader_owned_drain_boundary(self):
        send(self.peer, b'{"method":"before","params":{}}')
        boundary = self.client.notification_boundary(deadline=time.monotonic()+1)
        self.assertEqual(boundary, 1)
        send(self.peer, b'{"method":"after","params":{}}')
        observed = self.client.next_notification(boundary, deadline=time.monotonic()+1)
        self.assertEqual(observed[1]['method'], 'after')

    def test_completed_response_survives_immediate_eof(self):
        thread, result = self.start_call('work')
        frame(self.peer)
        send(self.peer, b'{"id":1,"result":42}')
        self.peer.shutdown(socket.SHUT_RDWR)
        thread.join(1)
        self.assertEqual(result, {'value': 42})

    def test_duplicate_provider_id_invalidates_pending_epoch(self):
        wire = b'{"id":14,"method":"item/fileChange/requestApproval","params":{}}'
        send(self.peer, wire)
        item = self.pending()
        send(self.peer, wire)
        self.assertEqual(self.peer.recv(1), b'')
        with self.assertRaises(rpc.TransportError): self.client.respond_server_request(item['handle'], {})

    def test_wrong_client_cannot_consume_handle_or_change_context(self):
        send(self.peer, b'{"id":"original","method":"item/fileChange/requestApproval","params":{"threadId":"original-thread","turnId":"original-turn","itemId":"original-item"}}')
        item = self.pending()
        other = rpc.RpcClient('ws://127.0.0.1:1')
        with self.assertRaises(rpc.TransportError): other.respond_server_request(item['handle'], {})
        item['id'] = 'wrong-id'
        item['params']['threadId'] = 'wrong-thread'
        saved = self.client._server_pending[item['handle']]
        self.assertEqual(saved['params']['threadId'], 'original-thread')
        self.assertEqual(saved['params']['turnId'], 'original-turn')
        self.assertEqual(saved['params']['itemId'], 'original-item')
        self.client.respond_server_request(item['handle'], {'decision':'decline'})
        self.assertEqual(frame(self.peer), (1, b'{"id":"original","result":{"decision":"decline"}}'))

    def test_reader_keeps_receiving_while_writer_is_held(self):
        # A caller that is writing must not prevent the sole reader from
        # observing an unrelated incoming frame when no expiration needs I/O.
        with self.client._write_lock:
            send(self.peer, b'{"method":"during-write","params":{}}')
            observed = self.client.next_notification(0, deadline=time.monotonic()+0.5)
            self.assertEqual(observed[1]['method'], 'during-write')

    def test_deadline_aborts_backpressured_concurrent_writer(self):
        self.client._sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        large_result = {}
        def large_call():
            try:
                self.client.request('large', {'payload':'x'*1000000}, deadline=time.monotonic()+5)
            except Exception as error:
                large_result['error'] = error
        writer = threading.Thread(target=large_call)
        self.workers.append(writer)
        writer.start()
        self.assertTrue(select.select([self.peer], [], [], 1)[0])
        self.assertTrue(writer.is_alive())
        started = time.monotonic()
        caller, result = self.start_call('short-deadline', timeout=0.1)
        caller.join(0.7)
        self.assertFalse(caller.is_alive())
        self.assertLess(time.monotonic()-started, 0.7)
        self.assertIsInstance(result.get('error'), rpc.TransportError)
        writer.join(1)
        self.assertFalse(writer.is_alive())
        self.assertIsInstance(large_result.get('error'), rpc.TransportError)

class PublicSocketWorkflow(unittest.TestCase):
    def test_connect_initialize_and_inflight_provider_response_on_same_tcp_connection(self):
        listener = socket.socket()
        listener.bind(('127.0.0.1', 0))
        listener.listen(1)
        listener.settimeout(3)
        errors = []
        finished = threading.Event()
        def server():
            try:
                conn, _ = listener.accept()
                with conn:
                    conn.settimeout(3)
                    header = b''
                    while b'\r\n\r\n' not in header:
                        header += read_exact(conn, 1)
                    key = next(line.split(b': ', 1)[1] for line in header.split(b'\r\n')
                               if line.lower().startswith(b'sec-websocket-key:'))
                    accept = base64.b64encode(hashlib.sha1(key + b'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest())
                    conn.sendall(b'HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + b'\r\n\r\n')
                    self.assertEqual(frame(conn), (1, b'{"id":1,"method":"initialize","params":{"clientInfo":{"name":"agent_chat_bridge","version":"0.1.0"},"capabilities":{"experimentalApi":true}}}'))
                    send(conn, b'{"id":1,"result":{}}')
                    self.assertEqual(frame(conn), (1, b'{"method":"initialized","params":{}}'))
                    self.assertEqual(frame(conn), (1, b'{"id":2,"method":"work"}'))
                    send(conn, b'{"id":"native-approval","method":"item/commandExecution/requestApproval","params":{"threadId":"t","turnId":"u","itemId":"i"}}')
                    self.assertEqual(frame(conn), (1, b'{"id":"native-approval","result":{"decision":"decline"}}'))
                    send(conn, b'{"id":2,"result":{"completed":true}}')
                    finished.wait(3)
            except Exception as error:
                errors.append(error)
        server_thread = threading.Thread(target=server)
        server_thread.start()
        client = rpc.RpcClient('ws://127.0.0.1:' + str(listener.getsockname()[1]),
                               timeout=2, server_request_methods=(COMMAND,))
        caller = None
        result = {}
        try:
            client.connect()
            def outbound():
                try: result['value'] = client.request('work')
                except Exception as error: result['error'] = error
            caller = threading.Thread(target=outbound)
            caller.start()
            item = client.next_server_request(deadline=time.monotonic()+1)
            self.assertTrue(caller.is_alive())
            client.respond_server_request(item['handle'], {'decision':'decline'})
            caller.join(2)
            self.assertFalse(caller.is_alive())
            self.assertEqual(result, {'value': {'completed': True}})
        finally:
            client.close()
            finished.set()
            server_thread.join(4)
            if caller is not None: caller.join(3)
            listener.close()
        self.assertFalse(server_thread.is_alive())
        self.assertEqual(errors, [])



import importlib.util
import base64
import hashlib
import socket
import threading
import time
import unittest

from agent_chat import rpc

class DeadlineTests(unittest.TestCase):
    def setUp(self):
        self.left, self.right = socket.socketpair()
        self.left.setblocking(False)
        self.client = rpc.RpcClient('ws://127.0.0.1:1', timeout=0.1)
        self.client._sock = self.left
    def tearDown(self):
        self.client.close()
        self.left.close()
        self.right.close()
    def test_read_timeout(self):
        with self.assertRaises(rpc.TransportError):
            self.client._read_exact(1, time.monotonic() + 0.05)
    def test_write_timeout_on_full_socket(self):
        self.left.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        with self.assertRaises(rpc.TransportError):
            self.client._send_bytes(b'x' * 1000000, time.monotonic() + 0.05)
    def test_read_and_write_do_not_share_deadline(self):
        result = []
        thread = threading.Thread(target=lambda: result.append(self.client._read_exact(1, time.monotonic() + 1)))
        thread.start()
        self.client._send_bytes(b'abc', time.monotonic() + 0.5)
        self.assertEqual(self.right.recv(3), b'abc')
        self.right.sendall(b'z')
        thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(result, [b'z'])
    def test_partial_write_preserves_bytes(self):
        self.left.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        payload = b'0123456789' * 10000
        got = bytearray()
        def reader():
            while len(got) < len(payload):
                got.extend(self.right.recv(min(1000, len(payload) - len(got))))
        thread = threading.Thread(target=reader)
        thread.start()
        self.client._send_bytes(payload, time.monotonic() + 2)
        thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(bytes(got), payload)
    def test_concurrent_read_and_write_frames_remain_intact(self):
        received = []
        thread = threading.Thread(target=lambda: received.append(self.client._receive_message(time.monotonic() + 1)))
        thread.start()
        self.client._send_frame(1, b'hello', time.monotonic() + 1)
        frame = self.right.recv(11)
        while len(frame) < 11:
            frame += self.right.recv(11 - len(frame))
        self.assertEqual(frame[:2], b'\x81\x85')
        self.assertEqual(bytes(v ^ frame[2 + i % 4] for i, v in enumerate(frame[6:])), b'hello')
        self.right.sendall(b'\x81\x05world')
        thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(received, [b'world'])
    def test_disconnect_wakes_blocked_read(self):
        result = []
        def read():
            try:
                self.client._read_exact(1, time.monotonic() + 2)
            except rpc.TransportError:
                result.append('closed')
        thread = threading.Thread(target=read)
        thread.start()
        time.sleep(0.03)
        self.client.close()
        thread.join(1)
        self.assertFalse(thread.is_alive())
        self.assertEqual(result, ['closed'])
    def test_close_serializes_detachment_before_competing_writer(self):
        close_written, proceed = threading.Event(), threading.Event()
        original = self.client._send_frame_locked
        def gated(opcode, payload, deadline=None, expected=None):
            original(opcode, payload, deadline, expected)
            if opcode == 8:
                close_written.set()
                proceed.wait(1)
        self.client._send_frame_locked = gated
        writer_outcome = []
        def write():
            try:
                self.client._send_frame(1, b'after-close', time.monotonic() + 1)
                writer_outcome.append('unexpected success')
            except rpc.TransportError:
                writer_outcome.append('closed')
        closer = threading.Thread(target=self.client.close)
        writer = threading.Thread(target=write)
        closer.start()
        try:
            self.assertTrue(close_written.wait(1))
            writer.start()
            time.sleep(0.03)
            self.assertTrue(writer.is_alive())
        finally:
            proceed.set()
            closer.join(1)
            if writer.ident is not None:
                writer.join(1)
        self.assertFalse(closer.is_alive())
        self.assertFalse(writer.is_alive())
        self.assertEqual(writer_outcome, ['closed'])
        wire = bytearray()
        while True:
            chunk = self.right.recv(1024)
            if not chunk: break
            wire.extend(chunk)
        self.assertEqual(len(wire), 8)
        self.assertEqual(wire[:2], b'\x88\x82')
    def test_old_socket_cannot_read_new_generation(self):
        old = self.left
        new, peer = socket.socketpair()
        new.setblocking(False)
        self.client._sock = new
        try:
            peer.sendall(b'x')
            with self.assertRaises(rpc.TransportError):
                self.client._read_exact(1, time.monotonic() + 0.5, old)
            self.assertEqual(self.client._read_exact(1, time.monotonic() + 0.5), b'x')
        finally:
            new.close()
            peer.close()
            self.client._sock = old
    def test_reconnect_between_readiness_and_read_fails_closed(self):
        old = self.left
        new, peer = socket.socketpair()
        new.setblocking(False)
        ready, proceed = threading.Event(), threading.Event()
        original_wait = self.client._wait_socket
        def paused_wait(sock, read, deadline):
            original_wait(sock, read, deadline)
            ready.set()
            proceed.wait(1)
        self.client._wait_socket = paused_wait
        outcome = []
        def read():
            try:
                self.client._read_exact(1, time.monotonic() + 2)
                outcome.append('unexpected success')
            except rpc.TransportError:
                outcome.append('connection changed')
        thread = threading.Thread(target=read)
        self.right.sendall(b'x')
        thread.start()
        try:
            self.assertTrue(ready.wait(1))
            self.client._sock = new
            proceed.set()
            thread.join(1)
            self.assertFalse(thread.is_alive())
            self.assertEqual(outcome, ['connection changed'])
        finally:
            proceed.set()
            thread.join(1)
            self.client._sock = old
            new.close()
            peer.close()
    def test_concurrent_frame_writers_serialize(self):
        payloads = [b'a' * 1000, b'b' * 1000]
        threads = [threading.Thread(target=self.client._send_frame, args=(1, value, time.monotonic() + 2)) for value in payloads]
        for thread in threads: thread.start()
        data = bytearray()
        while len(data) < 2 * (1000 + 8):
            data.extend(self.right.recv(4096))
        for thread in threads:
            thread.join(2)
            self.assertFalse(thread.is_alive())
        decoded = []
        for offset in (0, 1008):
            frame = data[offset:offset+1008]
            self.assertEqual(frame[:4], b'\x81\xfe\x03\xe8')
            decoded.append(bytes(v ^ frame[4 + i % 4] for i, v in enumerate(frame[8:])))
        self.assertCountEqual(decoded, payloads)
    def test_ordinary_request_uses_one_absolute_deadline(self):
        # Three valid notifications must not renew the original 2s budget.
        # The last notification arrives at 1.5s: restarting a 2s timeout
        # would return at about 3.5s, well beyond the 2.75s assertion.
        self.client.timeout = 2.0
        finished = threading.Event()
        sent_at = []
        errors = []
        def server():
            try:
                self.right.settimeout(4)
                self.right.recv(4096)
                event = b'{"method":"event"}'
                for offset in (0.5, 1.0, 1.5):
                    if finished.wait(max(0, started + offset - time.monotonic())):
                        return
                    self.right.sendall(bytes([0x81, len(event)]) + event)
                    sent_at.append(time.monotonic() - started)
                finished.wait(4)
            except Exception as error:
                errors.append(error)
        thread = threading.Thread(target=server)
        started = time.monotonic()
        thread.start()
        try:
            with self.assertRaises(rpc.TransportError):
                self.client.request('example', deadline=started + 2.0)
            elapsed = time.monotonic() - started
        finally:
            finished.set()
            thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(len(sent_at), 3)
        self.assertGreaterEqual(sent_at[-1], 1.4)
        self.assertLess(sent_at[-1], 1.75)
        self.assertGreaterEqual(elapsed, 1.8)
        self.assertLess(elapsed, 2.75)
        print(f'absolute deadline: notifications={sent_at!r}, elapsed={elapsed:.6f}s, '
              'budget=2.0s, accepted=[1.8,2.75)s, renewed>=3.4s', flush=True)

class RpcRegression(unittest.TestCase):
    def test_initialize_and_ordinary_rpc(self):
        listener = socket.socket()
        listener.bind(('127.0.0.1', 0))
        listener.listen(1)
        errors = []
        def readn(sock, n):
            data = bytearray()
            while len(data) < n:
                part = sock.recv(n - len(data))
                if not part:
                    raise EOFError()
                data.extend(part)
            return bytes(data)
        def frame(sock):
            h = readn(sock, 2)
            size = h[1] & 127
            if size == 126:
                size = int.from_bytes(readn(sock, 2), 'big')
            elif size == 127:
                size = int.from_bytes(readn(sock, 8), 'big')
            readn(sock, 4)
            readn(sock, size)
        def send(sock, payload):
            sock.sendall(bytes([0x81, len(payload)]) + payload)
        def server():
            try:
                conn, _ = listener.accept()
                with conn:
                    request = b''
                    while b'\r\n\r\n' not in request:
                        request += conn.recv(1)
                    key = next(line.split(b': ', 1)[1] for line in request.split(b'\r\n') if line.lower().startswith(b'sec-websocket-key:'))
                    accept = base64.b64encode(hashlib.sha1(key + b'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest())
                    handshake = b'HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + b'\r\n\r\n'
                    for offset in range(0, len(handshake), 7):
                        conn.sendall(handshake[offset:offset+7])
                        time.sleep(0.002)
                    frame(conn)
                    send(conn, b'{"id":1,"result":{}}')
                    frame(conn)
                    frame(conn)
                    send(conn, b'{"id":2,"result":{"ok":true}}')
            except Exception as error:
                errors.append(error)
            finally:
                listener.close()
        thread = threading.Thread(target=server)
        thread.start()
        client = rpc.RpcClient('ws://127.0.0.1:' + str(listener.getsockname()[1]))
        with client:
            self.assertEqual(client.request('example'), {'ok': True})
        thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])

