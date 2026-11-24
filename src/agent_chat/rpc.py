"""Small, dependency-free WebSocket transport for the local Codex app-server."""

from __future__ import annotations

import base64
import copy
from collections import deque
import hashlib
import json
import math
import os
import select
import socket
import struct
import threading
import time
import uuid
from typing import Any
from urllib.parse import urlsplit


_MAX_MESSAGE = 16 * 1024 * 1024
_MAX_HEADER = 64 * 1024
_PROVIDER_APPROVAL_METHODS = frozenset((
    'item/commandExecution/requestApproval', 'item/fileChange/requestApproval',
    'item/permissions/requestApproval',
))


class TransportError(OSError):
    """The connection failed, was interrupted, or sent invalid WebSocket data."""


class RpcError(Exception):
    """An error returned by the JSON-RPC server."""

    def __init__(self, code: Any, message: str, data: Any = None) -> None:
        super().__init__(f"RPC error {code}: {message}")
        self.code = code
        self.message = message
        self.data = data


class RpcClient:
    """Synchronous JSON-RPC client restricted to a local WebSocket endpoint."""

    def __init__(self, endpoint: str, timeout: float = 10, *,
                 server_request_methods: tuple[str, ...] = (), server_request_timeout: float = 60) -> None:
        if not isinstance(timeout, (int, float)) or timeout <= 0:
            raise ValueError("timeout must be positive")
        self.endpoint = endpoint
        self.timeout = float(timeout)
        if not math.isfinite(self.timeout):
            raise ValueError("timeout must be finite")
        self._host, self._port, self._target = self._parse_endpoint()
        self._sock: socket.socket | None = None
        self._write_lock = threading.RLock()
        self._connect_lock = threading.RLock()
        self._state = threading.Condition(threading.RLock())
        self._reader = None
        self._reader_sock = None
        self._reader_error = None
        self._epoch = 0
        self._connection_instance = uuid.uuid4().hex
        self._waiters = {}
        self._barriers = deque()
        if (not isinstance(server_request_methods, tuple)
                or any(not isinstance(method, str) or method not in _PROVIDER_APPROVAL_METHODS
                       for method in server_request_methods)):
            raise ValueError("server request methods must be explicit supported approval methods")
        if (isinstance(server_request_timeout, bool) or not isinstance(server_request_timeout, (int, float))
                or not math.isfinite(server_request_timeout) or server_request_timeout <= 0):
            raise ValueError("server request timeout must be positive and finite")
        self._server_methods = frozenset(server_request_methods)
        self._server_timeout = float(server_request_timeout)
        self._server_pending = {}
        self._server_queue = deque()
        self._server_ids = set()
        self._server_outcomes = deque(maxlen=1024)
        self._next_id = 1
        self.initialize_response: Any = None
        self._notifications = deque(maxlen=1024)
        self._notification_observations = deque(maxlen=1024)
        self._notification_sequence = 0
        self._notification_floor = 0

    def __enter__(self) -> "RpcClient":
        return self.connect()

    def __exit__(self, *_: object) -> None:
        self.close()

    def connect(self) -> "RpcClient":
        with self._connect_lock:
            return self._connect()

    def _connect(self) -> "RpcClient":
        if self._sock is not None:
            return self
        host, port, target = self._host, self._port, self._target
        self.initialize_response = None
        self._notifications.clear()
        self._notification_observations.clear()
        self._notification_floor = self._notification_sequence
        try:
            addresses = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
            if not addresses or any(not self._is_loopback(a[4][0]) for a in addresses):
                raise TransportError("endpoint does not resolve solely to loopback")
            family, socktype, proto, _, address = addresses[0]
            sock = socket.socket(family, socktype, proto)
            sock.settimeout(self.timeout)
            self._sock = sock
            sock.connect(address)
            sock.setblocking(False)
            self._handshake(host, target)
            self._ensure_reader()
            self._initialize()
            return self
        except RpcError:
            self.close()
            raise
        except (OSError, ValueError, UnicodeError) as exc:
            self.close()
            if isinstance(exc, TransportError):
                raise
            raise TransportError(str(exc)) from exc

    def close(self) -> None:
        with self._connect_lock:
            self._close()

    def _close(self) -> None:
        reader = self._reader
        with self._write_lock:
            sock = self._sock
            if sock is not None:
                try:
                    self._send_frame(0x8, b"\x03\xe8", expected=sock)
                except (OSError, TransportError):
                    pass
                finally:
                    self._disconnect(sock, TransportError("connection closed"))
        if reader is not None and reader is not threading.current_thread():
            reader.join(self.timeout + 1)
            if reader.is_alive():
                raise TransportError("socket reader did not stop")

    def _disconnect(self, sock, error):
        # Abort cannot wait behind a backpressured writer: shutdown wakes it.
        # Graceful close still owns _write_lock across frame + detachment.
        # Socket identity prevents an old reader from cancelling a new epoch.
        with self._state:
            if self._sock is not sock:
                return
            self._sock = None
            self._reader_error = error
            for waiter in self._waiters.values():
                if not waiter['event'].is_set():
                    waiter['error'] = error
                    waiter['event'].set()
            self._waiters.clear()
            for pending in self._server_pending.values():
                self._outcome(pending, 'disconnected')
            self._server_pending.clear()
            self._server_queue.clear()
            for barrier in self._barriers:
                barrier['error'] = error
                barrier['event'].set()
            self._barriers.clear()
            self._state.notify_all()
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        sock.close()


    def _ensure_reader(self):
        with self._state:
            sock = self._sock
            if sock is None:
                raise self._reader_error or TransportError("not connected")
            if self._reader_sock is sock and self._reader is not None and self._reader.is_alive():
                return sock
            self._epoch += 1
            self._server_ids.clear()
            self._reader_error = None
            self._reader_sock = sock
            self._reader = threading.Thread(target=self._read_loop, args=(sock,),
                                            name='agent-chat-rpc-reader', daemon=True)
            self._reader.start()
            return sock

    def _read_loop(self, sock):
        try:
            while self._sock is sock:
                self._expire_server_requests(sock)
                if not select.select([sock], [], [], 0.02)[0]:
                    with self._state:
                        while self._barriers:
                            barrier = self._barriers.popleft()
                            barrier['sequence'] = self._notification_sequence
                            barrier['event'].set()
                    continue
                with self._state:
                    frame_deadline = min([time.monotonic() + self.timeout] +
                                         [item['deadline'] for item in self._server_pending.values()])
                message = self._receive_json(frame_deadline, sock)
                if message is None:
                    continue
                observed_at = time.time()
                observed_monotonic_ns = time.monotonic_ns()
                if 'method' in message and 'id' in message:
                    self._server_request(message, sock)
                elif 'method' in message:
                    if not isinstance(message['method'], str):
                        raise TransportError('invalid notification method')
                    with self._state:
                        if self._sock is not sock:
                            return
                        params = message.get('params', {})
                        if isinstance(params, dict):
                            for handle, pending in list(self._server_pending.items()):
                                scope = pending['params']
                                same_thread = params.get('threadId') == scope.get('threadId')
                                resolved = (message['method'] == 'serverRequest/resolved' and same_thread
                                            and params.get('requestId') == pending['id'])
                                item = params.get('item', {})
                                completed = (message['method'] == 'item/completed' and same_thread
                                             and params.get('turnId') == scope.get('turnId')
                                             and isinstance(item, dict) and item.get('id') == scope.get('itemId'))
                                if resolved or completed:
                                    self._server_pending.pop(handle)
                                    self._outcome(pending, 'resolved' if resolved else 'completed')
                        self._record_notification(message, observed_at, observed_monotonic_ns)
                        self._state.notify_all()
                else:
                    request_id = message.get('id')
                    if type(request_id) not in (int, str):
                        raise TransportError('invalid RPC response id')
                    with self._state:
                        if self._sock is not sock:
                            return
                        waiter = self._waiters.get(request_id)
                        if waiter is not None:
                            if waiter['event'].is_set():
                                raise TransportError('duplicate RPC response')
                            if ('result' in message) == ('error' in message):
                                raise TransportError('malformed RPC response')
                            received_at = time.monotonic()
                            if received_at >= waiter['deadline']:
                                raise TransportError('request timed out')
                            # Decide timeliness before waking the caller. An
                            # already-set Event cannot turn a late reply into
                            # success, even if the caller missed its wait budget.
                            waiter['received_at'] = received_at
                            waiter['response'] = message
                            waiter['event'].set()
        except Exception as error:
            self._disconnect(sock, error if isinstance(error, TransportError) else TransportError(str(error)))

    def _outcome(self, pending, outcome):
        self._server_outcomes.append(dict(handle=pending['handle'], connection_epoch=pending['connection_epoch'],
                                          method=pending['method'], outcome=outcome))

    def _server_request(self, message, sock):
        request_id, method = message['id'], message['method']
        if type(request_id) not in (int, str) or not isinstance(method, str) or not method:
            self._send_json({'id': None, 'error': {'code': -32600, 'message': 'Invalid request'}}, expected=sock)
            return
        with self._state:
            if self._sock is not sock:
                return
            if request_id in self._server_ids:
                raise TransportError('duplicate server request id')
            self._server_ids.add(request_id)
        if method not in self._server_methods:
            self._reject_server_request(message, time.monotonic() + self.timeout, sock)
            return
        if not isinstance(message.get('params', {}), dict):
            self._send_json({'id': request_id, 'error': {'code': -32602, 'message': 'Invalid params'}}, expected=sock)
            return
        with self._state:
            if self._sock is not sock:
                return
            handle = uuid.uuid4().hex
            self._server_pending[handle] = dict(handle=handle, connection_epoch=self._epoch,
                id=request_id, method=method, params=copy.deepcopy(message.get('params', {})),
                deadline=time.monotonic() + self._server_timeout)
            self._server_queue.append(handle)
            self._state.notify_all()

    def _expire_server_requests(self, sock):
        with self._state:
            if not any(item['deadline'] <= time.monotonic() for item in self._server_pending.values()):
                return
        with self._write_lock:
            with self._state:
                if self._sock is not sock:
                    return
                expired = [item for item in self._server_pending.values() if item['deadline'] <= time.monotonic()]
                for item in expired:
                    self._server_pending.pop(item['handle'])
                    self._outcome(item, 'timed_out')
                self._server_queue = deque(handle for handle in self._server_queue if handle in self._server_pending)
                self._state.notify_all()
            for item in expired:
                self._send_json({'id': item['id'], 'error': {'code': -32000, 'message': 'Request timed out'}}, expected=sock)

    def connection_identity(self) -> dict[str, Any]:
        """Snapshot the actual live RPC socket and its client generation; no reconnect.

        Compare snapshots around the operation and independently verify the peer
        against the managed host. This is a bounded observation, not a lease.
        """
        with self._state:
            sock = self._sock
            if (sock is None or self._reader_sock is not sock or self._reader is None
                    or not self._reader.is_alive() or self._epoch <= 0):
                raise TransportError("RPC connection is not live")
            try:
                descriptor = sock.fileno()
                local = sock.getsockname()
                peer = sock.getpeername()
            except (OSError, ValueError) as error:
                raise TransportError("RPC connection identity is unavailable") from error
            if descriptor < 0 or self._sock is not sock or self._reader_sock is not sock:
                raise TransportError("RPC connection changed during identity sampling")
            return {"client_instance": self._connection_instance, "connection_epoch": self._epoch,
                    "pid": os.getpid(), "fd": descriptor, "local": local, "peer": peer}

    def approval_connection(self):
        """A readable epoch, excluding sockets still doing the HTTP handshake."""
        with self._state:
            if (self._sock is None or self._reader_sock is not self._sock
                    or self._reader is None or not self._reader.is_alive()):
                return None
            return self._epoch, self._notification_floor

    def poll_server_request(self):
        """Return a queued native request or None without an artificial timeout."""
        sock = self._ensure_reader()
        with self._state:
            if self._sock is not sock:
                raise self._reader_error or TransportError('connection closed')
            while self._server_queue:
                item = self._server_pending.get(self._server_queue.popleft())
                if item is not None and item['deadline'] > time.monotonic():
                    return copy.deepcopy(item)
            return None

    def next_server_request(self, *, deadline: float):
        """Bridge-only queue: never block the socket reader on a human decision."""
        sock = self._ensure_reader()
        with self._state:
            while self._sock is sock:
                while self._server_queue:
                    item = self._server_pending.get(self._server_queue.popleft())
                    if item is not None and item['deadline'] > time.monotonic():
                        return copy.deepcopy(item)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TransportError('server request wait timed out')
                self._state.wait(remaining)
            raise self._reader_error or TransportError('connection closed')

    def respond_server_request(self, handle: str, result: Any) -> None:
        """One-use same-connection response. The bridge validates actor/decision."""
        # Encode before consuming the handle, so invalid local data cannot strand it.
        json.dumps(result, allow_nan=False)
        with self._write_lock:
            with self._state:
                item = self._server_pending.get(handle)
                sock = self._sock
                if (item is None or sock is None or item['connection_epoch'] != self._epoch
                        or item['deadline'] <= time.monotonic()):
                    raise TransportError('unknown, stale or expired server request')
                self._server_pending.pop(handle)
            try:
                self._send_json({'id': item['id'], 'result': result}, expected=sock)
            except Exception:
                with self._state:
                    self._outcome(item, 'response_failed')
                self._disconnect(sock, TransportError('server response write failed'))
                raise
            with self._state:
                self._outcome(item, 'responded')

    def server_request_outcomes(self):
        with self._state:
            return copy.deepcopy(list(self._server_outcomes))

    def request(self, method: str, params: Any = None, *, deadline: float | None = None) -> Any:
        sock = self._ensure_reader()
        deadline = time.monotonic() + self.timeout if deadline is None else deadline
        self._set_deadline(deadline)
        with self._state:
            request_id = self._next_id
            self._next_id += 1
            waiter = {'event': threading.Event(), 'deadline': deadline}
            self._waiters[request_id] = waiter
        message: dict[str, Any] = {'id': request_id, 'method': method}
        if params is not None:
            message['params'] = params
        try:
            self._send_json(message, deadline, sock)
            if not waiter['event'].wait(max(0, deadline - time.monotonic())):
                error = TransportError('request timed out')
                self._disconnect(sock, error)
                raise error
            if 'error' in waiter:
                raise waiter['error']
            response = waiter['response']
            if 'error' in response:
                error = response['error']
                if not isinstance(error, dict):
                    raise TransportError('malformed RPC error')
                raise RpcError(error.get('code'), str(error.get('message', '')), error.get('data'))
            return response['result']
        except (OSError, ValueError, TypeError) as error:
            self._disconnect(sock, TransportError(str(error)))
            raise
        finally:
            with self._state:
                self._waiters.pop(request_id, None)

    def _initialize(self) -> None:
        self.initialize_response = self.request("initialize", {
            "clientInfo": {"name": "agent_chat_bridge", "version": "0.1.0"},
            "capabilities": {"experimentalApi": True},
        })
        self._send_json({"method": "initialized", "params": {}})

    def _record_notification(self, message: dict[str, Any], observed_at: float | None = None,
                             observed_monotonic_ns: int | None = None) -> None:
        if observed_at is None:
            observed_at = time.time()
        if observed_monotonic_ns is None:
            observed_monotonic_ns = time.monotonic_ns()
        if len(self._notifications) == self._notifications.maxlen:
            self._notification_floor = self._notifications[0][0]
        self._notification_sequence += 1
        self._notifications.append((self._notification_sequence, copy.deepcopy(message)))
        self._notification_observations.append({
            'sequence': self._notification_sequence,
            'observed_at': observed_at,
            'observed_monotonic_ns': observed_monotonic_ns,
            'connection_instance': self._connection_instance,
            'connection_epoch': self._epoch,
        })

    def notification_observations_since(self, boundary: int) -> list[dict[str, Any]]:
        """Reader receipt clocks only; no subscription or attribution guarantee."""
        with self._state:
            if type(boundary) is not int or boundary < self._notification_floor or boundary > self._notification_sequence:
                raise TransportError('notification observation boundary is unavailable')
            return copy.deepcopy([item for item in self._notification_observations if item['sequence'] > boundary])

    def notification_snapshot_since(self, boundary: int) -> tuple[list[tuple[int, dict[str, Any]]], list[dict[str, Any]]]:
        """Return events and reader receipts at one locked instant."""
        with self._state:
            if type(boundary) is not int or boundary < self._notification_floor or boundary > self._notification_sequence:
                raise TransportError('notification observation boundary is unavailable')
            return (copy.deepcopy([item for item in self._notifications if item[0] > boundary]),
                    copy.deepcopy([item for item in self._notification_observations if item['sequence'] > boundary]))

    def notifications_since(self, boundary: int) -> list[tuple[int, dict[str, Any]]]:
        """Raw observations in receive order; a dropped window is uncertainty."""
        with self._state:
            if type(boundary) is not int or boundary < self._notification_floor or boundary > self._notification_sequence:
                raise TransportError('notification observation boundary is unavailable')
            return copy.deepcopy([item for item in self._notifications if item[0] > boundary])

    def notification_boundary(self, *, deadline: float) -> int:
        """Ask the sole reader to drain already-readable wire data, then snapshot."""
        sock = self._ensure_reader()
        self._set_deadline(deadline)
        barrier = {'event': threading.Event()}
        with self._state:
            if self._sock is not sock:
                raise self._reader_error or TransportError('connection closed')
            self._barriers.append(barrier)
        if not barrier['event'].wait(max(0, deadline - time.monotonic())):
            with self._state:
                try: self._barriers.remove(barrier)
                except ValueError: pass
            raise TransportError('notification boundary timed out')
        if 'error' in barrier:
            raise barrier['error']
        return barrier['sequence']

    def next_notification(self, boundary: int, *, deadline: float) -> tuple[int, dict[str, Any]]:
        sock = self._ensure_reader()
        with self._state:
            while self._sock is sock:
                available = self.notifications_since(boundary)
                if available:
                    return available[0]
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TransportError('notification wait timed out')
                self._state.wait(remaining)
            raise self._reader_error or TransportError('connection closed')

    def _parse_endpoint(self) -> tuple[str, int, str]:
        try:
            parsed = urlsplit(self.endpoint)
        except ValueError as exc:
            raise ValueError("invalid WebSocket endpoint") from exc
        if (parsed.scheme != "ws" or parsed.username is not None or parsed.password is not None
                or parsed.query or parsed.fragment or parsed.path not in ("", "/")):
            raise ValueError("endpoint must be a bare local ws URL")
        host = parsed.hostname
        try:
            port = parsed.port
        except ValueError as exc:
            raise ValueError("endpoint must have a valid port") from exc
        if host not in {"127.0.0.1", "localhost", "::1"} or port is None or not 1 <= port <= 65535:
            raise ValueError("endpoint must be ws://127.0.0.1, localhost, or [::1] with a port")
        return host, port, "/"

    @staticmethod
    def _is_loopback(address: str) -> bool:
        try:
            return address == "127.0.0.1" or address == "::1"
        except ValueError:
            return False

    def _handshake(self, host: str, target: str) -> None:
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        host_header = f"[{host}]" if ":" in host else host
        port = self._port
        request = (f"GET {target} HTTP/1.1\r\nHost: {host_header}:{port}\r\n"
                   f"Upgrade: websocket\r\nConnection: Upgrade\r\n"
                   f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n")
        self._send_bytes(request.encode("ascii"))
        raw = self._read_until(b"\r\n\r\n", _MAX_HEADER)
        try:
            lines = raw.decode("iso-8859-1").split("\r\n")
            status = lines[0].split()
            headers = {line.split(":", 1)[0].lower(): line.split(":", 1)[1].strip()
                       for line in lines[1:] if ":" in line}
        except (UnicodeError, IndexError) as exc:
            raise TransportError("invalid WebSocket handshake") from exc
        expected = base64.b64encode(hashlib.sha1(
            (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii")).digest()).decode("ascii")
        if status[:2] != ["HTTP/1.1", "101"] or headers.get("upgrade", "").lower() != "websocket" or \
                "upgrade" not in headers.get("connection", "").lower() or headers.get("sec-websocket-accept") != expected:
            raise TransportError("WebSocket handshake rejected")

    def _send_json(self, value: dict[str, Any], deadline: float | None = None, expected: socket.socket | None = None) -> None:
        try:
            encoded = json.dumps(value, separators=(",", ":")).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise TransportError("RPC message is not JSON serializable") from exc
        if len(encoded) > _MAX_MESSAGE:
            raise TransportError("RPC message exceeds size limit")
        self._send_frame(0x1, encoded, deadline, expected)

    def _receive_json(self, deadline: float | None = None, expected: socket.socket | None = None) -> dict[str, Any] | None:
        payload = self._receive_message(deadline, expected)
        if payload is None:
            return None
        try:
            message = json.loads(payload.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise TransportError("invalid JSON-RPC message") from exc
        if not isinstance(message, dict):
            raise TransportError("JSON-RPC message must be an object")
        return message

    def _reject_server_request(self, request: dict[str, Any], deadline: float, expected: socket.socket | None = None) -> None:
        self._send_json({"id": request["id"], "error": {"code": -32601, "message": "Method not supported"}}, deadline, expected)

    def _send_frame(self, opcode: int, payload: bytes, deadline: float | None = None, expected: socket.socket | None = None) -> None:
        deadline = time.monotonic() + self.timeout if deadline is None else deadline
        if not self._write_lock.acquire(timeout=max(0, deadline - time.monotonic())):
            raise TransportError('frame write timed out waiting for writer')
        try:
            self._send_frame_locked(opcode, payload, deadline, expected)
        finally:
            self._write_lock.release()

    def _send_frame_locked(self, opcode: int, payload: bytes, deadline: float | None = None, expected: socket.socket | None = None) -> None:
        if len(payload) > _MAX_MESSAGE:
            raise TransportError("WebSocket frame exceeds size limit")
        length = len(payload)
        header = bytearray([0x80 | opcode])
        if length < 126:
            header.append(0x80 | length)
        elif length <= 0xffff:
            header.extend((0x80 | 126, *struct.pack("!H", length)))
        else:
            header.append(0x80 | 127)
            header.extend(struct.pack("!Q", length))
        mask = os.urandom(4)
        masked = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
        self._send_bytes(bytes(header) + mask + masked, deadline, expected)

    def _receive_message(self, deadline: float | None = None, expected: socket.socket | None = None) -> bytes | None:
        sock = self._sock if expected is None else expected
        if sock is None:
            raise TransportError("not connected")
        parts: list[bytes] = []
        opcode: int | None = None
        total = 0
        while True:
            first, second = self._read_exact(2, deadline, sock)
            fin, frame_opcode = bool(first & 0x80), first & 0x0f
            if first & 0x70:
                raise TransportError("WebSocket extensions are not supported")
            masked = bool(second & 0x80)
            if masked:
                raise TransportError("server WebSocket frames must not be masked")
            size = second & 0x7f
            if size == 126:
                size = struct.unpack("!H", self._read_exact(2, deadline, sock))[0]
            elif size == 127:
                size = struct.unpack("!Q", self._read_exact(8, deadline, sock))[0]
                if size >> 63:
                    raise TransportError("invalid WebSocket frame length")
            if size > _MAX_MESSAGE:
                raise TransportError("WebSocket frame exceeds size limit")
            if frame_opcode < 8 and total + size > _MAX_MESSAGE:
                raise TransportError("WebSocket message exceeds size limit")
            payload = self._read_exact(size, deadline, sock)
            if frame_opcode >= 8:
                if not fin or size > 125:
                    raise TransportError("invalid WebSocket control frame")
                if frame_opcode == 0x8:
                    self._send_frame(0x8, payload[:125], deadline, sock)
                    raise TransportError("WebSocket closed by server")
                if frame_opcode == 0x9:
                    self._send_frame(0xA, payload, deadline, sock)
                elif frame_opcode != 0xA:
                    raise TransportError("unsupported WebSocket opcode")
                continue
            if frame_opcode == 0x1:
                if opcode is not None:
                    raise TransportError("unexpected text frame")
                opcode = frame_opcode
            elif frame_opcode == 0x0:
                if opcode is None:
                    raise TransportError("unexpected continuation frame")
            else:
                raise TransportError("unsupported WebSocket opcode")
            total += size
            parts.append(payload)
            if fin:
                return b"".join(parts)

    def _send_bytes(self, data: bytes, deadline: float | None = None, expected: socket.socket | None = None) -> None:
        sock = self._sock if expected is None else expected
        if sock is None:
            raise TransportError("not connected")
        deadline = time.monotonic() + self.timeout if deadline is None else deadline
        try:
            remaining = memoryview(data)
            while remaining:
                self._wait_socket(sock, False, deadline)
                try:
                    sent = sock.send(remaining)
                except BlockingIOError:
                    continue
                if self._sock is not sock:
                    raise TransportError("connection changed")
                if not sent:
                    raise TransportError("connection closed")
                remaining = remaining[sent:]
        except OSError as exc:
            raise TransportError(str(exc)) from exc

    def _read_exact(self, size: int, deadline: float | None = None, expected: socket.socket | None = None) -> bytes:
        sock = self._sock if expected is None else expected
        if sock is None:
            raise TransportError("not connected")
        if self._sock is not sock:
            raise TransportError("connection changed")
        deadline = time.monotonic() + self.timeout if deadline is None else deadline
        chunks = bytearray()
        try:
            while len(chunks) < size:
                self._wait_socket(sock, True, deadline)
                try:
                    chunk = sock.recv(size - len(chunks))
                except BlockingIOError:
                    continue
                if self._sock is not sock:
                    raise TransportError("connection changed")
                if not chunk:
                    raise TransportError("connection closed")
                chunks.extend(chunk)
            return bytes(chunks)
        except socket.timeout as exc:
            raise TransportError("socket timed out") from exc
        except OSError as exc:
            raise TransportError(str(exc)) from exc

    def _set_deadline(self, deadline: float | None) -> None:
        if deadline is not None and deadline <= time.monotonic():
            raise TransportError("request timed out")

    def _wait_socket(self, sock: socket.socket, read: bool, deadline: float) -> None:
        if self._sock is not sock:
            raise TransportError("connection changed")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TransportError("request timed out")
        try:
            ready = select.select([sock] if read else [], [] if read else [sock], [], remaining)
        except (OSError, ValueError) as exc:
            raise TransportError("connection closed") from exc
        if self._sock is not sock:
            raise TransportError("connection changed")
        if not (ready[0] if read else ready[1]):
            raise TransportError("request timed out")

    def _read_until(self, marker: bytes, limit: int) -> bytes:
        data = bytearray()
        deadline = time.monotonic() + self.timeout
        sock = self._sock
        if sock is None:
            raise TransportError("not connected")
        while marker not in data:
            if len(data) >= limit:
                raise TransportError("WebSocket headers exceed size limit")
            data.extend(self._read_exact(1, deadline, sock))
        return bytes(data)
