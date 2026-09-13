"""Small, dependency-free WebSocket transport for the local Codex app-server."""

from __future__ import annotations

import base64
import hashlib
import json
import math
import os
import socket
import struct
import time
from typing import Any
from urllib.parse import urlsplit


_MAX_MESSAGE = 16 * 1024 * 1024
_MAX_HEADER = 64 * 1024


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

    def __init__(self, endpoint: str, timeout: float = 10) -> None:
        if not isinstance(timeout, (int, float)) or timeout <= 0:
            raise ValueError("timeout must be positive")
        self.endpoint = endpoint
        self.timeout = float(timeout)
        if not math.isfinite(self.timeout):
            raise ValueError("timeout must be finite")
        self._host, self._port, self._target = self._parse_endpoint()
        self._sock: socket.socket | None = None
        self._next_id = 1

    def __enter__(self) -> "RpcClient":
        return self.connect()

    def __exit__(self, *_: object) -> None:
        self.close()

    def connect(self) -> "RpcClient":
        if self._sock is not None:
            return self
        host, port, target = self._host, self._port, self._target
        try:
            addresses = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
            if not addresses or any(not self._is_loopback(a[4][0]) for a in addresses):
                raise TransportError("endpoint does not resolve solely to loopback")
            family, socktype, proto, _, address = addresses[0]
            sock = socket.socket(family, socktype, proto)
            sock.settimeout(self.timeout)
            self._sock = sock
            sock.connect(address)
            self._handshake(host, target)
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
        sock = self._sock
        if sock is None:
            return
        try:
            self._send_frame(0x8, b"\x03\xe8")
        except (OSError, TransportError):
            pass
        finally:
            self._sock = None
            try:
                sock.close()
            except OSError:
                pass

    def request(self, method: str, params: Any = None) -> Any:
        if self._sock is None:
            raise TransportError("not connected")
        request_id = self._next_id
        self._next_id += 1
        message: dict[str, Any] = {"id": request_id, "method": method}
        if params is not None:
            message["params"] = params
        deadline = time.monotonic() + self.timeout
        try:
            self._send_json(message, deadline)
            while True:
                response = self._receive_json(deadline)
                if response is None:
                    continue
                if "method" in response and "id" in response:
                    self._reject_server_request(response, deadline)
                    continue
                if response.get("id") != request_id:
                    continue
                if "error" in response:
                    error = response["error"]
                    if not isinstance(error, dict):
                        raise TransportError("malformed RPC error")
                    raise RpcError(error.get("code"), str(error.get("message", "")), error.get("data"))
                if "result" not in response:
                    raise TransportError("malformed RPC response")
                return response["result"]
        finally:
            if self._sock is not None:
                self._sock.settimeout(self.timeout)

    def _initialize(self) -> None:
        self.request("initialize", {
            "clientInfo": {"name": "agent_chat_bridge", "version": "0.1.0"},
            "capabilities": {"experimentalApi": True},
        })
        self._send_json({"method": "initialized", "params": {}})

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

    def _send_json(self, value: dict[str, Any], deadline: float | None = None) -> None:
        try:
            encoded = json.dumps(value, separators=(",", ":")).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise TransportError("RPC message is not JSON serializable") from exc
        if len(encoded) > _MAX_MESSAGE:
            raise TransportError("RPC message exceeds size limit")
        self._send_frame(0x1, encoded, deadline)

    def _receive_json(self, deadline: float | None = None) -> dict[str, Any] | None:
        payload = self._receive_message(deadline)
        if payload is None:
            return None
        try:
            message = json.loads(payload.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise TransportError("invalid JSON-RPC message") from exc
        if not isinstance(message, dict):
            raise TransportError("JSON-RPC message must be an object")
        return message

    def _reject_server_request(self, request: dict[str, Any], deadline: float) -> None:
        self._send_json({"id": request["id"], "error": {"code": -32601, "message": "Method not supported"}}, deadline)

    def _send_frame(self, opcode: int, payload: bytes, deadline: float | None = None) -> None:
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
        self._send_bytes(bytes(header) + mask + masked, deadline)

    def _receive_message(self, deadline: float | None = None) -> bytes | None:
        parts: list[bytes] = []
        opcode: int | None = None
        total = 0
        while True:
            first, second = self._read_exact(2, deadline)
            fin, frame_opcode = bool(first & 0x80), first & 0x0f
            if first & 0x70:
                raise TransportError("WebSocket extensions are not supported")
            masked = bool(second & 0x80)
            if masked:
                raise TransportError("server WebSocket frames must not be masked")
            size = second & 0x7f
            if size == 126:
                size = struct.unpack("!H", self._read_exact(2, deadline))[0]
            elif size == 127:
                size = struct.unpack("!Q", self._read_exact(8, deadline))[0]
                if size >> 63:
                    raise TransportError("invalid WebSocket frame length")
            if size > _MAX_MESSAGE:
                raise TransportError("WebSocket frame exceeds size limit")
            if frame_opcode < 8 and total + size > _MAX_MESSAGE:
                raise TransportError("WebSocket message exceeds size limit")
            payload = self._read_exact(size, deadline)
            if frame_opcode >= 8:
                if not fin or size > 125:
                    raise TransportError("invalid WebSocket control frame")
                if frame_opcode == 0x8:
                    self._send_frame(0x8, payload[:125], deadline)
                    raise TransportError("WebSocket closed by server")
                if frame_opcode == 0x9:
                    self._send_frame(0xA, payload, deadline)
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

    def _send_bytes(self, data: bytes, deadline: float | None = None) -> None:
        if self._sock is None:
            raise TransportError("not connected")
        try:
            self._set_deadline(deadline)
            self._sock.sendall(data)
        except OSError as exc:
            raise TransportError(str(exc)) from exc

    def _read_exact(self, size: int, deadline: float | None = None) -> bytes:
        if self._sock is None:
            raise TransportError("not connected")
        chunks = bytearray()
        try:
            while len(chunks) < size:
                self._set_deadline(deadline)
                chunk = self._sock.recv(size - len(chunks))
                if not chunk:
                    raise TransportError("connection closed")
                chunks.extend(chunk)
            return bytes(chunks)
        except socket.timeout as exc:
            raise TransportError("socket timed out") from exc
        except OSError as exc:
            raise TransportError(str(exc)) from exc

    def _set_deadline(self, deadline: float | None) -> None:
        if deadline is None or self._sock is None:
            return
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TransportError("request timed out")
        self._sock.settimeout(remaining)

    def _read_until(self, marker: bytes, limit: int) -> bytes:
        data = bytearray()
        while marker not in data:
            if len(data) >= limit:
                raise TransportError("WebSocket headers exceed size limit")
            data.extend(self._read_exact(1))
        return bytes(data)
