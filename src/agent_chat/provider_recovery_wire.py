"""Bounded local WebSocket JSON-RPC; jq performs ALL JSON operations."""
import base64
import hashlib
import os
import socket
import struct
import subprocess
import time


def jq(raw, query, **values):
    command = ['jq', '-ceS', query]
    for key, value in values.items():
        command.extend(['--arg', key, str(value)])
    answer = subprocess.run(command, input=raw, text=True, capture_output=True, timeout=3)
    if answer.returncode:
        raise ValueError('jq validation failed: ' + answer.stderr.strip()[:200])
    return answer.stdout.strip()


def field(raw, query):
    answer = subprocess.run(['jq', '-er', query], input=raw, text=True, capture_output=True, timeout=3)
    if answer.returncode:
        raise ValueError('missing/invalid JSON field: ' + query)
    return answer.stdout.strip()


def exact(sock, count, deadline=None):
    result = b''
    while len(result) < count:
        if deadline is not None:
            left = deadline - time.monotonic()
            if left <= 0: raise TimeoutError('recovery absolute read deadline exhausted')
            sock.settimeout(left)
        chunk = sock.recv(count - len(result))
        if not chunk:
            raise OSError('socket closed')
        result += chunk
    return result


def send_frame(sock, raw, mask=False, opcode=1):
    body = raw.encode()
    size = len(body)
    if size > 4 * 1024 * 1024:
        raise ValueError('bounded frame exceeded')
    prefix = bytes([0x80 | opcode, (0x80 if mask else 0) | (size if size < 126 else 126 if size < 65536 else 127)])
    if size >= 126:
        prefix += struct.pack('!H' if size < 65536 else '!Q', size)
    if mask:
        key = os.urandom(4)
        prefix += key
        body = bytes(value ^ key[index % 4] for index, value in enumerate(body))
    sock.sendall(prefix + body)


def receive_frame(sock, deadline=None):
    first, second = exact(sock, 2, deadline)
    if not first & 0x80 or first & 0x70 or first & 15 not in (1, 8):
        raise ValueError('unsupported WebSocket frame; no continuation inferred')
    if first & 15 == 8:
        raise OSError('peer closed')
    size = second & 127
    if size in (126, 127):
        size = struct.unpack('!H' if size == 126 else '!Q', exact(sock, 2 if size == 126 else 8, deadline))[0]
    if size > 4 * 1024 * 1024:
        raise ValueError('bounded response exceeded')
    key = exact(sock, 4, deadline) if second & 128 else None
    body = exact(sock, size, deadline)
    if key:
        body = bytes(value ^ key[index % 4] for index, value in enumerate(body))
    return body.decode()


def header(sock, deadline=None):
    data = b''
    while not data.endswith(b'\r\n\r\n'):
        if len(data) >= 8192:
            raise ValueError('header exceeds bound')
        data += exact(sock, 1, deadline)
    return data.decode('ascii')


class Wire:
    def __init__(self, peer, deadline):
        self.deadline = deadline
        self.sock = socket.create_connection(peer, timeout=self.remaining())
        self.serial = 0
        self.peer = peer

    def remaining(self):
        left = self.deadline - time.monotonic()
        if left <= 0:
            raise TimeoutError('120-second recovery deadline exhausted')
        return left

    def initialize(self):
        key = base64.b64encode(os.urandom(16)).decode()
        self.sock.settimeout(self.remaining())
        self.sock.sendall(('GET / HTTP/1.1\r\nHost: %s:%s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n'
                          'Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: %s\r\n\r\n' % (*self.peer, key)).encode())
        response = header(self.sock, self.deadline)
        accept = base64.b64encode(hashlib.sha1((key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest()).decode()
        lines = response.split('\r\n')
        values = [line.split(':', 1)[1].strip() for line in lines[1:]
                  if ':' in line and line.split(':', 1)[0].lower() == 'sec-websocket-accept']
        if not lines[0].startswith('HTTP/1.1 101 ') or values != [accept]:
            raise ValueError('WebSocket handshake mismatch')
        self.request('initialize', '{"clientInfo":{"name":"agent-chat-recovery-private","version":"2"},"capabilities":{"experimentalApi":true}}')
        send_frame(self.sock, '{"method":"initialized","params":{}}', mask=True)

    def request(self, method, params):
        self.serial += 1
        body = jq(params, '{id:($id|tonumber),method:$method,params:.}', id=self.serial, method=method)
        self.sock.settimeout(self.remaining())
        send_frame(self.sock, body, mask=True)
        for _ in range(64):
            self.sock.settimeout(self.remaining())
            raw = receive_frame(self.sock, self.deadline)
            matched = field(raw, 'if .id == ' + str(self.serial) + ' then "matched" else "other" end')
            if matched == 'other':
                continue
            return jq(raw, 'if has("error") then error("native RPC error: " + (.error|tostring)) else .result end')
        raise ValueError('notification bound exhausted')

    def close(self):
        self.sock.close()
