"""Goal-only managed peer selection; connect-only is not child ownership.

Capacity admission deliberately retains its separate owned-child contract.
No signal, process launch, native input or trust-pin update occurs here.
"""
import os
from pathlib import Path
import pwd
import re
import shlex
import subprocess
import time

from . import capacity_observer, native_owner_evidence, root_local, service
from .client_continuity import ClientContinuity


class _ManagedConnection:
    """Read the existing managed client's exact kernel socket, not an RPC claim."""
    def __init__(self, kernel, client, link, peer):
        self.kernel, self.client, self.link, self.peer = kernel, client, link, peer

    def connection_identity(self):
        rows = self.kernel.lsof('-p', str(self.client.pid), '-iTCP', '-sTCP:ESTABLISHED')
        selected = [row for row in rows if row[0] == self.client.pid
                    and row[2].endswith('->%s:%s' % self.peer)]
        if selected != [self.link] or self.kernel.process(self.client.pid) != self.client:
            raise capacity_observer.Blocked('managed client socket or generation changed')
        match = re.fullmatch(r'127\.0\.0\.1:([0-9]+)->127\.0\.0\.1:([0-9]+)', self.link[2])
        if (match is None or not 0 < int(match[1]) < 65536
                or int(match[2]) != self.peer[1] or not str(self.link[1]).isdigit()):
            raise capacity_observer.Blocked('managed client exact socket unavailable')
        return {'pid': self.client.pid, 'fd': int(self.link[1]),
                'local': ('127.0.0.1', int(match[1])), 'peer': self.peer}


def selected_goal_client(registration, kernel):
    """Select the configured mode, positively prove its peer, then recheck it."""
    blocked = capacity_observer.Blocked
    config = service._installed_server_config()
    if os.getuid() != config.uid or os.geteuid() != config.uid:
        raise blocked('managed client UID differs from configured Root')
    manager = service.ServiceManager(system='Darwin', home=Path(pwd.getpwuid(config.uid).pw_dir),
                                     server_bin=str(config.executable))
    paths = manager.paths('client')
    continuity = ClientContinuity(manager)  # construction does not open its journal
    wrapper = continuity._file(paths['wrapper'])
    lines = [line[5:] for line in wrapper.decode().splitlines() if line.startswith('exec ')]
    command = shlex.split(lines[0]) if len(lines) == 1 else []
    if not command:
        raise blocked('managed client command unavailable')
    if '--connect-only' not in command:
        # No independent-listener fallback if the configured owned mode fails.
        result = capacity_observer._selected_client(registration, kernel)
        if continuity._file(paths['wrapper']) != wrapper:
            raise blocked('managed client mode changed')
        return result

    digest, configured = continuity._config()
    if configured != command or len(command) < 6 or command[2] != 'bridge':
        raise blocked('configured connect-only bridge command differs')
    if command.count('--codex-server') != 1:
        raise blocked('configured native endpoint unavailable or ambiguous')
    index = command.index('--codex-server')
    if index + 1 >= len(command) or command[index + 1] != registration['endpoint']:
        raise blocked('configured native endpoint differs from registration')
    trusted = tuple(root_local._file(paths[key], config.uid) for key in ('config', 'wrapper'))
    fields = lambda p: (p.stat().st_dev, p.stat().st_ino, p.stat().st_uid,
                         p.stat().st_mode, p.stat().st_size, p.stat().st_mtime_ns)
    file_ids = tuple(map(fields, trusted))
    label = service.LABEL + '.client'

    def launched_pid():
        proc = subprocess.run(['/bin/launchctl', 'print', 'gui/' + str(config.uid) + '/' + label],
                              capture_output=True, text=True, timeout=3)
        if proc.returncode or proc.stderr:
            raise blocked('managed client launchd state unavailable')
        def unique(field):
            values = re.findall(r'^\s*' + field + r' = (.+?)\s*$', proc.stdout, re.MULTILINE)
            if len(values) != 1:
                raise blocked('managed client launchd field unavailable or ambiguous')
            return values[0]
        value = unique('pid')
        if (not value.isdigit() or int(value) <= 1 or int(value) != registration['pid']
                or unique('path') != str(paths['config']) or unique('program') != str(paths['wrapper'])):
            raise blocked('loaded managed client configuration or PID differs')
        return int(value)

    client = kernel.process(launched_pid())
    if (client.pid != registration['pid'] or client.uid != config.uid or client.ruid != config.uid
            or client.birth_sec <= 0 or not 0 <= time.time() - registration['heartbeat'] <= 15
            or registration['heartbeat'] < client.birth_sec + client.birth_usec / 1e6):
        raise blocked('managed client UID, birth or heartbeat differs')
    image, actual = continuity._kernel_command(client.pid)
    qualified = continuity._qualified_image(command[0], image)
    if (len(actual) != len(command) or actual[1:] != command[1:]
            or Path(actual[0]).resolve(strict=True) != Path(image).resolve(strict=True)
            or Path(client.image).resolve(strict=True) != Path(image).resolve(strict=True)):
        raise blocked('running client arguments or image differ from configuration')
    peer = native_owner_evidence.endpoint(registration['endpoint'])
    links = [row for row in kernel.lsof('-p', str(client.pid), '-iTCP', '-sTCP:ESTABLISHED')
             if row[0] == client.pid and row[2].endswith('->%s:%s' % peer)]
    if len(links) != 1:
        raise blocked('managed client native connection missing or ambiguous')
    connection = _ManagedConnection(kernel, client, links[0], peer)
    connection.connection_identity()
    listeners = kernel.lsof('-iTCP@127.0.0.1:' + str(peer[1]), '-sTCP:LISTEN')
    if len(listeners) != 1 or listeners[0][2] != '%s:%s' % peer:
        raise blocked('selected native listener unavailable or ambiguous')
    native = kernel.process(listeners[0][0])
    if native.pid != listeners[0][0] or native.birth_sec <= 0:
        raise blocked('selected native PID or birth unavailable')
    sample = kernel.sample_independent_listener(
        registration['endpoint'], native.pid, (native.birth_sec, native.birth_usec),
        connection, native_owner_evidence.IMAGE_SHA)
    if (kernel.process(client.pid) != client or launched_pid() != client.pid
            or continuity._kernel_command(client.pid) != (image, actual)
            or continuity._qualified_image(command[0], image) != qualified
            or continuity._config() != (digest, command)
            or tuple(map(fields, trusted)) != file_ids):
        raise blocked('managed client configuration or generation changed during peer verification')
    return client, links[0], sample, digest, (file_ids, qualified)
