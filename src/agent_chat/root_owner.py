"""Read-only liveness of an explicitly resumed native Codex CLI owner.

The bridge's subscription is not evidence that the user still has a CLI open.
Only an exact resume UUID and matching local endpoint qualify a process.
"""
import hashlib
import os
from pathlib import Path
import shlex
import subprocess
from urllib.parse import urlsplit

OWNER_FIELDS = ('owner_pid', 'owner_start', 'owner_uid', 'owner_endpoint', 'owner_generation', 'owner_identity')


def endpoint_identity(endpoint):
    try:
        value = urlsplit(endpoint)
        if value.scheme != 'ws' or value.username or value.password or value.query or value.fragment or value.path not in ('', '/'):
            return None
        host = value.hostname
        if host not in ('localhost', '127.0.0.1', '::1') or not value.port:
            return None
        return 'ws://' + ('[' + host + ']' if host == '::1' else host) + ':' + str(value.port)
    except (TypeError, ValueError, AttributeError):
        return None


class RootOwnerMonitor:
    def __init__(self, endpoint, run=subprocess.run):
        self.endpoint = endpoint_identity(endpoint)
        self.run = run

    def command(self, argv):
        return self.run(argv, text=True, capture_output=True, timeout=2,
                        env=dict(os.environ, LC_ALL='C', TZ='UTC'))

    def process(self, pid):
        identity = self.command(['ps', '-p', str(pid), '-o', 'uid=,lstart='])
        if identity.returncode == 1 and not identity.stdout.strip():
            return None
        if identity.returncode != 0:
            raise ValueError('native CLI identity read unavailable')
        fields = identity.stdout.split()
        if len(fields) != 6 or not fields[0].isdigit():
            raise ValueError('native CLI identity read invalid')
        program = self.command(['ps', '-p', str(pid), '-o', 'comm='])
        argv = self.command(['ps', '-ww', '-p', str(pid), '-o', 'args='])
        if program.returncode != 0 or argv.returncode != 0:
            raise ValueError('native CLI command read unavailable')
        after = self.command(['ps', '-p', str(pid), '-o', 'uid=,lstart='])
        if after.returncode != 0 or after.stdout.split() != fields:
            raise ValueError('native CLI identity changed during command read')
        return dict(pid=pid, uid=int(fields[0]), start=' '.join(fields[1:]),
                    program=Path(program.stdout.strip()).name, argv=shlex.split(argv.stdout.strip()))

    def matches(self, process, target):
        if process['uid'] != os.getuid() or process['program'] != 'codex':
            return False
        argv = process['argv']
        # Require one unambiguous explicit resume target, rather than --last,
        # cwd, a process nickname or a thread UUID embedded in prompt text.
        resumes = [index for index, word in enumerate(argv[1:], 1) if word == 'resume']
        if len(resumes) != 1 or resumes[0] + 1 >= len(argv) or argv[resumes[0] + 1] != target['thread_id']:
            return False
        endpoints = []
        for index, word in enumerate(argv[1:], 1):
            if word == '--remote' and index + 1 < len(argv): endpoints.append(argv[index + 1])
            elif word.startswith('--remote='): endpoints.append(word[len('--remote='):])
        return len(endpoints) == 1 and endpoint_identity(endpoints[0]) == self.endpoint

    def proof(self, process, target):
        fields = (str(process['pid']), process['start'], str(process['uid']), self.endpoint,
                  target['session_id'], target['thread_id'], str(target['generation']))
        return dict(state='open', reason=None, owner_pid=process['pid'], owner_start=process['start'],
                    owner_uid=process['uid'], owner_endpoint=self.endpoint, owner_generation=target['generation'],
                    owner_identity=hashlib.sha256('\0'.join(fields).encode()).hexdigest())

    def observe(self, target, receipt=None):
        if not self.endpoint:
            return dict(state='unknown', reason='native CLI owner endpoint is unavailable')
        try:
            if receipt and receipt.get('owner_identity'):
                process = self.process(receipt['owner_pid'])
                if process is None:
                    return dict(state='closed', reason='owning native CLI exited')
                if process['start'] != receipt['owner_start'] or process['uid'] != receipt['owner_uid']:
                    return dict(state='closed', reason='owning native CLI PID was reused')
                if not self.matches(process, target):
                    return dict(state='closed', reason='owning native CLI no longer matches this root')
                proof = self.proof(process, target)
                if proof['owner_identity'] != receipt['owner_identity']:
                    return dict(state='closed', reason='owning native CLI or root generation changed')
                return proof
            result = self.command(['pgrep', '-u', str(os.getuid()), '-x', 'codex'])
            if result.returncode not in (0, 1):
                raise ValueError('native CLI owner discovery unavailable')
            pids = result.stdout.split()
            if any(not pid.isdigit() for pid in pids):
                raise ValueError('native CLI owner discovery invalid')
            matches = []
            for pid in pids:
                process = self.process(int(pid))
                if process and self.matches(process, target): matches.append(process)
            if len(matches) != 1:
                return dict(state='unknown', reason='exact native CLI resume owner is absent or ambiguous')
            return self.proof(matches[0], target)
        except (OSError, subprocess.SubprocessError, ValueError):
            # Never print command lines, environments or raw utility errors.
            return dict(state='unknown', reason='native CLI owner liveness could not be verified')
