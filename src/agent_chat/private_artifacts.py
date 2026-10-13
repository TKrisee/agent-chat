"""Private reservation capabilities and factual, prospective process evidence."""
import hashlib
import json
import os
from pathlib import Path
import secrets
import stat
import subprocess
import time

if __package__:
    from .core import CoordError
else:
    # core.py remains an explicitly supported standalone entry point.
    from __main__ import CoordError


def artifact_path(kind, identity):
    root = Path(os.environ.get('AGENT_CHAT_STATE_DIR') or
                (Path.home() / '.local' / 'state' / 'agent-chat'))
    root.mkdir(parents=True, exist_ok=True)
    directory = root / kind
    directory.mkdir(mode=0o700, exist_ok=True)
    return directory / (hashlib.sha256(identity.encode()).hexdigest() + '.json')


def _parent(path):
    path = Path(path)
    try:
        fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        info = os.fstat(fd)
        if info.st_uid != os.getuid() or info.st_mode & 0o077:
            os.close(fd)
            raise CoordError('private artifact requires an owner-only directory')
        return fd, path.name
    except OSError:
        raise CoordError('private artifact directory is unavailable') from None


def _check(info):
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or
            info.st_nlink != 1 or stat.S_IMODE(info.st_mode) != 0o600):
        raise CoordError('private artifact must be an owner-only regular file')


def validate_destination(path):
    parent, name = _parent(path)
    try:
        try: _check(os.stat(name, dir_fd=parent, follow_symlinks=False))
        except FileNotFoundError: pass
    finally:
        os.close(parent)


def atomic_capture(path, capability):
    """Replace only a validated private file; never follow a file/parent symlink."""
    parent, name = _parent(path)
    temporary = '.' + secrets.token_hex(16)
    try:
        try:
            _check(os.stat(name, dir_fd=parent, follow_symlinks=False))
        except FileNotFoundError:
            pass
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                     0o600, dir_fd=parent)
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as stream:
                os.fchmod(stream.fileno(), 0o600)
                json.dump(capability, stream, sort_keys=True)
                stream.write('\n')
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, name, src_dir_fd=parent, dst_dir_fd=parent)
            os.fsync(parent)
        finally:
            try:
                os.unlink(temporary, dir_fd=parent)
            except FileNotFoundError:
                pass
    except OSError:
        raise CoordError('private capability capture failed; preserve reservation') from None
    finally:
        os.close(parent)


def read_capability(path):
    parent, name = _parent(path)
    try:
        fd = os.open(name, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW, dir_fd=parent)
        try:
            info = os.fstat(fd)
            _check(info)
            if info.st_size > 65536:
                raise CoordError('private capability is too large')
            with os.fdopen(fd, 'rb', closefd=False) as stream:
                data = stream.read(65537)
        finally:
            os.close(fd)
    except OSError:
        raise CoordError('private capability is unavailable') from None
    finally:
        os.close(parent)
    expression = '''if type == "object" and
        (keys | sort) == ["host_id","project","reservation_id","resource","server","session_id","token"] and
        all(.[]; type == "string" and length > 0 and
            (test("[\\u0000-\\u001f\\u007f]") | not) and (contains("\\\\") | not))
        then [.resource,.session_id,.host_id,.reservation_id,.token,.server,.project] | @tsv
        else error("invalid") end'''
    try:
        parsed = subprocess.run(['jq', '-er', expression], input=data,
                                capture_output=True, check=False)
    except OSError:
        raise CoordError('private capability requires jq') from None
    if parsed.returncode:
        raise CoordError('invalid private capability')
    return dict(zip(('resource', 'session_id', 'host_id', 'reservation_id', 'token',
                     'server', 'project'), parsed.stdout.decode().rstrip('\n').split('\t')))


class ProcessEvidence:
    """Single runner append-only journal. Missing final facts never imply closure."""

    def __init__(self, path, **identity):
        parent, name = _parent(path)
        self.fd = None
        try:
            self.fd = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                              0o600, dir_fd=parent)
            os.fchmod(self.fd, 0o600)
            os.fsync(parent)
        except OSError:
            if self.fd is not None:
                os.close(self.fd)
                self.fd = None
            raise CoordError('process evidence creation failed') from None
        finally:
            os.close(parent)
        self.identity = identity
        self.sequence = 0
        self.path = str(path)
        self.failed = False

    def record(self, event, **facts):
        if self.failed:
            raise CoordError('process evidence retention failed; preserve open guards')
        value = dict(self.identity, event=event, sequence=self.sequence,
                     observed_at=time.time(), **facts)
        data = (json.dumps(value, sort_keys=True) + '\n').encode()
        try:
            while data:
                written = os.write(self.fd, data)
                if written <= 0:
                    raise OSError('short evidence write')
                data = data[written:]
            os.fsync(self.fd)
        except OSError:
            self.failed = True
            raise CoordError('process evidence retention failed; preserve open guards') from None
        self.sequence += 1

    def close(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None
