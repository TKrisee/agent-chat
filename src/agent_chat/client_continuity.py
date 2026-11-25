"""One journaled SIGTERM to the configured connect-only launchd client.

No provider RPC, takeover, forced termination, lease reset or input replay. A
new service process is not evidence of RPC readiness or native/approval closure.
"""
import ctypes as C
import hashlib
import math
import os
from pathlib import Path
import plistlib
import re
import shlex
import sqlite3
import stat
import subprocess
import struct
import time
import uuid

from .service import ServiceError, LABEL


class ClientContinuity:
    def __init__(self, manager):
        self.manager = manager
        if manager._platform() != 'launchd':
            raise ServiceError('graceful client continuity currently requires launchd')
        self.target = 'gui/' + str(os.getuid()) + '/' + LABEL + '.client'
        self.journal = manager.home / '.local/state/agent-chat/client-continuity/requests.sqlite3'

    def _file(self, path):
        info = path.lstat()
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or
                stat.S_IMODE(info.st_mode) & 0o022):
            raise ServiceError('client service file must be owned, regular and not writable by others')
        return path.read_bytes()

    def _config(self):
        paths = self.manager.paths('client')
        raw = self._file(paths['config'])
        wrapper = self._file(paths['wrapper'])
        try:
            config = plistlib.loads(raw)
            lines = [line[5:] for line in wrapper.decode().splitlines() if line.startswith('exec ')]
            command = shlex.split(lines[0]) if len(lines) == 1 else []
        except (ValueError, UnicodeError, plistlib.InvalidFileException) as error:
            raise ServiceError('unreadable client service configuration') from error
        if (not isinstance(config, dict) or config.get('Label') != LABEL+'.client' or
                config.get('ProgramArguments') != [str(paths['wrapper'])] or
                config.get('KeepAlive') is not True or command.count('--connect-only') != 1):
            raise ServiceError('graceful restart requires the exact configured KeepAlive connect-only client')
        return hashlib.sha256(raw+b'\0'+wrapper).hexdigest(), command

    def _run(self, command):
        return self.manager.run(command, check=False, capture_output=True, text=True, timeout=5)

    def _process(self, pid):
        result = self._run(['ps','-p',str(pid),'-o','uid=','-o','lstart='])
        if result.stderr.strip():
            raise ServiceError('process generation observation unavailable: ' + result.stderr.strip()[:300])
        if result.returncode == 1 and not result.stdout.strip():
            return None
        fields = result.stdout.split()
        if result.returncode or len(fields) != 6 or not fields[0].isdigit():
            raise ServiceError('process generation observation unavailable')
        return dict(pid=pid, uid=int(fields[0]), start=' '.join(fields[1:]))

    def _service(self):
        result = self._run(['launchctl','print',self.target])
        if result.returncode:
            raise ServiceError('configured client service observation unavailable')
        pids = re.findall(r'^\s*pid = (\d+)\s*$', result.stdout, re.M)
        runs = re.findall(r'^\s*runs = (\d+)\s*$', result.stdout, re.M)
        if len(pids) > 1 or len(runs) != 1:
            raise ServiceError('ambiguous client service generation')
        return dict(pid=int(pids[0]) if pids else None, runs=int(runs[0]))

    @staticmethod
    def _decode_argv(raw):
        # Darwin KERN_PROCARGS2: argc, exec-path NUL, padding, argc NUL strings.
        # Stop after argv; never decode, return or log environment data.
        if len(raw) < 5:
            raise ServiceError('kernel client arguments are truncated')
        argc = struct.unpack_from('=i', raw)[0]
        if not 1 <= argc <= 4096:
            raise ServiceError('kernel client argument count is invalid')
        offset = raw.find(b'\0', 4)
        if offset < 0:
            raise ServiceError('kernel client executable path is truncated')
        while offset < len(raw) and raw[offset] == 0:
            offset += 1
        result = []
        for _ in range(argc):
            end = raw.find(b'\0', offset)
            if end < 0:
                raise ServiceError('kernel client argument vector is truncated')
            result.append(os.fsdecode(raw[offset:end]))
            offset = end + 1
        return result

    def _kernel_command(self, pid):
        # Local Darwin SDK: CTL_KERN=1, KERN_ARGMAX=8, KERN_PROCARGS2=49.
        # proc_pidpath is independent of user-editable argv[0].
        libc = C.CDLL('/usr/lib/libSystem.B.dylib', use_errno=True)
        libc.sysctl.argtypes = [C.POINTER(C.c_int), C.c_uint, C.c_void_p,
                               C.POINTER(C.c_size_t), C.c_void_p, C.c_size_t]
        maximum = C.c_int()
        size = C.c_size_t(C.sizeof(maximum))
        if libc.sysctl((C.c_int*2)(1, 8), 2, C.byref(maximum), C.byref(size), None, 0):
            raise ServiceError('kernel client argument limit unavailable')
        if not 4 < maximum.value <= 2*1024*1024:
            raise ServiceError('unsupported kernel client argument limit')
        buffer = C.create_string_buffer(maximum.value + 4)
        size = C.c_size_t(len(buffer))
        if libc.sysctl((C.c_int*3)(1, 49, pid), 3, buffer, C.byref(size), None, 0):
            raise ServiceError('kernel client arguments unavailable (errno '+str(C.get_errno())+')')
        if not 4 < size.value <= len(buffer):
            raise ServiceError('kernel client argument response length is invalid')
        argv = self._decode_argv(buffer.raw[:size.value])
        libproc = C.CDLL('/usr/lib/libproc.dylib', use_errno=True)
        libproc.proc_pidpath.argtypes = [C.c_int, C.c_void_p, C.c_uint32]
        image = C.create_string_buffer(4096)
        if libproc.proc_pidpath(pid, image, len(image)) <= 0:
            raise ServiceError('kernel client executable image unavailable')
        return os.fsdecode(image.value), argv

    @staticmethod
    def _image_file(path):
        path = Path(path).resolve(strict=True)
        info = path.stat()
        if (not stat.S_ISREG(info.st_mode) or info.st_uid not in (0, os.getuid()) or
                stat.S_IMODE(info.st_mode) & 0o022):
            raise ServiceError('configured client executable must be trusted and regular')
        return path, (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns)

    def _qualified_image(self, configured, observed):
        launcher, launcher_info = self._image_file(configured)
        image, image_info = self._image_file(observed)
        if launcher == image:
            return (launcher, launcher_info, image, image_info)
        # CPython Mac/Tools/pythonw.c derives this exact app image from the
        # linked Python framework and replaces argv[0] before POSIX_SPAWN_SETEXEC.
        # Do not accept another interpreter or a matching executable basename.
        framework = launcher.parent.parent
        version = framework.name
        if (launcher.parent.name != 'bin' or framework.parent.name != 'Versions' or
                framework.parent.parent.name != 'Python.framework' or
                not re.fullmatch(r'[0-9]+\.[0-9]+', version) or
                launcher.name != 'python'+version or
                image != (framework/'Resources/Python.app/Contents/MacOS/Python').resolve(strict=True)):
            raise ServiceError('running client executable differs from configured launcher')
        library, library_info = self._image_file(framework/'Python')
        linked = self._run(['/usr/bin/otool', '-L', str(launcher)])
        if linked.returncode or linked.stderr.strip():
            raise ServiceError('configured Python framework linkage unavailable')
        libraries = [line.strip().split(' (compatibility version ', 1)[0]
                     for line in linked.stdout.splitlines()[1:] if ' (compatibility version ' in line]
        if libraries.count(str(library)) != 1:
            raise ServiceError('configured Python launcher does not link the observed image framework')
        return (launcher, launcher_info, image, image_info, library, library_info)

    def inspect(self):
        config = self._config()
        digest, command = config
        observed = self._service()
        process = self._process(observed['pid']) if observed['pid'] else None
        if process:
            if process['uid'] != os.getuid():
                raise ServiceError('client process ownership/generation unavailable')
            image, actual = self._kernel_command(process['pid'])
            qualified = self._qualified_image(command[0], image)
            if len(actual) != len(command):
                raise ServiceError('running client arguments differ: configured/kernel count mismatch')
            if Path(actual[0]).resolve(strict=True) != Path(image).resolve(strict=True):
                raise ServiceError('kernel client argv executable differs from observed image')
            for index, (expected, value) in enumerate(zip(command[1:], actual[1:]), 1):
                if expected != value:
                    raise ServiceError('running client arguments differ at argv index '+str(index))
            if self._process(process['pid']) != process:
                raise ServiceError('client process generation changed during argument observation')
            if (self._kernel_command(process['pid']) != (image, actual) or
                    self._qualified_image(command[0], image) != qualified):
                raise ServiceError('client arguments or executable changed during observation')
        if observed != self._service() or config != self._config():
            raise ServiceError('client service changed during observation; no signal')
        if observed['pid'] and process is None:
            raise ServiceError('client process ownership/generation unavailable')
        return dict(service=self.target, config_sha256=digest, process=process,
                    service_runs=observed['runs'], mode='connect-only',
                    process_command_matches_config=True if process else None,
                    command_observation='darwin_kernel_argv_and_image' if process else None,
                    provider_continuity='not_observed', root_identity_continuity='not_observed',
                    reconnect_observed=None, runtime_ready=None, approval_state='not_inventoried',
                    process_identity_limit='PID/birth observations are not an atomic signal CAS')

    @staticmethod
    def _sync_directory(path):
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def _db(self, create=False):
        if create:
            # This is controller bookkeeping, never a coordinator or worker
            # identity database. Refuse symlink ancestors before creating it.
            for parent in reversed(self.journal.parent.parents):
                if parent.exists() and parent.is_symlink():
                    raise ServiceError('unsafe client continuity journal parent')
            # The configured home must already exist. Create each journal
            # ancestor separately and durably link it in its parent BEFORE
            # proceeding. Sync existing links too: a previous failed fsync may
            # have left a newly created directory visible but not durable.
            parent = self.manager.home
            if not stat.S_ISDIR(parent.lstat().st_mode):
                raise ServiceError('client service home must be an existing directory')
            for part in self.journal.parent.relative_to(parent).parts:
                child = parent / part
                try:
                    child.mkdir(mode=0o700)
                except FileExistsError:
                    pass
                if not stat.S_ISDIR(child.lstat().st_mode):
                    raise ServiceError('unsafe client continuity journal parent')
                self._sync_directory(parent)
                parent = child
            if self.journal.parent.is_symlink():
                raise ServiceError('unsafe client continuity journal parent')
            info = self.journal.parent.stat()
            if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077:
                raise ServiceError('client continuity journal directory must be owner-private')
            fd = os.open(self.journal, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
            os.close(fd)
            self._sync_directory(self.journal.parent)
        if not self.journal.exists():
            raise ServiceError('unknown graceful client request')
        info = self.journal.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077:
            raise ServiceError('client continuity journal must be an owner-private regular file')
        db = sqlite3.connect(self.journal if create else self.journal.resolve().as_uri()+'?mode=ro',
                             uri=not create, timeout=5)
        db.row_factory = sqlite3.Row
        if create:
            db.execute('PRAGMA synchronous=FULL')
            db.execute('CREATE TABLE IF NOT EXISTS requests (request_id TEXT PRIMARY KEY, '
                       'old_pid INTEGER NOT NULL, old_start TEXT NOT NULL, old_runs INTEGER NOT NULL, '
                       'config_sha TEXT NOT NULL, created_at REAL NOT NULL, signal_state TEXT NOT NULL, '
                       'error TEXT, UNIQUE(old_pid,old_start))')
            db.commit()
        return db

    @staticmethod
    def _request_id(value):
        try:
            if str(uuid.UUID(value)) != value: raise ValueError()
        except (ValueError, TypeError, AttributeError):
            raise ServiceError('request-id must be a canonical UUID') from None
        return value

    def _observe(self, request):
        result = dict(request_id=request['request_id'], service=self.target,
            old_process=dict(pid=request['old_pid'], start=request['old_start']),
            signal_state=request['signal_state'], signal_error=request['error'],
            old_client_exited=None, new_process=None, phase='outcome_unknown',
            cleanup_outcome='not_proven_by_process_exit', reconnect_observed=None, runtime_ready=None,
            provider_continuity='not_observed', root_identity_continuity='not_observed',
            approval_state='not_inventoried', no_repeat=True,
            process_identity_limit='PID/birth observations are not an atomic signal CAS')
        try:
            old = self._process(request['old_pid'])
            result['old_client_exited'] = old is None or old['start'] != request['old_start']
            current = self.inspect()
            result['current_config_matches'] = current['config_sha256'] == request['config_sha']
            current_process = current['process']
            if result['old_client_exited']:
                result['phase'] = 'old_client_exited'
                if (result['current_config_matches'] and current_process and
                        current['service_runs'] > request['old_runs'] and
                        (current_process['pid'], current_process['start']) != (request['old_pid'], request['old_start'])):
                    result['new_process'] = current_process
                    result['phase'] = 'new_generation_observed'
            else:
                result['phase'] = 'stop_requested' if request['signal_state']=='sent' else 'outcome_unknown'
        except (ServiceError, OSError, subprocess.SubprocessError) as error:
            result['observation_error'] = str(error)[:500]
        return result

    def status(self, request_id):
        request_id = self._request_id(request_id)
        db = self._db()
        try:
            row = db.execute('SELECT * FROM requests WHERE request_id=?', (request_id,)).fetchone()
            if row is None: raise ServiceError('unknown graceful client request')
            return self._observe(row)
        finally:
            db.close()

    def restart(self, request_id, *, expected_pid, expected_start, expected_config_sha, timeout=30):
        request_id = self._request_id(request_id)
        if (type(expected_pid) is not int or expected_pid <= 0 or not isinstance(expected_start, str) or not expected_start or
                not isinstance(expected_config_sha, str) or not re.fullmatch('[0-9a-f]{64}', expected_config_sha) or
                not math.isfinite(timeout) or not 0 <= timeout <= 60):
            raise ServiceError('exact process/config pins and a 0–60 second timeout are required')
        db = self._db(create=True)
        send = False
        try:
            db.execute('BEGIN IMMEDIATE')
            old = db.execute('SELECT * FROM requests WHERE request_id=?', (request_id,)).fetchone()
            if old:
                if (old['old_pid'],old['old_start'],old['config_sha']) != (expected_pid,expected_start,expected_config_sha):
                    raise ServiceError('graceful request replay conflicts with its immutable pins')
            else:
                current = self.inspect()
                if (not current['process'] or (current['process']['pid'],current['process']['start'],
                        current['config_sha256']) != (expected_pid,expected_start,expected_config_sha)):
                    raise ServiceError('client generation/config differs from supplied pins; no signal')
                # Commit BEFORE the signal. A crash or lost response in the
                # following interval is intentionally never retried.
                try:
                    db.execute('INSERT INTO requests VALUES(?,?,?,?,?,?,?,NULL)',
                        (request_id,expected_pid,expected_start,current['service_runs'],expected_config_sha,time.time(),'intent'))
                except sqlite3.IntegrityError:
                    raise ServiceError('this client generation already has a request; reconcile its existing ID') from None
                send = True
            db.commit()
            if send:
                try:
                    # Recheck after durable intent. Signal the positive PID,
                    # never the moving launchd label or a process group. macOS
                    # provides no PID/birth CAS here: retain that residual limit.
                    check = self.inspect()
                    if (not check['process'] or (check['process']['pid'],check['process']['start'],
                            check['service_runs'],check['config_sha256']) !=
                            (expected_pid,expected_start,current['service_runs'],expected_config_sha)):
                        raise ServiceError('generation changed after intent; signal not issued')
                    immediate = self._process(expected_pid)
                    if immediate != dict(pid=expected_pid, uid=os.getuid(), start=expected_start):
                        raise ServiceError('process changed immediately before signal; signal not issued')
                    answer = self._run(['/bin/kill','-TERM',str(expected_pid)])
                    state, error = ('sent', None) if answer.returncode == 0 else ('uncertain', (answer.stderr or 'signal command failed')[:500])
                except (ServiceError, OSError, subprocess.SubprocessError) as failure:
                    state, error = 'uncertain', str(failure)[:500]
                db.execute('UPDATE requests SET signal_state=?,error=? WHERE request_id=?', (state,error,request_id))
                db.commit()
        finally:
            db.close()
        deadline = time.monotonic()+timeout
        while True:
            result = self.status(request_id)
            if result['phase'] == 'new_generation_observed': return result
            if time.monotonic() >= deadline:
                result['deadline_reached'] = True
                return result
            time.sleep(min(.2, max(0, deadline-time.monotonic())))
