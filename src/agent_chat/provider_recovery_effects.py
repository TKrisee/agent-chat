"""PRIVATE candidate: exact provider selection, durable effects, bounded recovery.

No CLI/API registration is supplied. Only an authenticated Root authority may
bind the effectful DarwinLifecycle to a production operation. Selection is not
authorization. No boolean supplied by a caller can establish ready/restored.
Darwin lacks a signal(pid, birth) CAS: the revalidation-to-kill race remains.
"""
from dataclasses import dataclass, astuple
import ctypes
import errno
import hashlib
import os
from pathlib import Path
import plistlib
import re
import select
import signal
import socket
import sqlite3
import stat
import subprocess
import time

from agent_chat.native_owner_evidence import DarwinEvidence, EvidenceError, endpoint, dynamic_disk_association, lsof_records


class Blocked(RuntimeError):
    pass


@dataclass(frozen=True)
class Selection:
    uid: int
    pid: int
    birth_sec: int
    birth_usec: int
    endpoint: str
    image_path: str
    image_sha256: str


@dataclass(frozen=True)
class Outcome:
    request_id: str
    state: str
    reason: str
    effects: str
    ready: bool = False


def validate(selected):
    if (type(selected) is not Selection or type(selected.uid) is not int or selected.uid < 0
            or type(selected.pid) is not int or selected.pid <= 1
            or type(selected.birth_sec) is not int or selected.birth_sec <= 0
            or type(selected.birth_usec) is not int or not 0 <= selected.birth_usec < 1000000
            or not isinstance(selected.image_path, str) or not Path(selected.image_path).is_absolute()
            or not isinstance(selected.image_sha256, str)
            or not re.fullmatch('[a-f0-9]{64}', selected.image_sha256)):
        raise Blocked('invalid exact provider selection')
    try:
        endpoint(selected.endpoint)
    except (ValueError, TypeError) as error:
        raise Blocked('invalid provider endpoint') from error


def observe_selection(selected):
    """Read-only kernel evidence, including a second sample; never authorization."""
    validate(selected)
    try:
        kernel = DarwinEvidence()
        host, port = endpoint(selected.endpoint)
        def listeners():
            return kernel.lsof('-iTCP@' + host + ':' + str(port), '-sTCP:LISTEN')
        first = kernel.process(selected.pid)
        expected = (selected.pid, selected.uid, selected.uid, selected.birth_sec, selected.birth_usec,
                    selected.image_path)
        actual = (first.pid, first.uid, first.ruid, first.birth_sec, first.birth_usec, first.image)
        if actual != expected or selected.uid != os.getuid() or selected.uid != os.geteuid():
            raise Blocked('selected process UID, PID, birth or image differs')
        rows = listeners()
        if len(rows) != 1 or rows[0][0] != selected.pid or rows[0][2] != host + ':' + str(port):
            raise Blocked('selected listener missing, ambiguous or different')
        image = Path(selected.image_path)
        before = image.stat()
        if hashlib.sha256(image.read_bytes()).hexdigest() != selected.image_sha256:
            raise Blocked('selected image bytes differ')
        if kernel.process(selected.pid) != first or listeners() != rows or image.stat() != before:
            raise Blocked('selection changed during revalidation')
    except (EvidenceError, OSError, subprocess.SubprocessError) as error:
        raise Blocked('kernel selection unavailable: ' + str(error)) from error


class Journal:
    def __init__(self, path):
        self.path = Path(path)
        parent = self.path.parent
        info = parent.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o022:
            raise Blocked('journal parent must be a private owned directory')
        try:
            descriptor = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            info = self.path.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077:
                raise Blocked('journal must be a private owned regular file')
        else:
            os.close(descriptor)
            directory = os.open(parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        self.db = sqlite3.connect(self.path, timeout=5)
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('CREATE TABLE IF NOT EXISTS requests ('
                        'id TEXT PRIMARY KEY, uid INTEGER, pid INTEGER, birth_sec INTEGER, birth_usec INTEGER,'
                        'endpoint TEXT, image_path TEXT, image_sha256 TEXT, plan TEXT, state TEXT, reason TEXT, effects TEXT)')
        self.db.execute('CREATE TABLE IF NOT EXISTS observations ('
                        'request_id TEXT, observed_at REAL, old_generation TEXT, listener TEXT)')
        self.db.commit()

    def claim(self, request_id, selected, plan='no-effect-preflight'):
        self.db.execute('BEGIN IMMEDIATE')
        try:
            old = self.db.execute('SELECT * FROM requests WHERE id=?', (request_id,)).fetchone()
            if old:
                if tuple(old[1:8]) != astuple(selected) or old[8] != plan:
                    raise Blocked('request ID already has a different selection')
                result = Outcome(old[0], *old[9:])
                self.db.commit()
                return result
            completed_same=self.db.execute("SELECT id FROM requests WHERE pid=? AND birth_sec=? AND birth_usec=? AND state NOT IN ('blocked_selection','blocked_unsupported') LIMIT 1",(selected.pid,selected.birth_sec,selected.birth_usec)).fetchone()
            if completed_same:raise Blocked('selected generation already has a request '+completed_same[0])
            conflict = self.db.execute("SELECT id FROM requests WHERE (pid=? OR endpoint=?) AND "
                                       "state NOT IN ('blocked_selection','blocked_unsupported','verified','restored_known_local_state_prior_effects_unknown','restored_checkpoint_old_state_unknown','service_lifecycle_reconciled_receipt_settings')",
                                       (selected.pid, selected.endpoint)).fetchone()
            if conflict:
                raise Blocked('selected provider has unresolved request ' + conflict[0])
            self.db.execute('INSERT INTO requests VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',
                            (request_id, *astuple(selected), plan, 'unresolved',
                             'intent committed; reconcile without replay if interrupted', 'none'))
            self.db.commit()
            return None
        except BaseException:
            self.db.rollback()
            raise

    def save(self, request_id, state, reason, effects):
        with self.db:
            self.db.execute('UPDATE requests SET state=?,reason=?,effects=? WHERE id=?',
                            (state, reason, effects, request_id))
        return Outcome(request_id, state, reason, effects)

    def close(self):
        self.db.close()


@dataclass(frozen=True)
class Reconciliation:
    outcome: Outcome
    old_generation: str
    listener: str


def reconcile_private(path, request_id):
    """Observe uncertain effects without issuing TERM/start or clearing fences.

    A newly seen listener is NOT attributed to this request, and no process
    absence observation establishes thread/native-work or restoration completion.
    """
    journal = Journal(path)
    try:
        row = journal.db.execute('SELECT * FROM requests WHERE id=?', (request_id,)).fetchone()
        if row is None:
            raise Blocked('unknown request')
        selected = Selection(*row[1:8])
        outcome = Outcome(row[0], *row[9:])
        kernel = DarwinEvidence()
        ctypes.set_errno(0)
        try:
            process = kernel.process(selected.pid)
        except EvidenceError:
            old = 'old_generation_absent' if ctypes.get_errno() == errno.ESRCH else 'unavailable'
        else:
            same = (process.pid, process.uid, process.ruid, process.birth_sec, process.birth_usec) == (
                selected.pid, selected.uid, selected.uid, selected.birth_sec, selected.birth_usec)
            old = 'old_generation_present' if same else 'old_generation_absent_pid_reused'
        host, port = endpoint(selected.endpoint)
        answer = subprocess.run(['/usr/sbin/lsof', '-nP', '-a', '-iTCP@' + host + ':' + str(port),
                                 '-sTCP:LISTEN', '-F', 'pfn'], capture_output=True, text=True, timeout=3)
        if answer.returncode == 1 and not answer.stdout and not answer.stderr:
            listener = 'empty_selected_listener'
        elif answer.returncode == 0 and not answer.stderr:
            rows = lsof_records(answer.stdout)
            listener = 'observed_unattributed_listener_pids:' + ','.join(str(r[0]) for r in rows)
        else:
            listener = 'unavailable'
        with journal.db:
            journal.db.execute('INSERT INTO observations VALUES(?,?,?,?)',
                               (request_id, time.time(), old, listener))
        return Reconciliation(outcome, old, listener)
    finally:
        journal.close()


def prepare(path, request_id, selected):
    """No-effect preflight for deployments without an authorized lifecycle binding."""
    validate(selected)
    if not isinstance(request_id, str) or not re.fullmatch('[a-zA-Z0-9_-]{1,128}', request_id):
        raise Blocked('invalid request ID')
    journal = Journal(path)
    try:
        previous = journal.claim(request_id, selected)
        if previous:
            return previous
        try:
            observe_selection(selected)
        except Blocked as error:
            return journal.save(request_id, 'blocked_selection', str(error), 'none')
        return journal.save(request_id, 'blocked_unsupported',
                            'No authenticated Root binding for the retained-provider lifecycle adapter; '
                            'no same-Root/history/settings/input restoration verifier is configured.', 'none')
    finally:
        journal.close()


class DarwinLifecycle:
    """Real independent-process TERM and owned Popen replacement primitives.

    An old launcher receipt or launchd ownership is NOT required. Configuration
    must come from the authenticated Root gateway (not supplied user booleans).
    This private candidate has no such gateway and is not exposed in the app.
    The plist contains ProgramArguments, WorkingDirectory, Endpoint, ImageSHA256.
    It is a launch manifest, NOT a launchd job; no service configuration changes.
    """
    def __init__(self, selected, manifest, manifest_sha256):
        validate(selected)
        self.selected = selected
        self.manifest = Path(manifest)
        self.manifest_sha256 = manifest_sha256
        self.watch = None
        self.child = None
        self.closed_observed = False
        self.launch_claimed = False
        self.term_claimed = False
        self._config()

    def _config(self):
        info = self.manifest.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o022:
            raise Blocked('launch manifest must be owned and not writable by others')
        raw = self.manifest.read_bytes()
        if hashlib.sha256(raw).hexdigest() != self.manifest_sha256:
            raise Blocked('configured manifest changed')
        config = plistlib.loads(raw)
        argv = config.get('ProgramArguments')
        if (not isinstance(argv, list) or len(argv) != 4 or argv !=
                [self.selected.image_path, 'app-server', '--listen', self.selected.endpoint]
                or config.get('Endpoint') != self.selected.endpoint
                or config.get('ImageSHA256') != self.selected.image_sha256):
            raise Blocked('manifest does not identify exact provider command/artifact')
        image = Path(argv[0])
        image_info = image.stat()
        if (not stat.S_ISREG(image_info.st_mode) or stat.S_IMODE(image_info.st_mode) & 0o022
                or image_info.st_uid not in (0, os.getuid())
                or hashlib.sha256(image.read_bytes()).hexdigest() != self.selected.image_sha256):
            raise Blocked('configured executable artifact changed')
        cwd = config.get('WorkingDirectory')
        if not isinstance(cwd, str) or not Path(cwd).is_absolute() or not Path(cwd).is_dir():
            raise Blocked('configured working directory unavailable')
        return tuple(argv), cwd

    def plan_identity(self):
        return str(self.manifest.absolute()) + '\n' + self.manifest_sha256

    def arm(self):
        observe_selection(self.selected)
        self._config()
        dynamic_disk_association(self.selected.pid,
                                 (self.selected.birth_sec, self.selected.birth_usec), self.selected.image_path)
        # Kernel NOTE_EXIT binds the observation to the process registered now.
        self.watch = select.kqueue()
        try:
            event = select.kevent(self.selected.pid, filter=select.KQ_FILTER_PROC,
                                 flags=select.KQ_EV_ADD | select.KQ_EV_ONESHOT,
                                 fflags=select.KQ_NOTE_EXIT)
            self.watch.control([event], 0, 0)
            observe_selection(self.selected)
        except BaseException:
            self.watch.close()
            self.watch = None
            raise

    def stop_once(self):
        if self.term_claimed or self.watch is None:
            raise Blocked('TERM already claimed or exit watcher not armed')
        self.term_claimed = True
        self._config()
        observe_selection(self.selected)
        # Positive PID only, never a moving service label or process group.
        # There is no Darwin PID+birth CAS; this residual syscall race is explicit.
        os.kill(self.selected.pid, signal.SIGTERM)

    def closed(self):
        if self.watch is None:
            raise Blocked('generation-specific exit watcher unavailable')
        if not self.closed_observed:
            events = self.watch.control(None, 1, 0)
            self.closed_observed = any(e.ident == self.selected.pid and e.fflags & select.KQ_NOTE_EXIT
                                       and not e.flags & select.KQ_EV_ERROR for e in events)
        if not self.closed_observed:
            return False
        host, port = endpoint(self.selected.endpoint)
        answer = subprocess.run(['/usr/sbin/lsof', '-nP', '-a', '-iTCP@' + host + ':' + str(port),
                                 '-sTCP:LISTEN', '-F', 'pfn'], capture_output=True, text=True, timeout=3)
        # A missing process alone never proves listener/native work closure.
        return answer.returncode == 1 and not answer.stdout and not answer.stderr

    def start_once(self):
        if self.launch_claimed or not self.closed():
            raise Blocked('replacement already claimed or closure unproven')
        self.launch_claimed = True
        argv, cwd = self._config()
        # Preserve inherited provider settings/auth environment, excluding Agent
        # Chat actor tokens; do not synthesize settings or copy databases/history.
        environment = os.environ.copy()
        for key in ('AGENT_CHAT_SESSION', 'AGENT_CHAT_TOKEN', 'AGENT_CHAT_API_TOKEN', 'AGENT_CHAT_DB'):
            environment.pop(key, None)
        self.child = subprocess.Popen(argv, cwd=cwd, env=environment, start_new_session=True,
                                      stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return self.child.pid

    def close(self):
        if self.watch is not None:
            self.watch.close()
            self.watch = None
        # Do not terminate a replacement merely because a command timed out.

    def authenticate_replacement(self, deadline):
        """Prove an actual socket peer belongs to the new exact configured image.

        No thread/native input is submitted. This is process/socket identity,
        never evidence of Root restoration, app authentication or task success.
        """
        if self.child is None or self.child.poll() is not None:
            raise Blocked('owned replacement is not running')
        kernel = DarwinEvidence()
        peer = endpoint(self.selected.endpoint)
        last_error = None
        while time.monotonic() < deadline:
            try:
                process = kernel.process(self.child.pid)
                selected = Selection(self.selected.uid, process.pid, process.birth_sec, process.birth_usec,
                                     self.selected.endpoint, self.selected.image_path, self.selected.image_sha256)
                if (process.pid, process.birth_sec, process.birth_usec) == (
                        self.selected.pid, self.selected.birth_sec, self.selected.birth_usec):
                    raise Blocked('replacement reused old process generation')
                observe_selection(selected)
                dynamic_disk_association(process.pid, (process.birth_sec, process.birth_usec), process.image)
                self._config()
                with socket.create_connection(peer, timeout=min(1, max(.001, deadline - time.monotonic()))) as connection:
                    local = connection.getsockname()
                    name = '%s:%s->%s:%s' % (*peer, *local)
                    rows = kernel.lsof('-p', str(process.pid), '-iTCP', '-sTCP:ESTABLISHED')
                    if len([row for row in rows if row[0] == process.pid and row[2] == name]) != 1:
                        raise Blocked('replacement does not own connected peer')
                    if kernel.process(process.pid) != process or self.child.poll() is not None:
                        raise Blocked('replacement process changed during peer proof')
                return selected
            except (Blocked, EvidenceError, OSError, subprocess.SubprocessError) as error:
                last_error = error
                if self.child.poll() is not None:
                    break
                time.sleep(min(.05, max(0, deadline - time.monotonic())))
        raise Blocked('replacement peer verification unavailable: ' + str(last_error))


def execute_private(path, request_id, selected, lifecycle, *, timeout=120):
    """Private integration seam; not an authorization boundary or public command.

    A successful process replacement is deliberately never reported restored.
    Missing thread/peer/restoration evidence ends in a finite failed verification.
    Caller-provided ready flags and callbacks are not accepted.
    """
    validate(selected)
    if not isinstance(request_id, str) or not re.fullmatch('[a-zA-Z0-9_-]{1,128}', request_id):
        raise Blocked('invalid request ID')
    if type(timeout) not in (int, float) or not 0 < timeout <= 120:
        raise Blocked('deadline must be positive and at most 120 seconds')
    if lifecycle.selected != selected:
        raise Blocked('lifecycle selection differs')
    deadline = time.monotonic() + timeout
    journal = Journal(path)
    try:
        previous = journal.claim(request_id, selected, lifecycle.plan_identity())
        if previous:
            return previous  # Unknown effect: no second TERM or replacement.
        try:
            lifecycle.arm()
            journal.save(request_id, 'stop_intent', 'TERM effect unknown until observed', 'stop_unknown')
            lifecycle.stop_once()
            while not lifecycle.closed():
                if time.monotonic() >= deadline:
                    return journal.save(request_id, 'closure_unproven', 'deadline reached; no replacement', 'stop_unknown')
                time.sleep(min(.05, max(0, deadline - time.monotonic())))
            journal.save(request_id, 'closed', 'exact generation exit and empty selected listener observed', 'old_closed')
            if time.monotonic() >= deadline:
                return journal.save(request_id, 'closed', 'deadline reached before replacement', 'old_closed')
            journal.save(request_id, 'start_intent', 'replacement effect unknown until observed', 'start_unknown')
            pid = lifecycle.start_once()
            journal.save(request_id, 'replacement_started', 'owned replacement PID ' + str(pid), 'replacement_started')
            lifecycle.authenticate_replacement(deadline)
            return journal.save(request_id, 'failed_verification',
                                'owned replacement PID ' + str(pid) + '; configured signed process/socket peer verified; '
                                'same Root/thread/history/settings/input evidence unavailable; '
                                'no ready claim or input replay', 'replacement_started')
        except Exception as error:
            row = journal.db.execute('SELECT state,effects FROM requests WHERE id=?', (request_id,)).fetchone()
            return journal.save(request_id, row[0], str(error), row[1])
    finally:
        journal.close()
        lifecycle.close()
