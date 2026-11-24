"""Private authenticated retained-provider recovery candidate, no installed route.

Old administrative process selection is independent of trusted replacement RPC.
All JSON encoding, parsing, projection and canonicalization use jq exclusively.
"""
from contextlib import closing
import ctypes as C
from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import plistlib
import select
import signal
import sqlite3
import stat
import subprocess
import sys
import time

from . import provider_recovery_effects as v1
from agent_chat import service
from agent_chat.core import assert_original_owner_admission, execution_epoch_native_check
from agent_chat.native_owner_evidence import Bsd, DarwinEvidence, dynamic_disk_association, endpoint
from .provider_recovery_wire import Wire, jq, field

Blocked = v1.Blocked


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def old_selection(uid, pid, seconds, micros, address):
    selected = v1.Selection(uid, pid, seconds, micros, address, '/UNKNOWN', '0' * 64)
    v1.validate(selected)
    return selected


def kernel_identity(pid):
    kernel = DarwinEvidence()
    record = Bsd()
    if kernel.lib.proc_pidinfo(pid, 3, 0, C.byref(record), C.sizeof(record)) != C.sizeof(record) or record.pid != pid:
        raise Blocked('kernel PID/UID/birth unavailable')
    return record.pid, record.uid, record.ruid, record.start_sec, record.start_usec


def selected_kernel(selected):
    expected = (selected.pid, selected.uid, selected.uid, selected.birth_sec, selected.birth_usec)
    if selected.uid != os.getuid() or selected.uid != os.geteuid() or kernel_identity(selected.pid) != expected:
        raise Blocked('selected old UID/PID/birth differs')
    host, port = endpoint(selected.endpoint)
    rows = DarwinEvidence.lsof('-iTCP@' + host + ':' + str(port), '-sTCP:LISTEN')
    if len(rows) != 1 or rows[0][0] != selected.pid or rows[0][2] != host + ':' + str(port):
        raise Blocked('old selected listener differs or is ambiguous')
    if kernel_identity(selected.pid) != expected:
        raise Blocked('old selected generation changed')


def trusted_file(path, uid, *, root_owned=False, contents=True):
    path = Path(path)
    info = path.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_uid not in ((0, uid) if root_owned else (uid,))
            or stat.S_IMODE(info.st_mode) & 0o022):
        raise Blocked('unsafe configured file: ' + str(path))
    return path.read_bytes() if contents else b''


def readonly(path):
    return sqlite3.connect(Path(path).resolve(strict=True).as_uri() + '?mode=ro', uri=True)


class Authority:
    """Same configured local UID/service/Root authority as root-local, plus source provenance.

    Paths are taken from the installed service contract, never from request DB
    arguments. Private tests substitute only the installation descriptor; all
    file/SQL/provenance/connection checks still run. No auth-success callback.
    """
    def __init__(self, source_id):
        self.config = service._installed_server_config()
        config = self.config
        if os.getuid() != config.uid or os.geteuid() != config.uid:
            raise Blocked('configured real/effective UID differs')
        self.artifacts = tuple((str(p), digest(trusted_file(p, config.uid)))
                               for p in (config.plist, config.wrapper, config.executable))
        self.master = service.configured_server_database(config.plist)
        if self.master != config.master.resolve():
            raise Blocked('canonical configured database differs')
        trusted_file(self.master, config.uid, contents=False)
        self.registry = Path(str(self.master) + '.projects.sqlite3')
        trusted_file(self.registry, config.uid, contents=False)
        with closing(readonly(self.registry)) as registry:
            self.root = registry.execute('SELECT home_project,home_session,host_id,thread_id FROM root_config WHERE singleton=1').fetchone()
            owner = registry.execute('SELECT home_project,home_session FROM root_directory_owner WHERE singleton=1').fetchone()
        if not self.root or owner != self.root[:2] or self.root[0] != 'default':
            raise Blocked('canonical Root owner/binding unavailable; v2 supports default HOME only')
        self.source_id = source_id
        with closing(readonly(self.master)) as home:
            assert_original_owner_admission(home, self.root[1])
            execution_epoch_native_check(home, thread=self.root[3], actor=self.root[1])
            binding = home.execute('SELECT b.thread_id,b.parent_session,h.host_id FROM bridge_bindings b '
                'JOIN remote_session_hosts h ON h.session_id=b.session_id WHERE b.session_id=?', (self.root[1],)).fetchone()
            if binding != (self.root[3], None, self.root[2]):
                raise Blocked('canonical Root current thread or host differs')
            source = home.execute('SELECT m.body,m.recipient_session,o.channel FROM messages m '
                'JOIN operator_message_origins o ON o.message_id=m.id AND o.sender_session=m.sender_session '
                'JOIN operator_identities i ON i.session_id=m.sender_session WHERE m.id=?', (source_id,)).fetchone()
            if not source or source[1:] != (self.root[1], 'authenticated_operator_ui'):
                raise Blocked('original authenticated operator source to canonical Root is required')
            self.source_body = source[0]
            self.source_digest = digest(self.source_body.encode())
            unknown = home.execute("SELECT id FROM bridge_jobs WHERE thread_id=? AND status IN "
                                   "('adding','starting','uncertain','dispatching') LIMIT 1", (self.root[3],)).fetchone()
            if unknown:
                raise Blocked('exact Root input effect remains uncertain: ' + unknown[0])
            tables = {row[0] for row in home.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            self.recorded = '{}'
            self.recorded_checked_at = None
            if 'agent_control_observations' in tables:
                row = home.execute('SELECT model,reasoning_effort,cwd,permissions_json,checked_at FROM '
                    'agent_control_observations WHERE target_session=? AND thread_id=?',
                    (self.root[1], self.root[3])).fetchone()
                if row:
                    self.recorded_checked_at = row[4]
                    self.recorded = jq(row[3] or '{}', 'with_entries(select(.value != null))')
                    for name, value in zip(('model', 'reasoningEffort', 'cwd'), row[:3]):
                        if value is not None:
                            self.recorded = jq(self.recorded, '. + {($key):$value}', key=name, value=value)
            for table, query in (
                ('root_native_handoffs', 'SELECT 1 FROM root_native_handoffs WHERE active=1 AND (session_id=? OR thread_id=?)'),
                ('execution_epoch_handoffs', 'SELECT 1 FROM execution_epoch_handoffs WHERE settled_at IS NULL AND (actor=? OR thread=?)'),
                ('root_fresh_gates', 'SELECT 1 FROM root_fresh_gates WHERE active=1 AND (root_session=? OR old_thread_id=?)')):
                if table in tables and home.execute(query, (self.root[1], self.root[3])).fetchone():
                    raise Blocked('retained Root gate/native handoff requires reconciliation: ' + table)

    def check(self):
        current = Authority(self.source_id)
        if (current.root, current.source_digest, current.artifacts, current.recorded, current.recorded_checked_at) != (
                self.root, self.source_digest, self.artifacts, self.recorded, self.recorded_checked_at):
            raise Blocked('Root/service/operator authority changed')


SETTINGS = '["model","modelProvider","cwd","approvalPolicy","approvalsReviewer","reasoningEffort",' \
           '"sandbox","activePermissionProfile","runtimeWorkspaceRoots","multiAgentMode","serviceTier"]'


def settings(raw):
    return jq(raw, 'if (' + SETTINGS + ' - keys | length) != 0 then error("missing effective settings fields") '
              'else with_entries(select(.key as $k | ' + SETTINGS + ' | index($k))) end '
              '| if (.model|type)!="string" or (.modelProvider|type)!="string" or (.cwd|type)!="string" '
              'or (.sandbox|type)!="object" or (.activePermissionProfile|type)!="object" '
              'or (.runtimeWorkspaceRoots|type)!="array" or .approvalPolicy==null or .approvalsReviewer==null '
              'or .reasoningEffort==null or .multiAgentMode==null '
              'then error("incomplete effective settings") else . end')


class Recovery(v1.DarwinLifecycle):
    def __init__(self, selected, manifest, manifest_sha, authority):
        self.authority = authority
        self.baseline = None
        self.proof = None
        self.rpc = None
        super().__init__(selected, manifest, manifest_sha)

    def _config(self):
        raw = trusted_file(self.manifest, os.getuid())
        if digest(raw) != self.manifest_sha256:
            raise Blocked('replacement manifest changed')
        config = plistlib.loads(raw)
        argv = config.get('ProgramArguments')
        if (not isinstance(argv, list) or not argv or any(not isinstance(x, str) or not x for x in argv)
                or config.get('Endpoint') != self.selected.endpoint or '--listen' not in argv
                or argv.count('--listen') != 1 or argv[argv.index('--listen') + 1] != self.selected.endpoint):
            raise Blocked('replacement exact command/endpoint unavailable')
        image = str(Path(config['Image']).resolve(strict=True))
        if str(Path(argv[0]).resolve(strict=True)) != image:
            raise Blocked('replacement executable differs from pinned image')
        if digest(trusted_file(image, os.getuid(), root_owned=True)) != config['ImageSHA256']:
            raise Blocked('replacement image changed')
        artifacts = config.get('Artifacts', {})
        for path, expected in artifacts.items():
            if digest(trusted_file(path, os.getuid(), root_owned=True)) != expected:
                raise Blocked('replacement support artifact changed')
        baseline = trusted_file(config['Baseline'], os.getuid())
        if digest(baseline) != config['BaselineSHA256']:
            raise Blocked('continuity baseline changed')
        # Existing old peer may be untrusted; it cannot establish this baseline.
        # An original authenticated operator source must pin the historical file.
        if 'Continuity baseline SHA256: ' + config['BaselineSHA256'] not in self.authority.source_body:
            raise Blocked('independent authenticated historical continuity baseline unavailable')
        self.baseline = jq(baseline.decode(), 'if (.threadId|type)!="string" or (.history|type)!="array" '
                           'or (.queue|type)!="array" then error("incomplete continuity baseline") else . end')
        if field(self.baseline, '.threadId') != self.authority.root[3]:
            raise Blocked('baseline is for another Root thread')
        settings(jq(self.baseline, '.settings'))
        recorded_pair = '{"baseline":' + jq(self.baseline, '.settings') + ',"recorded":' + self.authority.recorded + '}'
        expected_recorded = jq(recorded_pair, '.recorded as $r | .baseline | with_entries(select(.key as $k | $r|has($k)))')
        if expected_recorded != self.authority.recorded:
            raise Blocked('historical baseline disagrees with available authenticated control observation fields')
        self.baseline_sha = config['BaselineSHA256']
        self.replacement_image, self.replacement_sha = image, config['ImageSHA256']
        self.peers = config['ObservedPeers']
        if {p['Role'] for p in self.peers} != {'App', 'Root'}:
            raise Blocked('exact App and Root connections required')
        cwd = config.get('WorkingDirectory')
        if not isinstance(cwd, str) or not Path(cwd).is_absolute() or not Path(cwd).is_dir():
            raise Blocked('replacement working directory unavailable')
        return tuple(argv), cwd

    def selection(self):
        self.authority.check()
        self._config()
        selected_kernel(self.selected)
        host, port = endpoint(self.selected.endpoint)
        for peer in self.peers:
            expected = (peer['PID'], os.getuid(), os.getuid(), peer['BirthSec'], peer['BirthUsec'])
            if kernel_identity(peer['PID']) != expected:
                raise Blocked('observed App/Root generation differs')
            name = host + ':' + str(peer['LocalPort']) + '->' + host + ':' + str(port)
            rows = DarwinEvidence.lsof('-p', str(peer['PID']), '-iTCP', '-sTCP:ESTABLISHED')
            if len([row for row in rows if row[0] == peer['PID'] and row[2] == name]) != 1:
                raise Blocked('observed App/Root connection differs')
        selected_kernel(self.selected)

    def arm(self):
        self.selection()
        self.watch = select.kqueue()
        try:
            self.watch.control([select.kevent(self.selected.pid, filter=select.KQ_FILTER_PROC,
                               flags=select.KQ_EV_ADD | select.KQ_EV_ONESHOT, fflags=select.KQ_NOTE_EXIT)], 0, 0)
            self.selection()
        except BaseException:
            self.close()
            raise

    def stop_once(self):
        if self.term_claimed or self.watch is None:
            raise Blocked('TERM already claimed or watcher absent')
        self.term_claimed = True
        self.selection()
        if time.monotonic() >= self.deadline:
            raise TimeoutError('deadline before TERM; no signal')
        os.kill(self.selected.pid, signal.SIGTERM)  # Darwin final PID-reuse race remains.

    def pin_peer(self):
        if self.child is None or self.child.poll() is not None:
            raise Blocked('owned replacement exited')
        kernel = DarwinEvidence()
        process = kernel.process(self.child.pid)
        chosen = v1.Selection(os.getuid(), process.pid, process.birth_sec, process.birth_usec,
                              self.selected.endpoint, self.replacement_image, self.replacement_sha)
        v1.observe_selection(chosen)
        code = dynamic_disk_association(process.pid, (process.birth_sec, process.birth_usec), self.replacement_image)
        name = '%s:%s->%s:%s' % (*self.rpc.sock.getpeername(), *self.rpc.sock.getsockname())
        rows = kernel.lsof('-p', str(process.pid), '-iTCP', '-sTCP:ESTABLISHED')
        if len([row for row in rows if row[0] == process.pid and row[2] == name]) != 1:
            raise Blocked('replacement does not own exact RPC connection')
        current = (process, code, self.rpc.sock.getsockname(), self.rpc.sock.getpeername())
        if self.proof is not None and current != self.proof:
            raise Blocked('replacement RPC peer changed')
        self.proof = current
        self._config()
        self.authority.check()

    def read(self, method, params):
        self.pin_peer()
        answer = self.rpc.request(method, params)
        self.pin_peer()
        return answer

    def restored_connections(self):
        """Require the selected existing App/Root actor generations to reconnect.

        The administration request selects the roles; kernel evidence establishes
        PID/birth/socket ownership, not a caller's assertion of reconnection.
        """
        grouped = {}
        for peer in self.peers:
            identity = (peer['PID'], os.getuid(), os.getuid(), peer['BirthSec'], peer['BirthUsec'])
            if kernel_identity(peer['PID']) != identity:
                raise Blocked('selected App/Root actor did not survive recovery')
            grouped[peer['PID']] = grouped.get(peer['PID'], 0) + 1
        host, port = endpoint(self.selected.endpoint)
        suffix = '->' + host + ':' + str(port)
        observation = '%s:%s->%s:%s' % (*self.rpc.sock.getsockname(), *self.rpc.sock.getpeername())
        for pid, count in grouped.items():
            rows = DarwinEvidence.lsof('-p', str(pid), '-iTCP', '-sTCP:ESTABLISHED')
            names = {row[2] for row in rows if row[0] == pid and row[2].endswith(suffix) and row[2] != observation}
            if len(names) < count:
                raise Blocked('selected existing App/Root connections not restored')

    def verify(self, journal, request_id, deadline):
        while True:
            if time.monotonic() >= deadline:
                raise TimeoutError('replacement listener deadline exhausted')
            try:
                self.rpc = Wire(endpoint(self.selected.endpoint), deadline)
                break
            except ConnectionRefusedError:
                if self.child.poll() is not None:
                    raise Blocked('replacement exited before listener')
                time.sleep(.02)
        self.pin_peer()  # Before sending any app-server protocol, including initialize.
        self.rpc.initialize()
        self.pin_peer()
        thread = self.authority.root[3]
        params = jq('{}', '{threadId:$thread,includeTurns:true}', thread=thread)
        before = self.read('thread/read', params)
        history = jq(before, 'if .thread.id != $thread or (.thread.turns|type)!="array" '
                     'then error("exact materialized thread/history missing") else .thread.turns end', thread=thread)
        if history != jq(self.baseline, '.history'):
            raise Blocked('retained Root history differs from authenticated baseline')
        # Same-thread metadata attachment is a native mutation and is journaled.
        # It has no input, settings overrides, thread/start or thread/fork fields.
        journal.save(request_id, 'resume_intent', 'same retained thread metadata attachment unknown', 'resume_unknown')
        observed = self.read('thread/resume', jq('{}', '{threadId:$thread,excludeTurns:true}', thread=thread))
        if field(observed, '.thread.id') != thread:
            raise Blocked('metadata attachment returned another thread')
        if settings(observed) != settings(jq(self.baseline, '.settings')):
            raise Blocked('effective settings differ or are incomplete')
        queue, cursor, seen = [], None, set()
        for _ in range(10):
            params = jq('{}', '{threadId:$thread,limit:100}', thread=thread)
            if cursor is not None:
                params = jq(params, '. + {cursor:$cursor}', cursor=cursor)
            page = self.read('thread/queue/list', params)
            queue.append(jq(page, 'if (.data|type)!="array" then error("queue data unavailable") else .data end'))
            cursor = field(page, 'if .nextCursor == null then "<END>" else .nextCursor end')
            if cursor == '<END>':
                break
            if not cursor or cursor in seen:
                raise Blocked('queue cursor repeated')
            seen.add(cursor)
        else:
            raise Blocked('bounded queue history exceeded')
        actual_queue = jq('[' + ','.join(queue) + ']', 'add')
        if actual_queue != jq(self.baseline, '.queue'):
            raise Blocked('exact Root pending input queue differs; no replay')
        after = self.read('thread/read', jq('{}', '{threadId:$thread,includeTurns:true}', thread=thread))
        final_history = jq(after, 'if .thread.id != $thread or .thread.status.type != "idle" '
                           'or .thread.canAcceptDirectInput != true then error("Root not restored idle/direct") '
                           'else .thread.turns end', thread=thread)
        if final_history != history:
            raise Blocked('Root history changed during verification')
        self.restored_connections()
        self.authority.check()
        if time.monotonic() >= deadline:
            raise TimeoutError('deadline before final verification; no success claim')
        process, code, local, peer = self.proof
        return (time.time(), self.authority.source_id, self.authority.source_digest, *self.authority.root,
                process.pid, process.birth_sec, process.birth_usec, self.replacement_image, self.replacement_sha,
                code, repr(local), repr(peer), digest(history.encode()), digest(settings(observed).encode()),
                digest(actual_queue.encode()), self.authority.recorded,
                jq(self.authority.recorded, SETTINGS + ' - keys'), self.authority.recorded_checked_at,
                self.manifest_sha256, self.baseline_sha)

    def close(self):
        if self.rpc is not None:
            self.rpc.close()
            self.rpc = None
        super().close()


@dataclass(frozen=True)
class Result:
    request_id: str
    state: str
    reason: str
    effects: str
    ready: bool


def recover(request_id, source_id, selected, manifest, manifest_sha, *, timeout=120):
    """Authenticated configured-Root entry. No caller readiness or authority flags."""
    if type(timeout) not in (int, float) or not 0 < timeout <= 120:
        raise Blocked('positive at-most-120-second budget required')
    deadline = time.monotonic() + timeout
    v1.validate(selected)
    authority = Authority(source_id)
    adapter = Recovery(selected, manifest, manifest_sha, authority)
    # The configured store owns the journal; no caller-provided database path.
    directory = authority.master.parent / 'provider-recovery'
    directory.mkdir(mode=0o700, exist_ok=True)
    journal = v1.Journal(directory / 'requests.sqlite3')
    journal.db.execute('CREATE TABLE IF NOT EXISTS verification_proofs ('
        'request_id TEXT PRIMARY KEY,verified_at REAL,source_id TEXT,source_sha TEXT,home_project TEXT,'
        'root_session TEXT,host TEXT,thread_id TEXT,provider_pid INTEGER,birth_sec INTEGER,birth_usec INTEGER,'
        'image_path TEXT,image_sha TEXT,code_hash TEXT,connection_local TEXT,connection_peer TEXT,'
        'history_sha TEXT,settings_sha TEXT,queue_sha TEXT,recorded_present TEXT,recorded_missing TEXT,'
        'recorded_checked_at REAL,manifest_sha TEXT,baseline_sha TEXT)')
    journal.db.commit()
    adapter.deadline = deadline
    try:
        plan = adapter.plan_identity() + '\n' + repr(authority.root) + '\n' + source_id + '\n' + authority.source_digest
        previous = journal.claim(request_id, selected, plan)
        if previous:
            return Result(previous.request_id, previous.state, previous.reason, previous.effects, previous.state == 'verified')
        try:
            adapter.arm()
            if time.monotonic() >= deadline:
                raise TimeoutError('deadline before TERM intent; no signal')
            journal.save(request_id, 'stop_intent', 'old generation TERM unknown', 'stop_unknown')
            adapter.stop_once()
            while not adapter.closed():
                if time.monotonic() >= deadline:
                    raise TimeoutError('closure not proven; replacement withheld')
                time.sleep(.02)
            journal.save(request_id, 'closed', 'old generation exit and selected listener closure', 'old_closed')
            if time.monotonic() >= deadline:
                raise TimeoutError('deadline before replacement; launch withheld')
            journal.save(request_id, 'start_intent', 'replacement launch unknown', 'start_unknown')
            adapter.start_once()
            journal.save(request_id, 'replacement_started', 'owned replacement started', 'replacement_started')
            proof = adapter.verify(journal, request_id, deadline)
            with journal.db:
                journal.db.execute('INSERT INTO verification_proofs VALUES(' + ','.join('?' for _ in range(24)) + ')',
                                   (request_id, *proof))
            saved = journal.save(request_id, 'verified', 'same Root/thread/history/settings/input verified on pinned replacement RPC peer', 'restored_verified')
        except Exception as error:
            state, effects = journal.db.execute('SELECT state,effects FROM requests WHERE id=?', (request_id,)).fetchone()
            saved = journal.save(request_id, state, type(error).__name__ + ': ' + str(error), effects)
        return Result(saved.request_id, saved.state, saved.reason, saved.effects, saved.state == 'verified')
    finally:
        journal.close()
        adapter.close()


def reconcile(request_id, source_id):
    """Authenticated read-only effect reconciliation; never replay or clear a fence."""
    authority = Authority(source_id)
    result = v1.reconcile_private(authority.master.parent / 'provider-recovery/requests.sqlite3', request_id)
    authority.check()
    return result
