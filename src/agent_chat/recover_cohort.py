"""Bounded, serial client recovery. No database access, authority or retry loop.

JSON manifests/journals are parsed by jq. The existing single-return validator
owns receipt decoding and HTTP serialization. An uncertain mutation is never
replayed: explicit resume can only reconcile its exact receipt-backed outcome.
"""
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import urllib.error

from .core import CoordError, Coordinator
from .private_artifacts import _parent, _check

LIMIT = 1024 * 1024


def _jq(data, expression, *args):
    try:
        done = subprocess.run(['jq', *args, expression], input=data,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    except OSError as error:
        raise CoordError('recover-cohort requires jq') from error
    if done.returncode:
        raise CoordError('invalid cohort manifest or journal: ' + done.stderr.decode()[:300])
    return done.stdout


def _read_private(path):
    if not Path(path).is_absolute():
        raise CoordError('cohort artifact paths must be absolute')
    parent, name = _parent(path)
    try:
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
        with os.fdopen(fd, 'rb') as stream:
            _check(os.fstat(stream.fileno()))
            data = stream.read(LIMIT + 1)
        if not data or len(data) > LIMIT:
            raise CoordError('cohort artifact is empty or exceeds 1 MiB')
        return data
    finally:
        os.close(parent)


def load_manifest(path, identity):
    data = _read_private(path)
    _jq(data, '''
      def text: type=="string" and length>0 and length<=4096 and (test("[\\u0000-\\u001f\\u007f]")|not);
      def sha: type=="string" and test("^[0-9a-f]{64}$");
      if (type=="object" and keys==["cohort_id","entries","host","project","server","session","version"]
        and .version==1 and ([.cohort_id,.host,.project,.server,.session]|all(text))
        and (.entries|type=="array" and length>=1 and length<=64)
        and all(.entries[]; type=="object" and keys==["evidence_sha256","receipt","receipt_sha256","reservation_id","resource"]
          and ([.receipt,.reservation_id,.resource]|all(text)) and (.receipt|startswith("/"))
          and (.receipt_sha256|sha) and (.evidence_sha256|sha))
        and ([.entries[].resource]|length== (unique|length))
        and ([.entries[].reservation_id]|length== (unique|length)))
      then true else error("manifest schema/bound/duplicate") end''', '-e')
    actual = _jq(data, '[.server,.project,.session,.host]|.[]', '-r').decode().splitlines()
    if actual != list(identity):
        raise CoordError('manifest server/project/session/host differs from this invocation')
    rows = _jq(data, '.entries[]|[.resource,.reservation_id,.receipt,.receipt_sha256,.evidence_sha256]|@tsv', '-r').decode().splitlines()
    keys = ('resource', 'reservation_id', 'receipt', 'receipt_sha256', 'evidence_sha256')
    entries = [dict(zip(keys, row.split('\t'))) for row in rows]
    # jq @tsv escapes backslashes: reject such paths rather than misaddress them.
    if any('\\' in value for entry in entries for value in entry.values()):
        raise CoordError('cohort fields cannot contain backslashes')
    return hashlib.sha256(data).hexdigest(), entries


def validate_prepared(entry, receipt_bytes, receipt, row, context, params, session, host):
    if hashlib.sha256(receipt_bytes).hexdigest() != entry['receipt_sha256']:
        raise CoordError('cohort receipt bytes changed')
    if (receipt['reservation_id'] != entry['reservation_id'] or
            params['resource'] != entry['resource'] or row['state'] != 'stale' or
            row.get('owner_session') != session or not host or row.get('host_id') != host):
        raise CoordError('cohort requires exact server-returned owner/host and current stale RID/resource')
    # A receipt for another process is not evidence for this reservation.
    relevant = {run['local_pid'] for run in context['runs'] if run['local_pid']}
    pids = set(receipt['pids'])
    if not pids <= relevant or len(pids) != len(receipt['pids']):
        raise CoordError('cohort receipt PID is not uniquely associated with this RID')
    if pids and (receipt['version'] != 2 or
                 {proof['pid'] for proof in receipt['process_closures']} != pids):
        raise CoordError('cohort requires per-PID v2 closure evidence')
    proofs = {proof['pid']: proof for proof in receipt.get('process_closures', [])}
    for run in context['runs']:
        if run['local_pid'] in proofs:
            stamp = proofs[run['local_pid']]['closed_at']
            if stamp < run['started_at'] or (run['closed_at'] is not None and stamp < run['closed_at']):
                raise CoordError('cohort per-PID proof predates its associated guard closure')
    if any(Coordinator._alive(pid, True) for pid in pids):
        raise CoordError('cohort receipt process group remains alive')


class Journal:
    def __init__(self, path, manifest_hash, resume):
        if not Path(path).is_absolute():
            raise CoordError('journal path must be absolute')
        self.parent, name = _parent(path)
        self.fd = None
        try:
            flags = os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK
            flags |= 0 if resume else os.O_CREAT | os.O_EXCL
            self.fd = os.open(name, flags, 0o600, dir_fd=self.parent)
            _check(os.fstat(self.fd))
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.manifest_hash = manifest_hash
            self.states = {}
            self.seq = 0
            if resume:
                data = os.read(self.fd, LIMIT + 1)
                if len(data) > LIMIT or not data.endswith(b'\n'):
                    raise CoordError('incomplete/oversized journal; preserve for reconciliation')
                _jq(data, 'if length>0 and all(.[]; .version==1 and .manifest_sha256==$pin) then true else error("journal binding") end', '-s', '-e', '--arg', 'pin', manifest_hash)
                lines = _jq(data, '.[]|[.seq,.index,.kind,.receipt_digest,.evidence_digest]|@tsv', '-s', '-r').decode().splitlines()
                for line in lines:
                    seq, index, kind, digest, evidence = line.split('\t')
                    if int(seq) != self.seq or kind not in ('header','attempt','success','rejected','uncertain','blocked'):
                        raise CoordError('invalid journal event sequence')
                    self.seq += 1
                    index = int(index)
                    prior = self.states.get(index, ('', '', ''))[0]
                    if kind == 'header':
                        if self.seq != 1 or index != -1: raise CoordError('invalid journal header')
                    else:
                        if self.seq == 1 or index < 0: raise CoordError('invalid journal entry')
                        if prior == 'success': raise CoordError('journal replays a confirmed success')
                        if kind in ('success','rejected','uncertain') and prior not in ('attempt','uncertain'):
                            raise CoordError('journal outcome lacks attempt')
                        self.states[index] = (kind, digest, evidence)
            else:
                self.append(-1, 'header')
                os.fsync(self.parent)
        except BaseException:
            self.close()
            raise

    def append(self, index, kind, digest='', evidence='', detail=''):
        data = _jq(b'', '{version:1,manifest_sha256:$pin,seq:$seq,index:$index,kind:$kind,receipt_digest:$digest,evidence_digest:$evidence,detail:$detail}',
                   '-n', '-c', '--arg', 'pin', self.manifest_hash,
                   '--argjson', 'seq', str(self.seq), '--argjson', 'index', str(index),
                   '--arg', 'kind', kind, '--arg', 'digest', digest,
                   '--arg', 'evidence', evidence, '--arg', 'detail', detail[:500])
        os.lseek(self.fd, 0, os.SEEK_END)
        offset = 0
        while offset < len(data):
            written = os.write(self.fd, data[offset:])
            if not written: raise OSError('short journal write')
            offset += written
        os.fsync(self.fd)
        self.seq += 1
        self.states[index] = (kind, digest, evidence)

    def close(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None
        if self.parent is not None:
            os.close(self.parent)
            self.parent = None


def recover_cohort(manifest, journal_path, *, resume, server, project, session, host, call, prepare):
    pin, entries = load_manifest(manifest, (server, project, session, host))
    journal = Journal(journal_path, pin, resume)
    completed = []
    try:
        if any(index >= len(entries) for index in journal.states):
            raise CoordError('journal entry exceeds manifest')
        for index, entry in enumerate(entries):
            kind, digest, evidence = journal.states.get(index, ('', '', ''))
            if kind == 'success':
                completed.append(entry['reservation_id'])
                continue
            if kind in ('attempt', 'uncertain'):
                result = call('reservation-result', dict(resource=entry['resource'], reservation_id=entry['reservation_id']))
                if (result.get('state') != 'cleared' or result.get('action') != 'recover' or
                    result.get('resource') != entry['resource'] or result.get('reservation_id') != entry['reservation_id'] or
                    result.get('owner_session') != session or result.get('host_id') != host or
                    result.get('receipt_backed_clear') is not True or result.get('receipt_sha256') != digest or
                    result.get('evidence_sha256') != evidence):
                    return dict(exit_code=2, state='uncertain', stopped_at=index, completed=completed,
                                reason='exact receipt-backed outcome unavailable; no replay')
                journal.append(index, 'success', digest, evidence, 'reconciled exact server outcome')
                completed.append(entry['reservation_id'])
                continue
            try:
                # No mutable RPC has begun: ordinary receipt/path/PID validation.
                params = prepare(entry)
            except (CoordError, OSError, ValueError, KeyError) as error:
                journal.append(index, 'blocked', detail=str(error))
                return dict(exit_code=2, state='blocked', stopped_at=index, completed=completed, reason=str(error))
            # Serialize exactly as the existing server receipt contract does.
            digest = hashlib.sha256(json.dumps(params['receipt'], sort_keys=True, separators=(',', ':')).encode()).hexdigest()
            evidence = entry['evidence_sha256']
            journal.append(index, 'attempt', digest, evidence)
            try:
                result = call('recover', params)
                if result.get('recovered') is not True or result.get('resource') != entry['resource']:
                    raise CoordError('recovery response did not confirm exact resource')
            except (CoordError, OSError, ValueError) as error:
                cause = error.__cause__
                rejected = isinstance(cause, urllib.error.HTTPError) and 400 <= cause.code < 500
                kind = 'rejected' if rejected else 'uncertain'
                journal.append(index, kind, digest, evidence, str(error))
                return dict(exit_code=2, state=kind, stopped_at=index, completed=completed, reason=str(error))
            journal.append(index, 'success', digest, evidence)
            completed.append(entry['reservation_id'])
        return dict(exit_code=0, state='complete', completed=completed)
    finally:
        journal.close()
