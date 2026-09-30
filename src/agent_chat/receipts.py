"""Versioned closure attestations shared by local and hosted coordination."""
import hashlib
from pathlib import Path


RECEIPT_FIELDS = {'version', 'resource', 'reservation_id', 'restored',
                  'processes_closed', 'closed_at', 'evidence', 'pids'}


def process_closure_proofs(receipt):
    """Validate receipt shape and return PID-specific historical attestations."""
    if not isinstance(receipt, dict):
        raise ValueError('receipt must be an object')
    version = receipt.get('version')
    if isinstance(version, bool) or version not in (1, 2):
        raise ValueError('receipt version must be 1 or 2')
    fields = RECEIPT_FIELDS | ({'process_closures'} if version == 2 else set())
    if set(receipt) != fields:
        raise ValueError('receipt must contain exactly the required fields')
    pids = receipt['pids']
    closed_at = receipt['closed_at']
    if not isinstance(pids, list) or any(isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0 for pid in pids):
        raise ValueError('receipt PIDs are invalid')
    if isinstance(closed_at, bool) or not isinstance(closed_at, (int, float)) or not closed_at > 0:
        raise ValueError('receipt closed_at is invalid')
    proofs = {}
    entries = receipt.get('process_closures', [])
    if not isinstance(entries, list):
        raise ValueError('receipt process_closures must be a list')
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != {'pid', 'closed_at', 'evidence', 'evidence_sha256'}:
            raise ValueError('process closure must contain pid, closed_at, evidence and evidence_sha256')
        pid, stamp, digest = entry['pid'], entry['closed_at'], entry['evidence_sha256']
        if isinstance(pid, bool) or not isinstance(pid, int) or pid not in pids or pid in proofs:
            raise ValueError('process closure PID must be uniquely listed in receipt pids')
        if isinstance(stamp, bool) or not isinstance(stamp, (int, float)) or not 0 < stamp <= closed_at:
            raise ValueError('process closure timestamp must be no later than resource closure')
        if not isinstance(entry['evidence'], str) or not entry['evidence'].strip():
            raise ValueError('process closure evidence path is required')
        if not isinstance(digest, str) or len(digest) != 64 or any(c not in '0123456789abcdef' for c in digest):
            raise ValueError('process closure evidence hash must be lowercase SHA-256')
        proofs[pid] = entry
    return proofs


def validate_process_evidence(proof, data):
    if not data or len(data) > 2 * 1024 * 1024:
        raise ValueError('process closure evidence is missing or too large')
    if hashlib.sha256(data).hexdigest() != proof['evidence_sha256']:
        raise ValueError('process closure evidence hash does not match receipt')


def read_process_evidence(proofs, receipt_path):
    reports = {}
    for pid, proof in proofs.items():
        path = Path(proof['evidence'])
        if not path.is_absolute():
            path = Path(receipt_path).resolve().parent / path
        # A bounded read also protects a client from oversized local reports.
        with path.open('rb') as stream:
            data = stream.read(2 * 1024 * 1024 + 1)
        validate_process_evidence(proof, data)
        reports[pid] = data
    return reports
