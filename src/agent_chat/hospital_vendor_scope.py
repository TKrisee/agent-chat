"""PRIVATE, NON-AUTHORIZING cohort checks. Not wired to any live interface."""
from __future__ import annotations
import csv
import hashlib
import io
import math
import secrets
from pathlib import Path

MANIFEST_SHA256 = 'da3e5cd019d727e611ff0ecec0382d1ef2bab41080ee310bc177b6c8dc6ba398'
OWNER = 'session_92cd8bfd4ee045798585e1f873c94d55'
CONSUMER = 'session_d96238daad0345b699ec1dd1972ab11b'
HOST = 'host_o6KZLd0kkjchCaH8OjoJZ9aZXHDlFiE2'
PROJECT = 'default'
UNITY_PO = 'session_9c386f8477db4f0aa561a9b23e779ade'
PREFIX = 'file:Assets/SurrounDead/'
COVERED = ('file:Assets/', 'file:Packages/', 'file:ProjectSettings/')
class Refused(ValueError): pass

def manifest():
    path = Path(__file__).with_name('hospital_vendor108.tsv')
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != MANIFEST_SHA256:
        raise Refused('manifest_changed')
    with io.StringIO(raw.decode('utf-8'),newline='') as stream:
        rows = list(csv.DictReader(stream, delimiter='\t'))
    result = {}
    for row in rows:
        name = row['resource']; rid = row['reservation_id']
        parts = name[5:].split('/') if name.startswith('file:') else []
        if (not name.startswith(PREFIX) or any(p in ('', '.', '..') for p in parts)
            or '\\' in name or '\x00' in name or not rid.startswith('reservation_')
            or row['state'] != 'STALE' or row['same_owner'] != OWNER
            or name in result or any(item[0] == rid for item in result.values())):
            raise Refused('manifest_invalid')
        result[name] = (rid, float(row['deadline']))
    if len(result) != 108:
        raise Refused('manifest_count')
    return result

def check_current(db, *, project, consumer, host, now):
    """Check fixed state only. NOT a grant, authentication, task or write guard."""
    if (project, consumer, host) != (PROJECT, CONSUMER, HOST):
        raise Refused('caller_mismatch')
    expected = manifest()
    world_host = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?',(OWNER,)).fetchone()
    consumer_host = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?',(CONSUMER,)).fetchone()
    if not world_host or not consumer_host or world_host[0] != HOST or consumer_host[0] != HOST:
        raise Refused('host_mismatch')
    rows = db.execute('SELECT name,owner_session,reservation_id,stale,deadline FROM resources WHERE owner_session IS NOT NULL').fetchall()
    # The actual copy owner may hold additional ordinary exact source scopes.
    # They are not foreign conflicts, and never substitute for the fixed 108.
    covered = {r[0]:r for r in rows if r[0].startswith(COVERED) and r[1] != CONSUMER}
    if set(covered) != set(expected):
        raise Refused('extra_or_missing_claim')
    for name,(rid,deadline) in expected.items():
        actual = covered[name]
        if (actual[1],actual[2],actual[3],actual[4]) != (OWNER,rid,1,deadline):
            raise Refused('claim_changed')
        if not deadline < now:
            raise Refused('claim_not_stale')
    return frozenset(expected)

def check_task_association(db, *, task_id, ack_id, body_sha256):
    """Verify stored association only; this does not establish task currentness."""
    if (not isinstance(task_id,str) or not task_id.startswith('message_')
        or not isinstance(ack_id,str) or not ack_id.startswith('message_')
        or not isinstance(body_sha256,str) or len(body_sha256)!=64
        or any(c not in '0123456789abcdef' for c in body_sha256)):
        raise Refused('task_tuple_invalid')
    task=db.execute('SELECT sender_session,recipient_session,body,acked_at FROM messages WHERE id=?',(task_id,)).fetchone()
    if (not task or task[0]!=UNITY_PO or task[1]!=CONSUMER or not isinstance(task[2],str) or task[3] is None
        or hashlib.sha256(task[2].encode('utf-8')).hexdigest()!=body_sha256):
        raise Refused('task_changed')
    read=db.execute("SELECT 1 FROM message_reads WHERE message_id=? AND reader_session=? AND provenance IN ('recipient_full_message','recipient_context_full')",(task_id,CONSUMER)).fetchone()
    ack=db.execute('''SELECT m.sender_session,m.recipient_session,r.reply_to
        FROM messages m JOIN message_replies r ON r.message_id=m.id
        JOIN reply_ack_idempotency a ON a.message_id=m.id
        WHERE m.id=? AND a.sender_session=? AND a.reply_to=?''',(ack_id,CONSUMER,task_id)).fetchone()
    if not read or not ack or tuple(ack)!=(CONSUMER,UNITY_PO,task_id):
        raise Refused('task_receipt_missing')
    return True

def check_owner_capability(db, *, bound_resource, bound_rid, supplied_token, now):
    """Check own task-scoped capability only; not a grant or task authority.

    A future authorized binding must supply its server-stored resource and RID,
    never choose them from request data, and recheck this inside each use.
    """
    if (not isinstance(bound_resource,str) or not bound_resource.startswith('operation:gameplay/')
        or not isinstance(bound_rid,str) or not bound_rid.startswith('reservation_')
        or not isinstance(supplied_token,str) or not supplied_token):
        raise Refused('requester_capability_invalid')
    row=db.execute('SELECT owner_session,reservation_id,token,stale,deadline FROM resources WHERE name=?',(bound_resource,)).fetchone()
    if (not row or row[0]!=CONSUMER or row[1]!=bound_rid or not isinstance(row[2],str)
        or not secrets.compare_digest(row[2],supplied_token) or row[3]!=0
        or type(row[4]) not in (int,float) or not math.isfinite(row[4])
        or type(now) not in (int,float) or not math.isfinite(now) or row[4]<=now):
        raise Refused('requester_capability_changed')
    return True

# Actual fixed task association. Detached synthetic task fixtures may patch these
# constants only inside their private process; no production selector exists.
TASK = 'message_WaKMDwzuwEHU1EAPK_qZ270Z-BZC4Pxq'
INTAKE = 'message_EU3QLzHNkMUkvpNvqneEWs1ienkJaoM4'
ESSENTIAL = 'message_WL7mOcfP1dY_buG-hcKnEdBErrh0ljRI'
TASK_SHA256 = '6010995c0a920b9208e9acfe78a0c572c8a41cdb13e8662b37b4baa8187e1927'
TASK_STARTED = 1793999754.469544
TASK_PRODUCTIVE_UNTIL = 1794006954.469544
THREAD = '01a11d1f-bb15-7750-affe-3029d7b4057d'
CONTROL = 'operation:gameplay/HospitalCurrentCopyBuildPlay02-20261106/vendor-read-control'
RUN_RESOURCE = 'operation:gameplay/HospitalCurrentCopyBuildPlay02-20261106/MainReadCopy'

def check_exact_task(db):
    """Verify exact recipient consumption, first ACK and essential PO ACK."""
    check_task_association(db,task_id=TASK,ack_id=INTAKE,body_sha256=TASK_SHA256)
    ack=db.execute('SELECT created_at,acked_at FROM messages WHERE id=?',(INTAKE,)).fetchone()
    if not ack or ack[0] != TASK_STARTED or ack[1] is None: raise Refused('task_intake_time_changed')
    essential=db.execute('''SELECT m.sender_session,m.recipient_session,r.reply_to
        FROM messages m JOIN message_replies r ON r.message_id=m.id
        JOIN reply_ack_idempotency a ON a.message_id=m.id
        WHERE m.id=? AND a.sender_session=? AND a.reply_to=?''',(ESSENTIAL,UNITY_PO,INTAKE)).fetchone()
    read=db.execute("SELECT 1 FROM message_reads WHERE message_id=? AND reader_session=? AND provenance IN ('recipient_full_message','recipient_context_full')",(ESSENTIAL,CONSUMER)).fetchone()
    binding=db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?',(CONSUMER,)).fetchone()
    if not essential or tuple(essential)!=(UNITY_PO,CONSUMER,INTAKE) or not read or not binding or binding[0]!=THREAD:
        raise Refused('essential_task_association_missing')
    return True

def check_selected_task(db, *, task_id, intake_id, essential_id, body_sha256, intake_sha256, essential_sha256, started_at, expires_at, now):
    """Revalidate one Root-selected stored association, never a worker selector."""
    if (any(not isinstance(x,str) or not x.startswith('message_') or len(x)>160 for x in (task_id,intake_id,essential_id))
        or task_id==TASK or len({task_id,intake_id,essential_id})!=3):
        raise Refused('selected_task_invalid')
    if (type(started_at) not in (int,float) or not math.isfinite(started_at)
        or type(expires_at) not in (int,float) or not math.isfinite(expires_at)
        or type(now) not in (int,float) or not math.isfinite(now)
        or not started_at < expires_at <= started_at+7200):
        raise Refused('selected_task_clock_invalid')
    if any(not isinstance(x,str) or len(x)!=64 or any(c not in '0123456789abcdef' for c in x) for x in (intake_sha256,essential_sha256)):
        raise Refused('selected_ack_hash_invalid')
    check_task_association(db,task_id=task_id,ack_id=intake_id,body_sha256=body_sha256)
    task=db.execute('SELECT created_at,acked_at FROM messages WHERE id=?',(task_id,)).fetchone()
    intake=db.execute('SELECT created_at,acked_at,body FROM messages WHERE id=?',(intake_id,)).fetchone()
    essential=db.execute('''SELECT m.sender_session,m.recipient_session,r.reply_to,m.created_at,m.body
        FROM messages m JOIN message_replies r ON r.message_id=m.id
        JOIN reply_ack_idempotency a ON a.message_id=m.id
        WHERE m.id=? AND a.sender_session=? AND a.reply_to=?''',(essential_id,UNITY_PO,intake_id)).fetchone()
    task_read=db.execute("SELECT read_at FROM message_reads WHERE message_id=? AND reader_session=? AND provenance IN ('recipient_full_message','recipient_context_full')",(task_id,CONSUMER)).fetchone()
    read=db.execute("SELECT read_at FROM message_reads WHERE message_id=? AND reader_session=? AND provenance IN ('recipient_full_message','recipient_context_full')",(essential_id,CONSUMER)).fetchone()
    intake_read=db.execute("SELECT read_at FROM message_reads WHERE message_id=? AND reader_session=? AND provenance IN ('recipient_full_message','recipient_context_full')",(intake_id,UNITY_PO)).fetchone()
    binding=db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?',(CONSUMER,)).fetchone()
    stamps=(task[0],task[1],intake[0],intake[1],essential[3],task_read[0],intake_read[0],read[0]) if task and intake and essential and task_read and intake_read and read else ()
    if len(stamps)!=8 or any(type(x) not in (int,float) or not math.isfinite(x) for x in stamps):
        raise Refused('selected_task_association_missing')
    if (not task or not intake or intake[0]!=started_at or intake[1] is None or not essential
        or tuple(essential[:3])!=(UNITY_PO,CONSUMER,intake_id) or not read or not intake_read or not binding or binding[0]!=THREAD
        or not task[0]<=started_at<=task[1]<=now or not started_at<=essential[3]<=intake[1]<=now
        or not task[0]<=task_read[0]<=started_at or not started_at<=intake_read[0]<=essential[3] or not essential[3]<=read[0]<=now
        or not isinstance(intake[2],str) or hashlib.sha256(intake[2].encode()).hexdigest()!=intake_sha256
        or not isinstance(essential[4],str) or hashlib.sha256(essential[4].encode()).hexdigest()!=essential_sha256):
        raise Refused('selected_task_association_missing')
    return True
