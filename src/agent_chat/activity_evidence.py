"""Request-scoped, provenance-preserving evidence; no acceptance inference."""
import hashlib
import time
import uuid
from agent_chat.core import CoordError


def install(db):
    db.executescript('''
      CREATE TABLE IF NOT EXISTS project_evidence_requests (
        work_id TEXT NOT NULL, request_id TEXT NOT NULL, customer_session TEXT NOT NULL,
        recorded_by TEXT NOT NULL, recorded_at REAL NOT NULL, PRIMARY KEY(work_id,request_id));
      CREATE TABLE IF NOT EXISTS project_evidence_events (
        id TEXT PRIMARY KEY, work_id TEXT NOT NULL, request_id TEXT NOT NULL,
        kind TEXT NOT NULL CHECK(kind IN ('change','validation','delivery')),
        message_id TEXT NOT NULL, before_id TEXT, after_id TEXT, before_sha256 TEXT, after_sha256 TEXT,
        summary TEXT NOT NULL, recorded_by TEXT NOT NULL, recorded_at REAL NOT NULL);
      CREATE TABLE IF NOT EXISTS project_artifact_decisions (
        delivery_id TEXT PRIMARY KEY, work_id TEXT NOT NULL, request_id TEXT NOT NULL,
        artifact_id TEXT NOT NULL, artifact_sha256 TEXT NOT NULL, operator_session TEXT NOT NULL,
        outcome TEXT NOT NULL CHECK(outcome IN ('accepted','declined')), origin TEXT NOT NULL,
        recorded_at REAL NOT NULL);
      CREATE TRIGGER IF NOT EXISTS project_evidence_requests_no_update BEFORE UPDATE ON project_evidence_requests BEGIN SELECT RAISE(ABORT,'evidence is immutable'); END;
      CREATE TRIGGER IF NOT EXISTS project_evidence_requests_no_delete BEFORE DELETE ON project_evidence_requests BEGIN SELECT RAISE(ABORT,'evidence is immutable'); END;
      CREATE TRIGGER IF NOT EXISTS project_evidence_events_no_update BEFORE UPDATE ON project_evidence_events BEGIN SELECT RAISE(ABORT,'evidence is immutable'); END;
      CREATE TRIGGER IF NOT EXISTS project_evidence_events_no_delete BEFORE DELETE ON project_evidence_events BEGIN SELECT RAISE(ABORT,'evidence is immutable'); END;
      CREATE TRIGGER IF NOT EXISTS project_artifact_decisions_no_update BEFORE UPDATE ON project_artifact_decisions BEGIN SELECT RAISE(ABORT,'artifact decision is immutable'); END;
      CREATE TRIGGER IF NOT EXISTS project_artifact_decisions_no_delete BEFORE DELETE ON project_artifact_decisions BEGIN SELECT RAISE(ABORT,'artifact decision is immutable'); END;
    ''')


def _id(value):
    if not isinstance(value,str) or not value or len(value)>500: raise CoordError('exact evidence identifier required')
    return value


def link(activity, work_id, request_id):
    _id(work_id); _id(request_id)
    with activity.coord.tx() as db:
        actor, po = activity._authorize(db, po_only=True)
        work = activity._work(db, work_id)
        if work['kind'] != 'task' or work['assigned_by'] != actor: raise CoordError('request evidence requires a PO-owned task')
        req=db.execute('SELECT sender_session,recipient_session FROM messages WHERE id=?',(request_id,)).fetchone()
        if not req or not db.execute("SELECT 1 FROM operator_message_origins WHERE message_id=? AND sender_session=? AND channel='authenticated_operator_ui'",(request_id, req['sender_session'])).fetchone():
            raise CoordError('request must be a server-stamped operator UI message')
        goal=activity._work(db,work['goal_id'])
        routed=db.execute('SELECT reply_to FROM message_replies WHERE message_id=?',(goal['source_request_id'],)).fetchone()
        if req['recipient_session'] != actor and not (req['recipient_session']==po['root_session'] and routed and routed['reply_to']==request_id):
            raise CoordError('request is not directly routed to PO or linked root objective')
        prior=db.execute('SELECT * FROM project_evidence_requests WHERE work_id=? AND request_id=?',(work_id,request_id)).fetchone()
        if prior:return dict(prior)
        db.execute('INSERT INTO project_evidence_requests VALUES(?,?,?,?,?)',(work_id,request_id,req['sender_session'],actor,time.time()))
        return dict(db.execute('SELECT * FROM project_evidence_requests WHERE work_id=? AND request_id=?',(work_id,request_id)).fetchone())


def _attachment(db, attachment_id):
    _id(attachment_id)
    row=db.execute('SELECT id,message_id,name,mime,size,length(content) AS actual_size FROM attachments WHERE id=?',(attachment_id,)).fetchone()
    if not row or row['size'] != row['actual_size'] or row['size'] > 8*1024*1024: raise CoordError('evidence attachment missing, inconsistent or over 8 MiB')
    content=db.execute('SELECT content FROM attachments WHERE id=?',(attachment_id,)).fetchone()['content']
    return row, hashlib.sha256(content).hexdigest()


def record(activity, work_id, request_id, kind, message_id, before_id, after_id, summary):
    for value in (work_id,request_id,message_id): _id(value)
    if kind not in ('change','validation','delivery'): raise CoordError('unsupported evidence kind; customer acceptance is not inferred')
    if not isinstance(summary,str) or not summary.strip() or len(summary)>2000: raise CoordError('evidence summary required (1–2000 characters)')
    with activity.coord.tx() as db:
        actor,po=activity._authorize(db,po_only=True); work=activity._work(db,work_id)
        if work['kind']!='task' or work['assigned_by']!=actor: raise CoordError('evidence requires a PO-owned task')
        association=db.execute('SELECT * FROM project_evidence_requests WHERE work_id=? AND request_id=?',(work_id,request_id)).fetchone()
        if not association: raise CoordError('exact customer request is not linked to task')
        msg=db.execute('SELECT id,sender_session,recipient_session FROM messages WHERE id=?',(message_id,)).fetchone()
        reply=db.execute('SELECT reply_to FROM message_replies WHERE message_id=?',(message_id,)).fetchone()
        before_hash=after_hash=None
        if kind in ('change','validation'):
            if not msg or (msg['sender_session'],msg['recipient_session'])!=(work['owner_session'],actor) or not reply or reply['reply_to']!=work['source_request_id']:
                raise CoordError('evidence message must be the assigned worker reply to its task source')
        if kind=='change':
            before,before_hash=_attachment(db,before_id); after,after_hash=_attachment(db,after_id)
            if before['message_id']!=work['source_request_id'] or after['message_id']!=message_id or before['name']!=after['name'] or before_hash==after_hash:
                raise CoordError('before and after must be different bytes of the same named task artifact')
        elif kind=='validation':
            if before_id is not None or after_id is not None: raise CoordError('validation records a worker report, not artifact proof')
        else:
            if before_id is not None: raise CoordError('delivery takes only the after attachment')
            after,after_hash=_attachment(db,after_id)
            if not msg or (msg['sender_session'],msg['recipient_session'])!=(actor,association['customer_session']) or after['message_id']!=message_id:
                raise CoordError('delivery must be a PO message carrying an artifact to the associated customer')
            if not db.execute("SELECT 1 FROM project_evidence_events WHERE work_id=? AND request_id=? AND kind='change' AND after_sha256=?",(work_id,request_id,after_hash)).fetchone():
                raise CoordError('delivery bytes do not match a recorded changed artifact')
        event_id='evidence_'+uuid.uuid4().hex
        db.execute('INSERT INTO project_evidence_events VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',(event_id,work_id,request_id,kind,message_id,before_id,after_id,before_hash,after_hash,summary.strip(),actor,time.time()))
        return dict(db.execute('SELECT * FROM project_evidence_events WHERE id=?',(event_id,)).fetchone())


def project(db, work_id):
    result=[]
    for association in db.execute('SELECT * FROM project_evidence_requests WHERE work_id=? ORDER BY recorded_at,request_id',(work_id,)):
        item=dict(association); item['events']=[]
        request=db.execute('SELECT m.created_at,m.body,o.channel FROM messages m JOIN operator_message_origins o ON o.message_id=m.id AND o.sender_session=m.sender_session WHERE m.id=?',(association['request_id'],)).fetchone()
        item['request_provenance'] = ({'created_at':request['created_at'],'channel':request['channel'],'stored_body_sha256':hashlib.sha256(request['body'].encode('utf-8')).hexdigest()} if request else {'state':'unavailable'})
        for row in db.execute('SELECT * FROM project_evidence_events WHERE work_id=? AND request_id=? ORDER BY recorded_at,id',(work_id,association['request_id'])):
            event=dict(row)
            msg=db.execute('SELECT sender_session,recipient_session,created_at,body FROM messages WHERE id=?',(event['message_id'],)).fetchone()
            event['message_provenance'] = ({'sender_session':msg['sender_session'],'recipient_session':msg['recipient_session'],'created_at':msg['created_at'],'stored_body_sha256':hashlib.sha256(msg['body'].encode('utf-8')).hexdigest()} if msg else {'state':'unavailable'})
            for which in ('before','after'):
                attachment_id=event[which+'_id']
                if attachment_id:
                    try:
                        attachment,current_hash=_attachment(db,attachment_id)
                        event[which+'_integrity']={'state':'matches_recorded' if current_hash==event[which+'_sha256'] else 'changed','name':attachment['name'],'size':attachment['size'],'mime':attachment['mime']}
                    except CoordError:
                        event[which+'_integrity']={'state':'unavailable'}
            if event['kind']=='delivery':
                read=db.execute('SELECT read_at,provenance FROM message_reads WHERE message_id=? AND reader_session=?',(event['message_id'],association['customer_session'])).fetchone()
                event['recipient_read']=dict(read) if read else None
                decision=db.execute('SELECT * FROM project_artifact_decisions WHERE delivery_id=?',(event['id'],)).fetchone()
                event['operator_decision']=dict(decision) if decision else None
            item['events'].append(event)
        item['dimensions'] = {}
        for kind in ('change','validation','delivery'):
            events=[event['id'] for event in item['events'] if event['kind']==kind]
            item['dimensions'][kind]={'state':'reported' if events else 'unknown','event_ids':events}
        decisions=[event['operator_decision'] for event in item['events'] if event['kind']=='delivery' and event['operator_decision']]
        undecided=sum(1 for event in item['events'] if event['kind']=='delivery' and not event['operator_decision'])
        item['customer_acceptance']=({'state':'decisions_recorded','decisions':decisions,'undecided_delivery_count':undecided,'reason':'specific delivery decisions only; task completion not inferred'} if decisions else {'state':'unknown','undecided_delivery_count':undecided,'reason':'no explicit operator artifact action recorded'})
        result.append(item)
    return result


def list_for(activity,work_id):
    _id(work_id)
    with activity.coord.tx() as db:
        activity._authorize(db,po_only=True); work=activity._work(db,work_id)
        if work['assigned_by']!=activity.coord.require_session(): raise CoordError('work is not PO-owned')
        return {'work_id':work_id,'requests':project(db,work_id)}


def operator_decide(coord, project_id, work_id, request_id, delivery_id, outcome):
    """Called only by the server-selected Basic+CSRF operator UI route."""
    for value in (work_id,request_id,delivery_id): _id(value)
    if outcome not in ('accepted','declined'): raise CoordError('explicit accepted or declined action required')
    with coord.tx() as db:
        actor=coord.require_session()
        meta=db.execute("SELECT value FROM meta WHERE key='project_id'").fetchone()
        if (meta['value'] if meta else 'default') != project_id: raise CoordError('project identity mismatch')
        if not db.execute('SELECT 1 FROM operator_identities WHERE session_id=?',(actor,)).fetchone(): raise CoordError('operator identity unavailable')
        event=db.execute("SELECT e.*,r.customer_session FROM project_evidence_events e JOIN project_evidence_requests r ON r.work_id=e.work_id AND r.request_id=e.request_id WHERE e.id=? AND e.work_id=? AND e.request_id=? AND e.kind='delivery'",(delivery_id,work_id,request_id)).fetchone()
        if not event or event['customer_session']!=actor: raise CoordError('exact operator delivery evidence unavailable')
        message=db.execute('SELECT sender_session,recipient_session FROM messages WHERE id=?',(event['message_id'],)).fetchone()
        artifact,digest=_attachment(db,event['after_id'])
        if not message or message['recipient_session']!=actor or message['sender_session']!=event['recorded_by'] or artifact['message_id']!=event['message_id'] or digest!=event['after_sha256']:
            raise CoordError('delivery artifact provenance changed or unavailable')
        previous=db.execute('SELECT * FROM project_artifact_decisions WHERE delivery_id=?',(delivery_id,)).fetchone()
        if previous:
            if previous['operator_session']!=actor or previous['outcome']!=outcome: raise CoordError('artifact decision already recorded differently')
            return dict(previous)
        db.execute('INSERT INTO project_artifact_decisions VALUES(?,?,?,?,?,?,?,?,?)',(delivery_id,work_id,request_id,event['after_id'],digest,actor,outcome,'operator_basic_csrf',time.time()))
        return dict(db.execute('SELECT * FROM project_artifact_decisions WHERE delivery_id=?',(delivery_id,)).fetchone())
