"""Recoverable, explicitly confirmed moves of one operator/agent conversation.

Project databases use WAL: destination import commits before source deletion.
The destination journal makes a interrupted copy/delete safely retryable; this
is deliberately not advertised as a crash-atomic multi-database transaction.
"""
import contextlib
import hashlib
import sqlite3
import time
import uuid

from .core import Coordinator, CoordError


TABLES = {
    'attachments': 'id,message_id,name,mime,size,content',
    'message_replies': 'message_id,reply_to',
    'message_batches': 'message_id,batch_id',
    'message_attention': 'message_id',
    'message_reads': 'message_id,reader_session,read_at',
    'reply_ack_idempotency': 'sender_session,reply_to,fingerprint,message_id',
}


def exists(db, table):
    return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone())


def record(db, row):
    result = {'message': dict(row)}
    for table, columns in TABLES.items():
        result[table] = [dict(item) for item in db.execute(
            f'SELECT {columns} FROM {table} WHERE message_id=? ORDER BY rowid', (row['id'],))]
    return result


def fingerprint(item):
    value = {**item, 'attachments': [
        {**{k: v for k, v in attachment.items() if k != 'content'},
         'sha256': hashlib.sha256(attachment['content']).hexdigest()}
        for attachment in item['attachments']]}
    return hashlib.sha256(Coordinator._canonical_bytes(value)).hexdigest()


def reply_fingerprint(item):
    return hashlib.sha256(Coordinator._canonical_bytes({
        'recipient_session': item['message']['recipient_session'],
        'body': item['message']['body'],
        'attachments': [{'name': a['name'], 'mime': a['mime'],
                         'sha256': hashlib.sha256(a['content']).hexdigest()}
                        for a in item['attachments']]})).hexdigest()


def mapped(item, identities):
    result = {table: [dict(row) for row in item[table]] for table in TABLES}
    result['message'] = {**item['message'],
                         'sender_session': identities[item['message']['sender_session']],
                         'recipient_session': identities[item['message']['recipient_session']]}
    for row in result['message_reads']:
        if row['reader_session'] != item['message']['recipient_session']:
            raise CoordError('selected history has a foreign message reader')
        row['reader_session'] = identities[row['reader_session']]
    for row in result['reply_ack_idempotency']:
        if row['sender_session'] != item['message']['sender_session'] or row['fingerprint'] != reply_fingerprint(item):
            raise CoordError('selected history has inconsistent ACK-reply metadata')
        row['sender_session'] = identities[row['sender_session']]
        row['fingerprint'] = reply_fingerprint(result)
    return result


class HistoryMove:
    def __init__(self, source_path, destination_path, source_project, destination_project,
                 source_session, destination_session, source_operator, destination_operator):
        self.source_path, self.destination_path = source_path, destination_path
        self.source_project, self.destination_project = source_project, destination_project
        self.source_session, self.destination_session = source_session, destination_session
        self.source_operator, self.destination_operator = source_operator, destination_operator
        if source_project == destination_project or str(source_path) == str(destination_path):
            raise CoordError('history move requires two different projects')
        if source_session == source_operator or destination_session == destination_operator:
            raise CoordError('history move requires distinct operator and agent identities')
        self.identities = {source_session: destination_session, source_operator: destination_operator}

    @contextlib.contextmanager
    def connections(self, write=False):
        with contextlib.ExitStack() as stack:
            source = stack.enter_context(contextlib.closing(sqlite3.connect(
                str(self.source_path), timeout=10, isolation_level=None)))
            destination = stack.enter_context(contextlib.closing(sqlite3.connect(
                str(self.destination_path), timeout=10, isolation_level=None)))
            for db in (source, destination):
                db.row_factory = sqlite3.Row
                db.execute('BEGIN IMMEDIATE' if write else 'BEGIN')
            try:
                yield source, destination
            finally:
                for db in (source, destination):
                    if db.in_transaction:
                        db.rollback()

    def quiescent(self, db):
        if not db.execute('SELECT 1 FROM sessions WHERE id=?', (self.source_session,)).fetchone():
            raise CoordError('source agent must be an existing exact session')
        if exists(db, 'bridge_bindings') and db.execute(
                'SELECT 1 FROM bridge_bindings WHERE session_id=?', (self.source_session,)).fetchone():
            raise CoordError('detach the source agent route before moving its history')
        if db.execute('SELECT 1 FROM resources WHERE owner_session=?', (self.source_session,)).fetchone() or db.execute(
                'SELECT 1 FROM guarded_runs WHERE session=? AND closed_at IS NULL', (self.source_session,)).fetchone():
            raise CoordError('source agent still owns a resource or open guard')
        if exists(db,'remote_guarded_runs') and db.execute(
                'SELECT 1 FROM remote_guarded_runs WHERE session_id=? AND closed_at IS NULL', (self.source_session,)).fetchone():
            raise CoordError('source agent still has an open remote guard')
        if exists(db, 'session_resets') and db.execute(
                "SELECT 1 FROM session_resets WHERE target_session=? AND status NOT IN ('completed','cancelled','failed')",
                (self.source_session,)).fetchone():
            raise CoordError('source agent has an unresolved conversation request')

    def selection(self, db):
        notice_filter = ('AND NOT EXISTS (SELECT 1 FROM root_project_notices n WHERE n.message_id=m.id)'
                         if exists(db, 'root_project_notices') else '')
        rows = db.execute(f'''SELECT m.* FROM messages m WHERE
            ((sender_session=? AND recipient_session=?) OR (sender_session=? AND recipient_session=?))
            {notice_filter} ORDER BY seq''',
            (self.source_session, self.source_operator, self.source_operator, self.source_session)).fetchall()
        return [record(db, row) for row in rows]

    def validate(self, source, destination, items):
        ids = {item['message']['id'] for item in items}
        by_id = {item['message']['id']: item for item in items}
        for row in source.execute('SELECT message_id,reply_to FROM message_replies'):
            if (row['message_id'] in ids) != (row['reply_to'] in ids):
                raise CoordError('reply crosses the selected conversation boundary; no history changed')
            if row['message_id'] in ids and by_id[row['reply_to']]['message']['seq'] >= by_id[row['message_id']]['message']['seq']:
                raise CoordError('selected history has an invalid reply order')
        for row in source.execute('SELECT * FROM reply_ack_idempotency'):
            if (row['message_id'] in ids) != (row['reply_to'] in ids):
                raise CoordError('ACK-reply reference crosses the selection boundary')
            if row['message_id'] in ids:
                child = by_id[row['message_id']]
                if not any(r['reply_to'] == row['reply_to'] for r in child['message_replies']):
                    raise CoordError('selected ACK-reply reference lacks its reply link')
        if exists(source, 'bridge_deliveries'):
            for row in source.execute('''SELECT d.message_id FROM bridge_deliveries d
                    JOIN bridge_jobs j ON j.id=d.job_id WHERE j.status NOT IN ('dispatched','cancelled')'''):
                if row['message_id'] in ids:
                    raise CoordError('selected history has an unresolved wake; reconcile it first')
        for item in items:
            target = mapped(item, self.identities)
            for row in target['reply_ack_idempotency']:
                if destination.execute('SELECT 1 FROM reply_ack_idempotency WHERE sender_session=? AND reply_to=?',
                                       (row['sender_session'],row['reply_to'])).fetchone():
                    raise CoordError('ACK-reply key already exists in destination')
            if destination.execute('SELECT 1 FROM messages WHERE id=?', (item['message']['id'],)).fetchone():
                raise CoordError('selected message ID already exists in destination')
            for row in item['message_batches']:
                if destination.execute('SELECT 1 FROM message_batches WHERE batch_id=?', (row['batch_id'],)).fetchone():
                    raise CoordError('selected batch ID already exists in destination')
            for row in item['attachments']:
                if len(row['content']) != row['size'] or destination.execute(
                        'SELECT 1 FROM attachments WHERE id=?', (row['id'],)).fetchone():
                    raise CoordError('attachment is inconsistent or conflicts with destination')

    def journal(self, db, request_id):
        if not exists(db, 'history_moves'):
            return None
        row = db.execute('SELECT * FROM history_moves WHERE request_id=?', (request_id,)).fetchone()
        if row and (row['source_project'], row['destination_project'], row['source_session'], row['destination_session'],
                    row['source_operator'], row['destination_operator']) != (
                    self.source_project, self.destination_project, self.source_session, self.destination_session,
                    self.source_operator, self.destination_operator):
            raise CoordError('history request ID belongs to different move parameters')
        return row

    def summary(self, db, journal):
        return dict(request_id=journal['request_id'], status=journal['status'], count=journal['count'],
                    digest=journal['digest'], source_project=self.source_project,
                    destination_project=self.destination_project,
                    operator_to_agent=journal['incoming'], agent_to_operator=journal['outgoing'],
                    attachments=journal['attachments'], replies=journal['replies'], group_deliveries=journal['groups'],
                    message_ids=[row['message_id'] for row in db.execute(
                        'SELECT message_id FROM history_move_items WHERE request_id=? ORDER BY source_seq', (journal['request_id'],))])

    def preview(self, request_id):
        with self.connections() as (source, destination):
            prior = self.journal(destination, request_id)
            if prior:
                return self.summary(destination, prior)
            self.quiescent(source)
            items = self.selection(source)
            self.validate(source, destination, items)
            return dict(request_id=request_id, status='preview', count=len(items),
                        digest=hashlib.sha256(Coordinator._canonical_bytes([fingerprint(item) for item in items])).hexdigest(),
                        source_project=self.source_project, destination_project=self.destination_project,
                        operator_to_agent=sum(item['message']['sender_session'] == self.source_operator for item in items),
                        agent_to_operator=sum(item['message']['sender_session'] == self.source_session for item in items),
                        attachments=sum(len(item['attachments']) for item in items),
                        replies=sum(len(item['message_replies']) for item in items),
                        group_deliveries=sum(len(item['message_batches']) for item in items),
                        message_ids=[item['message']['id'] for item in items])

    def import_items(self, db, request_id, items, digest):
        db.execute('''CREATE TABLE IF NOT EXISTS history_moves (
            request_id TEXT PRIMARY KEY,source_project TEXT NOT NULL,destination_project TEXT NOT NULL,
            source_session TEXT NOT NULL,destination_session TEXT NOT NULL,source_operator TEXT NOT NULL,
            destination_operator TEXT NOT NULL,count INTEGER NOT NULL,digest TEXT NOT NULL,status TEXT NOT NULL,
            incoming INTEGER NOT NULL,outgoing INTEGER NOT NULL,attachments INTEGER NOT NULL,
            replies INTEGER NOT NULL,groups INTEGER NOT NULL,created_at REAL NOT NULL,completed_at REAL)''')
        db.execute('''CREATE TABLE IF NOT EXISTS history_move_items (
            request_id TEXT NOT NULL,message_id TEXT UNIQUE NOT NULL,source_seq INTEGER NOT NULL,
            source_fingerprint TEXT NOT NULL,destination_fingerprint TEXT NOT NULL,
            PRIMARY KEY(request_id,message_id))''')
        db.execute('INSERT INTO history_moves VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL)',
                   (request_id,self.source_project,self.destination_project,self.source_session,self.destination_session,
                    self.source_operator,self.destination_operator,len(items),digest,'imported',
                    sum(i['message']['sender_session']==self.source_operator for i in items),
                    sum(i['message']['sender_session']==self.source_session for i in items),
                    sum(len(i['attachments']) for i in items),sum(len(i['message_replies']) for i in items),
                    sum(len(i['message_batches']) for i in items),time.time()))
        job = 'history_move_' + request_id
        now = time.time()
        db.execute('INSERT INTO bridge_jobs(id,thread_id,payload,status,queue_id,error,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)',
                   (job,'history-import','{}','cancelled',None,'Historical import; never dispatched',now,now))
        for original in items:
            item = mapped(original, self.identities)
            message = item['message']
            columns = [key for key in message if key != 'seq']
            db.execute(f"INSERT INTO messages({','.join(columns)}) VALUES({','.join('?' for _ in columns)})",
                       [message[key] for key in columns])
            for table, columns_text in TABLES.items():
                columns = columns_text.split(',')
                for row in item[table]:
                    db.execute(f"INSERT INTO {table}({columns_text}) VALUES({','.join('?' for _ in columns)})",
                               [row[key] for key in columns])
            db.execute('INSERT INTO bridge_deliveries(message_id,job_id) VALUES(?,?)', (message['id'],job))
            db.execute('INSERT INTO history_move_items VALUES(?,?,?,?,?)',
                       (request_id,message['id'],message['seq'],fingerprint(original),fingerprint(item)))

    def verify_import(self, db, request_id):
        job = db.execute('SELECT * FROM bridge_jobs WHERE id=?', ('history_move_' + request_id,)).fetchone()
        if not job or job['status'] != 'cancelled' or job['thread_id'] != 'history-import' or job['queue_id'] is not None:
            raise CoordError('historical wake suppression changed; source was not deleted')
        for manifest in db.execute('SELECT * FROM history_move_items WHERE request_id=? ORDER BY source_seq', (request_id,)):
            marker = db.execute('SELECT job_id FROM bridge_deliveries WHERE message_id=?', (manifest['message_id'],)).fetchone()
            if not marker or marker['job_id'] != job['id']:
                raise CoordError('historical wake suppression is missing; source was not deleted')
            row = db.execute('SELECT * FROM messages WHERE id=?', (manifest['message_id'],)).fetchone()
            if not row:
                raise CoordError('imported history is missing; source was not deleted')
            value = record(db, row)
            value['message']['seq'] = manifest['source_seq']
            if fingerprint(value) != manifest['destination_fingerprint']:
                raise CoordError('imported history changed; source was not deleted')

    def remove_source(self, source, destination, request_id):
        manifests = list(destination.execute('SELECT * FROM history_move_items WHERE request_id=? ORDER BY source_seq', (request_id,)))
        present = []
        for manifest in manifests:
            row = source.execute('SELECT * FROM messages WHERE id=?', (manifest['message_id'],)).fetchone()
            if row:
                if fingerprint(record(source,row)) != manifest['source_fingerprint']:
                    raise CoordError('source history changed after import; no source deletion performed')
                present.append(row)
        if present and len(present) != len(manifests):
            raise CoordError('partially missing source history requires operator investigation')
        ids = {row['id'] for row in present}
        for row in source.execute('SELECT message_id,reply_to FROM message_replies'):
            if (row['message_id'] in ids) != (row['reply_to'] in ids):
                raise CoordError('source reply boundary changed after import')
        for row in source.execute('SELECT message_id,reply_to FROM reply_ack_idempotency'):
            if (row['message_id'] in ids) != (row['reply_to'] in ids):
                raise CoordError('source ACK-reply boundary changed after import')
        for row in present:
            for table in TABLES:
                source.execute(f'DELETE FROM {table} WHERE message_id=?', (row['id'],))
            if exists(source,'bridge_deliveries'):
                source.execute('DELETE FROM bridge_deliveries WHERE message_id=?', (row['id'],))
            source.execute('DELETE FROM messages WHERE id=?', (row['id'],))

    def execute(self, request_id, expected_count, expected_digest, authorize=None):
        try:
            uuid.UUID(request_id)
        except (ValueError, TypeError, AttributeError):
            raise CoordError('history move requires a request UUID')
        with self.connections(write=True) as (source, destination):
            if authorize:
                authorize(destination)
            prior = self.journal(destination, request_id)
            if prior and (prior['count'], prior['digest']) != (expected_count, expected_digest):
                raise CoordError('retry confirmation differs from the original move')
            if prior and prior['status'] == 'completed':
                return self.summary(destination, prior)
            self.quiescent(source)
            if not prior:
                items = self.selection(source)
                self.validate(source, destination, items)
                digest = hashlib.sha256(Coordinator._canonical_bytes([fingerprint(item) for item in items])).hexdigest()
                if not items or (len(items),digest) != (expected_count,expected_digest):
                    raise CoordError('selection changed or is empty; inspect a fresh preview before confirming')
                self.import_items(destination,request_id,items,digest)
                self.verify_import(destination,request_id)
                destination.commit()
                destination.execute('BEGIN IMMEDIATE')
            if authorize:
                authorize(destination)
            self.verify_import(destination,request_id)
            self.remove_source(source,destination,request_id)
            if authorize:
                authorize(destination)
            source.commit()
            destination.execute("UPDATE history_moves SET status='completed',completed_at=? WHERE request_id=?", (time.time(),request_id))
            destination.commit()
            return self.summary(destination,self.journal(destination,request_id))
