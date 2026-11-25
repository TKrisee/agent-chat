"""Explicit, resumable direct inert project-artifact transfer with immutable bounded chunks.

Python 3.10 compatible: no blobopen, full-file allocation or alternate authority.
Final attachment metadata references the original durable chunk rows.
"""
import base64
import codecs
import hashlib
from pathlib import Path
import re
import time
import uuid

from .core import CoordError, MAX_ATTACHMENT_TOTAL_SIZE, PACKAGE_EXTENSIONS, PROJECT_EXTENSIONS

CHUNK = 1024 * 1024
LARGE_EXTENSIONS = {'.txt': 'text/plain', '.diff': 'text/plain', **PACKAGE_EXTENSIONS, **PROJECT_EXTENSIONS}


def mime_for(name):
    mime = LARGE_EXTENSIONS.get(Path(name).suffix.lower())
    if mime is None:
        raise CoordError('large transfer supports TXT, diff, Unity package, ZIP, ITR draft or EXR')
    return mime

_CONTROL = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]')


def _schema(db):
    db.execute('''CREATE TABLE IF NOT EXISTS attachment_transfers (
        id TEXT PRIMARY KEY, project TEXT NOT NULL, sender TEXT NOT NULL,
        host TEXT NOT NULL, recipient TEXT NOT NULL, name TEXT NOT NULL,
        size INTEGER NOT NULL, sha256 TEXT NOT NULL, body TEXT NOT NULL,
        reply_to TEXT, fingerprint TEXT NOT NULL, received INTEGER NOT NULL DEFAULT 0,
        state TEXT NOT NULL CHECK(state IN ('uploading','published','cancelled')),
        message_id TEXT, attachment_id TEXT UNIQUE, created_at REAL NOT NULL)''')
    db.execute('''CREATE UNIQUE INDEX IF NOT EXISTS one_active_attachment_transfer
        ON attachment_transfers(sender) WHERE state='uploading' ''')
    db.execute('''CREATE TABLE IF NOT EXISTS attachment_transfer_assignments (
        transfer_id TEXT PRIMARY KEY, task_id TEXT NOT NULL,
        FOREIGN KEY(transfer_id) REFERENCES attachment_transfers(id))''')
    for action in ('UPDATE', 'DELETE'):
        db.execute(f'''CREATE TRIGGER IF NOT EXISTS immutable_transfer_assignment_{action.lower()}
            BEFORE {action} ON attachment_transfer_assignments
            BEGIN SELECT RAISE(ABORT,'attachment transfer assignment is immutable'); END''')
    db.execute('''CREATE TABLE IF NOT EXISTS attachment_transfer_chunks (
        transfer_id TEXT NOT NULL, offset INTEGER NOT NULL, sha256 TEXT NOT NULL,
        content BLOB NOT NULL, PRIMARY KEY(transfer_id,offset),
        FOREIGN KEY(transfer_id) REFERENCES attachment_transfers(id))''')
    db.execute('''CREATE TRIGGER IF NOT EXISTS immutable_attachment_transfer_identity
        BEFORE UPDATE OF id,project,sender,host,recipient,name,size,sha256,body,reply_to,fingerprint,created_at
        ON attachment_transfers BEGIN SELECT RAISE(ABORT,'attachment transfer identity is immutable'); END''')
    for action in ('INSERT', 'UPDATE', 'DELETE'):
        record = 'NEW' if action == 'INSERT' else 'OLD'
        db.execute(f'''CREATE TRIGGER IF NOT EXISTS immutable_published_chunks_{action.lower()}
            BEFORE {action} ON attachment_transfer_chunks
            WHEN EXISTS (SELECT 1 FROM attachment_transfers WHERE id={record}.transfer_id AND state='published')
            BEGIN SELECT RAISE(ABORT,'published attachment bytes are immutable'); END''')


def _id(value):
    try:
        normalized = str(uuid.UUID(value))
    except (ValueError, TypeError, AttributeError) as error:
        raise CoordError('transfer ID must be a UUID') from error
    if value != normalized:
        raise CoordError('transfer ID must be a canonical UUID')
    return value


def _hash(value):
    if not isinstance(value, str) or not re.fullmatch('[0-9a-f]{64}', value):
        raise CoordError('transfer hash must be SHA256')
    return value


def _owned(db, coord, project, host, transfer_id):
    _id(transfer_id)
    row = db.execute('SELECT * FROM attachment_transfers WHERE id=?', (transfer_id,)).fetchone()
    if not row or (row['project'], row['sender'], row['host']) != (project, coord.require_session(), host):
        raise CoordError('attachment transfer is not owned by this project, sender and host')
    return row


def _receipt(row):
    return {'transfer_id': row['id'], 'received': row['received'], 'size': row['size'],
            'sha256': row['sha256'], 'state': row['state'], 'message_id': row['message_id'],
            'attachment_id': row['attachment_id'], 'chunk_size': CHUNK}


def begin(coord, project, host, params):
    fields = {'transfer_id', 'to', 'name', 'size', 'sha256', 'body', 'reply_to'}
    if set(params) not in (fields, fields | {'assignment_id'}):
        raise CoordError('large transfer requires exact message and attachment fields')
    assignment_id = params.get('assignment_id')
    if 'assignment_id' in params and (not isinstance(assignment_id,str) or not assignment_id):
        raise CoordError('large transfer assignment link must be a nonempty task ID')
    transfer_id = _id(params['transfer_id'])
    size, name = params['size'], params['name']
    if type(size) is not int or not 0 < size <= MAX_ATTACHMENT_TOTAL_SIZE:
        raise CoordError('large attachment must be between 1 byte and 500 MiB')
    if (not isinstance(name, str) or name != coord._attachment_name(name)
            or Path(name).suffix.lower() not in LARGE_EXTENSIONS):
        raise CoordError('large transfer supports TXT, diff, Unity package, ZIP, ITR draft or EXR')
    _hash(params['sha256'])
    if (not isinstance(params['body'], str) or len(params['body'].encode('utf-8')) > 65536
            or not isinstance(params['to'], str)
            or params['reply_to'] is not None and not isinstance(params['reply_to'], str)):
        raise CoordError('large transfer message fields are invalid')
    sender = coord.require_session()
    fingerprint = hashlib.sha256(coord._canonical_bytes(dict(params, project=project, sender=sender, host=host))).hexdigest()
    with coord.tx() as db:
        _schema(db)
        existing = db.execute('SELECT * FROM attachment_transfers WHERE id=?', (transfer_id,)).fetchone()
        if existing:
            row = _owned(db, coord, project, host, transfer_id)
            if row['fingerprint'] != fingerprint:
                raise CoordError('attachment transfer retry differs from its immutable message or file')
            link = db.execute('SELECT task_id FROM attachment_transfer_assignments WHERE transfer_id=?',(transfer_id,)).fetchone()
            if (link['task_id'] if link else None) != assignment_id:
                raise CoordError('attachment transfer assignment differs from retained intent')
            if row['state'] != 'published' and assignment_id is not None and coord._assignment_provenance(db,assignment_id,sender,row['recipient']) is None:
                raise CoordError('assignment link requires current owned task and same-manager peer')
            if row['state'] == 'cancelled':
                raise CoordError('attachment transfer was cancelled')
            return _receipt(row)
        # Use exact session IDs so a label change cannot retarget a staged file.
        if not db.execute('SELECT 1 FROM sessions WHERE id=?', (params['to'],)).fetchone():
            raise CoordError('large transfer requires an exact existing recipient session')
        if assignment_id is not None and coord._assignment_provenance(db,assignment_id,sender,params['to']) is None:
            raise CoordError('assignment link requires current owned task and same-manager peer')
        if params['reply_to'] is not None:
            coord._reply_parent(db, params['reply_to'], sender, params['to'])
        if db.execute("SELECT 1 FROM attachment_transfers WHERE sender=? AND state='uploading'", (sender,)).fetchone():
            raise CoordError('sender already has an active attachment transfer; resume or cancel it')
        db.execute('''INSERT INTO attachment_transfers
            (id,project,sender,host,recipient,name,size,sha256,body,reply_to,fingerprint,state,created_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,'uploading',?)''',
            (transfer_id, project, sender, host, params['to'], name, size, params['sha256'],
             params['body'], params['reply_to'], fingerprint, time.time()))
        if assignment_id is not None:
            db.execute('INSERT INTO attachment_transfer_assignments VALUES(?,?)',(transfer_id,assignment_id))
        return _receipt(_owned(db, coord, project, host, transfer_id))


def chunk(coord, project, host, params):
    if set(params) != {'transfer_id', 'offset', 'sha256', 'content_base64'}:
        raise CoordError('attachment chunk fields are invalid')
    offset, encoded = params['offset'], params['content_base64']
    if type(offset) is not int or offset < 0 or offset % CHUNK:
        raise CoordError('attachment chunk offset is invalid')
    digest = _hash(params['sha256'])
    if not isinstance(encoded, str) or len(encoded) > 4 * ((CHUNK + 2) // 3):
        raise CoordError('attachment chunk exceeds 1 MiB')
    try:
        content = base64.b64decode(encoded, validate=True)
    except ValueError as error:
        raise CoordError('attachment chunk encoding is invalid') from error
    if not content or len(content) > CHUNK or hashlib.sha256(content).hexdigest() != digest:
        raise CoordError('attachment chunk hash or size differs')
    with coord.tx() as db:
        _schema(db)
        row = _owned(db, coord, project, host, params['transfer_id'])
        if row['state'] == 'cancelled' or offset >= row['size'] or len(content) != min(CHUNK, row['size'] - offset):
            raise CoordError('attachment chunk is outside the retained transfer')
        if offset < row['received']:
            old = db.execute('SELECT sha256,content FROM attachment_transfer_chunks WHERE transfer_id=? AND offset=?',
                             (row['id'], offset)).fetchone()
            if not old or old['sha256'] != digest or old['content'] != content:
                raise CoordError('attachment chunk replay differs')
            return _receipt(row)
        if row['state'] != 'uploading' or offset != row['received']:
            raise CoordError('attachment chunk must match the committed offset')
        db.execute('INSERT INTO attachment_transfer_chunks VALUES(?,?,?,?)', (row['id'], offset, digest, content))
        db.execute('UPDATE attachment_transfers SET received=received+? WHERE id=?', (len(content), row['id']))
        return _receipt(_owned(db, coord, project, host, row['id']))


def progress(coord, project, host, params, *, cancel=False):
    if set(params) != {'transfer_id'}:
        raise CoordError('exact transfer ID is required')
    with coord.tx() as db:
        _schema(db)
        row = _owned(db, coord, project, host, params['transfer_id'])
        if cancel:
            if row['state'] == 'published':
                raise CoordError('published attachment cannot be cancelled')
            db.execute('DELETE FROM attachment_transfer_chunks WHERE transfer_id=?', (row['id'],))
            db.execute("UPDATE attachment_transfers SET state='cancelled' WHERE id=?", (row['id'],))
            row = _owned(db, coord, project, host, row['id'])
        return _receipt(row)


def chunks(db, transfer_id):
    """Yield one stored row at a time; never fetchall or join BLOBs."""
    for row in db.execute('SELECT offset,sha256,content FROM attachment_transfer_chunks WHERE transfer_id=? ORDER BY offset', (transfer_id,)):
        if len(row['content']) > CHUNK or hashlib.sha256(row['content']).hexdigest() != row['sha256']:
            raise CoordError('stored attachment chunk is invalid')
        yield row['offset'], row['content']


def ready_for_send(db, coord, transfer_id, project, host, recipient, body, reply_to, assignment_id=None):
    row = _owned(db, coord, project, host, transfer_id)
    link = db.execute('SELECT task_id FROM attachment_transfer_assignments WHERE transfer_id=?',(transfer_id,)).fetchone()
    if (row['recipient'], row['body'], row['reply_to'], link['task_id'] if link else None) != (recipient, body, reply_to, assignment_id):
        raise CoordError('large attachment final message differs from retained intent')
    if row['state'] == 'published':
        return row
    if row['state'] != 'uploading' or row['received'] != row['size']:
        raise CoordError('large attachment upload is incomplete')
    digest, offset = hashlib.sha256(), 0
    extension = Path(row['name']).suffix.lower()
    decoder = codecs.getincrementaldecoder('utf-8-sig')() if extension in ('.txt', '.diff') else None
    prefix, header = '', b''
    try:
        for position, data in chunks(db, transfer_id):
            if position != offset or len(data) != min(CHUNK, row['size'] - offset):
                raise CoordError('stored attachment chunks are incomplete')
            offset += len(data)
            digest.update(data)
            if len(header) < 32:
                header += data[:32-len(header)]
            if decoder is not None:
                text = decoder.decode(data, final=offset == row['size'])
                if _CONTROL.search(text):
                    raise CoordError('document attachments must not contain binary or control bytes')
                if len(prefix) < 2:
                    prefix += (text.lstrip() if not prefix else text)[:2-len(prefix)]
                if prefix == '#!':
                    raise CoordError('executable scripts are not allowed as attachments')
    except UnicodeError as error:
        raise CoordError('document attachments must contain UTF-8 text') from error
    if decoder is None:
        coord._validate_project_header(extension, header, row['size'])
    if offset != row['size'] or digest.hexdigest() != row['sha256']:
        raise CoordError('complete attachment size or SHA256 differs')
    return row


def send(coord, project, host, params):
    if set(params) != {'transfer_id'}:
        raise CoordError('large attachment publication requires the exact transfer ID')
    with coord.tx() as db:
        _schema(db)
        row = dict(_owned(db, coord, project, host, params['transfer_id']))
        link=db.execute('SELECT task_id FROM attachment_transfer_assignments WHERE transfer_id=?',(row['id'],)).fetchone()
        assignment_id=link['task_id'] if link else None
    return coord._send_prepared(row['recipient'], row['body'], [], row['reply_to'],
                                _large_transfer=(row['id'], project, host), assignment_id=assignment_id)


def published(db, attachment_id):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='attachment_transfers'").fetchone():
        return None
    return db.execute("SELECT * FROM attachment_transfers WHERE attachment_id=? AND state='published'", (attachment_id,)).fetchone()


def read_chunk(db, attachment, offset, prefix_sha256=None):
    row = published(db, attachment['id'])
    if (not row or (row['message_id'], row['name'], row['size'], row['sender']) !=
            (attachment['message_id'], attachment['name'], attachment['size'], attachment['sender_session'])
            or attachment['mime'] != mime_for(row['name']) or attachment['content'] != b''):
        raise CoordError('large attachment metadata is invalid')
    if type(offset) is not int or offset < 0 or offset > row['size'] or offset != row['size'] and offset % CHUNK:
        raise CoordError('attachment download offset is invalid')
    if prefix_sha256 is not None:
        _hash(prefix_sha256)
        digest = hashlib.sha256()
        length = 0
        for position, content in chunks(db, row['id']):
            if position >= offset:
                break
            if position != length or length + len(content) > offset:
                raise CoordError('attachment prefix is incomplete')
            digest.update(content)
            length += len(content)
        if length != offset or digest.hexdigest() != prefix_sha256:
            raise CoordError('resumed attachment prefix differs')
    content = b''
    if offset < row['size']:
        saved = db.execute('SELECT content,sha256 FROM attachment_transfer_chunks WHERE transfer_id=? AND offset=?', (row['id'], offset)).fetchone()
        if (not saved or len(saved['content']) != min(CHUNK, row['size'] - offset)
                or hashlib.sha256(saved['content']).hexdigest() != saved['sha256']):
            raise CoordError('stored attachment chunk is invalid')
        content = bytes(saved['content'])
    return {'attachment_id': attachment['id'], 'message_id': attachment['message_id'],
            'name': row['name'], 'mime': mime_for(row['name']), 'size': row['size'], 'sha256': row['sha256'],
            'offset': offset, 'chunk_sha256': hashlib.sha256(content).hexdigest(),
            'eof': offset + len(content) == row['size'], 'content': content}


def upload_file(call, source, transfer_id, message):
    """Real client path; a lost response leaves server progress for the same ID."""
    import os
    import stat
    source = Path(source)
    descriptor = os.open(source, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | os.O_NONBLOCK)
    with os.fdopen(descriptor, 'rb') as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode) or not 0 < before.st_size <= MAX_ATTACHMENT_TOTAL_SIZE:
            raise CoordError('large attachment must be a regular file of at most 500 MiB')
        digest = hashlib.sha256()
        while True:
            data = stream.read(CHUNK)
            if not data:
                break
            digest.update(data)
        size, sha = before.st_size, digest.hexdigest()
        identity = {'transfer_id': _id(transfer_id), 'to': message['to'], 'body': message['body'],
                    'reply_to': message.get('reply_to'), 'name': source.name, 'size': size, 'sha256': sha}
        if message.get('assignment_id') is not None:
            identity['assignment_id'] = message['assignment_id']
        def checked(result):
            fields = {'transfer_id', 'received', 'size', 'sha256', 'state', 'message_id', 'attachment_id', 'chunk_size'}
            if (not isinstance(result, dict) or set(result) != fields or result['transfer_id'] != transfer_id
                    or result['size'] != size or result['sha256'] != sha or result['chunk_size'] != CHUNK
                    or type(result['received']) is not int or not 0 <= result['received'] <= size
                    or result['received'] != size and result['received'] % CHUNK
                    or result['state'] not in ('uploading', 'published')):
                raise CoordError('large upload receipt differs from the exact file')
            return result['received']
        response = call('attachment-upload-begin', identity)
        offset = checked(response)
        if response['state'] != 'published':
            stream.seek(offset)
            while offset < size:
                current = os.fstat(stream.fileno())
                if (current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns) != (before.st_dev, before.st_ino, size, before.st_mtime_ns):
                    raise CoordError('large attachment source changed; retained transfer was not published')
                data = stream.read(min(CHUNK, size - offset))
                if len(data) != min(CHUNK, size - offset):
                    raise CoordError('large attachment source was truncated')
                response = call('attachment-upload-chunk', {'transfer_id': transfer_id, 'offset': offset,
                    'sha256': hashlib.sha256(data).hexdigest(), 'content_base64': base64.b64encode(data).decode('ascii')})
                expected = offset + len(data)
                if checked(response) != expected:
                    raise CoordError('large upload progress changed concurrently; reconcile the same transfer ID')
                offset = expected
        return call('attachment-upload-send', {'transfer_id': transfer_id})


def download_file(call, destination, params, project, session, host):
    """Resume only private, identity-bound partial bytes; publish without clobber."""
    import fcntl
    import os
    import secrets
    import stat
    path = Path(destination)
    if (not path.is_absolute() or path != Path(os.path.abspath(path)) or path.name in ('', '.', '..')
            or path.parent.resolve(strict=True) != path.parent):
        raise CoordError('attachment destination must be normalized and must not traverse a symlink')
    if not params.get('message_id') or params['source_project'] != project:
        raise CoordError('resumable download requires the exact original message and recipient project')
    identity = (project, session, host, params['source_project'], params['attachment_id'], params['message_id'])
    if any(not isinstance(value, str) or not value or '\t' in value or '\n' in value for value in identity):
        raise CoordError('resumable download identity is invalid')
    stem = '.agent-chat-resume-' + hashlib.sha256(('\t'.join(identity) + '\t' + str(path)).encode()).hexdigest()[:32]
    part, metadata, lock_name = stem + '.part', stem + '.tsv', stem + '.lock'
    directory = os.open(path.parent, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0) | getattr(os, 'O_NOFOLLOW', 0))
    lock = data_fd = None
    try:
        try:
            os.stat(path.name, dir_fd=directory, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            raise CoordError('attachment destination already exists')
        def private_file(name, flags):
            fd = os.open(name, flags | getattr(os, 'O_NOFOLLOW', 0), 0o600, dir_fd=directory)
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o600 or info.st_nlink != 1:
                os.close(fd)
                raise CoordError('attachment partial file is not an owned private regular file')
            return fd
        lock = private_file(lock_name, os.O_RDWR | os.O_CREAT)
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as error:
            raise CoordError('another download owns this destination') from error
        saved = None
        try:
            meta_fd = private_file(metadata, os.O_RDONLY)
        except FileNotFoundError:
            pass
        else:
            with os.fdopen(meta_fd, 'r', encoding='utf-8') as stream:
                raw = stream.read(4097)
            saved = raw.rstrip('\n').split('\t')
            if len(raw) > 4096 or len(saved) != 11 or tuple(saved[:6]) != identity:
                raise CoordError('attachment partial metadata differs from the requested route')
        data_fd = private_file(part, os.O_RDWR | os.O_CREAT)
        length = os.fstat(data_fd).st_size
        offset = 0
        if saved:
            try:
                offset = int(saved[9])
                total = int(saved[6])
            except ValueError as error:
                raise CoordError('attachment partial metadata is invalid') from error
            _hash(saved[7]); _hash(saved[10])
            if not 0 <= offset <= total <= MAX_ATTACHMENT_TOTAL_SIZE or offset > length or offset != total and offset % CHUNK:
                raise CoordError('attachment partial offset is invalid')
            # Bytes beyond the last fsynced checkpoint were never acknowledged.
            os.ftruncate(data_fd, offset)
        elif length:
            raise CoordError('attachment partial bytes have no matching checkpoint')
        digest = hashlib.sha256()
        os.lseek(data_fd, 0, os.SEEK_SET)
        remaining = offset
        while remaining:
            block = os.read(data_fd, min(CHUNK, remaining))
            if not block:
                raise CoordError('attachment partial bytes are incomplete')
            digest.update(block); remaining -= len(block)
        if saved and digest.hexdigest() != saved[10]:
            raise CoordError('attachment partial bytes differ from the saved prefix')
        first = True
        result = None
        while True:
            request = dict(params, offset=offset)
            if first:
                request['prefix_sha256'] = digest.hexdigest()
            result = call('attachment-download-chunk', request)
            fields = {'attachment_id','message_id','name','mime','size','sha256','offset','chunk_sha256',
                      'eof','content_base64','source_project','project','recipient_session','host_id'}
            if (not isinstance(result, dict) or set(result) != fields
                    or tuple(result[key] for key in ('project','recipient_session','host_id','source_project','attachment_id','message_id')) != identity
                    or result['offset'] != offset or type(result['size']) is not int
                    or not 0 < result['size'] <= MAX_ATTACHMENT_TOTAL_SIZE
                    or not isinstance(result['name'], str) or Path(result['name']).suffix.lower() not in LARGE_EXTENSIONS
                    or result['mime'] != mime_for(result['name'])
                    or '\t' in result['name'] or '\n' in result['name'] or len(result['name']) > 255
                    or type(result['eof']) is not bool or not isinstance(result['content_base64'], str)
                    or len(result['content_base64']) > 4 * ((CHUNK + 2) // 3)):
                raise CoordError('attachment chunk response identity or fields are invalid')
            _hash(result['sha256']); _hash(result['chunk_sha256'])
            bound = [*identity, str(result['size']), result['sha256'], result['name']]
            if saved and saved[:9] != bound:
                raise CoordError('attachment changed since the saved checkpoint')
            try:
                block = base64.b64decode(result['content_base64'], validate=True)
            except ValueError as error:
                raise CoordError('attachment chunk encoding is invalid') from error
            if (len(block) != min(CHUNK, result['size'] - offset)
                    or hashlib.sha256(block).hexdigest() != result['chunk_sha256']
                    or result['eof'] != (offset + len(block) == result['size'])):
                raise CoordError('attachment chunk size or digest differs')
            view = memoryview(block)
            while view:
                written = os.write(data_fd, view)
                if written <= 0:
                    raise CoordError('attachment partial write failed')
                view = view[written:]
            digest.update(block); offset += len(block)
            os.fsync(data_fd)
            saved = [*bound, str(offset), digest.hexdigest()]
            temporary = metadata + '.' + secrets.token_hex(8)
            meta_fd = private_file(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
            try:
                with os.fdopen(meta_fd, 'w', encoding='utf-8') as stream:
                    stream.write('\t'.join(saved) + '\n'); stream.flush(); os.fsync(stream.fileno())
                os.replace(temporary, metadata, src_dir_fd=directory, dst_dir_fd=directory)
                os.fsync(directory)
            finally:
                try: os.unlink(temporary, dir_fd=directory)
                except FileNotFoundError: pass
            first = False
            if result['eof']:
                break
        if digest.hexdigest() != result['sha256']:
            raise CoordError('complete attachment hash differs; partial file was not published')
        try:
            os.link(part, path.name, src_dir_fd=directory, dst_dir_fd=directory, follow_symlinks=False)
        except FileExistsError as error:
            raise CoordError('attachment destination already exists') from error
        os.fsync(directory)
        os.unlink(part, dir_fd=directory)
        os.unlink(metadata, dir_fd=directory)
        return {key: result[key] for key in ('attachment_id','message_id','name','mime','size','sha256',
                    'source_project','project','recipient_session','host_id')} | {'path': str(path)}
    finally:
        if data_fd is not None:
            os.close(data_fd)
        if lock is not None:
            os.close(lock)
        os.close(directory)
