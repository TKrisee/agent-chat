"""Attachment metadata for visible rooms, independent of message history windows."""
import contextlib
import sqlite3
from urllib.parse import quote

from .communication_view import CommunicationView, before_key, connection, cursor
from .core import CoordError


class AttachmentGallery(CommunicationView):
    def page(self, *, project=None, q=None, before=None, limit=48, root=None):
        if type(limit) is not int or not 1 <= limit <= 100:
            raise CoordError('limit must be 1–100')
        if any(value is not None and (not isinstance(value, str) or len(value) > 256)
               for value in (project, q)):
            raise CoordError('invalid attachment filter')
        catalog = self.catalog()
        known = {item['id']: item for item in catalog}
        if root:
            project = root[0]
        if project and project not in known:
            raise CoordError('project is not registered in this server')
        boundary = before_key(before)
        if boundary and boundary[1] not in known:
            raise CoordError('cursor project is not registered')
        selected = [known[project]] if project else catalog
        merged, errors = [], []
        for item in selected:
            try:
                with contextlib.closing(connection(self.projects.db_path(item['id']))) as db:
                    from .web import attachment_table, batch_table
                    if not attachment_table(db):
                        continue
                    if root:
                        condition = '((m.sender_session=? AND m.recipient_session=?) OR (m.sender_session=? AND m.recipient_session=?))'
                        args = [root[1], root[2], root[2], root[1]]
                        if batch_table(db):
                            condition += ' AND NOT EXISTS (SELECT 1 FROM message_batches b WHERE b.message_id=m.id)'
                    else:
                        condition, values, _ = self.private_filter(db)
                        args = list(values)
                    clauses = ['(' + condition + ')']
                    if not root and batch_table(db):
                        # One logical group send stores identical ordered files
                        # on every recipient copy. Keep one message, all its files.
                        clauses.append('NOT EXISTS (SELECT 1 FROM message_batches b '
                            'JOIN message_batches sibling ON sibling.batch_id=b.batch_id '
                            'JOIN messages earlier ON earlier.id=sibling.message_id '
                            'WHERE b.message_id=m.id AND earlier.seq<m.seq)')
                    if q:
                        clauses.append('instr(lower(a.name),lower(?))>0')
                        args.append(q)
                    if boundary:
                        clauses.append('(m.created_at,?,m.seq,a.id)<(?,?,?,?)')
                        args.extend((item['id'], *boundary))
                    rows = db.execute('SELECT a.id,a.name,a.mime,a.size,m.created_at,m.seq FROM attachments a '
                        'JOIN messages m ON m.id=a.message_id WHERE ' + ' AND '.join(clauses) +
                        ' ORDER BY m.created_at DESC,m.seq DESC,a.id DESC LIMIT ?', (*args, limit + 1))
                    for row in rows:
                        merged.append(dict(row, project_id=item['id'], project_name=item['name'],
                            url=('/api/root/gallery-attachments/' if root else '/api/gallery-attachments/') + quote(row['id'], safe='') + '?project=' + quote(item['id'], safe='')))
            except (sqlite3.Error, OSError, CoordError):
                errors.append(dict(project_id=item['id'], project_name=item['name'], error='Attachments unavailable'))
        merged.sort(key=self.key, reverse=True)
        page = merged[:limit]
        return dict(attachments=page, has_more=len(merged) > limit,
                    next_cursor=cursor(self.key(page[-1])) if len(merged) > limit else None,
                    projects=selected if root else catalog, partial=bool(errors), errors=errors)
