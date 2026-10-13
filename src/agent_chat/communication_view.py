"""Bounded cross-project history reads without coordinator initialization or ACKs."""
import base64
import contextlib
import json
import math
import sqlite3
from pathlib import Path
from urllib.parse import quote
from .core import CoordError, agent_labels


def connection(path):
    db = sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro', uri=True, timeout=2)
    db.row_factory=sqlite3.Row
    db.execute('PRAGMA query_only=ON')
    return db


def cursor(key):
    return base64.urlsafe_b64encode(json.dumps(key,separators=(',',':')).encode()).decode().rstrip('=')


def before_key(value):
    if value is None: return None
    try:
        if not isinstance(value,str) or len(value)>2048: raise ValueError()
        data=json.loads(base64.b64decode(value+'='*(-len(value)%4),altchars=b'-_',validate=True))
        if (not isinstance(data,list) or len(data)!=4 or type(data[0]) not in (int,float)
                or not math.isfinite(data[0]) or type(data[2]) is not int or data[2]<1
                or not all(isinstance(data[i],str) and data[i] for i in (1,3))): raise ValueError()
        return tuple(data)
    except (ValueError,TypeError,UnicodeError): raise CoordError('invalid communication history cursor')


class CommunicationView:
    def __init__(self,projects): self.projects=projects

    def catalog(self):
        with contextlib.closing(connection(self.projects.path)) as db:
            return [dict(row) for row in db.execute('SELECT id,name FROM projects ORDER BY name,id')]

    @staticmethod
    def key(item): return (item['created_at'],item['project_id'],item['seq'],item['id'])

    @staticmethod
    def serialize(db,rows,project,project_operator=None,full=False):
        # Reuse the existing read-only public serializer; no inbox/message reads.
        from .web import message_rows
        result=message_rows(rows,agent_labels(db),db,project_operator)
        for item in result:
            item.update(project_id=project['id'],project_name=project['name'])
            item['history_cursor']=cursor(CommunicationView.key(item))
            for attachment in item['attachments']:
                attachment['url'] = '/api/gallery-attachments/' + attachment['id'] + '?project=' + quote(project['id'], safe='')
            if not full and len(item['body'])>4000:
                item['body']=item['body'][:4000];item['body_truncated']=True
            else:item['body_truncated']=False
        return result

    @staticmethod
    def private_filter(db, alias='m'):
        operators = [row['session_id'] for row in db.execute(
            'SELECT session_id FROM operator_identities ORDER BY session_id')]
        if len(operators) != 1:
            raise CoordError('aggregate private-pair identity is ' + (
                'ambiguous' if operators else 'unavailable'))
        from .web import project_message_filter
        condition, args = project_message_filter(db, operators[0], alias)
        return condition, args, operators[0]

    def page(self, *, project=None, agent=None, q=None, before=None, after=None, limit=50):
        if type(limit) is not int or not 1<=limit<=100: raise CoordError('limit must be1–100')
        for value in (project,agent,q):
            if value is not None and (not isinstance(value,str) or len(value)>256): raise CoordError('invalid communication filter')
        catalog=self.catalog();known={item['id']:item for item in catalog}
        if project and project not in known: raise CoordError('project is not registered in this server')
        if before is not None and after is not None: raise CoordError('before and after cannot be combined')
        boundary=before_key(after if after is not None else before)
        if boundary and boundary[1] not in known: raise CoordError('cursor project is not registered')
        selected=[known[project]] if project else catalog
        merged=[];errors=[];loaded=0
        for item in selected:
            try:
                path=self.projects.db_path(item['id'])  # Authoritative registry mapping only.
                with contextlib.closing(connection(path)) as db:
                    condition, filter_args, project_operator = self.private_filter(db)
                    clauses=['('+condition+')'];args=list(filter_args)
                    if agent:
                        clauses.append('(sender_session=? OR recipient_session=?)');args.extend((agent,agent))
                    if q:
                        clauses.append('instr(lower(body),lower(?))>0');args.append(q)
                    if boundary:
                        clauses.append('(created_at,?,seq,id)'+('>' if after is not None else '<')+'(?,?,?,?)');args.extend((item['id'],*boundary))
                    where=' WHERE '+' AND '.join(clauses) if clauses else ''
                    direction='ASC' if after is not None else 'DESC'
                    rows=db.execute('SELECT m.seq,m.id,m.sender_session,m.recipient_session,m.body,m.created_at,m.acked_at FROM messages m'+where+
                                    f' ORDER BY created_at {direction},seq {direction},id {direction} LIMIT ?',(*args,limit+1)).fetchall()
                    merged.extend(self.serialize(db,rows,item,project_operator));loaded+=1
            except (sqlite3.Error,OSError,CoordError) as error:
                errors.append(dict(project_id=item['id'],project_name=item['name'],error=str(error)))
        merged.sort(key=self.key,reverse=after is None);more=len(merged)>limit;page=merged[:limit]
        messages=page if after is not None else list(reversed(page))
        return dict(messages=messages,has_more=more,
                    next_cursor=cursor(self.key(page[-1])) if more and page else None,
                    oldest_cursor=cursor(self.key(messages[0])) if messages else None,
                    newest_cursor=cursor(self.key(messages[-1])) if messages else None,
                    projects=catalog,loaded_projects=loaded,selected_projects=len(selected),partial=bool(errors),errors=errors,
                    read_only=True)

    def message(self,project,message_id):
        if not isinstance(message_id,str) or not 1<=len(message_id)<=256: raise CoordError('invalid message ID')
        known={item['id']:item for item in self.catalog()}
        if project not in known: raise CoordError('project is not registered in this server')
        with contextlib.closing(connection(self.projects.db_path(project))) as db:
            condition,args,project_operator=self.private_filter(db)
            rows=db.execute('SELECT m.seq,m.id,m.sender_session,m.recipient_session,m.body,m.created_at,m.acked_at FROM messages m WHERE m.id=? AND ('+condition+')',
                            (message_id,*args)).fetchall()
            if not rows: raise CoordError('message not found in selected project')
            return dict(message=self.serialize(db,rows,known[project],project_operator,full=True)[0],read_only=True)
