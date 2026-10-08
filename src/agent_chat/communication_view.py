"""Bounded cross-project history reads without coordinator initialization or ACKs."""
import base64
import contextlib
import json
import math
import sqlite3
from pathlib import Path
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
    def serialize(db,rows,project,full=False):
        # Reuse the existing read-only public serializer; no inbox/message reads.
        from .web import message_rows
        result=message_rows(rows,agent_labels(db),db)
        for item in result:
            item.update(project_id=project['id'],project_name=project['name'])
            if not full and len(item['body'])>4000:
                item['body']=item['body'][:4000];item['body_truncated']=True
            else:item['body_truncated']=False
        return result

    def page(self, *, project=None, agent=None, q=None, before=None, limit=50):
        if type(limit) is not int or not 1<=limit<=100: raise CoordError('limit must be1–100')
        for value in (project,agent,q):
            if value is not None and (not isinstance(value,str) or len(value)>256): raise CoordError('invalid communication filter')
        catalog=self.catalog();known={item['id']:item for item in catalog}
        if project and project not in known: raise CoordError('project is not registered in this server')
        boundary=before_key(before)
        if boundary and boundary[1] not in known: raise CoordError('cursor project is not registered')
        selected=[known[project]] if project else catalog
        merged=[];errors=[];loaded=0
        for item in selected:
            try:
                path=self.projects.db_path(item['id'])  # Authoritative registry mapping only.
                with contextlib.closing(connection(path)) as db:
                    clauses=[];args=[]
                    if agent:
                        clauses.append('(sender_session=? OR recipient_session=?)');args.extend((agent,agent))
                    if q:
                        clauses.append('instr(lower(body),lower(?))>0');args.append(q)
                    if boundary:
                        clauses.append('(created_at,?,seq,id)<(?,?,?,?)');args.extend((item['id'],*boundary))
                    where=' WHERE '+' AND '.join(clauses) if clauses else ''
                    rows=db.execute('SELECT seq,id,sender_session,recipient_session,body,created_at,acked_at FROM messages'+where+
                                    ' ORDER BY created_at DESC,seq DESC,id DESC LIMIT ?',(*args,limit+1)).fetchall()
                    merged.extend(self.serialize(db,rows,item));loaded+=1
            except (sqlite3.Error,OSError,CoordError) as error:
                errors.append(dict(project_id=item['id'],project_name=item['name'],error=str(error)))
        merged.sort(key=self.key,reverse=True);more=len(merged)>limit;page=merged[:limit]
        return dict(messages=list(reversed(page)),has_more=more,
                    next_cursor=cursor(self.key(page[-1])) if more and page else None,
                    projects=catalog,loaded_projects=loaded,selected_projects=len(selected),partial=bool(errors),errors=errors,
                    read_only=True)

    def message(self,project,message_id):
        if not isinstance(message_id,str) or not 1<=len(message_id)<=256: raise CoordError('invalid message ID')
        known={item['id']:item for item in self.catalog()}
        if project not in known: raise CoordError('project is not registered in this server')
        with contextlib.closing(connection(self.projects.db_path(project))) as db:
            rows=db.execute('SELECT seq,id,sender_session,recipient_session,body,created_at,acked_at FROM messages WHERE id=?',(message_id,)).fetchall()
            if not rows: raise CoordError('message not found in selected project')
            return dict(message=self.serialize(db,rows,known[project],full=True)[0],read_only=True)
