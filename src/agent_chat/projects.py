"""Project registry with one isolated coordinator database per project."""
import contextlib
import os
from pathlib import Path
import sqlite3
import time
import uuid

from .core import Coordinator, CoordError


class Projects:
    def __init__(self, database):
        self.database = Path(database).expanduser().resolve()
        self.path = Path(str(self.database) + '.projects.sqlite3')
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as db:
            db.execute('CREATE TABLE IF NOT EXISTS projects (id TEXT PRIMARY KEY, name TEXT NOT NULL, name_key TEXT UNIQUE NOT NULL, created_at REAL NOT NULL)')
            db.execute("INSERT OR IGNORE INTO projects VALUES('default','Default project','default project',?)", (time.time(),))
        os.chmod(self.path, 0o600)

    @contextlib.contextmanager
    def connection(self):
        db = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        db.row_factory = sqlite3.Row
        try: yield db
        finally: db.close()

    @staticmethod
    def name(value):
        if not isinstance(value, str): raise CoordError('project name must be text')
        value = value.strip()
        if not value or len(value) > 80 or any(ord(c) < 32 for c in value):
            raise CoordError('project name must contain 1–80 characters without control characters')
        return value

    def list(self):
        with self.connection() as db:
            return [dict(row) for row in db.execute("SELECT id,name,created_at FROM projects ORDER BY id<>'default',created_at,id")]

    def get(self, project='default'):
        if not isinstance(project, str) or not project:
            raise CoordError('project ID is required')
        with self.connection() as db:
            row = db.execute('SELECT id,name,created_at FROM projects WHERE id=?', (project,)).fetchone()
        if not row: raise CoordError('unknown project; select an existing project or create one explicitly')
        return dict(row)

    def db_path(self, project='default'):
        item = self.get(project)
        if item['id'] == 'default': return self.database
        return self.database.parent / (self.database.name + '.projects') / item['id'] / 'state.sqlite3'

    def create(self, name):
        name = self.name(name)
        key = 'project_' + uuid.uuid4().hex
        path = self.database.parent / (self.database.name + '.projects') / key / 'state.sqlite3'
        with self.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            try:
                if db.execute('SELECT 1 FROM projects WHERE name_key=?', (name.casefold(),)).fetchone():
                    raise CoordError('a project with this name already exists')
                Coordinator(path).close()
                os.chmod(path, 0o600)
                # Lets child commands keep an already resolved DB while inheriting
                # their project ID, without interpreting it as a second registry.
                c = sqlite3.connect(path)
                try:
                    c.execute("INSERT INTO meta(key,value) VALUES('project_id',?)", (key,))
                    c.execute("INSERT INTO meta(key,value) VALUES('project_registry',?)", (str(self.database),))
                    c.commit()
                finally: c.close()
                db.execute('INSERT INTO projects VALUES(?,?,?,?)', (key, name, name.casefold(), time.time()))
                db.commit()
            except BaseException:
                db.rollback()
                if path.exists():
                    for suffix in ('', '-wal', '-shm'):
                        Path(str(path)+suffix).unlink(missing_ok=True)
                    path.parent.rmdir()
                raise
        return self.get(key)

    def rename(self, project, name):
        self.get(project)
        name = self.name(name)
        with self.connection() as db:
            try:
                db.execute('UPDATE projects SET name=?,name_key=? WHERE id=?', (name, name.casefold(), project))
            except sqlite3.IntegrityError as error:
                raise CoordError('a project with this name already exists') from error
        return self.get(project)


def resolve_database(database, project):
    """Never create a project implicitly or redirect an already scoped database."""
    path = Path(database).expanduser().resolve()
    if path.is_file():
        with contextlib.closing(sqlite3.connect(path.as_uri()+'?mode=ro', uri=True)) as db:
            scoped = db.execute("SELECT value FROM meta WHERE key='project_id'").fetchone()
        if scoped:
            if project and project != scoped[0]: raise CoordError('database belongs to another project')
            return path
    if not project: return path
    selected = Projects(path).db_path(project)
    if project != 'default' and not selected.is_file():
        raise CoordError('project database is missing; restore it before continuing')
    return selected
