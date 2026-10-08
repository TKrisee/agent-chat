"""Project registry with one isolated coordinator database per project."""
import contextlib
import os
from pathlib import Path, PurePosixPath
import sqlite3
import time
import threading
import uuid

from .core import Coordinator, CoordError


class Projects:
    def __init__(self, database):
        self.root_lock = threading.RLock()
        self.database = Path(database).expanduser().resolve()
        self.path = Path(str(self.database) + '.projects.sqlite3')
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as db:
            db.execute('CREATE TABLE IF NOT EXISTS projects (id TEXT PRIMARY KEY, name TEXT NOT NULL, name_key TEXT UNIQUE NOT NULL, created_at REAL NOT NULL)')
            columns = {row['name'] for row in db.execute('PRAGMA table_info(projects)')}
            for column in ('workspace', 'instructions'):
                if column not in columns:
                    db.execute(f'ALTER TABLE projects ADD COLUMN {column} TEXT')
            db.execute("INSERT OR IGNORE INTO projects(id,name,name_key,created_at) VALUES('default','Default project','default project',?)", (time.time(),))
            db.execute('CREATE TABLE IF NOT EXISTS project_workspaces (project_id TEXT NOT NULL,position INTEGER NOT NULL,path TEXT NOT NULL,PRIMARY KEY(project_id,position),UNIQUE(project_id,path))')
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

    @staticmethod
    def workspace_list(values):
        """Validate explicit ordered host paths; the bridge resolves real paths."""
        if not isinstance(values, list) or not 1 <= len(values) <= 32:
            raise CoordError('project workspaces must contain 1–32 explicit directories')
        result = []
        for value in values:
            if (not isinstance(value, str) or not value.startswith('/') or len(value) > 4096
                    or str(PurePosixPath(value)) != value or '..' in PurePosixPath(value).parts
                    or any(ord(char) < 32 or ord(char) == 127 for char in value)):
                raise CoordError('project workspace must be a normalized absolute path on the agent host')
            if any(Projects.overlaps(value, prior) for prior in result):
                raise CoordError('project workspace members must be distinct and must not nest')
            result.append(value)
        return result

    @staticmethod
    def overlaps(left, right):
        left, right = PurePosixPath(left), PurePosixPath(right)
        return left == right or left in right.parents or right in left.parents

    @staticmethod
    def _table(db, name):
        return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone())

    @staticmethod
    def _members(db, project, primary, table='project_workspaces'):
        values = ([row['path'] for row in db.execute(
            f'SELECT path FROM {table} WHERE project_id=? ORDER BY position', (project,))]
            if Projects._table(db, table) else [])
        return values or ([primary] if primary else [])

    def _assignment_active(self, row):
        """Match configure's retirement gate; unreadable state remains reserved."""
        target, request = row['orchestrator_session'], row['request_id']
        if not row['agent']:
            return False
        if not target and not request:
            return True
        path = (self.database if row['project_id'] == 'default' else
                self.database.parent / (self.database.name + '.projects') / row['project_id'] / 'state.sqlite3')
        try:
            with contextlib.closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)) as state:
                registered = target and state.execute('SELECT 1 FROM sessions WHERE id=?', (target,)).fetchone()
                pending = request and self._table(state, 'session_resets') and state.execute(
                    "SELECT 1 FROM session_resets WHERE id=? AND status NOT IN ('completed','cancelled','failed')", (request,)).fetchone()
                return bool(registered or pending or not target)
        except sqlite3.Error:
            return True

    def validate_workspace_changes(self, db, changes):
        """Validate final saved sets against saved, active and pending grants."""
        final = {row['id']: self._members(db, row['id'], row['workspace'])
                 for row in db.execute('SELECT id,workspace FROM projects')}
        final.update(changes)
        for project, members in changes.items():
            for other, roots in final.items():
                if project != other and any(self.overlaps(left, right) for left in members for right in roots):
                    raise CoordError('project workspace overlaps another saved project')
        if self._table(db, 'root_config'):
            neutral = db.execute('SELECT cwd FROM root_config WHERE singleton=1').fetchone()
            roots = [row['path'] for row in db.execute('SELECT path FROM root_workspace_roots ORDER BY position')] if self._table(db, 'root_workspace_roots') else []
            roots = roots or ([neutral['cwd']] if neutral else [])
            if any(self.overlaps(member, root) for members in changes.values() for member in members for root in roots):
                raise CoordError('project workspace overlaps the neutral root workspace')
        if self._table(db, 'root_projects'):
            for assignment in db.execute('SELECT * FROM root_projects WHERE cwd IS NOT NULL'):
                if not self._assignment_active(assignment):
                    continue
                roots = self._members(db, assignment['project_id'], assignment['cwd'], 'root_project_workspaces')
                path = self.database if assignment['project_id'] == 'default' else self.database.parent / (self.database.name + '.projects') / assignment['project_id'] / 'state.sqlite3'
                try:
                    with contextlib.closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)) as state:
                        if self._table(state, 'session_reset_workspaces'):
                            roots += [row[0] for row in state.execute(
                                "SELECT w.path FROM session_reset_workspaces w JOIN session_resets r ON r.id=w.request_id WHERE r.project_scope=1 AND r.kind='reset' AND (r.status NOT IN ('completed','failed','cancelled') OR (r.status='completed' AND EXISTS (SELECT 1 FROM bridge_bindings b WHERE b.session_id=r.target_session AND b.thread_id=r.new_thread_id)))")]
                except sqlite3.Error:
                    raise CoordError('project reset workspace state is unreadable; configuration withheld')
                if any(project != assignment['project_id'] and self.overlaps(member, root)
                       for project, members in changes.items() for member in members for root in roots):
                    raise CoordError('project workspace overlaps an active or pending root assignment')

        if self._table(db, 'root_reconfigurations') and self._table(db, 'root_reconfiguration_workspaces'):
            for pending in db.execute('SELECT * FROM root_reconfigurations'):
                path = self.database if pending['project_id'] == 'default' else self.database.parent / (self.database.name + '.projects') / pending['project_id'] / 'state.sqlite3'
                reserve = True
                try:
                    with contextlib.closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)) as state:
                        job = state.execute('SELECT status FROM session_resets WHERE id=?', (pending['request_id'],)).fetchone()
                        reserve = not job or job[0] not in ('failed', 'cancelled')
                except sqlite3.Error:
                    pass
                roots = self._members(db, pending['project_id'], None, 'root_reconfiguration_workspaces')
                if reserve and any(project != pending['project_id'] and self.overlaps(member, root)
                                   for project, members in changes.items() for member in members for root in roots):
                    raise CoordError('project workspace overlaps a pending root reconfiguration')

    @staticmethod
    def _save_members(db, project, members, table='project_workspaces'):
        db.execute(f'DELETE FROM {table} WHERE project_id=?', (project,))
        db.executemany(f'INSERT INTO {table}(project_id,position,path) VALUES(?,?,?)',
                       [(project, position, path) for position, path in enumerate(members)])

    def configure_workspaces(self, project, workspaces):
        return self.configure_workspaces_batch([{'id': project, 'workspaces': workspaces}])[0]

    def configure_workspaces_batch(self, assignments):
        """Atomic saved configuration only; native permissions are unchanged."""
        if not isinstance(assignments, list) or not 1 <= len(assignments) <= 32:
            raise CoordError('workspace configuration must contain 1–32 project assignments')
        changes = {}
        for item in assignments:
            if (not isinstance(item, dict) or set(item) != {'id', 'workspaces'}
                    or not isinstance(item['id'], str) or not item['id'] or item['id'] in changes):
                raise CoordError('workspace configuration requires distinct exact project IDs and workspaces')
            changes[item['id']] = self.workspace_list(item['workspaces'])
        with self.root_lock, self.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            try:
                for project in changes:
                    if not db.execute('SELECT 1 FROM projects WHERE id=?', (project,)).fetchone():
                        raise CoordError('unknown project; configure an existing exact project ID')
                self.validate_workspace_changes(db, changes)
                for project, members in changes.items():
                    db.execute('UPDATE projects SET workspace=? WHERE id=?', (members[0], project))
                    self._save_members(db, project, members)
                db.commit()
            except BaseException:
                db.rollback()
                raise
        return [self.get(project) for project in changes]

    def list(self):
        with self.connection() as db:
            return [dict(row, workspaces=self._members(db, row['id'], row['workspace'])) for row in
                    db.execute("SELECT id,name,created_at,workspace FROM projects ORDER BY id<>'default',created_at,id")]

    def get(self, project='default'):
        if not isinstance(project, str) or not project:
            raise CoordError('project ID is required')
        with self.connection() as db:
            row = db.execute('SELECT id,name,created_at,workspace,instructions FROM projects WHERE id=?', (project,)).fetchone()
            if not row: raise CoordError('unknown project; select an existing project or create one explicitly')
            return dict(row, workspaces=self._members(db, row['id'], row['workspace']))

    def db_path(self, project='default'):
        item = self.get(project)
        if item['id'] == 'default': return self.database
        return self.database.parent / (self.database.name + '.projects') / item['id'] / 'state.sqlite3'

    def create(self, name, workspace=None, instructions=None, *, workspaces=None):
        with self.root_lock:
            return self._create(name, workspace, instructions, workspaces)

    def _create(self, name, workspace, instructions, workspaces=None):
        name = self.name(name)
        if workspaces is not None:
            members = self.workspace_list(workspaces)
            if workspace is not None and workspace != members[0]:
                raise CoordError('workspace alias must equal the first workspace member')
        elif workspace is None or (isinstance(workspace, str) and not workspace.strip()):
            members = []
        else:
            members = self.workspace_list([workspace.strip() if isinstance(workspace, str) else workspace])
        workspace = members[0] if members else None
        if instructions is not None:
            if not isinstance(instructions, str) or len(instructions.encode('utf-8')) > 59000:
                raise CoordError('project instructions must be text of at most 59000 UTF-8 bytes')
            instructions = instructions if instructions.strip() else None
        key = 'project_' + uuid.uuid4().hex
        path = self.database.parent / (self.database.name + '.projects') / key / 'state.sqlite3'
        with self.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            try:
                if db.execute('SELECT 1 FROM projects WHERE name_key=?', (name.casefold(),)).fetchone():
                    raise CoordError('a project with this name already exists')
                self.validate_workspace_changes(db, {key: members})
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
                db.execute('INSERT INTO projects(id,name,name_key,created_at,workspace,instructions) VALUES(?,?,?,?,?,?)',
                           (key, name, name.casefold(), time.time(), workspace, instructions))
                self._save_members(db, key, members)
                db.commit()
            except BaseException:
                db.rollback()
                if path.exists():
                    for suffix in ('', '-wal', '-shm'):
                        Path(str(path)+suffix).unlink(missing_ok=True)
                    path.parent.rmdir()
                raise
        # Project creation commits before onboarding. Stable root projection IDs
        # make a crash or partial join repairable by the next registry read.
        from .root_orchestrator import RootOrchestrator
        RootOrchestrator(self).sync()
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
