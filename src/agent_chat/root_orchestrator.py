"""One control conversation, with durable, quiet identities in isolated projects.

This registry never dispatches model input itself. Project mains use the existing
fresh-conversation state machine and authenticated host worker.
"""
import contextlib
import sqlite3
from pathlib import PurePosixPath
import time
import uuid

from .core import Coordinator, CoordError


def projection(db, session):
    if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_projections'").fetchone():
        row = db.execute('SELECT * FROM root_projections WHERE session_id=?', (session,)).fetchone()
        return dict(row) if row else None
    return None


def quiet(db, session):
    return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='quiet_root_sessions'").fetchone()
                and db.execute('SELECT 1 FROM quiet_root_sessions WHERE session_id=?', (session,)).fetchone())


def assert_local_lifecycle(db, session):
    if projection(db, session):
        raise CoordError('global root binding is shared across projects; use root detach before local reset, stop, removal or rebinding')


def assert_management_target(db, caller, target, *, inspect=False):
    # Read-only inspection also supports adopting an existing project main.
    if inspect:
        return
    if projection(db, caller):
        if not db.execute('SELECT 1 FROM project_orchestrators WHERE session_id=? AND root_session=?', (target, caller)).fetchone():
            raise CoordError('global root manages project orchestrators only; delegate worker lifecycle to their project orchestrator')
        return
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='project_orchestrators'").fetchone():
        # Isolated/legacy project stores have no root hierarchy to enforce.
        return
    if caller == target:
        return
    if not db.execute('SELECT 1 FROM project_orchestrators WHERE session_id=?', (caller,)).fetchone():
        raise CoordError('agents may manage only their own lifecycle')
    current, seen = target, set()
    while current not in seen:
        seen.add(current)
        from .management import manager
        binding = db.execute('SELECT parent_session FROM bridge_bindings WHERE session_id=?', (current,)).fetchone()
        if (not binding or not binding['parent_session']) and manager(db, current) == caller:
            return
        if not binding or not binding['parent_session']:
            break
        current = binding['parent_session']
        if current == caller:
            return
    raise CoordError('project orchestrators manage only their own bound children; retained parentless mains need explicit root delegate-management')


def workspace(value):
    if (not isinstance(value, str) or not value.startswith('/') or len(value) > 4096
            or str(PurePosixPath(value)) != value or '..' in PurePosixPath(value).parts
            or any(ord(c) < 32 for c in value)):
        raise CoordError('project workspace must be a normalized absolute path on the execution host')
    return value


def overlaps(left, right):
    a, b = PurePosixPath(left), PurePosixPath(right)
    return a == b or a in b.parents or b in a.parents


class RootOrchestrator:
    def __init__(self, projects):
        self.projects = projects
        with projects.connection() as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS root_config (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    home_project TEXT NOT NULL, home_session TEXT NOT NULL,
                    thread_id TEXT NOT NULL, host_id TEXT NOT NULL, cwd TEXT NOT NULL,
                    agent TEXT NOT NULL, created_at REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS root_projects (
                    project_id TEXT PRIMARY KEY, session_id TEXT NOT NULL,
                    joined INTEGER NOT NULL DEFAULT 0, error TEXT,
                    cwd TEXT, agent TEXT, model TEXT, effort TEXT, prompt TEXT,
                    request_id TEXT, orchestrator_session TEXT, adopted INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS root_directory_owner (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1), home_project TEXT NOT NULL, home_session TEXT NOT NULL);
            ''')
            db.executescript('''
                CREATE TABLE IF NOT EXISTS root_team_operations (id TEXT PRIMARY KEY,root_session TEXT NOT NULL,created_at REAL NOT NULL,resumed_at REAL);
                CREATE TABLE IF NOT EXISTS root_team_operation_projects (request_id TEXT NOT NULL,project_id TEXT NOT NULL,root_session TEXT NOT NULL,error TEXT,PRIMARY KEY(request_id,project_id));
                CREATE TABLE IF NOT EXISTS root_project_workspaces (project_id TEXT NOT NULL,position INTEGER NOT NULL,path TEXT NOT NULL,PRIMARY KEY(project_id,position));
                CREATE TABLE IF NOT EXISTS root_workspace_roots (position INTEGER PRIMARY KEY,path TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS root_reconfigurations (project_id TEXT PRIMARY KEY,request_id TEXT UNIQUE NOT NULL,target_session TEXT NOT NULL,old_thread_id TEXT NOT NULL,prompt TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS root_reconfiguration_workspaces (project_id TEXT NOT NULL,position INTEGER NOT NULL,path TEXT NOT NULL,PRIMARY KEY(project_id,position));
            ''')
            if 'mode' not in {row['name'] for row in db.execute('PRAGMA table_info(root_config)')}:
                db.execute("ALTER TABLE root_config ADD COLUMN mode TEXT NOT NULL DEFAULT 'running'")

            from .root_restart import schema
            schema(db)

    def config(self):
        with self.projects.connection() as db:
            row = db.execute('SELECT * FROM root_config WHERE singleton=1').fetchone()
            return dict(row, workspaces=[item['path'] for item in db.execute('SELECT path FROM root_workspace_roots ORDER BY position')] or [row['cwd']]) if row else None

    def path(self, project):
        path = self.projects.db_path(project)
        if not path.is_file():
            raise CoordError('project database is missing; restore it before continuing')
        return path

    def authorize(self, project, session, host):
        root = self.config()
        if not root or (project, session, host) != (root['home_project'], root['home_session'], root['host_id']):
            raise CoordError('use the global root own canonical home project/session and host')
        with contextlib.closing(Coordinator(self.path(project), session)) as coord:
            binding = coord.db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?', (session,)).fetchone()
            owner = coord.db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (session,)).fetchone()
            if not binding or binding['thread_id'] != root['thread_id'] or not owner or owner['host_id'] != host:
                raise CoordError('canonical root binding or host changed; routing withheld')
        return root

    def fresh(self, root):
        with contextlib.closing(Coordinator(self.path(root['home_project']), root['home_session'])) as coord:
            coord._fresh(coord.db, root['home_session'])

    def active(self, root):
        with self.projects.connection() as db:
            if db.execute("SELECT 1 FROM root_fresh_requests WHERE status<>'completed'").fetchone():
                raise CoordError('root fresh-start is pending; inspect root fresh-status')
        if root['mode'] != 'running':
            raise CoordError('root is detaching; finish clean detach or use root resume-routing')
        with self.projects.connection() as db:
            if db.execute('SELECT 1 FROM root_team_operations WHERE resumed_at IS NULL').fetchone():
                raise CoordError('root stop-all barrier is active; resume its exact request before creating or reconfiguring work')

    def team_lifecycle(self, project, session, host, operation, request_id, confirmed=True):
        root = self.authorize(project, session, host)
        try:
            request_id = str(uuid.UUID(request_id))
        except (ValueError, TypeError, AttributeError):
            raise CoordError('root team lifecycle request_id must be a UUID')
        if operation not in ('stop-all','resume-all','stop-status') or (operation != 'stop-status' and confirmed is not True):
            raise CoordError('root team lifecycle requires explicit user confirmation')
        with self.projects.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            prior = db.execute('SELECT * FROM root_team_operations WHERE id=?', (request_id,)).fetchone()
            if not prior:
                if operation != 'stop-all':
                    raise CoordError('use the exact original stop-all request ID')
                if db.execute('SELECT 1 FROM root_team_operations WHERE resumed_at IS NULL').fetchone():
                    raise CoordError('another stop-all request is active; inspect or resume that exact request')
                db.execute('INSERT INTO root_team_operations VALUES(?,?,?,NULL)', (request_id,root['home_session'],time.time()))
                db.execute('''INSERT INTO root_team_operation_projects(request_id,project_id,root_session)
                              SELECT ?,project_id,session_id FROM root_projects WHERE joined=1''', (request_id,))
            elif prior['root_session'] != root['home_session']:
                raise CoordError('root team lifecycle request belongs to another root')
            scope = [dict(row) for row in db.execute('SELECT * FROM root_team_operation_projects WHERE request_id=? ORDER BY project_id', (request_id,))]
            db.commit()
        from .root_lifecycle import pause, resume, status
        result, failures = [], []
        for item in scope:
            try:
                with contextlib.closing(Coordinator(self.path(item['project_id']),item['root_session'])) as coord:
                    if operation == 'stop-all':
                        state = pause(coord,item['root_session'],request_id)
                    elif operation == 'resume-all':
                        state = resume(coord,item['root_session'],request_id)
                    else:
                        state = status(coord.db,request_id)
                    result.append(dict(project=item['project_id'],**state))
            except (CoordError,sqlite3.Error,OSError,TypeError) as error:
                failures.append(dict(project=item['project_id'],error=str(error)))
        if operation == 'resume-all' and not failures:
            with self.projects.connection() as db:
                db.execute('UPDATE root_team_operations SET resumed_at=COALESCE(resumed_at,?) WHERE id=?', (time.time(),request_id))
        return dict(request_id=request_id,operation=operation,projects=result,errors=failures,
                    barrier_complete=not failures,process_closure_proven=False)

    def enroll(self, project, session, host, expected_thread, confirmed):
        from .bridge_state import BridgeState
        from .session_reset import SessionResetState
        self.projects.get(project)
        if confirmed is not True:
            raise CoordError('root enrollment requires explicit confirmation')
        existing = self.config()
        if existing:
            self.authorize(project, session, host)
            if existing['thread_id'] != expected_thread:
                raise CoordError('root enrollment retry must retain the original thread')
            self.sync()
            return self.status()
        with contextlib.closing(Coordinator(self.path(project), session)) as coord:
            coord.require_session()
            BridgeState(coord)
            from .core import assert_agent_running
            assert_agent_running(coord.db, session)
            owner = coord.db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (session,)).fetchone()
            if not owner or owner['host_id'] != host:
                raise CoordError('root must use its authenticated existing host')
            SessionResetState(coord).clean(session, expected_thread)
            coord._fresh(coord.db, session)
            if coord.db.execute("SELECT 1 FROM session_resets WHERE requester_session=? AND status NOT IN ('completed','failed','cancelled')", (session,)).fetchone():
                raise CoordError('resolve your pending independent-agent creations before root enrollment')
            if coord.db.execute('SELECT 1 FROM resource_queue WHERE session=?', (session,)).fetchone():
                raise CoordError('cancel root resource queue entries before control-only enrollment')
            observed = coord.db.execute('SELECT * FROM agent_control_observations WHERE target_session=?', (session,)).fetchone()
            if not observed or observed['thread_id'] != expected_thread or time.time() - observed['checked_at'] > 120:
                raise CoordError('refresh your own agent-status through the existing bridge before root enrollment')
            cwd = workspace(observed['cwd'])
            from .agent_control import AgentControlState
            from .projects import Projects
            members = Projects.workspace_list(AgentControlState.observed_workspaces(coord.db, session, cwd))
            if members[0] != cwd:
                raise CoordError('root cwd must be its first observed workspace')
            agent = coord.db.execute('SELECT agent FROM sessions WHERE id=?', (session,)).fetchone()['agent']
        with self.projects.connection() as db:
            directory = db.execute('SELECT * FROM root_directory_owner WHERE singleton=1').fetchone()
            retained = list(db.execute('SELECT project_id,cwd FROM root_projects WHERE cwd IS NOT NULL')) if directory and (directory['home_project'], directory['home_session']) == (project, session) else []
            retained_members = [member for row in retained for member in self.projects._members(db, row['project_id'], row['cwd'], 'root_project_workspaces')]
            saved_members = [member for row in db.execute('SELECT id,workspace FROM projects') for member in self.projects._members(db, row['id'], row['workspace'])]
        if any(overlaps(root, member) for root in members for member in retained_members + saved_members):
            raise CoordError('root workspace overlaps a retained project; start from a neutral directory')
        with self.projects.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            old = db.execute('SELECT * FROM root_config WHERE singleton=1').fetchone()
            if old and (old['home_project'], old['home_session'], old['thread_id'], old['host_id'], old['cwd']) != (project, session, expected_thread, host, cwd):
                db.rollback()
                raise CoordError('a different global root is already enrolled; detach it cleanly first')
            if not old:
                directory = db.execute('SELECT * FROM root_directory_owner WHERE singleton=1').fetchone()
                if directory and (directory['home_project'], directory['home_session']) != (project, session):
                    # A distinct new root gets distinct registrations; do not
                    # borrow the prior root's retained identities.
                    db.execute('DELETE FROM root_projects')
                db.execute('INSERT OR REPLACE INTO root_directory_owner VALUES(1,?,?)', (project, session))
                db.execute('INSERT INTO root_config(singleton,home_project,home_session,thread_id,host_id,cwd,agent,created_at) VALUES(1,?,?,?,?,?,?,?)', (project, session, expected_thread, host, cwd, agent, time.time()))
            db.execute('DELETE FROM root_workspace_roots')
            db.executemany('INSERT INTO root_workspace_roots VALUES(?,?)', list(enumerate(members)))
            db.commit()
        self.sync()
        return self.status()

    def sync(self):
        """Repair partial onboarding with stable IDs, never recreate native threads."""
        root = self.config()
        if not root or root['mode'] == 'detached':
            return
        with self.projects.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            if db.execute('SELECT 1 FROM root_team_operations WHERE resumed_at IS NULL').fetchone() or db.execute("SELECT 1 FROM root_fresh_requests WHERE status<>'completed'").fetchone():
                return
            for project in self.projects.list():
                session = root['home_session'] if project['id'] == root['home_project'] else 'session_' + uuid.uuid5(uuid.NAMESPACE_URL, root['home_project'] + ':' + root['home_session'] + ':' + project['id']).hex
                db.execute('INSERT OR IGNORE INTO root_projects(project_id,session_id) VALUES(?,?)', (project['id'], session))
            rows = [dict(row, workspaces=self.projects._members(db, row['project_id'], row['cwd'], 'root_project_workspaces')) for row in db.execute('SELECT * FROM root_projects')]
            for row in rows:
                try:
                    self.join(root, row)
                    db.execute('UPDATE root_projects SET joined=1,error=NULL WHERE project_id=?', (row['project_id'],))
                except (CoordError, OSError, sqlite3.Error) as error:
                    db.execute('UPDATE root_projects SET joined=0,error=? WHERE project_id=?', (str(error), row['project_id']))
            db.commit()
        for row in rows:
            self.notify_project(root, row['project_id'])

    def join(self, root, item):
        from .bridge_state import BridgeState
        from .remote_service import _schema
        with contextlib.closing(Coordinator(self.path(item['project_id']), item['session_id'])) as coord:
            BridgeState(coord)
            _schema(coord)
            coord.db.execute('''CREATE TABLE IF NOT EXISTS root_projections (
                session_id TEXT PRIMARY KEY, home_project TEXT NOT NULL, home_session TEXT NOT NULL,
                thread_id TEXT NOT NULL, cwd TEXT)''')
            if 'paused' not in {row['name'] for row in coord.db.execute('PRAGMA table_info(root_projections)')}:
                coord.db.execute('ALTER TABLE root_projections ADD COLUMN paused INTEGER NOT NULL DEFAULT 0')
            for column in ('model','effort'):
                if column not in {row['name'] for row in coord.db.execute('PRAGMA table_info(root_projections)')}:
                    coord.db.execute(f'ALTER TABLE root_projections ADD COLUMN {column} TEXT')
            with coord.tx() as db:
                db.execute('CREATE TABLE IF NOT EXISTS quiet_root_sessions (session_id TEXT PRIMARY KEY)')
                db.execute('INSERT OR IGNORE INTO quiet_root_sessions VALUES(?)', (item['session_id'],))
                prior = db.execute('SELECT * FROM sessions WHERE id=?', (item['session_id'],)).fetchone()
                if not prior:
                    if db.execute('SELECT 1 FROM retired_sessions WHERE id=?', (item['session_id'],)).fetchone():
                        raise CoordError('root registration was retired; explicit root recovery required')
                    label = root['agent']
                    if db.execute('SELECT 1 FROM sessions WHERE agent=?', (label,)).fetchone():
                        label = 'root-' + item['session_id'][-12:]
                    db.execute('INSERT INTO sessions(id,agent,registered_at) VALUES(?,?,?)', (item['session_id'], label, time.time()))
                binding = db.execute('SELECT * FROM bridge_bindings WHERE session_id=?', (item['session_id'],)).fetchone()
                if binding and (binding['thread_id'] != root['thread_id'] or binding['parent_session']):
                    raise CoordError('root project binding changed; refusing to overwrite it')
                other = db.execute('SELECT session_id FROM bridge_bindings WHERE thread_id=? AND session_id<>?', (root['thread_id'], item['session_id'])).fetchone()
                if other:
                    raise CoordError('root thread already belongs to another identity in this project')
                owner = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (item['session_id'],)).fetchone()
                if owner and owner['host_id'] != root['host_id']:
                    raise CoordError('root project identity belongs to another host')
                if not owner:
                    db.execute('INSERT INTO remote_session_hosts VALUES(?,?,?,?)', (item['session_id'], root['host_id'], time.time(), time.time()))
                if not binding:
                    db.execute('INSERT INTO bridge_bindings VALUES(?,?,NULL,NULL,?)', (item['session_id'], root['thread_id'], time.time()))
                db.execute('INSERT OR REPLACE INTO root_projections VALUES(?,?,?,?,?,?,?,?)', (item['session_id'], root['home_project'], root['home_session'], root['thread_id'], item['cwd'], int(root['mode'] != 'running'), item.get('model'), item.get('effort')))
                db.execute('CREATE TABLE IF NOT EXISTS project_orchestrators (session_id TEXT PRIMARY KEY, root_session TEXT NOT NULL)')
                if item.get('orchestrator_session'):
                    db.execute('DELETE FROM project_orchestrators')
                    db.execute('INSERT INTO project_orchestrators VALUES(?,?)', (item['orchestrator_session'], item['session_id']))

    def notify_project(self, root, project):
        if project == root['home_project']:
            return
        with contextlib.closing(Coordinator(self.path(root['home_project']), root['home_session'])) as coord:
            coord.db.execute('CREATE TABLE IF NOT EXISTS root_project_notices (project_id TEXT PRIMARY KEY, message_id TEXT NOT NULL)')
            with coord.tx() as db:
                if db.execute('SELECT 1 FROM root_project_notices WHERE project_id=?', (project,)).fetchone():
                    return
                operator = db.execute("SELECT id FROM sessions WHERE agent='operator' ORDER BY registered_at LIMIT 1").fetchone()
                if not operator:
                    raise CoordError('root home operator identity is missing')
                mid = 'message_' + uuid.uuid4().hex
                item = self.projects.get(project)
                setup = (f"Saved workspaces: {', '.join(item['workspaces'])}. Operator supplied project instructions. "
                         f"Provision this project's orchestrator through root provision --project-id {project} with one saved request UUID; "
                         "the saved assignment is forwarded without loading it into root's conversation. "
                         if item.get('workspace') and item.get('instructions') else
                         "Project creation scope is incomplete; ask the operator for missing workspace/task, then configure or adopt. ")
                body = (f"Project registered: {item['name']} (project ID {project}). Your distinct quiet root registration is bound to this same conversation. "
                        + setup + "Inspect root status; retain an existing designated team rather than creating a duplicate. "
                        "Do not acquire project resource holds as root. This notice creates no native agent or resource ownership.")
                db.execute('INSERT INTO messages(id,sender_session,recipient_session,body,created_at) VALUES(?,?,?,?,?)',
                           (mid, operator['id'], root['home_session'], body, time.time()))
                db.execute('INSERT INTO root_project_notices VALUES(?,?)', (project, mid))

    def route(self, project, session, host, target_project):
        root = self.authorize(project, session, host)
        if root['mode'] == 'detached':
            raise CoordError('finish root detach before routing or enrolling again')
        self.projects.get(target_project)
        self.sync()
        with self.projects.connection() as db:
            row = db.execute('SELECT * FROM root_projects WHERE project_id=?', (target_project,)).fetchone()
        if not row or not row['joined']:
            raise CoordError('root project onboarding incomplete: ' + (row['error'] if row else target_project))
        return dict(project=target_project, session=row['session_id'], cwd=row['cwd'])

    def delegate_management(self, project, session, host, target_project, po, target, expected_thread, expected_host, request_id, confirmed):
        root = self.authorize(project, session, host)
        self.fresh(root)
        with self.projects.connection() as db:
            row = db.execute('SELECT * FROM root_projects WHERE project_id=? AND joined=1', (target_project,)).fetchone()
        if not row or row['orchestrator_session'] != po:
            raise CoordError('management requires exact enrolled project and current designated PO')
        from .management import delegate
        with contextlib.closing(Coordinator(self.path(target_project), row['session_id'])) as coord:
            return delegate(coord, row['session_id'], po, target, expected_thread, expected_host, request_id, confirmed)

    def provision(self, project, session, host, target_project, agent, request_id, confirmed, model=None, effort=None):
        self.authorize(project, session, host)
        item = self.projects.get(target_project)
        if not item.get('workspace') or not item.get('instructions'):
            raise CoordError('project creation needs workspace and instructions before provisioning')
        prompt = (f"Act as project orchestrator for {item['name']} (project ID {target_project}). "
                  "Manage this project's workers through agent-chat using your own project/session. "
                  "Keep goals, implementation and detailed project context with this team. Global root relays bounded operator questions/answers; "
                  "send only routing/readiness metadata unless an operator question needs limited project detail. Tag each question with project and request ID. "
                  "Read the agent-chat quickstart and agent guide; preserve exact resource ownership and receipt guarantees.\n\n"
                  "Operator's project assignment (literal):\n" + item['instructions'])
        return self.configure(project, session, host, target_project, agent, item['workspace'], prompt,
                              request_id, confirmed, model, effort, workspaces=item['workspaces'])

    def configure(self, project, session, host, target_project, agent, cwd, prompt, request_id, confirmed, model=None, effort=None, workspaces=None):
        from .root_team_reset import assert_ready
        with contextlib.closing(Coordinator(self.path(target_project))) as reset_coord:
            assert_ready(reset_coord.db)
        root = self.authorize(project, session, host)
        self.active(root)
        self.fresh(root)
        route = self.route(project, session, host, target_project)
        from .projects import Projects
        members = Projects.workspace_list(workspaces if workspaces is not None else [cwd])
        if cwd is not None and cwd != members[0]:
            raise CoordError('cwd must equal the first project workspace')
        cwd = members[0]
        self.separate_workspace(target_project, members)
        try:
            request_id = str(uuid.UUID(request_id))
        except (ValueError, TypeError, AttributeError):
            raise CoordError('project orchestration request_id must be a UUID')
        if (confirmed is not True or not isinstance(agent, str) or not agent.strip() or len(agent) > 64
                or not isinstance(prompt, str) or not prompt.strip() or len(prompt.encode()) > 60000):
            raise CoordError('project configuration requires confirmation, unique role and bounded prompt')
        for value, limit in ((model, 128), (effort, 32)):
            if value is not None and (not isinstance(value, str) or not value.strip() or len(value) > limit):
                raise CoordError('invalid project model or reasoning effort')
        values = (cwd, agent, model, effort, prompt, request_id)
        # A replacement requires explicit retirement of the former main and a
        # new UUID. Never replace a live registration or replay a cancelled job.
        with self.projects.connection() as db:
            prior = dict(db.execute('SELECT * FROM root_projects WHERE project_id=?', (target_project,)).fetchone())
            prior_members = self.projects._members(db, target_project, prior['cwd'], 'root_project_workspaces')
        replace = False
        if prior['agent']:
            with contextlib.closing(Coordinator(self.path(target_project))) as coord:
                target_exists = coord.db.execute('SELECT 1 FROM sessions WHERE id=?', (prior['orchestrator_session'],)).fetchone()
                pending = coord.db.execute("SELECT 1 FROM session_resets WHERE id=? AND status NOT IN ('completed','failed','cancelled')", (prior['request_id'],)).fetchone()
                replace = not target_exists and not pending and request_id != prior['request_id']
        with contextlib.closing(Coordinator(self.path(target_project))) as coord:
            from .session_reset import SessionResetState
            resets = SessionResetState(coord)
            raw = coord.db.execute('SELECT * FROM session_resets WHERE id=?', (request_id,)).fetchone()
            original = resets.job(request_id) if raw else None
            if original and (prior['request_id'] != request_id or original['requester_session'] != route['session']
                    or original['kind'] != 'create' or not original['project_scope']
                    or original['prompt'] != self.project_prompt(target_project, route['session'], prompt)
                    or original['requested_cwd'] != cwd or original['requested_workspaces'] != members or original['requested_model'] != model or original['requested_effort'] != effort
                    or (prior['orchestrator_session'] and prior['orchestrator_session'] != original['target_session'])):
                raise CoordError('project setup UUID belongs to a different creation; preserve original request and inspect it')
        with self.projects.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            old = db.execute('SELECT * FROM root_projects WHERE project_id=?', (target_project,)).fetchone()
            if old['agent'] and not replace and (tuple(old[key] for key in ('cwd', 'agent', 'model', 'effort', 'prompt', 'request_id')) != values or prior_members != members):
                db.rollback()
                raise CoordError('project orchestrator already configured; inspect original request or adopt an existing main')
            db.execute('UPDATE root_projects SET cwd=?,agent=?,model=?,effort=?,prompt=?,request_id=?,adopted=0 WHERE project_id=?', (*values, target_project))
            self.projects._save_members(db, target_project, members, 'root_project_workspaces')
            db.commit()
        self.join(root, dict(project_id=target_project, session_id=route['session'], cwd=cwd))
        # The registry records the immutable intent before project mutation. A
        # retry always reuses this UUID, including after a dropped HTTP response.
        from .bridge_state import BridgeState
        from .session_reset import SessionResetState
        with contextlib.closing(Coordinator(self.path(target_project), route['session'])) as coord:
            BridgeState(coord)
            coord._fresh(coord.db, route['session'])
            resets = SessionResetState(coord)
            existing = coord.db.execute('SELECT * FROM session_resets WHERE id=?', (request_id,)).fetchone()
            if existing and existing['old_thread_id'] != root['thread_id'] and existing['status'] == 'completed':
                job = resets.public(resets.job(request_id))
            else:
                job = resets.create(agent, root['thread_id'], self.project_prompt(target_project, route['session'], prompt), request_id, True,
                    model=model, reasoning_effort=effort, cwd=cwd, project_scope=True, project_orchestrator=True, workspaces=members)
        with self.projects.connection() as db:
            db.execute('UPDATE root_projects SET orchestrator_session=? WHERE project_id=?', (job['target_session'], target_project))
        self.sync()
        return dict(project=target_project, root_session=route['session'], creation=job)

    def retry_setup(self, project, session, host, target_project, request_id, confirmed):
        from .root_team_reset import assert_ready
        with contextlib.closing(Coordinator(self.path(target_project))) as reset_coord:
            assert_ready(reset_coord.db)
        self.authorize(project, session, host)
        with self.projects.connection() as db:
            row = db.execute('SELECT * FROM root_projects WHERE project_id=?', (target_project,)).fetchone()
        if not row or row['request_id'] != request_id or row['adopted']:
            raise CoordError('retry setup requires the exact original configured project UUID')
        with self.projects.connection() as db:
            members = self.projects._members(db, target_project, row['cwd'], 'root_project_workspaces')
        return self.configure(project, session, host, target_project, row['agent'], row['cwd'], row['prompt'], row['request_id'], confirmed, row['model'], row['effort'], workspaces=members)

    @staticmethod
    def project_prompt(project, root_session, prompt):
        return (f'You are the project orchestrator for exact project ID {project}. '
                'Keep project requirements, architecture, decisions, instructions, design reasoning and detailed logs in this conversation and project checkpoints. '
                'Delegate all coding, source inspection, investigation, testing, technical verification and independent artifact review to bounded workers; do not perform those activities personally, including as a quick check or review substitute. Retain coordination, requirements and decision context. '
                'Stay responsive through concise asynchronous status/blocker updates, exact assignment ownership and request IDs. '
                'Send root only concise reports containing project ID, goal, status, blocker, decision needed and checkpoint path. '
                f'Send reports to the exact root session {root_session} within this project; do not export project requirements as global defaults. '
                'Manage independent workers through supported agent-create/control commands. Exact holds, queues and receipts remain mandatory. '
                'Delegate local-work hygiene to bounded workers: use narrow ignore rules for local checkpoints, preserve source/assets/docs and local copies when untracking, and never delete broadly, rewrite history, reset or publish without separate authority. '
                'Use the project workspace AGENTS.md when present; do not change model, identity, permissions, bindings, bridge or service without separate supported authorization.\n\n' + prompt)

    def adopt(self, project, session, host, target_project, target, expected_thread, cwd, confirmed, workspaces=None):
        from .root_team_reset import assert_ready
        with contextlib.closing(Coordinator(self.path(target_project))) as reset_coord:
            assert_ready(reset_coord.db)
        root = self.authorize(project, session, host)
        self.active(root)
        self.fresh(root)
        self.route(project, session, host, target_project)
        from .projects import Projects
        members = Projects.workspace_list(workspaces if workspaces is not None else [cwd])
        if cwd is not None and cwd != members[0]:
            raise CoordError('cwd must equal the first project workspace')
        cwd = members[0]
        if confirmed is not True or any(overlaps(member, neutral) for member in members for neutral in root['workspaces']):
            raise CoordError('adoption requires confirmation and a separate project workspace')
        self.separate_workspace(target_project, members)
        with self.projects.connection() as db:
            prior = db.execute('SELECT request_id FROM root_projects WHERE project_id=?', (target_project,)).fetchone()
        with contextlib.closing(Coordinator(self.path(target_project))) as coord:
            if prior['request_id']:
                job = coord.db.execute('SELECT status FROM session_resets WHERE id=?', (prior['request_id'],)).fetchone()
                if job and job['status'] not in ('completed','cancelled','failed'):
                    raise CoordError('resolve the pending project creation before adopting another main')
            row = coord.db.execute('SELECT id,agent FROM sessions WHERE id=? OR agent=?', (target, target)).fetchone()
            binding = coord.db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?', (row['id'],)).fetchone() if row else None
            observed = coord.db.execute('SELECT * FROM agent_control_observations WHERE target_session=?', (row['id'],)).fetchone() if row else None
            owner = coord.db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (row['id'],)).fetchone() if row else None
            if (not binding or binding['thread_id'] != expected_thread or expected_thread == root['thread_id']
                    or not owner or owner['host_id'] != host or not observed or time.time() - observed['checked_at'] > 120
                    or observed['thread_id'] != expected_thread or observed['cwd'] != cwd):
                raise CoordError('adopt an exact existing main on this host after a fresh agent-status observation of its workspace')
            from .agent_control import AgentControlState
            if AgentControlState.observed_workspaces(coord.db, row['id'], cwd) != members:
                raise CoordError('adoption requires the exact observed project workspace member list')
        with self.projects.connection() as db:
            db.execute('UPDATE root_projects SET cwd=?,agent=?,orchestrator_session=?,model=?,effort=?,adopted=1 WHERE project_id=?', (cwd, row['agent'], row['id'], observed['model'], observed['reasoning_effort'], target_project))
            self.projects._save_members(db, target_project, members, 'root_project_workspaces')
        self.sync()
        return self.status()

    def separate_workspace(self, project, members):
        if isinstance(members, str):
            members = [members]
        with self.projects.connection() as db:
            self.projects.validate_workspace_changes(db, {project: members})

    def reconfigure_workspaces(self, project, session, host, target_project, expected_thread, workspaces, prompt, request_id, confirmed):
        from .root_team_reset import assert_ready
        with contextlib.closing(Coordinator(self.path(target_project))) as reset_coord:
            assert_ready(reset_coord.db)
        from .projects import Projects
        from .bridge_state import BridgeState
        from .session_reset import SessionResetState
        root = self.authorize(project, session, host)
        self.active(root)
        self.fresh(root)
        route = self.route(project, session, host, target_project)
        members = Projects.workspace_list(workspaces)
        try:
            request_id = str(uuid.UUID(request_id))
        except (TypeError, ValueError, AttributeError):
            raise CoordError('project reconfiguration request_id must be a UUID')
        if confirmed is not True or not isinstance(prompt, str) or not prompt.strip() or len(prompt.encode()) > 60000:
            raise CoordError('project reconfiguration requires confirmation and bounded prompt')
        with self.projects.connection() as db:
            prior = db.execute('SELECT * FROM root_projects WHERE project_id=?', (target_project,)).fetchone()
            pending = db.execute('SELECT * FROM root_reconfigurations WHERE project_id=?', (target_project,)).fetchone()
            collision = db.execute('SELECT project_id FROM root_reconfigurations WHERE request_id=? AND project_id<>?', (request_id, target_project)).fetchone()
            if collision:
                raise CoordError('project reconfiguration UUID already belongs to another project')
        if not prior or not prior['orchestrator_session']:
            raise CoordError('reconfigure an exact existing project orchestrator')
        with contextlib.closing(Coordinator(self.path(target_project), route['session'])) as coord:
            BridgeState(coord)
            resets = SessionResetState(coord)
            if pending and pending['request_id'] == request_id:
                original = resets.job(request_id)
                if (pending['old_thread_id'], pending['prompt'], original['requested_workspaces']) != (expected_thread, prompt, members):
                    raise CoordError('project reconfiguration retry payload differs from the original')
                self.reconcile_reconfiguration(target_project)
                return dict(project=target_project, reconfiguration=resets.public(original))
            if pending and resets.job(pending['request_id'])['status'] not in ('completed', 'cancelled', 'failed'):
                raise CoordError('resolve the pending project workspace reconfiguration first')
            target = prior['orchestrator_session']
            binding = coord.db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?', (target,)).fetchone()
            existing = coord.db.execute('SELECT 1 FROM session_resets WHERE id=?', (request_id,)).fetchone()
            if not existing and (not binding or binding['thread_id'] != expected_thread or expected_thread == root['thread_id']):
                raise CoordError('project reconfiguration requires the exact current project main thread')
            self.separate_workspace(target_project, members)
            job = resets.request(target, expected_thread, self.project_prompt(target_project, route['session'], prompt),
                                 request_id, True, project_scope=True, workspaces=members)
        # Existing reset UUID is durable before the bounded registry journal.
        # If this write fails, retrying the same request reconstructs the intent.
        with self.projects.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('INSERT OR REPLACE INTO root_reconfigurations VALUES(?,?,?,?,?)',
                       (target_project, request_id, target, expected_thread, prompt))
            self.projects._save_members(db, target_project, members, 'root_reconfiguration_workspaces')
            db.commit()
        return dict(project=target_project, reconfiguration=job)

    def reconcile_reconfiguration(self, project):
        from .session_reset import SessionResetState
        with self.projects.connection() as db:
            pending = db.execute('SELECT * FROM root_reconfigurations WHERE project_id=?', (project,)).fetchone()
        if not pending:
            return None
        with contextlib.closing(Coordinator(self.path(project))) as coord:
            resets = SessionResetState(coord)
            job = resets.job(pending['request_id'])
            binding = coord.db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?', (pending['target_session'],)).fetchone()
        if job['status'] == 'completed':
            if not binding or binding['thread_id'] != job['new_thread_id']:
                raise CoordError('completed reconfiguration binding differs; preserve current assignment')
            with self.projects.connection() as db:
                db.execute('BEGIN IMMEDIATE')
                db.execute('UPDATE root_projects SET cwd=? WHERE project_id=?', (job['requested_workspaces'][0], project))
                self.projects._save_members(db, project, job['requested_workspaces'], 'root_project_workspaces')
                db.execute('DELETE FROM root_reconfiguration_workspaces WHERE project_id=?', (project,))
                row = dict(db.execute('SELECT * FROM root_projects WHERE project_id=?', (project,)).fetchone())
                db.commit()
            root = self.config()
            if root and root['mode'] == 'running':
                self.join(root, row)
        elif job['status'] in ('failed', 'cancelled'):
            with self.projects.connection() as db:
                db.execute('DELETE FROM root_reconfiguration_workspaces WHERE project_id=?', (project,))
        return resets.public(job)

    def status(self):
        root = self.config()
        if not root:
            return dict(enabled=False, projects=[])
        with self.projects.connection() as db:
            rows = [dict(row, workspaces=self.projects._members(db, row['project_id'], row['cwd'], 'root_project_workspaces')) for row in db.execute('SELECT * FROM root_projects ORDER BY project_id')]
        for row in rows:
            row.pop('prompt', None)
            setup = self.projects.get(row['project_id'])
            row['name'] = setup['name']
            row['creation_workspace'] = setup.get('workspace')
            row['creation_workspaces'] = setup['workspaces']
            row['reconfiguration'] = self.reconcile_reconfiguration(row['project_id'])
            if row['reconfiguration'] and row['reconfiguration']['status'] == 'completed':
                row['workspaces'] = row['reconfiguration']['requested_workspaces']
                row['cwd'] = row['workspaces'][0]
            row['has_instructions'] = bool(setup.get('instructions'))
            row['provision_ready'] = bool(setup.get('workspace') and setup.get('instructions'))
            row['creation'] = None
            row['orchestrator'] = None
            from .bridge_state import runtime_snapshot
            try:
                with contextlib.closing(Coordinator(self.path(row['project_id']))) as coord:
                    runtime = runtime_snapshot(coord.db)
                    if row['orchestrator_session']:
                        from .agent_control import AgentControlState
                        row['orchestrator'] = next((main for main in AgentControlState(coord).roster() if main['id'] == row['orchestrator_session']),None)
                        if row['orchestrator'] and row['orchestrator']['thread_id']:
                            observed = coord.db.execute('''SELECT model,reasoning_effort
                                FROM agent_control_observations WHERE target_session=? AND thread_id=?''',
                                (row['orchestrator_session'], row['orchestrator']['thread_id'])).fetchone()
                            if observed and observed['model']:
                                # The directory keeps provisioning settings; the
                                # overview shows the currently bound thread.
                                row.update(model=observed['model'], effort=observed['reasoning_effort'])
            except (CoordError, OSError, sqlite3.Error) as error:
                row.update(joined=0,error=str(error))
                runtime = None
            client = next((client for client in (runtime or {}).get('clients', []) if client['host_id'] == root['host_id']), None)
            row['bridge'] = dict(recent=bool(client and client['recent'] and not client['error']),
                                 project_scope_worker=bool(client and client.get('project_scope_worker')),
                                 project_workspaces_worker=bool(client and client.get('project_workspaces_worker')), error=client['error'] if client else 'no bridge heartbeat on root host')
            if row['request_id'] and not row['adopted'] and row['joined']:
                from .session_reset import SessionResetState
                with contextlib.closing(Coordinator(self.path(row['project_id']))) as coord:
                    if coord.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='session_resets'").fetchone():
                        job = coord.db.execute('SELECT * FROM session_resets WHERE id=?', (row['request_id'],)).fetchone()
                        if job:
                            row['creation'] = SessionResetState.public(SessionResetState(coord).job(job['id']))
            row['setup'] = 'routing_error' if not row['joined'] else ('adopted' if row['adopted'] else
                (row['creation']['status'] if row['creation'] else ('awaiting_original_request_retry' if row['request_id'] else
                 ('ready_to_provision' if row['provision_ready'] else 'needs_workspace_and_prompt'))))
            if row['reconfiguration'] and row['reconfiguration']['status'] not in ('completed', 'failed', 'cancelled'):
                row['setup'] = 'workspace_reconfiguration_' + row['reconfiguration']['status']
            if len(row['creation_workspaces']) > 1 and not row['bridge']['project_workspaces_worker']:
                row['bridge']['workspace_upgrade_required'] = True
        with self.projects.connection() as db:
            team_stops = [dict(item) for item in db.execute('SELECT * FROM root_team_operations WHERE resumed_at IS NULL')]
        return dict(enabled=True, root=root, projects=rows,team_stops=team_stops,
                    supported_lifecycle=['root fresh-start --expected-thread OLD_ROOT_THREAD_UUID --prompt-file BOOT_FILE --request-id UUID --confirm',
                                         'root fresh-status --request-id UUID',
                                         'root stop-all --request-id UUID --confirm',
                                         'root stop-status --request-id UUID',
                                         'root resume-all --request-id UUID --confirm'])

    def detach(self, project, session, host, confirmed):
        root = self.authorize(project, session, host)
        if confirmed is not True:
            raise CoordError('root detach requires explicit confirmation')
        if root['mode'] == 'detached':
            return self.finish_detach(project)
        self.fresh(root)
        with contextlib.closing(Coordinator(self.path(project))) as coord:
            observed = coord.db.execute('SELECT * FROM agent_control_observations WHERE target_session=?', (session,)).fetchone()
            if not observed or time.time()-observed['checked_at'] > 120 or observed['queue_count']:
                raise CoordError('refresh root agent-status and preserve queued app-server input before detaching')
        # Journal the admission pause first. Already crossed RPCs remain in the
        # ordinary wake ledger and block closure until reconciled. No queue input
        # is deleted. Failed checks leave routing paused and registration intact.
        with self.projects.connection() as db:
            db.execute("UPDATE root_config SET mode='detaching'")
        self.sync()
        # Retain registrations and inboxes. Only remove the global role once all
        # projections are clean; a failure makes a retry safe and reviewable.
        with self.projects.connection() as db:
            rows = [dict(row, workspaces=self.projects._members(db, row['project_id'], row['cwd'], 'root_project_workspaces')) for row in db.execute('SELECT * FROM root_projects')]
        from .session_reset import SessionResetState
        for row in rows:
            with contextlib.closing(Coordinator(self.path(row['project_id']), row['session_id'])) as coord:
                SessionResetState(coord).clean(row['session_id'], self.config()['thread_id'])
                if coord.db.execute("SELECT 1 FROM session_resets WHERE (requester_session=? OR (old_thread_id=? AND project_scope=1)) AND status NOT IN ('completed','failed','cancelled')", (row['session_id'], root['thread_id'])).fetchone():
                    raise CoordError('resolve pending root-created mains before detaching')
                coord._fresh(coord.db, row['session_id'])
                if coord.db.execute("SELECT 1 FROM agent_control_requests WHERE target_session=? AND status NOT IN ('completed','failed','cancelled')", (row['session_id'],)).fetchone():
                    raise CoordError('resolve root runtime inspection/control requests before detaching')
                if coord.db.execute('SELECT 1 FROM messages WHERE recipient_session=? AND acked_at IS NULL', (row['session_id'],)).fetchone():
                    raise CoordError('consume and acknowledge every root project delivery before detaching')
        for row in rows:
            with contextlib.closing(Coordinator(self.path(row['project_id']))) as coord:
                with coord.tx() as db:
                    if row['project_id'] != project:
                        db.execute('DELETE FROM bridge_bindings WHERE session_id=?', (row['session_id'],))
        with self.projects.connection() as db:
            db.execute("UPDATE root_config SET mode='detached'")
        return self.finish_detach(project)

    def finish_detach(self, project):
        with self.projects.connection() as db:
            rows = [dict(row, workspaces=self.projects._members(db, row['project_id'], row['cwd'], 'root_project_workspaces')) for row in db.execute('SELECT * FROM root_projects')]
        for row in rows:
            with contextlib.closing(Coordinator(self.path(row['project_id']))) as coord:
                coord.db.execute('DELETE FROM root_projections WHERE session_id=?', (row['session_id'],))
        with self.projects.connection() as db:
            db.execute('BEGIN IMMEDIATE')
            db.execute('DELETE FROM root_config')
            db.execute('DELETE FROM root_workspace_roots')
            db.commit()
        return dict(detached=True, registrations_retained=True)

    def resume_routing(self, project, session, host, confirmed):
        root = self.authorize(project, session, host)
        if root['mode'] == 'detached':
            raise CoordError('detach committed; finish root detach rather than resuming old routing')
        if confirmed is not True:
            raise CoordError('root routing resume requires explicit confirmation')
        self.fresh(root)
        with self.projects.connection() as db:
            db.execute("UPDATE root_config SET mode='running'")
        self.sync()
        return self.status()
