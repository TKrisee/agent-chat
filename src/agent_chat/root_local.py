"""Same-UID, configured-app Root adapter. Never a network privilege grant."""
from __future__ import annotations

import argparse
import contextlib
import json
import os
from pathlib import Path
import sqlite3
import stat
from types import SimpleNamespace
import uuid

from .core import CoordError
from .service import ServiceError, _installed_server_config, configured_server_database


FIELDS = {
    'readiness': {'projects', 'expected_thread'},
    'status': set(), 'enroll': {'expected_thread', 'confirm'},
    'in': {'project'}, 'route': {'project'},
    'configure': {'project', 'agent', 'cwd', 'prompt', 'request_id', 'confirm', 'model', 'effort'},
    'provision': {'project', 'agent', 'request_id', 'confirm', 'model', 'effort'},
    'delegate-management': {'project', 'po', 'target', 'expected_thread', 'expected_host', 'request_id', 'confirm'},
    'team-lifecycle': {'operation', 'request_id', 'confirm'},
    'stop-all': {'request_id', 'confirm'}, 'resume-all': {'request_id', 'confirm'},
    'stop-status': {'request_id'},
    'fresh-start': {'expected_thread', 'prompt', 'request_id', 'confirm'},
    'fresh-status': {'request_id'},
}


def _text(value, label):
    if not isinstance(value, str) or not value or len(value) > 4096 or any(ord(c) < 32 for c in value):
        raise CoordError('invalid local Root ' + label)
    return value


def _file(path, uid):
    path = Path(path)
    try:
        info = path.lstat()
    except OSError as error:
        raise CoordError('local Root trusted file is missing') from error
    if not stat.S_ISREG(info.st_mode) or info.st_uid != uid or path.resolve() != path:
        raise CoordError('local Root trusted file owner or path differs')
    return path


@contextlib.contextmanager
def _read(path):
    db = sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    try:
        yield db
    finally:
        db.close()


def _table(db, name):
    return bool(db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone())


def _target(master, registry, project, uid):
    _text(project, 'project')
    with _read(registry) as db:
        if not _table(db, 'projects') or not db.execute('SELECT 1 FROM projects WHERE id=?', (project,)).fetchone():
            raise CoordError('local Root project is not registered')
    if project == 'default':
        path = master
    else:
        # Registered project IDs still cannot introduce path traversal.
        if '/' in project or project in ('.', '..'):
            raise CoordError('invalid registered project path')
        path = master.parent / (master.name + '.projects') / project / 'state.sqlite3'
    _file(path, uid)
    with _read(path) as db:
        if not _table(db, 'meta'):
            raise CoordError('local Root project metadata is unavailable')
        metadata = dict(db.execute("SELECT key,value FROM meta WHERE key IN ('project_id','project_registry','schema_version')"))
        if metadata.get('schema_version') != '1':
            raise CoordError('local Root project schema is unsupported')
        if project == 'default':
            if metadata.get('project_id', 'default') != 'default':
                raise CoordError('local Root default project differs')
            if 'project_registry' in metadata and Path(metadata['project_registry']).resolve() != master:
                raise CoordError('local Root registry lineage differs')
        elif metadata.get('project_id') != project or not metadata.get('project_registry') or Path(metadata['project_registry']).resolve() != master:
            raise CoordError('local Root registry lineage differs')
    return path


def _preflight(master_db, service_plist, home_project, session, host, operation, params):
    config = _installed_server_config()
    # The caller cannot supply a UID. File ownership anchors both actual UIDs.
    try:
        owner = config.plist.lstat().st_uid
    except OSError as error:
        raise CoordError('local Root configured provider is unavailable') from error
    if os.getuid() != owner or os.geteuid() != owner:
        raise CoordError('local Root requires the configured application real and effective UID')
    for path in (config.plist, config.wrapper, config.executable):
        _file(path, owner)
    try:
        master = configured_server_database(service_plist)
    except ServiceError as error:
        raise CoordError(str(error)) from error
    if Path(os.path.abspath(master_db)) != master or master != config.master.resolve():
        raise CoordError('local Root master differs from the canonical configured master')
    _file(master, owner)
    registry = _file(Path(str(master) + '.projects.sqlite3'), owner)
    home = _target(master, registry, home_project, owner)
    with _read(registry) as db:
        root = db.execute('SELECT * FROM root_config WHERE singleton=1').fetchone() if _table(db, 'root_config') else None
        if root and (root['home_project'], root['home_session'], root['host_id']) != (home_project, session, host):
            raise CoordError('local Root canonical identity differs')
        if not root and operation != 'enroll' and operation != 'status':
            raise CoordError('local Root is not enrolled')
        if root and any(not _table(db, name) for name in ('root_fresh_requests', 'root_team_operations')):
            raise CoordError('local Root lifecycle schema is incomplete')
    with _read(home) as db:
        if any(not _table(db, name) for name in ('sessions', 'bridge_bindings', 'remote_session_hosts')):
            raise CoordError('local Root registration schema is incomplete')
        registered = db.execute('SELECT 1 FROM sessions WHERE id=?', (session,)).fetchone()
        binding = db.execute('SELECT thread_id,parent_session FROM bridge_bindings WHERE session_id=?', (session,)).fetchone()
        host_row = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (session,)).fetchone()
        if not registered or not binding or binding['parent_session'] or not host_row or host_row['host_id'] != host:
            raise CoordError('local Root existing registration, main binding or host differs')
        if root and binding['thread_id'] != root['thread_id']:
            raise CoordError('local Root current thread differs')
        if operation == 'enroll' and params['expected_thread'] != binding['thread_id']:
            raise CoordError('local Root expected thread differs')
    if 'project' in params:
        _target(master, registry, params['project'], owner)
    # Preserve the HTTP application's setup measurement barrier without creating
    # a MeasurementStore or starting its workers. Missing state is not inactive.
    if operation in ('enroll', 'configure', 'provision'):
        selected = home_project if operation == 'enroll' else params['project']
        with _read(master) as db:
            if not _table(db, 'usage_measurements'):
                raise CoordError('local Root measurement state is unavailable')
            if db.execute("SELECT 1 FROM usage_measurements WHERE project_id=? AND state IN ('starting','running')", (selected,)).fetchone():
                raise CoordError('finish the active project measurement before local Root setup')
    return master, bool(root)


def execute(operation, params, *, master_db, service_plist, home_project, session, host_id):
    if not isinstance(operation, str) or operation not in FIELDS or not isinstance(params, dict):
        raise CoordError('unsupported local Root method')
    optional = {'workspaces'} if operation == 'configure' else set()
    if not FIELDS[operation] <= set(params) <= FIELDS[operation] | optional:
        raise CoordError('invalid local Root method fields')
    for field in ('project', 'agent', 'po', 'target', 'expected_thread', 'expected_host', 'request_id'):
        if field in params:
            _text(params[field], field)
    if 'request_id' in params:
        try:
            uuid.UUID(params['request_id'])
        except ValueError as error:
            raise CoordError('local Root request_id must be a UUID') from error
    if 'confirm' in params and params['confirm'] is not True:
        raise CoordError('local Root mutation requires explicit confirmation')
    for value, label in ((home_project, 'home project'), (session, 'session'), (host_id, 'host')):
        _text(value, label)
    if operation == 'team-lifecycle' and params['operation'] not in ('stop-all', 'resume-all', 'stop-status'):
        raise CoordError('unsupported local Root team lifecycle operation')
    master, enrolled = _preflight(master_db, service_plist, home_project, session, host_id, operation, params)
    if operation == 'status' and not enrolled:
        return {'enrolled': False}
    from .projects import Projects
    from .root_orchestrator import RootOrchestrator
    owner = master.lstat().st_uid
    registry = Path(str(master) + '.projects.sqlite3')

    class LocalRoot(RootOrchestrator):
        def path(self, project):
            # sync and team/fresh workflows may reach projects other than the
            # nominated target. Validate every actual open, not only preflight.
            return _target(master, registry, project, owner)

    if operation == 'readiness':
        # This transport's same-UID provider trust does not attest the named
        # selector. Reuse exactly the machine-read projection without schema
        # initialization, sync, routing resolution or writable project access.
        class LocalReadiness(LocalRoot):
            def __init__(self):
                self.projects = SimpleNamespace(path=registry)

        return LocalReadiness().readiness(home_project, session, host_id,
                                         params['projects'], params['expected_thread'])

    roots = LocalRoot(Projects(master))
    args = (home_project, session, host_id)
    with roots.projects.root_lock:
        if operation == 'enroll':
            return roots.enroll(*args, params['expected_thread'], params['confirm'])
        roots.authorize(*args)
        if operation == 'status':
            roots.sync()
            return roots.status()
        if operation in ('in', 'route'):
            return roots.route(*args, params['project'])
        if operation in ('stop-all', 'resume-all', 'stop-status', 'team-lifecycle'):
            selected = params['operation'] if operation == 'team-lifecycle' else operation
            return roots.team_lifecycle(*args, selected, params['request_id'], params.get('confirm', True))
        if operation in ('fresh-start', 'fresh-status'):
            from .root_restart import RootRestart
            restart = RootRestart(roots)
            if operation == 'fresh-status':
                return restart.authorize_status(*args, params['request_id'])
            return restart.request(*args, params['expected_thread'], params['prompt'], params['request_id'], params['confirm'])
        if operation == 'delegate-management':
            return roots.delegate_management(*args, params['project'], params['po'], params['target'],
                params['expected_thread'], params['expected_host'], params['request_id'], params['confirm'])
        if operation == 'provision':
            return roots.provision(*args, params['project'], params['agent'], params['request_id'],
                params['confirm'], params['model'], params['effort'])
        return roots.configure(*args, params['project'], params['agent'], params['cwd'], params['prompt'],
            params['request_id'], params['confirm'], params['model'], params['effort'], workspaces=params.get('workspaces'))


def main(argv=None):
    parser = argparse.ArgumentParser(prog='agent-chat-client root-local', description=__doc__)
    config = _installed_server_config()
    parser.add_argument('--master-db', default=str(config.master), help='canonical configured master equality assertion')
    parser.add_argument('--service-plist', default=str(config.plist), help='canonical installed provider equality assertion')
    parser.add_argument('--home-project', required=True)
    parser.add_argument('--home-session', required=True, dest='session')
    parser.add_argument('--host-id', required=True)
    commands = parser.add_subparsers(dest='method', required=True)
    for method, fields in FIELDS.items():
        command = commands.add_parser(method)
        for field in sorted(fields):
            option = '--project-id' if field in ('project', 'projects') else '--' + field.replace('_', '-')
            if field == 'projects':
                command.add_argument(option, dest=field, action='append', required=True)
            elif field == 'confirm':
                command.add_argument(option, action='store_true')
            elif field in ('model', 'effort'):
                command.add_argument(option)
            elif field == 'agent':
                command.add_argument(option, default='orchestrator')
            else:
                command.add_argument(option, dest=field, required=True)
        if method == 'configure':
            command.add_argument('--workspace', dest='workspaces', action='append')
    values = vars(parser.parse_args(argv))
    method = values.pop('method')
    identity = {key: values.pop(key) for key in ('master_db', 'service_plist', 'home_project', 'session', 'host_id')}
    if values.get('workspaces') is None:
        values.pop('workspaces', None)
    print(json.dumps(execute(method, values, **identity), sort_keys=True))
    return 0
