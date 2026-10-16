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
    'delivery-metadata': {'expected_thread', 'message_ids'},
    'pause-routing': {'expected_thread', 'request_id', 'confirm'},
    'resume-routing': {'confirm'},
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


def _completed_routing_resume(master, registry, root, uid):
    """Read-only exception for an already committed, unchanged lifecycle effect."""
    if root['mode'] != 'running':
        return False
    from .root_orchestrator import RootOrchestrator
    try:
        with _read(registry) as db:
            if not all(_table(db, name) for name in ('root_routing_pauses', 'root_routing_pause_projects')):
                return False
            audit = db.execute('SELECT * FROM root_routing_pauses ORDER BY created_at DESC,id DESC LIMIT 1').fetchone()
            if (not audit or audit['status'] != 'resumed' or audit['resumed_at'] is None
                    or (audit['home_project'], audit['home_session'], audit['host_id'], audit['thread_id']) !=
                    (root['home_project'], root['home_session'], root['host_id'], root['thread_id'])):
                return False
            saved = [dict(row) for row in db.execute('SELECT project_id,session_id FROM root_routing_pause_projects WHERE request_id=? ORDER BY project_id', (audit['id'],))]
            current = [dict(row) for row in db.execute('SELECT project_id,session_id FROM root_projects ORDER BY project_id')]
            if not saved or saved != current or dict(project_id=root['home_project'], session_id=root['home_session']) not in saved:
                return False

        class Readback(RootOrchestrator):
            def __init__(self):
                pass  # Do not initialize or migrate any schema during preflight.

            def path(self, project):
                return _target(master, registry, project, uid)

        readback = Readback()
        return all(not readback._routing_projection(root, row, None) for row in saved)
    except (CoordError, sqlite3.Error, OSError):
        return False


def _delivery_metadata(master, home_project, session, host, expected_thread, message_ids):
    """Exact own-recipient history without content, native reads or writes."""
    registry = Path(str(master) + '.projects.sqlite3')
    home = _target(master, registry, home_project, master.lstat().st_uid)
    with _read(registry) as roots, _read(home) as db:
        roots.execute('BEGIN')
        db.execute('BEGIN')
        root = roots.execute('SELECT * FROM root_config WHERE singleton=1').fetchone()
        if not root or (root['home_project'], root['home_session'], root['host_id'], root['thread_id']) != (
                home_project, session, host, expected_thread):
            raise CoordError('local Root delivery canonical identity or thread differs')
        binding = db.execute('''SELECT b.thread_id,b.parent_session,h.host_id
            FROM bridge_bindings b JOIN remote_session_hosts h ON h.session_id=b.session_id
            WHERE b.session_id=?''', (session,)).fetchone()
        if not binding or (binding['thread_id'], binding['parent_session'], binding['host_id']) != (expected_thread, None, host):
            raise CoordError('local Root delivery current binding or host differs')
        if any(not _table(db, table) for table in ('messages', 'bridge_deliveries', 'bridge_jobs')):
            raise CoordError('local Root delivery metadata schema is unavailable')
        deliveries = []
        for message_id in message_ids:
            row = db.execute('''SELECT m.id AS message_id,m.created_at AS message_created_at,
                m.acked_at AS message_acked_at,d.job_id,j.thread_id,j.status AS job_status,j.queue_id,
                j.created_at AS job_created_at,j.updated_at AS job_updated_at
                FROM messages m LEFT JOIN bridge_deliveries d ON d.message_id=m.id
                LEFT JOIN bridge_jobs j ON j.id=d.job_id
                WHERE m.id=? AND m.recipient_session=?''', (message_id, session)).fetchone()
            if not row:
                raise CoordError('local Root message is unavailable for the canonical recipient')
            item = dict(row)
            if item['job_id'] is not None and item['thread_id'] != expected_thread:
                raise CoordError('local Root delivery job is unavailable for the current thread')
            item['client_user_message_id'] = item['job_id']
            item['job_prepared_at'] = None
            item['native_attempt_recorded_at_julian_day'] = None
            item['native_proof'] = []
            if item['job_id'] is not None:
                for table, column, field in (
                        ('bridge_job_origins', 'prepared_at', 'job_prepared_at'),
                        ('bridge_job_native_attempts', 'recorded_at', 'native_attempt_recorded_at_julian_day')):
                    if _table(db, table):
                        stamp = db.execute(f'SELECT {column} FROM {table} WHERE job_id=?', (item['job_id'],)).fetchone()
                        if stamp:
                            item[field] = stamp[0]
                if all(_table(roots, table) for table in ('native_policy_offers', 'native_policy_receipts')):
                    proofs = roots.execute('''SELECT r.version AS policy_version,r.turn_id AS native_turn_id,r.observed_at
                        FROM native_policy_receipts r JOIN native_policy_offers o
                        ON o.host=r.host AND o.thread=r.thread AND o.client_id=r.client_id AND o.version=r.version
                        WHERE o.host=? AND o.thread=? AND o.client_id=? AND o.project=? AND o.kind='wake'
                        ORDER BY r.observed_at,r.version''', (host, expected_thread, item['job_id'], home_project))
                    item['native_proof'] = [dict(proof) for proof in proofs
                                            if isinstance(proof['native_turn_id'], str) and proof['native_turn_id']]
            deliveries.append(item)
    return {
        'authority': {'scope': 'machine_read', 'named_session_authentication': 'UNVERIFIED'},
        'project': home_project, 'session': session, 'host_id': host, 'thread_id': expected_thread,
        'timestamp_semantics': {
            'message_created_at': 'persisted Unix seconds: message row creation',
            'message_acked_at': 'mutable Unix seconds: message acknowledgement, not native consumption',
            'job_created_at': 'persisted Unix seconds: job row creation',
            'job_updated_at': 'mutable Unix seconds: latest job write, not a dispatch, start or consumption timestamp',
            'job_prepared_at': 'persisted Unix seconds: immutable preparation provenance, when present',
            'native_attempt_recorded_at_julian_day': 'SQLite Julian day: first status update beyond prepared/cancelled, not native acceptance',
            'native_proof.observed_at': 'persisted Unix seconds: exact policy receipt observation, not native turn start',
        },
        'client_id_semantics': 'bridge job ID used as clientUserMessageId; not proof of native acceptance',
        'native_proof_semantics': 'existing exact host/thread/client/project policy receipt only; empty means no persisted proof',
        'deliveries': deliveries,
    }


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
        if operation in ('pause-routing', 'resume-routing'):
            if db.execute("SELECT 1 FROM root_fresh_requests WHERE status<>'completed'").fetchone():
                raise CoordError('root fresh-start is pending; routing change withheld')
            if operation == 'pause-routing' and root['mode'] not in ('running', 'maintenance_paused'):
                raise CoordError('root is detaching or detached; routing pause withheld')
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
        if operation in ('enroll', 'pause-routing', 'delivery-metadata') and params['expected_thread'] != binding['thread_id']:
            raise CoordError('local Root expected thread differs')
        if operation in ('pause-routing', 'resume-routing'):
            # Reject stale/missing native inspection before any schema migration.
            from .root_orchestrator import RootOrchestrator
            if operation != 'resume-routing' or not _completed_routing_resume(master, registry, root, owner):
                RootOrchestrator._routing_inspection(db, root)
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
    if 'message_ids' in params:
        ids = params['message_ids']
        if not isinstance(ids, list) or not 1 <= len(ids) <= 20:
            raise CoordError('local Root delivery requires 1 to 20 exact message IDs')
        for message_id in ids:
            _text(message_id, 'message ID')
        if len(set(ids)) != len(ids):
            raise CoordError('local Root delivery message IDs must be distinct')
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
    if operation == 'delivery-metadata':
        return _delivery_metadata(master, home_project, session, host_id, params['expected_thread'], params['message_ids'])
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
        if operation == 'pause-routing':
            return roots.pause_routing(*args, params['expected_thread'], params['request_id'], params['confirm'])
        if operation == 'resume-routing':
            return roots.resume_routing(*args, params['confirm'])
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
            option = ('--project-id' if field in ('project', 'projects') else
                      '--message-id' if field == 'message_ids' else '--' + field.replace('_', '-'))
            if field in ('projects', 'message_ids'):
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
