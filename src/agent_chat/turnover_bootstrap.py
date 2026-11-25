"""Exact, stop-scoped authority for a parked PO's one retained bootstrap.

The caller must authenticate the Root and hold the registry and project scope
stable across the relevant state transition. This module does not dispatch input.
"""
import time
from .core import CoordError


def schema(db):
    db.execute('''CREATE TABLE IF NOT EXISTS root_turnover_bootstraps (
        request_id TEXT PRIMARY KEY, project_id TEXT NOT NULL, stop_id TEXT NOT NULL,
        root_session TEXT NOT NULL, target_session TEXT NOT NULL, thread_id TEXT NOT NULL,
        host_id TEXT NOT NULL, created_at REAL NOT NULL)''')
    db.execute("CREATE TRIGGER IF NOT EXISTS turnover_bootstrap_no_update BEFORE UPDATE ON root_turnover_bootstraps BEGIN SELECT RAISE(ABORT,'turnover bootstrap immutable'); END")
    db.execute("CREATE TRIGGER IF NOT EXISTS turnover_bootstrap_no_delete BEFORE DELETE ON root_turnover_bootstraps BEGIN SELECT RAISE(ABORT,'turnover bootstrap immutable'); END")


def check_scope(db, registry, request_id):
    """Recheck the current stop, designation, job, host and native binding."""
    row = db.execute('''SELECT p.*, j.status, j.kind, j.requester_session,
        j.new_thread_id, j.settings_digest, j.new_settings_digest, j.queue_id,
        r.home_session, r.thread_id AS root_thread
        FROM root_parked_creations p JOIN root_parked_activations a ON a.request_id=p.request_id
        JOIN session_resets j ON j.id=p.request_id
        JOIN root_projections r ON r.session_id=p.root_session AND r.paused=0
        WHERE p.request_id=?''', (request_id,)).fetchone()
    if not row:
        raise CoordError('turnover bootstrap requires an activated parked creation')
    row = dict(row)
    current = registry.execute('''SELECT rp.session_id, rp.orchestrator_session, rp.request_id
        FROM root_projects rp JOIN root_team_operation_projects op
          ON op.project_id=rp.project_id AND op.request_id=?
        JOIN root_team_operations o ON o.id=op.request_id
        WHERE rp.project_id=? AND rp.joined=1 AND o.resumed_at IS NULL
          AND o.root_session=? AND op.root_session=rp.session_id''',
        (row['stop_id'], row['project_id'], row['home_session'])).fetchone()
    active = db.execute('SELECT id,root_session FROM root_team_stops WHERE active=1').fetchall()
    binding = db.execute('SELECT thread_id,parent_session FROM bridge_bindings WHERE session_id=?', (row['target_session'],)).fetchone()
    host = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (row['target_session'],)).fetchone()
    root_binding = db.execute('SELECT thread_id,parent_session FROM bridge_bindings WHERE session_id=?', (row['root_session'],)).fetchone()
    root_host = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (row['root_session'],)).fetchone()
    role = db.execute('SELECT root_session FROM project_orchestrators WHERE session_id=?', (row['target_session'],)).fetchone()
    manager = db.execute('SELECT manager_session,request_id FROM agent_management WHERE target_session=?', (row['target_session'],)).fetchone()
    if (not current or tuple(current) != (row['root_session'], row['target_session'], request_id)
            or len(active) != 1 or tuple(active[0]) != (row['stop_id'], row['root_session'])
            or row['kind'] != 'create' or row['requester_session'] != row['root_session']
            or not row['new_thread_id'] or not row['settings_digest']
            or row['settings_digest'] != row['new_settings_digest']
            or not binding or tuple(binding) != (row['new_thread_id'], None)
            or not host or host['host_id'] != row['host_id']
            or not root_binding or tuple(root_binding) != (row['thread_id'], None)
            or row['root_thread'] != row['thread_id']
            or not root_host or root_host['host_id'] != row['host_id']
            or not role or role['root_session'] != row['root_session']
            or not manager or tuple(manager) != (row['root_session'], request_id)):
        raise CoordError('turnover bootstrap stop, designation, verified job or binding changed')
    from .management import assert_governing_reset
    assert_governing_reset(db, row['reset_id'], row['home_session'], row['project_id'])
    return row


def admit(coord, registry, request_id, confirmed):
    """Record one Root-authorized bootstrap for an already parked exact job."""
    if confirmed is not True:
        raise CoordError('turnover bootstrap requires explicit Root confirmation')
    schema(coord.db)
    with coord.tx() as db:
        row = check_scope(db, registry, request_id)
        if coord.require_session() != row['root_session']:
            raise CoordError('turnover bootstrap requires the exact Root')
        values = (request_id, row['project_id'], row['stop_id'], row['root_session'],
                  row['target_session'], row['new_thread_id'], row['host_id'])
        saved = db.execute('SELECT * FROM root_turnover_bootstraps WHERE request_id=?', (request_id,)).fetchone()
        if saved:
            if tuple(saved)[:7] != values:
                raise CoordError('turnover bootstrap retry differs from original')
        else:
            if row['status'] != 'parked' or row['queue_id']:
                raise CoordError('turnover bootstrap requires an unqueued parked PO')
            db.execute('INSERT INTO root_turnover_bootstraps VALUES(?,?,?,?,?,?,?,?)', (*values, time.time()))
        return dict(db.execute('SELECT * FROM root_turnover_bootstraps WHERE request_id=?', (request_id,)).fetchone())


def authorized(db, registry, request_id):
    """Revalidate a previously issued grant before each native input boundary."""
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_turnover_bootstraps'").fetchone():
        raise CoordError('turnover bootstrap admission is absent')
    saved = db.execute('SELECT * FROM root_turnover_bootstraps WHERE request_id=?', (request_id,)).fetchone()
    row = check_scope(db, registry, request_id)
    values = (request_id, row['project_id'], row['stop_id'], row['root_session'],
              row['target_session'], row['new_thread_id'], row['host_id'])
    if not saved or tuple(saved)[:7] != values:
        raise CoordError('turnover bootstrap admission differs from current scope')
    return row


def reconcile_consumed(coord, registry, request_id, host_id, confirmed):
    """Record positive native consumption without delivering any input."""
    if confirmed is not True:
        raise CoordError('turnover bootstrap reconciliation requires explicit confirmation')
    row = authorized(coord.db, registry, request_id)
    if (coord.require_session() != row['root_session'] or host_id != row['host_id']
            or not row['queue_id'] or row['status'] not in ('queued', 'starting', 'completed')):
        raise CoordError('turnover reconciliation requires the exact Root and recorded queue item')
    if row['status'] == 'completed':
        return {'request_id': request_id, 'status': 'completed', 'already_recorded': True}

    from .native_history import turns_page
    from .provider_service import configured, option, selected_listener
    from .rpc import RpcClient
    from .service import ServiceManager
    from .session_reset import SessionResetState

    manager = ServiceManager()
    wrapper, command = configured(manager)
    endpoint = option(command, '--codex-server')
    original_listener = selected_listener(endpoint)
    with RpcClient(endpoint, timeout=3) as rpc:
        page = turns_page(rpc, {'threadId': row['new_thread_id'], 'limit': 25,
                               'itemsView': 'summary', 'sortDirection': 'desc'})
    matches = [turn['id'] for turn in page['data'] for item in turn.get('items', [])
               if item.get('type') == 'userMessage' and item.get('clientId') == request_id]
    if len(matches) != 1:
        raise CoordError('exact bootstrap consumption was not established')
    if configured(manager)[0] != wrapper or selected_listener(endpoint) != original_listener:
        raise CoordError('configured native listener changed during reconciliation')
    if authorized(coord.db, registry, request_id) != row:
        raise CoordError('turnover bootstrap changed during reconciliation')
    job = SessionResetState(coord).update(request_id, row['status'], 'completed',
                                         _turnover=True, error=None)
    return {'request_id': request_id, 'status': job['status'], 'thread_id': row['new_thread_id'],
            'native_turn_id': matches[0]}


def authorize_subordinate(db, registry, bootstrap_id, caller, thread, host, model, effort):
    """Authorize only the admitted PO to request an explicitly configured parked worker."""
    row = authorized(db, registry, bootstrap_id)
    if (row['status'] != 'completed' or caller != row['target_session']
            or thread != row['new_thread_id'] or host != row['host_id']
            or not isinstance(model, str) or not model.strip()
            or not isinstance(effort, str) or not effort.strip()):
        raise CoordError('parked subordinate requires the active successor PO and explicit settings')
    return row


def _completed_root_reply(coord, registry, request_id, message_id, host_id, confirmed):
    if confirmed is not True:
        raise CoordError('turnover continuation requires explicit confirmation')
    row = authorized(coord.db, registry, request_id)
    if (coord.require_session() != row['root_session'] or host_id != row['host_id']
            or row['status'] != 'completed'):
        raise CoordError('turnover continuation requires the exact completed PO bootstrap')
    message = coord.db.execute("SELECT m.sender_session,m.recipient_session,r.reply_to "
        "FROM messages m JOIN message_replies r ON r.message_id=m.id WHERE m.id=?",
                               (message_id,)).fetchone()
    if not message or tuple(message)[:2] != (row['root_session'], row['target_session']) or not message['reply_to']:
        raise CoordError('turnover continuation requires an exact Root reply to the PO')
    source = coord.db.execute('SELECT sender_session,recipient_session FROM messages WHERE id=?',
                              (message['reply_to'],)).fetchone()
    if not source or tuple(source) != (row['target_session'], row['root_session']):
        raise CoordError('turnover continuation source must belong to the exact Root and PO')
    return row


def _read_turnover_thread(rpc, thread_id, *, load=False):
    response = rpc.request('thread/read', {'threadId': thread_id})
    thread = response.get('thread') if isinstance(response, dict) else None
    status = thread.get('status') if isinstance(thread, dict) else None
    if (load and isinstance(status, dict) and status.get('type') == 'notLoaded'
            and thread.get('id') == thread_id):
        rpc.request('thread/resume', {'threadId': thread_id, 'excludeTurns': True})
        response = rpc.request('thread/read', {'threadId': thread_id})
        thread = response.get('thread') if isinstance(response, dict) else None
    return thread


def continue_turnover(coord, registry, request_id, message_id, host_id, confirmed):
    """Deliver one existing Root follow-up to the same stopped successor PO."""
    row = _completed_root_reply(coord, registry, request_id, message_id, host_id, confirmed)

    from .core import execution_epoch_handoff, execution_epoch_settle
    from .provider_service import configured, option, selected_listener
    from .rpc import RpcClient
    from .service import ServiceManager

    coord.db.execute('''CREATE TABLE IF NOT EXISTS root_turnover_continuations (
        request_id TEXT PRIMARY KEY, message_id TEXT NOT NULL, epoch_id TEXT NOT NULL,
        native_turn_id TEXT)''')
    prior = coord.db.execute('SELECT message_id,native_turn_id FROM root_turnover_continuations WHERE request_id=?',
                             (request_id,)).fetchone()
    if prior:
        if prior['message_id'] != message_id:
            raise CoordError('turnover continuation already has a different exact reply')
        return {'request_id': request_id, 'message_id': message_id, 'native_turn_id': prior['native_turn_id'],
                'status': 'started' if prior['native_turn_id'] else 'submitted_unknown'}

    manager = ServiceManager()
    wrapper, command = configured(manager)
    endpoint = option(command, '--codex-server')
    listener = selected_listener(endpoint)
    thread_id = row['new_thread_id']
    with RpcClient(endpoint, timeout=5) as rpc:
        thread = _read_turnover_thread(rpc, thread_id, load=True)
        queue = rpc.request('thread/queue/list', {'threadId': thread_id, 'limit': 1})
        if (not isinstance(thread, dict) or thread['id'] != thread_id or thread['status']['type'] != 'idle'
                or thread.get('canAcceptDirectInput') is not True or queue['data'] or queue.get('nextCursor')):
            raise CoordError('turnover continuation requires an idle exact PO thread and empty native queue')
        if configured(manager)[0] != wrapper or selected_listener(endpoint) != listener:
            raise CoordError('configured native listener changed before continuation')
        with coord.tx() as db:
            if authorized(db, registry, request_id) != row:
                raise CoordError('parked PO admission changed before continuation')
            control = db.execute("SELECT mode FROM agent_controls WHERE target_session=?",
                                 (row["target_session"],)).fetchone()
            if control and control["mode"] != "running":
                raise CoordError("parked PO has a separate control stop")
            epoch_id = execution_epoch_handoff(db, 'turn/start', thread_id, row['target_session'], host_id)
            db.execute('INSERT INTO root_turnover_continuations VALUES (?,?,?,NULL)',
                       (request_id, message_id, epoch_id))
        prompt = ('A new Root reply is available in Agent Chat. Read the complete message with '
                  f'its exact ID {message_id}, using your existing own project and session. '
                  'Continue only the saved parked roster administration in that reply. '
                  'The original stop remains active. Do not dispatch product work.')
        response = rpc.request('turn/start', {'threadId': thread_id, 'clientUserMessageId': message_id,
                               'input': [{'type': 'text', 'text': prompt, 'text_elements': []}]})
        turn_id = response['turn']['id']
        with coord.tx() as db:
            execution_epoch_settle(db, epoch_id, host_id, 'submitted')
            db.execute('UPDATE root_turnover_continuations SET native_turn_id=? WHERE request_id=?',
                       (turn_id, request_id))
    return {'request_id': request_id, 'message_id': message_id, 'thread_id': thread_id,
            'native_turn_id': turn_id, 'status': 'started'}


def _second_receipt(db, identity):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_turnover_second_continuations'").fetchone():
        return None
    request_id, continuation_id, message_id, *_ = identity
    row = db.execute("""SELECT * FROM root_turnover_second_continuations
        WHERE request_id=? OR continuation_id=? OR message_id=?""",
                     (request_id, continuation_id, message_id)).fetchone()
    if not row:
        return None
    fields = ('request_id', 'continuation_id', 'message_id', 'first_retirement_id', 'second_retirement_id')
    if tuple(row[key] for key in fields) != identity:
        raise CoordError('second turnover continuation already has a different exact request or reply')
    result = dict(zip(fields, identity))
    result.update(thread_id=row['thread_id'], native_turn_id=row['native_turn_id'],
                  status='started' if row['native_turn_id'] else 'submitted_unknown')
    if not row['native_turn_id']:
        result['native_outcome'] = 'unknown'
    if row['provider_error'] is not None:
        result.update(status='refused', provider_error=row['provider_error'])
    return result


def _first_started_continuation(db, scope, request_id, message_id):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_turnover_continuations'").fetchone():
        raise CoordError('second turnover continuation requires the recorded first start')
    row = db.execute("""SELECT c.message_id,c.native_turn_id FROM root_turnover_continuations c
        JOIN execution_epoch_handoffs e ON e.id=c.epoch_id
        JOIN messages first ON first.id=c.message_id
        JOIN messages second ON second.seq>first.seq AND second.id=?
        WHERE c.request_id=? AND e.thread=? AND e.actor=? AND e.host=?
            AND e.method='turn/start' AND e.outcome='submitted' AND e.settled_at IS NOT NULL""",
                     (message_id, request_id, scope['new_thread_id'], scope['target_session'], scope['host_id'])).fetchone()
    if not row or not row['native_turn_id']:
        raise CoordError('second turnover continuation requires the recorded first start and a distinct Root reply')
    return tuple(row)


def _require_uncertain_retirements(db, scope, retirement_ids):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_parked_retirements'").fetchone():
        raise CoordError('second turnover continuation requires both exact uncertain parked retirements')
    rows = db.execute("""SELECT r.* FROM root_parked_retirements r
        JOIN root_turnover_subordinates s ON s.request_id=r.request_id
            AND s.target_session=r.target_session AND s.project_id=r.project_id AND s.stop_id=r.stop_id
            AND s.bootstrap_id=r.bootstrap_id AND s.root_session=r.root_session
            AND s.thread_id=r.po_thread AND s.host_id=r.host_id
        JOIN session_resets j ON j.id=r.request_id AND j.target_session=r.target_session
            AND j.requester_session=r.po_session AND j.old_thread_id=r.po_thread AND j.host_id=r.host_id
        WHERE r.request_id IN (?,?) AND r.project_id=? AND r.stop_id=? AND r.bootstrap_id=? AND s.reset_id=?
            AND r.root_session=? AND r.po_session=? AND r.po_thread=? AND r.host_id=?
            AND j.kind='create' AND j.status='uncertain' AND j.new_thread_id IS NULL AND j.queue_id IS NULL
            AND NOT EXISTS (SELECT 1 FROM bridge_bindings b WHERE b.session_id=r.target_session)
        ORDER BY r.request_id""",
        (*retirement_ids, scope['project_id'], scope['stop_id'], scope['request_id'], scope['reset_id'],
         scope['root_session'], scope['target_session'], scope['new_thread_id'], scope['host_id'])).fetchall()
    if len(rows) != 2:
        raise CoordError('second turnover continuation requires both exact uncertain parked retirements')
    return tuple(map(tuple, rows))


def _assert_idle_turnover_thread(rpc, thread_id, *, load=False):
    thread = _read_turnover_thread(rpc, thread_id, load=load)
    status = thread.get('status') if isinstance(thread, dict) else None
    if (not isinstance(status, dict) or status.get('type') != 'idle'
            or thread.get('id') != thread_id or thread.get('canAcceptDirectInput') is not True):
        raise CoordError('second turnover continuation requires an idle exact PO thread')


def _assert_second_input_unblocked(db, scope):
    control = db.execute('SELECT mode FROM agent_controls WHERE target_session=?',
                         (scope['target_session'],)).fetchone()
    if control and control['mode'] != 'running':
        raise CoordError('parked PO has a separate control stop')
    # Provisioning can load the PO as a settings template on behalf of a worker.
    # A resume only concerns this continuation when both thread and actor match.
    thread, actor = scope['new_thread_id'], scope['target_session']
    if db.execute("""SELECT 1 FROM execution_epoch_handoffs
            WHERE settled_at IS NULL AND (
                (method IN ('turn/start','turn/steer','thread/queue/add','thread/queue/start')
                 AND (thread=? OR actor=?))
                OR (method='thread/resume' AND thread=? AND actor=?)
            )""", (thread, actor, thread, actor)).fetchone():
        raise CoordError('parked PO has an unsettled native input or resume')


def continue_turnover_second(coord, registry, request_id, continuation_id, message_id,
                             first_retirement_id, second_retirement_id, host_id, confirmed):
    """Admit at most one distinct second administrative reply for this bootstrap."""
    from uuid import UUID
    try:
        ids = tuple(str(UUID(value)) for value in (request_id, continuation_id, first_retirement_id, second_retirement_id))
    except (TypeError, ValueError, AttributeError) as error:
        raise CoordError('second turnover continuation requires four exact UUIDs') from error
    if ids != (request_id, continuation_id, first_retirement_id, second_retirement_id) or len(set(ids)) != 4:
        raise CoordError('second turnover continuation requires four distinct canonical UUIDs')
    scope = _completed_root_reply(coord, registry, request_id, message_id, host_id, confirmed)
    identity = (request_id, continuation_id, message_id, first_retirement_id, second_retirement_id)
    existing = _second_receipt(coord.db, identity)
    if existing:
        return existing
    first = _first_started_continuation(coord.db, scope, request_id, message_id)
    retirements = _require_uncertain_retirements(coord.db, scope, identity[3:])
    _assert_second_input_unblocked(coord.db, scope)

    from .core import execution_epoch_handoff, execution_epoch_settle
    from .native_history import turns_page
    from .provider_service import configured, option, selected_listener
    from .rpc import RpcClient, RpcError
    from .service import ServiceManager

    manager = ServiceManager()
    wrapper, command = configured(manager)
    endpoint = option(command, '--codex-server')
    listener = selected_listener(endpoint)
    thread_id = scope['new_thread_id']
    with RpcClient(endpoint, timeout=5) as rpc:
        _assert_idle_turnover_thread(rpc, thread_id, load=True)
        page = turns_page(rpc, dict(threadId=thread_id, limit=1, itemsView='notLoaded', sortDirection='desc'))
        previous = page.get('data') if isinstance(page, dict) else None
        if (not isinstance(previous, list) or len(previous) != 1 or previous[0].get('id') != first[1]
                or previous[0].get('status') not in ('completed', 'failed', 'interrupted')):
            raise CoordError('second turnover continuation requires the exact terminal prior turn')
        queue = rpc.request('thread/queue/list', {'threadId': thread_id, 'limit': 1})
        if not isinstance(queue, dict) or queue.get('data') != [] or queue.get('nextCursor') is not None:
            raise CoordError('second turnover continuation requires an empty native queue')
        _assert_idle_turnover_thread(rpc, thread_id)
        if configured(manager)[0] != wrapper or selected_listener(endpoint) != listener:
            raise CoordError('configured native listener changed before second continuation')

        with coord.tx() as db:
            existing = _second_receipt(db, identity)
            if existing:
                return existing
            if (_completed_root_reply(coord, registry, request_id, message_id, host_id, confirmed) != scope
                    or _first_started_continuation(db, scope, request_id, message_id) != first
                    or _require_uncertain_retirements(db, scope, identity[3:]) != retirements):
                raise CoordError('second turnover continuation proof changed before submission')
            _assert_second_input_unblocked(db, scope)
            db.execute("""CREATE TABLE IF NOT EXISTS root_turnover_second_continuations (
                request_id TEXT PRIMARY KEY, continuation_id TEXT NOT NULL UNIQUE,
                message_id TEXT NOT NULL UNIQUE, first_retirement_id TEXT NOT NULL,
                second_retirement_id TEXT NOT NULL, thread_id TEXT NOT NULL,
                epoch_id TEXT NOT NULL, native_turn_id TEXT, provider_error TEXT,
                CHECK (native_turn_id IS NULL OR provider_error IS NULL))""")
            epoch_id = execution_epoch_handoff(db, 'turn/start', thread_id, scope['target_session'], host_id)
            db.execute('INSERT INTO root_turnover_second_continuations VALUES(?,?,?,?,?,?,?,NULL,NULL)',
                       (*identity, thread_id, epoch_id))
        prompt = ('A new Root reply is available in Agent Chat. Read the complete message with '
                  f'its exact ID {message_id}, using your existing own project and session. '
                  'Continue only the two approved replacement requests in that reply. '
                  'The original stop remains active. Do not dispatch product work.')
        try:
            response = rpc.request('turn/start', {'threadId': thread_id, 'clientUserMessageId': message_id,
                                   'input': [{'type': 'text', 'text': prompt, 'text_elements': []}]})
        except RpcError as error:
            with coord.tx() as db:
                # A refusal is retained without asserting the preceding native effect was absent.
                db.execute('UPDATE root_turnover_second_continuations SET provider_error=? WHERE epoch_id=?',
                           (str(error)[:512], epoch_id))
            raise
        turn = response.get('turn') if isinstance(response, dict) else None
        turn_id = turn.get('id') if isinstance(turn, dict) else None
        if not isinstance(turn_id, str) or not turn_id or turn_id == first[1]:
            raise CoordError('second turnover continuation returned no distinct native turn; submission outcome is unknown')
        with coord.tx() as db:
            execution_epoch_settle(db, epoch_id, host_id, 'submitted')
            db.execute('UPDATE root_turnover_second_continuations SET native_turn_id=? WHERE epoch_id=?',
                       (turn_id, epoch_id))
    return _second_receipt(coord.db, identity)
