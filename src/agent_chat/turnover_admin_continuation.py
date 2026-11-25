"""One Root-authorized stopped-PO continuation per exact receipt chain."""
from uuid import UUID

from .core import CoordError, execution_epoch_handoff, execution_epoch_settle
from .turnover_bootstrap import (_completed_root_reply, _assert_idle_turnover_thread,
                                 _assert_second_input_unblocked)


def _retirement_proof(db, scope, retirement_ids):
    required = ('root_parked_retirements', 'root_parked_bound_retirements', 'root_turnover_subordinates')
    if any(not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone() for name in required):
        raise CoordError('admin continuation requires exact bound-worker retirement receipts')
    proofs = []
    for request_id in retirement_ids:
        row = db.execute('''SELECT r.request_id,r.target_session,r.project_id,r.stop_id,r.bootstrap_id,
            r.root_session,r.po_session,r.po_thread,r.host_id,
            b.agent,b.logical_owner,b.old_thread,b.old_rollout,b.repair_request_id,b.candidate,
            b.binding_updated_at,b.repair_status,b.creation_epoch,b.metadata_epoch
            FROM root_parked_retirements r
            JOIN root_parked_bound_retirements b ON b.request_id=r.request_id
            JOIN root_turnover_subordinates s ON s.request_id=r.request_id AND s.target_session=r.target_session
                AND s.project_id=r.project_id AND s.stop_id=r.stop_id AND s.bootstrap_id=r.bootstrap_id
                AND s.root_session=r.root_session AND s.thread_id=r.po_thread AND s.host_id=r.host_id
            JOIN session_resets j ON j.id=r.request_id AND j.target_session=r.target_session
                AND j.requester_session=r.po_session AND j.old_thread_id=r.po_thread AND j.host_id=r.host_id
                AND j.kind='create' AND j.status='parked'
                AND j.new_thread_id=b.old_thread AND j.queue_id IS NULL
            JOIN retired_sessions t ON t.id=r.target_session AND t.agent=b.agent
            WHERE r.request_id=? AND r.project_id=? AND r.stop_id=? AND r.bootstrap_id=? AND s.reset_id=?
                AND r.root_session=? AND r.po_session=? AND r.po_thread=? AND r.host_id=?
                AND NOT EXISTS (SELECT 1 FROM sessions a WHERE a.id=r.target_session)
                AND NOT EXISTS (SELECT 1 FROM bridge_bindings a WHERE a.session_id=r.target_session)''',
            (request_id, scope['project_id'], scope['stop_id'], scope['request_id'], scope['reset_id'],
             scope['root_session'], scope['target_session'], scope['new_thread_id'], scope['host_id'])).fetchone()
        if not row:
            raise CoordError('admin continuation requires every exact bound-worker retirement and tombstone')
        proofs.append(tuple(row))
    return tuple(proofs)


def _canonical(value):
    try:
        return isinstance(value, str) and str(UUID(value)) == value
    except (ValueError, TypeError, AttributeError):
        return False


def _prior(db, scope, bootstrap_id, previous_id, message_id):
    second = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_turnover_second_continuations'").fetchone()
    row = None
    if second:
        row = db.execute('''SELECT c.message_id,c.native_turn_id FROM root_turnover_second_continuations c
            JOIN execution_epoch_handoffs e ON e.id=c.epoch_id
            WHERE c.request_id=? AND c.continuation_id=? AND c.thread_id=?
              AND e.thread=? AND e.actor=? AND e.host=? AND e.method='turn/start'
              AND e.outcome='submitted' AND e.settled_at IS NOT NULL''',
            (bootstrap_id, previous_id, scope['new_thread_id'], scope['new_thread_id'], scope['target_session'], scope['host_id'])).fetchone()
    if not row and db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_turnover_admin_continuations'").fetchone():
        row = db.execute('''SELECT c.message_id,c.native_turn_id FROM root_turnover_admin_continuations c
            JOIN execution_epoch_handoffs e ON e.id=c.epoch_id
            WHERE c.bootstrap_id=? AND c.continuation_id=? AND c.thread_id=?
              AND e.thread=? AND e.actor=? AND e.host=? AND e.method='turn/start'
              AND e.outcome='submitted' AND e.settled_at IS NOT NULL''',
            (bootstrap_id, previous_id, scope['new_thread_id'], scope['new_thread_id'], scope['target_session'], scope['host_id'])).fetchone()
    if not row or not row['native_turn_id']:
        raise CoordError('admin continuation requires the exact previously submitted turn')
    ordered = db.execute('''SELECT 1 FROM messages prior JOIN messages next ON next.seq>prior.seq
        WHERE prior.id=? AND next.id=?''', (row['message_id'], message_id)).fetchone()
    if not ordered:
        raise CoordError('admin continuation requires a distinct later Root reply')
    return tuple(row)


def _receipt(db, identity, retirement_ids):
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_turnover_admin_continuations'").fetchone():
        return None
    bootstrap, continuation, previous, message = identity
    row = db.execute('''SELECT * FROM root_turnover_admin_continuations
        WHERE continuation_id=? OR previous_id=? OR message_id=?''', (continuation, previous, message)).fetchone()
    if not row:
        return None
    saved_ids = tuple(r['retirement_id'] for r in db.execute('''SELECT retirement_id FROM root_turnover_admin_retirements
        WHERE continuation_id=? ORDER BY retirement_id''', (row['continuation_id'],)))
    if (row['bootstrap_id'], row['continuation_id'], row['previous_id'], row['message_id']) != identity or saved_ids != retirement_ids:
        raise CoordError('admin continuation predecessor, request, reply or retirement set already used')
    result = dict(bootstrap_id=bootstrap, continuation_id=continuation, previous_id=previous,
                  message_id=message, thread_id=row['thread_id'], native_turn_id=row['native_turn_id'],
                  status='started' if row['native_turn_id'] else 'submitted_unknown')
    if not row['native_turn_id']:
        result['native_outcome'] = 'unknown'
    if row['provider_error'] is not None:
        result.update(status='refused', provider_error=row['provider_error'])
    return result


def continue_admin(coord, registry, bootstrap_id, continuation_id, previous_id, message_id,
                   retirement_ids, host_id, confirmed):
    """Submit only the exact new Root reply after the exact prior turn and receipts."""
    ids = (bootstrap_id, continuation_id, previous_id)
    if not all(_canonical(value) for value in ids) or len(set(ids)) != len(ids):
        raise CoordError('admin continuation requires distinct canonical UUIDs')
    if (not isinstance(retirement_ids, list) or not retirement_ids or len(retirement_ids) > 100
            or not all(_canonical(value) for value in retirement_ids)
            or len(set(retirement_ids)) != len(retirement_ids)):
        raise CoordError('admin continuation requires distinct exact retirement IDs')
    retirements = tuple(sorted(retirement_ids))
    identity = (bootstrap_id, continuation_id, previous_id, message_id)
    scope = _completed_root_reply(coord, registry, bootstrap_id, message_id, host_id, confirmed)
    existing = _receipt(coord.db, identity, retirements)
    if existing:
        return existing
    prior = _prior(coord.db, scope, bootstrap_id, previous_id, message_id)
    proof = _retirement_proof(coord.db, scope, retirements)
    _assert_second_input_unblocked(coord.db, scope)

    from .native_history import turns_page
    from .provider_service import configured, option, selected_listener
    from .rpc import RpcClient, RpcError
    from .service import ServiceManager
    manager = ServiceManager()
    wrapper, command = configured(manager)
    endpoint = option(command, '--codex-server')
    listener = selected_listener(endpoint)
    thread = scope['new_thread_id']
    with RpcClient(endpoint, timeout=5) as rpc:
        _assert_idle_turnover_thread(rpc, thread, load=True)
        page = turns_page(rpc, dict(threadId=thread, limit=1, itemsView='notLoaded', sortDirection='desc'))
        turns = page.get('data') if isinstance(page, dict) else None
        if (not isinstance(turns, list) or len(turns) != 1 or turns[0].get('id') != prior[1]
                or turns[0].get('status') not in ('completed', 'failed', 'interrupted')):
            raise CoordError('admin continuation requires the exact terminal prior turn')
        queue = rpc.request('thread/queue/list', {'threadId': thread, 'limit': 1})
        if not isinstance(queue, dict) or queue.get('data') != [] or queue.get('nextCursor') is not None:
            raise CoordError('admin continuation requires an empty native queue')
        _assert_idle_turnover_thread(rpc, thread)
        if configured(manager)[0] != wrapper or selected_listener(endpoint) != listener:
            raise CoordError('configured native listener changed before admin continuation')
        with coord.tx() as db:
            existing = _receipt(db, identity, retirements)
            if existing:
                return existing
            if (_completed_root_reply(coord, registry, bootstrap_id, message_id, host_id, confirmed) != scope
                    or _prior(db, scope, bootstrap_id, previous_id, message_id) != prior
                    or _retirement_proof(db, scope, retirements) != proof):
                raise CoordError('admin continuation proof changed before submission')
            _assert_second_input_unblocked(db, scope)
            db.execute('''CREATE TABLE IF NOT EXISTS root_turnover_admin_continuations (
                bootstrap_id TEXT NOT NULL, continuation_id TEXT PRIMARY KEY, previous_id TEXT NOT NULL UNIQUE,
                message_id TEXT NOT NULL UNIQUE, thread_id TEXT NOT NULL, epoch_id TEXT NOT NULL,
                native_turn_id TEXT, provider_error TEXT,
                CHECK(native_turn_id IS NULL OR provider_error IS NULL))''')
            db.execute('''CREATE TABLE IF NOT EXISTS root_turnover_admin_retirements (
                continuation_id TEXT NOT NULL, retirement_id TEXT NOT NULL,
                PRIMARY KEY(continuation_id,retirement_id))''')
            epoch = execution_epoch_handoff(db, 'turn/start', thread, scope['target_session'], host_id)
            db.execute('INSERT INTO root_turnover_admin_continuations VALUES(?,?,?,?,?,?,NULL,NULL)', (*identity, thread, epoch))
            db.executemany('INSERT INTO root_turnover_admin_retirements VALUES(?,?)',
                           [(continuation_id, item) for item in retirements])
        prompt = ('A new Root reply is available in Agent Chat. Read the complete message with '
                  f'its exact ID {message_id}, using your existing own project and session. '
                  'Continue only the administrative work authorized in that reply. '
                  'The original stop remains active. Do not dispatch product work.')
        try:
            response = rpc.request('turn/start', {'threadId': thread, 'clientUserMessageId': message_id,
                                   'input': [{'type': 'text', 'text': prompt, 'text_elements': []}]})
        except RpcError as error:
            with coord.tx() as db:
                db.execute('UPDATE root_turnover_admin_continuations SET provider_error=? WHERE epoch_id=?',
                           (str(error)[:512], epoch))
            raise
        turn = response.get('turn') if isinstance(response, dict) else None
        turn_id = turn.get('id') if isinstance(turn, dict) else None
        if not isinstance(turn_id, str) or not turn_id or turn_id == prior[1]:
            raise CoordError('admin continuation returned no distinct turn; native outcome is unknown')
        with coord.tx() as db:
            execution_epoch_settle(db, epoch, host_id, 'submitted')
            db.execute('UPDATE root_turnover_admin_continuations SET native_turn_id=? WHERE epoch_id=?',
                       (turn_id, epoch))
    return _receipt(coord.db, identity, retirements)
