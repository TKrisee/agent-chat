"""Continue a loaded canonical root after a recognized temporary outage.

No original input is replayed. Native queue admission, not turn/start's steering
semantics, serializes the continuation against an independently connected CLI.
"""
import time
import uuid
import math
import hashlib
import json

from .core import CoordError, assert_agent_running, assert_reset_unlocked
from .rpc import RpcError, TransportError
from .root_owner import RootOwnerMonitor, OWNER_FIELDS

CAPACITY_MESSAGE = 'Selected model is at capacity. Please try a different model.'
CONTINUATION = ('Continue only unfinished work already authorized in this conversation. '
                'Review prior actions and their effects before continuing; do not repeat '
                'completed actions or replay the previous request or tool calls. '
                'If no authorized work remains, stop.')
DELAYS = (30, 60, 120, 300)
FINAL = ('completed', 'cancelled', 'exhausted')
REMOTE_COMPACT_STREAM_PREFIX = ('Error running remote compact task: stream disconnected before completion: '
                                'An error occurred while processing your request')


def retry_delay(attempts):
    return DELAYS[min(attempts, len(DELAYS) - 1)]


def capacity_error(error):
    if not isinstance(error, dict):
        return False
    code = error.get('codexErrorInfo')
    message = error.get('message')
    if not isinstance(message, str):
        return False
    excluded = ('unauthorized', 'authentication', 'billing', 'quota', 'usage limit',
                'context window', 'sandbox', 'approval', 'safety', 'permission denied',
                'rate limit', 'rate_limit')
    if any(marker in message.lower() for marker in excluded):
        return False
    if code in ('serverOverloaded', 'internalServerError'):
        return True
    if isinstance(code, dict) and len(code) == 1:
        variant, details = next(iter(code.items()))
        if variant not in ('responseStreamDisconnected', 'responseStreamConnectionFailed',
                           'httpConnectionFailed', 'responseTooManyFailedAttempts') or not isinstance(details, dict):
            return False
        status = details.get('httpStatusCode')
        return (status is None and variant != 'responseTooManyFailedAttempts') or status in (500, 502, 503, 504)
    return code in (None, 'other') and (
        message == CAPACITY_MESSAGE or message.startswith(REMOTE_COMPACT_STREAM_PREFIX))


class CapacityState:
    def __init__(self, state):
        self.state, self.coord, self.db = state, state.coord, state.db
        self.db.execute('''CREATE TABLE IF NOT EXISTS bridge_capacity_recoveries (
            id TEXT PRIMARY KEY, session_id TEXT NOT NULL, thread_id TEXT NOT NULL,
            origin_turn_id TEXT NOT NULL UNIQUE, failed_turn_id TEXT NOT NULL,
            status TEXT NOT NULL, attempts INTEGER NOT NULL, due_at REAL NOT NULL,
            client_id TEXT, queue_id TEXT, retry_turn_id TEXT, error TEXT,
            created_at REAL NOT NULL, updated_at REAL NOT NULL)''')
        columns = {column:'TEXT' for column in ('settings_digest', 'model', 'reasoning_effort', 'native_proof', 'cancel_reason', 'cancel_mode', 'prior_cancel_reason', 'owner_start', 'owner_endpoint', 'owner_identity')}
        columns.update(owner_pid='INTEGER', owner_uid='INTEGER', owner_generation='REAL', rearms='INTEGER NOT NULL DEFAULT 0')
        for column, kind in columns.items():
            if column not in {row['name'] for row in self.db.execute('PRAGMA table_info(bridge_capacity_recoveries)')}:
                self.db.execute('ALTER TABLE bridge_capacity_recoveries ADD COLUMN ' + column + ' ' + kind)

    def targets(self):
        from .root_orchestrator import projection
        return [dict(row) for row in self.db.execute('''SELECT b.session_id,b.thread_id,b.updated_at AS generation
            FROM bridge_bindings b WHERE b.thread_id IS NOT NULL
            AND b.parent_session IS NULL AND b.agent_path IS NULL''')
            if (root := projection(self.db, row['session_id']))
            and root['home_session'] == row['session_id']
            and root['thread_id'] == row['thread_id']]

    def rows(self):
        return [dict(row) for row in self.db.execute('''SELECT * FROM bridge_capacity_recoveries
            WHERE status NOT IN ('completed','cancelled','exhausted') ORDER BY created_at''')]

    def gate(self, session_id, thread_id):
        if not any(target['session_id'] == session_id and target['thread_id'] == thread_id for target in self.targets()):
            return 'canonical root binding changed'
        try:
            assert_agent_running(self.db, session_id)
            assert_reset_unlocked(self.db, session_id)
        except CoordError as error:
            return str(error)
        if self.db.execute('''SELECT 1 FROM bridge_jobs WHERE thread_id=?
                AND status NOT IN ('dispatched','cancelled')''', (thread_id,)).fetchone():
            return 'managed input is pending'
        if self.db.execute('''SELECT 1 FROM agent_control_requests WHERE target_session=?
                AND status NOT IN ('completed','failed','cancelled','resolved_unchanged')''', (session_id,)).fetchone():
            return 'agent control is pending'
        for message in self.state.pending():
            route = self.state.resolve(message['recipient_session'])
            if route and route['thread_id'] == thread_id:
                return 'new managed input is pending'
        return None

    def schedule(self, session_id, thread_id, turn_id, now, settings_digest, model, reasoning_effort, owner):
        with self.coord.tx():
            if self.gate(session_id, thread_id):
                return None
            target = next(target for target in self.targets() if target['session_id'] == session_id and target['thread_id'] == thread_id)
            if (not isinstance(owner, dict) or set(owner) != set(OWNER_FIELDS)
                    or not isinstance(owner['owner_pid'], int) or isinstance(owner['owner_pid'], bool) or owner['owner_pid'] <= 0
                    or not isinstance(owner['owner_uid'], int) or isinstance(owner['owner_uid'], bool) or owner['owner_uid'] < 0
                    or owner['owner_generation'] != target['generation']
                    or any(not isinstance(owner[key], str) or not owner[key] or len(owner[key]) > 256 for key in ('owner_start', 'owner_endpoint', 'owner_identity'))):
                raise CoordError('exact native CLI owner evidence required for recovery')
            old = self.db.execute('''SELECT * FROM bridge_capacity_recoveries
                WHERE origin_turn_id=? OR failed_turn_id=? OR retry_turn_id=?''', (turn_id, turn_id, turn_id)).fetchone()
            if old:
                reason = old['cancel_reason']
                drift = reason in ('native model, effort or configuration changed during recovery',
                                   'native configuration changed before continuation admission')
                model_settled = reason == 'agent control is pending' and self.db.execute('''SELECT 1 FROM agent_control_requests
                    WHERE target_session=? AND action='model' AND status IN ('completed','resolved_unchanged')
                    AND updated_at>=?''', (session_id, old['updated_at'])).fetchone()
                proven = old['native_proof'] == 'not_submitted' or (old['native_proof'] or '').startswith(('queue_deleted:', 'turn_stopped:', 'turn_observed:'))
                handoff = (self.db.execute("SELECT 1 FROM sqlite_master WHERE name='root_native_handoffs'").fetchone()
                           and self.db.execute("SELECT 1 FROM root_native_handoffs WHERE kind='capacity' AND job_id=? AND active=1", (old['id'],)).fetchone())
                if old['status'] == 'cancelled' and (drift or model_settled) and proven and not handoff and old['owner_identity'] == owner['owner_identity']:
                    self.db.execute('''UPDATE bridge_capacity_recoveries SET status='pending',failed_turn_id=?,
                        due_at=?,settings_digest=?,model=?,reasoning_effort=?,client_id=NULL,queue_id=NULL,
                        cancel_reason=NULL,cancel_mode=NULL,prior_cancel_reason=?,rearms=rearms+1,error=?,updated_at=? WHERE id=?''',
                        (turn_id, max(old['due_at'], now + retry_delay(old['attempts'])), settings_digest, model, reasoning_effort,
                         reason, 'rearmed after settled model/configuration change', now, old['id']))
                    return self.get(old['id'])
                return dict(old)
            if any(row['thread_id'] == thread_id for row in self.rows()):
                return None
            job_id = 'capacity_' + uuid.uuid4().hex
            self.db.execute('''INSERT INTO bridge_capacity_recoveries
                (id,session_id,thread_id,origin_turn_id,failed_turn_id,status,attempts,due_at,created_at,updated_at,settings_digest,model,reasoning_effort,
                 owner_pid,owner_start,owner_uid,owner_endpoint,owner_generation,owner_identity)
                VALUES (?,?,?,?,?,'pending',0,?,?,?,?,?,?,?,?,?,?,?,?)''',
                (job_id, session_id, thread_id, turn_id, turn_id, now + DELAYS[0], now, now, settings_digest, model, reasoning_effort,
                 *(owner[key] for key in OWNER_FIELDS)))
            return self.get(job_id)

    def get(self, job_id):
        row = self.db.execute('SELECT * FROM bridge_capacity_recoveries WHERE id=?', (job_id,)).fetchone()
        if not row:
            raise CoordError('unknown capacity recovery')
        return dict(row)

    def update(self, job_id, expected, status, now, **values):
        allowed = {'attempts', 'due_at', 'client_id', 'queue_id', 'retry_turn_id', 'failed_turn_id', 'error', 'native_proof', 'cancel_reason', 'cancel_mode'}
        if status not in (*FINAL, 'uncertain', 'pending', 'adding', 'queued', 'starting', 'retrying', 'cancelling') or set(values) - allowed:
            raise CoordError('invalid capacity recovery transition')
        if not isinstance(now, (int, float)) or isinstance(now, bool) or not math.isfinite(now):
            raise CoordError('invalid capacity recovery clock')
        for key, value in values.items():
            if key in ('attempts', 'due_at'):
                if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value):
                    raise CoordError('invalid capacity recovery numeric metadata')
            elif value is not None and (not isinstance(value, str) or len(value) > 8192):
                raise CoordError('invalid capacity recovery evidence')
        with self.coord.tx():
            row = self.get(job_id)
            if row['status'] != expected or row['status'] in FINAL:
                raise CoordError('capacity recovery changed; dispatch withheld')
            if status == 'adding' and expected != status and (expected != 'pending' or values.get('attempts') != row['attempts'] + 1
                                      or values.get('client_id') != job_id + '_' + str(row['attempts'] + 1)
                                      or now < row['due_at']):
                raise CoordError('capacity retry budget or admission deadline changed')
            if status == 'starting' and expected != status and (expected != 'queued' or not row['queue_id'] or not row['client_id']):
                raise CoordError('capacity continuation requires its exact retained queue input')
            if 'attempts' in values and status != 'adding':
                raise CoordError('capacity retry attempt count is immutable outside admission')
            if status in ('adding', 'starting') and status != expected:
                reason = self.gate(row['session_id'], row['thread_id'])
                if reason:
                    raise CoordError(reason)
                target = next(target for target in self.targets() if target['session_id'] == row['session_id'] and target['thread_id'] == row['thread_id'])
                if not row['owner_identity'] or row['owner_generation'] != target['generation']:
                    raise CoordError('root owner generation changed; continuation withheld')
            assignments = ['status=?', 'updated_at=?'] + [key + '=?' for key in values]
            self.db.execute('UPDATE bridge_capacity_recoveries SET ' + ','.join(assignments) + ' WHERE id=?',
                            (status, now, *values.values(), job_id))
            return self.get(job_id)


class CapacityWorker:
    def __init__(self, bridge, clock=time.time, owner_monitor=None):
        self.bridge, self.state, self.rpc, self.clock = bridge, bridge.state, bridge.rpc, clock
        self.boundary = 0
        self.observed = {}
        self.attached = {}
        self.epoch = object()
        self.cold_failures = {}
        self.owner_monitor = owner_monitor or RootOwnerMonitor(getattr(self.rpc, 'endpoint', None))

    def owner(self, job):
        if not job.get('owner_identity'):
            return dict(state='unknown', reason='retained recovery lacks native CLI owner identity')
        target = next((target for target in self.state.capacity_targets()
                       if target['session_id'] == job['session_id'] and target['thread_id'] == job['thread_id']), None)
        if not target:
            return dict(state='closed', reason='canonical root binding changed')
        return self.owner_monitor.observe(target, job)

    def owner_admits(self, job):
        proof = self.owner(job)
        if proof['state'] == 'closed':
            self.cancel(job, proof['reason'], interrupt_owned=True)
            return False
        if proof['state'] != 'open':
            self.write(job, job['status'], error=proof['reason'])
            return False
        return True

    def write(self, job, status, **values):
        return self.state.capacity_update(job['id'], job['status'], status, self.clock(), **values)

    def native(self, thread_id):
        thread = self.rpc.request('thread/read', {'threadId': thread_id, 'includeTurns': False})['thread']
        if thread.get('id') != thread_id:
            raise CoordError('capacity observation is not for the exact thread')
        loaded = thread.get('status', {}).get('type') != 'notLoaded'
        status = thread.get('status', {}).get('type')
        direct = thread.get('canAcceptDirectInput') is True
        from .native_history import turns_page
        turns = turns_page(self.rpc, dict(threadId=thread_id, limit=1, itemsView='summary', sortDirection='desc'))['data'] if loaded else []
        latest = turns[0] if turns else None
        admitted = direct and (status == 'idle' or (status == 'systemError' and latest
                              and latest.get('status') == 'failed' and capacity_error(latest.get('error'))))
        return bool(admitted), latest

    def settings(self, thread_id):
        # Admission was checked loaded before this metadata-only attachment.
        # No model, effort, permission, cwd or other overrides are supplied.
        response = self.rpc.request('thread/resume', {'threadId': thread_id, 'excludeTurns': True})
        from .control_worker import ControlWorker
        config = ControlWorker.configuration(response, thread_id)
        keys = ('model', 'modelProvider', 'reasoningEffort', 'cwd', 'sandbox', 'approvalPolicy',
                'approvalsReviewer', 'activePermissionProfile', 'runtimeWorkspaceRoots', 'collaborationMode',
                'multiAgentMode', 'serviceTier', 'disabledPluginIds', 'instructionSources')
        if not isinstance(config.get('model'), str) or not config['model']:
            raise CoordError('native model evidence unavailable; capacity continuation withheld')
        digest = hashlib.sha256(json.dumps({key:config.get(key) for key in keys}, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        return digest, config['model'], config.get('reasoningEffort')

    def cancel(self, job, reason, interrupt_owned=False):
        interrupt_owned = interrupt_owned or job.get('cancel_mode') == 'owner_closed'
        job = self.write(job, 'cancelling', error=reason, cancel_reason=reason,
                         cancel_mode='owner_closed' if interrupt_owned else job.get('cancel_mode'))
        matches = [entry for entry in self.bridge.queue(job['thread_id'])
                   if entry.get('clientUserMessageId') == job['client_id'] and job['client_id']]
        proof = 'not_submitted' if not job['client_id'] else None
        for entry in matches:
            result = self.rpc.request('thread/queue/delete', {'threadId': job['thread_id'], 'queuedSubmissionId': entry['id']})
            if result.get('deleted') is True:
                proof = 'queue_deleted:' + entry['id']
        remaining = [entry for entry in self.bridge.queue(job['thread_id'])
                     if job['client_id'] and entry.get('clientUserMessageId') == job['client_id']]
        if remaining:
            # A refusal retains the exact item for a later cancellation attempt.
            self.write(job, 'cancelling', error=reason + '; own queue deletion not confirmed')
            return
        if not proof:
            _, latest = self.native(job['thread_id'])
            if latest and any(item.get('type') == 'userMessage' and item.get('clientId') == job['client_id']
                              for item in latest.get('items', [])):
                if interrupt_owned and latest['status'] == 'inProgress':
                    if job.get('native_proof') in ('interrupt_requested:' + latest['id'], 'interrupt_acknowledged:' + latest['id']):
                        self.write(job, 'cancelling', error='native CLI closed; awaiting exact recovery turn terminal proof')
                        return
                    job = self.write(job, 'cancelling', retry_turn_id=latest['id'], native_proof='interrupt_requested:' + latest['id'])
                    self.rpc.request('turn/interrupt', {'threadId': job['thread_id'], 'turnId': latest['id']})
                    self.write(job, 'cancelling', native_proof='interrupt_acknowledged:' + latest['id'],
                               error='native CLI closed; awaiting exact recovery turn terminal proof')
                    return  # Interrupt acknowledgement alone is not closure.
                proof = ('turn_stopped:' if interrupt_owned else 'turn_observed:') + latest['id']
        if not proof:
            self.write(job, 'uncertain', error=reason + '; native outcome unverified, cancellation not confirmed')
            return
        self.write(job, 'cancelled', error=reason, native_proof=proof)

    def advance(self, job, terminals):
        if job['status'] == 'uncertain':
            # Reconcile only: a newly visible queue input is cancelled, never
            # started, and an observed accepted turn settles the native handoff.
            entries = self.bridge.queue(job['thread_id'])
            matches = [entry for entry in entries if job['client_id'] and entry.get('clientUserMessageId') == job['client_id']]
            _, latest = self.native(job['thread_id'])
            own = latest and job['client_id'] and any(item.get('type') == 'userMessage' and item.get('clientId') == job['client_id']
                                                      for item in latest.get('items', []))
            if not matches and not own:
                return
            reason = job['cancel_reason'] or self.state.capacity_gate(job['session_id'], job['thread_id'])
            owner = self.owner(job)
            if owner['state'] == 'closed':
                self.cancel(job, owner['reason'], interrupt_owned=True)
                return
            if matches or reason:
                self.cancel(job, reason or 'ambiguous native request reconciled; retained continuation cancelled')
            else:
                self.write(job, 'retrying', retry_turn_id=latest['id'], native_proof='turn_observed:' + latest['id'], error=None)
            return
        if job['status'] == 'cancelling':
            self.cancel(job, job['error'])
            return
        reason = self.state.capacity_gate(job['session_id'], job['thread_id'])
        if reason:
            self.cancel(job, reason)
            return
        if not self.owner_admits(job):
            return
        idle, latest = self.native(job['thread_id'])
        if not latest:
            self.cancel(job, 'latest native turn unavailable')
            return
        own = job['client_id'] and any(item.get('type') == 'userMessage' and item.get('clientId') == job['client_id']
                                      for item in latest.get('items', []))
        if own:
            if latest['status'] == 'inProgress':
                self.write(job, 'retrying', retry_turn_id=latest['id'], native_proof='turn_observed:' + latest['id'])
                return
            terminal = terminals.get((job['thread_id'], latest['id']))
            failure = terminal.get('error') if terminal else latest.get('error')
            # Native history permits durable reconciliation after reconnect; a
            # continued temporary failure retains the same recovery sequence.
            if latest['status'] == 'failed' and capacity_error(failure):
                self.write(job, 'pending', failed_turn_id=latest['id'], retry_turn_id=latest['id'],
                           due_at=self.clock() + retry_delay(job['attempts']), client_id=None, queue_id=None,
                           error=failure['message'][:8192], native_proof='turn_observed:' + latest['id'])
                return
            self.write(job, 'completed' if latest['status'] == 'completed' else 'cancelled',
                       retry_turn_id=latest['id'], error=None if latest['status'] == 'completed' else
                       'continuation ended without retryable failure: ' + str((latest.get('error') or {}).get('message', latest['status']))[:7900],
                       native_proof='turn_observed:' + latest['id'])
            return
        if latest['id'] != job['failed_turn_id'] or latest['status'] != 'failed':
            self.cancel(job, 'new native input or progress superseded failed turn')
            return
        if not idle:
            self.cancel(job, 'root no longer available for direct continuation input')
            return
        if self.settings(job['thread_id'])[0] != job['settings_digest']:
            self.cancel(job, 'native model, effort or configuration changed during recovery')
            return
        entries = self.bridge.queue(job['thread_id'])
        matches = [entry for entry in entries if job['client_id'] and entry.get('clientUserMessageId') == job['client_id']]
        if any(entry not in matches for entry in entries):
            self.cancel(job, 'new native queued input superseded recovery')
            return
        if job['status'] in ('adding', 'starting', 'retrying'):
            if len(matches) == 1:
                job = self.write(job, 'queued', queue_id=matches[0]['id'], native_proof='retained_queue:' + matches[0]['id'])
            else:
                self.write(job, 'uncertain', error='native request outcome unverified; automatic replay withheld')
                return
        if job['status'] == 'pending':
            if self.clock() < job['due_at']:
                return
            if not self.owner_admits(job):
                return
            job = self.write(job, 'adding', attempts=job['attempts'] + 1,
                             client_id=job['id'] + '_' + str(job['attempts'] + 1), error=None)
            result = self.rpc.request('thread/queue/add', {'threadId': job['thread_id'], 'clientUserMessageId': job['client_id'],
                                      'input': [{'type': 'text', 'text': CONTINUATION, 'text_elements': []}]})
            # queue/add may auto-dispatch before its acknowledgement. Only
            # positive exact queue/history proof settles this native intention.
            job = self.write(job, 'adding', queue_id=result['queuedSubmission']['id'])
            return self.advance(job, terminals)
        if job['status'] == 'queued':
            idle, latest = self.native(job['thread_id'])
            entries = self.bridge.queue(job['thread_id'])
            if not idle or not latest or latest['id'] != job['failed_turn_id'] or latest['status'] != 'failed':
                self.cancel(job, 'native progress before continuation admission')
                return
            if len(entries) != 1 or entries[0]['id'] != job['queue_id'] or entries[0].get('clientUserMessageId') != job['client_id']:
                self.cancel(job, 'native queue changed before continuation admission')
                return
            if self.settings(job['thread_id'])[0] != job['settings_digest']:
                self.cancel(job, 'native configuration changed before continuation admission')
                return
            if not self.owner_admits(job):
                return
            job = self.write(job, 'starting', error=None)
            try:
                result = self.rpc.request('thread/queue/start', {'threadId': job['thread_id'], 'queuedSubmissionId': job['queue_id']})
            except RpcError as error:
                # Queue remains the durable unique input; a concurrent native turn
                # cannot be steered by this RPC. Reconcile/cancel on the next tick.
                self.write(job, 'queued', error=str(error))
                return
            self.write(job, 'retrying', retry_turn_id=result['turn']['id'])

    def tick(self, enabled=True):
        if not hasattr(self.rpc, 'notification_boundary'):
            return
        # Draining notifications never waits for a model response.
        end = self.rpc.notification_boundary(deadline=time.monotonic() + self.rpc.timeout)
        try:
            notifications = self.rpc.notifications_since(self.boundary)
        except TransportError:
            # Reconnection/drop means fresh discovery proof was lost. Existing
            # durable requests still reconcile, but historical failures are cold.
            notifications = []
        self.boundary = end
        terminals = {}
        for _, notification in notifications:
            if notification.get('method') != 'turn/completed':
                continue
            params = notification.get('params', {})
            turn = params.get('turn', {})
            if isinstance(turn, dict) and turn.get('id') and turn.get('status') in ('failed', 'completed', 'interrupted'):
                terminals[(params.get('threadId'), turn['id'])] = turn
        targets = self.state.capacity_targets()
        owner_proofs = {}
        known = {(job['thread_id'], turn_id) for job in self.state.capacity_jobs()
                 for turn_id in (job['origin_turn_id'], job['failed_turn_id'], job['retry_turn_id']) if turn_id}
        # thread/read does not imply notification subscription. Attach only to
        # the already-loaded canonical root, once per initialize response/root.
        # Passive same-turn inProgress -> failed also establishes fresh failure
        # if notification subscription was unavailable during a reconnect.
        epoch = getattr(self.rpc, 'initialize_response', self.rpc)
        if self.epoch is not epoch:
            self.epoch = epoch  # Retain identity; avoid object-ID reuse on reconnect.
            self.attached.clear()
            self.observed.clear()
            self.cold_failures.clear()
        for target in targets if enabled else []:
            thread_id = target['thread_id']
            if self.state.capacity_gate(target['session_id'], thread_id):
                self.observed.pop(thread_id, None)
                continue
            owner = self.owner_monitor.observe(target)
            if owner['state'] != 'open':
                self.state.observe(thread_id, 'recovery_blocked', owner['reason'])
                self.observed.pop(thread_id, None)
                continue
            owner_proofs[thread_id] = owner
            idle, latest = self.native(thread_id)
            if not idle and not latest:
                self.observed.pop(thread_id, None)
                continue
            if thread_id not in self.attached:
                self.settings(thread_id)
                self.attached[thread_id] = epoch
                _, latest = self.native(thread_id)
                self.cold_failures[thread_id] = latest['id'] if latest and latest['status'] == 'failed' else None
                # Resume/read may replay historical observations. Establish a
                # receive boundary after attachment before accepting new events.
                self.boundary = self.rpc.notification_boundary(deadline=time.monotonic() + self.rpc.timeout)
            previous = self.observed.get(thread_id)
            if latest and previous == (latest['id'], 'inProgress') and latest['status'] == 'failed':
                terminals[(thread_id, latest['id'])] = latest
            if latest and latest['status'] == 'failed':
                if capacity_error(latest.get('error')):
                    # An exact living native resume owner establishes current
                    # user intent even when this bridge starts after the failure.
                    terminals[(thread_id, latest['id'])] = latest
                else:
                    self.state.observe(thread_id, 'recovery_blocked', str((latest.get('error') or {}).get('message', 'native failure is not retryable'))[:8192])
            self.observed[thread_id] = (latest['id'], latest['status']) if latest else None
        for target in targets if enabled else []:
            for (thread_id, turn_id), turn in terminals.items():
                if target['thread_id'] != thread_id or turn['status'] != 'failed' or not capacity_error(turn.get('error')):
                    continue
                if (thread_id, turn_id) in known:
                    continue
                if thread_id not in owner_proofs:
                    continue
                idle, latest = self.native(thread_id)
                if idle and latest and latest['id'] == turn_id and latest['status'] == 'failed' and not self.bridge.queue(thread_id):
                    digest, model, effort = self.settings(thread_id)
                    owner = self.owner_monitor.observe(target, owner_proofs[thread_id])
                    if owner['state'] != 'open':
                        self.state.observe(thread_id, 'recovery_blocked', owner['reason'])
                        continue
                    job = self.state.capacity_schedule(target['session_id'], thread_id, turn_id, self.clock(), digest, model, effort,
                                                       {key:owner[key] for key in OWNER_FIELDS})
                    if job and job['status'] == 'pending' and not job['error']:
                        self.write(job, 'pending', error=turn['error']['message'][:8192])
        for job in self.state.capacity_jobs():
            try:
                if enabled:
                    self.advance(job, terminals)
                else:
                    self.cancel(job, 'capacity recovery disabled')
            except (RpcError, TransportError) as error:
                # A persisted native intention must be reconciled, never replayed.
                current = next((row for row in self.state.capacity_jobs() if row['id'] == job['id']), None)
                if current:
                    self.write(current, current['status'], error=str(error))
                if isinstance(error, TransportError):
                    raise
