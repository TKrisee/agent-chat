"""Exercise durable recovery with isolated coordination and a fake native runtime."""
import copy
import contextlib
from pathlib import Path
import tempfile
import unittest
import uuid
from unittest import mock

from agent_chat.bridge import Bridge
from agent_chat.bridge_state import BridgeState
from agent_chat.bridge_client import RemoteBridgeState
from agent_chat.capacity_recovery import CAPACITY_MESSAGE, CONTINUATION, REMOTE_COMPACT_STREAM_PREFIX, capacity_error
from agent_chat.core import Coordinator, CoordError
from agent_chat.rpc import RpcError, TransportError
from agent_chat.root_owner import OWNER_FIELDS
from test_remote_web import RemoteWebFixture


class CapacityRpc:
    timeout = 1

    def __init__(self, thread):
        self.thread = thread
        self.status = 'idle'
        self.direct = True
        self.turns = []
        self.entries = []
        self.notifications = []
        self.calls = []
        self.drop_add = self.drop_start = False
        self.on_add = self.on_start = None
        self.on_resume = None
        self.refuse_delete = False
        self.auto_add = False
        self.auto_fail = self.auto_complete = False
        self.interrupt_error = self.interrupt_drop = False
        self.config = dict(model='current-model', reasoningEffort='high', cwd='/work',
                           approvalPolicy='on-request', sandbox={'type': 'workspaceWrite'},
                           runtimeWorkspaceRoots=['/work'])

    def connect(self): return self
    def close(self): pass
    def notification_boundary(self, **kwargs): return len(self.notifications)
    def notifications_since(self, boundary): return list(enumerate(self.notifications[boundary:], boundary + 1))

    def emit(self, method, params):
        self.notifications.append(dict(method=method, params=copy.deepcopy(params)))

    def fail(self, error=None, notify=True):
        error = error or dict(message=CAPACITY_MESSAGE, codexErrorInfo='serverOverloaded')
        if not self.turns or self.turns[0]['status'] != 'inProgress':
            self.turns.insert(0, dict(id='failure-' + str(len(self.turns)), status='inProgress', items=[]))
        self.turns[0].update(status='failed', error=error)
        self.status = 'idle'
        if notify: self.emit('turn/completed', dict(threadId=self.thread, turn=self.turns[0]))

    def complete(self):
        self.turns[0].update(status='completed', error=None)
        self.status = 'idle'
        self.emit('turn/completed', dict(threadId=self.thread, turn=self.turns[0]))

    def request(self, method, params):
        self.calls.append((method, copy.deepcopy(params)))
        if method == 'thread/read':
            return dict(thread=dict(id=self.thread, status=dict(type=self.status), canAcceptDirectInput=self.direct,
                                    model=self.config['model'], reasoningEffort=self.config['reasoningEffort']))
        if method == 'thread/resume':
            if self.on_resume: self.on_resume()
            return dict(copy.deepcopy(self.config), thread=dict(id=self.thread))
        if method == 'thread/turns/list':
            return dict(data=copy.deepcopy(self.turns[:params['limit']]), nextCursor=None)
        if method == 'thread/queue/list': return dict(data=copy.deepcopy(self.entries), nextCursor=None)
        if method == 'thread/queue/add':
            entry = dict(id='queue-' + str(len(self.calls)), clientUserMessageId=params['clientUserMessageId'], input=params['input'])
            self.entries.append(entry)
            if self.on_add: self.on_add()
            if self.auto_add:
                self.entries.remove(entry)
                self.turns.insert(0, dict(id='auto-' + str(len(self.turns)), status='inProgress',
                                        items=[dict(type='userMessage', clientId=entry['clientUserMessageId'])]))
                self.status = 'active'
                if self.auto_fail: self.fail()
                if self.auto_complete: self.complete()
            if self.drop_add:
                self.drop_add = False
                raise TransportError('lost add acknowledgement')
            return dict(queuedSubmission=copy.deepcopy(entry))
        if method == 'thread/queue/start':
            if self.on_start: self.on_start()
            error_admission = self.status == 'systemError' and self.turns and self.turns[0]['status'] == 'failed' and capacity_error(self.turns[0].get('error'))
            if (self.status != 'idle' and not error_admission) or not self.direct:
                raise RpcError(-32600, 'thread already has an active or pending turn')
            entry = next(entry for entry in self.entries if entry['id'] == params['queuedSubmissionId'])
            self.entries.remove(entry)
            turn = dict(id='continuation-' + str(len(self.turns)), status='inProgress',
                        items=[dict(type='userMessage', clientId=entry['clientUserMessageId'])])
            self.turns.insert(0, turn)
            self.status = 'active'
            if self.drop_start:
                self.drop_start = False
                raise TransportError('lost start acknowledgement')
            return dict(turn=copy.deepcopy(turn))
        if method == 'thread/queue/delete':
            if self.refuse_delete: return dict(deleted=False)
            self.entries = [entry for entry in self.entries if entry['id'] != params['queuedSubmissionId']]
            return dict(deleted=True)
        if method == 'turn/interrupt':
            if self.interrupt_error: raise RpcError(-32601, 'fixture interrupt unavailable')
            if self.turns[0]['id'] != params['turnId']: raise RpcError(-32600, 'turn changed')
            self.turns[0]['status'] = 'interrupted'
            self.status = 'idle'
            if self.interrupt_drop:
                self.interrupt_drop = False
                raise TransportError('lost interrupt acknowledgement')
            return {}
        raise AssertionError(method)


def enroll(coord, thread):
    coord.db.execute('''CREATE TABLE root_projections (session_id TEXT PRIMARY KEY,
        home_session TEXT, thread_id TEXT, paused INTEGER)''')
    coord.db.execute('INSERT INTO root_projections VALUES(?,?,?,0)', (coord.session, coord.session, thread))


class FakeOwner:
    def __init__(self):
        self.state = 'open'
        self.pid = 4321
        self.start = 'fixed start'

    def observe(self, target, receipt=None):
        if self.state != 'open': return dict(state=self.state, reason='fixture native CLI owner ' + self.state)
        identity = str(self.pid) + ':' + self.start + ':' + str(target['generation'])
        if receipt and receipt.get('owner_identity') and receipt['owner_identity'] != identity:
            return dict(state='closed', reason='fixture native CLI PID reused or root rebound')
        return dict(state='open', reason=None, owner_pid=self.pid, owner_start=self.start,
                    owner_uid=123, owner_endpoint='ws://127.0.0.1:4500', owner_generation=target['generation'], owner_identity=identity)


class CapacityRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.coord = Coordinator(Path(self.tmp.name) / 'coord.db', 'main')
        self.coord.register('main')
        self.addCleanup(self.coord.close)
        self.state = BridgeState(self.coord)
        self.thread = str(uuid.uuid4())
        self.state.bind(thread_id=self.thread)
        enroll(self.coord, self.thread)
        self.rpc = CapacityRpc(self.thread)
        self.now = 1000
        self.owner = FakeOwner()
        self.build_bridge()
        self.tick()  # Attach while root is idle, before any outage.
        self.rpc.calls.clear()

    def build_bridge(self):
        self.bridge = Bridge(self.state, self.rpc)
        self.bridge.capacity_worker.clock = lambda: self.now
        self.bridge.capacity_worker.owner_monitor = self.owner

    def tick(self): self.bridge.tick()
    def row(self): return dict(self.coord.db.execute('SELECT * FROM bridge_capacity_recoveries ORDER BY rowid DESC LIMIT 1').fetchone())
    def count(self, method): return sum(call[0] == method for call in self.rpc.calls)
    def schedule(self):
        self.rpc.fail()
        self.tick()
        self.assertEqual(self.row()['status'], 'pending')

    def test_same_model_continuation_then_completion(self):
        initial = copy.deepcopy(self.rpc.config)
        self.schedule()
        self.now += 29
        self.tick()
        self.assertEqual(self.count('thread/queue/add'), 0)
        self.now += 1
        self.tick()
        self.assertEqual(self.row()['status'], 'retrying')
        self.assertEqual(self.row()['attempts'], 1)
        self.assertEqual(self.rpc.config, initial)
        add = next(params for method, params in self.rpc.calls if method == 'thread/queue/add')
        self.assertEqual(add['input'][0]['text'], CONTINUATION)
        self.assertEqual(set(add), {'threadId', 'clientUserMessageId', 'input'})
        for method, params in self.rpc.calls:
            if method == 'thread/resume': self.assertEqual(params, dict(threadId=self.thread, excludeTurns=True))
        self.assertEqual(self.count('turn/start'), 0)
        self.rpc.complete()
        self.tick()
        self.assertEqual(self.row()['status'], 'completed')
        self.assertEqual(self.state.status(mine=True)['capacity_recoveries'][0]['status'], 'completed')

    def test_persistent_retries_use_capped_delays_and_durable_attempt_count(self):
        self.schedule()
        for attempt, delay in enumerate((30, 60, 120, 300, 300, 300), 1):
            self.assertEqual(self.row()['due_at'], self.now + delay)
            self.now += delay
            self.tick()
            self.assertEqual(self.row()['attempts'], attempt)
            self.rpc.fail()
            self.tick()
            self.build_bridge()
        self.assertEqual(self.row()['status'], 'pending')
        self.assertEqual(self.row()['attempts'], 6)
        self.assertEqual(self.count('thread/queue/start'), 6)
        self.assertEqual(self.coord.db.execute('SELECT COUNT(*) FROM bridge_capacity_recoveries').fetchone()[0], 1)
        self.coord.db.execute('UPDATE root_projections SET paused=1')
        self.now += 300
        self.tick()
        self.assertEqual(self.row()['status'], 'cancelled')
        self.assertEqual(self.count('thread/queue/start'), 6)

    def test_pending_deadline_survives_restart_without_duplicate(self):
        self.schedule()
        identity = self.row()['id']
        due = self.row()['due_at']
        self.rpc.notifications.clear()
        self.state = BridgeState(self.coord)
        self.build_bridge()
        self.tick()
        self.assertEqual((self.row()['id'], self.row()['due_at']), (identity, due))
        self.now = due
        self.tick()
        self.assertEqual(self.count('thread/queue/add'), 1)

    def test_nonterminal_native_retry_does_not_schedule(self):
        self.rpc.turns = [dict(id='native-retry', status='inProgress', items=[])]
        self.rpc.status = 'active'
        self.rpc.emit('error', dict(threadId=self.thread, turnId='native-retry', willRetry=True,
                                   error=dict(message=CAPACITY_MESSAGE, codexErrorInfo='serverOverloaded')))
        self.tick()
        self.assertEqual(self.state.capacity_jobs(), [])
        self.rpc.fail()
        self.tick()
        self.assertEqual(self.row()['status'], 'pending')

    def test_capacity_classification_excludes_other_structured_failures(self):
        self.assertTrue(capacity_error(dict(message='busy', codexErrorInfo='serverOverloaded')))
        self.assertTrue(capacity_error(dict(message=CAPACITY_MESSAGE)))
        self.assertTrue(capacity_error(dict(message=CAPACITY_MESSAGE, codexErrorInfo='other')))
        for code in ('rateLimitExceeded', 'usageLimitExceeded', 'unauthorized', 'contextWindowExceeded', 'sessionBudgetExceeded', 'badRequest'):
            with self.subTest(code=code): self.assertFalse(capacity_error(dict(message=CAPACITY_MESSAGE, codexErrorInfo=code)))
        self.assertFalse(capacity_error(dict(message='prefix ' + CAPACITY_MESSAGE)))
        self.assertFalse(capacity_error(dict(message='model at capacity')))

    def test_structured_stream_and_temporary_server_errors_are_narrowly_retryable(self):
        for variant in ('responseStreamDisconnected', 'responseStreamConnectionFailed', 'httpConnectionFailed', 'responseTooManyFailedAttempts'):
            for status in (None, 500, 502, 503, 504, 400, 401, 402, 403, 408, 429):
                with self.subTest(variant=variant, status=status):
                    expected = status in (500, 502, 503, 504) or (status is None and variant != 'responseTooManyFailedAttempts')
                    self.assertEqual(capacity_error(dict(message='temporary stream transport failure', codexErrorInfo={variant: dict(httpStatusCode=status)})), expected)
        self.assertTrue(capacity_error(dict(message='temporary provider failure', codexErrorInfo='internalServerError')))
        self.assertFalse(capacity_error(dict(message='temporary failure', codexErrorInfo={'unknownFailure': dict(httpStatusCode=503)})))

    def test_exact_remote_compact_stream_failure_recovers_from_system_error(self):
        self.rpc.fail(dict(message=REMOTE_COMPACT_STREAM_PREFIX + '. Please try again. OpenAI request ID: request-test', codexErrorInfo='other'))
        self.rpc.status = 'systemError'
        self.tick()
        self.assertEqual(self.row()['status'], 'pending')
        self.now += 30
        self.tick()
        self.assertEqual(self.row()['status'], 'retrying')
        self.assertEqual(self.count('thread/queue/start'), 1)
        self.assertEqual(self.count('turn/start'), 0)

    def test_generic_other_auth_quota_and_policy_failures_remain_blocked(self):
        errors = [dict(message='Error running remote compact task: unauthorized', codexErrorInfo='other'),
                  dict(message='stream disconnected before completion', codexErrorInfo='other')]
        for code in ('unauthorized', 'usageLimitExceeded', 'contextWindowExceeded', 'sandboxError', 'cyberPolicy', 'misalignmentPolicyViolation', 'tooManyDenials'):
            errors.append(dict(message=REMOTE_COMPACT_STREAM_PREFIX + '. OpenAI request ID: request-test', codexErrorInfo=code))
        for suffix in (' billing failed', ' authentication required', ' quota exceeded', ' approval required'):
            errors.append(dict(message=REMOTE_COMPACT_STREAM_PREFIX + suffix, codexErrorInfo='other'))
        for error in errors:
            with self.subTest(error=error): self.assertFalse(capacity_error(error))

    def test_system_error_requires_direct_admission_and_failed_retryable_latest_turn(self):
        self.rpc.fail()
        self.rpc.status = 'systemError'
        self.rpc.direct = False
        self.tick()
        self.assertEqual(self.state.capacity_jobs(), [])
        self.rpc.direct = True
        self.rpc.turns[0]['status'] = 'inProgress'
        self.tick()
        self.assertEqual(self.state.capacity_jobs(), [])
        self.assertEqual(self.count('thread/queue/add'), 0)

    def test_nonretryable_continuation_failure_stops_with_native_error_visible(self):
        self.schedule()
        self.now += 30
        self.tick()
        self.rpc.fail(dict(message='authentication required', codexErrorInfo='unauthorized'))
        self.tick()
        self.assertEqual(self.row()['status'], 'cancelled')
        self.assertIn('authentication required', self.row()['error'])
        self.now += 1000
        self.tick()
        self.assertEqual(self.count('thread/queue/start'), 1)

    def test_historical_text_or_failure_without_fresh_terminal_is_cold(self):
        self.owner.state = 'unknown'
        self.rpc.fail(notify=False)
        self.rpc.turns[0]['items'].append(dict(type='userMessage', text=CAPACITY_MESSAGE))
        self.tick()
        self.assertEqual(self.state.capacity_jobs(), [])

    def test_loaded_root_attaches_once_and_passive_fresh_failure_recovers(self):
        self.rpc.turns = [dict(id='working', status='inProgress', items=[])]
        self.rpc.status = 'active'
        self.build_bridge()
        self.tick()
        self.assertEqual(self.count('thread/resume'), 1)
        self.tick()
        self.assertEqual(self.count('thread/resume'), 1)
        self.rpc.fail(notify=False)
        self.tick()
        self.assertEqual(self.row()['status'], 'pending')

    def test_disabled_recovery_does_not_attach_to_root(self):
        self.rpc.fail()
        with mock.patch.dict('os.environ', {'AGENT_CHAT_CAPACITY_RECOVERY': '0'}): self.tick()
        self.assertEqual(self.count('thread/resume'), 0)

    def test_cold_failed_attach_and_resume_replay_do_not_recover_history(self):
        self.owner.state = 'unknown'
        self.rpc.fail()
        self.build_bridge()
        self.rpc.on_resume = lambda: self.rpc.emit('turn/completed', dict(threadId=self.thread, turn=self.rpc.turns[0]))
        self.tick()
        self.rpc.on_resume = None
        self.tick()
        self.assertEqual(self.state.capacity_jobs(), [])
        self.owner.state = 'open'
        self.rpc.turns.insert(0, dict(id='fresh-post-attach', status='inProgress', items=[]))
        self.rpc.fail()
        self.tick()
        self.assertEqual(self.row()['status'], 'pending')

    def test_reconnect_clears_passive_freshness_and_reattaches_once(self):
        self.rpc.initialize_response = dict(connection=1)
        self.rpc.turns = [dict(id='working', status='inProgress', items=[])]
        self.rpc.status = 'active'
        self.tick()
        before = self.count('thread/resume')
        self.rpc.initialize_response = dict(connection=2)
        self.rpc.fail(notify=False)
        self.tick()
        self.assertGreater(self.count('thread/resume'), before)
        self.assertEqual(self.row()['status'], 'pending')
        count = self.count('thread/resume')
        self.tick()
        self.assertEqual(self.row()['status'], 'pending')
        self.assertLessEqual(self.count('thread/resume'), count + 1)

    def test_unloaded_root_and_unrelated_thread_notifications_are_cold(self):
        self.rpc.fail()
        self.rpc.status = 'notLoaded'
        self.build_bridge()
        self.tick()
        self.assertEqual(self.count('thread/resume'), 0)
        self.assertEqual(self.state.capacity_jobs(), [])
        self.rpc.status = 'idle'
        self.rpc.turns[0]['status'] = 'completed'
        self.rpc.emit('turn/completed', dict(threadId='other-thread', turn=self.rpc.turns[0]))
        self.tick()
        self.assertEqual(self.state.capacity_jobs(), [])

    def test_startup_current_failed_root_requires_positive_native_owner(self):
        self.rpc.fail(dict(message=REMOTE_COMPACT_STREAM_PREFIX + '. OpenAI request ID: fixture', codexErrorInfo='other'), notify=False)
        self.rpc.status = 'systemError'
        self.owner.state = 'unknown'
        self.build_bridge()
        self.tick()
        self.assertEqual(self.state.capacity_jobs(), [])
        self.assertIn('unknown', self.state.status(mine=True)['threads'][0]['error'])
        self.owner.state = 'open'
        self.tick()
        row = self.row()
        self.assertEqual(row['status'], 'pending')
        self.assertEqual(row['owner_pid'], self.owner.pid)
        self.assertEqual(row['owner_start'], self.owner.start)
        self.assertEqual(self.count('thread/queue/add'), 0)

    def test_owner_close_cancels_pending_while_bridge_keeps_runtime_loaded(self):
        self.schedule()
        self.owner.state = 'closed'
        self.now += 30
        self.tick()
        self.assertEqual(self.rpc.status, 'idle')
        self.assertEqual(self.row()['status'], 'cancelled')
        self.assertEqual(self.count('thread/queue/add'), 0)

    def test_unknown_owner_waits_without_input_then_same_owner_recovers(self):
        self.schedule()
        self.owner.state = 'unknown'
        self.now += 30
        self.tick()
        self.assertEqual(self.row()['status'], 'pending')
        self.assertIn('unknown', self.row()['error'])
        self.assertEqual(self.count('thread/queue/add'), 0)
        self.owner.state = 'open'
        self.tick()
        self.assertEqual(self.row()['status'], 'retrying')

    def test_pid_reuse_cancels_and_does_not_transfer_existing_sequence(self):
        self.schedule()
        self.owner.start = 'replacement start'
        self.now += 30
        self.tick()
        self.assertEqual(self.row()['status'], 'cancelled')
        self.assertIn('reused', self.row()['error'])
        self.tick()
        self.assertEqual(self.count('thread/queue/add'), 0)

    def test_completed_model_control_rearms_proven_cancellation_under_current_settings(self):
        self.schedule()
        identity = self.row()['id']
        self.coord.db.execute('''INSERT INTO agent_control_requests
            (id,requester_session,target_session,host_id,thread_id,action,interrupt,status,created_at,updated_at)
            VALUES ('model-control','main','main','host',?,'model',0,'waiting',?,?)''', (self.thread, self.now, self.now))
        self.bridge.capacity_worker.tick()
        self.assertEqual(self.row()['status'], 'cancelled')
        self.assertEqual(self.row()['native_proof'], 'not_submitted')
        self.rpc.config.update(model='new-current-model', reasoningEffort='low')
        self.now += 1
        self.coord.db.execute("UPDATE agent_control_requests SET status='completed',updated_at=? WHERE id='model-control'", (self.now,))
        self.tick()
        row = self.row()
        self.assertEqual((row['id'], row['status'], row['rearms'], row['attempts']), (identity, 'pending', 1, 0))
        self.assertEqual((row['model'], row['reasoning_effort']), ('new-current-model', 'low'))
        self.assertEqual(row['prior_cancel_reason'], 'agent control is pending')
        self.now += 30
        self.tick()
        add = next(params for method, params in self.rpc.calls if method == 'thread/queue/add')
        self.assertEqual(add['clientUserMessageId'], identity + '_1')
        self.assertEqual(add['input'][0]['text'], CONTINUATION)
        self.assertEqual(self.rpc.config['model'], 'new-current-model')

    def test_closed_owner_cancellation_is_not_rearmed_even_if_settings_change(self):
        self.schedule()
        self.owner.state = 'closed'
        self.tick()
        self.owner.state = 'open'
        self.rpc.config['model'] = 'changed-after-close'
        self.now += 1000
        self.tick()
        self.assertEqual(self.row()['status'], 'cancelled')
        self.assertEqual(self.row()['rearms'], 0)
        self.assertEqual(self.count('thread/queue/add'), 0)

    def test_auto_dispatched_add_is_correlated_without_explicit_start(self):
        self.schedule()
        self.rpc.auto_add = True
        self.now += 30
        self.tick()
        self.assertEqual(self.row()['status'], 'retrying')
        self.assertTrue(self.row()['native_proof'].startswith('turn_observed:'))
        self.assertEqual(self.count('thread/queue/start'), 0)

    def test_auto_dispatched_immediate_failures_retain_one_persistent_sequence(self):
        self.schedule()
        self.rpc.auto_add = self.rpc.auto_fail = True
        for delay in (30, 60, 120, 300, 300):
            self.now += delay
            self.tick()
            self.assertEqual(self.row()['status'], 'pending')
        self.assertEqual(self.row()['attempts'], 5)
        self.assertEqual(self.count('thread/queue/add'), 5)
        self.assertEqual(self.count('thread/queue/start'), 0)
        self.assertEqual(self.coord.db.execute('SELECT COUNT(*) FROM bridge_capacity_recoveries').fetchone()[0], 1)

    def test_close_after_auto_dispatch_interrupts_only_own_turn_and_requires_terminal_readback(self):
        self.schedule()
        self.rpc.auto_add = True
        self.rpc.on_add = lambda: setattr(self.owner, 'state', 'closed')
        self.now += 30
        self.tick()
        self.assertEqual(self.row()['status'], 'cancelling')
        self.assertEqual(self.count('turn/interrupt'), 1)
        params = next(params for method, params in self.rpc.calls if method == 'turn/interrupt')
        self.assertEqual(params['turnId'], self.rpc.turns[0]['id'])
        self.tick()
        self.assertEqual(self.row()['status'], 'cancelled')
        self.assertTrue(self.row()['native_proof'].startswith('turn_stopped:'))

    def test_close_does_not_interrupt_a_new_foreign_turn(self):
        self.schedule()
        self.now += 30
        self.tick()
        self.rpc.turns.insert(0, dict(id='foreign', status='inProgress', items=[dict(type='userMessage', clientId='human')]))
        self.owner.state = 'closed'
        self.tick()
        self.assertEqual(self.count('turn/interrupt'), 0)
        self.assertEqual(self.row()['status'], 'uncertain')

    def test_unsupported_close_interrupt_remains_visible_without_faking_stopped(self):
        self.schedule()
        self.rpc.auto_add = True
        self.rpc.on_add = lambda: setattr(self.owner, 'state', 'closed')
        self.rpc.interrupt_error = True
        self.now += 30
        self.tick()
        self.assertEqual(self.row()['status'], 'cancelling')
        self.assertIn('unavailable', self.row()['error'])
        self.assertEqual(self.rpc.turns[0]['status'], 'inProgress')
        self.tick()
        self.assertEqual(self.count('turn/interrupt'), 1)

    def test_child_and_noncanonical_root_binding_are_cold(self):
        self.coord.db.execute("UPDATE root_projections SET home_session='other-root'")
        self.rpc.fail()
        self.tick()
        self.assertEqual(self.state.capacity_jobs(), [])
        self.coord.db.execute("UPDATE root_projections SET home_session=session_id")
        self.coord.db.execute("UPDATE bridge_bindings SET parent_session='parent',agent_path='/root/child'")
        self.rpc.emit('turn/completed', dict(threadId=self.thread, turn=self.rpc.turns[0]))
        self.tick()
        self.assertEqual(self.state.capacity_jobs(), [])

    def test_model_effort_or_permission_drift_cancels(self):
        for key, value in (('model', 'different-model'), ('reasoningEffort', 'low'), ('approvalPolicy', 'never')):
            with self.subTest(key=key):
                original = copy.deepcopy(self.rpc.config)
                self.schedule()
                self.rpc.config[key] = value
                self.now += 30
                self.tick()
                self.assertEqual(self.row()['status'], 'cancelled')
                self.assertEqual(self.count('thread/queue/add'), 0)
                self.rpc.config = original
                self.rpc.turns.insert(0, dict(id='next-'+key, status='inProgress', items=[]))

    def test_paused_stopped_and_rebound_root_cancels(self):
        self.schedule()
        self.coord.db.execute('UPDATE root_projections SET paused=1')
        self.tick()
        self.assertEqual(self.row()['status'], 'cancelled')
        self.coord.db.execute('UPDATE root_projections SET paused=0')
        self.rpc.turns.insert(0, dict(id='next-stop', status='inProgress', items=[]))
        self.schedule()
        self.coord.db.execute("INSERT INTO agent_controls(target_session,mode,updated_at) VALUES('main','stopped',0)")
        self.tick()
        self.assertEqual(self.row()['status'], 'cancelled')
        self.coord.db.execute("UPDATE agent_controls SET mode='running'")
        self.rpc.turns.insert(0, dict(id='next-rebind', status='inProgress', items=[]))
        self.schedule()
        self.coord.db.execute('UPDATE bridge_bindings SET thread_id=?', (str(uuid.uuid4()),))
        self.tick()
        self.assertEqual(self.row()['status'], 'cancelled')

    def test_new_native_queue_and_new_turn_cancel(self):
        self.schedule()
        self.rpc.entries.append(dict(id='human-input', clientUserMessageId='human'))
        self.tick()
        self.assertEqual(self.row()['status'], 'cancelled')
        self.assertEqual(self.rpc.entries[0]['id'], 'human-input')
        self.assertEqual(self.count('thread/queue/add'), 0)
        self.rpc.entries.clear()
        self.rpc.turns.insert(0, dict(id='next', status='inProgress', items=[]))
        self.schedule()
        self.rpc.turns.insert(0, dict(id='new-input', status='completed', items=[]))
        self.tick()
        self.assertEqual(self.row()['status'], 'cancelled')

    def test_native_input_arriving_after_add_deletes_only_own_queue_item(self):
        self.schedule()
        self.rpc.on_add = lambda: self.rpc.entries.append(dict(id='human', clientUserMessageId='human'))
        self.now += 30
        self.tick()
        self.assertEqual(self.row()['status'], 'cancelled')
        self.assertEqual([entry['id'] for entry in self.rpc.entries], ['human'])
        self.assertEqual(self.count('thread/queue/start'), 0)

    def test_refused_queue_delete_retains_cancellation_until_positive_proof(self):
        self.schedule()
        self.rpc.refuse_delete = True
        self.rpc.on_add = lambda: self.rpc.entries.append(dict(id='human', clientUserMessageId='human'))
        self.now += 30
        self.tick()
        self.assertEqual(self.row()['status'], 'cancelling')
        self.assertEqual(len(self.rpc.entries), 2)
        self.rpc.refuse_delete = False
        self.tick()
        self.assertEqual(self.row()['status'], 'cancelled')
        self.assertTrue(self.row()['native_proof'].startswith('queue_deleted:'))
        self.assertEqual([entry['id'] for entry in self.rpc.entries], ['human'])

    def test_native_turn_racing_queue_start_is_not_steered(self):
        self.schedule()
        def race():
            self.rpc.status = 'active'
            self.rpc.turns.insert(0, dict(id='human-active', status='inProgress', items=[]))
        self.rpc.on_start = race
        self.now += 30
        self.tick()
        self.tick()
        self.assertEqual(self.row()['status'], 'cancelled')
        self.assertEqual(self.rpc.turns[0]['id'], 'human-active')
        self.assertEqual(self.count('turn/start'), 0)
        self.assertEqual(self.rpc.entries, [])

    def test_lost_add_and_start_acknowledgements_reconcile_without_replay(self):
        for lost in ('drop_add', 'drop_start'):
            with self.subTest(lost=lost):
                self.rpc.turns.insert(0, dict(id='outage-'+lost, status='inProgress', items=[]))
                self.schedule()
                setattr(self.rpc, lost, True)
                self.now += 30
                with self.assertRaises(TransportError): self.tick()
                self.build_bridge()
                self.tick()
                self.assertEqual(self.row()['status'], 'retrying')
                self.rpc.complete()
                self.tick()
        self.assertEqual(self.count('thread/queue/add'), 2)
        self.assertEqual(self.count('thread/queue/start'), 2)

    def test_missing_positive_rpc_evidence_is_uncertain_without_replay(self):
        self.schedule()
        self.rpc.drop_add = True
        self.now += 30
        with self.assertRaises(TransportError): self.tick()
        self.rpc.entries.clear()
        self.tick()
        self.assertEqual(self.row()['status'], 'uncertain')
        self.tick()
        self.assertEqual(self.count('thread/queue/add'), 1)

    def test_disabled_recovery_cancels_pending_and_prevents_discovery(self):
        self.schedule()
        with mock.patch.dict('os.environ', {'AGENT_CHAT_CAPACITY_RECOVERY': '0'}): self.tick()
        self.assertEqual(self.row()['status'], 'cancelled')
        self.rpc.turns.insert(0, dict(id='disabled-new', status='inProgress', items=[]))
        self.rpc.fail()
        with mock.patch.dict('os.environ', {'AGENT_CHAT_CAPACITY_RECOVERY': '0'}): self.tick()
        self.assertEqual(self.count('thread/queue/add'), 0)


class RemoteCapacityRecoveryTests(RemoteWebFixture):
    def setUp(self):
        super().setUp()
        self.call('register', agent='main')
        self.tid = str(uuid.uuid4())
        self.call('bind', thread=self.tid)
        with contextlib.closing(Coordinator(self.db, 'a')) as coord: enroll(coord, self.tid)
        self.state = RemoteBridgeState(self.client, self.url, dict(owner='capacity-test', secret='x'*40, host_id='mac'))
        self.state.call('acquire')
        self.rpc = CapacityRpc(self.tid)
        self.bridge = Bridge(self.state, self.rpc)
        self.now = 1000
        self.owner = FakeOwner()
        self.bridge.capacity_worker.clock = lambda: self.now
        self.bridge.capacity_worker.owner_monitor = self.owner
        self.bridge.tick()
        self.rpc.calls.clear()

    def pause_root(self):
        with contextlib.closing(Coordinator(self.db, 'a')) as coord:
            coord.db.execute('UPDATE root_projections SET paused=1')

    def native_handoffs(self):
        with contextlib.closing(Coordinator(self.db, 'a')) as coord:
            return [dict(row) for row in coord.db.execute('SELECT * FROM root_native_handoffs WHERE active=1')]

    def stop_before_start(self):
        original = self.state.capacity_update
        def update(job_id, expected, status, now, **values):
            result = original(job_id, expected, status, now, **values)
            if status == 'starting':
                self.pause_root()
                raise CoordError('fixture stop after persisted start admission')
            return result
        self.state.capacity_update = update
        self.rpc.fail()
        self.bridge.tick()
        self.now += 30
        with self.assertRaises(CoordError): self.bridge.tick()
        self.state.capacity_update = original

    def test_authenticated_http_recovery_and_native_handoff_settlement(self):
        self.rpc.fail()
        self.bridge.tick()
        row = self.state.capacity_jobs()[0]
        self.assertEqual((row['status'], row['model']), ('pending', 'current-model'))
        self.now += 30
        self.bridge.tick()
        self.assertEqual(self.state.capacity_jobs()[0]['status'], 'retrying')
        self.rpc.complete()
        self.bridge.tick()
        self.assertEqual(self.state.capacity_jobs(), [])
        with contextlib.closing(Coordinator(self.db, 'a')) as coord:
            self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM root_native_handoffs WHERE active=1').fetchone()[0], 0)
        self.assertEqual(self.call('bridge-status')['capacity_recoveries'][0]['status'], 'completed')

    def test_auto_dispatched_add_lost_ack_and_immediate_failure_settle_only_history_proof(self):
        self.rpc.fail()
        self.bridge.tick()
        self.rpc.auto_add = self.rpc.auto_fail = self.rpc.drop_add = True
        self.now += 30
        with self.assertRaises(TransportError): self.bridge.tick()
        self.assertEqual(self.state.capacity_jobs()[0]['status'], 'adding')
        self.assertEqual(len(self.native_handoffs()), 1)
        self.bridge.tick()
        self.assertEqual(self.state.capacity_jobs()[0]['status'], 'pending')
        self.assertEqual(self.native_handoffs(), [])
        self.assertFalse(any(method == 'thread/queue/start' for method, _ in self.rpc.calls))
        self.rpc.auto_fail = False
        self.rpc.auto_complete = True
        self.now += 60
        self.bridge.tick()
        self.assertEqual(self.call('bridge-status')['capacity_recoveries'][0]['status'], 'completed')
        self.assertEqual(self.native_handoffs(), [])

    def test_add_ack_without_queue_or_history_keeps_handoff_until_eventual_positive_proof(self):
        self.rpc.fail()
        self.bridge.tick()
        self.rpc.auto_add = True
        original = self.rpc.request
        saved = []
        def hidden_history(method, params):
            result = original(method, params)
            if method == 'thread/queue/add':
                saved.append(self.rpc.turns.pop(0))
                self.rpc.status = 'idle'
            return result
        self.rpc.request = hidden_history
        self.now += 30
        self.bridge.tick()
        self.assertEqual(self.state.capacity_jobs()[0]['status'], 'uncertain')
        self.assertEqual(len(self.native_handoffs()), 1)
        self.rpc.request = original
        self.rpc.turns.insert(0, saved[0])
        self.rpc.status = 'active'
        self.bridge.tick()
        self.assertEqual(self.state.capacity_jobs()[0]['status'], 'retrying')
        self.assertEqual(self.native_handoffs(), [])
        self.assertEqual(sum(method == 'thread/queue/add' for method, _ in self.rpc.calls), 1)

    def test_close_after_auto_dispatch_waits_for_exact_own_turn_interrupt_completion(self):
        self.rpc.fail()
        self.bridge.tick()
        self.rpc.auto_add = True
        self.rpc.on_add = lambda: setattr(self.owner, 'state', 'closed')
        self.now += 30
        self.bridge.tick()
        self.assertEqual(self.state.capacity_jobs()[0]['status'], 'cancelling')
        self.assertTrue(any(row['phase'].startswith('interrupting:') for row in self.native_handoffs()))
        self.bridge.tick()
        self.assertEqual(self.native_handoffs(), [])
        result = self.call('bridge-status')['capacity_recoveries'][0]
        self.assertEqual(result['status'], 'cancelled')
        self.assertTrue(result['native_proof'].startswith('turn_stopped:'))

    def test_managed_user_input_supersedes_pending_recovery(self):
        self.rpc.fail()
        self.bridge.tick()
        with contextlib.closing(Coordinator(self.db, self.server.sender_session)) as operator:
            operator.send('a', 'Fresh operator direction')
        self.now += 30
        self.bridge.tick()
        result = self.call('bridge-status')['capacity_recoveries'][0]
        self.assertEqual(result['status'], 'cancelled')
        payloads = [params['input'][0]['text'] for method, params in self.rpc.calls if method == 'thread/queue/add']
        self.assertNotIn(CONTINUATION, payloads)

    def test_stop_after_start_admission_before_rpc_settles_verified_queue_cancellation(self):
        self.stop_before_start()
        self.assertEqual(len(self.native_handoffs()), 1)
        self.assertEqual(self.native_handoffs()[0]['kind'], 'capacity')
        self.bridge.tick()
        self.assertEqual(self.native_handoffs(), [])
        result = self.call('bridge-status')['capacity_recoveries'][0]
        self.assertEqual(result['status'], 'cancelled')
        self.assertTrue(result['native_proof'].startswith('queue_deleted:'))

    def test_unknown_start_outcome_stays_uncertain_with_visible_native_handoff(self):
        self.stop_before_start()
        self.rpc.entries.clear()
        self.bridge.tick()
        result = self.call('bridge-status')['capacity_recoveries'][0]
        self.assertEqual(result['status'], 'uncertain')
        self.assertEqual(len(self.native_handoffs()), 1)
        self.assertEqual(self.state.capacity_jobs()[0]['status'], 'uncertain')
        self.bridge.tick()
        self.assertEqual(len(self.native_handoffs()), 1)

    def test_uncertain_cancel_reconciles_eventual_exact_queue_without_starting_it(self):
        self.stop_before_start()
        retained = self.rpc.entries.pop()
        self.bridge.tick()
        self.assertEqual(self.state.capacity_jobs()[0]['status'], 'uncertain')
        self.rpc.entries.append(retained)
        self.bridge.tick()
        self.assertEqual(self.native_handoffs(), [])
        self.assertEqual(self.rpc.entries, [])
        self.assertFalse(any(method == 'thread/queue/start' for method, _ in self.rpc.calls))
        self.assertEqual(self.call('bridge-status')['capacity_recoveries'][0]['status'], 'cancelled')

    def test_uncertain_cancel_reconciles_eventual_matching_turn_proof(self):
        self.stop_before_start()
        retained = self.rpc.entries.pop()
        self.bridge.tick()
        self.rpc.turns.insert(0, dict(id='late-native-turn', status='completed',
                                    items=[dict(type='userMessage', clientId=retained['clientUserMessageId'])]))
        self.bridge.tick()
        self.assertEqual(self.native_handoffs(), [])
        self.assertEqual(self.call('bridge-status')['capacity_recoveries'][0]['status'], 'cancelled')

    def test_lost_start_ack_then_stop_settles_observed_turn_proof(self):
        self.rpc.fail()
        self.bridge.tick()
        self.rpc.drop_start = True
        self.now += 30
        with self.assertRaises(TransportError): self.bridge.tick()
        self.pause_root()
        self.bridge.tick()
        self.assertEqual(self.native_handoffs(), [])
        result = self.call('bridge-status')['capacity_recoveries'][0]
        self.assertEqual(result['status'], 'cancelled')
        self.assertTrue(result['native_proof'].startswith('turn_observed:'))
