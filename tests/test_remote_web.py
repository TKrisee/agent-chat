import base64
import contextlib
import http.client
import json
import multiprocessing
import shutil
import sqlite3
import socketserver
import struct
import tempfile
import threading
import time
import unittest
import uuid
from unittest import mock
from pathlib import Path
from collections import deque

from agent_chat.auth import origin
from agent_chat.core import Coordinator
from agent_chat.remote import HttpClient, RemoteCoordError
from agent_chat.web import Handler, create_server
from agent_chat.inbox_diagnostics import TRACE_HEADER

TOKEN = 'test-only-token-for-remote-hosting-123456789'


def assert_persistent_scale_observation(case, *, samples, errors, rows, sink, client_count, polls_per_client,
                                        final_cleanup_deadline, observed_at):
    """Keep workload completion claims bounded by explicit synthetic-checkable facts."""
    case.assertLessEqual(observed_at, final_cleanup_deadline)
    case.assertEqual(errors, [None] * client_count)
    case.assertEqual([len(result) for result in samples], [polls_per_client] * client_count)
    case.assertTrue(all(all(finished - started < 10 for _, started, finished, _ in result) for result in samples))
    traces = {trace for result in samples for trace, _, _, _ in result}
    case.assertEqual(len(traces), client_count * polls_per_client)
    return assert_inbox_trace_observation(case, traces=traces, rows=rows, sink=sink)


def assert_inbox_trace_observation(case, *, traces, rows, sink):
    required_events = {
        ('handler', 'start'),
        *{(phase, edge) for phase in ('project', 'body', 'dispatch', 'response', 'transaction_acquire',
                                      'transaction_held', 'inbox_query') for edge in ('start', 'end')},
    }
    ended = {row['request_id'] for row in rows if (row['phase'], row['edge']) == ('response', 'end')}
    case.assertEqual(ended & traces, traces)
    traced = [row for row in rows if row['request_id'] in traces]
    case.assertTrue({'handler', 'project', 'body', 'dispatch', 'transaction_acquire', 'transaction_held',
                     'inbox_query', 'response'} <= {row['phase'] for row in traced})
    case.assertTrue(all(row['sequence'] > 0 and row.get('dropped', 0) == 0 for row in traced))
    for trace in traces:
        events = sorted((row for row in traced if row['request_id'] == trace), key=lambda row: row['sequence'])
        case.assertTrue(events)
        case.assertEqual([row['sequence'] for row in events], list(range(1, len(events) + 1)))
        positions = {}
        for index, row in enumerate(events):
            positions.setdefault((row['phase'], row['edge']), []).append(index)
        case.assertTrue(required_events <= set(positions))
        case.assertEqual((events[0]['phase'], events[0]['edge']), ('handler', 'start'))
        for phase in ('project', 'body', 'dispatch', 'response', 'transaction_acquire', 'transaction_held',
                      'inbox_query'):
            case.assertLess(positions[(phase, 'start')][0], positions[(phase, 'end')][-1])
        project_start, project_end = positions[('project', 'start')][0], positions[('project', 'end')][-1]
        body_start, body_end = positions[('body', 'start')][0], positions[('body', 'end')][-1]
        dispatch_start, dispatch_end = positions[('dispatch', 'start')][0], positions[('dispatch', 'end')][-1]
        response_start, response_end = positions[('response', 'start')][0], positions[('response', 'end')][-1]
        case.assertLess(project_start, project_end)
        case.assertLess(project_end, body_start)
        case.assertLess(body_start, body_end)
        case.assertLess(body_end, dispatch_start)
        case.assertLess(dispatch_start, dispatch_end)
        case.assertLess(dispatch_end, response_start)
        case.assertLess(response_start, response_end)
        case.assertEqual(response_end, len(events) - 1)
        for phase in ('transaction_acquire', 'transaction_held', 'inbox_query'):
            case.assertTrue(all(dispatch_start < index < dispatch_end
                                for edge in ('start', 'end') for index in positions[(phase, edge)]))
    case.assertTrue(sink._queue.empty())
    case.assertEqual(sink._dropped, 0)
    return traces


_STAGED_REPORT_HEADER = struct.Struct('!BBBBIIII')
_STAGED_REPORT_ACTOR = struct.Struct('!BBBBHHdddd')
_STAGED_REPORT_BYTES = _STAGED_REPORT_HEADER.size + 42 * _STAGED_REPORT_ACTOR.size
_STAGED_REPORT_ERRORS = (None, 'assertion-failed', 'fixture-error', 'server-closure-error')


def _encode_staged_guard_report(outcome):
    # One fixed-size frame (1700 bytes), with no exception text or raw replies.
    payload = bytearray(_STAGED_REPORT_HEADER.pack(
        outcome['status'] == 'completed', _STAGED_REPORT_ERRORS.index(outcome.get('error')),
        outcome['actors_closed'], 0, outcome.get('traces', 0), outcome.get('rows', 0),
        outcome.get('queue_remaining', 0), outcome.get('dropped', 0)))
    for actor in outcome['actors']:
        payload.extend(_STAGED_REPORT_ACTOR.pack(
            actor['admitted'], actor['closed'], 'worker-error' in actor['errors'],
            'close-error' in actor['errors'], actor['polls'], actor['pulses'],
            *(-1 if actor[key] is None else actor[key] for key in (
                'first_poll_seconds', 'first_poll_finished_seconds',
                'last_poll_finished_seconds', 'completed_seconds'))))
    return bytes(payload)


def _decode_staged_guard_report(payload):
    if len(payload) != _STAGED_REPORT_BYTES:
        raise ValueError('invalid fixture report size')
    status, error, closed, reserved, traces, rows, queued, dropped = _STAGED_REPORT_HEADER.unpack_from(payload)
    if status not in (0, 1) or closed not in (0, 1) or reserved != 0 or error >= len(_STAGED_REPORT_ERRORS):
        raise ValueError('invalid fixture report header')
    actors = []
    for index in range(42):
        admitted, actor_closed, worker_error, close_error, polls, pulses, *timings = (
            _STAGED_REPORT_ACTOR.unpack_from(payload, _STAGED_REPORT_HEADER.size + index * _STAGED_REPORT_ACTOR.size))
        if any(flag not in (0, 1) for flag in (admitted, actor_closed, worker_error, close_error)):
            raise ValueError('invalid fixture actor flags')
        actors.append(dict(
            admitted=bool(admitted), closed=bool(actor_closed), polls=polls, pulses=pulses,
            errors=(['worker-error'] if worker_error else []) + (['close-error'] if close_error else []),
            **{key: None if value == -1 else value for key, value in zip((
                'first_poll_seconds', 'first_poll_finished_seconds',
                'last_poll_finished_seconds', 'completed_seconds'), timings)}))
    return dict(status='completed' if status else 'failed', error=_STAGED_REPORT_ERRORS[error],
                actors_closed=bool(closed), clients=42, registrations=1, actors=actors,
                traces=traces, rows=rows, queue_remaining=queued, dropped=dropped)


def _run_shared_session_staged_guard_child(directory, case_started, report):
    """Only this fixture child owns the fresh DB, server and non-daemon actors."""
    case = RemoteWebTests('runTest')
    case.db = Path(directory) / 'state.sqlite3'
    case.diagnostic = Path(directory) / 'owner-only-inbox.jsonl'
    stop = threading.Event()
    workers = []
    admissions, first_polls, closeouts, completed = ([None] * 42 for _ in range(4))
    samples, pulses, errors = ([[] for _ in range(42)] for _ in range(3))
    outcome = {'status': 'failed', 'error': 'fixture-error'}
    observation_deadline, child_cleanup_deadline = case_started + 63, case_started + 110
    try:
        Coordinator(case.db).close()
        case.start(inbox_diagnostics=case.diagnostic)
        session, host = 'S0', 'H0'
        registration = case.call('register', session=session, host=host, agent='synthetic-s0')
        case.assertEqual(registration['session'], session)
        clients = [HttpClient(case.url, TOKEN, timeout=10) for _ in range(42)]
        case.assertEqual([client.timeout for client in clients], [10] * 42)
        # Observe records when produced, before rotated files can discard them.
        # At 0.5s between alternating operations, 64 inboxes/actor exceeds the
        # 63s observation window; 64 phase rows/trace also exceeds normal output.
        rows = deque(maxlen=42 * 64 * 64)
        overflow = threading.Event()
        sink = case.server.inbox_diagnostics
        original_record = sink.record

        def record(item):
            dropped = sink._dropped
            original_record(item)
            if len(rows) == rows.maxlen:
                overflow.set()
            rows.append(dict(item, dropped=max(dropped, sink._dropped)))

        def call(index, op, **params):
            return clients[index].call('/api/coord', {
                'op': op, 'session': session, 'host_id': host, 'params': params,
            })

        def actor(index):
            guard, claim = None, None
            try:
                if stop.wait(max(0, case_started + index * 1.1 - time.monotonic())):
                    return
                resource = f'file:fixture/s0-h0-actor-{index}'
                started = time.monotonic()
                context = call(index, 'context')
                claim = call(index, 'request', resource=resource, minutes=5)
                guard = call(index, 'begin-guard', resource=resource, token=claim['token'])
                attached = call(index, 'attach-guard-pid', run_id=guard['run_id'],
                                token=claim['token'], pid=100000 + index)
                admissions[index] = {
                    'resource': resource, 'context': context, 'claim': claim,
                    'guard': guard, 'attached': attached,
                    'started': started, 'finished': time.monotonic(),
                }
                inbox_next = True
                while not stop.is_set() and time.monotonic() < observation_deadline:
                    started = time.monotonic()
                    if inbox_next:
                        trace = str(uuid.uuid4())
                        first_polls[index] = first_polls[index] or started
                        reply = clients[index].call('/api/coord', {
                            'op': 'inbox', 'session': session, 'host_id': host, 'params': {},
                        }, inbox_trace_id=trace)
                        samples[index].append((trace, started, time.monotonic(), reply))
                    else:
                        reply = call(index, 'guard-pulse', run_id=guard['run_id'], token=claim['token'])
                        pulses[index].append((started, time.monotonic(), reply))
                    inbox_next = not inbox_next
                    stop.wait(.5)
            except BaseException:
                errors[index].append('worker-error')
                stop.set()
            finally:
                if guard is not None:
                    try:
                        closeouts[index] = call(index, 'close-guard', run_id=guard['run_id'],
                                                token=claim['token'], evidence_sha256='0' * 64)
                    except BaseException:
                        errors[index].append('close-error')
                        stop.set()
                completed[index] = time.monotonic()

        with mock.patch.object(sink, 'record', side_effect=record):
            workers = [threading.Thread(target=actor, args=(index,)) for index in range(42)]
            try:
                for worker in workers:
                    worker.start()
                # End observation even if a current request outlasts the cap.
                stop.wait(max(0, observation_deadline - time.monotonic()))
            finally:
                stop.set()
                for worker in workers:
                    if worker.ident is not None:
                        worker.join(max(0, child_cleanup_deadline - time.monotonic()))
            case.assertFalse(any(worker.is_alive() for worker in workers), 'actor closure unresolved')
            case.assertEqual(errors, [[] for _ in range(42)])
            traces = {trace for result in samples for trace, _, _, _ in result}
            while time.monotonic() < child_cleanup_deadline:
                ended = {row['request_id'] for row in list(rows)
                         if (row['phase'], row['edge']) == ('response', 'end')}
                if traces <= ended and sink._queue.empty():
                    break
                time.sleep(.01)
            _assert_shared_session_staged_guard_observation(
                case, admissions=admissions, samples=samples, pulses=pulses,
                first_polls=first_polls, closeouts=closeouts, completed=completed,
                rows=list(rows), sink=sink, overflow=overflow.is_set(),
                case_started=case_started, observation_deadline=observation_deadline,
                cleanup_deadline=child_cleanup_deadline)
        outcome = {
            'status': 'completed', 'clients': 42, 'registrations': 1,
            'traces': len(traces), 'rows': len(rows),
            'queue_remaining': sink._queue.qsize(), 'dropped': sink._dropped,
        }
    except BaseException as error:
        outcome = {'status': 'failed',
                   'error': 'assertion-failed' if isinstance(error, AssertionError) else 'fixture-error'}
    finally:
        stop.set()
        actors_closed = not any(worker.is_alive() for worker in workers)
        outcome['actors_closed'] = actors_closed
        outcome['actors'] = [{
            'admitted': admissions[index] is not None,
            'polls': len(samples[index]), 'pulses': len(pulses[index]),
            'first_poll_seconds': (None if first_polls[index] is None
                                   else first_polls[index] - case_started),
            'first_poll_finished_seconds': (samples[index][0][2] - case_started
                                            if samples[index] else None),
            'last_poll_finished_seconds': (samples[index][-1][2] - case_started
                                           if samples[index] else None),
            'completed_seconds': (None if completed[index] is None
                                  else completed[index] - case_started),
            'closed': closeouts[index] is not None and closeouts[index].get('closed') is True,
            'errors': errors[index],
        } for index in range(42)]
        # Never tear down server/DB under live actors. A blocked child remains
        # owned by the parent until exact termination and reaping complete.
        if actors_closed and hasattr(case, 'server'):
            try:
                case.stop()
            except BaseException:
                outcome = dict(outcome, status='failed', error='server-closure-error')
        try:
            report.send_bytes(_encode_staged_guard_report(outcome))
        finally:
            report.close()


def _assert_shared_session_staged_guard_observation(
        case, *, admissions, samples, pulses, first_polls, closeouts, completed,
        rows, sink, overflow, case_started, observation_deadline, cleanup_deadline):
    """Require actual actor coverage and outcomes; late actors have no fixed poll quota."""
    case.assertLessEqual(time.monotonic(), cleanup_deadline)
    case.assertFalse(overflow, 'bounded production-time trace observation overflowed')
    traces = set()
    for index in range(42):
        admission = admissions[index]
        case.assertIsNotNone(admission, f'actor {index} admission missing')
        case.assertIsInstance(admission['context'].get('messages'), list)
        case.assertIsInstance(admission['context'].get('resources'), list)
        claim, guard, attached = admission['claim'], admission['guard'], admission['attached']
        case.assertEqual(claim['state'], 'owned')
        case.assertEqual(claim['resource'], admission['resource'])
        case.assertTrue(claim['token'])
        case.assertTrue(claim['reservation_id'])
        case.assertEqual(guard['reservation_id'], claim['reservation_id'])
        case.assertEqual(guard['host_id'], 'H0')
        case.assertTrue(guard['run_id'])
        case.assertEqual(attached, {'run_id': guard['run_id'], 'attached': True})
        case.assertGreaterEqual(admission['started'], case_started + index * 1.1)
        case.assertTrue(samples[index], f'actor {index} inbox result missing')
        case.assertTrue(pulses[index], f'actor {index} pulse result missing')
        case.assertEqual(first_polls[index], samples[index][0][1])
        case.assertGreaterEqual(first_polls[index], admission['finished'])
        case.assertLess(first_polls[index], observation_deadline)
        case.assertIn(len(samples[index]) - len(pulses[index]), (0, 1))
        activities = []
        for trace, started, finished, reply in samples[index]:
            case.assertNotIn(trace, traces)
            traces.add(trace)
            case.assertLess(started, observation_deadline)
            case.assertGreaterEqual(finished, started)
            case.assertLess(finished - started, 10, f'actor {index} inbox reached request timeout')
            case.assertIsInstance(reply.get('messages'), list)
            activities.append((started, finished, 'inbox'))
        for started, finished, reply in pulses[index]:
            case.assertEqual(reply['state'], 'owned')
            case.assertEqual(reply['resource'], admission['resource'])
            case.assertEqual(reply['reservation_id'], claim['reservation_id'])
            case.assertIsInstance(reply['deadline'], (int, float))
            case.assertGreaterEqual(finished, started)
            case.assertLess(started, observation_deadline)
            activities.append((started, finished, 'pulse'))
        activities.sort()
        case.assertEqual([op for _, _, op in activities],
                         ['inbox' if i % 2 == 0 else 'pulse' for i in range(len(activities))])
        case.assertTrue(all(activities[i][0] >= activities[i - 1][1]
                            for i in range(1, len(activities))))
        case.assertEqual(closeouts[index], {'run_id': guard['run_id'], 'closed': True})
        case.assertGreaterEqual(completed[index], activities[-1][1])
        case.assertLessEqual(completed[index], cleanup_deadline)
    case.assertEqual({row['request_id'] for row in rows}, traces)
    case.assertTrue(all(row['outcome'] == 'ok' for row in rows))
    case.assertEqual(sink._sink_failures, 0)
    assert_inbox_trace_observation(case, traces=traces, rows=rows, sink=sink)


class SharedSessionStagedGuardTests(unittest.TestCase):
    def test_one_42_client_shared_session_staged_guard_isolated_fixture(self):
        # Fresh-state thread fixture: containment alone does not model production
        # contention, nested worker processes, or native service cadence.
        case_started = time.monotonic()
        cleanup_deadline = case_started + 120
        directory = tempfile.mkdtemp(prefix='agent-chat-staged-guard-')
        ctx = multiprocessing.get_context('spawn')
        receive, send = ctx.Pipe(duplex=False)
        child = ctx.Process(target=_run_shared_session_staged_guard_child,
                            args=(directory, case_started, send))
        forced, unresolved, started = False, False, False
        result, closure_error = None, None
        try:
            child.start()
            started = True
            send.close()
            # Drain the bounded frame while the exact child can still be writing.
            # Joining first can deadlock a child on a full result pipe.
            normal_deadline = case_started + 116
            while time.monotonic() < normal_deadline:
                if receive.poll(min(.1, max(0, normal_deadline - time.monotonic()))):
                    try:
                        result = _decode_staged_guard_report(receive.recv_bytes(_STAGED_REPORT_BYTES))
                    except (EOFError, OSError, ValueError, struct.error):
                        result = {'status': 'failed', 'error': 'invalid-or-missing-child-report'}
                    break
                if not child.is_alive() and not receive.poll():
                    break
            # A report has been consumed (or the receive phase ended without one).
            # Reserve the final seconds for exact termination and reaping.
            child.join(max(0, normal_deadline - time.monotonic()))
        finally:
            if started:
                try:
                    if child.is_alive():
                        forced = True
                        child.terminate()
                        child.join(max(0, min(2, cleanup_deadline - time.monotonic())))
                    if child.is_alive():
                        child.kill()
                        child.join(max(0, cleanup_deadline - .1 - time.monotonic()))
                    unresolved = child.is_alive()
                    if not unresolved:
                        child.join(0)
                        exitcode = child.exitcode
                        child.close()
                except BaseException:
                    unresolved = True
                    closure_error = 'child-termination-or-reaping-error'
            else:
                exitcode = None
            send.close()
            receive.close()
            self.containment = {
                'status': ('unresolved-closure' if unresolved else
                           'timeout-terminated' if forced else 'normal-exit'),
                'directory': directory if unresolved else None,
                'closure_error': closure_error,
                'elapsed_seconds': time.monotonic() - case_started,
            }
            if not unresolved:
                shutil.rmtree(directory)
        self.assertFalse(unresolved, f'unresolved child closure; preserved {directory}: {closure_error}')
        self.assertFalse(forced, f'fixture exceeded normal completion budget: {self.containment}; {result}')
        self.assertLessEqual(time.monotonic(), cleanup_deadline)
        self.assertEqual(exitcode, 0, result)
        self.assertIsNotNone(result, 'fixture child did not report an outcome')
        self.assertEqual(result['status'], 'completed', result)
        self.assertTrue(result['actors_closed'], result)
        print('PERSISTENT_SHARED_SESSION_SCALE', json.dumps(
            dict(result, containment=self.containment), sort_keys=True))



class RemoteWebFixture(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='agent-chat-http-')
        self.db = Path(self.tmp.name) / 'state.sqlite3'
        Coordinator(self.db).close()
        self.start()

    def start(self, **kwargs):
        self.server = create_server(self.db, port=0, api_token=TOKEN, **kwargs)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = 'http://127.0.0.1:' + str(self.server.server_port)
        self.client = HttpClient(self.url, TOKEN)

    def stop(self):
        self.server.stop_event.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(3)

    def tearDown(self):
        self.stop()
        self.tmp.cleanup()

    def request(self, method, path, payload=None, auth='bearer', headers=None):
        h = dict(headers or {})
        if auth == 'bearer': h['Authorization'] = 'Bearer ' + TOKEN
        if auth == 'basic': h['Authorization'] = 'Basic ' + base64.b64encode(('operator:' + TOKEN).encode()).decode()
        data = json.dumps(payload).encode() if payload is not None else None
        if data is not None: h['Content-Type'] = 'application/json'
        conn = http.client.HTTPConnection('127.0.0.1', self.server.server_port, timeout=3)
        try:
            conn.request(method, path, data, h)
            response = conn.getresponse()
            return response.status, response.read(), dict(response.getheaders())
        finally: conn.close()

    def call(self, op, session='a', host='mac', **params):
        return self.client.call('/api/coord', {'op': op, 'session': session, 'host_id': host, 'params': params})


class RemoteWebTests(RemoteWebFixture):
    def _diagnostic_fixture(self, *, session='a', host='mac', agent='alpha'):
        self.stop()
        self.diagnostic = Path(self.tmp.name) / 'owner-only-inbox.jsonl'
        self.start(inbox_diagnostics=self.diagnostic)
        self.call('register', session=session, host=host, agent=agent)

    def _traced_inbox(self, trace=None):
        trace = trace or str(uuid.uuid4())
        status, body, _ = self.request('POST', '/api/coord', {
            'op': 'inbox', 'session': 'a', 'host_id': 'mac', 'params': {},
        }, headers={TRACE_HEADER: trace})
        self.assertEqual(status, 200, body)
        return trace

    def _diagnostic_rows(self, request_id=None):
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            if self.diagnostic.exists() and self.diagnostic.read_text().count('\n') >= 12:
                rows = [json.loads(line) for line in self.diagnostic.read_text().splitlines()]
                if request_id is None or any(row['request_id'] == request_id and row['phase'] == 'response'
                                             and row['edge'] == 'end' for row in rows):
                    return rows
            time.sleep(.01)
        self.fail('traced inbox diagnostics did not drain')

    def test_opt_in_inbox_diagnostics_measures_normal_and_sqlite_wait_without_rpc_change(self):
        self._diagnostic_fixture()
        # Ordinary overlapping inbox/context traffic completes: this is not an
        # original timeout reproduction, and no barrier is used for this sample.
        threads = [threading.Thread(target=self.call, args=('context',), kwargs={'limit': 1}) for _ in range(3)]
        for thread in threads: thread.start()
        trace = self._traced_inbox()
        for thread in threads: thread.join(2)
        self.assertTrue(all(not thread.is_alive() for thread in threads))

        entered, release = threading.Event(), threading.Event()
        original_tx = Coordinator.tx
        @contextlib.contextmanager
        def barrier_tx(coord, observer=None):
            if observer:
                entered.set()
                self.assertTrue(release.wait(2))
            with original_tx(coord, observer=observer) as db:
                yield db
        result = []
        blocked_trace = str(uuid.uuid4())
        with mock.patch.object(Coordinator, 'tx', new=barrier_tx):
            worker = threading.Thread(target=lambda: result.append(self._traced_inbox(blocked_trace)))
            worker.start(); self.assertTrue(entered.wait(2))
            holder = sqlite3.connect(self.db, isolation_level=None, timeout=1)
            holder.execute('BEGIN IMMEDIATE'); release.set()
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                rows = ([json.loads(line) for line in self.diagnostic.read_text().splitlines()]
                        if self.diagnostic.exists() else [])
                if any(row['request_id'] == blocked_trace and row['phase'] == 'transaction_acquire'
                       and row['edge'] == 'start' for row in rows):
                    break
                time.sleep(.01)
            else:
                self.fail('blocked transaction acquisition did not reach the diagnostic sink')
            time.sleep(.1); holder.commit(); holder.close(); worker.join(3)
        self.assertFalse(worker.is_alive())
        self.assertEqual(len(result), 1)
        rows = self._diagnostic_rows(result[0])
        current = [row for row in rows if row['request_id'] == result[0]]
        phases = [(row['phase'], row['edge']) for row in current]
        self.assertIn(('transaction_acquire', 'start'), phases)
        self.assertIn(('transaction_acquire', 'end'), phases)
        self.assertIn(('transaction_held', 'start'), phases)
        self.assertIn(('inbox_query', 'end'), phases)
        acquire = [row for row in current if row['phase'] == 'transaction_acquire']
        self.assertGreaterEqual(acquire[-1]['elapsed_ms'] - acquire[0]['elapsed_ms'], 80)
        original = Coordinator._message_dicts
        def delayed_messages(coord, *args, **kwargs):
            time.sleep(.1)
            return original(coord, *args, **kwargs)
        with mock.patch.object(Coordinator, '_message_dicts', new=delayed_messages):
            delayed_trace = self._traced_inbox()
        rows = self._diagnostic_rows(delayed_trace)
        delayed = [row for row in rows if row['request_id'] == delayed_trace]
        held = [row for row in delayed if row['phase'] == 'transaction_held']
        self.assertGreaterEqual(held[-1]['elapsed_ms'] - held[0]['elapsed_ms'], 80)
        self.assertTrue(all('session' not in row and 'host' not in row and 'payload' not in row for row in rows))
        self.assertIn(trace, {row['request_id'] for row in rows})

    def test_disabled_or_noninbox_requests_never_create_diagnostic_records(self):
        trace = str(uuid.uuid4())
        self.call('register', agent='alpha')
        status, _, _ = self.request('POST', '/api/coord', {
            'op': 'inbox', 'session': 'a', 'host_id': 'mac', 'params': {},
        }, headers={TRACE_HEADER: trace})
        self.assertEqual(status, 200)
        self.assertFalse((Path(self.tmp.name) / 'owner-only-inbox.jsonl').exists())
        self._diagnostic_fixture()
        status, _, _ = self.request('POST', '/api/coord', {
            'op': 'status', 'session': 'a', 'host_id': 'mac', 'params': {},
        }, headers={TRACE_HEADER: trace})
        self.assertEqual(status, 200)
        time.sleep(.05)
        self.assertFalse(self.diagnostic.exists())

    def test_inbox_diagnostics_preserve_preparse_body_timing_and_swallowed_response_timeout(self):
        self._diagnostic_fixture()
        original_read_json = Handler.read_json
        def delayed_read_json(handler, *args, **kwargs):
            time.sleep(.1)
            return original_read_json(handler, *args, **kwargs)
        with mock.patch.object(Handler, 'read_json', new=delayed_read_json):
            delayed_trace = self._traced_inbox()
        rows = self._diagnostic_rows(delayed_trace)
        delayed = [row for row in rows if row['request_id'] == delayed_trace and row['phase'] == 'body']
        self.assertEqual([(row['edge'], row['outcome']) for row in delayed], [('start', 'ok'), ('end', 'ok')])
        self.assertGreaterEqual(delayed[-1]['elapsed_ms'] - delayed[0]['elapsed_ms'], 80)

        trace = str(uuid.uuid4())
        original_write = socketserver._SocketWriter.write
        def timeout_inbox_body(writer, data):
            if data.startswith(b'{"messages"'):
                raise TimeoutError
            return original_write(writer, data)
        with mock.patch.object(socketserver._SocketWriter, 'write', new=timeout_inbox_body):
            conn = http.client.HTTPConnection('127.0.0.1', self.server.server_port, timeout=3)
            try:
                conn.request('POST', '/api/coord', json.dumps({
                    'op': 'inbox', 'session': 'a', 'host_id': 'mac', 'params': {},
                }).encode(), {'Authorization': 'Bearer ' + TOKEN, 'Content-Type': 'application/json', TRACE_HEADER: trace})
                response = conn.getresponse()
                self.assertEqual(response.status, 200)
                with self.assertRaises(http.client.IncompleteRead):
                    response.read()
            finally:
                conn.close()
        rows = self._diagnostic_rows(trace)
        response = [row for row in rows if row['request_id'] == trace and row['phase'] == 'response']
        self.assertEqual([(row['edge'], row['outcome']) for row in response], [('start', 'ok'), ('end', 'error')])
        self._traced_inbox()

    def test_two_persistent_clients_poll_traced_inbox_with_bounded_cleanup(self):
        self._diagnostic_fixture()
        self.call('register', session='b', agent='beta')
        clients = [HttpClient(self.url, TOKEN, timeout=10), HttpClient(self.url, TOKEN, timeout=10)]
        self.assertEqual([client.timeout for client in clients], [10, 10])
        poll_deadline = time.monotonic() + 20
        cleanup_deadline = poll_deadline + 20
        stop = threading.Event()
        samples = [None, None]

        def poll(index, session):
            result = []
            next_poll = time.monotonic()
            while not stop.is_set() and time.monotonic() < poll_deadline:
                trace = str(uuid.uuid4())
                started = time.monotonic()
                reply = clients[index].call('/api/coord', {
                    'op': 'inbox', 'session': session, 'host_id': 'mac', 'params': {},
                }, inbox_trace_id=trace)
                result.append((trace, started, time.monotonic(), reply))
                next_poll += 1
                stop.wait(max(0, next_poll - time.monotonic()))
            samples[index] = result

        workers = [threading.Thread(target=poll, args=(0, 'a')), threading.Thread(target=poll, args=(1, 'b'))]
        for worker in workers: worker.start()
        try:
            for worker in workers:
                worker.join(max(0, cleanup_deadline - time.monotonic()))
        finally:
            stop.set()
            for worker in workers:
                worker.join(max(0, cleanup_deadline - time.monotonic()))
        self.assertFalse(any(worker.is_alive() for worker in workers))
        self.assertLessEqual(time.monotonic(), cleanup_deadline)
        self.assertTrue(all(samples))
        self.assertTrue(all(len(result) >= 20 for result in samples))
        self.assertTrue(all(all(finished - started < 10 for _, started, finished, _ in result) for result in samples))
        traces = {trace for result in samples for trace, _, _, _ in result}
        rows = self._diagnostic_rows()
        traced = [row for row in rows if row['request_id'] in traces]
        phases = {(row['phase'], row['edge']) for row in traced}
        self.assertTrue({'handler', 'project', 'body', 'dispatch', 'transaction_acquire', 'transaction_held',
                         'inbox_query', 'response'} <= {phase for phase, _ in phases})
        self.assertEqual({row['request_id'] for row in traced if (row['phase'], row['edge']) == ('response', 'end')}, traces)

    def test_all_ui_and_data_routes_require_auth(self):
        for path in ('/', '/app.js', '/api/config', '/api/snapshot', '/api/events', '/api/attachments/fake'):
            status, body, headers = self.request('GET', path, auth=None)
            self.assertEqual(status, 401, path)
            self.assertIn('Basic', headers['WWW-Authenticate'])
        self.assertEqual(self.request('GET', '/', auth='basic')[0], 200)
        status, _, headers = self.request('POST', '/api/coord', {}, auth='basic')
        self.assertEqual(status, 401)
        self.assertIn('Bearer', headers['WWW-Authenticate'])

    def test_proxy_host_and_origin_are_exact_and_default_ports_normalized(self):
        self.stop()
        self.start(host='0.0.0.0', public_url='https://chat.example.test:443')
        allowed = {'Host': 'chat.example.test', 'Origin': 'https://chat.example.test'}
        self.assertEqual(self.request('GET', '/api/config', headers=allowed)[0], 200)
        self.assertEqual(self.request('GET', '/api/config', headers={**allowed, 'Host': 'evil.test'})[0], 403)
        self.assertEqual(self.request('GET', '/api/config', headers={**allowed, 'Origin': 'https://evil.test'})[0], 403)
        self.assertEqual(origin('http://EXAMPLE.test:80/'), 'http://example.test')

    def test_nonlocal_bind_requires_token_and_public_origin(self):
        for options in ({'host': '0.0.0.0'}, {'host': '0.0.0.0', 'api_token': TOKEN}):
            with self.assertRaises(ValueError):
                create_server(self.db, port=0, **options)

    def test_remote_cli_data_reply_image_and_explicit_ack(self):
        self.call('register', agent='alpha')
        self.call('register', session='b', agent='beta')
        msg = self.call('send', to='b', body='Question')
        image = b'\x89PNG\r\n\x1a\n' + b'fixture'
        reply = self.call('send', session='b', to='a', body='Answer', reply_to=msg['id'], attachments=[{'name': 'answer.png', 'content_base64': base64.b64encode(image).decode()}])
        inbox = self.call('inbox')['messages']
        self.assertEqual(inbox[0]['reply_to'], msg['id'])
        self.assertIsNone(inbox[0]['acked_at'])
        attachment = reply['attachments'][0]
        self.assertEqual(self.request('GET', attachment['url'])[1], image)
        self.assertTrue(self.call('acknowledge', id=reply['id'])['acknowledged'])
        self.assertEqual(self.call('inbox')['messages'], [])
        with self.assertRaises(RemoteCoordError):
            self.call('status', host='another-mac')

    def test_agent_document_batch_downloads_and_rejects_unsupported_files(self):
        self.call('register', agent='alpha')
        self.call('register', session='b', agent='beta')
        documents = [('NOTES.TXT', b'\xef\xbb\xbfUnicode: \xc3\xa1\n'), ('guide.md', b'# Guide'),
                     ('data.json', b'{"test": true}'), ('data.xml', b'<note>data</note>')]
        sent = self.call('send', to='b', body='', attachments=[
            {'name': name, 'content_base64': base64.b64encode(content).decode()}
            for name, content in documents
        ])
        for attachment, (name, content) in zip(sent['attachments'], documents):
            self.assertEqual(attachment['name'], name)
            status, downloaded, headers = self.request('GET', attachment['url'])
            self.assertEqual((status, downloaded), (200, content))
            self.assertTrue(headers['Content-Disposition'].startswith('attachment;'))
            self.assertEqual(self.request('GET', attachment['url'], auth=None)[0], 401)
        with self.assertRaises(RemoteCoordError):
            self.call('send', to='b', body='', attachments=[
                {'name': 'program.exe', 'content_base64': base64.b64encode(b'\x89PNG\r\n\x1a\n').decode()}
            ])
        self.assertEqual(len(self.call('inbox', session='b')['messages']), 1)

    def test_fifty_agent_files_and_base64_batch_above_old_request_limit(self):
        self.call('register', agent='alpha')
        self.call('register', session='b', agent='beta')
        files = [{'name': f'file-{i}.txt', 'content_base64': base64.b64encode(b'valid text').decode()}
                 for i in range(50)]
        sent = self.call('send', to='b', body='', attachments=files)
        self.assertEqual(len(sent['attachments']), 50)
        with self.assertRaises(RemoteCoordError):
            self.call('send', to='b', body='', attachments=files + files[:1])
        self.assertEqual(len(self.call('inbox', session='b')['messages']), 1)
        image = b'\x89PNG\r\n\x1a\n' + b'x' * (9 * 1024 * 1024 - 8)
        encoded = base64.b64encode(image).decode()
        sent = self.call('send', to='b', body='', attachments=[
            {'name': f'large-{i}.png', 'content_base64': encoded} for i in range(7)])
        self.assertEqual(len(sent['attachments']), 7)
        self.assertEqual(self.request('GET', sent['attachments'][0]['url'])[1], image)

    def test_remote_video_upload_preserves_bytes_and_range_requires_auth(self):
        self.call('register', agent='alpha')
        self.call('register', session='b', agent='beta')
        fixtures = Path(__file__).parent / 'fixtures'
        clips = [(fixtures / f'attachment.{ext}').read_bytes() for ext in ('mp4', 'mov', 'webm')]
        sent = self.call('send', to='b', body='', attachments=[
            {'name': f'clip.{ext}', 'content_base64': base64.b64encode(content).decode()}
            for ext, content in zip(('mp4', 'mov', 'webm'), clips)])
        self.assertEqual([a['mime'] for a in sent['attachments']], ['video/mp4', 'video/quicktime', 'video/webm'])
        for attachment, content in zip(sent['attachments'], clips):
            self.assertEqual(self.request('GET', attachment['url'])[1], content)
            self.assertEqual(self.request('GET', attachment['url'], headers={'Range': 'bytes=0-15'})[:2],
                             (206, content[:16]))
            self.assertEqual(self.request('GET', attachment['url'], auth=None, headers={'Range': 'bytes=0-15'})[0], 401)

    def test_csrf_required_for_removal_and_operator_is_preserved(self):
        self.call('register', agent='alpha')
        self.assertEqual(self.request('POST', '/api/sessions/remove', {'id': 'a'})[0], 403)
        config = json.loads(self.request('GET', '/api/config')[1])
        headers = {'X-Agent-Chat-CSRF': config['csrf_token']}
        self.assertEqual(self.request('POST', '/api/sessions/remove', {'id': config['sender']['id']}, headers=headers)[0], 400)
        self.assertEqual(self.request('POST', '/api/sessions/remove', {'id': 'a'}, headers=headers)[0], 200)
        self.assertEqual(self.request('POST', '/api/sessions/remove', [], headers=headers)[0], 400)

    def test_large_image_without_caption_and_reply_preview(self):
        self.call('register', agent='alpha')
        self.call('register', session='b', agent='beta')
        question = self.call('send', to='b', body='Screenshot please')
        image = b'\x89PNG\r\n\x1a\n' + b'fixture' * 450000
        reply = self.call('send', session='b', to='a', body='', reply_to=question['id'], attachments=[{'name':'large.png','content_base64':base64.b64encode(image).decode()}])
        self.assertEqual(reply['reply_preview']['body'], 'Screenshot please')
        self.assertEqual(self.request('GET', reply['attachments'][0]['url'])[1], image)

    def test_invalid_machine_request_does_not_break_server_or_expose_files(self):
        for payload in ({}, [], {'op':'execute', 'session':'a', 'host_id':'mac','params':{}}):
            self.assertEqual(self.request('POST', '/api/coord', payload)[0], 400)
        self.assertEqual(self.request('GET', '/api/config')[0], 200)
        for endpoint in ('http://remote.example:8765', 'https://user:pass@example.test', 'https://example.test?q=token'):
            with self.assertRaises(RemoteCoordError): HttpClient(endpoint, TOKEN)


class PersistentWorkloadAssertionTests(unittest.TestCase):
    def _facts(self):
        traces = [str(uuid.uuid4()), str(uuid.uuid4())]
        samples = [[(trace, 1.0, 1.1, {})] for trace in traces]
        events = (('handler', 'start'), ('project', 'start'), ('project', 'end'), ('body', 'start'), ('body', 'end'),
                  ('dispatch', 'start'), ('transaction_acquire', 'start'), ('transaction_acquire', 'end'),
                  ('transaction_held', 'start'), ('inbox_query', 'start'), ('inbox_query', 'end'),
                  ('transaction_held', 'end'), ('dispatch', 'end'), ('response', 'start'), ('response', 'end'))
        rows = [{'request_id': trace, 'phase': phase, 'edge': edge, 'sequence': sequence}
                for trace in traces for sequence, (phase, edge) in enumerate(events, 1)]
        sink = mock.Mock()
        sink._queue.empty.return_value = True
        sink._dropped = 0
        return samples, rows, sink

    def _assert_facts(self, samples, rows, sink, *, errors=None, observed_at=1.0):
        return assert_persistent_scale_observation(
            self, samples=samples, errors=[None, None] if errors is None else errors, rows=rows, sink=sink,
            client_count=2, polls_per_client=1, final_cleanup_deadline=.5, observed_at=observed_at)

    def test_persistent_workload_assertions_reject_deadline_count_trace_queue_and_drop_violations(self):
        samples, rows, sink = self._facts()
        with self.assertRaises(AssertionError): self._assert_facts(samples, rows, sink)
        self._assert_facts(samples, rows, sink, observed_at=.5)
        with self.assertRaises(AssertionError): self._assert_facts(samples[:1], rows, sink, observed_at=.5)
        duplicate = [[(samples[0][0][0], 1.0, 1.1, {})], [(samples[0][0][0], 1.0, 1.1, {})]]
        with self.assertRaises(AssertionError): self._assert_facts(duplicate, rows, sink, observed_at=.5)
        sink._queue.empty.return_value = False
        with self.assertRaises(AssertionError): self._assert_facts(samples, rows, sink, observed_at=.5)
        sink._queue.empty.return_value = True
        sink._dropped = 1
        with self.assertRaises(AssertionError): self._assert_facts(samples, rows, sink, observed_at=.5)

    def test_persistent_workload_assertions_reject_response_phase_error_timeout_sequence_and_emitted_drop(self):
        samples, rows, sink = self._facts()
        response_missing = [dict(row, edge='start') if row['phase'] == 'response' else row for row in rows]
        with self.assertRaises(AssertionError): self._assert_facts(samples, response_missing, sink, observed_at=.5)
        phase_missing = [row for row in rows if row['phase'] != 'inbox_query']
        with self.assertRaises(AssertionError): self._assert_facts(samples, phase_missing, sink, observed_at=.5)
        with self.assertRaises(AssertionError): self._assert_facts(samples, rows, sink, errors=['worker-error', None], observed_at=.5)
        timed_out = [[(trace, started, started + 10, reply) for trace, started, _, reply in result] for result in samples]
        with self.assertRaises(AssertionError): self._assert_facts(timed_out, rows, sink, observed_at=.5)
        nonpositive_sequence = [dict(row, sequence=0) if index == 0 else row for index, row in enumerate(rows)]
        with self.assertRaises(AssertionError): self._assert_facts(samples, nonpositive_sequence, sink, observed_at=.5)
        emitted_drop = [dict(row, dropped=1) if index == 0 else row for index, row in enumerate(rows)]
        with self.assertRaises(AssertionError): self._assert_facts(samples, emitted_drop, sink, observed_at=.5)
        out_of_order = [dict(row, phase='body') if row['sequence'] == 2 else
                        dict(row, phase='project') if row['sequence'] == 4 else row for row in rows]
        with self.assertRaises(AssertionError): self._assert_facts(samples, out_of_order, sink, observed_at=.5)
        inbox_end_before_start = [
            dict(row, edge='end') if row['phase'] == 'inbox_query' and row['sequence'] == 10 else
            dict(row, edge='start') if row['phase'] == 'inbox_query' and row['sequence'] == 11 else row
            for row in rows
        ]
        with self.assertRaises(AssertionError): self._assert_facts(samples, inbox_end_before_start, sink, observed_at=.5)
