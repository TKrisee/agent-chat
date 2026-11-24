"""Disposable Root-local to parked binding publication with a native RPC fixture."""
import contextlib
from pathlib import Path
import sqlite3
import time
import unittest
import uuid
from unittest import mock

from agent_chat.core import CoordError, Coordinator, execution_epoch_handoff, execution_epoch_schema
from agent_chat.parked_binding_repair import _unsettled_worker_epoch
from agent_chat.remote import HttpClient
from agent_chat.root_lifecycle import released_parked, stopped
from agent_chat.root_team_reset import quarantined
from agent_chat.root_team_reset import handoff_begin
from agent_chat.rpc import RpcError
from agent_chat.session_reset import SessionResetState
import test_parked_provision_http as provision_fixture
import test_remote_web as web_fixture


class ParkedBindingRepairTests(provision_fixture.ParkedProvisionHttpTests):
    def test_same_registration_gets_one_durable_native_binding_under_original_stop(self):
        project, route, path, params, _, bridge = self.prepare_parked()
        self.root('direct-create', **params)
        original = self.rpc.request
        named = {}

        def provider(method, values):
            if method == 'model/list':
                return {'data': [{'model': model, 'supportedReasoningEfforts': [{'reasoningEffort': 'high'}]}
                                 for model in ('gpt-6.2-sol', 'gpt-6.1-terra')]}
            if method == 'thread/name/set':
                thread = values['threadId']
                rollout = Path(self.workspace) / (thread + '.rollout')
                rollout.write_bytes(b'fixture metadata without input')
                named[thread] = (values['name'], rollout)
                return {}
            result = original(method, values)
            if method == 'thread/read' and values['threadId'] in named:
                name, rollout = named[values['threadId']]
                result['thread'].update(name=name, path=str(rollout))
            return result

        with mock.patch.object(self.rpc, 'request', side_effect=provider):
            bridge.tick()
            self.root('turnover-bootstrap', project=project, request_id=params['request_id'], confirm=True)
            bridge.tick()
            with contextlib.closing(Coordinator(path, route['session'])) as coord:
                po = SessionResetState(coord).job(params['request_id'])
            client = HttpClient(self.url, web_fixture.TOKEN, project=project)
            client.call('/api/coord', dict(op='context', session=po['target_session'], host_id='mac', params={}))
            def create_worker(name):
                old_request = str(uuid.uuid4())
                child = client.call('/api/coord', dict(op='parked-agent-create', session=po['target_session'],
                    host_id='mac', params=dict(agent=name, expected_thread=po['new_thread_id'],
                    prompt='Retained worker role', request_id=old_request, bootstrap_id=params['request_id'],
                    confirm=True, model='gpt-6.1-terra', reasoning_effort='high', cwd=str(self.workspace))))
                bridge.tick()
                return child['target_session'], old_request

            target, old_request = create_worker('parked-fixture-worker')
            unknown_target, unknown_request = create_worker('parked-fixture-unknown')
            metadata_target, metadata_request = create_worker('parked-fixture-metadata')
        with contextlib.closing(Coordinator(path, route['session'])) as coord:
            worker = SessionResetState(coord).job(old_request)
            self.assertEqual(worker['status'], 'parked', worker['error'])
            old_thread = worker['new_thread_id']
            self.assertEqual(self.rpc.history[old_thread], [])
            self.assertEqual(self.rpc.queues[old_thread], [])
            originals = self.identity_rows(coord.db, target, old_request)
            unknown_worker = SessionResetState(coord).job(unknown_request)
            self.assertEqual(unknown_worker['status'], 'parked')
            unknown_thread = unknown_worker['new_thread_id']
            unknown_originals = self.identity_rows(coord.db, unknown_target, unknown_request)
            metadata_worker = SessionResetState(coord).job(metadata_request)
            self.assertEqual(metadata_worker['status'], 'parked')
            metadata_thread = metadata_worker['new_thread_id']
        with contextlib.closing(Coordinator(path, route['session'])) as coord:
            handoff_begin(coord.db, 'reset', {'id': str(uuid.uuid4()), 'target_session': 'unrelated-worker',
                                            'old_thread_id': po['new_thread_id']}, 'mac', 'creating')
        with contextlib.closing(Coordinator(path, route['session'])) as coord, coord.tx() as db:
            detached_epoch = execution_epoch_handoff(db, 'thread/resume', po['new_thread_id'], target, 'mac')
            detached_row = tuple(db.execute('SELECT * FROM execution_epoch_handoffs WHERE id=?',
                                           (detached_epoch,)).fetchone())
        missing = (old_thread, unknown_thread, metadata_thread)
        for thread in missing:
            named[thread][1].unlink()
        args = dict(project=project, stop_id=params['parked_stop'], target=target, expected_thread=old_thread,
                    expected_host='mac', original_request_id=old_request, request_id=str(uuid.uuid4()), confirm=True)
        calls = []
        drift = False
        lose_start_response = False

        def native(method, payload):
            calls.append((method, dict(payload)))
            thread = payload.get('threadId')
            if thread in missing and method == 'thread/read':
                raise RpcError(-32600, f'thread not loaded: {thread}')
            if thread in missing and method == 'thread/resume':
                raise RpcError(-32600, f'no rollout found for thread id {thread}')
            if method in ('turn/start', 'turn/steer', 'thread/queue/add', 'thread/queue/start'):
                self.fail('binding repair must not submit model input')
            if method == 'thread/name/set' and payload['name'] == 'parked-fixture-metadata':
                provider(method, payload)
                raise TimeoutError('fixture lost the native name response')
            if method == 'thread/name/set':
                self.assertEqual(payload['name'], 'parked-fixture-worker')
                self.assertEqual(self.rpc.history[thread], [])
                self.assertEqual(self.rpc.queues[thread], [])
                with contextlib.closing(Coordinator(path, route['session'])) as coord:
                    db = coord.db
                    binding = db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?',
                                         (target,)).fetchone()
                    pending = db.execute('''SELECT status,new_thread,metadata_epoch_id
                        FROM root_parked_binding_repairs WHERE request_id=?''', (args['request_id'],)).fetchone()
                    self.assertEqual(binding['thread_id'], old_thread)
                    self.assertEqual(tuple(pending)[:2], ('materializing', thread))
                    handoff = db.execute('SELECT method,settled_at FROM execution_epoch_handoffs WHERE id=?',
                                         (pending['metadata_epoch_id'],)).fetchone()
                    self.assertEqual(tuple(handoff), ('thread/name/set', None))
                    self.assertTrue(stopped(db, target))
            response = provider(method, payload)
            if lose_start_response and method == 'thread/start':
                raise TimeoutError('fixture lost the native start response')
            if drift and thread == po['new_thread_id'] and method == 'thread/resume':
                response['modelProvider'] = 'changed-provider-fixture'
            return response

        with contextlib.ExitStack() as stack:
            stack.enter_context(mock.patch('agent_chat.provider_service.configured',
                return_value=(b'fixture-wrapper', ['--codex-server', 'ws://127.0.0.1:1'])))
            stack.enter_context(mock.patch('agent_chat.provider_service.selected_listener', return_value='fixture-listener'))
            rpc = stack.enter_context(mock.patch('agent_chat.rpc.RpcClient'))
            rpc.return_value.__enter__.return_value.request.side_effect = native
            drift = True
            with self.assertRaisesRegex(CoordError, 'cannot reproduce the original parked settings digest'):
                self.root('repair-parked-binding', **args)
            self.assertNotIn('thread/start', [method for method, _ in calls])
            drift = False
            receipt = self.root('repair-parked-binding', **args)
            self.assertEqual(receipt['status'], 'published')
            self.assertEqual(receipt['rollout_path'], str(named[receipt['new_thread']][1]))
            self.assertEqual(receipt['settings_digest'], worker['settings_digest'])
            self.assertEqual(sum(method == 'thread/name/set' for method, _ in calls), 1)
            accepted_calls = list(calls)
            self.assertEqual(receipt, self.root('repair-parked-binding', **args))
            with self.assertRaisesRegex(CoordError, 'different exact request'):
                self.root('repair-parked-binding', **dict(args, request_id=str(uuid.uuid4())))
            self.assertEqual(accepted_calls, calls)
            self.assertEqual(sum(method == 'thread/start' for method, _ in calls), 1)

            unknown_args = dict(args, target=unknown_target, expected_thread=unknown_thread,
                                original_request_id=unknown_request, request_id=str(uuid.uuid4()))
            lose_start_response = True
            with self.assertRaisesRegex(TimeoutError, 'fixture lost the native start response'):
                self.root('repair-parked-binding', **unknown_args)
            accepted_calls = list(calls)
            unknown = self.root('repair-parked-binding', **unknown_args)
            self.assertEqual((unknown['status'], unknown['native_outcome'], unknown['new_thread']),
                             ('creating', 'unknown', None))
            with self.assertRaisesRegex(CoordError, 'different exact request'):
                self.root('repair-parked-binding', **dict(unknown_args, request_id=str(uuid.uuid4())))
            self.assertEqual(accepted_calls, calls)
            self.assertEqual(sum(method == 'thread/start' for method, _ in calls), 2)

            metadata_args = dict(args, target=metadata_target, expected_thread=metadata_thread,
                                 original_request_id=metadata_request, request_id=str(uuid.uuid4()))
            lose_start_response = False
            with self.assertRaisesRegex(TimeoutError, 'fixture lost the native name response'):
                self.root('repair-parked-binding', **metadata_args)
            accepted_calls = list(calls)
            unverified = self.root('repair-parked-binding', **metadata_args)
            self.assertEqual((unverified['status'], unverified['metadata_outcome'], unverified['rollout_path']),
                             ('materializing', 'readback_required', None))
            self.assertTrue(named[unverified['new_thread']][1].is_file())
            self.assertEqual(accepted_calls, calls)
            self.assertEqual(sum(method == 'thread/name/set' for method, _ in calls), 2)

        with contextlib.closing(Coordinator(path, route['session'])) as coord:
            db = coord.db
            self.assertEqual(tuple(db.execute('SELECT * FROM execution_epoch_handoffs WHERE id=?',
                                               (detached_epoch,)).fetchone()), detached_row)
            self.assertEqual(self.identity_rows(db, target, old_request), originals)
            self.assertEqual(self.identity_rows(db, unknown_target, unknown_request), unknown_originals)
            unchanged = db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?',
                                   (unknown_target,)).fetchone()
            self.assertEqual(unchanged['thread_id'], unknown_thread)
            unchanged_metadata = db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?',
                                            (metadata_target,)).fetchone()
            self.assertEqual(unchanged_metadata['thread_id'], metadata_thread)
            current = db.execute('SELECT thread_id,parent_session FROM bridge_bindings WHERE session_id=?', (target,)).fetchone()
            self.assertEqual(tuple(current), (receipt['new_thread'], None))
            self.assertEqual(receipt['old_thread'], old_thread)
            stop = db.execute('SELECT active,resumed_at FROM root_team_stops WHERE id=?',
                              (params['parked_stop'],)).fetchone()
            self.assertEqual(tuple(stop), (1, None))
            self.assertTrue(stopped(db, target))
            # Inspect the existing release predicate without changing the fixture's stop.
            db.execute('SAVEPOINT check_release')
            try:
                db.execute('UPDATE root_team_stops SET active=0,resumed_at=? WHERE id=?',
                           (time.time(), params['parked_stop']))
                self.assertTrue(released_parked(db, target))
                self.assertFalse(released_parked(db, unknown_target))
                self.assertFalse(released_parked(db, metadata_target))
            finally:
                db.execute('ROLLBACK TO check_release')
                db.execute('RELEASE check_release')

        def retire_args(worker, thread, repair, candidate):
            return dict(project=project, bootstrap_id=params['request_id'], stop_id=params['parked_stop'],
                        po=po['target_session'], expected_thread=po['new_thread_id'], expected_host='mac',
                        request_id=worker['id'], target=worker['target_session'], expected_agent=worker['target_agent'],
                        logical_owner='fixture-owner', expected_bound_thread=thread, repair_request_id=repair,
                        expected_old_rollout=str(Path(self.workspace) / f'rollout-fixture-{thread}.jsonl'),
                        expected_candidate=candidate, confirm=True)

        candidate = unverified['new_thread']
        bound_args = retire_args(metadata_worker, metadata_thread, metadata_args['request_id'], candidate)
        unknown_retire_args = retire_args(unknown_worker, unknown_thread, unknown_args['request_id'], 'none')
        with contextlib.closing(Coordinator(path, route['session'])) as coord:
            db = coord.db
            preserved_metadata = self.identity_rows(db, metadata_target, metadata_request)[1:]
            repair_rows = [tuple(row) for row in db.execute('SELECT * FROM root_parked_binding_repairs ORDER BY request_id')]
            epochs = [tuple(row) for row in db.execute('SELECT * FROM execution_epoch_handoffs ORDER BY id')]

        with mock.patch('agent_chat.rpc.RpcClient') as no_provider:
            for changed in ({'expected_candidate': str(uuid.uuid4())}, {'expected_bound_thread': old_thread},
                            {'target': po['target_session']}, {'expected_agent': 'other'}, {'confirm': False}):
                with self.subTest(changed=changed), self.assertRaises(CoordError):
                    self.root('retire-bound-parked', **dict(bound_args, **changed))
            with self.assertRaises(CoordError):
                self.root('retire-bound-parked', **retire_args(worker, old_thread, args['request_id'], 'none'))
            original_path = Path(bound_args['expected_old_rollout'])
            original_path.write_bytes(b'original rollout reappeared')
            with self.assertRaisesRegex(CoordError, 'rollout exists'):
                self.root('retire-bound-parked', **bound_args)
            original_path.unlink()
            with contextlib.closing(Coordinator(path, route['session'])) as coord, coord.tx() as db:
                db.execute('INSERT INTO bridge_bindings VALUES(?,?,NULL,NULL,?)',
                           ('unrelated-fixture-route', candidate, time.time()))
            try:
                with self.assertRaisesRegex(CoordError, 'candidate is bound elsewhere'):
                    self.root('retire-bound-parked', **bound_args)
            finally:
                with contextlib.closing(Coordinator(path, route['session'])) as coord, coord.tx() as db:
                    db.execute('DELETE FROM bridge_bindings WHERE session_id=?', ('unrelated-fixture-route',))

            retired = self.root('retire-bound-parked', **bound_args)
            self.assertEqual(retired['retained_candidate'], candidate)
            self.assertEqual(retired['repair_status'], 'materializing')
            self.assertFalse(retired['native_closure_proven'])
            self.assertEqual(retired, self.root('retire-bound-parked', **bound_args))
            with self.assertRaisesRegex(CoordError, 'retry scope differs'):
                self.root('retire-bound-parked', **dict(bound_args, logical_owner='different-owner'))
            orphan = self.root('retire-bound-parked', **unknown_retire_args)
            self.assertIsNone(orphan['retained_candidate'])
            self.assertEqual(orphan['repair_status'], 'creating')
            no_provider.assert_not_called()

        with contextlib.closing(Coordinator(path, route['session'])) as coord:
            db = coord.db
            for session, thread, original in ((metadata_target, metadata_thread, metadata_request),
                                               (unknown_target, unknown_thread, unknown_request)):
                self.assertIsNone(db.execute('SELECT 1 FROM sessions WHERE id=?', (session,)).fetchone())
                self.assertIsNone(db.execute('SELECT 1 FROM bridge_bindings WHERE session_id=?', (session,)).fetchone())
                self.assertIsNotNone(db.execute('SELECT 1 FROM retired_sessions WHERE id=?', (session,)).fetchone())
                self.assertTrue(quarantined(db, session=session, job=original))
                self.assertTrue(quarantined(db, thread=thread))
            self.assertTrue(quarantined(db, thread=candidate))
            self.assertEqual(self.identity_rows(db, target, old_request), originals)
            self.assertEqual(tuple(db.execute('SELECT active,resumed_at FROM root_team_stops WHERE id=?',
                                               (params['parked_stop'],)).fetchone()), (1, None))
            for session, original, preserved in ((metadata_target, metadata_request, preserved_metadata),
                                                  (unknown_target, unknown_request, unknown_originals[1:])):
                for query, value, saved in (
                    ('SELECT * FROM agent_management WHERE target_session=?', session, preserved[0]),
                    ('SELECT * FROM remote_session_hosts WHERE session_id=?', session, preserved[1]),
                    ('SELECT * FROM session_resets WHERE id=?', original, preserved[2]),
                    ('SELECT * FROM root_turnover_subordinates WHERE request_id=?', original, preserved[3]),
                ):
                    self.assertEqual(tuple(db.execute(query, (value,)).fetchone()), saved)
            self.assertEqual([tuple(row) for row in db.execute('SELECT * FROM root_parked_binding_repairs ORDER BY request_id')],
                             repair_rows)
            self.assertEqual([tuple(row) for row in db.execute('SELECT * FROM execution_epoch_handoffs ORDER BY id')], epochs)
            self.assertTrue(named[candidate][1].is_file())

    @staticmethod
    def identity_rows(db, target, original):
        return [tuple(db.execute(query, (value,)).fetchone()) for query, value in (
            ('SELECT * FROM sessions WHERE id=?', target),
            ('SELECT * FROM agent_management WHERE target_session=?', target),
            ('SELECT * FROM remote_session_hosts WHERE session_id=?', target),
            ('SELECT * FROM session_resets WHERE id=?', original),
            ('SELECT * FROM root_turnover_subordinates WHERE request_id=?', original),
        )]


class ParkedBindingEpochTests(unittest.TestCase):
    def test_detached_settings_template_resume_does_not_hide_worker_effects(self):
        target, old_thread, template, host = 'worker', 'old-worker-thread', 'po-template-thread', 'mac'
        with contextlib.closing(sqlite3.connect(':memory:')) as db:
            execution_epoch_schema(db)
            detached = execution_epoch_handoff(db, 'thread/resume', template, target, host)
            saved = db.execute('SELECT * FROM execution_epoch_handoffs WHERE id=?', (detached,)).fetchone()
            self.assertFalse(_unsettled_worker_epoch(db, target, old_thread, template, host))
            for method, thread, actor, epoch_host in (
                ('turn/start', template, target, host),
                ('thread/start', None, target, host),
                ('thread/resume', old_thread, target, host),
                ('thread/resume', old_thread, 'another-actor', host),
                ('thread/resume', 'unrelated-thread', target, host),
                ('thread/resume', template, target, 'another-host'),
                ('thread/resume', None, target, host),
            ):
                with self.subTest(method=method, thread=thread, actor=actor, host=epoch_host):
                    db.execute('SAVEPOINT worker_epoch')
                    try:
                        execution_epoch_handoff(db, method, thread, actor, epoch_host)
                        self.assertTrue(_unsettled_worker_epoch(db, target, old_thread, template, host))
                    finally:
                        db.execute('ROLLBACK TO worker_epoch')
                        db.execute('RELEASE worker_epoch')
            self.assertEqual(db.execute('SELECT * FROM execution_epoch_handoffs WHERE id=?',
                                        (detached,)).fetchone(), saved)


def load_tests(loader, _standard_tests, _pattern):
    suite = loader.loadTestsFromTestCase(ParkedBindingEpochTests)
    suite.addTest(ParkedBindingRepairTests('test_same_registration_gets_one_durable_native_binding_under_original_stop'))
    return suite
