import unittest
from unittest import mock

import test_bridge
from agent_chat.bridge import Bridge
from agent_chat.bridge_lease import BridgeManager
from agent_chat.bridge_state import source_trace
from agent_chat.core import CoordError
from agent_chat.rpc import RpcClient


class EligibilityServerTests(unittest.TestCase):
    coord = test_bridge.BridgeTests.coord
    send = test_bridge.BridgeTests.send

    def setUp(self):
        test_bridge.BridgeTests.setUp(self)
        self.manager = BridgeManager(self.db)
        self.addCleanup(self.manager.close)
        self.agent.db.execute('''CREATE TABLE IF NOT EXISTS remote_session_hosts
            (session_id TEXT PRIMARY KEY, host_id TEXT NOT NULL, last_seen_at REAL NOT NULL)''')
        for session in ('worker', 'human'):
            self.agent.db.execute('INSERT INTO remote_session_hosts VALUES(?,?,?)',
                                  (session, 'fixture-host', 1))
        self.identity = {'owner': 'fixture-owner', 'secret': 'x' * 40, 'host_id': 'fixture-host'}
        self.dispatch('acquire', {})

    def dispatch(self, op, params):
        return self.manager.dispatch(dict(self.identity, op=op, params=params))

    def obs(self):
        return {'thread_id': self.thread, 'connection_instance': 'fixture-connection',
                'connection_epoch': 3, 'start_monotonic_ns': 100, 'end_monotonic_ns': 150}

    def prepare(self, source, obs_marker='absent'):
        params = {'thread_id': self.thread,
                  'messages': [{'id': source, 'recipient_session': 'worker'}], 'payload': 'fixture'}
        if obs_marker != 'absent':
            params['eligibility_observation'] = obs_marker
        return self.dispatch('prepare', params)

    def trace(self, source):
        return source_trace(self.db, project_id='default', session_id='human',
                            host_id='fixture-host', source_id=source)

    def test_observation_is_transactionally_linked_and_clock_tagged(self):
        source = self.send()
        job = self.prepare(source, self.obs())
        trace = self.trace(source)
        self.assertIsNone(trace['eligibility_at'])
        observation = trace['job_eligibility_observation']
        self.assertEqual(observation['job_id'], job['id'])
        self.assertEqual((observation['status'], observation['clock_domain'], observation['coverage']),
                         ('observed', 'host_monotonic_ns', 'two_samples_not_continuous'))
        self.assertEqual((observation['start_monotonic_ns'], observation['end_monotonic_ns'],
                          observation['host_id']), (100, 150, 'fixture-host'))
        self.dispatch('release', {})
        self.assertEqual(self.trace(source)['job_eligibility_observation']['reason'], 'dispatcher_changed')

    def test_old_client_and_old_database_show_unknown(self):
        source = self.send()
        self.prepare(source)
        self.agent.db.execute('DROP TABLE bridge_eligibility_observations')
        self.assertEqual(self.trace(source)['job_eligibility_observation']['status'], 'unknown')

    def test_invalid_observations_do_not_prepare(self):
        source = self.send()
        invalid = [None, {}, dict(self.obs(), thread_id='wrong'),
                   dict(self.obs(), connection_epoch=True), dict(self.obs(), start_monotonic_ns=151),
                   dict(self.obs(), end_monotonic_ns=2**63)]
        for observation in invalid:
            with self.subTest(observation=observation), self.assertRaises(CoordError):
                self.prepare(source, observation)
            self.assertEqual(self.agent.db.execute('SELECT COUNT(*) FROM bridge_deliveries').fetchone()[0], 0)

    def test_local_real_bridge_state_and_rpc_dispatch_without_lease(self):
        source = self.send()
        native = RpcClient('ws://127.0.0.1:1')
        bridge = Bridge(self.state, native)
        identity = {'client_instance': 'fixture-connection', 'connection_epoch': 3}
        with mock.patch.object(native, 'request', side_effect=self.rpc.request), \
             mock.patch.object(native, 'connection_identity', return_value=identity), \
             mock.patch.object(self.state, 'prepare', wraps=self.state.prepare) as prepare, \
             mock.patch.object(bridge.capacity_worker, 'tick'):
            bridge.tick(observe_policies=False)
        observation = prepare.call_args.kwargs['eligibility_observation']
        self.assertEqual(observation['connection_instance'], identity['client_instance'])
        self.assertEqual(observation['connection_epoch'], identity['connection_epoch'])
        row = self.agent.db.execute('''SELECT j.status FROM bridge_jobs j
            JOIN bridge_deliveries d ON d.job_id=j.id WHERE d.message_id=?''', (source,)).fetchone()
        self.assertEqual(row['status'], 'dispatched')
        self.assertEqual(self.agent.db.execute('SELECT COUNT(*) FROM bridge_eligibility_observations').fetchone()[0], 0)

    def test_changed_claimed_lease_rolls_back_prepare(self):
        source = self.send()
        old = dict(self.agent.db.execute('SELECT * FROM bridge_dispatcher WHERE host_id=?',
                                         ('fixture-host',)).fetchone())
        self.agent.db.execute('UPDATE bridge_dispatcher SET owner=? WHERE host_id=?',
                              ('replacement', 'fixture-host'))
        with self.assertRaisesRegex(CoordError, 'ownership changed'):
            self.state.prepare(self.thread, [{'id': source, 'recipient_session': 'worker'}], 'fixture',
                               eligibility_observation=self.obs(), eligibility_lease=old)
        self.assertEqual(self.agent.db.execute('SELECT COUNT(*) FROM bridge_deliveries').fetchone()[0], 0)
