"""Intentional team stops suppress only actionable pending-work notices."""
import json
import os
import time
import unittest
import uuid
from unittest import mock

from agent_chat import root_lifecycle
from agent_chat.bridge_state import BridgeState
from agent_chat.core import Coordinator, CoordError, assert_agent_running
from agent_chat.remote_service import dispatch
from agent_chat.supervisor import SupervisorState
import test_root_local as local


class SupervisorTeamStopTests(unittest.TestCase):
    call = local.RootLocalTests.call
    enroll = local.RootLocalTests.enroll

    def setUp(self):
        local.RootLocalTests.setUp(self)
        self.enroll()
        self.coord = Coordinator(self.master, 'root')
        self.addCleanup(self.coord.close)
        dispatch(self.coord, {'op': 'register', 'session': 'po', 'host_id': 'host', 'params': {'agent': 'po'}})
        self.coord.session = 'po'
        self.bridge = BridgeState(self.coord)
        self.target_thread = str(uuid.uuid4())
        self.bridge.bind(self.target_thread)
        self.coord.session = 'root'
        self.coord.db.execute('CREATE TABLE IF NOT EXISTS project_orchestrators(session_id TEXT PRIMARY KEY,root_session TEXT NOT NULL)')
        self.coord.db.execute("INSERT INTO project_orchestrators VALUES('po','root')")
        self.source = self.coord.send('po', 'Preserve this original pending task.')
        self.wake = self.bridge.prepare(self.target_thread, [dict(id=self.source['id'], recipient_session='po')], 'Original pending wake.')
        self.bridge.heartbeat('http://isolated.invalid', os.getpid(), host_id='host')
        self.supervisor = SupervisorState(self.coord)
        self.coord.context()
        self.supervisor.configure(enabled=True, recovery_enabled=True)
        self.job = self.supervisor.jobs('host')[0]
        self.now = time.time()
        self.clock = mock.patch('agent_chat.supervisor.time.time', side_effect=lambda: self.now)
        self.clock.start()
        self.addCleanup(self.clock.stop)
        self.stop_id = str(uuid.uuid4())

    def pause(self):
        return root_lifecycle.pause(self.coord, 'root', self.stop_id)

    def observe(self, state='unloaded_pending', advance=0):
        self.now += advance
        return self.supervisor.observe(self.job, dict(source='native', state=state, summary=dict(
            before='unloaded', after='unloaded', contradictory=False, queue_count=0,
            history_state='available', turn_status=None)), 'host')

    def notices(self):
        return [dict(row) for row in self.coord.db.execute(
            "SELECT * FROM messages WHERE sender_session='system:supervisor' ORDER BY seq")]

    def preserved(self):
        tables = ('bridge_jobs', 'bridge_deliveries', 'message_reads', 'message_replies',
                  'supervisor_owner_policies', 'supervisor_policies', 'supervisor_recoveries',
                  'root_team_stops', 'root_team_stop_targets', 'root_team_stop_interrupts')
        return {table: [tuple(row) for row in self.coord.db.execute('SELECT * FROM ' + table)]
                for table in tables}

    def assert_not_suppressed(self):
        self.observe()
        self.observe(advance=61)
        self.assertEqual(len(self.notices()), 1)

    def test_valid_stop_keeps_observation_and_policy_without_pending_notice_or_replay(self):
        self.pause()
        before = self.preserved()
        for advance in (0, 61, 61):
            result = self.observe(advance=advance)
            self.assertTrue(result['accepted'])
            self.assertFalse(result['recovery_candidate'])
        self.assertEqual(self.notices(), [])
        self.assertEqual(self.preserved(), before)
        self.assertIsNone(self.coord.db.execute('SELECT acked_at FROM messages WHERE id=?', (self.source['id'],)).fetchone()[0])
        self.assertEqual(self.coord.db.execute('SELECT state FROM supervisor_observations').fetchone()[0], 'unloaded_pending')
        with self.assertRaisesRegex(CoordError, 'stop-all barrier is active'):
            assert_agent_running(self.coord.db, 'po')

    def test_effective_current_po_admission_allows_unloaded_pending_incident_and_recovery_candidate(self):
        self.pause()
        admission = root_lifecycle.admit_po(
            self.coord, 'root', self.stop_id, 'po', self.target_thread, 'host')
        self.assertTrue(admission['effective'])
        self.assertFalse(root_lifecycle.stopped(self.coord.db, 'po'))
        self.assertFalse(self.supervisor._intentional_team_stop(self.job))

        first = self.observe()
        self.assertFalse(first['recovery_candidate'])  # Existing incident-age gate is preserved.
        self.assertEqual(self.coord.db.execute(
            "SELECT state FROM supervisor_observations").fetchone()[0], 'unloaded_pending')
        self.now += 61
        self.bridge.heartbeat('http://isolated.invalid', os.getpid(), host_id='host')
        aged = self.observe()
        self.assertTrue(aged['recovery_candidate'])
        incident = self.coord.db.execute('''SELECT active,condition FROM supervisor_incidents
            WHERE owner_session='root' AND target_session='po' AND condition='unloaded_pending' ''').fetchone()
        self.assertEqual((incident['active'], incident['condition']), (1, 'unloaded_pending'))
        self.assertEqual(len(self.notices()), 1)
        self.assertEqual(self.coord.db.execute(
            'SELECT COUNT(*) FROM supervisor_recoveries').fetchone()[0], 0)  # Observation is not a recovery claim or wake.
        self.assertEqual(self.coord.db.execute(
            'SELECT status FROM bridge_jobs WHERE id=?', (self.wake['id'],)).fetchone()[0], 'prepared')

    def test_stale_po_admission_does_not_exempt_mismatched_current_host(self):
        self.pause()
        admission = root_lifecycle.admit_po(
            self.coord, 'root', self.stop_id, 'po', self.target_thread, 'host')
        self.assertTrue(admission['effective'])
        self.coord.db.execute("UPDATE remote_session_hosts SET host_id='changed-host' WHERE session_id='po'")
        self.assertFalse(root_lifecycle.po_admitted(self.coord.db, self.stop_id, 'po'))
        # The recorded stop target still matches the sampled job, so its stale
        # admission cannot suppress the ordinary stopped-worker notice gate.
        self.assertTrue(self.supervisor._intentional_team_stop(self.job))

    def test_noncurrent_designated_role_does_not_exempt_stopped_target(self):
        self.pause()
        admission = root_lifecycle.admit_po(
            self.coord, 'root', self.stop_id, 'po', self.target_thread, 'host')
        self.assertTrue(admission['effective'])
        self.coord.db.execute("UPDATE project_orchestrators SET root_session='another-root' WHERE session_id='po'")
        self.assertFalse(root_lifecycle.po_admitted(self.coord.db, self.stop_id, 'po'))
        self.assertTrue(self.supervisor._intentional_team_stop(self.job))

    def test_resume_restores_normal_notice_with_original_pending_input(self):
        self.pause()
        self.observe()
        self.observe(advance=61)
        self.assertEqual(self.notices(), [])
        root_lifecycle.resume(self.coord, 'root', self.stop_id)
        self.observe()
        self.observe(advance=61)
        self.assertEqual(len(self.notices()), 1)
        self.assertEqual([row[0] for row in self.coord.db.execute('SELECT id FROM bridge_jobs')], [self.wake['id']])
        self.assertEqual(self.coord.db.execute('SELECT message_id FROM bridge_deliveries WHERE job_id=?', (self.wake['id'],)).fetchone()[0], self.source['id'])
        self.assertIsNone(self.coord.db.execute('SELECT acked_at FROM messages WHERE id=?', (self.source['id'],)).fetchone()[0])

    def test_stop_does_not_dismiss_or_ack_historical_notice(self):
        self.assert_not_suppressed()
        historical = self.notices()
        self.pause()
        before = self.preserved()
        self.observe(advance=61)
        self.assertEqual(self.notices(), historical)
        self.assertEqual(self.preserved(), before)

    def test_source_reporting_notice_uses_the_same_narrow_stop_gate(self):
        self.coord.db.execute('INSERT INTO supervisor_recoveries VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
            ('recovery', 'root', 'po', self.job['route_generation'], 1, self.target_thread, 'host', 'completed',
             json.dumps([self.source['id']]), self.wake['id'], 1, self.now, self.now))
        self.pause()
        before = self.preserved()
        self.observe(state='idle')
        self.observe(state='idle', advance=61)
        self.assertEqual(self.notices(), [])
        self.assertEqual(self.preserved(), before)
        root_lifecycle.resume(self.coord, 'root', self.stop_id)
        self.observe(state='idle')
        self.observe(state='idle', advance=61)
        self.assertEqual(len(self.notices()), 1)
        self.assertIn('source_report_pending', self.notices()[0]['body'])

    def test_native_error_and_uncertain_wake_still_alert_under_stop(self):
        self.pause()
        for state in ('error', 'uncertain_wake'):
            self.observe(state=state)
            self.observe(state=state, advance=61)
        notices = self.notices()
        self.assertEqual(len(notices), 2)
        self.assertTrue(any('native_error' in row['body'] for row in notices))
        self.assertTrue(any('uncertain_wake' in row['body'] for row in notices))

    def test_host_error_still_alerts_under_stop(self):
        self.pause()
        self.coord.db.execute("UPDATE bridge_client_runtime SET error='fixture host failure'")
        self.supervisor.status()
        self.now += 61
        self.supervisor.status()
        self.assertEqual(len(self.notices()), 1)
        self.assertIn('error_host', self.notices()[0]['body'])

    def test_unknown_target_status_does_not_hide_a_notice(self):
        self.pause()
        self.coord.db.execute("UPDATE root_team_stop_targets SET status='unknown'")
        self.assert_not_suppressed()

    def test_mismatched_stop_target_thread_does_not_hide_a_notice(self):
        self.pause()
        self.coord.db.execute("UPDATE root_team_stop_targets SET thread_id='other-thread'")
        self.assert_not_suppressed()

    def test_mismatched_stop_target_host_does_not_hide_a_notice(self):
        self.pause()
        self.coord.db.execute("UPDATE root_team_stop_targets SET host_id='other-host'")
        self.assert_not_suppressed()

    def test_conflicting_active_stop_rows_do_not_hide_a_notice(self):
        self.pause()
        self.coord.db.execute('INSERT INTO root_team_stops VALUES(?,?,1,?,NULL)',
                              (str(uuid.uuid4()), 'root', self.now))
        self.assert_not_suppressed()

    def test_contradictory_resumed_barrier_does_not_hide_a_notice(self):
        self.pause()
        self.coord.db.execute('UPDATE root_team_stops SET resumed_at=?', (self.now,))
        self.assert_not_suppressed()

    def test_mismatched_stop_requester_does_not_hide_a_notice(self):
        self.pause()
        self.coord.db.execute("UPDATE root_team_stop_targets SET requester_session='other-root'")
        self.assert_not_suppressed()

    def test_malformed_stop_request_id_does_not_hide_a_notice(self):
        self.pause()
        self.coord.db.execute("UPDATE root_team_stops SET id='malformed'")
        self.coord.db.execute("UPDATE root_team_stop_targets SET stop_id='malformed'")
        self.assert_not_suppressed()

    def test_missing_stop_target_schema_does_not_hide_a_notice(self):
        self.pause()
        self.coord.db.execute('DROP TABLE root_team_stop_targets')
        self.assert_not_suppressed()
