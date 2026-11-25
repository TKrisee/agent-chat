"""Isolated persistent supervisor incident observations, without native I/O."""
import contextlib
import tempfile
import unittest
from pathlib import Path
from unittest import mock
from agent_chat.core import Coordinator
from agent_chat.supervisor import SupervisorState


class IncidentIdentityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.db = Path(self.temp.name)/'state.sqlite3'
        self.coord = Coordinator(self.db); self.addCleanup(self.coord.close)
        self.state = SupervisorState(self.coord)
        self.coord.db.execute('CREATE TABLE IF NOT EXISTS agent_controls(target_session TEXT PRIMARY KEY,mode TEXT)')
        self.coord.db.execute("INSERT INTO supervisor_owner_policies(owner_session,enabled,updated_at,recovery_enabled) VALUES('owner',1,0,0)")
        self.job = dict(owner_session='owner',target_session='target',route_generation='generation',host_id='host',thread_id='thread')
        self.route = dict(generation='generation',host_id='host',binding={'thread_id':'thread'},route={'thread_id':'thread'})
        self.now = 1000
        self.clock = mock.patch('agent_chat.supervisor.time.time',side_effect=lambda:self.now); self.clock.start(); self.addCleanup(self.clock.stop)
        self.patch_state()

    def patch_state(self):
        self.state._route = mock.Mock(return_value=self.route)
        self.state._allowed = mock.Mock(return_value=True)
        self.state._recovery_for = mock.Mock(return_value=None)

    def sample(self,error,count,state='error',advance=61):
        self.state.observe(self.job,dict(state=state,source='native',summary={'error':error,'wake':{'pending_unread_messages':count}}),'host')
        self.now += advance

    def notices(self):
        return [row['id'] for row in self.coord.db.execute("SELECT id FROM messages WHERE sender_session='system:supervisor' ORDER BY seq")]

    def test_counters_restart_distinct_error_and_recovery(self):
        for count in (62,74,86,98): self.sample('RPC error -32600: missing source rollout',count)
        self.assertEqual(len(self.notices()),1)
        original = self.notices()[0]
        self.state = SupervisorState(self.coord); self.patch_state()
        self.sample('RPC error -32600: missing source rollout',98)
        self.assertEqual(self.notices(),[original])
        self.sample('RPC error -32603: provider unavailable',98,advance=10)
        self.sample('RPC error -32603: provider unavailable',98,advance=61)
        self.assertEqual(len(self.notices()),1)
        self.sample('RPC error -32603: provider unavailable',98)
        self.assertEqual(len(self.notices()),2)
        self.sample('',98,state='idle')
        self.sample('RPC error -32603: provider unavailable',98)
        self.sample('RPC error -32603: provider unavailable',98)
        self.assertEqual(len(self.notices()),3)

    def test_separate_route_generation_is_a_separate_incident(self):
        self.sample('RPC error -32600: missing source rollout',62)
        self.sample('RPC error -32600: missing source rollout',74)
        self.assertEqual(len(self.notices()),1)
        self.route = dict(generation='new-generation',host_id='host',binding={'thread_id':'new-thread'},route={'thread_id':'new-thread'})
        self.job.update(route_generation='new-generation',thread_id='new-thread')
        self.patch_state()
        self.sample('RPC error -32600: missing source rollout',86)
        self.sample('RPC error -32600: missing source rollout',98)
        self.assertEqual(len(self.notices()),2)

    def test_legacy_active_unsigned_incident_adopts_signature_without_repeat(self):
        self.sample('error-one',62); self.sample('error-one',74)
        first = self.notices()
        self.coord.db.execute('ALTER TABLE supervisor_incidents DROP COLUMN error_signature')
        self.state = SupervisorState(self.coord); self.patch_state()
        self.sample('error-one',86); self.sample('error-one',98)
        self.assertEqual(self.notices(),first)
        self.assertIsNotNone(self.coord.db.execute('SELECT error_signature FROM supervisor_incidents').fetchone()[0])

    def test_error_signature_uses_complete_accepted_string(self):
        prefix = 'x'*512
        self.sample(prefix+'A',62); self.sample(prefix+'A',62)
        self.sample(prefix+'B',62); self.sample(prefix+'B',62)
        self.assertEqual(len(self.notices()),2)


if __name__ == '__main__': unittest.main()
