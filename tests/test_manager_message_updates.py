import sqlite3
import tempfile
import unittest
from pathlib import Path

from agent_chat import core


class ManagerMessageUpdateTests(unittest.TestCase):
    def setUp(self):
        self.private = tempfile.TemporaryDirectory(prefix='manager-message-update-', dir='/private/tmp')
        self.addCleanup(self.private.cleanup)
        self.path = Path(self.private.name) / 'project.sqlite3'
        self.po, self.worker, self.other = 'session_po', 'session_worker', 'session_other'
        self.task = 'work_current'
        self.coordinator = core.Coordinator(self.path, self.po)
        self.addCleanup(self.coordinator.close)
        self.coordinator.register('po')
        for session, label in ((self.worker, 'worker'), (self.other, 'other')):
            coord = core.Coordinator(self.path, session)
            coord.register(label)
            coord.close()
        self.coordinator.db.executescript('''
            INSERT INTO meta(key,value) VALUES('project_id','project_test');
            CREATE TABLE project_orchestrators (session_id TEXT, root_session TEXT);
            CREATE TABLE agent_management (target_session TEXT PRIMARY KEY, manager_session TEXT NOT NULL, request_id TEXT NOT NULL);
            CREATE TABLE project_work (id TEXT PRIMARY KEY, project_id TEXT NOT NULL, kind TEXT NOT NULL,
              goal_id TEXT, source_request_id TEXT NOT NULL UNIQUE, owner_session TEXT NOT NULL,
              owner_role TEXT NOT NULL, assigned_by TEXT NOT NULL, title TEXT NOT NULL, state TEXT NOT NULL,
              created_at REAL NOT NULL, updated_at REAL NOT NULL);
            INSERT INTO project_orchestrators VALUES('session_po','session_root');
            INSERT INTO agent_management VALUES('session_worker','session_po','request_worker');
            INSERT INTO agent_management VALUES('session_other','session_po','request_other');
            INSERT INTO project_work VALUES('work_current','project_test','task','goal_current','source_current',
              'session_worker','worker','session_po','Current task','in_progress',0,0);
        ''')

    def markers(self):
        return [tuple(row) for row in self.coordinator.db.execute(
            'SELECT message_id,task_id FROM manager_message_updates').fetchall()]

    def message_count(self):
        return self.coordinator.db.execute('SELECT count(*) FROM messages').fetchone()[0]

    def test_current_po_marks_later_direct_update_atomically(self):
        sent = self.coordinator.send(self.worker, 'Continue validation and report an actual blocker.', update_for=self.task)
        self.assertEqual(self.markers(), [(sent['id'], self.task)])

    def test_ordinary_send_is_unchanged_and_unmarked(self):
        sent = self.coordinator.send(self.worker, 'Ordinary later progress remains an ordinary send.')
        self.assertEqual(sent['recipient_session'], self.worker)
        self.assertEqual(self.markers(), [])

    def test_exact_typed_assignment_reply_allows_ordinary_po_follow_up(self):
        assignment = self.coordinator.send(self.worker, 'Original typed assignment.')
        self.coordinator.db.execute('UPDATE project_work SET source_request_id=? WHERE id=?',
                                    (assignment['id'], self.task))
        worker = core.Coordinator(self.path, self.worker)
        self.addCleanup(worker.close)
        worker.message(assignment['id'])
        typed = worker.send(self.po, 'Typed task progress.', reply_to=assignment['id'], ack_reply=True)
        self.coordinator.message(typed['id'])
        first = self.coordinator.send(self.worker, 'Ordinary first reply.', reply_to=typed['id'])
        follow_up = self.coordinator.send(self.worker, 'Later ordinary current-task update.', reply_to=typed['id'])
        self.assertEqual(self.markers(), [(first['id'], self.task), (follow_up['id'], self.task)])

    def test_immediately_linked_explicit_marker_can_carry_the_marker_forward(self):
        first = self.coordinator.send(self.worker, 'Explicit marked update.', update_for=self.task)
        second = self.coordinator.send(self.worker, 'Exact ordinary follow-up.', reply_to=first['id'])
        self.assertEqual(self.markers(), [(first['id'], self.task), (second['id'], self.task)])

    def test_marked_message_allows_ordinary_worker_reply_without_ack(self):
        first = self.coordinator.send(self.worker, 'Explicit marked update.', update_for=self.task)
        worker = core.Coordinator(self.path, self.worker)
        self.addCleanup(worker.close)
        worker.message(first['id'])
        reply = worker.send(self.po, 'Ordinary first reply.', reply_to=first['id'])
        self.assertNotIn('acknowledged_reply', reply)
        self.assertIsNone(self.coordinator.db.execute('SELECT acked_at FROM messages WHERE id=?', (first['id'],)).fetchone()[0])
        self.assertEqual(self.markers(), [(first['id'], self.task)])

    def test_unrelated_ordinary_reply_has_no_task_marker(self):
        worker = core.Coordinator(self.path, self.worker)
        self.addCleanup(worker.close)
        unrelated = worker.send(self.po, 'Unrelated note.')
        self.coordinator.message(unrelated['id'])
        reply = self.coordinator.send(self.worker, 'Ordinary reply.', reply_to=unrelated['id'])
        self.assertEqual(reply['reply_to'], unrelated['id'])
        self.assertEqual(self.markers(), [])

    def test_wrong_sender_task_and_recipient_fail_before_send_or_marker(self):
        worker = core.Coordinator(self.path, self.worker)
        self.addCleanup(worker.close)
        with self.assertRaisesRegex(core.CoordError, 'current designated project PO'):
            worker.send(self.po, 'not a PO update', update_for=self.task)
        with self.assertRaisesRegex(core.CoordError, 'exact current in_progress task'):
            self.coordinator.send(self.worker, 'wrong task', update_for='work_missing')
        with self.assertRaisesRegex(core.CoordError, 'exact current in_progress task'):
            self.coordinator.send(self.other, 'wrong owner', update_for=self.task)
        self.assertEqual(self.message_count(), 0)
        self.assertEqual(self.markers(), [])

    def test_first_ack_is_rejected_without_mutation(self):
        worker = core.Coordinator(self.path, self.worker)
        self.addCleanup(worker.close)
        source = worker.send(self.po, 'normal worker message')
        before = self.message_count()
        with self.assertRaisesRegex(core.CoordError, 'cannot be a first-ACK reply'):
            self.coordinator.send(self.worker, 'invalid first ACK', reply_to=source['id'], ack_reply=True,
                                  update_for=self.task)
        self.assertEqual(self.message_count(), before)
        self.assertEqual(self.markers(), [])

    def test_legacy_database_gains_optional_marker_table(self):
        self.coordinator.close()
        with sqlite3.connect(self.path) as db:
            db.execute('DROP TABLE manager_message_updates')
        self.coordinator = core.Coordinator(self.path, self.po)
        self.assertTrue(self.coordinator.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='manager_message_updates'").fetchone())

    def test_legacy_ordinary_later_reply_has_no_activity_schema_dependency(self):
        legacy_path = Path(self.private.name) / 'legacy.sqlite3'
        po = core.Coordinator(legacy_path, 'session_legacy_po')
        worker = core.Coordinator(legacy_path, 'session_legacy_worker')
        self.addCleanup(po.close)
        self.addCleanup(worker.close)
        po.register('legacy-po')
        worker.register('legacy-worker')
        source = worker.send('session_legacy_po', 'Legacy ordinary source.')
        po.message(source['id'])
        po.send('session_legacy_worker', 'Legacy required first ACK.', reply_to=source['id'], ack_reply=True)
        later = po.send('session_legacy_worker', 'Legacy ordinary later reply.', reply_to=source['id'])
        self.assertEqual(later['recipient_session'], 'session_legacy_worker')
        self.assertFalse(po.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='project_work'").fetchone())
        self.assertEqual(po.db.execute('SELECT count(*) FROM manager_message_updates').fetchone()[0], 0)


def load_tests(loader, tests, pattern):
    return unittest.TestSuite(ManagerMessageUpdateTests(name) for name in ManagerMessageUpdateTests.__dict__
                              if name.startswith('test_'))
