import tempfile
import time
import unittest
from pathlib import Path

from agent_chat.core import Coordinator, CoordError
from agent_chat.bridge_state import BridgeState
from agent_chat.supervisor import SupervisorState


class HistoricalDeliveryRoutingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        path = Path(self.tmp.name) / 'state.sqlite3'
        self.sender = Coordinator(path, 'sender')
        self.sender.register('sender')
        self.recipient = Coordinator(path, 'recipient')
        self.recipient.register('recipient')
        self.addCleanup(self.sender.close)
        self.addCleanup(self.recipient.close)
        self.bridge = BridgeState(self.recipient)
        self.bridge.bind(thread_id='00000000-0000-4000-8000-000000000001')
        self.supervisor = SupervisorState(self.recipient)

    def send(self, body='current instruction'):
        return self.sender.send('recipient', body)['id']

    def legacy(self, message, reader='recipient', acknowledged=True):
        if acknowledged:
            self.recipient.acknowledge(message)
        self.recipient.db.execute(
            'INSERT INTO message_reads(message_id,reader_session,read_at,provenance) VALUES(?,?,?,?)',
            (message, reader, 1, 'legacy_unknown'))

    def archive_boundary(self):
        self.recipient.db.execute(
            "INSERT OR REPLACE INTO meta(key,value) VALUES('message_delivery_archive_before',?)",
            (str(time.time()),))

    def test_migrated_acknowledged_history_does_not_starve_current_work(self):
        historical = [self.send('retained old instruction') for _ in range(25)]
        for message in historical:
            self.legacy(message)
        self.archive_boundary()
        before = [tuple(row) for row in self.recipient.db.execute(
            'SELECT id,body,acked_at FROM messages ORDER BY seq')]
        current = self.send()
        self.recipient.acknowledge(current)  # ACK alone must still not consume new input.
        self.assertEqual([item['id'] for item in self.bridge.pending()], [current])
        self.assertEqual([item['id'] for item in self.recipient.inbox()['messages']], [current])
        self.assertEqual(self.supervisor._wake_summary('00000000-0000-4000-8000-000000000001', 'recipient')
                         ['pending_unread_messages'], 1)
        with self.assertRaisesRegex(CoordError, 'read your inbox'):
            self.recipient.request('new-work', 5)
        context = self.recipient.context(limit=1)
        self.assertEqual([item['id'] for item in context['messages']], [current])
        self.assertFalse(context['has_more'])
        self.assertEqual(self.recipient.request('new-work', 5)['state'], 'owned')
        self.assertEqual(self.bridge.pending(), [])
        self.assertEqual([tuple(row) for row in self.recipient.db.execute(
            'SELECT id,body,acked_at FROM messages ORDER BY seq LIMIT 25')], before)
        self.assertEqual(self.recipient.db.execute(
            "SELECT COUNT(*) FROM message_reads WHERE provenance='legacy_unknown'").fetchone()[0], 25)
        self.assertEqual(len(self.recipient.inbox(all_messages=True)['messages']), 26)

    def test_unacknowledged_legacy_wrong_reader_and_ack_only_still_require_reads(self):
        pending = [self.send() for _ in range(3)]
        self.legacy(pending[0], acknowledged=False)
        self.legacy(pending[1], reader='sender')
        self.recipient.acknowledge(pending[2])
        self.assertEqual([item['id'] for item in self.bridge.pending()], pending)
        self.assertEqual([item['id'] for item in self.recipient.inbox()['messages']], pending)
        for message in pending:
            with self.assertRaisesRegex(CoordError, 'read your inbox'):
                self.recipient.request('pending-work', 5)
            self.recipient.message(message)
        self.assertEqual(self.recipient.request('pending-work', 5)['state'], 'owned')

    def test_real_three_column_migration_preserves_archive_and_pending_boundaries(self):
        archived, pending = self.send('completed historical work'), self.send('pending historical work')
        self.recipient.acknowledge(archived)
        self.recipient.db.execute('DROP TABLE message_reads')
        self.recipient.db.execute('CREATE TABLE message_reads '
            '(message_id TEXT PRIMARY KEY,reader_session TEXT NOT NULL,read_at REAL NOT NULL)')
        for message in (archived, pending):
            self.recipient.db.execute('INSERT INTO message_reads VALUES(?,?,?)',
                                      (message, 'recipient', 1))
        migrated = Coordinator(self.recipient.path, 'recipient')
        try:
            self.assertEqual([item['id'] for item in migrated.inbox()['messages']], [pending])
            self.assertEqual([item['id'] for item in self.bridge.pending()], [pending])
            with self.assertRaisesRegex(CoordError, 'read your inbox'):
                migrated.request('post-migration', 5)
            migrated.message(pending)
            self.assertEqual(migrated.request('post-migration', 5)['state'], 'owned')
            self.assertEqual(migrated.db.execute(
                'SELECT provenance FROM message_reads WHERE message_id=?', (archived,)).fetchone()[0],
                'legacy_unknown')
        finally:
            migrated.close()

    def test_prior_ack_without_any_read_row_is_archived_but_new_ack_is_not(self):
        historical = self.send('old ACK without read evidence')
        self.recipient.acknowledge(historical)
        self.archive_boundary()
        current = self.send('new ACK without read evidence')
        self.recipient.acknowledge(current)
        self.assertEqual([item['id'] for item in self.bridge.pending()], [current])
        self.assertEqual([item['id'] for item in self.recipient.inbox()['messages']], [current])
        with self.assertRaisesRegex(CoordError, 'read your inbox'):
            self.recipient.request('new-ack-is-not-proof', 5)
        self.recipient.message(current)
        self.assertEqual(self.recipient.request('new-ack-is-not-proof', 5)['state'], 'owned')
        self.assertIsNone(self.recipient.db.execute(
            'SELECT 1 FROM message_reads WHERE message_id=?', (historical,)).fetchone())

    def test_bridge_rechecks_archive_boundary_without_fabricating_read_evidence(self):
        historical = self.send('old completed task')
        job = self.bridge.prepare('00000000-0000-4000-8000-000000000001', self.bridge.pending(), 'original wake')
        self.assertTrue(self.bridge.still_unread(job['id']))
        self.legacy(historical)
        self.archive_boundary()
        self.assertFalse(self.bridge.still_unread(job['id']))
        self.assertEqual(self.bridge.wake_messages('00000000-0000-4000-8000-000000000001', [historical]), [])
        self.assertIsNone(self.bridge.prepare('00000000-0000-4000-8000-000000000001',
            [{'id': historical, 'recipient_session': 'recipient'}], 'must not replay'))
        self.assertFalse(self.supervisor._recovery_delivery_eligible(historical, 'recipient'))
        self.assertEqual(self.recipient.db.execute(
            'SELECT provenance FROM message_reads WHERE message_id=?', (historical,)).fetchone()[0],
            'legacy_unknown')
        self.assertEqual(self.recipient.message(historical)['message']['id'], historical)
        self.assertEqual(self.recipient.db.execute(
            'SELECT provenance FROM message_reads WHERE message_id=?', (historical,)).fetchone()[0],
            'recipient_full_message')

    def test_archived_legacy_ack_still_blocks_privileged_abandonment(self):
        from test_ownership_disposition import OwnershipDispositionTests
        fixture = OwnershipDispositionTests('test_privileged_consumption_requires_full_provenance_and_exact_recipient')
        fixture.setUp()
        try:
            claim = fixture._stale('file:archived-input.py')
            fixture.coord.db.execute('UPDATE resources SET stale=1 WHERE name=?', (claim['resource'],))
            fixture.coord.db.execute(
                'INSERT INTO messages(id,sender_session,recipient_session,body,created_at,acked_at) '
                'VALUES(?,?,?,?,?,?)', ('archived-legacy', 'other', 'old', 'historical evidence', 1, 2))
            fixture.coord.db.execute(
                'INSERT INTO message_reads(message_id,reader_session,read_at,provenance) VALUES(?,?,?,?)',
                ('archived-legacy', 'old', 1, 'legacy_unknown'))
            fixture.coord.db.execute(
                "INSERT OR REPLACE INTO meta(key,value) VALUES('message_delivery_archive_before','3')")
            before = tuple(fixture.coord.db.execute('SELECT * FROM resources WHERE name=?',
                                                    (claim['resource'],)).fetchone())
            result = fixture._apply(claim)
            self.assertEqual(result['outcome'], 'blocked')
            self.assertEqual(tuple(fixture.coord.db.execute('SELECT * FROM resources WHERE name=?',
                             (claim['resource'],)).fetchone()), before)
            self.assertEqual(fixture.coord.db.execute(
                'SELECT provenance FROM message_reads WHERE message_id=?', ('archived-legacy',)).fetchone()[0],
                'legacy_unknown')
        finally:
            fixture.tearDown()


if __name__ == '__main__':
    unittest.main()
