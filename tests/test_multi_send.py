import contextlib
import pathlib
import sqlite3
import sys
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
from agent_chat.core import CoordError, Coordinator


class MultiSendTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='agent-chat-many-')
        self.db = pathlib.Path(self.tmp.name) / 'state.sqlite3'
        self.sender, self.alpha, self.beta, self.outside = (self.register(name) for name in ('sender', 'alpha', 'beta', 'outside'))

    def tearDown(self): self.tmp.cleanup()

    def register(self, agent):
        coord = Coordinator(self.db, 'session-' + agent)
        try: return coord.register(agent)['session']
        finally: coord.close()

    def many(self, targets, body, reply_to=None):
        coord = Coordinator(self.db, self.sender)
        try: return coord.send_many(targets, body, reply_to)
        finally: coord.close()

    def inbox(self, session):
        coord = Coordinator(self.db, session)
        try: return coord.inbox(True)['messages']
        finally: coord.close()

    def count(self):
        with contextlib.closing(sqlite3.connect(self.db)) as db:
            return db.execute('SELECT COUNT(*) FROM messages').fetchone()[0]

    def test_invalid_target_is_atomic_and_aliases_deduplicate(self):
        before = self.count()
        with self.assertRaises(CoordError): self.many([self.alpha, 'missing'], 'no partial')
        self.assertEqual(self.count(), before)
        sent = self.many(['alpha', self.alpha, 'beta', self.beta], 'one each')
        self.assertEqual([item['recipient_session'] for item in sent], [self.alpha, self.beta])
        self.assertEqual(self.count(), before + 2)

    def test_each_recipient_gets_own_id_and_shared_ack_metadata(self):
        sent = self.many([self.alpha, self.beta], 'group message')
        alpha, beta = self.inbox(self.alpha), self.inbox(self.beta)
        self.assertEqual([item['id'] for item in alpha], [sent[0]['id']])
        self.assertEqual([item['id'] for item in beta], [sent[1]['id']])
        self.assertEqual(alpha[0]['batch_id'], beta[0]['batch_id'])
        self.assertEqual({item['id'] for item in alpha[0]['deliveries']}, {sent[0]['id'], sent[1]['id']})
        coord = Coordinator(self.db, self.beta)
        try: coord.acknowledge(sent[1]['id'])
        finally: coord.close()
        alpha = self.inbox(self.alpha)[0]
        acked = {item['id']: item['acked_at'] for item in alpha['deliveries']}
        self.assertIsNone(acked[sent[0]['id']])
        self.assertIsNotNone(acked[sent[1]['id']])

    def test_batch_metadata_is_complete_when_sibling_is_outside_inbox_page(self):
        sent = self.many([self.alpha, self.beta], 'page boundary')
        coord = Coordinator(self.db, self.alpha)
        try:
            row = coord.db.execute('SELECT id,sender_session,body,created_at,acked_at,seq FROM messages WHERE id=?', (sent[0]['id'],)).fetchone()
            item = coord._message_dicts(coord.db, [row])[0]
        finally: coord.close()
        self.assertEqual({delivery['id'] for delivery in item['deliveries']}, {sent[0]['id'], sent[1]['id']})

    def test_batch_followup_uses_matching_parent_copies_and_rejects_outsider(self):
        parent = self.many([self.alpha, self.beta], 'parent')
        child = self.many([self.beta, self.alpha], 'follow-up', parent[0]['id'])
        parent_for = {item['recipient_session']: item['id'] for item in parent}
        self.assertEqual({item['recipient_session']: item['reply_to'] for item in child}, parent_for)
        before = self.count()
        with self.assertRaises(CoordError): self.many([self.alpha, self.outside], 'bad follow-up', parent[0]['id'])
        self.assertEqual(self.count(), before)

    def test_batch_followup_subset_uses_the_matching_parent_copy(self):
        parent = self.many([self.alpha, self.beta], 'parent')
        alpha_parent = next(item for item in parent if item['recipient_session'] == self.alpha)
        beta_parent = next(item for item in parent if item['recipient_session'] == self.beta)
        child = self.many([self.beta], 'beta only follow-up', alpha_parent['id'])
        self.assertEqual((len(child), child[0]['recipient_session'], child[0]['reply_to']),
                         (1, self.beta, beta_parent['id']))


if __name__ == '__main__': unittest.main()
