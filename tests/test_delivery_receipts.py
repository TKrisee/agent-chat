"""Delivery receipts stay tied to visible agent-recipient rows and durable fields."""
import contextlib
import tempfile
import time
import unittest
from pathlib import Path

from agent_chat.bridge_state import BridgeState
from agent_chat.core import Coordinator, agent_labels
from agent_chat.web import MESSAGE_COLUMNS, message_rows, reader


class DeliveryReceiptProjectionTests(unittest.TestCase):
    def test_agent_ack_and_exact_job_projection_excludes_operator_recipient(self):
        with tempfile.TemporaryDirectory(prefix='delivery-receipt-test-') as directory:
            path = Path(directory) / 'state.sqlite3'
            with contextlib.closing(Coordinator(path, 'agent-a')) as coord:
                coord.register('Agent A')
                coord.session = 'agent-b'; coord.register('Agent B')
                coord.session = 'operator-a'; coord.register('Operator A')
                coord.db.execute("INSERT INTO operator_identities(session_id) VALUES('operator-a')")
                coord.session = 'operator-b'; coord.register('Operator B')
                coord.db.execute("INSERT INTO operator_identities(session_id) VALUES('operator-b')")
                BridgeState(coord)
                now = time.time()
                coord.db.execute('''INSERT INTO messages(id,sender_session,recipient_session,body,created_at,acked_at)
                    VALUES('agent-task','agent-a','agent-b','opaque body',?,?)''', (now, now + 1))
                coord.db.execute('''INSERT INTO messages(id,sender_session,recipient_session,body,created_at,acked_at)
                    VALUES('operator-pair','operator-a','operator-b','opaque body',?,?)''', (now, now + 1))
                coord.db.execute('''INSERT INTO bridge_jobs VALUES('wake-exact','agent-thread','secret payload',
                    'queued','secret queue id',NULL,10,11)''')
                coord.db.execute("INSERT INTO bridge_deliveries VALUES('agent-task','wake-exact')")
                coord.db.execute("INSERT INTO message_attention(message_id) VALUES('agent-task')")

            with reader(path) as db:
                rows = db.execute(f"SELECT {MESSAGE_COLUMNS} FROM messages ORDER BY id").fetchall()
                projected = message_rows(rows, agent_labels(db), db, receipt_operator='operator-a',
                                         include_agent_delivery_receipts=True)

        by_id = {item['id']: item for item in projected}
        agent = by_id['agent-task']
        self.assertEqual(agent['recipient_role'], 'agent')
        self.assertEqual(agent['delivery_receipt']['agent_ack_at'], now + 1)
        self.assertEqual(agent['delivery_receipt']['native_job'], {'status': 'queued', 'updated_at': 11})
        self.assertNotIn('secret', repr(agent['delivery_receipt']))
        pair = by_id['operator-pair']
        self.assertEqual(pair['recipient_role'], 'human_operator')
        self.assertNotIn('delivery_receipt', pair)


if __name__ == '__main__':
    unittest.main()
