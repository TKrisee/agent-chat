"""Idempotent, operator-authorized safe-pause requests for completed measurements."""
import hashlib
import time

from .core import Coordinator, CoordError


def request_pauses(database, sender_session, measurement_id, recipients):
    """Insert each direct request once, including retries after a server restart.

    The completion callback can span the registry and a separate project database.
    Stable message IDs make retrying after either database commit safe.
    """
    body = (
        'The usage measurement is complete. The operator selected Pause agents at end. '
        'Pause at your next safe checkpoint and start no new work. Safely close '
        'only owned processes and child work, preserve changes and user-open '
        'editors, restore required state, and retain CLOSED receipts before '
        'releasing owned resources. Send one short pause confirmation and '
        'checkpoint path as a reply to this message. Await explicit resume; '
        'end the idle turn without polling.'
    )
    coord = Coordinator(database, sender_session)
    batch_id = 'batch_measurement_' + hashlib.sha256(measurement_id.encode()).hexdigest()
    try:
        coord.require_session()
        unavailable = []
        with coord.tx() as db:
            for recipient in dict.fromkeys(recipients):
                if recipient == sender_session:
                    continue
                if not db.execute('SELECT 1 FROM sessions WHERE id=?', (recipient,)).fetchone():
                    unavailable.append(recipient)
                    continue
                key = hashlib.sha256((measurement_id + '\0' + recipient).encode()).hexdigest()
                message_id = 'message_measurement_' + key
                db.execute('''INSERT OR IGNORE INTO messages
                    (id,sender_session,recipient_session,body,created_at) VALUES(?,?,?,?,?)''',
                           (message_id, sender_session, recipient, body, time.time()))
                db.execute('INSERT OR IGNORE INTO message_batches(message_id,batch_id) VALUES(?,?)',
                           (message_id, batch_id))
                db.execute('INSERT OR IGNORE INTO message_attention(message_id) VALUES(?)', (message_id,))
        if unavailable:
            raise CoordError(f'Pause requested for available agents; {len(unavailable)} measured sessions are no longer registered')
    finally:
        coord.close()
