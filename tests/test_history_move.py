import hashlib
from pathlib import Path
import tempfile
import unittest
import uuid
from unittest import mock

from agent_chat.bridge_state import BridgeState
from agent_chat.core import Coordinator, CoordError
from agent_chat.history_move import HistoryMove, fingerprint, record


class HistoryMoveTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.source = Coordinator(Path(self.tmp.name) / 'source.db', 'source-operator')
        self.destination = Coordinator(Path(self.tmp.name) / 'destination.db', 'destination-operator')
        for coord, identities in ((self.source, [('source-operator','operator'),('old-agent','maintainer'),('other-agent','other')]),
                                  (self.destination,[('destination-operator','operator'),('new-agent','maintainer')])):
            self.addCleanup(coord.close)
            for session, label in identities:
                coord.session = session
                coord.register(label)
            BridgeState(coord)
        self.source.session = 'source-operator'
        self.destination.session = 'destination-operator'
        self.request = str(uuid.uuid4())
        self.move = HistoryMove(self.source.path,self.destination.path,'source','destination',
                                'old-agent','new-agent','source-operator','destination-operator')

    def send(self, sender, recipient, body, **params):
        self.source.session = sender
        return self.source._send_prepared(recipient,body,params.pop('attachments',[]),**params)

    def execute(self):
        preview = self.move.preview(self.request)
        return self.move.execute(self.request,preview['count'],preview['digest'])

    def records(self, coord):
        return [record(coord.db,row) for row in coord.db.execute('SELECT * FROM messages ORDER BY seq')]

    def test_content_attachments_replies_ack_retry_and_unrelated_rows(self):
        parent = self.send('source-operator','old-agent','α\nuser question')
        self.source.session = 'old-agent'
        self.source.message(parent['id'])
        attachments = [('same.txt','text/plain',b'first\n'),('same.txt','text/plain',b'\x00second\xff')]
        reply = self.send('old-agent','source-operator','answer\nβ',attachments=attachments,
                          reply_to=parent['id'],ack_reply=True)
        excluded = self.send('source-operator','other-agent','excluded')
        before = record(self.source.db,self.source.db.execute('SELECT * FROM messages WHERE id=?',(excluded['id'],)).fetchone())
        self.destination.session = 'destination-operator'
        existing = self.destination.send('new-agent','unrelated unread')
        old_destination = self.records(self.destination)
        result = self.execute()
        self.assertEqual((result['count'],result['attachments'],result['replies']),(2,2,1))
        self.assertEqual(self.records(self.source),[before])
        self.assertEqual(self.records(self.destination)[:1],old_destination)
        self.assertEqual(self.destination.db.execute('SELECT acked_at FROM messages WHERE id=?',(reply['id'],)).fetchone()[0],None)
        self.assertIsNotNone(self.destination.db.execute('SELECT acked_at FROM messages WHERE id=?',(parent['id'],)).fetchone()[0])
        self.assertEqual([r[0] for r in self.destination.db.execute('SELECT content FROM attachments ORDER BY rowid')],[a[2] for a in attachments])
        self.destination.session = 'new-agent'
        retry = self.destination._send_prepared('destination-operator','answer\nβ',attachments,
                                               reply_to=parent['id'],ack_reply=True)
        self.assertEqual(retry['id'],reply['id'])
        with self.assertRaisesRegex(CoordError,'differs'):
            self.destination._send_prepared('destination-operator','changed',attachments,reply_to=parent['id'],ack_reply=True)
        self.assertEqual([m['id'] for m in BridgeState(self.destination).pending()],[existing['id']])
        self.assertEqual(list(self.source.db.execute('PRAGMA foreign_key_check')),[])
        self.assertEqual(list(self.destination.db.execute('PRAGMA foreign_key_check')),[])
        self.assertEqual(self.move.execute(self.request,result['count'],result['digest']),result)

    def test_group_delivery_moves_without_its_other_recipient(self):
        self.source.session = 'source-operator'
        deliveries = self.source.send_group_prepared(['old-agent','other-agent'],'group',[])
        old = next(item for item in deliveries if item['recipient_session']=='old-agent')
        sibling = next(item for item in deliveries if item['recipient_session']=='other-agent')
        self.send('old-agent','source-operator','reply to my copy',reply_to=old['id'])
        sibling_before = record(self.source.db,self.source.db.execute('SELECT * FROM messages WHERE id=?',(sibling['id'],)).fetchone())
        result = self.execute()
        self.assertEqual((result['count'],result['group_deliveries']),(2,1))
        self.assertEqual(self.records(self.source),[sibling_before])
        self.assertEqual(BridgeState(self.destination).pending(),[])
        self.assertEqual(self.destination.db.execute('SELECT batch_id FROM message_batches WHERE message_id=?',(old['id'],)).fetchone()[0],old['batch_id'])
        self.assertEqual(self.source.db.execute('SELECT batch_id FROM message_batches WHERE message_id=?',(sibling['id'],)).fetchone()[0],sibling['batch_id'])

    def test_crossing_reply_in_both_directions_blocks_all_changes(self):
        selected = self.send('source-operator','old-agent','selected')
        other = self.send('other-agent','source-operator','other')
        for child,parent in ((selected['id'],other['id']),(other['id'],selected['id'])):
            with self.subTest(child=child):
                self.source.db.execute('INSERT INTO message_replies VALUES(?,?)',(child,parent))
                before = self.records(self.source)
                with self.assertRaisesRegex(CoordError,'crosses'):
                    self.move.preview(self.request)
                self.assertEqual(self.records(self.source),before)
                self.assertEqual(self.records(self.destination),[])
                self.source.db.execute('DELETE FROM message_replies')

    def test_changed_preview_collision_and_source_route_are_rejected(self):
        message = self.send('source-operator','old-agent','before')
        preview = self.move.preview(self.request)
        self.source.db.execute('UPDATE messages SET body=? WHERE id=?',('after',message['id']))
        with self.assertRaisesRegex(CoordError,'selection changed|Selection changed'):
            self.move.execute(self.request,preview['count'],preview['digest'])
        self.assertEqual(self.records(self.destination),[])
        self.destination.db.execute('INSERT INTO messages(id,sender_session,recipient_session,body,created_at) VALUES(?,?,?,?,?)',
                                    (message['id'],'destination-operator','new-agent','conflict',1))
        with self.assertRaisesRegex(CoordError,'already exists'):
            self.move.preview(self.request)
        self.destination.db.execute('DELETE FROM messages')
        self.source.session = 'old-agent'
        BridgeState(self.source).bind(str(uuid.uuid4()))
        with self.assertRaisesRegex(CoordError,'detach'):
            self.move.preview(self.request)

    def test_recover_after_import_commit_without_duplicate_or_wake(self):
        message = self.send('source-operator','old-agent','unacknowledged historical input')
        preview = self.move.preview(self.request)
        with mock.patch.object(self.move,'remove_source',side_effect=RuntimeError('simulated interruption')):
            with self.assertRaisesRegex(RuntimeError,'interruption'):
                self.move.execute(self.request,preview['count'],preview['digest'])
        self.assertEqual(len(self.records(self.source)),1)
        self.assertEqual(len(self.records(self.destination)),1)
        self.assertEqual(self.move.preview(self.request)['status'],'imported')
        self.assertEqual(BridgeState(self.destination).pending(),[])
        # Reopening the destination does not make historical input wakeable.
        reopened = Coordinator(self.destination.path,'new-agent')
        try:
            self.assertEqual(BridgeState(reopened).pending(),[])
        finally:
            reopened.close()
        result = self.move.execute(self.request,preview['count'],preview['digest'])
        self.assertEqual(result['status'],'completed')
        self.assertEqual(len(self.records(self.source)),0)
        self.assertEqual(len(self.records(self.destination)),1)
        self.assertEqual(self.destination.db.execute('SELECT acked_at FROM messages WHERE id=?',(message['id'],)).fetchone()[0],None)
        self.destination.session = 'destination-operator'
        fresh = self.destination.send('new-agent','new live input')
        self.assertEqual([m['id'] for m in BridgeState(self.destination).pending()],[fresh['id']])

    def test_recovery_refuses_changed_import_and_changed_source(self):
        self.send('source-operator','old-agent','original')
        preview = self.move.preview(self.request)
        with mock.patch.object(self.move,'remove_source',side_effect=RuntimeError('interrupt')):
            with self.assertRaises(RuntimeError):
                self.move.execute(self.request,preview['count'],preview['digest'])
        self.destination.db.execute("UPDATE messages SET body='edited'")
        with self.assertRaisesRegex(CoordError,'imported history changed'):
            self.move.execute(self.request,preview['count'],preview['digest'])
        self.destination.db.execute("UPDATE messages SET body='original'")
        self.source.db.execute("UPDATE messages SET body='edited source'")
        with self.assertRaisesRegex(CoordError,'source history changed'):
            self.move.execute(self.request,preview['count'],preview['digest'])
        self.assertEqual(len(self.records(self.source)),1)

    def test_metadata_inconsistency_and_ack_reference_boundary(self):
        parent = self.send('source-operator','old-agent','question')
        self.source.session = 'old-agent'
        self.source.message(parent['id'])
        reply = self.send('old-agent','source-operator','answer',reply_to=parent['id'],ack_reply=True)
        self.source.db.execute("UPDATE reply_ack_idempotency SET fingerprint='invalid'")
        with self.assertRaisesRegex(CoordError,'inconsistent'):
            self.move.preview(self.request)
        self.assertEqual(len(self.records(self.source)),2)
        self.assertEqual(self.records(self.destination),[])

    def test_orphan_open_remote_guard_refuses_preview(self):
        self.send('source-operator','old-agent','question')
        self.source.db.execute('CREATE TABLE remote_guarded_runs(session_id TEXT,closed_at REAL)')
        self.source.db.execute('INSERT INTO remote_guarded_runs VALUES(?,NULL)',('old-agent',))
        with self.assertRaisesRegex(CoordError,'remote guard'):
            self.move.preview(self.request)
        self.assertEqual(len(self.records(self.source)),1)
        self.assertEqual(self.records(self.destination),[])

    def test_missing_or_nonterminal_suppression_refuses_recovery(self):
        message = self.send('source-operator','old-agent','unread history')
        preview = self.move.preview(self.request)
        with mock.patch.object(self.move,'remove_source',side_effect=RuntimeError('interrupt')):
            with self.assertRaises(RuntimeError):
                self.move.execute(self.request,preview['count'],preview['digest'])
        job = 'history_move_' + self.request
        self.destination.db.execute('DELETE FROM bridge_deliveries WHERE message_id=?',(message['id'],))
        with self.assertRaisesRegex(CoordError,'wake suppression'):
            self.move.execute(self.request,preview['count'],preview['digest'])
        self.destination.db.execute('INSERT INTO bridge_deliveries VALUES(?,?)',(message['id'],job))
        self.destination.db.execute("UPDATE bridge_jobs SET status='prepared' WHERE id=?",(job,))
        with self.assertRaisesRegex(CoordError,'wake suppression'):
            self.move.execute(self.request,preview['count'],preview['digest'])
        self.assertEqual(len(self.records(self.source)),1)

    def test_expired_authorization_after_import_retains_source(self):
        self.send('source-operator','old-agent','unread history')
        preview = self.move.preview(self.request)
        calls = []
        def authorize(db):
            calls.append(True)
            if len(calls)>1:
                raise CoordError('reservation expired')
        with self.assertRaisesRegex(CoordError,'expired'):
            self.move.execute(self.request,preview['count'],preview['digest'],authorize)
        self.assertEqual(len(calls),2)
        self.assertEqual(len(self.records(self.source)),1)
        self.assertEqual(self.move.preview(self.request)['status'],'imported')
        self.assertEqual(BridgeState(self.destination).pending(),[])


if __name__ == '__main__':
    unittest.main()
