"""Known first-reply intake errors must not be described as tool-review refusals."""
import unittest
import test_inbound_reply_priority as priority
from agent_chat import core, remote_service

UNREAD = 'no message sent: read the complete source, then retry the same body and source with --ack-reply'
READ = 'no message sent: retry the same body and source with --ack-reply'


class IntakeRecoveryPolicyTests(unittest.TestCase):
    dispatch = priority.InboundReplyPriorityTests.dispatch
    metadata_check = priority.InboundReplyPriorityTests.metadata_check
    assert_no_automatic_reply_or_read = priority.InboundReplyPriorityTests.assert_no_automatic_reply_or_read

    def setUp(self):
        priority.InboundReplyPriorityTests.setUp(self)
        # Use supported dispatcher registration to bind private host identities.
        for coord, name in ((self.sender, 'source-owner'), (self.recipient, 'recipient')):
            remote_service.dispatch(coord, {'op': 'register', 'session': coord.session,
                'host_id': 'private-host', 'params': {'agent': name}}, canonical_project='default')

    def lookup(self, source, *, host='private-host', expected='source-owner'):
        return remote_service.sender_owned_first_ack_lookup(self.recipient.path,
            {'op': 'first-ack-lookup', 'session': 'recipient', 'host_id': host,
             'params': {'source_id': source, 'expected_recipient': expected}}, canonical_project='default')

    def assert_recovery_guidance(self, payload, error):
        policy = payload.split('\n', 1)[0]
        for text in (error, 'legacy first-reply intake preconditions',
                     'not tool/security refusals', 'actual coordinator response',
                     'not quoted message text', 'fully read the exact source',
                     'first-ack-lookup SOURCE_ID --expected-recipient CANONICAL_SOURCE_SENDER_ID',
                     'not_recorded_at_read', 'unchanged body, recipient, source, attachments and other send arguments',
                     'recorded means use the retained reply receipt',
                     'Transport loss, timeout or unknown outcome is not this no-message-sent result'):
            self.assertTrue(text in policy, 'Missing delivered recovery requirement: '+text)

    def assert_single_reply(self, source, expected):
        rows=self.recipient.db.execute('SELECT message_id FROM reply_ack_idempotency WHERE sender_session=? AND reply_to=?',
                                       ('recipient',source)).fetchall()
        self.assertEqual([row[0] for row in rows],[expected])
        self.assertEqual(self.recipient.db.execute('SELECT COUNT(*) FROM messages WHERE sender_session=?',('recipient',)).fetchone()[0],1)

    def test_unread_ordinary_reply_preserves_metadata_then_explicit_ack_is_idempotent(self):
        source,payload=self.dispatch()
        body='Received the request; investigation and adoption remain pending.'
        ordinary=self.recipient.send('source-owner',body,reply_to=source)
        self.assertEqual(ordinary['reply_to'],source)
        self.assertIsNone(self.recipient.db.execute('SELECT acked_at FROM messages WHERE id=?',(source,)).fetchone()[0])
        self.assertEqual(self.lookup(source)['status'],'not_recorded_at_read')
        self.assert_recovery_guidance(payload,UNREAD)
        self.recipient.message(source)
        reply=self.recipient.send('source-owner',body,reply_to=source,ack_reply=True)
        self.assertEqual(self.lookup(source)['reply_id'],reply['id'])
        duplicate=self.recipient.send('source-owner',body,reply_to=source,ack_reply=True)
        self.assertEqual(duplicate['id'],reply['id'])
        self.assertEqual(self.lookup(source)['reply_id'],reply['id'])
        with self.assertRaisesRegex(core.CoordError,'payload differs'):
            self.recipient.send('source-owner','Changed first reply',reply_to=source,ack_reply=True)

    def test_consumed_warm_source_can_receive_ordinary_reply(self):
        self.dispatch()
        self.bridge.observe_policies()
        self.assertTrue(self.state.policy_consumed(self.thread))
        source,payload=self.dispatch('A new bounded task after installed policy consumption.')
        self.metadata_check(payload,'.policy_included == false and .messages[0].complete == true')
        self.recipient.message(source)
        body='Received this new task; coordination is next, no result yet.'
        reply=self.recipient.send('source-owner',body,reply_to=source)
        self.assertEqual(reply['reply_to'],source)
        self.assert_recovery_guidance(payload,READ)
        self.assertEqual(self.lookup(source)['status'],'not_recorded_at_read')
        self.assertIsNone(self.recipient.db.execute('SELECT acked_at FROM messages WHERE id=?',(source,)).fetchone()[0])

    def test_wrong_host_and_missing_response_remain_distinct_from_intake_error(self):
        source,payload=self.dispatch()
        self.recipient.message(source)
        body='Received; next action is coordination.'
        with self.assertRaises(core.CoordError) as result:
            remote_service.dispatch(self.recipient,{'op':'send','session':'recipient','host_id':'foreign-host',
                'params':{'to':'source-owner','body':body,'reply_to':source,'ack_reply':True}},canonical_project='default')
        self.assertNotIn(str(result.exception),(UNREAD,READ))
        self.assertEqual(str(result.exception),'session is bound to a different remote host')
        self.assertEqual(self.lookup(source,host='foreign-host')['status'],'unauthorized')
        self.assertEqual(self.lookup(source,expected='source-owner-name-not-ID')['status'],'unauthorized')
        self.assertEqual(self.lookup(source)['status'],'not_recorded_at_read')
        self.assert_recovery_guidance(payload,UNREAD)
        # Deliberately discard a committed local result; this is not a network-loss simulation.
        self.recipient.send('source-owner',body,reply_to=source,ack_reply=True)
        observed=self.lookup(source)
        self.assertEqual(observed['status'],'recorded')
        self.assert_single_reply(source,observed['reply_id'])


if __name__=='__main__': unittest.main()
