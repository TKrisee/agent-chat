from __future__ import annotations

import base64
import contextlib
import hashlib
from pathlib import Path
import tempfile
import unittest

from agent_chat.core import Coordinator, CoordError, MAX_ATTACHMENT_SIZE
from agent_chat.remote_service import dispatch


MAIL = (b'From: Example Sender <sender@example.invalid>\r\n'
        b'To: Example Recipient <recipient@example.invalid>\r\n'
        b'Subject: Synthetic exported message\r\n'
        b'MIME-Version: 1.0\r\n'
        b'Content-Type: text/plain; charset=iso-8859-1\r\n'
        b'Content-Transfer-Encoding: 8bit\r\n\r\n'
        b'caf\xe9\r\n')


class EmlAttachmentValidationTests(unittest.TestCase):
    def test_export_is_opaque_exact_and_labeled_as_rfc822(self):
        self.assertEqual(Coordinator._prepare_attachment('../synthetic.EML', MAIL),
                         ('synthetic.EML', 'message/rfc822', MAIL))

    def test_empty_oversized_and_unsupported_files_remain_rejected(self):
        with self.assertRaisesRegex(CoordError, 'nonempty'):
            Coordinator._prepare_attachment('empty.eml', b'')
        at_limit = b'x' * MAX_ATTACHMENT_SIZE
        self.assertEqual(Coordinator._prepare_attachment('at-limit.eml', at_limit),
                         ('at-limit.eml', 'message/rfc822', at_limit))
        with self.assertRaisesRegex(CoordError, '10 MiB'):
            Coordinator._prepare_attachment('oversized.eml', b'x' * (MAX_ATTACHMENT_SIZE + 1))
        with self.assertRaisesRegex(CoordError, 'unsupported attachment type'):
            Coordinator._prepare_attachment('other.msg', MAIL)
        self.assertEqual(Coordinator._prepare_attachment('notes.txt', b'plain text\n'),
                         ('notes.txt', 'text/plain', b'plain text\n'))
        with self.assertRaisesRegex(CoordError, 'UTF-8'):
            Coordinator._prepare_attachment('bad.txt', b'\xff')


class EmlRecipientDownloadTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='agent-chat-eml-private-')
        self.addCleanup(temporary.cleanup)
        self.db = Path(temporary.name) / 'state.sqlite3'
        for session in ('operator', 'recipient', 'outsider'):
            with contextlib.closing(Coordinator(self.db, session)) as coord:
                coord.register(session)
        with contextlib.closing(Coordinator(self.db, 'operator')) as coord:
            coord.db.execute("INSERT INTO operator_identities(session_id) VALUES('operator')")
            self.sent = coord._send_prepared('recipient', 'Synthetic exported message',
                [coord._prepare_attachment('synthetic.eml', MAIL)])

    def call(self, *, session='recipient', host='recipient-host', project='default', **params):
        with contextlib.closing(Coordinator(self.db)) as coord:
            return dispatch(coord, {'op':'attachment-download', 'session':session,
                'host_id':host, 'params':params}, canonical_project=project)

    def test_exact_recipient_project_message_and_host_receive_original_bytes(self):
        attachment = self.sent['attachments'][0]
        result = self.call(source_project='default', message_id=self.sent['id'],
                           attachment_id=attachment['id'])
        self.assertEqual(base64.b64decode(result['content_base64'], validate=True), MAIL)
        self.assertEqual(result['sha256'], hashlib.sha256(MAIL).hexdigest())
        self.assertEqual((result['name'], result['mime'], result['size'],
                          result['recipient_session'], result['project'], result['host_id']),
                         ('synthetic.eml', 'message/rfc822', len(MAIL),
                          'recipient', 'default', 'recipient-host'))

    def test_other_actor_project_host_or_message_is_denied(self):
        attachment_id = self.sent['attachments'][0]['id']
        with self.assertRaises(CoordError):
            self.call(session='outsider', host='outsider-host', source_project='default',
                      message_id=self.sent['id'], attachment_id=attachment_id)
        with self.assertRaises(CoordError):
            self.call(project='other-project', source_project='default',
                      message_id=self.sent['id'], attachment_id=attachment_id)
        with self.assertRaises(CoordError):
            self.call(source_project='default', message_id='message_wrong',
                      attachment_id=attachment_id)
        # The prior successful download bound the recipient to its owning host.
        with self.assertRaises(CoordError):
            self.call(host='wrong-host', source_project='default',
                      message_id=self.sent['id'], attachment_id=attachment_id)
        with self.assertRaises(CoordError):
            self.call(source_project='default', attachment_id=attachment_id)


if __name__ == '__main__':
    unittest.main()
