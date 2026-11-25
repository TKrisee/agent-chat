import base64
import contextlib
import sqlite3
import tempfile
import threading
import unittest
from unittest import mock
from pathlib import Path

from agent_chat.core import CoordError, Coordinator, first_ack_lookup
from agent_chat.remote_service import dispatch


class ReplyAcknowledgementTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="agent-chat-reply-ack-")
        self.db = Path(self.tmp.name) / "state.sqlite3"
        self.a = self.register("a")
        self.b = self.register("b")
        self.c = self.register("c")

    def tearDown(self):
        self.tmp.cleanup()

    def register(self, name):
        coord = Coordinator(self.db, name)
        try:
            return coord.register(name)["session"]
        finally:
            coord.close()

    def call(self, op, session, **params):
        coord = Coordinator(self.db)
        try:
            return dispatch(coord, {"op": op, "session": session, "host_id": "test-host", "params": params})
        finally:
            coord.close()

    def test_remote_reply_ack_is_atomic_and_retry_idempotent(self):
        parent = self.call("send", self.a, to=self.b, body="question")
        attachment = [{"name": "proof.png", "content_base64": base64.b64encode(b"\x89PNG\r\n\x1a\nproof").decode()}]
        reply = self.call("send", self.b, to=self.a, body="answer", attachments=attachment,
                          reply_to=parent["id"], ack_reply=True)
        retry = self.call("send", self.b, to=self.a, body="answer", attachments=attachment,
                          reply_to=parent["id"], ack_reply=True)
        self.assertEqual(reply["id"], retry["id"])
        self.assertTrue(retry["acknowledged_reply"])
        self.assertEqual(len(retry["attachments"]), 1)
        inbox = self.call("inbox", self.b)
        self.assertEqual([message["id"] for message in inbox["messages"]], [parent["id"]])
        self.assertIsNotNone(inbox["messages"][0]["acked_at"])
        self.call("message", self.b, id=parent["id"])
        self.assertEqual(self.call("inbox", self.b)["messages"], [])
        with self.assertRaisesRegex(CoordError, "differs"):
            self.call("send", self.b, to=self.a, body="changed", attachments=attachment,
                      reply_to=parent["id"], ack_reply=True)

    def test_invalid_atomic_reply_does_not_ack_original(self):
        parent = self.call("send", self.a, to=self.b, body="question")
        unrelated = self.call("send", self.a, to=self.c, body="other")
        with self.assertRaises(CoordError):
            self.call("send", self.b, to=self.a, body="bad", reply_to=unrelated["id"], ack_reply=True)
        inbox = self.call("inbox", self.b)
        self.assertEqual([message["id"] for message in inbox["messages"]], [parent["id"]])
        with self.assertRaisesRegex(CoordError, "requires reply_to"):
            self.call("send", self.b, to=self.a, body="bad", ack_reply=True)

    def test_ack_reply_requires_an_incoming_parent_and_attachment_failure_rolls_back(self):
        parent = self.call("send", self.a, to=self.b, body="question")
        with self.assertRaisesRegex(CoordError, "caller inbox"):
            self.call("send", self.a, to=self.b, body="invalid", reply_to=parent["id"], ack_reply=True)
        with sqlite3.connect(self.db) as db:
            db.execute("CREATE TRIGGER reject_attachment BEFORE INSERT ON attachments "
                       "BEGIN SELECT RAISE(ABORT, 'attachment failure'); END")
        attachment = [{"name": "proof.png", "content_base64": base64.b64encode(b"\x89PNG\r\n\x1a\nproof").decode()}]
        with self.assertRaisesRegex(sqlite3.Error, "attachment failure"):
            self.call("send", self.b, to=self.a, body="answer", attachments=attachment,
                      reply_to=parent["id"], ack_reply=True)
        with sqlite3.connect(self.db) as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM messages").fetchone()[0], 1)
            self.assertIsNone(db.execute("SELECT acked_at FROM messages WHERE id=?", (parent["id"],)).fetchone()[0])

    def test_first_ack_lookup_uses_source_direction_and_typed_association(self):
        self.call('register', self.a, agent='a')
        self.call('register', self.b, agent='b')
        source = self.call('send', self.b, to=self.a, body='private source')
        absent = first_ack_lookup(self.db, project_id='default', session_id=self.a,
                                  host_id='test-host', source_id=source['id'],
                                  expected_recipient=self.b)
        self.assertEqual(absent['status'], 'not_recorded_at_read')
        self.assertNotIn('body', absent)
        self.assertNotIn('fingerprint', absent)

        reply = self.call('send', self.a, to=self.b, body='private reply',
                          reply_to=source['id'], ack_reply=True)
        recorded = first_ack_lookup(self.db, project_id='default', session_id=self.a,
                                    host_id='test-host', source_id=source['id'],
                                    expected_recipient=self.b)
        self.assertEqual(recorded['status'], 'recorded')
        self.assertEqual(recorded['reply_id'], reply['id'])
        self.assertEqual(recorded['conversation_id'], source['id'])
        self.assertNotIn('body', recorded)
        self.assertNotIn('fingerprint', recorded)

        inverse = first_ack_lookup(self.db, project_id='default', session_id=self.b,
                                    host_id='test-host', source_id=source['id'],
                                    expected_recipient=self.a)
        self.assertEqual(inverse, {'status': 'unauthorized'})

    def test_first_ack_lookup_classifies_marker_only_and_dangling_rows(self):
        self.call('register', self.a, agent='a')
        self.call('register', self.b, agent='b')
        source = self.call('send', self.b, to=self.a, body='private source')
        with contextlib.closing(sqlite3.connect(self.db)) as db:
            with db:
                db.execute('UPDATE messages SET acked_at=1 WHERE id=?', (source['id'],))
        result = first_ack_lookup(self.db, project_id='default', session_id=self.a,
                                  host_id='test-host', source_id=source['id'],
                                  expected_recipient=self.b)
        self.assertEqual(result['status'], 'inconsistent')

        with contextlib.closing(sqlite3.connect(self.db)) as db:
            with db:
                db.execute('UPDATE messages SET acked_at=NULL WHERE id=?', (source['id'],))
                db.execute('INSERT INTO reply_ack_idempotency VALUES(?,?,?,?)',
                           (self.a, source['id'], 'never-return-this', 'missing-reply'))
        result = first_ack_lookup(self.db, project_id='default', session_id=self.a,
                                  host_id='test-host', source_id=source['id'],
                                  expected_recipient=self.b)
        self.assertEqual(result['status'], 'inconsistent')
        self.assertNotIn('fingerprint', result)

    def test_first_ack_lookup_requires_existing_host_and_project_context(self):
        self.call('register', self.a, agent='a')
        self.call('register', self.b, agent='b')
        source = self.call('send', self.b, to=self.a, body='private source')
        with contextlib.closing(sqlite3.connect(self.db)) as db:
            with db:
                db.execute("INSERT INTO meta(key,value) VALUES('project_id','default')")
        self.assertEqual(first_ack_lookup(self.db, project_id='other-project',
                                          session_id=self.a, host_id='test-host',
                                          source_id=source['id'], expected_recipient=self.b),
                         {'status': 'unauthorized'})
        self.assertEqual(first_ack_lookup(self.db, project_id='default',
                                          session_id=self.a, host_id='different-host',
                                          source_id=source['id'], expected_recipient=self.b),
                         {'status': 'unauthorized'})
        with contextlib.closing(sqlite3.connect(self.db)) as db:
            with db:
                db.execute('DELETE FROM remote_session_hosts WHERE session_id=?', (self.a,))
        self.assertEqual(first_ack_lookup(self.db, project_id='default',
                                          session_id=self.a, host_id='test-host',
                                          source_id=source['id'], expected_recipient=self.b),
                         {'status': 'unauthorized'})

    def test_first_ack_lookup_treats_link_mismatch_and_missing_schema_as_inconsistent_or_unavailable(self):
        self.call('register', self.a, agent='a')
        self.call('register', self.b, agent='b')
        source = self.call('send', self.b, to=self.a, body='source')
        reply = self.call('send', self.a, to=self.b, body='reply',
                          reply_to=source['id'], ack_reply=True)
        other = self.call('send', self.b, to=self.a, body='other source')
        with contextlib.closing(sqlite3.connect(self.db)) as db:
            with db:
                db.execute('UPDATE message_replies SET reply_to=? WHERE message_id=?',
                           (other['id'], reply['id']))
        result = first_ack_lookup(self.db, project_id='default', session_id=self.a,
                                  host_id='test-host', source_id=source['id'],
                                  expected_recipient=self.b)
        self.assertEqual(result['status'], 'inconsistent')

        empty = Path(self.tmp.name) / 'unsupported.sqlite3'
        with contextlib.closing(sqlite3.connect(empty)) as db:
            with db:
                db.execute('CREATE TABLE meta (key TEXT, value TEXT)')
                db.execute("INSERT INTO meta VALUES('schema_version','1')")
        result = first_ack_lookup(empty, project_id='default', session_id=self.a,
                                  host_id='test-host', source_id=source['id'],
                                  expected_recipient=self.b)
        self.assertEqual(result, {'status': 'unavailable'})

    def test_first_ack_lookup_read_snapshot_stays_truthful_during_concurrent_ack(self):
        import agent_chat.core as core

        self.call('register', self.a, agent='a')
        self.call('register', self.b, agent='b')
        source = self.call('send', self.b, to=self.a, body='source')
        original_connect = sqlite3.connect
        snapshot_ready = threading.Event()
        writer_done = threading.Event()
        result_holder = []

        class ReadConnection:
            def __init__(self, conn):
                self._conn = conn

            @property
            def row_factory(self):
                return self._conn.row_factory

            @row_factory.setter
            def row_factory(self, value):
                self._conn.row_factory = value

            def execute(self, sql, args=()):
                cursor = self._conn.execute(sql, args)
                if sql.startswith('SELECT id,sender_session,recipient_session,acked_at FROM messages WHERE id=?'):
                    snapshot_ready.set()
                    if not writer_done.wait(5):
                        raise AssertionError('concurrent ACK writer did not finish')
                return cursor

            def close(self):
                self._conn.close()

        def connect(path, *args, **kwargs):
            conn = original_connect(path, *args, **kwargs)
            if kwargs.get('uri') and isinstance(path, str) and path.startswith('file:') and 'mode=ro' in path:
                return ReadConnection(conn)
            return conn

        def read():
            result_holder.append(core.first_ack_lookup(
                self.db, project_id='default', session_id=self.a, host_id='test-host',
                source_id=source['id'], expected_recipient=self.b))

        with mock.patch.object(core.sqlite3, 'connect', side_effect=connect):
            reader = threading.Thread(target=read)
            reader.start()
            self.assertTrue(snapshot_ready.wait(5))
            try:
                reply = self.call('send', self.a, to=self.b, body='reply',
                                  reply_to=source['id'], ack_reply=True)
            finally:
                writer_done.set()
            reader.join(5)
        self.assertFalse(reader.is_alive())
        self.assertEqual(result_holder[0]['status'], 'not_recorded_at_read')
        self.assertIsNotNone(reply['id'])
        confirmed = first_ack_lookup(self.db, project_id='default', session_id=self.a,
                                     host_id='test-host', source_id=source['id'],
                                     expected_recipient=self.b)
        self.assertEqual(confirmed['status'], 'recorded')


if __name__ == "__main__":
    unittest.main()
