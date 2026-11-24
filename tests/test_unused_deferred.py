import contextlib
import sqlite3

from test_remote_web import RemoteWebFixture
from agent_chat.remote import RemoteCoordError

class DeferredReservationTests(RemoteWebFixture):
    def test_deferred_request_is_rejected_without_reservation_or_queue(self):
        self.call('register', agent='worker')
        with self.assertRaisesRegex(RemoteCoordError, 'deferred reservation is not supported'):
            self.call('request', resource='deferred-example', minutes=5, deferred=True)
        with contextlib.closing(sqlite3.connect(self.db)) as db:
            self.assertEqual(db.execute('select count(*) from resources where name=?',('deferred-example',)).fetchone()[0],0)
            self.assertEqual(db.execute('select count(*) from resource_queue where resource=?',('deferred-example',)).fetchone()[0],0)
    def test_ordinary_grant_remains_available(self):
        self.call('register', agent='worker')
        claim=self.call('request', resource='ordinary-example', minutes=5)
        self.assertEqual(claim['state'],'owned')
        self.assertTrue(claim['token'])
