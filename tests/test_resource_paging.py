import pathlib
import sys
import tempfile
import time
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / 'src'))
from agent_chat.core import Coordinator
from agent_chat.web import snapshot


class ResourcePagingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='agent-chat-resource-pages-')
        self.db = pathlib.Path(self.tmp.name) / 'state.sqlite3'
        c = Coordinator(self.db, 'owner')
        c.register('owner')
        now = time.time()
        c.db.executemany('''INSERT INTO resources
            (name,owner_session,reservation_id,token,granted_at,deadline,stale,reason)
            VALUES(?,?,?,?,?,?,?,?)''', [
                (f'file:out/{i:04}.txt', 'owner' if i < 80 else None,
                 f'claim-{i}' if i < 80 else None, 'private-fixture-token' if i < 80 else None,
                 now if i < 80 else None, now+3600 if i < 30 else now-3600 if i < 80 else None,
                 int(30 <= i < 80), None) for i in range(130)])
        c.db.execute("INSERT INTO resource_queue(resource,session,queued_at) VALUES('file:out/0001.txt','owner',?)", (now,))
        self.before = list(c.db.execute('SELECT * FROM resources ORDER BY name'))
        c.close()

    def tearDown(self):
        self.tmp.cleanup()

    def test_pages_keep_totals_and_every_exact_resource_without_mutation(self):
        pages = [snapshot(self.db, resource_limit=50, resource_offset=offset) for offset in (0, 50, 100)]
        rows = [row for page in pages for row in page['resources']]
        self.assertEqual(len(rows), 130)
        self.assertEqual(len({row['resource'] for row in rows}), 130)
        self.assertEqual([len(page['resources']) for page in pages], [50, 50, 30])
        for page in pages:
            self.assertEqual((page['resource_page']['held'], page['resource_page']['stale'],
                              page['resource_page']['free'], page['resource_page']['waiting']), (80, 50, 50, 1))
        self.assertEqual(rows[0]['state'], 'owned')
        self.assertEqual(rows[30]['state'], 'stale')
        self.assertEqual(rows[80]['state'], 'free')
        self.assertEqual(rows[1]['queue'][0]['session'], 'owner')
        c = Coordinator(self.db)
        try:
            self.assertEqual([tuple(row) for row in c.db.execute('SELECT * FROM resources ORDER BY name')],
                             [tuple(row) for row in self.before])
        finally:
            c.close()

    def test_removed_final_page_clamps_to_remaining_page(self):
        result = snapshot(self.db, resource_limit=50, resource_offset=10000)
        self.assertEqual(result['resource_page']['offset'], 100)
        self.assertEqual(len(result['resources']), 30)

    def test_legacy_snapshot_remains_complete(self):
        result = snapshot(self.db)
        self.assertEqual(len(result['resources']), 130)
        self.assertNotIn('resource_page', result)

    def test_invalid_page_bounds(self):
        for limit, offset in [(0, 0), (201, 0), (True, 0), (50, -1), (50, True)]:
            with self.subTest(limit=limit, offset=offset), self.assertRaises(ValueError):
                snapshot(self.db, resource_limit=limit, resource_offset=offset)
