"""Isolated server admission regressions for manager-update steers."""
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest import mock

from agent_chat.bridge_state import BridgeState
from agent_chat.core import CoordError, Coordinator


class ManagerUpdateStateTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix='manager-update-')
        self.addCleanup(tmp.cleanup)
        path = Path(tmp.name) / 'state.sqlite3'
        self.worker = Coordinator(path, 'worker'); self.worker.register('worker'); self.addCleanup(self.worker.close)
        self.po = Coordinator(path, 'po'); self.po.register('po'); self.addCleanup(self.po.close)
        self.state = BridgeState(self.worker)
        self.thread = str(uuid.uuid4()); self.state.bind(thread_id=self.thread)
        with self.worker.tx() as db:
            db.execute('CREATE TABLE project_orchestrators (session_id TEXT PRIMARY KEY, root_session TEXT NOT NULL)')
            db.execute("INSERT INTO project_orchestrators VALUES ('po','root')")
            db.execute('CREATE TABLE agent_management (target_session TEXT PRIMARY KEY, manager_session TEXT NOT NULL, request_id TEXT NOT NULL)')
            db.execute("INSERT INTO agent_management VALUES ('worker','po','fixture')")
            db.execute('''CREATE TABLE project_work (id TEXT PRIMARY KEY, project_id TEXT, kind TEXT, goal_id TEXT,
                source_request_id TEXT UNIQUE, owner_session TEXT, owner_role TEXT, assigned_by TEXT, title TEXT,
                state TEXT, created_at REAL, updated_at REAL)''')
        self.assignment = self.po.send('worker', 'Original assignment')['id']; self.original = 'original-job'
        with self.worker.tx() as db:
            db.execute('INSERT INTO bridge_jobs VALUES (?,?,?,?,?,?,?,?)', (self.original,self.thread,'old','dispatched',None,None,1,1))
            db.execute('INSERT INTO bridge_deliveries VALUES (?,?)', (self.assignment,self.original))
            db.execute('INSERT INTO project_work VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
                       ('task','default','task','goal',self.assignment,'worker','worker','po','Task','in_progress',1,1))

    def update(self, sender=None, recipient='worker'):
        message = (sender or self.po).send(recipient, 'Update')['id']
        with self.worker.tx() as db: db.execute('INSERT INTO manager_message_updates VALUES (?,?)', (message,'task'))
        return message

    def raw_job(self, job_id):
        return dict(self.worker.db.execute('SELECT * FROM bridge_jobs WHERE id=?', (job_id,)).fetchone())

    def test_candidate_claim_epoch_and_cancel_preserve_source(self):
        update = self.update()
        self.assertTrue(next(m for m in self.state.pending() if m['id']==update)['manager_update_candidate'])
        candidate = self.state.manager_update_candidate(self.thread,update)
        self.assertEqual(candidate['original_job_id'],self.original)
        with mock.patch.object(self.state,'job',side_effect=self.raw_job):
            job=self.state.manager_update_claim(self.thread,update,'task',self.original,'turn','payload')
        self.assertIsNone(self.state.manager_update_claim(self.thread,update,'task',self.original,'turn','payload'))
        with self.assertRaisesRegex(CoordError,'requires turn/steer'):
            self.state.manager_update_epoch_check(self.thread,job['id'],'worker','thread/start')
        with self.worker.tx() as db:
            db.execute("UPDATE bridge_jobs SET status='cancelled' WHERE id=?", (job['id'],))
        self.assertFalse(any(m['id']==update for m in self.state.pending()))
        self.assertTrue(self.worker.db.execute('SELECT 1 FROM manager_update_claims WHERE message_id=?',(update,)).fetchone())
        later=self.po.send('worker','Unrelated later')['id']
        self.assertIn(later,[m['id'] for m in self.state.pending()])

    def test_fifo_wrong_sender_and_other_recipient_are_rejected(self):
        update=self.update()
        with self.worker.tx() as db: db.execute('INSERT INTO bridge_jobs VALUES (?,?,?,?,?,?,?,?)', ('older',self.thread,'x','prepared',None,None,2,2))
        self.assertIsNone(self.state.manager_update_candidate(self.thread,update))
        self.state.update('older','cancelled')
        other=Coordinator(self.worker.path,'other'); other.register('other'); self.addCleanup(other.close)
        self.assertIsNone(self.state.manager_update_candidate(self.thread,self.update(sender=other)))
        self.assertIsNone(self.state.manager_update_candidate(self.thread,self.update(recipient='po')))

    def test_lost_po_earlier_inbox_and_complete_read_withhold_candidate(self):
        earlier = self.po.send('worker', 'Earlier ordinary inbox')['id']
        update = self.update()
        self.assertIsNone(self.state.manager_update_candidate(self.thread, update))
        self.worker.message(earlier)
        self.assertIsNotNone(self.state.manager_update_candidate(self.thread, update))
        self.worker.message(update)
        self.assertIsNone(self.state.manager_update_candidate(self.thread, update))
        second = self.update()
        with self.worker.tx() as db:
            db.execute("UPDATE project_orchestrators SET session_id='replacement'")
        self.assertIsNone(self.state.manager_update_candidate(self.thread, second))

    def test_silent_batch_and_archived_earlier_history_do_not_block_update(self):
        silent = self.po.send('worker', 'Silent group history')['id']
        with self.worker.tx() as db:
            db.execute('INSERT INTO message_batches VALUES (?,?)', (silent, 'quiet-batch'))
        update = self.update()
        self.assertIsNotNone(self.state.manager_update_candidate(self.thread, update))
        self.worker.message(update)

        archived = self.po.send('worker', 'Archived history')['id']
        with self.worker.tx() as db:
            db.execute('UPDATE messages SET acked_at=1 WHERE id=?', (archived,))
            db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES ('message_delivery_archive_before','2')")
        later = self.update()
        self.assertIsNotNone(self.state.manager_update_candidate(self.thread, later))


if __name__ == '__main__': unittest.main()
