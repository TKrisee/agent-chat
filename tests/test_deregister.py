import json
import sqlite3
import tempfile
import unittest
import uuid
from pathlib import Path

from agent_chat.core import Coordinator, CoordError
from agent_chat.bridge_state import BridgeState
from agent_chat.web import snapshot


class DeregisterTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='agent-chat-deregister-')
        self.addCleanup(self.tmp.cleanup)
        self.path=Path(self.tmp.name)/'state.sqlite3'
        self.owner=self.coord('owner'); self.worker=self.coord('worker')
        self.state=BridgeState(self.worker)
        self.thread=str(uuid.uuid4()); self.state.bind(thread_id=self.thread)
        Path(str(self.path)+'.web-session.json').write_text(json.dumps({'id':'owner'}))

    def coord(self,sid):
        c=Coordinator(self.path,sid); c.register(sid); self.addCleanup(c.close); return c

    def test_clean_bound_leaf_removes_binding_preserves_history_and_rejects_reuse(self):
        self.worker.send('owner','Done')
        result=self.worker.remove_session('worker')
        self.assertTrue(result['removed'])
        self.assertEqual(snapshot(self.path)['messages'][0]['sender_agent'],'worker')
        self.assertEqual(self.owner.inbox()['messages'][0]['sender_agent'],'worker')
        self.assertIsNone(self.worker.db.execute("SELECT 1 FROM bridge_bindings WHERE session_id='worker'").fetchone())
        with self.assertRaises(CoordError): self.worker.register('worker')
        with self.assertRaises(sqlite3.IntegrityError):
            self.worker.db.execute("INSERT INTO sessions(id,agent,registered_at) VALUES('worker','legacy client',0)")

    def test_parent_cannot_be_removed_before_child_and_child_route_job_blocks_removal(self):
        child=self.coord('child'); child_state=BridgeState(child)
        child_state.bind(parent_session='worker',agent_path='/root/child')
        with self.assertRaisesRegex(CoordError,'descendant'): self.owner.remove_session('worker')
        message=self.owner.send('worker','Parent task')
        job=self.state.prepare(self.thread,[{'id':message['id'],'recipient_session':'worker'}],'payload')
        with self.assertRaisesRegex(CoordError,'unresolved'): child.remove_session('child')
        self.state.update(job['id'],'cancelled')
        child.remove_session('child'); self.owner.remove_session('worker')

    def test_unrelated_thread_job_does_not_block_clean_removal(self):
        other=self.coord('other'); otherstate=BridgeState(other)
        tid=str(uuid.uuid4()); otherstate.bind(thread_id=tid)
        message=self.owner.send('other','Task')
        otherstate.prepare(tid,[{'id':message['id'],'recipient_session':'other'}],'payload')
        self.worker.remove_session('worker')
        self.assertEqual(len(otherstate.jobs()),1)

    def test_batch_history_retains_removed_delivery_identity(self):
        other=self.coord('other')
        self.owner.send_many(['worker','other'],'For both')
        self.worker.remove_session('worker')
        inbox=other.inbox()['messages'][0]
        self.assertEqual({d['recipient_agent'] for d in inbox['deliveries']},{'worker','other'})
