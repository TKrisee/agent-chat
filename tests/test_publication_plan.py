import contextlib
import sqlite3
import tempfile
import unittest
from pathlib import Path
from agent_chat.core import Coordinator, CoordError
from agent_chat.publication_plan import Repository, Gitlink, Plan, decision_body, decision_state

class PublicationPlanTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        db = Path(self.tmp.name) / 'state.db'
        self.author = Coordinator(db, 'author'); self.addCleanup(self.author.close); self.author.register('Author')
        self.operator = Coordinator(db, 'operator'); self.addCleanup(self.operator.close); self.operator.register('Operator')
        self.other = Coordinator(db, 'other'); self.addCleanup(self.other.close); self.other.register('Other')
        with contextlib.closing(sqlite3.connect(db)) as conn, conn:
            conn.execute('INSERT INTO operator_identities(session_id) VALUES(?)', ('operator',))
            conn.execute('INSERT INTO operator_identities(session_id) VALUES(?)', ('other',))
        self.plan = Plan((Repository('parent','origin-parent','refs/heads/release','a'*40), Repository('child','origin-child','refs/heads/release','b'*40)), (Gitlink('parent','vendor/child','child','b'*40),))
    def question(self):
        return self.author.send('operator', decision_body(self.plan), decision_needed=True)['id']
    def answer(self, q, outcome):
        self.operator._operator_message_origin = True
        try: reply = self.operator.send('author', outcome, reply_to=q)
        finally: self.operator._operator_message_origin = False
        self.author.record_decision_answer(q, reply['id'], outcome)
    def test_exact_approval_and_changed_ref_or_gitlink(self):
        q = self.question(); self.assertEqual(decision_state(self.author, q, self.plan), 'pending')
        self.answer(q, 'accepted'); self.assertEqual(decision_state(self.author, q, self.plan), 'accepted')
        changed = Plan((Repository('parent','other-destination','refs/heads/release','a'*40), Repository('child','origin-child','refs/heads/release','b'*40)), self.plan.gitlinks)
        self.assertEqual(decision_state(self.author, q, changed), 'mismatch')
        changed_ref = Plan((self.plan.repositories[0], Repository('child','origin-child','refs/heads/other','b'*40)), self.plan.gitlinks)
        self.assertEqual(decision_state(self.author, q, changed_ref), 'mismatch')
        changed_effect = Plan(self.plan.repositories, (Gitlink('parent','other/path','child','b'*40),))
        self.assertEqual(decision_state(self.author, q, changed_effect), 'mismatch')
        self.assertEqual(decision_state(self.author, q, Plan(self.plan.repositories, ())), 'mismatch')
    def test_decline_and_wrong_actor(self):
        q = self.question(); self.answer(q, 'declined')
        self.assertEqual(decision_state(self.author, q, self.plan), 'declined')
        with self.assertRaises(CoordError): decision_state(self.other, q, self.plan)
    def test_missing_nested_repo_and_mutation_rejected(self):
        with self.assertRaises(ValueError): Plan((self.plan.repositories[0],), self.plan.gitlinks)
        with self.assertRaises((AttributeError, TypeError)): self.plan.repositories[0].ref = 'refs/heads/other'
        with self.assertRaises(ValueError): Plan(self.plan.repositories, (Gitlink('parent','vendor/child','child','c'*40),))

    def test_only_full_object_ids_and_changed_objects_invalidate(self):
        for value in ('HEAD', 'main', 'abc123', 'z'*40, 'a'*41):
            with self.subTest(value=value):
                with self.assertRaises(ValueError): Repository('parent','origin','refs/heads/main',value)
                with self.assertRaises(ValueError): Gitlink('parent','vendor/child','child',value)
        self.assertEqual(Repository('sha256','origin','refs/heads/main','a'*64).commit,'a'*64)
        q = self.question(); self.answer(q,'accepted')
        changed_parent = Plan((Repository('parent','origin-parent','refs/heads/release','c'*40),self.plan.repositories[1]),self.plan.gitlinks)
        self.assertEqual(decision_state(self.author,q,changed_parent),'mismatch')
        changed_child = Plan((self.plan.repositories[0],Repository('child','origin-child','refs/heads/release','c'*40)),(Gitlink('parent','vendor/child','child','c'*40),))
        self.assertEqual(decision_state(self.author,q,changed_child),'mismatch')

    def test_agent_reply_does_not_approve_and_conditional_is_pending(self):
        q = self.question()
        with self.assertRaises(CoordError): self.other.send('author', 'accepted', reply_to=q)
        self.assertEqual(decision_state(self.author, q, self.plan), 'pending')
        self.answer(q, 'conditional')
        self.assertEqual(decision_state(self.author, q, self.plan), 'pending')

if __name__ == '__main__': unittest.main()
