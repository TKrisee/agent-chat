"""Synthetic offers require full policy; legacy receipts cannot prove their bytes."""
import hashlib
import sqlite3
import subprocess
import tempfile
import unittest
from pathlib import Path
from agent_chat import communication
from agent_chat.core import CoordError
from agent_chat.policy_store import PolicyStore

class PolicyEvidenceIntegrity(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.store=PolicyStore(Path(self.tmp.name)/'policy.sqlite3')
        self.version=communication.policy_version()
        self.metadata=subprocess.run(['jq','-nc','--arg','version',self.version,'{policy_version:$version}'],text=True,capture_output=True,check=True).stdout.strip()
        self.standing=communication.STANDING_POLICY
        self.auth=communication.BOOT_AUTHORIZATION
        self.assertIn('An assigned goal',self.auth)
    def payload(self,kind,authorization):
        prefix={'wake':'','root':'Root preamble.\n','reset':'Reset preamble. '}[kind]
        return prefix+self.standing+' '+authorization+'\n'+self.metadata+'\nSynthetic task'
    def test_all_kinds_require_complete_versioned_authorization_and_exact_receipt(self):
        for kind in ('wake','root','reset'):
            for case,auth in [('missing',''),('altered',self.auth.replace('An assigned goal','An altered goal',1)),('truncated',self.auth[:-1])]:
                with self.subTest(kind=kind,case=case):
                    client=kind+'-'+case
                    self.assertFalse(self.store.offer('host','project',kind,client,kind,self.payload(kind,auth))['offered'])
                    with self.assertRaises(CoordError): self.store.receipt('host',kind,client,self.version,'turn',project='project')
            body=self.payload(kind,self.auth); client=kind+'-valid'
            self.assertTrue(self.store.offer('host','project',kind,client,kind,body)['offered'])
            offered=next(x for x in self.store.pending('host',project='project') if x['client_id']==client)
            self.assertEqual(offered['payload_sha256'],hashlib.sha256(body.encode()).hexdigest())
            for h,t,c in [('wrong',kind,client),('host','wrong',client),('host',kind,'wrong')]:
                with self.assertRaises(CoordError): self.store.receipt(h,t,c,self.version,'turn',project='project')
            with self.assertRaises(CoordError): self.store.receipt('host',kind,client,'wrong-version','turn',project='project')
            self.assertFalse(self.store.consumed('host',kind,project='project'))
            self.assertTrue(self.store.receipt('host',kind,client,self.version,'turn',project='project')['consumed'])
            self.assertTrue(self.store.consumed('host',kind,project='project'))
            self.assertTrue(self.store.receipt('host',kind,client,self.version,'turn',project='project')['consumed'])
            with self.assertRaises(CoordError): self.store.receipt('host',kind,client,self.version,'another-turn',project='project')
    def test_legacy_receipt_remains_immutable_but_does_not_prove_complete_policy(self):
        with self.store.connection() as db:
            db.execute('INSERT INTO native_policy_offers VALUES(?,?,?,?,?,?,?,?)',('host','thread','legacy',self.version,'project','wake','unknown-body-digest',1))
            db.execute('INSERT INTO native_policy_receipts VALUES(?,?,?,?,?,?)',('host','thread',self.version,'legacy','turn-old',2))
        self.assertFalse(self.store.consumed('host','thread',project='project'))
        self.assertEqual(self.store.pending('host',project='project'),[])
        with self.store.connection() as db:
            self.assertEqual(db.execute('SELECT client_id,turn_id FROM native_policy_receipts').fetchone()[:],('legacy','turn-old'))
            with self.assertRaises(sqlite3.IntegrityError): db.execute('DELETE FROM native_policy_receipts')
        full=self.payload('wake',self.auth)
        with self.assertRaises(CoordError): self.store.offer('host','project','wake','legacy','thread',full)
        self.assertTrue(self.store.offer('host','project','wake','new','thread',full)['offered'])
        self.assertFalse(self.store.consumed('host','thread',project='project'))
        self.assertEqual([row['client_id'] for row in self.store.pending('host',project='project')],['new'])
        self.assertTrue(self.store.receipt('host','thread','new',self.version,'turn-new',project='project')['consumed'])
        self.assertTrue(self.store.consumed('host','thread',project='project'))
        with self.store.connection() as db:
            self.assertEqual(db.execute('SELECT client_id,turn_id FROM native_policy_receipts').fetchone()[:],('legacy','turn-old'))

    def test_explicit_project_scope_does_not_borrow_a_verified_receipt(self):
        body=self.payload('wake',self.auth)
        self.assertTrue(self.store.offer('host','project-a','wake','client','thread',body)['offered'])
        self.assertTrue(self.store.receipt('host','thread','client',self.version,'turn',project='project-a')['consumed'])
        self.assertTrue(self.store.consumed('host','thread',project='project-a'))
        self.assertFalse(self.store.consumed('host','thread'))
        self.assertEqual(self.store.pending('host'),[])
        with self.assertRaises(CoordError):
            self.store.receipt('host','thread','client',self.version,'turn')
        self.assertFalse(self.store.consumed('host','thread',project='project-b'))
        self.assertEqual(self.store.pending('host',project='project-b'),[])
        with self.assertRaises(CoordError):
            self.store.receipt('host','thread','client',self.version,'turn',project='project-b')
        self.assertTrue(self.store.offer('host','project-b','wake','client-b','thread',body)['offered'])
        self.assertFalse(self.store.consumed('host','thread',project='project-b'))
        self.assertEqual([row['client_id'] for row in self.store.pending('host',project='project-b')],['client-b'])
        self.assertTrue(self.store.receipt('host','thread','client-b',self.version,'turn-b',project='project-b')['consumed'])
        self.assertTrue(self.store.consumed('host','thread',project='project-b'))
