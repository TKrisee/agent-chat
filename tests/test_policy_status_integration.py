"""Synthetic same-owner PolicyStore to Root status composition."""
import contextlib
import sqlite3
import subprocess
import unittest
from unittest.mock import patch
import test_root_local as fixture
from agent_chat import communication, policy_store

class StatusIntegration(unittest.TestCase):
    setUp=fixture.RootLocalTests.setUp
    call=fixture.RootLocalTests.call
    enroll=fixture.RootLocalTests.enroll
    def test_new_verified_receipt_does_not_rewrite_unknown_legacy(self):
        self.enroll(); store=policy_store.PolicyStore(self.projects.path)
        version=communication.policy_version()
        with contextlib.closing(sqlite3.connect(self.projects.path)) as db:
            db.execute('INSERT INTO native_policy_offers VALUES(?,?,?,?,?,?,?,?)',('host',self.thread,'old',version,'default','wake','a'*64,1))
            db.execute('INSERT INTO native_policy_receipts VALUES(?,?,?,?,?,?)',('host',self.thread,version,'old','old-turn',2));db.commit()
        before=self.call('policy-effective-status',expected_thread=self.thread)
        self.assertEqual(before['effective_state'],'unknown')
        metadata=subprocess.run(['jq','-nc','--arg','version',version,'{policy_version:$version}'],text=True,capture_output=True,check=True).stdout.strip()
        def decode(text):
            value=subprocess.run(['jq','-er','.policy_version','-'],input=text,text=True,capture_output=True,check=True).stdout.strip()
            return {'policy_version':value}
        payload=communication.STANDING_POLICY+' '+communication.BOOT_AUTHORIZATION+'\n'+metadata
        with patch.object(policy_store.json,'loads',side_effect=decode):
            self.assertTrue(store.offer('host','default','wake','new',self.thread,payload)['offered'])
        self.assertTrue(store.receipt('host',self.thread,'new',version,'new-turn',project='default')['consumed'])
        after=self.call('policy-effective-status',expected_thread=self.thread)
        self.assertEqual(after['effective_state'],'verified_current')
        self.assertEqual([(x['client_id'],x['turn_id']) for x in after['historical_receipts']],[('old','old-turn')])
        self.assertEqual([(x['client_id'],x['turn_id']) for x in after['verified_receipts']],[('new','new-turn')])
        with contextlib.closing(sqlite3.connect(self.projects.path)) as db:
            self.assertEqual(db.execute('SELECT client_id,turn_id FROM native_policy_receipts').fetchall(),[('old','old-turn')])
