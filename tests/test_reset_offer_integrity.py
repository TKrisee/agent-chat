"""Synthetic reset offers bind full versioned policy and exact receipt route."""
import hashlib
import subprocess
import tempfile
import unittest
from pathlib import Path

from agent_chat import communication
from agent_chat.core import CoordError
from agent_chat.policy_store import PolicyStore


class ResetOfferIntegrity(unittest.TestCase):
    def test_full_policy_and_exact_receipt_route(self):
        with tempfile.TemporaryDirectory() as temp:
            store=PolicyStore(Path(temp)/'policy.sqlite3')
            version=communication.policy_version()
            metadata=subprocess.run(['jq','-nc','--arg','version',version,'{policy_version:$version}'],text=True,capture_output=True,check=True).stdout.strip()
            prefix='Synthetic reset preamble. '
            coordination=communication.STANDING_POLICY
            authorization=communication.BOOT_AUTHORIZATION
            self.assertIn('An assigned goal',authorization)
            def payload(auth): return prefix+coordination+' '+auth+'\n'+metadata+'\nSynthetic task'
            for client,auth in [('missing',''),('altered',authorization+' altered'),('removed',authorization[:-1])]:
                body=payload(auth)
                if client=='altered': body=payload(authorization.replace('An assigned goal', 'An altered goal',1))
                self.assertFalse(store.offer('host-a','project','reset',client,'thread-a',body)['offered'],client)
                with self.assertRaises(CoordError): store.receipt('host-a','thread-a',client,version,'turn-x',project='project')
            full=payload(authorization)
            self.assertTrue(store.offer('host-a','project','reset','client-a','thread-a',full)['offered'])
            offer=store.pending('host-a',project='project')[0]
            self.assertEqual(offer['payload_sha256'],hashlib.sha256(full.encode()).hexdigest())
            for host,thread,client in [('host-b','thread-a','client-a'),('host-a','thread-b','client-a'),('host-a','thread-a','client-b')]:
                with self.assertRaises(CoordError): store.receipt(host,thread,client,version,'turn-x',project='project')
            self.assertFalse(store.consumed('host-a','thread-a',project='project'))
            self.assertTrue(store.receipt('host-a','thread-a','client-a',version,'turn-x',project='project')['consumed'])
            self.assertTrue(store.consumed('host-a','thread-a',project='project'))
