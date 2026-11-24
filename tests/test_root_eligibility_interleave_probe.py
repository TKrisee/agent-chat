import uuid
from unittest import mock
from test_root_pending_fresh import PendingFreshTests
from agent_chat.core import CoordError

class EligibilityInterleave(PendingFreshTests):
    def test_fresh_request_arrives_after_pause_guard(self):
        roots=self.fixture.roots
        inserted=[]
        def concurrent_request(root):
            row=self.request('created'); self.scope(row); self.gate(row,1); inserted.append(row['id'])
        with mock.patch.object(roots,'_routing_fresh',side_effect=concurrent_request):
            with self.assertRaisesRegex(CoordError,'pending'):
                roots.pause_routing('default','root','host',self.fixture.thread,str(uuid.uuid4()),True)
        self.assertTrue(inserted)
        self.assertEqual(roots.config()['mode'],'running', 'pause committed despite new pending fresh request')

def load_tests(loader,tests,pattern):
    return loader.loadTestsFromName('test_root_eligibility_interleave_probe.EligibilityInterleave.test_fresh_request_arrives_after_pause_guard')
