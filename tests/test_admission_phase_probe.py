import unittest
from test_native02_admission import Native02Tests
from synthetic_successor_allowance import allowance
from agent_chat import root_local, successor_admission as successor
from agent_chat.core import CoordError

class PhaseProbe(Native02Tests):
    def test_refusal_reports_phase_and_exact_predicate(self):
        self.free_heavy(); self.build(); before=self.evidence()
        with allowance(self.root,self.master,self.registry), self.assertRaises(CoordError) as captured:
            root_local.execute('capacity-successor-native02',{'confirm':True},master_db=self.master,service_plist=self.config.plist,home_project='default',session=successor.ROOT,host_id=successor.HOST)
        self.assertIn('request_id='+successor.NATIVE02_REQUEST,str(captured.exception))
        self.assertIn('phase=transaction_started; predicate=successor_packet_pending',str(captured.exception))
        self.assertIn('no replay',str(captured.exception))
        self.assertIn('readback',str(captured.exception))
        self.assertEqual(self.evidence(),before)

def load_tests(loader, tests, pattern):
    return unittest.TestSuite([PhaseProbe('test_refusal_reports_phase_and_exact_predicate')])
