import subprocess
import unittest
from test_guard_prospective_recovery import ProspectiveRecovery

class GuardExportComposition(ProspectiveRecovery):
    def test_export_and_parent_loss_recovery_coexist(self):
        def check_export():
            data=self.cli_call(self.po,'activity-export')
            program='any(.work[]; .id==$task and .acceptance.customer_acceptance.state=="unknown") and any(.sources[]; .id==$source)'
            subprocess.run(['jq','-e','--arg','task',self.task['id'],'--arg','source',self.assignment,program],
                           input=data,text=True,capture_output=True,check=True)
        check_export()
        self.exercise_recovery('lost')
        check_export()

def load_tests(loader, tests, pattern):
    return unittest.TestSuite([GuardExportComposition('test_export_and_parent_loss_recovery_coexist')])
