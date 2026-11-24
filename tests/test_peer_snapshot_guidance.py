import unittest
import test_peer_assignment_context as fixture

class SnapshotGuidanceTests(fixture.PeerAssignmentContextTests):
    def test_owned_runnable_task_gets_guidance_only(self):
        peer, _ = self.peer()
        snapshot = self.po_call('activity-snapshot', session=self.worker)
        guidance = snapshot['assignment_provenance_guidance']
        for phrase in ('fully read', 'current owned task', 'same-team peer', '--assignment-id', 'same ID or absence'):
            self.assertIn(phrase, guidance)
        self.assertNotIn('Own one new private exact-file snapshot task.', guidance)
        self.assertNotIn('assignment_provenance_guidance', self.po_call('activity-snapshot', session=peer))
        self.po_call('activity-transition', session=self.worker, work_id=self.task['id'], state='blocked')
        self.assertNotIn('assignment_provenance_guidance', self.po_call('activity-snapshot', session=self.worker))

def load_tests(loader, tests, pattern):
    return unittest.TestSuite([SnapshotGuidanceTests('test_owned_runnable_task_gets_guidance_only')])
