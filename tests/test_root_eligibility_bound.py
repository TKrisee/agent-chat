from test_root_pending_fresh import PendingFreshTests
from agent_chat.root_pending_fresh import routing_fresh_blocker

class TerminalBound(PendingFreshTests):
    def test_active_rows_cannot_hide_unresolved_terminal(self):
        for i in range(65):
            self.request('created',id='a%03d'%i)
        self.request('retired',id='z-terminal')
        with self.fixture.projects.connection() as db:
            self.assertIn('terminal eligibility is unresolved',routing_fresh_blocker(db,self.fixture.master,self.fixture.roots.config(),terminal_only=True))

def load_tests(loader,tests,pattern):
    return loader.loadTestsFromName('test_root_eligibility_bound.TerminalBound.test_active_rows_cannot_hide_unresolved_terminal')
