import contextlib
import sqlite3
import uuid
from unittest import mock
from agent_chat import root_local
from agent_chat.root_restart import RootRestart
from agent_chat.core import CoordError
from agent_chat.root_orchestrator import RootOrchestrator
from test_root_pending_fresh import PendingFreshTests

RID='876fd5a8-8966-411e-8236-47df1fc8bf99'
CID='e7e2075c-4ab8-4791-a952-e3321c505c03'
CANDIDATE='01a148f4-3358-79b3-b487-4210622bf57b'
class EligibilityLocalProbe(PendingFreshTests):
    def preflight(self):
        with mock.patch.object(RootOrchestrator, '_routing_inspection', return_value=None):
            return root_local._preflight(self.fixture.master, self.fixture.plist, 'default', 'root', 'host', 'pause-routing', {'expected_thread': self.fixture.thread})
    def cancelled(self, interrupt=False):
        error=('Candidate recovery required: exact retained thread is not materialized: RPC error -32600: no rollout found for thread id '+CANDIDATE)
        row=self.request('created',id=RID,native_action='creating',new_thread_id=CANDIDATE,native_proof_json=None,error=error)
        self.scope(row); self.gate(row,1)
        if interrupt:
            with contextlib.closing(sqlite3.connect(self.registry)) as db, db:
                db.execute("CREATE TRIGGER injected_interrupt BEFORE UPDATE ON root_fresh_pending_cancellations WHEN NEW.state='cancelled' BEGIN SELECT RAISE(ABORT,'injected finalization interruption'); END")
        restart=RootRestart(self.fixture.roots)
        if interrupt:
            with self.assertRaises(sqlite3.IntegrityError): restart.cancel_pending('default','root','host',self.fixture.thread,RID,CID,True)
        else:
            result=restart.cancel_pending('default','root','host',self.fixture.thread,RID,CID,True)
            self.assertEqual(result['pending_cancellation']['state'],'cancelled')
    def test_finalized_terminal_gate_does_not_report_pending(self):
        self.cancelled(); self.preflight()
    def test_interrupted_terminal_is_typed_unresolved(self):
        self.cancelled(True)
        with self.assertRaisesRegex(CoordError,'terminal.*unresolved'): self.preflight()
    def test_finalized_cancellation_allows_actual_routing_pause(self):
        self.cancelled()
        with mock.patch.object(RootOrchestrator, '_routing_inspection', return_value=None):
            result=self.fixture.roots.pause_routing('default','root','host',self.fixture.thread,str(uuid.uuid4()),True)
        self.assertEqual(result['status'],'paused')
    def test_interrupted_cancellation_withholds_actual_routing_pause(self):
        self.cancelled(True)
        with self.assertRaisesRegex(CoordError,'terminal.*unresolved'):
            self.fixture.roots.pause_routing('default','root','host',self.fixture.thread,str(uuid.uuid4()),True)
        self.assertEqual(self.fixture.roots.config()['mode'],'running')
    def test_finished_audit_with_changed_candidate_is_unknown(self):
        self.cancelled()
        with contextlib.closing(sqlite3.connect(self.registry)) as db, db:
            db.execute("UPDATE root_fresh_requests SET new_thread_id=? WHERE id=?", (str(uuid.uuid4()),RID))
        with self.assertRaisesRegex(CoordError,'terminal.*unresolved'): self.preflight()
    def test_finished_audit_with_active_gate_is_unknown(self):
        self.cancelled()
        with contextlib.closing(sqlite3.connect(self.fixture.master)) as db, db:
            db.execute("UPDATE root_fresh_gates SET active=1 WHERE request_id=?",(RID,))
        with self.assertRaisesRegex(CoordError,'terminal.*unresolved'): self.preflight()
    def test_unrelated_retired_status_without_audit_is_unknown(self):
        row=self.request('retired'); self.scope(row); self.gate(row,0)
        with self.assertRaisesRegex(CoordError,'terminal.*unresolved'): self.preflight()
    def test_terminal_only_keeps_separate_active_quarantine_path(self):
        from agent_chat.root_pending_fresh import routing_fresh_blocker
        self.cancelled()
        row=self.request('created'); self.scope(row); self.gate(row,1)
        with self.fixture.projects.connection() as db:
            self.assertIsNone(routing_fresh_blocker(db,self.fixture.master,self.fixture.roots.config(),terminal_only=True))
            self.assertIn('pending',routing_fresh_blocker(db,self.fixture.master,self.fixture.roots.config()))
    def test_active_remains_pending(self):
        row=self.request('created'); self.scope(row); self.gate(row,1)
        with self.assertRaisesRegex(CoordError,'pending'): self.preflight()

def load_tests(loader,tests,pattern):
    return loader.loadTestsFromNames(['test_root_eligibility_local_probe.EligibilityLocalProbe.test_terminal_only_keeps_separate_active_quarantine_path','test_root_eligibility_local_probe.EligibilityLocalProbe.test_finished_audit_with_changed_candidate_is_unknown','test_root_eligibility_local_probe.EligibilityLocalProbe.test_finished_audit_with_active_gate_is_unknown','test_root_eligibility_local_probe.EligibilityLocalProbe.test_unrelated_retired_status_without_audit_is_unknown','test_root_eligibility_local_probe.EligibilityLocalProbe.test_finalized_cancellation_allows_actual_routing_pause','test_root_eligibility_local_probe.EligibilityLocalProbe.test_interrupted_cancellation_withholds_actual_routing_pause','test_root_eligibility_local_probe.EligibilityLocalProbe.test_finalized_terminal_gate_does_not_report_pending','test_root_eligibility_local_probe.EligibilityLocalProbe.test_interrupted_terminal_is_typed_unresolved','test_root_eligibility_local_probe.EligibilityLocalProbe.test_active_remains_pending'])
