import contextlib, sqlite3, subprocess, time
import unittest
from test_guard_file_evidence import DeclaredFileEvidence
from agent_chat.remote import RemoteCoordError

class LegacyClosedProbe(DeclaredFileEvidence):
    def test_closed_unprepared_run_is_not_prospective_recovery(self):
        target, cap, rid = self.claim_file()
        self.cli_call(self.worker,'run','--require-host-witness','--declared-task-id',self.task['id'],'--declared-source-id',self.assignment,'--capability-file',str(cap),'--reservation-id',rid,'--process-evidence-file',str(self.fixture/'output'/'normal.jsonl'),'file:guard-file.txt','--','/bin/sh','-c','true')
        token=subprocess.run(['jq','-er','.token',str(cap)],capture_output=True,text=True,check=True).stdout.strip()
        with contextlib.closing(sqlite3.connect(self.server.projects.db_path(self.project_id))) as db:
            db.row_factory=sqlite3.Row
            row=db.execute('SELECT r.*,w.parent_pid,w.pgid,w.birth_sec,w.birth_usec,w.boot_id FROM remote_guarded_runs r JOIN remote_guard_witnesses w ON w.run_id=r.run_id WHERE r.reservation_id=?',(rid,)).fetchone()
            self.assertIsNotNone(row['closed_at'])
            self.assertIsNone(db.execute('SELECT 1 FROM remote_guard_recovery_intents WHERE run_id=?',(row['run_id'],)).fetchone())
            ob=db.execute("SELECT device,inode,size,mode,sha256 FROM remote_guard_file_observations WHERE run_id=? AND stage='final'",(row['run_id'],)).fetchone()
            proof=dict(pid=row['local_pid'],parent_pid=row['parent_pid'],pgid=row['pgid'],birth_sec=row['birth_sec'],birth_usec=row['birth_usec'],boot_id=row['boot_id'],observed_at=time.time(),leader_absent=True,parent_absent=True,group_absent=True)
            args=dict(run_id=row['run_id'],token=token,evidence_sha256=row['close_evidence_sha256'],observation=dict(ob),process_observation=proof)
        with self.assertRaisesRegex(RemoteCoordError,'not prospectively prepared'):
            self.po_call('guard-recovery-settle',session=self.worker,**args)


def load_tests(loader, tests, pattern):
    # Base classes are discovered in their own modules with their own setup.
    # Running their inherited tests under this leaf's expanded project fixture
    # changes those tests' preconditions and duplicates the owning contracts.
    return unittest.TestSuite(LegacyClosedProbe(name) for name in LegacyClosedProbe.__dict__ if name.startswith('test_'))
