import contextlib
import io
import os
import subprocess
import unittest
from unittest import mock
from agent_chat import cli, root_local, service
from test_admission_journal_probe import JournalFaults

class DiscoveryEnrollmentComposition(JournalFaults):
    def test_discover_preserves_existing_enrollment_and_identity(self):
        with self.projects.connection() as db:
            before=db.execute('SELECT count(*) FROM worker_admission_journal').fetchone()[0]
        self.assertEqual(before,1)
        self.assertEqual(self.status()['state'],'enrolled')
        out=io.StringIO()
        with mock.patch.dict(os.environ,{'CODEX_THREAD_ID':self.root_thread}), contextlib.redirect_stdout(out):
            self.assertEqual(cli.main(['root-local','discover']),0)
        expression='.state == "registered" and .conversation_relation == "same_hint" and .conversation_hint_is_authoritative == false and .attached_proven == false and .home_session == $session and .host_id == $host'
        subprocess.run(['jq','-e','--arg','session',self.root_sid,'--arg','host',self.host,expression],
                       input=out.getvalue(),text=True,capture_output=True,check=True)
        result=root_local.execute('worker-admission-enrollment-status',dict(self.enrollment_params),
            master_db=self.master,service_plist=service.INSTALLED_SERVER_CONFIG.plist,
            home_project='default',session=self.root_sid,host_id=self.host)
        self.assertEqual(result['request_id'],self.params['request_id'])
        self.assertFalse(result['runtime_authorized'])
        self.assertEqual(self.status()['state'],'enrolled')
        self.assertEqual(self.safe_count(),0)
        with self.projects.connection() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM worker_admission_journal').fetchone()[0],before)

def load_tests(loader,tests,pattern):
    return unittest.TestSuite([DiscoveryEnrollmentComposition('test_discover_preserves_existing_enrollment_and_identity')])
