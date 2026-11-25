"""Current installed source, private operator/PO/task UI acceptance only."""
import os
from pathlib import Path
import subprocess
import unittest
from unittest import mock
from agent_chat import root_local, service
from test_project_activity import ActivityWorkflow
from test_remote_web import TOKEN


class CurrentUiAcceptance(ActivityWorkflow):
    def start(self, **kwargs):
        base = Path(self.tmp.name).resolve()
        sessions = base / 'native-sessions'; sessions.mkdir()
        for module in (service, root_local):
            original_database = module.configured_server_database
            def checked_config():
                # ActivityWorkflow installs this explicit private fixture provider.
                config = service.INSTALLED_SERVER_CONFIG
                if not Path(config.plist).resolve().is_relative_to(base) or not Path(config.master).resolve().is_relative_to(base):
                    raise AssertionError('installed service discovery outside private fixture')
                return config
            def checked_database(plist=None, original=original_database):
                if plist is None or not Path(plist).resolve().is_relative_to(base):
                    raise AssertionError('installed plist outside private fixture')
                database = original(plist)
                if not Path(database).resolve().is_relative_to(base):
                    raise AssertionError('installed database outside private fixture')
                return database
            for name, fn in (('_installed_server_config', checked_config), ('configured_server_database', checked_database)):
                guard = mock.patch.object(module, name, side_effect=fn)
                guard.start(); self.addCleanup(guard.stop)
        return super().start(sessions_root=sessions, **kwargs)

    def tearDown(self):
        # Stop admitting requests and let the fixture's actual HTTP handlers
        # finish before closing their stores and removing the temporary tree.
        self.server.http_maintenance('start')
        self.server.stop_event.set()
        self.server.shutdown()
        self.thread.join(3)
        try:
            self.assertFalse(self.thread.is_alive(), 'private HTTP server did not stop')
            with self.server.http_admission:
                drained = self.server.http_admission.wait_for(lambda: not self.server.http_active, timeout=12)
            self.assertTrue(drained, 'private HTTP handlers did not drain; temporary tree retained')
        finally:
            self.server.server_close()
        self.tmp.cleanup()

    def test_request_to_two_tasks_and_rendered_backlog(self):
        project, po, worker, root, _ = self.projects['Alpha']
        source = self.message(root, po, 'Own this private UI acceptance objective.')
        goal = self.cli_query(self.activity_cli(po,'activity-adopt','--source-request-id',source,'--title','Private UI objective'),'.id')
        task_ids = []
        for title in ('Private task A','Private task B'):
            assignment = self.message(po,worker,title)
            result = self.activity_cli(po,'activity-assign','--goal-id',goal,'--worker',worker,'--source-request-id',assignment,'--title',title)
            task_ids.append(self.cli_query(result,'.id'))
        self.activity_cli(po,'activity-metadata',goal,'--dependency',task_ids[0],'--dependency',task_ids[1],'--next-action','Inspect both private tasks')
        snapshot = self.activity_cli(po,'activity-snapshot')
        self.assertEqual(self.cli_query(snapshot,'[.work[] | select(.id==$a or .id==$b)] | length',a=task_ids[0],b=task_ids[1]),'2')
        status, body, _ = self.request('GET','/api/root/snapshot',auth='basic')
        self.assertEqual(status,200,body)
        self.assertEqual(self.cli_query(body,'.root_overview.projects[] | select(.project_id==$project) | .objectives[] | select(.id==$goal) | .source_request_id',project=project,goal=goal),source)
        evidence=Path(os.environ.get('UI_EVIDENCE', str(Path(self.tmp.name) / 'ui-evidence')))
        evidence.mkdir(parents=True, exist_ok=True)
        env=dict(os.environ, UI_URL=self.url, UI_TOKEN=TOKEN, UI_PROJECT=project, UI_SOURCE=source,
                 UI_OWNER=po, UI_DEP_A=task_ids[0], UI_DEP_B=task_ids[1], UI_EVIDENCE=str(evidence))
        with mock.patch('agent_chat.goal_local_provider.read_project_goal', return_value={'status':'unavailable'}):
            result=subprocess.run(['node',str(Path(__file__).with_name('current_ui_acceptance.cjs'))],env=env,capture_output=True,text=True,timeout=35)
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)
        print(result.stdout.strip(), flush=True)


def load_tests(loader, tests, pattern):
    return unittest.TestSuite([CurrentUiAcceptance('test_request_to_two_tasks_and_rendered_backlog')])
