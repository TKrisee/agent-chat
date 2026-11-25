"""Private authenticated PO backlog metadata and separate native status."""
import os
import subprocess
import unittest
from pathlib import Path
from unittest import mock
from test_project_activity import ActivityWorkflow
from test_remote_web import TOKEN


class GoalVisibility(ActivityWorkflow):
    def test_adopted_goal_reports_metadata_without_native_goal(self):
        _, po, _, root, _ = self.projects['Alpha']
        source = self.message(root, po, 'Visibility objective')
        adopted = self.activity_cli(po, 'activity-adopt', '--source-request-id', source, '--title', 'Visibility')
        goal = self.cli_query(adopted, '.id')
        snapshot = self.activity_cli(po, 'activity-snapshot')
        self.assertEqual(self.cli_query(snapshot,
            '.work[] | select(.id==$id) | [has("dependencies"),has("next_action")] | all', id=goal), 'true')
        self.assertEqual(self.cli_query(snapshot, '.work[] | select(.id==$id) | .dependencies == null and .next_action == null', id=goal), 'true')

    def test_metadata_is_owned_project_scoped_and_rendered(self):
        project, po, worker, root, _ = self.projects['Alpha']
        source = self.message(root, po, 'Primary objective')
        goal = self.cli_query(self.activity_cli(po, 'activity-adopt', '--source-request-id', source, '--title', 'Primary objective'), '.id')
        second = self.message(root, po, 'Dependency objective')
        dependency = self.cli_query(self.activity_cli(po, 'activity-adopt', '--source-request-id', second, '--title', 'Dependency'), '.id')
        _, beta_po, _, beta_root, _ = self.projects['Beta']
        foreign_source = self.message(beta_root, beta_po, 'Other project', name='Beta')
        foreign = self.cli_query(self.activity_cli(beta_po, 'activity-adopt', '--source-request-id', foreign_source,
                                 '--title', 'Foreign', name='Beta'), '.id')
        self.activity_cli(worker, 'activity-metadata', goal, '--dependency', dependency, '--next-action', 'Review evidence', succeeds=False)
        self.activity_cli(po, 'activity-metadata', goal, '--dependency', dependency, '--next-action', 'Review evidence', name='Beta', succeeds=False)
        self.activity_cli(po, 'activity-metadata', goal, '--dependency', goal, succeeds=False)
        self.activity_cli(po, 'activity-metadata', goal, '--dependency', foreign, succeeds=False)
        self.activity_cli(po, 'activity-metadata', goal, '--dependency', dependency, '--next-action', 'Review evidence')
        self.activity_cli(po, 'activity-metadata', dependency)
        snapshot = self.activity_cli(po, 'activity-snapshot')
        self.assertEqual(self.cli_query(snapshot, '.work[] | select(.id==$id) | .dependencies[0]', id=goal), dependency)
        self.assertEqual(self.cli_query(snapshot, '.work[] | select(.id==$id) | .next_action', id=goal), 'Review evidence')
        self.assertEqual(self.cli_query(snapshot, '.work[] | select(.id==$id) | .dependencies == [] and .next_action == null', id=dependency), 'true')
        status, body, _ = self.request('GET', '/api/root/snapshot', auth='basic')
        self.assertEqual(status, 200, body)
        self.assertEqual(self.cli_query(body, '.root_overview.projects[] | select(.project_id==$project) | .objectives[] | select(.id==$id) | .source_request_id', project=project, id=goal), source)
        script = Path(__file__).with_name('goal_visibility_browser.cjs')
        env = dict(os.environ, GOAL_TEST_URL=self.url, GOAL_TEST_TOKEN=TOKEN, GOAL_TEST_PROJECT=project,
                   GOAL_TEST_SOURCE=source, GOAL_TEST_OWNER=po, GOAL_TEST_DEPENDENCY=dependency)
        result = subprocess.run(['node', str(script)], env=env, capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_mock_native_goal_and_unavailable_remain_separate_from_adoption(self):
        project, po, _, root, _ = self.projects['Alpha']
        with mock.patch('agent_chat.goal_local_provider.read_project_goal', return_value={'status':'active','observed_at':1}) as provider:
            status, body, _ = self.request('GET', '/api/root/goal-status?project=' + project, auth='basic')
            self.assertEqual(status, 200, body)
            self.assertEqual(self.cli_query(body, '.status'), 'active')
            source = self.message(root, po, 'Queued independently of mocked native goal')
            adopted = self.activity_cli(po, 'activity-adopt', '--source-request-id', source, '--title', 'Queued')
            self.assertEqual(self.cli_query(adopted, '.state'), 'queued')
            self.assertEqual(provider.call_count, 1)
        with mock.patch('agent_chat.goal_local_provider.read_project_goal', return_value={'status':'unavailable'}):
            status, body, _ = self.request('GET', '/api/root/goal-status?project=' + project, auth='basic')
            self.assertEqual(status, 200, body)
            self.assertEqual(self.cli_query(body, '.status'), 'unavailable')


def load_tests(loader, tests, pattern):
    return unittest.TestSuite(GoalVisibility(name) for name in GoalVisibility.__dict__ if name.startswith('test_'))
