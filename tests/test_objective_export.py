"""Private supported PO workflow: exported sources are not inferred acceptance."""
import hashlib
import os
from pathlib import Path
import unittest
import contextlib
import subprocess

from agent_chat.core import Coordinator
from agent_chat.remote import RemoteCoordError

from test_project_activity import ActivityWorkflow


class ObjectiveExportTests(ActivityWorkflow):
    def start(self, **kwargs):
        kwargs['sessions_root'] = Path(self.tmp.name) / 'native-sessions'
        return super().start(**kwargs)

    def test_complete_sources_counts_and_unknown_acceptance(self):
        project, po, worker, route, _ = self.projects['Alpha']
        objective_body = ('Request a usable changed artifact; a baseline launch is not delivery. π\n'
                          + 'Original requirement detail.\n' * 120)
        task_body = 'Build and baseline launch passed. Customer accepted (unverified body claim).'
        source = self.message(route, po, objective_body)
        goal = self.cli_query(self.activity_cli(po, 'activity-adopt', '--source-request-id', source,
                             '--title', 'Usable requested result'), '.id')
        task_source = self.message(po, worker, task_body)
        task = self.cli_query(self.activity_cli(po, 'activity-assign', '--goal-id', goal,
                             '--worker', worker, '--source-request-id', task_source,
                             '--title', 'Artifact change'), '.id')
        self.activity_cli(worker, 'activity-transition', task, '--state', 'completed')
        self.activity_cli(po, 'activity-metadata', goal, '--next-action', 'Await concrete validation and customer inspection')
        snapshot = self.activity_cli(po, 'activity-snapshot')
        # Existing all-state ledger and known-empty vs unknown dependencies work.
        self.assertEqual(self.cli_query(snapshot, '.work | length'), '2')
        self.assertEqual(self.cli_query(snapshot, '.work[] | select(.id==$task) | .state', task=task), 'completed')
        self.assertEqual(self.cli_query(snapshot, '.work[] | select(.id==$goal) | (.dependencies == [])', goal=goal), 'true')
        self.assertEqual(self.cli_query(snapshot, '.work[] | select(.id==$task) | (.dependencies == null)', task=task), 'true')
        evidence = os.environ.get('EXPORT_EVIDENCE_DIR')
        if evidence:
            Path(evidence, 'existing-snapshot.json').write_bytes(snapshot)
        # A separate complete source export must preserve exact routed bodies and
        # distinguish a recorded state from validation/customer acceptance.
        exported = self.activity_cli(po, 'activity-export')
        self.assertEqual(self.cli_query(exported, '.counts.work'), '2')
        body = subprocess.run(['jq', '-j', '--arg', 'id', source,
                               '.sources[] | select(.id==$id) | .body'],
                              input=exported, capture_output=True, check=True).stdout
        self.assertEqual(body, objective_body.encode('utf-8'))
        self.assertEqual(self.cli_query(exported, '.sources[] | select(.id==$id) | .body', id=source), objective_body.rstrip())
        self.assertEqual(self.cli_query(exported, '.sources[] | select(.id==$id) | .body_sha256', id=source),
                         hashlib.sha256(objective_body.encode()).hexdigest())
        self.assertEqual(self.cli_query(exported, '.sources[] | select(.id==$id) | .body', id=task_source), task_body)
        self.assertEqual(self.cli_query(exported, '.label'), 'interim_incomplete')
        self.assertEqual(self.cli_query(exported, '.work[] | select(.id==$task) | .acceptance.customer_acceptance.state', task=task), 'unknown')
        self.assertEqual(self.cli_query(exported, '.sources[] | select(.id==$id) | .instruction_origin', id=source), 'agent_message')
        self.assertEqual(self.cli_query(exported, '.sources[] | select(.id==$id) | .body_bytes', id=source), str(len(objective_body.encode())))
        self.assertEqual(self.cli_query(exported, '.counts.by_state.completed'), '1')
        self.assertEqual(self.cli_query(exported, '.counts.available_sources'), '2')
        self.assertEqual(self.cli_query(exported, '.work[] | select(.id==$task) | [.acceptance.change_evidence.state,.acceptance.validation.state,.acceptance.artifact_delivery.state] | join(",")', task=task), 'unknown,unknown,unknown')

    def seed_goal(self, title='Export goal'):
        _, po, _, root, _ = self.projects['Alpha']
        source = self.message(root, po, title)
        goal = self.cli_query(self.activity_cli(po, 'activity-adopt', '--source-request-id', source,
                             '--title', title), '.id')
        return source, goal

    def ledger(self):
        project, po, _, _, _ = self.projects['Alpha']
        with contextlib.closing(Coordinator(self.server.projects.db_path(project), po)) as coord:
            return {name: [tuple(row) for row in coord.db.execute('SELECT * FROM ' + name + ' ORDER BY rowid')]
                    for name in ('messages', 'message_reads', 'project_work', 'project_work_events',
                                 'project_work_metadata', 'project_work_dependencies')}

    def test_current_po_project_host_and_exact_params(self):
        project, po, worker, root, client = self.projects['Alpha']
        source, goal = self.seed_goal()
        _, other_po, _, other_root, _ = self.projects['Beta']
        foreign = self.message(other_root, other_po, 'PRIVATE OTHER PROJECT SOURCE', name='Beta')
        self.activity_cli(other_po, 'activity-adopt', '--source-request-id', foreign, '--title', 'Foreign goal', name='Beta')
        before = self.ledger()
        for actor, name, host in ((worker,'Alpha','mac'), (root,'Alpha','mac'),
                                  (po,'Alpha','wrong-host'), (other_po,'Alpha','mac'), (po,'Beta','mac')):
            self.activity_cli(actor, 'activity-export', name=name, host=host, succeeds=False)
        with self.assertRaises(RemoteCoordError):
            self.coord_call(client, po, 'activity-export', owner_session=other_po)
        exported = self.activity_cli(po, 'activity-export')
        self.assertNotIn(b'PRIVATE OTHER PROJECT SOURCE', exported)
        self.assertNotIn(foreign.encode(), exported)
        self.assertEqual(self.cli_query(exported, '.scope.owner_session'), po)
        self.assertEqual(self.cli_query(exported, '.counts.work'), '1')
        self.assertEqual(self.ledger(), before)
        # Reassignment invalidates the former PO's export authority. This private
        # role-removal input is not a fabricated delegated action/approval receipt.
        with contextlib.closing(Coordinator(self.server.projects.db_path(project), po)) as coord:
            with coord.tx() as db:
                db.execute('UPDATE project_orchestrators SET session_id=? WHERE session_id=?', (worker, po))
        self.activity_cli(po, 'activity-export', succeeds=False)
        reassigned = self.activity_cli(worker, 'activity-export')
        self.assertEqual(self.cli_query(reassigned, '.counts.work'), '0')
        self.assertNotIn(source.encode(), reassigned)

    def test_every_state_dependency_removal_and_no_consumption(self):
        _, po, worker, _, _ = self.projects['Alpha']
        source, goal = self.seed_goal()
        _, dependency = self.seed_goal('Former export prerequisite')
        self.activity_cli(po, 'activity-metadata', goal, '--dependency', dependency, '--next-action', 'Wait for export')
        initial = self.activity_cli(po, 'activity-export')
        self.assertEqual(self.cli_query(initial, '.work[] | select(.id==$id) | .dependencies[0]', id=goal), dependency)
        self.activity_cli(po, 'activity-metadata', goal, '--next-action', 'Customer changed sequencing; proceed independently')
        for state in ('in_progress','blocked','completed','cancelled'):
            message = self.message(po, worker, 'Assigned ' + state)
            task = self.cli_query(self.activity_cli(po, 'activity-assign', '--goal-id', goal,
                     '--worker', worker, '--source-request-id', message, '--title', state), '.id')
            self.activity_cli(worker, 'activity-transition', task, '--state', state)
        before = self.ledger()
        exported = self.activity_cli(po, 'activity-export')
        self.assertEqual(self.ledger(), before)
        self.assertEqual(self.cli_query(exported, '.counts.work'), '6')
        self.assertEqual(self.cli_query(exported, '[.counts.by_state.queued,.counts.by_state.in_progress,.counts.by_state.blocked,.counts.by_state.completed,.counts.by_state.cancelled] | join(",")'), '2,1,1,1,1')
        self.assertEqual(self.cli_query(exported, '.work[] | select(.id==$id) | (.dependencies == [])', id=goal), 'true')
        self.assertEqual(self.cli_query(exported, '.work[] | select(.id==$id) | (.dependencies == null)', id=dependency), 'true')
        self.assertEqual(self.cli_query(exported, '.counts.events'), '10')
        self.assertEqual(self.cli_query(exported, '.completeness.sources'), 'complete')

    def test_unavailable_associations_are_gaps_not_foreign_content(self):
        project, po, worker, root, _ = self.projects['Alpha']
        _, goal = self.seed_goal()
        tasks = []
        for name in ('missing','inaccessible','route-mismatch'):
            source = self.message(po, worker, 'Original assigned ' + name)
            task = self.cli_query(self.activity_cli(po, 'activity-assign', '--goal-id', goal,
                     '--worker', worker, '--source-request-id', source, '--title', name), '.id')
            tasks.append(task)
        foreign = self.message(worker, root, 'PRIVATE NONPARTICIPANT BODY')
        wrong_route = self.message(root, po, 'OWN INBOX BUT NOT THIS TASK ASSIGNMENT')
        # Corrupt retained associations only: these are negative fixture inputs,
        # not synthetic authorization/read/effect receipts or a supported edit.
        with contextlib.closing(Coordinator(self.server.projects.db_path(project), po)) as coord:
            with coord.tx() as db:
                for task, source in zip(tasks, ('missing-message', foreign, wrong_route)):
                    db.execute('UPDATE project_work SET source_request_id=? WHERE id=?', (source, task))
        before = self.ledger()
        exported = self.activity_cli(po, 'activity-export')
        self.assertEqual(self.ledger(), before)
        self.assertEqual(self.cli_query(exported, '.counts.unavailable_sources'), '3')
        self.assertEqual(self.cli_query(exported, '.counts.available_sources'), '1')
        self.assertEqual(self.cli_query(exported, '.completeness.sources'), 'incomplete')
        self.assertEqual(self.cli_query(exported, '[.gaps[] | select(.source_id) | .reason] | sort | join(",")'),
                         'source_inaccessible,source_missing,source_route_mismatch')
        self.assertNotIn(b'PRIVATE NONPARTICIPANT BODY', exported)
        self.assertNotIn(b'OWN INBOX BUT NOT THIS TASK ASSIGNMENT', exported)


def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for name in ObjectiveExportTests.__dict__:
        if name.startswith('test_'):
            suite.addTest(ObjectiveExportTests(name))
    return suite
