import copy
import hashlib
import os
import plistlib
import shlex
import threading
from unittest import mock
import contextlib
from pathlib import Path
import subprocess
import sys
import unittest
import uuid
from types import SimpleNamespace

from agent_chat.core import Coordinator, CoordError
from agent_chat.projects import Projects
from agent_chat.bridge import Bridge
from agent_chat.bridge_client import RemoteBridgeState
from agent_chat.bridge_state import BridgeState
from agent_chat.remote import HttpClient, RemoteCoordError
from agent_chat import root_local, service
from test_remote_web import RemoteWebFixture, TOKEN
from agent_chat.project_activity import ProjectActivity

class ObservedProvider:
    """Controlled App Server observation boundary; application state stays real."""
    def __init__(self):
        self.settings = {}
        self.queues = {}
        self.history = {}
        self.context = None
    def add(self, thread, cwd):
        self.queues[thread] = []
        self.history[thread] = []
        self.settings[thread] = dict(thread={'id': thread}, model='test-model', modelProvider='openai',
            cwd=cwd, runtimeWorkspaceRoots=[cwd], reasoningEffort='high',
            approvalPolicy='on-request', approvalsReviewer='user',
            activePermissionProfile={'id': ':workspace'},
            sandbox={'type': 'workspaceWrite', 'writableRoots': [cwd], 'networkAccess': False})
    def request(self, method, params, **kwargs):
        thread = params.get('threadId')
        if method == 'thread/read':
            return {'thread': {'id': thread, 'status': {'type':'idle'}, 'canAcceptDirectInput': True}}
        if method == 'thread/resume':
            return copy.deepcopy(self.settings[thread])
        if method == 'thread/start':
            thread = str(uuid.uuid4())
            self.add(thread, params['cwd'])
            values = self.settings[thread]
            values.update(model=params['model'], modelProvider=params['modelProvider'],
                approvalPolicy=params['approvalPolicy'], approvalsReviewer=params['approvalsReviewer'],
                runtimeWorkspaceRoots=params['runtimeWorkspaceRoots'],
                activePermissionProfile={'id': params['permissions'], 'extends': None},
                reasoningEffort=params.get('config', {}).get('model_reasoning_effort'),
                sandbox=dict(type='workspaceWrite', writableRoots=params['runtimeWorkspaceRoots'][1:],
                    networkAccess=False, excludeTmpdirEnvVar=False, excludeSlashTmp=False))
            return copy.deepcopy(values)
        if method == 'thread/queue/list':
            return {'data': copy.deepcopy(self.queues[thread]), 'nextCursor': None}
        if method == 'thread/turns/list':
            return {'data': ([{'items':copy.deepcopy(self.history[thread])}] if self.history[thread] else []), 'nextCursor': None}
        if method == 'thread/queue/add':
            item = dict(id='queue-'+str(uuid.uuid4()), clientUserMessageId=params['clientUserMessageId'], input=params['input'])
            self.queues[thread].append(item)
            return {'queuedSubmission': copy.deepcopy(item)}
        if method == 'thread/queue/start':
            item = next(item for item in self.queues[thread] if item['id']==params['queuedSubmissionId'])
            self.queues[thread].remove(item)
            self.history[thread].append(dict(type='userMessage', clientId=item['clientUserMessageId'], content=item['input']))
            return {}
        raise AssertionError('unexpected provider operation: ' + method)
    def connect(self): return self
    def close(self): pass

class ActivityAPI:
    """Use the real authenticated HTTP coordinator operation for every action."""
    def __init__(self, case, client, actor):
        self.case, self.client, self.actor = case, client, actor
    def call(self, op, **params):
        return self.case.coord_call(self.client, self.actor, 'activity-' + op, **params)
    def adopt(self, source_request_id, title):
        return self.call('adopt', source_request_id=source_request_id, title=title)
    def assign(self, goal_id, worker, source_request_id, title):
        return self.call('assign', goal_id=goal_id, worker=worker, source_request_id=source_request_id, title=title)
    def transition(self, work_id, state):
        return self.call('transition', work_id=work_id, state=state)
    def snapshot(self):
        return self.call('snapshot')
    def history(self, work_id):
        return self.call('history', work_id=work_id)['events']
    def link_escalation(self, origin_work_id, diagnostic_work_id, denial_message_id, next_action):
        return self.call('link-escalation', origin_work_id=origin_work_id,
                         diagnostic_work_id=diagnostic_work_id, denial_message_id=denial_message_id,
                         next_action=next_action)

class ActivityWorkflow(RemoteWebFixture):
    def setUp(self):
        super().setUp()
        self.addCleanup(self.stop_if_started)
        self.install_local_provider()
        self.provider = ObservedProvider()
        self.identity = dict(owner='activity-fixture-worker', secret='x'*40, host_id='mac')
        self.call('register', agent='root')
        self.root_thread = str(uuid.uuid4())
        self.call('bind', thread=self.root_thread)
        self.neutral = Path(self.tmp.name).resolve() / 'neutral'
        self.neutral.mkdir()
        self.provider.add(self.root_thread, str(self.neutral))
        state = RemoteBridgeState(self.client, self.url, self.identity)
        state.call('acquire')
        self.call('agent-status', refresh=True, expected_thread=self.root_thread, request_id=str(uuid.uuid4()))
        Bridge(state, self.provider).tick(observe_policies=False)
        self.root('enroll', expected_thread=self.root_thread, confirm=True)
        self.projects = {}
        for name in ('Alpha', 'Beta'):
            project = self.client.call('/api/projects/rpc', dict(op='create', name=name))['project']['id']
            self.call('context')
            client = HttpClient(self.url, TOKEN, project=project)
            workspace = Path(self.tmp.name).resolve() / name
            workspace.mkdir()
            configured = self.root('configure', project=project, agent='po-'+name.lower(),
                cwd=str(workspace), prompt='Own project activity test work.', request_id=str(uuid.uuid4()),
                confirm=True, model='test-model', effort='high')
            po = configured['creation']['target_session']
            state = RemoteBridgeState(client, self.url, self.identity)
            state.call('acquire')
            project_bridge = Bridge(state, self.provider)
            project_bridge.tick(observe_policies=False)
            job = state.reset_job(configured['creation']['id'])
            self.assertEqual(job['status'], 'completed', job)
            route = self.root('route', project=project)['session']
            worker = 'worker-' + name.lower()
            self.coord_call(client, worker, 'register', agent=worker)
            worker_thread = str(uuid.uuid4())
            self.coord_call(client, worker, 'bind', thread=worker_thread)
            self.provider.add(worker_thread, str(workspace))
            self.call('context')
            self.root('delegate-management', project=project, po=po, target=worker,
                      expected_thread=worker_thread, expected_host='mac', request_id=str(uuid.uuid4()), confirm=True)
            self.projects[name] = (project, po, worker, route, client)
        print('Real HTTP/bridge registration+inspection; validated same-UID local Root enrollment/configuration; completed PO creation and worker delegation: Alpha/Beta', flush=True)

    def stop_if_started(self):
        # unittest calls cleanup even if setUp fails; server is fixture-owned.
        if self.thread.is_alive():
            self.stop()
    def install_local_provider(self):
        # service.py documents replacing this environment provider in tests.
        # All actual local Root UID/file/plist/wrapper/DB/host/binding checks run.
        base = Path(self.tmp.name).resolve()
        self.service_plist = base / 'server.plist'
        wrapper, executable = base / 'wrapper', base / 'server-executable'
        executable.write_text('#!/bin/sh\nexit 0\n')
        wrapper.write_text('exec ' + ' '.join(shlex.quote(value) for value in (
            str(executable), '--db', str(self.db.resolve()), '--host', '127.0.0.1',
            '--port', str(self.server.server_port))) + '\n')
        with self.service_plist.open('wb') as stream:
            plistlib.dump(dict(Label='com.agent-chat.server', AssociatedBundleIdentifiers=['com.agent-chat.server'],
                WorkingDirectory=str(base), RunAtLoad=True, KeepAlive=True, ThrottleInterval=10,
                ProgramArguments=[str(wrapper)], EnvironmentVariables={}), stream)
        config = service.InstalledServerConfig(self.service_plist, wrapper, executable,
            self.db.resolve(), base, os.getuid(), '127.0.0.1', self.server.server_port)
        provider = mock.patch.object(service, 'INSTALLED_SERVER_CONFIG', config)
        provider.start()
        self.addCleanup(provider.stop)

    def root(self, op, **params):
        return root_local.execute(op, params, master_db=self.db.resolve(), service_plist=self.service_plist,
                                  home_project='default', session='a', host_id='mac')
    def coord_call(self, client, session, op, **params):
        return client.call('/api/coord', dict(op=op, session=session, host_id='mac', params=params))
    @contextlib.contextmanager
    def activity(self, name='Alpha', actor=None):
        project, po, worker, route, client = self.projects[name]
        yield ActivityAPI(self, client, actor or po)
    def message(self, sender, target, body, name='Alpha'):
        project = self.projects[name][0]
        with contextlib.closing(Coordinator(self.server.projects.db_path(project), sender)) as coord:
            coord.context()
            return coord.send(target, body)['id']

    def activity_cli(self, actor, *args, name='Alpha', host='mac', succeeds=True):
        env = {key: value for key, value in os.environ.items() if not key.startswith('AGENT_CHAT_')}
        env.update(AGENT_CHAT_HOST_ID=host, AGENT_CHAT_STATE_DIR=self.tmp.name,
                   PYTHONDONTWRITEBYTECODE='1', PYTHONPATH=str(Path(__file__).resolve().parents[1] / 'src'))
        result = subprocess.run([sys.executable, '-m', 'agent_chat', '--server', self.url,
            '--api-token', TOKEN, '--project', self.projects[name][0], '--session', actor, *args],
            env=env, capture_output=True, timeout=15)
        if succeeds:
            self.assertEqual(result.returncode, 0, result.stderr.decode())
        else:
            self.assertNotEqual(result.returncode, 0)
        return result.stdout

    def cli_query(self, output, query, **bindings):
        command = ['jq', '-er']
        for name, value in bindings.items():
            command.extend(['--arg', name, value])
        result = subprocess.run([*command, query], input=output, capture_output=True, check=True)
        return result.stdout.decode().strip()

    def test_cli_activity_roundtrip_preserves_source_role_and_host_authority(self):
        _, po, worker, root, _ = self.projects['Alpha']
        source = self.message(root, po, 'CLI-owned customer objective')
        adopted = self.activity_cli(po, 'activity-adopt', '--source-request-id', source, '--title', 'CLI goal')
        goal = self.cli_query(adopted, '.id')
        assignment = self.message(po, worker, 'CLI-owned worker contribution')
        assigned = self.activity_cli(po, 'activity-assign', '--goal-id', goal, '--worker', worker,
                                    '--source-request-id', assignment, '--title', 'CLI contribution')
        work = self.cli_query(assigned, '.id')
        self.assertEqual(self.cli_query(assigned, '[.owner_session,.source_request_id] | @tsv'),
                         worker + '\t' + assignment)
        self.activity_cli(worker, 'activity-transition', work, '--state', 'blocked')
        history = self.activity_cli(worker, 'activity-history', work)
        self.assertEqual(self.cli_query(history, '[.events[].state] | join(",")'), 'queued,blocked')
        snapshot = self.activity_cli(po, 'activity-snapshot')
        self.assertEqual(self.cli_query(snapshot, '.work[] | select(.id==$work) | .state', work=work), 'blocked')
        self.activity_cli(worker, 'activity-adopt', '--source-request-id', source, '--title', 'Not a PO', succeeds=False)
        self.activity_cli(po, 'activity-assign', '--goal-id', goal, '--worker', worker,
                          '--source-request-id', source, '--title', 'Wrong routed source', succeeds=False)
        self.activity_cli(po, 'activity-snapshot', host='wrong-host', succeeds=False)
        self.activity_cli(po, 'activity-snapshot', name='Beta', succeeds=False)

    def test_cli_activity_snapshot_after_turnover_to_direct_root_management(self):
        name = 'Direct'
        project_uuid = uuid.UUID('5fb8d3d13ec348e0993b25a3f3169289')
        with mock.patch('agent_chat.projects.uuid', SimpleNamespace(uuid4=lambda: project_uuid)):
            project = self.client.call('/api/projects/rpc', dict(op='create', name=name))['project']['id']
        client = HttpClient(self.url, TOKEN, project=project)
        workspace = Path(self.tmp.name).resolve() / name
        workspace.mkdir()
        self.call('context')
        configured = self.root('configure', project=project, agent='retiring-po', cwd=str(workspace),
            prompt='Own the retained objective.', request_id=str(uuid.uuid4()), confirm=True,
            model='test-model', effort='high')
        po = configured['creation']['target_session']
        root = self.root('route', project=project)['session']
        state = RemoteBridgeState(client, self.url, self.identity)
        state.call('acquire')
        bridge = Bridge(state, self.provider)
        bridge.tick(observe_policies=False)
        self.assertEqual(state.reset_job(configured['creation']['id'])['status'], 'completed')
        self.projects[name] = (project, po, None, root, client)
        source = self.message(root, po, 'Keep the original project objective.', name=name)
        goal = self.activity_cli(po, 'activity-adopt', '--source-request-id', source,
                                 '--title', 'Retained objective', name=name)
        goal_id = self.cli_query(goal, '.id')
        po_snapshot = self.activity_cli(po, 'activity-snapshot', name=name)
        self.assertEqual(self.cli_query(po_snapshot, '.work[0].id'), goal_id)
        preview = self.root('team-reset-preview', project=project, message_ids=[])
        reset_id = str(uuid.uuid4())
        result = self.root('team-reset', project=project, message_ids=[], request_id=reset_id,
                           expected_digest=preview['digest'], confirm=True)
        self.assertEqual(result['status'], 'completed')
        self.call('context')
        creation = self.root('direct-create', project=project, reset_id=reset_id, agent='direct-worker',
            cwd=str(workspace), prompt='Review retained project work.', request_id=str(uuid.uuid4()),
            confirm=True, model='test-model', effort='high')['creation']
        bridge.tick(observe_policies=False)
        self.assertEqual(state.reset_job(creation['id'])['status'], 'completed')
        worker = creation['target_session']

        for actor in (root, worker):
            with self.subTest(actor=actor):
                snapshot = self.activity_cli(actor, 'activity-snapshot', name=name)
                self.assertEqual(self.cli_query(snapshot, '.project'), project)
                self.assertEqual(self.cli_query(snapshot, '[.work[].id] | join(",")'), goal_id)
                self.assertEqual(self.cli_query(snapshot, '.work[0].owner_session'), po)
                self.activity_cli(actor, 'activity-export', name=name, succeeds=False)
                self.activity_cli(actor, 'activity-transition', goal_id, '--state', 'completed',
                                  name=name, succeeds=False)
        outsider = 'unassigned-worker'
        self.coord_call(client, outsider, 'register', agent=outsider)
        self.activity_cli(outsider, 'activity-snapshot', name=name, succeeds=False)
        self.activity_cli(self.projects['Beta'][3], 'activity-snapshot', name=name, succeeds=False)
        self.activity_cli(worker, 'activity-snapshot', name='Beta', succeeds=False)
        self.activity_cli(worker, 'activity-snapshot', name=name, host='wrong-host', succeeds=False)
        unchanged = self.activity_cli(root, 'activity-snapshot', name=name)
        self.assertEqual(self.cli_query(unchanged, '.work[0].state'), 'queued')

    def continuation_fixture(self):
        _, po, worker, root, client = self.projects['Alpha']
        source = self.message(root, po, 'Continue independent authorized contributions')
        blocked_source = self.message(po, worker, 'BLOCKED_ACTION_A must not enter a native wake')
        runnable_source = self.message(po, worker, 'INDEPENDENT_ACTION_B is already authorized')
        ordinary_source = self.message(po, worker, 'Ordinary informational message remains deliverable')
        with self.activity() as activity:
            goal = activity.adopt(source, 'Independent continuation')
            blocked = activity.assign(goal['id'], worker, blocked_source, 'Contribution A')
            runnable = activity.assign(goal['id'], worker, runnable_source, 'Contribution B')
        state = RemoteBridgeState(client, self.url, self.identity)
        thread = state.resolve(worker)['thread_id']
        return state, thread, worker, blocked, runnable, ordinary_source

    def test_blocked_assignment_withheld_while_independent_task_dispatches_once(self):
        state, thread, worker, blocked, runnable, ordinary = self.continuation_fixture()
        with self.activity() as activity:
            activity.transition(blocked['id'], 'blocked')
            activity.transition(blocked['goal_id'], 'completed')
        bridge = Bridge(state, self.provider)
        pending_ids = {message['id'] for message in state.pending()}
        with mock.patch.object(self.provider, 'request', wraps=self.provider.request) as native:
            bridge.tick(observe_policies=False)
            # A recreated bridge must retain the exact source/job association.
            Bridge(RemoteBridgeState(state.client, self.url, self.identity), self.provider).tick(observe_policies=False)
        adds = [call.args[1] for call in native.call_args_list
                if call.args[0] == 'thread/queue/add' and call.args[1]['threadId'] == thread]
        starts = [call.args[1] for call in native.call_args_list
                  if call.args[0] == 'thread/queue/start' and call.args[1]['threadId'] == thread]
        self.assertEqual(len(adds), 1)
        self.assertEqual(len(starts), 1)
        payload = '\n'.join(item['text'] for item in adds[0]['input'] if item['type'] == 'text')
        self.assertNotIn(blocked['source_request_id'], payload)
        self.assertNotIn('BLOCKED_ACTION_A', payload)
        self.assertIn(runnable['source_request_id'], payload)
        self.assertIn(ordinary, payload)
        self.assertNotIn(blocked['source_request_id'], pending_ids)
        self.assertTrue({runnable['source_request_id'], ordinary}.issubset(pending_ids))
        wake = adds[0]['clientUserMessageId']
        self.assertEqual(state.job(wake)['status'], 'dispatched')
        self.assertEqual([item['clientId'] for item in self.provider.history[thread]], [wake])
        with self.activity() as activity:
            work = {row['id']: row for row in activity.snapshot()['work']}
        self.assertEqual((work[blocked['id']]['owner_session'], work[blocked['id']]['source_request_id'],
                          work[blocked['id']]['state']), (worker, blocked['source_request_id'], 'blocked'))
        self.assertEqual(work[runnable['id']]['state'], 'queued')  # Dispatch is not task execution/result.
        unread = self.coord_call(state.client, worker, 'inbox')['messages']
        self.assertIn(blocked['source_request_id'], {message['id'] for message in unread})

    def test_blocked_assignment_race_rejects_whole_stale_payload(self):
        state, thread, worker, blocked, runnable, ordinary = self.continuation_fixture()
        bridge = Bridge(state, self.provider)
        messages = [message for message in state.pending() if message['recipient_session'] == worker]
        payload = bridge.payload(thread, messages)
        self.assertIn(blocked['source_request_id'], payload)
        self.assertIn(runnable['source_request_id'], payload)
        prepare = BridgeState.prepare
        def block_after_remote_validation(local_state, *args, **kwargs):
            # The HTTP lease has validated its pending snapshot. Commit the
            # real owner transition before the inner admission transaction.
            with self.activity() as activity:
                activity.transition(blocked['id'], 'blocked')
            return prepare(local_state, *args, **kwargs)
        with mock.patch.object(BridgeState, 'prepare', block_after_remote_validation):
            self.assertIsNone(state.prepare(thread, messages, payload))
        self.assertFalse(any(job['thread_id'] == thread for job in state.jobs()))
        self.assertEqual(self.provider.queues[thread], [])
        self.assertEqual(self.provider.history[thread], [])
        # The normal next tick creates a fresh payload for B and ordinary mail.
        bridge.tick(observe_policies=False)
        self.assertEqual(len(self.provider.history[thread]), 1)
        delivered = '\n'.join(item['text'] for item in self.provider.history[thread][0]['content']
                              if item['type'] == 'text')
        self.assertNotIn(blocked['source_request_id'], delivered)
        self.assertNotIn('BLOCKED_ACTION_A', delivered)
        self.assertIn(runnable['source_request_id'], delivered)
        self.assertIn(ordinary, delivered)

    def test_finished_assignments_are_not_new_native_work(self):
        state, thread, _, completed, cancelled, ordinary = self.continuation_fixture()
        with self.activity() as activity:
            activity.transition(completed['id'], 'completed')
            activity.transition(cancelled['id'], 'cancelled')
        pending_ids = {message['id'] for message in state.pending()}
        self.assertNotIn(completed['source_request_id'], pending_ids)
        self.assertNotIn(cancelled['source_request_id'], pending_ids)
        self.assertIn(ordinary, pending_ids)
        Bridge(state, self.provider).tick(observe_policies=False)
        self.assertEqual(len(self.provider.history[thread]), 1)
        payload = '\n'.join(item['text'] for item in self.provider.history[thread][0]['content']
                            if item['type'] == 'text')
        self.assertNotIn(completed['source_request_id'], payload)
        self.assertNotIn(cancelled['source_request_id'], payload)
        self.assertIn(ordinary, payload)

    def specialist_fixture(self):
        state, worker_thread, worker, blocked, runnable, _ = self.continuation_fixture()
        project, po, _, _, client = self.projects['Alpha']
        self.coord_call(client, worker, 'message', id=blocked['source_request_id'])
        exact_denial = 'Review denied operation A for its original actor and scope; no retry is authorized.'
        denial = self.coord_call(client, worker, 'send', to=po, body=exact_denial,
                                 reply_to=blocked['source_request_id'], ack_reply=True)['id']
        specialist = 'specialist-alpha'
        specialist_thread = str(uuid.uuid4())
        self.coord_call(client, specialist, 'register', agent=specialist)
        self.coord_call(client, specialist, 'bind', thread=specialist_thread)
        self.provider.add(specialist_thread, str(Path(self.tmp.name).resolve() / 'Alpha'))
        self.call('context')
        self.root('delegate-management', project=project, po=po, target=specialist,
                  expected_thread=specialist_thread, expected_host='mac',
                  request_id=str(uuid.uuid4()), confirm=True)
        diagnostic_source = self.message(po, specialist,
            'DIAGNOSTIC_C: inspect the integration failure and propose a repair; do not execute blocked A.')
        next_action = 'Original owner awaits integration repair and legitimate eligibility; no retry of denied A.'
        with self.activity() as activity:
            activity.transition(blocked['id'], 'blocked')
            diagnostic = activity.assign(blocked['goal_id'], specialist, diagnostic_source, 'Diagnose integration failure')
            linked = self.activity_cli(po, 'activity-link-escalation', '--origin-work-id', blocked['id'],
                '--diagnostic-work-id', diagnostic['id'], '--denial-message-id', denial, '--next-action', next_action)
            escalation = activity.snapshot()['escalations'][0]
            self.assertEqual(self.cli_query(linked, '.id'), escalation['id'])
            self.assertEqual(activity.link_escalation(blocked['id'], diagnostic['id'], denial, next_action), escalation)
            with self.assertRaises(RemoteCoordError):
                activity.link_escalation(blocked['id'], diagnostic['id'], denial, 'Retry operation A as the specialist')
        return SimpleNamespace(state=state, worker_thread=worker_thread, worker=worker, blocked=blocked,
            runnable=runnable, project=project, po=po, client=client, specialist=specialist,
            specialist_thread=specialist_thread, diagnostic=diagnostic, diagnostic_source=diagnostic_source,
            denial=denial, next_action=next_action, escalation=escalation, exact_denial=exact_denial)

    def test_specialist_escalation_retains_denial_while_independent_work_proceeds(self):
        case = self.specialist_fixture()
        state, worker_thread, worker = case.state, case.worker_thread, case.worker
        blocked, runnable, client, po = case.blocked, case.runnable, case.client, case.po
        specialist, specialist_thread = case.specialist, case.specialist_thread
        diagnostic, diagnostic_source = case.diagnostic, case.diagnostic_source
        denial, next_action, escalation = case.denial, case.next_action, case.escalation
        exact_denial = case.exact_denial
        fixture_root = Path(self.tmp.name).resolve()
        artifact = fixture_root / 'independent-B-result.txt'
        reply_body = fixture_root / 'independent-B-reply.txt'
        reply_body.write_text('Independent B fixture completed; attached attributable result.')
        executed = []
        request = self.provider.request
        def unavailable_specialist(method, params, **kwargs):
            if method == 'thread/read' and params.get('threadId') == specialist_thread:
                return {'thread': {'id': specialist_thread, 'status': {'type': 'notLoaded'},
                                   'canAcceptDirectInput': False}}
            result = request(method, params, **kwargs)
            if method == 'thread/queue/start' and params.get('threadId') == worker_thread:
                observed = self.provider.history[worker_thread][-1]
                payload = '\n'.join(item['text'] for item in observed['content'] if item['type'] == 'text')
                recipient = self.cli_query(payload.split('\n', 1)[1].encode(),
                    '.deliveries[] | select(.message_id==$source) | .recipient_session',
                    source=runnable['source_request_id'])
                self.assertEqual(recipient, worker)
                self.assertNotIn(blocked['source_request_id'], payload)
                self.assertEqual(self.provider.history[specialist_thread], [])
                self.activity_cli(worker, 'message', runnable['source_request_id'])
                self.assertFalse(artifact.exists())
                # Existing Bridge dispatch triggers actual benign fixture work;
                # the provider observation remains simulated, not native proof.
                subprocess.run([sys.executable, '-c',
                    'from pathlib import Path; import sys; '
                    'Path(sys.argv[1]).write_text("Independent B result\\nsource="+sys.argv[2]+"\\nowner="+sys.argv[3]+"\\n")',
                    str(artifact), runnable['source_request_id'], worker],
                    check=True, capture_output=True, timeout=10)
                sent = self.activity_cli(worker, 'send', '--to', po, '--body-file', str(reply_body),
                    '--attach', str(artifact), '--reply-to', runnable['source_request_id'], '--ack-reply')
                self.activity_cli(worker, 'activity-transition', runnable['id'], '--state', 'completed')
                executed.append((observed['clientId'], self.cli_query(sent, '.id')))
            return result
        bridge = Bridge(state, self.provider)
        with mock.patch.object(self.provider, 'request', side_effect=unavailable_specialist):
            bridge.tick(observe_policies=False)
        self.assertEqual(len(self.provider.history[worker_thread]), 1)
        self.assertEqual(self.provider.history[specialist_thread], [])
        self.assertIn(diagnostic_source, {message['id'] for message in state.pending()})
        with self.activity() as activity:
            pending_snapshot = activity.snapshot()
        self.assertEqual(pending_snapshot['escalations'], [escalation])
        self.assertEqual(len(executed), 1)
        wake, reply = executed[0]
        self.assertEqual(state.job(wake)['status'], 'dispatched')
        receipt = self.activity_cli(worker, 'first-ack-lookup', runnable['source_request_id'],
                                    '--expected-recipient', po)
        self.assertEqual(self.cli_query(receipt, '.reply_id'), reply)
        consumed = self.activity_cli(po, 'message', reply)
        self.assertEqual(self.cli_query(consumed, '.message.reply_to'), runnable['source_request_id'])
        attachment = self.cli_query(consumed, '.message.attachments[0].id')
        downloaded = fixture_root / 'received-independent-B-result.txt'
        self.activity_cli(po, 'attachment-download', '--source-project', case.project,
            '--message-id', reply, '--attachment-id', attachment, '--output', str(downloaded))
        expected = 'Independent B result\nsource=' + runnable['source_request_id'] + '\nowner=' + worker + '\n'
        self.assertEqual(downloaded.read_text(), expected)
        self.assertEqual(downloaded.read_bytes(), artifact.read_bytes())
        pending_work = {row['id']: row for row in pending_snapshot['work']}
        self.assertEqual(pending_work[runnable['id']]['state'], 'completed')
        self.assertEqual(pending_work[blocked['id']]['state'], 'blocked')
        self.assertEqual(pending_work[diagnostic['id']]['state'], 'queued')
        self.assertEqual(self.provider.history[specialist_thread], [])

        # Only specialist availability changes; no task or input is resent.
        bridge.tick(observe_policies=False)
        Bridge(RemoteBridgeState(client, self.url, self.identity), self.provider).tick(observe_policies=False)
        self.assertEqual(len(self.provider.history[worker_thread]), 1)
        self.assertEqual(len(self.provider.history[specialist_thread]), 1)
        worker_payload = '\n'.join(item['text'] for item in self.provider.history[worker_thread][0]['content']
                                   if item['type'] == 'text')
        specialist_payload = '\n'.join(item['text'] for item in self.provider.history[specialist_thread][0]['content']
                                       if item['type'] == 'text')
        self.assertIn(runnable['source_request_id'], worker_payload)
        self.assertIn(diagnostic_source, specialist_payload)
        self.assertNotIn(blocked['source_request_id'], worker_payload + specialist_payload)
        self.assertNotIn('BLOCKED_ACTION_A', worker_payload + specialist_payload)
        with self.activity() as activity:
            final = activity.snapshot()
        work = {row['id']: row for row in final['work']}
        self.assertEqual((work[blocked['id']]['owner_session'], work[blocked['id']]['source_request_id'],
                          work[blocked['id']]['state']), (worker, blocked['source_request_id'], 'blocked'))
        self.assertEqual(work[diagnostic['id']]['owner_session'], specialist)
        self.assertEqual(final['escalations'][0]['denial_message_id'], denial)
        self.assertEqual(final['escalations'][0]['next_action'], next_action)
        self.assertEqual(final['escalations'][0]['denial_provenance'], 'owner_report')
        self.assertEqual(final['escalations'][0]['purpose'], 'diagnostic_repair')
        self.assertEqual(self.coord_call(client, po, 'message', id=denial)['message']['body'], exact_denial)
        self.assertEqual(len(executed), 1)
        print('PASS B executed actual benign fixture, exact CLI reply/ACK/artifact consumed while A blocked and specialist pending; sha256=' +
              hashlib.sha256(downloaded.read_bytes()).hexdigest() + '; native provider simulated', flush=True)

    def test_escalation_rejects_unrelated_tasks_and_unverified_report_associations(self):
        _, _, worker, origin, independent, _ = self.continuation_fixture()
        project, po, _, root, client = self.projects['Alpha']
        specialist, thread = 'specialist-negative', str(uuid.uuid4())
        self.coord_call(client, specialist, 'register', agent=specialist)
        self.coord_call(client, specialist, 'bind', thread=thread)
        self.provider.add(thread, str(Path(self.tmp.name).resolve() / 'Alpha'))
        self.call('context')
        self.root('delegate-management', project=project, po=po, target=specialist,
                  expected_thread=thread, expected_host='mac', request_id=str(uuid.uuid4()), confirm=True)
        self.coord_call(client, worker, 'message', id=origin['source_request_id'])
        denial = self.coord_call(client, worker, 'send', to=po, body='Exact owner report',
                                 reply_to=origin['source_request_id'], ack_reply=True)['id']
        diagnostic_source = self.message(po, specialist, 'Diagnose integration without retrying denied work')
        other_goal_source = self.message(root, po, 'Unrelated objective')
        unrelated_source = self.message(po, specialist, 'Unrelated contribution')
        with self.activity() as activity:
            activity.transition(origin['id'], 'blocked')
            diagnostic = activity.assign(origin['goal_id'], specialist, diagnostic_source, 'Diagnostic task')
            other_goal = activity.adopt(other_goal_source, 'Other objective')
            unrelated = activity.assign(other_goal['id'], specialist, unrelated_source, 'Unrelated task')
        beta_project, beta_po, beta_worker, beta_root, _ = self.projects['Beta']
        beta_goal_source = self.message(beta_root, beta_po, 'Other project objective', name='Beta')
        beta_source = self.message(beta_po, beta_worker, 'Other project task', name='Beta')
        with self.activity('Beta') as activity:
            beta_goal = activity.adopt(beta_goal_source, 'Beta objective')
            beta_task = activity.assign(beta_goal['id'], beta_worker, beta_source, 'Beta task')
        wrong_reporter = self.message(root, po, 'A different actor claims a denial')
        wrong_recipient = self.coord_call(client, worker, 'send', to=root, body='Report to another recipient')['id']
        self.coord_call(client, worker, 'message', id=independent['source_request_id'])
        wrong_source = self.coord_call(client, worker, 'send', to=po, body='Other assignment report',
                                       reply_to=independent['source_request_id'], ack_reply=True)['id']
        self.coord_call(client, po, 'context')
        unread_report = self.coord_call(client, worker, 'send', to=po, body='Unread owner report',
                                        reply_to=origin['source_request_id'])['id']
        next_action = 'Await integration diagnosis; preserve original denial.'
        with self.activity() as activity:
            for task in (origin, independent, unrelated, beta_task, other_goal):
                with self.subTest(diagnostic=task['id']), self.assertRaises(RemoteCoordError):
                    activity.link_escalation(origin['id'], task['id'], denial, next_action)
            for report in (wrong_reporter, wrong_recipient, wrong_source, unread_report, 'missing-report'):
                with self.subTest(report=report), self.assertRaises(RemoteCoordError):
                    activity.link_escalation(origin['id'], diagnostic['id'], report, next_action)
            self.assertEqual(activity.snapshot()['escalations'], [])
        with self.activity(actor=worker) as activity, self.assertRaises(RemoteCoordError):
            activity.link_escalation(origin['id'], diagnostic['id'], denial, next_action)
        with self.assertRaises(RemoteCoordError):
            self.coord_call(client, po, 'activity-link-escalation', origin_work_id=origin['id'],
                diagnostic_work_id=diagnostic['id'], denial_message_id=denial, next_action=next_action,
                denial_provenance='authenticated_native_decision')
        with self.activity() as activity:
            self.assertEqual(activity.snapshot()['escalations'], [])
            link = activity.link_escalation(origin['id'], diagnostic['id'], denial, next_action)
            self.assertEqual(link['denial_provenance'], 'owner_report')

    def test_dispatched_specialist_executes_fixture_and_reports_original_artifact(self):
        case = self.specialist_fixture()
        fixture_root = Path(self.tmp.name).resolve()
        fixture_log = fixture_root / 'integration-fixture.log'
        fixture_log.write_text('Fixture integration issue: original operation remains denied.\n')
        report = fixture_root / 'diagnostic-result.txt'
        reply_body = fixture_root / 'diagnostic-reply.txt'
        reply_body.write_text('Diagnostic fixture completed; attached evidence. Original denied work remains blocked.')
        executions = []
        request = self.provider.request
        def execute_diagnostic(method, params, **kwargs):
            result = request(method, params, **kwargs)
            if method == 'thread/queue/start' and params['threadId'] == case.specialist_thread:
                observed = self.provider.history[case.specialist_thread][-1]
                text = '\n'.join(item['text'] for item in observed['content'] if item['type'] == 'text')
                metadata = text.split('\n', 1)[1].encode()
                recipient = self.cli_query(metadata,
                    '.deliveries[] | select(.message_id==$source) | .recipient_session',
                    source=case.diagnostic_source)
                self.assertEqual(recipient, case.specialist)
                self.activity_cli(case.specialist, 'message', case.diagnostic_source)
                self.assertFalse(report.exists())
                # Actual benign subprocess work on fixture-owned files. The
                # native provider remains simulated; this is not model proof.
                subprocess.run([sys.executable, '-c',
                    'from pathlib import Path; import sys; '
                    'Path(sys.argv[2]).write_text("Diagnostic report\\n" + Path(sys.argv[1]).read_text())',
                    str(fixture_log), str(report)], check=True, capture_output=True, timeout=10)
                sent = self.activity_cli(case.specialist, 'send', '--to', case.po,
                    '--body-file', str(reply_body), '--attach', str(report),
                    '--reply-to', case.diagnostic_source, '--ack-reply')
                self.activity_cli(case.specialist, 'activity-transition', case.diagnostic['id'], '--state', 'completed')
                executions.append((observed['clientId'], sent))
            return result
        with mock.patch.object(self.provider, 'request', side_effect=execute_diagnostic):
            Bridge(case.state, self.provider).tick(observe_policies=False)
            Bridge(RemoteBridgeState(case.client, self.url, self.identity), self.provider).tick(observe_policies=False)
        self.assertEqual(len(executions), 1)
        wake, sent = executions[0]
        self.assertEqual(case.state.job(wake)['status'], 'dispatched')
        reply = self.cli_query(sent, '.id')
        receipt = self.activity_cli(case.specialist, 'first-ack-lookup', case.diagnostic_source,
                                    '--expected-recipient', case.po)
        self.assertEqual(self.cli_query(receipt, '.reply_id'), reply)
        consumed = self.activity_cli(case.po, 'message', reply)
        self.assertEqual(self.cli_query(consumed, '.message.reply_to'), case.diagnostic_source)
        attachment = self.cli_query(consumed, '.message.attachments[0].id')
        downloaded = fixture_root / 'received-diagnostic-result.txt'
        self.activity_cli(case.po, 'attachment-download', '--source-project', case.project,
            '--message-id', reply, '--attachment-id', attachment, '--output', str(downloaded))
        self.assertEqual(downloaded.read_bytes(), report.read_bytes())
        snapshot = self.activity_cli(case.po, 'activity-snapshot')
        self.assertEqual(self.cli_query(snapshot, '.work[] | select(.id==$work) | .state',
                                       work=case.diagnostic['id']), 'completed')
        self.assertEqual(self.cli_query(snapshot, '.work[] | select(.id==$work) | .state',
                                       work=case.blocked['id']), 'blocked')
        self.assertEqual(self.cli_query(snapshot, '.escalations[0].denial_message_id'), case.denial)
        self.assertEqual(self.cli_query(snapshot, '.escalations[0].next_action'), case.next_action)
        print('PASS real fixture subprocess + CLI reply/ACK/attachment consumption; artifact sha256=' +
              hashlib.sha256(downloaded.read_bytes()).hexdigest() + '; native provider simulated', flush=True)

    def test_root_mediated_crossproject_diagnostic_uses_scoped_receipts(self):
        state, thread, worker, blocked, runnable, _ = self.continuation_fixture()
        alpha, po, _, alpha_root, client = self.projects['Alpha']
        beta, beta_po, specialist, beta_root, beta_client = self.projects['Beta']
        directory = Path(self.tmp.name).resolve()
        def send_cli(name, sender, recipient, body, source=None):
            body_file = directory / ('route-' + str(uuid.uuid4()) + '.txt')
            body_file.write_text(body)
            args = ['send', '--to', recipient, '--body-file', str(body_file)]
            if source:
                args.extend(['--reply-to', source, '--ack-reply'])
            return self.cli_query(self.activity_cli(sender, *args, name=name), '.id')
        def read_cli(name, actor, message):
            return self.activity_cli(actor, 'message', message, name=name)
        denial_text = 'Actor-reported exact denial: operation A is refused; no other actor may retry it.'
        next_action = 'Original owner waits for integration diagnosis and legitimate eligibility; denied A remains blocked.'
        read_cli('Alpha', worker, blocked['source_request_id'])
        denial = send_cli('Alpha', worker, po, denial_text, blocked['source_request_id'])
        read_cli('Alpha', po, denial)
        with self.activity() as activity:
            activity.transition(blocked['id'], 'blocked')
            goal_source = next(row['source_request_id'] for row in activity.snapshot()['work']
                               if row['id'] == blocked['goal_id'])
        request = send_cli('Alpha', po, alpha_root,
            'Request distinct integration diagnosis in Beta. Denial report=' + denial + '\n' +
            denial_text + '\nNext action: ' + next_action, goal_source)
        # The original project progresses before Root or Beta accepts the request.
        Bridge(state, self.provider).tick(observe_policies=False)
        self.assertEqual(len(self.provider.history[thread]), 1)
        delivered = '\n'.join(item['text'] for item in self.provider.history[thread][0]['content']
                              if item['type'] == 'text')
        self.assertIn(runnable['source_request_id'], delivered)
        self.assertNotIn(blocked['source_request_id'], delivered)
        root_request = read_cli('Alpha', alpha_root, request)
        self.assertEqual(self.cli_query(root_request, '.message.reply_to'), goal_source)
        self.assertEqual(self.root('route', project=beta)['session'], beta_root)
        routed = send_cli('Beta', beta_root, beta_po,
            'DIAGNOSTIC ONLY. Root relay of Alpha request ' + request + '; denial=' + denial +
            '. Inspect fixture integration; never execute operation A. Original next action: ' + next_action)
        read_cli('Beta', beta_po, routed)
        goal_output = self.activity_cli(beta_po, 'activity-adopt', '--source-request-id', routed,
            '--title', 'Crossproject integration diagnosis', name='Beta')
        beta_goal = self.cli_query(goal_output, '.id')
        source = send_cli('Beta', beta_po, specialist,
            'DIAGNOSTIC_BETA_C: produce fixture diagnosis only; no execution of denied Alpha operation A.')
        diagnostic_output = self.activity_cli(beta_po, 'activity-assign', '--goal-id', beta_goal,
            '--worker', specialist, '--source-request-id', source, '--title', 'Diagnose fixture integration', name='Beta')
        diagnostic = self.cli_query(diagnostic_output, '.id')
        # The originating PO cannot mutate the target project or claim its task.
        self.activity_cli(po, 'activity-transition', diagnostic, '--state', 'completed', name='Beta', succeeds=False)
        with self.activity() as activity:
            with self.assertRaises(RemoteCoordError):
                activity.link_escalation(blocked['id'], diagnostic, denial, next_action)
        beta_state = RemoteBridgeState(beta_client, self.url, self.identity)
        specialist_thread = beta_state.resolve(specialist)['thread_id']
        report = directory / 'crossproject-diagnostic.txt'
        replies = []
        native_request = self.provider.request
        def execute_fixture(method, params, **kwargs):
            result = native_request(method, params, **kwargs)
            if method == 'thread/queue/start' and params['threadId'] == specialist_thread:
                observed = self.provider.history[specialist_thread][-1]
                payload = '\n'.join(item['text'] for item in observed['content'] if item['type'] == 'text')
                self.assertIn(source, payload)
                self.assertNotIn(blocked['source_request_id'], payload)
                read_cli('Beta', specialist, source)
                self.assertFalse(report.exists())
                subprocess.run([sys.executable, '-c',
                    'from pathlib import Path; import sys; Path(sys.argv[1]).write_text("Fixture diagnostic: integration repair required; original action remains denied.\\n")',
                    str(report)], check=True, capture_output=True, timeout=10)
                digest = hashlib.sha256(report.read_bytes()).hexdigest()
                replies.append(send_cli('Beta', specialist, beta_po, 'Fixture diagnostic completed. sha256=' + digest, source))
                self.activity_cli(specialist, 'activity-transition', diagnostic, '--state', 'completed', name='Beta')
            return result
        with mock.patch.object(self.provider, 'request', side_effect=execute_fixture):
            Bridge(beta_state, self.provider).tick(observe_policies=False)
            Bridge(beta_state, self.provider).tick(observe_policies=False)
        self.assertEqual(len(replies), 1)
        result = read_cli('Beta', beta_po, replies[0])
        result_body = self.cli_query(result, '.message.body')
        beta_reply = send_cli('Beta', beta_po, beta_root,
            'Destination-owned diagnostic=' + diagnostic + '; exact worker reply=' + replies[0] + '\n' + result_body, routed)
        read_cli('Beta', beta_root, beta_reply)
        alpha_reply = send_cli('Alpha', alpha_root, po,
            'Root relay: Beta goal=' + beta_goal + '; diagnostic=' + diagnostic + '; Beta PO reply=' + beta_reply +
            '\n' + result_body + '\nOriginal denial=' + denial + '\nNext action: ' + next_action, request)
        relayed = read_cli('Alpha', po, alpha_reply)
        self.assertEqual(self.cli_query(relayed, '.message.reply_to'), request)
        for name, sender, recipient, original, reply in (
                ('Alpha', worker, po, blocked['source_request_id'], denial),
                ('Beta', specialist, beta_po, source, replies[0]),
                ('Beta', beta_po, beta_root, routed, beta_reply),
                ('Alpha', alpha_root, po, request, alpha_reply)):
            receipt = self.activity_cli(sender, 'first-ack-lookup', original,
                '--expected-recipient', recipient, name=name)
            self.assertEqual(self.cli_query(receipt, '.reply_id'), reply)
        with self.activity() as activity:
            current = next(row for row in activity.snapshot()['work'] if row['id'] == blocked['id'])
            self.assertEqual((current['owner_session'], current['source_request_id'], current['state']),
                             (worker, blocked['source_request_id'], 'blocked'))
            self.assertEqual(activity.snapshot()['escalations'], [])
        beta_snapshot = self.activity_cli(beta_po, 'activity-snapshot', name='Beta')
        self.assertEqual(self.cli_query(beta_snapshot, '.work[] | select(.id==$work) | .state',
                                       work=diagnostic), 'completed')
        self.assertEqual(self.cli_query(beta_snapshot, '.work[] | select(.id==$work) | .owner_session',
                                       work=diagnostic), specialist)
        self.assertEqual(self.cli_query(beta_snapshot, '.work[] | select(.id==$work) | .source_request_id',
                                       work=beta_goal), routed)
        self.assertEqual(self.cli_query(read_cli('Alpha', po, denial), '.message.body'), denial_text)
        self.assertIn(next_action, self.cli_query(relayed, '.message.body'))
        self.assertEqual(len(self.provider.history[thread]), 1)
        print('PASS existing Root-scoped crossproject CLI routes, target-owned task, fixture execution and exact ACK receipts; typed crossproject escalation association unavailable; native provider simulated', flush=True)

    def test_assigned_work_lifecycle_and_restart(self):
        project, po, worker, root, _ = self.projects['Alpha']
        source = self.message(root, po, 'Implement feature requested by operator')
        assignment = self.message(po, worker, 'Implement bounded worker contribution')
        with self.activity() as activity:
            self.assertFalse(activity.snapshot()['active'])
            goal = activity.adopt(source, 'Feature goal')
            self.assertEqual(activity.adopt(source, 'Feature goal')['id'], goal['id'])
            with self.assertRaises(CoordError): activity.adopt(source, 'Changed intent')
            self.assertTrue(activity.snapshot()['active'])
            task = activity.assign(goal['id'], worker, assignment, 'Contribution')
            self.assertEqual(activity.assign(goal['id'], worker, assignment, 'Contribution')['id'], task['id'])
            activity.transition(goal['id'], 'completed')
            self.assertTrue(activity.snapshot()['active'])  # worker alone contributes
        with self.activity(actor=worker) as activity:
            activity.transition(task['id'], 'in_progress')
            activity.transition(task['id'], 'blocked')
            self.assertIn(task['id'], activity.snapshot()['blocked'])
            with self.assertRaises(CoordError): activity.transition(goal['id'], 'cancelled')
        result = subprocess.run([sys.executable, __file__, '--restart', str(self.db), project, worker, task['id']],
                                text=True, capture_output=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        print(result.stdout.strip(), flush=True)
        with self.activity(actor=worker) as activity:
            self.assertTrue(activity.snapshot()['active'])
            self.assertEqual([event['state'] for event in activity.history(task['id'])], ['queued','in_progress','blocked'])
            activity.transition(task['id'], 'cancelled')
            self.assertFalse(activity.snapshot()['active'])
            with self.assertRaises(CoordError): activity.transition(task['id'], 'in_progress')
        with self.activity('Beta') as other:
            self.assertFalse(other.snapshot()['active'])
            for operation in (lambda: other.adopt(source, 'foreign'),
                              lambda: other.transition(task['id'], 'completed'),
                              lambda: other.history(goal['id'])):
                with self.assertRaises(CoordError): operation()
        with contextlib.closing(Coordinator(self.server.projects.db_path(project), po)) as coord:
            with self.assertRaises(CoordError): ProjectActivity(self.server.projects, self.projects['Beta'][0], coord)
        source2 = self.message(root, po, 'Second feature')
        with self.activity() as activity:
            goal2 = activity.adopt(source2, 'Second')
            activity.transition(goal2['id'], 'blocked')
            self.assertTrue(activity.snapshot()['active'])
            activity.transition(goal2['id'], 'cancelled')
            self.assertFalse(activity.snapshot()['active'])
            self.assertEqual(len(activity.snapshot()['work']), 3)
        print('PASS lifecycle: duplicate, worker-only active, blocked, complete/cancel, fresh-process restart, cross-project rejection', flush=True)

    def test_authority_source_and_assignment_boundaries(self):
        project, po, worker, root, client = self.projects['Alpha']
        outsider = 'unassigned-actor'
        self.coord_call(client, outsider, 'register', agent=outsider)
        source = self.message(root, po, 'Authorized feature')
        assignment = self.message(po, worker, 'Authorized worker assignment')
        wrong_direction = self.message(worker, po, 'Worker cannot originate a PO goal')
        wrong_recipient = self.message(root, worker, 'Not routed to PO')
        outsider_assignment = self.message(po, outsider, 'No manager relationship')
        with self.activity(actor=outsider) as activity:
            with self.assertRaises(CoordError): activity.adopt(source, 'Unauthorized adoption')
            with self.assertRaises(CoordError): activity.snapshot()
        with self.activity() as activity:
            for invalid_source in (wrong_direction, wrong_recipient, 'message_missing'):
                with self.assertRaises(CoordError): activity.adopt(invalid_source, 'Invalid source')
            goal = activity.adopt(source, 'Authorized feature')
            with self.assertRaises(CoordError): activity.assign(goal['id'], outsider, outsider_assignment, 'Not delegated')
            with self.assertRaises(CoordError): activity.assign(goal['id'], self.projects['Beta'][2], assignment, 'Wrong project')
            with self.assertRaises(CoordError): activity.assign(goal['id'], worker, source, 'Wrong direction')
            with self.assertRaises(CoordError): activity.transition(goal['id'], 'done')
            self.assertEqual(len(activity.snapshot()['work']), 1)
        with self.activity(actor=worker) as activity:
            with self.assertRaises(CoordError): activity.adopt(source, 'Worker adoption')
            with self.assertRaises(CoordError): activity.assign(goal['id'], worker, assignment, 'Worker assignment')
        print('PASS non-PO, non-delegated/cross-project worker, wrong-direction/missing-source and invalid-state rejection', flush=True)

    def test_terminal_replay_and_owned_event_history(self):
        project, po, worker, root, _ = self.projects['Alpha']
        source = self.message(root, po, 'History feature')
        assignment = self.message(po, worker, 'History task')
        with self.activity() as activity:
            goal = activity.adopt(source, 'History feature')
            task = activity.assign(goal['id'], worker, assignment, 'History task')
            self.assertEqual((goal['owner_session'], goal['owner_role'], goal['source_request_id']),
                             (po, 'project_orchestrator', source))
            self.assertEqual((task['owner_session'], task['owner_role'], task['assigned_by'], task['goal_id'], task['source_request_id']),
                             (worker, 'worker', po, goal['id'], assignment))
        with self.activity(actor=worker) as activity:
            activity.transition(task['id'], 'in_progress')
        with self.activity() as activity:
            activity.transition(task['id'], 'blocked')
            activity.transition(goal['id'], 'completed')
            self.assertTrue(activity.snapshot()['active'])
        with self.activity(actor=worker) as activity:
            activity.transition(task['id'], 'completed')
            before = activity.history(task['id'])
            self.assertEqual([row['state'] for row in before], ['queued','in_progress','blocked','completed'])
            self.assertEqual([row['actor_session'] for row in before], [po,worker,po,worker])
            self.assertEqual([row['previous_state'] for row in before], [None,'queued','in_progress','blocked'])
            activity.transition(task['id'], 'completed')
            self.assertEqual(activity.history(task['id']), before)
            with self.assertRaises(CoordError): activity.transition(task['id'], 'cancelled')
            self.assertFalse(activity.snapshot()['active'])
        fresh_assignment = self.message(po, worker, 'New assignment after goal finished')
        with self.activity() as activity:
            self.assertEqual(activity.assign(goal['id'], worker, assignment, 'History task')['id'], task['id'])
            self.assertEqual(activity.adopt(source, 'History feature')['id'], goal['id'])
            with self.assertRaises(CoordError): activity.assign(goal['id'], worker, fresh_assignment, 'Too late')
            with self.assertRaises(CoordError): activity.transition(goal['id'], 'in_progress')
            self.assertEqual(len(activity.snapshot()['work']), 2)
        with self.activity(actor=root) as activity:
            self.assertFalse(activity.snapshot()['active'])
            with self.assertRaises(CoordError): activity.transition(task['id'], 'blocked')
        print('PASS immutable owner/source relationships, actor history, terminal idempotence and new-assignment rejection', flush=True)

    def test_concurrent_source_adoption_is_single(self):
        project, po, worker, root, _ = self.projects['Alpha']
        source = self.message(root, po, 'Concurrent original request')
        barrier = threading.Barrier(2)
        results, failures = [], []
        def adopt():
            try:
                with self.activity() as activity:
                    barrier.wait(timeout=5)
                    results.append(activity.adopt(source, 'Concurrent feature')['id'])
            except Exception as error:
                failures.append(error)
        threads = [threading.Thread(target=adopt) for _ in range(2)]
        for thread in threads: thread.start()
        for thread in threads:
            thread.join(10)
            self.assertFalse(thread.is_alive())
        self.assertEqual(failures, [])
        self.assertEqual(len(results), 2)
        self.assertEqual(len(set(results)), 1)
        with self.activity() as activity:
            self.assertEqual(len(activity.snapshot()['work']), 1)
            self.assertEqual(len(activity.history(results[0])), 1)
        print('PASS two concurrent HTTP/Coordinator adoptions: one durable work row and one creation event', flush=True)

    def test_active_project_readback_and_root_selection(self):
        alpha, po, worker, root, client = self.projects['Alpha']
        beta, beta_po, _, beta_root, _ = self.projects['Beta']
        def active():
            return self.client.call('/api/projects/rpc', {'op':'active'})['projects']
        self.assertEqual(active(), [])  # Registered/bound/managed agents are not assigned work.
        self.assertEqual(self.root('active-projects')['projects'], [])
        source = self.message(root, po, 'Select actual active project')
        assignment = self.message(po, worker, 'Persist a blocked contribution')
        with self.activity() as activity:
            goal = activity.adopt(source, 'Active Alpha')
            task = activity.assign(goal['id'], worker, assignment, 'Blocked Alpha task')
            activity.transition(goal['id'], 'completed')
            activity.transition(task['id'], 'blocked')
        self.assertEqual([item['id'] for item in active()], [alpha])
        summary = active()[0]['activity']
        self.assertEqual(summary, {'active':True, 'unfinished':1, 'blocked':1})
        selected = self.root('active-projects')['projects']
        self.assertEqual([item['project_id'] for item in selected], [alpha])
        self.assertEqual(selected[0]['activity'], summary)
        status, body, _ = self.request('GET', '/api/projects/active', auth='basic')
        self.assertEqual(status, 200)
        decoded = subprocess.run(['jq', '-r', '.projects[].id'], input=body, capture_output=True, check=True)
        self.assertEqual(decoded.stdout.decode().splitlines(), [alpha])
        self.assertEqual(self.request('GET', '/api/projects/active', auth=None)[0], 401)
        with self.assertRaises(RemoteCoordError):
            self.client.call('/api/root/rpc', dict(op='active-projects',session='a',host_id='mac',params={}))
        with self.assertRaises(CoordError):
            root_local.execute('active-projects', {}, master_db=self.db.resolve(),
                service_plist=self.service_plist, home_project='default',session='a',host_id='wrong-host')
        with self.assertRaises(RemoteCoordError):
            self.client.call('/api/projects/rpc', {'op':'active','actor':po})
        source2 = self.message(beta_root, beta_po, 'Second assigned project', name='Beta')
        with self.activity('Beta') as activity:
            goal2 = activity.adopt(source2, 'Active Beta')
        self.assertEqual({item['id'] for item in active()}, {alpha,beta})
        with self.activity(actor=worker) as activity:
            activity.transition(task['id'], 'completed')
        self.assertEqual([item['project_id'] for item in self.root('active-projects')['projects']], [beta])
        with self.activity('Beta') as activity:
            activity.transition(goal2['id'], 'cancelled')
        self.assertEqual(active(), [])
        self.assertEqual(self.root('active-projects')['projects'], [])
        print('PASS persisted all-active metadata and authorized Root selection, blocked worker-only ACTIVE, terminal removal and no busy/registration heuristic', flush=True)

    def test_sidebar_project_list_and_active_filter_contract(self):
        """Exercise the sidebar's HTTP rows and actual JS predicate, without a DOM."""
        alpha, po, worker, root, _ = self.projects['Alpha']
        beta, beta_po, _, beta_root, _ = self.projects['Beta']
        app = Path(__file__).resolve().parents[1] / 'src/agent_chat/web/app.js'
        filter_script = r'''
const fs = require('fs');
const vm = require('vm');
const source = fs.readFileSync(process.argv[1], 'utf8');
const marker = 'const projectButtons = data.projects.filter(';
if (source.split(marker).length !== 2) throw Error('sidebar filter location changed');
const expression = source.split(marker)[1].split(').map(project => {')[0];
const predicate = vm.runInNewContext('(' + expression + ')', {
  state: { activeProjectsOnly: process.argv[2] === 'true' }
});
const rows = fs.readFileSync(0, 'utf8').trim().split('\n').filter(Boolean).map(line => {
  const [id, active] = line.split('\t');
  return {id, activity: {active: active === 'true'}};
});
process.stdout.write(rows.filter(predicate).map(project => project.id).join('\n'));
'''
        def verify_payload(body, expected):
            # jq owns JSON validation/extraction; Node runs the shipped predicate
            # on its tab-separated output, never parsing the response as JSON.
            contract = '''.projects | length > 0 and all(.[];
                (.activity.active | type) == "boolean" and
                (.activity.unfinished | type) == "number" and
                (.activity.blocked | type) == "number" and
                (.measurement_running | type) == "boolean" and
                (.activity | keys) == ["active", "blocked", "unfinished"])'''
            subprocess.run(['jq', '-e', contract], input=body,
                           capture_output=True, check=True)
            rows = subprocess.run(['jq', '-r', '.projects[] | [.id, .activity.active] | @tsv'],
                                  input=body, capture_output=True, check=True).stdout
            for enabled in ('false', 'true'):
                visible = subprocess.run(['node', '-e', filter_script, str(app), enabled],
                                         input=rows, capture_output=True, check=True)
                ids = set(visible.stdout.decode().splitlines())
                if enabled == 'true':
                    self.assertEqual(ids, expected)
                else:
                    self.assertTrue({alpha, beta}.issubset(ids))
            return body

        def check_lists(expected):
            for route in ('/api/projects', '/api/config'):
                status, body, _ = self.request('GET', route + '?project=' + alpha, auth='basic')
                self.assertEqual(status, 200)
                verify_payload(body, expected)
                self.assertEqual(self.request('GET', route, auth=None)[0], 401)
            return body

        body = check_lists(set())  # Registration alone is not ACTIVE.
        missing = subprocess.run(['jq', 'del(.projects[].activity)'], input=body,
                                 capture_output=True, check=True).stdout
        with self.assertRaises(subprocess.CalledProcessError):
            verify_payload(missing, set())  # Missing metadata cannot pass this contract.

        source = self.message(root, po, 'Sidebar active project selection')
        assignment = self.message(po, worker, 'Sidebar blocked contribution')
        with self.activity() as activity:
            goal = activity.adopt(source, 'Sidebar Alpha')
            task = activity.assign(goal['id'], worker, assignment, 'Sidebar task')
            activity.transition(goal['id'], 'completed')
            activity.transition(task['id'], 'blocked')
        check_lists({alpha})
        with self.activity(actor=worker) as activity:
            activity.transition(task['id'], 'completed')
        check_lists(set())
        beta_source = self.message(beta_root, beta_po, 'Sidebar second project', name='Beta')
        with self.activity('Beta') as activity:
            beta_goal = activity.adopt(beta_source, 'Sidebar Beta')
        check_lists({beta})
        with self.activity('Beta') as activity:
            activity.transition(beta_goal['id'], 'cancelled')
        check_lists(set())
        print('PASS authenticated sidebar/config rows and shipped filter: inactive, blocked worker-only, queued, completed/cancelled; missing activity rejected; no DOM claim', flush=True)

    def test_http_activity_auth_and_strict_scope(self):
        project, po, worker, root, client = self.projects['Alpha']
        bad = HttpClient(self.url, 'invalid-credential', project=project)
        with self.assertRaisesRegex(RemoteCoordError, 'Authentication required'):
            self.coord_call(bad, po, 'activity-snapshot')
        with self.assertRaisesRegex(RemoteCoordError, 'different remote host'):
            client.call('/api/coord', dict(op='activity-snapshot', session=po,
                        host_id='not-the-owning-host', params={}))
        for params in ({'actor':po}, {'session':po}, {'project':self.projects['Beta'][0]},
                       {'source_request_id':'invented'}, {'active':True}):
            with self.subTest(params=params), self.assertRaisesRegex(RemoteCoordError, 'invalid project activity parameters'):
                client.call('/api/coord', dict(op='activity-snapshot', session=po, host_id='mac', params=params))
        for params in ({'title':'missing source'}, {'source_request_id':[], 'title':'wrong type'},
                       {'source_request_id':'missing', 'title':''}):
            with self.subTest(params=params), self.assertRaises(RemoteCoordError):
                self.coord_call(client, po, 'activity-adopt', **params)
        beta = self.projects['Beta'][4]
        with self.assertRaises(RemoteCoordError):
            self.coord_call(beta, po, 'activity-snapshot')
        with self.activity() as activity:
            self.assertFalse(activity.snapshot()['active'])
            self.assertEqual(activity.snapshot()['work'], [])
        print('PASS HTTP credentials/host identity, strict actor/project fields and cross-project session rejection', flush=True)

    def test_local_provider_checks_and_shared_bearer_remain_enforced(self):
        with self.assertRaisesRegex(RemoteCoordError, 'shared bearer permits only exact root status readbacks'):
            self.client.call('/api/root/rpc', dict(op='enroll', session='a', host_id='mac',
                params=dict(expected_thread=self.root_thread, confirm=True)))
        with self.assertRaisesRegex(CoordError, 'canonical identity differs'):
            root_local.execute('status', {}, master_db=self.db.resolve(), service_plist=self.service_plist,
                               home_project='default', session='a', host_id='wrong-host')
        with self.assertRaisesRegex(CoordError, 'canonical configured master'):
            root_local.execute('status', {}, master_db=self.db.resolve().with_name('wrong.sqlite3'),
                               service_plist=self.service_plist, home_project='default', session='a', host_id='mac')
        original = self.service_plist.read_bytes()
        try:
            with self.service_plist.open('wb') as stream:
                plistlib.dump({'Label':'not-the-configured-provider'}, stream)
            with self.assertRaisesRegex(CoordError, 'unsupported shape'):
                self.root('status')
        finally:
            self.service_plist.write_bytes(original)
        self.assertTrue(self.root('status')['enabled'])
        print('PASS bearer write refusal and genuine local host/master/provider-shape checks', flush=True)

if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == '--restart':
        database, project, actor, task = sys.argv[2:]
        projects = Projects(database)
        with contextlib.closing(Coordinator(projects.db_path(project), actor)) as coord:
            activity = projects.activity(project, coord)
            assert activity.snapshot()['active']
            assert task in activity.snapshot()['blocked']
            assert activity.history(task)[-1]['state'] == 'blocked'
        print('Fresh-process restart: durable blocked task and ACTIVE confirmed')
    else:
        unittest.main()
