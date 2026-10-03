import argparse
import contextlib
import copy
import io
import os
from pathlib import Path
import subprocess
import time
import uuid
from unittest import mock

from agent_chat import cli
from agent_chat.bridge import Bridge
from agent_chat.bridge_client import RemoteBridgeState
from agent_chat.core import Coordinator
from agent_chat.remote import HttpClient, RemoteCoordError
from agent_chat.root_orchestrator import RootOrchestrator
from agent_chat.rpc import TransportError
from test_remote_web import RemoteWebFixture, TOKEN
from test_session_reset import ResetRpc


class ScopedRpc(ResetRpc):
    def __init__(self, thread, cwd):
        super().__init__(thread)
        self.original.update(cwd=cwd, runtimeWorkspaceRoots=[cwd],
            activePermissionProfile={'id':'global-policy', 'extends':None},
            sandbox=dict(type='workspaceWrite', writableRoots=['/projects/other'], networkAccess=True))
        self.started = {}

    def request(self, method, params):
        if method == 'thread/resume' and params['threadId'] in self.started:
            return copy.deepcopy(self.started[params['threadId']])
        result = super().request(method, params)
        if method == 'thread/start':
            result.update(cwd=params['cwd'], runtimeWorkspaceRoots=params['runtimeWorkspaceRoots'],
                model=params['model'], activePermissionProfile={'id':':workspace', 'extends':None},
                sandbox=dict(type='workspaceWrite', writableRoots=[], networkAccess=False,
                    excludeTmpdirEnvVar=False, excludeSlashTmp=False),
                reasoningEffort=params.get('config', {}).get('model_reasoning_effort'))
            if self.changed_permissions:
                result['sandbox'] = {'type':'dangerFullAccess'}
            self.started[result['thread']['id']] = copy.deepcopy(result)
        return result


class RootOrchestratorTests(RemoteWebFixture):
    def setUp(self):
        super().setUp()
        self.call('register', agent='root')
        self.thread_id = str(uuid.uuid4())
        self.call('bind', thread=self.thread_id)
        self.neutral = Path(self.tmp.name).resolve() / 'neutral'; self.neutral.mkdir()
        self.workspace = Path(self.tmp.name).resolve() / 'project-a'; self.workspace.mkdir()
        self.rpc = ScopedRpc(self.thread_id, str(self.neutral))
        self.identity = dict(owner='root-fixture-worker', secret='x'*40, host_id='mac')
        self.state = RemoteBridgeState(self.client, self.url, self.identity)
        self.state.call('acquire')
        self.bridge = Bridge(self.state, self.rpc)
        self.call('agent-status', refresh=True, expected_thread=self.thread_id, request_id=str(uuid.uuid4()))
        self.bridge.tick()
        self.root('enroll', expected_thread=self.thread_id, confirm=True)

    def root(self, op, session='a', host='mac', **params):
        return self.client.call('/api/root/rpc', dict(op=op, session=session, host_id=host, params=params))

    def project(self, name='Project A'):
        result = self.client.call('/api/projects/rpc', dict(op='create', name=name))['project']['id']
        self.call('context')  # Consume the canonical project-created notice.
        return result

    def routed(self, project, op, **params):
        route = self.root('route', project=project)
        client = HttpClient(self.url, TOKEN, project=project)
        return client.call('/api/coord', dict(op=op, session=route['session'], host_id='mac', params=params))

    def configure(self, project, **params):
        values = dict(project=project, agent='orchestrator', cwd=str(self.workspace),
            prompt='Read local checkpoint. Plan only; no resource mutation.', request_id=str(uuid.uuid4()),
            confirm=True, model='test-model', effort='high')
        values.update(params)
        return self.root('configure', **values)

    def project_bridge(self, project):
        client = HttpClient(self.url, TOKEN, project=project)
        state = RemoteBridgeState(client, self.url, self.identity)
        state.call('acquire')
        return state, Bridge(state, self.rpc)

    def test_new_project_joins_same_thread_with_distinct_host_pinned_identity_once(self):
        first, second = self.project(), self.project('Project B')
        routes = [self.root('route', project=p) for p in ('default', first, second)]
        self.assertEqual(len({r['session'] for r in routes}), 3)
        for route in routes:
            status = self.routed(route['project'], 'bridge-status', mine=True)
            self.assertEqual(status['bindings'][0]['thread_id'], self.thread_id)
        self.assertEqual(self.root('route', project=first), routes[1])
        self.assertEqual(len(self.call('inbox')['messages']), 2)
        self.assertFalse(self.rpc.started)  # Registry onboarding never starts a model.

    def test_quiet_projection_excludes_broadcast_but_keeps_explicit_report_and_ack(self):
        project = self.project()
        client = HttpClient(self.url, TOKEN, project=project)
        send = lambda op, **p: client.call('/api/coord', dict(op=op, session='worker', host_id='mac', params=p))
        send('register', agent='worker')
        send('send', body='Detailed project chatter', to=None)
        self.assertEqual(self.routed(project, 'inbox')['messages'], [])
        route = self.root('route', project=project)
        report = send('send', to=route['session'], body='Project A: decision needed; checkpoint /project-a/status.md')
        self.assertEqual(self.routed(project, 'message', id=report['id'])['message']['body'], 'Project A: decision needed; checkpoint /project-a/status.md')
        reply = self.routed(project, 'send', to='worker', body='Proceed within approved scope.', reply_to=report['id'], ack_reply=True)
        self.assertTrue(reply['acknowledged_reply'])
        self.assertEqual(self.call('inbox')['messages'][0]['body'].split(':')[0], 'Project registered')

    def test_saved_ui_assignment_provisions_once_without_brief_in_root_notice(self):
        brief = 'Create a calendar.\nKeep literal $(do-not-execute) and 日本語.\n'
        created = self.client.call('/api/projects/rpc', dict(op='create', name='Calendar',
            workspace=str(self.workspace), instructions=brief))['project']
        project = created['id']
        self.assertNotIn('instructions', created)
        self.assertTrue(created['has_instructions'])
        notices = self.call('context')['messages']
        notice = next(message for message in notices if project in message['body'])
        self.assertIn('root provision', notice['body'])
        self.assertNotIn(brief, notice['body'])
        ready = next(row for row in self.root('status')['projects'] if row['project_id'] == project)
        self.assertTrue(ready['provision_ready'])
        self.assertEqual(ready['setup'], 'ready_to_provision')
        self.assertNotIn('instructions', ready)
        params = dict(project=project, agent='orchestrator', request_id=str(uuid.uuid4()),
            confirm=True, model='test-model', effort='high')
        first = self.root('provision', **params)
        again = self.root('provision', **params)
        self.assertEqual(first['creation']['id'], again['creation']['id'])
        state, bridge = self.project_bridge(project)
        bridge.tick()
        status = self.routed(project, 'session-reset-status', to='orchestrator')
        self.assertEqual(status['resets'][0]['status'], 'completed')
        text = self.rpc.history[status['thread_id']][0]['content'][0]['text']
        self.assertTrue(text.endswith(brief))
        self.assertIn('Act as project orchestrator for Calendar', text)
        self.assertIn(project, text)
        self.assertEqual(len(self.rpc.history[status['thread_id']]), 1)
        self.assertEqual(self.rpc.started[status['thread_id']]['runtimeWorkspaceRoots'], [str(self.workspace)])
        renamed = self.client.call('/api/projects/rpc', dict(op='rename', id=project, name='Calendar renamed'))['project']
        self.assertNotIn('instructions', renamed)
        self.assertTrue(renamed['has_instructions'])
        self.assertEqual(self.server.projects.get(project)['instructions'], brief)

    def test_provision_missing_assignment_or_overlapping_workspace_dispatches_nothing(self):
        project = self.project()
        params = dict(project=project, agent='orchestrator', request_id=str(uuid.uuid4()),
            confirm=True, model=None, effort=None)
        with self.assertRaisesRegex(RemoteCoordError, 'workspace and instructions'):
            self.root('provision', **params)
        other = self.client.call('/api/projects/rpc', dict(op='create', name='Overlap',
            workspace=str(self.neutral), instructions='Do work.'))['project']['id']
        self.call('context')
        params.update(project=other, request_id=str(uuid.uuid4()))
        with self.assertRaisesRegex(RemoteCoordError, 'neutral'):
            self.root('provision', **params)
        self.assertEqual(self.rpc.started, {})

    def test_cli_provisions_saved_creation_without_reading_a_root_prompt_file(self):
        project = self.client.call('/api/projects/rpc', dict(op='create', name='CLI project',
            workspace=str(self.workspace), instructions='Plan only.'))['project']['id']
        self.call('context')
        with mock.patch.dict(os.environ, {'AGENT_CHAT_HOST_ID':'mac'}), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(cli.main(['--server', self.url, '--api-token', TOKEN, '--session', 'a',
                'root', 'provision', '--project-id', project, '--model', 'test-model',
                '--reasoning-effort', 'high', '--request-id', str(uuid.uuid4()), '--confirm']), 0)
        jobs = self.routed(project, 'session-reset-status', to='orchestrator')['resets']
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]['status'], 'pending')

    def test_creation_narrows_root_grants_and_dispatches_exact_project_prompt_once(self):
        project = self.project()
        result = self.configure(project)
        state, bridge = self.project_bridge(project)
        bridge.tick()
        status = self.routed(project, 'session-reset-status', to='orchestrator')
        self.assertEqual(status['resets'][0]['status'], 'completed')
        settings = self.rpc.started[status['thread_id']]
        self.assertEqual(settings['runtimeWorkspaceRoots'], [str(self.workspace)])
        self.assertEqual(settings['sandbox']['writableRoots'], [])
        self.assertFalse(settings['sandbox']['networkAccess'])
        self.assertEqual(settings['activePermissionProfile']['id'], ':workspace')
        text = self.rpc.history[status['thread_id']][0]['content'][0]['text']
        self.assertIn(project, text)
        self.assertIn(result['root_session'], text)
        self.assertNotIn('global-policy', text)
        # Identical lost-response retry does not create a second registration/thread.
        retry = self.configure(project, request_id=result['creation']['id'])
        bridge.tick()
        self.assertEqual(retry['creation']['target_session'], result['creation']['target_session'])
        self.assertEqual(len(self.rpc.started), 1)

    def test_legacy_worker_cannot_obtain_or_advance_scoped_creation(self):
        project = self.project(); result = self.configure(project)
        state, _ = self.project_bridge(project)
        self.assertEqual(state.call('reset-jobs'), [])
        with self.assertRaisesRegex(RemoteCoordError, 'upgrade the existing bridge'):
            state.call('reset-update', job_id=result['creation']['id'], expected='pending', status='creating')
        self.assertEqual(len(state.reset_jobs()), 1)
        self.assertFalse(self.rpc.started)

    def test_permissions_mismatch_withholds_binding_and_prompt(self):
        project = self.project(); self.configure(project)
        self.rpc.changed_permissions = True
        _, bridge = self.project_bridge(project); bridge.tick()
        status = self.routed(project, 'session-reset-status', to='orchestrator')
        self.assertEqual(status['resets'][0]['status'], 'created')
        self.assertIsNone(status['thread_id'])
        self.assertIn('settings differ', status['resets'][0]['error'])
        self.assertEqual(self.rpc.history[self.rpc.new], [])

    def test_lost_creation_response_stays_uncertain_and_never_replays(self):
        project = self.project(); self.configure(project)
        self.rpc.drop = 'thread/start'
        _, bridge = self.project_bridge(project)
        with self.assertRaises(TransportError): bridge.tick()
        bridge.tick()
        status = self.routed(project, 'session-reset-status', to='orchestrator')['resets'][0]
        self.assertEqual(status['status'], 'uncertain')
        self.assertEqual(len([m for m,p in self.rpc.calls if m == 'thread/start']), 1)
        self.assertEqual(self.rpc.history[self.rpc.new], [])

    def test_root_never_borrows_peer_or_resource_ownership(self):
        project = self.project()
        with self.assertRaisesRegex(RemoteCoordError, 'canonical home'):
            self.root('route', session='worker', project=project)
        with self.assertRaisesRegex(RemoteCoordError, 'canonical home'):
            self.root('route', host='other-host', project=project)
        with self.assertRaisesRegex(RemoteCoordError, 'control-only'):
            self.routed(project, 'request', resource='shared', minutes=5)
        with self.assertRaisesRegex(RemoteCoordError, 'root configure'):
            self.routed(project, 'agent-create', agent='bad', expected_thread=self.thread_id,
                prompt='bad', request_id=str(uuid.uuid4()), confirm=True, cwd='/projects/other')
        self.assertFalse(self.rpc.started)

    def test_root_local_reset_stop_rebind_and_removal_are_refused(self):
        for op, params in [('session-reset',dict(expected_thread=self.thread_id,prompt='fresh',request_id=str(uuid.uuid4()),confirm=True)),
                           ('agent-stop',dict(expected_thread=self.thread_id,request_id=str(uuid.uuid4()),confirm=True)),
                           ('unbind',{}), ('remove-session',dict(id='a'))]:
            with self.subTest(op=op), self.assertRaisesRegex(RemoteCoordError, 'global root binding'):
                self.call(op, **params)

    def test_neutral_and_distinct_project_workspaces_are_required(self):
        project = self.project()
        with self.assertRaisesRegex(RemoteCoordError, 'neutral'):
            self.configure(project, cwd=str(self.neutral))
        self.configure(project)
        second = self.project('Second')
        with self.assertRaisesRegex(RemoteCoordError, 'overlaps'):
            self.configure(second, cwd=str(self.workspace / 'child'))

    def test_partial_project_join_repairs_same_registration_and_retains_errors(self):
        with mock.patch.object(RootOrchestrator, 'join', side_effect=OSError('temporary onboarding failure')):
            project = self.project()
        snapshot = self.root('status')
        row = next(p for p in snapshot['projects'] if p['project_id'] == project)
        self.assertTrue(row['joined'])
        self.assertEqual(self.root('route', project=project)['session'], row['session_id'])
        self.assertEqual(len(self.call('inbox')['messages']), 1)

    def test_missing_database_never_creates_replacement(self):
        project = self.project(); path = self.server.projects.db_path(project)
        path.unlink()
        with self.assertRaisesRegex(RemoteCoordError, 'missing'):
            self.root('route', project=project)
        self.assertFalse(path.exists())

    def test_existing_team_adoption_changes_no_binding_or_holds(self):
        project = self.project(); client = HttpClient(self.url, TOKEN, project=project)
        call = lambda op, **p: client.call('/api/coord', dict(op=op,session='existing',host_id='mac',params=p))
        call('register', agent='existing')
        target = str(uuid.uuid4()); call('bind', thread=target)
        self.rpc.status[target], self.rpc.queues[target], self.rpc.history[target] = 'idle', [], []
        observed = copy.deepcopy(self.rpc.original); observed['cwd'] = str(self.workspace)
        self.rpc.started[target] = observed
        self.routed(project, 'agent-status', to='existing', refresh=True, expected_thread=target, request_id=str(uuid.uuid4()))
        _, bridge = self.project_bridge(project); bridge.tick()
        claim = call('request', resource='existing-held', minutes=5)
        status = self.root('adopt', project=project, target='existing', expected_thread=target, cwd=str(self.workspace), confirm=True)
        self.assertEqual(next(p for p in status['projects'] if p['project_id'] == project)['setup'], 'adopted')
        self.assertEqual(call('check', resource='existing-held', token=claim['token'])['reservation_id'], claim['reservation_id'])
        self.assertEqual(call('bridge-status', mine=True)['bindings'][0]['thread_id'], target)

    def test_detach_with_unread_delivery_pauses_without_cancel_then_resume(self):
        project = self.project(); route = self.root('route', project=project)
        client = HttpClient(self.url, TOKEN, project=project)
        client.call('/api/coord', dict(op='register',session='worker',host_id='mac',params={'agent':'worker'}))
        msg = client.call('/api/coord', dict(op='send',session='worker',host_id='mac',params={'to':route['session'],'body':'Decision needed'}))
        # Consume the home notice too, but retain the target delivery untouched.
        for item in self.call('inbox')['messages']: self.call('acknowledge', id=item['id'])
        with self.assertRaises(RemoteCoordError): self.root('detach', confirm=True)
        state, bridge = self.project_bridge(project)
        bridge.tick()
        self.assertEqual(self.rpc.queues[self.thread_id], [])
        self.assertEqual(self.routed(project,'inbox')['messages'][0]['id'], msg['id'])
        self.root('resume-routing', confirm=True)
        self.assertEqual(self.root('status')['root']['mode'], 'running')

    def test_clean_detach_retains_inboxes_and_home_binding_only(self):
        project = self.project(); route = self.root('route', project=project)
        for item in self.call('inbox')['messages']: self.call('acknowledge', id=item['id'])
        self.routed(project, 'context')
        self.assertTrue(self.root('detach', confirm=True)['registrations_retained'])
        self.assertFalse(self.server.roots.status()['enabled'])
        self.assertEqual(self.call('bridge-status', mine=True)['bindings'][0]['thread_id'], self.thread_id)
        client = HttpClient(self.url, TOKEN, project=project)
        result = client.call('/api/coord', dict(op='bridge-status',session=route['session'],host_id='mac',params={'mine':True}))
        self.assertEqual(result['bindings'], [])

    def test_cli_routes_only_own_projection_with_explicit_project_output(self):
        project = self.project()
        args = argparse.Namespace(session='a', api_token=TOKEN, project='default')
        out = io.StringIO()
        with mock.patch.dict(os.environ, {'AGENT_CHAT_HOST_ID':'mac'}), contextlib.redirect_stdout(out):
            self.assertEqual(cli._root_main(['in',project,'agents'], args, self.url), 0)
        checked = subprocess.run(['jq','-er','.project'], input=out.getvalue(), text=True, capture_output=True, check=True)
        self.assertEqual(checked.stdout.strip(), project)
        with self.assertRaisesRegex(Exception, 'management only'):
            cli._root_main(['in',project,'register','--agent','borrowed'], args, self.url)

    def test_browser_metadata_links_independent_project_main_to_global_root(self):
        project = self.project(); result = self.configure(project)
        _, bridge = self.project_bridge(project); bridge.tick()
        _, body, _ = self.request('GET','/api/snapshot?project='+project)
        root = self.root('route', project=project)['session']
        query = f'.sessions[] | select(.id=="{result["creation"]["target_session"]}") | .orchestration_parent'
        parent = subprocess.run(['jq','-er',query], input=body, capture_output=True, check=True).stdout.decode().strip()
        self.assertEqual(parent, root)

    def test_browser_root_template_cannot_bypass_project_orchestrator_hierarchy(self):
        self.configure('default'); self.call('context')
        status, body, _ = self.request('POST', '/api/sessions/reset', dict(op='create', agent='browser-worker',
            template_session='a', expected_thread=self.thread_id, prompt='Task only.', request_id=str(uuid.uuid4()), confirm=True),
            headers={'X-Agent-Chat-CSRF':self.server.csrf_token})
        self.assertEqual(status, 400)
        self.assertIn(b'root configure', body)
        self.assertEqual(len(self.state.reset_jobs()), 1)
        self.assertFalse(self.rpc.started)

    def test_root_manages_project_orchestrator_not_workers(self):
        project = self.project(); created = self.configure(project)
        state, bridge = self.project_bridge(project); bridge.tick()
        main = created['creation']['target_session']; thread = self.rpc.new
        client = HttpClient(self.url, TOKEN, project=project)
        def main_call(op, **params):
            return client.call('/api/coord',dict(op=op,session=main,host_id='mac',params=params))
        worker = main_call('agent-create',agent='worker',expected_thread=thread,prompt='Worker task.',request_id=str(uuid.uuid4()),confirm=True)
        # A worker can be pending/unbound; root still cannot retire it.
        with self.assertRaisesRegex(RemoteCoordError, 'project orchestrators only'):
            self.routed(project,'remove-session',id=worker['target_session'])
        with self.assertRaisesRegex(RemoteCoordError, 'project orchestrators only'):
            self.routed(project,'agent-stop',to=worker['target_session'],expected_thread=thread,request_id=str(uuid.uuid4()),confirm=True)
        with self.assertRaisesRegex(RemoteCoordError, 'root configure'):
            self.routed(project,'agent-create',agent='direct-worker',expected_thread=self.thread_id,prompt='Task.',request_id=str(uuid.uuid4()),confirm=True)
        self.assertNotIn(worker['target_session'],[row['id'] for row in self.routed(project,'agents')['agents']])
        stopped = self.routed(project,'agent-stop',to=main,expected_thread=thread,request_id=str(uuid.uuid4()),confirm=True)
        self.assertEqual(stopped['target_session'],main)

    def test_root_screen_excludes_project_technical_chatter_and_cannot_override_home(self):
        self.call('register', session='worker', agent='worker')
        self.call('send', session='worker', to=self.server.sender_session, body='Technical implementation must stay local')
        technical = self.call('send',session='worker',to='a',body='Hidden technical reply context')
        with self.assertRaisesRegex(RemoteCoordError,'same conversation'):
            self.call('send',to=self.server.sender_session,body='Invalid crossing',reply_to=technical['id'])
        sent = self.call('send',to=self.server.sender_session,body='Root decision report')
        project = self.project()
        for path in ('/api/root/snapshot','/api/root/messages'):
            status, body, _ = self.request('GET',path+'?project='+project)
            self.assertEqual(status,200)
            names = subprocess.run(['jq','-r','.messages[].body'],input=body,capture_output=True,check=True).stdout.decode()
            self.assertIn('Root decision report',names)
            self.assertNotIn('Technical implementation',names)
        status, _, _ = self.request('GET','/api/root/messages?agent=worker')
        self.assertEqual(status,400)
        hidden = self.call('send', session='worker',to=self.server.sender_session,body='Private detail')
        status, _, _ = self.request('GET','/api/root/messages/'+hidden['id'])
        self.assertEqual(status,404)
        status, _, _ = self.request('GET','/root',auth=None)
        self.assertEqual(status,401)

    def view_field(self, path, expression):
        status, body, _ = self.request('GET', path)
        self.assertEqual(status, 200, path)
        return subprocess.run(['jq', '-er', expression], input=body,
            capture_output=True, check=True).stdout.decode().strip()

    def test_project_feed_excludes_private_root_traffic_before_count_and_pagination(self):
        operator = self.server.sender_session
        self.call('register', session='worker', agent='worker')
        local = self.call('send', session='worker', to=operator, body='Project worker report')
        coordination = self.call('send', to='worker', body='Root project coordination')
        for index in range(55):
            self.call('send', to=operator, body=f'Private root report {index}')
        private = self.call('send', session=operator, to='a', body='Private operator request')
        reply = self.call('send', to=operator, body='Private root answer',
            reply_to=private['id'], ack_reply=True)
        self.assertTrue(reply['acknowledged_reply'])
        self.assertEqual(self.view_field('/api/snapshot', '.total_messages'), '2')
        self.assertEqual(self.view_field('/api/snapshot', '.history_truncated | tostring'), 'false')
        self.assertEqual(self.view_field('/api/snapshot', '.messages | length'), '2')
        coordination_seq = self.view_field('/api/messages/'+coordination['id'], '.seq')
        local_seq = self.view_field('/api/messages/'+local['id'], '.seq')
        first = self.view_field('/api/messages?limit=1', '.messages[0].id')
        self.assertEqual(first, coordination['id'])
        self.assertEqual(self.view_field('/api/messages?before='+coordination_seq+'&limit=1',
            '.messages[0].id'), local['id'])
        self.assertEqual(self.view_field('/api/messages?after='+local_seq+'&limit=1',
            '.messages[0].id'), coordination['id'])
        self.assertEqual(self.view_field('/api/messages?agent=a', '.messages | length'), '0')
        self.assertEqual(self.view_field('/api/messages?to_me=1', '.messages | length'), '1')
        self.assertEqual(self.request('GET', '/api/messages/'+private['id'])[0], 404)
        self.assertEqual(self.request('GET', '/api/root/messages/'+private['id'])[0], 200)
        self.assertEqual(self.view_field('/api/root/messages', '.messages[-1].id'), reply['id'])
        self.assertIsNotNone(self.call('message', id=private['id'])['message']['acked_at'])

    def test_project_feed_keeps_shared_root_batches_and_hides_retained_old_root_directs(self):
        operator = self.server.sender_session
        self.call('register', session='worker', agent='worker')
        shared = self.call('send', to=[operator, 'worker'], body='Shared project decision')
        self.assertEqual(self.view_field('/api/snapshot', '.messages | length'), '2')
        self.assertEqual(self.view_field('/api/snapshot', '.messages[0].deliveries | length'), '2')
        # Isolated historical fixture: routing removed, durable quiet identity remains.
        with contextlib.closing(Coordinator(self.db)) as coord:
            coord.db.execute('DELETE FROM root_projections WHERE session_id=?', ('a',))
        direct = self.call('send', to=operator, body='Retained root private history')
        self.assertEqual(self.view_field('/api/snapshot', '.messages | length'), '2')
        self.assertEqual(self.request('GET', '/api/messages/'+direct['id'])[0], 404)
        self.assertEqual(self.view_field('/api/messages', '.messages[0].body'), 'Shared project decision')

    def test_shared_reply_does_not_quote_private_root_history_in_project_view(self):
        operator = self.server.sender_session
        private = self.call('send', session=operator, to='a', body='Private operator context')
        shared = self.call('send', to=[operator], body='Explicitly shared reply', reply_to=private['id'])
        delivery = shared['deliveries'][0]['id']
        self.assertEqual(self.view_field('/api/messages/'+delivery, '.body'), 'Explicitly shared reply')
        self.assertEqual(self.view_field('/api/messages/'+delivery, '.reply_preview | tostring'), 'null')
        retained = self.call('message', session=operator, id=delivery)['message']
        self.assertEqual(retained['reply_to'], private['id'])
        self.assertEqual(retained['reply_preview']['body'], 'Private operator context')

    def test_service_notices_and_quoted_notices_have_truthful_attribution(self):
        self.project()
        notice = self.view_field('/api/root/messages',
            '.messages[] | select(.body | startswith("Project registered:")) | .id')
        self.assertEqual(self.view_field('/api/root/messages/'+notice, '.service_notice | tostring'), 'true')
        reply = self.call('send', to=self.server.sender_session, body='Setup completed',
            reply_to=notice, ack_reply=True)
        self.assertEqual(self.view_field('/api/root/messages/'+reply['id'],
            '.reply_preview.service_notice | tostring'), 'true')
        self.assertEqual(self.view_field('/api/root/messages/'+reply['id'],
            '.service_notice | tostring'), 'false')
        self.assertEqual(self.view_field('/api/root/messages/'+notice, '.sender_session'), self.server.sender_session)
        self.assertEqual(self.view_field('/api/snapshot', '.messages | length'), '0')
        ordinary = self.call('send', to=self.server.sender_session, body='Project registered: literal agent text')
        self.assertEqual(self.view_field('/api/root/messages/'+ordinary['id'], '.service_notice | tostring'), 'false')

    def test_interrupted_setup_retries_original_intent_without_new_payload(self):
        from agent_chat.session_reset import SessionResetState
        project = self.project(); request = str(uuid.uuid4())
        with mock.patch.object(SessionResetState, 'create', side_effect=OSError('response boundary fixture')):
            with self.assertRaises(RemoteCoordError): self.configure(project, request_id=request)
        row = next(p for p in self.root('status')['projects'] if p['project_id'] == project)
        self.assertEqual(row['setup'], 'awaiting_original_request_retry')
        first = self.root('retry-setup', project=project, request_id=request, confirm=True)
        second = self.root('retry-setup', project=project, request_id=request, confirm=True)
        self.assertEqual(first['creation']['target_session'], second['creation']['target_session'])
        self.assertFalse(self.rpc.started)

    def test_root_bridge_serializes_wakes_from_multiple_projects(self):
        projects = [self.project(), self.project('Project B')]
        for item in self.call('inbox')['messages']: self.call('acknowledge', id=item['id'])
        bridges=[]
        for project in projects:
            client=HttpClient(self.url,TOKEN,project=project); route=self.root('route',project=project)
            client.call('/api/coord',dict(op='register',session='reporter',host_id='mac',params={'agent':'reporter'}))
            client.call('/api/coord',dict(op='send',session='reporter',host_id='mac',params={'to':route['session'],'body':project+': decision required'}))
            bridges.append(self.project_bridge(project)[1])
        bridges[0].tick(); bridges[1].tick()
        self.assertEqual(len(self.rpc.history[self.thread_id]),1)
        self.assertIn(projects[0],self.rpc.history[self.thread_id][0]['content'][0]['text'])
        self.rpc.status[self.thread_id]='idle';bridges[1].tick()
        self.assertEqual(len(self.rpc.history[self.thread_id]),2)
        self.assertIn(projects[1],self.rpc.history[self.thread_id][1]['content'][0]['text'])
        self.assertTrue(all(self.routed(project,'inbox')['messages'][0]['acked_at'] is None for project in projects))

    def test_project_scoped_creation_refuses_noncanonical_host_alias_before_input(self):
        alias = Path(self.tmp.name).resolve()/'alias'; alias.symlink_to(self.workspace, target_is_directory=True)
        project=self.project();self.configure(project,cwd=str(alias))
        _,bridge=self.project_bridge(project);bridge.tick()
        job=self.routed(project,'session-reset-status',to='orchestrator')['resets'][0]
        self.assertEqual(job['status'],'pending')
        self.assertIn('canonical host path',job['error'])
        self.assertFalse(self.rpc.started)

    def test_global_routing_and_registration_survive_coordinator_restart(self):
        project=self.project();before=self.root('route',project=project)
        self.stop();self.start()
        self.assertEqual(self.root('route',project=project),before)
        self.assertEqual(len(self.call('inbox')['messages']),1)
        self.assertFalse(self.rpc.started)

    def test_status_distinguishes_missing_legacy_and_scoped_bridge(self):
        project=self.project();state,_=self.project_bridge(project)
        get=lambda:next(p['bridge'] for p in self.root('status')['projects'] if p['project_id']==project)
        self.assertFalse(get()['recent'])
        state.call('heartbeat',server='ws://127.0.0.1:4500',pid=42)
        self.assertTrue(get()['recent']);self.assertFalse(get()['project_scope_worker'])
        state.heartbeat('ws://127.0.0.1:4500',42)
        self.assertTrue(get()['project_scope_worker'])

    def test_read_only_root_cannot_create_writable_agents(self):
        project=self.project();self.configure(project)
        self.rpc.original['sandbox']={'type':'readOnly'}
        _,bridge=self.project_bridge(project);bridge.tick()
        job=self.routed(project,'session-reset-status',to='orchestrator')['resets'][0]
        self.assertEqual(job['status'],'pending')
        self.assertIn('does not authorize',job['error'])
        self.assertFalse(self.rpc.started)

    def test_active_target_measurement_refuses_setup_without_intent_or_thread(self):
        project=self.project()
        with mock.patch.object(self.server.measurements,'status',return_value={'active':{'id':'measurement-fixture'}}):
            with self.assertRaisesRegex(RemoteCoordError,'target project measurement'):
                self.configure(project)
        row=next(p for p in self.root('status')['projects'] if p['project_id']==project)
        self.assertIsNone(row['request_id']);self.assertFalse(self.rpc.started)

    def test_retained_identity_reenrollment_keeps_project_directory_and_main(self):
        project=self.project();self.configure(project)
        _,bridge=self.project_bridge(project);bridge.tick()
        before=next(p for p in self.root('status')['projects'] if p['project_id']==project)
        for item in self.call('inbox')['messages']:self.call('acknowledge',id=item['id'])
        self.routed(project,'context');self.root('detach',confirm=True)
        # Simulate the ordinary, already-tested home reset binding boundary.
        new=str(uuid.uuid4());self.call('bind',thread=new)
        self.rpc.status[new],self.rpc.queues[new],self.rpc.history[new]='idle',[],[]
        self.call('agent-status',refresh=True,expected_thread=new,request_id=str(uuid.uuid4()))
        self.bridge.tick()
        self.root('enroll',expected_thread=new,confirm=True)
        after=next(p for p in self.root('status')['projects'] if p['project_id']==project)
        self.assertEqual(after['session_id'],before['session_id'])
        self.assertEqual(after['orchestrator_session'],before['orchestrator_session'])
        self.assertEqual(after['cwd'],before['cwd'])
        self.assertEqual(after['setup'],'completed')
        self.assertEqual(len(self.rpc.started),1)

    def test_root_queue_and_existing_holds_block_enrollment_without_transfers(self):
        self.root('detach',confirm=True)
        claim=self.call('request',resource='held',minutes=5)
        with self.assertRaisesRegex(RemoteCoordError,'held reservation'):
            self.root('enroll',expected_thread=self.thread_id,confirm=True)
        self.assertEqual(self.call('check',resource='held',token=claim['token'])['reservation_id'],claim['reservation_id'])

    def test_prepared_root_wake_survives_refused_detach_without_rpc_admission(self):
        self.call('register',session='reporter',agent='reporter')
        self.call('send',session='reporter',to='root',body='Decision needed')
        # Another input can make the thread busy between the scheduler's first
        # idle read and its later admission check, leaving a prepared wake.
        request=self.rpc.request
        def become_busy(method,params):
            result=request(method,params)
            if method=='thread/read' and params['threadId']==self.thread_id:
                self.rpc.status[self.thread_id]='active'
            return result
        with mock.patch.object(self.rpc,'request',side_effect=become_busy):self.bridge.tick()
        before=self.state.jobs()[0]
        self.call('context')
        with self.assertRaises(RemoteCoordError):self.root('detach',confirm=True)
        self.bridge.tick()
        after=self.state.job(before['id'])
        self.assertEqual(after['status'],'prepared')
        self.assertEqual(self.rpc.queues[self.thread_id],[])
        self.root('resume-routing',confirm=True)
        self.rpc.status[self.thread_id]='idle';self.bridge.tick()
        self.assertEqual(self.state.job(before['id'])['status'],'dispatched')

    def test_project_measurement_excludes_global_root_and_keeps_project_main(self):
        self.assertEqual(self.server.measurements._bindings(self.db),[])
        project=self.project();self.configure(project)
        _,bridge=self.project_bridge(project);bridge.tick()
        bindings=self.server.measurements._bindings(self.server.projects.db_path(project))
        self.assertEqual(len(bindings),1)
        self.assertEqual(bindings[0][0],'orchestrator')
        self.assertNotEqual(bindings[0][2],self.thread_id)
