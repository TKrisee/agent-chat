import base64
import contextlib
import sqlite3
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import uuid
from unittest import mock

from agent_chat.core import Coordinator, CoordError, ValidationGuard
from agent_chat.projects import Projects, resolve_database
from agent_chat.bridge import Bridge
from agent_chat.bridge_client import RemoteBridgeState
from agent_chat.remote import HttpClient, RemoteCoordError
from test_remote_web import RemoteWebFixture, TOKEN
from test_bridge import FakeRpc


class ProjectStoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='agent-chat-projects-')
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name).resolve() / 'state.sqlite3'
        Coordinator(self.base).close()
        self.projects = Projects(self.base)

    def test_legacy_default_rename_stable_id_and_strict_selection(self):
        old = Coordinator(self.base, 'old'); old.register('legacy'); old.close()
        self.projects.rename('default', 'Example Project')
        self.assertEqual(Projects(self.base).list()[0]['name'], 'Example Project')
        self.assertEqual(self.projects.db_path(), self.base)
        with self.assertRaises(CoordError): self.projects.db_path('../bad')
        for bad in ('', 'x\nname', 'x'*81, None):
            with self.assertRaises(CoordError): self.projects.create(bad)
        with self.assertRaises(CoordError): self.projects.create(' EXAMPLE PROJECT ')
        c = Coordinator(self.base, 'old'); self.addCleanup(c.close)
        self.assertEqual(c.require_session(), 'old')

    def test_creation_scope_survives_reopen_without_loading_briefs_in_directory(self):
        brief = 'Build a planner.\nKeep literal $(shell-looking data) and 日本語.\n'
        item = self.projects.create('Scoped project', '/projects/planner', brief)
        reopened = Projects(self.base)
        self.assertEqual(reopened.get(item['id'])['instructions'], brief)
        self.assertEqual(reopened.get(item['id'])['workspace'], '/projects/planner')
        self.assertNotIn('instructions', reopened.list()[-1])
        self.assertIsNone(reopened.get('default')['instructions'])

    def test_invalid_creation_scope_creates_no_project(self):
        before = self.projects.list()
        for options in ({'workspace':'relative/path'}, {'workspace':'/bad\x00path'},
                        {'instructions':42}, {'instructions':'語' * 20000}):
            with self.assertRaises(CoordError):
                self.projects.create('Invalid', **options)
        self.assertEqual(self.projects.list(), before)

    def test_same_labels_and_resources_are_isolated_and_tokens_cannot_cross(self):
        item = self.projects.create('Second')
        path = self.projects.db_path(item['id'])
        a = Coordinator(self.base, 'worker'); b = Coordinator(path, 'worker')
        self.addCleanup(a.close); self.addCleanup(b.close)
        for c in (a,b): c.register('worker'); c.inbox()
        first = a.request('file:src/common.py', 5)
        second = b.request('file:src/common.py', 5)
        self.assertEqual(first['state'], 'owned'); self.assertEqual(second['state'], 'owned')
        with self.assertRaises(CoordError): b.check('file:src/common.py', first['token'])
        self.assertEqual(resolve_database(path, item['id']), path)
        with self.assertRaises(CoordError): resolve_database(path, 'default')
        self.projects.rename(item['id'], 'Renamed')
        self.assertEqual(self.projects.db_path(item['id']), path)
        with mock.patch.dict(os.environ, {'AGENT_CHAT_DB':str(self.base), 'AGENT_CHAT_PROJECT':item['id'], 'AGENT_CHAT_SESSION':'worker','AGENT_CHAT_TOKEN':second['token']}):
            with ValidationGuard('file:src/common.py') as guard:
                self.assertEqual(guard.coord.path, path)
                guard.pulse()

    def test_missing_project_db_is_never_recreated_implicitly(self):
        item = self.projects.create('Missing')
        path = self.projects.db_path(item['id']); path.unlink()
        with self.assertRaisesRegex(CoordError, 'missing'): resolve_database(self.base, item['id'])
        self.assertFalse(path.exists())



class ProjectWorkspaceStoreTests(unittest.TestCase):
    """Service-free saved configuration contracts in disposable SQLite stores."""
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='agent-chat-workspace-registry-')
        self.addCleanup(self.tmp.cleanup)
        self.home = Path(self.tmp.name).resolve()
        self.base = self.home / 'state.sqlite3'
        Coordinator(self.base).close()
        self.projects = Projects(self.base)

    def member(self, name):
        path = self.home / 'synthetic-Herd' / name
        path.mkdir(parents=True, exist_ok=True)
        return str(path)

    def legacy_pair(self):
        left = self.projects.create('INLST', instructions='Preserve INLST instructions')
        right = self.projects.create('LMX', instructions='Preserve LMX instructions')
        parent = str(self.home / 'synthetic-Herd')
        with self.projects.connection() as db:
            db.execute('UPDATE projects SET workspace=? WHERE id IN (?,?)', (parent, left['id'], right['id']))
        first = [self.member(name) for name in ('inlst-core', 'inlst-api', 'inlst-client', 'inlst-web')]
        second = [self.member(name) for name in ('lmx-account-registrar', 'lmx-api', 'lmx-web')]
        return left, right, first, second

    def assignment(self, project, roots, *, pending=False, registered=False):
        from agent_chat.root_orchestrator import RootOrchestrator
        RootOrchestrator(self.projects)
        target = 'synthetic-po-' + project
        request = 'synthetic-request-' + project
        with self.projects.connection() as db:
            db.execute('INSERT INTO root_projects(project_id,session_id,cwd,agent,request_id,orchestrator_session) VALUES(?,?,?,?,?,?)',
                       (project, 'synthetic-root-' + project, roots[0], 'synthetic-po', request, target))
            db.execute('CREATE TABLE IF NOT EXISTS root_project_workspaces(project_id TEXT,position INTEGER,path TEXT)')
            db.executemany('INSERT INTO root_project_workspaces VALUES(?,?,?)', [(project, index, root) for index, root in enumerate(roots)])
        with contextlib.closing(sqlite3.connect(self.projects.db_path(project))) as state:
            if registered:
                state.execute('INSERT INTO sessions(id,agent,registered_at) VALUES(?,?,?)', (target, 'synthetic-po', 1))
            state.execute('CREATE TABLE IF NOT EXISTS session_resets(id TEXT PRIMARY KEY,status TEXT)')
            state.execute('INSERT INTO session_resets VALUES(?,?)', (request, 'pending' if pending else 'completed'))
            state.commit()
        return target, request

    def test_ordered_members_primary_alias_reopen_and_legacy_fallback(self):
        paths = [self.member(name) for name in ('inlst-core', 'inlst-api', 'inlst-client', 'inlst-web')]
        item = self.projects.create('INLST', instructions='literal assignment', workspaces=paths)
        reopened = Projects(self.base)
        self.assertEqual(reopened.get(item['id'])['workspaces'], paths)
        self.assertEqual(reopened.get(item['id'])['workspace'], paths[0])
        self.assertEqual(reopened.get(item['id'])['instructions'], 'literal assignment')
        self.assertNotIn('instructions', reopened.list()[-1])
        self.assertEqual(reopened.list()[-1]['workspaces'], paths)
        legacy = self.projects.create('Legacy', self.member('legacy'), 'keep')
        with self.projects.connection() as db:
            db.execute('DELETE FROM project_workspaces WHERE project_id=?', (legacy['id'],))
        self.assertEqual(Projects(self.base).get(legacy['id'])['workspaces'], [legacy['workspace']])
        self.assertEqual(reopened.get('default')['workspaces'], [])

    def test_legacy_parent_transition_is_atomic_and_primary_is_first_member(self):
        left, right, first, second = self.legacy_pair()
        before = [self.projects.get(item['id']) for item in (left, right)]
        with self.assertRaisesRegex(CoordError, 'overlaps'):
            self.projects.configure_workspaces(left['id'], first)
        self.assertEqual([self.projects.get(item['id']) for item in (left, right)], before)
        self.projects.configure_workspaces_batch([
            {'id': left['id'], 'workspaces': first}, {'id': right['id'], 'workspaces': second}])
        for item, paths in ((left, first), (right, second)):
            changed = Projects(self.base).get(item['id'])
            self.assertEqual(changed['workspace'], paths[0])
            self.assertEqual(changed['workspaces'], paths)
            self.assertEqual((changed['name'], changed['instructions']), (item['name'], item['instructions']))
            self.assertNotIn(str(self.home / 'synthetic-Herd'), changed['workspaces'])

    def test_invalid_members_and_unknown_or_duplicate_projects_preserve_saved_setup(self):
        item = self.projects.create('Original', self.member('original'), 'keep instructions')
        before = self.projects.get(item['id'])
        parent = self.member('other')
        invalid = [[], 'not-a-list', ['relative'], [parent + '/..'], [parent + '//child'],
                   [parent + '/'], [parent + '\x00'], [parent + '\n'], [parent + '\x7f'], [parent, parent],
                   [parent, parent + '/child'], [None], [parent] * 33]
        for members in invalid:
            with self.subTest(members=members), self.assertRaises(CoordError):
                self.projects.configure_workspaces(item['id'], members)
            self.assertEqual(self.projects.get(item['id']), before)
        for batch in ([{'id': 'missing', 'workspaces': [parent]}],
                      [{'id': item['id'], 'workspaces': [parent]}] * 2,
                      [{'id': item['id'], 'workspaces': [parent], 'instructions': 'replace'}]):
            with self.assertRaises(CoordError): self.projects.configure_workspaces_batch(batch)
            self.assertEqual(self.projects.get(item['id']), before)

    def test_cross_project_all_member_overlap_and_failed_create_leave_no_artifacts(self):
        first = self.projects.create('First', workspaces=[self.member('first-main'), self.member('first-extra')])
        second = self.projects.create('Second', self.member('second'), 'preserve')
        before = self.projects.get(second['id'])
        with self.assertRaisesRegex(CoordError, 'overlaps'):
            self.projects.configure_workspaces(second['id'], [self.member('new-primary'), first['workspaces'][1] + '/nested'])
        self.assertEqual(self.projects.get(second['id']), before)
        count = len(self.projects.list())
        directory = self.base.parent / (self.base.name + '.projects')
        directories = set(directory.iterdir())
        with self.assertRaisesRegex(CoordError, 'overlaps'):
            self.projects.create('Conflict', workspaces=[first['workspaces'][1]])
        self.assertEqual(len(self.projects.list()), count)
        self.assertEqual(set(directory.iterdir()), directories)

    def test_neutral_root_and_registered_or_pending_assignments_block_updates(self):
        from agent_chat.root_orchestrator import RootOrchestrator
        RootOrchestrator(self.projects)
        item = self.projects.create('Target', self.member('target'), 'preserve')
        neutral = self.member('neutral')
        with self.projects.connection() as db:
            db.execute('INSERT INTO root_config(singleton,home_project,home_session,thread_id,host_id,cwd,agent,created_at) VALUES(1,?,?,?,?,?,?,?)',
                       ('default', 'root', 'synthetic-thread', 'synthetic-host', neutral, 'root', 1))
        before = self.projects.get(item['id'])
        with self.assertRaisesRegex(CoordError, 'neutral root'):
            self.projects.configure_workspaces(item['id'], [neutral + '/nested'])
        self.assertEqual(self.projects.get(item['id']), before)
        # Remove only synthetic enrollment metadata; no server or root sync runs.
        with self.projects.connection() as db: db.execute('DELETE FROM root_config')
        owner = self.projects.create('Assigned', self.member('assigned-saved'))
        active = [self.member('assigned-current'), self.member('assigned-secondary')]
        self.assignment(owner['id'], active, registered=True)
        with self.assertRaisesRegex(CoordError, 'active or pending'):
            self.projects.configure_workspaces(item['id'], [active[1] + '/nested'])
        self.assertEqual(self.projects.get(item['id']), before)
        pending_owner = self.projects.create('Pending', self.member('pending-saved'))
        pending = [self.member('pending-current')]
        self.assignment(pending_owner['id'], pending, pending=True)
        with self.assertRaisesRegex(CoordError, 'active or pending'):
            self.projects.configure_workspaces(item['id'], [pending[0] + '/nested'])
        self.assertEqual(self.projects.get(item['id']), before)

    def test_legacy_parent_active_assignment_requires_proven_retirement(self):
        left, right, first, second = self.legacy_pair()
        parent = str(self.home / 'synthetic-Herd')
        target, request = self.assignment(right['id'], [parent], registered=True)
        before = [self.projects.get(item['id']) for item in (left, right)]
        batch = [{'id': left['id'], 'workspaces': first}, {'id': right['id'], 'workspaces': second}]
        with self.assertRaisesRegex(CoordError, 'active or pending'):
            self.projects.configure_workspaces_batch(batch)
        self.assertEqual([self.projects.get(item['id']) for item in (left, right)], before)
        with contextlib.closing(sqlite3.connect(self.projects.db_path(right['id']))) as state:
            state.execute('DELETE FROM sessions WHERE id=?', (target,))
            state.execute("UPDATE session_resets SET status='pending' WHERE id=?", (request,))
            state.commit()
        with self.assertRaisesRegex(CoordError, 'active or pending'):
            self.projects.configure_workspaces_batch(batch)
        with contextlib.closing(sqlite3.connect(self.projects.db_path(right['id']))) as state:
            state.execute("UPDATE session_resets SET status='completed' WHERE id=?", (request,))
            state.commit()
        self.projects.configure_workspaces_batch(batch)
        self.assertEqual(self.projects.get(left['id'])['workspaces'], first)
        self.assertEqual(self.projects.get(right['id'])['workspaces'], second)


class ProjectHttpTests(RemoteWebFixture):
    def setUp(self):
        super().setUp()
        self.project = self.client.call('/api/projects/rpc', {'op':'create', 'name':'Other'})['project']['id']
        self.other = HttpClient(self.url, TOKEN, project=self.project)

    def rpc(self, op, session='a', **params):
        return self.other.call('/api/coord', {'op':op, 'session':session, 'host_id':'mac', 'params':params})

    def test_ui_config_messages_images_replies_and_removal_are_scoped(self):
        self.call('register', agent='default-agent')
        self.rpc('register', agent='other-agent'); self.rpc('register', session='b', agent='recipient')
        original = self.rpc('send', to='b', body='Private to project')
        image = b'\x89PNG\r\n\x1a\nfixture'
        reply = self.rpc('send', session='b', to='a', body='Answer', reply_to=original['id'], attachments=[{'name':'test.png','content_base64':base64.b64encode(image).decode()}])
        self.assertIn('project='+self.project, reply['attachments'][0]['url'])
        self.assertEqual(self.request('GET', reply['attachments'][0]['url'])[1], image)
        self.assertEqual(self.request('GET', reply['attachments'][0]['url'].split('?')[0])[0], 404)
        config = json.loads(self.request('GET', '/api/config?project='+self.project)[1])
        self.assertNotEqual(config['sender']['id'], self.server.sender_session)
        snapshot = json.loads(self.request('GET', '/api/snapshot?project='+self.project)[1])
        self.assertEqual(snapshot['total_messages'], 2)
        self.assertNotIn('default-agent', [s['agent'] for s in snapshot['sessions']])
        self.assertEqual(json.loads(self.request('GET', '/api/snapshot')[1])['total_messages'], 0)
        self.assertEqual(self.request('GET', '/api/messages/'+reply['id'])[0], 404)
        self.rpc('deregister', session='b')
        self.assertEqual(len(self.call('status')['sessions']), 2)  # default operator + agent
        snapshot = json.loads(self.request('GET', '/api/snapshot?project='+self.project)[1])
        self.assertEqual(snapshot['messages'][1]['sender_agent'], 'recipient')

    def test_invalid_scope_never_falls_back_and_creation_requires_csrf(self):
        for path, headers in [('/api/snapshot?project=unknown',{}),('/api/snapshot?project=default&project=default',{}),('/api/snapshot?project=default',{'X-Agent-Chat-Project':self.project})]:
            self.assertEqual(self.request('GET', path, headers=headers)[0], 400)
        self.assertEqual(self.request('POST', '/api/projects', {'name':'No CSRF'})[0], 403)
        cfg = json.loads(self.request('GET','/api/config')[1])
        headers = {'X-Agent-Chat-CSRF':cfg['csrf_token']}
        self.assertEqual(self.request('POST','/api/projects/rename',{'id':self.project,'name':'New name'},headers=headers)[0], 200)
        self.assertEqual(self.request('POST','/api/projects',{'name':'New name'},headers=headers)[0], 400)
        self.assertEqual(self.request('POST','/api/projects/rpc',{'op':'list'},auth='basic')[0], 401)

    def test_two_project_bridge_leases_and_wakes_remain_separate(self):
        self.call('register', agent='default-agent'); self.rpc('register', agent='other-agent')
        thread = str(uuid.uuid4())
        self.call('bind', thread=thread); self.rpc('bind', thread=thread)
        default_state = RemoteBridgeState(self.client,self.url,{'owner':'one','secret':'s'*40,'host_id':'mac'})
        other_state = RemoteBridgeState(self.other,self.url,{'owner':'two','secret':'t'*40,'host_id':'mac'})
        default_state.call('acquire'); other_state.call('acquire')
        path, sender, _ = self.server.project_context(self.project)
        c = Coordinator(path,sender)
        try: message = c.send('a','Please check this project')['id']
        finally: c.close()
        rpc = FakeRpc(); Bridge(other_state,rpc).tick()
        payload = next(p for m,p in rpc.calls if m == 'thread/queue/add')['input'][0]['text']
        metadata = json.loads(payload.split('\n')[-1])
        self.assertEqual(metadata['project'], self.project)
        self.assertIn(message,payload)
        self.assertEqual(default_state.jobs(), [])
        self.assertEqual(self.call('inbox')['messages'], [])
        self.assertIsNone(self.rpc('inbox')['messages'][0]['acked_at'])
        default_state.call('release'); other_state.call('release')

    def test_removed_remote_identity_cannot_be_resurrected_or_repinned(self):
        self.rpc('register',agent='finished'); self.rpc('deregister')
        with self.assertRaises(RemoteCoordError): self.rpc('register',agent='finished')
        path, _, _ = self.server.project_context(self.project)
        c=Coordinator(path)
        try:
            self.assertIsNone(c.db.execute("SELECT 1 FROM sessions WHERE id='a'").fetchone())
            self.assertEqual(c.db.execute("SELECT agent FROM retired_sessions WHERE id='a'").fetchone()[0], 'finished')
        finally: c.close()


class WorkspaceCliTests(unittest.TestCase):
    def test_repeatable_grouped_workspace_arguments_preserve_project_and_member_order(self):
        import argparse
        from agent_chat import cli
        client = mock.Mock()
        client.call.return_value = {'projects':[]}
        args = argparse.Namespace(session=None,project='default',api_token=None)
        with mock.patch('agent_chat.remote.HttpClient',return_value=client), mock.patch('builtins.print'):
            cli._project_main(['configure-workspaces-batch','--project-workspace','INLST=/synthetic/inlst-core',
                '--project-workspace','LMX=/synthetic/lmx-account-registrar','--project-workspace','INLST=/synthetic/inlst-api'],
                args,'http://synthetic',mock.Mock())
        self.assertEqual(client.call.call_args.args[1], {'op':'configure-workspaces-batch','assignments':[
            {'id':'INLST','workspaces':['/synthetic/inlst-core','/synthetic/inlst-api']},
            {'id':'LMX','workspaces':['/synthetic/lmx-account-registrar']}]})

