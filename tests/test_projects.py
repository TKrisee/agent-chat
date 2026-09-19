import base64
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
