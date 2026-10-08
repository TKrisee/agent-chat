"""Operator aggregate history leaves message/read/ACK/native journals untouched."""
import contextlib
import sqlite3
from agent_chat.core import Coordinator
from test_remote_web import RemoteWebFixture


class CommunicationViewTests(RemoteWebFixture):
    def setUp(self):
        super().setUp()
        self.second=self.client.call('/api/projects/rpc',dict(op='create',name='Second project'))['project']['id']
        self.paths=[self.db,self.server.projects.db_path(self.second)]
        self.ids=[]
        for n,path in enumerate(self.paths):
            with contextlib.closing(Coordinator(path,'alpha'+str(n))) as coord:
                coord.register('Alpha');coord.session='beta'+str(n);coord.register('Beta')
                coord.session='alpha'+str(n)
                first=coord.send('beta'+str(n),'needle original '+str(n));self.ids.append(first['id'])
                coord.session='beta'+str(n);coord.inbox();reply=coord.send('alpha'+str(n),'reply '+str(n),reply_to=first['id'],ack_reply=True);self.ids.append(reply['id'])
                long=coord.send('alpha'+str(n),'Long history '+('x'*4100));self.ids.append(long['id'])
                coord.db.execute('INSERT INTO retired_sessions SELECT id,agent,registered_at,? FROM sessions WHERE id=?',(1,'beta'+str(n)))
                coord.db.execute('DELETE FROM sessions WHERE id=?',('beta'+str(n),))
        self.before=self.snapshot()
        self.contexts_before=set(self.server.project_contexts)

    def snapshot(self):
        result=[]
        for path in self.paths:
            with contextlib.closing(sqlite3.connect(path)) as db:
                result.append((db.execute('SELECT id,acked_at FROM messages ORDER BY seq').fetchall(),
                    db.execute('SELECT * FROM message_reads ORDER BY message_id,reader_session').fetchall(),
                    db.execute('SELECT id,inbox_read_seq FROM sessions ORDER BY id').fetchall(),
                    db.execute('SELECT id,status FROM bridge_jobs ORDER BY id').fetchall()))
        return result

    def test_all_project_paging_reply_full_body_and_filters_do_not_mark_read(self):
        code,raw,_=self.request('GET','/api/communications?limit=2');self.assertEqual(code,200,raw)
        import subprocess
        subprocess.run(['jq','-e','.read_only and .has_more and (.messages|length)==2 and .loaded_projects==2'],input=raw,capture_output=True,check=True)
        cursor=subprocess.check_output(['jq','-r','.next_cursor'],input=raw).decode().strip()
        pages=[raw]
        while cursor!='null':
            code,raw,_=self.request('GET','/api/communications?limit=2&before='+cursor);self.assertEqual(code,200,raw)
            pages.append(raw);cursor=subprocess.check_output(['jq','-r','.next_cursor'],input=raw).decode().strip()
        joined=b'\n'.join(pages)
        subprocess.run(['jq','-se','[.[].messages[]] as $m | ($m|length)==6 and ($m|map(.project_id)|unique|length)==2 and ($m|map(.id)|unique|length)==6 and any($m[];.sender_agent=="Beta") and any($m[];.reply_to!=null)'],input=joined,capture_output=True,check=True)
        code,raw,_=self.request('GET','/api/communications?q=needle&agent=alpha0&project=default');self.assertEqual(code,200,raw)
        subprocess.run(['jq','-e','(.messages|length)==1 and .messages[0].body=="needle original 0"'],input=raw,capture_output=True,check=True)
        code,raw,_=self.request('GET','/api/communications/message?project='+self.second+'&id='+self.ids[-1]);self.assertEqual(code,200,raw)
        subprocess.run(['jq','-e','(.message.body|length)>4000 and .message.body_truncated==false'],input=raw,capture_output=True,check=True)
        self.assertEqual(self.snapshot(),self.before)
        self.assertEqual(set(self.server.project_contexts),self.contexts_before)

    def test_auth_invalid_scope_empty_and_missing_project_are_explicit(self):
        import subprocess
        code,_,_=self.request('GET','/api/communications',auth=None);self.assertEqual(code,401)
        code,_,_=self.request('GET','/api/communications?project=not-registered');self.assertEqual(code,400)
        code,_,_=self.request('GET','/api/communications?limit=101');self.assertEqual(code,400)
        code,_,_=self.request('GET','/api/communications?before=bad');self.assertEqual(code,400)
        code,raw,_=self.request('GET','/api/communications?q=no-such-body');self.assertEqual(code,200)
        subprocess.run(['jq','-e','(.messages|length)==0 and .partial==false'],input=raw,capture_output=True,check=True)
        self.paths[1].rename(self.paths[1].with_suffix('.unavailable'))
        code,raw,_=self.request('GET','/api/communications');self.assertEqual(code,200,raw)
        subprocess.run(['jq','-e','.partial and .loaded_projects==1 and (.errors|length)==1 and (.messages|length)==3'],input=raw,capture_output=True,check=True)
        self.paths[1].with_suffix('.unavailable').rename(self.paths[1])
        self.assertEqual(self.snapshot(),self.before)

    def test_menu_and_readonly_page_are_served_without_compose(self):
        code,raw,_=self.request('GET','/');self.assertEqual(code,200)
        html=raw.decode();self.assertLess(html.index('id="root-conversation"'),html.index('id="all-communication-view"'))
        self.assertLess(html.index('id="all-communication-view"'),html.index('id="all-conversations"'))
        code,raw,_=self.request('GET','/communications');self.assertEqual(code,200)
        self.assertNotIn('message-input',raw.decode());self.assertIn('href="/root"',raw.decode())
        self.assertEqual(self.snapshot(),self.before)

    def test_legacy_handler_does_not_publish_new_menu_before_backend_adoption(self):
        import importlib.util
        import os
        import pathlib
        import unittest
        source=os.environ.get('AGENT_CHAT_LEGACY_WEB_SOURCE')
        if not source:
            raise unittest.SkipTest('mixed-version fixture requires exact pre-integration handler source')
        index=self.server.web_root/'index.html'
        self.assertIn('<!-- agent-chat:all-communication-menu -->',index.read_text())
        self.assertNotIn('href="/communications"',index.read_text())
        spec=importlib.util.spec_from_file_location('agent_chat._legacy_communication_web',source)
        legacy=importlib.util.module_from_spec(spec);spec.loader.exec_module(legacy)
        self.stop()
        import threading
        from test_remote_web import TOKEN
        self.server=legacy.create_server(self.db,port=0,api_token=TOKEN,web_root=index.parent)
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start()
        self.url='http://127.0.0.1:'+str(self.server.server_port)
        code,raw,_=self.request('GET','/');self.assertEqual(code,200)
        self.assertNotIn('id="all-communication-view"',raw.decode())
        code,_,_=self.request('GET','/communications');self.assertEqual(code,404)
        self.assertEqual(self.snapshot(),self.before)
