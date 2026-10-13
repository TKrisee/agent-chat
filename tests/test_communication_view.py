"""Operator aggregate history leaves message/read/ACK/native journals untouched."""
import contextlib
import sqlite3
import subprocess
from agent_chat.core import Coordinator
from test_remote_web import RemoteWebFixture


class OperatorMessageUITests(RemoteWebFixture):
    def setUp(self):
        super().setUp()
        self.operator = self.server.sender_session
        with contextlib.closing(Coordinator(self.db, 'neutral-alpha')) as coord:
            coord.register('Alpha'); coord.inbox()
            self.incoming = coord.send(self.operator, 'neutral received with file')
            self.second = coord.send(self.operator, 'neutral second received')
            coord.session = 'neutral-beta'; coord.register('Beta'); coord.inbox()
            self.foreign = coord.send('neutral-alpha', 'another recipient unread')
            coord.session = self.operator
            self.outgoing = coord.send('neutral-alpha', 'outgoing operator message')
        code, raw, _ = self.request('GET', '/api/config', auth='basic')
        self.assertEqual(code, 200)
        self.csrf = subprocess.check_output(['jq', '-r', '.csrf_token'], input=raw).decode().strip()

    def _jq(self, raw, predicate):
        result = subprocess.run(['jq', '-e', predicate], input=raw, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr or result.stdout)

    def _protected(self):
        with contextlib.closing(sqlite3.connect(self.db)) as db:
            return (db.execute('SELECT * FROM message_reads ORDER BY message_id,reader_session').fetchall(),
                    db.execute('SELECT id,inbox_read_seq FROM sessions ORDER BY id').fetchall(),
                    db.execute('SELECT * FROM resources ORDER BY name').fetchall(),
                    db.execute('SELECT * FROM guarded_runs ORDER BY run_id').fetchall(),
                    db.execute('SELECT * FROM resource_queue ORDER BY seq').fetchall())

    def _ack(self, message_id, **options):
        return self.request('POST', '/api/messages/ack', {'id': message_id}, auth=options.pop('auth', 'basic'),
                            headers=options.pop('headers', {'X-Agent-Chat-CSRF': self.csrf}), **options)

    def test_incoming_ack_repeat_is_only_acknowledgement(self):
        before = self._protected()
        for _ in range(2):
            code, raw, _ = self._ack(self.incoming['id'])
            self.assertEqual(code, 200, raw)
            self._jq(raw, '.acknowledged == true')
        self.assertEqual(self._protected(), before)
        with contextlib.closing(sqlite3.connect(self.db)) as db:
            self.assertIsNotNone(db.execute('SELECT acked_at FROM messages WHERE id=?', (self.incoming['id'],)).fetchone()[0])
            self.assertIsNone(db.execute('SELECT acked_at FROM messages WHERE id=?', (self.foreign['id'],)).fetchone()[0])

    def test_operator_channel_csrf_foreign_outgoing_and_missing_refuse(self):
        before = self._protected()
        self.assertEqual(self._ack(self.incoming['id'], auth='bearer')[0], 403)
        self.assertEqual(self._ack(self.incoming['id'], headers={})[0], 403)
        self.assertEqual(self._ack(self.incoming['id'], auth=None)[0], 401)
        for message_id in (self.foreign['id'], self.outgoing['id'], 'message_missing'):
            self.assertEqual(self._ack(message_id)[0], 404)
        code, _, _ = self.request('POST', '/api/messages/ack', {'id': self.incoming['id'], 'recipient': self.operator},
                                  auth='basic', headers={'X-Agent-Chat-CSRF': self.csrf})
        self.assertEqual(code, 400)
        self.assertEqual(self._protected(), before)
        with contextlib.closing(sqlite3.connect(self.db)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM messages WHERE acked_at IS NOT NULL').fetchone()[0], 0)

    def test_finite_incoming_plan_exact_ids_and_new_arrivals(self):
        before = self._protected()
        code, raw, _ = self.request('GET', '/api/messages/ack-plan', auth='basic')
        self.assertEqual(code, 200, raw)
        self._jq(raw, '.count == 2 and .effect == "ACK_ONLY" and .recipient_session != null')
        ids = subprocess.check_output(['jq', '-r', '.ids[]'], input=raw).decode().splitlines()
        self.assertEqual(set(ids), {self.incoming['id'], self.second['id']})
        self.assertEqual(self._protected(), before)
        with contextlib.closing(Coordinator(self.db, 'neutral-alpha')) as coord:
            coord.inbox(); late = coord.send(self.operator, 'late message not selected')
        for message_id in ids:
            self.assertEqual(self._ack(message_id)[0], 200)
        with contextlib.closing(sqlite3.connect(self.db)) as db:
            self.assertIsNone(db.execute('SELECT acked_at FROM messages WHERE id=?', (late['id'],)).fetchone()[0])
        code, raw, _ = self.request('GET', '/api/messages/ack-plan', auth='basic')
        self.assertEqual(code, 200)
        self._jq(raw, '.count == 1')

    def test_aggregate_stamps_own_delivery_and_scoped_attachments(self):
        with contextlib.closing(Coordinator(self.db, 'neutral-alpha')) as coord:
            coord.db.execute('INSERT INTO attachments(id,message_id,name,mime,size,content) VALUES(?,?,?,?,?,?)',
                             ('attachment_neutral', self.incoming['id'], 'neutral.txt', 'text/plain', 5, b'hello'))
        code, raw, _ = self.request('GET', '/api/communications?project=default', auth='basic')
        self.assertEqual(code, 200)
        self._jq(raw, 'any(.messages[]; .body == "neutral received with file" and .operator_ack.id == .id and .attachments[0].url == "/api/gallery-attachments/attachment_neutral?project=default") and all(.messages[] | select(.body == "another recipient unread"); .operator_ack == null)')
        self.assertEqual(self.request('GET', '/api/gallery-attachments/attachment_neutral?project=default', auth='basic')[1], b'hello')

    def test_project_scope_and_origin_cannot_borrow_recipient(self):
        self.assertEqual(self.request('POST', '/api/messages/ack?project=unknown', {'id': self.incoming['id']}, auth='basic',
            headers={'X-Agent-Chat-CSRF': self.csrf})[0], 400)
        self.assertEqual(self._ack(self.incoming['id'], headers={'X-Agent-Chat-CSRF': self.csrf, 'Origin': 'https://foreign.invalid'})[0], 403)

    def test_group_copy_is_ackable_only_as_exact_operator_delivery(self):
        with contextlib.closing(Coordinator(self.db,'neutral-alpha')) as coord:
            coord.inbox(); coord.send_many([self.operator,'neutral-beta'],'neutral group copy')
        code,raw,_=self.request('GET','/api/communications?project=default&q=neutral+group+copy',auth='basic')
        self.assertEqual(code,200)
        self._jq(raw,'([.messages[].operator_ack.id | select(. != null)] | unique | length)==1 and all(.messages[] | select(.operator_ack != null); .operator_ack.recipient_session != null)')
        exact=subprocess.check_output(['jq','-r','[.messages[] | select(.operator_ack != null)][0].operator_ack.id'],input=raw).decode().strip()
        self.assertEqual(self._ack(exact)[0],200)
        with contextlib.closing(sqlite3.connect(self.db)) as db:
            rows=db.execute('SELECT recipient_session,acked_at FROM messages WHERE body=?',('neutral group copy',)).fetchall()
            self.assertEqual(sum(value is not None for _,value in rows),1)
            self.assertTrue(all(value is None for recipient,value in rows if recipient!=self.operator))

    def test_ack_plan_does_not_enroll_an_uninitialized_operator_context(self):
        item=self.server.projects.create('Neutral unopened context')
        before=set(self.server.project_contexts)
        code,_,_=self.request('GET','/api/messages/ack-plan?project='+item['id'],auth='basic')
        self.assertEqual(code,400)
        self.assertEqual(set(self.server.project_contexts),before)



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

    def test_bidirectional_cursor_order_with_tied_times_and_project_filters(self):
        for path in self.paths:
            with contextlib.closing(sqlite3.connect(path)) as db:
                db.execute('UPDATE messages SET created_at=42')
        pages=[]
        code,raw,_=self.request('GET','/api/communications?limit=2')
        self.assertEqual(code,200,raw)
        latest=raw
        while True:
            pages.append(raw)
            token=subprocess.check_output(['jq','-r','.next_cursor // empty'],input=raw).decode().strip()
            if not token:break
            code,raw,_=self.request('GET','/api/communications?limit=2&before='+token)
            self.assertEqual(code,200,raw)
        self.assertEqual(len(pages),3)
        first=pages[-1]
        token=subprocess.check_output(['jq','-r','.newest_cursor'],input=first).decode().strip()
        code,middle,_=self.request('GET','/api/communications?limit=2&after='+token)
        self.assertEqual(code,200,middle)
        token=subprocess.check_output(['jq','-r','.newest_cursor'],input=middle).decode().strip()
        code,last,_=self.request('GET','/api/communications?limit=2&after='+token)
        self.assertEqual(code,200,last)
        subprocess.run(['jq','-se','([.[].messages[]] | length)==6 and ([.[].messages[].id]|unique|length)==6 and '
            '([.[].messages[]|[.created_at,.project_id,.seq,.id]] as $keys | $keys==($keys|sort)) and '
            '.[0].has_more==false and .[1].has_more==true and .[2].has_more==false'],
            input=b'\n'.join((first,middle,last)),capture_output=True,check=True)
        subprocess.run(['jq','-se','.[0].messages==.[1].messages'],input=b'\n'.join((latest,last)),capture_output=True,check=True)
        first_cursor=subprocess.check_output(['jq','-r','.oldest_cursor'],input=first).decode().strip()
        code,filtered,_=self.request('GET','/api/communications?project=default&agent=alpha0&q=reply&after='+first_cursor)
        self.assertEqual(code,200,filtered)
        subprocess.run(['jq','-e','(.messages|length)==1 and .messages[0].project_id=="default" and '
            '.messages[0].body=="reply 0" and .messages[0].history_cursor==.oldest_cursor and .oldest_cursor==.newest_cursor'],
            input=filtered,capture_output=True,check=True)
        self.assertEqual(self.snapshot(),self.before)
        self.assertEqual(set(self.server.project_contexts),self.contexts_before)

    def test_forward_cursor_rejects_invalid_unknown_and_conflicting_boundaries(self):
        from agent_chat.communication_view import cursor
        for suffix in ('after=invalid','after='+cursor((1,'unknown',1,'message')),
                       'before='+cursor((1,'default',1,'message'))+'&after='+cursor((1,'default',1,'message'))):
            code,raw,_=self.request('GET','/api/communications?'+suffix)
            self.assertEqual(code,400,raw)
        self.assertEqual(self.snapshot(),self.before)
        self.assertEqual(set(self.server.project_contexts),self.contexts_before)

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

    def test_aggregate_hides_private_root_pairs_but_keeps_shared_and_ordinary_deliveries(self):
        import subprocess
        operator=self.server.sender_session
        with contextlib.closing(Coordinator(self.db,operator)) as coord:
            coord.session='root';coord.register('Root')
            coord.session='worker';coord.register('Worker')
            coord.db.execute('INSERT OR IGNORE INTO operator_identities VALUES(?)',(operator,))
            coord.db.execute('CREATE TABLE quiet_root_sessions (session_id TEXT PRIMARY KEY)')
            coord.db.execute('INSERT INTO quiet_root_sessions VALUES(?)',('root',))
            coord.session=operator
            private=coord.send('root','private root needle')
            ordinary=coord.send('worker','ordinary aggregate delivery')
            shared=coord.send_many(['root','worker'],'shared aggregate delivery')
            coord.session='root';coord.send(operator,'reverse private root needle')

        before=self.snapshot()

        code,raw,_=self.request('GET','/api/communications?project=default&q=private+root+needle')
        self.assertEqual(code,200,raw)
        subprocess.run(['jq','-e','(.messages|length)==0'],input=raw,capture_output=True,check=True)
        code,_,_=self.request('GET','/api/communications/message?project=default&id='+private['id'])
        self.assertEqual(code,400)
        code,raw,_=self.request('GET','/api/communications?project=default')
        self.assertEqual(code,200,raw)
        preserved=subprocess.run(['jq','-e','if any(.messages[];.body=="ordinary aggregate delivery") and any(.messages[];.body=="shared aggregate delivery") then true else error({messages:[.messages[].body],errors}|tojson) end'],
                                 input=raw,capture_output=True)
        self.assertEqual(preserved.returncode,0,preserved.stderr.decode())
        self.assertEqual(len(shared),2)
        self.assertEqual(self.snapshot(),before)

    def test_aggregate_visible_reply_omits_hidden_private_parent(self):
        import subprocess
        operator=self.server.sender_session
        with contextlib.closing(Coordinator(self.db,operator)) as coord:
            coord.session='root';coord.register('Root')
            coord.session='worker';coord.register('Worker')
            coord.db.execute('INSERT OR IGNORE INTO operator_identities VALUES(?)',(operator,))
            coord.db.execute('CREATE TABLE quiet_root_sessions (session_id TEXT PRIMARY KEY)')
            coord.db.execute('INSERT INTO quiet_root_sessions VALUES(?)',('root',))
            coord.session=operator;private=coord.send('root','hidden parent')
            coord.db.execute('INSERT INTO messages(id,sender_session,recipient_session,body,created_at) VALUES(?,?,?,?,?)',
                             ('visible-child','worker',operator,'visible child',1))
            coord.db.execute('INSERT INTO message_replies(message_id,reply_to) VALUES(?,?)',
                             ('visible-child',private['id']))

        code,raw,_=self.request('GET','/api/communications?project=default&q=visible+child')
        self.assertEqual(code,200,raw)
        check=subprocess.run(['jq','-e','(.messages|length)==1 and .messages[0].reply_to==null and .messages[0].reply_preview==null'],
                             input=raw,capture_output=True)
        self.assertEqual(check.returncode,0,check.stderr.decode())

    def test_aggregate_shared_batch_keeps_exact_delivery_metadata(self):
        import subprocess
        operator=self.server.sender_session
        with contextlib.closing(Coordinator(self.db,operator)) as coord:
            coord.session='root';coord.register('Root')
            coord.session='worker';coord.register('Worker')
            coord.session=operator;shared=coord.send_many(['root','worker'],'shared metadata')
            coord.session='worker';coord.acknowledge(shared[1]['id'])

        code,raw,_=self.request('GET','/api/communications?project=default&q=shared+metadata')
        self.assertEqual(code,200,raw)
        check=subprocess.run(['jq','-e','''[.messages[]|select(.body=="shared metadata")] as $messages |
            ($messages|length)==2 and all($messages[];
              (.batch_id|type)=="string" and (.deliveries|length)==2 and
              ([.deliveries[].recipient_session]|sort)==["root","worker"] and
              ([.deliveries[]|select(.recipient_session=="root")|.acked_at]|unique)==[null] and
              ([.deliveries[]|select(.recipient_session=="worker")|.acked_at]|length)==1 and
              ([.deliveries[]|select(.recipient_session=="worker")|.acked_at]|all(.!=null)) and
              ([.deliveries[].wake_requested]|unique)==[true])'''],input=raw,capture_output=True)
        self.assertEqual(check.returncode,0,check.stderr.decode())

    def test_aggregate_operator_cardinality_fails_closed_per_project(self):
        import subprocess
        created=self.client.call('/api/projects/rpc',dict(op='create',name='Created operator project'))['project']['id']
        with contextlib.closing(sqlite3.connect(self.server.projects.db_path(created))) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM operator_identities').fetchone()[0],1)
        with contextlib.closing(Coordinator(self.db,self.server.sender_session)) as coord:
            coord.db.execute('INSERT INTO operator_identities VALUES(?)',('other-operator',))

        code,raw,_=self.request('GET','/api/communications?project=default')
        self.assertEqual(code,200,raw)
        check=subprocess.run(['jq','-e','.partial and .loaded_projects==0 and (.errors|length)==1 and (.messages|length)==0'],
                             input=raw,capture_output=True)
        self.assertEqual(check.returncode,0,check.stderr.decode())
        code,_,_=self.request('GET','/api/communications/message?project=default&id='+self.ids[0])
        self.assertEqual(code,400)

        code,raw,_=self.request('GET','/api/communications?project='+self.second+'&q=needle+original+1')
        self.assertEqual(code,200,raw)
        check=subprocess.run(['jq','-e','(.messages|length)==1 and .messages[0].body=="needle original 1" and .partial==false'],
                             input=raw,capture_output=True)
        self.assertEqual(check.returncode,0,check.stderr.decode())
        code,_,_=self.request('POST','/api/sessions/remove',dict(id=self.server.sender_session),
                              headers={'X-Agent-Chat-CSRF':self.server.csrf_token})
        self.assertEqual(code,400)

    def test_aggregate_zero_operator_project_fails_closed_until_normal_setup(self):
        import subprocess
        interrupted=self.server.projects.create('Interrupted operator setup')['id']
        path=self.server.projects.db_path(interrupted)
        with contextlib.closing(sqlite3.connect(path)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM operator_identities').fetchone()[0],0)
            db.executemany('INSERT INTO sessions(id,agent,registered_at,inbox_read_seq) VALUES(?,?,?,?)',[
                ('zero-operator','Operator',1,0),('root','Root',1,0),('worker','Worker',1,0)])
            db.executemany('INSERT INTO messages(id,sender_session,recipient_session,body,created_at) VALUES(?,?,?,?,?)',[
                ('zero-parent','zero-operator','root','zero private parent',1),
                ('zero-child','worker','zero-operator','zero visible child',2)])
            db.execute('INSERT INTO message_replies(message_id,reply_to) VALUES(?,?)',
                       ('zero-child','zero-parent'))
            db.commit()
        def zero_snapshot():
            with contextlib.closing(sqlite3.connect(path)) as db:
                return (db.execute('SELECT id,acked_at FROM messages ORDER BY seq').fetchall(),
                        db.execute('SELECT * FROM message_reads ORDER BY message_id,reader_session').fetchall(),
                        db.execute('SELECT id,inbox_read_seq FROM sessions ORDER BY id').fetchall())
        before=zero_snapshot()

        for query in ('?project='+interrupted,'?project='+interrupted+'&q=zero+visible'):
            code,raw,_=self.request('GET','/api/communications'+query)
            self.assertEqual(code,200,raw)
            check=subprocess.run(['jq','-e','.partial and .loaded_projects==0 and (.errors|length)==1 and (.messages|length)==0'],
                                 input=raw,capture_output=True)
            self.assertEqual(check.returncode,0,check.stderr.decode())
        for message_id in ('zero-parent','zero-child'):
            code,_,_=self.request('GET','/api/communications/message?project='+interrupted+'&id='+message_id)
            self.assertEqual(code,400)
        code,raw,_=self.request('GET','/api/communications?q=needle+original+1')
        self.assertEqual(code,200,raw)
        check=subprocess.run(['jq','-e','.partial and (.errors|length)==1 and (.messages|length)==1 and .messages[0].body=="needle original 1"'],
                             input=raw,capture_output=True)
        self.assertEqual(check.returncode,0,check.stderr.decode())
        self.assertEqual(zero_snapshot(),before)

        self.server.project_context(interrupted)
        after_setup=zero_snapshot()
        code,raw,_=self.request('GET','/api/communications?project='+interrupted+'&q=zero+visible')
        self.assertEqual(code,200,raw)
        check=subprocess.run(['jq','-e','(.messages|length)==1 and .messages[0].body=="zero visible child" and .partial==false'],
                             input=raw,capture_output=True)
        self.assertEqual(check.returncode,0,check.stderr.decode())
        self.assertEqual(zero_snapshot(),after_setup)

    def test_menu_and_readonly_page_are_served_without_compose(self):
        code,raw,_=self.request('GET','/');self.assertEqual(code,200)
        html=raw.decode();self.assertLess(html.index('id="root-conversation"'),html.index('id="all-communication-view"'))
        self.assertLess(html.index('id="all-communication-view"'),html.index('id="all-conversations"'))
        code,raw,_=self.request('GET','/communications');self.assertEqual(code,200)
        self.assertIn('id="communication-panel"',raw.decode());self.assertIn('id="message-input"',raw.decode())
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
