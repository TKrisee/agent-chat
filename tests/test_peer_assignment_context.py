import subprocess
import sqlite3
import unittest
from agent_chat.remote import RemoteCoordError
import test_exact_file_binding as fixture

class PeerAssignmentContextTests(fixture.ExactFileBindingTests):
    def peer(self):
        created,thread,_=self.create_worker('peer')
        return created['target_session'],thread

    def test_explicit_task_context_reaches_peer_and_wake(self):
        peer,thread=self.peer()
        sent=self.po_call('send',session=self.worker,to=peer,body='private measurement',assignment_id=self.task['id'])
        payload=self.project_bridge_instance.payload(thread,[{'id':sent['id'],'recipient_session':peer}])
        program='[splits("\\n")|fromjson?|select(type=="object" and has("messages"))|.messages[]|select(.id==$id)] | length==1 and .[0].assignment_provenance.task_id==$task and .[0].assignment_provenance.source_request_id==$source and .[0].assignment_provenance.instruction_origin=="agent_message" and (tostring|contains("Own one new private exact-file snapshot task.")|not)'
        subprocess.run(['jq','-Rse','--arg','id',sent['id'],'--arg','task',self.task['id'],'--arg','source',self.assignment,program],input=payload,text=True,capture_output=True,check=True)
        received=self.po_call('message',session=peer,id=sent['id'])['message']
        self.assertEqual(received['assignment_provenance']['task_id'],self.task['id'])
        self.assertEqual(received['assignment_provenance']['source_request_id'],self.assignment)
        self.assertEqual(received['instruction_origin'],'agent_message')
        self.assertNotIn('body',received['assignment_provenance'])

    def test_same_text_distinct_tasks_and_exact_once_reply(self):
        peer,_=self.peer()
        self.po_call('context')
        source2=self.po_call('send',to=self.worker,body='Second separate assignment')['id']
        task2=self.po_call('activity-assign',goal_id=self.task['goal_id'],worker=self.worker,source_request_id=source2,title='Second private task')
        self.po_call('context',session=self.worker)
        first=self.po_call('send',session=self.worker,to=peer,body='same measurement',assignment_id=self.task['id'])
        second=self.po_call('send',session=self.worker,to=peer,body='same measurement',assignment_id=task2['id'])
        self.assertNotEqual(first['id'],second['id'])
        for message,source in ((first,self.assignment),(second,source2)):
            result=self.po_call('message',session=peer,id=message['id'])['message']
            self.assertEqual(result['assignment_provenance']['source_request_id'],source)
        one=self.po_call('send',session=peer,to=self.worker,body='same answer',reply_to=first['id'],ack_reply=True)
        two=self.po_call('send',session=peer,to=self.worker,body='same answer',reply_to=second['id'],ack_reply=True)
        self.assertNotEqual(one['id'],two['id'])
        again=self.po_call('send',session=peer,to=self.worker,body='same answer',reply_to=first['id'],ack_reply=True)
        self.assertEqual(again['id'],one['id'])
        with self.assertRaisesRegex(RemoteCoordError,'retry payload differs'):
            self.po_call('send',session=peer,to=self.worker,body='changed answer',reply_to=first['id'],ack_reply=True)

    def test_no_implicit_link_or_wrong_owner_or_root_disclosure(self):
        peer,_=self.peer()
        plain=self.po_call('send',session=self.worker,to=peer,body='unlinked')
        self.assertNotIn('assignment_provenance',self.po_call('message',session=peer,id=plain['id'])['message'])
        with self.assertRaises(RemoteCoordError): self.po_call('send',session=peer,to=self.worker,body='wrong owner',assignment_id=self.task['id'])
        with self.assertRaises(RemoteCoordError): self.po_call('send',session=self.worker,to=self.po,body='not peer',assignment_id=self.task['id'])
        with self.assertRaises(RemoteCoordError): self.po_call('send',session=self.worker,to=peer,body='not task',assignment_id='work_nonexistent')

    def test_blocked_task_is_not_linked_or_disclosed(self):
        peer,_=self.peer()
        sent=self.po_call('send',session=self.worker,to=peer,body='while active',assignment_id=self.task['id'])
        self.po_call('context')
        self.po_call('activity-transition',work_id=self.task['id'],state='blocked')
        self.assertNotIn('assignment_provenance',self.po_call('message',session=peer,id=sent['id'])['message'])
        with self.assertRaises(RemoteCoordError): self.po_call('send',session=self.worker,to=peer,body='blocked',assignment_id=self.task['id'])

    def test_linked_first_ack_retry_does_not_create_duplicate(self):
        peer,_=self.peer()
        request=self.po_call('send',session=peer,to=self.worker,body='peer request')
        self.po_call('message',session=self.worker,id=request['id'])
        args=dict(session=self.worker,to=peer,body='linked reply',reply_to=request['id'],ack_reply=True,assignment_id=self.task['id'])
        first=self.po_call('send',**args)
        again=self.po_call('send',**args)
        self.assertEqual(first['id'],again['id'])
        with self.assertRaisesRegex(RemoteCoordError,'retry payload differs'):
            self.po_call('send',**{**args,'assignment_id':None})
        received=self.po_call('message',session=peer,id=first['id'])['message']
        self.assertEqual(received['assignment_provenance']['task_id'],self.task['id'])
        self.po_call('context')
        self.po_call('activity-transition',work_id=self.task['id'],state='blocked')
        after=self.po_call('send',**args)
        self.assertEqual(after['id'],first['id'])

    def test_actual_cli_direct_send_with_explicit_link(self):
        peer,_=self.peer()
        body=self.fixture/'output'/'peer-body.txt';body.write_text('CLI peer task')
        output=self.cli_call(self.worker,'send','--to',peer,'--body-file',str(body),'--assignment-id',self.task['id'])
        message_id=subprocess.run(['jq','-er','.id'],input=output,text=True,capture_output=True,check=True).stdout.strip()
        received=self.po_call('message',session=peer,id=message_id)['message']
        self.assertEqual(received['assignment_provenance']['source_request_id'],self.assignment)
        with self.assertRaises(RemoteCoordError):
            self.po_call('send-many',session=self.worker,to=[peer],body='group',assignment_id=self.task['id'])

    def test_unread_assignment_cannot_be_linked(self):
        peer,_=self.peer()
        self.po_call('context')
        source=self.po_call('send',to=self.worker,body='New task, unread')['id']
        task=self.po_call('activity-assign',goal_id=self.task['goal_id'],worker=self.worker,source_request_id=source,title='Unread task')
        with self.assertRaises(RemoteCoordError): self.po_call('send',session=self.worker,to=peer,body='too early',assignment_id=task['id'])
        self.po_call('message',session=self.worker,id=source)
        sent=self.po_call('send',session=self.worker,to=peer,body='after reading',assignment_id=task['id'])
        result=self.po_call('message',session=peer,id=sent['id'])['message']
        self.assertEqual(result['assignment_provenance']['source_request_id'],source)

    def test_manager_relationship_loss_hides_and_refuses_link(self):
        peer,_=self.peer()
        sent=self.po_call('send',session=self.worker,to=peer,body='original link',assignment_id=self.task['id'])
        # Synthetic private loss of the peer's manager association.
        with sqlite3.connect(self.server.projects.db_path(self.project_id)) as db:
            db.execute('DELETE FROM agent_management WHERE target_session=?',(peer,))
        self.assertNotIn('assignment_provenance',self.po_call('message',session=peer,id=sent['id'])['message'])
        with self.assertRaises(RemoteCoordError): self.po_call('send',session=self.worker,to=peer,body='after loss',assignment_id=self.task['id'])

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(PeerAssignmentContextTests(name) for name in PeerAssignmentContextTests.__dict__ if name.startswith('test_'))
