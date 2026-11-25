import base64
import contextlib
import hashlib
import sqlite3
import subprocess
import unittest
import uuid
import test_peer_assignment_context as fixture
from agent_chat.remote import RemoteCoordError

class PeerLargeAssignmentTests(fixture.PeerAssignmentContextTests):
    def transfer_identity(self,peer,transfer,data,*,task=True):
        value=dict(transfer_id=transfer,to=peer,name='large.txt',size=len(data),
                   sha256=hashlib.sha256(data).hexdigest(),body='large body',reply_to=None)
        if task:value['assignment_id']=self.task['id']
        return value
    def upload(self,transfer,data,offset=0):
        return self.po_call('attachment-upload-chunk',session=self.worker,transfer_id=transfer,
            offset=offset,sha256=hashlib.sha256(data).hexdigest(),content_base64=base64.b64encode(data).decode())
    def test_resume_cli_retains_exact_link_and_idempotent_result(self):
        peer,_=self.peer();transfer=str(uuid.uuid4());data=b'a'*(1024*1024)+b'end'
        body=self.fixture/'output'/'body.txt';body.write_text('large body')
        attachment=self.fixture/'output'/'large.txt';attachment.write_bytes(data)
        self.po_call('attachment-upload-begin',session=self.worker,**self.transfer_identity(peer,transfer,data))
        self.upload(transfer,data[:1024*1024])
        with self.assertRaisesRegex(RemoteCoordError,'retry differs'):
            self.po_call('attachment-upload-begin',session=self.worker,**{**self.transfer_identity(peer,transfer,data),'assignment_id':'another-task'})
        with self.assertRaisesRegex(RemoteCoordError,'retry differs'):
            self.po_call('attachment-upload-begin',session=self.worker,**self.transfer_identity(peer,transfer,data,task=False))
        args=('send','--to',peer,'--body-file',str(body),'--large-attach',str(attachment),'--transfer-id',transfer,'--assignment-id',self.task['id'])
        output=self.cli_call(self.worker,*args)
        first=subprocess.run(['jq','-er','.id'],input=output,text=True,capture_output=True,check=True).stdout.strip()
        repeated=self.cli_call(self.worker,*args)
        second=subprocess.run(['jq','-er','.id'],input=repeated,text=True,capture_output=True,check=True).stdout.strip()
        self.assertEqual(first,second)
        message=self.po_call('message',session=peer,id=first)['message']
        self.assertEqual(message['assignment_provenance']['source_request_id'],self.assignment)
        self.assertEqual(message['instruction_origin'],'agent_message')
        with self.assertRaisesRegex(RemoteCoordError,'retry differs'):
            self.po_call('attachment-upload-begin',session=self.worker,**{**self.transfer_identity(peer,transfer,data),'assignment_id':'another-task'})
    def test_revoked_task_prevents_publication_without_message(self):
        peer,_=self.peer();transfer=str(uuid.uuid4());data=b'inert'
        self.po_call('attachment-upload-begin',session=self.worker,**self.transfer_identity(peer,transfer,data))
        self.upload(transfer,data)
        self.po_call('context');self.po_call('activity-transition',work_id=self.task['id'],state='blocked')
        with self.assertRaisesRegex(RemoteCoordError,'assignment link'):
            self.po_call('attachment-upload-send',session=self.worker,transfer_id=transfer)
        with contextlib.closing(sqlite3.connect(self.server.projects.db_path(self.project_id))) as db:
            row=db.execute('SELECT state,message_id FROM attachment_transfers WHERE id=?',(transfer,)).fetchone()
            self.assertEqual(row,('uploading',None))
            self.assertEqual(db.execute('SELECT count(*) FROM message_assignment_context').fetchone()[0],0)
    def test_invalid_task_is_refused_before_transfer_creation(self):
        peer,_=self.peer();transfer=str(uuid.uuid4());data=b'inert'
        with self.assertRaisesRegex(RemoteCoordError,'assignment link'):
            self.po_call('attachment-upload-begin',session=self.worker,**{**self.transfer_identity(peer,transfer,data),'assignment_id':'not-a-task'})
        with contextlib.closing(sqlite3.connect(self.server.projects.db_path(self.project_id))) as db:
            exists=db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='attachment_transfers'").fetchone()
            if exists:self.assertEqual(db.execute('SELECT count(*) FROM attachment_transfers WHERE id=?',(transfer,)).fetchone()[0],0)
    def test_unlinked_large_transfer_retains_existing_behavior(self):
        peer,_=self.peer();transfer=str(uuid.uuid4());data=b'inert'
        self.po_call('attachment-upload-begin',session=self.worker,**self.transfer_identity(peer,transfer,data,task=False))
        self.upload(transfer,data)
        sent=self.po_call('attachment-upload-send',session=self.worker,transfer_id=transfer)
        self.assertNotIn('assignment_provenance',self.po_call('message',session=peer,id=sent['id'])['message'])

    def test_lost_manager_prevents_publication_without_message(self):
        peer,_=self.peer();transfer=str(uuid.uuid4());data=b'inert'
        self.po_call('attachment-upload-begin',session=self.worker,**self.transfer_identity(peer,transfer,data))
        self.upload(transfer,data)
        # Synthetic private change to current team membership.
        with sqlite3.connect(self.server.projects.db_path(self.project_id)) as db:
            db.execute('DELETE FROM agent_management WHERE target_session=?',(peer,))
        with self.assertRaisesRegex(RemoteCoordError,'assignment link'):
            self.po_call('attachment-upload-send',session=self.worker,transfer_id=transfer)
        with contextlib.closing(sqlite3.connect(self.server.projects.db_path(self.project_id))) as db:
            self.assertEqual(db.execute('SELECT state,message_id FROM attachment_transfers WHERE id=?',(transfer,)).fetchone(),('uploading',None))
            self.assertEqual(db.execute('SELECT count(*) FROM message_assignment_context').fetchone()[0],0)

    def test_sender_management_loss_prevents_publication(self):
        peer,_=self.peer();transfer=str(uuid.uuid4());data=b'inert'
        self.po_call('attachment-upload-begin',session=self.worker,**self.transfer_identity(peer,transfer,data))
        self.upload(transfer,data)
        # Synthetic private removal of the owner's current manager relation.
        with sqlite3.connect(self.server.projects.db_path(self.project_id)) as db:
            db.execute('DELETE FROM agent_management WHERE target_session=?',(self.worker,))
        with self.assertRaisesRegex(RemoteCoordError,'assignment link'):
            self.po_call('attachment-upload-send',session=self.worker,transfer_id=transfer)
        with contextlib.closing(sqlite3.connect(self.server.projects.db_path(self.project_id))) as db:
            self.assertEqual(db.execute('SELECT state,message_id FROM attachment_transfers WHERE id=?',(transfer,)).fetchone(),('uploading',None))
            self.assertEqual(db.execute('SELECT count(*) FROM message_assignment_context').fetchone()[0],0)

def load_tests(loader,tests,pattern):
    return unittest.TestSuite(PeerLargeAssignmentTests(name) for name in PeerLargeAssignmentTests.__dict__ if name.startswith('test_'))
