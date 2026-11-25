import contextlib
import hashlib
import io
import sqlite3
import subprocess
import unittest
import uuid
import zipfile
from agent_chat.remote import RemoteCoordError
import test_peer_large_assignment as fixture

class CompositionTests(fixture.PeerLargeAssignmentTests):
    def packet(self):
        out = io.BytesIO()
        with zipfile.ZipFile(out, 'w', compression=zipfile.ZIP_STORED) as archive:
            archive.writestr('inert.bin', b'x' * (1024 * 1024 + 37))
        return out.getvalue()

    def packet_identity(self, peer, transfer, data):
        return {**self.transfer_identity(peer, transfer, data), 'name': 'project.zip'}

    def test_linked_zip_resumes_and_recipient_gets_exact_bytes(self):
        peer, _ = self.peer(); transfer = str(uuid.uuid4()); data = self.packet()
        body = self.fixture/'output'/'body.txt'; body.write_text('large body')
        source = self.fixture/'output'/'project.zip'; source.write_bytes(data)
        self.po_call('attachment-upload-begin', session=self.worker, **self.packet_identity(peer, transfer, data))
        self.upload(transfer, data[:1024*1024])
        args = ('send','--to',peer,'--body-file',str(body),'--large-attach',str(source),
                '--transfer-id',transfer,'--assignment-id',self.task['id'])
        output = self.cli_call(self.worker, *args)
        ids = subprocess.run(['jq','-er','[.id,.attachments[0].id,.attachments[0].mime] | @tsv'],
            input=output, text=True, capture_output=True, check=True).stdout.strip().split('\t')
        self.assertEqual(ids[2], 'application/zip')
        retry = self.cli_call(self.worker, *args)
        self.assertEqual(subprocess.run(['jq','-er','.id'],input=retry,text=True,capture_output=True,check=True).stdout.strip(),ids[0])
        self.assertEqual(self.po_call('message',session=peer,id=ids[0])['message']['assignment_provenance']['task_id'], self.task['id'])
        destination = self.fixture/'output'/'received.zip'
        self.cli_call(peer, 'attachment-download','--resume','--source-project',self.project_id,'--message-id',ids[0],
                      '--attachment-id',ids[1], '--output',str(destination))
        self.assertEqual(hashlib.sha256(destination.read_bytes()).digest(), hashlib.sha256(data).digest())

    def test_blocked_link_prevents_zip_publication(self):
        peer, _ = self.peer(); transfer = str(uuid.uuid4()); data = self.packet()
        self.po_call('attachment-upload-begin',session=self.worker,**self.packet_identity(peer,transfer,data))
        self.upload(transfer,data[:1024*1024]); self.upload(transfer,data[1024*1024:],offset=1024*1024)
        self.po_call('context'); self.po_call('activity-transition',work_id=self.task['id'],state='blocked')
        with self.assertRaisesRegex(RemoteCoordError,'assignment link'):
            self.po_call('attachment-upload-send',session=self.worker,transfer_id=transfer)
        with contextlib.closing(sqlite3.connect(self.server.projects.db_path(self.project_id))) as db:
            self.assertEqual(db.execute('SELECT state,message_id FROM attachment_transfers WHERE id=?',(transfer,)).fetchone(),('uploading',None))
            self.assertEqual(db.execute('SELECT count(*) FROM message_assignment_context').fetchone()[0],0)

def load_tests(loader, tests, pattern):
    return unittest.TestSuite(CompositionTests(name) for name in CompositionTests.__dict__ if name.startswith('test_'))
