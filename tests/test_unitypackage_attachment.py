"""Opaque generated package transport over an isolated authenticated endpoint."""
import base64
import gzip
import hashlib
import io
import subprocess
import tarfile
import unittest
from agent_chat import core
from agent_chat.remote import RemoteCoordError
from test_large_attachment_transfer import LargeAttachmentRequirementTests


class UnityPackageTransport(LargeAttachmentRequirementTests):
    def test_generated_package_roundtrips_to_exact_recipient(self):
        raw = io.BytesIO()
        with gzip.GzipFile(fileobj=raw,mode='wb',mtime=0,filename='') as compressed:
            with tarfile.open(fileobj=compressed,mode='w|') as archive:
                for name, data in [('0123456789abcdef/pathname',b'Assets/Fixture.txt\n'),('0123456789abcdef/asset',b'Generated inert fixture\n')]:
                    item = tarfile.TarInfo(name); item.size=len(data); item.mtime=0
                    archive.addfile(item,io.BytesIO(data))
        content = raw.getvalue()
        path = self.root / 'fixture.unitypackage'; path.write_bytes(content)
        result = self.command('large-sender','send','--to','large-recipient','--body-file',str(self.body),'--attach',str(path))
        ids = subprocess.run(['jq','-er','[.id,.attachments[0].id] | @tsv'],input=result,text=True,capture_output=True,check=True).stdout.strip().split('\t')
        params = dict(source_project='default',message_id=ids[0],attachment_id=ids[1])
        receipt = self.call('attachment-download',session='large-recipient',**params)
        self.assertEqual(base64.b64decode(receipt['content_base64']),content)
        self.assertEqual(receipt['sha256'],hashlib.sha256(content).hexdigest())
        self.assertEqual(receipt['mime'],'application/octet-stream')
        with self.assertRaises(RemoteCoordError): self.call('attachment-download',session='large-unrelated',**params)
        with self.assertRaises(RemoteCoordError): self.call('attachment-download',session='large-recipient',host='wrong-host',**params)

    def test_format_and_default_cap_are_narrow(self):
        for name, content in [('fake.unitypackage',b'not gzip'),('fake.unitypackage',b'\x1f\x8b\x08\xe0'+b'\0'*14),('fake.other',b'\x1f\x8b\x08'+b'\0'*20)]:
            with self.subTest(name=name), self.assertRaises(core.CoordError): core.Coordinator._prepare_attachment(name,content)
        with self.assertRaises(core.CoordError): core.Coordinator._prepare_attachment('too-large.unitypackage',b'\x1f\x8b\x08'+b'\0'*(10*1024*1024))


def load_tests(loader, tests, pattern):
    return unittest.TestSuite([UnityPackageTransport('test_generated_package_roundtrips_to_exact_recipient'),UnityPackageTransport('test_format_and_default_cap_are_narrow')])
