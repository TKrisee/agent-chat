"""Private original-byte native format and resumable package acceptance."""
import base64
import contextlib
import gzip
import os
import platform
import resource
from unittest import mock
import hashlib
import io
from pathlib import Path
import struct
import subprocess
import unittest
import uuid
import zipfile
from agent_chat import core
from agent_chat.remote import HttpClient, RemoteCoordError
from test_large_attachment_transfer import LargeAttachmentRequirementTests


def minimal_exr():
    # One uncompressed black RGB half-float pixel; no decoder/import is invoked.
    # https://openexr.com/en/latest/OpenEXRFileLayout.html
    def attribute(name, kind, value):
        return name.encode()+b'\0'+kind.encode()+b'\0'+struct.pack('<I',len(value))+value
    channels=b''.join(c+b'\0'+struct.pack('<iB3xii',1,0,1,1) for c in (b'B',b'G',b'R'))+b'\0'
    header=struct.pack('<II',20000630,2)
    for name,kind,value in (
        ('channels','chlist',channels),('compression','compression',b'\0'),
        ('dataWindow','box2i',struct.pack('<iiii',0,0,0,0)),
        ('displayWindow','box2i',struct.pack('<iiii',0,0,0,0)),
        ('lineOrder','lineOrder',b'\0'),('pixelAspectRatio','float',struct.pack('<f',1)),
        ('screenWindowCenter','v2f',struct.pack('<ff',0,0)),
        ('screenWindowWidth','float',struct.pack('<f',1))): header+=attribute(name,kind,value)
    header+=b'\0'
    return header+struct.pack('<Q',len(header)+8)+struct.pack('<ii',0,6)+b'\0'*6


class ArtifactPackageAcceptance(LargeAttachmentRequirementTests):
    def test_binary_package_larger_than_recorded_example_roundtrips(self):
        initial_rss=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        source=self.root/'inert-project.zip'
        with zipfile.ZipFile(source,'x',compression=zipfile.ZIP_STORED) as package:
            with package.open('README.txt','w',force_zip64=True) as entry:
                remaining=320_392_827
                block=b'Inert generated package content.\n'*(65536//33)
                while remaining:
                    part=block[:min(len(block),remaining)]
                    entry.write(part); remaining-=len(part)
        size=source.stat().st_size
        self.assertGreater(size,320_392_826)
        digest=hashlib.sha256()
        with source.open('rb') as stream:
            for part in iter(lambda:stream.read(65536),b''): digest.update(part)
        print('generated_package_bytes='+str(size)+' sha256='+digest.hexdigest())
        transfer_id=str(uuid.uuid4())
        send=['send','--to','large-recipient','--body-file',str(self.body),
            '--large-attach',str(source),'--transfer-id',transfer_id]
        calls=[]; original=HttpClient.call
        def counted(client,path,payload,**kwargs):
            if payload.get('op') in ('attachment-upload-chunk','attachment-download-chunk'):
                calls.append((payload['op'],payload['params']['offset']))
            return original(client,path,payload,**kwargs)
        with mock.patch.object(HttpClient,'call',counted):
            # The server commits the third chunk, then its owned HTTP socket
            # drops the reply. A restart resumes the exact ID, never the prefix.
            with self.lose_response(lambda value:value.get('transfer_id')==transfer_id and value.get('received')==3*self.CHUNK):
                with self.assertRaises(RemoteCoordError): self.command('large-sender',*send)
            self.assertEqual(self.call('attachment-upload-status',transfer_id=transfer_id)['received'],3*self.CHUNK)
            with source.open('rb') as stream:
                stream.seek(2*self.CHUNK); repeated=stream.read(self.CHUNK)
            duplicate=self.call('attachment-upload-chunk',transfer_id=transfer_id,offset=2*self.CHUNK,
                sha256=hashlib.sha256(repeated).hexdigest(),content_base64=base64.b64encode(repeated).decode('ascii'))
            self.assertEqual(duplicate['received'],3*self.CHUNK)
            self.stop(); self.start(); resumed_index=len(calls)
            with self.lose_response(lambda value:bool(value.get('attachments')) and value.get('sender_session')=='large-sender'):
                with self.assertRaises(RemoteCoordError): self.command('large-sender',*send)
            offsets=[offset for op,offset in calls[resumed_index:] if op=='attachment-upload-chunk']
            self.assertEqual(min(offsets),3*self.CHUNK); self.assertEqual(len(offsets),len(set(offsets)))
            self.stop(); self.start(); published_index=len(calls)
            sent=self.command('large-sender',*send)
            self.assertFalse(any(op=='attachment-upload-chunk' for op,_ in calls[published_index:]))
        fields=subprocess.run(['jq','-er','[.id,.attachments[0].id,.attachments[0].name,.attachments[0].mime,.attachments[0].size] | @tsv'],
            input=sent,text=True,capture_output=True,check=True).stdout.strip().split('\t')
        self.assertEqual(fields[2:],['inert-project.zip','application/zip',str(size)])
        with contextlib.closing(core.Coordinator(self.db)) as coord:
            self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM messages WHERE sender_session=? AND recipient_session=?',('large-sender','large-recipient')).fetchone()[0],1)
            self.assertEqual(tuple(coord.db.execute('SELECT COUNT(*),MAX(length(content)) FROM attachments').fetchone()),(1,0))
            self.assertLessEqual(coord.db.execute('SELECT MAX(length(content)) FROM attachment_transfer_chunks').fetchone()[0],self.CHUNK)
        destination=self.root/'received.zip'
        download=['attachment-download','--resume','--source-project','default',
            '--message-id',fields[0],'--attachment-id',fields[1],'--output',str(destination)]
        calls.clear()
        with mock.patch.object(HttpClient,'call',counted):
            with self.lose_response(lambda value:value.get('attachment_id')==fields[1] and value.get('offset')==2*self.CHUNK):
                with self.assertRaises(RemoteCoordError): self.command('large-recipient',*download)
            self.assertFalse(destination.exists())
            partials=list(self.root.glob('.agent-chat-resume-*.part'))
            self.assertEqual(len(partials),1); self.assertEqual(partials[0].stat().st_size,2*self.CHUNK)
            self.stop(); self.start(); resumed_index=len(calls)
            self.command('large-recipient',*download)
            offsets=[offset for op,offset in calls[resumed_index:] if op=='attachment-download-chunk']
            self.assertEqual(min(offsets),2*self.CHUNK); self.assertEqual(len(offsets),len(set(offsets)))
        actual=hashlib.sha256()
        with destination.open('rb') as stream:
            for part in iter(lambda:stream.read(65536),b''): actual.update(part)
        self.assertEqual(actual.hexdigest(),digest.hexdigest()); self.assertEqual(destination.stat().st_size,size)
        self.assertFalse(list(self.root.glob('.agent-chat-resume-*.part')))
        with self.assertRaisesRegex(core.CoordError,'already exists'): self.command('large-recipient',*download)
        params=dict(source_project='default',message_id=fields[0],attachment_id=fields[1],offset=0)
        with self.assertRaises(RemoteCoordError): self.call('attachment-download-chunk',session='large-unrelated',**params)
        with self.assertRaises(RemoteCoordError): self.call('attachment-download-chunk',session='large-recipient',host='wrong-host',**params)
        with self.assertRaises(RemoteCoordError): self.call('attachment-download-chunk',session='large-recipient',**dict(params,source_project='wrong-project'))
        growth=(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss-initial_rss)*(1 if platform.system()=='Darwin' else 1024)
        self.assertLess(growth,128*1024*1024,'bounded transfer must not allocate the whole package')
        print('one_publication=true upload_resume_offset='+str(3*self.CHUNK)+' download_resume_offset='+str(2*self.CHUNK)
            +' peak_rss_growth_bytes='+str(growth)+' retained_fixture_disk_bytes='+str(sum(x.stat().st_size for x in self.root.rglob('*') if x.is_file())))

    def test_missing_native_formats_preserve_bytes_and_recipient_boundary(self):
        # The .itrdraft sample is opaque original-byte transport data, not a
        # claim about application semantics or an invented decoder/schema.
        sentinel=self.root/'must-not-execute'
        source_bytes=('from pathlib import Path\nPath('+repr(str(sentinel))+').touch()\n').encode()
        formats=[('change.diff',b'--- a\n+++ b\n','text/plain'),
                 ('inert.py',source_bytes,'text/plain'),
                 ('inert.unitypackage',gzip.compress(b'Generated opaque package fixture',mtime=0),'application/octet-stream'),
                 ('inert.itrdraft',b'Inert project draft\0\x01\x02\xff','application/octet-stream'),
                 ('black.exr',minimal_exr(),'image/x-exr')]
        for name,data,mime in formats:
            with self.subTest(name=name):
                source=self.root/name; source.write_bytes(data)
                sent=self.command('large-sender','send','--to','large-recipient','--body-file',str(self.body),'--attach',str(source))
                fields=subprocess.run(['jq','-er','[.id,.attachments[0].id,.attachments[0].name,.attachments[0].mime,.attachments[0].size] | @tsv'],
                    input=sent,text=True,capture_output=True,check=True).stdout.strip().split('\t')
                self.assertEqual(fields[2:],[name,mime,str(len(data))])
                output=self.root/('received-'+name)
                self.command('large-recipient','attachment-download','--source-project','default',
                    '--message-id',fields[0],'--attachment-id',fields[1],'--output',str(output))
                self.assertEqual(output.read_bytes(),data)
                self.assertEqual(hashlib.sha256(output.read_bytes()).hexdigest(),hashlib.sha256(data).hexdigest())
                self.assertFalse(sentinel.exists())
                params=dict(source_project='default',message_id=fields[0],attachment_id=fields[1])
                with self.assertRaises(RemoteCoordError): self.call('attachment-download',session='large-unrelated',**params)
                with self.assertRaises(RemoteCoordError): self.call('attachment-download',session='large-recipient',host='wrong-host',**params)

    def test_packet_error_identifies_member_filename_and_size(self):
        good=self.root/'good.diff'; good.write_text('--- a\n+++ b\n')
        bad=self.root/'oversized.py'
        with bad.open('wb') as out: out.truncate(core.MAX_ATTACHMENT_SIZE+1)
        with self.assertRaisesRegex(core.CoordError,r'attachment #2.*oversized\.py.*10485761.*10485760'):
            self.command('large-sender','send','--to','large-recipient','--body-file',str(self.body),
                '--attach',str(good),'--attach',str(bad))
        with contextlib.closing(core.Coordinator(self.db)) as coord:
            self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM messages').fetchone()[0],0)

    def test_binary_headers_and_packet_member_errors_are_atomic(self):
        for name,data in [('bad.zip',b'not zip'),('bad.exr',b'not an EXR'),
                          ('future.exr',minimal_exr()[:4]+b'\x03'+minimal_exr()[5:]),
                          ('empty.itrdraft',b''),('script.py',b'#!/bin/sh\necho inert\n')]:
            with self.subTest(name=name):
                attachments=[dict(name='good.diff',content_base64=base64.b64encode(b'--- a\n+++ b\n').decode()),
                    dict(name=name,content_base64=base64.b64encode(data).decode())]
                with self.assertRaisesRegex(RemoteCoordError,'attachment #2: .*filename='+repr(name)):
                    self.call('send',to='large-recipient',body='Atomic packet',attachments=attachments)
        with self.assertRaisesRegex(RemoteCoordError,r'attachment #2: .*invalid base64; filename='):
            self.call('send',to='large-recipient',body='Malformed member',attachments=[dict(name='good.diff',content_base64='YQ=='),dict(name='bad.zip',content_base64='%%')])
        with contextlib.closing(core.Coordinator(self.db)) as coord:
            self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM messages').fetchone()[0],0)
            self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM attachments').fetchone()[0],0)

    def test_large_binary_invalid_header_and_changed_intent_remain_unpublished(self):
        data=b'not zip'+b'\0'*40; tid=str(uuid.uuid4())
        begin=dict(transfer_id=tid,to='large-recipient',name='invalid.zip',size=len(data),
            sha256=hashlib.sha256(data).hexdigest(),body='Exact package intent',reply_to=None)
        self.call('attachment-upload-begin',**begin)
        for changed in (dict(begin,to='large-unrelated'),dict(begin,name='invalid.itrdraft'),dict(begin,body='Changed')):
            with self.assertRaises(RemoteCoordError): self.call('attachment-upload-begin',**changed)
        self.call('attachment-upload-chunk',transfer_id=tid,offset=0,sha256=begin['sha256'],content_base64=base64.b64encode(data).decode())
        with self.assertRaisesRegex(RemoteCoordError,'header'): self.call('attachment-upload-send',transfer_id=tid)
        with contextlib.closing(core.Coordinator(self.db)) as coord:
            self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM messages').fetchone()[0],0)
        self.assertEqual(self.call('attachment-upload-cancel',transfer_id=tid)['state'],'cancelled')


def load_tests(loader,tests,pattern):
    names=[name for name in ArtifactPackageAcceptance.__dict__ if name.startswith('test_')]
    return unittest.TestSuite(ArtifactPackageAcceptance(name) for name in names)
