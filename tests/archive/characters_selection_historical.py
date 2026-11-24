"""Synthetic child/parent routing and digest fixtures; no Characters target access."""
import base64
import contextlib
import hashlib
import os
import tempfile
import time
from pathlib import Path
from unittest.mock import patch
from agent_chat import exact_file_read
from agent_chat.bridge_state import BridgeState, resolve_binding_route
from agent_chat.core import Coordinator, CoordError
from agent_chat.remote_service import dispatch
from test_exact_file_read import ExactFileReadTests

class CharacterSelectionTests(ExactFileReadTests):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='characters-selection-',dir=os.path.realpath(tempfile.gettempdir()))
        self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name); self.db=self.root/'coord.sqlite3'
        self.src=self.root/'external path'/'file name.txt'; self.src.parent.mkdir()
        self.src.write_bytes(b'spaced content')
        self.outdir=self.root/'output'; self.outdir.mkdir(mode=0o700)
        for key,value in (('SELECTED_PATH',str(self.src)),('SELECTED_PROJECT','default'),('SELECTED_SESSION','a'),('SELECTED_HOST','host-a')):
            item=patch.object(exact_file_read,key,value); item.start(); self.addCleanup(item.stop)
        self.call('register',agent='fixture')

    def test_selected_child_route_and_denials(self):
        with patch.object(exact_file_read,'CHARACTERS_FILES',{str(self.src):'fixture-pin'}), patch.object(exact_file_read,'CHARACTERS_HOST','host-a'):
            self.call('register',session='c',host='host-a',agent='fixture-reviewer')
            self.call('register',session='p',host='host-a',agent='fixture-parent')
            self.call('register',session='other',host='host-a',agent='wrong-actor')
            with patch.object(exact_file_read,'CHARACTERS_SESSION',None):
                with self.assertRaisesRegex(CoordError,'selection differs'):
                    self.call('exact-file-request',session='c',path=str(self.src),minutes=5)
            with patch.object(exact_file_read,'CHARACTERS_SESSION','c'), patch.object(exact_file_read,'CHARACTERS_PARENT','p'), patch.object(exact_file_read,'CHARACTERS_AGENT_PATH','/root/fixture'), patch.object(exact_file_read,'CHARACTERS_PARENT_THREAD','p-thread'):
                with self.assertRaises(CoordError):
                    self.call('exact-file-request',session='c',host='wrong-host',path=str(self.src),minutes=5)
                with self.assertRaisesRegex(CoordError,'selection differs'):
                    self.call('exact-file-request',session='c',path=str(self.src)+'-wrong',minutes=5)
                with contextlib.closing(Coordinator(self.db)) as coord:
                    with self.assertRaisesRegex(CoordError,'selection differs'):
                        dispatch(coord,{'op':'exact-file-request','session':'c','host_id':'host-a','params':{'path':str(self.src),'minutes':5}},canonical_project='wrong-project')
                with self.assertRaisesRegex(CoordError,'selection differs'):
                    self.call('exact-file-request',session='other',path=str(self.src),minutes=5)
                with self.assertRaisesRegex(CoordError,'route unavailable'):
                    self.call('exact-file-request',session='c',path=str(self.src),minutes=5)
                with contextlib.closing(Coordinator(self.db)) as coord:
                    BridgeState(coord)
                    coord.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?,?,?)',('p','p-thread',None,None,time.time()))
                    coord.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?,?,?)',('c',None,'p','/root/fixture',time.time()))
                for field,bad in (('thread_id','fake-child-thread'),('parent_session','other'),('agent_path','/root/other')):
                    with contextlib.closing(Coordinator(self.db)) as coord:
                        before=coord.db.execute('SELECT '+field+' FROM bridge_bindings WHERE session_id=?',('c',)).fetchone()[0]
                        coord.db.execute('UPDATE bridge_bindings SET '+field+'=? WHERE session_id=?',(bad,'c'))
                    with self.assertRaises(CoordError): self.call('exact-file-request',session='c',path=str(self.src),minutes=5)
                    with contextlib.closing(Coordinator(self.db)) as coord:
                        coord.db.execute('UPDATE bridge_bindings SET '+field+'=? WHERE session_id=?',(before,'c'))
                with contextlib.closing(Coordinator(self.db)) as coord:
                    coord.db.execute('UPDATE remote_session_hosts SET host_id=? WHERE session_id=?',('wrong-parent-host','p'))
                with self.assertRaisesRegex(CoordError,'reviewer route differs'):
                    self.call('exact-file-request',session='c',path=str(self.src),minutes=5)
                with contextlib.closing(Coordinator(self.db)) as coord:
                    coord.db.execute('UPDATE remote_session_hosts SET host_id=? WHERE session_id=?',('host-a','p'))
                claim=self.call('exact-file-request',session='c',path=str(self.src),minutes=5)
                self.assertEqual(claim['state'],'owned')
                with contextlib.closing(Coordinator(self.db)) as coord:
                    coord.db.execute('UPDATE bridge_bindings SET thread_id=? WHERE session_id=?',('changed-parent-thread','p'))
                with self.assertRaisesRegex(CoordError,'reviewer route differs'):
                    self.call('exact-file-begin',session='c',path=str(self.src),token=claim['token'],reservation_id=claim['reservation_id'])
                with contextlib.closing(Coordinator(self.db)) as coord:
                    coord.db.execute('UPDATE bridge_bindings SET thread_id=? WHERE session_id=?',('p-thread','p'))
                guard=self.call('exact-file-begin',session='c',path=str(self.src),token=claim['token'],reservation_id=claim['reservation_id'])
                pulse=self.call('exact-file-pulse',session='c',path=str(self.src),run_id=guard['run_id'],token=claim['token'])
                self.assertEqual(pulse['state'],'owned')
                self.call('close-guard',session='c',run_id=guard['run_id'],token=claim['token'],evidence_sha256='a'*64)
                receipt=dict(version=1,resource=str(self.src),reservation_id=claim['reservation_id'],restored=True,processes_closed=True,closed_at=time.time(),evidence='private.txt',pids=[])
                released=self.call('exact-file-release',session='c',path=str(self.src),token=claim['token'],receipt=receipt,evidence_base64=base64.b64encode(b'synthetic fixture without task child').decode())
                self.assertTrue(released['released'])

    def test_pinned_digest_precedes_publication(self):
        digest=hashlib.sha256(b'spaced content').hexdigest()
        with patch.object(exact_file_read,'CHARACTERS_FILES',{str(self.src):digest}):
            out=self.outdir/'pinned'; checks=[]
            result=exact_file_read.snapshot(str(self.src),str(out),1024,validate=lambda:checks.append('guard'))
            self.assertEqual((result['sha256'],out.read_bytes(),checks),(digest,b'spaced content',['guard','guard']))
            out.unlink()
            with patch.object(exact_file_read,'CHARACTERS_FILES',{str(self.src):'0'*64}):
                with self.assertRaisesRegex(CoordError,'frozen digest differs'): exact_file_read.snapshot(str(self.src),str(out),1024)
                self.assertFalse(out.exists())

    def test_exact_author_v2_manifest_set(self):
        # These are plain author-provided checksum lists, not target files.
        root=Path(os.environ['CHARACTERS_MANIFEST_DIR'])
        def records(name):
            result={}
            for line in (root/name).read_text().splitlines():
                digest,path=line.split(None,1)
                result[path]=digest
            return result
        selected=records('FROZEN_HASHES.txt')
        selected.update(records('DELTA_HASHES_v2.txt'))
        self.assertEqual(len(selected),13)
        self.assertEqual(dict(exact_file_read.CHARACTERS_FILES),selected)
        with self.assertRaises(TypeError):
            exact_file_read.CHARACTERS_FILES['/tmp/unlisted']='0'*64

    def test_each_synthetic_file_uses_its_own_pin(self):
        first=self.src
        second=self.src.parent/'second.txt'
        second.write_bytes(b'second content')
        pins={str(first):hashlib.sha256(b'spaced content').hexdigest(),
              str(second):hashlib.sha256(b'second content').hexdigest()}
        with patch.object(exact_file_read,'CHARACTERS_FILES',pins):
            for index,path in enumerate((first,second)):
                out=self.outdir/('ok-'+str(index))
                self.assertEqual(exact_file_read.snapshot(str(path),str(out),1024)['sha256'],pins[str(path)])
            pins[str(second)]=pins[str(first)]
            out=self.outdir/'wrong-pin'
            with self.assertRaisesRegex(CoordError,'frozen digest differs'):
                exact_file_read.snapshot(str(second),str(out),1024)
            self.assertFalse(out.exists())

    def test_existing_bridge_resolver_still_rejects_cycles(self):
        with contextlib.closing(Coordinator(self.db)) as coord:
            state=BridgeState(coord)
            coord.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?,?,?)',('c',None,'p','/root/c',time.time()))
            coord.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?,?,?)',('p',None,'c','/root/p',time.time()))
            with self.assertRaisesRegex(CoordError,'cyclic bridge parent route'):
                resolve_binding_route(coord.db,'c')
            with self.assertRaisesRegex(CoordError,'cyclic bridge parent route'):
                state.resolve('c')
