"""Private exact-file CLI/HTTP/child compatibility; no historical authority rows."""
import contextlib
import hashlib
import io
import os
from pathlib import Path
import resource
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest import mock

from agent_chat import cli, core, exact_file_read, remote_service, root_local, service
from agent_chat.remote import RemoteCoordError
from agent_chat.web import create_server


class SnapshotCopyWorkflow(unittest.TestCase):
    HOST = 'private-snapshot-host'
    ACTOR = 'private-snapshot-worker'
    TOKEN = 'private-snapshot-http-fixture-only'

    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix='agent-chat-snapshot-', dir='/private/tmp')
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name).resolve()
        self.db = self.root / 'state.sqlite3'
        self.source = self.root / 'source.bin'
        self.output_dir = self.root / 'output'; self.output_dir.mkdir(mode=0o700)
        self.sessions = self.root / 'sessions'; self.sessions.mkdir()
        self.home = self.root / 'home'; self.home.mkdir()
        self.codex = self.root / 'codex'; self.codex.mkdir()
        self.state = self.root / 'client-state'; self.state.mkdir()
        self.stack = contextlib.ExitStack(); self.addCleanup(self.stack.close)
        env = {k:v for k,v in os.environ.items() if not k.startswith('AGENT_CHAT_')
               and k not in ('HOME','CODEX_HOME','PYTHONPATH')}
        env.update(HOME=str(self.home), CODEX_HOME=str(self.codex),
                   AGENT_CHAT_STATE_DIR=str(self.state), AGENT_CHAT_ROOT=str(self.root),
                   AGENT_CHAT_HOST_ID=self.HOST, AGENT_CHAT_PROJECT='default')
        self.stack.enter_context(mock.patch.dict(os.environ, env, clear=True))
        for module in (service, root_local):
            for name in ('_installed_server_config','configured_server_database'):
                self.stack.enter_context(mock.patch.object(module, name,
                    side_effect=AssertionError('installed service discovery outside private fixture')))
        self.stack.enter_context(mock.patch.object(core, 'default_db',
            side_effect=AssertionError('default DB discovery outside private fixture')))
        # Test-only selection substitutes NEW private identifiers/path, never
        # historical production task authority or fabricated origin/read rows.
        for name,value in [('SELECTED_PROJECT','default'),('SELECTED_SESSION',self.ACTOR),
                           ('SELECTED_HOST',self.HOST),('SELECTED_PATH',str(self.source))]:
            self.stack.enter_context(mock.patch.object(exact_file_read, name, value))
        with contextlib.closing(core.Coordinator(self.db)):
            pass
        self.server = create_server(self.db, host='127.0.0.1', port=0,
                                    api_token=self.TOKEN, sessions_root=self.sessions)
        self.assertEqual(self.server.projects.path, Path(str(self.db)+'.projects.sqlite3'))
        self.assertEqual(self.server.measurements.sessions_root, self.sessions)
        thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        thread.start()
        def stop():
            self.server.stop_event.set(); self.server.shutdown(); self.server.server_close()
            thread.join(3); self.assertFalse(thread.is_alive())
        self.addCleanup(stop)
        self.url = 'http://127.0.0.1:' + str(self.server.server_port)
        self.command('register', '--agent', 'snapshot fixture')

    def command(self, *args):
        rc, raw = self.invoke(*args)
        self.assertEqual(rc,0)
        return raw

    def invoke(self, *args):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            rc = cli.main(['--server',self.url,'--api-token',self.TOKEN,'--project','default',
                           '--session',self.ACTOR,*args])
        return rc,out.getvalue()

    def field(self, raw, program):
        return subprocess.run(['jq','-er',program], input=raw, text=True,
            capture_output=True, check=True).stdout.strip()

    def prepare_copy(self, size):
        block = bytes(range(256))*256
        expected = hashlib.sha256()
        with self.source.open('xb') as stream:
            left=size
            while left:
                part=block[:min(left,len(block))]; stream.write(part); expected.update(part); left-=len(part)
        cap=self.output_dir/'capability.json'
        claim=self.command('exact-file-request','--path',str(self.source),'--minutes','5',
                           '--capability-file',str(cap))
        rid=self.field(claim,'.reservation_id')
        self.assertEqual(self.field(claim,'.state'),'owned')
        output=self.output_dir/'copied.bin'; proof=self.output_dir/'process.jsonl'
        args=['exact-file-read','--path',str(self.source),'--reservation-id',rid,
            '--capability-file',str(cap),'--output',str(output),'--max-bytes',str(size),
            '--process-evidence-file',str(proof)]
        return args,rid,output,proof,expected.hexdigest(),float(self.field(claim,'.deadline'))

    def copy_and_observe(self, size):
        args,rid,output,proof,expected,_=self.prepare_copy(size)
        result=self.command(*args)
        self.assertEqual(self.field(result,'.exit_code'),'0')
        actual=hashlib.sha256()
        with self.source.open('rb') as original, output.open('rb') as copied:
            while True:
                a=original.read(65536); b=copied.read(65536); self.assertEqual(a,b)
                if not a: break
                actual.update(b)
        self.assertEqual(output.stat().st_size,size)
        self.assertEqual(actual.hexdigest(),expected)
        # The journal comes from the real CLI runner/child, not this fixture.
        journal=proof.read_text()
        checks=['jq','-es','--arg','sid',self.ACTOR,'--arg','rid',rid,
            'all(.session_id==$sid) and any(.event=="admitted" and any(.reservations[]; .reservation_id==$rid)) '
            'and any(.event=="launcher_created") and any(.event=="leader_reaped" and .returncode==0)']
        subprocess.run(checks,input=journal,text=True,capture_output=True,check=True)
        outcome=self.command('exact-file-result','--path',str(self.source),'--reservation-id',rid)
        # Copy bytes and process-group closure do not prove every effect or
        # supply a server-verified executor completion/release receipt.
        self.assertEqual(self.field(outcome,'.state'),'pending')
        self.assertEqual(self.field(outcome,'.receipt_backed_clear|tostring'),'false')
        evidence=Path(__file__).resolve().parents[2]/'observed'
        evidence.mkdir(exist_ok=True)
        prefix=self._testMethodName+'-'+str(size)
        (evidence/(prefix+'-outcome.json')).write_text(outcome)
        shutil.copyfile(proof,evidence/(prefix+'-process.jsonl'))
        (evidence/(prefix+'-copy.txt')).write_text(
            f'bytes={size}\nsha256={actual.hexdigest()}\nexact_bytes=true\nserver_outcome=pending\n')
        peak=resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
        peak_bytes=int(peak if sys.platform=='darwin' else peak*1024)
        (evidence/(prefix+'-child-memory.txt')).write_text(
            f'payload_bytes={size}\nreaped_child_max_rss_bytes={peak_bytes}\n'
            'scope=cumulative maximum across reaped fixture children, including launcher/executor and jq\n'
            'not_exact_executor_pid_peak=true\n')
        print(f'copied_bytes={size} sha256={actual.hexdigest()} server_outcome=pending')
        print(f'payload_bytes={size} reaped_child_max_rss_bytes={peak_bytes}')
        return peak_bytes

    def test_small_complete_copy_keeps_missing_completion_proof_pending(self):
        self.copy_and_observe(262144)

    def test_above_eight_mib_copy_has_exact_bytes_and_hash(self):
        self.copy_and_observe(8*1024*1024+1)

    def test_explicit_sixty_four_mib_bound_has_bounded_child_memory(self):
        small_peak=self.copy_and_observe(262144)
        self.source=self.root/'large-source.bin'
        self.output_dir=self.root/'large-output'; self.output_dir.mkdir(mode=0o700)
        with mock.patch.object(exact_file_read,'SELECTED_PATH',str(self.source)):
            peak=self.copy_and_observe(64*1024*1024)
        # A payload-sized child allocation must not accompany the larger copy.
        # This is a finite fixture RSS regression, not a universal RSS budget.
        self.assertLess(peak-small_peak,16*1024*1024)

    def expired_capability(self, phase):
        size=4*65536
        args,rid,output,proof,_,deadline=self.prepare_copy(size)
        original=remote_service.dispatch; original_now=core._now
        expired=[]; pulses=[]
        def dispatch(coord, body, **kwargs):
            if body.get('op')=='exact-file-pulse':
                sizes=[p.stat().st_size for p in self.output_dir.glob('.agent-chat-snapshot-*')]
                staged=max(sizes,default=0); pulses.append(staged)
                eligible=(phase=='before_open' or
                          phase=='during_stream' and 0<staged<size or
                          phase=='before_publish' and staged==size)
                if eligible and not expired: expired.append(staged)
            return original(coord,body,**kwargs)
        with mock.patch.object(remote_service,'dispatch',side_effect=dispatch), \
             mock.patch.object(core,'_now',side_effect=lambda: deadline+1 if expired else original_now()):
            rc,result=self.invoke(*args)
        self.assertEqual(len(expired),1,'the intended actual server capability expiry was not reached')
        self.assertNotEqual(self.field(result,'.exit_code'),'0')
        self.assertFalse(output.exists())
        self.assertEqual(list(self.output_dir.glob('.agent-chat-snapshot-*')),[])
        outcome=self.command('exact-file-result','--path',str(self.source),'--reservation-id',rid)
        self.assertNotEqual(self.field(outcome,'.state'),'cleared')
        print(f'capability_expiry_phase={phase} staged_bytes={expired[0]} final_exists=false')

    def test_expired_capability_before_source_open(self): self.expired_capability('before_open')
    def test_expired_capability_during_stream(self): self.expired_capability('during_stream')
    def test_expired_capability_before_publication(self): self.expired_capability('before_publish')


class SnapshotStreamBoundaries(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory(prefix='agent-chat-snapshot-fault-',dir='/private/tmp')
        self.addCleanup(tmp.cleanup); self.root=Path(tmp.name)
        self.source=self.root/'input'; self.source.write_bytes(b'x'*(4*65536))
        self.output=self.root/'output'

    def assert_no_staging(self):
        self.assertEqual(list(self.root.glob('.agent-chat-snapshot-*')),[])

    def test_partial_read_error_cleans_only_owned_staging(self):
        read=os.read; count=[]
        sentinel=self.root/'unrelated'; sentinel.write_bytes(b'keep')
        def fail(fd,n):
            if count: raise OSError('injected private source read error')
            data=read(fd,n); count.append(len(data)); return data
        with mock.patch.object(exact_file_read.os,'read',side_effect=fail), self.assertRaises(core.CoordError):
            exact_file_read.snapshot(str(self.source),str(self.output),self.source.stat().st_size)
        self.assertEqual(count,[65536]); self.assertFalse(self.output.exists())
        self.assertEqual(sentinel.read_bytes(),b'keep'); self.assert_no_staging()

    def test_existing_request_limit_and_no_clobber_stay_enforced(self):
        for limit in (1024,64*1024*1024+1):
            with self.subTest(limit=limit),self.assertRaises(core.CoordError):
                exact_file_read.snapshot(str(self.source),str(self.output),limit)
            self.assertFalse(self.output.exists()); self.assert_no_staging()
        self.output.write_bytes(b'existing')
        with self.assertRaises(core.CoordError):
            exact_file_read.snapshot(str(self.source),str(self.output),self.source.stat().st_size)
        self.assertEqual(self.output.read_bytes(),b'existing'); self.assert_no_staging()

    def test_source_replacement_during_final_capability_check_rejects(self):
        read=os.read; eof=[]
        def observe(fd,n):
            data=read(fd,n)
            if not data: eof.append(True)
            return data
        def validate():
            if eof:
                other=self.root/'replacement'; other.write_bytes(b'changed'); other.replace(self.source)
        with mock.patch.object(exact_file_read.os,'read',side_effect=observe),self.assertRaises(core.CoordError):
            exact_file_read.snapshot(str(self.source),str(self.output),self.source.stat().st_size,validate=validate)
        self.assertFalse(self.output.exists()); self.assert_no_staging()

    def test_failure_after_publication_preserves_actual_output(self):
        fsync=os.fsync; calls=[]
        def fail_directory(fd):
            calls.append(fd)
            if len(calls)==2: raise OSError('injected post-publication directory sync failure')
            return fsync(fd)
        with mock.patch.object(exact_file_read.os,'fsync',side_effect=fail_directory),self.assertRaises(core.CoordError):
            exact_file_read.snapshot(str(self.source),str(self.output),self.source.stat().st_size)
        self.assertEqual(self.output.read_bytes(),self.source.read_bytes()); self.assert_no_staging()


def load_tests(loader, tests, pattern):
    names=['test_small_complete_copy_keeps_missing_completion_proof_pending',
           'test_above_eight_mib_copy_has_exact_bytes_and_hash',
           'test_explicit_sixty_four_mib_bound_has_bounded_child_memory',
           'test_expired_capability_before_source_open','test_expired_capability_during_stream',
           'test_expired_capability_before_publication']
    return unittest.TestSuite([*[SnapshotCopyWorkflow(name) for name in names],
                              loader.loadTestsFromTestCase(SnapshotStreamBoundaries)])
