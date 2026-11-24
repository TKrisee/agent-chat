import base64
import hashlib
import time
from unittest import mock
import concurrent.futures
import importlib.abc
import os
from pathlib import Path
import plistlib
import sqlite3
import subprocess
import sys
import tempfile
import types
import unittest
import uuid

UNIT = Path(__file__).resolve().parent
CANONICAL = UNIT.parent / 'src' / 'agent_chat'
# Test-only origin fence. Optional run manifest binds this execution to exact
# current bytes; ordinary unittest use still requires the canonical checkout.
import atexit
import importlib.machinery
CANONICAL_MODULES = (
    'core','service','rpc','agent_control','projects','remote_service','bridge_state',
    'root_orchestrator','lifecycle_producer','processes','receipts','guard_generation',
    'app_logging','session_reset','root_lifecycle','capacity_recovery','compaction',
    'management','root_restart','root_team_reset','root_owner','communication',
    'root_local','control_worker','native_admission_intent','worker_admission_intent',
    # Mandatory private Coordinator schema initialization, not admission calls.
    'capacity_admission','successor_admission',
    # Current Root active-state guard, including its import-time evidence types.
    'root_admin_attachment','native_owner_evidence',
    # Ordinary request dispatch compares this module's vendor-only resource ID.
    'hospital_vendor_scope',
    # Imported by successor resource discrimination; no observer is invoked.
    'capacity_observer','guard_witness')
ORIGINS = {'agent_chat': CANONICAL / '__init__.py', **{
    'agent_chat.'+name: CANONICAL / (name+'.py') for name in CANONICAL_MODULES}}
PINS = {}
manifest = os.environ.get('AGENT_CHAT_INTENT_IMPORT_MANIFEST')
if manifest:
    raw = Path(manifest).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == os.environ['AGENT_CHAT_INTENT_IMPORT_MANIFEST_SHA']
    for line in raw.decode().splitlines():
        name, filename, digest, mode = line.split('\t')
        assert name not in PINS
        PINS[name] = (Path(filename), digest, mode)
    assert set(PINS) == set(ORIGINS)
    for name, filename in ORIGINS.items():
        assert PINS[name][0] == filename.resolve()

class Fence(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'agent_chat' or fullname.startswith('agent_chat.'):
            if fullname not in ORIGINS:
                raise ImportError('outside canonical intent workflow import fence: '+fullname)
            spec = importlib.machinery.PathFinder.find_spec(fullname,path)
            if spec is None or Path(spec.origin).resolve() != ORIGINS[fullname].resolve():
                raise ImportError('noncanonical intent workflow import: '+fullname)
            if PINS:
                filename,digest,mode=PINS[fullname]
                assert hashlib.sha256(filename.read_bytes()).hexdigest()==digest
                assert format(filename.stat().st_mode & 0o777,'o')==mode
            return spec

def record_loaded_origins():
    folder=os.environ.get('AGENT_CHAT_INTENT_IMPORT_LOG_DIR')
    if not folder: return
    rows=[]
    for name, expected in sorted(ORIGINS.items()):
        module=sys.modules.get(name)
        if module is not None:
            actual=Path(module.__file__).resolve()
            assert actual==expected.resolve()
            digest=hashlib.sha256(actual.read_bytes()).hexdigest()
            if PINS: assert digest==PINS[name][1]
            rows.append('\t'.join((name,str(actual),digest,format(actual.stat().st_mode & 0o777,'o'))))
    target=Path(folder)/('loaded-'+str(os.getpid())+'.tsv')
    with target.open('x') as stream: stream.write('\n'.join(rows)+'\n')
atexit.register(record_loaded_origins)
_import_fence = Fence()
sys.meta_path.insert(0, _import_fence)
try:
    import agent_chat
    from agent_chat.core import Coordinator, CoordError
    from agent_chat.projects import Projects
    from agent_chat.remote_service import dispatch
    from agent_chat.bridge_state import BridgeState
    from agent_chat.agent_control import AgentControlState
    from agent_chat.control_worker import ControlWorker
    from agent_chat.root_orchestrator import RootOrchestrator
    from agent_chat import root_local, service, lifecycle_producer
finally:
    sys.meta_path.remove(_import_fence)
assert Path(root_local.__file__).resolve().parent == CANONICAL.resolve()
assert Path(Coordinator.__module__.replace('.', '/')).name == 'core'


def uid(): return str(uuid.uuid4())

class Provider:
    """Controlled native response boundary; never an OS grant or real provider."""
    def __init__(self, thread, cwd):
        self.thread, self.cwd = thread, str(cwd)
        self.provider = 'isolated-provider'
    def request(self, method, params):
        if params['threadId'] != self.thread:
            raise AssertionError('wrong native thread')
        if method == 'thread/read':
            return {'thread':{'id':self.thread,'status':{'type':'idle'},'canAcceptDirectInput':True}}
        if method != 'thread/resume' or set(params) != {'threadId','excludeTurns'}:
            raise AssertionError('unexpected native operation')
        value = {'thread':{'id':self.thread},'model':'isolated-model','reasoningEffort':'high',
            'cwd':self.cwd,'approvalPolicy':'on-request','approvalsReviewer':'user',
            'activePermissionProfile':None,'runtimeWorkspaceRoots':[self.cwd],
            'sandbox':{'type':'workspaceWrite','networkAccess':False,'excludeTmpdirEnvVar':True,
                       'excludeSlashTmp':True,'writableRoots':[self.cwd]}}
        if self.provider is not None: value['modelProvider'] = self.provider
        return value

class Flow(unittest.TestCase):
    def setUp(self):
        fence = Fence()
        sys.meta_path.insert(0, fence)
        self.addCleanup(sys.meta_path.remove, fence)
        self.temp = tempfile.TemporaryDirectory(prefix='intent-adapter-')
        self.base = Path(self.temp.name).resolve()
        self.master = self.base/'state.sqlite3'
        self.projects = Projects(self.master)
        self.host = 'host-isolated'
        self.root_sid = 'session-root-isolated'
        self.connections = []
        self.neutral = self.base/'neutral'; self.neutral.mkdir()
        self.project_dir = self.base/'project'; self.project_dir.mkdir()
        self.worker_dir = self.base/'existing-worker'; self.worker_dir.mkdir()
        self.root_coord, self.root_thread, _ = self.register('default',self.root_sid,'root',self.neutral)
        self.register('default','session-notice-recipient','operator',self.neutral)
        self.root = RootOrchestrator(self.projects)
        self.root_coord.context()
        self.root.enroll('default',self.root_sid,self.host,self.root_thread,True)
        project = self.projects.create('Isolated target',str(self.project_dir),'isolated task')
        self.project = project['id']
        self.po, po_thread, _ = self.register(self.project,'session-po','po',self.project_dir)
        self.root_coord.context()
        self.root.adopt('default',self.root_sid,self.host,self.project,'session-po',po_thread,str(self.project_dir),True)
        self.worker, self.thread, self.provider = self.register(self.project,'session-worker','worker',self.worker_dir)
        self.root_coord.context()
        self.root.delegate_management('default',self.root_sid,self.host,self.project,'session-po','session-worker',self.thread,self.host,uid(),True)
        repo = self.base/'repo'
        self.git('init','-q',str(repo))
        self.git('-C',str(repo),'-c','user.name=Test','-c','user.email=test@invalid','commit','--allow-empty','-qm','fixture')
        self.worktree = self.base/'independent'
        self.git('-C',str(repo),'worktree','add','--detach',str(self.worktree),'HEAD')
        gitdir = (self.worktree/'.git').read_text().strip().removeprefix('gitdir: ')
        self.credentials = self.base/'private'; self.credentials.mkdir(mode=0o700)
        self.cap = self.credentials/'producer.cap'
        self.producer = lifecycle_producer.enroll(self.worker,'session-worker',self.host,self.cap,
            current_scope={'workspace':'workspace:'+str(self.worktree), 'leaf_prefix':'file:encounter/',
                'gitdir':gitdir,'index_resource':'file:.git/worktrees/independent/index'})
        self.config = service.INSTALLED_SERVER_CONFIG
        plist = self.base/'server.plist'; wrapper = self.base/'server-wrapper'; executable = self.base/'server'
        executable.write_text('# isolated fixture executable\n')
        wrapper.write_text('exec '+str(executable)+' --db '+str(self.master)+' --host 127.0.0.1 --port 9999\n')
        with plist.open('wb') as stream:
            plistlib.dump({'Label':'com.agent-chat.server','AssociatedBundleIdentifiers':['com.agent-chat.server'],
                'WorkingDirectory':str(self.neutral),'RunAtLoad':True,'KeepAlive':True,'ThrottleInterval':10,
                'ProgramArguments':[str(wrapper)],'EnvironmentVariables':{}},stream)
        service.INSTALLED_SERVER_CONFIG = service.InstalledServerConfig(plist,wrapper,executable,self.master,self.neutral,os.getuid(),'127.0.0.1',9999)
        self.params = dict(project=self.project,target='session-worker',expected_thread=self.thread,
            expected_host=self.host,request_id=uid(),producer_id=self.producer,producer_credential=str(self.cap),
            expected_root=str(self.worktree),canonical_resource='native-unity:isolated-encounter',confirm=True)

    def intent_count(self):
        with self.projects.connection() as db:
            return db.execute('SELECT COUNT(*) FROM worker_admission_intents').fetchone()[0]

    def git(self,*args):
        # This repository is inert fixture data; never inherit user Git hooks,
        # templates, signing or a caller's selected repository/index.
        environment = {key:value for key,value in os.environ.items() if not key.startswith('GIT_')}
        template = self.base/'git-template'; template.mkdir(exist_ok=True)
        environment.update(GIT_CONFIG_NOSYSTEM='1', GIT_CONFIG_GLOBAL=os.devnull,
                           GIT_CONFIG_SYSTEM=os.devnull, GIT_TEMPLATE_DIR=str(template))
        result = subprocess.run(['git',*args],text=True,capture_output=True,cwd=self.base,env=environment)
        if result.returncode: raise AssertionError(result.stderr)
        return result.stdout.strip()

    def register(self, project, sid, label, cwd):
        coord = Coordinator(self.projects.db_path(project),sid)
        self.connections.append(coord)
        dispatch(coord,{'op':'register','session':sid,'host_id':self.host,'params':{'agent':label}})
        thread = uid()
        BridgeState(coord).bind(thread)
        provider = Provider(thread,cwd)
        self.observe(coord,thread,provider)
        return coord, thread, provider

    def observe(self,coord,thread,provider):
        coord.context()
        state = AgentControlState(coord)
        job = state.request(coord.session,thread,'inspect',uid(),False)
        bridge = types.SimpleNamespace(state=state,rpc=provider,queue=lambda thread: [])
        observation, *_ = ControlWorker(bridge).observe(job)
        state.update(job['id'],'pending','completed',observation=observation)

    def call(self, changes=None, actor=None):
        params = dict(self.params); params.update(changes or {})
        return root_local.execute('worker-admission-intent',params,master_db=self.master,
            service_plist=service.INSTALLED_SERVER_CONFIG.plist,home_project='default',
            session=actor or self.root_sid,host_id=self.host)

    def tearDown(self):
        if hasattr(self,'config'): service.INSTALLED_SERVER_CONFIG = self.config
        for coord in self.connections: coord.close()
        self.temp.cleanup()

    def test_positive_duplicate_concurrent_restart(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            values = list(pool.map(lambda _:self.call(),range(4)))
        self.assertEqual(sum(not v['duplicate'] for v in values),1)
        self.assertTrue(all(v['status']=='intent_only' and v['runtime_authorized'] is False for v in values))
        self.assertEqual(len({v['digest'] for v in values}),1)
        # Every call uses a fresh SQLite connection, including this replay.
        self.assertTrue(self.call()['duplicate'])
        self.assertEqual(self.intent_count(),1)

    def test_wrong_identity_and_scope(self):
        with self.assertRaises(CoordError): self.call(actor='session-po')
        for change in ({'project':'default'},{'target':'session-po'}, {'expected_thread':uid()},
                       {'expected_host':'wrong-host'},{'expected_root':str(self.worker_dir)}):
            with self.subTest(change=change), self.assertRaises(CoordError): self.call(change)

    def test_missing_provider_and_stale_settings(self):
        self.provider.provider = None
        self.observe(self.worker,self.thread,self.provider)
        with self.assertRaises(CoordError): self.call()
        self.provider.provider = 'isolated-provider'
        self.observe(self.worker,self.thread,self.provider)
        self.call()
        self.provider.provider = 'changed-provider'
        self.observe(self.worker,self.thread,self.provider)
        with self.assertRaises(CoordError): self.call()

    def test_physical_and_producer_proof(self):
        with self.assertRaises(CoordError): self.call({'producer_id':'missing'})
        self.cap.write_text('wrong-secret\n')
        with self.assertRaises(CoordError): self.call()

    def test_missing_physical(self):
        (self.worktree/'.git').unlink()
        with self.assertRaises(CoordError): self.call()

    def test_rebind_invalidates_retry(self):
        self.call()
        BridgeState(self.worker).bind(uid())
        with self.assertRaises(CoordError): self.call()


    def test_fresh_process_persistence(self):
        result = self.call()
        config = service.INSTALLED_SERVER_CONFIG
        code = f'''# PRIVATE_INTENT_REPLAY
import os, sys, sqlite3
from pathlib import Path
from urllib.parse import unquote, urlsplit
private = Path({str(self.base)!r}).resolve()
source = Path({str(UNIT.parent)!r}).resolve()
home = Path.home().resolve()
sys.path[:0] = [str(source/'src'), str(source/'tests')]
def audit(event, args):
    if event in ('socket.connect', 'socket.bind', 'os.system'):
        raise AssertionError('native/network effects forbidden in intent fixture')
    if event == 'subprocess.Popen' and not (isinstance(args[1], list) and len(args[1]) == 3 and args[1][:2] == ['jq', '-er']):
        raise AssertionError('only fixed fixture stdin parser permitted')
    if event == 'sqlite3.connect':
        raw = os.fsdecode(args[0]); uri = urlsplit(raw) if raw.startswith('file:') else None
        if uri and uri.netloc: raise AssertionError('SQLite URI authority forbidden')
        path = Path(unquote(uri.path) if uri else raw).resolve()
        if not path.is_relative_to(private): raise AssertionError('SQLite outside fixture')
    if event == 'open' and isinstance(args[0], (str, bytes, os.PathLike)):
        path = Path(os.fsdecode(args[0])).resolve()
        if path.is_relative_to(home) and not any(path.is_relative_to(root) for root in (source/'src', source/'tests', private)):
            raise AssertionError('nonfixture home access forbidden')
sys.addaudithook(audit)
from test_worker_admission_intent import service, root_local, Path, Fence
from agent_chat import core
def default_forbidden(): raise AssertionError('default DB discovery forbidden')
core.default_db = default_forbidden
def private_config():
    config = service.INSTALLED_SERVER_CONFIG
    if not all(Path(getattr(config, name)).resolve().is_relative_to(private) for name in ('plist','wrapper','executable','master','working_directory')):
        raise AssertionError('installed discovery outside fixture forbidden')
    return config
service._installed_server_config = root_local._installed_server_config = private_config
'''
        code += "service.INSTALLED_SERVER_CONFIG=service.InstalledServerConfig(" + ','.join(
            "Path("+repr(str(value))+")" if isinstance(value,Path) else repr(value)
            for value in (config.plist,config.wrapper,config.executable,config.master,config.working_directory,config.uid,config.host,config.port)) + ");\n"
        code += "fence=Fence(); sys.meta_path.insert(0,fence)\ntry:\n"
        code += "    try:\n        import agent_chat.cli\n    except ImportError:\n        pass\n    else:\n        raise AssertionError('child import fence did not reject unrelated CLI')\n"
        code += "    value=root_local.execute('worker-admission-intent',"+repr(self.params)+",master_db=Path("+repr(str(self.master))+"),service_plist=service.INSTALLED_SERVER_CONFIG.plist,home_project='default',session="+repr(self.root_sid)+",host_id="+repr(self.host)+")\nfinally:\n    sys.meta_path.remove(fence)\n"
        code += "assert value['duplicate'] and value['digest']=="+repr(result['digest'])+" and value['runtime_authorized'] is False; print('fresh process persisted intent')"
        environment = {key:value for key,value in os.environ.items()
                       if not key.startswith('AGENT_CHAT_') or key.startswith('AGENT_CHAT_INTENT_')}
        child = subprocess.Popen([sys.executable,'-I','-S','-B','-c',code],cwd=self.base,
                                 env=environment,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        out, error = child.communicate(timeout=15)
        print('fresh_process_pid',child.pid,'exit',child.returncode,out.strip(),flush=True)
        self.assertEqual(child.returncode,0,error)

    def test_expired_observation(self):
        from agent_chat import worker_admission_intent
        stamp = time.time() + 121
        with mock.patch.object(worker_admission_intent.time,'time',return_value=stamp):
            with self.assertRaisesRegex(CoordError,'fresh idle'): self.call()

    def test_changed_request_conflict(self):
        self.call()
        with self.assertRaisesRegex(CoordError,'conflicts'):
            self.call({'canonical_resource':'native-unity:different'})
        self.assertEqual(self.intent_count(),1)

    def test_cross_project(self):
        other = self.projects.create('Second project',str(self.base/'other'),'separate task')
        with self.assertRaises(CoordError): self.call({'project':other['id']})

    def test_real_guard_then_ordinary_return(self):
        def invoke(op, params):
            return dispatch(self.worker,dict(op=op,session='session-worker',host_id=self.host,params=params))
        self.worker.context()
        claim = invoke('request',dict(resource='file:intent-fixture',minutes=5))
        token = claim['token']
        guard = invoke('begin-guard',dict(resource='file:intent-fixture',token=token,reservation_id=claim['reservation_id']))
        with self.assertRaises(CoordError): self.call()
        evidence = b'Isolated guard admitted but never attached a PID or launched a job; exact guard closed before ordinary no-job return.'
        invoke('close-guard',dict(run_id=guard['run_id'],token=token,evidence_sha256=hashlib.sha256(evidence).hexdigest()))
        receipt = dict(version=1,resource='file:intent-fixture',reservation_id=claim['reservation_id'],restored=True,processes_closed=True,closed_at=time.time(),evidence='isolated-no-job-report',pids=[])
        released = invoke('release',dict(resource='file:intent-fixture',token=token,receipt=receipt,evidence_base64=base64.b64encode(evidence).decode()))
        self.assertTrue(released['released'])
        self.assertEqual(self.call()['status'],'intent_only')


    def test_settings_wrong_native_thread_never_persisted(self):
        original = self.provider.request
        def wrong(method, params):
            value = original(method,params)
            if method == 'thread/resume': value['thread']['id'] = uid()
            return value
        self.provider.request = wrong
        before = tuple(self.worker.db.execute('SELECT * FROM agent_control_observations WHERE target_session=?',('session-worker',)).fetchone())
        with self.assertRaisesRegex(CoordError,'different thread'):
            self.observe(self.worker,self.thread,self.provider)
        after = tuple(self.worker.db.execute('SELECT * FROM agent_control_observations WHERE target_session=?',('session-worker',)).fetchone())
        self.assertEqual(before,after)

    def test_changed_index_invalidates_retry(self):
        self.call()
        gitdir = Path((self.worktree/'.git').read_text().strip().removeprefix('gitdir: '))
        with (gitdir/'index').open('ab') as stream: stream.write(b'changed')
        with self.assertRaisesRegex(CoordError,'conflicts'): self.call()

    def test_request_identity_is_root_global(self):
        self.call()
        folder = self.base/'second'; folder.mkdir()
        other = self.projects.create('Other managed',str(folder),'isolated task')
        po, po_thread, _ = self.register(other['id'],'session-po2','po2',folder)
        self.root_coord.context()
        self.root.adopt('default',self.root_sid,self.host,other['id'],'session-po2',po_thread,str(folder),True)
        worker, thread, _ = self.register(other['id'],'session-worker2','worker2',folder)
        self.root_coord.context()
        self.root.delegate_management('default',self.root_sid,self.host,other['id'],'session-po2','session-worker2',thread,self.host,uid(),True)
        gitdir = (self.worktree/'.git').read_text().strip().removeprefix('gitdir: ')
        cap = self.credentials/'second.cap'
        producer = lifecycle_producer.enroll(worker,'session-worker2',self.host,cap,
            current_scope={'workspace':'workspace:'+str(self.worktree),'leaf_prefix':'file:encounter/',
                'gitdir':gitdir,'index_resource':'file:.git/worktrees/independent/index'})
        with self.assertRaisesRegex(CoordError,'conflicts'):
            self.call({'project':other['id'],'target':'session-worker2','expected_thread':thread,
                       'producer_id':producer,'producer_credential':str(cap)})
        self.assertEqual(self.intent_count(),1)

if __name__ == '__main__': unittest.main(verbosity=2)
