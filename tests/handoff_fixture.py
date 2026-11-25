"""Private launchd fixture wiring; genuine production authority checks remain.
Only installation/home/Root-CLI fixtures and HTTP scheduling are substituted.
Native provider image, peer, process ownership, launchd and journal are real.
"""
import os,plistlib,pwd,sqlite3,time,subprocess
from pathlib import Path
from types import SimpleNamespace

def configure(root):
 root=Path(root).resolve();assert root.is_relative_to(Path('/private/tmp/provider-owning-handoff-v3/validation/tests'))
 data=plistlib.loads((root/'fixture.plist').read_bytes())
 from agent_chat import service,client_continuity
 from agent_chat import provider_recovery_lifecycle as life
 service.LABEL=data['label'];client_continuity.LABEL=data['label']
 if (root/'inject-reattach').exists():
  from agent_chat import provider_service,provider_owner
  actual_restore=provider_service.HandoffRecovery.restored_connections
  def wait_for_private_actor_exit(adapter):
   (root/'resume-receipt-ready').write_text('actual authenticated response is durable')
   old=adapter.original_root_cli[0];generation=(old[0],old[1],old[3],old[4]);deadline=time.monotonic()+5
   while not provider_owner.absent(generation):
    if time.monotonic()>=deadline:raise RuntimeError('private test actor did not exit')
    time.sleep(.02)
   return actual_restore(adapter)
  provider_service.HandoffRecovery.restored_connections=wait_for_private_actor_exit
 actual_request=life.Wire.request
 def recorded_request(wire,method,params):
  with (root/'rpc-methods.log').open('a') as stream:stream.write(method+'\n')
  return actual_request(wire,method,params)
 life.Wire.request=recorded_request

 real_manager=service.ServiceManager
 def private_run(command,**kwargs):
  if command==['launchctl','kickstart','-k','gui/'+str(os.getuid())+'/'+data['label']+'.server']:
   with (root/'recorded-server-restarts.txt').open('a') as stream:stream.write('recorded server-only restart request\n')
   return subprocess.CompletedProcess(command,0,'','')
  return subprocess.run(command,**kwargs)
 def manager(**kwargs):
  kwargs.setdefault('run',private_run)
  kwargs.setdefault('home',root);kwargs.setdefault('server_bin',str(root/'server'))
  kwargs.setdefault('client_bin',str(root/'client'));kwargs.setdefault('python',data['python'])
  return real_manager(**kwargs)
 service.ServiceManager=manager
 service.INSTALLED_SERVER_CONFIG=service.InstalledServerConfig(root/'server.plist',root/'server-wrapper',root/'server',root/'master.sqlite3',root,os.getuid(),'127.0.0.1',59998)
 if (root/'inject-gate-crash').exists():
  actual_recover=life.recover_uncertain
  def short_failure(*args,**kwargs):
   kwargs['timeout']=45;return actual_recover(*args,**kwargs)
  life.recover_uncertain=short_failure
 life.CLI_IMAGE=root/'codex';life.CLI_SHA=life.v2.digest((root/'codex').read_bytes())
 real_getpwuid=pwd.getpwuid
 def private_home(uid):
  value=list(real_getpwuid(uid));value[5]=str(root);return pwd.struct_passwd(value)
 pwd.getpwuid=private_home
 return manager,data

def client(root,argv):
 manager,data=configure(root)
 from agent_chat import bridge_client as bridge,approvals,provider_owner
 if (Path(root)/'inject-gate-crash').exists():
  def crash_before_receipt(ledger,child):
   (Path(root)/'crashed-gate-pid.txt').write_text(str(child.pid))
   (Path(root)/'inject-gate-crash').unlink()
   os._exit(71)
  provider_owner.LaunchLedger.launched=crash_before_receipt
 class Registry:
  token='synthetic-private-transport-token'
  def __init__(self,*a,**k):pass
  def call(self,*a,**k):return {'projects':[]}
 class Pump:
  def __init__(self,*a,**k):pass
  def start(self):return self
  def close(self):pass
 class Usage:
  def __init__(self,*a,**k):pass
  def check(self,rpc):
   try:
    rpc.connect()
    with sqlite3.connect(Path(root)/'master.sqlite3') as db:
     db.execute('UPDATE bridge_client_runtime SET pid=?,heartbeat=?,error=NULL',(os.getpid(),time.time()));db.commit()
    return {'blocked':False}
   except Exception:return {'blocked':True,'reason':'private provider not connected yet'}
 bridge.HttpClient=Registry;bridge.UsageGuard=Usage;bridge.host_id=lambda:'private-host';approvals.ApprovalPump=Pump
 return bridge.main(argv)
