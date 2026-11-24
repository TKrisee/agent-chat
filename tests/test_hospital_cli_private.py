"""Private HTTP/CLI workflow; synthetic DB, identities, filesystem and credentials."""
import os, shutil, sqlite3, subprocess, sys, threading, unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from agent_chat.core import Coordinator
from agent_chat import remote_service, hospital_vendor_scope as s, hospital_vendor_binding as binding, capacity_admission as capacity
from tests.test_hospital_binding_private import ServerFlowTests

def jq(program,raw=b'',args=()):
 return subprocess.run(['jq',*args,program],input=raw,capture_output=True,check=True,timeout=5).stdout

def get_request(raw):
 expr='[.op,.session,.host_id,(.params.resource//""),(.params.run_id//""),(.params.token//""),(.params.pid//""|tostring),(.params.evidence_sha256//""),(.params.control_rid//""),(.params.control_token//""),(.params.run_token//""),(.params.output_resource//""),(.params.output_rid//""),(.params.output_token//""),(.params.output_run_id//""),(.params.output_path//""),(.params.operation_id//""),(.params.digest//""),(.params.byte_count//""|tostring)] | @tsv'
 fields=jq(expr,raw,args=('-r',)).decode().rstrip('\n').split('\t'); fields+=['']*(19-len(fields))
 op,session,host=fields[:3]; p={}
 if op in ('begin-guard-batch','hospital-vendor-begin'):
  lines=jq('.params.tuples[] | [.resource,.reservation_id,.session_id,.host_id,.token] | @tsv',raw,args=('-r',)).decode().splitlines()
  p={'tuples':[dict(zip(('resource','reservation_id','session_id','host_id','token'),line.split('\t'))) for line in lines]}
  if op=='begin-guard-batch': p['restore']=False
  else: p.update(resource=fields[3],control_rid=fields[8],control_token=fields[9],output_path=fields[15])
 elif op=='attach-guard-pid': p={'run_id':fields[4],'token':fields[5],'pid':int(fields[6])}
 elif op=='guard-pulse': p={'run_id':fields[4],'token':fields[5]}
 elif op=='close-guard': p={'run_id':fields[4],'token':fields[5],'evidence_sha256':fields[7]}
 elif op=='inbox': p={}
 elif op.startswith('hospital-vendor-'):
  p=dict(resource=fields[3],control_rid=fields[8],control_token=fields[9],run_id=fields[4],run_token=fields[10],output_resource=fields[11],output_rid=fields[12],output_token=fields[13],output_run_id=fields[14],output_path=fields[15])
  if op!='hospital-vendor-start': p['operation_id']=fields[16]
  if op=='hospital-vendor-finish': p.update(digest=fields[17],byte_count=int(fields[18]))
 else: raise ValueError('unexpected fixture op')
 return {'op':op,'session':session,'host_id':host,'params':p}

def response_bytes(op,result):
 if op in ('begin-guard-batch','hospital-vendor-begin'):
  lines=''.join('\t'.join(str(x[k]) for k in ('resource','reservation_id','run_id','host_id'))+'\n' for x in result['admissions'])
  return jq('[inputs | split("\\t") | {resource:.[0],reservation_id:.[1],run_id:.[2],host_id:.[3]}] | {batch_id:$batch,admissions:.}',lines.encode(),args=('-Rn','--arg','batch',result['batch_id']))
 if op=='inbox':
  if result['messages']: raise ValueError('unexpected fixture messages')
  return b'{"messages":[]}'
 if op.startswith('hospital-vendor-'):
  return jq('{state:$state,resource:$resource,operation_id:$operation,binding_id:$binding}',args=('-n','--arg','state',str(result.get('state','')),'--arg','resource',str(result.get('resource','')),'--arg','operation',str(result.get('operation_id','')),'--arg','binding',str(result.get('binding_id',''))))
 return jq('{run_id:$run,attached:true,closed:true}',args=('-n','--arg','run',str(result.get('run_id',''))))

class CLIFlowTests(ServerFlowTests):
 def _workflow(self, interruption=None):
  self.prepare_server(); master=Path(self.tmp.name)/'project.sqlite3'; owner=self
  class Handler(BaseHTTPRequestHandler):
   def log_message(self,*args): pass
   def do_POST(self):
    try:
     if self.path!='/api/coord' or self.headers.get('Authorization')!='Bearer private-fixture-api': raise ValueError('fixture auth')
     length=int(self.headers['Content-Length']); request=get_request(self.rfile.read(length))
     coord=Coordinator(master)
     try:
      if request['op']=='hospital-vendor-pulse':
       owner.pulses+=1
       if interruption in ('stop','revoke') and owner.pulses==4:
        if interruption=='stop':
         coord.db.execute('CREATE TABLE IF NOT EXISTS agent_controls(target_session TEXT PRIMARY KEY,mode TEXT)')
         coord.db.execute('INSERT INTO agent_controls VALUES(?,?)',(s.CONSUMER,'stopped'))
        else:
         registry=owner.roots.projects.path; st=registry.lstat(); identity=(st.st_dev,st.st_ino,st.st_uid,st.st_mode)
         with capacity.registry_authority_gate(registry,os.getuid(),identity) as (gate,check):
          coord.db.execute('BEGIN IMMEDIATE')
          has_selection=coord.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='hospital_vendor_selection'").fetchone()
          selected=coord.db.execute('SELECT id FROM hospital_vendor_selection').fetchone() if has_selection else None
          binding.revoke_in_transaction(coord.db,gate,'root_revoked',selected[0] if selected else binding.BINDING); check(); coord.db.commit()
      result=remote_service.dispatch(coord,request,canonical_project='default',roots=owner.roots)
     finally: coord.close()
     if ((interruption=='lost' and request['op']=='hospital-vendor-finish')
         or (interruption=='lost-start' and request['op']=='hospital-vendor-start')):
      self.connection.shutdown(2); self.connection.close(); return
     data=response_bytes(request['op'],result); self.send_response(200)
    except Exception as exc:
     owner.errors.append(type(exc).__name__+': '+str(exc)); data=b'{"error":"fixture refused"}'; self.send_response(400)
    self.send_header('Content-Type','application/json'); self.send_header('Content-Length',str(len(data))); self.end_headers(); self.wfile.write(data)
  self.errors=[]; self.pulses=0; http=ThreadingHTTPServer(('127.0.0.1',0),Handler)
  thread=threading.Thread(target=http.serve_forever,kwargs={'poll_interval':0.01}); thread.start()
  def stop(): http.shutdown(); thread.join(timeout=3); http.server_close(); self.assertFalse(thread.is_alive())
  self.addCleanup(stop)
  url='http://127.0.0.1:'+str(http.server_port)
  control=self.call('request',{'resource':s.CONTROL,'minutes':60}); run=self.call('request',{'resource':s.RUN_RESOURCE,'minutes':60})
  project=Path(self.tmp.name)/'main'; project.mkdir(mode=0o700)
  outroot=Path(self.tmp.name)/'external'; outroot.mkdir(mode=0o700); output=outroot/'copy'
  out=self.call('request',{'resource':str(outroot),'minutes':60})
  resource=next(iter(self.manifest)); src=project/resource[5:]; src.parent.mkdir(parents=True); payload=b'private-vendor-fixture-'*(400000); src.write_bytes(payload)
  runtime=Path(self.tmp.name)/'runtime'; shutil.copytree(Path(__file__).resolve().parents[1]/'src',runtime/'src')
  cli=runtime/'src/agent_chat/cli.py'; text=cli.read_text(); old="('http://127.0.0.1:8765',vendor_scope.PROJECT"; self.assertEqual(text.count(old),1); cli.write_text(text.replace(old,"('"+url+"',vendor_scope.PROJECT"))
  service=runtime/'src/agent_chat/service.py'
  with service.open('a') as f: f.write('\nINSTALLED_SERVER_CONFIG = InstalledServerConfig(plist=Path("/fixture"),wrapper=Path("/fixture"),executable=Path("/fixture"),master=Path('+repr(str(master))+'),working_directory=Path('+repr(str(project))+'),uid='+str(os.getuid())+',host="127.0.0.1",port='+str(http.server_port)+')\n')
  caps=Path(self.tmp.name)/'caps'; caps.mkdir(mode=0o700)
  def capability(name,result):
   path=caps/name
   args=['-n','--arg','resource',result['resource'],'--arg','sid',s.CONSUMER,'--arg','host',s.HOST,'--arg','rid',result['reservation_id'],'--arg','token',result['token'],'--arg','server',url,'--arg','project','default']
   path.write_bytes(jq('{resource:$resource,session_id:$sid,host_id:$host,reservation_id:$rid,token:$token,server:$server,project:$project}',args=args)); path.chmod(0o600); return str(path)
  ccap=capability('control.json',control); rcap=capability('run.json',run); ocap=capability('output.json',out)
  evidence=str(caps/'process.json'); env=dict(os.environ,AGENT_CHAT_HOST_ID=s.HOST,AGENT_CHAT_ROOT=str(project)); env.pop('AGENT_CHAT_TOKEN',None)
  bootstrap='import sys;sys.path.insert(0,sys.argv.pop(1));from agent_chat.cli import main;sys.exit(main())'
  command=[sys.executable,'-I','-c',bootstrap,str(runtime/'src'),'--session',s.CONSUMER,'--server',url,'--api-token','private-fixture-api','--project','default','hospital-vendor-copy','--resource',resource,'--output',str(output),'--control-capability-file',ccap,'--run-capability-file',rcap,'--output-capability-file',ocap,'--process-evidence-file',evidence]
  proc=subprocess.run(command,env=env,capture_output=True,text=True,timeout=20)
  self.assertGreater(len(payload),8*1024*1024)
  if interruption in ('lost','lost-start'):
   if interruption=='lost-start':
    self.assertNotEqual(proc.returncode,0); self.assertFalse(output.exists())
    self.assertEqual([x[0] for x in self.db.execute('SELECT state FROM hospital_vendor_file_events WHERE resource=? ORDER BY seq',(resource,))],['started'])
    retry=list(command); retry[-1]=str(caps/'second-process.json')
    again=subprocess.run(retry,env=env,capture_output=True,text=True,timeout=20)
    self.assertNotEqual(again.returncode,0); self.assertFalse(output.exists())
    self.assertEqual([x[0] for x in self.db.execute('SELECT state FROM hospital_vendor_file_events WHERE resource=? ORDER BY seq',(resource,))],['started'])
    self.assertEqual(self.db.execute('SELECT COUNT(*) FROM remote_guarded_runs WHERE closed_at IS NULL').fetchone()[0],0)
    return
   self.assertNotEqual(proc.returncode,0); self.assertEqual(output.read_bytes(),payload)
   self.assertEqual([x[0] for x in self.db.execute('SELECT state FROM hospital_vendor_file_events WHERE resource=? ORDER BY seq',(resource,))],['started','unverified'])
   self.assertEqual(self.db.execute('SELECT COUNT(*) FROM remote_guarded_runs WHERE closed_at IS NULL').fetchone()[0],0)
   retry=list(command); retry[-1]=str(caps/'second-process.json')
   again=subprocess.run(retry,env=env,capture_output=True,text=True,timeout=20)
   self.assertNotEqual(again.returncode,0); self.assertEqual(output.read_bytes(),payload)
   self.assertEqual([x[0] for x in self.db.execute('SELECT state FROM hospital_vendor_file_events WHERE resource=? ORDER BY seq',(resource,))],['started','unverified'])
   self.assertEqual(self.db.execute('SELECT COUNT(*) FROM remote_guarded_runs WHERE closed_at IS NULL').fetchone()[0],0)
   return
  if interruption:
   self.assertNotEqual(proc.returncode,0); self.assertFalse(output.exists()); self.assertEqual(list(outroot.iterdir()),[])
   self.assertEqual([x[0] for x in self.db.execute('SELECT state FROM hospital_vendor_file_events WHERE resource=? ORDER BY seq',(resource,))],['started'])
   self.assertEqual(self.db.execute('SELECT COUNT(*) FROM remote_guarded_runs WHERE closed_at IS NULL').fetchone()[0],0)
   self.assertEqual(self.db.execute('SELECT COUNT(*) FROM resources WHERE owner_session=? AND stale=1',(s.OWNER,)).fetchone()[0],108)
   return
  self.assertEqual(proc.returncode,0,(proc.stdout,proc.stderr,self.errors)); self.assertEqual(output.read_bytes(),payload)
  self.assertEqual(src.read_bytes(),payload)
  events=self.db.execute("SELECT state FROM hospital_vendor_file_events WHERE resource=? ORDER BY seq",(resource,)).fetchall()
  self.assertEqual([x[0] for x in events],['started','unverified']); self.assertEqual(self.db.execute('SELECT COUNT(*) FROM remote_guarded_runs WHERE closed_at IS NULL').fetchone()[0],0)
  self.assertEqual(self.db.execute('SELECT COUNT(*) FROM resources WHERE owner_session=? AND stale=1',(s.OWNER,)).fetchone()[0],108)
 def test_real_cli_guarded_fixed_read_copy(self): self._workflow()
 def test_root_revocation_during_stream_cleans_staging(self): self._workflow('revoke')
 def test_task_stop_during_stream_cleans_staging(self): self._workflow('stop')
 def test_lost_finish_response_stays_unverified(self): self._workflow('lost')
 def test_lost_start_response_blocks_replay(self): self._workflow('lost-start')
if __name__=='__main__': unittest.main()
