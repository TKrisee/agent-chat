"""Fixed task-specific read-only executor; authorization comes from server pulses.
"""
import hashlib
import os
import secrets
import stat
from . import hospital_vendor_output as output_guard
from . import hospital_vendor_scope as scope
from . import exact_file_read as reader

CHUNK_BYTES=1024*1024

def _stamp(st):
    return (st.st_dev,st.st_ino,st.st_size,st.st_mtime_ns,st.st_ctime_ns,st.st_mode,st.st_uid,st.st_nlink)

def _same_output(root, output, parentfd, output_root):
    checkfd,_=output_guard.external_output_parent(root,output)
    try:
        a,b=os.fstat(parentfd),os.fstat(checkfd)
        if (a.st_dev,a.st_ino)!=(b.st_dev,b.st_ino): raise output_guard.Refused('output_parent_changed')
        output_guard.require_parent_within(checkfd,output_root)
    finally: os.close(checkfd)

def _copy_one(project_root, resource, output, output_root, pulse):
    """Internal filesystem operation; public entry binds its server pulse and fixed root.
    """
    if resource not in scope.manifest(): raise scope.Refused('resource_not_selected')
    if not isinstance(project_root,str) or not project_root.startswith('/') or project_root.endswith('/'):
        raise scope.Refused('root_invalid')
    source=project_root+'/'+resource[5:]
    pulse(); parentfd,name=output_guard.external_output_parent(project_root,output)
    fds=[]; fd=None; tmp=None; tmp_created=False
    try:
        output_guard.require_parent_within(parentfd,output_root)
        fds,fd=reader._walk(source)
        st=os.fstat(fd)
        if not stat.S_ISREG(st.st_mode) or st.st_nlink!=1 or st.st_uid!=os.getuid():
            raise scope.Refused('source_invalid')
        parents=[(os.fstat(x).st_dev,os.fstat(x).st_ino) for x in fds]
        pulse(); _same_output(project_root,output,parentfd,output_root)
        tmp='.hospital-vendor-'+secrets.token_hex(16)
        out=os.open(tmp,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600,dir_fd=parentfd)
        tmp_created=True; digest=hashlib.sha256(); count=0
        with os.fdopen(out,'wb') as stream:
            while True:
                pulse()
                chunk=os.read(fd,CHUNK_BYTES)
                if not chunk: break
                stream.write(chunk); digest.update(chunk); count+=len(chunk)
            stream.flush(); os.fsync(stream.fileno())
        if count!=st.st_size or _stamp(os.fstat(fd))!=_stamp(st): raise scope.Refused('source_changed')
        checkfds,checkfd=reader._walk(source)
        try:
            if ([(os.fstat(x).st_dev,os.fstat(x).st_ino) for x in checkfds]!=parents
                or _stamp(os.fstat(checkfd))!=_stamp(st)):
                raise scope.Refused('source_changed')
        finally:
            os.close(checkfd)
            for x in reversed(checkfds): os.close(x)
        pulse(); _same_output(project_root,output,parentfd,output_root)
        if _stamp(os.fstat(fd))!=_stamp(st): raise scope.Refused('source_changed')
        finalfds,finalfd=reader._walk(source)
        try:
            if ([(os.fstat(x).st_dev,os.fstat(x).st_ino) for x in finalfds]!=parents
                or _stamp(os.fstat(finalfd))!=_stamp(st)):
                raise scope.Refused('source_changed')
        finally:
            os.close(finalfd)
            for x in reversed(finalfds): os.close(x)
        os.link(tmp,name,src_dir_fd=parentfd,dst_dir_fd=parentfd,follow_symlinks=False)
        os.unlink(tmp,dir_fd=parentfd); tmp_created=False; tmp=None; os.fsync(parentfd)
        return {'sha256':digest.hexdigest(),'bytes':count}
    finally:
        if tmp is not None and tmp_created:
            try: os.unlink(tmp,dir_fd=parentfd)
            except FileNotFoundError: pass
        if fd is not None: os.close(fd)
        for x in reversed(fds): os.close(x)
        os.close(parentfd)

class _NoRedirect(__import__('urllib.request',fromlist=['HTTPRedirectHandler']).HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs): raise scope.Refused('redirect_refused')

def main():
    """Fixed guarded helper; credentials arrive privately from the parent CLI."""
    import argparse, subprocess, sys, urllib.parse, urllib.request
    from .service import _installed_server_config
    parser=argparse.ArgumentParser(prog='hospital-vendor-exec')
    parser.add_argument('--resource',required=True); parser.add_argument('--output',required=True)
    parser.add_argument('--control-rid',required=True); parser.add_argument('--run-id',required=True)
    parser.add_argument('--output-rid',required=True); parser.add_argument('--output-run-id',required=True)
    args=parser.parse_args()
    server=os.environ.get('AGENT_CHAT_SERVER',''); project=os.environ.get('AGENT_CHAT_PROJECT','')
    session=os.environ.get('AGENT_CHAT_SESSION',''); host=os.environ.get('AGENT_CHAT_HOST_ID','')
    api=os.environ.get('AGENT_CHAT_API_TOKEN',''); control=os.environ.get('HOSPITAL_VENDOR_CONTROL_TOKEN','')
    run=os.environ.get('HOSPITAL_VENDOR_RUN_TOKEN',''); outtoken=os.environ.get('HOSPITAL_VENDOR_OUTPUT_TOKEN','')
    output_root=os.environ.get('HOSPITAL_VENDOR_OUTPUT_RESOURCE','')
    values=[server,project,session,host,api,control,run,outtoken,output_root,args.resource,args.output,
            args.control_rid,args.run_id,args.output_rid,args.output_run_id]
    if any(not x or '\n' in x or '\r' in x for x in values): raise scope.Refused('execution_context_invalid')
    parsed=urllib.parse.urlsplit(server); config=_installed_server_config()
    if (parsed.scheme!='http' or parsed.hostname!=config.host or parsed.port!=config.port
        or parsed.username is not None or parsed.password is not None or parsed.path not in ('','/')
        or parsed.query or parsed.fragment or project!=scope.PROJECT or session!=scope.CONSUMER or host!=scope.HOST):
        raise scope.Refused('execution_context_changed')
    if args.resource not in scope.manifest() or not output_root.startswith('/'):
        raise scope.Refused('execution_scope_invalid')
    opener=urllib.request.build_opener(urllib.request.ProxyHandler({}),_NoRedirect())
    operation=''
    def call(op, *, digest='', size=''):
        fields=[op,session,host,args.resource,args.control_rid,control,args.run_id,run,output_root,
                args.output_rid,outtoken,args.output_run_id,args.output,operation,digest,size]
        expression='''[inputs] as $x | {op:$x[0],session:$x[1],host_id:$x[2],params:{resource:$x[3],control_rid:$x[4],control_token:$x[5],run_id:$x[6],run_token:$x[7],output_resource:$x[8],output_rid:$x[9],output_token:$x[10],output_run_id:$x[11],output_path:$x[12]}} | if $x[0]=="hospital-vendor-start" then . elif $x[0]=="hospital-vendor-pulse" then .params.operation_id=$x[13] elif $x[0]=="hospital-vendor-finish" then .params.operation_id=$x[13] | .params.digest=$x[14] | .params.byte_count=($x[15]|tonumber) else error("invalid") end'''
        data=subprocess.run(['jq','-Rn',expression],input='\n'.join(fields)+'\n',text=True,capture_output=True,check=True,timeout=5).stdout.encode()
        req=urllib.request.Request(server.rstrip('/')+'/api/coord',data=data,headers={'Content-Type':'application/json','Authorization':'Bearer '+api,'X-Agent-Chat-Project':project},method='POST')
        with opener.open(req,timeout=5) as response:
            raw=response.read(65537)
        if len(raw)>65536: raise scope.Refused('response_too_large')
        query={'hospital-vendor-start':'.state=="started" and .resource==$resource and (.operation_id|type=="string")',
               'hospital-vendor-pulse':'.state=="active" and .resource==$resource and .operation_id==$operation',
               'hospital-vendor-finish':'.state=="unverified" and .resource==$resource'}[op]
        if subprocess.run(['jq','-e','--arg','resource',args.resource,'--arg','operation',operation,query],input=raw,capture_output=True,timeout=5).returncode:
            raise scope.Refused('server_validation_failed')
        if op=='hospital-vendor-start':
            return subprocess.run(['jq','-er','.operation_id'],input=raw,capture_output=True,text=False,check=True,timeout=5).stdout.decode().strip()
    operation=call('hospital-vendor-start')
    if not operation.startswith('vendor-copy-') or len(operation)!=44:
        raise scope.Refused('operation_id_invalid')
    result=_copy_one(str(config.working_directory),args.resource,args.output,output_root,lambda:call('hospital-vendor-pulse'))
    call('hospital-vendor-finish',digest=result['sha256'],size=str(result['bytes']))
    print('local copy reported; server outcome unverified bytes=%s sha256=%s' % (result['bytes'],result['sha256']),file=sys.stderr)

if __name__=='__main__': main()
