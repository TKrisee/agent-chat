"""Exact external file identity and bounded local snapshot for a guarded read."""
import argparse
import hashlib
import os
import secrets
import stat
import subprocess
import sys
import urllib.parse
import urllib.request
from types import MappingProxyType
from pathlib import Path
from .core import CoordError

MAX_BYTES = 64 * 1024 * 1024
CHUNK_BYTES = 64 * 1024
# The first supported locator is deliberately the one reviewed task, not a
# general escape from workspace or execution-domain resource fences.
SELECTED_PROJECT = 'default'
SELECTED_SESSION = 'session_8f421d93a401439ebdb5906138c808fd'
SELECTED_HOST = 'host_o6KZLd0kkjchCaH8OjoJZ9aZXHDlFiE2'
SELECTED_PATH = '/Users/kristoftischler/Library/Application Support/UnityHub/logs/cli-log.json'
# Exact retained child and parent-route selection; no direct child thread.
CHARACTERS_SESSION = 'session_qP0LHY6hfrHkUkXPYxvEdDf9pqVodUZo'
CHARACTERS_PARENT = 'session_8f421d93a401439ebdb5906138c808fd'
CHARACTERS_AGENT_PATH = '/root/screamer_binding_review'
CHARACTERS_PARENT_THREAD = '01a11d1f-76b8-7603-84c4-df6ad8e36891'
CHARACTERS_PROJECT = 'default'
CHARACTERS_HOST = 'host_o6KZLd0kkjchCaH8OjoJZ9aZXHDlFiE2'
_CHARACTERS_BASE = '/private/tmp/itr-characters-screamer-native-validation04-20261104/'
# Exact selected literal names and frozen author digests; never a prefix grant.
CHARACTERS_FILES = MappingProxyType({
    _CHARACTERS_BASE+'ASSOCIATION_PROVENANCE.txt': '27336703af51fd7bffaeb7ef4850e34481e7216cbaac9d224c42a465361465fa',
    _CHARACTERS_BASE+'BATCH_ALIAS_CONTRACT.md': 'ab9390567adf7ea64563b36b40678ce86885c0a9a88577bc97670cec9ce04c24',
    _CHARACTERS_BASE+'OUTPUT_MAPPING.tsv': 'a87c8a4a4aea9773b5af3ced7f0295d000e9504944fa096aca818e185a3ea964',
    _CHARACTERS_BASE+'SOURCE_MAPPING.tsv': '0e9d36f1eacead01f68cc88f9c3264a4c815cddda6d1f6a0b1bdc04c788ad3bd',
    _CHARACTERS_BASE+'ScreamerNativeValidation04Probe.cs': '900b8a1b3afdb914b4fc9488f21d68d4aefbdfb98d467e39de3e2b1f635e00ab',
    _CHARACTERS_BASE+'VALIDATION04_FROZEN_CONTRACT.md': 'c85cec948a940ccad60c36c5d17523e9d4ce0ab9dad483e7148a5bec122c2b7a',
    _CHARACTERS_BASE+'run-native04-editor.sh': '4b87ad8dad50bf928f3beac9c0668db785b022e5bb9ddeb81cd20f43e256eece',
    _CHARACTERS_BASE+'stage-create-payload.sh': 'a16b74261a4410eab62d379fca899d48fd5d1ada4bee1c5885603ca3a4a8dde2',
    _CHARACTERS_BASE+'stage-create.sh': '49a7a72f5e44575b9c6d742162b78683f436c17cb0f43d9c1ba3a0ecc7eea4c6',
    _CHARACTERS_BASE+'stage-delete-payload.sh': 'dfc5d1270c5d65ee05765f2dc2c78a21496051628640fd10b4c53403d0b70dfc',
    _CHARACTERS_BASE+'stage-delete.sh': 'd8e588f62cf102cd012354b52ec9aad0bd5b84da035650484456e22887b821ff',
    _CHARACTERS_BASE+'staging-ProjectVersion.txt': '783f2de974f75156071dec6bb0c52e1004fd919a5078f7f84c75a936d3d787c0',
    _CHARACTERS_BASE+'staging-manifest.json': 'faba985fe46a09fe25ad9ca65fda4c3ec3e1d29ba384e3101c78ce69c3a4b752',
})

def exact_path(path):
    if (not isinstance(path, str) or not path.startswith('/') or path == '/' or '\x00' in path
            or '\\' in path or any(ord(c) < 32 or ord(c) == 127 for c in path)
            or any(part in ('', '.', '..') for part in path[1:].split('/'))):
        raise CoordError('exact file path must be canonical absolute and traversal-free')
    return path

def _walk(path):
    parts=exact_path(path)[1:].split('/')
    fds=[]
    try:
        fd=os.open('/',os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        fds.append(fd)
        for part in parts[:-1]:
            fd=os.open(part,os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,dir_fd=fd)
            fds.append(fd)
        file_fd=os.open(parts[-1],os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW,dir_fd=fd)
        return fds,file_fd
    except BaseException:
        for fd in reversed(fds): os.close(fd)
        raise

def _stamp(info):
    return (info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns,info.st_ctime_ns,info.st_mode,info.st_uid,info.st_nlink)

def _output_parent(path):
    parts=exact_path(path)[1:].split('/')
    fd=os.open('/',os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for part in parts[:-1]:
            child=os.open(part,os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,dir_fd=fd)
            os.close(fd); fd=child
        info=os.fstat(fd)
        if info.st_uid!=os.getuid() or info.st_mode & 0o077:
            raise CoordError('exact file output directory must be owner-only')
        return fd,parts[-1]
    except BaseException:
        os.close(fd)
        raise

def snapshot(path, output, limit, *, validate=lambda: None):
    if isinstance(limit,bool) or not isinstance(limit,int) or not 1 <= limit <= MAX_BYTES:
        raise CoordError('exact file byte limit out of range')
    fds=[]; file_fd=None; parent=None; stage=None; stage_identity=None
    try:
        validate()
        fds,file_fd=_walk(path)
        info=os.fstat(file_fd)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink!=1 or info.st_uid!=os.getuid() or info.st_size>limit:
            raise CoordError('exact file identity or byte limit differs')
        parent_stamps=[(os.fstat(fd).st_dev,os.fstat(fd).st_ino) for fd in fds]
        parent,name=_output_parent(output)
        stage='.agent-chat-snapshot-'+secrets.token_hex(16)
        out=os.open(stage,os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,0o600,dir_fd=parent)
        stage_stat=os.fstat(out)
        stage_identity=(stage_stat.st_dev,stage_stat.st_ino)
        digest=hashlib.sha256(); count=0
        with os.fdopen(out,'wb') as stream:
            while True:
                validate()
                chunk=os.read(file_fd,min(CHUNK_BYTES,limit+1-count))
                if not chunk: break
                count+=len(chunk)
                if count>limit:
                    raise CoordError('exact file changed or exceeds byte limit')
                stream.write(chunk); digest.update(chunk)
            stream.flush(); os.fsync(stream.fileno())
            staged_stamp=_stamp(os.fstat(stream.fileno()))
        if count!=info.st_size or _stamp(os.fstat(file_fd))!=_stamp(info):
            raise CoordError('exact file changed or exceeds byte limit')
        validate()
        # Recheck physical identities after the final capability round trip.
        check_fds,check_fd=_walk(path)
        try:
            if ([(os.fstat(fd).st_dev,os.fstat(fd).st_ino) for fd in check_fds]!=parent_stamps
                    or _stamp(os.fstat(check_fd))!=_stamp(info)):
                raise CoordError('exact file physical identity changed')
        finally:
            os.close(check_fd)
            for fd in reversed(check_fds): os.close(fd)
        digest=digest.hexdigest()
        expected_digest=CHARACTERS_FILES.get(path)
        if expected_digest is not None and digest != expected_digest:
            raise CoordError('exact file frozen digest differs')
        check_parent,_=_output_parent(output)
        try:
            a,b=os.fstat(parent),os.fstat(check_parent)
            if (a.st_dev,a.st_ino)!=(b.st_dev,b.st_ino):
                raise CoordError('exact file output parent changed')
        finally: os.close(check_parent)
        if _stamp(os.stat(stage,dir_fd=parent,follow_symlinks=False))!=staged_stamp:
            raise CoordError('exact file staging identity changed')
        # Linking creates the final name atomically without replacing an
        # existing destination. A later failure must not erase that real effect.
        os.link(stage,name,src_dir_fd=parent,dst_dir_fd=parent,follow_symlinks=False)
        os.unlink(stage,dir_fd=parent); stage=None
        os.fsync(parent)
        final=os.stat(name,dir_fd=parent,follow_symlinks=False)
        if (final.st_dev,final.st_ino)!=stage_identity:
            raise CoordError('exact file published output identity changed')
        return {'path':path,'bytes':count,'sha256':digest,
                'device':info.st_dev,'inode':info.st_ino,'mtime_ns':info.st_mtime_ns,'snapshot_stable':True,
                'output_device':final.st_dev,'output_inode':final.st_ino,'output_mtime_ns':final.st_mtime_ns}
    except OSError as error:
        raise CoordError('exact file snapshot unavailable') from error
    finally:
        try:
            if parent is not None:
                try:
                    if stage is not None and stage_identity is not None:
                        try:
                            current=os.stat(stage,dir_fd=parent,follow_symlinks=False)
                        except FileNotFoundError:
                            pass
                        else:
                            if (current.st_dev,current.st_ino)==stage_identity:
                                os.unlink(stage,dir_fd=parent)
                finally: os.close(parent)
        finally:
            if file_fd is not None: os.close(file_fd)
            for fd in reversed(fds): os.close(fd)

def observe_output(path, limit, *, validate=lambda:None):
    """Fresh bounded host observation; never a claim of historical no-write."""
    validate()
    fds,fd=_walk(path)
    try:
        before=os.fstat(fd)
        if (not stat.S_ISREG(before.st_mode) or before.st_nlink!=1 or before.st_uid!=os.getuid()
                or before.st_size>limit):raise CoordError('exact file output identity or byte limit differs')
        digest=hashlib.sha256();count=0
        while True:
            validate()
            chunk=os.read(fd,min(CHUNK_BYTES,limit+1-count))
            if not chunk:break
            count+=len(chunk)
            if count>limit:raise CoordError('exact file output exceeds byte limit')
            digest.update(chunk)
        validate()
        again_fds,again_fd=_walk(path)
        try:
            if (_stamp(before)!=_stamp(os.fstat(fd)) or _stamp(before)!=_stamp(os.fstat(again_fd))
                    or [(os.fstat(x).st_dev,os.fstat(x).st_ino) for x in fds]!=
                    [(os.fstat(x).st_dev,os.fstat(x).st_ino) for x in again_fds] or count!=before.st_size):
                raise CoordError('exact file output changed during observation')
        finally:
            os.close(again_fd)
            for item in reversed(again_fds):os.close(item)
        return dict(bytes=count,sha256=digest.hexdigest(),output_device=before.st_dev,
                    output_inode=before.st_ino,output_mtime_ns=before.st_mtime_ns)
    finally:
        os.close(fd)
        for item in reversed(fds):os.close(item)

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--path',required=True); p.add_argument('--output',required=True); p.add_argument('--max-bytes',type=int,required=True)
    p.add_argument('--run-id',required=True); p.add_argument('--reservation-id',required=True); p.add_argument('--binding-id')
    p.add_argument('--execution-generation');p.add_argument('--expected-helper-sha256')
    a=p.parse_args()
    helper_sha=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    if a.binding_id and (not a.execution_generation or a.expected_helper_sha256!=helper_sha):
        raise CoordError('exact file helper generation unavailable or changed')
    server=os.environ.get('AGENT_CHAT_SERVER'); project=os.environ.get('AGENT_CHAT_PROJECT')
    session=os.environ.get('AGENT_CHAT_SESSION'); host=os.environ.get('AGENT_CHAT_HOST_ID')
    token=os.environ.get('AGENT_CHAT_TOKEN'); api_token=os.environ.get('AGENT_CHAT_API_TOKEN')
    if not all((server,project,session,host,token,api_token)): raise CoordError('exact file guard context unavailable')
    parsed=urllib.parse.urlsplit(server)
    if (parsed.scheme!='http' or parsed.hostname!='127.0.0.1' or not parsed.port
            or parsed.username is not None or parsed.password is not None or parsed.path not in ('','/')
            or parsed.query or parsed.fragment): raise CoordError('exact file server origin differs')
    if any('\n' in value or '\r' in value for value in (session,host,a.path,a.run_id,token)):
        raise CoordError('exact file guard context invalid')
    body=subprocess.run(['jq','-Rn','--arg','binding',a.binding_id or '', '--arg','output',a.output,'--argjson','limit',str(a.max_bytes),'[inputs] | {op:"exact-file-pulse",session:.[0],host_id:.[1],params:{path:.[2],run_id:.[3],token:.[4]}} | if $binding!="" then .params+={binding_id:$binding,output:$output,max_bytes:$limit} else . end'],
        input='\n'.join((session,host,a.path,a.run_id,token))+'\n',text=True,capture_output=True,check=True,timeout=5).stdout.encode()
    opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
    def validate():
        request=urllib.request.Request(server.rstrip('/')+'/api/coord',data=body,headers={
            'Content-Type':'application/json','Authorization':'Bearer '+api_token,'X-Agent-Chat-Project':project},method='POST')
        with opener.open(request,timeout=5) as response: data=response.read(65537)
        if len(data)>65536: raise CoordError('exact file guard response exceeds bound')
        result=subprocess.run(['jq','-e','--arg','path',a.path,'--arg','rid',a.reservation_id,
            '--arg','binding',a.binding_id or '', '--arg','output',a.output,'--argjson','limit',str(a.max_bytes),
            '.resource==$path and .reservation_id==$rid and .state=="owned" and ($binding=="" or (.binding_id==$binding and .output==$output and .max_bytes==$limit))'],input=data,capture_output=True,timeout=5)
        if result.returncode:
            raise CoordError('exact file guard binding changed')
    result=snapshot(a.path,a.output,a.max_bytes,validate=validate)
    if a.binding_id:
        # No Python JSON decoder: build and validate the typed host report using jq.
        args=['jq','-cn','--arg','session',session,'--arg','host',host,'--arg','path',a.path,
              '--arg','binding',a.binding_id,'--arg','output',a.output,'--arg','run',a.run_id,
              '--arg','rid',a.reservation_id,'--arg','generation',a.execution_generation,
              '--arg','token',token,'--arg','helper',helper_sha,'--arg','sha',result['sha256'],
              '--argjson','limit',str(a.max_bytes),'--argjson','pid',str(os.getpid())]
        for key in ('bytes','device','inode','mtime_ns','output_device','output_inode','output_mtime_ns'):
            args+=['--argjson',key,str(result[key])]
        args+=['{op:"exact-file-produced",session:$session,host_id:$host,params:{path:$path,binding_id:$binding,output:$output,max_bytes:$limit,run_id:$run,reservation_id:$rid,generation:$generation,token:$token,helper_sha256:$helper,pid:$pid,sha256:$sha,bytes:$bytes,device:$device,inode:$inode,mtime_ns:$mtime_ns,output_device:$output_device,output_inode:$output_inode,output_mtime_ns:$output_mtime_ns}}']
        report=subprocess.run(args,capture_output=True,check=True,timeout=5).stdout
        request=urllib.request.Request(server.rstrip('/')+'/api/coord',data=report,headers={
            'Content-Type':'application/json','Authorization':'Bearer '+api_token,'X-Agent-Chat-Project':project},method='POST')
        with opener.open(request,timeout=5) as response:data=response.read(65537)
        if len(data)>65536:raise CoordError('exact file production response exceeds bound')
        checked=subprocess.run(['jq','-e','--arg','run',a.run_id,'--arg','generation',a.execution_generation,
            '--arg','sha',result['sha256'],'--argjson','size',str(result['bytes']),
            '.run_id==$run and .generation==$generation and .state=="produced" and .sha256==$sha and .bytes==$size'],input=data,capture_output=True,timeout=5)
        if checked.returncode:raise CoordError('exact file production report unresolved; do not replay copy')
    print('bytes=%s sha256=%s device=%s inode=%s mtime_ns=%s snapshot_stable=true' %
          (result['bytes'],result['sha256'],result['device'],result['inode'],result['mtime_ns']),file=sys.stderr)

if __name__=='__main__': main()
