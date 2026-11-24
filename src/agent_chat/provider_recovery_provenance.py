"""Kernel-bound actual open SQLite store; no environment or arbitrary metadata inputs."""
from pathlib import Path
from contextlib import closing
import os, stat, subprocess, sqlite3
from . import provider_recovery_peer as v2
from .provider_recovery_wire import jq, field

def generation(selected):
    observed=v2.kernel_identity(selected.pid)
    if (observed[0],observed[1],observed[3],observed[4]) != (selected.pid,selected.uid,selected.birth_sec,selected.birth_usec):
        raise v2.Blocked('selected provider generation changed during local evidence capture')

def open_store(selected):
    generation(selected)
    reply=subprocess.run(['/usr/sbin/lsof','-nP','-a','-p',str(selected.pid),'-F0pftDin'],capture_output=True,timeout=3)
    if reply.returncode:raise v2.Blocked('selected provider open-file observation unavailable')
    # Parse lsof's native NUL field protocol, NOT JSON. Never retain unrelated paths.
    records=[];record={}
    for item in reply.stdout.split(b'\0'):
        item=item.lstrip(b'\n')
        if not item:continue
        key=chr(item[0]);value=os.fsdecode(item[1:])
        if key=='f':
            if record:records.append(record)
            record={}
        if key in ('f','t','D','i','n'):record[key]=value
    if record:records.append(record)
    found={}
    for row in records:
        if row.get('t')=='REG' and Path(row.get('n','')).name=='state_5.sqlite':
            try:found.setdefault((int(row['D'],16),int(row['i'])),set()).add(row['n'])
            except (KeyError,ValueError):raise v2.Blocked('open SQLite file identity incomplete')
    if len(found)!=1:raise v2.Blocked('selected provider has no unique observed open state_5.sqlite; configured store cannot be established')
    (dev,ino),paths=found.popitem();path=Path(sorted(paths)[0])
    info=path.lstat()
    if not path.is_absolute() or not stat.S_ISREG(info.st_mode) or info.st_uid!=selected.uid or (info.st_dev,info.st_ino)!=(dev,ino):
        raise v2.Blocked('observed SQLite path/file identity differs')
    generation(selected)
    return path,dev,ino

def capture_sources(selected,thread):
    identity=open_store(selected);path,dev,ino=identity
    query="SELECT json_object('thread',json_object('id',id,'archived',archived,'rollout_path',rollout_path,'history_mode',history_mode,'updated_at_ms',updated_at_ms,'model',model,'model_provider',model_provider,'reasoning_effort',reasoning_effort,'approval_mode',approval_mode,'cwd',cwd,'sandbox_policy',sandbox_policy)) FROM threads WHERE id=?"
    with closing(v2.readonly(path)) as db:
        rows=db.execute(query,(thread,)).fetchall()
        if len(rows)!=1:raise v2.Blocked('canonical Root has no unique record in observed provider SQLite store')
        metadata=jq(rows[0][0],'.thread.sandbox_policy |= (if type=="string" then fromjson else . end)')
        rollout=Path(field(metadata,'.thread.rollout_path'))
        if not rollout.is_absolute():raise v2.Blocked('relative rollout path requires unavailable provider cwd resolution')
        with open(rollout,'rb') as stream:
            info=os.fstat(stream.fileno());current=rollout.lstat()
            if (not stat.S_ISREG(info.st_mode) or info.st_uid!=selected.uid or
                (info.st_dev,info.st_ino)!=(current.st_dev,current.st_ino) or not stat.S_ISREG(current.st_mode) or
                not 0<info.st_size<=256*1024*1024):raise v2.Blocked('exact referenced rollout regular-file identity or bounded size unavailable')
            content=stream.read(info.st_size)
            if len(content)!=info.st_size:raise v2.Blocked('rollout changed during bounded metadata read')
            # jq streams exact source JSONL; only selected metadata, never message contents, leave this process.
            projection=subprocess.run(['jq','-nceS','--arg','thread',thread,'''
              reduce inputs as $r ({sessions:[],context:null,last_started:null,last_completed:null,last_aborted:null,last_assistant_metadata:null};
                if $r.type=="session_meta" then .sessions += [{timestamp:$r.timestamp,payload:($r.payload|{id,model_provider,runtime_workspace_roots})}]
                elif $r.type=="turn_context" then .context={timestamp:$r.timestamp,settings:($r.payload|{turn_id,model,effort,reasoning_effort,cwd,approval_policy,approvals_reviewer,active_permission_profile,sandbox_policy,permission_profile,file_system_sandbox_policy,multi_agent_version})}
                elif $r.type=="event_msg" and $r.payload.type=="task_started" then .last_started={timestamp:$r.timestamp,turn_id:$r.payload.turn_id}
                elif $r.type=="event_msg" and $r.payload.type=="task_complete" then .last_completed={timestamp:$r.timestamp,turn_id:$r.payload.turn_id}
                elif $r.type=="event_msg" and $r.payload.type=="turn_aborted" then .last_aborted={timestamp:$r.timestamp,turn_id:$r.payload.turn_id}
                elif $r.type=="response_item" and $r.payload.role=="assistant" then .last_assistant_metadata={timestamp:$r.timestamp,phase:$r.payload.phase}
                else . end)
              | if (.sessions|length)!=1 or .sessions[0].payload.id!=$thread then error("exact rollout canonical session mismatch") else . end
            '''],input=content,capture_output=True,timeout=8)
            if projection.returncode:raise v2.Blocked('exact rollout metadata invalid or canonical session differs')
            projected=projection.stdout.decode()
            after=rollout.lstat()
            if (after.st_dev,after.st_ino)!=(info.st_dev,info.st_ino) or after.st_size<info.st_size:
                raise v2.Blocked('referenced rollout replaced or shrunk during capture')
        if db.execute(query,(thread,)).fetchall()!=rows:raise v2.Blocked('canonical SQLite row changed during capture')
    if open_store(selected)!=identity:raise v2.Blocked('provider SQLite handle changed during capture')
    bundle=jq(projected,'''{metadata:($metadata|fromjson),context:.context,
      session_roots:{thread_id:.sessions[0].payload.id,timestamp:.sessions[0].timestamp,
        model_provider:.sessions[0].payload.model_provider,runtime_workspace_roots:.sessions[0].payload.runtime_workspace_roots,
        runtime_workspace_roots_present:(.sessions[0].payload.runtime_workspace_roots|type=="array")},
      auxiliary:{context:.context.settings},history_metadata:{last_started,last_completed,last_aborted,last_assistant_metadata}}''',metadata=metadata)
    sources=jq('{}','{database:{path:$path,device:($dev|tonumber),inode:($ino|tonumber),row_sha256:$rowsha,observed_provider_pid:($pid|tonumber)},rollout:{path:$rollout,sha256_prefix:$sha,bytes:($size|tonumber),session_id:$thread}}',path=path,dev=dev,ino=ino,rowsha=v2.digest(metadata.encode()),pid=selected.pid,rollout=rollout,sha=v2.digest(content),size=info.st_size,thread=thread)
    return bundle,sources
