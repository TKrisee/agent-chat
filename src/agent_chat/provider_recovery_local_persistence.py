"""Read-only kernel/store/rollout provenance adapter; UNKNOWN stays UNKNOWN."""
import os
from pathlib import Path
import stat
from . import provider_recovery_peer as v2
from .provider_recovery_wire import jq,field


def capture(selected,expected_thread):
    from .provider_recovery_provenance import capture_sources
    bundle,sources=capture_sources(selected,expected_thread)
    raw={name:jq(bundle,'.'+name+' // {}') for name in ('metadata','context','session_roots','auxiliary','history_metadata')}
    jq(bundle,'if .metadata.thread.id!=$thread or .metadata.thread.archived!=0 '
       'or (.metadata.thread.rollout_path|type)!="string" then error("canonical unarchived local thread binding absent") else . end',thread=expected_thread)
    if raw['session_roots']!='null':
        jq(raw['session_roots'],'if .thread_id!=$thread then error("session roots belong to another thread") else . end',thread=expected_thread)
    rollout=Path(field(raw['metadata'],'.thread.rollout_path'))
    info=rollout.lstat()  # exact rollout was read and bound by provenance.capture_sources.
    if not stat.S_ISREG(info.st_mode) or info.st_uid!=os.getuid() or info.st_size<=0:
        raise v2.Blocked('selected local materialized history file unavailable')
    result=jq(bundle,'''
      . as $all | .metadata.thread as $m | .context.settings as $c |
      {kind:"local-persistence",threadId:$thread,current_native_state:"UNKNOWN",
       current_turn:{id:$c.turn_id,observed_at:.context.timestamp,state:"UNKNOWN"},
       old_queue:"UNKNOWN",history_coverage:"UNKNOWN",
       known_settings:({model:($c.model//$m.model),modelProvider:($m.model_provider//.session_roots.model_provider),
         cwd:($c.cwd//$m.cwd),approvalPolicy:($c.approval_policy//$m.approval_mode),
         approvalsReviewer:$c.approvals_reviewer,reasoningEffort:($c.effort//$c.reasoning_effort//$m.reasoning_effort)}
         |with_entries(select(.value!=null))),
       known_profile_id:$c.active_permission_profile.id,
       known_sandbox:({type:(if $c.sandbox_policy.type=="workspace-write" then "workspaceWrite"
         elif $c.sandbox_policy.type=="read-only" then "readOnly"
         elif $c.sandbox_policy.type=="danger-full-access" then "dangerFullAccess" else null end),
         networkAccess:$c.sandbox_policy.network_access,excludeSlashTmp:$c.sandbox_policy.exclude_slash_tmp,
         excludeTmpdirEnvVar:$c.sandbox_policy.exclude_tmpdir_env_var}|with_entries(select(.value!=null))),
       recorded_local_facts:{metadata:{thread_id:$m.id,history_mode:$m.history_mode,updated_at_ms:$m.updated_at_ms,
         model:$m.model,model_provider:$m.model_provider,reasoning_effort:$m.reasoning_effort,approval_mode:$m.approval_mode,
         cwd:$m.cwd,sandbox_policy:$m.sandbox_policy},context:{timestamp:$all.context.timestamp,settings:$c},
         session_roots:$all.session_roots,auxiliary_context:($all.auxiliary.context|if type=="object" then
           {permission_profile,file_system_sandbox_policy,multi_agent_version} else null end),
         history_events:($all.history_metadata|if type=="object" then {last_started,last_completed,last_aborted,last_assistant_metadata} else null end)},
       unknown_settings:["multiAgentMode","serviceTier"],
       verification_limits:["old native state and queue unknown","full historical byte coverage unverified",
         "unmapped permission profile/file-system facts retained without inventing API equivalence"]}
       | if $all.session_roots.runtime_workspace_roots_present==true
         and ($all.session_roots.runtime_workspace_roots|type)=="array"
         then .known_settings.runtimeWorkspaceRoots=$all.session_roots.runtime_workspace_roots
         else .unknown_settings += ["runtimeWorkspaceRoots"] end
      ''',thread=expected_thread)
    result=jq(result,'.+{provenance_version:4,source_files:('+sources+'),history_reference:{path:$path,device:($dev|tonumber),inode:($ino|tonumber),size:($size|tonumber)}}',
              path=str(rollout),dev=info.st_dev,ino=info.st_ino,size=info.st_size)
    return result


def preserved_file(baseline):
    jq(baseline,'if .provenance_version!=4 then error("legacy caller-metadata checkpoint is not verified local provenance") else . end')
    path=Path(field(baseline,'.history_reference.path'))
    info=path.lstat()
    expected=(int(field(baseline,'.history_reference.device')),int(field(baseline,'.history_reference.inode')))
    if (info.st_dev,info.st_ino)!=expected or info.st_size<int(field(baseline,'.history_reference.size')):
        raise v2.Blocked('known local history file replaced, removed or shrunk; preservation unverified')
    # Same file/no shrink is intentionally NOT a full byte-history equality claim.
