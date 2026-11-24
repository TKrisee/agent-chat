"""Explicit configured-Root local administrative provider-recovery route."""
import argparse
from contextlib import closing
from pathlib import Path
from . import provider_recovery_lifecycle as operation
from . import provider_recovery_peer as peer, provider_recovery_effects as effects
from .provider_recovery_wire import jq

def _owned_request(authority,request):
    path=authority.master.parent/'provider-recovery-v3/requests.sqlite3'
    if not path.exists():raise operation.Blocked('no durable provider recovery journal')
    peer.trusted_file(path,authority.config.uid,contents=False)
    with closing(peer.readonly(path)) as db:
        row=db.execute('SELECT plan,state,reason,effects FROM requests WHERE id=?',(request,)).fetchone()
        suffix='\n'+repr(authority.root)+'\n'+authority.source_id+'\n'+authority.source_digest
        if not row or not row[0].endswith(suffix):raise operation.Blocked('request does not belong to exact current Root and authenticated operator source')
    authority.check()
    return path,row

def _result(request,state,reason,effect,*,old=None,listener=None):
    return jq('{}','{request_id:$request,state:$state,reason:$reason,effects:$effects,ready:false,old_generation:($old|if .=="" then null else . end),listener:($listener|if .=="" then null else . end)}',request=request,state=state,reason=reason,effects=effect,old=old or '',listener=listener or '')

def main(argv=None):
    parser=argparse.ArgumentParser(prog='agent-chat-client root-local provider-recovery',description=__doc__)
    commands=parser.add_subparsers(dest='operation',required=True)
    for action in ('stage','status','reconcile','recover-uncertain'):
        cmd=commands.add_parser(action)
        cmd.add_argument('--request-id',required=True);cmd.add_argument('--source-id',required=True)
        cmd.add_argument('--home-session',required=True);cmd.add_argument('--host-id',required=True)
        if action in ('stage','recover-uncertain'):
            for name in ('uid','pid','birth-sec','birth-usec'):cmd.add_argument('--'+name,type=int,required=True)
            cmd.add_argument('--endpoint',required=True);cmd.add_argument('--manifest',type=Path,required=True)
            cmd.add_argument('--manifest-sha256',required=True)
            if action=='recover-uncertain':cmd.add_argument('--timeout',type=float,default=120)
    args=parser.parse_args(argv)
    try:
        authority=operation.AdministrativeAuthority(args.source_id)
        if authority.root[1]!=args.home_session or authority.root[2]!=args.host_id:
            raise operation.Blocked('exact configured Root/session/host assertion differs')
        if args.operation in ('status','reconcile'):
            path,row=_owned_request(authority,args.request_id)
            if args.operation=='reconcile':
                result=effects.reconcile_private(path,args.request_id)
                authority.check()
                print(_result(args.request_id,result.outcome.state,result.outcome.reason,result.outcome.effects,
                              old=result.old_generation,listener=result.listener))
            else:print(_result(args.request_id,*row[1:]))
            return 0
        selected=peer.old_selection(args.uid,args.pid,args.birth_sec,args.birth_usec,args.endpoint)
        if args.operation=='stage':
            result=operation.stage_local_checkpoint(args.request_id,args.source_id,selected,args.manifest,args.manifest_sha256)
        else:
            result=operation.recover_uncertain(args.request_id,args.source_id,selected,args.manifest,args.manifest_sha256,timeout=args.timeout)
        print(_result(result.request_id,result.state,result.reason,result.effects))
        return 0 if result.state in ('staged','restored_known_local_state_prior_effects_unknown','restored_checkpoint_old_state_unknown') else 1
    except Exception as error:
        print(jq('{}','{state:"blocked",ready:false,error:$error}',error=type(error).__name__+': '+str(error)))
        return 2
