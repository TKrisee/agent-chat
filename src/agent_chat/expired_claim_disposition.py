"""Configured-Root exact expired validation-clone claim disposition.

This is NOT actor retirement, ordinary recovery, native exclusion or a receipt.
CLI output is bounded TSV; no JSON parser or new authentication principal.
"""
from __future__ import annotations
import argparse
import contextlib
import hashlib
import math
import os
import sqlite3
import stat
import subprocess
import time
import uuid
from .core import CoordError
from .guard_generation import guard_generation
from .ownership_disposition import (load_configured_root, target_path,
    _read_existing, _require, _has_table, _clean, _recheck_root_barriers,
    _recheck_registered_target, load_wbt_preview_configured_root,
    load_wbt_diagnosis_configured_root,
    load_wbt_resumed_configured_root, _wbt_resumed_project_stop_context,
    _load_wbt_configured_root_base,
    _sample, _wbt_normalize_trigger_sql,
    _wbt_retired_pending_intents_are_typed_admin_records,
    _wbt_retired_pending_intents_check, _project_resumed_stop_context)
from .root_local import _preflight

TABLE='root_expired_claim_dispositions'
COLUMNS=('request_id','payload','project','resource','owner_session','reservation_id',
         'deadline','root_session','root_host','snapshot','outcome','recorded_at')
_FIXED_EXPIRED_CLAIM_RESOURCES_V3=frozenset((
    'heavy-unity:2',
    'workspace:gameplay',
    'operation:gameplay-fivecase-YLN-20261014-a',
    'port:udp:51382',
))
_WBT_PROJECT_ID='project_ce8bc2489a654f45a9be6ae8e2107fda'
_WBT_COHORT_REF=('message_LQZLiPTothZ8DE76fO2J8f3tdtOO4GLl/'
                 'attachment_CqD-npf1BMSkUrYalKsrwPRdNrumeY-w')
_WBT_ABANDON_AUTHORITY_MESSAGE='message_xC-GTCbLz4ZyGJIzfIgqYkfAreQ7uzSu'
_WBT_ABANDON_COHORT_INDEX=2
_WBT_FOLLOW_ON_MANIFEST_VERSION=1
_WBT_FOLLOW_ON_MANIFEST_PATH='/private/tmp/wbt-follow-on-candidates-v1.json'
_WBT_FOLLOW_ON_MANIFEST_SHA256='d346dd860fa8cf67849444c02613b786c522d74c68680d6a59319b06967346e6'
_WBT_FOLLOW_ON_MANIFEST_SIZE=1355
_WBT_FOLLOW_ON_ROW_INDICES=(3,4,5)
_WBT_GROUP_MANIFEST_VERSION=1
_WBT_GROUP_MANIFEST_PATH='/private/tmp/wbt-follow-on-candidates-0080-v1.json'
_WBT_GROUP_MANIFEST_SHA256='5cbd724c440422022da80a5987add03de7c03b3ef3b92612dd574a237a0c58b2'
_WBT_GROUP_MANIFEST_SIZE=2394
_WBT_GROUP_ROW_INDICES=(6,7,8,9,10)
_WBT_NEXT_GROUP_MANIFEST_VERSION=1
_WBT_NEXT_GROUP_MANIFEST_PATH='/private/tmp/wbt-follow-on-candidates-0081-v1.json'
_WBT_NEXT_GROUP_MANIFEST_SHA256='d03ac6c86e2dfb69e9e8346ae9235ba86f74b5787aefec11a04141f43d1870a6'
_WBT_NEXT_GROUP_MANIFEST_SIZE=5043
_WBT_NEXT_GROUP_ROW_INDICES=(11,12,13,14,15)
_WBT_NEXT_GROUP_SOURCE_PATH='/private/tmp/wbt-295-row-linked-classification-v1.json'
_WBT_NEXT_GROUP_SOURCE_SHA256='84e002a8e4809c8d2f520255600ea223746c158ea8bbc1e42f2137fa11eba691'
_WBT_NEXT_GROUP_SOURCE_SIZE=194542
_WBT_NEXT_GROUP_SOURCE_ROWS=295
_WBT_FILE_CLASS_MANIFEST_VERSION=1
_WBT_FILE_CLASS_MANIFEST_PATH='/private/tmp/wbt-follow-on-file-class-selection-20261018-v3.json'
_WBT_FILE_CLASS_MANIFEST_SHA256='32b544b89de5fe83adfc7830479a187b04a109ddebb1b49fe4de56c76b53648e'
_WBT_FILE_CLASS_MANIFEST_SIZE=8524
_WBT_FILE_CLASS_ROW_INDICES=(16,17,18,19,20,21,22,23)
_WBT_FILE_CLASS_SOURCE_PATH='/private/tmp/wbt-295-row-linked-classification-v1.json'
_WBT_FILE_CLASS_SOURCE_SHA256='84e002a8e4809c8d2f520255600ea223746c158ea8bbc1e42f2137fa11eba691'
_WBT_FILE_CLASS_SOURCE_SIZE=194542
_WBT_FILE_CLASS_SOURCE_ROWS=295
_WBT_NEXT_FILE_CLASS_MANIFEST_VERSION=1
_WBT_NEXT_FILE_CLASS_MANIFEST_PATH='/private/tmp/wbt-follow-on-file-class-selection-20261018-v4.json'
_WBT_NEXT_FILE_CLASS_MANIFEST_SHA256='f268388820383f2f2b6eff30d39ec47d4873cf263b2c4bfeb9c7b2d0517a3d8c'
_WBT_NEXT_FILE_CLASS_MANIFEST_SIZE=11666
_WBT_NEXT_FILE_CLASS_ROW_INDICES=(24,25,26,27,28,29,30,31)
_WBT_NEXT_FILE_CLASS_SOURCE_PATH='/private/tmp/wbt-295-row-linked-classification-v1.json'
_WBT_NEXT_FILE_CLASS_SOURCE_SHA256='84e002a8e4809c8d2f520255600ea223746c158ea8bbc1e42f2137fa11eba691'
_WBT_NEXT_FILE_CLASS_SOURCE_SIZE=194542
_WBT_NEXT_FILE_CLASS_SOURCE_ROWS=295
_WBT_GG1_FILE_CLASS_MANIFEST_VERSION=1
_WBT_GG1_FILE_CLASS_MANIFEST_PATH='/private/tmp/wbt-follow-on-file-class-selection-20261018-v5.json'
_WBT_GG1_FILE_CLASS_MANIFEST_SHA256='9d018b9add0b38d5eecb06a6d6154b0ae110ac319d405735ab13f80bc8900631'
_WBT_GG1_FILE_CLASS_MANIFEST_SIZE=14812
_WBT_GG1_FILE_CLASS_ROW_INDICES=(32,33,34,35,36,37,38,39)
_WBT_GG1_FILE_CLASS_SOURCE_PATH='/private/tmp/wbt-295-row-linked-classification-v1.json'
_WBT_GG1_FILE_CLASS_SOURCE_SHA256='84e002a8e4809c8d2f520255600ea223746c158ea8bbc1e42f2137fa11eba691'
_WBT_GG1_FILE_CLASS_SOURCE_SIZE=194542
_WBT_GG1_FILE_CLASS_SOURCE_ROWS=295
_WBT_HGIDE_FILE_CLASS_MANIFEST_VERSION=1
_WBT_HGIDE_FILE_CLASS_MANIFEST_PATH='/private/tmp/wbt-follow-on-file-class-selection-20261018-v6.json'
_WBT_HGIDE_FILE_CLASS_MANIFEST_SHA256='a7d7f0391b20a8531808580df10a3df86c1ba893c15841055122fb407866629c'
_WBT_HGIDE_FILE_CLASS_MANIFEST_SIZE=20522
_WBT_HGIDE_FILE_CLASS_ROW_INDICES=(40,41,42,43,44,45,46,47)
_WBT_HGIDE_FILE_CLASS_SOURCE_PATH='/private/tmp/wbt-295-row-linked-classification-v1.json'
_WBT_HGIDE_FILE_CLASS_SOURCE_SHA256='84e002a8e4809c8d2f520255600ea223746c158ea8bbc1e42f2137fa11eba691'
_WBT_HGIDE_FILE_CLASS_SOURCE_SIZE=194542
_WBT_HGIDE_FILE_CLASS_SOURCE_ROWS=295
_WBT_6JC1_FILE_CLASS_MANIFEST_VERSION=1
_WBT_6JC1_FILE_CLASS_MANIFEST_PATH='/private/tmp/wbt-follow-on-file-class-selection-20261018-v7.json'
_WBT_6JC1_FILE_CLASS_MANIFEST_SHA256='7b73a6647d437f008a1ac8dde251368fe1c75bc398656298ed6db98a4faa2874'
_WBT_6JC1_FILE_CLASS_MANIFEST_SIZE=26045
_WBT_6JC1_FILE_CLASS_ROW_INDICES=(48,49,50,51,52,53,54,55)
_WBT_6JC1_FILE_CLASS_SOURCE_PATH='/private/tmp/wbt-295-row-linked-classification-v1.json'
_WBT_6JC1_FILE_CLASS_SOURCE_SHA256='84e002a8e4809c8d2f520255600ea223746c158ea8bbc1e42f2137fa11eba691'
_WBT_6JC1_FILE_CLASS_SOURCE_SIZE=194542
_WBT_6JC1_FILE_CLASS_SOURCE_ROWS=295
_WBT_HQG_FILE_CLASS_MANIFEST_VERSION=1
_WBT_HQG_FILE_CLASS_MANIFEST_PATH='/private/tmp/wbt-next-file-class-selection-20261018-v1.json'
_WBT_HQG_FILE_CLASS_MANIFEST_SHA256='31d63a6b143d7a4af8036a597025a005c4a8276cf23471f6d26b53b76447b471'
_WBT_HQG_FILE_CLASS_MANIFEST_SIZE=31722
_WBT_HQG_FILE_CLASS_ROW_INDICES=(56,57,58,59,60,61,62,63)
_WBT_HQG_FILE_CLASS_SOURCE_PATH='/private/tmp/wbt-295-row-linked-classification-v1.json'
_WBT_HQG_FILE_CLASS_SOURCE_SHA256='84e002a8e4809c8d2f520255600ea223746c158ea8bbc1e42f2137fa11eba691'
_WBT_HQG_FILE_CLASS_SOURCE_SIZE=194542
_WBT_HQG_FILE_CLASS_SOURCE_ROWS=295
_WBT_NWC_RESUMED_V1_MANIFEST_VERSION=1
_WBT_NWC_RESUMED_V1_MANIFEST_PATH='/private/tmp/wbt-nwc-resumed-file-class-selection-20261018-v1.json'
_WBT_NWC_RESUMED_V1_MANIFEST_SHA256='c5e1d560227fbacd50931af7819ebc09abeaa991e0ae91abf1c3f0a738ad3034'
_WBT_NWC_RESUMED_V1_MANIFEST_SIZE=35394
_WBT_NWC_RESUMED_V1_ROW_INDICES=(64,65,66,67,68,69,70,71)
_WBT_NWC_RESUMED_LEGACY_ROW_INDICES=(64,)
_WBT_NWC_RESUMED_MANIFEST_VERSION=2
_WBT_NWC_RESUMED_MANIFEST_PATH='/private/tmp/wbt-nwc-resumed-file-class-selection-20261018-v2.json'
_WBT_NWC_RESUMED_MANIFEST_SHA256='7d82829b016c34ee4c26c02280bae31f8a64cc5841d26fbf8370507d3cd2b184'
_WBT_NWC_RESUMED_MANIFEST_SIZE=35639
_WBT_NWC_RESUMED_ROW_INDICES=(65,66,67,68,69,70,71)
_WBT_NWC_RESUMED_SOURCE_PATH='/private/tmp/wbt-295-row-linked-classification-v1.json'
_WBT_NWC_RESUMED_SOURCE_SHA256='84e002a8e4809c8d2f520255600ea223746c158ea8bbc1e42f2137fa11eba691'
_WBT_NWC_RESUMED_SOURCE_SIZE=194542
_WBT_NWC_RESUMED_SOURCE_ROWS=295
_WBT_XU0J_MANIFEST_VERSION=1
_WBT_XU0J_MANIFEST_PATH='/private/tmp/wbt-Xu0j-next-eight-offline-manifest-v1.json'
_WBT_XU0J_MANIFEST_SHA256='9d33a86c5a2dfdefce7fbb8f1a76056572ce57b5af8fbcde273e87be601bf30f'
_WBT_XU0J_MANIFEST_SIZE=78650
_WBT_XU0J_ROW_INDICES=(72,73,74,75,76,77,78,79)

_WBT_N6LB_MANIFEST_VERSION=1
_WBT_N6LB_MANIFEST_PATH='/private/tmp/wbt-n6Lb-next-eight-offline-manifest-v1.json'
_WBT_N6LB_MANIFEST_SHA256='823d22def512255d5e37504ca417a999ee12ec87cf7fed12140b43b7ef528816'
_WBT_N6LB_MANIFEST_SIZE=98063
_WBT_N6LB_ROW_INDICES=(80,81,82,83,92,93,94,95)
_WBT_ABANDON_INTENTS='root_wbt_file_abandon_intents'
_WBT_ABANDON_EVENTS='root_wbt_file_abandon_events'
_WBT_ABANDON_STATES=frozenset(('accepted','prepared','blocked','failed','expired','applying','applied','unknown'))
_WBT_WITNESS_VERSION='wbt-exact-row-witness-v1'
_WBT_COHORT_ARTIFACT_PATH='/private/tmp/wbt-295-row-linked-classification-v1.json'
_WBT_COHORT_ARTIFACT_SHA256='84e002a8e4809c8d2f520255600ea223746c158ea8bbc1e42f2137fa11eba691'
_WBT_COHORT_ARTIFACT_SIZE=194542
_WBT_COHORT_ARTIFACT_MAX_SIZE=256000
_WBT_COHORT_ROW_COUNT=295
_WBT_COUNTS_SOURCE_PATH='/private/tmp/agent-chat-app-reservation-counts-v1.json'
_WBT_COUNTS_SOURCE_SHA256='68cdf32d39a58101da0d892e7ab19a92738c302dfa0f0b9b47e059260e3a94c2'
_WBT_LEDGER_SOURCE_PATH='/private/tmp/agent-chat-app-nonfree-reservation-ledger-v1.json'
_WBT_LEDGER_SOURCE_SHA256='5332bd406ec664ff4941ddc87685a0bc03348d1800fd54e78ae73ec4df2d59c0'
def digest(value):
    # A versioned, unambiguous Python tuple representation, NOT JSON.
    return hashlib.sha256(repr(value).encode('utf-8')).hexdigest()
def authority(root, service_plist, project, ids):
    _preflight(str(root.master),service_plist,root.home_project,
               root.home_session,root.host_id,'status',{})
    home=target_path(root,root.home_project)
    if not isinstance(ids,list) or not 1<=len(ids)<=16 or len(set(ids))!=len(ids):
        raise CoordError('exact original operator message IDs required')
    result=[]
    with _read_existing(home) as db:
        _require(db,'messages','operator_message_origins','operator_identities')
        for mid in ids:
            mid=_clean(mid,'operator message ID',512)
            row=db.execute('''SELECT m.id,m.body,m.recipient_session,o.channel
                FROM messages m
                JOIN operator_message_origins o ON o.message_id=m.id AND o.sender_session=m.sender_session
                JOIN operator_identities i ON i.session_id=m.sender_session
                WHERE m.id=?''',(mid,)).fetchone()
            if not row or row['recipient_session']!=root.home_session or row['channel']!='authenticated_operator_ui':
                raise CoordError('original operator UI authority to canonical Root is unavailable')
            result.append((mid,hashlib.sha256(row['body'].encode('utf-8')).hexdigest()))
    return tuple(result)
def selection(root,project,resource,owner_session,reservation_id,deadline):
    resource=_clean(resource,'registered literal resource',512)
    if resource=='validation-clone':
        version='expired-claim-v2'
    elif resource in _FIXED_EXPIRED_CLAIM_RESOURCES_V3:
        version='expired-claim-v3'
    else:
        raise CoordError('this bounded consumer selects only exact registered expired-claim literals')
    if not math.isfinite(deadline) or deadline<=0:
        raise CoordError('exact finite original deadline required')
    return (version,root.home_project,root.home_session,root.host_id,
        _clean(project,'project',512),resource,_clean(owner_session,'owner',512),
        _clean(reservation_id,'reservation',512),deadline)
def wbt_witness_selection(root,project,resource,owner_session,reservation_id,deadline,cohort_ref):
    if project!=_WBT_PROJECT_ID or cohort_ref!=_WBT_COHORT_REF:
        raise CoordError('exact configured Wbt project and source cohort are required')
    resource=_clean(resource,'exact resource',512)
    if '*' in resource or '?' in resource:
        raise CoordError('wildcard resource selection is unsupported')
    if not math.isfinite(deadline) or deadline<=0:
        raise CoordError('exact finite original deadline required')
    return (_WBT_WITNESS_VERSION,root.home_project,root.home_session,root.host_id,
        _clean(project,'project',512),resource,_clean(owner_session,'owner',512),
        _clean(reservation_id,'reservation',512),deadline,_WBT_COHORT_REF)

def _wbt_pinned_artifact_membership(selected, *, row_index=None):
    """Validate the pinned dated artifact and exact four-field tuple.

    jq performs all JSON parsing. The path, size, digest and source pins are
    constants; callers cannot substitute an artifact or claim its identity.
    """
    try:
        fd=os.open(_WBT_COHORT_ARTIFACT_PATH,
                   os.O_RDONLY|getattr(os,'O_NOFOLLOW',0))
        try:
            info=os.fstat(fd)
            if (not stat.S_ISREG(info.st_mode) or info.st_size!=_WBT_COHORT_ARTIFACT_SIZE
                    or info.st_size>_WBT_COHORT_ARTIFACT_MAX_SIZE):
                return 'artifact_invalid'
            with os.fdopen(fd,'rb',closefd=False) as stream:
                payload=stream.read(_WBT_COHORT_ARTIFACT_MAX_SIZE+1)
        finally:
            os.close(fd)
        if len(payload)!=_WBT_COHORT_ARTIFACT_SIZE:
            return 'artifact_invalid'
        if hashlib.sha256(payload).hexdigest()!=_WBT_COHORT_ARTIFACT_SHA256:
            return 'artifact_invalid'
        jq_filter='''
          def validrow:
            type=="object" and
            (keys|sort)==["closure_witness","complete_guard_generation","current_eligible_cas",
              "current_host","deadline","eligibility_reason","owner_session",
              "reservation_id","resource","snapshot_queue","snapshot_reason","snapshot_state"] and
            (.resource|type=="string" and length>0) and
            (.owner_session|type=="string" and length>0) and
            (.reservation_id|type=="string" and length>0) and
            (.deadline|type=="number" and isfinite);
          type=="object" and
          (keys|sort)==["evidence_kind","row_count","rows","source_files"] and
          .evidence_kind=="dated_supported_status_snapshot_not_current" and
          .row_count==$row_count and (.rows|type=="array" and length==$row_count) and
          (.source_files|type=="object" and (keys|sort)==["counts","ledger"]) and
          .source_files.counts.path==$counts_path and
          .source_files.counts.sha256==$counts_hash and
          .source_files.ledger.path==$ledger_path and
          .source_files.ledger.sha256==$ledger_hash and
          (.rows|all(.[];validrow)) and
          (.rows|([.[]|[.resource,.owner_session,.reservation_id,.deadline]]|unique|length)==$row_count) and
          (if $row_index == null then
             any(.rows[];
               .resource==$resource and .owner_session==$owner and
               .reservation_id==$rid and .deadline==$deadline)
           else
             (.rows[$row_index] | .resource==$resource and .owner_session==$owner and
               .reservation_id==$rid and .deadline==$deadline)
           end)
        '''
        proc=subprocess.run([
            'jq','-e','--arg','resource',selected[5],
            '--arg','owner',selected[6],'--arg','rid',selected[7],
            '--argjson','deadline',repr(selected[8]),
            '--argjson','row_count',str(_WBT_COHORT_ROW_COUNT),
            '--argjson','row_index','null' if row_index is None else str(row_index),
            '--arg','counts_path',_WBT_COUNTS_SOURCE_PATH,
            '--arg','counts_hash',_WBT_COUNTS_SOURCE_SHA256,
            '--arg','ledger_path',_WBT_LEDGER_SOURCE_PATH,
            '--arg','ledger_hash',_WBT_LEDGER_SOURCE_SHA256,
            jq_filter],input=payload,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,
            timeout=3,check=False)
        if proc.returncode!=0:
            return 'tuple_not_in_artifact_or_artifact_invalid'
        return 'member' if proc.stdout.strip()==b'true' else 'artifact_invalid'
    except (OSError,subprocess.SubprocessError,ValueError,TypeError):
        return 'artifact_unavailable'

def _wbt_follow_on_manifest_index(selected):
    """Return one exact selected-manifest index, or None on any uncertainty."""
    try:
        fd=os.open(_WBT_FOLLOW_ON_MANIFEST_PATH,
                   os.O_RDONLY|getattr(os,'O_NOFOLLOW',0))
        try:
            info=os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_size!=_WBT_FOLLOW_ON_MANIFEST_SIZE:
                return None
            with os.fdopen(fd,'rb',closefd=False) as stream:
                payload=stream.read(_WBT_FOLLOW_ON_MANIFEST_SIZE+1)
        finally:
            os.close(fd)
        if (len(payload)!=_WBT_FOLLOW_ON_MANIFEST_SIZE or
                hashlib.sha256(payload).hexdigest()!=_WBT_FOLLOW_ON_MANIFEST_SHA256):
            return None
        jq_filter='''
          def exactkeys($expected): (keys|sort)==$expected;
          def valid:
          type=="object" and exactkeys(["candidates","manifest_version","purpose","selection","source"]) and
          .manifest_version==$manifest_version and
          .purpose=="historical candidate selection only; not current eligibility" and
          (.source|type=="object" and (keys|sort)==["artifact","bytes","evidence_kind","row_count","sha256"] and
            .artifact==$artifact and .bytes==$bytes and .evidence_kind=="dated_supported_status_snapshot_not_current" and
            .row_count==$row_count and .sha256==$source_sha256) and
          (.selection|type=="object" and (keys|sort)==["excluded_reservation_id","maximum","order","owner_session","resource_prefix"] and
            .excluded_reservation_id==$excluded_rid and .maximum==8 and .order==["resource","reservation_id"] and
            .owner_session==$owner and .resource_prefix==$prefix) and
          (.candidates|type=="array" and length==3) and
          ([.candidates[].original_index]==$indices) and
          (.candidates|all(.[];type=="object" and
            (keys|sort)==["original_deadline","original_index","owner_session","reservation_id","resource"] and
            (.original_index|type=="number" and floor==.) and
            (.original_deadline|type=="number" and isfinite) and
            .owner_session==$owner and
            (.resource|type=="string" and startswith($prefix)) and
            (.reservation_id|type=="string" and length>0 and .!=$excluded_rid))) and
          ([.candidates[].resource]|unique|length)==3 and
          ([.candidates[].reservation_id]|unique|length)==3 and
          (.candidates==(.candidates|sort_by(.resource,.reservation_id)));
          if valid then
            ([.candidates[]|select(.resource==$resource and .owner_session==$owner and
              .reservation_id==$rid and .original_deadline==$deadline)]|if length==1 then .[0].original_index else false end)
          else false end
        '''
        proc=subprocess.run([
            'jq','-e','--argjson','manifest_version',str(_WBT_FOLLOW_ON_MANIFEST_VERSION),
            '--arg','artifact',_WBT_COHORT_ARTIFACT_PATH,'--argjson','bytes',str(_WBT_COHORT_ARTIFACT_SIZE),
            '--arg','source_sha256',_WBT_COHORT_ARTIFACT_SHA256,
            '--argjson','row_count',str(_WBT_COHORT_ROW_COUNT),
            '--arg','excluded_rid','reservation_viTB1CkNr6BIDqGJ9trNcwxDzaMVHOWg',
            '--arg','owner','session_3d8278d80221401e939587db90280dc1',
            '--arg','prefix','file:.finite-batch-64-0077/',
            '--argjson','indices','[3,4,5]',
            '--arg','resource',selected[5],'--arg','rid',selected[7],
            '--argjson','deadline',repr(selected[8]),jq_filter],
            input=payload,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,
            timeout=3,check=False)
        if proc.returncode!=0:
            return None
        index=int(proc.stdout.strip())
        if index not in _WBT_FOLLOW_ON_ROW_INDICES:
            return None
        return index
    except (OSError,subprocess.SubprocessError,ValueError,TypeError):
        return None

def _wbt_group_manifest_index(selected):
    """Return one exact index from the newly selected finite group, or None."""
    try:
        fd=os.open(_WBT_GROUP_MANIFEST_PATH,os.O_RDONLY|getattr(os,'O_NOFOLLOW',0))
        try:
            info=os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_size!=_WBT_GROUP_MANIFEST_SIZE:
                return None
            with os.fdopen(fd,'rb',closefd=False) as stream:
                payload=stream.read(_WBT_GROUP_MANIFEST_SIZE+1)
        finally:
            os.close(fd)
        if len(payload)!=_WBT_GROUP_MANIFEST_SIZE or hashlib.sha256(payload).hexdigest()!=_WBT_GROUP_MANIFEST_SHA256:
            return None
        jq_filter='''
          def exactkeys($expected): (keys|sort)==$expected;
          def valid:
          type=="object" and exactkeys(["candidates","manifest_version","purpose","selection","source"]) and
          .manifest_version==$manifest_version and
          .purpose=="historical candidate selection only; not current eligibility" and
          (.source|type=="object" and (keys|sort)==["artifact","bytes","evidence_kind","row_count","sha256"] and
            .artifact==$artifact and .bytes==$bytes and .evidence_kind=="dated_supported_status_snapshot_not_current" and
            .row_count==$row_count and .sha256==$source_sha256) and
          (.selection|type=="object" and (keys|sort)==["excluded_reservation_ids","maximum","order","owner_session","resource_prefix"] and
            .excluded_reservation_ids==$excluded_rids and .maximum==8 and .order==["resource","reservation_id"] and
            .owner_session==$owner and .resource_prefix==$prefix) and
          (.candidates|type=="array" and length==5) and
          ([.candidates[].original_index]==$indices) and
          (.candidates|all(.[];type=="object" and
            (keys|sort)==["original_deadline","original_index","owner_session","reservation_id","resource"] and
            (.original_index|type=="number" and floor==.) and
            (.original_deadline|type=="number" and isfinite) and .owner_session==$owner and
            (.resource|type=="string" and startswith($prefix)) and
            (.reservation_id|type=="string" and length>0) and
            ((.reservation_id as $rid | ($excluded_rids|index($rid))==null)))) and
          ([.candidates[].resource]|unique|length)==5 and
          ([.candidates[].reservation_id]|unique|length)==5 and
          (.candidates==(.candidates|sort_by(.resource,.reservation_id)));
          if valid then
            ([.candidates[]|select(.resource==$resource and .owner_session==$owner and
              .reservation_id==$rid and .original_deadline==$deadline)]|if length==1 then .[0].original_index else false end)
          else false end
        '''
        proc=subprocess.run([
            'jq','-e','--argjson','manifest_version',str(_WBT_GROUP_MANIFEST_VERSION),
            '--arg','artifact',_WBT_COHORT_ARTIFACT_PATH,'--argjson','bytes',str(_WBT_COHORT_ARTIFACT_SIZE),
            '--arg','source_sha256',_WBT_COHORT_ARTIFACT_SHA256,'--argjson','row_count',str(_WBT_COHORT_ROW_COUNT),
            '--argjson','excluded_rids','["reservation_viTB1CkNr6BIDqGJ9trNcwxDzaMVHOWg","reservation_TIn07NCQ8NUskc_oYmEzAGsjBzP_lc3m","reservation_aeBpWZroXUUZB3mXtKpx_fnPmCvdVeAr","reservation_-hMAlFaa5CQpbb-dAkq-gFaqjVkVKuaP"]',
            '--arg','owner','session_3d8278d80221401e939587db90280dc1',
            '--arg','prefix','file:.finite-batch-64-0080/',
            '--argjson','indices','[6,7,8,9,10]',
            '--arg','resource',selected[5],'--arg','rid',selected[7],
            '--argjson','deadline',repr(selected[8]),jq_filter],
            input=payload,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,timeout=3,check=False)
        if proc.returncode!=0:
            return None
        index=int(proc.stdout.strip())
        return index if index in _WBT_GROUP_ROW_INDICES else None
    except (OSError,subprocess.SubprocessError,ValueError,TypeError):
        return None

def _wbt_next_group_manifest_index(selected):
    """Return one exact index from the pinned 0081 manifest, or None."""
    try:
        fd=os.open(_WBT_NEXT_GROUP_MANIFEST_PATH,os.O_RDONLY|getattr(os,'O_NOFOLLOW',0))
        try:
            info=os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_size!=_WBT_NEXT_GROUP_MANIFEST_SIZE:
                return None
            with os.fdopen(fd,'rb',closefd=False) as stream:
                payload=stream.read(_WBT_NEXT_GROUP_MANIFEST_SIZE+1)
        finally:
            os.close(fd)
        if len(payload)!=_WBT_NEXT_GROUP_MANIFEST_SIZE or hashlib.sha256(payload).hexdigest()!=_WBT_NEXT_GROUP_MANIFEST_SHA256:
            return None
        jq_filter='''
          def exactkeys($expected): (keys|sort)==$expected;
          def valid_excluded:
            type=="array" and length==9 and
            ([.[].original_index]==[2,3,4,5,6,7,8,9,10]) and
            (all(.[]; type=="object" and
              (exactkeys(["completion_request_id","original_deadline","original_index","owner_session","reservation_id","resource","terminal_status"]) or
               exactkeys(["apply_cli_exit","completion_request_id","original_deadline","original_index","owner_session","reservation_id","resource","terminal_status"])) and
              .terminal_status=="applied" and
              (.completion_request_id|type=="string" and length>0) and
              (.original_deadline|type=="number" and isfinite) and
              .owner_session==$owner and
              (.reservation_id|type=="string" and length>0) and
              (.resource|type=="string") and
              (if .original_index<=5 then (.resource|startswith("file:.finite-batch-64-0077/"))
               else (.resource|startswith("file:.finite-batch-64-0080/")) end) and
              (if has("apply_cli_exit") then .apply_cli_exit==0 else true end))) and
            ([.[].resource]|unique|length)==9 and
            ([.[].reservation_id]|unique|length)==9 and
            ([.[].completion_request_id]|unique|length)==9;
          def valid:
            type=="object" and exactkeys(["candidates","manifest_version","purpose","selection","source"]) and
            .manifest_version==$manifest_version and
            .purpose=="offline finite historical candidate selection only; not current eligibility" and
            (.source|type=="object" and exactkeys(["artifact","bytes","evidence_kind","row_count","sha256"]) and
              .artifact==$artifact and .bytes==$bytes and
              .evidence_kind=="dated_supported_status_snapshot_not_current" and
              .row_count==$row_count and .sha256==$source_sha256) and
            (.selection|type=="object" and exactkeys(["excluded_completed_tuples","excluded_prefixes","maximum","order","owner_session","resource_prefix"]) and
              (.excluded_completed_tuples|valid_excluded) and
              .excluded_prefixes==["file:.finite-batch-64-0077/","file:.finite-batch-64-0080/"] and
              .maximum==8 and .order==["original_index"] and .owner_session==$owner and
              .resource_prefix==$prefix) and
            (.candidates|type=="array" and length==5 and
              ([.[].original_index]==$indices) and
              all(.[];type=="object" and exactkeys(["original_deadline","original_index","owner_session","reservation_id","resource"]) and
                (.original_index|type=="number" and floor==.) and
                (.original_deadline|type=="number" and isfinite) and .owner_session==$owner and
                (.resource|type=="string" and startswith($prefix)) and
                (.reservation_id|type=="string" and length>0)) and
              ([.[].resource]|unique|length)==5 and
              ([.[].reservation_id]|unique|length)==5);
          if valid then
            ([.candidates[]|select(.resource==$resource and .owner_session==$selected_owner and
              .reservation_id==$rid and .original_deadline==$deadline)]|
              if length==1 then .[0].original_index else false end)
          else false end
        '''
        proc=subprocess.run([
            'jq','-e','--argjson','manifest_version',str(_WBT_NEXT_GROUP_MANIFEST_VERSION),
            '--arg','artifact',_WBT_NEXT_GROUP_SOURCE_PATH,'--argjson','bytes',str(_WBT_NEXT_GROUP_SOURCE_SIZE),
            '--arg','source_sha256',_WBT_NEXT_GROUP_SOURCE_SHA256,'--argjson','row_count',str(_WBT_NEXT_GROUP_SOURCE_ROWS),
            '--arg','owner','session_3d8278d80221401e939587db90280dc1',
            '--arg','prefix','file:.finite-batch-64-0081/','--argjson','indices','[11,12,13,14,15]',
            '--arg','resource',selected[5],'--arg','selected_owner',selected[6],
            '--arg','rid',selected[7],'--argjson','deadline',repr(selected[8]),jq_filter],
            input=payload,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,timeout=3,check=False)
        if proc.returncode!=0:
            return None
        index=int(proc.stdout.strip())
        return index if index in _WBT_NEXT_GROUP_ROW_INDICES else None
    except (OSError,subprocess.SubprocessError,ValueError,TypeError):
        return None

def _wbt_file_class_manifest_index(selected):
    """Return one exact index from the pinned same-owner file manifest, or None."""
    try:
        fd=os.open(_WBT_FILE_CLASS_MANIFEST_PATH,
                   os.O_RDONLY|getattr(os,'O_NOFOLLOW',0))
        try:
            info=os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_size!=_WBT_FILE_CLASS_MANIFEST_SIZE:
                return None
            with os.fdopen(fd,'rb',closefd=False) as stream:
                payload=stream.read(_WBT_FILE_CLASS_MANIFEST_SIZE+1)
        finally:
            os.close(fd)
        if (len(payload)!=_WBT_FILE_CLASS_MANIFEST_SIZE or
                hashlib.sha256(payload).hexdigest()!=_WBT_FILE_CLASS_MANIFEST_SHA256):
            return None
        jq_filter='''
          def exactkeys($expected): (keys|sort)==$expected;
          def valid_completed:
            type=="array" and length==14 and
            ([.[].original_index]==[2,3,4,5,6,7,8,9,10,11,12,13,14,15]) and
            all(.[];type=="object" and
              exactkeys(["original_deadline","original_index","outcome","outcome_source","owner_session","reservation_id","resource"]) and
              (.original_deadline|type=="number" and isfinite) and
              .outcome=="applied" and .owner_session==$owner and
              (.reservation_id|type=="string" and length>0) and
              (.resource|type=="string" and startswith("file:")) and
              (if .original_index==2 then .outcome_source=="wbt-abandon-apply-ee58a8aa-7208-41e6-a305-b71376d9f893/root-derived-result.json"
               elif .original_index<=5 then .outcome_source=="023qy-three-row-results-v1.json"
               elif .original_index<=10 then .outcome_source=="3eoW-five-row-results-v1.json"
               else .outcome_source=="q8oj-five-row-results-v1.json" end)) and
            ([.[].resource]|unique|length)==14 and
            ([.[].reservation_id]|unique|length)==14;
          def valid:
            type=="object" and
            exactkeys(["candidates","excluded_completed_tuples","manifest_version","purpose","selection","source"]) and
            .manifest_version==$manifest_version and
            .purpose=="historical candidate selection only; not current eligibility" and
            (.source|type=="object" and exactkeys(["artifact","bytes","evidence_kind","row_count","sha256"]) and
              .artifact==$artifact and .bytes==$bytes and
              .evidence_kind=="dated_supported_status_snapshot_not_current" and
              .row_count==$row_count and .sha256==$source_sha256) and
            (.selection|type=="object" and exactkeys(["maximum","order","owner_session","resource_class"]) and
              .maximum==8 and .order==["original_index"] and .owner_session==$owner and .resource_class=="file:") and
            (.excluded_completed_tuples|valid_completed) and
            (.candidates|type=="array" and length==8 and
              ([.[].original_index]==$indices) and
              all(.[];type=="object" and
                exactkeys(["original_deadline","original_index","owner_session","reservation_id","resource"]) and
                (.original_index|type=="number" and floor==.) and
                (.original_deadline|type=="number" and isfinite) and
                .owner_session==$owner and
                (.resource|type=="string" and startswith("file:") and length>5 and
                  (contains("*")|not) and (contains("?")|not)) and
                (.reservation_id|type=="string" and length>0))) and
              ([.candidates[].resource]|unique|length)==8 and
              ([.candidates[].reservation_id]|unique|length)==8;
          if valid then
            ([.candidates[]|select(.resource==$resource and .owner_session==$selected_owner and
              .reservation_id==$rid and .original_deadline==$deadline)]|
              if length==1 then .[0].original_index else false end)
          else false end
        '''
        proc=subprocess.run([
            'jq','-e','--argjson','manifest_version',str(_WBT_FILE_CLASS_MANIFEST_VERSION),
            '--arg','artifact',_WBT_FILE_CLASS_SOURCE_PATH,
            '--argjson','bytes',str(_WBT_FILE_CLASS_SOURCE_SIZE),
            '--arg','source_sha256',_WBT_FILE_CLASS_SOURCE_SHA256,
            '--argjson','row_count',str(_WBT_FILE_CLASS_SOURCE_ROWS),
            '--arg','owner','session_3d8278d80221401e939587db90280dc1',
            '--argjson','indices','[16,17,18,19,20,21,22,23]',
            '--arg','resource',selected[5],'--arg','selected_owner',selected[6],
            '--arg','rid',selected[7],'--argjson','deadline',repr(selected[8]),jq_filter],
            input=payload,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,timeout=3,check=False)
        if proc.returncode!=0:
            return None
        index=int(proc.stdout.strip())
        return index if index in _WBT_FILE_CLASS_ROW_INDICES else None
    except (OSError,subprocess.SubprocessError,ValueError,TypeError):
        return None

def _wbt_next_file_class_manifest_index(selected):
    """Return one exact new index from the fixed post-v5 file manifest."""
    try:
        fd=os.open(_WBT_NEXT_FILE_CLASS_MANIFEST_PATH,
                   os.O_RDONLY|getattr(os,'O_NOFOLLOW',0))
        try:
            info=os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_size!=_WBT_NEXT_FILE_CLASS_MANIFEST_SIZE:
                return None
            with os.fdopen(fd,'rb',closefd=False) as stream:
                payload=stream.read(_WBT_NEXT_FILE_CLASS_MANIFEST_SIZE+1)
        finally:
            os.close(fd)
        if (len(payload)!=_WBT_NEXT_FILE_CLASS_MANIFEST_SIZE or
                hashlib.sha256(payload).hexdigest()!=_WBT_NEXT_FILE_CLASS_MANIFEST_SHA256):
            return None
        jq_filter='''
          def exactkeys($expected): (keys|sort)==$expected;
          def valid_completed:
            type=="array" and length==22 and
            ([.[].original_index]==[2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23]) and
            all(.[];type=="object" and
              exactkeys(["original_deadline","original_index","outcome","outcome_source","owner_session","reservation_id","resource"]) and
              (.original_deadline|type=="number" and isfinite) and
              .outcome=="applied" and .owner_session==$owner and
              (.reservation_id|type=="string" and length>0) and
              (.resource|type=="string" and startswith("file:")) and
              (if .original_index==2 then .outcome_source=="wbt-abandon-apply-ee58a8aa-7208-41e6-a305-b71376d9f893/root-derived-result.json"
               elif .original_index<=5 then .outcome_source=="023qy-three-row-results-v1.json"
               elif .original_index<=10 then .outcome_source=="3eoW-five-row-results-v1.json"
               elif .original_index<=15 then .outcome_source=="q8oj-five-row-results-v1.json"
               else .outcome_source=="tyXA-eight-row-results-v1.json" end)) and
            ([.[].resource]|unique|length)==22 and
            ([.[].reservation_id]|unique|length)==22;
          def valid:
            type=="object" and
            exactkeys(["candidates","excluded_completed_tuples","manifest_version","purpose","selection","source"]) and
            .manifest_version==$manifest_version and
            .purpose=="historical candidate selection only; not current eligibility" and
            (.source|type=="object" and exactkeys(["artifact","bytes","evidence_kind","row_count","sha256"]) and
              .artifact==$artifact and .bytes==$bytes and
              .evidence_kind=="dated_supported_status_snapshot_not_current" and
              .row_count==$row_count and .sha256==$source_sha256) and
            (.selection|type=="object" and exactkeys(["maximum","order","owner_session","resource_class"]) and
              .maximum==8 and .order==["original_index"] and
              .owner_session==$owner and .resource_class=="file:") and
            (.excluded_completed_tuples|valid_completed) and
            (.candidates|type=="array" and length==8 and
              ([.[].original_index]==$indices) and
              all(.[];type=="object" and
                exactkeys(["original_deadline","original_index","owner_session","reservation_id","resource"]) and
                (.original_index|type=="number" and floor==.) and
                (.original_deadline|type=="number" and isfinite) and
                .owner_session==$owner and
                (.resource|type=="string" and startswith("file:") and length>5 and
                  (contains("*")|not) and (contains("?")|not)) and
                (.reservation_id|type=="string" and length>0))) and
              ([.candidates[].resource]|unique|length)==8 and
              ([.candidates[].reservation_id]|unique|length)==8;
          if valid then
            ([.candidates[]|select(.resource==$resource and .owner_session==$selected_owner and
              .reservation_id==$rid and .original_deadline==$deadline)]|
              if length==1 then .[0].original_index else false end)
          else false end
        '''
        proc=subprocess.run([
            'jq','-e','--argjson','manifest_version',str(_WBT_NEXT_FILE_CLASS_MANIFEST_VERSION),
            '--arg','artifact',_WBT_NEXT_FILE_CLASS_SOURCE_PATH,
            '--argjson','bytes',str(_WBT_NEXT_FILE_CLASS_SOURCE_SIZE),
            '--arg','source_sha256',_WBT_NEXT_FILE_CLASS_SOURCE_SHA256,
            '--argjson','row_count',str(_WBT_NEXT_FILE_CLASS_SOURCE_ROWS),
            '--arg','owner','session_3d8278d80221401e939587db90280dc1',
            '--argjson','indices','[24,25,26,27,28,29,30,31]',
            '--arg','resource',selected[5],'--arg','selected_owner',selected[6],
            '--arg','rid',selected[7],'--argjson','deadline',repr(selected[8]),jq_filter],
            input=payload,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,timeout=3,check=False)
        if proc.returncode!=0:
            return None
        index=int(proc.stdout.strip())
        return index if index in _WBT_NEXT_FILE_CLASS_ROW_INDICES else None
    except (OSError,subprocess.SubprocessError,ValueError,TypeError):
        return None

def _wbt_gg1_file_class_manifest_index(selected):
    """Return one exact productive index from the fixed post-v6 manifest."""
    try:
        fd=os.open(_WBT_GG1_FILE_CLASS_MANIFEST_PATH,
                   os.O_RDONLY|getattr(os,'O_NOFOLLOW',0))
        try:
            info=os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_size!=_WBT_GG1_FILE_CLASS_MANIFEST_SIZE:
                return None
            with os.fdopen(fd,'rb',closefd=False) as stream:
                payload=stream.read(_WBT_GG1_FILE_CLASS_MANIFEST_SIZE+1)
        finally:
            os.close(fd)
        if (len(payload)!=_WBT_GG1_FILE_CLASS_MANIFEST_SIZE or
                hashlib.sha256(payload).hexdigest()!=_WBT_GG1_FILE_CLASS_MANIFEST_SHA256):
            return None
        jq_filter='''
          def exactkeys($expected): (keys|sort)==$expected;
          def valid_completed:
            type=="array" and length==30 and
            ([.[].original_index]==[2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31]) and
            all(.[];type=="object" and
              exactkeys(["original_deadline","original_index","outcome","outcome_source","owner_session","reservation_id","resource"]) and
              (.original_deadline|type=="number" and isfinite) and
              .outcome=="applied" and .owner_session==$owner and
              (.reservation_id|type=="string" and length>0) and
              (.resource|type=="string" and startswith("file:")) and
              (if .original_index==2 then .outcome_source=="wbt-abandon-apply-ee58a8aa-7208-41e6-a305-b71376d9f893/root-derived-result.json"
               elif .original_index<=5 then .outcome_source=="023qy-three-row-results-v1.json"
               elif .original_index<=10 then .outcome_source=="3eoW-five-row-results-v1.json"
               elif .original_index<=15 then .outcome_source=="q8oj-five-row-results-v1.json"
               elif .original_index<=23 then .outcome_source=="tyXA-eight-row-results-v1.json"
               else .outcome_source=="psbx-eight-row-results-v1.json" end)) and
            ([.[].resource]|unique|length)==30 and
            ([.[].reservation_id]|unique|length)==30;
          def valid:
            type=="object" and
            exactkeys(["candidates","excluded_completed_tuples","manifest_version","purpose","selection","source"]) and
            .manifest_version==$manifest_version and
            .purpose=="historical candidate selection only; not current eligibility" and
            (.source|type=="object" and exactkeys(["artifact","bytes","evidence_kind","row_count","sha256"]) and
              .artifact==$artifact and .bytes==$bytes and
              .evidence_kind=="dated_supported_status_snapshot_not_current" and
              .row_count==$row_count and .sha256==$source_sha256) and
            (.selection|type=="object" and exactkeys(["maximum","order","owner_session","resource_class"]) and
              .maximum==8 and .order==["original_index"] and
              .owner_session==$owner and .resource_class=="file:") and
            (.excluded_completed_tuples|valid_completed) and
            (.candidates|type=="array" and length==8 and
              ([.[].original_index]==$indices) and
              all(.[];type=="object" and
                exactkeys(["original_deadline","original_index","owner_session","reservation_id","resource"]) and
                (.original_index|type=="number" and floor==.) and
                (.original_deadline|type=="number" and isfinite) and
                .owner_session==$owner and
                (.resource|type=="string" and startswith("file:") and length>5 and
                  (contains("*")|not) and (contains("?")|not)) and
                (.reservation_id|type=="string" and length>0))) and
              ([.candidates[].resource]|unique|length)==8 and
              ([.candidates[].reservation_id]|unique|length)==8;
          if valid then
            ([.candidates[]|select(.resource==$resource and .owner_session==$selected_owner and
              .reservation_id==$rid and .original_deadline==$deadline)]|
              if length==1 then .[0].original_index else false end)
          else false end
        '''
        proc=subprocess.run([
            'jq','-e','--argjson','manifest_version',str(_WBT_GG1_FILE_CLASS_MANIFEST_VERSION),
            '--arg','artifact',_WBT_GG1_FILE_CLASS_SOURCE_PATH,
            '--argjson','bytes',str(_WBT_GG1_FILE_CLASS_SOURCE_SIZE),
            '--arg','source_sha256',_WBT_GG1_FILE_CLASS_SOURCE_SHA256,
            '--argjson','row_count',str(_WBT_GG1_FILE_CLASS_SOURCE_ROWS),
            '--arg','owner','session_3d8278d80221401e939587db90280dc1',
            '--argjson','indices','[32,33,34,35,36,37,38,39]',
            '--arg','resource',selected[5],'--arg','selected_owner',selected[6],
            '--arg','rid',selected[7],'--argjson','deadline',repr(selected[8]),jq_filter],
            input=payload,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,timeout=3,check=False)
        if proc.returncode!=0:
            return None
        index=int(proc.stdout.strip())
        return index if index in _WBT_GG1_FILE_CLASS_ROW_INDICES else None
    except (OSError,subprocess.SubprocessError,ValueError,TypeError):
        return None

def _wbt_hgide_file_class_manifest_index(selected):
    """Return one exact productive index from the fixed post-v7 manifest."""
    try:
        fd=os.open(_WBT_HGIDE_FILE_CLASS_MANIFEST_PATH,
                   os.O_RDONLY|getattr(os,'O_NOFOLLOW',0))
        try:
            info=os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_size!=_WBT_HGIDE_FILE_CLASS_MANIFEST_SIZE:
                return None
            with os.fdopen(fd,'rb',closefd=False) as stream:
                payload=stream.read(_WBT_HGIDE_FILE_CLASS_MANIFEST_SIZE+1)
        finally:
            os.close(fd)
        if (len(payload)!=_WBT_HGIDE_FILE_CLASS_MANIFEST_SIZE or
                hashlib.sha256(payload).hexdigest()!=_WBT_HGIDE_FILE_CLASS_MANIFEST_SHA256):
            return None
        jq_filter='''
          def exactkeys($expected): (keys|sort)==$expected;
          def valid_completed:
            type=="array" and length==38 and
            ([.[].original_index]==[2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39]) and
            all(.[];type=="object" and
              (if .original_index>=32 then exactkeys(["apply_cli_exit","original_deadline","original_index","outcome","outcome_capture","outcome_request_id","outcome_source","owner_session","reservation_id","resource"]) and (.outcome_request_id|type=="string" and length>0) and (.outcome_capture|type=="string" and length>0) and .apply_cli_exit==0 else exactkeys(["original_deadline","original_index","outcome","outcome_source","owner_session","reservation_id","resource"]) end) and
              (.original_deadline|type=="number" and isfinite) and
              .outcome=="applied" and .owner_session==$owner and
              (.reservation_id|type=="string" and length>0) and
              (.resource|type=="string" and startswith("file:")) and
              (if .original_index==2 then .outcome_source=="wbt-abandon-apply-ee58a8aa-7208-41e6-a305-b71376d9f893/root-derived-result.json"
               elif .original_index<=5 then .outcome_source=="023qy-three-row-results-v1.json"
               elif .original_index<=10 then .outcome_source=="3eoW-five-row-results-v1.json"
               elif .original_index<=15 then .outcome_source=="q8oj-five-row-results-v1.json"
               elif .original_index<=23 then .outcome_source=="tyXA-eight-row-results-v1.json"
               elif .original_index<=31 then .outcome_source=="psbx-eight-row-results-v1.json"
               else .outcome_source==".root-private/root-intake-20261017-2/Gg1-eight-row-results-v1.json" end)) and
            ([.[].resource]|unique|length)==38 and
            ([.[].reservation_id]|unique|length)==38;
          def valid:
            type=="object" and
            exactkeys(["candidates","excluded_completed_tuples","manifest_version","purpose","selection","source"]) and
            .manifest_version==$manifest_version and
            .purpose=="historical candidate selection only; not current eligibility" and
            (.source|type=="object" and exactkeys(["artifact","association","bytes","evidence_kind","row_count","sha256"]) and .association=="message_LQZLiPTothZ8DE76fO2J8f3tdtOO4GLl/attachment_CqD-npf1BMSkUrYalKsrwPRdNrumeY-w" and
              .artifact==$artifact and .bytes==$bytes and
              .evidence_kind=="dated_supported_status_snapshot_not_current" and
              .row_count==$row_count and .sha256==$source_sha256) and
            (.selection|type=="object" and exactkeys(["maximum","order","owner_session","resource_class","scope"]) and
              .maximum==8 and .order==["original_index"] and
              .owner_session==$owner and .resource_class=="file:" and .scope=="first remaining source-index tuples after completed indices 2 through 39") and
            (.excluded_completed_tuples|valid_completed) and
            (.candidates|type=="array" and length==8 and
              ([.[].original_index]==$indices) and
              all(.[];type=="object" and
                exactkeys(["original_deadline","original_index","owner_session","reservation_id","resource"]) and
                (.original_index|type=="number" and floor==.) and
                (.original_deadline|type=="number" and isfinite) and
                .owner_session==$owner and
                (.resource|type=="string" and startswith("file:") and length>5 and
                  (contains("*")|not) and (contains("?")|not)) and
                (.reservation_id|type=="string" and length>0))) and
              ([.candidates[].resource]|unique|length)==8 and
              ([.candidates[].reservation_id]|unique|length)==8;
          if valid then
            ([.candidates[]|select(.resource==$resource and .owner_session==$selected_owner and
              .reservation_id==$rid and .original_deadline==$deadline)]|
              if length==1 then .[0].original_index else false end)
          else false end
        '''
        proc=subprocess.run([
            'jq','-e','--argjson','manifest_version',str(_WBT_HGIDE_FILE_CLASS_MANIFEST_VERSION),
            '--arg','artifact',_WBT_HGIDE_FILE_CLASS_SOURCE_PATH,
            '--argjson','bytes',str(_WBT_HGIDE_FILE_CLASS_SOURCE_SIZE),
            '--arg','source_sha256',_WBT_HGIDE_FILE_CLASS_SOURCE_SHA256,
            '--argjson','row_count',str(_WBT_HGIDE_FILE_CLASS_SOURCE_ROWS),
            '--arg','owner','session_3d8278d80221401e939587db90280dc1',
            '--argjson','indices','[40,41,42,43,44,45,46,47]',
            '--arg','resource',selected[5],'--arg','selected_owner',selected[6],
            '--arg','rid',selected[7],'--argjson','deadline',repr(selected[8]),jq_filter],
            input=payload,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,timeout=3,check=False)
        if proc.returncode!=0:
            return None
        index=int(proc.stdout.strip())
        return index if index in _WBT_HGIDE_FILE_CLASS_ROW_INDICES else None
    except (OSError,subprocess.SubprocessError,ValueError,TypeError):
        return None

def _wbt_6jc1_file_class_manifest_index(selected):
    """Return one exact productive index from the fixed 6JC1 manifest."""
    try:
        fd=os.open(_WBT_6JC1_FILE_CLASS_MANIFEST_PATH,
                   os.O_RDONLY|getattr(os,'O_NOFOLLOW',0))
        try:
            info=os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_size!=_WBT_6JC1_FILE_CLASS_MANIFEST_SIZE:
                return None
            with os.fdopen(fd,'rb',closefd=False) as stream:
                payload=stream.read(_WBT_6JC1_FILE_CLASS_MANIFEST_SIZE+1)
        finally:
            os.close(fd)
        if (len(payload)!=_WBT_6JC1_FILE_CLASS_MANIFEST_SIZE or
                hashlib.sha256(payload).hexdigest()!=_WBT_6JC1_FILE_CLASS_MANIFEST_SHA256):
            return None
        jq_filter='''
          def exactkeys($expected): (keys|sort)==$expected;
          def valid_completed:
            type=="array" and length==46 and
            ([.[].original_index]==[2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47]) and
            all(.[];type=="object" and
              (if .original_index>=32 then exactkeys(["apply_cli_exit","original_deadline","original_index","outcome","outcome_capture","outcome_request_id","outcome_source","owner_session","reservation_id","resource"]) and (.outcome_request_id|type=="string" and length>0) and (.outcome_capture|type=="string" and length>0) and .apply_cli_exit==0 else exactkeys(["original_deadline","original_index","outcome","outcome_source","owner_session","reservation_id","resource"]) end) and
              (.original_deadline|type=="number" and isfinite) and
              .outcome=="applied" and .owner_session==$owner and
              (.reservation_id|type=="string" and length>0) and
              (.resource|type=="string" and startswith("file:")) and
              (if .original_index==2 then .outcome_source=="wbt-abandon-apply-ee58a8aa-7208-41e6-a305-b71376d9f893/root-derived-result.json"
               elif .original_index<=5 then .outcome_source=="023qy-three-row-results-v1.json"
               elif .original_index<=10 then .outcome_source=="3eoW-five-row-results-v1.json"
               elif .original_index<=15 then .outcome_source=="q8oj-five-row-results-v1.json"
               elif .original_index<=23 then .outcome_source=="tyXA-eight-row-results-v1.json"
               elif .original_index<=31 then .outcome_source=="psbx-eight-row-results-v1.json"
               elif .original_index<=39 then .outcome_source==".root-private/root-intake-20261017-2/Gg1-eight-row-results-v1.json"
               else .outcome_source==".root-private/root-intake-20261017-2/Hgide-eight-row-results-v1.json" end)) and
            ([.[].resource]|unique|length)==46 and
            ([.[].reservation_id]|unique|length)==46;
          def valid:
            type=="object" and
            exactkeys(["candidates","excluded_completed_tuples","manifest_version","purpose","selection","source"]) and
            .manifest_version==$manifest_version and
            .purpose=="historical candidate selection only; not current eligibility" and
            (.source|type=="object" and exactkeys(["artifact","association","bytes","evidence_kind","row_count","sha256"]) and
              .association=="message_LQZLiPTothZ8DE76fO2J8f3tdtOO4GLl/attachment_CqD-npf1BMSkUrYalKsrwPRdNrumeY-w" and
              .artifact==$artifact and .bytes==$bytes and
              .evidence_kind=="dated supported status snapshot; not current" and
              .row_count==$row_count and .sha256==$source_sha256) and
            (.selection|type=="object" and exactkeys(["maximum","order","owner_session","resource_class","scope"]) and
              .maximum==8 and .order==["original_index"] and
              .owner_session==$owner and .resource_class=="canonical file: only" and
              .scope=="first remaining original source-index tuples after completed exact indices 2 through 47") and
            (.excluded_completed_tuples|valid_completed) and
            (.candidates|type=="array" and length==8 and
              ([.[].original_index]==$indices) and
              all(.[];type=="object" and
                exactkeys(["original_deadline","original_index","owner_session","reservation_id","resource"]) and
                (.original_index|type=="number" and floor==.) and
                (.original_deadline|type=="number" and isfinite) and
                .owner_session==$owner and
                (.resource|type=="string" and startswith("file:") and length>5 and
                  (contains("*")|not) and (contains("?")|not)) and
                (.reservation_id|type=="string" and length>0))) and
              ([.candidates[].resource]|unique|length)==8 and
              ([.candidates[].reservation_id]|unique|length)==8;
          if valid then
            ([.candidates[]|select(.resource==$resource and .owner_session==$selected_owner and
              .reservation_id==$rid and .original_deadline==$deadline)]|
              if length==1 then .[0].original_index else false end)
          else false end
        '''
        proc=subprocess.run([
            'jq','-e','--argjson','manifest_version',str(_WBT_6JC1_FILE_CLASS_MANIFEST_VERSION),
            '--arg','artifact',_WBT_6JC1_FILE_CLASS_SOURCE_PATH,
            '--argjson','bytes',str(_WBT_6JC1_FILE_CLASS_SOURCE_SIZE),
            '--arg','source_sha256',_WBT_6JC1_FILE_CLASS_SOURCE_SHA256,
            '--argjson','row_count',str(_WBT_6JC1_FILE_CLASS_SOURCE_ROWS),
            '--arg','owner','session_3d8278d80221401e939587db90280dc1',
            '--argjson','indices','[48,49,50,51,52,53,54,55]',
            '--arg','resource',selected[5],'--arg','selected_owner',selected[6],
            '--arg','rid',selected[7],'--argjson','deadline',repr(selected[8]),jq_filter],
            input=payload,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,timeout=3,check=False)
        if proc.returncode!=0:
            return None
        index=int(proc.stdout.strip())
        return index if index in _WBT_6JC1_FILE_CLASS_ROW_INDICES else None
    except (OSError,subprocess.SubprocessError,ValueError,TypeError):
        return None

def _wbt_hqg_file_class_manifest_index(selected):
    """Return one productive index from the fixed historical hqg manifest."""
    try:
        fd=os.open(_WBT_HQG_FILE_CLASS_MANIFEST_PATH,
                   os.O_RDONLY|getattr(os,'O_NOFOLLOW',0))
        try:
            info=os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_size!=_WBT_HQG_FILE_CLASS_MANIFEST_SIZE:
                return None
            with os.fdopen(fd,'rb',closefd=False) as stream:
                payload=stream.read(_WBT_HQG_FILE_CLASS_MANIFEST_SIZE+1)
        finally:
            os.close(fd)
        if (len(payload)!=_WBT_HQG_FILE_CLASS_MANIFEST_SIZE or
                hashlib.sha256(payload).hexdigest()!=_WBT_HQG_FILE_CLASS_MANIFEST_SHA256):
            return None
        jq_filter='''
          def exactkeys($k): (keys|sort)==$k;
          def src($i): if $i<=47 then "6JC1-root-offline-selection-expected"
            else "6JC1-eight-row-results-v1" end;
          def valid_completed:
            exactkeys(["count","entries","indices","unique_reservation_ids","unique_resources"]) and
            .count==54 and .indices==[2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55] and
            .unique_reservation_ids==true and .unique_resources==true and
            (.entries | (type=="array" and length==54 and
              ([.[].original_index]==[2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55]) and
              all(.[]; type=="object" and .terminal_status=="applied" and .owner_session==$owner and
                (.original_deadline|type=="number" and isfinite) and (.reservation_id|type=="string" and length>0) and
                (.resource|type=="string" and startswith("file:")) and .completion_record==src(.original_index) and
                (.completion_request_id|type=="string" and length>0) and
                (if .original_index>=48 then .apply_cli_exit==0 and .prepare_cli_exit==0 else true end)))) and
            ([.entries[].resource]|unique|length)==54 and ([.entries[].reservation_id]|unique|length)==54;
          def valid:
            exactkeys(["claim_or_resource_state","completed_exclusions","current_eligibility","kind","manifest_version","selection","source","source_association","target_file_access"]) and
            .manifest_version==$mv and .kind=="offline_historical_candidate_manifest_only" and
            .claim_or_resource_state=="not evaluated" and .current_eligibility=="not evaluated" and .target_file_access=="none" and
            .source_association=="original cohort plus retained completion outcome records; historical membership only" and
            (.source|exactkeys(["bytes","evidence_kind","path","row_count","sha256"]) and .path==$artifact and
              .bytes==$bytes and .row_count==$rows and .sha256==$sha and .evidence_kind=="dated_supported_status_snapshot_not_current") and
            (.selection|exactkeys(["candidates","owner_session","resource_class","selected_indices","source_order","unique_reservation_ids","unique_resources"]) and
              .owner_session==$owner and .resource_class=="literal canonical file: only" and .selected_indices==$indices and
              .source_order=="original zero-based source index" and .unique_reservation_ids==true and .unique_resources==true and
              (.candidates|(type=="array" and length==8 and ([.[].original_index]==$indices) and
                all(.[];exactkeys(["original_deadline","original_index","owner_session","reservation_id","resource"]) and
                  (.original_deadline|type=="number" and isfinite) and .owner_session==$owner and
                  (.resource|type=="string" and startswith("file:") and length>5 and (contains("*")|not) and (contains("?")|not)) and
                  (.reservation_id|type=="string" and length>0)) and
                ([.[].resource]|unique|length)==8 and ([.[].reservation_id]|unique|length)==8))) and
            (.completed_exclusions|valid_completed) and
            (([.selection.candidates[].resource]+[.completed_exclusions.entries[].resource]|unique|length)==62) and
            (([.selection.candidates[].reservation_id]+[.completed_exclusions.entries[].reservation_id]|unique|length)==62);
          if valid then ([.selection.candidates[]|select(.resource==$resource and .owner_session==$selected_owner and
            .reservation_id==$rid and .original_deadline==$deadline)]|if length==1 then .[0].original_index else false end)
          else false end
        '''
        proc=subprocess.run(['jq','-e','--argjson','mv',str(_WBT_HQG_FILE_CLASS_MANIFEST_VERSION),
            '--arg','artifact',_WBT_HQG_FILE_CLASS_SOURCE_PATH,'--argjson','bytes',str(_WBT_HQG_FILE_CLASS_SOURCE_SIZE),
            '--arg','sha',_WBT_HQG_FILE_CLASS_SOURCE_SHA256,'--argjson','rows',str(_WBT_HQG_FILE_CLASS_SOURCE_ROWS),
            '--arg','owner','session_3d8278d80221401e939587db90280dc1','--argjson','indices','[56,57,58,59,60,61,62,63]',
            '--arg','resource',selected[5],'--arg','selected_owner',selected[6],'--arg','rid',selected[7],
            '--argjson','deadline',repr(selected[8]),jq_filter],input=payload,stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,timeout=3,check=False)
        if proc.returncode!=0: return None
        index=int(proc.stdout.strip())
        return index if index in _WBT_HQG_FILE_CLASS_ROW_INDICES else None
    except (OSError,subprocess.SubprocessError,ValueError,TypeError):
        return None

def _wbt_nwc_resumed_manifest_index(selected):
    """Return a productive index only from the fixed v2 resumed manifest."""
    try:
        fd=os.open(_WBT_NWC_RESUMED_MANIFEST_PATH,
                   os.O_RDONLY|getattr(os,'O_NOFOLLOW',0))
        try:
            info=os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_size!=_WBT_NWC_RESUMED_MANIFEST_SIZE:
                return None
            with os.fdopen(fd,'rb',closefd=False) as stream:
                payload=stream.read(_WBT_NWC_RESUMED_MANIFEST_SIZE+1)
        finally:
            os.close(fd)
        if (len(payload)!=_WBT_NWC_RESUMED_MANIFEST_SIZE or
                hashlib.sha256(payload).hexdigest()!=_WBT_NWC_RESUMED_MANIFEST_SHA256):
            return None
        jq_filter='''
          def exactkeys($k): (keys|sort)==$k;
          def valid_tuple($i): type=="object" and
            exactkeys(["original_deadline","original_index","owner_session","reservation_id","resource"]) and
            .original_index==$i and .owner_session==$owner and
            (.resource|type=="string" and startswith("file:") and length>5 and
              (contains("*")|not) and (contains("?")|not)) and
            (.reservation_id|type=="string" and length>0) and
            (.original_deadline|type=="number" and isfinite and .>0);
          def valid_completed:
            type=="array" and length==63 and
            ([.[].original_index]==[range(2;65)]) and
            all(.[]; type=="object" and .outcome_status=="applied" and
              .owner_session==$owner and (.resource|type=="string" and startswith("file:")) and
              (.reservation_id|type=="string" and length>0) and
              (.original_deadline|type=="number" and isfinite and .>0) and
              (.request_id|type=="string" and length>0) and
              (.outcome_source|type=="string" and length>0));
          if (type=="object" and exactkeys(["completed_exclusion_count","completed_exclusions","evidence_kind","owner_session","resource_class","schema","selected","selected_original_indexes","selection_order","source"]) and
              .schema=="wbt-offline-file-class-candidates-v1" and
              .owner_session==$owner and .resource_class=="literal canonical file: only" and
              .evidence_kind=="Offline historical candidates only; no current eligibility or remaining-count claim" and
              .selection_order=="original source index ascending" and
              .completed_exclusion_count==63 and (.completed_exclusions|valid_completed) and
              .selected_original_indexes==$indices and (.selected|type=="array" and length==7) and
              ([.selected[]|valid_tuple(.original_index)]|all) and
              ([.selected[].resource]|unique|length)==7 and
              ([.selected[].reservation_id]|unique|length)==7 and
              (.source|type=="object" and exactkeys(["association","bytes","path","sha256"]) and
                .path==$source_path and .bytes==$source_bytes and .sha256==$source_sha and
                .association=="original LQZ/CqD cohort")) then
            ([.selected[]|select(.resource==$resource and .owner_session==$selected_owner and
              .reservation_id==$rid and .original_deadline==$deadline)]|
              if length==1 then .[0].original_index else false end)
          else false end
        '''
        proc=subprocess.run(['jq','-e','--arg','owner','session_3d8278d80221401e939587db90280dc1',
            '--argjson','indices','[65,66,67,68,69,70,71]',
            '--arg','source_path',_WBT_NWC_RESUMED_SOURCE_PATH,
            '--argjson','source_bytes',str(_WBT_NWC_RESUMED_SOURCE_SIZE),
            '--arg','source_sha',_WBT_NWC_RESUMED_SOURCE_SHA256,
            '--arg','resource',selected[5],'--arg','selected_owner',selected[6],
            '--arg','rid',selected[7],'--argjson','deadline',repr(selected[8]),jq_filter],
            input=payload,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,timeout=3,check=False)
        if proc.returncode!=0:
            return None
        index=int(proc.stdout.strip())
        return index if index in _WBT_NWC_RESUMED_ROW_INDICES else None
    except (OSError,subprocess.SubprocessError,ValueError,TypeError):
        return None

def _wbt_nwc_resumed_v1_legacy_manifest_index(selected):
    """Recognize only completed index 64 from the immutable v1 manifest."""
    try:
        fd=os.open(_WBT_NWC_RESUMED_V1_MANIFEST_PATH,
                   os.O_RDONLY|getattr(os,'O_NOFOLLOW',0))
        try:
            info=os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_size!=_WBT_NWC_RESUMED_V1_MANIFEST_SIZE:
                return None
            with os.fdopen(fd,'rb',closefd=False) as stream:
                payload=stream.read(_WBT_NWC_RESUMED_V1_MANIFEST_SIZE+1)
        finally:
            os.close(fd)
        if (len(payload)!=_WBT_NWC_RESUMED_V1_MANIFEST_SIZE or
                hashlib.sha256(payload).hexdigest()!=_WBT_NWC_RESUMED_V1_MANIFEST_SHA256):
            return None
        jq_filter='''
          def exactkeys($k): (keys|sort)==$k;
          def valid_tuple($i): type=="object" and
            exactkeys(["original_deadline","original_index","owner_session","reservation_id","resource"]) and
            .original_index==$i and .owner_session==$owner and
            (.resource|type=="string" and startswith("file:") and length>5 and
              (contains("*")|not) and (contains("?")|not)) and
            (.reservation_id|type=="string" and length>0) and
            (.original_deadline|type=="number" and isfinite and .>0);
          def valid_completed:
            type=="array" and length==62 and
            ([.[].original_index]==[range(2;64)]) and
            all(.[]; type=="object" and .outcome_status=="applied" and
              .owner_session==$owner and (.resource|type=="string" and startswith("file:")) and
              (.reservation_id|type=="string" and length>0) and
              (.original_deadline|type=="number" and isfinite and .>0) and
              (.request_id|type=="string" and length>0) and
              (.outcome_source|type=="string" and length>0));
          if (type=="object" and exactkeys(["completed_exclusion_count","completed_exclusions","evidence_kind","owner_session","resource_class","schema","selected","selected_original_indexes","selection_order","source"]) and
              .schema=="wbt-offline-file-class-candidates-v1" and
              .owner_session==$owner and .resource_class=="literal canonical file: only" and
              .evidence_kind=="historical cohort selection only; not current eligibility or current claim count" and
              .selection_order=="original source index ascending" and
              .completed_exclusion_count==62 and (.completed_exclusions|valid_completed) and
              .selected_original_indexes==$indices and (.selected|type=="array" and length==8) and
              ([.selected[]|valid_tuple(.original_index)]|all) and
              ([.selected[].resource]|unique|length)==8 and
              ([.selected[].reservation_id]|unique|length)==8 and
              (.source|type=="object" and exactkeys(["association","bytes","path","sha256"]) and
                .path==$source_path and .bytes==$source_bytes and .sha256==$source_sha and
                .association=="original LQZ/CqD cohort")) then
            ([.selected[]|select(.resource==$resource and .owner_session==$selected_owner and
              .reservation_id==$rid and .original_deadline==$deadline)]|
              if length==1 then .[0].original_index else false end)
          else false end
        '''
        proc=subprocess.run(['jq','-e','--arg','owner','session_3d8278d80221401e939587db90280dc1',
            '--argjson','indices','[64,65,66,67,68,69,70,71]',
            '--arg','source_path',_WBT_NWC_RESUMED_SOURCE_PATH,
            '--argjson','source_bytes',str(_WBT_NWC_RESUMED_SOURCE_SIZE),
            '--arg','source_sha',_WBT_NWC_RESUMED_SOURCE_SHA256,
            '--arg','resource',selected[5],'--arg','selected_owner',selected[6],
            '--arg','rid',selected[7],'--argjson','deadline',repr(selected[8]),jq_filter],
            input=payload,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,timeout=3,check=False)
        if proc.returncode!=0:
            return None
        index=int(proc.stdout.strip())
        return index if index in _WBT_NWC_RESUMED_LEGACY_ROW_INDICES else None
    except (OSError,subprocess.SubprocessError,ValueError,TypeError):
        return None

def _wbt_xu0j_manifest_index(selected):
    """Return one exact productive index from the fixed Xu0j manifest."""
    try:
        fd=os.open(_WBT_XU0J_MANIFEST_PATH,
                   os.O_RDONLY|getattr(os,'O_NOFOLLOW',0))
        try:
            info=os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_size!=_WBT_XU0J_MANIFEST_SIZE:
                return None
            with os.fdopen(fd,'rb',closefd=False) as stream:
                payload=stream.read(_WBT_XU0J_MANIFEST_SIZE+1)
        finally:
            os.close(fd)
        if (len(payload)!=_WBT_XU0J_MANIFEST_SIZE or
                hashlib.sha256(payload).hexdigest()!=_WBT_XU0J_MANIFEST_SHA256):
            return None
        jq_filter='''
          def exactkeys($k): (keys|sort)==$k;
          def valid_selected($i): type=="object" and
            exactkeys(["global_reservation_matches","global_resource_matches","original_deadline","original_index","owner_session","reservation_id","resource"]) and
            .original_index==$i and .owner_session==$owner and
            (.resource|type=="string" and startswith("file:") and length>5 and
              (contains("*")|not) and (contains("?")|not)) and
            (.reservation_id|type=="string" and length>0) and
            (.original_deadline|type=="number" and isfinite and .>0) and
            .global_resource_matches==1 and .global_reservation_matches==1;
          def valid_exclusion:
            type=="object" and .outcome_status=="applied" and
            (.original_index|type=="number" and floor==.) and
            (.resource|type=="string" and startswith("file:")) and
            .owner_session==$owner and
            (.reservation_id|type=="string" and length>0) and
            (.original_deadline|type=="number" and isfinite and .>0) and
            (.request_id|type=="string" and length>0) and
            (.source_association|type=="object" and
              (.file|type=="string" and length>0) and
              (.sha256|type=="string" and length==64) and
              (.json_pointer|type=="string" and length>0));
          if (type=="object" and
              exactkeys(["completed_exclusions","current_scoped_source_prestate","provenance_qualification","purpose","schema","selection","source"]) and
              .schema=="wbt_offline_next_eight_manifest_v1" and
              .purpose=="Historical candidate selection only; not current eligibility or authorization to act." and
              (.source|type=="object" and exactkeys(["association","bytes","path","sha256"]) and
                .path==$source_path and .bytes==$source_bytes and .sha256==$source_sha and
                .association=="Original LQZ/CqD linked classification; 295 rows; dated supported status snapshot, not current.") and
              (.selection|type=="object" and exactkeys(["ordering","owner_session","resource_class","selected","selected_count","selected_original_indexes","uniqueness"]) and
                .owner_session==$owner and .resource_class=="literal canonical file: only" and
                .ordering=="first qualifying rows in original zero-based source-index order" and
                .selected_count==8 and .selected_original_indexes==$indices and
                (.uniqueness|type=="object" and exactkeys(["each_global_resource_and_reservation_occurrence_count","within_selected_reservation_ids_unique","within_selected_resources_unique"]) and
                  .each_global_resource_and_reservation_occurrence_count==1 and
                  .within_selected_reservation_ids_unique==true and .within_selected_resources_unique==true) and
                (.selected|type=="array" and length==8 and
                  ([.[]|valid_selected(.original_index)]|all) and
                  ([.[].original_index]==$indices) and
                  ([.[].resource]|unique|length)==8 and
                  ([.[].reservation_id]|unique|length)==8)) and
              (.completed_exclusions|type=="object" and exactkeys(["count","items","original_indexes","scope"]) and
                .count==70 and .original_indexes==[range(2;72)] and
                (.items|type=="array" and length==70 and
                  ([.[]|valid_exclusion]|all) and
                  ([.[].original_index]==[range(2;72)]) and
                  ([.[].request_id]|unique|length)==70))) then
            ([.selection.selected[]|select(.resource==$resource and .owner_session==$selected_owner and
              .reservation_id==$rid and .original_deadline==$deadline)]|
              if length==1 then .[0].original_index else false end)
          else false end
        '''
        proc=subprocess.run(['jq','-e','--arg','owner','session_3d8278d80221401e939587db90280dc1',
            '--argjson','indices','[72,73,74,75,76,77,78,79]',
            '--arg','source_path',_WBT_NWC_RESUMED_SOURCE_PATH,
            '--argjson','source_bytes',str(_WBT_NWC_RESUMED_SOURCE_SIZE),
            '--arg','source_sha',_WBT_NWC_RESUMED_SOURCE_SHA256,
            '--arg','resource',selected[5],'--arg','selected_owner',selected[6],
            '--arg','rid',selected[7],'--argjson','deadline',repr(selected[8]),jq_filter],
            input=payload,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,timeout=3,check=False)
        if proc.returncode!=0:
            return None
        index=int(proc.stdout.strip())
        return index if index in _WBT_XU0J_ROW_INDICES else None
    except (OSError,subprocess.SubprocessError,ValueError,TypeError):
        return None


def _wbt_n6lb_manifest_index(selected):
    """Return one exact productive index from the fixed n6Lb manifest."""
    try:
        fd=os.open(_WBT_N6LB_MANIFEST_PATH,
                   os.O_RDONLY|getattr(os,'O_NOFOLLOW',0))
        try:
            info=os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_size!=_WBT_N6LB_MANIFEST_SIZE:
                return None
            with os.fdopen(fd,'rb',closefd=False) as stream:
                payload=stream.read(_WBT_N6LB_MANIFEST_SIZE+1)
        finally:
            os.close(fd)
        if (len(payload)!=_WBT_N6LB_MANIFEST_SIZE or
                hashlib.sha256(payload).hexdigest()!=_WBT_N6LB_MANIFEST_SHA256):
            return None
        jq_filter='''
          def exactkeys($k): (keys|sort)==$k;
          def valid_selected($i): type=="object" and
            exactkeys(["global_reservation_matches","global_resource_matches","original_deadline","original_index","owner_session","reservation_id","resource"]) and
            .original_index==$i and .owner_session==$owner and
            (.resource|type=="string" and startswith("file:") and length>5 and
              (contains("*")|not) and (contains("?")|not)) and
            (.reservation_id|type=="string" and length>0) and
            (.original_deadline|type=="number" and isfinite and .>0) and
            .global_resource_matches==1 and .global_reservation_matches==1;
          def valid_exclusion:
            type=="object" and .outcome_status=="applied" and
            (.original_index|type=="number" and floor==.) and
            (.resource|type=="string" and startswith("file:")) and
            .owner_session==$owner and
            (.reservation_id|type=="string" and length>0) and
            (.original_deadline|type=="number" and isfinite and .>0) and
            (.request_id|type=="string" and length>0) and
            (.source_association|type=="object" and
              (.file|type=="string" and length>0) and
              (.sha256|type=="string" and length==64) and
              (.json_pointer|type=="string" and length>0));
          if (type=="object" and
              exactkeys(["completed_exclusions","current_scoped_source_prestate","provenance_qualification","purpose","schema","selection","source"]) and
              .schema=="wbt_offline_next_eight_manifest_v1" and
              .purpose=="Historical candidate selection only; not current eligibility or authorization to act." and
              (.source|type=="object" and exactkeys(["association","bytes","path","sha256"]) and
                .path==$source_path and .bytes==$source_bytes and .sha256==$source_sha and
                .association=="Original LQZ/CqD linked classification; 295 rows; dated supported status snapshot, not current.") and
              (.selection|type=="object" and exactkeys(["ordering","owner_session","resource_class","selected","selected_count","selected_original_indexes","uniqueness"]) and
                .owner_session==$owner and .resource_class=="literal canonical file: only" and
                .ordering=="first qualifying rows in original zero-based source-index order" and
                .selected_count==8 and .selected_original_indexes==$indices and
                (.uniqueness|type=="object" and exactkeys(["each_global_resource_and_reservation_occurrence_count","within_selected_reservation_ids_unique","within_selected_resources_unique"]) and
                  .each_global_resource_and_reservation_occurrence_count==true and
                  .within_selected_reservation_ids_unique==true and .within_selected_resources_unique==true) and
                (.selected|type=="array" and length==8 and
                  ([.[]|valid_selected(.original_index)]|all) and
                  ([.[].original_index]==$indices) and
                  ([.[].resource]|unique|length)==8 and
                  ([.[].reservation_id]|unique|length)==8)) and
              (.completed_exclusions|type=="object" and exactkeys(["count","items","original_indexes","scope"]) and
                .count==78 and .original_indexes==[range(2;80)] and
                (.items|type=="array" and length==78 and
                  ([.[]|valid_exclusion]|all) and
                  ([.[].original_index]==[range(2;80)]) and
                  ([.[].request_id]|unique|length)==78))) then
            ([.selection.selected[]|select(.resource==$resource and .owner_session==$selected_owner and
              .reservation_id==$rid and .original_deadline==$deadline)]|
              if length==1 then .[0].original_index else false end)
          else false end
        '''
        proc=subprocess.run(['jq','-e','--arg','owner','session_3d8278d80221401e939587db90280dc1',
            '--argjson','indices','[80,81,82,83,92,93,94,95]',
            '--arg','source_path',_WBT_NWC_RESUMED_SOURCE_PATH,
            '--argjson','source_bytes',str(_WBT_NWC_RESUMED_SOURCE_SIZE),
            '--arg','source_sha',_WBT_NWC_RESUMED_SOURCE_SHA256,
            '--arg','resource',selected[5],'--arg','selected_owner',selected[6],
            '--arg','rid',selected[7],'--argjson','deadline',repr(selected[8]),jq_filter],
            input=payload,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,timeout=3,check=False)
        if proc.returncode!=0:
            return None
        index=int(proc.stdout.strip())
        return index if index in _WBT_N6LB_ROW_INDICES else None
    except (OSError,subprocess.SubprocessError,ValueError,TypeError):
        return None

def _wbt_witness_projection(state,row,queue,auth,selected,observed_at):
    guards=state[11]
    open_guard_count=sum(1 for entry in guards if len(entry)>8 and entry[8] is None)
    queue_snapshot=tuple(tuple(item) for item in queue)
    return [
        ('status','observed'),
        ('cohort_ref',_WBT_COHORT_REF),
        ('cohort_membership_basis','local_pinned_artifact_sha256_and_exact_tuple'),
        ('artifact_byte_identity','MATCH_TO_REVIEWED_PINNED_DATED_SOURCE_BYTES'),
        ('artifact_receipt_validation','OFFLINE_CUSTODY_CHAIN_REVIEWED_NOT_REAUTHENTICATED_PER_CALL'),
        ('project',selected[4]),
        ('resource',selected[5]),
        ('owner_session',selected[6]),
        ('reservation_id',selected[7]),
        ('expected_deadline',selected[8]),
        ('observed_at',observed_at),
        ('observed_deadline',row['deadline']),
        ('reservation_stale',row['stale']),
        ('deadline_is_past',row['deadline']<observed_at),
        ('bound_thread_id',state[9][0]),
        ('registered_host_id',state[10][0]),
        ('configured_root_host_id',selected[3]),
        ('guard_generation',guards[0][1]),
        ('open_guard_count',open_guard_count),
        ('guard_snapshot_digest',digest(guards)),
        ('fifo_count',len(queue_snapshot)),
        ('fifo_is_empty',not queue_snapshot),
        ('fifo_snapshot_digest',digest(queue_snapshot)),
        ('project_barrier_snapshot_digest',digest(state[1])),
        ('operator_authority_digest',digest(auth)),
        ('preview_digest',digest((state,auth,_WBT_COHORT_REF))),
        ('durable_request','NONE; repeated calls recompute the current snapshot'),
        ('eligibility','NOT_DEMONSTRATED'),
        ('process_closure','UNVERIFIED'),
    ]

def _wbt_unknown_projection(selected,reason='exact_row_or_complete_snapshot_unavailable'):
    return [
        ('status','unknown'),('reason_code',reason),('cohort_ref',_WBT_COHORT_REF),
        ('artifact_byte_identity','NOT_CONFIRMED_BY_THIS_READ'),
        ('project',selected[4]),('resource',selected[5]),
        ('owner_session',selected[6]),('reservation_id',selected[7]),
        ('eligibility','NOT_DEMONSTRATED'),('process_closure','UNVERIFIED'),
        ('durable_request','NONE; repeated calls recompute the current snapshot'),
    ]


def _wbt_diagnosis_projection(check, sampled_at):
    return [
        ('status', check['classification']),
        ('request_id', check['request_id'] or ''),
        ('check_code', check['check_code']),
        ('sampled_at', sampled_at),
    ]


def _execute_wbt_currentness_diagnosis(*, master_db, service_plist, project, resource,
                                       owner_session, reservation_id, expected_deadline,
                                       operator_message_id, cohort_ref):
    """Diagnose the existing WBT Root-history gate without opening its target ledger."""
    root = load_wbt_diagnosis_configured_root(master_db, service_plist)
    if root.home_project != 'default':
        raise CoordError('configured canonical Root HOME is not the pinned default scope')
    auth = authority(root, service_plist, project, operator_message_id)
    selected = wbt_witness_selection(root, project, resource, owner_session,
                                     reservation_id, expected_deadline, cohort_ref)
    if _wbt_pinned_artifact_membership(selected) != 'member':
        return _wbt_diagnosis_projection({
            'classification': 'unknown', 'request_id': None,
            'check_code': 'fixed_cohort_tuple_not_proven'}, time.time())

    registry = _wbt_readonly_connection(root.registry)
    if registry is None:
        return _wbt_diagnosis_projection({
            'classification': 'unknown', 'request_id': None,
            'check_code': 'configured_registry_unavailable'}, time.time())
    with contextlib.closing(registry):
        registry.row_factory = sqlite3.Row
        try:
            registry.execute('BEGIN')
            current = registry.execute('''SELECT home_project,home_session,host_id,thread_id,mode
                FROM root_config WHERE singleton=1''').fetchone()
            if (not current or (current['home_project'], current['home_session'], current['host_id'])
                    != (root.home_project, root.home_session, root.host_id)):
                raise CoordError('configured Root generation changed')
            if current['mode'] != 'running':
                return _wbt_diagnosis_projection({
                    'classification': 'unknown', 'request_id': None,
                    'check_code': 'configured_root_not_running'}, time.time())
            _project_resumed_stop_context(root, registry, 'default', root.master)
            sampled_at = time.time()
            check = _wbt_retired_pending_intents_check(registry, current)
        except CoordError:
            raise
        except (sqlite3.Error, RuntimeError, TypeError, ValueError):
            return _wbt_diagnosis_projection({
                'classification': 'unknown', 'request_id': None,
                'check_code': 'configured_registry_currentness_unavailable'}, time.time())
        finally:
            with contextlib.suppress(sqlite3.Error):
                registry.rollback()
    # Keep the validated authority in the same code path without returning or
    # persisting its private digest or the source body.
    del auth
    return _wbt_diagnosis_projection(check, sampled_at)


def _wbt_abandon_trigger_sql():
    identity='''CREATE TRIGGER root_wbt_file_abandon_intent_identity
      BEFORE UPDATE ON root_wbt_file_abandon_intents
      WHEN OLD.request_id<>NEW.request_id OR OLD.payload<>NEW.payload
        OR OLD.payload_digest<>NEW.payload_digest OR OLD.created_at<>NEW.created_at
        OR OLD.expires_at<>NEW.expires_at
        OR NOT ((OLD.state='accepted' AND NEW.state IN ('prepared','blocked','failed','expired','unknown'))
          OR (OLD.state='prepared' AND NEW.state IN ('applying','blocked','failed','expired'))
          OR (OLD.state='applying' AND NEW.state IN ('applied','blocked','failed','expired','unknown')))
      BEGIN SELECT RAISE(ABORT,'WBT abandonment intent identity/transition is immutable'); END'''
    no_intent_delete='''CREATE TRIGGER root_wbt_file_abandon_intent_no_delete
      BEFORE DELETE ON root_wbt_file_abandon_intents
      BEGIN SELECT RAISE(ABORT,'WBT abandonment intent is immutable'); END'''
    no_event_update='''CREATE TRIGGER root_wbt_file_abandon_event_no_update
      BEFORE UPDATE ON root_wbt_file_abandon_events
      BEGIN SELECT RAISE(ABORT,'WBT abandonment journal is append-only'); END'''
    no_event_delete='''CREATE TRIGGER root_wbt_file_abandon_event_no_delete
      BEFORE DELETE ON root_wbt_file_abandon_events
      BEGIN SELECT RAISE(ABORT,'WBT abandonment journal is append-only'); END'''
    return {
        'root_wbt_file_abandon_intent_identity': (_WBT_ABANDON_INTENTS, identity),
        'root_wbt_file_abandon_intent_no_delete': (_WBT_ABANDON_INTENTS, no_intent_delete),
        'root_wbt_file_abandon_event_no_update': (_WBT_ABANDON_EVENTS, no_event_update),
        'root_wbt_file_abandon_event_no_delete': (_WBT_ABANDON_EVENTS, no_event_delete),
    }


def _wbt_abandon_schema(db, *, create=False):
    """Create/validate only the dedicated WBT intent and append-only journal."""
    intent_exists=_has_table(db,_WBT_ABANDON_INTENTS)
    event_exists=_has_table(db,_WBT_ABANDON_EVENTS)
    if intent_exists != event_exists:
        raise CoordError('WBT abandonment journal schema is partial')
    if create and not intent_exists:
        db.execute('''CREATE TABLE root_wbt_file_abandon_intents (
            request_id TEXT PRIMARY KEY NOT NULL,payload TEXT NOT NULL,
            payload_digest TEXT NOT NULL,state TEXT NOT NULL,created_at REAL NOT NULL,
            expires_at REAL NOT NULL,updated_at REAL NOT NULL,prepared_digest TEXT,
            evidence_digest TEXT,reason_code TEXT)''')
        db.execute('''CREATE TABLE root_wbt_file_abandon_events (
            request_id TEXT NOT NULL,seq INTEGER NOT NULL,state TEXT NOT NULL,
            recorded_at REAL NOT NULL,payload_digest TEXT NOT NULL,
            evidence_digest TEXT,reason_code TEXT,
            PRIMARY KEY(request_id,seq))''')
        for name,(_,sql) in _wbt_abandon_trigger_sql().items():
            db.execute(sql)
    if not _has_table(db,_WBT_ABANDON_INTENTS) or not _has_table(db,_WBT_ABANDON_EVENTS):
        raise CoordError('WBT abandonment journal schema is unavailable')
    expected=(
        (_WBT_ABANDON_INTENTS,('request_id','payload','payload_digest','state','created_at',
          'expires_at','updated_at','prepared_digest','evidence_digest','reason_code'),
          ('TEXT','TEXT','TEXT','TEXT','REAL','REAL','REAL','TEXT','TEXT','TEXT'),0),
        (_WBT_ABANDON_EVENTS,('request_id','seq','state','recorded_at','payload_digest',
          'evidence_digest','reason_code'),('TEXT','INTEGER','TEXT','REAL','TEXT','TEXT','TEXT'),1),
    )
    for table,names,types,pk_seq in expected:
        rows=list(db.execute('PRAGMA table_info('+table+')'))
        if (tuple(row['name'] for row in rows)!=names or
            tuple(row['type'].upper() for row in rows)!=types or
            any(row['notnull']!=1 for row in rows[:5 if table==_WBT_ABANDON_EVENTS else 7]) or
            tuple(row['pk'] for row in rows)!=tuple(
                (1 if (table==_WBT_ABANDON_INTENTS and name=='request_id') else
                 (1 if table==_WBT_ABANDON_EVENTS and name=='request_id' else
                  (2 if table==_WBT_ABANDON_EVENTS and name=='seq' else 0)))
                for name in names)):
            raise CoordError('WBT abandonment journal schema is incompatible')
    triggers={row['name']:(row['tbl_name'],_wbt_normalize_trigger_sql(row['sql']))
              for row in db.execute("SELECT name,tbl_name,sql FROM sqlite_master WHERE type='trigger'")}
    for name,(table,sql) in _wbt_abandon_trigger_sql().items():
        if triggers.get(name)!=(table,_wbt_normalize_trigger_sql(sql)):
            raise CoordError('WBT abandonment immutability trigger is not exact')
    if db.execute('SELECT 1 FROM '+_WBT_ABANDON_INTENTS+
                  " WHERE state NOT IN ('accepted','prepared','blocked','failed','expired','applying','applied','unknown') LIMIT 1").fetchone():
        raise CoordError('WBT abandonment intent state is unsupported')


def _wbt_abandon_append_event(db,request_id,state,at,payload_digest,
                              evidence_digest=None,reason_code=None):
    prior=db.execute('SELECT max(seq) FROM '+_WBT_ABANDON_EVENTS+' WHERE request_id=?',
                     (request_id,)).fetchone()[0]
    seq=1 if prior is None else int(prior)+1
    db.execute('INSERT INTO '+_WBT_ABANDON_EVENTS+
        '(request_id,seq,state,recorded_at,payload_digest,evidence_digest,reason_code) VALUES(?,?,?,?,?,?,?)',
        (request_id,seq,state,at,payload_digest,evidence_digest,reason_code))


def _wbt_abandon_existing(db,request_id,payload,payload_digest):
    row=db.execute('SELECT * FROM '+_WBT_ABANDON_INTENTS+' WHERE request_id=?',(request_id,)).fetchone()
    if row is None:
        return None
    if row['payload']!=payload or row['payload_digest']!=payload_digest:
        raise CoordError('WBT abandonment request UUID is bound to a different immutable payload')
    if (row['state'] not in _WBT_ABANDON_STATES or
        not all(type(row[name]) in (int,float) and math.isfinite(row[name]) and row[name]>0
                for name in ('created_at','expires_at','updated_at')) or
        row['expires_at']<=row['created_at']):
        raise CoordError('WBT abandonment request record is malformed')
    events=db.execute('SELECT seq,state,payload_digest FROM '+_WBT_ABANDON_EVENTS+
                      ' WHERE request_id=? ORDER BY seq LIMIT 7',(request_id,)).fetchall()
    if (not events or len(events)>6 or
        tuple(int(event['seq']) for event in events)!=tuple(range(1,len(events)+1)) or
        events[-1]['state']!=row['state'] or
        any(event['payload_digest']!=payload_digest for event in events)):
        raise CoordError('WBT abandonment transition journal is incomplete or mismatched')
    return row


def _wbt_abandon_result(row,*,replayed):
    return [('status',row['state']),('request_id',row['request_id']),
        ('created_at',row['created_at']),('expires_at',row['expires_at']),
        ('prepared_digest',row['prepared_digest'] or ''),
        ('reason_code',row['reason_code'] or ''),('replayed',bool(replayed)),
        ('process_closure','UNVERIFIED'),('eligibility','NOT_DEMONSTRATED')]


def _wbt_abandon_payload(selected,auth,expires_at,*,row_index,
                         admission_mode=None,root_stop_request_id=None):
    if row_index in _WBT_NWC_RESUMED_LEGACY_ROW_INDICES:
        if (admission_mode!='resumed-project-stop-v1' or
                not isinstance(root_stop_request_id,str)):
            raise CoordError('completed resumed WBT rows require the exact resumed-project admission mode')
        return repr(('wbt-file-abandon-v11-resumed-stop-v1',
            _WBT_NWC_RESUMED_V1_MANIFEST_VERSION,_WBT_NWC_RESUMED_V1_MANIFEST_SHA256,
            _WBT_COHORT_REF,_WBT_COHORT_ARTIFACT_SHA256,row_index,selected,auth,
            admission_mode,root_stop_request_id,expires_at,True))
    if row_index in _WBT_XU0J_ROW_INDICES:
        if (admission_mode!='resumed-project-stop-v1' or
                not isinstance(root_stop_request_id,str)):
            raise CoordError('selected WBT rows require the exact resumed-project admission mode')
        return repr(('wbt-file-abandon-v13-resumed-stop-v1',
            _WBT_XU0J_MANIFEST_VERSION,_WBT_XU0J_MANIFEST_SHA256,
            _WBT_COHORT_REF,_WBT_COHORT_ARTIFACT_SHA256,row_index,selected,auth,
            admission_mode,root_stop_request_id,expires_at,True))
    if row_index in _WBT_N6LB_ROW_INDICES:
        if (admission_mode!='resumed-project-stop-v1' or
                not isinstance(root_stop_request_id,str)):
            raise CoordError('selected WBT rows require the exact resumed-project admission mode')
        return repr(('wbt-file-abandon-v14-resumed-stop-v1',
            _WBT_N6LB_MANIFEST_VERSION,_WBT_N6LB_MANIFEST_SHA256,
            _WBT_COHORT_REF,_WBT_COHORT_ARTIFACT_SHA256,row_index,selected,auth,
            admission_mode,root_stop_request_id,expires_at,True))
    if row_index in _WBT_NWC_RESUMED_ROW_INDICES:
        if (admission_mode!='resumed-project-stop-v1' or
                not isinstance(root_stop_request_id,str)):
            raise CoordError('selected WBT rows require the exact resumed-project admission mode')
        return repr(('wbt-file-abandon-v12-resumed-stop-v1',
            _WBT_NWC_RESUMED_MANIFEST_VERSION,_WBT_NWC_RESUMED_MANIFEST_SHA256,
            _WBT_COHORT_REF,_WBT_COHORT_ARTIFACT_SHA256,row_index,selected,auth,
            admission_mode,root_stop_request_id,expires_at,True))
    if row_index==_WBT_ABANDON_COHORT_INDEX:
        # Preserve the exact v1 payload used by the already-applied index-2 request.
        return repr(('wbt-file-abandon-v1',_WBT_COHORT_REF,_WBT_COHORT_ARTIFACT_SHA256,
            _WBT_ABANDON_COHORT_INDEX,selected,auth,expires_at,True))
    if row_index in _WBT_FOLLOW_ON_ROW_INDICES:
        # Preserve exact v2 payloads for same-ID reconciliation of the prior group.
        return repr(('wbt-file-abandon-v2',_WBT_FOLLOW_ON_MANIFEST_VERSION,
            _WBT_FOLLOW_ON_MANIFEST_SHA256,_WBT_COHORT_REF,_WBT_COHORT_ARTIFACT_SHA256,
            row_index,selected,auth,expires_at,True))
    if row_index in _WBT_GROUP_ROW_INDICES:
        return repr(('wbt-file-abandon-v3',_WBT_GROUP_MANIFEST_VERSION,
            _WBT_GROUP_MANIFEST_SHA256,_WBT_COHORT_REF,_WBT_COHORT_ARTIFACT_SHA256,
            row_index,selected,auth,expires_at,True))
    if row_index in _WBT_NEXT_GROUP_ROW_INDICES:
        return repr(('wbt-file-abandon-v4',_WBT_NEXT_GROUP_MANIFEST_VERSION,
            _WBT_NEXT_GROUP_MANIFEST_SHA256,_WBT_COHORT_REF,_WBT_COHORT_ARTIFACT_SHA256,
            row_index,selected,auth,expires_at,True))
    if row_index in _WBT_FILE_CLASS_ROW_INDICES:
        return repr(('wbt-file-abandon-v5',_WBT_FILE_CLASS_MANIFEST_VERSION,
            _WBT_FILE_CLASS_MANIFEST_SHA256,_WBT_COHORT_REF,_WBT_COHORT_ARTIFACT_SHA256,
            row_index,selected,auth,expires_at,True))
    if row_index in _WBT_NEXT_FILE_CLASS_ROW_INDICES:
        return repr(('wbt-file-abandon-v6',_WBT_NEXT_FILE_CLASS_MANIFEST_VERSION,
            _WBT_NEXT_FILE_CLASS_MANIFEST_SHA256,_WBT_COHORT_REF,_WBT_COHORT_ARTIFACT_SHA256,
            row_index,selected,auth,expires_at,True))
    if row_index in _WBT_GG1_FILE_CLASS_ROW_INDICES:
        return repr(('wbt-file-abandon-v7',_WBT_GG1_FILE_CLASS_MANIFEST_VERSION,
            _WBT_GG1_FILE_CLASS_MANIFEST_SHA256,_WBT_COHORT_REF,_WBT_COHORT_ARTIFACT_SHA256,
            row_index,selected,auth,expires_at,True))
    if row_index in _WBT_HGIDE_FILE_CLASS_ROW_INDICES:
        return repr(('wbt-file-abandon-v8',_WBT_HGIDE_FILE_CLASS_MANIFEST_VERSION,
            _WBT_HGIDE_FILE_CLASS_MANIFEST_SHA256,_WBT_COHORT_REF,_WBT_COHORT_ARTIFACT_SHA256,
            row_index,selected,auth,expires_at,True))
    if row_index in _WBT_6JC1_FILE_CLASS_ROW_INDICES:
        return repr(('wbt-file-abandon-v9',_WBT_6JC1_FILE_CLASS_MANIFEST_VERSION,
            _WBT_6JC1_FILE_CLASS_MANIFEST_SHA256,_WBT_COHORT_REF,_WBT_COHORT_ARTIFACT_SHA256,
            row_index,selected,auth,expires_at,True))
    if row_index in _WBT_HQG_FILE_CLASS_ROW_INDICES:
        return repr(('wbt-file-abandon-v10',_WBT_HQG_FILE_CLASS_MANIFEST_VERSION,
            _WBT_HQG_FILE_CLASS_MANIFEST_SHA256,_WBT_COHORT_REF,_WBT_COHORT_ARTIFACT_SHA256,
            row_index,selected,auth,expires_at,True))
    raise CoordError('WBT file row is outside the reviewed finite manifests')


def _wbt_abandon_root(root,service_plist,project,path,auth,root_stop_request_id=None):
    registry=sqlite3.connect(root.registry,timeout=5,isolation_level=None)
    registry.row_factory=sqlite3.Row
    target=None
    try:
        registry.execute('BEGIN IMMEDIATE')
        current=registry.execute('SELECT home_project,home_session,host_id,mode FROM root_config WHERE singleton=1').fetchone()
        if not current or tuple(current)!=(root.home_project,root.home_session,root.host_id,'running'):
            raise CoordError('configured Root generation changed')
        if root_stop_request_id is None:
            _project_resumed_stop_context(root,registry,'default',root.master)
        _recheck_registered_target(root,registry,project,path)
        if not path.is_file():
            raise CoordError('exact registered WBT target store is unavailable')
        target=sqlite3.connect(path.resolve().as_uri()+'?mode=rw',uri=True,
                               timeout=5,isolation_level=None)
        target.row_factory=sqlite3.Row
        target.execute('BEGIN IMMEDIATE')
        if root_stop_request_id is None:
            context=_wbt_active_project_read_context(registry,root,project,path,target)
        else:
            context=_wbt_resumed_project_stop_context(root,registry,project,path,
                target,root_stop_request_id)
        if authority(root,service_plist,project,[_WBT_ABANDON_AUTHORITY_MESSAGE])!=auth:
            raise CoordError('original operator authority changed')
        return registry,target,context
    except BaseException:
        if target is not None:
            with contextlib.suppress(sqlite3.Error): target.rollback()
            target.close()
        with contextlib.suppress(sqlite3.Error): registry.rollback()
        registry.close()
        raise


def _wbt_abandon_scope_context(registry,root,project,path,target,
                              root_stop_request_id=None):
    if root_stop_request_id is None:
        return _wbt_active_project_read_context(registry,root,project,path,target)
    return _wbt_resumed_project_stop_context(root,registry,project,path,target,
                                             root_stop_request_id)


def _wbt_abandon_unlock(registry,target):
    with contextlib.suppress(sqlite3.Error): target.rollback()
    with contextlib.suppress(sqlite3.Error): registry.rollback()
    target.close(); registry.close()


def _wbt_abandon_commit_effect(target):
    """Commit the single target-store effect transaction."""
    target.commit()


def _wbt_abandon_owner_thread(root,service_plist,project,path,auth,owner,
                              root_stop_request_id=None):
    registry,target,_=_wbt_abandon_root(root,service_plist,project,path,auth,
                                        root_stop_request_id)
    try:
        binding=target.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?',(owner,)).fetchone()
        host=target.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?',(owner,)).fetchone()
        if not binding or not binding['thread_id'] or not host or host['host_id']!=root.host_id:
            raise CoordError('exact owner binding or configured local host is unavailable')
        thread_id=binding['thread_id']
        target.rollback(); registry.rollback()
        return thread_id
    finally:
        _wbt_abandon_unlock(registry,target)


def _wbt_abandon_sample_is_passive_empty(sample,thread_id):
    """Require a fresh exact-thread sample with no active turn or queued input."""
    return (isinstance(sample,dict) and sample.get('thread_id')==thread_id and
        sample.get('state') in ('idle','notLoaded') and
        type(sample.get('queue_count')) is int and sample['queue_count']==0 and
        isinstance(sample.get('started_at'),(int,float)) and
        math.isfinite(sample['started_at']) and time.time()-sample['started_at']<=5)


def _wbt_abandon_capture(*,root,service_plist,project,path,selected,auth,
                         request_id,payload,payload_digest,expires_at,
                         root_stop_request_id=None):
    try:
        thread_id=_wbt_abandon_owner_thread(root,service_plist,project,path,auth,
                                            selected[6],root_stop_request_id)
        sample=_sample(thread_id,root.endpoint,strict=True)
    except CoordError:
        return _wbt_abandon_finish_prepare(root,service_plist,project,path,auth,
            request_id,payload,payload_digest,expires_at,state='blocked',
            root_stop_request_id=root_stop_request_id,
            reason_code='passive_native_evidence_unavailable')
    registry,target,_=_wbt_abandon_root(root,service_plist,project,path,auth,
                                        root_stop_request_id)
    try:
        _wbt_abandon_schema(target)
        row=_wbt_abandon_existing(target,request_id,payload,payload_digest)
        if row is None or row['state']!='accepted':
            raise CoordError('WBT abandonment intent changed before preparation')
        if time.time()>=expires_at:
            updated=_wbt_abandon_transition(target,row,'expired',time.time(),None,'intent_expired')
            target.commit(); registry.rollback()
            return _wbt_abandon_result(updated,replayed=False)
        state,resource_row,queue=snapshot(target,root,selected,
            _wbt_abandon_scope_context(registry,root,project,path,target,
                                       root_stop_request_id),
            require_expired=True,allow_open_guards=False,require_complete_guard_schema=True)
        if resource_row['stale']!=1:
            updated=_wbt_abandon_transition(target,row,'blocked',time.time(),None,'claim_not_marked_stale')
            target.commit(); registry.rollback()
            return _wbt_abandon_result(updated,replayed=False)
        if not _wbt_abandon_sample_is_passive_empty(sample,state[9][0]):
            updated=_wbt_abandon_transition(target,row,'blocked',time.time(),None,'native_binding_or_sample_drift')
            target.commit(); registry.rollback()
            return _wbt_abandon_result(updated,replayed=False)
        stable=digest((payload_digest,state,sample['thread_id'],sample['state'],
                       sample['direct_input_available'],sample['queue_count']))
        evidence=digest((state,sample['started_at'],sample['captured_at'],
                         sample['elapsed_ms'],len(queue)))
        updated=_wbt_abandon_transition(target,row,'prepared',time.time(),stable,None,evidence_digest=evidence)
        target.commit(); registry.rollback()
        return _wbt_abandon_result(updated,replayed=False)
    except (CoordError,sqlite3.Error,RuntimeError,TypeError,ValueError):
        # The accepted intent is deliberately retained; an unavailable or
        # malformed passive sample cannot become an application effect.
        target.rollback()
        registry.rollback()
        return _wbt_abandon_finish_prepare(root,service_plist,project,path,auth,
            request_id,payload,payload_digest,expires_at,state='blocked',
            root_stop_request_id=root_stop_request_id,
            reason_code='passive_or_row_preflight_blocked')
    finally:
        if target is not None:
            _wbt_abandon_unlock(registry,target)


def _wbt_abandon_transition(db,row,state,now,prepared_digest,reason_code,*,evidence_digest=None):
    db.execute('UPDATE '+_WBT_ABANDON_INTENTS+
        ' SET state=?,updated_at=?,prepared_digest=?,evidence_digest=?,reason_code=? WHERE request_id=? AND state=?',
        (state,now,prepared_digest,evidence_digest,reason_code,row['request_id'],row['state']))
    if db.execute('SELECT changes()').fetchone()[0]!=1:
        raise CoordError('WBT abandonment state transition lost its compare-and-set')
    _wbt_abandon_append_event(db,row['request_id'],state,now,row['payload_digest'],evidence_digest,reason_code)
    updated=db.execute('SELECT * FROM '+_WBT_ABANDON_INTENTS+' WHERE request_id=?',(row['request_id'],)).fetchone()
    return updated


def _wbt_abandon_finish_prepare(root,service_plist,project,path,auth,request_id,
                                payload,payload_digest,expires_at,*,state,reason_code,
                                root_stop_request_id=None):
    registry,target,_=_wbt_abandon_root(root,service_plist,project,path,auth,
                                        root_stop_request_id)
    try:
        _wbt_abandon_schema(target)
        row=_wbt_abandon_existing(target,request_id,payload,payload_digest)
        if row is None or row['state']!='accepted':
            raise CoordError('WBT abandonment intent changed before preflight result')
        if time.time()>=expires_at: state,reason_code='expired','intent_expired'
        updated=_wbt_abandon_transition(target,row,state,time.time(),None,reason_code)
        target.commit(); registry.rollback()
        return _wbt_abandon_result(updated,replayed=False)
    finally:
        _wbt_abandon_unlock(registry,target)


def _execute_wbt_abandon(*,master_db,service_plist,project,resource,owner_session,
                         reservation_id,expected_deadline,operator_message_id,
                         action,request_id,expected_digest,acknowledge_unverified,
                         cohort_ref,expires_at,admission_mode=None,
                         root_stop_request_id=None):
    try: request_id=str(uuid.UUID(request_id))
    except (ValueError,TypeError,AttributeError):
        raise CoordError('one canonical durable WBT abandonment request UUID required')
    if project!=_WBT_PROJECT_ID or cohort_ref!=_WBT_COHORT_REF:
        raise CoordError('exact configured WBT project and source cohort are required')
    if operator_message_id!=[_WBT_ABANDON_AUTHORITY_MESSAGE]:
        raise CoordError('exact original Root authority message is required')
    if acknowledge_unverified is not True:
        raise CoordError('explicit UNVERIFIED native-closure acknowledgement required')
    if not isinstance(expires_at,(int,float)) or isinstance(expires_at,bool) or not math.isfinite(expires_at):
        raise CoordError('finite explicit UTC expires-at timestamp required')
    if expires_at<=0:
        raise CoordError('positive explicit UTC expires-at timestamp required')
    if admission_mode is not None:
        if admission_mode!='resumed-project-stop-v1':
            raise CoordError('unsupported WBT admission mode')
        try:
            canonical_stop_id=str(uuid.UUID(root_stop_request_id))
        except (ValueError,TypeError,AttributeError) as error:
            raise CoordError('one canonical resumed Root stop request UUID is required') from error
        if canonical_stop_id!=root_stop_request_id:
            raise CoordError('resumed Root stop request UUID must be canonical')
        root_stop_request_id=canonical_stop_id
    elif root_stop_request_id is not None:
        raise CoordError('Root stop request ID requires the dedicated resumed admission mode')
    if action=='wbt-abandon-apply' and (not isinstance(expected_digest,str) or len(expected_digest)!=64
        or any(c not in '0123456789abcdef' for c in expected_digest)):
        raise CoordError('exact prepared-record digest required for WBT abandonment apply')
    if action=='wbt-abandon-prepare' and expected_digest is not None:
        raise CoordError('prepare accepts no prepared-record digest')
    if admission_mode is None:
        root=load_wbt_preview_configured_root(master_db,service_plist)
    else:
        root=_load_wbt_configured_root_base(master_db,service_plist)
    if root.home_project!='default':
        raise CoordError('configured canonical Root HOME is not the pinned default scope')
    auth=authority(root,service_plist,project,operator_message_id)
    if admission_mode is not None:
        scoped=load_wbt_resumed_configured_root(master_db,service_plist,project,
                                                root_stop_request_id)
        if (scoped.home_project,scoped.home_session,scoped.host_id,scoped.master,scoped.registry)!=(
                root.home_project,root.home_session,root.host_id,root.master,root.registry):
            raise CoordError('configured Root identity changed during resumed authorization')
        root=scoped
    selected=wbt_witness_selection(root,project,resource,owner_session,reservation_id,
                                   expected_deadline,cohort_ref)
    legacy_only=False
    resumed_index=(_wbt_n6lb_manifest_index(selected)
                   if admission_mode=='resumed-project-stop-v1' else None)
    if resumed_index is None and admission_mode=='resumed-project-stop-v1':
        resumed_index=_wbt_xu0j_manifest_index(selected)
    resumed_v2_legacy_index=(_wbt_nwc_resumed_manifest_index(selected)
                   if admission_mode=='resumed-project-stop-v1' and resumed_index is None else None)
    resumed_v1_legacy_index=(_wbt_nwc_resumed_v1_legacy_manifest_index(selected)
                   if admission_mode=='resumed-project-stop-v1' and
                   resumed_index is None and resumed_v2_legacy_index is None else None)
    if resumed_index is not None:
        row_index=resumed_index
        if row_index not in _WBT_N6LB_ROW_INDICES:
            # Xu0j (v13) is retained history; only the new n6Lb set is
            # productive under this admission mode.
            legacy_only=True
    elif resumed_v2_legacy_index is not None:
        row_index=resumed_v2_legacy_index
        legacy_only=True
    elif resumed_v1_legacy_index is not None:
        row_index=resumed_v1_legacy_index
        legacy_only=True
    elif _wbt_pinned_artifact_membership(selected,row_index=_WBT_ABANDON_COHORT_INDEX)=='member':
        row_index=_WBT_ABANDON_COHORT_INDEX
        legacy_only=True
    else:
        prior_group_index=_wbt_follow_on_manifest_index(selected)
        if (prior_group_index is not None and
                _wbt_pinned_artifact_membership(selected,row_index=prior_group_index)=='member'):
            row_index=prior_group_index
            legacy_only=True
        else:
            row_index=_wbt_group_manifest_index(selected)
            if row_index is not None and _wbt_pinned_artifact_membership(
                    selected,row_index=row_index)=='member':
                legacy_only=True
            else:
                row_index=_wbt_next_group_manifest_index(selected)
                if (row_index is not None and
                        _wbt_pinned_artifact_membership(selected,row_index=row_index)=='member'):
                    # The v4 manifest is a completed historical set. Preserve
                    # same-ID reconciliation, but never admit a fresh intent.
                    legacy_only=True
            if row_index is None:
                row_index=_wbt_file_class_manifest_index(selected)
                if row_index is None:
                    row_index=_wbt_next_file_class_manifest_index(selected)
                    if row_index is not None:
                        legacy_only=True
                    else:
                        row_index=_wbt_gg1_file_class_manifest_index(selected)
                        if row_index is not None:
                            legacy_only=True
                        else:
                            row_index=_wbt_hgide_file_class_manifest_index(selected)
                            if row_index is not None:
                                # Preserve v8 stored outcomes without reopening
                                # the completed group for new productive work.
                                legacy_only=True
                            else:
                                row_index=_wbt_6jc1_file_class_manifest_index(selected)
                                if row_index is not None:
                                    # The v9 group is completed history: allow
                                    # exact stored-result reconciliation only.
                                    legacy_only=True
                                else:
                                    row_index=_wbt_hqg_file_class_manifest_index(selected)
                                    if row_index is None:
                                        raise CoordError('fixed WBT cohort index and exact claim tuple are not proven')
                else:
                    if _wbt_pinned_artifact_membership(selected,row_index=row_index)!='member':
                        raise CoordError('fixed WBT cohort index and exact claim tuple are not proven')
                    legacy_only=True
            if _wbt_pinned_artifact_membership(selected,row_index=row_index)!='member':
                raise CoordError('fixed WBT cohort index and exact claim tuple are not proven')
    if admission_mode is not None and row_index not in _WBT_N6LB_ROW_INDICES:
        legacy_only=True
    elif admission_mode is None and row_index in (
            _WBT_NWC_RESUMED_LEGACY_ROW_INDICES+_WBT_NWC_RESUMED_ROW_INDICES+
            _WBT_XU0J_ROW_INDICES+_WBT_N6LB_ROW_INDICES):
        raise CoordError('resumed App WBT rows require their dedicated admission mode')
    if _wbt_pinned_artifact_membership(selected,row_index=row_index)!='member':
        raise CoordError('fixed WBT cohort index and exact claim tuple are not proven')
    payload=_wbt_abandon_payload(selected,auth,float(expires_at),row_index=row_index,
        admission_mode=admission_mode,root_stop_request_id=root_stop_request_id)
    payload_digest=hashlib.sha256(payload.encode('utf-8')).hexdigest()
    path=target_path(root,project)
    registry,target,_=_wbt_abandon_root(root,service_plist,project,path,auth,
                                        root_stop_request_id)
    try:
        _require(target,'resources','resource_queue','sessions','bridge_bindings',
                 'remote_session_hosts','guarded_runs','root_team_stops')
        intent_exists=_has_table(target,_WBT_ABANDON_INTENTS)
        event_exists=_has_table(target,_WBT_ABANDON_EVENTS)
        if intent_exists!=event_exists:
            raise CoordError('WBT abandonment journal schema is partial')
        if intent_exists:
            _wbt_abandon_schema(target)
        elif action!='wbt-abandon-prepare':
            raise CoordError('WBT abandonment journal schema is unavailable')
        prior=(_wbt_abandon_existing(target,request_id,payload,payload_digest)
               if intent_exists else None)
        if legacy_only:
            if prior is None:
                raise CoordError('completed historical WBT tuples are same-ID reconciliation only')
            # Legacy outcomes are report-only. Never resample, start a new intent,
            # or continue a formerly prepared action through compatibility.
            return _wbt_abandon_result(prior,replayed=True)
        if prior is not None:
            if action=='wbt-abandon-prepare' or prior['state']!='prepared':
                return _wbt_abandon_result(prior,replayed=True)
            if prior['prepared_digest']!=expected_digest:
                raise CoordError('prepared evidence reference differs from the immutable request')
            if time.time()>=prior['expires_at']:
                expired=_wbt_abandon_transition(target,prior,'expired',time.time(),
                    prior['prepared_digest'],'intent_expired')
                target.commit(); registry.rollback()
                return _wbt_abandon_result(expired,replayed=False)
            applying=_wbt_abandon_transition(target,prior,'applying',time.time(),
                prior['prepared_digest'],None,evidence_digest=prior['evidence_digest'])
            target.commit(); registry.rollback()
            return _wbt_abandon_apply_effect(root,service_plist,project,path,selected,
                auth,request_id,payload,payload_digest,expected_digest,applying,
                root_stop_request_id)
        if action!='wbt-abandon-prepare':
            raise CoordError('exact WBT abandonment request has no prepared intent')
        if audit_schema(target) and target.execute(
                'SELECT 1 FROM '+TABLE+' WHERE request_id=?',(request_id,)).fetchone():
            raise CoordError('request UUID is already bound to a different expired-claim action')
        now=time.time()
        if not now<float(expires_at)<=now+900:
            raise CoordError('new WBT abandonment intent must expire within 900 seconds')
        if not intent_exists:
            _wbt_abandon_schema(target,create=True)
        target.execute('INSERT INTO '+_WBT_ABANDON_INTENTS+
            '(request_id,payload,payload_digest,state,created_at,expires_at,updated_at,prepared_digest,evidence_digest,reason_code) VALUES(?,?,?,?,?,?,?,?,?,?)',
            (request_id,payload,payload_digest,'accepted',now,float(expires_at),now,None,None,None))
        _wbt_abandon_append_event(target,request_id,'accepted',now,payload_digest)
        target.commit(); registry.rollback()
    finally:
        _wbt_abandon_unlock(registry,target)
    return _wbt_abandon_capture(root=root,service_plist=service_plist,project=project,
        path=path,selected=selected,auth=auth,request_id=request_id,payload=payload,
        payload_digest=payload_digest,expires_at=float(expires_at),
        root_stop_request_id=root_stop_request_id)


def _wbt_abandon_apply_effect(root,service_plist,project,path,selected,auth,
        request_id,payload,payload_digest,expected_digest,applying,
        root_stop_request_id=None):
    target=None; registry=None; commit_started=False
    try:
        thread_id=_wbt_abandon_owner_thread(root,service_plist,project,path,auth,
                                            selected[6],root_stop_request_id)
        sample=_sample(thread_id,root.endpoint,strict=True)
    except CoordError:
        return _wbt_abandon_record_apply_block(root,service_plist,project,path,auth,
            request_id,payload,payload_digest,'passive_native_evidence_unavailable',
            root_stop_request_id)
    registry,target,context=_wbt_abandon_root(root,service_plist,project,path,auth,
                                              root_stop_request_id)
    try:
        _wbt_abandon_schema(target)
        row=_wbt_abandon_existing(target,request_id,payload,payload_digest)
        if row is None or row['state']!='applying' or row['prepared_digest']!=expected_digest:
            raise CoordError('WBT abandonment apply state changed before claim transaction')
        if time.time()>=row['expires_at']:
            updated=_wbt_abandon_transition(target,row,'expired',time.time(),expected_digest,'intent_expired')
            target.commit(); registry.rollback()
            return _wbt_abandon_result(updated,replayed=False)
        state,resource_row,queue=snapshot(target,root,selected,context,
            require_expired=True,allow_open_guards=False,require_complete_guard_schema=True)
        if resource_row['stale']!=1 or not _wbt_abandon_sample_is_passive_empty(sample,state[9][0]):
            updated=_wbt_abandon_transition(target,row,'blocked',time.time(),expected_digest,
                'passive_native_or_claim_preflight_changed')
            target.commit(); registry.rollback()
            return _wbt_abandon_result(updated,replayed=False)
        stable=digest((payload_digest,state,sample['thread_id'],sample['state'],
                       sample['direct_input_available'],sample['queue_count']))
        if stable!=expected_digest:
            updated=_wbt_abandon_transition(target,row,'blocked',time.time(),expected_digest,
                'prepared_evidence_changed')
            target.commit(); registry.rollback()
            return _wbt_abandon_result(updated,replayed=False)
        if time.time()>=row['expires_at']:
            updated=_wbt_abandon_transition(target,row,'expired',time.time(),expected_digest,'intent_expired')
            target.commit(); registry.rollback()
            return _wbt_abandon_result(updated,replayed=False)
        audit_schema(target,create=True)
        _wbt_verify_expired_claim_audit_triggers(target)
        now=time.time()
        changed=target.execute("""UPDATE resources SET owner_session=NULL,reservation_id=NULL,
            token=NULL,granted_at=NULL,deadline=NULL,stale=0,reason=?
            WHERE name=? AND owner_session=? AND reservation_id=? AND deadline=? AND token IS ?
            AND stale=1 AND deadline<?""",
            ('Root exact WBT file-claim abandonment; native closure UNVERIFIED',
             selected[5],selected[6],selected[7],selected[8],resource_row['token'],now)).rowcount
        if changed!=1: raise CoordError('exact WBT expired claim compare-and-set failed')
        if tuple(tuple(q) for q in target.execute(
                'SELECT seq,resource,session,queued_at FROM resource_queue WHERE resource=? ORDER BY seq',
                (selected[5],)))!=state[-1]:
            raise CoordError('WBT resource FIFO changed inside abandonment transaction')
        outcome='wbt_file_claim_abandoned_native_unverified'
        values=(request_id,repr((payload,payload_digest,expected_digest,True)),
            selected[4],selected[5],selected[6],selected[7],selected[8],
            root.home_session,root.host_id,repr(state),outcome,now)
        target.execute('INSERT INTO '+TABLE+' VALUES('+','.join('?' for _ in COLUMNS)+')',values)
        event_digest=digest((state,sample['started_at'],sample['captured_at'],sample['elapsed_ms']))
        updated=_wbt_abandon_transition(target,row,'applied',now,expected_digest,
            None,evidence_digest=event_digest)
        commit_started=True
        _wbt_abandon_commit_effect(target); registry.rollback()
        return _wbt_abandon_result(updated,replayed=False)
    except (CoordError,sqlite3.Error,RuntimeError,TypeError,ValueError):
        # Before commit, rollback makes the claim effect definitely absent, so
        # preserve a terminal blocked result. A commit error is ambiguous and
        # leaves the prior durable applying state as the no-retry record.
        if target is not None and not commit_started:
            try:
                target.rollback(); registry.rollback()
                return _wbt_abandon_record_apply_block(root,service_plist,project,path,auth,
                    request_id,payload,payload_digest,'claim_transaction_rejected',
                    root_stop_request_id)
            except (CoordError,sqlite3.Error,RuntimeError,TypeError,ValueError):
                pass
        return _wbt_abandon_result(applying,replayed=False)
    finally:
        if target is not None and registry is not None:
            _wbt_abandon_unlock(registry,target)


def _wbt_verify_expired_claim_audit_triggers(db):
    expected={
      'root_expired_claim_no_update':(TABLE,"""CREATE TRIGGER root_expired_claim_no_update
        BEFORE UPDATE ON root_expired_claim_dispositions
        BEGIN SELECT RAISE(ABORT,'expired claim audit is immutable'); END"""),
      'root_expired_claim_no_delete':(TABLE,"""CREATE TRIGGER root_expired_claim_no_delete
        BEFORE DELETE ON root_expired_claim_dispositions
        BEGIN SELECT RAISE(ABORT,'expired claim audit is immutable'); END"""),
    }
    actual={row['name']:(row['tbl_name'],_wbt_normalize_trigger_sql(row['sql']))
            for row in db.execute("SELECT name,tbl_name,sql FROM sqlite_master WHERE type='trigger'")}
    for name,(table,sql) in expected.items():
        if actual.get(name)!=(table,_wbt_normalize_trigger_sql(sql)):
            raise CoordError('expired claim audit immutability trigger is not exact')


def _wbt_abandon_record_apply_block(root,service_plist,project,path,auth,
                                    request_id,payload,payload_digest,reason_code,
                                    root_stop_request_id=None):
    registry,target,_=_wbt_abandon_root(root,service_plist,project,path,auth,
                                        root_stop_request_id)
    try:
        _wbt_abandon_schema(target)
        row=_wbt_abandon_existing(target,request_id,payload,payload_digest)
        if row is None or row['state']!='applying':
            raise CoordError('WBT abandonment apply journal changed before blocked result')
        updated=_wbt_abandon_transition(target,row,'blocked',time.time(),
                                       row['prepared_digest'],reason_code)
        target.commit(); registry.rollback()
        return _wbt_abandon_result(updated,replayed=False)
    finally:
        _wbt_abandon_unlock(registry,target)

def _wbt_readonly_connection(path):
    """Open an existing Wbt preview store without creating a missing file."""
    try:
        return sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,
                               timeout=5,isolation_level=None)
    except (sqlite3.Error,OSError,TypeError,ValueError):
        return None

def _wbt_active_project_read_context(registry,root,project,target_path,db):
    """Validate the exact current active stop for this one read-only witness."""
    _require(registry,'root_config','root_fresh_requests','root_team_operations',
             'root_team_operation_projects','root_projects')
    config_columns={row['name'] for row in registry.execute('PRAGMA table_info(root_config)')}
    if not {'home_project','home_session','host_id','mode'}<=config_columns:
        raise CoordError('configured Root generation schema is incomplete')
    config=registry.execute('SELECT * FROM root_config WHERE singleton=1').fetchone()
    if (not config or (config['home_project'],config['home_session'],config['host_id'],config['mode'])
            !=(root.home_project,root.home_session,root.host_id,'running')):
        raise CoordError('configured Root generation changed')
    if not _wbt_retired_pending_intents_are_typed_admin_records(registry,config):
        raise CoordError('configured Root fresh-start history changed before WBT read')
    expected=root.master if project=='default' else root.master.parent/(root.master.name+'.projects')/project/'state.sqlite3'
    if target_path!=expected or not target_path.is_file():
        raise CoordError('registered target path changed before WBT read')
    operations=registry.execute('''SELECT id,root_session,created_at FROM root_team_operations
        WHERE resumed_at IS NULL ORDER BY id LIMIT 2''').fetchall()
    if len(operations)!=1:
        raise CoordError('exact active Root stop is unavailable or ambiguous')
    operation=operations[0]
    if operation['root_session']!=root.home_session:
        raise CoordError('active Root stop belongs to another Root')
    scope=registry.execute('''SELECT project_id,root_session,error FROM root_team_operation_projects
        WHERE request_id=? AND project_id=? LIMIT 2''',(operation['id'],project)).fetchall()
    if len(scope)!=1 or scope[0]['error'] is not None:
        raise CoordError('project is not in the exact active Root stop scope')
    enrolled=registry.execute('SELECT session_id,joined FROM root_projects WHERE project_id=?',(project,)).fetchone()
    if (not enrolled or type(enrolled['joined']) is not int or enrolled['joined']!=1
            or enrolled['session_id']!=scope[0]['root_session']):
        raise CoordError('target project Root enrollment changed')
    if not _has_table(db,'root_team_stops'):
        raise CoordError('exact active project stop is unavailable')
    columns={row['name'] for row in db.execute('PRAGMA table_info(root_team_stops)')}
    if not {'id','root_session','active','created_at','resumed_at'}<=columns:
        raise CoordError('project stop-barrier schema is unsupported')
    if db.execute('SELECT 1 FROM root_team_stops WHERE active IS NULL OR active NOT IN (0,1) LIMIT 1').fetchone():
        raise CoordError('project stop-barrier state is unsupported')
    active_stops=db.execute('SELECT id FROM root_team_stops WHERE active=1 ORDER BY id LIMIT 2').fetchall()
    if not active_stops:
        raise CoordError('exact active project stop is absent')
    if len(active_stops)>1:
        raise CoordError('target project has a conflicting active stop')
    if active_stops[0]['id']!=operation['id']:
        raise CoordError('target project has a conflicting active stop')
    stopped=db.execute('''SELECT root_session,active,created_at,resumed_at FROM root_team_stops
        WHERE id=? LIMIT 2''',(operation['id'],)).fetchall()
    if (len(stopped)!=1 or stopped[0]['root_session']!=scope[0]['root_session']
            or type(stopped[0]['active']) is not int or stopped[0]['active']!=1
            or stopped[0]['resumed_at'] is not None):
        raise CoordError('exact active project stop changed')
    operation_created=operation['created_at']
    stop_created=stopped[0]['created_at']
    if (type(operation_created) not in (int,float) or not math.isfinite(operation_created)
            or operation_created<=0 or type(stop_created) not in (int,float)
            or not math.isfinite(stop_created) or stop_created<=0):
        raise CoordError('active project stop timestamps are unsupported')
    return ('active-project-stop-read-v1',operation['id'],root.home_session,
            project,scope[0]['root_session'],operation_created,stop_created,1,None)

def guard_rows(db,resource,rid,*,allow_open=False):
    result=[]
    try:
        result.append(('guard_generation',guard_generation(db)))
    except RuntimeError as error:
        raise CoordError(str(error)) from error
    specs=(('guarded_runs',('run_id','resource','reservation_id','session','pid','pgid','started_at','closed_at')),
           ('remote_guarded_runs',('run_id','resource','reservation_id','session_id','host_id','local_pid','started_at','closed_at','close_evidence_sha256')))
    for table,cols in specs:
        if not _has_table(db,table):
            if table=='guarded_runs':raise CoordError('local guard metadata unavailable')
            continue
        available={r['name'] for r in db.execute('PRAGMA table_info('+table+')')}
        if not set(cols)<=available:raise CoordError('row-bound guard schema unsupported')
        # Resource exclusion depends on every currently open guard; exact-RID
        # audit retains that RID's full history. Closed rows for other RIDs
        # affect neither condition. SQLite producer triggers track intervening
        # guard-table mutations, including open/close ABA changes.
        rows=db.execute('SELECT '+','.join(cols)+' FROM '+table+
            ' WHERE (resource=? AND closed_at IS NULL) OR reservation_id=? ORDER BY run_id LIMIT 2049',
            (resource,rid)).fetchall()
        if len(rows)>2048:raise CoordError('row-bound guard metadata exceeds finite scope')
        if not allow_open and any(r['closed_at'] is None for r in rows):
            raise CoordError('selected resource or reservation has active guarded work')
        result.extend((table,*tuple(r[c] for c in cols)) for r in rows)
        if table=='remote_guarded_runs':
            for r in rows:
                for extra_table,cols2 in (('remote_guard_batches',('run_id','batch_id')),('remote_restoration_runs',('run_id','deadline'))):
                    if _has_table(db,extra_table):
                        extra=db.execute('SELECT '+','.join(cols2)+' FROM '+extra_table+' WHERE run_id=?',(r['run_id'],)).fetchone()
                        result.append((extra_table,r['run_id'],None if extra is None else tuple(extra)))
    return tuple(result)
def snapshot(db,root,selected,project_stop_context=None,*,require_expired=True,
             allow_open_guards=False,require_complete_guard_schema=False):
    _,_,_,_,project,resource,owner,rid,deadline=selected[:9]
    _require(db,'resources','resource_queue','sessions','bridge_bindings','remote_session_hosts','guarded_runs')
    if require_complete_guard_schema and not all(_has_table(db,table) for table in
            ('guarded_runs','remote_guarded_runs','remote_guard_batches','remote_restoration_runs')):
        raise CoordError('complete guarded-work schema unavailable')
    row=db.execute('SELECT name,owner_session,reservation_id,token,granted_at,deadline,stale,reason FROM resources WHERE name=?',(resource,)).fetchone()
    if not row or (row['owner_session'],row['reservation_id'],row['deadline'])!=(owner,rid,deadline):
        raise CoordError('exact registered owner/RID/deadline changed')
    if require_expired and row['deadline']>=time.time():raise CoordError('selected reservation is not expired')
    actor=db.execute('SELECT id,registered_at FROM sessions WHERE id=?',(owner,)).fetchone()
    binding=db.execute('SELECT thread_id,parent_session,agent_path,updated_at FROM bridge_bindings WHERE session_id=?',(owner,)).fetchone()
    host=db.execute('SELECT host_id,registered_at FROM remote_session_hosts WHERE session_id=?',(owner,)).fetchone()
    if not actor or not binding or not binding['thread_id'] or not host or host['host_id']!=root.host_id:
        raise CoordError('current actor registration/thread/owning-host lineage unavailable')
    guards=guard_rows(db,resource,rid,allow_open=allow_open_guards)
    queue=db.execute('SELECT seq,resource,session,queued_at FROM resource_queue WHERE resource=? ORDER BY seq LIMIT 2049',(resource,)).fetchall()
    if len(queue)>2048:raise CoordError('FIFO exceeds finite preview scope')
    token_hash=hashlib.sha256((row['token'] or '').encode()).hexdigest()
    state=(selected,project_stop_context,tuple(row[c] for c in ('name','owner_session','reservation_id')),
        token_hash,row['granted_at'],row['deadline'],row['stale'],row['reason'],
        tuple(actor),tuple(binding),tuple(host),guards,tuple(tuple(r) for r in queue))
    return state,row,queue
def audit_schema(db,create=False):
    if create:
        db.execute('''CREATE TABLE IF NOT EXISTS root_expired_claim_dispositions (
            request_id TEXT PRIMARY KEY NOT NULL,payload TEXT NOT NULL,
            project TEXT NOT NULL,resource TEXT NOT NULL,owner_session TEXT NOT NULL,
            reservation_id TEXT NOT NULL,deadline REAL NOT NULL,
            root_session TEXT NOT NULL,root_host TEXT NOT NULL,snapshot TEXT NOT NULL,
            outcome TEXT NOT NULL,recorded_at REAL NOT NULL)''')
    if not _has_table(db,TABLE):return False
    rows=list(db.execute('PRAGMA table_info('+TABLE+')'))
    if tuple(r['name'] for r in rows)!=COLUMNS:
        raise CoordError('expired-claim audit schema unsupported')
    for r in rows:
        typ='REAL' if r['name'] in ('deadline','recorded_at') else 'TEXT'
        if r['type'].upper()!=typ or r['notnull']!=1 or r['pk']!=int(r['name']=='request_id') or r['dflt_value'] is not None:
            raise CoordError('expired-claim audit schema unsupported')
    if create:
        db.execute("""CREATE TRIGGER IF NOT EXISTS root_expired_claim_no_update
            BEFORE UPDATE ON root_expired_claim_dispositions
            BEGIN SELECT RAISE(ABORT,'expired claim audit is immutable'); END""")
        db.execute("""CREATE TRIGGER IF NOT EXISTS root_expired_claim_no_delete
            BEFORE DELETE ON root_expired_claim_dispositions
            BEGIN SELECT RAISE(ABORT,'expired claim audit is immutable'); END""")
    return True
def execute(*,master_db,service_plist,project,resource,owner_session,reservation_id,
            expected_deadline,operator_message_id,action,request_id=None,
            expected_digest=None,acknowledge_unverified=False,cohort_ref=None,
            expires_at=None,admission_mode=None,root_stop_request_id=None):
    if action not in ('preview','apply','status','wbt-preview','wbt-diagnose',
                      'wbt-abandon-prepare','wbt-abandon-apply',
                      'wbt-resumed-prepare','wbt-resumed-apply'):
        raise CoordError('unsupported action')
    if action in ('wbt-abandon-prepare','wbt-abandon-apply',
                  'wbt-resumed-prepare','wbt-resumed-apply'):
        if expires_at is None:
            raise CoordError('explicit expires-at timestamp required')
        resumed=action in ('wbt-resumed-prepare','wbt-resumed-apply')
        if resumed:
            if admission_mode!='resumed-project-stop-v1' or root_stop_request_id is None:
                raise CoordError('exact resumed admission mode and Root stop request are required')
        elif admission_mode is not None or root_stop_request_id is not None:
            raise CoordError('resumed admission fields require the dedicated resumed action')
        return _execute_wbt_abandon(master_db=master_db,service_plist=service_plist,
            project=project,resource=resource,owner_session=owner_session,
            reservation_id=reservation_id,expected_deadline=expected_deadline,
            operator_message_id=operator_message_id,
            action=('wbt-abandon-prepare' if action=='wbt-resumed-prepare' else
                    'wbt-abandon-apply' if action=='wbt-resumed-apply' else action),
            request_id=request_id,
            expected_digest=expected_digest,acknowledge_unverified=acknowledge_unverified,
            cohort_ref=cohort_ref,expires_at=expires_at,
            admission_mode=admission_mode,root_stop_request_id=root_stop_request_id)
    if admission_mode is not None or root_stop_request_id is not None:
        raise CoordError('resumed admission fields are only valid for the dedicated resumed WBT action')
    if expires_at is not None:
        raise CoordError('expires-at is only valid for the WBT abandonment lifecycle')
    if action in ('wbt-preview','wbt-diagnose'):
        if project!=_WBT_PROJECT_ID or cohort_ref!=_WBT_COHORT_REF:
            raise CoordError('exact configured Wbt project and source cohort are required')
        if request_id is not None or expected_digest is not None or acknowledge_unverified:
            raise CoordError('read-only Wbt action accepts no durable request or apply arguments')
    elif cohort_ref is not None:
        raise CoordError('cohort-ref is only valid for the Wbt exact-row preview')
    if action=='wbt-diagnose':
        return _execute_wbt_currentness_diagnosis(
            master_db=master_db, service_plist=service_plist, project=project,
            resource=resource, owner_session=owner_session,
            reservation_id=reservation_id, expected_deadline=expected_deadline,
            operator_message_id=operator_message_id, cohort_ref=cohort_ref)
    if action=='wbt-preview':
        # Load only the canonical HOME scope first, then validate the original
        # operator authority before opening the fixed App target scope.
        root=load_wbt_preview_configured_root(master_db,service_plist)
        if root.home_project!='default':
            raise CoordError('configured canonical Root HOME is not the pinned default scope')
        auth=authority(root,service_plist,project,operator_message_id)
        selected=wbt_witness_selection(root,project,resource,owner_session,
                                       reservation_id,expected_deadline,cohort_ref)
        membership=_wbt_pinned_artifact_membership(selected)
        if membership!='member':
            return _wbt_unknown_projection(selected,'pinned_artifact_or_exact_tuple_not_proven')
        # Keep the normal Root/default resumed-stop authorization and generation
        # checks, while the WBT-only validator below binds the selected App stop.
        scoped_root=load_wbt_preview_configured_root(master_db,service_plist)
        if (scoped_root.home_project,scoped_root.home_session,scoped_root.host_id)!=(
                root.home_project,root.home_session,root.host_id):
            raise CoordError('configured Root generation changed during scoped authorization')
        root=scoped_root
        selective_project_scope=project
        # The selector is a single fixed enrolled App project; no caller-chosen
        # project can become a foreign-scope oracle.
        selected=wbt_witness_selection(root,project,resource,owner_session,
                                       reservation_id,expected_deadline,cohort_ref)
    else:
        selective_project_scope=project if action in ('preview','apply') else None
        root=load_configured_root(master_db,service_plist,project_scope=selective_project_scope)
        selected=selection(root,project,resource,owner_session,reservation_id,expected_deadline)
        auth=authority(root,service_plist,project,operator_message_id)
    path=target_path(root,project)
    if action not in ('preview','wbt-preview'):
        try:request_id=str(uuid.UUID(request_id))
        except (ValueError,TypeError,AttributeError):raise CoordError('one durable request UUID required')
    if action=='apply' and (not acknowledge_unverified or not isinstance(expected_digest,str) or len(expected_digest)!=64 or any(c not in '0123456789abcdef' for c in expected_digest)):
        raise CoordError('exact preview digest and explicit UNVERIFIED acknowledgement required')
    if action=='wbt-preview':
        registry_connection=_wbt_readonly_connection(root.registry)
        if registry_connection is None:
            return _wbt_unknown_projection(selected,'configured_registry_unavailable')
    else:
        registry_connection=sqlite3.connect(root.registry,timeout=5,isolation_level=None)
    with contextlib.closing(registry_connection) as registry:
        registry.row_factory=sqlite3.Row
        registry.execute('BEGIN IMMEDIATE' if action in ('preview','apply') else 'BEGIN')
        current=registry.execute('SELECT home_project,home_session,host_id FROM root_config WHERE singleton=1').fetchone()
        if not current or tuple(current)!=(root.home_project,root.home_session,root.host_id):
            raise CoordError('configured Root generation changed')
        _recheck_registered_target(root,registry,project,path)
        if action=='wbt-preview':
            target_connection=_wbt_readonly_connection(path)
            if target_connection is None:
                return _wbt_unknown_projection(selected,'target_project_store_unavailable')
        else:
            target_connection=sqlite3.connect(path,timeout=5,isolation_level=None)
        with contextlib.closing(target_connection) as db:
            db.row_factory=sqlite3.Row
            db.execute('BEGIN IMMEDIATE' if action=='apply' else 'BEGIN')
            if action=='wbt-preview':
                project_stop_context=_wbt_active_project_read_context(
                    registry,root,project,path,db)
            else:
                project_stop_context=_recheck_root_barriers(registry,
                    root=root if selective_project_scope is not None else None,
                    project=selective_project_scope,target_path=path if selective_project_scope is not None else None,
                    target_db=db if selective_project_scope is not None else None)
            # Revalidate real configured caller and immutable original operator
            # authority inside the registry/target cooperative lock boundary.
            if authority(root,service_plist,project,operator_message_id)!=auth:
                raise CoordError('operator authority changed')
            prior=None
            if action!='wbt-preview' and audit_schema(db):
                prior=db.execute('SELECT * FROM '+TABLE+' WHERE request_id=?',(request_id,)).fetchone() if request_id else None
            if prior is not None:
                if (prior['project'],prior['resource'],prior['owner_session'],prior['reservation_id'],prior['deadline'],prior['root_session'],prior['root_host'])!=(project,resource,owner_session,reservation_id,expected_deadline,root.home_session,root.host_id):
                    raise CoordError('request UUID is bound to another exact selection')
                if action=='apply' and prior['payload']!=repr((selected,auth,expected_digest,True)):
                    raise CoordError('immutable request replay payload differs')
                return [('request_id',request_id),('outcome',prior['outcome']),('recorded_at',prior['recorded_at']),('replayed',True),('process_closure','UNVERIFIED'),('current_row','NOT_REAPPLIED')]
            if action=='status':raise CoordError('exact request has no durable expired-claim result')
            if action=='wbt-preview':
                observed_at=time.time()
                try:
                    state,row,queue=snapshot(db,root,selected,project_stop_context,
                        require_expired=False,allow_open_guards=True,
                        require_complete_guard_schema=True)
                    if (type(row['stale']) is not int or row['stale'] not in (0,1)
                            or type(row['deadline']) not in (int,float)
                            or not math.isfinite(row['deadline'])):
                        raise CoordError('exact resource row state is unsupported')
                except (CoordError,sqlite3.Error,RuntimeError,TypeError,ValueError):
                    return _wbt_unknown_projection(selected)
                return _wbt_witness_projection(state,row,queue,auth,selected,observed_at)
            state,row,queue=snapshot(db,root,selected,project_stop_context)
            preview_digest=digest((state,auth))
            if action=='preview':
                return [('preview_digest',preview_digest),('resource',resource),('owner_session',owner_session),('reservation_id',reservation_id),('deadline',expected_deadline),('queue_count',len(queue)),*[(('fifo_'+str(q['seq'])),q['session']) for q in queue],('actor','PRESERVED'),('process_closure','UNVERIFIED')]
            if expected_digest!=preview_digest:raise CoordError('row/token/guard/registration/FIFO generation changed; obtain a new exact preview')
            # Never sample/stop the healthy actor, close old guards, clear jobs,
            # synthesize a receipt or modify FIFO. Only this exact expired row.
            audit_schema(db,create=True)
            now=time.time()
            changed=db.execute("""UPDATE resources SET owner_session=NULL,reservation_id=NULL,token=NULL,
                granted_at=NULL,deadline=NULL,stale=0,reason=?
                WHERE name=? AND owner_session=? AND reservation_id=? AND deadline=?
                AND token IS ? AND deadline<?""",
                ('Root exact expired-claim exception; native closure UNVERIFIED',resource,
                 owner_session,reservation_id,expected_deadline,row['token'],now)).rowcount
            if changed!=1:raise CoordError('exact expired claim CAS failed')
            if tuple(tuple(q) for q in db.execute('SELECT seq,resource,session,queued_at FROM resource_queue WHERE resource=? ORDER BY seq',(resource,)))!=state[-1]:
                raise CoordError('FIFO changed inside disposition')
            values=(request_id,repr((selected,auth,expected_digest,True)),project,resource,
                owner_session,reservation_id,expected_deadline,root.home_session,root.host_id,
                repr(state),'expired_claim_invalidated_actor_preserved',now)
            db.execute('INSERT INTO '+TABLE+' VALUES('+','.join('?' for _ in COLUMNS)+')',values)
            db.commit()
            return [('request_id',request_id),('outcome',values[-2]),('recorded_at',now),('queue_count',len(queue)),('actor','PRESERVED'),('token_transfer',False),('process_closure','UNVERIFIED'),('ordinary_admission','REQUIRED')]




def _assert_old_epoch_settled(db,thread,owner):
    # Submission/queue acceptance is not terminal completion. No drain producer is invented.
    checks=(('execution_epoch_handoffs','thread',thread,"(settled_at IS NULL OR outcome<>'rejected')"),
            ('root_native_handoffs','thread_id',thread,'1=1'),
            ('bridge_jobs','thread_id',thread,"(status<>'cancelled' OR queue_id IS NOT NULL)"),
            ('agent_control_requests','target_session',owner,'1=1'),
            ('session_resets','target_session',owner,'1=1'))
    for table,column,value,predicate in checks:
        if _has_table(db,table) and db.execute('SELECT 1 FROM '+table+' WHERE '+column+'=? AND '+predicate+' LIMIT 1',(value,)).fetchone():
            raise CoordError('unsettled old accepted/queued work blocks epoch transition: '+table)

def _prepare_execution_epoch(db, selected, *, request_id, instance_id, workspace,
                             leaf_prefix, gitdir, index_resource, root_generation, host, authority_digest):
    if not db.in_transaction: raise CoordError('Root preparation requires same-store transaction')
    try: request_id=str(uuid.UUID(request_id))
    except (ValueError,TypeError,AttributeError): raise CoordError('one prepared request UUID required') from None
    if any(not isinstance(value,str) or not value or len(value)>2048 or any(ord(c)<32 for c in value) for value in (workspace,leaf_prefix,gitdir,index_resource,instance_id)):
        raise CoordError('bounded exact current scope fields required')
    if not workspace.startswith('workspace:/') or not leaf_prefix.startswith('file:') or not leaf_prefix.endswith('/') or not gitdir.startswith('/'):
        raise CoordError('exact existing current native/leaf/gitdir scope required')
    from .remote_service import resource_name as normalize_resource
    if not index_resource.startswith('file:') or normalize_resource(index_resource)!=index_resource:
        raise CoordError('exact normalized current index resource required')
    enrolled_scope=db.execute('SELECT s.* FROM execution_producer_scopes s JOIN execution_instances i ON i.producer_id=s.producer_id WHERE i.id=?',(instance_id,)).fetchone()
    if not enrolled_scope or any(enrolled_scope[k]!=v for k,v in (('workspace',workspace),('leaf_prefix',leaf_prefix),('gitdir',gitdir),('index_resource',index_resource))):
        raise CoordError('trusted enrolled current scope mapping mismatched or unavailable')
    material=(selected,request_id,instance_id,workspace,leaf_prefix,gitdir,index_resource,enrolled_scope['scope_digest'],root_generation,host,authority_digest)
    payload=digest(material)
    prior=db.execute('SELECT * FROM execution_epoch_prepares WHERE id=?',(request_id,)).fetchone()
    if prior:
        if prior['payload_digest']!=payload: raise CoordError('prepared request replay conflicts')
        _assert_old_epoch_settled(db,prior['old_thread'],prior['old_owner'])
        return prior
    if db.execute("SELECT 1 FROM execution_epoch_prepares WHERE state='prepared' AND (resource=? OR workspace=?)",(selected[5],workspace)).fetchone():
        raise CoordError('another current-domain preparation is active')
    project,resource,owner,rid,deadline=selected[4:9]
    old=db.execute('SELECT * FROM resources WHERE name=?',(resource,)).fetchone()
    binding=db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?',(owner,)).fetchone()
    pinned=db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?',(owner,)).fetchone()
    if not old or (old['owner_session'],old['reservation_id'],old['deadline'])!=(owner,rid,deadline) or deadline>=time.time() or not binding or not pinned or pinned['host_id']!=host:
        raise CoordError('Root preparation exact original allocation/host changed')
    instance=db.execute('SELECT * FROM execution_instances WHERE id=?',(instance_id,)).fetchone()
    run=db.execute('SELECT * FROM remote_guarded_runs WHERE run_id=?',(instance['run_id'],)).fetchone() if instance else None
    row=db.execute('SELECT * FROM resources WHERE name=?',(workspace,)).fetchone()
    producer=db.execute('SELECT * FROM lifecycle_producers WHERE id=?',(instance['producer_id'],)).fetchone() if instance else None
    if (not producer or not instance or instance['resource']!=workspace or not run or run['local_pid']!=0 or run['closed_at'] is not None or
        run['host_id']!=host or producer['host_id']!=host or producer['owner_session']!=instance['owner_session'] or producer['enrolled_at']>run['started_at'] or not row or (row['owner_session'],row['reservation_id'])!=(instance['owner_session'],instance['reservation_id']) or row['deadline']<=time.time()):
        raise CoordError('current producer prelaunch ownership unavailable')
    scope=db.execute('SELECT * FROM execution_producer_scopes WHERE producer_id=?',(producer['id'],)).fetchone()
    if not scope or any(scope[k]!=v for k,v in (('workspace',workspace),('leaf_prefix',leaf_prefix),('gitdir',gitdir),('index_resource',index_resource))):
        raise CoordError('trusted enrolled current scope mapping mismatched or unavailable')
    _assert_old_epoch_settled(db,binding['thread_id'],owner)
    # Source/leaf roles are separate. Never clear them or silently treat native ownership as their grant.
    if db.execute('SELECT 1 FROM resources WHERE (substr(name,1,?)=? OR name=?) AND owner_session IS NOT NULL',(len(leaf_prefix),leaf_prefix,index_resource)).fetchone():
        raise CoordError('current mutable leaf ownership blocks preparation')
    for table in ('guarded_runs','remote_guarded_runs'):
        if not _has_table(db,table): raise CoordError('complete current guard schema unavailable')
        guardrows=db.execute('SELECT * FROM '+table+' WHERE closed_at IS NULL AND (resource=? OR substr(resource,1,?)=? OR resource=?) LIMIT 2049',(workspace,len(leaf_prefix),leaf_prefix,index_resource)).fetchall()
        if len(guardrows)>2048: raise CoordError('current native guard scope exceeds bound')
        for guard in guardrows:
            if table!='remote_guarded_runs' or guard['run_id']!=run['run_id']:
                raise CoordError('current in-flight native/leaf work blocks preparation')
    now=time.time()
    db.execute('INSERT INTO execution_epoch_prepares VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
        (request_id,resource,workspace,leaf_prefix,gitdir,index_resource,scope['scope_digest'],owner,rid,deadline,binding['thread_id'],host,root_generation,instance_id,now,min(now+120,row['deadline']),'prepared',payload))
    db.execute('INSERT INTO execution_epoch_prepare_events VALUES(?,?,?,?)',(request_id,'prepared',now,authority_digest))
    return db.execute('SELECT * FROM execution_epoch_prepares WHERE id=?',(request_id,)).fetchone()

def _execution_epoch_prestate(db, selected, attestation_id, root_generation, host):
    """Read-only bounded CAS projection; no OS or native operations."""
    _,_,_,_,project,resource,owner,rid,deadline=selected[:9]
    row=db.execute('SELECT * FROM resources WHERE name=?',(resource,)).fetchone()
    if not row or (row['owner_session'],row['reservation_id'],row['deadline'])!=(owner,rid,deadline):
        raise CoordError('epoch exact original allocation changed')
    if deadline>=time.time(): raise CoordError('epoch original allocation is not expired')
    binding=db.execute('SELECT thread_id FROM bridge_bindings WHERE session_id=?',(owner,)).fetchone()
    pinned=db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?',(owner,)).fetchone()
    if not binding or not binding['thread_id'] or not pinned or pinned['host_id']!=host:
        raise CoordError('epoch original owner host/thread binding unavailable')
    att=db.execute('SELECT * FROM execution_epoch_attestations WHERE id=?',(attestation_id,)).fetchone()
    if not att or (att['canonical_resource'],att['old_reservation'],att['root_generation'],att['host'])!=(resource,rid,root_generation,host):
        raise CoordError('epoch current authenticated attestation binding mismatch')
    if not 0<=time.time()-att['observed_at']<=5:
        raise CoordError('epoch current producer observation expired')
    guards=guard_rows(db,resource,rid,allow_open=True)
    starts=[]
    for table in ('guarded_runs','remote_guarded_runs'):
        if not _has_table(db,table): raise CoordError('epoch complete original guard schema unavailable')
        for guard in db.execute('SELECT * FROM '+table+' WHERE resource=? AND reservation_id=?',(resource,rid)):
            if guard['closed_at'] is None:
                starts.append(guard['started_at'])
            elif table == 'remote_guarded_runs':
                # An authenticated owner may close its run before this exact
                # allocation is transferred. Retain its ordering evidence;
                # never reopen it or infer physical closure from the epoch.
                evidence = guard['close_evidence_sha256']
                closed = guard['closed_at']
                if (guard['session_id'] != owner or guard['host_id'] != host or
                    not isinstance(evidence, str) or len(evidence) != 64 or
                    any(c not in '0123456789abcdef' for c in evidence) or
                    not isinstance(closed, (int, float)) or not math.isfinite(closed) or
                    not isinstance(guard['started_at'], (int, float)) or
                    not 0 < guard['started_at'] <= closed <= time.time()):
                    raise CoordError('epoch original closed guard evidence unavailable')
                starts.append(guard['started_at'])
        if db.execute('SELECT 1 FROM '+table+' WHERE resource=? AND closed_at IS NULL AND started_at>=?',(resource,att['boot_epoch'])).fetchone():
            raise CoordError('epoch current in-flight guarded work blocks transition')
    if not starts or any(not 0<start<att['boot_epoch'] for start in starts):
        raise CoordError('epoch boot ordering for original guards unavailable')
    sample=db.execute('SELECT * FROM execution_epoch_samples WHERE attestation_id=?',(attestation_id,)).fetchone()
    prepare=db.execute("SELECT * FROM execution_epoch_prepares WHERE id=? AND state='prepared'",(sample['prepare_id'],)).fetchone() if sample else None
    if (not prepare or prepare['expires_at']<=time.time() or prepare['resource']!=resource or prepare['old_reservation']!=rid or prepare['old_owner']!=owner or prepare['old_deadline']!=deadline or prepare['old_thread']!=binding['thread_id'] or
        prepare['root_generation']!=root_generation or prepare['instance_id']!=att['instance_id'] or prepare['host']!=host or
        prepare['workspace']!='workspace:'+att['backing'].rsplit(':',2)[0] or sample['gitdir'].rsplit(':',2)[0]!=prepare['gitdir'] or sample['writer_count']!=0):
        raise CoordError('fresh prepared current-domain zero-writer sample unavailable')
    scope=db.execute('SELECT * FROM execution_producer_scopes WHERE producer_id=?',(att['producer_id'],)).fetchone()
    if (not scope or scope['scope_digest']!=prepare['scope_digest'] or sample['scope_digest']!=scope['scope_digest'] or
        any(scope[k]!=prepare[k] for k in ('workspace','leaf_prefix','gitdir','index_resource'))):
        raise CoordError('trusted current scope mapping drifted')
    _assert_old_epoch_settled(db,binding['thread_id'],owner)
    if db.execute('SELECT 1 FROM resources WHERE (substr(name,1,?)=? OR name=?) AND owner_session IS NOT NULL',(len(prepare['leaf_prefix']),prepare['leaf_prefix'],prepare['index_resource'])).fetchone():
        raise CoordError('current mutable leaf state drifted')
    for table in ('guarded_runs','remote_guarded_runs'):
        if db.execute('SELECT 1 FROM '+table+' WHERE closed_at IS NULL AND (substr(resource,1,?)=? OR resource=?)',(len(prepare['leaf_prefix']),prepare['leaf_prefix'],prepare['index_resource'])).fetchone():
            raise CoordError('current mutable leaf/index guard state drifted')
    instance=db.execute('SELECT * FROM execution_instances WHERE id=?',(att['instance_id'],)).fetchone()
    run=db.execute('SELECT * FROM remote_guarded_runs WHERE run_id=?',(instance['run_id'],)).fetchone() if instance else None
    current=db.execute('SELECT * FROM resources WHERE name=?',(instance['resource'],)).fetchone() if instance else None
    if (not run or not current or run['local_pid']!=0 or run['closed_at'] is not None or
        run['host_id']!=host or (current['owner_session'],current['reservation_id'])!=(att['owner'],run['reservation_id']) or
        current['deadline']<=time.time() or instance['resource']!='workspace:'+att['backing'].rsplit(':',2)[0]):
        raise CoordError('epoch current backing ownership/prelaunch instance unavailable')
    if db.execute('SELECT 1 FROM remote_guarded_runs WHERE resource=? AND closed_at IS NULL AND run_id<>?',(instance['resource'],run['run_id'])).fetchone():
        raise CoordError('epoch current backing has other in-flight work')
    if db.execute('SELECT 1 FROM guarded_runs WHERE resource=? AND closed_at IS NULL',(instance['resource'],)).fetchone():
        raise CoordError('epoch current backing has local in-flight work')
    thread=binding['thread_id']
    pending=[]
    for table,column,columns,value in (
        ('execution_epoch_handoffs','thread','*',thread),
        ('root_native_handoffs','thread_id','*',thread),
        ('bridge_jobs','thread_id','id,thread_id,status,queue_id,created_at,updated_at',thread),
        ('agent_control_requests','target_session','id,target_session,action,status,thread_id,created_at,updated_at',owner),
        ('session_resets','target_session','id,target_session,old_thread_id,new_thread_id,status,queue_id,created_at,updated_at',owner)):
        if _has_table(db,table):
            rows=db.execute('SELECT '+columns+' FROM '+table+' WHERE '+column+'=? ORDER BY 1 LIMIT 2049',(value,)).fetchall()
            if len(rows)>2048: raise CoordError('current managed continuation scope exceeds bound')
            pending.append((table,tuple(tuple(r) for r in rows)))
    queue=tuple(tuple(q) for q in db.execute('SELECT seq,resource,session,queued_at FROM resource_queue WHERE resource=? ORDER BY seq LIMIT 2049',(resource,)))
    if len(queue)>2048: raise CoordError('epoch FIFO exceeds bounded scope')
    state=(selected,tuple(row),tuple(att),tuple(run),tuple(current),guards,queue,root_generation,tuple(prepare),tuple(sample),tuple(pending))
    return digest(state),row,att,thread


def _apply_execution_epoch(db, selected, *, request_id, expected_digest, attestation_id,
                           root_generation, host, authority_digest):
    """Internal effect. Only configured Root wrapper calls it in normal runtime."""
    from .core import execution_epoch_schema
    if not db.in_transaction: raise CoordError('epoch effect requires caller same-store transaction')
    request_id=str(uuid.UUID(request_id))
    material=(selected,request_id,expected_digest,attestation_id,root_generation,host,authority_digest)
    payload=digest(material)
    existing=db.execute('SELECT * FROM execution_epochs WHERE request_id=?',(request_id,)).fetchone()
    if existing:
        if existing['payload_digest']!=payload: raise CoordError('epoch request replay conflicts')
        return [('request_id',request_id),('state','managed_epoch_committed'),('replayed',True),('old_physical_state','UNVERIFIED')]
    observed,row,att,thread=_execution_epoch_prestate(db,selected,attestation_id,root_generation,host)
    if observed!=expected_digest: raise CoordError('epoch prestate/FIFO/guard generation changed')
    sample=db.execute('SELECT prepare_id FROM execution_epoch_samples WHERE attestation_id=?',(attestation_id,)).fetchone()
    if not sample or sample['prepare_id']!=request_id: raise CoordError('Root apply must use the exact prepared request UUID')
    project,resource,owner,rid,deadline=selected[4:9]
    db.execute('INSERT INTO execution_epochs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
        (request_id,resource,project,owner,rid,deadline,thread,host,root_generation,
         attestation_id,att['backing'],att['execution_id'],payload,time.time()))
    # Preserve queue, guards, outcomes and old session/thread histories verbatim.
    changed=db.execute('UPDATE resources SET owner_session=NULL,reservation_id=NULL,token=NULL,granted_at=NULL,deadline=NULL,stale=0,reason=? WHERE name=? AND owner_session=? AND reservation_id=? AND deadline=?',
        ('managed execution epoch; original physical state UNVERIFIED',resource,owner,rid,deadline)).rowcount
    if changed!=1: raise CoordError('epoch exact allocation CAS changed')
    if db.execute("UPDATE execution_epoch_prepares SET state='committed' WHERE id=? AND state='prepared'",(request_id,)).rowcount!=1: raise CoordError('prepared phase changed before epoch commit')
    db.execute('INSERT INTO execution_epoch_prepare_events VALUES(?,?,?,?)',(request_id,'committed',time.time(),authority_digest))
    return [('request_id',request_id),('state','managed_epoch_committed'),('replayed',False),('old_physical_state','UNVERIFIED')]


def execute_execution_epoch(*,master_db,service_plist,project,resource,owner_session,
        reservation_id,expected_deadline,operator_message_id,action,
        attestation_id=None,request_id=None,expected_digest=None,acknowledge_unverified=False,
        instance_id=None,workspace_resource=None,leaf_prefix=None,backing_gitdir=None,index_resource=None):
    """Fixed-installed configured-Root authority; deliberately no HTTP adapter."""
    if action not in ('epoch-prepare','epoch-preview','epoch-apply','epoch-cancel','epoch-status'): raise CoordError('invalid epoch action')
    if action!='epoch-prepare' and any(value is not None for value in (instance_id,workspace_resource,leaf_prefix,backing_gitdir,index_resource)):
        raise CoordError('current scope creation fields require epoch-prepare')
    if action in ('epoch-prepare','epoch-apply') and acknowledge_unverified is not True:
        raise CoordError('epoch apply requires explicit old-physical-state UNVERIFIED acknowledgement')
    root=load_configured_root(master_db,service_plist,project_scope=project)
    auth=authority(root,service_plist,project,operator_message_id)
    selected=selection(root,project,resource,owner_session,reservation_id,expected_deadline)
    from .lifecycle_producer import read_current_boot_id
    current_boot=read_current_boot_id() if action in ('epoch-prepare','epoch-preview','epoch-apply') else None
    generation=digest((root.home_project,root.home_session,root.host_id,current_boot))
    path=target_path(root,project)
    with contextlib.closing(sqlite3.connect(root.registry,timeout=5,isolation_level=None)) as registry:
        registry.row_factory=sqlite3.Row;registry.execute('BEGIN IMMEDIATE')
        actual=registry.execute('SELECT home_project,home_session,host_id FROM root_config WHERE singleton=1').fetchone()
        if not actual or tuple(actual)!=(root.home_project,root.home_session,root.host_id):
            raise CoordError('epoch configured Root generation changed')
        _recheck_registered_target(root,registry,project,path)
        with contextlib.closing(sqlite3.connect(path,timeout=5,isolation_level=None)) as db:
            db.row_factory=sqlite3.Row;db.execute('BEGIN IMMEDIATE')
            try:
                if action in ('epoch-preview','epoch-apply'):
                    att=db.execute('SELECT boot_id FROM execution_epoch_attestations WHERE id=?',(attestation_id,)).fetchone()
                    if not att or att['boot_id']!=current_boot: raise CoordError('current host kernel generation changed')
                if action=='epoch-prepare':
                    row=_prepare_execution_epoch(db,selected,request_id=request_id,instance_id=instance_id,workspace=workspace_resource,
                        leaf_prefix=leaf_prefix,gitdir=backing_gitdir,index_resource=index_resource,root_generation=generation,host=root.host_id,authority_digest=digest(auth))
                    result=[('request_id',row['id']),('state',row['state']),('expires_at',row['expires_at']),('root_generation',generation),('old_physical_state','UNVERIFIED')]
                elif action=='epoch-status':
                    row=db.execute('SELECT * FROM execution_epoch_prepares WHERE id=?',(str(uuid.UUID(request_id)),)).fetchone()
                    if not row or (row['resource'],row['old_owner'],row['old_reservation'],row['old_deadline'])!=(resource,owner_session,reservation_id,expected_deadline):
                        raise CoordError('exact historical prepared selection unavailable')
                    result=[('request_id',row['id']),('state',row['state']),('original_root_generation',row['root_generation']),('old_physical_state','UNVERIFIED')]
                elif action=='epoch-cancel':
                    row=db.execute('SELECT * FROM execution_epoch_prepares WHERE id=?',(str(uuid.UUID(request_id)),)).fetchone()
                    if not row or (row['resource'],row['old_owner'],row['old_reservation'],row['old_deadline'],row['host'])!=(resource,owner_session,reservation_id,expected_deadline,root.host_id) or row['state']=='committed':
                        raise CoordError('exact uncommitted current Root preparation required')
                    if row['state']=='prepared':
                        db.execute("UPDATE execution_epoch_prepares SET state='cancelled' WHERE id=?",(row['id'],))
                        db.execute('INSERT INTO execution_epoch_prepare_events VALUES(?,?,?,?)',(row['id'],'cancelled',time.time(),digest(auth)))
                    result=[('request_id',row['id']),('state','cancelled'),('old_physical_state','UNVERIFIED')]
                elif action=='epoch-preview':
                    stamp,*_=_execution_epoch_prestate(db,selected,attestation_id,generation,root.host_id)
                    result=[('prestate_digest',stamp),('root_generation',generation),('old_physical_state','UNVERIFIED'),('effect_eligibility','managed-current-domain-only; external/shared domains excluded')]
                else:
                    result=_apply_execution_epoch(db,selected,request_id=request_id,expected_digest=expected_digest,
                        attestation_id=attestation_id,root_generation=generation,host=root.host_id,authority_digest=digest(auth))
                db.commit()
            except BaseException:
                db.rollback();raise
        registry.commit()
    return result

def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('action',choices=('preview','apply','status','wbt-preview','wbt-diagnose',
                                     'wbt-abandon-prepare','wbt-abandon-apply',
                                     'wbt-resumed-prepare','wbt-resumed-apply','epoch-prepare','epoch-preview','epoch-apply','epoch-cancel','epoch-status'))
    for name in ('master-db','service-plist','project-id','resource','owner-session','reservation-id','expected-deadline'):
        p.add_argument('--'+name,required=True)
    p.add_argument('--operator-message-id',action='append',required=True)
    p.add_argument('--attestation-id')
    for name in ('instance-id','workspace-resource','leaf-prefix','backing-gitdir','index-resource'): p.add_argument('--'+name)
    p.add_argument('--request-id')
    p.add_argument('--expected-digest')
    p.add_argument('--expires-at')
    p.add_argument('--acknowledge-unverified',action='store_true')
    p.add_argument('--cohort-ref')
    p.add_argument('--admission-mode')
    p.add_argument('--root-stop-request-id')
    args=vars(p.parse_args(argv))
    args['project']=args.pop('project_id')
    try:args['expected_deadline']=float(args['expected_deadline'])
    except ValueError:p.error('expected-deadline must be the exact original finite number')
    if args['expires_at'] is not None:
        try: args['expires_at']=float(args['expires_at'])
        except ValueError: p.error('expires-at must be an exact finite UTC epoch timestamp')
    if args['action'] in ('epoch-prepare','epoch-preview','epoch-apply','epoch-cancel','epoch-status'):
        for key in ('expires_at','cohort_ref','admission_mode','root_stop_request_id'):
            value=args.pop(key)
            if value not in (None,False): p.error('unrelated lifecycle options are invalid for epoch actions')
        result=execute_execution_epoch(**args)
    else:
        if args.pop('attestation_id') is not None: p.error('attestation-id requires an epoch action')
        for key in ('instance_id','workspace_resource','leaf_prefix','backing_gitdir','index_resource'):
            if args.pop(key) is not None: p.error('current scope fields require an epoch action')
        result=execute(**args)
    for key,value in result:
        print(str(key)+'\t'+str(value))
    return 0
if __name__=='__main__':
    try:raise SystemExit(main())
    except CoordError as error:
        import sys
        print('ERROR\t'+str(error),file=sys.stderr)
        raise SystemExit(2)
