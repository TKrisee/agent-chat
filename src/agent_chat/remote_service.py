"""Whitelisted server-side dispatcher for :mod:`agent_chat.remote`."""
from __future__ import annotations

import base64
import hashlib
import json
import posixpath
import secrets
import sqlite3
import time
from typing import Any

from .core import (CoordError, Coordinator, MAX_ATTACHMENTS, MAX_ATTACHMENT_TOTAL_SIZE,
                   first_ack_lookup, CurrentnessNoEffect)
from .guard_generation import install_guard_generation
from .receipts import process_closure_proofs, validate_process_evidence


def _string(value: Any, name: str, required: bool = True) -> str | None:
    if value is None and not required:
        return None
    if not isinstance(value, str) or not value:
        raise CoordError(f"{name} must be a nonempty string")
    return value


def managed_operator_attachment(body, canonical_project, roots):
    """Cross-project read without constructing or migrating either coordinator."""
    if (not isinstance(body, dict) or set(body) - {"op", "session", "host_id", "params"}
            or body.get('op') != 'attachment-download' or not isinstance(body.get('params'), dict)
            or body['params'].get('source_project') == canonical_project):
        raise CoordError('invalid managed-project attachment request')
    return _attachment_download(None, _string(body.get('session'), 'session'),
                                _string(body.get('host_id'), 'host_id'), body['params'],
                                canonical_project, roots)


def sender_owned_first_ack_lookup(db_path, body: dict, *, canonical_project: str) -> dict:
    """Dedicated read-only action, intentionally outside normal dispatch setup."""
    if (not isinstance(body, dict) or set(body) != {'op', 'session', 'host_id', 'params'}
            or body.get('op') != 'first-ack-lookup' or not isinstance(body.get('params'), dict)
            or set(body['params']) != {'source_id', 'expected_recipient'}):
        raise CoordError('invalid first-ACK lookup request')
    params = body['params']
    return first_ack_lookup(
        db_path, project_id=canonical_project, session_id=body.get('session'),
        host_id=body.get('host_id'), source_id=params.get('source_id'),
        expected_recipient=params.get('expected_recipient'))


def source_trace_read(db_path, body: dict, *, canonical_project: str) -> dict:
    if (not isinstance(body, dict) or set(body) != {'op', 'session', 'host_id', 'params'}
            or body.get('op') != 'source-trace' or not isinstance(body.get('params'), dict)
            or set(body['params']) != {'source_id'}):
        raise CoordError('invalid source trace request')
    from .bridge_state import source_trace
    return source_trace(db_path, project_id=canonical_project, session_id=body['session'],
                        host_id=body['host_id'], source_id=body['params']['source_id'])


def dispatch(coord: Coordinator, body: dict, *, bridge_manager=None, inbox_diagnostics=None,
             canonical_project=None, roots=None) -> dict:
    """Execute one explicitly supported coordinator action.

    The request session is assigned to the Coordinator directly; server process
    environment can never accidentally impersonate a remote caller.
    """
    if not isinstance(body, dict) or set(body) - {"op", "session", "host_id", "params"}:
        raise CoordError("invalid coordinator request")
    op = _string(body.get("op"), "op")
    session = _string(body.get("session"), "session")
    host_id = _string(body.get("host_id"), "host_id")
    params = body.get("params", {})
    if not isinstance(params, dict):
        raise CoordError("params must be an object")
    if op == "register":
        _string(params.get("agent"), "agent")
    if coord.db.execute("SELECT 1 FROM retired_sessions WHERE id=?", (session,)).fetchone():
        raise CoordError("session is deregistered; register a new session")
    coord.session = session
    from .root_orchestrator import projection
    if projection(coord.db, session) and op in ('block','cancel','begin-guard','exact-file-begin','attach-guard','pulse-guard','rename'):
        raise CoordError('global root is control-only; use project agents for resource work and detach before a local rename')
    # Hosted file locks are logical keys, independent of server filesystem layout.
    coord.resource_name = resource_name
    if op == "rename":
        # Host authentication for a label edit must also leave expiry untouched.
        coord.db.execute("BEGIN IMMEDIATE")
        try:
            _schema(coord)
            _bind_host(coord, session, host_id, op)
        except BaseException:
            coord.db.rollback()
            raise
        else:
            coord.db.commit()
    else:
        # Remote inbox host binding is a real BEGIN IMMEDIATE acquisition before
        # the inbox query's own transaction.  Trace it separately so a lock wait
        # is not misattributed to later held inbox work.
        with coord.tx(observer=inbox_diagnostics if op == 'inbox' else None):
            _schema(coord)
            _bind_host(coord, session, host_id, op)
    if op in ('exact-file-bind','exact-file-binding-status','exact-file-revoke'):
        from . import exact_file_bindings as bindings
        if op=='exact-file-bind':return bindings.issue(coord,canonical_project,params)
        if op=='exact-file-revoke':return bindings.revoke(coord,canonical_project,params)
        if set(params)!={'binding_id'}:raise CoordError('invalid exact file binding status')
        return bindings.status(coord,canonical_project,params['binding_id'],host_id)
    if op in ('exact-file-request', 'exact-file-begin', 'exact-file-status',
              'exact-file-guard-context', 'exact-file-release', 'exact-file-recover', 'exact-file-cancel', 'exact-file-pulse', 'exact-file-result',
              'exact-file-produced', 'exact-file-complete', 'exact-file-verify-context', 'exact-file-reconcile'):
        from .exact_file_read import (exact_path, SELECTED_PROJECT, SELECTED_SESSION, SELECTED_HOST, SELECTED_PATH,
                                      CHARACTERS_PROJECT, CHARACTERS_SESSION, CHARACTERS_PARENT, CHARACTERS_AGENT_PATH,
                                      CHARACTERS_PARENT_THREAD, CHARACTERS_HOST, CHARACTERS_FILES)
        params=dict(params);binding_id=params.pop('binding_id',None);binding_scope=None
        path=exact_path(params.get('path'))
        if binding_id is not None:
            from . import exact_file_bindings as bindings
            output=params.pop('output',None);max_bytes=params.pop('max_bytes',None)
            execution_ops = ('exact-file-begin','exact-file-pulse','exact-file-produced','exact-file-complete','exact-file-verify-context','exact-file-reconcile')
            if op in execution_ops and (output is None or max_bytes is None):
                raise CoordError('exact file bound execution needs its destination and byte limit')
            if op not in execution_ops and (output is not None or max_bytes is not None):
                raise CoordError('unexpected exact file execution parameters')
            bindings.authorize(coord.db,canonical_project,session,host_id,path,binding_id,
                output=output,max_bytes=max_bytes,active=op in ('exact-file-request','exact-file-begin','exact-file-pulse'))
            binding_scope=dict(project=canonical_project,binding_id=binding_id,output=output,max_bytes=max_bytes)
        requested=(canonical_project,session,host_id,path)
        original=(SELECTED_PROJECT,SELECTED_SESSION,SELECTED_HOST,SELECTED_PATH)
        characters=(CHARACTERS_PROJECT,CHARACTERS_SESSION,CHARACTERS_HOST)
        if (binding_id is None and requested != original and
                (canonical_project,session,host_id)==characters and path in CHARACTERS_FILES and
                op not in ('exact-file-status','exact-file-result','exact-file-guard-context')):
            raise CoordError('legacy Characters exact-file selector is retired; no new execution or mutation is admitted')
        if binding_id is None and requested != original and ((canonical_project,session,host_id)!=characters or path not in CHARACTERS_FILES):
            raise CoordError('exact file selection differs from reviewed actor/project/host/path')
        if binding_id is None and requested != original:
            try:
                from .bridge_state import resolve_binding_route
                resolved=resolve_binding_route(coord.db,session)
            except sqlite3.Error as error:
                raise CoordError('exact file reviewer route unavailable') from error
            route=resolved.get('route') if resolved else None
            if not route or len(route)!=2 or resolved['thread_id']!=CHARACTERS_PARENT_THREAD:
                raise CoordError('exact file reviewer route differs')
            parent,child=route
            parent_host=coord.db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?',(CHARACTERS_PARENT,)).fetchone()
            if ((parent['session_id'],parent['thread_id'],parent['parent_session'],parent['agent_path']) !=
                    (CHARACTERS_PARENT,CHARACTERS_PARENT_THREAD,None,None) or
                (child['session_id'],child['thread_id'],child['parent_session'],child['agent_path']) !=
                    (CHARACTERS_SESSION,None,CHARACTERS_PARENT,CHARACTERS_AGENT_PATH) or
                not parent_host or parent_host['host_id'] != CHARACTERS_HOST):
                raise CoordError('exact file reviewer route differs')
        # Scoped only to these typed operations; the generic resource parser stays unchanged.
        coord.resource_name=exact_path
        if op in ('exact-file-produced','exact-file-complete'):
            if binding_scope is None:raise CoordError('typed execution reports require an explicit App binding')
            from . import exact_file_execution as execution
            return execution.report(coord,canonical_project,session,host_id,binding_scope,path,params,
                                    complete=op=='exact-file-complete')
        if op in ('exact-file-verify-context','exact-file-reconcile'):
            if binding_scope is None:raise CoordError('legacy readback requires an explicit App binding')
            from . import exact_file_execution as execution
            method=execution.verify_context if op=='exact-file-verify-context' else execution.reconcile
            return method(coord,canonical_project,session,host_id,binding_scope,path,params)
        if op == 'exact-file-request':
            if set(params)!={'path','minutes'}: raise CoordError('invalid exact file request')
            def record_pending(db, acquired):
                if binding_id is not None:
                    bindings.authorize(db,canonical_project,session,host_id,path,binding_id)
                    db.execute('INSERT INTO exact_file_binding_reservations VALUES(?,?)',(acquired['reservation_id'],binding_id))
                db.execute("INSERT INTO remote_reservation_outcomes (reservation_id,resource,owner_session,host_id,state,created_at) VALUES(?,?,?,?,?,?)", (acquired['reservation_id'],acquired['name'],session,host_id,'pending',time.time()))
            def no_effect_currentness(db, resource, owner):
                # This invocation has not acquired or queued. A prior claim or
                # queue entry makes its overall outcome ambiguous, so do not
                # advertise a clean recovery in that case.
                if binding_id is None:
                    return
                held=db.execute('SELECT 1 FROM resources WHERE name=? AND owner_session IS NOT NULL',(resource,)).fetchone()
                queued=db.execute('SELECT 1 FROM resource_queue WHERE resource=? AND session=?',(resource,owner)).fetchone()
                prior=db.execute('SELECT 1 FROM exact_file_binding_reservations WHERE binding_id=?',(binding_id,)).fetchone()
                from .core import assert_agent_running
                assert_agent_running(db, owner)
                # The outer route check can race with revocation, expiry or
                # task/owner changes. Revalidate on this same transaction.
                bindings.authorize(db,canonical_project,session,host_id,path,binding_id)
                if held or queued or prior:
                    return
                raise CurrentnessNoEffect({
                    'kind':'inbox_currentness','action':'exact-file-request',
                    'owner_session':session,'host_id':host_id,'project_id':canonical_project,
                    'binding_id':binding_id,'path':path,'minutes':params['minutes'],
                    'requested_effect_absent':True,'eligibility_unverified':True,
                    'next_action':'fully consume own inbox, then recheck the original binding, scope and lease before continuing; do not retry an uncertain outcome',
                })
            result=coord.request(path,params['minutes'],record_pending,on_currentness=no_effect_currentness)
            if binding_id is not None and result['state']=='owned':
                with coord.tx() as db:
                    bindings.authorize(db,canonical_project,session,host_id,path,binding_id,reservation_id=result['reservation_id'])
            return result
        if op == 'exact-file-begin':
            if set(params)!={'path','token','reservation_id'}: raise CoordError('invalid exact file guard')
            return _begin_guard(coord,session,host_id,{'resource':path,'token':params['token'],'reservation_id':params['reservation_id']},binding_scope=binding_scope)
        if op == 'exact-file-pulse':
            if set(params)!={'path','run_id','token'}: raise CoordError('invalid exact file pulse')
            result=_guard_pulse(coord,session,host_id,{'run_id':params['run_id'],'token':params['token']},binding_scope=binding_scope)
            if result['resource']!=path: raise CoordError('exact file guard path differs')
            return result
        if op == 'exact-file-cancel':
            if set(params)!={'path'}: raise CoordError('invalid exact file cancel')
            return coord.cancel(path)
        if op == 'exact-file-result':
            if set(params)!={'path','reservation_id'}: raise CoordError('invalid exact file result')
            rid=_string(params['reservation_id'],'reservation_id')
            with coord.tx() as db:
                execution_result = {'state':'unknown','reason':'no retained typed execution intent/report'}
                if binding_id is not None:
                    bindings.authorize(db,canonical_project,session,host_id,path,binding_id,reservation_id=rid,active=False)
                    from . import exact_file_execution as execution
                    execution_result = execution.result(db,rid,binding_id)
                row=db.execute('SELECT state,action,receipt_sha256,evidence_sha256,cleared_at FROM remote_reservation_outcomes WHERE reservation_id=? AND resource=? AND owner_session=? AND host_id=?',(rid,path,session,host_id)).fetchone()
                if not row: return {'state':'unknown','execution':execution_result}
                if row['state']=='pending':
                    if coord.retained_reservation_result(db,path,rid)['state']=='unknown': return {'state':'unknown','execution':execution_result}
                    return {'state':'pending','resource':path,'reservation_id':rid,'receipt_backed_clear':False,'execution':execution_result}
                if row['state']=='cleared':
                    return {'state':'cleared','resource':path,'reservation_id':rid,'action':row['action'],'receipt_sha256':row['receipt_sha256'],'evidence_sha256':row['evidence_sha256'],'recorded_at':row['cleared_at'],'receipt_backed_clear':True,'execution':execution_result}
                return {'state':'unknown','execution':execution_result}
        if op == 'exact-file-status':
            if set(params)!={'path'}: raise CoordError('invalid exact file status')
            return coord.status(True,[path])
        if op == 'exact-file-guard-context':
            if set(params)!={'path'}: raise CoordError('invalid exact file guard context')
            with coord.tx() as db:
                row=coord._row(db,path)
                if not row or row['owner_session']!=session: raise CoordError('reservation proof requires its owning session')
                owner=db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?',(session,)).fetchone()
                if not owner or owner['host_id']!=host_id: raise CoordError('reservation proof requires its owning host')
                runs=[dict(item) for item in db.execute('SELECT run_id,local_pid,started_at,closed_at,close_evidence_sha256 FROM remote_guarded_runs WHERE reservation_id=?',(row['reservation_id'],))]
                return {'reservation_id':row['reservation_id'],'runs':runs}
        required={'path','receipt','evidence_base64'} | ({'token'} if op=='exact-file-release' else set())
        if set(params) not in (required, required | {'process_evidence_base64'}): raise CoordError('invalid exact file return')
        args=dict(params); args['resource']=args.pop('path')
        return _remote_clear(coord,session,host_id,args,'release' if op=='exact-file-release' else 'recover')
    # Keep this list auditable.  Do not replace it with getattr.
    if op == "register": return coord.register(_string(params.get("agent"), "agent"))
    if op == "rename": return coord.rename(_string(params.get("agent"), "agent"))
    if op == "status":
        result = coord.status(params.get("mine", False), params.get("resources"))
        with coord.tx() as db:
            for row in result['resources']:
                if row['owner_session'] != session:
                    continue
                # Recheck the exact current tuple before attaching its stored
                # host binding. Do not echo a caller-supplied host as proof or
                # expose another owner's host through the general status list.
                owner = db.execute('''SELECT h.host_id FROM resources r
                    JOIN remote_session_hosts h ON h.session_id=r.owner_session
                    WHERE r.name=? AND r.reservation_id=? AND r.owner_session=?''',
                    (row['resource'], row['reservation_id'], row['owner_session'])).fetchone()
                if owner is not None:
                    row['host_id'] = owner['host_id']
        return result
    activity_fields = {
        'activity-adopt': {'source_request_id', 'title'},
        'activity-assign': {'goal_id', 'worker', 'source_request_id', 'title'},
        'activity-transition': {'work_id', 'state'},
        'activity-metadata': {'work_id', 'dependencies', 'next_action'},
        'activity-link-escalation': {'origin_work_id', 'diagnostic_work_id', 'denial_message_id', 'next_action'},
        'activity-snapshot': set(),
        'activity-export': set(),
        'activity-history': {'work_id'},
        'activity-evidence-link': {'work_id', 'request_id'},
        'activity-evidence-record': {'work_id', 'request_id', 'kind', 'message_id', 'before_id', 'after_id', 'summary'},
        'activity-evidence-list': {'work_id'},
    }
    if op in activity_fields:
        if set(params) != activity_fields[op]:
            raise CoordError('invalid project activity parameters')
        if roots is None or canonical_project is None:
            raise CoordError('project activity requires a server-selected project registry')
        activity = roots.projects.activity(canonical_project, coord)
        if op in ('activity-evidence-link', 'activity-evidence-record', 'activity-evidence-list'):
            from . import activity_evidence
            if op == 'activity-evidence-link': return activity_evidence.link(activity, params['work_id'], params['request_id'])
            if op == 'activity-evidence-list': return activity_evidence.list_for(activity, params['work_id'])
            return activity_evidence.record(activity, params['work_id'], params['request_id'], params['kind'], params['message_id'], params['before_id'], params['after_id'], params['summary'])
        if op == 'activity-adopt':
            return activity.adopt(_string(params['source_request_id'], 'source_request_id'), params['title'])
        if op == 'activity-assign':
            return activity.assign(_string(params['goal_id'], 'goal_id'),
                                   _string(params['worker'], 'worker'),
                                   _string(params['source_request_id'], 'source_request_id'), params['title'])
        if op == 'activity-transition':
            return activity.transition(_string(params['work_id'], 'work_id'), params['state'])
        if op == 'activity-metadata':
            return activity.metadata(_string(params['work_id'], 'work_id'), params['dependencies'], params['next_action'])
        if op == 'activity-link-escalation':
            return activity.link_escalation(_string(params['origin_work_id'], 'origin_work_id'),
                _string(params['diagnostic_work_id'], 'diagnostic_work_id'),
                _string(params['denial_message_id'], 'denial_message_id'), params['next_action'])
        if op == 'activity-history':
            return {'events': activity.history(_string(params['work_id'], 'work_id'))}
        if op == 'activity-export':
            return activity.export()
        return activity.snapshot()
    if op == "inbox": return coord.inbox(bool(params.get("all", False)), observer=inbox_diagnostics)
    if op == "policy":
        from .communication import current_policy
        return current_policy()
    if op == "context": return coord.context(params.get("limit", 20), params.get("max_bytes", 12288),
                                               params.get("cursor"), params.get("message_ids"), params.get("resources"),
                                               params.get("resource_cursor"))
    if op == "message": return coord.message(_string(params.get("id"), "id"))
    if op == "sent-message": return coord.sent_message(_string(params.get("id"), "id"))
    if op in ('attachment-upload-begin', 'attachment-upload-status', 'attachment-upload-chunk',
              'attachment-upload-send', 'attachment-upload-cancel'):
        from . import attachment_transfer
        if not isinstance(canonical_project, str) or not canonical_project:
            raise CoordError('large attachment requires an explicit authenticated project')
        operations = {'attachment-upload-begin': attachment_transfer.begin,
                      'attachment-upload-chunk': attachment_transfer.chunk,
                      'attachment-upload-send': attachment_transfer.send}
        if op in operations:
            return operations[op](coord, canonical_project, host_id, params)
        return attachment_transfer.progress(coord, canonical_project, host_id, params,
                                            cancel=op == 'attachment-upload-cancel')
    if op == 'attachment-download-chunk':
        required = {'source_project', 'attachment_id', 'message_id', 'offset'}
        if set(params) not in (required, required | {'prefix_sha256'}):
            raise CoordError('large download requires exact message, attachment and offset')
        base = {key: params[key] for key in ('source_project', 'attachment_id', 'message_id')}
        return _attachment_download(coord, session, host_id, base, canonical_project, roots,
                                    _chunk_offset=params['offset'], _prefix_sha256=params.get('prefix_sha256'))
    if op == "attachment-download":
        return _attachment_download(coord, session, host_id, params, canonical_project, roots)
    if op == "acknowledge": return coord.acknowledge(_string(params.get("id"), "id"))
    if op == 'publication-plan-create':
        if set(params) != {'to', 'plan'}: raise CoordError('invalid publication plan parameters')
        return coord.publication_plan_create(_string(params.get('to'), 'to'), params.get('plan'))
    if op == 'publication-plan-status':
        if set(params) != {'id', 'plan'}: raise CoordError('invalid publication plan status parameters')
        return coord.publication_plan_status(_string(params.get('id'), 'id'), params.get('plan'))
    if op == "decision-mark":
        if set(params) != {'id'}:
            raise CoordError('invalid decision mark parameters')
        return coord.mark_decision_needed(_string(params.get('id'), 'id'))
    if op == "decision-status":
        if set(params) != {'id'}:
            raise CoordError('invalid decision status parameters')
        return coord.decision_status(_string(params.get('id'), 'id'))
    if op == "decision-diagnostic":
        if set(params) != {'id', 'dependency'}:
            raise CoordError('invalid decision diagnostic parameters')
        result = coord.decision_diagnostic(_string(params.get('id'), 'id'), _string(params.get('dependency'), 'dependency'))
        result['scope']['project_id'] = canonical_project
        return result
    if op == "decision-dismiss":
        if set(params) != {'id', 'reason'}:
            raise CoordError('invalid decision dismissal parameters')
        return coord.dismiss_decision(_string(params.get('id'), 'id'), _string(params.get('reason'), 'reason'))
    if op == "decision-answer":
        if set(params) != {'id', 'reply_id', 'outcome'}:
            raise CoordError('invalid decision answer parameters')
        return coord.record_decision_answer(_string(params.get('id'), 'id'),
                                            _string(params.get('reply_id'), 'reply_id'),
                                            _string(params.get('outcome'), 'outcome'))
    if op in ('deferred-request','deferred-activate','deferred-cancel','deferred-status'):
        from . import deferred_reservation
        return deferred_reservation.dispatch(coord, session, host_id, canonical_project, roots, op, params)
    if op == "request":
        # A generic grant issues executable authority immediately. Do not
        # silently accept a deferred request until that lifecycle is supported.
        if 'deferred' in params:
            raise CoordError('deferred reservation is not supported; no reservation issued')
        def record_pending(db, acquired):
            db.execute("INSERT INTO remote_reservation_outcomes (reservation_id,resource,owner_session,host_id,state,created_at) VALUES(?,?,?,?,?,?)", (acquired["reservation_id"], acquired["name"], session, host_id, "pending", time.time()))
        resource = _string(params.get("resource"), "resource")
        from .capacity_admission import RESOURCES, registry_authority_gate
        from .hospital_vendor_scope import CONTROL as vendor_control
        if resource == vendor_control:
            if roots is None or canonical_project != 'default': raise CoordError('vendor control requires configured default project')
            from . import root_local, service
            config=service._installed_server_config()
            if roots.projects.database != config.master or roots.path(canonical_project).resolve() != coord.path.resolve():
                raise CoordError('vendor control configured binding differs')
            registry=root_local._file(roots.projects.path,config.uid); st=registry.lstat()
            identity=(st.st_dev,st.st_ino,st.st_uid,st.st_mode)
            with registry_authority_gate(registry,config.uid,identity) as (registry_db,check_registry):
                return coord.request(resource,params.get('minutes'),record_pending,
                                     registry_db=registry_db,registry_check=check_registry)
        if resource not in RESOURCES or roots is None or canonical_project is None:
            return coord.request(resource, params.get("minutes"), record_pending)
        from . import root_local, service
        config = service._installed_server_config()
        master = roots.projects.database
        # Alternate installations retain ordinary no-ticket behavior.  A managed
        # ticket cannot be consumed without a gate: Coordinator.request checks it
        # again inside its target transaction, including tickets appearing now.
        if master != config.master:
            return coord.request(resource, params.get("minutes"), record_pending)
        if roots.path(canonical_project).resolve() != coord.path.resolve():
            raise CoordError("server-selected registry/project binding differs")
        registry = root_local._file(roots.projects.path, config.uid)
        st = registry.lstat()
        identity = (st.st_dev, st.st_ino, st.st_uid, st.st_mode)
        from .successor_admission import observe_for_request
        with observe_for_request(coord.db, resource, session) as successor_native:
            with registry_authority_gate(registry, config.uid, identity) as (registry_db, check_registry):
                return coord.request(resource, params.get("minutes"), record_pending,
                                     registry_db=registry_db, registry_check=check_registry,
                                     successor_native=successor_native)
    if op == "renew":
        if set(params) != {"resource", "minutes", "reservation_id", "token"}:
            raise CoordError('invalid renewal parameters')
        return coord.renew(_string(params.get("resource"), "resource"), params.get("minutes"),
                           _string(params.get("reservation_id"), "reservation_id"),
                           _string(params.get("token"), "token"), host_id=host_id)
    if op == "cancel": return coord.cancel(_string(params.get("resource"), "resource"))
    if op == "check": return coord.check(_string(params.get("resource"), "resource"), _string(params.get("token"), "token"))
    if op == "block": return coord.block(_string(params.get("resource"), "resource"), _string(params.get("reason"), "reason"))
    if op == "link-reply": return coord.link_reply(_string(params.get("id"), "id"), _string(params.get("reply_to"), "reply_to"))
    if op == "send":
        attachments = params.get("attachments", [])
        body = params.get("body", "")
        if not isinstance(body, str): raise CoordError("body must be a string")
        ack_reply = params.get("ack_reply", False)
        if not isinstance(ack_reply, bool): raise CoordError("ack_reply must be a boolean")
        decision_needed = params.get('decision_needed', False)
        if not isinstance(decision_needed, bool): raise CoordError('decision_needed must be a boolean')
        update_for = params.get('update_for')
        if update_for is not None and (not isinstance(update_for, str) or not update_for):
            raise CoordError('update marker requires one explicit task')
        targets = params.get("to")
        if isinstance(targets, str):
            return _send(coord, _string(targets, "to"), body, attachments, params.get("reply_to"), ack_reply,
                         decision_needed, params.get('assignment_id'), update_for)
        if params.get('assignment_id') is not None:
            raise CoordError('assignment link requires exactly one direct recipient')
        if update_for is not None:
            raise CoordError('update marker requires exactly one direct recipient')
        if targets is not None and (not isinstance(targets, list) or not all(isinstance(item, str) for item in targets)):
            raise CoordError("to must be a recipient string or a list of recipient strings")
        if ack_reply:
            raise CoordError("ack_reply is only supported for a direct recipient")
        if decision_needed:
            raise CoordError('decision_needed is only supported for one direct recipient')
        if targets is None and params.get("reply_to") is not None:
            raise CoordError("reply_to requires an explicit recipient group")
        results = coord.send_group_prepared(targets, body, _prepare_attachments(coord, attachments), params.get("reply_to"))
        return _group_result(results)
    if op == "send-many":
        if params.get('assignment_id') is not None:
            raise CoordError('assignment link requires exactly one direct recipient')
        if params.get('update_for') is not None:
            raise CoordError('update marker requires exactly one direct recipient')
        targets = params.get("to")
        if not isinstance(targets, list) or not all(isinstance(x, str) for x in targets):
            raise CoordError("to must be a list of recipient strings")
        return {"messages": coord.send_many(targets, _string(params.get("body"), "body", False) or "", params.get("reply_to"))}
    if op == "deregister": return coord.remove_session(coord.require_session())
    if op == 'admin-recover':
        from .agent_control import AdminRecoveryState
        allowed = {'project', 'resource', 'reservation_id', 'owner_session', 'request_id',
                   'operator', 'reason', 'evidence'}
        if set(params) != allowed:
            raise CoordError('admin-recover requires the exact project, target, audit, and evidence fields')
        return AdminRecoveryState(coord).recover(project=params.get('project'), canonical_project=canonical_project,
            resource=params.get('resource'), reservation_id=params.get('reservation_id'),
            owner_session=params.get('owner_session'), request_id=params.get('request_id'),
            operator=params.get('operator'), reason=params.get('reason'), evidence=params.get('evidence'),
            host_id=host_id)
    if op == "remove-session": return coord.remove_session(_string(params.get("id"), "id"))
    if op in ('supervisor-configure','supervisor-status'):
        from .bridge_state import BridgeState
        from .supervisor import SupervisorState
        BridgeState(coord); supervisor=SupervisorState(coord)
        if op == 'supervisor-configure':
            if set(params) not in ({'enabled'}, {'recovery_enabled'}):
                raise CoordError('supervisor-configure accepts exactly one of enabled or recovery_enabled')
            if not isinstance(next(iter(params.values())), bool):
                raise CoordError('supervisor-configure setting must be boolean')
            if 'recovery_enabled' in params:
                return supervisor._configure_recovery(params['recovery_enabled'])
            return supervisor.configure(params['enabled'])
        if set(params) - {'to'}: raise CoordError('supervisor-status accepts only optional to')
        return supervisor.status(params.get('to'))
    if op in ('agent-stop','agent-resume','agent-model','agent-model-resolve','agent-status','agent-investigation-preload','agents'):
        from .bridge_state import BridgeState
        from .agent_control import AgentControlState
        BridgeState(coord)
        measurements = getattr(bridge_manager, 'measurements', None)
        controls = AgentControlState(coord, sessions_root=getattr(measurements, 'sessions_root', None))
        if op == 'agent-model-resolve':
            return controls.resolve_unchanged(params.get('request_id'), params.get('expected_thread'),
                                              params.get('resolution_id'), params.get('confirm'))
        if op == 'agents':
            rows = controls.roster()
            from .root_orchestrator import projection
            if projection(coord.db, session):
                allowed = {row['session_id'] for row in coord.db.execute('SELECT session_id FROM project_orchestrators WHERE root_session=?',(session,))} | {session}
                rows = [row for row in rows if row['id'] in allowed]
            return {'agents':rows}
        target = _string(params.get('to') or session, 'to')
        if op == 'agent-status':
            refresh = None
            if params.get('refresh'):
                refresh = controls.request(target, params.get('expected_thread'), 'inspect', params.get('request_id'), True)
            return dict(controls.status(target), refresh=refresh)
        if op == 'agent-investigation-preload':
            if set(params) != {'to','expected_thread','continuation_id','request_id','confirm'}:
                raise CoordError('investigation preload requires the exact PO request fields')
            return controls.request_investigation_preload(target, params['expected_thread'],
                params['continuation_id'], params['request_id'], params['confirm'])
        return controls.request(target, params.get('expected_thread'), op.removeprefix('agent-'), params.get('request_id'),
                                params.get('confirm'), params.get('interrupt',False), model=params.get('model'),
                                reasoning_effort=params.get('reasoning_effort'),
                                allow_cold_resume=params.get('allow_cold_resume', False))
    if op.startswith('session-reset') or op in ('agent-create', 'parked-agent-create'):
        from .bridge_state import BridgeState
        from .session_reset import SessionResetState
        BridgeState(coord)
        resets = SessionResetState(coord)
        if op == 'parked-agent-create':
            if not roots:
                raise CoordError('parked subordinate requires the project registry')
            from .turnover_bootstrap import authorize_subordinate
            with roots.projects.connection() as registry:
                registry.execute('BEGIN IMMEDIATE')
                proof = authorize_subordinate(coord.db, registry, params.get('bootstrap_id'), session,
                    params.get('expected_thread'), host_id, params.get('model'), params.get('reasoning_effort'))
                if proof['project_id'] != canonical_project:
                    raise CoordError('parked subordinate project differs')
                result = resets.create(params.get('agent'), params.get('expected_thread'), params.get('prompt'),
                    params.get('request_id'), params.get('confirm'), model=params.get('model'),
                    reasoning_effort=params.get('reasoning_effort'), cwd=params.get('cwd'),
                    _park_stop=proof['stop_id'], _sub_bootstrap=params.get('bootstrap_id'), _sub_registry=registry)
                registry.commit()
                return result
        if op == 'session-reset-revalidate-info':
            return resets.revalidate_info(_string(params.get('id'), 'id'))
        if op == 'session-reset-revalidate':
            return resets.revalidate_workspace(_string(params.get('id'), 'id'), params.get('proof'), params.get('confirm'))
        if op == 'session-reset-status':
            return resets.status(_string(params.get('to') or session, 'to'))
        if op == 'session-reset-cancel':
            return resets.cancel(_string(params.get('id'), 'id'))
        if op == 'session-reset-resolve':
            return resets.resolve_creation(_string(params.get('id'), 'id'), params.get('thread'), params.get('confirm_created'))
        if op == 'session-reset-retry':
            return resets.retry_prompt(_string(params.get('id'), 'id'), params.get('confirm_not_started'))
        if op == 'agent-create':
            return resets.create(params.get('agent'), params.get('expected_thread'), params.get('prompt'),
                params.get('request_id'), params.get('confirm'), model=params.get('model'),
                reasoning_effort=params.get('reasoning_effort'), cwd=params.get('cwd'))
        if op == 'session-reset':
            return resets.request(_string(params.get('to') or session, 'to'), params.get('expected_thread'),
                                  params.get('prompt'), params.get('request_id'), params.get('confirm'),
                                  model=params.get('model'), reasoning_effort=params.get('reasoning_effort'))
    if op in ("bind", "unbind", "bridge-status", "bridge-retry", "bridge-resolve"):
        from .bridge_state import BridgeState
        state = BridgeState(coord)
        if op == "bind": return state.bind(params.get("thread"), params.get("parent_session"), params.get("agent_path"))
        if op == "unbind": return state.unbind()
        if op == "bridge-status":
            mine = params.get("mine", False)
            if not isinstance(mine, bool):
                raise CoordError("mine must be a boolean")
            return state.status(mine)
        if op in ("bridge-retry", "bridge-resolve"):
            from .bridge import exclusive_bridge
            recovery = bridge_manager.recovery(state.resolve(session)) if bridge_manager else exclusive_bridge(coord.path)
            with recovery:
                if op == "bridge-retry": return state.retry(_string(params.get("job_id"), "job_id"), bool(params.get("confirm_not_started", False)))
                return state.resolve_delivered(_string(params.get("job_id"), "job_id"), bool(params.get("confirm_delivered", False)))
    if op == "guard-context":
        resource = resource_name(_string(params.get("resource"), "resource"))
        with coord.tx() as db:
            r = coord._row(db, resource)
            if not r or r["owner_session"] != session:
                raise CoordError("reservation proof requires its owning session")
            owner = db.execute("SELECT host_id FROM remote_session_hosts WHERE session_id=?", (r["owner_session"],)).fetchone() if r else None
            if not owner or owner["host_id"] != host_id:
                raise CoordError("reservation proof requires its owning host")
            if params.get("token") is not None:
                coord._verify_owner(db,resource,session,_string(params.get("token"),"token"),True)
            runs = [dict(row) for row in db.execute("SELECT run_id,local_pid,started_at,closed_at,close_evidence_sha256 FROM remote_guarded_runs WHERE reservation_id=?", (r["reservation_id"],))]
            if db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='remote_guard_witnesses'").fetchone():
                for run in runs:
                    witness = db.execute('SELECT pid,parent_pid,pgid,uid,ruid,birth_sec,birth_usec,boot_id,executable,executable_sha256,recorded_at FROM remote_guard_witnesses WHERE run_id=?', (run['run_id'],)).fetchone()
                    if witness:
                        run.update(dict(witness)); run['witness_provenance'] = 'host_reported'
                    from .guard_witness import file_status
                    file_evidence = file_status(db, run)
                    if file_evidence is not None: run['file_evidence'] = file_evidence
            return {"reservation_id": r["reservation_id"], "runs": runs}
    if op == "reservation-result":
        resource = resource_name(_string(params.get("resource"), "resource"))
        reservation_id = _string(params.get("reservation_id"), "reservation_id")
        with coord.tx() as db:
            outcome = db.execute("SELECT state,action,receipt_sha256,evidence_sha256,cleared_at,owner_session,host_id FROM remote_reservation_outcomes WHERE reservation_id=? AND resource=? AND owner_session=? AND host_id=?", (reservation_id, resource, session, host_id)).fetchone()
            if not outcome:
                return {"state": "unknown"}
            if outcome["state"] == "pending":
                retained = coord.retained_reservation_result(db, resource, reservation_id)
                if retained["state"] == "unknown": return retained
                return {"state": "pending", "resource": resource, "reservation_id": reservation_id, "receipt_backed_clear": False}
            if outcome["state"] == "cleared":
                return {"state": "cleared", "resource": resource, "reservation_id": reservation_id, "owner_session": outcome["owner_session"], "host_id": outcome["host_id"], "action": outcome["action"], "receipt_sha256": outcome["receipt_sha256"], "evidence_sha256": outcome["evidence_sha256"], "recorded_at": outcome["cleared_at"], "receipt_backed_clear": True}
            return {"state": "unknown"}
    if op in ('producer-establish', 'producer-close', 'producer-status', 'producer-epoch-attest'):
        from .lifecycle_producer import dispatch as producer_dispatch
        return producer_dispatch(coord, session, host_id, op, params)
    if op in ('hospital-vendor-start','hospital-vendor-pulse','hospital-vendor-finish'):
        return _hospital_vendor_dispatch(coord,session,host_id,op,params,canonical_project,roots)
    if op == 'hospital-vendor-begin': return _hospital_vendor_begin(coord,session,host_id,params,canonical_project,roots)
    if op == "begin-guard": return _begin_guard(coord, session, host_id, params, file_scope=(roots,canonical_project))
    if op == "begin-guard-batch": return _begin_guard_batch(coord, session, host_id, params)
    if op == "attach-guard-pid": return _attach_guard_pid(coord, session, host_id, params)
    if op == "attach-guard-witness":
        from .guard_witness import attach
        return attach(coord, session, host_id, params, roots.projects if roots else None)
    if op == 'guard-file-observe':
        from .guard_witness import file_observe
        return file_observe(coord, session, host_id, params, roots.projects if roots else None)
    if op == 'guard-recovery-settle':
        from .guard_witness import recovery
        return recovery(coord, session, host_id, params, roots.projects if roots else None)
    if op == "guard-pulse": return _guard_pulse(coord, session, host_id, params)
    if op == "close-guard": return _close_guard(coord, session, host_id, params)
    if op in ('resource-uncertainty-record','resource-uncertainty-status'):
        required={'resource','reservation_id','token','request_id'}
        if op == 'resource-uncertainty-record': required.add('reason')
        if set(params) != required: raise CoordError('invalid resource uncertainty parameters')
        return coord.resource_uncertainty(_string(params.get('resource'),'resource'),
            _string(params.get('reservation_id'),'reservation_id'),_string(params.get('token'),'token'),
            host_id,_string(params.get('request_id'),'request_id'),
            reason=params.get('reason'),record=op == 'resource-uncertainty-record')
    if op in ("release", "recover"): return _remote_clear(coord, session, host_id, params, op)
    raise CoordError("unsupported remote coordinator operation: " + op)


def resource_name(raw):
    if not isinstance(raw, str) or not raw or raw != raw.strip():
        raise CoordError("resource name is invalid")
    if raw.startswith('file:'):
        part = raw[5:]
        normalized = posixpath.normpath(part)
        if not part or part.startswith('/') or normalized in ('.', '..') or normalized.startswith('../') or '\x00' in part or '\\' in part:
            raise CoordError('file resource must be a repo-relative path within the project')
        return 'file:' + normalized
    if any(c.isspace() for c in raw) or '..' in raw.split('/'):
        raise CoordError('resource name is invalid')
    return raw


def _prepare_attachments(coord: Coordinator, attachments: Any) -> list[tuple[str, str, bytes]]:
    if not isinstance(attachments, list) or len(attachments) > MAX_ATTACHMENTS:
        raise CoordError(f"attachments must contain at most {MAX_ATTACHMENTS} files")
    prepared = []
    total = 0
    for index, item in enumerate(attachments, 1):
        if not isinstance(item, dict) or not isinstance(item.get("name"), str) or not isinstance(item.get("content_base64"), str):
            raise CoordError(f"attachment #{index}: remote attachment is invalid")
        encoded = item["content_base64"]
        # Estimate decoded size before allocating; validate=True still checks
        # exact bytes and the per-format validator remains authoritative.
        estimated = len(encoded) // 4 * 3 - min(2, len(encoded) - len(encoded.rstrip('=')))
        if total + estimated > MAX_ATTACHMENT_TOTAL_SIZE:
            raise CoordError('attachments exceed 500 MiB total raw size')
        try: content = base64.b64decode(encoded, validate=True)
        except ValueError as error: raise CoordError(f"attachment #{index}: remote attachment is invalid base64; filename={item['name']!r}; decoded size unknown") from error
        total += len(content)
        if total > MAX_ATTACHMENT_TOTAL_SIZE:
            raise CoordError('attachments exceed 500 MiB total raw size')
        try:
            prepared.append(coord._prepare_attachment(item["name"], content))
        except CoordError as error:
            raise CoordError(f"attachment #{index}: {error}; filename={item['name']!r}; size={len(content)} bytes") from error
    return prepared


def _attachment_download(coord: Coordinator, session: str, host_id: str, params: dict,
                         canonical_project: str | None, roots=None, *, _chunk_offset=None,
                         _prefix_sha256=None) -> dict:
    if set(params) not in ({"source_project", "attachment_id"},
                           {"source_project", "attachment_id", "message_id"}):
        raise CoordError("attachment-download requires exact source_project, attachment_id and optional message_id fields")
    source_project = _string(params.get("source_project"), "source_project")
    attachment_id = _string(params.get("attachment_id"), "attachment_id")
    message_id = _string(params["message_id"], "message_id") if "message_id" in params else None
    if canonical_project != source_project:
        if _chunk_offset is not None:
            raise CoordError('large attachment chunks require the original recipient project')
        if not roots or not message_id:
            raise CoordError("managed-project attachment download requires trusted Root records and exact message ID")
        item = roots.operator_attachment(canonical_project, session, host_id,
                                         source_project, message_id, attachment_id)
        content = item.pop("content")
        return {**item, "source_project": source_project, "project": canonical_project,
                "recipient_session": session, "host_id": host_id,
                "content_base64": base64.b64encode(content).decode("ascii")}
    from .root_orchestrator import projection
    root = projection(coord.db, session)
    if (root and root["home_project"] == source_project
            and root["home_session"] == session):
        # Canonical Root also receives task reports directly from its current
        # designated PO. This remains exact own-inbox access, not a managed-
        # project or arbitrary agent attachment privilege.
        po_report = bool(message_id and coord.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='project_orchestrators'").fetchone() and coord.db.execute(
            "SELECT 1 FROM messages m JOIN project_orchestrators p ON p.session_id=m.sender_session "
            "WHERE m.id=? AND m.recipient_session=? AND p.root_session=?",
            (message_id,session,session)).fetchone())
        item = coord.private_inbox_attachment(attachment_id,
            message_id=message_id if po_report else None, operator_sender=not po_report,
            _chunk_offset=_chunk_offset, _prefix_sha256=_prefix_sha256)
        if message_id is not None and item["message_id"] != message_id:
            raise CoordError("attachment does not belong to the exact original message")
    else:
        if message_id is None:
            raise CoordError("ordinary recipient download requires the exact original message ID")
        item = coord.private_inbox_attachment(attachment_id, message_id=message_id,
                                             operator_sender=False, _chunk_offset=_chunk_offset,
                                             _prefix_sha256=_prefix_sha256)
    content = item.pop("content")
    return {**item, "source_project": source_project, "project": canonical_project,
            "recipient_session": session, "host_id": host_id,
            "content_base64": base64.b64encode(content).decode("ascii")}


def _send(coord: Coordinator, target: str, body: str, attachments: Any, reply_to: Any,
          ack_reply: bool = False, decision_needed: bool = False, assignment_id=None, update_for=None) -> dict:
    return coord._send_prepared(target, body, _prepare_attachments(coord, attachments), reply_to,
                                ack_reply, decision_needed, assignment_id=assignment_id, update_for=update_for)


def _group_result(results: list[dict]) -> dict:
    return {"batch_id": results[0]["batch_id"], "deliveries": [
        {"id": delivery["id"], "recipient_session": delivery["recipient_session"],
         "recipient_agent": delivery["recipient_agent"]}
        for delivery in results[0]["deliveries"]
    ]}


def _schema(coord: Coordinator) -> None:
    # executescript implicitly commits SQLite transactions.  These individual
    # statements therefore retain the BEGIN IMMEDIATE lock established by tx().
    coord.db.execute("CREATE TABLE IF NOT EXISTS remote_session_hosts (session_id TEXT PRIMARY KEY, host_id TEXT NOT NULL, registered_at REAL NOT NULL, last_seen_at REAL NOT NULL)")
    coord.db.execute("CREATE TABLE IF NOT EXISTS remote_guarded_runs (run_id TEXT PRIMARY KEY, resource TEXT NOT NULL, reservation_id TEXT NOT NULL, session_id TEXT NOT NULL, host_id TEXT NOT NULL, local_pid INTEGER NOT NULL, started_at REAL NOT NULL, closed_at REAL, close_evidence_sha256 TEXT)")
    coord.db.execute("CREATE TABLE IF NOT EXISTS remote_restoration_runs (run_id TEXT PRIMARY KEY, deadline REAL NOT NULL)")
    coord.db.execute("CREATE TABLE IF NOT EXISTS remote_guard_batches (run_id TEXT PRIMARY KEY, batch_id TEXT NOT NULL)")
    coord.db.execute("CREATE TABLE IF NOT EXISTS remote_reservation_outcomes (reservation_id TEXT PRIMARY KEY, resource TEXT NOT NULL, owner_session TEXT NOT NULL, host_id TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('pending','cleared')), action TEXT, receipt_sha256 TEXT, evidence_sha256 TEXT, created_at REAL NOT NULL, cleared_at REAL, CHECK((state='pending' AND action IS NULL AND receipt_sha256 IS NULL AND evidence_sha256 IS NULL AND cleared_at IS NULL) OR (state='cleared' AND action IN ('release','recover') AND receipt_sha256 IS NOT NULL AND evidence_sha256 IS NOT NULL AND cleared_at IS NOT NULL)))")
    coord.db.execute("CREATE TABLE IF NOT EXISTS deferred_reservations (request_id TEXT PRIMARY KEY, reservation_id TEXT UNIQUE NOT NULL, resource TEXT NOT NULL, owner_session TEXT NOT NULL, host_id TEXT NOT NULL, project_id TEXT NOT NULL, workspace TEXT NOT NULL, path TEXT NOT NULL, task_id TEXT NOT NULL, source_id TEXT NOT NULL, thread_id TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('deferred','active','cancelled')), created_at REAL NOT NULL, changed_at REAL NOT NULL)")
    install_guard_generation(coord.db)
    from .goal_selection_generation import install as install_goal_generation, PROJECT as GOAL_PROJECT
    install_goal_generation(coord.db, GOAL_PROJECT)


def _bind_host(coord: Coordinator, session: str, host: str, op: str) -> None:
    from .root_team_reset import quarantined
    if quarantined(coord.db,session=session):
        raise CoordError('session is quarantined by team reset; old credentials cannot control current work')
    if coord.db.execute('SELECT 1 FROM retired_sessions WHERE id=?',(session,)).fetchone():
        raise CoordError('session is retired; old credentials cannot control the current project')
    """Pin a remote identity before an operation can mention local PIDs/proofs."""
    row = coord.db.execute("SELECT host_id FROM remote_session_hosts WHERE session_id=?", (session,)).fetchone()
    if row and row["host_id"] != host:
        raise CoordError("session is bound to a different remote host")
    if row:
        coord.db.execute("UPDATE remote_session_hosts SET last_seen_at=? WHERE session_id=?", (time.time(), session))
        return
    # Legacy reservations/runs have PIDs meaningful only on their original host.
    held = coord.db.execute("SELECT 1 FROM resources WHERE owner_session=? LIMIT 1", (session,)).fetchone()
    local_runs = coord.db.execute("SELECT 1 FROM guarded_runs WHERE session=? AND closed_at IS NULL LIMIT 1", (session,)).fetchone()
    if held or local_runs:
        raise CoordError("cannot bind a legacy session with held resources or open local guarded runs to a remote host")
    # A remote caller must still be registered except register itself.
    if op != "register" and not coord.db.execute("SELECT 1 FROM sessions WHERE id=?", (session,)).fetchone():
        raise CoordError("unknown session; register first")
    coord.db.execute("INSERT INTO remote_session_hosts VALUES(?,?,?,?)", (session, host, time.time(), time.time()))


def _begin_guard_batch(coord: Coordinator, session: str, host: str, p: dict, *, vendor=None) -> dict:
    from .root_lifecycle import stopped
    if stopped(coord.db,session):
        raise CoordError('root stop-all barrier withheld new guarded work')
    """Admit a bounded same-caller set before any launcher can be activated."""
    if set(p) - {'tuples', 'restore', 'max_seconds'}:
        raise CoordError('invalid batch admission parameters')
    tuples = p.get('tuples')
    restore = p.get('restore', False)
    if not isinstance(tuples, list) or not 1 <= len(tuples) <= 64 or type(restore) is not bool:
        raise CoordError('invalid batch admission')
    budget = p.get('max_seconds', 120)
    if restore:
        if type(budget) is not int or not 1 <= budget <= 900:
            raise CoordError('invalid batch restoration budget')
    elif 'max_seconds' in p:
        raise CoordError('batch restoration budget requires restoration mode')
    prepared, seen = [], set()
    for item in tuples:
        if not isinstance(item, dict) or set(item) != {'resource', 'session_id', 'host_id', 'reservation_id', 'token'}:
            raise CoordError('invalid batch admission tuple')
        if any(not isinstance(item[key], str) or not item[key] for key in item):
            raise CoordError('invalid batch admission tuple')
        if item['session_id'] != session or item['host_id'] != host:
            raise CoordError('batch admission requires the authenticated caller host session')
        resource = coord.resource_name(item['resource'])
        from .hospital_vendor_scope import CONTROL as vendor_control, RUN_RESOURCE as vendor_run
        if resource == vendor_control: raise CoordError('vendor control cannot authorize arbitrary execution')
        if resource == vendor_run and vendor is None: raise CoordError('vendor run cannot authorize arbitrary execution')
        if resource != item['resource'] or resource in seen:
            raise CoordError('batch admission requires distinct exact resources')
        seen.add(resource)
        prepared.append(item)
    from .core import assert_agent_running
    from .root_orchestrator import projection
    with coord.tx() as db:
        if vendor is not None:
            from . import hospital_vendor_binding as vendor_binding, hospital_vendor_scope as vendor_scope
            if restore or len(prepared)!=2 or prepared[0]['resource']!=vendor_scope.RUN_RESOURCE or not prepared[1]['resource'].startswith('/'):
                raise CoordError('fixed vendor guard tuple differs')
            vendor_binding.validate_in_transaction(db,session,host,vendor['control_rid'],vendor['control_token'],vendor['resource'],vendor['gate'])
        coord._fresh(db, session)
        if projection(db, session):
            raise CoordError('global root cannot admit resource work')
        assert_agent_running(db, session, cleanup=True)
        from .core import assert_original_owner_admission
        assert_original_owner_admission(db, session)
        pinned = db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (session,)).fetchone()
        if not pinned or pinned['host_id'] != host:
            raise CoordError('batch admission requires the authenticated caller host session')
        rows = []
        for item in prepared:
            row = coord._verify_owner(db, item['resource'], session, item['token'], restore)
            from .core import assert_execution_domain_admission
            assert_execution_domain_admission(db,item['resource'],session,row['reservation_id'])
            if db.execute('SELECT 1 FROM deferred_reservations WHERE reservation_id=?',(row['reservation_id'],)).fetchone():
                raise CoordError('deferred-origin reservation cannot authorize batch execution')
            if row['reservation_id'] != item['reservation_id']:
                raise CoordError('batch admission reservation changed')
            from .exact_file_bindings import reserved_binding
            if reserved_binding(db,row['reservation_id']):
                raise CoordError('exact file capability cannot authorize a batch executor')
            if restore:
                if coord._state(row) != 'stale':
                    raise CoordError('batch restoration requires stale reservations')
                if db.execute('SELECT 1 FROM remote_guarded_runs WHERE reservation_id=? AND closed_at IS NULL',
                              (row['reservation_id'],)).fetchone():
                    raise CoordError('close existing guarded runs before batch restoration')
            rows.append(row)
        # No row or association is created until the final tuple is validated.
        batch_id = 'remote_batch_' + secrets.token_hex(16)
        admissions, started = [], time.time()
        for row in rows:
            run_id = 'remote_run_' + secrets.token_hex(16)
            db.execute('INSERT INTO remote_guarded_runs VALUES(?,?,?,?,?,?,?,?,?)',
                       (run_id, row['name'], row['reservation_id'], session, host, 0, started, None, None))
            db.execute('INSERT INTO remote_guard_batches VALUES(?,?)', (run_id, batch_id))
            if restore:
                db.execute('INSERT INTO remote_restoration_runs VALUES(?,?)', (run_id, started + budget))
            admissions.append({'resource': row['name'], 'reservation_id': row['reservation_id'],
                               'run_id': run_id, 'host_id': host})
        if vendor is not None: vendor['gate'].check()
    return {'batch_id': batch_id, 'admissions': admissions}


def _begin_guard(coord: Coordinator, session: str, host: str, p: dict, *, binding_scope=None, file_scope=None) -> dict:
    from .root_lifecycle import stopped
    if stopped(coord.db,session):
        raise CoordError('root stop-all barrier withheld new guarded work')
    resource, token = _string(p.get("resource"), "resource"), _string(p.get("token"), "token")
    from .hospital_vendor_scope import CONTROL as vendor_control, RUN_RESOURCE as vendor_run
    if coord.resource_name(resource) in (vendor_control,vendor_run): raise CoordError('vendor scopes cannot authorize arbitrary execution')
    restore = p.get("restore", False)
    if not isinstance(restore, bool): raise CoordError("restore must be a boolean")
    witness_required = p.get('require_host_witness', False)
    if type(witness_required) is not bool or (witness_required and (restore or not p.get('reservation_id'))):
        raise CoordError('host witness requires one exact non-restoration reservation')
    declared = ('declared_task_id' in p or 'declared_source_id' in p)
    prospective = p.get('require_prospective_recovery', False)
    if type(prospective) is not bool or (prospective and not declared):
        raise CoordError('prospective recovery requires a declared witnessed file')
    if declared and (not witness_required or set(('declared_task_id','declared_source_id')) - set(p) or binding_scope is not None):
        raise CoordError('declared file requires a witnessed exact task and source')
    budget = p.get("max_seconds")
    if restore:
        reservation_id = _string(p.get("reservation_id"), "reservation_id")
        budget = 120 if budget is None else budget
        if not isinstance(budget, int) or isinstance(budget, bool) or not 1 <= budget <= 900:
            raise CoordError("restoration max_seconds must be an integer from 1 to 900")
    elif budget is not None:
        raise CoordError("max_seconds requires restoration mode")
    else:
        reservation_id = (None if p.get("reservation_id") is None else
                          _string(p.get("reservation_id"), "reservation_id"))
    with coord.tx() as db:
        coord._fresh(db, session)
        r = coord._verify_owner(db, coord.resource_name(resource), session, token, restore)
        from .core import assert_execution_domain_admission
        assert_execution_domain_admission(db,r['name'],session,r['reservation_id'])
        if reservation_id is not None and r['reservation_id'] != reservation_id:
            raise CoordError('guard reservation_id does not match')
        from . import exact_file_bindings as bindings
        associated=bindings.reserved_binding(db,r['reservation_id'])
        if associated and binding_scope is None:
            raise CoordError('exact file capability permits only its typed snapshot executor')
        if binding_scope is not None:
            bound=bindings.authorize(db,binding_scope['project'],session,host,r['name'],binding_scope['binding_id'],
                output=binding_scope['output'],max_bytes=binding_scope['max_bytes'],reservation_id=r['reservation_id'])
        from .core import assert_agent_running
        assert_agent_running(db, session, cleanup=True)
        from .core import assert_original_owner_admission
        assert_original_owner_admission(db, session)
        declaration = None
        deferred = db.execute('SELECT * FROM deferred_reservations WHERE reservation_id=?',(r['reservation_id'],)).fetchone()
        if deferred and (deferred['state'] != 'active' or (not restore and (not declared or not witness_required or binding_scope is not None or not file_scope or file_scope[1] != deferred['project_id'] or p.get('declared_task_id') != deferred['task_id'] or p.get('declared_source_id') != deferred['source_id']))):
            raise CoordError('deferred-origin reservation requires its activated declared executor scope')
        if declared:
            if not file_scope or not file_scope[0]: raise CoordError('declared file requires project registry')
            from .guard_witness import task_file_scope
            declaration = task_file_scope(db, file_scope[0].projects, file_scope[1], session, host,
                                          r['name'], p['declared_task_id'], p['declared_source_id'])
            if deferred and declaration != tuple(deferred[k] for k in ('project_id','workspace','path','task_id','source_id','thread_id')):
                raise CoordError('deferred reservation scope changed')
        if restore:
            if coord._state(r) != "stale": raise CoordError("restoration requires a stale reservation")
            if r["reservation_id"] != reservation_id: raise CoordError("restoration reservation_id does not match")
            if db.execute("SELECT 1 FROM remote_guarded_runs WHERE reservation_id=? AND closed_at IS NULL", (reservation_id,)).fetchone():
                raise CoordError("close existing guarded runs before starting restoration")
        run_id = "remote_run_" + hashlib.sha256((host + session + str(time.time())).encode()).hexdigest()[:32]
        db.execute("INSERT INTO remote_guarded_runs VALUES(?,?,?,?,?,?,?,?,?)",
                   (run_id, r["name"], r["reservation_id"], session, host, 0, time.time(), None, None))
        execution_intent = None
        if binding_scope is not None:
            from . import exact_file_execution as execution
            execution_intent = execution.begin(db,run_id,r['reservation_id'],bound)
        if witness_required:
            db.execute('INSERT INTO remote_guard_witness_intents(run_id) VALUES(?)', (run_id,))
        if declaration:
            db.execute('INSERT INTO remote_guard_file_scopes VALUES(?,?,?,?,?,?,?)', (run_id, *declaration))
        if prospective:
            db.execute('INSERT INTO remote_guard_recovery_intents VALUES(?,?)', (run_id,time.time()))
        if restore:
            db.execute("INSERT INTO remote_restoration_runs VALUES(?,?)", (run_id, time.time() + budget))
    result = {"run_id": run_id, "reservation_id": r["reservation_id"], "host_id": host}
    if execution_intent is not None:result['execution']=execution_intent
    if declaration: result['declared_file_path'] = declaration[2]
    if prospective: result['prospective_recovery'] = True
    return result


def _check_guard(coord: Coordinator, db, row, session: str, token: str) -> dict:
    restoration = db.execute("SELECT deadline FROM remote_restoration_runs WHERE run_id=?", (row["run_id"],)).fetchone()
    r = coord._verify_owner(db, row["resource"], session, token, restoration is not None)
    from .core import assert_execution_domain_admission
    assert_execution_domain_admission(db,row['resource'],session,r['reservation_id'])
    if r["reservation_id"] != row["reservation_id"]: raise CoordError("guarded run reservation has changed")
    if restoration:
        if coord._state(r) != "stale": raise CoordError("restoration requires a stale reservation")
        if time.time() >= restoration["deadline"]: raise CoordError("restoration deadline has expired")
    result = {"state": coord._state(r), "resource": r["name"], "reservation_id": r["reservation_id"], "deadline": r["deadline"]}
    if restoration: result.update(restore=True, restoration_deadline=restoration["deadline"])
    return result


def _attach_guard_pid(coord: Coordinator, session: str, host: str, p: dict) -> dict:
    run_id, token, pid = _string(p.get("run_id"), "run_id"), _string(p.get("token"), "token"), p.get("pid")
    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0: raise CoordError("pid must be a positive integer")
    with coord.tx() as db:
        row = db.execute("SELECT * FROM remote_guarded_runs WHERE run_id=?", (run_id,)).fetchone()
        if not row or row["session_id"] != session or row["host_id"] != host or row["closed_at"] is not None or row["local_pid"] != 0: raise CoordError("remote guarded run cannot be attached")
        if (db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='remote_guard_witness_intents'").fetchone() and
                db.execute('SELECT 1 FROM remote_guard_witness_intents WHERE run_id=?', (run_id,)).fetchone()):
            raise CoordError('guarded run requires a host witness')
        _check_guard(coord, db, row, session, token)
        db.execute("UPDATE remote_guarded_runs SET local_pid=? WHERE run_id=?", (pid, run_id))
    return {"run_id": run_id, "attached": True}


def _guard_pulse(coord: Coordinator, session: str, host: str, p: dict, *, binding_scope=None) -> dict:
    run_id, token = _string(p.get("run_id"), "run_id"), _string(p.get("token"), "token")
    with coord.tx() as db:
        row = db.execute("SELECT * FROM remote_guarded_runs WHERE run_id=?", (run_id,)).fetchone()
        if not row or row["session_id"] != session or row["host_id"] != host or row["closed_at"] is not None:
            raise CoordError("remote guarded run is not open on this host")
        if (db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='remote_guard_witness_intents'").fetchone() and
                db.execute('SELECT 1 FROM remote_guard_witness_intents WHERE run_id=?', (run_id,)).fetchone() and
                not db.execute('SELECT 1 FROM remote_guard_witnesses WHERE run_id=?', (run_id,)).fetchone()):
            raise CoordError('guarded run requires its host witness before pulse')
        if (db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='remote_guard_file_scopes'").fetchone() and
                db.execute('SELECT 1 FROM remote_guard_file_scopes WHERE run_id=?', (run_id,)).fetchone() and
                not db.execute('SELECT 1 FROM remote_guard_file_checks WHERE run_id=?', (run_id,)).fetchone()):
            raise CoordError('declared file recheck is required before pulse')
        result=_check_guard(coord, db, row, session, token)
        if binding_scope is not None:
            from . import exact_file_bindings as bindings
            bindings.authorize(db,binding_scope['project'],session,host,row['resource'],binding_scope['binding_id'],
                output=binding_scope['output'],max_bytes=binding_scope['max_bytes'],reservation_id=row['reservation_id'])
            result.update(binding_id=binding_scope['binding_id'],output=binding_scope['output'],max_bytes=binding_scope['max_bytes'])
        return result


def _close_guard(coord: Coordinator, session: str, host: str, p: dict) -> dict:
    run_id, token = _string(p.get("run_id"), "run_id"), _string(p.get("token"), "token")
    evidence = _string(p.get("evidence_sha256"), "evidence_sha256")
    if len(evidence) != 64 or any(c not in "0123456789abcdef" for c in evidence): raise CoordError("closure evidence hash must be lowercase SHA-256")
    with coord.tx() as db:
        row = db.execute("SELECT * FROM remote_guarded_runs WHERE run_id=?", (run_id,)).fetchone()
        if not row or row["session_id"] != session or row["host_id"] != host:
            raise CoordError("remote guarded run does not belong to this host session")
        current = coord._verify_owner(db, row["resource"], session, token, True)
        if current["reservation_id"] != row["reservation_id"]:
            raise CoordError("guarded run reservation has changed")
        if row["closed_at"] is not None:
            if row["close_evidence_sha256"] != evidence:
                raise CoordError("guarded run closure evidence conflicts")
            return {"run_id": run_id, "closed": True}
        db.execute("UPDATE remote_guarded_runs SET closed_at=COALESCE(closed_at,?),close_evidence_sha256=? WHERE run_id=?",
                   (time.time(), evidence, run_id))
    return {"run_id": run_id, "closed": True}


def _remote_clear(coord: Coordinator, session: str, host: str, p: dict, action: str) -> dict:
    resource = coord.resource_name(_string(p.get("resource"), "resource"))
    receipt = p.get("receipt")
    evidence = p.get("evidence_base64")
    if not isinstance(receipt, dict) or not isinstance(evidence, str):
        raise CoordError("remote release/recovery requires receipt object and base64 evidence")
    try: evidence_bytes = base64.b64decode(evidence, validate=True)
    except ValueError as error: raise CoordError("remote receipt evidence is invalid base64") from error
    if not evidence_bytes or len(evidence_bytes) > 2 * 1024 * 1024:
        raise CoordError("remote receipt evidence is missing or too large")
    try:
        proofs = process_closure_proofs(receipt)
        reports = p.get('process_evidence_base64', {})
        if not isinstance(reports, dict) or set(reports) != {str(pid) for pid in proofs}:
            raise ValueError('process closure evidence must cover exactly the receipt proofs')
        for pid, proof in proofs.items():
            if not isinstance(reports[str(pid)], str):
                raise ValueError('process closure evidence is invalid base64')
            validate_process_evidence(proof, base64.b64decode(reports[str(pid)], validate=True))
    except ValueError as error: raise CoordError(str(error)) from error
    if receipt.get("resource") != resource or receipt.get("restored") is not True or receipt.get("processes_closed") is not True:
        raise CoordError("remote receipt does not bind a restored reservation")
    if not isinstance(receipt["closed_at"], (int, float)) or isinstance(receipt["closed_at"], bool) or not isinstance(receipt["pids"], list) or any(not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0 for pid in receipt["pids"]):
        raise CoordError("remote receipt has invalid closure data")
    with coord.tx() as db:
        from .core import assert_execution_domain_return
        assert_execution_domain_return(db,resource,receipt['closed_at'])
        coord._fresh(db, session)
        r = coord._row(db, resource)
        if not r or not r["owner_session"]:
            raise CoordError("resource is not held")
        if r["owner_session"] != session:
            raise CoordError("reservation proof requires its owning session")
        owner_host = db.execute("SELECT host_id FROM remote_session_hosts WHERE session_id=?", (r["owner_session"],)).fetchone()
        if not owner_host or owner_host["host_id"] != host:
            raise CoordError("cannot release or recover a reservation without the owning host's proof")
        if action == "release": coord._verify_owner(db, resource, session, _string(p.get("token"), "token"), True)
        if action == "recover" and coord._state(r) != "stale": raise CoordError("only a stale reservation can be recovered")
        if receipt["reservation_id"] != r["reservation_id"] or not r["granted_at"] <= receipt["closed_at"] <= time.time():
            raise CoordError("remote receipt does not bind this reservation")
        open_rows = db.execute("SELECT run_id,local_pid,host_id,started_at FROM remote_guarded_runs WHERE reservation_id=? AND closed_at IS NULL", (r["reservation_id"],)).fetchall()
        if action == "recover" and open_rows:
            # The server cannot inspect another host's PIDs.  It accepts only
            # the owning host's already client-validated receipt, and only when
            # each activated process is explicitly covered by that proof.
            # PID 0 is safe to clear only because the client gate never allows
            # execution until attach-guard-pid commits the actual process group.
            if any(row["host_id"] != host or (row["local_pid"] != 0 and row["local_pid"] not in receipt["pids"]) or proofs.get(row["local_pid"], receipt)["closed_at"] < row["started_at"] for row in open_rows):
                raise CoordError("remote open guarded run is not covered by this host receipt")
            db.executemany("UPDATE remote_guarded_runs SET closed_at=?,close_evidence_sha256=? WHERE run_id=?",
                           [(proofs.get(row["local_pid"], receipt)["closed_at"], proofs.get(row["local_pid"], {}).get('evidence_sha256', hashlib.sha256(evidence_bytes).hexdigest()), row["run_id"]) for row in open_rows])
        open_runs = db.execute("SELECT 1 FROM remote_guarded_runs WHERE reservation_id=? AND closed_at IS NULL", (r["reservation_id"],)).fetchone()
        if open_runs: raise CoordError("remote guarded run remains open")
        latest = db.execute("SELECT MAX(closed_at) value FROM remote_guarded_runs WHERE reservation_id=?", (r["reservation_id"],)).fetchone()["value"]
        if latest is not None and receipt["closed_at"] < latest: raise CoordError("remote receipt predates guarded run closure")
        canonical = json.dumps(receipt, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(canonical.encode()).hexdigest()
        if db.execute("SELECT 1 FROM receipts WHERE reservation_id=? OR receipt_sha256=?", (r["reservation_id"], digest)).fetchone():
            raise CoordError("receipt has already been used")
        evidence_digest = hashlib.sha256(evidence_bytes).hexdigest()
        recorded_at = time.time()
        db.execute("INSERT INTO receipts VALUES(?,?,?,?,?,?)", (r["reservation_id"], digest, evidence_digest, canonical, action, recorded_at))
        outcome = db.execute("SELECT resource,owner_session,host_id,state FROM remote_reservation_outcomes WHERE reservation_id=?", (r["reservation_id"],)).fetchone()
        if outcome:
            if (outcome["resource"], outcome["owner_session"], outcome["host_id"], outcome["state"]) != (resource, session, host, "pending"):
                raise CoordError("remote reservation outcome is inconsistent")
            if db.execute("UPDATE remote_reservation_outcomes SET state='cleared',action=?,receipt_sha256=?,evidence_sha256=?,cleared_at=? WHERE reservation_id=? AND state='pending'", (action, digest, evidence_digest, recorded_at, r["reservation_id"])).rowcount != 1:
                raise CoordError("remote reservation outcome could not be committed")
        db.execute("UPDATE resources SET owner_session=NULL,reservation_id=NULL,token=NULL,granted_at=NULL,deadline=NULL,stale=0,reason=NULL WHERE name=?", (resource,))
    return {"resource": resource, "released" if action == "release" else "recovered": True}

def _hospital_vendor_begin(coord,session,host,p,canonical_project,roots):
    from . import hospital_vendor_scope as scope, root_local, service
    from .capacity_admission import registry_authority_gate
    if (roots is None or canonical_project!=scope.PROJECT or set(p)!={'tuples','resource','control_rid','control_token','output_path'}):
        raise CoordError('invalid fixed vendor admission')
    for key in ('resource','control_rid','control_token','output_path'): _string(p.get(key),key)
    tuples=p['tuples']
    if not isinstance(tuples,list) or len(tuples)!=2 or not isinstance(tuples[1],dict): raise CoordError('invalid fixed vendor tuple')
    output_root=tuples[1].get('resource'); output=p['output_path']
    if (not isinstance(output_root,str) or not output_root.startswith('/') or output_root.endswith('/')
        or not output.startswith(output_root+'/') or any(x in ('','.','..') for x in output[1:].split('/'))
        or '\\' in output or any(ord(x)<32 or ord(x)==127 for x in output)):
        raise CoordError('fixed vendor output path differs')
    config=service._installed_server_config()
    if roots.projects.database!=config.master or roots.path('default').resolve()!=coord.path.resolve():
        raise CoordError('fixed vendor configured binding differs')
    registry=root_local._file(roots.projects.path,config.uid); st=registry.lstat()
    identity=(st.st_dev,st.st_ino,st.st_uid,st.st_mode)
    with registry_authority_gate(registry,config.uid,identity) as (gate,check):
        vendor={'control_rid':p['control_rid'],'control_token':p['control_token'],'resource':p['resource'],'gate':gate}
        from . import hospital_vendor_binding as vendor_binding
        from .capacity_admission import AdmissionRefused
        try: result=_begin_guard_batch(coord,session,host,{'tuples':tuples,'restore':False},vendor=vendor)
        except (vendor_binding.Refused,scope.Refused,AdmissionRefused) as error:
            raise CoordError('fixed vendor binding refused') from error
        check(); return result

def _hospital_vendor_dispatch(coord,session,host,op,p,canonical_project,roots):
    """Fixed vendor read pulse; requires separate owned copy-run guard."""
    from . import hospital_vendor_binding as vendor, hospital_vendor_scope as scope, root_local, service
    from .capacity_admission import registry_authority_gate, AdmissionRefused
    base={'resource','control_rid','control_token','run_id','run_token',
          'output_resource','output_rid','output_token','output_run_id','output_path'}
    required=base if op=='hospital-vendor-start' else base|{'operation_id'}
    if op=='hospital-vendor-finish': required=required|{'digest','byte_count'}
    if set(p)!=required or canonical_project!='default' or roots is None:
        raise CoordError('invalid fixed vendor request')
    for key in required-{'byte_count'}: _string(p.get(key),key)
    output=p['output_path']; output_root=p['output_resource']
    if (not output_root.startswith('/') or output_root.endswith('/') or not output.startswith(output_root+'/')
        or any(x in ('','.','..') for x in output[1:].split('/'))
        or '\\' in output or any(ord(x)<32 or ord(x)==127 for x in output)):
        raise CoordError('fixed vendor output path differs')
    config=service._installed_server_config()
    if roots.projects.database != config.master or roots.path('default').resolve()!=coord.path.resolve():
        raise CoordError('fixed vendor configured binding differs')
    registry=root_local._file(roots.projects.path,config.uid); st=registry.lstat()
    identity=(st.st_dev,st.st_ino,st.st_uid,st.st_mode)
    with registry_authority_gate(registry,config.uid,identity) as (gate,check):
        with coord.tx() as db:
            row=db.execute('SELECT * FROM remote_guarded_runs WHERE run_id=?',(p['run_id'],)).fetchone()
            if (not row or row['resource']!=scope.RUN_RESOURCE or row['session_id']!=session
                or row['host_id']!=host or row['closed_at'] is not None or row['local_pid']<=0):
                raise CoordError('fixed vendor requires the owned task-copy guard')
            run_check=_check_guard(coord,db,row,session,p['run_token'])
            out=db.execute('SELECT * FROM remote_guarded_runs WHERE run_id=?',(p['output_run_id'],)).fetchone()
            if (not p['output_resource'].startswith('/') or not out or out['resource']!=p['output_resource']
                or out['reservation_id']!=p['output_rid'] or out['session_id']!=session
                or out['host_id']!=host or out['closed_at'] is not None or out['local_pid']<=0
                or out['local_pid']!=row['local_pid']):
                raise CoordError('fixed vendor requires the owned output guard')
            out_check=_check_guard(coord,db,out,session,p['output_token'])
            if min(run_check['deadline'],out_check['deadline'])-time.time()<=2700:
                raise CoordError('fixed vendor cleanup reserve unavailable')
            args=(db,session,host,p['control_rid'],p['control_token'],p['resource'],p['run_id'],p['output_run_id'],p['output_resource'],p['output_path'])
            try:
                if op=='hospital-vendor-start': result=vendor.start_file_in_transaction(*args,gate)
                elif op=='hospital-vendor-pulse': result=vendor.pulse_file_in_transaction(*args,p['operation_id'],gate)
                else: result=vendor.finish_file_in_transaction(*args,p['operation_id'],p['digest'],p['byte_count'],gate)
            except (vendor.Refused,scope.Refused,AdmissionRefused) as error:
                raise CoordError('fixed vendor binding refused') from error
            check()
            return result
