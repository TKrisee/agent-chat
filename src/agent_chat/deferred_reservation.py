"""Prospective server-mediated non-activation, never physical no-write proof."""
import time
import uuid
from .core import (CoordError, assert_agent_running, assert_original_owner_admission,
                   assert_execution_domain_admission, assert_execution_domain_return)
from .guard_witness import task_file_scope


def dispatch(coord, session, host, project, roots, op, params):
    required = {'request_id'}
    if op == 'deferred-request': required |= {'resource','minutes','task_id','source_id'}
    if set(params) != required or not roots or not project:
        raise CoordError('invalid deferred reservation parameters or project')
    request_id = params['request_id']
    try:
        if not isinstance(request_id,str) or str(uuid.UUID(request_id)) != request_id:
            raise ValueError()
    except (ValueError, AttributeError):
        raise CoordError('deferred request_id must be a canonical UUID')
    if op == 'deferred-request':
        resource = coord.resource_name(params['resource'])
        if resource != params['resource'] or not resource.startswith('file:'):
            raise CoordError('deferred reservation requires canonical file resource')
        # Existing requests are reconciled by status; a new attempt never
        # adopts or reissues an existing grant.
        def record(db, row):
            if db.execute('SELECT 1 FROM deferred_reservations WHERE request_id=?',(request_id,)).fetchone():
                raise CoordError('deferred request already exists; reconcile status')
            scope = task_file_scope(db, roots.projects, project, session, host,
                                    resource, params['task_id'], params['source_id'])
            now = time.time()
            db.execute('INSERT INTO deferred_reservations VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                       (request_id,row['reservation_id'],resource,session,host,*scope,'deferred',now,now))
            db.execute("INSERT INTO remote_reservation_outcomes (reservation_id,resource,owner_session,host_id,state,created_at) VALUES(?,?,?,?,?,?)",
                       (row['reservation_id'],resource,session,host,'pending',now))
        result = coord.request(resource,params['minutes'],record,new_only=True)
        result.update(request_id=request_id, deferred_state='deferred', authority_issued=False)
        return result
    with coord.tx() as db:
        coord._fresh(db,session)
        row = db.execute('SELECT * FROM deferred_reservations WHERE request_id=?',(request_id,)).fetchone()
        if not row or (row['owner_session'],row['host_id'],row['project_id']) != (session,host,project):
            raise CoordError('deferred reservation unavailable to this owner, host and project')
        result = {'request_id':request_id,'reservation_id':row['reservation_id'],
                  'resource':row['resource'],'state':row['state'],
                  'authority_issued':row['state']=='active',
                  'physical_effects':'unknown'}
        if op == 'deferred-status': return result
        current=coord._row(db,row['resource'])
        if row['state']=='cancelled':
            if op=='deferred-cancel': return result
            raise CoordError('cancelled reservation cannot activate')
        if not current or (current['owner_session'],current['reservation_id']) != (session,row['reservation_id']):
            raise CoordError('deferred reservation ownership changed')
        if row['state']=='active':
            if op=='deferred-cancel': raise CoordError('activated reservation cannot return unused')
            if coord._state(current) != 'owned': raise CoordError('activated reservation is stale')
            coord._verify_owner(db,row['resource'],session,current['token'])
            assert_execution_domain_admission(db,row['resource'],session,row['reservation_id'])
            assert_agent_running(db,session,cleanup=True)
            assert_original_owner_admission(db,session)
            actual=task_file_scope(db,roots.projects,project,session,host,row['resource'],row['task_id'],row['source_id'])
            if actual != tuple(row[k] for k in ('project_id','workspace','path','task_id','source_id','thread_id')):
                raise CoordError('deferred reservation scope changed')
            result['token']=current['token']
            return result
        if op=='deferred-activate':
            if coord._state(current) != 'owned': raise CoordError('deferred reservation is stale')
            coord._verify_owner(db,row['resource'],session,current['token'])
            assert_execution_domain_admission(db,row['resource'],session,row['reservation_id'])
            assert_agent_running(db,session,cleanup=True)
            assert_original_owner_admission(db,session)
            actual=task_file_scope(db,roots.projects,project,session,host,row['resource'],row['task_id'],row['source_id'])
            if actual != tuple(row[k] for k in ('project_id','workspace','path','task_id','source_id','thread_id')):
                raise CoordError('deferred reservation scope changed')
            db.execute("UPDATE deferred_reservations SET state='active',changed_at=? WHERE request_id=? AND state='deferred'",(time.time(),request_id))
            result.update(state='active',authority_issued=True,token=current['token'])
            return result
        # Preserve existing execution-domain return barriers; an unexpected
        # run contradicts the no-authority record and must fail closed.
        assert_execution_domain_return(db,row['resource'])
        for table in ('guarded_runs','remote_guarded_runs'):
            if db.execute('SELECT 1 FROM '+table+' WHERE reservation_id=? LIMIT 1',(row['reservation_id'],)).fetchone():
                raise CoordError('deferred reservation has unexpected execution history')
        db.execute("UPDATE deferred_reservations SET state='cancelled',changed_at=? WHERE request_id=? AND state='deferred'",(time.time(),request_id))
        db.execute('UPDATE resources SET owner_session=NULL,reservation_id=NULL,token=NULL,granted_at=NULL,deadline=NULL,stale=0,reason=NULL WHERE name=? AND reservation_id=?',(row['resource'],row['reservation_id']))
        result.update(state='cancelled',authority_issued=False,server_recorded_non_activation=True)
        return result
