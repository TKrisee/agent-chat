"""Reporting-only API workflow; synthetic consumption inputs, no native fixture."""
import contextlib
import subprocess
import uuid
import unittest
from agent_chat.bridge_state import BridgeState
from agent_chat.core import Coordinator
from agent_chat.remote import RemoteCoordError
from agent_chat.supervisor import SupervisorState
from test_remote_web import RemoteWebFixture

class Rows:
    def __init__(self,rows): self.rows=rows
    def fetchall(self): return self.rows
    def __iter__(self): return iter(self.rows)
    def fetchone(self): return self.rows[0] if self.rows else None

class CorruptAssociationRead:
    """Read-result corruption simulation, never a database/history write."""
    def __init__(self,db,change): self.db,self.change=db,change
    def execute(self,sql,args=()):
        result=self.db.execute(sql,args)
        if 'FROM reply_ack_idempotency a' in sql:
            rows=[dict(row) for row in result.fetchall()]
            return Rows(self.change(rows))
        return result

class SupervisorAckReportingTests(unittest.TestCase):
    def test_owned_reporting_workflow_and_fail_closed_associations(self):
        case=RemoteWebFixture()
        case.setUp()
        self.addCleanup(case.doCleanups)
        self.addCleanup(case.tearDown)
        for sid in ('owner-fixture','target-fixture','third-fixture'):
            case.call('register',session=sid,host=sid+'-host',agent=sid)
        thread=str(uuid.uuid4())
        case.call('bind',session='owner-fixture',host='owner-fixture-host',thread=thread)
        coord=Coordinator(case.db,session='owner-fixture')
        self.addCleanup(coord.close)
        bridge=BridgeState(coord)
        state=SupervisorState(coord)
        def send(sender,recipient,body='identical content',**options):
            return case.call('send',session=sender,host=sender+'-host',to=recipient,body=body,**options)
        def prepared(messages):
            job=bridge.prepare(thread,[dict(id=m['id'],recipient_session='owner-fixture') for m in messages],'never submitted')
            ids=subprocess.run(['jq','-cn','--args','$ARGS.positional',*[m['id'] for m in messages]],capture_output=True,text=True,check=True).stdout
            return dict(native_consumed=1,wake_id=job['id'],owner_session='target-fixture',target_session='owner-fixture',message_ids_json=ids)
        def transport(message):
            case.call('acknowledge',session='owner-fixture',host='owner-fixture-host',id=message['id'])
        def first_ack():
            source=send('owner-fixture','target-fixture')
            case.call('context',session='target-fixture',host='target-fixture-host',message_ids=[source['id']])
            ack=send('target-fixture','owner-fixture',reply_to=source['id'],ack_reply=True)
            self.assertTrue(ack['acknowledged_reply'])
            return source,ack
        source,ack=first_ack()
        recovery=prepared([ack])
        before=state._recovery_reporting(recovery)
        self.assertEqual(before['state'],'awaiting_notice_acknowledgement')
        self.assertEqual(before['informational_ack_count'],1)
        self.assertIsNone(state._recovery_reporting(dict(recovery,native_consumed=0)))
        transport(ack)
        self.assertEqual(state._recovery_reporting(recovery)['state'],'reported')
        evidence=state._recovery_reporting_evidence(recovery)
        self.assertTrue(evidence['complete'])
        item=evidence['deliveries'][0]
        self.assertEqual(item['first_ack_role'],{'state':'verified','source_message_id':source['id']})
        self.assertEqual(item['source_reply_obligation'],'not_required_informational_ack')
        self.assertEqual(item['factual_result'],'UNRECORDED')
        self.assertEqual(item['substantive_ack'],'NOT_DEMONSTRATED')
        self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM message_replies WHERE reply_to=?',(ack['id'],)).fetchone()[0],0)
        # Identical body with no server ACK marker is still a real obligation.
        for options in ({},{'reply_to':source['id']}):
            with self.subTest(ordinary=options):
                ordinary=send('target-fixture','owner-fixture',**options)
                job=prepared([ordinary])
                transport(ordinary)
                self.assertEqual(state._recovery_reporting(job)['state'],'awaiting_acknowledged_reply')
                self.assertEqual(state._report_first_ack_role(ordinary['id'])['state'],'not_recorded')
        _,second_ack=first_ack()
        task=send('target-fixture','owner-fixture',body='new independent task')
        mixed=prepared([second_ack,task])
        transport(second_ack);transport(task)
        self.assertEqual(state._recovery_reporting(mixed)['state'],'awaiting_acknowledged_reply')
        # Public invalid marker creation never gains an exemption.
        with self.assertRaises(RemoteCoordError):
            send('third-fixture','owner-fixture',reply_to=source['id'],ack_reply=True)
        with self.assertRaises(RemoteCoordError):
            case.call('acknowledge',session='owner-fixture',host='wrong-host',id=ack['id'])
        # Wrong/missing/duplicate/uncertain structural data is read-simulated,
        # not inserted into immutable coordinator history.
        changes=[lambda rows:[],lambda rows:rows+rows,
                 lambda rows:[dict(rows[0],ack_sender='third-fixture')],
                 lambda rows:[dict(rows[0],ack_parent='message_wrong')],
                 lambda rows:[dict(rows[0],parent_recipient=None)],
                 lambda rows:[dict(rows[0],fingerprint='invalid')],
                 lambda rows:[dict(rows[0],parent_seq=rows[0]['seq'])]]
        real_db=state.db
        for change in changes:
            with self.subTest(corrupt=changes.index(change)):
                state.db=CorruptAssociationRead(real_db,change)
                try:
                    self.assertNotEqual(state._report_first_ack_role(ack['id'])['state'],'verified')
                    self.assertEqual(state._recovery_reporting(recovery)['state'],'awaiting_acknowledged_reply')
                finally:
                    state.db=real_db
        self.assertEqual(state._report_first_ack_role(ack['id'])['state'],'verified')
        self.assertEqual(coord.db.execute('SELECT COUNT(*) FROM reply_ack_idempotency WHERE message_id=?',(ack['id'],)).fetchone()[0],1)

        class MissingAssociationTable:
            def execute(self,sql,args=()):
                if "sqlite_master" in sql and "name='reply_ack_idempotency'" in sql:
                    return Rows([])
                return real_db.execute(sql,args)
        state.db=MissingAssociationTable()
        try:
            self.assertEqual(state._report_first_ack_role(ack['id'])['state'],'unknown')
            self.assertEqual(state._recovery_reporting(recovery)['state'],'awaiting_acknowledged_reply')
        finally:
            state.db=real_db
        # Trusted informational-notice read fixture only; never forged into DB.
        class NoticeRead:
            def execute(self,sql,args=()):
                result=real_db.execute(sql,args)
                if 'SELECT d.message_id,m.acked_at' in sql:
                    return Rows([dict(row,informational=1) for row in result.fetchall()])
                if 'FROM supervisor_notice_provenance WHERE message_id=' in sql:
                    return Rows([{'sender_session':'target-fixture','recipient_session':'owner-fixture'}])
                return result
        state.db=NoticeRead()
        try:
            self.assertEqual(state._recovery_reporting(job)['state'],'reported')
            self.assertEqual(state._recovery_reporting(job)['informational_notice_count'],1)
            self.assertEqual(state._recovery_reporting_evidence(job)['deliveries'][0]['source_reply_obligation'],
                             'not_required_informational_notice')
        finally:
            state.db=real_db
