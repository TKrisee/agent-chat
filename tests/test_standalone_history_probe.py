from agent_chat.core import first_ack_lookup
from agent_chat.remote_service import dispatch
from test_history_move import HistoryMoveTests

class StandaloneHistoryProbe(HistoryMoveTests):
    def test_real_move_preserves_matching_standalone_evidence(self):
        for coord, session in ((self.source,'old-agent'), (self.destination,'new-agent')):
            dispatch(coord,dict(op='register',session=session,host_id='private-history-host',params=dict(agent='maintainer')))
        source=self.send('source-operator','old-agent','Inert history source')
        self.source.session='old-agent'
        self.source.message(source['id']);self.source.acknowledge(source['id'])
        before=first_ack_lookup(self.source.path,project_id='source',session_id='old-agent',host_id='private-history-host',source_id=source['id'],expected_recipient='source-operator')
        self.assertEqual(before['status'],'not_recorded_at_read')
        result=self.execute()
        old_messages=self.source.db.execute('SELECT count(*) FROM messages WHERE id=?',(source['id'],)).fetchone()[0]
        old_evidence=self.source.db.execute('SELECT count(*) FROM standalone_message_acknowledgements WHERE message_id=?',(source['id'],)).fetchone()[0]
        new_evidence=self.destination.db.execute('SELECT count(*) FROM standalone_message_acknowledgements WHERE message_id=?',(source['id'],)).fetchone()[0]
        after=first_ack_lookup(self.destination.path,project_id='destination',session_id='new-agent',host_id='private-history-host',source_id=source['id'],expected_recipient='destination-operator')
        print('move_status='+result['status']+' source_messages='+str(old_messages)+' source_evidence='+str(old_evidence)+' destination_evidence='+str(new_evidence)+' before='+before['status']+' after='+after['status'],flush=True)
        self.assertEqual(after['status'],'not_recorded_at_read')
        self.assertEqual(old_evidence,0)
        self.assertEqual(new_evidence,1)

    def test_legacy_absent_table_preserves_fingerprint_and_unknown(self):
        import sqlite3
        from agent_chat.history_move import fingerprint,record
        for coord, session in ((self.source,'old-agent'), (self.destination,'new-agent')):
            dispatch(coord,dict(op='register',session=session,host_id='private-history-host',params=dict(agent='maintainer')))
        source=self.send('source-operator','old-agent','legacy marker')
        self.source.db.execute('UPDATE messages SET acked_at=1 WHERE id=?',(source['id'],))
        row=self.source.db.execute('SELECT * FROM messages WHERE id=?',(source['id'],)).fetchone()
        with_empty=record(self.source.db,row)
        old={k:v for k,v in with_empty.items() if k!='standalone_message_acknowledgements'}
        self.assertEqual(fingerprint(with_empty),fingerprint(old))
        self.source.db.execute('DROP TABLE standalone_message_acknowledgements')
        self.assertEqual(fingerprint(record(self.source.db,row)),fingerprint(old))
        self.assertEqual(self.execute()['status'],'completed')
        result=first_ack_lookup(self.destination.path,project_id='destination',session_id='new-agent',host_id='private-history-host',source_id=source['id'],expected_recipient='destination-operator')
        self.assertEqual(result['status'],'inconsistent')
        self.assertEqual(self.destination.db.execute('SELECT count(*) FROM standalone_message_acknowledgements').fetchone()[0],0)

    def test_mismatched_evidence_refuses_move_without_deletion(self):
        from agent_chat.core import CoordError
        source=self.send('source-operator','old-agent','bad metadata')
        self.source.session='old-agent'; self.source.acknowledge(source['id'])
        self.source.db.execute('UPDATE standalone_message_acknowledgements SET recipient_session=? WHERE message_id=?',('other-agent',source['id']))
        with self.assertRaisesRegex(CoordError,'inconsistent standalone acknowledgement'):
            self.execute()
        self.assertEqual(self.source.db.execute('SELECT count(*) FROM messages WHERE id=?',(source['id'],)).fetchone()[0],1)
        self.assertEqual(self.source.db.execute('SELECT count(*) FROM standalone_message_acknowledgements WHERE message_id=?',(source['id'],)).fetchone()[0],1)
        self.assertEqual(self.destination.db.execute('SELECT count(*) FROM messages WHERE id=?',(source['id'],)).fetchone()[0],0)

    def test_interrupted_import_retries_without_duplicate_evidence(self):
        from unittest import mock
        source=self.send('source-operator','old-agent','interrupted')
        self.source.session='old-agent';self.source.acknowledge(source['id'])
        preview=self.move.preview(self.request)
        with mock.patch.object(self.move,'remove_source',side_effect=RuntimeError('private interruption')):
            with self.assertRaisesRegex(RuntimeError,'private interruption'):
                self.move.execute(self.request,preview['count'],preview['digest'])
        self.assertEqual(self.source.db.execute('SELECT count(*) FROM standalone_message_acknowledgements').fetchone()[0],1)
        self.assertEqual(self.destination.db.execute('SELECT count(*) FROM standalone_message_acknowledgements').fetchone()[0],1)
        self.assertEqual(self.move.execute(self.request,preview['count'],preview['digest'])['status'],'completed')
        self.assertEqual(self.source.db.execute('SELECT count(*) FROM standalone_message_acknowledgements').fetchone()[0],0)
        self.assertEqual(self.destination.db.execute('SELECT count(*) FROM standalone_message_acknowledgements').fetchone()[0],1)
