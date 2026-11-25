"""Durable explicit operator decision marks and author-recorded replies."""
from __future__ import annotations

import contextlib
import sqlite3
import tempfile
import unittest
from pathlib import Path

from agent_chat.core import CoordError, Coordinator
from agent_chat.remote_service import dispatch
from agent_chat.web import message_rows


class OperatorDecisionTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='agent-chat-decisions-')
        self.addCleanup(self.tmp.cleanup)
        self.db_path = Path(self.tmp.name) / 'state.sqlite3'
        self.root = Coordinator(self.db_path, 'root')
        self.addCleanup(self.root.close)
        self.root.register('Root')
        self.operator = Coordinator(self.db_path, 'operator')
        self.addCleanup(self.operator.close)
        self.operator.register('Operator')
        self.other = Coordinator(self.db_path, 'other')
        self.addCleanup(self.other.close)
        self.other.register('Other')
        self.agent = Coordinator(self.db_path, 'agent')
        self.addCleanup(self.agent.close)
        self.agent.register('Agent')
        with contextlib.closing(sqlite3.connect(self.db_path)) as db, db:
            db.execute('INSERT INTO operator_identities(session_id) VALUES(?)', ('operator',))
            db.execute('INSERT INTO operator_identities(session_id) VALUES(?)', ('other',))

    def message_projection(self, message_id):
        with contextlib.closing(sqlite3.connect(self.db_path)) as db:
            db.row_factory = sqlite3.Row
            rows = db.execute('SELECT * FROM messages WHERE id=?', (message_id,)).fetchall()
            return message_rows(rows, {}, db)[0]

    def remote(self, coord, session, op, params):
        return dispatch(coord, {'op': op, 'session': session, 'host_id': 'decision-test-host', 'params': params},
                        canonical_project='default')

    def first_root_reply(self, source_id, body):
        # Complete own-source read and real first-ACK association through the API.
        self.root.message(source_id)
        return self.root.send('operator', body, reply_to=source_id, ack_reply=True)

    def test_flagged_send_is_direct_and_requires_operator_recipient(self):
        question = self.root.send('operator', 'PRIVATE_QUESTION', decision_needed=True)
        row = self.message_projection(question['id'])
        self.assertEqual(row['decision']['status'], 'pending')
        self.assertIsNone(row['decision']['reply_message_id'])
        with self.assertRaisesRegex(CoordError, 'registered operator'):
            self.root.send('agent', 'NOT_A_DECISION', decision_needed=True)
        with self.assertRaisesRegex(CoordError, 'differ from the author'):
            self.operator.send('operator', 'SELF', decision_needed=True)
        with self.assertRaisesRegex(CoordError, 'new direct message'):
            self.root.send('operator', 'REPLY_MARK', reply_to=question['id'], decision_needed=True)

    def test_existing_message_annotation_is_author_only_and_idempotent(self):
        question = self.root.send('operator', 'EXISTING_QUESTION')
        first = self.root.mark_decision_needed(question['id'])
        second = self.root.mark_decision_needed(question['id'])
        self.assertTrue(first['created'])
        self.assertFalse(second['created'])
        self.assertEqual(self.message_projection(question['id'])['decision']['status'], 'pending')
        with self.assertRaisesRegex(CoordError, 'authored by the caller'):
            self.agent.mark_decision_needed(question['id'])
        with self.assertRaisesRegex(CoordError, 'registered operator'):
            self.operator.mark_decision_needed(self.operator.send('root', 'NOT_ROOT_AUTHORED')['id'])

    def test_contextual_reply_question_can_be_marked_without_rewriting_thread_or_ack(self):
        source = self.operator.send('root', 'OPERATOR_SOURCE')
        question = self.root.send('operator', 'CONTEXTUAL_DECISION',
                                  reply_to=source['id'], ack_reply=True)
        with contextlib.closing(sqlite3.connect(self.db_path)) as db, db:
            source_before = db.execute('SELECT body,acked_at FROM messages WHERE id=?',
                                       (source['id'],)).fetchone()
            question_before = db.execute(
                'SELECT body FROM messages WHERE id=?', (question['id'],)).fetchone()
            ancestry_before = db.execute(
                'SELECT reply_to FROM message_replies WHERE message_id=?',
                (question['id'],)).fetchone()

        self.assertIsNotNone(source_before[1], 'the reply may acknowledge its parent as usual')
        self.assertTrue(self.root.mark_decision_needed(question['id'])['created'])
        operator_reply = self.operator.send('root', 'CONDITIONAL_REPLY',
                                            reply_to=question['id'])
        outcome = self.root.record_decision_answer(question['id'], operator_reply['id'],
                                                   'conditional')
        self.assertTrue(outcome['recorded'])

        with contextlib.closing(sqlite3.connect(self.db_path)) as db, db:
            source_after = db.execute('SELECT body,acked_at FROM messages WHERE id=?',
                                      (source['id'],)).fetchone()
            question_after = db.execute(
                'SELECT body FROM messages WHERE id=?', (question['id'],)).fetchone()
            ancestry_after = db.execute(
                'SELECT reply_to FROM message_replies WHERE message_id=?',
                (question['id'],)).fetchone()
        self.assertEqual(tuple(source_after), tuple(source_before))
        self.assertEqual(tuple(question_after), tuple(question_before))
        self.assertEqual(ancestry_after[0], ancestry_before[0])
        self.assertEqual(ancestry_after[0], source['id'])
        self.assertEqual(self.message_projection(question['id'])['decision']['answer_outcome'],
                         'conditional')

    def test_typed_remote_author_operations_and_exact_parameter_shape(self):
        sent = self.remote(self.root, 'root', 'send', {
            'to': 'operator', 'body': 'REMOTE_DECISION', 'decision_needed': True,
        })
        self.assertEqual(sent['recipient_session'], 'operator')
        reply = self.operator.send('root', 'REMOTE_CONDITIONAL_REPLY', reply_to=sent['id'])
        recorded = self.remote(self.root, 'root', 'decision-answer', {
            'id': sent['id'], 'reply_id': reply['id'], 'outcome': 'conditional',
        })
        self.assertEqual(recorded['outcome'], 'conditional')
        marked = self.root.send('operator', 'EXISTING_REMOTE_MARK')
        result = self.remote(self.root, 'root', 'decision-mark', {'id': marked['id']})
        self.assertTrue(result['decision_needed'])
        with self.assertRaisesRegex(CoordError, 'invalid decision mark parameters'):
            self.remote(self.root, 'root', 'decision-mark', {'id': marked['id'], 'author': 'operator'})

    def test_answer_requires_exact_recipient_reply_and_records_conditional_separately(self):
        question = self.root.send('operator', 'MAINTENANCE_DECISION', decision_needed=True)
        unrelated = self.operator.send('root', 'UNRELATED_REPLY')
        with self.assertRaisesRegex(CoordError, 'exact direct reply'):
            self.root.record_decision_answer(question['id'], unrelated['id'], 'conditional')
        reply = self.operator.send('root', 'CONDITIONAL_ANSWER', reply_to=question['id'])
        before = self.message_projection(question['id'])
        self.assertEqual(before['decision']['status'], 'reply_received')
        first = self.root.record_decision_answer(question['id'], reply['id'], 'conditional')
        repeated = self.root.record_decision_answer(question['id'], reply['id'], 'conditional')
        self.assertTrue(first['recorded'])
        self.assertFalse(repeated['recorded'])
        with self.assertRaisesRegex(CoordError, 'already recorded differently'):
            self.root.record_decision_answer(question['id'], reply['id'], 'accepted')
        projected = self.message_projection(question['id'])['decision']
        self.assertEqual(projected['status'], 'answer_recorded')
        self.assertEqual(projected['answer_outcome'], 'conditional')
        self.assertEqual(projected['reply_message_id'], reply['id'])
        with contextlib.closing(sqlite3.connect(self.db_path)) as db, db:
            original = db.execute('SELECT body,acked_at FROM messages WHERE id=?', (question['id'],)).fetchone()
        self.assertEqual(original[0], 'MAINTENANCE_DECISION')
        self.assertIsNone(original[1], 'recording an outcome does not rewrite or ACK the original message')

    def test_clarification_outcome_keeps_original_decision_pending(self):
        question = self.root.send('operator', 'NESTED_DECISION', decision_needed=True)
        clarification = self.operator.send('root', 'PLEASE_EXPLAIN', reply_to=question['id'])
        recorded = self.root.record_decision_answer(question['id'], clarification['id'], 'clarification')
        self.assertTrue(recorded['recorded'])
        self.assertEqual(self.message_projection(question['id'])['decision']['status'], 'pending')

    def test_followup_answer_in_same_nested_dialogue_records_original_decision(self):
        self.operator._operator_message_origin = True
        question = self.root.send('operator', 'NESTED_DECISION', decision_needed=True)
        clarification = self.operator.send('root', 'PLEASE_EXPLAIN', reply_to=question['id'])
        explanation = self.first_root_reply(clarification['id'], 'EXPLANATION')
        answer = self.operator.send('root', 'ACTUAL_ANSWER', reply_to=explanation['id'])
        recorded = self.root.record_decision_answer(question['id'], answer['id'], 'accepted')
        self.assertTrue(recorded['recorded'])
        decision = self.message_projection(question['id'])['decision']
        self.assertEqual(decision['status'], 'answer_recorded')
        self.assertEqual(decision['reply_message_id'], answer['id'])
        self.assertEqual(decision['answer_outcome'], 'accepted')

    def test_group_message_cannot_be_annotated_and_other_operator_cannot_record(self):
        deliveries = self.root.send_many(['operator', 'agent'], 'GROUP_QUESTION')
        batch_question = next(item for item in deliveries if item['recipient_session'] == 'operator')
        with self.assertRaisesRegex(CoordError, 'batch delivery'):
            self.root.mark_decision_needed(batch_question['id'])
        question = self.root.send('operator', 'OTHER_AUTHOR_CHECK', decision_needed=True)
        reply = self.operator.send('root', 'REPLY', reply_to=question['id'])
        with self.assertRaisesRegex(CoordError, 'only the original message author'):
            self.other.record_decision_answer(question['id'], reply['id'], 'accepted')

    def ui_reply(self, parent, body='OPERATOR_INPUT', actor=None):
        actor = actor or self.operator
        actor._operator_message_origin = True
        try:
            return actor.send('root', body, reply_to=parent)
        finally:
            actor._operator_message_origin = False

    def dialogue(self):
        question = self.root.send('operator', 'PRIVATE_DECISION', decision_needed=True)
        clarification = self.ui_reply(question['id'], 'PLEASE_EXPLAIN')
        explanation = self.root.send('operator', 'EXPLANATION', reply_to=clarification['id'], ack_reply=True)
        answer = self.ui_reply(explanation['id'], 'ACTUAL_INPUT')
        return question, clarification, explanation, answer

    def rows(self, table):
        with contextlib.closing(sqlite3.connect(self.db_path)) as db:
            return db.execute('SELECT * FROM '+table+' ORDER BY rowid').fetchall()

    def test_actual_clarification_then_nested_answer_and_readonly_author_status(self):
        question = self.root.send('operator', 'CLARIFY_INPUT', decision_needed=True)
        clarification = self.ui_reply(question['id'])
        self.root.record_decision_answer(question['id'], clarification['id'], 'clarification')
        pending = self.root.decision_status(question['id'])
        self.assertFalse(pending['terminal']); self.assertEqual(pending['status'], 'pending')
        self.assertEqual(pending['answer_outcome'], 'clarification')
        explanation = self.root.send('operator', 'EXPLANATION', reply_to=clarification['id'], ack_reply=True)
        answer = self.ui_reply(explanation['id'], 'PRIVATE_ENVIRONMENT_INPUT_DO_NOT_EXPOSE')
        self.assertEqual(self.root.decision_status(question['id'])['status'], 'reply_received')
        preserved = {name:self.rows(name) for name in ('messages','message_replies','operator_message_origins','message_reads','operator_decision_answers')}
        self.root.record_decision_answer(question['id'], answer['id'], 'accepted')
        self.assertFalse(self.root.record_decision_answer(question['id'], clarification['id'], 'clarification')['recorded'])
        status = self.root.decision_status(question['id'])
        self.assertTrue(status['terminal']); self.assertEqual(len(status['answer_history']), 2)
        self.assertEqual(status['reply_message_id'], answer['id'])
        self.assertEqual(status['answer_origin'], 'authenticated_operator_ui')
        self.assertNotIn('body', status); self.assertNotIn('PRIVATE_ENVIRONMENT_INPUT_DO_NOT_EXPOSE', repr(status))
        self.assertTrue(all('reply_seq' not in event for event in status['answer_history']))
        self.assertEqual(preserved, {name:self.rows(name) for name in preserved})
        before = self.rows('operator_decision_events')
        self.root.decision_status(question['id'])
        self.assertEqual(before,self.rows('operator_decision_events'))

    def test_legacy_clarification_persists_with_new_nested_terminal_without_backfill(self):
        question, clarification, explanation, answer = self.dialogue()
        self.root.db.execute('INSERT INTO operator_decision_answers VALUES(?,?,?,?,?)',
                             (question['id'],clarification['id'],'root','clarification',123.0)); self.root.db.commit()
        before = self.rows('operator_decision_answers')
        self.assertEqual(self.root.decision_status(question['id'])['status'], 'reply_received')
        self.root.record_decision_answer(question['id'],answer['id'],'conditional')
        self.assertEqual(before,self.rows('operator_decision_answers'))
        self.assertEqual(len(self.rows('operator_decision_events')),1)
        self.assertTrue(self.root.decision_status(question['id'])['answer_history'][0]['legacy'])
        for table in ('operator_decision_answers','operator_decision_events'):
            with self.assertRaises(sqlite3.IntegrityError):self.root.db.execute('UPDATE '+table+" SET outcome='declined'")
            with self.assertRaises(sqlite3.IntegrityError):self.root.db.execute('DELETE FROM '+table)
            self.root.db.rollback()

    def test_legacy_direct_terminal_stays_terminal_despite_later_reply(self):
        question=self.root.send('operator','OLD_DECISION',decision_needed=True)
        reply=self.operator.send('root','OLD_DIRECT',reply_to=question['id'])
        self.root.db.execute('INSERT INTO operator_decision_answers VALUES(?,?,?,?,?)',(question['id'],reply['id'],'root','accepted',123.0));self.root.db.commit()
        later=self.ui_reply(question['id'],'LATER_COMMENT')
        status=self.root.decision_status(question['id'])
        self.assertTrue(status['terminal']);self.assertEqual(status['reply_message_id'],reply['id'])
        self.assertEqual(status['latest_reply_message_id'],later['id'])
        self.assertEqual(status['answer_origin'],'registered_operator_direct')
        self.assertFalse(self.root.record_decision_answer(question['id'],reply['id'],'accepted')['recorded'])
        with self.assertRaises(CoordError):self.root.record_decision_answer(question['id'],later['id'],'declined')
        self.assertEqual(self.rows('operator_decision_events'),[])

    def test_direct_registered_operator_origin_compatibility_is_not_ui_promotion(self):
        question=self.root.send('operator','DIRECT_COMPATIBILITY',decision_needed=True)
        reply=self.operator.send('root','DIRECT_INPUT',reply_to=question['id'])
        self.root.record_decision_answer(question['id'],reply['id'],'declined')
        self.assertEqual(self.root.decision_status(question['id'])['answer_origin'],'registered_operator_direct')
        self.assertEqual(self.rows('operator_message_origins'),[])

    def test_nested_requires_every_operator_origin_without_direct_fallback(self):
        question=self.root.send('operator','NESTED_ORIGIN',decision_needed=True)
        unstamped=self.operator.send('root','UNSTAMPED',reply_to=question['id'])
        explanation=self.first_root_reply(unstamped['id'], 'EXPLAIN')
        final=self.ui_reply(explanation['id'])
        with self.assertRaisesRegex(CoordError,'authenticated operator UI'):
            self.root.record_decision_answer(question['id'],final['id'],'accepted')
        self.assertEqual(self.rows('operator_decision_events'),[])
        self.assertFalse(self.root.decision_status(question['id'])['terminal'])

    def test_missing_final_or_contradictory_author_origin_refuses(self):
        question=self.root.send('operator','ORIGIN_CHECK',decision_needed=True)
        clarification=self.ui_reply(question['id'])
        explanation=self.first_root_reply(clarification['id'], 'EXPLAIN')
        final=self.operator.send('root','UNSTAMPED_FINAL',reply_to=explanation['id'])
        with self.assertRaises(CoordError):self.root.record_decision_answer(question['id'],final['id'],'accepted')
        self.root.db.execute('INSERT INTO operator_message_origins VALUES(?,?,?)',(explanation['id'],'root',1.0));self.root.db.commit()
        final2=self.ui_reply(explanation['id'])
        with self.assertRaises(CoordError):self.root.record_decision_answer(question['id'],final2['id'],'accepted')

    def test_foreign_author_project_and_status_override_privacy(self):
        question,clarification,explanation,answer=self.dialogue()
        for actor in (self.other,self.agent):
            with self.assertRaisesRegex(CoordError,'unavailable to this author'):actor.decision_status(question['id'])
        with self.assertRaisesRegex(CoordError,'unavailable to this author'):self.root.decision_status('message_absent')
        with self.assertRaisesRegex(CoordError,'invalid decision status parameters'):
            self.remote(self.root,'root','decision-status',{'id':question['id'],'author':'root'})
        self.root.db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('project_id','foreign')");self.root.db.commit()
        with self.assertRaisesRegex(CoordError,'different project'):self.root.decision_status(question['id'])
        with self.assertRaises(CoordError):self.root.record_decision_answer(question['id'],answer['id'],'accepted')
        self.assertEqual(self.rows('operator_decision_events'),[])

    def test_wrong_participant_recipient_role_and_batch_ancestry_refuse(self):
        question,clarification,explanation,answer=self.dialogue()
        # Private corrupted-chain fixtures; no mutation of real messages.
        self.root.db.execute("UPDATE messages SET sender_session='other' WHERE id=?",(answer['id'],));self.root.db.commit()
        with self.assertRaises(CoordError):self.root.record_decision_answer(question['id'],answer['id'],'accepted')
        self.root.db.execute("UPDATE messages SET sender_session='operator',recipient_session='agent' WHERE id=?",(answer['id'],));self.root.db.commit()
        with self.assertRaises(CoordError):self.root.record_decision_answer(question['id'],answer['id'],'accepted')
        self.root.db.execute("UPDATE messages SET recipient_session='root' WHERE id=?",(answer['id'],))
        self.root.db.execute('INSERT INTO message_batches VALUES(?,?)',(explanation['id'],'fixture_batch'));self.root.db.commit()
        with self.assertRaises(CoordError):self.root.record_decision_answer(question['id'],answer['id'],'accepted')
        self.root.db.execute('DELETE FROM message_batches WHERE message_id=?',(explanation['id'],))
        self.root.db.execute("INSERT INTO operator_identities VALUES('root')");self.root.db.commit()
        with self.assertRaises(CoordError):self.root.record_decision_answer(question['id'],answer['id'],'accepted')

    def test_broken_cyclic_and_nonmonotonic_chain_refuse_atomically(self):
        question,clarification,explanation,answer=self.dialogue()
        before=self.rows('operator_decision_events')
        self.root.db.execute('UPDATE message_replies SET reply_to=? WHERE message_id=?',(answer['id'],explanation['id']));self.root.db.commit()
        with self.assertRaises(CoordError):self.root.record_decision_answer(question['id'],answer['id'],'accepted')
        self.root.db.execute('UPDATE message_replies SET reply_to=? WHERE message_id=?',(clarification['id'],explanation['id']))
        self.root.db.execute('DELETE FROM message_replies WHERE message_id=?',(clarification['id'],));self.root.db.commit()
        with self.assertRaises(CoordError):self.root.record_decision_answer(question['id'],answer['id'],'accepted')
        self.assertEqual(before,self.rows('operator_decision_events'))

    def test_depth_limit_and_projection_node_budget_never_clear_pending(self):
        question=self.root.send('operator','DEPTH',decision_needed=True);parent=question['id']
        for depth in range(33):
            parent=(self.ui_reply(parent) if depth%2==0 else self.first_root_reply(parent, 'EXPLAIN'))['id']
        with self.assertRaisesRegex(CoordError,'32 edges'):self.root.record_decision_answer(question['id'],parent,'accepted')
        state=self.root.decision_status(question['id']);self.assertFalse(state['terminal']);self.assertTrue(state['ancestry_limited'])
        question2=self.root.send('operator','BREADTH',decision_needed=True)
        for _ in range(129):self.ui_reply(question2['id'])
        state=self.root.decision_status(question2['id']);self.assertFalse(state['terminal']);self.assertTrue(state['ancestry_limited'])
        with self.assertRaises(CoordError):self.root.record_decision_answer(question2['id'],state['latest_reply_message_id'] or parent,'accepted')

    def test_clarification_progress_branch_and_out_of_order_terminal_refuse(self):
        question=self.root.send('operator','ORDER',decision_needed=True)
        clarification=self.ui_reply(question['id'])
        self.root.record_decision_answer(question['id'],clarification['id'],'clarification')
        explanation=self.first_root_reply(clarification['id'], 'EXPLAIN');answer=self.ui_reply(explanation['id'])
        newer_explanation=self.first_root_reply(answer['id'], 'FOLLOWUP');newer=self.ui_reply(newer_explanation['id'])
        with self.assertRaisesRegex(CoordError,'latest'):self.root.record_decision_answer(question['id'],answer['id'],'accepted')
        self.root.record_decision_answer(question['id'],newer['id'],'accepted')
        self.assertFalse(self.root.record_decision_answer(question['id'],clarification['id'],'clarification')['recorded'])
        with self.assertRaises(CoordError):self.root.record_decision_answer(question['id'],newer['id'],'declined')

    def legacy_clarification(self, question, reply):
        self.root.db.execute('INSERT INTO operator_decision_answers VALUES(?,?,?,?,?)',
                             (question['id'], reply['id'], 'root', 'clarification', 123.0))
        self.root.db.commit()

    def test_legacy_clarification_then_direct_final_preserves_history_and_terminal(self):
        for outcome in ('accepted', 'declined', 'conditional'):
            with self.subTest(outcome=outcome):
                question=self.root.send('operator','LEGACY_DIRECT',decision_needed=True)
                clarify=self.ui_reply(question['id'],'OLD_CLARIFICATION')
                self.legacy_clarification(question,clarify)
                final=self.ui_reply(question['id'],'NEW_DIRECT_FINAL')
                status=self.root.decision_status(question['id'])
                self.assertEqual(status['status'],'reply_received')
                self.assertEqual(status['reply_message_id'],final['id'])
                preserved={name:self.rows(name) for name in ('messages','message_replies','operator_message_origins','message_reads','operator_decision_answers')}
                self.assertTrue(self.root.record_decision_answer(question['id'],final['id'],outcome)['recorded'])
                self.assertFalse(self.root.record_decision_answer(question['id'],final['id'],outcome)['recorded'])
                self.assertFalse(self.root.record_decision_answer(question['id'],clarify['id'],'clarification')['recorded'])
                self.assertEqual(preserved,{name:self.rows(name) for name in preserved})
                status=self.root.decision_status(question['id'])
                self.assertEqual([item['outcome'] for item in status['answer_history']],['clarification',outcome])
                self.assertTrue(status['terminal'])
                self.ui_reply(question['id'],'LATER_COMMENT')
                self.assertTrue(self.root.decision_status(question['id'])['terminal'])
                with self.assertRaises(CoordError):self.root.record_decision_answer(question['id'],final['id'],'declined' if outcome!='declined' else 'accepted')

    def test_same_legacy_reply_explicit_terminal_classification_is_distinct_and_idempotent(self):
        for outcome in ('accepted','declined','conditional'):
            with self.subTest(outcome=outcome):
                question=self.root.send('operator','DIRECTION_ONLY',decision_needed=True)
                reply=self.ui_reply(question['id'],'VARIANTS_NOT_PRODUCTION_APPROVAL')
                self.legacy_clarification(question,reply)
                before={name:self.rows(name) for name in ('operator_decision_answers','messages','message_replies','operator_message_origins','message_reads')}
                self.assertFalse(self.root.decision_status(question['id'])['terminal'])
                self.assertTrue(self.root.record_decision_answer(question['id'],reply['id'],outcome)['recorded'])
                self.assertEqual(before,{name:self.rows(name) for name in before})
                status=self.root.decision_status(question['id'])
                self.assertEqual([e['reply_message_id'] for e in status['answer_history']],[reply['id'],reply['id']])
                self.assertEqual([e['legacy'] for e in status['answer_history']],[True,False])
                self.assertTrue(status['terminal'])
                self.assertFalse(self.root.record_decision_answer(question['id'],reply['id'],outcome)['recorded'])
                self.assertFalse(self.root.record_decision_answer(question['id'],reply['id'],'clarification')['recorded'])
                with self.assertRaises(CoordError):self.root.record_decision_answer(question['id'],reply['id'],'declined' if outcome!='declined' else 'accepted')

    def test_same_reply_classification_is_legacy_only_current_and_original_author(self):
        question=self.root.send('operator','LEGACY_CURRENT',decision_needed=True)
        old=self.ui_reply(question['id']);self.legacy_clarification(question,old)
        self.ui_reply(question['id'],'NEWER_DIRECT')
        with self.assertRaises(CoordError):self.root.record_decision_answer(question['id'],old['id'],'conditional')
        with self.assertRaises(CoordError):self.agent.record_decision_answer(question['id'],old['id'],'conditional')
        other=self.root.send('operator','NEW_EVENT_ONLY',decision_needed=True)
        reply=self.ui_reply(other['id']);self.root.record_decision_answer(other['id'],reply['id'],'clarification')
        with self.assertRaises(CoordError):self.root.record_decision_answer(other['id'],reply['id'],'conditional')
        self.assertFalse(self.root.decision_status(other['id'])['terminal'])

    def test_newer_direct_clarifications_progress_but_nested_unrelated_branch_refuses(self):
        question=self.root.send('operator','DIRECT_PROGRESS',decision_needed=True)
        first=self.ui_reply(question['id']);self.root.record_decision_answer(question['id'],first['id'],'clarification')
        old_branch=self.first_root_reply(first['id'], 'OLD_BRANCH')
        second=self.operator.send('root','DIRECT_WEAKER',reply_to=question['id'])
        self.root.record_decision_answer(question['id'],second['id'],'clarification')
        branch_final=self.ui_reply(old_branch['id'],'UNRELATED_NESTED')
        with self.assertRaises(CoordError):self.root.record_decision_answer(question['id'],branch_final['id'],'accepted')
        final=self.ui_reply(question['id'],'LATEST_DIRECT_FINAL')
        self.root.record_decision_answer(question['id'],final['id'],'accepted')
        self.assertEqual(len(self.root.decision_status(question['id'])['answer_history']),3)

    def test_direct_older_reply_and_stale_classification_cannot_bypass_latest(self):
        question=self.root.send('operator','STALE_DIRECT',decision_needed=True)
        first=self.ui_reply(question['id']);self.legacy_clarification(question,first)
        old=self.ui_reply(question['id'],'OLDER');latest=self.ui_reply(question['id'],'LATEST')
        with self.assertRaises(CoordError):self.root.record_decision_answer(question['id'],old['id'],'accepted')
        self.root.record_decision_answer(question['id'],latest['id'],'declined')

    def test_legacy_same_reply_classification_atomic_failure_leaves_no_terminal(self):
        question=self.root.send('operator','ATOMIC_CLASSIFICATION',decision_needed=True)
        reply=self.ui_reply(question['id']);self.legacy_clarification(question,reply)
        self.root.db.execute("CREATE TRIGGER fail_classification BEFORE INSERT ON operator_decision_events BEGIN SELECT RAISE(ABORT,'fixture failure'); END")
        self.root.db.commit()
        with self.assertRaises(sqlite3.IntegrityError):self.root.record_decision_answer(question['id'],reply['id'],'conditional')
        self.assertEqual(len(self.root.decision_status(question['id'])['answer_history']),1)
        self.assertFalse(self.root.decision_status(question['id'])['terminal'])

    def test_legacy_same_reply_competing_terminal_classifications_have_one_winner(self):
        from concurrent.futures import ThreadPoolExecutor
        question=self.root.send('operator','CLASSIFICATION_RACE',decision_needed=True)
        reply=self.ui_reply(question['id']);self.legacy_clarification(question,reply)
        def attempt(outcome):
            with contextlib.closing(Coordinator(self.db_path,'root')) as actor:
                try:return actor.record_decision_answer(question['id'],reply['id'],outcome)['outcome']
                except CoordError:return 'refused'
        with ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(attempt,('conditional','declined')))
        self.assertEqual(results.count('refused'),1)
        self.assertEqual(len(self.rows('operator_decision_events')),1)
        self.assertEqual(len(self.root.decision_status(question['id'])['answer_history']),2)

    def test_repeated_direct_clarifications_refuse_before_exceeding_history_budget(self):
        question=self.root.send('operator','BOUNDED_DIRECT_PROGRESS',decision_needed=True)
        for index in range(32):
            reply=self.ui_reply(question['id'],'CLARIFY_'+str(index))
            self.root.record_decision_answer(question['id'],reply['id'],'clarification')
        before=self.rows('operator_decision_events')
        final=self.ui_reply(question['id'],'OVER_BUDGET')
        with self.assertRaisesRegex(CoordError,'bounded'):self.root.record_decision_answer(question['id'],final['id'],'conditional')
        self.assertEqual(before,self.rows('operator_decision_events'))
        self.assertEqual(len(self.root.decision_status(question['id'])['answer_history']),32)
        self.assertFalse(self.root.decision_status(question['id'])['terminal'])

    def test_separately_marked_inner_question_does_not_answer_outer(self):
        outer,clarification,inner,answer=self.dialogue()
        self.root.mark_decision_needed(inner['id'])
        with self.assertRaises(CoordError):self.root.record_decision_answer(outer['id'],answer['id'],'accepted')
        self.root.record_decision_answer(inner['id'],answer['id'],'accepted')
        self.assertFalse(self.root.decision_status(outer['id'])['terminal'])

    def test_concurrent_terminal_one_winner_and_failure_rollback(self):
        from concurrent.futures import ThreadPoolExecutor
        question=self.root.send('operator','CONCURRENT',decision_needed=True);reply=self.ui_reply(question['id'])
        def attempt(outcome):
            with contextlib.closing(Coordinator(self.db_path,'root')) as actor:
                try:return actor.record_decision_answer(question['id'],reply['id'],outcome)['outcome']
                except CoordError:return 'refused'
        with ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(attempt,('accepted','declined')))
        self.assertEqual(results.count('refused'),1);self.assertEqual(len(self.rows('operator_decision_events')),1)
        question2=self.root.send('operator','ROLLBACK',decision_needed=True);reply2=self.ui_reply(question2['id'])
        self.root.db.execute("CREATE TRIGGER fixture_failure BEFORE INSERT ON operator_decision_events BEGIN SELECT RAISE(ABORT,'fixture atomic failure'); END");self.root.db.commit()
        before=self.rows('operator_decision_events')
        with self.assertRaises(sqlite3.IntegrityError):self.root.record_decision_answer(question2['id'],reply2['id'],'accepted')
        self.assertEqual(before,self.rows('operator_decision_events'));self.assertFalse(self.root.decision_status(question2['id'])['terminal'])


    def test_forged_operator_stamp_and_corrupt_event_history_never_authorize(self):
        question=self.root.send('operator','FORGED_STAMP',decision_needed=True)
        reply=self.operator.send('root','UNSTAMPED',reply_to=question['id'])
        self.root.db.execute('INSERT INTO operator_message_origins VALUES(?,?,?)',(reply['id'],'other',1.0));self.root.db.commit()
        with self.assertRaises(CoordError):self.root.record_decision_answer(question['id'],reply['id'],'accepted')
        self.assertEqual(self.rows('operator_decision_events'),[])
        question2=self.root.send('operator','CORRUPT_PRIVATE_HISTORY',decision_needed=True)
        final=self.ui_reply(question2['id'])
        # Deliberately corrupted private stored event; never a production repair.
        self.root.db.execute('INSERT INTO operator_decision_events(message_id,reply_message_id,recorded_by,outcome,recorded_at) VALUES(?,?,?,?,?)',(question2['id'],final['id'],'other','accepted',1.0));self.root.db.commit()
        with self.assertRaises(CoordError):self.root.decision_status(question2['id'])
        self.assertEqual(self.message_projection(question2['id'])['decision']['status'],'pending')


    def test_later_independent_mark_does_not_reopen_or_invalidate_recorded_event(self):
        question,clarification,explanation,answer=self.dialogue()
        self.root.record_decision_answer(question['id'],answer['id'],'accepted')
        self.root.mark_decision_needed(explanation['id'])
        self.assertTrue(self.root.decision_status(question['id'])['terminal'])
        self.assertFalse(self.root.record_decision_answer(question['id'],answer['id'],'accepted')['recorded'])
        self.assertFalse(self.root.decision_status(explanation['id'])['terminal'])
        with self.assertRaisesRegex(CoordError,'another decision'):
            self.root.record_decision_answer(explanation['id'],answer['id'],'accepted')
        self.assertTrue(self.root.decision_status(question['id'])['terminal'])



if __name__ == '__main__':
    unittest.main()
