"""Private Native02 successor regression; real Coordinator/SQLite, native facts fixture only."""
import contextlib
import io
import subprocess
import time
from unittest.mock import patch
from test_build01_admission import Build01Tests
from synthetic_successor_allowance import allowance
from agent_chat import successor_admission as successor, root_local
from agent_chat.core import CoordError

class Native02Tests(Build01Tests):
    def native02(self, **changes):
        return self.issue(successor.NATIVE02_REQUEST, authorization_source_id=changes.pop('authorization_source_id',successor.BUILD_SOURCE), **changes)

    def test_native02_after_actual_spent_build_and_immutable_history(self):
        self.free_heavy(); self.build(); self.state='active'
        earlier=self.request(); self.release(earlier)
        self.assertIsNotNone(self.db.execute('SELECT 1 FROM successor_admission_uses WHERE request_id=?',(successor.BUILD_REQUEST,)).fetchone())
        self.state='idle'; before=self.evidence()
        result=self.native02()
        self.assertEqual((result['request_id'],result['unit']),(successor.NATIVE02_REQUEST,successor.NATIVE02_UNIT))
        for table,rows in before.items():
            self.assertEqual([tuple(r) for r in self.db.execute('SELECT * FROM '+table)][:len(rows)],rows)
        self.state='active'; claim=self.request()
        used=self.db.execute('SELECT consumer,reservation_id FROM successor_admission_uses WHERE request_id=?',(successor.NATIVE02_REQUEST,)).fetchone()
        self.assertEqual(tuple(used),(successor.CONSUMER,claim['reservation_id']))
        self.release(claim); self.state='idle'
        with self.assertRaises(successor.DiagnosticRefused) as recorded: self.native02()
        self.assertEqual(recorded.exception.predicate, "attempt_already_recorded")
        with self.assertRaises(CoordError): self.request()

    def test_pending_any_successor_rejected_without_new_ticket(self):
        self.free_heavy(); self.build(); before=self.evidence()
        with self.assertRaises(successor.DiagnosticRefused) as caught: self.native02()
        self.assertEqual(caught.exception.predicate, 'successor_packet_pending')
        self.assertEqual(self.evidence(),before)
        self.assertIsNone(self.db.execute('SELECT 1 FROM successor_admissions WHERE request_id=?',(successor.NATIVE02_REQUEST,)).fetchone())

    def test_native02_wrong_packet_source_and_native_state_fail_closed(self):
        self.free_heavy(); before=self.evidence()
        for overrides in ({'authorization_source_id':'message_wrong'},{'home_session':'wrong'},{'host_id':'wrong'},{'confirm':False}):
            with self.subTest(overrides=overrides), self.assertRaises(successor.Refused): self.native02(**overrides)
        with self.assertRaises(successor.Refused): self.issue('wrong-packet',authorization_source_id=successor.BUILD_SOURCE)
        self.assertEqual(self.evidence(), before)

    def test_synthetic_native_active_refused(self):
        self.free_heavy()
        before = self.evidence()
        self.state, self.direct, self.queue = "active", True, 0
        with self.assertRaises(successor.DiagnosticRefused) as caught: self.native02()
        self.assertEqual(caught.exception.predicate, "native_tuple_differs")
        self.assertEqual(self.evidence(), before)

    def test_synthetic_native_direct_false_refused(self):
        self.free_heavy()
        before = self.evidence()
        self.state, self.direct, self.queue = "idle", False, 0
        with self.assertRaises(successor.DiagnosticRefused) as caught: self.native02()
        self.assertEqual(caught.exception.predicate, "native_direct_input_false")
        self.assertEqual(self.evidence(), before)

    def test_synthetic_native_queued_refused(self):
        self.free_heavy()
        before = self.evidence()
        self.state, self.direct, self.queue = "idle", True, 1
        with self.assertRaises(successor.DiagnosticRefused) as caught: self.native02()
        self.assertEqual(caught.exception.predicate, "native_tuple_differs")
        self.assertEqual(self.evidence(), before)


    def test_native02_heavy_capacity_blocks_and_no_write(self):
        before=self.evidence()
        with self.assertRaises(successor.DiagnosticRefused) as caught: self.native02()
        self.assertEqual(caught.exception.predicate, 'heavy_capacity_unavailable')
        self.assertEqual(self.evidence(),before)

    def test_native02_heavy_queue_blocks_and_no_write(self):
        self.free_heavy()
        self.db.execute("INSERT INTO resource_queue(resource,session,queued_at) VALUES('heavy-unity:2','new-worker',?)",(time.time(),))
        self.db.commit(); before=self.evidence()
        with self.assertRaises(successor.DiagnosticRefused) as caught: self.native02()
        self.assertEqual(caught.exception.predicate, 'heavy_capacity_unavailable')
        self.assertEqual(self.evidence(),before)

    def test_native02_consumer_wrong_owner_and_capacity_race_rollback(self):
        self.free_heavy(); self.native02(); before=self.evidence()
        with self.assertRaises(CoordError): self.request(session='new-worker')
        original=self.coord._acquire
        def acquire(*args,**kwargs):
            result=original(*args,**kwargs)
            self.db.execute("UPDATE resources SET owner_session='new-worker',reservation_id='race',stale=1 WHERE name='heavy-unity:2'")
            return result
        with patch.object(self.coord,'_acquire',side_effect=acquire):
            with self.assertRaises(successor.Refused): self.request()
        self.assertEqual(self.evidence(),before)
        self.assertIsNone(self.db.execute("SELECT owner_session FROM resources WHERE name='validation-clone'").fetchone()[0])

    def test_native02_producer_capacity_race_no_ticket(self):
        self.free_heavy(); before=self.evidence()
        def hold():
            self.db.execute("UPDATE resources SET owner_session='new-worker',reservation_id='race',stale=1 WHERE name='heavy-unity:2'")
            self.db.commit()
        self.after_native=hold
        with self.assertRaises(successor.DiagnosticRefused) as caught: self.native02()
        self.assertEqual(caught.exception.predicate, 'heavy_capacity_unavailable')
        after=self.evidence()
        # The deliberately concurrent hold is real fixture state, not a
        # mutation by the rejected producer. It must remain visible.
        self.assertEqual(tuple(self.db.execute("SELECT owner_session,reservation_id,stale FROM resources WHERE name='heavy-unity:2'").fetchone()),('new-worker','race',1))
        self.assertEqual([row for row in after['resources'] if row[0]!='heavy-unity:2'],
                         [row for row in before['resources'] if row[0]!='heavy-unity:2'])
        for table in before.keys()-{'resources'}:self.assertEqual(after[table],before[table])

    def test_native02_parser_invokes_exact_root_operation(self):
        self.free_heavy(); out=io.StringIO()
        with allowance(self.root,self.master,self.registry), contextlib.redirect_stdout(out):
            result=root_local.main(['--home-project','default','--home-session',successor.ROOT,'--host-id',successor.HOST,'capacity-successor-native02','--confirm'])
        self.assertEqual(result,0)
        parsed=subprocess.run(['jq','-er','.request_id + "|" + .unit'],input=out.getvalue(),text=True,capture_output=True,check=True)
        self.assertEqual(parsed.stdout.strip(),successor.NATIVE02_REQUEST+'|'+successor.NATIVE02_UNIT)
