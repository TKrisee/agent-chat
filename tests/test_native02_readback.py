"""Private real SQLite/WAL + Root-local readback; no configured store/native calls."""
import io
import contextlib
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch
from test_native02_admission import Native02Tests
from agent_chat import cli, root_local, successor_admission as successor, capacity_observer as observer
from agent_chat import native02_readback as readback
from agent_chat.core import CoordError

class ReadbackTests(Native02Tests):
    def observe(self):
        return root_local.execute('capacity-successor-native02-readback',{},master_db=self.master,service_plist=self.config.plist,home_project='default',session=successor.ROOT,host_id=successor.HOST)
    def wrapped(self):
        output=io.StringIO(); errors=io.StringIO()
        argv=['agent-chat-client','root-local','--home-project','default','--home-session',successor.ROOT,'--host-id',successor.HOST,'capacity-successor-native02-readback']
        with patch.object(sys,'argv',argv), patch.object(successor,'issue',side_effect=AssertionError('issuer called')), \
                patch.object(observer,'_native_observations',side_effect=AssertionError('native called')), \
                patch.object(observer,'_consumer_native_observations',side_effect=AssertionError('native called')), \
                contextlib.redirect_stdout(output), contextlib.redirect_stderr(errors):
            code=cli.entrypoint()
        return code,output.getvalue(),errors.getvalue()
    def test_no_rows_root_parser_and_no_native_call(self):
        before=self.evidence()
        with patch.object(observer,'_native_observations',side_effect=AssertionError('native called')):
            out=io.StringIO()
            with contextlib.redirect_stdout(out):
                code=root_local.main(['--home-project','default','--home-session',successor.ROOT,'--host-id',successor.HOST,'capacity-successor-native02-readback'])
            self.assertEqual(code,0)
            parsed=subprocess.run(['jq','-er','.state + "|" + .issuer_completion + "|" + (.sql_mutation_performed|tostring)'],input=out.getvalue(),text=True,capture_output=True,check=True)
            self.assertEqual(parsed.stdout.strip(),'no_rows_at_snapshot|not asserted|false')
        self.assertEqual(before,self.evidence())
    def test_pending_then_spent_and_no_token_disclosure(self):
        self.free_heavy(); self.native02(); before=self.evidence()
        first=self.observe()
        self.assertEqual(first['state'],'recorded_no_use_at_snapshot')
        self.assertEqual(before,self.evidence())
        self.assertNotIn('token',str(first))
        self.state='active'; claim=self.request(); self.release(claim)
        second=self.observe()
        self.assertEqual(second['state'],'recorded_use_at_snapshot')
        self.assertEqual(second['use'][0]['reservation_id'],claim['reservation_id'])
        self.assertNotIn('token',str(second))
    def test_bad_root_and_missing_schema_fail_closed(self):
        with self.assertRaises(CoordError):
            root_local.execute('capacity-successor-native02-readback',{},master_db=self.master,service_plist=self.config.plist,home_project='default',session='wrong',host_id=successor.HOST)
        with tempfile.TemporaryDirectory() as folder:
            target=Path(folder).resolve()/'empty.sqlite3'
            with contextlib.closing(sqlite3.connect(target)) as connection:
                connection.execute('PRAGMA journal_mode=WAL')
                connection.execute('CREATE TABLE fixture(x)')
                connection.commit()
                with patch.object(observer,'_root_scope',return_value={'root_thread':successor.ROOT_THREAD}):
                    with self.assertRaisesRegex(readback.ReadbackUnavailable,'schema_unavailable'):
                        readback.inspect(target,registry=self.registry,home_project='default',home_session=successor.ROOT,host_id=successor.HOST)
    def test_wal_snapshot_does_not_reclassify_later_commit(self):
        self.free_heavy(); original=readback._bounded; triggered=[]
        def around(db,query,args):
            rows=original(db,query,args)
            if 'FROM successor_admissions WHERE' in query and not triggered:
                triggered.append(True); self.native02()
            return rows
        with patch.object(readback,'_bounded',side_effect=around):
            earlier=self.observe()
        self.assertEqual(earlier['state'],'no_rows_at_snapshot')
        self.assertEqual(self.observe()['state'],'recorded_no_use_at_snapshot')
    def test_readonly_open_failure_stays_unavailable(self):
        with patch.object(observer,'_open_ro',side_effect=sqlite3.OperationalError('unable to open database file')):
            with self.assertRaisesRegex(readback.ReadbackUnavailable,'sqlite_read_error'):
                readback.inspect(self.master,registry=self.registry,home_project='default',home_session=successor.ROOT,host_id=successor.HOST)
    def test_mismatched_ticket_cannot_be_reported_pending(self):
        self.free_heavy(); self.build()
        ticket=list(self.db.execute('SELECT * FROM successor_admissions WHERE request_id=?',(successor.BUILD_REQUEST,)).fetchone())
        observation=list(self.db.execute('SELECT * FROM successor_admission_observations WHERE request_id=?',(successor.BUILD_REQUEST,)).fetchone())
        ticket[0]=successor.NATIVE02_REQUEST; ticket[1]='wrong-resource'
        observation[0]=successor.NATIVE02_REQUEST
        self.db.execute('INSERT INTO successor_admissions VALUES('+','.join('?' for _ in ticket)+')',ticket)
        self.db.execute('INSERT INTO successor_admission_observations VALUES('+','.join('?' for _ in observation)+')',observation)
        self.db.commit()
        self.assertEqual(self.observe()['state'],'inconsistent')
        code,out,err=self.wrapped()
        parsed=subprocess.run(['jq','-er','.state'],input=out,text=True,capture_output=True,check=True)
        self.assertEqual((code,err,parsed.stdout.strip()),(0,'','inconsistent'))

    def test_configured_drift_and_unreadable_family_are_unknown(self):
        before=self.evidence()
        with patch.object(root_local,'configured_server_database',return_value=self.root/'different.sqlite3'):
            result=self.observe()
            self.assertEqual(result['state'],'UNKNOWN')
            self.assertEqual('configured_preflight_unverified',result['reason'])
        with patch.object(readback,'_family',side_effect=readback.ReadbackUnavailable('family_unavailable',number=13)):
            result=self.observe()
            self.assertEqual(result['state'],'UNKNOWN')
            self.assertEqual(('family_unavailable',13),(result['reason'],result['error_number']))
        self.assertEqual(before,self.evidence())

    def test_missing_sidecar_never_opens_or_falls_back(self):
        with tempfile.TemporaryDirectory() as folder:
            target=Path(folder).resolve()/'master'
            target.touch()
            with self.assertRaisesRegex(readback.ReadbackUnavailable,'family_unavailable'):
                readback._family(target)

    def test_ambiguous_exit_may_have_committed(self):
        self.free_heavy()
        # The synthetic producer really commits; model a later generic failure.
        self.native02()
        synthetic_result='fixed successor admission refused'
        before=self.evidence()
        result=self.observe()
        self.assertEqual(synthetic_result,'fixed successor admission refused')
        self.assertEqual(result['state'],'recorded_no_use_at_snapshot')
        self.assertEqual(result['issuer_completion'],'not asserted')
        self.assertEqual(before,self.evidence())

    def test_open_failure_is_precise_unknown_at_root_entry(self):
        with patch.object(observer,'_open_ro',side_effect=sqlite3.OperationalError('unable to open database file')):
            result=self.observe()
        self.assertEqual(result['state'],'UNKNOWN')
        self.assertEqual('sqlite_read_error',result['reason'])

    def test_old_spent_history_and_store_are_not_changed(self):
        self.free_heavy(); self.build(); self.state='active'
        old_claim=self.request(); self.release(old_claim)
        self.state='idle'; self.native02()
        before=self.evidence()
        data_version=self.db.execute('PRAGMA data_version').fetchone()[0]
        schema_version=self.db.execute('PRAGMA schema_version').fetchone()[0]
        observed=self.observe()
        self.assertEqual(observed['state'],'recorded_no_use_at_snapshot')
        self.assertEqual(before,self.evidence())
        self.assertEqual(data_version,self.db.execute('PRAGMA data_version').fetchone()[0])
        self.assertEqual(schema_version,self.db.execute('PRAGMA schema_version').fetchone()[0])

    def test_unavailable_provider_at_cli_start_is_unknown(self):
        output=io.StringIO()
        with patch.object(root_local,'_installed_server_config',side_effect=root_local.ServiceError('provider unavailable')):
            with contextlib.redirect_stdout(output):
                code=root_local.main(['--home-project','default','--home-session',successor.ROOT,'--host-id',successor.HOST,'capacity-successor-native02-readback'])
        self.assertEqual(code,0)
        parsed=subprocess.run(['jq','-er','.state + "|" + .reason'],input=output.getvalue(),text=True,capture_output=True,check=True)
        self.assertEqual(parsed.stdout.strip(),'UNKNOWN|provider_unavailable')

    def test_malformed_text_never_leaks_through_real_cli(self):
        sentinel='SYNTHETIC_PRIVATE_SENTINEL_42'
        self.db.execute('UPDATE resources SET owner_session=CAST(? AS TEXT) WHERE name=?',
                        (sqlite3.Binary(sentinel.encode()+b'\xff'),successor.RESOURCE))
        self.db.commit()
        version=self.db.execute('PRAGMA data_version').fetchone()[0]
        schema=self.db.execute('PRAGMA schema_version').fetchone()[0]
        with patch.object(observer,'_native_observations',side_effect=AssertionError('native called')):
            code,out,err=self.wrapped()
        self.assertEqual(code,0)
        self.assertNotIn(sentinel,out+err)
        parsed=subprocess.run(['jq','-er','.state + "|" + .reason'],input=out,text=True,capture_output=True,check=True)
        self.assertEqual(parsed.stdout.strip(),'UNKNOWN|sqlite_read_error')
        self.assertEqual(version,self.db.execute('PRAGMA data_version').fetchone()[0])
        self.assertEqual(schema,self.db.execute('PRAGMA schema_version').fetchone()[0])

    def test_error_boundaries_never_echo_private_payload(self):
        secret='SYNTHETIC_PROVIDER_PATH_SENTINEL_84'
        cases=[(root_local,'_installed_server_config',root_local.ServiceError(secret),'provider_unavailable'),
               (root_local,'_preflight',sqlite3.OperationalError(secret),'sqlite_read_error'),
               (root_local,'_preflight',CoordError(secret),'configured_preflight_unverified'),
               (readback.os,'open',OSError(13,secret,'/synthetic/'+secret),'family_unavailable')]
        before=self.evidence()
        for owner,name,error,reason in cases:
            with self.subTest(reason=reason), patch.object(owner,name,side_effect=error):
                code,out,err=self.wrapped()
            self.assertEqual(code,0)
            self.assertNotIn(secret,out+err)
            parsed=subprocess.run(['jq','-er','.state + "|" + .reason'],input=out,text=True,capture_output=True,check=True)
            self.assertEqual(parsed.stdout.strip(),'UNKNOWN|'+reason)
        self.assertEqual(before,self.evidence())

    def test_wrapper_cli_absent_pending_and_spent(self):
        def state():
            before=self.evidence()
            version=self.db.execute('PRAGMA data_version').fetchone()[0]
            code,out,err=self.wrapped()
            self.assertEqual((code,err),(0,''))
            parsed=subprocess.run(['jq','-er','.state'],input=out,text=True,capture_output=True,check=True)
            self.assertEqual(before,self.evidence())
            self.assertEqual(version,self.db.execute('PRAGMA data_version').fetchone()[0])
            return parsed.stdout.strip()
        self.assertEqual(state(),'no_rows_at_snapshot')
        self.free_heavy(); self.native02()
        self.assertEqual(state(),'recorded_no_use_at_snapshot')
        self.state='active'; claim=self.request(); self.release(claim)
        self.assertEqual(state(),'recorded_use_at_snapshot')
