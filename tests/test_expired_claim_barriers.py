from __future__ import annotations

import contextlib
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import plistlib
import sqlite3
import subprocess
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from agent_chat.core import Coordinator, CoordError
from agent_chat import expired_claim_disposition as expired_claim_module
from agent_chat.ownership_disposition import (
    ConfiguredRoot,
    _project_resumed_stop_context,
    _read_existing,
    _recheck_root_barriers,
    load_configured_root,
    load_wbt_preview_configured_root,
    target_path,
)
from agent_chat.service import InstalledServerConfig
from agent_chat.expired_claim_disposition import (
    _WBT_COHORT_REF, _WBT_PROJECT_ID, _WBT_COUNTS_SOURCE_PATH,
    _WBT_COUNTS_SOURCE_SHA256, _WBT_LEDGER_SOURCE_PATH, _WBT_LEDGER_SOURCE_SHA256,
    execute as execute_expired_claim, selection as expired_claim_selection,
)
from agent_chat.guard_generation import guard_generation
from agent_chat.remote_service import _schema as install_remote_schema
from agent_chat.root_restart import schema as install_root_restart_schema


class ExpiredClaimBarrierEligibilityTests(unittest.TestCase):
    STOP = '09ddf267-e992-4fd7-a5ab-a5ee39bdc7e3'
    FIXED_EXPIRED_CLAIM_RESOURCES_V3 = (
        'heavy-unity:2',
        'workspace:gameplay',
        'operation:gameplay-fivecase-YLN-20261014-a',
        'port:udp:51382',
    )

    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='agent-chat-expired-claim-stop-')
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name).resolve()
        self.master = root / 'state.sqlite3'
        self.registry = Path(str(self.master) + '.projects.sqlite3')
        # Unselected historical manifests are absent in this private test world.
        # Scoped synthetic contexts below replace these paths where membership is
        # under test; production constants remain unchanged after each test.
        names=('_WBT_FOLLOW_ON_MANIFEST_PATH','_WBT_GROUP_MANIFEST_PATH',
               '_WBT_NEXT_GROUP_MANIFEST_PATH','_WBT_FILE_CLASS_MANIFEST_PATH',
               '_WBT_NEXT_FILE_CLASS_MANIFEST_PATH','_WBT_GG1_FILE_CLASS_MANIFEST_PATH',
               '_WBT_HGIDE_FILE_CLASS_MANIFEST_PATH','_WBT_6JC1_FILE_CLASS_MANIFEST_PATH',
               '_WBT_HQG_FILE_CLASS_MANIFEST_PATH','_WBT_NWC_RESUMED_V1_MANIFEST_PATH',
               '_WBT_NWC_RESUMED_MANIFEST_PATH','_WBT_XU0J_MANIFEST_PATH','_WBT_N6LB_MANIFEST_PATH')
        self.production_manifest_paths={name:getattr(expired_claim_module,name) for name in names}
        missing_manifests = patch.multiple('agent_chat.expired_claim_disposition',
            **{name:str(root/('absent-'+name+'.json')) for name in names})
        missing_manifests.start()
        self.addCleanup(missing_manifests.stop)
        self.plist = root / 'server.plist'
        self.wrapper = root / 'wrapper'
        self.executable = root / 'server-bin'
        self.executable.write_text('', encoding='utf-8')
        self.config = InstalledServerConfig(self.plist, self.wrapper, self.executable, self.master,
                                            root, os.getuid(), '127.0.0.1', 8765)
        self.patch = patch('agent_chat.service._installed_server_config', return_value=self.config)
        self.patch.start()
        self.addCleanup(self.patch.stop)
        ownership_provider = patch('agent_chat.ownership_disposition._installed_server_config', return_value=self.config)
        ownership_provider.start()
        self.addCleanup(ownership_provider.stop)
        self.wrapper.write_text('#!/bin/sh\nexec ' + str(self.executable) + ' --db ' + str(self.master)
                                + ' --host 127.0.0.1 --port 8765\n', encoding='utf-8')
        with self.plist.open('wb') as stream:
            plistlib.dump({'Label':'com.agent-chat.server',
                           'AssociatedBundleIdentifiers':['com.agent-chat.server'],
                           'WorkingDirectory':str(root), 'RunAtLoad':True, 'KeepAlive':True,
                           'ThrottleInterval':10, 'EnvironmentVariables':{},
                           'ProgramArguments':[str(self.wrapper)]}, stream)
        self.coord = Coordinator(self.master)
        self.coord.db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('project_id','default')")
        self.coord.db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('project_registry',?)", (str(self.master),))
        self.coord.db.executescript('''CREATE TABLE bridge_bindings (session_id TEXT PRIMARY KEY,
            thread_id TEXT UNIQUE,parent_session TEXT,agent_path TEXT,updated_at REAL);
            CREATE TABLE remote_session_hosts (session_id TEXT PRIMARY KEY,host_id TEXT,
            registered_at REAL,last_seen_at REAL);
            CREATE TABLE bridge_jobs (id TEXT PRIMARY KEY,thread_id TEXT,payload TEXT,status TEXT,
            queue_id TEXT,error TEXT,created_at REAL,updated_at REAL);
            CREATE TABLE IF NOT EXISTS root_team_stops (
            id TEXT PRIMARY KEY, root_session TEXT NOT NULL, active INTEGER NOT NULL,
            created_at REAL NOT NULL, resumed_at REAL);
            CREATE TABLE IF NOT EXISTS root_team_stop_targets (
            id TEXT PRIMARY KEY, stop_id TEXT NOT NULL, requester_session TEXT NOT NULL,
            target_session TEXT NOT NULL, host_id TEXT, thread_id TEXT, status TEXT NOT NULL,
            turn_id TEXT, error TEXT, created_at REAL NOT NULL, updated_at REAL NOT NULL,
            UNIQUE(stop_id,target_session));''')
        now = time.time()
        self.coord.db.execute('INSERT INTO root_team_stops VALUES(?,?,?,?,?)',
                              (self.STOP, 'root', 0, now - 10, now - 5))
        with contextlib.closing(sqlite3.connect(self.registry)) as db:
            db.executescript('''CREATE TABLE projects (id TEXT PRIMARY KEY);
                CREATE TABLE root_config (singleton INTEGER PRIMARY KEY,home_project TEXT,
                    home_session TEXT,host_id TEXT,mode TEXT);
                CREATE TABLE root_fresh_requests (id TEXT PRIMARY KEY,status TEXT);
                CREATE TABLE root_team_operations (id TEXT PRIMARY KEY,root_session TEXT NOT NULL,
                    created_at REAL NOT NULL,resumed_at REAL);
                CREATE TABLE root_team_operation_projects (request_id TEXT NOT NULL,
                    project_id TEXT NOT NULL,root_session TEXT NOT NULL,error TEXT,
                    PRIMARY KEY(request_id,project_id));
                CREATE TABLE root_projects (project_id TEXT PRIMARY KEY,session_id TEXT NOT NULL,
                    joined INTEGER NOT NULL);''')
            db.execute("INSERT INTO projects VALUES('default')")
            db.execute("INSERT INTO root_config VALUES(1,'default','root','host-root','running')")
            db.execute('INSERT INTO root_team_operations VALUES(?,?,?,NULL)',
                       (self.STOP, 'root', now - 10))
            db.execute('INSERT INTO root_team_operation_projects VALUES(?,?,?,NULL)',
                       (self.STOP, 'default', 'root'))
            db.execute("INSERT INTO root_projects VALUES('default','root',1)")
            db.commit()
        self.root = ConfiguredRoot(self.master, self.registry, 'default', 'root', 'host-root',
                                   'ws://127.0.0.1:4500')

    def tearDown(self):
        self.coord.close()

    def context(self, *, project='default'):
        path = target_path(self.root, project)
        with _read_existing(self.registry) as db:
            return _project_resumed_stop_context(self.root, db, project, path)

    def test_selection_uses_separate_exact_v2_and_v3_literal_families(self):
        legacy = expired_claim_selection(self.root, 'default', 'validation-clone',
                                         'owner', 'reservation', 1234.5)
        self.assertEqual((legacy[0], legacy[5]), ('expired-claim-v2', 'validation-clone'))
        for resource in self.FIXED_EXPIRED_CLAIM_RESOURCES_V3:
            with self.subTest(resource=resource):
                selected = expired_claim_selection(self.root, 'default', resource,
                                                   'owner', 'reservation', 1234.5)
                self.assertEqual((selected[0], selected[5]), ('expired-claim-v3', resource))
        for resource in (
                'heavy-unity:1', 'heavy-unity:*', 'heavy-unity:2:extra',
                'workspace:*', 'workspace:gameplay-extra',
                'operation:gameplay-fivecase-YLN-20261014-*',
                'port:udp:*', 'port:udp:51382-extra', 'addon', 'validation-clone:extra'):
            with self.subTest(rejected=resource), self.assertRaises(CoordError):
                expired_claim_selection(self.root, 'default', resource,
                                        'owner', 'reservation', 1234.5)

    def expired_claim_arguments(self):
        self.coord.session = 'owner'
        self.coord.register('worker')
        claim = self.coord.request('validation-clone', 5)
        deadline = time.time() - 60
        self.coord.db.execute('UPDATE resources SET deadline=?,token=NULL WHERE name=?',
                              (deadline, claim['resource']))
        self.coord.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?,?,?)',
                              ('owner', 'thread-owner', None, None, time.time()))
        self.coord.db.execute('INSERT INTO remote_session_hosts VALUES(?,?,?,?)',
                              ('owner', 'host-root', time.time(), time.time()))
        arguments = dict(master_db=self.master, service_plist=self.plist, project='default',
            resource='validation-clone', owner_session='owner', reservation_id=claim['reservation_id'],
            expected_deadline=deadline, operator_message_id=['source-1'], action='preview')
        return arguments, claim, deadline

    def wbt_target_arguments(self, resource='wbt:exact-row'):
        project = _WBT_PROJECT_ID
        target = self.master.parent / (self.master.name + '.projects') / project / 'state.sqlite3'
        target.parent.mkdir(parents=True, exist_ok=True)
        target_coord = Coordinator(target)
        self.addCleanup(target_coord.close)
        target_coord.db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('project_id',?)", (project,))
        target_coord.db.execute("INSERT OR REPLACE INTO meta(key,value) VALUES('project_registry',?)",
                                (str(self.master),))
        install_remote_schema(target_coord)
        target_coord.db.execute('''CREATE TABLE IF NOT EXISTS bridge_bindings (
            session_id TEXT PRIMARY KEY,thread_id TEXT UNIQUE,parent_session TEXT,
            agent_path TEXT,updated_at REAL)''')
        target_coord.db.execute('''CREATE TABLE IF NOT EXISTS bridge_jobs (
            id TEXT PRIMARY KEY,thread_id TEXT,payload TEXT,status TEXT,
            queue_id TEXT,error TEXT,created_at REAL,updated_at REAL)''')
        target_coord.session = 'wbt-owner'
        target_coord.register('reviewer_group')
        claim = target_coord.request(resource, 3600)
        now = time.time()
        target_coord.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?,?,?)',
                                ('wbt-owner', 'thread-wbt-owner', None, None, now))
        target_coord.db.execute('INSERT INTO remote_session_hosts VALUES(?,?,?,?)',
                                ('wbt-owner', 'host-root', now, now))
        target_coord.db.execute('''CREATE TABLE IF NOT EXISTS root_team_stops (
            id TEXT PRIMARY KEY, root_session TEXT NOT NULL, active INTEGER NOT NULL,
            created_at REAL NOT NULL, resumed_at REAL)''')
        target_coord.db.execute('INSERT INTO root_team_stops VALUES(?,?,?,?,?)',
                                (self.STOP, 'root', 1, now - 100, None))
        with contextlib.closing(sqlite3.connect(self.registry)) as registry:
            registry.execute('INSERT INTO projects VALUES(?)', (project,))
            registry.execute('INSERT INTO root_team_operation_projects VALUES(?,?,?,NULL)',
                             (self.STOP, project, 'root'))
            registry.execute('INSERT INTO root_projects VALUES(?,?,1)', (project, 'root'))
            registry.commit()
        target_coord.db.commit()
        arguments = dict(master_db=self.master, service_plist=self.plist, project=project,
            resource=resource, owner_session='wbt-owner', reservation_id=claim['reservation_id'],
            expected_deadline=claim['deadline'], operator_message_id=['wbt-operator-source'],
            action='wbt-preview', cohort_ref=_WBT_COHORT_REF)
        return arguments, target_coord, target

    @staticmethod
    def _database_dump(path):
        with contextlib.closing(sqlite3.connect(path)) as db:
            return tuple(db.iterdump())

    @staticmethod
    def _sqlite_sidecar_state(path):
        suffixes=('-journal','-wal','-shm')
        return tuple((suffix,Path(str(path)+suffix).read_bytes()
                      if Path(str(path)+suffix).is_file() else None)
                     for suffix in suffixes)

    def _wbt_authority(self):
        return patch('agent_chat.expired_claim_disposition.authority',
                     return_value=(('wbt-operator-source', 'bodyhash'),))

    def _install_real_wbt_authority(self, arguments):
        """Persist the exact synthetic authenticated operator-to-Root source."""
        message_id=arguments['operator_message_id'][0]
        self.coord.session='root'
        self.coord.register('root')
        self.coord.db.execute('INSERT OR REPLACE INTO bridge_bindings VALUES(?,?,?,?,?)',
                              ('root','thread-root',None,None,time.time()))
        self.coord.db.execute('INSERT OR REPLACE INTO remote_session_hosts VALUES(?,?,?,?)',
                              ('root','host-root',time.time(),time.time()))
        with contextlib.closing(sqlite3.connect(self.registry)) as registry:
            config_columns={row[1] for row in registry.execute('PRAGMA table_info(root_config)')}
            if 'thread_id' not in config_columns:
                registry.execute('ALTER TABLE root_config ADD COLUMN thread_id TEXT')
            registry.execute("UPDATE root_config SET thread_id='thread-root' WHERE singleton=1")
            registry.commit()
        self.coord.db.execute('INSERT OR IGNORE INTO operator_identities VALUES(?)',('operator-source',))
        self.coord.db.execute('''INSERT OR REPLACE INTO messages
            (id,sender_session,recipient_session,body,created_at,acked_at)
            VALUES(?,?,?,?,?,NULL)''',
            (message_id,'operator-source','root','authorized exact WBT preview fixture',time.time()))
        self.coord.db.execute('INSERT OR REPLACE INTO operator_message_origins VALUES(?,?,?)',
            (message_id,'operator-source','authenticated_operator_ui'))
        self.coord.db.commit()

    def _install_cancelled_fresh_intent(self, *, audit_state='cancelled',
                                        request_id='11111111-aaaa-4bbb-8ccc-111111111111',
                                        candidate='11111111-2222-4333-8444-555555555555',
                                        current_thread='thread-root', outcome='candidate_not_materialized_native_outcome_unknown',
                                        audit_request_id=None, audit_candidate=None,
                                        reason=None, authorization=None):
        """Persist the exact typed terminal-cancellation shape in this disposable registry."""
        error=('Candidate recovery required: exact retained thread is not materialized: '
               'RPC error -32600: no rollout found for thread id '+candidate)
        with contextlib.closing(sqlite3.connect(self.registry)) as registry:
            registry.row_factory=sqlite3.Row
            columns={row[1] for row in registry.execute('PRAGMA table_info(root_fresh_requests)')}
            for name, declaration in (
                    ('home_project','TEXT'),('home_session','TEXT'),('host_id','TEXT'),
                    ('old_thread_id','TEXT'),('native_action','TEXT'),('new_thread_id','TEXT'),
                    ('queue_id','TEXT'),('native_proof_json','TEXT'),('response_proof_json','TEXT'),
                    ('owner_proof_json','TEXT'),('error','TEXT')):
                if name not in columns:
                    registry.execute('ALTER TABLE root_fresh_requests ADD COLUMN '+name+' '+declaration)
            config_columns={row[1] for row in registry.execute('PRAGMA table_info(root_config)')}
            if 'thread_id' not in config_columns:
                registry.execute('ALTER TABLE root_config ADD COLUMN thread_id TEXT')
            registry.execute('UPDATE root_config SET thread_id=? WHERE singleton=1',(current_thread,))
            install_root_restart_schema(registry)
            reason = reason or ('Request '+request_id[:3]+' handoff intent cancelled after exact retained candidate '
                'not-loaded evidence. Candidate/history and native outcome remain unknown; no native closure, '
                'deletion, or project-stop change is asserted.')
            authorization = authorization or json.dumps(dict(
                instruction_origin='app_po_pending_intent_cancellation_assignment',
                assignment_message_id='message_synthetic-cancellation-assignment',
                request_id=request_id,candidate_thread_id=candidate,expected_thread=current_thread,
                boundary='cancel only this future app handoff intent and its request gates; preserve native unknown and all candidate/history'),
                sort_keys=True)
            registry.execute('''INSERT INTO root_fresh_requests
                (id,status,home_project,home_session,host_id,old_thread_id,native_action,new_thread_id,
                 queue_id,native_proof_json,response_proof_json,owner_proof_json,error)
                VALUES(?,?,?,?,?,?,'creating',?,NULL,NULL,NULL,NULL,?)''',
                (request_id,'retired','default','root','host-root',current_thread,candidate,error))
            registry.execute('''INSERT INTO root_fresh_pending_cancellations VALUES
                (?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                ('22222222-aaaa-4bbb-8ccc-222222222222',audit_request_id or request_id,
                 'default','root','host-root',current_thread,
                 audit_candidate or candidate,
                 outcome,reason,authorization,
                 audit_state,time.time(),time.time()))
            registry.commit()
        return request_id,candidate

    def _install_retired_replacement_intent(self, *, request_id='11111111-bbbb-4ccc-8ddd-111111111111',
                                            candidate='22222222-bbbb-4ccc-8ddd-222222222222',
                                            reconciliation='33333333-bbbb-4ccc-8ddd-333333333333',
                                            retirement_id='44444444-bbbb-4ccc-8ddd-444444444444',
                                            outcome='replacement_start_outcome_unknown_no_native_closure',
                                            state='retired', authorization_override=None,
                                            include_replacement=True, include_start_uncertain=True):
        """Persist a synthetic instance of the typed immutable Root retirement contract."""
        now=time.time()
        replacement_auth=json.dumps(dict(
            instruction_origin='direct_native_user_reply',
            authorization_ref='request_user_input_async:call_fixture0000000000:0',
            question_item_id=['request_user_input_async','call_fixture0000000000',0],
            answer='Replace candidate within same request (Recommended)',
            question='The exact candidate '+candidate+' request '+request_id
                +' has no saved rollout and needs replacement verification.',
            request_id=request_id,original_candidate_thread_id=candidate,
            canonical_session='root',host_id='host-root',
            record_provenance='Root neutral record of actual native user question/reply; not an agent-chat server-stamped message'),
            sort_keys=True)
        retirement_auth=json.dumps(dict(
            instruction_origin='root_delegated_bounded_recovery_instruction',
            supporting_native_user_authorization_ref='request_user_input_async:call_fixture0000000000:0',
            question_item_id=['request_user_input_async','call_fixture0000000000',0],
            question='The exact candidate '+candidate+' in request '+request_id
                +' was not materialized; preserve native outcome as unknown.',
            answer='Replace candidate within same request (Recommended)',
            record_provenance='Root neutral record of actual native replacement authorization; the retirement is the separately delegated bounded recovery action, not a new direct-native reply',
            action='retire_uncertain_same_request_after_replacement_settings_drift',
            request_id=request_id,reconciliation_id=reconciliation,expected_thread='thread-root'),
            sort_keys=True)
        if authorization_override is not None:
            retirement_auth=authorization_override
        reason=('Fresh-start request retired by operator direction; native replacement outcome remains uncertain '
                'and no native closure or cleanup is asserted.')
        with contextlib.closing(sqlite3.connect(self.registry)) as registry:
            registry.row_factory=sqlite3.Row
            install_root_restart_schema(registry)
            columns={row['name'] for row in registry.execute('PRAGMA table_info(root_fresh_requests)')}
            for name,declaration in (
                    ('home_project','TEXT'),('home_session','TEXT'),('host_id','TEXT'),
                    ('old_thread_id','TEXT'),('native_action','TEXT'),('new_thread_id','TEXT'),
                    ('queue_id','TEXT'),('native_proof_json','TEXT'),('response_proof_json','TEXT'),
                    ('owner_proof_json','TEXT'),('error','TEXT')):
                if name not in columns:
                    registry.execute('ALTER TABLE root_fresh_requests ADD COLUMN '+name+' '+declaration)
            root_columns={row['name'] for row in registry.execute('PRAGMA table_info(root_config)')}
            if 'thread_id' not in root_columns:
                registry.execute('ALTER TABLE root_config ADD COLUMN thread_id TEXT')
            registry.execute("UPDATE root_config SET thread_id='thread-root' WHERE singleton=1")
            registry.execute('''INSERT INTO root_fresh_requests
                (id,status,home_project,home_session,host_id,old_thread_id,native_action,new_thread_id,
                 queue_id,native_proof_json,response_proof_json,owner_proof_json,error)
                VALUES(?,'retired','default','root','host-root','thread-root','creating',?,NULL,NULL,NULL,NULL,?)''',
                (request_id,candidate,'Candidate creation settings proof differed; no boot was consumed.'))
            if include_replacement:
                registry.execute('''INSERT INTO root_fresh_replacements VALUES
                    (?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                    (request_id,reconciliation,'default','root','host-root','thread-root',candidate,None,
                     'settings-digest','start_uncertain','settings drift',replacement_auth,now,now))
            registry.execute('''INSERT INTO root_fresh_retirements VALUES
                (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                (retirement_id,request_id,'default','root','host-root','thread-root',reconciliation,
                 candidate,None,outcome,reason,retirement_auth,state,now,now))
            events=[(reconciliation,'requested','synthetic original request authorization',now),
                    (reconciliation,'preflight_verified','exact Root and old-thread preflight',now+0.001),
                    (reconciliation,'start_started','one native start was in flight',now+0.002)]
            if include_start_uncertain:
                events.append((reconciliation,'start_uncertain','candidate result remains unresolved',now+0.003))
            registry.executemany('''INSERT INTO root_fresh_replacement_events
                (reconciliation_id,event,detail,recorded_at) VALUES(?,?,?,?)''',events)
            registry.commit()
        return request_id,candidate

    def _wbt_artifact(self, arguments, rows=None, *, omit_field=None, raw=None, expected_hash=None):
        if raw is None:
            selected=(arguments['resource'],arguments['owner_session'],
                      arguments['reservation_id'],arguments['expected_deadline'])
            if rows is None:
                rows=[selected,('wbt:other-row','fixture-owner','fixture-rid',
                                arguments['expected_deadline']+1.0)]
            encoded=[]
            for resource,owner,rid,deadline in rows:
                encoded.append(subprocess.run(['jq','-cn','--arg','resource',resource,'--arg','owner',owner,
                    '--arg','rid',rid,'--argjson','deadline',repr(deadline),'--arg','omit',omit_field or '',
                    '{complete_guard_generation:null,closure_witness:null,current_eligible_cas:"not_demonstrated",current_host:null,deadline:$deadline,eligibility_reason:"unknown",owner_session:$owner,reservation_id:$rid,resource:$resource,snapshot_queue:[],snapshot_reason:null,snapshot_state:"stale"} | if $omit=="" then . else del(.[$omit]) end'],
                    capture_output=True,check=True).stdout)
            raw=subprocess.run(['jq','-sc','--arg','counts',_WBT_COUNTS_SOURCE_PATH,'--arg','counts_sha',_WBT_COUNTS_SOURCE_SHA256,
                '--arg','ledger',_WBT_LEDGER_SOURCE_PATH,'--arg','ledger_sha',_WBT_LEDGER_SOURCE_SHA256,
                '{evidence_kind:"dated_supported_status_snapshot_not_current",row_count:length,source_files:{counts:{path:$counts,sha256:$counts_sha},ledger:{path:$ledger,sha256:$ledger_sha}},rows:.}'],
                input=b''.join(encoded),capture_output=True,check=True).stdout
        path=self.master.parent/'wbt-cohort-fixture.json'
        path.write_bytes(raw)
        return patch.multiple(
            'agent_chat.expired_claim_disposition',
            _WBT_COHORT_ARTIFACT_PATH=str(path),
            _WBT_COHORT_ARTIFACT_SHA256=expected_hash or hashlib.sha256(raw).hexdigest(),
            _WBT_COHORT_ARTIFACT_SIZE=len(raw),
            _WBT_COHORT_ROW_COUNT=(len(rows) if rows is not None else 2))

    def _wbt_follow_on_arguments(self, original_index):
        candidates=[
            {'original_index':3,'resource':'file:.finite-batch-64-0077/src/agent_chat/remote_service.py',
             'owner_session':'session_3d8278d80221401e939587db90280dc1',
             'reservation_id':'reservation_TIn07NCQ8NUskc_oYmEzAGsjBzP_lc3m',
             'original_deadline':1791928079.0198998},
            {'original_index':4,'resource':'file:.finite-batch-64-0077/tests/test_remote_coord.py',
             'owner_session':'session_3d8278d80221401e939587db90280dc1',
             'reservation_id':'reservation_aeBpWZroXUUZB3mXtKpx_fnPmCvdVeAr',
             'original_deadline':1791928079.145308},
            {'original_index':5,'resource':'file:.finite-batch-64-0077/tests/test_remote_run.py',
             'owner_session':'session_3d8278d80221401e939587db90280dc1',
             'reservation_id':'reservation_-hMAlFaa5CQpbb-dAkq-gFaqjVkVKuaP',
             'original_deadline':1791928079.264997},
        ]
        chosen=next(row for row in candidates if row['original_index']==original_index)
        arguments,target_coord,target,_=self.wbt_abandon_arguments(chosen['resource'])
        target_coord.session=chosen['owner_session']
        target_coord.register('reviewer_group')
        target_coord.db.execute('UPDATE resources SET owner_session=?,reservation_id=?,deadline=?,stale=1 WHERE name=?',
            (chosen['owner_session'],chosen['reservation_id'],chosen['original_deadline'],chosen['resource']))
        target_coord.db.execute('DELETE FROM bridge_bindings WHERE session_id=?',('wbt-owner',))
        target_coord.db.execute('DELETE FROM remote_session_hosts WHERE session_id=?',('wbt-owner',))
        target_coord.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?,?,?)',
            (chosen['owner_session'],'thread-wbt-owner',None,None,time.time()))
        target_coord.db.execute('INSERT INTO remote_session_hosts VALUES(?,?,?,?)',
            (chosen['owner_session'],'host-root',time.time(),time.time()))
        target_coord.db.commit()
        arguments['reservation_id']=chosen['reservation_id']
        arguments['owner_session']=chosen['owner_session']
        arguments['expected_deadline']=chosen['original_deadline']
        rows=[('fixture:source-row-0','fixture-owner-0','fixture-rid-0',chosen['original_deadline']-3),
              ('fixture:source-row-1','fixture-owner-1','fixture-rid-1',chosen['original_deadline']-2),
              ('fixture:source-row-2','fixture-owner-2','fixture-rid-2',chosen['original_deadline']-1)]+[
              (row['resource'],row['owner_session'],row['reservation_id'],row['original_deadline'])
              for row in candidates]
        artifact=self._wbt_artifact(arguments,rows=rows)
        return arguments,target_coord,target,artifact,candidates,chosen

    def _wbt_follow_on_manifest(self, candidates, *, raw_override=None, expected_hash=None):
        source_hash=expired_claim_module._WBT_COHORT_ARTIFACT_SHA256
        source_size=expired_claim_module._WBT_COHORT_ARTIFACT_SIZE
        source_rows=expired_claim_module._WBT_COHORT_ROW_COUNT
        manifest={
            'manifest_version':expired_claim_module._WBT_FOLLOW_ON_MANIFEST_VERSION,
            'purpose':'historical candidate selection only; not current eligibility',
            'source':{'artifact':expired_claim_module._WBT_COHORT_ARTIFACT_PATH,
                      'sha256':source_hash,'bytes':source_size,'row_count':source_rows,
                      'evidence_kind':'dated_supported_status_snapshot_not_current'},
            'selection':{'owner_session':'session_3d8278d80221401e939587db90280dc1',
                         'resource_prefix':'file:.finite-batch-64-0077/',
                         'excluded_reservation_id':'reservation_viTB1CkNr6BIDqGJ9trNcwxDzaMVHOWg',
                         'order':['resource','reservation_id'],'maximum':8},
            'candidates':candidates,
        }
        raw=raw_override if raw_override is not None else (
            json.dumps(manifest,sort_keys=True,separators=(',',':'))+'\n').encode('utf-8')
        path=self.master.parent/'wbt-follow-on-fixture.json'
        path.write_bytes(raw)
        return patch.multiple('agent_chat.expired_claim_disposition',
            _WBT_FOLLOW_ON_MANIFEST_PATH=str(path),
            _WBT_FOLLOW_ON_MANIFEST_SHA256=expected_hash or hashlib.sha256(raw).hexdigest(),
            _WBT_FOLLOW_ON_MANIFEST_SIZE=len(raw))

    def _wbt_group_candidates(self):
        return [
            {'original_index':6,'resource':'file:.finite-batch-64-0080/docs/reference/agent-operations.md',
             'owner_session':'session_3d8278d80221401e939587db90280dc1',
             'reservation_id':'reservation_BDSuc5qBwuQCAYTX_lDwcsxDDNg3BZjf',
             'original_deadline':1791934356.1897569},
            {'original_index':7,'resource':'file:.finite-batch-64-0080/src/agent_chat/cli.py',
             'owner_session':'session_3d8278d80221401e939587db90280dc1',
             'reservation_id':'reservation_c6WRF-AMnOCsTWy_NfnGYHcTZWUrimcB',
             'original_deadline':1791934344.306678},
            {'original_index':8,'resource':'file:.finite-batch-64-0080/src/agent_chat/remote_service.py',
             'owner_session':'session_3d8278d80221401e939587db90280dc1',
             'reservation_id':'reservation_Pg7c3LndYFDhJZDZizNrKngq1-Pl16To',
             'original_deadline':1791934355.752901},
            {'original_index':9,'resource':'file:.finite-batch-64-0080/tests/test_remote_coord.py',
             'owner_session':'session_3d8278d80221401e939587db90280dc1',
             'reservation_id':'reservation_t5Dn1WRf6tqISQ2f1LpNgDTsEF5rPxjE',
             'original_deadline':1791934354.9793081},
            {'original_index':10,'resource':'file:.finite-batch-64-0080/tests/test_remote_run.py',
             'owner_session':'session_3d8278d80221401e939587db90280dc1',
             'reservation_id':'reservation_L9h-WOpq1Thng1IJMBySZwixeH0-wKRL',
             'original_deadline':1791934356.719527},
        ]

    def _wbt_group_manifest(self, candidates, *, raw_override=None, expected_hash=None):
        source_hash=expired_claim_module._WBT_COHORT_ARTIFACT_SHA256
        source_size=expired_claim_module._WBT_COHORT_ARTIFACT_SIZE
        source_rows=expired_claim_module._WBT_COHORT_ROW_COUNT
        manifest={
            'manifest_version':expired_claim_module._WBT_GROUP_MANIFEST_VERSION,
            'purpose':'historical candidate selection only; not current eligibility',
            'source':{'artifact':expired_claim_module._WBT_COHORT_ARTIFACT_PATH,
                      'sha256':source_hash,'bytes':source_size,'row_count':source_rows,
                      'evidence_kind':'dated_supported_status_snapshot_not_current'},
            'selection':{'owner_session':'session_3d8278d80221401e939587db90280dc1',
                         'resource_prefix':'file:.finite-batch-64-0080/',
                         'excluded_reservation_ids':[
                             'reservation_viTB1CkNr6BIDqGJ9trNcwxDzaMVHOWg',
                             'reservation_TIn07NCQ8NUskc_oYmEzAGsjBzP_lc3m',
                             'reservation_aeBpWZroXUUZB3mXtKpx_fnPmCvdVeAr',
                             'reservation_-hMAlFaa5CQpbb-dAkq-gFaqjVkVKuaP'],
                         'order':['resource','reservation_id'],'maximum':8},
            'candidates':candidates,
        }
        raw=raw_override if raw_override is not None else (
            json.dumps(manifest,sort_keys=True,separators=(',',':'))+'\n').encode('utf-8')
        path=self.master.parent/'wbt-group-fixture.json'
        path.write_bytes(raw)
        return patch.multiple('agent_chat.expired_claim_disposition',
            _WBT_GROUP_MANIFEST_PATH=str(path),
            _WBT_GROUP_MANIFEST_SHA256=expected_hash or hashlib.sha256(raw).hexdigest(),
            _WBT_GROUP_MANIFEST_SIZE=len(raw))

    def _wbt_group_artifact_context(self, arguments, candidates, *, rows=None):
        # Build fresh patch contexts on every entry. Several lifecycle tests
        # deliberately enter this context once for PREPARE and again for APPLY.
        case=self
        class ReusableArtifactContext:
            def __enter__(self):
                self.stack=contextlib.ExitStack()
                self.stack.enter_context(case._wbt_artifact(arguments,rows=rows))
                self.stack.enter_context(case._wbt_group_manifest(candidates))
                return self
            def __exit__(self,*exc):
                return self.stack.__exit__(*exc)
        return ReusableArtifactContext()

    def _wbt_group_arguments(self, original_index):
        candidates=self._wbt_group_candidates()
        chosen=next(row for row in candidates if row['original_index']==original_index)
        arguments,target_coord,target,artifact=self.wbt_abandon_arguments(chosen['resource'])
        return arguments,target_coord,target,artifact,candidates,chosen

    def test_wbt_witness_preview_is_exact_read_only_and_keeps_eligibility_unproven(self):
        arguments, target_coord, target = self.wbt_target_arguments()
        artifact=self._wbt_artifact(arguments)
        self.assertNotEqual(target, self.master)
        before_target = self._database_dump(target)
        before_registry = self._database_dump(self.registry)
        with artifact, self._wbt_authority():
            result = dict(execute_expired_claim(**arguments))
        self.assertEqual(result['status'], 'observed')
        self.assertEqual(result['cohort_ref'], _WBT_COHORT_REF)
        self.assertEqual(result['cohort_membership_basis'],'local_pinned_artifact_sha256_and_exact_tuple')
        self.assertEqual(result['artifact_byte_identity'],'MATCH_TO_REVIEWED_PINNED_DATED_SOURCE_BYTES')
        self.assertEqual(result['artifact_receipt_validation'],
                         'OFFLINE_CUSTODY_CHAIN_REVIEWED_NOT_REAUTHENTICATED_PER_CALL')
        self.assertEqual(result['project'], _WBT_PROJECT_ID)
        self.assertEqual(result['resource'], 'wbt:exact-row')
        self.assertEqual(result['owner_session'], 'wbt-owner')
        self.assertEqual(result['bound_thread_id'], 'thread-wbt-owner')
        self.assertEqual(result['registered_host_id'], 'host-root')
        self.assertEqual(result['configured_root_host_id'], 'host-root')
        with _read_existing(self.registry) as registry:
            operation_created=registry.execute(
                'SELECT created_at FROM root_team_operations WHERE id=?',(self.STOP,)).fetchone()[0]
        self.assertEqual(result['project_barrier_snapshot_digest'],
                         expired_claim_module.digest(('active-project-stop-read-v1',self.STOP,
                             'root',_WBT_PROJECT_ID,'root',
                             operation_created,
                             target_coord.db.execute('SELECT created_at FROM root_team_stops WHERE id=?',
                                                     (self.STOP,)).fetchone()[0],1,None)))
        self.assertEqual(result['open_guard_count'], 0)
        self.assertEqual(result['fifo_count'], 0)
        self.assertTrue(result['fifo_is_empty'])
        self.assertEqual(result['eligibility'], 'NOT_DEMONSTRATED')
        self.assertEqual(result['process_closure'], 'UNVERIFIED')
        self.assertTrue(result['preview_digest'])
        self.assertTrue(result['durable_request'].startswith('NONE;'))

    def test_wbt_active_stop_preview_composes_real_root_operator_authority_without_writes(self):
        arguments,target_coord,target=self.wbt_target_arguments('wbt:composed-authority')
        arguments['operator_message_id']=['operator-message-to-canonical-root']
        self._install_real_wbt_authority(arguments)
        artifact=self._wbt_artifact(arguments)
        before_master=self._database_dump(self.master)
        before_registry=self._database_dump(self.registry)
        before_target=self._database_dump(target)
        before_sidecars={str(path):self._sqlite_sidecar_state(path)
                         for path in (self.master,self.registry,target)}
        with artifact, \
                patch('agent_chat.root_local._installed_server_config',return_value=self.config), \
                patch('agent_chat.root_local.configured_server_database',return_value=self.master):
            result=dict(execute_expired_claim(**arguments))
        self.assertEqual(result['status'],'observed')
        self.assertEqual(result['operator_authority_digest'],
                         expired_claim_module.digest((('operator-message-to-canonical-root',
                           hashlib.sha256(b'authorized exact WBT preview fixture').hexdigest()),)))
        self.assertEqual(self._database_dump(self.master),before_master)
        self.assertEqual(self._database_dump(self.registry),before_registry)
        self.assertEqual(self._database_dump(target),before_target)
        after_sidecars={str(path):self._sqlite_sidecar_state(path)
                        for path in (self.master,self.registry,target)}
        self.assertEqual(after_sidecars,before_sidecars)
        self.assertEqual(result['eligibility'],'NOT_DEMONSTRATED')
        self.assertEqual(result['process_closure'],'UNVERIFIED')
        self.assertEqual(before_target, self._database_dump(target))
        self.assertEqual(before_registry, self._database_dump(self.registry))
        self.assertIsNone(target_coord.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_expired_claim_dispositions'").fetchone())

    def test_wbt_preview_accepts_only_typed_terminal_cancellation_and_default_loader_still_denies(self):
        arguments,target_coord,target=self.wbt_target_arguments('wbt:cancelled-intent')
        self._install_real_wbt_authority(arguments)
        request_id,candidate=self._install_cancelled_fresh_intent()
        with self.assertRaisesRegex(CoordError,'pending fresh-start'):
            load_configured_root(self.master,self.plist,project_scope='default')
        root=load_wbt_preview_configured_root(self.master,self.plist)
        self.assertEqual((root.home_project,root.home_session,root.host_id),('default','root','host-root'))
        artifact=self._wbt_artifact(arguments)
        before_master=self._database_dump(self.master)
        before_registry=self._database_dump(self.registry)
        before_target=self._database_dump(target)
        before_sidecars={str(path):self._sqlite_sidecar_state(path)
                         for path in (self.master,self.registry,target)}
        with artifact,self._wbt_authority():
            result=dict(execute_expired_claim(**arguments))
        self.assertEqual(result['status'],'observed')
        self.assertEqual(result['eligibility'],'NOT_DEMONSTRATED')
        self.assertEqual(result['process_closure'],'UNVERIFIED')
        self.assertEqual(self._database_dump(self.master),before_master)
        self.assertEqual(self._database_dump(self.registry),before_registry)
        self.assertEqual(self._database_dump(target),before_target)
        self.assertEqual({str(path):self._sqlite_sidecar_state(path)
                          for path in (self.master,self.registry,target)},before_sidecars)
        with contextlib.closing(sqlite3.connect(self.registry)) as registry:
            state=registry.execute('SELECT status,new_thread_id FROM root_fresh_requests WHERE id=?',
                                   (request_id,)).fetchone()
        self.assertEqual(tuple(state),('retired',candidate))

    def test_wbt_preview_accepts_exact_typed_retirement_and_diagnosis_still_never_opens_target(self):
        arguments,target_coord,target=self.wbt_target_arguments('wbt:typed-retirement')
        arguments['operator_message_id']=['operator-message-to-canonical-root']
        self._install_real_wbt_authority(arguments)
        request_id,candidate=self._install_retired_replacement_intent()
        with self.assertRaisesRegex(CoordError,'pending fresh-start'):
            load_configured_root(self.master,self.plist,project_scope='default')
        artifact=self._wbt_artifact(arguments)
        before={str(path):self._database_dump(path)
                for path in (self.master,self.registry,target)}
        sidecars={str(path):self._sqlite_sidecar_state(path)
                  for path in (self.master,self.registry,target)}
        with artifact, \
                patch('agent_chat.root_local._installed_server_config',return_value=self.config), \
                patch('agent_chat.root_local.configured_server_database',return_value=self.master):
            result=dict(execute_expired_claim(**arguments))
        self.assertEqual(result['status'],'observed')
        self.assertEqual(result['operator_authority_digest'],
                         expired_claim_module.digest((('operator-message-to-canonical-root',
                           hashlib.sha256(b'authorized exact WBT preview fixture').hexdigest()),)))
        self.assertEqual(result['eligibility'],'NOT_DEMONSTRATED')
        self.assertEqual(result['process_closure'],'UNVERIFIED')
        self.assertEqual(self._database_dump(self.master),before[str(self.master)])
        self.assertEqual(self._database_dump(self.registry),before[str(self.registry)])
        self.assertEqual(self._database_dump(target),before[str(target)])
        self.assertEqual({str(path):self._sqlite_sidecar_state(path)
                          for path in (self.master,self.registry,target)},sidecars)
        with contextlib.closing(sqlite3.connect(self.registry)) as registry:
            row=registry.execute('SELECT status,new_thread_id FROM root_fresh_requests WHERE id=?',
                                  (request_id,)).fetchone()
        self.assertEqual(tuple(row),('retired',candidate))

        diagnostic=dict(arguments,action='wbt-diagnose')
        real_open=expired_claim_module._wbt_readonly_connection
        opened=[]
        def tracked_open(path):
            opened.append(Path(path).resolve())
            self.assertEqual(Path(path).resolve(),self.registry.resolve())
            return real_open(path)
        with artifact,patch('agent_chat.root_local._installed_server_config',return_value=self.config), \
                patch('agent_chat.root_local.configured_server_database',return_value=self.master), \
                patch('agent_chat.expired_claim_disposition._wbt_readonly_connection',
                      side_effect=tracked_open),patch(
                'agent_chat.expired_claim_disposition.snapshot',
                side_effect=AssertionError('diagnosis must stop before target snapshot')):
            diagnosed=dict(execute_expired_claim(**diagnostic))
        self.assertEqual(diagnosed['status'],'clear')
        self.assertEqual(diagnosed['request_id'],'')
        self.assertEqual(diagnosed['check_code'],'typed_history_clear')
        self.assertEqual(set(diagnosed),{'status','request_id','check_code','sampled_at'})
        self.assertEqual(opened,[self.registry.resolve()])
        self.assertEqual({str(path):self._database_dump(path)
                          for path in (self.master,self.registry,target)},before)
        self.assertEqual({str(path):self._sqlite_sidecar_state(path)
                          for path in (self.master,self.registry,target)},sidecars)

    def test_wbt_preview_rejects_malformed_or_wrong_candidate_retirement_authorization(self):
        arguments,_,_=self.wbt_target_arguments('wbt:bad-retirement-auth')
        self._install_real_wbt_authority(arguments)
        wrong=json.dumps(dict(
            instruction_origin='root_delegated_bounded_recovery_instruction',
            supporting_native_user_authorization_ref='request_user_input_async:fixture-call:0',
            question_item_id=['request_user_input_async','fixture-call',0],
            question='Question about a different request and candidate',
            answer='Replace candidate within same request (Recommended)',
            record_provenance='Root neutral record of actual native replacement authorization; the retirement is the separately delegated bounded recovery action, not a new direct-native reply',
            action='retire_uncertain_same_request_after_replacement_settings_drift',
            request_id='99999999-bbbb-4ccc-8ddd-999999999999',
            reconciliation_id='33333333-bbbb-4ccc-8ddd-333333333333',
            expected_thread='thread-root'),sort_keys=True)
        self._install_retired_replacement_intent(authorization_override=wrong)
        with self.assertRaisesRegex(CoordError,'terminal typed WBT administration'):
            load_wbt_preview_configured_root(self.master,self.plist)

    def test_wbt_preview_rejects_weakened_retirement_immutability_trigger(self):
        arguments,_,_=self.wbt_target_arguments('wbt:weak-retirement-trigger')
        self._install_real_wbt_authority(arguments)
        self._install_retired_replacement_intent()
        with contextlib.closing(sqlite3.connect(self.registry)) as registry:
            registry.execute('DROP TRIGGER immutable_root_fresh_retirement_identity')
            registry.execute('''CREATE TRIGGER immutable_root_fresh_retirement_identity
                BEFORE UPDATE ON root_fresh_retirements WHEN 0
                BEGIN SELECT RAISE(ABORT,'root fresh-start retirement audit is immutable'); END''')
            registry.commit()
        with self.assertRaisesRegex(CoordError,'terminal typed WBT administration'):
            load_wbt_preview_configured_root(self.master,self.plist)

    def test_wbt_preview_keeps_retirement_blocked_when_another_request_is_pending(self):
        arguments,_,_=self.wbt_target_arguments('wbt:retirement-and-pending')
        self._install_real_wbt_authority(arguments)
        self._install_retired_replacement_intent()
        with contextlib.closing(sqlite3.connect(self.registry)) as registry:
            registry.execute("INSERT INTO root_fresh_requests(id,status) VALUES('unrelated-active','pending')")
            registry.commit()
        with self.assertRaisesRegex(CoordError,'terminal typed WBT administration'):
            load_wbt_preview_configured_root(self.master,self.plist)

    def test_wbt_preview_transactional_recheck_rejects_new_request_after_valid_retirement(self):
        arguments,_,_=self.wbt_target_arguments('wbt:retirement-transaction-race')
        self._install_real_wbt_authority(arguments)
        self._install_retired_replacement_intent()
        artifact=self._wbt_artifact(arguments)
        original=expired_claim_module.wbt_witness_selection
        calls={'count':0}
        def selection_with_new_pending(*args,**kwargs):
            selected=original(*args,**kwargs)
            calls['count']+=1
            if calls['count']==2:
                with contextlib.closing(sqlite3.connect(self.registry)) as registry:
                    registry.execute("INSERT INTO root_fresh_requests(id,status) VALUES('late-active','pending')")
                    registry.commit()
            return selected
        with artifact,self._wbt_authority(),patch(
                'agent_chat.expired_claim_disposition.wbt_witness_selection',
                side_effect=selection_with_new_pending),patch(
                'agent_chat.expired_claim_disposition.snapshot') as snapshot_fn:
            with self.assertRaisesRegex(CoordError,'fresh-start history changed before WBT read'):
                execute_expired_claim(**arguments)
            snapshot_fn.assert_not_called()
        self.assertEqual(calls['count'],2)

    def test_wbt_preview_rejects_retirement_without_matching_replacement_record(self):
        arguments,_,_=self.wbt_target_arguments('wbt:retirement-without-replacement')
        self._install_real_wbt_authority(arguments)
        self._install_retired_replacement_intent(include_replacement=False)
        with self.assertRaisesRegex(CoordError,'terminal typed WBT administration'):
            load_wbt_preview_configured_root(self.master,self.plist)

    def test_wbt_preview_requires_retirement_terminal_state_and_immutable_start_uncertain_event(self):
        arguments,_,_=self.wbt_target_arguments('wbt:retirement-event-required')
        self._install_real_wbt_authority(arguments)
        self._install_retired_replacement_intent(include_start_uncertain=False)
        with self.assertRaisesRegex(CoordError,'terminal typed WBT administration'):
            load_wbt_preview_configured_root(self.master,self.plist)

    def test_wbt_preview_rejects_nonterminal_retirement_audit_state(self):
        arguments,_,_=self.wbt_target_arguments('wbt:retirement-nonterminal')
        self._install_real_wbt_authority(arguments)
        self._install_retired_replacement_intent(state='retiring')
        with self.assertRaisesRegex(CoordError,'terminal typed WBT administration'):
            load_wbt_preview_configured_root(self.master,self.plist)

    def test_wbt_diagnosis_returns_only_first_current_root_blocker_without_opening_target(self):
        arguments,target_coord,target=self.wbt_target_arguments('wbt:diagnosis-first-blocker')
        arguments['action']='wbt-diagnose'
        self._install_real_wbt_authority(arguments)
        self._install_cancelled_fresh_intent()
        blocker='33333333-aaaa-4bbb-8ccc-333333333333'
        with contextlib.closing(sqlite3.connect(self.registry)) as registry:
            registry.execute('''INSERT INTO root_fresh_requests
                (id,status,home_project,home_session,host_id,old_thread_id,native_action,
                 new_thread_id,queue_id,native_proof_json,response_proof_json,owner_proof_json,error)
                VALUES(?, 'created','default','root','host-root','thread-root','creating',?,NULL,NULL,NULL,NULL,NULL)''',
                (blocker,'44444444-aaaa-4bbb-8ccc-444444444444'))
            registry.commit()
        artifact=self._wbt_artifact(arguments)
        before={str(path):self._database_dump(path) for path in (self.master,self.registry,target)}
        before_sidecars={str(path):self._sqlite_sidecar_state(path)
                         for path in (self.master,self.registry,target)}
        real_open=expired_claim_module._wbt_readonly_connection
        opened=[]
        def tracked_open(path):
            opened.append(Path(path).resolve())
            self.assertEqual(Path(path).resolve(),self.registry.resolve())
            return real_open(path)
        with artifact,patch('agent_chat.root_local._installed_server_config',return_value=self.config), \
                patch('agent_chat.root_local.configured_server_database',return_value=self.master), \
                patch('agent_chat.expired_claim_disposition._wbt_readonly_connection',
                            side_effect=tracked_open),patch(
                'agent_chat.expired_claim_disposition.snapshot',side_effect=AssertionError('target snapshot forbidden')):
            result=dict(execute_expired_claim(**arguments))
        self.assertEqual(result['status'],'blocked')
        self.assertEqual(result['request_id'],blocker)
        self.assertEqual(result['check_code'],'request_not_terminal_typed_cancellation')
        self.assertIn('sampled_at',result)
        self.assertEqual(set(result),{'status','request_id','check_code','sampled_at'})
        self.assertEqual(opened,[self.registry.resolve()])
        self.assertEqual({str(path):self._database_dump(path) for path in (self.master,self.registry,target)},before)
        self.assertEqual({str(path):self._sqlite_sidecar_state(path)
                          for path in (self.master,self.registry,target)},before_sidecars)

    def test_wbt_diagnosis_all_clear_stops_before_target(self):
        arguments,target_coord,target=self.wbt_target_arguments('wbt:diagnosis-clear')
        arguments['action']='wbt-diagnose'
        self._install_real_wbt_authority(arguments)
        self._install_cancelled_fresh_intent()
        artifact=self._wbt_artifact(arguments)
        real_open=expired_claim_module._wbt_readonly_connection
        opened=[]
        def tracked_open(path):
            opened.append(Path(path).resolve())
            self.assertEqual(Path(path).resolve(),self.registry.resolve())
            return real_open(path)
        before={str(path):self._database_dump(path) for path in (self.master,self.registry,target)}
        before_sidecars={str(path):self._sqlite_sidecar_state(path)
                         for path in (self.master,self.registry,target)}
        with artifact,patch('agent_chat.root_local._installed_server_config',return_value=self.config), \
                patch('agent_chat.root_local.configured_server_database',return_value=self.master), \
                patch('agent_chat.expired_claim_disposition._wbt_readonly_connection',
                            side_effect=tracked_open),patch(
                'agent_chat.expired_claim_disposition.snapshot',side_effect=AssertionError('target snapshot forbidden')):
            clear=dict(execute_expired_claim(**arguments))
        self.assertEqual(clear['status'],'clear')
        self.assertEqual(clear['request_id'],'')
        self.assertEqual(clear['check_code'],'typed_history_clear')
        self.assertEqual(opened,[self.registry.resolve()])
        self.assertEqual({str(path):self._database_dump(path) for path in (self.master,self.registry,target)},before)
        self.assertEqual({str(path):self._sqlite_sidecar_state(path)
                          for path in (self.master,self.registry,target)},before_sidecars)

    def test_wbt_diagnosis_malformed_audit_is_value_free(self):
        arguments,_,target=self.wbt_target_arguments('wbt:diagnosis-malformed-audit')
        arguments['action']='wbt-diagnose'
        self._install_real_wbt_authority(arguments)
        self._install_cancelled_fresh_intent(authorization='{"secret":"private-not-output"}')
        artifact=self._wbt_artifact(arguments)
        real_open=expired_claim_module._wbt_readonly_connection
        opened=[]
        def tracked_open(path):
            opened.append(Path(path).resolve())
            self.assertEqual(Path(path).resolve(),self.registry.resolve())
            return real_open(path)
        before={str(path):self._database_dump(path) for path in (self.master,self.registry,target)}
        before_sidecars={str(path):self._sqlite_sidecar_state(path)
                         for path in (self.master,self.registry,target)}
        with artifact,patch('agent_chat.root_local._installed_server_config',return_value=self.config), \
                patch('agent_chat.root_local.configured_server_database',return_value=self.master), \
                patch('agent_chat.expired_claim_disposition._wbt_readonly_connection',
                      side_effect=tracked_open),patch(
                'agent_chat.expired_claim_disposition.snapshot',side_effect=AssertionError('target snapshot forbidden')):
            malformed=dict(execute_expired_claim(**arguments))
        self.assertEqual(malformed['status'],'blocked')
        self.assertEqual(malformed['request_id'],'11111111-aaaa-4bbb-8ccc-111111111111')
        self.assertEqual(malformed['check_code'],'cancellation_authorization_not_typed_for_request')
        self.assertNotIn('private-not-output',repr(malformed))
        self.assertEqual(set(malformed),{'status','request_id','check_code','sampled_at'})
        self.assertEqual(opened,[self.registry.resolve()])
        self.assertEqual({str(path):self._database_dump(path) for path in (self.master,self.registry,target)},before)
        self.assertEqual({str(path):self._sqlite_sidecar_state(path)
                          for path in (self.master,self.registry,target)},before_sidecars)

    def test_wbt_diagnosis_wrong_context_denies_or_returns_unknown_without_history_id(self):
        arguments,_,_=self.wbt_target_arguments('wbt:diagnosis-denial')
        arguments['action']='wbt-diagnose'
        self._install_real_wbt_authority(arguments)
        self._install_cancelled_fresh_intent()
        artifact=self._wbt_artifact(arguments)
        wrong_source=dict(arguments)
        wrong_source['operator_message_id']=['message_not_an_authorized_source']
        wrong_project=dict(arguments)
        wrong_project['project']='another-project'
        wrong_cohort=dict(arguments)
        wrong_cohort['cohort_ref']='message_other/attachment_other'
        history_calls=[]
        def unexpected_history_open(path):
            history_calls.append(Path(path).resolve())
            raise AssertionError('history disclosure must wait for exact authorization')
        with artifact,patch('agent_chat.root_local._installed_server_config',return_value=self.config), \
                patch('agent_chat.root_local.configured_server_database',return_value=self.master), \
                patch('agent_chat.expired_claim_disposition._wbt_readonly_connection',
                      side_effect=unexpected_history_open):
            with self.assertRaisesRegex(CoordError,'exact configured Wbt project'):
                execute_expired_claim(**wrong_project)
            with self.assertRaisesRegex(CoordError,'exact configured Wbt project'):
                execute_expired_claim(**wrong_cohort)
            with self.assertRaisesRegex(CoordError,'original operator UI authority'):
                execute_expired_claim(**wrong_source)
            wrong_tuple=dict(arguments)
            wrong_tuple['resource']='wbt:not-in-pinned-cohort'
            result=dict(execute_expired_claim(**wrong_tuple))
        self.assertEqual(result['status'],'unknown')
        self.assertEqual(result['request_id'],'')
        self.assertEqual(result['check_code'],'fixed_cohort_tuple_not_proven')
        self.assertEqual(set(result),{'status','request_id','check_code','sampled_at'})
        self.assertEqual(history_calls,[])

    def test_wbt_diagnosis_unreadable_registry_is_unknown_without_request_id(self):
        arguments,_,target=self.wbt_target_arguments('wbt:diagnosis-unreadable')
        arguments['action']='wbt-diagnose'
        self._install_real_wbt_authority(arguments)
        self._install_cancelled_fresh_intent()
        artifact=self._wbt_artifact(arguments)
        before={str(path):self._database_dump(path) for path in
                (self.master,self.registry,target)}
        before_sidecars={str(path):self._sqlite_sidecar_state(path) for path in
                         (self.master,self.registry,target)}
        opened=[]
        def unavailable_registry(path):
            opened.append(Path(path).resolve())
            self.assertEqual(Path(path).resolve(),self.registry.resolve())
            return None
        with artifact,patch('agent_chat.root_local._installed_server_config',return_value=self.config), \
                patch('agent_chat.root_local.configured_server_database',return_value=self.master), \
                patch('agent_chat.expired_claim_disposition._wbt_readonly_connection',
                      side_effect=unavailable_registry),patch(
                'agent_chat.expired_claim_disposition.snapshot',
                side_effect=AssertionError('target snapshot forbidden')),patch(
                'agent_chat.ownership_disposition.RpcClient',
                side_effect=AssertionError('provider construction forbidden')):
            result=dict(execute_expired_claim(**arguments))
        self.assertEqual(result['status'],'unknown')
        self.assertEqual(result['request_id'],'')
        self.assertEqual(result['check_code'],'configured_registry_unavailable')
        self.assertEqual(opened,[self.registry.resolve()])
        self.assertEqual({str(path):self._database_dump(path) for path in
                          (self.master,self.registry,target)},before)
        self.assertEqual({str(path):self._sqlite_sidecar_state(path) for path in
                          (self.master,self.registry,target)},before_sidecars)

    def test_wbt_diagnosis_incomplete_request_schema_returns_unknown_without_id(self):
        arguments,_,target=self.wbt_target_arguments('wbt:diagnosis-incomplete-schema')
        arguments['action']='wbt-diagnose'
        self._install_real_wbt_authority(arguments)
        with contextlib.closing(sqlite3.connect(self.registry)) as registry:
            registry.execute('DROP TABLE root_fresh_requests')
            registry.execute('CREATE TABLE root_fresh_requests (id TEXT PRIMARY KEY,status TEXT)')
            registry.execute('INSERT INTO root_fresh_requests VALUES(?,?)',
                             ('55555555-aaaa-4bbb-8ccc-555555555555','created'))
            registry.commit()
        artifact=self._wbt_artifact(arguments)
        before={str(path):self._database_dump(path) for path in
                (self.master,self.registry,target)}
        before_sidecars={str(path):self._sqlite_sidecar_state(path) for path in
                         (self.master,self.registry,target)}
        real_open=expired_claim_module._wbt_readonly_connection
        opened=[]
        def tracked_open(path):
            opened.append(Path(path).resolve())
            self.assertEqual(Path(path).resolve(),self.registry.resolve())
            return real_open(path)
        with artifact,patch('agent_chat.root_local._installed_server_config',return_value=self.config), \
                patch('agent_chat.root_local.configured_server_database',return_value=self.master), \
                patch('agent_chat.expired_claim_disposition._wbt_readonly_connection',
                      side_effect=tracked_open),patch(
                'agent_chat.expired_claim_disposition.snapshot',
                side_effect=AssertionError('target snapshot forbidden')),patch(
                'agent_chat.ownership_disposition.RpcClient',
                side_effect=AssertionError('provider construction forbidden')):
            result=dict(execute_expired_claim(**arguments))
        self.assertEqual(result['status'],'unknown')
        self.assertEqual(result['request_id'],'')
        self.assertEqual(result['check_code'],'fresh_request_schema_incomplete')
        self.assertEqual(set(result),{'status','request_id','check_code','sampled_at'})
        self.assertEqual(opened,[self.registry.resolve()])
        self.assertEqual({str(path):self._database_dump(path) for path in
                          (self.master,self.registry,target)},before)
        self.assertEqual({str(path):self._sqlite_sidecar_state(path) for path in
                          (self.master,self.registry,target)},before_sidecars)

    def test_wbt_diagnosis_rechecks_running_mode_before_history_disclosure(self):
        arguments,_,target=self.wbt_target_arguments('wbt:diagnosis-mode-race')
        arguments['action']='wbt-diagnose'
        self._install_real_wbt_authority(arguments)
        self._install_cancelled_fresh_intent()
        artifact=self._wbt_artifact(arguments)
        real_open=expired_claim_module._wbt_readonly_connection
        opened=[]
        expected_after_mode_change={}
        def switch_mode_then_open(path):
            opened.append(Path(path).resolve())
            self.assertEqual(Path(path).resolve(),self.registry.resolve())
            with contextlib.closing(sqlite3.connect(self.registry)) as registry:
                registry.execute("UPDATE root_config SET mode='maintenance_paused' WHERE singleton=1")
                registry.commit()
            expected_after_mode_change.update({str(item):self._database_dump(item) for item in
                                                (self.master,self.registry,target)})
            return real_open(path)
        with artifact,patch('agent_chat.root_local._installed_server_config',return_value=self.config), \
                patch('agent_chat.root_local.configured_server_database',return_value=self.master), \
                patch('agent_chat.expired_claim_disposition._wbt_readonly_connection',
                      side_effect=switch_mode_then_open),patch(
                'agent_chat.expired_claim_disposition.snapshot',
                side_effect=AssertionError('target snapshot forbidden')),patch(
                'agent_chat.ownership_disposition.RpcClient',
                side_effect=AssertionError('provider construction forbidden')):
            result=dict(execute_expired_claim(**arguments))
        self.assertEqual(result['status'],'unknown')
        self.assertEqual(result['request_id'],'')
        self.assertEqual(result['check_code'],'configured_root_not_running')
        self.assertEqual(set(result),{'status','request_id','check_code','sampled_at'})
        self.assertEqual(opened,[self.registry.resolve()])
        self.assertTrue(expected_after_mode_change)
        self.assertEqual({str(path):self._database_dump(path) for path in
                          (self.master,self.registry,target)},expected_after_mode_change)

    def test_wbt_preview_rejects_nonterminal_or_mismatched_retirement_history(self):
        arguments,_,_=self.wbt_target_arguments('wbt:bad-cancellation')
        self._install_real_wbt_authority(arguments)
        self._install_cancelled_fresh_intent(audit_state='cancelling')
        with self.assertRaisesRegex(CoordError,'terminal typed WBT administration'):
            load_wbt_preview_configured_root(self.master,self.plist)

    def test_wbt_preview_rejects_mismatched_cancellation_outcome(self):
        arguments,_,_=self.wbt_target_arguments('wbt:mismatched-cancellation')
        self._install_real_wbt_authority(arguments)
        self._install_cancelled_fresh_intent(outcome='native_closed')
        with self.assertRaisesRegex(CoordError,'terminal typed WBT administration'):
            load_wbt_preview_configured_root(self.master,self.plist)

    def test_wbt_preview_rejects_malformed_or_misbound_creator_authorization(self):
        arguments,_,_=self.wbt_target_arguments('wbt:malformed-cancel-authorization')
        self._install_real_wbt_authority(arguments)
        self._install_cancelled_fresh_intent(authorization='not-json')
        with self.assertRaisesRegex(CoordError,'terminal typed WBT administration'):
            load_wbt_preview_configured_root(self.master,self.plist)

    def test_wbt_preview_fails_closed_when_jq_validator_is_unavailable(self):
        arguments,_,_=self.wbt_target_arguments('wbt:no-jq-validator')
        self._install_real_wbt_authority(arguments)
        self._install_cancelled_fresh_intent()
        with patch('agent_chat.ownership_disposition.subprocess.run',side_effect=FileNotFoundError):
            with self.assertRaisesRegex(CoordError,'terminal typed WBT administration'):
                load_wbt_preview_configured_root(self.master,self.plist)

    def test_wbt_preview_rejects_creator_authorization_bound_to_other_candidate(self):
        arguments,_,_=self.wbt_target_arguments('wbt:misbound-cancel-authorization')
        self._install_real_wbt_authority(arguments)
        request_id='11111111-aaaa-4bbb-8ccc-111111111111'
        candidate='11111111-2222-4333-8444-555555555555'
        authorization=json.dumps(dict(
            instruction_origin='app_po_pending_intent_cancellation_assignment',
            assignment_message_id='message_synthetic-cancellation-assignment',
            request_id=request_id,candidate_thread_id='99999999-2222-4333-8444-555555555555',
            expected_thread='thread-root',
            boundary='cancel only this future app handoff intent and its request gates; preserve native unknown and all candidate/history'),
            sort_keys=True)
        self._install_cancelled_fresh_intent(authorization=authorization)
        with self.assertRaisesRegex(CoordError,'terminal typed WBT administration'):
            load_wbt_preview_configured_root(self.master,self.plist)

    def test_wbt_preview_rejects_weakened_identity_trigger(self):
        arguments,_,_=self.wbt_target_arguments('wbt:weak-cancel-identity-trigger')
        self._install_real_wbt_authority(arguments)
        self._install_cancelled_fresh_intent()
        with contextlib.closing(sqlite3.connect(self.registry)) as registry:
            registry.execute('DROP TRIGGER immutable_root_fresh_pending_cancellation_identity')
            registry.execute('''CREATE TRIGGER immutable_root_fresh_pending_cancellation_identity
                BEFORE UPDATE ON root_fresh_pending_cancellations WHEN 0
                BEGIN SELECT RAISE(ABORT,'root fresh-start pending cancellation audit is immutable'); END''')
            registry.commit()
        with self.assertRaisesRegex(CoordError,'terminal typed WBT administration'):
            load_wbt_preview_configured_root(self.master,self.plist)

    def test_wbt_preview_rejects_weakened_delete_trigger(self):
        arguments,_,_=self.wbt_target_arguments('wbt:weak-cancel-delete-trigger')
        self._install_real_wbt_authority(arguments)
        self._install_cancelled_fresh_intent()
        with contextlib.closing(sqlite3.connect(self.registry)) as registry:
            registry.execute('DROP TRIGGER immutable_root_fresh_pending_cancellation_delete')
            registry.execute('''CREATE TRIGGER immutable_root_fresh_pending_cancellation_delete
                BEFORE DELETE ON root_fresh_pending_cancellations WHEN 0
                BEGIN SELECT RAISE(ABORT,'root fresh-start pending cancellation audit is immutable'); END''')
            registry.commit()
        with self.assertRaisesRegex(CoordError,'terminal typed WBT administration'):
            load_wbt_preview_configured_root(self.master,self.plist)

    def test_wbt_preview_rejects_creator_reason_bound_to_other_request(self):
        arguments,_,_=self.wbt_target_arguments('wbt:misbound-cancel-reason')
        self._install_real_wbt_authority(arguments)
        self._install_cancelled_fresh_intent(reason=('Request 876 handoff intent cancelled after exact retained candidate '
            'not-loaded evidence. Candidate/history and native outcome remain unknown; no native closure, '
            'deletion, or project-stop change is asserted.'))
        with self.assertRaisesRegex(CoordError,'terminal typed WBT administration'):
            load_wbt_preview_configured_root(self.master,self.plist)

    def test_wbt_preview_rejects_mismatched_cancellation_linkage_and_current_thread(self):
        arguments,_,_=self.wbt_target_arguments('wbt:mislinked-cancellation')
        self._install_real_wbt_authority(arguments)
        self._install_cancelled_fresh_intent(audit_request_id='another-request')
        with self.assertRaisesRegex(CoordError,'terminal typed WBT administration'):
            load_wbt_preview_configured_root(self.master,self.plist)

    def test_wbt_preview_rejects_current_thread_drift_after_cancellation(self):
        arguments,_,_=self.wbt_target_arguments('wbt:thread-drift-cancellation')
        self._install_real_wbt_authority(arguments)
        self._install_cancelled_fresh_intent()
        with contextlib.closing(sqlite3.connect(self.registry)) as registry:
            registry.execute("UPDATE root_config SET thread_id='thread-drifted' WHERE singleton=1")
            registry.commit()
        with self.assertRaisesRegex(CoordError,'terminal typed WBT administration'):
            load_wbt_preview_configured_root(self.master,self.plist)

    def test_wbt_preview_rejects_unprotected_or_host_drifted_cancellation(self):
        arguments,_,_=self.wbt_target_arguments('wbt:unprotected-cancellation')
        self._install_real_wbt_authority(arguments)
        self._install_cancelled_fresh_intent()
        with contextlib.closing(sqlite3.connect(self.registry)) as registry:
            registry.execute('DROP TRIGGER immutable_root_fresh_pending_cancellation_delete')
            registry.commit()
        with self.assertRaisesRegex(CoordError,'terminal typed WBT administration'):
            load_wbt_preview_configured_root(self.master,self.plist)

    def test_wbt_preview_rejects_cancellation_from_other_configured_host(self):
        arguments,_,_=self.wbt_target_arguments('wbt:host-drift-cancellation')
        self._install_real_wbt_authority(arguments)
        self._install_cancelled_fresh_intent()
        with contextlib.closing(sqlite3.connect(self.registry)) as registry:
            registry.execute("UPDATE root_config SET host_id='host-other' WHERE singleton=1")
            registry.commit()
        with self.assertRaisesRegex(CoordError,'terminal typed WBT administration'):
            load_wbt_preview_configured_root(self.master,self.plist)

    def test_wbt_preview_rejects_other_pending_intent_alongside_valid_cancellation(self):
        arguments,_,_=self.wbt_target_arguments('wbt:active-with-cancelled')
        self._install_real_wbt_authority(arguments)
        self._install_cancelled_fresh_intent()
        with contextlib.closing(sqlite3.connect(self.registry)) as registry:
            registry.execute("INSERT INTO root_fresh_requests(id,status) VALUES('unrelated-active','pending')")
            registry.commit()
        with self.assertRaisesRegex(CoordError,'terminal typed WBT administration'):
            load_wbt_preview_configured_root(self.master,self.plist)

    def test_wbt_preview_transactional_recheck_rejects_new_pending_request(self):
        arguments,_,_=self.wbt_target_arguments('wbt:transaction-race')
        self._install_real_wbt_authority(arguments)
        self._install_cancelled_fresh_intent()
        artifact=self._wbt_artifact(arguments)
        original=expired_claim_module.wbt_witness_selection
        calls={'count':0}

        def selection_with_new_pending(*args,**kwargs):
            selected=original(*args,**kwargs)
            calls['count']+=1
            if calls['count']==2:
                with contextlib.closing(sqlite3.connect(self.registry)) as registry:
                    registry.execute("INSERT INTO root_fresh_requests(id,status) VALUES('late-active','pending')")
                    registry.commit()
            return selected

        with artifact,self._wbt_authority(),patch(
                'agent_chat.expired_claim_disposition.wbt_witness_selection',
                side_effect=selection_with_new_pending),patch(
                'agent_chat.expired_claim_disposition.snapshot') as snapshot_fn:
            with self.assertRaisesRegex(CoordError,'fresh-start history changed before WBT read'):
                execute_expired_claim(**arguments)
            snapshot_fn.assert_not_called()
        self.assertEqual(calls['count'],2)

    def test_wbt_preview_reports_open_guard_and_queue_without_claiming_eligibility(self):
        arguments, target_coord, target = self.wbt_target_arguments('wbt:open-row')
        artifact=self._wbt_artifact(arguments)
        target_coord.db.execute('''INSERT INTO guarded_runs
            (run_id,resource,reservation_id,session,pid,pgid,started_at,closed_at)
            VALUES(?,?,?,?,?,?,?,NULL)''',
            ('wbt-open-guard', 'wbt:open-row', 'other-rid', 'wbt-owner', 0, None, time.time()))
        target_coord.db.execute('INSERT INTO resource_queue(resource,session,queued_at) VALUES(?,?,?)',
                                ('wbt:open-row', 'queued-session', time.time()))
        target_coord.db.commit()
        before = self._database_dump(target)
        with artifact, self._wbt_authority():
            result = dict(execute_expired_claim(**arguments))
        self.assertEqual(result['status'], 'observed')
        self.assertEqual(result['open_guard_count'], 1)
        self.assertEqual(result['fifo_count'], 1)
        self.assertFalse(result['fifo_is_empty'])
        self.assertEqual(result['eligibility'], 'NOT_DEMONSTRATED')
        self.assertEqual(before, self._database_dump(target))

    def test_wbt_preview_unknown_on_exact_row_or_complete_schema_drift(self):
        arguments, target_coord, target = self.wbt_target_arguments('wbt:drift-row')
        artifact=self._wbt_artifact(arguments)
        wrong = arguments | {'reservation_id':'different-rid'}
        with artifact, self._wbt_authority(), patch('agent_chat.expired_claim_disposition.snapshot') as snapshot_fn:
            result = dict(execute_expired_claim(**wrong))
            snapshot_fn.assert_not_called()
        self.assertEqual(result['status'], 'unknown')
        self.assertEqual(result['reason_code'], 'pinned_artifact_or_exact_tuple_not_proven')
        self.assertEqual(result['eligibility'], 'NOT_DEMONSTRATED')
        self.assertEqual(result['process_closure'], 'UNVERIFIED')
        target_coord.db.execute('DROP TABLE remote_restoration_runs')
        target_coord.db.commit()
        with artifact, self._wbt_authority():
            incomplete = dict(execute_expired_claim(**arguments))
        self.assertEqual(incomplete['status'], 'unknown')
        self.assertEqual(incomplete['reason_code'], 'exact_row_or_complete_snapshot_unavailable')
        self.assertIsNone(target_coord.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_expired_claim_dispositions'").fetchone())

    def test_wbt_preview_rejects_wrong_scope_before_loading_target(self):
        arguments, _, _ = self.wbt_target_arguments('wbt:scope-row')
        wrong_project = arguments | {'project':'some-other-project'}
        with patch('agent_chat.expired_claim_disposition.load_configured_root') as load:
            with self.assertRaisesRegex(CoordError, 'exact configured Wbt project'):
                execute_expired_claim(**wrong_project)
            load.assert_not_called()
        wrong_cohort = arguments | {'cohort_ref':'message-unrelated/attachment-unrelated'}
        with patch('agent_chat.expired_claim_disposition.load_configured_root') as load:
            with self.assertRaisesRegex(CoordError, 'exact configured Wbt project'):
                execute_expired_claim(**wrong_cohort)
            load.assert_not_called()

    def test_wbt_preview_authority_denial_precedes_row_snapshot(self):
        arguments, _, _ = self.wbt_target_arguments('wbt:authority-row')
        with patch('agent_chat.expired_claim_disposition._wbt_pinned_artifact_membership') as membership_fn:
            with patch('agent_chat.expired_claim_disposition.authority', side_effect=CoordError('authority denied')):
                with self.assertRaisesRegex(CoordError, 'authority denied'):
                    execute_expired_claim(**arguments)
            membership_fn.assert_not_called()

    def test_wbt_preview_rechecks_authority_inside_active_stop_read(self):
        arguments, _, _ = self.wbt_target_arguments('wbt:authority-drift')
        artifact=self._wbt_artifact(arguments)
        with artifact, patch(
                'agent_chat.expired_claim_disposition.authority',
                side_effect=[(('wbt-operator-source','bodyhash'),), CoordError('authority changed')]), \
                patch('agent_chat.expired_claim_disposition.snapshot') as snapshot_fn:
            with self.assertRaisesRegex(CoordError,'authority changed'):
                execute_expired_claim(**arguments)
            snapshot_fn.assert_not_called()

    def test_wbt_preview_fails_closed_when_exact_active_stop_drifts_before_snapshot(self):
        arguments,target_coord,target=self.wbt_target_arguments('wbt:active-stop-drift')
        artifact=self._wbt_artifact(arguments)
        race_state={}
        real_recheck=expired_claim_module._recheck_registered_target

        def recheck_then_resume_target(root,registry,project,path):
            result=real_recheck(root,registry,project,path)
            target_coord.db.execute('UPDATE root_team_stops SET active=0,resumed_at=? WHERE id=?',
                                    (time.time(),self.STOP))
            target_coord.db.commit()
            race_state['target_after_race']=self._database_dump(target)
            return result

        with artifact,self._wbt_authority(),patch(
                'agent_chat.expired_claim_disposition._recheck_registered_target',
                side_effect=recheck_then_resume_target):
            with self.assertRaisesRegex(CoordError,'exact active project stop is absent'):
                execute_expired_claim(**arguments)
        self.assertEqual(race_state['target_after_race'],self._database_dump(target))

    def test_wbt_preview_requires_exact_active_enrolled_project_stop(self):
        arguments,target_coord,_=self.wbt_target_arguments('wbt:active-stop-scope')
        artifact=self._wbt_artifact(arguments)
        target_coord.db.execute('UPDATE root_team_stops SET root_session=? WHERE id=?',
                                ('foreign-project-root',self.STOP))
        target_coord.db.commit()
        with artifact,self._wbt_authority(),patch(
                'agent_chat.expired_claim_disposition.snapshot') as snapshot_fn:
            with self.assertRaisesRegex(CoordError,'exact active project stop changed'):
                execute_expired_claim(**arguments)
            snapshot_fn.assert_not_called()

    def test_wbt_preview_rejects_conflicting_active_project_stop(self):
        arguments,target_coord,_=self.wbt_target_arguments('wbt:active-stop-conflict')
        artifact=self._wbt_artifact(arguments)
        target_coord.db.execute('INSERT INTO root_team_stops VALUES(?,?,?,?,?)',
                                ('other-active-stop','root',1,time.time(),None))
        target_coord.db.commit()
        with artifact,self._wbt_authority(),patch(
                'agent_chat.expired_claim_disposition.snapshot') as snapshot_fn:
            with self.assertRaisesRegex(CoordError,'conflicting active stop'):
                execute_expired_claim(**arguments)
            snapshot_fn.assert_not_called()

    def test_wbt_preview_rechecks_configured_master_plist_and_owner_host(self):
        arguments,target_coord,_=self.wbt_target_arguments('wbt:configured-provider')
        artifact=self._wbt_artifact(arguments)
        with artifact,self._wbt_authority():
            wrong_master=arguments|{'master_db':self.master.parent/'wrong-master.sqlite3'}
            with self.assertRaisesRegex(CoordError,'does not match the installed configured server'):
                execute_expired_claim(**wrong_master)
            wrong_plist=arguments|{'service_plist':self.master.parent/'wrong-server.plist'}
            with self.assertRaisesRegex(CoordError,'not the installed source'):
                execute_expired_claim(**wrong_plist)
            with contextlib.closing(sqlite3.connect(self.registry)) as registry:
                registry.execute("UPDATE root_config SET home_session='foreign-root' WHERE singleton=1")
                registry.commit()
            with self.assertRaisesRegex(CoordError,'active stop operation belongs to another Root'):
                execute_expired_claim(**arguments)
        # Restore the canonical fixture for the subsequent host/binding checks.
        with contextlib.closing(sqlite3.connect(self.registry)) as registry:
            registry.execute("UPDATE root_config SET home_session='root' WHERE singleton=1")
            registry.commit()
        with artifact,self._wbt_authority():
            target_coord.db.execute('UPDATE remote_session_hosts SET host_id=? WHERE session_id=?',
                                    ('foreign-host','wbt-owner'))
            target_coord.db.commit()
            wrong_host=dict(execute_expired_claim(**arguments))
        self.assertEqual(wrong_host['status'],'unknown')
        self.assertEqual(wrong_host['reason_code'],'exact_row_or_complete_snapshot_unavailable')
        with artifact,self._wbt_authority():
            target_coord.db.execute('UPDATE remote_session_hosts SET host_id=? WHERE session_id=?',
                                    ('host-root','wbt-owner'))
            target_coord.db.execute('DELETE FROM bridge_bindings WHERE session_id=?',('wbt-owner',))
            target_coord.db.commit()
            missing_binding=dict(execute_expired_claim(**arguments))
        self.assertEqual(missing_binding['status'],'unknown')
        self.assertEqual(missing_binding['reason_code'],'exact_row_or_complete_snapshot_unavailable')

    def test_wbt_preview_rejects_unrelated_current_row_before_snapshot(self):
        arguments, _, _ = self.wbt_target_arguments('wbt:unrelated-current-row')
        rows=[('wbt:other-row','fixture-owner','fixture-rid',arguments['expected_deadline']+1.0),
              ('wbt:another-row','fixture-owner','fixture-rid-2',arguments['expected_deadline']+2.0)]
        artifact=self._wbt_artifact(arguments,rows)
        with artifact, self._wbt_authority(), patch('agent_chat.expired_claim_disposition.snapshot') as snapshot_fn:
            result=dict(execute_expired_claim(**arguments))
            snapshot_fn.assert_not_called()
        self.assertEqual(result['status'],'unknown')
        self.assertEqual(result['reason_code'],'pinned_artifact_or_exact_tuple_not_proven')
        self.assertEqual(result['artifact_byte_identity'],'NOT_CONFIRMED_BY_THIS_READ')

    def test_wbt_preview_rejects_bad_hash_schema_and_duplicate_artifact_rows(self):
        arguments, _, _ = self.wbt_target_arguments('wbt:artifact-gate-row')
        raw_artifact=self._wbt_artifact(arguments)
        with raw_artifact, patch('agent_chat.expired_claim_disposition._WBT_COHORT_ARTIFACT_SHA256','0'*64), \
                self._wbt_authority(), patch('agent_chat.expired_claim_disposition.snapshot') as snapshot_fn:
            bad_hash=dict(execute_expired_claim(**arguments))
            snapshot_fn.assert_not_called()
        self.assertEqual(bad_hash['status'],'unknown')
        missing_field=self._wbt_artifact(arguments,omit_field='current_host')
        with missing_field, self._wbt_authority(), patch('agent_chat.expired_claim_disposition.snapshot') as snapshot_fn:
            bad_schema=dict(execute_expired_claim(**arguments))
            snapshot_fn.assert_not_called()
        self.assertEqual(bad_schema['status'],'unknown')
        selected=(arguments['resource'],arguments['owner_session'],
                  arguments['reservation_id'],arguments['expected_deadline'])
        duplicate=self._wbt_artifact(arguments,[selected,selected])
        with duplicate, self._wbt_authority(), patch('agent_chat.expired_claim_disposition.snapshot') as snapshot_fn:
            duplicated=dict(execute_expired_claim(**arguments))
            snapshot_fn.assert_not_called()
        self.assertEqual(duplicated['status'],'unknown')

    def test_wbt_preview_rejects_missing_symlinked_or_oversized_artifact(self):
        arguments, _, _ = self.wbt_target_arguments('wbt:artifact-file-gate')
        raw_artifact=self._wbt_artifact(arguments)
        artifact_path=self.master.parent/'wbt-cohort-fixture.json'
        link_path=self.master.parent/'wbt-cohort-link.json'
        link_path.symlink_to(artifact_path)
        with raw_artifact, patch('agent_chat.expired_claim_disposition._WBT_COHORT_ARTIFACT_PATH',str(link_path)), \
                self._wbt_authority(), patch('agent_chat.expired_claim_disposition.snapshot') as snapshot_fn:
            symlinked=dict(execute_expired_claim(**arguments))
            snapshot_fn.assert_not_called()
        self.assertEqual(symlinked['status'],'unknown')
        missing_path=self.master.parent/'missing-wbt-artifact.json'
        with patch('agent_chat.expired_claim_disposition._WBT_COHORT_ARTIFACT_PATH',str(missing_path)), \
                self._wbt_authority(), patch('agent_chat.expired_claim_disposition.snapshot') as snapshot_fn:
            missing=dict(execute_expired_claim(**arguments))
            snapshot_fn.assert_not_called()
        self.assertEqual(missing['status'],'unknown')
        oversized=b'x'*257000
        large_artifact=self._wbt_artifact(arguments,raw=oversized)
        with large_artifact, self._wbt_authority(), \
                patch('agent_chat.expired_claim_disposition.snapshot') as snapshot_fn:
            large=dict(execute_expired_claim(**arguments))
            snapshot_fn.assert_not_called()
        self.assertEqual(large['status'],'unknown')
        with patch('agent_chat.expired_claim_disposition.authority', side_effect=CoordError('authority denied')):
            with patch('agent_chat.expired_claim_disposition.snapshot') as snapshot_fn:
                with self.assertRaisesRegex(CoordError, 'authority denied'):
                    execute_expired_claim(**arguments)
                snapshot_fn.assert_not_called()

    def test_wbt_preview_registry_disappearing_before_readonly_open_is_not_recreated(self):
        arguments, _, target = self.wbt_target_arguments('wbt:registry-open-race')
        artifact=self._wbt_artifact(arguments)
        target_before=target.read_bytes()
        registry_path=self.registry
        self.assertTrue(registry_path.is_file())
        real_open=expired_claim_module._wbt_readonly_connection
        race_state={}

        def unlink_registry_then_open(path):
            if path == registry_path:
                race_state['registry_sidecars']=self._sqlite_sidecar_state(registry_path)
                race_state['target_sidecars']=self._sqlite_sidecar_state(target)
                registry_path.unlink()
            return real_open(path)

        with artifact, self._wbt_authority(), patch(
                'agent_chat.expired_claim_disposition._wbt_readonly_connection',
                side_effect=unlink_registry_then_open):
            result=dict(execute_expired_claim(**arguments))
        self.assertEqual(result['status'],'unknown')
        self.assertEqual(result['reason_code'],'configured_registry_unavailable')
        self.assertFalse(registry_path.exists())
        self.assertEqual(target.read_bytes(),target_before)
        self.assertEqual(self._sqlite_sidecar_state(registry_path),race_state['registry_sidecars'])
        self.assertEqual(self._sqlite_sidecar_state(target),race_state['target_sidecars'])

    def test_wbt_preview_target_disappearing_after_registered_recheck_is_not_recreated(self):
        arguments, _, target = self.wbt_target_arguments('wbt:target-open-race')
        artifact=self._wbt_artifact(arguments)
        race_state={}
        self.assertTrue(target.is_file())
        real_recheck=expired_claim_module._recheck_registered_target

        def recheck_then_unlink(root, registry, project, path):
            result=real_recheck(root,registry,project,path)
            race_state['registry_bytes']=self.registry.read_bytes()
            race_state['registry_sidecars']=self._sqlite_sidecar_state(self.registry)
            race_state['target_sidecars']=self._sqlite_sidecar_state(target)
            Path(path).unlink()
            return result

        with artifact, self._wbt_authority(), patch(
                'agent_chat.expired_claim_disposition._recheck_registered_target',
                side_effect=recheck_then_unlink):
            result=dict(execute_expired_claim(**arguments))
        self.assertEqual(result['status'],'unknown')
        self.assertEqual(result['reason_code'],'target_project_store_unavailable')
        self.assertFalse(target.exists())
        self.assertEqual(self.registry.read_bytes(),race_state['registry_bytes'])
        self.assertEqual(self._sqlite_sidecar_state(target),race_state['target_sidecars'])
        self.assertEqual(self._sqlite_sidecar_state(self.registry),race_state['registry_sidecars'])

    def test_exact_selective_resume_is_eligible_but_global_loaders_stay_blocked(self):
        with self.assertRaisesRegex(CoordError, 'stop barrier is active'):
            load_configured_root(self.master, self.plist)
        root = load_configured_root(self.master, self.plist, project_scope='default')
        self.assertEqual(root.home_session, 'root')
        context = self.context()
        self.assertEqual(context[:5], ('resumed-project-stop-v1', self.STOP, 'root', 'default', 'root'))
        self.assertGreater(context[-1], context[-2])

    def test_expired_claim_preview_uses_scoped_resume_but_status_keeps_global_guard(self):
        self.coord.session = 'owner'
        self.coord.register('worker')
        claim = self.coord.request('validation-clone', 5)
        deadline = time.time() - 60
        self.coord.db.execute('UPDATE resources SET deadline=?,token=NULL WHERE name=?',
                              (deadline, claim['resource']))
        self.coord.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?,?,?)',
                              ('owner', 'thread-owner', None, None, time.time()))
        self.coord.db.execute('INSERT INTO remote_session_hosts VALUES(?,?,?,?)',
                              ('owner', 'host-root', time.time(), time.time()))
        arguments = dict(master_db=self.master, service_plist=self.plist, project='default',
            resource='validation-clone', owner_session='owner', reservation_id=claim['reservation_id'],
            expected_deadline=deadline, operator_message_id=['source-1'], action='preview')
        with patch('agent_chat.expired_claim_disposition.authority', return_value=(('source-1','bodyhash'),)):
            result = dict(execute_expired_claim(**arguments))
            self.assertTrue(len(result['preview_digest']) == 64)
            self.assertEqual(result['reservation_id'], claim['reservation_id'])
            request_id = '9f746301-25fc-4889-b435-a7753501e8ba'
            apply_arguments = arguments | {'action':'apply','request_id':request_id,
                'expected_digest':result['preview_digest'],'acknowledge_unverified':True}
            applied = dict(execute_expired_claim(**apply_arguments))
            self.assertEqual(applied['outcome'], 'expired_claim_invalidated_actor_preserved')
            self.assertEqual(applied['process_closure'], 'UNVERIFIED')
            row_after_apply = tuple(self.coord.db.execute(
                'SELECT owner_session,reservation_id,token,deadline FROM resources WHERE name=?',
                ('validation-clone',)).fetchone())
            with self.assertRaisesRegex(CoordError, 'stop barrier is active'):
                execute_expired_claim(**(arguments | {'action':'status','request_id':request_id}))
            replay = dict(execute_expired_claim(**apply_arguments))
            self.assertTrue(replay['replayed'])
            self.assertEqual(replay['outcome'], applied['outcome'])
            self.assertEqual(replay['recorded_at'], applied['recorded_at'])
            self.assertEqual(replay['current_row'], 'NOT_REAPPLIED')
            self.assertEqual(tuple(self.coord.db.execute(
                'SELECT owner_session,reservation_id,token,deadline FROM resources WHERE name=?',
                ('validation-clone',)).fetchone()), row_after_apply)
            self.assertEqual(self.coord.db.execute(
                'SELECT count(*) FROM root_expired_claim_dispositions WHERE request_id=?',
                (request_id,)).fetchone()[0], 1)

    def test_guard_selection_bounds_relevant_rows_not_closed_other_claim_history(self):
        arguments, claim, _ = self.expired_claim_arguments()
        self.coord.db.executemany('''INSERT INTO guarded_runs
            (run_id,resource,reservation_id,session,pid,pgid,started_at,closed_at)
            VALUES(?,?,?,?,?,?,?,?)''',
            ((f'history-{i}', 'validation-clone', f'other-rid-{i}', 'old-owner', 0, None,
              time.time() - 20, time.time() - 10) for i in range(2050)))
        with patch('agent_chat.expired_claim_disposition.authority',
                   return_value=(('source-1', 'bodyhash'),)):
            preview = dict(execute_expired_claim(**arguments))
        self.assertEqual(len(preview['preview_digest']), 64)
        self.assertEqual(self.coord.db.execute(
            "SELECT count(*) FROM guarded_runs WHERE resource='validation-clone'").fetchone()[0], 2050)
        self.assertEqual(self.coord.db.execute(
            'SELECT reservation_id FROM resources WHERE name=?', ('validation-clone',)).fetchone()[0],
            claim['reservation_id'])

    def test_open_foreign_resource_guard_and_exact_claim_uncertainty_are_retained(self):
        arguments, claim, _ = self.expired_claim_arguments()
        now = time.time()
        self.coord.db.execute('''INSERT INTO guarded_runs
            (run_id,resource,reservation_id,session,pid,pgid,started_at,closed_at)
            VALUES(?,?,?,?,?,?,?,NULL)''',
            ('foreign-open', 'validation-clone', 'another-rid', 'other-owner', 0, None, now))
        with patch('agent_chat.expired_claim_disposition.authority',
                   return_value=(('source-1', 'bodyhash'),)):
            with self.assertRaisesRegex(CoordError, 'active guarded work'):
                execute_expired_claim(**arguments)
        self.coord.db.execute('UPDATE guarded_runs SET closed_at=? WHERE run_id=?',
                              (time.time(), 'foreign-open'))
        self.coord.db.execute('''INSERT INTO guarded_runs
            (run_id,resource,reservation_id,session,pid,pgid,started_at,closed_at)
            VALUES(?,?,?,?,?,?,?,NULL)''',
            ('exact-open', 'elsewhere', claim['reservation_id'], 'owner', 0, None, time.time()))
        with patch('agent_chat.expired_claim_disposition.authority',
                   return_value=(('source-1', 'bodyhash'),)):
            with self.assertRaisesRegex(CoordError, 'active guarded work'):
                execute_expired_claim(**arguments)

    def test_guard_lookup_is_indexed_and_remote_producers_advance_generation(self):
        plan = tuple(row[-1] for row in self.coord.db.execute('''EXPLAIN QUERY PLAN
            SELECT run_id,resource,reservation_id,session,pid,pgid,started_at,closed_at
            FROM guarded_runs WHERE (resource=? AND closed_at IS NULL) OR reservation_id=?
            ORDER BY run_id LIMIT 2049''', ('validation-clone', 'rid')))
        self.assertTrue(any('guarded_runs_resource_closed_run' in item for item in plan), plan)
        self.assertTrue(any('guarded_runs_reservation_run' in item for item in plan), plan)
        install_remote_schema(self.coord)
        before = guard_generation(self.coord.db)
        self.coord.db.execute('INSERT INTO remote_guarded_runs VALUES(?,?,?,?,?,?,?,?,?)',
            ('remote-run', 'validation-clone', 'foreign-rid', 'other-owner', 'foreign-host',
             0, time.time(), None, None))
        inserted = guard_generation(self.coord.db)
        self.coord.db.execute('INSERT INTO remote_guard_batches VALUES(?,?)', ('remote-run', 'batch'))
        associated = guard_generation(self.coord.db)
        self.coord.db.execute('UPDATE remote_guarded_runs SET closed_at=? WHERE run_id=?',
                              (time.time(), 'remote-run'))
        closed = guard_generation(self.coord.db)
        self.assertGreater(inserted, before)
        self.assertGreater(associated, inserted)
        self.assertGreater(closed, associated)

    def test_decoy_trigger_with_expected_name_and_body_fails_closed(self):
        self.coord.db.execute('DROP TRIGGER root_guard_generation_guarded_runs_ai')
        self.coord.db.execute('CREATE TABLE guard_hook_decoy (id TEXT PRIMARY KEY)')
        self.coord.db.execute('''CREATE TRIGGER root_guard_generation_guarded_runs_ai
            AFTER INSERT ON guard_hook_decoy BEGIN
              UPDATE root_guard_generation SET generation=generation+1 WHERE singleton=1;
              SELECT CASE WHEN changes()!=1 THEN RAISE(ABORT,'guard generation unavailable') END;
            END''')
        with self.assertRaisesRegex(RuntimeError, 'hook definition unsupported'):
            guard_generation(self.coord.db)
        before = self.coord.db.execute(
            'SELECT generation FROM root_guard_generation WHERE singleton=1').fetchone()[0]
        self.coord.db.execute('''INSERT INTO guarded_runs
            (run_id,resource,reservation_id,session,pid,pgid,started_at,closed_at)
            VALUES(?,?,?,?,?,?,?,?)''',
            ('untracked', 'validation-clone', 'rid', 'owner', 0, None,
             time.time(), time.time()))
        after = self.coord.db.execute(
            'SELECT generation FROM root_guard_generation WHERE singleton=1').fetchone()[0]
        self.assertEqual(after, before)

    def test_exact_history_overflow_fails_closed_without_digest_or_effect(self):
        arguments, claim, _ = self.expired_claim_arguments()
        self.coord.db.executemany('''INSERT INTO guarded_runs
            (run_id,resource,reservation_id,session,pid,pgid,started_at,closed_at)
            VALUES(?,?,?,?,?,?,?,?)''',
            ((f'exact-history-{i}', 'elsewhere', claim['reservation_id'], 'owner', 0, None,
              time.time() - 20, time.time() - 10) for i in range(2049)))
        before = tuple(self.coord.db.execute(
            'SELECT owner_session,reservation_id,token,deadline FROM resources WHERE name=?',
            ('validation-clone',)).fetchone())
        with patch('agent_chat.expired_claim_disposition.authority',
                   return_value=(('source-1', 'bodyhash'),)):
            with self.assertRaisesRegex(CoordError, 'row-bound guard metadata exceeds finite scope'):
                execute_expired_claim(**arguments)
        self.assertEqual(tuple(self.coord.db.execute(
            'SELECT owner_session,reservation_id,token,deadline FROM resources WHERE name=?',
            ('validation-clone',)).fetchone()), before)
        self.assertEqual(self.coord.db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='root_expired_claim_dispositions'").fetchone()[0], 0)

    def test_guard_insert_then_close_after_preview_invalidates_apply_and_preserves_ledgers(self):
        arguments, _, _ = self.expired_claim_arguments()
        with patch('agent_chat.expired_claim_disposition.authority',
                   return_value=(('source-1', 'bodyhash'),)):
            preview = dict(execute_expired_claim(**arguments))
            now = time.time()
            self.coord.db.execute('''INSERT INTO guarded_runs
                (run_id,resource,reservation_id,session,pid,pgid,started_at,closed_at)
                VALUES(?,?,?,?,?,?,?,NULL)''',
                ('transient-open', 'validation-clone', 'foreign-rid', 'foreign-owner', 0, None, now))
            self.coord.db.execute('UPDATE guarded_runs SET closed_at=? WHERE run_id=?',
                                  (time.time(), 'transient-open'))
            apply_args = arguments | {'action':'apply',
                'request_id':'ab5c4b79-5d2a-4fcb-8277-9423c7a7aa31',
                'expected_digest':preview['preview_digest'],'acknowledge_unverified':True}
            with self.assertRaisesRegex(CoordError, 'generation changed'):
                execute_expired_claim(**apply_args)
        self.assertEqual(self.coord.db.execute(
            'SELECT count(*) FROM guarded_runs WHERE run_id=?', ('transient-open',)).fetchone()[0], 1)
        self.assertEqual(self.coord.db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='root_expired_claim_dispositions'").fetchone()[0], 0)
        self.assertEqual(self.coord.db.execute(
            'SELECT count(*) FROM resource_queue WHERE resource=?', ('validation-clone',)).fetchone()[0], 0)
        self.assertEqual(self.coord.db.execute(
            'SELECT active FROM root_team_stops WHERE id=?', (self.STOP,)).fetchone()[0], 0)

    def test_scoped_resume_is_rechecked_inside_locked_registry_and_target(self):
        root = load_configured_root(self.master, self.plist, project_scope='default')
        path = target_path(root, 'default')
        with contextlib.closing(sqlite3.connect(root.registry, isolation_level=None)) as registry:
            registry.row_factory = sqlite3.Row
            registry.execute('BEGIN IMMEDIATE')
            with contextlib.closing(sqlite3.connect(path, isolation_level=None)) as target:
                target.row_factory = sqlite3.Row
                target.execute('BEGIN')
                context = _recheck_root_barriers(registry, root=root, project='default',
                                                 target_path=path, target_db=target)
                self.assertEqual(context, self.context())
                target.rollback()
            registry.rollback()
        self.coord.db.execute('UPDATE root_team_stops SET active=1,resumed_at=NULL WHERE id=?', (self.STOP,))
        with contextlib.closing(sqlite3.connect(root.registry, isolation_level=None)) as registry:
            registry.row_factory = sqlite3.Row
            registry.execute('BEGIN IMMEDIATE')
            with contextlib.closing(sqlite3.connect(path, isolation_level=None)) as target:
                target.row_factory = sqlite3.Row
                target.execute('BEGIN')
                with self.assertRaisesRegex(CoordError, 'target project stop barrier is active'):
                    _recheck_root_barriers(registry, root=root, project='default',
                                           target_path=path, target_db=target)
                target.rollback()
            registry.rollback()

    def test_stopped_missing_foreign_and_changed_project_barriers_fail_closed(self):
        path = target_path(self.root, 'default')
        self.coord.db.execute('UPDATE root_team_stops SET active=1 WHERE id=?', (self.STOP,))
        with self.assertRaisesRegex(CoordError, 'target project stop barrier is active'):
            self.context()
        self.coord.db.execute('UPDATE root_team_stops SET active=0 WHERE id=?', (self.STOP,))
        self.coord.db.execute('DELETE FROM root_team_stops WHERE id=?', (self.STOP,))
        with self.assertRaisesRegex(CoordError, 'exact scoped project resume is unavailable'):
            self.context()
        now = time.time()
        self.coord.db.execute('INSERT INTO root_team_stops VALUES(?,?,?,?,?)',
                              (self.STOP, 'foreign-root', 0, now - 10, now - 5))
        with self.assertRaisesRegex(CoordError, 'exact scoped project resume is unavailable'):
            self.context()
        self.coord.db.execute('UPDATE root_team_stops SET root_session=? WHERE id=?', ('root', self.STOP))
        with contextlib.closing(sqlite3.connect(self.registry)) as db:
            db.execute("UPDATE root_projects SET session_id='changed-root' WHERE project_id='default'")
            db.commit()
        with self.assertRaisesRegex(CoordError, 'project Root enrollment changed'):
            self.context()

    def test_wrong_scope_foreign_root_conflict_and_local_orphan_stop_fail(self):
        path = target_path(self.root, 'default')
        with _read_existing(self.registry) as db:
            with self.assertRaisesRegex(CoordError, 'not in the exact original stop scope'):
                _project_resumed_stop_context(self.root, db, 'other-project',
                    self.master.parent / (self.master.name + '.projects') / 'other-project' / 'state.sqlite3',
                    self.coord.db)
        with contextlib.closing(sqlite3.connect(self.registry)) as db:
            db.execute("UPDATE root_team_operations SET root_session='foreign-root' WHERE id=?", (self.STOP,))
            db.commit()
        with self.assertRaisesRegex(CoordError, 'belongs to another Root'):
            self.context()
        with contextlib.closing(sqlite3.connect(self.registry)) as db:
            db.execute("UPDATE root_team_operations SET root_session='root' WHERE id=?", (self.STOP,))
            db.execute('INSERT INTO root_team_operations VALUES(?,?,?,NULL)',
                       ('second-stop', 'root', time.time()))
            db.commit()
        with self.assertRaisesRegex(CoordError, 'conflicting active stop operations'):
            self.context()
        with contextlib.closing(sqlite3.connect(self.registry)) as db:
            db.execute('DELETE FROM root_team_operations WHERE id=?', ('second-stop',))
            db.commit()
        self.coord.db.execute('INSERT INTO root_team_stops VALUES(?,?,?,?,?)',
                              ('orphan-stop', 'root', 1, time.time(), None))
        with self.assertRaisesRegex(CoordError, 'target project stop barrier is active'):
            self.context()

    def test_global_resume_or_no_active_target_stop_keeps_ordinary_path(self):
        with contextlib.closing(sqlite3.connect(self.registry)) as db:
            db.execute('UPDATE root_team_operations SET resumed_at=? WHERE id=?', (time.time(), self.STOP))
            db.commit()
        self.assertIsNone(self.context())
        self.assertEqual(load_configured_root(self.master, self.plist).home_session, 'root')

    def wbt_abandon_arguments(self, resource='file:.operator-message-ui-0065/tests/operator_message_ui_browser.cjs'):
        arguments,target_coord,target=self.wbt_target_arguments(resource)
        arguments.update(action='wbt-abandon-prepare',request_id='33333333-aaaa-4bbb-8ccc-333333333333',
            expires_at=time.time()+300,acknowledge_unverified=True,
            operator_message_id=[expired_claim_module._WBT_ABANDON_AUTHORITY_MESSAGE])
        hqg_default=resource=='file:.operator-message-ui-0065/tests/operator_message_ui_browser.cjs'
        if hqg_default:
            deadline=self._wbt_hqg_file_class_candidates()[0]['original_deadline']
        else:
            deadline=time.time()-30
        target_coord.db.execute('UPDATE resources SET deadline=?,stale=1 WHERE name=?',(deadline,resource))
        target_coord.db.commit()
        arguments['expected_deadline']=deadline
        self._install_real_wbt_authority(arguments)
        self._install_cancelled_fresh_intent()
        if resource.startswith('file:.finite-batch-64-0080/'):
            candidates=self._wbt_group_candidates()
            chosen=next(row for row in candidates if row['resource']==resource)
            target_coord.session=chosen['owner_session']
            target_coord.register('reviewer_group')
            target_coord.db.execute('UPDATE resources SET owner_session=?,reservation_id=?,deadline=?,stale=1 WHERE name=?',
                (chosen['owner_session'],chosen['reservation_id'],chosen['original_deadline'],resource))
            target_coord.db.execute('DELETE FROM bridge_bindings WHERE session_id=?',('wbt-owner',))
            target_coord.db.execute('DELETE FROM remote_session_hosts WHERE session_id=?',('wbt-owner',))
            target_coord.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?,?,?)',
                (chosen['owner_session'],'thread-wbt-owner',None,None,time.time()))
            target_coord.db.execute('INSERT INTO remote_session_hosts VALUES(?,?,?,?)',
                (chosen['owner_session'],'host-root',time.time(),time.time()))
            target_coord.db.commit()
            arguments['owner_session']=chosen['owner_session']
            arguments['reservation_id']=chosen['reservation_id']
            arguments['expected_deadline']=chosen['original_deadline']
            rows=[('fixture:source-row-'+str(index),'fixture-owner-'+str(index),
                   'fixture-rid-'+str(index),chosen['original_deadline']-10+index)
                  for index in range(6)]+[
                  (row['resource'],row['owner_session'],row['reservation_id'],row['original_deadline'])
                  for row in candidates]
            artifact=self._wbt_group_artifact_context(arguments,candidates,rows=rows)
        elif resource.startswith('file:.finite-batch-64-0081/'):
            candidates=self._wbt_next_group_candidates()
            chosen=next(row for row in candidates if row['resource']==resource)
            target_coord.session=chosen['owner_session']
            target_coord.register('reviewer_group')
            target_coord.db.execute('UPDATE resources SET owner_session=?,reservation_id=?,deadline=?,stale=1 WHERE name=?',
                (chosen['owner_session'],chosen['reservation_id'],chosen['original_deadline'],resource))
            target_coord.db.execute('DELETE FROM bridge_bindings WHERE session_id=?',('wbt-owner',))
            target_coord.db.execute('DELETE FROM remote_session_hosts WHERE session_id=?',('wbt-owner',))
            target_coord.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?,?,?)',
                (chosen['owner_session'],'thread-wbt-owner',None,None,time.time()))
            target_coord.db.execute('INSERT INTO remote_session_hosts VALUES(?,?,?,?)',
                (chosen['owner_session'],'host-root',time.time(),time.time()))
            target_coord.db.commit()
            arguments['owner_session']=chosen['owner_session']
            arguments['reservation_id']=chosen['reservation_id']
            arguments['expected_deadline']=chosen['original_deadline']
            artifact=self._wbt_next_group_artifact_context(arguments,candidates)
        elif resource.startswith('file:.lease-maintenance-0061/'):
            candidates=self._wbt_file_class_candidates()
            chosen=next(row for row in candidates if row['resource']==resource)
            target_coord.session=chosen['owner_session']
            target_coord.register('reviewer_group')
            target_coord.db.execute('UPDATE resources SET owner_session=?,reservation_id=?,deadline=?,stale=1 WHERE name=?',
                (chosen['owner_session'],chosen['reservation_id'],chosen['original_deadline'],resource))
            target_coord.db.execute('DELETE FROM bridge_bindings WHERE session_id=?',('wbt-owner',))
            target_coord.db.execute('DELETE FROM remote_session_hosts WHERE session_id=?',('wbt-owner',))
            target_coord.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?,?,?)',
                (chosen['owner_session'],'thread-wbt-owner',None,None,time.time()))
            target_coord.db.execute('INSERT INTO remote_session_hosts VALUES(?,?,?,?)',
                (chosen['owner_session'],'host-root',time.time(),time.time()))
            target_coord.db.commit()
            arguments['owner_session']=chosen['owner_session']
            arguments['reservation_id']=chosen['reservation_id']
            arguments['expected_deadline']=chosen['original_deadline']
            artifact=self._wbt_file_class_context(arguments,candidates)
        elif resource in {row['resource'] for row in self._wbt_next_file_class_candidates()}:
            candidates=self._wbt_next_file_class_candidates()
            chosen=next(row for row in candidates if row['resource']==resource)
            target_coord.session=chosen['owner_session']
            target_coord.register('reviewer_file_class_next')
            target_coord.db.execute('UPDATE resources SET owner_session=?,reservation_id=?,deadline=?,stale=1 WHERE name=?',
                (chosen['owner_session'],chosen['reservation_id'],chosen['original_deadline'],resource))
            target_coord.db.execute('DELETE FROM bridge_bindings WHERE session_id=?',('wbt-owner',))
            target_coord.db.execute('DELETE FROM remote_session_hosts WHERE session_id=?',('wbt-owner',))
            target_coord.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?,?,?)',
                (chosen['owner_session'],'thread-wbt-owner',None,None,time.time()))
            target_coord.db.execute('INSERT INTO remote_session_hosts VALUES(?,?,?,?)',
                (chosen['owner_session'],'host-root',time.time(),time.time()))
            target_coord.db.commit()
            arguments['owner_session']=chosen['owner_session']
            arguments['reservation_id']=chosen['reservation_id']
            arguments['expected_deadline']=chosen['original_deadline']
            artifact=self._wbt_next_file_class_context(arguments,candidates)
        elif resource in {row['resource'] for row in self._wbt_gg1_file_class_candidates()}:
            candidates=self._wbt_gg1_file_class_candidates()
            chosen=next(row for row in candidates if row['resource']==resource)
            target_coord.session=chosen['owner_session']
            target_coord.register('reviewer_file_class_gg1')
            target_coord.db.execute('UPDATE resources SET owner_session=?,reservation_id=?,deadline=?,stale=1 WHERE name=?',
                (chosen['owner_session'],chosen['reservation_id'],chosen['original_deadline'],resource))
            target_coord.db.execute('DELETE FROM bridge_bindings WHERE session_id=?',('wbt-owner',))
            target_coord.db.execute('DELETE FROM remote_session_hosts WHERE session_id=?',('wbt-owner',))
            target_coord.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?,?,?)',
                (chosen['owner_session'],'thread-wbt-owner',None,None,time.time()))
            target_coord.db.execute('INSERT INTO remote_session_hosts VALUES(?,?,?,?)',
                (chosen['owner_session'],'host-root',time.time(),time.time()))
            target_coord.db.commit()
            arguments['owner_session']=chosen['owner_session']
            arguments['reservation_id']=chosen['reservation_id']
            arguments['expected_deadline']=chosen['original_deadline']
            artifact=self._wbt_gg1_file_class_context(arguments,candidates)
        elif resource in {row['resource'] for row in self._wbt_hgide_file_class_candidates()}:
            candidates=self._wbt_hgide_file_class_candidates()
            chosen=next(row for row in candidates if row['resource']==resource)
            target_coord.session=chosen['owner_session']
            target_coord.register('reviewer_file_class_hgide')
            target_coord.db.execute('UPDATE resources SET owner_session=?,reservation_id=?,deadline=?,stale=1 WHERE name=?',
                (chosen['owner_session'],chosen['reservation_id'],chosen['original_deadline'],resource))
            target_coord.db.execute('DELETE FROM bridge_bindings WHERE session_id=?',('wbt-owner',))
            target_coord.db.execute('DELETE FROM remote_session_hosts WHERE session_id=?',('wbt-owner',))
            target_coord.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?,?,?)',
                (chosen['owner_session'],'thread-wbt-owner',None,None,time.time()))
            target_coord.db.execute('INSERT INTO remote_session_hosts VALUES(?,?,?,?)',
                (chosen['owner_session'],'host-root',time.time(),time.time()))
            target_coord.db.commit()
            arguments['owner_session']=chosen['owner_session']
            arguments['reservation_id']=chosen['reservation_id']
            arguments['expected_deadline']=chosen['original_deadline']
            artifact=self._wbt_hgide_file_class_context(arguments,candidates)
        elif resource in {row['resource'] for row in self._wbt_6jc1_file_class_candidates()}:
            candidates=self._wbt_6jc1_file_class_candidates()
            chosen=next(row for row in candidates if row['resource']==resource)
            target_coord.session=chosen['owner_session']
            target_coord.register('reviewer_file_class_6jc1')
            target_coord.db.execute('UPDATE resources SET owner_session=?,reservation_id=?,deadline=?,stale=1 WHERE name=?',
                (chosen['owner_session'],chosen['reservation_id'],chosen['original_deadline'],resource))
            target_coord.db.execute('DELETE FROM bridge_bindings WHERE session_id=?',('wbt-owner',))
            target_coord.db.execute('DELETE FROM remote_session_hosts WHERE session_id=?',('wbt-owner',))
            target_coord.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?,?,?)',
                (chosen['owner_session'],'thread-wbt-owner',None,None,time.time()))
            target_coord.db.execute('INSERT INTO remote_session_hosts VALUES(?,?,?,?)',
                (chosen['owner_session'],'host-root',time.time(),time.time()))
            target_coord.db.commit()
            arguments['owner_session']=chosen['owner_session']
            arguments['reservation_id']=chosen['reservation_id']
            arguments['expected_deadline']=chosen['original_deadline']
            artifact=self._wbt_6jc1_file_class_context(arguments,candidates)
        elif resource in {row['resource'] for row in self._wbt_hqg_file_class_candidates()}:
            candidates=self._wbt_hqg_file_class_candidates()
            chosen=next(row for row in candidates if row['resource']==resource)
            target_coord.session=chosen['owner_session']
            target_coord.register('reviewer_file_class_hqg')
            target_coord.db.execute('UPDATE resources SET owner_session=?,reservation_id=?,deadline=?,stale=1 WHERE name=?',
                (chosen['owner_session'],chosen['reservation_id'],chosen['original_deadline'],resource))
            target_coord.db.execute('DELETE FROM bridge_bindings WHERE session_id=?',('wbt-owner',))
            target_coord.db.execute('DELETE FROM remote_session_hosts WHERE session_id=?',('wbt-owner',))
            target_coord.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?,?,?)',
                (chosen['owner_session'],'thread-wbt-owner',None,None,time.time()))
            target_coord.db.execute('INSERT INTO remote_session_hosts VALUES(?,?,?,?)',
                (chosen['owner_session'],'host-root',time.time(),time.time()))
            target_coord.db.commit()
            arguments['owner_session']=chosen['owner_session']
            arguments['reservation_id']=chosen['reservation_id']
            arguments['expected_deadline']=chosen['original_deadline']
            artifact=self._wbt_hqg_file_class_context(arguments,candidates)
        else:
            rows=[('fixture:before','fixture-owner-1','fixture-rid-1',deadline-2),
                  ('fixture:before-2','fixture-owner-2','fixture-rid-2',deadline-1),
                  (resource,arguments['owner_session'],arguments['reservation_id'],deadline)]
            artifact=self._wbt_artifact(arguments,rows=rows)
        return arguments,target_coord,target,artifact

    @staticmethod
    def _passive_not_loaded_sample(thread_id,endpoint,*,strict):
        now=time.time()
        return {'thread_id':thread_id,'state':'notLoaded','turn_id':None,'queue_count':0,
                'direct_input_available':False,'started_at':now,'captured_at':now,'elapsed_ms':1}

    def _wbt_abandon_call(self,arguments,artifact,**extra_patches):
        with artifact,patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample',
                      side_effect=self._passive_not_loaded_sample),\
                contextlib.ExitStack() as stack:
            for target_name,patcher in extra_patches.items():
                stack.enter_context(patch(target_name,patcher))
            return dict(execute_expired_claim(**arguments))

    def test_wbt_abandon_prepare_reconcile_apply_is_one_exact_audited_lifecycle(self):
        arguments,target_coord,target,artifact=self.wbt_abandon_arguments()
        before_owner=target_coord.db.execute(
            'SELECT owner_session,reservation_id,deadline,stale FROM resources WHERE name=?',
            (arguments['resource'],)).fetchone()
        sample_calls={'value':0}
        def observing_sample(thread_id,endpoint,*,strict):
            # Read through an independent connection: accepted intent and its
            # first journal event must already be durable before observation.
            with contextlib.closing(sqlite3.connect(target)) as observer:
                row=observer.execute('SELECT state FROM root_wbt_file_abandon_intents WHERE request_id=?',
                                     (arguments['request_id'],)).fetchone()
                events=observer.execute('SELECT state FROM root_wbt_file_abandon_events WHERE request_id=? ORDER BY seq',
                                        (arguments['request_id'],)).fetchall()
            if sample_calls['value']==0:
                self.assertEqual(row[0],'accepted')
                self.assertEqual(tuple(event[0] for event in events),('accepted',))
            else:
                self.assertEqual(row[0],'applying')
            sample_calls['value']+=1
            return self._passive_not_loaded_sample(thread_id,endpoint,strict=strict)
        with artifact,patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample',side_effect=observing_sample) as native_sample:
            prepared=dict(execute_expired_claim(**arguments))
            replay=dict(execute_expired_claim(**arguments))
            self.assertEqual(native_sample.call_count,1)
            apply_args=arguments|{'action':'wbt-abandon-apply','expected_digest':prepared['prepared_digest']}
            applied=dict(execute_expired_claim(**apply_args))
            replayed=dict(execute_expired_claim(**apply_args))
            self.assertEqual(native_sample.call_count,2)
        self.assertEqual(prepared['status'],'prepared')
        self.assertEqual(replay['status'],'prepared')
        self.assertTrue(replay['replayed'])
        self.assertEqual(applied['status'],'applied')
        self.assertEqual(replayed['status'],'applied')
        self.assertTrue(replayed['replayed'])
        self.assertEqual(applied['process_closure'],'UNVERIFIED')
        self.assertEqual(applied['eligibility'],'NOT_DEMONSTRATED')
        current=target_coord.db.execute(
            'SELECT owner_session,reservation_id,deadline,stale FROM resources WHERE name=?',
            (arguments['resource'],)).fetchone()
        self.assertIsNone(current['owner_session']); self.assertIsNone(current['reservation_id'])
        self.assertIsNone(current['deadline']); self.assertEqual(current['stale'],0)
        audit=target_coord.db.execute(
            'SELECT outcome,root_session,root_host FROM root_expired_claim_dispositions WHERE request_id=?',
            (arguments['request_id'],)).fetchone()
        self.assertEqual(tuple(audit),('wbt_file_claim_abandoned_native_unverified','root','host-root'))
        intent=target_coord.db.execute(
            'SELECT state,payload_digest,created_at,expires_at FROM root_wbt_file_abandon_intents WHERE request_id=?',
            (arguments['request_id'],)).fetchone()
        self.assertEqual(intent['state'],'applied')
        self.assertGreater(intent['expires_at'],intent['created_at'])
        events=tuple(row['state'] for row in target_coord.db.execute(
            'SELECT state FROM root_wbt_file_abandon_events WHERE request_id=? ORDER BY seq',
            (arguments['request_id'],)))
        self.assertEqual(events,('accepted','prepared','applying','applied'))
        self.assertEqual(before_owner['owner_session'],arguments['owner_session'])
        self.assertEqual(target_coord.db.execute('SELECT active FROM root_team_stops WHERE id=?',
            (self.STOP,)).fetchone()[0],1)

    def _assert_wbt_group_candidate_lifecycle(self, original_index):
        manifest_path=Path(expired_claim_module._WBT_GROUP_MANIFEST_PATH)
        manifest_bytes=manifest_path.read_bytes()
        self.assertEqual(len(manifest_bytes),expired_claim_module._WBT_GROUP_MANIFEST_SIZE)
        self.assertEqual(hashlib.sha256(manifest_bytes).hexdigest(),
                         expired_claim_module._WBT_GROUP_MANIFEST_SHA256)
        candidates=self._wbt_group_candidates()
        chosen=next(row for row in candidates if row['original_index']==original_index)
        arguments,target_coord,target,artifact=self.wbt_abandon_arguments(chosen['resource'])
        with artifact:
            group_manifest_hash=expired_claim_module._WBT_GROUP_MANIFEST_SHA256
            with patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample',
                      side_effect=self._passive_not_loaded_sample) as native_sample:
                selected=(expired_claim_module._WBT_WITNESS_VERSION,'default','root','host-root',
                    arguments['project'],arguments['resource'],arguments['owner_session'],
                    arguments['reservation_id'],arguments['expected_deadline'],_WBT_COHORT_REF)
                self.assertEqual(expired_claim_module._wbt_group_manifest_index(selected),original_index)
                self.assertEqual(expired_claim_module._wbt_pinned_artifact_membership(
                    selected,row_index=original_index),'member')
                prepared=dict(execute_expired_claim(**arguments))
                replay=dict(execute_expired_claim(**arguments))
                apply_args=arguments|{'action':'wbt-abandon-apply',
                    'expected_digest':prepared['prepared_digest']}
                applied=dict(execute_expired_claim(**apply_args))
                applied_replay=dict(execute_expired_claim(**apply_args))
                self.assertEqual(native_sample.call_count,2)
        self.assertEqual((prepared['status'],prepared['replayed']),('prepared',False))
        self.assertEqual((replay['status'],replay['replayed']),('prepared',True))
        self.assertEqual((applied['status'],applied['replayed']),('applied',False))
        self.assertEqual((applied_replay['status'],applied_replay['replayed']),('applied',True))
        self.assertEqual(applied['process_closure'],'UNVERIFIED')
        self.assertEqual(applied['eligibility'],'NOT_DEMONSTRATED')
        row=target_coord.db.execute(
            'SELECT owner_session,reservation_id,deadline,stale FROM resources WHERE name=?',
            (chosen['resource'],)).fetchone()
        self.assertEqual(tuple(row),(None,None,None,0))
        intent=target_coord.db.execute(
            'SELECT payload,state FROM root_wbt_file_abandon_intents WHERE request_id=?',
            (arguments['request_id'],)).fetchone()
        self.assertTrue(intent['payload'].startswith("('wbt-file-abandon-v3', "))
        self.assertIn(group_manifest_hash,intent['payload'])
        self.assertIn(str(original_index),intent['payload'])
        self.assertEqual(intent['state'],'applied')
        self.assertEqual(target_coord.db.execute(
            'SELECT count(*) FROM root_expired_claim_dispositions WHERE request_id=?',
            (arguments['request_id'],)).fetchone()[0],1)
        events=tuple(row[0] for row in target_coord.db.execute(
            'SELECT state FROM root_wbt_file_abandon_events WHERE request_id=? ORDER BY seq',
            (arguments['request_id'],)))
        self.assertEqual(events,('accepted','prepared','applying','applied'))
        self.assertEqual(target_coord.db.execute('SELECT active FROM root_team_stops WHERE id=?',
            (self.STOP,)).fetchone()[0],1)
        self.assertTrue(Path(target).exists())

    def _wbt_file_class_candidates(self):
        return [
            {'original_index':16,'resource':'file:.lease-maintenance-0061/docs/reference/agent-operations.md','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_esGvHNpCaboLzLiqSpwPasA9s_tzVKw5','original_deadline':1791594613.139986},
            {'original_index':17,'resource':'file:.lease-maintenance-0061/src/agent_chat/cli.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_LBqTQYU7gtkMXxmg9Y2Lny2gsaCGTMor','original_deadline':1791594602.551452},
            {'original_index':18,'resource':'file:.lease-maintenance-0061/src/agent_chat/core.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_89vBJvKmG-e4oH4Ur1ZDg1xaA_AQdlby','original_deadline':1791594595.023468},
            {'original_index':19,'resource':'file:.lease-maintenance-0061/src/agent_chat/remote_service.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_-hBocnG9lbaXQSWtleWPkyBihf7yAZbD','original_deadline':1791594598.853732},
            {'original_index':20,'resource':'file:.lease-maintenance-0061/tests/test_core.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_Ta1kWwDeAEQp-h5iQrzSHeiFltJR4DxY','original_deadline':1791594605.92199},
            {'original_index':21,'resource':'file:.lease-maintenance-0061/tests/test_remote_run.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_lkunghH-Mj-indAg_B4sqOUBYJaNyw_u','original_deadline':1791594609.379068},
            {'original_index':22,'resource':'file:.managed-turn-error-0086/src/agent_chat/native_history.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_OPw9W10WNgO-mksIQd5DLwzzWBpn2PbJ','original_deadline':1792046786.6493351},
            {'original_index':23,'resource':'file:.managed-turn-error-0086/src/agent_chat/supervisor_worker.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_jpjwcmeAOUP9MlmxiHQk30QpyFKkpC0H','original_deadline':1792046786.771927},
        ]

    def _wbt_file_class_arguments(self, original_index):
        candidates=self._wbt_file_class_candidates()
        chosen=next(row for row in candidates if row['original_index']==original_index)
        with patch('agent_chat.root_lifecycle.stopped',return_value=False):
            arguments,target_coord,target,_=self.wbt_abandon_arguments('wbt:exact-row')
        target_coord.session=chosen['owner_session']
        target_coord.register('reviewer_file_class')
        target_coord.db.execute('UPDATE resources SET name=?,owner_session=?,reservation_id=?,deadline=?,stale=1 WHERE name=?',
            (chosen['resource'],chosen['owner_session'],chosen['reservation_id'],chosen['original_deadline'],'wbt:exact-row'))
        target_coord.db.execute('DELETE FROM bridge_bindings WHERE session_id=?',('wbt-owner',))
        target_coord.db.execute('DELETE FROM remote_session_hosts WHERE session_id=?',('wbt-owner',))
        target_coord.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?,?,?)',
            (chosen['owner_session'],'thread-wbt-owner',None,None,time.time()))
        target_coord.db.execute('INSERT INTO remote_session_hosts VALUES(?,?,?,?)',
            (chosen['owner_session'],'host-root',time.time(),time.time()))
        target_coord.db.commit()
        arguments.update(resource=chosen['resource'],owner_session=chosen['owner_session'],reservation_id=chosen['reservation_id'],
                         expected_deadline=chosen['original_deadline'])
        return arguments,target_coord,target,chosen,candidates

    def _wbt_next_file_class_candidates(self):
        return [
            {'original_index':24,'resource':'file:.managed-turn-error-0086/tests/test_native_history.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_bxLykzGOxjWOY0TBm9E5Jcr5KlxkwHWV','original_deadline':1792046786.891563},
            {'original_index':25,'resource':'file:.message-consumption-0055-fixture/tests/test_ownership_disposition.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_N9WD9xRn7fdi9Hk-ZJSLAMTfvljhhiAS','original_deadline':1791564140.574848},
            {'original_index':26,'resource':'file:.message-consumption-0055/docs/reference/agent-operations.md','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_DVs04kzFt2S3606O5JAapn3j9HvNFt__','original_deadline':1791554847.757059},
            {'original_index':27,'resource':'file:.message-consumption-0055/src/agent_chat/bridge_state.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_5KjguNuHATIJ177iSYSV2v_202g1vwYu','original_deadline':1791554827.3029351},
            {'original_index':28,'resource':'file:.message-consumption-0055/src/agent_chat/core.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_gLMcbsX5BELuBmhkS2Sa0G6A2RPrygdD','original_deadline':1791554824.630796},
            {'original_index':29,'resource':'file:.message-consumption-0055/src/agent_chat/supervisor.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_AurTYdKPEURVEEBaZRjq3rd77lrcO1XM','original_deadline':1791554831.032682},
            {'original_index':30,'resource':'file:.message-consumption-0055/tests/test_bridge.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_Ajl_eybv18QkXk0ohG6KBP29FzRCXXxW','original_deadline':1791555052.1342988},
            {'original_index':31,'resource':'file:.message-consumption-0055/tests/test_bridge_remote.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_fjFltNHncgkAYZ0Xv9AJ60k3_rEN5-hk','original_deadline':1791554842.656},
        ]

    def _wbt_next_file_class_arguments(self, original_index):
        candidates=self._wbt_next_file_class_candidates()
        chosen=next(row for row in candidates if row['original_index']==original_index)
        with patch('agent_chat.root_lifecycle.stopped',return_value=False):
            arguments,target_coord,target,_=self.wbt_abandon_arguments('wbt:exact-row')
        target_coord.session=chosen['owner_session']
        target_coord.register('reviewer_file_class_next')
        target_coord.db.execute('UPDATE resources SET name=?,owner_session=?,reservation_id=?,deadline=?,stale=1 WHERE name=?',
            (chosen['resource'],chosen['owner_session'],chosen['reservation_id'],chosen['original_deadline'],'wbt:exact-row'))
        target_coord.db.execute('DELETE FROM bridge_bindings WHERE session_id=?',('wbt-owner',))
        target_coord.db.execute('DELETE FROM remote_session_hosts WHERE session_id=?',('wbt-owner',))
        target_coord.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?,?,?)',
            (chosen['owner_session'],'thread-wbt-owner',None,None,time.time()))
        target_coord.db.execute('INSERT INTO remote_session_hosts VALUES(?,?,?,?)',
            (chosen['owner_session'],'host-root',time.time(),time.time()))
        target_coord.db.commit()
        arguments.update(resource=chosen['resource'],owner_session=chosen['owner_session'],
            reservation_id=chosen['reservation_id'],expected_deadline=chosen['original_deadline'])
        return arguments,target_coord,target,chosen,candidates

    def _wbt_next_file_class_context(self, arguments, candidates, rows=None):
        # Explicit test-owned records: no historical archive is read or asserted.
        case=self
        class SyntheticContext:
            def __enter__(self):
                with contextlib.ExitStack() as stack:
                    source_rows=rows if rows is not None else case._wbt_file_class_rows(arguments,candidates)
                    stack.enter_context(case._wbt_artifact(arguments,rows=source_rows))
                    encoded=[]
                    for row in candidates:
                        encoded.append(subprocess.run(['jq','-cn','--argjson','index',str(row['original_index']),
                            '--arg','resource',row['resource'],'--arg','owner',row['owner_session'],
                            '--arg','reservation',row['reservation_id'],'--argjson','deadline',repr(row['original_deadline']),
                            '{original_index:$index,resource:$resource,owner_session:$owner,reservation_id:$reservation,original_deadline:$deadline}'],
                            capture_output=True,check=True).stdout)
                    source=Path(expired_claim_module._WBT_COHORT_ARTIFACT_PATH)
                    source_bytes=source.read_bytes(); source_hash=hashlib.sha256(source_bytes).hexdigest()
                    program="""def origin: if .==2 then "wbt-abandon-apply-ee58a8aa-7208-41e6-a305-b71376d9f893/root-derived-result.json" elif .<=5 then "023qy-three-row-results-v1.json" elif .<=10 then "3eoW-five-row-results-v1.json" elif .<=15 then "q8oj-five-row-results-v1.json" else "tyXA-eight-row-results-v1.json" end;
                    {manifest_version:1,purpose:"historical candidate selection only; not current eligibility",source:{artifact:$artifact,bytes:$bytes,evidence_kind:"dated_supported_status_snapshot_not_current",row_count:$rows,sha256:$sha},selection:{maximum:8,order:["original_index"],owner_session:$owner,resource_class:"file:"},excluded_completed_tuples:[range(2;24) | . as $i | {original_index:$i,original_deadline:1,owner_session:$owner,reservation_id:("synthetic-reservation-"+($i|tostring)),resource:("file:synthetic-"+($i|tostring)),outcome:"applied",outcome_source:($i|origin)}],candidates:.}"""
                    manifest=subprocess.run(['jq','-sc','--arg','artifact',str(source),'--argjson','bytes',str(len(source_bytes)),
                        '--argjson','rows',str(len(source_rows)),'--arg','sha',source_hash,
                        '--arg','owner','session_3d8278d80221401e939587db90280dc1',program],
                        input=b''.join(encoded),capture_output=True,check=True).stdout
                    path=case.master.parent/'synthetic-next-file-class.json';path.write_bytes(manifest)
                    stack.enter_context(patch.multiple('agent_chat.expired_claim_disposition',
                        _WBT_NEXT_FILE_CLASS_MANIFEST_PATH=str(path),
                        _WBT_NEXT_FILE_CLASS_MANIFEST_SHA256=hashlib.sha256(manifest).hexdigest(),
                        _WBT_NEXT_FILE_CLASS_MANIFEST_SIZE=len(manifest),
                        _WBT_NEXT_FILE_CLASS_SOURCE_PATH=str(source),_WBT_NEXT_FILE_CLASS_SOURCE_SHA256=source_hash,
                        _WBT_NEXT_FILE_CLASS_SOURCE_SIZE=len(source_bytes),_WBT_NEXT_FILE_CLASS_SOURCE_ROWS=len(source_rows)))
                    self.stack=stack.pop_all()
                return self
            def __exit__(self,*exc): return self.stack.__exit__(*exc)
        return SyntheticContext()

    def _wbt_gg1_file_class_candidates(self):
        return [
            {'original_index':32,'resource':'file:.message-consumption-0055/tests/test_context.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_DSNXmSDLUMlXsYGAR7zZfmrHSdSEX8mr','original_deadline':1791554834.460922},
            {'original_index':33,'resource':'file:.message-consumption-0055/tests/test_context_freshness.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_fYbGo18OKEx-jc3TFaK9rLZ_kPoWXi1y','original_deadline':1791554837.154465},
            {'original_index':34,'resource':'file:.message-consumption-0055/tests/test_core.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_qAJt8-wc2CsURVZkkjbywjwyS-5Msbkw','original_deadline':1791554840.2223492},
            {'original_index':35,'resource':'file:.message-consumption-0055/tests/test_supervisor.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_D_3JfFFalJkoyjFx4io0LiGy41cgJq0a','original_deadline':1791554845.418402},
            {'original_index':36,'resource':'file:.message-consumption-0055/tests/test_supervisor_notices.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_cFDmGmKoDn2Y_kFfNZWHnBWgWnZA0QXl','original_deadline':1791555055.0780869},
            {'original_index':37,'resource':'file:.multi-root-model-contract-0062/docs/reference/agent-operations.md','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_xNOmRXXhdyQDJs7gMclxxRvPBMrrPFwj','original_deadline':1791603614.1843998},
            {'original_index':38,'resource':'file:.multi-root-model-contract-0062/src/agent_chat/control_worker.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_X7gxw4PZxO_q12EFFwJT3L0Jao80QGXL','original_deadline':1791603595.110868},
            {'original_index':39,'resource':'file:.multi-root-model-contract-0062/src/agent_chat/rpc.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_XuDxvOaE16oBcMaufkLQFuEWfm4dDF46','original_deadline':1791603598.328842},
        ]

    def _wbt_gg1_file_class_arguments(self, original_index):
        candidates=self._wbt_gg1_file_class_candidates()
        chosen=next(row for row in candidates if row['original_index']==original_index)
        with patch('agent_chat.root_lifecycle.stopped',return_value=False):
            arguments,target_coord,target,_=self.wbt_abandon_arguments('wbt:exact-row')
        target_coord.session=chosen['owner_session']
        target_coord.register('reviewer_file_class_gg1')
        target_coord.db.execute('UPDATE resources SET name=?,owner_session=?,reservation_id=?,deadline=?,stale=1 WHERE name=?',
            (chosen['resource'],chosen['owner_session'],chosen['reservation_id'],chosen['original_deadline'],'wbt:exact-row'))
        target_coord.db.execute('DELETE FROM bridge_bindings WHERE session_id=?',('wbt-owner',))
        target_coord.db.execute('DELETE FROM remote_session_hosts WHERE session_id=?',('wbt-owner',))
        target_coord.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?,?,?)',
            (chosen['owner_session'],'thread-wbt-owner',None,None,time.time()))
        target_coord.db.execute('INSERT INTO remote_session_hosts VALUES(?,?,?,?)',
            (chosen['owner_session'],'host-root',time.time(),time.time()))
        target_coord.db.commit()
        arguments.update(resource=chosen['resource'],owner_session=chosen['owner_session'],
            reservation_id=chosen['reservation_id'],expected_deadline=chosen['original_deadline'])
        return arguments,target_coord,target,chosen,candidates

    def _wbt_gg1_file_class_context(self, arguments, candidates, rows=None):
        # Explicit test-owned records: no historical archive is read or asserted.
        case=self
        class SyntheticContext:
            def __enter__(self):
                with contextlib.ExitStack() as stack:
                    source_rows=rows if rows is not None else case._wbt_file_class_rows(arguments,candidates)
                    stack.enter_context(case._wbt_artifact(arguments,rows=source_rows))
                    encoded=[]
                    for row in candidates:
                        encoded.append(subprocess.run(['jq','-cn','--argjson','index',str(row['original_index']),
                            '--arg','resource',row['resource'],'--arg','owner',row['owner_session'],
                            '--arg','reservation',row['reservation_id'],'--argjson','deadline',repr(row['original_deadline']),
                            '{original_index:$index,resource:$resource,owner_session:$owner,reservation_id:$reservation,original_deadline:$deadline}'],
                            capture_output=True,check=True).stdout)
                    source=Path(expired_claim_module._WBT_COHORT_ARTIFACT_PATH)
                    source_bytes=source.read_bytes(); source_hash=hashlib.sha256(source_bytes).hexdigest()
                    program="""def origin: if .==2 then "wbt-abandon-apply-ee58a8aa-7208-41e6-a305-b71376d9f893/root-derived-result.json" elif .<=5 then "023qy-three-row-results-v1.json" elif .<=10 then "3eoW-five-row-results-v1.json" elif .<=15 then "q8oj-five-row-results-v1.json" elif .<=23 then "tyXA-eight-row-results-v1.json" else "psbx-eight-row-results-v1.json" end;
                    {manifest_version:1,purpose:"historical candidate selection only; not current eligibility",source:{artifact:$artifact,bytes:$bytes,evidence_kind:"dated_supported_status_snapshot_not_current",row_count:$rows,sha256:$sha},selection:{maximum:8,order:["original_index"],owner_session:$owner,resource_class:"file:"},excluded_completed_tuples:[range(2;32) | . as $i | {original_index:$i,original_deadline:1,owner_session:$owner,reservation_id:("synthetic-reservation-"+($i|tostring)),resource:("file:synthetic-"+($i|tostring)),outcome:"applied",outcome_source:($i|origin)}],candidates:.}"""
                    manifest=subprocess.run(['jq','-sc','--arg','artifact',str(source),'--argjson','bytes',str(len(source_bytes)),
                        '--argjson','rows',str(len(source_rows)),'--arg','sha',source_hash,
                        '--arg','owner','session_3d8278d80221401e939587db90280dc1',program],
                        input=b''.join(encoded),capture_output=True,check=True).stdout
                    path=case.master.parent/'synthetic-gg1-file-class.json';path.write_bytes(manifest)
                    stack.enter_context(patch.multiple('agent_chat.expired_claim_disposition',
                        _WBT_GG1_FILE_CLASS_MANIFEST_PATH=str(path),
                        _WBT_GG1_FILE_CLASS_MANIFEST_SHA256=hashlib.sha256(manifest).hexdigest(),
                        _WBT_GG1_FILE_CLASS_MANIFEST_SIZE=len(manifest),
                        _WBT_GG1_FILE_CLASS_SOURCE_PATH=str(source),_WBT_GG1_FILE_CLASS_SOURCE_SHA256=source_hash,
                        _WBT_GG1_FILE_CLASS_SOURCE_SIZE=len(source_bytes),_WBT_GG1_FILE_CLASS_SOURCE_ROWS=len(source_rows)))
                    self.stack=stack.pop_all()
                return self
            def __exit__(self,*exc): return self.stack.__exit__(*exc)
        return SyntheticContext()

    def _wbt_hgide_file_class_candidates(self):
        return [
            {'original_index':40,'resource':'file:.multi-root-model-contract-0062/tests/test_agent_model.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_uJqVOsIZCuuKSufno4Y84VceICQpqGwY','original_deadline':1791603602.533458},
            {'original_index':41,'resource':'file:.multi-root-model-contract-0062/tests/test_rpc.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_PmibxSxPDBX_TmlQleuZd0hX8ZjAr_Lh','original_deadline':1791603610.114312},
            {'original_index':42,'resource':'file:.native-thread-model-ordinary-0063/docs/reference/agent-operations.md','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_q5Q6H9NcGpurGLuwqIp1VoD-uv9-c8yb','original_deadline':1791707989.408392},
            {'original_index':43,'resource':'file:.native-thread-model-ordinary-0063/src/agent_chat/agent_control.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_nGxuIJs7h-zCN_o0a1FjTGYd832xU4dY','original_deadline':1791707904.836443},
            {'original_index':44,'resource':'file:.native-thread-model-ordinary-0063/src/agent_chat/bridge.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_FhTfoFT8l2YcB3ciKz-EV3S6txKsmQqD','original_deadline':1791707988.0077188},
            {'original_index':45,'resource':'file:.native-thread-model-ordinary-0063/src/agent_chat/bridge_lease.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_hxbsbGTm1DsvBKJHuaUc81yG13zTVp9Z','original_deadline':1791707988.634374},
            {'original_index':46,'resource':'file:.native-thread-model-ordinary-0063/src/agent_chat/bridge_state.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_6WvnzXgE0weWQEYSIa6Kl0cj-mt6cSY4','original_deadline':1791707987.721306},
            {'original_index':47,'resource':'file:.native-thread-model-ordinary-0063/src/agent_chat/control_worker.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_pg3OXih9cOkTA3sfcp0xioADYT82-0yb','original_deadline':1791707988.303356},
        ]

    def _wbt_hgide_file_class_arguments(self, original_index):
        candidates=self._wbt_hgide_file_class_candidates()
        chosen=next(row for row in candidates if row['original_index']==original_index)
        with patch('agent_chat.root_lifecycle.stopped',return_value=False):
            arguments,target_coord,target,_=self.wbt_abandon_arguments('wbt:exact-row')
        target_coord.session=chosen['owner_session']
        target_coord.register('reviewer_file_class_hgide')
        target_coord.db.execute('UPDATE resources SET name=?,owner_session=?,reservation_id=?,deadline=?,stale=1 WHERE name=?',
            (chosen['resource'],chosen['owner_session'],chosen['reservation_id'],chosen['original_deadline'],'wbt:exact-row'))
        target_coord.db.execute('DELETE FROM bridge_bindings WHERE session_id=?',('wbt-owner',))
        target_coord.db.execute('DELETE FROM remote_session_hosts WHERE session_id=?',('wbt-owner',))
        target_coord.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?,?,?)',
            (chosen['owner_session'],'thread-wbt-owner',None,None,time.time()))
        target_coord.db.execute('INSERT INTO remote_session_hosts VALUES(?,?,?,?)',
            (chosen['owner_session'],'host-root',time.time(),time.time()))
        target_coord.db.commit()
        arguments.update(resource=chosen['resource'],owner_session=chosen['owner_session'],
            reservation_id=chosen['reservation_id'],expected_deadline=chosen['original_deadline'])
        return arguments,target_coord,target,chosen,candidates

    def _wbt_hgide_file_class_context(self, arguments, candidates, rows=None):
        # Explicit test-owned records: no historical archive is read or asserted.
        case=self
        class SyntheticContext:
            def __enter__(self):
                with contextlib.ExitStack() as stack:
                    source_rows=rows if rows is not None else case._wbt_file_class_rows(arguments,candidates)
                    stack.enter_context(case._wbt_artifact(arguments,rows=source_rows))
                    encoded=[]
                    for row in candidates:
                        encoded.append(subprocess.run(['jq','-cn','--argjson','index',str(row['original_index']),
                            '--arg','resource',row['resource'],'--arg','owner',row['owner_session'],
                            '--arg','reservation',row['reservation_id'],'--argjson','deadline',repr(row['original_deadline']),
                            '{original_index:$index,resource:$resource,owner_session:$owner,reservation_id:$reservation,original_deadline:$deadline}'],
                            capture_output=True,check=True).stdout)
                    source=Path(expired_claim_module._WBT_COHORT_ARTIFACT_PATH)
                    source_bytes=source.read_bytes(); source_hash=hashlib.sha256(source_bytes).hexdigest()
                    program="""def origin: if .==2 then "wbt-abandon-apply-ee58a8aa-7208-41e6-a305-b71376d9f893/root-derived-result.json" elif .<=5 then "023qy-three-row-results-v1.json" elif .<=10 then "3eoW-five-row-results-v1.json" elif .<=15 then "q8oj-five-row-results-v1.json" elif .<=23 then "tyXA-eight-row-results-v1.json" elif .<=31 then "psbx-eight-row-results-v1.json" else ".root-private/root-intake-20261017-2/Gg1-eight-row-results-v1.json" end;
                    {manifest_version:1,purpose:"historical candidate selection only; not current eligibility",source:{artifact:$artifact,association:"message_LQZLiPTothZ8DE76fO2J8f3tdtOO4GLl/attachment_CqD-npf1BMSkUrYalKsrwPRdNrumeY-w",bytes:$bytes,evidence_kind:"dated_supported_status_snapshot_not_current",row_count:$rows,sha256:$sha},selection:{maximum:8,order:["original_index"],owner_session:$owner,resource_class:"file:",scope:"first remaining source-index tuples after completed indices 2 through 39"},excluded_completed_tuples:[range(2;40) | . as $i | {original_index:$i,original_deadline:1,owner_session:$owner,reservation_id:("synthetic-reservation-"+($i|tostring)),resource:("file:synthetic-"+($i|tostring)),outcome:"applied",outcome_source:($i|origin)} + (if $i>=32 then {apply_cli_exit:0,outcome_capture:"synthetic-only",outcome_request_id:("synthetic-request-"+($i|tostring))} else {} end)],candidates:.}"""
                    manifest=subprocess.run(['jq','-sc','--arg','artifact',str(source),'--argjson','bytes',str(len(source_bytes)),
                        '--argjson','rows',str(len(source_rows)),'--arg','sha',source_hash,
                        '--arg','owner','session_3d8278d80221401e939587db90280dc1',program],
                        input=b''.join(encoded),capture_output=True,check=True).stdout
                    path=case.master.parent/'synthetic-hgide-file-class.json';path.write_bytes(manifest)
                    stack.enter_context(patch.multiple('agent_chat.expired_claim_disposition',
                        _WBT_HGIDE_FILE_CLASS_MANIFEST_PATH=str(path),
                        _WBT_HGIDE_FILE_CLASS_MANIFEST_SHA256=hashlib.sha256(manifest).hexdigest(),
                        _WBT_HGIDE_FILE_CLASS_MANIFEST_SIZE=len(manifest),
                        _WBT_HGIDE_FILE_CLASS_SOURCE_PATH=str(source),_WBT_HGIDE_FILE_CLASS_SOURCE_SHA256=source_hash,
                        _WBT_HGIDE_FILE_CLASS_SOURCE_SIZE=len(source_bytes),_WBT_HGIDE_FILE_CLASS_SOURCE_ROWS=len(source_rows)))
                    self.stack=stack.pop_all()
                return self
            def __exit__(self,*exc): return self.stack.__exit__(*exc)
        return SyntheticContext()

    def _wbt_6jc1_file_class_candidates(self):
        return [
            {'original_index':48,'resource':'file:.native-thread-model-ordinary-0063/tests/test_agent_model.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_BcavpO0r745KF9qUybZJP6_deW6VuS7a','original_deadline':1791707988.893236},
            {'original_index':49,'resource':'file:.native-thread-model-ordinary-0063/tests/test_bridge.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_cts4nAV_UN1i8dHQ__MO-qD9ZWj6hAyW','original_deadline':1791707989.1490371},
            {'original_index':50,'resource':'file:.operator-message-ui-0065/docs/reference/agent-operations.md','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_AynA9q7IyYlcaSrGmZ7f2gVkWhqqRQ8P','original_deadline':1791801363.368866},
            {'original_index':51,'resource':'file:.operator-message-ui-0065/src/agent_chat/communication_view.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_-ET8oRg-aWAdTSU06buHzN1hZs1nS5g2','original_deadline':1791801360.098263},
            {'original_index':52,'resource':'file:.operator-message-ui-0065/src/agent_chat/web.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_GX8IkIVeJQxv5Fsvb8UfQ1YjFoowFoT8','original_deadline':1791801359.57241},
            {'original_index':53,'resource':'file:.operator-message-ui-0065/src/agent_chat/web/app.js','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_t0hvzXjj8W_Pe7du12ww4PWmhMgINFKC','original_deadline':1791801360.661719},
            {'original_index':54,'resource':'file:.operator-message-ui-0065/src/agent_chat/web/index.html','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_8NNe-oJdOYkGHOtDsAGQ0JPbtq9C5PIn','original_deadline':1791801361.1641018},
            {'original_index':55,'resource':'file:.operator-message-ui-0065/src/agent_chat/web/style.css','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_l8YSrhXYNA2LwNeg7Np83vnHLIo_-gsP','original_deadline':1791801361.67447},
        ]

    def _wbt_6jc1_file_class_arguments(self, original_index):
        candidates=self._wbt_6jc1_file_class_candidates()
        chosen=next(row for row in candidates if row['original_index']==original_index)
        with patch('agent_chat.root_lifecycle.stopped',return_value=False):
            arguments,target_coord,target,_=self.wbt_abandon_arguments('wbt:exact-row')
        target_coord.session=chosen['owner_session']
        target_coord.register('reviewer_file_class_6jc1')
        target_coord.db.execute('UPDATE resources SET name=?,owner_session=?,reservation_id=?,deadline=?,stale=1 WHERE name=?',
            (chosen['resource'],chosen['owner_session'],chosen['reservation_id'],chosen['original_deadline'],'wbt:exact-row'))
        target_coord.db.execute('DELETE FROM bridge_bindings WHERE session_id=?',('wbt-owner',))
        target_coord.db.execute('DELETE FROM remote_session_hosts WHERE session_id=?',('wbt-owner',))
        target_coord.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?,?,?)',
            (chosen['owner_session'],'thread-wbt-owner',None,None,time.time()))
        target_coord.db.execute('INSERT INTO remote_session_hosts VALUES(?,?,?,?)',
            (chosen['owner_session'],'host-root',time.time(),time.time()))
        target_coord.db.commit()
        arguments.update(resource=chosen['resource'],owner_session=chosen['owner_session'],
            reservation_id=chosen['reservation_id'],expected_deadline=chosen['original_deadline'])
        return arguments,target_coord,target,chosen,candidates

    def _wbt_6jc1_file_class_context(self, arguments, candidates, rows=None):
        # Test-owned jq data and scoped pins exercise the validator, not the
        # unavailable historical archive or its original provenance.
        return self._wbt_6jc1_synthetic_context(arguments,candidates,rows=rows)

    def _wbt_6jc1_synthetic_context(self, arguments, candidates, rows=None):
        case=self
        class FixedArtifactContext:
            def __enter__(self):
                self.stack=contextlib.ExitStack()
                self.stack.enter_context(patch('agent_chat.ownership_disposition._installed_server_config',return_value=case.config))
                self.stack.enter_context(patch('agent_chat.ownership_disposition.configured_server_database',return_value=case.master))
                source_rows=rows if rows is not None else case._wbt_file_class_rows(arguments,candidates)
                self.stack.enter_context(case._wbt_artifact(arguments,rows=source_rows))
                # Synthetic structural data only: these entries are not historical receipts.
                encoded=[]
                for row in candidates:
                    encoded.append(subprocess.run(['jq','-cn','--argjson','index',str(row['original_index']),
                        '--arg','resource',row['resource'],'--arg','owner',row['owner_session'],
                        '--arg','reservation',row['reservation_id'],'--argjson','deadline',repr(row['original_deadline']),
                        '{original_index:$index,resource:$resource,owner_session:$owner,reservation_id:$reservation,original_deadline:$deadline}'],
                        capture_output=True,check=True).stdout)
                source=Path(expired_claim_module._WBT_COHORT_ARTIFACT_PATH)
                source_bytes=source.read_bytes(); source_hash=hashlib.sha256(source_bytes).hexdigest()
                program='''def origin: if .==2 then "wbt-abandon-apply-ee58a8aa-7208-41e6-a305-b71376d9f893/root-derived-result.json" elif .<=5 then "023qy-three-row-results-v1.json" elif .<=10 then "3eoW-five-row-results-v1.json" elif .<=15 then "q8oj-five-row-results-v1.json" elif .<=23 then "tyXA-eight-row-results-v1.json" elif .<=31 then "psbx-eight-row-results-v1.json" elif .<=39 then ".root-private/root-intake-20261017-2/Gg1-eight-row-results-v1.json" else ".root-private/root-intake-20261017-2/Hgide-eight-row-results-v1.json" end;
                {manifest_version:1,purpose:"historical candidate selection only; not current eligibility",source:{artifact:$artifact,association:"message_LQZLiPTothZ8DE76fO2J8f3tdtOO4GLl/attachment_CqD-npf1BMSkUrYalKsrwPRdNrumeY-w",bytes:$bytes,evidence_kind:"dated supported status snapshot; not current",row_count:$rows,sha256:$sha},selection:{maximum:8,order:["original_index"],owner_session:$owner,resource_class:"canonical file: only",scope:"first remaining original source-index tuples after completed exact indices 2 through 47"},excluded_completed_tuples:[range(2;48) | . as $i | {original_index:$i,original_deadline:1,owner_session:$owner,reservation_id:("synthetic-reservation-"+($i|tostring)),resource:("file:synthetic-"+($i|tostring)),outcome:"applied",outcome_source:($i|origin)} + (if $i>=32 then {apply_cli_exit:0,outcome_capture:"synthetic-only",outcome_request_id:("synthetic-request-"+($i|tostring))} else {} end)],candidates:.}'''
                manifest=subprocess.run(['jq','-sc','--arg','artifact',str(source),'--argjson','bytes',str(len(source_bytes)),
                    '--argjson','rows',str(len(source_rows)),'--arg','sha',source_hash,
                    '--arg','owner','session_3d8278d80221401e939587db90280dc1',program],
                    input=b''.join(encoded),capture_output=True,check=True).stdout
                path=case.master.parent/'wbt-6jc1-file-class-fixture.json'
                path.write_bytes(manifest)
                self.stack.enter_context(patch.multiple('agent_chat.expired_claim_disposition',
                    _WBT_6JC1_FILE_CLASS_MANIFEST_PATH=str(path),
                    _WBT_6JC1_FILE_CLASS_MANIFEST_SHA256=hashlib.sha256(manifest).hexdigest(),
                    _WBT_6JC1_FILE_CLASS_MANIFEST_SIZE=len(manifest),
                    _WBT_6JC1_FILE_CLASS_SOURCE_PATH=str(source),
                    _WBT_6JC1_FILE_CLASS_SOURCE_SHA256=source_hash,
                    _WBT_6JC1_FILE_CLASS_SOURCE_SIZE=len(source_bytes),
                    _WBT_6JC1_FILE_CLASS_SOURCE_ROWS=len(source_rows)))
                return self
            def __exit__(self,*exc):
                return self.stack.__exit__(*exc)
        return FixedArtifactContext()

    def _wbt_hqg_file_class_candidates(self):
        return [
            {'original_index':56,'resource':'file:.operator-message-ui-0065/tests/operator_message_ui_browser.cjs','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_ZLi8m2YFzmxwEyzblklBgdYasXXPSJ-u','original_deadline':1791801362.786963},
            {'original_index':57,'resource':'file:.operator-message-ui-0065/tests/test_communication_view.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_x9paK8__AEf1es600sT3jiWgsEDEHEYe','original_deadline':1791801362.202919},
            {'original_index':58,'resource':'file:.ownership-disposition-0039-sampler/docs/reference/agent-operations.md','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_ZFJCgfkkio2iCEef398jQcvfSCUMDH6Z','original_deadline':1791536774.969249},
            {'original_index':59,'resource':'file:.ownership-disposition-0039-sampler/pyproject.toml','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_GY1UuuvtpMX2oN6gXR5Q41TsZGAqMrJq','original_deadline':1791536775.096697},
            {'original_index':60,'resource':'file:.ownership-disposition-0039-sampler/src/agent_chat/agent_control.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_Sq_olIdS6RXwolWbTA6M2DGxF9szPyNJ','original_deadline':1791536773.9387782},
            {'original_index':61,'resource':'file:.ownership-disposition-0039-sampler/src/agent_chat/bridge.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_yVn2V-lcnkBKBgOZPXhNvwA6__kVuMUG','original_deadline':1791536774.3247821},
            {'original_index':62,'resource':'file:.ownership-disposition-0039-sampler/src/agent_chat/bridge_client.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_52cbdh6JsQwG4jrM10w-HXcgmU7wsBlp','original_deadline':1791536774.5967},
            {'original_index':63,'resource':'file:.ownership-disposition-0039-sampler/src/agent_chat/bridge_lease.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_ylIKjg-ySJ66IdxC2uldbWCyWeU7-oyo','original_deadline':1791536774.453158},
        ]

    def _wbt_hqg_file_class_arguments(self, original_index):
        candidates=self._wbt_hqg_file_class_candidates()
        chosen=next(row for row in candidates if row['original_index']==original_index)
        with patch('agent_chat.root_lifecycle.stopped',return_value=False):
            arguments,target_coord,target,_=self.wbt_abandon_arguments('wbt:exact-row')
        target_coord.session=chosen['owner_session']
        target_coord.register('reviewer_file_class_hqg')
        target_coord.db.execute('UPDATE resources SET name=?,owner_session=?,reservation_id=?,deadline=?,stale=1 WHERE name=?',
            (chosen['resource'],chosen['owner_session'],chosen['reservation_id'],chosen['original_deadline'],'wbt:exact-row'))
        target_coord.db.execute('DELETE FROM bridge_bindings WHERE session_id=?',('wbt-owner',))
        target_coord.db.execute('DELETE FROM remote_session_hosts WHERE session_id=?',('wbt-owner',))
        target_coord.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?,?,?)',
            (chosen['owner_session'],'thread-wbt-owner',None,None,time.time()))
        target_coord.db.execute('INSERT INTO remote_session_hosts VALUES(?,?,?,?)',
            (chosen['owner_session'],'host-root',time.time(),time.time()))
        target_coord.db.commit()
        arguments.update(resource=chosen['resource'],owner_session=chosen['owner_session'],
            reservation_id=chosen['reservation_id'],expected_deadline=chosen['original_deadline'])
        return arguments,target_coord,target,chosen,candidates

    def _wbt_hqg_file_class_context(self, arguments, candidates, rows=None):
        # Explicit test-owned records: no historical archive is read or asserted.
        case=self
        class SyntheticContext:
            def __enter__(self):
                with contextlib.ExitStack() as stack:
                    source_rows=rows if rows is not None else case._wbt_file_class_rows(arguments,candidates)
                    stack.enter_context(case._wbt_artifact(arguments,rows=source_rows))
                    encoded=[]
                    for row in candidates:
                        encoded.append(subprocess.run(['jq','-cn','--argjson','index',str(row['original_index']),
                            '--arg','resource',row['resource'],'--arg','owner',row['owner_session'],
                            '--arg','reservation',row['reservation_id'],'--argjson','deadline',repr(row['original_deadline']),
                            '{original_index:$index,resource:$resource,owner_session:$owner,reservation_id:$reservation,original_deadline:$deadline}'],
                            capture_output=True,check=True).stdout)
                    source=Path(expired_claim_module._WBT_COHORT_ARTIFACT_PATH)
                    source_bytes=source.read_bytes(); source_hash=hashlib.sha256(source_bytes).hexdigest()
                    program="""{manifest_version:1,kind:"offline_historical_candidate_manifest_only",claim_or_resource_state:"not evaluated",current_eligibility:"not evaluated",target_file_access:"none",source_association:"original cohort plus retained completion outcome records; historical membership only",source:{path:$artifact,bytes:$bytes,evidence_kind:"dated_supported_status_snapshot_not_current",row_count:$rows,sha256:$sha},selection:{candidates:.,owner_session:$owner,resource_class:"literal canonical file: only",selected_indices:[range(56;64)],source_order:"original zero-based source index",unique_reservation_ids:true,unique_resources:true},completed_exclusions:{count:54,indices:[range(2;56)],unique_reservation_ids:true,unique_resources:true,entries:[range(2;56)|. as $i|{original_index:$i,original_deadline:1,owner_session:$owner,reservation_id:("synthetic-reservation-"+($i|tostring)),resource:("file:synthetic-"+($i|tostring)),terminal_status:"applied",completion_request_id:("synthetic-request-"+($i|tostring)),completion_record:(if $i<=47 then "6JC1-root-offline-selection-expected" else "6JC1-eight-row-results-v1" end)}+(if $i>=48 then {apply_cli_exit:0,prepare_cli_exit:0} else {} end)]}}"""
                    manifest=subprocess.run(['jq','-sc','--arg','artifact',str(source),'--argjson','bytes',str(len(source_bytes)),
                        '--argjson','rows',str(len(source_rows)),'--arg','sha',source_hash,
                        '--arg','owner','session_3d8278d80221401e939587db90280dc1',program],
                        input=b''.join(encoded),capture_output=True,check=True).stdout
                    path=case.master.parent/'synthetic-hqg-file-class.json';path.write_bytes(manifest)
                    stack.enter_context(patch.multiple('agent_chat.expired_claim_disposition',
                        _WBT_HQG_FILE_CLASS_MANIFEST_PATH=str(path),
                        _WBT_HQG_FILE_CLASS_MANIFEST_SHA256=hashlib.sha256(manifest).hexdigest(),
                        _WBT_HQG_FILE_CLASS_MANIFEST_SIZE=len(manifest),
                        _WBT_HQG_FILE_CLASS_SOURCE_PATH=str(source),_WBT_HQG_FILE_CLASS_SOURCE_SHA256=source_hash,
                        _WBT_HQG_FILE_CLASS_SOURCE_SIZE=len(source_bytes),_WBT_HQG_FILE_CLASS_SOURCE_ROWS=len(source_rows)))
                    self.stack=stack.pop_all()
                return self
            def __exit__(self,*exc): return self.stack.__exit__(*exc)
        return SyntheticContext()

    def _wbt_file_class_rows(self, arguments, candidates):
        rows=[('fixture:source-row-'+str(index),'fixture-owner-'+str(index),
               'fixture-rid-'+str(index),arguments['expected_deadline']-1000+index)
              for index in range(expired_claim_module._WBT_COHORT_ROW_COUNT)]
        for candidate in candidates:
            rows[candidate['original_index']]=(candidate['resource'],candidate['owner_session'],
                candidate['reservation_id'],candidate['original_deadline'])
        return rows

    def _wbt_file_class_context(self, arguments, candidates, rows=None):
        # Explicit test-owned records: no historical archive is read or asserted.
        case=self
        class SyntheticContext:
            def __enter__(self):
                with contextlib.ExitStack() as stack:
                    source_rows=rows if rows is not None else case._wbt_file_class_rows(arguments,candidates)
                    stack.enter_context(case._wbt_artifact(arguments,rows=source_rows))
                    encoded=[]
                    for row in candidates:
                        encoded.append(subprocess.run(['jq','-cn','--argjson','index',str(row['original_index']),
                            '--arg','resource',row['resource'],'--arg','owner',row['owner_session'],
                            '--arg','reservation',row['reservation_id'],'--argjson','deadline',repr(row['original_deadline']),
                            '{original_index:$index,resource:$resource,owner_session:$owner,reservation_id:$reservation,original_deadline:$deadline}'],
                            capture_output=True,check=True).stdout)
                    source=Path(expired_claim_module._WBT_COHORT_ARTIFACT_PATH)
                    source_bytes=source.read_bytes(); source_hash=hashlib.sha256(source_bytes).hexdigest()
                    program="""def origin: if .==2 then "wbt-abandon-apply-ee58a8aa-7208-41e6-a305-b71376d9f893/root-derived-result.json" elif .<=5 then "023qy-three-row-results-v1.json" elif .<=10 then "3eoW-five-row-results-v1.json" else "q8oj-five-row-results-v1.json" end;
                    {manifest_version:1,purpose:"historical candidate selection only; not current eligibility",source:{artifact:$artifact,bytes:$bytes,evidence_kind:"dated_supported_status_snapshot_not_current",row_count:$rows,sha256:$sha},selection:{maximum:8,order:["original_index"],owner_session:$owner,resource_class:"file:"},excluded_completed_tuples:[range(2;16) | . as $i | {original_index:$i,original_deadline:1,owner_session:$owner,reservation_id:("synthetic-reservation-"+($i|tostring)),resource:("file:synthetic-"+($i|tostring)),outcome:"applied",outcome_source:($i|origin)}],candidates:.}"""
                    manifest=subprocess.run(['jq','-sc','--arg','artifact',str(source),'--argjson','bytes',str(len(source_bytes)),
                        '--argjson','rows',str(len(source_rows)),'--arg','sha',source_hash,
                        '--arg','owner','session_3d8278d80221401e939587db90280dc1',program],
                        input=b''.join(encoded),capture_output=True,check=True).stdout
                    path=case.master.parent/'synthetic-file-class.json';path.write_bytes(manifest)
                    stack.enter_context(patch.multiple('agent_chat.expired_claim_disposition',
                        _WBT_FILE_CLASS_MANIFEST_PATH=str(path),
                        _WBT_FILE_CLASS_MANIFEST_SHA256=hashlib.sha256(manifest).hexdigest(),
                        _WBT_FILE_CLASS_MANIFEST_SIZE=len(manifest),
                        _WBT_FILE_CLASS_SOURCE_PATH=str(source),_WBT_FILE_CLASS_SOURCE_SHA256=source_hash,
                        _WBT_FILE_CLASS_SOURCE_SIZE=len(source_bytes),_WBT_FILE_CLASS_SOURCE_ROWS=len(source_rows)))
                    self.stack=stack.pop_all()
                return self
            def __exit__(self,*exc): return self.stack.__exit__(*exc)
        return SyntheticContext()

    def _wbt_next_group_candidates(self):
        return [
            {'original_index':11,'resource':'file:.finite-batch-64-0081/docs/reference/agent-operations.md',
             'owner_session':'session_3d8278d80221401e939587db90280dc1',
             'reservation_id':'reservation_RE-FDl33P3OrkNtwu45PuyXCem0G3VtC',
             'original_deadline':1791937111.215241},
            {'original_index':12,'resource':'file:.finite-batch-64-0081/src/agent_chat/cli.py',
             'owner_session':'session_3d8278d80221401e939587db90280dc1',
             'reservation_id':'reservation_iZIofkhtsKcsE7d6qHazz1FQ2kobGlEP',
             'original_deadline':1791937109.935928},
            {'original_index':13,'resource':'file:.finite-batch-64-0081/src/agent_chat/remote_service.py',
             'owner_session':'session_3d8278d80221401e939587db90280dc1',
             'reservation_id':'reservation_nfF9kGwqKphNzjWSnBY5UZ4OI16v0bky',
             'original_deadline':1791937111.224559},
            {'original_index':14,'resource':'file:.finite-batch-64-0081/tests/test_remote_coord.py',
             'owner_session':'session_3d8278d80221401e939587db90280dc1',
             'reservation_id':'reservation_7VrsHkMhMGZ3de268Pwxz1rFCt-ymYPk',
             'original_deadline':1791937111.232606},
            {'original_index':15,'resource':'file:.finite-batch-64-0081/tests/test_remote_run.py',
             'owner_session':'session_3d8278d80221401e939587db90280dc1',
             'reservation_id':'reservation_VWmB6ATgRH7H6ZYP1pf-ZdnI2toTXA2H',
             'original_deadline':1791937111.304415},
        ]

    def _wbt_next_group_arguments(self, original_index):
        candidates=self._wbt_next_group_candidates()
        chosen=next(row for row in candidates if row['original_index']==original_index)
        arguments,target_coord,target,artifact=self.wbt_abandon_arguments(chosen['resource'])
        return arguments,target_coord,target,artifact,candidates,chosen

    def _wbt_next_group_artifact_context(self, arguments, candidates):
        case=self
        class ReusableArtifactContext:
            def __enter__(self):
                with contextlib.ExitStack() as stack:
                    deadline=arguments['expected_deadline']
                    rows=[('fixture:source-row-'+str(index),'fixture-owner-'+str(index),
                           'fixture-rid-'+str(index),deadline-20+index)
                          for index in range(11)]+[
                          (row['resource'],row['owner_session'],row['reservation_id'],row['original_deadline'])
                          for row in candidates]
                    stack.enter_context(case._wbt_artifact(arguments,rows=rows))
                    encoded=[]
                    for row in candidates:
                        encoded.append(subprocess.run(['jq','-cn','--argjson','index',str(row['original_index']),
                            '--arg','resource',row['resource'],'--arg','owner',row['owner_session'],
                            '--arg','reservation',row['reservation_id'],'--argjson','deadline',repr(row['original_deadline']),
                            '{original_index:$index,resource:$resource,owner_session:$owner,reservation_id:$reservation,original_deadline:$deadline}'],capture_output=True,check=True).stdout)
                    source=Path(expired_claim_module._WBT_COHORT_ARTIFACT_PATH)
                    source_bytes=source.read_bytes();source_hash=hashlib.sha256(source_bytes).hexdigest()
                    program="""{manifest_version:1,purpose:"offline finite historical candidate selection only; not current eligibility",source:{artifact:$artifact,bytes:$bytes,evidence_kind:"dated_supported_status_snapshot_not_current",row_count:$rows,sha256:$sha},selection:{excluded_completed_tuples:[range(2;11) | . as $i | {original_index:$i,original_deadline:1,owner_session:$owner,reservation_id:("synthetic-reservation-"+($i|tostring)),resource:("file:.finite-batch-64-"+(if $i<=5 then "0077/" else "0080/" end)+($i|tostring)),terminal_status:"applied",completion_request_id:("synthetic-request-"+($i|tostring))}],excluded_prefixes:["file:.finite-batch-64-0077/","file:.finite-batch-64-0080/"],maximum:8,order:["original_index"],owner_session:$owner,resource_prefix:"file:.finite-batch-64-0081/"},candidates:.}"""
                    manifest=subprocess.run(['jq','-sc','--arg','artifact',str(source),'--argjson','bytes',str(len(source_bytes)),
                        '--argjson','rows',str(len(rows)),'--arg','sha',source_hash,
                        '--arg','owner','session_3d8278d80221401e939587db90280dc1',program],input=b''.join(encoded),capture_output=True,check=True).stdout
                    path=case.master.parent/'synthetic-next-group.json';path.write_bytes(manifest)
                    stack.enter_context(patch.multiple('agent_chat.expired_claim_disposition',
                        _WBT_NEXT_GROUP_MANIFEST_PATH=str(path),_WBT_NEXT_GROUP_MANIFEST_SHA256=hashlib.sha256(manifest).hexdigest(),
                        _WBT_NEXT_GROUP_MANIFEST_SIZE=len(manifest),_WBT_NEXT_GROUP_SOURCE_PATH=str(source),
                        _WBT_NEXT_GROUP_SOURCE_SHA256=source_hash,_WBT_NEXT_GROUP_SOURCE_SIZE=len(source_bytes),_WBT_NEXT_GROUP_SOURCE_ROWS=len(rows)))
                    self.stack=stack.pop_all()
                return self
            def __exit__(self,*exc):
                return self.stack.__exit__(*exc)
        return ReusableArtifactContext()

    def test_next_group_failed_enter_restores_scoped_source_pins(self):
        arguments,_,_,_,candidates,_=self._wbt_next_group_arguments(11)
        before=(expired_claim_module._WBT_COHORT_ARTIFACT_PATH,
                expired_claim_module._WBT_COHORT_ARTIFACT_SHA256,
                expired_claim_module._WBT_COHORT_ARTIFACT_SIZE,
                expired_claim_module._WBT_COHORT_ROW_COUNT)
        with patch.object(Path,'read_bytes',side_effect=FileNotFoundError('synthetic absent historical fixture')):
            with self.assertRaisesRegex(FileNotFoundError,'synthetic absent'):
                with self._wbt_next_group_artifact_context(arguments,candidates):
                    self.fail('fixture unexpectedly entered')
        self.assertEqual((expired_claim_module._WBT_COHORT_ARTIFACT_PATH,
                          expired_claim_module._WBT_COHORT_ARTIFACT_SHA256,
                          expired_claim_module._WBT_COHORT_ARTIFACT_SIZE,
                          expired_claim_module._WBT_COHORT_ROW_COUNT),before)

    def _assert_wbt_next_group_candidate_lifecycle(self, original_index):
        candidates=self._wbt_next_group_candidates()
        chosen=next(row for row in candidates if row['original_index']==original_index)
        arguments,target_coord,target,artifact,_,_=self._wbt_next_group_arguments(original_index)
        with self._wbt_next_group_artifact_context(arguments,candidates),\
                patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample',
                      side_effect=self._passive_not_loaded_sample) as native_sample:
            selected=(expired_claim_module._WBT_WITNESS_VERSION,'default','root','host-root',
                arguments['project'],arguments['resource'],arguments['owner_session'],
                arguments['reservation_id'],arguments['expected_deadline'],_WBT_COHORT_REF)
            self.assertEqual(expired_claim_module._wbt_next_group_manifest_index(selected),original_index)
            self.assertEqual(expired_claim_module._wbt_pinned_artifact_membership(
                selected,row_index=original_index),'member')
            prepared=dict(execute_expired_claim(**arguments))
            replay=dict(execute_expired_claim(**arguments))
            apply_args=arguments|{'action':'wbt-abandon-apply',
                'expected_digest':prepared['prepared_digest']}
            applied=dict(execute_expired_claim(**apply_args))
            applied_replay=dict(execute_expired_claim(**apply_args))
            self.assertEqual(native_sample.call_count,2)
        self.assertEqual((prepared['status'],prepared['replayed']),('prepared',False))
        self.assertEqual((replay['status'],replay['replayed']),('prepared',True))
        self.assertEqual((applied['status'],applied['replayed']),('applied',False))
        self.assertEqual((applied_replay['status'],applied_replay['replayed']),('applied',True))
        self.assertEqual(applied['process_closure'],'UNVERIFIED')
        self.assertEqual(applied['eligibility'],'NOT_DEMONSTRATED')
        row=target_coord.db.execute(
            'SELECT owner_session,reservation_id,deadline,stale FROM resources WHERE name=?',
            (chosen['resource'],)).fetchone()
        self.assertEqual(tuple(row),(None,None,None,0))
        intent=target_coord.db.execute(
            'SELECT payload,state FROM root_wbt_file_abandon_intents WHERE request_id=?',
            (arguments['request_id'],)).fetchone()
        self.assertTrue(intent['payload'].startswith("('wbt-file-abandon-v4', "))
        self.assertIn(expired_claim_module._WBT_NEXT_GROUP_MANIFEST_SHA256,intent['payload'])
        self.assertIn(str(original_index),intent['payload'])
        self.assertEqual(intent['state'],'applied')
        self.assertEqual(target_coord.db.execute(
            'SELECT count(*) FROM root_expired_claim_dispositions WHERE request_id=?',
            (arguments['request_id'],)).fetchone()[0],1)
        events=tuple(row[0] for row in target_coord.db.execute(
            'SELECT state FROM root_wbt_file_abandon_events WHERE request_id=? ORDER BY seq',
            (arguments['request_id'],)))
        self.assertEqual(events,('accepted','prepared','applying','applied'))
        self.assertEqual(target_coord.db.execute('SELECT active FROM root_team_stops WHERE id=?',
            (self.STOP,)).fetchone()[0],1)
        self.assertTrue(Path(target).exists())

    def _assert_completed_v4_same_id_reconciliation(self, original_index):
        arguments,target_coord,target,artifact,candidates,chosen=self._wbt_next_group_arguments(original_index)
        # Historical completed requests remain readable after their finite
        # request expiry; the exact immutable payload still binds this value.
        arguments=arguments|{'expires_at':time.time()-0.1}
        self._assert_legacy_stored_outcome_is_same_id_read_only(
            arguments,target_coord,target,artifact,original_index,
            next_group_candidates=candidates)

    def test_completed_index_11_v4_same_id_reconciles_read_only(self):
        self._assert_completed_v4_same_id_reconciliation(11)

    def test_completed_index_12_v4_same_id_reconciles_read_only(self):
        self._assert_completed_v4_same_id_reconciliation(12)

    def test_completed_index_13_v4_same_id_reconciles_read_only(self):
        self._assert_completed_v4_same_id_reconciliation(13)

    def test_completed_index_14_v4_same_id_reconciles_read_only(self):
        self._assert_completed_v4_same_id_reconciliation(14)

    def test_completed_index_15_v4_same_id_reconciles_read_only(self):
        self._assert_completed_v4_same_id_reconciliation(15)

    def _assert_completed_v4_fresh_uuid_denied(self, original_index):
        arguments,target_coord,target,artifact,candidates,chosen=self._wbt_next_group_arguments(original_index)
        arguments=arguments|{'request_id':'55555555-aaaa-4bbb-8ccc-555555555555'}
        before=self._database_dump(target)
        with self._wbt_next_group_artifact_context(arguments,candidates),\
                patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample') as native_sample:
            with self.assertRaisesRegex(CoordError,'same-ID reconciliation only'):
                execute_expired_claim(**arguments)
            native_sample.assert_not_called()
        self.assertEqual(self._database_dump(target),before)
        self.assertEqual(target_coord.db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
            (expired_claim_module._WBT_ABANDON_INTENTS,)).fetchone()[0],0)

    def test_completed_index_11_v4_fresh_uuid_denied_before_bootstrap(self):
        self._assert_completed_v4_fresh_uuid_denied(11)

    def test_completed_index_12_v4_fresh_uuid_denied_before_bootstrap(self):
        self._assert_completed_v4_fresh_uuid_denied(12)

    def test_completed_index_13_v4_fresh_uuid_denied_before_bootstrap(self):
        self._assert_completed_v4_fresh_uuid_denied(13)

    def test_completed_index_14_v4_fresh_uuid_denied_before_bootstrap(self):
        self._assert_completed_v4_fresh_uuid_denied(14)

    def test_completed_index_15_v4_fresh_uuid_denied_before_bootstrap(self):
        self._assert_completed_v4_fresh_uuid_denied(15)

    def _assert_completed_v7_same_id_reconciliation(self, original_index):
        arguments,target_coord,target,chosen,candidates=self._wbt_gg1_file_class_arguments(original_index)
        arguments=arguments|{'expires_at':time.time()-0.1}
        artifact=self._wbt_gg1_file_class_context(arguments,candidates)
        self._assert_legacy_stored_outcome_is_same_id_read_only(
            arguments,target_coord,target,artifact,original_index,
            gg1_file_class_candidates=candidates)

    def _assert_completed_v7_fresh_uuid_denied(self, original_index):
        arguments,target_coord,target,chosen,candidates=self._wbt_gg1_file_class_arguments(original_index)
        arguments=arguments|{'request_id':'88888888-aaaa-4bbb-8ccc-888888888888'}
        before=self._database_dump(target)
        self.assertEqual(target_coord.db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
            (expired_claim_module._WBT_ABANDON_INTENTS,)).fetchone()[0],0)
        with self._wbt_gg1_file_class_context(arguments,candidates),\
                patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample') as native_sample:
            with self.assertRaisesRegex(CoordError,'same-ID reconciliation only'):
                execute_expired_claim(**arguments)
            native_sample.assert_not_called()
        self.assertEqual(self._database_dump(target),before)
        self.assertEqual(target_coord.db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
            (expired_claim_module._WBT_ABANDON_INTENTS,)).fetchone()[0],0)

    def test_completed_index_32_v7_same_id_reconciles_read_only(self): self._assert_completed_v7_same_id_reconciliation(32)
    def test_completed_index_33_v7_same_id_reconciles_read_only(self): self._assert_completed_v7_same_id_reconciliation(33)
    def test_completed_index_34_v7_same_id_reconciles_read_only(self): self._assert_completed_v7_same_id_reconciliation(34)
    def test_completed_index_35_v7_same_id_reconciles_read_only(self): self._assert_completed_v7_same_id_reconciliation(35)
    def test_completed_index_36_v7_same_id_reconciles_read_only(self): self._assert_completed_v7_same_id_reconciliation(36)
    def test_completed_index_37_v7_same_id_reconciles_read_only(self): self._assert_completed_v7_same_id_reconciliation(37)
    def test_completed_index_38_v7_same_id_reconciles_read_only(self): self._assert_completed_v7_same_id_reconciliation(38)
    def test_completed_index_39_v7_same_id_reconciles_read_only(self): self._assert_completed_v7_same_id_reconciliation(39)

    def test_completed_index_32_v7_fresh_uuid_denied_before_bootstrap(self): self._assert_completed_v7_fresh_uuid_denied(32)
    def test_completed_index_33_v7_fresh_uuid_denied_before_bootstrap(self): self._assert_completed_v7_fresh_uuid_denied(33)
    def test_completed_index_34_v7_fresh_uuid_denied_before_bootstrap(self): self._assert_completed_v7_fresh_uuid_denied(34)
    def test_completed_index_35_v7_fresh_uuid_denied_before_bootstrap(self): self._assert_completed_v7_fresh_uuid_denied(35)
    def test_completed_index_36_v7_fresh_uuid_denied_before_bootstrap(self): self._assert_completed_v7_fresh_uuid_denied(36)
    def test_completed_index_37_v7_fresh_uuid_denied_before_bootstrap(self): self._assert_completed_v7_fresh_uuid_denied(37)
    def test_completed_index_38_v7_fresh_uuid_denied_before_bootstrap(self): self._assert_completed_v7_fresh_uuid_denied(38)
    def test_completed_index_39_v7_fresh_uuid_denied_before_bootstrap(self): self._assert_completed_v7_fresh_uuid_denied(39)

    def test_wbt_gg1_file_class_nonmember_and_malformed_manifest_fail_closed(self):
        arguments,target_coord,target,chosen,candidates=self._wbt_gg1_file_class_arguments(32)
        before=self._database_dump(target)
        with self._wbt_gg1_file_class_context(arguments,candidates),\
                patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample') as native_sample:
            invalid=(arguments|{'resource':'file:.message-consumption-0055/not-selected.txt'},
                arguments|{'resource':'file:.message-consumption-0055/../outside.py'},
                arguments|{'owner_session':'session-other'},
                arguments|{'reservation_id':'reservation-other'},
                arguments|{'expected_deadline':arguments['expected_deadline']+1.0})
            for candidate_args in invalid:
                with self.assertRaisesRegex(CoordError,'fixed WBT cohort index and exact claim tuple'):
                    execute_expired_claim(**candidate_args)
            native_sample.assert_not_called()
            manifest_path=Path(expired_claim_module._WBT_GG1_FILE_CLASS_MANIFEST_PATH)
            raw=manifest_path.read_bytes()
            bad=b'{"manifest_version":1,"purpose":"tampered"}'
            manifest_path.write_bytes(bad)
            with patch.object(expired_claim_module,'_WBT_GG1_FILE_CLASS_MANIFEST_SHA256',
                              hashlib.sha256(bad).hexdigest()),\
                 patch.object(expired_claim_module,'_WBT_GG1_FILE_CLASS_MANIFEST_SIZE',len(bad)):
                with self.assertRaisesRegex(CoordError,'fixed WBT cohort index and exact claim tuple'):
                    execute_expired_claim(**arguments)
            native_sample.assert_not_called()
            manifest_path.write_bytes(raw)
        self.assertEqual(self._database_dump(target),before)
        self.assertEqual(target_coord.db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
            (expired_claim_module._WBT_ABANDON_INTENTS,)).fetchone()[0],0)

    def test_wbt_gg1_file_class_duplicate_and_source_row_drift_fail_closed(self):
        arguments,target_coord,target,chosen,candidates=self._wbt_gg1_file_class_arguments(32)
        before=self._database_dump(target)
        rows=self._wbt_file_class_rows(arguments,candidates)
        rows[chosen['original_index']]=(chosen['resource']+'-drift',chosen['owner_session'],
            chosen['reservation_id'],chosen['original_deadline'])
        with self._wbt_gg1_file_class_context(arguments,candidates,rows=rows),\
                patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample') as native_sample:
            with self.assertRaisesRegex(CoordError,'fixed WBT cohort index and exact claim tuple'):
                execute_expired_claim(**arguments)
            native_sample.assert_not_called()
        with self._wbt_gg1_file_class_context(arguments,candidates),\
                patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample') as native_sample:
            manifest_path=Path(expired_claim_module._WBT_GG1_FILE_CLASS_MANIFEST_PATH)
            original=manifest_path.read_bytes()
            duplicated=subprocess.run(['jq','-c','.candidates[1]=.candidates[0]'],
                input=original,stdout=subprocess.PIPE,stderr=subprocess.PIPE,check=True).stdout
            manifest_path.write_bytes(duplicated)
            with patch.object(expired_claim_module,'_WBT_GG1_FILE_CLASS_MANIFEST_SHA256',
                              hashlib.sha256(duplicated).hexdigest()),\
                 patch.object(expired_claim_module,'_WBT_GG1_FILE_CLASS_MANIFEST_SIZE',len(duplicated)):
                with self.assertRaisesRegex(CoordError,'fixed WBT cohort index and exact claim tuple'):
                    execute_expired_claim(**arguments)
            native_sample.assert_not_called()
        self.assertEqual(self._database_dump(target),before)
        self.assertEqual(target_coord.db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
            (expired_claim_module._WBT_ABANDON_INTENTS,)).fetchone()[0],0)

    def _assert_completed_v8_same_id_reconciliation(self, original_index):
        arguments,target_coord,target,chosen,candidates=self._wbt_hgide_file_class_arguments(original_index)
        arguments=arguments|{'expires_at':time.time()-0.1}
        self._assert_legacy_stored_outcome_is_same_id_read_only(
            arguments,target_coord,target,self._wbt_hgide_file_class_context(arguments,candidates),
            original_index,hgide_file_class_candidates=candidates)

    def _assert_completed_v8_fresh_uuid_denied(self, original_index):
        arguments,target_coord,target,chosen,candidates=self._wbt_hgide_file_class_arguments(original_index)
        arguments=arguments|{'request_id':'88888888-aaaa-4bbb-8ccc-888888888888'}
        before=self._database_dump(target)
        with self._wbt_hgide_file_class_context(arguments,candidates),\
                patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample') as native_sample:
            with self.assertRaisesRegex(CoordError,'same-ID reconciliation only'):
                execute_expired_claim(**arguments)
            native_sample.assert_not_called()
        self.assertEqual(self._database_dump(target),before)
        self.assertEqual(target_coord.db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
            (expired_claim_module._WBT_ABANDON_INTENTS,)).fetchone()[0],0)

    def test_hgide_index_40_v8_reconciles_read_only(self): self._assert_completed_v8_same_id_reconciliation(40)
    def test_hgide_index_41_v8_reconciles_read_only(self): self._assert_completed_v8_same_id_reconciliation(41)
    def test_hgide_index_42_v8_reconciles_read_only(self): self._assert_completed_v8_same_id_reconciliation(42)
    def test_hgide_index_43_v8_reconciles_read_only(self): self._assert_completed_v8_same_id_reconciliation(43)
    def test_hgide_index_44_v8_reconciles_read_only(self): self._assert_completed_v8_same_id_reconciliation(44)
    def test_hgide_index_45_v8_reconciles_read_only(self): self._assert_completed_v8_same_id_reconciliation(45)
    def test_hgide_index_46_v8_reconciles_read_only(self): self._assert_completed_v8_same_id_reconciliation(46)
    def test_hgide_index_47_v8_reconciles_read_only(self): self._assert_completed_v8_same_id_reconciliation(47)
    def test_hgide_index_40_v8_fresh_uuid_denied(self): self._assert_completed_v8_fresh_uuid_denied(40)
    def test_hgide_index_41_v8_fresh_uuid_denied(self): self._assert_completed_v8_fresh_uuid_denied(41)
    def test_hgide_index_42_v8_fresh_uuid_denied(self): self._assert_completed_v8_fresh_uuid_denied(42)
    def test_hgide_index_43_v8_fresh_uuid_denied(self): self._assert_completed_v8_fresh_uuid_denied(43)
    def test_hgide_index_44_v8_fresh_uuid_denied(self): self._assert_completed_v8_fresh_uuid_denied(44)
    def test_hgide_index_45_v8_fresh_uuid_denied(self): self._assert_completed_v8_fresh_uuid_denied(45)
    def test_hgide_index_46_v8_fresh_uuid_denied(self): self._assert_completed_v8_fresh_uuid_denied(46)
    def test_hgide_index_47_v8_fresh_uuid_denied(self): self._assert_completed_v8_fresh_uuid_denied(47)

    def _assert_6jc1_completed_same_id_readonly(self, original_index):
        arguments,target_coord,target,chosen,candidates=self._wbt_6jc1_file_class_arguments(original_index)
        arguments=arguments|{'expires_at':time.time()-0.1}
        self._assert_legacy_stored_outcome_is_same_id_read_only(
            arguments,target_coord,target,self._wbt_6jc1_file_class_context(arguments,candidates),
            original_index)

    def _assert_6jc1_completed_fresh_uuid_denied(self, original_index):
        arguments,target_coord,target,chosen,candidates=self._wbt_6jc1_file_class_arguments(original_index)
        arguments=arguments|{'request_id':'99999999-aaaa-4bbb-8ccc-999999999999'}
        before=self._database_dump(target)
        self.assertEqual(target_coord.db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
            (expired_claim_module._WBT_ABANDON_INTENTS,)).fetchone()[0],0)
        with self._wbt_6jc1_file_class_context(arguments,candidates),\
                patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample') as native_sample:
            with self.assertRaisesRegex(CoordError,'same-ID reconciliation only'):
                execute_expired_claim(**arguments)
            native_sample.assert_not_called()
        self.assertEqual(self._database_dump(target),before)
        self.assertEqual(target_coord.db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
            (expired_claim_module._WBT_ABANDON_INTENTS,)).fetchone()[0],0)

    def test_6jc1_index_48_reconciles_read_only(self): self._assert_6jc1_completed_same_id_readonly(48)
    def test_6jc1_synthetic_manifest_mechanism_accepts_only_exact_tuple(self):
        arguments,_,_,_,candidates=self._wbt_6jc1_file_class_arguments(48)
        selected=(expired_claim_module._WBT_WITNESS_VERSION,'default','root','host-root',
                  arguments['project'],arguments['resource'],arguments['owner_session'],
                  arguments['reservation_id'],arguments['expected_deadline'],_WBT_COHORT_REF)
        with self._wbt_6jc1_synthetic_context(arguments,candidates):
            self.assertEqual(expired_claim_module._wbt_6jc1_file_class_manifest_index(selected),48)
            self.assertIsNone(expired_claim_module._wbt_6jc1_file_class_manifest_index(
                selected[:7]+('wrong-reservation',)+selected[8:]))
            self.assertIsNone(expired_claim_module._wbt_6jc1_file_class_manifest_index(
                selected[:6]+('wrong-owner',)+selected[7:]))
            with patch.object(expired_claim_module,'_WBT_6JC1_FILE_CLASS_SOURCE_PATH','/synthetic/wrong'):
                self.assertIsNone(expired_claim_module._wbt_6jc1_file_class_manifest_index(selected))
            path=Path(expired_claim_module._WBT_6JC1_FILE_CLASS_MANIFEST_PATH)
            path.write_bytes(path.read_bytes()+b' ')
            self.assertIsNone(expired_claim_module._wbt_6jc1_file_class_manifest_index(selected))
    def test_6jc1_production_pins_reject_synthetic_bytes(self):
        self.assertEqual(self.production_manifest_paths['_WBT_6JC1_FILE_CLASS_MANIFEST_PATH'],
                         '/private/tmp/wbt-follow-on-file-class-selection-20261018-v7.json')
        self.assertEqual(expired_claim_module._WBT_6JC1_FILE_CLASS_SOURCE_PATH,
                         '/private/tmp/wbt-295-row-linked-classification-v1.json')
        self.assertEqual((expired_claim_module._WBT_6JC1_FILE_CLASS_MANIFEST_SIZE,
                          expired_claim_module._WBT_6JC1_FILE_CLASS_SOURCE_SIZE,
                          expired_claim_module._WBT_6JC1_FILE_CLASS_SOURCE_ROWS),(26045,194542,295))
        self.assertEqual(expired_claim_module._WBT_6JC1_FILE_CLASS_MANIFEST_SHA256,
                         '7b73a6647d437f008a1ac8dde251368fe1c75bc398656298ed6db98a4faa2874')
        self.assertEqual(expired_claim_module._WBT_6JC1_FILE_CLASS_SOURCE_SHA256,
                         '84e002a8e4809c8d2f520255600ea223746c158ea8bbc1e42f2137fa11eba691')
        arguments,_,_,_,candidates=self._wbt_6jc1_file_class_arguments(48)
        selected=(expired_claim_module._WBT_WITNESS_VERSION,'default','root','host-root',
                  arguments['project'],arguments['resource'],arguments['owner_session'],
                  arguments['reservation_id'],arguments['expected_deadline'],_WBT_COHORT_REF)
        path=self.master.parent/'synthetic-not-historical.json';path.write_bytes(b'{}')
        with patch.object(expired_claim_module,'_WBT_6JC1_FILE_CLASS_MANIFEST_PATH',str(path)):
            self.assertIsNone(expired_claim_module._wbt_6jc1_file_class_manifest_index(selected))
    def test_6jc1_same_id_changed_payload_never_rewrites_or_samples(self):
        arguments,target_coord,target,_,candidates=self._wbt_6jc1_file_class_arguments(48)
        with self._wbt_6jc1_file_class_context(arguments,candidates),\
                patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample') as native_sample:
            payload,_=self._seed_synthetic_applied_legacy_intent(arguments,target_coord,48)
            before=self._database_dump(target)
            with self.assertRaisesRegex(CoordError,'request UUID is bound to a different immutable payload'):
                execute_expired_claim(**(arguments|{'expires_at':arguments['expires_at']+1}))
            native_sample.assert_not_called()
            self.assertEqual(self._database_dump(target),before)
            self.assertEqual(target_coord.db.execute('SELECT payload FROM root_wbt_file_abandon_intents WHERE request_id=?',(arguments['request_id'],)).fetchone()['payload'],payload)
    def test_6jc1_synthetic_fresh_uuid_remains_denied(self):
        with patch.object(self,'_wbt_6jc1_file_class_context',side_effect=self._wbt_6jc1_synthetic_context):
            self._assert_6jc1_completed_fresh_uuid_denied(48)
    def test_6jc1_synthetic_invalid_scope_remains_denied(self):
        with patch.object(self,'_wbt_6jc1_file_class_context',side_effect=self._wbt_6jc1_synthetic_context):
            self.test_6jc1_file_class_nonmember_and_bad_manifest_fail_closed()
    def test_6jc1_index_49_reconciles_read_only(self): self._assert_6jc1_completed_same_id_readonly(49)
    def test_6jc1_index_50_reconciles_read_only(self): self._assert_6jc1_completed_same_id_readonly(50)
    def test_6jc1_index_51_reconciles_read_only(self): self._assert_6jc1_completed_same_id_readonly(51)
    def test_6jc1_index_52_reconciles_read_only(self): self._assert_6jc1_completed_same_id_readonly(52)
    def test_6jc1_index_53_reconciles_read_only(self): self._assert_6jc1_completed_same_id_readonly(53)
    def test_6jc1_index_54_reconciles_read_only(self): self._assert_6jc1_completed_same_id_readonly(54)
    def test_6jc1_index_55_reconciles_read_only(self): self._assert_6jc1_completed_same_id_readonly(55)
    def test_6jc1_index_48_rejects_fresh_uuid(self): self._assert_6jc1_completed_fresh_uuid_denied(48)
    def test_6jc1_index_49_rejects_fresh_uuid(self): self._assert_6jc1_completed_fresh_uuid_denied(49)
    def test_6jc1_index_50_rejects_fresh_uuid(self): self._assert_6jc1_completed_fresh_uuid_denied(50)
    def test_6jc1_index_51_rejects_fresh_uuid(self): self._assert_6jc1_completed_fresh_uuid_denied(51)
    def test_6jc1_index_52_rejects_fresh_uuid(self): self._assert_6jc1_completed_fresh_uuid_denied(52)
    def test_6jc1_index_53_rejects_fresh_uuid(self): self._assert_6jc1_completed_fresh_uuid_denied(53)
    def test_6jc1_index_54_rejects_fresh_uuid(self): self._assert_6jc1_completed_fresh_uuid_denied(54)
    def test_6jc1_index_55_rejects_fresh_uuid(self): self._assert_6jc1_completed_fresh_uuid_denied(55)

    def _assert_hqg_file_class_candidate_lifecycle(self, original_index):
        arguments,target_coord,target,chosen,candidates=self._wbt_hqg_file_class_arguments(original_index)
        with self._wbt_hqg_file_class_context(arguments,candidates),\
                patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample',
                      side_effect=self._passive_not_loaded_sample) as native_sample:
            selected=(expired_claim_module._WBT_WITNESS_VERSION,'default','root','host-root',
                arguments['project'],arguments['resource'],arguments['owner_session'],
                arguments['reservation_id'],arguments['expected_deadline'],_WBT_COHORT_REF)
            fixture_manifest_digest=expired_claim_module._WBT_HQG_FILE_CLASS_MANIFEST_SHA256
            self.assertEqual(expired_claim_module._wbt_hqg_file_class_manifest_index(selected),original_index)
            self.assertEqual(expired_claim_module._wbt_pinned_artifact_membership(
                selected,row_index=original_index),'member')
            prepared=dict(execute_expired_claim(**arguments))
            prepared_replay=dict(execute_expired_claim(**arguments))
            apply_args=arguments|{'action':'wbt-abandon-apply',
                'expected_digest':prepared['prepared_digest']}
            applied=dict(execute_expired_claim(**apply_args))
            applied_replay=dict(execute_expired_claim(**apply_args))
            self.assertEqual(native_sample.call_count,2)
        self.assertEqual((prepared['status'],prepared['replayed']),('prepared',False))
        self.assertEqual((prepared_replay['status'],prepared_replay['replayed']),('prepared',True))
        self.assertEqual((applied['status'],applied['replayed']),('applied',False))
        self.assertEqual((applied_replay['status'],applied_replay['replayed']),('applied',True))
        self.assertEqual(applied['process_closure'],'UNVERIFIED')
        self.assertEqual(applied['eligibility'],'NOT_DEMONSTRATED')
        row=target_coord.db.execute(
            'SELECT owner_session,reservation_id,deadline,stale FROM resources WHERE name=?',
            (chosen['resource'],)).fetchone()
        self.assertEqual(tuple(row),(None,None,None,0))
        intent=target_coord.db.execute(
            'SELECT payload,state FROM root_wbt_file_abandon_intents WHERE request_id=?',
            (arguments['request_id'],)).fetchone()
        self.assertTrue(intent['payload'].startswith("('wbt-file-abandon-v10', "))
        self.assertIn(fixture_manifest_digest,intent['payload'])
        self.assertIn(str(original_index),intent['payload'])
        self.assertEqual(intent['state'],'applied')
        self.assertEqual(target_coord.db.execute(
            'SELECT count(*) FROM root_expired_claim_dispositions WHERE request_id=?',
            (arguments['request_id'],)).fetchone()[0],1)
        states=tuple(row[0] for row in target_coord.db.execute(
            'SELECT state FROM root_wbt_file_abandon_events WHERE request_id=? ORDER BY seq',
            (arguments['request_id'],)))
        self.assertEqual(states,('accepted','prepared','applying','applied'))
        self.assertEqual(target_coord.db.execute('SELECT active FROM root_team_stops WHERE id=?',
            (self.STOP,)).fetchone()[0],1)
        self.assertTrue(Path(target).exists())

    def test_hqg_file_class_index_56_uses_v10_lifecycle(self): self._assert_hqg_file_class_candidate_lifecycle(56)
    def test_hqg_file_class_index_57_uses_v10_lifecycle(self): self._assert_hqg_file_class_candidate_lifecycle(57)
    def test_hqg_file_class_index_58_uses_v10_lifecycle(self): self._assert_hqg_file_class_candidate_lifecycle(58)
    def test_hqg_file_class_index_59_uses_v10_lifecycle(self): self._assert_hqg_file_class_candidate_lifecycle(59)
    def test_hqg_file_class_index_60_uses_v10_lifecycle(self): self._assert_hqg_file_class_candidate_lifecycle(60)
    def test_hqg_file_class_index_61_uses_v10_lifecycle(self): self._assert_hqg_file_class_candidate_lifecycle(61)
    def test_hqg_file_class_index_62_uses_v10_lifecycle(self): self._assert_hqg_file_class_candidate_lifecycle(62)
    def test_hqg_file_class_index_63_uses_v10_lifecycle(self): self._assert_hqg_file_class_candidate_lifecycle(63)

    def test_hqg_file_class_nonmember_drift_and_bad_manifest_fail_closed(self):
        arguments,target_coord,target,chosen,candidates=self._wbt_hqg_file_class_arguments(56)
        before=self._database_dump(target)
        with self._wbt_hqg_file_class_context(arguments,candidates),\
                patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample') as native_sample:
            for invalid in (arguments|{'resource':'file:.operator-message-ui-0065/not-selected.py'},
                            arguments|{'resource':'file:.operator-message-ui-0065/../outside.py'},
                            arguments|{'owner_session':'session-other'},
                            arguments|{'reservation_id':'reservation-other'},
                            arguments|{'expected_deadline':arguments['expected_deadline']+1.0}):
                with self.assertRaisesRegex(CoordError,'fixed WBT cohort index and exact claim tuple'):
                    execute_expired_claim(**invalid)
            native_sample.assert_not_called()
            manifest_path=Path(expired_claim_module._WBT_HQG_FILE_CLASS_MANIFEST_PATH)
            original=manifest_path.read_bytes()
            malformed=subprocess.run(['jq','-cn','{manifest_version:1,purpose:"tampered"}'],
                                     capture_output=True,check=True).stdout
            manifest_path.write_bytes(malformed)
            with patch.object(expired_claim_module,'_WBT_HQG_FILE_CLASS_MANIFEST_SHA256',
                              hashlib.sha256(malformed).hexdigest()),\
                 patch.object(expired_claim_module,'_WBT_HQG_FILE_CLASS_MANIFEST_SIZE',len(malformed)):
                with self.assertRaisesRegex(CoordError,'fixed WBT cohort index and exact claim tuple'):
                    execute_expired_claim(**arguments)
            manifest_path.write_bytes(original)
            native_sample.assert_not_called()
        self.assertEqual(self._database_dump(target),before)
        self.assertEqual(target_coord.db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
            (expired_claim_module._WBT_ABANDON_INTENTS,)).fetchone()[0],0)

    def test_6jc1_file_class_nonmember_and_bad_manifest_fail_closed(self):
        arguments,target_coord,target,chosen,candidates=self._wbt_6jc1_file_class_arguments(48)
        before=self._database_dump(target)
        with self._wbt_6jc1_file_class_context(arguments,candidates),\
                patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample') as native_sample:
            for invalid in (arguments|{'resource':'file:.operator-message-ui-0065/not-selected.py'},
                            arguments|{'owner_session':'session-other'},
                            arguments|{'reservation_id':'reservation-other'},
                            arguments|{'expected_deadline':arguments['expected_deadline']+1.0}):
                with self.assertRaisesRegex(CoordError,'fixed WBT cohort index and exact claim tuple'):
                    execute_expired_claim(**invalid)
            native_sample.assert_not_called()
            path=Path(expired_claim_module._WBT_6JC1_FILE_CLASS_MANIFEST_PATH)
            original=path.read_bytes()
            duplicated=subprocess.run(['jq','-c','.candidates[1]=.candidates[0]'],
                input=original,stdout=subprocess.PIPE,stderr=subprocess.PIPE,check=True).stdout
            path.write_bytes(duplicated)
            with patch.object(expired_claim_module,'_WBT_6JC1_FILE_CLASS_MANIFEST_SHA256',
                              hashlib.sha256(duplicated).hexdigest()),\
                 patch.object(expired_claim_module,'_WBT_6JC1_FILE_CLASS_MANIFEST_SIZE',len(duplicated)):
                with self.assertRaisesRegex(CoordError,'fixed WBT cohort index and exact claim tuple'):
                    execute_expired_claim(**arguments)
            path.write_bytes(original)
            malformed=b'{"manifest_version":1,"purpose":"tampered"}'
            path.write_bytes(malformed)
            with patch.object(expired_claim_module,'_WBT_6JC1_FILE_CLASS_MANIFEST_SHA256',
                              hashlib.sha256(malformed).hexdigest()),\
                 patch.object(expired_claim_module,'_WBT_6JC1_FILE_CLASS_MANIFEST_SIZE',len(malformed)):
                with self.assertRaisesRegex(CoordError,'fixed WBT cohort index and exact claim tuple'):
                    execute_expired_claim(**arguments)
            path.write_bytes(original)
            native_sample.assert_not_called()
        self.assertEqual(self._database_dump(target),before)
        self.assertEqual(target_coord.db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
            (expired_claim_module._WBT_ABANDON_INTENTS,)).fetchone()[0],0)

        rows=self._wbt_file_class_rows(arguments,candidates)
        rows[chosen['original_index']]=(chosen['resource']+'-drift',chosen['owner_session'],
            chosen['reservation_id'],chosen['original_deadline'])
        with self._wbt_6jc1_file_class_context(arguments,candidates,rows=rows),\
                patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample') as native_sample:
            with self.assertRaisesRegex(CoordError,'fixed WBT cohort index and exact claim tuple'):
                execute_expired_claim(**arguments)
            native_sample.assert_not_called()
        self.assertEqual(self._database_dump(target),before)
        self.assertEqual(target_coord.db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
            (expired_claim_module._WBT_ABANDON_INTENTS,)).fetchone()[0],0)

    def test_hgide_file_class_nonmember_and_bad_manifest_fail_closed(self):
        arguments,target_coord,target,chosen,candidates=self._wbt_hgide_file_class_arguments(40)
        before=self._database_dump(target)
        with self._wbt_hgide_file_class_context(arguments,candidates),\
                patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample') as native_sample:
            invalid=(arguments|{'resource':'file:.multi-root-model-contract-0062/not-selected.py'},
                arguments|{'resource':'file:.multi-root-model-contract-0062/../outside.py'},
                arguments|{'owner_session':'session-other'},
                arguments|{'reservation_id':'reservation-other'},
                arguments|{'expected_deadline':arguments['expected_deadline']+1.0})
            for candidate_args in invalid:
                with self.assertRaisesRegex(CoordError,'fixed WBT cohort index and exact claim tuple'):
                    execute_expired_claim(**candidate_args)
            native_sample.assert_not_called()
            path=Path(expired_claim_module._WBT_HGIDE_FILE_CLASS_MANIFEST_PATH)
            original=path.read_bytes()
            malformed=b'{"manifest_version":1,"purpose":"tampered"}'
            path.write_bytes(malformed)
            with patch.object(expired_claim_module,'_WBT_HGIDE_FILE_CLASS_MANIFEST_SHA256',
                              hashlib.sha256(malformed).hexdigest()),\
                 patch.object(expired_claim_module,'_WBT_HGIDE_FILE_CLASS_MANIFEST_SIZE',len(malformed)):
                with self.assertRaisesRegex(CoordError,'fixed WBT cohort index and exact claim tuple'):
                    execute_expired_claim(**arguments)
            path.write_bytes(original)
            native_sample.assert_not_called()
        self.assertEqual(self._database_dump(target),before)
        self.assertEqual(target_coord.db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
            (expired_claim_module._WBT_ABANDON_INTENTS,)).fetchone()[0],0)

    def test_hgide_file_class_duplicate_and_source_row_drift_fail_closed(self):
        arguments,target_coord,target,chosen,candidates=self._wbt_hgide_file_class_arguments(40)
        before=self._database_dump(target)
        rows=self._wbt_file_class_rows(arguments,candidates)
        rows[chosen['original_index']]=(chosen['resource']+'-drift',chosen['owner_session'],
            chosen['reservation_id'],chosen['original_deadline'])
        with self._wbt_hgide_file_class_context(arguments,candidates,rows=rows),\
                patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample') as native_sample:
            with self.assertRaisesRegex(CoordError,'fixed WBT cohort index and exact claim tuple'):
                execute_expired_claim(**arguments)
            native_sample.assert_not_called()
        with self._wbt_hgide_file_class_context(arguments,candidates),\
                patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample') as native_sample:
            path=Path(expired_claim_module._WBT_HGIDE_FILE_CLASS_MANIFEST_PATH)
            original=path.read_bytes()
            duplicated=subprocess.run(['jq','-c','.candidates[1]=.candidates[0]'],
                input=original,stdout=subprocess.PIPE,stderr=subprocess.PIPE,check=True).stdout
            path.write_bytes(duplicated)
            with patch.object(expired_claim_module,'_WBT_HGIDE_FILE_CLASS_MANIFEST_SHA256',
                              hashlib.sha256(duplicated).hexdigest()),\
                 patch.object(expired_claim_module,'_WBT_HGIDE_FILE_CLASS_MANIFEST_SIZE',len(duplicated)):
                with self.assertRaisesRegex(CoordError,'fixed WBT cohort index and exact claim tuple'):
                    execute_expired_claim(**arguments)
            path.write_bytes(original)
            native_sample.assert_not_called()
        self.assertEqual(self._database_dump(target),before)
        self.assertEqual(target_coord.db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
            (expired_claim_module._WBT_ABANDON_INTENTS,)).fetchone()[0],0)

    def _assert_completed_v5_same_id_reconciliation(self, original_index):
        arguments,target_coord,target,chosen,candidates=self._wbt_file_class_arguments(original_index)
        arguments=arguments|{'expires_at':time.time()-0.1}
        artifact=self._wbt_file_class_context(arguments,candidates)
        self._assert_legacy_stored_outcome_is_same_id_read_only(
            arguments,target_coord,target,artifact,original_index,
            file_class_candidates=candidates)

    def test_completed_index_16_v5_same_id_reconciles_read_only(self):
        self._assert_completed_v5_same_id_reconciliation(16)

    def test_completed_index_17_v5_same_id_reconciles_read_only(self):
        self._assert_completed_v5_same_id_reconciliation(17)

    def test_completed_index_18_v5_same_id_reconciles_read_only(self):
        self._assert_completed_v5_same_id_reconciliation(18)

    def test_completed_index_19_v5_same_id_reconciles_read_only(self):
        self._assert_completed_v5_same_id_reconciliation(19)

    def test_completed_index_20_v5_same_id_reconciles_read_only(self):
        self._assert_completed_v5_same_id_reconciliation(20)

    def test_completed_index_21_v5_same_id_reconciles_read_only(self):
        self._assert_completed_v5_same_id_reconciliation(21)

    def test_completed_index_22_v5_same_id_reconciles_read_only(self):
        self._assert_completed_v5_same_id_reconciliation(22)

    def test_completed_index_23_v5_same_id_reconciles_read_only(self):
        self._assert_completed_v5_same_id_reconciliation(23)

    def _assert_completed_v5_fresh_uuid_denied(self, original_index):
        arguments,target_coord,target,chosen,candidates=self._wbt_file_class_arguments(original_index)
        arguments=arguments|{'request_id':'66666666-aaaa-4bbb-8ccc-666666666666'}
        before=self._database_dump(target)
        with self._wbt_file_class_context(arguments,candidates),\
                patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample') as native_sample:
            with self.assertRaisesRegex(CoordError,'same-ID reconciliation only'):
                execute_expired_claim(**arguments)
            native_sample.assert_not_called()
        self.assertEqual(self._database_dump(target),before)
        self.assertEqual(target_coord.db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
            (expired_claim_module._WBT_ABANDON_INTENTS,)).fetchone()[0],0)

    def test_completed_index_16_v5_fresh_uuid_denied_before_bootstrap(self):
        self._assert_completed_v5_fresh_uuid_denied(16)

    def test_completed_index_17_v5_fresh_uuid_denied_before_bootstrap(self):
        self._assert_completed_v5_fresh_uuid_denied(17)

    def test_completed_index_18_v5_fresh_uuid_denied_before_bootstrap(self):
        self._assert_completed_v5_fresh_uuid_denied(18)

    def test_completed_index_19_v5_fresh_uuid_denied_before_bootstrap(self):
        self._assert_completed_v5_fresh_uuid_denied(19)

    def test_completed_index_20_v5_fresh_uuid_denied_before_bootstrap(self):
        self._assert_completed_v5_fresh_uuid_denied(20)

    def test_completed_index_21_v5_fresh_uuid_denied_before_bootstrap(self):
        self._assert_completed_v5_fresh_uuid_denied(21)

    def test_completed_index_22_v5_fresh_uuid_denied_before_bootstrap(self):
        self._assert_completed_v5_fresh_uuid_denied(22)

    def test_completed_index_23_v5_fresh_uuid_denied_before_bootstrap(self):
        self._assert_completed_v5_fresh_uuid_denied(23)

    def _assert_completed_v6_same_id_reconciliation(self, original_index):
        arguments,target_coord,target,chosen,candidates=self._wbt_next_file_class_arguments(original_index)
        arguments=arguments|{'expires_at':time.time()-0.1}
        artifact=self._wbt_next_file_class_context(arguments,candidates)
        self._assert_legacy_stored_outcome_is_same_id_read_only(
            arguments,target_coord,target,artifact,original_index,
            next_file_class_candidates=candidates)

    def test_next_file_class_synthetic_validator_rejects_wrong_owner_and_tamper(self):
        arguments,_,_,_,candidates=self._wbt_next_file_class_arguments(24)
        production_digest=expired_claim_module._WBT_NEXT_FILE_CLASS_MANIFEST_SHA256
        selected=(expired_claim_module._WBT_WITNESS_VERSION,'default','root','host-root',
                  arguments['project'],arguments['resource'],arguments['owner_session'],
                  arguments['reservation_id'],arguments['expected_deadline'],_WBT_COHORT_REF)
        with self._wbt_next_file_class_context(arguments,candidates):
            self.assertNotEqual(expired_claim_module._WBT_NEXT_FILE_CLASS_MANIFEST_SHA256,production_digest)
            self.assertEqual(expired_claim_module._wbt_next_file_class_manifest_index(selected),24)
            self.assertIsNone(expired_claim_module._wbt_next_file_class_manifest_index(
                selected[:6]+('wrong-owner',)+selected[7:]))
            path=Path(expired_claim_module._WBT_NEXT_FILE_CLASS_MANIFEST_PATH)
            path.write_bytes(path.read_bytes()+b' ')
            self.assertIsNone(expired_claim_module._wbt_next_file_class_manifest_index(selected))

    def test_synthetic_manifest_failed_enter_restores_cohort_pins(self):
        arguments,_,_,_,candidates=self._wbt_next_file_class_arguments(24)
        names=('_WBT_COHORT_ARTIFACT_PATH','_WBT_COHORT_ARTIFACT_SHA256',
               '_WBT_COHORT_ARTIFACT_SIZE','_WBT_COHORT_ROW_COUNT')
        before=tuple(getattr(expired_claim_module,name) for name in names)
        with patch.object(Path,'read_bytes',side_effect=OSError('synthetic fixture read failure')):
            with self.assertRaisesRegex(OSError,'synthetic fixture read failure'):
                with self._wbt_next_file_class_context(arguments,candidates):
                    self.fail('failed fixture entry must not succeed')
        self.assertEqual(tuple(getattr(expired_claim_module,name) for name in names),before)

    def test_completed_index_24_v6_same_id_reconciles_read_only(self):
        self._assert_completed_v6_same_id_reconciliation(24)

    def test_completed_index_25_v6_same_id_reconciles_read_only(self):
        self._assert_completed_v6_same_id_reconciliation(25)

    def test_completed_index_26_v6_same_id_reconciles_read_only(self):
        self._assert_completed_v6_same_id_reconciliation(26)

    def test_completed_index_27_v6_same_id_reconciles_read_only(self):
        self._assert_completed_v6_same_id_reconciliation(27)

    def test_completed_index_28_v6_same_id_reconciles_read_only(self):
        self._assert_completed_v6_same_id_reconciliation(28)

    def test_completed_index_29_v6_same_id_reconciles_read_only(self):
        self._assert_completed_v6_same_id_reconciliation(29)

    def test_completed_index_30_v6_same_id_reconciles_read_only(self):
        self._assert_completed_v6_same_id_reconciliation(30)

    def test_completed_index_31_v6_same_id_reconciles_read_only(self):
        self._assert_completed_v6_same_id_reconciliation(31)

    def _assert_completed_v6_fresh_uuid_denied(self, original_index):
        arguments,target_coord,target,chosen,candidates=self._wbt_next_file_class_arguments(original_index)
        arguments=arguments|{'request_id':'77777777-aaaa-4bbb-8ccc-777777777777'}
        before=self._database_dump(target)
        self.assertEqual(target_coord.db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
            (expired_claim_module._WBT_ABANDON_INTENTS,)).fetchone()[0],0)
        with self._wbt_next_file_class_context(arguments,candidates),\
                patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample') as native_sample:
            with self.assertRaisesRegex(CoordError,'same-ID reconciliation only'):
                execute_expired_claim(**arguments)
            native_sample.assert_not_called()
        self.assertEqual(self._database_dump(target),before)
        self.assertEqual(target_coord.db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
            (expired_claim_module._WBT_ABANDON_INTENTS,)).fetchone()[0],0)

    def test_completed_index_24_v6_fresh_uuid_denied_before_bootstrap(self):
        self._assert_completed_v6_fresh_uuid_denied(24)

    def test_completed_index_25_v6_fresh_uuid_denied_before_bootstrap(self):
        self._assert_completed_v6_fresh_uuid_denied(25)

    def test_completed_index_26_v6_fresh_uuid_denied_before_bootstrap(self):
        self._assert_completed_v6_fresh_uuid_denied(26)

    def test_completed_index_27_v6_fresh_uuid_denied_before_bootstrap(self):
        self._assert_completed_v6_fresh_uuid_denied(27)

    def test_completed_index_28_v6_fresh_uuid_denied_before_bootstrap(self):
        self._assert_completed_v6_fresh_uuid_denied(28)

    def test_completed_index_29_v6_fresh_uuid_denied_before_bootstrap(self):
        self._assert_completed_v6_fresh_uuid_denied(29)

    def test_completed_index_30_v6_fresh_uuid_denied_before_bootstrap(self):
        self._assert_completed_v6_fresh_uuid_denied(30)

    def test_completed_index_31_v6_fresh_uuid_denied_before_bootstrap(self):
        self._assert_completed_v6_fresh_uuid_denied(31)

    def test_wbt_file_class_nonmember_and_malformed_manifest_fail_before_effect(self):
        arguments,target_coord,target,chosen,candidates=self._wbt_file_class_arguments(16)
        before=self._database_dump(target)
        with self._wbt_file_class_context(arguments,candidates),\
                patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample') as native_sample:
            bad_arguments=(arguments|{'resource':'file:.lease-maintenance-0061/unselected.txt'},
                arguments|{'resource':'file:.lease-maintenance-0061/../outside.py'},
                arguments|{'owner_session':'session-other'},
                arguments|{'reservation_id':'reservation-other'},
                arguments|{'expected_deadline':arguments['expected_deadline']+1.0})
            for invalid in bad_arguments:
                with self.assertRaisesRegex(CoordError,'fixed WBT cohort index and exact claim tuple'):
                    execute_expired_claim(**invalid)
            native_sample.assert_not_called()
            manifest_path=Path(expired_claim_module._WBT_FILE_CLASS_MANIFEST_PATH)
            raw=manifest_path.read_bytes()
            bad=b'{"manifest_version":1,"purpose":"tampered"}'
            manifest_path.write_bytes(bad)
            with patch.object(expired_claim_module,'_WBT_FILE_CLASS_MANIFEST_SHA256',
                              hashlib.sha256(bad).hexdigest()),\
                 patch.object(expired_claim_module,'_WBT_FILE_CLASS_MANIFEST_SIZE',len(bad)):
                with self.assertRaisesRegex(CoordError,'fixed WBT cohort index and exact claim tuple'):
                    execute_expired_claim(**arguments)
            native_sample.assert_not_called()
            manifest_path.write_bytes(raw)
        self.assertEqual(self._database_dump(target),before)
        self.assertEqual(target_coord.db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
            (expired_claim_module._WBT_ABANDON_INTENTS,)).fetchone()[0],0)

    def test_wbt_file_class_duplicate_manifest_and_cohort_row_drift_fail_before_effect(self):
        arguments,target_coord,target,chosen,candidates=self._wbt_file_class_arguments(16)
        before=self._database_dump(target)
        rows=self._wbt_file_class_rows(arguments,candidates)
        rows[chosen['original_index']]=(chosen['resource']+'-drift',chosen['owner_session'],
            chosen['reservation_id'],chosen['original_deadline'])
        with self._wbt_file_class_context(arguments,candidates,rows=rows),\
                patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample') as native_sample:
            with self.assertRaisesRegex(CoordError,'fixed WBT cohort index and exact claim tuple'):
                execute_expired_claim(**arguments)
            native_sample.assert_not_called()
        with self._wbt_file_class_context(arguments,candidates),\
                patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample') as native_sample:
            manifest_path=Path(expired_claim_module._WBT_FILE_CLASS_MANIFEST_PATH)
            original=manifest_path.read_bytes()
            duplicated=subprocess.run(['jq','-c','.candidates[1]=.candidates[0]'],
                input=original,stdout=subprocess.PIPE,stderr=subprocess.PIPE,check=True).stdout
            manifest_path.write_bytes(duplicated)
            with patch.object(expired_claim_module,'_WBT_FILE_CLASS_MANIFEST_SHA256',
                              hashlib.sha256(duplicated).hexdigest()),\
                 patch.object(expired_claim_module,'_WBT_FILE_CLASS_MANIFEST_SIZE',len(duplicated)):
                with self.assertRaisesRegex(CoordError,'fixed WBT cohort index and exact claim tuple'):
                    execute_expired_claim(**arguments)
            native_sample.assert_not_called()
        self.assertEqual(self._database_dump(target),before)
        self.assertEqual(target_coord.db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
            (expired_claim_module._WBT_ABANDON_INTENTS,)).fetchone()[0],0)

    def _assert_old_group_new_uuid_denied(self, original_index):
        arguments,target_coord,target,artifact,candidates,chosen=self._wbt_group_arguments(original_index)
        before=self._database_dump(target)
        with artifact,patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample') as native_sample:
            with self.assertRaisesRegex(CoordError,'same-ID reconciliation only'):
                execute_expired_claim(**arguments)
            native_sample.assert_not_called()
        self.assertEqual(self._database_dump(target),before)
        self.assertEqual(target_coord.db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
            (expired_claim_module._WBT_ABANDON_INTENTS,)).fetchone()[0],0)

    def test_completed_index_6_rejects_fresh_uuid_before_bootstrap(self):
        self._assert_old_group_new_uuid_denied(6)

    def test_completed_index_7_rejects_fresh_uuid_before_bootstrap(self):
        self._assert_old_group_new_uuid_denied(7)

    def test_completed_index_8_rejects_fresh_uuid_before_bootstrap(self):
        self._assert_old_group_new_uuid_denied(8)

    def test_completed_index_9_rejects_fresh_uuid_before_bootstrap(self):
        self._assert_old_group_new_uuid_denied(9)

    def test_completed_index_10_rejects_fresh_uuid_before_bootstrap(self):
        self._assert_old_group_new_uuid_denied(10)

    def _assert_old_group_same_id_reconciliation(self, original_index):
        arguments,target_coord,target,artifact,candidates,chosen=self._wbt_group_arguments(original_index)
        self._assert_legacy_stored_outcome_is_same_id_read_only(
            arguments,target_coord,target,artifact,original_index,group_candidates=candidates)

    def test_completed_index_6_v3_outcome_reconciles_without_sampling_or_rewrite(self):
        self._assert_old_group_same_id_reconciliation(6)

    def test_completed_index_7_v3_outcome_reconciles_without_sampling_or_rewrite(self):
        self._assert_old_group_same_id_reconciliation(7)

    def test_completed_index_8_v3_outcome_reconciles_without_sampling_or_rewrite(self):
        self._assert_old_group_same_id_reconciliation(8)

    def test_completed_index_9_v3_outcome_reconciles_without_sampling_or_rewrite(self):
        self._assert_old_group_same_id_reconciliation(9)

    def test_completed_index_10_v3_outcome_reconciles_without_sampling_or_rewrite(self):
        self._assert_old_group_same_id_reconciliation(10)

    def test_wbt_next_group_nonmembers_and_manifest_tamper_fail_before_target_mutation(self):
        arguments,target_coord,target,artifact,candidates,chosen=self._wbt_next_group_arguments(11)
        before=self._database_dump(target)
        bad_arguments=(
            arguments|{'resource':'file:.finite-batch-64-0081/not-selected.txt'},
            arguments|{'resource':'heavy-unity:2'},
            arguments|{'owner_session':'session-other'},
            arguments|{'reservation_id':'reservation-other'},
            arguments|{'expected_deadline':arguments['expected_deadline']+1.0},
        )
        with self._wbt_next_group_artifact_context(arguments,candidates),\
                patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample') as native_sample:
            for invalid in bad_arguments:
                with self.assertRaisesRegex(CoordError,'fixed WBT cohort index and exact claim tuple'):
                    execute_expired_claim(**invalid)
            native_sample.assert_not_called()
            manifest_path=Path(expired_claim_module._WBT_NEXT_GROUP_MANIFEST_PATH)
            tampered=b'{"manifest_version":1,"purpose":"tampered"}'
            manifest_path.write_bytes(tampered)
            with patch.object(expired_claim_module,'_WBT_NEXT_GROUP_MANIFEST_SHA256',
                              hashlib.sha256(tampered).hexdigest()),\
                 patch.object(expired_claim_module,'_WBT_NEXT_GROUP_MANIFEST_SIZE',len(tampered)):
                with self.assertRaisesRegex(CoordError,'fixed WBT cohort index and exact claim tuple'):
                    execute_expired_claim(**arguments)
            native_sample.assert_not_called()
        self.assertEqual(self._database_dump(target),before)
        self.assertEqual(target_coord.db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
            (expired_claim_module._WBT_ABANDON_INTENTS,)).fetchone()[0],0)

    def _assert_old_follow_on_new_uuid_denied(self, original_index):
        arguments,target_coord,target,artifact,candidates,chosen=self._wbt_follow_on_arguments(original_index)
        before=self._database_dump(target)
        with artifact:
            manifest=self._wbt_follow_on_manifest(candidates)
            with manifest,patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                    patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                    patch('agent_chat.expired_claim_disposition._sample') as native_sample:
                selected=(expired_claim_module._WBT_WITNESS_VERSION,'default','root','host-root',
                    arguments['project'],arguments['resource'],arguments['owner_session'],
                    arguments['reservation_id'],arguments['expected_deadline'],_WBT_COHORT_REF)
                self.assertEqual(expired_claim_module._wbt_follow_on_manifest_index(selected),original_index)
                with self.assertRaisesRegex(CoordError,'same-ID reconciliation only'):
                    execute_expired_claim(**arguments)
                native_sample.assert_not_called()
        self.assertEqual(self._database_dump(target),before)
        self.assertEqual(target_coord.db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
            (expired_claim_module._WBT_ABANDON_INTENTS,)).fetchone()[0],0)

    def test_completed_index_3_legacy_payload_rejects_new_uuid(self):
        self._assert_old_follow_on_new_uuid_denied(3)

    def test_completed_index_4_legacy_payload_rejects_new_uuid(self):
        self._assert_old_follow_on_new_uuid_denied(4)

    def test_completed_index_5_legacy_payload_rejects_new_uuid(self):
        self._assert_old_follow_on_new_uuid_denied(5)

    def test_wbt_follow_on_selector_rejects_nonmembers_and_wrong_tuple_before_target_mutation(self):
        arguments,target_coord,target,artifact,candidates,chosen=self._wbt_follow_on_arguments(3)
        before=self._database_dump(target)
        bad_arguments=(
            arguments|{'resource':'file:.finite-batch-64-0077/not-selected.txt'},
            arguments|{'resource':'heavy-unity:2'},
            arguments|{'owner_session':'session-other'},
            arguments|{'reservation_id':'reservation-other'},
            arguments|{'expected_deadline':arguments['expected_deadline']+1.0},
        )
        with artifact:
            manifest=self._wbt_follow_on_manifest(candidates)
            with manifest, patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                    patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                    patch('agent_chat.expired_claim_disposition._sample') as native_sample:
                for invalid in bad_arguments:
                    with self.assertRaisesRegex(CoordError,'fixed WBT cohort index and exact claim tuple'):
                        execute_expired_claim(**invalid)
                native_sample.assert_not_called()
        self.assertEqual(self._database_dump(target),before)
        self.assertEqual(target_coord.db.execute(
            'SELECT count(*) FROM sqlite_master WHERE type=\'table\' AND name=?',
            (expired_claim_module._WBT_ABANDON_INTENTS,)).fetchone()[0],0)

    def _seed_synthetic_applied_legacy_intent(self, arguments, target_coord, row_index):
        root=expired_claim_module.load_wbt_preview_configured_root(self.master,self.plist)
        auth=expired_claim_module.authority(root,self.plist,arguments['project'],
                                             arguments['operator_message_id'])
        selected=expired_claim_module.wbt_witness_selection(root,arguments['project'],
            arguments['resource'],arguments['owner_session'],arguments['reservation_id'],
            arguments['expected_deadline'],arguments['cohort_ref'])
        if row_index in expired_claim_module._WBT_NWC_RESUMED_LEGACY_ROW_INDICES:
            # Freeze the completed index-64 v11 bytes independently; the
            # replacement manifest must not rewrite this stored outcome.
            payload=repr(('wbt-file-abandon-v11-resumed-stop-v1',1,
                expired_claim_module._WBT_NWC_RESUMED_V1_MANIFEST_SHA256,
                _WBT_COHORT_REF,expired_claim_module._WBT_COHORT_ARTIFACT_SHA256,
                row_index,selected,auth,arguments['admission_mode'],
                arguments['root_stop_request_id'],arguments['expires_at'],True))
            self.assertEqual(payload,expired_claim_module._wbt_abandon_payload(
                selected,auth,arguments['expires_at'],row_index=row_index,
                admission_mode=arguments['admission_mode'],
                root_stop_request_id=arguments['root_stop_request_id']))
        elif row_index in expired_claim_module._WBT_N6LB_ROW_INDICES:
            payload=repr(('wbt-file-abandon-v14-resumed-stop-v1',1,
                expired_claim_module._WBT_N6LB_MANIFEST_SHA256,
                _WBT_COHORT_REF,expired_claim_module._WBT_COHORT_ARTIFACT_SHA256,
                row_index,selected,auth,arguments['admission_mode'],
                arguments['root_stop_request_id'],arguments['expires_at'],True))
            self.assertEqual(payload,expired_claim_module._wbt_abandon_payload(
                selected,auth,arguments['expires_at'],row_index=row_index,
                admission_mode=arguments['admission_mode'],
                root_stop_request_id=arguments['root_stop_request_id']))
        elif row_index in expired_claim_module._WBT_XU0J_ROW_INDICES:
            payload=repr(('wbt-file-abandon-v13-resumed-stop-v1',1,
                expired_claim_module._WBT_XU0J_MANIFEST_SHA256,
                _WBT_COHORT_REF,expired_claim_module._WBT_COHORT_ARTIFACT_SHA256,
                row_index,selected,auth,arguments['admission_mode'],
                arguments['root_stop_request_id'],arguments['expires_at'],True))
            self.assertEqual(payload,expired_claim_module._wbt_abandon_payload(
                selected,auth,arguments['expires_at'],row_index=row_index,
                admission_mode=arguments['admission_mode'],
                root_stop_request_id=arguments['root_stop_request_id']))
        elif row_index in expired_claim_module._WBT_NWC_RESUMED_ROW_INDICES:
            payload=repr(('wbt-file-abandon-v12-resumed-stop-v1',2,
                expired_claim_module._WBT_NWC_RESUMED_MANIFEST_SHA256,
                _WBT_COHORT_REF,expired_claim_module._WBT_COHORT_ARTIFACT_SHA256,
                row_index,selected,auth,arguments['admission_mode'],
                arguments['root_stop_request_id'],arguments['expires_at'],True))
            self.assertEqual(payload,expired_claim_module._wbt_abandon_payload(
                selected,auth,arguments['expires_at'],row_index=row_index,
                admission_mode=arguments['admission_mode'],
                root_stop_request_id=arguments['root_stop_request_id']))
        elif row_index in expired_claim_module._WBT_FOLLOW_ON_ROW_INDICES:
            # Independently form the synthetic v2 tuple to preserve the
            # serialization check without asserting unavailable history.
            payload=repr(('wbt-file-abandon-v2',1,
                expired_claim_module._WBT_FOLLOW_ON_MANIFEST_SHA256,
                _WBT_COHORT_REF,expired_claim_module._WBT_COHORT_ARTIFACT_SHA256,
                row_index,selected,auth,arguments['expires_at'],True))
            self.assertEqual(payload,expired_claim_module._wbt_abandon_payload(
                selected,auth,arguments['expires_at'],row_index=row_index))
        elif row_index in expired_claim_module._WBT_GROUP_ROW_INDICES:
            payload=repr(('wbt-file-abandon-v3',1,
                expired_claim_module._WBT_GROUP_MANIFEST_SHA256,
                _WBT_COHORT_REF,expired_claim_module._WBT_COHORT_ARTIFACT_SHA256,
                row_index,selected,auth,arguments['expires_at'],True))
            self.assertEqual(payload,expired_claim_module._wbt_abandon_payload(
                selected,auth,arguments['expires_at'],row_index=row_index))
        elif row_index in expired_claim_module._WBT_NEXT_GROUP_ROW_INDICES:
            # Independently construct the v4 tuple from this synthetic fixture's
            # digest rather than deriving bytes from the payload constructor.
            payload=repr(('wbt-file-abandon-v4',1,
                expired_claim_module._WBT_NEXT_GROUP_MANIFEST_SHA256,
                _WBT_COHORT_REF,expired_claim_module._WBT_COHORT_ARTIFACT_SHA256,
                row_index,selected,auth,arguments['expires_at'],True))
            self.assertEqual(payload,expired_claim_module._wbt_abandon_payload(
                selected,auth,arguments['expires_at'],row_index=row_index))
        elif row_index in expired_claim_module._WBT_FILE_CLASS_ROW_INDICES:
            # Check the v5 synthetic tuple independently of the active
            # payload constructor; this fixture does not assert historical bytes.
            payload=repr(('wbt-file-abandon-v5',1,
                expired_claim_module._WBT_FILE_CLASS_MANIFEST_SHA256,
                _WBT_COHORT_REF,expired_claim_module._WBT_COHORT_ARTIFACT_SHA256,
                row_index,selected,auth,arguments['expires_at'],True))
            self.assertEqual(payload,expired_claim_module._wbt_abandon_payload(
                selected,auth,arguments['expires_at'],row_index=row_index))
        elif row_index in expired_claim_module._WBT_NEXT_FILE_CLASS_ROW_INDICES:
            # Independently construct the synthetic v6 tuple so the fixture
            # tests read-only reconciliation without claiming historical bytes.
            payload=repr(('wbt-file-abandon-v6',1,
                expired_claim_module._WBT_NEXT_FILE_CLASS_MANIFEST_SHA256,
                _WBT_COHORT_REF,expired_claim_module._WBT_COHORT_ARTIFACT_SHA256,
                row_index,selected,auth,arguments['expires_at'],True))
            self.assertEqual(payload,expired_claim_module._wbt_abandon_payload(
                selected,auth,arguments['expires_at'],row_index=row_index))
        elif row_index in expired_claim_module._WBT_GG1_FILE_CLASS_ROW_INDICES:
            # Independently check synthetic v7 serialization; the tuple remains
            # reconciliation-only and is not rebuilt from v8 metadata.
            payload=repr(('wbt-file-abandon-v7',1,
                expired_claim_module._WBT_GG1_FILE_CLASS_MANIFEST_SHA256,
                _WBT_COHORT_REF,expired_claim_module._WBT_COHORT_ARTIFACT_SHA256,
                row_index,selected,auth,arguments['expires_at'],True))
            self.assertEqual(payload,expired_claim_module._wbt_abandon_payload(
                selected,auth,arguments['expires_at'],row_index=row_index))
        elif row_index in expired_claim_module._WBT_HGIDE_FILE_CLASS_ROW_INDICES:
            # Check the fixture's v8 payload independently of the constructor;
            # indices 40-47 remain reconciliation-only.
            payload=repr(('wbt-file-abandon-v8',1,
                expired_claim_module._WBT_HGIDE_FILE_CLASS_MANIFEST_SHA256,
                _WBT_COHORT_REF,expired_claim_module._WBT_COHORT_ARTIFACT_SHA256,
                row_index,selected,auth,arguments['expires_at'],True))
            self.assertEqual(payload,expired_claim_module._wbt_abandon_payload(
                selected,auth,arguments['expires_at'],row_index=row_index))
        elif row_index in expired_claim_module._WBT_6JC1_FILE_CLASS_ROW_INDICES:
            # The prior applied intent belongs to this same synthetic fixture;
            # its immutable tuple carries the scoped fixture's actual digest.
            payload=repr(('wbt-file-abandon-v9',1,
                expired_claim_module._WBT_6JC1_FILE_CLASS_MANIFEST_SHA256,
                _WBT_COHORT_REF,expired_claim_module._WBT_COHORT_ARTIFACT_SHA256,
                row_index,selected,auth,arguments['expires_at'],True))
            self.assertEqual(payload,expired_claim_module._wbt_abandon_payload(
                selected,auth,arguments['expires_at'],row_index=row_index))
        else:
            payload=expired_claim_module._wbt_abandon_payload(
                selected,auth,arguments['expires_at'],row_index=row_index)
        payload_digest=hashlib.sha256(payload.encode('utf-8')).hexdigest()
        prepared_digest='a'*64
        now=time.time()
        expired_claim_module._wbt_abandon_schema(target_coord.db,create=True)
        expired_claim_module.audit_schema(target_coord.db,create=True)
        target_coord.db.execute('''INSERT INTO root_wbt_file_abandon_intents
            (request_id,payload,payload_digest,state,created_at,expires_at,updated_at,
             prepared_digest,evidence_digest,reason_code)
            VALUES(?,?,?,'applied',?,?,?,?,?,NULL)''',
            (arguments['request_id'],payload,payload_digest,
             min(now-1.0,float(arguments['expires_at'])-1.0),arguments['expires_at'],now,
             prepared_digest,'b'*64))
        for state in ('accepted','prepared','applying','applied'):
            expired_claim_module._wbt_abandon_append_event(
                target_coord.db,arguments['request_id'],state,now,payload_digest,
                'b'*64 if state in ('prepared','applying','applied') else None)
        audit_values=(arguments['request_id'],repr((payload,payload_digest,prepared_digest,True)),
            arguments['project'],arguments['resource'],arguments['owner_session'],
            arguments['reservation_id'],arguments['expected_deadline'],root.home_session,
            root.host_id,repr(('synthetic applied legacy receipt','UNVERIFIED')),
            'wbt_file_claim_abandoned_native_unverified',now)
        target_coord.db.execute('INSERT INTO root_expired_claim_dispositions VALUES('+','.join('?' for _ in expired_claim_module.COLUMNS)+')',audit_values)
        target_coord.db.commit()
        return payload,prepared_digest

    def _assert_legacy_stored_outcome_is_same_id_read_only(self, arguments, target_coord,
                                                           target, artifact, row_index,
                                                           follow_on_candidates=None,
                                                           group_candidates=None,
                                                           next_group_candidates=None,
                                                           file_class_candidates=None,
                                                           next_file_class_candidates=None,
                                                           gg1_file_class_candidates=None,
                                                           hgide_file_class_candidates=None,
                                                           nwc_resumed_candidates=None):
        with contextlib.ExitStack() as setup:
            if follow_on_candidates is not None:
                setup.enter_context(artifact)
                setup.enter_context(self._wbt_follow_on_manifest(follow_on_candidates))
            elif group_candidates is not None:
                setup.enter_context(artifact)
            elif next_group_candidates is not None:
                setup.enter_context(artifact)
            elif file_class_candidates is not None:
                setup.enter_context(artifact)
            elif next_file_class_candidates is not None:
                # Synthetic current fixture; immutable payload is checked below.
                setup.enter_context(artifact)
            elif gg1_file_class_candidates is not None:
                setup.enter_context(artifact)
            elif nwc_resumed_candidates is not None:
                setup.enter_context(artifact)
            else:
                setup.enter_context(artifact)
            if follow_on_candidates is not None:
                matched=[candidate for candidate in follow_on_candidates
                    if candidate['original_index']==row_index]
                self.assertEqual(len(matched),1)
                self.assertEqual((matched[0]['resource'],matched[0]['owner_session'],
                    matched[0]['reservation_id'],matched[0]['original_deadline']),
                    (arguments['resource'],arguments['owner_session'],
                     arguments['reservation_id'],arguments['expected_deadline']))
            if group_candidates is not None:
                matched=[candidate for candidate in group_candidates
                    if candidate['original_index']==row_index]
                self.assertEqual(len(matched),1)
                self.assertEqual((matched[0]['resource'],matched[0]['owner_session'],
                    matched[0]['reservation_id'],matched[0]['original_deadline']),
                    (arguments['resource'],arguments['owner_session'],
                     arguments['reservation_id'],arguments['expected_deadline']))
            if next_group_candidates is not None:
                matched=[candidate for candidate in next_group_candidates
                    if candidate['original_index']==row_index]
                self.assertEqual(len(matched),1)
                self.assertEqual((matched[0]['resource'],matched[0]['owner_session'],
                    matched[0]['reservation_id'],matched[0]['original_deadline']),
                    (arguments['resource'],arguments['owner_session'],
                     arguments['reservation_id'],arguments['expected_deadline']))
            if file_class_candidates is not None:
                matched=[candidate for candidate in file_class_candidates
                    if candidate['original_index']==row_index]
                self.assertEqual(len(matched),1)
                self.assertEqual((matched[0]['resource'],matched[0]['owner_session'],
                    matched[0]['reservation_id'],matched[0]['original_deadline']),
                    (arguments['resource'],arguments['owner_session'],
                     arguments['reservation_id'],arguments['expected_deadline']))
            if next_file_class_candidates is not None:
                matched=[candidate for candidate in next_file_class_candidates
                    if candidate['original_index']==row_index]
                self.assertEqual(len(matched),1)
                self.assertEqual((matched[0]['resource'],matched[0]['owner_session'],
                    matched[0]['reservation_id'],matched[0]['original_deadline']),
                    (arguments['resource'],arguments['owner_session'],
                     arguments['reservation_id'],arguments['expected_deadline']))
            if gg1_file_class_candidates is not None:
                matched=[candidate for candidate in gg1_file_class_candidates
                    if candidate['original_index']==row_index]
                self.assertEqual(len(matched),1)
                self.assertEqual((matched[0]['resource'],matched[0]['owner_session'],
                    matched[0]['reservation_id'],matched[0]['original_deadline']),
                    (arguments['resource'],arguments['owner_session'],
                     arguments['reservation_id'],arguments['expected_deadline']))
            if hgide_file_class_candidates is not None:
                matched=[candidate for candidate in hgide_file_class_candidates
                    if candidate['original_index']==row_index]
                self.assertEqual(len(matched),1)
                self.assertEqual((matched[0]['resource'],matched[0]['owner_session'],
                    matched[0]['reservation_id'],matched[0]['original_deadline']),
                    (arguments['resource'],arguments['owner_session'],
                     arguments['reservation_id'],arguments['expected_deadline']))
            if nwc_resumed_candidates is not None:
                matched=[candidate for candidate in nwc_resumed_candidates
                    if candidate['original_index']==row_index]
                self.assertEqual(len(matched),1)
                self.assertEqual((matched[0]['resource'],matched[0]['owner_session'],
                    matched[0]['reservation_id'],matched[0]['original_deadline']),
                    (arguments['resource'],arguments['owner_session'],
                     arguments['reservation_id'],arguments['expected_deadline']))
            with patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master):
                payload,prepared_digest=self._seed_synthetic_applied_legacy_intent(
                    arguments,target_coord,row_index)
            before=self._database_dump(target)
            with patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample') as native_sample:
                prepared_reconcile=dict(execute_expired_claim(**arguments))
                apply_reconcile=dict(execute_expired_claim(**(arguments|{
                    'action':('wbt-resumed-apply' if nwc_resumed_candidates is not None
                              else 'wbt-abandon-apply'),
                    'expected_digest':prepared_digest})))
                native_sample.assert_not_called()
            self.assertEqual(prepared_reconcile['status'],'applied')
            self.assertEqual(apply_reconcile['status'],'applied')
            self.assertTrue(prepared_reconcile['replayed'])
            self.assertTrue(apply_reconcile['replayed'])
            self.assertEqual(prepared_reconcile['process_closure'],'UNVERIFIED')
            self.assertEqual(prepared_reconcile['eligibility'],'NOT_DEMONSTRATED')
            self.assertEqual(self._database_dump(target),before)
            stored=target_coord.db.execute(
                'SELECT payload,state FROM root_wbt_file_abandon_intents WHERE request_id=?',
                (arguments['request_id'],)).fetchone()
            self.assertEqual(stored['payload'],payload)
            self.assertEqual(stored['state'],'applied')

    def test_index2_v1_applied_outcome_is_reconciled_without_sampling_or_rewrite(self):
        arguments,target_coord,target,artifact=self.wbt_abandon_arguments(
            'file:.finite-batch-64-0077/docs/reference/agent-operations.md')
        self._assert_legacy_stored_outcome_is_same_id_read_only(
            arguments,target_coord,target,artifact,2)

    def test_completed_index_2_v1_legacy_payload_rejects_new_uuid_before_bootstrap(self):
        arguments,target_coord,target,artifact=self.wbt_abandon_arguments(
            'file:.finite-batch-64-0077/docs/reference/agent-operations.md')
        arguments=arguments|{'request_id':'44444444-aaaa-4bbb-8ccc-444444444444'}
        before=self._database_dump(target)
        self.assertEqual(target_coord.db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
            (expired_claim_module._WBT_ABANDON_INTENTS,)).fetchone()[0],0)
        with artifact,patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample') as native_sample:
            with self.assertRaisesRegex(CoordError,'same-ID reconciliation only'):
                execute_expired_claim(**arguments)
            native_sample.assert_not_called()
        self.assertEqual(self._database_dump(target),before)
        self.assertEqual(target_coord.db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
            (expired_claim_module._WBT_ABANDON_INTENTS,)).fetchone()[0],0)

    def test_index3_v2_applied_outcome_is_reconciled_without_sampling_or_rewrite(self):
        arguments,target_coord,target,artifact,candidates,chosen=self._wbt_follow_on_arguments(3)
        self._assert_legacy_stored_outcome_is_same_id_read_only(
            arguments,target_coord,target,artifact,3,candidates)

    def test_index4_v2_applied_outcome_is_reconciled_without_sampling_or_rewrite(self):
        arguments,target_coord,target,artifact,candidates,chosen=self._wbt_follow_on_arguments(4)
        self._assert_legacy_stored_outcome_is_same_id_read_only(
            arguments,target_coord,target,artifact,4,candidates)

    def test_index5_v2_applied_outcome_is_reconciled_without_sampling_or_rewrite(self):
        arguments,target_coord,target,artifact,candidates,chosen=self._wbt_follow_on_arguments(5)
        self._assert_legacy_stored_outcome_is_same_id_read_only(
            arguments,target_coord,target,artifact,5,candidates)

    def test_wbt_follow_on_manifest_duplicate_tamper_and_cohort_row_drift_fail_closed(self):
        arguments,target_coord,target,artifact,candidates,chosen=self._wbt_follow_on_arguments(3)
        before=self._database_dump(target)
        duplicate=list(candidates)
        duplicate[1]=duplicate[0].copy()
        with artifact:
            duplicate_manifest=self._wbt_follow_on_manifest(duplicate)
            with duplicate_manifest, patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                    patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                    patch('agent_chat.expired_claim_disposition._sample') as native_sample:
                with self.assertRaisesRegex(CoordError,'fixed WBT cohort index and exact claim tuple'):
                    execute_expired_claim(**arguments)
                native_sample.assert_not_called()
            tampered_manifest=self._wbt_follow_on_manifest(candidates,
                raw_override=b'{"manifest_version":1,"purpose":"tampered"}')
            with tampered_manifest, patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                    patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                    patch('agent_chat.expired_claim_disposition._sample') as native_sample:
                with self.assertRaisesRegex(CoordError,'fixed WBT cohort index and exact claim tuple'):
                    execute_expired_claim(**arguments)
                native_sample.assert_not_called()
        self.assertEqual(self._database_dump(target),before)

    def test_wbt_group_selector_rejects_nonmembers_and_tuple_drift_before_target_mutation(self):
        arguments,target_coord,target,artifact,candidates,chosen=self._wbt_group_arguments(6)
        before=self._database_dump(target)
        bad_arguments=(
            arguments|{'resource':'file:.finite-batch-64-0080/not-selected.txt'},
            arguments|{'resource':'heavy-unity:2'},
            arguments|{'owner_session':'session-other'},
            arguments|{'reservation_id':'reservation-other'},
            arguments|{'expected_deadline':arguments['expected_deadline']+1.0},
        )
        with artifact,patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample') as native_sample:
            for invalid in bad_arguments:
                with self.assertRaisesRegex(CoordError,'fixed WBT cohort index and exact claim tuple'):
                    execute_expired_claim(**invalid)
            native_sample.assert_not_called()
        self.assertEqual(self._database_dump(target),before)
        self.assertEqual(target_coord.db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
            (expired_claim_module._WBT_ABANDON_INTENTS,)).fetchone()[0],0)

    def test_wbt_group_manifest_duplicate_tamper_and_source_row_drift_fail_closed(self):
        arguments,target_coord,target,artifact,candidates,chosen=self._wbt_group_arguments(6)
        before=self._database_dump(target)
        duplicate=list(candidates)
        duplicate[1]=duplicate[0].copy()
        with artifact:
            duplicate_manifest=self._wbt_group_manifest(duplicate)
            with duplicate_manifest,patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                    patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                    patch('agent_chat.expired_claim_disposition._sample') as native_sample:
                with self.assertRaisesRegex(CoordError,'fixed WBT cohort index and exact claim tuple'):
                    execute_expired_claim(**arguments)
                native_sample.assert_not_called()
            tampered_manifest=self._wbt_group_manifest(candidates,
                raw_override=b'{"manifest_version":1,"purpose":"tampered"}')
            with tampered_manifest,patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                    patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                    patch('agent_chat.expired_claim_disposition._sample') as native_sample:
                with self.assertRaisesRegex(CoordError,'fixed WBT cohort index and exact claim tuple'):
                    execute_expired_claim(**arguments)
                native_sample.assert_not_called()
        self.assertEqual(self._database_dump(target),before)
        self.assertEqual(target_coord.db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
            (expired_claim_module._WBT_ABANDON_INTENTS,)).fetchone()[0],0)

        source_rows=[('fixture:source-row-'+str(index),'fixture-owner-'+str(index),
                      'fixture-rid-'+str(index),chosen['original_deadline']-10+index)
                     for index in range(6)]
        source_rows += [('file:.finite-batch-64-0080/changed.py',chosen['owner_session'],
                         chosen['reservation_id'],chosen['original_deadline'])]
        source_rows += [(row['resource'],row['owner_session'],row['reservation_id'],row['original_deadline'])
                        for row in candidates[1:]]
        with self._wbt_group_artifact_context(arguments,candidates,rows=source_rows),\
                patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample') as native_sample:
            with self.assertRaisesRegex(CoordError,'fixed WBT cohort index and exact claim tuple'):
                execute_expired_claim(**arguments)
            native_sample.assert_not_called()
        self.assertEqual(self._database_dump(target),before)
        self.assertEqual(target_coord.db.execute(
            'SELECT count(*) FROM sqlite_master WHERE type=\'table\' AND name=?',
            (expired_claim_module._WBT_ABANDON_INTENTS,)).fetchone()[0],0)

        # A pinned manifest member cannot override a different original row at its index.
        tampered_rows=[('fixture:source-row-0','fixture-owner-0','fixture-rid-0',chosen['original_deadline']-3),
                       ('fixture:source-row-1','fixture-owner-1','fixture-rid-1',chosen['original_deadline']-2),
                       ('fixture:source-row-2','fixture-owner-2','fixture-rid-2',chosen['original_deadline']-1),
                       ('file:.finite-batch-64-0077/changed.py',chosen['owner_session'],
                        chosen['reservation_id'],chosen['original_deadline'])]
        for candidate in candidates[1:]:
            tampered_rows.append((candidate['resource'],candidate['owner_session'],
                candidate['reservation_id'],candidate['original_deadline']))
        artifact_drift=self._wbt_artifact(arguments,rows=tampered_rows)
        with artifact_drift:
            manifest_drift=self._wbt_follow_on_manifest(candidates)
            with manifest_drift, patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                    patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                    patch('agent_chat.expired_claim_disposition._sample') as native_sample:
                with self.assertRaisesRegex(CoordError,'fixed WBT cohort index and exact claim tuple'):
                    execute_expired_claim(**arguments)
                native_sample.assert_not_called()
        self.assertEqual(self._database_dump(target),before)

    def test_wbt_abandon_request_id_payload_mismatch_and_bad_authority_do_not_mutate(self):
        arguments,target_coord,target,artifact=self.wbt_abandon_arguments()
        with artifact,patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample',
                      side_effect=self._passive_not_loaded_sample):
            first=dict(execute_expired_claim(**arguments))
            before=self._database_dump(target)
            altered=arguments|{'expires_at':arguments['expires_at']+1}
            with self.assertRaisesRegex(CoordError,'different immutable payload'):
                execute_expired_claim(**altered)
            wrong=arguments|{'request_id':'44444444-aaaa-4bbb-8ccc-444444444444',
                'operator_message_id':['message-not-authorized']}
            with self.assertRaisesRegex(CoordError,'exact original Root authority'):
                execute_expired_claim(**wrong)
        self.assertEqual(first['status'],'prepared')
        self.assertEqual(self._database_dump(target),before)
        self.assertEqual(target_coord.db.execute('SELECT count(*) FROM root_wbt_file_abandon_intents').fetchone()[0],1)

    def test_wbt_abandon_overlapping_apply_has_one_winner(self):
        arguments,target_coord,target,artifact=self.wbt_abandon_arguments()
        with artifact,patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample',
                      side_effect=self._passive_not_loaded_sample):
            prepared=dict(execute_expired_claim(**arguments))
        apply_args=arguments|{'action':'wbt-abandon-apply',
            'expected_digest':prepared['prepared_digest']}
        entered=threading.Event(); release=threading.Event()
        sample_lock=threading.Lock(); sample_count={'value':0}
        def held_sample(thread_id,endpoint,*,strict):
            with sample_lock:
                sample_count['value']+=1
                first=sample_count['value']==1
            if first:
                entered.set()
                if not release.wait(5):
                    raise RuntimeError('test did not release first apply sample')
            return self._passive_not_loaded_sample(thread_id,endpoint,strict=strict)
        with artifact,patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample',side_effect=held_sample):
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                first=pool.submit(execute_expired_claim,**apply_args)
                self.assertTrue(entered.wait(5),'first apply did not reach its sample')
                second=pool.submit(execute_expired_claim,**apply_args)
                second_result=dict(second.result(timeout=5))
                release.set()
                first_result=dict(first.result(timeout=10))
        self.assertEqual(first_result['status'],'applied')
        self.assertIn(second_result['status'],('applying','applied'))
        self.assertEqual(sample_count['value'],1)
        self.assertEqual(target_coord.db.execute(
            'SELECT count(*) FROM root_expired_claim_dispositions WHERE request_id=?',
            (arguments['request_id'],)).fetchone()[0],1)
        states=tuple(row['state'] for row in target_coord.db.execute(
            'SELECT state FROM root_wbt_file_abandon_events WHERE request_id=? ORDER BY seq',
            (arguments['request_id'],)))
        self.assertEqual(states,('accepted','prepared','applying','applied'))
        self.assertEqual(tuple(target_coord.db.execute(
            'SELECT owner_session,reservation_id,deadline,stale FROM resources WHERE name=?',
            (arguments['resource'],)).fetchone()),(None,None,None,0))

    def test_wbt_abandon_interruption_after_applying_is_not_resampled(self):
        arguments,target_coord,target,artifact=self.wbt_abandon_arguments()
        with artifact,patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample',
                      side_effect=self._passive_not_loaded_sample):
            prepared=dict(execute_expired_claim(**arguments))
        apply_args=arguments|{'action':'wbt-abandon-apply',
            'expected_digest':prepared['prepared_digest']}
        with artifact,patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample') as native_sample,\
                patch('agent_chat.expired_claim_disposition._wbt_abandon_apply_effect',
                      side_effect=RuntimeError('synthetic interruption after applying commit')):
            with self.assertRaisesRegex(RuntimeError,'after applying commit'):
                execute_expired_claim(**apply_args)
            state=target_coord.db.execute(
                'SELECT state FROM root_wbt_file_abandon_intents WHERE request_id=?',
                (arguments['request_id'],)).fetchone()[0]
            events=tuple(row['state'] for row in target_coord.db.execute(
                'SELECT state FROM root_wbt_file_abandon_events WHERE request_id=? ORDER BY seq',
                (arguments['request_id'],)))
            replay=dict(execute_expired_claim(**apply_args))
            self.assertEqual(native_sample.call_count,0)
        self.assertEqual(state,'applying')
        self.assertEqual(events,('accepted','prepared','applying'))
        self.assertEqual(replay['status'],'applying')
        self.assertTrue(replay['replayed'])
        audit_exists=target_coord.db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_expired_claim_dispositions'").fetchone()
        self.assertFalse(audit_exists)
        self.assertEqual(tuple(target_coord.db.execute(
            'SELECT owner_session,reservation_id,stale FROM resources WHERE name=?',
            (arguments['resource'],)).fetchone()),
            (arguments['owner_session'],arguments['reservation_id'],1))

    def test_wbt_abandon_commit_ambiguity_is_not_retried(self):
        arguments,target_coord,target,artifact=self.wbt_abandon_arguments()
        with artifact,patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample',
                      side_effect=self._passive_not_loaded_sample) as native_sample:
            prepared=dict(execute_expired_claim(**arguments))
            apply_args=arguments|{'action':'wbt-abandon-apply',
                'expected_digest':prepared['prepared_digest']}
            original_commit=expired_claim_module._wbt_abandon_commit_effect
            def commit_then_lose_ack(db):
                original_commit(db)
                raise sqlite3.OperationalError('synthetic commit acknowledgement lost')
            with patch('agent_chat.expired_claim_disposition._wbt_abandon_commit_effect',
                       side_effect=commit_then_lose_ack):
                ambiguous=dict(execute_expired_claim(**apply_args))
            self.assertEqual(ambiguous['status'],'applying')
            self.assertEqual(native_sample.call_count,2)
            replay=dict(execute_expired_claim(**apply_args))
            self.assertEqual(native_sample.call_count,2)
        self.assertEqual(replay['status'],'applied')
        self.assertTrue(replay['replayed'])
        self.assertEqual(target_coord.db.execute(
            'SELECT count(*) FROM root_expired_claim_dispositions WHERE request_id=?',
            (arguments['request_id'],)).fetchone()[0],1)
        self.assertEqual(tuple(target_coord.db.execute(
            'SELECT owner_session,reservation_id,stale FROM resources WHERE name=?',
            (arguments['resource'],)).fetchone()),
            (None,None,0))
        states=tuple(row['state'] for row in target_coord.db.execute(
            'SELECT state FROM root_wbt_file_abandon_events WHERE request_id=? ORDER BY seq',
            (arguments['request_id'],)))
        self.assertEqual(states,('accepted','prepared','applying','applied'))

    def test_wbt_abandon_expiry_before_prepare_and_expired_apply_preserve_claim(self):
        arguments,target_coord,target,artifact=self.wbt_abandon_arguments()
        original=tuple(target_coord.db.execute(
            'SELECT owner_session,reservation_id,token,deadline,stale FROM resources WHERE name=?',
            (arguments['resource'],)).fetchone())
        with artifact,patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample',
                      side_effect=self._passive_not_loaded_sample):
            expired_new=arguments|{'expires_at':time.time()-1}
            with self.assertRaisesRegex(CoordError,'positive explicit|900 seconds'):
                execute_expired_claim(**expired_new)
            self.assertEqual(target_coord.db.execute(
                'SELECT count(*) FROM sqlite_master WHERE type=\'table\' AND name=?',
                (expired_claim_module._WBT_ABANDON_INTENTS,)).fetchone()[0],0)
            prepared=dict(execute_expired_claim(**arguments))
        self.assertEqual(prepared['status'],'prepared')
        expired_apply=arguments|{'action':'wbt-abandon-apply','expected_digest':prepared['prepared_digest']}
        with artifact,patch.object(expired_claim_module,'time',type('Clock',(),{'time':staticmethod(lambda:arguments['expires_at']+1)})),\
                patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master):
            result=dict(execute_expired_claim(**expired_apply))
        self.assertEqual(result['status'],'expired')
        self.assertEqual(tuple(target_coord.db.execute(
            'SELECT owner_session,reservation_id,token,deadline,stale FROM resources WHERE name=?',
            (arguments['resource'],)).fetchone()),original)
        self.assertEqual(target_coord.db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='root_expired_claim_dispositions'").fetchone()[0],0)

    def test_wbt_abandon_failed_passive_sample_is_terminal_and_not_reobserved(self):
        arguments,target_coord,target,artifact=self.wbt_abandon_arguments()
        with artifact,patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample',
                      side_effect=CoordError('synthetic passive failure')) as native_sample:
            blocked=dict(execute_expired_claim(**arguments))
            replay=dict(execute_expired_claim(**arguments))
            self.assertEqual(native_sample.call_count,1)
        self.assertEqual(blocked['status'],'blocked')
        self.assertEqual(blocked['reason_code'],'passive_native_evidence_unavailable')
        self.assertTrue(replay['replayed'])
        self.assertEqual(replay['status'],'blocked')
        row=target_coord.db.execute('SELECT owner_session,reservation_id,stale FROM resources WHERE name=?',
                                    (arguments['resource'],)).fetchone()
        self.assertEqual(tuple(row),(arguments['owner_session'],arguments['reservation_id'],1))

    def test_wbt_abandon_rejects_active_native_turn(self):
        arguments,target_coord,target,artifact=self.wbt_abandon_arguments()
        active={'thread_id':'thread-wbt-owner','state':'active','turn_id':'turn-active',
                'queue_count':0,'direct_input_available':False,
                'started_at':time.time(),'captured_at':time.time(),'elapsed_ms':1}
        with artifact,patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample',return_value=active):
            result=dict(execute_expired_claim(**arguments))
        self.assertEqual((result['status'],result['reason_code']),
                         ('blocked','native_binding_or_sample_drift'))
        self.assertEqual(tuple(target_coord.db.execute(
            'SELECT owner_session,reservation_id,stale FROM resources WHERE name=?',
            (arguments['resource'],)).fetchone()),
            (arguments['owner_session'],arguments['reservation_id'],1))

    def test_wbt_abandon_open_guard_blocks_without_clearing_claim(self):
        arguments,target_coord,target,artifact=self.wbt_abandon_arguments()
        target_coord.db.execute('''INSERT INTO guarded_runs
            (run_id,resource,reservation_id,session,pid,pgid,started_at,closed_at)
            VALUES(?,?,?,?,?,?,?,NULL)''',
            ('wbt-abandon-open-guard',arguments['resource'],'unrelated-rid',
             arguments['owner_session'],0,None,time.time()))
        target_coord.db.commit()
        with artifact,patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample',
                      side_effect=self._passive_not_loaded_sample):
            result=dict(execute_expired_claim(**arguments))
        self.assertEqual((result['status'],result['reason_code']),
                         ('blocked','passive_or_row_preflight_blocked'))
        self.assertEqual(tuple(target_coord.db.execute(
            'SELECT owner_session,reservation_id,stale FROM resources WHERE name=?',
            (arguments['resource'],)).fetchone()),
            (arguments['owner_session'],arguments['reservation_id'],1))

    def test_wbt_abandon_audit_failure_rolls_back_claim_and_is_not_retried(self):
        arguments,target_coord,target,artifact=self.wbt_abandon_arguments()
        with artifact,patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample',
                      side_effect=self._passive_not_loaded_sample) as native_sample:
            prepared=dict(execute_expired_claim(**arguments))
            expired_claim_module.audit_schema(target_coord.db,create=True)
            target_coord.db.execute('''CREATE TRIGGER fixture_fail_wbt_audit_insert
                BEFORE INSERT ON root_expired_claim_dispositions
                BEGIN SELECT RAISE(ABORT,'synthetic audit failure'); END''')
            target_coord.db.commit()
            apply_args=arguments|{'action':'wbt-abandon-apply',
                'expected_digest':prepared['prepared_digest']}
            result=dict(execute_expired_claim(**apply_args))
            replay=dict(execute_expired_claim(**apply_args))
            self.assertEqual(native_sample.call_count,2)
        self.assertEqual(result['status'],'blocked')
        self.assertEqual(result['reason_code'],'claim_transaction_rejected')
        self.assertEqual(replay['status'],'blocked')
        self.assertTrue(replay['replayed'])
        self.assertEqual(tuple(target_coord.db.execute(
            'SELECT owner_session,reservation_id,stale FROM resources WHERE name=?',
            (arguments['resource'],)).fetchone()),
            (arguments['owner_session'],arguments['reservation_id'],1))
        self.assertEqual(target_coord.db.execute(
            'SELECT count(*) FROM root_expired_claim_dispositions WHERE request_id=?',
            (arguments['request_id'],)).fetchone()[0],0)
        states=tuple(row['state'] for row in target_coord.db.execute(
            'SELECT state FROM root_wbt_file_abandon_events WHERE request_id=? ORDER BY seq',
            (arguments['request_id'],)))
        self.assertEqual(states,('accepted','prepared','applying','blocked'))

    def test_wbt_abandon_applied_history_remains_applied_after_expiry(self):
        arguments,target_coord,target,artifact=self.wbt_abandon_arguments()
        with artifact,patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample',
                      side_effect=self._passive_not_loaded_sample) as native_sample:
            prepared=dict(execute_expired_claim(**arguments))
            apply_args=arguments|{'action':'wbt-abandon-apply',
                'expected_digest':prepared['prepared_digest']}
            applied=dict(execute_expired_claim(**apply_args))
            with patch.object(expired_claim_module,'time',type('Clock',(),{
                    'time':staticmethod(lambda:arguments['expires_at']+30)})):
                replay=dict(execute_expired_claim(**arguments))
            self.assertEqual(native_sample.call_count,2)
        self.assertEqual(applied['status'],'applied')
        self.assertEqual(replay['status'],'applied')
        self.assertTrue(replay['replayed'])
        self.assertEqual(replay['process_closure'],'UNVERIFIED')
        self.assertEqual(replay['eligibility'],'NOT_DEMONSTRATED')
        self.assertEqual(target_coord.db.execute(
            'SELECT count(*) FROM root_expired_claim_dispositions WHERE request_id=?',
            (arguments['request_id'],)).fetchone()[0],1)

    def test_wbt_abandon_prepared_reference_drift_blocks_without_clearing_claim(self):
        arguments,target_coord,target,artifact=self.wbt_abandon_arguments()
        with artifact,patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample',
                      side_effect=self._passive_not_loaded_sample):
            prepared=dict(execute_expired_claim(**arguments))
            target_coord.db.execute('INSERT INTO resource_queue(resource,session,queued_at) VALUES(?,?,?)',
                                    (arguments['resource'],'queued-session',time.time()))
            target_coord.db.commit()
            apply_args=arguments|{'action':'wbt-abandon-apply','expected_digest':prepared['prepared_digest']}
            blocked=dict(execute_expired_claim(**apply_args))
            replay=dict(execute_expired_claim(**apply_args))
        self.assertEqual(blocked['status'],'blocked')
        self.assertEqual(blocked['reason_code'],'prepared_evidence_changed')
        self.assertEqual(replay['status'],'blocked')
        row=target_coord.db.execute('SELECT owner_session,reservation_id,stale FROM resources WHERE name=?',
                                    (arguments['resource'],)).fetchone()
        self.assertEqual(tuple(row),(arguments['owner_session'],arguments['reservation_id'],1))
        self.assertEqual(target_coord.db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='root_expired_claim_dispositions'").fetchone()[0],0)


    @staticmethod
    def _wbt_nwc_resumed_candidates():
        return [
            {'original_index':64,'resource':'file:.ownership-disposition-0039-sampler/src/agent_chat/control_worker.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_aK90xNe2-4KGOLPfdmui6Ny3sLi4AYNv','original_deadline':1791536774.208071},
            {'original_index':65,'resource':'file:.ownership-disposition-0039-sampler/src/agent_chat/core.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_anFogf6lcY-xTmqX9ASuOiGrdpTwD9iV','original_deadline':1791536774.076214},
            {'original_index':66,'resource':'file:.ownership-disposition-0039-sampler/src/agent_chat/ownership_disposition.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_aW6fW87SNM8QHKc1oWOaZiYihDpyA6h1','original_deadline':1791536773.802954},
            {'original_index':67,'resource':'file:.ownership-disposition-0039-sampler/src/agent_chat/service.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_0Wx92Og4or55Rm7dO_XFRqhl0Mv7sOd4','original_deadline':1791536888.441303},
            {'original_index':68,'resource':'file:.ownership-disposition-0039-sampler/tests/test_control_worker.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_ZW3lJckbdFBm_qKUROsZemIQLebnSyyb','original_deadline':1791536774.840426},
            {'original_index':69,'resource':'file:.ownership-disposition-0039-sampler/tests/test_ownership_disposition.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_U-FhEanqr1s1lMHbQyeMqaatJxsRgw_H','original_deadline':1791536774.7181902},
            {'original_index':70,'resource':'file:.ownership-disposition-0039/docs/reference/agent-operations.md','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_slw_IPHfVot9XBCjs3RevIAj-Xx9QJgF','original_deadline':1791531445.416812},
            {'original_index':71,'resource':'file:.ownership-disposition-0039/src/agent_chat/agent_control.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_g_3ePh_qGx1FUCbqdLiLUgoTMmcsWkio','original_deadline':1791531444.4997241},
        ]

    @staticmethod
    def _wbt_xu0j_candidates():
        return [
            {'original_index':72,'resource':'file:.ownership-disposition-0039/src/agent_chat/cli.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_wnzbdfvhK1c50DVLr1v-8pHrN6-Ka23r','original_deadline':1791531444.8373399},
            {'original_index':73,'resource':'file:.ownership-disposition-0039/src/agent_chat/remote_service.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_LzFRRQ80OnczwQTMIsFDpb6rJjeenG77','original_deadline':1791531444.613033},
            {'original_index':74,'resource':'file:.ownership-disposition-0039/src/agent_chat/root_orchestrator.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_CYWc3I8KmTwWPaeMdQXehi4J-eFZd1hy','original_deadline':1791531444.9516008},
            {'original_index':75,'resource':'file:.ownership-disposition-0039/src/agent_chat/web.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_rKTv8EJK14wjuBSMekzWM6sBDrZYLZxm','original_deadline':1791531444.724334},
            {'original_index':76,'resource':'file:.ownership-disposition-0039/tests/test_agent_control.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_AXrco_wHHTKZHPfow0k6B2-uZYPMdWkX','original_deadline':1791531445.0662882},
            {'original_index':77,'resource':'file:.ownership-disposition-0039/tests/test_remote_coord.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_kzVlXO4VvZQp3eEC7OFoNugMdmrlE82H','original_deadline':1791531445.18835},
            {'original_index':78,'resource':'file:.ownership-disposition-0039/tests/test_root_orchestrator.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_TUUYxs_63yRFdqJG4R1Z5UiKR4OGMi6k','original_deadline':1791531445.303651},
            {'original_index':79,'resource':'file:.ownership-disposition-0050/docs/reference/agent-operations.md','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_Jngh7xgTN5zgJ_csppF4ixzG31WVKKHb','original_deadline':1791552358.23707},
        ]

    @staticmethod
    def _wbt_n6lb_candidates():
        return [
            {'original_index':80,'resource':'file:.ownership-disposition-0050/pyproject.toml','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_wnSHuaSrUfP6G6eQ4V5Z9UIrDDA7rL9d','original_deadline':1791552366.541216},
            {'original_index':81,'resource':'file:.ownership-disposition-0050/src/agent_chat/ownership_disposition.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_4H5Vsgm6UhQ1MTYBRHJZlkkMsNQ1KrLq','original_deadline':1791552334.417357},
            {'original_index':82,'resource':'file:.ownership-disposition-0050/src/agent_chat/service.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_L8cOsEuOyKb6qNdfuV2hlHEgJHfVEfvn','original_deadline':1791552325.42197},
            {'original_index':83,'resource':'file:.ownership-disposition-0050/tests/test_ownership_disposition.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_pUFwZf6NglQ78Mn6pTxIEGDlMP7RHdhT','original_deadline':1791552342.253939},
            {'original_index':92,'resource':'file:.ownership-disposition-layout-resource/docs/reference/agent-operations.md','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_4WkyDD70ux61STSTiZV4Wl9pWu9QpZrN','original_deadline':1791606287.276523},
            {'original_index':93,'resource':'file:.ownership-disposition-layout-resource/src/agent_chat/ownership_disposition.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_MV69_EEnY4I3Ow4YTZvLPNpGezAWkEkH','original_deadline':1791606281.31541},
            {'original_index':94,'resource':'file:.ownership-disposition-layout-resource/src/agent_chat/service.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_Afur7APdBcMzdOggC5jAYNnBU3o62Y1U','original_deadline':1791606283.297556},
            {'original_index':95,'resource':'file:.ownership-disposition-layout-resource/tests/test_ownership_disposition.py','owner_session':'session_3d8278d80221401e939587db90280dc1','reservation_id':'reservation_JXaQYpH-yr54Ufka6LDZPCB1x0TBEjIq','original_deadline':1791606285.386085},
        ]

    @staticmethod
    def _wbt_n6lb_wrong_owner_candidates():
        return [
            {'original_index':84,'resource':'file:.ownership-disposition-0056/docs/reference/agent-operations.md','owner_session':'session_j7i_Q3IA5xmqmb23XxYS1FNdLnpgBctH','reservation_id':'reservation_SRQgJyJ9pO6X_DvRQBn9W7JJcEi6XpK4','original_deadline':1791560641.452609},
            {'original_index':85,'resource':'file:.ownership-disposition-0056/src/agent_chat/ownership_disposition.py','owner_session':'session_j7i_Q3IA5xmqmb23XxYS1FNdLnpgBctH','reservation_id':'reservation_83WL46LEBEdGH_t1up8bR4dYr9sKTkOz','original_deadline':1791560641.153024},
            {'original_index':86,'resource':'file:.ownership-disposition-0056/src/agent_chat/service.py','owner_session':'session_j7i_Q3IA5xmqmb23XxYS1FNdLnpgBctH','reservation_id':'reservation_WX5982jUOmRrZLIi8HxXgAYtjoIBjqIO','original_deadline':1791561020.9620988},
            {'original_index':87,'resource':'file:.ownership-disposition-0056/tests/test_ownership_disposition.py','owner_session':'session_j7i_Q3IA5xmqmb23XxYS1FNdLnpgBctH','reservation_id':'reservation_bwXd7Fw4DxhkKQhAbTRPd8fsOHsRVnVg','original_deadline':1791560641.306324},
            {'original_index':88,'resource':'file:.ownership-disposition-0057/docs/reference/agent-operations.md','owner_session':'session_j7i_Q3IA5xmqmb23XxYS1FNdLnpgBctH','reservation_id':'reservation_WPeTsiSQjo8SAHd-UqrP1rHClnaGLCmL','original_deadline':1791562885.018125},
            {'original_index':89,'resource':'file:.ownership-disposition-0057/src/agent_chat/ownership_disposition.py','owner_session':'session_j7i_Q3IA5xmqmb23XxYS1FNdLnpgBctH','reservation_id':'reservation_y6S1fOxM8xPxxSP5SJ6zhQ5kW_YlSd6I','original_deadline':1791562884.728085},
            {'original_index':90,'resource':'file:.ownership-disposition-0057/src/agent_chat/service.py','owner_session':'session_j7i_Q3IA5xmqmb23XxYS1FNdLnpgBctH','reservation_id':'reservation_wgKiBkkvmn76403kYPFJBa_ldZ4Uyuso','original_deadline':1791562885.150561},
            {'original_index':91,'resource':'file:.ownership-disposition-0057/tests/test_ownership_disposition.py','owner_session':'session_j7i_Q3IA5xmqmb23XxYS1FNdLnpgBctH','reservation_id':'reservation_keGlqUXIxy3kBYADIfUzYkvaviwyxwUS','original_deadline':1791562884.874487},
        ]

    def _wbt_xu0j_context(self,arguments,candidates,rows=None):
        return self._wbt_resumed_synthetic_context(arguments,candidates,rows)

    def _wbt_nwc_resumed_context(self,arguments,candidates,rows=None):
        return self._wbt_resumed_synthetic_context(arguments,candidates,rows)

    def _wbt_resumed_synthetic_context(self,arguments,candidates,rows=None):
        # All four manifests are test-owned structural fixtures, never history.
        case=self
        class SyntheticContext:
            def __enter__(self):
                with contextlib.ExitStack() as stack:
                    source_rows=rows if rows is not None else case._wbt_file_class_rows(arguments,candidates)
                    stack.enter_context(case._wbt_artifact(arguments,rows=source_rows))
                    source=Path(expired_claim_module._WBT_COHORT_ARTIFACT_PATH)
                    data=source.read_bytes(); source_hash=hashlib.sha256(data).hexdigest()
                    owner='session_3d8278d80221401e939587db90280dc1'
                    def encode(selected):
                        return b''.join(subprocess.run(['jq','-cn','--argjson','index',str(r['original_index']),
                            '--arg','resource',r['resource'],'--arg','owner',r['owner_session'],
                            '--arg','reservation',r['reservation_id'],'--argjson','deadline',repr(r['original_deadline']),
                            '{original_index:$index,resource:$resource,owner_session:$owner,reservation_id:$reservation,original_deadline:$deadline}'],capture_output=True,check=True).stdout for r in selected)
                    def emit(selected, program, extra):
                        return subprocess.run(['jq','-sc','--arg','artifact',str(source),'--argjson','bytes',str(len(data)),
                            '--arg','sha',source_hash,'--arg','owner',owner]+extra+[program],input=encode(selected),capture_output=True,check=True).stdout
                    old_program="""{schema:"wbt-offline-file-class-candidates-v1",owner_session:$owner,resource_class:"literal canonical file: only",evidence_kind:$evidence,selection_order:"original source index ascending",completed_exclusion_count:$count,completed_exclusions:[range(2;($count+2))|. as $i|{original_index:$i,original_deadline:1,owner_session:$owner,reservation_id:("synthetic-rid-"+($i|tostring)),resource:("file:synthetic-"+($i|tostring)),outcome_status:"applied",request_id:("synthetic-request-"+($i|tostring)),outcome_source:"synthetic-only"}],selected_original_indexes:[.[].original_index],selected:.,source:{path:$artifact,bytes:$bytes,sha256:$sha,association:"original LQZ/CqD cohort"}}"""
                    new_program="""{schema:"wbt_offline_next_eight_manifest_v1",purpose:"Historical candidate selection only; not current eligibility or authorization to act.",current_scoped_source_prestate:{},provenance_qualification:"synthetic test only",source:{path:$artifact,bytes:$bytes,sha256:$sha,association:"Original LQZ/CqD linked classification; 295 rows; dated supported status snapshot, not current."},selection:{ordering:"first qualifying rows in original zero-based source-index order",owner_session:$owner,resource_class:"literal canonical file: only",selected:(map(.+{global_resource_matches:1,global_reservation_matches:1})),selected_count:8,selected_original_indexes:[.[].original_index],uniqueness:{each_global_resource_and_reservation_occurrence_count:$unique,within_selected_reservation_ids_unique:true,within_selected_resources_unique:true}},completed_exclusions:{count:$count,original_indexes:[range(2;($count+2))],scope:"synthetic test only",items:[range(2;($count+2))|. as $i|{original_index:$i,original_deadline:1,owner_session:$owner,reservation_id:("synthetic-rid-"+($i|tostring)),resource:("file:synthetic-"+($i|tostring)),outcome_status:"applied",request_id:("synthetic-request-"+($i|tostring)),source_association:{file:"synthetic",sha256:("0"*64),json_pointer:"/synthetic"}}]}}"""
                    nwc=case._wbt_nwc_resumed_candidates()
                    cases=[('_WBT_NWC_RESUMED_V1_MANIFEST',nwc,old_program,['--arg','evidence','historical cohort selection only; not current eligibility or current claim count','--argjson','count','62']),
                           ('_WBT_NWC_RESUMED_MANIFEST',nwc[1:],old_program,['--arg','evidence','Offline historical candidates only; no current eligibility or remaining-count claim','--argjson','count','63']),
                           ('_WBT_XU0J_MANIFEST',case._wbt_xu0j_candidates(),new_program,['--argjson','count','70','--argjson','unique','1']),
                           ('_WBT_N6LB_MANIFEST',case._wbt_n6lb_candidates(),new_program,['--argjson','count','78','--argjson','unique','true'])]
                    pins={'_WBT_NWC_RESUMED_SOURCE_PATH':str(source),'_WBT_NWC_RESUMED_SOURCE_SHA256':source_hash,'_WBT_NWC_RESUMED_SOURCE_SIZE':len(data),'_WBT_NWC_RESUMED_SOURCE_ROWS':len(source_rows)}
                    for stem,selected,program,extra in cases:
                        raw=emit(selected,program,extra);path=case.master.parent/('synthetic-'+stem+'.json');path.write_bytes(raw)
                        pins.update({stem+'_PATH':str(path),stem+'_SHA256':hashlib.sha256(raw).hexdigest(),stem+'_SIZE':len(raw)})
                    stack.enter_context(patch.multiple('agent_chat.expired_claim_disposition',**pins))
                    self.stack=stack.pop_all()
                return self
            def __exit__(self,*exc): return self.stack.__exit__(*exc)
        return SyntheticContext()

    def _wbt_resumed_arguments(self,original_index=80):
        candidates=(self._wbt_hqg_file_class_candidates()+self._wbt_nwc_resumed_candidates()+
                    self._wbt_xu0j_candidates()+self._wbt_n6lb_candidates()+
                    self._wbt_n6lb_wrong_owner_candidates())
        chosen=next(row for row in candidates if row['original_index']==original_index)
        with patch('agent_chat.root_lifecycle.stopped',return_value=False):
            arguments,target_coord,target,_=self.wbt_abandon_arguments('wbt:exact-row')
        target_coord.session=chosen['owner_session']
        target_coord.register('reviewer_resumed_file_class')
        target_coord.db.execute('UPDATE resources SET name=?,owner_session=?,reservation_id=?,deadline=?,stale=1 WHERE name=?',
            (chosen['resource'],chosen['owner_session'],chosen['reservation_id'],chosen['original_deadline'],'wbt:exact-row'))
        target_coord.db.execute('DELETE FROM bridge_bindings WHERE session_id=?',('wbt-owner',))
        target_coord.db.execute('DELETE FROM remote_session_hosts WHERE session_id=?',('wbt-owner',))
        target_coord.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?,?,?)',
            (chosen['owner_session'],'thread-wbt-resumed-owner',None,None,time.time()))
        target_coord.db.execute('INSERT INTO remote_session_hosts VALUES(?,?,?,?)',
            (chosen['owner_session'],'host-root',time.time(),time.time()))
        operation_created=target_coord.db.execute(
            'SELECT created_at FROM root_team_stops WHERE id=?',(self.STOP,)).fetchone()[0]
        with contextlib.closing(sqlite3.connect(self.registry)) as registry:
            registry.execute('UPDATE root_team_operations SET created_at=? WHERE id=?',
                (operation_created-1,self.STOP))
            registry.execute('UPDATE root_team_operations SET resumed_at=? WHERE id=?',
                (operation_created+3,self.STOP))
            registry.commit()
        target_coord.db.execute('UPDATE root_team_stops SET active=0,resumed_at=? WHERE id=?',
            (operation_created+2,self.STOP))
        target_coord.db.commit()
        arguments.update(action='wbt-resumed-prepare',admission_mode='resumed-project-stop-v1',
            root_stop_request_id=self.STOP,resource=chosen['resource'],
            owner_session=chosen['owner_session'],reservation_id=chosen['reservation_id'],
            expected_deadline=chosen['original_deadline'])
        return arguments,target_coord,target,chosen,candidates,self._wbt_xu0j_context(arguments,candidates)

    def test_wbt_resumed_file_manifest_prepares_and_applies_under_exact_completed_stop(self):
        arguments,target_coord,target,chosen,candidates,artifact=self._wbt_resumed_arguments()
        new_candidates=self._wbt_n6lb_candidates()
        with artifact:
            for row in new_candidates:
                selected=expired_claim_module.wbt_witness_selection(self.root,_WBT_PROJECT_ID,
                    row['resource'],row['owner_session'],row['reservation_id'],
                    row['original_deadline'],_WBT_COHORT_REF)
                self.assertEqual(expired_claim_module._wbt_n6lb_manifest_index(selected),
                    row['original_index'])
                self.assertIsNone(expired_claim_module._wbt_xu0j_manifest_index(selected))
                self.assertIsNone(expired_claim_module._wbt_nwc_resumed_manifest_index(selected))
                self.assertIsNone(expired_claim_module._wbt_nwc_resumed_v1_legacy_manifest_index(selected))
                self.assertIsNone(expired_claim_module._wbt_n6lb_manifest_index(
                    selected[:6]+('foreign-owner',)+selected[7:]))
                self.assertIsNone(expired_claim_module._wbt_n6lb_manifest_index(
                    selected[:5]+('file:unreviewed-resource',)+selected[6:]))
                self.assertIsNone(expired_claim_module._wbt_n6lb_manifest_index(
                    selected[:7]+('reservation:unreviewed',)+selected[8:]))
                self.assertIsNone(expired_claim_module._wbt_n6lb_manifest_index(
                    selected[:8]+(selected[8]+0.25,)))
            for row in self._wbt_n6lb_wrong_owner_candidates():
                selected=expired_claim_module.wbt_witness_selection(self.root,_WBT_PROJECT_ID,
                    row['resource'],row['owner_session'],row['reservation_id'],
                    row['original_deadline'],_WBT_COHORT_REF)
                self.assertIsNone(expired_claim_module._wbt_n6lb_manifest_index(selected))
                self.assertIsNone(expired_claim_module._wbt_xu0j_manifest_index(selected))
            for row in self._wbt_nwc_resumed_candidates():
                if row['original_index'] not in expired_claim_module._WBT_NWC_RESUMED_ROW_INDICES:
                    continue
                selected=expired_claim_module.wbt_witness_selection(self.root,_WBT_PROJECT_ID,
                    row['resource'],row['owner_session'],row['reservation_id'],
                    row['original_deadline'],_WBT_COHORT_REF)
                self.assertIsNone(expired_claim_module._wbt_n6lb_manifest_index(selected))
                self.assertIsNone(expired_claim_module._wbt_xu0j_manifest_index(selected))
                self.assertEqual(expired_claim_module._wbt_nwc_resumed_manifest_index(selected),
                    row['original_index'])
            old_row=next(row for row in candidates if row['original_index']==64)
            old_selected=expired_claim_module.wbt_witness_selection(self.root,_WBT_PROJECT_ID,
                old_row['resource'],old_row['owner_session'],old_row['reservation_id'],
                old_row['original_deadline'],_WBT_COHORT_REF)
            self.assertIsNone(expired_claim_module._wbt_xu0j_manifest_index(old_selected))
            self.assertIsNone(expired_claim_module._wbt_nwc_resumed_manifest_index(old_selected))
            self.assertEqual(expired_claim_module._wbt_nwc_resumed_v1_legacy_manifest_index(old_selected),64)
            with patch.object(expired_claim_module,'_WBT_N6LB_MANIFEST_SHA256','0'*64):
                self.assertIsNone(expired_claim_module._wbt_n6lb_manifest_index(
                    expired_claim_module.wbt_witness_selection(self.root,_WBT_PROJECT_ID,
                        new_candidates[0]['resource'],new_candidates[0]['owner_session'],
                        new_candidates[0]['reservation_id'],new_candidates[0]['original_deadline'],
                        _WBT_COHORT_REF)))
        stopped_route=dict(arguments,action='wbt-abandon-prepare',
            admission_mode=None,root_stop_request_id=None,
            request_id='55555555-aaaa-4bbb-8ccc-555555555555')
        with artifact,patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample',side_effect=AssertionError('stopped route must not sample')):
            with self.assertRaises(CoordError):
                execute_expired_claim(**stopped_route)
        self.assertFalse(expired_claim_module._has_table(target_coord.db,
            expired_claim_module._WBT_ABANDON_INTENTS))
        def exercise(case_arguments,case_coord,case_target,case_row,case_artifact):
            with self.subTest(productive_original_index=case_row['original_index']):
                with case_artifact,patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                        patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                        patch('agent_chat.expired_claim_disposition._sample',side_effect=self._passive_not_loaded_sample):
                    prepared=dict(execute_expired_claim(**case_arguments))
                    self.assertEqual(prepared['status'],'prepared')
                    altered=dict(case_arguments,expires_at=case_arguments['expires_at']+1)
                    with self.assertRaisesRegex(CoordError,'different immutable payload'):
                        execute_expired_claim(**altered)
                    stored=case_coord.db.execute('SELECT payload FROM root_wbt_file_abandon_intents WHERE request_id=?',
                        (case_arguments['request_id'],)).fetchone()
                    self.assertIn("'wbt-file-abandon-v14-resumed-stop-v1'",stored[0])
                    self.assertIn(self.STOP,stored[0])
                    apply_args=dict(case_arguments,action='wbt-resumed-apply',
                        expected_digest=prepared['prepared_digest'])
                    applied=dict(execute_expired_claim(**apply_args))
                    self.assertEqual(applied['status'],'applied')
                    replay=dict(execute_expired_claim(**apply_args))
                    self.assertEqual(replay['status'],'applied')
                    self.assertTrue(replay['replayed'])
                with contextlib.closing(sqlite3.connect(case_target)) as case_db:
                    self.assertEqual(case_db.execute('SELECT stale FROM resources WHERE name=?',
                        (case_row['resource'],)).fetchone()[0],0)
                    self.assertEqual(tuple(case_db.execute(
                        'SELECT active,resumed_at FROM root_team_stops WHERE id=?',(self.STOP,)).fetchone())[0],0)
        case_arguments=arguments
        case_coord=target_coord
        case_target=target
        case_row=chosen
        case_artifact=artifact
        for position,row in enumerate(new_candidates):
            if position:
                case_coord.db.execute('UPDATE resources SET name=?,owner_session=?,reservation_id=?,deadline=?,stale=1,reason=NULL WHERE name=?',
                    (row['resource'],row['owner_session'],row['reservation_id'],
                     row['original_deadline'],case_row['resource']))
                case_coord.db.commit()
                case_arguments=dict(arguments,resource=row['resource'],owner_session=row['owner_session'],
                    reservation_id=row['reservation_id'],expected_deadline=row['original_deadline'],
                    request_id=f"77777777-aaaa-4bbb-8ccc-{row['original_index']:012d}",
                    expires_at=time.time()+300)
                case_row=row
            exercise(case_arguments,case_coord,case_target,case_row,case_artifact)

    def test_wbt_synthetic_72_79_v13_same_id_payload_is_read_only(self):
        historical_expiry={72:1792350859.0,73:1792352277.0,74:1792353539.0,
            75:1792354598.0,76:1792355261.0,77:1792356361.0,
            78:1792357376.0,79:1792358573.0}
        historical_rows=self._wbt_xu0j_candidates()
        first=historical_rows[0]
        arguments,target_coord,target,chosen,candidates,artifact=self._wbt_resumed_arguments(
            first['original_index'])
        current=first
        for row in historical_rows:
            with self.subTest(original_index=row['original_index']):
                if row['original_index']!=current['original_index']:
                    target_coord.db.execute('UPDATE resources SET name=?,owner_session=?,reservation_id=?,deadline=?,stale=1,reason=NULL WHERE name=?',
                        (row['resource'],row['owner_session'],row['reservation_id'],
                         row['original_deadline'],current['resource']))
                    target_coord.db.commit()
                    current=row
                arguments=dict(arguments,resource=row['resource'],owner_session=row['owner_session'],
                    reservation_id=row['reservation_id'],expected_deadline=row['original_deadline'],
                    request_id=f"88888888-aaaa-4bbb-8ccc-{row['original_index']:012d}",
                    expires_at=historical_expiry[row['original_index']])
                self._assert_legacy_stored_outcome_is_same_id_read_only(
                    arguments,target_coord,target,artifact,row['original_index'],
                    nwc_resumed_candidates=historical_rows)

    def test_wbt_completed_72_79_fresh_uuid_denied_before_bootstrap_or_sample(self):
        historical_rows=self._wbt_xu0j_candidates()
        first=historical_rows[0]
        arguments,target_coord,target,chosen,candidates,artifact=self._wbt_resumed_arguments(
            first['original_index'])
        current=first
        for row in historical_rows:
            with self.subTest(original_index=row['original_index']):
                if row['original_index']!=current['original_index']:
                    target_coord.db.execute('UPDATE resources SET name=?,owner_session=?,reservation_id=?,deadline=?,stale=1,reason=NULL WHERE name=?',
                        (row['resource'],row['owner_session'],row['reservation_id'],
                         row['original_deadline'],current['resource']))
                    target_coord.db.commit()
                    current=row
                arguments=dict(arguments,resource=row['resource'],owner_session=row['owner_session'],
                    reservation_id=row['reservation_id'],expected_deadline=row['original_deadline'],
                    request_id=f"99999999-aaaa-4bbb-8ccc-{row['original_index']:012d}",
                    expires_at=1792350859)
                before=self._database_dump(target)
                self.assertEqual(target_coord.db.execute(
                    "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
                    (expired_claim_module._WBT_ABANDON_INTENTS,)).fetchone()[0],0)
                with artifact,patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                        patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                        patch('agent_chat.expired_claim_disposition._sample') as native_sample:
                    with self.assertRaisesRegex(CoordError,'same-ID reconciliation only'):
                        execute_expired_claim(**arguments)
                    native_sample.assert_not_called()
                self.assertEqual(self._database_dump(target),before)
                self.assertEqual(target_coord.db.execute(
                    "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
                    (expired_claim_module._WBT_ABANDON_INTENTS,)).fetchone()[0],0)

    def test_wbt_synthetic_65_71_v12_same_id_payload_is_read_only(self):
        historical_expiry={65:1792339161.0,66:1792340644.0,67:1792341647.0,
            68:1792342662.0,69:1792343409.0,70:1792344090.0,71:1792345361.0}
        historical_rows=self._wbt_nwc_resumed_candidates()
        first=historical_rows[1]
        arguments,target_coord,target,chosen,candidates,artifact=self._wbt_resumed_arguments(
            first['original_index'])
        current=first
        for row in historical_rows[1:]:
            with self.subTest(original_index=row['original_index']):
                if row['original_index']!=current['original_index']:
                    target_coord.db.execute('UPDATE resources SET name=?,owner_session=?,reservation_id=?,deadline=?,stale=1,reason=NULL WHERE name=?',
                        (row['resource'],row['owner_session'],row['reservation_id'],
                         row['original_deadline'],current['resource']))
                    target_coord.db.commit()
                    current=row
                arguments=dict(arguments,resource=row['resource'],owner_session=row['owner_session'],
                    reservation_id=row['reservation_id'],expected_deadline=row['original_deadline'],
                    request_id=f"66666666-aaaa-4bbb-8ccc-{row['original_index']:012d}",
                    expires_at=historical_expiry[row['original_index']])
                self._assert_legacy_stored_outcome_is_same_id_read_only(
                    arguments,target_coord,target,artifact,row['original_index'],
                    nwc_resumed_candidates=self._wbt_nwc_resumed_candidates())

    def test_wbt_completed_65_71_fresh_uuid_denied_before_bootstrap_or_sample(self):
        historical_rows=self._wbt_nwc_resumed_candidates()
        first=historical_rows[1]
        arguments,target_coord,target,chosen,candidates,artifact=self._wbt_resumed_arguments(
            first['original_index'])
        current=first
        for row in historical_rows[1:]:
            with self.subTest(original_index=row['original_index']):
                if row['original_index']!=current['original_index']:
                    target_coord.db.execute('UPDATE resources SET name=?,owner_session=?,reservation_id=?,deadline=?,stale=1,reason=NULL WHERE name=?',
                        (row['resource'],row['owner_session'],row['reservation_id'],
                         row['original_deadline'],current['resource']))
                    target_coord.db.commit()
                    current=row
                arguments=dict(arguments,resource=row['resource'],owner_session=row['owner_session'],
                    reservation_id=row['reservation_id'],expected_deadline=row['original_deadline'],
                    request_id=f"33333333-aaaa-4bbb-8ccc-{row['original_index']:012d}")
                arguments['expires_at']=1792345361
                before=self._database_dump(target)
                self.assertEqual(target_coord.db.execute(
                    "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
                    (expired_claim_module._WBT_ABANDON_INTENTS,)).fetchone()[0],0)
                with artifact,patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                        patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                        patch('agent_chat.expired_claim_disposition._sample') as native_sample:
                    with self.assertRaisesRegex(CoordError,'same-ID reconciliation only'):
                        execute_expired_claim(**arguments)
                    native_sample.assert_not_called()
                self.assertEqual(self._database_dump(target),before)
                self.assertEqual(target_coord.db.execute(
                    "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
                    (expired_claim_module._WBT_ABANDON_INTENTS,)).fetchone()[0],0)

    def test_wbt_synthetic_manifest_keeps_index64_v11_same_id_payload_read_only(self):
        arguments,target_coord,target,chosen,candidates,artifact=self._wbt_resumed_arguments(64)
        arguments=arguments|{'expires_at':time.time()-0.1}
        self._assert_legacy_stored_outcome_is_same_id_read_only(
            arguments,target_coord,target,artifact,64,nwc_resumed_candidates=candidates)

    def test_wbt_resumed_manifest_denies_fresh_index64_before_bootstrap_or_sample(self):
        arguments,target_coord,target,chosen,candidates,artifact=self._wbt_resumed_arguments(64)
        arguments=arguments|{'request_id':'33333333-aaaa-4bbb-8ccc-333333333333'}
        before=self._database_dump(target)
        self.assertEqual(target_coord.db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
            (expired_claim_module._WBT_ABANDON_INTENTS,)).fetchone()[0],0)
        with artifact,patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample') as native_sample:
            with self.assertRaisesRegex(CoordError,'same-ID reconciliation only'):
                execute_expired_claim(**arguments)
            native_sample.assert_not_called()
        self.assertEqual(self._database_dump(target),before)
        self.assertEqual(target_coord.db.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?",
            (expired_claim_module._WBT_ABANDON_INTENTS,)).fetchone()[0],0)

    def test_wbt_resumed_file_manifest_requires_exact_completed_current_witness_before_intent(self):
        cases=('wrong_id','active_global','bad_time','active_target','wrong_enrollment')
        arguments,target_coord,target,chosen,candidates,artifact=self._wbt_resumed_arguments()
        with contextlib.closing(sqlite3.connect(self.registry)) as registry:
            original_operation_resumed=registry.execute(
                'SELECT resumed_at FROM root_team_operations WHERE id=?',(self.STOP,)).fetchone()[0]
            original_enrollment=registry.execute(
                'SELECT session_id FROM root_projects WHERE project_id=?',(_WBT_PROJECT_ID,)).fetchone()[0]
        original_target_stop=tuple(target_coord.db.execute(
            'SELECT active,resumed_at FROM root_team_stops WHERE id=?',(self.STOP,)).fetchone())
        for case_name in cases:
            with self.subTest(case=case_name):
                original_stop_id=arguments['root_stop_request_id']
                with contextlib.closing(sqlite3.connect(self.registry)) as registry:
                    if case_name=='wrong_id':
                        arguments['root_stop_request_id']='11111111-2222-4333-8444-555555555555'
                    elif case_name=='active_global':
                        registry.execute('INSERT INTO root_team_operations VALUES(?,?,?,NULL)',
                            ('second-stop','root',time.time()))
                    elif case_name=='bad_time':
                        registry.execute('UPDATE root_team_operations SET resumed_at=? WHERE id=?',
                            (original_target_stop[1]-1,self.STOP))
                    elif case_name=='wrong_enrollment':
                        registry.execute('UPDATE root_projects SET session_id=? WHERE project_id=?',
                            ('foreign-project-root',_WBT_PROJECT_ID))
                    registry.commit()
                if case_name=='active_target':
                    target_coord.db.execute('UPDATE root_team_stops SET active=1,resumed_at=NULL WHERE id=?',
                        (self.STOP,))
                    target_coord.db.commit()
                before=tuple(target_coord.db.iterdump())
                with artifact,patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                        patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                        patch('agent_chat.expired_claim_disposition._sample',side_effect=AssertionError('sample must not run')):
                    with self.assertRaises(CoordError):
                        execute_expired_claim(**arguments)
                self.assertEqual(tuple(target_coord.db.iterdump()),before)
                self.assertFalse(expired_claim_module._has_table(target_coord.db,
                    expired_claim_module._WBT_ABANDON_INTENTS))
                arguments['root_stop_request_id']=original_stop_id
                with contextlib.closing(sqlite3.connect(self.registry)) as registry:
                    if case_name=='active_global':
                        registry.execute('DELETE FROM root_team_operations WHERE id=?',('second-stop',))
                    elif case_name=='bad_time':
                        registry.execute('UPDATE root_team_operations SET resumed_at=? WHERE id=?',
                            (original_target_stop[1]-1,self.STOP))
                    elif case_name=='wrong_enrollment':
                        registry.execute('UPDATE root_projects SET session_id=? WHERE project_id=?',
                            (original_enrollment,_WBT_PROJECT_ID))
                    registry.commit()
                if case_name=='active_target':
                    target_coord.db.execute('UPDATE root_team_stops SET active=?,resumed_at=? WHERE id=?',
                        (*original_target_stop,self.STOP))
                    target_coord.db.commit()
                if case_name=='bad_time':
                    with contextlib.closing(sqlite3.connect(self.registry)) as registry:
                        registry.execute('UPDATE root_team_operations SET resumed_at=? WHERE id=?',
                            (original_operation_resumed,self.STOP))
                        registry.commit()

    def test_wbt_resumed_mode_keeps_completed_hqg_tuple_readonly_and_denies_new_uuid_prebootstrap(self):
        arguments,target_coord,target,new_row,candidates,artifact=self._wbt_resumed_arguments()
        old_row=next(row for row in candidates if row['original_index']==56)
        target_coord.db.execute('UPDATE resources SET name=?,owner_session=?,reservation_id=?,deadline=?,stale=1 WHERE name=?',
            (old_row['resource'],old_row['owner_session'],old_row['reservation_id'],
             old_row['original_deadline'],new_row['resource']))
        target_coord.db.execute('DELETE FROM bridge_bindings WHERE session_id=?',(new_row['owner_session'],))
        target_coord.db.execute('DELETE FROM remote_session_hosts WHERE session_id=?',(new_row['owner_session'],))
        target_coord.db.execute('INSERT INTO bridge_bindings VALUES(?,?,?,?,?)',
            (old_row['owner_session'],'thread-wbt-historical-owner',None,None,time.time()))
        target_coord.db.execute('INSERT INTO remote_session_hosts VALUES(?,?,?,?)',
            (old_row['owner_session'],'host-root',time.time(),time.time()))
        target_coord.db.commit()
        arguments.update(resource=old_row['resource'],owner_session=old_row['owner_session'],
            reservation_id=old_row['reservation_id'],expected_deadline=old_row['original_deadline'])
        before=tuple(target_coord.db.iterdump())
        with self._wbt_hqg_file_class_context(arguments,self._wbt_hqg_file_class_candidates()),patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample',side_effect=AssertionError('legacy row must not be sampled')):
            with self.assertRaisesRegex(CoordError,'same-ID reconciliation only'):
                execute_expired_claim(**arguments)
        self.assertEqual(tuple(target_coord.db.iterdump()),before)
        self.assertFalse(expired_claim_module._has_table(target_coord.db,
            expired_claim_module._WBT_ABANDON_INTENTS))

    def test_wbt_resumed_apply_rechecks_changed_stop_before_native_sample(self):
        arguments,target_coord,target,chosen,candidates,artifact=self._wbt_resumed_arguments()
        with artifact,patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample',side_effect=self._passive_not_loaded_sample):
            prepared=dict(execute_expired_claim(**arguments))
            self.assertEqual(prepared['status'],'prepared')
            target_coord.db.execute('UPDATE root_team_stops SET active=1,resumed_at=NULL WHERE id=?',
                (self.STOP,))
            target_coord.db.commit()
            apply_args=dict(arguments,action='wbt-resumed-apply',expected_digest=prepared['prepared_digest'])
            with patch('agent_chat.expired_claim_disposition._sample',side_effect=AssertionError('drifted stop must block before native sample')):
                with self.assertRaisesRegex(CoordError,'active stop'):
                    execute_expired_claim(**apply_args)
            state=target_coord.db.execute('SELECT state FROM root_wbt_file_abandon_intents WHERE request_id=?',
                (arguments['request_id'],)).fetchone()[0]
            self.assertEqual(state,'prepared')
            self.assertEqual(target_coord.db.execute('SELECT stale FROM resources WHERE name=?',
                (chosen['resource'],)).fetchone()[0],1)

    def test_wbt_resumed_overlapping_apply_has_one_effect(self):
        arguments,target_coord,target,chosen,candidates,artifact=self._wbt_resumed_arguments()
        with artifact,patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample',side_effect=self._passive_not_loaded_sample):
            prepared=dict(execute_expired_claim(**arguments))
        apply_args=dict(arguments,action='wbt-resumed-apply',expected_digest=prepared['prepared_digest'])
        entered=threading.Event(); release=threading.Event()
        sample_lock=threading.Lock(); sample_count={'value':0}
        def held_sample(thread_id,endpoint,*,strict):
            with sample_lock:
                sample_count['value']+=1
            entered.set()
            if not release.wait(5):
                raise RuntimeError('test did not release resumed apply sample')
            return self._passive_not_loaded_sample(thread_id,endpoint,strict=strict)
        with artifact,patch('agent_chat.root_local._installed_server_config',return_value=self.config),\
                patch('agent_chat.root_local.configured_server_database',return_value=self.master),\
                patch('agent_chat.expired_claim_disposition._sample',side_effect=held_sample):
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                first=pool.submit(execute_expired_claim,**apply_args)
                self.assertTrue(entered.wait(5))
                second=dict(execute_expired_claim(**apply_args))
                release.set()
                first_result=dict(first.result(timeout=5))
        self.assertEqual(first_result['status'],'applied')
        self.assertEqual(second['status'],'applying')
        self.assertEqual(sample_count['value'],1)
        self.assertEqual(target_coord.db.execute('SELECT stale FROM resources WHERE name=?',
            (chosen['resource'],)).fetchone()[0],0)
        self.assertEqual(target_coord.db.execute(
            'SELECT count(*) FROM root_expired_claim_dispositions WHERE request_id=?',
            (arguments['request_id'],)).fetchone()[0],1)
        states=tuple(row['state'] for row in target_coord.db.execute(
            'SELECT state FROM root_wbt_file_abandon_events WHERE request_id=? ORDER BY seq',
            (arguments['request_id'],)))
        self.assertEqual(states,('accepted','prepared','applying','applied'))


if __name__ == '__main__':
    unittest.main()
