"""Durable owner supervision and incident-linked exact-thread recovery."""
import hashlib
import json
import math
import pathlib
import sqlite3
import time
import uuid

from .core import CoordError


SYSTEM_SENDER = 'system:supervisor'
_UNSET = object()
ACTIONABLE = ('unavailable_host', 'stale_host', 'error_host', 'no_binding',
              'unloaded_pending', 'uncertain_wake', 'native_error',
              'source_report_pending')
HOST_CONDITIONS = ('unavailable_host', 'stale_host', 'error_host', 'no_binding')
_NATIVE_ERROR_PHASES = frozenset(
    [prefix + phase for prefix in ('', 'recovery-before-resume:', 'recovery-after-resume:')
     for phase in ('thread/read:before','thread/queue/list','thread/turns/list','thread/read:after','snapshot:interpret')]
    + ['recovery:claim','recovery:prepare-wake','recovery:attempt-admission',
       'recovery:thread/resume','recovery:ready-report','recovery:uncertain-report',
       'recovery:consumption-check','observation:classify','observation:report'])
_REPORT_MAX_DELIVERIES = 32
_REPORT_MAX_REPLIES = 8
_REPORT_MAX_ITEM_BYTES = 8192
_REPORT_MAX_BYTES = 16 * 1024
_REPORT_MAX_ID_BYTES = 256


def reject_cold_epoch(db, *, method, thread, actor, host, wake_id, reason):
    if method != 'thread/resume' or not thread or not wake_id:
        return False
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='cold_message_loads'").fetchone():
        return False
    row = db.execute("SELECT source_id FROM cold_message_loads WHERE thread_id=? AND target_session=? AND host_id=? AND wake_id=? AND status='claimed'", (thread,actor,host,wake_id)).fetchone()
    if not row:
        return False
    now=time.time()
    db.execute("UPDATE cold_message_loads SET status='rejected',updated_at=? WHERE source_id=? AND status='claimed'", (now,row['source_id']))
    db.execute('INSERT INTO cold_message_load_events(source_id,status,detail,created_at) VALUES(?,?,?,?)', (row['source_id'],'rejected',str(reason)[:512],now))
    return True


def cold_epoch_check(coord, db, *, method, thread, actor, host, wake_id):
    """Revalidate a pending cold load inside the native epoch transaction."""
    if method != 'thread/resume' or not thread:
        return
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='cold_message_loads'").fetchone():
        return
    rows = db.execute("SELECT * FROM cold_message_loads WHERE thread_id=? AND status IN ('claimed','attempting')", (thread,)).fetchmany(2)
    if not rows:
        return
    if len(rows) != 1:
        raise CoordError('ambiguous cold message epoch')
    row = rows[0]
    if row['status'] != 'claimed' or (row['wake_id'], row['target_session'], row['host_id']) != (wake_id, actor, host):
        raise CoordError('cold message epoch identity changed')
    supervisor = SupervisorState.__new__(SupervisorState)
    supervisor.coord, supervisor.db = coord, db
    job = dict(row)
    supervisor._cold_check(job, host, pending_wake=wake_id)
    from .project_activity import non_runnable_task_sources
    if (row['source_id'] in non_runnable_task_sources(db)
            or not supervisor._recovery_delivery_eligible(row['source_id'], actor)):
        raise CoordError('cold message source no longer eligible')
    linked = db.execute('SELECT message_id FROM bridge_deliveries WHERE job_id=?', (wake_id,)).fetchall()
    pristine = db.execute('''SELECT 1 FROM bridge_jobs j JOIN bridge_job_origins o ON o.job_id=j.id
        WHERE j.id=? AND j.thread_id=? AND j.status='prepared' AND j.queue_id IS NULL AND j.error IS NULL
        AND NOT EXISTS (SELECT 1 FROM bridge_job_native_attempts a WHERE a.job_id=j.id)
        AND NOT EXISTS (SELECT 1 FROM agent_control_requests r WHERE r.native_input_job_id=j.id)
        AND NOT EXISTS (SELECT 1 FROM bridge_root_wake_batches b WHERE b.job_id=j.id)''', (wake_id,thread)).fetchone()
    if not pristine or len(linked) != 1 or linked[0]['message_id'] != row['source_id']:
        raise CoordError('cold message wake no longer pristine')
    if db.execute("SELECT 1 FROM bridge_jobs WHERE thread_id=? AND id<>? AND status NOT IN ('dispatched','cancelled')", (thread,wake_id)).fetchone():
        raise CoordError('cold message conflicts with another wake')
    now=time.time()
    db.execute("UPDATE cold_message_loads SET status='attempting',updated_at=? WHERE source_id=?", (now,row['source_id']))
    db.execute('INSERT INTO cold_message_load_events(source_id,status,created_at) VALUES(?,?,?)', (row['source_id'],'attempting',now))


class SupervisorState:
    def __init__(self, coord):
        self.coord, self.db = coord, coord.db
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS cold_message_loads (
                source_id TEXT PRIMARY KEY, wake_id TEXT NOT NULL UNIQUE,
                owner_session TEXT NOT NULL, target_session TEXT NOT NULL,
                thread_id TEXT NOT NULL, host_id TEXT NOT NULL,
                route_generation TEXT NOT NULL, status TEXT NOT NULL,
                created_at REAL NOT NULL, updated_at REAL NOT NULL);
            CREATE TRIGGER IF NOT EXISTS cold_message_load_identity_immutable
            BEFORE UPDATE OF source_id,wake_id,owner_session,target_session,thread_id,host_id,route_generation,created_at ON cold_message_loads
            BEGIN SELECT RAISE(ABORT,'cold message identity is immutable'); END;
            CREATE TRIGGER IF NOT EXISTS cold_message_load_delete_forbidden
            BEFORE DELETE ON cold_message_loads
            BEGIN SELECT RAISE(ABORT,'cold message history is immutable'); END;
            CREATE TABLE IF NOT EXISTS cold_message_load_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT, source_id TEXT NOT NULL,
                status TEXT NOT NULL, detail TEXT, created_at REAL NOT NULL);
            CREATE TRIGGER IF NOT EXISTS cold_message_event_update_forbidden BEFORE UPDATE ON cold_message_load_events
            BEGIN SELECT RAISE(ABORT,'cold message events are immutable'); END;
            CREATE TRIGGER IF NOT EXISTS cold_message_event_delete_forbidden BEFORE DELETE ON cold_message_load_events
            BEGIN SELECT RAISE(ABORT,'cold message events are immutable'); END;
            CREATE TABLE IF NOT EXISTS supervisor_policies (
                owner_session TEXT NOT NULL, target_session TEXT NOT NULL, enabled INTEGER NOT NULL,
                updated_at REAL NOT NULL, PRIMARY KEY(owner_session,target_session));
            CREATE TABLE IF NOT EXISTS supervisor_observations (
                owner_session TEXT NOT NULL, target_session TEXT NOT NULL, route_generation TEXT NOT NULL,
                source TEXT NOT NULL, state TEXT NOT NULL, summary_json TEXT NOT NULL,
                checked_at REAL NOT NULL, next_due REAL NOT NULL, error_count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(owner_session,target_session));
            CREATE TABLE IF NOT EXISTS supervisor_owner_policies (
                owner_session TEXT PRIMARY KEY, enabled INTEGER NOT NULL, updated_at REAL NOT NULL,
                recovery_enabled INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS supervisor_incidents (
                owner_session TEXT NOT NULL, target_session TEXT NOT NULL, route_generation TEXT NOT NULL,
                condition TEXT NOT NULL, first_seen REAL NOT NULL, last_seen REAL NOT NULL,
                last_sample REAL NOT NULL, message_id TEXT, active INTEGER NOT NULL DEFAULT 1,
                episode INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY(owner_session,target_session,route_generation,condition));
            CREATE TABLE IF NOT EXISTS supervisor_recoveries (
                id TEXT PRIMARY KEY, owner_session TEXT NOT NULL, target_session TEXT NOT NULL,
                route_generation TEXT NOT NULL, episode INTEGER NOT NULL, thread_id TEXT NOT NULL,
                host_id TEXT NOT NULL, status TEXT NOT NULL, message_ids_json TEXT NOT NULL,
                wake_id TEXT, native_consumed INTEGER NOT NULL DEFAULT 0,
                created_at REAL NOT NULL, updated_at REAL NOT NULL,
                UNIQUE(owner_session,target_session,route_generation,episode));
        ''')
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS supervisor_notice_provenance (
                message_id TEXT PRIMARY KEY, sender_session TEXT NOT NULL CHECK(sender_session='system:supervisor'),
                recipient_session TEXT NOT NULL, created_at REAL NOT NULL);
            CREATE TRIGGER IF NOT EXISTS supervisor_notice_provenance_update BEFORE UPDATE ON supervisor_notice_provenance
            BEGIN SELECT RAISE(ABORT,'supervisor notice provenance is immutable'); END;
            CREATE TRIGGER IF NOT EXISTS supervisor_notice_provenance_delete BEFORE DELETE ON supervisor_notice_provenance
            BEGIN SELECT RAISE(ABORT,'supervisor notice provenance is immutable'); END;
            CREATE TABLE IF NOT EXISTS supervisor_notice_migrations (version INTEGER PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS supervisor_root_routing_notices (
                request_id TEXT PRIMARY KEY,home_project TEXT NOT NULL,root_session TEXT NOT NULL,
                host_id TEXT NOT NULL,thread_id TEXT NOT NULL,message_id TEXT UNIQUE NOT NULL,created_at REAL NOT NULL);
            CREATE TRIGGER IF NOT EXISTS supervisor_root_routing_notices_update BEFORE UPDATE ON supervisor_root_routing_notices
            BEGIN SELECT RAISE(ABORT,'root routing notice receipt is immutable'); END;
            CREATE TRIGGER IF NOT EXISTS supervisor_root_routing_notices_delete BEFORE DELETE ON supervisor_root_routing_notices
            BEGIN SELECT RAISE(ABORT,'root routing notice receipt is immutable'); END;
        """)
        # Public registration reserves this exact SID; send requires a registered
        # caller. Never infer provenance from a display name or message body.
        with self.coord.tx():
            if not self.db.execute('SELECT 1 FROM supervisor_notice_migrations WHERE version=1').fetchone():
                if not self.db.execute('SELECT 1 FROM sessions WHERE id=?',(SYSTEM_SENDER,)).fetchone():
                    self.db.execute("""INSERT OR IGNORE INTO supervisor_notice_provenance
                        SELECT id,sender_session,recipient_session,created_at FROM messages WHERE sender_session=?""",(SYSTEM_SENDER,))
                self.db.execute('INSERT INTO supervisor_notice_migrations VALUES(1)')
        if 'recovery_enabled' not in {row['name'] for row in self.db.execute('PRAGMA table_info(supervisor_owner_policies)')}:
            self.db.execute('ALTER TABLE supervisor_owner_policies ADD COLUMN recovery_enabled INTEGER NOT NULL DEFAULT 0')
        if 'error_count' not in {row['name'] for row in self.db.execute('PRAGMA table_info(supervisor_observations)')}:
            self.db.execute('ALTER TABLE supervisor_observations ADD COLUMN error_count INTEGER NOT NULL DEFAULT 0')
        columns = {row['name'] for row in self.db.execute('PRAGMA table_info(supervisor_incidents)')}
        if 'active' not in columns:
            self.db.execute('ALTER TABLE supervisor_incidents ADD COLUMN active INTEGER NOT NULL DEFAULT 1')
        if 'episode' not in columns:
            self.db.execute('ALTER TABLE supervisor_incidents ADD COLUMN episode INTEGER NOT NULL DEFAULT 1')
        if 'error_signature' not in columns:
            self.db.execute('ALTER TABLE supervisor_incidents ADD COLUMN error_signature TEXT')

    def root_routing_resumed(self, roots, project, session, host, request_id):
        """Emit only a verified committed Root lifecycle effect; never caller prose."""
        root = roots.authorize(project, session, host)
        if self.coord.require_session() != session or self.coord.path.resolve() != roots.path(project).resolve():
            raise CoordError('root routing notice requires the canonical own home coordinator')
        with roots.projects.connection() as directory:
            audit = directory.execute('SELECT * FROM root_routing_pauses ORDER BY created_at DESC,id DESC LIMIT 1').fetchone()
            if (root['mode'] != 'running' or not audit or audit['id'] != request_id
                    or audit['status'] != 'resumed' or audit['resumed_at'] is None
                    or (audit['home_project'], audit['home_session'], audit['host_id'], audit['thread_id']) !=
                    (project, session, host, root['thread_id'])
                    or directory.execute("SELECT 1 FROM root_fresh_requests WHERE status<>'completed'").fetchone()):
                raise CoordError('root routing notice requires the exact completed current resume audit')
            saved = [dict(row) for row in directory.execute('SELECT project_id,session_id FROM root_routing_pause_projects WHERE request_id=? ORDER BY project_id', (request_id,))]
            current = [dict(row) for row in directory.execute('SELECT project_id,session_id FROM root_projects ORDER BY project_id')]
            if not saved or saved != current or dict(project_id=project, session_id=session) not in saved:
                raise CoordError('root routing notice retained cohort differs')
        if any(roots._routing_projection(root, row, None) for row in saved):
            raise CoordError('root routing notice requires every retained projection resumed')
        identity = (request_id, project, session, host, root['thread_id'])
        message_id = 'supervisor_routing_' + hashlib.sha256('|'.join(identity).encode()).hexdigest()[:32]
        body = ('System supervisor notice: Root routing maintenance resumed. Request=' + request_id
                + ' thread=' + root['thread_id'] + ' host=' + host
                + '. This is system lifecycle evidence, not an operator instruction or source-task completion. '
                'Existing project pauses and authorization limits remain. Consume and acknowledge this notice; '
                'system:supervisor has no reply route.')
        with self.coord.tx() as db:
            prior = db.execute('SELECT * FROM supervisor_root_routing_notices WHERE request_id=?', (request_id,)).fetchone()
            if prior:
                message = db.execute('SELECT sender_session,recipient_session,body FROM messages WHERE id=?', (message_id,)).fetchone()
                provenance = db.execute('SELECT sender_session,recipient_session FROM supervisor_notice_provenance WHERE message_id=?', (message_id,)).fetchone()
                if ((prior['request_id'], prior['home_project'], prior['root_session'], prior['host_id'], prior['thread_id']) != identity
                        or prior['message_id'] != message_id or not message or not provenance
                        or tuple(message) != (SYSTEM_SENDER, session, body) or tuple(provenance) != (SYSTEM_SENDER, session)):
                    raise CoordError('root routing completion notice receipt differs')
                return message_id
            if (db.execute('SELECT 1 FROM messages WHERE id=?', (message_id,)).fetchone()
                    or db.execute('SELECT 1 FROM supervisor_notice_provenance WHERE message_id=?', (message_id,)).fetchone()):
                raise CoordError('root routing completion notice identity already exists')
            now = time.time()
            db.execute('INSERT INTO messages(id,sender_session,recipient_session,body,created_at) VALUES(?,?,?,?,?)',
                       (message_id, SYSTEM_SENDER, session, body, now))
            db.execute('INSERT INTO supervisor_notice_provenance VALUES(?,?,?,?)', (message_id, SYSTEM_SENDER, session, now))
            db.execute('INSERT INTO supervisor_root_routing_notices VALUES(?,?,?,?,?,?,?)', (*identity, message_id, now))
        return message_id

    def _operator(self):
        try:
            return json.loads(pathlib.Path(str(self.coord.path) + '.web-session.json').read_text(encoding='utf-8')).get('id')
        except (OSError, ValueError, AttributeError):
            row = self.db.execute("SELECT id FROM sessions WHERE agent='operator'").fetchone()
            return row['id'] if row else None

    def _owner_allowed(self, owner):
        return owner not in (SYSTEM_SENDER, self._operator()) and (bool(self._root(owner)) or self._designated(owner))

    def target(self, value):
        if not isinstance(value, str) or not value:
            raise CoordError('supervisor target must be an exact registered session or agent')
        row = self.db.execute('SELECT id,agent FROM sessions WHERE id=? OR agent=? ORDER BY (id=?) DESC',
                              (value, value, value)).fetchone()
        if not row:
            raise CoordError('supervisor target must be an exact registered session or agent')
        return dict(row)

    def _root(self, session):
        from .root_orchestrator import projection
        return projection(self.db, session)

    def _designated(self, session):
        return bool(self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='project_orchestrators'").fetchone()
                    and self.db.execute('SELECT 1 FROM project_orchestrators WHERE session_id=?', (session,)).fetchone())

    def _allowed(self, owner, target):
        registered = self.db.execute('SELECT agent FROM sessions WHERE id=?', (target,)).fetchone()
        if not registered or registered['agent'] == 'operator':
            return False
        if not self._owner_allowed(owner) or target in (SYSTEM_SENDER, self._operator()):
            return False
        if self._root(owner):
            return bool(self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='project_orchestrators'").fetchone()
                        and self.db.execute('SELECT 1 FROM project_orchestrators WHERE root_session=? AND session_id=?',
                                            (owner, target)).fetchone())
        if not self._designated(owner) or target == owner or self._root(target) or self._designated(target):
            return False
        control = self.db.execute('SELECT mode FROM agent_controls WHERE target_session=?', (target,)).fetchone()
        if control and control['mode'] == 'stopped':
            return False
        # Retained unbound historical registrations are not an active team.
        # A current route, prior supervision, or a supported worker creation
        # establishes the managed relationship, including a lost binding.
        return bool(self.db.execute('SELECT 1 FROM bridge_bindings WHERE session_id=?', (target,)).fetchone()
                    or self.db.execute('SELECT 1 FROM supervisor_observations WHERE owner_session=? AND target_session=?', (owner, target)).fetchone()
                    or self.db.execute("SELECT 1 FROM session_resets WHERE target_session=? AND kind='create' AND status NOT IN ('cancelled','failed')", (target,)).fetchone())

    def _route(self, target):
        route, seen, session = [], set(), target
        while session:
            if session in seen:
                raise CoordError('cyclic bridge parent route')
            seen.add(session)
            row = self.db.execute('SELECT * FROM bridge_bindings WHERE session_id=?', (session,)).fetchone()
            if not row:
                route = None
                break
            route.append(dict(row))
            if row['thread_id']:
                route = {'thread_id': row['thread_id'], 'route': list(reversed(route))}
                break
            session = row['parent_session']
        else:
            route = None
        if isinstance(route, list):
            route = None
        binding = self.db.execute('SELECT * FROM bridge_bindings WHERE session_id=?', (target,)).fetchone()
        binding = dict(binding) if binding else None
        root_session = route['route'][0]['session_id'] if route else target
        host = self.db.execute('SELECT host_id FROM remote_session_hosts WHERE session_id=?', (root_session,)).fetchone()
        generation = hashlib.sha256(json.dumps({'binding': binding, 'route': route,
                                                'host_id': host['host_id'] if host else None},
                                               sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        return {'route': route, 'binding': binding, 'generation': generation,
                'host_id': host['host_id'] if host else None}

    def _host_condition(self, route, now):
        if not route['host_id']:
            return 'unavailable_host'
        runtime = self.db.execute('SELECT * FROM bridge_client_runtime WHERE host_id=?',
                                  (route['host_id'],)).fetchone()
        if not runtime or not runtime['pid']:
            return 'unavailable_host'
        if runtime['error']:
            return 'error_host'
        if now - runtime['heartbeat'] >= 30:
            return 'stale_host'
        return None

    def _wake_summary(self, thread_id, target):
        # Use the same migration archive boundary as ordinary bridge delivery.
        rows = [row['status'] for row in self.db.execute(
            "SELECT status FROM bridge_jobs WHERE thread_id=? AND status NOT IN ('dispatched','cancelled')", (thread_id,))]
        unread = self.db.execute('''SELECT COUNT(*) FROM messages m WHERE m.recipient_session=?
            AND m.sender_session<>m.recipient_session
            AND NOT EXISTS (SELECT 1 FROM message_reads r WHERE r.message_id=m.id
                AND r.reader_session=m.recipient_session
                AND r.provenance IN ('recipient_full_message','recipient_context_full')) AND NOT (m.acked_at IS NOT NULL AND m.acked_at <= COALESCE(CAST((SELECT value FROM meta WHERE key='message_delivery_archive_before') AS REAL),-1))
            AND NOT EXISTS (SELECT 1 FROM bridge_deliveries d WHERE d.message_id=m.id)
            AND (NOT EXISTS (SELECT 1 FROM message_batches b WHERE b.message_id=m.id)
                 OR EXISTS (SELECT 1 FROM message_attention a WHERE a.message_id=m.id))''', (target,)).fetchone()[0]
        return {'pending_wakes': len(rows), 'pending_unread_messages': unread,
                'wake_states': rows[:8], 'uncertain_wake': 'uncertain' in rows}

    def _intentional_team_stop(self, job):
        # Admission remains governed by root_lifecycle. This narrower check only
        # quiets pending-work notices for an exact, supported stop snapshot.
        try:
            row = self.db.execute('''SELECT s.id AS stop_id,t.id AS target_id
                FROM root_team_stops s JOIN root_team_stop_targets t ON t.stop_id=s.id
                WHERE s.active=1 AND s.resumed_at IS NULL
                  AND (SELECT COUNT(*) FROM root_team_stops WHERE active=1)=1
                  AND t.requester_session=s.root_session AND t.target_session=?
                  AND t.host_id=? AND t.thread_id=?
                  AND t.status IN ('pending','waiting','interrupting','completed')
                  AND EXISTS(SELECT 1 FROM root_projections r WHERE r.session_id=s.root_session)
                  AND NOT EXISTS(SELECT 1 FROM root_projections r WHERE r.session_id=t.target_session)
                  AND NOT EXISTS(SELECT 1 FROM operator_identities o WHERE o.session_id=t.target_session)''',
                (job['target_session'], job.get('host_id'), job.get('thread_id'))).fetchone()
        except sqlite3.OperationalError:
            return False  # Absent or unsupported stop schema is not intent.
        if not row:
            return False
        try:
            exact_target = row['target_id'] == str(uuid.uuid5(uuid.UUID(row['stop_id']), job['target_session']))
            if not exact_target:
                return False
            # A current PO stop admission preserves coordination authority for
            # this exact target while the rest of the team remains stopped.
            # Do not silence its pending-work incident as though it were an
            # ordinary stopped worker; lifecycle/recovery gates still apply.
            from .root_lifecycle import po_admitted
            return not po_admitted(self.db, row['stop_id'], job['target_session'])
        except (ValueError, TypeError, AttributeError):
            return False

    def _incident(self, job, condition, summary):
        if condition not in ACTIONABLE:
            return
        if condition in ('unloaded_pending', 'source_report_pending') and self._intentional_team_stop(job):
            return
        now = time.time()
        key = (job['owner_session'], job['target_session'], job['route_generation'], condition)
        error = summary.get('error') if condition == 'native_error' else None
        phase = summary.get('phase') if condition == 'native_error' else None
        if not isinstance(phase,str) or phase not in _NATIVE_ERROR_PHASES: phase = None
        legacy_signature = hashlib.sha256(error.encode('utf-8')).hexdigest() if isinstance(error,str) else None
        signature = (hashlib.sha256((phase+'\0'+error).encode('utf-8')).hexdigest()
                     if phase and isinstance(error,str) else legacy_signature)
        row = self.db.execute('''SELECT * FROM supervisor_incidents WHERE owner_session=? AND target_session=?
            AND route_generation=? AND condition=?''', key).fetchone()
        if not row:
            self.db.execute('''INSERT INTO supervisor_incidents(owner_session,target_session,route_generation,condition,first_seen,last_seen,last_sample,message_id,active,episode,error_signature)
                VALUES(?,?,?,?,?,?,?,NULL,1,1,?)''', (*key, now, now, now, signature))
            return
        if not row['active']:
            self.db.execute('''UPDATE supervisor_incidents SET active=1,episode=episode+1,first_seen=?,last_seen=?,last_sample=?,message_id=NULL,error_signature=?
                WHERE owner_session=? AND target_session=? AND route_generation=? AND condition=?''', (now, now, now, signature, *key))
            return
        # Older clients lacking phase must not reopen the same known error.
        if not phase and isinstance(error,str) and row['error_signature']:
            phased = {hashlib.sha256((item+'\0'+error).encode('utf-8')).hexdigest() for item in _NATIVE_ERROR_PHASES}
            if row['error_signature'] in phased:
                signature = row['error_signature']
        # Enrich an existing legacy error identity without creating a duplicate
        # episode for the same previously recorded error text.
        if phase and row['error_signature'] == legacy_signature and signature != legacy_signature:
            self.db.execute('''UPDATE supervisor_incidents SET error_signature=? WHERE owner_session=? AND target_session=? AND route_generation=? AND condition=?''', (signature,*key))
        elif signature is not None and row['error_signature'] is not None and signature != row['error_signature']:
            self.db.execute('''UPDATE supervisor_incidents SET episode=episode+1,first_seen=?,last_seen=?,last_sample=?,message_id=NULL,error_signature=?
                WHERE owner_session=? AND target_session=? AND route_generation=? AND condition=?''', (now,now,now,signature,*key))
            return
        if signature is not None and row['error_signature'] is None:
            self.db.execute('''UPDATE supervisor_incidents SET error_signature=? WHERE owner_session=? AND target_session=? AND route_generation=? AND condition=?''', (signature,*key))
        self.db.execute('''UPDATE supervisor_incidents SET last_seen=?,last_sample=? WHERE owner_session=?
            AND target_session=? AND route_generation=? AND condition=?''', (now, now, *key))
        if row['message_id'] or now - row['first_seen'] < 60:
            return
        current = self._route(job['target_session'])
        if current['generation'] != job['route_generation'] or current['host_id'] != job.get('host_id'):
            return
        message_id = 'supervisor_' + hashlib.sha256(('|'.join(key) + '|' + str(row['episode'])).encode()).hexdigest()[:32]
        evidence = json.dumps(summary, sort_keys=True, separators=(',', ':'))[:1200]
        body = ('System supervisor notice: ' + condition + '. owner=' + job['owner_session']
                + ' target=' + job['target_session'] + ' thread=' + str(job.get('thread_id'))
                + ' host=' + str(job.get('host_id')) + ' route_generation=' + job['route_generation']
                + ' phase=' + (phase or 'unknown') + '. Evidence: ' + evidence
                + '. Inspect this exact agent status and bridge state; retain its wake and ownership records, and do not replay an uncertain input. '
                'A queued or dispatched wake is not proof of useful execution; native start, task completion and result consumption require separate evidence. '
                'This informational notice needs consumption and ACK only; '
                'system:supervisor has no reply route. ACK is not source-task completion or process/ownership closure.')
        if condition == 'source_report_pending':
            body += (" Native history confirms only that the input was consumed; it does not "
                     "establish the source task's result. Do not replay the input. Obtain the "
                     "original recipient's factual result through the retained parent chain; "
                     "only that recipient may complete its direct acknowledged reply. Use fresh "
                     "project-scoped context with explicit own server/project/session selection. "
                     "A repeated request is not lease renewal; any further resource work requires "
                     "legitimate current ownership.")
        self.db.execute('''INSERT OR IGNORE INTO messages(id,sender_session,recipient_session,body,created_at)
            VALUES(?,?,?,?,?)''', (message_id, SYSTEM_SENDER, job['owner_session'], body, now))
        self.db.execute('INSERT OR IGNORE INTO supervisor_notice_provenance VALUES(?,?,?,?)',
                        (message_id, SYSTEM_SENDER, job['owner_session'], now))
        self.db.execute('''UPDATE supervisor_incidents SET message_id=? WHERE owner_session=? AND target_session=?
            AND route_generation=? AND condition=?''', (message_id, *key))

    def _coordinator_evidence(self, policy, route, condition, summary):
        """Persist non-native route/host evidence; it never pretends to be a worker sample."""
        self._incident(dict(policy, route_generation=route['generation'], host_id=route['host_id']), condition, summary)

    def _resolve_conditions(self, policy, route, except_condition=None, conditions=None):
        args = [policy['owner_session'], policy['target_session'], route['generation']]
        clause = ''
        if except_condition:
            clause = ' AND condition<>?'
            args.append(except_condition)
        if conditions:
            clause += ' AND condition IN (' + ','.join('?' for _ in conditions) + ')'
            args.extend(conditions)
        self.db.execute('''UPDATE supervisor_incidents SET active=0 WHERE owner_session=? AND target_session=?
            AND route_generation=? AND active=1''' + clause, args)

    def _suppress_obsolete(self, policy, route):
        """Withdraw only an as-yet-undelivered system alert after authority changes."""
        self.db.execute('''UPDATE supervisor_incidents SET active=0 WHERE owner_session=? AND target_session=?
            AND route_generation<>?''', (policy['owner_session'], policy['target_session'], route['generation']))
        rows = self.db.execute('''SELECT message_id FROM supervisor_incidents WHERE owner_session=? AND target_session=?
            AND route_generation<>? AND message_id IS NOT NULL''',
                               (policy['owner_session'], policy['target_session'], route['generation'])).fetchall()
        for row in rows:
            message_id = row['message_id']
            deleted = self.db.execute('''DELETE FROM messages WHERE id=? AND sender_session=? AND recipient_session=?
                AND NOT EXISTS (SELECT 1 FROM message_reads r WHERE r.message_id=messages.id
                    AND r.reader_session=messages.recipient_session
                    AND r.provenance IN ('recipient_full_message','recipient_context_full'))
                AND NOT EXISTS (SELECT 1 FROM bridge_deliveries WHERE message_id=messages.id)
                AND NOT EXISTS (SELECT 1 FROM message_batches WHERE message_id=messages.id)''',
                                      (message_id, SYSTEM_SENDER, policy['owner_session']))
            if deleted.rowcount:
                self.db.execute('UPDATE supervisor_incidents SET message_id=NULL WHERE message_id=?', (message_id,))

    def _withdraw_revoked(self, owner, target):
        """Revoke only this service's unsent notices, never prepared native input."""
        for row in self.db.execute('SELECT message_id FROM supervisor_incidents WHERE owner_session=? AND target_session=? AND message_id IS NOT NULL', (owner, target)):
            self.db.execute('''DELETE FROM messages WHERE id=? AND sender_session=? AND recipient_session=?
                AND NOT EXISTS (SELECT 1 FROM message_reads r WHERE r.message_id=messages.id
                    AND r.reader_session=messages.recipient_session
                    AND r.provenance IN ('recipient_full_message','recipient_context_full'))
                AND NOT EXISTS (SELECT 1 FROM bridge_deliveries WHERE message_id=messages.id)
                AND NOT EXISTS (SELECT 1 FROM message_batches WHERE message_id=messages.id)''',
                            (row['message_id'], SYSTEM_SENDER, owner))
        self.db.execute('UPDATE supervisor_incidents SET active=0 WHERE owner_session=? AND target_session=?', (owner, target))

    def _reconcile_route(self, policy):
        route, now = self._route(policy['target_session']), time.time()
        self._suppress_obsolete(policy, route)
        if not policy['enabled']:
            self._resolve_conditions(policy, route)
            return route
        control = self.db.execute('SELECT mode FROM agent_controls WHERE target_session=?', (policy['target_session'],)).fetchone()
        if control and control['mode'] != 'running':
            self._resolve_conditions(policy, route)
            return route
        if not route['binding']:
            self._coordinator_evidence(policy, route, 'no_binding', {'route': 'unbound'})
        elif route['binding'].get('thread_id') is not None:
            condition = self._host_condition(route, now)
            if condition:
                self._coordinator_evidence(policy, route, condition,
                                           {'host_id': route['host_id'], 'checked_at': now})
                # Host availability cannot resolve a native observation.
                self._resolve_conditions(policy, route, condition, conditions=HOST_CONDITIONS)
            else:
                # Host recovery cannot resolve a native delivery condition.
                # Only a coherent subsequent native sample can do that.
                self._resolve_conditions(policy, route, conditions=HOST_CONDITIONS)
        return route

    def _managed(self, owner):
        if self._root(owner):
            if not self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='project_orchestrators'").fetchone():
                return []
            return [row['session_id'] for row in self.db.execute(
                'SELECT session_id FROM project_orchestrators WHERE root_session=? ORDER BY session_id', (owner,))]
        if not self._designated(owner):
            raise CoordError('caller is not authorized to configure supervision')
        candidates = [row['id'] for row in self.db.execute('SELECT id FROM sessions ORDER BY id')]
        return [target for target in candidates if self._allowed(owner, target)]

    def configure(self, enabled=_UNSET, recovery_enabled=_UNSET):
        if enabled is _UNSET and recovery_enabled is _UNSET:
            raise CoordError('supervisor configure requires enabled or recovery_enabled')
        for name, value in (('enabled', enabled), ('recovery_enabled', recovery_enabled)):
            if value is not _UNSET and not isinstance(value, bool):
                raise CoordError('supervisor ' + name + ' must be boolean')
        owner = self.coord.require_session()
        with self.coord.tx():
            if not self._owner_allowed(owner):
                raise CoordError('caller is not authorized to configure supervision')
            self.coord._fresh(self.db, owner)
            self.db.execute('''INSERT INTO supervisor_owner_policies
                (owner_session,enabled,updated_at,recovery_enabled) VALUES(?,?,?,?)
                ON CONFLICT(owner_session) DO UPDATE SET
                enabled=CASE WHEN ? THEN excluded.enabled ELSE supervisor_owner_policies.enabled END,
                recovery_enabled=CASE WHEN ? THEN excluded.recovery_enabled ELSE supervisor_owner_policies.recovery_enabled END,
                updated_at=excluded.updated_at''',
                            (owner, int(enabled) if enabled is not _UNSET else 0, time.time(),
                             int(recovery_enabled) if recovery_enabled is not _UNSET else 0,
                             enabled is not _UNSET, recovery_enabled is not _UNSET))
            effective_enabled = self.db.execute('SELECT enabled FROM supervisor_owner_policies WHERE owner_session=?',
                                                (owner,)).fetchone()['enabled']
            for target in self._managed(owner):
                self._reconcile_route({'owner_session': owner, 'target_session': target, 'enabled': effective_enabled})
        return self.status()

    def _configure_recovery(self, enabled):
        """Compatibility delegate; configuration has one authorization path."""
        return self.configure(recovery_enabled=enabled)

    def _rows(self, owner, target=None):
        enabled_row = self.db.execute('SELECT enabled,updated_at FROM supervisor_owner_policies WHERE owner_session=?', (owner,)).fetchone()
        enabled = int(enabled_row['enabled']) if enabled_row else 0
        updated_at = enabled_row['updated_at'] if enabled_row else 0
        if target is not None:
            target = self.target(target)['id']
            if not self._allowed(owner, target):
                raise CoordError('caller is not authorized to read this supervised target')
            return [{'owner_session': owner, 'target_session': target, 'enabled': enabled, 'updated_at': updated_at}]
        return [{'owner_session': owner, 'target_session': item, 'enabled': enabled, 'updated_at': updated_at}
                for item in self._managed(owner)]

    @staticmethod
    def _observation(row):
        if not row:
            return None
        result = dict(row)
        result['summary'] = json.loads(result.pop('summary_json'))
        return result

    def _recovery_status(self, owner, target, route):
        """Owner-scoped journal evidence; readiness/dispatch never prove consumption."""
        row = self.db.execute('''SELECT * FROM supervisor_recoveries
            WHERE owner_session=? AND target_session=? ORDER BY created_at DESC,id DESC LIMIT 1''',
                              (owner, target)).fetchone()
        if not row:
            return None
        current = (row['route_generation'] == route['generation']
                   and row['host_id'] == route['host_id'] and route['route']
                   and row['thread_id'] == route['route']['thread_id'])
        # This flag is written only after the authenticated exact-client-ID
        # native-history report. Historical proof remains bound to its old thread.
        consumed = row['native_consumed'] == 1
        observation = self.db.execute('''SELECT summary_json FROM supervisor_observations
            WHERE owner_session=? AND target_session=?''', (owner, target)).fetchone()
        try:
            history_unavailable = bool(observation and
                                       json.loads(observation['summary_json']).get('history_state') == 'unavailable')
        except (TypeError, ValueError):
            history_unavailable = False
        outcome = ('consumed' if consumed else
                   'history_unavailable' if history_unavailable else
                   'pending' if current and row['status'] in ('claimed', 'attempting', 'awaiting_consumption')
                   else 'unresolved')
        result = {key: row[key] for key in (
            'id', 'owner_session', 'target_session', 'thread_id', 'host_id',
            'route_generation', 'episode', 'wake_id', 'created_at', 'updated_at')}
        result.update(attempt_state=row['status'], binding_state='current' if current else 'stale_route',
                      message_ids=json.loads(row['message_ids_json']), native_consumption=outcome,
                      provenance='coordinator-journal')
        reporting = self._recovery_reporting(row)
        if reporting:
            result['source_reporting'] = reporting
            result['source_reporting_evidence'] = self._recovery_reporting_evidence(row)
            report_incident = self.db.execute('''SELECT active,message_id FROM supervisor_incidents
                WHERE owner_session=? AND target_session=? AND route_generation=?
                  AND condition='source_report_pending' ''',
                                              (owner, target, row['route_generation'])).fetchone()
            result['source_reporting_obligation'] = {
                'state': ('active' if report_incident and report_incident['active'] else
                          'resolved' if report_incident else 'unrecorded'),
                'notice_sent': bool(report_incident and report_incident['message_id']),
            }
        incident = self.db.execute('''SELECT condition,episode,first_seen,last_seen,last_sample,message_id,active
            FROM supervisor_incidents WHERE owner_session=? AND target_session=? AND route_generation=?
            AND condition='unloaded_pending' AND episode=?''',
                                   (owner, target, row['route_generation'], row['episode'])).fetchone()
        result['incident'] = dict(incident) if incident else {
            'condition': 'unloaded_pending', 'episode': row['episode']}
        result['incident']['state'] = ('unavailable' if not incident else
                                       'suppressed' if not current else 'resolved' if not incident['active'] else
                                       'alerted' if incident['message_id'] else 'suspect')
        result['native_consumption_proof'] = ({
            'source': 'native-history', 'recorded_at': row['updated_at'],
            'thread_id': row['thread_id'], 'host_id': row['host_id'], 'client_id': row['wake_id']
        } if consumed else None)
        return result

    def _report_first_ack_role(self, message_id):
        """Validate immutable ACK role; never classify content or task completion."""
        unknown = {'state': 'unknown'}
        if not self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='reply_ack_idempotency'").fetchone():
            return unknown
        rows = self.db.execute('''SELECT a.sender_session AS ack_sender,a.reply_to AS ack_parent,
                a.fingerprint,a.message_id,m.sender_session,m.recipient_session,m.seq,m.created_at,
                r.reply_to,parent.sender_session AS parent_sender,
                parent.recipient_session AS parent_recipient,parent.seq AS parent_seq,
                parent.acked_at AS parent_acked_at
            FROM reply_ack_idempotency a
            LEFT JOIN messages m ON m.id=a.message_id
            LEFT JOIN message_replies r ON r.message_id=m.id
            LEFT JOIN messages parent ON parent.id=a.reply_to
            WHERE a.message_id=? LIMIT 2''', (message_id,)).fetchall()
        if not rows:
            return {'state': 'not_recorded'}
        if len(rows) != 1:
            return unknown
        row = rows[0]
        if (row['message_id'] != message_id or row['ack_sender'] != row['sender_session']
                or row['ack_parent'] != row['reply_to']
                or row['parent_sender'] != row['recipient_session']
                or row['parent_recipient'] != row['sender_session']
                or row['sender_session'] == row['recipient_session']
                or not isinstance(row['seq'], int) or not isinstance(row['parent_seq'], int)
                or row['parent_seq'] >= row['seq']
                or not isinstance(row['created_at'], (int, float)) or not math.isfinite(row['created_at'])
                or not isinstance(row['parent_acked_at'], (int, float)) or not math.isfinite(row['parent_acked_at'])
                or not isinstance(row['fingerprint'], str) or len(row['fingerprint']) != 64
                or any(value not in '0123456789abcdef' for value in row['fingerprint'])
                or self.db.execute('SELECT 1 FROM message_batches WHERE message_id IN (?,?)',
                                   (message_id,row['ack_parent'])).fetchone()):
            return unknown
        return {'state': 'verified', 'source_message_id': row['ack_parent']}

    def _recovery_reporting(self, recovery):
        """Message reporting settlement is never source-task or process proof."""
        if recovery['native_consumed'] != 1 or not recovery['wake_id']:
            return None
        deliveries = [dict(item) for item in self.db.execute('''
            SELECT d.message_id,m.acked_at,
                   EXISTS(SELECT 1 FROM supervisor_notice_provenance p WHERE p.message_id=m.id
                          AND p.sender_session=m.sender_session AND p.recipient_session=m.recipient_session) AS informational,
                   EXISTS(SELECT 1 FROM message_replies r
                          JOIN messages reply ON reply.id=r.message_id
                          WHERE r.reply_to=d.message_id
                            AND reply.sender_session=m.recipient_session
                            AND reply.recipient_session=m.sender_session) AS replied
            FROM bridge_deliveries d JOIN messages m ON m.id=d.message_id
            WHERE d.job_id=? ORDER BY m.seq''', (recovery['wake_id'],))]
        for item in deliveries:
            item['informational_ack'] = self._report_first_ack_role(item['message_id'])['state'] == 'verified'
        reported = bool(deliveries) and all(
            (item['acked_at'] is not None if item['informational'] or item['informational_ack']
             else bool(item['replied'])) for item in deliveries)
        real_pending = any(not (item['informational'] or item['informational_ack']) and
                           not item['replied'] for item in deliveries)
        result = {'state': ('reported' if reported else 'awaiting_acknowledged_reply'
                            if real_pending or not deliveries else 'awaiting_notice_acknowledgement'),
                  'delivered_count': len(deliveries)}
        informational = sum(bool(item['informational']) for item in deliveries)
        informational_acks = sum(bool(item['informational_ack']) for item in deliveries)
        if informational:
            result['informational_notice_count'] = informational
        if informational_acks:
            result['informational_ack_count'] = informational_acks
        if informational or informational_acks:
            result['scope'] = 'message_reporting_only'
        return result

    @staticmethod
    def _report_json_size(value):
        return len(json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode('utf-8'))

    def _recovery_reporting_evidence(self, recovery):
        """Bounded metadata-only source evidence; never message content or task proof."""
        result = {'state': 'unknown', 'complete': False, 'truncated': False,
                  'deliveries': [], 'factual_result': 'UNRECORDED'}
        owner, target, wake_id = (recovery['owner_session'], recovery['target_session'],
                                  recovery['wake_id'])
        if (recovery['native_consumed'] != 1 or not isinstance(wake_id, str) or
                not wake_id or len(wake_id.encode('utf-8')) > _REPORT_MAX_ID_BYTES):
            return result
        try:
            expected_ids = json.loads(recovery['message_ids_json'])
        except (TypeError, ValueError):
            return result
        if (not isinstance(expected_ids, list) or not expected_ids or
                any(not isinstance(value, str) or not value or
                    len(value.encode('utf-8')) > _REPORT_MAX_ID_BYTES for value in expected_ids) or
                len(set(expected_ids)) != len(expected_ids)):
            return result
        expected_set = set(expected_ids)
        project_row = self.db.execute("SELECT value FROM meta WHERE key='project_id'").fetchone()
        project_id = project_row['value'] if project_row else 'default'
        deliveries = self.db.execute('''SELECT d.job_id,d.message_id,m.sender_session,m.recipient_session,
                m.acked_at,m.created_at,m.seq
            FROM bridge_deliveries d LEFT JOIN messages m ON m.id=d.message_id
            WHERE d.job_id=? ORDER BY m.seq,d.message_id LIMIT ?''',
                                      (wake_id, _REPORT_MAX_DELIVERIES + 1)).fetchall()
        truncated = len(deliveries) > _REPORT_MAX_DELIVERIES
        integrity_unknown = False
        seen_ids = set()
        for delivery in deliveries[:_REPORT_MAX_DELIVERIES]:
            message_id = delivery['message_id']
            base = {'delivery_id': message_id if isinstance(message_id, str) and
                    0 < len(message_id.encode('utf-8')) <= _REPORT_MAX_ID_BYTES else None,
                    'original_message_id': message_id if isinstance(message_id, str) and
                    0 < len(message_id.encode('utf-8')) <= _REPORT_MAX_ID_BYTES else None,
                    'wake_id': wake_id, 'factual_result': 'UNRECORDED'}
            if (delivery['job_id'] != wake_id or base['original_message_id'] is None or
                    message_id not in expected_set or delivery['recipient_session'] != target or
                    message_id in seen_ids or not isinstance(delivery['sender_session'], str) or
                    not isinstance(delivery['recipient_session'], str) or
                    max(len(delivery['sender_session'].encode('utf-8')),
                        len(delivery['recipient_session'].encode('utf-8'))) > _REPORT_MAX_ID_BYTES):
                integrity_unknown = True
                base.update(state='unknown', transport_ack={'state': 'unknown', 'acked_at': None},
                            directed_replies=[], ack_reply={'state': 'unknown'},
                            substantive_ack='NOT_DEMONSTRATED', decision={'state': 'unknown'})
                result['deliveries'].append(base)
                continue
            seen_ids.add(message_id)
            sender, recipient = delivery['sender_session'], delivery['recipient_session']
            acked_at = delivery['acked_at']
            if acked_at is not None and (not isinstance(acked_at, (int, float)) or
                                         not math.isfinite(acked_at)):
                integrity_unknown = True
                ack_state, acked_at = 'unknown', None
            else:
                ack_state = 'acknowledged' if acked_at is not None else 'not_acknowledged'
            item = dict(base, sender_session=sender, recipient_session=recipient,
                        original_created_at=(delivery['created_at'] if isinstance(delivery['created_at'], (int, float))
                                             and math.isfinite(delivery['created_at']) else None),
                        transport_ack={'state': ack_state, 'acked_at': acked_at},
                        directed_replies=[], ack_reply={'state': 'unrecorded'},
                        substantive_ack='NOT_DEMONSTRATED',
                        decision={'state': 'unrecorded'}, state='recorded')
            if item['original_created_at'] is None:
                integrity_unknown = True
                item['state'] = 'unknown'
            first_ack = self._report_first_ack_role(message_id)
            item['first_ack_role'] = first_ack
            item['source_reply_obligation'] = ('not_required_informational_ack'
                                                if first_ack['state'] == 'verified' else 'required')
            if first_ack['state'] == 'unknown':
                integrity_unknown = True
                item['state'] = 'unknown'
            provenance = self.db.execute('''SELECT sender_session,recipient_session
                FROM supervisor_notice_provenance WHERE message_id=?''', (message_id,)).fetchone()
            item['informational_notice'] = ('verified' if provenance and
                provenance['sender_session'] == sender and provenance['recipient_session'] == recipient
                else 'not_recorded' if provenance is None else 'unknown')
            if item['informational_notice'] == 'verified':
                item['source_reply_obligation'] = 'not_required_informational_notice'
            if provenance and item['informational_notice'] == 'unknown':
                integrity_unknown = True
                item['state'] = 'unknown'
            reply_rows = self.db.execute('''SELECT r.message_id,r.reply_to,reply.sender_session,
                    reply.recipient_session,reply.created_at
                FROM message_replies r JOIN messages reply ON reply.id=r.message_id
                WHERE r.reply_to=? ORDER BY reply.seq,r.message_id LIMIT ?''',
                                         (message_id, _REPORT_MAX_REPLIES + 1)).fetchall()
            if len(reply_rows) > _REPORT_MAX_REPLIES:
                truncated = True
            valid_replies = {}
            for reply in reply_rows[:_REPORT_MAX_REPLIES]:
                rid = reply['message_id']
                created = reply['created_at']
                if (not isinstance(rid, str) or not 0 < len(rid.encode('utf-8')) <= _REPORT_MAX_ID_BYTES or
                        reply['reply_to'] != message_id or reply['sender_session'] != recipient or
                        reply['recipient_session'] != sender or not isinstance(created, (int, float)) or
                        not math.isfinite(created)):
                    integrity_unknown = True
                    item['state'] = 'unknown'
                    continue
                valid_replies[rid] = {'reply_message_id': rid, 'reply_to': message_id,
                                      'sender_session': recipient, 'recipient_session': sender,
                                      'created_at': created}
            item['directed_replies'] = list(valid_replies.values())
            if self.db.execute('SELECT 1 FROM sqlite_master WHERE type=? AND name=?',
                               ('table', 'reply_ack_idempotency')).fetchone():
                ack_rows = self.db.execute('''SELECT sender_session,reply_to,message_id FROM reply_ack_idempotency
                    WHERE reply_to=? ORDER BY sender_session LIMIT 2''', (message_id,)).fetchall()
                if len(ack_rows) > 1:
                    integrity_unknown = True
                    item['ack_reply'] = {'state': 'unknown'}
                elif ack_rows:
                    ackrow = ack_rows[0]
                    ackreply = valid_replies.get(ackrow['message_id'])
                    if (ackrow['reply_to'] != message_id or ackrow['sender_session'] != recipient or
                            not ackreply or self.db.execute(
                                'SELECT 1 FROM message_batches WHERE message_id=?',
                                (ackrow['message_id'],)).fetchone()):
                        integrity_unknown = True
                        item['ack_reply'] = {'state': 'unknown'}
                    else:
                        item['ack_reply'] = {'state': 'recorded',
                                             'reply_message_id': ackreply['reply_message_id'],
                                             'created_at': ackreply['created_at']}
            else:
                integrity_unknown = True
                item['ack_reply'] = {'state': 'unknown'}
            request = self.db.execute('''SELECT message_id,author_session,recipient_session,project_id,created_at
                FROM operator_decision_requests WHERE message_id=?''', (message_id,)).fetchone()
            if request:
                if (request['author_session'] != sender or request['recipient_session'] != recipient or
                        request['project_id'] != project_id or not isinstance(request['created_at'], (int, float)) or
                        not math.isfinite(request['created_at']) or self.db.execute(
                            'SELECT 1 FROM message_batches WHERE message_id=?', (message_id,)).fetchone() or
                        not self.db.execute('SELECT 1 FROM operator_identities WHERE session_id=?',
                                            (recipient,)).fetchone()):
                    integrity_unknown = True
                    item['decision'] = {'state': 'unknown'}
                else:
                    answer = self.db.execute('''SELECT a.message_id,a.reply_message_id,a.recorded_by,a.outcome,
                            a.recorded_at,m.sender_session,m.recipient_session,r.reply_to
                        FROM operator_decision_answers a
                        LEFT JOIN messages m ON m.id=a.reply_message_id
                        LEFT JOIN message_replies r ON r.message_id=a.reply_message_id
                        WHERE a.message_id=?''', (message_id,)).fetchone()
                    if answer:
                        if (answer['message_id'] != message_id or answer['reply_to'] != message_id or
                                answer['recorded_by'] != sender or answer['sender_session'] != recipient or
                                answer['recipient_session'] != sender or answer['outcome'] not in
                                ('accepted', 'declined', 'conditional', 'clarification') or
                                not isinstance(answer['recorded_at'], (int, float)) or
                                not math.isfinite(answer['recorded_at']) or
                                answer['reply_message_id'] not in valid_replies or
                                self.db.execute('SELECT 1 FROM message_batches WHERE message_id=?',
                                                (answer['reply_message_id'],)).fetchone()):
                            integrity_unknown = True
                            item['decision'] = {'state': 'unknown'}
                        else:
                            item['decision'] = {'state': 'recorded',
                                'request_message_id': message_id,
                                'request_created_at': request['created_at'],
                                'answer_reply_message_id': answer['reply_message_id'],
                                'recorded_by': answer['recorded_by'], 'outcome': answer['outcome'],
                                'recorded_at': answer['recorded_at']}
                    else:
                        # The recovery target is an agent, while decision-needed
                        # requests are addressed to an operator. A reply alone
                        # cannot supply an operator answer record.
                        item['decision'] = {'state': 'unrecorded',
                                            'request_message_id': message_id,
                                            'request_created_at': request['created_at']}
            else:
                # An answer without its immutable request is inconsistent evidence.
                orphan = self.db.execute('SELECT 1 FROM operator_decision_answers WHERE message_id=?',
                                         (message_id,)).fetchone()
                if orphan:
                    integrity_unknown = True
                    item['decision'] = {'state': 'unknown'}
            if self._report_json_size(item) > _REPORT_MAX_ITEM_BYTES:
                truncated = True
                item = dict(base, sender_session=sender, recipient_session=recipient,
                            state='unknown', transport_ack={'state': ack_state, 'acked_at': acked_at},
                            directed_replies=[], ack_reply={'state': 'unknown'},
                            substantive_ack='NOT_DEMONSTRATED', decision={'state': 'unknown'})
            candidate = result['deliveries'] + [item]
            if self._report_json_size(dict(result, deliveries=candidate)) > _REPORT_MAX_BYTES:
                truncated = True
                break
            result['deliveries'].append(item)
        if seen_ids != expected_set:
            integrity_unknown = True
        result['truncated'] = truncated
        result['complete'] = not truncated and not integrity_unknown and seen_ids == expected_set and len(deliveries) == len(result['deliveries'])
        result['state'] = ('truncated' if truncated else 'unknown' if integrity_unknown else
                           'complete' if result['complete'] else 'unknown')
        return result


    def status(self, target=None):
        owner, result = self.coord.require_session(), []
        with self.coord.tx():
            for policy in self._rows(owner, target)[:50]:
                route = self._reconcile_route(policy)
                observation = self.db.execute('SELECT * FROM supervisor_observations WHERE owner_session=? AND target_session=?',
                                              (owner, policy['target_session'])).fetchone()
                incidents = [dict(row) for row in self.db.execute('''SELECT condition,route_generation,first_seen,last_seen,last_sample,message_id,active,episode
                    FROM supervisor_incidents WHERE owner_session=? AND target_session=? ORDER BY last_seen DESC LIMIT 10''',
                    (owner, policy['target_session']))]
                for incident in incidents:
                    incident['state'] = ('suppressed' if incident['route_generation'] != route['generation'] else
                                         'resolved' if not incident['active'] else
                                         'alerted' if incident['message_id'] else 'suspect')
                route_kind = ('no_binding' if not route['binding'] else
                              'route_only_inconclusive' if route['binding'].get('thread_id') is None else 'bound')
                observed = self._observation(observation)
                if observed and observed['route_generation'] != route['generation']:
                    observed['source'], observed['state'] = 'server-cache', 'stale_route'
                if route_kind == 'no_binding':
                    observed = {'source': 'coordinator', 'state': 'no_binding', 'summary': {'route': 'unbound'},
                                'checked_at': time.time(), 'next_due': None, 'error_count': 0}
                elif route_kind == 'route_only_inconclusive':
                    observed = {'source': 'coordinator', 'state': 'route_only_inconclusive',
                                'summary': {'route': 'child binding is not sampled as parent'},
                                'checked_at': time.time(), 'next_due': None, 'error_count': 0}
                elif policy['enabled'] and self._host_condition(route, time.time()):
                    observed = {'source': 'coordinator', 'state': self._host_condition(route, time.time()),
                                'summary': {'host_id': route['host_id']}, 'checked_at': time.time(),
                                'next_due': None, 'error_count': 0}
                if observed:
                    observed['age_seconds'] = max(0, time.time() - observed['checked_at'])
                result.append(dict(policy, route_generation=route['generation'],
                                   thread_id=(route['route'] or {}).get('thread_id') if route['route'] else None,
                                   host_id=route['host_id'], owner_session=owner, route_kind=route_kind,
                                   observation=observed, incidents=incidents,
                                   recovery=self._recovery_status(owner, policy['target_session'], route)))
        owner_policy = self.db.execute('SELECT enabled,recovery_enabled FROM supervisor_owner_policies WHERE owner_session=?', (owner,)).fetchone()
        return {'owner_session': owner, 'enabled': bool(owner_policy and owner_policy['enabled']),
                'recovery_enabled': bool(owner_policy and owner_policy['recovery_enabled']),
                'targets': result, 'has_more': len(self._rows(owner, target)) > 50}


    def _recovery_allowed(self, job, *, cold_message=False):
        """Recheck coordinator gates at every recovery mutation boundary."""
        consent = self.db.execute('''SELECT 1 FROM supervisor_owner_policies
            WHERE owner_session=? AND enabled=1 AND recovery_enabled=1''', (job['owner_session'],)).fetchone()
        if not consent and not cold_message:
            raise CoordError('supervisor recovery consent withheld')
        from .root_orchestrator import projection
        from .session_reset import assert_reset_unlocked
        from .core import assert_agent_running
        assert_agent_running(self.db, job['target_session'])
        assert_reset_unlocked(self.db, job['target_session'])
        if any((projection(self.db, session) or {}).get('paused')
               for session in (job['owner_session'], job['target_session'])):
            raise CoordError('paused supervisor recovery withheld')
        route = self._route(job['target_session'])
        if (not route['binding'] or route['binding']['thread_id'] != job['thread_id']
                or len(route['route']['route']) != 1 or self._host_condition(route, time.time())):
            raise CoordError('unavailable supervisor recovery withheld')
        if self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='remote_guarded_runs'").fetchone():
            rows = self.db.execute('''SELECT run_id,resource,reservation_id,
                session_id AS owner_session,host_id,local_pid AS pid,NULL AS pgid,
                started_at,closed_at FROM remote_guarded_runs
                WHERE session_id=? AND closed_at IS NULL''',
                (job['target_session'],)).fetchmany(2)
            if rows:
                from .d6b4_retirement import HOST, OWNER, is_retired_guard
                # This exact administrative audit permits ordinary delivery to
                # its original owner. It does not close the guard, restore work,
                # clear holdings, or assert original process termination.
                retired_delivery = (len(rows) == 1 and job['target_session'] == OWNER
                    and job.get('host_id') == HOST and route.get('host_id') == HOST
                    and route.get('generation') == job.get('route_generation')
                    and is_retired_guard(self.db, 'remote_guarded_runs', dict(rows[0])))
                if not retired_delivery:
                    raise CoordError('open guarded work recovery withheld')

    def _recovery_view(self, row):
        if not row:
            return None
        result = {key: row[key] for key in ('id', 'status', 'wake_id', 'episode', 'native_consumed')}
        result['messages'] = [{'id': value, 'recipient_session': row['target_session']}
                              for value in json.loads(row['message_ids_json'])]
        return result

    def _recovery_for(self, job):
        return self.db.execute('''SELECT * FROM supervisor_recoveries
            WHERE owner_session=? AND target_session=? AND route_generation=?
            ORDER BY created_at DESC LIMIT 1''',
                               (job['owner_session'], job['target_session'], job['route_generation'])).fetchone()

    def _recovery_delivery_eligible(self, message_id, target):
        return bool(self.db.execute('''SELECT 1 FROM messages m
            WHERE m.id=? AND m.recipient_session=? AND m.sender_session<>m.recipient_session
              AND NOT EXISTS (SELECT 1 FROM message_reads r WHERE r.message_id=m.id
                  AND r.reader_session=m.recipient_session
                  AND r.provenance IN ('recipient_full_message','recipient_context_full')) AND NOT (m.acked_at IS NOT NULL AND m.acked_at <= COALESCE(CAST((SELECT value FROM meta WHERE key='message_delivery_archive_before') AS REAL),-1))
              AND (NOT EXISTS (SELECT 1 FROM message_batches b WHERE b.message_id=m.id)
                   OR EXISTS (SELECT 1 FROM message_attention a WHERE a.message_id=m.id))''',
                                   (message_id, target)).fetchone())

    def _recovery_candidate(self, job, summary):
        # An unknown status, native queue or inconsistent sample is not a load failure.
        if (summary.get('before') not in ('unloaded', 'notLoaded')
                or summary.get('after') != summary.get('before')
                or summary.get('contradictory') is not False
                or summary.get('history_state') != 'available'
                or summary.get('queue_count') != 0 or summary.get('turn_status') == 'inProgress'):
            return None
        try:
            self._recovery_allowed(job)
        except CoordError:
            return None
        incident = self.db.execute('''SELECT * FROM supervisor_incidents
            WHERE owner_session=? AND target_session=? AND route_generation=?
              AND condition='unloaded_pending' AND active=1''',
                                   (job['owner_session'], job['target_session'], job['route_generation'])).fetchone()
        if not incident or time.time() - incident['first_seen'] < 60:
            return None
        previous = self._recovery_for(job)
        if previous and (previous['status'] != 'completed' or previous['episode'] == incident['episode']):
            return None  # Never replay an episode or an ambiguous attempt.
        wakes = self.db.execute('''SELECT * FROM bridge_jobs WHERE thread_id=?
            AND status NOT IN ('dispatched','cancelled') ORDER BY created_at''', (job['thread_id'],)).fetchall()
        if any(wake['status'] != 'prepared' for wake in wakes):
            return None
        if wakes:
            wake = wakes[0]
            rows = self.db.execute('''SELECT d.message_id AS id
                FROM bridge_deliveries d LEFT JOIN messages m ON m.id=d.message_id
                WHERE d.job_id=? ORDER BY m.seq''', (wake['id'],)).fetchall()
            if not rows or len(rows) > 20 or any(
                    not self._recovery_delivery_eligible(row['id'], job['target_session']) for row in rows):
                return None
            message_ids, wake_id = [row['id'] for row in rows], wake['id']
        else:
            rows = self.db.execute('''SELECT m.id FROM messages m
                WHERE m.recipient_session=? AND m.sender_session<>m.recipient_session
                AND NOT EXISTS (SELECT 1 FROM message_reads r WHERE r.message_id=m.id
                    AND r.reader_session=m.recipient_session
                    AND r.provenance IN ('recipient_full_message','recipient_context_full')) AND NOT (m.acked_at IS NOT NULL AND m.acked_at <= COALESCE(CAST((SELECT value FROM meta WHERE key='message_delivery_archive_before') AS REAL),-1))
                AND NOT EXISTS (SELECT 1 FROM bridge_deliveries d WHERE d.message_id=m.id)
                AND (NOT EXISTS (SELECT 1 FROM message_batches b WHERE b.message_id=m.id)
                     OR EXISTS (SELECT 1 FROM message_attention a WHERE a.message_id=m.id))
                ORDER BY m.seq LIMIT 20''', (job['target_session'],)).fetchall()
            message_ids, wake_id = [row['id'] for row in rows], None
        return (incident, message_ids, wake_id) if message_ids else None

    def _recovery_report(self, job, report):
        """Incident-linked transitions reuse the authenticated observation transport."""
        summary, action = report['summary'], report['state']
        self._recovery_allowed(job)
        if action == 'recovery_claim':
            candidate = self._recovery_candidate(job, summary)
            if not candidate:
                return {'accepted': True, 'claimed': False}
            incident, message_ids, wake_id = candidate
            identity = '|'.join((job['owner_session'], job['target_session'],
                                 job['route_generation'], str(incident['episode'])))
            recovery_id = 'recovery_' + hashlib.sha256(identity.encode()).hexdigest()[:32]
            now = time.time()
            self.db.execute('''INSERT INTO supervisor_recoveries VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                            (recovery_id, job['owner_session'], job['target_session'], job['route_generation'],
                             incident['episode'], job['thread_id'], job['host_id'], 'claimed',
                             json.dumps(message_ids), wake_id, 0, now, now))
            return {'accepted': True, 'claimed': True, 'recovery': self._recovery_view(self._recovery_for(job))}
        row = self._recovery_for(job)
        if not row or row['id'] != summary.get('recovery_id'):
            raise CoordError('stale supervisor recovery withheld')
        wake_id = summary.get('wake_id') or row['wake_id']
        wake = self.db.execute('SELECT * FROM bridge_jobs WHERE id=? AND thread_id=?',
                               (wake_id, job['thread_id'])).fetchone()
        linked = {item['message_id'] for item in self.db.execute(
            'SELECT message_id FROM bridge_deliveries WHERE job_id=?', (wake_id,))}
        expected = set(json.loads(row['message_ids_json']))
        if not wake or linked != expected:
            raise CoordError('supervisor recovery delivery changed')
        if action == 'recovery_attempt':
            if row['status'] != 'claimed':
                return {'accepted': True, 'resume_allowed': False}
            if (wake['status'] != 'prepared' or summary.get('before') not in ('unloaded', 'notLoaded')
                    or summary.get('after') != summary.get('before')
                    or summary.get('contradictory') is not False or summary.get('queue_count') != 0
                    or summary.get('history_state') != 'available'
                    or summary.get('turn_status') == 'inProgress'
                    or self.db.execute('''SELECT 1 FROM bridge_jobs WHERE thread_id=?
                        AND status NOT IN ('prepared','dispatched','cancelled')''',
                                       (job['thread_id'],)).fetchone()
                    or any(not self._recovery_delivery_eligible(value, job['target_session']) for value in expected)):
                raise CoordError('supervisor recovery admission changed')
            self.db.execute("UPDATE supervisor_recoveries SET status='attempting',wake_id=?,updated_at=? WHERE id=?",
                            (wake_id, time.time(), row['id']))
            return {'accepted': True, 'resume_allowed': True}
        if action == 'recovery_ready':
            if (row['status'] not in ('attempting', 'uncertain')
                    or summary.get('before') != 'idle' or summary.get('after') != 'idle'
                    or summary.get('direct_input') is not True or summary.get('contradictory') is not False
                    or summary.get('queue_count') != 0 or summary.get('history_state') != 'available'):
                raise CoordError('supervisor recovery loaded proof missing')
            self.db.execute("UPDATE supervisor_recoveries SET status='awaiting_consumption',updated_at=? WHERE id=?",
                            (time.time(), row['id']))
        elif action == 'recovery_consumed':
            if (row['status'] not in ('attempting', 'uncertain', 'awaiting_consumption')
                    or wake['status'] == 'cancelled' or summary.get('consumed_client_id') != wake_id
                    or summary.get('observed_thread_id') != job['thread_id']
                    or summary.get('proof') != 'native-history'):
                raise CoordError('supervisor recovery consumption proof missing')
            self.db.execute("UPDATE supervisor_recoveries SET status='completed',native_consumed=1,updated_at=? WHERE id=?",
                            (time.time(), row['id']))
            reporting = self._recovery_reporting(self._recovery_for(job))
            if (not reporting or not reporting['delivered_count']
                    or reporting['state'] not in ('awaiting_acknowledged_reply','awaiting_notice_acknowledgement','reported')):
                raise CoordError('supervisor recovery reporting obligation is missing')
            if reporting and reporting['state'] == 'awaiting_acknowledged_reply':
                self._incident(job, 'source_report_pending', dict(summary, source_reporting=reporting))
        elif action == 'recovery_uncertain':
            if row['status'] in ('claimed', 'attempting'):
                self.db.execute("UPDATE supervisor_recoveries SET status='uncertain',wake_id=?,updated_at=? WHERE id=?",
                                (wake_id, time.time(), row['id']))
        else:
            raise CoordError('invalid supervisor recovery report')
        return {'accepted': True, 'recovery': self._recovery_view(self._recovery_for(job))}

    def _cold_owner(self, target):
        from .management import manager
        owner = manager(self.db, target)
        if owner and self._designated(owner) and not self._designated(target):
            return owner
        if self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='project_orchestrators'").fetchone():
            row = self.db.execute('SELECT root_session FROM project_orchestrators WHERE session_id=?', (target,)).fetchone()
            if row and self._root(row['root_session']):
                return row['root_session']
        if (not owner or not self._root(owner) or target == SYSTEM_SENDER
                or not self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='root_direct_creations'").fetchone()):
            return None
        if not self.db.execute('''
                SELECT 1 FROM root_direct_creations d
                JOIN meta p ON p.key='project_id' AND p.value=d.project_id
                JOIN sessions r ON r.id=d.root_session
                JOIN sessions w ON w.id=d.target_session AND w.agent<>'operator'
                WHERE d.root_session=? AND d.target_session=?
                    AND NOT EXISTS (SELECT 1 FROM operator_identities i WHERE i.session_id=w.id)
            ''', (owner, target)).fetchone():
            return None
        from .root_orchestrator import assert_management_target
        try:
            assert_management_target(self.db, owner, target)
        except CoordError:
            return None
        return owner

    def _cold_check(self, job, host, *, pending_wake=None):
        from .root_lifecycle import stopped
        from .root_restart import held
        owner, target = job.get('owner_session'), job.get('target_session')
        if not owner or self._cold_owner(target) != owner or not self._owner_allowed(owner):
            raise CoordError('cold message management changed')
        if self.db.execute("SELECT 1 FROM cold_message_loads WHERE target_session=? AND status IN ('claimed','attempting') AND wake_id IS NOT ?", (target,pending_wake)).fetchone():
            raise CoordError('prior cold message effect is uncertain')
        if any(stopped(self.db, sid) or held(self.db, sid) for sid in (owner, target)):
            raise CoordError('cold message project or agent stopped')
        route = self._route(target)
        if (not route['route'] or route['host_id'] != host
                or route['generation'] != job.get('route_generation')
                or route['route']['thread_id'] != job.get('thread_id')):
            raise CoordError('cold message route changed')
        self._recovery_allowed(job, cold_message=True)
        held_resources = self.db.execute('SELECT 1 FROM resources WHERE owner_session=? LIMIT 1', (target,)).fetchone()
        guarded = False
        for table, column in (('guarded_runs','session'), ('remote_guarded_runs','session_id')):
            if self.db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone():
                guarded = guarded or bool(self.db.execute(f'SELECT 1 FROM {table} WHERE {column}=? AND closed_at IS NULL LIMIT 1', (target,)).fetchone())
        if held_resources or guarded:
            raise CoordError('cold message guarded work withheld')
        return route

    def _cold_jobs(self, host):
        from .bridge_state import BridgeState
        state = BridgeState(self.coord)
        messages = state.pending()
        # An interrupted preparation may be re-entered only with durable
        # pristine proof and its exact single delivery.
        for wake in state.jobs():
            if wake.get('pristine') is not True:
                continue
            rows = self.db.execute('SELECT m.id,m.recipient_session FROM bridge_deliveries d JOIN messages m ON m.id=d.message_id WHERE d.job_id=?', (wake['id'],)).fetchall()
            if len(rows) == 1 and state.still_unread(wake['id']):
                messages.append(dict(rows[0], wake_id=wake['id']))
        result, seen = [], set()
        for message in messages:
            target = message['recipient_session']
            if target in seen or self.db.execute('SELECT 1 FROM cold_message_loads WHERE source_id=?', (message['id'],)).fetchone():
                continue
            owner = self._cold_owner(target)
            if not owner:
                continue
            route = self._route(target)
            if not route.get('route'):
                continue
            job = dict(cold_message=True, source_id=message['id'], wake_id=message.get('wake_id'),
                       owner_session=owner, target_session=target, host_id=host,
                       thread_id=route['route']['thread_id'], route_generation=route['generation'],
                       admission='running', wake_summary={}, holds=0, open_guards=[])
            try:
                self._cold_check(job, host)
            except CoordError:
                continue
            result.append(job); seen.add(target)
        return result

    def jobs(self, host):
        now, result = time.time(), []
        from .agent_control import AgentControlState
        result.extend(self._cold_jobs(host))
        with self.coord.tx():
            for owner in self.db.execute('SELECT * FROM supervisor_owner_policies WHERE enabled=1'):
                managed = self._managed(owner['owner_session']) if self._owner_allowed(owner['owner_session']) else []
                for old in self.db.execute('SELECT DISTINCT target_session FROM supervisor_incidents WHERE owner_session=?', (owner['owner_session'],)):
                    if old['target_session'] not in managed:
                        self._withdraw_revoked(owner['owner_session'], old['target_session'])
                if not self._owner_allowed(owner['owner_session']):
                    continue
                for target in managed:
                    policy = {'owner_session': owner['owner_session'], 'target_session': target,
                              'enabled': owner['enabled'], 'updated_at': owner['updated_at']}
                    route = self._reconcile_route(policy)
                # A direct child binding retains its own route-only identity and is never sampled as its parent.
                    if (route['host_id'] != host or not route['route'] or not route['binding']
                        or route['binding'].get('thread_id') is None or self._host_condition(route, now)):
                        continue
                    observation = self.db.execute('SELECT * FROM supervisor_observations WHERE owner_session=? AND target_session=?',
                                              (policy['owner_session'], policy['target_session'])).fetchone()
                    if observation and observation['route_generation'] == route['generation'] and observation['next_due'] > now:
                        continue
                    wakes = self._wake_summary(route['route']['thread_id'], target)
                    control = self.db.execute('SELECT mode FROM agent_controls WHERE target_session=?',
                                          (policy['target_session'],)).fetchone()
                    ownership = AgentControlState(self.coord).ownership(policy['target_session'])
                    result.append(dict(policy, thread_id=route['route']['thread_id'], route_generation=route['generation'],
                                   host_id=host, wake_summary=wakes, admission=control['mode'] if control else 'running',
                                   holds=len(ownership['reservations']), open_guards=ownership['open_guards'][:12],
                                   reservation_ids=[item['resource'] for item in ownership['reservations'][:8]],
                                   recovery=self._recovery_view(self._recovery_for(dict(policy, route_generation=route['generation'])))))
        return result

    def _cold_ready(self, job, summary, host):
        with self.coord.tx():
            row = self.db.execute('SELECT * FROM cold_message_loads WHERE source_id=?', (job.get('source_id'),)).fetchone()
            if (not row or row['status'] != 'attempting' or row['host_id'] != host
                    or any(row[key] != job.get(key) for key in ('owner_session','target_session','thread_id','route_generation'))
                    or row['wake_id'] != summary.get('wake_id')
                    or summary.get('before') != 'idle' or summary.get('after') != 'idle'
                    or summary.get('direct_input') is not True or summary.get('contradictory') is not False
                    or summary.get('queue_count') != 0 or summary.get('history_state') != 'available'):
                raise CoordError('cold message loaded proof missing')
            now=time.time()
            self.db.execute("UPDATE cold_message_loads SET status='loaded',updated_at=? WHERE source_id=?", (now,row['source_id']))
            self.db.execute('INSERT INTO cold_message_load_events(source_id,status,created_at) VALUES(?,?,?)', (row['source_id'],'loaded',now))
            return {'accepted':True}

    def _cold_attempt(self, job, summary, host):
        from .project_activity import non_runnable_task_sources
        source, wake_id = job.get('source_id'), summary.get('wake_id')
        if not source or not wake_id or job.get('cold_message') is not True:
            raise CoordError('invalid cold message attempt')
        with self.coord.tx():
            self._cold_check(job, host)
            if (summary.get('before') not in ('notLoaded', 'unloaded')
                    or summary.get('after') != summary.get('before')
                    or summary.get('contradictory') is not False
                    or summary.get('history_state') != 'available'
                    or summary.get('queue_count') != 0 or summary.get('turn_status') == 'inProgress'):
                raise CoordError('cold message native observation changed')
            if source in non_runnable_task_sources(self.db) or not self._recovery_delivery_eligible(source, job['target_session']):
                raise CoordError('cold message source no longer eligible')
            wake = self.db.execute('''SELECT j.* FROM bridge_jobs j JOIN bridge_job_origins o ON o.job_id=j.id
                WHERE j.id=? AND j.thread_id=? AND j.status='prepared' AND j.queue_id IS NULL AND j.error IS NULL
                AND NOT EXISTS (SELECT 1 FROM bridge_job_native_attempts a WHERE a.job_id=j.id)
                AND NOT EXISTS (SELECT 1 FROM agent_control_requests r WHERE r.native_input_job_id=j.id)
                AND NOT EXISTS (SELECT 1 FROM bridge_root_wake_batches b WHERE b.job_id=j.id)''',
                (wake_id, job['thread_id'])).fetchone()
            linked = self.db.execute('SELECT message_id FROM bridge_deliveries WHERE job_id=?', (wake_id,)).fetchall()
            if not wake or len(linked) != 1 or linked[0]['message_id'] != source:
                raise CoordError('cold message pristine wake changed')
            if self.db.execute("SELECT 1 FROM bridge_jobs WHERE thread_id=? AND id<>? AND status NOT IN ('dispatched','cancelled')", (job['thread_id'], wake_id)).fetchone():
                raise CoordError('cold message conflicts with another wake')
            prior=self.db.execute('SELECT * FROM cold_message_loads WHERE source_id=? OR wake_id=?', (source,wake_id)).fetchone()
            now = time.time()
            values=(source,wake_id,job['owner_session'],job['target_session'],job['thread_id'],host,job['route_generation'])
            if prior:
                return {'accepted':True, 'resume_allowed':False}
            self.db.execute('INSERT INTO cold_message_loads VALUES(?,?,?,?,?,?,?,?,?,?)', (*values,'claimed',now,now))
            self.db.execute('INSERT INTO cold_message_load_events(source_id,status,created_at) VALUES(?,?,?)',(source,'claimed',now))
            return {'accepted':True, 'resume_allowed':True}

    def observe(self, job, report, host):
        required = {'state', 'summary', 'source'}
        if not isinstance(job, dict) or not isinstance(report, dict) or set(report) != required or report['source'] != 'native':
            raise CoordError('invalid supervisor observation')
        if not isinstance(report['state'], str) or not isinstance(report['summary'], dict):
            raise CoordError('invalid supervisor observation')
        if report['state'] == 'cold_message_attempt':
            return self._cold_attempt(job, report['summary'], host)
        if report['state'] == 'cold_message_ready':
            return self._cold_ready(job, report['summary'], host)
        now = time.time()
        with self.coord.tx():
            owner = self.db.execute('SELECT * FROM supervisor_owner_policies WHERE owner_session=? AND enabled=1',
                                    (job.get('owner_session'),)).fetchone()
            policy = {'owner_session': job.get('owner_session'), 'target_session': job.get('target_session'), 'enabled': 1} if owner and self._allowed(job.get('owner_session'), job.get('target_session')) else None
            route = self._route(job.get('target_session')) if policy else None
            if (not policy or route['host_id'] != host or route['generation'] != job.get('route_generation')
                    or not route['binding'] or route['binding'].get('thread_id') is None or not route['route']
                    or route['route']['thread_id'] != job.get('thread_id')):
                raise CoordError('stale supervisor observation withheld')
            if report['state'].startswith('recovery_'):
                return self._recovery_report(job, report)
            previous = self.db.execute('SELECT error_count FROM supervisor_observations WHERE owner_session=? AND target_session=?',
                                       (job['owner_session'], job['target_session'])).fetchone()
            errors = (previous['error_count'] if previous else 0) + 1 if report['state'] == 'error' else 0
            interval = (20 if report['state'] in ('active', 'queued') else
                        (30, 60, 120, 300)[min(errors - 1, 3)] if report['state'] == 'error' else 60)
            self.db.execute('''INSERT INTO supervisor_observations(owner_session,target_session,route_generation,source,state,summary_json,checked_at,next_due,error_count)
                VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(owner_session,target_session) DO UPDATE SET
                route_generation=excluded.route_generation,source=excluded.source,state=excluded.state,summary_json=excluded.summary_json,
                checked_at=excluded.checked_at,next_due=excluded.next_due,error_count=excluded.error_count''',
                            (job['owner_session'], job['target_session'], route['generation'], 'native', report['state'],
                             json.dumps(report['summary'], sort_keys=True), now, now + interval, errors))
            control = self.db.execute('SELECT mode FROM agent_controls WHERE target_session=?', (job['target_session'],)).fetchone()
            gated = control and control['mode'] != 'running'
            condition = None if gated else {'unloaded_pending': 'unloaded_pending', 'uncertain': 'uncertain_wake',
                         'uncertain_wake': 'uncertain_wake', 'error': 'native_error'}.get(report['state'])
            recovery = self._recovery_for(dict(job, route_generation=route['generation']))
            reporting = self._recovery_reporting(recovery) if recovery else None
            if not condition and not gated and reporting and reporting['state'] == 'awaiting_acknowledged_reply':
                condition = 'source_report_pending'
                report = dict(report, summary=dict(report['summary'], source_reporting=reporting))
            if condition:
                self._incident(job, condition, report['summary'])
                self._resolve_conditions(policy, route, condition)
            else:
                self._resolve_conditions(policy, route)
            recovery_candidate = (report['state'] == 'unloaded_pending'
                                  and self._recovery_candidate(job, report['summary']) is not None)
        return {'accepted': True, 'recovery_candidate': recovery_candidate}
