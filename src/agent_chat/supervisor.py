"""Durable owner supervision and incident-linked exact-thread recovery."""
import hashlib
import json
import pathlib
import time

from .core import CoordError


SYSTEM_SENDER = 'system:supervisor'
_UNSET = object()
ACTIONABLE = ('unavailable_host', 'stale_host', 'error_host', 'no_binding',
              'unloaded_pending', 'uncertain_wake', 'native_error',
              'source_report_pending')
HOST_CONDITIONS = ('unavailable_host', 'stale_host', 'error_host', 'no_binding')


class SupervisorState:
    def __init__(self, coord):
        self.coord, self.db = coord, coord.db
        self.db.executescript('''
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

    def _incident(self, job, condition, summary):
        if condition not in ACTIONABLE:
            return
        now = time.time()
        key = (job['owner_session'], job['target_session'], job['route_generation'], condition)
        row = self.db.execute('''SELECT * FROM supervisor_incidents WHERE owner_session=? AND target_session=?
            AND route_generation=? AND condition=?''', key).fetchone()
        if not row:
            self.db.execute('''INSERT INTO supervisor_incidents(owner_session,target_session,route_generation,condition,first_seen,last_seen,last_sample,message_id,active,episode)
                VALUES(?,?,?,?,?,?,?,NULL,1,1)''', (*key, now, now, now))
            return
        if not row['active']:
            self.db.execute('''UPDATE supervisor_incidents SET active=1,episode=episode+1,first_seen=?,last_seen=?,last_sample=?,message_id=NULL
                WHERE owner_session=? AND target_session=? AND route_generation=? AND condition=?''', (now, now, now, *key))
            return
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
                + ' host=' + str(job.get('host_id')) + ' route_generation=' + job['route_generation'] + '. Evidence: ' + evidence
                + '. Inspect this exact agent status and bridge state. This informational notice needs consumption and ACK only; '
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
                self._resolve_conditions(policy, route, condition)
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
        reported = bool(deliveries) and all(item['acked_at'] is not None and
                                             (item['informational'] or item['replied']) for item in deliveries)
        real_pending = any(not item['informational'] and (item['acked_at'] is None or not item['replied'])
                           for item in deliveries)
        result = {'state': ('reported' if reported else 'awaiting_acknowledged_reply'
                            if real_pending or not deliveries else 'awaiting_notice_acknowledgement'),
                  'delivered_count': len(deliveries)}
        informational = sum(bool(item['informational']) for item in deliveries)
        if informational:
            result.update(informational_notice_count=informational, scope='message_reporting_only')
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


    def _recovery_allowed(self, job):
        """Recheck coordinator gates at every recovery mutation boundary."""
        consent = self.db.execute('''SELECT 1 FROM supervisor_owner_policies
            WHERE owner_session=? AND enabled=1 AND recovery_enabled=1''', (job['owner_session'],)).fetchone()
        if not consent:
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
            if self.db.execute('SELECT 1 FROM remote_guarded_runs WHERE session_id=? AND closed_at IS NULL',
                               (job['target_session'],)).fetchone():
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

    def jobs(self, host):
        now, result = time.time(), []
        from .agent_control import AgentControlState
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

    def observe(self, job, report, host):
        required = {'state', 'summary', 'source'}
        if not isinstance(job, dict) or not isinstance(report, dict) or set(report) != required or report['source'] != 'native':
            raise CoordError('invalid supervisor observation')
        if not isinstance(report['state'], str) or not isinstance(report['summary'], dict):
            raise CoordError('invalid supervisor observation')
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
