"""Live approval projection and owning-connection bridge handoff.

The server holds display/decision state only. The bridge's RpcClient retains the
one-use native capability; neither browser handles nor persisted logs can answer
an RPC after its connection has gone away.
"""
import copy
import math
import threading
import time
import uuid

from .core import CoordError
from .rpc import TransportError

METHODS = ('item/commandExecution/requestApproval', 'item/fileChange/requestApproval', 'item/permissions/requestApproval')
CHOICES = ('accept', 'decline', 'cancel')


def permission_profile(value):
    """Accept only fully displayable, bounded documented legacy permission fields."""
    if not isinstance(value, dict) or set(value) != {'network', 'fileSystem'}:
        raise CoordError('unsupported requested permissions')
    result = {}
    network = value['network']
    if network is not None:
        if not isinstance(network, dict) or set(network) != {'enabled'} or network['enabled'] is not True:
            raise CoordError('unsupported requested network permissions')
        result['network'] = {'enabled': True}
    fs = value['fileSystem']
    if fs is not None:
        if not isinstance(fs, dict) or not {'read', 'write'} <= set(fs) or set(fs) - {'read', 'write', 'entries', 'globScanMaxDepth'}:
            raise CoordError('unsupported requested filesystem permissions')
        clean = {'read': None, 'write': None}
        for key in ('read', 'write'):
            paths = fs[key]
            if paths is not None:
                if (not isinstance(paths, list) or len(paths) > 40 or
                    any(not isinstance(path, str) or not path.startswith('/') or len(path) > 512
                        or any(ord(char) < 32 or ord(char) == 127 for char in path) for path in paths)):
                    raise CoordError('invalid requested filesystem paths')
                clean[key] = list(paths)
        entries = fs.get('entries')
        if entries is not None:
            if not isinstance(entries, list) or len(entries) > 40:
                raise CoordError('invalid requested filesystem entries')
            for entry in entries:
                if not isinstance(entry, dict) or set(entry) != {'path','access'} or entry['access'] not in ('read','write','deny'):
                    raise CoordError('invalid requested filesystem entry')
                path = entry['path']
                if not isinstance(path, dict): raise CoordError('invalid requested filesystem path')
                kind = path.get('type')
                if kind in ('path','glob_pattern'):
                    key = 'path' if kind == 'path' else 'pattern'
                    value = path.get(key)
                    if set(path) != {'type',key} or not isinstance(value, str) or not value or (kind == 'path' and not value.startswith('/')) or len(value) > 512 or any(ord(c) < 32 or ord(c) == 127 for c in value):
                        raise CoordError('invalid requested filesystem path')
                elif kind == 'special':
                    value = path.get('value')
                    if set(path) != {'type','value'} or not isinstance(value, dict) or value.get('kind') not in ('root','minimal','project_roots','tmpdir','slash_tmp'):
                        raise CoordError('unsupported special filesystem path')
                    if value['kind'] == 'project_roots':
                        if set(value) != {'kind','subpath'} or (value['subpath'] is not None and (not isinstance(value['subpath'], str) or len(value['subpath']) > 512 or any(ord(c) < 32 or ord(c) == 127 for c in value['subpath']))):
                            raise CoordError('invalid project root subpath')
                    elif set(value) != {'kind'}: raise CoordError('invalid special filesystem path')
                else: raise CoordError('unsupported filesystem path type')
            clean['entries'] = copy.deepcopy(entries)
        depth = fs.get('globScanMaxDepth')
        if depth is not None:
            if isinstance(depth, bool) or not isinstance(depth, int) or not 0 <= depth <= 1000:
                raise CoordError('invalid glob scan depth')
            clean['globScanMaxDepth'] = depth
        if any(clean.get(k) for k in ('read','write','entries')):
            result['fileSystem'] = clean
    if not result:
        raise CoordError('empty requested permissions')
    return result


def permission_choices(profile):
    groups = [name for name in ('network', 'fileSystem') if name in profile]
    options = groups + (['all'] if len(groups) == 2 else [])
    return ['deny', 'custom'] + [f'{name}_{scope}' for name in options for scope in ('turn', 'session')]


def permission_items(profile):
    items = []
    if 'network' in profile: items.append(('network', 'Network access'))
    fs = profile.get('fileSystem', {})
    for kind in ('read','write'):
        items.extend((f'{kind}:{i}', f'{kind}: {path}') for i, path in enumerate(fs.get(kind) or []))
    items.extend((f'fs:{i}', f"{entry['access']}: {entry['path']}") for i, entry in enumerate(fs.get('entries', [])))
    return items


def selected_permissions(profile, selection):
    allowed = dict(permission_items(profile))
    if not isinstance(selection, list) or not selection or any(not isinstance(x, str) or x not in allowed for x in selection) or len(selection) != len(set(selection)):
        raise CoordError('invalid permission subset')
    fs = profile.get('fileSystem', {})
    reads = {f'read:{i}' for i, path in enumerate(fs.get('read') or []) if path != '/'}
    reads.update(f'fs:{i}' for i, entry in enumerate(fs.get('entries', []))
                 if entry['access'] == 'read' and entry['path'] != {'type':'path','path':'/'}
                 and entry['path'] != {'type':'special','value':{'kind':'root'}})
    writes = {f'write:{i}' for i, _ in enumerate(fs.get('write') or [])}
    writes.update(f'fs:{i}' for i, entry in enumerate(fs.get('entries', [])) if entry['access'] == 'write')
    if writes.intersection(selection) and not reads.issubset(selection):
        raise CoordError('selected writes require potentially narrowing read restrictions; select them or choose another subset')
    result = {}
    if 'network' in selection: result['network'] = copy.deepcopy(profile['network'])
    selected = {'read': [], 'write': [], 'entries': []}
    for token in selection:
        if token == 'network': continue
        kind, index = token.split(':')
        source = 'entries' if kind == 'fs' else kind
        selected[source].append(copy.deepcopy(fs[source][int(index)]))
    if any(selected.values()):
        # Deny entries are restrictions, not optional capabilities. Preserve all
        # of them whenever any filesystem grant is returned.
        selected['entries'] = [copy.deepcopy(entry) for index, entry in enumerate(fs.get('entries', []))
                               if f'fs:{index}' in selection or entry['access'] == 'deny']
        result['fileSystem'] = {'read': selected['read'] or None, 'write': selected['write'] or None}
        if selected['entries']: result['fileSystem']['entries'] = selected['entries']
        if 'globScanMaxDepth' in fs and selected['entries']: result['fileSystem']['globScanMaxDepth'] = fs['globScanMaxDepth']
    return result


def text(value, label, limit=512):
    if not isinstance(value, str) or not value or len(value) > limit:
        raise CoordError('invalid approval ' + label)
    return value


def context(params):
    return {key: text(params.get(key), key) for key in ('threadId', 'turnId', 'itemId')}


def presentation(request):
    params = request['params']
    if request['method'] == METHODS[2]:
        profile = permission_profile(params.get('permissions'))
        cwd = text(params.get('cwd'), 'cwd', 4096)
        if not cwd.startswith('/'):
            raise CoordError('invalid permission working directory')
        environment = params.get('environmentId')
        if environment is not None:
            environment = text(environment, 'environment', 512)
        details = [label for _, label in permission_items(profile)]
        if 'fileSystem' in profile:
            details.append('For filesystem selections, all requested deny restrictions are retained. Selecting a write requires potentially narrowing requested read restrictions; unrelated paths may remain ambiguous.')
        if 'globScanMaxDepth' in profile.get('fileSystem', {}):
            details.append(f"Glob scan maximum depth: {profile['fileSystem']['globScanMaxDepth']}")
        preview = {'kind': 'permissions', 'reason': text(params.get('reason') or 'No reason provided', 'reason', 4096),
                   'cwd': cwd, 'permissions': '\n'.join(details)}
        if environment is not None: preview['environment'] = environment
        if len(preview['permissions']) > 4096:
            raise CoordError('approval preview exceeds the complete-display limit')
        return preview, permission_choices(profile)
    result = {'kind': 'command' if request['method'] == METHODS[0] else 'file change'}
    for key in ('reason', 'command', 'cwd', 'grantRoot'):
        if isinstance(params.get(key), str):
            if len(params[key]) > 4096:
                raise CoordError('approval preview exceeds the complete-display limit')
            result[key] = params[key]
    network = params.get('networkApprovalContext')
    if isinstance(network, dict):
        result['kind'] = 'network access'
        result['host'] = str(network.get('host', ''))[:512]
        result['protocol'] = str(network.get('protocol', ''))[:64]
    # Extra permission content is explicit in the prompt; never silently grant
    # amendments or session-wide privileges via this narrow choice set.
    if params.get('additionalPermissions') is not None:
        raise CoordError('additional permission approvals are not supported by this one-time prompt')
    offered = params.get('availableDecisions')
    if offered is not None and not isinstance(offered, list):
        raise CoordError('invalid provider decision list')
    choices = [choice for choice in CHOICES if offered is None or choice in offered]
    if not choices:
        raise CoordError('provider offered no supported one-time approval decision')
    return result, choices


class ApprovalInbox:
    """One WebServer/project lifetime; no pending serialization or recovery."""
    def __init__(self):
        self.lock = threading.RLock()
        self.epochs = {}
        self.rows = {}
        self.audit = None
        self.operator = None

    def _finish(self, row, outcome):
        row['status'] = outcome
        if self.audit:
            self.audit('approval_' + outcome, correlation_id=row['handle'])

    def _expire(self):
        now = time.monotonic()
        for row in self.rows.values():
            if row['status'] in ('pending', 'proposed') and row['deadline'] <= now:
                self._finish(row, 'expired')
        if len(self.rows) > 256:
            for handle, row in list(self.rows.items()):
                if row['status'] not in ('pending', 'proposed'):
                    del self.rows[handle]
                    if len(self.rows) <= 256:
                        break

    def invalidate(self, host):
        with self.lock:
            for row in self.rows.values():
                if row['host'] == host and row['status'] in ('pending', 'proposed'):
                    self._finish(row, 'disconnected')
            self.epochs.pop(host, None)

    def sync(self, host, owner, epoch):
        text(epoch, 'epoch')
        with self.lock:
            if self.epochs.get(host) != (owner, epoch):
                self.invalidate(host)
                self.epochs[host] = (owner, epoch)
            self._expire()
            return {'epoch': epoch}

    @staticmethod
    def route(coord, host, thread):
        text(thread, 'thread')
        rows = coord.db.execute('''SELECT b.session_id,s.agent FROM bridge_bindings b
            JOIN remote_session_hosts h ON h.session_id=b.session_id
            JOIN sessions s ON s.id=b.session_id
            WHERE b.thread_id=? AND h.host_id=?''', (thread, host)).fetchall()
        return dict(rows[0]) if len(rows) == 1 else None

    def publish(self, coord, host, owner, p):
        base = {'epoch', 'key', 'method', 'context', 'preview', 'choices', 'remaining'}
        if set(p) not in (base, base | {'requested_permissions'}):
            raise CoordError('invalid approval publication fields')
        permission = p['method'] == METHODS[2]
        if permission != ('requested_permissions' in p):
            raise CoordError('invalid requested permissions publication')
        profile = permission_profile(p['requested_permissions']) if permission else None
        valid_choices = permission_choices(profile) if permission else CHOICES
        if (p['method'] not in METHODS or not isinstance(p['context'], dict)
                or set(p['context']) != {'threadId', 'turnId', 'itemId'}
                or not isinstance(p['preview'], dict) or not isinstance(p['choices'], list)
                or not p['choices'] or any(choice not in valid_choices for choice in p['choices'])
                or len(set(p['choices'])) != len(p['choices'])):
            raise CoordError('invalid approval publication')
        if permission and p['choices'] != valid_choices:
            raise CoordError('invalid permission choices')
        if (set(p['preview']) - {'kind','reason','command','cwd','grantRoot','host','protocol','permissions','environment'}
                or any(not isinstance(value, str) or len(value) > 4096 for value in p['preview'].values())):
            raise CoordError('invalid redacted approval preview')
        scope = context(p['context'])
        key = text(p['key'], 'request key')
        remaining = p['remaining']
        if (isinstance(remaining, bool) or not isinstance(remaining, (int, float))
                or not math.isfinite(remaining) or not 0 < remaining <= 120):
            raise CoordError('invalid approval lifetime')
        route = self.route(coord, host, scope['threadId'])
        if not route or not self.operator or not self.audit:
            raise CoordError('approval has no bound project/operator route')
        if not coord.db.execute('SELECT 1 FROM operator_identities WHERE session_id=?', (self.operator,)).fetchone():
            raise CoordError('approval operator enrollment is unavailable')
        with self.lock:
            if self.epochs.get(host) != (owner, p['epoch']):
                raise CoordError('approval connection epoch is stale')
            self._expire()
            for row in self.rows.values():
                if (row['host'], row['owner'], row['epoch'], row['key']) == (host, owner, p['epoch'], key):
                    if any(row.get(field) != p.get(field) for field in ('method', 'context', 'preview', 'choices', 'requested_permissions')):
                        raise CoordError('approval request identity changed')
                    return {'handle': row['handle']}
            handle = str(uuid.uuid4())
            self.rows[handle] = dict(copy.deepcopy(p), handle=handle, host=host, owner=owner,
                operator=self.operator, session=route['session_id'], agent=route['agent'],
                deadline=time.monotonic() + remaining, status='pending', decision=None)
            return {'handle': handle}

    def public(self, operator):
        with self.lock:
            self._expire()
            return [dict(handle=row['handle'], agent=row['agent'], preview=copy.deepcopy(row['preview']),
                         choices=list(row['choices']), status=row['status'], decision=row['decision'],
                         item_outcome=copy.deepcopy(row.get('item_outcome')),
                         permission_items=[{'id': key, 'label': label} for key, label in permission_items(permission_profile(row['requested_permissions']))] if row['method'] == METHODS[2] else [],
                         remaining=max(0, row['deadline'] - time.monotonic()))
                    for row in self.rows.values() if row['operator'] == operator]

    def decide(self, operator, handle, choice):
        with self.lock:
            self._expire()
            row = self.rows.get(text(handle, 'handle'))
            if not row or row['operator'] != operator or row['status'] != 'pending':
                raise CoordError('unknown, stale or already decided approval')
            if row['method'] == METHODS[2] and isinstance(choice, dict):
                if set(choice) != {'scope','selection'} or choice['scope'] not in ('turn','session'):
                    raise CoordError('invalid permission grant scope')
                selected_permissions(permission_profile(row['requested_permissions']), choice['selection'])
            elif not isinstance(choice, str) or choice not in row['choices'] or choice == 'custom':
                raise CoordError('decision was not offered for this approval')
            row['decision'], row['status'] = choice, 'proposed'
            # This is not a response or execution receipt. Only the owning
            # bridge can advance it after successfully writing on its socket.
            return {'handle': handle, 'status': 'proposed'}

    def poll(self, host, owner, epoch, handles):
        if not isinstance(handles, list) or len(handles) > 128 or any(not isinstance(h, str) for h in handles):
            raise CoordError('invalid approval handles')
        with self.lock:
            self._expire()
            if self.epochs.get(host) != (owner, epoch):
                raise CoordError('approval connection epoch is stale')
            result = []
            for handle in handles:
                row = self.rows.get(handle)
                if not row or (row['host'], row['owner'], row['epoch']) != (host, owner, epoch):
                    raise CoordError('approval projection disappeared or changed owner')
                result.append({'handle': handle, 'status': row['status'], 'decision': row['decision']})
            return {'requests': result}

    def finish(self, host, owner, epoch, handle, outcome, item_outcome=None):
        if outcome not in ('response_sent', 'response_uncertain', 'disconnected', 'expired', 'resolved', 'completed'):
            raise CoordError('invalid approval outcome')
        if item_outcome is not None:
            if (outcome != 'completed' or not isinstance(item_outcome, dict)
                    or set(item_outcome) not in ({'type', 'status'}, {'type', 'status', 'exitCode'})
                    or item_outcome.get('type') not in ('commandExecution', 'fileChange')
                    or item_outcome.get('status') not in ('completed', 'failed', 'declined')
                    or ('exitCode' in item_outcome and (item_outcome['type'] != 'commandExecution'
                        or isinstance(item_outcome['exitCode'], bool) or not isinstance(item_outcome['exitCode'], int)))):
                raise CoordError('invalid approval item outcome')
        with self.lock:
            self._expire()
            row = self.rows.get(handle)
            if not row or (row['host'], row['owner'], row['epoch']) != (host, owner, epoch):
                raise CoordError('approval outcome has wrong owner or epoch')
            if item_outcome is not None:
                if row['method'] == METHODS[2]:
                    raise CoordError('permission approval has no command or file item outcome')
                expected = 'commandExecution' if row['method'] == METHODS[0] else 'fileChange'
                if item_outcome['type'] != expected:
                    raise CoordError('approval item type does not match request')
                previous = row.get('item_outcome')
                if previous is not None and previous != item_outcome:
                    raise CoordError('approval item outcome conflicts with recorded evidence')
            if row['status'] == outcome:
                if item_outcome is not None and row.get('item_outcome') is None:
                    row['item_outcome'] = copy.deepcopy(item_outcome)
                return {'status': outcome}
            allowed = {'response_sent': ('proposed',), 'response_uncertain': ('proposed', 'response_sent', 'expired'), 'resolved': ('pending', 'proposed', 'response_sent'),
                       'completed': ('pending', 'proposed', 'response_sent', 'resolved'), 'disconnected': ('pending', 'proposed'),
                       'expired': ('pending', 'proposed')}
            if row['status'] not in allowed[outcome]:
                raise CoordError('approval outcome is late or inconsistent')
            if item_outcome is not None:
                row['item_outcome'] = copy.deepcopy(item_outcome)
            self._finish(row, outcome)
            return {'status': outcome}


class ApprovalPump:
    """Independent of outbound callers and the RpcClient's sole socket reader."""
    def __init__(self, rpc, states):
        self.rpc, self.states = rpc, states
        self.stop = threading.Event()
        self.worker = None
        self.process = uuid.uuid4().hex
        self.epoch = None
        self.pending = {}
        self.sequence = 0

    def start(self):
        self.worker = threading.Thread(target=self.run, name='agent-chat-approval-pump', daemon=True)
        self.worker.start()
        return self

    def close(self):
        self.stop.set()
        if self.worker and self.worker is not threading.current_thread():
            self.worker.join(4)
            if self.worker.is_alive():
                raise TransportError('approval pump did not stop')
        self._invalidate()

    def _invalidate(self):
        for item in list(self.pending.values()):
            if item['response_sent'] and item['reported']:
                continue
            try:
                item['state'].call('approval-finish', epoch=item['epoch'], handle=item['public'], outcome='response_uncertain' if item['response_sent'] else 'disconnected')
            except (CoordError, OSError, ValueError):
                pass
        self.pending.clear()

    def tick(self):
        connection = self.rpc.approval_connection()
        if connection is None:
            self._invalidate()
            return
        generation, notification_floor = connection
        epoch = self.process + ':' + str(generation)
        if epoch != self.epoch:
            self._invalidate()
            self.epoch = epoch
            self.sequence = notification_floor
        states = list(self.states())
        for state in states:
            state.call('approval-sync', epoch=epoch)
        # Drain native requests without ever blocking the sole reader.
        while True:
            request = self.rpc.poll_server_request()
            if request is None:
                break
            scope = context(request['params'])
            routes = [state for state in states if state.call('approval-route', thread=scope['threadId'])['route']]
            if len(routes) != 1:
                raise CoordError('native approval has no unique bound project route')
            state = routes[0]
            preview, choices = presentation(request)
            extra = {'requested_permissions': request['params']['permissions']} if request['method'] == METHODS[2] else {}
            reply = state.call('approval-publish', epoch=epoch, key=uuid.uuid4().hex,
                method=request['method'], context=scope, preview=preview, choices=choices,
                remaining=min(120, request['deadline'] - time.monotonic()), **extra)
            self.pending[request['handle']] = dict(request=request, state=state, public=reply['handle'],
                                                  epoch=epoch, response_sent=False, reported=False, native_resolved=False)
        for item in list(self.pending.values()):
            request = item['request']
            if item['response_sent'] or item['native_resolved']:
                continue
            rows = item['state'].call('approval-poll', epoch=epoch, handles=[item['public']])['requests']
            row = rows[0]
            if row['status'] in ('expired', 'disconnected'):
                raise TransportError('approval expired before owning response')
            if row['status'] == 'proposed':
                if request['method'] == METHODS[2]:
                    choice = row['decision']
                    profile = permission_profile(request['params']['permissions'])
                    if isinstance(choice, dict):
                        response = {'permissions': selected_permissions(profile, choice['selection']), 'scope': choice['scope']}
                    elif choice == 'deny':
                        response = {'permissions': {}, 'scope': 'turn'}
                    else:
                        group, scope = choice.rsplit('_', 1)
                        response = {'permissions': profile if group == 'all' else {group: profile[group]}, 'scope': scope}
                else:
                    response = {'decision': row['decision']}
                self.rpc.respond_server_request(request['handle'], response)
                item['response_sent'] = True
                item['state'].call('approval-finish', epoch=epoch, handle=item['public'], outcome='response_sent')
                item['reported'] = True
        while True:
            available = self.rpc.notifications_since(self.sequence)
            if not available:
                break
            self.sequence, message = available[0]
            params = message.get('params', {})
            for handle, item in list(self.pending.items()):
                req = item['request']
                scope = req['params']
                resolved = (message.get('method') == 'serverRequest/resolved'
                    and params.get('threadId') == scope['threadId'] and params.get('requestId') == req['id'])
                completed = (message.get('method') == 'item/completed'
                    and params.get('threadId') == scope['threadId'] and params.get('turnId') == scope['turnId']
                    and isinstance(params.get('item'), dict) and params['item'].get('id') == scope['itemId'])
                if resolved or completed:
                    fields = {}
                    if completed and req['method'] in METHODS[:2]:
                        native = params['item']
                        expected = 'commandExecution' if req['method'] == METHODS[0] else 'fileChange'
                        if native.get('type') == expected and native.get('status') in ('completed', 'failed', 'declined'):
                            observed = {'type': expected, 'status': native['status']}
                            code = native.get('exitCode')
                            if expected == 'commandExecution' and isinstance(code, int) and not isinstance(code, bool):
                                observed['exitCode'] = code
                            fields['item_outcome'] = observed
                    item['state'].call('approval-finish', epoch=epoch, handle=item['public'],
                                       outcome='completed' if completed else 'resolved', **fields)
                    item['native_resolved'] = True
                    if completed:
                        del self.pending[handle]
        for handle, item in list(self.pending.items()):
            if item['request']['deadline'] <= time.monotonic():
                if not item['response_sent'] and not item['native_resolved']:
                    raise TransportError('native approval timed out')
                # An outcome already has a nonsecret audit record. Never keep a
                # native capability indefinitely waiting for a completion event.
                del self.pending[handle]

    def run(self):
        while not self.stop.is_set():
            try:
                self.tick()
            except (CoordError, TransportError, OSError, ValueError, KeyError, TypeError):
                # A lost route/projection/lease never authorizes a fallback or a
                # response on a replacement connection. Close cancels waiters.
                self.rpc.close()
                self._invalidate()
            self.stop.wait(0.1)
