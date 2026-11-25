"""Ephemeral, job-scoped observed content; no durable or per-source claim."""
from __future__ import annotations


def correlate(job_id, thread_id, records, *, start_turn=None, gap=None):
    unknown = {'status': 'unknown', 'reason': gap or 'no_exact_user_item', 'attribution': 'job_turn_only', 'coverage': 'unverified'}
    if gap:
        return unknown
    matches = []
    for seq, event, meta in records:
        if not isinstance(event, dict) or not isinstance(meta, dict) or meta.get('sequence') != seq:
            return dict(unknown, reason='invalid_record')
        p = event.get('params')
        if not isinstance(p, dict) or p.get('threadId') != thread_id:
            continue
        if event.get('method') in ('thread/unloaded', 'thread/closed', 'thread/unsubscribed'):
            return dict(unknown, reason='observed_lifecycle_gap')
        item = p.get('item')
        if event.get('method') == 'item/started' and isinstance(item, dict) and item.get('type') == 'userMessage' and item.get('clientId') == job_id and isinstance(p.get('turnId'), str) and p['turnId']:
            matches.append((seq, p['turnId']))
    if len(matches) != 1:
        return dict(unknown, reason='ambiguous_input' if matches else 'no_exact_user_item')
    input_sequence, turn = matches[0]
    if start_turn is not None and start_turn != turn:
        return dict(unknown, reason='turn_mismatch')
    # Multiple user items on the same turn cannot support exclusive job attribution.
    for _, event, _ in records:
        p = event.get('params', {}) if isinstance(event, dict) else {}
        item = p.get('item', {}) if isinstance(p, dict) else {}
        if (event.get('method') == 'item/started' and p.get('threadId') == thread_id and p.get('turnId') == turn
                and isinstance(item, dict) and item.get('type') == 'userMessage' and item.get('clientId') != job_id):
            return dict(unknown, reason='ambiguous_input')
    for seq, event, meta in records:
        p = event.get('params', {}) if isinstance(event, dict) else {}
        if (seq > input_sequence and event.get('method') == 'item/agentMessage/delta' and p.get('threadId') == thread_id
                and p.get('turnId') == turn and isinstance(p.get('itemId'), str) and p['itemId']
                and isinstance(p.get('delta'), str) and p['delta'].strip()):
            import math
            if (type(meta.get('observed_at')) not in (int, float) or not math.isfinite(meta['observed_at'])
                    or type(meta.get('observed_monotonic_ns')) is not int or meta['observed_monotonic_ns'] < 0
                    or not isinstance(meta.get('connection_instance'), str) or not meta['connection_instance']
                    or type(meta.get('connection_epoch')) is not int or meta['connection_epoch'] <= 0):
                return dict(unknown, reason='invalid_reader_clock')
            return {'status':'observed', 'attribution':'job_turn_only', 'coverage':'unverified',
                    'job_id':job_id, 'thread_id':thread_id, 'turn_id':turn, 'item_id':p['itemId'],
                    'observed_at':meta.get('observed_at'), 'observed_monotonic_ns':meta.get('observed_monotonic_ns'),
                    'connection_instance':meta.get('connection_instance'), 'connection_epoch':meta.get('connection_epoch'), 'sequence':seq}
    return dict(unknown, reason='no_agent_text_observed')


def _exact_terminal(window):
    """Find a terminal event after one exact observed original user item."""
    matches = []
    for seq, event, meta in window['records']:
        if not isinstance(event, dict) or not isinstance(meta, dict) or meta.get('sequence') != seq:
            return False
        params = event.get('params')
        if not isinstance(params, dict) or params.get('threadId') != window['thread']:
            continue
        item = params.get('item')
        if (event.get('method') == 'item/started' and isinstance(item, dict)
                and item.get('type') == 'userMessage' and item.get('clientId') == window['job']
                and isinstance(params.get('turnId'), str) and params['turnId']):
            matches.append((seq, params['turnId']))
    if len(matches) != 1:
        return False
    input_seq, turn_id = matches[0]
    if window['turn'] is not None and window['turn'] != turn_id:
        return False
    for seq, event, _ in window['records']:
        params = event.get('params', {})
        if not isinstance(params, dict):
            continue
        turn = params.get('turn')
        if (seq > input_seq and event.get('method') == 'turn/completed'
                and params.get('threadId') == window['thread'] and isinstance(turn, dict)
                and turn.get('id') == turn_id and turn.get('status') in ('completed', 'interrupted', 'failed')):
            return True
    return False


class StageWindows:
    """Non-authoritative, bounded process-local windows; never initiates native calls."""
    def __init__(self, rpc, state=None):
        self.rpc = rpc
        self.state = state
        self.windows = {}

    def arm(self, job):
        ident = job['id']
        if ident in self.windows:
            return
        if len(self.windows) >= 128:
            # Preserve immediate inspection; reclaim only at capacity and never
            # discard a pending publication or withdrawal.
            for prior_id, prior in list(self.windows.items()):
                gap = prior.get('gap')
                gap_settled = (gap and (not prior.get('publish_attempted')
                                        or prior.get('censored_reason') == gap))
                if prior.get('settled') or gap_settled:
                    del self.windows[prior_id]
                    break
            if len(self.windows) >= 128:
                return
        try:
            identity = self.rpc.connection_identity()
            import time
            boundary = self.rpc.notification_boundary(deadline=time.monotonic() + self.rpc.timeout)
            after = self.rpc.connection_identity()
            key = ('client_instance', 'connection_epoch')
            if any(identity.get(k) != after.get(k) for k in key):
                return
            self.windows[ident] = {'job':ident, 'thread':job['thread_id'], 'identity':identity,
                                   'cursor':boundary, 'records':[], 'turn':None, 'result':None}
        except Exception:
            # Observation must never interfere with established dispatch guards.
            return

    def started(self, job_id, result):
        window = self.windows.get(job_id)
        if window is None:
            return
        turn = result.get('turn') if isinstance(result, dict) else None
        turn_id = turn.get('id') if isinstance(turn, dict) else None
        if not isinstance(turn_id, str) or not turn_id:
            window['settled'] = False
            window['gap'] = 'invalid_start_response'
            window['result'] = correlate(window['job'], window['thread'], [], gap=window['gap'])
        else:
            window['turn'] = turn_id
            # A start response can arrive after observed terminal content.
            # Reopen only to withdraw an inconsistent previously published turn.
            if window.get('settled') and not _exact_terminal(window):
                window['settled'] = False
                window['gap'] = 'turn_mismatch'
                window['result'] = correlate(window['job'], window['thread'], [], gap='turn_mismatch')
                self._publish(window)

    def poll(self):
        for ident, window in list(self.windows.items()):
            if window.get('gap'):
                window['settled'] = False
                self._publish(window)
                continue
            # Exact terminal plus settled metadata closes this observation.
            # Keep its final result inspectable until capacity reclamation.
            if window.get('settled'):
                continue
            try:
                identity = self.rpc.connection_identity()
                prior = window['identity']
                if any(identity.get(k) != prior.get(k) for k in ('client_instance', 'connection_epoch')):
                    raise ValueError('reconnect')
                raw, stamps = self.rpc.notification_snapshot_since(window['cursor'])
                if [seq for seq, _ in raw] != [stamp.get('sequence') for stamp in stamps]:
                    raise ValueError('window_gap')
                terminal_arrived = False
                for (seq, event), stamp in zip(raw, stamps):
                    if (stamp.get('connection_instance') != prior['client_instance']
                        or stamp.get('connection_epoch') != prior['connection_epoch']):
                        raise ValueError('reconnect')
                    window['records'].append((seq, event, stamp))
                    window['cursor'] = seq
                    params = event.get('params') if isinstance(event, dict) else None
                    turn = params.get('turn') if isinstance(params, dict) else None
                    if (isinstance(params, dict) and event.get('method') == 'turn/completed'
                            and params.get('threadId') == window['thread'] and isinstance(turn, dict)
                            and isinstance(turn.get('id'), str) and turn['id']
                            and turn.get('status') in ('completed', 'interrupted', 'failed')
                            and (window['turn'] is None or window['turn'] == turn['id'])):
                        terminal_arrived = True
                if len(window['records']) > 1024:
                    raise ValueError('window_overflow')
                window['result'] = correlate(window['job'], window['thread'], window['records'], start_turn=window['turn'])
                if terminal_arrived:
                    window['terminal_observed'] = _exact_terminal(window)
            except Exception:
                window['gap'] = 'window_unavailable'
                window['result'] = correlate(window['job'], window['thread'], [], gap='window_unavailable')
            self._publish(window)
            window['settled'] = False
            if not window.get('gap') and window.get('terminal_observed'):
                result = window['result']
                if result['status'] == 'observed':
                    settled = (not self.state or not hasattr(self.state, 'native_stage_record')
                               or window.get('published_result') == result)
                else:
                    settled = (result.get('reason') in ('ambiguous_input', 'no_agent_text_observed')
                               and (not window.get('publish_attempted')
                                    or window.get('censored_reason') == result.get('reason')))
                window['settled'] = settled

    def _publish(self, window):
        result = window.get('result')
        if not self.state or not result:
            return
        try:
            if result['status'] == 'observed' and hasattr(self.state, 'native_stage_record'):
                if window.get('published_result') == result:
                    return
                window['publish_attempted'] = True
                self.state.native_stage_record(window['job'], result)
                window['published'] = True
                window['published_result'] = dict(result)
            elif (window.get('publish_attempted') and hasattr(self.state, 'native_stage_censor')
                  and result.get('reason') in {'window_unavailable','ambiguous_input','turn_mismatch',
                       'observed_lifecycle_gap','invalid_record','invalid_reader_clock','invalid_start_response'}):
                if window.get('censored_reason') == result['reason']:
                    return
                self.state.native_stage_censor(window['job'], result['reason'])
                window['censored_reason'] = result['reason']
        except Exception:
            # A telemetry failure never resubmits native input; a later poll may retry only metadata.
            return
