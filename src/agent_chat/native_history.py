"""Read-only history compatibility for native runtimes without turn pagination."""
from .core import CoordError
from .rpc import RpcError

_CURSOR = 'agent-chat-full-history-offset:'


class HistoryUnavailable(CoordError):
    """Neither native history read can supply exact usable proof."""


KNOWN_ERROR_CODES = frozenset(('contextWindowExceeded','sessionBudgetExceeded','usageLimitExceeded','rateLimitExceeded','flexUnavailable','serverOverloaded','cyberPolicy','misalignmentPolicyViolation','tooManyDenials','internalServerError','unauthorized','badRequest','threadRollbackFailed','sandboxError','other',))


def _utf8_text(value, bound):
    if not isinstance(value, str):
        return None
    try:
        return value if len(value.encode('utf-8')) <= bound else None
    except UnicodeEncodeError:
        return None


HTTP_ERROR_TAGS = frozenset(('httpConnectionFailed', 'responseStreamConnectionFailed',
                            'responseStreamDisconnected', 'responseTooManyFailedAttempts'))


def _finite_error_info(value):
    if isinstance(value, str):
        return value if _utf8_text(value, 256) in KNOWN_ERROR_CODES else None
    if not isinstance(value, dict) or len(value) != 1:
        return None
    tag = next(iter(value))
    inner = value[tag]
    if not isinstance(inner, dict):
        return None
    if tag in HTTP_ERROR_TAGS:
        if set(inner) - {'httpStatusCode'}:
            return None
        if 'httpStatusCode' not in inner:
            return {tag: {}}
        code = inner['httpStatusCode']
        if code is None:
            return {tag: {'httpStatusCode': None}}
        if type(code) is int and 0 <= code <= 65535:
            return {tag: {'httpStatusCode': code}}
        return None
    if tag == 'activeTurnNotSteerable' and set(inner) == {'turnKind'}:
        kind = inner['turnKind']
        if isinstance(kind, str) and kind in ('review', 'compact'):
            return {tag: {'turnKind': kind}}
    return None


def _project_turn_metadata(turn):
    """Conservative named metadata, never raw objects/items or wire fidelity."""
    fields = ('message', 'additionalDetails', 'codexErrorInfo')
    if not isinstance(turn, dict):
        return dict(id=None, status=None, error=None,
                    error_field_status=dict(error='unsupported', **{k: 'unknown' for k in fields}))
    status = {k: 'absent' for k in fields}
    error = turn.get('error')
    projected = None
    if 'error' not in turn:
        error_state = 'absent'
    elif error is None:
        error_state = 'null'
    elif not isinstance(error, dict):
        error_state = 'unsupported'
        status = {k: 'unknown' for k in fields}
    else:
        error_state = 'supported'
        projected = {}
        for name in fields:
            value = error.get(name)
            safe = _utf8_text(value, 256 if name == 'codexErrorInfo' else 4096)
            if name == 'codexErrorInfo':
                safe = _finite_error_info(value)
            projected[name] = safe
            status[name] = ('absent' if name not in error else 'null' if value is None
                            else 'supported' if safe is not None else 'unsupported')
    return dict(id=_utf8_text(turn.get('id'), 512), status=_utf8_text(turn.get('status'), 256),
                error=projected, error_field_status=dict(error=error_state, **status))


def turns_page(rpc, params):
    cursor = params.get('cursor')
    fallback_cursor = isinstance(cursor, str) and cursor.startswith(_CURSOR)
    if not fallback_cursor:
        try:
            response = rpc.request('thread/turns/list', params)
            if params.get('itemsView') == 'notLoaded':
                if not isinstance(response, dict) or not isinstance(response.get('data'), list):
                    raise HistoryUnavailable('native turn metadata is unavailable')
                return dict(data=[_project_turn_metadata(turn) for turn in response['data']],
                            nextCursor=response.get('nextCursor'))
            return response
        except RpcError as error:
            if error.code != -32601:
                raise
    thread_id = params['threadId']
    response = rpc.request('thread/read', {'threadId': thread_id, 'includeTurns': True})
    thread = response.get('thread') if isinstance(response, dict) else None
    if (not isinstance(thread, dict) or thread.get('id') != thread_id
            or not isinstance(thread.get('turns'), list)
            or any(not isinstance(turn, dict) or not isinstance(turn.get('id'), str) or not turn['id']
                   or turn.get('status') not in ('inProgress', 'completed', 'interrupted', 'failed')
                   or not isinstance(turn.get('items'), list)
                   or any(not isinstance(item, dict) or not isinstance(item.get('type'), str) or not item['type']
                          for item in turn['items']) for turn in thread['turns'])):
        raise HistoryUnavailable('native history readback must include the exact thread and explicit turns')
    if cursor and not fallback_cursor:
        raise CoordError('native history pagination changed; inspect current runtime')
    try:
        offset = int(cursor[len(_CURSOR):]) if fallback_cursor else 0
    except ValueError:
        raise CoordError('native history fallback cursor is invalid')
    if offset < 0:
        raise CoordError('native history fallback cursor is invalid')
    turns = thread['turns']
    if params.get('sortDirection', 'desc') == 'desc':
        turns = list(reversed(turns))
    limit = params.get('limit', 25)
    selected = turns[offset:offset + limit]
    if params.get('itemsView', 'summary') == 'summary':
        # Our workers consume only IDs/status and user-message client IDs. Keep
        # tool outputs and message bodies out of their summary evidence.
        selected = [dict(id=turn['id'], status=turn['status'], error=turn.get('error'), items=[
            {key: item[key] for key in ('id', 'type', 'clientId') if key in item}
            for item in turn['items']]) for turn in selected]
    elif params.get('itemsView') == 'notLoaded':
        selected = [_project_turn_metadata(turn) for turn in selected]
    following = offset + len(selected)
    return {'data': selected, 'nextCursor': _CURSOR + str(following) if following < len(turns) else None}


def bounded_turns_summary_page(rpc, thread_id, limit=25, cursor=None):
    """Provider-backed finite summary only; deliberately has no thread/read fallback."""
    if (not isinstance(thread_id, str) or not thread_id or type(limit) is not int or not 1 <= limit <= 100):
        raise CoordError('bounded native turn summary requires an exact thread and finite limit')
    params = {'threadId': thread_id, 'limit': limit, 'itemsView': 'summary', 'sortDirection': 'desc'}
    if cursor:
        params['cursor'] = cursor
    try:
        page = rpc.request('thread/turns/list', params)
    except RpcError as error:
        if error.code == -32601:
            raise HistoryUnavailable('bounded native turn summary is unsupported') from error
        raise
    if not isinstance(page, dict) or not isinstance(page.get('data'), list):
        raise HistoryUnavailable('bounded native turn summary is unavailable')
    result = []
    for turn in page['data']:
        if (not isinstance(turn, dict) or not isinstance(turn.get('id'), str) or not turn['id']
                or turn.get('status') not in ('inProgress', 'completed', 'interrupted', 'failed')
                or 'error' not in turn or not isinstance(turn.get('items'), list)):
            raise HistoryUnavailable('bounded native turn summary is incomplete')
        items = []
        for item in turn['items']:
            if not isinstance(item, dict) or not isinstance(item.get('type'), str) or not item['type']:
                raise HistoryUnavailable('bounded native turn summary item is malformed')
            items.append({key: item[key] for key in ('id', 'type', 'clientId') if key in item})
        result.append(dict(id=turn['id'], status=turn['status'], error=turn['error'], items=items))
    return {'data': result, 'nextCursor': page.get('nextCursor')}


def turn_items(rpc, thread_id, turn_id, limit=100):
    """Read only items for one exact turn, with a bounded native fallback."""
    if (not isinstance(thread_id, str) or not thread_id or not isinstance(turn_id, str) or not turn_id
            or type(limit) is not int or limit < 1 or limit > 1000):
        raise CoordError('exact native turn item read requires bounded thread and turn IDs')
    cursor = None
    items = []
    for _ in range(10):
        params = {'threadId': thread_id, 'turnId': turn_id, 'limit': limit,
                  'sortDirection': 'asc'}
        if cursor:
            params['cursor'] = cursor
        try:
            page = rpc.request('thread/items/list', params)
        except RpcError as error:
            if error.code == -32601:
                raise HistoryUnavailable('bounded exact native turn items are unsupported') from error
            raise
        if not isinstance(page, dict) or not isinstance(page.get('data'), list):
            raise HistoryUnavailable('exact native turn items are unavailable')
        for entry in page['data']:
            if (not isinstance(entry, dict) or entry.get('turnId') != turn_id
                    or not isinstance(entry.get('item'), dict)
                    or not isinstance(entry['item'].get('type'), str) or not entry['item']['type']):
                raise HistoryUnavailable('exact native turn item projection is malformed')
            items.append(entry['item'])
            if len(items) > 1000:
                raise HistoryUnavailable('exact native turn exceeds the finite item proof bound')
        cursor = page.get('nextCursor')
        if not cursor:
            return items
    raise HistoryUnavailable('exact native turn exceeds the finite page proof bound')
