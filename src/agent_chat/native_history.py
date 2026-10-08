"""Read-only history compatibility for native runtimes without turn pagination."""
from .core import CoordError
from .rpc import RpcError

_CURSOR = 'agent-chat-full-history-offset:'


class HistoryUnavailable(CoordError):
    """Neither native history read can supply exact usable proof."""


def turns_page(rpc, params):
    cursor = params.get('cursor')
    fallback_cursor = isinstance(cursor, str) and cursor.startswith(_CURSOR)
    if not fallback_cursor:
        try:
            return rpc.request('thread/turns/list', params)
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
        selected = [dict(id=turn['id'], status=turn['status'], items=[
            {key: item[key] for key in ('id', 'type', 'clientId') if key in item}
            for item in turn['items']]) for turn in selected]
    elif params.get('itemsView') == 'notLoaded':
        selected = [dict(id=turn['id'], status=turn['status']) for turn in selected]
    following = offset + len(selected)
    return {'data': selected, 'nextCursor': _CURSOR + str(following) if following < len(turns) else None}
