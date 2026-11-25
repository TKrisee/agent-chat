"""Private host-local goal reader; server binding/publication is not yet wired."""
from dataclasses import dataclass
import time

SELECTED_HOST = 'host_o6KZLd0kkjchCaH8OjoJZ9aZXHDlFiE2'
MAX_AGE = 5.0
GOAL_STATES = frozenset(('active', 'blocked', 'paused', 'usageLimited', 'budgetLimited', 'complete'))


class GoalReadUnavailable(Exception):
    pass


@dataclass(frozen=True)
class SelectedGoalRead:
    project_id: str
    po_session: str
    thread_id: str
    host_id: str
    designation_generation: str
    issued_at: float
    registration: dict


@dataclass(frozen=True)
class GoalObservation:
    selection: SelectedGoalRead
    status: str
    observed_at: float


def read_selected_goal(selection: SelectedGoalRead, rpc) -> GoalObservation:
    """Read one server-selected thread. Caller must revalidate selection before publishing."""
    from .capacity_observer import _observing_peer
    from .goal_provider_evidence import selected_goal_client
    from .native_owner_evidence import DarwinEvidence, same_owner
    if not isinstance(selection, SelectedGoalRead):
        raise GoalReadUnavailable('invalid selection')
    if selection.host_id != SELECTED_HOST:
        raise GoalReadUnavailable('unsupported host')
    if any(not isinstance(x, str) or not x for x in (selection.project_id, selection.po_session,
                                                       selection.thread_id, selection.designation_generation)):
        raise GoalReadUnavailable('incomplete selection')
    started = time.time()
    if type(selection.issued_at) not in (int, float) or not 0 <= started - selection.issued_at <= MAX_AGE:
        raise GoalReadUnavailable('stale selection')
    try:
        kernel = DarwinEvidence()
        initial = selected_goal_client(selection.registration, kernel)
        before = rpc.connection_identity()
        pair = _observing_peer(kernel, selection.registration, initial[2], before)
        response = rpc.request('thread/goal/get', {'threadId': selection.thread_id})
        after = rpc.connection_identity()
        current = selected_goal_client(selection.registration, kernel)
        same_owner(initial[2], current[2])
        if initial[:2] != current[:2] or initial[3:] != current[3:] or before != after:
            raise GoalReadUnavailable('provider generation changed')
        _observing_peer(kernel, selection.registration, current[2], after, expected=pair)
        now = time.time()
        if not 0 <= now - selection.issued_at <= MAX_AGE:
            raise GoalReadUnavailable('stale observation')
        if not isinstance(response, dict) or set(response) != {'goal'}:
            raise GoalReadUnavailable('invalid response')
        goal = response['goal']
        if goal is None:
            status = 'none'
        elif isinstance(goal, dict) and goal.get('threadId') == selection.thread_id and goal.get('status') in GOAL_STATES:
            status = goal['status']
        else:
            raise GoalReadUnavailable('wrong thread or status')
        return GoalObservation(selection, status, now)
    except GoalReadUnavailable:
        raise
    except Exception as exc:
        raise GoalReadUnavailable('provider verification or request failed') from exc
