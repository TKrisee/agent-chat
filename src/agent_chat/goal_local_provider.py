"""Private same-host designated-PO read, with no goal mutation."""
from contextlib import closing
from pathlib import Path
import sqlite3
import time

from .goal_peer_read import MAX_AGE, read_selected_goal
from .goal_registry_read import select_designated_goal, require_current
from .rpc import RpcClient


def _readonly(path):
    selected = Path(path).resolve(strict=True)
    db = sqlite3.connect(selected.as_uri() + '?mode=ro', uri=True, timeout=1)
    db.execute('PRAGMA query_only=ON')
    return db


def read_project_goal(projects, project_id):
    """Select only existing registry projects; errors never become asserted goal state."""
    reason = "selection_unavailable"
    try:
        target = projects.get(project_id)
        if target['id'] != project_id:
            return {'status': 'unavailable', 'reason': reason}
        registry_path = projects.path
        project_path = projects.db_path(target['id'])
        with closing(_readonly(registry_path)) as registry, closing(_readonly(project_path)) as project:
            selection = select_designated_goal(registry, project, target['id'])
            # Validate the locally selected managed listener before connecting to
            # the endpoint recorded by its dispatcher; the reader rechecks it.
            reason = "peer_verification_unavailable"
            from .goal_provider_evidence import selected_goal_client
            from .native_owner_evidence import DarwinEvidence
            selected_goal_client(selection.registration, DarwinEvidence())
            reason = "native_observation_unavailable"
            with RpcClient(selection.registration['endpoint'], timeout=3) as rpc:
                observation = read_selected_goal(selection, rpc)
                reason = "selection_revalidation_unavailable"
                require_current(selection, registry, project)
                reason = "native_observation_unavailable"
                stale = not 0 <= time.time() - observation.observed_at <= MAX_AGE
            reason = "diagnostic_unavailable"
        if stale:
            return {'status': 'unavailable', 'reason': 'observation_stale'}
        return {'status': observation.status, 'observed_at': observation.observed_at}
    except Exception:
        return {'status': 'unavailable', 'reason': reason}
