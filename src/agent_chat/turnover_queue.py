"""Queue checks for the single stopped-project administrative bootstrap.

The caller must independently establish and revalidate the exact Root grant,
parked job, designation and binding. These checks do not grant authority.
"""
from .core import CoordError


def require_empty_queue(entries):
    """Permit bootstrap submission only after an observed empty native queue."""
    if not isinstance(entries, list) or entries:
        raise CoordError('turnover bootstrap requires an empty native queue')


def require_only_bootstrap(entries, request_id, queue_id):
    """Permit start only for the sole queue item created by this exact job."""
    if (not isinstance(entries, list) or len(entries) != 1
            or not isinstance(entries[0], dict)
            or not isinstance(request_id, str) or not request_id
            or not isinstance(queue_id, str) or not queue_id
            or entries[0].get('id') != queue_id
            or entries[0].get('clientUserMessageId') != request_id):
        raise CoordError('turnover bootstrap requires its sole exact native queue item')
