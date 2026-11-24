"""Read-only evidence required before binding a freshly parked native thread."""
from pathlib import Path

from .core import CoordError
from .native_history import turns_page


def verify_materialized(worker, job):
    """Check exact metadata, a nonempty rollout, empty work and effective settings.

    This check does not write native metadata.  Its caller must journal the
    single metadata mutation before invoking it and must never retry an unknown
    mutation merely because this readback is unavailable.
    """
    thread_id = job['new_thread_id']
    thread = worker.rpc.request('thread/read', {'threadId': thread_id, 'includeTurns': False})['thread']
    if (thread.get('id') != thread_id or thread.get('name') != job['target_agent']
            or thread.get('status', {}).get('type') != 'idle'
            or thread.get('canAcceptDirectInput') is not True):
        raise CoordError('parked materialization metadata is not the exact idle registration')
    raw_path = thread.get('path')
    if not isinstance(raw_path, str) or not raw_path:
        raise CoordError('parked materialization has no rollout path')
    path = Path(raw_path)
    if not path.is_absolute() or not path.is_file() or path.stat().st_size == 0:
        raise CoordError('parked materialization rollout is not a nonempty local file')
    if worker.bridge.queue(thread_id):
        raise CoordError('parked materialization queue is not empty')
    page = turns_page(worker.rpc, {'threadId': thread_id, 'limit': 1,
                                  'itemsView': 'summary', 'sortDirection': 'desc'})
    if page['data'] or page.get('nextCursor'):
        raise CoordError('parked materialization history is not empty')
    _, digest = worker.settings(thread_id, exact=True, model=job.get('requested_model'), effort=job.get('requested_effort'))
    if not digest or digest != job['settings_digest']:
        raise CoordError('parked materialization settings differ from requested settings')
    return {'thread_id': thread_id, 'path': str(path), 'settings_digest': digest}
