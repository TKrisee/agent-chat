"""Exact setting comparison with named-profile workspace root resolution."""
import copy
import hashlib
import json
from pathlib import PurePosixPath

from .core import CoordError


def settings_data(data):
    keys = ('model', 'modelProvider', 'cwd', 'approvalPolicy', 'approvalsReviewer',
            'reasoningEffort', 'sandbox', 'activePermissionProfile', 'runtimeWorkspaceRoots',
            'multiAgentMode', 'serviceTier')
    values = {key: data.get(key) for key in keys}
    if not all(values.get(key) for key in ('model', 'modelProvider', 'cwd', 'sandbox')):
        raise CoordError('app-server did not return complete settings; reset withheld')
    return values, hashlib.sha256(json.dumps(values, sort_keys=True).encode()).hexdigest()


def profile_chain(config, profile):
    """Return only the selected policy and its parents; never provider credentials."""
    profiles, chain = config.get('permissions') or {}, {}
    while profile and not profile.startswith(':'):
        if profile in chain or not isinstance(profiles.get(profile), dict):
            raise CoordError('named permission profile could not be resolved; creation withheld')
        chain[profile] = copy.deepcopy(profiles[profile])
        profile = profiles[profile].get('extends')
        if not isinstance(profile, str):
            raise CoordError('named permission profile requires an explicit parent; creation withheld')
    if profile != ':workspace':
        raise CoordError('workspace profile reclassification requires :workspace ancestry')
    return chain


def resolve_workspace_settings(source, desired, source_profiles, target_profiles):
    """Reclassify existing explicit profile grants, without granting new roots."""
    profile = (source.get('activePermissionProfile') or {}).get('id')
    if not profile or profile.startswith(':') or source['cwd'] == desired['cwd']:
        return copy.deepcopy(desired)
    # Validate the complete selected inheritance chains, including network and
    # filesystem rules. A project-local override must never silently change policy.
    left = profile_chain({'permissions': source_profiles}, profile)
    right = profile_chain({'permissions': target_profiles}, profile)
    if left != right:
        raise CoordError('named permission profile differs in requested workspace; creation withheld')
    result = copy.deepcopy(desired)
    if source['sandbox'].get('type') != 'workspaceWrite' or result['sandbox'].get('type') != 'workspaceWrite':
        raise CoordError('unsupported named workspace policy reclassification')
    prior = set(source.get('runtimeWorkspaceRoots') or []) | set(source['sandbox'].get('writableRoots') or [])
    roots = set()
    for policy in left.values():
        for root, enabled in (policy.get('workspace_roots') or {}).items():
            if not isinstance(enabled, bool) or not isinstance(root, str) or not PurePosixPath(root).is_absolute() or str(PurePosixPath(root)) != root or '..' in PurePosixPath(root).parts:
                raise CoordError('unsupported named workspace root; creation withheld')
            if enabled:
                if root not in prior:
                    raise CoordError('profile workspace root was not present in original effective permissions')
                roots.add(root)
    writable = result['sandbox'].get('writableRoots')
    if not isinstance(writable, list):
        raise CoordError('missing workspace writable roots; creation withheld')
    implicit = set(result.get('runtimeWorkspaceRoots') or [])
    result['sandbox']['writableRoots'] = writable + sorted(roots - implicit - set(writable))
    return result


def creation_settings(source, job, cwd):
    result = copy.deepcopy(source)
    if job.get('requested_model'): result['model'] = job['requested_model']
    if job.get('requested_effort'): result['reasoningEffort'] = job['requested_effort']
    if job.get('requested_cwd'):
        result['cwd'] = cwd
        result['runtimeWorkspaceRoots'] = [cwd]
    return settings_data(result)[0]


def revalidation_proof(rpc, job):
    """Read-only host proof; the worker repeats admission checks before input."""
    from .rpc import RpcError
    thread = job['new_thread_id']
    observed = rpc.request('thread/read', {'threadId': thread, 'includeTurns': False})['thread']
    if observed.get('status', {}).get('type') != 'idle' or observed.get('canAcceptDirectInput') is not True:
        raise CoordError('candidate is not idle with direct-input admission; revalidation withheld')
    queue = rpc.request('thread/queue/list', {'threadId': thread})
    if queue['data'] or queue.get('nextCursor'):
        raise CoordError('candidate queue is not empty; revalidation withheld')
    try:
        history = rpc.request('thread/turns/list', {'threadId': thread, 'limit': 1, 'itemsView': 'summary', 'sortDirection': 'desc'})
        if history['data'] or history.get('nextCursor'):
            raise CoordError('candidate already has input history; revalidation withheld')
    except RpcError as error:
        if error.code != -32600 or error.message != f'thread {thread} is not materialized yet; thread/turns/list is unavailable before first user message':
            raise
    source, _ = settings_data(rpc.request('thread/resume', {'threadId': job['old_thread_id'], 'excludeTurns': True}))
    profile = (source.get('activePermissionProfile') or {}).get('id')
    if not profile or profile.startswith(':'):
        raise CoordError('workspace revalidation requires a named permission profile')
    profiles = [profile_chain(rpc.request('config/read', {'cwd': cwd, 'includeLayers': False})['config'], profile)
                for cwd in (source['cwd'], job['requested_cwd'])]
    return dict(thread_id=thread, source=source, source_profiles=profiles[0], target_profiles=profiles[1])
