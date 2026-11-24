"""Test-only authorization for isolated, hypothetical successor fixtures.

This is not evidence that any historical request is replayable.
"""
import contextlib
import os
import tempfile
from pathlib import Path
from unittest.mock import patch
from agent_chat import successor_admission as successor

@contextlib.contextmanager
def allowance(root, master, registry):
    root, master, registry = map(lambda p: Path(p).resolve(), (root, master, registry))
    tmp = Path(tempfile.gettempdir()).resolve()
    if root == tmp or not root.is_relative_to(tmp) or not master.is_relative_to(root) or not registry.is_relative_to(root):
        raise ValueError('fixture paths must belong to an isolated temporary directory')
    def checked(request_id, actor, host, target, actual_registry, project):
        if (request_id not in successor.REQUESTS or actor != successor.ROOT or host != successor.HOST
                or Path(target).resolve() != master or Path(actual_registry).resolve() != registry or project != 'default'):
            raise ValueError('synthetic fixture scope differs')
    with patch.object(successor, '_require_unconsumed_allowance', side_effect=checked):
        yield
