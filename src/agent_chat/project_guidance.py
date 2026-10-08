"""Install or stage the optional, developer-customizable project-PO AGENTS.md baseline."""
from __future__ import annotations

import argparse
import importlib.resources
import os
import tempfile
from pathlib import Path


def stage_baseline(candidate, template):
    """Refresh the managed staging file without following an existing alias."""
    candidate.parent.mkdir(mode=0o700, exist_ok=True)
    if candidate.exists() and candidate.is_dir():
        raise IsADirectoryError(f'baseline staging path is a directory: {candidate}')
    descriptor, temporary = tempfile.mkstemp(prefix='.project-orchestrator-AGENTS.', dir=candidate.parent)
    try:
        with os.fdopen(descriptor, 'w') as output:
            output.write(template)
        # replace() swaps the directory entry. It does not write through a
        # symlink or mutate another hard-link to the former staged inode.
        os.replace(temporary, candidate)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workspace', required=True, help='project workspace')
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--install', action='store_true', help='install only when AGENTS.md is absent')
    mode.add_argument('--update', action='store_true', help='stage the current baseline without replacing AGENTS.md')
    args = parser.parse_args(argv)
    workspace = Path(args.workspace).expanduser().resolve()
    if not workspace.is_dir():
        parser.error('workspace must be an existing directory')
    template = importlib.resources.files('agent_chat').joinpath('templates/project-orchestrator-AGENTS.md').read_text()
    target = workspace / 'AGENTS.md'
    candidate = workspace / '.agent-chat' / 'project-orchestrator-AGENTS.baseline.md'
    if args.update:
        stage_baseline(candidate, template)
        print(f'Staged current baseline at {candidate}; {target} remains unchanged.')
        return
    try:
        descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    except FileExistsError:
        descriptor = None
    if descriptor is not None:
        with os.fdopen(descriptor, 'w') as output:
            output.write(template)
        print(f'Installed {target}. Developers may customize this copy.')
        return
    stage_baseline(candidate, template)
    print(f'Preserved existing {target}; current baseline available at {candidate} for manual merge.')


if __name__ == '__main__':
    main()
