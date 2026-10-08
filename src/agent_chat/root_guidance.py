"""Install the optional, developer-customizable global root AGENTS.md baseline."""
from __future__ import annotations

import argparse
import importlib.resources
from pathlib import Path


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workspace', required=True, help='neutral root workspace')
    parser.add_argument('--install', action='store_true', help='install only when AGENTS.md is absent')
    args = parser.parse_args(argv)
    if not args.install:
        parser.error('--install is required')
    workspace = Path(args.workspace).expanduser().resolve()
    if not workspace.is_dir():
        parser.error('workspace must be an existing directory')
    template = importlib.resources.files('agent_chat').joinpath('templates/root-AGENTS.md').read_text()
    target = workspace / 'AGENTS.md'
    if target.exists():
        candidate = workspace / '.agent-chat' / 'root-AGENTS.baseline.md'
        candidate.parent.mkdir(mode=0o700, exist_ok=True)
        if not candidate.exists():
            candidate.write_text(template)
        print(f'Preserved {target}; baseline available at {candidate} for manual merge.')
        return
    target.write_text(template)
    print(f'Installed {target}. Developers may customize this copy.')


if __name__ == '__main__':
    main()
