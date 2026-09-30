"""Read-only checks for an agent's local tools and chat connection."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys

from .core import CoordError
from .remote import HttpClient


def _writable_directory(path: Path, *, allow_missing: bool = False) -> bool:
    if allow_missing:
        while not path.exists() and path != path.parent:
            if path.is_symlink():
                return False
            path = path.parent
    return path.is_dir() and os.access(path, os.R_OK | os.W_OK | os.X_OK)


def diagnose(*, server: str, api_token: str | None, project: str | None = None,
             bridge: bool = False, codex_bin: str = 'codex') -> dict:
    checks = []

    def check(name, ok, detail):
        checks.append({'name': name, 'ok': bool(ok), 'detail': detail})

    check('python', sys.version_info >= (3, 10), 'Python 3.10 or newer is required.')
    check('platform', platform.system() in ('Darwin', 'Linux'), 'Supported platforms are macOS and Linux.')
    check('jq', shutil.which('jq'), 'jq must be on PATH for registration examples and usage measurements.')
    check('ps', shutil.which('ps'), 'ps must be on PATH for process-closure receipt checks.')
    cli = shutil.which('agent-chat-client')
    checkout = Path(__file__).resolve().parents[2] / 'bin' / 'agent-chat-client'
    check('client_cli', cli, 'agent-chat-client is on PATH.' if cli else
          ('Checkout launcher exists; add its bin directory to PATH for agents.' if checkout.is_file() else
           'Install agent-chat and add its executable directory to PATH.'))
    state = Path(os.environ.get('AGENT_CHAT_STATE_DIR') or (Path.home() / '.local' / 'state' / 'agent-chat'))
    root = Path(os.environ.get('AGENT_CHAT_ROOT') or os.getcwd())
    check('host_state', state.is_absolute() and _writable_directory(state, allow_missing=True),
          'Host state must be an absolute writable directory, consistent with the local bridge and private to this machine.')
    check('project_root', root.is_absolute() and _writable_directory(root),
          'Project root must be an absolute existing readable and writable directory.')
    token_present = isinstance(api_token, str) and bool(api_token.strip())
    check('api_token', token_present, 'Provide AGENT_CHAT_API_TOKEN privately; its value is never displayed.')
    project = project or 'default'
    if token_present:
        try:
            response = HttpClient(server, api_token, project='default', timeout=5).call('/api/projects/rpc', {'op': 'list'})
            projects = response.get('projects')
            if not isinstance(projects, list) or any(not isinstance(item, dict) or not isinstance(item.get('id'), str) for item in projects):
                raise ValueError('invalid project list')
            check('server', True, 'Authenticated project listing succeeded.')
            check('project', any(item['id'] == project for item in projects),
                  'The configured project ID must exist on the server.')
        except (CoordError, OSError, ValueError, TypeError):
            # Server errors and subprocess output can contain arbitrary secrets.
            check('server', False, 'Could not authenticate and list projects; check server URL, token and connectivity.')
    else:
        check('server', False, 'Server authentication cannot be checked without an API token.')
    if bridge:
        executable = shutil.which(codex_bin)
        check('codex', executable, 'An executable Codex CLI is required for automatic wakes; use --codex-bin to select it.')
        if executable:
            environment = os.environ.copy()
            for name in ('AGENT_CHAT_API_TOKEN', 'AGENT_CHAT_TOKEN'):
                environment.pop(name, None)
            for name, command, detail in (
                ('codex_version', ['--version'], 'Codex version command must succeed; verify protocol compatibility in docs/bridge.md.'),
                ('codex_login', ['login', 'status'], 'Codex must already be authenticated; run codex login separately if needed.'),
            ):
                try:
                    result = subprocess.run([executable, *command], stdin=subprocess.DEVNULL,
                                            capture_output=True, timeout=5, check=False, env=environment)
                    ok = result.returncode == 0
                except (OSError, subprocess.SubprocessError):
                    ok = False
                check(name, ok, detail)
    return {'ready': all(item['ok'] for item in checks), 'checks': checks,
            'notes': ['Project toolchains and native child follow-up capabilities must be supplied by the agent runtime.',
                      'This check does not start a bridge, register an agent or create a host identity.']}


def main(argv=None, *, connection=None) -> int:
    parser = argparse.ArgumentParser(prog='agent-chat-client doctor', description=__doc__)
    parser.add_argument('--bridge', action='store_true', help='also check the Codex executable and existing login')
    parser.add_argument('--codex-bin', default='codex', help='Codex executable name or path')
    args = parser.parse_args(argv)
    report = diagnose(server=connection.server, api_token=connection.api_token,
                      project=connection.project, bridge=args.bridge, codex_bin=args.codex_bin)
    print(json.dumps(report, sort_keys=True))
    return 0 if report['ready'] else 2
