"""Private process boundary for the unchanged installed client main loop.

Provider/HTTP observations are test stand-ins. SIGTERM, child exit, and the
client's actual finally/ApprovalPump invalidation path are real.
"""
import contextlib
import os
from pathlib import Path
import sys
import time
import signal
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from agent_chat import bridge_client
from agent_chat.approvals import ApprovalPump

root = Path(sys.argv[1]).resolve()
if not root.is_dir() or not (root / 'fixture-owned').is_file():
    raise SystemExit('explicit private fixture directory required')

def event(value):
    with (root / 'events').open('a') as stream:
        stream.write(str(os.getpid()) + '\t' + value + '\n')
        stream.flush()
        os.fsync(stream.fileno())

class Http:
    token = 'inert-private-fixture'
    def __init__(self, *args, **kwargs): pass

class Rpc:
    def __init__(self, *args, **kwargs): pass
    def connect(self):
        event('rpc-connected')
        if (root/'pause-connect').exists():
            (root/'inside-connect').touch()
            while not (root/'release-connect').exists(): time.sleep(.01)
        return self
    def close(self):
        while (root / 'block-cleanup').exists() and not (root / 'release-cleanup').exists():
            time.sleep(.02)
        event('rpc-closed')

class State:
    def __init__(self, *args, **kwargs): pass
    def call(self, operation, **params):
        event(operation + (':' + params['outcome'] if operation == 'approval-finish' else ''))
        if operation == 'acquire' and (root/'pause-acquire').exists():
            (root/'inside-acquire').touch()
            while not (root/'release-acquire').exists(): time.sleep(.01)
        return {}
    def heartbeat(self, endpoint, pid, error=None):
        event('heartbeat:' + str(pid))
        if pid: (root / ('ready-' + str(pid))).touch()

class Pump(ApprovalPump):
    def start(self):
        # Explicit private pending/request-response observations at the pump
        # boundary; actual close/_invalidate must preserve their distinction.
        self.pending = {
            'pending': dict(state=State(), epoch='fixture-epoch', public='fixture-pending', response_sent=False, reported=False),
            'uncertain': dict(state=State(), epoch='fixture-epoch', public='fixture-uncertain', response_sent=True, reported=False),
        }
        event('approval-fixture-pending')
        return self

class Bridge:
    def __init__(self, *args): pass
    def tick(self, **kwargs): event('tick-existing-records-no-input')

real_identity = bridge_client.client_identity
@contextlib.contextmanager
def identity(server_url, state_file=None):
    # Use the application's real owner/secret/host persistence and flock with
    # an explicit private file. No helper parses its JSON bytes.
    with real_identity(server_url, root/'identity.json') as value:
        event('identity-open:' + value['owner'])
        try: yield value
        finally: event('identity-closed')

stop_codex = bridge_client._stop_codex
real_signal = signal.signal
def install_signal(number, handler):
    if not callable(handler): return real_signal(number, handler)
    def observed(*args):
        handler(*args)
        event('signal-stop-observed')
        (root/'signal-observed').touch()
    return real_signal(number, observed)
def stop_owned(process):
    if process is not None: raise AssertionError('connect-only spawned a provider')
    stop_codex(process)
    event('external-provider-not-stopped')

with contextlib.ExitStack() as patches:
    for name, value in dict(HttpClient=Http, RpcClient=Rpc, RemoteBridgeState=State,
            Bridge=Bridge, client_identity=identity, host_id=lambda: 'fixture-host',
            UsageGuard=mock.Mock(return_value=mock.Mock(check=mock.Mock(return_value={'blocked': False}))),
            HostPolicyScheduler=mock.Mock(return_value=mock.Mock(observe=mock.Mock(return_value=[]))),
            _stop_codex=stop_owned).items():
        patches.enter_context(mock.patch.object(bridge_client, name, value))
    patches.enter_context(mock.patch('agent_chat.approvals.ApprovalPump', Pump))
    patches.enter_context(mock.patch.object(bridge_client.signal, 'signal', install_signal))
    result = bridge_client.main(['--server', 'http://fixture.invalid', '--project', 'fixture',
        '--connect-only', '--codex-server', 'ws://fixture.invalid', '--interval', '.2'])
event('main-exit:' + str(result))
raise SystemExit(result)
