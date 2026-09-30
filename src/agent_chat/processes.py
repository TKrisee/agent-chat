"""Conservative, host-local checks for receipt process closure."""

from datetime import datetime, timezone
import os
import subprocess
import time


def receipt_pid_alive(pid: int, closed_at: float) -> bool:
    """A reused PID is harmless only if its current process began after closure.

    ps exposes start times on both macOS and Linux. Its whole-second timestamp
    is a lower bound: equality is ambiguous and must continue to block release.
    Missing/inaccessible process metadata also leaves the hold in place.
    """
    if (isinstance(closed_at, bool) or not isinstance(closed_at, (int, float))
            or not 0 < closed_at <= time.time()):
        raise ValueError("receipt closed_at is invalid")
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    try:
        result = subprocess.run(
            ['ps', '-p', str(pid), '-o', 'lstart='],
            env=dict(os.environ, LC_ALL='C', TZ='UTC0'),
            capture_output=True, text=True, timeout=5, check=True,
        )
        started = datetime.strptime(result.stdout.strip(), '%a %b %d %H:%M:%S %Y')
        return started.replace(tzinfo=timezone.utc).timestamp() <= closed_at
    except (OSError, ValueError, subprocess.SubprocessError):
        return True
