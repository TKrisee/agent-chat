"""The observer test fixture must not replace the package during discovery."""
import subprocess
import sys
import unittest
from pathlib import Path


class ObserverImportIsolationTests(unittest.TestCase):
    def test_discovery_import_preserves_package_and_successor(self):
        root = Path(__file__).resolve().parents[1]
        script = (
            "import importlib, sys\n"
            f"sys.path[:0] = [{str(root / 'src')!r}, {str(root / 'tests')!r}]\n"
            "import agent_chat\n"
            "original = sys.modules['agent_chat']\n"
            "import test_capacity_observer_boottime\n"
            "assert sys.modules['agent_chat'] is original\n"
            "importlib.import_module('agent_chat.successor_admission')\n"
        )
        completed = subprocess.run([sys.executable, '-I', '-S', '-c', script], capture_output=True, text=True)
        self.assertEqual(completed.returncode, 0, completed.stderr)
