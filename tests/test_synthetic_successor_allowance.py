"""Scope checks for hypothetical, disposable fixture authorization only."""
import tempfile
import unittest
from pathlib import Path
from agent_chat import successor_admission as successor
from synthetic_successor_allowance import allowance

class AllowanceScope(unittest.TestCase):
    def test_scope_and_guard_restoration(self):
        original = successor._require_unconsumed_allowance
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            master, registry = root / 'master', root / 'registry'
            with allowance(root, master, registry):
                guard = successor._require_unconsumed_allowance
                guard(successor.REQUEST, successor.ROOT, successor.HOST, master, registry, 'default')
                for values in ((successor.REQUEST, 'wrong', successor.HOST, master, registry, 'default'),
                               (successor.REQUEST, successor.ROOT, 'wrong', master, registry, 'default'),
                               (successor.REQUEST, successor.ROOT, successor.HOST, root/'other', registry, 'default'),
                               (successor.REQUEST, successor.ROOT, successor.HOST, master, registry, 'foreign'),
                               ('wrong', successor.ROOT, successor.HOST, master, registry, 'default')):
                    with self.assertRaises(ValueError): guard(*values)
            self.assertIs(successor._require_unconsumed_allowance, original)
            with self.assertRaises(ValueError):
                with allowance(root, root.parent/'outside', registry): pass
