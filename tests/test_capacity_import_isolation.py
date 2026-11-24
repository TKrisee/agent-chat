"""Discovery must not replace shared app modules with test stubs."""
import importlib
import sys
import unittest


class CapacityImportIsolation(unittest.TestCase):
    def test_import_and_fixture_cleanup_preserve_real_modules(self):
        names = ('agent_chat.service', 'agent_chat.root_lifecycle', 'agent_chat.root_restart')
        originals = {name: importlib.import_module(name) for name in names}
        functions = {('agent_chat.root_lifecycle', name): getattr(originals['agent_chat.root_lifecycle'], name)
                     for name in ('schema','stopped','task_execution_admission')}
        functions[('agent_chat.root_restart','held')] = originals['agent_chat.root_restart'].held
        path_before = list(sys.path)
        fixture = importlib.import_module('test_capacity_admission')
        self.assertEqual(sys.path,path_before)
        for name in names:
            self.assertIs(sys.modules[name], originals[name])
        case = fixture.CapacityAdmissionTests('test_raw_open_guard_without_a_fresh_unused_admission_stays_blocked')
        try:
            case.setUp()
            for (module, name), original in functions.items():
                self.assertIsNot(getattr(originals[module], name), original)
        finally:
            case.doCleanups()
        for (module, name), original in functions.items():
            self.assertIs(getattr(originals[module], name), original)
        for name in names:
            self.assertIs(sys.modules[name], originals[name])
