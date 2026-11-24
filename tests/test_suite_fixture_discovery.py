"""The leaf fixture does not rediscover owning classes with changed setup."""
import unittest

class FixtureDiscoveryTests(unittest.TestCase):
    def test_leaf_and_owning_classes_are_discovered_once(self):
        names=('test_root_local','test_creator_management','test_exact_file_binding',
               'test_guard_file_evidence','test_legacy_closed_recovery_probe')
        def cases(suite):
            for child in suite:
                if isinstance(child,unittest.TestSuite): yield from cases(child)
                else: yield child.id()
        ids=[]
        for name in names:
            ids.extend(cases(unittest.defaultTestLoader.loadTestsFromName(name)))
        self.assertEqual(len(ids),len(set(ids)))
        self.assertEqual(sum(x.endswith('RootLocalTests.test_po_admission_does_not_release_preexisting_native_controls') for x in ids),1)
        self.assertEqual(sum(x.endswith('DeclaredFileEvidence.test_wrong_source_denied_before_guard_or_child') for x in ids),1)
        self.assertEqual(sum(x.endswith('LegacyClosedProbe.test_closed_unprepared_run_is_not_prospective_recovery') for x in ids),1)
        self.assertFalse(any('LegacyClosedProbe.test_po_admission_' in x or 'DeclaredFileEvidence.test_po_admission_' in x for x in ids))
