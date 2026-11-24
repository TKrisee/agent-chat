"""Discovery may import capacity fixtures long before running their tests."""
import time
from unittest.mock import patch
import test_capacity_admission as capacity

class DelayedCapacityFixture(capacity.CapacityAdmissionTests):
    def test_fixture_remains_fresh_after_delayed_module_import(self):
        with patch.object(capacity, 'NOW', time.time() - 120):
            self.test_busy_native_owner_refuses_without_any_audit_or_claim_clear()

def load_tests(loader, tests, pattern):
    return loader.loadTestsFromName('test_fixture_remains_fresh_after_delayed_module_import', DelayedCapacityFixture)
