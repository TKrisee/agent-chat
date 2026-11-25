import subprocess,time,unittest
from unittest.mock import patch
import test_provider_owning_integration as fixtures
from agent_chat import provider_service as s
class ClockScope(unittest.TestCase):
 def test_fixture_sleep_does_not_intercept_subprocess_wait(self):
  fixture=fixtures.Integration('test_receipt_and_real_actor_transition');fixture.setUp();self.addCleanup(fixture.doCleanups)
  self.assertIsNot(s.time,time);self.assertIs(subprocess.time,time)
  with patch.object(s.time,'sleep') as sleep:
   result=subprocess.run(['/bin/sleep','0.05'],timeout=2,check=False)
  self.assertEqual(result.returncode,0);sleep.assert_not_called()
if __name__=='__main__':unittest.main(verbosity=2)
