import unittest
from unittest.mock import patch
from agent_chat import provider_service as service, provider_recovery_lifecycle as lifecycle
import test_provider_owned_readiness as fixtures

class AuthoritySeparation(unittest.TestCase):
 def test_clear_readiness_then_authority_denial_blocks(self):
  fixture=fixtures.Readiness('test_owned_error_clears_via_real_heartbeat_then_succeeds_once')
  fixture.setUp()
  self.addCleanup(fixture.doCleanups)
  def clear_and_deny(seconds):
   fixture.heartbeat(None)
   fixture.f.authority.check.side_effect=lifecycle.Blocked('synthetic authority denied')
  with patch.object(service.time,'sleep',side_effect=clear_and_deny):
   result=fixture.f.run_controller()
  self.assertEqual(result.state,'resume_intent')
  self.assertIn('synthetic authority denied',result.reason)
  fixture.f.adapter.stop_once.assert_called_once()
  fixture.f.adapter.start_once.assert_called_once()
  self.assertEqual([x.args[0] for x in fixture.f.adapter.read.call_args_list],['thread/resume','thread/read'])
if __name__=='__main__': unittest.main(verbosity=2)
