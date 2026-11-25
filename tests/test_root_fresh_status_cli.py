"""Operator-facing fresh-status output keeps candidate and readiness distinct."""
import argparse
import contextlib
import io
import subprocess
import unittest
from unittest import mock

from agent_chat.cli import _root_main


class FreshStatusCliContractTests(unittest.TestCase):
    def assert_notice(self, rendered, request_id, state, reason, prerequisite):
        reason_args = ['--arg', 'reason', reason] if reason is not None else ['--argjson', 'reason', 'null']
        check = subprocess.run(
            ['jq', '-e', '--arg', 'request', request_id, '--arg', 'state', state,
             *reason_args, '--arg', 'prerequisite', prerequisite,
             '.operator_notice == {request_id:$request,state:$state,reason:$reason,prerequisite:$prerequisite}'],
            input=rendered, text=True, capture_output=True)
        self.assertEqual(check.returncode, 0, check.stderr)

    def invoke(self, status):
        output = io.StringIO()
        args = argparse.Namespace(session='canonical-home', api_token='fixture', project='default')
        with mock.patch('agent_chat.remote.client_host_id', return_value='fixture-host'), \
                mock.patch('agent_chat.cli._logged_call', return_value=status) as call, \
                contextlib.redirect_stdout(output):
            code = _root_main(['fresh-status', '--request-id', 'request-419'],
                              args, 'https://fixture.invalid', mock.Mock())
        self.assertEqual(code, 0)
        self.assertEqual(call.call_args.args[3]['params'], {'request_id': 'request-419'})
        return output.getvalue()

    def test_unready_candidate_is_visible_but_has_no_resume_guidance(self):
        rendered = self.invoke({
            'id': 'request-419',
            'status': 'created',
            'handoff_state': 'candidate_recovery_required',
            'boot_consumed': False,
            'response_state': 'pending',
            'owner_state': 'unknown',
            'handoff_ready': False,
            'canonical_current_thread_id': 'old-thread',
            'candidate_thread_id': 'candidate-thread',
            'next_action': 'retry_exact_candidate_load_same_request',
            'blocker': 'candidate thread is not loaded',
        })

        self.assertIn('"id": "request-419"', rendered)
        self.assertIn('"handoff_state": "candidate_recovery_required"', rendered)
        self.assertIn('"boot_consumed": false', rendered)
        self.assertIn('"response_state": "pending"', rendered)
        self.assertIn('"owner_state": "unknown"', rendered)
        self.assertIn('"handoff_ready": false', rendered)
        self.assertIn('"canonical_current_thread_id": "old-thread"', rendered)
        self.assertIn('"candidate_thread_id": "candidate-thread"', rendered)
        self.assertIn('"next_action": "retry_exact_candidate_load_same_request"', rendered)
        self.assertIn('"blocker": "candidate thread is not loaded"', rendered)
        self.assertIn('"operator_notice"', rendered)
        self.assertIn('"prerequisite": "retry_exact_candidate_load_same_request"', rendered)
        self.assertIn('"reason": "candidate thread is not loaded"', rendered)
        self.assertIn('"request_id": "request-419"', rendered)
        self.assertIn('"state": "candidate_recovery_required"', rendered)
        self.assert_notice(rendered, 'request-419', 'candidate_recovery_required',
                           'candidate thread is not loaded', 'retry_exact_candidate_load_same_request')
        self.assertNotIn('operator_report', rendered)
        self.assertNotIn('/resume', rendered)

    def test_boot_and_response_proof_do_not_claim_owner_or_handoff_readiness(self):
        rendered = self.invoke({
            'id': 'request-419',
            'status': 'completed',
            'handoff_state': 'response_observed_owner_unverified',
            'boot_consumed': True,
            'response_state': 'observed',
            'owner_state': 'unknown',
            'handoff_ready': False,
            'canonical_current_thread_id': 'candidate-thread',
            'candidate_thread_id': 'candidate-thread',
            'next_action': 'verify_candidate_cli_owner',
            'blocker': 'exact CLI owner proof is not verified',
        })

        self.assertIn('"boot_consumed": true', rendered)
        self.assertIn('"response_state": "observed"', rendered)
        self.assertIn('"owner_state": "unknown"', rendered)
        self.assertIn('"handoff_ready": false', rendered)
        self.assertIn('"next_action": "verify_candidate_cli_owner"', rendered)
        self.assertIn('"prerequisite": "verify_candidate_cli_owner"', rendered)
        self.assertIn('"reason": "exact CLI owner proof is not verified"', rendered)
        self.assert_notice(rendered, 'request-419', 'response_observed_owner_unverified',
                           'exact CLI owner proof is not verified', 'verify_candidate_cli_owner')
        self.assertNotIn('operator_report', rendered)
        self.assertNotIn('/resume', rendered)

    def test_completed_status_preserves_official_resume_guidance(self):
        rendered = self.invoke({
            'id': 'request-419',
            'status': 'completed',
            'handoff_state': 'ready',
            'boot_consumed': True,
            'response_state': 'observed',
            'owner_state': 'verified',
            'handoff_ready': True,
            'canonical_current_thread_id': 'candidate-thread',
            'candidate_thread_id': 'candidate-thread',
            'next_action': 'use_completed_operator_report',
            'blocker': None,
            'operator_report': {
                'request_id': 'request-419',
                'new_thread_id': 'candidate-thread',
                'cli_guidance': 'After verified boot, use /resume and select the new conversation.',
            },
        })

        self.assertIn('"handoff_ready": true', rendered)
        self.assertIn('"operator_report"', rendered)
        self.assertIn('/resume and select the new conversation', rendered)
        self.assertIn('"state": "ready"', rendered)
        self.assert_notice(rendered, 'request-419', 'ready', None, 'use_completed_operator_report')


if __name__ == '__main__':
    unittest.main()
