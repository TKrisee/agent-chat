import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch
from agent_chat.core import CoordError
from agent_chat.supervisor_worker import SupervisorWorker

class PhaseCleanup(unittest.TestCase):
    def test_failed_uncertainty_report_must_not_be_labeled_as_resume_error(self):
        state=SimpleNamespace(supervisor_observe=Mock())
        rpc=SimpleNamespace(request=Mock(side_effect=OSError('native resume failure')))
        worker=SupervisorWorker(SimpleNamespace(state=state,rpc=rpc))
        summary=dict(after='notLoaded',contradictory=False,history_state='available',queue_count=0)
        def report(job, kind, value):
            if kind == 'unloaded_pending': return {'recovery_candidate':True}
            if kind == 'recovery_claim': return {'claimed':True,'recovery':{'id':'recovery','wake_id':'wake'}}
            if kind == 'recovery_attempt': return {'resume_allowed':True}
            if kind == 'recovery_uncertain': raise CoordError('uncertainty report failed')
            raise AssertionError(kind)
        job={'thread_id':'thread','admission':'running','wake_summary':{'uncertain_wake':False,'pending_wakes':1,'pending_unread_messages':1}}
        with patch.object(worker,'_snapshot',return_value=summary), patch.object(worker,'_report',side_effect=report):
            worker.advance(job)
        observed=state.supervisor_observe.call_args.args[1]['summary']
        self.assertEqual((observed['phase'], observed['error']),('recovery:uncertain-report','uncertainty report failed'))
        self.assertEqual(observed['prior_failure'],{'phase':'recovery:thread/resume','error':'native resume failure'})

    def test_successful_uncertain_report_retains_original_phase(self):
        state=SimpleNamespace(supervisor_observe=Mock())
        rpc=SimpleNamespace(request=Mock(side_effect=OSError('native resume failure')))
        worker=SupervisorWorker(SimpleNamespace(state=state,rpc=rpc))
        summary=dict(after='notLoaded',contradictory=False,history_state='available',queue_count=0)
        def report(job, kind, value):
            if kind == 'unloaded_pending': return {'recovery_candidate':True}
            if kind == 'recovery_claim': return {'claimed':True,'recovery':{'id':'recovery','wake_id':'wake'}}
            if kind == 'recovery_attempt': return {'resume_allowed':True}
            if kind == 'recovery_uncertain': return {'accepted':True}
            raise AssertionError(kind)
        job={'thread_id':'thread','admission':'running','wake_summary':{'uncertain_wake':False,'pending_wakes':1,'pending_unread_messages':1}}
        with patch.object(worker,'_snapshot',return_value=summary), patch.object(worker,'_report',side_effect=report):
            worker.advance(job)
        observed=state.supervisor_observe.call_args.args[1]['summary']
        self.assertEqual((observed['phase'],observed['error']),('recovery:thread/resume','native resume failure'))
        self.assertNotIn('prior_failure',observed)
