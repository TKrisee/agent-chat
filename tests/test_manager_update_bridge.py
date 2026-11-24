import unittest
from agent_chat.bridge import Bridge, BeforeSubmissionRejected, _ExecutionEpochRpc
from agent_chat.rpc import TransportError, RpcError


class State:
    def __init__(self): self.jobs = {}; self.claims = 0; self.prior = []
    def manager_update_candidate(self, thread, message):
        return {'task_id':'task', 'recipient_session':'worker', 'original_job_id':'original'}
    def manager_update_prior(self, thread, message, task, original, turn):
        return self.prior
    def manager_update_claim(self, thread, message, task, original, turn, payload):
        self.claims += 1
        job = {'id':'update', 'thread_id':thread, 'target_session':'worker', 'status':'prepared',
               'payload':payload, 'manager_update':{'task_id':task, 'original_job_id':original,
                                                     'turn_id':turn, 'recipient_session':'worker'}}
        self.jobs[job['id']] = job
        return job
    def update(self, job, status, **kwargs): self.jobs[job]['status'] = status


class Rpc:
    def __init__(self):
        self.flags=[]; self.direct=True; self.original='original'; self.status='inProgress'; self.inputs=[]
        self.queue=[]; self.history=[]; self.failure=None
    def request(self, method, params, **kwargs):
        if method=='thread/read': return {'thread':{'id':'thread','status':{'type':'active','activeFlags':self.flags},'canAcceptDirectInput':self.direct}}
        if method=='thread/queue/list': return {'data':self.queue,'nextCursor':None}
        if method=='thread/turns/list':
            return {'data':[{'id':'turn','status':self.status,'items':[{'type':'userMessage','clientId':self.original}]+self.history}], 'nextCursor':None}
        raise AssertionError(method)


class ManagerUpdateTests(unittest.TestCase):
    def setUp(self):
        self.state=State(); self.rpc=Rpc(); self.bridge=Bridge(self.state,self.rpc)
        self.bridge.payload=lambda thread, messages:'guidance'
        self.message={'id':'message','recipient_session':'worker','manager_update_candidate':True}
        def send(job, method, params):
            self.rpc.inputs.append((job,method,params))
            if self.rpc.failure: raise self.rpc.failure
            return {'turnId':'turn'}
        self.bridge.epoch_request=send
    def submit(self): return self.bridge.manager_update_submit('thread',[self.message])
    def test_busy_target_receives_one_checked_steer(self):
        self.assertTrue(self.submit())
        self.assertEqual(self.state.jobs['update']['status'],'dispatched')
        _,method,p=self.rpc.inputs[0]
        self.assertEqual((method,p['expectedTurnId'],p['clientUserMessageId']),('turn/steer','turn','update'))
    def test_waiting_approval_or_input_is_not_claimed(self):
        for flag in ('waitingOnApproval','waitingOnUserInput'):
            self.rpc.flags=[flag]
            self.assertFalse(self.submit())
        self.assertEqual(self.state.claims,0)
    def test_wrong_original_or_foreign_input_never_claims(self):
        self.rpc.original='wrong'; self.assertFalse(self.submit())
        self.rpc.original='original'; self.rpc.history=[{'type':'userMessage','clientId':'foreign'}]
        self.assertFalse(self.submit()); self.assertEqual(self.state.claims,0)
    def test_prior_same_assignment_update_is_allowed_but_foreign_is_not(self):
        self.state.prior = ['previous']
        self.rpc.history = [{'type':'userMessage','clientId':'previous'}]
        self.assertTrue(self.submit())
        self.assertEqual(self.state.jobs['update']['status'], 'dispatched')
        self.rpc.history.append({'type':'userMessage','clientId':'foreign'})
        self.assertFalse(self.submit())
        self.assertEqual(len(self.rpc.inputs), 1)
    def test_duplicate_prior_or_original_input_is_rejected(self):
        self.state.prior = ['previous']
        self.rpc.history = [{'type':'userMessage','clientId':'previous'}, {'type':'userMessage','clientId':'previous'}]
        self.assertFalse(self.submit())
        self.rpc.history = [{'type':'userMessage','clientId':'original'}]
        self.assertFalse(self.submit())
    def test_proven_pre_submission_rejection_is_never_replayed(self):
        self.rpc.failure = BeforeSubmissionRejected('native epoch rejected before submission')
        self.assertTrue(self.submit())
        self.assertEqual(self.state.jobs['update']['status'], 'cancelled')
        self.assertEqual(len(self.rpc.inputs), 1)
    def test_stale_turn_error_never_retries(self):
        self.rpc.failure=RpcError(-32600,'stale expected turn')
        self.assertTrue(self.submit()); self.assertEqual(self.state.jobs['update']['status'],'uncertain')
        self.bridge.advance(self.state.jobs['update'])
        self.assertEqual(len(self.rpc.inputs),1); self.assertEqual(self.state.jobs['update']['status'],'uncertain')
    def test_lost_response_reconciles_only_exact_turn_and_client(self):
        self.rpc.failure=TransportError('lost response')
        self.assertTrue(self.submit()); self.assertEqual(self.state.jobs['update']['status'],'uncertain')
        self.bridge.advance(self.state.jobs['update']); self.assertEqual(len(self.rpc.inputs),1)
        self.rpc.history.append({'type':'userMessage','clientId':'update'})
        self.bridge.advance(self.state.jobs['update'])
        self.assertEqual(self.state.jobs['update']['status'],'dispatched'); self.assertEqual(len(self.rpc.inputs),1)
    def test_queue_and_missing_direct_capability_do_not_claim(self):
        self.rpc.queue=[{'id':'foreign'}]; self.assertFalse(self.submit())
        self.rpc.queue=[]; self.rpc.direct=False; self.assertFalse(self.submit())
        self.assertEqual(self.state.claims,0)


class EpochRejectionTests(unittest.TestCase):
    def test_coordinator_explicit_rejection_is_typed_and_never_reaches_native(self):
        class Remote:
            def call(self, op, **params): return {'rejected':'ownership changed'}
        class Native:
            def request(self, *args, **kwargs): raise AssertionError('native must not be called')
        rpc = _ExecutionEpochRpc(Remote(), Native())
        with self.assertRaises(BeforeSubmissionRejected) as caught:
            rpc.request('turn/steer', {'threadId':'thread'})
        from agent_chat.core import CoordError
        self.assertIsInstance(caught.exception, CoordError)
    def test_generic_coordinator_error_remains_ambiguous(self):
        from agent_chat.core import CoordError
        class Remote:
            def call(self, op, **params): raise CoordError('not an explicit rejection receipt')
        class Native:
            def request(self, *args, **kwargs): raise AssertionError('native must not be called')
        with self.assertRaises(CoordError) as caught:
            _ExecutionEpochRpc(Remote(), Native()).request('turn/steer', {'threadId':'thread'})
        self.assertNotIsInstance(caught.exception, BeforeSubmissionRejected)

    def test_local_epoch_rejection_is_typed_before_native(self):
        from contextlib import contextmanager
        from unittest import mock
        from agent_chat.core import CoordError
        class Coordinator:
            @contextmanager
            def tx(self): yield object()
        class Local:
            coord = Coordinator()
        class Native:
            def request(self, *args, **kwargs): raise AssertionError('native must not be called')
        with mock.patch('agent_chat.supervisor.cold_epoch_check', side_effect=CoordError('stopped')), \
             mock.patch('agent_chat.supervisor.reject_cold_epoch', return_value=True):
            with self.assertRaises(BeforeSubmissionRejected):
                _ExecutionEpochRpc(Local(), Native()).request('turn/steer', {'threadId':'thread'})
