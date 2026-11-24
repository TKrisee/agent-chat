import unittest
from agent_chat.native_stage_observer import correlate

def record(seq, method, params):
    return seq, {'method':method, 'params':params}, {'sequence':seq, 'observed_at':100.0+seq, 'observed_monotonic_ns':1000+seq, 'connection_instance':'c', 'connection_epoch':1}
def item(client='job', turn='turn'):
    return record(1, 'item/started', {'threadId':'thread','turnId':turn,'item':{'type':'userMessage','clientId':client}})
def delta(turn='turn'):
    return record(2, 'item/agentMessage/delta', {'threadId':'thread','turnId':turn,'itemId':'i','delta':'ok'})
class Tests(unittest.TestCase):
    def test_pre_response_and_lost_response(self):
        for turn in ('turn', None):
            result=correlate('job','thread',[item(),delta()],start_turn=turn)
            self.assertEqual((result['status'],result['observed_monotonic_ns'],result['coverage']),('observed',1002,'unverified'))
    def test_wrong_input_and_turn(self):
        self.assertEqual(correlate('job','thread',[item('other'),delta()])['status'],'unknown')
        self.assertEqual(correlate('job','thread',[item(),delta()],start_turn='wrong')['status'],'unknown')
    def test_multiple_inputs_and_gap(self):
        self.assertEqual(correlate('job','thread',[item(),item('other'),delta()])['status'],'unknown')
        for reason in ('overflow','reconnect','unsubscribe','unload','reset'):
            self.assertEqual(correlate('job','thread',[item(),delta()],gap=reason)['status'],'unknown')
    def test_batch_returns_job_only_without_source_attribution(self):
        result=correlate('job','thread',[item(),delta()])
        self.assertEqual(result['attribution'],'job_turn_only')
        self.assertFalse(any(k in result for k in ('source_id','recipient_id','delivery_at','native_first_response_at')))
    def test_blank_and_other_turn_are_not_content(self):
        blank=record(2, 'item/agentMessage/delta', {'threadId':'thread','turnId':'turn','itemId':'i','delta':'   '})
        self.assertEqual(correlate('job','thread',[item(),blank,delta('other')])['status'],'unknown')
    def test_committed_but_lost_response_still_sends_gap_censor(self):
        from agent_chat.native_stage_observer import StageWindows
        class State:
            def __init__(self): self.recorded=[]; self.censored=[]
            def native_stage_record(self, job, result):
                self.recorded.append((job,result))
                raise OSError('private lost response after commit')
            def native_stage_censor(self, job, reason): self.censored.append((job,reason))
        state=State(); windows=StageWindows(None,state)
        window={'job':'job','result':correlate('job','thread',[item(),delta()])}
        windows._publish(window)
        window['result']={'status':'unknown','reason':'window_unavailable'}
        windows._publish(window)
        self.assertEqual(state.censored,[('job','window_unavailable')])
