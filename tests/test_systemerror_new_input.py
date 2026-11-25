"""Private real bridge tick after an independently consumed earlier source."""
import contextlib
import os
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest import mock
from agent_chat.bridge import Bridge
from agent_chat.bridge_state import BridgeState
from agent_chat.core import Coordinator
from agent_chat.rpc import TransportError
from test_project_activity import ObservedProvider


class FailedUsageProvider(ObservedProvider):
    def __init__(self):
        super().__init__(); self.calls=[]; self.status_value='systemError'; self.direct=True
        self.usage=True; self.error_code='usageLimitExceeded'
    def request(self,method,params,**kwargs):
        self.calls.append((method,dict(params)))
        if method=='thread/read':
            return {'thread':{'id':params['threadId'],'status':{'type':self.status_value},'canAcceptDirectInput':self.direct}}
        if method=='thread/turns/list':
            if params.get('itemsView') == 'summary' and self.history.get(params['threadId']):
                return {'data':[{'id':'new-turn','status':'inProgress','items':self.history[params['threadId']]}], 'nextCursor':None}
            return {'data':[{'id':'old-terminal-turn','status':'failed','error':{'message':'usage limit reached','codexErrorInfo':self.error_code}}],'nextCursor':None}
        if method=='account/rateLimits/read': return {'ordinaryUsageAllowed':self.usage}
        return super().request(method,params,**kwargs)


class SystemErrorNewInputTests(unittest.TestCase):
    def test_pristine_prepared_job_recovers_after_bridge_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            db=Path(directory)/'state.sqlite3'
            with contextlib.closing(Coordinator(db,'sender')) as sender: sender.register('sender')
            with contextlib.closing(Coordinator(db,'worker')) as worker:
                worker.register('worker'); state=BridgeState(worker); thread=str(uuid.uuid4()); state.bind(thread_id=thread)
                with contextlib.closing(Coordinator(db,'sender')) as sender: sender.send('worker','New source')
                job=state.prepare(thread,[state.pending()[0]],'original wake')
                self.assertIs(job['pristine'],True)
                provider=FailedUsageProvider(); provider.add(thread,directory)
                with mock.patch.dict(os.environ,{'AGENT_CHAT_CAPACITY_RECOVERY':'0'}): Bridge(state,provider).tick(observe_policies=False)
                self.assertEqual(state.job(job['id'])['status'],'dispatched')
                self.assertEqual(sum(m=='thread/queue/add' for m,_ in provider.calls),1)

    def test_previously_attempted_explicit_retry_is_not_special_cased(self):
        with tempfile.TemporaryDirectory() as directory:
            db=Path(directory)/'state.sqlite3'
            with contextlib.closing(Coordinator(db,'sender')) as sender: sender.register('sender')
            with contextlib.closing(Coordinator(db,'worker')) as worker:
                worker.register('worker'); state=BridgeState(worker); thread=str(uuid.uuid4()); state.bind(thread_id=thread)
                with contextlib.closing(Coordinator(db,'sender')) as sender: sender.send('worker','Earlier source')
                source=state.pending()[0]
                job=state.prepare(thread,[source],'original wake')
                state.update(job['id'],'adding'); state.update(job['id'],'failed',error='earlier failure')
                state.retry(job['id'],confirmed=True)
                provider=FailedUsageProvider(); provider.add(thread,directory)
                Bridge(state,provider).advance(state.job(job['id']))
                self.assertFalse(any(m in ('thread/queue/add','thread/queue/start') for m,_ in provider.calls))

    def test_usage_changed_after_selection_never_crosses_queue_add(self):
        class ChangedUsage(FailedUsageProvider):
            def __init__(self): super().__init__(); self.reads=0
            def request(self,method,params,**kwargs):
                if method == 'account/rateLimits/read':
                    self.reads += 1
                    self.usage = self.reads == 1
                return super().request(method,params,**kwargs)
        with tempfile.TemporaryDirectory() as directory:
            db=Path(directory)/'state.sqlite3'
            with contextlib.closing(Coordinator(db,'sender')) as sender: sender.register('sender')
            with contextlib.closing(Coordinator(db,'worker')) as worker:
                worker.register('worker'); state=BridgeState(worker); thread=str(uuid.uuid4()); state.bind(thread_id=thread)
                with contextlib.closing(Coordinator(db,'sender')) as sender: sender.send('worker','New source')
                provider=ChangedUsage(); provider.add(thread,directory)
                with mock.patch.dict(os.environ,{'AGENT_CHAT_CAPACITY_RECOVERY':'0'}): Bridge(state,provider).tick(observe_policies=False)
                self.assertEqual(provider.reads,2)
                self.assertFalse(any(m in ('thread/queue/add','thread/queue/start') for m,_ in provider.calls))

    def test_queue_add_can_autostart_and_lost_response_is_not_replayed(self):
        class Autostart(FailedUsageProvider):
            def request(self, method, params, **kwargs):
                result = super().request(method, params, **kwargs)
                if method == 'thread/queue/add':
                    item = self.queues[params['threadId']].pop()
                    self.history[params['threadId']].append({'type':'userMessage','clientId':item['clientUserMessageId'],'content':item['input']})
                    self.status_value = 'active'
                    raise TransportError('lost queue/add response after native acceptance')
                return result
        with tempfile.TemporaryDirectory() as directory:
            db=Path(directory)/'state.sqlite3'
            with contextlib.closing(Coordinator(db,'sender')) as sender: sender.register('sender')
            with contextlib.closing(Coordinator(db,'worker')) as worker:
                worker.register('worker'); state=BridgeState(worker); thread=str(uuid.uuid4()); state.bind(thread_id=thread)
                with contextlib.closing(Coordinator(db,'sender')) as sender: source=sender.send('worker','New source')['id']
                provider=Autostart(); provider.add(thread,directory)
                bridge=Bridge(state,provider)
                with mock.patch.dict(os.environ,{'AGENT_CHAT_CAPACITY_RECOVERY':'0'}):
                    with self.assertRaises(TransportError): bridge.tick(observe_policies=False)
                    bridge.tick(observe_policies=False)
                job_id=worker.db.execute('SELECT job_id FROM bridge_deliveries WHERE message_id=?',(source,)).fetchone()['job_id']
                self.assertEqual(state.job(job_id)['status'],'dispatched')
                self.assertEqual(sum(m=='thread/queue/add' for m,_ in provider.calls),1)
                self.assertEqual(sum(m=='thread/queue/start' for m,_ in provider.calls),0)

    def test_unavailable_or_nonqualifying_state_never_dispatches(self):
        for condition in ('usage_false','usage_null','direct_false','direct_null','active','other_error'):
            with self.subTest(condition=condition), tempfile.TemporaryDirectory() as directory:
                db=Path(directory)/'state.sqlite3'
                with contextlib.closing(Coordinator(db,'sender')) as sender: sender.register('sender')
                with contextlib.closing(Coordinator(db,'worker')) as worker:
                    worker.register('worker'); state=BridgeState(worker); thread=str(uuid.uuid4()); state.bind(thread_id=thread)
                    with contextlib.closing(Coordinator(db,'sender')) as sender: source=sender.send('worker','New source')['id']
                    provider=FailedUsageProvider(); provider.add(thread,directory)
                    if condition=='usage_false': provider.usage=False
                    if condition=='usage_null': provider.usage=None
                    if condition=='direct_false': provider.direct=False
                    if condition=='direct_null': provider.direct=None
                    if condition=='active': provider.status_value='active'
                    if condition=='other_error': provider.error_code='unauthorized'
                    with mock.patch.dict(os.environ,{'AGENT_CHAT_CAPACITY_RECOVERY':'0'}): Bridge(state,provider).tick(observe_policies=False)
                    self.assertFalse(worker.db.execute('SELECT 1 FROM bridge_deliveries WHERE message_id=?',(source,)).fetchone())
                    self.assertFalse(any(method in ('thread/queue/add','thread/queue/start','turn/start','thread/resume') for method,_ in provider.calls))

    def test_new_unread_input_after_terminal_usage_failure_is_dispatched_once(self):
        with tempfile.TemporaryDirectory() as directory:
            db=Path(directory)/'state.sqlite3'
            with contextlib.closing(Coordinator(db,'sender')) as sender:
                sender.register('sender')
            with contextlib.closing(Coordinator(db,'worker')) as worker:
                worker.register('worker')
                state=BridgeState(worker); thread=str(uuid.uuid4()); state.bind(thread_id=thread)
                with contextlib.closing(Coordinator(db,'sender')) as sender:
                    old=sender.send('worker','Earlier independent source')['id']
                worker.message(old)
                with contextlib.closing(Coordinator(db,'sender')) as sender:
                    fresh=sender.send('worker','New authorized continuation')['id']
                provider=FailedUsageProvider(); provider.add(thread,directory)
                with mock.patch.dict(os.environ,{'AGENT_CHAT_CAPACITY_RECOVERY':'0'}):
                    Bridge(state,provider).tick(observe_policies=False)
                deliveries=worker.db.execute('SELECT message_id,job_id FROM bridge_deliveries ORDER BY message_id').fetchall()
                self.assertNotIn(old,[row['message_id'] for row in deliveries])
                self.assertIn(fresh,[row['message_id'] for row in deliveries], 'fresh direct-input-capable thread received no wake')
                self.assertEqual(sum(method=='thread/queue/start' for method,_ in provider.calls),1)
                self.assertFalse(any(method in ('thread/start','thread/resume') for method,_ in provider.calls))


if __name__ == '__main__': unittest.main()
