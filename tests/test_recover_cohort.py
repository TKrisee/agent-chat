"""Real isolated coordinator/HTTP/client recovery workflows; no manual DB rows."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import unittest

from test_remote_web import RemoteWebFixture, TOKEN
from agent_chat.cli import _prepare_return
from agent_chat.core import CoordError
from agent_chat.recover_cohort import recover_cohort
from agent_chat.private_artifacts import atomic_capture


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def query(data, expression):
    return subprocess.check_output(['jq', '-er', expression], input=data).decode().strip()


class CohortTests(RemoteWebFixture):
    def setUp(self):
        super().setUp()
        self.root = Path(self.tmp.name)
        self.root.chmod(0o700)
        self.call('register', agent='alpha')
        self.call('register', session='b', agent='beta')
        self.call('context')
        self.counter = 0

    def private(self, name, value):
        path = self.root / name
        path.write_text(json.dumps(value, ensure_ascii=False))
        path.chmod(0o600)
        return str(path)

    def claim(self, name, minutes=.003, owner='a'):
        return self.call('request', session=owner, resource='file:' + name, minutes=minutes)

    def entry(self, claim, pids=(), proofs=()):
        self.counter += 1
        evidence = self.root / ('evidence-' + str(self.counter) + '.txt')
        evidence.write_text('Isolated resource bytes unchanged; completed foreground work only.\n')
        receipt = dict(version=2 if pids else 1, resource=claim['resource'],
                       reservation_id=claim['reservation_id'], restored=True,
                       processes_closed=True, closed_at=time.time(), evidence=str(evidence), pids=list(pids))
        if pids: receipt['process_closures'] = list(proofs)
        path = self.private('receipt-' + str(self.counter) + '.json', receipt)
        return dict(resource=claim['resource'], reservation_id=claim['reservation_id'], receipt=path,
                    receipt_sha256=sha(path), evidence_sha256=sha(evidence))

    def manifest(self, entries, **overrides):
        value = dict(version=1, cohort_id='test-cohort', server=self.url, project='default',
                     session='a', host='mac', entries=entries)
        value.update(overrides)
        return self.private('manifest.json', value)

    def prepare(self, entry):
        return _prepare_return(dict(resource=entry['resource'], receipt=entry['receipt']),
                               self.invoke, expected=entry, session='a', host='mac')

    def invoke(self, operation, params):
        return self.call(operation, **params)

    def run_cohort(self, entries=None, resume=False, call=None):
        path = self.manifest(entries) if entries is not None else str(self.root / 'manifest.json')
        return recover_cohort(path, str(self.root / 'journal.jsonl'), resume=resume,
                              server=self.url, project='default', session='a', host='mac',
                              call=call or self.invoke, prepare=self.prepare)

    def expired(self, *claims):
        time.sleep(max(0, max(c['deadline'] for c in claims) - time.time()) + .03)

    def state(self, claim):
        return self.call('status', resources=[claim['resource']])['resources'][0]

    def cli(self, *args):
        command, env = self.cli_command(*args)
        return subprocess.run(command, env=env, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE, timeout=20)

    def cli_command(self, *args):
        env = dict(os.environ, AGENT_CHAT_SESSION='a', AGENT_CHAT_HOST_ID='mac',
                   AGENT_CHAT_PROJECT='default', AGENT_CHAT_STATE_DIR=str(self.root / 'state'))
        env.pop('AGENT_CHAT_TOKEN', None)
        env.pop('AGENT_CHAT_DB', None)
        return [sys.executable, '-m', 'agent_chat.cli', '--server', self.url,
                '--project', 'default', '--api-token', TOKEN, *args], env

    def test_real_cli_two_recoveries_and_resume_never_replays_success(self):
        a, b = self.claim('one'), self.claim('two')
        self.expired(a, b)
        manifest = self.manifest([self.entry(a), self.entry(b)])
        args = ('recover-cohort', '--manifest', manifest, '--journal', str(self.root / 'journal.jsonl'))
        result = self.cli(*args)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(query(result.stdout, '.state'), 'complete')
        # A different later reservation must remain untouched by completed resume.
        newer = self.claim('one', minutes=1)
        result = self.cli(*args, '--resume')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.state(newer)['reservation_id'], newer['reservation_id'])
        events = (self.root / 'journal.jsonl').read_bytes()
        self.assertEqual(query(events, 'select(.kind=="attempt")|.index'), '0\n1')

    def test_bad_receipt_pin_stops_before_second_target(self):
        a, b = self.claim('bad'), self.claim('untouched')
        self.expired(a, b)
        entry = self.entry(a)
        Path(entry['receipt']).write_text('{}')
        result = self.run_cohort([entry, self.entry(b)])
        self.assertEqual(result['state'], 'blocked')
        self.assertEqual(self.state(b)['reservation_id'], b['reservation_id'])

    def test_wrong_owner_and_wrong_rid_fail_closed(self):
        other = self.claim('other', owner='b')
        self.expired(other)
        result = self.run_cohort([self.entry(other)])
        self.assertEqual(result['state'], 'blocked')
        self.assertEqual(self.state(other)['owner_session'], 'b')
        # A changed manifest cannot be resumed against this journal.
        entry = self.entry(other); entry['reservation_id'] = 'wrong-rid'
        with self.assertRaises(CoordError): self.run_cohort([entry], resume=True)

    def test_changed_target_rid_is_not_recovered(self):
        claim = self.claim('target'); self.expired(claim)
        entry = self.entry(claim); entry['reservation_id'] = 'wrong-rid'
        result = self.run_cohort([entry])
        self.assertEqual(result['state'], 'blocked')
        self.assertEqual(self.state(claim)['reservation_id'], claim['reservation_id'])

    def test_evidence_hash_and_foreign_pid_rejected(self):
        claim = self.claim('evidence'); self.expired(claim)
        entry = self.entry(claim); entry['evidence_sha256'] = '0' * 64
        self.assertEqual(self.run_cohort([entry])['state'], 'blocked')
        # Use a real completed unrelated child; never borrow a host/native PID.
        child = subprocess.Popen([sys.executable, '-c', 'pass'])
        child.wait(timeout=5)
        entry = self.entry(claim, [child.pid], [dict(pid=child.pid, closed_at=time.time(),
                           evidence=str(self.root / 'evidence-1.txt'), evidence_sha256=sha(self.root / 'evidence-1.txt'))])
        with self.assertRaisesRegex(CoordError, 'not uniquely associated'):
            self.prepare(entry)

    def test_partial_currentness_refusal_explicit_resume(self):
        a, b = self.claim('first'), self.claim('second'); self.expired(a, b)
        entries = [self.entry(a), self.entry(b)]
        def call(op, params):
            result = self.invoke(op, params)
            if op == 'recover' and params['resource'] == a['resource']:
                self.call('send', session='b', to='a', body='new task before next mutation')
            return result
        result = self.run_cohort(entries, call=call)
        self.assertEqual(result['completed'], [a['reservation_id']])
        self.assertEqual(result['state'], 'rejected')
        self.assertEqual(self.state(b)['reservation_id'], b['reservation_id'])
        self.call('context')
        result = self.run_cohort(resume=True)
        self.assertEqual(result['state'], 'complete')
        self.assertEqual(self.state(a)['state'], 'free')

    def test_lost_response_reconciles_exact_outcome_without_second_effect(self):
        a, b = self.claim('lost'), self.claim('later'); self.expired(a, b)
        calls = []
        def drop(op, params):
            result = self.invoke(op, params)
            if op == 'recover':
                calls.append(params['resource'])
                raise OSError('controlled response loss after real commit')
            return result
        result = self.run_cohort([self.entry(a), self.entry(b)], call=drop)
        self.assertEqual(result['state'], 'uncertain')
        self.assertEqual(self.state(b)['reservation_id'], b['reservation_id'])
        def record(op, params):
            if op == 'recover': calls.append(params['resource'])
            return self.invoke(op, params)
        result = self.run_cohort(resume=True, call=record)
        self.assertEqual(result['state'], 'complete')
        self.assertEqual(calls, [a['resource'], b['resource']])

    def test_unknown_transport_outcome_never_replayed(self):
        claim = self.claim('unknown'); self.expired(claim)
        def stop(op, params):
            if op == 'recover': raise OSError('controlled pre-send transport interruption')
            return self.invoke(op, params)
        self.assertEqual(self.run_cohort([self.entry(claim)], call=stop)['state'], 'uncertain')
        result = self.run_cohort(resume=True)
        self.assertEqual(result['state'], 'uncertain')
        self.assertEqual(self.state(claim)['reservation_id'], claim['reservation_id'])

    def test_manifest_bound_and_invocation_identity(self):
        entry = dict(resource='file:x', reservation_id='r', receipt='/none',
                     receipt_sha256='0'*64, evidence_sha256='0'*64)
        with self.assertRaises(CoordError): self.run_cohort([entry]*65)
        with self.assertRaises(CoordError): self.run_cohort([entry]*2)
        self.manifest([entry], host='another-host')
        with self.assertRaises(CoordError): self.run_cohort()

    def test_real_guarded_closure_v2_and_receipt_pid_binding(self):
        claim = self.claim('guarded', minutes=.07)
        cap = self.root / 'cap.json'
        atomic_capture(cap, dict(resource=claim['resource'], reservation_id=claim['reservation_id'],
                                 token=claim['token'], session_id='a', host_id='mac', server=self.url, project='default'))
        evidence = self.root / 'process.jsonl'
        target = self.root / 'work.txt'; target.write_text('original')
        code = 'from pathlib import Path; p=Path(' + repr(str(target)) + '); p.write_text("temporary"); p.write_text("original")'
        run = self.cli('run', '--capability-file', str(cap), '--reservation-id', claim['reservation_id'],
                       '--process-evidence-file', str(evidence), claim['resource'], '--', sys.executable, '-c', code)
        self.assertEqual(run.returncode, 0, run.stderr)
        self.assertEqual(target.read_text(), 'original')
        context = self.call('guard-context', resource=claim['resource'])
        self.assertEqual(len(context['runs']), 1)
        guard = context['runs'][0]
        self.assertIsNotNone(guard['closed_at'])
        self.expired(claim)
        proof = dict(pid=guard['local_pid'], closed_at=guard['closed_at'], evidence=str(evidence), evidence_sha256=sha(evidence))
        invalid = dict(proof, closed_at=guard['started_at'] - .01)
        with self.assertRaisesRegex(CoordError, 'predates'):
            self.prepare(self.entry(claim, [guard['local_pid']], [invalid]))
        result = self.run_cohort([self.entry(claim, [guard['local_pid']], [proof])])
        self.assertEqual(result['state'], 'complete', result)

    def test_live_restoration_guard_rejected_without_closing_it(self):
        claim = self.claim('live-restore'); self.expired(claim)
        cap = self.root / 'live-cap.json'
        atomic_capture(cap, dict(resource=claim['resource'], reservation_id=claim['reservation_id'],
                                 token=claim['token'], session_id='a', host_id='mac', server=self.url, project='default'))
        command, env = self.cli_command('run', '--restore', '--max-seconds', '10',
                        '--capability-file', str(cap), '--reservation-id', claim['reservation_id'],
                        '--process-evidence-file', str(self.root / 'live-process.jsonl'), claim['resource'],
                        '--', sys.executable, '-c', 'import time; time.sleep(2)')
        process = subprocess.Popen(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            deadline = time.monotonic() + 5
            while True:
                context = self.call('guard-context', resource=claim['resource'])
                if context['runs'] and context['runs'][0]['local_pid']:
                    break
                self.assertLess(time.monotonic(), deadline)
                time.sleep(.03)
            result = self.run_cohort([self.entry(claim)])
            self.assertEqual(result['state'], 'blocked')
            self.assertIn('alive', result['reason'])
            current = self.call('guard-context', resource=claim['resource'])
            self.assertIsNone(current['runs'][0]['closed_at'])
        finally:
            output, error = process.communicate(timeout=15)
        self.assertEqual(process.returncode, 0, error)

    def test_ordinary_single_recover_api_preserved(self):
        claim = self.claim('ordinary'); self.expired(claim)
        entry = self.entry(claim)
        result = self.cli('recover', claim['resource'], '--receipt', entry['receipt'])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(query(result.stdout, '.recovered'), 'true')

    def test_journal_lock_and_truncation_fail_closed(self):
        from agent_chat.recover_cohort import Journal, load_manifest
        claim = self.claim('locked'); self.expired(claim)
        manifest = self.manifest([self.entry(claim)])
        pin, _ = load_manifest(manifest, (self.url, 'default', 'a', 'mac'))
        path = str(self.root / 'journal.jsonl')
        journal = Journal(path, pin, False)
        try:
            with self.assertRaises(OSError): self.run_cohort(resume=True)
        finally: journal.close()
        with open(path, 'ab') as stream: stream.write(b'{')
        with self.assertRaises(CoordError): self.run_cohort(resume=True)
        self.assertEqual(self.state(claim)['reservation_id'], claim['reservation_id'])

    def identity_cohort(self, label, entries=None, *, resume=False, call=None, prepare=None):
        path = str(self.root / (label + '-manifest.json'))
        if entries is not None:
            self.private(label + '-manifest.json', dict(version=1, cohort_id=label, server=self.url,
                         project='default', session='a', host='mac', entries=entries))
        return recover_cohort(path, str(self.root / (label + '-journal.jsonl')), resume=resume,
                   server=self.url, project='default', session='a', host='mac',
                   call=call or self.invoke, prepare=prepare or self.prepare)

    def test_preflight_missing_wrong_owner_host_fail_before_mutation(self):
        for field in ('owner_session', 'host_id'):
            for mode in ('missing', 'wrong'):
                with self.subTest(field=field, mode=mode):
                    label = field + '-' + mode
                    claim = self.claim(label); self.expired(claim)
                    calls = []
                    def fault(op, params):
                        result = self.invoke(op, params)
                        if op == 'status':
                            row = result['resources'][0]
                            self.assertEqual((row['owner_session'], row['host_id']), ('a', 'mac'))
                            # Fault injection only AFTER an actual authenticated
                            # HTTP response; never used as positive authority.
                            if mode == 'missing': row.pop(field)
                            else: row[field] = 'b' if field == 'owner_session' else 'wrong-host'
                        return result
                    def prepare(entry):
                        return _prepare_return(dict(resource=entry['resource'], receipt=entry['receipt']),
                                   fault, expected=entry, session='a', host='mac')
                    def record(op, params):
                        if op == 'recover': calls.append(op)
                        return self.invoke(op, params)
                    result = self.identity_cohort(label, [self.entry(claim)], call=record, prepare=prepare)
                    self.assertEqual(result['state'], 'blocked', result)
                    self.assertEqual(calls, [])
                    self.assertEqual(self.state(claim)['reservation_id'], claim['reservation_id'])
                    # A separate explicit invocation with the real response can
                    # proceed, without relaxing the immutable manifest/journal.
                    result = self.identity_cohort(label, resume=True, call=record)
                    self.assertEqual(result['state'], 'complete', result)
                    self.assertEqual(calls, ['recover'])

    def test_lost_result_missing_wrong_owner_host_never_replays_or_advances(self):
        for field in ('owner_session', 'host_id'):
            for mode in ('missing', 'wrong'):
                with self.subTest(field=field, mode=mode):
                    label = 'result-' + field + '-' + mode
                    first, later = self.claim(label), self.claim(label + '-later')
                    self.expired(first, later)
                    calls = []
                    def drop(op, params):
                        result = self.invoke(op, params)
                        if op == 'recover':
                            calls.append(params['resource'])
                            raise OSError('controlled response loss after real committed recovery')
                        return result
                    result = self.identity_cohort(label, [self.entry(first), self.entry(later)], call=drop)
                    self.assertEqual(result['state'], 'uncertain')
                    def fault(op, params):
                        if op == 'recover': calls.append(params['resource'])
                        result = self.invoke(op, params)
                        if op == 'reservation-result':
                            self.assertEqual((result['owner_session'], result['host_id']), ('a', 'mac'))
                            if mode == 'missing': result.pop(field)
                            else: result[field] = 'b' if field == 'owner_session' else 'wrong-host'
                        return result
                    result = self.identity_cohort(label, resume=True, call=fault)
                    self.assertEqual(result['state'], 'uncertain', result)
                    self.assertEqual(result['completed'], [])
                    self.assertEqual(calls, [first['resource']])
                    self.assertEqual(self.state(later)['reservation_id'], later['reservation_id'])
                    journal = (self.root / (label + '-journal.jsonl')).read_bytes()
                    successes = subprocess.check_output(['jq','-ser','[.[]|select(.kind=="success")]|length'], input=journal).decode().strip()
                    self.assertEqual(successes, '0')
                    def record(op, params):
                        if op == 'recover': calls.append(params['resource'])
                        return self.invoke(op, params)
                    result = self.identity_cohort(label, resume=True, call=record)
                    self.assertEqual(result['state'], 'complete', result)
                    self.assertEqual(calls, [first['resource'], later['resource']])

    def test_authoritative_identity_and_cross_owner_host_filters(self):
        claim = self.claim('authoritative'); self.expired(claim)
        # Spoofed params cannot replace the authenticated outer session/host or
        # the actual stored resource binding used to enrich the response.
        row = self.call('status', resources=[claim['resource']], owner_session='b', host_id='spoof')['resources'][0]
        self.assertEqual((row['owner_session'], row['host_id']), ('a', 'mac'))
        other = self.call('status', session='b', resources=[claim['resource']])['resources'][0]
        self.assertEqual(other['owner_session'], 'a')
        self.assertNotIn('host_id', other)
        with self.assertRaises(CoordError):
            self.call('status', host='wrong-host', resources=[claim['resource']])
        self.assertEqual(self.run_cohort([self.entry(claim)])['state'], 'complete')
        outcome = self.call('reservation-result', resource=claim['resource'], reservation_id=claim['reservation_id'],
                            owner_session='b', host_id='spoof')
        self.assertEqual((outcome['owner_session'], outcome['host_id']), ('a', 'mac'))
        self.assertEqual(outcome['state'], 'cleared')
        self.assertEqual(self.call('reservation-result', session='b', resource=claim['resource'],
                                  reservation_id=claim['reservation_id']), {'state':'unknown'})
        with self.assertRaises(CoordError):
            self.call('reservation-result', host='wrong-host', resource=claim['resource'],
                      reservation_id=claim['reservation_id'])
        self.assertEqual(self.call('reservation-result', resource=claim['resource'],
                                  reservation_id='different-rid'), {'state':'unknown'})

    def test_identity_http_operations_still_require_bearer_authentication(self):
        for operation in ('status', 'reservation-result'):
            payload = dict(op=operation, session='a', host_id='mac', params={})
            self.assertEqual(self.request('POST', '/api/coord', payload, auth=None)[0], 401)
            self.assertEqual(self.request('POST', '/api/coord', payload, auth='basic')[0], 401)
            self.assertEqual(self.request('POST', '/api/coord', payload, auth=None,
                                          headers={'Authorization':'Bearer invalid-test-token'})[0], 401)


if __name__ == '__main__': unittest.main()
