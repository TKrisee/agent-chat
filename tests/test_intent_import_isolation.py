"""Import-order regression for the intent fixture's scoped source fence."""
import importlib
import contextlib
import hashlib
import os
from pathlib import Path
import runpy
import sys
import tempfile
import unittest
from unittest import mock


class IntentImportIsolation(unittest.TestCase):
    def test_import_order_and_scoped_fence(self):
        before = list(sys.meta_path)
        intent = importlib.import_module('test_worker_admission_intent')
        self.assertEqual(sys.meta_path, before)
        name = 'agent_chat.cli'
        previous = sys.modules.pop(name, None)
        fence = intent.Fence()
        try:
            sys.meta_path.insert(0, fence)
            with self.assertRaisesRegex(ImportError, 'outside canonical intent workflow import fence'):
                importlib.import_module(name)
        finally:
            sys.meta_path.remove(fence)
            if previous is not None:
                sys.modules[name] = previous
        self.assertEqual(sys.meta_path, before)
        self.assertEqual(importlib.import_module(name).__name__, name)

    @contextlib.contextmanager
    def cold_module(self, name):
        missing = object()
        previous = sys.modules.pop(name, missing)
        package = sys.modules['agent_chat']
        leaf = name.rsplit('.', 1)[1]
        attribute = getattr(package, leaf, missing)
        if attribute is not missing:
            delattr(package, leaf)
        try:
            yield
        finally:
            sys.modules.pop(name, None)
            if previous is not missing:
                sys.modules[name] = previous
            if attribute is missing:
                if hasattr(package, leaf):
                    delattr(package, leaf)
            else:
                setattr(package, leaf, attribute)

    @contextlib.contextmanager
    def active_fence(self, intent):
        fence = intent.Fence()
        sys.meta_path.insert(0, fence)
        try:
            yield
        finally:
            sys.meta_path.remove(fence)

    def test_current_private_schema_cold_imports(self):
        intent = importlib.import_module('test_worker_admission_intent')
        with self.cold_module('agent_chat.capacity_admission'), \
             self.cold_module('agent_chat.successor_admission'), \
             tempfile.TemporaryDirectory(prefix='intent-schema-') as directory, \
             self.active_fence(intent):
            self.assertNotIn('agent_chat.capacity_admission', sys.modules)
            self.assertNotIn('agent_chat.successor_admission', sys.modules)
            with contextlib.closing(intent.Coordinator(Path(directory) / 'state.sqlite3', 'schema-probe')) as coord:
                for table in ('capacity_admissions', 'successor_admissions', 'successor_admission_uses'):
                    self.assertEqual(coord.db.execute('SELECT count(*) FROM ' + table).fetchone()[0], 0)
            for name in ('capacity_admission', 'successor_admission'):
                self.assertEqual(Path(sys.modules['agent_chat.' + name].__file__).resolve(),
                                 (intent.CANONICAL / (name + '.py')).resolve())

    def test_schema_import_preserves_origin_hash_and_mode_guards(self):
        intent = importlib.import_module('test_worker_admission_intent')
        package = sys.modules['agent_chat']
        name = 'agent_chat.capacity_admission'
        path = (intent.CANONICAL / 'capacity_admission.py').resolve()
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        mode = format(path.stat().st_mode & 0o777, 'o')
        with tempfile.TemporaryDirectory(prefix='intent-foreign-') as directory:
            (Path(directory) / 'capacity_admission.py').write_text("raise RuntimeError('foreign module executed')\n")
            with self.cold_module(name), self.active_fence(intent), \
                 mock.patch.object(package, '__path__', [directory]):
                with self.assertRaisesRegex(ImportError, 'noncanonical intent workflow import'):
                    importlib.import_module(name)
        for expected_digest, expected_mode in (('0' * 64, mode), (digest, '0')):
            with self.subTest(hash=expected_digest, mode=expected_mode), self.cold_module(name), \
                 self.active_fence(intent), mock.patch.dict(intent.PINS, {name:(path, expected_digest, expected_mode)}, clear=True):
                with self.assertRaises(AssertionError):
                    importlib.import_module(name)
        with self.cold_module(name), self.active_fence(intent), \
             mock.patch.dict(intent.PINS, {name:(path, digest, mode)}, clear=True):
            self.assertEqual(Path(importlib.import_module(name).__file__).resolve(), path)

    def test_optional_manifest_rejects_changed_digest_and_scope(self):
        intent = importlib.import_module('test_worker_admission_intent')
        before = list(sys.meta_path)
        rows = ['\t'.join((name, str(path.resolve()), hashlib.sha256(path.read_bytes()).hexdigest(),
                           format(path.stat().st_mode & 0o777, 'o'))) for name, path in sorted(intent.ORIGINS.items())]
        with tempfile.TemporaryDirectory(prefix='intent-manifest-') as directory:
            manifest = Path(directory) / 'inputs.tsv'
            for content, wrong_digest in (('\n'.join(rows) + '\n', True), ('\n'.join(rows[:-1]) + '\n', False)):
                manifest.write_text(content)
                digest = '0' * 64 if wrong_digest else hashlib.sha256(manifest.read_bytes()).hexdigest()
                with self.subTest(wrong_digest=wrong_digest), mock.patch.dict(os.environ, {
                    'AGENT_CHAT_INTENT_IMPORT_MANIFEST':str(manifest), 'AGENT_CHAT_INTENT_IMPORT_MANIFEST_SHA':digest}):
                    with self.assertRaises(AssertionError):
                        runpy.run_path(intent.__file__, run_name='intent_invalid_manifest')
                self.assertEqual(sys.meta_path, before)

    def test_fixture_failure_restores_fence(self):
        intent = importlib.import_module('test_worker_admission_intent')
        before = list(sys.meta_path)
        def fail_fixture(*args, **kwargs):
            self.assertTrue(any(isinstance(finder, intent.Fence) for finder in sys.meta_path))
            raise RuntimeError('fixture failure')
        case = intent.Flow('test_wrong_identity_and_scope')
        result = unittest.TestResult()
        with mock.patch.object(intent, 'Projects', side_effect=fail_fixture):
            case.run(result)
        self.assertEqual(len(result.errors), 1)
        self.assertIn('fixture failure', result.errors[0][1])
        self.assertEqual(sys.meta_path, before)
