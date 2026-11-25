import hashlib
import os
from pathlib import Path
import plistlib
import pwd
import sqlite3
from types import SimpleNamespace
import unittest
from tempfile import TemporaryDirectory
from unittest import mock

from agent_chat import native_owner_evidence as native
from agent_chat import provider_owner, provider_recovery_launch_config as launch_config
from agent_chat.provider_recovery_peer import Blocked
from agent_chat.provider_recovery_wire import jq


class ProviderCurrentConfigTests(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory(prefix='provider-current-config-')
        self.addCleanup(directory.cleanup)
        self.base = Path(directory.name).resolve()
        self.uid = os.getuid()
        self.config_path = self.base / 'config.toml'
        self.config_path.write_text('model = "current-model"\n', encoding='utf-8')
        self.store = self.base / 'state_5.sqlite'
        self.store.touch()
        self.config = {
            'EnvironmentVariables': {
                'HOME': str(Path(pwd.getpwuid(self.uid).pw_dir).resolve()),
                'CODEX_HOME': str(self.base),
                'CODEX_SQLITE_HOME': str(self.base),
                'PATH': '/usr/bin:/bin',
                'TMPDIR': str(self.base),
                'LANG': 'en_US.UTF-8',
            },
            'ProgramArguments': ['codex', 'app-server', '-c', f'sqlite_home="{self.base}"'],
            'CodexConfigSHA256': '0' * 64,
        }

    def test_current_valid_config_with_or_without_legacy_pin(self):
        expected = hashlib.sha256(self.config_path.read_bytes()).hexdigest()
        _, actual, _ = launch_config.manifest(self.config, self.uid)
        self.assertEqual(actual, expected)

        without_pin = dict(self.config)
        del without_pin['CodexConfigSHA256']
        _, actual, _ = launch_config.manifest(without_pin, self.uid)
        self.assertEqual(actual, expected)
        self.config_path.unlink()
        _, actual, _ = launch_config.manifest(self.config, self.uid)
        self.assertEqual(actual, '<ABSENT>')

    def test_invalid_or_unsafe_config_is_refused(self):
        for content in (b'model = [\n', b'\xff\n'):
            with self.subTest(content=content):
                self.config_path.write_bytes(content)
                with self.assertRaisesRegex(Blocked, 'not valid UTF-8 TOML'):
                    launch_config.manifest(self.config, self.uid)

        self.config_path.unlink()
        self.config_path.symlink_to(self.base / 'absent.toml')
        with self.assertRaisesRegex(Blocked, 'unsafe configured file'):
            launch_config.manifest(self.config, self.uid)

    def test_staged_operation_rechecks_current_bytes_and_store(self):
        store_info = self.store.stat()
        selected = SimpleNamespace(uid=self.uid)
        with mock.patch.object(launch_config, 'open_store',
                               return_value=(self.store, store_info.st_dev, store_info.st_ino)), \
                mock.patch.object(launch_config, 'kernel_locations', return_value={'CODEX_HOME': str(self.base)}):
            captured = launch_config.bind(selected, self.config)
        baseline = jq(captured, '{replacement_configuration:.}')
        self.assertEqual(launch_config.check_baseline(self.config, baseline, self.uid),
                         self.config['EnvironmentVariables'])

        old = self.config_path.read_bytes()
        self.config_path.write_text('model = "another-model"\n', encoding='utf-8')
        with self.assertRaisesRegex(Blocked, 'staged replacement configuration/store identity differs'):
            launch_config.check_baseline(self.config, baseline, self.uid)

        self.config_path.write_bytes(old)
        self.store.rename(self.base / 'prior-state.sqlite')
        self.store.touch()
        with self.assertRaisesRegex(Blocked, 'staged replacement configuration/store identity differs'):
            launch_config.check_baseline(self.config, baseline, self.uid)

        wrong_argv = dict(self.config, ProgramArguments=['codex', 'app-server', '-c', 'sqlite_home="/other"'])
        with self.assertRaisesRegex(Blocked, 'explicitly bind selected sqlite_home'):
            launch_config.manifest(wrong_argv, self.uid)

    def test_owned_launch_retains_exact_manifest_and_image_checks(self):
        image = self.base / 'codex'
        image.write_bytes(b'provider executable fixture')
        image_sha = hashlib.sha256(image.read_bytes()).hexdigest()
        address = 'ws://127.0.0.1:4500'
        config = dict(self.config, Image=str(image), Endpoint=address, ImageSHA256=image_sha,
                      WorkingDirectory=str(self.base),
                      ProgramArguments=[str(image), 'app-server', '--listen', address, *self.config['ProgramArguments'][-2:]])
        manifest = self.base / 'provider.plist'
        raw = plistlib.dumps(config)
        manifest.write_bytes(raw)
        pin = hashlib.sha256(raw).hexdigest()
        run = provider_owner.subprocess.run

        def run_with_codesign_fixture(args, **kwargs):
            if args[:2] == ['/usr/bin/codesign', '--verify']:
                return SimpleNamespace(returncode=0)
            return run(args, **kwargs)

        with mock.patch.object(native, 'IMAGE', image), mock.patch.object(native, 'IMAGE_SHA', image_sha), \
                mock.patch.object(provider_owner.subprocess, 'run', side_effect=run_with_codesign_fixture):
            self.assertEqual(provider_owner.manifest(manifest, pin, address)[0], config)
            self.assertEqual(manifest.read_bytes(), raw)
            with self.assertRaisesRegex(Blocked, 'managed launch manifest changed'):
                provider_owner.manifest(manifest, '1' * 64, address)
            image.write_bytes(b'new same-signer release fixture')
            self.assertEqual(provider_owner.manifest(manifest, pin, address)[0], config)
            self.assertEqual(manifest.read_bytes(), raw)
            with self.assertRaisesRegex(Blocked, 'managed provider disk bytes differ'):
                provider_owner.manifest(manifest, pin, address, require_manifest_image=True)
        with mock.patch.object(native, 'IMAGE', image), mock.patch.object(native, 'IMAGE_SHA', image_sha), \
                mock.patch.object(provider_owner.subprocess, 'run', return_value=SimpleNamespace(returncode=1)):
            with self.assertRaisesRegex(Blocked, 'managed provider signature verification failed'):
                provider_owner.manifest(manifest, pin, address)

    def test_managed_handoff_checks_checkpoint_before_releasing_exec_gate(self):
        store_info = self.store.stat()
        selected = SimpleNamespace(uid=self.uid)
        with mock.patch.object(launch_config, 'open_store',
                               return_value=(self.store, store_info.st_dev, store_info.st_ino)), \
                mock.patch.object(launch_config, 'kernel_locations', return_value={'CODEX_HOME': str(self.base)}):
            captured = launch_config.bind(selected, self.config)
        baseline = jq(captured, '{replacement_configuration:.}')
        directory = self.base / 'provider-owning'
        directory.mkdir()
        manifest = directory / 'provider.plist'
        address = 'ws://127.0.0.1:4500'
        pin = 'a' * 64
        generation = (self.uid, 123, 456, 789, address)
        db = sqlite3.connect(directory / 'service-requests.sqlite3')
        with db:
            db.execute('CREATE TABLE service_requests(id,source,source_sha,manifest,pin,uid,pid,birth_sec,birth_usec,endpoint,state)')
            db.execute('INSERT INTO service_requests VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                       ('fixture', 'source', 'source-digest', str(manifest), pin, *generation, 'running'))
        db.close()
        recovery = self.base / 'provider-recovery-v3'
        recovery.mkdir()
        db = sqlite3.connect(recovery / 'requests.sqlite3')
        with db:
            db.execute('CREATE TABLE requests(id,uid,pid,birth_sec,birth_usec,endpoint,state)')
            db.execute('INSERT INTO requests VALUES(?,?,?,?,?,?,?)', ('fixture', *generation, 'stop_intent'))
            db.execute('CREATE TABLE prestop_proofs(request_id,baseline,baseline_sha,source_id,source_sha)')
            db.execute('INSERT INTO prestop_proofs VALUES(?,?,?,?,?)',
                       ('fixture', baseline, hashlib.sha256(baseline.encode()).hexdigest(), 'source', 'source-digest'))
        db.close()

        ledger = mock.Mock()
        child = SimpleNamespace(pid=987)
        self.config.update(Image='/fixture/codex', ImageSHA256='b' * 64)
        ledger.launched.side_effect = lambda ignored: self.config_path.write_text('model="after-term"\n', encoding='utf-8')
        with mock.patch.object(provider_owner, 'manifest',
                               return_value=(self.config, self.config['EnvironmentVariables'], str(self.base))), \
                mock.patch.object(provider_owner, 'installed_image_sha', return_value='b' * 64), \
                mock.patch.object(provider_owner, 'LaunchLedger', return_value=ledger), \
                mock.patch.object(provider_owner, 'subprocess', SimpleNamespace(
                    Popen=mock.Mock(return_value=child), DEVNULL=provider_owner.subprocess.DEVNULL)):
            provider = provider_owner.OwnedProvider(manifest, pin, address)
            with self.assertRaisesRegex(Blocked, 'staged replacement configuration/store identity differs'):
                provider.start({}, None)
        ledger.claim.assert_called_once()
        ledger.launched.assert_called_once_with(child)
        ledger.release.assert_not_called()

    def test_abandoned_unclaimed_handoff_does_not_block_an_ordinary_start(self):
        directory = self.base / 'provider-owning'
        directory.mkdir()
        provider = object.__new__(provider_owner.OwnedProvider)
        provider.path = directory / 'provider.plist'
        provider.pin = 'a' * 64
        provider.address = 'ws://127.0.0.1:4500'
        db = sqlite3.connect(directory / 'service-requests.sqlite3')
        with db:
            db.execute('CREATE TABLE service_requests(id,source,source_sha,manifest,pin,uid,pid,birth_sec,birth_usec,endpoint,state)')
            for state in ('staging', 'submit_intent'):
                db.execute('INSERT INTO service_requests VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                           (state, 'source', 'digest', str(provider.path), provider.pin, self.uid, 123, 456, 789,
                            provider.address, state))
        db.close()

        self.assertIsNone(provider._handoff_baseline())

    def test_running_observation_uses_live_signer_after_disk_image_changes(self):
        image = self.base / 'codex'
        image.write_bytes(b'currently installed release')
        birth = (8, 9)
        pid = 123
        with mock.patch.object(native, 'dynamic_disk_association') as association, \
                mock.patch.object(native, 'dynamic_signed_identity', return_value='a' * 40) as live:
            self.assertEqual(native.observed_generation(pid, birth, image, '0' * 64), (None, 'a' * 40))
        association.assert_not_called()
        live.assert_called_once_with(pid, birth, image)

    def test_live_signer_proof_keeps_exact_generation_and_checks_actual_signer(self):
        image = self.base / 'codex'
        birth = (8, 9)
        pid = 123
        process = native.Process(pid, 55, 55, self.uid, self.uid, *birth, str(image))
        kernel = mock.Mock()
        kernel.process.return_value = process
        with mock.patch.object(native, 'DarwinEvidence', return_value=kernel), \
                mock.patch.object(native, '_kernel_code_hash', return_value='a' * 40), \
                mock.patch.object(native.subprocess, 'run', return_value=SimpleNamespace(returncode=0)) as verify:
            self.assertEqual(native.dynamic_signed_identity(pid, birth, image), 'a' * 40)
            self.assertEqual(verify.call_args.args[0][-1], '123')
            self.assertEqual(verify.call_args.kwargs['cwd'], '/')
            self.assertIn('-R=', verify.call_args.args[0][-2])
            verify.return_value = SimpleNamespace(returncode=1)
            with self.assertRaisesRegex(native.EvidenceError, 'signer verification failed'):
                native.dynamic_signed_identity(pid, birth, image)
            with self.assertRaisesRegex(native.EvidenceError, 'generation differs'):
                native.dynamic_signed_identity(pid, (8, 10), image)
