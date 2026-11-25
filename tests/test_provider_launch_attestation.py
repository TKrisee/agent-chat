from dataclasses import replace
import ctypes
import hashlib
import os
from pathlib import Path
import plistlib
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from agent_chat import native_owner_evidence as native, provider_image_admission as admission
from agent_chat import provider_owner as owner, provider_service


@unittest.skipUnless(sys.platform == 'darwin', 'requires Darwin kernel signing evidence')
class ManagedLaunchAttestationTests(unittest.TestCase):
    def test_optional_attestation_timeouts_do_not_interrupt_bridge(self):
        provider = object.__new__(owner.OwnedProvider)
        provider.child = SimpleNamespace(pid=123)
        provider.config = {'Image':'/fixture/codex'}
        provider.attested = False
        provider.next_attestation = provider.next_cli_attestation = 0
        provider.last_cli_attestation = None
        failure = subprocess.TimeoutExpired('codesign',10)
        with patch.object(owner,'identity',return_value=(123,os.getuid(),8,9)), \
                patch.object(native,'DarwinEvidence'), \
                patch.object(owner,'installed_image_sha',side_effect=failure), \
                patch.object(provider_service,'authority_for_handoff',side_effect=failure):
            provider.attest_connected(None)
            self.assertFalse(provider.attested)
            provider.attested = True
            provider.attest_connected(None)
            self.assertIsNone(provider.last_cli_attestation)

    def test_live_kernel_signature_requires_valid_and_excludes_marked_processes(self):
        for flags in (0x20000000, 0x00000001, 0x21000001, 0x30000001):
            with self.subTest(flags=flags):
                def csops(pid, operation, address, size):
                    if operation == 5:
                        ctypes.memmove(address,b'x'*20,20)
                    else:
                        ctypes.cast(address,ctypes.POINTER(ctypes.c_uint))[0]=flags
                    return 0
                with patch.object(native.C,'CDLL',return_value=SimpleNamespace(csops=Mock(side_effect=csops))):
                    with self.assertRaisesRegex(native.EvidenceError,'signing state is invalid'):
                        native._kernel_code_hash(123)

    def test_verified_managed_child_survives_same_signer_disk_replacement(self):
        with tempfile.TemporaryDirectory(prefix='agent-chat-managed-witness-') as folder:
            directory = Path(folder).resolve()
            program = directory / 'fixture.c'
            program.write_text('#include <unistd.h>\nint main(void) { return sleep(DELAY) == 0 ? 0 : 1; }\n')
            image = directory / 'fixture'
            replacement = directory / 'replacement'
            for file, delay in ((image, 15), (replacement, 16)):
                subprocess.run(['/usr/bin/cc', f'-DDELAY={delay}', str(program), '-o', str(file)],
                               check=True, capture_output=True, timeout=8)
                subprocess.run(['/usr/bin/codesign', '--force', '--sign', '-', '--identifier', 'fixture.same', str(file)],
                               check=True, capture_output=True, timeout=8)
            image_sha = hashlib.sha256(image.read_bytes()).hexdigest()
            self.assertNotEqual(hashlib.sha256(replacement.read_bytes()).hexdigest(), image_sha)
            address = 'ws://127.0.0.1:4500'
            manifest = directory / 'provider.plist'
            raw = plistlib.dumps({'Image':str(image),'Endpoint':address})
            manifest.write_bytes(raw)
            pin = hashlib.sha256(raw).hexdigest()
            child = subprocess.Popen([str(image)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            cli_argv = (str(image),'--remote',address,'resume','synthetic-root')
            cli = subprocess.Popen(cli_argv, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            try:
                time.sleep(.05)
                kernel = native.DarwinEvidence()
                process = kernel.process(child.pid)
                cli_process = kernel.process(cli.pid)
                parent = kernel.process(os.getpid())
                birth = process.birth_sec, process.birth_usec
                cli_birth = cli_process.birth_sec,cli_process.birth_usec
                cli_generation = cli_process.pid,cli_process.uid,*cli_birth
                with owner.LaunchLedger(directory / 'owned-provider.sqlite3', pin, address) as ledger:
                    ledger.serial = 1
                    ledger.child = child
                    parent_generation = parent.pid,parent.uid,parent.birth_sec,parent.birth_usec
                    child_generation = process.pid,process.uid,*birth
                    with ledger.db:
                        ledger.db.execute('INSERT INTO child_launches VALUES(?,?,?,?,?,?,?,?,?,?,?)',
                                          (1,'started',*parent_generation,*child_generation,time.time()))
                    with self.assertRaisesRegex(owner.Blocked, 'no prior verified evidence'):
                        owner.retained_child_attestation(manifest,pin,address,process,parent)
                    with patch.object(admission,'REQUIREMENT','identifier "fixture.same"'):
                        code_hash = native.dynamic_signed_identity(process.pid,birth,image)
                        ledger.record_attestation(process,parent_generation,image_sha,code_hash,native._kernel_code_hash(process.pid))
                        cli_code = native.dynamic_signed_identity(cli_process.pid,cli_birth,image)
                        ledger.record_cli_attestation(parent_generation,child_generation,cli_generation,str(image),cli_argv,
                                                      'exact-root-owner',image_sha,cli_code,native._kernel_code_hash(cli_process.pid))
                        with self.assertRaisesRegex(owner.Blocked,'requires actual disk image drift'):
                            owner.retained_child_attestation(manifest,pin,address,process,parent)
                        with patch.object(owner,'configured_attestation_manifest',return_value=(manifest,pin)),patch.object(native,'IMAGE',image):
                            with self.assertRaisesRegex(owner.Blocked,'requires actual disk image drift'):
                                owner.configured_cli_attestation(address,cli_generation,str(image),cli_argv,'exact-root-owner')
                        os.replace(replacement,image)
                        with self.assertRaisesRegex(native.EvidenceError, 'signer verification failed'):
                            native.dynamic_signed_identity(process.pid,birth,image)
                        self.assertEqual(owner.retained_child_attestation(manifest,pin,address,process,parent),
                                         (image_sha,code_hash))
                    with patch.object(owner,'configured_attestation_manifest',return_value=(manifest,pin)),patch.object(native,'IMAGE',image):
                        self.assertEqual(owner.configured_cli_attestation(address,cli_generation,str(image),cli_argv,'exact-root-owner'),
                                         (image_sha,cli_code))
                        for generation,argv,actor in ((child_generation,cli_argv,'exact-root-owner'),
                                                      (cli_generation,cli_argv[:-1],'exact-root-owner'),
                                                      (cli_generation,cli_argv,'wrong-root-owner')):
                            with self.assertRaisesRegex(owner.Blocked,'no prior verified evidence'):
                                owner.configured_cli_attestation(address,generation,str(image),argv,actor)
                    with self.assertRaisesRegex(owner.Blocked, 'no prior verified evidence'):
                        owner.retained_child_attestation(manifest,pin,address,replace(process,birth_usec=birth[1]+1),parent)
                    with self.assertRaises(sqlite3.IntegrityError),ledger.db:
                        ledger.db.execute('UPDATE child_launch_attestations SET kernel_cdhash=? WHERE serial=1',('0'*40,))
                    with ledger.db:
                        ledger.db.execute("UPDATE child_launches SET state='closed' WHERE serial=1")
                    with self.assertRaisesRegex(owner.Blocked, 'no prior verified evidence'):
                        owner.retained_child_attestation(manifest,pin,address,process,parent)
                    with patch.object(owner,'configured_attestation_manifest',return_value=(manifest,pin)),patch.object(native,'IMAGE',image):
                        self.assertEqual(owner.configured_cli_attestation(address,cli_generation,str(image),cli_argv,'exact-root-owner'),
                                         (image_sha,cli_code))
            finally:
                for process_to_close in (child,cli):
                    if process_to_close.poll() is None:process_to_close.terminate()
                    process_to_close.wait(timeout=5)
