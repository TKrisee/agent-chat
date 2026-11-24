"""Kernel signing association must use the owned child's actual image and birth."""
import dataclasses
import importlib.util
import pathlib
import platform
import subprocess
import sys
import time
import unittest
from unittest.mock import patch

PATH=pathlib.Path(__file__).resolve().parents[1]/'src/agent_chat/native_owner_evidence.py'
spec=importlib.util.spec_from_file_location('native_inprocess_subject',PATH)
subject=importlib.util.module_from_spec(spec);sys.modules[spec.name]=subject;spec.loader.exec_module(subject)

@unittest.skipUnless(platform.system()=='Darwin','Darwin security APIs required')
class InProcessSigningTests(unittest.TestCase):
    def test_owned_child_signed_image_and_mismatches(self):
        kernel=subject.DarwinEvidence()
        child=subprocess.Popen(['/bin/sleep','30'])
        try:
            deadline=time.monotonic()+3
            while True:
                identity=kernel.process(child.pid)
                if pathlib.Path(identity.image).name=='sleep':break
                if time.monotonic()>=deadline or child.poll() is not None: self.fail('owned child did not exec sleep')
                time.sleep(.01)
            birth=(identity.birth_sec,identity.birth_usec)
            digest=subject.dynamic_disk_association(child.pid,birth,identity.image)
            self.assertRegex(digest,r'^[0-9a-f]{40,64}$')
            with self.assertRaises(subject.EvidenceError):
                subject.dynamic_disk_association(child.pid,(birth[0],birth[1]+1),identity.image)
            with self.assertRaises(subject.EvidenceError):
                subject.dynamic_disk_association(child.pid,birth,'/usr/bin/true')
            actual_process=subject.DarwinEvidence.process
            samples=[]
            def changed_after(_self,pid):
                value=actual_process(_self,pid);samples.append(value)
                return dataclasses.replace(value,birth_usec=value.birth_usec+1) if len(samples)==2 else value
            with patch.object(subject.DarwinEvidence,'process',changed_after):
                with self.assertRaisesRegex(subject.EvidenceError,'process changed'):
                    subject.dynamic_disk_association(child.pid,birth,identity.image)
            self.assertEqual(len(samples),2)
        finally:
            child.terminate();child.wait(timeout=5)
        with self.assertRaises(subject.EvidenceError):
            subject.dynamic_disk_association(child.pid,birth,identity.image)

if __name__=='__main__':unittest.main(verbosity=2)
