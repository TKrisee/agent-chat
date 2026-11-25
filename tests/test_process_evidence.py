"""Evidence creation failures remain fail closed and do not expose paths."""
import errno
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock
from agent_chat.core import CoordError
from agent_chat.private_artifacts import ProcessEvidence

class ProcessEvidenceCreationTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path=Path(self.tmp.name)/'private-evidence'
    def test_existing_evidence_preserved(self):
        self.path.write_text('existing facts')
        with self.assertRaisesRegex(CoordError,'exclusive create: destination exists') as error:
            ProcessEvidence(self.path)
        self.assertNotIn(str(self.path),str(error.exception))
        self.assertEqual(self.path.read_text(),'existing facts')
    def test_permission_denial_is_actionable_without_private_path(self):
        original=os.open
        def denied(path,flags,*args,**kwargs):
            if flags & os.O_CREAT: raise PermissionError(errno.EACCES,'secret path must not leak',str(self.path))
            return original(path,flags,*args,**kwargs)
        with mock.patch('agent_chat.private_artifacts.os.open',side_effect=denied):
            with self.assertRaisesRegex(CoordError,'exclusive create: access denied') as error:
                ProcessEvidence(self.path)
        self.assertNotIn('secret',str(error.exception))
        self.assertNotIn(str(self.path),str(error.exception))
        self.assertFalse(self.path.exists())
    def test_post_creation_failures_preserve_partial_file_and_close_descriptor(self):
        for operation,stage in [('fchmod','owner mode'),('fsync','directory sync')]:
            with self.subTest(operation=operation):
                target=self.path.with_name(operation)
                with mock.patch('agent_chat.private_artifacts.os.'+operation,side_effect=OSError(errno.EIO,'failure')):
                    with self.assertRaisesRegex(CoordError,stage+': I/O failure'):
                        ProcessEvidence(target)
                self.assertTrue(target.exists())
                with self.assertRaisesRegex(CoordError,'destination exists'): ProcessEvidence(target)
    def test_malformed_directory_and_symlink_rejected(self):
        with self.assertRaises(CoordError): ProcessEvidence(self.path/'absent'/'file')
        self.path.symlink_to(Path(self.tmp.name)/'target')
        with self.assertRaises(CoordError): ProcessEvidence(self.path)
        self.assertFalse((Path(self.tmp.name)/'target').exists())
