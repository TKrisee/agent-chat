import os
import tempfile
import unittest
from pathlib import Path

from agent_chat import project_guidance
from agent_chat.root_orchestrator import RootOrchestrator


class ProjectGuidanceTests(unittest.TestCase):
    def test_install_and_update_preserve_developer_agents_copy(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            target = workspace / 'AGENTS.md'
            target.write_text('# Developer additions\n')
            project_guidance.main(['--workspace', str(workspace), '--install'])
            baseline = workspace / '.agent-chat' / 'project-orchestrator-AGENTS.baseline.md'
            self.assertEqual(target.read_text(), '# Developer additions\n')
            self.assertIn('baseline: v1', baseline.read_text())
            baseline.write_text('stale staged baseline\n')
            project_guidance.main(['--workspace', str(workspace), '--update'])
            self.assertEqual(target.read_text(), '# Developer additions\n')
            self.assertIn('baseline: v1', baseline.read_text())

    def test_install_creates_portable_project_baseline_when_absent(self):
        with tempfile.TemporaryDirectory() as directory:
            project_guidance.main(['--workspace', directory, '--install'])
            content = (Path(directory) / 'AGENTS.md').read_text()
            self.assertIn('Delegate all coding', content)
            self.assertIn('Do not perform those activities yourself, including as a quick check or review substitute.', content)
            self.assertIn('Stay responsive while work is delegated', content)
            self.assertIn("Retain this project's requirements, architecture, decisions", content)
            self.assertNotIn('/Users/', content)

    def test_install_preserves_a_symlink_and_update_only_stages_when_absent(self):
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            external = workspace / 'missing-developer-agents.md'
            target = workspace / 'AGENTS.md'
            target.symlink_to(external.name)
            project_guidance.main(['--workspace', directory, '--install'])
            self.assertTrue(target.is_symlink())
            self.assertFalse(external.exists())
            self.assertTrue((workspace / '.agent-chat' / 'project-orchestrator-AGENTS.baseline.md').is_file())
        with tempfile.TemporaryDirectory() as directory:
            workspace = Path(directory)
            project_guidance.main(['--workspace', directory, '--update'])
            self.assertFalse((workspace / 'AGENTS.md').exists())
            self.assertFalse((workspace / 'AGENTS.md').is_symlink())
            self.assertTrue((workspace / '.agent-chat' / 'project-orchestrator-AGENTS.baseline.md').is_file())

    def test_update_replaces_a_staged_alias_without_mutating_developer_agents(self):
        for alias in ('symlink', 'hardlink'):
            with self.subTest(alias=alias), tempfile.TemporaryDirectory() as directory:
                workspace = Path(directory)
                target = workspace / 'AGENTS.md'
                target.write_text('# Developer additions\n')
                baseline = workspace / '.agent-chat' / 'project-orchestrator-AGENTS.baseline.md'
                baseline.parent.mkdir()
                if alias == 'symlink':
                    baseline.symlink_to(target)
                else:
                    os.link(target, baseline)
                project_guidance.main(['--workspace', directory, '--update'])
                self.assertEqual(target.read_text(), '# Developer additions\n')
                self.assertFalse(baseline.is_symlink())
                self.assertIn('baseline: v1', baseline.read_text())
                self.assertNotEqual(target.stat().st_ino, baseline.stat().st_ino)

    def test_root_prompt_preserves_the_categorical_po_boundary(self):
        prompt = RootOrchestrator.project_prompt('project-id', 'root-session', 'assignment')
        self.assertIn('Keep project requirements, architecture, decisions', prompt)
        self.assertIn('Delegate all coding, source inspection, investigation, testing, technical verification and independent artifact review', prompt)
        self.assertIn('do not perform those activities personally, including as a quick check or review substitute', prompt)
        self.assertIn('Stay responsive through concise asynchronous status/blocker updates', prompt)


if __name__ == '__main__':
    unittest.main()
