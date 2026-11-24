"""Darwin kernel boot-time parser and real sysctl boundary."""
import importlib.util
import pathlib
import platform
import sys
import time
import types
import unittest
from unittest.mock import patch

package=types.ModuleType('agent_chat');package.__path__=[]
guard=types.ModuleType('agent_chat.guard_generation');guard.guard_generation=lambda *args:None
path=pathlib.Path(__file__).resolve().parents[1]/'src/agent_chat/capacity_observer.py'
spec=importlib.util.spec_from_file_location('agent_chat.capacity_observer',path)
observer=importlib.util.module_from_spec(spec)
with patch.dict(sys.modules, {'agent_chat':package, 'agent_chat.guard_generation':guard, spec.name:observer}):
    spec.loader.exec_module(observer)

class BootTimeTests(unittest.TestCase):
    def check_output(self,text):
        with patch.object(observer.subprocess,'run',return_value=types.SimpleNamespace(returncode=0,stderr='',stdout=text)):
            return observer._boot_time()

    @unittest.skipUnless(platform.system()=='Darwin','Darwin sysctl required')
    def test_actual_kernel_sysctl(self):
        value=observer._boot_time()
        self.assertGreater(value,0);self.assertLessEqual(value,time.time())

    def test_structured_date_suffix_and_compact(self):
        sec=int(time.time())-100
        self.assertAlmostEqual(self.check_output(f'{{ sec = {sec}, usec = 870469 }} Thu Oct 29 08:19:55 2026\n'),sec+.870469,places=5)
        self.assertEqual(self.check_output(f'{{sec={sec},usec=0}}'),float(sec))

    def test_malformed_missing_duplicate_and_range_fail_closed(self):
        sec=int(time.time())-100
        bad=[f'{{sec={sec},usec=1,sec={sec}}}',f'{{sec={sec},usec=1,usec=2}}',
             f'{{sec={sec}}}',f'{{usec=1}}',f'sec={sec},usec=1',
             f'{{sec={sec},usec=1000000}}',f'{{sec={sec},usec=-1}}',
             '{sec=0,usec=0}',f'{{sec={int(time.time())+86400},usec=0}}',
             f'{{sec={sec},usec=1}} garbage',f'{{sec={sec},usec=1}} sec=2',
             f'{{sec={sec},usec=1,unknown=2}}',f'{{sec={sec},usec=1.0}}']
        for value in bad:
            with self.subTest(value=value),self.assertRaises(observer.Blocked):self.check_output(value)

    def test_future_fraction_is_rejected(self):
        with patch.object(observer.time,'time',return_value=1000.2):
            with self.assertRaises(observer.Blocked):self.check_output('{sec=1000,usec=900000}')

    def test_failed_sysctl_is_unavailable(self):
        with patch.object(observer.subprocess,'run',return_value=types.SimpleNamespace(returncode=1,stderr='error',stdout='')):
            with self.assertRaises(observer.Blocked):observer._boot_time()

if __name__=='__main__':unittest.main(verbosity=2)
